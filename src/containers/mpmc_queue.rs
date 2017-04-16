use std::sync::atomic::{AtomicUsize, Ordering};
use std::marker::PhantomData;

use handle::{HandleInner, Handle, IdHandle, ResizingHandle, BoundedHandle, Like};
use primitives::atomic_ext::AtomicExt;
use primitives::index_allocator::IndexAllocator;
use containers::storage::{Place, Storage};
use containers::scratch::Scratch;

// Pointers are only wrapped to 2*Capacity to distinguish full from empty states, so must wrap before indexing!
//  ___________________
// |___|_X_|_X_|___|___|
//       ^       ^
//       H       T
//
// (H == T) => Empty
// (H != T) && (H%C == T%C) => Full
//
//
// Each cell on the ring stores an access count in the high bits:
//  ____________________________
// | access count | value index |
// |____BITS/4____|__REMAINING__|
//
// An odd access count indicates that the cell contains a value,
// while an even access count indicates that the cell is empty.
// All access counts are initialized to zero.
// The access count is used to prevent a form of the ABA problem,
// where a producer tries to store into a cell which is no longer
// the tail of the queue, and happens to have the same value index.

const TAG_BITS: usize = ::POINTER_BITS/4;
const VALUE_MASK: usize = !0 >> TAG_BITS;
const TAG_MASK: usize = !VALUE_MASK;
const TAG_BIT: usize = 1 << (::POINTER_BITS - TAG_BITS);
const WRAP_THRESHOLD: usize = !0 ^ (!0 >> 1);

#[derive(Debug)]
pub struct MpmcQueueInner<T: Like<usize>> {
    // If a value in the buffer has the EMPTY_BIT set, the
    // corresponding "value slot" is empty.
    ring: Vec<AtomicUsize>,
    // Pair of pointers into the ring buffer
    head: AtomicUsize,
    tail: AtomicUsize,
    phantom: PhantomData<T>,
}

fn next_cell(mut index: usize, size2: usize) -> usize {
    index += 1;
    if index >= WRAP_THRESHOLD {
        index = index % size2;
    }
    index
}

fn wraps_around(start: usize, end: usize, size: usize) -> bool {
    let size2 = size*2;
    (end % size) < (start % size) || ((start + size) % size2 == (end % size2))
}

fn rotate_slice<T>(slice: &mut [T], places: usize) {
    slice.reverse();
    let (a, b) = slice.split_at_mut(places);
    a.reverse();
    b.reverse();
}

impl<T: Like<usize>> MpmcQueueInner<T> {
    pub fn new<I: IntoIterator<Item=T>>(iter: I) -> Self {
        MpmcQueueInner {
            ring: iter.into_iter().map(Into::into).map(AtomicUsize::new).collect(),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            phantom: PhantomData
        }
    }

    pub fn extend<I: IntoIterator<Item=T>>(&mut self, iter: I) where I::IntoIter: ExactSizeIterator {
        let iter = iter.into_iter();
        let size = self.ring.len();
        let extra = iter.len();
        self.ring.reserve_exact(extra);
        self.ring.extend(iter.map(Into::into).map(AtomicUsize::new));

        // If the queue wraps around the buffer, shift the elements
        // along such that the start section of the queue is moved to the
        // new end of the buffer.
        let head = self.head.get_mut();
        let tail = self.tail.get_mut();
        if wraps_around(*head, *tail, size) {
            rotate_slice(&mut self.ring[*head..], extra);
            *head += extra;
        }
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub unsafe fn push(&self, value: &mut T) -> bool {
        let size = self.ring.len();
        let size2 = size*2;

        let index = value.borrow_mut();

        loop {
            match self.tail.try_update_indirect(|tail| {
                let head = self.head.load(Ordering::SeqCst);
                // If not full
                if (tail % size2) != (head + size) % size2 {
                    // Try updating cell at tail position
                    Ok(&self.ring[tail % size])
                } else {
                    // We observed a full queue, so stop trying
                    Err(false)
                }
            }, |tail, cell| {
                // If cell at tail is empty
                if cell & TAG_BIT == 0 {
                    // Swap in our index, and mark as full
                    Ok((cell & TAG_MASK).wrapping_add(TAG_BIT) | *index)
                } else {
                    // Cell is full, another thread is midway through an insertion
                    // Try to assist the stalled thread
                    let _ = self.tail.compare_exchange_weak(tail, next_cell(tail, size2), Ordering::SeqCst, Ordering::Relaxed);
                    // Retry the insertion now that we've helped the other thread to progress
                    Err(true)
                }
            }) {
                Ok((tail, prev_cell, _)) => {
                    // Update the tail pointer if necessary
                    while self.tail.compare_exchange_weak(tail, next_cell(tail, size2), Ordering::SeqCst, Ordering::Relaxed) == Err(tail) {}
                    *index = prev_cell & VALUE_MASK;
                    return true;
                }
                Err(false) => return false,
                Err(true) => {},
            }
        }
    }

    pub unsafe fn pop(&self, value: &mut T) -> bool {
        let size = self.ring.len();
        let size2 = size*2;

        let index = value.borrow_mut();

        loop {
            match self.head.try_update_indirect(|head| {
                let tail = self.tail.load(Ordering::SeqCst);
                // If not empty
                if head % size2 != tail % size2 {
                    // Try updating cell at head position
                    Ok(&self.ring[head % size])
                } else {
                    // We observed an empty queue, so stop trying
                    Err(false)
                }
            }, |head, cell| {
                // If cell at head is full
                if cell & TAG_BIT != 0 {
                    // Swap in our index, and mark as empty
                    Ok((cell & TAG_MASK).wrapping_add(TAG_BIT) | *index)
                } else {
                    // Cell is empty, another thread is midway through a removal
                    // Try to assist the stalled thread
                    let _ = self.head.compare_exchange_weak(head, next_cell(head, size2), Ordering::SeqCst, Ordering::Relaxed);
                    // Retry the insertion now that we've helped the other thread to progress
                    Err(true)
                }
            }) {
                Ok((head, prev_cell, _)) => {
                    // Update the tail pointer if necessary
                    while self.head.compare_exchange_weak(head, next_cell(head, size2), Ordering::SeqCst, Ordering::Relaxed) == Err(head) {}
                    *index = prev_cell & VALUE_MASK;
                    return true;
                }
                Err(false) => return false,
                Err(true) => {},
            }
        }
    }
}

define_id!(MpmcQueueAccessorId);

pub struct MpmcQueueWrapper<T> {
    storage: Storage<T>,
    scratch: Scratch<MpmcQueueAccessorId, Place<T>>,
    inner: MpmcQueueInner<Place<T>>,
    id_alloc: IndexAllocator
}

impl<T> MpmcQueueWrapper<T> {
    pub fn new<H: Handle<HandleInner=Self>>(id_limit: usize, size: usize) -> H {
        assert!(id_limit > 0);
        let mut storage = Storage::with_capacity(id_limit + size);
        let scratch = Scratch::new(storage.none_storing_iter(id_limit));
        let inner = MpmcQueueInner::new(storage.none_storing_iter(size));
        let id_alloc = IndexAllocator::new(id_limit);

        Handle::new(MpmcQueueWrapper {
            storage: storage,
            scratch: scratch,
            inner: inner,
            id_alloc: id_alloc,
        })
    }

    pub unsafe fn push(&self, id: &mut MpmcQueueAccessorId, value: T) -> Result<(), T> {
        let place = self.scratch.get_mut(id);
        self.storage.replace(place, Some(value));
        if self.inner.push(place) {
            Ok(())
        } else {
            Err(self.storage.replace(place, None).expect("Some(value) in container"))
        }
    }

    pub unsafe fn pop(&self, id: &mut MpmcQueueAccessorId) -> Result<T, ()> {
        let place = self.scratch.get_mut(id);
        if self.inner.pop(place) {
            Ok(self.storage.replace(place, None).expect("Some(value) in container"))
        } else {
            Err(())
        }
    }
}

impl<T> HandleInner<MpmcQueueAccessorId> for MpmcQueueWrapper<T> {
    type IdAllocator = IndexAllocator;
    fn id_allocator(&self) -> &IndexAllocator {
        &self.id_alloc
    }
    fn raise_id_limit(&mut self, new_limit: usize) {
        let old_limit = self.id_limit();
        assert!(new_limit > old_limit);
        let extra = new_limit - old_limit;
        self.storage.reserve(extra);
        self.scratch.extend(self.storage.none_storing_iter(extra));
        self.id_alloc.resize(new_limit);
    }
}

#[derive(Debug)]
pub struct MpmcQueueReceiver<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>>(IdHandle<H, MpmcQueueAccessorId>);

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> MpmcQueueReceiver<T, H> {
    pub fn receive(&mut self) -> Result<T, ()> {
        self.0.with_mut(|inner, id| unsafe { inner.pop(id) })
    }

    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(MpmcQueueReceiver)
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Clone for MpmcQueueReceiver<T, H> {
    fn clone(&self) -> Self {
        MpmcQueueReceiver(self.0.clone())
    }
}

pub type ResizingMpmcQueueReceiver<T> = MpmcQueueReceiver<T, ResizingHandle<MpmcQueueWrapper<T>>>;
pub type BoundedMpmcQueueReceiver<T> = MpmcQueueReceiver<T, BoundedHandle<MpmcQueueWrapper<T>>>;

#[derive(Debug)]
pub struct MpmcQueueSender<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>>(IdHandle<H, MpmcQueueAccessorId>);

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> MpmcQueueSender<T, H> {
    pub fn send(&mut self, value: T) -> Result<(), T> {
        self.0.with_mut(|inner, id| unsafe { inner.push(id, value) })
    }
    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(MpmcQueueSender)
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Clone for MpmcQueueSender<T, H> {
    fn clone(&self) -> Self {
        MpmcQueueSender(self.0.clone())
    }
}

pub type ResizingMpmcQueueSender<T> = MpmcQueueSender<T, ResizingHandle<MpmcQueueWrapper<T>>>;
pub type BoundedMpmcQueueSender<T> = MpmcQueueSender<T, BoundedHandle<MpmcQueueWrapper<T>>>;

pub fn new<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>>(max_accessors: usize, size: usize) -> (MpmcQueueSender<T, H>, MpmcQueueReceiver<T, H>) {
    let inner = MpmcQueueWrapper::new(max_accessors, size);
    (MpmcQueueSender(IdHandle::new(&inner)), MpmcQueueReceiver(IdHandle::new(&inner)))
}
