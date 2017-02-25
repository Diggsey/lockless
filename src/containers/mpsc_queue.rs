use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::marker::PhantomData;

use handle::{HandleInner, Handle, IdHandle, ResizingHandle, BoundedHandle, ContainerInner, HandleInnerBase, Tag0, HandleInner1, Id};
use primitives::atomic_ext::AtomicExt;
use primitives::index_allocator::IndexAllocator;
use primitives::invariant::Invariant;

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
pub struct MpscQueueInner<T, SenderTag> {
    // All of the actual values are stored here
    values: Vec<UnsafeCell<Option<T>>>,
    // Mapping from sender ID to an index into the values array
    // Each sender owns exactly one "value slot"
    indices: Vec<UnsafeCell<usize>>,
    // The remaining "value slots" are owned by the ring buffer
    // If a value in the buffer has the EMPTY_BIT set, the
    // corresponding "value slot" is empty.
    ring: Vec<AtomicUsize>,
    // Pair of pointers into the ring buffer
    head: AtomicUsize,
    tail: AtomicUsize,
    phantom: Invariant<SenderTag>,
}

unsafe impl<T: Send, SenderTag> Sync for MpscQueueInner<T, SenderTag> {}

impl<T, SenderTag> ContainerInner<SenderTag> for MpscQueueInner<T, SenderTag> {
    fn raise_id_limit(&mut self, new_limit: usize) {
        let size = self.ring.len();

        assert!(new_limit > self.id_limit());
        assert!(new_limit + size <= TAG_BIT, "Queue too large for system's atomic support");

        let mut len = self.indices.len();
        self.values.reserve_exact(new_limit + size - len);
        self.indices.reserve_exact(new_limit - len);
        while len < new_limit {
            self.values.push(UnsafeCell::new(None));
            self.indices.push(UnsafeCell::new(size + len));
            len += 1;
        }
    }

    fn id_limit(&self) -> usize {
        self.indices.len()
    }
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

impl<T, SenderTag> MpscQueueInner<T, SenderTag> {
    pub fn new(size: usize, max_senders: usize) -> Self {
        assert!(max_senders > 0);
        let mut result = MpscQueueInner {
            values: Vec::with_capacity(max_senders+size),
            indices: Vec::with_capacity(max_senders),
            ring: Vec::with_capacity(size),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            phantom: PhantomData
        };
        for i in 0..size {
            result.values.push(UnsafeCell::new(None));
            result.ring.push(AtomicUsize::new(i));
        }
        result.raise_id_limit(max_senders);
        result
    }

    pub fn resize(&mut self, new_size: usize) {
        let size = self.ring.len();
        let extra = new_size - size;
        self.ring.reserve_exact(extra);
        self.values.reserve_exact(extra);
        for _ in 0..extra {
            let index = self.values.len();
            self.values.push(UnsafeCell::new(None));
            self.ring.push(AtomicUsize::new(index));
        }

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

    pub unsafe fn push(&self, id: Id<SenderTag>, value: T) -> Result<(), T> {
        let id: usize = id.into();
        let size = self.ring.len();
        let size2 = size*2;

        // Need the extra brackets to avoid compiler bug:
        // https://github.com/rust-lang/rust/issues/28935
        let ref mut index = *(&self.indices[id]).get();
        *(&self.values[*index]).get() = Some(value);

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
                    Err(true)
                }
            }) {
                Ok((tail, prev_cell, _)) => {
                    // Update the tail pointer if necessary
                    while self.tail.compare_exchange_weak(tail, next_cell(tail, size2), Ordering::SeqCst, Ordering::Relaxed) == Err(tail) {}
                    *index = prev_cell & VALUE_MASK;
                    return Ok(());
                }
                Err(false) => return Err((*(&self.values[*index]).get()).take().expect("Constraint was violated")),
                Err(true) => {},
            }
        }
    }

    pub unsafe fn pop(&self) -> Result<T, ()> {
        let size = self.ring.len();
        let size2 = size*2;
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // If the queue is empty
        if head % size2 == tail % size2 {
            Err(())
        } else {
            let cell = self.ring[head % size].fetch_add(TAG_BIT, Ordering::AcqRel);
            assert!(cell & TAG_BIT != 0, "Producer advanced without adding an item!");
            let result = (*(&self.values[cell & VALUE_MASK]).get()).take().expect("Constraint was violated");
            self.head.store((head+1) % size2, Ordering::Release);
            Ok(result)
        }
    }
}

#[derive(Debug)]
pub struct MpscQueueReceiver<H: Handle>(H);

impl<T, H: Handle, Tag> MpscQueueReceiver<H> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, Tag>> {
    pub fn new(size: usize, max_senders: usize) -> Self {
        MpscQueueReceiver(HandleInnerBase::new(MpscQueueInner::new(size, max_senders)))
    }

    pub fn receive(&mut self) -> Result<T, ()> {
        // This is safe because we guarantee that we are unique
        self.0.with(|inner| unsafe { inner.pop() })
    }
}

type SenderTag = Tag0;
type Inner<T> = HandleInner1<SenderTag, IndexAllocator, MpscQueueInner<T, SenderTag>>;
pub type ResizingMpscQueueReceiver<T> = MpscQueueReceiver<ResizingHandle<Inner<T>>>;
pub type BoundedMpscQueueReceiver<T> = MpscQueueReceiver<BoundedHandle<Inner<T>>>;

#[derive(Debug)]
pub struct MpscQueueSender<H: Handle, SenderTag>(IdHandle<SenderTag, H>) where H::HandleInner: HandleInner<SenderTag>;

impl<T, H: Handle, SenderTag> MpscQueueSender<H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> + HandleInner<SenderTag> {
    pub fn new(receiver: &MpscQueueReceiver<H>) -> Self {
        MpscQueueSender(IdHandle::new(&receiver.0))
    }
    pub fn try_new(receiver: &MpscQueueReceiver<H>) -> Option<Self> {
        IdHandle::try_new(&receiver.0).map(MpscQueueSender)
    }

    pub fn send(&mut self, value: T) -> Result<(), T> {
        self.0.with_mut(|inner, id| unsafe { inner.push(id, value) })
    }
    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(MpscQueueSender)
    }
}

impl<H: Handle, SenderTag> Clone for MpscQueueSender<H, SenderTag> where H::HandleInner: HandleInner<SenderTag> {
    fn clone(&self) -> Self {
        MpscQueueSender(self.0.clone())
    }
}

pub type ResizingMpscQueueSender<T> = MpscQueueSender<ResizingHandle<Inner<T>>, SenderTag>;
pub type BoundedMpscQueueSender<T> = MpscQueueSender<BoundedHandle<Inner<T>>, SenderTag>;
