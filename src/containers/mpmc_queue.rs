use std::sync::atomic::{AtomicUsize, Ordering};
use std::marker::PhantomData;

use handle::{HandleInner, Handle, IdHandle, ResizingHandle, BoundedHandle, ContainerInner, HandleInnerBase, Tag0, Tag1, HandleInner2, Id, Distinct};
use primitives::atomic_ext::AtomicExt;
use primitives::index_allocator::IndexAllocator;
use primitives::invariant::Invariant;
use containers::id_map::IdMap2;

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
pub struct MpmcQueueInner<T, SenderTag, ReceiverTag> {
    // All of the actual values are stored here
    id_map: IdMap2<T, SenderTag, ReceiverTag>,
    // If a value in the buffer has the EMPTY_BIT set, the
    // corresponding "value slot" is empty.
    ring: Vec<AtomicUsize>,
    // Pair of pointers into the ring buffer
    head: AtomicUsize,
    tail: AtomicUsize,
    phantom: Invariant<(SenderTag, ReceiverTag)>,
}

impl<T, SenderTag, ReceiverTag> ContainerInner<SenderTag> for MpmcQueueInner<T, SenderTag, ReceiverTag> where (SenderTag, ReceiverTag): Distinct {
    fn raise_id_limit(&mut self, new_limit: usize) {
        ContainerInner::<SenderTag>::raise_id_limit(&mut self.id_map, new_limit);
    }

    fn id_limit(&self) -> usize {
        ContainerInner::<SenderTag>::id_limit(&self.id_map)
    }
}

impl<T, SenderTag, ReceiverTag> ContainerInner<ReceiverTag> for MpmcQueueInner<T, SenderTag, ReceiverTag> where (SenderTag, ReceiverTag): Distinct {
    fn raise_id_limit(&mut self, new_limit: usize) {
        ContainerInner::<SenderTag>::raise_id_limit(&mut self.id_map, new_limit);
    }

    fn id_limit(&self) -> usize {
        ContainerInner::<SenderTag>::id_limit(&self.id_map)
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

impl<T, SenderTag, ReceiverTag> MpmcQueueInner<T, SenderTag, ReceiverTag> where (SenderTag, ReceiverTag): Distinct {
    pub fn new(size: usize, sender_id_limit: usize, receiver_id_limit: usize) -> Self {
        assert!(sender_id_limit > 0);
        assert!(receiver_id_limit > 0);
        let mut result = MpmcQueueInner {
            id_map: IdMap2::new(),
            ring: Vec::with_capacity(size),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            phantom: PhantomData
        };
        result.id_map.reserve(size, sender_id_limit + receiver_id_limit);
        for _ in 0..size {
            result.ring.push(AtomicUsize::new(result.id_map.push_value(None)));
        }
        ContainerInner::<SenderTag>::raise_id_limit(&mut result, sender_id_limit);
        ContainerInner::<ReceiverTag>::raise_id_limit(&mut result, receiver_id_limit);
        result
    }

    pub fn resize(&mut self, new_size: usize) {
        let size = self.ring.len();
        let extra = new_size - size;
        self.ring.reserve_exact(extra);
        self.id_map.reserve_values(extra);
        for _ in 0..extra {
            let index = self.id_map.push_value(None);
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
        let size = self.ring.len();
        let size2 = size*2;

        let mut index = self.id_map.store(id, Some(value));

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
                    return Ok(());
                }
                Err(false) => return Err(self.id_map.load_at(*index).expect("Constraint was violated")),
                Err(true) => {},
            }
        }
    }

    pub unsafe fn pop(&self, id: Id<ReceiverTag>) -> Result<T, ()> {
        let size = self.ring.len();
        let size2 = size*2;

        let mut index = self.id_map.store(id, None);

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
                    return Ok(self.id_map.load_at(*index).expect("Constraint was violated"));
                }
                Err(false) => return Err(()),
                Err(true) => {},
            }
        }
    }
}

#[derive(Debug)]
pub struct MpmcQueueReceiver<H: Handle, ReceiverTag>(IdHandle<ReceiverTag, H>) where H::HandleInner: HandleInner<ReceiverTag>;

impl<T, H: Handle, SenderTag, ReceiverTag> MpmcQueueReceiver<H, ReceiverTag> where H::HandleInner: HandleInner<ReceiverTag, ContainerInner=MpmcQueueInner<T, SenderTag, ReceiverTag>>, (SenderTag, ReceiverTag): Distinct {
    pub fn new(size: usize, max_senders: usize, max_receivers: usize) -> Self {
        MpmcQueueReceiver(IdHandle::new(&HandleInnerBase::new(MpmcQueueInner::new(size, max_senders, max_receivers))))
    }

    pub fn receive(&mut self) -> Result<T, ()> {
        self.0.with_mut(|inner, id| unsafe { inner.pop(id) })
    }

    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(MpmcQueueReceiver)
    }
}

impl<H: Handle, ReceiverTag> Clone for MpmcQueueReceiver<H, ReceiverTag> where H::HandleInner: HandleInner<ReceiverTag> {
    fn clone(&self) -> Self {
        MpmcQueueReceiver(self.0.clone())
    }
}

type SenderTag = Tag0;
type ReceiverTag = Tag1;
type Inner<T> = HandleInner2<SenderTag, IndexAllocator, ReceiverTag, IndexAllocator, MpmcQueueInner<T, SenderTag, ReceiverTag>>;
pub type ResizingMpmcQueueReceiver<T> = MpmcQueueReceiver<ResizingHandle<Inner<T>>, ReceiverTag>;
pub type BoundedMpmcQueueReceiver<T> = MpmcQueueReceiver<BoundedHandle<Inner<T>>, ReceiverTag>;

#[derive(Debug)]
pub struct MpmcQueueSender<H: Handle, SenderTag>(IdHandle<SenderTag, H>) where H::HandleInner: HandleInner<SenderTag>;

impl<T, H: Handle, SenderTag, ReceiverTag> MpmcQueueSender<H, SenderTag> where H::HandleInner: HandleInner<SenderTag, ContainerInner=MpmcQueueInner<T, SenderTag, ReceiverTag>>, (SenderTag, ReceiverTag): Distinct {
    pub fn send(&mut self, value: T) -> Result<(), T> {
        self.0.with_mut(|inner, id| unsafe { inner.push(id, value) })
    }
    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(MpmcQueueSender)
    }
}

impl<H: Handle, SenderTag> Clone for MpmcQueueSender<H, SenderTag> where H::HandleInner: HandleInner<SenderTag> {
    fn clone(&self) -> Self {
        MpmcQueueSender(self.0.clone())
    }
}

pub type ResizingMpscQueueSender<T> = MpmcQueueSender<ResizingHandle<Inner<T>>, SenderTag>;
pub type BoundedMpscQueueSender<T> = MpmcQueueSender<BoundedHandle<Inner<T>>, SenderTag>;

pub fn new<T, H: Handle, SenderTag, ReceiverTag>(size: usize, max_senders: usize, max_receivers: usize) -> (MpmcQueueSender<H, SenderTag>, MpmcQueueReceiver<H, ReceiverTag>)
    where H::HandleInner: HandleInner<SenderTag, ContainerInner=MpmcQueueInner<T, SenderTag, ReceiverTag>> + HandleInner<ReceiverTag, ContainerInner=MpmcQueueInner<T, SenderTag, ReceiverTag>>, (SenderTag, ReceiverTag): Distinct
{
    let inner = HandleInnerBase::new(MpmcQueueInner::new(size, max_senders, max_receivers));
    (MpmcQueueSender(IdHandle::new(&inner)), MpmcQueueReceiver(IdHandle::new(&inner)))
}
