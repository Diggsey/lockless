use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;

use handle::{HasLen, Resizable, Handle, IdHandle, ResizingHandle, BoundedHandle};
use primitives::atomic_ext::AtomicExt;

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
pub struct MpscQueueInner<T> {
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
}

unsafe impl<T: Send> Sync for MpscQueueInner<T> {}

impl<T> HasLen for MpscQueueInner<T> {
    fn len(&self) -> usize {
        self.indices.len()
    }
}

impl<T> Resizable for MpscQueueInner<T> {
    fn resize(&mut self, new_len: usize) {
        let size = self.ring.len();

        assert!(new_len > self.len());
        assert!(new_len + size <= TAG_BIT, "Queue too large for system's atomic support");

        let mut len = self.indices.len();
        self.values.reserve_exact(new_len + size - len);
        self.indices.reserve_exact(new_len - len);
        while len < new_len {
            self.values.push(UnsafeCell::new(None));
            self.indices.push(UnsafeCell::new(size + len));
            len += 1;
        }
    }
}

fn next_cell(mut index: usize, size2: usize) -> usize {
    index += 1;
    if index >= WRAP_THRESHOLD {
        index = index % size2;
    }
    index
}

impl<T> MpscQueueInner<T> {
    pub fn new(size: usize, max_senders: usize) -> Self {
        let mut result = MpscQueueInner {
            values: Vec::with_capacity(max_senders+size),
            indices: Vec::with_capacity(max_senders),
            ring: Vec::with_capacity(size),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0)
        };
        for i in 0..size {
            result.values.push(UnsafeCell::new(None));
            result.ring.push(AtomicUsize::new(i));
        }
        result.resize(max_senders);
        result
    }

    pub unsafe fn push(&self, id: usize, value: T) -> Result<(), T> {
        let size = self.ring.len();
        let size2 = size*2;

        // Need the extra brackets to avoid compiler bug:
        // https://github.com/rust-lang/rust/issues/28935
        let ref mut index = *(&self.indices[id]).get();
        *(&self.values[*index]).get() = Some(value);

        loop {
            match self.tail.try_update_indirect(|tail| {
                let head = self.head.load(Ordering::Acquire);
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
                    let _ = self.tail.compare_exchange_weak(tail, next_cell(tail, size2), Ordering::Relaxed, Ordering::Relaxed);
                    Err(true)
                }
            }) {
                Ok((tail, prev_cell, _)) => {
                    // Update the tail pointer if necessary
                    while self.tail.compare_exchange_weak(tail, next_cell(tail, size2), Ordering::Relaxed, Ordering::Relaxed) == Err(tail) {}
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
        // Read value at head
        let cell = self.ring[head % size].load(Ordering::Acquire);
        if cell & TAG_BIT == 0 {
            Err(())
        } else {
            let result = (*(&self.values[cell & VALUE_MASK]).get()).take().expect("Tag bit must not be set for empty items");
            self.ring[head % size].store(cell.wrapping_add(TAG_BIT), Ordering::Release);
            self.head.store((head+1) % size2, Ordering::Release);
            Ok(result)
        }
    }
}

#[derive(Debug)]
pub struct MpscQueueReceiver<H: Handle>(H);

impl<T, H: Handle<Target=MpscQueueInner<T>>> MpscQueueReceiver<H> {
    pub fn new(size: usize, max_senders: usize) -> Self {
        MpscQueueReceiver(Handle::new(MpscQueueInner::new(size, max_senders)))
    }

    pub fn receive(&mut self) -> Result<T, ()> {
        // This is safe because we guarantee that we are unique
        self.0.with(|inner| unsafe { inner.pop() })
    }
}

pub type ResizingMpscQueueReceiver<T> = MpscQueueReceiver<ResizingHandle<MpscQueueInner<T>>>;
pub type BoundedMpscQueueReceiver<T> = MpscQueueReceiver<BoundedHandle<MpscQueueInner<T>>>;

#[derive(Debug, Clone)]
pub struct MpscQueueSender<H: Handle>(IdHandle<H>);

impl<T, H: Handle<Target=MpscQueueInner<T>>> MpscQueueSender<H> {
    pub fn new(receiver: &MpscQueueReceiver<H>) -> Self {
        MpscQueueSender(IdHandle::new(&receiver.0))
    }
    pub fn try_new(receiver: &MpscQueueReceiver<H>) -> Option<Self> {
        IdHandle::try_new(&receiver.0).map(MpscQueueSender)
    }

    pub fn send(&mut self, value: T) -> Result<(), T> {
        self.0.with(|inner, id| unsafe { inner.push(id, value) })
    }
    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(MpscQueueSender)
    }
}

pub type ResizingMpscQueueSender<T> = MpscQueueSender<ResizingHandle<MpscQueueInner<T>>>;
pub type BoundedMpscQueueSender<T> = MpscQueueSender<BoundedHandle<MpscQueueInner<T>>>;
