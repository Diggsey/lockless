use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;

use handle::{HasLen, Resizable, Handle, IdHandle, ResizingHandle, BoundedHandle};
use primitives::atomic_ext::AtomicExt;

// Pointers are only wrapped to 2*Capacity, so must wrap before indexing!
//  ___________________
// |___|_X_|_X_|___|___|
//       ^       ^
//       H       T
//
// (H == T) => Empty
// (H != T) && (H%C == T%C) => Full

const EMPTY_BIT: usize = !0 ^ (!0 >> 1);

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
        assert!(new_len > self.len());

        let size = self.ring.len();
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
            result.ring.push(AtomicUsize::new(i | EMPTY_BIT));
        }
        result.resize(max_senders);
        result
    }

    pub unsafe fn push(&self, id: usize, value: T) -> Result<(), T> {
        let size = self.ring.len();
        let size2 = size*2;
        let head = self.head.load(Ordering::Relaxed);
        // Try reserving space
        if let Ok((tail, _)) = self.tail.try_update(|tail| {
            // If not full
            if tail != (head + size) % size2 {
                Ok((tail+1) % size2)
            } else {
                Err(())
            }
        }) {
            // Need the extra brackets to avoid compiler bug:
            // https://github.com/rust-lang/rust/issues/28935
            let ref mut index = *(&self.indices[id]).get();
            *(&self.values[*index]).get() = Some(value);
            *index = self.ring[tail % size].swap(*index, Ordering::AcqRel) ^ EMPTY_BIT;
            Ok(())
        } else {
            Err(value)
        }
    }

    pub unsafe fn pop(&self) -> Result<T, ()> {
        let size = self.ring.len();
        let size2 = size*2;
        let head = self.head.load(Ordering::Relaxed);
        // Read value at head
        let index = self.ring[head % size].fetch_or(EMPTY_BIT, Ordering::Acquire);
        if index & EMPTY_BIT == 0 {
            let result = (*(&self.values[index]).get()).take().expect("Empty bit must be set for empty items");
            self.head.store((head+1) % size2, Ordering::Release);
            Ok(result)
        } else {
            Err(())
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
