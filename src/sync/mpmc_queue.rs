use std::sync::atomic::{AtomicUsize, Ordering};
use std::borrow::Borrow;
use std::mem;
use futures::task::{self, Task};
use futures::{StartSend, AsyncSink, Async, Sink, Stream, Poll};

use handle::{Handle, IdHandle, ResizingHandle, BoundedHandle, HandleInner};
use primitives::index_allocator::IndexAllocator;
use containers::atomic_cell_array::AtomicCellArrayInner;
use containers::mpmc_queue;
use containers::storage::{Place, Storage};
use containers::scratch::Scratch;


const CLOSE_FLAG: usize = !0 ^ (!0 >> 1);
const MSG_COUNT_MASK: usize = !CLOSE_FLAG;
const MSG_COUNT_ZERO: usize = CLOSE_FLAG >> 1;


define_id!(MpmcQueueAccessorId);

#[derive(Debug)]
pub struct MpmcQueueWrapper<T> {
    // Stores the tasks of all accessors
    task_storage: Storage<Task>,
    task_scratch: Scratch<MpmcQueueAccessorId, Place<Task>>,
    task_array: AtomicCellArrayInner<Place<Task>>,
    // Stores the message count + MSG_COUNT_ZERO in the low bits
    // May go "negative" (less than MSG_COUNT_ZERO) if there are outstanding pop operations
    // If high bit is set, the queue is closed
    // These are upper and lower bounds
    msg_count_upper: AtomicUsize,
    msg_count_lower: AtomicUsize,
    // Message queue
    msg_storage: Storage<T>,
    msg_scratch: Scratch<MpmcQueueAccessorId, Place<T>>,
    msg_queue: mpmc_queue::MpmcQueueInner<Place<T>>,
    // Parked thread queue
    parked_sender_queue: mpmc_queue::MpmcQueueInner<usize>,
    parked_receiver_queue: mpmc_queue::MpmcQueueInner<usize>,
    pending_receive_flags: Scratch<MpmcQueueAccessorId, usize>,
    // Buffer size
    buffer_size: usize,
    // Sender count
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
    // Id allocator
    id_alloc: IndexAllocator,
}

#[derive(Debug)]
pub struct SendError<T>(T);

impl<T> HandleInner<MpmcQueueAccessorId> for MpmcQueueWrapper<T> {
    type IdAllocator = IndexAllocator;

    fn id_allocator(&self) -> &IndexAllocator {
        &self.id_alloc
    }

    fn raise_id_limit(&mut self, new_limit: usize) {
        let old_limit = self.id_limit();
        assert!(new_limit > old_limit);
        let extra = new_limit - old_limit;
        
        // Reserve space for additional task handles
        self.task_storage.reserve(extra*2);
        self.task_scratch.extend(self.task_storage.none_storing_iter(extra));
        self.task_array.extend(self.task_storage.none_storing_iter(extra));

        // Reserve space for additional messages
        self.msg_storage.reserve(extra*2);
        self.msg_scratch.extend(self.msg_storage.none_storing_iter(extra));
        self.msg_queue.extend(self.msg_storage.none_storing_iter(extra));

        // Reserve space for additional parked tasks
        self.parked_sender_queue.extend((0..extra).map(|_| 0));
        self.parked_receiver_queue.extend((0..extra).map(|_| 0));
        self.pending_receive_flags.extend((0..extra).map(|_| 0));

        // Extra IDs
        self.id_alloc.resize(new_limit);
    }
}

impl<T> MpmcQueueWrapper<T> {
    pub fn new<H: Handle<HandleInner=Self>>(max_accessors: usize, size: usize) -> H {
        assert!(max_accessors > 0);
        // Need capacity for both scratch space and task array
        let mut task_storage = Storage::with_capacity(max_accessors*2);
        let task_scratch = Scratch::new(task_storage.none_storing_iter(max_accessors));
        let task_array = AtomicCellArrayInner::new(task_storage.none_storing_iter(max_accessors));

        // Need capacity for both scratch space and message queue
        let mut msg_storage = Storage::with_capacity(max_accessors*2 + size);
        let msg_scratch = Scratch::new(msg_storage.none_storing_iter(max_accessors));
        let msg_queue = mpmc_queue::MpmcQueueInner::new(msg_storage.none_storing_iter(max_accessors + size));

        // Needs space for every sender
        let parked_sender_queue = mpmc_queue::MpmcQueueInner::new((0..max_accessors).map(|_| 0));
        let parked_receiver_queue = mpmc_queue::MpmcQueueInner::new((0..max_accessors).map(|_| 0));
        let pending_receive_flags = Scratch::new((0..max_accessors).map(|_| 0));

        let id_alloc = IndexAllocator::new(max_accessors);

        Handle::new(MpmcQueueWrapper {
            task_storage: task_storage,
            task_scratch: task_scratch,
            task_array: task_array,
            msg_count_upper: AtomicUsize::new(MSG_COUNT_ZERO),
            msg_count_lower: AtomicUsize::new(MSG_COUNT_ZERO),
            msg_storage: msg_storage,
            msg_scratch: msg_scratch,
            msg_queue: msg_queue,
            parked_sender_queue: parked_sender_queue,
            parked_receiver_queue: parked_receiver_queue,
            pending_receive_flags: pending_receive_flags,
            buffer_size: size,
            sender_count: AtomicUsize::new(0),
            receiver_count: AtomicUsize::new(0),
            id_alloc: id_alloc,
        })
    }

    unsafe fn inc_msg_count_upper(&self, id: &mut MpmcQueueAccessorId) -> Result<bool, ()> {
        // Try increasing the message count
        let prev = self.msg_count_upper.fetch_add(1, Ordering::AcqRel);
        if prev & CLOSE_FLAG != 0 {
            self.msg_count_lower.fetch_sub(1, Ordering::AcqRel);
            Err(())
        } else if (prev & MSG_COUNT_MASK) < self.buffer_size + MSG_COUNT_ZERO {
            Ok(false)
        } else {
            let mut index = *(*id).borrow();
            while !self.parked_sender_queue.push(&mut index) {}
            // Make sure queue was not closed
            if self.msg_count_upper.load(Ordering::SeqCst) & CLOSE_FLAG == 0 {
                // Queue was not closed, park was successful
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    unsafe fn inc_msg_count_lower(&self, id: &mut MpmcQueueAccessorId) {
        self.msg_count_lower.fetch_add(1, Ordering::AcqRel);

        // Wake a receiver if necessary
        let mut index = 0;
        if self.parked_receiver_queue.pop(&mut index) {
            self.wake_task(id, index);
        }
    }

    unsafe fn dec_msg_count_lower(&self, id: &mut MpmcQueueAccessorId) -> bool {
        let prev = self.msg_count_lower.fetch_sub(1, Ordering::AcqRel);
        if prev & CLOSE_FLAG != 0 {
            self.msg_count_lower.fetch_add(1, Ordering::AcqRel);
            true
        } else if (prev & MSG_COUNT_MASK) > MSG_COUNT_ZERO {
            true
        } else {
            let mut index = *(*id).borrow();
            while !self.parked_receiver_queue.push(&mut index) {}
            // Make sure queue was not closed
            if self.msg_count_upper.load(Ordering::SeqCst) & CLOSE_FLAG == 0 {
                // Queue was not closed, park was successful
                false
            } else {
                true
            }
        }
    }

    unsafe fn dec_msg_count_upper(&self, id: &mut MpmcQueueAccessorId) {
        self.msg_count_upper.fetch_sub(1, Ordering::AcqRel);

        // Wake a sender if necessary
        let mut index = 0;
        if self.parked_sender_queue.pop(&mut index) {
            self.wake_task(id, index);
        }
    }

    // Wake another parked task
    unsafe fn wake_task(&self, id: &mut MpmcQueueAccessorId, task_id: usize) {
        let place = self.task_scratch.get_mut(id);
        self.task_array.swap(task_id, place);
        if let Some(task) = self.task_storage.replace(place, None) {
            task.unpark();
        }
    }

    // Returns true if was already parked
    unsafe fn park_self(&self, id: &mut MpmcQueueAccessorId) -> bool {
        let task_id = *(*id).borrow();
        let place = self.task_scratch.get_mut(id);
        self.task_storage.replace(place, Some(task::park()));
        self.task_array.swap(task_id, place);
        self.task_storage.replace(place, None).is_some()
    }

    // Undo a previous `park_self` operation
    unsafe fn unpark_self(&self, id: &mut MpmcQueueAccessorId) {
        let task_id = *(*id).borrow();
        let place = self.task_scratch.get_mut(id);
        self.task_array.swap(task_id, place);
        self.task_storage.replace(place, None);
    }

    pub unsafe fn push(&self, id: &mut MpmcQueueAccessorId, value: T) -> StartSend<T, SendError<T>> {
        // Check if we're currently parked, while updating our task handle at the same time
        // ~fancy~ :P
        if self.park_self(id) {
            return Ok(AsyncSink::NotReady(value));
        }

        // Increasing the message count may fail if we've been closed
        match self.inc_msg_count_upper(id) {
            Ok(true) => {},
            Ok(false) => { self.unpark_self(id); },
            Err(()) => return Err(SendError(value))
        }

        // Put our value into the scratch space
        let place = self.msg_scratch.get_mut(id);
        self.msg_storage.replace(place, Some(value));
        // We know this will succeed eventually, because we managed to increment the message count
        while !self.msg_queue.push(place) {}

        // Increase the message count lower bound
        self.inc_msg_count_lower(id);
        
        // All done
        Ok(AsyncSink::Ready)
    }

    // Must not be called while there are any remaining receivers
    unsafe fn drain(&self, id: &mut MpmcQueueAccessorId) {
        while self.pop_inner(id).is_some() {}
    }

    unsafe fn pop_inner(&self, id: &mut MpmcQueueAccessorId) -> Option<T> {
        // Get a place to exchange for the result
        let place = self.msg_scratch.get_mut(id);
        if self.msg_queue.pop(place) {
            // Decrease the message count upper bound
            self.dec_msg_count_upper(id);
            let result = self.msg_storage.replace(place, None).expect("Some(value)");
            Some(result)
        } else {
            None
        }
    }

    pub unsafe fn pop(&self, id: &mut MpmcQueueAccessorId) -> Async<Option<T>> {
        // Check if we're currently parked, while updating our task handle at the same time
        // ~fancy~ :P
        if self.park_self(id) {
            return Async::NotReady;
        }

        // If a poll hasn't been started yet
        if mem::replace(self.pending_receive_flags.get_mut(id), 0) == 0 {
            // Decreasing the message count returns true if a message is ready
            if !self.dec_msg_count_lower(id) {
                *self.pending_receive_flags.get_mut(id) = 1;
                return Async::NotReady
            }
        }
        self.unpark_self(id);

        // This may fail if no messages are available
        match self.pop_inner(id) {
            Some(value) => Async::Ready(Some(value)),
            None => Async::Ready(None),
        }
    }

    pub unsafe fn close(&self, id: &mut MpmcQueueAccessorId) {
        // Mark ourselves as closed
        self.msg_count_upper.fetch_or(CLOSE_FLAG, Ordering::SeqCst);
        self.msg_count_lower.fetch_or(CLOSE_FLAG, Ordering::SeqCst);

        // Wake any waiting tasks
        let mut index = 0;
        while self.parked_sender_queue.pop(&mut index) {
            self.wake_task(id, index);
        }
        while self.parked_receiver_queue.pop(&mut index) {
            self.wake_task(id, index);
        }
    }

    pub unsafe fn inc_sender_count(&self) {
        self.sender_count.fetch_add(1, Ordering::AcqRel);
    }

    pub unsafe fn dec_sender_count(&self, id: &mut MpmcQueueAccessorId) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.close(id);
        }
    }

    pub unsafe fn inc_receiver_count(&self) {
        self.receiver_count.fetch_add(1, Ordering::AcqRel);
    }

    pub unsafe fn dec_receiver_count(&self, id: &mut MpmcQueueAccessorId) {
        if self.receiver_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.close(id);
            self.drain(id);
        }
    }
}

#[derive(Debug)]
pub struct MpmcQueueReceiver<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>>(IdHandle<H, MpmcQueueAccessorId>);

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> MpmcQueueReceiver<T, H> {
    fn inc_receiver_count(self) -> Self {
        self.0.with(|inner| unsafe { inner.inc_receiver_count() });
        self
    }
    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(|inner| MpmcQueueReceiver(inner).inc_receiver_count())
    }
    pub fn close(&mut self) {
        self.0.with_mut(|inner, id| unsafe { inner.close(id) })
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Stream for MpmcQueueReceiver<T, H> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        // This is safe because we guarantee that we are unique
        Ok(self.0.with_mut(|inner, id| unsafe { inner.pop(id) }))
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Clone for MpmcQueueReceiver<T, H> {
    fn clone(&self) -> Self {
        MpmcQueueReceiver(self.0.clone()).inc_receiver_count()
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Drop for MpmcQueueReceiver<T, H> {
    fn drop(&mut self) {
        self.0.with_mut(|inner, id| unsafe { inner.dec_receiver_count(id) })
    }
}

pub type ResizingMpmcQueueReceiver<T> = MpmcQueueReceiver<T, ResizingHandle<MpmcQueueWrapper<T>>>;
pub type BoundedMpmcQueueReceiver<T> = MpmcQueueReceiver<T, BoundedHandle<MpmcQueueWrapper<T>>>;

#[derive(Debug)]
pub struct MpmcQueueSender<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>>(IdHandle<H, MpmcQueueAccessorId>);

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> MpmcQueueSender<T, H> {
    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(|inner| MpmcQueueSender(inner).inc_sender_count())
    }
    fn inc_sender_count(self) -> Self {
        self.0.with(|inner| unsafe { inner.inc_sender_count() });
        self
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Clone for MpmcQueueSender<T, H> {
    fn clone(&self) -> Self {
        MpmcQueueSender(self.0.clone()).inc_sender_count()
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Sink for MpmcQueueSender<T, H> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        self.0.with_mut(|inner, id| unsafe { inner.push(id, msg) })
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>> Drop for MpmcQueueSender<T, H> {
    fn drop(&mut self) {
        // Wake up the receiver
        self.0.with_mut(|inner, id| unsafe { inner.dec_sender_count(id) })
    }
}

pub type ResizingMpmcQueueSender<T> = MpmcQueueSender<T, ResizingHandle<MpmcQueueWrapper<T>>>;
pub type BoundedMpmcQueueSender<T> = MpmcQueueSender<T, BoundedHandle<MpmcQueueWrapper<T>>>;

pub fn new<T, H: Handle<HandleInner=MpmcQueueWrapper<T>>>(max_accessors: usize, size: usize) -> (MpmcQueueSender<T, H>, MpmcQueueReceiver<T, H>) {
    let inner = MpmcQueueWrapper::new(max_accessors, size);
    (MpmcQueueSender(IdHandle::new(&inner)).inc_sender_count(), MpmcQueueReceiver(IdHandle::new(&inner)).inc_receiver_count())
}
