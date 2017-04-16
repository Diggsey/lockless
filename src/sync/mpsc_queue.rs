use std::sync::atomic::{AtomicUsize, Ordering};
use std::borrow::Borrow;
use futures::task::{self, Task};
use futures::{StartSend, AsyncSink, Async, Sink, Stream, Poll};

use handle::{Handle, IdHandle, ResizingHandle, BoundedHandle, HandleInner, Like};
use primitives::atomic_ext::AtomicExt;
use primitives::index_allocator::IndexAllocator;
use containers::atomic_cell_array::AtomicCellArrayInner;
use containers::mpsc_queue;
use containers::storage::{Place, Storage};
use containers::scratch::Scratch;


const CLOSE_FLAG: usize = !0 ^ (!0 >> 1);
const MSG_COUNT_MASK: usize = !CLOSE_FLAG;


define_id!(MpscQueueSenderId);
define_id!(MpscQueueAccessorId);

fn with_sender_id<R, F: FnOnce(&mut MpscQueueAccessorId) -> R>(sender_id: &mut MpscQueueSenderId, f: F) -> R {
    <MpscQueueAccessorId as Like<usize>>::virtual_borrow(*Borrow::<usize>::borrow(sender_id) + 1, f)
}
unsafe fn with_receiver_id<R, F: FnOnce(&mut MpscQueueAccessorId) -> R>(f: F) -> R {
    <MpscQueueAccessorId as Like<usize>>::virtual_borrow(0, f)
}
fn sender_index(sender_id: &mut MpscQueueSenderId) -> usize {
    with_sender_id(sender_id, |id| *(*id).borrow())
}
fn receiver_index() -> usize {
    unsafe { with_receiver_id(|id| *(*id).borrow()) }
}

#[derive(Debug)]
pub struct MpscQueueWrapper<T> {
    // Stores the tasks of all senders and the receiver
    // Index 0 contains the receiver task
    // Index 1..(n+1) contains tasks for senders 0..n
    task_storage: Storage<Task>,
    task_scratch: Scratch<MpscQueueAccessorId, Place<Task>>,
    task_array: AtomicCellArrayInner<Place<Task>>,
    // Stores the message count in the low bits
    // If the high bit is set, the queue is closed
    msg_count: AtomicUsize,
    // Message queue
    msg_storage: Storage<T>,
    msg_scratch: Scratch<MpscQueueSenderId, Place<T>>,
    msg_queue: mpsc_queue::MpscQueueInner<Place<T>>,
    // Parked thread queue
    parked_queue: mpsc_queue::MpscQueueInner<usize>,
    // Buffer size
    buffer_size: usize,
    // Sender count
    sender_count: AtomicUsize,
    // Id allocator
    id_alloc: IndexAllocator,
}

#[derive(Debug)]
pub struct SendError<T>(T);

impl<T> HandleInner<MpscQueueSenderId> for MpscQueueWrapper<T> {
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
        self.parked_queue.extend((0..extra).map(|_| 0));

        // Extra IDs
        self.id_alloc.resize(new_limit);
    }
}

impl<T> MpscQueueWrapper<T> {
    pub fn new<H: Handle<HandleInner=Self>>(max_senders: usize, size: usize) -> H {
        assert!(max_senders > 0);
        // Need capacity for both scratch space and task array
        let mut task_storage = Storage::with_capacity(max_senders*2+2);
        let task_scratch = Scratch::new(task_storage.none_storing_iter(max_senders+1));
        let task_array = AtomicCellArrayInner::new(task_storage.none_storing_iter(max_senders+1));

        // Need capacity for both scratch space and message queue
        let mut msg_storage = Storage::with_capacity(max_senders*2 + size);
        let msg_scratch = Scratch::new(msg_storage.none_storing_iter(max_senders));
        let msg_queue = mpsc_queue::MpscQueueInner::new(msg_storage.none_storing_iter(max_senders + size));

        // Needs space for every sender
        let parked_queue = mpsc_queue::MpscQueueInner::new((0..max_senders).map(|_| 0));

        let id_alloc = IndexAllocator::new(max_senders);

        Handle::new(MpscQueueWrapper {
            task_storage: task_storage,
            task_scratch: task_scratch,
            task_array: task_array,
            msg_count: AtomicUsize::new(0),
            msg_storage: msg_storage,
            msg_scratch: msg_scratch,
            msg_queue: msg_queue,
            parked_queue: parked_queue,
            buffer_size: size,
            sender_count: AtomicUsize::new(0),
            id_alloc: id_alloc,
        })
    }

    unsafe fn inc_msg_count(&self, id: &mut MpscQueueSenderId) -> Result<bool, ()> {
        // Try increasing the message count
        match self.msg_count.try_update(|prev| {
            if prev & CLOSE_FLAG == 0 {
                Ok(prev+1)
            } else {
                Err(())
            }
        }) {
            Ok((prev, _)) => {
                if (prev & MSG_COUNT_MASK) >= self.buffer_size {
                    let mut index = sender_index(id);
                    while !self.parked_queue.push(&mut index) {}
                    
                    // Make sure queue was not closed
                    if self.msg_count.load(Ordering::SeqCst) & CLOSE_FLAG == 0 {
                        // Queue was not closed, park was successful
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    Ok(false)
                }
            },
            Err(()) => Err(())
        }
    }

    unsafe fn dec_msg_count(&self) {
        // Decrease the message count
        self.msg_count.fetch_sub(1, Ordering::SeqCst);

        // Wake a task if necessary
        let _ = self.parked_queue.pop(|task_id| {
            with_receiver_id(|id| self.wake_task(id, *task_id));
        });
    }

    // Wake another parked task
    unsafe fn wake_task(&self, id: &mut MpscQueueAccessorId, task_id: usize) {
        let place = self.task_scratch.get_mut(id);
        self.task_array.swap(task_id, place);
        if let Some(task) = self.task_storage.replace(place, None) {
            task.unpark();
        }
    }

    // Returns true if was already parked
    unsafe fn park_self(&self, id: &mut MpscQueueAccessorId) -> bool {
        let task_id = *(*id).borrow();
        let place = self.task_scratch.get_mut(id);
        self.task_storage.replace(place, Some(task::park()));
        self.task_array.swap(task_id, place);
        self.task_storage.replace(place, None).is_some()
    }

    // Undo a previous `park_self` operation
    unsafe fn unpark_self(&self, id: &mut MpscQueueAccessorId) {
        let task_id = *(*id).borrow();
        let place = self.task_scratch.get_mut(id);
        self.task_array.swap(task_id, place);
        self.task_storage.replace(place, None);
    }

    pub unsafe fn push(&self, id: &mut MpscQueueSenderId, value: T) -> StartSend<T, SendError<T>> {
        // Check if we're currently parked, while updating our task handle at the same time
        // ~fancy~ :P
        if with_sender_id(id, |accessor_id| self.park_self(accessor_id)) {
            return Ok(AsyncSink::NotReady(value));
        }

        // Increasing the message count may fail if we've been closed
        match self.inc_msg_count(id) {
            Ok(true) => {},
            Ok(false) => { with_sender_id(id, |accessor_id| self.unpark_self(accessor_id)); },
            Err(()) => return Err(SendError(value))
        }

        // Put our value into the scratch space
        let place = self.msg_scratch.get_mut(id);
        self.msg_storage.replace(place, Some(value));
        // We know this will succeed eventually, because we managed to increment the message count
        while !self.msg_queue.push(place) {}

        // Wake the receiver if necessary
        with_sender_id(id, |accessor_id| self.wake_task(accessor_id, receiver_index()));
        
        // All done
        Ok(AsyncSink::Ready)
    }

    unsafe fn pop_inner(&self) -> Result<T, ()> {
        self.msg_queue.pop(|place| {
            self.msg_storage.replace(place, None).expect("Some(value)")
        })
    }

    unsafe fn drain(&self) {
        while self.pop_inner().is_ok() {
            self.dec_msg_count();
        }
    }

    pub unsafe fn pop(&self) -> Async<Option<T>> {
        match self.pop_inner() {
            Ok(value) => {
                self.dec_msg_count();
                Async::Ready(Some(value))
            },
            Err(()) => {
                // Park ourselves
                with_receiver_id(|id| self.park_self(id));
                // Check that queue is still empty
                match self.pop_inner() {
                    Ok(value) => {
                        // Queue became non-empty
                        // Take an item and return
                        with_receiver_id(|id| self.unpark_self(id));
                        self.dec_msg_count();
                        Async::Ready(Some(value))
                    },
                    Err(()) => {
                        // Queue is still empty, if it's closed and there are no remaining message
                        // then we're done!
                        if self.msg_count.load(Ordering::SeqCst) == CLOSE_FLAG || self.sender_count.load(Ordering::SeqCst) == 0 {
                            Async::Ready(None)
                        } else {
                            Async::NotReady
                        }
                    }
                }
            }
        }
    }

    pub unsafe fn close(&self) {
        // Mark ourselves as closed
        self.msg_count.fetch_or(CLOSE_FLAG, Ordering::SeqCst);

        // Wake any waiting tasks
        while let Ok(task_index) = self.parked_queue.pop(|index| *index) {
            with_receiver_id(|id| self.wake_task(id, task_index));
        }
    }

    pub unsafe fn inc_sender_count(&self) {
        self.sender_count.fetch_add(1, Ordering::AcqRel);
    }

    pub unsafe fn dec_sender_count(&self, id: &mut MpscQueueSenderId) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            with_sender_id(id, |accessor_id| self.wake_task(accessor_id, receiver_index()));
        }
    }
}

#[derive(Debug)]
pub struct MpscQueueReceiver<T, H: Handle<HandleInner=MpscQueueWrapper<T>>>(H);

impl<T, H: Handle<HandleInner=MpscQueueWrapper<T>>> MpscQueueReceiver<T, H> {
    pub fn new(max_senders: usize, size: usize) -> Self {
        MpscQueueReceiver(MpscQueueWrapper::new(max_senders, size))
    }

    pub fn close(&mut self) {
        // This is safe because we guarantee that we are unique
        self.0.with(|inner| unsafe { inner.close() })
    }
}

impl<T, H: Handle<HandleInner=MpscQueueWrapper<T>>> Stream for MpscQueueReceiver<T, H> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        // This is safe because we guarantee that we are unique
        Ok(self.0.with(|inner| unsafe { inner.pop() }))
    }
}

impl<T, H: Handle<HandleInner=MpscQueueWrapper<T>>> Drop for MpscQueueReceiver<T, H> {
    fn drop(&mut self) {
        // Drain the channel of all pending messages
        self.0.with(|inner| unsafe {
            inner.close();
            inner.drain();
        })
    }
}

pub type ResizingMpscQueueReceiver<T> = MpscQueueReceiver<T, ResizingHandle<MpscQueueWrapper<T>>>;
pub type BoundedMpscQueueReceiver<T> = MpscQueueReceiver<T, BoundedHandle<MpscQueueWrapper<T>>>;

#[derive(Debug)]
pub struct MpscQueueSender<T, H: Handle<HandleInner=MpscQueueWrapper<T>>>(IdHandle<H, MpscQueueSenderId>);

impl<T, H: Handle<HandleInner=MpscQueueWrapper<T>>> MpscQueueSender<T, H> {
    pub fn new(receiver: &MpscQueueReceiver<T, H>) -> Self {
        MpscQueueSender(IdHandle::new(&receiver.0)).inc_sender_count()
    }
    pub fn try_new(receiver: &MpscQueueReceiver<T, H>) -> Option<Self> {
        IdHandle::try_new(&receiver.0).map(|inner| MpscQueueSender(inner).inc_sender_count())
    }
    pub fn try_clone(&self) -> Option<Self> {
        self.0.try_clone().map(|inner| MpscQueueSender(inner).inc_sender_count())
    }
    fn inc_sender_count(self) -> Self {
        self.0.with(|inner| unsafe { inner.inc_sender_count() });
        self
    }
}

impl<T, H: Handle<HandleInner=MpscQueueWrapper<T>>> Clone for MpscQueueSender<T, H> {
    fn clone(&self) -> Self {
        MpscQueueSender(self.0.clone()).inc_sender_count()
    }
}

impl<T, H: Handle<HandleInner=MpscQueueWrapper<T>>> Sink for MpscQueueSender<T, H> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        self.0.with_mut(|inner, id| unsafe { inner.push(id, msg) })
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<T, H: Handle<HandleInner=MpscQueueWrapper<T>>> Drop for MpscQueueSender<T, H> {
    fn drop(&mut self) {
        // Wake up the receiver
        self.0.with_mut(|inner, id| unsafe { inner.dec_sender_count(id) })
    }
}

pub type ResizingMpscQueueSender<T> = MpscQueueSender<T, ResizingHandle<MpscQueueWrapper<T>>>;
pub type BoundedMpscQueueSender<T> = MpscQueueSender<T, BoundedHandle<MpscQueueWrapper<T>>>;
