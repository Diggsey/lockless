use std::sync::atomic::{AtomicUsize, Ordering};
use futures::task::{self, Task};
use futures::{StartSend, AsyncSink, Async, Sink, Stream, Poll};

use handle::{Handle, IdHandle, ResizingHandle, BoundedHandle};
use primitives::atomic_ext::AtomicExt;
use containers::atomic_cell_array::AtomicCellArrayInner;
use containers::mpsc_queue;


const CLOSE_FLAG: usize = !0 ^ (!0 >> 1);
const MSG_COUNT_MASK: usize = !CLOSE_FLAG;

#[derive(Debug)]
pub struct MpscQueueInner<T> {
    // Stores the tasks of all senders and the receiver
    // Index 0 contains the receiver task
    // Index 1..(n+1) contains tasks for senders 0..n
    tasks: AtomicCellArrayInner<Option<Task>>,
    // Stores the message count in the low bits
    // If the high bit is set, the queue is closed
    msg_count: AtomicUsize,
    // Message queue
    msg_queue: mpsc_queue::MpscQueueInner<T>,
    // Parked thread queue
    parked_queue: mpsc_queue::MpscQueueInner<usize>,
    // Buffer size
    buffer_size: usize,
    // Sender count
    sender_count: AtomicUsize
}

#[derive(Debug)]
pub struct SendError<T>(T);

impl<T> IdLimit for MpscQueueInner<T> {
    fn id_limit(&self) -> usize {
        self.msg_queue.id_limit()
    }
}

impl<T> RaisableIdLimit for MpscQueueInner<T> {
    fn raise_id_limit(&mut self, new_limit: usize) {
        let old_limit = self.id_limit();
        assert!(new_limit > old_limit);
        let extra = new_limit - old_limit;
        
        // Reserve space for additional task handles
        self.tasks.reserve_exact(extra, extra);
        self.tasks.raise_id_limit(new_limit+1);
        for _ in 0..extra {
            self.tasks.push(None);
        }

        // Reserve space for additional messages
        self.msg_queue.raise_id_limit(new_limit);
        self.msg_queue.resize(self.buffer_size + new_limit);

        // Reserve space for additional parked tasks
        self.parked_queue.raise_id_limit(new_limit);
        self.parked_queue.resize(new_limit);
    }
}

impl<T> MpscQueueInner<T> {
    pub fn new(size: usize, max_senders: usize) -> Self {
        assert!(max_senders > 0);
        let mut result = MpscQueueInner {
            tasks: AtomicCellArrayInner::with_capacity(max_senders+1, max_senders+1),
            msg_count: AtomicUsize::new(0),
            msg_queue: mpsc_queue::MpscQueueInner::new(size + max_senders, max_senders),
            parked_queue: mpsc_queue::MpscQueueInner::new(max_senders, max_senders),
            buffer_size: size,
            sender_count: AtomicUsize::new(0)
        };
        for _ in 0..(max_senders+1) {
            result.tasks.push(None);
        }
        result
    }

    unsafe fn inc_msg_count(&self, id: usize) -> Result<bool, ()> {
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
                    while self.parked_queue.push(id, id).is_err() {}
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
        if let Ok(task_id) = self.parked_queue.pop() {
            self.wake_task(0, task_id+1);
        }
    }

    unsafe fn wake_task(&self, id: usize, task_id: usize) {
        if let Some(task) = self.tasks.swap(task_id, id, None) {
            task.unpark();
        }
    }

    pub unsafe fn push(&self, id: usize, mut value: T) -> StartSend<T, SendError<T>> {
        // Check if we're currently parked, while updating our task handle at the same time
        // ~fancy~ :P
        if let Some(_) = self.tasks.swap(id+1, id+1, Some(task::park())) {
            return Ok(AsyncSink::NotReady(value));
        }

        // Increasing the message count may fail if we've been closed
        match self.inc_msg_count(id) {
            Ok(true) => {},
            Ok(false) => { self.tasks.swap(id+1, id+1, None); },
            Err(()) => return Err(SendError(value))
        }

        // We know this will succeed eventually
        while let Err(v) = self.msg_queue.push(id, value) {
            value = v;
        }

        // Wake the receiver if necessary
        self.wake_task(id+1, 0);
        
        // All done
        Ok(AsyncSink::Ready)
    }

    unsafe fn drain(&self) {
        while self.msg_queue.pop().is_ok() {
            self.dec_msg_count();
        }
    }

    pub unsafe fn pop(&self) -> Async<Option<T>> {
        match self.msg_queue.pop() {
            Ok(value) => {
                self.dec_msg_count();
                Async::Ready(Some(value))
            },
            Err(()) => {
                // Park ourselves
                self.tasks.swap(0, 0, Some(task::park()));
                // Check that queue is still empty
                match self.msg_queue.pop() {
                    Ok(value) => {
                        // Queue became non-empty
                        // Take an item and return
                        self.tasks.swap(0, 0, None);
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
        while let Ok(task_id) = self.parked_queue.pop() {
            self.wake_task(0, task_id+1);
        }
    }

    pub unsafe fn inc_sender_count(&self) {
        self.sender_count.fetch_add(1, Ordering::AcqRel);
    }

    pub unsafe fn dec_sender_count(&self, id: usize) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.wake_task(id+1, 0);
        }
    }
}

#[derive(Debug)]
pub struct MpscQueueReceiver<T, H: Handle<HandleInner=MpscQueueInner<T>>>(H);

impl<T, H: Handle<HandleInner=MpscQueueInner<T>>> MpscQueueReceiver<T, H> {
    pub fn new(size: usize, max_senders: usize) -> Self {
        MpscQueueReceiver(Handle::new(MpscQueueInner::new(size, max_senders)))
    }

    pub fn close(&mut self) {
        // This is safe because we guarantee that we are unique
        self.0.with(|inner| unsafe { inner.close() })
    }
}

impl<T, H: Handle<HandleInner=MpscQueueInner<T>>> Stream for MpscQueueReceiver<T, H> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        // This is safe because we guarantee that we are unique
        Ok(self.0.with(|inner| unsafe { inner.pop() }))
    }
}

impl<T, H: Handle<HandleInner=MpscQueueInner<T>>> Drop for MpscQueueReceiver<T, H> {
    fn drop(&mut self) {
        // Drain the channel of all pending messages
        self.0.with(|inner| unsafe {
            inner.close();
            inner.drain();
        })
    }
}

pub type ResizingMpscQueueReceiver<T> = MpscQueueReceiver<T, ResizingHandle<MpscQueueInner<T>>>;
pub type BoundedMpscQueueReceiver<T> = MpscQueueReceiver<T, BoundedHandle<MpscQueueInner<T>>>;

#[derive(Debug, Clone)]
pub struct SenderTag;

#[derive(Debug)]
pub struct MpscQueueSender<T, H: Handle<HandleInner=MpscQueueInner<T>>>(IdHandle<SenderTag, H>);

impl<T, H: Handle<HandleInner=MpscQueueInner<T>>> MpscQueueSender<T, H> {
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

impl<T, H: Handle<HandleInner=MpscQueueInner<T>>> Clone for MpscQueueSender<T, H> {
    fn clone(&self) -> Self {
        MpscQueueSender(self.0.clone()).inc_sender_count()
    }
}

impl<T, H: Handle<HandleInner=MpscQueueInner<T>>> Sink for MpscQueueSender<T, H> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        self.0.with_mut(|inner, id| unsafe { inner.push(id, msg) })
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<T, H: Handle<HandleInner=MpscQueueInner<T>>> Drop for MpscQueueSender<T, H> {
    fn drop(&mut self) {
        // Wake up the receiver
        self.0.with_mut(|inner, id| unsafe { inner.dec_sender_count(id) })
    }
}

pub type ResizingMpscQueueSender<T> = MpscQueueSender<T, ResizingHandle<MpscQueueInner<T>>>;
pub type BoundedMpscQueueSender<T> = MpscQueueSender<T, BoundedHandle<MpscQueueInner<T>>>;
