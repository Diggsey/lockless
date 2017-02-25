use std::sync::atomic::{AtomicUsize, Ordering};
use futures::task::{self, Task};
use futures::{StartSend, AsyncSink, Async, Sink, Stream, Poll};

use handle::{Handle, IdHandle, ResizingHandle, BoundedHandle, ContainerInner, Id, HandleInnerBase, HandleInner1, Tag0, HandleInner};
use primitives::atomic_ext::AtomicExt;
use primitives::index_allocator::IndexAllocator;
use primitives::invariant::Invariant;
use containers::atomic_cell_array::AtomicCellArrayInner;
use containers::mpsc_queue;


const CLOSE_FLAG: usize = !0 ^ (!0 >> 1);
const MSG_COUNT_MASK: usize = !CLOSE_FLAG;

// A sender ID is converted to an accessor ID by adding 1
#[derive(Debug, Copy, Clone)]
struct AccessorTag<SenderTag>(Invariant<SenderTag>);

impl<SenderTag> From<Id<SenderTag>> for Id<AccessorTag<SenderTag>> {
    fn from(id: Id<SenderTag>) -> Self {
        (Into::<usize>::into(id)+1).into()
    }
}

// The receiver ID is always 0
fn receiver_id<SenderTag>() -> Id<AccessorTag<SenderTag>> {
    0.into()
}

#[derive(Debug)]
pub struct MpscQueueInner<T, SenderTag> {
    // Stores the tasks of all senders and the receiver
    // Index 0 contains the receiver task
    // Index 1..(n+1) contains tasks for senders 0..n
    tasks: AtomicCellArrayInner<Option<Task>, AccessorTag<SenderTag>>,
    // Stores the message count in the low bits
    // If the high bit is set, the queue is closed
    msg_count: AtomicUsize,
    // Message queue
    msg_queue: mpsc_queue::MpscQueueInner<T, SenderTag>,
    // Parked thread queue
    parked_queue: mpsc_queue::MpscQueueInner<Id<SenderTag>, SenderTag>,
    // Buffer size
    buffer_size: usize,
    // Sender count
    sender_count: AtomicUsize
}

#[derive(Debug)]
pub struct SendError<T>(T);

impl<T> ContainerInner<SenderTag> for MpscQueueInner<T, SenderTag> {
    fn id_limit(&self) -> usize {
        self.msg_queue.id_limit()
    }

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

impl<T, SenderTag> MpscQueueInner<T, SenderTag> {
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

    unsafe fn inc_msg_count(&self, id: Id<SenderTag>) -> Result<bool, ()> {
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
            self.wake_task(receiver_id(), task_id.into());
        }
    }

    // Wake another parked task
    unsafe fn wake_task(&self, id: Id<AccessorTag<SenderTag>>, task_id: Id<AccessorTag<SenderTag>>) {
        if let Some(task) = self.tasks.swap(task_id.into(), id, None) {
            task.unpark();
        }
    }

    // Returns true if was already parked
    unsafe fn park_self(&self, id: Id<AccessorTag<SenderTag>>) -> bool {
        self.tasks.swap(id.into(), id, Some(task::park())).is_some()
    }

    // Undo a previous `park_self` operation
    unsafe fn unpark_self(&self, id: Id<AccessorTag<SenderTag>>) {
        self.tasks.swap(id.into(), id, None);
    }

    pub unsafe fn push(&self, id: Id<SenderTag>, mut value: T) -> StartSend<T, SendError<T>> {
        // Check if we're currently parked, while updating our task handle at the same time
        // ~fancy~ :P
        if self.park_self(id.into()) {
            return Ok(AsyncSink::NotReady(value));
        }

        // Increasing the message count may fail if we've been closed
        match self.inc_msg_count(id) {
            Ok(true) => {},
            Ok(false) => { self.unpark_self(id.into()); },
            Err(()) => return Err(SendError(value))
        }

        // We know this will succeed eventually
        while let Err(v) = self.msg_queue.push(id, value) {
            value = v;
        }

        // Wake the receiver if necessary
        self.wake_task(id.into(), receiver_id());
        
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
                self.park_self(receiver_id());
                // Check that queue is still empty
                match self.msg_queue.pop() {
                    Ok(value) => {
                        // Queue became non-empty
                        // Take an item and return
                        self.unpark_self(receiver_id());
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
            self.wake_task(receiver_id(), task_id.into());
        }
    }

    pub unsafe fn inc_sender_count(&self) {
        self.sender_count.fetch_add(1, Ordering::AcqRel);
    }

    pub unsafe fn dec_sender_count(&self, id: Id<SenderTag>) {
        if self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.wake_task(id.into(), receiver_id());
        }
    }
}

#[derive(Debug)]
pub struct MpscQueueReceiver<T, H: Handle, SenderTag>(H) where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>>;

impl<T, H: Handle, SenderTag> MpscQueueReceiver<T, H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> {
    pub fn new(size: usize, max_senders: usize) -> Self {
        MpscQueueReceiver(HandleInnerBase::new(MpscQueueInner::new(size, max_senders)))
    }

    pub fn close(&mut self) {
        // This is safe because we guarantee that we are unique
        self.0.with(|inner| unsafe { inner.close() })
    }
}

impl<T, H: Handle, SenderTag> Stream for MpscQueueReceiver<T, H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        // This is safe because we guarantee that we are unique
        Ok(self.0.with(|inner| unsafe { inner.pop() }))
    }
}

impl<T, H: Handle, SenderTag> Drop for MpscQueueReceiver<T, H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> {
    fn drop(&mut self) {
        // Drain the channel of all pending messages
        self.0.with(|inner| unsafe {
            inner.close();
            inner.drain();
        })
    }
}

type SenderTag = Tag0;
type Inner<T> = HandleInner1<SenderTag, IndexAllocator, MpscQueueInner<T, SenderTag>>;
pub type ResizingMpscQueueReceiver<T> = MpscQueueReceiver<T, ResizingHandle<Inner<T>>, SenderTag>;
pub type BoundedMpscQueueReceiver<T> = MpscQueueReceiver<T, BoundedHandle<Inner<T>>, SenderTag>;

#[derive(Debug)]
pub struct MpscQueueSender<T, H: Handle, SenderTag>(IdHandle<SenderTag, H>) where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> + HandleInner<SenderTag>;

impl<T, H: Handle, SenderTag> MpscQueueSender<T, H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> + HandleInner<SenderTag> {
    pub fn new(receiver: &MpscQueueReceiver<T, H, SenderTag>) -> Self {
        MpscQueueSender(IdHandle::new(&receiver.0)).inc_sender_count()
    }
    pub fn try_new(receiver: &MpscQueueReceiver<T, H, SenderTag>) -> Option<Self> {
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

impl<T, H: Handle, SenderTag> Clone for MpscQueueSender<T, H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> + HandleInner<SenderTag> {
    fn clone(&self) -> Self {
        MpscQueueSender(self.0.clone()).inc_sender_count()
    }
}

impl<T, H: Handle, SenderTag> Sink for MpscQueueSender<T, H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> + HandleInner<SenderTag> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, msg: T) -> StartSend<T, SendError<T>> {
        self.0.with_mut(|inner, id| unsafe { inner.push(id, msg) })
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
        Ok(Async::Ready(()))
    }
}

impl<T, H: Handle, SenderTag> Drop for MpscQueueSender<T, H, SenderTag> where H::HandleInner: HandleInnerBase<ContainerInner=MpscQueueInner<T, SenderTag>> + HandleInner<SenderTag> {
    fn drop(&mut self) {
        // Wake up the receiver
        self.0.with_mut(|inner, id| unsafe { inner.dec_sender_count(id) })
    }
}

pub type ResizingMpscQueueSender<T> = MpscQueueSender<T, ResizingHandle<Inner<T>>, SenderTag>;
pub type BoundedMpscQueueSender<T> = MpscQueueSender<T, BoundedHandle<Inner<T>>, SenderTag>;
