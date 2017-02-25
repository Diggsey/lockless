use std::sync::Arc;

use super::{HandleInnerBase, HandleInner, Handle, IdHandle, Id, IdAllocator};

/// Implementation of Handle which panics if it runs out of IDs
#[derive(Debug)]
pub struct BoundedHandle<H> {
    inner: Arc<H>
}

unsafe impl<H: HandleInnerBase> Handle for BoundedHandle<H> {
    type HandleInner = H;

    fn try_allocate_id<Tag>(&self) -> Option<Id<Tag>> where Self::HandleInner: HandleInner<Tag> {
        self.inner.id_allocator().try_allocate_id()
    }

    fn free_id<Tag>(&self, id: Id<Tag>) where Self::HandleInner: HandleInner<Tag> {
        self.inner.id_allocator().free_id(id)
    }

    fn with<R, F: FnOnce(&<Self::HandleInner as HandleInnerBase>::ContainerInner) -> R>(&self, f: F) -> R {
        f(self.inner.inner())
    }

    fn new(inner: Self::HandleInner) -> Self {
        BoundedHandle {
            inner: Arc::new(inner)
        }
    }

    fn id_limit<Tag>(&self) -> usize where Self::HandleInner: HandleInner<Tag> {
        self.inner.id_allocator().id_limit()
    }
}

impl<H> Clone for BoundedHandle<H> {
    fn clone(&self) -> Self {
        BoundedHandle {
            inner: self.inner.clone()
        }
    }
}


pub type BoundedIdHandle<Tag, H> = IdHandle<Tag, BoundedHandle<H>>;
