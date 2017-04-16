use std::sync::Arc;

use super::{HandleInner, Handle, IdHandle, IdAllocator};

/// Implementation of Handle which panics if it runs out of IDs
#[derive(Debug)]
pub struct BoundedHandle<H> {
    inner: Arc<H>
}

unsafe impl<H> Handle for BoundedHandle<H> {
    type HandleInner = H;

    fn try_allocate_id<IdType>(&self) -> Option<IdType> where Self::HandleInner: HandleInner<IdType> {
        self.inner.id_allocator().try_allocate_id()
    }

    fn free_id<IdType>(&self, id: IdType) where Self::HandleInner: HandleInner<IdType> {
        self.inner.id_allocator().free_id(id)
    }

    fn with<R, F: FnOnce(&Self::HandleInner) -> R>(&self, f: F) -> R {
        f(&self.inner)
    }

    fn new(inner: Self::HandleInner) -> Self {
        BoundedHandle {
            inner: Arc::new(inner)
        }
    }

    fn id_limit<IdType>(&self) -> usize where Self::HandleInner: HandleInner<IdType> {
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
