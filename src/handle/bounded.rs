use std::sync::Arc;

use super::{HandleInner, IdLimit, Handle, IdHandle};

/// Implementation of Handle which panics if it runs out of IDs
#[derive(Debug)]
pub struct BoundedHandle<T> {
    inner: Arc<HandleInner<T>>
}

unsafe impl<T> Handle for BoundedHandle<T> where T: IdLimit {
    type Target = T;

    fn try_allocate_id(&self) -> Option<usize> {
        self.inner.index_allocator.allocate()
    }

    fn free_id(&self, id: usize) {
        self.inner.index_allocator.free(id)
    }

    fn with<R, F: FnOnce(&Self::Target) -> R>(&self, f: F) -> R {
        f(&self.inner.inner)
    }

    fn new(inner: T) -> Self {
        BoundedHandle {
            inner: Arc::new(HandleInner::new(inner))
        }
    }
}

impl<T> Clone for BoundedHandle<T> {
    fn clone(&self) -> Self {
        BoundedHandle {
            inner: self.inner.clone()
        }
    }
}


pub type BoundedIdHandle<T> = IdHandle<BoundedHandle<T>>;
