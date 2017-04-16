use std::sync::Arc;
use parking_lot::RwLock;

use super::{Handle, IdHandle, IdAllocator, HandleInner};

/// Implementation of Handle which resizes the data structure as needed
#[derive(Debug)]
pub struct ResizingHandle<H> {
    inner: Arc<RwLock<H>>
}

unsafe impl<H> Handle for ResizingHandle<H> {
    type HandleInner = H;

    fn try_allocate_id<IdType>(&self) -> Option<IdType> where Self::HandleInner: HandleInner<IdType> {
        let prev_limit = {
            // Optimistically try getting a fresh ID
            let guard = self.inner.read();
            let id_alloc = guard.id_allocator();
            if let Some(id) = id_alloc.try_allocate_id() {
                return Some(id);
            }
            id_alloc.id_limit()
        };
        {
            // Try again, in case another thread already resized the contents
            let mut guard = self.inner.write();
            let (mut maybe_id, new_limit) = {
                let id_alloc = guard.id_allocator();
                (id_alloc.try_allocate_id(), id_alloc.id_limit())
            };
            
            // Even if we get an ID, if the container is still the same size,
            // resize it anyway, to avoid this slower path from happening
            // repeatedly.
            if prev_limit == new_limit || maybe_id.is_none() {
                guard.raise_id_limit(new_limit*2);
                if maybe_id.is_none() {
                    maybe_id = guard.id_allocator().try_allocate_id();
                }
            }

            maybe_id
        }
    }

    fn free_id<IdType>(&self, id: IdType) where Self::HandleInner: HandleInner<IdType> {
        self.inner.read().id_allocator().free_id(id)
    }

    fn with<R, F: FnOnce(&Self::HandleInner) -> R>(&self, f: F) -> R {
        f(&self.inner.read())
    }

    fn new(inner: Self::HandleInner) -> Self {
        ResizingHandle {
            inner: Arc::new(RwLock::new(inner))
        }
    }

    fn id_limit<IdType>(&self) -> usize where Self::HandleInner: HandleInner<IdType> {
        self.inner.read().id_allocator().id_limit()
    }
}

impl<H> Clone for ResizingHandle<H> {
    fn clone(&self) -> Self {
        ResizingHandle {
            inner: self.inner.clone()
        }
    }
}


pub type ResizingIdHandle<Tag, H> = IdHandle<Tag, ResizingHandle<H>>;
