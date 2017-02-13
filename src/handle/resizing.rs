use std::sync::Arc;
use parking_lot::RwLock;

use super::{HandleInner, IdLimit, Handle, IdHandle};

// Implemented for data structures where the length can be increased
pub trait RaisableIdLimit: IdLimit {
    fn raise_id_limit(&mut self, new_limit: usize);
}

/// Implementation of Handle which resizes the data structure as needed
#[derive(Debug)]
pub struct ResizingHandle<T> {
    inner: Arc<RwLock<HandleInner<T>>>
}

unsafe impl<T> Handle for ResizingHandle<T> where T: RaisableIdLimit {
    type Target = T;

    fn try_allocate_id(&self) -> Option<usize> {
        let prev_len = {
            // Optimistically try getting a fresh ID
            let guard = self.inner.read();
            if let Some(id) = guard.index_allocator.allocate() {
                return Some(id);
            }
            guard.index_allocator.len()
        };
        {
            // Try again, in case another thread already resized the contents
            let mut guard = self.inner.write();
            let mut maybe_id = guard.index_allocator.allocate();
            if prev_len == guard.index_allocator.len() || maybe_id.is_none() {
                guard.index_allocator.resize(prev_len*2);
                guard.inner.raise_id_limit(prev_len*2);
                if maybe_id.is_none() {
                    maybe_id = guard.index_allocator.allocate();
                }
            }

            maybe_id
        }
    }

    fn free_id(&self, id: usize) {
        self.inner.read().index_allocator.free(id)
    }

    fn with<R, F: FnOnce(&Self::Target) -> R>(&self, f: F) -> R {
        f(&self.inner.read().inner)
    }

    fn new(inner: T) -> Self {
        ResizingHandle {
            inner: Arc::new(RwLock::new(HandleInner::new(inner)))
        }
    }
}

impl<T> Clone for ResizingHandle<T> {
    fn clone(&self) -> Self {
        ResizingHandle {
            inner: self.inner.clone()
        }
    }
}


pub type ResizingIdHandle<T> = IdHandle<ResizingHandle<T>>;
