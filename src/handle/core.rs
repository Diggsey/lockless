/// This encapsulates the pattern of wrapping a fixed-size lock-free data-structure inside an
/// RwLock, and automatically resizing it when the number of concurrent handles increases.

use primitives::index_allocator::IndexAllocator;

// Implemented for data structures with a length
pub trait HasLen {
    fn len(&self) -> usize;
}

#[derive(Debug)]
pub struct HandleInner<T> {
    pub inner: T,
    pub index_allocator: IndexAllocator
}

impl<T: HasLen> HandleInner<T> {
    pub fn new(inner: T) -> Self {
        let len = inner.len();
        HandleInner {
            inner: inner,
            index_allocator: IndexAllocator::new(len)
        }
    }
}

pub unsafe trait Handle: Sized + Clone {
    type Target: HasLen;

    fn try_allocate_id(&self) -> Option<usize>;
    fn free_id(&self, id: usize);
    fn with<R, F: FnOnce(&Self::Target) -> R>(&mut self, f: F) -> R;
    fn new(inner: Self::Target) -> Self;
}

// `ResizingHandle`s will automatically allocate themselves an ID, which
// may require resizing the data structure.
#[derive(Debug)]
pub struct IdHandle<H: Handle> {
    id: usize,
    handle: H
}

impl<H: Handle> IdHandle<H> {
    pub fn try_clone(&self) -> Option<Self> {
        Self::try_new(&self.handle)
    }

    pub fn new(handle: &H) -> Self {
        Self::try_new(handle).expect("Failed to allocate an ID")
    }
    pub fn try_new(handle: &H) -> Option<Self> {
        if let Some(id) = handle.try_allocate_id() {
            Some(IdHandle {
                id: id,
                handle: handle.clone()
            })
        } else {
            None
        }
    }
    pub fn id(&self) -> usize {
        self.id
    }
    pub fn with<R, F: FnOnce(&H::Target, usize) -> R>(&mut self, f: F) -> R {
        let id = self.id;
        self.handle.with(move |v| f(v, id))
    }
}

impl<H: Handle> Clone for IdHandle<H> {
    fn clone(&self) -> Self {
        Self::new(&self.handle)
    }
}

impl<H: Handle> Drop for IdHandle<H> {
    fn drop(&mut self) {
        self.handle.free_id(self.id)
    }
}
