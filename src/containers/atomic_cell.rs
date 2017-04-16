use std::sync::atomic::{AtomicUsize, Ordering};
use std::marker::PhantomData;

use handle::{Handle, IdHandle, ResizingHandle, BoundedHandle, HandleInner, Like};
use primitives::index_allocator::IndexAllocator;
use containers::storage::{Storage, Place};
use containers::scratch::Scratch;

#[derive(Debug)]
pub struct AtomicCellInner<T: Like<usize>>(AtomicUsize, PhantomData<T>);

impl<T: Like<usize>> AtomicCellInner<T> {
    pub fn new(value: T) -> Self {
        AtomicCellInner(AtomicUsize::new(value.into()), PhantomData)
    }
    pub unsafe fn swap(&self, value: &mut T) {
        let value = value.borrow_mut();
        *value = self.0.swap(*value, Ordering::AcqRel);
    }
}

define_id!(AtomicCellId);

pub struct AtomicCellWrapper<T> {
    storage: Storage<T>,
    scratch: Scratch<AtomicCellId, Place<T>>,
    inner: AtomicCellInner<Place<T>>,
    id_alloc: IndexAllocator
}

impl<T> AtomicCellWrapper<T> {
    pub fn new<H: Handle<HandleInner=Self>>(id_limit: usize, value: T) -> H {
        assert!(id_limit > 0);
        let mut storage = Storage::with_capacity(id_limit + 1);
        let scratch = Scratch::new(storage.none_storing_iter(id_limit));;
        let inner = AtomicCellInner::new(storage.store(Some(value)));
        let id_alloc = IndexAllocator::new(id_limit);

        Handle::new(AtomicCellWrapper {
            storage: storage,
            scratch: scratch,
            inner: inner,
            id_alloc: id_alloc,
        })
    }

    pub unsafe fn swap(&self, id: &mut AtomicCellId, value: T) -> T {
        let place = self.scratch.get_mut(id);
        self.storage.replace(place, Some(value));
        self.inner.swap(place);
        self.storage.replace(place, None).expect("Some(value) in container")
    }
}

impl<T> HandleInner<AtomicCellId> for AtomicCellWrapper<T> {
    type IdAllocator = IndexAllocator;
    fn id_allocator(&self) -> &IndexAllocator {
        &self.id_alloc
    }
    fn raise_id_limit(&mut self, new_limit: usize) {
        let old_limit = self.id_limit();
        assert!(new_limit > old_limit);
        let extra = new_limit - old_limit;
        self.storage.reserve(extra);
        self.scratch.extend(self.storage.none_storing_iter(extra));
        self.id_alloc.resize(new_limit);
    }
}

#[derive(Debug)]
pub struct AtomicCell<T, H: Handle<HandleInner=AtomicCellWrapper<T>>>(IdHandle<H, AtomicCellId>);

impl<T, H: Handle<HandleInner=AtomicCellWrapper<T>>> AtomicCell<T, H> {
    pub fn new(max_accessors: usize, value: T) -> Self {
        AtomicCell(IdHandle::new(&AtomicCellWrapper::new(max_accessors, value)))
    }

    pub fn swap(&mut self, value: T) -> T {
        self.0.with_mut(move |inner, id| unsafe { inner.swap(id, value) })
    }
}

impl<T, H: Handle<HandleInner=AtomicCellWrapper<T>>> Clone for AtomicCell<T, H> {
    fn clone(&self) -> Self {
        AtomicCell(self.0.clone())
    }
}

pub type ResizingAtomicCell<T> = AtomicCell<T, ResizingHandle<AtomicCellWrapper<T>>>;
pub type BoundedAtomicCell<T> = AtomicCell<T, BoundedHandle<AtomicCellWrapper<T>>>;
