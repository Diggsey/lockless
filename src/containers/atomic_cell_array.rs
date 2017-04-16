use std::sync::atomic::{AtomicUsize, Ordering};
use std::marker::PhantomData;

use handle::{HandleInner, Handle, IdHandle, ResizingHandle, BoundedHandle, Like};
use primitives::index_allocator::IndexAllocator;
use containers::storage::{Storage, Place};
use containers::scratch::Scratch;

#[derive(Debug)]
pub struct AtomicCellArrayInner<T: Like<usize>>(Vec<AtomicUsize>, PhantomData<T>);

impl<T: Like<usize>> AtomicCellArrayInner<T> {
    pub fn push(&mut self, value: T) {
        self.0.push(AtomicUsize::new(value.into()));
    }

    pub fn reserve(&mut self, extra: usize) {
        self.0.reserve_exact(extra);
    }

    pub fn insert(&mut self, index: usize, value: T) {
        self.0.insert(index, AtomicUsize::new(value.into()));
    }

    pub fn new<I: IntoIterator<Item=T>>(iter: I) -> Self {
        let mut result = AtomicCellArrayInner(Vec::new(), PhantomData);
        result.extend(iter);
        result
    }

    pub fn extend<I: IntoIterator<Item=T>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        for value in iter {
            self.push(value);
        }
    }

    pub unsafe fn swap(&self, index: usize, value: &mut T) {
        let value = value.borrow_mut();
        *value = self.0[index].swap(*value, Ordering::AcqRel)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

define_id!(AtomicCellArrayId);

#[derive(Debug)]
pub struct AtomicCellArrayWrapper<T> {
    storage: Storage<T>,
    scratch: Scratch<AtomicCellArrayId, Place<T>>,
    inner: AtomicCellArrayInner<Place<T>>,
    id_alloc: IndexAllocator
}

impl<T> AtomicCellArrayWrapper<T> {
    pub fn new<H: Handle<HandleInner=Self>, I: IntoIterator<Item=T>>(id_limit: usize, values: I) -> H {
        assert!(id_limit > 0);
        let mut storage = Storage::with_capacity(id_limit + 1);
        let scratch = Scratch::new(storage.none_storing_iter(id_limit));
        let inner = AtomicCellArrayInner::new(values.into_iter().map(|v| storage.store(Some(v))));
        let id_alloc = IndexAllocator::new(id_limit);

        Handle::new(AtomicCellArrayWrapper {
            storage: storage,
            scratch: scratch,
            inner: inner,
            id_alloc: id_alloc,
        })
    }

    pub fn push(&mut self, value: T) {
        let place = self.storage.store(Some(value));
        self.inner.push(place);
    }

    pub fn insert(&mut self, index: usize, value: T) {
        let place = self.storage.store(Some(value));
        self.inner.insert(index, place);
    }

    pub unsafe fn swap(&self, id: &mut AtomicCellArrayId, index: usize, value: T) -> T {
        let place = self.scratch.get_mut(id);
        self.storage.replace(place, Some(value));
        self.inner.swap(index, place);
        self.storage.replace(place, None).expect("Some(value) in container")
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T> HandleInner<AtomicCellArrayId> for AtomicCellArrayWrapper<T> {
    type IdAllocator = IndexAllocator;
    fn id_allocator(&self) -> &IndexAllocator {
        &self.id_alloc
    }
    fn raise_id_limit(&mut self, new_limit: usize) {
        let old_limit = self.id_limit();
        assert!(new_limit > old_limit);
        let extra = new_limit - self.id_limit();
        self.storage.reserve(extra);
        self.scratch.extend(self.storage.none_storing_iter(extra));
        self.id_alloc.resize(new_limit);
    }
}

#[derive(Debug)]
pub struct AtomicCellArray<T, H: Handle<HandleInner=AtomicCellArrayWrapper<T>>>(IdHandle<H, AtomicCellArrayId>);

impl<T, H: Handle<HandleInner=AtomicCellArrayWrapper<T>>> AtomicCellArray<T, H> {
    pub fn new<I: IntoIterator<Item=T>>(max_accessors: usize, values: I) -> Self {
        AtomicCellArray(IdHandle::new(&AtomicCellArrayWrapper::new(max_accessors, values)))
    }

    pub fn swap(&mut self, index: usize, value: T) -> T {
        self.0.with_mut(move |inner, id| unsafe { inner.swap(id, index, value) })
    }

    pub fn len(&self) -> usize {
        self.0.with(|inner| inner.len())
    }
}

impl<T, H: Handle<HandleInner=AtomicCellArrayWrapper<T>>> Clone for AtomicCellArray<T, H> {
    fn clone(&self) -> Self {
        AtomicCellArray(self.0.clone())
    }
}

pub type ResizingAtomicCellArray<T> = AtomicCellArray<T, ResizingHandle<AtomicCellArrayWrapper<T>>>;
pub type BoundedAtomicCellArray<T> = AtomicCellArray<T, BoundedHandle<AtomicCellArrayWrapper<T>>>;
