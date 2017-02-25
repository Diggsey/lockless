use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::marker::PhantomData;

use handle::{HandleInner, Handle, IdHandle, ResizingHandle, BoundedHandle, Tag0, HandleInnerBase, ContainerInner, HandleInner1, Id};
use primitives::index_allocator::IndexAllocator;
use primitives::invariant::Invariant;

#[derive(Debug)]
pub struct AtomicCellArrayInner<T, Tag> {
    values: Vec<UnsafeCell<Option<T>>>,
    indices: Vec<UnsafeCell<usize>>,
    current: Vec<AtomicUsize>,
    phantom: Invariant<Tag>,
}

unsafe impl<T: Send, Tag> Sync for AtomicCellArrayInner<T, Tag> {}

impl<T, Tag> ContainerInner<Tag> for AtomicCellArrayInner<T, Tag> {
    fn raise_id_limit(&mut self, new_limit: usize) {
        assert!(new_limit > self.id_limit());

        let extra_len = new_limit - self.id_limit();
        self.values.reserve_exact(extra_len);
        self.indices.reserve_exact(extra_len);
        for _ in 0..extra_len {
            self.indices.push(UnsafeCell::new(self.values.len()));
            self.values.push(UnsafeCell::new(None));
        }
    }
    fn id_limit(&self) -> usize {
        self.indices.len()
    }
}

impl<T, Tag> AtomicCellArrayInner<T, Tag> {
    pub fn reserve_exact(&mut self, additional_cells: usize, additional_ids: usize) {
        self.values.reserve_exact(additional_cells + additional_ids);
        self.indices.reserve_exact(additional_ids);
        self.current.reserve_exact(additional_cells);
    }

    fn place(&mut self, value: T) -> AtomicUsize {
        let id = self.values.len();
        self.values.push(UnsafeCell::new(Some(value)));
        AtomicUsize::new(id)
    }

    pub fn push(&mut self, value: T) {
        let idx = self.place(value);
        self.current.push(idx);
    }

    pub fn insert(&mut self, index: usize, value: T) {
        let idx = self.place(value);
        self.current.insert(index, idx)
    }

    pub fn with_capacity(capacity: usize, id_limit: usize) -> Self {
        let mut result = AtomicCellArrayInner {
            values: Vec::new(),
            indices: Vec::new(),
            current: Vec::new(),
            phantom: PhantomData
        };
        result.reserve_exact(capacity, id_limit);
        result.raise_id_limit(id_limit);
        result
    }

    pub fn new(id_limit: usize) -> Self {
        Self::with_capacity(0, id_limit)
    }
    
    pub unsafe fn swap(&self, index: usize, id: Id<Tag>, value: T) -> T {
        let id: usize = id.into();
        // Need the extra brackets to avoid compiler bug:
        // https://github.com/rust-lang/rust/issues/28935
        let ref mut idx = *(&self.indices[id]).get();
        *(&self.values[*idx]).get() = Some(value);
        *idx = self.current[index].swap(*idx, Ordering::AcqRel);
        (*self.values[*idx].get()).take().expect("Cell should contain a value!")
    }
}

#[derive(Debug)]
pub struct AtomicCellArray<H: Handle, Tag>(IdHandle<Tag, H>) where H::HandleInner: HandleInner<Tag>;

impl<T, H: Handle, Tag> AtomicCellArray<H, Tag> where H::HandleInner: HandleInner<Tag, ContainerInner=AtomicCellArrayInner<T, Tag>> {
    pub fn new(max_accessors: usize) -> Self {
        AtomicCellArray(IdHandle::new(&HandleInnerBase::new(AtomicCellArrayInner::new(max_accessors))))
    }

    pub fn swap(&mut self, index: usize, value: T) -> T {
        self.0.with_mut(move |inner, id| unsafe { inner.swap(index, id, value) })
    }

    pub fn len(&self) -> usize {
        self.0.with(|inner| inner.current.len())
    }
}

impl<H: Handle, Tag> Clone for AtomicCellArray<H, Tag> where H::HandleInner: HandleInner<Tag> {
    fn clone(&self) -> Self {
        AtomicCellArray(self.0.clone())
    }
}

type Inner<T> = HandleInner1<Tag0, IndexAllocator, AtomicCellArrayInner<T, Tag0>>;
pub type ResizingAtomicCellArray<T> = AtomicCellArray<ResizingHandle<Inner<T>>, Tag0>;
pub type BoundedAtomicCellArray<T> = AtomicCellArray<BoundedHandle<Inner<T>>, Tag0>;
