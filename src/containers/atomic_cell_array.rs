use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;

use handle::{IdLimit, RaisableIdLimit, Handle, IdHandle, ResizingHandle, BoundedHandle};

#[derive(Debug)]
pub struct AtomicCellArrayInner<T> {
    values: Vec<UnsafeCell<Option<T>>>,
    indices: Vec<UnsafeCell<usize>>,
    current: Vec<AtomicUsize>
}

unsafe impl<T: Send> Sync for AtomicCellArrayInner<T> {}

impl<T> IdLimit for AtomicCellArrayInner<T> {
    fn id_limit(&self) -> usize {
        self.indices.len()
    }
}

impl<T> RaisableIdLimit for AtomicCellArrayInner<T> {
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
}

impl<T> AtomicCellArrayInner<T> {
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
            current: Vec::new()
        };
        result.reserve_exact(capacity, id_limit);
        result.raise_id_limit(id_limit);
        result
    }

    pub fn new(id_limit: usize) -> Self {
        Self::with_capacity(0, id_limit)
    }
    
    pub unsafe fn swap(&self, index: usize, id: usize, value: T) -> T {
        // Need the extra brackets to avoid compiler bug:
        // https://github.com/rust-lang/rust/issues/28935
        let ref mut idx = *(&self.indices[id]).get();
        *(&self.values[*idx]).get() = Some(value);
        *idx = self.current[index].swap(*idx, Ordering::AcqRel);
        (*self.values[*idx].get()).take().expect("Cell should contain a value!")
    }
}

#[derive(Debug, Clone)]
pub struct AtomicCellArray<H: Handle>(IdHandle<H>);

impl<T, H: Handle<Target=AtomicCellArrayInner<T>>> AtomicCellArray<H> {
    pub fn new(max_accessors: usize) -> Self {
        AtomicCellArray(IdHandle::new(&Handle::new(AtomicCellArrayInner::new(max_accessors))))
    }

    pub fn swap(&mut self, index: usize, value: T) -> T {
        self.0.with_mut(move |inner, id| unsafe { inner.swap(index, id, value) })
    }

    pub fn len(&self) -> usize {
        self.0.with(|inner| inner.current.len())
    }
}

pub type ResizingAtomicCellArray<T> = AtomicCellArray<ResizingHandle<AtomicCellArrayInner<T>>>;
pub type BoundedAtomicCellArray<T> = AtomicCellArray<BoundedHandle<AtomicCellArrayInner<T>>>;
