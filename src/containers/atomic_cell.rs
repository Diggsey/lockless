use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;

use handle::{HasLen, Resizable, Handle, IdHandle, ResizingHandle, BoundedHandle};

#[derive(Debug)]
pub struct AtomicCellInner<T> {
    values: Vec<UnsafeCell<Option<T>>>,
    indices: Vec<UnsafeCell<usize>>,
    current: AtomicUsize
}

unsafe impl<T: Send> Sync for AtomicCellInner<T> {}

impl<T> HasLen for AtomicCellInner<T> {
    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<T> Resizable for AtomicCellInner<T> {
    fn resize(&mut self, new_len: usize) {
        assert!(new_len > self.len());

        let mut len = self.values.len();
        self.values.reserve_exact(new_len + 1 - len);
        self.indices.reserve_exact(new_len - len);
        while len < new_len {
            len += 1;
            self.values.push(UnsafeCell::new(None));
            self.indices.push(UnsafeCell::new(len));
        }
    }
}

impl<T> AtomicCellInner<T> {
    pub fn new(value: T, max_accessors: usize) -> Self {
        let mut result = AtomicCellInner {
            values: Vec::with_capacity(max_accessors+1),
            indices: Vec::with_capacity(max_accessors),
            current: AtomicUsize::new(0)
        };
        result.values.push(UnsafeCell::new(Some(value)));
        result.resize(max_accessors);
        result
    }
    pub unsafe fn swap(&self, id: usize, value: T) -> T {
        // Need the extra brackets to avoid compiler bug:
        // https://github.com/rust-lang/rust/issues/28935
        let ref mut index = *(&self.indices[id]).get();
        *(&self.values[*index]).get() = Some(value);
        *index = self.current.swap(*index, Ordering::AcqRel);
        (*self.values[*index].get()).take().expect("Cell should contain a value!")
    }
}

#[derive(Debug, Clone)]
pub struct AtomicCell<H: Handle>(IdHandle<H>);

impl<T, H: Handle<Target=AtomicCellInner<T>>> AtomicCell<H> {
    pub fn new(value: T, max_accessors: usize) -> Self {
        AtomicCell(IdHandle::new(&Handle::new(AtomicCellInner::new(value, max_accessors))))
    }

    pub fn swap(&mut self, value: T) -> T {
        self.0.with(move |inner, id| unsafe { inner.swap(id, value) })
    }
}

pub type ResizingAtomicCell<T> = AtomicCell<ResizingHandle<AtomicCellInner<T>>>;
pub type BoundedAtomicCell<T> = AtomicCell<BoundedHandle<AtomicCellInner<T>>>;
