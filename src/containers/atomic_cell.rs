use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::marker::PhantomData;

use handle::{ContainerInner, Handle, IdHandle, ResizingHandle, BoundedHandle, HandleInner, HandleInner1, Tag0, HandleInnerBase, Id};
use primitives::index_allocator::IndexAllocator;
use primitives::invariant::Invariant;

#[derive(Debug)]
pub struct AtomicCellInner<T, Tag> {
    values: Vec<UnsafeCell<Option<T>>>,
    indices: Vec<UnsafeCell<usize>>,
    current: AtomicUsize,
    phantom: Invariant<Tag>,
}

unsafe impl<T: Send, Tag> Sync for AtomicCellInner<T, Tag> {}

impl<T, Tag> ContainerInner<Tag> for AtomicCellInner<T, Tag> {
    fn raise_id_limit(&mut self, new_limit: usize) {
        assert!(new_limit > self.id_limit());

        let mut len = self.values.len();
        self.values.reserve_exact(new_limit + 1 - len);
        self.indices.reserve_exact(new_limit - len);
        while len < new_limit {
            len += 1;
            self.values.push(UnsafeCell::new(None));
            self.indices.push(UnsafeCell::new(len));
        }
    }
    fn id_limit(&self) -> usize {
        self.indices.len()
    }
}

impl<T, Tag> AtomicCellInner<T, Tag> {
    pub fn new(value: T, max_accessors: usize) -> Self {
        let mut result = AtomicCellInner {
            values: Vec::with_capacity(max_accessors+1),
            indices: Vec::with_capacity(max_accessors),
            current: AtomicUsize::new(0),
            phantom: PhantomData
        };
        result.values.push(UnsafeCell::new(Some(value)));
        result.raise_id_limit(max_accessors);
        result
    }
    pub unsafe fn swap(&self, id: Id<Tag>, value: T) -> T {
        let id: usize = id.into();
        // Need the extra brackets to avoid compiler bug:
        // https://github.com/rust-lang/rust/issues/28935
        let ref mut index = *(&self.indices[id]).get();
        *(&self.values[*index]).get() = Some(value);
        *index = self.current.swap(*index, Ordering::AcqRel);
        (*self.values[*index].get()).take().expect("Cell should contain a value!")
    }
}

#[derive(Debug)]
pub struct AtomicCell<H: Handle, Tag>(IdHandle<Tag, H>) where H::HandleInner: HandleInner<Tag>;

impl<T, H: Handle, Tag> AtomicCell<H, Tag> where H::HandleInner: HandleInnerBase<ContainerInner=AtomicCellInner<T, Tag>> + HandleInner<Tag> {
    pub fn new(value: T, max_accessors: usize) -> Self {
        AtomicCell(IdHandle::new(&HandleInnerBase::new(AtomicCellInner::new(value, max_accessors))))
    }

    pub fn swap(&mut self, value: T) -> T {
        self.0.with_mut(move |inner, id| unsafe { inner.swap(id, value) })
    }
}

impl<H: Handle, Tag> Clone for AtomicCell<H, Tag> where H::HandleInner: HandleInner<Tag> {
    fn clone(&self) -> Self {
        AtomicCell(self.0.clone())
    }
}

type Inner<T> = HandleInner1<Tag0, IndexAllocator, AtomicCellInner<T, Tag0>>;
pub type ResizingAtomicCell<T> = AtomicCell<ResizingHandle<Inner<T>>, Tag0>;
pub type BoundedAtomicCell<T> = AtomicCell<BoundedHandle<Inner<T>>, Tag0>;
