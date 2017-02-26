use std::sync::atomic::{AtomicUsize, Ordering};
use std::marker::PhantomData;

use handle::{ContainerInner, Handle, IdHandle, ResizingHandle, BoundedHandle, HandleInner, HandleInner1, Tag0, HandleInnerBase, Id};
use primitives::index_allocator::IndexAllocator;
use primitives::invariant::Invariant;
use containers::id_map::IdMap1;

#[derive(Debug)]
pub struct AtomicCellInner<T, Tag> {
    id_map: IdMap1<T, Tag>,
    current: AtomicUsize,
    phantom: Invariant<Tag>,
}

impl<T, Tag> ContainerInner<Tag> for AtomicCellInner<T, Tag> {
    fn raise_id_limit(&mut self, new_limit: usize) {
        self.id_map.raise_id_limit(new_limit);
    }
    fn id_limit(&self) -> usize {
        self.id_map.id_limit()
    }
}

impl<T, Tag> AtomicCellInner<T, Tag> {
    pub fn new(value: T, id_limit: usize) -> Self {
        let mut id_map = IdMap1::new();
        id_map.reserve(1, id_limit);
        let current = AtomicUsize::new(id_map.push_value(Some(value)));
        id_map.raise_id_limit(id_limit);

        AtomicCellInner {
            id_map: id_map,
            current: current,
            phantom: PhantomData
        }
    }
    pub unsafe fn swap(&self, id: Id<Tag>, value: T) -> T {
        let mut idx = self.id_map.store(id, Some(value));
        *idx = self.current.swap(*idx, Ordering::AcqRel);
        self.id_map.load_at(*idx).expect("Cell should contain a value!")
    }
}

#[derive(Debug)]
pub struct AtomicCell<H: Handle, Tag>(IdHandle<Tag, H>) where H::HandleInner: HandleInner<Tag>;

impl<T, H: Handle, Tag> AtomicCell<H, Tag> where H::HandleInner: HandleInner<Tag, ContainerInner=AtomicCellInner<T, Tag>> {
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
