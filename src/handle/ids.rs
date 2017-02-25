use std::marker::PhantomData;

use primitives::invariant::Invariant;
use primitives::index_allocator::IndexAllocator;

#[derive(Debug)]
pub struct Id<Tag>(usize, Invariant<Tag>);

impl<Tag> Copy for Id<Tag> {}
impl<Tag> Clone for Id<Tag> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag> From<Id<Tag>> for usize {
    fn from(id: Id<Tag>) -> usize {
        id.0
    }
}

impl<Tag> From<usize> for Id<Tag> {
    fn from(id: usize) -> Id<Tag> {
        Id(id, PhantomData)
    }
}

pub trait IdAllocator<Tag> {
    fn new(limit: usize) -> Self;
    fn try_allocate_id(&self) -> Option<Id<Tag>>;
    fn free_id(&self, id: Id<Tag>);
    fn id_limit(&self) -> usize;
    fn raise_id_limit(&mut self, new_limit: usize);
}

impl<Tag> IdAllocator<Tag> for IndexAllocator {
    fn new(limit: usize) -> Self {
        IndexAllocator::new(limit)
    }
    fn try_allocate_id(&self) -> Option<Id<Tag>> {
        self.try_allocate().map(Into::into)
    }
    fn free_id(&self, id: Id<Tag>) {
        self.free(id.0).into()
    }
    fn id_limit(&self) -> usize {
        self.len()
    }
    fn raise_id_limit(&mut self, new_limit: usize) {
        self.resize(new_limit)
    }
}
