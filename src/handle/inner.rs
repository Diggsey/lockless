use super::ids::IdAllocator;

pub trait HandleInner<IdType> {
    type IdAllocator: IdAllocator<IdType>;

    fn raise_id_limit(&mut self, new_limit: usize);
    fn id_allocator(&self) -> &Self::IdAllocator;
    fn id_limit(&self) -> usize {
        self.id_allocator().id_limit()
    }
}
