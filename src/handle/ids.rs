use std::borrow::{Borrow, BorrowMut};

use primitives::index_allocator::IndexAllocator;

pub trait Like<T>: Into<T> + From<T> + Borrow<T> + BorrowMut<T> {
    fn virtual_borrow<R, F: FnOnce(&mut Self) -> R>(value: T, f: F) -> R {
        let mut v = value.into();
        let result = f(&mut v);
        let _: T = v.into();
        result
    }
}
impl<T, U: Into<T> + From<T> + Borrow<T> + BorrowMut<T>> Like<T> for U {}

macro_rules! define_id {
    ($name:ident) => {
        #[derive(Debug)]
        pub struct $name(usize);

        impl From<usize> for $name {
            fn from(value: usize) -> Self { $name(value) }
        }
        impl From<$name> for usize {
            fn from(value: $name) -> Self { value.0 }
        }
        impl ::std::borrow::Borrow<usize> for $name {
            fn borrow(&self) -> &usize { &self.0 }
        }
        impl ::std::borrow::BorrowMut<usize> for $name {
            fn borrow_mut(&mut self) -> &mut usize { &mut self.0 }
        }
    }
}

pub trait IdAllocator<IdType> {
    fn new(limit: usize) -> Self;
    fn try_allocate_id(&self) -> Option<IdType>;
    fn free_id(&self, id: IdType);
    fn id_limit(&self) -> usize;
}

impl<IdType: Like<usize>> IdAllocator<IdType> for IndexAllocator {
    fn new(limit: usize) -> Self {
        IndexAllocator::new(limit)
    }
    fn try_allocate_id(&self) -> Option<IdType> {
        self.try_allocate().map(Into::into)
    }
    fn free_id(&self, id: IdType) {
        self.free(id.into())
    }
    fn id_limit(&self) -> usize {
        self.len()
    }
}
