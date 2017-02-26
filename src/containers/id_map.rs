use std::marker::PhantomData;
use std::cell::UnsafeCell;

use primitives::invariant::Invariant;
use handle::{Id, ContainerInner};

#[derive(Debug)]
pub struct IdMap1<T, Tag> {
    values: Vec<UnsafeCell<Option<T>>>,
    indices: Vec<UnsafeCell<usize>>,
    phantom: Invariant<Tag>
}

unsafe impl<T: Send, Tag> Sync for IdMap1<T, Tag> {}

impl<T, Tag> IdMap1<T, Tag> {
    pub fn new() -> Self {
        IdMap1 {
            values: Vec::new(),
            indices: Vec::new(),
            phantom: PhantomData
        }
    }

    pub fn reserve_values(&mut self, extra_values: usize) {
        self.values.reserve_exact(extra_values);
    }

    pub fn reserve(&mut self, extra_values: usize, extra_ids: usize) {
        self.values.reserve_exact(extra_values + extra_ids);
        self.indices.reserve_exact(extra_ids);
    }

    pub fn push_value(&mut self, value: Option<T>) -> usize {
        let result = self.values.len();
        self.values.push(UnsafeCell::new(value));
        result
    }

    pub unsafe fn store_at(&self, idx: usize, value: Option<T>) {
        *(&self.values[idx]).get() = value;
    }

    pub unsafe fn load_at(&self, idx: usize) -> Option<T> {
        (*(&self.values[idx]).get()).take()
    }

    pub unsafe fn store(&self, id: Id<Tag>, value: Option<T>) -> &mut usize {
        let id: usize = id.into();
        let ref mut idx = *(&self.indices[id]).get();
        self.store_at(*idx, value);
        idx
    }
    pub unsafe fn load(&self, id: Id<Tag>) -> Option<T> {
        let id: usize = id.into();
        let idx = *(&self.indices[id]).get();
        self.load_at(idx)
    }
}

impl<T, Tag> ContainerInner<Tag> for IdMap1<T, Tag> {
    fn raise_id_limit(&mut self, new_limit: usize) {
        assert!(new_limit > self.id_limit());

        let extra_len = new_limit - self.indices.len();
        self.indices.reserve_exact(extra_len);
        self.values.reserve_exact(extra_len);
        for _ in 0..extra_len {
            self.indices.push(UnsafeCell::new(self.values.len()));
            self.values.push(UnsafeCell::new(None));
        }
    }

    fn id_limit(&self) -> usize {
        self.indices.len()
    }
}