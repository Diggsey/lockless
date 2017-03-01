use std::marker::PhantomData;
use std::cell::UnsafeCell;

use primitives::invariant::Invariant;
use handle::{Id, ContainerInner, Distinct};

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


#[derive(Debug)]
pub struct IdMap2<T, Tag0, Tag1> {
    values: Vec<UnsafeCell<Option<T>>>,
    indices: Vec<UnsafeCell<usize>>,
    id_limit0: usize,
    phantom: Invariant<(Tag0, Tag1)>
}

unsafe impl<T: Send, Tag0, Tag1> Sync for IdMap2<T, Tag0, Tag1> {}

pub trait IdIntoUsize2<Tag0, Tag1> where (Tag0, Tag1): Distinct {
    fn into_usize(self, id_limit0: usize) -> usize;
}

impl<Tag0, Tag1> IdIntoUsize2<Tag0, Tag1> for Id<Tag0> where (Tag0, Tag1): Distinct {
    fn into_usize(self, _: usize) -> usize { self.into() }
}

impl<Tag0, Tag1> IdIntoUsize2<Tag0, Tag1> for Id<Tag1> where (Tag0, Tag1): Distinct {
    fn into_usize(self, id_limit0: usize) -> usize { id_limit0 + Into::<usize>::into(self) }
}

impl<T, Tag0, Tag1> IdMap2<T, Tag0, Tag1> where (Tag0, Tag1): Distinct {
    pub fn new() -> Self {
        IdMap2 {
            values: Vec::new(),
            indices: Vec::new(),
            id_limit0: 0,
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

    pub unsafe fn store<Id: IdIntoUsize2<Tag0, Tag1>>(&self, id: Id, value: Option<T>) -> &mut usize {
        let id: usize = id.into_usize(self.id_limit0);
        let ref mut idx = *(&self.indices[id]).get();
        self.store_at(*idx, value);
        idx
    }
    pub unsafe fn load<Id: IdIntoUsize2<Tag0, Tag1>>(&self, id: Id) -> Option<T> {
        let id: usize = id.into_usize(self.id_limit0);
        let idx = *(&self.indices[id]).get();
        self.load_at(idx)
    }
}

fn rotate_slice<T>(slice: &mut [T], places: usize) {
    slice.reverse();
    let (a, b) = slice.split_at_mut(places);
    a.reverse();
    b.reverse();
}

impl<T, Tag0, Tag1> ContainerInner<Tag0> for IdMap2<T, Tag0, Tag1> where (Tag0, Tag1): Distinct {
    fn raise_id_limit(&mut self, new_limit: usize) {
        let id_limit = ContainerInner::<Tag0>::id_limit(self);
        assert!(new_limit > id_limit);

        let extra_len = new_limit - id_limit;
        self.indices.reserve_exact(extra_len);
        self.values.reserve_exact(extra_len);
        for _ in 0..extra_len {
            self.indices.push(UnsafeCell::new(self.values.len()));
            self.values.push(UnsafeCell::new(None));
        }
        rotate_slice(&mut self.indices[self.id_limit0..], extra_len);
        self.id_limit0 += extra_len;
    }

    fn id_limit(&self) -> usize {
        self.id_limit0
    }
}

impl<T, Tag0, Tag1> ContainerInner<Tag1> for IdMap2<T, Tag0, Tag1> where (Tag0, Tag1): Distinct {
    fn raise_id_limit(&mut self, new_limit: usize) {
        let id_limit = ContainerInner::<Tag1>::id_limit(self);
        assert!(new_limit > id_limit);

        let extra_len = new_limit - id_limit;
        self.indices.reserve_exact(extra_len);
        self.values.reserve_exact(extra_len);
        for _ in 0..extra_len {
            self.indices.push(UnsafeCell::new(self.values.len()));
            self.values.push(UnsafeCell::new(None));
        }
    }

    fn id_limit(&self) -> usize {
        self.indices.len() - self.id_limit0
    }
}