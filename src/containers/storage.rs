use std::marker::PhantomData;
use std::cell::UnsafeCell;
use std::mem;
use std::borrow::{Borrow, BorrowMut};

#[derive(Debug)]
pub struct Place<T>(usize, PhantomData<T>);

impl<T> From<usize> for Place<T> {
    fn from(idx: usize) -> Self {
        Place(idx, PhantomData)
    }
}

impl<T> From<Place<T>> for usize {
    fn from(place: Place<T>) -> Self {
        place.0
    }
}

impl<T> Borrow<usize> for Place<T> {
    fn borrow(&self) -> &usize {
        &self.0
    }
}

impl<T> BorrowMut<usize> for Place<T> {
    fn borrow_mut(&mut self) -> &mut usize {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct Storage<T>(Vec<UnsafeCell<Option<T>>>);
unsafe impl<T: Send> Sync for Storage<T> {}

impl<T> Storage<T> {
    pub fn store(&mut self, value: Option<T>) -> Place<T> {
        let result = self.0.len().into();
        self.0.push(UnsafeCell::new(value));
        result
    }
    pub unsafe fn replace(&self, place: &mut Place<T>, value: Option<T>) -> Option<T> {
        mem::replace(&mut *(&self.0[place.0]).get(), value)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Storage(Vec::with_capacity(capacity))
    }
    pub fn new() -> Self {
        Self::with_capacity(0)
    }
    pub fn reserve(&mut self, extra: usize) {
        self.0.reserve_exact(extra);
    }
    pub fn none_storing_iter(&mut self, n: usize) -> NoneStoringIter<T> {
        NoneStoringIter {
            storage: self,
            n: n,
        }
    }
}

pub struct NoneStoringIter<'a, T: 'a> {
    storage: &'a mut Storage<T>,
    n: usize,
}

impl<'a, T: 'a> Iterator for NoneStoringIter<'a, T> {
    type Item = Place<T>;

    fn next(&mut self) -> Option<Place<T>> {
        if self.n == 0 {
            None
        } else {
            self.n -= 1;
            Some(self.storage.store(None))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.n, Some(self.n))
    }
}

impl<'a, T: 'a> ExactSizeIterator for NoneStoringIter<'a, T> {}
