use std::cell::UnsafeCell;
use std::marker::PhantomData;

use handle::Like;
use primitives::invariant::Invariant;

#[derive(Debug)]
pub struct Scratch<T: Like<usize>, U>(Vec<UnsafeCell<U>>, Invariant<T>);
unsafe impl<T: Like<usize>, U: Send> Sync for Scratch<T, U> {}

impl<T: Like<usize>, U> Scratch<T, U> {
    pub fn store(&mut self, value: U) {
        self.0.push(UnsafeCell::new(value));
    }
    pub unsafe fn get_mut(&self, id: &mut T) -> &mut U {
        &mut *(&self.0[*id.borrow()]).get()
    }
    pub fn new<I: IntoIterator<Item=U>>(iter: I) -> Self {
        let mut result = Scratch(Vec::new(), PhantomData);
        result.extend(iter);
        result
    }
    pub fn extend<I: IntoIterator<Item=U>>(&mut self, iter: I) {
        self.0.extend(iter.into_iter().map(UnsafeCell::new));
    }
}
