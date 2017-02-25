use std::marker::PhantomData;

pub struct InvariantInner<T: ?Sized>(*mut T);
unsafe impl<T: ?Sized> Send for InvariantInner<T> {}
unsafe impl<T: ?Sized> Sync for InvariantInner<T> {}

pub type Invariant<T> = PhantomData<InvariantInner<T>>;
