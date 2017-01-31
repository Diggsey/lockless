use std::sync::atomic::{
    AtomicUsize,
    AtomicIsize,
    AtomicPtr,
    Ordering
};

pub trait AtomicExt {
    type Value: Copy;

    fn try_update<E, F: FnMut(Self::Value) -> Result<Self::Value, E>>(&self, mut f: F) -> Result<(Self::Value, Self::Value), E> {
        let mut prev = self.load_impl(Ordering::Relaxed);
        loop {
            match f(prev) {
                Ok(next) => match self.compare_exchange_weak_impl(prev, next, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => return Ok((prev, next)),
                    Err(new_prev) => prev = new_prev,
                },
                Err(e) => return Err(e)
            }
        }
    }

    fn load_impl(&self, ordering: Ordering) -> Self::Value;
    fn compare_exchange_weak_impl(
        &self,
        current: Self::Value,
        new: Self::Value,
        success: Ordering,
        failure: Ordering
    ) -> Result<Self::Value, Self::Value>;
}

macro_rules! atomic_ext_defaults {
    () => {
        fn load_impl(&self, ordering: Ordering) -> Self::Value {
            self.load(ordering)
        }
        fn compare_exchange_weak_impl(
            &self,
            current: Self::Value,
            new: Self::Value,
            success: Ordering,
            failure: Ordering
        ) -> Result<Self::Value, Self::Value> {
            self.compare_exchange_weak(current, new, success, failure)
        }
    }
}

impl AtomicExt for AtomicUsize {
    type Value = usize;
    atomic_ext_defaults!();
}
impl AtomicExt for AtomicIsize {
    type Value = isize;
    atomic_ext_defaults!();
}
impl<T> AtomicExt for AtomicPtr<T> {
    type Value = *mut T;
    atomic_ext_defaults!();
}
