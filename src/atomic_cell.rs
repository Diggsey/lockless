use std::sync::atomic::{AtomicPtr, Ordering};
use std::cell::Cell;
use std::sync::Arc;
use std::ptr;
use std::mem;


// Helper functions for allocation in stable rust
fn allocate<T>() -> *mut T {
    let mut v = Vec::with_capacity(1);
    let result = v.as_mut_ptr();
    mem::forget(v);
    result
}

unsafe fn free<T>(ptr: *mut T) {
    Vec::from_raw_parts(ptr, 0, 1);
}

// Shared part of an AtomicCell
struct Inner<T> {
    value: AtomicPtr<T>,
}

impl<T> Inner<T> {
    fn new(value: T) -> Inner<T> {
        let p = allocate();
        unsafe {
            ptr::write(p, value);
        }
        Inner {
            value: AtomicPtr::new(p)
        }
    }
    fn unwrap(self) -> T {
        let p = self.value.load(Ordering::Relaxed);
        unsafe {
            let result = ptr::read(p);
            free(p);
            result
        }
    }
    fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value.load(Ordering::Relaxed) }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let p = self.value.load(Ordering::Relaxed);
        unsafe {
            ptr::read(p);
            free(p);
        }
    }
}

// Manages heap space for a T - may or may not contain an initialized T
struct StorageCell<T>(Cell<*mut T>);

impl<T> StorageCell<T> {
    fn new() -> Self {
        StorageCell(Cell::new(allocate()))
    }
    fn write(&self, value: T) {
        unsafe {
            ptr::write(self.0.get(), value);
        }
    }
    unsafe fn read(&self) -> T {
        ptr::read(self.0.get())
    }
    fn get(&self) -> *mut T {
        self.0.get()
    }
    unsafe fn set(&self, p: *mut T) {
        self.0.set(p)
    }
}

impl<T> Drop for StorageCell<T> {
    fn drop(&mut self) {
        unsafe {
            free(self.get())
        }
    }
}

unsafe impl<T: Send> Send for StorageCell<T> {}

/// Lock-free concurrent cell supporting an atomic "swap" operation
pub struct AtomicCell<T> {
    space: StorageCell<T>,
    inner: Arc<Inner<T>>
}

impl<T> AtomicCell<T> {
    pub fn new(value: T) -> Self {
        AtomicCell {
            space: StorageCell::new(),
            inner: Arc::new(Inner::new(value))
        }
    }
    pub fn swap(&self, value: T) -> T {
        unsafe {
            // Store the value into the space we own
            self.space.write(value);
            // Swap our space with the shared space atomically
            self.space.set(self.inner.value.swap(self.space.get(), Ordering::Relaxed));
            // Retrieve the value from the returned space
            self.space.read()
        }
    }
    pub fn try_unwrap(self) -> Result<T, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => Ok(inner.unwrap()),
            Err(inner) => Err(AtomicCell { space: self.space, inner: inner })
        }
    }
    pub fn get_mut(&mut self) -> Option<&mut T> {
        Arc::get_mut(&mut self.inner).map(|inner| inner.get_mut())
    }
}

impl<T> Clone for AtomicCell<T> {
    fn clone(&self) -> Self {
        AtomicCell {
            space: StorageCell::new(),
            inner: self.inner.clone()
        }
    }
}
