/// PrependList is a low-level primitive supporting two safe operations:
/// `push`, which prepends a node to the list, and `swap` which replaces the list with another

use std::sync::atomic::{AtomicPtr, Ordering};
use std::{ptr, mem};

pub type NodePtr<T> = Option<Box<Node<T>>>;

#[derive(Debug)]
pub struct Node<T> {
    pub value: T,
    pub next: NodePtr<T>
}

#[derive(Debug)]
pub struct PrependList<T>(AtomicPtr<Node<T>>);

fn replace_forget<T>(dest: &mut T, value: T) {
    mem::forget(mem::replace(dest, value))
}

impl<T> PrependList<T> {
    fn into_raw(ptr: NodePtr<T>) -> *mut Node<T> {
        match ptr {
            Some(b) => Box::into_raw(b),
            None => ptr::null_mut()
        }
    }
    unsafe fn from_raw(ptr: *mut Node<T>) -> NodePtr<T> {
        if ptr == ptr::null_mut() {
            None
        } else {
            Some(Box::from_raw(ptr))
        }
    }

    pub fn new(ptr: NodePtr<T>) -> Self {
        PrependList(AtomicPtr::new(Self::into_raw(ptr)))
    }
    pub fn swap(&self, ptr: NodePtr<T>) -> NodePtr<T> {
        unsafe {
            Self::from_raw(self.0.swap(Self::into_raw(ptr), Ordering::AcqRel))
        }
    }
    pub fn push(&self, mut node: Box<Node<T>>) {
        let mut current = self.0.load(Ordering::Relaxed);
        loop {
            replace_forget(&mut node.next, unsafe { Self::from_raw(current) });
            match self.0.compare_exchange_weak(current, &mut *node, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    mem::forget(node);
                    return
                },
                Err(p) => current = p
            }
        }
    }
}

impl<T> Drop for PrependList<T> {
    fn drop(&mut self) {
        unsafe { Self::from_raw(self.0.swap(ptr::null_mut(), Ordering::Relaxed)) };
    }
}