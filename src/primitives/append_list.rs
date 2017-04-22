/// AppendList is a low-level primitive supporting two safe operations:
/// `push`, which appends a node to the list, and `iter` which iterates the list
/// The list cannot be shrunk whilst in use.

use std::sync::atomic::{AtomicPtr, Ordering};
use std::{ptr, mem};

type NodePtr<T> = Option<Box<Node<T>>>;

#[derive(Debug)]
struct Node<T> {
    value: T,
    next: AppendList<T>
}

#[derive(Debug)]
pub struct AppendList<T>(AtomicPtr<Node<T>>);

impl<T> AppendList<T> {
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

    fn new_internal(ptr: NodePtr<T>) -> Self {
        AppendList(AtomicPtr::new(Self::into_raw(ptr)))
    }

    pub fn new() -> Self {
        Self::new_internal(None)
    }

    pub fn append(&self, value: T) {
        self.append_list(AppendList::new_internal(Some(Box::new(Node {
            value: value,
            next: AppendList::new()
        }))));
    }

    unsafe fn append_ptr(&self, p: *mut Node<T>) {
        loop {
            match self.0.compare_exchange_weak(ptr::null_mut(), p, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(head) => if !head.is_null() {
                    return (*head).next.append_ptr(p);
                }
            }
        }
    }

    pub fn append_list(&self, other: AppendList<T>) {
        let p = other.0.load(Ordering::Acquire);
        mem::forget(other);
        unsafe { self.append_ptr(p) };
    }

    pub fn iter(&self) -> AppendListIterator<T> {
        AppendListIterator(&self.0)
    }
}

impl<'a, T> IntoIterator for &'a AppendList<T> {
    type Item = &'a T;
    type IntoIter = AppendListIterator<'a, T>;

    fn into_iter(self) -> AppendListIterator<'a, T> {
        self.iter()
    }
}

impl<T> Drop for AppendList<T> {
    fn drop(&mut self) {
        unsafe { Self::from_raw(mem::replace(self.0.get_mut(), ptr::null_mut())) };
    }
}

#[derive(Debug)]
pub struct AppendListIterator<'a, T: 'a>(&'a AtomicPtr<Node<T>>);

impl<'a, T: 'a> Iterator for AppendListIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        let p = self.0.load(Ordering::Acquire);
        if p.is_null() {
            None
        } else {
            unsafe {
                self.0 = &(*p).next.0;
                Some(&(*p).value)
            }
        }
    }
}
