use super::inner::HandleInner;

/// This encapsulates the pattern of wrapping a fixed-size lock-free data-structure inside an
/// RwLock, and automatically resizing it when the number of concurrent handles increases.

pub unsafe trait Handle: Sized + Clone {
    type HandleInner;

    fn try_allocate_id<IdType>(&self) -> Option<IdType> where Self::HandleInner: HandleInner<IdType>;
    fn free_id<IdType>(&self, id: IdType) where Self::HandleInner: HandleInner<IdType>;
    fn with<R, F: FnOnce(&Self::HandleInner) -> R>(&self, f: F) -> R;
    fn new(inner: Self::HandleInner) -> Self;
    fn id_limit<IdType>(&self) -> usize where Self::HandleInner: HandleInner<IdType>;
}

// `ResizingHandle`s will automatically allocate themselves an ID, which
// may require resizing the data structure.
#[derive(Debug)]
pub struct IdHandle<H: Handle, IdType> where H::HandleInner: HandleInner<IdType> {
    id: Option<IdType>,
    handle: H
}

impl<H: Handle, IdType> IdHandle<H, IdType> where H::HandleInner: HandleInner<IdType> {
    pub fn try_clone(&self) -> Option<Self> {
        Self::try_new(&self.handle)
    }

    pub fn new(handle: &H) -> Self {
        Self::try_new(handle).expect("Failed to allocate an ID")
    }
    pub fn try_new(handle: &H) -> Option<Self> {
        if let Some(id) = handle.try_allocate_id() {
            Some(IdHandle {
                id: Some(id),
                handle: handle.clone()
            })
        } else {
            None
        }
    }
    pub fn id(&self) -> &IdType {
        self.id.as_ref().expect("Some(id)")
    }
    pub fn with<R, F: FnOnce(&H::HandleInner) -> R>(&self, f: F) -> R {
        self.handle.with(f)
    }
    pub fn with_mut<R, F: FnOnce(&H::HandleInner, &mut IdType) -> R>(&mut self, f: F) -> R {
        let id = self.id.as_mut().expect("Some(id)");
        self.handle.with(move |v| f(v, id))
    }
}

impl<H: Handle, IdType> Clone for IdHandle<H, IdType> where H::HandleInner: HandleInner<IdType> {
    fn clone(&self) -> Self {
        Self::new(&self.handle)
    }
}

impl<H: Handle, IdType> Drop for IdHandle<H, IdType> where H::HandleInner: HandleInner<IdType> {
    fn drop(&mut self) {
        self.handle.free_id(self.id.take().expect("Some(id)"))
    }
}
