use super::ids::Id;
use super::inner::{HandleInner, HandleInnerBase};

/// This encapsulates the pattern of wrapping a fixed-size lock-free data-structure inside an
/// RwLock, and automatically resizing it when the number of concurrent handles increases.

pub unsafe trait Handle: Sized + Clone {
    type HandleInner: HandleInnerBase;

    fn try_allocate_id<Tag>(&self) -> Option<Id<Tag>> where Self::HandleInner: HandleInner<Tag>;
    fn free_id<Tag>(&self, id: Id<Tag>) where Self::HandleInner: HandleInner<Tag>;
    fn with<R, F: FnOnce(&<Self::HandleInner as HandleInnerBase>::ContainerInner) -> R>(&self, f: F) -> R;
    fn new(inner: Self::HandleInner) -> Self;
    fn id_limit<Tag>(&self) -> usize where Self::HandleInner: HandleInner<Tag>;
}

// `ResizingHandle`s will automatically allocate themselves an ID, which
// may require resizing the data structure.
#[derive(Debug)]
pub struct IdHandle<Tag, H: Handle> where H::HandleInner: HandleInner<Tag> {
    id: Id<Tag>,
    handle: H
}

impl<Tag, H: Handle> IdHandle<Tag, H> where H::HandleInner: HandleInner<Tag> {
    pub fn try_clone(&self) -> Option<Self> {
        Self::try_new(&self.handle)
    }

    pub fn new(handle: &H) -> Self {
        Self::try_new(handle).expect("Failed to allocate an ID")
    }
    pub fn try_new(handle: &H) -> Option<Self> {
        if let Some(id) = handle.try_allocate_id() {
            Some(IdHandle {
                id: id,
                handle: handle.clone()
            })
        } else {
            None
        }
    }
    pub fn id(&self) -> Id<Tag> {
        self.id
    }
    pub fn with<R, F: FnOnce(&<H::HandleInner as HandleInnerBase>::ContainerInner) -> R>(&self, f: F) -> R {
        self.handle.with(f)
    }
    pub fn with_mut<R, F: FnOnce(&<H::HandleInner as HandleInnerBase>::ContainerInner, Id<Tag>) -> R>(&mut self, f: F) -> R {
        let id = self.id;
        self.handle.with(move |v| f(v, id))
    }
}

impl<Tag, H: Handle> Clone for IdHandle<Tag, H> where H::HandleInner: HandleInner<Tag> {
    fn clone(&self) -> Self {
        Self::new(&self.handle)
    }
}

impl<Tag, H: Handle> Drop for IdHandle<Tag, H> where H::HandleInner: HandleInner<Tag> {
    fn drop(&mut self) {
        self.handle.free_id(self.id)
    }
}
