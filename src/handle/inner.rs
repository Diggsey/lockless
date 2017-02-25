use std::marker::PhantomData;

use primitives::invariant::Invariant;

use super::ids::IdAllocator;
use super::core::Handle;

pub trait ContainerInner<Tag> {
    fn raise_id_limit(&mut self, new_limit: usize);
    fn id_limit(&self) -> usize;
}

pub trait HandleInnerBase {
    type ContainerInner;

    fn new<H: Handle<HandleInner=Self>>(container: Self::ContainerInner) -> H where Self: Sized;
    fn inner(&self) -> &Self::ContainerInner;
}

pub trait HandleInner<Tag>: HandleInnerBase {
    type IdAllocator: IdAllocator<Tag>;

    fn id_allocator(&self) -> &Self::IdAllocator;
    fn raise_id_limit(&mut self, new_limit: usize);
}

pub trait Distinct {}

macro_rules! declare_tags {
    // End of list, all items in result or omitted entirely
    (@perms ($($res:ty,)*) ()) => {
        impl Distinct for ($($res,)*) {}
    };
    // End of recursion, but some items not used, do nothing
    (@perms ($($res:ty,)*) ($($pre:ty,)+)) => {};
    // Main recursion
    (@perms ($($res:ty,)*) ($($pre:ty,)*) $cur:ty, $($post:ty,)*) => {
        // Recurse along list
        declare_tags!(@perms ($($res,)*) ($($pre,)* $cur,) $($post,)*);
        // Recurse with current as next item in result
        declare_tags!(@perms ($($res,)* $cur,) () $($pre,)* $($post,)*);
    };
    (@subsets ($($res:ty,)*)) => {
        // Take every permutation of the subset
        declare_tags!(@perms () () $($res,)*);
    };
    (@subsets ($($res:ty,)*) $head:ty, $($tail:ty,)*) => {
        // Recurse with element in subset
        declare_tags!(@subsets ($($res,)* $head,) $($tail,)*);
        // Recurse without element in subset
        declare_tags!(@subsets ($($res,)*) $($tail,)*);
    };
    ($($t:ident),*) => {
        $(
            #[derive(Debug)]
            pub struct $t;
        )*
        // Find every permutation of list
        declare_tags!(@subsets () $($t,)*);
    };
}

declare_tags!(Tag0, Tag1, Tag2, Tag3);

pub struct HandleInner1<Tag0, IdAlloc0, C> {
    container: C,
    id_alloc0: IdAlloc0,
    phantom: Invariant<(Tag0,)>,
}

impl<Tag0, IdAlloc0: IdAllocator<Tag0>, C: ContainerInner<Tag0>> HandleInnerBase for HandleInner1<Tag0, IdAlloc0, C> {
    type ContainerInner = C;
    fn new<H: Handle<HandleInner=Self>>(container: C) -> H {
        let limit0 = <C as ContainerInner<Tag0>>::id_limit(&container);
        Handle::new(HandleInner1 {
            container: container,
            id_alloc0: IdAlloc0::new(limit0),
            phantom: PhantomData
        })
    }
    fn inner(&self) -> &Self::ContainerInner {
        &self.container
    }
}

impl<Tag0, IdAlloc0: IdAllocator<Tag0>, C: ContainerInner<Tag0>> HandleInner<Tag0> for HandleInner1<Tag0, IdAlloc0, C> {
    type IdAllocator = IdAlloc0;
    fn id_allocator(&self) -> &Self::IdAllocator {
        &self.id_alloc0
    }
    fn raise_id_limit(&mut self, new_limit: usize) {
        self.container.raise_id_limit(new_limit);
        self.id_alloc0.raise_id_limit(new_limit);
    }
}

pub struct HandleInner2<Tag0, IdAlloc0: IdAllocator<Tag0>, Tag1, IdAlloc1: IdAllocator<Tag1>, C> {
    container: C,
    id_alloc0: IdAlloc0,
    id_alloc1: IdAlloc1,
    phantom: Invariant< (Tag0, Tag1)>,
}

impl<Tag0, IdAlloc0: IdAllocator<Tag0>, Tag1, IdAlloc1: IdAllocator<Tag1>, C: ContainerInner<Tag0> + ContainerInner<Tag1>> HandleInnerBase for HandleInner2<Tag0, IdAlloc0, Tag1, IdAlloc1, C> {
    type ContainerInner = C;
    fn new<H: Handle<HandleInner=Self>>(container: C) -> H {
        let limit0 = <C as ContainerInner<Tag0>>::id_limit(&container);
        let limit1 = <C as ContainerInner<Tag1>>::id_limit(&container);
        Handle::new(HandleInner2 {
            container: container,
            id_alloc0: IdAlloc0::new(limit0),
            id_alloc1: IdAlloc1::new(limit1),
            phantom: PhantomData
        })
    }
    fn inner(&self) -> &Self::ContainerInner {
        &self.container
    }
}

impl<Tag0, IdAlloc0: IdAllocator<Tag0>, Tag1, IdAlloc1: IdAllocator<Tag1>, C: ContainerInner<Tag0> + ContainerInner<Tag1>> HandleInner<Tag0> for HandleInner2<Tag0, IdAlloc0, Tag1, IdAlloc1, C> where (Tag0, Tag1): Distinct {
    type IdAllocator = IdAlloc0;
    fn id_allocator(&self) -> &Self::IdAllocator {
        &self.id_alloc0
    }
    fn raise_id_limit(&mut self, new_limit: usize) {
        ContainerInner::<Tag0>::raise_id_limit(&mut self.container, new_limit);
        self.id_alloc0.raise_id_limit(new_limit);
    }
}

impl<Tag0, IdAlloc0: IdAllocator<Tag0>, Tag1, IdAlloc1: IdAllocator<Tag1>, C: ContainerInner<Tag0> + ContainerInner<Tag1>> HandleInner<Tag1> for HandleInner2<Tag0, IdAlloc0, Tag1, IdAlloc1, C> where (Tag0, Tag1): Distinct {
    type IdAllocator = IdAlloc1;
    fn id_allocator(&self) -> &Self::IdAllocator {
        &self.id_alloc1
    }
    fn raise_id_limit(&mut self, new_limit: usize) {
        ContainerInner::<Tag1>::raise_id_limit(&mut self.container, new_limit);
        self.id_alloc1.raise_id_limit(new_limit);
    }
}
