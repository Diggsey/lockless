# Lockless

This is an attempt to build useful high-level lock-free data structures,
by designing simple, composable primitives and incrementally building complexity.

Most of the data structures built upon these primitives, are designed
to perform zero allocation during their primary function. Allocation is only performed
during setup, or when cloning handles to the data structures.

This allocation-light design is the primary differentiator from other crates providing
lock-free algorithms, and it means that none of the data structures provided here
are unbounded.


## Modules

### `primitives`

  This module contains simple, low-level building blocks, which can be used in isolation.

  Currently, this contains:
  - `AppendList` - an append-only list, which can be concurrently iterated.
  - `AtomicCell` - a `Cell`-like type, supporting only an atomic `swap` operation.
  - `AtomicExt` - a set of extension methods to simplify working with atomics.
  - `IndexAllocator` - a type which can assign IDs from a contiguous slab.
  - `PrependList` - a prepend-only list, which also supports an atomic `swap` operation.

### `handle`

  This module creates an abstraction for shared ownership of data, where each owner is
  automatically assigned a unique ID.

  There are multiple implementations:
  - `BoundedIdHandle` is completely lock and allocation free, but has a predefined
    limit on the number of concurrent owners. Exceeding this limit will cause a panic.
  - `ResizingIdHandle` wraps the data structure in a `parking_lot::RwLock`. Normal
    usage is still lock and allocation free, but exceeding the maximum number of
    concurrent owners will cause a write-lock to be taken, and the data structure
    to be automatically resized to accomodate the additional owners.

### `containers`

  This module contains medium-level data structures, often based on the `IdHandle`
  abstraction.

  Currently, this contains:
  - `AtomicCell` - an alternative to the primitive `AtomicCell` making slightly different
    trade-offs. It is slightly slower (approximately 15% slower in benchmarks), but can be
    composed into other data structures based around the `IdHandle` abstraction.


## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D


## License

MIT OR Apache-2.0
