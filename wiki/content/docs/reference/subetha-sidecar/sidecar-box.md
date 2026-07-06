---
weight: 30
---

# `SidecarBox<T>` - RAII registration

```rust,no_run
pub struct SidecarBox<T: AdaptiveInstance> {
    handle: SidecarHandle,
    inner: Box<T>,
}

impl<T: AdaptiveInstance> SidecarBox<T> {
    pub fn new(value: T) -> Self;
    pub fn id(&self) -> InstanceId;
    pub fn stats(&self) -> Option<InstanceStats>;
}

impl<T: AdaptiveInstance> std::ops::Deref for SidecarBox<T> {
    type Target = T;
}
```

`SidecarBox::new(primitive)` is the one-call way to register a
primitive with the global sidecar and get a value that:

- **Derefs to the primitive.** `sb.method()` calls `T::method()`.
- **Tracks its registration**. `sb.id()` returns the `InstanceId`;
  `sb.stats()` returns a snapshot of the current `InstanceStats`.
- **Auto-unregisters on `Drop`.** The internal `SidecarHandle`
  drops before the inner `Box<T>`, blocking on any in-flight scan
  cycle so the sidecar cannot see freed memory.

## Field order is load-bearing

```rust,no_run
pub struct SidecarBox<T: AdaptiveInstance> {
    // ORDER MATTERS: handle drops before inner.
    handle: SidecarHandle,
    inner: Box<T>,
}
```

Rust drops struct fields in declaration order. The handle's `Drop`
calls `sidecar.unregister(id)`, which:

1. Removes the slot from the registry (subsequent scan iterations
   skip it).
2. Blocks until any currently-executing scan iteration finishes.

By the time `handle`'s drop returns, no scan thread holds a pointer
into the instance. Then `inner: Box<T>` drops, freeing the
header / ring memory. Reversing the field order would let the box
drop while a scan thread was mid-read of the header → use-after-free.

## How it captures pointers

```rust,no_run
pub fn new(value: T) -> Self {
    let inner = Box::new(value);
    let header = NonNull::from(inner.header());
    let ring = NonNull::from(inner.ring());
    let instance_ref: &dyn AdaptiveInstance = &*inner;
    let instance_ptr: *const dyn AdaptiveInstance = instance_ref;
    let instance = unsafe {
        NonNull::new_unchecked(instance_ptr as *mut dyn AdaptiveInstance)
    };
    let policy = inner.make_policy();
    let sidecar = global();
    let id = unsafe { sidecar.register_raw(header, ring, Some(instance), policy) };
    Self {
        handle: SidecarHandle { id, sidecar },
        inner,
    }
}
```

The `Box::new` allocation gives the inner value a stable heap
address. `NonNull::from(&field)` then captures stable interior
pointers. The `Box` lifetime keeps those interior pointers valid
for as long as the `SidecarBox` lives.

## `Deref` makes the wrapper transparent

```rust,no_run
let sb = SidecarBox::new(SharedHashMap::<u32, u32>::create("/tmp/m.bin", 1024).unwrap());
sb.insert(42, 4242).unwrap();    // -> SharedHashMap::insert
sb.get(&42);                       // -> SharedHashMap::get
sb.ring();                         // -> AdaptiveInstance::ring via SharedHashMap impl
```

Calling `sb.method()` resolves via deref to `T`'s inherent method
or trait method. `AdaptiveInstance` itself must be in scope for the
trait methods (`header()`, `ring()`, `make_policy()`,
`apply_migration()`) to be callable via method syntax.

## When `SidecarBox` doesn't fit

`SidecarBox<T>` owns the primitive. For ownership patterns that
need `Arc<T>` instead (multiple owners, cross-thread sharing
without `Send`/`Sync` on the box), use `Sidecar::register_raw`
directly. The lifetime invariant then moves to the caller: call
`Sidecar::unregister(id)` before the last `Arc` drop.

## See also

- [`AdaptiveInstance`](adaptive-instance.md) - the trait `T` must
  implement.
- [Sidecar registry](registry.md) - what `register_raw` and
  `unregister` do internally.
- [Compose primitives via `SidecarBox` how-to](../../how-to/sidecar-box.md)
  for end-to-end patterns.
