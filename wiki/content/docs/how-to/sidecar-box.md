---
weight: 30
---

# Compose primitives via `SidecarBox`

`SidecarBox<T>` is the RAII wrapper around an `AdaptiveInstance`.
It boxes the primitive, registers it with the global sidecar at
construction, and unregisters on drop. For the vast majority of
use cases this is the only registration API you should touch.

This guide covers the patterns where it works, the patterns where
it does not, and the escape hatches when it does not.

## The default pattern

The CXC primitives' constructors are `create(path, capacity)` /
`open(path, capacity)` returning `Result<Self>`. Wrap them in
`SidecarBox::new`:

```rust,no_run
use subetha_cxc::SharedHashMap;
use subetha_sidecar::SidecarBox;

let m = SidecarBox::new(
    SharedHashMap::<u32, u64>::create("/tmp/sessions.bin", 1024).unwrap()
);
m.insert(42, 4242).unwrap();
```

`SidecarBox::new` does three things:

1. Boxes the value so the heap address is stable.
2. Captures `NonNull` pointers to the value's `HandshakeHeader`,
   `ObservationRing`, and trait object.
3. Calls the value's `make_policy()` to get the policy and
   registers everything with `global().register_raw(...)`, storing
   the returned `InstanceId` inside the wrapper.

`Deref<Target = T>` makes the wrapper transparent: `m.insert(42,
4242)` resolves to `SharedHashMap::insert(42, 4242)` via deref.
Trait methods on `AdaptiveInstance` (`header()`, `ring()`,
`make_policy()`, `apply_migration()`) are accessible the same way
as long as `AdaptiveInstance` is in scope.

## Drop order is load-bearing

```rust,no_run
pub struct SidecarBox<T: AdaptiveInstance> {
    // ORDER MATTERS: handle drops before inner.
    handle: SidecarHandle,
    inner: Box<T>,
}
```

Rust drops struct fields in declaration order. `handle` drops
first; its `Drop` calls `sidecar.unregister(id)`, which blocks
until any in-flight scan iteration on the relevant NUMA node
finishes. Then `inner: Box<T>` drops, freeing the header / ring
memory. Reversing the field order lets the box drop while a scan
thread holds pointers into the header. That is a use-after-free.

This invariant is what makes the wrapper safe. So long as you use
`SidecarBox::new(prim)` and let it drop normally, the order is
correct by construction.

## Sharing across threads: `Arc<SidecarBox<T>>`

`SidecarBox<T>` is `Send + Sync` if `T` is. For multi-threaded
access from owning code wrap in `Arc`:

```rust,no_run
use std::sync::Arc;
use subetha_cxc::SharedRWLock;
use subetha_sidecar::SidecarBox;

let lock = Arc::new(SidecarBox::new(
    SharedRWLock::<u64>::create("/tmp/cfg.bin", 0).unwrap()
));

let lock_clone = lock.clone();
std::thread::spawn(move || {
    lock_clone.with_write(|v| *v += 1);
});

lock.with_read(|v| println!("v = {}", v));
```

The `Arc<SidecarBox<T>>` pattern is correct for the common case.
The `SidecarBox`'s drop runs when the last `Arc` drops, the
sidecar gets unregistered, then the primitive memory is freed.
The deref chain `Arc -> SidecarBox -> T` is unambiguous.

> [!IMPORTANT]
> **The `Arc` goes around the `SidecarBox`, not the other way
> around.** Putting the `Arc` inside (as in
> `SidecarBox::new(Arc::new(prim))`) registers the `Arc`'s heap
> address with the sidecar. That is not what you want. The header
> and ring live inside the primitive, not on the `Arc`.

## When `SidecarBox` does not fit: `Arc`-from-construction

`SidecarBox<T>` owns the inner `T` via `Box<T>`. A primitive
that ships an `Arc<Self>`-returning `new` (because its internal
state requires shared ownership from construction) does not
compose with `SidecarBox::new(value)` cleanly.

For that case, use `Sidecar::register_raw` directly:

```rust,no_run
use std::sync::Arc;
use std::ptr::NonNull;
use subetha_sidecar::{AdaptiveInstance, global};

let prim: Arc<MyPrimitive> = MyPrimitive::new_arc();
let header = NonNull::from(prim.header());
let ring = NonNull::from(prim.ring());

let instance_ref: &dyn AdaptiveInstance = &*prim;
let instance_ptr: *const dyn AdaptiveInstance = instance_ref;
let instance = unsafe {
    NonNull::new_unchecked(instance_ptr as *mut dyn AdaptiveInstance)
};

let policy = prim.make_policy();

let id = unsafe {
    global().register_raw(header, ring, Some(instance), policy)
};

// ... use `prim` ...

// CRITICAL: unregister BEFORE the last Arc drops.
global().unregister(id);
drop(prim);
```

The lifetime invariant moves to you: `unregister(id)` must happen
before the underlying memory is freed. If the last `Arc` drops
first, the sidecar still holds a `NonNull<HandshakeHeader>`
pointing at freed memory until the next scan does its read lock.

`unregister` blocks until any in-flight scan finishes, so calling
it before the last `Arc::drop` is sufficient: when `unregister`
returns, no scan thread holds a pointer into the instance, and
the `Arc::drop` that follows is safe.

## When `SidecarBox` does not fit: stats-only registration

Some primitives want sidecar observation (so they show up in
`InstanceStats` and the policy decision flow) but do not want the
sidecar to call `apply_migration` on them. Pass `None` for the
instance pointer and `Box::new(NoMigrationPolicy)`:

```rust,no_run
use subetha_sidecar::{NoMigrationPolicy, global};

let id = unsafe {
    global().register_raw(
        NonNull::from(prim.header()),
        NonNull::from(prim.ring()),
        None,   // no instance pointer = no apply_migration calls
        Box::new(NoMigrationPolicy),
    )
};
```

The scan thread still drains the observation ring and accumulates
`InstanceStats`. The policy still gets called; it just returns
`None` from `decide()`. The instance never gets migrated. This is
the right shape for primitives whose strategy is fixed at
construction (every primitive in the `subetha-cxc` MMF family
defaults to this) or for telemetry-only observers.

## Common pitfalls

**Constructing a SidecarBox-wrapped primitive inside a tight
loop.** The sidecar has a hard cap of 10,000
simultaneously-registered instances (`DEFAULT_MAX_INSTANCES`). A
criterion benchmark that calls `SidecarBox::new(...)` inside
`b.iter(|| { ... })` registers a new instance per iteration,
accumulates registrations across the warm-up and measurement
phases, and panics with a diagnostic when it crosses the cap.

The fix is to lift the construction outside the `b.iter` closure
and reuse the instance:

```rust,no_run
// WRONG: registers a new instance per iteration.
b.iter(|| {
    let m = SidecarBox::new(
        SharedHashMap::<u32, u64>::create("/tmp/m.bin", 1024).unwrap()
    );
    m.insert(black_box(42), black_box(4242)).unwrap();
});

// RIGHT: register once, reuse inside b.iter.
let m = SidecarBox::new(
    SharedHashMap::<u32, u64>::create("/tmp/m.bin", 1024).unwrap()
);
b.iter(|| {
    m.insert(black_box(42), black_box(4242)).unwrap();
});
```

The diagnostic message names the cap, hints at the `b.iter()` /
loop misuse, and suggests `Sidecar::set_max_instances(...)` as the
escape hatch for intentional heavy-registration workloads.

**Wrapping a `SidecarBox<T>` in another `SidecarBox`.** The type
system blocks this directly. `SidecarBox<T>` does not implement
`AdaptiveInstance`, so `SidecarBox::new(SidecarBox::new(prim))`
fails to compile.

**Passing a borrowed reference where ownership is expected.**
`SidecarBox::new` takes the value by move, not by reference. A
primitive that needs to be aliased across threads goes inside an
`Arc` (`Arc::new(SidecarBox::new(prim))`), not behind a `&`.

## See also

- [`SidecarBox<T>`](../reference/subetha-sidecar/sidecar-box.md) -
  the RAII wrapper's API surface.
- [`AdaptiveInstance`](../reference/subetha-sidecar/adaptive-instance.md) -
  the trait `T` must satisfy.
- [Sidecar registry](../reference/subetha-sidecar/registry.md) -
  what `register_raw` and `unregister` do internally.
- [Tune the sidecar](tune-sidecar.md) - raising the instance cap
  and other knobs.
