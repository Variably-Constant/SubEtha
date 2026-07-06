---
weight: 10
---

# `AdaptiveInstance` trait

The contract a primitive satisfies to be registered with the sidecar.

```rust,no_run
pub trait AdaptiveInstance: Send + Sync + 'static {
    fn header(&self) -> &HandshakeHeader;
    fn ring(&self) -> &ObservationRing;
    fn make_policy(&self) -> Box<dyn Policy>;

    /// Called by the sidecar when the policy returns a new strategy
    /// tag. Default implementation: just set the tag on the header.
    fn apply_migration(&self, new_tag: u32) {
        self.header().set_tag(new_tag);
    }
}
```

## Required methods

### `header() -> &HandshakeHeader`

Returns a reference to the instance's `HandshakeHeader`. The sidecar
keeps a `NonNull<HandshakeHeader>` (captured at registration time)
that aliases this address; the `SidecarBox` machinery guarantees
the header stays alive (and at a stable address) until the handle
is dropped.

### `ring() -> &ObservationRing`

Returns a reference to the instance's `ObservationRing`. Same
aliasing contract as `header()`. The sidecar's scan thread calls
`ring().pop()` repeatedly during each scan cycle to drain
observations.

### `make_policy() -> Box<dyn Policy>`

Factory for the policy that governs this instance. Called once at
registration time. Returning `Box<NoMigrationPolicy>` opts the
primitive out of any migration decisions (the sidecar still drains
the observation ring; it just never asks the policy for a tag).

## Optional method

### `apply_migration(&self, new_tag: u32)`

Default implementation: `self.header().set_tag(new_tag)`. PIC-only
update; no data-layout migration.

Primitives that need a heavy migration (data-layout swap)
override this to perform the swap, then update the tag. The
substrate ships `MigrationGuard` in `subetha_core::migration`
to bracket the swap so readers entering the old layout drain
before the old layout is freed. Example shape:

```rust,no_run
fn apply_migration(&self, new_tag: u32) {
    let strategy = MyStrategy::from_u32(new_tag);
    let new_payload = self.build_payload_for(strategy);
    // begin() bumps the generation and installs new_tag atomically
    // (both representations are now live).
    let guard = MigrationGuard::begin(self.header(), new_tag);
    self.install_new_payload(new_payload);
    guard.wait_quiescent();           // drains the old generation's readers
    // after wait_quiescent returns, the old payload is safe to free.
}
```

`MigrationGuard::begin` does the bump-generation + tag-swap in one
step (so the new PIC branch target is already published when it
returns); `wait_quiescent` blocks until in-flight readers on the old
generation have drained, after which the coordinator owns the old
payload exclusively and can free it. The guard does *not* auto-drain on
`Drop` - the explicit `wait_quiescent` call is required - and no
separate `set_tag` call is needed. How the primitive installs the new
payload is up to it: a `Vec<u8>` swap, a `Box<dyn Strategy>` swap, or
anything else the reader side can observe by reading the generation.

## Bounds

- **`Send + Sync`**: the sidecar's scan thread (different from
  whichever thread owns the primitive) calls `apply_migration` on
  the instance via the raw `*const dyn AdaptiveInstance` it captured
  at registration. Both bounds are required for that call to be
  sound.
- **`'static`**: the sidecar's registry holds the raw pointer until
  `unregister`. The lifetime cannot be reflected in the registry's
  storage type, so the trait requires `'static`. `SidecarBox` boxes
  the value to give it a heap address that is stable for the box's
  lifetime; the `Drop` ordering (handle drops first, blocking on
  scan, then inner drops) prevents use-after-free.

## Registering an instance

Three ways:

```rust,no_run
// 1. The high-level wrapper. Recommended.
//    For MMF primitives (`subetha-cxc`) whose constructors
//    return Result<Self>, call SidecarBox::new explicitly:
let m = SidecarBox::new(
    SharedHashMap::<u32, u64>::create("/tmp/m.bin", 1024).unwrap()
);

// 2. The raw registration. Useful when SidecarBox doesn't fit the
//    ownership story (e.g., a custom AdaptiveInstance impl that
//    lives behind an Arc<MyPrim> from construction).
let arc: Arc<MyCustomPrim> = MyCustomPrim::new_arc();
let header = NonNull::from(arc.header());
let ring = NonNull::from(arc.ring());
let policy = arc.make_policy();
let instance_ptr: *const dyn AdaptiveInstance = &*arc;
let id = unsafe {
    global().register_raw(
        header, ring,
        Some(NonNull::new_unchecked(instance_ptr as *mut _)),
        policy,
    )
};
// ... use arc ...
global().unregister(id);  // BEFORE the last Arc drops!

// 3. Stats-only (no migration). Pass None for the instance pointer.
//    apply_migration is never called for this instance.
let id = unsafe {
    global().register_raw(header, ring, None, Box::new(NoMigrationPolicy))
};
```

> [!WARNING]
> **Form 2 puts the lifetime invariant on you.** `unregister` must
> happen before the underlying memory is freed. `SidecarBox` exists
> precisely so the common case does not require manual ordering.

## See also

- [`SidecarBox<T>`](sidecar-box.md) - the RAII wrapper.
- [`Policy`](policy.md) - what `make_policy()` returns.
- [`HandshakeHeader`](../subetha-core/handshake.md) - what `header()`
  returns.
- [`ObservationRing`](../subetha-core/observation.md) - what `ring()`
  returns.
