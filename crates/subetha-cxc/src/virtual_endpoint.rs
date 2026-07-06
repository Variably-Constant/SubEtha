//! `VirtualEndpoint`: substrate-level endpoint identity that resolves
//! to either a local `LocaleAdaptiveRing` or a remote-via-QUIC
//! target at runtime.
//!
//! The substrate's local data path is mmap-backed rings; the
//! cross-host extension is the QUIC bridge. Today callers know
//! which they're talking to at construction time: they hold an
//! `Arc<LocaleAdaptiveRing>` for local peers and configure a
//! `QuicBridgeClient` for remote peers. `VirtualEndpoint` unifies
//! the two behind one identifier so application code calls
//! `endpoint.send(payload)` without grepping config for "is this
//! peer local or remote".
//!
//! # Registry model
//!
//! Each substrate process has one in-process
//! `VirtualEndpointRegistry` that maps `EndpointId -> EndpointTarget`.
//! The default registry is a process-global `OnceLock`. Callers
//! that want a custom lifecycle (per-test isolation, per-tenant
//! routing) construct their own registry and pass `&registry` to
//! the endpoint constructors.
//!
//! `EndpointTarget` is an enum: `Local(Arc<LocaleAdaptiveRing>)` or
//! `Remote(RemoteEndpoint)`. The remote variant holds the address +
//! optional bridge handle and is wired up when QUIC support is
//! enabled.
//!
//! # Pin protocol
//!
//! `VirtualEndpoint::pin_current_target()` returns a
//! `PinnedEndpoint<'_>` that captures the active target at pin time.
//! A subsequent `registry.rebind(endpoint_id, new_target)` bumps the
//! registry's generation counter; the pin sees
//! `is_still_valid() == false` and the holder re-acquires.
//!
//! For local targets, `PinnedEndpoint::as_local()` returns
//! `&LocaleAdaptiveRing`; the caller chains directly into the
//! existing locale-axis pin (`pin_current_locale()`) and from there
//! into the shape-axis pin. The full chain reaches the native
//! primitive through three Acquire loads, one per axis level.

use std::cell::Cell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::locale_adaptive_ring::LocaleAdaptiveRing;

/// Application-supplied identifier for a virtual endpoint. Opaque
/// to the substrate; the registry maps it to a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointId(pub u64);

/// What a virtual endpoint resolves to at runtime.
#[derive(Clone)]
pub enum EndpointTarget {
    /// Local target: bytes flow through a `LocaleAdaptiveRing` on
    /// this host. No network involved.
    Local(Arc<LocaleAdaptiveRing>),
    /// Remote target: bytes flow over the wire to another host's
    /// substrate instance. The address tells the QUIC bridge where
    /// to connect; the substrate does not require QUIC to be
    /// enabled for the local path to work.
    Remote(RemoteEndpoint),
}

/// Describes a remote substrate endpoint reachable over the network.
#[derive(Clone, Debug)]
pub struct RemoteEndpoint {
    /// Server address of the remote substrate's QUIC bridge.
    pub server_addr: SocketAddr,
    /// SNI / server name for TLS validation.
    pub server_name: String,
}

/// In-process registry mapping `EndpointId` to `EndpointTarget`.
/// Each rebind bumps the registry-wide generation counter so
/// pinned-endpoint holders see invalidation.
pub struct VirtualEndpointRegistry {
    table: RwLock<HashMap<EndpointId, EndpointTarget>>,
    /// Bumped on every successful bind / rebind / unbind.
    generation: AtomicU64,
}

impl VirtualEndpointRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self {
            table: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// Insert or replace the target for `id`. Bumps the generation.
    pub fn bind(&self, id: EndpointId, target: EndpointTarget) {
        let mut table = self.table.write().expect("registry table poisoned");
        table.insert(id, target);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Remove the binding for `id`, returning the prior target if
    /// any. Bumps the generation.
    pub fn unbind(&self, id: EndpointId) -> Option<EndpointTarget> {
        let mut table = self.table.write().expect("registry table poisoned");
        let prior = table.remove(&id);
        if prior.is_some() {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
        prior
    }

    /// Look up the target for `id`. Returns a clone so the caller
    /// does not hold the registry lock across awaits.
    pub fn lookup(&self, id: EndpointId) -> Option<EndpointTarget> {
        let table = self.table.read().expect("registry table poisoned");
        table.get(&id).cloned()
    }

    /// Current generation. Pinned endpoints compare captured
    /// generation against this on `is_still_valid()`.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Number of bound endpoints.
    pub fn len(&self) -> usize {
        self.table.read().expect("registry table poisoned").len()
    }

    /// True when no endpoints are bound.
    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

impl Default for VirtualEndpointRegistry {
    fn default() -> Self { Self::new() }
}

/// A virtual endpoint handle. Holds a reference to the registry +
/// the endpoint id. Application code calls `pin_current_target()`
/// to get a target snapshot, then dispatches on
/// `as_local()` / `as_remote()` to do work.
pub struct VirtualEndpoint {
    registry: Arc<VirtualEndpointRegistry>,
    id: EndpointId,
}

impl VirtualEndpoint {
    /// Bind a fresh endpoint and return a handle to it.
    pub fn bind(
        registry: Arc<VirtualEndpointRegistry>,
        id: EndpointId,
        target: EndpointTarget,
    ) -> Self {
        registry.bind(id, target);
        Self { registry, id }
    }

    /// Construct a handle pointing at an already-bound endpoint.
    /// Returns `None` if no binding exists.
    pub fn attach(
        registry: Arc<VirtualEndpointRegistry>,
        id: EndpointId,
    ) -> Option<Self> {
        registry.lookup(id)?;
        Some(Self { registry, id })
    }

    /// The endpoint's id.
    pub fn id(&self) -> EndpointId { self.id }

    /// Capture the current target and return a pinned handle.
    /// Returns `None` if the endpoint has been unbound.
    pub fn pin_current_target(&self) -> Option<PinnedEndpoint<'_>> {
        let captured_gen = self.registry.generation();
        let target = self.registry.lookup(self.id)?;
        Some(PinnedEndpoint {
            registry: &self.registry,
            id: self.id,
            pinned_generation: captured_gen,
            target,
            _not_sync: PhantomData,
        })
    }
}

/// Pinned snapshot of a virtual endpoint's target. Captures the
/// registry generation at pin time; one Acquire load on
/// `is_still_valid()` detects rebinds.
pub struct PinnedEndpoint<'a> {
    registry: &'a VirtualEndpointRegistry,
    id: EndpointId,
    pinned_generation: u64,
    target: EndpointTarget,
    _not_sync: PhantomData<Cell<()>>,
}

impl<'a> PinnedEndpoint<'a> {
    /// Endpoint id this pin was captured for.
    pub fn id(&self) -> EndpointId { self.id }

    /// Generation captured at pin time.
    pub fn pinned_generation(&self) -> u64 { self.pinned_generation }

    /// One Acquire load on the registry's generation counter.
    /// Returns `true` while the pin is current; `false` if any
    /// `bind` / `unbind` has happened on ANY endpoint in the
    /// registry since pin time.
    ///
    /// The coarse-grained check is intentional: a registry-wide
    /// generation is one atomic load per check, vs per-endpoint
    /// generations which require lookup + dereference. Callers
    /// that pin many endpoints in a tight loop trade some
    /// false-positive re-acquires for a much cheaper validity check.
    pub fn is_still_valid(&self) -> bool {
        self.registry.generation() == self.pinned_generation
    }

    /// Returns `Some(&Arc<LocaleAdaptiveRing>)` when the captured
    /// target is local, `None` otherwise. The Arc is borrowed from
    /// the pinned target snapshot and lives for the pin's lifetime.
    pub fn as_local(&self) -> Option<&Arc<LocaleAdaptiveRing>> {
        match &self.target {
            EndpointTarget::Local(ring) => Some(ring),
            _ => None,
        }
    }

    /// Returns `Some(&RemoteEndpoint)` when the captured target is
    /// remote, `None` otherwise.
    pub fn as_remote(&self) -> Option<&RemoteEndpoint> {
        match &self.target {
            EndpointTarget::Remote(remote) => Some(remote),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("ve_{pid}_{nonce}_{name}"));
        p
    }

    fn local_target(name: &str) -> EndpointTarget {
        let ring = Arc::new(
            LocaleAdaptiveRing::create(tmp(name), 1, 1, 64)
                .expect("locale ring create"),
        );
        ring.register_producer().expect("p");
        ring.register_consumer().expect("c");
        EndpointTarget::Local(ring)
    }

    #[test]
    fn bind_then_lookup() {
        let registry = Arc::new(VirtualEndpointRegistry::new());
        let id = EndpointId(42);
        let target = local_target("bind");
        registry.bind(id, target);
        assert!(matches!(
            registry.lookup(id),
            Some(EndpointTarget::Local(_)),
        ));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn rebind_bumps_generation() {
        let registry = Arc::new(VirtualEndpointRegistry::new());
        let id = EndpointId(7);
        let gen_before = registry.generation();
        registry.bind(id, local_target("rebind_a"));
        let gen_after_first = registry.generation();
        assert!(gen_after_first > gen_before);
        registry.bind(id, local_target("rebind_b"));
        assert!(registry.generation() > gen_after_first);
    }

    #[test]
    fn pin_invalidates_on_rebind() {
        let registry = Arc::new(VirtualEndpointRegistry::new());
        let id = EndpointId(99);
        let endpoint = VirtualEndpoint::bind(
            registry.clone(), id, local_target("pin_inv_a"),
        );
        let pin = endpoint.pin_current_target().expect("pinned");
        assert!(pin.is_still_valid());
        assert!(pin.as_local().is_some());
        assert!(pin.as_remote().is_none());

        // Rebind to a different target; pin must invalidate.
        registry.bind(id, local_target("pin_inv_b"));
        assert!(!pin.is_still_valid(),
                "pin must invalidate after rebind");

        // Re-acquire reaches the new target.
        let pin2 = endpoint.pin_current_target().expect("re-pinned");
        assert!(pin2.is_still_valid());
    }

    #[test]
    fn pin_invalidates_on_unbind() {
        let registry = Arc::new(VirtualEndpointRegistry::new());
        let id = EndpointId(123);
        let endpoint = VirtualEndpoint::bind(
            registry.clone(), id, local_target("unbind_test"),
        );
        let pin = endpoint.pin_current_target().expect("pinned");
        registry.unbind(id);
        assert!(!pin.is_still_valid());
        assert!(endpoint.pin_current_target().is_none(),
                "after unbind, re-pin returns None");
    }

    #[test]
    fn attach_returns_none_for_unbound() {
        let registry = Arc::new(VirtualEndpointRegistry::new());
        let id = EndpointId(404);
        assert!(VirtualEndpoint::attach(registry, id).is_none());
    }

    #[test]
    fn remote_target_pins_correctly() {
        let registry = Arc::new(VirtualEndpointRegistry::new());
        let id = EndpointId(8080);
        registry.bind(id, EndpointTarget::Remote(RemoteEndpoint {
            server_addr: "127.0.0.1:8080".parse().unwrap(),
            server_name: "remote.example".to_string(),
        }));
        let endpoint = VirtualEndpoint::attach(registry, id)
            .expect("attached");
        let pin = endpoint.pin_current_target().expect("pinned");
        assert!(pin.as_local().is_none());
        let remote = pin.as_remote().expect("remote target");
        assert_eq!(remote.server_name, "remote.example");
    }

    #[test]
    fn pin_chains_into_locale_axis() {
        let registry = Arc::new(VirtualEndpointRegistry::new());
        let id = EndpointId(1);
        let target = local_target("chain");
        registry.bind(id, target);
        let endpoint = VirtualEndpoint::attach(registry, id).expect("attached");

        let pin_endpoint = endpoint.pin_current_target().expect("pinned");
        let ring = pin_endpoint.as_local().expect("local target");
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_anon().expect("anon locale");
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), crate::RingShape::Spsc);
        assert!(pin_endpoint.is_still_valid()
            && pin_locale.is_still_valid()
            && pin_shape.is_still_valid());
    }
}
