//! E2E demonstration of `VirtualEndpoint`: substrate-level endpoint
//! identity that resolves to local or remote at runtime, with the
//! pin protocol composing across the full five-axis chain.
//!
//! Lifecycle:
//!  1. Create a process-local VirtualEndpointRegistry.
//!  2. Bind two endpoints by id: one Local (to a LocaleAdaptiveRing),
//!     one Remote (placeholder address; no QUIC needed for this E2E).
//!  3. Pin the local endpoint and walk the FIVE-axis pin chain:
//!     VirtualEndpoint -> Local -> LocaleAdaptiveRing -> AdaptiveRing
//!     -> PinnedRing -> SpscRingCore. Round-trip items through the
//!     native primitive at the bottom.
//!  4. Rebind the local endpoint to a different LocaleAdaptiveRing.
//!     Pin invalidates. Re-acquire reaches the new target.
//!  5. Verify the remote endpoint resolves to a RemoteEndpoint
//!     struct (no QUIC connection needed; the resolution itself is
//!     the demonstration).
//!  6. Final integrity check.
//!
//! Run with:
//!     cargo run --release --example virtual_endpoint_lifecycle

use std::sync::Arc;
use std::time::Instant;

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::virtual_endpoint::{
    EndpointId, EndpointTarget, RemoteEndpoint, VirtualEndpoint,
    VirtualEndpointRegistry,
};
use subetha_cxc::{LocaleAdaptiveRing, RingShape};

const ITEMS_LOCAL_A: u64 = 5_000;
const ITEMS_LOCAL_B: u64 = 5_000;
const CAPACITY: usize = 128;

fn main() {
    println!("=== VirtualEndpoint five-axis pin chain E2E ===");
    println!();

    let registry = Arc::new(VirtualEndpointRegistry::new());
    let start = Instant::now();
    println!("[init] registry generation = {}, bindings = {}",
             registry.generation(), registry.len());

    let mut produced_count = 0u64;
    let mut produced_sum = 0u64;
    let mut consumed_count = 0u64;
    let mut consumed_sum = 0u64;

    // Two local targets (separate LocaleAdaptiveRings) and one
    // remote target placeholder.
    let local_ring_a = make_local_ring("local_a");
    let local_ring_b = make_local_ring("local_b");
    let remote_target = EndpointTarget::Remote(RemoteEndpoint {
        server_addr: "192.0.2.42:443".parse().unwrap(),
        server_name: "peer-host.example".to_string(),
    });

    let endpoint_local_id = EndpointId(100);
    let endpoint_remote_id = EndpointId(200);

    // ----- stage 1: bind both endpoints -----
    println!();
    println!("[stage 1] bind two endpoints (one Local, one Remote)");
    let endpoint_local = VirtualEndpoint::bind(
        registry.clone(),
        endpoint_local_id,
        EndpointTarget::Local(local_ring_a.clone()),
    );
    let endpoint_remote = VirtualEndpoint::bind(
        registry.clone(),
        endpoint_remote_id,
        remote_target.clone(),
    );
    println!("    registry generation = {}, bindings = {}",
             registry.generation(), registry.len());

    // ----- stage 2: five-axis pin chain via the local endpoint -----
    println!();
    println!("[stage 2] five-axis pin chain through endpoint_local");
    println!("          VirtualEndpoint -> Local -> LocaleAdaptiveRing");
    println!("                          -> AdaptiveRing -> PinnedRing -> SpscRingCore");
    println!("          round-trip {ITEMS_LOCAL_A} u64s through the native primitive at the bottom");
    {
        let pin_endpoint = endpoint_local.pin_current_target()
            .expect("endpoint_local must resolve");
        let ring = pin_endpoint.as_local().expect("local target");
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_anon().expect("anon locale");
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);
        assert!(pin_endpoint.is_still_valid()
            && pin_locale.is_still_valid()
            && pin_shape.is_still_valid());

        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for i in 0..ITEMS_LOCAL_A {
            let payload = i.to_le_bytes();
            while pin_shape.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            produced_count += 1;
            produced_sum += i;
            while pin_shape.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            consumed_sum += u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
        }
        println!("    round-tripped {ITEMS_LOCAL_A} via the five-axis pin chain");
    }

    // ----- stage 3: rebind endpoint_local; pin invalidates -----
    println!();
    println!("[stage 3] rebind endpoint_local to a fresh LocaleAdaptiveRing");
    {
        let pre_pin = endpoint_local.pin_current_target()
            .expect("pre-pin");
        let gen_before = registry.generation();
        registry.bind(
            endpoint_local_id,
            EndpointTarget::Local(local_ring_b.clone()),
        );
        println!("    registry generation: {gen_before} -> {}",
                 registry.generation());
        assert!(!pre_pin.is_still_valid(),
                "pre-pin must invalidate on rebind");
        println!("    pre-pin.is_still_valid() = false (invalidated as expected)");
    }

    // ----- stage 4: re-pin reaches the new target -----
    println!();
    println!("[stage 4] re-pin endpoint_local; reach the new target");
    println!("          round-trip {ITEMS_LOCAL_B} u64s through the rebind target");
    {
        let pin_endpoint = endpoint_local.pin_current_target()
            .expect("re-pin");
        let ring = pin_endpoint.as_local().expect("local target");
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_anon().expect("anon locale");
        let pin_shape = adaptive.pin_current_shape();

        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for i in 0..ITEMS_LOCAL_B {
            let v = i + 0xB000_0000;
            let payload = v.to_le_bytes();
            while pin_shape.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            produced_count += 1;
            produced_sum += v;
            while pin_shape.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            consumed_sum += u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
        }
        println!("    round-tripped {ITEMS_LOCAL_B} via the rebound target");
    }

    // ----- stage 5: remote endpoint resolves correctly -----
    println!();
    println!("[stage 5] verify endpoint_remote resolves to RemoteEndpoint");
    {
        let pin_endpoint = endpoint_remote.pin_current_target()
            .expect("remote pin");
        assert!(pin_endpoint.as_local().is_none());
        let remote = pin_endpoint.as_remote().expect("remote target");
        assert_eq!(remote.server_name, "peer-host.example");
        assert_eq!(remote.server_addr.to_string(), "192.0.2.42:443");
        println!("    remote.server_addr = {}", remote.server_addr);
        println!("    remote.server_name = {}", remote.server_name);
    }

    // ----- result -----
    let elapsed = start.elapsed();
    let final_gen = registry.generation();
    println!();
    println!("=== Result ===");
    println!("  elapsed:                {elapsed:?}");
    println!("  produced count:         {produced_count}");
    println!("  consumed count:         {consumed_count}");
    println!("  produced sum:           {produced_sum}");
    println!("  consumed sum:           {consumed_sum}");
    println!("  registry generation:    0 -> {final_gen}");
    println!("  bindings active:        {}", registry.len());

    assert_eq!(produced_count, consumed_count,
               "INTEGRITY FAIL: count mismatch");
    assert_eq!(produced_sum, consumed_sum,
               "INTEGRITY FAIL: sum mismatch");
    assert!(final_gen >= 3,
            "expected at least 3 bumps (two binds + one rebind)");
    println!("  integrity:              PASS");
    println!("    every item arrived exactly once across both local targets");
    println!("    five-axis pin chain composed cleanly at both Local targets");
    println!("    remote endpoint resolved to RemoteEndpoint without QUIC connection");
    println!("    pin invalidation worked across endpoint rebind");
}

fn make_local_ring(name: &str) -> Arc<LocaleAdaptiveRing> {
    let base_path = std::env::temp_dir()
        .join(format!("ve_e2e_{}_{name}", std::process::id()));
    let ring = Arc::new(
        LocaleAdaptiveRing::create(&base_path, 1, 1, CAPACITY)
            .expect("locale ring create"),
    );
    ring.register_producer().expect("p");
    ring.register_consumer().expect("c");
    ring
}
