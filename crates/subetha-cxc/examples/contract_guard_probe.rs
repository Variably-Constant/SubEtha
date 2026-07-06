//! Ring-contract legality layer - E2E.
//!
//! Demonstrates the jobs of a [`RingContract`] against the live
//! adaptive ring + sidecar, with running-binary effects observed:
//!
//!  1. **Unified attach check.** A contract with `max_concurrent_push`
//!     tighter than the ring's sizing rejects the over-limit producer
//!     with `TooManyProducers`, derived from the contract.
//!  2. **Feasible-region filter (adversarial).** Under a `Fifo`
//!     contract the sidecar's count-based policy wants the sharded
//!     `Mpmc` shape (which reorders across producers); the contract
//!     refuses it and the ring auto-morphs to the order-preserving
//!     `Vyukov` instead. A control arm with no contract morphs to
//!     `Mpmc` as usual.
//!  3. **Feasible-region filter (enablement).** Under a
//!     `FifoPerProducer` contract the sharded `Mpmc` IS legal, so the
//!     ring auto-morphs straight to it.
//!  4. **Cross-process admission contract.** A second OS process
//!     registers producers against a shared admission counter bounded
//!     by the contract's `max_concurrent_push`; the (k+1)th GLOBAL
//!     producer is rejected across the process boundary.
//!
//! Run:
//!     cargo run --release --example contract_guard_probe -p subetha-cxc

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::{
    AdaptiveRing, AdaptiveRingSidecar, DefaultRingShapePolicy,
};
use subetha_cxc::ring_contract::{RingContract, OrderingContract};
use subetha_cxc::{RingShape, SharedAtomicU64};

const CAP: usize = 256;

fn fifo(push: u8, pop: u8) -> RingContract {
    RingContract { max_concurrent_push: push, max_concurrent_pop: pop,
                ordering: OrderingContract::Fifo, capacity_bound: None }
}
fn per_producer(push: u8, pop: u8) -> RingContract {
    RingContract { max_concurrent_push: push, max_concurrent_pop: pop,
                ordering: OrderingContract::FifoPerProducer, capacity_bound: None }
}

fn wait_for_shape(ring: &AdaptiveRing, target: RingShape, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while ring.current_shape() != target && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Register 4P/4C, wait for the sidecar to reach `expect`, and report
/// the shape it landed on plus a round-trip integrity check on it.
fn run_morph_arm(label: &str, contract: Option<RingContract>, expect: RingShape) -> RingShape {
    let mut ring = AdaptiveRing::create_anon(4, 4, CAP).expect("create");
    if let Some(g) = contract {
        ring = ring.with_contract(g);
    }
    let ring = Arc::new(ring);
    let sidecar = AdaptiveRingSidecar::spawn(
        Arc::clone(&ring),
        DefaultRingShapePolicy { hysteresis: Duration::from_millis(15) },
        Duration::from_millis(5),
    );
    let _p: Vec<_> = (0..4).map(|_| ring.register_producer().expect("p")).collect();
    let _c: Vec<_> = (0..4).map(|_| ring.register_consumer().expect("c")).collect();

    // The sidecar walks SPSC -> ... -> its contract-legal target.
    wait_for_shape(&ring, expect, Duration::from_secs(3));
    let shape = ring.current_shape();

    // Round-trip a batch on whatever shape it settled on, producer and
    // consumer CONCURRENT (the ring is smaller than the batch, so a
    // serial fill-then-drain would deadlock on a full ring). Producer 0's
    // lane is in consumer 0's drain subset under the round-robin.
    let n = 2000u64;
    let rp = Arc::clone(&ring);
    let producer = thread::spawn(move || {
        let mut payload = [0u8; 16];
        for i in 0..n {
            payload[..8].copy_from_slice(&i.to_le_bytes());
            while rp.try_send(0, &payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    let mut out = [0u8; 64];
    let mut got = 0u64;
    let deadline = Instant::now() + Duration::from_secs(5);
    while got < n {
        if ring.try_recv(0, &mut out).is_ok() {
            got += 1;
        } else if Instant::now() > deadline {
            break;
        } else {
            std::hint::spin_loop();
        }
    }
    producer.join().unwrap();
    sidecar.shutdown();
    println!("  {label:<22} -> shape {shape:?}, round-tripped {got}/{n}");
    assert_eq!(got, n, "{label}: the contract-selected shape must carry data");
    shape
}

/// Cross-process child: register producers against the shared admission
/// counter, enforcing the contract's max_concurrent_push GLOBALLY.
fn admission_child(counter_path: &str, max_push: u8, attempts: usize) {
    let counter = SharedAtomicU64::open(counter_path).expect("open counter");
    let contract = RingContract {
        max_concurrent_push: max_push, max_concurrent_pop: 0,
        ordering: OrderingContract::Unordered, capacity_bound: None,
    };
    let mut admitted = 0;
    let mut rejected = 0;
    for _ in 0..attempts {
        let cur = counter.fetch_add(1, Ordering::AcqRel);
        if contract.permits_producer(cur as usize) {
            admitted += 1;
        } else {
            counter.fetch_sub(1, Ordering::AcqRel);
            rejected += 1;
        }
    }
    println!("child(pid {}): {admitted} admitted, {rejected} rejected (global limit {max_push})",
             std::process::id());
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--admission-child") {
        admission_child(&argv[2], argv[3].parse().unwrap(), argv[4].parse().unwrap());
        return;
    }

    println!("operation-contract legality layer - E2E\n");

    // Unified attach check: a contract tighter than the ring's sizing
    // (push <= 3 on a ring built for 8) rejects the 4th producer.
    println!("[1/4] attach check: contract caps producers at 3 on a ring sized for 8");
    let ring = AdaptiveRing::create_anon(8, 8, CAP).expect("create")
        .with_contract(RingContract {
            max_concurrent_push: 3, max_concurrent_pop: 8,
            ordering: OrderingContract::Unordered, capacity_bound: None,
        });
    for i in 0..3 {
        ring.register_producer().unwrap_or_else(|_| panic!("producer {i} should register"));
    }
    let rejected = ring.register_producer();
    println!("    registered 3 producers OK; 4th -> {rejected:?}");
    assert!(rejected.is_err(), "4th producer must be rejected by the contract");

    // Adversarial filter: under Fifo the count-based policy wants the
    // sharded Mpmc, but the contract steers the auto-morph to Vyukov.
    println!("\n[2/4] adversarial: 4P/4C; policy wants Mpmc; contract should steer the morph");
    let fifo_shape = run_morph_arm("Fifo contract", Some(fifo(8, 8)), RingShape::Vyukov);
    let control_shape = run_morph_arm("no contract (control)", None, RingShape::Mpmc);
    assert_eq!(fifo_shape, RingShape::Vyukov,
               "Fifo contract must steer the auto-morph to order-preserving Vyukov");
    assert_eq!(control_shape, RingShape::Mpmc,
               "no contract morphs to the sharded Mpmc default");

    // Enablement filter: FifoPerProducer permits the sharded Mpmc.
    println!("\n[3/4] enablement: FifoPerProducer permits the sharded Mpmc");
    let pp_shape = run_morph_arm("FifoPerProducer contract", Some(per_producer(8, 8)), RingShape::Mpmc);
    assert_eq!(pp_shape, RingShape::Mpmc,
               "FifoPerProducer must permit the sharded Mpmc morph");

    // Cross-process admission: a child process registers producers
    // against a shared counter bounded by the contract's push limit.
    println!("\n[4/4] cross-process admission: global producer limit 3, child attempts 6");
    let counter_path = std::env::temp_dir()
        .join(format!("subetha_contract_admit_{}", std::process::id()));
    let counter = SharedAtomicU64::create(&counter_path, 0).expect("create counter");
    let self_exe = std::env::current_exe().expect("exe");
    let status = std::process::Command::new(&self_exe)
        .arg("--admission-child")
        .arg(counter_path.display().to_string())
        .arg("3")
        .arg("6")
        .status()
        .expect("spawn child");
    assert!(status.success());
    let final_count = counter.load(Ordering::Acquire);
    println!("    shared admission counter settled at {final_count} (== the limit 3)");
    assert_eq!(final_count, 3, "global admission must hold exactly the contract's limit");
    std::fs::remove_file(&counter_path).ok();

    println!("\nall checks passed: the contract unified the attach check, steered the");
    println!("auto-morph inside the declared ordering envelope, and enforced a");
    println!("cross-process producer admission limit.");
}
