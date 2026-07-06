//! Unified throughput bench harness for every ring primitive in
//! the substrate, plus external-library baselines for comparison.
//!
//! Invocation:
//!     bench_throughput <primitive> <locale> <capacity> <n_producers> <n_consumers> <n_items>
//!
//! Output (single line, key=value pairs):
//!     primitive=<name> locale=<l> cap=<c> P=<np> C=<nc> N=<n> elapsed_ms=<e> throughput_M_per_s=<t>
//!
//! On precondition failure the harness emits one line:
//!     primitive=<name> locale=<l> cap=<c> P=<np> C=<nc> N=<n> SKIP=<reason>
//!
//! Primitives supported:
//!   spsc            - SharedRingSpsc (1P/1C Lamport)
//!   mpsc            - SharedRingMpsc (composed N Lamport)
//!   mpsc-fifo       - SharedRingMpscFifo (single Vyukov)
//!   mpmc            - SharedRingMpmc (composed N x M grid)
//!   vyukov          - SharedRing (Vyukov MPMC, global FIFO)
//!   broadcast       - SharedBroadcastRing (1P/NC fan-out)
//!   pubsub          - PubSubRing (1P/NC absolute-position)
//!   adaptive-spsc   - AdaptiveRing in SPSC shape (1P/1C)
//!   adaptive-mpsc   - AdaptiveRing in MPSC shape (NP/1C)
//!   adaptive-mpmc   - AdaptiveRing in MPMC shape (NP/NC)
//!   adaptive-vyukov - AdaptiveRing in Vyukov shape (NP/NC)
//!   locale-adaptive - LocaleAdaptiveRing (any shape any locale)
//!   capacity-spsc   - CapacityAdaptiveRing in SPSC (no morph; steady-state)
//!   capacity-mpsc   - CapacityAdaptiveRing in MPSC
//!   capacity-mpmc   - CapacityAdaptiveRing in MPMC
//!   capacity-vyukov - CapacityAdaptiveRing in Vyukov
//!   capacity-broadcast - CapacityBroadcastRing (no morph)
//!   capacity-pubsub - CapacityPubSubRing (no morph)
//!   crossbeam       - crossbeam-channel bounded (external baseline)
//!   std-mpsc        - std::sync::mpsc::sync_channel (external baseline)
//!
//! Locale (subetha only; crossbeam / std-mpsc ignore locale):
//!   anon | file | shmfs
//!
//! Examples:
//!   bench_throughput spsc anon 4096 1 1 1000000
//!   bench_throughput vyukov shmfs 1024 4 4 1000000
//!   bench_throughput crossbeam - 4096 4 1 1000000

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use subetha_cxc::adaptive_ring::{AdaptiveRing, RingShape};
use subetha_cxc::capacity_adaptive_ring::CapacityAdaptiveRing;
use subetha_cxc::capacity_broadcast_ring::CapacityBroadcastRing;
use subetha_cxc::capacity_pubsub_ring::CapacityPubSubRing;
use subetha_cxc::locale_adaptive_ring::{Locale, LocaleAdaptiveRing};
use subetha_cxc::mpmc_ring::SharedRingMpmc;
use subetha_cxc::mpsc_ring::{SharedRingMpsc, SharedRingMpscFifo};
use subetha_cxc::protocol_pubsub::{PubSubReadError, PubSubRing};
use subetha_cxc::shared_broadcast_ring::SharedBroadcastRing;
use subetha_cxc::shared_ring::{SharedRing, SharedRingSpsc};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        eprintln!(
            "usage: {} <primitive> <locale> <capacity> <n_producers> <n_consumers> <n_items>",
            args[0]
        );
        std::process::exit(2);
    }
    let primitive = args[1].as_str();
    let locale = args[2].as_str();
    let capacity: usize = args[3].parse().expect("capacity must be usize");
    let n_producers: usize = args[4].parse().expect("n_producers must be usize");
    let n_consumers: usize = args[5].parse().expect("n_consumers must be usize");
    let n_items: u64 = args[6].parse().expect("n_items must be u64");

    let result = match primitive {
        "spsc" => bench_spsc(locale, capacity, n_items),
        "mpsc" => bench_mpsc_composed(locale, capacity, n_producers, n_items),
        "mpsc-fifo" => bench_mpsc_fifo(locale, capacity, n_producers, n_items),
        "mpmc" => bench_mpmc_composed(locale, capacity, n_producers, n_consumers, n_items),
        "vyukov" => bench_vyukov(locale, capacity, n_producers, n_consumers, n_items),
        "broadcast" => bench_broadcast(locale, capacity, n_consumers, n_items),
        "pubsub" => bench_pubsub(locale, capacity, n_consumers, n_items),
        "adaptive-spsc" => bench_adaptive(locale, capacity, RingShape::Spsc, 1, 1, n_items),
        "adaptive-mpsc" => bench_adaptive(locale, capacity, RingShape::Mpsc, n_producers, 1, n_items),
        "adaptive-mpmc" => {
            bench_adaptive(locale, capacity, RingShape::Mpmc, n_producers, n_consumers, n_items)
        }
        "adaptive-vyukov" => {
            bench_adaptive(locale, capacity, RingShape::Vyukov, n_producers, n_consumers, n_items)
        }
        "adaptive-pinned-spsc" => bench_adaptive_pinned(locale, capacity, RingShape::Spsc, 1, 1, n_items),
        "adaptive-pinned-mpsc" => bench_adaptive_pinned(locale, capacity, RingShape::Mpsc, n_producers, 1, n_items),
        "adaptive-pinned-mpmc" => bench_adaptive_pinned(locale, capacity, RingShape::Mpmc, n_producers, n_consumers, n_items),
        "adaptive-pinned-vyukov" => bench_adaptive_pinned(locale, capacity, RingShape::Vyukov, n_producers, n_consumers, n_items),
        "locale-adaptive" => bench_locale_adaptive(locale, capacity, n_producers, n_consumers, n_items),
        "capacity-spsc" => {
            bench_capacity_adaptive(locale, capacity, RingShape::Spsc, 1, 1, n_items)
        }
        "capacity-mpsc" => {
            bench_capacity_adaptive(locale, capacity, RingShape::Mpsc, n_producers, 1, n_items)
        }
        "capacity-mpmc" => bench_capacity_adaptive(
            locale, capacity, RingShape::Mpmc, n_producers, n_consumers, n_items,
        ),
        "capacity-vyukov" => bench_capacity_adaptive(
            locale, capacity, RingShape::Vyukov, n_producers, n_consumers, n_items,
        ),
        "capacity-pinned-spsc" => bench_capacity_pinned(locale, capacity, RingShape::Spsc, 1, 1, n_items),
        "capacity-pinned-mpsc" => bench_capacity_pinned(locale, capacity, RingShape::Mpsc, n_producers, 1, n_items),
        "capacity-pinned-mpmc" => bench_capacity_pinned(locale, capacity, RingShape::Mpmc, n_producers, n_consumers, n_items),
        "capacity-pinned-vyukov" => bench_capacity_pinned(locale, capacity, RingShape::Vyukov, n_producers, n_consumers, n_items),
        "capacity-broadcast" => bench_capacity_broadcast(locale, capacity, n_consumers, n_items),
        "capacity-pinned-broadcast" => bench_capacity_broadcast_pinned(locale, capacity, n_consumers, n_items),
        "capacity-pubsub" => bench_capacity_pubsub(locale, capacity, n_consumers, n_items),
        "crossbeam" => bench_crossbeam(capacity, n_producers, n_consumers, n_items),
        "std-mpsc" => bench_std_mpsc(capacity, n_producers, n_items),
        other => {
            eprintln!("unknown primitive: {other}");
            std::process::exit(2);
        }
    };

    emit_result(primitive, locale, capacity, n_producers, n_consumers, n_items, result);
}

enum BenchResult {
    Ok {
        elapsed_ms: f64,
        throughput_m_per_s: f64,
        eff_p: usize,
        eff_c: usize,
        eff_n: u64,
    },
    Skip(String),
}

fn emit_result(
    primitive: &str,
    locale: &str,
    cap: usize,
    req_p: usize,
    req_c: usize,
    req_n: u64,
    r: BenchResult,
) {
    match r {
        BenchResult::Ok {
            elapsed_ms,
            throughput_m_per_s,
            eff_p,
            eff_c,
            eff_n,
        } => {
            println!(
                "primitive={primitive} locale={locale} cap={cap} reqP={req_p} reqC={req_c} reqN={req_n} effP={eff_p} effC={eff_c} effN={eff_n} elapsed_ms={elapsed_ms:.3} throughput_M_per_s={throughput_m_per_s:.3}"
            );
        }
        BenchResult::Skip(reason) => {
            println!(
                "primitive={primitive} locale={locale} cap={cap} reqP={req_p} reqC={req_c} reqN={req_n} SKIP={reason}"
            );
        }
    }
}

fn bench_result_from(t0: Instant, eff_n: u64, eff_p: usize, eff_c: usize) -> BenchResult {
    let elapsed = t0.elapsed();
    BenchResult::Ok {
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        throughput_m_per_s: eff_n as f64 / elapsed.as_secs_f64() / 1_000_000.0,
        eff_p,
        eff_c,
        eff_n,
    }
}

// ============================================================================
// SPSC: SharedRingSpsc (1P/1C Lamport)
// ============================================================================

fn bench_spsc(locale: &str, capacity: usize, n_items: u64) -> BenchResult {
    if locale != "anon" {
        return BenchResult::Skip(format!("locale-{locale}-unsupported"));
    }
    let (producer, consumer) = match SharedRingSpsc::create_anon_pair(capacity) {
        Ok(pc) => pc,
        Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
    };

    let t0 = Instant::now();
    // SPSC Producer + Consumer are !Sync (per-thread tokens by
    // design). Move them directly into their threads rather than
    // wrapping in Arc.
    let producer_h = thread::spawn(move || {
        let payload = [0u8; 56];
        for _ in 0..n_items {
            while producer.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    let consumer_h = thread::spawn(move || {
        let mut buf = [0u8; 64];
        for _ in 0..n_items {
            while consumer.try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    producer_h.join().unwrap();
    consumer_h.join().unwrap();
    bench_result_from(t0, n_items, 1, 1)
}

// ============================================================================
// MPSC composed (SharedRingMpsc - N Lamport rings)
// ============================================================================

fn bench_mpsc_composed(
    locale: &str,
    capacity: usize,
    n_producers: usize,
    n_items: u64,
) -> BenchResult {
    if locale != "anon" {
        return BenchResult::Skip(format!("locale-{locale}-unsupported"));
    }
    let (producers, consumer) = match SharedRingMpsc::create_anon_pool(n_producers, capacity) {
        Ok(pc) => pc,
        Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
    };
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for prod in producers {
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                while prod.try_push(&payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }
    // MpscConsumer is !Sync (per-thread token). Drain on the
    // main thread instead of an Arc-shared consumer thread.
    let mut buf = [0u8; 64];
    let mut drained: u64 = 0;
    while drained < total {
        if consumer.try_pop(&mut buf).is_ok() {
            drained += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, 1)
}

// ============================================================================
// MPSC FIFO (SharedRingMpscFifo - single Vyukov)
// ============================================================================

fn bench_mpsc_fifo(
    locale: &str,
    capacity: usize,
    n_producers: usize,
    n_items: u64,
) -> BenchResult {
    if locale != "anon" {
        return BenchResult::Skip(format!("locale-{locale}-unsupported"));
    }
    let (producers, consumer) = match SharedRingMpscFifo::create_anon_pool(n_producers, capacity) {
        Ok(pc) => pc,
        Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
    };
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for prod in producers {
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                while prod.try_push(&payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }
    // MpscFifoConsumer is !Send (uses a !Send inner field for the
    // FIFO override). Drain on the main thread instead of
    // spawning a consumer thread.
    let mut buf = [0u8; 64];
    let mut drained: u64 = 0;
    while drained < total {
        if consumer.try_pop(&mut buf).is_ok() {
            drained += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, 1)
}

// ============================================================================
// MPMC composed (SharedRingMpmc - N x M grid)
// ============================================================================

fn bench_mpmc_composed(
    locale: &str,
    capacity: usize,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    if locale != "anon" {
        return BenchResult::Skip(format!("locale-{locale}-unsupported"));
    }
    if n_producers < n_consumers {
        return BenchResult::Skip(format!("n_producers-{n_producers}-lt-n_consumers-{n_consumers}"));
    }
    let (producers, consumers) =
        match SharedRingMpmc::create_anon_grid(n_producers, n_consumers, capacity) {
            Ok(pc) => pc,
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        };
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for prod in producers {
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                while prod.try_push(&payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }
    let mut consumer_handles = Vec::new();
    for cons in consumers {
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            loop {
                if cons.try_pop(&mut buf).is_ok() {
                    if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                        return;
                    }
                } else if d.load(Ordering::Relaxed) >= total {
                    return;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

// ============================================================================
// Vyukov MPMC (SharedRing - global FIFO)
// ============================================================================

fn bench_vyukov(
    locale: &str,
    capacity: usize,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let ring: Arc<SharedRing> = match locale {
        "anon" => match SharedRing::create_anon(capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_vyukov_{}.bin", std::process::id()));
            match SharedRing::create(&path, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_vyukov_{}", std::process::id());
            let total_size = subetha_cxc::shared_ring::ring_file_size(capacity);
            let shm = match subetha_cxc::shm_file::ShmFile::create_or_open_named(&name, total_size)
            {
                Ok(s) => s,
                Err(e) => return BenchResult::Skip(format!("shm-create-failed-{e}")),
            };
            match SharedRing::create_from_shm(shm, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for _ in 0..n_producers {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                while r.try_push(&payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }
    let mut consumer_handles = Vec::new();
    for _ in 0..n_consumers {
        let r = Arc::clone(&ring);
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            loop {
                if r.try_pop(&mut buf).is_ok() {
                    if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                        return;
                    }
                } else if d.load(Ordering::Relaxed) >= total {
                    return;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

// ============================================================================
// Broadcast (SharedBroadcastRing - 1P/NC fan-out)
// ============================================================================

fn bench_broadcast(
    locale: &str,
    capacity: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let ring: Arc<SharedBroadcastRing> = match locale {
        "anon" => match SharedBroadcastRing::create_anon(capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_broadcast_{}.bin", std::process::id()));
            match SharedBroadcastRing::create(&path, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_broadcast_{}", std::process::id());
            let total_size = subetha_cxc::shared_broadcast_ring::broadcast_file_size(capacity);
            let shm = match subetha_cxc::shm_file::ShmFile::create_or_open_named(&name, total_size)
            {
                Ok(s) => s,
                Err(e) => return BenchResult::Skip(format!("shm-create-failed-{e}")),
            };
            match SharedBroadcastRing::create_from_shm(shm, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    let consumer_ids: Vec<usize> = match (0..n_consumers).map(|_| ring.register_consumer()).collect()
    {
        Ok(ids) => ids,
        Err(e) => return BenchResult::Skip(format!("register-failed-{e:?}")),
    };

    let t0 = Instant::now();
    let r = Arc::clone(&ring);
    let producer_h = thread::spawn(move || {
        let payload = [0u8; 52];
        for _ in 0..n_items {
            while r.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    let mut consumer_handles = Vec::new();
    for cid in consumer_ids {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            for _ in 0..n_items {
                while r.try_recv(cid, &mut buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        consumer_handles.push(h);
    }
    producer_h.join().unwrap();
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, n_items, 1, n_consumers)
}

// ============================================================================
// PubSub (PubSubRing - 1P/NC absolute-position with KeepAll back-pressure)
// ============================================================================

fn bench_pubsub(
    locale: &str,
    capacity: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let ring: Arc<PubSubRing> = match locale {
        "anon" => match PubSubRing::create_anon(capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_pubsub_{}.bin", std::process::id()));
            match PubSubRing::create(&path, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_pubsub_{}", std::process::id());
            let total_size = subetha_cxc::protocol_pubsub::pubsub_ring_file_size(capacity);
            let shm = match subetha_cxc::shm_file::ShmFile::create_or_open_named(&name, total_size)
            {
                Ok(s) => s,
                Err(e) => return BenchResult::Skip(format!("shm-create-failed-{e}")),
            };
            match PubSubRing::create_from_shm(shm, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };

    // Pre-capture each subscriber's starting position (= 0 on a
    // fresh ring) and let them advance independently. KeepAll
    // back-pressure: producer waits while head reaches capacity
    // so no subscriber loses data.
    let sub_positions: Vec<Arc<AtomicU64>> = (0..n_consumers)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let t0 = Instant::now();
    let r_prod = Arc::clone(&ring);
    let sub_pos_for_prod: Vec<Arc<AtomicU64>> = sub_positions.to_vec();
    let producer_h = thread::spawn(move || {
        let payload = [0u8; 56];
        for i in 0..n_items {
            // Back-pressure: head must stay within capacity slots
            // ahead of the slowest subscriber.
            loop {
                let min_sub = sub_pos_for_prod
                    .iter()
                    .map(|a| a.load(Ordering::Acquire))
                    .min()
                    .unwrap_or(0);
                if i.saturating_sub(min_sub) < capacity as u64 {
                    break;
                }
                std::hint::spin_loop();
            }
            r_prod.publish(&payload);
        }
    });
    let mut consumer_handles = Vec::new();
    for pos_arc in &sub_positions {
        let r = Arc::clone(&ring);
        let pos = Arc::clone(pos_arc);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            let mut p: u64 = 0;
            while p < n_items {
                match r.read_at(p, &mut buf) {
                    Ok(()) => {
                        p += 1;
                        pos.store(p, Ordering::Release);
                    }
                    Err(PubSubReadError::Pending) => std::hint::spin_loop(),
                    Err(PubSubReadError::Lost) => {
                        // Should not happen under back-pressure; if
                        // it does, abort the bench.
                        eprintln!("pubsub bench: subscriber lost at p={p}");
                        return;
                    }
                }
            }
        });
        consumer_handles.push(h);
    }
    producer_h.join().unwrap();
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, n_items, 1, n_consumers)
}

// ============================================================================
// AdaptiveRing (any shape, anon locale)
// ============================================================================

fn bench_adaptive(
    locale: &str,
    capacity: usize,
    shape: RingShape,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let max_p = n_producers.max(1);
    let max_c = n_consumers.max(1);
    let ring: Arc<AdaptiveRing> = match locale {
        "anon" => match AdaptiveRing::create_anon(max_p, max_c, capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_adaptive_{}", std::process::id()));
            match AdaptiveRing::create(&path, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_adaptive_{}", std::process::id());
            match AdaptiveRing::create_shmfs(&name, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    for _ in 0..n_producers {
        if let Err(e) = ring.register_producer() {
            return BenchResult::Skip(format!("register-producer-{e:?}"));
        }
    }
    for _ in 0..n_consumers {
        if let Err(e) = ring.register_consumer() {
            return BenchResult::Skip(format!("register-consumer-{e:?}"));
        }
    }
    if shape != RingShape::Spsc
        && let Err(e) = ring.morph_to(shape)
    {
        return BenchResult::Skip(format!("morph-failed-{e:?}"));
    }
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for pid in 0..n_producers {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                while r.try_send(pid, &payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }
    let mut consumer_handles = Vec::new();
    for cid in 0..n_consumers {
        let r = Arc::clone(&ring);
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            if n_consumers == 1 {
                // Single-consumer fast path: tight loop, no
                // shared-atomic per item. Matches bench_spsc's
                // pattern so apples-to-apples comparison is fair.
                for _ in 0..total {
                    while r.try_recv(cid, &mut buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
            } else {
                // Multi-consumer: shared atomic counter for
                // collective-termination. Relaxed ordering is
                // enough because we only need monotonic-eventually
                // for termination, not happens-before for any
                // payload.
                loop {
                    if r.try_recv(cid, &mut buf).is_ok() {
                        if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                            return;
                        }
                    } else if d.load(Ordering::Relaxed) >= total {
                        return;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

// ============================================================================
// LocaleAdaptiveRing (specify locale at construction; same shape behaviour)
// ============================================================================

fn bench_locale_adaptive(
    locale: &str,
    capacity: usize,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let max_p = n_producers.max(1);
    let max_c = n_consumers.max(1);
    let starting_locale = match locale {
        "anon" => Locale::Anon,
        "file" => Locale::File,
        "shmfs" => Locale::ShmFs,
        other => return BenchResult::Skip(format!("locale-{other}-unknown")),
    };
    let path = std::env::temp_dir()
        .join(format!("bench_locale_adaptive_{}", std::process::id()));
    let ring = match LocaleAdaptiveRing::create(&path, max_p, max_c, capacity) {
        Ok(r) => Arc::new(r),
        Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
    };
    if starting_locale != Locale::Anon
        && let Err(e) = ring.migrate_to(starting_locale)
    {
        return BenchResult::Skip(format!("migrate-locale-failed-{e:?}"));
    }
    for _ in 0..n_producers {
        if let Err(e) = ring.register_producer() {
            return BenchResult::Skip(format!("register-producer-{e:?}"));
        }
    }
    for _ in 0..n_consumers {
        if let Err(e) = ring.register_consumer() {
            return BenchResult::Skip(format!("register-consumer-{e:?}"));
        }
    }
    // Pick the inner-shape based on registration counts. SPSC
    // wedges if more than one producer pushes; we must morph the
    // active backing to MPSC / MPMC explicitly. LocaleAdaptiveRing
    // exposes the per-locale AdaptiveRing via accessors.
    let target_shape = if n_producers > 1 && n_consumers > 1 {
        RingShape::Mpmc
    } else if n_producers > 1 {
        RingShape::Mpsc
    } else {
        RingShape::Spsc
    };
    if target_shape != RingShape::Spsc {
        let active_backing: &AdaptiveRing = match starting_locale {
            Locale::Anon => ring.anon_ring(),
            Locale::File => ring.file_ring(),
            Locale::ShmFs => ring.shmfs_ring(),
        };
        if let Err(e) = active_backing.morph_to(target_shape) {
            return BenchResult::Skip(format!("inner-shape-morph-{e:?}"));
        }
    }
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for pid in 0..n_producers {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                while r.try_send(pid, &payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }
    let mut consumer_handles = Vec::new();
    for cid in 0..n_consumers {
        let r = Arc::clone(&ring);
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            if n_consumers == 1 {
                for _ in 0..total {
                    while r.try_recv(cid, &mut buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
            } else {
                loop {
                    if r.try_recv(cid, &mut buf).is_ok() {
                        if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                            return;
                        }
                    } else if d.load(Ordering::Relaxed) >= total {
                        return;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

// ============================================================================
// CapacityAdaptiveRing (steady-state, no morph)
// ============================================================================

fn bench_capacity_adaptive(
    locale: &str,
    capacity: usize,
    shape: RingShape,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let max_p = n_producers.max(1);
    let max_c = n_consumers.max(1);
    let ring: Arc<CapacityAdaptiveRing> = match locale {
        "anon" => match CapacityAdaptiveRing::create_anon(max_p, max_c, capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_capadapt_{}.bin", std::process::id()));
            match CapacityAdaptiveRing::create(&path, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_capadapt_{}", std::process::id());
            match CapacityAdaptiveRing::create_shmfs(&name, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    for _ in 0..n_producers {
        if let Err(e) = ring.register_producer() {
            return BenchResult::Skip(format!("register-producer-{e:?}"));
        }
    }
    for _ in 0..n_consumers {
        if let Err(e) = ring.register_consumer() {
            return BenchResult::Skip(format!("register-consumer-{e:?}"));
        }
    }
    if shape != RingShape::Spsc
        && let Err(e) = ring.ring_handle().morph_to(shape)
    {
        return BenchResult::Skip(format!("morph-shape-{e:?}"));
    }
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for pid in 0..n_producers {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                while r.try_send(pid, &payload).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        producer_handles.push(h);
    }
    let mut consumer_handles = Vec::new();
    for cid in 0..n_consumers {
        let r = Arc::clone(&ring);
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            if n_consumers == 1 {
                for _ in 0..total {
                    while r.try_recv(cid, &mut buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
            } else {
                loop {
                    if r.try_recv(cid, &mut buf).is_ok() {
                        if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                            return;
                        }
                    } else if d.load(Ordering::Relaxed) >= total {
                        return;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

// ============================================================================
// CapacityBroadcastRing (steady-state)
// ============================================================================

fn bench_capacity_broadcast(
    locale: &str,
    capacity: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let ring: Arc<CapacityBroadcastRing> = match locale {
        "anon" => match CapacityBroadcastRing::create_anon(capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_capbcast_{}.bin", std::process::id()));
            match CapacityBroadcastRing::create(&path, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_capbcast_{}", std::process::id());
            match CapacityBroadcastRing::create_shmfs(&name, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    let consumer_ids: Vec<usize> = match (0..n_consumers).map(|_| ring.register_consumer()).collect()
    {
        Ok(ids) => ids,
        Err(e) => return BenchResult::Skip(format!("register-failed-{e:?}")),
    };

    let t0 = Instant::now();
    let r = Arc::clone(&ring);
    let producer_h = thread::spawn(move || {
        let payload = [0u8; 52];
        for _ in 0..n_items {
            while r.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    let mut consumer_handles = Vec::new();
    for cid in consumer_ids {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            for _ in 0..n_items {
                while r.try_recv(cid, &mut buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        consumer_handles.push(h);
    }
    producer_h.join().unwrap();
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, n_items, 1, n_consumers)
}

// ============================================================================
// CapacityPubSubRing (steady-state)
// ============================================================================

fn bench_capacity_pubsub(
    locale: &str,
    capacity: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let ring: Arc<CapacityPubSubRing> = match locale {
        "anon" => match CapacityPubSubRing::create_anon(capacity) {
            Ok(r) => r,
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_cappubsub_{}.bin", std::process::id()));
            match CapacityPubSubRing::create(&path, capacity) {
                Ok(r) => r,
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_cappubsub_{}", std::process::id());
            match CapacityPubSubRing::create_shmfs(&name, capacity) {
                Ok(r) => r,
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    let subscribers: Vec<_> = (0..n_consumers)
        .map(|_| ring.subscribe_from_oldest())
        .collect();
    let sub_positions: Vec<Arc<AtomicU64>> = (0..n_consumers)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let t0 = Instant::now();
    let r_prod = Arc::clone(&ring);
    let pos_for_prod: Vec<Arc<AtomicU64>> = sub_positions.to_vec();
    let producer_h = thread::spawn(move || {
        let payload = [0u8; 56];
        for i in 0..n_items {
            loop {
                let min_sub = pos_for_prod
                    .iter()
                    .map(|a| a.load(Ordering::Acquire))
                    .min()
                    .unwrap_or(0);
                if i.saturating_sub(min_sub) < capacity as u64 {
                    break;
                }
                std::hint::spin_loop();
            }
            r_prod.publish(&payload);
        }
    });
    let mut consumer_handles = Vec::new();
    for (sub_i, mut sub) in subscribers.into_iter().enumerate() {
        let pos = Arc::clone(&sub_positions[sub_i]);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            let mut count: u64 = 0;
            while count < n_items {
                match sub.try_next(&mut buf) {
                    Ok(()) => {
                        count += 1;
                        pos.store(count, Ordering::Release);
                    }
                    Err(PubSubReadError::Pending) => std::hint::spin_loop(),
                    Err(PubSubReadError::Lost) => {
                        eprintln!("cap-pubsub bench: subscriber lost at count={count}");
                        return;
                    }
                }
            }
        });
        consumer_handles.push(h);
    }
    producer_h.join().unwrap();
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, n_items, 1, n_consumers)
}

// ============================================================================
// External baseline: crossbeam-channel bounded
// ============================================================================

fn bench_crossbeam(
    capacity: usize,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let (tx, rx) = crossbeam_channel::bounded::<[u8; 56]>(capacity);
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for _ in 0..n_producers {
        let tx = tx.clone();
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                tx.send(payload).expect("crossbeam send");
            }
        });
        producer_handles.push(h);
    }
    drop(tx);
    let mut consumer_handles = Vec::new();
    for _ in 0..n_consumers {
        let rx = rx.clone();
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            while rx.recv().is_ok() {
                if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                    return;
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

// ============================================================================
// External baseline: std::sync::mpsc::sync_channel
// ============================================================================

fn bench_std_mpsc(capacity: usize, n_producers: usize, n_items: u64) -> BenchResult {
    let (tx, rx) = std::sync::mpsc::sync_channel::<[u8; 56]>(capacity);
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for _ in 0..n_producers {
        let tx = tx.clone();
        let h = thread::spawn(move || {
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                tx.send(payload).expect("std mpsc send");
            }
        });
        producer_handles.push(h);
    }
    drop(tx);
    let consumer_h = thread::spawn(move || {
        for _ in 0..total {
            rx.recv().expect("std mpsc recv");
        }
    });
    for h in producer_handles {
        h.join().unwrap();
    }
    consumer_h.join().unwrap();
    bench_result_from(t0, total, n_producers, 1)
}

// ============================================================================
// Pinned variants - measure the production hot path.
//
// Wrappers (AdaptiveRing, LocaleAdaptiveRing, CapacityAdaptiveRing,
// CapacityBroadcastRing) all expose a "pin the current state and
// expose the inner primitive directly" pattern. Hot loops grab the
// pin once when the shape/locale/capacity is stable, hot-loop on
// the inner primitive's native ops, and periodically check
// is_still_valid() to catch morphs.
//
// These benches drive that production-style flow: pin once, hot-
// loop, check validity at the end of every batch. The throughput
// numbers here should match the underlying native primitive's
// numbers (within a single Acquire-load + branch per batch).
// ============================================================================

/// AdaptiveRing pinned: pin the shape once, hot-loop on
/// PinnedRing's native dispatch methods (spsc_try_push etc).
fn bench_adaptive_pinned(
    locale: &str,
    capacity: usize,
    shape: RingShape,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let max_p = n_producers.max(1);
    let max_c = n_consumers.max(1);
    let ring: Arc<AdaptiveRing> = match locale {
        "anon" => match AdaptiveRing::create_anon(max_p, max_c, capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_adaptive_pinned_{}", std::process::id()));
            match AdaptiveRing::create(&path, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_adaptive_pinned_{}", std::process::id());
            match AdaptiveRing::create_shmfs(&name, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    for _ in 0..n_producers {
        if let Err(e) = ring.register_producer() {
            return BenchResult::Skip(format!("register-producer-{e:?}"));
        }
    }
    for _ in 0..n_consumers {
        if let Err(e) = ring.register_consumer() {
            return BenchResult::Skip(format!("register-consumer-{e:?}"));
        }
    }
    if shape != RingShape::Spsc
        && let Err(e) = ring.morph_to(shape)
    {
        return BenchResult::Skip(format!("morph-failed-{e:?}"));
    }
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for pid in 0..n_producers {
        let r = Arc::clone(&ring);
        let h = thread::spawn(move || {
            // Pin the shape ONCE; hot-loop on PinnedRing's native
            // per-shape dispatch (bypasses the shape_tag.load per
            // op the adaptive try_send pays).
            let pin = r.pin_current_shape();
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                match shape {
                    RingShape::Spsc => while pin.spsc_try_push(&payload).is_err() { std::hint::spin_loop(); }
                    RingShape::Mpsc => while pin.mpsc_try_push(pid, &payload).is_err() { std::hint::spin_loop(); }
                    RingShape::Mpmc => while pin.mpmc_try_push(pid, &payload).is_err() { std::hint::spin_loop(); }
                    RingShape::Vyukov => while pin.vyukov_try_push(&payload).is_err() { std::hint::spin_loop(); }
                }
            }
        });
        producer_handles.push(h);
    }
    let mut consumer_handles = Vec::new();
    for cid in 0..n_consumers {
        let r = Arc::clone(&ring);
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let pin = r.pin_current_shape();
            let mut buf = [0u8; 64];
            if n_consumers == 1 {
                for _ in 0..total {
                    loop {
                        let ok = match shape {
                            RingShape::Spsc => pin.spsc_try_pop(&mut buf).is_ok(),
                            RingShape::Mpsc => pin.mpsc_try_pop(&mut buf).is_ok(),
                            RingShape::Mpmc => pin.mpmc_try_pop(cid, &mut buf).is_ok(),
                            RingShape::Vyukov => pin.vyukov_try_pop(&mut buf).is_ok(),
                        };
                        if ok { break; }
                        std::hint::spin_loop();
                    }
                }
            } else {
                loop {
                    let ok = match shape {
                        RingShape::Spsc => pin.spsc_try_pop(&mut buf).is_ok(),
                        RingShape::Mpsc => pin.mpsc_try_pop(&mut buf).is_ok(),
                        RingShape::Mpmc => pin.mpmc_try_pop(cid, &mut buf).is_ok(),
                        RingShape::Vyukov => pin.vyukov_try_pop(&mut buf).is_ok(),
                    };
                    if ok {
                        if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                            return;
                        }
                    } else if d.load(Ordering::Relaxed) >= total {
                        return;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

/// CapacityAdaptiveRing pinned: pin the capacity wrapper, get the
/// inner AdaptiveRing handle, then pin that for shape and hot-loop
/// on PinnedRing's native dispatch. Skips BOTH the wrapper's
/// stale-list mutex (steady-state overhead) AND the AdaptiveRing
/// shape_tag.load.
fn bench_capacity_pinned(
    locale: &str,
    capacity: usize,
    shape: RingShape,
    n_producers: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let max_p = n_producers.max(1);
    let max_c = n_consumers.max(1);
    let cap_ring: Arc<CapacityAdaptiveRing> = match locale {
        "anon" => match CapacityAdaptiveRing::create_anon(max_p, max_c, capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_cap_pinned_{}.bin", std::process::id()));
            match CapacityAdaptiveRing::create(&path, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_cap_pinned_{}", std::process::id());
            match CapacityAdaptiveRing::create_shmfs(&name, max_p, max_c, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    for _ in 0..n_producers {
        if let Err(e) = cap_ring.register_producer() {
            return BenchResult::Skip(format!("register-producer-{e:?}"));
        }
    }
    for _ in 0..n_consumers {
        if let Err(e) = cap_ring.register_consumer() {
            return BenchResult::Skip(format!("register-consumer-{e:?}"));
        }
    }
    if shape != RingShape::Spsc
        && let Err(e) = cap_ring.ring_handle().morph_to(shape)
    {
        return BenchResult::Skip(format!("morph-shape-{e:?}"));
    }
    // Capture the underlying AdaptiveRing handle once. Production
    // would re-acquire on pin_current_capacity().is_still_valid()
    // returning false.
    let inner: Arc<AdaptiveRing> = cap_ring.ring_handle();
    let items_per_producer = n_items / n_producers as u64;
    let total = items_per_producer * n_producers as u64;
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut producer_handles = Vec::new();
    for pid in 0..n_producers {
        let r = Arc::clone(&inner);
        let h = thread::spawn(move || {
            let pin = r.pin_current_shape();
            let payload = [0u8; 56];
            for _ in 0..items_per_producer {
                match shape {
                    RingShape::Spsc => while pin.spsc_try_push(&payload).is_err() { std::hint::spin_loop(); }
                    RingShape::Mpsc => while pin.mpsc_try_push(pid, &payload).is_err() { std::hint::spin_loop(); }
                    RingShape::Mpmc => while pin.mpmc_try_push(pid, &payload).is_err() { std::hint::spin_loop(); }
                    RingShape::Vyukov => while pin.vyukov_try_push(&payload).is_err() { std::hint::spin_loop(); }
                }
            }
        });
        producer_handles.push(h);
    }
    let mut consumer_handles = Vec::new();
    for cid in 0..n_consumers {
        let r = Arc::clone(&inner);
        let d = Arc::clone(&drained);
        let h = thread::spawn(move || {
            let pin = r.pin_current_shape();
            let mut buf = [0u8; 64];
            if n_consumers == 1 {
                for _ in 0..total {
                    loop {
                        let ok = match shape {
                            RingShape::Spsc => pin.spsc_try_pop(&mut buf).is_ok(),
                            RingShape::Mpsc => pin.mpsc_try_pop(&mut buf).is_ok(),
                            RingShape::Mpmc => pin.mpmc_try_pop(cid, &mut buf).is_ok(),
                            RingShape::Vyukov => pin.vyukov_try_pop(&mut buf).is_ok(),
                        };
                        if ok { break; }
                        std::hint::spin_loop();
                    }
                }
            } else {
                loop {
                    let ok = match shape {
                        RingShape::Spsc => pin.spsc_try_pop(&mut buf).is_ok(),
                        RingShape::Mpsc => pin.mpsc_try_pop(&mut buf).is_ok(),
                        RingShape::Mpmc => pin.mpmc_try_pop(cid, &mut buf).is_ok(),
                        RingShape::Vyukov => pin.vyukov_try_pop(&mut buf).is_ok(),
                    };
                    if ok {
                        if d.fetch_add(1, Ordering::Relaxed) + 1 >= total {
                            return;
                        }
                    } else if d.load(Ordering::Relaxed) >= total {
                        return;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }
        });
        consumer_handles.push(h);
    }
    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, total, n_producers, n_consumers)
}

/// CapacityBroadcastRing pinned: skip the wrapper's stale-list
/// mutex, hot-loop on the inner SharedBroadcastRing directly.
fn bench_capacity_broadcast_pinned(
    locale: &str,
    capacity: usize,
    n_consumers: usize,
    n_items: u64,
) -> BenchResult {
    let cap_ring: Arc<CapacityBroadcastRing> = match locale {
        "anon" => match CapacityBroadcastRing::create_anon(capacity) {
            Ok(r) => Arc::new(r),
            Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
        },
        "file" => {
            let path = std::env::temp_dir()
                .join(format!("bench_capbcast_pinned_{}.bin", std::process::id()));
            match CapacityBroadcastRing::create(&path, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        "shmfs" => {
            let name = format!("bench_capbcast_pinned_{}", std::process::id());
            match CapacityBroadcastRing::create_shmfs(&name, capacity) {
                Ok(r) => Arc::new(r),
                Err(e) => return BenchResult::Skip(format!("create-failed-{e:?}")),
            }
        }
        _ => return BenchResult::Skip(format!("locale-{locale}-unknown")),
    };
    let consumer_ids: Vec<usize> = match (0..n_consumers).map(|_| cap_ring.register_consumer()).collect()
    {
        Ok(ids) => ids,
        Err(e) => return BenchResult::Skip(format!("register-failed-{e:?}")),
    };
    let inner = cap_ring.ring_handle();

    let t0 = Instant::now();
    let r = Arc::clone(&inner);
    let producer_h = thread::spawn(move || {
        let payload = [0u8; 52];
        for _ in 0..n_items {
            while r.try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    let mut consumer_handles = Vec::new();
    for cid in consumer_ids {
        let r = Arc::clone(&inner);
        let h = thread::spawn(move || {
            let mut buf = [0u8; 64];
            for _ in 0..n_items {
                while r.try_recv(cid, &mut buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        });
        consumer_handles.push(h);
    }
    producer_h.join().unwrap();
    for h in consumer_handles {
        h.join().unwrap();
    }
    bench_result_from(t0, n_items, 1, n_consumers)
}
