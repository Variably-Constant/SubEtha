//! `MmfDispatcher` cross-family end-to-end demo.
//!
//! This example demonstrates the dispatcher's per-call routing
//! across all three MMF primitive families: `SharedRing` (streaming
//! MPMC), the `SharedDeque` family (work-stealing, delegated
//! through `DequeDispatcher`), and `SharedHashMap` (key-value
//! lookup). Three workload shapes are dispatched in a single binary;
//! each lands on the right family and the contents are drained /
//! looked up bit-exact.
//!
//! ## Routing decisions exercised
//!
//! - `MmfWorkloadShape::StreamingMpmc { .. }` routes to
//!   `MmfFamily::SharedRing`.
//! - `MmfWorkloadShape::WorkStealing(producer_fast(K))` routes to
//!   `MmfFamily::SharedDeque(DequeVariant::Khl)`.
//! - `MmfWorkloadShape::KeyValueLookup { .. }` routes to
//!   `MmfFamily::SharedHashMap`.
//!
//! ## Run
//!
//! ```bash
//! cargo run --release --example mmf_dispatcher_demo -p subetha-cxc
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::{
    BackgroundScheduler, DequeDispatcher, DequeVariant, KhlSteal, LineItem,
    MmfDispatcher, MmfFamily, MmfWorkloadShape, Pass, SharedHashMap, SharedRing,
    WorkloadShape,
};

const N_RING: usize = 20;
const N_DEQUE: usize = 60;
const N_MAP: usize = 16;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_mmf_demo_{pid}_{nonce}_{name}.bin"));
    p
}

fn u32_item(id: u32) -> LineItem {
    LineItem::new(&id.to_le_bytes()).expect("item")
}

fn item_id(item: &LineItem) -> u32 {
    u32::from_le_bytes(item.payload[..4].try_into().unwrap())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ring_path = tmp("ring");
    let khl_path = tmp("khl");
    let map_path = tmp("map");

    // Three workload shapes, one per family.
    let streaming = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let work_stealing = MmfWorkloadShape::WorkStealing(WorkloadShape::producer_fast(
        N_DEQUE,
    ));
    let key_value = MmfWorkloadShape::KeyValueLookup {
        n_readers: 1,
        n_writers: 1,
    };

    // Ask the dispatcher to pick a family for each shape.
    let f_stream = MmfDispatcher::pick(streaming);
    let f_steal = MmfDispatcher::pick(work_stealing);
    let f_kv = MmfDispatcher::pick(key_value);
    println!("[main] MmfDispatcher routing:");
    println!("       streaming     -> {f_stream:?}");
    println!("       work_stealing -> {f_steal:?}");
    println!("       key_value     -> {f_kv:?}");
    assert_eq!(f_stream, MmfFamily::SharedRing);
    assert_eq!(f_steal, MmfFamily::SharedDeque(DequeVariant::Khl));
    assert_eq!(f_kv, MmfFamily::SharedHashMap);

    // Confirm the signature-based path picks the same families.
    assert_eq!(MmfDispatcher::pick_by_signature(streaming), f_stream);
    assert_eq!(MmfDispatcher::pick_by_signature(work_stealing), f_steal);
    assert_eq!(MmfDispatcher::pick_by_signature(key_value), f_kv);
    println!("[main] signature-based path agrees on all 3 families");

    // === Family 1: SharedRing (streaming MPMC) ===
    let ring = SharedRing::create(&ring_path, 64)
        .map_err(|e| format!("ring create: {e:?}"))?;
    let expected_ring_sum: u64 = (0..N_RING as u64).sum();
    let t0 = Instant::now();
    for id in 0..N_RING as u32 {
        let mut payload = [0u8; 56];
        payload[..4].copy_from_slice(&id.to_le_bytes());
        ring.try_push(&payload)
            .map_err(|e| format!("ring push: {e:?}"))?;
    }
    let push_time = t0.elapsed();

    let mut ring_sum = 0u64;
    let mut ring_count = 0usize;
    let mut out = [0u8; 56];
    while ring.try_pop(&mut out).is_ok() {
        let id = u32::from_le_bytes(out[..4].try_into().unwrap());
        ring_sum += id as u64;
        ring_count += 1;
    }
    println!(
        "[ring] pushed {N_RING} items in {push_time:?}; drained {ring_count}, \
         sum {ring_sum} (expected {expected_ring_sum})"
    );
    assert_eq!(ring_count, N_RING);
    assert_eq!(ring_sum, expected_ring_sum);

    // === Family 2: SharedDeque (work-stealing, KHL) via DequeDispatcher ===
    let deque_dispatcher = DequeDispatcher::builder()
        .with_khl(&khl_path, 256)?
        .build();
    let batch: Vec<LineItem> = (0..N_DEQUE as u32).map(u32_item).collect();
    let expected_deque_sum: u64 = (0..N_DEQUE as u64).sum();
    let t0 = Instant::now();
    let chosen = deque_dispatcher
        .dispatch_batch(WorkloadShape::producer_fast(N_DEQUE), &batch)?;
    let publish_time = t0.elapsed();
    assert_eq!(chosen, DequeVariant::Khl);

    // Drain KHL in a thief thread to mimic the production topology.
    let khl = Arc::clone(deque_dispatcher.khl().expect("khl handle"));
    let stop = Arc::new(AtomicBool::new(false));
    let drained = Arc::new(AtomicU64::new(0));
    let summed = Arc::new(AtomicU64::new(0));
    let thief = {
        let khl = Arc::clone(&khl);
        let stop = Arc::clone(&stop);
        let drained = Arc::clone(&drained);
        let summed = Arc::clone(&summed);
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match khl.steal_slot() {
                    KhlSteal::Success(r) => {
                        for i in 0..r.n_items {
                            drained.fetch_add(1, Ordering::AcqRel);
                            summed.fetch_add(
                                item_id(&r.items[i]) as u64,
                                Ordering::AcqRel,
                            );
                        }
                    }
                    KhlSteal::Empty | KhlSteal::Retry => std::hint::spin_loop(),
                }
            }
        })
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    while drained.load(Ordering::Acquire) < N_DEQUE as u64 {
        if Instant::now() > deadline {
            break;
        }
        std::hint::spin_loop();
    }
    stop.store(true, Ordering::Release);
    thief.join().expect("thief");
    let deque_drained = drained.load(Ordering::Acquire);
    let deque_sum = summed.load(Ordering::Acquire);
    println!(
        "[deque] dispatched {N_DEQUE}-item batch via Khl in {publish_time:?}; \
         drained {deque_drained}, sum {deque_sum} (expected {expected_deque_sum})"
    );
    assert_eq!(deque_drained, N_DEQUE as u64);
    assert_eq!(deque_sum, expected_deque_sum);

    // === Family 3: SharedHashMap (key-value) ===
    let map: SharedHashMap<u32, u32> =
        SharedHashMap::create(&map_path, 64).map_err(|e| format!("map create: {e:?}"))?;
    let t0 = Instant::now();
    for k in 0..N_MAP as u32 {
        map.insert(k, k * k)
            .map_err(|e| format!("map insert: {e:?}"))?;
    }
    let insert_time = t0.elapsed();
    let mut hits = 0usize;
    let mut sq_sum = 0u64;
    for k in 0..N_MAP as u32 {
        let v = map.get(&k).expect("map miss");
        assert_eq!(v, k * k);
        hits += 1;
        sq_sum += v as u64;
    }
    let expected_sq_sum: u64 = (0..N_MAP as u64).map(|k| k * k).sum();
    println!(
        "[map] inserted {N_MAP} entries in {insert_time:?}; \
         looked up {hits} hits, sum-of-squares {sq_sum} (expected {expected_sq_sum})"
    );
    assert_eq!(hits, N_MAP);
    assert_eq!(sq_sum, expected_sq_sum);

    println!(
        "[main] all 3 MmfDispatcher routes round-tripped bit-exact \
         (SharedRing + SharedDeque(Khl) + SharedHashMap)"
    );

    // === BackgroundScheduler via start_by_workload_shape ===
    // Wire-or-keep proof: the scheduler consumes the dispatcher's
    // routing decision. Submit-side is StreamingMpmc (-> SharedRing);
    // result-side is WorkStealing (-> SharedDeque<PassSlot>).
    let sched_submit = tmp("sched_submit");
    let sched_result = tmp("sched_result");
    let sched_hb = tmp("sched_hb");
    let submit_shape = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let result_shape =
        MmfWorkloadShape::WorkStealing(WorkloadShape::producer_fast(8));

    // Register a Pass handler that doubles each arg byte.
    let closure_id: u32 = 0x6D6D_6664; // 'mmfd' in ASCII
    subetha_cxc::register_handler(closure_id, |args| {
        Ok(args.iter().map(|b| b.wrapping_mul(2)).collect())
    });

    let (sched, submit_family, result_family) =
        BackgroundScheduler::start_by_workload_shape(
            &sched_submit,
            submit_shape,
            &sched_result,
            result_shape,
            &sched_hb,
            32,
            8,
        )
        .map_err(|e| format!("start_by_workload_shape: {e:?}"))?;
    println!(
        "[sched] BackgroundScheduler picked submit={submit_family:?}, \
         result={result_family:?}"
    );
    assert_eq!(submit_family, MmfFamily::SharedRing);
    assert!(matches!(result_family, MmfFamily::SharedDeque(_)));

    let submitter = sched.submitter();
    let collector = sched.collector();
    let t0 = Instant::now();
    let token = submitter
        .submit(&Pass {
            closure_id,
            args: vec![3, 5, 7, 11, 13],
        })
        .map_err(|e| format!("submit: {e:?}"))?;
    let mut sched_result_payload = None;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Ok(r) = collector.try_recv() {
            sched_result_payload = Some(r);
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    let r = sched_result_payload
        .ok_or("scheduler result did not arrive within 2s")?;
    let sched_rt = t0.elapsed();
    assert_eq!(r.token, token);
    match r.result {
        Ok(data) => {
            println!(
                "[sched] Pass round-trip in {sched_rt:?}; token={token}; \
                 result={data:?} (expected [6, 10, 14, 22, 26])"
            );
            assert_eq!(data, vec![6, 10, 14, 22, 26]);
        }
        Err(e) => return Err(format!("scheduler Pass failed: {e:?}").into()),
    }
    println!(
        "[main] BackgroundScheduler via start_by_workload_shape round-tripped \
         a Pass bit-exact through MmfDispatcher-picked transports"
    );

    drop(sched);
    subetha_cxc::unregister_handler(closure_id);

    std::fs::remove_file(&ring_path).ok();
    std::fs::remove_file(&khl_path).ok();
    std::fs::remove_file(&map_path).ok();
    std::fs::remove_file(&sched_submit).ok();
    std::fs::remove_file(&sched_result).ok();
    std::fs::remove_file(&sched_hb).ok();
    Ok(())
}
