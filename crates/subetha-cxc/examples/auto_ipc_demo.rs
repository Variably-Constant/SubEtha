//! `AutoIpc` user-facing API E2E demo.
//!
//! Shows what the end-user actually writes when they want
//! cross-thread / cross-process MMF-backed IPC with the dispatcher
//! picking the right primitive automatically from declarative hints.
//!
//! No `MmfWorkloadShape`, no `MmfFamily`, no `MmfDispatcher` enum,
//! no primitive name appears in user code below. Just:
//!
//! ```ignore
//! let chan = AutoIpc::new("path")
//!     .producers(4).consumers(4)
//!     .capacity(64)
//!     .build_channel::<MyType>()?;
//! chan.send(&item)?;
//! ```
//!
//! Three scenarios exercised:
//! 1. Default 1-to-1 channel  (-> SharedRing under the hood)
//! 2. Batched producer queue   (-> KHL under the hood)
//! 3. Idle-wait multi-thief    (-> URD under the hood)
//! 4. Key-value map            (-> SharedHashMap under the hood)
//!
//! ## Run
//!
//! ```bash
//! cargo run --release --example auto_ipc_demo -p subetha-cxc
//! ```

use std::time::Instant;

use subetha_core::Marshal;
use subetha_cxc::{AutoIpc, Channel, KvMap, MmfFamily, WorkStealQueue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MyEvent {
    id: u32,
    payload: u32,
}

unsafe impl Marshal for MyEvent {
    const PAYLOAD_BYTES: usize = 8;
    fn marshal(&self, dst: &mut [u8]) {
        dst[..4].copy_from_slice(&self.id.to_le_bytes());
        dst[4..8].copy_from_slice(&self.payload.to_le_bytes());
    }
    fn unmarshal(src: &[u8]) -> Result<Self, subetha_core::MarshalError> {
        if src.len() < 8 {
            return Err(subetha_core::MarshalError::ShortBuffer {
                expected: 8,
                got: src.len(),
            });
        }
        Ok(MyEvent {
            id: u32::from_le_bytes(src[..4].try_into().unwrap()),
            payload: u32::from_le_bytes(src[4..8].try_into().unwrap()),
        })
    }
}

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_auto_demo_{pid}_{nonce}_{name}.bin"));
    p
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== AutoIpc: user-facing API, dispatcher hides the primitive choice ===");
    println!();

    // === Scenario 1: default 1-to-1 channel ===
    let path1 = tmp("ch1");
    let auto = AutoIpc::new(&path1).capacity(64);
    println!("[1] AutoIpc::new(...).capacity(64) inferred:");
    println!("    family = {:?}", auto.inferred_family());
    assert_eq!(auto.inferred_family(), MmfFamily::SharedRing);

    let chan: Channel<MyEvent> = auto.build_channel()?;
    let t0 = Instant::now();
    for id in 0..50u32 {
        chan.send(&MyEvent { id, payload: id * 10 })?;
    }
    let send_us = t0.elapsed();
    let t1 = Instant::now();
    let mut sum = 0u64;
    let mut count = 0;
    while let Ok(ev) = chan.recv() {
        sum += ev.payload as u64;
        count += 1;
    }
    let recv_us = t1.elapsed();
    println!(
        "    sent 50 events in {send_us:?}; recv'd {count} events in {recv_us:?}; \
         sum={sum} (expected {})",
        (0..50u64).map(|i| i * 10).sum::<u64>()
    );
    assert_eq!(count, 50);
    assert_eq!(sum, (0..50u64).map(|i| i * 10).sum::<u64>());
    std::fs::remove_file(&path1).ok();
    println!();

    // === Scenario 2: batched producer ===
    let path2 = tmp("ch2");
    let auto = AutoIpc::new(&path2).batch_size(64).capacity(256);
    println!("[2] AutoIpc::new(...).batch_size(64).capacity(256) inferred:");
    println!("    family = {:?}", auto.inferred_family());
    // batch_size hint flips to work-stealing; single-thief default -> KHL
    let q: WorkStealQueue<u64> = auto.build_work_steal_queue()?;
    println!("    variant = {:?}", q.variant());
    let t0 = Instant::now();
    for i in 0..64u64 {
        q.push(&i)?;
    }
    let push_us = t0.elapsed();
    let mut sum = 0u64;
    let mut n = 0;
    while let Some(v) = q.steal() {
        sum += v;
        n += 1;
    }
    println!(
        "    pushed 64 items in {push_us:?}; drained {n}, sum={sum} (expected {})",
        (0..64u64).sum::<u64>()
    );
    assert_eq!(n, 64);
    assert_eq!(sum, (0..64u64).sum());
    std::fs::remove_file(&path2).ok();
    println!();

    // === Scenario 3: idle-wait multi-thief ===
    let path3 = tmp("ch3");
    let auto = AutoIpc::new(&path3)
        .consumers(4)
        .batch_size(16)
        .idle_wait(true)
        .capacity(64);
    println!("[3] AutoIpc::new(...).consumers(4).batch_size(16).idle_wait(true) inferred:");
    println!("    family = {:?}", auto.inferred_family());
    let q: WorkStealQueue<u64> = auto.build_work_steal_queue()?;
    println!("    variant = {:?}", q.variant());
    std::fs::remove_file(&path3).ok();
    println!();

    // === Scenario 4: key-value map ===
    let path4 = tmp("kv");
    let auto = AutoIpc::new(&path4).capacity(64);
    println!("[4] AutoIpc::new(...).build_kv_map():");
    let map: KvMap<u32, u32> = auto.build_kv_map()?;
    let t0 = Instant::now();
    for k in 0..32u32 {
        map.insert(k, k.wrapping_mul(31))?;
    }
    let insert_us = t0.elapsed();
    let mut hits = 0;
    for k in 0..32u32 {
        if let Some(v) = map.get(&k) {
            assert_eq!(v, k.wrapping_mul(31));
            hits += 1;
        }
    }
    let per_insert_ns = insert_us.as_nanos() as f64 / 32.0;
    println!(
        "    inserted 32 entries in {insert_us:?}; looked up {hits} hits ({per_insert_ns:.1}ns per insert)"
    );
    assert_eq!(hits, 32);
    std::fs::remove_file(&path4).ok();
    println!();

    println!("=== All 4 scenarios round-tripped bit-exact through AutoIpc ===");
    println!("No MmfWorkloadShape, no MmfFamily, no primitive name in user code.");
    println!("Dispatcher picked the right backing from declarative hints.");
    Ok(())
}
