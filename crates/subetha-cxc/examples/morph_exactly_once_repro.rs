//! Minimal exactly-once stress for capacity morphs under MULTIPLE
//! concurrent producers. The library's existing concurrent morph
//! test uses a single producer; this drives 2 producers + 1 consumer
//! while a morpher thread hammers the capacity between two sizes, and
//! asserts every item is delivered exactly once (no loss, no
//! duplication). Pure library exercise - no policy, no sidecar.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::RingShape;
use subetha_cxc::CapacityAdaptiveRing;

const PER_PRODUCER: u64 = 2_000_000;

fn payload(pid: u16, seq: u64) -> [u8; 56] {
    let mut p = [0u8; 56];
    p[..8].copy_from_slice(&seq.to_le_bytes());
    p[8..10].copy_from_slice(&pid.to_le_bytes());
    p
}

fn main() {
    let ring = Arc::new(CapacityAdaptiveRing::create_anon(2, 1, 256).unwrap());
    ring.register_producer().unwrap();
    ring.register_producer().unwrap();
    let cid = ring.register_consumer().unwrap();
    // The ring defaults to SPSC; 2 producers REQUIRE MPSC. Morph
    // before any producer pushes - pushing 2 producers at an SPSC
    // ring violates its single-producer contract. Capacity morphs
    // mirror the shape, so MPSC is preserved across the hammering.
    ring.ring_handle().morph_to(RingShape::Mpsc).unwrap();

    let done = Arc::new(AtomicBool::new(false));

    // Consumer: per-producer exactly-once via seen-sets.
    let r = Arc::clone(&ring);
    let d = Arc::clone(&done);
    let consumer = std::thread::spawn(move || {
        let mut seen: [HashSet<u64>; 2] = [HashSet::new(), HashSet::new()];
        let mut out = [0u8; 64];
        loop {
            match r.try_recv(cid, &mut out) {
                Ok(_) => {
                    let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                    let pid = u16::from_le_bytes(out[8..10].try_into().unwrap()) as usize;
                    assert!(seen[pid].insert(seq),
                            "DUPLICATE: producer {pid} seq {seq}");
                }
                Err(_) => {
                    if d.load(Ordering::Acquire) {
                        return seen;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    });

    // Morpher: hammer the capacity between two sizes.
    let r = Arc::clone(&ring);
    let d = Arc::clone(&done);
    let morpher = std::thread::spawn(move || {
        let sizes = [256usize, 1024, 512, 2048];
        let mut i = 0usize;
        let mut n = 0u64;
        while !d.load(Ordering::Acquire) {
            ring_morph(&r, sizes[i % sizes.len()]);
            i += 1;
            n += 1;
            std::thread::sleep(Duration::from_micros(200));
        }
        n
    });

    // Two producers.
    let mut prods = Vec::new();
    for pid in 0..2u16 {
        let r = Arc::clone(&ring);
        prods.push(std::thread::spawn(move || {
            for seq in 0..PER_PRODUCER {
                while r.try_send(pid as usize, &payload(pid, seq)).is_err() {
                    std::hint::spin_loop();
                }
            }
        }));
    }

    for p in prods {
        p.join().unwrap();
    }
    // Drain to completion, then stop.
    let deadline = Instant::now() + Duration::from_secs(30);
    // Wait for the ring to quiesce (best-effort) before declaring done.
    while ring.ring_handle().approx_len() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    std::thread::sleep(Duration::from_millis(50));
    done.store(true, Ordering::Release);
    let morphs = morpher.join().unwrap();
    let seen = consumer.join().unwrap();

    let total: usize = seen.iter().map(|s| s.len()).sum();
    let expected = 2 * PER_PRODUCER as usize;
    println!("morphs: {morphs}");
    println!("producer 0 delivered: {} / {}", seen[0].len(), PER_PRODUCER);
    println!("producer 1 delivered: {} / {}", seen[1].len(), PER_PRODUCER);
    println!("total: {total} / {expected}");
    assert_eq!(seen[0].len(), PER_PRODUCER as usize, "producer 0 loss");
    assert_eq!(seen[1].len(), PER_PRODUCER as usize, "producer 1 loss");
    println!("EXACTLY-ONCE HELD across {morphs} morphs under 2 concurrent producers");
}

fn ring_morph(ring: &CapacityAdaptiveRing, cap: usize) {
    ring.morph_capacity_to(cap).ok();
}
