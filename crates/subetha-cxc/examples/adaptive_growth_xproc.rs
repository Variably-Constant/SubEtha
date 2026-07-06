//! Cross-process E2E proof of the AUTOMATIC AdaptiveRing: a ring
//! created with a 1-producer / 1-consumer hint grows to 3 producer
//! PROCESSES and 2 consumer PROCESSES with zero registration errors,
//! morphing SPSC -> MPSC -> MPMC on its own as peers join, and every
//! item is delivered exactly once with per-producer FIFO intact.
//!
//! What it exercises, in order:
//! 1. Producer A registers (slot 0) and streams - the ring holds SPSC.
//! 2. Producers B and C register while A is mid-stream. Slot 1 and 2
//!    exceed the construction hint, so registration GROWS the ring
//!    (new per-producer MMF backings, published via the shared peer
//!    directory) and the driver's consumer morphs to MPSC on its next
//!    pop with no sidecar and no explicit morph call.
//! 3. A second consumer process registers mid-stream: the shape
//!    morphs to MPMC and ring ownership rebalances across the two
//!    consumers (single-reader handoff, never two poppers on one
//!    Lamport core).
//! 4. Exactly-once accounting: the two consumers' per-producer
//!    tallies sum to exactly what the three producers sent, and each
//!    (producer, consumer) stream is seq-monotone (per-producer FIFO).
//!
//! Run: `cargo run --release -p subetha-cxc --example adaptive_growth_xproc`
//! Exit code 0 with a PASS line per assertion; any violation exits 1.

use std::io::BufRead;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::{AdaptiveRing, RingShape};

const ITEMS_A: u64 = 120_000;
const ITEMS_B: u64 = 60_000;
const ITEMS_C: u64 = 60_000;
const CAPACITY: usize = 4096;
const MAX_SLOTS: usize = 8;
const DEADLINE: Duration = Duration::from_secs(60);

fn payload(slot: u64, seq: u64) -> [u8; 16] {
    let mut p = [0u8; 16];
    p[..8].copy_from_slice(&slot.to_le_bytes());
    p[8..].copy_from_slice(&seq.to_le_bytes());
    p
}

#[derive(Default)]
struct Tally {
    count: [u64; MAX_SLOTS],
    last_seq: [Option<u64>; MAX_SLOTS],
    monotone: bool,
}

impl Tally {
    fn new() -> Self {
        Self { monotone: true, ..Self::default() }
    }

    fn record(&mut self, buf: &[u8]) {
        let slot = u64::from_le_bytes(buf[..8].try_into().unwrap()) as usize;
        let seq = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        assert!(slot < MAX_SLOTS, "unexpected producer slot {slot}");
        if let Some(prev) = self.last_seq[slot]
            && seq <= prev
        {
            self.monotone = false;
        }
        self.last_seq[slot] = Some(seq);
        self.count[slot] += 1;
    }
}

fn producer_main(prefix: &str, items: u64) {
    let ring = AdaptiveRing::open(prefix, 1, 1, CAPACITY).expect("open");
    let slot = ring.register_producer().expect("register_producer");
    let mut sent = 0u64;
    while sent < items {
        match ring.try_send(slot, &payload(slot as u64, sent)) {
            Ok(()) => sent += 1,
            Err(_) => std::hint::spin_loop(),
        }
    }
    ring.unregister_producer(slot);
    println!("SENT {slot} {sent}");
}

fn consumer2_main(prefix: &str) {
    let ring = AdaptiveRing::open(prefix, 1, 1, CAPACITY).expect("open");
    let slot = ring.register_consumer().expect("register_consumer");
    let stop_flag = format!("{prefix}.stop");
    let mut tally = Tally::new();
    let mut buf = [0u8; 64];
    let mut idle_after_stop = 0u32;
    loop {
        match ring.try_recv(slot, &mut buf) {
            Ok(n) => {
                tally.record(&buf[..n]);
                idle_after_stop = 0;
            }
            Err(_) => {
                if std::path::Path::new(&stop_flag).exists() {
                    idle_after_stop += 1;
                    if idle_after_stop > 20_000 {
                        break;
                    }
                }
                std::hint::spin_loop();
            }
        }
    }
    ring.unregister_consumer(slot);
    let counts: Vec<String> = tally.count.iter().map(|c| c.to_string()).collect();
    println!(
        "RECV {} {} {}",
        slot,
        if tally.monotone { "monotone" } else { "INVERTED" },
        counts.join(",")
    );
}

fn spawn_role(exe: &str, role: &str, prefix: &str, items: u64) -> std::process::Child {
    Command::new(exe)
        .args([role, prefix, &items.to_string()])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn child")
}

fn wait_for_shape(
    ring: &AdaptiveRing,
    tally: &mut Tally,
    want: RingShape,
    label: &str,
    deadline: Instant,
) {
    let mut buf = [0u8; 64];
    while ring.current_shape() != want {
        if let Ok(n) = ring.try_recv(0, &mut buf) {
            tally.record(&buf[..n]);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the automatic {label} morph (still {:?})",
            ring.current_shape()
        );
    }
    println!("PASS: shape morphed to {want:?} automatically ({label})");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("producer") => {
            let items: u64 = args[3].parse().expect("items");
            producer_main(&args[2], items);
            return;
        }
        Some("consumer2") => {
            consumer2_main(&args[2]);
            return;
        }
        _ => {}
    }

    // ---- Driver: creator + first consumer + orchestrator ----
    let exe = std::env::current_exe().unwrap();
    let exe = exe.to_str().unwrap();
    let dir = std::env::temp_dir().join(format!("subetha_growth_{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let prefix = dir.join("ring");
    let prefix = prefix.to_str().unwrap().to_owned();
    let stop_flag = format!("{prefix}.stop");

    // The whole point: the hint is ONE producer / ONE consumer. No
    // grammar declared, so nothing below is allowed to fail.
    let ring = AdaptiveRing::create(&prefix, 1, 1, CAPACITY).expect("create");
    let my_slot = ring.register_consumer().expect("register_consumer");
    assert_eq!(my_slot, 0);

    let deadline = Instant::now() + DEADLINE;
    let mut tally = Tally::new();
    let mut buf = [0u8; 64];

    // Stage 1: one producer -> the ring settles on SPSC.
    let child_a = spawn_role(exe, "producer", &prefix, ITEMS_A);
    wait_for_shape(&ring, &mut tally, RingShape::Spsc, "1P/1C", deadline);

    // Stage 2: two more producer PROCESSES join mid-stream. Slots 1
    // and 2 are PAST the construction hint - registration grows the
    // ring instead of erroring, and the shape follows on its own.
    let child_b = spawn_role(exe, "producer", &prefix, ITEMS_B);
    wait_for_shape(&ring, &mut tally, RingShape::Mpsc, "2P/1C growth", deadline);
    let child_c = spawn_role(exe, "producer", &prefix, ITEMS_C);
    let mut spins = 0u64;
    while ring.published_producers() < 3 {
        if let Ok(n) = ring.try_recv(0, &mut buf) {
            tally.record(&buf[..n]);
        }
        spins += 1;
        assert!(spins < u64::MAX && Instant::now() < deadline,
                "timed out waiting for 3rd producer backing to publish");
    }
    println!(
        "PASS: ring grew to {} published producer backings (hint was 1, zero errors)",
        ring.published_producers()
    );

    // Stage 3: a second consumer PROCESS joins -> MPMC, ownership
    // rebalances between the two consumers.
    let child_d = spawn_role(exe, "consumer2", &prefix, 0);
    wait_for_shape(&ring, &mut tally, RingShape::Mpmc, "2C join", deadline);

    // Drain while the producers finish, then signal stop and drain
    // the tail.
    let mut children = [(child_a, ITEMS_A), (child_b, ITEMS_B), (child_c, ITEMS_C)];
    for (child, _) in children.iter_mut() {
        loop {
            if let Ok(n) = ring.try_recv(0, &mut buf) {
                tally.record(&buf[..n]);
                continue;
            }
            if let Some(status) = child.try_wait().expect("try_wait") {
                assert!(status.success(), "producer child failed");
                break;
            }
            assert!(Instant::now() < deadline, "timed out draining");
        }
    }
    std::fs::write(&stop_flag, b"1").expect("stop flag");
    let mut idle = 0u32;
    while idle < 20_000 {
        match ring.try_recv(0, &mut buf) {
            Ok(n) => {
                tally.record(&buf[..n]);
                idle = 0;
            }
            Err(_) => idle += 1,
        }
    }

    // Collect the second consumer's tally from its stdout.
    let out = child_d.wait_with_output().expect("consumer2 output");
    assert!(out.status.success(), "consumer2 child failed");
    let mut d_counts = [0u64; MAX_SLOTS];
    let mut d_monotone = false;
    for line in out.stdout.lines() {
        let line = line.unwrap();
        if let Some(rest) = line.strip_prefix("RECV ") {
            let parts: Vec<&str> = rest.split(' ').collect();
            d_monotone = parts[1] == "monotone";
            for (i, c) in parts[2].split(',').enumerate() {
                d_counts[i] = c.parse().unwrap();
            }
        }
    }

    // Stage 4: exactly-once accounting across both consumer processes.
    let sent = [ITEMS_A, ITEMS_B, ITEMS_C];
    for (slot, want) in sent.iter().enumerate() {
        let got = tally.count[slot] + d_counts[slot];
        assert_eq!(
            got, *want,
            "producer {slot}: sent {want}, consumers received {got} \
             (driver {} + joiner {})",
            tally.count[slot], d_counts[slot]
        );
    }
    assert!(tally.monotone, "driver consumer saw a per-producer seq inversion");
    assert!(d_monotone, "joining consumer saw a per-producer seq inversion");
    println!(
        "PASS: exactly-once across 2 consumer processes \
         (driver {:?} + joiner {:?} == sent {:?})",
        &tally.count[..3], &d_counts[..3], sent
    );
    println!("PASS: per-producer FIFO monotone on both consumers");

    std::fs::remove_dir_all(&dir).ok();
    println!("ALL PASS: automatic growth + shape morphs, zero registration errors");
}
