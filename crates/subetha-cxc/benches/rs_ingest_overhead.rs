//! Ingest throughput for the block-RS decoder, per datagram.
//!
//! `Decoder::ingest` runs once per received datagram and is the receiver's
//! hot path, so the accounting it carries - the per-reason refusal counters
//! and the seen / refused block-id ranges - is paid on every datagram whether
//! or not anything is wrong. This measures what that path costs.
//!
//! Three arms, because a datagram takes a different amount of that path
//! depending on where it lands:
//!
//!  - **accept**: a shard for a live block, the full path through the session
//!    gate, the shape checks, the window insert and the decode attempt.
//!  - **refuse-delivered**: a shard for a block already emitted, which is what
//!    a stalled or duplicate-heavy link actually carries. Exits at the
//!    delivered gate.
//!  - **refuse-epoch**: a shard stamped with another session's epoch, the
//!    earliest exit that still parses a header.
//!
//! The two refusal arms are the ones the accounting is FOR, so they are the
//! ones whose cost has to be honest: a counter that makes the refusal path
//! expensive would penalise exactly the link that is already in trouble.

use std::hint::black_box;
use std::time::Instant;

use subetha_cxc::reliable_udp::{Decoder, Encoder, EPOCH_OFFSET};

const SHARD: usize = 1200;
const K: usize = 8;
const R: usize = 2;

/// Datagrams for `blocks` worth of items, as one flat batch.
fn build(blocks: usize) -> Vec<Vec<u8>> {
    let mut enc = Encoder::new(K, R, SHARD);
    let mut pkts = Vec::new();
    for i in 0..(K * blocks) as u64 {
        let mut item = vec![0u8; SHARD];
        item[..8].copy_from_slice(&i.to_le_bytes());
        pkts.extend(enc.push(&item));
    }
    pkts.extend(enc.flush());
    pkts
}

/// Run `f` over `pkts` repeatedly for at least `min_ms`, returning
/// nanoseconds per datagram.
fn per_datagram(min_ms: u128, pkts: &[Vec<u8>], mut f: impl FnMut(&[u8])) -> f64 {
    // Warm up: the first pass faults the buffers in and trains the branches.
    for p in pkts {
        f(p);
    }
    let start = Instant::now();
    let mut n = 0u64;
    while start.elapsed().as_millis() < min_ms {
        for p in pkts {
            f(p);
            n += 1;
        }
    }
    start.elapsed().as_nanos() as f64 / n as f64
}

fn main() {
    println!("block-RS decoder ingest, k={K} r={R} shard={SHARD}B\n");

    let pkts = build(16);

    // accept: a fresh decoder per pass would reallocate, so measure the
    // steady state - one decoder taking the same batch, where every datagram
    // after the first pass is a duplicate. Split out below; this arm is the
    // first-arrival cost, measured over one pass at a time.
    let accept_ns = {
        let mut total = 0.0;
        let mut passes = 0u32;
        let start = Instant::now();
        while start.elapsed().as_millis() < 200 {
            let mut dec = Decoder::new();
            let t = Instant::now();
            for p in &pkts {
                black_box(dec.on_packet(p));
            }
            total += t.elapsed().as_nanos() as f64 / pkts.len() as f64;
            passes += 1;
        }
        total / passes as f64
    };

    // refuse-delivered: drive one decoder past the whole batch, then feed it
    // again - every datagram is now below the delivery frontier.
    let refuse_delivered_ns = {
        let mut dec = Decoder::new();
        for p in &pkts {
            black_box(dec.on_packet(p));
        }
        per_datagram(200, &pkts, |p| {
            black_box(dec.on_packet(p));
        })
    };

    // refuse-epoch: a decoder that has latched a different session.
    let refuse_epoch_ns = {
        let mut foreign: Vec<Vec<u8>> = pkts.clone();
        for p in &mut foreign {
            let e = u32::from_le_bytes([
                p[EPOCH_OFFSET],
                p[EPOCH_OFFSET + 1],
                p[EPOCH_OFFSET + 2],
                p[EPOCH_OFFSET + 3],
            ])
            .wrapping_add(1);
            p[EPOCH_OFFSET..EPOCH_OFFSET + 4].copy_from_slice(&e.to_le_bytes());
        }
        let mut dec = Decoder::new();
        black_box(dec.on_packet(&pkts[0]));
        per_datagram(200, &foreign, |p| {
            black_box(dec.on_packet(p));
        })
    };

    // Bytes per nanosecond IS gigabytes per second.
    let gbps = |ns: f64| SHARD as f64 / ns;
    println!("  accept            {accept_ns:8.1} ns/datagram   {:8.1} GB/s", gbps(accept_ns));
    println!(
        "  refuse-delivered  {refuse_delivered_ns:8.1} ns/datagram   {:8.1} GB/s",
        gbps(refuse_delivered_ns)
    );
    println!(
        "  refuse-epoch      {refuse_epoch_ns:8.1} ns/datagram   {:8.1} GB/s",
        gbps(refuse_epoch_ns)
    );
    println!(
        "\n  refusal is {:.1}x cheaper than acceptance (delivered gate), \
         {:.1}x (epoch gate)",
        accept_ns / refuse_delivered_ns,
        accept_ns / refuse_epoch_ns,
    );
}
