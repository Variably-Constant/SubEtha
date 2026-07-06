//! End-to-end QUIC bridge: two SpscRingCores on localhost, bytes
//! flow through a real QUIC connection across real UDP datagrams.
//!
//! Demonstrates the substrate's `QuicBridgeClient` + `QuicBridgeServer`
//! primitive pair. Application code on the producer side calls
//! `producer_ring.try_push`; the bridge ships bytes across the wire;
//! consumer code on the other side calls `consumer_ring.try_pop`.
//! The bridge is invisible to application code.
//!
//! Egress is burst-batched: the client drains every already-
//! available ring slot into one contiguous buffer and hands quinn
//! a single multi-slot write, so the per-item cost is a 64-byte
//! memcpy instead of a per-slot await through the reactor.
//!
//! Run with:
//!     cargo run --release --example quic_bridge_e2e --features quic-bridge

#![cfg(feature = "quic-bridge")]

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::quic_bridge::{
    install_default_crypto_provider, make_self_signed_pair, QuicBridgeClient,
    QuicBridgeServer,
};
use subetha_cxc::AdaptiveRing;

const N_ITEMS: u64 = 100_000;
const RING_CAPACITY: usize = 4096;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_default_crypto_provider();

    println!("QUIC bridge E2E: {N_ITEMS} items across real QUIC on 127.0.0.1");
    println!("(burst-batched egress: ring backlog -> one quinn::write_all per batch)");
    println!();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    rt.block_on(run_demo())
}

async fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    let (server_config, client_config) = make_self_signed_pair("localhost")?;

    // Local AdaptiveRings for both halves (the substrate's default
    // ring type; shape-morphing + pin protocol come along for free).
    // 1P/1C registration -> initial SPSC shape.
    let producer_ring = Arc::new(
        AdaptiveRing::create_anon(1, 1, RING_CAPACITY)
            .map_err(|e| format!("producer ring create: {e:?}"))?,
    );
    producer_ring.register_producer().map_err(|e| format!("p reg: {e:?}"))?;
    producer_ring.register_consumer().map_err(|e| format!("c reg: {e:?}"))?;
    let consumer_ring = Arc::new(
        AdaptiveRing::create_anon(1, 1, RING_CAPACITY)
            .map_err(|e| format!("consumer ring create: {e:?}"))?,
    );
    consumer_ring.register_producer().map_err(|e| format!("p reg: {e:?}"))?;
    consumer_ring.register_consumer().map_err(|e| format!("c reg: {e:?}"))?;

    let server = QuicBridgeServer::bind(
        consumer_ring.clone(),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        server_config,
    )?;
    let server_addr = server.local_addr()?;
    println!("[server] listening on {server_addr}");

    let client = QuicBridgeClient::new(
        producer_ring.clone(),
        server_addr,
        client_config,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
    );

    let consumed = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    // Server task: accept one connection + drain.
    let server_task = tokio::spawn(async move {
        let n = server.accept_one().await?;
        println!("[server] received {n} items into consumer_ring");
        Ok::<u64, subetha_cxc::quic_bridge::QuicBridgeError>(n)
    });

    // Client task: ship N items via burst-batched egress.
    let client_task = tokio::spawn(async move {
        client.run(N_ITEMS, "localhost").await?;
        println!("[client] shipped + finished {N_ITEMS} items across QUIC");
        Ok::<(), subetha_cxc::quic_bridge::QuicBridgeError>(())
    });

    // Application code (separate OS thread): push N items into
    // producer_ring; pop N items off consumer_ring. The QUIC bridge
    // is invisible at the API surface.
    let producer_for_app = producer_ring.clone();
    let consumer_for_app = consumer_ring.clone();
    let consumed_for_app = consumed.clone();
    let shutdown_for_app = shutdown.clone();
    let app_handle = std::thread::spawn(move || -> Result<u64, String> {
        let drain_consumer = consumer_for_app.clone();
        let drain_consumed = consumed_for_app.clone();
        let drain_shutdown = shutdown_for_app.clone();
        let drain = std::thread::spawn(move || -> u64 {
            let mut sum: u64 = 0;
            let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
            while drain_consumed.load(Ordering::Acquire) < N_ITEMS
                && !drain_shutdown.load(Ordering::Acquire)
            {
                if drain_consumer.try_recv(0, &mut out).is_ok() {
                    let v = u64::from_le_bytes(out[..8].try_into().unwrap());
                    sum += v;
                    drain_consumed.fetch_add(1, Ordering::AcqRel);
                } else {
                    std::hint::spin_loop();
                }
            }
            sum
        });

        for i in 0..N_ITEMS {
            let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
            buf[..8].copy_from_slice(&i.to_le_bytes());
            while producer_for_app.try_send(0, &buf).is_err() {
                std::hint::spin_loop();
            }
        }
        let sum = drain.join().map_err(|_| "drain thread panicked")?;
        Ok(sum)
    });

    let t0 = Instant::now();

    server_task.await??;
    client_task.await??;
    let app_sum = app_handle.join().map_err(|_| "app thread panicked")??;
    shutdown.store(true, Ordering::Release);

    let elapsed = t0.elapsed();
    let expected_sum = (0..N_ITEMS).sum::<u64>();
    let total_consumed = consumed.load(Ordering::Acquire);
    let throughput = N_ITEMS as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Result ===");
    println!("  elapsed:               {elapsed:?}");
    println!("  items shipped:         {N_ITEMS}");
    println!("  items consumed:        {total_consumed}");
    println!("  application sum:       {app_sum}");
    println!("  expected sum:          {expected_sum}");
    println!("  throughput:            {throughput:.0} items/s");

    assert_eq!(total_consumed, N_ITEMS, "INTEGRITY FAIL: count mismatch");
    assert_eq!(app_sum, expected_sum, "INTEGRITY FAIL: sum mismatch");
    println!("  integrity:             PASS");
    println!("    every item arrived exactly once across the QUIC bridge");
    println!("    egress used burst-batched writes (one stream write per ring backlog)");

    // Give the QUIC endpoint a moment to flush close frames.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
