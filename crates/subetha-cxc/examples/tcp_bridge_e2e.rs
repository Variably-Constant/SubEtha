//! End-to-end TcpBridge: two SpscRingCores on localhost, bytes flow
//! through a TCP connection. Demonstrates the substrate's TcpBridge
//! primitive pair as the QUIC bridge's lighter-weight TCP sibling.
//!
//! Run with:
//!     cargo run --release --example tcp_bridge_e2e --features tcp-bridge

#![cfg(feature = "tcp-bridge")]

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::tcp_bridge::{TcpBridgeClient, TcpBridgeServer};
use subetha_cxc::AdaptiveRing;

const N_ITEMS: u64 = 100_000;
const RING_CAPACITY: usize = 4096;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TcpBridge E2E: {N_ITEMS} items across plain TCP on 127.0.0.1");
    println!();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    rt.block_on(run_demo())
}

async fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    let producer_ring = Arc::new(
        AdaptiveRing::create_anon(1, 1, RING_CAPACITY)
            .map_err(|e| format!("producer create: {e:?}"))?,
    );
    producer_ring.register_producer().map_err(|e| format!("p reg: {e:?}"))?;
    producer_ring.register_consumer().map_err(|e| format!("c reg: {e:?}"))?;
    let consumer_ring = Arc::new(
        AdaptiveRing::create_anon(1, 1, RING_CAPACITY)
            .map_err(|e| format!("consumer create: {e:?}"))?,
    );
    consumer_ring.register_producer().map_err(|e| format!("p reg: {e:?}"))?;
    consumer_ring.register_consumer().map_err(|e| format!("c reg: {e:?}"))?;

    let server = TcpBridgeServer::bind(
        consumer_ring.clone(),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
    )
    .await?;
    let server_addr = server.local_addr()?;
    println!("[server] listening on {server_addr}");

    let client = TcpBridgeClient::new(producer_ring.clone(), server_addr);

    let consumed = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let server_task = tokio::spawn(async move {
        let n = server.accept_one().await?;
        println!("[server] received {n} items");
        Ok::<u64, subetha_cxc::tcp_bridge::TcpBridgeError>(n)
    });

    let client_task = tokio::spawn(async move {
        client.run(N_ITEMS).await?;
        println!("[client] shipped {N_ITEMS} items");
        Ok::<(), subetha_cxc::tcp_bridge::TcpBridgeError>(())
    });

    let producer_for_app = producer_ring.clone();
    let consumer_for_app = consumer_ring.clone();
    let consumed_for_app = consumed.clone();
    let shutdown_for_app = shutdown.clone();
    let app_handle = std::thread::spawn(move || -> Result<u64, String> {
        let drain_c = consumer_for_app.clone();
        let drain_cnt = consumed_for_app.clone();
        let drain_shut = shutdown_for_app.clone();
        let drain = std::thread::spawn(move || -> u64 {
            let mut sum: u64 = 0;
            let mut out = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
            while drain_cnt.load(Ordering::Acquire) < N_ITEMS
                && !drain_shut.load(Ordering::Acquire)
            {
                if drain_c.try_recv(0, &mut out).is_ok() {
                    let v = u64::from_le_bytes(out[..8].try_into().unwrap());
                    sum += v;
                    drain_cnt.fetch_add(1, Ordering::AcqRel);
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
        let sum = drain.join().map_err(|_| "drain panicked")?;
        Ok(sum)
    });

    let t0 = Instant::now();
    server_task.await??;
    client_task.await??;
    let app_sum = app_handle.join().map_err(|_| "app panicked")??;
    shutdown.store(true, Ordering::Release);

    let elapsed = t0.elapsed();
    let expected_sum: u64 = (0..N_ITEMS).sum();
    let total_consumed = consumed.load(Ordering::Acquire);
    let throughput = N_ITEMS as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Result ===");
    println!("  elapsed:      {elapsed:?}");
    println!("  items:        {N_ITEMS}");
    println!("  consumed:     {total_consumed}");
    println!("  app sum:      {app_sum}");
    println!("  expected sum: {expected_sum}");
    println!("  throughput:   {throughput:.0} items/s");

    assert_eq!(total_consumed, N_ITEMS, "count mismatch");
    assert_eq!(app_sum, expected_sum, "sum mismatch");
    println!("  integrity:    PASS");
    println!("    every item arrived exactly once across the TCP bridge");
    Ok(())
}

#[cfg(not(feature = "tcp-bridge"))]
fn main() {
    eprintln!("Enable the tcp-bridge Cargo feature to run this example.");
}
