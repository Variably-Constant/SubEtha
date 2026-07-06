//! End-to-end demo of [`BlockingTcpBridge`].
//!
//! Producer thread pushes items into a local `BlockingSpscRing`
//! (the "client-side" ring). A tokio task runs
//! `BlockingTcpBridgeClient::run` which calls `recv_blocking` on
//! the client ring; received bytes go over a TCP connection to a
//! `BlockingTcpBridgeServer` task in the same binary. The server
//! task calls `send_blocking` on a second `BlockingSpscRing` (the
//! "server-side" ring). A consumer thread drains the server ring
//! via `recv_blocking`.
//!
//! Two wake bridges are exercised:
//! - producer thread -> client-ring's consumer waker -> bridge
//!   client task's recv_blocking returns -> socket write.
//! - server task's send_blocking on server ring -> server-ring's
//!   producer waker -> consumer thread's recv_blocking returns.
//!
//! Asserts FIFO integrity across the full path + reports throughput.
//!
//! Run:
//!     cargo run --release --example blocking_tcp_bridge_e2e --features tcp-bridge

#![cfg(feature = "tcp-bridge")]

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::blocking_tcp_bridge::{
    BlockingTcpBridgeClient, BlockingTcpBridgeServer,
};
use subetha_cxc::BlockingSpscRing;

const N_ITEMS: u64 = 5_000;
const RING_CAPACITY: usize = 64;
const PRODUCER_PAUSE: Duration = Duration::from_micros(200);
const PRODUCER_BATCH: u64 = 16;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("=== BlockingTcpBridge E2E ===");
    println!("  items:      {N_ITEMS}");
    println!("  capacity:   {RING_CAPACITY}");
    println!();

    let client_ring = Arc::new(
        BlockingSpscRing::create_anon(RING_CAPACITY).expect("client ring"),
    );
    let server_ring = Arc::new(
        BlockingSpscRing::create_anon(RING_CAPACITY).expect("server ring"),
    );

    // Bind the server.
    let server = BlockingTcpBridgeServer::bind(
        Arc::clone(&server_ring),
        SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
    )
    .await
    .expect("server bind");
    let server_addr = server.local_addr().expect("server addr");

    let t0 = Instant::now();

    // Spawn the server task.
    let server_task = tokio::spawn(async move {
        server.accept_one().await.expect("server accept_one")
    });

    // Spawn the producer thread (writes into the client ring).
    let producer_ring = Arc::clone(&client_ring);
    let producer = thread::spawn(move || {
        for i in 0..N_ITEMS {
            let mut payload = [0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            producer_ring
                .send_blocking(&payload, Some(Duration::from_secs(5)))
                .expect("producer send");
            if (i + 1) % PRODUCER_BATCH == 0 {
                thread::sleep(PRODUCER_PAUSE);
            }
        }
    });

    // Spawn the consumer thread (reads from the server ring).
    let consumer_ring = Arc::clone(&server_ring);
    let consumer = thread::spawn(move || {
        let mut buf = [0u8; 64];
        let mut got: Vec<u64> = Vec::with_capacity(N_ITEMS as usize);
        for _ in 0..N_ITEMS {
            consumer_ring
                .recv_blocking(&mut buf, Some(Duration::from_secs(5)))
                .expect("consumer recv");
            got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
        }
        got
    });

    // Spawn the bridge client task (drains client ring, ships over TCP).
    let client_ring2 = Arc::clone(&client_ring);
    let bridge_client = tokio::spawn(async move {
        let client = BlockingTcpBridgeClient::new(client_ring2, server_addr);
        client.run(N_ITEMS).await.expect("bridge client run");
    });

    // Join everything.
    bridge_client.await.expect("bridge client join");
    let n_received = server_task.await.expect("server task join");
    producer.join().expect("producer join");
    let got = consumer.join().expect("consumer join");
    let elapsed = t0.elapsed();

    assert_eq!(n_received, N_ITEMS, "server reported wrong receive count");
    let expected: Vec<u64> = (0..N_ITEMS).collect();
    assert_eq!(got, expected, "FIFO integrity broke across the bridge");

    println!("=== Result ===");
    println!("  elapsed:    {elapsed:?}");
    println!(
        "  throughput: {:.2} K items/s",
        N_ITEMS as f64 / elapsed.as_secs_f64() / 1_000.0,
    );
    println!();
    println!("PASS - {N_ITEMS} items across producer -> blocking ring -> TCP -> blocking ring -> consumer");
}
