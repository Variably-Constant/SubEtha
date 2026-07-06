//! Use the substrate internally, Tokio externally. A SubEtha ring's
//! `recv().await` is a plain `std::future::Future`, so it runs on ANY
//! executor - including Tokio. This drives a SubEtha `ReactiveReceiver`
//! from a real `#[tokio::main]` runtime, with no SubEtha executor in
//! the picture at all.
//!
//! The consumer is a `tokio::spawn`ed task. It `.await`s items off the
//! SubEtha ring AND `.await`s `tokio::time::sleep` between batches, so
//! it is a first-class Tokio task coexisting with Tokio's own
//! primitives. When the producer pauses, the ring drains and the recv
//! future returns `Pending`, parking the Tokio task; the producer's
//! next send fires the task's (Tokio-supplied) `Waker` and Tokio
//! re-polls it. The substrate ring is the readiness source; Tokio is
//! the runtime.
//!
//! Run:
//!     cargo run --release --example tokio_interop -p subetha-cxc

use std::time::{Duration, Instant};

use subetha_cxc::reactor::anon_pair;
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

const N: u64 = 100_000;
const PAUSES: u64 = 5;

#[tokio::main]
async fn main() {
    let (tx, rx) = anon_pair(64).expect("channel");

    let expected: u64 = (0..N).sum();

    println!("== SubEtha ring driven by the Tokio runtime ==");
    println!("a tokio::spawn task awaits {N} items off a SubEtha ring");
    println!("(the recv future is std::future::Future - no SubEtha executor involved)\n");

    let t0 = Instant::now();

    // Producer: a plain OS thread streams items, pausing a few times so
    // the Tokio task genuinely parks on an empty ring and is re-woken by
    // the next send firing Tokio's Waker.
    let producer = std::thread::spawn(move || {
        let pause_every = N / (PAUSES + 1);
        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
        for i in 0..N {
            if i > 0 && i.is_multiple_of(pause_every) {
                std::thread::sleep(Duration::from_millis(15));
            }
            buf[..8].copy_from_slice(&i.to_le_bytes());
            while tx.try_send(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    // Consumer: a Tokio task. It interleaves the SubEtha ring await with
    // a Tokio timer await to prove both run on the same Tokio runtime.
    let consumer = tokio::spawn(async move {
        let mut sum = 0u64;
        let mut received = 0u64;
        let yield_every = N / 10;
        for _ in 0..N {
            let item = rx.recv().await;
            sum = sum.wrapping_add(u64::from_le_bytes(item[..8].try_into().unwrap()));
            received += 1;
            if received.is_multiple_of(yield_every) {
                // A Tokio-native await, in the same task as the SubEtha
                // await - they share the runtime.
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }
        sum
    });

    let sum = consumer.await.expect("consumer task");
    producer.join().expect("producer thread");
    let elapsed = t0.elapsed();

    assert_eq!(sum, expected, "every item delivered exactly once");

    println!("Tokio task drained all {N} items off the SubEtha ring in {elapsed:?}");
    println!("{:.2} M items/s, integrity OK", N as f64 / elapsed.as_secs_f64() / 1e6);
    println!("the same recv().await ran on Tokio that runs on SubEtha's own \
              executor - the future is executor-agnostic.");
}
