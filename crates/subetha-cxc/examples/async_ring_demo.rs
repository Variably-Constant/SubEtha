//! End-to-end async demo of [`AsyncSpscRing`].
//!
//! Spawns producer + consumer tasks using a hand-rolled
//! executor-agnostic block-on (so no tokio dependency is needed
//! to demonstrate the unlock). The consumer uses `recv(timeout)
//! .await`; the producer uses `send(payload, timeout).await`. The
//! ring is configured small (capacity 16) and the producer paces
//! itself so the consumer hits the park path on most calls.
//!
//! Asserts at the end:
//! - every item delivered in send order (FIFO integrity)
//! - measured park count > 0 (otherwise the async future returned
//!   Poll::Ready on first poll and the wake bridge was never
//!   exercised, so the test does not prove the async unlock)
//!
//! Run:
//!     cargo run --release --example async_ring_demo

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::{AsyncSpscRing, BlockingSpscRing};

const N_ITEMS: u64 = 5_000;
const RING_CAPACITY: usize = 16;
const PRODUCER_PAUSE: Duration = Duration::from_micros(300);
const PRODUCER_BATCH: u64 = 8;

/// Per-task waker counter; lets us assert that the future returned
/// Pending at least once (the async unlock).
struct CountingWaker {
    pending_returns: AtomicU64,
    woken: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        let mut g = self.woken.lock().unwrap();
        *g = true;
        self.cv.notify_one();
    }
}

fn block_on<F: Future>(mut fut: F, counter: &Arc<CountingWaker>) -> F::Output {
    // Re-arm the wake flag so each call's first poll always runs.
    // Without this, the wake flag stays false after the previous
    // call drained it, and the loop blocks on the condvar forever
    // because the new future has not been polled yet (so no one
    // is going to wake it).
    *counter.woken.lock().unwrap() = true;

    let waker: Waker = Arc::clone(counter).into();
    let mut cx = Context::from_waker(&waker);
    // SAFETY: future is on the stack and is never moved after this point.
    let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
    loop {
        {
            let mut g = counter.woken.lock().unwrap();
            while !*g {
                g = counter.cv.wait(g).unwrap();
            }
            *g = false;
        }
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => {
                counter.pending_returns.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }
    }
}

fn fresh_counter() -> Arc<CountingWaker> {
    Arc::new(CountingWaker {
        pending_returns: AtomicU64::new(0),
        // Start true so the first poll runs without an initial wake.
        woken: std::sync::Mutex::new(true),
        cv: std::sync::Condvar::new(),
    })
}

fn main() {
    println!("=== AsyncSpscRing E2E demo ===");
    println!("  items:                 {N_ITEMS}");
    println!("  capacity:              {RING_CAPACITY}");
    println!("  producer pause every:  {PRODUCER_BATCH} items");
    println!("  producer pause:        {:?}", PRODUCER_PAUSE);
    println!();

    let ring = Arc::new(BlockingSpscRing::create_anon(RING_CAPACITY).expect("ring"));
    let adapter = Arc::new(AsyncSpscRing::new(Arc::clone(&ring)));

    let t0 = Instant::now();

    let a_prod = Arc::clone(&adapter);
    let producer = thread::spawn(move || {
        let counter = fresh_counter();
        let mut pending_pushes: u64 = 0;
        for i in 0..N_ITEMS {
            let mut payload = vec![0u8; 56];
            payload[..8].copy_from_slice(&i.to_le_bytes());
            let before = counter.pending_returns.load(Ordering::Relaxed);
            block_on(a_prod.send(payload, Duration::from_secs(5)), &counter).expect("send");
            let after = counter.pending_returns.load(Ordering::Relaxed);
            if after > before {
                pending_pushes += 1;
            }
            if (i + 1) % PRODUCER_BATCH == 0 {
                thread::sleep(PRODUCER_PAUSE);
            }
        }
        pending_pushes
    });

    let a_cons = Arc::clone(&adapter);
    let consumer = thread::spawn(move || {
        let counter = fresh_counter();
        let mut got: Vec<u64> = Vec::with_capacity(N_ITEMS as usize);
        let mut pending_recvs: u64 = 0;
        for _ in 0..N_ITEMS {
            let before = counter.pending_returns.load(Ordering::Relaxed);
            let bytes = block_on(a_cons.recv(Duration::from_secs(5)), &counter).expect("recv");
            let after = counter.pending_returns.load(Ordering::Relaxed);
            if after > before {
                pending_recvs += 1;
            }
            let v = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            got.push(v);
        }
        (got, pending_recvs)
    });

    let pending_pushes = producer.join().expect("producer join");
    let (got, pending_recvs) = consumer.join().expect("consumer join");
    let elapsed = t0.elapsed();

    let expected: Vec<u64> = (0..N_ITEMS).collect();
    assert_eq!(got, expected, "FIFO integrity broke");
    assert!(
        pending_recvs > 0,
        "consumer side never returned Pending; the async wake bridge \
         was not exercised. Adjust producer pause / batch."
    );

    println!("=== Result ===");
    println!("  elapsed:               {elapsed:?}");
    println!(
        "  throughput:            {:.3} M items/s",
        N_ITEMS as f64 / elapsed.as_secs_f64() / 1_000_000.0,
    );
    println!("  producer pending:      {pending_pushes} ({:.1}% of sends parked)",
        pending_pushes as f64 / N_ITEMS as f64 * 100.0);
    println!("  consumer pending:      {pending_recvs} ({:.1}% of recvs parked)",
        pending_recvs as f64 / N_ITEMS as f64 * 100.0);
    println!();
    println!("PASS - {N_ITEMS} items via .recv().await, wake bridge exercised");
}
