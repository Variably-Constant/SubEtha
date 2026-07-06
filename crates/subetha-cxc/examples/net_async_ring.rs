//! async `recv().await` woken by a NETWORK packet - no async runtime,
//! no tokio. The last leg of the universal interface: the same
//! `recv().await` that resolves on a thread's push or a sibling
//! process's push also resolves on a remote host's packet, because all
//! three reduce to "the consumer ring advanced, fire the local Waker."
//!
//! Topology (one process here, real localhost TCP between the halves):
//!
//!   feeder thread -> producer ring -> [client thread: ring -> TCP socket]
//!        --- network ---
//!   [server thread: TCP socket -> consumer ring + fire waker]
//!        -> reactor thread -> parked recv().await -> consumer
//!
//! The producer pauses a few times (15 ms each), so the consumer's
//! `recv().await` genuinely parks on an empty ring (its driver thread
//! and reactor thread both asleep in the kernel) and resumes when the
//! next network packet arrives and the server fires the consumer's
//! waker. The reactor's wait is heal-bounded (a 50 ms backstop so a
//! lost wake cannot hang it), but the packet-driven wake is what
//! resumes each pause: completion well inside the 5 x 50 ms heal floor
//! shows the wake itself fired, not the backstop.
//!
//! Run:
//!     cargo run --release --example net_async_ring -p subetha-cxc

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use subetha_cxc::cross_process_waker::{CrossProcessWaker, MAX_WAITERS_DEFAULT};
use subetha_cxc::net_bridge::{serve_one, ship};
use subetha_cxc::reactor::{block_on, receiver_cross};
use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

const N: u64 = 200_000;
const CAP: usize = 1024;
const PAUSES: u64 = 5;

fn main() {
    // Consumer side: its own ring + waker + reactor. The reactor bridges
    // ring-advance to the parked future's Waker, blind to whether the
    // advance came from a thread, a process, or - here - a packet.
    let consumer_ring = Arc::new(SpscRingCore::create_anon(CAP).expect("consumer ring"));
    let xwaker = Arc::new(
        CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT).expect("waker"),
    );
    let rx = receiver_cross(Arc::clone(&consumer_ring), Arc::clone(&xwaker));

    // Producer side: a ring the client drains onto the wire.
    let producer_ring = Arc::new(SpscRingCore::create_anon(CAP).expect("producer ring"));

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");

    println!("== async recv().await woken by a network packet (no tokio) ==");
    println!("listening on {addr}");
    println!("streaming {N} items ring -> TCP -> ring -> reactor -> parked recv().await\n");

    let t0 = Instant::now();

    // Server thread: socket -> consumer ring + fire the consumer's waker.
    let server = {
        let consumer_ring = Arc::clone(&consumer_ring);
        let xwaker = Arc::clone(&xwaker);
        std::thread::spawn(move || {
            serve_one(&listener, &consumer_ring, &xwaker).expect("serve")
        })
    };

    // Feeder thread: items -> producer ring, pausing a few times so the
    // consumer parks awaiting the next packet.
    let feeder = {
        let producer_ring = Arc::clone(&producer_ring);
        std::thread::spawn(move || {
            let pause_every = N / (PAUSES + 1);
            let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
            for i in 0..N {
                if i > 0 && i.is_multiple_of(pause_every) {
                    std::thread::sleep(Duration::from_millis(15));
                }
                buf[..8].copy_from_slice(&i.to_le_bytes());
                while producer_ring.try_push(&buf).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    };

    // Client thread: producer ring -> TCP socket.
    let client = std::thread::spawn(move || {
        ship(addr, producer_ring, N).expect("ship");
    });

    // Consumer: parked recv().await, driven only by the arriving packets.
    let sum = block_on(async move {
        let mut s = 0u64;
        for expected in 0..N {
            let item = rx.recv().await;
            let seq = u64::from_le_bytes(item[..8].try_into().unwrap());
            assert_eq!(seq, expected, "network FIFO order violated");
            s = s.wrapping_add(seq);
        }
        s
    });

    feeder.join().expect("feeder");
    client.join().expect("client");
    let received = server.join().expect("server");
    let elapsed = t0.elapsed();

    assert_eq!(received, N, "server received all items");
    assert_eq!(sum, (0..N).sum(), "consumer checksum");

    println!("consumer drained all {N} items in order, woken by network packets \
              across {PAUSES} producer pauses, in {elapsed:?}");
    println!("{:.2} M items/s end to end (ring -> TCP -> ring -> parked recv), integrity OK",
             N as f64 / elapsed.as_secs_f64() / 1e6);
    println!("no async runtime: blocking std::net sockets on threads, SubEtha reactor for the wake.");
}
