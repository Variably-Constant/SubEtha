//! `net_bridge`: a TCP bridge with NO async runtime. One connection
//! ferries a producer ring on one host to a consumer ring on another,
//! using blocking `std::net` sockets on dedicated threads.
//!
//! Where the feature-gated `tcp_bridge` module uses `tokio::net`, this
//! path needs no executor at all. The bridge watches exactly one
//! socket, so a blocking `read()` on its own thread is the right shape:
//! it parks in the kernel until a packet arrives - the kernel's socket
//! wait IS the network reactor - costs zero CPU while idle, and pulls
//! in no runtime. Async socket I/O earns its keep multiplexing many
//! sockets on few threads; a single-connection ferry has nothing to
//! multiplex.
//!
//! # The wake hand-off
//!
//! The server reads slots off the wire, pushes them into the local
//! consumer ring, and fires the consumer's [`CrossProcessWaker`] once
//! per socket read that produced slots. That wake is what lets a
//! parked `recv().await` (driven by [`crate::reactor`]) resolve when a
//! NETWORK packet arrives - the same reactor that bridges a sibling
//! process's push bridges a remote host's packet, because both reduce
//! to "the consumer ring advanced, fire the local Waker."
//!
//! # Data path
//!
//! Egress burst-batches: every already-available producer slot (up to
//! [`EGRESS_BATCH_SLOTS`]) ships in one write. Ingress is chunked: each
//! `read` takes whatever the wire has, complete 64-byte slots are
//! pushed as they assemble, and a partial slot carries to the next
//! read. `TCP_NODELAY` is set on both ends.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

use crate::cross_process_waker::CrossProcessWaker;
use crate::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

/// One wire slot equals one ring payload.
const SLOT: usize = SPSC_PAYLOAD_BYTES;

/// Slots per batched egress write (16 KiB of payload per syscall at the
/// 64-byte slot size).
pub const EGRESS_BATCH_SLOTS: usize = 256;

/// Ingress socket-read buffer in bytes.
const INGRESS_BUF_BYTES: usize = 64 * 1024;

/// Connect to `addr` and ship `n_items` slots drained from
/// `producer_ring` across a blocking TCP connection. Burst-batched:
/// every already-available slot (up to [`EGRESS_BATCH_SLOTS`]) goes out
/// in one write; a lone item ships immediately.
pub fn ship(
    addr: SocketAddr,
    producer_ring: Arc<SpscRingCore>,
    n_items: u64,
) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;
    // Frame header: 8-byte big-endian item count.
    stream.write_all(&n_items.to_be_bytes())?;

    let mut batch = vec![0u8; EGRESS_BATCH_SLOTS * SLOT];
    let mut slot = [0u8; SLOT];
    let mut shipped: u64 = 0;
    while shipped < n_items {
        let budget = EGRESS_BATCH_SLOTS.min((n_items - shipped) as usize);
        let mut filled = 0usize;
        while filled < budget {
            match producer_ring.try_pop(&mut slot) {
                Ok(_) => {
                    batch[filled * SLOT..(filled + 1) * SLOT]
                        .copy_from_slice(&slot);
                    filled += 1;
                }
                Err(_) => break,
            }
        }
        if filled == 0 {
            // Producer ring momentarily empty; yield and retry.
            std::thread::yield_now();
            continue;
        }
        stream.write_all(&batch[..filled * SLOT])?;
        shipped += filled as u64;
    }
    stream.shutdown(Shutdown::Write)?;
    Ok(())
}

/// Accept one connection on `listener`, read its framed slot stream,
/// push each complete slot into `consumer_ring`, and fire `xwaker` once
/// per socket read that produced slots - waking the consumer's reactor
/// so a parked `recv().await` resolves on the arriving packet. Returns
/// the count of items received.
///
/// The blocking `accept` + `read` are the network reactor: the thread
/// parks in the kernel until the peer connects / sends, then signals
/// the consumer ring.
pub fn serve_one(
    listener: &TcpListener,
    consumer_ring: &Arc<SpscRingCore>,
    xwaker: &Arc<CrossProcessWaker>,
) -> std::io::Result<u64> {
    let (mut stream, _) = listener.accept()?;
    stream.set_nodelay(true)?;
    let mut header = [0u8; 8];
    stream.read_exact(&mut header)?;
    let total: u64 = u64::from_be_bytes(header);

    let mut buf = vec![0u8; INGRESS_BUF_BYTES];
    let mut carry: Vec<u8> = Vec::with_capacity(SLOT);
    let mut received: u64 = 0;
    while received < total {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "peer closed before sending all framed items",
            ));
        }
        let mut pushed_any = false;
        let mut data: &[u8] = &buf[..n];

        // Complete a carried-over partial slot first.
        if !carry.is_empty() {
            let need = SLOT - carry.len();
            let take = need.min(data.len());
            carry.extend_from_slice(&data[..take]);
            data = &data[take..];
            if carry.len() == SLOT {
                push_spin(consumer_ring, xwaker, &carry);
                carry.clear();
                received += 1;
                pushed_any = true;
            }
        }
        // Drain whole slots out of this read.
        while data.len() >= SLOT && received < total {
            push_spin(consumer_ring, xwaker, &data[..SLOT]);
            data = &data[SLOT..];
            received += 1;
            pushed_any = true;
        }
        // Stash any trailing partial slot for the next read.
        if !data.is_empty() {
            carry.extend_from_slice(data);
        }

        // One wake per read that landed slots: the reactor coalesces it
        // into a single local Waker fire and the consumer drains all
        // newly-available items.
        if pushed_any {
            xwaker.wake_up_to(consumer_ring.head());
        }
    }
    Ok(total)
}

fn push_spin(ring: &SpscRingCore, xwaker: &CrossProcessWaker, slot: &[u8]) {
    while ring.try_push(slot).is_err() {
        // Ring full and a parked consumer has not yet been signalled for
        // this read (the end-of-read wake fires later). Wake it now so it
        // drains, else this push and the parked recv() would deadlock.
        xwaker.wake_up_to(ring.head());
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cross_process_waker::MAX_WAITERS_DEFAULT;
    use crate::reactor::{block_on, receiver_cross};

    #[test]
    fn network_packet_wakes_parked_recv() {
        // A real localhost TCP connection ferries items into a consumer
        // ring; the parked recv().await is woken by the server's signal,
        // which is driven by the arriving packet. No async runtime.
        const N: u64 = 5_000;
        const CAP: usize = 256;

        let consumer_ring = Arc::new(SpscRingCore::create_anon(CAP).unwrap());
        let xwaker = Arc::new(
            CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT).unwrap(),
        );
        let rx = receiver_cross(Arc::clone(&consumer_ring), Arc::clone(&xwaker));

        let producer_ring = Arc::new(SpscRingCore::create_anon(CAP).unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server thread: socket -> consumer ring + signal.
        let server = {
            let consumer_ring = Arc::clone(&consumer_ring);
            let xwaker = Arc::clone(&xwaker);
            std::thread::spawn(move || {
                serve_one(&listener, &consumer_ring, &xwaker).unwrap()
            })
        };

        // Feeder thread: items -> producer ring (with a couple of pauses
        // so the consumer genuinely parks awaiting a packet).
        let feeder = {
            let producer_ring = Arc::clone(&producer_ring);
            std::thread::spawn(move || {
                let mut buf = [0u8; SLOT];
                for i in 0..N {
                    if i > 0 && i % (N / 3) == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    buf[..8].copy_from_slice(&i.to_le_bytes());
                    while producer_ring.try_push(&buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        };

        // Client thread: producer ring -> socket.
        let client = std::thread::spawn(move || {
            ship(addr, producer_ring, N).unwrap();
        });

        // Consumer: parked recv().await, woken by the network packets.
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

        feeder.join().unwrap();
        client.join().unwrap();
        assert_eq!(server.join().unwrap(), N);
        assert_eq!(sum, (0..N).sum());
    }

    #[test]
    fn tiny_consumer_ring_does_not_deadlock_on_full() {
        // A 4-slot consumer ring against a batch-shipping client forces
        // the server's push_spin to hit Full constantly while the
        // consumer is often parked. Without the wake-on-Full hand-off
        // this deadlocks; with it, every item still drains.
        const N: u64 = 4_000;
        const CAP: usize = 4;

        let consumer_ring = Arc::new(SpscRingCore::create_anon(CAP).unwrap());
        let xwaker = Arc::new(
            CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT).unwrap(),
        );
        let rx = receiver_cross(Arc::clone(&consumer_ring), Arc::clone(&xwaker));
        let producer_ring = Arc::new(SpscRingCore::create_anon(CAP).unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = {
            let consumer_ring = Arc::clone(&consumer_ring);
            let xwaker = Arc::clone(&xwaker);
            std::thread::spawn(move || {
                serve_one(&listener, &consumer_ring, &xwaker).unwrap()
            })
        };
        let feeder = {
            let producer_ring = Arc::clone(&producer_ring);
            std::thread::spawn(move || {
                let mut buf = [0u8; SLOT];
                for i in 0..N {
                    buf[..8].copy_from_slice(&i.to_le_bytes());
                    while producer_ring.try_push(&buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        };
        let client = std::thread::spawn(move || {
            ship(addr, producer_ring, N).unwrap();
        });

        let sum = block_on(async move {
            let mut s = 0u64;
            for expected in 0..N {
                let item = rx.recv().await;
                let seq = u64::from_le_bytes(item[..8].try_into().unwrap());
                assert_eq!(seq, expected, "FIFO order violated under Full pressure");
                s = s.wrapping_add(seq);
            }
            s
        });

        feeder.join().unwrap();
        client.join().unwrap();
        assert_eq!(server.join().unwrap(), N);
        assert_eq!(sum, (0..N).sum());
    }
}
