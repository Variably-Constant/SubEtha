//! `BlockingTcpBridge`: TCP forwarder pair built on
//! [`crate::blocking_spsc_ring::BlockingSpscRing`]
//! so neither the producer-side nor the consumer-side burns
//! scheduler slices polling an empty / full ring.
//!
//! Where the existing `TcpBridge` calls `tokio::task::yield_now`
//! when the local ring is empty (client side) or full (server
//! side), this primitive uses `recv_blocking` / `send_blocking` on
//! `BlockingSpscRing` via `tokio::task::spawn_blocking`. The
//! blocking call parks the worker thread on a SHARED `futex` (or
//! `WaitOnAddress`) and returns within microseconds of the next
//! ring event. Result: an idle bridge consumes zero CPU; a
//! freshly-published item ships across the wire one wake +
//! socket-write later.
//!
//! Cargo gate: `tcp-bridge` feature (matches the existing
//! `TcpBridge` primitive).

#![cfg(feature = "tcp-bridge")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::blocking_spsc_ring::{BlockingError, BlockingSpscRing};
use crate::shared_ring::RingError;

/// Errors returned by the blocking TCP bridge halves
/// ([`BlockingTcpBridgeClient`] / [`BlockingTcpBridgeServer`]).
#[derive(Debug)]
pub enum BlockingTcpBridgeError {
    Io(std::io::Error),
    Blocking(BlockingError),
    Ring(RingError),
    Closed,
}

impl std::fmt::Display for BlockingTcpBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Blocking(e) => write!(f, "blocking ring: {e:?}"),
            Self::Ring(e) => write!(f, "ring: {e:?}"),
            Self::Closed => write!(f, "connection closed"),
        }
    }
}

impl std::error::Error for BlockingTcpBridgeError {}

impl From<std::io::Error> for BlockingTcpBridgeError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}
impl From<BlockingError> for BlockingTcpBridgeError {
    fn from(e: BlockingError) -> Self { Self::Blocking(e) }
}

/// Default per-call timeout for the inner recv/send-blocking
/// calls. Bounds the worker-thread lifetime so a hung peer cannot
/// strand the bridge indefinitely; the bridge loops on Timeout
/// internally and re-tries until the caller's `n_items` budget is
/// satisfied.
const TICK_TIMEOUT: Duration = Duration::from_secs(5);

/// Slots per batched egress write. The blocking client parks for
/// the FIRST item (the zero-CPU-idle property), then burst-drains
/// every slot already in the ring via `try_pop` before paying one
/// socket write.
pub const EGRESS_BATCH_SLOTS: usize = 256;

/// Ingress socket-read buffer in bytes.
const INGRESS_BUF_BYTES: usize = 64 * 1024;

const SLOT: usize = crate::spsc_ring::SPSC_PAYLOAD_BYTES;

/// Client half: drains items from a local producer ring + ships
/// them across a TCP connection.
pub struct BlockingTcpBridgeClient {
    producer_ring: Arc<BlockingSpscRing>,
    server_addr: SocketAddr,
}

impl BlockingTcpBridgeClient {
    pub fn new(producer_ring: Arc<BlockingSpscRing>, server_addr: SocketAddr) -> Self {
        Self { producer_ring, server_addr }
    }

    /// Connect + ship `n_items` items. The bridge parks on the
    /// ring's `recv_blocking` (off the runtime thread) until the
    /// FIRST item arrives - an idle bridge consumes zero CPU - then
    /// burst-drains every slot already in the ring via `try_pop`
    /// and ships the whole batch in one socket write.
    pub async fn run(&self, n_items: u64) -> Result<(), BlockingTcpBridgeError> {
        let stream = TcpStream::connect(self.server_addr).await?;
        // Nagle off: latency-sensitive slots; batched writes keep
        // segments MSS-filled under load.
        stream.set_nodelay(true)?;
        #[cfg(target_os = "linux")]
        crate::net_tune::tune_tcp_socket(std::os::fd::AsRawFd::as_raw_fd(&stream));
        let mut stream = stream;
        stream.write_all(&n_items.to_be_bytes()).await?;

        let mut shipped: u64 = 0;
        // One staging buffer for the whole stream, threaded through
        // each spawn_blocking round and back: a fresh
        // `vec![0u8; 16 KiB]` per batch pays an alloc + memset and
        // cold-line stores for every 256 slots; the reused buffer
        // stays cache-warm.
        let mut staging = vec![0u8; EGRESS_BATCH_SLOTS * SLOT];
        while shipped < n_items {
            let ring = Arc::clone(&self.producer_ring);
            let budget = EGRESS_BATCH_SLOTS.min((n_items - shipped) as usize);
            let mut batch = staging;
            // Park for the first item, then drain the backlog
            // non-blockingly - all on the blocking pool.
            let res = tokio::task::spawn_blocking(move || {
                let outcome = (|| {
                    ring.recv_blocking(&mut batch[..SLOT], Some(TICK_TIMEOUT))?;
                    let mut filled = 1usize;
                    while filled < budget {
                        let dst = &mut batch[filled * SLOT..(filled + 1) * SLOT];
                        if ring.try_pop(dst).is_err() {
                            break;
                        }
                        filled += 1;
                    }
                    Ok::<usize, BlockingError>(filled)
                })();
                (batch, outcome)
            })
            .await
            .map_err(|join_err| {
                BlockingTcpBridgeError::Io(std::io::Error::other(
                    format!("recv blocking task join failed: {join_err}"),
                ))
            })?;
            let (returned, outcome) = res;
            staging = returned;
            match outcome {
                Ok(filled) => {
                    stream.write_all(&staging[..filled * SLOT]).await?;
                    shipped += filled as u64;
                }
                Err(BlockingError::Timeout) => continue,
                Err(e) => return Err(BlockingTcpBridgeError::Blocking(e)),
            }
        }
        stream.shutdown().await?;
        Ok(())
    }
}

/// Server half: accepts one connection + pushes received bytes into
/// the local consumer ring via `send_blocking` (also off the
/// runtime thread).
pub struct BlockingTcpBridgeServer {
    consumer_ring: Arc<BlockingSpscRing>,
    listener: TcpListener,
}

impl BlockingTcpBridgeServer {
    pub async fn bind(
        consumer_ring: Arc<BlockingSpscRing>,
        addr: SocketAddr,
    ) -> Result<Self, BlockingTcpBridgeError> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { consumer_ring, listener })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    /// Accept one connection + drain its framed payload stream into
    /// the consumer ring. Returns the count of items received.
    ///
    /// Chunked ingress: each socket `read` takes whatever bytes the
    /// wire has; complete slots push into the ring via the
    /// non-blocking `try_push` fast path, falling back to a parked
    /// `send_blocking` (off the runtime thread) only when the ring
    /// is full. A partial slot carries to the next read. A
    /// persistent full-ring timeout (one retry after the first
    /// `TICK_TIMEOUT`) surfaces as an error - the bridge's contract
    /// is "no items dropped", so an undrained consumer is a fault,
    /// not a discard.
    pub async fn accept_one(&self) -> Result<u64, BlockingTcpBridgeError> {
        let (stream, _) = self.listener.accept().await?;
        stream.set_nodelay(true)?;
        #[cfg(target_os = "linux")]
        crate::net_tune::tune_tcp_socket(std::os::fd::AsRawFd::as_raw_fd(&stream));
        let mut stream = stream;
        let mut header = [0u8; 8];
        stream.read_exact(&mut header).await?;
        let total: u64 = u64::from_be_bytes(header);

        let mut buf = vec![0u8; INGRESS_BUF_BYTES];
        let mut carry: Vec<u8> = Vec::with_capacity(SLOT);
        let mut received: u64 = 0;
        while received < total {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                return Err(BlockingTcpBridgeError::Closed);
            }
            let mut data: &[u8] = &buf[..n];
            if !carry.is_empty() {
                let need = SLOT - carry.len();
                let take = need.min(data.len());
                carry.extend_from_slice(&data[..take]);
                data = &data[take..];
                if carry.len() == SLOT {
                    let slot: [u8; SLOT] = carry[..SLOT].try_into().expect("slot-sized");
                    self.push_slot(slot).await?;
                    carry.clear();
                    received += 1;
                }
            }
            while data.len() >= SLOT && received < total {
                let slot: [u8; SLOT] = data[..SLOT].try_into().expect("slot-sized");
                self.push_slot(slot).await?;
                data = &data[SLOT..];
                received += 1;
            }
            if !data.is_empty() {
                carry.extend_from_slice(data);
            }
        }
        Ok(total)
    }

    /// Ring push with the blocking discipline: `try_push` fast
    /// path; on a full ring, park via `send_blocking` off the
    /// runtime thread, retrying once before surfacing the timeout.
    async fn push_slot(&self, slot: [u8; SLOT]) -> Result<(), BlockingTcpBridgeError> {
        if self.consumer_ring.try_push(&slot).is_ok() {
            return Ok(());
        }
        for attempt in 0..2 {
            let ring = Arc::clone(&self.consumer_ring);
            let res = tokio::task::spawn_blocking(move || {
                ring.send_blocking(&slot, Some(TICK_TIMEOUT))
            })
            .await
            .map_err(|join_err| {
                BlockingTcpBridgeError::Io(std::io::Error::other(
                    format!("send blocking task join failed: {join_err}"),
                ))
            })?;
            match res {
                Ok(()) => return Ok(()),
                Err(BlockingError::Timeout) if attempt == 0 => continue,
                Err(e) => return Err(BlockingTcpBridgeError::Blocking(e)),
            }
        }
        Err(BlockingTcpBridgeError::Blocking(BlockingError::Timeout))
    }
}
