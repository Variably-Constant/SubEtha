//! `TcpBridge`: cross-host substrate primitive over plain TCP.
//!
//! Where `QuicBridge` provides encrypted, multiplexed, congestion-
//! controlled transport, `TcpBridge` is the trusted-network primitive
//! for callers that want maximum throughput without QUIC's
//! per-packet TLS overhead. Cross-platform via
//! `tokio::net::TcpStream`.
//!
//! # Data path: burst-batched egress, chunked ingress
//!
//! A per-slot socket write (one `write_all` await per 64-byte item)
//! serializes the whole bridge on syscall + reactor latency -
//! microseconds per item against a wire that moves the same bytes
//! in nanoseconds. The client therefore BURST-DRAINS the ring:
//! every already-available slot (up to [`EGRESS_BATCH_SLOTS`]) is
//! copied into one contiguous buffer and shipped with a single
//! write. A lone item still ships immediately - batching never
//! waits for items that have not arrived - so latency-sensitive
//! request/response traffic is not penalized. `TCP_NODELAY` is set
//! on both ends for the same reason; under a saturating stream the
//! kernel still fills segments to MSS from the batched writes.
//!
//! The server mirrors this with chunked reads: `read()` returns
//! whatever bytes the wire has (never blocking for a full batch),
//! complete 64-byte slots are pushed into the consumer ring, and a
//! partial slot carries over to the next read.

#![cfg(feature = "tcp-bridge")]

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::adaptive_ring::{AdaptiveRing, ADAPTIVE_SPSC_PAYLOAD_BYTES};

/// Slots per batched egress write (16 KiB of payload per syscall at
/// the 64-byte slot size).
pub const EGRESS_BATCH_SLOTS: usize = 256;

/// Ingress socket-read buffer in bytes.
const INGRESS_BUF_BYTES: usize = 64 * 1024;

// ===================================================================
// Stream-generic data path: the burst-batched egress + chunked
// ingress loops touch only the AsyncRead/AsyncWrite trait surface, so
// they run identically over a plain `TcpStream` and over a
// `tokio_rustls::TlsStream<TcpStream>`. The TCP and TCP+TLS bridges
// therefore SHARE one wire protocol - the only difference between
// them is the record layer the bytes pass through, which is the
// fairest possible TCP-vs-TLS comparison.
// ===================================================================

/// Ship `n_items` ring slots over any async stream, framed by an
/// 8-byte big-endian item count. Burst-batches every already-available
/// slot (up to [`EGRESS_BATCH_SLOTS`]) into one `write_all`; a lone
/// item ships immediately.
pub(crate) async fn ship_stream<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    producer_ring: &AdaptiveRing,
    n_items: u64,
) -> Result<(), TcpBridgeError> {
    const SLOT: usize = ADAPTIVE_SPSC_PAYLOAD_BYTES;
    stream.write_all(&n_items.to_be_bytes()).await?;

    let mut batch = vec![0u8; EGRESS_BATCH_SLOTS * SLOT];
    let mut shipped: u64 = 0;
    while shipped < n_items {
        let budget = EGRESS_BATCH_SLOTS.min((n_items - shipped) as usize);
        let mut filled = 0usize;
        while filled < budget {
            let dst = &mut batch[filled * SLOT..(filled + 1) * SLOT];
            match producer_ring.try_recv(0, dst) {
                Ok(_) => filled += 1,
                Err(_) => break,
            }
        }
        if filled == 0 {
            tokio::task::yield_now().await;
            continue;
        }
        stream.write_all(&batch[..filled * SLOT]).await?;
        shipped += filled as u64;
    }
    // Flush before the half-close so any plaintext still buffered in the
    // record layer is encrypted and written to the socket (a no-op for a
    // plain TcpStream; forces the final TLS record out).
    stream.flush().await?;
    stream.shutdown().await?;
    // Barrier: wait for the receiver to finish before returning. The
    // receiver closes its side once it has drained all `total` bytes,
    // which surfaces here as EOF (or a reset, equally final). Without
    // this, the client process can exit and RST the socket while the
    // receiver - slower for TLS, which must decrypt - is still draining
    // the last records, surfacing as a spurious connection-reset error.
    let mut sink = [0u8; 64];
    loop {
        match stream.read(&mut sink).await {
            Ok(0) | Err(_) => break,
            Ok(_) => continue,
        }
    }
    Ok(())
}

/// Drain a framed payload stream off any async-readable stream into
/// the consumer ring. Reads the 8-byte item-count header, then chunks
/// reads into complete slots (a partial slot carries to the next
/// read). Returns the framed item count.
pub(crate) async fn drain_stream<R: AsyncRead + Unpin>(
    stream: &mut R,
    consumer_ring: &AdaptiveRing,
) -> Result<u64, TcpBridgeError> {
    const SLOT: usize = ADAPTIVE_SPSC_PAYLOAD_BYTES;
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let total: u64 = u64::from_be_bytes(header);

    let mut buf = vec![0u8; INGRESS_BUF_BYTES];
    let mut carry: Vec<u8> = Vec::with_capacity(SLOT);
    let mut received: u64 = 0;
    while received < total {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Err(TcpBridgeError::Closed);
        }
        let mut data: &[u8] = &buf[..n];
        if !carry.is_empty() {
            let need = SLOT - carry.len();
            let take = need.min(data.len());
            carry.extend_from_slice(&data[..take]);
            data = &data[take..];
            if carry.len() == SLOT {
                while consumer_ring.try_send(0, &carry).is_err() {
                    tokio::task::yield_now().await;
                }
                carry.clear();
                received += 1;
            }
        }
        while data.len() >= SLOT && received < total {
            while consumer_ring.try_send(0, &data[..SLOT]).is_err() {
                tokio::task::yield_now().await;
            }
            data = &data[SLOT..];
            received += 1;
        }
        if !data.is_empty() {
            carry.extend_from_slice(data);
        }
    }
    Ok(total)
}

/// Errors the TCP bridge halves can return.
#[derive(Debug)]
pub enum TcpBridgeError {
    Io(std::io::Error),
    Closed,
}

impl std::fmt::Display for TcpBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Closed => write!(f, "connection closed"),
        }
    }
}

impl std::error::Error for TcpBridgeError {}

impl From<std::io::Error> for TcpBridgeError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}

/// Client half: pulls bytes from a local producer ring and ships
/// them across a TCP connection.
pub struct TcpBridgeClient {
    producer_ring: Arc<AdaptiveRing>,
    server_addr: SocketAddr,
}

impl TcpBridgeClient {
    /// Takes the substrate's default ring type (AdaptiveRing); the
    /// caller registers producer/consumer ids on it first. The
    /// bridge uses producer_id 0.
    pub fn new(producer_ring: Arc<AdaptiveRing>, server_addr: SocketAddr) -> Self {
        Self { producer_ring, server_addr }
    }

    /// Connect + ship `n_items` slots across the TCP connection.
    /// Burst-batched egress: every already-available slot (up to
    /// [`EGRESS_BATCH_SLOTS`]) goes out in one socket write; a lone
    /// item ships immediately.
    pub async fn run(&self, n_items: u64) -> Result<(), TcpBridgeError> {
        let stream = TcpStream::connect(self.server_addr).await?;
        // Nagle off: the bridge ferries latency-sensitive slots;
        // batched writes keep segments MSS-filled under load.
        stream.set_nodelay(true)?;
        #[cfg(target_os = "linux")]
        crate::net_tune::tune_tcp_socket(std::os::fd::AsRawFd::as_raw_fd(&stream));
        let mut stream = stream;
        ship_stream(&mut stream, &self.producer_ring, n_items).await
    }
}

/// Server half: accepts one incoming connection + pushes received
/// bytes into a local consumer ring.
pub struct TcpBridgeServer {
    consumer_ring: Arc<AdaptiveRing>,
    listener: TcpListener,
}

impl TcpBridgeServer {
    pub async fn bind(
        consumer_ring: Arc<AdaptiveRing>,
        addr: SocketAddr,
    ) -> Result<Self, TcpBridgeError> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { consumer_ring, listener })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    /// Accept one connection, read its framed payload stream, push
    /// into the consumer ring. Returns the count of items received.
    ///
    /// Chunked ingress: each socket `read` takes whatever bytes the
    /// wire has - it never waits for a full batch, so a lone item
    /// flows through with no added latency - and complete slots are
    /// pushed as they assemble; a partial slot carries to the next
    /// read.
    pub async fn accept_one(&self) -> Result<u64, TcpBridgeError> {
        let (stream, _) = self.listener.accept().await?;
        stream.set_nodelay(true)?;
        #[cfg(target_os = "linux")]
        crate::net_tune::tune_tcp_socket(std::os::fd::AsRawFd::as_raw_fd(&stream));
        let mut stream = stream;
        drain_stream(&mut stream, &self.consumer_ring).await
    }
}
