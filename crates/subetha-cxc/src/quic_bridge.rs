//! `QuicBridge`: cross-host substrate primitive that ferries bytes
//! between a producer `SpscRingCore` on one host and a consumer
//! `SpscRingCore` on another host via QUIC streams.
//!
//! The substrate's local data path is shared-memory rings; the
//! cross-host extension is QUIC. This primitive wraps that wire
//! protocol behind a typed pair (client + server) so callers do
//! not reimplement endpoint setup, certificate handling, frame
//! format, and connection lifecycle in each application.
//!
//! # Data path: burst-batched egress, chunked ingress
//!
//! A per-slot `write_all` await (one stream write per 64-byte item)
//! serializes the bridge on reactor latency - microseconds per item
//! regardless of wire speed - so the client BURST-DRAINS the ring:
//! every already-available slot (up to [`EGRESS_BATCH_SLOTS`]) is
//! copied into one contiguous buffer and handed to quinn in a
//! single write. The 64-byte memcpy per slot is noise next to the
//! TLS record processing the bytes pay anyway (quinn copies into
//! its send queue and encrypts in user space; there is no zero-copy
//! egress through an encrypting transport). A lone item still ships
//! immediately - batching never waits for items that have not
//! arrived.
//!
//! The server mirrors this with chunked stream reads: each `read`
//! takes whatever the stream has buffered, complete slots are
//! pushed into the consumer ring as they assemble, and a partial
//! slot carries to the next read.
//!
//! # Frame format
//!
//! Each connection carries one uni-directional stream. The stream
//! starts with an 8-byte big-endian item count `N`, followed by
//! `N * SPSC_PAYLOAD_BYTES` bytes back-to-back. Simplest framing
//! that lets the receiver know when to stop.
//!
//! # Optional dependency
//!
//! QUIC support is via quinn + rustls + rcgen. The module is gated
//! behind the `quic-bridge` Cargo feature; enabling the feature
//! pulls those crates in as regular dependencies. Callers that do
//! not enable the feature compile without the QUIC dep tree.

#![cfg(feature = "quic-bridge")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::adaptive_ring::{AdaptiveRing, ADAPTIVE_SPSC_PAYLOAD_BYTES};

/// Slots per batched egress write (16 KiB of payload per stream
/// write at the 64-byte slot size).
pub const EGRESS_BATCH_SLOTS: usize = 256;

/// Ingress stream-read buffer in bytes.
const INGRESS_BUF_BYTES: usize = 64 * 1024;

/// Errors the QUIC bridge halves can return.
#[derive(Debug)]
pub enum QuicBridgeError {
    /// rcgen / rustls TLS setup failed.
    Tls(String),
    /// QUIC endpoint or connection error.
    Quic(String),
    /// stdlib I/O error binding the endpoint.
    Io(std::io::Error),
}

impl std::fmt::Display for QuicBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tls(s) => write!(f, "tls: {s}"),
            Self::Quic(s) => write!(f, "quic: {s}"),
            Self::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl std::error::Error for QuicBridgeError {}

impl From<std::io::Error> for QuicBridgeError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}

/// Client half of the QUIC bridge. Pulls bytes from a local
/// producer ring and ships them across QUIC to a remote server.
pub struct QuicBridgeClient {
    producer_ring: Arc<AdaptiveRing>,
    server_addr: SocketAddr,
    client_config: ClientConfig,
    bind_addr: SocketAddr,
}

impl QuicBridgeClient {
    /// Construct a client that pulls from `producer_ring` (the
    /// substrate's default ring type, AdaptiveRing) and connects to
    /// `server_addr`. Bind the local UDP socket at `bind_addr`
    /// (typically `0.0.0.0:0` to let the OS pick a port). The
    /// producer_ring must have at least one registered producer +
    /// consumer; the bridge uses producer_id 0 / consumer_id 0.
    pub fn new(
        producer_ring: Arc<AdaptiveRing>,
        server_addr: SocketAddr,
        client_config: ClientConfig,
        bind_addr: SocketAddr,
    ) -> Self {
        Self { producer_ring, server_addr, client_config, bind_addr }
    }

    /// Connect to the server and ship `n_items` slots from the
    /// producer ring across one uni stream. Burst-batched egress:
    /// every already-available slot (up to [`EGRESS_BATCH_SLOTS`])
    /// goes out in one stream write; a lone item ships immediately.
    ///
    /// Returns when all `n_items` have been written + the stream is
    /// finished (the peer has acknowledged the FIN).
    pub async fn run(
        &self,
        n_items: u64,
        server_name: &str,
    ) -> Result<(), QuicBridgeError> {
        const SLOT: usize = ADAPTIVE_SPSC_PAYLOAD_BYTES;
        let mut endpoint = Endpoint::client(self.bind_addr)?;
        endpoint.set_default_client_config(self.client_config.clone());
        let conn = endpoint
            .connect(self.server_addr, server_name)
            .map_err(|e| QuicBridgeError::Quic(e.to_string()))?
            .await
            .map_err(|e| QuicBridgeError::Quic(e.to_string()))?;
        let mut send = conn
            .open_uni()
            .await
            .map_err(|e| QuicBridgeError::Quic(e.to_string()))?;

        // Frame header: 8-byte big-endian item count.
        send.write_all(&n_items.to_be_bytes())
            .await
            .map_err(|e| QuicBridgeError::Quic(e.to_string()))?;

        let mut batch = vec![0u8; EGRESS_BATCH_SLOTS * SLOT];
        let mut shipped: u64 = 0;
        while shipped < n_items {
            let budget = EGRESS_BATCH_SLOTS.min((n_items - shipped) as usize);
            let mut filled = 0usize;
            while filled < budget {
                let dst = &mut batch[filled * SLOT..(filled + 1) * SLOT];
                match self.producer_ring.try_recv(0, dst) {
                    Ok(_) => filled += 1,
                    Err(_) => break,
                }
            }
            if filled == 0 {
                tokio::task::yield_now().await;
                continue;
            }
            send.write_all(&batch[..filled * SLOT])
                .await
                .map_err(|e| QuicBridgeError::Quic(e.to_string()))?;
            shipped += filled as u64;
        }
        send.finish().map_err(|e| QuicBridgeError::Quic(e.to_string()))?;
        send.stopped().await.map_err(|e| QuicBridgeError::Quic(e.to_string()))?;
        // UDP segmentation-offload diagnostic: datagrams per send
        // io > 1 means the platform's GSO path is engaged (quinn
        // batches multiple datagrams into one sendmsg/WSASendMsg).
        let udp = conn.stats().udp_tx;
        eprintln!(
            "[quic] udp_tx datagrams={} ios={} (gso batching {:.1}x)",
            udp.datagrams,
            udp.ios,
            udp.datagrams as f64 / udp.ios.max(1) as f64,
        );
        Ok(())
    }
}

/// Server half of the QUIC bridge. Accepts one incoming connection,
/// reads bytes from the client's uni stream, and pushes them into
/// a local consumer ring.
pub struct QuicBridgeServer {
    consumer_ring: Arc<AdaptiveRing>,
    endpoint: Endpoint,
}

impl QuicBridgeServer {
    /// Bind a QUIC server endpoint at `addr` that pushes received
    /// bytes into `consumer_ring` (the substrate's default ring
    /// type, AdaptiveRing). consumer_ring must have at least one
    /// registered producer + consumer; the bridge uses
    /// producer_id 0 / consumer_id 0.
    pub fn bind(
        consumer_ring: Arc<AdaptiveRing>,
        addr: SocketAddr,
        server_config: ServerConfig,
    ) -> Result<Self, QuicBridgeError> {
        let endpoint = Endpoint::server(server_config, addr)?;
        Ok(Self { consumer_ring, endpoint })
    }

    /// Address the server endpoint is bound to (useful when the
    /// caller passed `0.0.0.0:0` and needs to learn the OS-assigned
    /// port).
    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }

    /// Accept one incoming connection, read its uni stream, and
    /// push each received slot into the consumer ring. Returns
    /// the number of items received when the client's stream has
    /// been fully drained.
    pub async fn accept_one(&self) -> Result<u64, QuicBridgeError> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| QuicBridgeError::Quic("endpoint closed".into()))?;
        let conn = incoming
            .await
            .map_err(|e| QuicBridgeError::Quic(e.to_string()))?;
        let mut recv = conn
            .accept_uni()
            .await
            .map_err(|e| QuicBridgeError::Quic(e.to_string()))?;

        let mut header = [0u8; 8];
        recv.read_exact(&mut header)
            .await
            .map_err(|e| QuicBridgeError::Quic(e.to_string()))?;
        let total: u64 = u64::from_be_bytes(header);

        // Chunked ingress: each read takes whatever the stream has
        // buffered (never waiting for a full batch), complete slots
        // push into the consumer ring as they assemble, and a
        // partial slot carries to the next read.
        const SLOT: usize = ADAPTIVE_SPSC_PAYLOAD_BYTES;
        let mut buf = vec![0u8; INGRESS_BUF_BYTES];
        let mut carry: Vec<u8> = Vec::with_capacity(SLOT);
        let mut received: u64 = 0;
        while received < total {
            let n = recv
                .read(&mut buf)
                .await
                .map_err(|e| QuicBridgeError::Quic(e.to_string()))?
                .ok_or_else(|| QuicBridgeError::Quic(
                    "stream finished early".into(),
                ))?;
            let mut data: &[u8] = &buf[..n];
            if !carry.is_empty() {
                let need = SLOT - carry.len();
                let take = need.min(data.len());
                carry.extend_from_slice(&data[..take]);
                data = &data[take..];
                if carry.len() == SLOT {
                    while self.consumer_ring.try_send(0, &carry).is_err() {
                        tokio::task::yield_now().await;
                    }
                    carry.clear();
                    received += 1;
                }
            }
            while data.len() >= SLOT && received < total {
                while self.consumer_ring.try_send(0, &data[..SLOT]).is_err() {
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
}

/// Generate a self-signed certificate for `sni_name`, returned as
/// raw DER bytes: `(cert_der, pkcs8_key_der)`. The cross-host
/// building block: generate ONCE, ship both files to the host(s)
/// that run servers and the cert alone to the host(s) that run
/// clients, then rebuild the configs from bytes with
/// [`make_server_config_from_der`] / [`make_client_config_from_der`].
/// The SNI string is what clients pass to `connect` (it names the
/// cert, not the wire address, so any LAN IP works).
pub fn generate_self_signed_cert(
    sni_name: &str,
) -> Result<(Vec<u8>, Vec<u8>), QuicBridgeError> {
    let cert = rcgen::generate_simple_self_signed(vec![sni_name.to_string()])
        .map_err(|e| QuicBridgeError::Tls(e.to_string()))?;
    Ok((
        cert.cert.der().to_vec(),
        cert.key_pair.serialize_der(),
    ))
}

fn default_transport_config() -> Result<TransportConfig, QuicBridgeError> {
    let mut transport = TransportConfig::default();
    transport.max_idle_timeout(Some(
        Duration::from_secs(30)
            .try_into()
            .map_err(|e: quinn::VarIntBoundsExceeded| QuicBridgeError::Quic(e.to_string()))?,
    ));
    // Size the flow-control windows for a high-BDP WAN path. quinn's default
    // per-stream receive window leaves a single-stream transfer window-limited
    // (throughput = stream_receive_window / RTT), so a long-RTT link runs far
    // below its capacity - on a 50 Mbit/s, 21 ms path the default pins a single
    // stream near ~15 Mbit/s. 16 MB per stream + a 32 MB send window cover a
    // gigabit path past 100 ms RTT, so the congestion controller - not flow
    // control - sets the rate (what a production QUIC config does).
    let win = |bytes: u64| -> Result<quinn::VarInt, QuicBridgeError> {
        quinn::VarInt::from_u64(bytes).map_err(|e| QuicBridgeError::Quic(e.to_string()))
    };
    transport.stream_receive_window(win(16 * 1024 * 1024)?);
    transport.receive_window(win(64 * 1024 * 1024)?);
    transport.send_window(32 * 1024 * 1024);
    // BBR congestion control. quinn's default is loss-based CUBIC, which on a
    // jittery WAN UDP path treats sporadic reordering / loss as congestion and
    // collapses the rate; a rate-based controller (what production QUIC, e.g.
    // Google's, runs) holds the link. This matches the controller class the
    // RLC transport uses, so the comparison is congestion-controller-fair.
    transport.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
    Ok(transport)
}

/// Build a server config from raw DER bytes produced by
/// [`generate_self_signed_cert`] (possibly on another host).
pub fn make_server_config_from_der(
    cert_der: &[u8],
    key_der: &[u8],
) -> Result<ServerConfig, QuicBridgeError> {
    let key = PrivateKeyDer::Pkcs8(key_der.to_vec().into());
    let cert = CertificateDer::from(cert_der.to_vec());
    let mut server_config = ServerConfig::with_single_cert(vec![cert], key)
        .map_err(|e| QuicBridgeError::Tls(e.to_string()))?;
    server_config.transport_config(Arc::new(default_transport_config()?));
    Ok(server_config)
}

/// Build a client config that trusts exactly the given DER cert.
pub fn make_client_config_from_der(
    cert_der: &[u8],
) -> Result<ClientConfig, QuicBridgeError> {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(cert_der.to_vec()))
        .map_err(|e| QuicBridgeError::Tls(e.to_string()))?;
    let crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| QuicBridgeError::Tls(e.to_string()))?,
    ));
    client_config.transport_config(Arc::new(default_transport_config()?));
    Ok(client_config)
}

/// Helper that builds a self-signed TLS server config + matching
/// client config trusting that cert, both in one process. Single-
/// host / demo use; cross-host callers split the steps via
/// [`generate_self_signed_cert`] + the `from_der` constructors so
/// the cert can travel between hosts as bytes.
pub fn make_self_signed_pair(
    sni_name: &str,
) -> Result<(ServerConfig, ClientConfig), QuicBridgeError> {
    let (cert_der, key_der) = generate_self_signed_cert(sni_name)?;
    Ok((
        make_server_config_from_der(&cert_der, &key_der)?,
        make_client_config_from_der(&cert_der)?,
    ))
}

/// One-shot rustls crypto provider install. Safe to call multiple
/// times; only the first call has effect. The substrate does not
/// install a provider by default because the choice of crypto
/// backend belongs to the caller; this helper sets a sensible
/// default (ring - measured equal to aws-lc-rs on hosts without
/// VAES, with a lighter build chain) so example binaries work out
/// of the box.
pub fn install_default_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}
