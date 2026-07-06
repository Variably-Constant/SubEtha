//! `TcpTlsBridge`: the plain [`TcpBridge`](crate::tcp_bridge) wire
//! protocol carried inside a rustls 1.3 record layer.
//!
//! This is the encrypted TCP contender for the transport head-to-head.
//! It deliberately shares the burst-batched egress / chunked ingress
//! data path with the plain TCP bridge (`ship_stream` / `drain_stream`) - the only
//! difference on the wire is the AEAD record layer the bytes pass
//! through. That makes the TCP-vs-TCP+TLS column of the bench measure
//! exactly the TLS cost (handshake + per-record AEAD), nothing else.
//!
//! The rustls `ServerConfig` / `ClientConfig` are the same ones the
//! RLC `--tls` path builds in [`crate::rlc_crypto`], so every encrypted
//! contender in the bench shares one self-signed certificate.

#![cfg(feature = "tcp-tls-bridge")]

use std::net::SocketAddr;
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::adaptive_ring::AdaptiveRing;
use crate::tcp_bridge::{drain_stream, ship_stream, TcpBridgeError};

/// Client half: opens a TCP connection, completes a TLS 1.3 handshake,
/// then ships ring slots through the encrypted stream.
pub struct TcpTlsBridgeClient {
    producer_ring: Arc<AdaptiveRing>,
    server_addr: SocketAddr,
    config: Arc<ClientConfig>,
}

impl TcpTlsBridgeClient {
    pub fn new(
        producer_ring: Arc<AdaptiveRing>,
        server_addr: SocketAddr,
        config: Arc<ClientConfig>,
    ) -> Self {
        Self { producer_ring, server_addr, config }
    }

    /// Connect, handshake under `server_name` (the cert SNI), and ship
    /// `n_items` slots. The data path after the handshake is identical
    /// to the plain TCP bridge.
    pub async fn run(&self, n_items: u64, server_name: &str) -> Result<(), TcpBridgeError> {
        let tcp = TcpStream::connect(self.server_addr).await?;
        tcp.set_nodelay(true)?;
        #[cfg(target_os = "linux")]
        crate::net_tune::tune_tcp_socket(std::os::fd::AsRawFd::as_raw_fd(&tcp));
        let connector = TlsConnector::from(Arc::clone(&self.config));
        let domain = ServerName::try_from(server_name.to_owned())
            .map_err(|e| TcpBridgeError::Io(std::io::Error::other(e)))?;
        let mut tls = connector.connect(domain, tcp).await?;
        ship_stream(&mut tls, &self.producer_ring, n_items).await
    }
}

/// Server half: accepts one TCP connection, completes the TLS 1.3
/// handshake, then drains the encrypted payload stream into the
/// consumer ring.
pub struct TcpTlsBridgeServer {
    consumer_ring: Arc<AdaptiveRing>,
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

impl TcpTlsBridgeServer {
    pub async fn bind(
        consumer_ring: Arc<AdaptiveRing>,
        addr: SocketAddr,
        config: Arc<ServerConfig>,
    ) -> Result<Self, TcpBridgeError> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { consumer_ring, listener, acceptor: TlsAcceptor::from(config) })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    /// Accept one connection, handshake, and drain its framed payload.
    /// Returns the framed item count.
    pub async fn accept_one(&self) -> Result<u64, TcpBridgeError> {
        let (tcp, _) = self.listener.accept().await?;
        tcp.set_nodelay(true)?;
        #[cfg(target_os = "linux")]
        crate::net_tune::tune_tcp_socket(std::os::fd::AsRawFd::as_raw_fd(&tcp));
        let mut tls = self.acceptor.accept(tcp).await?;
        drain_stream(&mut tls, &self.consumer_ring).await
    }
}
