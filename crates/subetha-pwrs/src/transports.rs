//! The bridges that carry a ring's items to another host over TCP or
//! QUIC, and the list of transports this module carries.

use std::net::SocketAddr;
use std::sync::Arc;

use pwrs::prelude::*;

use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::quic_bridge::{self, QuicBridgeClient as SubethaQuicClient, QuicBridgeServer as SubethaQuicServer};
use subetha_cxc::tcp_bridge::{TcpBridgeClient as SubethaTcpClient, TcpBridgeServer as SubethaTcpServer};

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, open_err, size};
use crate::sensing::Endpoint;

assert_send!(TcpBridgeClient, TcpBridgeServer, QuicBridgeClient, QuicBridgeServer);

/// One runtime for every bridge in the process, started the first time
/// a bridge needs it.
///
/// The bridges are written against an async runtime and the calls here
/// are ordinary blocking ones, so each waits on this. One runtime
/// rather than one per bridge, because a runtime owns threads and a
/// process that makes several bridges should not pay for several sets
/// of them.
static BRIDGE_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn bridge_runtime() -> PsResult<&'static tokio::runtime::Runtime> {
    if let Some(running) = BRIDGE_RUNTIME.get() {
        return Ok(running);
    }
    let built = tokio::runtime::Runtime::new().map_err(|e| op_err("starting the bridge runtime", e))?;
    // A set that fails hands back the runtime it could not store,
    // because another thread published one first. Both are equally
    // good, so the one that lost the race is shut down here and every
    // caller goes on to use the winner's.
    if let Err(surplus) = BRIDGE_RUNTIME.set(built) {
        surplus.shutdown_background();
    }
    BRIDGE_RUNTIME.get().ok_or_else(|| op_err("starting the bridge runtime", "the runtime could not be started"))
}

/// One address from a host and a port.
fn socket_addr_of(host: &str, port: u16) -> PsResult<SocketAddr> {
    use std::net::ToSocketAddrs;
    (host, port)
        .to_socket_addrs()
        .map_err(|e| arg_err(format!("{host}:{port} is not an address: {e}")))?
        .next()
        .ok_or_else(|| arg_err(format!("{host}:{port} named no address")))
}

/// The ring a bridge carries, opened by the bridge itself so the ring
/// object a script holds and the bridge are two handles on one file.
fn open_ring(ps: &Pipeline<'_>, path: &str, capacity: u64, producers: Option<u64>, consumers: Option<u64>) -> PsResult<(String, Arc<AdaptiveRing>)> {
    let path = full_path(ps, path)?;
    let p = size(producers.unwrap_or(1), "the producer count")?;
    let c = size(consumers.unwrap_or(1), "the consumer count")?;
    if p < 1 || c < 1 {
        return Err(arg_err("a ring needs at least one producer and one consumer"));
    }
    let ring = AdaptiveRing::open(&path, p, c, size(capacity, "the capacity")?).map_err(|e| open_err("the ring", &path, e))?;
    Ok((path, Arc::new(ring)))
}

/// The sending end of a bridge carrying a ring's items over a TCP
/// connection to another host.
///
/// The ring is one this process already writes to. The bridge takes
/// what is in it and ships it, so a process that fills a ring locally
/// reaches a reader on another machine without changing how it writes.
#[psclass(name = "SubEtha.TcpBridgeClient", mode = proxy)]
pub struct TcpBridgeClient {
    /// The file of the ring the bridge takes items from.
    pub ring_path: String,
    /// The address of the reading end.
    pub server: Endpoint,
    #[psfield(skip)]
    ring: Arc<AdaptiveRing>,
    #[psfield(skip)]
    addr: SocketAddr,
}

/// The operations of a `SubEtha.TcpBridgeClient`.
#[psmethods]
impl TcpBridgeClient {
    /// Connects and ships `items` of them, waiting until all have gone.
    pub fn run(&self, items: u64) -> PsResult<()> {
        let bridge = SubethaTcpClient::new(Arc::clone(&self.ring), self.addr);
        let runtime = bridge_runtime()?;
        runtime.block_on(bridge.run(items)).map_err(|e| op_err("shipping", e))
    }
}

/// Makes the sending end of a TCP bridge from the ring at RingPath to
/// the reading end at ServerHost and ServerPort.
///
/// # Examples
///
/// `$client = New-SubEthaTcpBridgeClient -RingPath C:\ipc\ring -Capacity 64 -ServerHost 10.0.0.5 -ServerPort 9000`
#[cmdlet(verb = "New", noun = "SubEthaTcpBridgeClient", alias = "New-SETcpBridgeClient", output = ["SubEtha.TcpBridgeClient"])]
#[derive(Default)]
pub struct NewSubEthaTcpBridgeClient {
    /// The file of the ring to take items from.
    #[param(mandatory, position = 0)]
    pub ring_path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The host of the reading end.
    #[param(mandatory, position = 2)]
    pub server_host: String,
    /// The port of the reading end.
    #[param(mandatory, position = 3)]
    pub server_port: u16,
    /// How many producers the ring was made for; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers the ring was made for; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
}

impl Cmdlet for NewSubEthaTcpBridgeClient {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let (ring_path, ring) = open_ring(ps, &self.ring_path, self.capacity, self.max_producers, self.max_consumers)?;
        let addr = socket_addr_of(&self.server_host, self.server_port)?;
        ps.write(TcpBridgeClient { ring_path, server: Endpoint { host: addr.ip().to_string(), port: addr.port() }, ring, addr })
    }
}

/// The reading end of a TCP bridge, putting what arrives into a ring
/// this process reads from.
#[psclass(name = "SubEtha.TcpBridgeServer", mode = proxy)]
pub struct TcpBridgeServer {
    /// The file of the ring arriving items go into.
    pub ring_path: String,
    #[psfield(skip)]
    inner: Arc<SubethaTcpServer>,
}

/// The operations of a `SubEtha.TcpBridgeServer`.
#[psmethods]
impl TcpBridgeServer {
    /// The address this listens on, worth reading when the port was
    /// left to the system to pick.
    pub fn local_addr(&self) -> PsResult<Endpoint> {
        let addr = self.inner.local_addr().map_err(|e| op_err("reading the address", e))?;
        Ok(Endpoint { host: addr.ip().to_string(), port: addr.port() })
    }

    /// Takes one connection, reads it to its end, and returns how many
    /// items arrived. Waits until the sending end has finished.
    pub fn accept_one(&self) -> PsResult<u64> {
        let listening = Arc::clone(&self.inner);
        let runtime = bridge_runtime()?;
        runtime.block_on(listening.accept_one()).map_err(|e| op_err("reading", e))
    }
}

/// Makes the reading end of a TCP bridge on LocalHost and LocalPort,
/// putting arriving items into the ring at RingPath. A port of zero
/// lets the system pick one, which LocalAddr then reports.
///
/// # Examples
///
/// `$server = New-SubEthaTcpBridgeServer -RingPath C:\ipc\ring -Capacity 64 -LocalPort 9000`
#[cmdlet(verb = "New", noun = "SubEthaTcpBridgeServer", alias = "New-SETcpBridgeServer", output = ["SubEtha.TcpBridgeServer"])]
#[derive(Default)]
pub struct NewSubEthaTcpBridgeServer {
    /// The file of the ring to put arriving items into.
    #[param(mandatory, position = 0)]
    pub ring_path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The port to listen on; zero lets the system pick.
    #[param(mandatory, position = 2)]
    pub local_port: u16,
    /// The host to listen on; every address when absent.
    #[param]
    pub local_host: Option<String>,
    /// How many producers the ring was made for; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers the ring was made for; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
}

impl Cmdlet for NewSubEthaTcpBridgeServer {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let (ring_path, ring) = open_ring(ps, &self.ring_path, self.capacity, self.max_producers, self.max_consumers)?;
        let host = self.local_host.clone().unwrap_or_else(|| "0.0.0.0".to_string());
        let addr = socket_addr_of(&host, self.local_port)?;
        let runtime = bridge_runtime()?;
        let inner = runtime.block_on(SubethaTcpServer::bind(ring, addr)).map_err(|e| op_err("listening", e))?;
        ps.write(TcpBridgeServer { ring_path, inner: Arc::new(inner) })
    }
}

/// A certificate and its key, both as `byte[]`.
#[psclass(name = "SubEtha.Certificate")]
#[derive(Clone, Default)]
pub struct Certificate {
    /// What the certificate is issued for, and what a client passes as
    /// the server name.
    pub name: String,
    /// The certificate, a `byte[]`.
    pub cert: PsObject,
    /// Its key, a `byte[]`.
    pub key: PsObject,
}

/// Makes a certificate and its key for a QUIC bridge. Name is what the
/// certificate is issued for and what a client passes as ServerName; it
/// names the certificate rather than the address, so any address the
/// reading end is reachable at works. The certificate is signed by
/// nobody, so the reading end holds both and the sending end holds the
/// certificate alone, which is what it checks the reading end against.
///
/// # Examples
///
/// `$cert = New-SubEthaSelfSignedCert -Name 'localhost'`
#[cmdlet(verb = "New", noun = "SubEthaSelfSignedCert", alias = "New-SESelfSignedCert", output = ["SubEtha.Certificate"])]
#[derive(Default)]
pub struct NewSubEthaSelfSignedCert {
    /// What the certificate is issued for.
    #[param(mandatory, position = 0)]
    pub name: String,
}

impl Cmdlet for NewSubEthaSelfSignedCert {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let (cert, key) = quic_bridge::generate_self_signed_cert(&self.name).map_err(|e| op_err("making a certificate", e))?;
        ps.write(Certificate { name: self.name.clone(), cert: PsObject::from_slice(&cert)?, key: PsObject::from_slice(&key)? })
    }
}

/// The sending end of a bridge carrying a ring's items over QUIC.
#[psclass(name = "SubEtha.QuicBridgeClient", mode = proxy)]
pub struct QuicBridgeClient {
    /// The file of the ring the bridge takes items from.
    pub ring_path: String,
    /// The address of the reading end.
    pub server: Endpoint,
    /// The name the reading end's certificate was issued for.
    pub server_name: String,
    #[psfield(skip)]
    ring: Arc<AdaptiveRing>,
    #[psfield(skip)]
    addr: SocketAddr,
    /// The certificate bytes rather than the config built from them, so
    /// the QUIC library's types stay inside the crate that owns them.
    /// Building the config again per run costs nothing beside opening a
    /// connection, which is what a run does.
    #[psfield(skip)]
    cert: Vec<u8>,
    #[psfield(skip)]
    bind: SocketAddr,
}

/// The operations of a `SubEtha.QuicBridgeClient`.
#[psmethods]
impl QuicBridgeClient {
    /// Connects and ships `items` of them, waiting until the reading
    /// end has acknowledged the last of them.
    pub fn run(&self, items: u64) -> PsResult<()> {
        let config = quic_bridge::make_client_config_from_der(&self.cert).map_err(|e| op_err("trusting the certificate", e))?;
        let bridge = SubethaQuicClient::new(Arc::clone(&self.ring), self.addr, config, self.bind);
        let runtime = bridge_runtime()?;
        runtime.block_on(bridge.run(items, &self.server_name)).map_err(|e| op_err("shipping", e))
    }
}

/// Makes the sending end of a QUIC bridge from the ring at RingPath to
/// the reading end at ServerHost and ServerPort, trusting Cert, the
/// certificate that end was made with, issued for ServerName.
///
/// # Examples
///
/// `$client = New-SubEthaQuicBridgeClient -RingPath C:\ipc\ring -Capacity 64 -ServerHost 10.0.0.5 -ServerPort 9000 -Cert $cert.Cert -ServerName 'host'`
#[cmdlet(verb = "New", noun = "SubEthaQuicBridgeClient", alias = "New-SEQuicBridgeClient", output = ["SubEtha.QuicBridgeClient"])]
#[derive(Default)]
pub struct NewSubEthaQuicBridgeClient {
    /// The file of the ring to take items from.
    #[param(mandatory, position = 0)]
    pub ring_path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The host of the reading end.
    #[param(mandatory, position = 2)]
    pub server_host: String,
    /// The port of the reading end.
    #[param(mandatory, position = 3)]
    pub server_port: u16,
    /// The certificate the reading end was made with, a `byte[]`.
    #[param(mandatory)]
    pub cert: PsObject,
    /// The name the certificate was issued for.
    #[param(mandatory)]
    pub server_name: String,
    /// The host to send from; every address when absent.
    #[param]
    pub local_host: Option<String>,
    /// The port to send from; one the system picks when absent.
    #[param]
    pub local_port: Option<u16>,
    /// How many producers the ring was made for; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers the ring was made for; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
}

impl Cmdlet for NewSubEthaQuicBridgeClient {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        quic_bridge::install_default_crypto_provider();
        let cert = bytes(&self.cert)?.to_vec();
        // Built and thrown away here so a certificate that cannot be
        // read is refused when the client is made, rather than on the
        // first attempt to send.
        quic_bridge::make_client_config_from_der(&cert).map_err(|e| op_err("trusting the certificate", e))?;
        let (ring_path, ring) = open_ring(ps, &self.ring_path, self.capacity, self.max_producers, self.max_consumers)?;
        let addr = socket_addr_of(&self.server_host, self.server_port)?;
        let bind = match &self.local_host {
            Some(host) => socket_addr_of(host, self.local_port.unwrap_or(0))?,
            None => SocketAddr::from(([0, 0, 0, 0], self.local_port.unwrap_or(0))),
        };
        ps.write(QuicBridgeClient {
            ring_path,
            server: Endpoint { host: addr.ip().to_string(), port: addr.port() },
            server_name: self.server_name.clone(),
            ring,
            addr,
            cert,
            bind,
        })
    }
}

/// The reading end of a QUIC bridge, putting what arrives into a ring
/// this process reads from.
#[psclass(name = "SubEtha.QuicBridgeServer", mode = proxy)]
pub struct QuicBridgeServer {
    /// The file of the ring arriving items go into.
    pub ring_path: String,
    #[psfield(skip)]
    inner: Arc<SubethaQuicServer>,
}

/// The operations of a `SubEtha.QuicBridgeServer`.
#[psmethods]
impl QuicBridgeServer {
    /// The address this listens on.
    pub fn local_addr(&self) -> PsResult<Endpoint> {
        let addr = self.inner.local_addr().map_err(|e| op_err("reading the address", e))?;
        Ok(Endpoint { host: addr.ip().to_string(), port: addr.port() })
    }

    /// Takes one connection, reads it to its end, and returns how many
    /// items arrived.
    pub fn accept_one(&self) -> PsResult<u64> {
        let listening = Arc::clone(&self.inner);
        let runtime = bridge_runtime()?;
        runtime.block_on(listening.accept_one()).map_err(|e| op_err("reading", e))
    }
}

/// Makes the reading end of a QUIC bridge on LocalHost and LocalPort,
/// proving itself with Cert and Key, putting arriving items into the
/// ring at RingPath. A port of zero lets the system pick one, which
/// LocalAddr then reports.
///
/// # Examples
///
/// `$server = New-SubEthaQuicBridgeServer -RingPath C:\ipc\ring -Capacity 64 -LocalPort 9000 -Cert $cert.Cert -Key $cert.Key`
#[cmdlet(verb = "New", noun = "SubEthaQuicBridgeServer", alias = "New-SEQuicBridgeServer", output = ["SubEtha.QuicBridgeServer"])]
#[derive(Default)]
pub struct NewSubEthaQuicBridgeServer {
    /// The file of the ring to put arriving items into.
    #[param(mandatory, position = 0)]
    pub ring_path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The port to listen on; zero lets the system pick.
    #[param(mandatory, position = 2)]
    pub local_port: u16,
    /// The certificate this end proves itself with, a `byte[]`.
    #[param(mandatory)]
    pub cert: PsObject,
    /// The certificate's key, a `byte[]`.
    #[param(mandatory)]
    pub key: PsObject,
    /// The host to listen on; every address when absent.
    #[param]
    pub local_host: Option<String>,
    /// How many producers the ring was made for; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers the ring was made for; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
}

impl Cmdlet for NewSubEthaQuicBridgeServer {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        quic_bridge::install_default_crypto_provider();
        let cert = bytes(&self.cert)?;
        let key = bytes(&self.key)?;
        let config = quic_bridge::make_server_config_from_der(&cert, &key).map_err(|e| op_err("reading the certificate", e))?;
        let (ring_path, ring) = open_ring(ps, &self.ring_path, self.capacity, self.max_producers, self.max_consumers)?;
        let host = self.local_host.clone().unwrap_or_else(|| "0.0.0.0".to_string());
        let addr = socket_addr_of(&host, self.local_port)?;
        // Binding is not an async call but it builds an endpoint that
        // registers with the runtime, so it has to happen inside one.
        let runtime = bridge_runtime()?;
        let entered = runtime.enter();
        let bound = SubethaQuicServer::bind(ring, addr, config).map_err(|e| op_err("listening", e));
        drop(entered);
        ps.write(QuicBridgeServer { ring_path, inner: Arc::new(bound?) })
    }
}

/// Writes the names of the transports this module carries: sens, the
/// link across a lossy network, and the tcp and quic bridges.
///
/// # Examples
///
/// `Get-SubEthaTransport`
#[cmdlet(verb = "Get", noun = "SubEthaTransport", alias = "Get-SETransport", output = ["System.String"])]
#[derive(Default)]
pub struct GetSubEthaTransport {}

impl Cmdlet for GetSubEthaTransport {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        for name in ["sens", "tcp", "quic"] {
            ps.write(name)?;
        }
        Ok(())
    }
}
