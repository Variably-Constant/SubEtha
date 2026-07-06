//! Cross-HOST bridge harness: one role per machine, real LAN hop.
//!
//! Exercises the three bridge stacks between two physical hosts:
//!
//! - `quic`: `QuicBridgeClient` / `QuicBridgeServer` (quinn + rustls,
//!   self-signed cert shipped between hosts as DER files)
//! - `tcp`:  `TcpBridgeClient` / `TcpBridgeServer`
//! - `btcp`: `BlockingTcpBridgeClient` / `BlockingTcpBridgeServer`
//!   (futex-parked `BlockingSpscRing` endpoints - zero CPU at idle)
//!
//! Two modes:
//!
//! - **oneway**: the client host's app pushes `--items` sequenced
//!   slots into its local ring; the bridge ships them across the
//!   wire; the server host's app drains its local ring and ASSERTS
//!   strict sequence order + count + sum. Both sides print
//!   machine-parsable `RESULT` lines (client ship rate, server
//!   first-to-last drain rate).
//! - **rtt**: both hosts run a server AND a client (two rings
//!   each); the `ping` role round-trips `--rounds` items through
//!   ring -> wire -> remote ring -> remote app echo -> wire -> ring
//!   and reports min/avg/p50/p99/max round-trip latency. The `pong`
//!   role echoes. Both roles print `BOUND` after their server binds
//!   and then WAIT FOR A LINE ON STDIN before connecting their
//!   client - the orchestrator releases both once both are bound,
//!   so neither side races the other's listener.
//!
//! Certificates (QUIC only): generate ONE self-signed pair anywhere
//! with `--gen-cert <cert.der> <key.der>`, ship both files to every
//! host that runs a QUIC server and the cert file to every host
//! that runs a QUIC client. The SNI is the fixed string
//! `subetha-lan` (it names the cert, not the wire address).
//!
//! Examples:
//!     bridge_lan --gen-cert /tmp/c.der /tmp/k.der
//!     bridge_lan --transport tcp --role server --bind 0.0.0.0:7401 --items 200000
//!     bridge_lan --transport tcp --role client --connect 192.168.1.210:7401 --items 200000
//!     bridge_lan --transport quic --role pong --bind 0.0.0.0:7402 \
//!         --connect 192.168.1.210:7401 --rounds 2000 --cert /tmp/c.der --key /tmp/k.der
//!
//! Build: cargo build --release --example bridge_lan \
//!     --features quic-bridge,tcp-bridge

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomOrd};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::blocking_spsc_ring::{BlockingError, BlockingSpscRing};
use subetha_cxc::blocking_tcp_bridge::{
    BlockingTcpBridgeClient, BlockingTcpBridgeServer,
};
use subetha_cxc::quic_bridge::{
    generate_self_signed_cert, install_default_crypto_provider,
    make_client_config_from_der, make_server_config_from_der,
    QuicBridgeClient, QuicBridgeServer,
};
use subetha_cxc::tcp_bridge::{TcpBridgeClient, TcpBridgeServer};
#[cfg(feature = "tcp-tls-bridge")]
use subetha_cxc::tcp_tls_bridge::{TcpTlsBridgeClient, TcpTlsBridgeServer};
use subetha_cxc::sharded_udp::{ShardedReceiver, ShardedSender};
use subetha_cxc::udp_bridge::{ReliableUdpReceiver, ReliableUdpSender};
use subetha_cxc::AdaptiveRing;

const SNI: &str = "subetha-lan";
const RING_CAPACITY: usize = 8192;
const SLOT: usize = ADAPTIVE_SPSC_PAYLOAD_BYTES;
const BLOCKING_TICK: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Quic,
    Tcp,
    /// TCP carried inside a rustls 1.3 record layer: the encrypted-TCP
    /// contender. Identical framing/batching to `Tcp`; the only wire
    /// delta is the AEAD record layer (needs the `tcp-tls-bridge`
    /// feature + a shared `--cert` / `--key`).
    Tcptls,
    Btcp,
    /// Sens-O-Matic: reliable-UDP FEC transport. Unlike the three
    /// stream bridges (which ferry batched 64-byte ring slots), it
    /// ships MTU-sized items as forward-error-corrected datagrams, so
    /// it is measured at its natural framing for a goodput head-to-head.
    Sens,
    /// Raw UDP one-way blast: NO reliability, NO congestion control.
    /// The unprotected-datagram reference - it reveals both the raw
    /// link ceiling (clean) and how much a bare datagram stream loses
    /// when the link drops or rate-limits (delivery ratio reported
    /// alongside goodput, since "goodput" alone hides UDP's losses).
    Udp,
}

impl Transport {
    fn from_name(s: &str) -> Result<Self, String> {
        match s {
            "quic" => Ok(Self::Quic),
            "tcp" => Ok(Self::Tcp),
            "tcptls" => Ok(Self::Tcptls),
            "btcp" => Ok(Self::Btcp),
            "sens" => Ok(Self::Sens),
            "udp" => Ok(Self::Udp),
            other => Err(format!("unknown transport: {other}")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Quic => "quic",
            Self::Tcp => "tcp",
            Self::Tcptls => "tcptls",
            Self::Btcp => "btcp",
            Self::Sens => "sens",
            Self::Udp => "udp",
        }
    }
}

struct Args {
    transport: Transport,
    role: String,
    bind: Option<SocketAddr>,
    connect: Option<SocketAddr>,
    items: u64,
    rounds: u64,
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    /// Sens-O-Matic item size (its MTU datagram payload). Ignored by the
    /// stream bridges, which ferry the fixed 64-byte ring slot.
    item_bytes: usize,
    /// Sens-O-Matic receiver-side loss injection percent (the stream
    /// bridges take wire loss from netem instead, since TCP/QUIC handle
    /// loss in the kernel/quinn, not the app).
    loss: u32,
    /// Sens-O-Matic receiver-side REVERSE-path loss: drop this percent of
    /// OUTGOING feedback, to exercise bidirectional loss accounting (forward
    /// `--loss` vs reverse `--fb-loss`) without per-direction netem.
    fb_loss: u32,
    seed: u64,
    /// Sens-O-Matic block geometry: k data + r parity shards. r=0 disables
    /// FEC (no parity datagrams), isolating the encoding/parity cost from
    /// the rest of the datagram path.
    k: usize,
    r: usize,
    /// Sens-O-Matic stream shards: N independent streams across N threads
    /// (shard `s` on port base+s), the whole data path distributed over
    /// cores. 1 = the single-threaded path.
    shards: usize,
    /// Benchmark the clean-link loopback ceiling: pin the feed-forward link
    /// sensor to "clean" (StubSensor) so the controller can reach Passthrough.
    /// The platform sensor reads the host's real NIC, which is the wrong
    /// interface for a loopback transport (loopback never drops), so it would
    /// otherwise hold a thin parity band that understates the loopback ceiling.
    clean_sensor: bool,
    /// Disable the bufferbloat pacer (the un-paced baseline for an A/B).
    no_pace: bool,
    /// Disable proactive burst-recovery (the reactive-NAK baseline for an A/B).
    no_proactive: bool,
    /// Drive interleave from the Gilbert-Elliott burst model instead of the
    /// jitter heuristic (receiver-side A/B knob).
    ge_burst: bool,
    /// Inject Gilbert-Elliott burst loss at the receiver: `(p, r)` per-10000
    /// transition probs, mean burst `10000 / r`. `(0, 0)` disables it.
    burst_loss: (u32, u32),
    /// Synthetically fire one OS path event at startup (the item-12 active
    /// path-event observer), for a host where flapping a real interface is
    /// impractical. The real proof flaps a route / MTU on the running host.
    sim_path_event: bool,
    /// Erasure code for the Sens transport: "rs" (block Cauchy Reed-Solomon,
    /// the default), "rlc" (sliding-window Random Linear Code), or "auto"
    /// (unified endpoint that switches RLC <-> RS on measured loss).
    fec: String,
    /// Code override for `--fec auto`: "auto" (loss-driven, default), "rlc"
    /// (force the sliding-window code), or "rs" (force the block code).
    code: String,
    /// Pin the RLC code at its initial parameters, ignoring sensing feedback
    /// (the static baseline for an adaptive-vs-static A/B). Adaptive is default.
    rlc_static: bool,
    /// Wrap the RLC transport in the optional TLS 1.3 record layer (needs the
    /// `tls` feature and a shared `--cert` / `--key` pair from `--gen-cert`).
    tls: bool,
    /// Rebind the RLC client's socket after this many items (a NAT-rebinding /
    /// interface-switch stand-in); the receiver follows the connection id. 0 off.
    migrate_after: u64,
    /// Operator-driven code-switch schedule for `--fec auto`, exercising the
    /// RLC<->RS handover (and the RS->RLC stream-resync) deterministically:
    /// `--switch-seq 50000:rs,120000:rlc` forces RS at item 50000 and back to RLC
    /// at item 120000. Each `(item_index, code)` fires force_switch once when the
    /// sender reaches that index. Empty = no forced switches (loss-driven only).
    switch_seq: Vec<(u64, String)>,
    /// Run the stream-multiplexing transport: two streams (one Protected / RLC,
    /// one Bulk / ARQ) over one connection, delivered independently.
    mux: bool,
    /// RLC sender flow-control window (outstanding source symbols). 0 = the
    /// transport default. On a high-BDP WAN path the default caps throughput at
    /// `window * symbol_len / RTT`; raising it lets the sender fill the pipe.
    flow_window: u32,
    /// Drive the RLC sender's in-flight bound from BBR's dynamic congestion
    /// window (cwnd_gain * BtlBw * RTprop) instead of the static flow_window, so
    /// it self-sizes to the path and ProbeBW grows it to fill a high-BDP link.
    bbr_cwnd: bool,
    /// RLC repair cadence: one repair symbol per this many source symbols. Lower
    /// = more parity = more induced loss recovered FORWARD (no ARQ stall), so a
    /// pushed rate converts to goodput instead of retransmits. 0 = default (4).
    rlc_step: usize,
    /// Batch steady-state DATA datagrams into one `sendmsg` via UDP GSO
    /// (`UDP_SEGMENT`), collapsing the per-symbol syscall cost (Sens/RLC only).
    gso: bool,
    /// Static rate pacing: spread the in-flight window over the RTT so a larger
    /// `--flow-window` fills the BDP without bursting the bottleneck (Sens/RLC).
    paced: bool,
    /// Fixed-rate pacing target in Mbit/s (the offensive FEC-push): drive the
    /// wire toward the path's raw capacity, past where loss-based control backs
    /// off, and let the FEC recover the induced loss. 0 = off (Sens/RLC only).
    pace_mbit: f64,
    /// Adaptive FEC-push start rate in Mbit/s (0 = off): closed-loop pacing that
    /// probes up while the FEC absorbs the induced loss and backs off to the
    /// delivered rate on a path drop - fills headroom AND survives variance.
    adaptive_push: f64,
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().collect();

    // Cert generation is a standalone mode: write the pair and exit.
    if argv.get(1).map(|s| s.as_str()) == Some("--gen-cert") {
        let cert_path = argv.get(2).ok_or("--gen-cert needs <cert> <key> paths")?;
        let key_path = argv.get(3).ok_or("--gen-cert needs <cert> <key> paths")?;
        let (cert, key) = generate_self_signed_cert(SNI)
            .map_err(|e| format!("cert generation: {e}"))?;
        std::fs::write(cert_path, &cert).map_err(|e| e.to_string())?;
        std::fs::write(key_path, &key).map_err(|e| e.to_string())?;
        println!("wrote {cert_path} ({} bytes) + {key_path} ({} bytes), sni={SNI}",
                 cert.len(), key.len());
        std::process::exit(0);
    }

    let mut transport = None;
    let mut role = None;
    let mut bind = None;
    let mut connect = None;
    let mut items = 200_000u64;
    let mut rounds = 2_000u64;
    let mut cert = None;
    let mut key = None;
    let mut item_bytes = 1408usize; // ~MTU (22 * 64-byte slots), Sens-O-Matic only
    let mut loss = 0u32;
    let mut fb_loss = 0u32;
    let mut seed = 1u64;
    let mut k = 8usize;
    let mut r = 2usize;
    let mut shards = 1usize;
    let mut clean_sensor = false;
    let mut no_pace = false;
    let mut no_proactive = false;
    let mut ge_burst = false;
    let mut burst_loss = (0u32, 0u32);
    let mut sim_path_event = false;
    let mut fec = "rs".to_string();
    let mut code = "auto".to_string();
    let mut rlc_static = false;
    let mut tls = false;
    let mut migrate_after = 0u64;
    let mut switch_seq: Vec<(u64, String)> = Vec::new();
    let mut mux = false;
    let mut flow_window = 0u32;
    let mut bbr_cwnd = false;
    let mut rlc_step = 0usize;
    let mut gso = false;
    let mut paced = false;
    let mut pace_mbit = 0.0f64;
    let mut adaptive_push = 0.0f64;

    let mut i = 1;
    while i < argv.len() {
        let need = |n: usize| -> Result<&String, String> {
            argv.get(n).ok_or_else(|| format!("{} needs a value", argv[n - 1]))
        };
        match argv[i].as_str() {
            "--transport" => {
                transport = Some(Transport::from_name(need(i + 1)?)?);
                i += 2;
            }
            "--role" => {
                role = Some(need(i + 1)?.clone());
                i += 2;
            }
            "--bind" => {
                bind = Some(need(i + 1)?.parse().map_err(|e| format!("--bind: {e}"))?);
                i += 2;
            }
            "--connect" => {
                connect = Some(need(i + 1)?.parse().map_err(|e| format!("--connect: {e}"))?);
                i += 2;
            }
            "--items" => {
                items = need(i + 1)?.parse().map_err(|e| format!("--items: {e}"))?;
                i += 2;
            }
            "--rounds" => {
                rounds = need(i + 1)?.parse().map_err(|e| format!("--rounds: {e}"))?;
                i += 2;
            }
            "--cert" => {
                cert = Some(std::fs::read(need(i + 1)?).map_err(|e| format!("--cert: {e}"))?);
                i += 2;
            }
            "--key" => {
                key = Some(std::fs::read(need(i + 1)?).map_err(|e| format!("--key: {e}"))?);
                i += 2;
            }
            "--item-bytes" => {
                item_bytes = need(i + 1)?.parse().map_err(|e| format!("--item-bytes: {e}"))?;
                i += 2;
            }
            "--loss" => {
                loss = need(i + 1)?.parse().map_err(|e| format!("--loss: {e}"))?;
                i += 2;
            }
            "--fb-loss" => {
                fb_loss = need(i + 1)?.parse().map_err(|e| format!("--fb-loss: {e}"))?;
                i += 2;
            }
            "--seed" => {
                seed = need(i + 1)?.parse().map_err(|e| format!("--seed: {e}"))?;
                i += 2;
            }
            "--k" => {
                k = need(i + 1)?.parse().map_err(|e| format!("--k: {e}"))?;
                i += 2;
            }
            "--r" => {
                r = need(i + 1)?.parse().map_err(|e| format!("--r: {e}"))?;
                i += 2;
            }
            "--shards" => {
                shards = need(i + 1)?.parse().map_err(|e| format!("--shards: {e}"))?;
                i += 2;
            }
            "--clean-sensor" => {
                clean_sensor = true;
                i += 1;
            }
            "--no-pace" => {
                no_pace = true;
                i += 1;
            }
            "--no-proactive" => {
                no_proactive = true;
                i += 1;
            }
            "--ge-burst" => {
                ge_burst = true;
                i += 1;
            }
            "--sim-path-event" => {
                sim_path_event = true;
                i += 1;
            }
            "--fec" => {
                fec = need(i + 1)?.clone();
                i += 2;
            }
            "--code" => {
                code = need(i + 1)?.clone();
                i += 2;
            }
            "--rlc-static" => {
                rlc_static = true;
                i += 1;
            }
            "--tls" => {
                tls = true;
                i += 1;
            }
            "--migrate-after" => {
                migrate_after = need(i + 1)?.parse().map_err(|e| format!("--migrate-after: {e}"))?;
                i += 2;
            }
            "--switch-seq" => {
                for entry in need(i + 1)?.split(',') {
                    let (at, code) = entry
                        .split_once(':')
                        .ok_or_else(|| format!("--switch-seq: expected item:code, got {entry}"))?;
                    let at: u64 = at.parse().map_err(|e| format!("--switch-seq item: {e}"))?;
                    let code = code.to_ascii_lowercase();
                    if code != "rs" && code != "rlc" {
                        return Err(format!("--switch-seq code must be rs|rlc, got {code}"));
                    }
                    switch_seq.push((at, code));
                }
                i += 2;
            }
            "--mux" => {
                mux = true;
                i += 1;
            }
            "--flow-window" => {
                flow_window = need(i + 1)?.parse().map_err(|e| format!("--flow-window: {e}"))?;
                i += 2;
            }
            "--bbr-cwnd" => {
                bbr_cwnd = true;
                i += 1;
            }
            "--gso" => {
                gso = true;
                i += 1;
            }
            "--paced" => {
                paced = true;
                i += 1;
            }
            "--pace-mbit" => {
                pace_mbit = need(i + 1)?.parse().map_err(|e| format!("--pace-mbit: {e}"))?;
                i += 2;
            }
            "--adaptive-push" => {
                adaptive_push =
                    need(i + 1)?.parse().map_err(|e| format!("--adaptive-push: {e}"))?;
                i += 2;
            }
            "--rlc-step" => {
                rlc_step = need(i + 1)?.parse().map_err(|e| format!("--rlc-step: {e}"))?;
                i += 2;
            }
            "--burst-loss" => {
                let p = need(i + 1)?.parse().map_err(|e| format!("--burst-loss p: {e}"))?;
                let r = need(i + 2)?.parse().map_err(|e| format!("--burst-loss r: {e}"))?;
                burst_loss = (p, r);
                i += 3;
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    Ok(Args {
        transport: transport.ok_or("--transport is required")?,
        role: role.ok_or("--role is required")?,
        bind,
        connect,
        items,
        rounds,
        cert,
        key,
        item_bytes: item_bytes.max(8),
        loss: loss.min(100),
        fb_loss: fb_loss.min(100),
        seed,
        k: k.max(1),
        r,
        shards: shards.max(1),
        clean_sensor,
        no_pace,
        no_proactive,
        ge_burst,
        burst_loss,
        sim_path_event,
        fec,
        code,
        rlc_static,
        tls,
        migrate_after,
        switch_seq,
        mux,
        flow_window,
        bbr_cwnd,
        rlc_step,
        gso,
        paced,
        pace_mbit,
        adaptive_push,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_default_crypto_provider();
    let args = parse_args()?;

    // Raw UDP blast is a synchronous datagram reference - no tokio,
    // no reliability, no congestion control.
    if args.transport == Transport::Udp {
        return match args.role.as_str() {
            "server" => udp_blast_server(&args),
            "client" => udp_blast_client(&args),
            other => Err(format!("udp supports server|client, not {other}").into()),
        };
    }

    // Sens-O-Matic is a synchronous datagram transport - it does not use
    // the tokio runtime the stream bridges need.
    if args.transport == Transport::Sens {
        if args.fec == "rlc" && args.mux {
            return match args.role.as_str() {
                "server" => mux_server(&args),
                "client" => mux_client(&args),
                other => Err(format!("mux supports server|client, not {other}").into()),
            };
        }
        if args.fec == "rlc" {
            return match args.role.as_str() {
                "server" => sens_rlc_server(&args),
                "client" => sens_rlc_client(&args),
                "ping" => sens_rlc_rtt(&args, true),
                "pong" => sens_rlc_rtt(&args, false),
                other => {
                    Err(format!("rlc fec supports server|client|ping|pong, not {other}").into())
                }
            };
        }
        if args.fec == "auto" {
            return match args.role.as_str() {
                "server" => sens_auto_server(&args),
                "client" => sens_auto_client(&args),
                other => Err(format!("auto fec supports server|client, not {other}").into()),
            };
        }
        return match args.role.as_str() {
            "server" => sens_oneway_server(&args),
            "client" => sens_oneway_client(&args),
            "ping" => sens_rtt(&args, true),
            "pong" => sens_rtt(&args, false),
            other => Err(format!("unknown role: {other} (server|client|ping|pong)").into()),
        };
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    match args.role.as_str() {
        "server" => rt.block_on(run_oneway_server(&args)),
        "client" => rt.block_on(run_oneway_client(&args)),
        "ping" => rt.block_on(run_rtt(&args, true)),
        "pong" => rt.block_on(run_rtt(&args, false)),
        other => Err(format!("unknown role: {other} (server|client|ping|pong)").into()),
    }
}

// ===================================================================
// Ring pair abstraction: the QUIC/TCP bridges ferry AdaptiveRings,
// the blocking TCP bridge ferries BlockingSpscRings. One enum keeps
// the app-side push/pop loops uniform.
// ===================================================================

enum AppRing {
    Adaptive(Arc<AdaptiveRing>),
    Blocking(Arc<BlockingSpscRing>),
}

impl AppRing {
    fn new(transport: Transport) -> Result<Self, String> {
        match transport {
            Transport::Quic | Transport::Tcp | Transport::Tcptls => {
                let ring = AdaptiveRing::create_anon(1, 1, RING_CAPACITY)
                    .map_err(|e| format!("ring create: {e:?}"))?;
                ring.register_producer().map_err(|e| format!("reg p: {e:?}"))?;
                ring.register_consumer().map_err(|e| format!("reg c: {e:?}"))?;
                Ok(Self::Adaptive(Arc::new(ring)))
            }
            Transport::Btcp => Ok(Self::Blocking(Arc::new(
                BlockingSpscRing::create_anon(RING_CAPACITY)
                    .map_err(|e| format!("blocking ring create: {e:?}"))?,
            ))),
            // Sens-O-Matic and raw UDP ship items directly (no app
            // ring); both are dispatched in main() before this point.
            Transport::Sens => Err("sens does not use AppRing".into()),
            Transport::Udp => Err("udp does not use AppRing".into()),
        }
    }

    /// App-side push: spin for the lock-free rings, futex-park for
    /// the blocking ring - each primitive's native discipline.
    fn push(&self, payload: &[u8]) {
        match self {
            Self::Adaptive(r) => {
                while r.try_send(0, payload).is_err() {
                    std::hint::spin_loop();
                }
            }
            Self::Blocking(r) => loop {
                match r.send_blocking(payload, Some(BLOCKING_TICK)) {
                    Ok(()) => break,
                    Err(BlockingError::Timeout) => continue,
                    Err(e) => panic!("send_blocking: {e:?}"),
                }
            },
        }
    }

    /// App-side pop into `out` (>= 64 bytes).
    fn pop(&self, out: &mut [u8]) {
        match self {
            Self::Adaptive(r) => {
                while r.try_recv(0, out).is_err() {
                    std::hint::spin_loop();
                }
            }
            Self::Blocking(r) => loop {
                match r.recv_blocking(out, Some(BLOCKING_TICK)) {
                    Ok(_) => break,
                    Err(BlockingError::Timeout) => continue,
                    Err(e) => panic!("recv_blocking: {e:?}"),
                }
            },
        }
    }
}

// ===================================================================
// Bridge task spawns: one server-accept future + one client-ship
// future per direction, dispatched per transport.
// ===================================================================

fn spawn_server(
    transport: Transport,
    ring: &AppRing,
    bind: SocketAddr,
    args: &Args,
) -> Result<tokio::task::JoinHandle<Result<u64, String>>, String> {
    match (transport, ring) {
        (Transport::Quic, AppRing::Adaptive(r)) => {
            let cert = args.cert.as_ref().ok_or("quic server needs --cert")?;
            let key = args.key.as_ref().ok_or("quic server needs --key")?;
            let config = make_server_config_from_der(cert, key)
                .map_err(|e| format!("server config: {e}"))?;
            let server = QuicBridgeServer::bind(Arc::clone(r), bind, config)
                .map_err(|e| format!("quic bind: {e}"))?;
            Ok(tokio::spawn(async move {
                server.accept_one().await.map_err(|e| format!("quic accept: {e}"))
            }))
        }
        (Transport::Tcp, AppRing::Adaptive(r)) => {
            let r = Arc::clone(r);
            Ok(tokio::spawn(async move {
                let server = TcpBridgeServer::bind(r, bind)
                    .await
                    .map_err(|e| format!("tcp bind: {e}"))?;
                server.accept_one().await.map_err(|e| format!("tcp accept: {e}"))
            }))
        }
        #[cfg(feature = "tcp-tls-bridge")]
        (Transport::Tcptls, AppRing::Adaptive(r)) => {
            let cert = args.cert.as_ref().ok_or("tcptls server needs --cert")?;
            let key = args.key.as_ref().ok_or("tcptls server needs --key")?;
            // Same self-signed cert the QUIC + RLC-TLS contenders use.
            let config = subetha_cxc::rlc_crypto::server_config(cert, key)
                .map_err(|e| format!("tls server config: {e}"))?;
            let r = Arc::clone(r);
            Ok(tokio::spawn(async move {
                let server = TcpTlsBridgeServer::bind(r, bind, config)
                    .await
                    .map_err(|e| format!("tcptls bind: {e}"))?;
                server.accept_one().await.map_err(|e| format!("tcptls accept: {e}"))
            }))
        }
        (Transport::Btcp, AppRing::Blocking(r)) => {
            let r = Arc::clone(r);
            Ok(tokio::spawn(async move {
                let server = BlockingTcpBridgeServer::bind(r, bind)
                    .await
                    .map_err(|e| format!("btcp bind: {e}"))?;
                server.accept_one().await.map_err(|e| format!("btcp accept: {e}"))
            }))
        }
        _ => Err("ring/transport mismatch".into()),
    }
}

fn spawn_client(
    transport: Transport,
    ring: &AppRing,
    connect: SocketAddr,
    n_items: u64,
    args: &Args,
) -> Result<tokio::task::JoinHandle<Result<(), String>>, String> {
    match (transport, ring) {
        (Transport::Quic, AppRing::Adaptive(r)) => {
            let cert = args.cert.as_ref().ok_or("quic client needs --cert")?;
            let config = make_client_config_from_der(cert)
                .map_err(|e| format!("client config: {e}"))?;
            let client = QuicBridgeClient::new(
                Arc::clone(r),
                connect,
                config,
                "0.0.0.0:0".parse().expect("wildcard addr"),
            );
            Ok(tokio::spawn(async move {
                client.run(n_items, SNI).await.map_err(|e| format!("quic run: {e}"))
            }))
        }
        (Transport::Tcp, AppRing::Adaptive(r)) => {
            let client = TcpBridgeClient::new(Arc::clone(r), connect);
            Ok(tokio::spawn(async move {
                client.run(n_items).await.map_err(|e| format!("tcp run: {e}"))
            }))
        }
        #[cfg(feature = "tcp-tls-bridge")]
        (Transport::Tcptls, AppRing::Adaptive(r)) => {
            let cert = args.cert.as_ref().ok_or("tcptls client needs --cert")?;
            let config = subetha_cxc::rlc_crypto::client_config(cert)
                .map_err(|e| format!("tls client config: {e}"))?;
            let client = TcpTlsBridgeClient::new(Arc::clone(r), connect, config);
            Ok(tokio::spawn(async move {
                client.run(n_items, SNI).await.map_err(|e| format!("tcptls run: {e}"))
            }))
        }
        (Transport::Btcp, AppRing::Blocking(r)) => {
            let client = BlockingTcpBridgeClient::new(Arc::clone(r), connect);
            Ok(tokio::spawn(async move {
                client.run(n_items).await.map_err(|e| format!("btcp run: {e}"))
            }))
        }
        _ => Err("ring/transport mismatch".into()),
    }
}

// ===================================================================
// oneway mode
// ===================================================================

async fn run_oneway_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let bind = args.bind.ok_or("server needs --bind")?;
    let items = args.items;
    let ring = AppRing::new(args.transport)?;

    let server_task = spawn_server(args.transport, &ring, bind, args)?;
    println!("BOUND {bind}");

    // Drain app: assert strict sequence order + sum while measuring
    // first-pop..last-pop wall time (the end-to-end delivery rate as
    // the application observes it).
    let drain_ring = match &ring {
        AppRing::Adaptive(r) => AppRing::Adaptive(Arc::clone(r)),
        AppRing::Blocking(r) => AppRing::Blocking(Arc::clone(r)),
    };
    let drain = std::thread::spawn(move || -> (u128, u64) {
        let mut out = [0u8; SLOT];
        let mut sum = 0u64;
        let mut t_first: Option<Instant> = None;
        // Periodic mid-flight count, so a time-bounded harness can sample
        // delivered goodput in a fixed window (matches the sens / udp paths).
        let mut last_progress = Instant::now();
        for expected in 0..items {
            drain_ring.pop(&mut out);
            let started = *t_first.get_or_insert_with(Instant::now);
            let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
            assert_eq!(seq, expected,
                       "order violated: got seq {seq}, expected {expected}");
            sum = sum.wrapping_add(seq);
            if last_progress.elapsed() >= Duration::from_millis(500) {
                eprintln!("FORECAST t={:.1}s received={}",
                          started.elapsed().as_secs_f64(), expected + 1);
                last_progress = Instant::now();
            }
        }
        let elapsed = t_first.expect("popped at least one item").elapsed();
        (elapsed.as_nanos(), sum)
    });

    let received = server_task.await??;
    let (first_to_last_ns, sum) = drain.join().expect("drain thread");

    assert_eq!(received, items, "bridge delivered {received} of {items}");
    let expected_sum = (0..items).fold(0u64, |a, b| a.wrapping_add(b));
    assert_eq!(sum, expected_sum, "sum mismatch");

    let secs = first_to_last_ns as f64 / 1e9;
    println!(
        "RESULT mode=oneway role=server transport={} items={} first_to_last_ns={} \
         ns_per_item={:.1} mitems_per_s={:.3} mbit_per_s={:.1} order_ok=true sum_ok=true",
        args.transport.name(), items, first_to_last_ns,
        first_to_last_ns as f64 / items as f64,
        items as f64 / secs / 1e6,
        items as f64 * SLOT as f64 * 8.0 / secs / 1e6,
    );
    Ok(())
}

async fn run_oneway_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let connect = args.connect.ok_or("client needs --connect")?;
    let items = args.items;
    let ring = AppRing::new(args.transport)?;

    // Producer app: pushes sequenced slots as fast as the ring
    // accepts them; the bridge drains concurrently.
    let push_ring = match &ring {
        AppRing::Adaptive(r) => AppRing::Adaptive(Arc::clone(r)),
        AppRing::Blocking(r) => AppRing::Blocking(Arc::clone(r)),
    };
    let app = std::thread::spawn(move || {
        let mut buf = [0u8; SLOT];
        for seq in 0..items {
            buf[..8].copy_from_slice(&seq.to_le_bytes());
            push_ring.push(&buf);
        }
    });

    let t0 = Instant::now();
    let client_task = spawn_client(args.transport, &ring, connect, items, args)?;
    client_task.await??;
    let ship_ns = t0.elapsed().as_nanos();
    app.join().expect("producer app thread");

    let secs = ship_ns as f64 / 1e9;
    println!(
        "RESULT mode=oneway role=client transport={} items={} ship_ns={} \
         ns_per_item={:.1} mitems_per_s={:.3} mbit_per_s={:.1}",
        args.transport.name(), items, ship_ns,
        ship_ns as f64 / items as f64,
        items as f64 / secs / 1e6,
        items as f64 * SLOT as f64 * 8.0 / secs / 1e6,
    );
    Ok(())
}

// ===================================================================
// rtt mode: ping pushes round r, waits for the echo of round r, and
// records the full ring -> wire -> remote ring -> remote app ->
// wire -> ring latency. pong echoes.
// ===================================================================

async fn run_rtt(args: &Args, is_ping: bool) -> Result<(), Box<dyn std::error::Error>> {
    let bind = args.bind.ok_or("rtt roles need --bind")?;
    let connect = args.connect.ok_or("rtt roles need --connect")?;
    let rounds = args.rounds;

    let out_ring = AppRing::new(args.transport)?; // local app -> wire
    let in_ring = AppRing::new(args.transport)?;  // wire -> local app

    // Bind the inbound server first, then gate the outbound connect
    // on the orchestrator's go-line so both peers' listeners are up
    // before either client dials.
    let server_task = spawn_server(args.transport, &in_ring, bind, args)?;
    println!("BOUND {bind}");
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;

    let client_task = spawn_client(args.transport, &out_ring, connect, rounds, args)?;

    let role = if is_ping { "ping" } else { "pong" };
    let app_out = match &out_ring {
        AppRing::Adaptive(r) => AppRing::Adaptive(Arc::clone(r)),
        AppRing::Blocking(r) => AppRing::Blocking(Arc::clone(r)),
    };
    let app_in = match &in_ring {
        AppRing::Adaptive(r) => AppRing::Adaptive(Arc::clone(r)),
        AppRing::Blocking(r) => AppRing::Blocking(Arc::clone(r)),
    };
    let echoed = Arc::new(AtomicU64::new(0));
    let echoed_app = Arc::clone(&echoed);

    let app = std::thread::spawn(move || -> Vec<u64> {
        let mut buf = [0u8; SLOT];
        let mut out = [0u8; SLOT];
        if is_ping {
            let mut samples = Vec::with_capacity(rounds as usize);
            for r in 0..rounds {
                buf[..8].copy_from_slice(&r.to_le_bytes());
                let t0 = Instant::now();
                app_out.push(&buf);
                app_in.pop(&mut out);
                samples.push(t0.elapsed().as_nanos() as u64);
                let seq = u64::from_le_bytes(out[..8].try_into().unwrap());
                assert_eq!(seq, r, "echo mismatch: got {seq}, expected {r}");
                echoed_app.fetch_add(1, AtomOrd::Relaxed);
            }
            samples
        } else {
            for _ in 0..rounds {
                app_in.pop(&mut out);
                app_out.push(&out[..SLOT]);
                echoed_app.fetch_add(1, AtomOrd::Relaxed);
            }
            Vec::new()
        }
    });

    let received = server_task.await??;
    client_task.await??;
    let mut samples = app.join().expect("rtt app thread");

    assert_eq!(received, rounds, "server side received {received} of {rounds}");
    assert_eq!(echoed.load(AtomOrd::Acquire), rounds);

    if is_ping {
        samples.sort_unstable();
        let n = samples.len();
        let sum: u128 = samples.iter().map(|&v| v as u128).sum();
        println!(
            "RESULT mode=rtt role={} transport={} rounds={} min_ns={} avg_ns={} \
             p50_ns={} p99_ns={} max_ns={}",
            role, args.transport.name(), rounds,
            samples[0],
            sum / n as u128,
            samples[n / 2],
            samples[(n * 99 / 100).min(n - 1)],
            samples[n - 1],
        );
    } else {
        println!("RESULT mode=rtt role={} transport={} rounds={} echoed_ok=true",
                 role, args.transport.name(), rounds);
    }
    Ok(())
}

// ===================================================================
// Sens-O-Matic: reliable-UDP FEC transport, measured at its natural
// MTU-item framing (synchronous; no ring, no tokio). The stream bridges
// ferry batched 64-byte ring slots; this ships forward-error-corrected
// MTU datagrams, so the head-to-head metric is application goodput and
// round-trip latency at the same payload, with loss the dividing line.
// ===================================================================

// ===================================================================
// Raw UDP one-way blast: the unprotected-datagram reference. No
// reliability, no congestion control, no FEC - it characterises both
// the raw link ceiling (clean) and how a bare datagram stream fares
// when the link drops (delivery ratio reported alongside goodput).
// ===================================================================

/// Out-of-data-range sentinel: the sender repeats it after the last
/// data item so the receiver can stop without a FIN (UDP has none).
const UDP_EOF_MARKER: u64 = u64::MAX;

/// Bind a UDP socket with generous SO_RCVBUF / SO_SNDBUF, so the loss
/// the bench measures is the LINK's (netem), not a socket-buffer
/// overflow - the same buffer treatment the reliable transports get.
fn bind_udp_blast(addr: SocketAddr) -> std::io::Result<std::net::UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let sock = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_recv_buffer_size(16 * 1024 * 1024).ok();
    sock.set_send_buffer_size(16 * 1024 * 1024).ok();
    sock.set_reuse_address(true)?;
    sock.bind(&addr.into())?;
    Ok(sock.into())
}

fn udp_blast_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let bind = args.bind.ok_or("server needs --bind")?;
    let (items, item_bytes) = (args.items, args.item_bytes.max(16));
    let sock = bind_udp_blast(bind)?;
    // Quiet-window timeout: once the sender stops (lossy tail loses the
    // EOF sentinels), a recv that blocks this long ends the transfer.
    sock.set_read_timeout(Some(Duration::from_millis(1500)))?;
    println!("BOUND {bind}");

    let mut buf = vec![0u8; item_bytes.max(2048)];
    let mut received: u64 = 0;
    let mut saw_eof = false;
    let mut t_first: Option<Instant> = None;
    let mut t_last = Instant::now();
    // Periodic mid-flight count, so a time-bounded harness can sample
    // delivered goodput in a fixed window (same as the sens path).
    let mut last_progress = Instant::now();
    loop {
        if let Some(t) = t_first
            && last_progress.elapsed() >= Duration::from_millis(500)
        {
            eprintln!("FORECAST t={:.1}s received={received}", t.elapsed().as_secs_f64());
            last_progress = Instant::now();
        }
        match sock.recv_from(&mut buf) {
            Ok((n, _)) if n >= 8 => {
                let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                if seq == UDP_EOF_MARKER {
                    saw_eof = true;
                    if received >= items {
                        break;
                    }
                    continue;
                }
                if seq < items {
                    t_first.get_or_insert_with(Instant::now);
                    t_last = Instant::now();
                    received += 1;
                    if received >= items {
                        break;
                    }
                }
            }
            Ok(_) => continue,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // No data for the quiet window: the sender finished (or
                // never started). End on data-seen / EOF; else keep
                // waiting for the first packet.
                if saw_eof || received > 0 {
                    break;
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    let first_to_last_ns = match t_first {
        Some(t) => t_last.duration_since(t).as_nanos().max(1),
        None => return Err("udp server received nothing".into()),
    };
    let secs = first_to_last_ns as f64 / 1e9;
    let delivery_ratio = received as f64 / items as f64;
    // Goodput is DELIVERED bytes / time - it does not credit UDP for
    // datagrams the link dropped. delivery_ratio carries the loss.
    let goodput = received as f64 * item_bytes as f64 * 8.0 / secs / 1e6;
    println!(
        "RESULT mode=oneway role=server transport=udp items={items} received={received} \
         delivery_ratio={delivery_ratio:.4} first_to_last_ns={first_to_last_ns} \
         ns_per_item={:.1} mitems_per_s={:.3} mbit_per_s={goodput:.1} order_ok=na sum_ok=na",
        first_to_last_ns as f64 / received.max(1) as f64,
        received as f64 / secs / 1e6,
    );
    Ok(())
}

fn udp_blast_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let connect = args.connect.ok_or("client needs --connect")?;
    let (items, item_bytes) = (args.items, args.item_bytes.max(16));
    let sock = bind_udp_blast("0.0.0.0:0".parse().expect("wildcard addr"))?;
    sock.connect(connect)?;

    let mut buf = vec![0u8; item_bytes];
    let t0 = Instant::now();
    for seq in 0..items {
        buf[..8].copy_from_slice(&seq.to_le_bytes());
        // Raw blast: no flow control, no pacing. A blocking socket
        // self-limits only when its own send buffer fills; the link's
        // qdisc drops whatever exceeds the rate. A real UDP blaster
        // does NOT retry a dropped datagram, so neither do we.
        loop {
            match sock.send(&buf) {
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(_) => break,
            }
        }
    }
    // Offered rate excludes the sentinel tail below.
    let ship_ns = t0.elapsed().as_nanos();

    // Repeat the EOF sentinel so a few survive a lossy link; seq =
    // u64::MAX is out of the data range, so the receiver never miscounts it.
    buf[..8].copy_from_slice(&UDP_EOF_MARKER.to_le_bytes());
    for _ in 0..64 {
        sock.send(&buf).ok();
        std::thread::sleep(Duration::from_millis(2));
    }

    let secs = ship_ns as f64 / 1e9;
    println!(
        "RESULT mode=oneway role=client transport=udp items={items} ship_ns={ship_ns} \
         ns_per_item={:.1} mitems_per_s={:.3} offered_mbit_per_s={:.1}",
        ship_ns as f64 / items as f64,
        items as f64 / secs / 1e6,
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
    );
    Ok(())
}

fn sens_oneway_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let bind = args.bind.ok_or("server needs --bind")?;
    let (items, item_bytes) = (args.items, args.item_bytes);
    if args.shards > 1 {
        return sens_sharded_server(args, bind, items, item_bytes);
    }
    let mut recv = ReliableUdpReceiver::bind(bind)?;
    if args.loss > 0 {
        recv = recv.with_debug_loss(args.loss, args.seed);
    }
    if args.fb_loss > 0 {
        recv = recv.with_feedback_drop(args.fb_loss);
    }
    if args.burst_loss.1 > 0 {
        recv = recv.with_gilbert_loss(args.burst_loss.0, args.burst_loss.1, args.seed);
    }
    if args.ge_burst {
        recv.set_ge_burst(true);
    }
    if args.sim_path_event {
        recv.inject_path_event();
    }
    println!("BOUND {bind}");

    let (mut got, mut sum) = (0u64, 0u64);
    let mut t_first: Option<Instant> = None;
    let t0 = Instant::now();
    // Item 16: log the Sprout forecast on a slow cadence so a variable-rate run
    // shows it tracking - and a conservative lower bound leading - the rate steps.
    let mut last_fc_log = Instant::now();
    while got < items {
        if t0.elapsed() > Duration::from_secs(180) {
            return Err(format!("sens server timeout: got {got} / {items}").into());
        }
        for item in recv.poll()? {
            t_first.get_or_insert_with(Instant::now);
            let seq = u64::from_le_bytes(item[..8].try_into().unwrap());
            assert_eq!(seq, got, "order violated: got seq {seq}, expected {got}");
            sum = sum.wrapping_add(seq);
            got += 1;
        }
        if last_fc_log.elapsed() >= Duration::from_millis(500) {
            let (lp, lc, ls) = recv.leo_cadence().unwrap_or((0.0, 0.0, 0.0));
            // `received` lets a time-bounded harness sample mid-flight goodput
            // (bytes delivered in a fixed window) by reading the last count.
            eprintln!(
                "FORECAST t={:.1}s received={got} forecast_mbit={:.1} \
                 leo_period_s={:.1} leo_conf={:.2} leo_to_spike_s={:.1}",
                t0.elapsed().as_secs_f64(),
                recv.forecast_bps() as f64 / 1e6,
                lp,
                lc,
                ls
            );
            last_fc_log = Instant::now();
        }
    }
    // Grace: keep feeding feedback so the sender learns the final ack.
    for _ in 0..100 {
        recv.nudge_feedback().ok();
        std::thread::sleep(Duration::from_millis(2));
    }
    let ns = t_first.expect("popped at least one item").elapsed().as_nanos();
    let expected_sum = (0..items).fold(0u64, |a, b| a.wrapping_add(b));
    assert_eq!(sum, expected_sum, "sum mismatch");
    let secs = ns as f64 / 1e9;
    // Reordering-guard telemetry. `false_recoveries` counts the D-SACK events
    // the guard caught (a spurious retransmit whose reordered original later
    // arrived); a nonzero value under reordering is the guard firing on real
    // wire traffic. `peak_loss` is the peak loss estimate (telemetry).
    let peak_loss = recv.peak_loss_x255();
    let false_recoveries = recv.false_recovery_count();
    // Gilbert-Elliott fitted mean burst length (consecutive lost shards), -1
    // before the fit converges. The A/B headline: under a known bursty channel
    // it recovers the real burst length the jitter heuristic only proxies.
    let mean_burst = recv.mean_burst_len();
    // Clock skew (Moon-Skelly-Towsley) and the skew-corrected OWD trend: on a
    // path with relative clock drift the raw trend reads a false slope; the
    // de-biased trend the controller consumes removes it.
    let owd_skew = recv.owd_skew();
    let owd_trend_debiased = recv.owd_trend_debiased();
    // ACK cadence (microseconds): shortens under reverse-path (feedback) loss,
    // so a smaller value than the 1000us default means the receiver detected its
    // feedback was being lost and sped up.
    let ack_interval_us = recv.ack_interval().as_micros();
    let fb_loss_est = recv.feedback_loss_est();
    // The peer's link class (from its Link frame): 0 unknown / 1 loopback /
    // 2 wired / 3 Wi-Fi / 4 cellular, plus a normalized quality.
    let (peer_link_class, peer_link_quality) = recv.peer_link();
    // Active OS path-event observer (item 12): how many route / carrier / MTU
    // events this end's netlink watcher fired, the local egress MTU it reads,
    // and the sender's MTU it learned from the `Pmtu` frame. Flapping a route
    // or dropping the MTU on this host mid-run bumps `net_events` and `pmtu`.
    let net_events = recv.net_event_count();
    let local_pmtu = recv.local_pmtu();
    let peer_pmtu = recv.peer_pmtu();
    // Peak path shift this end's observer reached: a mid-run route / MTU event
    // spikes it toward 1.0 (the live shift has since decayed) - the direct
    // proof the observer fired on the side that experienced the event.
    let path_shift_peak = recv.net_event_shift_peak();
    // WBest available-bandwidth estimate (item 13): the receiver measured the
    // dispersion of the sender's probe pairs / train.
    let (wbest_avail, wbest_cap) = recv.wbest_bps();
    let wbest_avail_mbit = wbest_avail as f64 / 1e6;
    let wbest_cap_mbit = wbest_cap as f64 / 1e6;
    // AccECN (item 15): how many of the sender's ECN-capable packets we saw and
    // how many the AQM marked CE - the raw counters behind the graded ce_rate.
    let (ce_count, ect_count) = recv.accecn_counts();
    // Sprout forecast (item 16): the receiver's final 5th-percentile next-tick
    // deliverable-rate prediction.
    let forecast_mbit = recv.forecast_bps() as f64 / 1e6;
    // LEO cadence (item 17): the handover period the receiver detected from the
    // OWD autocorrelation, its confidence, and the predicted time to next spike.
    let (leo_period_s, leo_conf, leo_to_spike_s) = recv.leo_cadence().unwrap_or((0.0, 0.0, 0.0));
    println!(
        "RESULT mode=oneway role=server transport=sens items={items} first_to_last_ns={ns} \
         ns_per_item={:.1} mitems_per_s={:.3} mbit_per_s={:.1} order_ok=true sum_ok=true \
         peak_loss_x255={peak_loss} false_recoveries={false_recoveries} \
         ack_interval_us={ack_interval_us} fb_loss_est={fb_loss_est:.3} \
         peer_link_class={peer_link_class} peer_link_quality={peer_link_quality} \
         mean_burst={mean_burst:.2} owd_skew={owd_skew:.6} owd_trend_debiased={owd_trend_debiased:.6} \
         net_events={net_events} local_pmtu={local_pmtu} peer_pmtu={peer_pmtu} \
         path_shift_peak={path_shift_peak:.3} \
         wbest_avail_mbit={wbest_avail_mbit:.1} wbest_cap_mbit={wbest_cap_mbit:.1} \
         ce_count={ce_count} ect_count={ect_count} forecast_mbit={forecast_mbit:.1} \
         leo_period_s={leo_period_s:.1} leo_conf={leo_conf:.2} leo_to_spike_s={leo_to_spike_s:.1}",
        ns as f64 / items as f64,
        items as f64 / secs / 1e6,
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
    );
    Ok(())
}

fn sens_oneway_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let connect = args.connect.ok_or("client needs --connect")?;
    let (items, item_bytes) = (args.items, args.item_bytes);
    if args.shards > 1 {
        return sens_sharded_client(args, connect, items, item_bytes);
    }
    let mut send = ReliableUdpSender::bind("0.0.0.0:0", connect, args.k, args.r, item_bytes)?;
    if args.clean_sensor {
        send = send.with_sensor(Box::new(subetha_cxc::link_sensor::StubSensor));
    }
    if args.no_pace {
        send.set_pacing(false);
    }
    if args.no_proactive {
        send.set_proactive_recovery(false);
    }
    if args.sim_path_event {
        send.inject_path_event();
    }
    let mut buf = vec![0u8; item_bytes];
    let t0 = Instant::now();
    for seq in 0..items {
        buf[..8].copy_from_slice(&seq.to_le_bytes());
        // Respect flow control: pause while the in-flight window is full,
        // pumping acks so we resume the instant window space frees.
        while send.flow_blocked() {
            send.pump_feedback().ok();
            if send.flow_blocked() {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        send.send_item(&buf)?;
    }
    send.flush()?;
    let acked = send.drain_until_acked(Duration::from_secs(120))?;
    let ns = t0.elapsed().as_nanos();
    let secs = ns as f64 / 1e9;
    let (passthrough_blocks, fec_blocks) = send.coding_counts();
    let path_obs = send
        .path_observation()
        .map(|(ttl, ecn, hops)| format!("ttl={ttl},ecn={ecn},hops={hops}"))
        .unwrap_or_else(|| "none".to_string());
    // Congestion share of the peer's loss (Biaz + Spike): high under a rising-
    // delay congestion drop, low under random wireless loss.
    let cong_frac = send.congestion_fraction();
    // Reverse-path (feedback) loss the sender measured from the receiver's
    // LossAcct: high when the feedback path is dropping, distinct from forward
    // data loss.
    let rev_loss = send.rev_loss();
    // BBR passive path model recovered from the ACK stream: bottleneck
    // bandwidth (Mbit/s), RTprop (ms), and the BDP (blocks) that sizes the
    // bottleneck-busy in-flight window.
    let btlbw_mbit = send.btlbw_bps() as f64 / 1e6;
    let rtprop_ms = send.rtprop_us() as f64 / 1e3;
    let bdp_blocks = send.bdp_blocks();
    // Self-induced queue delay (bufferbloat) and the resulting paced flow
    // window: under a deep buffer the queue rises and the window clamps toward
    // the BDP instead of growing, draining the queue.
    let queue_delay_ms = send.queue_delay_ms();
    let flow_window = send.flow_window();
    // Mean RTT under load: the sustained latency the bufferbloat pacer holds
    // down (the headline bufferbloat metric, vs the min RTT which only shows
    // the best moment).
    let rtt_mean_ms = send.rtt_mean_ms();
    // Link-liveness: dead spells detected, probes sent while dead, and blocks
    // proactively burst-retransmitted on recovery.
    let (dead_episodes, probes_sent, recovered_blocks) = send.liveness_stats();
    // Recovery interval: time from link-back to the pre-outage backlog fully
    // re-delivered - the recovery speed, isolated from the total transfer time.
    let recovery_ms = send.recovery_interval_ms();
    // Wi-Fi mesh / backhaul-hop estimate: round(log2(first-hop PHY / BtlBw))
    // gated on a healthy first hop and inflated RTT.
    let backhaul_hops = send.backhaul_hops();
    let first_hop_mbps = send.first_hop_mbps();
    // RTT-shape fingerprint: Sarle's bimodality (> 5/9 = a Wi-Fi hop on the
    // path) and the derived Wi-Fi confidence, which fills the link class when
    // the OS wireless read is unavailable.
    let rtt_bimodality = send.rtt_bimodality();
    let rtt_wifi_conf = send.rtt_wifi_confidence();
    // Active OS path-event observer (item 12): events this end's watcher fired,
    // this end's egress MTU, the peer's MTU learned from its `Pmtu` frame, and
    // the event-driven path-shift contribution the controller fused.
    let net_events = send.net_event_count();
    let local_pmtu = send.local_pmtu();
    let peer_pmtu = send.peer_pmtu();
    // Peak event-driven path shift over the run: a mid-transfer route / MTU
    // event spikes this to ~1.0 even though the live shift has since decayed.
    let path_shift_evt = send.net_event_shift_peak();
    // WBest active-probe estimate (item 13): the receiver's available-bandwidth /
    // effective-capacity report. The capacity cross-checks the passive BtlBw
    // above; the available bandwidth tracks a rate-limited / loaded bottleneck.
    let (avail_bw, wbest_cap) = send.avail_bw_bps();
    let avail_bw_mbit = avail_bw as f64 / 1e6;
    let wbest_cap_mbit = wbest_cap as f64 / 1e6;
    // Trace mini-traceroute + path asymmetry (item 14): the hops the Trace sweep
    // discovered toward the peer (router IP + per-hop RTT) and the forward vs
    // reverse hop-count asymmetry.
    let trace = send.trace_hops();
    for h in trace {
        eprintln!("TRACE hop ttl={} router={} rtt_us={}", h.ttl, h.addr, h.rtt_us);
    }
    let (asym_fwd, asym_rev, asym) = send.path_asymmetry();
    let fmt_opt = |o: Option<u8>| o.map(|v| v.to_string()).unwrap_or_else(|| "na".into());
    let (asym_fwd, asym_rev, asym) = (fmt_opt(asym_fwd), fmt_opt(asym_rev), fmt_opt(asym));
    let trace_hops = trace.len();
    // AccECN graded CE rate (item 15): the fraction of our ECN-capable packets an
    // AQM marked CE - a graded congestion signal that leads loss.
    let ce_rate = send.ce_rate();
    // Sprout forecast (item 16): the receiver's 5th-percentile next-tick
    // deliverable-rate prediction, which pre-sizes the window ahead of a dip.
    let forecast_mbit = send.forecast_bps() as f64 / 1e6;
    println!(
        "RESULT mode=oneway role=client transport=sens items={items} ship_ns={ns} \
         ns_per_item={:.1} mitems_per_s={:.3} mbit_per_s={:.1} fully_acked={acked} \
         passthrough_blocks={passthrough_blocks} fec_blocks={fec_blocks} path_obs={path_obs} \
         cong_frac={cong_frac:.3} rev_loss={rev_loss:.3} \
         btlbw_mbit={btlbw_mbit:.1} rtprop_ms={rtprop_ms:.3} bdp_blocks={bdp_blocks} \
         queue_delay_ms={queue_delay_ms:.1} flow_window={flow_window} rtt_mean_ms={rtt_mean_ms:.1} \
         dead_episodes={dead_episodes} probes_sent={probes_sent} recovered_blocks={recovered_blocks} \
         recovery_ms={recovery_ms:.1} first_hop_mbps={first_hop_mbps:.1} backhaul_hops={backhaul_hops} \
         rtt_bimodality={rtt_bimodality:.3} rtt_wifi_conf={rtt_wifi_conf:.3} \
         net_events={net_events} local_pmtu={local_pmtu} peer_pmtu={peer_pmtu} \
         path_shift_evt={path_shift_evt:.3} \
         avail_bw_mbit={avail_bw_mbit:.1} wbest_cap_mbit={wbest_cap_mbit:.1} \
         trace_hops={trace_hops} asym_fwd={asym_fwd} asym_rev={asym_rev} path_asymmetry={asym} \
         ce_rate={ce_rate:.3} forecast_mbit={forecast_mbit:.1}",
        ns as f64 / items as f64,
        items as f64 / secs / 1e6,
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
    );
    Ok(())
}

// Sliding-window RLC transport (--fec rlc): the convolutional erasure code that
// recovers an isolated loss from the next repair without a retransmit round
// trip. The block-RS path above is the MDS baseline; this is the low-latency
// primary. Both deliver every item in order; the RESULT lines expose how much
// RLC recovered without ARQ vs how often the ARQ floor fired.

fn sens_rlc_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use subetha_cxc::sens_rlc::SensOMaticRlcReceiver;
    let bind = args.bind.ok_or("server needs --bind")?;
    let (items, item_bytes) = (args.items, args.item_bytes.max(16));
    let mut recv = SensOMaticRlcReceiver::bind(bind, item_bytes)?;
    // Gilbert-Elliott burst loss (--burst-loss p r) drives the adaptive A/B; a
    // flat Bernoulli (--loss) is the isolated-loss case. GE takes precedence.
    if args.burst_loss.1 > 0 {
        recv = recv.with_gilbert_loss(args.burst_loss.0, args.burst_loss.1, args.seed);
    } else if args.loss > 0 {
        recv = recv.with_debug_loss(args.loss, args.seed);
    }
    #[cfg(feature = "tls")]
    if args.tls {
        let cert = args.cert.as_ref().ok_or("--tls needs --cert")?;
        let key = args.key.as_ref().ok_or("--tls needs --key")?;
        recv = recv.with_tls_server(subetha_cxc::rlc_crypto::server_config(cert, key)?)?;
    }
    #[cfg(not(feature = "tls"))]
    if args.tls {
        return Err("--tls requires building with --features tls".into());
    }
    println!("BOUND {bind}");
    #[cfg(feature = "tls")]
    if args.tls {
        recv.handshake()?;
    }

    let (mut got, mut sum) = (0u64, 0u64);
    let mut t_first: Option<Instant> = None;
    let t0 = Instant::now();
    // Periodic mid-flight count, so a time-bounded harness can sample
    // delivered goodput in a fixed window (matches the brs / stream paths).
    let mut last_progress = Instant::now();
    while got < items {
        if t0.elapsed() > Duration::from_secs(180) {
            return Err(format!("rlc server timeout: got {got} / {items}").into());
        }
        for item in recv.poll()? {
            t_first.get_or_insert_with(Instant::now);
            let seq = u64::from_le_bytes(item[..8].try_into().unwrap());
            assert_eq!(seq, got, "order violated: got seq {seq}, expected {got}");
            sum = sum.wrapping_add(seq);
            got += 1;
        }
        if let Some(t) = t_first
            && last_progress.elapsed() >= Duration::from_millis(500)
        {
            eprintln!("FORECAST t={:.1}s received={got}", t.elapsed().as_secs_f64());
            last_progress = Instant::now();
        }
    }
    // Grace: keep polling so the sender learns the final delivery frontier.
    for _ in 0..100 {
        recv.poll().ok();
        std::thread::sleep(Duration::from_millis(2));
    }
    let ns = t_first.expect("popped at least one item").elapsed().as_nanos();
    let expected_sum = (0..items).fold(0u64, |a, b| a.wrapping_add(b));
    assert_eq!(sum, expected_sum, "sum mismatch");
    let secs = ns as f64 / 1e9;
    // rlc_recovered = source symbols rebuilt by FEC with no retransmit;
    // naks_sent = the ARQ floor for losses the coding window could not cover.
    // The channel estimate is what the receiver fed back to drive adaptation:
    // measured loss, fitted Gilbert-Elliott mean burst, and congestion share.
    let (est_loss, est_burst, est_cong) = recv.channel_estimate();
    println!(
        "RESULT mode=oneway role=server transport=sens fec=rlc items={items} \
         first_to_last_ns={ns} mbit_per_s={:.1} order_ok=true sum_ok=true \
         rlc_recovered={} naks_sent={} feedback_sent={} migrations={} \
         path_validations={} path_validation_failures={} \
         est_loss={est_loss:.3} est_mean_burst={est_burst:.2} est_cong={est_cong:.3}",
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
        recv.rlc_recovered(),
        recv.naks_sent(),
        recv.feedback_sent(),
        recv.migrations(),
        recv.path_validations(),
        recv.path_validation_failures(),
    );
    Ok(())
}

fn sens_rlc_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use subetha_cxc::sens_rlc::SensOMaticRlcSender;
    let connect = args.connect.ok_or("client needs --connect")?;
    let (items, item_bytes) = (args.items, args.item_bytes.max(16));
    // Window 32, one repair every 4 source symbols (code rate 4/5, matching the
    // block-RS k=8 r=2 baseline for a fair A/B), dense coefficients. These are
    // the INITIAL parameters; with adaptation on (the default) the sensing
    // feedback retunes the window / cadence / density during the run. --rlc-static
    // pins them here for the static baseline.
    let step = if args.rlc_step > 0 { args.rlc_step } else { 4 };
    let mut send = SensOMaticRlcSender::bind("0.0.0.0:0", connect, 32, step, 15, item_bytes)?;
    if args.flow_window > 0 {
        send = send.with_flow_window(args.flow_window);
    }
    if args.bbr_cwnd {
        send = send.with_bbr_cwnd(true);
    }
    if args.gso {
        send = send.with_gso(true);
    }
    if args.paced {
        send = send.with_paced(true);
    }
    if args.pace_mbit > 0.0 {
        send = send.with_pace_mbit(args.pace_mbit);
    }
    if args.adaptive_push > 0.0 {
        // Cruise between 50 Mbit/s and a 500 Mbit/s safety ceiling, starting at
        // the given rate. The packet-pair estimator drives the pace to ~70% of
        // the measured raw capacity; the ceiling bounds any estimator excursion.
        send = send.with_adaptive_push(args.adaptive_push, 50.0, 500.0);
    }
    if args.rlc_static {
        send = send.with_static_params();
    }
    // --sim-path-event arms the OS path-event observer (item 12) so a synthesized
    // route / carrier change mid-stream drives a PROACTIVE, validated migration.
    if args.sim_path_event {
        send = send.with_path_observer(None);
    }
    #[cfg(feature = "tls")]
    if args.tls {
        let cert = args.cert.as_ref().ok_or("--tls needs --cert")?;
        send = send.with_tls_client(subetha_cxc::rlc_crypto::client_config(cert)?)?;
        send.handshake()?;
    }
    #[cfg(not(feature = "tls"))]
    if args.tls {
        return Err("--tls requires building with --features tls".into());
    }
    let mut buf = [0u8; 8];
    let t0 = Instant::now();
    let mut migrated = false;
    let mut path_evented = false;
    for seq in 0..items {
        // Rebind the local socket once mid-stream to stand in for a NAT
        // rebinding / interface switch. The connection id rides inside each
        // frame, so the receiver follows the session to the new 4-tuple.
        if args.migrate_after > 0 && !migrated && seq == args.migrate_after {
            send.migrate()?;
            migrated = true;
            eprintln!("MIGRATED after {seq} items -> new local port");
        }
        // Synthesize an OS path event mid-stream: the next send migrates
        // proactively and the receiver pre-validates the new path.
        if args.sim_path_event && !path_evented && seq == items / 2 {
            send.inject_path_event();
            path_evented = true;
            eprintln!("PATH EVENT injected at {seq} -> proactive validated migration");
        }
        buf.copy_from_slice(&seq.to_le_bytes());
        send.send_item(&buf)?;
    }
    let acked = send.drain_until_acked(items as u32, Duration::from_secs(120))?;
    let ns = t0.elapsed().as_nanos();
    let secs = ns as f64 / 1e9;
    // Final coding parameters and how many times feedback retuned them: the
    // direct evidence the sensing plane drove the code (window / rate / density
    // tracking the channel) vs the pinned static baseline.
    let (cw, cs, cd, con) = send.coding_params();
    let mode = if args.rlc_static { "static" } else { "adaptive" };
    println!(
        "RESULT mode=oneway role=client transport=sens fec=rlc rlc_mode={mode} items={items} \
         ship_ns={ns} mbit_per_s={:.1} fully_acked={acked} \
         final_window={cw} final_step={cs} final_dt={cd} coding_on={con} \
         adapt_count={} feedback_recv={} rtt_ms={:.3} bbr_btlbw_mbit={:.1} \
         proactive_migrations={} tls={}",
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
        send.adapt_count(),
        send.feedback_recv(),
        send.rtt_ms(),
        // BBR's measured bottleneck-bandwidth estimate (telemetry).
        send.btlbw_bps() * 8.0 / 1e6,
        send.proactive_migrations(),
        args.tls,
    );
    Ok(())
}

/// Map the `--code` override to a unified code policy.
fn code_policy(code: &str) -> subetha_cxc::sens_unified::CodePolicy {
    use subetha_cxc::sens_unified::CodePolicy;
    match code {
        "rlc" => CodePolicy::ForceRlc,
        "rs" => CodePolicy::ForceRs,
        _ => CodePolicy::default_auto(),
    }
}

// Unified Sens-O-Matic (--fec auto): one endpoint carrying both erasure codes,
// switching RLC <-> RS mid-stream on the loss the receiver feeds back. The
// receiver injects `--loss` into both decoders so the loss-driven switch is
// observable without a real lossy link.
fn sens_auto_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use subetha_cxc::sens_unified::{UnifiedConfig, UnifiedSensReceiver};
    let bind = args.bind.ok_or("server needs --bind")?;
    let (items, item_bytes) = (args.items, args.item_bytes);
    let cfg = UnifiedConfig {
        policy: code_policy(&args.code),
        symbol_len: item_bytes + 8,
        k: args.k,
        r: args.r,
        rlc_flow_window: if args.flow_window > 0 { args.flow_window } else { 4096 },
        debug_loss: args.loss,
        seed: args.seed,
        rlc_step: if args.rlc_step > 0 { args.rlc_step as u16 } else { 4 },
        rlc_static: args.rlc_static,
    };
    #[cfg(not(feature = "tls"))]
    if args.tls {
        return Err("--tls requires building with --features tls".into());
    }
    #[cfg(feature = "tls")]
    let mut recv = if args.tls {
        let cert = args.cert.as_ref().ok_or("--tls needs --cert")?;
        let key = args.key.as_ref().ok_or("--tls needs --key")?;
        UnifiedSensReceiver::bind_tls(bind, cfg, subetha_cxc::rlc_crypto::server_config(cert, key)?)?
    } else {
        UnifiedSensReceiver::bind(bind, cfg)?
    };
    #[cfg(not(feature = "tls"))]
    let mut recv = UnifiedSensReceiver::bind(bind, cfg)?;
    let mut got: u64 = 0;
    let mut expected: u64 = 0;
    let mut order_ok = true;
    let mut sum: u128 = 0;
    // Per-item delivery latency. The client stamps SystemTime nanos in bytes
    // [8..16]; the server is the same host, so SystemTime is the shared clock and
    // (deliver - send) is the true one-way latency. This is the axis where RLC's
    // incremental delivery beats RS buffering a full k-block before its first
    // decode, and recovery-from-next-repair beats a block-decode wait.
    let mut lat_ns: Vec<u64> = Vec::with_capacity(items as usize);
    let t0 = Instant::now();
    let mut last_progress = Instant::now();
    while got < items {
        let delivered = recv.poll()?;
        if delivered.is_empty() {
            if last_progress.elapsed() > Duration::from_secs(30) {
                break;
            }
            std::thread::sleep(Duration::from_micros(200));
            continue;
        }
        last_progress = Instant::now();
        for it in delivered {
            let mut s = [0u8; 8];
            s.copy_from_slice(&it[..8]);
            let seq = u64::from_le_bytes(s);
            if seq != expected {
                order_ok = false;
            }
            if it.len() >= 16 {
                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                let send_ns = u64::from_le_bytes(it[8..16].try_into().unwrap());
                lat_ns.push(now_ns.saturating_sub(send_ns));
            }
            expected += 1;
            sum += seq as u128;
            got += 1;
        }
    }
    let ns = t0.elapsed().as_nanos();
    let secs = ns as f64 / 1e9;
    // Linger briefly, still polling, so the receiver's final acks flush and the
    // client's drain completes instead of waiting out its ack timeout.
    let linger = Instant::now();
    while linger.elapsed() < Duration::from_millis(500) {
        recv.poll().ok();
        std::thread::sleep(Duration::from_micros(500));
    }
    let expected_sum = if items > 0 {
        (items as u128 - 1) * items as u128 / 2
    } else {
        0
    };
    let sum_ok = sum == expected_sum;
    // TTFD is the first delivered item's latency (in-order delivery => lat_ns[0]
    // is seq 0); capture it before sorting for percentiles.
    let ttfd_us = lat_ns.first().copied().unwrap_or(0) as f64 / 1000.0;
    lat_ns.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat_ns.is_empty() {
            return 0.0;
        }
        let idx = (((lat_ns.len() - 1) as f64) * p).round() as usize;
        lat_ns[idx] as f64 / 1000.0
    };
    let (lat_p50_us, lat_p99_us) = (pct(0.50), pct(0.99));
    let lat_max_us = lat_ns.last().copied().unwrap_or(0) as f64 / 1000.0;
    println!(
        "RESULT mode=oneway role=server transport=sens fec=auto items={got} \
         first_to_last_ns={ns} mbit_per_s={:.1} order_ok={order_ok} sum_ok={sum_ok} \
         switches={} final_code={:?} ttfd_us={ttfd_us:.1} lat_p50_us={lat_p50_us:.1} \
         lat_p99_us={lat_p99_us:.1} lat_max_us={lat_max_us:.1}",
        got as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
        recv.switches(),
        recv.active_code(),
    );
    if got < items {
        return Err(format!("auto server timeout: got {got} / {items}").into());
    }
    Ok(())
}

fn sens_auto_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use subetha_cxc::sens_unified::{SensCode, UnifiedConfig, UnifiedSensSender};
    let connect = args.connect.ok_or("client needs --connect")?;
    let (items, item_bytes) = (args.items, args.item_bytes);
    let cfg = UnifiedConfig {
        policy: code_policy(&args.code),
        symbol_len: item_bytes + 8,
        k: args.k,
        r: args.r,
        rlc_flow_window: if args.flow_window > 0 { args.flow_window } else { 4096 },
        debug_loss: 0,
        seed: args.seed,
        rlc_step: if args.rlc_step > 0 { args.rlc_step as u16 } else { 4 },
        rlc_static: args.rlc_static,
    };
    #[cfg(not(feature = "tls"))]
    if args.tls {
        return Err("--tls requires building with --features tls".into());
    }
    #[cfg(feature = "tls")]
    let mut send = if args.tls {
        let cert = args.cert.as_ref().ok_or("--tls needs --cert")?;
        UnifiedSensSender::connect_tls("0.0.0.0:0", connect, cfg, subetha_cxc::rlc_crypto::client_config(cert)?)?
    } else {
        UnifiedSensSender::connect("0.0.0.0:0", connect, cfg)?
    };
    #[cfg(not(feature = "tls"))]
    let mut send = UnifiedSensSender::connect("0.0.0.0:0", connect, cfg)?;
    let mut buf = vec![0u8; item_bytes];
    // Optional pacing for the latency measurement (--pace-mbit): at a rate below
    // capacity the pipe stays empty, so the server's per-item latency reflects the
    // code's true delivery latency (RS block-fill + decode vs RLC incremental)
    // rather than send-queue depth. 0 = unpaced (bulk throughput mode).
    let pace_ns: u64 = if args.pace_mbit > 0.0 {
        (item_bytes as f64 * 8.0 / (args.pace_mbit * 1e6) * 1e9) as u64
    } else {
        0
    };
    let t0 = Instant::now();
    for seq in 0..items {
        // Operator-driven switch schedule: force the handover at the scheduled
        // item BEFORE sending it, so item `at` is the first carried on the new
        // code. Exercises the RS->RLC stream-resync deterministically.
        for (at, code) in &args.switch_seq {
            if seq == *at {
                let to = if code == "rs" { SensCode::Rs } else { SensCode::Rlc };
                send.force_switch(to)?;
            }
        }
        buf[..8].copy_from_slice(&seq.to_le_bytes());
        // Send-time wall-clock stamp for the server's latency measurement.
        if buf.len() >= 16 {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            buf[8..16].copy_from_slice(&now_ns.to_le_bytes());
        }
        send.send_item(&buf)?;
        if pace_ns > 0 {
            let target = t0 + Duration::from_nanos(pace_ns.saturating_mul(seq + 1));
            let now = Instant::now();
            if target > now {
                std::thread::sleep(target - now);
            }
        }
    }
    let acked = send.finish()?;
    let ns = t0.elapsed().as_nanos();
    let secs = ns as f64 / 1e9;
    let (rw, rstep, rdt, rcoding) = send.rlc_coding_params();
    let radapt = send.rlc_adapt_count();
    let rawloss = send.raw_loss_estimate();
    let (rsent, rrecv) = send.raw_sent_recv();
    println!(
        "RESULT mode=oneway role=client transport=sens fec=auto items={items} \
         ship_ns={ns} mbit_per_s={:.1} fully_acked={acked} switches={} final_code={:?} \
         rlc_win={rw} rlc_step={rstep} rlc_dt={rdt} rlc_coding={rcoding} rlc_adapt={radapt} \
         raw_loss_est={rawloss:.4} sent={rsent} recv={rrecv}",
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
        send.switches(),
        send.active_code(),
    );
    Ok(())
}

/// Stream-multiplexing demo. Two streams ride one connection: stream 1 is
/// Protected (RLC repairs - the latency-critical role) and stream 2 is Bulk (ARQ
/// only - the throughput role). Each carries the same `items`-long u64 sequence;
/// the server reassembles and verifies each independently. Loss is injected on
/// the protected stream to show it recovers forward while the bulk stream relies
/// on ARQ - and a loss on one never blocks the other.
fn mux_expected(items: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(items as usize * 8);
    for i in 0..items {
        v.extend_from_slice(&i.to_le_bytes());
    }
    v
}

fn mux_server(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use subetha_cxc::stream_mux::{Protection, StreamMuxReceiver};
    let bind = args.bind.ok_or("server needs --bind")?;
    let (items, item_bytes) = (args.items, args.item_bytes.max(16));
    let mut recv = StreamMuxReceiver::bind(bind, item_bytes)?;
    // Inject loss on the protected stream (1) so its forward repair is exercised;
    // the bulk stream (2) stays clean to show independent delivery.
    if args.loss > 0 {
        recv = recv.with_stream_loss(1, args.loss, args.seed);
    }
    recv.expect_stream(1, Protection::Protected);
    recv.expect_stream(2, Protection::Bulk);
    println!("BOUND {bind}");

    let expected = mux_expected(items);
    let want = expected.len();
    let (mut got1, mut got2) = (Vec::new(), Vec::new());
    let (mut fin1, mut fin2) = (false, false);
    let t0 = Instant::now();
    let mut t_first: Option<Instant> = None;
    while !(fin1 && fin2) {
        if t0.elapsed() > Duration::from_secs(180) {
            return Err(format!(
                "mux server timeout: s1 {}/{want} fin={fin1}, s2 {}/{want} fin={fin2}",
                got1.len(),
                got2.len()
            )
            .into());
        }
        for d in recv.poll()? {
            t_first.get_or_insert_with(Instant::now);
            match d.stream_id {
                1 => {
                    got1.extend_from_slice(&d.data);
                    fin1 |= d.fin;
                }
                2 => {
                    got2.extend_from_slice(&d.data);
                    fin2 |= d.fin;
                }
                _ => {}
            }
        }
    }
    for _ in 0..100 {
        recv.poll().ok();
        std::thread::sleep(Duration::from_millis(2));
    }
    let ns = t_first.expect("delivered at least one symbol").elapsed().as_nanos();
    let s1_ok = got1 == expected;
    let s2_ok = got2 == expected;
    let secs = ns as f64 / 1e9;
    let total_bytes = (got1.len() + got2.len()) as f64;
    println!(
        "RESULT mode=mux role=server transport=sens fec=rlc items={items} \
         first_to_last_ns={ns} mbit_per_s={:.1} \
         stream1=protected stream1_ok={s1_ok} stream2=bulk stream2_ok={s2_ok} \
         fec_recovered={} naks_sent={}",
        total_bytes * 8.0 / secs / 1e6,
        recv.fec_recovered(),
        recv.naks_sent(),
    );
    if !(s1_ok && s2_ok) {
        return Err("mux delivery mismatch".into());
    }
    Ok(())
}

fn mux_client(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use subetha_cxc::stream_mux::{Protection, StreamMuxSender};
    let connect = args.connect.ok_or("client needs --connect")?;
    let (items, item_bytes) = (args.items, args.item_bytes.max(16));
    // conn window 512 symbols total, 256 per stream - the two-level cap.
    let mut send = StreamMuxSender::bind("0.0.0.0:0", connect, item_bytes, 512, 256)?;
    send.open_stream(1, Protection::Protected);
    send.open_stream(2, Protection::Bulk);
    let payload = mux_expected(items);
    let chunk = item_bytes * 4;
    let t0 = Instant::now();
    let mut off = 0usize;
    while off < payload.len() {
        let end = (off + chunk).min(payload.len());
        let fin = end == payload.len();
        send.write(1, &payload[off..end], fin)?;
        send.write(2, &payload[off..end], fin)?;
        off = end;
    }
    let ok = send.flush(Duration::from_secs(120))?;
    let ns = t0.elapsed().as_nanos();
    let secs = ns as f64 / 1e9;
    let total = payload.len() as f64 * 2.0;
    println!(
        "RESULT mode=mux role=client transport=sens fec=rlc items={items} \
         ship_ns={ns} mbit_per_s={:.1} fully_acked={ok} \
         stream1={:?} stream2={:?}",
        total * 8.0 / secs / 1e6,
        send.stream_protection(1).unwrap(),
        send.stream_protection(2).unwrap(),
    );
    Ok(())
}

// Sharded variants: N independent streams across N threads (shard `s` on
// port base+s), the whole data path distributed over cores. The receiver
// reassembles round-robin into the global item order.

fn sens_sharded_server(
    args: &Args,
    bind: SocketAddr,
    items: u64,
    item_bytes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut recv =
        ShardedReceiver::bind(bind.ip(), bind.port(), args.shards, items, args.loss, args.seed)?;
    println!("BOUND {bind} (sharded x{})", args.shards);

    let (mut got, mut sum) = (0u64, 0u64);
    let mut t_first: Option<Instant> = None;
    while got < items {
        let item = recv
            .recv_item()
            .ok_or("a shard ended before delivering all its items")?;
        t_first.get_or_insert_with(Instant::now);
        let seq = u64::from_le_bytes(item[..8].try_into().unwrap());
        assert_eq!(seq, got, "order violated: got seq {seq}, expected {got}");
        sum = sum.wrapping_add(seq);
        got += 1;
    }
    recv.finish();
    let ns = t_first.expect("popped at least one item").elapsed().as_nanos();
    let expected_sum = (0..items).fold(0u64, |a, b| a.wrapping_add(b));
    assert_eq!(sum, expected_sum, "sum mismatch");
    let secs = ns as f64 / 1e9;
    println!(
        "RESULT mode=oneway role=server transport=sens shards={} items={items} \
         first_to_last_ns={ns} ns_per_item={:.1} mitems_per_s={:.3} mbit_per_s={:.1} \
         order_ok=true sum_ok=true",
        args.shards,
        ns as f64 / items as f64,
        items as f64 / secs / 1e6,
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
    );
    Ok(())
}

fn sens_sharded_client(
    args: &Args,
    connect: SocketAddr,
    items: u64,
    item_bytes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut send = ShardedSender::bind(
        connect.ip(),
        connect.port(),
        args.shards,
        args.k,
        args.r,
        item_bytes,
    )?;
    let mut buf = vec![0u8; item_bytes];
    let t0 = Instant::now();
    for seq in 0..items {
        buf[..8].copy_from_slice(&seq.to_le_bytes());
        send.send_item(&buf);
    }
    let acked = send.finish();
    let ns = t0.elapsed().as_nanos();
    let secs = ns as f64 / 1e9;
    println!(
        "RESULT mode=oneway role=client transport=sens shards={} items={items} ship_ns={ns} \
         ns_per_item={:.1} mitems_per_s={:.3} mbit_per_s={:.1} fully_acked={acked}",
        args.shards,
        ns as f64 / items as f64,
        items as f64 / secs / 1e6,
        items as f64 * item_bytes as f64 * 8.0 / secs / 1e6,
    );
    Ok(())
}

fn sens_rtt(args: &Args, is_ping: bool) -> Result<(), Box<dyn std::error::Error>> {
    let bind = args.bind.ok_or("rtt roles need --bind")?;
    let connect = args.connect.ok_or("rtt roles need --connect")?;
    let (rounds, item_bytes) = (args.rounds, args.item_bytes);

    let mut recv = ReliableUdpReceiver::bind(bind)?;
    if args.loss > 0 {
        recv = recv.with_debug_loss(args.loss, args.seed);
    }
    println!("BOUND {bind}");
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;

    // k=1 so each item is its own block and ships immediately (a
    // request-response round, not a buffered stream); r=2 keeps a single
    // round recoverable under loss.
    let mut send = ReliableUdpSender::bind("0.0.0.0:0", connect, 1, 2, item_bytes)?;
    let mut buf = vec![0u8; item_bytes];

    if is_ping {
        let mut samples: Vec<u64> = Vec::with_capacity(rounds as usize);
        for r in 0..rounds {
            buf[..8].copy_from_slice(&r.to_le_bytes());
            let t0 = Instant::now();
            send.send_item(&buf)?;
            send.flush()?;
            'wait: loop {
                send.pump_feedback().ok();
                recv.nudge_feedback().ok();
                for item in recv.poll()? {
                    let seq = u64::from_le_bytes(item[..8].try_into().unwrap());
                    if seq == r {
                        samples.push(t0.elapsed().as_nanos() as u64);
                        break 'wait;
                    }
                }
            }
        }
        send.drain_until_acked(Duration::from_secs(10)).ok();
        samples.sort_unstable();
        let n = samples.len();
        let sum: u128 = samples.iter().map(|&v| v as u128).sum();
        println!(
            "RESULT mode=rtt role=ping transport=sens rounds={rounds} min_ns={} avg_ns={} \
             p50_ns={} p99_ns={} max_ns={}",
            samples[0], sum / n as u128, samples[n / 2],
            samples[(n * 99 / 100).min(n - 1)], samples[n - 1],
        );
    } else {
        let mut echoed = 0u64;
        let t0 = Instant::now();
        while echoed < rounds {
            if t0.elapsed() > Duration::from_secs(180) {
                return Err(format!("sens pong timeout: echoed {echoed} / {rounds}").into());
            }
            send.pump_feedback().ok();
            recv.nudge_feedback().ok();
            for item in recv.poll()? {
                send.send_item(&item)?;
                send.flush()?;
                echoed += 1;
            }
        }
        send.drain_until_acked(Duration::from_secs(10)).ok();
        println!("RESULT mode=rtt role=pong transport=sens rounds={rounds} echoed_ok=true");
    }
    Ok(())
}

/// Request-response round-trip latency over the sliding-window RLC transport -
/// the FEC counterpart to `sens_rtt` (which is the block-RS path). Each round is
/// one item sent and echoed back; under loss the window parity recovers a lost
/// round FORWARD with no retransmit round trip, so the tail latency stays low
/// where an ARQ stream (TCP/QUIC) stalls for a retransmit. Optional `--tls`
/// measures the encrypted transport; the AEAD seal/open is per-packet (no extra
/// round trips), so the plaintext and TLS round-trip times differ only by
/// microseconds. Both roles hold a sender (to the peer) and a receiver (local).
fn sens_rlc_rtt(args: &Args, is_ping: bool) -> Result<(), Box<dyn std::error::Error>> {
    use subetha_cxc::sens_rlc::{SensOMaticRlcReceiver, SensOMaticRlcSender};
    let bind = args.bind.ok_or("rtt roles need --bind")?;
    let connect = args.connect.ok_or("rtt roles need --connect")?;
    let (rounds, item_bytes) = (args.rounds, args.item_bytes.max(16));

    let mut recv = SensOMaticRlcReceiver::bind(bind, item_bytes)?;
    if args.loss > 0 {
        recv = recv.with_debug_loss(args.loss, args.seed);
    }
    // Same initial coding as the oneway RLC path: window 32, one repair every
    // `step` source symbols (default 4 = code rate 4/5), dense coefficients - so
    // the single-item rounds are recovered forward by the window parity.
    let step = if args.rlc_step > 0 { args.rlc_step } else { 4 };
    let mut send = SensOMaticRlcSender::bind("0.0.0.0:0", connect, 32, step, 15, item_bytes)?;

    println!("BOUND {bind}");
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;

    #[cfg(feature = "tls")]
    if args.tls {
        let cert = args.cert.as_ref().ok_or("--tls needs --cert")?;
        let key = args.key.as_ref().ok_or("--tls needs --key")?;
        // Two independent RLC sessions (ping->pong and pong->ping) each carry a
        // TLS 1.3 handshake. Serialize them by opposite ordering so neither side
        // blocks its peer: ping drives its SENDER session first, then its
        // receiver; pong drives its receiver first, then its sender.
        if is_ping {
            send = send.with_tls_client(subetha_cxc::rlc_crypto::client_config(cert)?)?;
            send.handshake()?;
            recv = recv.with_tls_server(subetha_cxc::rlc_crypto::server_config(cert, key)?)?;
            recv.handshake()?;
        } else {
            recv = recv.with_tls_server(subetha_cxc::rlc_crypto::server_config(cert, key)?)?;
            recv.handshake()?;
            send = send.with_tls_client(subetha_cxc::rlc_crypto::client_config(cert)?)?;
            send.handshake()?;
        }
    }
    #[cfg(not(feature = "tls"))]
    if args.tls {
        return Err("--tls requires building with --features tls".into());
    }

    // The logical request is just the 8-byte round id; `pack_symbol` pads it to
    // the `item_bytes` wire symbol (a 2-byte length prefix reserves the rest), so
    // the round trip still carries a full MTU symbol while the payload stays small
    // - matching the oneway RLC path, which also ships small items in MTU symbols.
    let mut buf = vec![0u8; 8];
    if is_ping {
        let mut samples: Vec<u64> = Vec::with_capacity(rounds as usize);
        for r in 0..rounds {
            buf[..8].copy_from_slice(&r.to_le_bytes());
            let t0 = Instant::now();
            send.send_item(&buf)?;
            'wait: loop {
                send.pump()?;
                for item in recv.poll()? {
                    let seq = u64::from_le_bytes(item[..8].try_into().unwrap());
                    if seq == r {
                        samples.push(t0.elapsed().as_nanos() as u64);
                        break 'wait;
                    }
                }
            }
        }
        send.drain_until_acked(rounds as u32, Duration::from_secs(10)).ok();
        samples.sort_unstable();
        let n = samples.len();
        let sum: u128 = samples.iter().map(|&v| v as u128).sum();
        println!(
            "RESULT mode=rtt role=ping transport=sens fec=rlc rounds={rounds} min_ns={} \
             avg_ns={} p50_ns={} p99_ns={} max_ns={} tls={}",
            samples[0],
            sum / n as u128,
            samples[n / 2],
            samples[(n * 99 / 100).min(n - 1)],
            samples[n - 1],
            args.tls,
        );
    } else {
        let mut echoed = 0u64;
        let t0 = Instant::now();
        while echoed < rounds {
            if t0.elapsed() > Duration::from_secs(180) {
                return Err(format!("rlc pong timeout: echoed {echoed} / {rounds}").into());
            }
            send.pump()?;
            for item in recv.poll()? {
                send.send_item(&item)?;
                echoed += 1;
            }
        }
        send.drain_until_acked(rounds as u32, Duration::from_secs(10)).ok();
        println!(
            "RESULT mode=rtt role=pong transport=sens fec=rlc rounds={rounds} echoed_ok=true tls={}",
            args.tls,
        );
    }
    Ok(())
}
