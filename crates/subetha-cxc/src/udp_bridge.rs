//! Sens-O-Matic bridge: ordered, lossless item delivery over
//! [`std::net::UdpSocket`] with no TLS and no async runtime.
//!
//! Sens-O-Matic is the reliable-UDP FEC transport - a sighted,
//! forward-correcting alternative to a blind, reactive ARQ stack. It
//! *senses* the channel (in-band loss, one-way-delay trend, radio link
//! stats) and *corrects ahead* (Cauchy Reed-Solomon FEC first, ARQ only
//! as the floor), named for the Sub-Etha Sens-O-Matic that detects
//! Sub-Etha signals. The protocol coding lives in [`crate::reliable_udp`];
//! this module is its socket layer. [`SensOMaticSender`] /
//! [`SensOMaticReceiver`] are the public names for the bridge pair.
//!
//! This is the socket layer over [`crate::reliable_udp`]. It ships
//! byte-slice items from one endpoint to another with FEC-primary /
//! ARQ-fallback reliability and an automatic parity rate. It depends
//! only on `std` - no tokio, no quinn, no rustls - so a trusted-network
//! bridge that wants UDP's properties without encryption pays nothing
//! for a TLS stack it does not use.
//!
//! [`ReliableUdpSender`] stages items into FEC blocks and answers ARQ
//! retransmit requests; [`ReliableUdpReceiver`] reassembles blocks,
//! FEC-recovers losses, delivers items in order, and feeds ACK / NAK /
//! loss reports back. The receiver socket parks on a read timeout (zero
//! idle CPU; the timeout also drives tail-ARQ), and the sender socket is
//! non-blocking so item throughput never waits on feedback.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::control_table::ControlTable;
use crate::fusion::{FusionPolicy, ImmediateUpConservativeDown, SensorSnapshot};
use crate::interleave::Interleaver;
use crate::control_frame::{
    decode_control, encode_control, is_control, AckFrame, ControlPacket, LinkFrame, LossAcctFrame,
    LossFrame, NakFrame, PathFrame, PmtuFrame, RingFrame, TimingFrame,
};
use crate::link_sensor::{platform_sensor, LinkClass, LinkSensor};
use crate::net_events::NetEventObserver;
use crate::path_model_sensor::PathModel;
use crate::path_sensor::PathSensor;
use crate::rtt_shape_sensor::RttShape;
use crate::reliable_udp::{is_outer_datagram, Decoder, Encoder, Feedback, NAK_NONE};

/// Receive-buffer size for an inbound CONTROL datagram. Generous: a control
/// packet carrying every frame is well under this, and over-sizing costs only
/// stack.
const CONTROL_RECV_BUF: usize = 256;

/// Extract the ack / NAK / loss frames of a decoded control packet into the
/// sender-side [`Feedback`] its controller already consumes. Absent frames
/// fall back to neutral defaults (no ack, no NAK, zero loss).
fn feedback_from_control(cp: &ControlPacket) -> Feedback {
    let (nak_block, nak_mask) = cp.nak.map(|n| (n.block, n.mask)).unwrap_or((NAK_NONE, 0));
    let loss = cp.loss.unwrap_or_default();
    Feedback {
        ack_through: cp.ack.map(|a| a.ack_through).unwrap_or(0),
        nak_block,
        nak_mask,
        loss_x255: loss.loss_x255,
        burstiness_x255: loss.burstiness_x255,
        owd_trend_class: loss.owd_trend_class,
        loss_class: loss.loss_class,
    }
}


/// The Sens-O-Matic sender carrying the **block Reed-Solomon** erasure code -
/// the original, MDS, fixed-parity, std-only code. Sens-O-Matic is the protocol
/// (the reliable FEC-UDP transport); the erasure code is its swappable detail,
/// like a cipher suite. The other code, sliding-window RLC, is
/// [`crate::sens_rlc::SensOMaticRlcSender`]. A branded alias for
/// [`ReliableUdpSender`].
pub type SensOMaticRsSender = ReliableUdpSender;

/// The Sens-O-Matic receiver for the block Reed-Solomon code. RLC counterpart:
/// [`crate::sens_rlc::SensOMaticRlcReceiver`]. A branded alias for
/// [`ReliableUdpReceiver`].
pub type SensOMaticRsReceiver = ReliableUdpReceiver;

/// Bare Sens-O-Matic aliases default to the Reed-Solomon code (the original).
/// Spell the code explicitly with [`SensOMaticRsSender`] /
/// [`crate::sens_rlc::SensOMaticRlcSender`] when it matters.
pub type SensOMaticSender = ReliableUdpSender;
/// Bare Sens-O-Matic receiver alias (Reed-Solomon code); see [`SensOMaticSender`].
pub type SensOMaticReceiver = ReliableUdpReceiver;

/// Largest datagram the receiver will read. A shard is `DATA_HEADER +
/// shard_len` bytes; this bounds `shard_len` to a typical MTU payload.
const RECV_BUF: usize = 2048;

/// The `vlen` argument type of `sendmmsg` / `recvmmsg`. Linux types it as
/// `unsigned int`; the BSDs type it as `size_t`. Aliasing keeps the one
/// scatter-gather code path compiling on both.
#[cfg(target_os = "linux")]
type MmsgLen = libc::c_uint;
#[cfg(target_os = "freebsd")]
type MmsgLen = usize;

/// Minimum spacing between NAKs for the SAME block. Feedback is emitted
/// on every poll, so without this a single lost block draws a NAK on
/// every packet and the sender retransmits it hundreds of times per
/// round-trip. One re-request per this interval is roughly one per RTT
/// on a LAN / Wi-Fi link.
const NAK_COOLDOWN: Duration = Duration::from_millis(12);

/// Target socket buffer size (receive and send). The flow window keeps
/// ~256 blocks of `k + r` shards in flight (~1 MiB); a buffer this size
/// holds that backlog so a fast clean link does not overflow the kernel
/// buffer and manufacture loss that would keep FEC needlessly armed. The
/// OS clamps the request to its configured maximum.
const SOCK_BUF_BYTES: usize = 8 << 20;

/// Size `sock`'s receive and send buffers to [`SOCK_BUF_BYTES`]. Best-effort:
/// a kernel that refuses or clamps the request just keeps a smaller buffer.
fn size_socket_buffers(sock: &UdpSocket) {
    let s = socket2::SockRef::from(sock);
    s.set_recv_buffer_size(SOCK_BUF_BYTES).ok();
    s.set_send_buffer_size(SOCK_BUF_BYTES).ok();
}

/// Minimum spacing between plain ACK feedback packets. The ack frontier
/// is cumulative, so it does not need a syscall on every datagram - a
/// NAK, a timeout drive, or this interval elapsing each force one.
const ACK_INTERVAL: Duration = Duration::from_millis(1);

/// Cap on NAKs emitted in one poll cycle. The receiver re-requests every
/// gap it is holding in a single round-trip (selective NAK) instead of
/// chasing them serially, but a burst of loss can leave many gaps at
/// once; this bounds the feedback burst per cycle and the rest are picked
/// up on the next poll (every few ms), so recovery stays parallel without
/// a feedback storm.
const MAX_NAKS_PER_CYCLE: usize = 64;

/// How often the sender emits a heartbeat (timestamp + ring digest).
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(20);

/// WBest active probe (item 13). A round is emitted this often; it is low
/// intrusion (a few dozen padded packets every couple of seconds), so it does
/// not perturb the transfer it measures.
const BW_PROBE_INTERVAL: Duration = Duration::from_secs(2);
/// Packet pairs in stage 1 (effective-capacity median) and packets in the
/// stage-2 train (available-bandwidth measurement).
const BW_PROBE_PAIRS: u8 = 8;
const BW_PROBE_TRAIN: u8 = 12;
/// On-wire size of each probe datagram. Large enough that the bottleneck
/// serialization dispersion is tens of microseconds (measurable against the
/// clock and jitter), the size the receiver's estimator assumes.
const BW_PROBE_BYTES: usize = 1400;

/// Trace mini-traceroute (item 14). A sweep of probes at IP TTL 1..=`MAX_TRACE_HOPS`
/// is emitted this often; each expired probe draws an ICMP TimeExceeded the
/// sender reads off its error queue for the per-hop router and RTT. The cadence
/// only drives the Linux error-queue path, so it is dead on other targets.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const TRACE_INTERVAL: Duration = Duration::from_secs(3);
const MAX_TRACE_HOPS: u8 = 8;

/// Sprout forecast tick (item 16): the receiver integrates arrivals over this
/// interval into one rate observation, the "next tick" the forecast bounds.
const FORECAST_TICK: Duration = Duration::from_millis(50);
/// Headroom above the forecast the predictive window cap allows, so the sender
/// keeps probing the link (the forecast can climb back) and the cap bites only
/// on a real dip - a forecast below `BtlBw / FORECAST_HEADROOM`.
const FORECAST_HEADROOM: f64 = 2.0;

/// How often the sender polls its platform link sensor (slow cadence,
/// never per packet).
const LINK_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

/// Floor the bufferbloat pacer will not shrink the flow window below, so a
/// transient BDP under-estimate cannot choke the pipe to a standstill.
const MIN_PACED_WINDOW: u32 = 4;

/// Target self-induced queue delay (ms) the LEDBAT pacer holds the window at:
/// enough standing queue to keep the bottleneck busy, little enough that the
/// added latency is small. RFC 6817 uses 100 ms for background bulk; a reliable
/// real-time transport wants the queue much shorter.
const PACE_TARGET_MS: f32 = 10.0;

/// Minimum spacing between pacer adjustments when `RTprop` is not yet known (1
/// ms). The queue responds a round trip after a window change, so the pacer
/// adjusts at most once per round trip; before the first RTT sample it falls
/// back to this floor.
const MIN_PACE_INTERVAL_US: u64 = 1000;

/// Multiple of the smoothed RTT after which TOTAL silence (no feedback of any
/// kind) marks the link dead. Several round trips with nothing back is a
/// liveness failure, not jitter.
const DEAD_RTT_MULTIPLE: u64 = 8;

/// Floor on the dead-link timeout (250 ms): a healthy link returns feedback
/// every few ms, so a quarter second of total silence is dead regardless of a
/// tiny RTT. The dead timeout is `max(DEAD_RTT_MULTIPLE * SRTT, this)`, and the
/// probe cadence while dead reuses it.
const DEAD_FLOOR_US: u64 = 250_000;

/// Floor on the rate the recovery resend is paced at (1 MB/s = 8 Mbit/s) when
/// no BtlBw estimate is available yet, so recovery still makes progress on a
/// link whose capacity was never measured.
const MIN_RECOVERY_BYTES_PER_S: u64 = 1_000_000;

/// Token-bucket depth for the paced recovery resend (8 KB ~ a few datagrams):
/// large enough to keep the pipe fed, small enough that the resend stays paced
/// at BtlBw rather than bursting.
const RECOVERY_BUCKET_BYTES: f64 = 8192.0;

/// Round trips of grace after the recovery resend drains during which the pacer
/// still holds (lets the recovery's queue clear before normal control resumes).
const RECOVERY_GRACE_RTTS: u64 = 4;

/// Wi-Fi-shape confidence above which the RTT-bimodality fingerprint fills the
/// link class as Wi-Fi when the OS wireless read is unavailable. A clear
/// margin above the bimodality threshold, so borderline shapes do not flip it.
const WIFI_SHAPE_CONFIDENCE: f32 = 0.15;

/// Sender half of the reliable-UDP bridge.
pub struct ReliableUdpSender {
    sock: crate::dgram::DgramSock,
    enc: Encoder,
    interleaver: Interleaver,
    control: Arc<ControlTable>,
    /// Fusion controller: maps receiver-reported sensors to coding knobs.
    fusion: Box<dyn FusionPolicy + Send>,
    /// Platform link sensor (radio / interface stats), polled slowly.
    link_sensor: Box<dyn LinkSensor + Send>,
    /// Last link-stress reading (0..1), fed forward into fusion.
    link_stress: f32,
    /// Last link class and a normalized quality from the link sensor, reported
    /// to the peer in the `Link` frame. `class_shift` spikes to 1.0 on a class
    /// change (a handoff - Wi-Fi to cellular, a wired uplink dropping to Wi-Fi)
    /// and decays, pre-arming protection like a hop-count shift does.
    link_class: LinkClass,
    link_quality: u8,
    class_shift: f32,
    /// Last first-hop PHY rate (kbit/s) and normalized MCS from the link sensor.
    /// The PHY rate is `nominal` for mesh-hop detection (the rate one Wi-Fi hop
    /// can carry); the MCS gates it (a healthy first hop means a low end-to-end
    /// `BtlBw` is a downstream backhaul hop, not a weak local radio).
    link_phy_kbps: u32,
    link_mcs_norm: f32,
    /// EWMA share of recent loss the peer classed congestion (0..1), from the
    /// `loss_class` it echoes. Congestion loss drives parity up broadly; a
    /// wireless drop is recovered locally without inflating effective loss.
    congestion_fraction: f32,
    /// Bidirectional control-plane loss accounting. `ctrl_out` counts heartbeat
    /// control packets sent, `ctrl_recv` counts feedback control packets
    /// received, and `peer_seq` is the highest `seq` the receiver has reported
    /// (how many feedback packets it sent). `rev_loss` is the share of the
    /// receiver's feedback we missed - reverse-path loss that stalls ARQ,
    /// distinct from the forward-path data loss the receiver measures.
    ctrl_out: u32,
    ctrl_recv: u32,
    peer_seq: u32,
    rev_loss: f32,
    /// The forward-loss fraction (0..=1) the receiver last fed back, stored so
    /// the unified endpoint can read it to drive the RS -> RLC code switch.
    last_fwd_loss: f32,
    /// Path sensor fed by the peer's echoed TTL / ECN observations: hop-count
    /// shifts and ECN congestion, both feed-forward predictors of loss.
    path_sensor: PathSensor,
    /// Active OS path-event observer: a background netlink / route watcher that
    /// spikes a path shift the instant the kernel announces a route, carrier,
    /// or MTU change - ahead of any loss, and ahead of the passive hop-count
    /// shift the `path_sensor` derives a round trip later. Fused as a third
    /// `path_shift` source. Its local egress MTU is reported to the peer in a
    /// `Pmtu` frame.
    net_events: NetEventObserver,
    /// The peer's last reported path MTU (from its `Pmtu` frame), and a decaying
    /// shift that spikes when that MTU drops - a peer-side handoff (a lower-MTU
    /// link engaging at the other end) is a path event this end should pre-arm
    /// for too. 0 = no report yet.
    peer_pmtu: u16,
    peer_pmtu_shift: f32,
    /// Peak event-driven path shift reached over the run (the OS-observer spike
    /// or a peer-MTU-drop spike). The instantaneous shift decays within a few
    /// seconds of the event, so this peak-hold is what makes a mid-transfer
    /// path event visible in the end-of-run telemetry.
    net_event_shift_peak: f32,
    /// BBR-style passive path model: bottleneck bandwidth, RTprop, and BDP,
    /// recovered from the ACK stream. Sizes the flow window and informs the
    /// pacer; it does not feed parity directly.
    path_model: PathModel,
    /// RTT-distribution shape fingerprint: a bimodal RTT (a fast first-transmit
    /// cluster and a slow retried cluster) means a Wi-Fi hop on the path, so the
    /// link class can be filled even when the local OS wireless read is
    /// unavailable (a wired host whose peer is on Wi-Fi).
    rtt_shape: RttShape,
    /// Per-block first-send time (block id, time_us) in send order, so an ACK
    /// that delivers a block yields its round-trip time. Pruned below the ack
    /// frontier each feedback, so it stays bounded by the in-flight window.
    block_send_us: VecDeque<(u32, u64)>,
    /// The full (un-paced) flow window captured at construction; the bufferbloat
    /// pacer only ever clamps the encoder's window DOWN from this toward the BDP
    /// to drain a self-induced queue, and restores it when the queue clears.
    flow_window_max: u32,
    /// Whether the bufferbloat pacer is active. On by default; an A/B harness
    /// can disable it to measure the un-paced baseline.
    pacing_enabled: bool,
    /// The LEDBAT pacer's flow window as a real number (the integer encoder
    /// window is its rounding). Starts at the full window and is nudged toward
    /// the size that holds the queue at [`PACE_TARGET_MS`].
    paced_window: f32,
    /// Time of the last pacer adjustment (microseconds since `start`); the pacer
    /// adjusts at most once per round trip.
    last_pace_us: u64,
    /// Link-liveness state. `last_feedback_at` is when the sender last received
    /// ANY feedback; when the silence exceeds a PTO derived from the smoothed
    /// RTT the link is declared dead. While dead the producer is already held by
    /// flow-control backpressure (the window cannot advance with no ACKs); the
    /// sender adds a periodic probe (a retransmit of the oldest unacked block)
    /// to both detect recovery and pre-position the stalled frontier. On the
    /// first feedback after a dead spell it proactively bursts the whole unacked
    /// window oldest-first, rather than waiting a round trip per NAK.
    last_feedback_at: Instant,
    link_dead: bool,
    last_probe_at: Instant,
    /// Whether proactive burst-recovery is enabled (the A/B baseline disables it
    /// to fall back to reactive NAK recovery).
    proactive_recovery: bool,
    /// Telemetry: dead spells detected, probes sent, blocks proactively
    /// retransmitted on recovery.
    dead_episodes: u64,
    probes_sent: u64,
    recovered_blocks: u64,
    /// Proactive-recovery resend queue (datagrams, oldest block first) and its
    /// token bucket. On recovery the whole still-unacked gap is enqueued here
    /// and drained at the item-6 BtlBw rate - the rate that fills the pipe
    /// without overflowing the buffer - so the recovery cooperates with the
    /// bufferbloat pacer instead of dumping a burst that trips it.
    recovery_dgrams: VecDeque<Vec<u8>>,
    recovery_tokens: f64,
    last_recovery_us: u64,
    /// Until this time (microseconds since `start`) the bufferbloat pacer holds
    /// its window instead of clamping: we KNOW a recovery resend is in flight,
    /// so the queue it briefly adds is an expected, intentional transient, not
    /// steady-state bloat. Without this the recovery would still throttle the
    /// window it just refilled. Extended while the resend drains, plus a grace
    /// of a few round trips for the queue to clear.
    recovery_grace_until_us: u64,
    /// Recovery-interval measurement: when a dead spell ends, `recovery_target`
    /// is the highest block id sent so far and `recovery_started_us` the time;
    /// when the ack frontier reaches that target the whole pre-outage backlog is
    /// re-delivered and `recovery_interval_us` records how long it took. This
    /// isolates the recovery speed (proactive resend vs reactive NAK learning)
    /// from the noisy total transfer time. `recovery_target == 0` means idle.
    recovery_target: u32,
    recovery_started_us: u64,
    recovery_interval_us: u64,
    /// Monotonic clock origin for heartbeat timestamps.
    start: Instant,
    /// When the last heartbeat went out.
    last_hb: Instant,
    /// When the link sensor was last polled.
    last_link_sample: Instant,
    /// When the last WBest probe round (item 13) was emitted, and its round id.
    /// A round is a burst of padded packet-pair probes followed by a packet
    /// train; the receiver measures their dispersion and reports the available
    /// bandwidth back, which the sender cross-checks against its passive BtlBw.
    last_bw_probe: Instant,
    bw_probe_round: u8,
    /// The receiver's most recent WBest report (kbit/s): available bandwidth and
    /// effective capacity. 0 = none yet.
    avail_bw_kbps: u64,
    wbest_capacity_kbps: u64,
    /// Trace mini-traceroute (item 14): the connected peer, the probe cadence /
    /// round, the per-TTL send time (for the RTT), the discovered hops, and the
    /// forward/reverse path-asymmetry tracker. The probe-emission fields only
    /// drive the Linux error-queue path, so they are dead on other targets.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    trace_peer: SocketAddr,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    last_trace: Instant,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    trace_round: u8,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    trace_send_us: Vec<u64>,
    trace_hops: Vec<crate::trace_sensor::TraceHop>,
    asym: crate::trace_sensor::PathAsymmetry,
    /// AccECN (item 15): the graded CE rate the peer's cumulative CE / ECT counts
    /// imply (`ce_count / ect_count`).
    ce_rate: f32,
    /// Sprout forecast (item 16): the peer's 5th-percentile next-tick deliverable
    /// rate (bytes/s), so the sender pre-sizes its window ahead of a dip.
    forecast_bps: u64,
    /// LEO cadence (item 17): the peer's detected handover period (seconds), its
    /// confidence, and the seconds to the next predicted spike. When a spike is
    /// imminent the sender pre-arms FEC one cycle ahead.
    leo_period_s: f32,
    leo_conf: f32,
    leo_secs_to_spike: f32,
}

/// ABI of the `WSASendMsg` extension entry point (Windows). It is not a
/// direct `ws2_32` export, so it is fetched once via
/// `WSAIoctl(SIO_GET_EXTENSION_FUNCTION_POINTER)`.
#[cfg(target_os = "windows")]
type LpfnWsaSendMsg = unsafe extern "system" fn(
    usize,
    *const windows_sys::Win32::Networking::WinSock::WSAMSG,
    u32,
    *mut u32,
    *mut core::ffi::c_void,
    *const core::ffi::c_void,
) -> i32;

/// Process-wide cache of the `WSASendMsg` pointer. `None` means the load
/// failed, so USO is treated as unsupported and the caller falls back to
/// per-datagram sends.
#[cfg(target_os = "windows")]
static WSASENDMSG_PTR: std::sync::OnceLock<Option<LpfnWsaSendMsg>> =
    std::sync::OnceLock::new();

/// Fetch (and cache) the `WSASendMsg` extension function pointer using the
/// given socket. The pointer is valid for every socket in the process, so
/// the first successful load is reused for the program's lifetime.
#[cfg(target_os = "windows")]
fn load_wsasendmsg(sock: usize) -> Option<LpfnWsaSendMsg> {
    *WSASENDMSG_PTR.get_or_init(|| {
        use windows_sys::Win32::Networking::WinSock::WSAIoctl;
        const SIO_GET_EXTENSION_FUNCTION_POINTER: u32 = 0xC800_0006;
        // WSAID_WSASENDMSG = {a441e712-754f-43ca-84a7-0dee44cf606d}
        let guid = windows_sys::core::GUID {
            data1: 0xa441_e712,
            data2: 0x754f,
            data3: 0x43ca,
            data4: [0x84, 0xa7, 0x0d, 0xee, 0x44, 0xcf, 0x60, 0x6d],
        };
        let mut func: usize = 0;
        let mut bytes: u32 = 0;
        // SAFETY: WSAIoctl on a valid connected socket; guid/func/bytes
        // outlive the call; the out buffer is exactly usize-sized.
        let rc = unsafe {
            WSAIoctl(
                sock,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                &guid as *const _ as *const core::ffi::c_void,
                size_of::<windows_sys::core::GUID>() as u32,
                &mut func as *mut usize as *mut core::ffi::c_void,
                size_of::<usize>() as u32,
                &mut bytes,
                std::ptr::null_mut(),
                None,
            )
        };
        if rc != 0 || func == 0 {
            None
        } else {
            let p = func as *const core::ffi::c_void;
            // SAFETY: WSAIoctl populated `func` with the WSASendMsg entry
            // point, whose ABI matches `LpfnWsaSendMsg`.
            Some(unsafe { std::mem::transmute::<*const core::ffi::c_void, LpfnWsaSendMsg>(p) })
        }
    })
}

/// ABI of the `WSARecvMsg` extension entry point (Windows). Like
/// `WSASendMsg` it is not a direct `ws2_32` export, so it is fetched once
/// via `WSAIoctl(SIO_GET_EXTENSION_FUNCTION_POINTER)`.
#[cfg(target_os = "windows")]
type LpfnWsaRecvMsg = unsafe extern "system" fn(
    usize,
    *mut windows_sys::Win32::Networking::WinSock::WSAMSG,
    *mut u32,
    *mut core::ffi::c_void,
    *const core::ffi::c_void,
) -> i32;

/// Process-wide cache of the `WSARecvMsg` pointer. `None` means the load
/// failed, so the receiver falls back to a plain `recv` with no TTL / ECN
/// cmsg.
#[cfg(target_os = "windows")]
static WSARECVMSG_PTR: std::sync::OnceLock<Option<LpfnWsaRecvMsg>> =
    std::sync::OnceLock::new();

/// Fetch (and cache) the `WSARecvMsg` extension function pointer. Valid for
/// every socket in the process, so the first successful load is reused for
/// the program's lifetime. Mirrors [`load_wsasendmsg`].
#[cfg(target_os = "windows")]
fn load_wsarecvmsg(sock: usize) -> Option<LpfnWsaRecvMsg> {
    *WSARECVMSG_PTR.get_or_init(|| {
        use windows_sys::Win32::Networking::WinSock::WSAIoctl;
        const SIO_GET_EXTENSION_FUNCTION_POINTER: u32 = 0xC800_0006;
        // WSAID_WSARECVMSG = {f689d7c8-6f1f-436b-8a53-e54fe351c322}
        let guid = windows_sys::core::GUID {
            data1: 0xf689_d7c8,
            data2: 0x6f1f,
            data3: 0x436b,
            data4: [0x8a, 0x53, 0xe5, 0x4f, 0xe3, 0x51, 0xc3, 0x22],
        };
        let mut func: usize = 0;
        let mut bytes: u32 = 0;
        // SAFETY: WSAIoctl on a valid socket; guid/func/bytes outlive the
        // call; the out buffer is exactly usize-sized.
        let rc = unsafe {
            WSAIoctl(
                sock,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                &guid as *const _ as *const core::ffi::c_void,
                size_of::<windows_sys::core::GUID>() as u32,
                &mut func as *mut usize as *mut core::ffi::c_void,
                size_of::<usize>() as u32,
                &mut bytes,
                std::ptr::null_mut(),
                None,
            )
        };
        if rc != 0 || func == 0 {
            None
        } else {
            let p = func as *const core::ffi::c_void;
            // SAFETY: WSAIoctl populated `func` with the WSARecvMsg entry
            // point, whose ABI matches `LpfnWsaRecvMsg`.
            Some(unsafe { std::mem::transmute::<*const core::ffi::c_void, LpfnWsaRecvMsg>(p) })
        }
    })
}

/// Whether the USO send path is enabled (default on). `SUBETHA_USO=0`
/// disables it for the per-datagram A/B baseline. Read once and cached.
#[cfg(target_os = "windows")]
fn uso_enabled() -> bool {
    static EN: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *EN.get_or_init(|| std::env::var("SUBETHA_USO").map(|v| v != "0").unwrap_or(true))
}

/// Count of USO sends the kernel accepted for in-stack segmentation.
#[cfg(target_os = "windows")]
static USO_OFFLOAD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Count of USO sends the kernel rejected, forcing per-datagram fallback.
#[cfg(target_os = "windows")]
static USO_FALLBACK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Process-wide USO telemetry as `(offload_batches, fallback_batches)`. A
/// nonzero first value means `WSASendMsg` with `UDP_SEND_MSG_SIZE` engaged
/// in-stack segmentation; a nonzero second means the kernel rejected USO and
/// the sender fell back to per-datagram sends. Windows-only; `(0, 0)`
/// everywhere else.
pub fn uso_stats() -> (u64, u64) {
    #[cfg(target_os = "windows")]
    {
        use std::sync::atomic::Ordering::Relaxed;
        (USO_OFFLOAD.load(Relaxed), USO_FALLBACK.load(Relaxed))
    }
    #[cfg(not(target_os = "windows"))]
    {
        (0, 0)
    }
}

impl ReliableUdpSender {
    /// Bind `local` and target `peer`. `k` data shards and an initial
    /// `r` parity shards per block; `max_item` is the largest item byte
    /// length. The socket is connected to `peer` and set non-blocking.
    /// Uses a private default [`ControlTable`] (interleave depth 1 =
    /// pass-through); use [`bind_with_control`](Self::bind_with_control)
    /// to share one with a controller.
    pub fn bind(
        local: impl ToSocketAddrs,
        peer: SocketAddr,
        k: usize,
        r: usize,
        max_item: usize,
    ) -> io::Result<Self> {
        Self::bind_with_control(local, peer, k, r, max_item, Arc::new(ControlTable::new()))
    }

    /// Like [`bind`](Self::bind) but shares a [`ControlTable`] with a
    /// controller, so interleave depth (and, as further knobs are
    /// wired, parity and coding level) are driven from it at runtime.
    pub fn bind_with_control(
        local: impl ToSocketAddrs,
        peer: SocketAddr,
        k: usize,
        r: usize,
        max_item: usize,
        control: Arc<ControlTable>,
    ) -> io::Result<Self> {
        // The per-block shard bitmap is a u32, so a block can hold at most
        // MAX_SHARDS (32) shards. k data shards alone must fit (k > MAX_SHARDS
        // overflows `1 << shard_index`); the encoder caps adaptive parity so
        // k + r stays within the bound. Reject an out-of-range k loudly here
        // rather than letting it silently corrupt the bitmap and stall delivery.
        if !(1..=crate::reliable_udp::MAX_SHARDS).contains(&k) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "RS data-shard count k={k} out of range: need 1 <= k <= {}",
                    crate::reliable_udp::MAX_SHARDS
                ),
            ));
        }
        let sock = UdpSocket::bind(local)?;
        sock.connect(peer)?;
        sock.set_nonblocking(true)?;
        size_socket_buffers(&sock);
        // Item 14: turn on the ICMP error queue (so an expired-TTL Trace probe's
        // TimeExceeded is delivered) and per-packet RX TTL (so the feedback's hop
        // count gives the reverse-path length for the asymmetry). Linux only.
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            crate::trace_sensor::enable_icmp_errors(sock.as_raw_fd());
        }
        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        {
            enable_ttl_ecn(&sock);
            // Item 15: mark our data ECN-capable so an AQM marks CE, not drops.
            set_ect(&sock);
        }
        // Wrap as the plain-UDP DgramSock backend AFTER the raw-fd feature setup
        // above: the standalone RS path keeps the fd (via as_udp) for GRO / TTL
        // / ECN / connected-send / USO; the unified path swaps in a demux socket.
        let sock = crate::dgram::DgramSock::from_udp(sock);
        let depth = control.interleave_depth() as usize;
        let now = Instant::now();
        let enc = Encoder::new(k, r, max_item);
        let flow_window_max = enc.flow_window();
        Ok(Self {
            sock,
            enc,
            interleaver: Interleaver::new(depth),
            control,
            fusion: Box::new(ImmediateUpConservativeDown::new(8)),
            link_sensor: platform_sensor(None),
            link_stress: 0.0,
            link_class: LinkClass::Unknown,
            link_quality: 0,
            class_shift: 0.0,
            link_phy_kbps: 0,
            link_mcs_norm: 0.0,
            congestion_fraction: 0.0,
            ctrl_out: 0,
            ctrl_recv: 0,
            peer_seq: 0,
            rev_loss: 0.0,
            last_fwd_loss: 0.0,
            path_sensor: PathSensor::new(),
            net_events: NetEventObserver::start(None),
            peer_pmtu: 0,
            peer_pmtu_shift: 0.0,
            net_event_shift_peak: 0.0,
            // Goodput block size: k data shards of `max_item` payload each
            // (parity and headers are wire overhead, not delivered data).
            path_model: PathModel::new(k * max_item),
            rtt_shape: RttShape::new(),
            block_send_us: VecDeque::new(),
            flow_window_max,
            pacing_enabled: true,
            paced_window: flow_window_max as f32,
            last_pace_us: 0,
            last_feedback_at: now,
            link_dead: false,
            last_probe_at: now,
            proactive_recovery: true,
            dead_episodes: 0,
            probes_sent: 0,
            recovered_blocks: 0,
            recovery_dgrams: VecDeque::new(),
            recovery_tokens: 0.0,
            last_recovery_us: 0,
            recovery_grace_until_us: 0,
            recovery_target: 0,
            recovery_started_us: 0,
            recovery_interval_us: 0,
            start: now,
            // Backdated so the very first `send_item` emits a heartbeat (after
            // one block, before the bottleneck queue fills), letting the
            // receiver's loss differentiator capture the empty-queue ROTT
            // baseline. Without this the first heartbeat lands at one interval,
            // by when a fast-filling queue is already full and the Spike has no
            // baseline to measure congestion against.
            last_hb: now.checked_sub(HEARTBEAT_INTERVAL).unwrap_or(now),
            // Backdated so the very first `maybe_sample_link` reads the
            // adapter immediately: the link-stress feed-forward must be live
            // from the first block, not after one sample interval (otherwise
            // a clean-but-degraded link could drop to Passthrough before the
            // sensor is ever read).
            last_link_sample: now.checked_sub(LINK_SAMPLE_INTERVAL).unwrap_or(now),
            last_bw_probe: now,
            bw_probe_round: 0,
            avail_bw_kbps: 0,
            wbest_capacity_kbps: 0,
            trace_peer: peer,
            last_trace: now,
            trace_round: 0,
            trace_send_us: vec![0u64; MAX_TRACE_HOPS as usize + 1],
            trace_hops: Vec::new(),
            asym: crate::trace_sensor::PathAsymmetry::new(),
            ce_rate: 0.0,
            forecast_bps: 0,
            leo_period_s: 0.0,
            leo_conf: 0.0,
            leo_secs_to_spike: 0.0,
        })
    }

    /// The current link-stress reading (0..1) from the platform sensor.
    pub fn link_stress(&self) -> f32 {
        self.link_stress
    }

    /// The last `(ttl, ecn, hop_count)` the peer echoed about THIS endpoint's
    /// packets, or `None` if no `Path` frame has arrived yet. A nonzero TTL
    /// proves the receiver extracted it from the wire and the control plane
    /// carried it back. Diagnostics for the path-sensing feed-forward.
    pub fn path_observation(&self) -> Option<(u8, u8, u8)> {
        self.path_sensor.last()
    }

    /// Count of OS path events (route / carrier / MTU changes) the active
    /// observer has seen. A nonzero value is the durable proof a real path
    /// event fired - the active observer's headline signal (telemetry).
    pub fn net_event_count(&self) -> u64 {
        self.net_events.event_count()
    }

    /// This endpoint's egress path MTU in bytes (0 = unknown), reported to the
    /// peer in the `Pmtu` frame (telemetry).
    pub fn local_pmtu(&self) -> u16 {
        self.net_events.pmtu().unwrap_or(0)
    }

    /// The peer's last reported path MTU in bytes (0 = none yet), from its
    /// `Pmtu` frame (telemetry).
    pub fn peer_pmtu(&self) -> u16 {
        self.peer_pmtu
    }

    /// The current event-driven path-shift contribution: the larger of the OS
    /// observer's decaying spike and the peer-MTU-drop spike (telemetry).
    pub fn net_event_shift(&self) -> f32 {
        self.net_events.path_shift().max(self.peer_pmtu_shift)
    }

    /// The peak event-driven path shift reached over the run. Unlike the
    /// instantaneous shift, which decays within a few seconds of the event,
    /// this holds the spike, so a mid-transfer path event stays visible at the
    /// end of the run (telemetry).
    pub fn net_event_shift_peak(&self) -> f32 {
        self.net_event_shift_peak
    }

    /// Synthetically fire a path event (the `--sim-path-event` demo path on a
    /// host where flapping a real interface is impractical). The production
    /// path is the active OS observer.
    pub fn inject_path_event(&self) {
        self.net_events.inject_event();
    }

    /// Synthetically set this endpoint's egress MTU (a drop also records a path
    /// event), as a real OS MTU change would. For tests / demos; production
    /// reads it from the active observer.
    pub fn inject_pmtu(&self, mtu: u16) {
        self.net_events.inject_pmtu(mtu);
    }

    /// The current congestion share (0..=1) of the peer's reported loss, from
    /// its echoed `loss_class` (Biaz + Spike). High when loss is congestion-
    /// driven (rising delay), low when it is random wireless loss. Diagnostics
    /// for the loss differentiator.
    pub fn congestion_fraction(&self) -> f32 {
        self.congestion_fraction
    }

    /// Reverse-path (feedback) loss share (0..=1): the fraction of the
    /// receiver's feedback control packets this sender missed, from the
    /// `LossAcct` the receiver echoes. Distinct from the forward-path data loss
    /// the receiver measures; lost feedback stalls ARQ, so the receiver responds
    /// by shortening its ACK cadence. Diagnostics.
    pub fn rev_loss(&self) -> f32 {
        self.rev_loss
    }

    /// The platform link-sensor backend in use (diagnostics).
    pub fn link_backend(&self) -> &'static str {
        self.link_sensor.backend()
    }

    /// BBR passive path model: bottleneck bandwidth in bits/sec, round-trip
    /// propagation delay in microseconds, and the bandwidth-delay product in
    /// blocks - all recovered from the ACK stream with no probe traffic. The
    /// BDP is the in-flight window that keeps the bottleneck busy without a
    /// standing queue. Diagnostics / window-sizing input.
    pub fn btlbw_bps(&self) -> u64 {
        self.path_model.btlbw_bps()
    }

    pub fn rtprop_us(&self) -> u64 {
        self.path_model.rtprop_us()
    }

    pub fn bdp_blocks(&self) -> u64 {
        self.path_model.bdp_blocks()
    }

    /// Estimated Wi-Fi backhaul-hop count (0..=3) behind the first hop, from the
    /// first-hop PHY rate (item 5) vs the measured `BtlBw` (item 6), gated on a
    /// healthy first hop and inflated RTT. Nonzero answers "are we behind a
    /// Wi-Fi-backhauled repeater" - which TTL cannot, since an L2 bridge does
    /// not decrement it. Diagnostics / parity-bias input.
    pub fn backhaul_hops(&self) -> u8 {
        self.path_model.backhaul_hops(
            self.link_phy_kbps as u64 * 1000,
            self.link_mcs_norm,
            self.congestion_fraction,
        )
    }

    /// The first-hop Wi-Fi PHY rate in Mbit/s (`nominal`) the link sensor read,
    /// or 0 off Wi-Fi. The mesh-hop count is `round(log2(this / BtlBw))` gated.
    /// Diagnostics.
    pub fn first_hop_mbps(&self) -> f32 {
        self.link_phy_kbps as f32 / 1000.0
    }

    /// The link class, with the RTT-shape fingerprint filling in for the OS read
    /// when it is unavailable: if the local sensor returned `Unknown` but the
    /// end-to-end RTT distribution is clearly bimodal, a Wi-Fi hop is on the
    /// path, so the class is reported as Wi-Fi.
    fn inferred_link_class(&self) -> LinkClass {
        if self.link_class == LinkClass::Unknown
            && self.rtt_shape.wifi_confidence() > WIFI_SHAPE_CONFIDENCE
        {
            LinkClass::Wifi
        } else {
            self.link_class
        }
    }

    /// Sarle's bimodality coefficient of the RTT distribution (`> 5/9` is
    /// bimodal - a Wi-Fi hop), or -1 before enough samples. Diagnostics.
    pub fn rtt_bimodality(&self) -> f32 {
        self.rtt_shape.bimodality().map(|b| b as f32).unwrap_or(-1.0)
    }

    /// Confidence in `0..=1` that the path carries a Wi-Fi hop, from the RTT
    /// shape alone. Diagnostics.
    pub fn rtt_wifi_confidence(&self) -> f32 {
        self.rtt_shape.wifi_confidence()
    }

    /// Self-induced queue delay in milliseconds (`RTT_now - RTprop`): the
    /// bufferbloat the sender is causing. The LEDBAT pacer holds this near its
    /// target by sizing the flow window. Diagnostics.
    pub fn queue_delay_ms(&self) -> f32 {
        self.path_model.queue_delay_us() as f32 / 1000.0
    }

    /// Mean RTT in milliseconds across the transfer - the sustained latency the
    /// bufferbloat pacer holds down. Diagnostics.
    pub fn rtt_mean_ms(&self) -> f32 {
        self.path_model.rtt_mean_us() as f32 / 1000.0
    }

    /// Current in-flight flow window (blocks). Equals the configured maximum on
    /// an unbloated path; smaller when the bufferbloat pacer has clamped it
    /// toward the BDP. Diagnostics.
    pub fn flow_window(&self) -> u32 {
        self.enc.flow_window()
    }

    /// Enable or disable the bufferbloat pacer. Disabling restores the full
    /// flow window and holds it there - the un-paced baseline for an A/B.
    pub fn set_pacing(&mut self, enabled: bool) {
        self.pacing_enabled = enabled;
        if !enabled {
            self.enc.set_flow_window(self.flow_window_max);
        }
    }

    /// Enable or disable proactive burst-recovery on link recovery. Disabling
    /// falls back to reactive NAK recovery - the A/B baseline.
    pub fn set_proactive_recovery(&mut self, enabled: bool) {
        self.proactive_recovery = enabled;
    }

    /// `true` while the link is declared dead (a PTO of total feedback
    /// silence). Diagnostics.
    pub fn link_dead(&self) -> bool {
        self.link_dead
    }

    /// Link-liveness telemetry: dead spells detected, probes sent while dead,
    /// and blocks proactively retransmitted on recovery. Diagnostics.
    pub fn liveness_stats(&self) -> (u64, u64, u64) {
        (self.dead_episodes, self.probes_sent, self.recovered_blocks)
    }

    /// The last recovery interval in milliseconds: time from the link coming
    /// back to the pre-outage backlog being fully re-delivered. Isolates the
    /// recovery speed (proactive resend vs reactive NAK learning) from the
    /// total transfer time. 0 if no recovery has completed. Diagnostics.
    pub fn recovery_interval_ms(&self) -> f32 {
        self.recovery_interval_us as f32 / 1000.0
    }

    /// The shared control table driving this sender.
    pub fn control(&self) -> &Arc<ControlTable> {
        &self.control
    }

    /// `(passthrough_blocks, fec_blocks)` sealed so far. A nonzero first value
    /// proves the controller dropped FEC fully off the wire (Passthrough) on a
    /// clean link; the second counts blocks that carried parity.
    pub fn coding_counts(&self) -> (u64, u64) {
        self.enc.coding_counts()
    }

    /// Replace the platform link sensor (e.g. a caller-driven or stub sensor).
    /// The sensor is a feed-forward loss predictor fused with the receiver's
    /// measured loss; swapping it lets a caller drive link stress directly.
    pub fn with_sensor(mut self, sensor: Box<dyn LinkSensor + Send>) -> Self {
        self.link_sensor = sensor;
        self
    }

    /// Replace the fusion policy that maps fused sensor readings to a coding
    /// decision (level, parity, interleave). The default is
    /// `ImmediateUpConservativeDown`; a caller can tune the confidence windows
    /// (how long to drop to Passthrough, how fast to re-arm).
    pub fn with_fusion(mut self, policy: Box<dyn FusionPolicy + Send>) -> Self {
        self.fusion = policy;
        self
    }

    /// Enable the tower outer code: every `d` data blocks ship with
    /// `r_outer` fire-and-forget outer-parity blocks that reconstruct
    /// whole-lost data blocks with no retransmit.
    pub fn enable_tower(&mut self, d: usize, r_outer: usize) {
        self.enc.enable_tower(d, r_outer);
    }

    /// Swap the datagram socket for one the caller already built (a demux socket
    /// the unified endpoint shares across both codes).
    pub fn set_sock(&mut self, sock: crate::dgram::DgramSock) {
        self.sock = sock;
    }

    /// The bound local address (useful when binding to port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Stage and transmit one item. A full block's datagrams pass
    /// through the interleaver (which holds up to `depth` blocks and
    /// emits column-major), then any pending feedback is drained so ARQ
    /// and flow control keep up.
    pub fn send_item(&mut self, item: &[u8]) -> io::Result<()> {
        self.sync_interleave()?;
        let block = self.enc.push(item);
        if !block.is_empty() {
            let pkts = self.interleaver.add_block(block);
            self.send_batch(&pkts)?;
            // Stamp this block's send time so the ACK that delivers it yields
            // an RTT for the BBR path model. The block just sealed by `push`
            // is `next_block_id - 1`. At the default interleave depth (1 =
            // pass-through) seal time is wire time; deeper interleaving adds a
            // bounded offset the RTprop min-filter sees through.
            let sealed = self.enc.next_block_id().wrapping_sub(1);
            self.block_send_us
                .push_back((sealed, self.start.elapsed().as_micros() as u64));
            // Control + feedback ride the per-BLOCK boundary, not every
            // staged item, so the hot path does not pay a recv syscall
            // per item (a `k`-fold reduction). Sample the link BEFORE the
            // heartbeat so the Link frame it carries reports the current
            // class / quality, not the previous block's.
            self.maybe_sample_link();
            self.maybe_send_heartbeat()?;
            self.maybe_send_bw_probe()?;
            self.maybe_send_trace()?;
            self.drain_feedback()?;
        }
        Ok(())
    }

    /// Flush a short final block and any blocks still buffered in the
    /// interleaver.
    pub fn flush(&mut self) -> io::Result<()> {
        let block = self.enc.flush();
        if !block.is_empty() {
            let pkts = self.interleaver.add_block(block);
            self.send_batch(&pkts)?;
        }
        let tail = self.interleaver.flush();
        self.send_batch(&tail)?;
        Ok(())
    }

    /// Re-read the interleave depth from the control table; on a change,
    /// the interleaver flushes its buffered blocks (sent here) before
    /// adopting the new depth.
    fn sync_interleave(&mut self) -> io::Result<()> {
        let want = self.control.interleave_depth() as usize;
        if want != self.interleaver.depth() {
            let pkts = self.interleaver.set_depth(want);
            self.send_batch(&pkts)?;
        }
        Ok(())
    }

    /// `true` when in-flight blocks have hit the flow window and the
    /// producer should pause until acks free space.
    pub fn flow_blocked(&self) -> bool {
        self.enc.flow_blocked()
    }

    /// Unacked blocks held for possible retransmission.
    pub fn pending_len(&self) -> usize {
        self.enc.pending_len()
    }

    /// Drain immediately-available feedback (apply acks, send any ARQ)
    /// and emit a heartbeat / link sample if due, WITHOUT blocking. Call
    /// this in a producer's backpressure loop while
    /// [`flow_blocked`](Self::flow_blocked) is true - unlike
    /// [`drain_until_acked`](Self::drain_until_acked) it returns at once,
    /// so the producer resumes the instant an ack frees window space.
    pub fn pump_feedback(&mut self) -> io::Result<()> {
        self.maybe_sample_link();
        self.maybe_send_heartbeat()?;
        self.drain_feedback()
    }

    /// The forward-loss fraction (0..=1) the receiver last fed back over the
    /// control plane. The unified endpoint reads this while RS is the active
    /// code to drive the RS -> RLC switch.
    pub fn fb_loss(&self) -> f64 {
        self.last_fwd_loss as f64
    }

    /// Drive feedback / ARQ until every block is acked or `timeout`
    /// elapses. Call after [`flush`](Self::flush) to guarantee the tail
    /// is delivered. Returns `true` if fully acked.
    pub fn drain_until_acked(&mut self, timeout: Duration) -> io::Result<bool> {
        let start = Instant::now();
        while self.enc.pending_len() > 0 {
            if start.elapsed() > timeout {
                return Ok(false);
            }
            self.maybe_send_heartbeat()?;
            self.drain_feedback()?;
            // Brief park so this tail drain is not a busy spin; the
            // receiver emits feedback on its own ~20ms timeout cadence.
            std::thread::sleep(Duration::from_micros(200));
        }
        Ok(true)
    }

    /// Read and apply all immediately-available feedback datagrams,
    /// transmitting any ARQ retransmits they request.
    fn drain_feedback(&mut self) -> io::Result<()> {
        let mut buf = [0u8; CONTROL_RECV_BUF];
        loop {
            // Standalone path reads the connected socket with the IP-TTL cmsg
            // (item 14 reverse-hop count); the demux path has no fd, so it pops
            // its queue via the connected recv (no TTL observation there).
            let res = match self.sock.as_udp() {
                Some(u) => recv_with_ttl(u, &mut buf),
                None => self.sock.recv(&mut buf).map(|n| (n, None)),
            };
            match res {
                Ok((n, ttl)) => {
                    // Item 14 reverse-hop count: the feedback's IP TTL gives how
                    // many hops the peer's packets crossed on the way back.
                    if let Some(t) = ttl {
                        self.asym
                            .observe_reverse(crate::path_sensor::hop_count_from_ttl(t));
                    }
                    if let Some(cp) = decode_control(&buf[..n]) {
                        // A feedback control packet from the receiver: count it,
                        // and read its LossAcct to learn how many feedback
                        // packets the receiver sent (peer_seq). The reverse-path
                        // loss is what we missed, as a share of what it sent -
                        // distinct from the forward data loss the receiver
                        // measures. The in-flight is negligible at scale, so the
                        // ratio converges to the loss fraction.
                        self.ctrl_recv = self.ctrl_recv.wrapping_add(1);
                        // Link-liveness: ANY feedback means the link is alive.
                        // Note whether we were dead; the proactive recovery
                        // burst fires AFTER `on_feedback` below applies this
                        // ACK, so it resends only the still-unacked (genuinely
                        // lost) blocks - not the whole window, most of which a
                        // dead-link recovery ACK frees at once (the data
                        // arrived; only the ACKs were lost).
                        self.last_feedback_at = Instant::now();
                        let was_dead = self.link_dead;
                        self.link_dead = false;
                        // Begin a recovery-interval measurement (both modes):
                        // the frontier must climb to the highest block sent so
                        // far for the pre-outage backlog to be fully delivered.
                        if was_dead {
                            self.recovery_target = self.enc.next_block_id();
                            self.recovery_started_us = self.start.elapsed().as_micros() as u64;
                        }
                        if let Some(la) = cp.loss_acct {
                            if la.seq > self.peer_seq {
                                self.peer_seq = la.seq;
                            }
                            let missed = self.peer_seq.saturating_sub(self.ctrl_recv);
                            self.rev_loss =
                                (missed as f32 / self.peer_seq.max(1) as f32).clamp(0.0, 1.0);
                        }
                        // Feed the path sensor before fusion, so a hop-count
                        // shift or ECN congestion in this packet is already
                        // reflected when the controller recomputes.
                        if let Some(p) = cp.path {
                            self.path_sensor.observe(p.ttl, p.ecn, p.hop_count);
                            // Item 14 forward-hop count: how many hops the peer
                            // reports OUR packets crossed (vs the reverse above).
                            self.asym.observe_forward(p.hop_count);
                            // Item 15 AccECN: the graded CE rate is the peer's
                            // cumulative CE marks over its ECN-capable packets.
                            // The cumulative ratio (not a per-feedback delta) is
                            // what stays stable: feedback fires every few packets,
                            // so at a low mark rate most intervals see zero new CE
                            // marks and a per-frame delta reads a noisy 0 - the
                            // running ratio is the AQM's mark rate directly.
                            if p.ect_count > 0 {
                                self.ce_rate =
                                    (p.ce_count as f32 / p.ect_count as f32).clamp(0.0, 1.0);
                            }
                        }
                        // The peer's egress MTU: a drop is a peer-side path
                        // event (a lower-MTU link engaged at the other end), so
                        // spike the shift to pre-arm this end too.
                        if let Some(pm) = cp.pmtu {
                            if self.peer_pmtu != 0 && pm.pmtu != 0 && pm.pmtu < self.peer_pmtu {
                                self.peer_pmtu_shift = 1.0;
                            }
                            if pm.pmtu != 0 {
                                self.peer_pmtu = pm.pmtu;
                            }
                        }
                        // WBest report (item 13): the receiver's available-
                        // bandwidth / effective-capacity estimate, held for
                        // telemetry and the cross-check against the passive BtlBw.
                        if let Some(ab) = cp.avail_bw {
                            self.avail_bw_kbps = ab.avail_kbps;
                            self.wbest_capacity_kbps = ab.capacity_kbps;
                        }
                        // Sprout forecast (item 16): the receiver's next-tick
                        // deliverable-rate lower bound, in bytes/s, used to
                        // pre-size the flow window ahead of a dip.
                        if let Some(fc) = cp.forecast {
                            self.forecast_bps = fc.forecast_kbps * 1000 / 8;
                        }
                        // LEO cadence (item 17): the receiver's detected handover
                        // period and time-to-next-spike, for the pre-arm.
                        if let Some(pe) = cp.periodicity {
                            self.leo_period_s = pe.period_ds as f32 / 10.0;
                            self.leo_secs_to_spike = pe.secs_to_spike_ds as f32 / 10.0;
                            self.leo_conf = pe.confidence_x255 as f32 / 255.0;
                        }
                        let fb = feedback_from_control(&cp);
                        let rtx = self.enc.on_feedback(&fb);
                        self.send_batch(&rtx)?;
                        // Proactive recovery: now that this ACK has freed every
                        // block the receiver actually got, ENQUEUE whatever is
                        // STILL unacked oldest-first - the genuinely-lost gap -
                        // for a BtlBw-paced resend, instead of waiting a round
                        // trip per NAK to relearn it. The resend is metered
                        // (`drain_recovery`) so it fills the pipe without
                        // overflowing, and the pacer is told to expect it.
                        if was_dead && self.proactive_recovery {
                            let gap = self.enc.retransmit_all_data();
                            if !gap.is_empty() {
                                self.recovered_blocks += self.enc.pending_len() as u64;
                                self.recovery_dgrams.extend(gap);
                                self.last_recovery_us = self.start.elapsed().as_micros() as u64;
                                self.recovery_tokens = 0.0;
                            }
                        }
                        // Recovery complete once the frontier reaches the target
                        // captured at the dead->alive transition (the whole
                        // pre-outage backlog re-delivered). Record the interval.
                        if self.recovery_target != 0 && fb.ack_through >= self.recovery_target {
                            self.recovery_interval_us = (self.start.elapsed().as_micros() as u64)
                                .saturating_sub(self.recovery_started_us);
                            self.recovery_target = 0;
                        }
                        // BBR passive path model: pop the send times of every
                        // block this ACK delivered; the freshest (highest id)
                        // gives the round-trip time, and the cumulative
                        // `ack_through` gives the delivered count. The model's
                        // own anchored sampling window guards against coalesced
                        // ACKs, so no send-span is needed here.
                        let now_us = self.start.elapsed().as_micros() as u64;
                        let mut rtt_us = 0u64;
                        let mut newest_send = 0u64;
                        while let Some(&(id, sent)) = self.block_send_us.front() {
                            if id < fb.ack_through {
                                newest_send = sent;
                                rtt_us = now_us.saturating_sub(sent);
                                self.block_send_us.pop_front();
                            } else {
                                break;
                            }
                        }
                        self.path_model
                            .on_ack(fb.ack_through as u64, now_us, rtt_us, newest_send);
                        // Fold the RTT into the shape fingerprint: a bimodal
                        // distribution is the signature of a Wi-Fi hop.
                        if rtt_us > 0 {
                            self.rtt_shape.observe(rtt_us as f64);
                        }
                        self.apply_fusion(&fb);
                    }
                }
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut
                        || e.kind() == io::ErrorKind::ConnectionReset
                        || e.kind() == io::ErrorKind::ConnectionRefused
                        || e.kind() == io::ErrorKind::HostUnreachable
                        || e.kind() == io::ErrorKind::NetworkUnreachable =>
                {
                    // A pending ICMP error the kernel surfaces on a regular recv
                    // because IP_RECVERR is on: a port-unreachable (peer not up -
                    // ConnectionReset on Windows, ConnectionRefused on Linux/BSD)
                    // or a TTL-expired-in-transit from our own item-14 Trace
                    // probes (HostUnreachable). None is a real connection
                    // failure; the error queue is drained separately for the
                    // trace, so ignore it here rather than kill the transfer.
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        self.drain_recovery()?;
        self.check_liveness()?;
        Ok(())
    }

    /// Meter the proactive-recovery resend at the item-6 BtlBw rate (a token
    /// bucket): send as many queued gap datagrams as the accrued byte budget
    /// allows, so the whole gap refills the pipe at the bottleneck rate -
    /// far faster than reactive one-block-per-round-trip NAK recovery, yet
    /// without the buffer overflow an unpaced dump caused. While draining (and
    /// for a few round trips after) it arms the pacer grace, so the queue this
    /// adds is not mistaken for steady-state bloat.
    fn drain_recovery(&mut self) -> io::Result<()> {
        if self.recovery_dgrams.is_empty() {
            return Ok(());
        }
        let now_us = self.start.elapsed().as_micros() as u64;
        let elapsed = now_us.saturating_sub(self.last_recovery_us);
        self.last_recovery_us = now_us;
        let rate_bytes = (self.path_model.btlbw_bps() / 8).max(MIN_RECOVERY_BYTES_PER_S) as f64;
        self.recovery_tokens += rate_bytes * elapsed as f64 / 1_000_000.0;
        if self.recovery_tokens > RECOVERY_BUCKET_BYTES {
            self.recovery_tokens = RECOVERY_BUCKET_BYTES;
        }
        while let Some(front) = self.recovery_dgrams.front() {
            let size = front.len() as f64;
            if self.recovery_tokens < size {
                break;
            }
            self.recovery_tokens -= size;
            let dgram = self.recovery_dgrams.pop_front().expect("front exists");
            self.send(&dgram)?;
        }
        // Hold the pacer through the resend and a few round trips after, so the
        // recovery's transient queue clears before normal control resumes.
        let grace = RECOVERY_GRACE_RTTS * self.path_model.rtt_now_us().max(MIN_PACE_INTERVAL_US);
        self.recovery_grace_until_us = now_us + grace;
        Ok(())
    }

    /// Declare the link dead after a PTO of total feedback silence, and while
    /// dead send a periodic probe - a retransmit of the oldest unacked block -
    /// which both elicits feedback (so recovery is noticed regardless of the
    /// receiver's own cadence) and pre-positions the block the receiver's
    /// frontier is stalled on. New data is already held by flow-control
    /// backpressure (the window cannot advance with no ACKs), so this is the
    /// only traffic the dead state adds beyond the cheap heartbeat.
    fn check_liveness(&mut self) -> io::Result<()> {
        let silence_us = self.last_feedback_at.elapsed().as_micros() as u64;
        let dead_timeout =
            (DEAD_RTT_MULTIPLE * self.path_model.rtt_now_us()).max(DEAD_FLOOR_US);
        if silence_us <= dead_timeout {
            return Ok(());
        }
        if !self.link_dead {
            self.link_dead = true;
            self.dead_episodes += 1;
        }
        // Probe at the dead-timeout cadence while the link stays dark.
        if self.last_probe_at.elapsed().as_micros() as u64 >= dead_timeout
            && let Some(oldest) = self.enc.oldest_pending()
        {
            let probe = self.enc.probe_block(oldest);
            if !probe.is_empty() {
                self.send_batch(&probe)?;
                self.probes_sent += 1;
            }
            self.last_probe_at = Instant::now();
        }
        Ok(())
    }

    /// Emit a heartbeat (timestamp + ring-shape digest) if the interval
    /// has elapsed. The timestamp lets the receiver measure the OWD
    /// trend; the digest lets it forecast demand.
    fn maybe_send_heartbeat(&mut self) -> io::Result<()> {
        if self.last_hb.elapsed() >= HEARTBEAT_INTERVAL {
            let mut cp = ControlPacket::new();
            // The clock beat: drives the receiver's OWD-trend slope and jitter.
            cp.timing = Some(TimingFrame {
                send_ts: self.start.elapsed().as_micros() as u64,
                echo_ts: 0,
            });
            // Source-ring shape (the legacy heartbeat payload, now a frame).
            // Backlog proxy: in-flight blocks (the real AdaptiveIpc integration
            // reads the source ring's fill instead).
            cp.ring = Some(RingFrame {
                fill_pct: self.enc.in_flight().min(255) as u8,
                ring_kind: 0,
                producers: 1,
                consumers: 1,
                trend: 1,
                flags: 0,
            });
            // Bidirectional loss accounting: our heartbeat-send count and how
            // many feedback packets we have received, so the receiver can tell
            // its feedback is reaching us (and shorten its cadence if not).
            self.ctrl_out = self.ctrl_out.wrapping_add(1);
            cp.loss_acct = Some(LossAcctFrame {
                seq: self.ctrl_out,
                last_recv_seq: self.ctrl_recv,
            });
            // Our link class + quality, so the peer knows what kind of link
            // (Wi-Fi / wired / cellular) carries this end of the path. The class
            // falls back to the RTT-shape fingerprint when the OS read is
            // unavailable.
            cp.link = Some(LinkFrame {
                class: self.inferred_link_class().as_u8(),
                quality: self.link_quality,
            });
            // Our egress path MTU, so the peer can track a handoff on this end.
            if let Some(pm) = self.net_events.pmtu() {
                cp.pmtu = Some(PmtuFrame { pmtu: pm });
            }
            let buf = encode_control(&cp);
            self.send(&buf)?;
            self.last_hb = Instant::now();
        }
        Ok(())
    }

    /// Emit one WBest probe round (item 13): `BW_PROBE_PAIRS` back-to-back packet
    /// pairs (stage 1, effective capacity) followed by a `BW_PROBE_TRAIN`-packet
    /// train (stage 2, available bandwidth). Every probe is a control datagram
    /// padded to `BW_PROBE_BYTES` carrying a single `BwProbe` frame stamped with
    /// the round id and its index; the receiver measures the dispersions and
    /// reports the estimate back. Sent as one burst so the bottleneck serializes
    /// the packets, which is what the dispersion measures.
    fn maybe_send_bw_probe(&mut self) -> io::Result<()> {
        if self.last_bw_probe.elapsed() < BW_PROBE_INTERVAL {
            return Ok(());
        }
        let round = self.bw_probe_round;
        self.bw_probe_round = self.bw_probe_round.wrapping_add(1);
        let total = 2 * BW_PROBE_PAIRS + BW_PROBE_TRAIN;
        for idx in 0..total {
            let mut cp = ControlPacket::new();
            cp.bw_probe.push(crate::control_frame::BwProbeFrame {
                probe_id: round,
                idx,
                send_ts: self.start.elapsed().as_micros() as u64,
            });
            let mut buf = encode_control(&cp);
            crate::control_frame::pad_control_to(&mut buf, BW_PROBE_BYTES);
            self.send(&buf)?;
        }
        self.last_bw_probe = Instant::now();
        Ok(())
    }

    /// The receiver's most recent WBest report: (available bandwidth, effective
    /// capacity) in bits/s, both 0 until the first report lands. The sender
    /// cross-checks the capacity against its passive [`btlbw_bps`](Self::btlbw_bps).
    pub fn avail_bw_bps(&self) -> (u64, u64) {
        (self.avail_bw_kbps * 1000, self.wbest_capacity_kbps * 1000)
    }

    /// Emit one Trace sweep (item 14): a probe at each IP TTL 1..=`MAX_TRACE_HOPS`,
    /// stamping the per-TTL send time, then drain whatever ICMP TimeExceeded
    /// replies have arrived. Linux only (the error queue is an `IP_RECVERR`
    /// capability); a no-op elsewhere.
    #[cfg(target_os = "linux")]
    fn maybe_send_trace(&mut self) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        // Trace (the IP_RECVERR error queue) needs the kernel fd; the demux path
        // has none, so trace is simply off there (a sensor, not correctness).
        let Some(fd) = self.sock.as_udp().map(|u| u.as_raw_fd()) else {
            return Ok(());
        };
        if self.last_trace.elapsed() >= TRACE_INTERVAL {
            self.trace_round = self.trace_round.wrapping_add(1);
            let now = self.start.elapsed().as_micros() as u64;
            for ttl in 1..=MAX_TRACE_HOPS {
                let mut cp = ControlPacket::new();
                cp.trace.push(crate::control_frame::TraceFrame {
                    hop_ttl: ttl,
                    probe_id: self.trace_round,
                });
                let buf = encode_control(&cp);
                // A probe send may surface a prior probe's latched ICMP error
                // (IP_RECVERR); that is the trace working, not a failure, so a
                // send error here just means this probe is skipped this round.
                crate::trace_sensor::send_at_ttl(fd, self.trace_peer, &buf, ttl)
                    .ok();
                self.trace_send_us[ttl as usize] = now;
            }
            self.last_trace = Instant::now();
        }
        let now = self.start.elapsed().as_micros() as u64;
        for (router, payload) in crate::trace_sensor::drain_icmp_errors(fd) {
            // The expired probe's payload is echoed back; its Trace frame's TTL
            // is the hop index, and `now - send_time[ttl]` is the per-hop RTT.
            if let Some(cp) = decode_control(&payload)
                && let Some(tf) = cp.trace.first()
            {
                let ttl = tf.hop_ttl;
                let sent = self.trace_send_us.get(ttl as usize).copied().unwrap_or(0);
                let rtt_us = now.saturating_sub(sent);
                if !self.trace_hops.iter().any(|h| h.ttl == ttl) {
                    self.trace_hops.push(crate::trace_sensor::TraceHop {
                        ttl,
                        addr: router,
                        rtt_us,
                    });
                    self.trace_hops.sort_by_key(|h| h.ttl);
                }
            }
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn maybe_send_trace(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// The hops the Trace sweep discovered toward the peer (item 14): each is a
    /// `(ttl, router address, RTT)` from an ICMP TimeExceeded.
    pub fn trace_hops(&self) -> &[crate::trace_sensor::TraceHop] {
        &self.trace_hops
    }

    /// Forward / reverse path hop counts and their asymmetry (item 14), or `None`
    /// for a direction not yet observed.
    pub fn path_asymmetry(&self) -> (Option<u8>, Option<u8>, Option<u8>) {
        (self.asym.forward(), self.asym.reverse(), self.asym.asymmetry())
    }

    /// The graded AccECN CE rate (item 15): the fraction of our ECN-capable
    /// packets the AQM marked CE, `delta_CE / delta_ECT` from the peer's counts.
    pub fn ce_rate(&self) -> f32 {
        self.ce_rate
    }

    /// The peer's Sprout forecast (item 16): the 5th-percentile next-tick
    /// deliverable rate (bits/s), 0 until the first forecast arrives. Drives the
    /// predictive window cap and leads a dip down.
    pub fn forecast_bps(&self) -> u64 {
        self.forecast_bps * 8
    }

    /// The LEO pre-arm path-shift (item 17): the detection confidence when a
    /// confident handover cadence's next spike is within the pre-arm window,
    /// else 0 - so protection arms one cycle ahead of the spike.
    fn leo_prearm_shift(&self) -> f32 {
        const LEO_PRE_ARM_WINDOW_S: f32 = 2.0;
        if self.leo_conf >= 0.4
            && self.leo_period_s > 0.0
            && self.leo_secs_to_spike <= LEO_PRE_ARM_WINDOW_S
        {
            self.leo_conf
        } else {
            0.0
        }
    }

    /// The peer's detected LEO handover cadence (item 17): `(period_s,
    /// confidence, secs_to_next_spike)`. `period_s == 0` means none detected.
    pub fn leo_cadence(&self) -> (f32, f32, f32) {
        (self.leo_period_s, self.leo_conf, self.leo_secs_to_spike)
    }

    /// Poll the platform link sensor on the slow cadence and cache its
    /// stress reading for the fusion controller.
    fn maybe_sample_link(&mut self) {
        if self.last_link_sample.elapsed() >= LINK_SAMPLE_INTERVAL {
            let snap = self.link_sensor.sample();
            self.link_stress = snap.link_stress();
            // A class change (a handoff) is a path event: spike the shift so the
            // controller pre-arms, the same way a hop-count change does. Skip the
            // first reading (Unknown -> something is not a handoff).
            if self.link_class != LinkClass::Unknown && self.link_class != snap.class {
                self.class_shift = 1.0;
            }
            self.link_class = snap.class;
            self.link_quality = snap
                .signal_quality
                .unwrap_or(((1.0 - self.link_stress) * 100.0) as u8);
            // First-hop PHY rate + MCS for mesh-hop detection.
            self.link_phy_kbps = snap.phy_rate_kbps.unwrap_or(0);
            self.link_mcs_norm = snap.mcs_norm.unwrap_or(0.0);
            self.last_link_sample = Instant::now();
        }
        // Decay the class-shift and the peer-MTU-drop shift each poll so the
        // handoff pre-arms fade (the net-event shift decays on its own clock).
        self.class_shift *= 0.9;
        self.peer_pmtu_shift *= 0.9;
    }

    /// Run the fusion controller on the receiver's reported sensors plus
    /// the local link sensor, and publish the resulting coding knobs into
    /// the control table and the encoder. This is where the adaptive loop
    /// closes on the sender.
    fn apply_fusion(&mut self, fb: &crate::reliable_udp::Feedback) {
        // Fold the peer's loss-class report into the congestion-share EWMA, but
        // only on a feedback that carries loss (code 0 = no loss holds the
        // share, so it reflects the last loss regime when loss resumes).
        // 2 = congestion -> 1.0, 3 = mixed -> 0.5, 1 = wireless -> 0.0.
        if fb.loss_class != 0 {
            let contribution = match fb.loss_class {
                2 => 1.0,
                3 => 0.5,
                _ => 0.0,
            };
            self.congestion_fraction += (contribution - self.congestion_fraction) * 0.125;
        }
        // The event-driven path shift (OS observer or peer-MTU-drop), captured
        // once so its transient peak is held for end-of-run telemetry even as
        // the live value decays.
        let event_shift = self.net_events.path_shift().max(self.peer_pmtu_shift);
        self.net_event_shift_peak = self.net_event_shift_peak.max(event_shift);
        let snap = SensorSnapshot {
            loss: fb.loss_x255 as f32 / 255.0,
            burstiness: fb.burstiness_x255 as f32 / 255.0,
            owd_trend: match fb.owd_trend_class {
                2 => 0.1,
                0 => -0.1,
                _ => 0.0,
            },
            link_stress: self.link_stress,
            // A path shift from any of four feed-forward sources: the passive
            // hop-count change (`path_sensor`), a link-class handoff
            // (`class_shift`), an OS-announced route / carrier / MTU event
            // (`net_events`, ahead of any loss), or a peer-side MTU drop
            // (`peer_pmtu_shift`). The strongest wins.
            path_shift: self
                .path_sensor
                .path_shift()
                .max(self.class_shift)
                .max(event_shift)
                // LEO pre-arm (item 17): when the peer has detected a confident
                // handover cadence and its next spike is within the pre-arm
                // window, spike the path shift NOW - one cycle ahead of the delay
                // spike, so protection is armed before the handover lands.
                .max(self.leo_prearm_shift()),
            // AccECN graded CE rate (item 15) when the peer reports counters;
            // the path-sensor's single-CE-bit reading is the floor so a first CE
            // still registers before the rate has accumulated.
            ecn_ce: self.ce_rate.max(self.path_sensor.ecn_ce()),
            congestion_fraction: self.congestion_fraction,
            rev_loss: self.rev_loss,
            // Self-induced queue delay from the BBR path model (item 6 RTprop):
            // RTT_now - RTprop, the bufferbloat signal.
            queue_delay_ms: self.path_model.queue_delay_us() as f32 / 1000.0,
            // Wi-Fi backhaul-hop estimate (item 5 first-hop PHY vs item 6 BtlBw):
            // more hops bias parity up.
            backhaul_hops: self.backhaul_hops(),
        };
        self.last_fwd_loss = snap.loss;
        let d = self.fusion.decide(&snap);
        self.control.set_level(d.level);
        self.control.set_parity_r(d.parity_r);
        self.control.set_interleave_depth(d.interleave_depth);
        // Provision parity to actually COVER the measured loss for this block's k
        // (r/(k+r) >= loss), with the controller's decision as the floor - so a
        // high-loss block recovers in-FEC up to the bitmap ceiling instead of
        // falling to ARQ round trips at the old fixed parity<=6.
        self.enc.set_parity_covering(d.parity_r as usize, snap.loss);
        self.pace_flow_window(snap.queue_delay_ms);
    }

    /// LEDBAT delay-based pacer (RFC 6817): hold the self-induced queue near
    /// [`PACE_TARGET_MS`] by nudging the flow window once per round trip in
    /// proportion to how far the measured queue delay is from target. When the
    /// queue is deeper than target the window shrinks (drain); when it is
    /// shallower it grows (probe), each step bounded so one round trip never
    /// cuts the window by more than half. This settles the window at the size
    /// that keeps the bottleneck busy with about one target's worth of queue,
    /// rather than the binary snap-to-BDP / snap-to-full that oscillated.
    ///
    /// On a clean link the queue delay is ~0, so `off_target` stays positive
    /// and the window holds at its full configured value - the pacer only ever
    /// engages once WE are the ones filling a buffer.
    fn pace_flow_window(&mut self, queue_delay_ms: f32) {
        if !self.pacing_enabled {
            return;
        }
        // Recovery grace: while a proactive resend is in flight (or for a few
        // round trips after), the queue is an expected, intentional transient -
        // not steady-state bloat - so hold the window rather than clamp it. This
        // is what lets the recovery refill the pipe without the pacer then
        // throttling the very window it restored.
        let now = self.start.elapsed().as_micros() as u64;
        if !self.recovery_dgrams.is_empty() || now < self.recovery_grace_until_us {
            return;
        }
        // The queue responds one CURRENT round trip after a window change (the
        // inflated RTT under load, not the bloat-free RTprop), so adjust at most
        // once per smoothed RTT - adjusting faster than the feedback loop closes
        // over-corrects and oscillates. Fall back to a 1 ms floor before the
        // first RTT sample lands.
        let interval = self.path_model.rtt_now_us().max(MIN_PACE_INTERVAL_US);
        if now < self.last_pace_us + interval {
            return;
        }
        self.last_pace_us = now;
        // off_target: +1 when the queue is empty, 0 at target, negative when the
        // queue is deeper than target. The step is clamped so a single round
        // trip never removes more than half the window.
        let off_target = (PACE_TARGET_MS - queue_delay_ms) / PACE_TARGET_MS;
        let w = self.paced_window;
        let step = off_target.clamp(-0.5 * w, w);
        self.paced_window = (w + step).clamp(MIN_PACED_WINDOW as f32, self.flow_window_max as f32);
        let mut target = self.paced_window.round() as u32;
        // Item 16 predictive cap: when the Sprout forecast (the conservative
        // next-tick deliverable rate) falls well below the historical BtlBw, a
        // dip is coming - scale the window down NOW, before the queue (and the
        // loss) the dip would cause builds. The LEDBAT step above only reacts
        // after the queue has formed; this leads it. The `FORECAST_HEADROOM`
        // factor leaves room to send ABOVE the forecast, so the sender keeps
        // probing the link and the forecast can climb back after a dip - without
        // it the cap is self-reinforcing (the send rate collapses to the forecast,
        // so the arrivals the forecast is built from never reveal a faster link).
        let btlbw = self.path_model.btlbw_bps();
        if self.forecast_bps > 0 && btlbw > 0 {
            let ratio =
                (self.forecast_bps as f64 * FORECAST_HEADROOM / btlbw as f64).clamp(0.1, 1.0);
            let cap = ((self.flow_window_max as f64 * ratio).ceil() as u32).max(MIN_PACED_WINDOW);
            target = target.min(cap);
        }
        if target != self.enc.flow_window() {
            self.enc.set_flow_window(target);
        }
    }

    fn send(&self, pkt: &[u8]) -> io::Result<()> {
        let mut spins = 0u32;
        loop {
            match self.sock.send(pkt) {
                Ok(_) => return Ok(()),
                // Send buffer full = the link is saturated. PACE: wait for
                // buffer space instead of dropping. Dropping here
                // manufactures loss and lets the sender outrun the link,
                // so FEC/ARQ then has to recover the sender's OWN datagrams
                // - a throughput collapse, not a wire loss.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    spins += 1;
                    if spins > 20_000 {
                        // ~1s saturated: the peer is likely gone; let
                        // ARQ / FEC cope rather than spin forever.
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_micros(50));
                }
                // A pending ICMP error the connected socket surfaces on send:
                // a reset / refused (peer not up), or a TTL-expired-in-transit
                // that IP_RECVERR latched from our own item-14 Trace probes
                // (HostUnreachable / NetworkUnreachable). None is a real send
                // failure; drop this datagram and let ARQ / FEC recover.
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionRefused
                            | io::ErrorKind::HostUnreachable
                            | io::ErrorKind::NetworkUnreachable
                    ) =>
                {
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Send a whole block's datagrams. On Linux this uses UDP GSO
    /// (`UDP_SEGMENT`): the same-size datagrams concatenate into ONE buffer
    /// the kernel segments into many wire datagrams, so a block costs one
    /// `sendmsg` and one skb instead of `k+r` skbs - the clean-link
    /// throughput lever QUIC uses. The kernel splits on the wire, so the
    /// receiver is unchanged. On Windows it is USO (`WSASendMsg` with the
    /// `UDP_SEND_MSG_SIZE` control message), the same one-buffer/kernel-
    /// segments model. On FreeBSD it is one `sendmmsg` per batch; elsewhere
    /// one `send` per datagram. Falls back to per-datagram sends if the
    /// kernel lacks segmentation offload. Pacing and ICMP-reset handling
    /// match [`send`](Self::send).
    fn send_batch(&self, pkts: &[Vec<u8>]) -> io::Result<()> {
        if pkts.is_empty() {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            self.send_gso(pkts)
        }
        #[cfg(target_os = "freebsd")]
        {
            self.send_mmsg(pkts)
        }
        #[cfg(target_os = "windows")]
        {
            // `SUBETHA_USO=0` forces the per-datagram path - the A/B baseline
            // for measuring the USO segmentation win in one harness.
            if uso_enabled() {
                self.send_uso(pkts)
            } else {
                for pkt in pkts {
                    self.send(pkt)?;
                }
                Ok(())
            }
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "windows"
        )))]
        {
            for pkt in pkts {
                self.send(pkt)?;
            }
            Ok(())
        }
    }

    /// UDP GSO egress (Linux). Groups consecutive same-size datagrams (GSO
    /// requires a uniform segment size) into one buffer of up to 64
    /// segments / 60 KiB and sends each group with a `UDP_SEGMENT` control
    /// message; the kernel segments it into individual wire datagrams. A
    /// lone datagram takes the plain paced `send`. If the kernel rejects
    /// GSO, the rest of the batch falls back to `sendmmsg`.
    #[cfg(target_os = "linux")]
    fn send_gso(&self, pkts: &[Vec<u8>]) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        // The demux path has no kernel fd for GSO; send each datagram plainly.
        let fd = match self.sock.as_udp() {
            Some(u) => u.as_raw_fd(),
            None => {
                for p in pkts {
                    self.sock.send(p)?;
                }
                return Ok(());
            }
        };
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1500);
        let mut i = 0usize;
        while i < pkts.len() {
            let seg = pkts[i].len();
            buf.clear();
            let mut j = i;
            while j < pkts.len()
                && pkts[j].len() == seg
                && (j - i) < 64
                && buf.len() + seg <= 61440
            {
                buf.extend_from_slice(&pkts[j]);
                j += 1;
            }
            if j - i <= 1 || seg == 0 || seg > u16::MAX as usize {
                self.send(&pkts[i])?;
                i += 1;
                continue;
            }
            if !self.send_gso_buf(fd, &buf, seg as u16)? {
                // Kernel lacks GSO: send the remaining datagrams plainly.
                return self.send_mmsg(&pkts[i..]);
            }
            i = j;
        }
        Ok(())
    }

    /// One `sendmsg` with a `UDP_SEGMENT` control message. `Ok(true)` if
    /// sent (or paced through), `Ok(false)` if the kernel rejected GSO so
    /// the caller can fall back.
    #[cfg(target_os = "linux")]
    fn send_gso_buf(&self, fd: libc::c_int, buf: &[u8], seg_size: u16) -> io::Result<bool> {
        const UDP_SEGMENT: libc::c_int = 103;
        let mut iov = libc::iovec {
            iov_base: buf.as_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        let mut cmsg_space = [0u64; 8]; // 64 B, 8-byte aligned for cmsghdr
        // SAFETY: a zeroed msghdr with one iovec and a single UDP_SEGMENT
        // cmsg of a `u16`; `iov`/`buf`/`cmsg_space` outlive the sendmsg, and
        // CMSG_SPACE(2) <= 64 B so the cmsg fits.
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_space.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = unsafe { libc::CMSG_SPACE(size_of::<u16>() as u32) } as _;
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_UDP;
            (*cmsg).cmsg_type = UDP_SEGMENT;
            (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<u16>() as u32) as _;
            std::ptr::write_unaligned(libc::CMSG_DATA(cmsg) as *mut u16, seg_size);
        }
        let mut spins = 0u32;
        loop {
            // SAFETY: msg points at the live iov + cmsg; fd is the connected
            // socket.
            let n = unsafe { libc::sendmsg(fd, &msg, 0) };
            if n >= 0 {
                return Ok(true);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                // ENOPROTOOPT/EOPNOTSUPP/EINVAL: the kernel does not offer GSO.
                // EIO: the kernel offers it but the NIC cannot segment - a virtio
                // device with `tx-udp-segmentation` fixed-off returns EIO at send
                // time. Both mean "fall back to plain sendmmsg" (the RLC path
                // handles the same EIO in flush_gso).
                Some(libc::ENOPROTOOPT)
                | Some(libc::EOPNOTSUPP)
                | Some(libc::EINVAL)
                | Some(libc::EIO) => {
                    return Ok(false);
                }
                _ => match err.kind() {
                    io::ErrorKind::WouldBlock => {
                        spins += 1;
                        if spins > 20_000 {
                            return Ok(true);
                        }
                        std::thread::sleep(Duration::from_micros(50));
                    }
                    io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::HostUnreachable
                    | io::ErrorKind::NetworkUnreachable => {
                        // A pending ICMP error (peer not up, or a TTL-expired
                        // from our item-14 Trace probes via IP_RECVERR); drop and
                        // let ARQ / FEC recover, as for the per-datagram send.
                        return Ok(true);
                    }
                    _ => return Err(err),
                },
            }
        }
    }

    /// UDP USO egress (Windows). The Windows analogue of GSO: groups
    /// consecutive same-size datagrams into one buffer of up to 64 segments
    /// / 60 KiB and hands each group to `WSASendMsg` with a
    /// `UDP_SEND_MSG_SIZE` control message; the kernel segments it into
    /// individual wire datagrams (one path through the stack instead of
    /// `k+r`). A lone datagram takes the plain paced `send`. If the kernel
    /// rejects USO, the rest of the batch falls back to per-datagram sends.
    #[cfg(target_os = "windows")]
    fn send_uso(&self, pkts: &[Vec<u8>]) -> io::Result<()> {
        use std::os::windows::io::AsRawSocket;
        // The demux path has no kernel socket handle for USO; send plainly.
        if self.sock.as_udp().is_none() {
            for p in pkts {
                self.sock.send(p)?;
            }
            return Ok(());
        }
        let sock = self.sock.as_udp().expect("Udp checked above").as_raw_socket() as usize;
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1500);
        let mut i = 0usize;
        while i < pkts.len() {
            let seg = pkts[i].len();
            buf.clear();
            let mut j = i;
            while j < pkts.len()
                && pkts[j].len() == seg
                && (j - i) < 64
                && buf.len() + seg <= 61440
            {
                buf.extend_from_slice(&pkts[j]);
                j += 1;
            }
            if j - i <= 1 || seg == 0 || seg > u16::MAX as usize {
                self.send(&pkts[i])?;
                i += 1;
                continue;
            }
            if !self.send_uso_buf(sock, &buf, seg as u32)? {
                // Kernel lacks USO: send the remaining datagrams plainly.
                for pkt in &pkts[i..] {
                    self.send(pkt)?;
                }
                return Ok(());
            }
            i = j;
        }
        Ok(())
    }

    /// One `WSASendMsg` with a `UDP_SEND_MSG_SIZE` control message. `Ok(true)`
    /// if sent (or paced through), `Ok(false)` if the kernel rejected USO so
    /// the caller can fall back. Pacing and ICMP-reset handling match
    /// [`send`](Self::send).
    #[cfg(target_os = "windows")]
    fn send_uso_buf(&self, sock: usize, buf: &[u8], seg_size: u32) -> io::Result<bool> {
        use windows_sys::Win32::Networking::WinSock::{WSAGetLastError, WSABUF, WSAMSG};
        // WSASendMsg is an extension function; load (and cache) its pointer.
        // A failed load means USO is unavailable: fall back.
        let Some(wsasendmsg) = load_wsasendmsg(sock) else {
            return Ok(false);
        };
        // Stable Windows ABI values, declared locally so the cmsg layout is
        // explicit and independent of windows-sys constant typing.
        const IPPROTO_UDP: i32 = 17;
        const UDP_SEND_MSG_SIZE: i32 = 2;
        const SOCKET_ERROR: i32 = -1;
        const WSAEINVAL: i32 = 10022;
        const WSAEWOULDBLOCK: i32 = 10035;
        const WSAEMSGSIZE: i32 = 10040;
        const WSAENOPROTOOPT: i32 = 10042;
        const WSAECONNRESET: i32 = 10054;
        const WSAECONNREFUSED: i32 = 10061;

        let mut data = WSABUF {
            len: buf.len() as u32,
            buf: buf.as_ptr() as *mut u8,
        };
        // Control buffer holds one WSACMSGHDR + a u32 segment size.
        // 64-bit layout: cmsg_len (usize) @0, cmsg_level (i32) @8,
        // cmsg_type (i32) @12, WSA_CMSG_DATA @16. WSA_CMSG_LEN(4) = 20,
        // WSA_CMSG_SPACE(4) = 24. `[u64; 4]` gives 32 B, 8-byte aligned.
        let mut ctrl = [0u64; 4];
        let cp = ctrl.as_mut_ptr() as *mut u8;
        // SAFETY: cp points at 32 B of 8-aligned scratch; the four writes
        // land at offsets 0/8/12/16, all within bounds, matching the
        // WSACMSGHDR layout plus its data word.
        unsafe {
            std::ptr::write_unaligned(cp as *mut usize, 20usize);
            std::ptr::write_unaligned(cp.add(8) as *mut i32, IPPROTO_UDP);
            std::ptr::write_unaligned(cp.add(12) as *mut i32, UDP_SEND_MSG_SIZE);
            std::ptr::write_unaligned(cp.add(16) as *mut u32, seg_size);
        }
        let msg = WSAMSG {
            name: std::ptr::null_mut(),
            namelen: 0,
            lpBuffers: &mut data,
            dwBufferCount: 1,
            Control: WSABUF { len: 24, buf: cp },
            dwFlags: 0,
        };
        let mut sent = 0u32;
        let mut spins = 0u32;
        loop {
            // SAFETY: msg points at the live data/ctrl buffers, which outlive
            // the call; sock is the connected socket handle; no overlapped
            // structure or completion routine.
            let rc = unsafe {
                wsasendmsg(
                    sock,
                    &msg,
                    0,
                    &mut sent,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                )
            };
            if rc != SOCKET_ERROR {
                USO_OFFLOAD.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(true);
            }
            // SAFETY: plain thread-local error fetch, no preconditions.
            let err = unsafe { WSAGetLastError() };
            match err {
                // Kernel lacks USO (or rejected the concatenated buffer):
                // signal the caller to fall back to per-datagram sends.
                WSAEINVAL | WSAENOPROTOOPT | WSAEMSGSIZE => {
                    USO_FALLBACK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(false);
                }
                // Send buffer full: PACE rather than drop (see `send`).
                WSAEWOULDBLOCK => {
                    spins += 1;
                    if spins > 20_000 {
                        return Ok(true);
                    }
                    std::thread::sleep(Duration::from_micros(50));
                }
                // ICMP-driven reset / refused: drop and let ARQ / FEC recover.
                WSAECONNRESET | WSAECONNREFUSED => return Ok(true),
                _ => return Err(io::Error::from_raw_os_error(err)),
            }
        }
    }

    /// One-`sendmmsg`-per-batch egress (Linux/FreeBSD). The socket is
    /// connected, so each datagram needs only its iovec, no destination.
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    fn send_mmsg(&self, pkts: &[Vec<u8>]) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        // The demux path has no kernel fd for sendmmsg; send each datagram plainly.
        let fd = match self.sock.as_udp() {
            Some(u) => u.as_raw_fd(),
            None => {
                for p in pkts {
                    self.sock.send(p)?;
                }
                return Ok(());
            }
        };
        let mut iovecs: Vec<libc::iovec> = pkts
            .iter()
            .map(|p| libc::iovec {
                iov_base: p.as_ptr() as *mut libc::c_void,
                iov_len: p.len(),
            })
            .collect();
        let mut msgs: Vec<libc::mmsghdr> = Vec::with_capacity(pkts.len());
        for i in 0..pkts.len() {
            // SAFETY: a zeroed mmsghdr with only msg_iov / msg_iovlen set
            // is a valid scatter-gather send descriptor on a connected
            // socket; the iovec it points at lives in `iovecs` for the
            // whole call.
            let mut hdr: libc::mmsghdr = unsafe { std::mem::zeroed() };
            hdr.msg_hdr.msg_iov = iovecs.as_mut_ptr().wrapping_add(i);
            hdr.msg_hdr.msg_iovlen = 1 as _;
            msgs.push(hdr);
        }
        let mut sent = 0usize;
        let mut spins = 0u32;
        while sent < msgs.len() {
            let count = (msgs.len() - sent) as MmsgLen;
            // SAFETY: msgs[sent..] is `count` valid mmsghdrs whose iovecs
            // reference the live `pkts` buffers; fd is the connected socket.
            let n = unsafe { libc::sendmmsg(fd, msgs.as_mut_ptr().add(sent), count, 0) };
            if n > 0 {
                sent += n as usize;
                spins = 0;
                continue;
            }
            let err = io::Error::last_os_error();
            match err.kind() {
                // Send buffer full: PACE rather than drop (see `send`).
                io::ErrorKind::WouldBlock => {
                    spins += 1;
                    if spins > 20_000 {
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_micros(50));
                }
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionRefused => {
                    return Ok(());
                }
                _ => return Err(err),
            }
        }
        Ok(())
    }
}

/// Receiver half of the reliable-UDP bridge.
pub struct ReliableUdpReceiver {
    sock: crate::dgram::DgramSock,
    dec: Decoder,
    peer: Option<SocketAddr>,
    /// Count of datagrams actually read off the socket (telemetry; lets
    /// a caller distinguish "no packets arriving" from "packets arrive
    /// but do not decode/deliver").
    recv_count: u64,
    /// Per-block time of last NAK, to rate-limit re-requests of each gap
    /// to ~one per RTT while still NAKing every gap in parallel. Pruned
    /// below the delivery frontier each cycle.
    nak_history: BTreeMap<u32, Instant>,
    /// When the last plain ACK feedback packet was sent, to rate-limit ACKs.
    last_feedback: Instant,
    /// Bidirectional control-plane loss accounting. `ctrl_out` counts feedback
    /// control packets sent, `ctrl_recv` counts heartbeat control packets
    /// received, and `peer_acked` is the highest `last_recv_seq` the sender has
    /// reported (how many of our feedback packets it received). When our
    /// feedback is being lost (`ctrl_out` outruns `peer_acked`) the ACK cadence
    /// shortens, so a lost feedback packet does not stall ARQ.
    ctrl_out: u32,
    ctrl_recv: u32,
    peer_acked: u32,
    /// `ctrl_out` / `peer_acked` snapshots at the previous heartbeat, so the
    /// feedback-loss estimate is a WINDOWED rate (advance of each between
    /// heartbeats) rather than a cumulative count - the latter is dominated by
    /// the in-flight backlog, which grows with link delay.
    ctrl_out_at_last_hb: u32,
    peer_acked_at_last_hb: u32,
    /// Last computed reverse-path (feedback) loss fraction (diagnostics).
    fb_loss_est: f32,
    /// WBest available-bandwidth estimator (item 13): measures the dispersion of
    /// the sender's probe pairs / train and computes the available bandwidth,
    /// reported back in the feedback so the sender can cross-check its passive
    /// BtlBw. `wbest_round` is the probe round it is accumulating; a new round id
    /// resets it. `wbest_*_kbps` are the latest computed estimate for telemetry.
    wbest: crate::wbest_sensor::WBestEstimator,
    wbest_round: Option<u8>,
    wbest_avail_kbps: u64,
    wbest_capacity_kbps: u64,
    /// The peer's link class / quality, from the `Link` frame it echoes - so
    /// this end knows what kind of link (Wi-Fi / wired / cellular) carries the
    /// other end of the path.
    peer_link_class: u8,
    peer_link_quality: u8,
    /// Current ACK cadence, shortened under reverse-path (feedback) loss.
    ack_interval: Duration,
    /// Test knob: drop this percent of OUTGOING feedback to inject reverse-path
    /// loss (the forward-path counterpart is `debug_drop_pct`). Zero normally.
    fb_drop_pct: u32,
    fb_drop_rng: u64,
    /// Test knob: artificial one-way delay on the feedback path, to
    /// reproduce a real link's recovery round-trip on loopback. Zero in
    /// normal operation. Feedback queues here and releases when due.
    fb_delay: Duration,
    fb_pending: VecDeque<(Instant, Vec<u8>)>,
    /// Max gaps NAK'd per poll cycle. The default re-requests every gap in
    /// parallel; setting 1 reproduces serial head-only recovery (one gap
    /// per round-trip) for A/B comparison.
    nak_batch: usize,
    /// Max time a gap (the head block) is held for recovery before it is
    /// skipped to unblock the stream. Long by default, so delivery is
    /// effectively reliable; a shorter value trades reliability for
    /// bounded latency.
    max_hold: Duration,
    /// The head block being waited on and when it became the head, for the
    /// hold-time deadline.
    head_block: u32,
    head_since: Instant,
    /// Monotonic clock origin for heartbeat receive timestamps.
    start: Instant,
    /// Diagnostic loss injection: drop this percent of received DATA
    /// datagrams before decoding, to validate FEC / ARQ on a lossless
    /// link (loopback). Zero in normal operation.
    debug_drop_pct: u32,
    drop_rng: u64,
    /// Diagnostic Gilbert-Elliott BURST loss (per-10000 transition probs): in
    /// the Bad state every datagram is dropped, `ge_loss_r/10000` returns to
    /// Good and `ge_loss_p/10000` enters Bad, giving a mean burst of
    /// `10000 / ge_loss_r`. A known bursty channel for the burst-model A/B.
    /// `ge_loss_r == 0` disables it.
    ge_loss_p: u32,
    ge_loss_r: u32,
    ge_bad: bool,
    /// Diagnostic WHOLE-block loss: drop every shard of any data block
    /// whose id is a multiple of this (0 = off). Such a block cannot be
    /// ARQ-recovered (its retransmits are dropped too), so it isolates
    /// tower recovery. Outer-parity blocks are never dropped.
    drop_block_mod: u32,
    /// Diagnostic loss BURST: drop every data datagram whose arrival index
    /// falls in `[burst_at, burst_at + burst_len)` - one concentrated loss
    /// event, to show a throughput blip and its full recovery in the trace.
    /// Zero length = off.
    burst_at: u64,
    burst_len: u64,
    /// Whether the receive socket is connected to the peer. Linux/FreeBSD
    /// (for recvmmsg / GRO) and Windows (for `WSARecvMsg`) connect after the
    /// first datagram; feedback then goes via `send()`, because BSD rejects
    /// `send_to()` on a connected UDP socket with EISCONN. A platform that
    /// stays unconnected keeps `send_to()`.
    connected: bool,
    /// Reused receive buffers for the batched `recvmmsg` path.
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    rbufs: Vec<Vec<u8>>,
    /// Whether `UDP_GRO` took on the socket (Linux). When set, the receiver
    /// reads coalesced super-buffers via `recvmsg` and splits them by the
    /// GRO segment size - the receive-side counterpart of GSO. When unset
    /// (old kernel) it keeps the per-datagram `recvmmsg` path.
    #[cfg(target_os = "linux")]
    gro_on: bool,
    /// 64 KiB buffer for one coalesced GRO read (Linux).
    #[cfg(target_os = "linux")]
    gro_buf: Vec<u8>,
    /// Most recent IP TTL observed on an inbound datagram (0 = none yet), read
    /// from the per-packet cmsg. Echoed to the sender in a `Path` frame so its
    /// controller sees hop-count shifts.
    last_ttl: u8,
    /// Most recent IP TOS byte observed (its low two bits are the ECN field).
    last_tos: u8,
    /// AccECN (item 15) cumulative counts of the peer's CE-marked and ECN-capable
    /// packets, echoed in the `Path` frame so the sender derives a graded CE rate
    /// from the deltas (an AQM marks CE before it tail-drops).
    ce_count: u64,
    ect_count: u64,
    /// Sprout-style forecast (item 16): the arrival-rate Kalman filter, the bytes
    /// received since the last forecast tick, and when that tick was. The
    /// 5th-percentile next-tick forecast is echoed to the sender in a `Forecast`
    /// frame so it pre-sizes ahead of a dip.
    forecast: crate::forecast_sensor::ArrivalForecast,
    fc_bytes: u64,
    fc_last: Instant,
    /// LEO handover-cadence detector (item 17): autocorrelates the heartbeat OWD
    /// trace for a periodic delay spike and reports the period + seconds-to-next
    /// in a `Periodicity` frame, so the sender pre-arms one cycle ahead.
    periodicity: crate::periodicity_sensor::PeriodicitySensor,
    /// Active OS path-event observer (this end's route / carrier / MTU
    /// watcher). Reports its egress MTU to the peer in a `Pmtu` frame; its
    /// event count is the proof a real path event fired on this host.
    net_events: NetEventObserver,
    /// The peer's last reported path MTU (from its `Pmtu` frame), 0 = none yet.
    peer_pmtu: u16,
    /// Peak path shift this end's active observer reached over the run, sampled
    /// as datagrams arrive. The instantaneous shift decays within seconds of
    /// the event, so this peak-hold is what makes a mid-transfer route / MTU
    /// event on this (receiver) end visible in the end-of-run telemetry.
    net_event_shift_peak: f32,
}

/// Enable `UDP_GRO` on a connected receive socket so the kernel coalesces
/// consecutive same-size datagrams into one `recvmsg`. Returns whether the
/// option took (false on kernels without GRO, where the caller keeps the
/// per-datagram `recvmmsg` path).
#[cfg(target_os = "linux")]
fn enable_gro(sock: &UdpSocket) -> bool {
    use std::os::fd::AsRawFd;
    const UDP_GRO: libc::c_int = 104;
    let on: libc::c_int = 1;
    // SAFETY: setsockopt on a valid fd with an int-sized option value that
    // outlives the call.
    let rc = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_UDP,
            UDP_GRO,
            &on as *const libc::c_int as *const libc::c_void,
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    rc == 0
}

/// Ask the kernel to deliver each datagram's IP TTL and TOS byte as control
/// messages, so the receiver passively observes the peer's hop count and ECN
/// markings (no protocol cost). Best-effort: a kernel that refuses either
/// option just yields no such cmsg, and the path sensor stays at its defaults.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn enable_ttl_ecn(sock: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let fd = sock.as_raw_fd();
    let on: libc::c_int = 1;
    // SAFETY: setsockopt on a valid fd with an int-sized option value that
    // outlives the call.
    let set = |opt: libc::c_int| unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_IP,
            opt,
            &on as *const libc::c_int as *const libc::c_void,
            size_of::<libc::c_int>() as libc::socklen_t,
        );
    };
    set(libc::IP_RECVTTL);
    set(libc::IP_RECVTOS);
}

/// Mark this socket's outgoing packets ECN-capable (ECT(0)), so an ECN-enabled
/// AQM on the path marks CE under congestion instead of tail-dropping - the
/// signal the AccECN counters (item 15) count. Best-effort.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn set_ect(sock: &UdpSocket) {
    use std::os::fd::AsRawFd;
    // ECT(0) is the ECN field value 0b10 in the low two bits of the IP TOS byte.
    let tos: libc::c_int = 0b10;
    // SAFETY: setsockopt on a valid fd with an int-sized value that outlives it.
    unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_TOS,
            &tos as *const libc::c_int as *const libc::c_void,
            size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Read a TTL / TOS ancillary value as a single byte. Linux delivers the
/// `IP_TTL` cmsg as a 4-byte `int`; the BSDs deliver it as a 1-byte
/// `u_char`. Reading by the cmsg's own payload length (an `int` when four
/// or more bytes are present, otherwise one byte) yields the same value on
/// either platform. The caller passes a pointer the CMSG walk validated.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn cmsg_scalar_u8(cmsg: *const libc::cmsghdr) -> u8 {
    // SAFETY: `cmsg` comes from CMSG_FIRSTHDR / CMSG_NXTHDR, so it points at
    // a valid cmsghdr whose payload occupies `cmsg_len - CMSG_LEN(0)` bytes;
    // each read below stays inside that payload.
    unsafe {
        let hdr_len = libc::CMSG_LEN(0) as usize;
        // `cmsg_len` is `size_t` on Linux and `socklen_t` on the BSDs; the
        // inferred cast widens both to usize without a same-type cast on the
        // platform where it is already usize.
        let total: usize = (*cmsg).cmsg_len as _;
        let payload = total.saturating_sub(hdr_len);
        if payload >= size_of::<libc::c_int>() {
            let mut v: libc::c_int = 0;
            std::ptr::copy_nonoverlapping(
                libc::CMSG_DATA(cmsg),
                &mut v as *mut libc::c_int as *mut u8,
                size_of::<libc::c_int>(),
            );
            v as u8
        } else if payload >= 1 {
            let mut b: u8 = 0;
            std::ptr::copy_nonoverlapping(libc::CMSG_DATA(cmsg), &mut b, 1);
            b
        } else {
            0
        }
    }
}

/// `recv` on a connected socket, also extracting the datagram's IP TTL from the
/// `IP_TTL` cmsg (item 14 reverse-hop count). Returns the byte count and the TTL
/// when present. Linux / BSD only; elsewhere it is a plain `recv` with no TTL.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
fn recv_with_ttl(sock: &UdpSocket, buf: &mut [u8]) -> io::Result<(usize, Option<u8>)> {
    use std::mem::zeroed;
    use std::os::fd::AsRawFd;
    // SAFETY: msghdr and its iov / control buffers are stack locals that live
    // across the recvmsg; the cmsg walk uses the kernel-filled control buffer.
    unsafe {
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        let mut cbuf = [0u8; 64];
        let mut msg: libc::msghdr = zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cbuf.len() as _;
        let n = libc::recvmsg(sock.as_raw_fd(), &mut msg, 0);
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut ttl = None;
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::IPPROTO_IP
                && ((*cmsg).cmsg_type == libc::IP_TTL || (*cmsg).cmsg_type == libc::IP_RECVTTL)
            {
                ttl = Some(cmsg_scalar_u8(cmsg));
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
        Ok((n as usize, ttl))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
fn recv_with_ttl(sock: &UdpSocket, buf: &mut [u8]) -> io::Result<(usize, Option<u8>)> {
    sock.recv(buf).map(|n| (n, None))
}

/// Ask the Windows stack to deliver each datagram's IP hop limit (TTL) and
/// TOS / ECN as control messages on `WSARecvMsg`, so the receiver passively
/// observes the peer's hop count and ECN markings. Mirrors the IPv4 path of
/// the Windows reference stack (msquic): `IP_HOPLIMIT` + `IP_RECVTOS` +
/// `IP_ECN`, each best-effort - a build that refuses an option just yields
/// no such cmsg and the path sensor keeps its defaults.
#[cfg(target_os = "windows")]
fn enable_ttl_ecn_win(sock: &UdpSocket) {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        setsockopt, IPPROTO_IP, IP_ECN, IP_HOPLIMIT, IP_RECVTOS,
    };
    let s = sock.as_raw_socket() as usize;
    let on: i32 = 1;
    // SAFETY: setsockopt on a valid socket with an int-sized option value
    // that outlives the call; return code ignored (best-effort).
    let set = |opt: i32| unsafe {
        setsockopt(
            s,
            IPPROTO_IP,
            opt,
            &on as *const i32 as *const u8,
            size_of::<i32>() as i32,
        );
    };
    set(IP_HOPLIMIT);
    set(IP_RECVTOS);
    set(IP_ECN);
}

/// Whether the GRO receive path is wanted (default on). `SUBETHA_GRO=0`
/// keeps the per-datagram `recvmmsg` path for the A/B baseline. Cached.
#[cfg(target_os = "linux")]
fn gro_wanted() -> bool {
    static EN: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *EN.get_or_init(|| std::env::var("SUBETHA_GRO").map(|v| v != "0").unwrap_or(true))
}

/// Count of `recvmsg` calls on the GRO path.
#[cfg(target_os = "linux")]
static GRO_RECVMSG: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Count of individual datagrams split out of coalesced GRO super-buffers.
#[cfg(target_os = "linux")]
static GRO_SEGMENTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Process-wide GRO telemetry as `(recvmsg_calls, segments_delivered)`. When
/// segments greatly exceeds calls, the kernel coalesced many wire datagrams
/// per syscall - the receive-side win. Linux-only; `(0, 0)` elsewhere.
pub fn gro_stats() -> (u64, u64) {
    #[cfg(target_os = "linux")]
    {
        use std::sync::atomic::Ordering::Relaxed;
        (GRO_RECVMSG.load(Relaxed), GRO_SEGMENTS.load(Relaxed))
    }
    #[cfg(not(target_os = "linux"))]
    {
        (0, 0)
    }
}

impl ReliableUdpReceiver {
    /// Bind `local`. The socket gets a 20ms read timeout so the receiver
    /// parks on data yet wakes often enough to drive tail-ARQ feedback.
    pub fn bind(local: impl ToSocketAddrs) -> io::Result<Self> {
        let sock = UdpSocket::bind(local)?;
        sock.set_read_timeout(Some(Duration::from_millis(4)))?;
        size_socket_buffers(&sock);
        // Observe each datagram's TTL / ECN passively: request the cmsgs here
        // (Linux / FreeBSD via IP_RECVTTL / IP_RECVTOS, Windows via
        // IP_HOPLIMIT / IP_RECVTOS / IP_ECN) and read them on the recv path.
        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        enable_ttl_ecn(&sock);
        #[cfg(target_os = "windows")]
        enable_ttl_ecn_win(&sock);
        // Wrap as the plain-UDP DgramSock backend after the raw-fd cmsg setup;
        // the standalone path keeps the fd (via as_udp) for the TTL/ECN recvmsg.
        let sock = crate::dgram::DgramSock::from_udp(sock);
        Ok(Self {
            sock,
            dec: Decoder::new(),
            peer: None,
            recv_count: 0,
            nak_history: BTreeMap::new(),
            last_feedback: Instant::now(),
            ctrl_out: 0,
            ctrl_recv: 0,
            peer_acked: 0,
            ctrl_out_at_last_hb: 0,
            peer_acked_at_last_hb: 0,
            fb_loss_est: 0.0,
            wbest: crate::wbest_sensor::WBestEstimator::new(BW_PROBE_BYTES),
            wbest_round: None,
            wbest_avail_kbps: 0,
            wbest_capacity_kbps: 0,
            peer_link_class: 0,
            peer_link_quality: 0,
            ack_interval: ACK_INTERVAL,
            fb_drop_pct: 0,
            fb_drop_rng: 0x243F6A8885A308D3,
            fb_delay: Duration::ZERO,
            fb_pending: VecDeque::new(),
            nak_batch: MAX_NAKS_PER_CYCLE,
            // Default: hold a gap for a long time so delivery is
            // effectively reliable; recovery almost always lands first.
            max_hold: Duration::from_secs(60),
            head_block: 0,
            head_since: Instant::now(),
            start: Instant::now(),
            debug_drop_pct: 0,
            drop_rng: 0x9E3779B97F4A7C15,
            ge_loss_p: 0,
            ge_loss_r: 0,
            ge_bad: false,
            drop_block_mod: 0,
            burst_at: 0,
            burst_len: 0,
            connected: false,
            #[cfg(any(target_os = "linux", target_os = "freebsd"))]
            rbufs: Vec::new(),
            #[cfg(target_os = "linux")]
            gro_on: false,
            #[cfg(target_os = "linux")]
            gro_buf: Vec::new(),
            last_ttl: 0,
            last_tos: 0,
            ce_count: 0,
            ect_count: 0,
            forecast: crate::forecast_sensor::ArrivalForecast::new(),
            fc_bytes: 0,
            fc_last: Instant::now(),
            periodicity: crate::periodicity_sensor::PeriodicitySensor::new(),
            net_events: NetEventObserver::start(None),
            peer_pmtu: 0,
            net_event_shift_peak: 0.0,
        })
    }

    /// Enable diagnostic whole-block loss: drop every shard of any data
    /// block whose id is a multiple of `m` (outer-parity blocks are never
    /// dropped). Such blocks are unrecoverable by ARQ, so successful
    /// delivery proves tower recovery.
    pub fn with_block_drop_mod(mut self, m: u32) -> Self {
        self.drop_block_mod = m;
        self
    }

    /// Enable a diagnostic loss BURST: drop every data datagram arriving in
    /// the window `[at, at + len)` (by arrival index) - one concentrated
    /// loss event - so the throughput trace shows a blip followed by full
    /// recovery as the held backlog drains.
    pub fn with_burst_loss(mut self, at: u64, len: u64) -> Self {
        self.burst_at = at;
        self.burst_len = len;
        self
    }

    /// Swap the datagram socket for one the caller already built (a demux socket
    /// the unified endpoint shares across both codes).
    pub fn set_sock(&mut self, sock: crate::dgram::DgramSock) {
        self.sock = sock;
    }

    /// The bound local address (useful when binding to port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Set how long a gap is held for recovery before being skipped to
    /// unblock the stream. A long value (the default is 60s) makes
    /// delivery effectively reliable; a short value bounds latency at the
    /// cost of dropping a gap that has not recovered in time.
    pub fn with_max_hold(mut self, hold: Duration) -> Self {
        self.max_hold = hold;
        self
    }

    /// Test knob: inject an artificial one-way delay on the feedback path,
    /// so a loopback run reproduces the recovery round-trip of a real
    /// (Wi-Fi / LAN) link. With zero delay feedback is sent inline; with a
    /// delay it queues and releases when due. Used to demonstrate that
    /// selective (parallel) NAK keeps throughput up where serial
    /// one-gap-per-round-trip recovery would stall.
    pub fn with_feedback_delay(mut self, delay: Duration) -> Self {
        self.fb_delay = delay;
        self
    }

    /// Cap the gaps NAK'd per poll cycle. The default re-requests every
    /// held gap in parallel (one round-trip for all); `1` reproduces the
    /// serial head-only recovery (one gap per round-trip) for A/B
    /// measurement of the head-of-line behavior.
    pub fn with_nak_batch(mut self, batch: usize) -> Self {
        self.nak_batch = batch.max(1);
        self
    }

    /// Datagrams read off the socket so far (telemetry).
    pub fn recv_count(&self) -> u64 {
        self.recv_count
    }

    /// Peak loss estimate (0..=255) the decoder has reported (telemetry).
    pub fn peak_loss_x255(&self) -> u8 {
        self.dec.peak_loss_x255()
    }

    /// Count of D-SACK false recoveries the decoder's reordering guard detected:
    /// spurious retransmissions whose reordered original later arrived. A
    /// nonzero value on a reorder-carrying link is the guard firing on the wire.
    pub fn false_recovery_count(&self) -> u64 {
        self.dec.false_recovery_count()
    }

    /// Drive the reported burstiness from the Gilbert-Elliott burst model (a
    /// real mean burst length) instead of the jitter heuristic - the A/B knob.
    pub fn set_ge_burst(&mut self, on: bool) {
        self.dec.set_ge_burst(on);
    }

    /// Fitted mean burst length from the Gilbert-Elliott model, or -1 before
    /// the fit converges (telemetry / A/B).
    pub fn mean_burst_len(&self) -> f32 {
        self.dec.mean_burst_len()
    }

    /// Estimated clock skew and the skew-corrected OWD trend the controller
    /// consumes - the raw trend minus the skew (telemetry).
    pub fn owd_skew(&self) -> f64 {
        self.dec.owd_skew()
    }

    pub fn owd_trend_debiased(&self) -> f64 {
        self.dec.owd_trend_debiased()
    }

    /// Inject reverse-path (feedback) loss: drop `pct` percent of OUTGOING
    /// feedback packets, the counterpart of [`with_debug_loss`](Self::with_debug_loss)
    /// (which drops inbound data). Used to show the feedback-redundancy response
    /// without netem.
    pub fn with_feedback_drop(mut self, pct: u32) -> Self {
        self.fb_drop_pct = pct.min(100);
        self
    }

    /// The current ACK cadence (telemetry); shortens under reverse-path loss.
    pub fn ack_interval(&self) -> Duration {
        self.ack_interval
    }

    /// Recompute the ACK cadence from reverse-path (feedback) loss: the share of
    /// our feedback the sender has not acknowledged receiving, beyond the normal
    /// in-flight. When our feedback is being lost, shorten the cadence so a lost
    /// ACK does not stall ARQ; restore it when feedback gets through. `ctrl_out -
    /// peer_acked` is feedback in flight plus lost; the sender reports
    /// `peer_acked` only on its ~20ms heartbeat cadence, so a steady backlog of
    /// a few dozen is normal in-flight and only a fraction well above it is loss.
    fn update_feedback_cadence(&mut self) {
        // Windowed loss rate over this heartbeat interval: how much feedback we
        // sent (`d_out`) versus how much more the sender acknowledged receiving
        // (`d_peer`). The cumulative in-flight backlog cancels, so this reflects
        // CURRENT reverse-path loss independent of link delay.
        let d_out = self.ctrl_out.saturating_sub(self.ctrl_out_at_last_hb);
        let d_peer = self.peer_acked.saturating_sub(self.peer_acked_at_last_hb);
        self.ctrl_out_at_last_hb = self.ctrl_out;
        self.peer_acked_at_last_hb = self.peer_acked;
        // Need enough feedback in the window for a stable ratio.
        if d_out >= 10 {
            let fb_loss = d_out.saturating_sub(d_peer) as f32 / d_out as f32;
            self.fb_loss_est = fb_loss;
            self.ack_interval = if fb_loss > 0.2 {
                ACK_INTERVAL / 4
            } else {
                ACK_INTERVAL
            };
        }
    }

    /// Last computed reverse-path (feedback) loss fraction the receiver measured
    /// from the sender's `LossAcct` reports (diagnostics).
    pub fn feedback_loss_est(&self) -> f32 {
        self.fb_loss_est
    }

    /// The peer's `(link_class, quality)` from the `Link` frame it echoes
    /// (class code: 0 unknown, 1 loopback, 2 wired, 3 Wi-Fi, 4 cellular).
    pub fn peer_link(&self) -> (u8, u8) {
        (self.peer_link_class, self.peer_link_quality)
    }

    /// AccECN (item 15) cumulative counts of the peer's CE-marked and ECN-capable
    /// packets this receiver has observed. A nonzero `ect` confirms the sender's
    /// ECT marking reached us; a rising `ce` is the AQM's congestion signal.
    pub fn accecn_counts(&self) -> (u64, u64) {
        (self.ce_count, self.ect_count)
    }

    /// The receiver's current Sprout forecast (item 16): the 5th-percentile
    /// next-tick deliverable rate it predicts (bits/s).
    pub fn forecast_bps(&self) -> u64 {
        (self.forecast.forecast_bps() * 8.0) as u64
    }

    /// The detected LEO handover cadence (item 17): `(period_s, confidence,
    /// secs_to_next_spike)`, or `None` until a periodic delay cadence is found.
    pub fn leo_cadence(&self) -> Option<(f64, f64, f64)> {
        let (period, conf) = self.periodicity.detected_period()?;
        Some((period, conf, self.periodicity.secs_to_next_spike().unwrap_or(0.0)))
    }

    /// Count of OS path events (route / carrier / MTU changes) this end's
    /// active observer has seen. The durable proof a real path event fired on
    /// this host - flapping a route or dropping the MTU bumps it (telemetry).
    pub fn net_event_count(&self) -> u64 {
        self.net_events.event_count()
    }

    /// This (receiver) endpoint's egress path MTU in bytes (0 = unknown),
    /// reported to the sender in the `Pmtu` frame (telemetry).
    pub fn local_pmtu(&self) -> u16 {
        self.net_events.pmtu().unwrap_or(0)
    }

    /// The peer's (sender's) last reported path MTU in bytes (0 = none yet),
    /// from its `Pmtu` frame (telemetry).
    pub fn peer_pmtu(&self) -> u16 {
        self.peer_pmtu
    }

    /// The active observer's current decaying path-shift (telemetry).
    pub fn net_event_shift(&self) -> f32 {
        self.net_events.path_shift()
    }

    /// The peak path shift this end's active observer reached over the run. A
    /// mid-transfer route / carrier / MTU event spikes this toward 1.0 even
    /// though the live shift has since decayed (telemetry).
    pub fn net_event_shift_peak(&self) -> f32 {
        self.net_event_shift_peak
    }

    /// Synthetically fire a path event on this end (the `--sim-path-event`
    /// demo). The production path is the active OS observer.
    pub fn inject_path_event(&self) {
        self.net_events.inject_event();
    }

    /// Synthetically set this endpoint's egress MTU (a drop also records a path
    /// event), as a real OS MTU change would. For tests / demos; production
    /// reads it from the active observer.
    pub fn inject_pmtu(&self, mtu: u16) {
        self.net_events.inject_pmtu(mtu);
    }

    /// Diagnostic snapshot of the block blocking in-order delivery:
    /// `(block_id, received_shards, k, decoded)`, or `None` if unseen.
    pub fn head_status(&self) -> Option<(u32, u32, usize, bool)> {
        self.dec.head_status()
    }

    /// Enable diagnostic loss injection: drop `pct` percent of incoming
    /// DATA datagrams (seeded, reproducible) to exercise FEC / ARQ on a
    /// link that does not lose packets on its own.
    pub fn with_debug_loss(mut self, pct: u32, seed: u64) -> Self {
        self.debug_drop_pct = pct.min(100);
        self.drop_rng = seed | 1;
        self
    }

    /// Enable diagnostic Gilbert-Elliott BURST loss: a two-state chain with
    /// per-10000 transition probabilities `p` (Good->Bad) and `r` (Bad->Good),
    /// dropping every datagram in the Bad state. Mean burst length is
    /// `10000 / r`, steady loss `p / (p + r)`. A known bursty channel for
    /// validating the burst model against the jitter heuristic.
    pub fn with_gilbert_loss(mut self, p_per_10k: u32, r_per_10k: u32, seed: u64) -> Self {
        self.ge_loss_p = p_per_10k;
        self.ge_loss_r = r_per_10k.max(1);
        self.drop_rng = seed | 1;
        self
    }

    /// Change the diagnostic loss rate at runtime (0 disables). Lets a test
    /// flip a clean link to lossy mid-stream to exercise the controller's
    /// re-arm and the ARQ floor on blocks that shipped at Passthrough.
    pub fn set_debug_loss(&mut self, pct: u32) {
        self.debug_drop_pct = pct.min(100);
    }

    /// `true` if this datagram belongs to a whole-block-dropped data
    /// block (a data block whose id is a multiple of `drop_block_mod`).
    /// Outer-parity datagrams are never dropped.
    fn drop_whole_block(&self, buf: &[u8]) -> bool {
        if self.drop_block_mod == 0 || buf.len() < 5 || is_outer_datagram(buf) {
            return false;
        }
        let bid = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
        bid.is_multiple_of(self.drop_block_mod)
    }

    /// `true` while the receiver is inside a configured loss-burst window
    /// (by datagram arrival index). `recv_count` is incremented before the
    /// drop checks, so it is the current datagram's 1-based index.
    #[inline]
    fn in_burst(&self) -> bool {
        self.burst_len != 0
            && self.recv_count >= self.burst_at
            && self.recv_count < self.burst_at + self.burst_len
    }

    #[inline]
    fn roll_drop(&mut self) -> bool {
        // Gilbert-Elliott burst loss: drop only in the Bad state, then advance
        // the two-state chain. Mean burst = 10000 / ge_loss_r.
        if self.ge_loss_r > 0 {
            let drop = self.ge_bad;
            self.drop_rng = self
                .drop_rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let roll = ((self.drop_rng >> 33) as u32) % 10000;
            if self.ge_bad {
                if roll < self.ge_loss_r {
                    self.ge_bad = false;
                }
            } else if roll < self.ge_loss_p {
                self.ge_bad = true;
            }
            return drop;
        }
        if self.debug_drop_pct == 0 {
            return false;
        }
        self.drop_rng = self
            .drop_rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.drop_rng >> 33) as u32) % 100 < self.debug_drop_pct
    }

    /// Demux-path receive: the unified endpoint's demux reader has already
    /// classified datagrams onto this receiver's queue, so there is no kernel
    /// fd for the batched recvmmsg / WSARecvMsg path. Pop the queue and process
    /// each datagram. Returns `true` when nothing was queued (idle), the same
    /// "nothing arrived" convention the fd recv paths use.
    fn recv_demux_drain(&mut self, out: &mut Vec<Vec<u8>>) -> io::Result<bool> {
        let mut buf = [0u8; RECV_BUF];
        let mut idle = true;
        loop {
            match self.sock.recv_from(&mut buf) {
                Ok((n, _)) => {
                    self.process_datagram(&buf[..n], out);
                    idle = false;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(idle)
    }

    /// Process one received datagram: a heartbeat feeds the timing
    /// estimator; the injected-loss filters swallow it; otherwise it is
    /// decoded and any newly deliverable items are appended to `out`.
    fn process_datagram(&mut self, buf: &[u8], out: &mut Vec<Vec<u8>>) {
        self.recv_count += 1;
        // Peak-hold this end's active path-event shift: a route / carrier / MTU
        // event spikes the observer's shift, which decays within seconds, so
        // sampling on each arrival captures the transient for the telemetry.
        let evt_shift = self.net_events.path_shift();
        if evt_shift > self.net_event_shift_peak {
            self.net_event_shift_peak = evt_shift;
        }
        if self.roll_drop() {
            return;
        }
        if is_control(buf) {
            if let Some(cp) = decode_control(buf) {
                // A control packet from the sender (a heartbeat). Count it for
                // reverse-path loss accounting, and read its LossAcct to learn
                // how many of OUR feedback packets the sender has received.
                self.ctrl_recv = self.ctrl_recv.wrapping_add(1);
                if let Some(la) = cp.loss_acct
                    && la.last_recv_seq > self.peer_acked
                {
                    self.peer_acked = la.last_recv_seq;
                }
                if let Some(lk) = cp.link {
                    self.peer_link_class = lk.class;
                    self.peer_link_quality = lk.quality;
                }
                if let Some(pm) = cp.pmtu
                    && pm.pmtu != 0
                {
                    self.peer_pmtu = pm.pmtu;
                }
                if let Some(t) = cp.timing {
                    let recv_ts = self.start.elapsed().as_micros() as u64;
                    self.dec.on_heartbeat(t.send_ts, recv_ts);
                    // Item 17: feed the relative OWD (recv minus send timestamp -
                    // the constant clock offset cancels in the autocorrelation's
                    // mean subtraction) to the LEO cadence detector.
                    let owd = recv_ts as f64 - t.send_ts as f64;
                    self.periodicity.observe(owd, recv_ts);
                }
                if !cp.bw_probe.is_empty() {
                    // Sub-microsecond arrival so a small dispersion at a high
                    // capacity is still resolved.
                    let arrival_us = self.start.elapsed().as_nanos() as f64 / 1000.0;
                    self.ingest_bw_probe(&cp.bw_probe, arrival_us);
                }
                self.update_feedback_cadence();
            }
        } else if self.drop_whole_block(buf) {
            // Whole-block loss injection: swallow it.
        } else if self.in_burst() {
            // Loss-burst injection: swallow it.
        } else {
            // AccECN (item 15): count this data packet's ECN. An ECN-capable
            // packet (ECT0 / ECT1 / CE) advances ect_count; a CE mark advances
            // ce_count - the AQM's congestion signal, which it sets before it
            // tail-drops. Echoed cumulatively in the Path frame.
            let ecn = self.last_tos & 0b11;
            if ecn != 0 {
                self.ect_count += 1;
                if ecn == crate::path_sensor::ECN_CE {
                    self.ce_count += 1;
                }
            }
            // Item 16: this data datagram's bytes are an arrival the Sprout
            // forecaster integrates over the tick (the path's deliverable rate).
            self.fc_bytes += buf.len() as u64;
            // Stamp the arrival so the decoder's loss differentiator measures
            // shard inter-arrival (the Biaz input); the clock origin is shared
            // with the heartbeat OWD above.
            let recv_us = self.start.elapsed().as_micros() as u64;
            out.extend(self.dec.on_packet_at(buf, recv_us));
        }
    }

    /// Run one Sprout forecast tick if `FORECAST_TICK` has elapsed: feed the
    /// bytes received since the last tick over that interval, then reset the
    /// accumulator. The forecast itself is read in the feedback build.
    fn maybe_observe_forecast(&mut self) {
        let dt = self.fc_last.elapsed();
        if dt >= FORECAST_TICK {
            self.forecast.observe(self.fc_bytes, dt.as_secs_f64());
            self.fc_bytes = 0;
            self.fc_last = Instant::now();
        }
    }

    /// Feed the WBest estimator one probe datagram's frame at its arrival time.
    /// A new round id resets the estimator; pair probes (`idx < 2*pairs`) and
    /// train probes (the rest) are routed by index. Recomputes the estimate
    /// (kbit/s) once both stages have samples.
    fn ingest_bw_probe(&mut self, probes: &[crate::control_frame::BwProbeFrame], arrival_us: f64) {
        let pair_probes = 2 * BW_PROBE_PAIRS;
        for f in probes {
            if self.wbest_round != Some(f.probe_id) {
                self.wbest.reset();
                self.wbest_round = Some(f.probe_id);
            }
            if f.idx < pair_probes {
                self.wbest.on_pair_probe(f.idx % 2, arrival_us);
            } else {
                self.wbest.on_train_probe(arrival_us);
            }
        }
        if let Some(c) = self.wbest.effective_capacity_bps() {
            self.wbest_capacity_kbps = (c / 1000.0) as u64;
        }
        if let Some(a) = self.wbest.available_bps() {
            self.wbest_avail_kbps = (a / 1000.0) as u64;
        }
    }

    /// The WBest estimate this receiver has computed: (available bandwidth,
    /// effective capacity) in bits/s, both 0 until a probe round completes.
    pub fn wbest_bps(&self) -> (u64, u64) {
        (self.wbest_avail_kbps * 1000, self.wbest_capacity_kbps * 1000)
    }

    /// Read datagrams into `out`. On Linux/FreeBSD, once the peer is known
    /// the socket is connected and a whole burst is read in one `recvmmsg`
    /// syscall - the per-datagram `recvfrom` was a top kernel cost on the
    /// receiver. The first datagram and other platforms use a single
    /// `recv_from`. Returns `true` when no data arrived (timeout park).
    fn recv_into(&mut self, out: &mut Vec<Vec<u8>>) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        if self.peer.is_some() {
            // GRO coalesces a whole burst into one skb; fall back to the
            // per-datagram recvmmsg batch on kernels without GRO.
            if self.gro_on {
                return self.recv_gro(out);
            }
            return self.recv_batch(out);
        }
        #[cfg(target_os = "freebsd")]
        if self.peer.is_some() {
            return self.recv_batch(out);
        }
        #[cfg(target_os = "windows")]
        if self.peer.is_some() {
            return self.recv_wsamsg(out);
        }
        let mut buf = [0u8; RECV_BUF];
        match self.sock.recv_from(&mut buf) {
            Ok((n, src)) => {
                self.peer = Some(src);
                // Connect to the peer (the transport is point-to-point) so
                // the batched path needs no per-datagram source capture.
                // Reached only on the first datagram on Linux/FreeBSD;
                // best-effort, since recvmmsg works unconnected too.
                #[cfg(target_os = "linux")]
                {
                    self.connected = self.sock.connect(src).is_ok();
                    // Turn on GRO now the socket is connected; the next poll
                    // reads coalesced super-buffers. `SUBETHA_GRO=0` keeps
                    // the recvmmsg path for the A/B baseline.
                    self.gro_on = self.connected
                        && gro_wanted()
                        && self.sock.as_udp().map(enable_gro).unwrap_or(false);
                }
                #[cfg(target_os = "freebsd")]
                {
                    self.connected = self.sock.connect(src).is_ok();
                }
                #[cfg(target_os = "windows")]
                {
                    // Connect so WSARecvMsg reads from the peer with no source
                    // capture and feedback rides send() like the connected
                    // Unix paths.
                    self.connected = self.sock.connect(src).is_ok();
                }
                self.process_datagram(&buf[..n], out);
                Ok(false)
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionRefused
                ) =>
            {
                // The ICMP-port-unreachable artifact on a connected UDP
                // socket - ConnectionReset on Windows, ConnectionRefused on
                // Linux/BSD; treat it like a timeout.
                Ok(true)
            }
            Err(e) => Err(e),
        }
    }

    /// Walk one received message's control buffer for the IP TTL and TOS
    /// cmsgs requested by [`enable_ttl_ecn`], updating `last_ttl` /
    /// `last_tos` so the next `Path` frame echoes them (the TOS byte's low
    /// two bits are the ECN field). FreeBSD may tag the TTL with cmsg type
    /// `IP_RECVTTL` and Linux with `IP_TTL`; both spellings are accepted.
    /// Used by the per-datagram `recvmmsg` batch path; the GRO path inlines
    /// the same read alongside its segment-size cmsg.
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    fn observe_ttl_tos(&mut self, msg: &libc::msghdr) {
        // SAFETY: `msg` is a live msghdr whose `msg_control` the kernel
        // filled; the CMSG walk stays within the reported `msg_controllen`,
        // and `cmsg_scalar_u8` reads only within each cmsg's payload.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(msg as *const libc::msghdr);
            while !cmsg.is_null() {
                let level = (*cmsg).cmsg_level;
                let cty = (*cmsg).cmsg_type;
                if level == libc::IPPROTO_IP
                    && (cty == libc::IP_TTL || cty == libc::IP_RECVTTL)
                {
                    self.last_ttl = cmsg_scalar_u8(cmsg);
                } else if level == libc::IPPROTO_IP
                    && (cty == libc::IP_TOS || cty == libc::IP_RECVTOS)
                {
                    self.last_tos = cmsg_scalar_u8(cmsg);
                }
                cmsg = libc::CMSG_NXTHDR(msg as *const libc::msghdr, cmsg);
            }
        }
    }

    /// Batched receive: up to `RECV_BATCH` datagrams from the connected
    /// socket in one `recvmmsg` syscall. `MSG_WAITFORONE` parks (up to the
    /// socket read timeout) for the first datagram, then takes everything
    /// else already queued.
    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    fn recv_batch(&mut self, out: &mut Vec<Vec<u8>>) -> io::Result<bool> {
        use std::os::fd::AsRawFd;
        const RECV_BATCH: usize = 32;
        // 64 B of cmsg scratch per message: room for the IP_TTL and IP_TOS
        // ancillary objects (`CMSG_SPACE(int)` + `CMSG_SPACE(byte)` < 64) so
        // each datagram's TTL / ECN lands in its own slot.
        const CMSG_WORDS: usize = 8;
        if self.rbufs.len() < RECV_BATCH {
            self.rbufs.resize_with(RECV_BATCH, || vec![0u8; RECV_BUF]);
        }
        // The demux path has no kernel fd for recvmmsg; drain its queue plainly.
        if self.sock.as_udp().is_none() {
            return self.recv_demux_drain(out);
        }
        let fd = self.sock.as_udp().expect("Udp checked above").as_raw_fd();
        let mut iovecs: Vec<libc::iovec> = self
            .rbufs
            .iter_mut()
            .take(RECV_BATCH)
            .map(|b| libc::iovec {
                iov_base: b.as_mut_ptr() as *mut libc::c_void,
                iov_len: RECV_BUF,
            })
            .collect();
        // One cmsg scratch buffer per message; the kernel writes each
        // datagram's TTL / TOS ancillary data into its own slot and sets
        // that message's `msg_controllen` to the bytes it wrote.
        let mut ctrl: Vec<[u64; CMSG_WORDS]> = vec![[0u64; CMSG_WORDS]; RECV_BATCH];
        let mut msgs: Vec<libc::mmsghdr> = Vec::with_capacity(RECV_BATCH);
        for (i, slot) in ctrl.iter_mut().enumerate() {
            // SAFETY: a zeroed mmsghdr with msg_iov / msg_iovlen pointing at
            // the live iovec and msg_control / msg_controllen pointing at this
            // message's cmsg slot is a valid receive descriptor on a connected
            // socket; both buffers outlive the recvmmsg call.
            let mut hdr: libc::mmsghdr = unsafe { std::mem::zeroed() };
            hdr.msg_hdr.msg_iov = iovecs.as_mut_ptr().wrapping_add(i);
            hdr.msg_hdr.msg_iovlen = 1 as _;
            hdr.msg_hdr.msg_control = slot.as_mut_ptr() as *mut libc::c_void;
            hdr.msg_hdr.msg_controllen = (CMSG_WORDS * size_of::<u64>()) as _;
            msgs.push(hdr);
        }
        // An explicit timeout bounds the wait for the FIRST message. With a
        // NULL timeout FreeBSD's recvmmsg blocks until every `vlen` buffer
        // fills - MSG_WAITFORONE only sets MSG_DONTWAIT *after* the first
        // message, so at end-of-stream the first receive blocks forever
        // (FreeBSD does not honor SO_RCVTIMEO here the way Linux does). The
        // timeout matches the socket read-timeout park that drives tail-ARQ
        // and is equivalent to the SO_RCVTIMEO behavior on Linux.
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 4_000_000,
        };
        // SAFETY: msgs is RECV_BATCH valid descriptors into the live rbufs;
        // fd is the connected socket; ts outlives the call. The pointer is
        // `*mut` (Linux) and coerces to `*const` (FreeBSD).
        let n = unsafe {
            libc::recvmmsg(
                fd,
                msgs.as_mut_ptr(),
                RECV_BATCH as MmsgLen,
                libc::MSG_WAITFORONE,
                &mut ts as *mut libc::timespec,
            )
        };
        if n == 0 {
            // FreeBSD returns 0 when the recvmmsg timeout expires with no
            // data; Linux returns -1/EAGAIN. Both mean the read-timeout park,
            // which must drive tail-ARQ feedback, NOT surface as an error
            // (an error here skips the feedback in poll() and the sender's
            // drain_until_acked then waits forever for ACKs that never come).
            return Ok(true);
        }
        if n < 0 {
            let e = io::Error::last_os_error();
            return match e.kind() {
                io::ErrorKind::WouldBlock
                | io::ErrorKind::TimedOut
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionRefused => Ok(true),
                _ => Err(e),
            };
        }
        for (i, msg) in msgs.iter().take(n as usize).enumerate() {
            let len = msg.msg_len as usize;
            if len == 0 || len > RECV_BUF {
                continue;
            }
            // Pull this datagram's TTL / TOS out of its own cmsg slot before
            // the decode borrow. recvmmsg set this message's `msg_controllen`
            // to the bytes it wrote, so the walk reads only real ancillary
            // data.
            self.observe_ttl_tos(&msg.msg_hdr);
            // Copy out so the decode can take &mut self; the on-decode
            // path copies the shard regardless. `i` indexes the parallel
            // rbufs slot, copied before the &mut self decode borrow.
            let mut tmp = [0u8; RECV_BUF];
            tmp[..len].copy_from_slice(&self.rbufs[i][..len]);
            self.process_datagram(&tmp[..len], out);
        }
        Ok(false)
    }

    /// Coalesced receive (Linux GRO). One `recvmsg` reads a super-buffer of
    /// up to 64 KiB that the kernel coalesced from many same-size datagrams;
    /// its `UDP_GRO` control message carries the segment size, so the buffer
    /// splits back into the individual shards. The first read parks on the
    /// socket timeout (so a quiet link still drives tail-ARQ); queued
    /// super-buffers are then drained with `MSG_DONTWAIT`. This is the
    /// receive-side counterpart of GSO: one skb up the stack instead of
    /// `k + r`. Returns `true` only when nothing arrived (timeout park).
    #[cfg(target_os = "linux")]
    fn recv_gro(&mut self, out: &mut Vec<Vec<u8>>) -> io::Result<bool> {
        use std::os::fd::AsRawFd;
        use std::sync::atomic::Ordering::Relaxed;
        const UDP_GRO: libc::c_int = 104;
        const GRO_BUF: usize = 65536;
        if self.gro_buf.len() < GRO_BUF {
            self.gro_buf.resize(GRO_BUF, 0);
        }
        if self.sock.as_udp().is_none() {
            return self.recv_demux_drain(out);
        }
        let fd = self.sock.as_udp().expect("Udp checked above").as_raw_fd();
        let mut got_any = false;
        let mut first = true;
        loop {
            let mut iov = libc::iovec {
                iov_base: self.gro_buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: GRO_BUF,
            };
            // Room for the UDP_GRO cmsg plus the IP_TTL and IP_TOS cmsgs.
            let mut cmsg_space = [0u64; 16];
            // SAFETY: a zeroed msghdr with one iovec into the live gro_buf
            // and a cmsg scratch buffer that outlives the call.
            let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
            msg.msg_iov = &mut iov;
            msg.msg_iovlen = 1;
            msg.msg_control = cmsg_space.as_mut_ptr() as *mut libc::c_void;
            msg.msg_controllen = (cmsg_space.len() * size_of::<u64>()) as _;
            let flags = if first { 0 } else { libc::MSG_DONTWAIT };
            // SAFETY: fd is the connected socket; msg points at live buffers.
            let n = unsafe { libc::recvmsg(fd, &mut msg, flags) };
            if n < 0 {
                let e = io::Error::last_os_error();
                return match e.kind() {
                    io::ErrorKind::WouldBlock
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionRefused => Ok(!got_any),
                    _ => Err(e),
                };
            }
            let n = n as usize;
            // Segment size from the UDP_GRO cmsg; absent = a single datagram.
            let mut seg = n;
            // SAFETY: msg.msg_control points at the cmsg buffer the kernel
            // filled; the CMSG walk stays within the reported msg_controllen.
            unsafe {
                let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
                while !cmsg.is_null() {
                    let level = (*cmsg).cmsg_level;
                    let cty = (*cmsg).cmsg_type;
                    if level == libc::SOL_UDP && cty == UDP_GRO {
                        let mut s: libc::c_int = 0;
                        std::ptr::copy_nonoverlapping(
                            libc::CMSG_DATA(cmsg),
                            &mut s as *mut libc::c_int as *mut u8,
                            size_of::<libc::c_int>(),
                        );
                        if s > 0 {
                            seg = s as usize;
                        }
                    } else if level == libc::IPPROTO_IP && cty == libc::IP_TTL {
                        let mut t: libc::c_int = 0;
                        std::ptr::copy_nonoverlapping(
                            libc::CMSG_DATA(cmsg),
                            &mut t as *mut libc::c_int as *mut u8,
                            size_of::<libc::c_int>(),
                        );
                        self.last_ttl = t as u8;
                    } else if level == libc::IPPROTO_IP && cty == libc::IP_TOS {
                        // The IP_TOS cmsg is a single byte; its low two bits
                        // are the ECN field.
                        let mut tos: u8 = 0;
                        std::ptr::copy_nonoverlapping(libc::CMSG_DATA(cmsg), &mut tos, 1);
                        self.last_tos = tos;
                    }
                    cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
                }
            }
            if seg == 0 {
                seg = n;
            }
            // Split the coalesced buffer into shards. All segments are `seg`
            // bytes except possibly the final remainder.
            let mut off = 0usize;
            let mut segs = 0u64;
            while off < n {
                let end = (off + seg).min(n);
                let len = end - off;
                if len > 0 && len <= RECV_BUF {
                    let mut tmp = [0u8; RECV_BUF];
                    tmp[..len].copy_from_slice(&self.gro_buf[off..end]);
                    self.process_datagram(&tmp[..len], out);
                    segs += 1;
                }
                off = end;
            }
            GRO_RECVMSG.fetch_add(1, Relaxed);
            GRO_SEGMENTS.fetch_add(segs, Relaxed);
            got_any = true;
            first = false;
            // Bound the drain so one poll cannot spin without yielding.
            if out.len() > 4096 {
                return Ok(false);
            }
        }
    }

    /// Receive one datagram on Windows via `WSARecvMsg`, reading the IP hop
    /// limit and TOS / ECN from its control messages - the Windows analogue
    /// of the Linux/FreeBSD cmsg path. The socket is connected to the peer by
    /// the time this runs, so no source capture is needed and the read parks
    /// on the socket timeout (driving tail-ARQ). Falls back to a plain
    /// connected `recv` if the `WSARecvMsg` extension is unavailable. Returns
    /// `true` only when nothing arrived (timeout park).
    #[cfg(target_os = "windows")]
    fn recv_wsamsg(&mut self, out: &mut Vec<Vec<u8>>) -> io::Result<bool> {
        use std::os::windows::io::AsRawSocket;
        use windows_sys::Win32::Networking::WinSock::{WSAGetLastError, WSABUF, WSAMSG};
        if self.sock.as_udp().is_none() {
            return self.recv_demux_drain(out);
        }
        let sock = self.sock.as_udp().expect("Udp checked above").as_raw_socket() as usize;
        let Some(wsarecvmsg) = load_wsarecvmsg(sock) else {
            // Extension unavailable: plain connected recv, no TTL / ECN cmsg.
            let mut buf = [0u8; RECV_BUF];
            return match self.sock.recv(&mut buf) {
                Ok(n) => {
                    self.process_datagram(&buf[..n], out);
                    Ok(false)
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    Ok(true)
                }
                Err(e) => Err(e),
            };
        };
        const SOCKET_ERROR: i32 = -1;
        const WSAEMSGSIZE: i32 = 10040;
        const WSAEWOULDBLOCK: i32 = 10035;
        const WSAETIMEDOUT: i32 = 10060;
        const WSAECONNRESET: i32 = 10054;
        const WSAECONNREFUSED: i32 = 10061;
        let mut buf = [0u8; RECV_BUF];
        let mut data = WSABUF {
            len: RECV_BUF as u32,
            buf: buf.as_mut_ptr(),
        };
        // Control buffer for the hop-limit + TOS / ECN cmsgs. Each is a 16 B
        // WSACMSGHDR + a 4 B int, space-aligned to 24 B; `[u64; 16]` = 128 B
        // holds several comfortably.
        let mut ctrl = [0u64; 16];
        let mut msg = WSAMSG {
            name: std::ptr::null_mut(),
            namelen: 0,
            lpBuffers: &mut data,
            dwBufferCount: 1,
            Control: WSABUF {
                len: (ctrl.len() * size_of::<u64>()) as u32,
                buf: ctrl.as_mut_ptr() as *mut u8,
            },
            dwFlags: 0,
        };
        let mut recvd = 0u32;
        // SAFETY: msg points at the live data / ctrl buffers, which outlive
        // the call; sock is the connected socket; no overlapped structure or
        // completion routine.
        let rc = unsafe {
            wsarecvmsg(
                sock,
                &mut msg,
                &mut recvd,
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        };
        if rc == SOCKET_ERROR {
            // SAFETY: plain thread-local error fetch, no preconditions.
            let err = unsafe { WSAGetLastError() };
            return match err {
                // Read-timeout park (drives tail-ARQ), ICMP reset / refused,
                // or an over-size datagram: nothing usable this cycle.
                WSAEWOULDBLOCK | WSAETIMEDOUT | WSAECONNRESET | WSAECONNREFUSED
                | WSAEMSGSIZE => Ok(true),
                _ => Err(io::Error::from_raw_os_error(err)),
            };
        }
        let n = recvd as usize;
        if n == 0 || n > RECV_BUF {
            return Ok(true);
        }
        // Walk the control buffer the kernel filled (`msg.Control.len` holds
        // the bytes written) for the TTL and TOS / ECN cmsgs.
        let ctrl_len = (msg.Control.len as usize).min(ctrl.len() * size_of::<u64>());
        // SAFETY: `ctrl` holds `ctrl_len` bytes the kernel initialized.
        let cbytes = unsafe { std::slice::from_raw_parts(ctrl.as_ptr() as *const u8, ctrl_len) };
        self.observe_wsa_cmsgs(cbytes);
        self.process_datagram(&buf[..n], out);
        Ok(false)
    }

    /// Walk a `WSARecvMsg` control buffer for the IPv4 hop-limit (`IP_TTL`)
    /// and TOS / ECN (`IP_TOS` / `IP_ECN`) cmsgs, updating `last_ttl` /
    /// `last_tos` (the TOS byte's low two bits are the ECN field). Each
    /// Windows cmsg payload is a 4-byte `int`. The 64-bit `WSACMSGHDR` is
    /// `cmsg_len` (usize) at 0, `cmsg_level` (i32) at 8, `cmsg_type` (i32)
    /// at 12, and the data at 16 (the header size aligned up to the 8-byte
    /// natural alignment).
    #[cfg(target_os = "windows")]
    fn observe_wsa_cmsgs(&mut self, control: &[u8]) {
        use windows_sys::Win32::Networking::WinSock::{IPPROTO_IP, IP_ECN, IP_TOS, IP_TTL};
        const HDR: usize = 16;
        let lvl_ip = IPPROTO_IP;
        let mut off = 0usize;
        while off + HDR <= control.len() {
            // SAFETY: every read is bounds-checked against control.len()
            // before it runs, and `control` holds that many initialized bytes.
            let cmsg_len =
                unsafe { std::ptr::read_unaligned(control.as_ptr().add(off) as *const usize) };
            if cmsg_len < HDR || off + cmsg_len > control.len() {
                break;
            }
            let level =
                unsafe { std::ptr::read_unaligned(control.as_ptr().add(off + 8) as *const i32) };
            let cty =
                unsafe { std::ptr::read_unaligned(control.as_ptr().add(off + 12) as *const i32) };
            if level == lvl_ip && cmsg_len - HDR >= size_of::<i32>() {
                let val = unsafe {
                    std::ptr::read_unaligned(control.as_ptr().add(off + HDR) as *const i32)
                };
                if cty == IP_TTL {
                    self.last_ttl = val as u8;
                } else if cty == IP_TOS || cty == IP_ECN {
                    self.last_tos = val as u8;
                }
            }
            // Advance to the next header, the cmsg length aligned up to 8.
            off += (cmsg_len + 7) & !7;
        }
    }

    /// Receive one datagram (or hit the read timeout), decode it, send
    /// feedback to the peer, and return any items that became
    /// deliverable in stream order. On timeout, feedback is sent with
    /// tail-ARQ drive so a stalled final block recovers.
    pub fn poll(&mut self) -> io::Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        // Read one datagram, or a whole batch in one syscall where the
        // platform supports it. `timed_out` is true when no data arrived
        // (the read-timeout park), which drives tail-ARQ feedback.
        let timed_out = self.recv_into(&mut out)?;
        // Release any delayed feedback whose injected link latency has
        // elapsed (no-op unless a feedback delay is configured).
        self.flush_delayed_feedback();
        if let Some(peer) = self.peer {
            let base = self.dec.feedback(timed_out);
            let now = Instant::now();
            // Plain ACK (cumulative frontier + sensors) on the ACK cadence
            // or a timeout drive. The NAK rides the selective pass below,
            // so strip it from the ACK packet.
            if timed_out || self.last_feedback.elapsed() >= self.ack_interval {
                let mut ack = base;
                ack.nak_block = NAK_NONE;
                ack.nak_mask = 0;
                self.queue_feedback(peer, &ack);
                self.last_feedback = now;
            }
            // Selective NAK: re-request EVERY gap the window is holding in
            // this one cycle (capped), each rate-limited per-block to ~one
            // per RTT. This is the head-of-line fix: retransmits for all
            // gaps flow in a single round-trip and the delivery frontier
            // advances in bulk, instead of recovering one gap per
            // round-trip while the wire stalls behind it.
            for (block, mask) in self.dec.missing_blocks(self.nak_batch, timed_out) {
                if mask == 0 {
                    continue;
                }
                let fresh = self
                    .nak_history
                    .get(&block)
                    .is_none_or(|t| now.duration_since(*t) >= NAK_COOLDOWN);
                if fresh {
                    let mut nfb = base;
                    nfb.nak_block = block;
                    nfb.nak_mask = mask;
                    self.queue_feedback(peer, &nfb);
                    self.nak_history.insert(block, now);
                }
            }
            // Prune per-block NAK history below the delivery frontier; those
            // blocks are delivered and will never be NAK'd again.
            let nd = self.dec.next_needed();
            self.nak_history = self.nak_history.split_off(&nd);
        }
        // Hold-time deadline: a gap held longer than max_hold is skipped
        // so the stream is not blocked forever by an unrecoverable block.
        let head = self.dec.next_needed();
        if head != self.head_block {
            self.head_block = head;
            self.head_since = Instant::now();
        } else if self.head_since.elapsed() > self.max_hold && self.dec.window_len() > 0 {
            out.extend(self.dec.skip_head());
            self.head_block = self.dec.next_needed();
            self.head_since = Instant::now();
        }
        Ok(out)
    }

    /// Encode and dispatch a feedback packet to `peer`. With no feedback
    /// delay configured it sends inline; with a delay it queues for release
    /// by [`flush_delayed_feedback`](Self::flush_delayed_feedback), so a
    /// loopback run can reproduce a real link's recovery round-trip.
    /// Feedback is best-effort and self-healing (the ack frontier is
    /// cumulative), so a transient send error must not abort the loop.
    /// Encode the receiver-side control state as a CONTROL packet: an ACK
    /// frame, a NAK frame when one is pending, a LOSS frame with the fused
    /// channel readings, and a PATH frame echoing the peer's last observed
    /// TTL / ECN so the sender's controller sees hop-count shifts and ECN
    /// congestion before they reach the loss estimate.
    fn control_bytes(&self, fb: &Feedback) -> Vec<u8> {
        let mut cp = ControlPacket::new();
        cp.ack = Some(AckFrame {
            ack_through: fb.ack_through,
        });
        if fb.nak_block != NAK_NONE {
            cp.nak = Some(NakFrame {
                block: fb.nak_block,
                mask: fb.nak_mask,
            });
        }
        cp.loss = Some(LossFrame {
            loss_x255: fb.loss_x255,
            burstiness_x255: fb.burstiness_x255,
            owd_trend_class: fb.owd_trend_class,
            loss_class: fb.loss_class,
        });
        if self.last_ttl != 0 {
            cp.path = Some(PathFrame {
                ttl: self.last_ttl,
                ecn: self.last_tos & 0b11,
                hop_count: crate::path_sensor::hop_count_from_ttl(self.last_ttl),
                ce_count: self.ce_count,
                ect_count: self.ect_count,
            });
        }
        // Our egress path MTU, so a handoff on this (receiver) end rides the
        // feedback to the sender's controller.
        if let Some(pm) = self.net_events.pmtu() {
            cp.pmtu = Some(PmtuFrame { pmtu: pm });
        }
        // Bidirectional loss accounting: report how many feedback packets we
        // have sent and how many sender heartbeats we have received, so the
        // sender separates forward (data) loss from reverse (feedback) loss.
        cp.loss_acct = Some(LossAcctFrame {
            seq: self.ctrl_out,
            last_recv_seq: self.ctrl_recv,
        });
        // WBest report (item 13): our measured available bandwidth / effective
        // capacity, so the sender can cross-check its passive BtlBw.
        if self.wbest_capacity_kbps != 0 {
            cp.avail_bw = Some(crate::control_frame::AvailBwFrame {
                avail_kbps: self.wbest_avail_kbps,
                capacity_kbps: self.wbest_capacity_kbps,
            });
        }
        // Sprout forecast (item 16): the 5th-percentile next-tick deliverable
        // rate, so the sender pre-sizes ahead of a dip.
        let fc_kbps = (self.forecast.forecast_bps() * 8.0 / 1000.0) as u64;
        if fc_kbps != 0 {
            cp.forecast = Some(crate::control_frame::ForecastFrame {
                forecast_kbps: fc_kbps,
            });
        }
        // LEO cadence (item 17): a detected handover period and time-to-next-spike
        // (deciseconds), so the sender pre-arms one cycle ahead.
        if let Some((period_s, conf)) = self.periodicity.detected_period() {
            let to_spike = self.periodicity.secs_to_next_spike().unwrap_or(0.0);
            cp.periodicity = Some(crate::control_frame::PeriodicityFrame {
                period_ds: (period_s * 10.0).round() as u64,
                secs_to_spike_ds: (to_spike * 10.0).round() as u64,
                confidence_x255: (conf.clamp(0.0, 1.0) * 255.0) as u8,
            });
        }
        encode_control(&cp)
    }

    fn queue_feedback(&mut self, peer: SocketAddr, fb: &Feedback) {
        // Item 16: integrate one forecast tick before building the feedback that
        // carries the forecast.
        self.maybe_observe_forecast();
        // Count this feedback packet as sent BEFORE building it, so the LossAcct
        // seq it carries includes itself.
        self.ctrl_out = self.ctrl_out.wrapping_add(1);
        let fbuf = self.control_bytes(fb);
        // Inject reverse-path loss: the receiver did send it (ctrl_out counted
        // it), but it never reaches the sender, so the sender's peer_acked lags.
        if self.fb_drop_pct > 0 {
            self.fb_drop_rng = self
                .fb_drop_rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            if ((self.fb_drop_rng >> 33) as u32) % 100 < self.fb_drop_pct {
                return;
            }
        }
        if self.fb_delay.is_zero() {
            self.send_feedback_bytes(&fbuf, peer);
        } else {
            self.fb_pending
                .push_back((Instant::now() + self.fb_delay, fbuf));
        }
    }

    /// Send one feedback datagram to the peer. The receive socket is
    /// connected on Linux/FreeBSD (for recvmmsg / GRO), and BSD rejects
    /// `send_to` on a connected UDP socket with EISCONN - so `send()` once
    /// connected, `send_to()` only while still unconnected (Windows / other).
    fn send_feedback_bytes(&self, bytes: &[u8], peer: SocketAddr) {
        if self.connected {
            self.sock.send(bytes).ok();
        } else {
            self.sock.send_to(bytes, peer).ok();
        }
    }

    /// Send any delayed feedback whose release time has arrived. A no-op
    /// when no feedback delay is configured. The queue is in release-time
    /// order (pushes use a monotonic clock), so a front-to-back drain
    /// stops at the first not-yet-due entry.
    fn flush_delayed_feedback(&mut self) {
        if self.fb_pending.is_empty() {
            return;
        }
        let now = Instant::now();
        let Some(peer) = self.peer else { return };
        while let Some((due, _)) = self.fb_pending.front() {
            if *due > now {
                break;
            }
            let (_, bytes) = self.fb_pending.pop_front().unwrap();
            self.send_feedback_bytes(&bytes, peer);
        }
    }

    /// Send one feedback packet to the peer with tail-ARQ drive (used as
    /// a grace flush after all items are delivered, so the sender learns
    /// the final ack).
    pub fn nudge_feedback(&mut self) -> io::Result<()> {
        if let Some(peer) = self.peer {
            self.ctrl_out = self.ctrl_out.wrapping_add(1);
            let fb = self.dec.feedback(true);
            let fbuf = self.control_bytes(&fb);
            self.send_feedback_bytes(&fbuf, peer);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// k must fit the u32 shard bitmap: a k > MAX_SHARDS would overflow
    /// `1 << shard_index` and silently corrupt delivery, so bind rejects it.
    #[test]
    fn bind_rejects_oversized_k() {
        let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();
        assert!(
            ReliableUdpSender::bind("127.0.0.1:0", peer, 33, 1, 64).is_err(),
            "k=33 > MAX_SHARDS must be rejected"
        );
        assert!(
            ReliableUdpSender::bind("127.0.0.1:0", peer, 0, 1, 64).is_err(),
            "k=0 must be rejected"
        );
        assert!(
            ReliableUdpSender::bind("127.0.0.1:0", peer, 16, 8, 64).is_ok(),
            "k=16 r=8 (k+r=24) must be accepted"
        );
    }

    /// Real loopback sockets, real UDP datagrams, diagnostic loss on the
    /// receiver. Ships `n` u64 items and asserts exact in-order
    /// delivery, proving the FEC + ARQ stack over an actual socket.
    fn loopback_round_trip(n: u64, k: usize, r: usize, loss_pct: u32, seed: u64) {
        let (addr_tx, addr_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let rx = std::thread::spawn(move || {
            let mut recv = ReliableUdpReceiver::bind("127.0.0.1:0")
                .unwrap()
                .with_debug_loss(loss_pct, seed);
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            let mut got: Vec<u64> = Vec::new();
            let start = Instant::now();
            while (got.len() as u64) < n {
                if start.elapsed() > Duration::from_secs(20) {
                    break;
                }
                for item in recv.poll().unwrap() {
                    got.push(u64::from_le_bytes(item.try_into().unwrap()));
                }
            }
            // Grace: let the sender learn the final ack.
            for _ in 0..10 {
                recv.nudge_feedback().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            done_tx.send(()).ok();
            got
        });

        let recv_addr = addr_rx.recv().unwrap();
        let tx = std::thread::spawn(move || {
            let mut send =
                ReliableUdpSender::bind("127.0.0.1:0", recv_addr, k, r, 8).unwrap();
            for i in 0..n {
                while send.flow_blocked() {
                    send.drain_until_acked(Duration::from_millis(50)).ok();
                }
                send.send_item(&i.to_le_bytes()).unwrap();
            }
            send.flush().unwrap();
            send.drain_until_acked(Duration::from_secs(15)).unwrap();
            done_rx.recv_timeout(Duration::from_secs(20)).ok();
        });

        let got = rx.join().unwrap();
        tx.join().unwrap();
        let expected: Vec<u64> = (0..n).collect();
        assert_eq!(got, expected, "loopback exact in-order delivery");
    }

    #[test]
    fn loopback_clean() {
        loopback_round_trip(500, 8, 2, 0, 1);
    }

    #[test]
    fn loopback_lossy_fec() {
        // ~12% injected loss, r=3 over k=8: FEC carries most blocks.
        loopback_round_trip(500, 8, 3, 12, 7);
    }

    #[test]
    fn loopback_heavy_arq() {
        // ~30% injected loss: ARQ fallback must carry the remainder.
        loopback_round_trip(300, 8, 2, 30, 1234);
    }

    /// The item-12 active path-event slice over a real loopback bridge: an
    /// injected path event registers on the receiver, and each endpoint's
    /// egress MTU rides its `Pmtu` frame to the peer. The assertion is the
    /// cross-check `peer_pmtu == the other side's local_pmtu`, so it holds
    /// faithfully whether or not the host exposes a readable MTU (both 0 on a
    /// host without one). Distinct injected MTUs make the round-trip
    /// discriminating rather than coincidental.
    #[test]
    fn path_event_registers_and_pmtu_round_trips() {
        let n = 400u64;
        let (addr_tx, addr_rx) = mpsc::channel();
        // Receiver result: (net_events, peer_pmtu seen, local_pmtu reported).
        let (rres_tx, rres_rx) = mpsc::channel();

        let rx = std::thread::spawn(move || {
            let mut recv = ReliableUdpReceiver::bind("127.0.0.1:0").unwrap();
            // Force a known egress MTU and a path event on this (receiver) end.
            recv.inject_pmtu(1400);
            recv.inject_path_event();
            addr_tx.send(recv.local_addr().unwrap()).unwrap();
            let mut got = 0u64;
            let start = Instant::now();
            while got < n {
                if start.elapsed() > Duration::from_secs(20) {
                    break;
                }
                for _item in recv.poll().unwrap() {
                    got += 1;
                }
            }
            // Grace: keep the feedback flowing so the sender's heartbeat (with
            // its Pmtu frame) is drained and our own feedback Pmtu is sent.
            for _ in 0..60 {
                recv.nudge_feedback().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            rres_tx
                .send((recv.net_event_count(), recv.peer_pmtu(), recv.local_pmtu()))
                .unwrap();
            got
        });

        let recv_addr = addr_rx.recv().unwrap();
        let (sres_tx, sres_rx) = mpsc::channel();
        let tx = std::thread::spawn(move || {
            let mut send = ReliableUdpSender::bind("127.0.0.1:0", recv_addr, 8, 2, 8).unwrap();
            // Force a distinct known egress MTU on the sender.
            send.inject_pmtu(1280);
            for i in 0..n {
                while send.flow_blocked() {
                    send.drain_until_acked(Duration::from_millis(50)).ok();
                }
                send.send_item(&i.to_le_bytes()).unwrap();
            }
            send.flush().unwrap();
            send.drain_until_acked(Duration::from_secs(15)).unwrap();
            // Drain the receiver's feedback so its Pmtu frame lands here.
            for _ in 0..60 {
                send.pump_feedback().ok();
                std::thread::sleep(Duration::from_millis(2));
            }
            sres_tx
                .send((send.net_event_count(), send.peer_pmtu(), send.local_pmtu()))
                .unwrap();
        });

        let got = rx.join().unwrap();
        tx.join().unwrap();
        assert_eq!(got, n, "all items delivered");
        let (recv_events, recv_peer_pmtu, recv_local_pmtu) = rres_rx.recv().unwrap();
        let (send_events, send_peer_pmtu, send_local_pmtu) = sres_rx.recv().unwrap();
        // The injected path event registered on the receiver.
        assert!(recv_events >= 1, "receiver path event registered");
        // The injected path event (MTU drop on the sender, plus the inject)
        // registered on the sender too.
        assert!(send_events >= 1, "sender path event registered");
        // Each endpoint's egress MTU rode its frame to the peer, faithfully.
        assert_eq!(
            send_peer_pmtu, recv_local_pmtu,
            "sender learned the receiver's MTU via the feedback Pmtu frame"
        );
        assert_eq!(
            recv_peer_pmtu, send_local_pmtu,
            "receiver learned the sender's MTU via the heartbeat Pmtu frame"
        );
    }
}
