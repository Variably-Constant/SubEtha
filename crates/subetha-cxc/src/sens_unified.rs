//! Unified Sens-O-Matic endpoint: one transport that carries BOTH erasure
//! codes and switches between them mid-stream on the loss the receiver
//! already measures and feeds back.
//!
//! Sens-O-Matic treats the erasure code as a swappable detail (like a cipher
//! suite): the sliding-window Random Linear Code ([`crate::sens_rlc`]) and the
//! block Cauchy Reed-Solomon code ([`crate::udp_bridge`]) deliver every item
//! in order, differing only in HOW they recover loss. Their operating regimes
//! are complementary, and the boundary is a measured loss level:
//!
//!  - **RLC wins at low-to-moderate loss** - incremental forward recovery from
//!    the next repair (no block-wait, no retransmit round trip), so it holds a
//!    low latency tail, and its sliding window carries less overhead than a
//!    block code until loss is dense.
//!  - **RS wins at high sustained loss** - a systematic MDS block code recovers
//!    any `r` erasures per `k + r` shards, the most parity-efficient recovery
//!    once loss is dense. Critically, RLC's adaptive redundancy hard-caps at
//!    one repair per source symbol (50% redundancy, `STEP_MIN = 1` in
//!    [`crate::rlc_control`]), so above the loss its rate law saturates at it
//!    cannot provision enough and its goodput collapses; RS's `r` has no such
//!    ceiling (`k + r <= 256`).
//!
//! The crossover sits at roughly **22-25% loss** when both codes are provisioned
//! for the loss level (RLC's flow window sized to the path BDP, RS's parity
//! provisioned per loss). It is lower on a high-RTT path because RLC's rate-law
//! margin grows with the round trip and drives the code to its redundancy
//! ceiling at a lower loss. The loss-driven switch moves UP to RS at the
//! crossover (~23.5%, `q8 = 60`) and back DOWN to RLC at ~12% (a wide hysteresis
//! band, so a loss level hovering at the boundary does not flap). A persistent
//! RLC flow-block escapes to RS on its own, the backstop for a path whose
//! crossover sits below the threshold, where RLC would stall before the loss
//! reading crosses it.
//!
//! The switch is driven by the FEEDBACK frame's loss byte (`loss_q8`, the
//! forward loss quantized to a `u8` as `loss * 256`), which both codes' senders
//! already receive over the control plane. `CodeSwitchController` applies the
//! threshold with immediate-up / conservative-down hysteresis (the same shape
//! as [`crate::rlc_control::RlcController`]): it raises protection - switching
//! to the stronger high-loss code - the instant the loss sustains above the up
//! threshold, but only relaxes back to RLC after the loss sustains below the
//! down threshold for `hold` ticks, since dropping the stronger code under a
//! brief quiet spell risks a recovery gap.

use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::dgram::{new_demux_queue, DemuxQueue, DgramSock};
use crate::sens_rlc::{SensOMaticRlcReceiver, SensOMaticRlcSender};
use crate::reliable_udp::RejectCounts;
use crate::udp_bridge::{ReliableUdpReceiver, ReliableUdpSender};

/// Which erasure code the unified transport is currently carrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SensCode {
    /// Sliding-window Random Linear Code (low-to-moderate loss, low latency).
    Rlc,
    /// Block Cauchy Reed-Solomon (high sustained loss, parity-efficient).
    Rs,
}

/// How the unified transport selects its erasure code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodePolicy {
    /// Loss-driven with hysteresis. `up_q8` / `down_q8` are forward-loss
    /// thresholds (quantized `loss * 256`, matching the FEEDBACK frame):
    /// switch RLC -> RS when loss sustains above `up_q8`, RS -> RLC when it
    /// sustains below `down_q8`. `up_q8 > down_q8` is the hysteresis band.
    Auto { up_q8: u8, down_q8: u8 },
    /// Force the sliding-window RLC code regardless of loss (operator override).
    ForceRlc,
    /// Force the block Reed-Solomon code regardless of loss (operator override).
    ForceRs,
}

impl CodePolicy {
    /// The default loss-driven policy, thresholds set from the measured crossover
    /// with RS provisioned to cover the loss: switch UP to RS at ~15%
    /// (`q8 = CROSSOVER_LOSS_Q8 = 38`, where RS overtakes RLC on both throughput
    /// and bounded tail latency) and back DOWN to RLC at ~10% (`q8 = 26`). RLC
    /// keeps the sub-crossover regime for its lower TTFD / median; the ~5-point
    /// hysteresis band keeps a loss level hovering at the boundary from flapping
    /// the code.
    pub fn default_auto() -> Self {
        CodePolicy::Auto { up_q8: CROSSOVER_LOSS_Q8, down_q8: 26 }
    }

    /// The code this policy starts a connection on. Auto and ForceRlc start on
    /// RLC (the low-latency primary); ForceRs starts on RS.
    pub fn initial_code(&self) -> SensCode {
        match self {
            CodePolicy::ForceRs => SensCode::Rs,
            CodePolicy::Auto { .. } | CodePolicy::ForceRlc => SensCode::Rlc,
        }
    }
}

/// Loss in q8 (the FEEDBACK frame's `loss * 256`) at the measured crossover
/// where block-RS overtakes sliding-window RLC: ~15% (38/256). RS provisions
/// parity to cover the loss (Encoder::set_parity_covering) and then wins both
/// throughput and bounded tail latency from ~15% up; RLC keeps the low-loss
/// edge (lower TTFD / median, incremental delivery). The earlier 23.5% pin was
/// measured against RS capped at r=8 (33% recovery), which understated RS.
pub const CROSSOVER_LOSS_Q8: u8 = 38;

/// Immediate-up / conservative-down controller that turns a stream of fed-back
/// `loss_q8` samples into code-switch decisions under a [`CodePolicy`].
///
/// Up-switches (to the stronger high-loss RS code) fire the instant the loss
/// sustains above the up threshold for `up_hold` samples; down-switches (back
/// to RLC) require `down_hold` sustained-below samples, a longer streak, so a
/// brief lull does not strip the stronger code while loss is still bursty.
#[derive(Debug, Clone)]
pub struct CodeSwitchController {
    policy: CodePolicy,
    code: SensCode,
    up_streak: u32,
    down_streak: u32,
    up_hold: u32,
    down_hold: u32,
    switches: u64,
    /// Set when a flow-block ESCAPE (not a loss-threshold up-switch) moved to RS:
    /// RLC stalled at this loss, so a down-switch back would just stall again and
    /// flap. The latch suppresses the down-switch after a stall-escape (the loss
    /// estimate at a stall-loss can sit below the down threshold, which would
    /// otherwise pull straight back to a code that cannot keep up).
    escape_latched: bool,
}

impl CodeSwitchController {
    /// A controller under `policy`, starting on the policy's initial code.
    /// `up_hold` consecutive over-threshold samples confirm an up-switch;
    /// `down_hold` (typically larger) under-threshold samples confirm the
    /// relax back to RLC.
    pub fn new(policy: CodePolicy, up_hold: u32, down_hold: u32) -> Self {
        Self {
            policy,
            code: policy.initial_code(),
            up_streak: 0,
            down_streak: 0,
            up_hold: up_hold.max(1),
            down_hold: down_hold.max(1),
            switches: 0,
            escape_latched: false,
        }
    }

    /// A controller with sensible default holds: an up-switch confirms in 3
    /// feedback intervals (loss spiked and held, robust to window noise), a
    /// down-switch in 8 (loss must stay low a while before dropping the
    /// stronger code).
    pub fn with_policy(policy: CodePolicy) -> Self {
        Self::new(policy, 3, 8)
    }

    /// The code currently selected.
    pub fn code(&self) -> SensCode {
        self.code
    }

    /// Total confirmed code switches so far (telemetry).
    pub fn switches(&self) -> u64 {
        self.switches
    }

    /// Feed one fed-back forward-loss sample (`loss_q8 = loss * 256`). Returns
    /// `Some(new_code)` exactly on the sample that confirms a switch, else
    /// `None`. A forced policy never switches.
    pub fn observe(&mut self, loss_q8: u8) -> Option<SensCode> {
        let (up_q8, down_q8) = match self.policy {
            CodePolicy::ForceRlc | CodePolicy::ForceRs => return None,
            CodePolicy::Auto { up_q8, down_q8 } => (up_q8, down_q8),
        };
        match self.code {
            SensCode::Rlc => {
                if loss_q8 >= up_q8 {
                    self.up_streak += 1;
                    self.down_streak = 0;
                    if self.up_streak >= self.up_hold {
                        self.code = SensCode::Rs;
                        self.up_streak = 0;
                        self.switches += 1;
                        return Some(SensCode::Rs);
                    }
                } else {
                    self.up_streak = 0;
                }
            }
            SensCode::Rs => {
                if !self.escape_latched && loss_q8 <= down_q8 {
                    self.down_streak += 1;
                    self.up_streak = 0;
                    if self.down_streak >= self.down_hold {
                        self.code = SensCode::Rlc;
                        self.down_streak = 0;
                        self.switches += 1;
                        return Some(SensCode::Rlc);
                    }
                } else {
                    self.down_streak = 0;
                }
            }
        }
        None
    }

    /// Align the controller to `to` for a switch driven OUTSIDE `observe` (the
    /// flow-block escape), counting it and resetting the hysteresis streaks so the
    /// band restarts from the new code. Returns whether it switched: a forced
    /// policy stays put (returns `false`), as does an already-on-`to` controller.
    pub fn force(&mut self, to: SensCode) -> bool {
        if matches!(self.policy, CodePolicy::ForceRlc | CodePolicy::ForceRs) {
            return false;
        }
        if self.code != to {
            self.code = to;
            self.switches += 1;
            self.up_streak = 0;
            self.down_streak = 0;
            // A stall-escape to RS latches the code: RLC could not keep up at this
            // loss, so suppress the down-switch that would flap straight back. A
            // deliberate return to RLC (operator force) re-arms the down direction.
            self.escape_latched = to == SensCode::Rs;
            true
        } else {
            false
        }
    }
}

// ---------------------------------------------------------------------------
// CODE_SWITCH control frame + first-byte demux
// ---------------------------------------------------------------------------

/// CODE_SWITCH control-frame type byte. Disjoint from RS data (1) / control
/// (4), the RLC frames (10..=14), and QUIC (first byte has 0x40 set), so one
/// socket demuxes all of them unambiguously by the first wire byte.
pub const PKT_CODE_SWITCH: u8 = 9;

/// Wire: `[9][boundary u64-le][to_code u8]`. `boundary` is the count of items
/// the sender has delivered across both codes up to the switch; the receiver
/// keeps draining the old decoder until its cumulative delivery reaches it,
/// then activates `to_code`. 10 bytes.
fn encode_code_switch(boundary: u64, to: SensCode) -> [u8; 10] {
    let mut v = [0u8; 10];
    v[0] = PKT_CODE_SWITCH;
    v[1..9].copy_from_slice(&boundary.to_le_bytes());
    v[9] = match to {
        SensCode::Rlc => 0,
        SensCode::Rs => 1,
    };
    v
}

fn decode_code_switch(buf: &[u8]) -> Option<(u64, SensCode)> {
    if buf.len() < 10 || buf[0] != PKT_CODE_SWITCH {
        return None;
    }
    let boundary = u64::from_le_bytes(buf[1..9].try_into().ok()?);
    let to = if buf[9] == 0 { SensCode::Rlc } else { SensCode::Rs };
    Some((boundary, to))
}

/// One CODE_SWITCH the demux reader observed (receiver side).
pub(crate) type SwitchSignal = Arc<Mutex<Option<(u64, SensCode)>>>;

/// Unified raw-loss feedback frame type byte. Disjoint from RS (1 / 4), RLC
/// (10..=14), CODE_SWITCH (9), and QUIC (first byte 0x40 set).
pub const PKT_UNIFIED_FB: u8 = 8;

/// Wire: `[8][received u64-le]` - the receiver's cumulative count of forward
/// data/repair datagrams seen. The sender pairs it with its own sent count to
/// get the true raw channel loss, independent of either code's recovery.
fn encode_unified_fb(received: u64) -> [u8; 9] {
    let mut v = [0u8; 9];
    v[0] = PKT_UNIFIED_FB;
    v[1..9].copy_from_slice(&received.to_le_bytes());
    v
}

fn decode_unified_fb(buf: &[u8]) -> Option<u64> {
    if buf.len() < 9 || buf[0] != PKT_UNIFIED_FB {
        return None;
    }
    Some(u64::from_le_bytes(buf[1..9].try_into().ok()?))
}

/// How often the receiver reports its cumulative received-datagram count.
const UNIFIED_FB_PERIOD: Duration = Duration::from_millis(50);

/// How long a peer keeps receiving raw-loss feedback after its last
/// datagram. Covers a sparse sender's cadence with wide margin (600 feedback
/// periods) and bounds the outbound set to peers that recently spoke, so a
/// spoofed source address ages out instead of accumulating.
pub const FB_PEER_RETENTION: Duration = Duration::from_secs(30);

/// The deadline [`UnifiedSensSender::finish`] drains for.
pub const DEFAULT_FINISH_DEADLINE: Duration = Duration::from_secs(120);

/// Counter slots the demux reader publishes.
const DEMUX_STAT_SLOTS: usize = 7;
/// Nanoseconds since the reader started, stamped at the top of every loop:
/// the heartbeat a staleness check reads.
const DEMUX_SLOT_LAST_ITER: usize = 5;
/// Socket errors that were neither `WouldBlock` nor `TimedOut`.
const DEMUX_SLOT_ERRORS: usize = 6;
/// Minimum datagrams sent in a sample window before the raw-loss estimate is
/// trusted (a tiny window is too noisy to switch on).
const MIN_LOSS_SAMPLE: u64 = 30;

/// Route one inbound Sens datagram (already classified as non-QUIC) to the
/// matching per-code queue by its first byte, tallying forward data/repair for
/// the raw-loss numerator and capturing CODE_SWITCH / UNIFIED_FB control. Shared
/// by the standalone demux reader thread and the one-port QUIC demux socket.
#[allow(clippy::too_many_arguments)]
pub(crate) fn route_sens_inbound(
    data: Vec<u8>,
    from: SocketAddr,
    kts: Option<i128>,
    rlc_q: &DemuxQueue,
    rs_q: &DemuxQueue,
    switch_signal: Option<&SwitchSignal>,
    fb_received: Option<&AtomicU64>,
    recv_counter: Option<&AtomicU64>,
    hs_q: Option<&DemuxQueue>,
) {
    let b0 = data.first().copied().unwrap_or(0);
    if let Some(c) = recv_counter
        && (b0 == 1 || b0 == 10 || b0 == 11)
    {
        c.fetch_add(1, Ordering::Relaxed);
    }
    if b0 == 1 || b0 == 4 {
        rs_q.lock().unwrap().push_back((data, from, kts));
    } else if (10..=14).contains(&b0)
        || b0 == crate::sens_rlc::PKT_RLC_PATH_CHALLENGE
        || b0 == crate::sens_rlc::PKT_RLC_PATH_RESPONSE
    {
        // The RLC data range plus the two path-validation frames. Named
        // rather than folded into the range, which would swallow the crypto
        // types the next arm routes to the handshake driver.
        rlc_q.lock().unwrap().push_back((data, from, kts));
    } else if (b0 == 15 || b0 == 16)
        && let Some(hq) = hs_q
    {
        // PKT_RLC_CRYPTO (15) / PKT_RLC_CRYPTO_ACK (16): the one-port Sens TLS
        // handshake. The standalone path completes its handshake before the demux
        // reader starts, so it passes `None` and these never arrive there; the
        // one-port path routes them to the handshake driver's queue.
        hq.lock().unwrap().push_back((data, from, kts));
    } else if b0 == PKT_UNIFIED_FB
        && let (Some(fb), Some(v)) = (fb_received, decode_unified_fb(&data))
    {
        fb.store(v, Ordering::Relaxed);
    } else if b0 == PKT_CODE_SWITCH
        && let (Some(sig), Some(p)) = (switch_signal, decode_code_switch(&data))
    {
        *sig.lock().unwrap() = Some(p);
    }
}

/// splitmix64 step: a cheap, seedable PRNG for the demux loss injector.
fn next_rand(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Spawn the demux reader: read the one real socket and route each datagram to
/// the matching code's queue by its first byte. The classification is a single
/// byte compare per datagram (the hot path stays branch-light; the per-code
/// decoders carry their own GF(256) SIMD). A `switch_signal` (receiver side)
/// captures CODE_SWITCH frames; on the sender side it is `None` and any stray
/// CODE_SWITCH is dropped.
#[allow(clippy::too_many_arguments)]
fn spawn_demux(
    sock: UdpSocket,
    rlc_q: DemuxQueue,
    rs_q: DemuxQueue,
    switch_signal: Option<SwitchSignal>,
    recv_counter: Option<Arc<AtomicU64>>,
    fb_received: Option<Arc<AtomicU64>>,
    loss_pct: u32,
    seed: u64,
    stop: Arc<AtomicBool>,
    stats: Option<Arc<[AtomicU64; DEMUX_STAT_SLOTS]>>,
    demux_start: Instant,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let stop_report = Arc::clone(&stop);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(s) = &stats {
            s[4].store(Arc::as_ptr(&rlc_q) as usize as u64, Ordering::Relaxed);
        }
        let mut buf = vec![0u8; 2048];
        // Every peer heard from inside FB_PEER_RETENTION, with the instant it
        // last spoke. Feedback goes to all of them, not just the most recent
        // speaker, so a sparse sender's loss estimate matures instead of
        // starving whenever another peer speaks after it.
        let mut peers: Vec<(SocketAddr, Instant)> = Vec::new();
        let mut last_fb = Instant::now();
        let mut rng = seed;
        // Set while the socket is returning real errors, so entering and
        // leaving that state each report exactly once instead of either
        // going unmentioned or flooding stderr every 200us.
        let mut erroring = false;
        while !stop.load(Ordering::Relaxed) {
            if let Some(s) = &stats {
                s[0].fetch_add(1, Ordering::Relaxed);
                // The heartbeat a watchdog reads: a reader wedged in recv or
                // blocked pushing to a queue stops advancing this.
                s[DEMUX_SLOT_LAST_ITER]
                    .store(demux_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            let io_ok = match crate::dgram::udp_recv_with_kts(&sock, &mut buf) {
                Ok((n, from, kts)) if n > 0 => {
                    let b0 = buf[0];
                    if let Some(s) = &stats {
                        s[1].fetch_add(1, Ordering::Relaxed);
                        if (10..=14).contains(&b0)
                            || b0 == crate::sens_rlc::PKT_RLC_PATH_CHALLENGE
                            || b0 == crate::sens_rlc::PKT_RLC_PATH_RESPONSE
                        {
                            s[3].fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    match peers.iter_mut().find(|(a, _)| *a == from) {
                        Some((_, seen)) => *seen = Instant::now(),
                        None => peers.push((from, Instant::now())),
                    }
                    // A path challenge is a verbatim echo needing no session
                    // state, so it is answered here at wire latency and kept
                    // out of the queue: admission completes in one round trip
                    // whatever the pump's cadence, and a challenge burst
                    // cannot pile ahead of control frames in FIFO order.
                    if b0 == crate::sens_rlc::PKT_RLC_PATH_CHALLENGE
                        && n >= crate::sens_rlc::PATH_FRAME_LEN
                    {
                        let mut resp = Vec::with_capacity(crate::sens_rlc::PATH_FRAME_LEN);
                        resp.push(crate::sens_rlc::PKT_RLC_PATH_RESPONSE);
                        resp.extend_from_slice(&buf[1..crate::sens_rlc::PATH_FRAME_LEN]);
                        sock.send_to(&resp, from).ok();
                        continue;
                    }
                    // Uniform link-loss injection on the forward data/repair
                    // stream (RS data 1, RLC data 10 / repair 11): drop BEFORE
                    // counting or routing, so the raw-loss estimate AND the codes
                    // both see a realistic lossy link. Control frames pass.
                    let is_fwd = b0 == 1 || b0 == 10 || b0 == 11;
                    let dropped =
                        loss_pct > 0 && is_fwd && (next_rand(&mut rng) % 100) < loss_pct as u64;
                    if !dropped {
                        // QUIC (0x40 bit set) and unknown first bytes are dropped
                        // by route_sens_inbound; the one-port quinn demux consumes
                        // QUIC separately.
                        route_sens_inbound(
                            buf[..n].to_vec(),
                            from,
                            kts,
                            &rlc_q,
                            &rs_q,
                            switch_signal.as_ref(),
                            fb_received.as_deref(),
                            recv_counter.as_deref(),
                            // Standalone path: the handshake completed before this
                            // reader started, so no crypto frames arrive here.
                            None,
                        );
                    }
                    true
                }
                Ok(_) => true,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Some(s) = &stats {
                        s[2].fetch_add(1, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_micros(100));
                    true
                }
                Err(e) if e.kind() == io::ErrorKind::TimedOut => true,
                Err(e) => {
                    if let Some(s) = &stats {
                        s[DEMUX_SLOT_ERRORS].fetch_add(1, Ordering::Relaxed);
                    }
                    if !erroring {
                        erroring = true;
                        eprintln!("subetha: demux reader socket error: {e}");
                    }
                    std::thread::sleep(Duration::from_micros(200));
                    false
                }
            };
            if io_ok && erroring {
                erroring = false;
                eprintln!("subetha: demux reader socket recovered");
            }
            // Receiver: report the cumulative received-datagram count back so
            // the sender derives the true raw channel loss (sent vs received),
            // which neither code's post-recovery feedback reveals.
            if let Some(c) = &recv_counter
                && last_fb.elapsed() >= UNIFIED_FB_PERIOD
            {
                last_fb = Instant::now();
                peers.retain(|(_, seen)| seen.elapsed() < FB_PEER_RETENTION);
                let frame = encode_unified_fb(c.load(Ordering::Relaxed));
                for (dst, _) in &peers {
                    sock.send_to(&frame, *dst).ok();
                }
            }
        }
        }));
        // A reader that stops while nobody asked it to leaves a process that
        // looks healthy and has stopped hearing the world, so both ways out
        // announce themselves.
        if !stop_report.load(Ordering::Relaxed) {
            eprintln!("subetha: demux reader exited without a stop request");
        }
        if let Err(p) = r {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            eprintln!("subetha: demux reader panicked: {msg}");
        }
    })
}

/// How often the sender samples the fed-back loss and asks the controller for a
/// switch. Time-based (not per-item) so the controller's hold counts track the
/// receiver's ~10ms feedback cadence rather than the item rate.
const SWITCH_SAMPLE_PERIOD: Duration = Duration::from_millis(50);
/// Warmup before the switch is evaluated: the in-flight window ramps from 0 to
/// the flow window at connection start, and that growth reads as loss; wait for
/// it to stabilize so the ramp does not trip a spurious switch.
const SWITCH_WARMUP: Duration = Duration::from_millis(1000);
/// Feedback windows accumulated AFTER the warmup before the loss estimate is
/// trusted to move the code. The decaying accumulator is cold at warmup-end (its
/// first window's raw ratio dominates), so a start-of-stream retransmit burst
/// reads as a spike that crosses the up threshold and flaps the code. Holding the
/// switch until a few windows have decayed in lets the estimate mature first.
const MIN_ACCUM_WINDOWS: u32 = 6;
/// Drain deadline for a code handover (the in-flight tail of the old code must
/// be delivered before the new code starts, for in-order delivery).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// How long RLC's DELIVERY FRONTIER may stay stuck (no item delivered while the
/// send window is full) before the transport gives up on RLC and migrates to RS.
/// This is the genuine-deadlock backstop: a frontier that does not advance for
/// this long means RLC cannot decode the loss it is seeing (extreme loss past its
/// redundancy ceiling), which the loss-driven `maybe_switch` cannot catch because
/// a stalled sender produces no fresh loss sample. It is measured against frontier
/// progress (the send loop resets the timer whenever a delivery lands), so a
/// recoverable hard gap at sub-ceiling loss does NOT trip it - only a true stall.
/// Measured against frontier progress, so it fires fast (the stalling unified RLC
/// needs prompt rescue - a slower value starves it into a multi-second stall).
const RLC_BLOCK_ESCAPE: Duration = Duration::from_millis(750);
/// Drain deadline for the flow-block escape specifically: the stuck window's
/// frontier is retransmitted (over a high-loss link, so each copy may also be
/// lost) until fully delivered, so it must be generous enough to land every item
/// before RS takes over (no gap = in-order delivery preserved).
const ESCAPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Hard cap on the sender-side replay ring (items). The ring normally holds only
/// the un-acked tail `[acked_through, items_total)` (evicted as RLC confirms
/// delivery), but at extreme loss that tail can grow; this bounds the memory. If
/// the un-acked tail ever exceeds the cap, the RLC->RS handover falls back to
/// draining RLC so no item is dropped. 65536 * symbol covers the worst observed
/// 30%-loss tail with headroom.
const SENT_RING_CAP: usize = 65536;
/// Recycled replay-ring buffers held for reuse. A trimmed (delivered) buffer is
/// returned here instead of freed, and the next seal reuses it instead of
/// allocating - so the per-item path does no heap alloc/free in steady state.
/// Sized to the in-flight working set (a few flow-windows) rather than the full
/// ring cap: the pool only needs to bridge trim-tail to send-head, and capping it
/// keeps idle memory bounded when the ring shrinks. At small item sizes (where the
/// item rate, and thus the alloc churn, is highest) this removes ~190k alloc/free
/// pairs per second from the hot path.
const RING_POOL_CAP: usize = 1024;
/// CODE_SWITCH is a one-off control frame sent on the (drained, quiet) path at
/// the switch point; send it a few times so a single drop does not strand the
/// receiver on the old decoder.
const CODE_SWITCH_REPEATS: usize = 6;

// ---------------------------------------------------------------------------
// Unified sender
// ---------------------------------------------------------------------------

/// Background reporter for the one-port path: periodically send the cumulative
/// received-datagram count to the Sens peer (the raw-loss numerator). The QUIC
/// demux socket feeds the receiver's queues, so there is no demux thread to do
/// it; this small thread covers just the feedback send.
fn spawn_fb_reporter(
    sock: Arc<UdpSocket>,
    recv_counter: Arc<AtomicU64>,
    peer: Arc<Mutex<Option<SocketAddr>>>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(UNIFIED_FB_PERIOD);
            if let Some(dst) = *peer.lock().unwrap() {
                let frame = encode_unified_fb(recv_counter.load(Ordering::Relaxed));
                sock.send_to(&frame, dst).ok();
            }
        }
    })
}

/// Construction parameters shared by the unified sender and receiver.
#[derive(Debug, Clone, Copy)]
pub struct UnifiedConfig {
    /// Erasure-code selection policy (loss-driven Auto, or a forced code).
    pub policy: CodePolicy,
    /// Item / symbol size in bytes (matches the application's record size).
    pub symbol_len: usize,
    /// Reed-Solomon block geometry: `k` data shards.
    pub k: usize,
    /// Reed-Solomon base parity shards `r` (the receiver provisions per loss).
    pub r: usize,
    /// RLC sender flow window (outstanding source symbols); 0 = transport
    /// default. Size it to the path BDP so RLC fills the pipe (the fair-A/B
    /// config; the default caps RLC ~2x below its capability on a high-BDP path).
    pub rlc_flow_window: u32,
    /// Receiver-side diagnostic loss injection (percent, 0 = off) applied to
    /// BOTH decoders, with `seed` for reproducibility. Drives the loss-based
    /// switch without a real lossy link.
    pub debug_loss: u32,
    /// Seed for the reproducible `debug_loss` drop sequence.
    pub seed: u64,
    /// RLC repair cadence: one repair every `rlc_step` source symbols (redundancy
    /// `1/(rlc_step+1)`). The starting value; the adaptive controller retunes it
    /// per measured loss unless `rlc_static` pins it.
    pub rlc_step: u16,
    /// Pin the RLC coding parameters (disable the adaptive controller), holding a
    /// fixed code rate instead of letting the sensing plane retune window / step /
    /// density. The adaptive controller's disable-on-clean state drops coding
    /// entirely on a quiet assessment and then pays an ARQ round trip on the next
    /// loss; pinning trades that latency risk for a constant redundancy.
    pub rlc_static: bool,
}

impl UnifiedConfig {
    /// Defaults: loss-driven Auto policy, MTU-sized items, RS (8, 2), RLC flow
    /// window sized for a filled BDP, no injected loss.
    pub fn new(symbol_len: usize) -> Self {
        Self {
            policy: CodePolicy::default_auto(),
            symbol_len,
            k: 8,
            r: 2,
            rlc_flow_window: 4096,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        }
    }
}

/// Unified Sens-O-Matic sender: carries items over whichever erasure code the
/// loss-driven controller selects, switching RLC <-> RS mid-stream via a
/// drain-barrier handover. One real socket is shared by both codes through
/// per-code demux queues fed by a background reader.
pub struct UnifiedSensSender {
    real: Arc<UdpSocket>,
    peer: SocketAddr,
    rlc: SensOMaticRlcSender,
    rs: ReliableUdpSender,
    active: SensCode,
    ctrl: CodeSwitchController,
    /// Cumulative items handed to the application across both codes (the switch
    /// boundary the receiver keys on).
    items_total: u64,
    /// Cumulative entries into `send_item`, counted before any other
    /// statement in the method.
    send_item_calls: u64,
    /// Demux reader loop counters: `[iterations, recv_ok, would_block,
    /// rlc_frames_routed, rlc_queue_ptr, last_iter_nanos, socket_errors]`,
    /// written by the reader thread; slot 4 holds the pushed-to queue's
    /// `Arc` address.
    demux_stats: Arc<[AtomicU64; DEMUX_STAT_SLOTS]>,
    /// The reader's clock origin, so `last_iter_nanos` reads as an age.
    demux_start: Instant,
    last_sample: Instant,
    /// Connection start, for the switch-evaluation warmup.
    started: Instant,
    /// Datagrams sent through both codes' demux sockets (raw-loss numerator).
    sent_counter: Arc<AtomicU64>,
    /// Receiver's last-reported cumulative received-datagram count.
    fb_received: Arc<AtomicU64>,
    /// Sent / received baselines captured at the previous evaluated window.
    prev_sent: u64,
    prev_received: u64,
    /// Size-weighted decaying raw-loss estimate (-1 = uninitialized). Decay the
    /// lost / sent COUNTS (`loss_acc` / `sent_acc`) and take their ratio, rather
    /// than EWMA-ing per-window ratios: a small feedback window with one drop
    /// reads a spuriously high ratio, and an equal-weight EWMA of ratios over-
    /// weights it, inflating the estimate at low loss (3% read as ~11%). Weighting
    /// by datagram count makes the estimate track the true channel loss.
    ewma_loss: f64,
    /// Decaying sums of lost and sent forward datagrams (the size-weighted
    /// estimate's numerator / denominator); their ratio is `ewma_loss`.
    loss_acc: f64,
    sent_acc: f64,
    /// Feedback windows accumulated since the warmup ended. The switch is gated on
    /// this reaching `MIN_ACCUM_WINDOWS` so a cold accumulator cannot flap the code.
    post_warm_windows: u32,
    /// Recently-sent item payloads, kept so a code switch can RESEND the un-acked
    /// tail over the new code instead of slowly draining the old one. Holds the
    /// global index range `[ring_base, items_total)`; the front is evicted once
    /// RLC confirms delivery (its `acked_through`) and is hard-capped so a stalled
    /// receiver cannot grow it without bound. This is the sender-side replay ring.
    sent_ring: VecDeque<Vec<u8>>,
    /// Global index of `sent_ring[0]` (the oldest retained item).
    ring_base: u64,
    /// Recycled wire-payload buffers (capacity retained, length reset). Trimmed
    /// ring buffers land here; the next seal pops one instead of allocating.
    ring_pool: Vec<Vec<u8>>,
    /// Unified AEAD record layer (TLS feature). When set, every item payload is
    /// sealed before it enters the replay ring and goes to either code, so the
    /// RLC<->RS switch is crypto-transparent and the wire is confidential. The
    /// seal packet number is the item's global index (sealed once, in order), so
    /// a resend reuses it and the receiver opens by index.
    #[cfg(feature = "tls")]
    crypto: Option<crate::rlc_crypto::CryptoState>,
    stop: Arc<AtomicBool>,
    demux: Option<JoinHandle<()>>,
}

impl UnifiedSensSender {
    /// Bind a local socket, connect to `peer`, and bring up both codes sharing
    /// it. Starts on the policy's initial code (RLC for Auto / ForceRlc).
    pub fn connect<A: ToSocketAddrs>(local: A, peer: SocketAddr, cfg: UnifiedConfig) -> io::Result<Self> {
        let udp = UdpSocket::bind(local)?;
        udp.set_nonblocking(true)?;
        Self::assemble(udp, peer, cfg, 0)
    }

    /// Like [`connect`](Self::connect) but runs a TLS 1.3 handshake to `peer`
    /// first and AEAD-seals every item: the auto-switching transport made
    /// confidential for an untrusted WAN. The handshake completes before the
    /// demux reader takes the socket, so its frames never reach the data path.
    #[cfg(feature = "tls")]
    pub fn connect_tls<A: ToSocketAddrs>(
        local: A,
        peer: SocketAddr,
        cfg: UnifiedConfig,
        tls: std::sync::Arc<rustls::ClientConfig>,
    ) -> io::Result<Self> {
        let udp = UdpSocket::bind(local)?;
        udp.set_nonblocking(true)?;
        let mut cs = crate::rlc_crypto::CryptoState::new_client(tls)
            .map_err(io::Error::other)?;
        let hs = DgramSock::from_udp(udp.try_clone()?);
        crate::sens_rlc::drive_handshake(&hs, Some(peer), &mut cs, true)?;
        let mut s = Self::assemble(udp, peer, cfg, crate::rlc_crypto::TAG_LEN)?;
        s.crypto = Some(cs);
        Ok(s)
    }

    /// Build the sender over an already-bound (and, for TLS, already-handshaked)
    /// socket: bring up both codes sharing it and spawn the demux reader.
    fn assemble(
        udp: UdpSocket,
        peer: SocketAddr,
        cfg: UnifiedConfig,
        seal_overhead: usize,
    ) -> io::Result<Self> {
        // Both codes carry the wire payload, which is the item plus the AEAD tag
        // when TLS is on; size their symbols for the sealed width so pack_symbol
        // and the RS shard split never overflow.
        let wire_sym = cfg.symbol_len + seal_overhead;
        // Left UNCONNECTED: the per-code demux sockets send via send_to(peer),
        // and send_to on a connected socket is rejected on Windows. The demux
        // reader still only ever hears from `peer` on this private socket.
        // A clone for the demux thread: UdpSocket is Send, DgramSock is not
        // (its io_uring variant is not Send), so the thread holds the raw socket.
        let thread_sock = udp.try_clone()?;
        thread_sock.set_nonblocking(true)?;
        let real = Arc::new(udp);
        let rlc_q = new_demux_queue();
        let rs_q = new_demux_queue();
        let sent_counter = Arc::new(AtomicU64::new(0));
        let fb_received = Arc::new(AtomicU64::new(0));

        let mut rlc = SensOMaticRlcSender::bind("0.0.0.0:0", peer, 32, cfg.rlc_step as usize, 15, wire_sym)?;
        if cfg.rlc_flow_window > 0 {
            rlc = rlc.with_flow_window(cfg.rlc_flow_window);
        }
        if cfg.rlc_static {
            rlc = rlc.with_static_params();
        } else {
            // The RLC leg is the latency-priority code (the switch hands bulk /
            // high-loss traffic to block-RS). Keep a light FEC floor on at all
            // times so an isolated loss recovers in-window instead of falling to
            // an ARQ round trip that head-of-line-stalls the in-order stream.
            rlc = rlc.with_latency_priority();
        }
        let rlc_sock = DgramSock::demux_counted(
            Arc::clone(&real),
            Arc::clone(&rlc_q),
            Arc::clone(&sent_counter),
        );
        rlc_sock.connect(peer).ok();
        rlc.set_sock(rlc_sock);

        let mut rs = ReliableUdpSender::bind("0.0.0.0:0", peer, cfg.k, cfg.r, wire_sym)?;
        let rs_sock = DgramSock::demux_counted(
            Arc::clone(&real),
            Arc::clone(&rs_q),
            Arc::clone(&sent_counter),
        );
        rs_sock.connect(peer).ok();
        rs.set_sock(rs_sock);

        let stop = Arc::new(AtomicBool::new(false));
        let demux_stats: Arc<[AtomicU64; DEMUX_STAT_SLOTS]> =
            Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let demux_start = Instant::now();
        let demux = spawn_demux(
            thread_sock,
            rlc_q,
            rs_q,
            None,
            None,
            Some(Arc::clone(&fb_received)),
            0,
            1,
            Arc::clone(&stop),
            Some(Arc::clone(&demux_stats)),
            demux_start,
        );

        Ok(Self {
            real,
            peer,
            rlc,
            rs,
            demux_stats,
            demux_start,
            active: cfg.policy.initial_code(),
            ctrl: CodeSwitchController::with_policy(cfg.policy),
            items_total: 0,
            send_item_calls: 0,
            last_sample: Instant::now(),
            started: Instant::now(),
            sent_counter,
            fb_received,
            prev_sent: 0,
            prev_received: 0,
            ewma_loss: -1.0,
            loss_acc: 0.0,
            sent_acc: 0.0,
            post_warm_windows: 0,
            sent_ring: VecDeque::new(),
            ring_base: 0,
            ring_pool: Vec::new(),
            #[cfg(feature = "tls")]
            crypto: None,
            stop,
            demux: Some(demux),
        })
    }

    /// Fill `buf` (cleared, capacity reused) with the wire payload for `item`:
    /// AEAD-sealed in place (TLS) or the raw bytes. Sealed once, in send order, so
    /// the packet number equals the item's global index. Reusing a pooled `buf`
    /// keeps the per-item send path allocation-free in steady state.
    fn seal_into(&self, item: &[u8], buf: &mut Vec<u8>) -> io::Result<()> {
        buf.clear();
        buf.extend_from_slice(item);
        #[cfg(feature = "tls")]
        if let Some(cs) = &self.crypto {
            cs.seal(buf).map_err(io::Error::other)?;
        }
        Ok(())
    }

    /// The code currently transmitting.
    pub fn active_code(&self) -> SensCode {
        self.active
    }

    /// Confirmed code switches so far.
    pub fn switches(&self) -> u64 {
        self.ctrl.switches()
    }

    /// The RLC leg's live coding parameters `(window, step, dt, coding_on)`
    /// (telemetry: shows what the adaptive controller settled at vs the baseline).
    pub fn rlc_coding_params(&self) -> (u16, u16, u8, bool) {
        self.rlc.coding_params()
    }

    /// Times the RLC leg's coding parameters changed under feedback (telemetry).
    pub fn rlc_adapt_count(&self) -> u64 {
        self.rlc.adapt_count()
    }

    /// The switch controller's current EWMA raw-loss estimate (sent-vs-received
    /// datagrams), 0.0..1.0, or a negative value before the first sample. This is
    /// the signal the up/down thresholds compare against, so it shows whether the
    /// estimate tracks the true channel loss (telemetry).
    pub fn raw_loss_estimate(&self) -> f64 {
        self.ewma_loss
    }

    /// Cumulative (datagrams sent through both codes' demux sockets, receiver's
    /// last-reported forward-received count). The raw inputs to the loss estimate;
    /// `(sent - recv) / sent` should equal the channel loss if the counts are
    /// clean (telemetry to find a sent-side over-count / recv-side under-count).
    pub fn raw_sent_recv(&self) -> (u64, u64) {
        (
            self.sent_counter.load(Ordering::Relaxed),
            self.fb_received.load(Ordering::Relaxed),
        )
    }

    /// The RLC sender's transmit-side probe: `(last_sid,
    /// wire_datagrams, acked_through, outstanding)`. Splits a stall
    /// between "items never packed" (`last_sid` frozen), "packed but
    /// never handed to the socket" (`wire_datagrams` frozen), and
    /// "handed to the socket but never acknowledged" (`outstanding`
    /// growing with `acked_through` frozen).
    pub fn rlc_tx_probe(&self) -> (u32, u64, u32, usize) {
        self.rlc.tx_probe()
    }

    /// Routing probe: `(active code, RS unacked blocks, items accepted
    /// by send_item, send_item entries)`. `send_item_calls` counts
    /// every entry into `send_item` before any other statement, so
    /// `send_item_calls > items_total` measures Ok-returning exits
    /// above the accept point, and `send_item_calls` frozen means the
    /// method was never invoked on this instance.
    pub fn route_probe(&self) -> (SensCode, usize, u64, u64) {
        (self.active, self.rs.pending_len(), self.items_total, self.send_item_calls)
    }

    /// The RLC sender's control-plane arrivals: `(naks_seen, acks_seen,
    /// feedback_recv)`, counted as the pump processes each frame.
    pub fn rlc_ctrl_probe(&self) -> (u64, u64, u64) {
        self.rlc.ctrl_probe()
    }

    /// The local address of the shared real socket - the port this
    /// sender's datagrams leave from and its demux reads.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.real.local_addr()
    }

    /// Whether the demux reader thread is still running. `false` means
    /// no inbound frame reaches either code's queue or the feedback
    /// counter again; a panic report is on stderr.
    pub fn demux_alive(&self) -> bool {
        self.demux.as_ref().is_some_and(|h| !h.is_finished())
    }

    /// How long since the demux reader last completed a loop. A reader
    /// wedged inside its recv or blocked pushing to a queue is alive by
    /// [`demux_alive`](Self::demux_alive) and stale by this, which is the
    /// pair that separates a healthy idle socket from a deaf one. Compare
    /// it against the reader's own cadence: an idle reader still loops
    /// every 100us, so anything past a few milliseconds is wedged.
    pub fn demux_stale_for(&self) -> Duration {
        let last = self.demux_stats[DEMUX_SLOT_LAST_ITER].load(Ordering::Relaxed);
        let now = self.demux_start.elapsed().as_nanos() as u64;
        Duration::from_nanos(now.saturating_sub(last))
    }

    /// Socket errors the demux reader met that were neither `WouldBlock`
    /// nor a read timeout. Each transition into and out of the erroring
    /// state is also reported on stderr.
    pub fn demux_errors(&self) -> u64 {
        self.demux_stats[DEMUX_SLOT_ERRORS].load(Ordering::Relaxed)
    }

    /// Demux reader loop counters: `(iterations, recv_ok, would_block,
    /// rlc_frames_routed)`. Iterations frozen with the thread alive is
    /// a reader blocked inside the recv; iterations climbing with
    /// recv_ok frozen is a socket no datagram reaches; recv_ok climbing
    /// with routed frozen is a frame the routing arms refuse.
    pub fn demux_probe(&self) -> (u64, u64, u64, u64) {
        (
            self.demux_stats[0].load(Ordering::Relaxed),
            self.demux_stats[1].load(Ordering::Relaxed),
            self.demux_stats[2].load(Ordering::Relaxed),
            self.demux_stats[3].load(Ordering::Relaxed),
        )
    }

    /// The push/pop seam across the RLC queue: `(push_side_queue_ptr,
    /// pop_side)` where `pop_side` is the pump socket's
    /// `(pop_attempts, pop_yields, queue_ptr, queue_len)`. The two
    /// pointers differing is a construction fork - the reader pushes a
    /// queue the pump never reads. `pop_attempts` frozen means the pump
    /// never polls its socket; `queue_len` growing means frames pile
    /// unread on one shared queue.
    pub fn queue_seam_probe(&self) -> (u64, Option<(u64, u64, u64, u64)>) {
        (
            self.demux_stats[4].load(Ordering::Relaxed),

            self.rlc.sock_probe(),
        )
    }

    /// Frame types the RLC pump handled outside the counted control
    /// arms: `(challenges_echoed, default_arm_drops,
    /// last_dropped_byte)`. Pops that appear in neither ctrl_probe nor
    /// here were sealed frames skipped under TLS.
    pub fn rlc_pump_types(&self) -> (u64, u64, u8) {
        self.rlc.pump_types()
    }

    /// Send one item over the active code, then periodically sample the fed-back
    /// loss and switch codes if the controller calls for it. The item is recorded
    /// in the replay ring so a switch can resend the un-acked tail over the new
    /// code rather than draining the old one.
    pub fn send_item(&mut self, item: &[u8]) -> io::Result<()> {
        self.send_item_calls += 1;
        // Env-gated entry trace (`SUBETHA_SEND_TRACE=1`): the compiled
        // artifact reports its own execution on stderr, capped per
        // instance so a hot sender cannot flood a log.
        if self.send_item_calls <= 32 {
            static TRACE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
            if *TRACE.get_or_init(|| std::env::var_os("SUBETHA_SEND_TRACE").is_some_and(|v| v == "1")) {
                eprintln!(
                    "subetha: send_item enter self={:p} len={} calls={}",
                    self, item.len(), self.send_item_calls,
                );
            }
        }
        // Seal to the wire payload once (the packet number is this item's global
        // index); both codes carry it and the replay ring stores it, so a resend
        // reuses the same packet number and the switch is crypto-transparent. Seal
        // into a recycled buffer so the hot path does no per-item heap alloc.
        let mut payload = self.ring_pool.pop().unwrap_or_default();
        self.seal_into(item, &mut payload)?;
        match self.active {
            SensCode::Rlc => {
                // Own RLC's flow-window wait here (via the non-blocking
                // try_send_item) instead of letting rlc.send_item block out of
                // sight: when the window will not clear, RLC cannot decode the
                // loss it is seeing (extreme loss past its redundancy ceiling), so
                // a persistent block IS the trigger to migrate to RS. The loss-
                // driven maybe_switch cannot catch this - a stalled sender emits no
                // fresh loss sample, and the stall arrives inside the startup
                // warmup. The handover resends the un-acked tail over RS (from the
                // replay ring), so no slow RLC drain is needed.
                // Progress-aware deadlock detection: escape only when RLC's
                // delivery frontier is STUCK for RLC_BLOCK_ESCAPE, not merely when
                // a single send flow-blocks while RLC is still delivering (slow but
                // recovering). A blocked-but-advancing frontier is RLC working
                // through loss at its own pace - that is the loss-threshold's job to
                // switch on, not the deadlock backstop's; escaping there flaps the
                // code (escape to RS, then the accurate loss estimate, being below
                // the down threshold, switches straight back).
                let mut escape_start = Instant::now();
                let mut last_acked = self.rlc.acked_through();
                loop {
                    if self.rlc.try_send_item(&payload)? {
                        break;
                    }
                    self.rlc.pump_once()?;
                    let acked_now = self.rlc.acked_through();
                    if acked_now > last_acked {
                        last_acked = acked_now;
                        escape_start = Instant::now();
                    }
                    if escape_start.elapsed() > RLC_BLOCK_ESCAPE {
                        if self.ctrl.force(SensCode::Rs) {
                            // Resend the un-acked tail [acked_through, items_total)
                            // over RS, then this item.
                            self.switch_rlc_to_rs()?;
                            self.send_via_rs(&payload)?;
                        } else {
                            // A forced-RLC policy: honor it with the blocking send.
                            self.rlc.send_item(&payload)?;
                        }
                        break;
                    }
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
            SensCode::Rs => {
                self.send_via_rs(&payload)?;
            }
        }
        // Record in the replay ring (global index = items_total), advance, and
        // trim the delivered front + hard-cap.
        self.sent_ring.push_back(payload);
        self.items_total += 1;
        self.trim_sent_ring();
        if self.last_sample.elapsed() >= SWITCH_SAMPLE_PERIOD {
            self.last_sample = Instant::now();
            self.maybe_switch()?;
        }
        Ok(())
    }

    /// Evict replay-ring items RLC has confirmed delivered (below its cumulative
    /// frontier) and hard-cap the ring length. Preserves the invariant
    /// `items_total == ring_base + sent_ring.len()`.
    fn trim_sent_ring(&mut self) {
        if self.active == SensCode::Rlc {
            let frontier = self.rlc.acked_through() as u64;
            while self.ring_base < frontier && !self.sent_ring.is_empty() {
                if let Some(buf) = self.sent_ring.pop_front() {
                    self.recycle(buf);
                }
                self.ring_base += 1;
            }
        }
        while self.sent_ring.len() > SENT_RING_CAP {
            if let Some(buf) = self.sent_ring.pop_front() {
                self.recycle(buf);
            }
            self.ring_base += 1;
        }
    }

    /// Return a trimmed wire-payload buffer to the pool for reuse by the next
    /// seal, capped so a shrinking ring does not pin idle memory.
    fn recycle(&mut self, buf: Vec<u8>) {
        if self.ring_pool.len() < RING_POOL_CAP {
            self.ring_pool.push(buf);
        }
    }

    /// RLC -> RS handover by RESEND (not drain): announce the boundary RLC has
    /// delivered to, switch, and resend the un-acked tail `[boundary,
    /// items_total)` over RS from the replay ring, in order. RS is reliable, so
    /// it recovers the tail fast at any loss - no waiting on RLC's slow frontier
    /// recovery. Falls back to draining RLC only if the cap evicted un-acked
    /// items (so nothing is ever dropped).
    fn switch_rlc_to_rs(&mut self) -> io::Result<()> {
        let boundary = self.rlc.acked_through() as u64;
        let frame = encode_code_switch(boundary, SensCode::Rs);
        for _ in 0..CODE_SWITCH_REPEATS {
            self.real.send_to(&frame, self.peer).ok();
            std::thread::sleep(Duration::from_millis(2));
        }
        self.active = SensCode::Rs;
        if boundary >= self.ring_base {
            let start = (boundary - self.ring_base) as usize;
            let end = self.sent_ring.len();
            for i in start..end {
                let item = self.sent_ring[i].clone();
                self.send_via_rs(&item)?;
            }
        } else {
            // Un-acked tail underflowed the cap: drain RLC so nothing is lost.
            let target = self.rlc.next_source_id();
            self.rlc.drain_until_acked(target, ESCAPE_DRAIN_TIMEOUT)?;
        }
        Ok(())
    }

    /// Send one item over RS, waiting out RS flow-control back-pressure (RS's ARQ
    /// guarantees the window clears, so this wait is bounded by delivery, not by a
    /// decode cliff). Shared by the RS steady state and the RLC escape handover.
    fn send_via_rs(&mut self, item: &[u8]) -> io::Result<()> {
        while self.rs.flow_blocked() {
            self.rs.pump_feedback().ok();
            if self.rs.flow_blocked() {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        self.rs.send_item(item)
    }

    /// Sample the active code's fed-back loss and switch codes if the controller
    /// confirms a crossing of the configured thresholds.
    fn maybe_switch(&mut self) -> io::Result<()> {
        // The raw channel loss from sent-vs-received datagram counts: code-
        // agnostic, so it does not collapse when the active code recovers the
        // loss (which is what made the active code's own feedback flap).
        let sent = self.sent_counter.load(Ordering::Relaxed);
        let recv = self.fb_received.load(Ordering::Relaxed);
        if recv == 0 {
            return Ok(()); // no raw-loss report from the receiver yet
        }
        // Warmup: the in-flight window ramps 0 -> flow window at start, and that
        // growth reads as loss; track the baseline but do not evaluate until it
        // stabilizes, so the ramp does not trip a spurious switch.
        if self.started.elapsed() < SWITCH_WARMUP {
            self.prev_sent = sent;
            self.prev_received = recv;
            return Ok(());
        }
        if self.prev_received == 0 {
            // First report: set the baseline, evaluate from the next window.
            self.prev_sent = sent;
            self.prev_received = recv;
            return Ok(());
        }
        // Align the window to FEEDBACK arrivals: skip ticks with no new report,
        // so a tick landing between reports does not read a spurious 100% loss
        // (sent advanced, received not yet updated this window).
        if recv <= self.prev_received {
            return Ok(());
        }
        let sent_d = sent.saturating_sub(self.prev_sent);
        if sent_d < MIN_LOSS_SAMPLE {
            return Ok(()); // window too small to trust; keep accumulating
        }
        let recv_d = recv.saturating_sub(self.prev_received);
        self.prev_sent = sent;
        self.prev_received = recv;
        let lost_d = sent_d.saturating_sub(recv_d) as f64;
        // Size-weighted decaying loss: decay the lost / sent COUNTS and take their
        // ratio, NOT an equal-weight EWMA of per-window ratios. A small feedback
        // window with one drop reads a spuriously high ratio, and equal-weight
        // averaging over-read low loss ~3.5x (3% measured as ~11%); weighting by
        // datagram count makes large windows dominate so the estimate tracks the
        // true channel loss. The 0.95 decay (effective window ~20 feedback samples)
        // keeps it recent yet smooths the retransmit-burst windows that a tighter
        // decay let spike across the up threshold and flap the code.
        self.loss_acc = 0.95 * self.loss_acc + lost_d;
        self.sent_acc = 0.95 * self.sent_acc + sent_d as f64;
        self.ewma_loss = if self.sent_acc > 0.0 {
            self.loss_acc / self.sent_acc
        } else {
            0.0
        };
        // Gate the switch until the accumulator has matured past its cold start: at
        // warmup-end loss_acc/sent_acc are near-empty, so the first post-warmup
        // window's raw ratio (a start-of-stream burst) would otherwise dominate the
        // estimate and trip a spurious up-switch. Keep accumulating, just do not act
        // on it yet.
        if self.post_warm_windows < MIN_ACCUM_WINDOWS {
            self.post_warm_windows += 1;
            return Ok(());
        }
        let loss_q8 = (self.ewma_loss * 256.0).clamp(0.0, 255.0) as u8;
        if let Some(to) = self.ctrl.observe(loss_q8) {
            self.do_switch(to)?;
        }
        Ok(())
    }

    /// Code handover. RLC -> RS RESENDS the un-acked tail over RS (RS is reliable
    /// and fast at any loss, so it never waits on RLC's slow frontier recovery).
    /// RS -> RLC drains RS first (RS's ARQ clears its window quickly), then starts
    /// RLC from the fully-delivered boundary. In-order delivery holds either way.
    fn do_switch(&mut self, to: SensCode) -> io::Result<()> {
        match (self.active, to) {
            (SensCode::Rlc, SensCode::Rs) => self.switch_rlc_to_rs(),
            _ => self.do_switch_with_drain(to, DRAIN_TIMEOUT),
        }
    }

    /// `do_switch` with an explicit drain deadline. The flow-block escape passes a
    /// generous one ([`ESCAPE_DRAIN_TIMEOUT`]) because draining a stuck window
    /// over a high-loss link (retransmitting its frontier, each copy itself
    /// lossy) takes far longer than a healthy handover.
    fn do_switch_with_drain(&mut self, to: SensCode, drain_timeout: Duration) -> io::Result<()> {
        match self.active {
            SensCode::Rlc => {
                let target = self.rlc.next_source_id();
                self.rlc.drain_until_acked(target, drain_timeout)?;
            }
            SensCode::Rs => {
                self.rs.flush()?;
                self.rs.drain_until_acked(drain_timeout)?;
            }
        }
        let frame = encode_code_switch(self.items_total, to);
        for _ in 0..CODE_SWITCH_REPEATS {
            self.real.send_to(&frame, self.peer).ok();
            std::thread::sleep(Duration::from_millis(2));
        }
        self.active = to;
        // Returning to RLC: another code carried [old RLC frontier, items_total),
        // so RLC's source-id stream diverged from the global index. Re-base it to
        // the global boundary so the resumed stream's source ids equal the global
        // item indices the receiver expects (it re-bases in lockstep on the same
        // boundary), instead of stalling on holes RLC will never resend or
        // replaying its stale pre-switch buffer.
        if to == SensCode::Rlc {
            self.rlc.skip_to(self.items_total as u32);
        }
        Ok(())
    }

    /// Flush and drain the active code so the final items are delivered. Returns
    /// whether everything was acked before the deadline.
    ///
    /// Drains for [`DEFAULT_FINISH_DEADLINE`]; a peer that died mid-stream
    /// holds the caller for that whole window, so a caller with its own
    /// shutdown budget wants [`finish_within`](Self::finish_within).
    pub fn finish(&mut self) -> io::Result<bool> {
        self.finish_within(DEFAULT_FINISH_DEADLINE)
    }

    /// [`finish`](Self::finish) with the caller's own deadline. Returns
    /// `false` when the deadline passed with items still unacked, which is
    /// the ordinary answer for a peer that stopped responding - the caller
    /// decides whether that is a failure.
    pub fn finish_within(&mut self, deadline: Duration) -> io::Result<bool> {
        match self.active {
            SensCode::Rlc => {
                let target = self.rlc.next_source_id();
                self.rlc.drain_until_acked(target, deadline)
            }
            SensCode::Rs => {
                self.rs.flush()?;
                self.rs.drain_until_acked(deadline)
            }
        }
    }

    /// Force the active code to `to` now (operator override), via the same
    /// handover an automatic switch uses (RLC->RS resend / RS->RLC drain), and
    /// keep the controller in sync so it does not immediately switch back. No-op
    /// if already on `to`.
    pub fn force_switch(&mut self, to: SensCode) -> io::Result<()> {
        if to != self.active {
            self.ctrl.force(to);
            self.do_switch(to)?;
        }
        Ok(())
    }
}

impl Drop for UnifiedSensSender {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.demux.take() {
            h.join().ok();
        }
    }
}

// ---------------------------------------------------------------------------
// Unified receiver
// ---------------------------------------------------------------------------

/// Unified Sens-O-Matic receiver: demuxes both codes off one socket and
/// delivers items in order across mid-stream code switches. The sender's
/// drain-barrier guarantees the old code is fully delivered before the new code
/// starts, so the receiver simply runs the active decoder and switches at the
/// announced boundary.
pub struct UnifiedSensReceiver {
    real: Arc<UdpSocket>,
    rlc: SensOMaticRlcReceiver,
    rs: ReliableUdpReceiver,
    active: SensCode,
    switch_signal: SwitchSignal,
    pending_switch: Option<(u64, SensCode)>,
    delivered_total: u64,
    /// Global index of the next item the RS decoder will deliver. RS delivers in
    /// its own local order; this maps that to the global stream so the un-acked
    /// tail an RLC->RS handover resends over RS can be deduped against what RLC
    /// already delivered. Set to the handover boundary on RLC->RS; advances per RS
    /// item thereafter.
    rs_next_global: u64,
    switches: u64,
    /// Unified AEAD record layer (TLS feature). When set, each item a decoder
    /// delivers is opened with its global index as the packet number before it
    /// reaches the application; duplicates (the resend overlap) are skipped before
    /// opening, so the packet number always matches the seal. A `OnceLock` shared
    /// with the handshake driver: the one-port server completes its handshake on a
    /// thread (the QUIC endpoint owns the socket, so the Sens handshake rides the
    /// demux queue) and publishes the keys here once; `bind_tls` sets it inline.
    #[cfg(feature = "tls")]
    crypto: Arc<std::sync::OnceLock<crate::rlc_crypto::CryptoState>>,
    /// TLS is expected on this receiver (set by `bind_tls` / `from_shared_tls`):
    /// `poll` withholds delivery until `crypto` is published, so a data frame that
    /// races ahead of the handshake completion is never opened with absent keys.
    #[cfg(feature = "tls")]
    expect_tls: bool,
    stop: Arc<AtomicBool>,
    demux: Option<JoinHandle<()>>,
    /// Same slots the sender's reader publishes; a receiver whose reader
    /// wedges is the case that presents as a healthy, deaf process. `None`
    /// on a receiver fed by an external demux, which owns no reader here.
    demux_stats: Option<Arc<[AtomicU64; DEMUX_STAT_SLOTS]>>,
    demux_start: Instant,
}

impl UnifiedSensReceiver {
    /// Bind `local` and bring up both decoders sharing it.
    pub fn bind<A: ToSocketAddrs>(local: A, cfg: UnifiedConfig) -> io::Result<Self> {
        let udp = UdpSocket::bind(local)?;
        udp.set_nonblocking(true)?;
        Self::assemble(udp, cfg, 0)
    }

    /// Like [`bind`](Self::bind) but runs a TLS 1.3 server handshake first and
    /// AEAD-opens every delivered item: the WAN-confidential counterpart to
    /// [`UnifiedSensSender::connect_tls`]. The handshake completes before the
    /// demux reader takes the socket.
    #[cfg(feature = "tls")]
    pub fn bind_tls<A: ToSocketAddrs>(
        local: A,
        cfg: UnifiedConfig,
        tls: std::sync::Arc<rustls::ServerConfig>,
    ) -> io::Result<Self> {
        let udp = UdpSocket::bind(local)?;
        udp.set_nonblocking(true)?;
        let mut cs = crate::rlc_crypto::CryptoState::new_server(tls)
            .map_err(io::Error::other)?;
        let hs = DgramSock::from_udp(udp.try_clone()?);
        crate::sens_rlc::drive_handshake(&hs, None, &mut cs, false)?;
        let mut s = Self::assemble(udp, cfg, crate::rlc_crypto::TAG_LEN)?;
        s.crypto.set(cs).ok();
        s.expect_tls = true;
        Ok(s)
    }

    /// Build the receiver over an already-bound (and, for TLS, already-handshaked)
    /// socket: bring up both decoders sharing it and spawn the demux reader.
    fn assemble(udp: UdpSocket, cfg: UnifiedConfig, seal_overhead: usize) -> io::Result<Self> {
        // The decoder must accept the sealed wire width (item + AEAD tag under
        // TLS); the RS decoder learns its shard width from the wire header, so
        // only the RLC decoder's symbol size needs widening here.
        let wire_sym = cfg.symbol_len + seal_overhead;
        let thread_sock = udp.try_clone()?;
        thread_sock.set_nonblocking(true)?;
        let real = Arc::new(udp);
        let rlc_q = new_demux_queue();
        let rs_q = new_demux_queue();

        // No per-code debug loss: the unified path injects loss uniformly at the
        // demux (below), modelling a real lossy link AND letting the raw-loss
        // estimate see it (a sub-receiver drop would be invisible to the demux
        // count).
        let mut rlc = SensOMaticRlcReceiver::bind("0.0.0.0:0", wire_sym)?;
        rlc.set_sock(DgramSock::demux(Arc::clone(&real), Arc::clone(&rlc_q)));

        let mut rs = ReliableUdpReceiver::bind("0.0.0.0:0")?;
        rs.set_sock(DgramSock::demux(Arc::clone(&real), Arc::clone(&rs_q)));

        let switch_signal: SwitchSignal = Arc::new(Mutex::new(None));
        let recv_counter = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let demux_stats: Arc<[AtomicU64; DEMUX_STAT_SLOTS]> =
            Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let demux_start = Instant::now();
        let demux = spawn_demux(
            thread_sock,
            rlc_q,
            rs_q,
            Some(Arc::clone(&switch_signal)),
            Some(recv_counter),
            None,
            cfg.debug_loss,
            cfg.seed,
            Arc::clone(&stop),
            Some(Arc::clone(&demux_stats)),
            demux_start,
        );

        Ok(Self {
            real,
            rlc,
            rs,
            active: cfg.policy.initial_code(),
            switch_signal,
            pending_switch: None,
            delivered_total: 0,
            rs_next_global: 0,
            switches: 0,
            #[cfg(feature = "tls")]
            crypto: Arc::new(std::sync::OnceLock::new()),
            #[cfg(feature = "tls")]
            expect_tls: false,
            stop,
            demux: Some(demux),
            demux_stats: Some(demux_stats),
            demux_start,
        })
    }

    /// Build a receiver fed by an EXTERNAL demux (the one-port QUIC endpoint's
    /// socket routes Sens datagrams into `rlc_q` / `rs_q` / `switch_signal` and
    /// tallies `recv_counter`). `send_sock` is a clone of the shared socket for
    /// control + raw-loss feedback. No demux thread is spawned (the QUIC socket
    /// feeds the queues); a small reporter thread sends the feedback to the peer
    /// the QUIC socket records in `sens_peer`.
    #[allow(clippy::too_many_arguments)]
    pub fn from_shared(
        send_sock: Arc<UdpSocket>,
        rlc_q: DemuxQueue,
        rs_q: DemuxQueue,
        switch_signal: SwitchSignal,
        recv_counter: Arc<AtomicU64>,
        sens_peer: Arc<Mutex<Option<SocketAddr>>>,
        cfg: UnifiedConfig,
        seal_overhead: usize,
    ) -> io::Result<Self> {
        // The RLC decoder must accept the sealed wire width (item + AEAD tag under
        // TLS) so it frames the symbols the sender shipped; the RS decoder learns
        // its shard width from the wire header, so only the RLC width needs it.
        let mut rlc = SensOMaticRlcReceiver::bind("0.0.0.0:0", cfg.symbol_len + seal_overhead)?;
        rlc.set_sock(DgramSock::demux(Arc::clone(&send_sock), rlc_q));
        let mut rs = ReliableUdpReceiver::bind("0.0.0.0:0")?;
        rs.set_sock(DgramSock::demux(Arc::clone(&send_sock), rs_q));
        let stop = Arc::new(AtomicBool::new(false));
        let demux = spawn_fb_reporter(Arc::clone(&send_sock), recv_counter, sens_peer, Arc::clone(&stop));
        Ok(Self {
            real: send_sock,
            rlc,
            rs,
            active: cfg.policy.initial_code(),
            switch_signal,
            pending_switch: None,
            delivered_total: 0,
            rs_next_global: 0,
            switches: 0,
            #[cfg(feature = "tls")]
            crypto: Arc::new(std::sync::OnceLock::new()),
            #[cfg(feature = "tls")]
            expect_tls: false,
            stop,
            demux: Some(demux),
            // The QUIC endpoint owns the reader; this receiver has no
            // heartbeat of its own to report.
            demux_stats: None,
            demux_start: Instant::now(),
        })
    }

    /// Like [`from_shared`](Self::from_shared) but runs a TLS 1.3 server handshake
    /// over the demux'd `hs_q`. The one-port QUIC endpoint owns the socket, so the
    /// Sens handshake cannot own a recv loop; it rides the same demux queue as data
    /// (the demux routes `PKT_RLC_CRYPTO` frames into `hs_q`). The handshake runs
    /// on a thread and publishes the 1-RTT keys to the shared `crypto` cell once
    /// complete; `poll` withholds delivery until then. Returns immediately so the
    /// caller can start the QUIC + Sens clients that drive the handshake.
    #[cfg(feature = "tls")]
    #[allow(clippy::too_many_arguments)]
    pub fn from_shared_tls(
        send_sock: Arc<UdpSocket>,
        rlc_q: DemuxQueue,
        rs_q: DemuxQueue,
        hs_q: DemuxQueue,
        switch_signal: SwitchSignal,
        recv_counter: Arc<AtomicU64>,
        sens_peer: Arc<Mutex<Option<SocketAddr>>>,
        cfg: UnifiedConfig,
        tls: std::sync::Arc<rustls::ServerConfig>,
    ) -> io::Result<Self> {
        let mut s = Self::from_shared(
            Arc::clone(&send_sock),
            rlc_q,
            rs_q,
            switch_signal,
            recv_counter,
            sens_peer,
            cfg,
            crate::rlc_crypto::TAG_LEN,
        )?;
        s.expect_tls = true;
        let crypto = Arc::clone(&s.crypto);
        let stop = Arc::clone(&s.stop);
        let hs_sock = DgramSock::demux(send_sock, hs_q);
        std::thread::spawn(move || {
            let mut cs = match crate::rlc_crypto::CryptoState::new_server(tls) {
                Ok(c) => c,
                Err(_) => return,
            };
            // Drive the server handshake over the demux'd queue (peer learned from
            // the first flight); publish the keys once the 1-RTT secrets derive.
            if !stop.load(Ordering::Relaxed)
                && crate::sens_rlc::drive_handshake(&hs_sock, None, &mut cs, false).is_ok()
            {
                crypto.set(cs).ok();
            }
        });
        Ok(s)
    }

    /// The decoder currently delivering.
    pub fn active_code(&self) -> SensCode {
        self.active
    }

    /// Code switches the receiver has followed.
    pub fn switches(&self) -> u64 {
        self.switches
    }

    /// How long since this receiver's demux reader last completed a loop,
    /// or `None` when an external demux feeds it and it owns no reader.
    /// A wedged reader still reports `demux_alive`, so this is what
    /// separates a healthy idle socket from a process that has silently
    /// stopped hearing the world: an idle reader loops every 100us.
    pub fn demux_stale_for(&self) -> Option<Duration> {
        let stats = self.demux_stats.as_ref()?;
        let last = stats[DEMUX_SLOT_LAST_ITER].load(Ordering::Relaxed);
        let now = self.demux_start.elapsed().as_nanos() as u64;
        Some(Duration::from_nanos(now.saturating_sub(last)))
    }

    /// This receiver's demux reader counters, `(iterations, recv_ok,
    /// would_block, rlc_frames_routed)`, or `None` when an external demux
    /// feeds it. `recv_ok` climbing while `routed` stalls is a datagram
    /// the routing arms refuse; both frozen with iterations climbing is a
    /// socket nothing reaches. The sender reports the same shape.
    pub fn demux_probe(&self) -> Option<(u64, u64, u64, u64)> {
        let s = self.demux_stats.as_ref()?;
        Some((
            s[0].load(Ordering::Relaxed),
            s[1].load(Ordering::Relaxed),
            s[2].load(Ordering::Relaxed),
            s[3].load(Ordering::Relaxed),
        ))
    }

    /// Socket errors this receiver's demux reader met that were neither
    /// `WouldBlock` nor a read timeout, or `None` when an external demux
    /// feeds it. Entering and leaving the erroring state also report on
    /// stderr.
    pub fn demux_errors(&self) -> Option<u64> {
        Some(self.demux_stats.as_ref()?[DEMUX_SLOT_ERRORS].load(Ordering::Relaxed))
    }

    /// Whether this receiver's demux reader thread is still running.
    pub fn demux_alive(&self) -> bool {
        self.demux.as_ref().is_some_and(|h| !h.is_finished())
    }

    /// Whether either decoder adopted a replacement session since this was
    /// last called, clearing the flag. Edge-triggered: one report per
    /// adoption.
    pub fn take_session_changed(&mut self) -> bool {
        let rlc = self.rlc.take_session_changed();
        let rs = self.rs.take_session_changed();
        rlc || rs
    }

    /// The RLC connection ids holding a decode window, in first-seen order.
    /// Empty before any peer is seen.
    pub fn live_rlc_sessions(&self) -> Vec<u64> {
        self.rlc.live_sessions()
    }

    /// One block-RS window's `(next_needed, highest_seen)` block ids, the
    /// peer of [`rlc_session_frontier`](Self::rlc_session_frontier).
    /// `highest_seen` ahead of `next_needed` is a window stalled on a block
    /// behind its frontier.
    pub fn rs_session_frontier(&self, epoch: u32) -> Option<(u32, u32, u64, Option<SocketAddr>)> {
        self.rs.session_frontier(epoch)
    }

    /// One block-RS window's ingest refusals by reason. Read beside
    /// [`rs_session_frontier`](Self::rs_session_frontier): a frontier that
    /// is not moving while these climb names the gate holding the shards
    /// out, which is the difference between a shard that never arrived and
    /// one that arrived and was turned away.
    pub fn rs_session_rejects(&self, epoch: u32) -> Option<RejectCounts> {
        self.rs.session_rejects(epoch)
    }

    /// Epochs under an admission challenge, with the address challenged.
    /// A restarted peer sits here until its nonce returns, and every
    /// datagram it sends meanwhile is refused.
    pub fn rs_pending_admissions(&self) -> Vec<(u32, SocketAddr)> {
        self.rs.pending_admissions()
    }

    /// The block-RS session epochs holding a decode window, in first-seen
    /// order.
    pub fn live_rs_sessions(&self) -> Vec<u32> {
        self.rs.live_sessions()
    }

    /// Peers refused a decode window on either code.
    pub fn session_refusals(&self) -> u64 {
        self.rlc.session_refusals() + self.rs.session_refusals()
    }

    /// One RLC session's delivery position: `(delivered_through,
    /// highest_seen)`, or `None` for an id with no window.
    pub fn rlc_session_frontier(&self, cid: u64) -> Option<(u32, u32)> {
        self.rlc.session_frontier(cid)
    }

    /// One RLC session's control plane: `(naks_sent, acks_sent,
    /// sends_skipped, peer_validated)`, or `None` for an id with no
    /// window.
    pub fn rlc_session_control(&self, cid: u64) -> Option<(u64, u64, u64, bool)> {
        self.rlc.session_control(cid)
    }

    /// The address one RLC session's control sends target, or `None`
    /// for an id with no window or no recorded peer.
    pub fn rlc_session_peer(&self, cid: u64) -> Option<SocketAddr> {
        self.rlc.session_admissions_for(cid)
    }

    /// Successful path validations, summed over every RLC session.
    pub fn rlc_path_validations(&self) -> u64 {
        self.rlc.path_validations()
    }

    /// Path-validation timeouts, summed over every RLC session. A
    /// count climbing without bound is a session re-challenging an
    /// address that never answers inside the window.
    pub fn rlc_path_validation_failures(&self) -> u64 {
        self.rlc.path_validation_failures()
    }

    /// `(adopted, challenges_that_went_unanswered)` for replacement
    /// sessions, summed over both codes. A refused forgery raises the
    /// second without the first.
    pub fn session_adoption_counts(&self) -> (u64, u64) {
        let (ra, rf) = self.rlc.session_adoption_counts();
        let (sa, sf) = self.rs.session_adoption_counts();
        (ra + sa, rf + sf)
    }

    /// Admission challenges the block-RS receiver has armed. Read against
    /// [`session_adoption_counts`](Self::session_adoption_counts): a
    /// candidate epoch that raised neither an admission nor a failure was
    /// either never challenged, which this distinguishes, or is still
    /// inside its answer window. RS-only, so it is not summed with the RLC
    /// side, which has no counterpart.
    pub fn rs_session_challenges_armed(&self) -> u64 {
        self.rs.session_challenges_armed()
    }

    /// The bound local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.real.local_addr()
    }

    /// Recover an item from a delivered wire payload: AEAD-open (TLS) with `pn`
    /// the item's global index, or pass the bytes through. A failed open (a
    /// tampered datagram) surfaces as an error rather than delivering bad data.
    #[cfg_attr(not(feature = "tls"), allow(unused_variables, unused_mut))]
    fn open_payload(&self, mut payload: Vec<u8>, pn: u64) -> io::Result<Vec<u8>> {
        #[cfg(feature = "tls")]
        if let Some(cs) = self.crypto.get() {
            let n = cs
                .open(pn, &mut payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            payload.truncate(n);
            return Ok(payload);
        }
        Ok(payload)
    }

    /// Drive the active decoder and return the items it delivered this call,
    /// each tagged with the identity of the peer that sent it: the RLC
    /// connection id, or the block-RS session epoch widened to `u64`.
    ///
    /// Both codes decode a window per peer. The code-switch layer above them
    /// does not: the delivery frontier, the switch boundary and the TLS packet
    /// number are per endpoint. A mesh node pins a code and leaves TLS off, or
    /// drives [`SensOMaticRlcReceiver`] / [`ReliableUdpReceiver`] directly.
    pub fn poll_from(&mut self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        self.poll_tagged()
    }

    /// Drive the active decoder and return the items it delivered this call.
    /// Honors a pending CODE_SWITCH once the active decoder has delivered every
    /// item up to the announced boundary.
    pub fn poll(&mut self) -> io::Result<Vec<Vec<u8>>> {
        Ok(self.poll_tagged()?.into_iter().map(|(_, item)| item).collect())
    }

    /// The one drain both public forms share, carrying each item's peer tag
    /// from the decoder that delivered it rather than reconstructing it after.
    fn poll_tagged(&mut self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        // One-port TLS: the handshake completes asynchronously on a thread (the
        // QUIC endpoint owns the socket), so until the keys are published, withhold
        // delivery. The decoders keep buffering inbound frames; the peer only sends
        // data after ITS handshake finished, so the backlog is at most a few frames
        // and they open correctly once the keys land. (bind_tls sets the keys
        // inline before returning, so this gate is already clear there.)
        #[cfg(feature = "tls")]
        if self.expect_tls && self.crypto.get().is_none() {
            return Ok(Vec::new());
        }
        if self.pending_switch.is_none() {
            self.pending_switch = self.switch_signal.lock().unwrap().take();
        }
        let out = match self.active {
            SensCode::Rlc => {
                // Open each payload with its global index as the packet number.
                // The tag rides from the decoder, so an item is attributed to the
                // peer that actually sent it rather than to whoever spoke last.
                let raw = self.rlc.poll_from()?;
                let mut d = Vec::with_capacity(raw.len());
                for (cid, payload) in raw {
                    let item = self.open_payload(payload, self.delivered_total)?;
                    self.delivered_total += 1;
                    d.push((cid, item));
                }
                d
            }
            SensCode::Rs => {
                // RS delivers in its own local order; map each to its global index
                // (rs_next_global, advancing per item). After an RLC->RS resend
                // handover the leading items overlap what RLC already delivered, so
                // drop any whose global index is below the delivery frontier
                // (before opening, so the packet number always matches the seal).
                //
                // The tag is the sending peer's session epoch, widened.
                let raw = self.rs.poll_from()?;
                let mut d = Vec::with_capacity(raw.len());
                for (epoch, payload) in raw {
                    if self.rs_next_global >= self.delivered_total {
                        let item = self.open_payload(payload, self.rs_next_global)?;
                        self.delivered_total += 1;
                        d.push((u64::from(epoch), item));
                    }
                    self.rs_next_global += 1;
                }
                d
            }
        };
        if let Some((boundary, to)) = self.pending_switch
            && self.delivered_total >= boundary
        {
            // The sender repeats CODE_SWITCH for reliability; only act (and
            // count) when the target differs from the active code, so the
            // repeats do not inflate the switch tally or re-switch.
            if to != self.active {
                match to {
                    SensCode::Rs => {
                        // The RS stream resumes at the boundary (RLC's delivery
                        // frontier); index its local order from there.
                        self.rs_next_global = boundary;
                    }
                    SensCode::Rlc => {
                        // Returning to RLC: re-base the decoder to the boundary so
                        // it delivers the resumed stream from there (whose source
                        // ids the sender re-aligned to the global index) and does
                        // not replay its stale pre-switch buffer or stall on holes
                        // the other code already delivered.
                        self.rlc.skip_to(boundary as u32);
                    }
                }
                self.active = to;
                self.switches += 1;
            }
            self.pending_switch = None;
        }
        Ok(out)
    }
}

impl Drop for UnifiedSensReceiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.demux.take() {
            h.join().ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_policies_never_switch() {
        for policy in [CodePolicy::ForceRlc, CodePolicy::ForceRs] {
            let mut c = CodeSwitchController::with_policy(policy);
            let start = c.code();
            for q in [0u8, 80, 200, 255, 10, 0] {
                assert_eq!(c.observe(q), None, "forced policy must not switch");
            }
            assert_eq!(c.code(), start);
            assert_eq!(c.switches(), 0);
        }
    }

    #[test]
    fn force_rs_starts_on_rs() {
        let c = CodeSwitchController::with_policy(CodePolicy::ForceRs);
        assert_eq!(c.code(), SensCode::Rs);
    }

    #[test]
    fn auto_starts_on_rlc_then_up_switches_when_loss_sustains() {
        let mut c = CodeSwitchController::new(CodePolicy::default_auto(), 2, 8);
        assert_eq!(c.code(), SensCode::Rlc);
        // 12% loss (q8 ~30) is below the ~15% up threshold (q8 38): no switch.
        assert_eq!(c.observe(30), None);
        assert_eq!(c.observe(30), None);
        assert_eq!(c.code(), SensCode::Rlc);
        // 18% loss (q8 46) above the up threshold: one sample arms, the second
        // (up_hold = 2) confirms the switch to RS.
        assert_eq!(c.observe(46), None, "first over-threshold sample only arms");
        assert_eq!(c.observe(46), Some(SensCode::Rs), "second confirms up-switch");
        assert_eq!(c.code(), SensCode::Rs);
        assert_eq!(c.switches(), 1);
    }

    #[test]
    fn stall_escape_latches_rs_and_does_not_flap() {
        // A flow-block escape to RS (RLC stalled at this loss) must NOT down-switch
        // back even when the loss estimate sits below the down threshold: returning
        // to a code that just stalled flaps, and the RS->RLC handover then corrupts
        // in-order delivery. The latch holds RS after a stall-escape.
        let mut c = CodeSwitchController::new(CodePolicy::default_auto(), 2, 4);
        assert!(c.force(SensCode::Rs), "stall-escape forces to RS");
        assert_eq!(c.code(), SensCode::Rs);
        for i in 0..20 {
            assert_eq!(c.observe(5), None, "latched RS must not down-switch at tick {i}");
        }
        assert_eq!(c.code(), SensCode::Rs);
        assert_eq!(c.switches(), 1, "no flap: only the one escape switch");
    }

    #[test]
    fn a_single_loss_spike_does_not_flap_the_code() {
        let mut c = CodeSwitchController::new(CodePolicy::default_auto(), 2, 8);
        // One isolated spike over the threshold then back down: up_hold = 2 is
        // not met, so no switch (the streak resets on the low sample).
        assert_eq!(c.observe(200), None);
        assert_eq!(c.observe(10), None);
        assert_eq!(c.observe(200), None);
        assert_eq!(c.code(), SensCode::Rlc, "an isolated spike must not switch");
        assert_eq!(c.switches(), 0);
    }

    #[test]
    fn down_switch_needs_a_longer_sustained_low_streak() {
        let mut c = CodeSwitchController::new(CodePolicy::default_auto(), 2, 8);
        // Drive up to RS first.
        c.observe(80);
        assert_eq!(c.observe(80), Some(SensCode::Rs));
        // Loss drops below the 10% down threshold (q8 26). It must SUSTAIN for
        // down_hold = 8 samples; a brief low spell does not relax the code.
        for _ in 0..7 {
            assert_eq!(c.observe(10), None, "down-switch must not fire early");
        }
        assert_eq!(c.observe(10), Some(SensCode::Rlc), "8th low sample relaxes to RLC");
        assert_eq!(c.code(), SensCode::Rlc);
        assert_eq!(c.switches(), 2);
    }

    #[test]
    fn hysteresis_band_holds_rs_between_thresholds() {
        let mut c = CodeSwitchController::new(CodePolicy::default_auto(), 2, 8);
        c.observe(80);
        c.observe(80); // now on RS
        assert_eq!(c.code(), SensCode::Rs);
        // Loss in the band (down_q8=26 < q8=32 < up_q8=38): neither relaxes nor
        // re-arms; RS holds across the whole band (no flapping).
        for _ in 0..20 {
            assert_eq!(c.observe(32), None);
        }
        assert_eq!(c.code(), SensCode::Rs, "RS holds inside the hysteresis band");
    }

    // A real two-socket loopback round trip that forces an RLC -> RS handover
    // mid-stream and asserts every item is delivered exactly once, in order,
    // across the switch. Exercises the demux sockets, the drain-barrier, the
    // CODE_SWITCH frame, and the receiver's boundary merge end to end.
    /// Two concurrent senders through the unified endpoint, pinned to RLC (the
    /// mesh shape, and the code Auto runs at low loss). Every item of both
    /// streams must arrive, and `poll_from` must attribute each to the peer
    /// that actually sent it.
    ///
    /// The tag assertion is the point. Delivery alone passes even when every
    /// item is labelled with whoever spoke last, which is the misattribution a
    /// mesh node cannot detect from its own side.
    /// The same two-peer shape pinned to block-RS. The unified endpoint hands
    /// its RS half a demux socket, which is shared and fed by a reader that
    /// takes every source address, so that receiver has to route by session
    /// epoch rather than serve one peer.
    /// Three peers through the unified endpoint on block-RS. Two is not enough
    /// to exercise admission: one peer always takes the free first-admission
    /// slot, so a broken challenge path still delivers both. Three forces two
    /// separate challenges, and the challenge answer travels back over the
    /// sender's demux socket.
    /// Two peers on DIFFERENT codes through one receiver. Under `Auto` each
    /// sender runs its own switch controller, so a mesh whose links see
    /// different loss can have peers disagree about which code is live.
    ///
    /// The receiver holds one `active` code and polls only that decoder, so a
    /// peer sending the other code is never drained. This is the endpoint-wide
    /// switch boundary meeting a per-peer topology.
    #[test]
    #[ignore = "subetha-11: one active code per endpoint; peers on different codes are not both drained"]
    fn unified_peers_on_different_codes_both_deliver() {
        use std::sync::mpsc;
        let sym = 64usize;
        let base = UnifiedConfig {
            policy: CodePolicy::default_auto(),
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", base).unwrap();
        let addr = recv.local_addr().unwrap();
        let per_peer: u64 = 40;
        let total = per_peer * 2;

        let (tx, rx) = mpsc::channel();
        let rh = std::thread::spawn(move || {
            let mut recv = recv;
            let mut got: Vec<u64> = Vec::new();
            let start = Instant::now();
            while (got.len() as u64) < total && start.elapsed() < Duration::from_secs(20) {
                let items = recv.poll().unwrap_or_default();
                let empty = items.is_empty();
                for it in items {
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&it[..8]);
                    got.push(u64::from_le_bytes(s));
                }
                if empty {
                    std::thread::sleep(Duration::from_micros(200));
                }
            }
            tx.send(got).ok();
        });

        // One peer pinned to each code, which is the steady state a divergent
        // Auto switch reaches.
        let mut handles = Vec::new();
        for (p, policy) in [CodePolicy::ForceRlc, CodePolicy::ForceRs].into_iter().enumerate() {
            let mut cfg = base;
            cfg.policy = policy;
            handles.push(std::thread::spawn(move || {
                let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
                let mut buf = vec![0u8; 8];
                for i in 0..per_peer {
                    buf[..8].copy_from_slice(&(((p as u64) << 56) | i).to_le_bytes());
                    if send.send_item(&buf).is_err() {
                        break;
                    }
                }
                send.finish().ok();
            }));
        }
        for h in handles {
            h.join().ok();
        }

        let got = rx.recv_timeout(Duration::from_secs(25)).unwrap();
        rh.join().ok();
        for p in 0..2u64 {
            let mine: Vec<u64> = got
                .iter()
                .filter(|v| (*v >> 56) == p)
                .map(|v| v & 0x00FF_FFFF_FFFF_FFFF)
                .collect();
            assert_eq!(
                mine,
                (0..per_peer).collect::<Vec<_>>(),
                "peer {p} was not drained; the receiver polls one active code",
            );
        }
    }

    /// poll() must return promptly whether or not traffic is flowing: a mesh
    /// consumer polls one receiver per node in a loop, and a poll that blocks
    /// for seconds starves every other duty on that loop. Measured on a
    /// four-node mesh: a strict 1Hz log printed ~6 samples in ~40s.
    #[test]
    fn unified_poll_returns_promptly_under_sparse_traffic() {
        use std::sync::mpsc;
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRlc,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        let addr = recv.local_addr().unwrap();

        // Three peers on heartbeat-shaped traffic, one dying early: the mesh
        // shape where the seconds-scale poll was measured.
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let done_rx = std::sync::Arc::new(std::sync::Mutex::new(done_rx));
        let mut senders = Vec::new();
        for p in 0..3u64 {
            let done_rx = std::sync::Arc::clone(&done_rx);
            senders.push(std::thread::spawn(move || {
                let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
                let buf = vec![7u8; 8];
                std::thread::sleep(Duration::from_millis(150 * p));
                let n = if p == 1 { 2 } else { 8 };
                for _ in 0..n {
                    if send.send_item(&buf).is_err() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(400));
                }
                if p == 1 {
                    return;
                }
                done_rx.lock().unwrap().recv_timeout(Duration::from_secs(20)).ok();
            }));
        }

        let mut recv = recv;
        let mut worst = Duration::ZERO;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(6) {
            let t = Instant::now();
            recv.poll().ok();
            worst = worst.max(t.elapsed());
        }
        done_tx.send(()).ok();
        done_tx.send(()).ok();
        for s in senders {
            s.join().ok();
        }
        assert!(
            worst < Duration::from_millis(500),
            "a single poll() blocked for {worst:?} under sparse traffic",
        );
    }

    /// Three peers through the unified endpoint on ForceRlc, sending SPARSELY -
    /// one small item every 300ms - with one going silent partway. The
    /// consumer's topology: a heartbeat mesh where a node dies.
    ///
    /// Combines what the other multi-peer tests each cover separately: the
    /// demux socket, sparse traffic that lets the receiver's timers run between
    /// frames, and a peer that stops.
    #[test]
    fn unified_three_sparse_peers_survive_one_going_silent() {
        use std::sync::mpsc;
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRlc,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let rounds: u64 = 10;
        let silent_after: u64 = 3;
        let peers: u64 = 3;

        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        let addr = recv.local_addr().unwrap();
        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        let rh = std::thread::spawn(move || {
            let mut recv = recv;
            let mut got: Vec<(u64, u64)> = Vec::new();
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(15) && stop_rx.try_recv().is_err() {
                let batch: Vec<(u64, Vec<u8>)> = recv.poll_from().unwrap_or_default();
                for (tag, it) in batch {
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&it[..8]);
                    let v = u64::from_le_bytes(s);
                    got.push((tag, v));
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            got
        });

        let mut handles = Vec::new();
        for p in 0..peers {
            handles.push(std::thread::spawn(move || {
                let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
                let mut buf = vec![0u8; 8];
                let n = if p == 2 { silent_after } else { rounds };
                for i in 0..n {
                    buf[..8].copy_from_slice(&((p << 56) | i).to_le_bytes());
                    if send.send_item(&buf).is_err() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(300));
                }
                if p != 2 {
                    std::thread::sleep(Duration::from_secs(2));
                }
                send.finish().ok();
            }));
        }
        for h in handles {
            h.join().ok();
        }
        stop_tx.send(()).ok();
        let got: Vec<(u64, u64)> = rh.join().expect("collector thread");

        let tags: std::collections::BTreeSet<u64> = got.iter().map(|(t, _)| *t).collect();
        for p in 0..2u64 {
            let mine: Vec<u64> = got
                .iter()
                .filter(|(_, v)| (*v >> 56) == p)
                .map(|(_, v)| v & 0x00FF_FFFF_FFFF_FFFF)
                .collect();
            assert_eq!(
                mine,
                (0..rounds).collect::<Vec<_>>(),
                "surviving peer {p} stopped being delivered; got {} of {rounds}, \
                 tags seen {tags:?}",
                mine.len(),
            );
        }
    }

    /// Raw-loss feedback reaches every peer inside the retention window,
    /// not just the most recent speaker. The first sender goes quiet while
    /// a second keeps talking; before per-peer targeting the first one's
    /// fed-back count stayed frozen at zero from the moment the second
    /// spoke, so its loss estimate could never mature.
    #[test]
    fn fb_reaches_a_peer_that_stopped_speaking() {
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRlc,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let mut recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        let addr = recv.local_addr().unwrap();

        let mut quiet = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
        let mut talker = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();

        // The quiet peer speaks once, then never again.
        quiet.send_item(&0u64.to_le_bytes()).unwrap();

        // The talker keeps the socket busy, so it is always the last
        // speaker the reader saw.
        let start = Instant::now();
        let mut i = 1u64;
        while start.elapsed() < Duration::from_secs(3) {
            talker.send_item(&i.to_le_bytes()).ok();
            i += 1;
            recv.poll().ok();
            std::thread::sleep(Duration::from_millis(20));
        }

        let (_, quiet_fb) = quiet.raw_sent_recv();
        assert!(
            quiet_fb > 0,
            "a peer that stopped speaking starved of raw-loss feedback",
        );
    }

    /// finish_within honours the caller's deadline instead of holding it
    /// for the two-minute default when the peer is gone.
    #[test]
    fn finish_within_returns_on_the_callers_deadline() {
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRlc,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        // A receiver that never existed: the drain can never be acked.
        let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let mut send = UnifiedSensSender::connect("0.0.0.0:0", dead, cfg).unwrap();
        send.send_item(&7u64.to_le_bytes()).ok();

        let t0 = Instant::now();
        let acked = send.finish_within(Duration::from_millis(300)).unwrap();
        let waited = t0.elapsed();
        assert!(!acked, "a dead peer cannot have acked the drain");
        assert!(
            waited < Duration::from_secs(5),
            "finish_within held the caller for {waited:?}, past its own deadline",
        );
    }

    /// A live reader keeps its heartbeat fresh, so staleness separates an
    /// idle socket from a deaf one.
    #[test]
    fn demux_heartbeat_stays_fresh_on_a_live_reader() {
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRlc,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        // The reader loops on a 100us idle cadence, so let several pass.
        std::thread::sleep(Duration::from_millis(50));
        assert!(recv.demux_alive(), "reader died");
        let stale = recv.demux_stale_for().expect("receiver owns its reader");
        assert!(
            stale < Duration::from_secs(1),
            "an idle-but-live reader reported {stale:?} of staleness",
        );
        assert_eq!(recv.demux_errors(), Some(0), "no socket errors expected");
    }

    #[test]
    fn unified_three_peers_on_block_rs_all_deliver() {
        use std::sync::mpsc;
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRs,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        let addr = recv.local_addr().unwrap();
        let per_peer: u64 = 50;
        let peers: u64 = 3;
        let total = per_peer * peers;

        let (tx, rx) = mpsc::channel();
        let rh = std::thread::spawn(move || {
            let mut recv = recv;
            let mut got: Vec<u64> = Vec::with_capacity(total as usize);
            let start = Instant::now();
            while (got.len() as u64) < total && start.elapsed() < Duration::from_secs(30) {
                let items = recv.poll().unwrap_or_default();
                let empty = items.is_empty();
                for it in items {
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&it[..8]);
                    got.push(u64::from_le_bytes(s));
                }
                if empty {
                    std::thread::sleep(Duration::from_micros(200));
                }
            }
            tx.send(got).ok();
        });

        let gate = Arc::new(std::sync::Barrier::new(peers as usize));
        let mut handles = Vec::new();
        for p in 0..peers {
            let gate = Arc::clone(&gate);
            handles.push(std::thread::spawn(move || {
                let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
                let mut buf = vec![0u8; 8];
                gate.wait();
                let start = Instant::now();
                for i in 0..per_peer {
                    if start.elapsed() > Duration::from_secs(20) {
                        break;
                    }
                    buf[..8].copy_from_slice(&((p << 56) | i).to_le_bytes());
                    if send.send_item(&buf).is_err() {
                        break;
                    }
                }
                send.finish().ok();
            }));
        }
        for h in handles {
            h.join().ok();
        }

        let got = rx.recv_timeout(Duration::from_secs(35)).unwrap();
        rh.join().ok();
        for p in 0..peers {
            let mine: Vec<u64> = got
                .iter()
                .filter(|v| (*v >> 56) == p)
                .map(|v| v & 0x00FF_FFFF_FFFF_FFFF)
                .collect();
            assert_eq!(
                mine,
                (0..per_peer).collect::<Vec<_>>(),
                "peer {p} of {peers} did not deliver through the unified block-RS path",
            );
        }
    }

    #[test]
    fn unified_two_peers_on_block_rs_both_deliver() {
        use std::sync::mpsc;
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRs,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        let addr = recv.local_addr().unwrap();
        let per_peer: u64 = 60;
        let peers: u64 = 2;
        let total = per_peer * peers;

        let (tx, rx) = mpsc::channel();
        let rh = std::thread::spawn(move || {
            let mut recv = recv;
            let mut got: Vec<u64> = Vec::with_capacity(total as usize);
            let start = Instant::now();
            while (got.len() as u64) < total && start.elapsed() < Duration::from_secs(25) {
                let items = recv.poll().unwrap_or_default();
                let empty = items.is_empty();
                for it in items {
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&it[..8]);
                    got.push(u64::from_le_bytes(s));
                }
                if empty {
                    std::thread::sleep(Duration::from_micros(200));
                }
            }
            tx.send(got).ok();
        });

        let gate = Arc::new(std::sync::Barrier::new(peers as usize));
        let mut handles = Vec::new();
        for p in 0..peers {
            let gate = Arc::clone(&gate);
            handles.push(std::thread::spawn(move || {
                let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
                let mut buf = vec![0u8; 8];
                gate.wait();
                let start = Instant::now();
                for i in 0..per_peer {
                    if start.elapsed() > Duration::from_secs(15) {
                        break;
                    }
                    buf[..8].copy_from_slice(&((p << 56) | i).to_le_bytes());
                    if send.send_item(&buf).is_err() {
                        break;
                    }
                }
                send.finish().ok();
            }));
        }
        for h in handles {
            h.join().ok();
        }

        let got = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        rh.join().ok();
        for p in 0..peers {
            let mine: Vec<u64> = got
                .iter()
                .filter(|v| (*v >> 56) == p)
                .map(|v| v & 0x00FF_FFFF_FFFF_FFFF)
                .collect();
            assert_eq!(
                mine,
                (0..per_peer).collect::<Vec<_>>(),
                "block-RS peer {p} must deliver every item alongside the other peer",
            );
        }
    }

    #[test]
    fn unified_two_peers_deliver_and_are_attributed_separately() {
        use std::sync::mpsc;
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::ForceRlc,
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        let addr = recv.local_addr().unwrap();
        let per_peer: u64 = 150;
        let peers: u64 = 2;
        let total = per_peer * peers;

        let (tx, rx) = mpsc::channel();
        let rh = std::thread::spawn(move || {
            let mut recv = recv;
            let mut got: Vec<(u64, u64)> = Vec::with_capacity(total as usize);
            let start = Instant::now();
            while (got.len() as u64) < total && start.elapsed() < Duration::from_secs(25) {
                let items = recv.poll_from().unwrap_or_default();
                let empty = items.is_empty();
                for (tag, it) in items {
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&it[..8]);
                    got.push((tag, u64::from_le_bytes(s)));
                }
                if empty {
                    std::thread::sleep(Duration::from_micros(200));
                }
            }
            tx.send(got).ok();
        });

        let mut handles = Vec::new();
        for p in 0..peers {
            handles.push(std::thread::spawn(move || {
                let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
                let mut buf = vec![0u8; 8];
                let start = Instant::now();
                for i in 0..per_peer {
                    if start.elapsed() > Duration::from_secs(15) {
                        break;
                    }
                    buf[..8].copy_from_slice(&((p << 56) | i).to_le_bytes());
                    if send.send_item(&buf).is_err() {
                        break;
                    }
                }
                send.finish().ok();
            }));
        }
        for h in handles {
            h.join().ok();
        }

        let got = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        rh.join().ok();

        for p in 0..peers {
            let mine: Vec<u64> = got
                .iter()
                .filter(|(_, v)| (v >> 56) == p)
                .map(|(_, v)| v & 0x00FF_FFFF_FFFF_FFFF)
                .collect();
            assert_eq!(
                mine,
                (0..per_peer).collect::<Vec<_>>(),
                "peer {p} must deliver every item in order alongside the other peer",
            );
            // Every item a peer sent must carry ONE tag, and the two peers'
            // tags must differ - otherwise the attribution is a label, not a
            // routing fact.
            let tags: std::collections::BTreeSet<u64> =
                got.iter().filter(|(_, v)| (v >> 56) == p).map(|(t, _)| *t).collect();
            assert_eq!(tags.len(), 1, "peer {p} items must all carry one tag, got {tags:?}");
        }
        let all_tags: std::collections::BTreeSet<u64> = got.iter().map(|(t, _)| *t).collect();
        assert_eq!(all_tags.len(), 2, "the two peers must be attributed distinctly");
    }

    #[test]
    fn unified_delivers_in_order_across_a_forced_switch() {
        use std::sync::mpsc;
        let sym = 64usize;
        let cfg = UnifiedConfig {
            policy: CodePolicy::default_auto(),
            symbol_len: sym,
            k: 8,
            r: 2,
            rlc_flow_window: 256,
            debug_loss: 0,
            seed: 1,
            rlc_step: 4,
            rlc_static: false,
        };
        let recv = UnifiedSensReceiver::bind("127.0.0.1:0", cfg).unwrap();
        let addr = recv.local_addr().unwrap();
        let n: u64 = 4000;

        let (tx, rx) = mpsc::channel();
        let rh = std::thread::spawn(move || {
            let mut recv = recv;
            let mut got: Vec<u64> = Vec::with_capacity(n as usize);
            let start = Instant::now();
            while (got.len() as u64) < n && start.elapsed() < Duration::from_secs(25) {
                let items = recv.poll().unwrap_or_default();
                let empty = items.is_empty();
                for it in items {
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&it[..8]);
                    got.push(u64::from_le_bytes(s));
                }
                if empty {
                    std::thread::sleep(Duration::from_micros(200));
                }
            }
            tx.send((got, recv.switches())).ok();
        });

        let mut send = UnifiedSensSender::connect("0.0.0.0:0", addr, cfg).unwrap();
        // Items must leave room for the RLC symbol's length prefix
        // (item.len() + LEN_PREFIX <= symbol_len), so ship the 8-byte seq.
        let mut buf = vec![0u8; 8];
        for seq in 0..n / 2 {
            buf[..8].copy_from_slice(&seq.to_le_bytes());
            send.send_item(&buf).unwrap();
        }
        send.force_switch(SensCode::Rs).unwrap();
        assert_eq!(send.active_code(), SensCode::Rs);
        for seq in n / 2..n {
            buf[..8].copy_from_slice(&seq.to_le_bytes());
            send.send_item(&buf).unwrap();
        }
        send.finish().unwrap();

        let (got, rswitches) = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        rh.join().ok();
        assert_eq!(got.len() as u64, n, "every item delivered exactly once");
        for (i, &v) in got.iter().enumerate() {
            assert_eq!(v, i as u64, "delivery in order across the switch at index {i}");
        }
        assert!(rswitches >= 1, "receiver followed the code switch");
    }
}
