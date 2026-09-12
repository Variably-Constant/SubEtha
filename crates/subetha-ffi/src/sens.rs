//! Sens-O-Matic: the loss-adaptive datagram transport, through the C ABI.
//!
//! A sender ships items to one peer and a receiver delivers what arrives,
//! both over a single UDP socket carrying an erasure code that switches
//! between the sliding-window and block forms as measured loss moves. The
//! code in force, and how often it has changed, are in the stats.
//!
//! In strict mode a poll drives the decoder on the calling thread and
//! returns what that call delivered. In managed mode the object owns a
//! thread that drives it, and a poll takes from what that thread has
//! already collected.
//!
//! The TLS entry points seal every item under TLS 1.3, with the handshake
//! carried on the transport's own reliable delivery. They cross the
//! boundary as DER bytes rather than as a configuration object:
//! `subetha_sens_self_signed_cert` mints a certificate and its key, the
//! receiver is built from both and the sender from the certificate alone.
//! The bytes travel between hosts however the caller already moves
//! configuration, which is the shape the QUIC bridge uses.

use std::collections::VecDeque;
use std::ffi::c_char;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use subetha_cxc::sens_rlc::{SensOMaticRlcReceiver, SensOMaticRlcSender};
use subetha_cxc::sens_unified::{SensCode, UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender};
use subetha_cxc::udp_bridge::{SensOMaticRsReceiver, SensOMaticRsSender};

#[cfg(not(feature = "tls"))]
use crate::error::SUBETHA_E_NOT_SUPPORTED;
use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY, SUBETHA_E_RING_IO,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_SENS};
use crate::ring::{out_buffer, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind, SUBETHA_MODE_MANAGED};

/// The half that ships items to a peer.
pub const SUBETHA_SENS_SENDER: u32 = 0;
/// The half that binds and delivers what arrives.
pub const SUBETHA_SENS_RECEIVER: u32 = 1;

/// The sliding-window random linear code, for low to moderate loss.
pub const SUBETHA_SENS_CODE_RLC: u32 = 0;
/// The block Reed-Solomon code, for high sustained loss.
pub const SUBETHA_SENS_CODE_RS: u32 = 1;

/// The accompanying count is a real number.
pub const SUBETHA_DROPS_EXACT: u32 = 0;
/// It happened and this host will not say how many. The count is zero and
/// means nothing.
pub const SUBETHA_DROPS_OCCURRED: u32 = 1;
/// This host reports nothing either way. The count is zero and means
/// nothing.
pub const SUBETHA_DROPS_UNKNOWN: u32 = 2;

fn report_of(report: subetha_cxc::dgram::DropReport) -> u32 {
    match report {
        subetha_cxc::dgram::DropReport::Exact => SUBETHA_DROPS_EXACT,
        subetha_cxc::dgram::DropReport::Occurred => SUBETHA_DROPS_OCCURRED,
        subetha_cxc::dgram::DropReport::Unknown => SUBETHA_DROPS_UNKNOWN,
    }
}

/// A snapshot of one half of a Sens-O-Matic link.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_sens_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// One of `SUBETHA_SENS_SENDER` or `SUBETHA_SENS_RECEIVER`.
    pub role: u32,
    /// The code carrying the stream now: `SUBETHA_SENS_CODE_RLC` or
    /// `SUBETHA_SENS_CODE_RS`.
    pub code: u32,
    /// Times the code has changed since the link came up.
    pub switches: u64,
    /// Items sent, or items delivered on a receiver.
    pub items: u64,
    /// The sender's forward-loss estimate, from zero to one. Zero on a
    /// receiver, which measures loss for the sender rather than itself.
    pub loss: f64,
    /// Datagrams this half put on the wire that the far end never reported
    /// receiving, whatever became of them: a slow reader there, a full
    /// buffer, a lossy link. Read `missed_report` before this number.
    ///
    /// Only a sender can compute it, since only a sender learns both
    /// counts. It advances a feedback window at a time and skips windows
    /// too small to trust, so it lags the wire.
    pub missed: u64,
    /// Whether `missed` is a real number: `SUBETHA_DROPS_EXACT` on a
    /// unified sender, `SUBETHA_DROPS_UNKNOWN` on every receiver and on the
    /// standalone codes, which carry no count of what the far end saw.
    pub missed_report: u32,
    /// Datagrams this half's kernel dropped because its receive buffer was
    /// full, which is what a caller pumping too slowly loses. Read
    /// `kernel_dropped_report` before this number.
    pub kernel_dropped: u64,
    /// What this host will say about `kernel_dropped`. Hosts differ: one
    /// gives a per-socket count, one says only that it happened, one says
    /// nothing. A zero count paired with `SUBETHA_DROPS_OCCURRED` or
    /// `SUBETHA_DROPS_UNKNOWN` is not a claim that none were lost.
    pub kernel_dropped_report: u32,
    /// Datagrams a unified receiver read and no decoder claimed: traffic
    /// that reached the process and was discarded before any decoder saw
    /// it. Zero on every other half.
    pub unroutable: u64,
    /// Data datagrams a sealed receiver dropped because their peer had not
    /// completed its handshake. The peer's own code and retransmits carry
    /// the items again once it has; a count that keeps climbing is a peer
    /// sending data with no handshake at all. Zero without the sealed path.
    pub preauth_dropped: u64,
    /// Items a sealed receiver took from a decoder and could not open: no
    /// completed handshake bound to the item's tag, or a failed seal. Zero
    /// without the sealed path.
    pub unopened: u64,
    /// Handshakes a sealed receiver dropped: a flight its handshake state
    /// rejected, or a peer that went silent before finishing. Zero without
    /// the sealed path.
    pub handshake_failures: u64,
}

fn code_of(code: SensCode) -> u32 {
    match code {
        SensCode::Rlc => SUBETHA_SENS_CODE_RLC,
        SensCode::Rs => SUBETHA_SENS_CODE_RS,
    }
}

/// Every half is boxed: they differ by enough that an unboxed enum is the
/// size of the largest one wherever it is held.
///
/// A standalone code is a half like any other. The erasure code is a
/// swappable detail of the transport, so pinning one is a construction
/// choice rather than a different family, and `subetha_sens_send`,
/// `subetha_sens_poll`, `subetha_sens_local_port` and
/// `subetha_sens_read_stats` serve all six.
enum Half {
    Sender(Box<Mutex<UnifiedSensSender>>),
    Receiver(Box<Mutex<UnifiedSensReceiver>>),
    RlcSender(Box<Mutex<SensOMaticRlcSender>>),
    RlcReceiver(Box<Mutex<SensOMaticRlcReceiver>>),
    RsSender(Box<Mutex<SensOMaticRsSender>>),
    RsReceiver(Box<Mutex<SensOMaticRsReceiver>>),
}

impl Half {
    /// The code carrying this half, which for a standalone endpoint is the
    /// one it was built with and cannot change.
    fn code(&self) -> u32 {
        match self {
            Half::Sender(s) => code_of(s.lock().active_code()),
            Half::Receiver(r) => code_of(r.lock().active_code()),
            Half::RlcSender(_) | Half::RlcReceiver(_) => SUBETHA_SENS_CODE_RLC,
            Half::RsSender(_) | Half::RsReceiver(_) => SUBETHA_SENS_CODE_RS,
        }
    }

    /// Whether this half receives. A sender delivers nothing and a
    /// receiver sends nothing, and both refusals are by role rather than
    /// by code.
    fn receives(&self) -> bool {
        matches!(self, Half::Receiver(_) | Half::RlcReceiver(_) | Half::RsReceiver(_))
    }
}

pub(crate) struct SensObject {
    half: Arc<Half>,
    mode: u32,
    role: u32,
    items: Arc<AtomicU64>,
    /// What a receiver has delivered and no caller has taken yet. A poll
    /// yields one item, and the decoder hands them over in batches.
    delivered: Arc<Mutex<VecDeque<Vec<u8>>>>,
    stop: Arc<AtomicBool>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl SensObject {
    /// Stop the managed thread and wait for it, so a destroy never leaves
    /// a poll running against a socket that is about to close.
    pub(crate) fn interrupt(&self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.worker.lock().take()
            && handle.join().is_err()
        {
            eprintln!("subetha: a Sens-O-Matic poll thread panicked");
        }
    }
}

impl Drop for SensObject {
    fn drop(&mut self) {
        self.interrupt();
    }
}

fn with_sens(handle: subetha_handle, f: impl FnOnce(&SensObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_SENS, |object| match object {
        Object::Sens(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a Sens-O-Matic half"),
    })
}

/// # Safety
/// `addr` is a NUL-terminated UTF-8 string.
unsafe fn address(addr: *const c_char, what: &str) -> Result<SocketAddr, i32> {
    let text = unsafe { text(addr, what) }?;
    text.parse()
        .map_err(|e| fail(SUBETHA_E_INVALID_ARGUMENT, format!("{what} {text}: {e}")))
}

/// Drive whichever decoder this half holds and put everything it has ready
/// into `queue`, counting it. A sending half has no decoder and drains
/// nothing, which is not an error here: the roles are separated at the
/// entry point, where the caller can be told.
fn drain_into(
    half: &Half,
    queue: &Mutex<VecDeque<Vec<u8>>>,
    items: &AtomicU64,
) -> Result<(), i32> {
    let io = |e: std::io::Error| fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind()));
    let ready = match half {
        Half::Receiver(r) => r.lock().poll().map_err(io)?,
        Half::RlcReceiver(r) => r.lock().poll().map_err(io)?,
        Half::RsReceiver(r) => r.lock().poll().map_err(io)?,
        _ => return Ok(()),
    };
    if ready.is_empty() {
        return Ok(());
    }
    items.fetch_add(ready.len() as u64, Ordering::Relaxed);
    queue.lock().extend(ready);
    Ok(())
}

/// Issue a handle for a built sending half.
///
/// # Safety
/// `out` is a valid pointer.
unsafe fn issue_sender(sender: UnifiedSensSender, mode: u32, out: *mut subetha_handle) -> i32 {
    unsafe { issue_half(Half::Sender(Box::new(Mutex::new(sender))), mode, out) }
}

/// Issue a handle for a built receiving half.
///
/// # Safety
/// `out` is a valid pointer.
unsafe fn issue_receiver(receiver: UnifiedSensReceiver, mode: u32, out: *mut subetha_handle) -> i32 {
    unsafe { issue_half(Half::Receiver(Box::new(Mutex::new(receiver))), mode, out) }
}

/// Issue a handle for any built half, and where it receives in managed
/// mode give it the thread that drives its decoder. Every constructor in
/// this family ends here, so no two of them can drift on what managed
/// mode means or on which role a half reports.
///
/// # Safety
/// `out` is a valid pointer.
unsafe fn issue_half(half: Half, mode: u32, out: *mut subetha_handle) -> i32 {
    let receives = half.receives();
    let object = SensObject {
        half: Arc::new(half),
        mode,
        role: if receives { SUBETHA_SENS_RECEIVER } else { SUBETHA_SENS_SENDER },
        items: Arc::new(AtomicU64::new(0)),
        delivered: Arc::new(Mutex::new(VecDeque::new())),
        stop: Arc::new(AtomicBool::new(false)),
        worker: Mutex::new(None),
    };
    if !receives {
        return unsafe { issue(Object::Sens(object), out) };
    }
    let code = unsafe { issue(Object::Sens(object), out) };
    if code != SUBETHA_OK || mode != SUBETHA_MODE_MANAGED {
        return code;
    }
    // The handle was just issued, so the borrow below names a live object.
    let handle = unsafe { *out };
    with_sens(handle, |s| {
        let half = Arc::clone(&s.half);
        let queue = Arc::clone(&s.delivered);
        let items = Arc::clone(&s.items);
        let stop = Arc::clone(&s.stop);
        let spawned = std::thread::Builder::new()
            .name("subetha-sens-poll".to_owned())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    if drain_into(&half, &queue, &items).is_err() {
                        // The code is reported to whoever polls next;
                        // a thread has nobody to return it to.
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            });
        match spawned {
            Ok(h) => {
                *s.worker.lock() = Some(h);
                SUBETHA_OK
            }
            Err(e) => fail(SUBETHA_E_RING_IO, format!("no thread for the poll: {e}")),
        }
    })
}

/// Make the sending half: it ships items from `local_addr` to `peer_addr`,
/// coding them in symbols of `symbol_len` bytes.
///
/// # Safety
/// `local_addr` and `peer_addr` are NUL-terminated UTF-8 strings; `out` is
/// a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_sender(
    local_addr: *const c_char,
    peer_addr: *const c_char,
    symbol_len: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if symbol_len == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "symbol_len is zero");
        }
        let local = match unsafe { address(local_addr, "local_addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let peer = match unsafe { address(peer_addr, "peer_addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let sender = match UnifiedSensSender::connect(local, peer, UnifiedConfig::new(symbol_len)) {
            Ok(s) => s,
            Err(e) => return fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        };
        unsafe { issue_sender(sender, mode, out) }
    })
}

/// Make the receiving half: it binds `local_addr` and delivers what
/// arrives, decoding symbols of `symbol_len` bytes. Port zero binds a port
/// the system chooses, which `subetha_sens_local_port` reports.
///
/// In managed mode the object starts a thread that drives the decoder, so
/// items are collected whether or not a caller is polling.
///
/// # Safety
/// `local_addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_receiver(
    local_addr: *const c_char,
    symbol_len: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if symbol_len == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "symbol_len is zero");
        }
        let local = match unsafe { address(local_addr, "local_addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let receiver = match UnifiedSensReceiver::bind(local, UnifiedConfig::new(symbol_len)) {
            Ok(r) => r,
            Err(e) => return fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        };
        unsafe { issue_receiver(receiver, mode, out) }
    })
}

/// Ship `len` bytes at `bytes` to the peer.
///
/// # Safety
/// `bytes` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_send(
    handle: subetha_handle,
    bytes: *const u8,
    len: usize,
) -> i32 {
    with_sens(handle, |s| {
        if bytes.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "bytes is null");
        }
        if s.half.receives() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a receiving half sends nothing");
        }
        // The caller guarantees `len` readable bytes at a non-null pointer.
        let item = unsafe { std::slice::from_raw_parts(bytes, len) };
        let sent = match &*s.half {
            Half::Sender(sender) => sender.lock().send_item(item),
            Half::RlcSender(sender) => sender.lock().send_item(item),
            Half::RsSender(sender) => sender.lock().send_item(item),
            // Refused above by role, so this arm is unreachable rather
            // than a case that needs an answer.
            _ => return fail(SUBETHA_E_INVALID_ARGUMENT, "a receiving half sends nothing"),
        };
        match sent {
            Ok(()) => {
                s.items.fetch_add(1, Ordering::Relaxed);
                SUBETHA_OK
            }
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    })
}

/// Push out whatever this half is holding back, and answer once it has
/// gone to the socket.
///
/// A block code fills `k` data shards before it codes and transmits, so a
/// caller with fewer items than that in hand has a stream that has not
/// left yet and no elapsed time will move it. This is the call that moves
/// it, and a caller sending in bursts smaller than a block, or ending a
/// stream, needs it.
///
/// The sliding-window code and the unified endpoint send each item as it
/// arrives, so on those halves this succeeds having done nothing. It is
/// answered the same way on every half on purpose: a caller that flushes
/// where it should is then correct whichever code it was handed, which is
/// the point of the code being a swappable detail.
///
/// A receiving half holds nothing back and refuses.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_sens_flush(handle: subetha_handle) -> i32 {
    with_sens(handle, |s| {
        if s.half.receives() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a receiving half holds nothing back");
        }
        match &*s.half {
            Half::RsSender(sender) => match sender.lock().flush() {
                Ok(()) => SUBETHA_OK,
                Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
            },
            _ => SUBETHA_OK,
        }
    })
}

/// The next delivered item into `out`, its length into `out_len`.
/// `SUBETHA_E_RING_EMPTY` when nothing has arrived yet.
///
/// In strict mode this drives the decoder on the calling thread first.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_poll(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_sens(handle, |s| {
        if !s.half.receives() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a sending half delivers nothing");
        }
        if s.mode != SUBETHA_MODE_MANAGED
            && let Err(code) = drain_into(&s.half, &s.delivered, &s.items)
        {
            return code;
        }
        let Some(item) = s.delivered.lock().pop_front() else {
            return SUBETHA_E_RING_EMPTY;
        };
        let buf = match unsafe { out_buffer(out, cap, out_len, item.len()) } {
            Ok(b) => b,
            Err(code) => {
                // The item is not lost for want of a buffer: it goes back
                // at the front so the next call with room still gets it.
                s.delivered.lock().push_front(item);
                return code;
            }
        };
        buf[..item.len()].copy_from_slice(&item);
        // Checked non-null by out_buffer; the caller guarantees it is writable.
        unsafe { *out_len = item.len() };
        SUBETHA_OK
    })
}

/// The port this half's socket is bound to, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_local_port(handle: subetha_handle, out: *mut u16) -> i32 {
    with_sens(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let addr = match &*s.half {
            Half::Sender(sender) => sender.lock().local_addr(),
            Half::Receiver(receiver) => receiver.lock().local_addr(),
            Half::RlcSender(sender) => sender.lock().local_addr(),
            Half::RlcReceiver(receiver) => receiver.lock().local_addr(),
            Half::RsSender(sender) => sender.lock().local_addr(),
            Half::RsReceiver(receiver) => receiver.lock().local_addr(),
        };
        match addr {
            Ok(a) => {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = a.port() };
                SUBETHA_OK
            }
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    })
}

/// A snapshot of this half into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_read_stats(
    handle: subetha_handle,
    out: *mut subetha_sens_stats,
) -> i32 {
    with_sens(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // A standalone endpoint reports zero switches because it cannot
        // switch: the code was pinned at construction. Zero is the true
        // count rather than a stand-in for one this layer cannot read.
        //
        // `missed` needs both what was sent and what arrived, and only a
        // unified sender learns the second, so every other half says it
        // does not know rather than reporting a zero.
        let (switches, loss, missed, missed_report, drops) = match &*s.half {
            Half::Sender(sender) => {
                let sender = sender.lock();
                (
                    sender.switches(),
                    sender.raw_loss_estimate(),
                    sender.missed(),
                    SUBETHA_DROPS_EXACT,
                    sender.kernel_drops(),
                )
            }
            Half::Receiver(receiver) => {
                let receiver = receiver.lock();
                (
                    receiver.switches(),
                    0.0,
                    0,
                    SUBETHA_DROPS_UNKNOWN,
                    receiver.kernel_drops(),
                )
            }
            Half::RlcReceiver(r) => (0, 0.0, 0, SUBETHA_DROPS_UNKNOWN, r.lock().kernel_drops()),
            Half::RsReceiver(r) => (0, 0.0, 0, SUBETHA_DROPS_UNKNOWN, r.lock().kernel_drops()),
            // A standalone sender's socket carries feedback rather than
            // data, and neither code reports the far end's arrival count,
            // so there is nothing here either half can answer with.
            Half::RlcSender(_) | Half::RsSender(_) => {
                (0, 0.0, 0, SUBETHA_DROPS_UNKNOWN, (0, subetha_cxc::dgram::DropReport::Unknown))
            }
        };
        // Where an item went that was not delivered: read only from a
        // unified receiver, which is the half that demuxes and, sealed,
        // handshakes. Every other half has none of these to count.
        let (unroutable, sealed) = if let Half::Receiver(receiver) = &*s.half {
            let receiver = receiver.lock();
            // A receiver fed by a demux outside it has no count of its own.
            // The ABI builds none of those, so a missing count is a
            // receiver this layer did not make.
            let unroutable = match receiver.demux_unroutable() {
                Some(n) => n,
                None => return fail(SUBETHA_E_RING_IO, "this receiver has no demux of its own to count with"),
            };
            (unroutable, sealed_counts(&receiver))
        } else {
            (0, (0, 0, 0))
        };
        let code = s.half.code();
        let stats = subetha_sens_stats {
            mode: s.mode,
            role: s.role,
            code,
            switches,
            items: s.items.load(Ordering::Relaxed),
            loss,
            missed,
            missed_report,
            kernel_dropped: drops.0,
            kernel_dropped_report: report_of(drops.1),
            unroutable,
            preauth_dropped: sealed.0,
            unopened: sealed.1,
            handshake_failures: sealed.2,
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// A sealed receiver's own accounting of what it dropped before, at and
/// after a handshake: `(preauth_dropped, unopened, handshake_failures)`.
#[cfg(feature = "tls")]
fn sealed_counts(receiver: &UnifiedSensReceiver) -> (u64, u64, u64) {
    (receiver.tls_preauth_dropped(), receiver.tls_unopened(), receiver.handshake_failures())
}

/// Without the sealed path nothing handshakes, so nothing is dropped at
/// one.
#[cfg(not(feature = "tls"))]
fn sealed_counts(_receiver: &UnifiedSensReceiver) -> (u64, u64, u64) {
    (0, 0, 0)
}

/// Wait up to `timeout_ms` for the far end to acknowledge everything this
/// sending half has sent, retransmitting what it has not, and write
/// whether it all was into `out_acked`. This is what ends a stream: a send
/// returns once an item is on the wire, and the last items of a stream
/// have nothing after them to carry their repair, so a sender that closes
/// without this can lose its tail on a lossy path, and a stream whose
/// head was lost delivers nothing at all until the head is sent again.
///
/// `false` in `out_acked` is the ordinary answer for a peer that stopped
/// responding; the caller decides whether that is a failure. A receiving
/// half has nothing to finish and refuses.
///
/// # Safety
/// `out_acked` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_finish(
    handle: subetha_handle,
    timeout_ms: i64,
    out_acked: *mut bool,
) -> i32 {
    with_sens(handle, |s| {
        if out_acked.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_acked is null");
        }
        if s.half.receives() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a receiving half has nothing to finish");
        }
        let timeout_ms = match u64::try_from(timeout_ms) {
            Ok(t) => t,
            Err(e) => return fail(SUBETHA_E_INVALID_ARGUMENT, format!("a finish has a deadline: {e}")),
        };
        let deadline = std::time::Duration::from_millis(timeout_ms);
        let io = |e: std::io::Error| fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind()));
        let acked = if let Half::Sender(sender) = &*s.half {
            sender.lock().finish_within(deadline).map_err(io)
        } else if let Half::RlcSender(sender) = &*s.half {
            let mut sender = sender.lock();
            let target = sender.next_source_id();
            sender.drain_until_acked(target, deadline).map_err(io)
        } else if let Half::RsSender(sender) = &*s.half {
            let mut sender = sender.lock();
            match sender.flush() {
                Ok(()) => sender.drain_until_acked(deadline).map_err(io),
                Err(e) => Err(io(e)),
            }
        } else {
            // Refused above by role; every receiving half lands here.
            Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a receiving half has nothing to finish"))
        };
        match acked {
            Ok(acked) => {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_acked = acked };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// What a build without the feature answers, naming the feature so the
/// caller is told what to turn on rather than that the call is unknown.
#[cfg(not(feature = "tls"))]
fn unavailable() -> i32 {
    fail(SUBETHA_E_NOT_SUPPORTED, "this library was built without the tls feature")
}

/// Make a sending half carrying the sliding-window code alone, with no
/// switching: the code is pinned at construction and stays.
///
/// `window` source symbols are kept live, one repair goes out every `step`
/// symbols, and `density` is the coding density threshold. These are the
/// knobs the unified endpoint sets from measured loss; naming them is the
/// reason to reach for this constructor instead of it.
///
/// # Safety
/// `local_addr` and `peer_addr` are NUL-terminated UTF-8 strings; `out` is
/// a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_sens_rlc_sender(
    local_addr: *const c_char,
    peer_addr: *const c_char,
    symbol_len: usize,
    window: usize,
    step: usize,
    density: u8,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (local, peer, mode) =
            match unsafe { sender_arguments(local_addr, peer_addr, symbol_len, mode, out) } {
                Ok(v) => v,
                Err(code) => return code,
            };
        if window == 0 || step == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "window and step are each at least 1");
        }
        match SensOMaticRlcSender::bind(local, peer, window, step, density, symbol_len) {
            Ok(s) => unsafe { issue_half(Half::RlcSender(Box::new(Mutex::new(s))), mode, out) },
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    })
}

/// Make a receiving half for the sliding-window code alone. Port zero binds
/// a port the system chooses, which `subetha_sens_local_port` reports.
///
/// No session exists until a peer is seen, and each connection id that
/// arrives opens its own, so one receiver serves several senders without
/// one of them evicting another's decode window.
///
/// # Safety
/// `local_addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_rlc_receiver(
    local_addr: *const c_char,
    symbol_len: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (local, mode) =
            match unsafe { receiver_arguments(local_addr, symbol_len, mode, out) } {
                Ok(v) => v,
                Err(code) => return code,
            };
        match SensOMaticRlcReceiver::bind(local, symbol_len) {
            Ok(r) => unsafe { issue_half(Half::RlcReceiver(Box::new(Mutex::new(r))), mode, out) },
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    })
}

/// Make a sending half carrying the block Reed-Solomon code alone, with no
/// switching: the code is pinned at construction and stays.
///
/// A block is `k` data shards and `r` parity shards, and `max_item` is the
/// largest item this half will be given. `k + r` is capped by the wire
/// format's per-block shard bitmap, and a request past it is refused by the
/// transport rather than trimmed.
///
/// # Safety
/// `local_addr` and `peer_addr` are NUL-terminated UTF-8 strings; `out` is
/// a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_sens_rs_sender(
    local_addr: *const c_char,
    peer_addr: *const c_char,
    k: usize,
    r: usize,
    max_item: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        // max_item stands in for the symbol length here: it is what this
        // half must be able to carry, and zero is as meaningless.
        let (local, peer, mode) =
            match unsafe { sender_arguments(local_addr, peer_addr, max_item, mode, out) } {
                Ok(v) => v,
                Err(code) => return code,
            };
        if k == 0 || r == 0 {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "k and r are each at least 1: a block with no data shards \
                 carries nothing and one with no parity corrects nothing",
            );
        }
        match SensOMaticRsSender::bind(local, peer, k, r, max_item) {
            Ok(s) => unsafe { issue_half(Half::RsSender(Box::new(Mutex::new(s))), mode, out) },
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    })
}

/// Make a receiving half for the block Reed-Solomon code alone. Port zero
/// binds a port the system chooses, which `subetha_sens_local_port`
/// reports.
///
/// The block geometry rides the wire, so this half takes none: it decodes
/// whatever `k` and `r` a sender chose.
///
/// # Safety
/// `local_addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_rs_receiver(
    local_addr: *const c_char,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        // The symbol length is carried per block rather than fixed at
        // construction, so 1 stands for "a length this half does not set"
        // and only the zero check on the other constructors is skipped.
        let (local, mode) = match unsafe { receiver_arguments(local_addr, 1, mode, out) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match SensOMaticRsReceiver::bind(local) {
            Ok(r) => unsafe { issue_half(Half::RsReceiver(Box::new(Mutex::new(r))), mode, out) },
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    })
}

/// The checks every sending constructor makes before it binds anything, so
/// the six of them cannot disagree about what a bad argument is.
///
/// # Safety
/// The address arguments are NUL-terminated UTF-8 strings.
unsafe fn sender_arguments(
    local_addr: *const c_char,
    peer_addr: *const c_char,
    symbol_len: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(SocketAddr, SocketAddr, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    if symbol_len == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "symbol_len is zero"));
    }
    let local = unsafe { address(local_addr, "local_addr") }?;
    let peer = unsafe { address(peer_addr, "peer_addr") }?;
    let mode = resolve_mode(mode)?;
    Ok((local, peer, mode))
}

/// The same for every receiving constructor.
///
/// # Safety
/// `local_addr` is a NUL-terminated UTF-8 string.
unsafe fn receiver_arguments(
    local_addr: *const c_char,
    symbol_len: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(SocketAddr, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    if symbol_len == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "symbol_len is zero"));
    }
    let local = unsafe { address(local_addr, "local_addr") }?;
    let mode = resolve_mode(mode)?;
    Ok((local, mode))
}

/// Whether this library carries the sealed Sens path, into `out`. A caller
/// choosing a path at run time asks this rather than making a call and
/// reading the refusal.
///
/// Separate from `subetha_transports_available`, which answers for the
/// bridges: a build can carry one and not the other, since the bridges
/// pull an async runtime and this path does not.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_tls_available(out: *mut bool) -> i32 {
    entry(|| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let available = cfg!(feature = "tls");
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = available };
        SUBETHA_OK
    })
}

/// Mint a self-signed certificate for `sni` and copy it and its private
/// key out as DER bytes. `cert_len` and `key_len` receive the byte counts
/// the call produced, or the counts it would need when a buffer is too
/// small, in which case the answer is `SUBETHA_E_BUFFER_TOO_SMALL` and
/// nothing is copied.
///
/// The receiving half needs both; a sending half needs the certificate
/// alone, and trusts exactly that one. `sni` names the certificate rather
/// than the wire address, so a peer reached at any address presents it.
/// Passing null for `sni` takes the transport's own default name, which is
/// what `subetha_sens_sender_tls` asserts when given no name of its own.
///
/// # Safety
/// `sni` is null or a NUL-terminated UTF-8 string. `cert_out` points to
/// `cert_cap` writable bytes and `key_out` to `key_cap`; both lengths are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_sens_self_signed_cert(
    sni: *const c_char,
    cert_out: *mut u8,
    cert_cap: usize,
    cert_len: *mut usize,
    key_out: *mut u8,
    key_cap: usize,
    key_len: *mut usize,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        #[cfg(feature = "tls")]
        {
            unsafe { tls::self_signed(sni, cert_out, cert_cap, cert_len, key_out, key_cap, key_len) }
        }
        #[cfg(not(feature = "tls"))]
        {
            let _unused = (sni, cert_out, cert_cap, cert_len, key_out, key_cap, key_len);
            unavailable()
        }
    })
}

/// Make a sending half that handshakes with `peer_addr` before any item
/// moves, and seals every item after it.
///
/// # Safety
/// `local_addr` and `peer_addr` are NUL-terminated UTF-8 strings;
/// `server_name` is null or one; `cert` points to `cert_len` readable
/// bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_sens_sender_tls(
    local_addr: *const c_char,
    peer_addr: *const c_char,
    symbol_len: usize,
    mode: u32,
    cert: *const u8,
    cert_len: usize,
    server_name: *const c_char,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        #[cfg(feature = "tls")]
        {
            unsafe {
                tls::sender(local_addr, peer_addr, symbol_len, mode, cert, cert_len, server_name, out)
            }
        }
        #[cfg(not(feature = "tls"))]
        {
            let _unused =
                (local_addr, peer_addr, symbol_len, mode, cert, cert_len, server_name, out);
            unavailable()
        }
    })
}

/// Make a receiving half that serves sealed senders: it binds `local_addr`
/// and stands up before any peer dials, and each dialing peer handshakes
/// on its own and seals its own stream.
///
/// `cert` and `key` are the DER certificate and private key this half
/// presents, both required. `peers` is the number of concurrent senders it
/// is provisioned for and must be at least one.
///
/// `code` pins the erasure code to `SUBETHA_SENS_CODE_RLC` or
/// `SUBETHA_SENS_CODE_RS`, and there is no automatic choice here. That is
/// the transport's own refusal rather than a limit this layer invents: a
/// switch boundary belongs to one endpoint, so an automatic switch under
/// several peers would misdeliver silently. A single-peer caller who wants
/// the code to follow measured loss uses `subetha_sens_receiver`.
///
/// # Safety
/// `local_addr` is a NUL-terminated UTF-8 string; `cert` points to
/// `cert_len` readable bytes and `key` to `key_len`; `out` is a valid
/// pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_sens_receiver_tls(
    local_addr: *const c_char,
    symbol_len: usize,
    mode: u32,
    cert: *const u8,
    cert_len: usize,
    key: *const u8,
    key_len: usize,
    code: u32,
    peers: usize,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(rc) = require_initialized() {
            return rc;
        }
        #[cfg(feature = "tls")]
        {
            unsafe {
                tls::receiver(
                    local_addr, symbol_len, mode, cert, cert_len, key, key_len, code, peers, out,
                )
            }
        }
        #[cfg(not(feature = "tls"))]
        {
            let _unused =
                (local_addr, symbol_len, mode, cert, cert_len, key, key_len, code, peers, out);
            unavailable()
        }
    })
}

#[cfg(feature = "tls")]
mod tls {
    use std::ffi::c_char;

    use subetha_cxc::rlc_crypto;
    use subetha_cxc::sens_unified::{
        CodePolicy, UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender,
    };

    use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_OK};
    use crate::handle::subetha_handle;
    use crate::ring::{copy_out, text};
    use crate::runtime::resolve_mode;

    use super::{
        address, issue_receiver, issue_sender, SUBETHA_SENS_CODE_RLC, SUBETHA_SENS_CODE_RS,
    };

    /// A refusal from the crypto layer is the configuration talking rather
    /// than the caller's arguments, so it carries the transport's io code
    /// with that layer's own words instead of being flattened into an
    /// argument error.
    fn crypto_failed(what: &str, e: String) -> i32 {
        fail(SUBETHA_E_RING_IO, format!("{what}: {e}"))
    }

    /// The bytes a pointer and length name, refusing an empty one by name:
    /// a zero-length certificate reaches the crypto layer as an unhelpful
    /// parse error.
    ///
    /// # Safety
    /// `data` is null or points to `len` readable bytes.
    unsafe fn der<'a>(data: *const u8, len: usize, what: &str) -> Result<&'a [u8], i32> {
        if data.is_null() {
            return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("{what} is null")));
        }
        if len == 0 {
            return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("{what} is empty")));
        }
        // The caller guarantees `len` readable bytes at a non-null pointer.
        Ok(unsafe { std::slice::from_raw_parts(data, len) })
    }

    /// # Safety
    /// The pointer contract of `subetha_sens_self_signed_cert`.
    pub(super) unsafe fn self_signed(
        sni: *const c_char,
        cert_out: *mut u8,
        cert_cap: usize,
        cert_len: *mut usize,
        key_out: *mut u8,
        key_cap: usize,
        key_len: *mut usize,
    ) -> i32 {
        let pair = if sni.is_null() {
            rlc_crypto::self_signed_cert()
        } else {
            match unsafe { text(sni, "sni") } {
                Ok(name) => rlc_crypto::self_signed_cert_for(&[name]),
                Err(code) => return code,
            }
        };
        let (cert, key) = match pair {
            Ok(pair) => pair,
            Err(e) => return crypto_failed("no self-signed certificate", e),
        };
        // Both copies are attempted before either result is read, so a
        // caller that sized one buffer right and the other wrong learns
        // both counts from one call. The certificate's code is reported
        // first when neither fits.
        let cert_fit = unsafe { copy_out(&cert, cert_out, cert_cap, cert_len) };
        let key_fit = unsafe { copy_out(&key, key_out, key_cap, key_len) };
        if let Err(code) = cert_fit {
            return code;
        }
        if let Err(code) = key_fit {
            return code;
        }
        SUBETHA_OK
    }

    /// # Safety
    /// The pointer contract of `subetha_sens_sender_tls`.
    #[allow(clippy::too_many_arguments)]
    pub(super) unsafe fn sender(
        local_addr: *const c_char,
        peer_addr: *const c_char,
        symbol_len: usize,
        mode: u32,
        cert: *const u8,
        cert_len: usize,
        server_name: *const c_char,
        out: *mut subetha_handle,
    ) -> i32 {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if symbol_len == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "symbol_len is zero");
        }
        let local = match unsafe { address(local_addr, "local_addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let peer = match unsafe { address(peer_addr, "peer_addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let trusted = match unsafe { der(cert, cert_len, "cert") } {
            Ok(d) => d,
            Err(code) => return code,
        };
        let cfg = match rlc_crypto::client_config(trusted) {
            Ok(c) => c,
            Err(e) => return crypto_failed("the certificate was not usable as a trust root", e),
        };
        let config = UnifiedConfig::new(symbol_len);
        let built = if server_name.is_null() {
            UnifiedSensSender::connect_tls(local, peer, config, cfg)
        } else {
            match unsafe { text(server_name, "server_name") } {
                Ok(name) => UnifiedSensSender::connect_tls_named(local, peer, config, cfg, name),
                Err(code) => return code,
            }
        };
        match built {
            Ok(s) => unsafe { issue_sender(s, mode, out) },
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    }

    /// # Safety
    /// The pointer contract of `subetha_sens_receiver_tls`.
    #[allow(clippy::too_many_arguments)]
    pub(super) unsafe fn receiver(
        local_addr: *const c_char,
        symbol_len: usize,
        mode: u32,
        cert: *const u8,
        cert_len: usize,
        key: *const u8,
        key_len: usize,
        code: u32,
        peers: usize,
        out: *mut subetha_handle,
    ) -> i32 {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if symbol_len == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "symbol_len is zero");
        }
        if peers == 0 {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "a listener provisioned for zero peers serves nobody; pass the \
                 concurrent sender count it is for",
            );
        }
        let policy = match code {
            SUBETHA_SENS_CODE_RLC => CodePolicy::ForceRlc,
            SUBETHA_SENS_CODE_RS => CodePolicy::ForceRs,
            other => {
                return fail(
                    SUBETHA_E_INVALID_ARGUMENT,
                    format!(
                        "code {other} names no erasure code; a sealed listener pins \
                         SUBETHA_SENS_CODE_RLC or SUBETHA_SENS_CODE_RS, because a \
                         switch boundary belongs to one endpoint and an automatic \
                         switch under several peers would misdeliver"
                    ),
                )
            }
        };
        let local = match unsafe { address(local_addr, "local_addr") } {
            Ok(a) => a,
            Err(rc) => return rc,
        };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(rc) => return rc,
        };
        let cert_der = match unsafe { der(cert, cert_len, "cert") } {
            Ok(d) => d,
            Err(rc) => return rc,
        };
        let key_der = match unsafe { der(key, key_len, "key") } {
            Ok(d) => d,
            Err(rc) => return rc,
        };
        let cfg = match rlc_crypto::server_config(cert_der, key_der) {
            Ok(c) => c,
            Err(e) => return crypto_failed("the certificate and key were not usable", e),
        };
        let mut config = UnifiedConfig::new(symbol_len);
        config.policy = policy;
        match UnifiedSensReceiver::listen_tls(local, config, cfg, peers) {
            Ok(r) => unsafe { issue_receiver(r, mode, out) },
            Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
        }
    }
}
