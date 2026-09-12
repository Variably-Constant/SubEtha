//! The pub/sub ring through the C ABI: one publisher that never waits for
//! anyone, and subscribers that each carry their own absolute position,
//! anonymous or in a file that survives a restart. A publish assigns the
//! item a position; a read at a position finds it, or finds it not yet
//! published, or finds it overwritten, in which case the subscriber's
//! position moves to the ring's head. The ABI keeps one waker beside the
//! ring so a subscriber can wait for the next publish. Strict and managed
//! modes are the same here.
//!
//! In the file locale `path` names the ring file itself, as in the Rust
//! API, with the waker at `<path>.cwaker.bin`; in shared memory the ring
//! is the region `{name}` and the waker `{name}_cwaker`.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::protocol_pubsub::{pubsub_ring_file_size, PubSubReadError, PubSubRing};
use subetha_cxc::replay_positions::SubscriberPosition;

use crate::error::{
    fail, io_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_PUBSUB_LOST, SUBETHA_E_PUBSUB_PENDING,
    SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_PUBSUB, SUBETHA_KIND_SUBSCRIBER};
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, namespace, out_buffer, read_options, shm_region,
    subetha_ring_options, subetha_unlink_report, text, waker_for, with_suffix, Locale,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting};

/// Bytes one pub/sub slot carries; a publish zero-fills past its payload
/// and a read yields the whole slot.
pub const SUBETHA_PUBSUB_PAYLOAD_BYTES: usize = 56;

/// A snapshot of a pub/sub ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_pubsub_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots in the ring.
    pub capacity: u64,
    /// The position the next publish takes; items published so far.
    pub head: u64,
}

pub(crate) struct PubSubObject {
    ring: Arc<PubSubRing>,
    waker: Arc<CrossProcessWaker>,
    mode: u32,
}

/// Where a subscriber keeps its position: in this handle, or in a file
/// it can reopen after a restart.
enum Position {
    Anon(AtomicU64),
    File(SubscriberPosition),
}

impl Position {
    fn get(&self) -> u64 {
        match self {
            Position::Anon(p) => p.load(Ordering::Acquire),
            Position::File(p) => p.get(),
        }
    }

    fn advance(&self, by: u64) -> u64 {
        match self {
            Position::Anon(p) => p.fetch_add(by, Ordering::AcqRel) + by,
            Position::File(p) => p.advance(by),
        }
    }

    fn set(&self, to: u64) {
        match self {
            Position::Anon(p) => p.store(to, Ordering::Release),
            Position::File(p) => p.set(to),
        }
    }
}

pub(crate) struct SubscriberObject {
    ring: Arc<PubSubRing>,
    waker: Arc<CrossProcessWaker>,
    position: Position,
    mode: u32,
    waiting: Waiting,
}

impl PubSubObject {
    fn build(ring: PubSubRing, locale: &Locale<'_>, options: subetha_ring_options) -> Result<Self, i32> {
        let mode = resolve_mode(options.mode)?;
        let waker = waker_for(locale, options.max_waiters, ".cwaker.bin", "_cwaker")?;
        Ok(Self {
            ring: Arc::new(ring),
            waker: Arc::new(waker),
            mode,
        })
    }

    /// Nothing parks on the ring handle itself; subscribers park on the
    /// shared waker and are woken by their own destroy.
    pub(crate) fn interrupt(&self) {}

    fn publish(&self, payload: &[u8]) -> u64 {
        let position = self.ring.publish(payload);
        self.waker.wake_all();
        position
    }

    fn stats(&self) -> subetha_pubsub_stats {
        subetha_pubsub_stats {
            mode: self.mode,
            capacity: self.ring.capacity() as u64,
            head: self.ring.head(),
        }
    }
}

impl SubscriberObject {
    /// Wake this subscriber's parked wait so a destroy can proceed. The
    /// waker is shared with every subscriber; the others re-check and
    /// park again.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.waker.wake_all();
    }

    /// Read the item at the subscriber's position and advance past it. A
    /// lost position moves to the ring's head before the code is returned.
    fn try_next(&self, out: &mut [u8]) -> Result<(), i32> {
        let position = self.position.get();
        match self.ring.read_at(position, out) {
            Ok(()) => {
                self.position.advance(1);
                Ok(())
            }
            Err(PubSubReadError::Lost) => {
                self.position.set(self.ring.head());
                Err(fail(
                    SUBETHA_E_PUBSUB_LOST,
                    format!("position {position} was overwritten; the subscriber now stands at the head"),
                ))
            }
            Err(PubSubReadError::Pending) => Err(SUBETHA_E_PUBSUB_PENDING),
        }
    }

    fn next_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, &self.waker, deadline, SUBETHA_E_PUBSUB_PENDING, || self.try_next(out))
    }
}

fn with_pubsub(handle: subetha_handle, f: impl FnOnce(&PubSubObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_PUBSUB, |object| match object {
        Object::PubSub(p) => f(p),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a pub/sub ring"),
    })
}

fn with_subscriber(handle: subetha_handle, f: impl FnOnce(&SubscriberObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_SUBSCRIBER, |object| match object {
        Object::Subscriber(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a subscriber"),
    })
}

fn place(ring: std::io::Result<PubSubRing>, locale: &Locale<'_>, options: subetha_ring_options, out: *mut subetha_handle) -> i32 {
    let ring = match ring {
        Ok(r) => r,
        Err(e) => return io_code(e),
    };
    match PubSubObject::build(ring, locale, options) {
        Ok(object) => unsafe { issue(Object::PubSub(object), out) },
        Err(code) => code,
    }
}

/// Create a pub/sub ring in anonymous memory. `capacity` is a power of two
/// of at least 2; `scan_interval_us` in the options is ignored, the ring
/// has no sidecar.
///
/// # Safety
/// `options` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_create_anon(capacity: u32, options: *const subetha_ring_options, out: *mut subetha_handle) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let cap = match checked_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        place(PubSubRing::create_anon(cap), &Locale::Anon, options, out)
    })
}

/// Create a file-backed pub/sub ring at `path`, or attach to one that
/// exists there with the same capacity, with the waker at
/// `<path>.cwaker.bin`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_create(
    path: *const c_char,
    capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let path = match unsafe { text(path, "path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let cap = match checked_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        place(PubSubRing::create(path, cap), &Locale::File(path), options, out)
    })
}

/// Attach to a file-backed pub/sub ring another process created at `path`.
/// `SUBETHA_E_RING_LAYOUT_MISMATCH` when the file is absent or was created
/// with another capacity.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_open(
    path: *const c_char,
    expected_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let path = match unsafe { text(path, "path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let cap = match checked_capacity(expected_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        place(PubSubRing::open(path, cap), &Locale::File(path), options, out)
    })
}

/// Create a pub/sub ring in the named shared-memory region `name`, with
/// its waker in `{name}_cwaker`.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_create_shm(
    name: *const c_char,
    capacity: u32,
    shm_namespace: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let name = match unsafe { text(name, "name") } {
            Ok(n) => n,
            Err(code) => return code,
        };
        let ns = match namespace(shm_namespace) {
            Ok(ns) => ns,
            Err(code) => return code,
        };
        let cap = match checked_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let sddl = match unsafe { crate::ring::sddl_of(&options) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        let region = match shm_region(name, pubsub_ring_file_size(cap), ns, sddl) {
            Ok(r) => r,
            Err(code) => return code,
        };
        let locale = Locale::Shm { name, namespace: ns, create: true, sddl };
        place(PubSubRing::create_from_shm(region, cap), &locale, options, out)
    })
}

/// Attach to a pub/sub ring another process created in named shared
/// memory; the namespace must match the creator's.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_open_shm(
    name: *const c_char,
    expected_capacity: u32,
    shm_namespace: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let name = match unsafe { text(name, "name") } {
            Ok(n) => n,
            Err(code) => return code,
        };
        let ns = match namespace(shm_namespace) {
            Ok(ns) => ns,
            Err(code) => return code,
        };
        let cap = match checked_capacity(expected_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let sddl = match unsafe { crate::ring::sddl_of(&options) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        let region = match shm_region(name, pubsub_ring_file_size(cap), ns, sddl) {
            Ok(r) => r,
            Err(code) => return code,
        };
        let locale = Locale::Shm { name, namespace: ns, create: false, sddl };
        place(PubSubRing::open_from_shm(region, cap), &locale, options, out)
    })
}

/// Publish up to `SUBETHA_PUBSUB_PAYLOAD_BYTES` bytes, never waiting: the
/// oldest slot is overwritten whether or not a subscriber has read it. The
/// position assigned lands in `out_position` when it is not null. One
/// publisher at a time is the caller's contract.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_position` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_publish(handle: subetha_handle, data: *const u8, len: usize, out_position: *mut u64) -> i32 {
    with_pubsub(handle, |p| {
        if len > SUBETHA_PUBSUB_PAYLOAD_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_PUBSUB_PAYLOAD_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let position = p.publish(payload);
        if !out_position.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_position = position };
        }
        SUBETHA_OK
    })
}

/// Read the slot at an absolute `position` into `out`, which holds at
/// least `SUBETHA_PUBSUB_PAYLOAD_BYTES`. `SUBETHA_E_PUBSUB_PENDING` when it
/// is not published yet, `SUBETHA_E_PUBSUB_LOST` when it was overwritten.
///
/// # Safety
/// `out` points to `cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_read_at(handle: subetha_handle, position: u64, out: *mut u8, cap: usize) -> i32 {
    with_pubsub(handle, |p| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if cap < SUBETHA_PUBSUB_PAYLOAD_BYTES {
            return fail(crate::error::SUBETHA_E_BUFFER_TOO_SMALL, format!("{SUBETHA_PUBSUB_PAYLOAD_BYTES} bytes needed, {cap} given"));
        }
        // SAFETY: the caller guarantees `cap` writable bytes at `out`.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, cap) };
        match p.ring.read_at(position, buf) {
            Ok(()) => SUBETHA_OK,
            Err(PubSubReadError::Pending) => SUBETHA_E_PUBSUB_PENDING,
            Err(PubSubReadError::Lost) => fail(SUBETHA_E_PUBSUB_LOST, format!("position {position} was overwritten")),
        }
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_read_stats(handle: subetha_handle, out: *mut subetha_pubsub_stats) -> i32 {
    with_pubsub(handle, |p| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = p.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Wake every subscriber parked in a wait on this ring; each re-checks and
/// parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_pubsub_wake_all(handle: subetha_handle) -> i32 {
    with_pubsub(handle, |p| {
        p.waker.wake_all();
        SUBETHA_OK
    })
}

fn issue_subscriber(p: &PubSubObject, position: Position, mode: u32, out: *mut subetha_handle) -> i32 {
    if out.is_null() {
        return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
    }
    let object = SubscriberObject {
        ring: Arc::clone(&p.ring),
        waker: Arc::clone(&p.waker),
        position,
        mode,
        waiting: Waiting::new(),
    };
    unsafe { issue(Object::Subscriber(object), out) }
}

/// Subscribe to the ring `handle` names, starting at `position`, with the
/// position kept in the handle. `subetha_pubsub_read_stats` gives the
/// head, which is where a subscriber that wants only new items starts.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_subscribe(handle: subetha_handle, position: u64, mode: u32, out: *mut subetha_handle) -> i32 {
    with_pubsub(handle, |p| {
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        issue_subscriber(p, Position::Anon(AtomicU64::new(position)), mode, out)
    })
}

/// Subscribe with the position kept in the file `position_path`, which
/// is created at `initial` when `create` is true and opened as it is when
/// false, so a subscriber that restarts resumes where it stopped.
///
/// # Safety
/// `position_path` is a NUL-terminated UTF-8 string; `out` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_subscribe_file(
    handle: subetha_handle,
    position_path: *const c_char,
    initial: u64,
    create: bool,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    with_pubsub(handle, |p| {
        let path = match unsafe { text(position_path, "position_path") } {
            Ok(s) => Path::new(s),
            Err(code) => return code,
        };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let position = if create {
            SubscriberPosition::create(path, initial)
        } else {
            SubscriberPosition::open(path)
        };
        match position {
            Ok(position) => issue_subscriber(p, Position::File(position), mode, out),
            Err(e) => io_code(e),
        }
    })
}

/// Read the item at the subscriber's position into `out`, at least
/// `SUBETHA_PUBSUB_PAYLOAD_BYTES`, and advance past it.
/// `SUBETHA_E_PUBSUB_PENDING` when nothing newer is published;
/// `SUBETHA_E_PUBSUB_LOST` when the position was overwritten, after which
/// the subscriber stands at the ring's head.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_subscriber_try_next(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_subscriber(handle, |s| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_PUBSUB_PAYLOAD_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match s.try_next(buf) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = SUBETHA_PUBSUB_PAYLOAD_BYTES };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// `subetha_subscriber_try_next`, parking while nothing newer is
/// published, until a publish or `timeout_ms` elapses; a lost position
/// returns at once. The waiting contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_subscriber_next_wait(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_subscriber(handle, |s| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_PUBSUB_PAYLOAD_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match s.next_wait(buf, deadline) {
            Ok(()) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = SUBETHA_PUBSUB_PAYLOAD_BYTES };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// The subscriber's position, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_subscriber_position(handle: subetha_handle, out: *mut u64) -> i32 {
    with_subscriber(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let position = s.position.get();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = position };
        SUBETHA_OK
    })
}

/// Move the subscriber's position forward by `by` without reading; the
/// new position lands in `out` when it is not null.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_subscriber_skip(handle: subetha_handle, by: u64, out: *mut u64) -> i32 {
    with_subscriber(handle, |s| {
        let position = s.position.advance(by);
        if !out.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out = position };
        }
        SUBETHA_OK
    })
}

/// Set the subscriber's position outright.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_subscriber_set_position(handle: subetha_handle, position: u64) -> i32 {
    with_subscriber(handle, |s| {
        s.position.set(position);
        SUBETHA_OK
    })
}

/// The mode a subscriber was created in, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_subscriber_mode(handle: subetha_handle, out: *mut u32) -> i32 {
    with_subscriber(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = s.mode };
        SUBETHA_OK
    })
}

/// Remove the ring file at `path` and its waker file. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        found.remove(PathBuf::from(path));
        found.remove(with_suffix(path, ".cwaker.bin"));
        unsafe { finish_unlink(found, report) }
    })
}

/// Remove a subscriber's position file. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `position_path` is a NUL-terminated UTF-8 string; `report` is null or
/// a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_pubsub_unlink_position(position_path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(position_path, "position_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        found.remove(PathBuf::from(path));
        unsafe { finish_unlink(found, report) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;
    use std::time::Duration;
    use subetha_cxc::protocol_pubsub::PUBSUB_PAYLOAD_BYTES;

    #[test]
    fn the_exported_size_is_the_rings_own() {
        assert_eq!(SUBETHA_PUBSUB_PAYLOAD_BYTES, PUBSUB_PAYLOAD_BYTES);
    }

    #[test]
    fn a_subscriber_reads_in_order_waits_for_the_next_publish_and_is_told_what_it_lost() {
        let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
        let ring = Arc::new(PubSubObject::build(PubSubRing::create_anon(4).unwrap(), &Locale::Anon, options).unwrap());
        let subscriber = Arc::new(SubscriberObject {
            ring: Arc::clone(&ring.ring),
            waker: Arc::clone(&ring.waker),
            position: Position::Anon(AtomicU64::new(0)),
            mode: SUBETHA_MODE_STRICT,
            waiting: Waiting::new(),
        });
        let mut out = [0u8; SUBETHA_PUBSUB_PAYLOAD_BYTES];
        assert_eq!(subscriber.try_next(&mut out).unwrap_err(), SUBETHA_E_PUBSUB_PENDING);
        assert_eq!(ring.publish(b"one"), 0);
        assert_eq!(ring.publish(b"two"), 1);
        subscriber.try_next(&mut out).unwrap();
        assert_eq!(&out[..3], b"one");
        subscriber.try_next(&mut out).unwrap();
        assert_eq!(&out[..3], b"two");
        assert_eq!(subscriber.position.get(), 2);

        let waiter = {
            let subscriber = Arc::clone(&subscriber);
            std::thread::spawn(move || {
                let mut out = [0u8; SUBETHA_PUBSUB_PAYLOAD_BYTES];
                subscriber.next_wait(&mut out, None).map(|()| out[..5].to_vec())
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        ring.publish(b"three");
        assert_eq!(waiter.join().unwrap().unwrap(), b"three");

        // Four more publishes overwrite position 3 before it is read.
        for i in 0..5u8 {
            ring.publish(&[i]);
        }
        assert_eq!(subscriber.try_next(&mut out).unwrap_err(), SUBETHA_E_PUBSUB_LOST);
        assert_eq!(subscriber.position.get(), ring.ring.head(), "a lost subscriber stands at the head");
        let soon = Some(Instant::now() + Duration::from_millis(20));
        assert_eq!(subscriber.next_wait(&mut out, soon).unwrap_err(), crate::error::SUBETHA_E_TIMEOUT);
    }
}
