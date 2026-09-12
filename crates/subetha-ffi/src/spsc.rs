//! The blocking single-producer single-consumer ring through the C ABI:
//! one handle names the ring and its two wakers. One thread pushes and one
//! pops, each with a try and a waiting form. Strict and managed modes are
//! the same here: the ring runs no background work.
//!
//! In the file locale the ring the Rust API creates under a base path is
//! the same ring: `<base>.ring.bin`, `<base>.cw.bin` for the consumer's
//! waker and `<base>.pw.bin` for the producer's.

use std::ffi::c_char;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::blocking_spsc_ring::BlockingSpscRing;

use crate::error::{
    blocking_code, fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY,
    SUBETHA_E_RING_FULL, SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_SPSC};
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, out_buffer, subetha_unlink_report, text,
    with_suffix, SUBETHA_RING_SLOT_BYTES,
};
use crate::batch::{run_pop_many, run_push_many};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting};

/// A snapshot of a blocking SPSC ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_spsc_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots in the ring.
    pub capacity: u64,
    /// Items pushed so far.
    pub head: u64,
    /// Items popped so far.
    pub tail: u64,
    /// Items in the ring, read without a lock.
    pub approx_len: u64,
    /// Items the predictive wait caught without a wake, since creation.
    pub predictive_catches: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
    /// Whether predictive waiting is enabled on the consumer.
    pub phase_locking: bool,
}

pub(crate) struct SpscObject {
    /// Shared rather than owned outright: a blocking bridge half built on
    /// this ring holds the same ring through its own `Arc`, and the two
    /// objects are destroyed in whichever order the caller chooses.
    ring: Arc<BlockingSpscRing>,
    mode: u32,
    waiting: Waiting,
    /// The pollable notifiers attached to this ring by any process; every
    /// push signals them.
    notifiers: subetha_cxc::cross_process_notifier::NotifierSet,
}

impl SpscObject {
    fn new(ring: BlockingSpscRing, mode: u32, notifiers: subetha_cxc::cross_process_notifier::NotifierSet) -> Result<Self, i32> {
        Ok(Self {
            ring: Arc::new(ring),
            mode: resolve_mode(mode)?,
            waiting: Waiting::new(),
            notifiers,
        })
    }

    /// The ring itself, for a bridge half that carries it across a socket
    /// and needs to outlive this borrow of the handle table. The bridge
    /// is behind the transports' feature, so this has no caller without
    /// it and stays for the shape to be the same in every build.
    #[cfg_attr(not(feature = "tcp-bridge"), allow(dead_code))]
    pub(crate) fn ring(&self) -> Arc<BlockingSpscRing> {
        Arc::clone(&self.ring)
    }

    /// Wake every parked waiter so a destroy can proceed; each returns
    /// `SUBETHA_E_DESTROYED`.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.ring.consumer_waker().wake_all();
        self.ring.producer_waker().wake_all();
    }

    fn try_push(&self, payload: &[u8]) -> Result<(), i32> {
        self.ring.try_push(payload).map_err(ring_code)?;
        self.notifiers.signal();
        Ok(())
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, i32> {
        self.ring.try_pop(out).map_err(ring_code)
    }

    fn push_wait(&self, payload: &[u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, self.ring.producer_waker(), deadline, SUBETHA_E_RING_FULL, || {
            self.try_push(payload)
        })
    }

    fn pop_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        wait_until(&self.waiting, self.ring.consumer_waker(), deadline, SUBETHA_E_RING_EMPTY, || {
            self.try_pop(out)
        })
    }

    fn stats(&self) -> subetha_spsc_stats {
        let inner = self.ring.inner();
        subetha_spsc_stats {
            mode: self.mode,
            capacity: inner.capacity() as u64,
            head: inner.head(),
            tail: inner.tail(),
            approx_len: inner.approx_len() as u64,
            predictive_catches: self.ring.phase_predictive_catches(),
            waker_full: self.waiting.waker_full(),
            phase_locking: self.ring.phase_locking_enabled(),
        }
    }
}

pub(crate) fn with_spsc(handle: subetha_handle, f: impl FnOnce(&SpscObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_SPSC, |object| match object {
        Object::Spsc(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a blocking SPSC ring"),
    })
}

/// The ring a constructor built, or the code that refused it; `base` is
/// the file base the notifier record lives beside, none for an anonymous
/// ring.
fn build(
    ring: Result<BlockingSpscRing, subetha_cxc::blocking_spsc_ring::BlockingError>,
    mode: u32,
    base: Option<&Path>,
    out: *mut subetha_handle,
) -> i32 {
    let ring = match ring {
        Ok(r) => r,
        Err(e) => return blocking_code(e),
    };
    let notifiers = match base {
        None => subetha_cxc::cross_process_notifier::NotifierSet::anon(),
        Some(base) => match subetha_cxc::cross_process_notifier::NotifierSet::file(base) {
            Ok(set) => set,
            Err(e) => return crate::error::notify_code(e),
        },
    };
    match SpscObject::new(ring, mode, notifiers) {
        Ok(object) => unsafe { issue(Object::Spsc(object), out) },
        Err(code) => code,
    }
}

/// Create a blocking SPSC ring in anonymous memory, reachable from this
/// process only. `capacity` is a power of two of at least 2; `mode` is one
/// of the `SUBETHA_MODE_` constants.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_create_anon(capacity: u32, mode: u32, out: *mut subetha_handle) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let cap = match checked_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        build(BlockingSpscRing::create_anon(cap), mode, None, out)
    })
}

/// Create a file-backed blocking SPSC ring under `base_path`: the ring at
/// `<base_path>.ring.bin` and its wakers at `.cw.bin` and `.pw.bin`.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_create(
    base_path: *const c_char,
    capacity: u32,
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
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let cap = match checked_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        build(BlockingSpscRing::create(base, cap), mode, Some(base), out)
    })
}

/// Attach to a file-backed blocking SPSC ring another process created
/// under `base_path`. `SUBETHA_E_RING_LAYOUT_MISMATCH` when the ring is
/// absent or was created with another capacity.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_open(
    base_path: *const c_char,
    expected_capacity: u32,
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
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let cap = match checked_capacity(expected_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        build(BlockingSpscRing::open(base, cap), mode, Some(base), out)
    })
}

/// Push `len` bytes without waiting; the slot semantics are the adaptive
/// ring's. `SUBETHA_E_RING_FULL` when there is no room.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_spsc(handle, |s| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match s.try_push(payload) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Pop one slot into `out` without waiting; `cap` must be at least
/// `SUBETHA_RING_SLOT_BYTES` and `out_len` receives the slot's size.
/// `SUBETHA_E_RING_EMPTY` when there is nothing to take.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_try_pop(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_spsc(handle, |s| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match s.try_pop(buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Push `count` payloads of `len` bytes each, the first at `items` and
/// each next one `stride` bytes on, under one handle lookup and one panic
/// guard. The batch contract is `subetha_ring_try_push_many`'s.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_done` is a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_try_push_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_spsc(handle, |s| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_push_many(items, stride, len, count, out_done, |payload| match s.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Pop up to `count` slots, the first written at `out` and each next one
/// `stride` bytes on; `stride` is at least `SUBETHA_RING_SLOT_BYTES`. The
/// batch contract is `subetha_ring_try_pop_many`'s.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_try_pop_many(
    handle: subetha_handle,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_spsc(handle, |s| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, SUBETHA_RING_SLOT_BYTES, count, out_done, |buf| match s.try_pop(buf) {
                Ok(_) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Push, parking until there is room or `timeout_ms` elapses; the waiting
/// contract is `subetha_ring_push_wait`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_push_wait(handle: subetha_handle, data: *const u8, len: usize, timeout_ms: i64) -> i32 {
    with_spsc(handle, |s| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match s.push_wait(payload, deadline) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Pop, parking until an item arrives or `timeout_ms` elapses; the waiting
/// contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_pop_wait(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_spsc(handle, |s| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match s.pop_wait(buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Enable or disable predictive waiting on the consumer: when a producer
/// arrives on a regular cadence, the consumer parks until just before the
/// predicted arrival and spins a short guard band instead of paying the
/// wake. Off by default; it wins only for an in-process consumer whose
/// producer contends for cores, and loses across a process boundary.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_spsc_set_phase_locking(handle: subetha_handle, enabled: bool) -> i32 {
    with_spsc(handle, |s| {
        s.ring.set_phase_locking(enabled);
        SUBETHA_OK
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_read_stats(handle: subetha_handle, out: *mut subetha_spsc_stats) -> i32 {
    with_spsc(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = s.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Wake every thread parked in a wait on this ring; each re-checks the
/// ring and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_spsc_wake_all(handle: subetha_handle) -> i32 {
    with_spsc(handle, |s| {
        s.ring.consumer_waker().wake_all();
        s.ring.producer_waker().wake_all();
        SUBETHA_OK
    })
}

/// Remove the three files a file-backed blocking SPSC ring under
/// `base_path` names. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `report` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_unlink(base_path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        for suffix in [".ring.bin", ".cw.bin", ".pw.bin"] {
            found.remove(with_suffix(base, suffix));
        }
        for path in subetha_cxc::cross_process_notifier::NotifyRecordHandle::file_paths_under(base) {
            found.remove(path);
        }
        unsafe { finish_unlink(found, report) }
    })
}

/// Attach a pollable notifier to the SPSC ring `ring` names, for this
/// process to watch: every push through the ABI in any process on the
/// ring signals it. The contract is `subetha_ring_notifier`'s; beside a
/// file-backed ring the record is `<base>.notify.bin` and the Unix
/// notifiers `<base>.notify.<index>`, removed by `subetha_spsc_unlink`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_spsc_notifier(ring: subetha_handle, out: *mut subetha_handle) -> i32 {
    with_spsc(ring, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match s.notifiers.attach() {
            Ok(notifier) => unsafe { issue(Object::Notifier(crate::notifier::NotifierObject::new(notifier)), out) },
            Err(e) => crate::error::notify_code(e),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{SUBETHA_E_DESTROYED, SUBETHA_E_TIMEOUT};
    use crate::runtime::SUBETHA_MODE_STRICT;
    use std::sync::Arc;
    use std::time::Duration;

    fn anon() -> SpscObject {
        let ring = BlockingSpscRing::create_anon(8).expect("an anonymous blocking ring");
        SpscObject::new(ring, SUBETHA_MODE_STRICT, subetha_cxc::cross_process_notifier::NotifierSet::anon()).expect("an object")
    }

    #[test]
    fn a_push_wakes_a_parked_pop_and_a_full_ring_parks_the_push() {
        let object = Arc::new(anon());
        let mut buf = [0u8; SUBETHA_RING_SLOT_BYTES];
        let soon = Some(Instant::now() + Duration::from_millis(20));
        assert_eq!(object.pop_wait(&mut buf, soon).unwrap_err(), SUBETHA_E_TIMEOUT);

        let consumer = {
            let object = Arc::clone(&object);
            std::thread::spawn(move || {
                let mut buf = [0u8; SUBETHA_RING_SLOT_BYTES];
                let n = object.pop_wait(&mut buf, None).unwrap();
                buf[..n].to_vec()
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        object.try_push(b"hello").unwrap();
        let got = consumer.join().unwrap();
        assert_eq!(&got[..5], b"hello");

        for i in 0..8u8 {
            object.try_push(&[i]).unwrap();
        }
        assert_eq!(object.try_push(b"x").unwrap_err(), SUBETHA_E_RING_FULL);
        let producer = {
            let object = Arc::clone(&object);
            std::thread::spawn(move || object.push_wait(b"ninth", None))
        };
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(object.try_pop(&mut buf).unwrap(), SUBETHA_RING_SLOT_BYTES);
        assert_eq!(producer.join().unwrap(), Ok(()));
        assert_eq!(object.stats().approx_len, 8);
    }

    #[test]
    fn a_destroy_interrupt_releases_a_waiter() {
        let object = Arc::new(anon());
        let waiter = {
            let object = Arc::clone(&object);
            std::thread::spawn(move || {
                let mut buf = [0u8; SUBETHA_RING_SLOT_BYTES];
                object.pop_wait(&mut buf, None)
            })
        };
        std::thread::sleep(Duration::from_millis(20));
        object.interrupt();
        assert_eq!(waiter.join().unwrap().unwrap_err(), SUBETHA_E_DESTROYED);
    }
}
