//! The Vyukov MPMC ring through the C ABI: one bounded queue any number of
//! producers and consumers share, each slot consumed exactly once, with a
//! try and a waiting form of push and pop, the stuck-slot scan and heal a
//! watchdog runs after a producer died, and a flush for the file locale.
//! The ABI keeps a consumer waker and a producer waker beside the ring so
//! the waiting forms park across processes; strict and managed modes are
//! the same here, the ring runs no background work.
//!
//! In the file locale `path` names the ring file itself, as in the Rust
//! API, and the wakers are `<path>.cwaker.bin` and `<path>.pwaker.bin`. In
//! shared memory the ring is the region `{name}` and the wakers
//! `{name}_cwaker` and `{name}_pwaker`.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::time::Instant;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::shared_ring::{ring_file_size, SharedRing};

use crate::error::{
    fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY, SUBETHA_E_RING_FULL,
    SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::batch::{run_pop_many, run_push_many};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_VYUKOV};
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, namespace, out_buffer, read_options, shm_region,
    subetha_ring_options, subetha_unlink_report, text, wakers_for, with_suffix, Locale,
    SUBETHA_RING_PAYLOAD_MAX,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting, WAKE_ANY};

/// A snapshot of a Vyukov ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_vyukov_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots in the ring.
    pub capacity: u64,
    /// Slots claimed by producers so far.
    pub producer_seq: u64,
    /// Slots drained by consumers so far.
    pub consumer_seq: u64,
    /// Items waiting, read without a lock.
    pub approx_len: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
}

pub(crate) struct VyukovObject {
    ring: SharedRing,
    consumer_waker: CrossProcessWaker,
    producer_waker: CrossProcessWaker,
    mode: u32,
    waiting: Waiting,
}

impl VyukovObject {
    fn build(ring: SharedRing, locale: &Locale<'_>, options: subetha_ring_options) -> Result<Self, i32> {
        let mode = resolve_mode(options.mode)?;
        let (consumer_waker, producer_waker) = wakers_for(locale, options.max_waiters)?;
        Ok(Self {
            ring,
            consumer_waker,
            producer_waker,
            mode,
            waiting: Waiting::new(),
        })
    }

    /// Wake every parked waiter so a destroy can proceed; each returns
    /// `SUBETHA_E_DESTROYED`.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.consumer_waker.wake_all();
        self.producer_waker.wake_all();
    }

    fn try_push(&self, payload: &[u8]) -> Result<(), i32> {
        self.ring.try_push(payload).map_err(ring_code)?;
        self.consumer_waker.wake_one_up_to(WAKE_ANY);
        Ok(())
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, i32> {
        let n = self.ring.try_pop(out).map_err(ring_code)?;
        self.producer_waker.wake_one_up_to(WAKE_ANY);
        Ok(n)
    }

    fn push_wait(&self, payload: &[u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, &self.producer_waker, deadline, SUBETHA_E_RING_FULL, || self.try_push(payload))
    }

    fn pop_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        wait_until(&self.waiting, &self.consumer_waker, deadline, SUBETHA_E_RING_EMPTY, || self.try_pop(out))
    }

    fn stats(&self) -> subetha_vyukov_stats {
        subetha_vyukov_stats {
            mode: self.mode,
            capacity: self.ring.capacity() as u64,
            producer_seq: self.ring.producer_seq(),
            consumer_seq: self.ring.consumer_seq(),
            approx_len: self.ring.approx_len() as u64,
            waker_full: self.waiting.waker_full(),
        }
    }
}

fn with_vyukov(handle: subetha_handle, f: impl FnOnce(&VyukovObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_VYUKOV, |object| match object {
        Object::Vyukov(v) => f(v),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a Vyukov ring"),
    })
}

fn place(ring: Result<SharedRing, subetha_cxc::shared_ring::RingError>, locale: &Locale<'_>, options: subetha_ring_options, out: *mut subetha_handle) -> i32 {
    let ring = match ring {
        Ok(r) => r,
        Err(e) => return ring_code(e),
    };
    match VyukovObject::build(ring, locale, options) {
        Ok(object) => unsafe { issue(Object::Vyukov(object), out) },
        Err(code) => code,
    }
}

/// Create a Vyukov ring in anonymous memory, reachable from this process
/// only. `capacity` is a power of two of at least 2; `scan_interval_us`
/// in the options is ignored, the ring has no sidecar.
///
/// # Safety
/// `options` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_create_anon(capacity: u32, options: *const subetha_ring_options, out: *mut subetha_handle) -> i32 {
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
        place(SharedRing::create_anon(cap), &Locale::Anon, options, out)
    })
}

/// Create a file-backed Vyukov ring at `path`, truncating any file there,
/// with its wakers at `<path>.cwaker.bin` and `<path>.pwaker.bin`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_create(
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
        place(SharedRing::create(path, cap), &Locale::File(path), options, out)
    })
}

/// Attach to a file-backed Vyukov ring another process created at `path`.
/// `SUBETHA_E_RING_LAYOUT_MISMATCH` when the file is absent or was created
/// with another capacity.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_open(
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
        place(SharedRing::open(path, cap), &Locale::File(path), options, out)
    })
}

/// Create a Vyukov ring in the named shared-memory region `name`, in the
/// namespace `SUBETHA_SHM_SESSION` or `SUBETHA_SHM_MACHINE`, with its
/// wakers in `{name}_cwaker` and `{name}_pwaker`.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_create_shm(
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
        let region = match shm_region(name, ring_file_size(cap), ns, sddl) {
            Ok(r) => r,
            Err(code) => return code,
        };
        let locale = Locale::Shm { name, namespace: ns, create: true, sddl };
        place(SharedRing::create_from_shm(region, cap), &locale, options, out)
    })
}

/// Attach to a Vyukov ring another process created in named shared
/// memory; the namespace must match the creator's.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_open_shm(
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
        let region = match shm_region(name, ring_file_size(cap), ns, sddl) {
            Ok(r) => r,
            Err(code) => return code,
        };
        let locale = Locale::Shm { name, namespace: ns, create: false, sddl };
        place(SharedRing::open_from_shm(region, cap), &locale, options, out)
    })
}

/// Push up to `SUBETHA_RING_PAYLOAD_MAX` bytes without waiting; the slot
/// is zero past the payload. `SUBETHA_E_RING_FULL` when there is no room.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_vyukov(handle, |v| {
        if len > SUBETHA_RING_PAYLOAD_MAX {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_PAYLOAD_MAX}-byte payload"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match v.try_push(payload) {
            Ok(()) => SUBETHA_OK,
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
pub unsafe extern "C" fn subetha_vyukov_try_push_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_vyukov(handle, |v| {
        if len > SUBETHA_RING_PAYLOAD_MAX {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_PAYLOAD_MAX}-byte payload"));
        }
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_push_many(items, stride, len, count, out_done, |payload| match v.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Pop up to `count` payloads, the first written at `out` and each next
/// one `stride` bytes on; `stride` is at least
/// `SUBETHA_RING_PAYLOAD_MAX`. The batch contract is
/// `subetha_ring_try_pop_many`'s.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_try_pop_many(
    handle: subetha_handle,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_vyukov(handle, |v| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, SUBETHA_RING_PAYLOAD_MAX, count, out_done, |buf| match v.try_pop(buf) {
                Ok(_) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Pop one slot's payload, `SUBETHA_RING_PAYLOAD_MAX` bytes, into `out`
/// without waiting; `cap` must be at least that.
/// `SUBETHA_E_RING_EMPTY` when there is nothing to take.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_try_pop(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_vyukov(handle, |v| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_PAYLOAD_MAX) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match v.try_pop(buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Push, parking until there is room or `timeout_ms` elapses; the waiting
/// contract is `subetha_ring_push_wait`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_push_wait(handle: subetha_handle, data: *const u8, len: usize, timeout_ms: i64) -> i32 {
    with_vyukov(handle, |v| {
        if len > SUBETHA_RING_PAYLOAD_MAX {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_PAYLOAD_MAX}-byte payload"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match v.push_wait(payload, deadline) {
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
pub unsafe extern "C" fn subetha_vyukov_pop_wait(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_vyukov(handle, |v| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_PAYLOAD_MAX) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match v.pop_wait(buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_read_stats(handle: subetha_handle, out: *mut subetha_vyukov_stats) -> i32 {
    with_vyukov(handle, |v| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = v.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the ring's dirty pages to its file. Nothing to do in the
/// anonymous and shared-memory locales.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_vyukov_flush(handle: subetha_handle) -> i32 {
    with_vyukov(handle, |v| match v.ring.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => ring_code(e),
    })
}

/// Find the first slot at or after `from`, within the claimed but
/// undrained window, that a producer claimed and never published, which
/// is what a producer that died mid-push leaves behind. `out_found` says
/// whether there is one and `out_pos` names it. Never called by the data
/// path; a watchdog calls it once it has established that a producer is
/// dead.
///
/// # Safety
/// `out_found` and `out_pos` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_next_stuck_slot(handle: subetha_handle, from: u64, out_found: *mut bool, out_pos: *mut u64) -> i32 {
    with_vyukov(handle, |v| {
        if out_found.is_null() || out_pos.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_found or out_pos is null");
        }
        let found = v.ring.next_stuck_slot(from);
        // SAFETY: checked non-null; the caller guarantees they are writable.
        unsafe {
            *out_found = found.is_some();
            *out_pos = found.unwrap_or(0);
        }
        SUBETHA_OK
    })
}

/// Publish the slot at `pos` on behalf of a producer that died after
/// claiming it, so the next consumer drains it; its bytes are whatever
/// that producer wrote. `out_healed` is false when the slot was not stuck.
/// The caller must have established that the producer is dead: a live
/// one racing this call publishes the same value, and the consumer then
/// drains a slot that producer never finished writing.
///
/// # Safety
/// `out_healed` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_heal_stuck_slot(handle: subetha_handle, pos: u64, out_healed: *mut bool) -> i32 {
    with_vyukov(handle, |v| {
        if out_healed.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_healed is null");
        }
        match v.ring.heal_stuck_slot(pos) {
            Ok(healed) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_healed = healed };
                SUBETHA_OK
            }
            Err(e) => ring_code(e),
        }
    })
}

/// Wake every thread parked in a wait on this ring; each re-checks the
/// ring and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_vyukov_wake_all(handle: subetha_handle) -> i32 {
    with_vyukov(handle, |v| {
        v.consumer_waker.wake_all();
        v.producer_waker.wake_all();
        SUBETHA_OK
    })
}

/// Remove the ring file at `path` and its two waker files. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
        for suffix in [".cwaker.bin", ".pwaker.bin"] {
            found.remove(with_suffix(path, suffix));
        }
        unsafe { finish_unlink(found, report) }
    })
}

/// A shared-memory ring's regions are released with the last handle that
/// maps them, as for `subetha_ring_unlink_shm`; this reports zero removed
/// and succeeds.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_vyukov_unlink_shm(name: *const c_char, shm_namespace: u32, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if let Err(code) = unsafe { text(name, "name") } {
            return code;
        }
        if let Err(code) = namespace(shm_namespace) {
            return code;
        }
        if !report.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *report = subetha_unlink_report::default() };
        }
        crate::error::set_detail("shared-memory names are released with the last handle; nothing to remove by name");
        SUBETHA_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;
    use std::sync::Arc;
    use std::time::Duration;

    fn anon(capacity: usize) -> VyukovObject {
        let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
        VyukovObject::build(SharedRing::create_anon(capacity).unwrap(), &Locale::Anon, options).unwrap()
    }

    #[test]
    fn pushes_from_two_threads_all_arrive_and_a_pop_parks_until_one_does() {
        let object = Arc::new(anon(64));
        let mut out = [0u8; SUBETHA_RING_PAYLOAD_MAX];
        let soon = Some(Instant::now() + Duration::from_millis(20));
        assert_eq!(object.pop_wait(&mut out, soon).unwrap_err(), crate::error::SUBETHA_E_TIMEOUT);
        let producers: Vec<_> = (0..2u8)
            .map(|p| {
                let object = Arc::clone(&object);
                std::thread::spawn(move || {
                    for i in 0..100u8 {
                        object.push_wait(&[p, i], None).unwrap();
                    }
                })
            })
            .collect();
        let mut seen = [0u32; 2];
        for _ in 0..200 {
            let n = object.pop_wait(&mut out, None).unwrap();
            assert_eq!(n, SUBETHA_RING_PAYLOAD_MAX);
            seen[out[0] as usize] += 1;
        }
        for p in producers {
            p.join().unwrap();
        }
        assert_eq!(seen, [100, 100]);
        assert_eq!(object.stats().approx_len, 0);
        assert_eq!(object.stats().producer_seq, 200);
    }

    #[test]
    fn a_stuck_slot_scan_finds_nothing_on_a_healthy_ring() {
        let object = anon(8);
        object.try_push(b"a").unwrap();
        assert_eq!(object.ring.next_stuck_slot(0), None);
        assert!(!object.ring.heal_stuck_slot(0).unwrap(), "a published slot is not stuck");
    }
}
