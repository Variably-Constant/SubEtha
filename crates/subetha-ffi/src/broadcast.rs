//! The broadcast ring through the C ABI: one producer, up to sixteen
//! registered consumers that each read the whole stream, a push that is
//! refused while any registered consumer has not read the slot it would
//! overwrite, and a try and a waiting form on both sides. The ABI keeps a
//! consumer waker and a producer waker beside the ring; a push wakes every
//! parked consumer, a receive wakes a parked producer. Strict and managed
//! modes are the same here.
//!
//! In the file locale `path` names the ring file itself, as in the Rust
//! API, with the wakers at `<path>.cwaker.bin` and `<path>.pwaker.bin`; in
//! shared memory the ring is the region `{name}` and the wakers
//! `{name}_cwaker` and `{name}_pwaker`.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::shared_broadcast_ring::{broadcast_file_size, BroadcastError, SharedBroadcastRing};

use crate::error::{
    broadcast_code, fail, SUBETHA_E_BROADCAST_INVALID_CONSUMER, SUBETHA_E_INVALID_ARGUMENT,
    SUBETHA_E_RING_EMPTY, SUBETHA_E_RING_FULL, SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::batch::{run_pop_many, run_push_many};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_BROADCAST};
use crate::ring::{
    bytes, deadline_from, finish_unlink, namespace, out_buffer, read_options, shm_region, subetha_ring_options,
    subetha_unlink_report, text, wakers_for, with_suffix, Locale,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting, WAKE_ANY};

/// Bytes one broadcast slot carries; a push zero-fills past its payload
/// and a receive yields the whole slot.
pub const SUBETHA_BROADCAST_PAYLOAD_BYTES: usize = 52;
/// Consumers a broadcast ring can have registered at once.
pub const SUBETHA_BROADCAST_MAX_CONSUMERS: u32 = 16;

/// A snapshot of a broadcast ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_broadcast_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Consumers currently registered.
    pub active_consumers: u32,
    /// Slots in the ring.
    pub capacity: u64,
    /// Items pushed since creation.
    pub producer_position: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
    /// Whether every registered consumer has read everything pushed.
    pub fully_drained: bool,
}

pub(crate) struct BroadcastObject {
    ring: SharedBroadcastRing,
    consumer_waker: CrossProcessWaker,
    producer_waker: CrossProcessWaker,
    mode: u32,
    waiting: Waiting,
}

impl BroadcastObject {
    fn build(ring: SharedBroadcastRing, locale: &Locale<'_>, options: subetha_ring_options) -> Result<Self, i32> {
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
        self.ring.try_push(payload).map_err(broadcast_code)?;
        // Every registered consumer wants the item, so every parked one wakes.
        self.consumer_waker.wake_all();
        Ok(())
    }

    fn try_recv(&self, consumer: usize, out: &mut [u8]) -> Result<usize, i32> {
        let n = self.ring.try_recv(consumer, out).map_err(broadcast_code)?;
        self.producer_waker.wake_one_up_to(WAKE_ANY);
        Ok(n)
    }

    fn push_wait(&self, payload: &[u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, &self.producer_waker, deadline, SUBETHA_E_RING_FULL, || self.try_push(payload))
    }

    fn recv_wait(&self, consumer: usize, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        wait_until(&self.waiting, &self.consumer_waker, deadline, SUBETHA_E_RING_EMPTY, || {
            self.try_recv(consumer, out)
        })
    }

    fn stats(&self) -> subetha_broadcast_stats {
        subetha_broadcast_stats {
            mode: self.mode,
            active_consumers: self.ring.active_consumer_count() as u32,
            capacity: self.ring.capacity() as u64,
            producer_position: self.ring.producer_position(),
            waker_full: self.waiting.waker_full(),
            fully_drained: self.ring.is_fully_drained(),
        }
    }
}

fn with_broadcast(handle: subetha_handle, f: impl FnOnce(&BroadcastObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_BROADCAST, |object| match object {
        Object::Broadcast(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a broadcast ring"),
    })
}

/// A broadcast capacity: at least 2 slots.
fn broadcast_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity < 2 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("capacity {capacity} is below 2")));
    }
    Ok(capacity as usize)
}

fn place(ring: Result<SharedBroadcastRing, BroadcastError>, locale: &Locale<'_>, options: subetha_ring_options, out: *mut subetha_handle) -> i32 {
    let ring = match ring {
        Ok(r) => r,
        Err(e) => return broadcast_code(e),
    };
    match BroadcastObject::build(ring, locale, options) {
        Ok(object) => unsafe { issue(Object::Broadcast(object), out) },
        Err(code) => code,
    }
}

/// Create a broadcast ring in anonymous memory. `capacity` is at least 2;
/// `scan_interval_us` in the options is ignored, the ring has no sidecar.
///
/// # Safety
/// `options` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_create_anon(capacity: u32, options: *const subetha_ring_options, out: *mut subetha_handle) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let options = match unsafe { read_options(options, out) } {
            Ok(o) => o,
            Err(code) => return code,
        };
        let cap = match broadcast_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        place(SharedBroadcastRing::create_anon(cap), &Locale::Anon, options, out)
    })
}

/// Create a file-backed broadcast ring at `path`, or attach to one that
/// exists there with the same capacity, with the wakers at
/// `<path>.cwaker.bin` and `<path>.pwaker.bin`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_create(
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
        let cap = match broadcast_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        place(SharedBroadcastRing::create(path, cap), &Locale::File(path), options, out)
    })
}

/// Attach to a file-backed broadcast ring another process created at
/// `path`. `SUBETHA_E_RING_LAYOUT_MISMATCH` when the file is absent or was
/// created with another capacity.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_open(
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
        let cap = match broadcast_capacity(expected_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        place(SharedBroadcastRing::open(path, cap), &Locale::File(path), options, out)
    })
}

/// Create a broadcast ring in the named shared-memory region `name`, with
/// its wakers in `{name}_cwaker` and `{name}_pwaker`.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_create_shm(
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
        let cap = match broadcast_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let sddl = match unsafe { crate::ring::sddl_of(&options) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        let region = match shm_region(name, broadcast_file_size(cap), ns, sddl) {
            Ok(r) => r,
            Err(code) => return code,
        };
        let locale = Locale::Shm { name, namespace: ns, create: true, sddl };
        place(SharedBroadcastRing::create_from_shm(region, cap), &locale, options, out)
    })
}

/// Attach to a broadcast ring another process created in named shared
/// memory; the namespace must match the creator's.
///
/// # Safety
/// `name` is a NUL-terminated UTF-8 string; `options` and `out` are valid
/// pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_open_shm(
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
        let cap = match broadcast_capacity(expected_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let sddl = match unsafe { crate::ring::sddl_of(&options) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        let region = match shm_region(name, broadcast_file_size(cap), ns, sddl) {
            Ok(r) => r,
            Err(code) => return code,
        };
        let locale = Locale::Shm { name, namespace: ns, create: false, sddl };
        place(SharedBroadcastRing::open_from_shm(region, cap), &locale, options, out)
    })
}

/// Register as a consumer and receive the index every receive takes. The
/// consumer starts at the producer's current position, not at history.
/// `SUBETHA_E_BROADCAST_NO_CONSUMER_SLOT` when all
/// `SUBETHA_BROADCAST_MAX_CONSUMERS` are taken.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_register_consumer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_broadcast(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match b.ring.register_consumer() {
            Ok(index) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = index as u32 };
                SUBETHA_OK
            }
            Err(e) => broadcast_code(e),
        }
    })
}

/// Give a consumer index back; the producer no longer waits for it.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_broadcast_unregister_consumer(handle: subetha_handle, consumer: u32) -> i32 {
    with_broadcast(handle, |b| {
        if consumer >= SUBETHA_BROADCAST_MAX_CONSUMERS {
            return fail(SUBETHA_E_INVALID_ARGUMENT, format!("consumer {consumer} is out of range"));
        }
        b.ring.unregister_consumer(consumer as usize);
        b.producer_waker.wake_one_up_to(WAKE_ANY);
        SUBETHA_OK
    })
}

/// Push up to `SUBETHA_BROADCAST_PAYLOAD_BYTES` bytes without waiting.
/// `SUBETHA_E_RING_FULL` while a registered consumer has not read the slot
/// this push would overwrite.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_broadcast(handle, |b| {
        if len > SUBETHA_BROADCAST_PAYLOAD_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_BROADCAST_PAYLOAD_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match b.try_push(payload) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Push `count` payloads of `len` bytes each, the first at `items` and
/// each next one `stride` bytes on, under one handle lookup and one panic
/// guard. Every consumer sees every one of them, as with a single push.
/// The batch contract is `subetha_ring_try_push_many`'s.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_done` is a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_try_push_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_broadcast(handle, |b| {
        if len > SUBETHA_BROADCAST_PAYLOAD_BYTES {
            return fail(
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
                format!("{len} bytes exceed the {SUBETHA_BROADCAST_PAYLOAD_BYTES}-byte slot"),
            );
        }
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_push_many(items, stride, len, count, out_done, |payload| match b.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Receive up to `count` unread slots for `consumer`, the first written at
/// `out` and each next one `stride` bytes on; `stride` is at least
/// `SUBETHA_BROADCAST_PAYLOAD_BYTES`. The batch contract is
/// `subetha_ring_try_pop_many`'s.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_try_recv_many(
    handle: subetha_handle,
    consumer: u32,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_broadcast(handle, |b| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, SUBETHA_BROADCAST_PAYLOAD_BYTES, count, out_done, |buf| {
                match b.try_recv(consumer as usize, buf) {
                    Ok(_) => SUBETHA_OK,
                    Err(code) => code,
                }
            })
        }
    })
}

/// Receive the next unread slot for `consumer` into `out` without
/// waiting; `cap` must be at least `SUBETHA_BROADCAST_PAYLOAD_BYTES` and
/// `out_len` receives that size.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_try_recv(handle: subetha_handle, consumer: u32, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_broadcast(handle, |b| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_BROADCAST_PAYLOAD_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match b.try_recv(consumer as usize, buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Push, parking until the slowest consumer frees the slot or `timeout_ms`
/// elapses; the waiting contract is `subetha_ring_push_wait`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_push_wait(handle: subetha_handle, data: *const u8, len: usize, timeout_ms: i64) -> i32 {
    with_broadcast(handle, |b| {
        if len > SUBETHA_BROADCAST_PAYLOAD_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_BROADCAST_PAYLOAD_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match b.push_wait(payload, deadline) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Receive, parking until the producer pushes or `timeout_ms` elapses; the
/// waiting contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_recv_wait(
    handle: subetha_handle,
    consumer: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_broadcast(handle, |b| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_BROADCAST_PAYLOAD_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match b.recv_wait(consumer as usize, buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Items `consumer` has not read yet, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_lag(handle: subetha_handle, consumer: u32, out: *mut u64) -> i32 {
    with_broadcast(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if consumer >= SUBETHA_BROADCAST_MAX_CONSUMERS {
            return fail(SUBETHA_E_INVALID_ARGUMENT, format!("consumer {consumer} is out of range"));
        }
        // A slot nothing holds is refused rather than answered. Its
        // cursor stays where its last holder stopped, so the distance
        // from the producer to it is a number about nobody, and writing
        // that into `out` alongside SUBETHA_OK would present it as a
        // reading.
        let Some(lag) = b.ring.try_lag(consumer as usize) else {
            return fail(
                SUBETHA_E_BROADCAST_INVALID_CONSUMER,
                format!("consumer {consumer} holds no slot on this ring"),
            );
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = lag };
        SUBETHA_OK
    })
}

/// Wait until `want` consumers have registered, writing how many there
/// are when the wait ends into `out`.
///
/// A count below `want` is not an error. It is the shortfall, and it is
/// what the caller needs in order to say which of its readers never
/// arrived.
///
/// # Why a producer wants this
///
/// A consumer registers at the head, so everything published before it
/// registered is lost to it and nothing reports that: the push
/// succeeds, the ring does not error, and a reader that started late is
/// indistinguishable from one that is slow. Across processes the window
/// is however long starting one takes. Publishing only once the readers
/// are here is the only thing that closes it, because afterwards there
/// is nothing left to detect.
///
/// `timeout_ms` must be a real timeout. `SUBETHA_WAIT_FOREVER` is
/// refused rather than honored: a producer waiting without end for a
/// worker that will never start is a hang with nothing to diagnose it,
/// which is the failure this call exists to replace with a number.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_wait_for_consumers(
    handle: subetha_handle,
    want: u32,
    timeout_ms: i64,
    out: *mut u32,
) -> i32 {
    with_broadcast(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if want > SUBETHA_BROADCAST_MAX_CONSUMERS {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("want {want} is more consumers than a ring holds"),
            );
        }
        if timeout_ms < 0 {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("timeout_ms {timeout_ms} must be a real timeout, not a wait without end"),
            );
        }
        let have = b
            .ring
            .wait_for_consumers(want as usize, Duration::from_millis(timeout_ms as u64));
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = have as u32 };
        SUBETHA_OK
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_broadcast_read_stats(handle: subetha_handle, out: *mut subetha_broadcast_stats) -> i32 {
    with_broadcast(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = b.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the ring's dirty pages to its file. Nothing to do in the
/// anonymous and shared-memory locales.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_broadcast_flush(handle: subetha_handle) -> i32 {
    with_broadcast(handle, |b| match b.ring.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => broadcast_code(e),
    })
}

/// Wake every thread parked in a wait on this ring; each re-checks the
/// ring and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_broadcast_wake_all(handle: subetha_handle) -> i32 {
    with_broadcast(handle, |b| {
        b.consumer_waker.wake_all();
        b.producer_waker.wake_all();
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
pub unsafe extern "C" fn subetha_broadcast_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;
    use std::sync::Arc;
    use std::time::Duration;
    use subetha_cxc::shared_broadcast_ring::{BROADCAST_PAYLOAD_BYTES, MAX_CONSUMERS};

    #[test]
    fn the_exported_sizes_are_the_rings_own() {
        assert_eq!(SUBETHA_BROADCAST_PAYLOAD_BYTES, BROADCAST_PAYLOAD_BYTES);
        assert_eq!(SUBETHA_BROADCAST_MAX_CONSUMERS as usize, MAX_CONSUMERS);
    }

    #[test]
    fn every_consumer_sees_every_item_and_the_slowest_gates_the_producer() {
        let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
        let object = Arc::new(
            BroadcastObject::build(SharedBroadcastRing::create_anon(4).unwrap(), &Locale::Anon, options).unwrap(),
        );
        let a = object.ring.register_consumer().unwrap();
        let b = object.ring.register_consumer().unwrap();
        for i in 0..4u8 {
            object.try_push(&[i]).unwrap();
        }
        assert_eq!(object.try_push(b"x").unwrap_err(), SUBETHA_E_RING_FULL, "nobody has read slot 0");
        let mut out = [0u8; SUBETHA_BROADCAST_PAYLOAD_BYTES];
        for i in 0..4u8 {
            assert_eq!(object.try_recv(a, &mut out).unwrap(), SUBETHA_BROADCAST_PAYLOAD_BYTES);
            assert_eq!(out[0], i);
        }
        assert_eq!(object.try_push(b"x").unwrap_err(), SUBETHA_E_RING_FULL, "b has read nothing");
        let pusher = {
            let object = Arc::clone(&object);
            std::thread::spawn(move || object.push_wait(&[9], None))
        };
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(object.try_recv(b, &mut out).unwrap(), SUBETHA_BROADCAST_PAYLOAD_BYTES);
        assert_eq!(pusher.join().unwrap(), Ok(()));
        assert_eq!(object.ring.lag(b), 4);
        assert_eq!(object.stats().active_consumers, 2);
        assert!(!object.stats().fully_drained);
    }
}
