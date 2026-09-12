//! The capacity-adaptive broadcast ring through the C ABI: a broadcast
//! ring whose slot count changes at run time. A morph builds a fresh
//! backing, moves the producer to it, mirrors every consumer registration
//! onto it, and leaves the old backing on a stale list each consumer
//! drains first, so subscribers stay in lockstep across the morph.
//! Consumers register and never unregister. Strict and managed modes are
//! the same here: the ring runs no background work. The ABI keeps a
//! consumer waker and a producer waker beside the ring.
//!
//! In the file locale the backings are `<base>.cap_<N>_g<seq>.bin` and
//! the wakers `<base>.cwaker.bin` and `<base>.pwaker.bin`; in shared
//! memory `{prefix}_cap_<N>_g<seq>` and `{prefix}_cwaker`, `{prefix}_pwaker`,
//! in the session namespace.

use std::ffi::c_char;
use std::path::Path;
use std::time::Instant;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::capacity_broadcast_ring::CapacityBroadcastRing;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::shm_file::ShmNamespace;

use crate::broadcast::SUBETHA_BROADCAST_PAYLOAD_BYTES;
use crate::capacity::remove_backings;
use crate::error::{
    broadcast_capacity_code, broadcast_code, fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY,
    SUBETHA_E_RING_FULL, SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_CAPACITY_BROADCAST};
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, out_buffer, read_options, subetha_ring_options,
    subetha_unlink_report, text, wakers_for, with_suffix, Locale,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting, WAKE_ANY};

/// A snapshot of a capacity-adaptive broadcast ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_capacity_broadcast_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Consumers registered on the active backing.
    pub active_consumers: u32,
    /// Capacity of the backing the producer writes to.
    pub current_capacity: u64,
    /// Bumped by every morph.
    pub pin_generation: u64,
    /// Items pushed into the active backing since it was built.
    pub producer_position: u64,
    /// Capacity of the backing held in the warm cache; zero when none.
    pub warm_capacity: u64,
    /// Morphs that consumed a warm backing instead of building one.
    pub warm_hits: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
}

pub(crate) struct CapacityBroadcastObject {
    ring: CapacityBroadcastRing,
    consumer_waker: CrossProcessWaker,
    producer_waker: CrossProcessWaker,
    mode: u32,
    waiting: Waiting,
}

/// The options this ring accepts: a mode and waiter slots; stamps and a
/// contract are the adaptive ring's and refused here by name.
fn checked_options(options: &subetha_ring_options) -> Result<u32, i32> {
    let mode = resolve_mode(options.mode)?;
    if options.stamps != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a broadcast ring carries no ordering stamps"));
    }
    let c = &options.contract;
    if c.max_producers != 0 || c.max_consumers != 0 || c.ordering != 0 || c.capacity_bound != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a broadcast ring declares no contract"));
    }
    if options.frame_block != 0 || options.frame_blocks != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a broadcast ring takes no frame region geometry"));
    }
    if !options.shm_sddl.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a capacity broadcast ring's regions take the platform's default descriptor; shm_sddl must be null"));
    }
    Ok(mode)
}

impl CapacityBroadcastObject {
    fn build(ring: CapacityBroadcastRing, locale: &Locale<'_>, options: &subetha_ring_options, mode: u32) -> Result<Self, i32> {
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

    fn stats(&self) -> subetha_capacity_broadcast_stats {
        let active = self.ring.ring_handle();
        subetha_capacity_broadcast_stats {
            mode: self.mode,
            active_consumers: active.active_consumer_count() as u32,
            current_capacity: self.ring.current_capacity() as u64,
            pin_generation: self.ring.pin_generation(),
            producer_position: active.producer_position(),
            warm_capacity: self.ring.warm_capacity().map_or(0, |c| c as u64),
            warm_hits: self.ring.warm_hits(),
            waker_full: self.waiting.waker_full(),
        }
    }
}

fn with_ring(handle: subetha_handle, f: impl FnOnce(&CapacityBroadcastObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_CAPACITY_BROADCAST, |object| match object {
        Object::CapacityBroadcast(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a capacity-adaptive broadcast ring"),
    })
}

/// Create a capacity-adaptive broadcast ring in anonymous memory.
/// `initial_capacity` is a power of two of at least 2.
///
/// # Safety
/// `options` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_create_anon(
    initial_capacity: u32,
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
        let mode = match checked_options(&options) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let cap = match checked_capacity(initial_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = match CapacityBroadcastRing::create_anon(cap) {
            Ok(r) => r,
            Err(e) => return broadcast_capacity_code(e),
        };
        match CapacityBroadcastObject::build(ring, &Locale::Anon, &options, mode) {
            Ok(object) => unsafe { issue(Object::CapacityBroadcast(object), out) },
            Err(code) => code,
        }
    })
}

/// Create a file-backed capacity-adaptive broadcast ring under
/// `base_path`, or attach to one that exists there.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_create(
    base_path: *const c_char,
    initial_capacity: u32,
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
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mode = match checked_options(&options) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let cap = match checked_capacity(initial_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = match CapacityBroadcastRing::create(base, cap) {
            Ok(r) => r,
            Err(e) => return broadcast_capacity_code(e),
        };
        match CapacityBroadcastObject::build(ring, &Locale::File(base), &options, mode) {
            Ok(object) => unsafe { issue(Object::CapacityBroadcast(object), out) },
            Err(code) => code,
        }
    })
}

/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
unsafe fn shm_ring(
    name_prefix: *const c_char,
    initial_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
    create: bool,
) -> i32 {
    if let Err(code) = require_initialized() {
        return code;
    }
    let options = match unsafe { read_options(options, out) } {
        Ok(o) => o,
        Err(code) => return code,
    };
    let prefix = match unsafe { text(name_prefix, "name_prefix") } {
        Ok(p) => p,
        Err(code) => return code,
    };
    let mode = match checked_options(&options) {
        Ok(m) => m,
        Err(code) => return code,
    };
    let cap = match checked_capacity(initial_capacity) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let ring = match CapacityBroadcastRing::create_shmfs(prefix, cap) {
        Ok(r) => r,
        Err(e) => return broadcast_capacity_code(e),
    };
    let locale = Locale::Shm { name: prefix, namespace: ShmNamespace::Session, create, sddl: None };
    match CapacityBroadcastObject::build(ring, &locale, &options, mode) {
        Ok(object) => unsafe { issue(Object::CapacityBroadcast(object), out) },
        Err(code) => code,
    }
}

/// Create a capacity-adaptive broadcast ring in named shared memory under
/// `name_prefix`, in the session namespace, initializing its wakers.
///
/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_create_shm(
    name_prefix: *const c_char,
    initial_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| unsafe { shm_ring(name_prefix, initial_capacity, options, out, true) })
}

/// Attach to a capacity-adaptive broadcast ring another process created
/// in named shared memory under `name_prefix`; the wakers are attached,
/// not reset.
///
/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_open_shm(
    name_prefix: *const c_char,
    initial_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| unsafe { shm_ring(name_prefix, initial_capacity, options, out, false) })
}

/// Register as a consumer; every morph mirrors the registration onto the
/// next backing, and there is no unregistering. The contract is
/// `subetha_broadcast_register_consumer`'s.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_register_consumer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_ring(handle, |b| {
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

/// Push without waiting; the contract is `subetha_broadcast_try_push`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_ring(handle, |b| {
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

/// Receive without waiting, draining stale backings before the active
/// one; the contract is `subetha_broadcast_try_recv`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_try_recv(
    handle: subetha_handle,
    consumer: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_ring(handle, |b| {
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
pub unsafe extern "C" fn subetha_capacity_broadcast_push_wait(handle: subetha_handle, data: *const u8, len: usize, timeout_ms: i64) -> i32 {
    with_ring(handle, |b| {
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
pub unsafe extern "C" fn subetha_capacity_broadcast_recv_wait(
    handle: subetha_handle,
    consumer: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_ring(handle, |b| {
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

/// Morph the ring's capacity to `new_capacity`, a power of two of at
/// least 2; every registered consumer follows.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_broadcast_morph_capacity(handle: subetha_handle, new_capacity: u32) -> i32 {
    with_ring(handle, |b| match b.ring.morph_capacity_to(new_capacity as usize) {
        Ok(()) => SUBETHA_OK,
        Err(e) => broadcast_capacity_code(e),
    })
}

/// Build the backing a coming morph to `capacity` needs, off the data
/// path; the contract is `subetha_capacity_prewarm`'s.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_broadcast_prewarm(handle: subetha_handle, capacity: u32) -> i32 {
    with_ring(handle, |b| match b.ring.prewarm(capacity as usize) {
        Ok(()) => SUBETHA_OK,
        Err(e) => broadcast_capacity_code(e),
    })
}

/// Drop the warm backing.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_broadcast_clear_warm(handle: subetha_handle) -> i32 {
    with_ring(handle, |b| {
        b.ring.clear_warm();
        SUBETHA_OK
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_read_stats(handle: subetha_handle, out: *mut subetha_capacity_broadcast_stats) -> i32 {
    with_ring(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = b.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Wake every thread parked in a wait on this ring; each re-checks the
/// ring and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_broadcast_wake_all(handle: subetha_handle) -> i32 {
    with_ring(handle, |b| {
        b.consumer_waker.wake_all();
        b.producer_waker.wake_all();
        SUBETHA_OK
    })
}

/// Remove every backing file under `base_path` and the two waker files;
/// the contract is `subetha_capacity_unlink`'s.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `report` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_broadcast_unlink(base_path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        for suffix in [".cwaker.bin", ".pwaker.bin"] {
            found.remove(with_suffix(base, suffix));
        }
        match remove_backings(base, &mut found) {
            Ok(()) => unsafe { finish_unlink(found, report) },
            Err(code) => code,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;

    #[test]
    fn a_consumer_follows_a_morph_and_reads_the_stale_backing_first() {
        let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
        let ring = CapacityBroadcastRing::create_anon(4).unwrap();
        let object = CapacityBroadcastObject::build(ring, &Locale::Anon, &options, SUBETHA_MODE_STRICT).unwrap();
        let consumer = object.ring.register_consumer().unwrap();
        for i in 0..4u8 {
            object.try_push(&[i]).unwrap();
        }
        assert_eq!(object.try_push(&[9]).unwrap_err(), SUBETHA_E_RING_FULL);
        object.ring.morph_capacity_to(8).unwrap();
        for i in 4..8u8 {
            object.try_push(&[i]).unwrap();
        }
        let mut out = [0u8; SUBETHA_BROADCAST_PAYLOAD_BYTES];
        for i in 0..8u8 {
            assert_eq!(object.try_recv(consumer, &mut out).unwrap(), SUBETHA_BROADCAST_PAYLOAD_BYTES);
            assert_eq!(out[0], i);
        }
        assert_eq!(object.try_recv(consumer, &mut out).unwrap_err(), SUBETHA_E_RING_EMPTY);
        let stats = object.stats();
        assert_eq!(stats.current_capacity, 8);
        assert_eq!(stats.pin_generation, 1);
        assert_eq!(stats.active_consumers, 1);
    }
}
