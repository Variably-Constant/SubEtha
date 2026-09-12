//! The capacity-adaptive pub/sub ring through the C ABI: a pub/sub ring
//! whose slot count changes at run time under a chain of backings. The
//! publisher writes to the newest backing; each subscriber carries a
//! backing index and a position, drains one backing, and crosses into the
//! next when it catches up. A backing no subscriber still reads is
//! reclaimed by `gc`. Strict and managed modes are the same here: the ring
//! runs no background work. The ABI keeps one waker beside the ring so a
//! subscriber can wait for the next publish.
//!
//! In the file locale the backings are `<base>.cap_<N>_g<seq>.bin` and the
//! waker `<base>.cwaker.bin`; in shared memory `{prefix}_cap_<N>_g<seq>` and
//! `{prefix}_cwaker`, in the session namespace.

use std::ffi::c_char;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::capacity_pubsub_ring::{CapacityPubSubRing, CapacityPubSubSubscriber};
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::protocol_pubsub::PubSubReadError;
use subetha_cxc::shm_file::ShmNamespace;

use crate::capacity::remove_backings;
use crate::error::{
    fail, pubsub_capacity_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_PUBSUB_LOST, SUBETHA_E_PUBSUB_PENDING,
    SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_CAPACITY_PUBSUB, SUBETHA_KIND_CAPACITY_SUBSCRIBER};
use crate::pubsub::SUBETHA_PUBSUB_PAYLOAD_BYTES;
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, out_buffer, read_options, subetha_ring_options,
    subetha_unlink_report, text, waker_for, with_suffix, Locale,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting};

/// A snapshot of a capacity-adaptive pub/sub ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_capacity_pubsub_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Backings in the chain, the active one included.
    pub chain_len: u32,
    /// Capacity of the backing the publisher writes to.
    pub current_capacity: u64,
    /// Bumped by every morph.
    pub pin_generation: u64,
    /// Slots across every backing in the chain.
    pub chain_total_capacity: u64,
    /// Capacity of the backing held in the warm cache; zero when none.
    pub warm_capacity: u64,
    /// Morphs that consumed a warm backing instead of building one.
    pub warm_hits: u64,
}

pub(crate) struct CapacityPubSubObject {
    ring: Arc<CapacityPubSubRing>,
    waker: Arc<CrossProcessWaker>,
    mode: u32,
}

pub(crate) struct CapacitySubscriberObject {
    subscriber: Mutex<CapacityPubSubSubscriber>,
    waker: Arc<CrossProcessWaker>,
    mode: u32,
    waiting: Waiting,
}

fn checked_options(options: &subetha_ring_options) -> Result<u32, i32> {
    let mode = resolve_mode(options.mode)?;
    if options.stamps != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a pub/sub ring carries no ordering stamps"));
    }
    let c = &options.contract;
    if c.max_producers != 0 || c.max_consumers != 0 || c.ordering != 0 || c.capacity_bound != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a pub/sub ring declares no contract"));
    }
    if options.frame_block != 0 || options.frame_blocks != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a pub/sub ring takes no frame region geometry"));
    }
    if !options.shm_sddl.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a capacity pub/sub ring's regions take the platform's default descriptor; shm_sddl must be null"));
    }
    Ok(mode)
}

impl CapacityPubSubObject {
    fn build(ring: Arc<CapacityPubSubRing>, locale: &Locale<'_>, options: &subetha_ring_options, mode: u32) -> Result<Self, i32> {
        let waker = waker_for(locale, options.max_waiters, ".cwaker.bin", "_cwaker")?;
        Ok(Self {
            ring,
            waker: Arc::new(waker),
            mode,
        })
    }

    /// Nothing parks on the ring handle itself.
    pub(crate) fn interrupt(&self) {}

    fn publish(&self, payload: &[u8]) -> u64 {
        let position = self.ring.publish(payload);
        self.waker.wake_all();
        position
    }

    fn stats(&self) -> subetha_capacity_pubsub_stats {
        subetha_capacity_pubsub_stats {
            mode: self.mode,
            chain_len: self.ring.chain_len() as u32,
            current_capacity: self.ring.current_capacity() as u64,
            pin_generation: self.ring.pin_generation(),
            chain_total_capacity: self.ring.chain_total_capacity() as u64,
            warm_capacity: self.ring.warm_capacity().map_or(0, |c| c as u64),
            warm_hits: self.ring.warm_hits(),
        }
    }
}

impl CapacitySubscriberObject {
    /// Wake this subscriber's parked wait so a destroy can proceed.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.waker.wake_all();
    }

    fn try_next(&self, out: &mut [u8]) -> Result<(), i32> {
        match self.subscriber.lock().try_next(out) {
            Ok(()) => Ok(()),
            Err(PubSubReadError::Pending) => Err(SUBETHA_E_PUBSUB_PENDING),
            Err(PubSubReadError::Lost) => Err(fail(SUBETHA_E_PUBSUB_LOST, "the position was overwritten before it was read")),
        }
    }

    fn next_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, &self.waker, deadline, SUBETHA_E_PUBSUB_PENDING, || self.try_next(out))
    }
}

fn with_ring(handle: subetha_handle, f: impl FnOnce(&CapacityPubSubObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_CAPACITY_PUBSUB, |object| match object {
        Object::CapacityPubSub(p) => f(p),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a capacity-adaptive pub/sub ring"),
    })
}

fn with_subscriber(handle: subetha_handle, f: impl FnOnce(&CapacitySubscriberObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_CAPACITY_SUBSCRIBER, |object| match object {
        Object::CapacitySubscriber(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a capacity pub/sub subscriber"),
    })
}

/// Create a capacity-adaptive pub/sub ring in anonymous memory.
/// `initial_capacity` is a power of two of at least 2.
///
/// # Safety
/// `options` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_create_anon(
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
        let ring = match CapacityPubSubRing::create_anon(cap) {
            Ok(r) => r,
            Err(e) => return pubsub_capacity_code(e),
        };
        match CapacityPubSubObject::build(ring, &Locale::Anon, &options, mode) {
            Ok(object) => unsafe { issue(Object::CapacityPubSub(object), out) },
            Err(code) => code,
        }
    })
}

/// Create a file-backed capacity-adaptive pub/sub ring under `base_path`,
/// or attach to one that exists there.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_create(
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
        let ring = match CapacityPubSubRing::create(base, cap) {
            Ok(r) => r,
            Err(e) => return pubsub_capacity_code(e),
        };
        match CapacityPubSubObject::build(ring, &Locale::File(base), &options, mode) {
            Ok(object) => unsafe { issue(Object::CapacityPubSub(object), out) },
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
    let ring = match CapacityPubSubRing::create_shmfs(prefix, cap) {
        Ok(r) => r,
        Err(e) => return pubsub_capacity_code(e),
    };
    let locale = Locale::Shm { name: prefix, namespace: ShmNamespace::Session, create, sddl: None };
    match CapacityPubSubObject::build(ring, &locale, &options, mode) {
        Ok(object) => unsafe { issue(Object::CapacityPubSub(object), out) },
        Err(code) => code,
    }
}

/// Create a capacity-adaptive pub/sub ring in named shared memory under
/// `name_prefix`, in the session namespace, initializing its waker.
///
/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_create_shm(
    name_prefix: *const c_char,
    initial_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| unsafe { shm_ring(name_prefix, initial_capacity, options, out, true) })
}

/// Attach to a capacity-adaptive pub/sub ring another process created in
/// named shared memory under `name_prefix`; the waker is attached, not
/// reset.
///
/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_open_shm(
    name_prefix: *const c_char,
    initial_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| unsafe { shm_ring(name_prefix, initial_capacity, options, out, false) })
}

/// Publish up to `SUBETHA_PUBSUB_PAYLOAD_BYTES` bytes into the newest
/// backing, never waiting; the position within that backing lands in
/// `out_position` when it is not null.
///
/// # Safety
/// `data` points to `len` readable bytes; `out_position` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_publish(handle: subetha_handle, data: *const u8, len: usize, out_position: *mut u64) -> i32 {
    with_ring(handle, |p| {
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

fn issue_subscriber(p: &CapacityPubSubObject, subscriber: CapacityPubSubSubscriber, mode: u32, out: *mut subetha_handle) -> i32 {
    if out.is_null() {
        return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
    }
    let object = CapacitySubscriberObject {
        subscriber: Mutex::new(subscriber),
        waker: Arc::clone(&p.waker),
        mode,
        waiting: Waiting::new(),
    };
    unsafe { issue(Object::CapacitySubscriber(object), out) }
}

/// Subscribe from the newest backing's current head: only what is
/// published from now on.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_subscribe_from_now(handle: subetha_handle, mode: u32, out: *mut subetha_handle) -> i32 {
    with_ring(handle, |p| {
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        issue_subscriber(p, p.ring.subscribe_from_now(), mode, out)
    })
}

/// Subscribe from the start of the oldest backing still in the chain:
/// every item still held, backing by backing.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_subscribe_from_oldest(handle: subetha_handle, mode: u32, out: *mut subetha_handle) -> i32 {
    with_ring(handle, |p| {
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        issue_subscriber(p, p.ring.subscribe_from_oldest(), mode, out)
    })
}

/// Morph the ring's capacity to `new_capacity`, a power of two of at
/// least 2, appending a fresh backing to the chain.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_pubsub_morph_capacity(handle: subetha_handle, new_capacity: u32) -> i32 {
    with_ring(handle, |p| match p.ring.morph_capacity_to(new_capacity as usize) {
        Ok(()) => SUBETHA_OK,
        Err(e) => pubsub_capacity_code(e),
    })
}

/// Build the backing a coming morph to `capacity` needs, off the data
/// path; the contract is `subetha_capacity_prewarm`'s.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_pubsub_prewarm(handle: subetha_handle, capacity: u32) -> i32 {
    with_ring(handle, |p| match p.ring.prewarm(capacity as usize) {
        Ok(()) => SUBETHA_OK,
        Err(e) => pubsub_capacity_code(e),
    })
}

/// Drop the warm backing.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_pubsub_clear_warm(handle: subetha_handle) -> i32 {
    with_ring(handle, |p| {
        p.ring.clear_warm();
        SUBETHA_OK
    })
}

/// Reclaim the oldest backings no subscriber still reads; the number
/// reclaimed lands in `out` when it is not null. The active backing is
/// never reclaimed.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_gc(handle: subetha_handle, out: *mut u32) -> i32 {
    with_ring(handle, |p| {
        let reclaimed = p.ring.gc();
        if !out.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out = reclaimed as u32 };
        }
        SUBETHA_OK
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_read_stats(handle: subetha_handle, out: *mut subetha_capacity_pubsub_stats) -> i32 {
    with_ring(handle, |p| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = p.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Wake every subscriber parked in a wait on this ring.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_pubsub_wake_all(handle: subetha_handle) -> i32 {
    with_ring(handle, |p| {
        p.waker.wake_all();
        SUBETHA_OK
    })
}

/// Read the next item into `out`, at least `SUBETHA_PUBSUB_PAYLOAD_BYTES`,
/// crossing into the next backing when this one is drained.
/// `SUBETHA_E_PUBSUB_PENDING` when nothing newer is published,
/// `SUBETHA_E_PUBSUB_LOST` when the position was overwritten.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_subscriber_try_next(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
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

/// `subetha_capacity_subscriber_try_next`, parking while nothing newer is
/// published; the waiting contract is `subetha_subscriber_next_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_subscriber_next_wait(
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

/// Where the subscriber stands: the backing's index in the chain into
/// `out_backing` and the position within it into `out_position`; either
/// may be null.
///
/// # Safety
/// `out_backing` and `out_position` are null or valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_subscriber_position(handle: subetha_handle, out_backing: *mut u64, out_position: *mut u64) -> i32 {
    with_subscriber(handle, |s| {
        let subscriber = s.subscriber.lock();
        if !out_backing.is_null() {
            // SAFETY: checked non-null; the caller guarantees it is writable.
            unsafe { *out_backing = subscriber.backing_idx() };
        }
        if !out_position.is_null() {
            // SAFETY: as above.
            unsafe { *out_position = subscriber.position() };
        }
        SUBETHA_OK
    })
}

/// The mode a subscriber was created in, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_subscriber_mode(handle: subetha_handle, out: *mut u32) -> i32 {
    with_subscriber(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = s.mode };
        SUBETHA_OK
    })
}

/// Remove every backing file under `base_path` and the waker file; the
/// contract is `subetha_capacity_unlink`'s.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `report` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_pubsub_unlink(base_path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        found.remove(with_suffix(base, ".cwaker.bin"));
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
    fn a_subscriber_crosses_the_chain_after_a_morph_and_gc_reclaims_behind_it() {
        let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
        let ring = CapacityPubSubRing::create_anon(4).unwrap();
        let object = CapacityPubSubObject::build(ring, &Locale::Anon, &options, SUBETHA_MODE_STRICT).unwrap();
        let subscriber = CapacitySubscriberObject {
            subscriber: Mutex::new(object.ring.subscribe_from_now()),
            waker: Arc::clone(&object.waker),
            mode: SUBETHA_MODE_STRICT,
            waiting: Waiting::new(),
        };
        for i in 0..3u8 {
            object.publish(&[i]);
        }
        object.ring.morph_capacity_to(8).unwrap();
        for i in 3..6u8 {
            object.publish(&[i]);
        }
        assert_eq!(object.stats().chain_len, 2);
        let mut out = [0u8; SUBETHA_PUBSUB_PAYLOAD_BYTES];
        for i in 0..6u8 {
            subscriber.try_next(&mut out).unwrap();
            assert_eq!(out[0], i, "the old backing drains before the new one");
        }
        assert_eq!(subscriber.try_next(&mut out).unwrap_err(), SUBETHA_E_PUBSUB_PENDING);
        let (backing, position) = {
            let s = subscriber.subscriber.lock();
            (s.backing_idx(), s.position())
        };
        assert_eq!((backing, position), (1, 3));
        drop(subscriber);
        assert_eq!(object.ring.gc(), 1, "the drained backing is reclaimed once nobody reads it");
        assert_eq!(object.stats().chain_len, 1);
    }
}
