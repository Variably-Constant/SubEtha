//! The bare Lamport SPSC pair through the C ABI: a producer handle and a
//! consumer handle over one ring, try forms only, no wakers. The blocking
//! SPSC ring is the same ring with wakers beside it. Each handle is one
//! thread's at a time in the Rust API; here calls on one handle are
//! serialized. Strict and managed modes are the same: no background work.
//!
//! In the file locale `path` names the ring file itself, as in the Rust
//! API, so a pair the Rust API created at a path is the same pair.

use std::ffi::c_char;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::shared_ring::{Consumer, Producer, RingError, SharedRingSpsc};

use crate::error::{fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::batch::{run_pop_many, run_push_many};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_LAMPORT_CONSUMER, SUBETHA_KIND_LAMPORT_PRODUCER};
use crate::ring::{bytes, checked_capacity, finish_unlink, out_buffer, subetha_unlink_report, text, SUBETHA_RING_SLOT_BYTES};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A snapshot of a Lamport pair's producer side.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_lamport_producer_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots in the ring.
    pub capacity: u64,
    /// Items pushed so far.
    pub head: u64,
}

/// A snapshot of a Lamport pair's consumer side.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_lamport_consumer_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots in the ring.
    pub capacity: u64,
    /// Items popped so far.
    pub tail: u64,
}

pub(crate) struct LamportProducerObject {
    producer: Mutex<Producer>,
    mode: u32,
}

pub(crate) struct LamportConsumerObject {
    consumer: Mutex<Consumer>,
    mode: u32,
}

impl LamportProducerObject {
    /// Nothing parks on a bare pair, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

impl LamportConsumerObject {
    /// Nothing parks on a bare pair, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

fn with_producer(handle: subetha_handle, f: impl FnOnce(&LamportProducerObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LAMPORT_PRODUCER, |object| match object {
        Object::LamportProducer(p) => f(p),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a Lamport producer"),
    })
}

fn with_consumer(handle: subetha_handle, f: impl FnOnce(&LamportConsumerObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LAMPORT_CONSUMER, |object| match object {
        Object::LamportConsumer(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a Lamport consumer"),
    })
}

/// Place a built pair and write its two handles.
///
/// # Safety
/// `out_producer` and `out_consumer` are valid pointers.
unsafe fn issue_pair(pair: Result<(Producer, Consumer), RingError>, mode: u32, out_producer: *mut subetha_handle, out_consumer: *mut subetha_handle) -> i32 {
    let (producer, consumer) = match pair {
        Ok(pair) => pair,
        Err(e) => return ring_code(e),
    };
    let rc = unsafe {
        issue(
            Object::LamportProducer(LamportProducerObject { producer: Mutex::new(producer), mode }),
            out_producer,
        )
    };
    if rc != SUBETHA_OK {
        return rc;
    }
    unsafe {
        issue(
            Object::LamportConsumer(LamportConsumerObject { consumer: Mutex::new(consumer), mode }),
            out_consumer,
        )
    }
}

fn pair_args(capacity: u32, mode: u32, out_producer: *mut subetha_handle, out_consumer: *mut subetha_handle) -> Result<(usize, u32), i32> {
    if out_producer.is_null() || out_consumer.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out_producer or out_consumer is null"));
    }
    Ok((checked_capacity(capacity)?, resolve_mode(mode)?))
}

/// Create a Lamport pair in anonymous memory: the producer handle into
/// `out_producer`, the consumer handle into `out_consumer`.
///
/// # Safety
/// `out_producer` and `out_consumer` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_create_anon_pair(
    capacity: u32,
    mode: u32,
    out_producer: *mut subetha_handle,
    out_consumer: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let (cap, mode) = match pair_args(capacity, mode, out_producer, out_consumer) {
            Ok(a) => a,
            Err(code) => return code,
        };
        unsafe { issue_pair(SharedRingSpsc::create_anon_pair(cap), mode, out_producer, out_consumer) }
    })
}

/// Create a file-backed Lamport pair at `path`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out_producer` and
/// `out_consumer` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_create_pair(
    path: *const c_char,
    capacity: u32,
    mode: u32,
    out_producer: *mut subetha_handle,
    out_consumer: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let (cap, mode) = match pair_args(capacity, mode, out_producer, out_consumer) {
            Ok(a) => a,
            Err(code) => return code,
        };
        unsafe { issue_pair(SharedRingSpsc::create_pair(path, cap), mode, out_producer, out_consumer) }
    })
}

/// Attach to a file-backed Lamport ring another process created at `path`
/// and receive both handles; a process plays one side and destroys the
/// other. One producer and one consumer across every process is the
/// caller's contract.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out_producer` and
/// `out_consumer` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_open_pair(
    path: *const c_char,
    expected_capacity: u32,
    mode: u32,
    out_producer: *mut subetha_handle,
    out_consumer: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let path = match unsafe { text(path, "path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let (cap, mode) = match pair_args(expected_capacity, mode, out_producer, out_consumer) {
            Ok(a) => a,
            Err(code) => return code,
        };
        unsafe { issue_pair(SharedRingSpsc::open_pair(path, cap), mode, out_producer, out_consumer) }
    })
}

/// Push `len` bytes without waiting; the slot semantics are the adaptive
/// ring's. `SUBETHA_E_RING_FULL` when there is no room.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_producer(handle, |p| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match p.producer.lock().try_push(payload) {
            Ok(()) => SUBETHA_OK,
            Err(e) => ring_code(e),
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
pub unsafe extern "C" fn subetha_lamport_try_push_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_producer(handle, |p| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        let producer = p.producer.lock();
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_push_many(items, stride, len, count, out_done, |payload| match producer.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(e) => ring_code(e),
            })
        }
    })
}

/// Pop up to `count` slots, the first written at `out` and each next one
/// `stride` bytes on. The batch contract is
/// `subetha_ring_try_pop_many`'s.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_try_pop_many(
    handle: subetha_handle,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_consumer(handle, |c| {
        let consumer = c.consumer.lock();
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, SUBETHA_RING_SLOT_BYTES, count, out_done, |buf| match consumer.try_pop(buf) {
                Ok(_) => SUBETHA_OK,
                Err(e) => ring_code(e),
            })
        }
    })
}

/// Pop one slot into `out` without waiting; `cap` must be at least
/// `SUBETHA_RING_SLOT_BYTES`. `SUBETHA_E_RING_EMPTY` when there is nothing
/// to take.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_try_pop(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_consumer(handle, |c| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match c.consumer.lock().try_pop(buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(e) => ring_code(e),
        }
    })
}

/// A snapshot of the producer side into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_producer_read_stats(handle: subetha_handle, out: *mut subetha_lamport_producer_stats) -> i32 {
    with_producer(handle, |p| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let producer = p.producer.lock();
        let stats = subetha_lamport_producer_stats {
            mode: p.mode,
            capacity: producer.capacity() as u64,
            head: producer.head(),
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// A snapshot of the consumer side into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_consumer_read_stats(handle: subetha_handle, out: *mut subetha_lamport_consumer_stats) -> i32 {
    with_consumer(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let consumer = c.consumer.lock();
        let stats = subetha_lamport_consumer_stats {
            mode: c.mode,
            capacity: consumer.capacity() as u64,
            tail: consumer.tail(),
        };
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove the ring file a Lamport pair was created at. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_lamport_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
        unsafe { finish_unlink(found, report) }
    })
}
