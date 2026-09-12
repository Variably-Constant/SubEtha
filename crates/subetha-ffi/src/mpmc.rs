//! The blocking MPMC grid through the C ABI: N producer handles, each the
//! sole writer of its own ring, and M consumer handles, each the sole
//! drainer of a subset of those rings (`ring % M`), every side with a try
//! and a waiting form. Calls on one handle are serialized, as in the MPSC
//! pool. Strict and managed modes are the same: no background work.
//!
//! In the file locale the grid the Rust API creates under a prefix is the
//! same grid: `<prefix>.ring.<i>.bin` and `<prefix>.pw.<i>.bin` per
//! producer, `<prefix>.cw.<m>.bin` per consumer. Every process that opens
//! the grid receives every handle, uses the ones it plays, and destroys
//! the rest.

use std::ffi::c_char;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::blocking_mpmc_ring::{BlockingMpmcConsumer, BlockingMpmcProducer, BlockingMpmcRing};
use subetha_cxc::cross_process_waker::CrossProcessWaker;

use crate::error::{
    blocking_code, fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY,
    SUBETHA_E_RING_FULL, SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::batch::{run_pop_many, run_push_many};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_MPMC_CONSUMER, SUBETHA_KIND_MPMC_PRODUCER};
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, out_buffer, subetha_unlink_report, text,
    with_suffix, SUBETHA_RING_SLOT_BYTES,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting};

/// A snapshot of one producer of a blocking MPMC grid.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_mpmc_producer_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Slots in this producer's ring.
    pub capacity: u64,
    /// Items this producer pushed so far.
    pub head: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
}

/// A snapshot of one consumer of a blocking MPMC grid.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_mpmc_consumer_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Producer rings in this consumer's subset.
    pub n_rings: u32,
    /// Items pending across the subset, read without a lock.
    pub approx_subset_len: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
}

pub(crate) struct MpmcProducerObject {
    producer: Mutex<BlockingMpmcProducer>,
    waker: Arc<CrossProcessWaker>,
    mode: u32,
    waiting: Waiting,
}

pub(crate) struct MpmcConsumerObject {
    consumer: Mutex<BlockingMpmcConsumer>,
    waker: Arc<CrossProcessWaker>,
    mode: u32,
    waiting: Waiting,
}

impl MpmcProducerObject {
    fn new(producer: BlockingMpmcProducer, mode: u32) -> Self {
        let waker = Arc::clone(producer.own_waker());
        Self {
            producer: Mutex::new(producer),
            waker,
            mode,
            waiting: Waiting::new(),
        }
    }

    /// Wake a parked push so a destroy can proceed; it returns
    /// `SUBETHA_E_DESTROYED`.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.waker.wake_all();
    }

    fn try_push(&self, payload: &[u8]) -> Result<(), i32> {
        self.producer.lock().try_push(payload).map_err(ring_code)
    }

    fn push_wait(&self, payload: &[u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, &self.waker, deadline, SUBETHA_E_RING_FULL, || self.try_push(payload))
    }

    fn stats(&self) -> subetha_mpmc_producer_stats {
        let p = self.producer.lock();
        subetha_mpmc_producer_stats {
            mode: self.mode,
            capacity: p.capacity() as u64,
            head: p.head(),
            waker_full: self.waiting.waker_full(),
        }
    }
}

impl MpmcConsumerObject {
    fn new(consumer: BlockingMpmcConsumer, mode: u32) -> Self {
        let waker = Arc::clone(consumer.own_waker());
        Self {
            consumer: Mutex::new(consumer),
            waker,
            mode,
            waiting: Waiting::new(),
        }
    }

    /// Wake a parked pop so a destroy can proceed; it returns
    /// `SUBETHA_E_DESTROYED`.
    pub(crate) fn interrupt(&self) {
        self.waiting.close();
        self.waker.wake_all();
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, i32> {
        self.consumer.lock().try_pop(out).map_err(ring_code)
    }

    fn pop_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        wait_until(&self.waiting, &self.waker, deadline, SUBETHA_E_RING_EMPTY, || self.try_pop(out))
    }

    fn stats(&self) -> subetha_mpmc_consumer_stats {
        let c = self.consumer.lock();
        subetha_mpmc_consumer_stats {
            mode: self.mode,
            n_rings: c.n_rings() as u32,
            approx_subset_len: c.approx_subset_len() as u64,
            waker_full: self.waiting.waker_full(),
        }
    }
}

fn with_producer(handle: subetha_handle, f: impl FnOnce(&MpmcProducerObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_MPMC_PRODUCER, |object| match object {
        Object::MpmcProducer(p) => f(p),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an MPMC producer"),
    })
}

fn with_consumer(handle: subetha_handle, f: impl FnOnce(&MpmcConsumerObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_MPMC_CONSUMER, |object| match object {
        Object::MpmcConsumer(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an MPMC consumer"),
    })
}

/// The arguments every grid constructor checks: at least one consumer, at
/// least as many producers, a ring capacity, a mode, and somewhere to write
/// the handles.
fn grid_args(
    n_producers: u32,
    n_consumers: u32,
    capacity: u32,
    mode: u32,
    out_producers: *mut subetha_handle,
    out_consumers: *mut subetha_handle,
) -> Result<(usize, usize, usize, u32), i32> {
    if n_consumers == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "n_consumers must be at least 1"));
    }
    if n_producers < n_consumers {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("n_producers {n_producers} is below n_consumers {n_consumers}; every consumer owns at least one ring"),
        ));
    }
    if out_producers.is_null() || out_consumers.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out_producers or out_consumers is null"));
    }
    Ok((n_producers as usize, n_consumers as usize, checked_capacity(capacity)?, resolve_mode(mode)?))
}

/// Place a built grid's objects and write their handles.
///
/// # Safety
/// `out_producers` and `out_consumers` point to as many writable handles
/// as there are producers and consumers.
unsafe fn issue_grid(
    producers: Vec<BlockingMpmcProducer>,
    consumers: Vec<BlockingMpmcConsumer>,
    mode: u32,
    out_producers: *mut subetha_handle,
    out_consumers: *mut subetha_handle,
) -> i32 {
    for (i, producer) in producers.into_iter().enumerate() {
        let object = MpmcProducerObject::new(producer, mode);
        // SAFETY: the caller guarantees one writable handle per producer.
        let rc = unsafe { issue(Object::MpmcProducer(object), out_producers.add(i)) };
        if rc != SUBETHA_OK {
            return rc;
        }
    }
    for (m, consumer) in consumers.into_iter().enumerate() {
        let object = MpmcConsumerObject::new(consumer, mode);
        // SAFETY: the caller guarantees one writable handle per consumer.
        let rc = unsafe { issue(Object::MpmcConsumer(object), out_consumers.add(m)) };
        if rc != SUBETHA_OK {
            return rc;
        }
    }
    SUBETHA_OK
}

/// Create a blocking MPMC grid in anonymous memory: `n_producers` rings of
/// `capacity` slots partitioned across `n_consumers` consumers, one handle
/// each into `out_producers` and `out_consumers`.
///
/// # Safety
/// `out_producers` points to `n_producers` writable handles and
/// `out_consumers` to `n_consumers`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_create_anon_grid(
    n_producers: u32,
    n_consumers: u32,
    capacity: u32,
    mode: u32,
    out_producers: *mut subetha_handle,
    out_consumers: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let (np, nc, cap, mode) = match grid_args(n_producers, n_consumers, capacity, mode, out_producers, out_consumers) {
            Ok(a) => a,
            Err(code) => return code,
        };
        let (producers, consumers) = match BlockingMpmcRing::create_anon_grid(np, nc, cap) {
            Ok(grid) => grid,
            Err(e) => return blocking_code(e),
        };
        unsafe { issue_grid(producers, consumers, mode, out_producers, out_consumers) }
    })
}

/// Create a file-backed blocking MPMC grid under `path_prefix`.
///
/// # Safety
/// `path_prefix` is a NUL-terminated UTF-8 string; `out_producers` points to
/// `n_producers` writable handles and `out_consumers` to `n_consumers`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_create_grid(
    path_prefix: *const c_char,
    n_producers: u32,
    n_consumers: u32,
    capacity: u32,
    mode: u32,
    out_producers: *mut subetha_handle,
    out_consumers: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let prefix = match unsafe { text(path_prefix, "path_prefix") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let (np, nc, cap, mode) = match grid_args(n_producers, n_consumers, capacity, mode, out_producers, out_consumers) {
            Ok(a) => a,
            Err(code) => return code,
        };
        let (producers, consumers) = match BlockingMpmcRing::create_grid(prefix, np, nc, cap) {
            Ok(grid) => grid,
            Err(e) => return blocking_code(e),
        };
        unsafe { issue_grid(producers, consumers, mode, out_producers, out_consumers) }
    })
}

/// Attach to a file-backed blocking MPMC grid another process created
/// under `path_prefix`, with the counts and capacity it was created with.
///
/// # Safety
/// `path_prefix` is a NUL-terminated UTF-8 string; `out_producers` points to
/// `n_producers` writable handles and `out_consumers` to `n_consumers`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_open_grid(
    path_prefix: *const c_char,
    n_producers: u32,
    n_consumers: u32,
    expected_capacity: u32,
    mode: u32,
    out_producers: *mut subetha_handle,
    out_consumers: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let prefix = match unsafe { text(path_prefix, "path_prefix") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let (np, nc, cap, mode) =
            match grid_args(n_producers, n_consumers, expected_capacity, mode, out_producers, out_consumers) {
                Ok(a) => a,
                Err(code) => return code,
            };
        let (producers, consumers) = match BlockingMpmcRing::open_grid(prefix, np, nc, cap) {
            Ok(grid) => grid,
            Err(e) => return blocking_code(e),
        };
        unsafe { issue_grid(producers, consumers, mode, out_producers, out_consumers) }
    })
}

/// Push `len` bytes into this producer's ring without waiting; the slot
/// semantics are the adaptive ring's. `SUBETHA_E_RING_FULL` when there is
/// no room.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_producer(handle, |p| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        match unsafe { bytes(data, len) } {
            Ok(payload) => match p.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(code) => code,
            },
            Err(code) => code,
        }
    })
}

/// Push, parking until this producer's ring has room or `timeout_ms`
/// elapses; the waiting contract is `subetha_ring_push_wait`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_push_wait(handle: subetha_handle, data: *const u8, len: usize, timeout_ms: i64) -> i32 {
    with_producer(handle, |p| {
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
        match p.push_wait(payload, deadline) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Push `count` payloads of `len` bytes each into this producer's ring,
/// the first at `items` and each next one `stride` bytes on, under one
/// handle lookup and one panic guard. The batch contract is
/// `subetha_ring_try_push_many`'s.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_done` is a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_try_push_many(
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
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_push_many(items, stride, len, count, out_done, |payload| match p.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Pop up to `count` slots from the grid, the first written at `out` and
/// each next one `stride` bytes on. The batch contract is
/// `subetha_ring_try_pop_many`'s.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_try_pop_many(
    handle: subetha_handle,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_consumer(handle, |c| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, SUBETHA_RING_SLOT_BYTES, count, out_done, |buf| match c.try_pop(buf) {
                Ok(_) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Pop one slot from whichever ring of this consumer's subset has one,
/// without waiting; `cap` must be at least `SUBETHA_RING_SLOT_BYTES`.
/// `SUBETHA_E_RING_EMPTY` when the subset is empty.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_try_pop(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_consumer(handle, |c| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match c.try_pop(buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Pop, parking until a producer of this consumer's subset pushes or
/// `timeout_ms` elapses; the waiting contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_pop_wait(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_consumer(handle, |c| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match c.pop_wait(buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// A snapshot of one producer into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_producer_read_stats(handle: subetha_handle, out: *mut subetha_mpmc_producer_stats) -> i32 {
    with_producer(handle, |p| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = p.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// A snapshot of one consumer into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_consumer_read_stats(handle: subetha_handle, out: *mut subetha_mpmc_consumer_stats) -> i32 {
    with_consumer(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = c.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Remove every file a file-backed blocking MPMC grid under `path_prefix`
/// names, for the counts it was created with. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `path_prefix` is a NUL-terminated UTF-8 string; `report` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_mpmc_unlink(
    path_prefix: *const c_char,
    n_producers: u32,
    n_consumers: u32,
    report: *mut subetha_unlink_report,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let prefix = match unsafe { text(path_prefix, "path_prefix") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = UnlinkReport::default();
        for i in 0..n_producers {
            found.remove(with_suffix(prefix, &format!(".ring.{i}.bin")));
            found.remove(with_suffix(prefix, &format!(".pw.{i}.bin")));
        }
        for m in 0..n_consumers {
            found.remove(with_suffix(prefix, &format!(".cw.{m}.bin")));
        }
        unsafe { finish_unlink(found, report) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;
    use std::time::Duration;

    #[test]
    fn each_consumer_drains_its_own_subset_and_parks_until_it_has_work() {
        let (producers, consumers) = BlockingMpmcRing::create_anon_grid(4, 2, 8).expect("a grid");
        let producers: Vec<Arc<MpmcProducerObject>> = producers
            .into_iter()
            .map(|p| Arc::new(MpmcProducerObject::new(p, SUBETHA_MODE_STRICT)))
            .collect();
        let consumers: Vec<Arc<MpmcConsumerObject>> = consumers
            .into_iter()
            .map(|c| Arc::new(MpmcConsumerObject::new(c, SUBETHA_MODE_STRICT)))
            .collect();
        assert_eq!(consumers[0].stats().n_rings, 2);

        let drains: Vec<_> = consumers
            .iter()
            .map(|consumer| {
                let consumer = Arc::clone(consumer);
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    let mut buf = [0u8; SUBETHA_RING_SLOT_BYTES];
                    for _ in 0..2 {
                        consumer.pop_wait(&mut buf, None).unwrap();
                        got.push(buf[0]);
                    }
                    got.sort_unstable();
                    got
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(20));
        for (i, p) in producers.iter().enumerate() {
            p.try_push(&[i as u8]).unwrap();
        }
        let mut all: Vec<u8> = drains.into_iter().flat_map(|d| d.join().unwrap()).collect();
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3]);
    }
}
