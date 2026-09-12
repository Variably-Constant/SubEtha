//! The Chase-Lev work-stealing deque through the C ABI, at an element
//! size the caller declares. One process owns the deque and pushes and
//! pops its bottom end without a compare-and-swap; any number of thieves
//! in any number of processes open it and steal from the top end with one
//! compare-and-swap each. The element layout (size, alignment and a
//! caller-chosen tag) is written into the deque's header at create and
//! checked at every open. The waiting forms park on a consumer waker and
//! a producer waker the ABI keeps beside the deque file, at
//! `<path>.cwaker.bin` and `<path>.pwaker.bin`: a push wakes waiting
//! thieves and a steal wakes an owner waiting for room. Strict and
//! managed modes are the same here, the deque runs no background work.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::time::Instant;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::raw_deque::RawDeque;
use subetha_cxc::shared_deque::DequeError;

use crate::error::{
    deque_code, fail, SUBETHA_E_DEQUE_NOT_OWNER, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY, SUBETHA_E_RING_FULL,
    SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_DEQUE};
use crate::ring::{
    bytes, deadline_from, finish_unlink, out_buffer, read_options, subetha_ring_options, subetha_unlink_report, text,
    wakers_for, with_suffix, Locale,
};
use crate::batch::{run_pop_many, run_push_many};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::stack::{read_layout, subetha_element_layout};
use crate::wait::{wait_until, Waiting, WAKE_ANY};

/// The handle was created by `subetha_deque_create`: it pushes, pops and
/// steals.
pub const SUBETHA_DEQUE_OWNER: u32 = 0;
/// The handle was opened by `subetha_deque_open_thief`: it steals.
pub const SUBETHA_DEQUE_THIEF: u32 = 1;

/// A snapshot of a work-stealing deque.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_deque_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// `SUBETHA_DEQUE_OWNER` or `SUBETHA_DEQUE_THIEF`.
    pub role: u32,
    /// Slots in the deque, a power of two.
    pub capacity: u64,
    /// Bytes per slot: the element size rounded up to eight.
    pub slot_bytes: u64,
    /// Bytes per element.
    pub element_size: u64,
    /// The element alignment the layout declares.
    pub alignment: u64,
    /// The layout tag the deque was created with.
    pub tag: u64,
    /// Elements taken from the top so far, by steals and by the owner's
    /// pop of the last element.
    pub top: i64,
    /// Elements the owner has pushed, less the ones it popped.
    pub bottom: i64,
    /// `bottom - top`, racing concurrent pushes and steals.
    pub approx_len: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
}

pub(crate) struct DequeObject {
    deque: RawDeque,
    role: u32,
    consumer_waker: CrossProcessWaker,
    producer_waker: CrossProcessWaker,
    mode: u32,
    waiting: Waiting,
}

impl DequeObject {
    fn build(deque: RawDeque, role: u32, path: &Path, options: subetha_ring_options) -> Result<Self, i32> {
        let mode = resolve_mode(options.mode)?;
        let (consumer_waker, producer_waker) = wakers_for(&Locale::File(path), options.max_waiters)?;
        Ok(Self {
            deque,
            role,
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

    fn element_size(&self) -> usize {
        self.deque.layout().slot_size
    }

    fn require_owner(&self) -> Result<(), i32> {
        if self.role == SUBETHA_DEQUE_OWNER {
            Ok(())
        } else {
            Err(fail(SUBETHA_E_DEQUE_NOT_OWNER, "this handle opened the deque as a thief"))
        }
    }

    fn try_push(&self, payload: &[u8]) -> Result<(), i32> {
        self.require_owner()?;
        if payload.len() > self.element_size() {
            return Err(fail(
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
                format!("{} bytes exceed the {}-byte element", payload.len(), self.element_size()),
            ));
        }
        self.deque.push(payload).map_err(deque_code)?;
        self.consumer_waker.wake_one_up_to(WAKE_ANY);
        Ok(())
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, i32> {
        self.require_owner()?;
        match self.deque.pop(out) {
            Some(n) => Ok(n),
            None => Err(SUBETHA_E_RING_EMPTY),
        }
    }

    fn try_steal(&self, out: &mut [u8]) -> Result<usize, i32> {
        match self.deque.steal(out) {
            Some(n) => {
                self.producer_waker.wake_one_up_to(WAKE_ANY);
                Ok(n)
            }
            None => Err(SUBETHA_E_RING_EMPTY),
        }
    }

    fn push_wait(&self, payload: &[u8], deadline: Option<Instant>) -> Result<(), i32> {
        self.require_owner()?;
        wait_until(&self.waiting, &self.producer_waker, deadline, SUBETHA_E_RING_FULL, || self.try_push(payload))
    }

    fn steal_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        wait_until(&self.waiting, &self.consumer_waker, deadline, SUBETHA_E_RING_EMPTY, || self.try_steal(out))
    }

    fn stats(&self) -> subetha_deque_stats {
        let layout = self.deque.layout();
        subetha_deque_stats {
            mode: self.mode,
            role: self.role,
            capacity: self.deque.capacity() as u64,
            slot_bytes: self.deque.slot_bytes() as u64,
            element_size: layout.slot_size as u64,
            alignment: layout.alignment as u64,
            tag: layout.tag,
            top: self.deque.top(),
            bottom: self.deque.bottom(),
            approx_len: self.deque.approx_len() as u64,
            waker_full: self.waiting.waker_full(),
        }
    }
}

fn with_deque(handle: subetha_handle, f: impl FnOnce(&DequeObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_DEQUE, |object| match object {
        Object::Deque(d) => f(d),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a deque"),
    })
}

fn place(deque: Result<RawDeque, DequeError>, role: u32, path: &Path, options: subetha_ring_options, out: *mut subetha_handle) -> i32 {
    let deque = match deque {
        Ok(d) => d,
        Err(e) => return deque_code(e),
    };
    match DequeObject::build(deque, role, path, options) {
        Ok(object) => unsafe { issue(Object::Deque(object), out) },
        Err(code) => code,
    }
}

/// Create the deque at `path` as its owner, truncating any file there.
/// `capacity` is a power of two of at least 1; the calling process is
/// recorded as the owner, and single-owner discipline is the caller's
/// contract. `scan_interval_us` in the options is ignored, the deque has
/// no sidecar.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout`, `options` and `out`
/// are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_create(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
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
        if capacity == 0 || !capacity.is_power_of_two() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, format!("capacity {capacity} is not a power of two of at least 1"));
        }
        let layout = match unsafe { read_layout(layout) } {
            Ok(l) => l,
            Err(code) => return code,
        };
        place(RawDeque::create(path, capacity as usize, layout), SUBETHA_DEQUE_OWNER, path, options, out)
    })
}

/// Open the deque another process created at `path` as a thief: the
/// handle steals, and its push and pop return
/// `SUBETHA_E_DEQUE_NOT_OWNER`. `SUBETHA_E_RING_IO` names an absent file
/// and `SUBETHA_E_RING_LAYOUT_MISMATCH` one of another layout.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout`, `options` and `out`
/// are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_open_thief(
    path: *const c_char,
    layout: *const subetha_element_layout,
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
        let layout = match unsafe { read_layout(layout) } {
            Ok(l) => l,
            Err(code) => return code,
        };
        place(RawDeque::open_as_thief(path, layout), SUBETHA_DEQUE_THIEF, path, options, out)
    })
}

/// Owner side: push up to `element_size` bytes onto the bottom without
/// waiting; the slot is zero past the payload. `SUBETHA_E_RING_FULL` when
/// the deque is at capacity, `SUBETHA_E_RING_PAYLOAD_TOO_LARGE` for more
/// bytes than an element, `SUBETHA_E_DEQUE_NOT_OWNER` from a thief.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_deque(handle, |d| {
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match d.try_push(payload) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Owner side: push `count` elements of `len` bytes each, the first at
/// `items` and each next one `stride` bytes on, under one handle lookup
/// and one panic guard. The batch contract is
/// `subetha_ring_try_push_many`'s; a thief's handle is
/// `SUBETHA_E_DEQUE_NOT_OWNER` before anything is pushed.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_done` is a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_try_push_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_deque(handle, |d| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_push_many(items, stride, len, count, out_done, |payload| match d.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Thief side: steal up to `count` elements, the first written at `out`
/// and each next one `stride` bytes on; `stride` is at least the element
/// size. The batch contract is `subetha_ring_try_pop_many`'s.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_try_steal_many(
    handle: subetha_handle,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_deque(handle, |d| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, d.deque.slot_bytes(), count, out_done, |buf| match d.try_steal(buf) {
                Ok(_) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Owner side: push, parking until a steal makes room or `timeout_ms`
/// elapses; the waiting contract is `subetha_ring_push_wait`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_push_wait(handle: subetha_handle, data: *const u8, len: usize, timeout_ms: i64) -> i32 {
    with_deque(handle, |d| {
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match d.push_wait(payload, deadline) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Owner side: pop the bottom element, `slot_bytes` bytes, into `out`;
/// `cap` must be at least that. `SUBETHA_E_RING_EMPTY` when the deque is
/// empty or a thief took the last element first,
/// `SUBETHA_E_DEQUE_NOT_OWNER` from a thief.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_try_pop(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_deque(handle, |d| {
        let buf = match unsafe { out_buffer(out, cap, out_len, d.deque.slot_bytes()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match d.try_pop(buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Steal the top element, `slot_bytes` bytes, into `out` without waiting;
/// `cap` must be at least that. Any handle steals. `SUBETHA_E_RING_EMPTY`
/// when the deque is empty or another taker won the element.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_try_steal(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_deque(handle, |d| {
        let buf = match unsafe { out_buffer(out, cap, out_len, d.deque.slot_bytes()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match d.try_steal(buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Steal, parking until the owner pushes or `timeout_ms` elapses; the
/// waiting contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_steal_wait(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_deque(handle, |d| {
        let buf = match unsafe { out_buffer(out, cap, out_len, d.deque.slot_bytes()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match d.steal_wait(buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// A snapshot of the deque into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_read_stats(handle: subetha_handle, out: *mut subetha_deque_stats) -> i32 {
    with_deque(handle, |d| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = d.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the deque's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_deque_flush(handle: subetha_handle) -> i32 {
    with_deque(handle, |d| match d.deque.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => crate::error::io_code(e),
    })
}

/// Wake every thread parked in a wait on this deque; each re-checks the
/// deque and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_deque_wake_all(handle: subetha_handle) -> i32 {
    with_deque(handle, |d| {
        d.consumer_waker.wake_all();
        d.producer_waker.wake_all();
        SUBETHA_OK
    })
}

/// Remove the deque file at `path` and its two waker files. The contract
/// is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_deque_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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
    use subetha_cxc::raw_treiber_stack::ElementLayout;

    /// A deque file for one test under the temp directory, with its
    /// wakers; removed when the guard drops.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!("subetha-ffi-deque-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            for suffix in [".cwaker.bin", ".pwaker.bin"] {
                found.remove(with_suffix(&self.0, suffix));
            }
            assert_eq!(found.failed, 0, "the deque's files were removed: {:?}", found.first_failure);
        }
    }

    fn layout() -> ElementLayout {
        ElementLayout { slot_size: 12, alignment: 4, tag: 11 }
    }

    fn options() -> subetha_ring_options {
        subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 4, scan_interval_us: 0, ..Default::default() }
    }

    fn owner(path: &Path, capacity: usize) -> DequeObject {
        DequeObject::build(RawDeque::create(path, capacity, layout()).unwrap(), SUBETHA_DEQUE_OWNER, path, options()).unwrap()
    }

    fn thief(path: &Path) -> DequeObject {
        DequeObject::build(RawDeque::open_as_thief(path, layout()).unwrap(), SUBETHA_DEQUE_THIEF, path, options()).unwrap()
    }

    #[test]
    fn the_owner_pushes_and_pops_lifo_and_a_thief_steals_fifo() {
        let scratch = Scratch::new("roles");
        let owner = owner(&scratch.0, 16);
        let thief = thief(&scratch.0);
        assert_eq!(owner.stats().slot_bytes, 16);
        assert_eq!(owner.stats().element_size, 12);
        assert_eq!(thief.stats().role, SUBETHA_DEQUE_THIEF);
        for i in 0..6u8 {
            owner.try_push(&[i; 12]).unwrap();
        }
        assert_eq!(owner.try_push(&[0; 13]).unwrap_err(), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
        assert_eq!(thief.try_push(&[9; 12]).unwrap_err(), SUBETHA_E_DEQUE_NOT_OWNER);
        let mut out = [0xFFu8; 16];
        assert_eq!(thief.try_pop(&mut out).unwrap_err(), SUBETHA_E_DEQUE_NOT_OWNER);
        assert_eq!(owner.try_pop(&mut out).unwrap(), 16);
        assert_eq!(&out[..12], &[5; 12]);
        assert_eq!(&out[12..], &[0; 4]);
        assert_eq!(thief.try_steal(&mut out).unwrap(), 16);
        assert_eq!(out[0], 0);
        assert_eq!(thief.try_steal(&mut out).unwrap(), 16);
        assert_eq!(out[0], 1);
        assert_eq!(owner.stats().approx_len, 3);
        assert_eq!(owner.stats().top, 2);
        assert_eq!(owner.stats().bottom, 5);
        for _ in 0..3 {
            owner.try_pop(&mut out).unwrap();
        }
        assert_eq!(owner.try_pop(&mut out).unwrap_err(), SUBETHA_E_RING_EMPTY);
        assert_eq!(thief.try_steal(&mut out).unwrap_err(), SUBETHA_E_RING_EMPTY);
        drop(thief);
        drop(owner);
    }

    #[test]
    fn a_steal_parks_until_a_push_and_a_push_parks_until_a_steal() {
        let scratch = Scratch::new("waits");
        let owner = Arc::new(owner(&scratch.0, 4));
        let thief = Arc::new(thief(&scratch.0));
        let mut out = [0u8; 16];
        let soon = Some(Instant::now() + Duration::from_millis(20));
        assert_eq!(thief.steal_wait(&mut out, soon).unwrap_err(), crate::error::SUBETHA_E_TIMEOUT);
        let pusher = {
            let owner = Arc::clone(&owner);
            std::thread::spawn(move || {
                for i in 0..200u8 {
                    owner.push_wait(&[i; 12], None).unwrap();
                }
            })
        };
        let mut seen = 0u32;
        while seen < 200 {
            thief.steal_wait(&mut out, None).unwrap();
            seen += 1;
        }
        pusher.join().unwrap();
        assert_eq!(owner.stats().approx_len, 0);
        drop(thief);
        drop(owner);
    }
}
