//! The shared Treiber stack through the C ABI: a bounded lock-free LIFO
//! any number of processes push to and pop from, at an element size the
//! caller declares. The element layout (size, alignment and a
//! caller-chosen tag) is written into the stack's header at create and
//! checked at every attach, so two callers that disagree about what the
//! bytes mean are refused rather than paired. A try and a waiting form
//! of push and pop park on a consumer waker and a producer waker the ABI
//! keeps beside the stack file, at `<path>.cwaker.bin` and
//! `<path>.pwaker.bin`. The stack lives in a file, as the Rust type does;
//! strict and managed modes are the same here, the stack runs no
//! background work.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::time::Instant;

use subetha_cxc::adaptive_ring::UnlinkReport;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::raw_treiber_stack::{ElementLayout, RawTreiberStack};
use subetha_cxc::shared_treiber_stack::{StackError, STACK_NIL};

use crate::error::{
    fail, stack_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY, SUBETHA_E_RING_FULL,
    SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_STACK};
use crate::ring::{
    bytes, deadline_from, finish_unlink, out_buffer, read_options, subetha_ring_options, subetha_unlink_report, text,
    wakers_for, with_suffix, Locale,
};
use crate::batch::{run_pop_many, run_push_many};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};
use crate::wait::{wait_until, Waiting, WAKE_ANY};

/// The element layout a stack or deque is created with and every attach
/// states. `element_size` bytes are stored per element; `alignment` is
/// the element's alignment, a power of two; `tag` is a value the caller
/// chooses to name the element's type, and an attach whose tag differs
/// from the creator's is refused with `SUBETHA_E_RING_LAYOUT_MISMATCH`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_element_layout {
    /// Bytes per element; a push copies at most this many.
    pub element_size: u64,
    /// The element's alignment in bytes, a power of two.
    pub alignment: u64,
    /// A caller-chosen value naming the element's type.
    pub tag: u64,
}

/// A snapshot of a shared stack.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_stack_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Elements the stack can hold.
    pub capacity: u64,
    /// Bytes per element.
    pub element_size: u64,
    /// The element alignment the layout declares.
    pub alignment: u64,
    /// The layout tag the stack was created with.
    pub tag: u64,
    /// Elements on the stack, counted by walking it; racing pushes and pops.
    pub approx_len: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
}

pub(crate) struct StackObject {
    stack: RawTreiberStack,
    consumer_waker: CrossProcessWaker,
    producer_waker: CrossProcessWaker,
    mode: u32,
    waiting: Waiting,
}

impl StackObject {
    fn build(stack: RawTreiberStack, path: &Path, options: subetha_ring_options) -> Result<Self, i32> {
        let mode = resolve_mode(options.mode)?;
        let (consumer_waker, producer_waker) = wakers_for(&Locale::File(path), options.max_waiters)?;
        Ok(Self {
            stack,
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
        self.stack.slot_size()
    }

    fn try_push(&self, payload: &[u8]) -> Result<(), i32> {
        if payload.len() > self.element_size() {
            return Err(fail(
                SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
                format!("{} bytes exceed the {}-byte element", payload.len(), self.element_size()),
            ));
        }
        self.stack.push(payload).map_err(stack_code)?;
        self.consumer_waker.wake_one_up_to(WAKE_ANY);
        Ok(())
    }

    fn try_pop(&self, out: &mut [u8]) -> Result<usize, i32> {
        match self.stack.pop(out) {
            Some(n) => {
                self.producer_waker.wake_one_up_to(WAKE_ANY);
                Ok(n)
            }
            None => Err(SUBETHA_E_RING_EMPTY),
        }
    }

    fn push_wait(&self, payload: &[u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, &self.producer_waker, deadline, SUBETHA_E_RING_FULL, || self.try_push(payload))
    }

    fn pop_wait(&self, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        wait_until(&self.waiting, &self.consumer_waker, deadline, SUBETHA_E_RING_EMPTY, || self.try_pop(out))
    }

    fn peek(&self, out: &mut [u8]) -> Result<usize, i32> {
        match self.stack.peek(out) {
            Some(n) => Ok(n),
            None => Err(SUBETHA_E_RING_EMPTY),
        }
    }

    fn stats(&self) -> subetha_stack_stats {
        let layout = self.stack.layout();
        subetha_stack_stats {
            mode: self.mode,
            capacity: self.stack.capacity() as u64,
            element_size: layout.slot_size as u64,
            alignment: layout.alignment as u64,
            tag: layout.tag,
            approx_len: self.stack.approx_len() as u64,
            waker_full: self.waiting.waker_full(),
        }
    }
}

fn with_stack(handle: subetha_handle, f: impl FnOnce(&StackObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_STACK, |object| match object {
        Object::Stack(s) => f(s),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a stack"),
    })
}

/// The layout a caller passed, checked: non-null, a non-zero element
/// size that fits a `u32`, and a power-of-two alignment.
///
/// # Safety
/// `layout` is null or a valid pointer.
pub(crate) unsafe fn read_layout(layout: *const subetha_element_layout) -> Result<ElementLayout, i32> {
    if layout.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "layout is null"));
    }
    // SAFETY: checked non-null; the caller guarantees it is readable.
    let layout = unsafe { *layout };
    if layout.element_size == 0 || layout.element_size > u32::MAX as u64 {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("element_size {} is not between 1 and {}", layout.element_size, u32::MAX),
        ));
    }
    if layout.alignment == 0 || !layout.alignment.is_power_of_two() || layout.alignment > u32::MAX as u64 {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("alignment {} is not a power of two that fits 32 bits", layout.alignment),
        ));
    }
    Ok(ElementLayout {
        slot_size: layout.element_size as usize,
        alignment: layout.alignment as usize,
        tag: layout.tag,
    })
}

/// A stack capacity: at least one element, below the index the stack
/// uses for "none".
fn stack_capacity(capacity: u32) -> Result<usize, i32> {
    if capacity == 0 || capacity == STACK_NIL {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("capacity {capacity} is not between 1 and {}", STACK_NIL - 1),
        ));
    }
    Ok(capacity as usize)
}

/// The arguments every stack constructor reads, in order, so the first
/// refusal names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout`, `options` and `out`
/// are valid pointers.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, ElementLayout, subetha_ring_options), i32> {
    require_initialized()?;
    let options = unsafe { read_options(options, out) }?;
    let path = Path::new(unsafe { text(path, "path") }?);
    let capacity = stack_capacity(capacity)?;
    let layout = unsafe { read_layout(layout) }?;
    Ok((path, capacity, layout, options))
}

fn place(stack: Result<RawTreiberStack, StackError>, path: &Path, options: subetha_ring_options, out: *mut subetha_handle) -> i32 {
    let stack = match stack {
        Ok(s) => s,
        Err(e) => return stack_code(e),
    };
    match StackObject::build(stack, path, options) {
        Ok(object) => unsafe { issue(Object::Stack(object), out) },
        Err(code) => code,
    }
}

/// Obtain the stack at `path`: an empty one is initialized when the file
/// does not exist, and an existing one is attached with its entries in
/// place. A file built with another capacity or layout is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`. `capacity` is at least 1;
/// `scan_interval_us` in the options is ignored, the stack has no sidecar.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout`, `options` and `out`
/// are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_create(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, layout, options) = match unsafe { read_arguments(path, capacity, layout, options, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawTreiberStack::create(path, capacity, layout), path, options, out)
    })
}

/// Attach to the stack another process created at `path`; the file must
/// exist, which is what tells this apart from `subetha_stack_create`.
/// `SUBETHA_E_RING_IO` names an absent file and
/// `SUBETHA_E_RING_LAYOUT_MISMATCH` one of another shape.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout`, `options` and `out`
/// are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_open(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, layout, options) = match unsafe { read_arguments(path, capacity, layout, options, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawTreiberStack::open(path, capacity, layout), path, options, out)
    })
}

/// Truncate the file at `path` and initialize an empty stack there,
/// discarding every entry other handles share; for a caller that owns the
/// path. On Windows the file must not be mapped by any handle.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `layout`, `options` and `out`
/// are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_reset(
    path: *const c_char,
    capacity: u32,
    layout: *const subetha_element_layout,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, capacity, layout, options) = match unsafe { read_arguments(path, capacity, layout, options, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        place(RawTreiberStack::reset(path, capacity, layout), path, options, out)
    })
}

/// Push up to `element_size` bytes without waiting; the element is zero
/// past the payload. `SUBETHA_E_RING_FULL` when the stack is at capacity,
/// `SUBETHA_E_RING_PAYLOAD_TOO_LARGE` for more bytes than an element.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_try_push(handle: subetha_handle, data: *const u8, len: usize) -> i32 {
    with_stack(handle, |s| {
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

/// Push `count` elements of `len` bytes each, the first at `items` and
/// each next one `stride` bytes on, under one handle lookup and one panic
/// guard. The batch contract is `subetha_ring_try_push_many`'s.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_done` is a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_try_push_many(
    handle: subetha_handle,
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_stack(handle, |s| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_push_many(items, stride, len, count, out_done, |payload| match s.try_push(payload) {
                Ok(()) => SUBETHA_OK,
                Err(code) => code,
            })
        }
    })
}

/// Pop up to `count` elements, the first written at `out` and each next
/// one `stride` bytes on; `stride` is at least the element size. The batch
/// contract is `subetha_ring_try_pop_many`'s.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_try_pop_many(
    handle: subetha_handle,
    out: *mut u8,
    stride: usize,
    count: usize,
    out_done: *mut usize,
) -> i32 {
    with_stack(handle, |s| {
        // SAFETY: the caller guarantees the array and the count word.
        unsafe {
            run_pop_many(out, stride, s.element_size(), count, out_done, |buf| match s.try_pop(buf) {
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
pub unsafe extern "C" fn subetha_stack_push_wait(handle: subetha_handle, data: *const u8, len: usize, timeout_ms: i64) -> i32 {
    with_stack(handle, |s| {
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

/// Pop the top element, `element_size` bytes, into `out` without waiting;
/// `cap` must be at least that. `SUBETHA_E_RING_EMPTY` when there is
/// nothing to take.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_try_pop(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_stack(handle, |s| {
        let buf = match unsafe { out_buffer(out, cap, out_len, s.element_size()) } {
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

/// Pop, parking until an element arrives or `timeout_ms` elapses; the
/// waiting contract is `subetha_ring_pop_wait`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_pop_wait(
    handle: subetha_handle,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_stack(handle, |s| {
        let buf = match unsafe { out_buffer(out, cap, out_len, s.element_size()) } {
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

/// Copy the top element into `out` without popping it. The copy is a
/// snapshot: a pop on another thread can retire the element while it is
/// read. `SUBETHA_E_RING_EMPTY` when the stack is empty.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_peek(handle: subetha_handle, out: *mut u8, cap: usize, out_len: *mut usize) -> i32 {
    with_stack(handle, |s| {
        let buf = match unsafe { out_buffer(out, cap, out_len, s.element_size()) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match s.peek(buf) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// A snapshot of the stack into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_read_stats(handle: subetha_handle, out: *mut subetha_stack_stats) -> i32 {
    with_stack(handle, |s| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = s.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Write the stack's dirty pages to its file.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_stack_flush(handle: subetha_handle) -> i32 {
    with_stack(handle, |s| match s.stack.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => stack_code(e),
    })
}

/// Wake every thread parked in a wait on this stack; each re-checks the
/// stack and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_stack_wake_all(handle: subetha_handle) -> i32 {
    with_stack(handle, |s| {
        s.consumer_waker.wake_all();
        s.producer_waker.wake_all();
        SUBETHA_OK
    })
}

/// Remove the stack file at `path` and its two waker files. The contract
/// is `subetha_ring_unlink`'s.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `report` is null or a valid
/// pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_stack_unlink(path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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

    /// A stack file for one test under the temp directory, with its
    /// wakers; removed when the guard drops.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!("subetha-ffi-stack-{name}-{}.bin", std::process::id())))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let mut found = UnlinkReport::default();
            found.remove(self.0.clone());
            for suffix in [".cwaker.bin", ".pwaker.bin"] {
                found.remove(with_suffix(&self.0, suffix));
            }
            assert_eq!(found.failed, 0, "the stack's files were removed: {:?}", found.first_failure);
        }
    }

    fn layout(element_size: usize) -> ElementLayout {
        ElementLayout { slot_size: element_size, alignment: 8, tag: 7 }
    }

    fn build(path: &Path, capacity: usize, element_size: usize) -> StackObject {
        let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 4, scan_interval_us: 0, ..Default::default() };
        StackObject::build(RawTreiberStack::create(path, capacity, layout(element_size)).unwrap(), path, options).unwrap()
    }

    #[test]
    fn pushes_from_two_threads_all_arrive_and_a_pop_parks_until_one_does() {
        let scratch = Scratch::new("threads");
        let object = Arc::new(build(&scratch.0, 64, 8));
        assert_eq!(object.stats().element_size, 8);
        assert_eq!(object.stats().tag, 7);
        let mut out = [0u8; 8];
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
            assert_eq!(n, 8);
            seen[out[0] as usize] += 1;
        }
        for p in producers {
            p.join().unwrap();
        }
        assert_eq!(seen, [100, 100]);
        assert_eq!(object.stats().approx_len, 0);
        drop(object);
    }

    #[test]
    fn the_element_size_bounds_a_push_and_a_pop_buffer() {
        let scratch = Scratch::new("bounds");
        let object = build(&scratch.0, 2, 16);
        assert_eq!(object.try_push(&[1; 17]).unwrap_err(), SUBETHA_E_RING_PAYLOAD_TOO_LARGE);
        object.try_push(&[1; 3]).unwrap();
        object.try_push(&[2; 16]).unwrap();
        assert_eq!(object.try_push(&[3; 16]).unwrap_err(), SUBETHA_E_RING_FULL);
        let mut out = [0xFFu8; 16];
        assert_eq!(object.peek(&mut out).unwrap(), 16);
        assert_eq!(out, [2; 16]);
        assert_eq!(object.try_pop(&mut out).unwrap(), 16);
        assert_eq!(object.try_pop(&mut out).unwrap(), 16);
        assert_eq!(&out[..3], &[1; 3]);
        assert_eq!(&out[3..], &[0; 13]);
        assert_eq!(object.try_pop(&mut out).unwrap_err(), SUBETHA_E_RING_EMPTY);
        assert_eq!(object.peek(&mut out).unwrap_err(), SUBETHA_E_RING_EMPTY);
        drop(object);
    }

    #[test]
    fn a_layout_is_checked_before_a_file_is_touched() {
        let bad = subetha_element_layout { element_size: 0, alignment: 8, tag: 0 };
        assert_eq!(unsafe { read_layout(&bad) }.unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        let bad = subetha_element_layout { element_size: 8, alignment: 3, tag: 0 };
        assert_eq!(unsafe { read_layout(&bad) }.unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(unsafe { read_layout(std::ptr::null()) }.unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        let good = subetha_element_layout { element_size: 24, alignment: 8, tag: 9 };
        assert_eq!(unsafe { read_layout(&good) }.unwrap(), ElementLayout { slot_size: 24, alignment: 8, tag: 9 });
        assert_eq!(stack_capacity(0).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(stack_capacity(u32::MAX).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(stack_capacity(1).unwrap(), 1);
    }
}
