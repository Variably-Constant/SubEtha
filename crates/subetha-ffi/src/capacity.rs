//! The capacity-adaptive ring through the C ABI: an adaptive ring whose
//! slot count changes at run time. A morph builds a fresh backing at the
//! target, moves the producers to it, and leaves the old backing on a
//! stale list the consumer drains first, so nothing in flight is lost and
//! nothing is copied. The compound morph changes shape, capacity and
//! locale in one transition; a prewarm builds the next backing early so
//! the morph pays only the swap.
//!
//! Strict mode morphs when the caller says so. Managed mode attaches a
//! capacity sidecar that scans at the caller's interval and grows or
//! shrinks by fill ratio with hysteresis. The ABI keeps a consumer waker
//! and a producer waker beside the ring for the waiting forms.
//!
//! In the file locale the backings are `<base>.cap_<N>.bin` and, after a
//! morph, `<base>.cap_<N>_g<seq>.bin`, each an adaptive-ring prefix of its
//! own; the wakers are `<base>.cwaker.bin` and `<base>.pwaker.bin`. In
//! shared memory the backings are `{prefix}_cap_<N>` and the wakers
//! `{prefix}_cwaker` and `{prefix}_pwaker`, in the session namespace.

use std::ffi::c_char;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use subetha_cxc::adaptive_ring::{RingShape, UnlinkReport};
use subetha_cxc::capacity_adaptive_ring::{
    BackingTarget, CapacityAdaptiveRing, CapacityAdaptiveRingSidecar, DefaultCapacityPolicy, RingConfig,
};
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::shm_file::ShmNamespace;

use crate::error::{
    adaptive_code, capacity_code, fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_EMPTY,
    SUBETHA_E_RING_FULL, SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_CAPACITY_RING};
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, ordering_constant, ordering_mode, out_buffer,
    read_options, subetha_ring_options, subetha_unlink_report, text, wakers_for, with_suffix, Locale,
    SUBETHA_RING_SHAPE_MPMC, SUBETHA_RING_SHAPE_MPSC, SUBETHA_RING_SHAPE_SPSC, SUBETHA_RING_SHAPE_VYUKOV,
    SUBETHA_RING_SLOT_BYTES, SUBETHA_STAMPS_DEFAULT, SUBETHA_STAMPS_NONE,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind, SUBETHA_MODE_MANAGED};
use crate::wait::{wait_until, Waiting, WAKE_ANY};

/// In a compound morph, keep the current value of that axis.
pub const SUBETHA_KEEP: u32 = u32::MAX;

/// Morph target locale: keep the current one.
pub const SUBETHA_TARGET_KEEP: u32 = 0;
/// Morph target locale: anonymous memory.
pub const SUBETHA_TARGET_ANON: u32 = 1;
/// Morph target locale: files under the base path in `target_name`.
pub const SUBETHA_TARGET_FILE: u32 = 2;
/// Morph target locale: shared memory under the prefix in `target_name`.
pub const SUBETHA_TARGET_SHM: u32 = 3;

/// A snapshot of a capacity-adaptive ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_capacity_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// The active backing's shape, one of the `SUBETHA_RING_SHAPE_` constants.
    pub shape: u32,
    /// The live ordering mode, one of the `SUBETHA_ORDERING_` constants.
    pub ordering_mode: u32,
    /// Capacity of the backing the producers write to.
    pub current_capacity: u64,
    /// Bumped by every morph; a pin taken at an older value is stale.
    pub pin_generation: u64,
    /// Items in the active backing, read without a lock.
    pub approx_len: u64,
    /// Capacity of the backing held in the warm cache; zero when none.
    pub warm_capacity: u64,
    /// Morphs that consumed a warm backing instead of building one.
    pub warm_hits: u64,
    /// Items the consumer took from stale backings since creation.
    pub stale_pops: u64,
    /// Cross-producer inversions observed; zero on an unstamped ring.
    pub inversions: u64,
    /// Morphs the managed-mode sidecar issued; zero in strict mode.
    pub sidecar_morphs: u64,
    /// Prewarms the managed-mode sidecar issued; zero in strict mode.
    pub sidecar_prewarms: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
    /// Whether the backings carry ordering stamps.
    pub stamped: bool,
}

pub(crate) struct CapacityObject {
    ring: Arc<CapacityAdaptiveRing>,
    consumer_waker: CrossProcessWaker,
    producer_waker: CrossProcessWaker,
    sidecar: Mutex<Option<CapacityAdaptiveRingSidecar>>,
    mode: u32,
    waiting: Waiting,
}

/// The options a capacity ring accepts: any mode, the default stamp
/// source or none, and no contract, since the ring declares none.
fn checked_options(options: &subetha_ring_options) -> Result<(u32, bool), i32> {
    let mode = resolve_mode(options.mode)?;
    let stamped = match options.stamps {
        SUBETHA_STAMPS_NONE => false,
        SUBETHA_STAMPS_DEFAULT => true,
        other => {
            return Err(fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("stamps {other}: a capacity ring stamps from the host's default source or not at all"),
            ))
        }
    };
    let c = &options.contract;
    if c.max_producers != 0 || c.max_consumers != 0 || c.ordering != 0 || c.capacity_bound != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a capacity ring declares no contract"));
    }
    if options.frame_block != 0 || options.frame_blocks != 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a capacity ring takes no frame region geometry"));
    }
    if !options.shm_sddl.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "a capacity ring's regions take the platform's default descriptor; shm_sddl must be null"));
    }
    if mode == SUBETHA_MODE_MANAGED && options.scan_interval_us == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "managed mode needs scan_interval_us; the library names no default"));
    }
    Ok((mode, stamped))
}

impl CapacityObject {
    fn build(ring: CapacityAdaptiveRing, locale: &Locale<'_>, options: &subetha_ring_options, mode: u32) -> Result<Self, i32> {
        let (consumer_waker, producer_waker) = wakers_for(locale, options.max_waiters)?;
        let ring = Arc::new(ring);
        let sidecar = if mode == SUBETHA_MODE_MANAGED {
            Some(CapacityAdaptiveRingSidecar::spawn(
                Arc::clone(&ring),
                DefaultCapacityPolicy::default(),
                Duration::from_micros(options.scan_interval_us),
            ))
        } else {
            None
        };
        Ok(Self {
            ring,
            consumer_waker,
            producer_waker,
            sidecar: Mutex::new(sidecar),
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

    fn try_push(&self, producer_id: usize, payload: &[u8]) -> Result<(), i32> {
        self.ring.try_send(producer_id, payload).map_err(ring_code)?;
        self.consumer_waker.wake_one_up_to(WAKE_ANY);
        Ok(())
    }

    fn try_pop(&self, consumer_id: usize, out: &mut [u8]) -> Result<usize, i32> {
        let n = self.ring.try_recv(consumer_id, out).map_err(ring_code)?;
        self.producer_waker.wake_one_up_to(WAKE_ANY);
        Ok(n)
    }

    fn push_wait(&self, producer_id: usize, payload: &[u8], deadline: Option<Instant>) -> Result<(), i32> {
        wait_until(&self.waiting, &self.producer_waker, deadline, SUBETHA_E_RING_FULL, || {
            self.try_push(producer_id, payload)
        })
    }

    fn pop_wait(&self, consumer_id: usize, out: &mut [u8], deadline: Option<Instant>) -> Result<usize, i32> {
        wait_until(&self.waiting, &self.consumer_waker, deadline, SUBETHA_E_RING_EMPTY, || {
            self.try_pop(consumer_id, out)
        })
    }

    fn stats(&self) -> subetha_capacity_stats {
        let active = self.ring.ring_handle();
        let shape = match active.current_shape() {
            RingShape::Spsc => SUBETHA_RING_SHAPE_SPSC,
            RingShape::Mpsc => SUBETHA_RING_SHAPE_MPSC,
            RingShape::Mpmc => SUBETHA_RING_SHAPE_MPMC,
            RingShape::Vyukov => SUBETHA_RING_SHAPE_VYUKOV,
        };
        let (sidecar_morphs, sidecar_prewarms) = self
            .sidecar
            .lock()
            .as_ref()
            .map_or((0, 0), |s| (s.morphs_triggered(), s.prewarms_issued()));
        subetha_capacity_stats {
            mode: self.mode,
            shape,
            ordering_mode: self.ring.ordering_mode().map_or(0, ordering_constant),
            current_capacity: self.ring.current_capacity() as u64,
            pin_generation: self.ring.pin_generation(),
            approx_len: active.approx_len() as u64,
            warm_capacity: self.ring.warm_capacity().map_or(0, |c| c as u64),
            warm_hits: self.ring.warm_hits(),
            stale_pops: self.ring.stale_pops(),
            inversions: self.ring.inversions(),
            sidecar_morphs,
            sidecar_prewarms,
            waker_full: self.waiting.waker_full(),
            stamped: self.ring.is_stamped(),
        }
    }
}

impl Drop for CapacityObject {
    fn drop(&mut self) {
        if let Some(sidecar) = self.sidecar.lock().take() {
            sidecar.shutdown();
        }
    }
}

fn with_capacity(handle: subetha_handle, f: impl FnOnce(&CapacityObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_CAPACITY_RING, |object| match object {
        Object::Capacity(c) => f(c),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a capacity-adaptive ring"),
    })
}

fn counts(max_producers: u32, max_consumers: u32) -> Result<(usize, usize), i32> {
    if max_producers == 0 || max_consumers == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_producers and max_consumers must be at least 1"));
    }
    Ok((max_producers as usize, max_consumers as usize))
}

/// Create a capacity-adaptive ring in anonymous memory.
///
/// # Safety
/// `options` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_create_anon(
    max_producers: u32,
    max_consumers: u32,
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
        let (mode, stamped) = match checked_options(&options) {
            Ok(v) => v,
            Err(code) => return code,
        };
        let (mp, mc) = match counts(max_producers, max_consumers) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let cap = match checked_capacity(initial_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = if stamped {
            CapacityAdaptiveRing::create_anon_stamped(mp, mc, cap)
        } else {
            CapacityAdaptiveRing::create_anon(mp, mc, cap)
        };
        let ring = match ring {
            Ok(r) => r,
            Err(e) => return capacity_code(e),
        };
        match CapacityObject::build(ring, &Locale::Anon, &options, mode) {
            Ok(object) => unsafe { issue(Object::Capacity(object), out) },
            Err(code) => code,
        }
    })
}

/// Create a file-backed capacity-adaptive ring under `base_path`, or
/// attach to one that exists there: the active backing is
/// `<base_path>.cap_<initial_capacity>.bin` with the adaptive ring's own
/// files under it, and the wakers `<base_path>.cwaker.bin` and
/// `<base_path>.pwaker.bin` are created or attached.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_create(
    base_path: *const c_char,
    max_producers: u32,
    max_consumers: u32,
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
        let (mode, stamped) = match checked_options(&options) {
            Ok(v) => v,
            Err(code) => return code,
        };
        let (mp, mc) = match counts(max_producers, max_consumers) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let cap = match checked_capacity(initial_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = if stamped {
            CapacityAdaptiveRing::create_stamped(base, mp, mc, cap)
        } else {
            CapacityAdaptiveRing::create(base, mp, mc, cap)
        };
        let ring = match ring {
            Ok(r) => r,
            Err(e) => return capacity_code(e),
        };
        match CapacityObject::build(ring, &Locale::File(base), &options, mode) {
            Ok(object) => unsafe { issue(Object::Capacity(object), out) },
            Err(code) => code,
        }
    })
}

/// Attach to a file-backed capacity-adaptive ring another handle created
/// under `base_path`, with the counts and initial capacity it was created
/// with; the backing is opened, not re-initialized, and the wakers are
/// attached. This handle's morph state starts at the initial capacity. A
/// capacity other than the creator's names a backing of its own, which is
/// absent and refused as `SUBETHA_E_RING_IO`.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_open(
    base_path: *const c_char,
    max_producers: u32,
    max_consumers: u32,
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
        let (mode, stamped) = match checked_options(&options) {
            Ok(v) => v,
            Err(code) => return code,
        };
        let (mp, mc) = match counts(max_producers, max_consumers) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let cap = match checked_capacity(initial_capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = match CapacityAdaptiveRing::open(base, mp, mc, cap, stamped) {
            Ok(r) => r,
            Err(e) => return capacity_code(e),
        };
        match CapacityObject::build(ring, &Locale::File(base), &options, mode) {
            Ok(object) => unsafe { issue(Object::Capacity(object), out) },
            Err(code) => code,
        }
    })
}

/// Build a shared-memory capacity ring under `name_prefix` in the session
/// namespace, initializing the wakers when `create` is true and attaching
/// to them when false.
///
/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
unsafe fn shm_ring(
    name_prefix: *const c_char,
    max_producers: u32,
    max_consumers: u32,
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
    let (mode, stamped) = match checked_options(&options) {
        Ok(v) => v,
        Err(code) => return code,
    };
    let (mp, mc) = match counts(max_producers, max_consumers) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let cap = match checked_capacity(initial_capacity) {
        Ok(c) => c,
        Err(code) => return code,
    };
    let ring = if stamped {
        CapacityAdaptiveRing::create_shmfs_stamped(prefix, mp, mc, cap)
    } else {
        CapacityAdaptiveRing::create_shmfs(prefix, mp, mc, cap)
    };
    let ring = match ring {
        Ok(r) => r,
        Err(e) => return capacity_code(e),
    };
    let locale = Locale::Shm { name: prefix, namespace: ShmNamespace::Session, create, sddl: None };
    match CapacityObject::build(ring, &locale, &options, mode) {
        Ok(object) => unsafe { issue(Object::Capacity(object), out) },
        Err(code) => code,
    }
}

/// Create a capacity-adaptive ring in named shared memory under
/// `name_prefix`, in the session namespace: the active backing is
/// `{name_prefix}_cap_<initial_capacity>` and the wakers `{name_prefix}_cwaker`
/// and `{name_prefix}_pwaker`.
///
/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_create_shm(
    name_prefix: *const c_char,
    max_producers: u32,
    max_consumers: u32,
    initial_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| unsafe { shm_ring(name_prefix, max_producers, max_consumers, initial_capacity, options, out, true) })
}

/// Attach to a capacity-adaptive ring another process created in named
/// shared memory under `name_prefix`, with the counts and capacity it was
/// created with; the wakers are attached, not reset.
///
/// # Safety
/// `name_prefix` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_open_shm(
    name_prefix: *const c_char,
    max_producers: u32,
    max_consumers: u32,
    initial_capacity: u32,
    options: *const subetha_ring_options,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| unsafe { shm_ring(name_prefix, max_producers, max_consumers, initial_capacity, options, out, false) })
}

/// Register as a producer on the active backing; every morph mirrors the
/// registration onto the next backing.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_register_producer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_capacity(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match c.ring.register_producer() {
            Ok(id) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = id as u32 };
                SUBETHA_OK
            }
            Err(e) => adaptive_code(e),
        }
    })
}

/// Register as a consumer on the active backing.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_register_consumer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_capacity(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match c.ring.register_consumer() {
            Ok(id) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = id as u32 };
                SUBETHA_OK
            }
            Err(e) => adaptive_code(e),
        }
    })
}

/// Push into the active backing without waiting; the slot semantics are
/// `subetha_ring_try_push`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_try_push(handle: subetha_handle, producer_id: u32, data: *const u8, len: usize) -> i32 {
    with_capacity(handle, |c| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match c.try_push(producer_id as usize, payload) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Pop without waiting, draining stale backings before the active one so
/// items keep their order across a morph; the slot semantics are
/// `subetha_ring_try_pop`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_try_pop(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_capacity(handle, |c| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match c.try_pop(consumer_id as usize, buf) {
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
pub unsafe extern "C" fn subetha_capacity_push_wait(
    handle: subetha_handle,
    producer_id: u32,
    data: *const u8,
    len: usize,
    timeout_ms: i64,
) -> i32 {
    with_capacity(handle, |c| {
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
        match c.push_wait(producer_id as usize, payload, deadline) {
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
pub unsafe extern "C" fn subetha_capacity_pop_wait(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_capacity(handle, |c| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match c.pop_wait(consumer_id as usize, buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Morph the ring's capacity to `new_capacity`, a power of two of at least
/// 2. Producers move to the fresh backing; the consumer drains the old one
/// first. Concurrent morphs serialize; the data path is never blocked.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_morph_capacity(handle: subetha_handle, new_capacity: u32) -> i32 {
    with_capacity(handle, |c| match c.ring.morph_capacity_to(new_capacity as usize) {
        Ok(()) => SUBETHA_OK,
        Err(e) => capacity_code(e),
    })
}

/// The compound morph target a call describes.
///
/// # Safety
/// `target_name` is null or a NUL-terminated UTF-8 string.
unsafe fn config_from(shape: u32, capacity: u32, target_kind: u32, target_name: *const c_char) -> Result<RingConfig, i32> {
    let shape = match shape {
        SUBETHA_KEEP => None,
        SUBETHA_RING_SHAPE_SPSC => Some(RingShape::Spsc),
        SUBETHA_RING_SHAPE_MPSC => Some(RingShape::Mpsc),
        SUBETHA_RING_SHAPE_MPMC => Some(RingShape::Mpmc),
        SUBETHA_RING_SHAPE_VYUKOV => Some(RingShape::Vyukov),
        other => return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("shape {other} is not a shape"))),
    };
    let capacity = if capacity == 0 { None } else { Some(checked_capacity(capacity)?) };
    let locale = match target_kind {
        SUBETHA_TARGET_KEEP => None,
        SUBETHA_TARGET_ANON => Some(BackingTarget::Anon),
        SUBETHA_TARGET_FILE => Some(BackingTarget::File(PathBuf::from(unsafe { text(target_name, "target_name") }?))),
        SUBETHA_TARGET_SHM => Some(BackingTarget::Shm(unsafe { text(target_name, "target_name") }?.to_owned())),
        other => return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("target kind {other} is not a target"))),
    };
    Ok(RingConfig { shape, capacity, locale })
}

/// Change any of shape, capacity and locale in one transition: `shape` is
/// a `SUBETHA_RING_SHAPE_` constant or `SUBETHA_KEEP`, `capacity` a power
/// of two or zero to keep, `target_kind` a `SUBETHA_TARGET_` constant with
/// `target_name` the base path or shared-memory prefix it needs. A new
/// locale stays the locale of every later morph and prewarm. A change of
/// shape alone morphs the active backing in place.
///
/// # Safety
/// `target_name` is null or a NUL-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_morph(
    handle: subetha_handle,
    shape: u32,
    capacity: u32,
    target_kind: u32,
    target_name: *const c_char,
) -> i32 {
    with_capacity(handle, |c| {
        let config = match unsafe { config_from(shape, capacity, target_kind, target_name) } {
            Ok(cfg) => cfg,
            Err(code) => return code,
        };
        match c.ring.morph_to_config(&config) {
            Ok(()) => SUBETHA_OK,
            Err(e) => capacity_code(e),
        }
    })
}

/// Build the backing a coming morph to `capacity` needs, off the data
/// path, so that morph pays only the swap. One backing is held; a prewarm
/// at another capacity replaces it.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_prewarm(handle: subetha_handle, capacity: u32) -> i32 {
    with_capacity(handle, |c| match c.ring.prewarm(capacity as usize) {
        Ok(()) => SUBETHA_OK,
        Err(e) => capacity_code(e),
    })
}

/// Drop the warm backing, releasing its memory or files.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_clear_warm(handle: subetha_handle) -> i32 {
    with_capacity(handle, |c| {
        c.ring.clear_warm();
        SUBETHA_OK
    })
}

/// Set the ordering mode of a stamped capacity ring across the active
/// backing and every stale one still draining; the contract is
/// `subetha_ring_set_ordering_mode`'s.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_set_ordering_mode(handle: subetha_handle, mode: u32) -> i32 {
    with_capacity(handle, |c| {
        let mode = match ordering_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        match c.ring.set_ordering_mode(mode) {
            Ok(()) => SUBETHA_OK,
            Err(e) => ring_code(e),
        }
    })
}

/// A snapshot of the ring's state into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_read_stats(handle: subetha_handle, out: *mut subetha_capacity_stats) -> i32 {
    with_capacity(handle, |c| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = c.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Wake every thread parked in a wait on this ring; each re-checks the
/// ring and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_capacity_wake_all(handle: subetha_handle) -> i32 {
    with_capacity(handle, |c| {
        c.consumer_waker.wake_all();
        c.producer_waker.wake_all();
        SUBETHA_OK
    })
}

/// Remove every file under `base_path`'s directory whose name starts with
/// `<base name>.cap_`, which is every backing the ring ever built there,
/// plus the two waker files. The contract is `subetha_ring_unlink`'s.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `report` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_capacity_unlink(base_path: *const c_char, report: *mut subetha_unlink_report) -> i32 {
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

/// Remove every file beside `base` whose name starts with the base name
/// followed by `.cap_`.
pub(crate) fn remove_backings(base: &Path, found: &mut UnlinkReport) -> Result<(), i32> {
    let dir = match base.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let Some(stem) = base.file_name() else {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "base_path has no file name"));
    };
    let mut prefix = stem.to_owned();
    prefix.push(".cap_");
    let prefix = prefix.to_string_lossy().into_owned();
    let entries = std::fs::read_dir(&dir).map_err(|e| {
        fail(crate::error::SUBETHA_E_RING_IO, format!("{} cannot be listed: {e}", dir.display()))
    })?;
    let mut matched: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| fail(crate::error::SUBETHA_E_RING_IO, format!("{} cannot be listed: {e}", dir.display())))?;
        if entry.file_name().to_string_lossy().starts_with(&prefix) {
            matched.push(entry.path());
        }
    }
    matched.sort();
    for path in matched {
        found.remove(path);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;

    fn strict() -> subetha_ring_options {
        subetha_ring_options {
            mode: SUBETHA_MODE_STRICT,
            max_waiters: 0,
            scan_interval_us: 0,
            stamps: SUBETHA_STAMPS_NONE,
            ..Default::default()
        }
    }

    #[test]
    fn a_morph_keeps_every_item_in_flight_and_in_order() {
        let options = strict();
        let ring = CapacityAdaptiveRing::create_anon(1, 1, 4).unwrap();
        let object = CapacityObject::build(ring, &Locale::Anon, &options, SUBETHA_MODE_STRICT).unwrap();
        let pid = object.ring.register_producer().unwrap();
        let cid = object.ring.register_consumer().unwrap();
        for i in 0..4u8 {
            object.try_push(pid, &[i]).unwrap();
        }
        assert_eq!(object.try_push(pid, &[9]).unwrap_err(), SUBETHA_E_RING_FULL);
        object.ring.morph_capacity_to(16).unwrap();
        assert_eq!(object.stats().current_capacity, 16);
        assert_eq!(object.stats().pin_generation, 1);
        for i in 4..10u8 {
            object.try_push(pid, &[i]).unwrap();
        }
        let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
        for i in 0..10u8 {
            object.try_pop(cid, &mut out).unwrap();
            assert_eq!(out[0], i, "the stale backing drains before the active one");
        }
        assert_eq!(object.try_pop(cid, &mut out).unwrap_err(), SUBETHA_E_RING_EMPTY);
        assert_eq!(object.stats().stale_pops, 4);
    }

    #[test]
    fn the_options_a_capacity_ring_refuses_are_named() {
        let mut options = strict();
        options.stamps = crate::ring::SUBETHA_STAMPS_TSC;
        assert_eq!(checked_options(&options).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        let mut options = strict();
        options.contract.max_producers = 2;
        assert_eq!(checked_options(&options).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        let mut options = strict();
        options.mode = SUBETHA_MODE_MANAGED;
        assert_eq!(checked_options(&options).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        options.scan_interval_us = 1000;
        assert_eq!(checked_options(&options).unwrap(), (SUBETHA_MODE_MANAGED, false));
    }
}
