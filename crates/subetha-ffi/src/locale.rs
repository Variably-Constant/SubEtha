//! The locale-adaptive ring through the C ABI: three adaptive rings, one
//! in anonymous memory, one in files, one in shared memory, all built at
//! creation, with one live at a time. A migration moves the items in
//! flight to the target and flips a tag every attached process reads, so
//! the same ring can start in-process and move cross-process while it runs.
//!
//! Strict mode migrates when the caller says so. Managed mode attaches a
//! locale sidecar that scans at the caller's interval and migrates to the
//! locale the caller requested once the hysteresis has elapsed. The ABI
//! keeps a consumer waker and a producer waker beside the ring, in files
//! under the base path, for the waiting forms.
//!
//! The files are the Rust API's own: `<base>.locale.tag.bin`,
//! `<base>.locale.gen.bin`, the file backing under
//! `<base>.locale.file.ring`, the shared-memory backing under a prefix
//! derived from the base path, and the wakers `<base>.cwaker.bin` and
//! `<base>.pwaker.bin`.

use std::ffi::c_char;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::cross_process_waker::CrossProcessWaker;
use subetha_cxc::locale_adaptive_ring::{
    DefaultLocalePolicy, Locale as RingLocale, LocaleAdaptiveRing, LocaleAdaptiveRingSidecar,
};

use crate::error::{
    adaptive_code, fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_NOT_SUPPORTED, SUBETHA_E_RING_EMPTY,
    SUBETHA_E_RING_FULL, SUBETHA_E_RING_PAYLOAD_TOO_LARGE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_LOCALE_RING};
use crate::ring::{
    bytes, checked_capacity, deadline_from, finish_unlink, ordering_constant, ordering_mode, out_buffer,
    read_options, subetha_ring_options, subetha_unlink_report, text, wakers_for, with_suffix, Locale,
    SUBETHA_RING_SLOT_BYTES, SUBETHA_STAMPS_DEFAULT, SUBETHA_STAMPS_NONE,
};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind, SUBETHA_MODE_MANAGED};
use crate::wait::{wait_until, Waiting, WAKE_ANY};

/// Locale: anonymous memory, this process only.
pub const SUBETHA_LOCALE_ANON: u32 = 0;
/// Locale: files, page-cached and persistent.
pub const SUBETHA_LOCALE_FILE: u32 = 1;
/// Locale: named shared memory, RAM-resident.
pub const SUBETHA_LOCALE_SHM: u32 = 2;

/// A snapshot of a locale-adaptive ring.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_locale_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// The live locale, one of the `SUBETHA_LOCALE_` constants.
    pub current_locale: u32,
    /// The live ordering mode, one of the `SUBETHA_ORDERING_` constants.
    pub ordering_mode: u32,
    /// Bumped by every migration; shared across processes.
    pub locale_generation: u64,
    /// Items in the live backing, read without a lock.
    pub approx_len: u64,
    /// Cross-producer inversions across the three backings; zero unstamped.
    pub inversions: u64,
    /// Migrations the managed-mode sidecar issued; zero in strict mode.
    pub sidecar_migrations: u64,
    /// Waits refused because every waiter slot was in use.
    pub waker_full: u64,
    /// Whether the backings carry ordering stamps.
    pub stamped: bool,
}

pub(crate) struct LocaleObject {
    ring: Arc<LocaleAdaptiveRing>,
    consumer_waker: CrossProcessWaker,
    producer_waker: CrossProcessWaker,
    sidecar: Mutex<Option<LocaleAdaptiveRingSidecar>>,
    mode: u32,
    waiting: Waiting,
}

pub(crate) fn locale_constant(locale: RingLocale) -> u32 {
    match locale {
        RingLocale::Anon => SUBETHA_LOCALE_ANON,
        RingLocale::File => SUBETHA_LOCALE_FILE,
        RingLocale::ShmFs => SUBETHA_LOCALE_SHM,
    }
}

fn locale_from(locale: u32) -> Result<RingLocale, i32> {
    match locale {
        SUBETHA_LOCALE_ANON => Ok(RingLocale::Anon),
        SUBETHA_LOCALE_FILE => Ok(RingLocale::File),
        SUBETHA_LOCALE_SHM => Ok(RingLocale::ShmFs),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("locale {other} is not a locale"))),
    }
}

impl LocaleObject {
    fn build(ring: LocaleAdaptiveRing, base: &Path, options: &subetha_ring_options, mode: u32) -> Result<Self, i32> {
        let (consumer_waker, producer_waker) = wakers_for(&Locale::File(base), options.max_waiters)?;
        let ring = Arc::new(ring);
        let sidecar = if mode == SUBETHA_MODE_MANAGED {
            Some(LocaleAdaptiveRingSidecar::spawn(
                Arc::clone(&ring),
                DefaultLocalePolicy::default(),
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

    fn live(&self) -> &AdaptiveRing {
        match self.ring.current_locale() {
            RingLocale::Anon => self.ring.anon_ring(),
            RingLocale::File => self.ring.file_ring(),
            RingLocale::ShmFs => self.ring.shmfs_ring(),
        }
    }

    fn stats(&self) -> subetha_locale_stats {
        let sidecar_migrations = self.sidecar.lock().as_ref().map_or(0, |s| s.migrations_triggered());
        subetha_locale_stats {
            mode: self.mode,
            current_locale: locale_constant(self.ring.current_locale()),
            ordering_mode: self.ring.ordering_mode().map_or(0, ordering_constant),
            locale_generation: self.ring.locale_generation(),
            approx_len: self.live().approx_len() as u64,
            inversions: self.ring.inversions(),
            sidecar_migrations,
            waker_full: self.waiting.waker_full(),
            stamped: self.ring.is_stamped(),
        }
    }
}

impl Drop for LocaleObject {
    fn drop(&mut self) {
        if let Some(sidecar) = self.sidecar.lock().take() {
            sidecar.shutdown();
        }
    }
}

fn with_locale(handle: subetha_handle, f: impl FnOnce(&LocaleObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_LOCALE_RING, |object| match object {
        Object::LocaleRing(l) => f(l),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a locale-adaptive ring"),
    })
}

/// Borrow the ring a handle names, for an endpoint that binds it as a
/// local target. The same refusal as any other call on the wrong kind.
pub(crate) fn with_locale_ring(
    handle: subetha_handle,
    f: impl FnOnce(Arc<LocaleAdaptiveRing>) -> i32,
) -> i32 {
    with_locale(handle, |l| f(Arc::clone(&l.ring)))
}

/// Create a locale-adaptive ring under `base_path`, or attach to one that
/// exists there. The ring starts in anonymous memory. Stamps come from the
/// host's default source or not at all, and the ring declares no
/// contract.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_locale_ring_create(
    base_path: *const c_char,
    max_producers: u32,
    max_consumers: u32,
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
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mode = match resolve_mode(options.mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let stamped = match options.stamps {
            SUBETHA_STAMPS_NONE => false,
            SUBETHA_STAMPS_DEFAULT => true,
            other => {
                return fail(
                    SUBETHA_E_INVALID_ARGUMENT,
                    format!("stamps {other}: a locale ring stamps from the host's default source or not at all"),
                )
            }
        };
        let c = &options.contract;
        if c.max_producers != 0 || c.max_consumers != 0 || c.ordering != 0 || c.capacity_bound != 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a locale ring declares no contract");
        }
        if options.frame_block != 0 || options.frame_blocks != 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a locale ring takes no frame region geometry");
        }
        if !options.shm_sddl.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a locale ring's regions take the platform's default descriptor; shm_sddl must be null");
        }
        if mode == SUBETHA_MODE_MANAGED && options.scan_interval_us == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "managed mode needs scan_interval_us; the library names no default");
        }
        if max_producers == 0 || max_consumers == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "max_producers and max_consumers must be at least 1");
        }
        let cap = match checked_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let (mp, mc) = (max_producers as usize, max_consumers as usize);
        let ring = if stamped {
            LocaleAdaptiveRing::create_with_ordering_stamps(base, mp, mc, cap)
        } else {
            LocaleAdaptiveRing::create(base, mp, mc, cap)
        };
        let ring = match ring {
            Ok(r) => r,
            Err(e) => return ring_code(e),
        };
        match LocaleObject::build(ring, base, &options, mode) {
            Ok(object) => unsafe { issue(Object::LocaleRing(object), out) },
            Err(code) => code,
        }
    })
}

/// Attach to the locale ring another process created under `base_path`
/// with the same counts and capacity, without re-initializing any
/// backing; the locale tag, the generation and the file and shared-memory
/// backings are the creator's, and this handle's anonymous backing is its
/// own. `stamps` in the options must match the creator's choice.
/// `SUBETHA_E_RING_IO` names an absent backing and
/// `SUBETHA_E_RING_LAYOUT_MISMATCH` one of another shape.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `options` and `out` are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_locale_ring_open(
    base_path: *const c_char,
    max_producers: u32,
    max_consumers: u32,
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
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mode = match resolve_mode(options.mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let stamped = match options.stamps {
            SUBETHA_STAMPS_NONE => false,
            SUBETHA_STAMPS_DEFAULT => true,
            other => {
                return fail(
                    SUBETHA_E_INVALID_ARGUMENT,
                    format!("stamps {other}: a locale ring stamps from the host's default source or not at all"),
                )
            }
        };
        let c = &options.contract;
        if c.max_producers != 0 || c.max_consumers != 0 || c.ordering != 0 || c.capacity_bound != 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a locale ring declares no contract");
        }
        if options.frame_block != 0 || options.frame_blocks != 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a locale ring takes no frame region geometry");
        }
        if !options.shm_sddl.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a locale ring's regions take the platform's default descriptor; shm_sddl must be null");
        }
        if mode == SUBETHA_MODE_MANAGED && options.scan_interval_us == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "managed mode needs scan_interval_us; the library names no default");
        }
        if max_producers == 0 || max_consumers == 0 {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "max_producers and max_consumers must be at least 1");
        }
        let cap = match checked_capacity(capacity) {
            Ok(c) => c,
            Err(code) => return code,
        };
        let ring = match LocaleAdaptiveRing::open(base, max_producers as usize, max_consumers as usize, cap, stamped) {
            Ok(r) => r,
            Err(e) => return ring_code(e),
        };
        match LocaleObject::build(ring, base, &options, mode) {
            Ok(object) => unsafe { issue(Object::LocaleRing(object), out) },
            Err(code) => code,
        }
    })
}

/// Register as a producer on all three backings, so the live one always
/// has the registration.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_locale_ring_register_producer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_locale(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match l.ring.register_producer() {
            Ok(id) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = id as u32 };
                SUBETHA_OK
            }
            Err(e) => adaptive_code(e),
        }
    })
}

/// Register as a consumer on all three backings.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_locale_ring_register_consumer(handle: subetha_handle, out: *mut u32) -> i32 {
    with_locale(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match l.ring.register_consumer() {
            Ok(id) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = id as u32 };
                SUBETHA_OK
            }
            Err(e) => adaptive_code(e),
        }
    })
}

/// Push into the live locale without waiting; the slot semantics are
/// `subetha_ring_try_push`'s.
///
/// # Safety
/// `data` points to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_locale_ring_try_push(handle: subetha_handle, producer_id: u32, data: *const u8, len: usize) -> i32 {
    with_locale(handle, |l| {
        if len > SUBETHA_RING_SLOT_BYTES {
            return fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{len} bytes exceed the {SUBETHA_RING_SLOT_BYTES}-byte slot"));
        }
        let payload = match unsafe { bytes(data, len) } {
            Ok(p) => p,
            Err(code) => return code,
        };
        match l.try_push(producer_id as usize, payload) {
            Ok(()) => SUBETHA_OK,
            Err(code) => code,
        }
    })
}

/// Pop from the live locale without waiting; the slot semantics are
/// `subetha_ring_try_pop`'s.
///
/// # Safety
/// `out` points to `cap` writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_locale_ring_try_pop(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_locale(handle, |l| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        match l.try_pop(consumer_id as usize, buf) {
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
pub unsafe extern "C" fn subetha_locale_ring_push_wait(
    handle: subetha_handle,
    producer_id: u32,
    data: *const u8,
    len: usize,
    timeout_ms: i64,
) -> i32 {
    with_locale(handle, |l| {
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
        match l.push_wait(producer_id as usize, payload, deadline) {
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
pub unsafe extern "C" fn subetha_locale_ring_pop_wait(
    handle: subetha_handle,
    consumer_id: u32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    timeout_ms: i64,
) -> i32 {
    with_locale(handle, |l| {
        let buf = match unsafe { out_buffer(out, cap, out_len, SUBETHA_RING_SLOT_BYTES) } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let deadline = match deadline_from(timeout_ms) {
            Ok(d) => d,
            Err(code) => return code,
        };
        match l.pop_wait(consumer_id as usize, buf, deadline) {
            Ok(n) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out_len = n };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Move the ring to `locale`, one of the `SUBETHA_LOCALE_` constants,
/// carrying the items in flight across. A migration to the live locale
/// is a no-op.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_locale_ring_migrate(handle: subetha_handle, locale: u32) -> i32 {
    with_locale(handle, |l| {
        let target = match locale_from(locale) {
            Ok(t) => t,
            Err(code) => return code,
        };
        match l.ring.migrate_to(target) {
            Ok(()) => SUBETHA_OK,
            Err(e) => ring_code(e),
        }
    })
}

/// Managed mode: tell the sidecar the locale the caller wants; it migrates
/// once its hysteresis has elapsed. `SUBETHA_E_NOT_SUPPORTED` in strict
/// mode, where `subetha_locale_ring_migrate` is the call.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_locale_ring_request(handle: subetha_handle, locale: u32) -> i32 {
    with_locale(handle, |l| {
        let target = match locale_from(locale) {
            Ok(t) => t,
            Err(code) => return code,
        };
        match l.sidecar.lock().as_ref() {
            Some(sidecar) => {
                sidecar.request_locale(target);
                SUBETHA_OK
            }
            None => fail(SUBETHA_E_NOT_SUPPORTED, "strict mode has no sidecar; migrate directly"),
        }
    })
}

/// Set the ordering mode of a stamped locale ring on all three backings;
/// the contract is `subetha_ring_set_ordering_mode`'s.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_locale_ring_set_ordering_mode(handle: subetha_handle, mode: u32) -> i32 {
    with_locale(handle, |l| {
        let mode = match ordering_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        match l.ring.set_ordering_mode(mode) {
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
pub unsafe extern "C" fn subetha_locale_ring_read_stats(handle: subetha_handle, out: *mut subetha_locale_stats) -> i32 {
    with_locale(handle, |l| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = l.stats();
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Wake every thread parked in a wait on this ring; each re-checks the
/// ring and parks again unless it finds what it waited for.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_locale_ring_wake_all(handle: subetha_handle) -> i32 {
    with_locale(handle, |l| {
        l.consumer_waker.wake_all();
        l.producer_waker.wake_all();
        SUBETHA_OK
    })
}

/// Remove what a locale ring under `base_path` leaves behind. The ring
/// removes its tag, its generation and its file backing when its handle is
/// destroyed, in every process that holds one, so a process that keeps
/// its handle keeps its mapping while the files are already gone; this
/// call removes the two wakers and any file a peer's destroy could not,
/// for the creation hint `max_producers`. The contract is
/// `subetha_ring_unlink`'s.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `report` is null or a
/// valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_locale_ring_unlink(base_path: *const c_char, max_producers: u32, report: *mut subetha_unlink_report) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        let base = match unsafe { text(base_path, "base_path") } {
            Ok(p) => Path::new(p),
            Err(code) => return code,
        };
        let mut found = AdaptiveRing::unlink(with_suffix(base, ".locale.file.ring"), max_producers as usize);
        for suffix in [".locale.tag.bin", ".locale.gen.bin", ".cwaker.bin", ".pwaker.bin"] {
            found.remove(with_suffix(base, suffix));
        }
        unsafe { finish_unlink(found, report) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SUBETHA_MODE_STRICT;

    #[test]
    fn a_migration_carries_the_items_in_flight_and_the_stats_follow() {
        let base = std::env::temp_dir().join(format!(
            "subetha-ffi-locale-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let options = subetha_ring_options {
            mode: SUBETHA_MODE_STRICT,
            max_waiters: 0,
            scan_interval_us: 0,
            stamps: SUBETHA_STAMPS_NONE,
            ..Default::default()
        };
        let ring = LocaleAdaptiveRing::create(&base, 1, 1, 8).unwrap();
        let object = LocaleObject::build(ring, &base, &options, SUBETHA_MODE_STRICT).unwrap();
        let pid = object.ring.register_producer().unwrap();
        let cid = object.ring.register_consumer().unwrap();
        assert_eq!(object.stats().current_locale, SUBETHA_LOCALE_ANON);
        for i in 0..3u8 {
            object.try_push(pid, &[i]).unwrap();
        }
        object.ring.migrate_to(RingLocale::File).unwrap();
        let stats = object.stats();
        assert_eq!(stats.current_locale, SUBETHA_LOCALE_FILE);
        assert_eq!(stats.locale_generation, 1);
        assert_eq!(stats.approx_len, 3);
        let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
        for i in 0..3u8 {
            object.try_pop(cid, &mut out).unwrap();
            assert_eq!(out[0], i);
        }
        assert_eq!(object.try_pop(cid, &mut out).unwrap_err(), SUBETHA_E_RING_EMPTY);
        drop(object);
        // The ring removes its own files as it drops; the wakers are the ABI's.
        let mut found = AdaptiveRing::unlink(with_suffix(&base, ".locale.file.ring"), 1);
        for suffix in [".locale.tag.bin", ".locale.gen.bin", ".cwaker.bin", ".pwaker.bin"] {
            found.remove(with_suffix(&base, suffix));
        }
        assert_eq!(found.failed, 0, "{:?}", found.first_failure);
        assert!(found.removed >= 2, "the wakers at least: {found:?}");
    }
}
