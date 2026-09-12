//! Process-wide state: initialization, the default mode, shutdown, the ABI
//! version, panic accounting, and the guards every entry point runs under.
//!
//! `subetha_init` is required before any other call and `subetha_shutdown`
//! before the library is unloaded. A panic inside any entry point is caught
//! at the boundary: the call returns `SUBETHA_E_PANIC`, the handle it ran on
//! is poisoned, and the message is kept.

use std::any::Any;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;

use parking_lot::Mutex;

use crate::error::{
    fail, set_detail, SUBETHA_E_HANDLES_WERE_LIVE, SUBETHA_E_INIT_CONFLICT,
    SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_NOT_INITIALIZED, SUBETHA_E_PANIC, SUBETHA_E_SHUT_DOWN,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, Table, SUBETHA_KIND_RING};
use crate::ring::RingObject;

/// ABI version, major part. The ABI is unstable until the major reaches
/// 1, which it does once tiers 0 through 3 have shipped and a consumer
/// outside this repository has used them. Both halves are load-bearing:
/// the first real user is what finds the awkward signature, and the
/// freeze is what makes an awkward signature permanent.
pub const SUBETHA_ABI_VERSION_MAJOR: u32 = 0;
/// ABI version, minor part: the highest tier of `C_ABI_TIERS.md` that
/// has shipped, plus one. A consumer reads it to learn which families it
/// is linked against without probing for symbols, which is the whole
/// reason `subetha_abi_version` exists.
///
/// | minor | tier | what it adds |
/// |---|---|---|
/// | 1 | 0 | runtime, handle table, error model, the adaptive ring |
/// | 2 | 1 | the remaining rings and channels, stack, deque, notifier |
/// | 3 | 2 | ten shared-state families |
/// | 4 | 3 | eight coordination families |
/// | 5 | 4 | the transports: Sens-O-Matic and the TCP and QUIC bridges |
/// | 6 | 5 | the probabilistic and specialist structures |
///
/// `abi_version_matches_the_tiers_document` in `tests/header.rs` holds
/// this against the document, so the two cannot drift.
pub const SUBETHA_ABI_VERSION_MINOR: u32 = 6;
/// ABI version, patch part. Moves for a fix inside a tier that changes
/// no signature.
pub const SUBETHA_ABI_VERSION_PATCH: u32 = 0;

/// Strict mode: no thread starts unless a call says so, data-path calls
/// write into caller buffers, and allocation is documented per function.
pub const SUBETHA_MODE_STRICT: u32 = 0;
/// Managed mode: the object runs the background work the Rust API runs.
pub const SUBETHA_MODE_MANAGED: u32 = 1;
/// Take the process default chosen at `subetha_init`.
pub const SUBETHA_MODE_DEFAULT: u32 = 2;

const STATE_UNINITIALIZED: u8 = 0;
const STATE_READY: u8 = 1;
const STATE_SHUT_DOWN: u8 = 2;

static STATE: AtomicU8 = AtomicU8::new(STATE_UNINITIALIZED);
static DEFAULT_MODE: AtomicU32 = AtomicU32::new(SUBETHA_MODE_STRICT);
/// Serializes init and shutdown against each other and records whether the
/// exit hook is registered; the hot path reads `STATE` without it.
static TRANSITION: Mutex<bool> = Mutex::new(false);
static TABLE: OnceLock<Table> = OnceLock::new();
static PANICS: AtomicU64 = AtomicU64::new(0);
static LAST_PANIC: Mutex<Option<String>> = Mutex::new(None);
static CRATE_VERSION: OnceLock<CString> = OnceLock::new();

pub(crate) fn table() -> &'static Table {
    TABLE.get_or_init(Table::new)
}

/// The mode an object gets from the mode a caller passed.
pub(crate) fn resolve_mode(mode: u32) -> Result<u32, i32> {
    match mode {
        SUBETHA_MODE_STRICT | SUBETHA_MODE_MANAGED => Ok(mode),
        SUBETHA_MODE_DEFAULT => Ok(DEFAULT_MODE.load(Ordering::Acquire)),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("mode {other} is not a mode"))),
    }
}

pub(crate) fn require_initialized() -> Result<(), i32> {
    match STATE.load(Ordering::Acquire) {
        STATE_READY => Ok(()),
        STATE_SHUT_DOWN => Err(fail(SUBETHA_E_SHUT_DOWN, "subetha_shutdown has run")),
        _ => Err(fail(SUBETHA_E_NOT_INITIALIZED, "subetha_init has not been called")),
    }
}

/// The text of a panic payload: the `&str` or `String` a `panic!` carries,
/// or a fixed description for a payload of any other type.
fn panic_text(payload: &(dyn Any + Send)) -> String {
    match (payload.downcast_ref::<&str>(), payload.downcast_ref::<String>()) {
        (Some(s), _) => (*s).to_owned(),
        (None, Some(s)) => s.clone(),
        (None, None) => "panic with a payload that is not text".to_owned(),
    }
}

fn record_panic(payload: Box<dyn Any + Send>) -> String {
    let text = panic_text(payload.as_ref());
    PANICS.fetch_add(1, Ordering::AcqRel);
    *LAST_PANIC.lock() = Some(text.clone());
    set_detail(format!("panic: {text}"));
    text
}

/// Run an entry point that touches no handle. A panic becomes
/// `SUBETHA_E_PANIC`.
pub(crate) fn entry(f: impl FnOnce() -> i32) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(code) => code,
        Err(payload) => {
            record_panic(payload);
            SUBETHA_E_PANIC
        }
    }
}

/// Run an entry point on the object a handle names, which must be of
/// `kind`. A panic poisons the handle and becomes `SUBETHA_E_PANIC`.
pub(crate) fn with_kind(handle: subetha_handle, kind: u32, f: impl FnOnce(&Object) -> i32) -> i32 {
    if let Err(code) = require_initialized() {
        return code;
    }
    let borrowed = match table().borrow(handle, kind) {
        Ok(b) => b,
        Err(code) => return code,
    };
    match catch_unwind(AssertUnwindSafe(|| f(borrowed.object()))) {
        Ok(code) => code,
        Err(payload) => {
            let text = record_panic(payload);
            borrowed.poison(text);
            SUBETHA_E_PANIC
        }
    }
}

/// Run an entry point on the ring a handle names. A panic poisons the
/// handle and becomes `SUBETHA_E_PANIC`.
pub(crate) fn with_ring(handle: subetha_handle, f: impl FnOnce(&RingObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_RING, |object| match object {
        Object::Ring(ring) => f(ring),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a ring"),
    })
}

/// Place a newly built object and write its handle to `out`.
///
/// # Safety
/// `out` must be a valid pointer, checked by the caller.
pub(crate) unsafe fn issue(object: Object, out: *mut subetha_handle) -> i32 {
    let handle = table().insert(object);
    // SAFETY: the caller checked `out` is non-null and writable.
    unsafe { *out = handle };
    SUBETHA_OK
}
extern "C" fn report_at_exit() {
    if STATE.load(Ordering::Acquire) == STATE_READY {
        let live = table().live();
        if live > 0 {
            eprintln!(
                "subetha: the process is exiting with {live} live handle(s) and no \
                 subetha_shutdown; call subetha_shutdown before the library is unloaded"
            );
        }
    }
}

/// Initialize the library with the mode objects take when they ask for
/// `SUBETHA_MODE_DEFAULT`. Required before any other call. Calling it again
/// with the same default succeeds; with a different default it fails with
/// `SUBETHA_E_INIT_CONFLICT` and changes nothing. After `subetha_shutdown`,
/// calling it again re-initializes.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_init(default_mode: u32) -> i32 {
    entry(|| {
        if default_mode != SUBETHA_MODE_STRICT && default_mode != SUBETHA_MODE_MANAGED {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("default_mode {default_mode} is neither strict nor managed"),
            );
        }
        let mut registered = TRANSITION.lock();
        if STATE.load(Ordering::Acquire) == STATE_READY {
            let current = DEFAULT_MODE.load(Ordering::Acquire);
            if current == default_mode {
                return SUBETHA_OK;
            }
            return fail(
                SUBETHA_E_INIT_CONFLICT,
                format!("initialized with default mode {current}; {default_mode} was asked for"),
            );
        }
        DEFAULT_MODE.store(default_mode, Ordering::Release);
        STATE.store(STATE_READY, Ordering::Release);
        if !*registered {
            unsafe extern "C" {
                fn atexit(cb: extern "C" fn()) -> i32;
            }
            // SAFETY: atexit takes a plain C function pointer that lives for
            // the whole process.
            let rc = unsafe { atexit(report_at_exit) };
            if rc != 0 {
                eprintln!("subetha: atexit refused the shutdown report hook (rc {rc})");
            }
            *registered = true;
        }
        SUBETHA_OK
    })
}

/// Shut the library down: destroy every live handle, joining any thread an
/// object owns, and refuse every later call until `subetha_init` runs
/// again. Returns `SUBETHA_E_HANDLES_WERE_LIVE` when handles had to be
/// closed, with the count in the detail and a line on stderr, since a
/// handle still open at shutdown is a caller that forgot one.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_shutdown() -> i32 {
    entry(|| {
        let _serialized = TRANSITION.lock();
        if let Err(code) = require_initialized() {
            return code;
        }
        let closed = table().destroy_all();
        STATE.store(STATE_SHUT_DOWN, Ordering::Release);
        if closed > 0 {
            eprintln!("subetha: shutdown closed {closed} handle(s) the caller had not destroyed");
            return fail(SUBETHA_E_HANDLES_WERE_LIVE, format!("{closed} handle(s) were live"));
        }
        SUBETHA_OK
    })
}

/// How many thread words the borrow guard's registry holds.
///
/// Every reclamation walks this after its barrier, so it is the second
/// term in what closing a handle costs, and it only ever grows: a word is
/// returned when its thread ends but the table keeps it for the next one.
/// Exposed so a benchmark can record the walk length it measured against
/// rather than infer it from the order its rows ran in.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_borrow_registry_len() -> u64 {
    crate::epoch::registry_len() as u64
}

/// The ABI version as `(major << 16) | (minor << 8) | patch`. Safe before
/// `subetha_init`.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_abi_version() -> u32 {
    (SUBETHA_ABI_VERSION_MAJOR << 16) | (SUBETHA_ABI_VERSION_MINOR << 8) | SUBETHA_ABI_VERSION_PATCH
}

/// The ABI version as a static string. Safe before `subetha_init`.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_abi_version_string() -> *const c_char {
    const S: &CStr = c"0.6.0";
    S.as_ptr()
}

/// The version of the crate this library was built from, as a string that
/// lives for the whole process. Safe before `subetha_init`.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_crate_version_string() -> *const c_char {
    CRATE_VERSION
        .get_or_init(|| {
            CString::new(env!("CARGO_PKG_VERSION"))
                .expect("CARGO_PKG_VERSION is a version string and holds no NUL")
        })
        .as_ptr()
}

/// The process default mode, into `out`.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_default_mode(out: *mut u32) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = DEFAULT_MODE.load(Ordering::Acquire) };
        SUBETHA_OK
    })
}
/// Panics caught at the boundary since the process started. Safe before
/// `subetha_init`.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_panic_count() -> u64 {
    PANICS.load(Ordering::Acquire)
}

/// Copy the most recent caught panic's message into `buf`, NUL-terminated,
/// and return the size the whole message needs including the terminator;
/// the same contract as `subetha_last_error_detail`. Zero means no panic has
/// been caught.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_last_panic_message(buf: *mut c_char, cap: usize) -> usize {
    let guard = LAST_PANIC.lock();
    if let Some(text) = guard.as_deref() {
        let needed = text.len() + 1;
        if buf.is_null() || cap == 0 {
            return needed;
        }
        let n = text.len().min(cap - 1);
        // SAFETY: the caller guarantees `cap` writable bytes at `buf`, and
        // `n + 1 <= cap`.
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), buf.cast::<u8>(), n);
            *buf.add(n) = 0;
        }
        needed
    } else {
        0
    }
}
/// Handles currently live, poisoned ones included.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_live_handles() -> u64 {
    table().live()
}

/// Destroy the object a handle names. Refuses new calls on it, wakes any
/// thread waiting inside it (those calls return `SUBETHA_E_DESTROYED`),
/// waits for every call in flight to leave, then drops the object, joining
/// any thread it owns. A poisoned handle is destroyed the same way. The
/// handle names nothing afterwards, and can never name anything again.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_handle_destroy(handle: subetha_handle) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        table().destroy(handle)
    })
}

/// The kind of object a handle names, into `out`: one of the `SUBETHA_KIND_`
/// constants. Works on a poisoned handle.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_handle_kind(handle: subetha_handle, out: *mut u32) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match table().kind_of(handle) {
            Ok(kind) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = kind };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}

/// Whether a handle is poisoned, into `out`.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_handle_is_poisoned(handle: subetha_handle, out: *mut bool) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match table().is_poisoned(handle) {
            Ok(poisoned) => {
                // SAFETY: checked non-null; the caller guarantees it is writable.
                unsafe { *out = poisoned };
                SUBETHA_OK
            }
            Err(code) => code,
        }
    })
}
/// Panic inside a call on `handle`, so a test can watch the boundary catch
/// it and poison the handle. Present only with the `test-hooks` feature.
#[cfg(feature = "test-hooks")]
#[unsafe(no_mangle)]
pub extern "C" fn subetha_test_panic_on(handle: subetha_handle) -> i32 {
    with_ring(handle, |ring| panic!("subetha_test_panic_on at {:p}", ring as *const RingObject))
}

/// Panic inside a call that touches no handle. Present only with the
/// `test-hooks` feature.
#[cfg(feature = "test-hooks")]
#[unsafe(no_mangle)]
pub extern "C" fn subetha_test_panic_free() -> i32 {
    entry(|| panic!("subetha_test_panic_free"))
}

/// The recent ring-trace events touching `ring`, oldest first, as
/// printable lines, so a failing workload can report what the other
/// threads did to a ring. Empty in a release build, where the trace is
/// not compiled. Present only with the `test-hooks` feature.
#[cfg(feature = "test-hooks")]
pub fn subetha_test_ring_trace(ring: usize, limit: usize) -> Vec<String> {
    #[cfg(debug_assertions)]
    {
        subetha_cxc::ring_trace::recent_for(ring, limit)
    }
    #[cfg(not(debug_assertions))]
    {
        let _without_the_trace = (ring, limit);
        Vec::new()
    }
}

/// Write this process's recent ring-trace events to stderr, oldest
/// first, each prefixed with `RING-TRACE` and this process id. Returns
/// the number of lines written, or `SUBETHA_E_PANIC`.
///
/// The trace is a per-process buffer, so a workload split across
/// processes keeps one per process and a parent that creates no rings
/// has an empty one. A child is the only place its own history exists,
/// which is why a role calls this on its own failure path instead of
/// leaving the parent to ask for something it cannot see.
///
/// `ring_index` selects a sub-ring. Two kinds of event carry no ring of
/// their own and so appear whatever is passed:
///
/// - a peer taking or releasing a slot,
/// - the counts a shape decision read.
///
/// A dump without those cannot show whether a slot was held twice or
/// merely in turn.
///
/// Zero in a release build, where the trace is not compiled. Present
/// only with the `test-hooks` feature.
#[cfg(feature = "test-hooks")]
#[unsafe(no_mangle)]
pub extern "C" fn subetha_test_ring_trace_dump(ring_index: usize, limit: usize) -> i32 {
    entry(|| {
        let lines = subetha_test_ring_trace(ring_index, limit);
        let pid = std::process::id();
        for line in &lines {
            eprintln!("RING-TRACE [{pid}] {line}");
        }
        eprintln!("RING-TRACE [{pid}] {} line(s) for ring {ring_index}", lines.len());
        i32::try_from(lines.len()).unwrap_or(i32::MAX)
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_packed_version_agrees_with_its_string() {
        let packed = subetha_abi_version();
        let major = packed >> 16;
        let minor = (packed >> 8) & 0xff;
        let patch = packed & 0xff;
        let s = unsafe { CStr::from_ptr(subetha_abi_version_string()) }.to_str().unwrap();
        assert_eq!(s, format!("{major}.{minor}.{patch}"));
        let v = unsafe { CStr::from_ptr(subetha_crate_version_string()) }.to_str().unwrap();
        assert_eq!(v, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn modes_resolve_and_the_rest_is_refused() {
        assert_eq!(resolve_mode(SUBETHA_MODE_STRICT).unwrap(), SUBETHA_MODE_STRICT);
        assert_eq!(resolve_mode(SUBETHA_MODE_MANAGED).unwrap(), SUBETHA_MODE_MANAGED);
        assert_eq!(resolve_mode(7).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
    }

    fn a_call_that_panics() -> i32 {
        panic!("kept for the caller")
    }

    #[test]
    fn a_panic_is_caught_counted_and_kept() {
        let before = subetha_panic_count();
        let code = entry(a_call_that_panics);
        assert_eq!(code, SUBETHA_E_PANIC);
        assert_eq!(subetha_panic_count(), before + 1);
        let needed = unsafe { subetha_last_panic_message(std::ptr::null_mut(), 0) };
        let mut buf = vec![0 as c_char; needed];
        let copied = unsafe { subetha_last_panic_message(buf.as_mut_ptr(), buf.len()) };
        assert_eq!(copied, needed);
        let text = unsafe { CStr::from_ptr(buf.as_ptr()) }.to_str().unwrap();
        assert_eq!(text, "kept for the caller");
    }
}
