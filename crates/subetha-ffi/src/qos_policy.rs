//! Quality-of-service policy through the C ABI: what a workload needs,
//! held where a sidecar reads it on every scan and a caller changes it
//! while traffic runs.
//!
//! Each field is its own atomic, so setting one is atomic and setting
//! several is not. There is deliberately no call that writes the whole
//! policy at once: it would look atomic and would not be, and a reader
//! between two of its stores would see a policy nobody asked for. A
//! caller that needs several fields to land together sets them in the
//! order that leaves every intermediate state safe to act on.
//!
//! `subetha_qos_read` has the same property in the other direction: it
//! reads five atomics, so it can return a mixture of two policies if a
//! writer runs during it. Every field it returns was real; the
//! combination need not have been.
//!
//! Strict and managed modes are the same here: a policy runs nothing.

use std::sync::Arc;
use std::time::Duration;

use subetha_cxc::qos_policy::{Durability, History, Ordering, QosPolicy};

use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_QOS_POLICY};
use crate::locale::locale_constant;
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// In-process memory; the bytes go when the process does.
pub const SUBETHA_QOS_VOLATILE: u32 = 0;
/// Named shared memory; outlives the producer while a holder keeps it.
pub const SUBETHA_QOS_TRANSIENT: u32 = 1;
/// File-backed; the bytes reach the page cache and can be flushed.
pub const SUBETHA_QOS_PERSISTENT: u32 = 2;

/// Loss is acceptable.
pub const SUBETHA_QOS_BEST_EFFORT: u32 = 0;
/// Loss is not.
pub const SUBETHA_QOS_RELIABLE: u32 = 1;

/// Keep the last `history_depth` items.
pub const SUBETHA_QOS_KEEP_LAST: u32 = 0;
/// Keep everything, so `history_depth` says nothing.
pub const SUBETHA_QOS_KEEP_ALL: u32 = 1;

/// Each producer's items stay in its own order; producers do not order
/// against each other.
pub const SUBETHA_QOS_PER_PRODUCER: u32 = 0;
/// One order across every producer.
pub const SUBETHA_QOS_GLOBAL_FIFO: u32 = 1;

/// The streaming preset: volatile, best-effort, last 1024, 100 ms.
pub const SUBETHA_QOS_PRESET_STREAMING: u32 = 0;
/// The reliable publish-subscribe preset.
pub const SUBETHA_QOS_PRESET_RELIABLE_PUBSUB: u32 = 1;
/// The persistent log preset.
pub const SUBETHA_QOS_PRESET_PERSISTENT_LOG: u32 = 2;

/// A policy read in one call.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_qos {
    /// One of `SUBETHA_QOS_VOLATILE`, `_TRANSIENT`, `_PERSISTENT`.
    pub durability: u32,
    /// One of `SUBETHA_QOS_BEST_EFFORT`, `_RELIABLE`.
    pub reliability: u32,
    /// One of `SUBETHA_QOS_KEEP_LAST`, `_KEEP_ALL`.
    pub history_kind: u32,
    /// How many items to keep, when `history_kind` is
    /// `SUBETHA_QOS_KEEP_LAST`. Zero and meaningless otherwise.
    pub history_depth: u32,
    /// The latency budget in nanoseconds.
    pub max_latency_nanos: u64,
    /// One of `SUBETHA_QOS_PER_PRODUCER`, `_GLOBAL_FIFO`.
    pub ordering: u32,
}

pub(crate) struct QosPolicyObject {
    policy: Arc<QosPolicy>,
    mode: u32,
}

impl QosPolicyObject {
    /// Nothing parks on a policy, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

fn with_policy(handle: subetha_handle, f: impl FnOnce(&QosPolicyObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_QOS_POLICY, |object| match object {
        Object::QosPolicy(q) => f(q),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a quality-of-service policy"),
    })
}

fn durability_of(tag: u32) -> Result<Durability, i32> {
    match tag {
        SUBETHA_QOS_VOLATILE => Ok(Durability::Volatile),
        SUBETHA_QOS_TRANSIENT => Ok(Durability::Transient),
        SUBETHA_QOS_PERSISTENT => Ok(Durability::Persistent),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("durability {other} names none"))),
    }
}

fn ordering_of(tag: u32) -> Result<Ordering, i32> {
    match tag {
        SUBETHA_QOS_PER_PRODUCER => Ok(Ordering::PerProducer),
        SUBETHA_QOS_GLOBAL_FIFO => Ok(Ordering::GlobalFifo),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("ordering {other} names none"))),
    }
}

fn history_of(kind: u32, depth: u32) -> Result<History, i32> {
    match kind {
        SUBETHA_QOS_KEEP_ALL => Ok(History::KeepAll),
        SUBETHA_QOS_KEEP_LAST if depth == 0 => Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "keeping the last zero items keeps nothing; ask for keep-all or a depth",
        )),
        SUBETHA_QOS_KEEP_LAST => Ok(History::KeepLastN(depth)),
        other => Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("history kind {other} names none"))),
    }
}

/// Make a policy from one of the `SUBETHA_QOS_PRESET_` constants.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_qos_create_preset(
    preset: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let policy = match preset {
            SUBETHA_QOS_PRESET_STREAMING => QosPolicy::streaming_default(),
            SUBETHA_QOS_PRESET_RELIABLE_PUBSUB => QosPolicy::reliable_pubsub_default(),
            SUBETHA_QOS_PRESET_PERSISTENT_LOG => QosPolicy::persistent_log_default(),
            other => {
                return fail(SUBETHA_E_INVALID_ARGUMENT, format!("preset {other} names none"))
            }
        };
        let object = QosPolicyObject { policy: Arc::new(policy), mode };
        unsafe { issue(Object::QosPolicy(object), out) }
    })
}

/// Make a policy from every field at once. Every field is validated
/// before anything is built, so a refusal leaves no handle.
///
/// # Safety
/// `initial` and `out` are valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_qos_create(
    initial: *const subetha_qos,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() || initial.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "initial or out is null");
        }
        // Checked non-null; the caller guarantees it is readable.
        let want = unsafe { *initial };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let durability = match durability_of(want.durability) {
            Ok(d) => d,
            Err(code) => return code,
        };
        let reliability = match want.reliability {
            SUBETHA_QOS_BEST_EFFORT => subetha_cxc::qos_policy::Reliability::BestEffort,
            SUBETHA_QOS_RELIABLE => subetha_cxc::qos_policy::Reliability::Reliable,
            other => {
                return fail(
                    SUBETHA_E_INVALID_ARGUMENT,
                    format!("reliability {other} names none"),
                )
            }
        };
        let history = match history_of(want.history_kind, want.history_depth) {
            Ok(h) => h,
            Err(code) => return code,
        };
        let ordering = match ordering_of(want.ordering) {
            Ok(o) => o,
            Err(code) => return code,
        };
        let policy = QosPolicy::new(
            durability,
            reliability,
            history,
            Duration::from_nanos(want.max_latency_nanos),
        );
        policy.set_ordering(ordering);
        let object = QosPolicyObject { policy: Arc::new(policy), mode };
        unsafe { issue(Object::QosPolicy(object), out) }
    })
}

/// Read the whole policy into `out`.
///
/// Five atomics are read one after another, so a writer running during
/// this can leave a mixture of two policies: every field was real, the
/// combination need not have been.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_qos_read(handle: subetha_handle, out: *mut subetha_qos) -> i32 {
    with_policy(handle, |q| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let snapshot = q.policy.snapshot();
        let (history_kind, history_depth) = match snapshot.history {
            History::KeepAll => (SUBETHA_QOS_KEEP_ALL, 0),
            History::KeepLastN(n) => (SUBETHA_QOS_KEEP_LAST, n),
        };
        let read = subetha_qos {
            durability: snapshot.durability as u32,
            reliability: snapshot.reliability as u32,
            history_kind,
            history_depth,
            max_latency_nanos: snapshot.max_latency.as_nanos().min(u128::from(u64::MAX)) as u64,
            ordering: snapshot.ordering as u32,
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = read };
        SUBETHA_OK
    })
}

/// Set the durability. Atomic on its own.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_qos_set_durability(handle: subetha_handle, durability: u32) -> i32 {
    with_policy(handle, |q| match durability_of(durability) {
        Ok(d) => {
            q.policy.set_durability(d);
            SUBETHA_OK
        }
        Err(code) => code,
    })
}

/// Set the reliability. Atomic on its own.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_qos_set_reliability(handle: subetha_handle, reliability: u32) -> i32 {
    with_policy(handle, |q| match reliability {
        SUBETHA_QOS_BEST_EFFORT => {
            q.policy.set_reliability(subetha_cxc::qos_policy::Reliability::BestEffort);
            SUBETHA_OK
        }
        SUBETHA_QOS_RELIABLE => {
            q.policy.set_reliability(subetha_cxc::qos_policy::Reliability::Reliable);
            SUBETHA_OK
        }
        other => fail(SUBETHA_E_INVALID_ARGUMENT, format!("reliability {other} names none")),
    })
}

/// Set the history. `depth` is read only for `SUBETHA_QOS_KEEP_LAST`, and
/// a depth of zero there is refused rather than kept as a policy that
/// keeps nothing. Atomic on its own: the kind and the depth share one
/// word.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_qos_set_history(handle: subetha_handle, kind: u32, depth: u32) -> i32 {
    with_policy(handle, |q| match history_of(kind, depth) {
        Ok(h) => {
            q.policy.set_history(h);
            SUBETHA_OK
        }
        Err(code) => code,
    })
}

/// Set the latency budget in nanoseconds. Atomic on its own.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_qos_set_max_latency(handle: subetha_handle, nanos: u64) -> i32 {
    with_policy(handle, |q| {
        q.policy.set_max_latency(Duration::from_nanos(nanos));
        SUBETHA_OK
    })
}

/// Set the ordering. Atomic on its own.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_qos_set_ordering(handle: subetha_handle, ordering: u32) -> i32 {
    with_policy(handle, |q| match ordering_of(ordering) {
        Ok(o) => {
            q.policy.set_ordering(o);
            SUBETHA_OK
        }
        Err(code) => code,
    })
}

/// The locale this policy's durability asks for, into `out`, as one of the
/// `SUBETHA_LOCALE_` constants.
///
/// It is a recommendation, not a move: a ring changes locale when a caller
/// tells it to.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_qos_recommended_locale(
    handle: subetha_handle,
    out: *mut u32,
) -> i32 {
    with_policy(handle, |q| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let locale = q.policy.durability().recommended_locale();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = locale_constant(locale) };
        SUBETHA_OK
    })
}

/// The ring capacity this policy's history asks for, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_qos_recommended_capacity(
    handle: subetha_handle,
    out: *mut u64,
) -> i32 {
    with_policy(handle, |q| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let capacity = q.policy.history().recommended_capacity();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = capacity as u64 };
        SUBETHA_OK
    })
}

/// The mode this policy was created in, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_qos_mode(handle: subetha_handle, out: *mut u32) -> i32 {
    with_policy(handle, |q| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = q.mode };
        SUBETHA_OK
    })
}
