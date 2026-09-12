//! Codes, their names, and the per-thread detail behind a failing call.
//!
//! One integer space: the hundreds digit is the domain. Codes below 100
//! belong to the runtime and the handle table, 100 to 199 to rings. Later
//! tiers take the next hundreds. A code alone is enough to act on;
//! [`subetha_last_error_detail`] carries the specifics (a path, two sizes
//! that disagreed) for the call that just failed on this thread.

use std::cell::RefCell;
use std::ffi::{c_char, CStr};

/// The call succeeded.
pub const SUBETHA_OK: i32 = 0;

/// No call may precede `subetha_init`.
pub const SUBETHA_E_NOT_INITIALIZED: i32 = 1;
/// `subetha_init` was called again with a different default mode.
pub const SUBETHA_E_INIT_CONFLICT: i32 = 2;
/// An argument was null, zero where a count is required, or out of range.
pub const SUBETHA_E_INVALID_ARGUMENT: i32 = 3;
/// The handle names no live object: never issued, already destroyed, or
/// issued by an earlier occupant of the same table slot.
pub const SUBETHA_E_INVALID_HANDLE: i32 = 4;
/// A panic inside an earlier call on this handle left the object
/// unusable. Destroy it and create another.
pub const SUBETHA_E_HANDLE_POISONED: i32 = 5;
/// This call panicked. The handle it ran on, if any, is now poisoned;
/// `subetha_last_panic_message` has the text.
pub const SUBETHA_E_PANIC: i32 = 6;
/// The handle is live but names an object of another kind.
pub const SUBETHA_E_WRONG_KIND: i32 = 7;
/// An allocation failed.
pub const SUBETHA_E_OUT_OF_MEMORY: i32 = 8;
/// A caller-provided buffer is too small; the detail names the size needed.
pub const SUBETHA_E_BUFFER_TOO_SMALL: i32 = 9;
/// A string argument is not valid UTF-8.
pub const SUBETHA_E_INVALID_UTF8: i32 = 10;
/// The timeout elapsed before the condition held.
pub const SUBETHA_E_TIMEOUT: i32 = 11;
/// The library has been shut down; `subetha_init` again before any other call.
pub const SUBETHA_E_SHUT_DOWN: i32 = 12;
/// `subetha_shutdown` found live handles and closed them; the detail says how many.
pub const SUBETHA_E_HANDLES_WERE_LIVE: i32 = 13;
/// The requested behavior exists in the library but not on this platform
/// or in this mode.
pub const SUBETHA_E_NOT_SUPPORTED: i32 = 14;
/// The object was destroyed while this call was waiting on it.
pub const SUBETHA_E_DESTROYED: i32 = 15;

/// The ring is full.
pub const SUBETHA_E_RING_FULL: i32 = 100;
/// The ring is empty.
pub const SUBETHA_E_RING_EMPTY: i32 = 101;
/// A backing exists but its magic or capacity does not match the request.
pub const SUBETHA_E_RING_LAYOUT_MISMATCH: i32 = 102;
/// The payload exceeds what one slot carries.
pub const SUBETHA_E_RING_PAYLOAD_TOO_LARGE: i32 = 103;
/// The operation needs ordering stamps the ring was not built with.
pub const SUBETHA_E_RING_NOT_STAMPED: i32 = 104;
/// Another consumer holds the drainer lease; back off and retry.
pub const SUBETHA_E_RING_NOT_DRAINER: i32 = 105;
/// A morph was asked for while the previous shape still holds a backlog.
pub const SUBETHA_E_RING_STALE_BACKLOG: i32 = 106;
/// The OS refused an open, map, or remove; the detail names the kind.
pub const SUBETHA_E_RING_IO: i32 = 107;
/// Producer registration refused by a declared contract or the slot ceiling.
pub const SUBETHA_E_RING_TOO_MANY_PRODUCERS: i32 = 108;
/// Consumer registration refused by a declared contract or the slot ceiling.
pub const SUBETHA_E_RING_TOO_MANY_CONSUMERS: i32 = 109;
/// Registration claimed a slot but the grown backing could not be created.
pub const SUBETHA_E_RING_GROWTH_FAILED: i32 = 110;
/// Every waiter slot is in use; poll with the try variant instead.
pub const SUBETHA_E_RING_WAKER_FULL: i32 = 111;
/// A waker region exists but does not match the ring's request.
pub const SUBETHA_E_RING_WAKER_LAYOUT: i32 = 112;
/// Every consumer slot of the broadcast ring is taken.
pub const SUBETHA_E_BROADCAST_NO_CONSUMER_SLOT: i32 = 113;
/// The consumer index is out of range or not registered.
pub const SUBETHA_E_BROADCAST_INVALID_CONSUMER: i32 = 114;
/// The position has not been published yet.
pub const SUBETHA_E_PUBSUB_PENDING: i32 = 115;
/// The position was overwritten before it was read; the subscriber's
/// position has moved to the ring's head.
pub const SUBETHA_E_PUBSUB_LOST: i32 = 116;
/// The deque handle is a thief's; push and pop belong to the owner.
pub const SUBETHA_E_DEQUE_NOT_OWNER: i32 = 117;
/// The map has no free slot for the key.
pub const SUBETHA_E_MAP_FULL: i32 = 118;
/// The key has no entry in the map.
pub const SUBETHA_E_MAP_KEY_ABSENT: i32 = 119;
/// The arena has no room for the whole value.
pub const SUBETHA_E_ARENA_FULL: i32 = 120;
/// The reference reaches past what the arena holds.
pub const SUBETHA_E_ARENA_INVALID_REF: i32 = 121;
/// The handle was opened read-only and the call writes.
pub const SUBETHA_E_READ_ONLY: i32 = 122;
/// The index is at or past the length or the capacity.
pub const SUBETHA_E_OUT_OF_BOUNDS: i32 = 123;
/// Every pin slot in the epoch table is held by a live process.
pub const SUBETHA_E_EPOCHS_PINS_EXHAUSTED: i32 = 124;
/// Every ticket slot in the epoch table is held by a live process.
pub const SUBETHA_E_EPOCHS_TICKETS_EXHAUSTED: i32 = 125;
/// Someone else holds the lock, or a writer is waiting for it.
pub const SUBETHA_E_WOULD_BLOCK: i32 = 126;
/// The call needs the lease and this process does not hold it.
pub const SUBETHA_E_NOT_OWNER: i32 = 127;
/// Every slot in the fence clock is registered.
pub const SUBETHA_E_FENCE_CLOCK_FULL: i32 = 128;
/// The barrier has no live peer to wait for, so a wait would never be
/// released by anyone.
pub const SUBETHA_E_BARRIER_NO_LIVE_PEERS: i32 = 129;
/// The epoch waited on is behind the one the barrier has reached, so the
/// caller is asking to join a rendezvous that is already over.
pub const SUBETHA_E_BARRIER_EPOCH_PASSED: i32 = 130;
/// Every holder slot on the shared value is held by a live process.
pub const SUBETHA_E_ARC_HOLDERS_EXHAUSTED: i32 = 131;
/// The claim on a lazy value belongs to a process that is gone, so no
/// one will publish it. `subetha_lazy_reclaim` frees it for the next
/// caller to claim.
pub const SUBETHA_E_LAZY_CLAIMANT_GONE: i32 = 132;
/// The lazy value is claimed by a live process and not yet published.
pub const SUBETHA_E_LAZY_NOT_CLAIMED: i32 = 133;

/// The name of a code, as a static NUL-terminated string. Safe from any
/// thread at any time; an unknown code names itself as unknown.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_strerror(code: i32) -> *const c_char {
    let s: &'static CStr = match code {
        SUBETHA_OK => c"ok",
        SUBETHA_E_NOT_INITIALIZED => c"subetha_init has not been called",
        SUBETHA_E_INIT_CONFLICT => c"subetha_init was called again with a different default mode",
        SUBETHA_E_INVALID_ARGUMENT => c"an argument is null, zero, or out of range",
        SUBETHA_E_INVALID_HANDLE => c"the handle names no live object",
        SUBETHA_E_HANDLE_POISONED => c"an earlier panic left this handle's object unusable",
        SUBETHA_E_PANIC => c"the call panicked",
        SUBETHA_E_WRONG_KIND => c"the handle names an object of another kind",
        SUBETHA_E_OUT_OF_MEMORY => c"an allocation failed",
        SUBETHA_E_BUFFER_TOO_SMALL => c"the caller's buffer is too small",
        SUBETHA_E_INVALID_UTF8 => c"a string argument is not valid UTF-8",
        SUBETHA_E_TIMEOUT => c"the timeout elapsed",
        SUBETHA_E_SHUT_DOWN => c"the library has been shut down",
        SUBETHA_E_HANDLES_WERE_LIVE => c"shutdown closed handles that were still live",
        SUBETHA_E_NOT_SUPPORTED => c"not supported on this platform or in this mode",
        SUBETHA_E_DESTROYED => c"the object was destroyed while this call waited on it",
        SUBETHA_E_RING_FULL => c"the ring is full",
        SUBETHA_E_RING_EMPTY => c"the ring is empty",
        SUBETHA_E_RING_LAYOUT_MISMATCH => c"a backing exists with a different layout",
        SUBETHA_E_RING_PAYLOAD_TOO_LARGE => c"the payload exceeds one slot",
        SUBETHA_E_RING_NOT_STAMPED => c"the ring carries no ordering stamps",
        SUBETHA_E_RING_NOT_DRAINER => c"another consumer holds the drainer lease",
        SUBETHA_E_RING_STALE_BACKLOG => c"the previous shape still holds a backlog",
        SUBETHA_E_RING_IO => c"the OS refused an open, map or remove",
        SUBETHA_E_RING_TOO_MANY_PRODUCERS => c"producer registration refused",
        SUBETHA_E_RING_TOO_MANY_CONSUMERS => c"consumer registration refused",
        SUBETHA_E_RING_GROWTH_FAILED => c"the grown backing could not be created",
        SUBETHA_E_RING_WAKER_FULL => c"every waiter slot is in use",
        SUBETHA_E_RING_WAKER_LAYOUT => c"a waker region exists with a different layout",
        SUBETHA_E_BROADCAST_NO_CONSUMER_SLOT => c"every consumer slot of the broadcast ring is taken",
        SUBETHA_E_BROADCAST_INVALID_CONSUMER => c"the consumer index is out of range or not registered",
        SUBETHA_E_PUBSUB_PENDING => c"the position has not been published yet",
        SUBETHA_E_PUBSUB_LOST => c"the position was overwritten before it was read",
        SUBETHA_E_DEQUE_NOT_OWNER => c"the deque handle is a thief's; push and pop belong to the owner",
        SUBETHA_E_MAP_FULL => c"the map has no free slot for the key",
        SUBETHA_E_MAP_KEY_ABSENT => c"the key has no entry in the map",
        SUBETHA_E_ARENA_FULL => c"the arena has no room for the whole value",
        SUBETHA_E_ARENA_INVALID_REF => c"the reference reaches past what the arena holds",
        SUBETHA_E_READ_ONLY => c"the handle was opened read-only",
        SUBETHA_E_OUT_OF_BOUNDS => c"the index is at or past the length or the capacity",
        SUBETHA_E_EPOCHS_PINS_EXHAUSTED => c"every pin slot is held by a live process",
        SUBETHA_E_EPOCHS_TICKETS_EXHAUSTED => c"every ticket slot is held by a live process",
        SUBETHA_E_FENCE_CLOCK_FULL => c"every slot in the fence clock is registered",
        SUBETHA_E_BARRIER_NO_LIVE_PEERS => c"the barrier has no live peer to wait for",
        SUBETHA_E_BARRIER_EPOCH_PASSED => c"the barrier has already passed the epoch waited on",
        SUBETHA_E_ARC_HOLDERS_EXHAUSTED => c"every holder slot on the shared value is held",
        SUBETHA_E_LAZY_CLAIMANT_GONE => c"the claim on the lazy value belongs to a process that is gone",
        SUBETHA_E_LAZY_NOT_CLAIMED => c"the caller does not hold the claim on the lazy value",
        SUBETHA_E_WOULD_BLOCK => c"someone else holds the lock, or a writer is waiting for it",
        SUBETHA_E_NOT_OWNER => c"the call needs the lease and this process does not hold it",
        _ => c"unknown subetha code",
    };
    s.as_ptr()
}

thread_local! {
    static DETAIL: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Record the specifics of the failure the current call is about to report.
pub(crate) fn set_detail(text: impl Into<String>) {
    DETAIL.with(|d| *d.borrow_mut() = text.into());
}

/// Record a failure's specifics and return its code, so a failing arm reads
/// as one expression.
pub(crate) fn fail(code: i32, text: impl Into<String>) -> i32 {
    set_detail(text);
    code
}

/// Copy the detail of the last failing call on this thread into `buf`,
/// NUL-terminated, and return the number of bytes the whole detail needs
/// including the terminator. A return larger than `cap` means the copy was
/// cut short; call again with a buffer of the returned size. With `buf` null
/// or `cap` zero nothing is copied and only the size is returned. The detail
/// describes the most recent failure and is meaningful only right after a
/// call returned a code other than SUBETHA_OK.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_last_error_detail(buf: *mut c_char, cap: usize) -> usize {
    DETAIL.with(|d| {
        let d = d.borrow();
        let needed = d.len() + 1;
        if buf.is_null() || cap == 0 {
            return needed;
        }
        let n = d.len().min(cap - 1);
        // SAFETY: the caller guarantees `cap` writable bytes at `buf`, and
        // `n + 1 <= cap`.
        unsafe {
            std::ptr::copy_nonoverlapping(d.as_ptr(), buf.cast::<u8>(), n);
            *buf.add(n) = 0;
        }
        needed
    })
}

/// The map from a ring's own error to the code that names it.
pub(crate) fn ring_code(e: subetha_cxc::shared_ring::RingError) -> i32 {
    use subetha_cxc::shared_ring::RingError;
    match e {
        RingError::Full => SUBETHA_E_RING_FULL,
        RingError::Empty => SUBETHA_E_RING_EMPTY,
        RingError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        RingError::PayloadTooLarge => SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
        RingError::NotStamped => SUBETHA_E_RING_NOT_STAMPED,
        RingError::NotDrainer => SUBETHA_E_RING_NOT_DRAINER,
        RingError::StaleBacklog => SUBETHA_E_RING_STALE_BACKLOG,
        RingError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a notifier's error to the code that names it.
pub(crate) fn notify_code(e: subetha_cxc::cross_process_notifier::NotifyError) -> i32 {
    use subetha_cxc::cross_process_notifier::NotifyError;
    match e {
        NotifyError::Io(kind, text) => fail(SUBETHA_E_RING_IO, format!("notifier io error: {kind:?}: {text}")),
        NotifyError::LayoutMismatch => fail(SUBETHA_E_RING_LAYOUT_MISMATCH, "a notifier record exists with another layout"),
    }
}

/// The map from a refused hold on a ring's backings to the code that
/// names it.
///
/// A locale with no files to remove and a region built for another
/// holder count are both the caller's argument being wrong about the
/// ring in front of it, which is what the invalid-argument code says;
/// an exhausted table is the ring's own capacity being reached, and
/// reads like a full ring rather than a bad call.
pub(crate) fn last_holder_code(e: subetha_cxc::adaptive_ring::LastHolderError) -> i32 {
    use subetha_cxc::adaptive_ring::LastHolderError;
    use subetha_cxc::ring_holders::HoldersError;
    match e {
        LastHolderError::NoBackingFiles => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "an anonymous ring has no backing files for a last holder to remove",
        ),
        LastHolderError::ShmNotSupported => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "a shared-memory ring does not carry a holder table",
        ),
        LastHolderError::Region(HoldersError::Exhausted) => fail(
            SUBETHA_E_RING_FULL,
            "every holder slot on this ring is held by a live process",
        ),
        LastHolderError::Region(HoldersError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the holders region beside this ring was built for a different max_holders",
        ),
        LastHolderError::Region(HoldersError::IoError(kind)) => {
            fail(SUBETHA_E_RING_IO, format!("holders region io error: {kind:?}"))
        }
    }
}

/// The map from a shared hash map's error to the code that names it.
pub(crate) fn map_code(e: subetha_cxc::shared_hash_map::MapError) -> i32 {
    use subetha_cxc::shared_hash_map::MapError;
    match e {
        MapError::Full => SUBETHA_E_MAP_FULL,
        MapError::PayloadTooLarge => fail(SUBETHA_E_INVALID_ARGUMENT, "a key or value is not its declared size, or the sizes exceed one entry"),
        MapError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        MapError::KeyAbsent => SUBETHA_E_MAP_KEY_ABSENT,
        MapError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a string arena's error to the code that names it.
pub(crate) fn arena_code(e: subetha_cxc::shared_string_arena::ArenaError) -> i32 {
    use subetha_cxc::shared_string_arena::ArenaError;
    match e {
        ArenaError::Full => SUBETHA_E_ARENA_FULL,
        ArenaError::InvalidRef => SUBETHA_E_ARENA_INVALID_REF,
        ArenaError::InvalidUtf8 => fail(SUBETHA_E_INVALID_UTF8, "the value is not UTF-8"),
        ArenaError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        ArenaError::ReadOnly => fail(SUBETHA_E_READ_ONLY, "the arena was opened read-only"),
        ArenaError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a holder table's error to the code that names it.
pub(crate) fn holders_code(e: subetha_cxc::shared_holder_table::SharedHolderError) -> i32 {
    use subetha_cxc::shared_holder_table::SharedHolderError;
    match e {
        SharedHolderError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        SharedHolderError::EmptyCapacity => fail(SUBETHA_E_INVALID_ARGUMENT, "a capacity of zero has no slot to claim"),
        SharedHolderError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a heartbeat table's error to the code that names it.
pub(crate) fn heartbeat_code(e: subetha_cxc::heartbeat::HeartbeatError) -> i32 {
    use subetha_cxc::heartbeat::HeartbeatError;
    match e {
        HeartbeatError::TableFull => fail(SUBETHA_E_RING_FULL, "every slot in the table is held"),
        HeartbeatError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        HeartbeatError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a leader election's error to the code that names it.
pub(crate) fn leader_code(e: subetha_cxc::shared_leader_election::LeaderError) -> i32 {
    use subetha_cxc::shared_leader_election::LeaderError;
    match e {
        LeaderError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        LeaderError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from an owner lease's error to the code that names it.
pub(crate) fn lease_code(e: subetha_cxc::owner_lease::LeaseError) -> i32 {
    use subetha_cxc::owner_lease::LeaseError;
    match e {
        LeaseError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        LeaseError::PayloadTooLarge => SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
        LeaseError::NotOwner => SUBETHA_E_NOT_OWNER,
        LeaseError::Contention => SUBETHA_E_WOULD_BLOCK,
        LeaseError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a condition variable's error to the code that names it.
pub(crate) fn condvar_code(e: subetha_cxc::shared_condvar::CondvarError) -> i32 {
    use subetha_cxc::shared_condvar::CondvarError;
    match e {
        CondvarError::WakerFull => SUBETHA_E_RING_WAKER_FULL,
        CondvarError::Timeout => SUBETHA_E_TIMEOUT,
        CondvarError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        CondvarError::Io(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a semaphore's error to the code that names it.
pub(crate) fn semaphore_code(e: subetha_cxc::shared_semaphore::SemaphoreError) -> i32 {
    use subetha_cxc::shared_semaphore::SemaphoreError;
    match e {
        SemaphoreError::WouldBlock => SUBETHA_E_WOULD_BLOCK,
        SemaphoreError::Timeout => SUBETHA_E_TIMEOUT,
        SemaphoreError::ReleaseOverflow => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "the release pushed the count past max_permits",
        ),
        SemaphoreError::Atomic(a) => atomic_code(a),
    }
}

/// The map from a shared lock's error to the code that names it.
pub(crate) fn rwlock_code(e: subetha_cxc::shared_rw_lock::RWLockError) -> i32 {
    use subetha_cxc::shared_rw_lock::RWLockError;
    match e {
        RWLockError::WouldBlock => SUBETHA_E_WOULD_BLOCK,
        RWLockError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        RWLockError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a fence clock's error to the code that names it.
pub(crate) fn fence_clock_code(e: subetha_cxc::shared_fence_clock::FenceClockError) -> i32 {
    use subetha_cxc::shared_fence_clock::FenceClockError;
    match e {
        FenceClockError::Full => SUBETHA_E_FENCE_CLOCK_FULL,
        FenceClockError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        FenceClockError::InvalidSlot => fail(SUBETHA_E_INVALID_ARGUMENT, "the slot is not one this clock holds"),
        FenceClockError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from an epoch table's error to the code that names it.
pub(crate) fn epochs_code(e: subetha_cxc::shared_epochs::EpochError) -> i32 {
    use subetha_cxc::shared_epochs::EpochError;
    match e {
        EpochError::PinsExhausted => SUBETHA_E_EPOCHS_PINS_EXHAUSTED,
        EpochError::TicketsExhausted => SUBETHA_E_EPOCHS_TICKETS_EXHAUSTED,
        EpochError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        EpochError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a shared cell's error to the code that names it.
pub(crate) fn cell_code(e: subetha_cxc::shared_cell::SharedCellError) -> i32 {
    use subetha_cxc::shared_cell::SharedCellError;
    match e {
        SharedCellError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        SharedCellError::PayloadTooLarge => SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
        SharedCellError::NotInitialized => fail(SUBETHA_E_INVALID_HANDLE, "the cell holds no value yet"),
        SharedCellError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a shared B-tree map's error to the code that names it.
pub(crate) fn btree_code(e: subetha_cxc::shared_btree_map::BTreeError) -> i32 {
    use subetha_cxc::shared_btree_map::BTreeError;
    match e {
        BTreeError::Full => fail(SUBETHA_E_RING_FULL, "the map has no node for the split this insert needs"),
        BTreeError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        BTreeError::InvalidConfig => fail(SUBETHA_E_INVALID_ARGUMENT, "a key or value is not its declared size, or the capacity is zero"),
        BTreeError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a shared linked list's error to the code that names it.
pub(crate) fn list_code(e: subetha_cxc::shared_linked_list::LinkedListError) -> i32 {
    use subetha_cxc::shared_linked_list::LinkedListError;
    match e {
        LinkedListError::Region(r) => region_code(r),
        LinkedListError::InvalidHandle => fail(SUBETHA_E_OUT_OF_BOUNDS, "the index names no node of this list"),
        LinkedListError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        LinkedListError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a shared atomic's error to the code that names it.
pub(crate) fn atomic_code(e: subetha_cxc::shared_atomic::SharedAtomicError) -> i32 {
    use subetha_cxc::shared_atomic::SharedAtomicError;
    match e {
        SharedAtomicError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        SharedAtomicError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a once cell's error to the code that names it.
pub(crate) fn once_code(e: subetha_cxc::shared_once_cell::SharedOnceError) -> i32 {
    use subetha_cxc::shared_once_cell::SharedOnceError;
    match e {
        SharedOnceError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        SharedOnceError::PayloadTooLarge => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "value_bytes is zero or past what a lazy value holds",
        ),
        SharedOnceError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a shared value's error to the code that names it.
pub(crate) fn arc_code(e: subetha_cxc::shared_arc::ArcError) -> i32 {
    use subetha_cxc::shared_arc::ArcError;
    match e {
        ArcError::HoldersExhausted => SUBETHA_E_ARC_HOLDERS_EXHAUSTED,
        ArcError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        ArcError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from an epoch barrier's error to the code that names it.
pub(crate) fn barrier_code(e: subetha_cxc::epoch_barrier::BarrierError) -> i32 {
    use subetha_cxc::epoch_barrier::BarrierError;
    match e {
        BarrierError::Atomic(inner) => atomic_code(inner),
        BarrierError::EpochTooFarBehind => SUBETHA_E_BARRIER_EPOCH_PASSED,
        BarrierError::Timeout => SUBETHA_E_TIMEOUT,
        BarrierError::NoLivePeers => SUBETHA_E_BARRIER_NO_LIVE_PEERS,
    }
}

/// The map from a shared region's error to the code that names it.
pub(crate) fn region_code(e: subetha_cxc::shared_region::RegionError) -> i32 {
    use subetha_cxc::shared_region::RegionError;
    match e {
        RegionError::Full => SUBETHA_E_RING_FULL,
        RegionError::InvalidPtr => SUBETHA_E_OUT_OF_BOUNDS,
        RegionError::PayloadTooLarge => fail(SUBETHA_E_INVALID_ARGUMENT, "the bytes are not the element size"),
        RegionError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        RegionError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a shared vec's error to the code that names it.
pub(crate) fn vec_code(e: subetha_cxc::shared_vec::VecError) -> i32 {
    use subetha_cxc::shared_vec::VecError;
    match e {
        VecError::Full => SUBETHA_E_RING_FULL,
        VecError::OutOfBounds => SUBETHA_E_OUT_OF_BOUNDS,
        VecError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        VecError::PayloadTooLarge => fail(SUBETHA_E_INVALID_ARGUMENT, "the bytes are not the element size"),
        VecError::ReadOnly => fail(SUBETHA_E_READ_ONLY, "the vec was opened read-only"),
        VecError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a shared slab's error to the code that names it.
pub(crate) fn slab_code(e: subetha_cxc::shared_slab::SlabError) -> i32 {
    use subetha_cxc::shared_slab::SlabError;
    match e {
        SlabError::OutOfBounds => SUBETHA_E_OUT_OF_BOUNDS,
        SlabError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        SlabError::ReadOnly => fail(SUBETHA_E_READ_ONLY, "the slab was opened read-only"),
        SlabError::PayloadTooLarge => fail(SUBETHA_E_INVALID_ARGUMENT, "the bytes are not the record size"),
        SlabError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a Treiber stack's error to the code that names it.
pub(crate) fn stack_code(e: subetha_cxc::shared_treiber_stack::StackError) -> i32 {
    use subetha_cxc::shared_treiber_stack::StackError;
    match e {
        StackError::Full => SUBETHA_E_RING_FULL,
        StackError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        StackError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a work-stealing deque's error to the code that names it.
pub(crate) fn deque_code(e: subetha_cxc::shared_deque::DequeError) -> i32 {
    use subetha_cxc::shared_deque::DequeError;
    match e {
        DequeError::Io(text) => fail(SUBETHA_E_RING_IO, format!("io error: {text}")),
        DequeError::InvalidCapacity => fail(SUBETHA_E_INVALID_ARGUMENT, "the capacity is not a power of two of at least 1, or the layout is empty or misaligned"),
        DequeError::InvalidMagic => fail(SUBETHA_E_RING_LAYOUT_MISMATCH, "the file is not a deque with this layout"),
        DequeError::CapacityMismatch { file_capacity, requested } => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            format!("the deque holds {file_capacity} slots, not {requested}"),
        ),
        DequeError::SlotBytesMismatch { file_slot_bytes, type_slot_bytes } => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            format!("the deque's slots are {file_slot_bytes} bytes, not {type_slot_bytes}"),
        ),
        DequeError::Full => SUBETHA_E_RING_FULL,
        DequeError::Marshal(e) => fail(SUBETHA_E_RING_PAYLOAD_TOO_LARGE, format!("{e:?}")),
    }
}

/// The map from a registration error to the code that names it.
pub(crate) fn adaptive_code(e: subetha_cxc::adaptive_ring::AdaptiveError) -> i32 {
    use subetha_cxc::adaptive_ring::AdaptiveError;
    match e {
        AdaptiveError::TooManyProducers => SUBETHA_E_RING_TOO_MANY_PRODUCERS,
        AdaptiveError::TooManyConsumers => SUBETHA_E_RING_TOO_MANY_CONSUMERS,
        AdaptiveError::GrowthFailed => SUBETHA_E_RING_GROWTH_FAILED,
    }
}

/// The map from a broadcast ring's error to the code that names it.
pub(crate) fn broadcast_code(e: subetha_cxc::shared_broadcast_ring::BroadcastError) -> i32 {
    use subetha_cxc::shared_broadcast_ring::BroadcastError;
    match e {
        BroadcastError::Full => SUBETHA_E_RING_FULL,
        BroadcastError::Empty => SUBETHA_E_RING_EMPTY,
        BroadcastError::NoConsumerSlot => SUBETHA_E_BROADCAST_NO_CONSUMER_SLOT,
        BroadcastError::InvalidConsumer => SUBETHA_E_BROADCAST_INVALID_CONSUMER,
        BroadcastError::PayloadTooLarge => SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
        BroadcastError::LayoutMismatch => SUBETHA_E_RING_LAYOUT_MISMATCH,
        BroadcastError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from an I/O error a ring constructor reports to the code that
/// names it: invalid data is a layout that does not match, anything else
/// the OS's refusal.
pub(crate) fn io_code(e: std::io::Error) -> i32 {
    if e.kind() == std::io::ErrorKind::InvalidData {
        fail(SUBETHA_E_RING_LAYOUT_MISMATCH, e.to_string())
    } else {
        fail(SUBETHA_E_RING_IO, format!("io error: {:?}: {e}", e.kind()))
    }
}

/// The map from a capacity morph's error to the code that names it.
pub(crate) fn capacity_code(e: subetha_cxc::capacity_adaptive_ring::CapacityMorphError) -> i32 {
    use subetha_cxc::capacity_adaptive_ring::CapacityMorphError;
    match e {
        CapacityMorphError::InvalidCapacity => fail(SUBETHA_E_INVALID_ARGUMENT, "the capacity is not a power of two of at least 2"),
        CapacityMorphError::CannotShrinkInFlight { in_flight, new_capacity } => fail(
            SUBETHA_E_RING_STALE_BACKLOG,
            format!("{in_flight} items in flight exceed the new capacity {new_capacity}"),
        ),
        CapacityMorphError::Ring(e) => ring_code(e),
        CapacityMorphError::Adaptive(e) => adaptive_code(e),
    }
}

/// The map from a broadcast capacity morph's error to the code that names it.
pub(crate) fn broadcast_capacity_code(e: subetha_cxc::capacity_broadcast_ring::BroadcastCapacityMorphError) -> i32 {
    use subetha_cxc::capacity_broadcast_ring::BroadcastCapacityMorphError;
    match e {
        BroadcastCapacityMorphError::InvalidCapacity => fail(SUBETHA_E_INVALID_ARGUMENT, "the capacity is not a power of two of at least 2"),
        BroadcastCapacityMorphError::Broadcast(e) => broadcast_code(e),
        BroadcastCapacityMorphError::Io(e) => io_code(e),
    }
}

/// The map from a pub/sub capacity morph's error to the code that names it.
pub(crate) fn pubsub_capacity_code(e: subetha_cxc::capacity_pubsub_ring::PubSubCapacityMorphError) -> i32 {
    use subetha_cxc::capacity_pubsub_ring::PubSubCapacityMorphError;
    match e {
        PubSubCapacityMorphError::InvalidCapacity => fail(SUBETHA_E_INVALID_ARGUMENT, "the capacity is not a power of two of at least 2"),
        PubSubCapacityMorphError::Io(e) => io_code(e),
    }
}

/// The map from a blocking ring's error to the code that names it.
pub(crate) fn blocking_code(e: subetha_cxc::blocking_spsc_ring::BlockingError) -> i32 {
    use subetha_cxc::blocking_spsc_ring::BlockingError;
    match e {
        BlockingError::Ring(e) => ring_code(e),
        BlockingError::WakerFull => SUBETHA_E_RING_WAKER_FULL,
        BlockingError::Timeout => SUBETHA_E_TIMEOUT,
        BlockingError::WakerLayout => SUBETHA_E_RING_WAKER_LAYOUT,
        BlockingError::Io(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind:?}")),
    }
}

/// The map from a waker error to the code that names it.
pub(crate) fn waker_code(e: subetha_cxc::cross_process_waker::WakerError) -> i32 {
    use subetha_cxc::cross_process_waker::WakerError;
    match e {
        WakerError::Full => SUBETHA_E_RING_WAKER_FULL,
        WakerError::Timeout => SUBETHA_E_TIMEOUT,
        WakerError::LayoutMismatch => SUBETHA_E_RING_WAKER_LAYOUT,
        WakerError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("waker io error: {kind:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_code_has_a_name_and_the_unknown_one_says_so() {
        for code in [
            SUBETHA_OK,
            SUBETHA_E_NOT_INITIALIZED,
            SUBETHA_E_INIT_CONFLICT,
            SUBETHA_E_INVALID_ARGUMENT,
            SUBETHA_E_INVALID_HANDLE,
            SUBETHA_E_HANDLE_POISONED,
            SUBETHA_E_PANIC,
            SUBETHA_E_WRONG_KIND,
            SUBETHA_E_OUT_OF_MEMORY,
            SUBETHA_E_BUFFER_TOO_SMALL,
            SUBETHA_E_INVALID_UTF8,
            SUBETHA_E_TIMEOUT,
            SUBETHA_E_SHUT_DOWN,
            SUBETHA_E_HANDLES_WERE_LIVE,
            SUBETHA_E_NOT_SUPPORTED,
            SUBETHA_E_DESTROYED,
            SUBETHA_E_RING_FULL,
            SUBETHA_E_RING_EMPTY,
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            SUBETHA_E_RING_PAYLOAD_TOO_LARGE,
            SUBETHA_E_RING_NOT_STAMPED,
            SUBETHA_E_RING_NOT_DRAINER,
            SUBETHA_E_RING_STALE_BACKLOG,
            SUBETHA_E_RING_IO,
            SUBETHA_E_RING_TOO_MANY_PRODUCERS,
            SUBETHA_E_RING_TOO_MANY_CONSUMERS,
            SUBETHA_E_RING_GROWTH_FAILED,
            SUBETHA_E_RING_WAKER_FULL,
            SUBETHA_E_RING_WAKER_LAYOUT,
            SUBETHA_E_BROADCAST_NO_CONSUMER_SLOT,
            SUBETHA_E_BROADCAST_INVALID_CONSUMER,
            SUBETHA_E_PUBSUB_PENDING,
            SUBETHA_E_PUBSUB_LOST,
            SUBETHA_E_DEQUE_NOT_OWNER,
            SUBETHA_E_MAP_FULL,
            SUBETHA_E_MAP_KEY_ABSENT,
            SUBETHA_E_ARENA_FULL,
            SUBETHA_E_ARENA_INVALID_REF,
            SUBETHA_E_READ_ONLY,
            SUBETHA_E_OUT_OF_BOUNDS,
        ] {
            let s = unsafe { CStr::from_ptr(subetha_strerror(code)) };
            assert_ne!(s.to_str().unwrap(), "unknown subetha code", "code {code} has no name");
        }
        let s = unsafe { CStr::from_ptr(subetha_strerror(9_999)) };
        assert_eq!(s.to_str().unwrap(), "unknown subetha code");
    }

    #[test]
    fn detail_reports_the_size_it_needs_and_never_overruns() {
        set_detail("a path and two sizes");
        let needed = unsafe { subetha_last_error_detail(std::ptr::null_mut(), 0) };
        assert_eq!(needed, "a path and two sizes".len() + 1);
        let mut small = [0x7f as c_char; 8];
        let got = unsafe { subetha_last_error_detail(small.as_mut_ptr(), small.len()) };
        assert_eq!(got, needed, "a short buffer still reports the full size");
        assert_eq!(small[7], 0, "the copy is NUL-terminated inside the buffer");
        let text = unsafe { CStr::from_ptr(small.as_ptr()) }.to_str().unwrap();
        assert_eq!(text, "a path ");
        let mut full = vec![0 as c_char; needed];
        let got = unsafe { subetha_last_error_detail(full.as_mut_ptr(), full.len()) };
        assert_eq!(got, needed);
        let text = unsafe { CStr::from_ptr(full.as_ptr()) }.to_str().unwrap();
        assert_eq!(text, "a path and two sizes");
    }
}
