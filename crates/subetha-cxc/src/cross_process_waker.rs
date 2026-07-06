//! `CrossProcessWaker`: a futex-shaped wait/wake primitive sitting
//! in shared memory (MMF or named-shm), portable across Linux /
//! Windows / macOS / FreeBSD.
//!
//! # The problem
//!
//! SubEtha's bounded rings deliver bytes between threads / processes
//! without any kernel involvement on the hot path. That's a win
//! when the consumer can keep up - try_recv either returns an item
//! or returns `Empty` and the caller decides what to do. The
//! pattern breaks down when the consumer wants to BLOCK on an empty
//! ring without spinning: there's no kernel-side handle to wait on,
//! and a busy-wait burns one CPU per blocked consumer.
//!
//! `CrossProcessWaker` closes the gap. It's the userspace `futex`,
//! ported to the substrate. The producer publishes a monotonic
//! sequence atom on every push; a blocked consumer parks on a wake
//! list in shared memory, registering the sequence it wants to be
//! woken at. When the producer's sequence advances past that
//! target, the producer's post-publish path fires a single
//! syscall-level wake and the consumer's `wait` returns.
//!
//! # Cross-platform wake
//!
//! The primitive calls the platform's wait / wake syscalls
//! directly (NOT via the `atomic-wait` crate, which hard-codes
//! `FUTEX_PRIVATE_FLAG` on Linux and so cannot work across
//! processes):
//!
//! - Linux / Android: `futex(FUTEX_WAIT)` / `futex(FUTEX_WAKE)`
//!   without the PRIVATE flag - the kernel hashes by the page's
//!   physical address so any process that mapped the same MMF
//!   page joins the same wait queue.
//! - FreeBSD: `_umtx_op(UMTX_OP_WAIT_UINT)` /
//!   `_umtx_op(UMTX_OP_WAKE)` - the non-PRIVATE umtx ops, whose
//!   sleep queues the kernel keys by PHYSICAL address exactly so
//!   process-shared synchronization works (per `_umtx_op(2)`).
//!   Same cross-process semantics as the Linux arm.
//! - Windows: `WaitOnAddress` / `WakeByAddressSingle` for
//!   process-private (anon-backed) wakers - those calls are
//!   INTRA-PROCESS only per Microsoft's docs. Cross-process
//!   (file / named-shm backed) wakers wait on the hardware
//!   MONITOR tier instead (`crate::monitor_wait`): monitors are
//!   physical-address based, so a store from another process to
//!   the shared MMF line wakes the waiter - the platform's only
//!   non-polling cross-process wake. On Windows hosts without
//!   MONITORX/WAITPKG, cross-process waits fall back to the
//!   wait-timeout + re-check recovery the blocking wrappers
//!   already run.
//! - macOS / other: polling fallback (correct, but wastes CPU
//!   when idle).
//!
//! All shipping syscalls operate on a user-space address (no
//! kernel handle bookkeeping per primitive), so the cross-process
//! Linux case needs only that the waker's atomic lives in a
//! mapping both processes have (named-shm or file-backed mmap).
//! The kernel sees the same physical page from both sides and
//! the wake reaches the parker.
//!
//! # Storage layout
//!
//! ```text
//! +--------------------------------------+ offset 0
//! | WakerHeader (64 bytes, one cache line)|
//! |   magic: u64                          |
//! |   capacity: u32                       |
//! |   _pad                                |
//! +--------------------------------------+ offset 64
//! | WakerSlot[0] (64 bytes)              |
//! |   state: AtomicU32 (FREE/PARKED/WOKEN)|
//! |   _pad                                |
//! |   target_seq: AtomicU64               |
//! |   _pad                                |
//! +--------------------------------------+
//! | WakerSlot[1] ... WakerSlot[N-1]      |
//! +--------------------------------------+
//! ```
//!
//! Each slot is one cache line so producer's wake-scan and
//! parker's state writes don't false-share across slots.
//!
//! # Wake protocol
//!
//! ## Consumer (parker) side
//!
//! 1. Scan slots for one with `state == FREE`.
//! 2. CAS that slot's state from FREE to a transient RESERVED state.
//! 3. Write `target_seq` (the sequence we want to be woken at).
//! 4. Store state from RESERVED to PARKED with Release ordering -
//!    this publishes the slot to producers and is the
//!    happens-before edge for `target_seq`.
//! 5. Call the platform's wait syscall on `&slot.state` with
//!    expected = PARKED. The kernel verifies `state == PARKED`
//!    before sleeping (Linux's futex_wait semantics; Windows'
//!    WaitOnAddress likewise); if a producer's wake-CAS already
//!    landed (state == WOKEN), wait returns immediately without
//!    entering the kernel sleep path.
//! 6. On return, store state back to FREE and release the slot.
//!
//! ## Producer (waker) side
//!
//! On every successful publish, call `wake_up_to(producer_seq)`.
//! That scans slots:
//!
//! 1. Acquire-load `state`. If not PARKED, skip.
//! 2. Relaxed-load `target_seq`. The Acquire on `state` acquired
//!    the parker's Release-store, so prior writes (incl. target_seq)
//!    are visible.
//! 3. If `producer_seq >= target_seq`, CAS state from PARKED to
//!    WOKEN. On CAS success, call the platform's wake-one syscall
//!    on `&slot.state` and increment the wake counter.
//!
//! The CAS guards against a double-wake when multiple producers
//! race to wake the same slot.
//!
//! # Wake-before-park race
//!
//! Between a blocked-recv's "try_recv returned Empty" check and
//! its `try_park` call, a producer can publish AND call wake_up_to
//! that finds zero parked slots. The standard recovery is the
//! double-check in the blocking-recv wrapper: after parking,
//! re-call try_recv before calling wait. If try_recv succeeds,
//! release the token and return. Only if it still returns Empty
//! does the consumer call wait.
//!
//! # Linux-futex-raw escape hatch
//!
//! The Cargo feature `linux-futex-raw` exposes the direct
//! `libc::syscall(SYS_futex, ...)` surface to callers that need
//! `FUTEX_WAIT_BITSET`, `FUTEX_REQUEUE`, or other ops the portable
//! `atomic-wait` abstraction does not expose. Linux-only.

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

/// Magic bytes identifying a CrossProcessWaker region. Used by
/// `open` to reject the wrong kind of MMF.
pub const WAKER_MAGIC: u64 = 0xE7E7_5742_4B45_5201; // "..WBKER.."

/// Default slot capacity. Caller-overridable on construction.
pub const MAX_WAITERS_DEFAULT: usize = 32;

const STATE_FREE: u32 = 0;
const STATE_RESERVED: u32 = 1;
const STATE_PARKED: u32 = 2;
const STATE_WOKEN: u32 = 3;

#[repr(C, align(64))]
struct WakerHeader {
    magic: u64,
    capacity: u32,
    _pad0: [u8; 4],
    /// One bit per slot index (< 64): set while the slot is
    /// PARKED-ish. Producers' wake scans load this word first; a
    /// zero mask makes the no-waiters case - the overwhelmingly
    /// common one on a healthy ring - ONE cache line instead of
    /// `capacity` slot lines. The bit is advisory: stale-set bits
    /// are filtered by the per-slot state check, and a not-yet-set
    /// bit is covered by the parker's pre-wait double-check, the
    /// same race window the full scan always had. Capacities > 64
    /// skip the mask and full-scan.
    parked_mask: AtomicU64,
    _pad: [u8; 64 - 24],
}

#[repr(C, align(64))]
struct WakerSlot {
    state: AtomicU32,
    _pad1: [u8; 4],
    target_seq: AtomicU64,
    _pad2: [u8; 64 - 16],
}

const _: () = {
    assert!(std::mem::size_of::<WakerHeader>() == 64);
    assert!(std::mem::size_of::<WakerSlot>() == 64);
};

/// Errors returned by waker operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakerError {
    /// All slots in use. Caller's fallback path: spin until the
    /// thing they care about is ready (the same path they'd use
    /// without a waker).
    Full,
    /// `wait()` returned because its timeout elapsed before any
    /// producer fired a wake.
    Timeout,
    /// `open()` rejected the MMF because magic / capacity did
    /// not match.
    LayoutMismatch,
    /// I/O error from the underlying mmap.
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for WakerError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

impl std::fmt::Display for WakerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "no free waker slot"),
            Self::Timeout => write!(f, "wait timed out"),
            Self::LayoutMismatch => write!(f, "waker layout mismatch on open"),
            Self::IoError(k) => write!(f, "waker mmap io error: {k:?}"),
        }
    }
}

impl std::error::Error for WakerError {}

/// Returned by `try_park`; identifies which slot the parker
/// reserved. Pass back into `wait` and `release`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakerToken {
    slot: u32,
}

impl WakerToken {
    /// The slot index this token is bound to. Useful for
    /// debugging / instrumentation.
    pub fn slot_index(&self) -> u32 { self.slot }
}

/// Bytes required for a waker region holding `capacity` slots.
pub const fn waker_region_size(capacity: usize) -> usize {
    std::mem::size_of::<WakerHeader>() + capacity * std::mem::size_of::<WakerSlot>()
}

/// Cross-process wake list. See module docs for the protocol.
pub struct CrossProcessWaker {
    _backing: WakerBacking,
    raw_ptr: *mut u8,
    capacity: usize,
}

unsafe impl Send for CrossProcessWaker {}
unsafe impl Sync for CrossProcessWaker {}

#[allow(dead_code)]
enum WakerBacking {
    Anon(MmapMut),
    File(File, MmapMut),
    Shm(crate::shm_file::ShmFile),
}

unsafe fn init_waker_layout_raw(ptr: *mut u8, capacity: usize) {
    let hdr_ptr = ptr as *mut WakerHeader;
    unsafe {
        std::ptr::write_bytes(hdr_ptr as *mut u8, 0, std::mem::size_of::<WakerHeader>());
        (*hdr_ptr).magic = WAKER_MAGIC;
        (*hdr_ptr).capacity = capacity as u32;
    }
    let slots_base = unsafe { ptr.add(std::mem::size_of::<WakerHeader>()) };
    for i in 0..capacity {
        let slot_ptr = unsafe {
            slots_base.add(i * std::mem::size_of::<WakerSlot>())
        } as *mut WakerSlot;
        unsafe {
            std::ptr::write(slot_ptr, WakerSlot {
                state: AtomicU32::new(STATE_FREE),
                _pad1: [0; 4],
                target_seq: AtomicU64::new(0),
                _pad2: [0; 64 - 16],
            });
        }
    }
}

impl CrossProcessWaker {
    /// Anon (in-process) waker. Cross-thread only; for cross-
    /// process use `create` (file) or `create_from_shm` (named).
    pub fn create_anon(capacity: usize) -> Result<Self, WakerError> {
        assert!(capacity >= 1, "capacity must be >= 1");
        let total = waker_region_size(capacity);
        let mut mmap = MmapOptions::new().len(total).map_anon()?;
        let raw_ptr = mmap.as_mut_ptr();
        unsafe { init_waker_layout_raw(raw_ptr, capacity); }
        Ok(Self {
            _backing: WakerBacking::Anon(mmap),
            raw_ptr,
            capacity,
        })
    }

    /// File-backed waker. Cross-process visible via the OS page
    /// cache.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, WakerError> {
        assert!(capacity >= 1, "capacity must be >= 1");
        let total = waker_region_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let raw_ptr = mmap.as_mut_ptr();
        unsafe { init_waker_layout_raw(raw_ptr, capacity); }
        Ok(Self {
            _backing: WakerBacking::File(file, mmap),
            raw_ptr,
            capacity,
        })
    }

    /// Open an existing file-backed waker. Validates magic +
    /// capacity.
    pub fn open(
        path: impl AsRef<Path>,
        expected_capacity: usize,
    ) -> Result<Self, WakerError> {
        let total = waker_region_size(expected_capacity);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if (file.metadata()?.len() as usize) < total {
            return Err(WakerError::LayoutMismatch);
        }
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let raw_ptr = mmap.as_mut_ptr();
        let hdr = unsafe { &*(raw_ptr as *const WakerHeader) };
        if hdr.magic != WAKER_MAGIC || hdr.capacity as usize != expected_capacity {
            return Err(WakerError::LayoutMismatch);
        }
        Ok(Self {
            _backing: WakerBacking::File(file, mmap),
            raw_ptr,
            capacity: expected_capacity,
        })
    }

    /// Build a fresh waker on top of a named-shm region. Cross-
    /// process visible via the `logical_name` of the underlying
    /// [`ShmFile`](crate::shm_file::ShmFile); RAM-resident.
    pub fn create_from_shm(
        mut shm: crate::shm_file::ShmFile,
        capacity: usize,
    ) -> Result<Self, WakerError> {
        assert!(capacity >= 1, "capacity must be >= 1");
        let total = waker_region_size(capacity);
        if shm.len() < total {
            return Err(WakerError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        unsafe { init_waker_layout_raw(raw_ptr, capacity); }
        Ok(Self {
            _backing: WakerBacking::Shm(shm),
            raw_ptr,
            capacity,
        })
    }

    /// Open an existing named-shm waker without re-initialising
    /// the layout.
    pub fn open_from_shm(
        mut shm: crate::shm_file::ShmFile,
        expected_capacity: usize,
    ) -> Result<Self, WakerError> {
        let total = waker_region_size(expected_capacity);
        if shm.len() < total {
            return Err(WakerError::LayoutMismatch);
        }
        let raw_ptr = shm.as_mut_slice().as_mut_ptr();
        let hdr = unsafe { &*(raw_ptr as *const WakerHeader) };
        if hdr.magic != WAKER_MAGIC || hdr.capacity as usize != expected_capacity {
            return Err(WakerError::LayoutMismatch);
        }
        Ok(Self {
            _backing: WakerBacking::Shm(shm),
            raw_ptr,
            capacity: expected_capacity,
        })
    }

    /// Slot count fixed at construction.
    pub fn capacity(&self) -> usize { self.capacity }

    #[inline]
    fn slot(&self, idx: usize) -> &WakerSlot {
        let base = unsafe { self.raw_ptr.add(std::mem::size_of::<WakerHeader>()) };
        unsafe { &*(base.add(idx * std::mem::size_of::<WakerSlot>()) as *const WakerSlot) }
    }

    /// Reserve a slot and park at `target_seq`. The caller will
    /// be woken when some producer calls `wake_up_to(seq)` with
    /// `seq >= target_seq`. Returns the token the caller passes
    /// to `wait` / `release`.
    ///
    /// On `Err(Full)`, every slot is currently in use; the caller
    /// falls back to spinning. This is the same fallback they'd
    /// use without a waker at all.
    pub fn try_park(&self, target_seq: u64) -> Result<WakerToken, WakerError> {
        for idx in 0..self.capacity {
            let slot = self.slot(idx);
            // FREE -> RESERVED CAS. On success, this slot is
            // ours; on failure, another parker beat us to it,
            // try the next slot.
            if slot
                .state
                .compare_exchange(
                    STATE_FREE,
                    STATE_RESERVED,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // Write target_seq before publishing as PARKED.
                // The Release store on state below is the
                // happens-before edge for this Relaxed store.
                slot.target_seq.store(target_seq, Ordering::Relaxed);
                // Publish the slot to producers.
                slot.state.store(STATE_PARKED, Ordering::Release);
                self.mask_set(idx);
                return Ok(WakerToken { slot: idx as u32 });
            }
        }
        Err(WakerError::Full)
    }

    /// Block until either some producer wakes this token or the
    /// optional timeout elapses. After return (Ok or Err), the
    /// token's slot is released; the caller does NOT need to
    /// call `release` separately.
    ///
    /// If the slot's state was already transitioned to `WOKEN`
    /// before the wait call entered the kernel, the wait returns
    /// immediately (no kernel sleep).
    /// Whether this waker's backing is reachable from other
    /// processes (file / named-shm) rather than process-private
    /// anonymous memory. Windows' wait ladder branches on this:
    /// `WaitOnAddress` never receives a cross-process wake, so
    /// cross-process-backed waits stay on the hardware monitor
    /// tier (physical-address based) for their whole duration.
    fn is_cross_process(&self) -> bool {
        !matches!(self._backing, WakerBacking::Anon(_))
    }

    fn header(&self) -> &WakerHeader {
        unsafe { &*(self.raw_ptr as *const WakerHeader) }
    }

    #[inline]
    fn mask_set(&self, idx: usize) {
        if idx < 64 {
            self.header()
                .parked_mask
                .fetch_or(1u64 << idx, Ordering::Release);
        }
    }

    #[inline]
    fn mask_clear(&self, idx: usize) {
        if idx < 64 {
            self.header()
                .parked_mask
                .fetch_and(!(1u64 << idx), Ordering::Release);
        }
    }

    /// Iterator over candidate slot indices for a wake scan: the
    /// parked-mask bits when the mask covers every slot, else the
    /// full range.
    #[inline]
    fn wake_candidates(&self) -> WakeCandidates {
        if self.capacity <= 64 {
            WakeCandidates::Mask(
                self.header().parked_mask.load(Ordering::Acquire),
            )
        } else {
            WakeCandidates::Range(0, self.capacity)
        }
    }

    pub fn wait(
        &self,
        token: WakerToken,
        timeout: Option<Duration>,
    ) -> Result<(), WakerError> {
        let slot = self.slot(token.slot as usize);
        let cross_process = self.is_cross_process();
        let result = match timeout {
            None => {
                loop {
                    let cur = slot.state.load(Ordering::Acquire);
                    if cur != STATE_PARKED {
                        break Ok(());
                    }
                    platform_wait::wait_forever(
                        &slot.state, STATE_PARKED, cross_process,
                    );
                }
            }
            Some(d) => {
                // Deadline re-check loop. The wait syscall is only a
                // hint: futex and the MONITOR/MWAIT tier may both wake
                // SPURIOUSLY, so `state` - not the syscall's return -
                // is the authority. A spurious wake re-loops and waits
                // the remaining time; only a real producer transition
                // (state != PARKED) returns Ok, and only an elapsed
                // deadline returns Timeout. Without this loop a single
                // spurious wake returned Ok, making waits end early and
                // freeing the slot before a wake_all could see it.
                let deadline = Instant::now() + d;
                loop {
                    if slot.state.load(Ordering::Acquire) != STATE_PARKED {
                        break Ok(());
                    }
                    match deadline.checked_duration_since(Instant::now()) {
                        Some(remaining) if !remaining.is_zero() => {
                            platform_wait::wait_with_timeout(
                                &slot.state, STATE_PARKED, remaining, cross_process,
                            );
                        }
                        _ => {
                            // Deadline reached; one last check catches a
                            // wake that landed at the wire.
                            break if slot.state.load(Ordering::Acquire) != STATE_PARKED {
                                Ok(())
                            } else {
                                Err(WakerError::Timeout)
                            };
                        }
                    }
                }
            }
        };
        slot.state.store(STATE_FREE, Ordering::Release);
        self.mask_clear(token.slot as usize);
        result
    }

    /// Release a parked slot without waiting. Used by the
    /// blocking-recv wrapper's wake-before-park-race recovery:
    /// after parking, the wrapper double-checks try_recv; if
    /// that succeeds, it calls release to give the slot back
    /// without entering the kernel.
    pub fn release(&self, token: WakerToken) {
        let slot = self.slot(token.slot as usize);
        slot.state.store(STATE_FREE, Ordering::Release);
        self.mask_clear(token.slot as usize);
    }

    /// Producer's post-publish wake call. Scans every slot;
    /// for each PARKED slot whose `target_seq <= seq`, CASes
    /// state to WOKEN and fires a single-slot wake. Returns the
    /// number of consumers woken.
    pub fn wake_up_to(&self, seq: u64) -> usize {
        let mut count = 0usize;
        for idx in self.wake_candidates() {
            let slot = self.slot(idx);
            let cur_state = slot.state.load(Ordering::Acquire);
            if cur_state != STATE_PARKED {
                continue;
            }
            // The Acquire on state acquires the parker's Release
            // store, so target_seq is safe to read Relaxed.
            let tgt = slot.target_seq.load(Ordering::Relaxed);
            if seq < tgt {
                continue;
            }
            // Try to claim the wake. If the CAS fails another
            // producer already woke this slot (or the parker
            // released it); skip.
            if slot
                .state
                .compare_exchange(
                    STATE_PARKED,
                    STATE_WOKEN,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                platform_wait::wake_one(&slot.state, self.is_cross_process());
                count += 1;
            }
        }
        count
    }

    /// Wake AT MOST ONE PARKED slot whose `target_seq <= seq`. Used
    /// by Mesa-style condvar `notify_one`: notifier bumps the
    /// generation, then wakes exactly one waiter (if any) so the
    /// other parked waiters stay parked.
    ///
    /// Returns 1 if a waiter was woken, 0 if none qualified.
    pub fn wake_one_up_to(&self, seq: u64) -> usize {
        for idx in self.wake_candidates() {
            let slot = self.slot(idx);
            let cur_state = slot.state.load(Ordering::Acquire);
            if cur_state != STATE_PARKED {
                continue;
            }
            let tgt = slot.target_seq.load(Ordering::Relaxed);
            if seq < tgt {
                continue;
            }
            if slot
                .state
                .compare_exchange(
                    STATE_PARKED,
                    STATE_WOKEN,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                platform_wait::wake_one(&slot.state, self.is_cross_process());
                return 1;
            }
        }
        0
    }

    /// Wake every PARKED slot regardless of `target_seq`. Used
    /// during shutdown / drain so blocked consumers see the
    /// terminate signal.
    pub fn wake_all(&self) -> usize {
        let mut count = 0usize;
        for idx in self.wake_candidates() {
            let slot = self.slot(idx);
            if slot
                .state
                .compare_exchange(
                    STATE_PARKED,
                    STATE_WOKEN,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                platform_wait::wake_one(&slot.state, self.is_cross_process());
                count += 1;
            }
        }
        count
    }
}

/// Wake-scan candidate indices: set bits of the parked mask, or a
/// plain range when the capacity outgrows the 64-bit mask.
enum WakeCandidates {
    Mask(u64),
    Range(usize, usize),
}

impl Iterator for WakeCandidates {
    type Item = usize;
    #[inline]
    fn next(&mut self) -> Option<usize> {
        match self {
            WakeCandidates::Mask(m) => {
                if *m == 0 {
                    return None;
                }
                let idx = m.trailing_zeros() as usize;
                *m &= *m - 1;
                Some(idx)
            }
            WakeCandidates::Range(next, end) => {
                if next < end {
                    let idx = *next;
                    *next += 1;
                    Some(idx)
                } else {
                    None
                }
            }
        }
    }
}

// ============================================================================
// Platform wait / wake. We do NOT use the atomic-wait crate
// because that crate hard-codes FUTEX_PRIVATE_FLAG on Linux,
// which restricts the futex to a single process and breaks the
// cross-process wake claim. The waker calls the platform's
// SHARED futex / WaitOnAddress / wake APIs directly.
//
// Every wait first runs the bounded MONITOR-class tier (see
// crate::monitor_wait): MONITORX/MWAITX or UMONITOR/UMWAIT light
// sleep on the slot's cache line for ~tens of microseconds, woken
// for free by the producer's state store - cross-process included,
// since hardware monitors are physical-address based. Only when
// that budget expires does the wait escalate to the per-platform
// kernel park below.
//
// Cross-process status per platform:
// - Linux / Android: SHARED futex (no PRIVATE flag) works
//   across processes when the atomic sits in a SHARED mmap.
// - FreeBSD: _umtx_op with the non-PRIVATE UMTX_OP_WAIT_UINT /
//   UMTX_OP_WAKE ops; the kernel keys those sleep queues by
//   physical address ("same variable mapped multiple times will
//   give one key value" - _umtx_op(2)), so waiters across
//   processes sharing an MMF page join one queue.
// - Windows: WaitOnAddress is INTRA-PROCESS only per the docs,
//   so it serves anon-backed wakers; file / shm-backed wakers
//   stay on the monitor tier for their whole wait (the
//   cross_process flag below selects this), which IS
//   cross-process because hardware monitors key on physical
//   addresses. Hosts without MONITORX/WAITPKG fall back to the
//   wait-timeout + re-check recovery in the blocking wrappers.
// - macOS / others: polling fallback; correct but wastes CPU
//   under heavy idle.
// ============================================================================

mod platform_wait {
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

    /// macOS 14.4+ `os_sync_*` public-futex symbols resolved at RUNTIME via
    /// `dlsym`, so the binary LINKS against an older SDK (e.g. 10.15, whose
    /// libsystem has no `os_sync_*`) and degrades to the polling fallback there,
    /// while taking the fast path on 14.4+. The flag constants are plain
    /// integers (no link dependency), so only the three functions are resolved.
    #[cfg(target_os = "macos")]
    mod os_sync_dyn {
        use std::ffi::c_void;
        use std::sync::atomic::{AtomicUsize, Ordering};
        pub type WaitFn = unsafe extern "C" fn(*mut c_void, u64, usize, u32) -> i32;
        pub type WaitTimeoutFn =
            unsafe extern "C" fn(*mut c_void, u64, usize, u32, u32, u64) -> i32;
        pub type WakeFn = unsafe extern "C" fn(*mut c_void, usize, u32) -> i32;
        // 1 = not yet probed, 0 = absent (older macOS), else = resolved fn ptr.
        static WAIT: AtomicUsize = AtomicUsize::new(1);
        static WAIT_TO: AtomicUsize = AtomicUsize::new(1);
        static WAKE: AtomicUsize = AtomicUsize::new(1);
        fn cached(slot: &AtomicUsize, name: &[u8]) -> usize {
            let v = slot.load(Ordering::Relaxed);
            if v != 1 {
                return v;
            }
            // SAFETY: RTLD_DEFAULT lookup of a C symbol by NUL-terminated name.
            let r = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr() as *const _) } as usize;
            slot.store(r, Ordering::Relaxed);
            r
        }
        pub fn wait() -> Option<WaitFn> {
            match cached(&WAIT, b"os_sync_wait_on_address\0") {
                0 => None,
                // SAFETY: a non-null resolution of this symbol has this ABI.
                p => Some(unsafe { std::mem::transmute::<usize, WaitFn>(p) }),
            }
        }
        pub fn wait_timeout() -> Option<WaitTimeoutFn> {
            match cached(&WAIT_TO, b"os_sync_wait_on_address_with_timeout\0") {
                0 => None,
                p => Some(unsafe { std::mem::transmute::<usize, WaitTimeoutFn>(p) }),
            }
        }
        pub fn wake() -> Option<WakeFn> {
            match cached(&WAKE, b"os_sync_wake_by_address_any\0") {
                0 => None,
                p => Some(unsafe { std::mem::transmute::<usize, WakeFn>(p) }),
            }
        }
    }

    /// The monitor tier: MONITORX/MWAITX (AMD) or UMONITOR/UMWAIT
    /// (WAITPKG) light-sleep waiting for a bounded cycle budget
    /// BEFORE the kernel park. Two wins when the wait resolves
    /// inside the budget: the producer's wake is its existing
    /// state-CAS (no syscall on either side), and - because
    /// hardware monitors are physical-address based - the wake
    /// crosses process boundaries on shared MMF pages, which on
    /// Windows is the only non-polling cross-process wake the
    /// platform offers (WaitOnAddress is intra-process). Returning
    /// `false` (budget expired / unsupported CPU /
    /// SUBETHA_NO_MONITOR_WAIT=1) falls through to the kernel
    /// park, which re-checks the value itself - so the tier can
    /// never lose a wake, only hand off.
    #[inline]
    fn monitor_tier(atomic: &AtomicU32, expected: u32) -> bool {
        crate::monitor_wait::monitor_wait_u32(
            atomic,
            expected,
            crate::monitor_wait::monitor_wait_budget_cycles(),
        )
    }

    pub fn wait_forever(atomic: &AtomicU32, expected: u32, _cross_process: bool) {
        if monitor_tier(atomic, expected) {
            return;
        }
        // Windows + cross-process backing: WaitOnAddress never
        // receives a wake from another process, so the monitor IS
        // the wait - re-arm in budget-sized chunks until the value
        // changes. The core holds C0.1 light sleep rather than
        // releasing to the OS; that is the only non-polling
        // cross-process wait the platform offers.
        #[cfg(windows)]
        if _cross_process
            && crate::monitor_wait::monitor_wait_kind().is_some()
        {
            let budget = crate::monitor_wait::monitor_wait_budget_cycles();
            while atomic.load(std::sync::atomic::Ordering::Acquire) == expected {
                crate::monitor_wait::monitor_wait_u32(atomic, expected, budget);
            }
            return;
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    atomic.as_ptr(),
                    libc::FUTEX_WAIT,
                    expected as libc::c_int,
                    std::ptr::null::<libc::timespec>(),
                );
            }
        }
        #[cfg(target_os = "freebsd")]
        {
            // UMTX_OP_WAIT_UINT (the non-PRIVATE op): the kernel
            // keys the sleep queue by the variable's PHYSICAL
            // address, so waiters in any process that mapped the
            // same MMF page share one queue - FreeBSD's native
            // equivalent of the no-FUTEX_PRIVATE_FLAG Linux call.
            // Sleeps only while *obj == val, exactly futex_wait.
            unsafe {
                libc::_umtx_op(
                    atomic.as_ptr() as *mut libc::c_void,
                    libc::UMTX_OP_WAIT_UINT,
                    expected as libc::c_ulong,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
            }
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::Threading::{WaitOnAddress, INFINITE};
            let expected_local = expected;
            unsafe {
                WaitOnAddress(
                    atomic.as_ptr() as *const std::ffi::c_void,
                    &expected_local as *const u32 as *const std::ffi::c_void,
                    std::mem::size_of::<u32>(),
                    INFINITE,
                );
            }
        }
        #[cfg(target_os = "macos")]
        {
            // The public futex (macOS 14.4+): compare-and-wait on the address;
            // OS_SYNC_WAIT_ON_ADDRESS_SHARED keys the queue for a shared-memory
            // address, allowing a futex wake from another process (the backing
            // decides the flag). The symbol is resolved at runtime; on older
            // macOS it is absent, so poll like the generic fallback below.
            if let Some(wait_fn) = os_sync_dyn::wait() {
                let flags = if _cross_process {
                    libc::OS_SYNC_WAIT_ON_ADDRESS_SHARED
                } else {
                    libc::OS_SYNC_WAIT_ON_ADDRESS_NONE
                };
                unsafe {
                    wait_fn(
                        atomic.as_ptr() as *mut libc::c_void,
                        expected as u64,
                        std::mem::size_of::<u32>(),
                        flags,
                    );
                }
            } else {
                std::thread::yield_now();
                while atomic.load(std::sync::atomic::Ordering::Acquire) == expected {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "android",
                      target_os = "freebsd", target_os = "macos", windows)))]
        {
            std::thread::yield_now();
            while atomic.load(std::sync::atomic::Ordering::Acquire) == expected {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    pub fn wait_with_timeout(
        atomic: &AtomicU32,
        expected: u32,
        timeout: Duration,
        _cross_process: bool,
    ) -> bool {
        let monitor_start = std::time::Instant::now();
        if monitor_tier(atomic, expected) {
            return true;
        }
        // The monitor budget counts against the caller's timeout;
        // the kernel park gets the remainder.
        let timeout = match timeout.checked_sub(monitor_start.elapsed()) {
            Some(rest) if !rest.is_zero() => rest,
            _ => {
                return atomic.load(std::sync::atomic::Ordering::Acquire)
                    != expected;
            }
        };
        // Windows + cross-process backing: stay on the monitor for
        // the whole timeout (see wait_forever).
        #[cfg(windows)]
        if _cross_process
            && crate::monitor_wait::monitor_wait_kind().is_some()
        {
            let deadline = std::time::Instant::now() + timeout;
            let budget = crate::monitor_wait::monitor_wait_budget_cycles();
            loop {
                if crate::monitor_wait::monitor_wait_u32(atomic, expected, budget) {
                    return true;
                }
                if std::time::Instant::now() >= deadline {
                    return atomic.load(std::sync::atomic::Ordering::Acquire)
                        != expected;
                }
            }
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let ts = libc::timespec {
                tv_sec: timeout.as_secs() as libc::time_t,
                tv_nsec: timeout.subsec_nanos() as libc::c_long,
            };
            unsafe {
                let rc = libc::syscall(
                    libc::SYS_futex,
                    atomic.as_ptr(),
                    libc::FUTEX_WAIT,
                    expected as libc::c_int,
                    &ts as *const libc::timespec,
                    std::ptr::null::<()>(),
                    0u32,
                );
                if rc == -1 {
                    let err = *libc::__errno_location();
                    return err != libc::ETIMEDOUT;
                }
            }
            true
        }
        #[cfg(target_os = "freebsd")]
        {
            // Relative timeout, monotonic clock by default: uaddr2
            // points at the timespec and uaddr carries that
            // structure's size, per _umtx_op(2).
            let mut ts = libc::timespec {
                tv_sec: timeout.as_secs() as libc::time_t,
                tv_nsec: timeout.subsec_nanos() as libc::c_long,
            };
            let rc = unsafe {
                libc::_umtx_op(
                    atomic.as_ptr() as *mut libc::c_void,
                    libc::UMTX_OP_WAIT_UINT,
                    expected as libc::c_ulong,
                    std::mem::size_of::<libc::timespec>() as *mut libc::c_void,
                    &mut ts as *mut libc::timespec as *mut libc::c_void,
                )
            };
            if rc == -1 {
                let err = unsafe { *libc::__error() };
                return err != libc::ETIMEDOUT;
            }
            true
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::Threading::WaitOnAddress;
            let expected_local = expected;
            let ms = timeout.as_millis().min(u32::MAX as u128) as u32;
            let rc = unsafe {
                WaitOnAddress(
                    atomic.as_ptr() as *const std::ffi::c_void,
                    &expected_local as *const u32 as *const std::ffi::c_void,
                    std::mem::size_of::<u32>(),
                    ms,
                )
            };
            rc != 0
        }
        #[cfg(target_os = "macos")]
        {
            if let Some(wait_fn) = os_sync_dyn::wait_timeout() {
                let flags = if _cross_process {
                    libc::OS_SYNC_WAIT_ON_ADDRESS_SHARED
                } else {
                    libc::OS_SYNC_WAIT_ON_ADDRESS_NONE
                };
                let rc = unsafe {
                    wait_fn(
                        atomic.as_ptr() as *mut libc::c_void,
                        expected as u64,
                        std::mem::size_of::<u32>(),
                        flags,
                        libc::OS_CLOCK_MACH_ABSOLUTE_TIME,
                        timeout.as_nanos().min(u64::MAX as u128) as u64,
                    )
                };
                if rc < 0 {
                    return std::io::Error::last_os_error().raw_os_error()
                        != Some(libc::ETIMEDOUT);
                }
                true
            } else {
                // Older macOS (< 14.4): poll with a deadline, like the generic
                // fallback. Returns true if the value changed, false on timeout.
                let deadline = std::time::Instant::now() + timeout;
                loop {
                    if atomic.load(std::sync::atomic::Ordering::Acquire) != expected {
                        return true;
                    }
                    if std::time::Instant::now() >= deadline {
                        return atomic.load(std::sync::atomic::Ordering::Acquire) != expected;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "android",
                      target_os = "freebsd", target_os = "macos", windows)))]
        {
            let deadline = std::time::Instant::now() + timeout;
            let step = Duration::from_millis(2);
            loop {
                if atomic.load(std::sync::atomic::Ordering::Acquire) != expected {
                    return true;
                }
                let now = std::time::Instant::now();
                if now >= deadline {
                    return false;
                }
                let remaining = deadline - now;
                std::thread::sleep(remaining.min(step));
            }
        }
    }

    pub fn wake_one(atomic: &AtomicU32, _cross_process: bool) {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    atomic.as_ptr(),
                    libc::FUTEX_WAKE,
                    1i32,
                );
            }
        }
        #[cfg(target_os = "freebsd")]
        {
            // val = max threads to wake; same shared (non-PRIVATE)
            // physical-address-keyed queue the waiters parked on.
            unsafe {
                libc::_umtx_op(
                    atomic.as_ptr() as *mut libc::c_void,
                    libc::UMTX_OP_WAKE,
                    1 as libc::c_ulong,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
            }
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::Threading::WakeByAddressSingle;
            unsafe {
                WakeByAddressSingle(atomic.as_ptr() as *const std::ffi::c_void);
            }
        }
        #[cfg(target_os = "macos")]
        {
            // Mirror of the wait flag: SHARED wakes waiters in any process that
            // mapped the same region. On older macOS the symbol is absent and
            // the waiter polls, so no explicit wake is needed.
            if let Some(wake_fn) = os_sync_dyn::wake() {
                let flags = if _cross_process {
                    libc::OS_SYNC_WAKE_BY_ADDRESS_SHARED
                } else {
                    libc::OS_SYNC_WAKE_BY_ADDRESS_NONE
                };
                unsafe {
                    wake_fn(
                        atomic.as_ptr() as *mut libc::c_void,
                        std::mem::size_of::<u32>(),
                        flags,
                    );
                }
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "android",
                      target_os = "freebsd", target_os = "macos", windows)))]
        {
            drop(atomic);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn anon_round_trip() {
        let waker = CrossProcessWaker::create_anon(4).expect("create");
        let token = waker.try_park(10).expect("park");
        let waker_c = Arc::new(waker);
        let waker_p = Arc::clone(&waker_c);
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            let n = waker_p.wake_up_to(15);
            assert!(n >= 1);
        });
        waker_c.wait(token, Some(Duration::from_secs(2))).expect("wake");
        h.join().unwrap();
    }

    #[test]
    fn wait_returns_immediately_if_already_woken() {
        let waker = CrossProcessWaker::create_anon(2).expect("create");
        let token = waker.try_park(5).expect("park");
        assert_eq!(waker.wake_up_to(10), 1);
        let t0 = Instant::now();
        waker.wait(token, Some(Duration::from_secs(1))).expect("ok");
        assert!(t0.elapsed() < Duration::from_millis(50),
                "wait should return fast since wake fired before wait entered");
    }

    #[test]
    fn timeout_works() {
        let waker = CrossProcessWaker::create_anon(2).expect("create");
        let token = waker.try_park(100).expect("park");
        let t0 = Instant::now();
        let err = waker.wait(token, Some(Duration::from_millis(80)));
        assert_eq!(err, Err(WakerError::Timeout));
        assert!(t0.elapsed() >= Duration::from_millis(70));
    }

    #[test]
    fn full_when_all_slots_taken() {
        let waker = CrossProcessWaker::create_anon(2).expect("create");
        let _a = waker.try_park(1).expect("park 0");
        let _b = waker.try_park(2).expect("park 1");
        assert_eq!(waker.try_park(3), Err(WakerError::Full));
    }

    #[test]
    fn release_lets_others_park() {
        let waker = CrossProcessWaker::create_anon(2).expect("create");
        let a = waker.try_park(1).expect("park 0");
        let _b = waker.try_park(2).expect("park 1");
        waker.release(a);
        let _c = waker.try_park(3).expect("re-park 0");
    }

    #[test]
    fn wake_all_drains_blocked_consumers() {
        let waker = Arc::new(CrossProcessWaker::create_anon(4).expect("create"));
        let mut handles = Vec::new();
        for target in 0..4u64 {
            let w = Arc::clone(&waker);
            let token = w.try_park(target + 1000).expect("park");
            handles.push(thread::spawn(move || {
                w.wait(token, Some(Duration::from_secs(2))).expect("woken");
            }));
        }
        thread::sleep(Duration::from_millis(20));
        assert_eq!(waker.wake_all(), 4);
        for h in handles { h.join().unwrap(); }
    }
}
