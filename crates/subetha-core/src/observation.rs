//! Per-instance observation ring.
//!
//! Each primitive instance owns one ring. The threads that operate on
//! the instance push small observation records into it, and the sidecar
//! pops them. Until some ring in the process is armed, a push is one
//! relaxed load of a process-global count and a branch.
//!
//! The ring is multi-producer and single-consumer: any number of threads
//! push at once, each record lands whole in a slot of its own, and one
//! consumer at a time pops. A producer never blocks; if the ring is full,
//! the push is dropped and returns `false` (sampling, not coordination).
//!
//! Each observation carries a `producer_thread_id` (a process-local
//! sequential u32 allocated lazily per-thread via [`thread_id`]). The
//! sidecar's drain folds these into `InstanceStats`' per-op-kind count
//! of distinct producer threads, which its policies read to tell one
//! producer from several.

use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering};

const RING_CAPACITY: usize = 4096;

/// One observation record: 24 bytes, 8-byte aligned, carrying a
/// per-thread sequential identifier alongside the op data.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Observation {
    /// Stable identifier for the originating primitive instance.
    pub instance_id: u32,
    /// Op kind (primitive-specific).
    pub op_kind: u16,
    /// Bit flags (contention, miss, cold path, etc.).
    pub flags: u16,
    /// Latency in raw TSC ticks.
    pub latency_ticks: u64,
    /// Process-local sequential thread id of the producer
    /// (returned by [`thread_id`]). 0 = unspecified.
    pub producer_thread_id: u32,
    /// Names the four bytes the `u64` field's alignment pads the record
    /// to, so it has no uninitialized padding. The ring does not carry
    /// it: a popped record's `_reserved` is 0.
    pub _reserved: u32,
}

impl Observation {
    pub const ZERO: Self = Self {
        instance_id: 0,
        op_kind: 0,
        flags: 0,
        latency_ticks: 0,
        producer_thread_id: 0,
        _reserved: 0,
    };
}

/// Process-local sequential thread id.
///
/// Returns the same value on every call from the current thread. The
/// id is allocated lazily on first call per thread via an atomic
/// counter - no syscalls. The first thread that calls this gets id 1
/// (id 0 is reserved for "unspecified" so the Observation default is
/// distinguishable from a real thread).
///
/// Stable across all observation pushes from the same thread; valid
/// for the lifetime of the process; not valid across forks (the child
/// keeps the parent's counter but reissues new ids to its own threads).
#[inline]
pub fn thread_id() -> u32 {
    thread_local! {
        static TID: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
    }
    TID.with(|cell| {
        let cached = cell.get();
        if cached != 0 {
            return cached;
        }
        static NEXT: AtomicU32 = AtomicU32::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        // Wrap-around: if the process spawned more than ~4 billion
        // threads we'd hit 0 again. Skip 0 to keep it as the sentinel.
        let id = if id == 0 { NEXT.fetch_add(1, Ordering::Relaxed) } else { id };
        cell.set(id);
        id
    })
}

/// Process-global count of currently-armed observation rings.
///
/// The hot-path guard [`any_observer_armed`] reads this. When it is 0 - no
/// consumer has attached a sidecar anywhere in the process, the raw-handle
/// production case for every primitive - a per-op observation guard is a
/// single relaxed load of this global plus a branch, with the actual push
/// kept out-of-line behind `#[cold]` so the op's hot path stays small
/// enough to inline into its caller. Incremented on the disarmed->armed
/// edge in [`ObservationRing::arm`];
/// decremented when an armed ring drops.
pub static ARMED_COUNT: AtomicU32 = AtomicU32::new(0);

/// True if any observation ring in the process is currently armed - one
/// relaxed load of the [`ARMED_COUNT`] global, touching no
/// primitive state (no `self`, no boxed-ring deref). The intended shape is
/// `if any_observer_armed() { self.push_<op>_cold() }` where the cold method
/// is `#[cold] #[inline(never)]`: the raw-handle hot path never reads the
/// cold boxed-ring line and never grows past its caller's inline threshold.
#[inline(always)]
pub fn any_observer_armed() -> bool {
    ARMED_COUNT.load(Ordering::Relaxed) != 0
}

/// One slot of the ring: an [`Observation`]'s fields held in atomics,
/// and the position the slot is at. A slot at position `p` takes a
/// record while its sequence is `p`, holds one once it is `p + 1`, and
/// is free for position `p + RING_CAPACITY` once the consumer has taken
/// it. The fields are written before the sequence is published and read
/// after it is seen, so no record is read while it is being written.
#[repr(C)]
struct Slot {
    sequence: AtomicU32,
    instance_id: AtomicU32,
    /// `op_kind` in the low 16 bits, `flags` in the high 16.
    kind_and_flags: AtomicU32,
    producer_thread_id: AtomicU32,
    latency_ticks: AtomicU64,
}

const _: () = assert!(core::mem::size_of::<Slot>() == core::mem::size_of::<Observation>());

/// Multi-producer, single-consumer ring of observations.
///
/// `tail` is the next position a producer claims and `head` the next one
/// the consumer pops. Producers claim a position with a compare-and-swap
/// on `tail` and never read `head`; only the consumer moves `head`.
#[repr(C, align(64))]
pub struct ObservationRing {
    head: AtomicU32,
    _pad0: [u8; 60],
    tail: AtomicU32,
    /// Producer-side gate. A ring starts disarmed; producers skip every
    /// push (no `thread_id` TLS, no struct store) until a consumer arms
    /// it via [`ObservationRing::arm`] at sidecar registration. On a raw
    /// `create()` handle no sidecar ever attaches, so the push is elided
    /// entirely. Co-located with `tail` so the per-push check reads the
    /// cache line the producer already owns.
    armed: AtomicBool,
    _pad_a: [u8; 3],
    /// Lazily-allocated heap buffer of `RING_CAPACITY` slots, null until
    /// armed. A raw `create()` handle that never attaches a sidecar never
    /// allocates the 96 KiB buffer - the dominant per-instance cost of
    /// the observation machinery. `arm()` allocates it, sets each slot's
    /// sequence to its index, and publishes the pointer before setting
    /// `armed`. Co-located with `tail`/`armed` so the producer reads gate
    /// + buffer pointer from one cache line.
    buf: AtomicPtr<Slot>,
    _pad1: [u8; 48],
}

impl ObservationRing {
    pub const fn new() -> Self {
        Self {
            head: AtomicU32::new(0),
            _pad0: [0; 60],
            tail: AtomicU32::new(0),
            armed: AtomicBool::new(false),
            _pad_a: [0; 3],
            buf: AtomicPtr::new(core::ptr::null_mut()),
            _pad1: [0; 48],
        }
    }

    /// Heap layout of the lazily-allocated observation buffer.
    #[inline]
    fn buf_layout() -> std::alloc::Layout {
        std::alloc::Layout::array::<Slot>(RING_CAPACITY)
            .expect("observation buffer layout is valid")
    }

    /// Push one observation. Returns `true` on success, `false` if the
    /// ring was full (observation dropped).
    ///
    /// If `obs.producer_thread_id` is 0 (the default), the current
    /// thread's id is stamped in automatically via [`thread_id`]. Call
    /// sites that prefer explicit attribution can pre-fill the field;
    /// the auto-stamp keeps the per-primitive push sites mechanical.
    #[inline(always)]
    pub fn push(&self, obs: Observation) -> bool {
        // Hot gate: a single relaxed load of the process-global,
        // predicted-not-taken on the raw-handle path. Crucially the actual
        // store machinery is out-of-line behind `#[cold]`, so this method's
        // hot path is just load + test + branch - small enough that callers
        // inline it instead of emitting a call per op. The caller-built `obs`
        // is unused on this path and sinks into the cold body. When no
        // consumer is armed anywhere in the process, ARMED_COUNT is 0 and
        // the per-op observation cost is effectively nil (measured: a
        // bit_vec get stays at its ~1.8 ns no-observation floor).
        if ARMED_COUNT.load(Ordering::Relaxed) == 0 {
            return false;
        }
        self.push_cold(obs)
    }

    /// Push an observation described by just its op kind and flags - the
    /// shape essentially every primitive uses. Passing scalars (not a built
    /// `Observation`) is what keeps the win whole: the struct is constructed
    /// only inside the `#[cold]` body, so the caller's hot path is a lone
    /// relaxed load + predicted branch and nothing materializes on it. This
    /// is the preferred per-op observation entry point; reserve
    /// [`push`](Self::push) for the rare site that pre-fills
    /// `latency_ticks` / `instance_id`.
    #[inline(always)]
    pub fn push_op(&self, op_kind: u16, flags: u16) -> bool {
        if ARMED_COUNT.load(Ordering::Relaxed) == 0 {
            return false;
        }
        self.push_cold(Observation { op_kind, flags, ..Observation::ZERO })
    }

    /// Out-of-line store path, reached only when some ring in the process is
    /// armed. `#[cold]` + `#[inline(never)]` keep the per-op observation
    /// machinery off every primitive's hot path; the still-cheap
    /// `self.armed` recheck confirms it is this ring that a consumer attached.
    #[cold]
    #[inline(never)]
    fn push_cold(&self, mut obs: Observation) -> bool {
        if !self.armed.load(Ordering::Relaxed) {
            return false;
        }
        // Armed implies the buffer was allocated and published before
        // `armed` was set; the Acquire load pairs with the Release store
        // in `arm`. The null guard covers the brief window where a relaxed
        // observer sees `armed` ahead of the buffer pointer.
        let buf = self.buf.load(Ordering::Acquire);
        if buf.is_null() {
            return false;
        }
        if obs.producer_thread_id == 0 {
            obs.producer_thread_id = thread_id();
        }
        let mut pos = self.tail.load(Ordering::Relaxed);
        loop {
            // SAFETY: `buf` holds `RING_CAPACITY` initialized slots for as
            // long as the ring lives, and the index is reduced modulo that.
            let slot = unsafe { &*buf.add(pos as usize % RING_CAPACITY) };
            let lead = slot.sequence.load(Ordering::Acquire).wrapping_sub(pos) as i32;
            if lead == 0 {
                // The slot is free for `pos`: claim the position.
                match self.tail.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        slot.instance_id.store(obs.instance_id, Ordering::Relaxed);
                        slot.kind_and_flags.store(
                            u32::from(obs.op_kind) | (u32::from(obs.flags) << 16),
                            Ordering::Relaxed,
                        );
                        slot.producer_thread_id.store(obs.producer_thread_id, Ordering::Relaxed);
                        slot.latency_ticks.store(obs.latency_ticks, Ordering::Relaxed);
                        slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
                        return true;
                    }
                    Err(current) => pos = current,
                }
            } else if lead < 0 {
                // The slot still holds the record from one lap back: the
                // ring is full.
                return false;
            } else {
                // Another producer claimed `pos` first.
                pos = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    /// Consumer-side pop. Single-consumer; caller must serialize.
    ///
    /// Returns `None` when the next record in order has not been
    /// published yet, including one a producer has claimed and is still
    /// writing.
    pub fn pop(&self) -> Option<Observation> {
        let buf = self.buf.load(Ordering::Acquire);
        if buf.is_null() {
            return None;
        }
        let pos = self.head.load(Ordering::Relaxed);
        // SAFETY: as in `push_cold`.
        let slot = unsafe { &*buf.add(pos as usize % RING_CAPACITY) };
        if slot.sequence.load(Ordering::Acquire) != pos.wrapping_add(1) {
            return None;
        }
        let kind_and_flags = slot.kind_and_flags.load(Ordering::Relaxed);
        let obs = Observation {
            instance_id: slot.instance_id.load(Ordering::Relaxed),
            op_kind: kind_and_flags as u16,
            flags: (kind_and_flags >> 16) as u16,
            latency_ticks: slot.latency_ticks.load(Ordering::Relaxed),
            producer_thread_id: slot.producer_thread_id.load(Ordering::Relaxed),
            _reserved: 0,
        };
        slot.sequence.store(pos.wrapping_add(RING_CAPACITY as u32), Ordering::Release);
        self.head.store(pos.wrapping_add(1), Ordering::Relaxed);
        Some(obs)
    }

    /// Arm the ring so producers begin pushing observations. Called once
    /// when a consumer (the sidecar) registers the owning instance. This
    /// lazily allocates the 96 KiB observation buffer, with each slot free
    /// for its first position, and publishes it before setting `armed`,
    /// so a raw `create()` handle that never arms pays nothing - neither
    /// the per-push work nor the buffer allocation. Until armed,
    /// [`push`](Self::push) is a single relaxed load + return. Idempotent:
    /// racing callers publish one buffer, and a caller that loses the race
    /// frees its own.
    pub fn arm(&self) {
        if self.buf.load(Ordering::Acquire).is_null() {
            let layout = Self::buf_layout();
            // SAFETY: layout has non-zero size; alloc_zeroed yields a
            // valid all-zero block, and zero is a valid value of every
            // atomic in a `Slot`.
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) } as *mut Slot;
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            // Slot `i` starts free for position `i`. The buffer is not
            // published yet, so nothing else reads these slots.
            for i in 1..RING_CAPACITY {
                // SAFETY: `i` is below the `RING_CAPACITY` slots allocated.
                unsafe { (*ptr.add(i)).sequence.store(i as u32, Ordering::Relaxed) };
            }
            // Publish. If a concurrent caller won the race, free ours.
            if self
                .buf
                .compare_exchange(
                    core::ptr::null_mut(),
                    ptr,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                unsafe { std::alloc::dealloc(ptr as *mut u8, layout); }
            }
        }
        // Transition to armed and, on the disarmed->armed edge only, bump the
        // process-global gate so the hot-path guard stops short-circuiting.
        // `swap` makes the edge atomic under racing arm() callers.
        if !self.armed.swap(true, Ordering::Release) {
            ARMED_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// True once a consumer has armed the ring.
    #[inline]
    pub fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }
}

impl Default for ObservationRing {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ObservationRing {
    fn drop(&mut self) {
        // Balance the arm() increment so the process-global gate returns to 0
        // once the last armed ring is gone. `get_mut` gives exclusive access.
        if *self.armed.get_mut() {
            ARMED_COUNT.fetch_sub(1, Ordering::Relaxed);
        }
        // Free the lazily-allocated buffer if this ring was ever armed.
        let buf = *self.buf.get_mut();
        if !buf.is_null() {
            // SAFETY: `buf` was allocated in `arm` with this exact layout
            // and is owned solely by this ring.
            unsafe { std::alloc::dealloc(buf as *mut u8, Self::buf_layout()); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_roundtrip() {
        let ring = ObservationRing::new();
        ring.arm(); // a ring only accepts pushes once a consumer arms it
        let obs = Observation { instance_id: 42, op_kind: 1, flags: 0, latency_ticks: 100, producer_thread_id: 0, _reserved: 0 };
        assert!(ring.push(obs));
        let got = ring.pop().unwrap();
        assert_eq!(got.instance_id, 42);
        assert_eq!(got.op_kind, 1);
        assert_eq!(got.latency_ticks, 100);
        // push() auto-stamped the current thread's id.
        assert_ne!(got.producer_thread_id, 0);
        assert!(ring.pop().is_none());
    }

    #[test]
    fn ring_fills_then_drops() {
        let ring = ObservationRing::new();
        ring.arm(); // a ring only accepts pushes once a consumer arms it
        let obs = Observation::ZERO;
        for _ in 0..RING_CAPACITY {
            assert!(ring.push(obs));
        }
        assert!(!ring.push(obs));
    }

    #[test]
    fn thread_id_stable_across_calls_from_same_thread() {
        let a = thread_id();
        let b = thread_id();
        assert_eq!(a, b);
        assert_ne!(a, 0, "thread_id must never return 0 (the sentinel)");
    }

    #[test]
    fn thread_id_distinct_across_threads() {
        use std::sync::mpsc;
        let main_id = thread_id();
        let (tx, rx) = mpsc::channel();
        let t1 = std::thread::spawn(move || {
            tx.send(thread_id()).unwrap();
        });
        let id1 = rx.recv().unwrap();
        t1.join().unwrap();
        assert_ne!(id1, main_id, "spawned thread must have distinct id");
        assert_ne!(id1, 0);
    }

    #[test]
    fn push_auto_stamps_thread_id_when_zero() {
        let ring = ObservationRing::new();
        ring.arm(); // a ring only accepts pushes once a consumer arms it
        let obs = Observation {
            instance_id: 1,
            op_kind: 1,
            flags: 0,
            latency_ticks: 0,
            producer_thread_id: 0,
            _reserved: 0,
        };
        ring.push(obs);
        let got = ring.pop().unwrap();
        let me = thread_id();
        assert_eq!(got.producer_thread_id, me);
    }

    #[test]
    fn disarmed_ring_drops_push_until_armed() {
        // A fresh ring is disarmed: producers skip every push so raw
        // create() handles pay nothing for observation. push returns
        // false and nothing is enqueued until a consumer arms it.
        let ring = ObservationRing::new();
        assert!(!ring.is_armed());
        assert!(!ring.push(Observation::ZERO));
        assert!(ring.pop().is_none());
        ring.arm();
        assert!(ring.is_armed());
        assert!(ring.push(Observation::ZERO));
        assert!(ring.pop().is_some());
    }

    #[test]
    fn push_preserves_explicit_thread_id() {
        let ring = ObservationRing::new();
        ring.arm(); // a ring only accepts pushes once a consumer arms it
        let obs = Observation {
            instance_id: 1,
            op_kind: 1,
            flags: 0,
            latency_ticks: 0,
            producer_thread_id: 42,
            _reserved: 0,
        };
        ring.push(obs);
        let got = ring.pop().unwrap();
        assert_eq!(got.producer_thread_id, 42, "explicit tid should not be overwritten");
    }

    #[test]
    fn every_push_from_concurrent_producers_is_popped_once() {
        // Threads sharing one primitive instance push into its one ring
        // at once. Each round fills the ring exactly, so every push has
        // room, and every push that returns true comes out once, whole.
        const THREADS: u64 = 8;
        const ROUNDS: usize = 64;
        let per_thread = RING_CAPACITY as u64 / THREADS;
        let ring = ObservationRing::new();
        ring.arm();
        for round in 0..ROUNDS {
            let start = std::sync::Barrier::new(THREADS as usize);
            let accepted: u64 = std::thread::scope(|s| {
                let pushers: Vec<_> = (0..THREADS)
                    .map(|t| {
                        let (ring, start) = (&ring, &start);
                        s.spawn(move || {
                            start.wait();
                            (0..per_thread)
                                .filter(|i| {
                                    ring.push(Observation {
                                        instance_id: t as u32,
                                        latency_ticks: (t << 32) | i,
                                        ..Observation::ZERO
                                    })
                                })
                                .count() as u64
                        })
                    })
                    .collect();
                pushers.into_iter().map(|p| p.join().unwrap()).sum()
            });
            let mut popped = std::collections::HashSet::new();
            while let Some(obs) = ring.pop() {
                assert_eq!(
                    u64::from(obs.instance_id),
                    obs.latency_ticks >> 32,
                    "round {round}: a record holds fields of two different pushes"
                );
                assert!(
                    popped.insert(obs.latency_ticks),
                    "round {round}: record {:#x} came out twice",
                    obs.latency_ticks
                );
            }
            assert_eq!(
                accepted,
                THREADS * per_thread,
                "round {round}: a push was refused with room in the ring"
            );
            assert_eq!(
                popped.len() as u64,
                accepted,
                "round {round}: {accepted} pushes returned true and {} records came out",
                popped.len()
            );
        }
    }
}
