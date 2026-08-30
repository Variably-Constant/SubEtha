//! `HolderTable` - a fixed array of claimable slots, each stamped with
//! the process holding it, so a holder that dies can be told from one
//! that is working.
//!
//! # Consumers
//!
//! The peer directory's consumer slots, the pin table behind
//! [`SharedEpochs`](crate::shared_epochs::SharedEpochs), and the
//! holders of a [`SharedArc`](crate::shared_arc::SharedArc). Each takes
//! a numbered slot, holds it, and releases it, and each has to resolve
//! the holder whose process dies without releasing - which a bare count
//! cannot, because a number cannot be asked whether it is still
//! running.
//!
//! # A view, not a mapping
//!
//! The table does not own memory. It is constructed over slots the
//! caller has already mapped, so it sits inside whatever header layout
//! that caller needs - an epoch counter beside its pins, a payload
//! beside its holders.
//!
//! # Reserve, then publish
//!
//! A slot is claimed in two steps. A caller whose payload depends on
//! shared state read at claim time must make the slot visible before
//! reading that state, or another party observes the slot as free, acts
//! on that, and is wrong an instant later.
//! [`reserve`](HolderTable::reserve) publishes the slot with no payload
//! decided; [`publish`](HolderTable::publish) then fills it in.
//! [`claim`](HolderTable::claim) does both for a caller whose payload
//! does not depend on anything read between them.
//!
//! A reader that must not act on a half-formed claim uses
//! [`try_fold`](HolderTable::try_fold), which reports that it saw a
//! reservation instead of reading past it.

use std::mem::size_of;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// A slot nothing holds.
pub const HOLDER_FREE: u64 = 0;

/// A slot claimed whose payload has not been decided yet. Held across
/// the caller's decision and never reported as a holder.
pub const HOLDER_RESERVED: u64 = u64::MAX;

/// One claimable slot: a payload word and the process holding it, on
/// its own cache line so two holders never share one.
#[repr(C, align(64))]
pub struct HolderSlot {
    /// [`HOLDER_FREE`], [`HOLDER_RESERVED`], or the caller's payload.
    pub state: AtomicU64,
    /// The process that claimed it, for the dead-holder reap.
    pub owner_pid: AtomicU32,
    _pad: [u8; 52],
}

const _: () = {
    assert!(size_of::<HolderSlot>() == 64);
};

/// Bytes `capacity` slots occupy.
pub const fn holder_table_size(capacity: usize) -> usize {
    capacity * size_of::<HolderSlot>()
}

/// A view over `capacity` [`HolderSlot`]s the caller has mapped.
pub struct HolderTable {
    base: *const HolderSlot,
    capacity: usize,
}

// The slots are atomics in shared memory; the view is a pointer and a
// length, and every access goes through an atomic.
unsafe impl Send for HolderTable {}
unsafe impl Sync for HolderTable {}

impl HolderTable {
    /// Build a view over `capacity` slots at `base`.
    ///
    /// # Safety
    /// `base` addresses at least `holder_table_size(capacity)` bytes
    /// that are mapped, 64-byte aligned, and live for as long as the
    /// view. A freshly zeroed region is already a table of free slots.
    pub unsafe fn from_ptr(base: *const u8, capacity: usize) -> Self {
        debug_assert!(
            (base as usize).is_multiple_of(64),
            "holder slots must be 64-byte aligned"
        );
        Self { base: base as *const HolderSlot, capacity }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn slot(&self, i: usize) -> &HolderSlot {
        debug_assert!(i < self.capacity);
        unsafe { &*self.base.add(i) }
    }

    /// Claim a slot without deciding its payload, returning its index.
    ///
    /// The slot is visible as reserved from the moment this returns, so
    /// a caller reading shared state to build its payload reads it
    /// after the claim is observable. Follow with
    /// [`publish`](Self::publish) or [`release`](Self::release); a
    /// reservation left standing blocks [`try_fold`](Self::try_fold)
    /// until the holder's process is found gone.
    pub fn reserve(&self) -> Option<usize> {
        let pid = std::process::id();
        for i in 0..self.capacity {
            let slot = self.slot(i);
            if slot.state.load(Ordering::Acquire) != HOLDER_FREE {
                continue;
            }
            if slot
                .state
                .compare_exchange(
                    HOLDER_FREE,
                    HOLDER_RESERVED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                slot.owner_pid.store(pid, Ordering::Release);
                return Some(i);
            }
        }
        None
    }

    /// Fill in a reserved slot's payload.
    ///
    /// # Panics
    /// If `payload` is [`HOLDER_FREE`] or [`HOLDER_RESERVED`], which
    /// would make a held slot read as free or as forever-forming.
    pub fn publish(&self, slot: usize, payload: u64) {
        assert!(
            payload != HOLDER_FREE && payload != HOLDER_RESERVED,
            "payload {payload} collides with a reserved state"
        );
        self.slot(slot).state.store(payload, Ordering::Release);
    }

    /// Claim a slot and publish `payload` in one step, for a caller
    /// whose payload does not depend on anything read between the two.
    pub fn claim(&self, payload: u64) -> Option<usize> {
        let i = self.reserve()?;
        self.publish(i, payload);
        Some(i)
    }

    /// Claim ONE named slot, or report that it is already held.
    ///
    /// [`reserve`](Self::reserve) takes whichever slot is free, which is
    /// what a caller wants when the slots are interchangeable. A caller
    /// for whom they are not - one that must hold the slot standing for
    /// a particular thing, and would be wrong holding any other - names
    /// it here instead. Returns whether the claim succeeded.
    ///
    /// # Panics
    /// If `payload` is [`HOLDER_FREE`] or [`HOLDER_RESERVED`], as
    /// [`publish`](Self::publish) does.
    pub fn try_claim_slot(&self, i: usize, payload: u64) -> bool {
        assert!(
            payload != HOLDER_FREE && payload != HOLDER_RESERVED,
            "payload {payload} collides with a reserved state"
        );
        let slot = self.slot(i);
        if slot.state.load(Ordering::Acquire) != HOLDER_FREE {
            return false;
        }
        if slot
            .state
            .compare_exchange(HOLDER_FREE, HOLDER_RESERVED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        slot.owner_pid.store(std::process::id(), Ordering::Release);
        slot.state.store(payload, Ordering::Release);
        true
    }

    /// Return a slot to the table.
    pub fn release(&self, slot: usize) {
        let s = self.slot(slot);
        s.owner_pid.store(0, Ordering::Release);
        s.state.store(HOLDER_FREE, Ordering::Release);
    }

    /// The payload a slot holds, or `None` if it is free or still
    /// forming.
    pub fn payload(&self, slot: usize) -> Option<u64> {
        match self.slot(slot).state.load(Ordering::Acquire) {
            HOLDER_FREE | HOLDER_RESERVED => None,
            v => Some(v),
        }
    }

    /// Slots held, reservations included.
    pub fn live(&self) -> usize {
        (0..self.capacity)
            .filter(|i| self.slot(*i).state.load(Ordering::Acquire) != HOLDER_FREE)
            .count()
    }

    /// Fold over every published payload, or report that a slot was
    /// mid-claim.
    ///
    /// `None` means a reservation was seen and the fold is not
    /// answerable yet - the caller decides whether to retry, to reap,
    /// or to treat it as a reason to do nothing. Reading past a
    /// reservation would mean acting on a table that is about to gain a
    /// holder.
    pub fn try_fold<T>(&self, init: T, mut f: impl FnMut(T, u64) -> T) -> Option<T> {
        let mut acc = init;
        for i in 0..self.capacity {
            match self.slot(i).state.load(Ordering::Acquire) {
                HOLDER_FREE => {}
                HOLDER_RESERVED => return None,
                v => acc = f(acc, v),
            }
        }
        Some(acc)
    }

    /// Free every slot whose holding process is gone, and report how
    /// many went.
    ///
    /// A slot outlives its process only when that process died holding
    /// it, so it names a holder that will never release. A slot claimed
    /// but not yet stamped with a pid is mid-claim, not dead, and is
    /// left alone.
    pub fn reap_dead(&self) -> usize {
        let mut freed = 0;
        for i in 0..self.capacity {
            let slot = self.slot(i);
            let state = slot.state.load(Ordering::Acquire);
            if state == HOLDER_FREE {
                continue;
            }
            let pid = slot.owner_pid.load(Ordering::Acquire);
            if pid == 0 || crate::peer_directory::process_alive(pid) {
                continue;
            }
            if slot
                .state
                .compare_exchange(state, HOLDER_FREE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                freed += 1;
            }
        }
        freed
    }
}

impl std::fmt::Debug for HolderTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HolderTable")
            .field("capacity", &self.capacity)
            .field("live", &self.live())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Slots on the heap, 64-byte aligned, standing in for a mapping.
    struct Backing {
        _buf: Vec<HolderSlot>,
        table: HolderTable,
    }

    fn backing(capacity: usize) -> Backing {
        let mut buf: Vec<HolderSlot> = (0..capacity)
            .map(|_| HolderSlot {
                state: AtomicU64::new(HOLDER_FREE),
                owner_pid: AtomicU32::new(0),
                _pad: [0; 52],
            })
            .collect();
        let table = unsafe { HolderTable::from_ptr(buf.as_mut_ptr() as *const u8, capacity) };
        Backing { _buf: buf, table }
    }

    #[test]
    fn a_claim_takes_the_first_free_slot_and_a_release_returns_it() {
        let b = backing(4);
        let t = &b.table;
        assert_eq!(t.claim(7), Some(0));
        assert_eq!(t.claim(8), Some(1));
        assert_eq!(t.payload(0), Some(7));
        assert_eq!(t.live(), 2);
        t.release(0);
        assert_eq!(t.payload(0), None);
        assert_eq!(t.claim(9), Some(0), "the released slot is reused");
    }

    #[test]
    fn a_full_table_refuses() {
        let b = backing(2);
        let t = &b.table;
        assert_eq!(t.claim(1), Some(0));
        assert_eq!(t.claim(2), Some(1));
        assert_eq!(t.claim(3), None);
        assert_eq!(t.live(), 2, "the refusal left both holders alone");
    }

    /// The ordering the whole primitive exists for: a slot is visible
    /// before its payload is decided, and a reader must not read past
    /// that.
    #[test]
    fn a_reservation_is_visible_but_not_yet_a_payload() {
        let b = backing(4);
        let t = &b.table;
        let i = t.reserve().unwrap();
        assert_eq!(t.payload(i), None, "reserved is not a published payload");
        assert_eq!(t.live(), 1, "but it does hold the slot");
        assert_eq!(
            t.try_fold(0u64, |a, v| a + v),
            None,
            "a fold must not read past a slot that is mid-claim"
        );
        t.publish(i, 42);
        assert_eq!(t.try_fold(0u64, |a, v| a + v), Some(42));
    }

    #[test]
    fn a_fold_sums_every_published_payload() {
        let b = backing(8);
        let t = &b.table;
        for p in 1..=5u64 {
            t.claim(p).unwrap();
        }
        assert_eq!(t.try_fold(0u64, |a, v| a + v), Some(15));
        assert_eq!(t.try_fold(u64::MAX, |a, v| a.min(v)), Some(1));
    }

    #[test]
    fn a_slot_whose_process_died_is_reaped() {
        let b = backing(4);
        let t = &b.table;
        t.claim(7).unwrap();
        let dead = t.slot(1);
        dead.state.store(9, Ordering::Release);
        dead.owner_pid.store(u32::MAX - 1, Ordering::Release);
        assert_eq!(t.live(), 2);
        assert_eq!(t.reap_dead(), 1);
        assert_eq!(t.live(), 1, "the live holder is untouched");
        assert_eq!(t.payload(0), Some(7));
    }

    #[test]
    fn a_reservation_whose_process_died_is_reaped_too() {
        let b = backing(2);
        let t = &b.table;
        let dead = t.slot(0);
        dead.state.store(HOLDER_RESERVED, Ordering::Release);
        dead.owner_pid.store(u32::MAX - 1, Ordering::Release);
        assert_eq!(
            t.try_fold(0u64, |a, v| a + v),
            None,
            "the fold is blocked while it stands"
        );
        assert_eq!(t.reap_dead(), 1);
        assert_eq!(t.try_fold(0u64, |a, v| a + v), Some(0), "and unblocked after");
    }

    #[test]
    #[should_panic(expected = "collides with a reserved state")]
    fn publishing_a_reserved_sentinel_is_refused() {
        let b = backing(2);
        let i = b.table.reserve().unwrap();
        b.table.publish(i, HOLDER_FREE);
    }

    #[test]
    fn concurrent_claims_never_hand_two_callers_one_slot() {
        let b = std::sync::Arc::new(backing(64));
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        std::thread::scope(|s| {
            for _ in 0..8 {
                let b = std::sync::Arc::clone(&b);
                let seen = std::sync::Arc::clone(&seen);
                s.spawn(move || {
                    let mut mine = Vec::new();
                    for p in 1..=8u64 {
                        if let Some(i) = b.table.claim(p) {
                            mine.push(i);
                        }
                    }
                    seen.lock().unwrap().extend(mine);
                });
            }
        });
        let mut all = seen.lock().unwrap().clone();
        let total = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total, "a slot was handed to two callers");
        assert_eq!(b.table.live(), total);
    }

    unsafe impl Send for Backing {}
    unsafe impl Sync for Backing {}
}
