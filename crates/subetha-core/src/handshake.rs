//! Per-instance handshake header: generation + in-flight refcounts.
//!
//! The header is two cache lines wide to keep the read-mostly generation
//! field from false-sharing with the write-hot in-flight counters.

use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Layout invariant: 64-byte aligned, 128 bytes total (two cache lines).
///
/// Line 0 is read-mostly (generation + strategy tag).
/// Line 1 is write-hot (in-flight counters for the two live generations).
#[repr(C, align(64))]
pub struct HandshakeHeader {
    // Cache line 0: read-mostly.
    /// Current generation. Op entry captures this; migration bumps it.
    pub generation: AtomicU32,
    /// PIC strategy tag. Hot-path branch target. One byte semantically;
    /// stored as u32 for atomic alignment.
    pub strategy_tag: AtomicU32,
    _pad0: [u8; 56],

    // Cache line 1: write-hot.
    /// In-flight op counts, one slot per generation parity.
    /// Indexed by `generation & 1`. Op entry increments; exit decrements.
    pub in_flight: [AtomicU64; 2],
    _pad1: [u8; 48],
}

impl HandshakeHeader {
    pub const fn new() -> Self {
        Self {
            generation: AtomicU32::new(0),
            strategy_tag: AtomicU32::new(0),
            _pad0: [0; 56],
            in_flight: [AtomicU64::new(0), AtomicU64::new(0)],
            _pad1: [0; 48],
        }
    }

    /// Enter an op. Returns the captured generation, which the caller
    /// must pass to [`Self::exit_op`] to release the in-flight slot.
    ///
    /// Uses the standard RCU/epoch **double-check** pattern: load the
    /// generation, increment the matching in_flight slot, re-load the
    /// generation; retry if the generation changed between loads. This
    /// closes the race where a migration completes (bump + drain + free)
    /// after the first load but before the increment, which would
    /// otherwise leave the reader holding an in_flight slot on a freed
    /// generation.
    ///
    /// Adds ~1 cycle (a second Acquire load) on the common no-migration
    /// path. The Acquire on the in-flight fetch_add also enables the
    /// pair `state.store(Release) + fence(SeqCst) + in_flight.load(Acquire)`
    /// for primitives that need to skip wakeups when no waiters exist.
    #[inline(always)]
    pub fn enter_op(&self) -> u32 {
        loop {
            let current = self.generation.load(Ordering::Acquire);
            let slot = (current & 1) as usize;
            self.in_flight[slot].fetch_add(1, Ordering::AcqRel);
            let recheck = self.generation.load(Ordering::Acquire);
            if recheck == current {
                return current;
            }
            // Generation changed between our gen load and our in_flight
            // increment. Our increment is on the wrong slot - undo and
            // retry on the new generation.
            self.in_flight[slot].fetch_sub(1, Ordering::AcqRel);
            core::hint::spin_loop();
        }
    }

    /// Read the in-flight count for a given generation.
    ///
    /// Used by primitive coordinators to decide whether to skip wakeup
    /// after a state transition. For the safe skip-wakeup pattern,
    /// pair this with a `SeqCst` fence after the state store and use
    /// `Acquire` ordering on this load.
    #[inline]
    pub fn in_flight_count(&self, generation: u32) -> u64 {
        self.in_flight[(generation & 1) as usize].load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn exit_op(&self, captured_gen: u32) {
        self.in_flight[(captured_gen & 1) as usize].fetch_sub(1, Ordering::Release);
    }

    /// Read the current strategy tag without entering an op.
    #[inline(always)]
    pub fn tag(&self) -> u32 {
        self.strategy_tag.load(Ordering::Relaxed)
    }

    /// Bump generation only. Used for data-layout migration without
    /// changing the strategy tag.
    ///
    /// Returns the old generation so the caller can drain its in-flight slot.
    pub fn bump_generation(&self) -> u32 {
        let old_value = self.generation.load(Ordering::Acquire);
        self.generation.store(old_value.wrapping_add(1), Ordering::Release);
        old_value
    }

    /// Set the strategy tag in place. PIC-only update; does NOT bump
    /// generation. Use when the strategy change does not require any
    /// data-layout migration (e.g., switching wait strategy in a
    /// once-shot primitive).
    #[inline]
    pub fn set_tag(&self, new_tag: u32) {
        self.strategy_tag.store(new_tag, Ordering::Release);
    }

    /// Bump generation and atomically swap the strategy tag.
    ///
    /// After this returns, new ops will read the new tag; in-flight ops
    /// on the old generation continue to completion. Returns the old
    /// generation so the caller can wait on its in-flight counter.
    pub fn migrate(&self, new_tag: u32) -> u32 {
        let old_value = self.generation.load(Ordering::Acquire);
        self.strategy_tag.store(new_tag, Ordering::Relaxed);
        self.generation.store(old_value.wrapping_add(1), Ordering::Release);
        old_value
    }

    /// Wait until the given generation has zero in-flight ops.
    ///
    /// Spins. Caller is the migration coordinator and migration is rare,
    /// so spinning is acceptable here.
    pub fn drain(&self, generation: u32) {
        let slot = (generation & 1) as usize;
        while self.in_flight[slot].load(Ordering::Acquire) != 0 {
            core::hint::spin_loop();
        }
    }
}

impl Default for HandshakeHeader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_size_is_two_cache_lines() {
        assert_eq!(core::mem::size_of::<HandshakeHeader>(), 128);
        assert_eq!(core::mem::align_of::<HandshakeHeader>(), 64);
    }

    #[test]
    fn enter_exit_balances() {
        let h = HandshakeHeader::new();
        let g = h.enter_op();
        assert_eq!(g, 0);
        assert_eq!(h.in_flight[0].load(Ordering::Relaxed), 1);
        h.exit_op(g);
        assert_eq!(h.in_flight[0].load(Ordering::Relaxed), 0);
    }

    #[test]
    fn migrate_bumps_generation_and_swaps_tag() {
        let h = HandshakeHeader::new();
        assert_eq!(h.tag(), 0);
        let old = h.migrate(7);
        assert_eq!(old, 0);
        assert_eq!(h.generation.load(Ordering::Acquire), 1);
        assert_eq!(h.tag(), 7);
    }

    #[test]
    fn bump_generation_does_not_touch_tag() {
        let h = HandshakeHeader::new();
        h.set_tag(3);
        let old = h.bump_generation();
        assert_eq!(old, 0);
        assert_eq!(h.generation.load(Ordering::Acquire), 1);
        assert_eq!(h.tag(), 3, "tag must not change on generation bump");
    }

    #[test]
    fn set_tag_does_not_touch_generation() {
        let h = HandshakeHeader::new();
        h.set_tag(5);
        assert_eq!(h.tag(), 5);
        assert_eq!(h.generation.load(Ordering::Acquire), 0, "generation must not change on tag set");
    }

    #[test]
    fn drain_returns_when_in_flight_zero() {
        let h = HandshakeHeader::new();
        h.drain(0);
        let g = h.enter_op();
        h.exit_op(g);
        h.drain(0);
    }
}
