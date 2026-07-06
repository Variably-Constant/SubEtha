//! The atomic control table: the lock-free bridge between the slow
//! sensor/controller loop and the fast per-packet data path.
//!
//! A controller running on its own cadence (sensor polling, loss
//! estimation) publishes its decisions here with Relaxed stores. The
//! hot path reads a single field with one Relaxed load and branches to
//! the minimal coding work for the current level - no locks, no
//! syscalls, no allocation. This is what makes the adaptive machinery
//! "consulted as needed", never run per packet.
//!
//! All fields are `u8` so each read/write is a single atomic
//! instruction. Relaxed ordering is correct here: the control values
//! are advisory tuning knobs, not data that gates memory safety, so the
//! hot path tolerates reading a value one tick stale.

use std::sync::atomic::{AtomicU8, Ordering};

/// Coding escalation level read on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CodingLevel {
    /// memcpy passthrough - clean link, zero ECC work.
    Passthrough = 0,
    /// inter-packet erasure FEC only.
    Fec = 1,
    /// FEC plus transmit interleaving for burst tolerance.
    Interleave = 2,
    /// adds intra-packet bad-FCS salvage.
    Salvage = 3,
}

impl CodingLevel {
    /// Map a raw control byte to a level (saturating at `Salvage`).
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => CodingLevel::Passthrough,
            1 => CodingLevel::Fec,
            2 => CodingLevel::Interleave,
            _ => CodingLevel::Salvage,
        }
    }
}

/// Lock-free tuning knobs shared between the controller and the data
/// path. Cheap to construct; share via `Arc`.
#[derive(Debug)]
pub struct ControlTable {
    level: AtomicU8,
    parity_r: AtomicU8,
    interleave_depth: AtomicU8,
    inner_fec: AtomicU8,
    tower_depth: AtomicU8,
}

impl Default for ControlTable {
    fn default() -> Self {
        // Defaults match a clean small-LAN link: FEC on with r=2, no
        // interleave, no salvage, no outer tower.
        Self {
            level: AtomicU8::new(CodingLevel::Fec as u8),
            parity_r: AtomicU8::new(2),
            interleave_depth: AtomicU8::new(1),
            inner_fec: AtomicU8::new(0),
            tower_depth: AtomicU8::new(0),
        }
    }
}

impl ControlTable {
    /// A fresh table at the clean-link defaults.
    pub fn new() -> Self {
        Self::default()
    }

    // --- hot-path reads (one Relaxed load each) ---

    /// Current coding escalation level.
    #[inline]
    pub fn level(&self) -> CodingLevel {
        CodingLevel::from_u8(self.level.load(Ordering::Relaxed))
    }

    /// Current FEC parity shards per block.
    #[inline]
    pub fn parity_r(&self) -> u8 {
        self.parity_r.load(Ordering::Relaxed)
    }

    /// Current interleave depth (1 = no interleaving).
    #[inline]
    pub fn interleave_depth(&self) -> u8 {
        self.interleave_depth.load(Ordering::Relaxed).max(1)
    }

    /// Whether the intra-packet salvage code is engaged.
    #[inline]
    pub fn inner_fec(&self) -> bool {
        self.inner_fec.load(Ordering::Relaxed) != 0
    }

    /// Outer-tower rung depth (0 = block code only).
    #[inline]
    pub fn tower_depth(&self) -> u8 {
        self.tower_depth.load(Ordering::Relaxed)
    }

    // --- controller-side writes (Relaxed stores) ---

    /// Publish a new coding level.
    pub fn set_level(&self, level: CodingLevel) {
        self.level.store(level as u8, Ordering::Relaxed);
    }

    /// Publish a new parity count.
    pub fn set_parity_r(&self, r: u8) {
        self.parity_r.store(r, Ordering::Relaxed);
    }

    /// Publish a new interleave depth (clamped to at least 1).
    pub fn set_interleave_depth(&self, d: u8) {
        self.interleave_depth.store(d.max(1), Ordering::Relaxed);
    }

    /// Engage or disengage the intra-packet salvage code.
    pub fn set_inner_fec(&self, on: bool) {
        self.inner_fec.store(on as u8, Ordering::Relaxed);
    }

    /// Publish a new outer-tower rung depth.
    pub fn set_tower_depth(&self, d: u8) {
        self.tower_depth.store(d, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn defaults_are_clean_link() {
        let t = ControlTable::new();
        assert_eq!(t.level(), CodingLevel::Fec);
        assert_eq!(t.parity_r(), 2);
        assert_eq!(t.interleave_depth(), 1);
        assert!(!t.inner_fec());
        assert_eq!(t.tower_depth(), 0);
    }

    #[test]
    fn writes_are_visible_to_reads() {
        let t = ControlTable::new();
        t.set_level(CodingLevel::Interleave);
        t.set_parity_r(4);
        t.set_interleave_depth(8);
        t.set_inner_fec(true);
        t.set_tower_depth(1);
        assert_eq!(t.level(), CodingLevel::Interleave);
        assert_eq!(t.parity_r(), 4);
        assert_eq!(t.interleave_depth(), 8);
        assert!(t.inner_fec());
        assert_eq!(t.tower_depth(), 1);
    }

    #[test]
    fn interleave_depth_floor_is_one() {
        let t = ControlTable::new();
        t.set_interleave_depth(0);
        assert_eq!(t.interleave_depth(), 1);
    }

    #[test]
    fn shared_across_threads() {
        // Controller thread writes; data-path thread reads. The read
        // must always see a valid level (never a torn value).
        let t = Arc::new(ControlTable::new());
        let writer = {
            let t = Arc::clone(&t);
            std::thread::spawn(move || {
                for i in 0..10_000u32 {
                    t.set_level(CodingLevel::from_u8((i % 4) as u8));
                }
            })
        };
        let mut seen = 0u32;
        for _ in 0..10_000 {
            // black_box forces the load so the tear-test is real.
            if std::hint::black_box(t.level()) == CodingLevel::Passthrough {
                seen += 1;
            }
        }
        writer.join().unwrap();
        // `seen` is observed, not asserted to a value (it is a race);
        // the point is no torn read panicked the `from_u8` match.
        std::hint::black_box(seen);
    }
}
