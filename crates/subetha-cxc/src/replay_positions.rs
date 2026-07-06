//! `SubscriberPosition`: Aeron-inspired MMF-resident position
//! counter for resumable cross-process subscribers.
//!
//! A subscriber that consumes from a ring needs a way to checkpoint
//! "I have consumed up to position N" so that if the subscriber
//! crashes / restarts, it can resume from N rather than from the
//! ring's tail (which may have advanced past lost items).
//! `SubscriberPosition` is the standalone primitive that holds N
//! in an MMF file, surviving process restarts.
//!
//! # Why standalone
//!
//! The substrate's ring primitives (SpscRingCore, SharedRing,
//! AdaptiveRing) maintain head/tail counters internally; those
//! counters track ring slot positions, not subscriber positions.
//! `SubscriberPosition` is the caller-managed counter that bridges
//! "ring is at slot K" with "this subscriber has acknowledged up
//! to absolute position P". Callers compute the relationship
//! between K and P themselves (typically by walking the ring's
//! head pointer at startup + remembering the offset).
//!
//! # MMF residency
//!
//! The position lives in a `SharedAtomicU64` file. Two processes
//! that open the same path see the same position counter
//! atomically. This matches the substrate's MMF-resident-control
//! pattern (locale_tag, pin_generation, etc.).
//!
//! # Replay semantics
//!
//! After a restart, the subscriber:
//! 1. Reopens the SubscriberPosition file by path.
//! 2. Reads the persisted position via [`get`](SubscriberPosition::get).
//! 3. Reopens the source ring and re-attaches.
//! 4. Resumes consumption from the recorded position (caller's
//!    responsibility to map absolute position to ring slot index).
//!
//! Wraparound caveat: a regular ring's slot array is bounded; if
//! the producer outraces the subscriber's checkpoint by more than
//! ring capacity, the lost items are gone. Callers needing
//! guaranteed-no-loss replay back the source ring with a large
//! enough capacity OR snapshot positions frequently enough that
//! checkpoint-position never lags producer-position by more than
//! one ring sweep.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::shared_atomic::SharedAtomicU64;

/// MMF-backed monotonically-increasing consumer position counter.
pub struct SubscriberPosition {
    counter: Arc<SharedAtomicU64>,
}

impl SubscriberPosition {
    /// Create a new position counter at `path` initialised to
    /// `initial`.
    pub fn create(
        path: impl AsRef<Path>,
        initial: u64,
    ) -> Result<Self, std::io::Error> {
        let counter = SharedAtomicU64::create(path, initial)
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
        Ok(Self { counter: Arc::new(counter) })
    }

    /// Open an existing position counter at `path` for read/write.
    /// Used by a subscriber restart path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let counter = SharedAtomicU64::open(path)
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
        Ok(Self { counter: Arc::new(counter) })
    }

    /// Current position (Acquire load).
    pub fn get(&self) -> u64 { self.counter.load(Ordering::Acquire) }

    /// Advance the position by `by`. Returns the NEW position.
    /// Atomic; safe for one subscriber to call concurrently with
    /// another holder reading via `get`.
    pub fn advance(&self, by: u64) -> u64 {
        let prior = self.counter.fetch_add(by, Ordering::AcqRel);
        prior + by
    }

    /// Set the position to `new` unconditionally. Used by restart
    /// paths that want to reset rather than advance.
    pub fn set(&self, new: u64) {
        self.counter.store(new, Ordering::Release);
    }

    /// Compare-and-set semantics. Returns `Ok(new)` if the previous
    /// value matched `expected`; `Err(actual)` otherwise.
    pub fn compare_and_set(
        &self,
        expected: u64,
        new: u64,
    ) -> Result<u64, u64> {
        match self.counter.compare_exchange(
            expected, new, Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => Ok(new),
            Err(actual) => Err(actual),
        }
    }

    /// Clone the underlying `Arc<SharedAtomicU64>` so a second
    /// in-process holder can read the same counter cheaply.
    pub fn counter_handle(&self) -> Arc<SharedAtomicU64> {
        self.counter.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("subpos_{pid}_{nonce}_{name}.bin"));
        p
    }

    #[test]
    fn create_then_get_returns_initial() {
        let path = tmp("init");
        let pos = SubscriberPosition::create(&path, 42).expect("create");
        assert_eq!(pos.get(), 42);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn advance_returns_new_position() {
        let path = tmp("advance");
        let pos = SubscriberPosition::create(&path, 0).expect("create");
        assert_eq!(pos.advance(5), 5);
        assert_eq!(pos.advance(7), 12);
        assert_eq!(pos.get(), 12);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn set_overrides_unconditionally() {
        let path = tmp("set");
        let pos = SubscriberPosition::create(&path, 100).expect("create");
        pos.set(9999);
        assert_eq!(pos.get(), 9999);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn compare_and_set_succeeds_on_expected() {
        let path = tmp("cas_ok");
        let pos = SubscriberPosition::create(&path, 0).expect("create");
        assert_eq!(pos.compare_and_set(0, 10), Ok(10));
        assert_eq!(pos.get(), 10);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn compare_and_set_fails_on_mismatch() {
        let path = tmp("cas_fail");
        let pos = SubscriberPosition::create(&path, 5).expect("create");
        assert_eq!(pos.compare_and_set(0, 999), Err(5));
        assert_eq!(pos.get(), 5);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_after_create_sees_same_position() {
        let path = tmp("reopen");
        let a = SubscriberPosition::create(&path, 100).expect("create");
        a.advance(50);
        let b = SubscriberPosition::open(&path).expect("open");
        assert_eq!(b.get(), 150);
        b.advance(25);
        assert_eq!(a.get(), 175);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn position_survives_drop_then_reopen() {
        let path = tmp("survive_drop");
        {
            let a = SubscriberPosition::create(&path, 0).expect("create");
            a.advance(42);
            // a drops here; underlying MMF file persists.
        }
        let b = SubscriberPosition::open(&path).expect("reopen after drop");
        assert_eq!(b.get(), 42);
        std::fs::remove_file(&path).ok();
    }
}
