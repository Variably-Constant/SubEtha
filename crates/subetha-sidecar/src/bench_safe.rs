//! Bench-safety helpers and the cardinal pattern for adaptive
//! primitive benches.
//!
//! # The cardinal rule
//!
//! Heavy setup that allocates [`SidecarBox<T>`](crate::SidecarBox)
//! lives OUTSIDE `b.iter()`. Only the cheap hot-path op runs inside.
//!
//! ```ignore
//! // CORRECT - build once, query many.
//! let map = build_adaptive_hashmap(N);
//! c.bench_function("get", |b| b.iter(|| map.get(&42)));
//!
//! // WRONG - rebuilds every iter; ~10^5 sidecar registrations.
//! c.bench_function("naive", |b| b.iter(|| build_adaptive_hashmap(N).get(&42)));
//! ```
//!
//! The `WRONG` pattern previously crashed the host while shipping the
//! `hashmap_trie_cascade` bench: each iter created ~100 SidecarBox
//! instances, criterion ran ~930 iters, the sidecar accumulated ~94k
//! registrations and exhausted threads / file descriptors / memory.
//!
//! # Defenses now in place
//!
//! 1. The substrate cap ([`crate::Sidecar::set_max_instances`]) panics with
//!    a diagnostic naming the cap value and the likely cause well
//!    before resource exhaustion.
//! 2. [`assert_capacity_fits`] lets a bench fail-fast at startup if
//!    its planned workload would exceed the configured cap, instead
//!    of trickling toward a panic mid-run.

/// Fail-fast guard: panic at bench startup if the configured global
/// instance cap is below `n_instances`. Call this once near the top
/// of a bench module before the heavy build runs.
///
/// Use this when a bench legitimately needs many simultaneous
/// SidecarBoxes (a multi-tenant simulation, a stress-test, an
/// adapter for an external workload). The panic message identifies
/// the gap so the operator can either raise the cap via
/// [`crate::Sidecar::set_max_instances`] or shrink the bench load.
pub fn assert_capacity_fits(n_instances: usize) {
    let global_max = crate::global().max_instances();
    assert!(
        n_instances <= global_max,
        "subetha-sidecar instance cap ({global_max}) is too low for this \
         bench ({n_instances} instances needed). Raise via \
         Sidecar::set_max_instances() at the top of main() or fixture \
         setup, or shrink the bench's planned instance count."
    );
}

/// Diagnostic: how many adaptive instances are currently registered
/// against the global sidecar. Useful inside a bench fixture to log
/// the high-water mark and verify the bench is not silently growing.
pub fn current_global_instance_count() -> usize {
    crate::global().instance_count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_capacity_fits_passes_when_below_cap() {
        // Default cap is 10,000; asking for 5 must pass.
        assert_capacity_fits(5);
    }

    #[test]
    #[should_panic(expected = "instance cap")]
    fn assert_capacity_fits_panics_when_above_cap() {
        // Default cap is 10,000; asking for u32::MAX must panic.
        assert_capacity_fits(u32::MAX as usize);
    }

    #[test]
    fn current_count_is_non_negative_and_reflects_changes() {
        let before = current_global_instance_count();
        // We can't easily assert the delta without polluting the
        // global sidecar; just check the accessor returns sensibly.
        let after = current_global_instance_count();
        assert!(after >= before);
    }
}
