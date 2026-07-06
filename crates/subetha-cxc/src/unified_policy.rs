//! Unified cost-function policy for the capacity-adaptive ring.
//!
//! The shape, capacity, and ordering sidecars each optimize one axis
//! blind to the others: the shape policy reads peer counts, the
//! capacity policy reads fill ratio, and neither knows that a shape
//! change multiplies the total slot inventory (an MPSC ring has one
//! sub-ring per producer) or that growing capacity relieves the fill
//! pressure the shape change just created. [`UnifiedPolicy`] scores
//! every reachable `(shape, capacity)` configuration with ONE cost
//! function and descends greedily to the cheapest, emitting a single
//! [`RingConfig`] compound move that changes both axes at once.
//!
//! The unification that matters is shape-capacity COUPLING: the
//! number of sub-rings a shape allocates multiplies the total slot
//! inventory, so a shape change silently changes the fill ratio the
//! capacity decision depends on. Two independent policies cannot see
//! this - the shape policy morphs SPSC -> MPSC, the total slots jump
//! Nx, the fill ratio collapses, and the capacity policy thrashes
//! chasing the perturbed signal. The unified policy folds both into
//! one cost and emits one compound move.
//!
//! The cost balances three forces:
//!
//! - **footprint** - total allocated slots (`capacity * n_sub_rings`);
//!   smaller is cheaper on memory.
//! - **fill pressure** - how close the candidate runs to backpressure
//!   under the current item count; a convex penalty so near-full is
//!   sharply punished. Pulls opposite footprint: more total slots
//!   relieve fill but cost memory, and the argmin sits where the
//!   marginal relief equals the marginal memory cost. Because fill is
//!   computed against `capacity * n_sub_rings`, the shape's sub-ring
//!   multiplication enters the SAME term the capacity is chosen on -
//!   this is the coupling the independent policies miss.
//! - **shape overhead** - a fixed per-op structural cost ordering the
//!   valid shapes (SPSC cheapest, then MPSC, MPMC, Vyukov) so the
//!   policy prefers the simplest shape that fits the peer counts when
//!   footprint and fill tie. Shape is driven by peer counts (a hard
//!   validity constraint) plus this throughput preference; it is NOT
//!   driven by the observed inversion rate - inversions are harmless
//!   unless the application DECLARES a global-ordering requirement,
//!   and that declaration is the ordering sidecar's axis, kept
//!   separate here.
//! - **transition** - the morph cost from the current config, warm-
//!   aware: a capacity already in the wrapper's warm cache morphs
//!   ~100x cheaper than a cold build, and an in-place shape morph is
//!   microsecond-scale. Acts as switching hysteresis - a move happens
//!   only when its steady-state saving beats its transition price.
//!
//! Locale is NOT a load-driven axis either: cross-process visibility
//! and persistence are application declarations, not properties a
//! throughput observation can discover. The unified policy holds the
//! locale fixed at whatever the ring was constructed with and never
//! migrates it autonomously. The candidate space is `valid_shapes x
//! {capacity/2, capacity, capacity*2}` - small and exhaustively
//! scorable every scan tick.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::adaptive_ring::RingShape;
use crate::capacity_adaptive_ring::{CapacityAdaptiveRing, RingConfig};
use crate::policy_gate::{ConfidenceGate, GateConfig};

/// Weights on the cost terms. Defaults are seeded from the
/// warm-backing and compound-morph measurements: a cold capacity
/// morph is the expensive operation (millisecond-scale), so
/// `transition` carries real switching hysteresis; backpressure
/// hurts throughput more than memory does, so `fill` outweighs
/// `footprint`.
#[derive(Debug, Clone, Copy)]
pub struct UnifiedWeights {
    pub w_footprint: f64,
    pub w_fill: f64,
    /// Weight on the per-op structural shape overhead (prefer the
    /// simplest shape that fits the peers).
    pub w_shape: f64,
    pub w_transition: f64,
}

impl Default for UnifiedWeights {
    fn default() -> Self {
        Self {
            w_footprint: 1.0,
            w_fill: 3.0,
            w_shape: 1.0,
            w_transition: 2.0,
        }
    }
}

/// What the unified sidecar samples each scan and hands to
/// [`UnifiedPolicy::score`] / [`UnifiedPolicy::decide`].
#[derive(Debug, Clone, Copy)]
pub struct UnifiedObservation {
    pub current_shape: RingShape,
    pub current_capacity: usize,
    pub active_producers: usize,
    pub active_consumers: usize,
    /// Approximate item count across every sub-ring right now.
    pub approx_len: usize,
    /// Capacity currently held in the ring's warm cache, if any -
    /// a candidate at this capacity morphs warm (cheap).
    pub warm_capacity: Option<usize>,
}

/// Number of sub-rings a shape allocates for the given peer counts.
/// SPSC and Vyukov are single-ring; MPSC is one ring per producer;
/// MPMC is one ring per producer-consumer pair.
fn n_sub_rings(shape: RingShape, producers: usize, consumers: usize) -> usize {
    let p = producers.max(1);
    let c = consumers.max(1);
    match shape {
        RingShape::Spsc | RingShape::Vyukov => 1,
        RingShape::Mpsc => p,
        RingShape::Mpmc => p * c,
    }
}

/// Whether a shape can host the given peer counts.
fn shape_valid(shape: RingShape, producers: usize, consumers: usize) -> bool {
    match shape {
        RingShape::Spsc => producers <= 1 && consumers <= 1,
        RingShape::Mpsc => consumers <= 1,
        RingShape::Mpmc | RingShape::Vyukov => true,
    }
}

/// Greedy cost-descent policy over the `(shape, capacity)` space.
pub struct UnifiedPolicy {
    pub weights: UnifiedWeights,
    pub min_capacity: usize,
    pub max_capacity: usize,
}

impl Default for UnifiedPolicy {
    fn default() -> Self {
        Self {
            weights: UnifiedWeights::default(),
            min_capacity: 64,
            max_capacity: 65536,
        }
    }
}

impl UnifiedPolicy {
    /// Steady-state cost of running `(shape, capacity)` under `obs`,
    /// excluding any transition cost. Lower is better. Callers never
    /// compare costs across different `obs`.
    pub fn score(&self, shape: RingShape, capacity: usize, obs: &UnifiedObservation) -> f64 {
        let p = obs.active_producers;
        let c = obs.active_consumers;
        let n_sub = n_sub_rings(shape, p, c);
        let total_slots = (capacity * n_sub) as f64;

        // footprint: allocated slots, normalized so the term sits in
        // (0, 1] across the candidate space.
        let max_slots = (self.max_capacity * p.max(1) * c.max(1)).max(1) as f64;
        let footprint = total_slots / max_slots;

        // fill pressure: convex in the predicted fill ratio. Computed
        // against total_slots (capacity * n_sub_rings), so the shape's
        // sub-ring multiplication enters the same term the capacity is
        // chosen on - the coupling independent policies miss.
        let fill = (obs.approx_len as f64 / total_slots.max(1.0)).clamp(0.0, 1.0);
        let fill_pressure = fill * fill;

        // shape overhead: a per-op structural cost so the cheapest
        // shape that fits the peers is preferred when footprint and
        // fill tie. SPSC has no CAS; MPSC/MPMC give each producer its
        // own sub-ring (coordination-free); Vyukov serializes ALL
        // producers on one tail, so its CAS-contention cost grows
        // with the producer count - which is why MPSC wins for many
        // producers even though Vyukov's single ring is cheaper on
        // memory. Vyukov is only justified by a declared global-
        // ordering requirement, which is the ordering sidecar's
        // separate axis, not modeled here.
        let shape_overhead = match shape {
            RingShape::Spsc => 0.0,
            RingShape::Mpsc => 0.05,
            RingShape::Mpmc => 0.10,
            RingShape::Vyukov => 0.10 + 0.08 * p.saturating_sub(1) as f64,
        };

        self.weights.w_footprint * footprint
            + self.weights.w_fill * fill_pressure
            + self.weights.w_shape * shape_overhead
    }

    /// Transition cost from the current config to a candidate,
    /// warm-aware. Zero when nothing changes; an in-place shape morph
    /// is cheap; a cold capacity build is the expensive operation; a
    /// warm-cached capacity is ~100x cheaper than cold.
    fn transition_cost(
        &self,
        shape: RingShape,
        capacity: usize,
        obs: &UnifiedObservation,
    ) -> f64 {
        let shape_changed = shape != obs.current_shape;
        let cap_changed = capacity != obs.current_capacity;
        let shape_term = if shape_changed { 0.01 } else { 0.0 };
        let cap_term = if cap_changed {
            if obs.warm_capacity == Some(capacity) { 0.02 } else { 1.0 }
        } else {
            0.0
        };
        shape_term + cap_term
    }

    /// Candidate capacities: ladder-adjacent to the current one,
    /// clamped to `[min, max]`, deduplicated. Greedy descent walks
    /// one ladder step per tick.
    fn candidate_capacities(&self, current: usize) -> Vec<usize> {
        let mut out = Vec::with_capacity(3);
        for cap in [current / 2, current, current.saturating_mul(2)] {
            let cap = cap.clamp(self.min_capacity, self.max_capacity);
            if cap.is_power_of_two() && !out.contains(&cap) {
                out.push(cap);
            }
        }
        out
    }

    /// The configuration with the lowest total cost (steady-state +
    /// transition). `None` when the current config already wins -
    /// nothing to move. Otherwise a [`RingConfig`] with exactly the
    /// changed axes set.
    pub fn decide(&self, obs: &UnifiedObservation) -> Option<RingConfig> {
        let (best_shape, best_cap) = self.best_config(obs)?;
        if best_shape == obs.current_shape && best_cap == obs.current_capacity {
            return None;
        }
        Some(RingConfig {
            shape: (best_shape != obs.current_shape).then_some(best_shape),
            capacity: (best_cap != obs.current_capacity).then_some(best_cap),
            locale: None,
        })
    }

    /// The argmin `(shape, capacity)` over the candidate space,
    /// including the transition cost from the current config (so a
    /// marginal steady-state gain that does not beat the morph price
    /// stays put). `None` only when no shape is valid for the peers.
    pub fn best_config(&self, obs: &UnifiedObservation) -> Option<(RingShape, usize)> {
        let shapes = [
            RingShape::Spsc,
            RingShape::Mpsc,
            RingShape::Mpmc,
            RingShape::Vyukov,
        ];
        let caps = self.candidate_capacities(obs.current_capacity);
        let mut best: Option<((RingShape, usize), f64)> = None;
        for &shape in &shapes {
            if !shape_valid(shape, obs.active_producers, obs.active_consumers) {
                continue;
            }
            for &cap in &caps {
                let cost = self.score(shape, cap, obs)
                    + self.weights.w_transition * self.transition_cost(shape, cap, obs);
                let better = match best {
                    None => true,
                    Some((_, bc)) => cost < bc,
                };
                if better {
                    best = Some(((shape, cap), cost));
                }
            }
        }
        best.map(|(cfg, _)| cfg)
    }
}

/// Background scanner that drives one [`CapacityAdaptiveRing`] from a
/// [`UnifiedPolicy`] via [`CapacityAdaptiveRing::morph_to_config`].
///
/// Each scan: build the observation, ask the policy, prewarm a
/// repeated capacity target off the morph lock (so the eventual move
/// is warm), gate the decision through a [`ConfidenceGate`], and
/// execute the gated compound move. The gate keys on the recommended
/// `(shape, capacity)` DESTINATION - a repeated identical target
/// accrues conviction - and is shocked on a peer-count change.
pub struct UnifiedSidecar {
    handle: Option<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    morphs_triggered: Arc<AtomicU64>,
    prewarms_issued: Arc<AtomicU64>,
    /// Sum of scan-tick scoring durations in nanoseconds, plus a
    /// count, for average decision-overhead reporting.
    scan_ns_total: Arc<AtomicU64>,
    scan_count: Arc<AtomicU64>,
}

impl UnifiedSidecar {
    /// Spawn a unified sidecar. `gate_cfg` damps decisions; pass
    /// `GateConfig::default()` (disabled) for raw greedy descent or
    /// an enabled config to require conviction before each move.
    pub fn spawn(
        ring: Arc<CapacityAdaptiveRing>,
        policy: UnifiedPolicy,
        scan_interval: Duration,
        gate_cfg: GateConfig,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let morphs_triggered = Arc::new(AtomicU64::new(0));
        let prewarms_issued = Arc::new(AtomicU64::new(0));
        let scan_ns_total = Arc::new(AtomicU64::new(0));
        let scan_count = Arc::new(AtomicU64::new(0));

        let stop_c = Arc::clone(&stop);
        let morphs_c = Arc::clone(&morphs_triggered);
        let prewarms_c = Arc::clone(&prewarms_issued);
        let scan_ns_c = Arc::clone(&scan_ns_total);
        let scan_ct_c = Arc::clone(&scan_count);
        let handle = std::thread::spawn(move || {
            let mut gate: ConfidenceGate<(RingShape, usize)> = ConfidenceGate::new(gate_cfg);
            let mut last_peers = (0usize, 0usize);
            let mut last_target: Option<usize> = None;
            let mut first = true;
            while !stop_c.load(Ordering::Acquire) {
                let active = ring.ring_handle();
                let peers = (active.active_producers(), active.active_consumers());
                let approx_len = active.approx_len();
                let current_shape = active.current_shape();
                drop(active);

                if !first && peers != last_peers {
                    gate.shock();
                }
                last_peers = peers;
                first = false;

                let obs = UnifiedObservation {
                    current_shape,
                    current_capacity: ring.current_capacity(),
                    active_producers: peers.0,
                    active_consumers: peers.1,
                    approx_len,
                    warm_capacity: ring.warm_capacity(),
                };

                let t = Instant::now();
                let decision = policy.decide(&obs);
                scan_ns_c.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                scan_ct_c.fetch_add(1, Ordering::Relaxed);

                // Prewarm a repeated capacity target off the morph
                // lock so the eventual move is warm. Only on a
                // sustained target (two scans agreeing) to avoid
                // thrashing the one-slot warm cache.
                match decision.as_ref().and_then(|cfg| cfg.capacity) {
                    Some(target_cap) if target_cap != obs.current_capacity => {
                        if last_target == Some(target_cap)
                            && ring.warm_capacity() != Some(target_cap)
                            && ring.prewarm(target_cap).is_ok()
                        {
                            prewarms_c.fetch_add(1, Ordering::Relaxed);
                        }
                        last_target = Some(target_cap);
                    }
                    _ => last_target = None,
                }

                let dest_key = decision.as_ref().map(|cfg| {
                    (
                        cfg.shape.unwrap_or(obs.current_shape),
                        cfg.capacity.unwrap_or(obs.current_capacity),
                    )
                });
                if gate.observe(dest_key).is_some()
                    && let Some(cfg) = decision
                    && ring.morph_to_config(&cfg).is_ok()
                {
                    morphs_c.fetch_add(1, Ordering::Relaxed);
                }

                std::thread::sleep(scan_interval);
            }
        });

        Self {
            handle: Some(handle),
            stop,
            morphs_triggered,
            prewarms_issued,
            scan_ns_total,
            scan_count,
        }
    }

    pub fn morphs_triggered(&self) -> u64 {
        self.morphs_triggered.load(Ordering::Relaxed)
    }

    pub fn prewarms_issued(&self) -> u64 {
        self.prewarms_issued.load(Ordering::Relaxed)
    }

    /// Average scan-tick scoring time in nanoseconds (decision
    /// overhead). `0` before the first scan.
    pub fn avg_scan_ns(&self) -> u64 {
        let n = self.scan_count.load(Ordering::Relaxed);
        self.scan_ns_total
            .load(Ordering::Relaxed)
            .checked_div(n)
            .unwrap_or(0)
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            h.join().ok();
        }
    }
}

impl Drop for UnifiedSidecar {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            h.join().ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(
        shape: RingShape,
        cap: usize,
        p: usize,
        c: usize,
        len: usize,
    ) -> UnifiedObservation {
        UnifiedObservation {
            current_shape: shape,
            current_capacity: cap,
            active_producers: p,
            active_consumers: c,
            approx_len: len,
            warm_capacity: None,
        }
    }

    #[test]
    fn single_producer_light_load_prefers_spsc() {
        let policy = UnifiedPolicy::default();
        // Start oversized + wrong shape; the argmin should pick SPSC
        // (the cheapest shape valid for 1P/1C).
        let o = obs(RingShape::Mpmc, 1024, 1, 1, 2);
        let (shape, cap) = policy.best_config(&o).unwrap();
        assert_eq!(shape, RingShape::Spsc, "1P/1C light load wants SPSC");
        assert!(cap <= 1024, "and no bigger than needed");
    }

    #[test]
    fn multi_producer_heavy_load_moves_both_axes() {
        let policy = UnifiedPolicy::default();
        // 4 producers, 1 consumer, nearly full at the current cap:
        // SPSC is invalid, and the fill pressure demands growth.
        let o = obs(RingShape::Spsc, 256, 4, 1, 1000);
        let cfg = policy.decide(&o).expect("a move is needed");
        assert!(cfg.shape.is_some(), "shape must change off SPSC");
        assert_eq!(cfg.shape.unwrap(), RingShape::Mpsc, "4P/1C wants MPSC");
        assert_eq!(cfg.capacity, Some(512), "and one ladder step up under pressure");
    }

    #[test]
    fn multi_producer_prefers_mpsc_over_vyukov_at_every_fill() {
        let policy = UnifiedPolicy::default();
        // 4P/1C: MPSC's four sub-rings win on throughput (Vyukov
        // serializes all four producers on one tail). This must hold
        // at HIGH fill (where MPSC's 4x slots also relieve pressure)
        // AND at LOW fill (where Vyukov's single ring is cheaper on
        // memory but its producer-scaled CAS contention still loses).
        for len in [10usize, 800, 1900] {
            let o = obs(RingShape::Mpsc, 512, 4, 1, len);
            assert_eq!(policy.best_config(&o).unwrap().0, RingShape::Mpsc,
                       "4P throughput favors MPSC at fill len={len}, not Vyukov");
        }
    }

    #[test]
    fn transition_hysteresis_holds_marginal_gains() {
        let policy = UnifiedPolicy::default();
        // At a near-optimal config, a candidate one ladder step away
        // whose steady-state saving is tiny must NOT beat the cold
        // morph price - decide returns None (stay put).
        let o = obs(RingShape::Spsc, 64, 1, 1, 4);
        assert!(policy.decide(&o).is_none(),
                "a marginal gain below the morph price stays put");
    }

    #[test]
    fn warm_capacity_lowers_transition_cost() {
        let policy = UnifiedPolicy::default();
        // Under real fill pressure a grow is wanted; mark the grow
        // target warm and confirm the policy still selects it (warm
        // makes the move cheaper, never more expensive).
        let mut o = obs(RingShape::Spsc, 256, 1, 1, 240);
        o.warm_capacity = Some(512);
        let cfg = policy.decide(&o).expect("grow under pressure");
        assert_eq!(cfg.capacity, Some(512));
    }

    #[test]
    fn at_optimum_decide_returns_none() {
        let policy = UnifiedPolicy::default();
        // 2P/1C, comfortably provisioned MPSC: no cheaper neighbor.
        let o = obs(RingShape::Mpsc, 256, 2, 1, 50);
        assert!(policy.decide(&o).is_none());
    }
}
