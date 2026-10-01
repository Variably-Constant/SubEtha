//! The lengths a blocking wait spins, monitor-waits and then parks, derived
//! from measured costs by the ski-rental rule, or 0.5.1's fixed lengths
//! where there are no measured costs.
//!
//! The rule, with every cost in TSC cycles (counter ticks on aarch64):
//! - B, the hold, is the park's wake latency minus the spin's. A waiter
//!   that has held its processor for B has paid what parking would have
//!   cost.
//! - S0 is the monitor's wake latency minus the spin's.
//! - A waiter spins to S0, then monitor-waits to B, where the host has a
//!   monitor that held in calibration and an invariant TSC; otherwise it
//!   spins to B. Then it parks.
//!
//! [`derive`](fn@derive) is that rule as a pure function, and [`fallback`]
//! is the plan with no measurements. Neither reads a clock or the
//! environment.

use crate::wait_instr::{MwaitxHint, UserWait};

/// The instruction family a monitor phase uses, with the state it asks
/// for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorFamily {
    /// `MONITORX` / `MWAITX` with this C-state hint.
    Mwaitx(MwaitxHint),
    /// `UMONITOR` / `UMWAIT` with this state.
    Waitpkg(UserWait),
    /// `LDAXR` / `WFE` on aarch64, which takes no hint.
    ArmWfe,
}

/// How a waiter parks, which sets the park's wake latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParkKind {
    /// A waker whose waiters all live in this process.
    Local,
    /// A file- or shm-backed waker, woken from another process.
    CrossProcess,
}

/// How busy the machine is, as a share of its logical processors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LoadBand {
    /// Under a quarter of the logical processors busy.
    UnderQuarter,
    /// A quarter to under a half busy.
    UnderHalf,
    /// A half to under three quarters busy.
    UnderThreeQuarters,
    /// Three quarters or more busy.
    Rest,
}

impl LoadBand {
    /// The band `busy` logical processors of `processors` fall in.
    pub fn of(busy: f64, processors: u32) -> LoadBand {
        let share = if processors == 0 { 1.0 } else { busy / f64::from(processors) };
        if share < 0.25 {
            LoadBand::UnderQuarter
        } else if share < 0.5 {
            LoadBand::UnderHalf
        } else if share < 0.75 {
            LoadBand::UnderThreeQuarters
        } else {
            LoadBand::Rest
        }
    }

    /// Every band, quietest first.
    pub const ALL: [LoadBand; 4] = [
        LoadBand::UnderQuarter,
        LoadBand::UnderHalf,
        LoadBand::UnderThreeQuarters,
        LoadBand::Rest,
    ];
}

/// A monitor's measured costs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonitorFacts {
    /// The family, with the hint whose measured wake was lower.
    pub family: MonitorFamily,
    /// Wake latency of a waiter monitor-waiting with that hint.
    pub wake: u64,
    /// The longest an arm took, in calibration, to return with the line
    /// unchanged and nothing ending it early. An arm that returns this
    /// fast in a wait did not hold.
    pub floor_cycles: u64,
    /// The family's timer units per TSC cycle: 1 for an `MWAITX` count and
    /// a `UMWAIT` deadline alike, as their manuals state.
    pub units_per_cycle: f64,
}

/// The measured costs one plan is derived from: one core class, one load
/// band, one park kind.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Facts {
    /// Cycles one spin round's pause takes.
    pub pause_cycles: f64,
    /// Wake latency of a waiter spinning.
    pub spin_wake: u64,
    /// Wake latency of a waiter parked in the kernel.
    pub park_wake: u64,
    /// The monitor, where the host has one that held in calibration.
    pub monitor: Option<MonitorFacts>,
    /// The TSC advances at one rate in every P-state and C-state.
    pub invariant_tsc: bool,
}

/// The environment's settings a plan honors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Overrides {
    /// `SUBETHA_NO_MONITOR_WAIT=1`: no monitor phase; the spin runs to B.
    pub no_monitor: bool,
    /// `SUBETHA_MONITOR_WAIT_CYCLES`: the monitor phase's length.
    pub monitor_cycles: Option<u64>,
    /// `SUBETHA_PRE_PARK_SPIN`: the spin phase's rounds.
    pub spin_rounds: Option<u32>,
}

/// The monitor phase of a plan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonitorPhase {
    pub family: MonitorFamily,
    /// How long the phase lasts, in TSC cycles.
    pub budget_cycles: u64,
    /// The family's timer units per TSC cycle.
    pub units_per_cycle: f64,
    /// An arm returning within this many cycles with the line unchanged
    /// did not hold; `None` in the fallback plan, which has no measured
    /// floor.
    pub guard_floor_cycles: Option<u64>,
}

/// The lengths one wait uses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Plan {
    /// Pause rounds before the monitor phase, or before the park where
    /// there is none.
    pub spin_rounds: u32,
    /// The monitor phase, where there is one.
    pub monitor: Option<MonitorPhase>,
}

/// 0.5.1's spin: 32 rounds of a try and a pause.
pub const FALLBACK_SPIN_ROUNDS: u32 = 32;

/// 0.5.1's plan: [`FALLBACK_SPIN_ROUNDS`] rounds, then `budget_cycles` on
/// the monitor where `family` names one, with `MWAITX` asking for C1 and
/// `UMWAIT` for C0.1. Overrides apply as in [`derive`](fn@derive).
pub fn fallback(family: Option<MonitorFamily>, budget_cycles: u64, overrides: &Overrides) -> Plan {
    let family = family.filter(|_| !overrides.no_monitor).map(|family| match family {
        MonitorFamily::Mwaitx(_) => MonitorFamily::Mwaitx(MwaitxHint::C1),
        MonitorFamily::Waitpkg(_) => MonitorFamily::Waitpkg(UserWait::C01),
        MonitorFamily::ArmWfe => MonitorFamily::ArmWfe,
    });
    Plan {
        spin_rounds: overrides.spin_rounds.unwrap_or(FALLBACK_SPIN_ROUNDS),
        monitor: family.map(|family| MonitorPhase {
            family,
            budget_cycles: overrides.monitor_cycles.unwrap_or(budget_cycles),
            units_per_cycle: 1.0,
            guard_floor_cycles: None,
        }),
    }
}

/// The plan the ski-rental rule gives `facts`, with `overrides` applied.
///
/// B is `park_wake - spin_wake`. With a monitor that held, an invariant
/// TSC and no override against it, the spin runs to S0 =
/// `monitor.wake - spin_wake`, in whole pause rounds rounded up, and the
/// monitor from S0 to B. Where B is no longer than S0 the monitor phase
/// would be empty, so the spin runs to B instead. Without a monitor phase
/// the spin runs to B.
pub fn derive(facts: &Facts, overrides: &Overrides) -> Plan {
    let hold = facts.park_wake.saturating_sub(facts.spin_wake);
    let rounds = |cycles: u64| -> u32 {
        if facts.pause_cycles <= 0.0 {
            return FALLBACK_SPIN_ROUNDS;
        }
        let rounds = (cycles as f64 / facts.pause_cycles).ceil();
        if rounds >= f64::from(u32::MAX) { u32::MAX } else { rounds as u32 }
    };

    let monitor = facts
        .monitor
        .filter(|_| facts.invariant_tsc && !overrides.no_monitor)
        .and_then(|monitor| {
            let s0 = monitor.wake.saturating_sub(facts.spin_wake);
            let budget = overrides.monitor_cycles.unwrap_or(hold.saturating_sub(s0));
            (budget > 0).then_some((s0, MonitorPhase {
                family: monitor.family,
                budget_cycles: budget,
                units_per_cycle: monitor.units_per_cycle,
                guard_floor_cycles: Some(monitor.floor_cycles),
            }))
        });

    let spin_rounds = overrides.spin_rounds.unwrap_or_else(|| match monitor {
        Some((s0, _)) => rounds(s0),
        None => rounds(hold),
    });
    Plan { spin_rounds, monitor: monitor.map(|(_, phase)| phase) }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Windows host's quiet unpinned figures from the brief (Ryzen 9
    /// 7900X, MGEIXTV3): spin wake 260, monitor wake 2,068, park wake
    /// 19,952, and about 60 cycles a pause.
    fn measured() -> Facts {
        Facts {
            pause_cycles: 60.0,
            spin_wake: 260,
            park_wake: 19_952,
            monitor: Some(MonitorFacts {
                family: MonitorFamily::Mwaitx(MwaitxHint::C0),
                wake: 2_068,
                floor_cycles: 1_034,
                units_per_cycle: 2.0,
            }),
            invariant_tsc: true,
        }
    }

    #[test]
    fn a_host_with_a_monitor_spins_to_s0_then_monitors_to_b() {
        let plan = derive(&measured(), &Overrides::default());
        // S0 = 2,068 - 260 = 1,808 cycles, 31 pauses of 60 rounded up.
        assert_eq!(plan.spin_rounds, 31);
        let monitor = plan.monitor.expect("a monitor phase");
        // B = 19,952 - 260 = 19,692; the monitor covers B - S0.
        assert_eq!(monitor.budget_cycles, 19_692 - 1_808);
        assert_eq!(monitor.family, MonitorFamily::Mwaitx(MwaitxHint::C0));
        assert_eq!(monitor.guard_floor_cycles, Some(1_034));
        assert_eq!(monitor.units_per_cycle, 2.0);
    }

    #[test]
    fn a_host_without_a_monitor_spins_to_b() {
        let facts = Facts { monitor: None, ..measured() };
        let plan = derive(&facts, &Overrides::default());
        assert_eq!(plan.monitor, None);
        // 19,692 / 60 = 328.2, rounded up.
        assert_eq!(plan.spin_rounds, 329);
    }

    #[test]
    fn without_an_invariant_tsc_there_is_no_monitor_phase() {
        let facts = Facts { invariant_tsc: false, ..measured() };
        let plan = derive(&facts, &Overrides::default());
        assert_eq!(plan.monitor, None);
        assert_eq!(plan.spin_rounds, 329);
    }

    #[test]
    fn a_hold_no_longer_than_s0_spins_to_the_hold_and_parks() {
        // B = 1,500 - 260 = 1,240, under S0 = 1,808.
        let facts = Facts { park_wake: 1_500, ..measured() };
        let plan = derive(&facts, &Overrides::default());
        assert_eq!(plan.monitor, None);
        assert_eq!(plan.spin_rounds, 21);
    }

    #[test]
    fn the_environment_overrides_win_over_the_measured_lengths() {
        let no_monitor = derive(&measured(), &Overrides { no_monitor: true, ..Overrides::default() });
        assert_eq!(no_monitor.monitor, None);
        assert_eq!(no_monitor.spin_rounds, 329, "the spin runs to B");

        let pure_park = derive(&measured(), &Overrides { no_monitor: true, spin_rounds: Some(0), ..Overrides::default() });
        assert_eq!(pure_park, Plan { spin_rounds: 0, monitor: None });

        let fixed_tier = derive(&measured(), &Overrides { monitor_cycles: Some(90_000), ..Overrides::default() });
        assert_eq!(fixed_tier.monitor.expect("a monitor phase").budget_cycles, 90_000);
        assert_eq!(fixed_tier.spin_rounds, 31);
    }

    #[test]
    fn the_fallback_is_the_fixed_ladder_with_c1_and_c01() {
        let none = Overrides::default();
        let mwaitx = fallback(Some(MonitorFamily::Mwaitx(MwaitxHint::C0)), 90_000, &none);
        assert_eq!(mwaitx.spin_rounds, 32);
        let phase = mwaitx.monitor.expect("a monitor phase");
        assert_eq!(phase.family, MonitorFamily::Mwaitx(MwaitxHint::C1));
        assert_eq!(phase.budget_cycles, 90_000);
        assert_eq!(phase.guard_floor_cycles, None);

        let waitpkg = fallback(Some(MonitorFamily::Waitpkg(UserWait::C02)), 90_000, &none);
        assert_eq!(waitpkg.monitor.expect("a monitor phase").family, MonitorFamily::Waitpkg(UserWait::C01));

        assert_eq!(fallback(None, 90_000, &none), Plan { spin_rounds: 32, monitor: None });
        let off = Overrides { no_monitor: true, ..none };
        assert_eq!(fallback(Some(MonitorFamily::ArmWfe), 90_000, &off).monitor, None);
    }

    #[test]
    fn load_bands_split_the_machine_into_quarters() {
        assert_eq!(LoadBand::of(0.0, 24), LoadBand::UnderQuarter);
        assert_eq!(LoadBand::of(5.99, 24), LoadBand::UnderQuarter);
        assert_eq!(LoadBand::of(6.0, 24), LoadBand::UnderHalf);
        assert_eq!(LoadBand::of(12.0, 24), LoadBand::UnderThreeQuarters);
        assert_eq!(LoadBand::of(18.0, 24), LoadBand::Rest);
        assert_eq!(LoadBand::of(24.0, 24), LoadBand::Rest);
        assert_eq!(LoadBand::of(1.0, 0), LoadBand::Rest, "no processor count reads as fully busy");
    }
}
