//! Monitor-based wait tier: hardware `MONITOR`/`MWAIT`-class waiting
//! between the spin tier and the kernel-park tier.
//!
//! The wait ladder this slots into:
//!
//! | Tier | Mechanism | Wait scale | Core while waiting | Producer wake cost |
//! |---|---|---|---|---|
//! | spin | `PAUSE` loop | ns | busy | free (the store) |
//! | **monitor (this module)** | `MONITORX`/`MWAITX` (AMD) or `UMONITOR`/`UMWAIT` (WAITPKG) | us, bounded | light sleep (C0.1) | free (the store) |
//! | park | futex / `_umtx_op` / `WaitOnAddress` / a named event (Windows, cross-process) | unbounded | released to the OS | one syscall |
//!
//! The monitor tier's two properties the other tiers lack:
//!
//! - **The producer's wake is free.** The waiter arms a hardware
//!   monitor on the slot's cache line; any store to that line trips
//!   it. The producer's existing state-CAS is the wake - no syscall
//!   on the wake side, unlike every kernel-park mechanism.
//! - **Monitors are physical-address based** (AMD APM / Intel SDM
//!   `MONITOR` semantics), so a store from another process that
//!   mapped the same MMF page wakes the waiter. On Windows, where
//!   `WaitOnAddress` is intra-process only, it is the one
//!   cross-process wake that needs no kernel object; past the
//!   budget a cross-process waiter parks on a named event instead.
//!
//! What it is not: a park. `MWAITX` / `UMWAIT` hold the core in a
//! shallow sleep state with a hardware deadline; the OS cannot
//! schedule other work there. The tier therefore takes a bounded
//! cycle budget and reports `false` on expiry so the caller
//! escalates to the kernel park.
//!
//! # Instruction facts (verified against the Linux kernel's
//! `arch/x86/include/asm/mwait.h` and the Intel SDM UMWAIT page)
//!
//! - `MONITORX`: address in `rAX`, `ECX` = extensions (0),
//!   `EDX` = hints (0). Both extension registers must be zero -
//!   nonzero raises #GP, and the Windows x64 ABI happily leaves
//!   argument garbage in `RCX` if the wrapper does not pin it.
//! - `MWAITX`: `EAX` = hints (0), `EBX` = max wait "expressed in SW
//!   P0 clocks; the software P0 frequency is the same as the TSC
//!   frequency", `ECX` bit 1 = enable the timer.
//! - `UMONITOR r64`: address operand.
//! - `UMWAIT r32`: register operand = control (bit 0: 1 = C0.1
//!   shallow/fast wake, 0 = C0.2 deeper; other bits #GP); implicit
//!   `EDX:EAX` = absolute TSC deadline; wakes on monitored store,
//!   deadline, or the OS's `IA32_UMWAIT_CONTROL` cap (CF set).
//! - Detection: MWAITX = CPUID `0x8000_0001` ECX bit 29 (AMD);
//!   WAITPKG = CPUID `7.0` ECX bit 5 (Intel Tiger Lake+ / Sapphire
//!   Rapids+, AMD Zen 5+).
//!
//! Both waits can wake spuriously (interrupts trip monitors), so
//! the loop re-arms until the value changes or the budget expires.
//!
//! # Tuning
//!
//! - `SUBETHA_NO_MONITOR_WAIT=1` disables the tier (callers fall
//!   straight from spin to park).
//! - `SUBETHA_MONITOR_WAIT_CYCLES=<n>` overrides the default
//!   per-wait budget ([`DEFAULT_MONITOR_BUDGET_CYCLES`]).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::ordering::read_tsc;
use crate::wait_instr::{MwaitxHint, UserWait};
use crate::wait_plan::{MonitorFamily, MonitorPhase};

/// The fixed ladder's monitor phase in TSC cycles, and the budget of
/// [`monitor_wait_u32`] and [`monitor_wait_u64`] where
/// `SUBETHA_MONITOR_WAIT_CYCLES` is not set: about 25 to 30 us on a 3 to
/// 3.5 GHz part.
pub const DEFAULT_MONITOR_BUDGET_CYCLES: u64 = 90_000;

/// Which monitor-wait instruction family this host runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorWaitKind {
    /// Intel WAITPKG: `UMONITOR` / `UMWAIT` (also AMD Zen 5+).
    /// Preferred when both families exist: the deadline is an
    /// absolute TSC value (no u32 clamp) and the C-state hint is
    /// explicit.
    Waitpkg,
    /// AMD `MONITORX` / `MWAITX` (Excavator+, all Zen).
    Mwaitx,
    /// AArch64 `LDAXR` + `WFE`: load-exclusive arms the exclusive
    /// monitor on the line; the global monitor's Exclusive->Open
    /// transition - any store at all to the line, including from another
    /// core or process - generates the wake event with no explicit
    /// `SEV` (ARM barrier-litmus appendix). Base-ISA instructions,
    /// so every aarch64 host takes this arm; wait granularity is
    /// bounded by interrupts and the kernel's timer event stream
    /// rather than a per-wait hardware deadline, and the loop
    /// enforces the cycle budget on `CNTVCT_EL0`.
    ArmWfe,
}

struct MonitorConfig {
    kind: Option<MonitorWaitKind>,
    budget_cycles: u64,
}

fn config() -> &'static MonitorConfig {
    static CONFIG: OnceLock<MonitorConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let disabled = std::env::var_os("SUBETHA_NO_MONITOR_WAIT")
            .is_some_and(|v| v == "1");
        let budget_cycles = std::env::var("SUBETHA_MONITOR_WAIT_CYCLES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(default_budget_cycles);
        MonitorConfig {
            kind: if disabled { None } else { detect_kind() },
            budget_cycles,
        }
    })
}

/// The monitor-wait family available on this host (`None` when the
/// CPU exposes neither, when the build target is not x86_64, or
/// when `SUBETHA_NO_MONITOR_WAIT=1`). Cached after the first call.
///
/// Detection is CPUID-trusting: a hypervisor that cannot virtualize
/// the instructions hides the feature bit, and one that advertises
/// it must back it. The env kill switch is the escape hatch for a
/// host that lies.
pub fn monitor_wait_kind() -> Option<MonitorWaitKind> {
    config().kind
}

/// The fixed ladder's monitor budget in counter ticks:
/// `SUBETHA_MONITOR_WAIT_CYCLES`, or [`DEFAULT_MONITOR_BUDGET_CYCLES`]
/// (28 us of ticks on aarch64). A calibrated plan's monitor phase has a
/// length of its own, which
/// [`active_plan`](crate::wait_active::active_plan) answers.
pub fn monitor_wait_budget_cycles() -> u64 {
    config().budget_cycles
}

#[cfg(target_arch = "x86_64")]
fn detect_kind() -> Option<MonitorWaitKind> {
    use core::arch::x86_64::__cpuid;
    // WAITPKG: CPUID 7.0 ECX bit 5. Preferred over MWAITX (see
    // MonitorWaitKind docs). The max-basic-leaf check guards the
    // leaf-7 read on ancient parts.
    let max_basic = core::arch::x86_64::__cpuid_count(0, 0).eax;
    if max_basic >= 7 {
        let leaf7 = core::arch::x86_64::__cpuid_count(7, 0);
        if leaf7.ecx & (1 << 5) != 0 {
            return Some(MonitorWaitKind::Waitpkg);
        }
    }
    // MWAITX: CPUID 0x8000_0001 ECX bit 29, behind the max
    // extended leaf.
    let max_extended = __cpuid(0x8000_0000).eax;
    if max_extended >= 0x8000_0001 {
        let ext1 = __cpuid(0x8000_0001);
        if ext1.ecx & (1 << 29) != 0 {
            return Some(MonitorWaitKind::Mwaitx);
        }
    }
    None
}

#[cfg(target_arch = "aarch64")]
fn detect_kind() -> Option<MonitorWaitKind> {
    // WFE / LDAXR are base A64; no probe needed. A hint-as-NOP
    // implementation degrades the wait to a budget-bounded spin -
    // correct, just warmer.
    Some(MonitorWaitKind::ArmWfe)
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn detect_kind() -> Option<MonitorWaitKind> {
    None
}

/// Per-arch default budget targeting the same ~28 us window:
/// x86 TSCs tick at GHz rates so the constant works directly;
/// aarch64's generic timer ticks at `CNTFRQ_EL0` (24 MHz - 1 GHz),
/// so the budget derives from the reported frequency.
#[cfg(not(target_arch = "aarch64"))]
fn default_budget_cycles() -> u64 {
    DEFAULT_MONITOR_BUDGET_CYCLES
}

#[cfg(target_arch = "aarch64")]
fn default_budget_cycles() -> u64 {
    // 28 us worth of counter ticks; floor of 64 keeps a sane
    // budget even if the register reads 0 on a broken emulator.
    (crate::ordering::counter_frequency_hz() * 28 / 1_000_000).max(64)
}

/// Wait on the monitor tier until `*atomic != expected` or the
/// cycle budget expires.
///
/// Returns `true` when the value changed (the caller's condition
/// fired) and `false` when the budget expired or the tier is
/// unavailable - in both `false` cases the caller escalates to its
/// kernel park, which re-checks the value itself, so a race here
/// costs one tier transition, never a lost wake.
///
/// Lost-wake freedom within the tier comes from the hardware
/// monitor protocol: arm the monitor first, re-check the value,
/// then wait. A store that lands between the re-check and the wait
/// instruction trips the already-armed monitor and the wait
/// returns immediately.
#[inline]
pub fn monitor_wait_u32(atomic: &AtomicU32, expected: u32, budget_cycles: u64) -> bool {
    let Some(kind) = monitor_wait_kind() else {
        return false;
    };
    monitor_wait_u32_with(kind, atomic, expected, budget_cycles)
}

/// As [`monitor_wait_u32`] with the family chosen explicitly
/// (bench harnesses A/B the families; production callers use the
/// probed default).
#[cfg(target_arch = "x86_64")]
pub fn monitor_wait_u32_with(
    kind: MonitorWaitKind,
    atomic: &AtomicU32,
    expected: u32,
    budget_cycles: u64,
) -> bool {
    run_monitor_phase_u32(&fixed_phase(kind, budget_cycles), atomic, expected)
}

/// AArch64 body: `LDAXR` arms the exclusive monitor with acquire
/// semantics, the value re-check happens on the loaded result, and
/// `WFE` light-sleeps until an event - which includes any store to
/// the armed line (the global monitor's Exclusive->Open transition
/// generates the event; no `SEV` needed from the storer), an
/// interrupt, or the kernel's timer event stream tick. Spurious
/// wakes re-arm; the budget is enforced on `CNTVCT_EL0` ticks.
#[cfg(target_arch = "aarch64")]
pub fn monitor_wait_u32_with(
    kind: MonitorWaitKind,
    atomic: &AtomicU32,
    expected: u32,
    budget_cycles: u64,
) -> bool {
    if kind != MonitorWaitKind::ArmWfe {
        return false;
    }
    let deadline = read_tsc().wrapping_add(budget_cycles);
    let addr = atomic.as_ptr();
    loop {
        let cur: u32;
        unsafe {
            // Load-exclusive-acquire: arms the monitor and is the
            // value check, collapsing the x86 arm-then-check pair
            // into one instruction.
            core::arch::asm!(
                "ldaxr {v:w}, [{a}]",
                v = out(reg) cur,
                a = in(reg) addr,
                options(nostack, preserves_flags),
            );
        }
        if cur != expected {
            unsafe {
                // Hygiene: drop the exclusive reservation.
                core::arch::asm!("clrex", options(nomem, nostack, preserves_flags));
            }
            return true;
        }
        let now = read_tsc();
        if deadline.wrapping_sub(now) > i64::MAX as u64
            || deadline == now
        {
            unsafe {
                core::arch::asm!("clrex", options(nomem, nostack, preserves_flags));
            }
            return atomic.load(Ordering::Acquire) != expected;
        }
        unsafe {
            core::arch::asm!("wfe", options(nomem, nostack, preserves_flags));
        }
        if atomic.load(Ordering::Acquire) != expected {
            return true;
        }
        // Event-stream tick or interrupt: re-arm and loop.
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn monitor_wait_u32_with(
    _kind: MonitorWaitKind,
    _atomic: &AtomicU32,
    _expected: u32,
    _budget_cycles: u64,
) -> bool {
    false
}

/// As [`monitor_wait_u32`] for a 64-bit atom (ring head counters
/// and slot sequences are `AtomicU64`). Same protocol, same
/// guarantees: the monitor watches the whole line, the width only
/// affects the value re-check.
#[inline]
pub fn monitor_wait_u64(
    atomic: &std::sync::atomic::AtomicU64,
    expected: u64,
    budget_cycles: u64,
) -> bool {
    let Some(kind) = monitor_wait_kind() else {
        return false;
    };
    monitor_wait_u64_with(kind, atomic, expected, budget_cycles)
}

#[cfg(target_arch = "x86_64")]
pub fn monitor_wait_u64_with(
    kind: MonitorWaitKind,
    atomic: &std::sync::atomic::AtomicU64,
    expected: u64,
    budget_cycles: u64,
) -> bool {
    run_monitor_phase_u64(&fixed_phase(kind, budget_cycles), atomic, expected)
}

#[cfg(target_arch = "aarch64")]
pub fn monitor_wait_u64_with(
    kind: MonitorWaitKind,
    atomic: &std::sync::atomic::AtomicU64,
    expected: u64,
    budget_cycles: u64,
) -> bool {
    if kind != MonitorWaitKind::ArmWfe {
        return false;
    }
    let deadline = read_tsc().wrapping_add(budget_cycles);
    let addr = atomic.as_ptr();
    loop {
        let cur: u64;
        unsafe {
            core::arch::asm!(
                "ldaxr {v}, [{a}]",
                v = out(reg) cur,
                a = in(reg) addr,
                options(nostack, preserves_flags),
            );
        }
        if cur != expected {
            unsafe {
                core::arch::asm!("clrex", options(nomem, nostack, preserves_flags));
            }
            return true;
        }
        let now = read_tsc();
        if deadline.wrapping_sub(now) > i64::MAX as u64 || deadline == now {
            unsafe {
                core::arch::asm!("clrex", options(nomem, nostack, preserves_flags));
            }
            return atomic.load(Ordering::Acquire) != expected;
        }
        unsafe {
            core::arch::asm!("wfe", options(nomem, nostack, preserves_flags));
        }
        if atomic.load(Ordering::Acquire) != expected {
            return true;
        }
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn monitor_wait_u64_with(
    _kind: MonitorWaitKind,
    _atomic: &std::sync::atomic::AtomicU64,
    _expected: u64,
    _budget_cycles: u64,
) -> bool {
    false
}

/// Arms that returned within their phase's guard floor with the watched
/// value unchanged, since this process started.
static DID_NOT_HOLD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many monitor arms returned within their phase's guard floor with
/// the watched value unchanged, the monitor not holding. Each such arm
/// ended its phase's monitor use: the rest of that phase spun.
pub fn monitor_arms_that_did_not_hold() -> u64 {
    DID_NOT_HOLD.load(Ordering::Relaxed)
}

/// The phase the fixed-budget functions run: `kind` with 0.5.1's states,
/// C1 for `MWAITX` and C0.1 for `UMWAIT`, one timer unit per TSC cycle,
/// and no guard floor.
#[cfg_attr(not(any(target_arch = "x86_64", windows)), allow(dead_code))]
pub(crate) fn fixed_phase(kind: MonitorWaitKind, budget_cycles: u64) -> MonitorPhase {
    MonitorPhase {
        family: match kind {
            MonitorWaitKind::Mwaitx => MonitorFamily::Mwaitx(MwaitxHint::C1),
            MonitorWaitKind::Waitpkg => MonitorFamily::Waitpkg(UserWait::C01),
            MonitorWaitKind::ArmWfe => MonitorFamily::ArmWfe,
        },
        budget_cycles,
        units_per_cycle: 1.0,
        guard_floor_cycles: None,
    }
}

/// Runs `phase` on `atomic` until its value is no longer `expected` or
/// the phase's budget is spent, and answers whether the value changed.
///
/// Each arm is re-armed until the deadline, since an interrupt ends an
/// arm as a store does, and so does a timer that counts faster than the
/// TSC. An arm that returns within the phase's guard floor with the value
/// unchanged did not hold: it is counted in
/// [`monitor_arms_that_did_not_hold`] and the rest of the budget spins. A
/// remainder shorter than the guard floor spins without arming, since an
/// arm that short ends within the floor on its timer alone. A family this
/// host lacks answers `false` at once.
#[cfg_attr(
    not(any(target_arch = "x86_64", target_arch = "aarch64")),
    allow(unused_variables)
)]
pub fn run_monitor_phase_u32(phase: &MonitorPhase, atomic: &AtomicU32, expected: u32) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        run_phase(phase, atomic, || atomic.load(Ordering::Acquire) == expected)
    }
    #[cfg(target_arch = "aarch64")]
    {
        phase.family == MonitorFamily::ArmWfe
            && monitor_wait_u32_with(MonitorWaitKind::ArmWfe, atomic, expected, phase.budget_cycles)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        false
    }
}

/// As [`run_monitor_phase_u32`] for a 64-bit value.
#[cfg_attr(
    not(any(target_arch = "x86_64", target_arch = "aarch64")),
    allow(unused_variables)
)]
pub fn run_monitor_phase_u64(
    phase: &MonitorPhase,
    atomic: &std::sync::atomic::AtomicU64,
    expected: u64,
) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        run_phase(phase, atomic, || atomic.load(Ordering::Acquire) == expected)
    }
    #[cfg(target_arch = "aarch64")]
    {
        phase.family == MonitorFamily::ArmWfe
            && monitor_wait_u64_with(MonitorWaitKind::ArmWfe, atomic, expected, phase.budget_cycles)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        false
    }
}

/// The x86_64 phase loop over the line holding `line`. `unchanged`
/// answers whether the awaited store has still not landed.
#[cfg(target_arch = "x86_64")]
fn run_phase<L>(phase: &MonitorPhase, line: &L, unchanged: impl Fn() -> bool) -> bool {
    use crate::wait_instr::{Monitorx, Waitpkg, Waited};
    use core::num::NonZeroU32;

    let deadline = read_tsc().wrapping_add(phase.budget_cycles);
    // True once `now` has reached the deadline: the difference wraps
    // below i64::MAX exactly then.
    let reached = |now: u64| now.wrapping_sub(deadline) <= i64::MAX as u64;
    // Spins out the rest of the budget; true when the store lands first.
    let spin_rest = || {
        while unchanged() {
            if reached(read_tsc()) {
                return false;
            }
            core::hint::spin_loop();
        }
        true
    };
    loop {
        if !unchanged() {
            return true;
        }
        let start = read_tsc();
        if reached(start) {
            return !unchanged();
        }
        let remaining = deadline.wrapping_sub(start);
        // An arm asked for less than the guard floor ends within it on its
        // timer alone, which the check below would read as a monitor that
        // did not hold, so a remainder that short spins instead.
        if phase.guard_floor_cycles.is_some_and(|floor| remaining < floor) {
            return spin_rest();
        }
        let waited = match phase.family {
            MonitorFamily::Mwaitx(hint) => {
                let Some(token) = Monitorx::on_this_host() else {
                    return false;
                };
                let units = (remaining as f64 * phase.units_per_cycle).ceil();
                let units = if units >= f64::from(u32::MAX) { u32::MAX } else { units.max(1.0) as u32 };
                let units = NonZeroU32::new(units).unwrap_or(NonZeroU32::MIN);
                token.wait(line, &unchanged, hint, units)
            }
            MonitorFamily::Waitpkg(state) => {
                let Some(token) = Waitpkg::on_this_host() else {
                    return false;
                };
                token.wait(line, &unchanged, state, deadline)
            }
            MonitorFamily::ArmWfe => return false,
        };
        if waited == Waited::AlreadyChanged || !unchanged() {
            return true;
        }
        if let Some(floor) = phase.guard_floor_cycles
            && read_tsc().wrapping_sub(start) < floor
        {
            DID_NOT_HOLD.fetch_add(1, Ordering::Relaxed);
            return spin_rest();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn detection_runs_and_is_cached() {
        let first = monitor_wait_kind();
        let second = monitor_wait_kind();
        assert_eq!(first, second, "probe must be stable across calls");
        println!("monitor-wait kind: {first:?}, budget {} cycles",
                 monitor_wait_budget_cycles());
    }

    #[test]
    fn returns_immediately_when_value_already_differs() {
        let atomic = AtomicU32::new(7);
        // Whatever the tier support, a pre-changed value reports
        // true-or-false without sleeping the full budget.
        let t0 = Instant::now();
        let changed = monitor_wait_u32(&atomic, 5, 500_000_000);
        let elapsed = t0.elapsed();
        if monitor_wait_kind().is_some() {
            assert!(changed, "value != expected must report changed");
        } else {
            assert!(!changed, "unsupported tier reports false");
        }
        assert!(elapsed < Duration::from_millis(200),
                "must not consume the whole budget: {elapsed:?}");
    }

    #[test]
    fn budget_expiry_returns_false_when_nothing_stores() {
        if monitor_wait_kind().is_none() {
            return;
        }
        let atomic = AtomicU32::new(1);
        let t0 = Instant::now();
        // ~30M cycles = ~10ms at 3GHz: long enough to measure, short
        // enough for a test.
        let changed = monitor_wait_u32(&atomic, 1, 30_000_000);
        let elapsed = t0.elapsed();
        assert!(!changed, "no store happened; must report expiry");
        assert!(elapsed >= Duration::from_micros(500),
                "expiry must actually wait, got {elapsed:?}");
        assert!(elapsed < Duration::from_secs(2),
                "expiry must be bounded, got {elapsed:?}");
    }

    #[test]
    fn cross_thread_store_wakes_the_waiter() {
        if monitor_wait_kind().is_none() {
            return;
        }
        let atomic = Arc::new(AtomicU32::new(0));
        let waker_side = Arc::clone(&atomic);
        let h = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            waker_side.store(1, Ordering::Release);
        });
        let t0 = Instant::now();
        // Budget ~3s at 3GHz.
        let changed = monitor_wait_u32(&atomic, 0, 9_000_000_000);
        let elapsed = t0.elapsed();
        h.join().expect("storer thread");
        assert!(changed, "store must wake the monitor waiter");
        // It returned because the value changed, not because the budget
        // ran out. A tighter bound would assert how promptly the OS
        // schedules the storer thread, which under load is hundreds of
        // milliseconds and says nothing about the monitor.
        assert!(elapsed < Duration::from_secs(3),
                "wake must beat the spin budget, got {elapsed:?}");
    }
}
