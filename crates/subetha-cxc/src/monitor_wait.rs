//! Monitor-based wait tier: hardware MONITOR/MWAIT-class waiting
//! between the spin tier and the kernel-park tier.
//!
//! The wait ladder this slots into:
//!
//! | Tier | Mechanism | Wait scale | Core while waiting | Producer wake cost |
//! |---|---|---|---|---|
//! | spin | `PAUSE` loop | ns | busy | free (the store) |
//! | **monitor (this module)** | `MONITORX`/`MWAITX` (AMD) or `UMONITOR`/`UMWAIT` (WAITPKG) | us, bounded | light sleep (C0.1) | free (the store) |
//! | park | futex / `_umtx_op` / `WaitOnAddress` | unbounded | released to the OS | one syscall |
//!
//! The monitor tier's two properties the other tiers lack:
//!
//! - **The producer's wake is free.** The waiter arms a hardware
//!   monitor on the slot's cache line; ANY store to that line trips
//!   it. The producer's existing state-CAS IS the wake - no syscall
//!   on the wake side, unlike every kernel-park mechanism.
//! - **Monitors are physical-address based** (AMD APM / Intel SDM
//!   MONITOR semantics), so a store from ANOTHER PROCESS that
//!   mapped the same MMF page wakes the waiter. On Windows - where
//!   `WaitOnAddress` is intra-process only - this is the first
//!   non-polling cross-process wake the substrate has.
//!
//! What it is NOT: a park. `MWAITX` / `UMWAIT` hold the core in a
//! shallow sleep state with a hardware deadline; the OS cannot
//! schedule other work there. The tier therefore takes a bounded
//! cycle budget and reports `false` on expiry so the caller
//! escalates to the kernel park.
//!
//! # Instruction facts (verified against the Linux kernel's
//! `arch/x86/include/asm/mwait.h` and the Intel SDM UMWAIT page)
//!
//! - `MONITORX`: address in `rAX`, `ECX` = extensions (0),
//!   `EDX` = hints (0). Both extension registers MUST be zero -
//!   nonzero raises #GP, and the Windows x64 ABI happily leaves
//!   argument garbage in `RCX` if the wrapper does not pin it.
//! - `MWAITX`: `EAX` = hints (0), `EBX` = max wait "expressed in SW
//!   P0 clocks; the software P0 frequency is the same as the TSC
//!   frequency", `ECX` bit 1 = enable the timer.
//! - `UMONITOR r64`: address operand.
//! - `UMWAIT r32`: register operand = control (bit 0: 1 = C0.1
//!   shallow/fast wake, 0 = C0.2 deeper; other bits #GP); implicit
//!   `EDX:EAX` = ABSOLUTE TSC deadline; wakes on monitored store,
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

/// Default monitor-tier budget in TSC cycles before escalating to
/// the kernel park: ~25-30 us on contemporary 3-3.5 GHz parts.
/// Sized to dominate a kernel park+wake round trip (single-digit
/// us) so waits that resolve quickly never pay the syscall, while
/// a genuinely idle waiter escalates to the zero-CPU park within
/// tens of microseconds.
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
    /// transition - ANY store to the line, including from another
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

/// The active per-wait budget in TSC cycles.
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
/// monitor protocol: arm the monitor FIRST, re-check the value,
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
    let deadline = read_tsc().wrapping_add(budget_cycles);
    let addr = atomic.as_ptr() as *const u8;
    loop {
        // Arm, THEN check, THEN wait - the order the hardware
        // protocol requires for lost-wake freedom.
        unsafe {
            match kind {
                MonitorWaitKind::Waitpkg => umonitor(addr),
                MonitorWaitKind::Mwaitx => monitorx(addr),
                // The aarch64 family never reaches the x86_64 body.
                MonitorWaitKind::ArmWfe => return false,
            }
        }
        if atomic.load(Ordering::Acquire) != expected {
            return true;
        }
        let now = read_tsc();
        let remaining = deadline.wrapping_sub(now);
        // wrapping_sub > i64::MAX as u64 means `now` passed the
        // deadline (the subtraction wrapped negative). remaining of
        // exactly 0 is also expiry: MWAITX with EBX = 0 and the
        // timer enabled is not a defined "wait zero cycles", so it
        // never reaches the instruction.
        if remaining == 0 || remaining > i64::MAX as u64 {
            return atomic.load(Ordering::Acquire) != expected;
        }
        unsafe {
            match kind {
                MonitorWaitKind::Waitpkg => umwait(deadline),
                MonitorWaitKind::Mwaitx => {
                    mwaitx(remaining.min(u32::MAX as u64) as u32)
                }
                MonitorWaitKind::ArmWfe => return false,
            }
        }
        if atomic.load(Ordering::Acquire) != expected {
            return true;
        }
        if read_tsc().wrapping_sub(deadline) <= i64::MAX as u64 {
            // Deadline reached or passed.
            return atomic.load(Ordering::Acquire) != expected;
        }
        // Spurious wake (interrupt tripped the monitor): re-arm.
    }
}

/// AArch64 body: `LDAXR` arms the exclusive monitor with acquire
/// semantics, the value re-check happens on the loaded result, and
/// `WFE` light-sleeps until an event - which includes ANY store to
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
            // Load-exclusive-acquire: arms the monitor AND is the
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
/// guarantees: the monitor watches the LINE, the width only
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
    let deadline = read_tsc().wrapping_add(budget_cycles);
    let addr = atomic.as_ptr() as *const u8;
    loop {
        unsafe {
            match kind {
                MonitorWaitKind::Waitpkg => umonitor(addr),
                MonitorWaitKind::Mwaitx => monitorx(addr),
                MonitorWaitKind::ArmWfe => return false,
            }
        }
        if atomic.load(Ordering::Acquire) != expected {
            return true;
        }
        let now = read_tsc();
        let remaining = deadline.wrapping_sub(now);
        if remaining == 0 || remaining > i64::MAX as u64 {
            return atomic.load(Ordering::Acquire) != expected;
        }
        unsafe {
            match kind {
                MonitorWaitKind::Waitpkg => umwait(deadline),
                MonitorWaitKind::Mwaitx => {
                    mwaitx(remaining.min(u32::MAX as u64) as u32)
                }
                MonitorWaitKind::ArmWfe => return false,
            }
        }
        if atomic.load(Ordering::Acquire) != expected {
            return true;
        }
        if read_tsc().wrapping_sub(deadline) <= i64::MAX as u64 {
            return atomic.load(Ordering::Acquire) != expected;
        }
    }
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

// ===================================================================
// Instruction wrappers. Mnemonics, not .byte soup - LLVM's
// integrated assembler accepts them without target-feature gates
// (the Linux kernel compiles the identical `asm volatile("mwaitx")`
// under clang with no -mmwaitx). Register pinning per the verified
// conventions above; ECX/EDX are explicitly zeroed for MONITORX
// because nonzero extension bits raise #GP and the Windows x64 ABI
// leaves caller garbage in RCX.
// ===================================================================

#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn monitorx(addr: *const u8) {
    unsafe {
        core::arch::asm!(
            "monitorx",
            in("rax") addr,
            in("ecx") 0u32,
            in("edx") 0u32,
            options(nostack, preserves_flags),
        );
    }
}

/// `EBX` = max wait in TSC-frequency clocks; `ECX` bit 1 enables
/// the timer; `EAX` hints 0 (C1-class shallow sleep).
///
/// RBX is reserved by LLVM for inline asm, so the timeout travels
/// in a scratch register and swaps through RBX around the
/// instruction.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn mwaitx(max_cycles: u32) {
    unsafe {
        core::arch::asm!(
            "xchg {scratch}, rbx",
            "mwaitx",
            "xchg {scratch}, rbx",
            scratch = inout(reg) max_cycles as u64 => _,
            in("eax") 0u32,
            in("ecx") 2u32,
            options(nostack, preserves_flags),
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn umonitor(addr: *const u8) {
    unsafe {
        core::arch::asm!(
            "umonitor {addr}",
            addr = in(reg) addr,
            options(nostack, preserves_flags),
        );
    }
}

/// Control bit 0 = 1 selects C0.1 (shallow, fastest wake) - this is
/// a latency tier. Implicit `EDX:EAX` carries the absolute TSC
/// deadline. CF (OS-cap expiry) is irrelevant to us: the caller's
/// loop re-checks value + deadline either way.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn umwait(deadline_tsc: u64) {
    let lo = deadline_tsc as u32;
    let hi = (deadline_tsc >> 32) as u32;
    unsafe {
        core::arch::asm!(
            "umwait {ctl:e}",
            ctl = in(reg) 1u32,
            in("eax") lo,
            in("edx") hi,
            options(nostack),
        );
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
        // Budget ~3s at 3GHz: the wake must beat it by orders of
        // magnitude.
        let changed = monitor_wait_u32(&atomic, 0, 9_000_000_000);
        let elapsed = t0.elapsed();
        h.join().expect("storer thread");
        assert!(changed, "store must wake the monitor waiter");
        assert!(elapsed < Duration::from_millis(500),
                "wake must arrive promptly, got {elapsed:?}");
    }
}
