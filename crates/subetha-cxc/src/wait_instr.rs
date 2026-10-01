//! The user-mode monitor and wait instructions, each reachable only
//! through a token that proves this host reported it.
//!
//! [`Monitorx`] and [`Waitpkg`] are made only by their `on_this_host`
//! constructors, which read [`cpuid`], and every instruction here is a
//! method on one of them. A host without a family cannot make its token
//! and so cannot reach its instructions. Each instruction increments a
//! process-wide count of its executions, which [`executions`] reads.
//!
//! The encodings follow the manuals. `MONITORX` takes the address in rAX;
//! ECX holds extensions, none are defined and any set bit raises #GP, and
//! EDX holds hints, none are defined, so both are zero (AMD APM Vol. 3,
//! MONITORX). `MWAITX` takes its C-state hint in EAX and its extensions in
//! ECX, where bit 1 enables a timer whose count is in EBX, and a zero
//! count with the timer enabled means no timer at all (AMD APM Vol. 3,
//! MWAITX). `UMWAIT` and `TPAUSE` take the requested state in bit 0 of a
//! register operand and an absolute TSC deadline in EDX:EAX, and set CF
//! when the operating system's limit ended the wait (Intel SDM, UMWAIT and
//! TPAUSE).

use core::num::NonZeroU32;
use core::sync::atomic::{AtomicU64, Ordering};

use subetha_core::cpuid::cpuid;

/// How many times each instruction has run in this process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Executions {
    pub monitorx: u64,
    pub mwaitx: u64,
    pub umonitor: u64,
    pub umwait: u64,
    pub tpause: u64,
}

/// The process-wide counts, on a line of their own, so no line a monitor
/// watches holds them and an increment is never the store that ends a
/// wait.
#[repr(align(64))]
struct Counters {
    monitorx: AtomicU64,
    mwaitx: AtomicU64,
    umonitor: AtomicU64,
    umwait: AtomicU64,
    tpause: AtomicU64,
}

static COUNTERS: Counters = Counters {
    monitorx: AtomicU64::new(0),
    mwaitx: AtomicU64::new(0),
    umonitor: AtomicU64::new(0),
    umwait: AtomicU64::new(0),
    tpause: AtomicU64::new(0),
};

/// How many times each instruction has run since this process started.
pub fn executions() -> Executions {
    Executions {
        monitorx: COUNTERS.monitorx.load(Ordering::Relaxed),
        mwaitx: COUNTERS.mwaitx.load(Ordering::Relaxed),
        umonitor: COUNTERS.umonitor.load(Ordering::Relaxed),
        umwait: COUNTERS.umwait.load(Ordering::Relaxed),
        tpause: COUNTERS.tpause.load(Ordering::Relaxed),
    }
}

/// How a wait ended, as far as the processor says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waited {
    /// The check between arming and waiting found the store had already
    /// landed, so the wait instruction did not run.
    AlreadyChanged,
    /// The wait instruction ran. It ended on a store to the line, an
    /// interrupt, its timer or deadline, or an event the processor does
    /// not name; neither vendor reports which.
    Ran,
    /// `UMWAIT` or `TPAUSE` ran and the operating system's limit ended it,
    /// which the processor reports by setting CF. `MWAITX` has no such
    /// limit and never answers this.
    OsLimit,
}

/// The C-state `MWAITX` asks for.
///
/// The manual encodes the state in EAX bits 7:4 as the state minus one, so
/// C0 is 0xF0 and C1 is 0x00, and the other bits stay zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MwaitxHint {
    /// C0, the lightest.
    C0,
    /// C1.
    C1,
}

impl MwaitxHint {
    const fn eax(self) -> u32 {
        match self {
            Self::C0 => 0xF0,
            Self::C1 => 0x00,
        }
    }
}

/// The state `UMWAIT` and `TPAUSE` are asked for, in bit 0 of their
/// register operand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserWait {
    /// C0.1: the faster wake and the smaller saving. Bit 0 set.
    C01,
    /// C0.2: the slower wake, the larger saving, and more of the core for
    /// its other hardware thread. Bit 0 clear. An operating system can
    /// forbid it through IA32_UMWAIT_CONTROL bit 0, which turns this
    /// request into C0.1.
    C02,
}

impl UserWait {
    const fn control(self) -> u32 {
        match self {
            Self::C01 => 1,
            Self::C02 => 0,
        }
    }
}

/// Proof that this host reports `MONITORX` and `MWAITX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Monitorx(());

impl Monitorx {
    /// The proof, when CPUID reports the pair, and `None` otherwise.
    pub fn on_this_host() -> Option<Self> {
        cpuid().monitorx.then_some(Self(()))
    }

    /// Arms a monitor on the line holding `watched` and, unless
    /// `unchanged` answers false, waits until a store from another
    /// processor lands on that line, an interrupt arrives, or the timer
    /// has counted `units`.
    ///
    /// `unchanged` runs between the two instructions, as the manual
    /// requires: `MWAITX` runs only if the awaited store has not already
    /// happened. A store landing after the check trips the monitor armed
    /// before it, so the wait still ends.
    ///
    /// Nothing reports why the wait ended, so the caller reads the watched
    /// value again whatever this returns. How far one unit of the count
    /// reaches is a property of the part: the manual says the timer counts
    /// TSC clocks, and a Ryzen 9 7900X measured half of one per unit.
    pub fn wait<T>(
        self,
        watched: &T,
        unchanged: impl FnOnce() -> bool,
        hint: MwaitxHint,
        units: NonZeroU32,
    ) -> Waited {
        let line = core::ptr::from_ref(watched).cast::<u8>();
        COUNTERS.monitorx.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `self` exists only where CPUID reported MONITORX, so the
        // opcode is defined. `line` comes from a live reference, and
        // MONITORX applies the checks of a one-byte read to it.
        unsafe { monitorx(line) };
        if !unchanged() {
            return Waited::AlreadyChanged;
        }
        // SAFETY: as above for MWAITX. The count is nonzero, so the timer
        // bounds the wait.
        unsafe { mwaitx(hint.eax(), units.get()) };
        COUNTERS.mwaitx.fetch_add(1, Ordering::Relaxed);
        Waited::Ran
    }
}

/// Proof that this host reports WAITPKG: `UMONITOR`, `UMWAIT` and
/// `TPAUSE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Waitpkg(());

impl Waitpkg {
    /// The proof, when CPUID reports WAITPKG, and `None` otherwise.
    pub fn on_this_host() -> Option<Self> {
        cpuid().waitpkg.then_some(Self(()))
    }

    /// Arms a monitor on the line holding `watched` and, unless
    /// `unchanged` answers false, waits until a store lands on that line,
    /// an interrupt arrives, the TSC reaches `deadline_tsc`, or the
    /// operating system's limit runs out.
    ///
    /// `unchanged` runs between arming and waiting, as in
    /// [`Monitorx::wait`].
    pub fn wait<T>(
        self,
        watched: &T,
        unchanged: impl FnOnce() -> bool,
        state: UserWait,
        deadline_tsc: u64,
    ) -> Waited {
        let line = core::ptr::from_ref(watched).cast::<u8>();
        COUNTERS.umonitor.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `self` exists only where CPUID reported WAITPKG, so the
        // opcode is defined. `line` comes from a live reference.
        unsafe { umonitor(line) };
        if !unchanged() {
            return Waited::AlreadyChanged;
        }
        // SAFETY: as above for UMWAIT. The control value has only bit 0 in
        // play, so no reserved bit is set.
        let os_limit = unsafe { umwait(state.control(), deadline_tsc) };
        COUNTERS.umwait.fetch_add(1, Ordering::Relaxed);
        if os_limit {
            Waited::OsLimit
        } else {
            Waited::Ran
        }
    }

    /// Pauses until the TSC reaches `deadline_tsc`, an interrupt arrives,
    /// or the operating system's limit runs out. No line is watched.
    pub fn pause(self, state: UserWait, deadline_tsc: u64) -> Waited {
        COUNTERS.tpause.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `self` exists only where CPUID reported WAITPKG, so the
        // opcode is defined, and only bit 0 of the control is set.
        if unsafe { tpause(state.control(), deadline_tsc) } {
            Waited::OsLimit
        } else {
            Waited::Ran
        }
    }
}

/// `MONITORX` with the address in rAX and zero extensions and hints.
///
/// # Safety
///
/// The host must report `MONITORX`, and `line` must point into mapped
/// memory.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn monitorx(line: *const u8) {
    // SAFETY: the caller's contract. MONITORX writes no register and no
    // flag.
    unsafe {
        core::arch::asm!(
            "monitorx",
            in("rax") line,
            in("ecx") 0u32,
            in("edx") 0u32,
            options(nostack, preserves_flags),
        );
    }
}

/// `MWAITX` with the timer enabled for `count` units and `eax` as the
/// C-state hint.
///
/// rbx is reserved to the compiler and cannot be named as an operand, so
/// the count is exchanged into it around the instruction and back.
///
/// # Safety
///
/// The host must report `MONITORX`, and `count` must be nonzero, since a
/// zero count with the timer enabled waits without a timer.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn mwaitx(eax: u32, count: u32) {
    // SAFETY: the caller's contract. MWAITX leaves registers and flags as
    // a NOP does, and the two exchanges restore rbx.
    unsafe {
        core::arch::asm!(
            "xchg {count}, rbx",
            "mwaitx",
            "xchg {count}, rbx",
            count = inout(reg) u64::from(count) => _,
            in("eax") eax,
            in("ecx") 2u32,
            options(nostack, preserves_flags),
        );
    }
}

/// `UMONITOR` on `line`.
///
/// # Safety
///
/// The host must report WAITPKG, and `line` must point into mapped memory.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn umonitor(line: *const u8) {
    // SAFETY: the caller's contract.
    unsafe {
        core::arch::asm!("umonitor {line}", line = in(reg) line, options(nostack));
    }
}

/// `UMWAIT` until `deadline`, answering whether the operating system's
/// limit ended it.
///
/// # Safety
///
/// The host must report WAITPKG, and `control` must set no bit above
/// bit 0.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn umwait(control: u32, deadline: u64) -> bool {
    let limit: u8;
    // SAFETY: the caller's contract. UMWAIT writes CF, which is read here
    // and so is not claimed preserved.
    unsafe {
        core::arch::asm!(
            "umwait {control:e}",
            "setc {limit}",
            control = in(reg) control,
            limit = out(reg_byte) limit,
            in("eax") deadline as u32,
            in("edx") (deadline >> 32) as u32,
            options(nostack),
        );
    }
    limit != 0
}

/// `TPAUSE` until `deadline`, answering whether the operating system's
/// limit ended it.
///
/// # Safety
///
/// The host must report WAITPKG, and `control` must set no bit above
/// bit 0.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn tpause(control: u32, deadline: u64) -> bool {
    let limit: u8;
    // SAFETY: the caller's contract, as for UMWAIT.
    unsafe {
        core::arch::asm!(
            "tpause {control:e}",
            "setc {limit}",
            control = in(reg) control,
            limit = out(reg_byte) limit,
            in("eax") deadline as u32,
            in("edx") (deadline >> 32) as u32,
            options(nostack),
        );
    }
    limit != 0
}

// Off x86_64 CPUID reports neither family, so no token is ever made and
// none of these is reached.

#[cfg(not(target_arch = "x86_64"))]
unsafe fn monitorx(_line: *const u8) {
    unreachable!("MONITORX reached without a Monitorx token");
}

#[cfg(not(target_arch = "x86_64"))]
unsafe fn mwaitx(_eax: u32, _count: u32) {
    unreachable!("MWAITX reached without a Monitorx token");
}

#[cfg(not(target_arch = "x86_64"))]
unsafe fn umonitor(_line: *const u8) {
    unreachable!("UMONITOR reached without a Waitpkg token");
}

#[cfg(not(target_arch = "x86_64"))]
unsafe fn umwait(_control: u32, _deadline: u64) -> bool {
    unreachable!("UMWAIT reached without a Waitpkg token");
}

#[cfg(not(target_arch = "x86_64"))]
unsafe fn tpause(_control: u32, _deadline: u64) -> bool {
    unreachable!("TPAUSE reached without a Waitpkg token");
}

#[cfg(all(test, target_arch = "x86_64"))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn a_token_exists_exactly_where_cpuid_reports_its_family() {
        let reported = cpuid();
        assert_eq!(Monitorx::on_this_host().is_some(), reported.monitorx);
        assert_eq!(Waitpkg::on_this_host().is_some(), reported.waitpkg);
    }

    /// On a host without a family, that family's counters read zero at
    /// every moment of the test process, whatever other tests run beside
    /// this one, since nothing raises them without the family's token. A
    /// family the host has is exercised first.
    #[test]
    fn a_family_this_host_lacks_never_runs() {
        let line = AtomicU64::new(0);
        let units = NonZeroU32::new(10_000).expect("nonzero");
        if let Some(mx) = Monitorx::on_this_host() {
            assert_eq!(mx.wait(&line, || true, MwaitxHint::C0, units), Waited::Ran);
        }
        if let Some(wp) = Waitpkg::on_this_host() {
            // SAFETY: reading the TSC has no precondition on x86_64.
            let deadline = unsafe { core::arch::x86_64::_rdtsc() } + 10_000;
            assert_ne!(wp.wait(&line, || true, UserWait::C01, deadline), Waited::AlreadyChanged);
        }

        let seen = executions();
        if !cpuid().monitorx {
            assert_eq!(
                (seen.monitorx, seen.mwaitx),
                (0, 0),
                "MONITORX or MWAITX ran on a host that does not report them"
            );
        }
        if !cpuid().waitpkg {
            assert_eq!(
                (seen.umonitor, seen.umwait, seen.tpause),
                (0, 0, 0),
                "a WAITPKG instruction ran on a host that does not report WAITPKG"
            );
        }
    }

    #[test]
    fn a_monitor_wait_is_skipped_when_the_store_has_already_landed() {
        let line = AtomicU64::new(1);
        let before = executions();
        if let Some(mx) = Monitorx::on_this_host() {
            let outcome = mx.wait(
                &line,
                || line.load(Ordering::Acquire) == 0,
                MwaitxHint::C0,
                NonZeroU32::new(1_000_000).expect("nonzero"),
            );
            assert_eq!(outcome, Waited::AlreadyChanged);
            assert!(executions().monitorx > before.monitorx, "the monitor was not counted as armed");
        }
        if let Some(wp) = Waitpkg::on_this_host() {
            // SAFETY: reading the TSC has no precondition on x86_64.
            let deadline = unsafe { core::arch::x86_64::_rdtsc() } + 1_000_000_000;
            let outcome = wp.wait(&line, || line.load(Ordering::Acquire) == 0, UserWait::C01, deadline);
            assert_eq!(outcome, Waited::AlreadyChanged);
            assert!(executions().umonitor > before.umonitor, "the monitor was not counted as armed");
        }
    }

    /// A store from another thread ends the wait. The timer or deadline
    /// bounds each arm and the loop re-arms, so a store the monitor missed
    /// costs an arm rather than hanging the test.
    #[test]
    fn a_store_from_another_thread_ends_a_monitor_wait() {
        let monitorx = Monitorx::on_this_host();
        let waitpkg = Waitpkg::on_this_host();
        if monitorx.is_none() && waitpkg.is_none() {
            return;
        }
        let line = Arc::new(AtomicU64::new(0));
        let storer = {
            let line = Arc::clone(&line);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                line.store(1, Ordering::Release);
            })
        };

        let before = executions();
        let units = NonZeroU32::new(1_000_000).expect("nonzero");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while line.load(Ordering::Acquire) == 0 {
            assert!(std::time::Instant::now() < deadline, "the store never ended the wait");
            if let Some(mx) = monitorx {
                mx.wait(&*line, || line.load(Ordering::Acquire) == 0, MwaitxHint::C0, units);
            } else if let Some(wp) = waitpkg {
                // SAFETY: reading the TSC has no precondition on x86_64.
                let arm_deadline = unsafe { core::arch::x86_64::_rdtsc() } + 2_000_000;
                wp.wait(&*line, || line.load(Ordering::Acquire) == 0, UserWait::C01, arm_deadline);
            }
        }
        storer.join().expect("the storing thread panicked");
        let after = executions();
        assert!(
            after.mwaitx > before.mwaitx || after.umwait > before.umwait,
            "the loop finished without a wait instruction running, so it tested nothing"
        );
    }

    /// With nobody storing, the timer alone ends the wait, and the count is
    /// never zero, so no request can wait without one.
    #[test]
    fn the_timer_ends_a_monitor_wait_nobody_stores_to() {
        let Some(mx) = Monitorx::on_this_host() else {
            return;
        };
        let line = AtomicU64::new(0);
        for units in [1, 1_000, 100_000] {
            let units = NonZeroU32::new(units).expect("nonzero");
            assert_eq!(mx.wait(&line, || true, MwaitxHint::C0, units), Waited::Ran);
            assert_eq!(mx.wait(&line, || true, MwaitxHint::C1, units), Waited::Ran);
        }
    }

    #[test]
    fn a_user_wait_and_a_timed_pause_end_at_their_deadline() {
        let Some(wp) = Waitpkg::on_this_host() else {
            return;
        };
        let line = AtomicU64::new(0);
        for state in [UserWait::C01, UserWait::C02] {
            // SAFETY: reading the TSC has no precondition on x86_64.
            let now = unsafe { core::arch::x86_64::_rdtsc() };
            assert_ne!(wp.wait(&line, || true, state, now + 100_000), Waited::AlreadyChanged);
            assert_ne!(wp.pause(state, now + 100_000), Waited::AlreadyChanged);
        }
    }
}
