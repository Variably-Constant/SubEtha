//! Cache-line stewardship: instruction-level hints the compiler
//! never emits on its own.
//!
//! LLVM treats every store identically; it has no model of WHICH
//! core reads a line next. These wrappers encode that knowledge at
//! the three points the substrate has it:
//!
//! | op | when | effect |
//! |---|---|---|
//! | [`prefetchw`] | before a CAS/RMW on a contended line | requests the line in Modified state, collapsing the read-for-ownership round trip the CAS pays otherwise |
//! | [`cldemote`] | after the producer publishes a slot + sequence | pushes the just-written line toward the shared LLC so the consumer's first read is an LLC hit, not a cross-core snoop-and-forward |
//! | [`sfence`] | after non-temporal stores, before the `Release` publish | orders weakly-ordered streaming stores ahead of the sequence store other cores acquire on |
//!
//! Safety/portability posture (per the project's fallback mandate):
//! every op compiles to a plain no-op on non-x86_64 targets, and on
//! x86_64 each is either architecturally NOP-safe on silicon that
//! lacks it or baseline-guaranteed:
//!
//! - `PREFETCHW` (`0F 0D /1`): prefetch hints never fault; AMD
//!   since K6, Intel since Broadwell, and pre-Broadwell Intel
//!   executes the encoding as a NOP (it sits in the NOP-reserved
//!   hint space).
//! - `CLDEMOTE` (`NP 0F 1C /0`): per the Intel ISA reference,
//!   "on processors which do not support the CLDEMOTE instruction
//!   (including legacy hardware) the instruction will be treated
//!   as a NOP". CPUID `7.0` ECX bit 25 reports real support
//!   ([`has_cldemote`]) for diagnostics; execution needs no gate.
//! - `SFENCE` is baseline x86-64 (SSE2).
//!
//! Both hint wrappers are emitted as explicit byte sequences, not
//! mnemonics, so the assembler never gates them behind target
//! features the baseline build does not enable.

/// Write-intent prefetch of the cache line containing `addr`.
/// Call immediately before a `compare_exchange` / `fetch_*` on a
/// line another core probably owns: the line arrives in Modified
/// state instead of being upgraded mid-RMW.
#[inline(always)]
pub fn prefetchw(addr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        // PREFETCHW m8 = 0F 0D /1; modrm 0x08 = [rax] with reg=/1.
        core::arch::asm!(
            ".byte 0x0f, 0x0d, 0x08",
            in("rax") addr,
            options(nostack, preserves_flags, readonly),
        );
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        _ = addr;
    }
}

/// Demote the cache line containing `addr` toward the shared LLC.
/// Call after the producer's final store to a line whose next
/// reader is another core (slot payload, published sequence).
/// Architecturally a NOP wherever unsupported; the hint may also
/// be ignored by hardware - it is never load-bearing.
#[inline(always)]
pub fn cldemote(addr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        // CLDEMOTE m8 = NP 0F 1C /0; modrm 0x00 = [rax] with reg=/0.
        core::arch::asm!(
            ".byte 0x0f, 0x1c, 0x00",
            in("rax") addr,
            options(nostack, preserves_flags, readonly),
        );
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        _ = addr;
    }
}

/// Store fence: orders all prior stores (including non-temporal
/// ones, which `Release` ordering alone does NOT cover) before any
/// later store. Required between a streaming copy and the
/// sequence-publish store that makes it visible.
#[inline(always)]
pub fn sfence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_sfence();
    }
    #[cfg(not(target_arch = "x86_64"))]
    core::sync::atomic::fence(core::sync::atomic::Ordering::Release);
}

/// Whether this CPU actually implements CLDEMOTE (CPUID `7.0` ECX
/// bit 25). Diagnostic only - [`cldemote`] is NOP-safe regardless.
pub fn has_cldemote() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        use std::sync::OnceLock;
        static PROBE: OnceLock<bool> = OnceLock::new();
        *PROBE.get_or_init(|| {
            let max_basic = core::arch::x86_64::__cpuid_count(0, 0).eax;
            max_basic >= 7
                && core::arch::x86_64::__cpuid_count(7, 0).ecx & (1 << 25) != 0
        })
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hints_execute_without_faulting() {
        // The whole point: safe to execute blind on any x86_64 (and
        // no-ops elsewhere). Run each against stack and heap lines.
        let stack_val: u64 = 42;
        let heap_val = Box::new([0u8; 256]);
        for addr in [
            &stack_val as *const u64 as *const u8,
            heap_val.as_ptr(),
            unsafe { heap_val.as_ptr().add(128) },
        ] {
            prefetchw(addr);
            cldemote(addr);
        }
        sfence();
        assert_eq!(stack_val, 42);
    }

    #[test]
    fn cldemote_probe_is_stable() {
        assert_eq!(has_cldemote(), has_cldemote());
        println!("cldemote supported: {}", has_cldemote());
    }
}
