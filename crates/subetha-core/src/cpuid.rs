//! Tiny CPUID feature-detection surface that adaptive primitives
//! across the SubEtha workspace consult to gate
//! hardware-acceleration paths.
//!
//! The substrate exposes:
//!
//! - [`has_waitpkg`] - is the WAITPKG ISA extension
//!   (`UMONITOR` / `UMWAIT` / `TPAUSE`) available?
//! - [`has_movdir64b`] - is the MOVDIR64B ISA extension (atomic
//!   non-temporal 64-byte cache-line store) available?
//!
//! WAITPKG was introduced on Intel Tremont (2019) and Tiger Lake
//! (2020) microarchitectures, and on AMD Zen 5 (2024). Hosts older
//! than that need a `PAUSE`-spin fallback; primitives that wait on
//! a cache line (e.g. `SharedDequeUrd` in `subetha-cxc`) pick the
//! wait strategy at runtime by calling this function once and
//! caching the result.
//!
//! MOVDIR64B was introduced on Intel Tremont (2019) and Tiger Lake
//! (2020), and on AMD Zen 5 (2024). It is the atomic 64-byte
//! non-temporal store; `SharedDequeUrd` uses it to publish a whole
//! mailbox line in one transaction, eliminating the cross-CCX
//! coherence-upgrade traffic that the byte-by-byte fallback path
//! pays on hosts without it.
//!
//! Both flags are probed via CPUID leaf 7 sub-leaf 0
//! (`Structured Extended Feature Flags`): ECX bit 5 = WAITPKG,
//! ECX bit 28 = MOVDIR64B.

#[cfg(target_arch = "x86_64")]
use std::sync::OnceLock;

/// Returns `true` when the CPU advertises the WAITPKG ISA extension
/// (`UMONITOR` / `UMWAIT` / `TPAUSE`).
///
/// On x86_64 the result is cached after the first probe. On
/// non-x86_64 targets this always returns `false`.
#[cfg(target_arch = "x86_64")]
pub fn has_waitpkg() -> bool {
    *waitpkg_available()
}

#[cfg(not(target_arch = "x86_64"))]
pub fn has_waitpkg() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
fn waitpkg_available() -> &'static bool {
    static CACHE: OnceLock<bool> = OnceLock::new();
    CACHE.get_or_init(|| {
        use std::arch::x86_64::{__cpuid, __cpuid_count};
        // Validate CPUID leaf 7 is implemented by reading the
        // max-leaf id from leaf 0. Pre-Nehalem silicon may not
        // implement leaf 7 at all. `__cpuid` and `__cpuid_count`
        // are safe on the current x86_64 stable surface; the
        // `#[cfg(target_arch = "x86_64")]` gate above guarantees
        // the only required invariant.
        let max_leaf = __cpuid(0).eax;
        if max_leaf < 7 {
            return false;
        }
        // CPUID leaf 7, sub-leaf 0, ECX bit 5 = WAITPKG.
        let r = __cpuid_count(7, 0);
        (r.ecx >> 5) & 1 == 1
    })
}

/// Returns `true` when the CPU advertises the MOVDIR64B ISA
/// extension (atomic non-temporal 64-byte cache-line store).
///
/// MOVDIR64B encodes as `66 0F 38 F8 /r` and writes a 64-byte-
/// aligned source cache line to a 64-byte-aligned destination cache
/// line in one atomic transaction that bypasses the writing core's
/// L1d (Write-Combining store). It eliminates the RFO coherence
/// upgrade that a byte-by-byte fallback path pays when the
/// destination line is in a remote core's L1d in M-state.
///
/// Available on Intel Tremont (2019), Tiger Lake (2020) and later
/// Intel cores, and on AMD Zen 5 (2024) and later AMD cores.
///
/// On x86_64 the result is cached after the first probe. On
/// non-x86_64 targets this always returns `false`.
#[cfg(target_arch = "x86_64")]
pub fn has_movdir64b() -> bool {
    *movdir64b_available()
}

#[cfg(not(target_arch = "x86_64"))]
pub fn has_movdir64b() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
fn movdir64b_available() -> &'static bool {
    static CACHE: OnceLock<bool> = OnceLock::new();
    CACHE.get_or_init(|| {
        use std::arch::x86_64::{__cpuid, __cpuid_count};
        let max_leaf = __cpuid(0).eax;
        if max_leaf < 7 {
            return false;
        }
        // CPUID leaf 7, sub-leaf 0, ECX bit 28 = MOVDIR64B.
        let r = __cpuid_count(7, 0);
        (r.ecx >> 28) & 1 == 1
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_waitpkg_does_not_panic_and_caches() {
        // Exercise the cached probe path twice; the actual answer
        // depends on the host CPU and is not asserted. The second
        // call hits the `OnceLock` cache.
        let first = has_waitpkg();
        let second = has_waitpkg();
        assert_eq!(first, second, "has_waitpkg must be idempotent");
    }

    #[test]
    fn has_movdir64b_does_not_panic_and_caches() {
        let first = has_movdir64b();
        let second = has_movdir64b();
        assert_eq!(first, second, "has_movdir64b must be idempotent");
    }
}
