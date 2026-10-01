//! What the processor reports through CPUID, read once per process.
//!
//! [`cpuid`] returns one snapshot of the facts the substrate reads: the
//! vendor and signature, the two user-mode monitor-wait families, RDTSCP
//! and the invariant TSC, the cache and monitor line sizes, the
//! hypervisor, and whether the processor has cores of more than one kind.
//! [`has_waitpkg`] and [`has_movdir64b`] answer from the same snapshot.
//!
//! An instruction's availability is its feature bit and nothing else.
//! Family and model are for reports: firmware can withdraw `MONITORX`
//! through `HWCR[MonMwaitDis]`, which also clears its CPUID bit (AMD PPR
//! 54945, `CPUID_Fn80000001_ECX[29]`), and a hypervisor can hide either
//! family from its guests.
//!
//! A leaf is read only when its range reaches it. On Intel a leaf past the
//! maximum returns the highest basic leaf's data rather than zeros, and
//! those bits would read as features.
//!
//! WAITPKG (`UMONITOR` / `UMWAIT` / `TPAUSE`) is CPUID leaf 7 sub-leaf 0
//! ECX bit 5, and MOVDIR64B (the atomic non-temporal 64-byte cache-line
//! store) is bit 28 of the same register. Intel Tremont, Alder Lake and
//! Sapphire Rapids cores have both; Intel Tiger Lake and AMD Zen 5 have
//! MOVDIR64B without WAITPKG, and no AMD core through Zen 5 has WAITPKG.
//! Primitives that wait on a cache line (e.g. `SharedDequeUrd` in
//! `subetha-cxc`) pick their wait strategy from these answers, and
//! `SharedDequeUrd` publishes a whole mailbox line with MOVDIR64B where it
//! is reported.

use std::sync::OnceLock;

/// What this processor reports, as plain values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cpuid {
    /// The vendor string from leaf 0, such as `AuthenticAMD`. Empty when
    /// this build is not for x86_64.
    pub vendor: String,
    /// Family from leaf 1, with the extended family folded in the way both
    /// vendors' manuals define. For reports only.
    pub family: u32,
    /// Model from leaf 1, with the extended model folded in. For reports
    /// only.
    pub model: u32,
    /// Stepping from leaf 1. For reports only.
    pub stepping: u32,
    /// `MONITORX` and `MWAITX`: CPUID Fn8000_0001_ECX bit 29.
    pub monitorx: bool,
    /// `UMONITOR`, `UMWAIT` and `TPAUSE`: CPUID.(EAX=7,ECX=0):ECX bit 5.
    pub waitpkg: bool,
    /// `MONITOR` and `MWAIT`: CPUID Fn0000_0001_ECX bit 3. They run only at
    /// privilege level 0 unless the operating system enables them for
    /// user mode, so they are reported and never executed.
    pub monitor: bool,
    /// `MOVDIR64B`: CPUID.(EAX=7,ECX=0):ECX bit 28.
    pub movdir64b: bool,
    /// `RDTSCP`: CPUID Fn8000_0001_EDX bit 27.
    pub rdtscp: bool,
    /// The TSC advancing at one rate through every P-state and C-state:
    /// CPUID Fn8000_0007_EDX bit 8. Without it a count of TSC cycles is
    /// not a length of time.
    pub invariant_tsc: bool,
    /// The line `CLFLUSH` flushes, which is the cache line: CPUID
    /// Fn0000_0001_EBX bits 15:8, in quadwords, here in bytes. `None` when
    /// `CLFLUSH` is not reported (Fn0000_0001_EDX bit 19), since the field
    /// is then undefined.
    pub cache_line_bytes: Option<u32>,
    /// The range a monitor watches, from leaf 5. `None` where leaf 5 is
    /// past the basic range.
    pub monitor_line: Option<MonitorLine>,
    /// What leaf 0x4000_0000 says, when leaf 1 reports a hypervisor
    /// (Fn0000_0001_ECX bit 31).
    pub hypervisor: Option<Hypervisor>,
    /// The processor has cores of more than one kind: Intel's hybrid bit,
    /// CPUID.(EAX=7,ECX=0):EDX bit 15, or AMD's HeterogeneousCores, CPUID
    /// Fn8000_0026_EAX bit 30.
    pub hybrid: bool,
}

/// The smallest and largest range a monitor watches, in bytes, exactly as
/// leaf 5 reports them: EAX bits 15:0 and EBX bits 15:0. A host that masks
/// the monitor families can report zeros here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorLine {
    pub smallest: u32,
    pub largest: u32,
}

/// A hypervisor's signature at leaf 0x4000_0000.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hypervisor {
    /// The twelve signature bytes with trailing padding removed, such as
    /// `Microsoft Hv` or `KVMKVMKVM`.
    pub vendor: String,
}

/// The kind of core a hybrid processor reports for the core that ran the
/// CPUID instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoreKind {
    /// Intel leaf 0x1A core type 0x40 (Intel Core), or AMD Fn8000_0026
    /// EBX CoreType 0.
    Performance,
    /// Intel leaf 0x1A core type 0x20 (Intel Atom), or AMD Fn8000_0026
    /// EBX CoreType 1.
    Efficiency,
    /// A core type value neither manual names, as the processor reported
    /// it.
    Other(u8),
}

/// This processor's answers, read on first use and kept for the life of
/// the process.
pub fn cpuid() -> &'static Cpuid {
    static READ: OnceLock<Cpuid> = OnceLock::new();
    READ.get_or_init(read)
}

/// Returns `true` when the CPU advertises the WAITPKG ISA extension
/// (`UMONITOR` / `UMWAIT` / `TPAUSE`). Always `false` on non-x86_64
/// targets.
pub fn has_waitpkg() -> bool {
    cpuid().waitpkg
}

/// Returns `true` when the CPU advertises the MOVDIR64B ISA extension
/// (atomic non-temporal 64-byte cache-line store).
///
/// MOVDIR64B encodes as `66 0F 38 F8 /r` and writes 64 bytes read from
/// its source to a 64-byte-aligned destination cache line in one
/// atomic transaction that bypasses the writing core's L1d
/// (Write-Combining store). It eliminates the RFO coherence upgrade that
/// a byte-by-byte fallback path pays when the destination line is in a
/// remote core's L1d in M-state. Always `false` on non-x86_64 targets.
pub fn has_movdir64b() -> bool {
    cpuid().movdir64b
}

/// The kind of the core the calling thread is running on, read from CPUID
/// on that core: Intel leaf 0x1A EAX bits 31:24, or AMD Fn8000_0026 EBX
/// bits 31:28. `None` when the processor does not report itself hybrid.
///
/// The answer describes whichever core executed CPUID, so a caller that
/// wants a particular core pins the thread to it first.
pub fn core_kind_here() -> Option<CoreKind> {
    if !cpuid().hybrid {
        return None;
    }
    read_core_kind()
}

#[cfg(target_arch = "x86_64")]
fn read() -> Cpuid {
    use core::arch::x86_64::{__cpuid, __cpuid_count};

    let leaf0 = __cpuid(0);
    let max_basic = leaf0.eax;
    let max_extended = __cpuid(0x8000_0000).eax;

    let leaf1 = (max_basic >= 1).then(|| __cpuid(1));
    let (family, model, stepping) = leaf1.map_or((0, 0, 0), |r| signature(r.eax));
    let leaf7 = (max_basic >= 7).then(|| __cpuid_count(7, 0));

    let ext1 = (max_extended >= 0x8000_0001).then(|| __cpuid(0x8000_0001));
    let ext7 = (max_extended >= 0x8000_0007).then(|| __cpuid(0x8000_0007));
    let ext26 = (max_extended >= 0x8000_0026).then(|| __cpuid_count(0x8000_0026, 0));

    let hypervisor = leaf1.filter(|r| bit(r.ecx, 31)).map(|_| {
        let signature = __cpuid(0x4000_0000);
        Hypervisor {
            vendor: text(&[signature.ebx, signature.ecx, signature.edx]),
        }
    });

    Cpuid {
        vendor: text(&[leaf0.ebx, leaf0.edx, leaf0.ecx]),
        family,
        model,
        stepping,
        monitorx: ext1.is_some_and(|r| bit(r.ecx, 29)),
        waitpkg: leaf7.is_some_and(|r| bit(r.ecx, 5)),
        monitor: leaf1.is_some_and(|r| bit(r.ecx, 3)),
        movdir64b: leaf7.is_some_and(|r| bit(r.ecx, 28)),
        rdtscp: ext1.is_some_and(|r| bit(r.edx, 27)),
        invariant_tsc: ext7.is_some_and(|r| bit(r.edx, 8)),
        cache_line_bytes: leaf1
            .filter(|r| bit(r.edx, 19))
            .map(|r| ((r.ebx >> 8) & 0xFF) * 8),
        monitor_line: (max_basic >= 5).then(|| {
            let leaf5 = __cpuid(5);
            MonitorLine {
                smallest: leaf5.eax & 0xFFFF,
                largest: leaf5.ebx & 0xFFFF,
            }
        }),
        hypervisor,
        hybrid: leaf7.is_some_and(|r| bit(r.edx, 15)) || ext26.is_some_and(|r| bit(r.eax, 30)),
    }
}

#[cfg(not(target_arch = "x86_64"))]
fn read() -> Cpuid {
    Cpuid {
        vendor: String::new(),
        family: 0,
        model: 0,
        stepping: 0,
        monitorx: false,
        waitpkg: false,
        monitor: false,
        movdir64b: false,
        rdtscp: false,
        invariant_tsc: false,
        cache_line_bytes: None,
        monitor_line: None,
        hypervisor: None,
        hybrid: false,
    }
}

#[cfg(target_arch = "x86_64")]
fn read_core_kind() -> Option<CoreKind> {
    use core::arch::x86_64::{__cpuid, __cpuid_count};

    let max_basic = __cpuid(0).eax;
    if max_basic >= 0x1A {
        let core_type = (__cpuid_count(0x1A, 0).eax >> 24) as u8;
        if core_type != 0 {
            return Some(match core_type {
                0x40 => CoreKind::Performance,
                0x20 => CoreKind::Efficiency,
                other => CoreKind::Other(other),
            });
        }
    }
    let max_extended = __cpuid(0x8000_0000).eax;
    if max_extended >= 0x8000_0026 {
        let core_type = ((__cpuid_count(0x8000_0026, 0).ebx >> 28) & 0xF) as u8;
        return Some(match core_type {
            0 => CoreKind::Performance,
            1 => CoreKind::Efficiency,
            other => CoreKind::Other(other),
        });
    }
    None
}

#[cfg(not(target_arch = "x86_64"))]
fn read_core_kind() -> Option<CoreKind> {
    None
}

#[cfg(target_arch = "x86_64")]
fn bit(register: u32, index: u32) -> bool {
    (register >> index) & 1 == 1
}

/// Family, model and stepping from leaf 1's EAX.
///
/// The extended family adds in only when the base family is 0Fh, and the
/// extended model forms the high nibble only when the base family is 0Fh
/// or, on Intel, 06h (AMD APM Vol. 3, CPUID Fn0000_0001_EAX; Intel SDM,
/// CPUID). AMD reserves the extended model below family 0Fh and reports it
/// as zero there, so one rule serves both vendors.
#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
fn signature(eax: u32) -> (u32, u32, u32) {
    let stepping = eax & 0xF;
    let base_model = (eax >> 4) & 0xF;
    let base_family = (eax >> 8) & 0xF;
    let extended_model = (eax >> 16) & 0xF;
    let extended_family = (eax >> 20) & 0xFF;

    let family = if base_family == 0xF {
        base_family + extended_family
    } else {
        base_family
    };
    let model = if base_family == 0xF || base_family == 0x6 {
        (extended_model << 4) | base_model
    } else {
        base_model
    };
    (family, model, stepping)
}

/// Registers read as the ASCII they hold, low byte first, with trailing
/// NUL padding removed. A byte that is not UTF-8 shows as the replacement
/// character, so a strange signature reads as strange rather than
/// shorter.
#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
fn text(registers: &[u32]) -> String {
    let bytes: Vec<u8> = registers.iter().flat_map(|r| r.to_le_bytes()).collect();
    String::from_utf8_lossy(&bytes)
        .trim_end_matches('\0')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_answers_are_read_once_and_kept() {
        assert!(
            std::ptr::eq(cpuid(), cpuid()),
            "two calls read CPUID twice rather than keeping the first answer"
        );
    }

    #[test]
    fn has_waitpkg_and_has_movdir64b_answer_from_the_snapshot() {
        assert_eq!(has_waitpkg(), cpuid().waitpkg);
        assert_eq!(has_movdir64b(), cpuid().movdir64b);
    }

    #[test]
    fn the_signature_folds_the_extended_fields_as_the_manuals_define() {
        // A Ryzen 9 7900X: family 19h, model 61h, stepping 2.
        assert_eq!(signature(0x00A6_0F12), (0x19, 0x61, 2));
        // A Ryzen 7 2700: family 17h, model 08h, stepping 2.
        assert_eq!(signature(0x0080_0F82), (0x17, 0x08, 2));
        // An Intel family 06h part, model 9Eh: the extended model is the
        // high nibble and the extended family does not add in.
        assert_eq!(signature(0x0009_06EA), (0x06, 0x9E, 0xA));
        // Below family 0Fh on anything but 06h, the base model stands
        // alone even when the extended field is set.
        assert_eq!(signature(0x0001_0543), (0x05, 0x4, 3));
    }

    #[test]
    fn register_text_reads_low_byte_first_and_drops_padding() {
        // KVM's signature, NUL-padded to twelve bytes.
        assert_eq!(text(&[0x4B4D_564B, 0x564B_4D56, 0x0000_004D]), "KVMKVMKVM");
        // A signature with an interior space keeps it.
        assert_eq!(text(&[0x7263_694D, 0x666F_736F, 0x7648_2074]), "Microsoft Hv");
    }

    #[test]
    fn only_a_hybrid_processor_names_a_core_kind() {
        if !cpuid().hybrid {
            assert_eq!(core_kind_here(), None);
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn an_x86_host_names_its_vendor_and_a_whole_cache_line() {
        let c = cpuid();
        assert_eq!(c.vendor.len(), 12, "vendor {:?} is not twelve bytes", c.vendor);
        if let Some(line) = c.cache_line_bytes {
            assert!(
                line.is_power_of_two() && line >= 8,
                "a {line}-byte cache line is not a power of two of at least one quadword"
            );
        }
    }
}
