//! Which logical processors share a core, and what kind each core is.
//!
//! [`topology`] reads, once per process, one entry per logical processor
//! the operating system reports:
//! - Windows: `GetSystemCpuSetInformation`. `CoreIndex` is shared by the
//!   hardware threads of one core within a processor group, and
//!   `EfficiencyClass` is higher for faster, less efficient cores.
//! - Linux: `/sys/devices/system/cpu/cpuN/topology`, where
//!   `physical_package_id` and `core_id` together name a core. On a hybrid
//!   x86 processor each core's kind is read by CPUID on that processor,
//!   from a helper thread pinned there.
//!
//! Elsewhere, and where the read fails, there is no topology.

use std::collections::BTreeMap;
use std::sync::OnceLock;

#[cfg_attr(not(target_os = "linux"), allow(unused_imports))]
use subetha_core::cpuid::CoreKind;

/// What sets one kind of core apart from another on this host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoreClass {
    /// Windows' `EfficiencyClass`: higher is faster and less efficient.
    Efficiency(u8),
    /// The core type CPUID reports on a hybrid x86 processor.
    Cpuid(CoreKind),
}

/// One logical processor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalProcessor {
    /// The processor group it belongs to on Windows; 0 elsewhere.
    pub group: u16,
    /// Its number within the group, as affinity calls take it.
    pub number: u32,
    /// An identifier every logical processor of the same core shares.
    pub core: u32,
    /// Its core's class, where the platform reports one.
    pub class: Option<CoreClass>,
}

/// Every logical processor the operating system reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    pub processors: Vec<LogicalProcessor>,
}

impl Topology {
    /// The logical processors that share `processor`'s core, `processor`
    /// included, in the order the operating system reported them.
    pub fn siblings(&self, processor: &LogicalProcessor) -> Vec<LogicalProcessor> {
        self.processors
            .iter()
            .filter(|p| p.group == processor.group && p.core == processor.core)
            .copied()
            .collect()
    }

    /// The logical processors of each class, keyed by the class's debug
    /// name, in reported order within each.
    pub fn by_class(&self) -> BTreeMap<String, Vec<LogicalProcessor>> {
        let mut classes: BTreeMap<String, Vec<LogicalProcessor>> = BTreeMap::new();
        for processor in &self.processors {
            classes.entry(format!("{:?}", processor.class)).or_default().push(*processor);
        }
        classes
    }
}

/// This host's topology, read on first use and kept for the life of the
/// process. `None` where it is not read.
pub fn topology() -> Option<&'static Topology> {
    static READ: OnceLock<Option<Topology>> = OnceLock::new();
    READ.get_or_init(read).as_ref()
}

#[cfg(windows)]
fn read() -> Option<Topology> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetSystemCpuSetInformation(
            information: *mut u8,
            buffer_length: u32,
            returned_length: *mut u32,
            process: *mut core::ffi::c_void,
            flags: u32,
        ) -> i32;
    }
    let mut needed = 0u32;
    // SAFETY: a null buffer of length 0 asks only for the needed length,
    // which is written through a valid pointer.
    unsafe {
        GetSystemCpuSetInformation(core::ptr::null_mut(), 0, &mut needed, core::ptr::null_mut(), 0)
    };
    if needed == 0 {
        return None;
    }
    let mut buffer = vec![0u8; needed as usize];
    let mut returned = 0u32;
    // SAFETY: the buffer is `needed` bytes long, as its length says.
    let ok = unsafe {
        GetSystemCpuSetInformation(
            buffer.as_mut_ptr(),
            needed,
            &mut returned,
            core::ptr::null_mut(),
            0,
        )
    };
    if ok == 0 {
        return None;
    }
    buffer.truncate(returned as usize);
    parse_cpu_sets(&buffer)
}

/// The processors in a `GetSystemCpuSetInformation` buffer: a run of
/// `SYSTEM_CPU_SET_INFORMATION` records, each starting with its own size
/// and type. A record of type 0 (`CpuSetInformation`) holds `Group` at
/// byte 12, `LogicalProcessorIndex` at 14, `CoreIndex` at 15 and
/// `EfficiencyClass` at 18.
#[cfg(any(test, windows))]
fn parse_cpu_sets(buffer: &[u8]) -> Option<Topology> {
    let mut processors = Vec::new();
    let mut at = 0usize;
    while at + 8 <= buffer.len() {
        let size = u32::from_le_bytes(buffer[at..at + 4].try_into().ok()?) as usize;
        let kind = u32::from_le_bytes(buffer[at + 4..at + 8].try_into().ok()?);
        if size < 8 || at + size > buffer.len() {
            return None;
        }
        if kind == 0 && size >= 19 {
            let group = u16::from_le_bytes(buffer[at + 12..at + 14].try_into().ok()?);
            processors.push(LogicalProcessor {
                group,
                number: u32::from(buffer[at + 14]),
                core: u32::from(buffer[at + 15]),
                class: Some(CoreClass::Efficiency(buffer[at + 18])),
            });
        }
        at += size;
    }
    (!processors.is_empty()).then_some(Topology { processors })
}

#[cfg(target_os = "linux")]
fn read() -> Option<Topology> {
    let mut topology = read_sysfs(std::path::Path::new("/sys/devices/system/cpu"))?;
    if subetha_core::cpuid::cpuid().hybrid {
        for processor in &mut topology.processors {
            processor.class = kind_of(processor.number).map(CoreClass::Cpuid);
        }
    }
    Some(topology)
}

/// The kind of core `number` is, read by a helper thread pinned to it, so
/// the caller's own affinity is left as it was. `None` when the pin fails
/// or the processor names no kind.
#[cfg(target_os = "linux")]
fn kind_of(number: u32) -> Option<CoreKind> {
    std::thread::spawn(move || {
        if crate::cpu_affinity::pin_current_thread_to_core(number as usize) {
            subetha_core::cpuid::core_kind_here()
        } else {
            None
        }
    })
    .join()
    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

/// The processors under a sysfs cpu directory: each `cpuN` directory with
/// a readable `topology/physical_package_id` and `topology/core_id`. An
/// offline processor has no topology directory and is left out. A
/// directory entry that cannot be read makes the whole read `None`.
#[cfg(any(test, target_os = "linux"))]
fn read_sysfs(root: &std::path::Path) -> Option<Topology> {
    let read_number = |path: std::path::PathBuf| -> Option<u32> {
        std::fs::read_to_string(path).ok()?.trim().parse().ok()
    };
    let mut processors = Vec::new();
    for entry in std::fs::read_dir(root).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let Some(number) = name
            .to_str()
            .and_then(|n| n.strip_prefix("cpu"))
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        let topology = entry.path().join("topology");
        let (Some(package), Some(core)) = (
            read_number(topology.join("physical_package_id")),
            read_number(topology.join("core_id")),
        ) else {
            continue;
        };
        processors.push(LogicalProcessor {
            group: 0,
            number,
            core: (package << 16) | (core & 0xFFFF),
            class: None,
        });
    }
    processors.sort_by_key(|p| p.number);
    (!processors.is_empty()).then_some(Topology { processors })
}

#[cfg(not(any(windows, target_os = "linux")))]
fn read() -> Option<Topology> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One SYSTEM_CPU_SET_INFORMATION record of type 0.
    fn record(group: u16, number: u8, core: u8, efficiency: u8) -> Vec<u8> {
        let mut r = vec![0u8; 32];
        r[0..4].copy_from_slice(&32u32.to_le_bytes());
        r[4..8].copy_from_slice(&0u32.to_le_bytes());
        r[8..12].copy_from_slice(&(256 + u32::from(number)).to_le_bytes());
        r[12..14].copy_from_slice(&group.to_le_bytes());
        r[14] = number;
        r[15] = core;
        r[18] = efficiency;
        r
    }

    #[test]
    fn cpu_set_records_give_each_processor_its_core_and_class() {
        let buffer: Vec<u8> = [record(0, 0, 0, 1), record(0, 1, 0, 1), record(0, 2, 1, 0)].concat();
        let topology = parse_cpu_sets(&buffer).expect("three processors");
        assert_eq!(topology.processors.len(), 3);
        let first = topology.processors[0];
        assert_eq!(
            topology.siblings(&first).iter().map(|p| p.number).collect::<Vec<_>>(),
            vec![0, 1],
            "processors 0 and 1 share core 0"
        );
        assert_eq!(topology.processors[2].class, Some(CoreClass::Efficiency(0)));
        assert_eq!(topology.by_class().len(), 2, "two efficiency classes");
    }

    #[test]
    fn a_cpu_set_record_that_overruns_the_buffer_is_no_topology() {
        let mut buffer = record(0, 0, 0, 0);
        buffer[0..4].copy_from_slice(&64u32.to_le_bytes());
        assert_eq!(parse_cpu_sets(&buffer), None);
    }

    #[test]
    fn a_sysfs_tree_gives_each_online_processor_its_core() {
        let root = std::env::temp_dir().join(format!("subetha-topology-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(&root).expect("clear the fake sysfs tree an earlier run left");
        }
        for (number, package, core) in [(0u32, 0u32, 0u32), (1, 0, 0), (2, 0, 1), (3, 0, 1)] {
            let dir = root.join(format!("cpu{number}")).join("topology");
            std::fs::create_dir_all(&dir).expect("create the fake topology directory");
            std::fs::write(dir.join("physical_package_id"), format!("{package}\n")).expect("write");
            std::fs::write(dir.join("core_id"), format!("{core}\n")).expect("write");
        }
        // An offline processor: its directory exists, its topology does not.
        std::fs::create_dir_all(root.join("cpu4")).expect("create cpu4");
        std::fs::create_dir_all(root.join("cpufreq")).expect("create cpufreq");

        let topology = read_sysfs(&root).expect("four online processors");
        std::fs::remove_dir_all(&root).expect("remove the fake sysfs tree");
        assert_eq!(topology.processors.iter().map(|p| p.number).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        let third = topology.processors[2];
        assert_eq!(
            topology.siblings(&third).iter().map(|p| p.number).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn this_host_reports_every_processor_once_with_itself_among_its_siblings() {
        let topology = topology().expect("this platform is read");
        let mut seen = std::collections::HashSet::new();
        for processor in &topology.processors {
            assert!(
                seen.insert((processor.group, processor.number)),
                "processor {:?} reported twice",
                processor
            );
            assert!(topology.siblings(processor).contains(processor));
        }
    }
}
