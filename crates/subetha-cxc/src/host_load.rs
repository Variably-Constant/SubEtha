//! How busy the machine's logical processors were over an interval, from
//! the operating system's cumulative processor times.
//!
//! [`CpuTimes::read`] takes one reading, and [`CpuTimes::busy_since`] turns
//! two readings into the number of logical processors the whole machine,
//! every process included, kept busy between them:
//! - Windows: `GetSystemTimes`, whose kernel time includes idle time.
//! - Linux: the aggregate `cpu` line of `/proc/stat`; idle and iowait
//!   count as not busy.
//! - FreeBSD: `sysctl kern.cp_time`; idle counts as not busy.
//! - macOS: `host_statistics` with `HOST_CPU_LOAD_INFO`; idle counts as
//!   not busy.
//!
//! Elsewhere there is no reading.

/// The machine's cumulative busy and total processor time, in the
/// operating system's own unit, and its logical processor count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuTimes {
    busy: u64,
    total: u64,
    processors: u32,
}

impl CpuTimes {
    /// One reading, or `None` where this platform has no reader or the
    /// read failed.
    pub fn read() -> Option<Self> {
        read()
    }

    /// Logical processors kept busy between `earlier` and `self`: the busy
    /// share of the processor time that elapsed, times the logical
    /// processor count. `None` when no processor time elapsed between the
    /// readings or a count went backwards, as a wrapped counter does.
    pub fn busy_since(&self, earlier: &CpuTimes) -> Option<f64> {
        let total = self.total.checked_sub(earlier.total)?;
        let busy = self.busy.checked_sub(earlier.busy)?;
        if total == 0 || busy > total {
            return None;
        }
        Some(busy as f64 / total as f64 * f64::from(self.processors))
    }

    /// The logical processor count this reading was taken against.
    pub fn processors(&self) -> u32 {
        self.processors
    }
}

#[cfg(windows)]
fn read() -> Option<CpuTimes> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetActiveProcessorCount, GetSystemTimes};

    // ALL_PROCESSOR_GROUPS: the count across every processor group.
    const ALL_PROCESSOR_GROUPS: u16 = 0xFFFF;
    let zero = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
    let (mut idle, mut kernel, mut user) = (zero, zero, zero);
    // SAFETY: three valid out-pointers to FILETIME.
    if unsafe { GetSystemTimes(&mut idle, &mut kernel, &mut user) } == 0 {
        return None;
    }
    let value = |t: FILETIME| (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime);
    let total = value(kernel).checked_add(value(user))?;
    let busy = total.checked_sub(value(idle))?;
    // SAFETY: takes no pointer.
    let processors = unsafe { GetActiveProcessorCount(ALL_PROCESSOR_GROUPS) };
    (processors > 0).then_some(CpuTimes { busy, total, processors })
}

#[cfg(target_os = "linux")]
fn read() -> Option<CpuTimes> {
    parse_proc_stat(&std::fs::read_to_string("/proc/stat").ok()?)
}

/// A reading from the text of `/proc/stat`: the aggregate `cpu` line's
/// fields in order are user, nice, system, idle, iowait, irq, softirq and
/// steal, then guest and guest_nice, which user and nice already include.
/// The processor count is the number of `cpuN` lines.
#[cfg(any(test, target_os = "linux"))]
fn parse_proc_stat(stat: &str) -> Option<CpuTimes> {
    let aggregate = stat.lines().find(|line| line.starts_with("cpu "))?;
    let fields: Vec<u64> = aggregate
        .split_whitespace()
        .skip(1)
        .map(|field| field.parse().ok())
        .collect::<Option<_>>()?;
    if fields.len() < 4 {
        return None;
    }
    let idle = fields[3] + fields.get(4).copied().unwrap_or(0);
    let total: u64 = fields.iter().take(8).sum();
    let busy = total.checked_sub(idle)?;
    let processors = stat
        .lines()
        .filter(|line| line.starts_with("cpu") && line.as_bytes().get(3).is_some_and(u8::is_ascii_digit))
        .count();
    let processors = u32::try_from(processors).ok()?;
    (processors > 0).then_some(CpuTimes { busy, total, processors })
}

#[cfg(target_os = "freebsd")]
fn read() -> Option<CpuTimes> {
    // CP_USER, CP_NICE, CP_SYS, CP_INTR, CP_IDLE.
    let mut cp_time = [0 as libc::c_long; 5];
    let mut len = core::mem::size_of_val(&cp_time);
    // SAFETY: the name is NUL-terminated, and the buffer and its length
    // describe `cp_time`.
    let rc = unsafe {
        libc::sysctlbyname(
            c"kern.cp_time".as_ptr(),
            cp_time.as_mut_ptr().cast(),
            &mut len,
            core::ptr::null(),
            0,
        )
    };
    if rc != 0 || len != core::mem::size_of_val(&cp_time) {
        return None;
    }
    let ticks = cp_time.map(|t| u64::try_from(t).unwrap_or(0));
    let total: u64 = ticks.iter().sum();
    let busy = total.checked_sub(ticks[4])?;
    let processors = sysctl_int(c"hw.ncpu")?;
    Some(CpuTimes { busy, total, processors })
}

#[cfg(target_os = "macos")]
fn read() -> Option<CpuTimes> {
    use std::sync::OnceLock;

    // host_statistics flavor HOST_CPU_LOAD_INFO returns four natural_t
    // tick counts: user, system, idle, nice.
    const HOST_CPU_LOAD_INFO: i32 = 3;
    const HOST_CPU_LOAD_INFO_COUNT: u32 = 4;
    unsafe extern "C" {
        fn mach_host_self() -> u32;
        fn host_statistics(host: u32, flavor: i32, info: *mut i32, count: *mut u32) -> i32;
    }
    // One send right to the host port, taken once for the process.
    static HOST: OnceLock<u32> = OnceLock::new();
    // SAFETY: takes no argument.
    let host = *HOST.get_or_init(|| unsafe { mach_host_self() });
    let mut ticks = [0u32; 4];
    let mut count = HOST_CPU_LOAD_INFO_COUNT;
    // SAFETY: `ticks` holds HOST_CPU_LOAD_INFO_COUNT integers and `count`
    // says so.
    let rc = unsafe {
        host_statistics(host, HOST_CPU_LOAD_INFO, ticks.as_mut_ptr().cast(), &mut count)
    };
    if rc != 0 || count != HOST_CPU_LOAD_INFO_COUNT {
        return None;
    }
    let [user, system, idle, nice] = ticks.map(u64::from);
    let busy = user + system + nice;
    let total = busy + idle;
    let processors = sysctl_int(c"hw.logicalcpu")?;
    Some(CpuTimes { busy, total, processors })
}

/// A positive integer sysctl, read by name.
#[cfg(any(target_os = "freebsd", target_os = "macos"))]
fn sysctl_int(name: &core::ffi::CStr) -> Option<u32> {
    let mut value: libc::c_int = 0;
    let mut len = core::mem::size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated, and the buffer and its length
    // describe `value`.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            (&raw mut value).cast(),
            &mut len,
            core::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 || len != core::mem::size_of::<libc::c_int>() {
        return None;
    }
    u32::try_from(value).ok().filter(|&n| n > 0)
}

#[cfg(not(any(windows, target_os = "linux", target_os = "freebsd", target_os = "macos")))]
fn read() -> Option<CpuTimes> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn busy_processors_are_the_busy_share_of_elapsed_time_times_the_count() {
        let earlier = CpuTimes { busy: 100, total: 1_000, processors: 8 };
        let later = CpuTimes { busy: 400, total: 2_000, processors: 8 };
        assert_eq!(later.busy_since(&earlier), Some(2.4));
    }

    #[test]
    fn no_elapsed_time_or_a_count_going_backwards_reads_as_no_answer() {
        let reading = CpuTimes { busy: 100, total: 1_000, processors: 8 };
        assert_eq!(reading.busy_since(&reading), None);
        let wrapped = CpuTimes { busy: 50, total: 900, processors: 8 };
        assert_eq!(wrapped.busy_since(&reading), None);
    }

    #[test]
    fn proc_stat_counts_idle_and_iowait_as_not_busy_and_counts_each_cpu_line() {
        let stat = "cpu  100 5 50 800 20 3 2 1 7 0\n\
                    cpu0 50 2 25 400 10 1 1 0 3 0\n\
                    cpu1 50 3 25 400 10 2 1 1 4 0\n\
                    intr 12345\n\
                    ctxt 678\n";
        let reading = parse_proc_stat(stat).expect("a reading");
        assert_eq!(reading.total, 981, "user through steal, with guest left out");
        assert_eq!(reading.busy, 161);
        assert_eq!(reading.processors, 2);
    }

    #[test]
    fn proc_stat_without_an_aggregate_line_is_no_reading() {
        assert_eq!(parse_proc_stat("intr 1\nctxt 2\n"), None);
    }

    #[cfg(any(windows, target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    #[test]
    fn a_reading_is_taken_on_every_supported_platform() {
        let reading = CpuTimes::read().expect("this platform has a reader");
        assert!(reading.processors() >= 1);
        assert!(reading.busy <= reading.total);
    }

    /// Two threads spinning through the whole interval, the test's own and
    /// one it starts, read as at least half a logical processor busy: the
    /// reading counts the whole machine, so a loaded machine reads busier
    /// still.
    #[cfg(any(windows, target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    #[test]
    fn a_spinning_thread_reads_as_busy() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let stop = Arc::new(AtomicBool::new(false));
        let spinner = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            })
        };
        let before = CpuTimes::read().expect("a reading");
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(200) {
            std::hint::spin_loop();
        }
        let after = CpuTimes::read().expect("a reading");
        stop.store(true, Ordering::Relaxed);
        spinner.join().expect("the spinning thread panicked");
        let busy = after.busy_since(&before).expect("processor time elapsed");
        assert!(
            busy >= 0.5 && busy <= f64::from(after.processors()) + 0.5,
            "two spinning threads read as {busy:.2} busy logical processors of {}",
            after.processors()
        );
    }
}
