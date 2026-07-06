//! Cross-platform CPU core pinning for controlled measurements.
//!
//! Pinning the producer and consumer to fixed, distinct cores removes
//! the run-to-run scheduler variance that otherwise swamps small
//! cache-coherence effects in a 1P/1C throughput measurement. The
//! function is best-effort: it returns `false` (leaving the thread on
//! the OS default) on platforms without a supported affinity API, so
//! a caller can report that its measurement was uncontrolled rather
//! than silently trusting a noisy number.

/// Pin the calling thread to a single logical core. Returns `true`
/// if the affinity was actually set.
#[cfg(target_os = "windows")]
pub fn pin_current_thread_to_core(core: usize) -> bool {
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, SetThreadAffinityMask,
    };
    if core >= usize::BITS as usize {
        return false;
    }
    let mask: usize = 1usize << core;
    // Returns the previous affinity mask, or 0 on failure.
    unsafe { SetThreadAffinityMask(GetCurrentThread(), mask) != 0 }
}

/// Pin the calling thread to a single logical core. Returns `true`
/// if the affinity was actually set.
#[cfg(target_os = "linux")]
pub fn pin_current_thread_to_core(core: usize) -> bool {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        ) == 0
    }
}

/// Best-effort no-op on platforms without a supported affinity API.
#[cfg(not(any(target_os = "windows", target_os = "linux")))]
pub fn pin_current_thread_to_core(_core: usize) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_to_core_zero() {
        // Core 0 exists everywhere; on a supported platform the pin
        // succeeds, elsewhere it is the documented no-op.
        let ok = pin_current_thread_to_core(0);
        #[cfg(any(target_os = "windows", target_os = "linux"))]
        assert!(ok, "pinning to core 0 should succeed on this platform");
        #[cfg(not(any(target_os = "windows", target_os = "linux")))]
        assert!(!ok);
    }
}
