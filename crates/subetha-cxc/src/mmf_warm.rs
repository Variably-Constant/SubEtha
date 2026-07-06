//! MMF warm-up: prefault a freshly mapped region in one call
//! instead of paying a page fault per 4 KiB on first touch.
//!
//! A newly mapped multi-megabyte ring is pure fault surface: every
//! first-touch store traps, allocates, and resumes - thousands of
//! soft faults front-loaded onto the first pass through the ring,
//! exactly where latency measurements start. Each platform has a
//! one-call populate:
//!
//! - **Linux**: `madvise(MADV_POPULATE_WRITE)` (kernel 5.14+)
//!   prefaults page tables writable; unlike `MADV_WILLNEED` it
//!   "does not hide errors, can be applied to (parts of) existing
//!   mappings and will always populate" (madvise(2)). Kernels
//!   without it return `EINVAL`; the fallback is `MADV_WILLNEED`.
//! - **Windows**: `PrefetchVirtualMemory` (Windows 8+) brings the
//!   ranges in "using large, concurrent I/O requests" instead of
//!   the "many smaller I/Os that would be issued via page
//!   faulting", and is documented as purely advisory.
//! - **FreeBSD / macOS / other unix**: `madvise(MADV_WILLNEED)`.
//!
//! All paths are advisory: failure is reported but never fatal,
//! and the region works identically (just lazier) when the call
//! does nothing.
//!
//! # Measured per-platform verdict (32 MiB ring, open-side attach)
//!
//! - **Linux**: `MADV_POPULATE_WRITE` moves the fault storm out of
//!   the traffic path - first full drain drops from 54-63 ms
//!   (lazy) to 7-13 ms (populated), with the cost paid once at
//!   attach. ENABLED by default.
//! - **Windows**: `PrefetchVirtualMemory` on a page-cache-hot
//!   backing measured pure overhead (+6 ms on open, no drain win),
//!   so the automatic path SKIPS it. `SUBETHA_MMF_WARM=1` forces
//!   it on for the case the API is documented for: cold files
//!   paged out to disk, where one large batched I/O beats
//!   per-page demand faults.

/// Prefault `len` bytes at `ptr`. Returns whether the platform
/// call reported success; callers treat `false` as "lazy faulting
/// it is", not as an error.
///
/// # Safety
/// `ptr..ptr+len` must be a live mapping owned by the caller.
pub unsafe fn warm_region(ptr: *mut u8, len: usize) -> bool {
    if len == 0 {
        return true;
    }
    // A/B escape hatch: SUBETHA_NO_MMF_WARM=1 restores lazy
    // demand-faulting everywhere.
    {
        use std::sync::OnceLock;
        static DISABLED: OnceLock<bool> = OnceLock::new();
        if *DISABLED.get_or_init(|| {
            std::env::var_os("SUBETHA_NO_MMF_WARM").is_some_and(|v| v == "1")
        }) {
            return false;
        }
    }
    #[cfg(target_os = "linux")]
    unsafe {
        // POPULATE_WRITE: rings are written by their owner right
        // after creation; write-faulting now resolves the pages to
        // their final writable state in one pass.
        if libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_POPULATE_WRITE)
            == 0
        {
            return true;
        }
        libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_WILLNEED) == 0
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    unsafe {
        libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_WILLNEED) == 0
    }
    #[cfg(windows)]
    unsafe {
        // Off by default: measured pure overhead on page-cache-hot
        // backings (see module docs). SUBETHA_MMF_WARM=1 engages it
        // for cold-file attach workloads.
        use std::sync::OnceLock;
        static FORCED: OnceLock<bool> = OnceLock::new();
        if !*FORCED.get_or_init(|| {
            std::env::var_os("SUBETHA_MMF_WARM").is_some_and(|v| v == "1")
        }) {
            return false;
        }
        use windows_sys::Win32::System::Memory::{
            PrefetchVirtualMemory, WIN32_MEMORY_RANGE_ENTRY,
        };
        let range = WIN32_MEMORY_RANGE_ENTRY {
            VirtualAddress: ptr as *mut core::ffi::c_void,
            NumberOfBytes: len,
        };
        PrefetchVirtualMemory(
            windows_sys::Win32::System::Threading::GetCurrentProcess(),
            1,
            &range,
            0,
        ) != 0
    }
    #[cfg(not(any(unix, windows)))]
    {
        _ = ptr;
        true
    }
}

/// Warm an entire mutable mapping.
pub fn warm_mmap(mmap: &mut memmap2::MmapMut) -> bool {
    unsafe { warm_region(mmap.as_mut_ptr(), mmap.len()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warming_an_anon_mapping_succeeds_and_leaves_it_usable() {
        let mut mmap = memmap2::MmapOptions::new()
            .len(4 * 1024 * 1024)
            .map_anon()
            .expect("anon map");
        let ok = unsafe { warm_region(mmap.as_mut_ptr(), mmap.len()) };
        println!("warm_region reported: {ok}");
        // Whatever the report, the mapping must work end to end.
        let len = mmap.len();
        mmap[0] = 0x5A;
        mmap[len - 1] = 0xA5;
        assert_eq!(mmap[0], 0x5A);
        assert_eq!(mmap[len - 1], 0xA5);
    }

    #[test]
    fn zero_len_is_trivially_true() {
        let mut byte = 0u8;
        assert!(unsafe { warm_region(&mut byte as *mut u8, 0) });
    }
}
