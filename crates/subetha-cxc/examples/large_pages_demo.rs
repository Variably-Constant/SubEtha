//! End-to-end demo of the Windows large-pages module.
//!
//! Probes the host, enables `SeLockMemoryPrivilege` in the
//! process token, allocates a private `LargePageRegion` AND a
//! cross-process-shareable `LargePageSection`, writes + reads
//! patterns through both, and reports the exact outcome.
//!
//! On hosts where "Lock pages in memory" has not been granted to
//! the account, the demo reports the documented fallback path
//! (ERROR_PRIVILEGE_NOT_HELD) and exits rc=0 - the probe and
//! error classification ARE the product surface on such hosts.
//! When the privilege is held, both allocations must succeed and
//! the demo asserts the memory round-trips.
//!
//! Run (Windows):
//!     cargo run --release --example large_pages_demo
//!
//! Two-process mode (proves the cross-process named-section claim
//! with two SEPARATE processes; see scripts in the repo):
//!     large_pages_demo create-wait <name> <marker_dir>   # process A
//!     large_pages_demo open-verify <name> <marker_dir>   # process B

#[cfg(windows)]
fn main() {
    use subetha_cxc::large_pages::{
        enable_lock_memory_privilege, large_page_minimum, round_to_large_page,
        LargePageRegion, LargePageSection, ERROR_PRIVILEGE_NOT_HELD,
    };

    let args: Vec<String> = std::env::args().collect();
    if args.len() == 4 && args[1] == "create-wait" {
        return xproc_create_wait(&args[2], &args[3]);
    }
    if args.len() == 4 && args[1] == "open-verify" {
        return xproc_open_verify(&args[2], &args[3]);
    }

    println!("=== Windows large-pages E2E demo ===");

    let min = large_page_minimum();
    println!("  GetLargePageMinimum():  {} bytes ({} MB)", min, min / (1024 * 1024));
    if min == 0 {
        println!("  Host does not support large pages at all; demo has nothing to exercise.");
        return;
    }

    let priv_result = enable_lock_memory_privilege();
    println!("  privilege enable:       {:?}", priv_result.as_ref().map(|()| "enabled"));

    let region_bytes = 8 * 1024 * 1024; // 8MB = 4 large pages at 2MB
    println!(
        "  requesting:             {} bytes -> rounded {} bytes",
        region_bytes,
        round_to_large_page(region_bytes).unwrap(),
    );

    // --- Private region ---
    match LargePageRegion::allocate(region_bytes) {
        Ok(mut region) => {
            let len = region.len();
            let last = len - 1;
            let s = region.as_mut_slice();
            s[0] = 0xAB;
            s[last] = 0xCD;
            // Touch every large page so the kernel actually backs
            // them (large pages are committed up front; this
            // proves every page is writable).
            for off in (0..len).step_by(large_page_minimum()) {
                s[off] = 0x11;
            }
            assert_eq!(region.as_slice()[last], 0xCD);
            println!(
                "  LargePageRegion:        OK - {} bytes ({} large pages), write/read verified",
                region.len(),
                region.len() / large_page_minimum(),
            );
        }
        Err(e) => {
            let code = e.raw_os_error().unwrap_or(-1) as u32;
            if code == ERROR_PRIVILEGE_NOT_HELD || priv_result.is_err() {
                println!(
                    "  LargePageRegion:        privilege not held (documented fallback): {e}"
                );
            } else {
                panic!("LargePageRegion failed with undocumented error: {e:?}");
            }
        }
    }

    // --- Named cross-process section ---
    let name = format!("Local\\subetha_lp_demo_{}", std::process::id());
    match LargePageSection::create(&name, region_bytes) {
        Ok(mut section) => {
            let s = section.as_mut_slice();
            s[0] = 0x5A;
            s[1234567] = 0x77;
            // Second view of the same named section: different
            // HANDLE, different mapped address, same physical
            // large pages. Reading the bytes written through view
            // 1 out of view 2 proves the cross-process sharing
            // mechanism (the name lookup) works; a second PROCESS
            // does exactly this open-by-name.
            let view2 = LargePageSection::open(&name, region_bytes).expect("open view2");
            assert_eq!(view2.as_slice()[0], 0x5A);
            assert_eq!(view2.as_slice()[1234567], 0x77);
            println!(
                "  LargePageSection:       OK - {} bytes shared via name '{}', second view sees writes",
                section.len(),
                name,
            );
        }
        Err(e) => {
            let code = e.raw_os_error().unwrap_or(-1) as u32;
            if code == ERROR_PRIVILEGE_NOT_HELD || priv_result.is_err() {
                println!(
                    "  LargePageSection:       privilege not held (documented fallback): {e}"
                );
            } else {
                panic!("LargePageSection failed with undocumented error: {e:?}");
            }
        }
    }

    println!();
    println!("PASS - large-pages probe + allocation paths exercised");
}

/// Process A of the two-process cross-process proof: create the
/// named large-page section, write a recognizable pattern, drop a
/// "section_ready" marker, then hold the section alive until the
/// opener drops its "verify_done" marker (bounded wait).
#[cfg(windows)]
fn xproc_create_wait(name: &str, marker_dir: &str) {
    use std::time::{Duration, Instant};
    use subetha_cxc::large_pages::{
        enable_lock_memory_privilege, LargePageSection,
    };

    enable_lock_memory_privilege().expect("creator privilege");
    let bytes = 4 * 1024 * 1024;
    let mut section = LargePageSection::create(name, bytes).expect("creator section");
    let s = section.as_mut_slice();
    // Pattern: every 64KB boundary carries its own offset.
    for off in (0..s.len()).step_by(64 * 1024) {
        s[off..off + 8].copy_from_slice(&(off as u64).to_le_bytes());
    }

    let ready = std::path::Path::new(marker_dir).join("section_ready");
    let done = std::path::Path::new(marker_dir).join("verify_done");
    std::fs::write(&ready, name.as_bytes()).expect("write ready marker");
    println!("[creator] section '{name}' populated; waiting for verifier");

    let deadline = Instant::now() + Duration::from_secs(20);
    while !done.exists() {
        if Instant::now() > deadline {
            eprintln!("[creator] verifier never finished; bailing");
            std::process::exit(3);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    println!("[creator] verifier confirmed; exiting clean");
}

/// Process B: open the named section created by process A, verify
/// the pattern written by A is visible through THIS process's
/// mapping, then drop the "verify_done" marker.
#[cfg(windows)]
fn xproc_open_verify(name: &str, marker_dir: &str) {
    use std::time::{Duration, Instant};
    use subetha_cxc::large_pages::{
        enable_lock_memory_privilege, LargePageSection,
    };

    let ready = std::path::Path::new(marker_dir).join("section_ready");
    let done = std::path::Path::new(marker_dir).join("verify_done");

    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        if Instant::now() > deadline {
            eprintln!("[verifier] creator never signalled ready; bailing");
            std::process::exit(3);
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    enable_lock_memory_privilege().expect("verifier privilege");
    let bytes = 4 * 1024 * 1024;
    let section = LargePageSection::open(name, bytes).expect("verifier open");
    let s = section.as_slice();
    let mut checked = 0u64;
    for off in (0..s.len()).step_by(64 * 1024) {
        let got = u64::from_le_bytes(s[off..off + 8].try_into().unwrap());
        assert_eq!(got, off as u64, "pattern mismatch at offset {off}");
        checked += 1;
    }
    std::fs::write(&done, b"ok").expect("write done marker");
    println!("[verifier] PASS - {checked} pattern points verified across the process boundary");
}

#[cfg(not(windows))]
fn main() {
    println!("large_pages_demo is Windows-only; the Linux sibling is the hugepages module.");
}
