//! E2E: hugepage / large-page-backed `AdaptiveRing` (opt-in).
//!
//! Constructs an `AdaptiveRing` whose every backing is laid out in huge /
//! large pages (Linux `MAP_HUGETLB`, Windows `MEM_LARGE_PAGES`, FreeBSD
//! `MAP_ALIGNED_SUPER`, macOS x86_64 `VM_FLAGS_SUPERPAGE_SIZE_2MB`) via
//! `AdaptiveRing::create_hugepage`, then streams
//! items through two
//! distinct backing types - the SPSC ring (`SpscRingCore`) and, after a
//! morph, the Vyukov global-FIFO ring (`SharedRing`) - verifying
//! integrity end to end. This proves the composed adaptive ring is fully
//! functional on hugepage-resident memory, not just that a region can be
//! allocated.
//!
//! Hugepages are an opt-in that needs a privilege / reservation:
//!  - Linux: reserved hugepages (`sudo sysctl -w vm.nr_hugepages=512`).
//!  - Windows: `SeLockMemoryPrivilege` ("Lock pages in memory").
//!  - FreeBSD: none - superpages are a transparent hint
//!    (`vm.pmap.pg_ps_enabled`), so the path normally just works.
//!  - macOS: none (x86_64 only) - the 2 MB superpage is requested on
//!    demand; Apple Silicon has no userspace superpage API.
//!
//! When that is absent the example prints why and exits 0 - the ring
//! wiring itself is covered by the lib tests with standard backings.
//!
//! Run:
//!     cargo run --release --example adaptive_ring_hugepage -p subetha-cxc

#[cfg(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"))]
fn main() {
    use std::time::Instant;
    use subetha_cxc::{AdaptiveRing, RingShape};

    const CAPACITY: usize = 4096;
    const N: u64 = 200_000;
    const MAX_P: usize = 4;
    const MAX_C: usize = 2;

    println!("=== AdaptiveRing on HUGEPAGES E2E (opt-in, cross-platform) ===");

    let ring = match AdaptiveRing::create_hugepage(MAX_P, MAX_C, CAPACITY) {
        Ok(r) => r,
        Err(e) => {
            println!("hugepage-backed AdaptiveRing unavailable: {e:?}");
            #[cfg(target_os = "linux")]
            println!("  reserve hugepages: sudo sysctl -w vm.nr_hugepages=512");
            #[cfg(windows)]
            println!("  grant 'Lock pages in memory' (secpol.msc), re-login, \
                      run elevated.");
            #[cfg(target_os = "freebsd")]
            println!("  superpages are transparent (vm.pmap.pg_ps_enabled=1); \
                      an aligned mmap failing here is unexpected.");
            #[cfg(target_os = "macos")]
            println!("  macOS x86_64 superpages are requested on demand; failure \
                      means 2 MB contiguous physical memory was unavailable, or \
                      this is Apple Silicon (no userspace superpage API).");
            println!("  (the create_hugepage wiring is also covered by the \
                      lib region tests.)");
            return;
        }
    };
    println!("[init] AdaptiveRing backings (SPSC + {MAX_P} MPSC + {MAX_P} MPMC \
              + Vyukov)");
    println!("       all laid out in huge/large pages, capacity \
              {CAPACITY}/backing");

    let mut out = [0u8; 64];

    // --- Leg A: SPSC backing (SpscRingCore on hugepages), in-order ---
    // Push/pop go through a PinnedRing handle that snapshots the current
    // shape; the pin is dropped before the morph below.
    let spsc_elapsed = {
        let pin = ring.pin_current_shape();
        let t0 = Instant::now();
        for i in 0..N {
            let payload = i.to_le_bytes();
            while pin.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            while pin.spsc_try_pop(&mut out).is_err() {
                std::hint::spin_loop();
            }
            let v = u64::from_le_bytes(out[..8].try_into().unwrap());
            assert_eq!(v, i, "SPSC leg: out-of-order at {i}");
        }
        t0.elapsed()
    };
    println!("[leg A: SPSC]   {N} items round-tripped in order in {spsc_elapsed:?}");

    // --- Leg B: Vyukov backing (SharedRing on hugepages), after morph ---
    // The SPSC backlog is fully drained above, so the morph is clean; it
    // bumps the pin generation, so we take a fresh pin for the new shape.
    ring.morph_to(RingShape::Vyukov).expect("morph to Vyukov");
    let pin = ring.pin_current_shape();
    let t1 = Instant::now();
    let mut vy_sum = 0u64;
    let mut vy_count = 0u64;
    for i in 0..N {
        let payload = i.to_le_bytes();
        while pin.vyukov_try_push(&payload).is_err() {
            std::hint::spin_loop();
        }
        while pin.vyukov_try_pop(&mut out).is_err() {
            std::hint::spin_loop();
        }
        vy_sum += u64::from_le_bytes(out[..8].try_into().unwrap());
        vy_count += 1;
    }
    let vy_elapsed = t1.elapsed();
    let expected: u64 = (0..N).sum();
    assert_eq!(vy_count, N, "Vyukov leg: count");
    assert_eq!(vy_sum, expected, "Vyukov leg: sum checksum");
    println!("[leg B: Vyukov] {N} items round-tripped in {vy_elapsed:?} \
              (sum checksum OK)");

    println!();
    println!("=== Result ===");
    println!("  SPSC + Vyukov backings both hugepage-resident and functional");
    println!("  integrity: PASS (in-order SPSC + sum-checksum Vyukov)");
}

#[cfg(not(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos")))]
fn main() {
    eprintln!("AdaptiveRing hugepage backing is wired for Linux, Windows, FreeBSD + macOS.");
}
