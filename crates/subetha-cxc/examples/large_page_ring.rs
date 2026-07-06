//! Rings backed by real large / huge pages - not a standalone region
//! that nothing reads, but the actual MPMC grid laid out IN the page
//! and streamed through.
//!
//! Three legs, one per ring family:
//!
//! 1. **Sharded MPMC grid on one large page** (per-producer FIFO).
//!    Every SPSC lane of the grid is carved from a single 2 MB-paged
//!    region, so the whole grid sits on a handful of large-page TLB
//!    entries instead of thousands of 4 KB ones. N producer threads +
//!    M consumer threads stream millions of items through it; integrity
//!    is checked (per-producer FIFO + exact total count).
//!
//! 2. **Vyukov MPMC ring on one large page** (global FIFO). The single
//!    shared-counter ring laid out in a large-page region, driven by N
//!    producers + M consumers; integrity is checked by exact count plus
//!    a sum-of-ids checksum (no item lost or duplicated).
//!
//! 3. **Cross-process SPSC ring on a shared large-page region** (both
//!    OSes). The parent lays a ring out in a large-page region a second
//!    process can attach to - a named `LargePageSection` on Windows, a
//!    hugetlbfs-backed `SharedHugepageRegion` on Linux - spawns a child
//!    that attaches to the SAME region, and streams items parent ->
//!    child through shared large-page physical memory. Only the
//!    region-attach mechanism (named section vs hugetlbfs path) is
//!    gated; the ring and the producer/consumer loop are shared.
//!
//! Large pages need a privilege / reservation that may be absent:
//!  - Windows: `SeLockMemoryPrivilege` ("Lock pages in memory").
//!  - Linux: reserved hugepages (`/proc/sys/vm/nr_hugepages`).
//!
//! When that is missing the example prints why and exits 0 - the ring
//! wiring itself is covered by the lib tests with a heap region.
//!
//! Run:
//!     cargo run --release --example large_page_ring -p subetha-cxc

fn main() {
    // Child-drain mode for the cross-process leg (both OSes); only the
    // region-attach call inside differs per platform.
    #[cfg(any(windows, target_os = "linux"))]
    {
        let args: Vec<String> = std::env::args().collect();
        if args.len() >= 5 && args[1] == "--drain" {
            #[cfg(windows)]
            windows_child_drain(
                &args[2],
                args[3].parse().expect("cap"),
                args[4].parse().expect("count"),
            );
            #[cfg(target_os = "linux")]
            linux_child_drain(
                &args[2],
                args[3].parse().expect("cap"),
                args[4].parse().expect("count"),
            );
            return;
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    {
        run_inprocess_grid();
        run_inprocess_vyukov();
        run_cross_process_section();
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        println!("large pages are only wired for Windows and Linux; \
                  this platform has no large-page backing.");
    }
}

// ---------------------------------------------------------------------
// Leg 1: in-process MPMC grid carved from one large page.
// ---------------------------------------------------------------------

#[cfg(any(windows, target_os = "linux"))]
fn run_inprocess_grid() {
    use std::collections::HashMap;
    use std::thread;
    use std::time::Instant;
    use subetha_cxc::spsc_ring::{spsc_ring_file_size, SPSC_PAYLOAD_BYTES};
    use subetha_cxc::SharedRingMpmc;

    const N_PRODUCERS: usize = 8;
    const N_CONSUMERS: usize = 4;
    const CAPACITY: usize = 4096;
    const ITEMS_EACH: u32 = 500_000;

    let total_items = ITEMS_EACH as u64 * N_PRODUCERS as u64;
    let need = spsc_ring_file_size(CAPACITY) * N_PRODUCERS;

    println!("== Leg 1: MPMC grid on ONE large page ==");
    println!("{N_PRODUCERS} producers x {N_CONSUMERS} consumers, \
              capacity {CAPACITY}/lane, {ITEMS_EACH} items each");
    println!("grid layout needs {need} bytes ({:.2} MB)", need as f64 / 1e6);

    let region = match acquire_region(need) {
        Ok(r) => r,
        Err(e) => {
            print_no_large_pages(&e);
            return;
        }
    };
    let backed_bytes = region_len(&region);
    println!("backed by a large-page region of {backed_bytes} bytes \
              ({:.2} MB, {} large pages)",
             backed_bytes as f64 / 1e6,
             backed_bytes / large_page_size());

    let (producers, consumers) = SharedRingMpmc::create_grid_in_region(
        region, N_PRODUCERS, N_CONSUMERS, CAPACITY,
    ).expect("grid laid out in the large-page region");

    let t0 = Instant::now();

    let producer_handles: Vec<_> = producers
        .into_iter()
        .enumerate()
        .map(|(pid, p)| {
            thread::spawn(move || {
                let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
                for seq in 0..ITEMS_EACH {
                    buf[..4].copy_from_slice(&(pid as u32).to_le_bytes());
                    buf[4..8].copy_from_slice(&seq.to_le_bytes());
                    while p.try_push(&buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    let target_per_consumer =
        (ITEMS_EACH as usize * N_PRODUCERS / N_CONSUMERS) as u32;
    let consumer_handles: Vec<_> = consumers
        .into_iter()
        .map(|c| {
            thread::spawn(move || -> u32 {
                // Per-producer FIFO check: each pid's seq must arrive in
                // order at whichever consumer owns its lane.
                let mut next: HashMap<u32, u32> = HashMap::new();
                let mut total: u32 = 0;
                let mut out = [0u8; SPSC_PAYLOAD_BYTES];
                while total < target_per_consumer {
                    if c.try_pop(&mut out).is_ok() {
                        let pid = u32::from_le_bytes(out[..4].try_into().unwrap());
                        let seq = u32::from_le_bytes(out[4..8].try_into().unwrap());
                        let want = next.entry(pid).or_insert(0);
                        assert_eq!(*want, seq,
                            "per-producer FIFO violated: pid {pid} \
                             expected {want} got {seq}");
                        *want += 1;
                        total += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                total
            })
        })
        .collect();

    for h in producer_handles {
        h.join().unwrap();
    }
    let mut drained: u64 = 0;
    for h in consumer_handles {
        drained += h.join().unwrap() as u64;
    }
    let elapsed = t0.elapsed();

    assert_eq!(drained, total_items, "every item delivered exactly once");
    println!("streamed {total_items} items through the large-page grid in {elapsed:?}");
    println!("{:.2} M items/s, integrity OK (per-producer FIFO + exact count)\n",
             total_items as f64 / elapsed.as_secs_f64() / 1e6);
}

// ---------------------------------------------------------------------
// Leg 2: Vyukov global-FIFO MPMC ring on one large page.
// ---------------------------------------------------------------------

#[cfg(any(windows, target_os = "linux"))]
fn run_inprocess_vyukov() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::Instant;
    use subetha_cxc::{ring_file_size, SharedRing, PAYLOAD_BYTES};

    const N_PRODUCERS: usize = 8;
    const N_CONSUMERS: usize = 4;
    const CAPACITY: usize = 65536;
    const ITEMS_EACH: u64 = 500_000;

    let total = ITEMS_EACH * N_PRODUCERS as u64;
    let need = ring_file_size(CAPACITY);

    println!("== Leg 2: Vyukov global-FIFO MPMC ring on one large page ==");
    println!("{N_PRODUCERS} producers x {N_CONSUMERS} consumers, \
              capacity {CAPACITY}, {ITEMS_EACH} items each");
    println!("ring layout needs {need} bytes ({:.2} MB)", need as f64 / 1e6);

    let region = match acquire_region(need) {
        Ok(r) => r,
        Err(e) => {
            print_no_large_pages(&e);
            return;
        }
    };
    let backed_bytes = region_len(&region);
    println!("backed by a large-page region of {backed_bytes} bytes \
              ({:.2} MB, {} large pages)",
             backed_bytes as f64 / 1e6,
             backed_bytes / large_page_size());

    let ring = Arc::new(
        SharedRing::create_in_region(region, CAPACITY)
            .expect("Vyukov ring laid out in the large-page region"),
    );
    // Every id in 0..total appears exactly once; the consumers' summed
    // ids equal the closed-form sum only when nothing was lost or
    // duplicated.
    let expected_sum: u64 = (0..total).sum();
    let drained = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();

    let producer_handles: Vec<_> = (0..N_PRODUCERS)
        .map(|pid| {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                let mut buf = [0u8; PAYLOAD_BYTES];
                for seq in 0..ITEMS_EACH {
                    let id = pid as u64 * ITEMS_EACH + seq;
                    buf[..8].copy_from_slice(&id.to_le_bytes());
                    while ring.try_push(&buf).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    let consumer_handles: Vec<_> = (0..N_CONSUMERS)
        .map(|_| {
            let ring = Arc::clone(&ring);
            let drained = Arc::clone(&drained);
            thread::spawn(move || -> u64 {
                let mut local_sum: u64 = 0;
                let mut out = [0u8; PAYLOAD_BYTES];
                loop {
                    if ring.try_pop(&mut out).is_ok() {
                        local_sum +=
                            u64::from_le_bytes(out[..8].try_into().unwrap());
                        drained.fetch_add(1, Ordering::AcqRel);
                    } else if drained.load(Ordering::Acquire) >= total {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                local_sum
            })
        })
        .collect();

    for h in producer_handles {
        h.join().unwrap();
    }
    let mut got_sum: u64 = 0;
    for h in consumer_handles {
        got_sum += h.join().unwrap();
    }
    let elapsed = t0.elapsed();

    assert_eq!(drained.load(Ordering::Acquire), total, "exact item count");
    assert_eq!(got_sum, expected_sum, "no item lost or duplicated");
    println!("streamed {total} items through the large-page Vyukov ring in {elapsed:?}");
    println!("{:.2} M items/s, integrity OK (exact count + sum-of-ids checksum)\n",
             total as f64 / elapsed.as_secs_f64() / 1e6);
}

// Per-OS region acquisition. Both region types implement `RegionOwner`,
// so the generic `create_grid_in_region` / `create_in_region` accept
// either.

#[cfg(windows)]
fn acquire_region(bytes: usize)
    -> std::io::Result<subetha_cxc::large_pages::LargePageRegion>
{
    use subetha_cxc::large_pages::{enable_lock_memory_privilege, LargePageRegion};
    enable_lock_memory_privilege()?;
    LargePageRegion::allocate(bytes)
}

#[cfg(target_os = "linux")]
fn acquire_region(bytes: usize)
    -> std::io::Result<subetha_cxc::hugepages::HugepageRegion>
{
    use subetha_cxc::hugepages::{HugepageRegion, HugepageSize, HUGEPAGE_2MB};
    let pages = bytes.div_ceil(HUGEPAGE_2MB);
    HugepageRegion::allocate(pages, HugepageSize::Mb2)
}

#[cfg(windows)]
fn region_len(r: &subetha_cxc::large_pages::LargePageRegion) -> usize { r.len() }

#[cfg(target_os = "linux")]
fn region_len(r: &subetha_cxc::hugepages::HugepageRegion) -> usize { r.len() }

#[cfg(windows)]
fn large_page_size() -> usize {
    subetha_cxc::large_pages::large_page_minimum().max(1)
}

#[cfg(target_os = "linux")]
fn large_page_size() -> usize { subetha_cxc::hugepages::HUGEPAGE_2MB }

#[cfg(any(windows, target_os = "linux"))]
fn print_no_large_pages(e: &std::io::Error) {
    println!("large pages unavailable on this host: {e}");
    #[cfg(windows)]
    println!("  grant 'Lock pages in memory' (secpol.msc) and re-login, \
              then run elevated.");
    #[cfg(target_os = "linux")]
    println!("  reserve hugepages: \
              sudo sysctl -w vm.nr_hugepages=512");
    println!("  (the ring wiring itself is proven by the lib region tests.)\n");
}

// ---------------------------------------------------------------------
// Leg 3: cross-process SPSC ring on a named large-page section.
// ---------------------------------------------------------------------

#[cfg(windows)]
fn run_cross_process_section() {
    use std::process::Command;
    use std::thread;
    use std::time::Instant;
    use subetha_cxc::large_pages::{enable_lock_memory_privilege, LargePageSection};
    use subetha_cxc::spsc_ring::{spsc_ring_file_size, SpscRingCore, SPSC_PAYLOAD_BYTES};

    const CAPACITY: usize = 1024;
    const COUNT: u32 = 200_000;

    println!("== Leg 3: cross-process SPSC ring on a named large-page section ==");

    if let Err(e) = enable_lock_memory_privilege() {
        println!("large-page section unavailable: {e}\n");
        return;
    }

    let name = format!("Local\\subetha_lpring_{}", std::process::id());
    let bytes = spsc_ring_file_size(CAPACITY);
    let section = match LargePageSection::create(&name, bytes) {
        Ok(s) => s,
        Err(e) => {
            println!("could not create large-page section: {e}\n");
            return;
        }
    };
    // Lay the SPSC ring out IN the shared section (writes the header).
    let ring = SpscRingCore::create_in_region(section, CAPACITY)
        .expect("ring laid out in the large-page section");
    println!("parent created section {name} ({bytes} bytes), ring header written");

    // Spawn ourselves as the draining child against the same name.
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(exe)
        .arg("--drain")
        .arg(&name)
        .arg(CAPACITY.to_string())
        .arg(COUNT.to_string())
        .spawn()
        .expect("spawn child drainer");

    let t0 = Instant::now();
    let producer = thread::spawn(move || {
        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
        for seq in 0..COUNT {
            buf[..4].copy_from_slice(&seq.to_le_bytes());
            while ring.try_push(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
        // Hold the ring (and thus the section mapping) until the child
        // has drained everything.
        ring
    });

    let status = child.wait().expect("child wait");
    let _ring = producer.join().unwrap();
    let elapsed = t0.elapsed();

    if status.success() {
        println!("parent pushed {COUNT} items; child drained + verified them \
                  through shared large-page memory in {elapsed:?}");
        println!("{:.2} M items/s cross-process, integrity OK\n",
                 COUNT as f64 / elapsed.as_secs_f64() / 1e6);
    } else {
        println!("child drainer exited with failure: {status:?}\n");
    }
}

#[cfg(windows)]
fn windows_child_drain(name: &str, capacity: usize, count: u32) {
    use subetha_cxc::large_pages::{enable_lock_memory_privilege, LargePageSection};
    use subetha_cxc::spsc_ring::{spsc_ring_file_size, SpscRingCore, SPSC_PAYLOAD_BYTES};

    enable_lock_memory_privilege().expect("child: lock-memory privilege");
    let bytes = spsc_ring_file_size(capacity);

    // The parent may not have created the section yet; retry the open.
    let section = loop {
        match LargePageSection::open(name, bytes) {
            Ok(s) => break s,
            Err(_) => std::hint::spin_loop(),
        }
    };
    let ring = SpscRingCore::open_in_region(section, capacity)
        .expect("child: open ring in section");

    let mut out = [0u8; SPSC_PAYLOAD_BYTES];
    let mut received: u32 = 0;
    while received < count {
        if ring.try_pop(&mut out).is_ok() {
            let seq = u32::from_le_bytes(out[..4].try_into().unwrap());
            assert_eq!(seq, received, "child: FIFO order violated");
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    // Exit 0 signals the parent the integrity check passed.
}

// ---------------------------------------------------------------------
// Leg 3 (Linux): cross-process SPSC ring on a hugetlbfs-backed region.
//
// The Linux analogue of the Windows named section: a hugetlbfs file
// mmap'd MAP_SHARED. A second process opens the same path and maps it,
// so the ring lives in shared hugepage physical memory. The attach
// mechanism (path vs section name) is the only platform-gated part.
// ---------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn hugetlbfs_ring_path() -> std::path::PathBuf {
    std::path::PathBuf::from(
        format!("/dev/hugepages/subetha_hpring_{}", std::process::id()),
    )
}

#[cfg(target_os = "linux")]
fn cross_process_pages(capacity: usize) -> usize {
    use subetha_cxc::hugepages::HUGEPAGE_2MB;
    use subetha_cxc::spsc_ring::spsc_ring_file_size;
    spsc_ring_file_size(capacity).div_ceil(HUGEPAGE_2MB).max(1)
}

#[cfg(target_os = "linux")]
fn run_cross_process_section() {
    use std::process::Command;
    use std::thread;
    use std::time::Instant;
    use subetha_cxc::hugepages::{HugepageSize, SharedHugepageRegion};
    use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

    const CAPACITY: usize = 1024;
    const COUNT: u32 = 200_000;

    println!("== Leg 3: cross-process SPSC ring on a hugetlbfs-backed region ==");

    let path = hugetlbfs_ring_path();
    let pages = cross_process_pages(CAPACITY);
    let region = match SharedHugepageRegion::create(&path, pages, HugepageSize::Mb2) {
        Ok(r) => r,
        Err(e) => {
            println!("hugetlbfs-backed section unavailable: {e}");
            println!("  mount a writable hugetlbfs and reserve pages: \
                      sudo mount -t hugetlbfs nodev /dev/hugepages && \
                      sudo chmod 1777 /dev/hugepages && \
                      sudo sysctl -w vm.nr_hugepages=512\n");
            return;
        }
    };
    // Lay the SPSC ring out IN the shared hugepage region (writes header).
    let ring = SpscRingCore::create_in_region(region, CAPACITY)
        .expect("ring laid out in the hugetlbfs region");
    println!("parent created hugetlbfs region {} ({pages} hugepage(s)), \
              ring header written", path.display());

    // Spawn ourselves as the draining child against the same path.
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(exe)
        .arg("--drain")
        .arg(&path)
        .arg(CAPACITY.to_string())
        .arg(COUNT.to_string())
        .spawn()
        .expect("spawn child drainer");

    let t0 = Instant::now();
    let producer = thread::spawn(move || {
        let mut buf = [0u8; SPSC_PAYLOAD_BYTES];
        for seq in 0..COUNT {
            buf[..4].copy_from_slice(&seq.to_le_bytes());
            while ring.try_push(&buf).is_err() {
                std::hint::spin_loop();
            }
        }
        // Hold the ring (and thus the hugepage mapping) until the child
        // has drained everything; the creator unlinks the file on drop.
        ring
    });

    let status = child.wait().expect("child wait");
    let _ring = producer.join().unwrap();
    let elapsed = t0.elapsed();

    if status.success() {
        println!("parent pushed {COUNT} items; child drained + verified them \
                  through shared hugepage memory in {elapsed:?}");
        println!("{:.2} M items/s cross-process, integrity OK\n",
                 COUNT as f64 / elapsed.as_secs_f64() / 1e6);
    } else {
        println!("child drainer exited with failure: {status:?}\n");
    }
}

#[cfg(target_os = "linux")]
fn linux_child_drain(path: &str, capacity: usize, count: u32) {
    use subetha_cxc::hugepages::{HugepageSize, SharedHugepageRegion};
    use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

    let pages = cross_process_pages(capacity);
    // The parent may not have created the file yet; retry the open.
    let region = loop {
        match SharedHugepageRegion::open(path, pages, HugepageSize::Mb2) {
            Ok(r) => break r,
            Err(_) => std::hint::spin_loop(),
        }
    };
    let ring = SpscRingCore::open_in_region(region, capacity)
        .expect("child: open ring in hugetlbfs region");

    let mut out = [0u8; SPSC_PAYLOAD_BYTES];
    let mut received: u32 = 0;
    while received < count {
        if ring.try_pop(&mut out).is_ok() {
            let seq = u32::from_le_bytes(out[..4].try_into().unwrap());
            assert_eq!(seq, received, "child: FIFO order violated");
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    // Exit 0 signals the parent the integrity check passed.
}
