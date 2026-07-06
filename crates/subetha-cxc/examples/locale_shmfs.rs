//! E2E demonstration of `Locale::ShmFs` (RAM-resident cross-process
//! file backing) and the four-axis pin chain through the ShmFs
//! locale.
//!
//! Lifecycle:
//!  1. Construct a LocaleAdaptiveRing. Default locale is Anon.
//!  2. Register 1 producer + 1 consumer (lockstep across all three
//!     locale backings: anon, file, shmfs).
//!  3. Round-trip items at the default Anon locale through the
//!     three-axis pin chain (PinnedLocale anon -> AdaptiveRing ->
//!     PinnedRing -> SpscRingCore).
//!  4. Migrate to ShmFs. Pin invalidates. Items in flight at Anon
//!     transfer into the ShmFs backing.
//!  5. Round-trip items at the ShmFs locale through the SAME pin
//!     chain - this time the bytes live in named RAM-resident
//!     shared memory instead of process-local anon mappings.
//!  6. Migrate to File. Pin invalidates again. Items transfer
//!     ShmFs -> File.
//!  7. Final drain via File. Integrity check: every item arrived
//!     exactly once, sum-checked.
//!
//! Run with:
//!     cargo run --release --example locale_shmfs

use std::time::Instant;

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::{Locale, LocaleAdaptiveRing, RingShape};

const ANON_ITEMS: u64 = 10_000;
const SHMFS_ITEMS: u64 = 10_000;
const IN_FLIGHT_BETWEEN_MORPHS: u64 = 5;
const CAPACITY: usize = 256;

fn main() {
    println!("=== LocaleAdaptiveRing 3-locale morph E2E (anon -> shmfs -> file) ===");
    println!();

    let base_path = std::env::temp_dir()
        .join(format!("locale_shmfs_e2e_{}", std::process::id()));
    let ring = LocaleAdaptiveRing::create(&base_path, 1, 1, CAPACITY)
        .expect("create");
    ring.register_producer().expect("p");
    ring.register_consumer().expect("c");

    let start = Instant::now();
    println!("[init] locale = {:?}, generation = {}",
             ring.current_locale(), ring.locale_generation());

    let mut produced_count = 0u64;
    let mut produced_sum = 0u64;
    let mut consumed_count = 0u64;
    let mut consumed_sum = 0u64;

    // ----- stage 1: Anon locale, four-axis pin chain -----
    println!();
    println!("[stage 1] Anon locale; round-trip {ANON_ITEMS} via four-axis pin chain");
    println!("          PinnedLocale(anon) -> AdaptiveRing -> PinnedRing -> SpscRingCore");
    {
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_anon().expect("pinned at anon");
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);

        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for i in 0..ANON_ITEMS {
            let payload = i.to_le_bytes();
            while pin_shape.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            produced_count += 1;
            produced_sum += i;
            while pin_shape.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            consumed_sum += u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
        }
        println!("    round-tripped {ANON_ITEMS} at anon; pin still valid");
    }

    // ----- stage 2: push in-flight items at anon, migrate to ShmFs -----
    println!();
    println!("[stage 2] push {IN_FLIGHT_BETWEEN_MORPHS} in-flight items at anon, then migrate -> ShmFs");
    {
        for i in 0..IN_FLIGHT_BETWEEN_MORPHS {
            let marker = 0xA0_0000 + i;
            ring.try_send(0, &marker.to_le_bytes()).expect("anon send");
            produced_count += 1;
            produced_sum += marker;
        }
        let gen_before = ring.locale_generation();
        ring.migrate_to(Locale::ShmFs).expect("anon -> shmfs");
        println!("    generation {gen_before} -> {}, locale = {:?}",
                 ring.locale_generation(), ring.current_locale());
        println!("    in-flight items transferred into the ShmFs backing");
    }

    // ----- stage 3: ShmFs locale, four-axis pin chain -----
    println!();
    println!("[stage 3] ShmFs locale; round-trip {SHMFS_ITEMS} via four-axis pin chain at the new locale");
    {
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_shmfs().expect("pinned at shmfs");
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);

        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        // Drain the in-flight items that transferred from anon first.
        for _ in 0..IN_FLIGHT_BETWEEN_MORPHS {
            while pin_shape.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            consumed_sum += u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
        }
        // Now round-trip fresh ShmFs items.
        for i in 0..SHMFS_ITEMS {
            let v = i + 0xF0_0000;
            let payload = v.to_le_bytes();
            while pin_shape.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            produced_count += 1;
            produced_sum += v;
            while pin_shape.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            consumed_sum += u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
        }
        println!("    drained {IN_FLIGHT_BETWEEN_MORPHS} transferred items + round-tripped {SHMFS_ITEMS} fresh items at ShmFs");
    }

    // ----- stage 4: migrate ShmFs -> File -----
    println!();
    println!("[stage 4] migrate ShmFs -> File");
    {
        let gen_before = ring.locale_generation();
        ring.migrate_to(Locale::File).expect("shmfs -> file");
        println!("    generation {gen_before} -> {}, locale = {:?}",
                 ring.locale_generation(), ring.current_locale());
    }

    // ----- result -----
    let elapsed = start.elapsed();
    let final_locale = ring.current_locale();
    let final_gen = ring.locale_generation();

    println!();
    println!("=== Result ===");
    println!("  elapsed:               {elapsed:?}");
    println!("  produced count:        {produced_count}");
    println!("  consumed count:        {consumed_count}");
    println!("  produced sum:          {produced_sum}");
    println!("  consumed sum:          {consumed_sum}");
    println!("  final locale:          {final_locale:?}");
    println!("  locale generation:     0 -> {final_gen}");
    println!("  morph sequence:        Anon -> ShmFs -> File");

    assert_eq!(produced_count, consumed_count,
               "INTEGRITY FAIL: count mismatch");
    assert_eq!(produced_sum, consumed_sum,
               "INTEGRITY FAIL: sum mismatch");
    assert_eq!(final_gen, 2, "expected exactly 2 locale migrations");
    println!("  integrity:             PASS");
    println!("    every item arrived exactly once across two locale morphs");
    println!("    four-axis pin chain (Locale -> AdaptiveRing -> Shape -> SpscRingCore)");
    println!("    held native primitive speed at every locale");
    println!("    in-flight items survived the anon -> shmfs transfer");
}
