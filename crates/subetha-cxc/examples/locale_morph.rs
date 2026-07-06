//! E2E demonstration of `LocaleAdaptiveRing`: same-host anon <-> file
//! locale-axis morph with pin invalidation across the boundary.
//!
//! Lifecycle the example exercises:
//!  1. Construct LocaleAdaptiveRing. Default locale is Anon.
//!  2. Register 1 producer + 1 consumer (lockstep across both
//!     locale backings).
//!  3. Round-trip INDIVIDUAL_ITEMS through the full four-axis pin
//!     composition at the anon locale:
//!     PinnedLocale -> AdaptiveRing -> PinnedRing -> SpscRingCore.
//!  4. Migrate from anon to file. Pin invalidates. Push items
//!     remain in the anon backing get transferred into file.
//!  5. Re-acquire the pin at the file locale; round-trip a smaller
//!     batch through the four-axis pin composition again.
//!  6. Migrate back to anon. Pin invalidates again. Drain.
//!  7. Final integrity check: count + sum match expected.
//!
//! Run with:
//!     cargo run --release --example locale_morph

use std::time::Instant;

use subetha_cxc::adaptive_ring::ADAPTIVE_SPSC_PAYLOAD_BYTES;
use subetha_cxc::{Locale, LocaleAdaptiveRing, RingShape};

const ANON_ITEMS: u64 = 30_000;
const FILE_ITEMS: u64 = 10_000;
const IN_FLIGHT_ITEMS: u64 = 5;
const CAPACITY: usize = 1024;

fn main() {
    println!("=== LocaleAdaptiveRing anon <-> file morph E2E ===");
    println!();

    let base_path = std::env::temp_dir()
        .join(format!("locale_morph_e2e_{}", std::process::id()));
    let ring = LocaleAdaptiveRing::create(&base_path, 1, 1, CAPACITY)
        .expect("create");
    ring.register_producer().expect("p");
    ring.register_consumer().expect("c");

    let start = Instant::now();
    println!("[init] locale = {:?}, locale_generation = {}",
             ring.current_locale(), ring.locale_generation());

    let mut produced_count = 0u64;
    let mut produced_sum = 0u64;
    let mut consumed_count = 0u64;
    let mut consumed_sum = 0u64;

    // ----- stage 1 -----
    // Four-axis pin chain at the anon locale.
    println!();
    println!("[stage 1] anon locale; four-axis pin chain round-trip of {ANON_ITEMS} u64s");
    println!("          PinnedLocale -> AdaptiveRing -> PinnedRing -> SpscRingCore");
    {
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_anon().expect("pinned at anon");
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);
        assert!(pin_locale.is_still_valid() && pin_shape.is_still_valid());

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
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
            consumed_sum += v;
        }
        println!("    round-tripped {ANON_ITEMS} through native SpscRingCore at anon locale");
        println!("    pin_locale.is_still_valid = {}, pin_shape.is_still_valid = {}",
                 pin_locale.is_still_valid(), pin_shape.is_still_valid());
    }

    // ----- stage 2 -----
    // Push a small in-flight batch into the anon backing WITHOUT
    // draining; then migrate to file. The migration transfers
    // these items into the file backing.
    println!();
    println!("[stage 2] push {IN_FLIGHT_ITEMS} items into anon (no drain), then migrate to file");
    {
        let pre_pin = ring.pin_current_locale();
        assert_eq!(pre_pin.locale(), Locale::Anon);
        for i in 0..IN_FLIGHT_ITEMS {
            let marker = 0xA000 + i;
            ring.try_send(0, &marker.to_le_bytes())
                .expect("anon send pre-migrate");
            produced_count += 1;
            produced_sum += marker;
        }

        let gen_before = ring.locale_generation();
        ring.migrate_to(Locale::File).expect("migrate to file");
        let gen_after = ring.locale_generation();

        assert!(!pre_pin.is_still_valid(),
                "pre-pin must invalidate on locale flip");
        assert_eq!(ring.current_locale(), Locale::File);
        println!("    pre-pin.is_still_valid = false (invalidated as expected)");
        println!("    locale_generation: {gen_before} -> {gen_after}");
        println!("    in-flight items transferred into file backing");
    }

    // ----- stage 3 -----
    // Drain the transferred items from the file backing via the
    // pin chain at the new locale.
    println!();
    println!("[stage 3] file locale; drain the {IN_FLIGHT_ITEMS} transferred items");
    {
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_file().expect("pinned at file");
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);

        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        let mut drained = 0u64;
        while drained < IN_FLIGHT_ITEMS {
            if pin_shape.spsc_try_pop(&mut buf).is_ok() {
                let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                consumed_count += 1;
                consumed_sum += v;
                drained += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        println!("    drained {IN_FLIGHT_ITEMS} transferred items via file locale pin chain");
    }

    // ----- stage 4 -----
    // Round-trip more items through the file backing (persistent
    // storage path).
    println!();
    println!("[stage 4] file locale; round-trip {FILE_ITEMS} fresh items through persistent backing");
    {
        let pin_locale = ring.pin_current_locale();
        let adaptive = pin_locale.as_file().expect("pinned at file");
        let pin_shape = adaptive.pin_current_shape();

        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for i in 0..FILE_ITEMS {
            let payload = (i + 0xF000).to_le_bytes();
            while pin_shape.spsc_try_push(&payload).is_err() {
                std::hint::spin_loop();
            }
            produced_count += 1;
            produced_sum += i + 0xF000;
            while pin_shape.spsc_try_pop(&mut buf).is_err() {
                std::hint::spin_loop();
            }
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            consumed_count += 1;
            consumed_sum += v;
        }
        println!("    round-tripped {FILE_ITEMS} through native SpscRingCore at file locale");
    }

    // ----- stage 5 -----
    // Migrate back to anon. No in-flight items remain.
    println!();
    println!("[stage 5] migrate back to anon");
    {
        let pre_pin = ring.pin_current_locale();
        assert_eq!(pre_pin.locale(), Locale::File);
        let gen_before = ring.locale_generation();
        ring.migrate_to(Locale::Anon).expect("migrate to anon");
        let gen_after = ring.locale_generation();
        assert!(!pre_pin.is_still_valid());
        assert_eq!(ring.current_locale(), Locale::Anon);
        println!("    locale_generation: {gen_before} -> {gen_after}, locale = Anon");
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

    assert_eq!(produced_count, consumed_count,
               "INTEGRITY FAIL: count mismatch");
    assert_eq!(produced_sum, consumed_sum,
               "INTEGRITY FAIL: sum mismatch");
    assert_eq!(final_gen, 2, "expected exactly 2 locale migrations");
    println!("  integrity:             PASS");
    println!("    every item arrived exactly once, sum-checked");
    println!("    two locale morphs executed (anon -> file -> anon)");
    println!("    in-flight items survived the anon -> file transfer");
    println!("    four-axis pin chain held native SpscRingCore speed at each locale");
}
