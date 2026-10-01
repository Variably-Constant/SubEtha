//! Properties that hold for every value the pointer types accept: a
//! capability never sits at an address misaligned for its type, and a
//! `RaspBatch`'s SIMD checks give its scalar check's answer.

use subetha_pointers::adaptive_cheri_pointer::{
    CapabilityPermission, ReadableCapability, WritableCapability,
};
use subetha_pointers::adaptive_rasp_batch::{RaspBatch, RaspPermission};

const READ: u32 = CapabilityPermission::Read as u32;
const READ_WRITE: u32 = CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32;

#[test]
fn a_readable_capability_is_not_narrowed_to_a_misaligned_address() {
    let storage = vec![0u64; 4];
    let (cap, anchor) = ReadableCapability::from_slice(&storage, READ);
    let base = anchor.as_ptr() as usize;
    assert!(
        cap.narrow(base + 1, 8, READ).is_err(),
        "a u64 capability was narrowed to an address one byte past a u64"
    );
}

#[test]
fn a_readable_capability_is_not_made_at_a_misaligned_address() {
    let storage = [0u64; 4];
    let base = storage.as_ptr() as usize;
    let made = unsafe { ReadableCapability::new((base + 1) as *const u64, base, 32, READ) };
    assert!(made.is_err(), "a u64 capability was made one byte past a u64");
}

#[test]
fn a_writable_capability_is_not_narrowed_to_a_misaligned_address() {
    let mut storage = [0u64; 4];
    let base = storage.as_mut_ptr() as usize;
    #[allow(unused_mut)]
    let mut writable =
        unsafe { WritableCapability::new(storage.as_mut_ptr(), base, 32, READ_WRITE) }
            .expect("an aligned capability over the whole array");
    assert!(
        writable.narrow(base + 1, 8, READ_WRITE).is_err(),
        "a u64 capability was narrowed to an address one byte past a u64"
    );
}

#[test]
fn raspbatch_simd_checks_agree_with_the_scalar_check_across_the_sign_bit() {
    // Each entry's base sits below 2^63 and its pointer above; push_raw
    // accepts them, and every check must read them the same way.
    let mut batch: RaspBatch<u8> = RaspBatch::new();
    for i in 0..16u64 {
        batch
            .push_raw(0x8000_0000_0000_0000 + i, 0x7FFF_FFFF_FFFF_FFF0 + i, 0x40, RaspPermission::Read as u32)
            .expect("the entry lies inside its region");
    }
    let scalar = batch.count_valid_scalar();
    assert_eq!(scalar, 16, "the scalar check counts every entry valid");
    assert_eq!(batch.count_valid(), scalar, "the dispatched count disagrees with the scalar one");
    assert_eq!(
        batch.check_read_all(),
        batch.check_read_all_scalar(),
        "the dispatched per-entry results disagree with the scalar ones"
    );
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    if std::is_x86_feature_detected!("avx2") {
        assert_eq!(unsafe { batch.count_valid_avx2() }, scalar, "the AVX2 count disagrees");
        assert_eq!(
            unsafe { batch.check_read_all_avx2() },
            batch.check_read_all_scalar(),
            "the AVX2 per-entry results disagree"
        );
    }
}
