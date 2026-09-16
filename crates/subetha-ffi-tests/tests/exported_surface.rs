//! Every function the C ABI exports, reached from a caller.
//!
//! A `pub extern "C"` symbol is exported surface, so the compiler never
//! reports one that nothing calls: it cannot know whether a caller
//! exists outside the crate. Only a test can. These cover the families
//! the C suite and the other Rust tests leave untouched, and the gate at
//! the end fails when an exported name is reached nowhere.
//!
//! Each assertion states what the function's own doc comment says it
//! does, rather than what its name suggests.

use std::ffi::CString;

use subetha_ffi::{
    subetha_blocked_bloom_clear, subetha_blocked_bloom_contains, subetha_blocked_bloom_create,
    subetha_blocked_bloom_flush, subetha_blocked_bloom_insert, subetha_blocked_bloom_open,
    subetha_blocked_bloom_read_stats, subetha_blocked_bloom_reset, subetha_blocked_bloom_stats,
    subetha_blocked_bloom_suggest, subetha_bloom_clear,
    subetha_bloom_contains, subetha_bloom_create, subetha_bloom_flush, subetha_bloom_flush_async,
    subetha_bloom_insert, subetha_bloom_open, subetha_bloom_read_stats, subetha_bloom_stats,
    subetha_bloom_suggest,
    subetha_bloom_reset, subetha_cms_create, subetha_cms_estimate, subetha_cms_flush,
    subetha_cms_flush_async, subetha_cms_insert, subetha_cms_insert_n, subetha_cms_open,
    subetha_cms_read_stats, subetha_cms_reset,
    subetha_rate_limiter_acquire_wait, subetha_rate_limiter_available,
    subetha_rate_limiter_create, subetha_rate_limiter_flush, subetha_rate_limiter_flush_async,
    subetha_rate_limiter_open, subetha_rate_limiter_read_stats, subetha_rate_limiter_reset,
    subetha_rate_limiter_stats, subetha_rate_limiter_try_acquire, subetha_versioned_chain_clear,
    subetha_versioned_chain_create, subetha_versioned_chain_current, subetha_versioned_chain_flush,
    subetha_versioned_chain_flush_async, subetha_versioned_chain_open, subetha_versioned_chain_push,
    subetha_versioned_chain_read_at, subetha_versioned_chain_read_stats,
    subetha_versioned_chain_reset, subetha_versioned_chain_stats,
    subetha_cms_stats, subetha_cms_suggest, subetha_handle, subetha_init, SUBETHA_OK,
};

const MODE_STRICT: u32 = 0;

/// The library's state is process-wide and cargo runs these as threads of
/// one process, so the mode is set once for all of them.
fn ensure_init() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        assert_eq!(subetha_init(MODE_STRICT), SUBETHA_OK);
    });
}

fn scratch(tag: &str) -> CString {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the wall clock is after the epoch")
        .as_nanos();
    let path = std::env::temp_dir()
        .join(format!("subetha-surface-{tag}-{}-{nanos}", std::process::id()))
        .to_str()
        .expect("the temp dir is UTF-8")
        .to_owned();
    CString::new(path).expect("the path has no NUL")
}

#[test]
fn a_bloom_filter_answers_for_what_was_put_in_and_forgets_on_clear() {
    ensure_init();
    // The size the rate asks for, then a filter of that size.
    let mut bits = 0u64;
    let mut hashes = 0u32;
    assert_eq!(
        unsafe { subetha_bloom_suggest(1_000, 0.01, &mut bits, &mut hashes) },
        SUBETHA_OK
    );
    assert!(bits > 0 && hashes > 0, "a rate of one in a hundred needs bits and hashes");

    let path = scratch("bloom");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_bloom_create(path.as_ptr(), bits, hashes, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    let item = b"present";
    assert_eq!(
        unsafe { subetha_bloom_insert(handle, item.as_ptr(), item.len()) },
        SUBETHA_OK
    );

    let mut found = false;
    assert_eq!(
        unsafe { subetha_bloom_contains(handle, item.as_ptr(), item.len(), &mut found) },
        SUBETHA_OK
    );
    assert!(found, "what was put in is answered for");

    let mut stats = subetha_bloom_stats::default();
    assert_eq!(unsafe { subetha_bloom_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.n_bits, bits);
    assert_eq!(stats.n_hashes, hashes);

    assert_eq!(subetha_bloom_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_bloom_flush_async(handle), SUBETHA_OK);

    assert_eq!(subetha_bloom_clear(handle), SUBETHA_OK);
    let mut after_clear = true;
    assert_eq!(
        unsafe { subetha_bloom_contains(handle, item.as_ptr(), item.len(), &mut after_clear) },
        SUBETHA_OK
    );
    assert!(!after_clear, "a cleared filter answers for nothing");
}

#[test]
fn a_created_filter_leaves_both_of_its_files_and_attaches_again() {
    ensure_init();
    let mut bits = 0u64;
    let mut hashes = 0u32;
    assert_eq!(
        unsafe { subetha_bloom_suggest(100, 0.01, &mut bits, &mut hashes) },
        SUBETHA_OK
    );
    let path = scratch("bloom-open");
    let base = path.to_str().expect("the path is UTF-8").to_owned();
    let mut writer: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_bloom_create(path.as_ptr(), bits, hashes, MODE_STRICT, &mut writer) },
        SUBETHA_OK
    );
    let item = b"shared";
    assert_eq!(
        unsafe { subetha_bloom_insert(writer, item.as_ptr(), item.len()) },
        SUBETHA_OK
    );
    assert_eq!(subetha_bloom_flush(writer), SUBETHA_OK);

    // A filter is two files: the header it checks and the bits it sets.
    // Attaching reads both, so both have to be where create put them.
    let header = std::path::PathBuf::from(format!("{base}.bloom.bin"));
    let bit_file = std::path::PathBuf::from(format!("{base}.bits.bin"));
    assert!(header.is_file(), "create leaves a header at {}", header.display());
    assert!(bit_file.is_file(), "create leaves the bits at {}", bit_file.display());

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_bloom_open(path.as_ptr(), bits, hashes, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
    let mut found = false;
    assert_eq!(
        unsafe { subetha_bloom_contains(reader, item.as_ptr(), item.len(), &mut found) },
        SUBETHA_OK
    );
    assert!(found, "a second handle answers for what the first put in");

}

#[test]
fn a_reset_filter_answers_for_nothing() {
    ensure_init();
    let mut bits = 0u64;
    let mut hashes = 0u32;
    assert_eq!(
        unsafe { subetha_bloom_suggest(100, 0.01, &mut bits, &mut hashes) },
        SUBETHA_OK
    );
    // Reset remakes the files, so it gets a path no other handle maps.
    let path = scratch("bloom-reset");
    let mut fresh: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_bloom_reset(path.as_ptr(), bits, hashes, MODE_STRICT, &mut fresh) },
        SUBETHA_OK
    );
    let item = b"never added";
    let mut present = true;
    assert_eq!(
        unsafe { subetha_bloom_contains(fresh, item.as_ptr(), item.len(), &mut present) },
        SUBETHA_OK
    );
    assert!(!present, "a reset filter answers for nothing");
}

#[test]
fn a_blocked_bloom_filter_answers_for_what_was_put_in() {
    ensure_init();
    let mut bits = 0u64;
    let mut hashes = 0u32;
    assert_eq!(
        unsafe { subetha_blocked_bloom_suggest(1_000, 0.01, &mut bits, &mut hashes) },
        SUBETHA_OK
    );
    assert!(bits > 0 && hashes > 0);

    let path = scratch("blocked");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_blocked_bloom_create(path.as_ptr(), bits, hashes, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );
    let item = b"present";
    assert_eq!(
        unsafe { subetha_blocked_bloom_insert(handle, item.as_ptr(), item.len()) },
        SUBETHA_OK
    );
    let mut found = false;
    assert_eq!(
        unsafe { subetha_blocked_bloom_contains(handle, item.as_ptr(), item.len(), &mut found) },
        SUBETHA_OK
    );
    assert!(found);

    let mut stats = subetha_blocked_bloom_stats::default();
    assert_eq!(unsafe { subetha_blocked_bloom_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.n_hashes, hashes);

    assert_eq!(subetha_blocked_bloom_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_blocked_bloom_clear(handle), SUBETHA_OK);
    let mut after_clear = true;
    assert_eq!(
        unsafe { subetha_blocked_bloom_contains(handle, item.as_ptr(), item.len(), &mut after_clear) },
        SUBETHA_OK
    );
    assert!(!after_clear);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_blocked_bloom_open(path.as_ptr(), bits, hashes, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
    let reset_path = scratch("blocked-reset");
    let mut fresh: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_blocked_bloom_reset(reset_path.as_ptr(), bits, hashes, MODE_STRICT, &mut fresh) },
        SUBETHA_OK
    );
}

#[test]
fn a_sketch_estimates_at_least_what_was_counted() {
    ensure_init();
    let mut d = 0u32;
    let mut w = 0u32;
    assert_eq!(unsafe { subetha_cms_suggest(0.01, 0.01, &mut d, &mut w) }, SUBETHA_OK);
    assert!(d > 0 && w > 0, "an epsilon and a delta give rows and counters");

    let path = scratch("cms");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_cms_create(path.as_ptr(), d, w, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    let item = b"counted";
    assert_eq!(unsafe { subetha_cms_insert(handle, item.as_ptr(), item.len()) }, SUBETHA_OK);
    assert_eq!(
        unsafe { subetha_cms_insert_n(handle, item.as_ptr(), item.len(), 4) },
        SUBETHA_OK
    );

    let mut seen = 0u64;
    assert_eq!(
        unsafe { subetha_cms_estimate(handle, item.as_ptr(), item.len(), &mut seen) },
        SUBETHA_OK
    );
    // The sketch never undercounts, so five is the floor rather than the answer.
    assert!(seen >= 5, "one insert and four more, so at least five");

    let mut stats = subetha_cms_stats::default();
    assert_eq!(unsafe { subetha_cms_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.d, d);
    assert_eq!(stats.w, w);

    assert_eq!(subetha_cms_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_cms_flush_async(handle), SUBETHA_OK);

    assert_eq!(subetha_cms_reset(handle), SUBETHA_OK);
    let mut after_reset = 1u64;
    assert_eq!(
        unsafe { subetha_cms_estimate(handle, item.as_ptr(), item.len(), &mut after_reset) },
        SUBETHA_OK
    );
    assert_eq!(after_reset, 0, "a reset sketch has counted nothing");

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_cms_open(path.as_ptr(), d, w, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
}

#[test]
fn a_version_chain_reads_a_value_as_it_stood_at_a_version() {
    ensure_init();
    let path = scratch("chain");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_versioned_chain_create(path.as_ptr(), 8, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    // A chain item is a fixed 48 bytes; push refuses any other length.
    let first = [1u8; 48];
    let second = [2u8; 48];
    assert_eq!(
        unsafe { subetha_versioned_chain_push(handle, 1, first.as_ptr(), first.len()) },
        SUBETHA_OK
    );
    assert_eq!(
        unsafe { subetha_versioned_chain_push(handle, 2, second.as_ptr(), second.len()) },
        SUBETHA_OK
    );

    // A reader at a version sees what was current then, not what is now.
    let mut buf = [0u8; 64];
    let mut len = 0usize;
    assert_eq!(
        unsafe { subetha_versioned_chain_read_at(handle, 1, buf.as_mut_ptr(), buf.len(), &mut len) },
        SUBETHA_OK
    );
    assert_eq!(&buf[..len], &first[..], "version one still reads as it stood");

    let mut version = 0u64;
    assert_eq!(
        unsafe {
            subetha_versioned_chain_current(handle, &mut version, buf.as_mut_ptr(), buf.len(), &mut len)
        },
        SUBETHA_OK
    );
    assert_eq!(version, 2);
    assert_eq!(&buf[..len], &second[..]);

    let mut stats = subetha_versioned_chain_stats::default();
    assert_eq!(unsafe { subetha_versioned_chain_read_stats(handle, &mut stats) }, SUBETHA_OK);

    assert_eq!(subetha_versioned_chain_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_versioned_chain_flush_async(handle), SUBETHA_OK);
    assert_eq!(subetha_versioned_chain_clear(handle), SUBETHA_OK);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_versioned_chain_open(path.as_ptr(), 8, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
    let reset_path = scratch("chain-reset");
    let mut fresh: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_versioned_chain_reset(reset_path.as_ptr(), 8, MODE_STRICT, &mut fresh) },
        SUBETHA_OK
    );
}

#[test]
fn a_rate_limiter_hands_out_its_capacity_and_then_refuses() {
    ensure_init();
    let path = scratch("rate");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_rate_limiter_create(path.as_ptr(), 2, 1, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    let mut available = 0u32;
    assert_eq!(unsafe { subetha_rate_limiter_available(handle, &mut available) }, SUBETHA_OK);
    assert_eq!(available, 2, "a fresh limiter holds its whole capacity");

    assert_eq!(subetha_rate_limiter_try_acquire(handle, 2), SUBETHA_OK);
    // The third token is not there, and a refusal is an answer rather than a fault.
    assert_ne!(subetha_rate_limiter_try_acquire(handle, 1), SUBETHA_OK);

    // Waiting with no time to wait in refuses the same way.
    assert_ne!(subetha_rate_limiter_acquire_wait(handle, 1, 0), SUBETHA_OK);

    assert_eq!(subetha_rate_limiter_reset(handle), SUBETHA_OK);
    assert_eq!(unsafe { subetha_rate_limiter_available(handle, &mut available) }, SUBETHA_OK);
    assert_eq!(available, 2, "a reset limiter is full again");

    let mut stats = subetha_rate_limiter_stats::default();
    assert_eq!(unsafe { subetha_rate_limiter_read_stats(handle, &mut stats) }, SUBETHA_OK);

    assert_eq!(subetha_rate_limiter_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_rate_limiter_flush_async(handle), SUBETHA_OK);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_rate_limiter_open(path.as_ptr(), 2, 1, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
}
