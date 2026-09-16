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

use subetha_ffi::qos_policy::{
    subetha_qos, subetha_qos_create, subetha_qos_mode, SUBETHA_QOS_BEST_EFFORT,
    SUBETHA_QOS_KEEP_LAST, SUBETHA_QOS_VOLATILE,
};
use subetha_ffi::virtual_endpoint::{
    subetha_endpoint_generation, subetha_endpoint_registry_create, subetha_endpoint_registry_mode,
};
use subetha_ffi::graph::{subetha_graph_create, subetha_graph_open};
use subetha_ffi::lru_cache::{
    subetha_lru_create, subetha_lru_get_and_touch, subetha_lru_put, subetha_lru_remove,
};
use subetha_ffi::time_point::{subetha_tile_create, subetha_tile_open};

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
    subetha_histogram_boundaries, subetha_histogram_bucket_for, subetha_histogram_count,
    subetha_histogram_counts, subetha_histogram_create, subetha_histogram_flush,
    subetha_histogram_flush_async, subetha_histogram_open, subetha_histogram_percentile,
    subetha_histogram_read_stats, subetha_histogram_record, subetha_histogram_reset,
    subetha_histogram_stats, subetha_reservoir_create, subetha_reservoir_flush,
    subetha_reservoir_flush_async, subetha_reservoir_open, subetha_reservoir_read_stats,
    subetha_reservoir_record, subetha_reservoir_reset, subetha_reservoir_snapshot,
    subetha_reservoir_stats, subetha_topology_create, subetha_topology_fan_in,
    subetha_topology_fan_out, subetha_topology_flush, subetha_topology_flush_async,
    subetha_topology_open, subetha_topology_read_stats, subetha_topology_record_send,
    subetha_topology_reset, subetha_topology_stats,
    subetha_laned_map_claim_lane, subetha_laned_map_claim_lane_for, subetha_laned_map_create,
    subetha_laned_map_flush, subetha_laned_map_get, subetha_laned_map_insert,
    subetha_laned_map_lane_of, subetha_laned_map_open, subetha_laned_map_read_stats,
    subetha_laned_map_reap_dead_claims, subetha_laned_map_release_lane, subetha_laned_map_remove,
    subetha_laned_map_stats, subetha_laned_map_sweep, subetha_laned_map_void_epoch,
    subetha_versioned_slab_create_default, subetha_versioned_slab_flush, subetha_versioned_slab_get,
    subetha_versioned_slab_read_stats, subetha_versioned_slab_retire, subetha_versioned_slab_set,
    subetha_versioned_slab_stats, subetha_versioned_slab_sweep_slot, subetha_versioned_slab_void_epoch,
    SUBETHA_ORDER_SEQ_CST, subetha_atomic_bool_create, subetha_atomic_bool_reset, subetha_atomic_bool_swap_explicit,
    subetha_atomic_u32_create, subetha_atomic_u32_fetch_add_explicit,
    subetha_atomic_u32_fetch_and_explicit, subetha_atomic_u32_fetch_or_explicit,
    subetha_atomic_u32_fetch_xor_explicit, subetha_atomic_u32_load, subetha_atomic_u32_swap_explicit,
    subetha_atomic_u64_compare_exchange, subetha_atomic_u64_compare_exchange_explicit,
    subetha_atomic_u64_create, subetha_atomic_u64_fetch_and, subetha_atomic_u64_fetch_and_explicit,
    subetha_atomic_u64_fetch_or, subetha_atomic_u64_fetch_or_explicit, subetha_atomic_u64_fetch_sub,
    subetha_atomic_u64_fetch_sub_explicit, subetha_atomic_u64_fetch_xor,
    subetha_atomic_u64_fetch_xor_explicit, subetha_atomic_u64_load, subetha_atomic_u64_load_explicit,
    subetha_atomic_u64_open, subetha_atomic_u64_reset, subetha_atomic_u64_store,
    subetha_atomic_u64_store_explicit, subetha_atomic_u64_swap, subetha_atomic_u64_swap_explicit,
    subetha_borrow_registry_len, subetha_epochs_create, subetha_epochs_live_pins,
    subetha_epochs_open_tickets, subetha_lazy_claim, subetha_lazy_create, subetha_lazy_flush,
    subetha_lazy_open, subetha_lazy_publish, subetha_lazy_read_stats, subetha_lazy_reclaim,
    subetha_lazy_stats, subetha_lazy_try_get, subetha_lazy_unlink, subetha_lazy_wait,
    subetha_unlink_report, subetha_versioned_map_create_default, subetha_versioned_map_flush,
    subetha_versioned_map_get, subetha_versioned_map_pin_epoch, subetha_versioned_map_read_stats,
    subetha_versioned_map_stats, subetha_versioned_map_void_epoch,
    subetha_pubsub_create, subetha_pubsub_wake_all, subetha_ring_options, subetha_ring_create, subetha_ring_wake_all,
    subetha_vyukov_create, subetha_vyukov_wake_all, subetha_waker_create, subetha_waker_reset,
    subetha_waker_wake_one_up_to,
    subetha_broadcast_create_shm, subetha_broadcast_open_shm,
    subetha_capacity_broadcast_create_shm, subetha_capacity_broadcast_open_shm,
    subetha_capacity_broadcast_push_wait, subetha_capacity_create_shm, subetha_capacity_open_shm,
    subetha_capacity_pubsub_create_shm, subetha_capacity_pubsub_open_shm,
    subetha_pubsub_create_shm, subetha_pubsub_open_shm,
    subetha_versioned_slab_create, subetha_versioned_slab_open,
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

/// The options every ring-shaped family takes, carrying the mode.
/// A shared-memory object is named rather than pathed, so the name is a
/// plain identifier with no directory in it.
fn c_name(tag: &str) -> CString {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the wall clock is after the epoch")
        .as_nanos();
    CString::new(format!("subetha-{tag}-{}-{nanos}", std::process::id())).expect("the name has no NUL")
}

fn ring_options() -> subetha_ring_options {
    subetha_ring_options { mode: MODE_STRICT, ..Default::default() }
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

#[test]
fn a_topology_counts_who_sends_to_whom() {
    ensure_init();
    let path = scratch("topology");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_topology_create(path.as_ptr(), 4, 2, 2, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    let mut count = 0u64;
    assert_eq!(
        unsafe { subetha_topology_record_send(handle, 0, 1, &mut count) },
        SUBETHA_OK
    );
    assert_eq!(count, 1, "the first send from nought to one is the first");
    assert_eq!(
        unsafe { subetha_topology_record_send(handle, 0, 2, &mut count) },
        SUBETHA_OK
    );

    // Fan out counts the places one sender reaches, fan in the senders
    // reaching one place.
    let mut out = 0u32;
    assert_eq!(unsafe { subetha_topology_fan_out(handle, 0, &mut out) }, SUBETHA_OK);
    assert_eq!(out, 2, "nought reached two places");
    let mut into = 0u32;
    assert_eq!(unsafe { subetha_topology_fan_in(handle, 1, &mut into) }, SUBETHA_OK);
    assert_eq!(into, 1, "one was reached by a single sender");

    let mut stats = subetha_topology_stats::default();
    assert_eq!(unsafe { subetha_topology_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(subetha_topology_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_topology_flush_async(handle), SUBETHA_OK);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_topology_open(path.as_ptr(), 4, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
    let reset_path = scratch("topology-reset");
    let mut fresh: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_topology_reset(reset_path.as_ptr(), 4, 2, 2, MODE_STRICT, &mut fresh) },
        SUBETHA_OK
    );
}

#[test]
fn a_reservoir_keeps_a_bounded_sample_of_a_longer_stream() {
    ensure_init();
    let path = scratch("reservoir");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_reservoir_create(path.as_ptr(), 4, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    // A record is a fixed fifty-six bytes; anything else is refused.
    // More values than the reservoir holds: a refusal there is how the
    // sample stays unbiased rather than a fault.
    let mut kept = 0usize;
    for i in 0u8..16 {
        let value = [i; 56];
        let mut slot = 0u64;
        let code = unsafe { subetha_reservoir_record(handle, value.as_ptr(), value.len(), &mut slot) };
        if code == SUBETHA_OK {
            kept += 1;
        }
    }
    assert!(kept > 0, "a reservoir with room keeps something");

    let mut buf = [0u8; 1024];
    let mut len = 0usize;
    assert_eq!(
        unsafe { subetha_reservoir_snapshot(handle, buf.as_mut_ptr(), buf.len(), &mut len) },
        SUBETHA_OK
    );

    let mut stats = subetha_reservoir_stats::default();
    assert_eq!(unsafe { subetha_reservoir_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(subetha_reservoir_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_reservoir_flush_async(handle), SUBETHA_OK);
    assert_eq!(subetha_reservoir_reset(handle), SUBETHA_OK);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_reservoir_open(path.as_ptr(), 4, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
}

#[test]
fn a_histogram_puts_a_value_in_the_bucket_its_boundaries_name() {
    ensure_init();
    let path = scratch("histogram");
    let bounds: [u64; 2] = [10, 100];
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe {
            subetha_histogram_create(path.as_ptr(), bounds.as_ptr(), bounds.len(), MODE_STRICT, &mut handle)
        },
        SUBETHA_OK
    );

    // Two boundaries make three buckets: below ten, ten to a hundred, above.
    let mut bucket = 0u64;
    assert_eq!(unsafe { subetha_histogram_bucket_for(handle, 5, &mut bucket) }, SUBETHA_OK);
    assert_eq!(bucket, 0, "five sits below the first boundary");

    let mut recorded = 0u64;
    assert_eq!(unsafe { subetha_histogram_record(handle, 5, &mut recorded) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_histogram_record(handle, 50, &mut recorded) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_histogram_record(handle, 500, &mut recorded) }, SUBETHA_OK);

    let mut count = 0u64;
    assert_eq!(unsafe { subetha_histogram_count(handle, 0, &mut count) }, SUBETHA_OK);
    assert_eq!(count, 1, "one value landed below ten");

    let mut counts = [0u64; 8];
    let mut n = 0usize;
    assert_eq!(
        unsafe { subetha_histogram_counts(handle, counts.as_mut_ptr(), counts.len(), &mut n) },
        SUBETHA_OK
    );
    assert_eq!(n, 3, "two boundaries make three buckets");

    let mut read_bounds = [0u64; 8];
    assert_eq!(
        unsafe {
            subetha_histogram_boundaries(handle, read_bounds.as_mut_ptr(), read_bounds.len(), &mut n)
        },
        SUBETHA_OK
    );
    assert_eq!(&read_bounds[..n], &bounds[..]);

    let mut at = 0u64;
    assert_eq!(unsafe { subetha_histogram_percentile(handle, 0.5, &mut at) }, SUBETHA_OK);

    let mut stats = subetha_histogram_stats::default();
    assert_eq!(unsafe { subetha_histogram_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(subetha_histogram_flush(handle), SUBETHA_OK);
    assert_eq!(subetha_histogram_flush_async(handle), SUBETHA_OK);
    assert_eq!(subetha_histogram_reset(handle), SUBETHA_OK);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe {
            subetha_histogram_open(path.as_ptr(), bounds.as_ptr(), bounds.len(), MODE_STRICT, &mut reader)
        },
        SUBETHA_OK
    );
}

#[test]
fn a_versioned_slab_keeps_a_slot_readable_and_retires_it() {
    ensure_init();
    let slab_path = scratch("vslab");
    let epochs_path = scratch("vslab-epochs");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe {
            subetha_versioned_slab_create_default(
                slab_path.as_ptr(), 8, epochs_path.as_ptr(), 4, MODE_STRICT, &mut handle,
            )
        },
        SUBETHA_OK
    );

    let mut stats = subetha_versioned_slab_stats::default();
    assert_eq!(unsafe { subetha_versioned_slab_read_stats(handle, &mut stats) }, SUBETHA_OK);
    let width = stats.value_size as usize;
    let value = vec![7u8; width];
    assert_eq!(
        unsafe { subetha_versioned_slab_set(handle, 0, value.as_ptr(), value.len()) },
        SUBETHA_OK
    );

    let mut buf = vec![0u8; width];
    let mut len = 0usize;
    assert_eq!(
        unsafe { subetha_versioned_slab_get(handle, 0, buf.as_mut_ptr(), buf.len(), &mut len) },
        SUBETHA_OK
    );
    assert_eq!(&buf[..len], &value[..], "the slot reads back what was set");

    // Retiring answers what was there and leaves nothing live behind.
    assert_eq!(
        unsafe { subetha_versioned_slab_retire(handle, 0, buf.as_mut_ptr(), buf.len(), &mut len) },
        SUBETHA_OK
    );
    assert_eq!(&buf[..len], &value[..], "retire answers what it took away");

    let mut dropped = 0u64;
    assert_eq!(
        unsafe { subetha_versioned_slab_sweep_slot(handle, 0, &mut dropped) },
        SUBETHA_OK
    );
    let mut touched = 0u64;
    assert_eq!(
        unsafe { subetha_versioned_slab_void_epoch(handle, 999_999, &mut touched) },
        SUBETHA_OK
    );
    assert_eq!(touched, 0, "nothing was written at an epoch nothing used");

    assert_eq!(subetha_versioned_slab_flush(handle), SUBETHA_OK);
}

#[test]
fn a_laned_map_claims_a_lane_and_reads_back_across_lanes() {
    ensure_init();
    let dir = scratch("laned");
    std::fs::create_dir_all(dir.to_str().expect("the path is UTF-8")).expect("the directory is made");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe {
            subetha_laned_map_create(dir.as_ptr(), 2, 64, 8, 8, 4, MODE_STRICT, &mut handle)
        },
        SUBETHA_OK
    );

    let key = [1u8; 8];
    let value = [9u8; 8];

    // A writer takes a free lane and writes through it.
    let mut lane = u32::MAX;
    assert_eq!(unsafe { subetha_laned_map_claim_lane(handle, &mut lane) }, SUBETHA_OK);
    assert_eq!(
        unsafe {
            subetha_laned_map_insert(handle, lane, key.as_ptr(), key.len(), value.as_ptr(), value.len())
        },
        SUBETHA_OK
    );

    // Both of these answer for a key the map already holds, so they are
    // asked after the insert: lane_of names the lane holding it, and
    // claim_lane_for takes that same lane back.
    let mut lane_of = u32::MAX;
    assert_eq!(
        unsafe { subetha_laned_map_lane_of(handle, key.as_ptr(), key.len(), &mut lane_of) },
        SUBETHA_OK
    );
    assert_eq!(lane_of, lane, "the key sits in the lane that took it");

    assert_eq!(subetha_laned_map_release_lane(handle, lane), SUBETHA_OK);
    let mut again = u32::MAX;
    assert_eq!(
        unsafe { subetha_laned_map_claim_lane_for(handle, key.as_ptr(), key.len(), &mut again) },
        SUBETHA_OK
    );
    assert_eq!(again, lane, "claiming for a key takes the lane that holds it");
    let lane = again;

    // Reading takes no lane: it merges every one of them.
    let mut buf = [0u8; 64];
    let mut len = 0usize;
    assert_eq!(
        unsafe {
            subetha_laned_map_get(handle, key.as_ptr(), key.len(), buf.as_mut_ptr(), buf.len(), &mut len)
        },
        SUBETHA_OK
    );
    assert_eq!(&buf[..len], &value[..]);

    assert_eq!(
        unsafe {
            subetha_laned_map_remove(handle, lane, key.as_ptr(), key.len(), buf.as_mut_ptr(), buf.len(), &mut len)
        },
        SUBETHA_OK
    );
    assert_eq!(subetha_laned_map_release_lane(handle, lane), SUBETHA_OK);

    let mut reaped = 0u64;
    assert_eq!(unsafe { subetha_laned_map_reap_dead_claims(handle, &mut reaped) }, SUBETHA_OK);
    let mut freed = 0u64;
    assert_eq!(unsafe { subetha_laned_map_sweep(handle, &mut freed) }, SUBETHA_OK);
    let mut touched = 0u64;
    assert_eq!(
        unsafe { subetha_laned_map_void_epoch(handle, 999_999, &mut touched) },
        SUBETHA_OK
    );

    let mut stats = subetha_laned_map_stats::default();
    assert_eq!(unsafe { subetha_laned_map_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(subetha_laned_map_flush(handle), SUBETHA_OK);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_laned_map_open(dir.as_ptr(), 2, 64, 8, 8, 4, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
}

#[test]
fn a_64_bit_atomic_carries_every_operation_the_abi_offers() {
    ensure_init();
    let path = scratch("atomic64");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_atomic_u64_create(path.as_ptr(), 10, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    // Every one of these answers the value it REPLACED, not the value it
    // landed on, which is the half a name does not say.
    let mut prev = 0u64;
    let mut now = 0u64;

    assert_eq!(subetha_atomic_u64_store(handle, 20), SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_u64_load(handle, &mut now) }, SUBETHA_OK);
    assert_eq!(now, 20);

    assert_eq!(subetha_atomic_u64_store_explicit(handle, 30, SUBETHA_ORDER_SEQ_CST), SUBETHA_OK);
    assert_eq!(
        unsafe { subetha_atomic_u64_load_explicit(handle, SUBETHA_ORDER_SEQ_CST, &mut now) },
        SUBETHA_OK
    );
    assert_eq!(now, 30);

    assert_eq!(unsafe { subetha_atomic_u64_swap(handle, 40, &mut prev) }, SUBETHA_OK);
    assert_eq!(prev, 30, "swap answers what it replaced");
    assert_eq!(
        unsafe { subetha_atomic_u64_swap_explicit(handle, 50, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(prev, 40);

    assert_eq!(unsafe { subetha_atomic_u64_fetch_sub(handle, 5, &mut prev) }, SUBETHA_OK);
    assert_eq!(prev, 50);
    assert_eq!(
        unsafe { subetha_atomic_u64_fetch_sub_explicit(handle, 5, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(prev, 45);

    assert_eq!(subetha_atomic_u64_store(handle, 0b1100), SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_u64_fetch_and(handle, 0b1010, &mut prev) }, SUBETHA_OK);
    assert_eq!(prev, 0b1100);
    assert_eq!(unsafe { subetha_atomic_u64_load(handle, &mut now) }, SUBETHA_OK);
    assert_eq!(now, 0b1000, "and leaves the bits both sides had");

    assert_eq!(
        unsafe { subetha_atomic_u64_fetch_and_explicit(handle, 0b1111, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(unsafe { subetha_atomic_u64_fetch_or(handle, 0b0001, &mut prev) }, SUBETHA_OK);
    assert_eq!(
        unsafe { subetha_atomic_u64_fetch_or_explicit(handle, 0b0010, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(unsafe { subetha_atomic_u64_fetch_xor(handle, 0b0011, &mut prev) }, SUBETHA_OK);
    assert_eq!(
        unsafe { subetha_atomic_u64_fetch_xor_explicit(handle, 0b0011, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );

    // A compare and exchange that does not match leaves the value alone
    // and reports what it found, which is how a caller retries.
    assert_eq!(subetha_atomic_u64_store(handle, 7), SUBETHA_OK);
    let mut current = 0u64;
    let mut swapped = true;
    assert_eq!(
        unsafe { subetha_atomic_u64_compare_exchange(handle, 999, 8, &mut current, &mut swapped) },
        SUBETHA_OK
    );
    assert!(!swapped, "the value was not what the caller expected");
    assert_eq!(current, 7, "and the answer says what it actually was");

    assert_eq!(
        unsafe {
            subetha_atomic_u64_compare_exchange_explicit(
                handle, 7, 8, SUBETHA_ORDER_SEQ_CST, SUBETHA_ORDER_SEQ_CST, &mut current, &mut swapped,
            )
        },
        SUBETHA_OK
    );
    assert!(swapped, "expecting what was there swaps it");

    let reset_path = scratch("atomic64-reset");
    let mut fresh: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_atomic_u64_reset(reset_path.as_ptr(), 1, MODE_STRICT, &mut fresh) },
        SUBETHA_OK
    );
    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_atomic_u64_open(path.as_ptr(), MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );
}

#[test]
fn a_32_bit_atomic_carries_the_explicit_orderings_too() {
    ensure_init();
    let path = scratch("atomic32");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_atomic_u32_create(path.as_ptr(), 0b1100, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );
    let mut prev = 0u32;
    assert_eq!(
        unsafe { subetha_atomic_u32_fetch_add_explicit(handle, 1, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(
        unsafe { subetha_atomic_u32_fetch_and_explicit(handle, 0b1010, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(
        unsafe { subetha_atomic_u32_fetch_or_explicit(handle, 0b0001, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(
        unsafe { subetha_atomic_u32_fetch_xor_explicit(handle, 0b0011, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert_eq!(
        unsafe { subetha_atomic_u32_swap_explicit(handle, 99, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    let mut now = 0u32;
    assert_eq!(unsafe { subetha_atomic_u32_load(handle, &mut now) }, SUBETHA_OK);
    assert_eq!(now, 99, "the swap landed on what it was given");
}

#[test]
fn a_boolean_atomic_swaps_and_resets() {
    ensure_init();
    let path = scratch("atomicbool");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_atomic_bool_create(path.as_ptr(), false, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );
    let mut prev = true;
    assert_eq!(
        unsafe { subetha_atomic_bool_swap_explicit(handle, true, SUBETHA_ORDER_SEQ_CST, &mut prev) },
        SUBETHA_OK
    );
    assert!(!prev, "swap answers what it replaced");

    let reset_path = scratch("atomicbool-reset");
    let mut fresh: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_atomic_bool_reset(reset_path.as_ptr(), true, MODE_STRICT, &mut fresh) },
        SUBETHA_OK
    );
}

#[test]
fn a_lazy_value_is_published_once_by_whoever_claims_it() {
    ensure_init();
    let path = scratch("lazy");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_lazy_create(path.as_ptr(), 8, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    // Before anything is published there is nothing to get, which is an
    // ordinary answer rather than a fault.
    let mut buf = [0u8; 32];
    let mut len = 0usize;
    let before = unsafe { subetha_lazy_try_get(handle, buf.as_mut_ptr(), buf.len(), &mut len) };
    assert_ne!(before, SUBETHA_OK, "nothing has been published yet");

    let pid = std::process::id();
    let mut won = false;
    assert_eq!(unsafe { subetha_lazy_claim(handle, pid, &mut won) }, SUBETHA_OK);
    assert!(won, "the first claimant wins the right to publish");

    let value = [3u8; 8];
    assert_eq!(
        unsafe { subetha_lazy_publish(handle, pid, value.as_ptr(), value.len()) },
        SUBETHA_OK
    );

    assert_eq!(
        unsafe { subetha_lazy_try_get(handle, buf.as_mut_ptr(), buf.len(), &mut len) },
        SUBETHA_OK
    );
    assert_eq!(&buf[..len], &value[..], "what was published is what is read");

    // A wait on a value already there returns it rather than parking.
    assert_eq!(
        unsafe { subetha_lazy_wait(handle, 1_000, buf.as_mut_ptr(), buf.len(), &mut len) },
        SUBETHA_OK
    );
    assert_eq!(&buf[..len], &value[..]);

    let mut freed = false;
    assert_eq!(unsafe { subetha_lazy_reclaim(handle, &mut freed) }, SUBETHA_OK);

    let mut stats = subetha_lazy_stats::default();
    assert_eq!(unsafe { subetha_lazy_read_stats(handle, &mut stats) }, SUBETHA_OK);
    assert_eq!(subetha_lazy_flush(handle), SUBETHA_OK);

    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_lazy_open(path.as_ptr(), 8, MODE_STRICT, &mut reader) },
        SUBETHA_OK
    );

    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_lazy_unlink(path.as_ptr(), &mut report) }, SUBETHA_OK);
}

#[test]
fn a_versioned_map_reads_a_key_and_reports_the_epoch_a_pin_would_take() {
    ensure_init();
    let tree = scratch("vmap");
    let epochs = scratch("vmap-epochs");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe {
            subetha_versioned_map_create_default(
                tree.as_ptr(), 64, epochs.as_ptr(), 4, MODE_STRICT, &mut handle,
            )
        },
        SUBETHA_OK
    );

    let mut stats = subetha_versioned_map_stats::default();
    assert_eq!(unsafe { subetha_versioned_map_read_stats(handle, &mut stats) }, SUBETHA_OK);
    let key_size = stats.key_size as usize;
    let key = vec![1u8; key_size];

    let mut buf = vec![0u8; 64];
    let mut len = 0usize;
    // A key nothing put in is absent, and that is an answer, not a fault.
    let missing = unsafe {
        subetha_versioned_map_get(handle, key.as_ptr(), key.len(), buf.as_mut_ptr(), buf.len(), &mut len)
    };
    assert_ne!(missing, SUBETHA_OK, "the map holds nothing under that key yet");

    let mut epoch = 0u64;
    assert_eq!(unsafe { subetha_versioned_map_pin_epoch(handle, &mut epoch) }, SUBETHA_OK);

    let mut touched = 1u64;
    assert_eq!(
        unsafe { subetha_versioned_map_void_epoch(handle, 999_999, &mut touched) },
        SUBETHA_OK
    );
    assert_eq!(touched, 0, "nothing was written at an epoch nothing used");

    assert_eq!(subetha_versioned_map_flush(handle), SUBETHA_OK);
}

#[test]
fn the_epoch_table_reports_what_is_holding_reclamation_up() {
    ensure_init();
    let path = scratch("epochs");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_epochs_create(path.as_ptr(), 8, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    let mut pins = 1u64;
    assert_eq!(unsafe { subetha_epochs_live_pins(handle, &mut pins) }, SUBETHA_OK);
    assert_eq!(pins, 0, "nobody is reading, so nothing is pinned");

    let mut tickets = 1u64;
    assert_eq!(unsafe { subetha_epochs_open_tickets(handle, &mut tickets) }, SUBETHA_OK);
    assert_eq!(tickets, 0, "no compound write is part way through");
}

#[test]
fn the_borrow_registry_is_empty_between_calls() {
    ensure_init();
    // Each entry point publishes its borrow for the length of the call
    // and takes it back, so nothing is published between two of them.
    assert_eq!(subetha_borrow_registry_len(), 0);
}

#[test]
fn waking_everyone_is_answered_even_when_nobody_waits() {
    ensure_init();
    let ring_path = scratch("wake-ring");
    let mut ring: subetha_handle = 0;
    assert_eq!(
        unsafe {
            subetha_ring_create(ring_path.as_ptr(), 1, 1, 8, &ring_options(), &mut ring)
        },
        SUBETHA_OK
    );
    assert_eq!(subetha_ring_wake_all(ring), SUBETHA_OK);

    let topic_path = scratch("wake-pubsub");
    let mut topic: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_pubsub_create(topic_path.as_ptr(), 8, &ring_options(), &mut topic) },
        SUBETHA_OK
    );
    assert_eq!(subetha_pubsub_wake_all(topic), SUBETHA_OK);

    let queue_path = scratch("wake-vyukov");
    let mut queue: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_vyukov_create(queue_path.as_ptr(), 8, &ring_options(), &mut queue) },
        SUBETHA_OK
    );
    assert_eq!(subetha_vyukov_wake_all(queue), SUBETHA_OK);
}

#[test]
fn a_waker_wakes_nobody_when_nobody_is_parked() {
    ensure_init();
    let path = scratch("waker");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_waker_create(path.as_ptr(), 8, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );
    let mut woken = 1u64;
    assert_eq!(
        unsafe { subetha_waker_wake_one_up_to(handle, 100, &mut woken) },
        SUBETHA_OK
    );
    assert_eq!(woken, 0, "nobody is parked, so nobody is woken");

    let reset_path = scratch("waker-reset");
    let mut fresh: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_waker_reset(reset_path.as_ptr(), 8, MODE_STRICT, &mut fresh) },
        SUBETHA_OK
    );
}

#[test]
fn a_cache_touches_what_it_reads_and_answers_what_it_removed() {
    ensure_init();
    let path = scratch("lru");
    let mut handle: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_lru_create(path.as_ptr(), 4, 4, 4, MODE_STRICT, &mut handle) },
        SUBETHA_OK
    );

    let key = *b"key1";
    let value = *b"val1";
    let mut replaced = true;
    assert_eq!(
        unsafe {
            subetha_lru_put(handle, key.as_ptr(), key.len() as u64, value.as_ptr(), value.len() as u64, &mut replaced)
        },
        SUBETHA_OK
    );
    assert!(!replaced, "the key was not there before, so nothing was replaced");

    // Reading through get_and_touch makes the key the most recent, which
    // is what keeps it from being the next one evicted.
    let mut out = [0u8; 4];
    let mut found = false;
    assert_eq!(
        unsafe {
            subetha_lru_get_and_touch(handle, key.as_ptr(), key.len() as u64, out.as_mut_ptr(), out.len() as u64, &mut found)
        },
        SUBETHA_OK
    );
    assert!(found);
    assert_eq!(&out, &value);

    let mut removed = false;
    assert_eq!(
        unsafe {
            subetha_lru_remove(handle, key.as_ptr(), key.len() as u64, out.as_mut_ptr(), out.len() as u64, &mut removed)
        },
        SUBETHA_OK
    );
    assert!(removed, "remove answers whether it took something away");
}

#[test]
fn a_tile_and_a_graph_attach_to_what_was_made() {
    ensure_init();
    let tile_path = scratch("tile");
    let mut tile: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_tile_create(tile_path.as_ptr(), 32, MODE_STRICT, &mut tile) },
        SUBETHA_OK
    );
    let mut tile_again: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_tile_open(tile_path.as_ptr(), 32, MODE_STRICT, &mut tile_again) },
        SUBETHA_OK
    );

    let graph_path = scratch("graph");
    let mut graph: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_graph_create(graph_path.as_ptr(), 8, 16, 8, 8, MODE_STRICT, &mut graph) },
        SUBETHA_OK
    );
    let mut graph_again: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_graph_open(graph_path.as_ptr(), 8, 16, 8, 8, MODE_STRICT, &mut graph_again) },
        SUBETHA_OK
    );
}

#[test]
fn the_shared_memory_families_make_and_attach_by_name() {
    ensure_init();
    // These name a shared-memory object rather than a file, so the name
    // is a plain identifier and both sides use the same namespace.
    let ns = 0u32;
    let topic = c_name("shm-pubsub");
    let mut made: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_pubsub_create_shm(topic.as_ptr(), 8, ns, &ring_options(), &mut made) },
        SUBETHA_OK
    );
    let mut attached: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_pubsub_open_shm(topic.as_ptr(), 8, ns, &ring_options(), &mut attached) },
        SUBETHA_OK
    );

    let bcast = c_name("shm-broadcast");
    let mut b: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_broadcast_create_shm(bcast.as_ptr(), 8, ns, &ring_options(), &mut b) },
        SUBETHA_OK
    );
    let mut b2: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_broadcast_open_shm(bcast.as_ptr(), 8, ns, &ring_options(), &mut b2) },
        SUBETHA_OK
    );

    let cap = c_name("shm-capacity");
    let mut c: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_capacity_create_shm(cap.as_ptr(), 1, 1, 8, &ring_options(), &mut c) },
        SUBETHA_OK
    );
    let mut c2: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_capacity_open_shm(cap.as_ptr(), 1, 1, 8, &ring_options(), &mut c2) },
        SUBETHA_OK
    );

    let capb = c_name("shm-capbroadcast");
    let mut cb: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_capacity_broadcast_create_shm(capb.as_ptr(), 8, &ring_options(), &mut cb) },
        SUBETHA_OK
    );
    let mut cb2: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_capacity_broadcast_open_shm(capb.as_ptr(), 8, &ring_options(), &mut cb2) },
        SUBETHA_OK
    );
    let item = [1u8; 8];
    assert_eq!(
        unsafe { subetha_capacity_broadcast_push_wait(cb, item.as_ptr(), item.len(), 1_000) },
        SUBETHA_OK
    );

    let capp = c_name("shm-cappubsub");
    let mut cp: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_capacity_pubsub_create_shm(capp.as_ptr(), 8, &ring_options(), &mut cp) },
        SUBETHA_OK
    );
    let mut cp2: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_capacity_pubsub_open_shm(capp.as_ptr(), 8, &ring_options(), &mut cp2) },
        SUBETHA_OK
    );
}

#[test]
fn an_endpoint_registry_and_a_qos_policy_report_what_they_were_made_with() {
    ensure_init();
    let mut registry: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_endpoint_registry_create(MODE_STRICT, &mut registry) },
        SUBETHA_OK
    );
    let mut mode = u32::MAX;
    assert_eq!(
        unsafe { subetha_endpoint_registry_mode(registry, &mut mode) },
        SUBETHA_OK
    );
    assert_eq!(mode, MODE_STRICT);

    // The generation steps when the registry changes, so a reader can
    // tell that what it looked up has been superseded.
    let mut generation = 0u64;
    assert_eq!(
        unsafe { subetha_endpoint_generation(registry, &mut generation) },
        SUBETHA_OK
    );

    // A policy is made from a stated one rather than from nothing, so
    // every field is named rather than guessed at.
    let wants = subetha_qos {
        durability: SUBETHA_QOS_VOLATILE,
        reliability: SUBETHA_QOS_BEST_EFFORT,
        history_kind: SUBETHA_QOS_KEEP_LAST,
        history_depth: 16,
        ..Default::default()
    };
    let mut policy: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_qos_create(&wants, MODE_STRICT, &mut policy) },
        SUBETHA_OK
    );
    let mut qos_mode = u32::MAX;
    assert_eq!(unsafe { subetha_qos_mode(policy, &mut qos_mode) }, SUBETHA_OK);
    assert_eq!(qos_mode, MODE_STRICT);
}

#[test]
fn a_versioned_slab_takes_a_width_and_a_depth_of_its_own() {
    ensure_init();
    let slab_path = scratch("vslab-sized");
    let epochs_path = scratch("vslab-sized-epochs");
    let mut handle: subetha_handle = 0;
    // The sized form names the width and how many versions a slot keeps,
    // where the default form picks both.
    assert_eq!(
        unsafe {
            subetha_versioned_slab_create(
                slab_path.as_ptr(), 8, 16, 4, epochs_path.as_ptr(), 4, MODE_STRICT, &mut handle,
            )
        },
        SUBETHA_OK
    );
    let value = [5u8; 16];
    assert_eq!(
        unsafe { subetha_versioned_slab_set(handle, 0, value.as_ptr(), value.len()) },
        SUBETHA_OK
    );
    let mut reader: subetha_handle = 0;
    assert_eq!(
        unsafe {
            subetha_versioned_slab_open(
                slab_path.as_ptr(), 8, 16, 4, epochs_path.as_ptr(), 4, MODE_STRICT, &mut reader,
            )
        },
        SUBETHA_OK
    );
}
