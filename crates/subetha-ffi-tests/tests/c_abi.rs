//! Runs the C suite, and drives C consumers and producers in another
//! process against rings this process shares with them, so the attach, the
//! file-backed wakers and the unlink are exercised across a real process
//! boundary for every family that crosses one.
//!
//! The library's state is process-wide, so everything runs in order inside
//! one test; the peer role is a second test that acts only when spawned
//! with `SUBETHA_FFI_PEER` set, playing the family `SUBETHA_FFI_PEER_KIND`
//! names.

use std::env::VarError;
use std::ffi::CString;
use std::process::{Child, Command};

use subetha_ffi::{
    subetha_arena_create, subetha_arena_get, subetha_arena_intern, subetha_arena_read_stats, subetha_arena_ref_len,
    subetha_arena_ref_offset, subetha_arena_ref_pack, subetha_arena_stats, subetha_arena_unlink,
    subetha_broadcast_create, subetha_broadcast_push_wait, subetha_broadcast_read_stats, subetha_broadcast_stats,
    subetha_broadcast_unlink, subetha_capacity_create, subetha_capacity_push_wait, subetha_capacity_register_producer,
    subetha_capacity_unlink, subetha_deque_create, subetha_deque_push_wait, subetha_deque_unlink, subetha_element_layout,
    subetha_hashmap_create, subetha_hashmap_get, subetha_hashmap_insert, subetha_hashmap_read_stats, subetha_hashmap_stats,
    subetha_hashmap_unlink,
    subetha_notifier_drain, subetha_notifier_native, subetha_notifier_wait, subetha_ring_notifier,
    subetha_ring_register_consumer, subetha_ring_try_pop, subetha_stack_create, subetha_stack_push_wait,
    subetha_stack_unlink, SUBETHA_E_RING_EMPTY, SUBETHA_E_TIMEOUT,
    subetha_list_create, subetha_list_first, subetha_list_get, subetha_list_next, subetha_list_push_back,
    subetha_list_read_stats, subetha_list_stats, subetha_list_unlink, SUBETHA_LIST_HEAD_INDEX,
    subetha_btree_create, subetha_btree_get, subetha_btree_insert, subetha_btree_read_stats, subetha_btree_stats,
    subetha_btree_unlink, SUBETHA_E_MAP_KEY_ABSENT,
    subetha_cell_create, subetha_cell_get, subetha_cell_read_stats, subetha_cell_set, subetha_cell_stats,
    subetha_cell_unlink, subetha_cell_version,
    subetha_frame_region_alloc, subetha_frame_region_create, subetha_frame_region_read_stats,
    subetha_frame_region_stats, subetha_frame_region_unlink, subetha_frame_region_write,
    SUBETHA_E_RING_FULL, SUBETHA_FRAME_NO_BLOCK,
    subetha_epochs_advance, subetha_epochs_create, subetha_epochs_now, subetha_epochs_read_stats,
    subetha_epochs_reclaim_horizon, subetha_epochs_stats, subetha_epochs_unlink,
    subetha_rwlock_create, subetha_rwlock_read_stats, subetha_rwlock_stats, subetha_rwlock_unlink,
    subetha_rwlock_unlock, subetha_rwlock_write,
    subetha_atomic_u32_compare_exchange, subetha_atomic_u32_create, subetha_atomic_u32_fetch_add,
    subetha_atomic_u32_fetch_sub, subetha_atomic_u32_load, subetha_atomic_u32_store,
    subetha_condvar_create, subetha_condvar_generation, subetha_condvar_notify_all,
    subetha_condvar_read_stats, subetha_condvar_stats, subetha_condvar_unlink, subetha_condvar_wait,
    subetha_current_pid, subetha_owner_lease_create, subetha_owner_lease_read,
    subetha_owner_lease_read_stats, subetha_owner_lease_release, subetha_owner_lease_stats,
    subetha_owner_lease_tick_epoch, subetha_owner_lease_try_acquire, subetha_owner_lease_unlink,
    SUBETHA_LEASE_NO_OWNER,
    subetha_epoch_barrier_create, subetha_epoch_barrier_live_peers, subetha_epoch_barrier_unlink,
    subetha_epoch_barrier_wait_timeout,
    subetha_shared_arc_create, subetha_shared_arc_read, subetha_shared_arc_strong_count,
    subetha_shared_arc_unlink, SUBETHA_ARC_KEEP,
    subetha_waker_create, subetha_waker_unlink, subetha_waker_wake_up_to,
    subetha_fence_clock_compute_global_fence, subetha_fence_clock_create,
    subetha_fence_clock_merge, subetha_fence_clock_register, subetha_fence_clock_tick,
    subetha_fence_clock_unlink, subetha_hlc,
    subetha_heartbeat_create, subetha_heartbeat_read_slot, subetha_heartbeat_read_stats,
    subetha_heartbeat_register, subetha_heartbeat_slot, subetha_heartbeat_stats,
    subetha_heartbeat_tick_epoch, subetha_heartbeat_unlink, subetha_heartbeat_unregister,
    subetha_leader_create, subetha_leader_read_stats, subetha_leader_stats, subetha_leader_tick_epoch,
    subetha_leader_try_claim, subetha_leader_unlink,
    SUBETHA_HEARTBEAT_EMPTY_PID, SUBETHA_LEADER_NONE,
    subetha_semaphore_acquire, subetha_semaphore_create, subetha_semaphore_read_stats,
    subetha_semaphore_release, subetha_semaphore_stats, subetha_semaphore_unlink,
    subetha_atomic_bool_create, subetha_atomic_bool_load_explicit, subetha_atomic_u64_create,
    subetha_atomic_u64_fetch_add_explicit, subetha_atomic_u64_load, subetha_atomic_unlink,
    SUBETHA_ORDER_ACQUIRE, SUBETHA_ORDER_RELAXED,
    subetha_region_allocate, subetha_region_create, subetha_region_free, subetha_region_get,
    subetha_region_read_stats, subetha_region_stats, subetha_region_unlink,
    subetha_slab_create, subetha_slab_get, subetha_slab_set, subetha_slab_slot_version, subetha_slab_unlink,
    subetha_vec_create, subetha_vec_get, subetha_vec_push_back, subetha_vec_read_stats, subetha_vec_stats,
    subetha_vec_unlink,
    subetha_handle, subetha_handle_destroy, subetha_init, subetha_locale_ring_create,
    subetha_locale_ring_migrate, subetha_locale_ring_push_wait, subetha_locale_ring_register_producer,
    subetha_locale_ring_unlink, subetha_mpmc_create_grid, SUBETHA_LOCALE_FILE,
    subetha_mpmc_pop_wait, subetha_mpmc_push_wait, subetha_mpmc_unlink, subetha_mpsc_create_pool,
    subetha_mpsc_pop_wait, subetha_mpsc_push_wait, subetha_mpsc_unlink, subetha_pubsub_create,
    subetha_pubsub_publish, subetha_pubsub_subscribe_file, subetha_pubsub_unlink, subetha_pubsub_unlink_position,
    subetha_ring_create, subetha_ring_options, subetha_ring_push_wait, subetha_ring_register_producer,
    subetha_ring_unlink, subetha_shutdown, subetha_spsc_create, subetha_spsc_push_wait, subetha_spsc_unlink,
    subetha_subscriber_position, subetha_unlink_report, subetha_vyukov_create, subetha_vyukov_pop_wait,
    subetha_vyukov_unlink, SUBETHA_MODE_STRICT, SUBETHA_OK, SUBETHA_RING_PAYLOAD_MAX, SUBETHA_RING_SLOT_BYTES,
};
use subetha_ffi_tests::{
    subetha_ctest_peer, subetha_ctest_peer_arena, subetha_ctest_peer_atomic, subetha_ctest_peer_broadcast,
    subetha_ctest_peer_btree, subetha_ctest_peer_capacity, subetha_ctest_peer_cell, subetha_ctest_peer_deque,
    subetha_ctest_peer_epochs, subetha_ctest_peer_frame_region, subetha_ctest_peer_list,
    subetha_ctest_peer_condvar, subetha_ctest_peer_epoch_barrier, subetha_ctest_peer_fence_clock,
    subetha_ctest_peer_fleet, subetha_ctest_peer_owner_lease, subetha_ctest_peer_rwlock,
    subetha_ctest_peer_semaphore, subetha_ctest_peer_shared_arc, subetha_ctest_peer_waker,
    subetha_ctest_peer_hashmap, subetha_ctest_peer_locale, subetha_ctest_peer_mpmc, subetha_ctest_peer_mpsc,
    subetha_ctest_peer_notify,
    subetha_ctest_peer_pubsub, subetha_ctest_peer_region, subetha_ctest_peer_slab, subetha_ctest_peer_spsc,
    subetha_ctest_peer_stack, subetha_ctest_peer_vec, subetha_ctest_peer_vyukov,
    subetha_ctest_run, subetha_ctest_wait_native,
};

/// The layout the vec and slab exchanges share with the C peers: a
/// sixteen-byte element holding a decimal index, zero past it.
const INDEX_LAYOUT: subetha_element_layout = subetha_element_layout { element_size: 16, alignment: 1, tag: 0x49_4e44_4558_3136 };

/// The same element under the alignment the list's nodes are built at.
const INDEX_LIST_LAYOUT: subetha_element_layout = subetha_element_layout { element_size: 16, alignment: 4, tag: 0x49_4e44_4558_3136 };

const PEER_ITEMS: u32 = 500;

/// How long a push in an exchange waits for the peer to make room, the
/// bound the C peers put on their own pops; a peer that died leaves the
/// push failing with `SUBETHA_E_TIMEOUT` instead of parked for good.
const PEER_WAIT_MS: i64 = 10_000;

fn scratch_prefix(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the wall clock is after the epoch")
        .as_nanos();
    std::env::temp_dir()
        .join(format!("subetha-ffi-{tag}-{}-{nanos}", std::process::id()))
        .to_str()
        .expect("the temp dir is UTF-8")
        .to_owned()
}

fn c_string(s: &str) -> CString {
    CString::new(s).expect("the string has no NUL")
}

#[test]
fn peer_role() {
    let prefix = match std::env::var("SUBETHA_FFI_PEER") {
        Ok(p) => p,
        Err(VarError::NotPresent) => return,
        Err(e) => panic!("SUBETHA_FFI_PEER is set but unreadable: {e}"),
    };
    let kind = match std::env::var("SUBETHA_FFI_PEER_KIND") {
        Ok(k) => k,
        Err(VarError::NotPresent) => "ring".to_owned(),
        Err(e) => panic!("SUBETHA_FFI_PEER_KIND is set but unreadable: {e}"),
    };
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let c_prefix = c_string(&prefix);
    let problems = match kind.as_str() {
        "ring" => unsafe { subetha_ctest_peer(c_prefix.as_ptr(), PEER_ITEMS) },
        "spsc" => unsafe { subetha_ctest_peer_spsc(c_prefix.as_ptr(), PEER_ITEMS) },
        "mpsc" => unsafe { subetha_ctest_peer_mpsc(c_prefix.as_ptr(), PEER_ITEMS) },
        "mpmc" => unsafe { subetha_ctest_peer_mpmc(c_prefix.as_ptr(), PEER_ITEMS) },
        "vyukov" => unsafe { subetha_ctest_peer_vyukov(c_prefix.as_ptr(), PEER_ITEMS) },
        "broadcast" => unsafe { subetha_ctest_peer_broadcast(c_prefix.as_ptr(), PEER_ITEMS) },
        "pubsub" => unsafe { subetha_ctest_peer_pubsub(c_prefix.as_ptr(), PEER_ITEMS) },
        "capacity" => unsafe { subetha_ctest_peer_capacity(c_prefix.as_ptr(), PEER_ITEMS) },
        "locale" => unsafe { subetha_ctest_peer_locale(c_prefix.as_ptr(), PEER_ITEMS) },
        "stack" => unsafe { subetha_ctest_peer_stack(c_prefix.as_ptr(), PEER_ITEMS) },
        "deque" => unsafe { subetha_ctest_peer_deque(c_prefix.as_ptr(), PEER_ITEMS) },
        "notify" => unsafe { subetha_ctest_peer_notify(c_prefix.as_ptr(), PEER_ITEMS) },
        "hashmap" => unsafe { subetha_ctest_peer_hashmap(c_prefix.as_ptr(), PEER_ITEMS) },
        "arena" => unsafe { subetha_ctest_peer_arena(c_prefix.as_ptr(), PEER_ITEMS) },
        "vec" => unsafe { subetha_ctest_peer_vec(c_prefix.as_ptr(), PEER_ITEMS) },
        "slab" => unsafe { subetha_ctest_peer_slab(c_prefix.as_ptr(), PEER_ITEMS) },
        "region" => unsafe { subetha_ctest_peer_region(c_prefix.as_ptr(), PEER_ITEMS) },
        "atomic" => unsafe { subetha_ctest_peer_atomic(c_prefix.as_ptr(), PEER_ITEMS) },
        "list" => unsafe { subetha_ctest_peer_list(c_prefix.as_ptr(), PEER_ITEMS) },
        "btree" => unsafe { subetha_ctest_peer_btree(c_prefix.as_ptr(), PEER_ITEMS) },
        "cell" => unsafe { subetha_ctest_peer_cell(c_prefix.as_ptr(), CELL_ROUNDS) },
        "frames" => unsafe { subetha_ctest_peer_frame_region(c_prefix.as_ptr(), PEER_ITEMS) },
        "epochs" => unsafe { subetha_ctest_peer_epochs(c_prefix.as_ptr(), PEER_ITEMS) },
        "rwlock" => unsafe { subetha_ctest_peer_rwlock(c_prefix.as_ptr(), LOCK_ROUNDS) },
        "semaphore" => unsafe { subetha_ctest_peer_semaphore(c_prefix.as_ptr(), SEMAPHORE_ROUNDS) },
        "condvar" => unsafe { subetha_ctest_peer_condvar(c_prefix.as_ptr(), CONDVAR_ROUNDS) },
        "lease" => unsafe { subetha_ctest_peer_owner_lease(c_prefix.as_ptr(), LEASE_MARK) },
        "fleet" => unsafe { subetha_ctest_peer_fleet(c_prefix.as_ptr(), FLEET_IN_FLIGHT) },
        "barrier" => unsafe { subetha_ctest_peer_epoch_barrier(c_prefix.as_ptr(), BARRIER_ROUNDS) },
        "fence" => unsafe { subetha_ctest_peer_fence_clock(c_prefix.as_ptr(), FENCE_ROUNDS) },
        "arc" => unsafe { subetha_ctest_peer_shared_arc(c_prefix.as_ptr(), ARC_BYTES) },
        "waker" => unsafe { subetha_ctest_peer_waker(c_prefix.as_ptr(), WAKER_ROUNDS) },
        other => panic!("SUBETHA_FFI_PEER_KIND {other} names no peer"),
    };
    let rc = subetha_shutdown();
    assert_eq!(rc, SUBETHA_OK, "the peer left a handle open");
    std::process::exit(problems);
}

fn spawn_peer(kind: &str, prefix: &str) -> Child {
    Command::new(std::env::current_exe().expect("the test binary's own path"))
        .arg("peer_role")
        .arg("--exact")
        .arg("--nocapture")
        .env("SUBETHA_FFI_PEER", prefix)
        .env("SUBETHA_FFI_PEER_KIND", kind)
        .spawn()
        .expect("the peer process spawns")
}

fn wait_for(mut child: Child, kind: &str) {
    let status = child.wait().expect("the peer process is waited on");
    assert!(status.success(), "the {kind} peer reported problems: {status}");
}

/// The index a slot carries as a decimal string, zero past it.
fn index_of(out: &[u8], len: usize) -> u32 {
    assert_eq!(len, SUBETHA_RING_SLOT_BYTES, "a pop yields the whole slot");
    index_in(out)
}

fn index_in(out: &[u8]) -> u32 {
    let end = out.iter().position(|b| *b == 0).expect("the slot is zero past the payload");
    let text = std::str::from_utf8(&out[..end]).expect("the payload is a decimal string");
    text.parse().expect("the payload is an index")
}

#[test]
fn the_c_suite_passes_and_c_peers_cross_a_process_boundary_for_every_family() {
    let suite_prefix = scratch_prefix("suite");
    let c_prefix = c_string(&suite_prefix);
    let failures = unsafe { subetha_ctest_run(c_prefix.as_ptr()) };
    assert_eq!(failures, 0, "{failures} C check(s) failed; each is named on stderr");

    // The suite ends initialized in strict mode.
    ring_exchange();
    spsc_exchange();
    mpsc_exchange();
    mpmc_exchange();
    vyukov_exchange();
    broadcast_exchange();
    pubsub_exchange();
    capacity_exchange();
    locale_exchange();
    stack_exchange();
    deque_exchange();
    notifier_exchange();
    hashmap_exchange();
    arena_exchange();
    vec_exchange();
    slab_exchange();
    region_exchange();
    atomic_exchange();
    list_exchange();
    btree_exchange();
    cell_exchange();
    frame_region_exchange();
    epochs_exchange();
    rwlock_exchange();
    epoch_barrier_exchange();
    fence_clock_exchange();
    shared_arc_exchange();
    waker_exchange();
    semaphore_exchange();
    condvar_exchange();
    owner_lease_exchange();
    fleet_recovery_exchange();
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// Rounds of the cell handshake: enough that a value has to cross the
/// boundary repeatedly rather than once.
const CELL_ROUNDS: u32 = 32;

/// One cell, two processes, taking turns through its version: this one
/// writes a value, a C peer waits for it and writes it back reversed, and
/// this one waits for that. Neither ever reads its own write, because the
/// version it waits for is the one the other side leaves behind.
fn cell_exchange() {
    let path = scratch_prefix("cell");
    let c_path = c_string(&path);
    let mut cell: subetha_handle = 0;
    assert_eq!(unsafe { subetha_cell_create(c_path.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut cell) }, SUBETHA_OK);
    let child = spawn_peer("cell", &path);
    let mut out = [0u8; 8];
    let mut len = 0usize;
    let mut version = 0u32;
    for round in 0..CELL_ROUNDS {
        let sent = u64::from(round).wrapping_mul(0x0101_0101_0101_0101).to_le_bytes();
        assert_eq!(unsafe { subetha_cell_set(cell, sent.as_ptr(), sent.len()) }, SUBETHA_OK);
        // A write is two version steps, so a round of two writes is four,
        // and the peer's reply lands on the fourth of this round's.
        let want = (round + 1) * 4;
        loop {
            assert_eq!(unsafe { subetha_cell_version(cell, &mut version) }, SUBETHA_OK);
            if version >= want {
                break;
            }
            std::hint::spin_loop();
        }
        assert_eq!(unsafe { subetha_cell_get(cell, out.as_mut_ptr(), out.len(), &mut len) }, SUBETHA_OK);
        assert_eq!(len, 8);
        let mut reversed = sent;
        reversed.reverse();
        assert_eq!(out, reversed, "round {round}: the peer returned the value reversed");
    }
    wait_for(child, "cell");
    let mut stats = subetha_cell_stats::default();
    assert_eq!(unsafe { subetha_cell_read_stats(cell, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.value_size, 8);
    assert_eq!(stats.version, u64::from(CELL_ROUNDS) * 4, "both sides wrote once per round");
    assert_eq!(subetha_handle_destroy(cell), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_cell_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the cell's file");
}

/// The B-tree has one writer at a time, so the two processes take turns:
/// this one inserts indexed entries and stops, a C peer checks them,
/// removes the even ones and inserts its own, and this one checks what is
/// left.
fn btree_exchange() {
    let path = scratch_prefix("btree");
    let c_path = c_string(&path);
    let mut map: subetha_handle = 0;
    let tag = 0x49_4e44_4558_3332u64;
    let rc = unsafe { subetha_btree_create(c_path.as_ptr(), 512, 4, 8, tag, SUBETHA_MODE_STRICT, &mut map) };
    assert_eq!(rc, SUBETHA_OK);
    for i in 0..PEER_ITEMS {
        let key = i.to_be_bytes();
        let value = (u64::from(i) * 2).to_le_bytes();
        let rc = unsafe {
            subetha_btree_insert(
                map,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, SUBETHA_OK, "btree insert {i} failed");
    }
    let child = spawn_peer("btree", &path);
    wait_for(child, "btree");

    let mut out = [0u8; 8];
    let mut len = 0usize;
    // The peer removed the even keys and left the odd ones.
    for i in 0..PEER_ITEMS {
        let key = i.to_be_bytes();
        let rc = unsafe { subetha_btree_get(map, key.as_ptr(), key.len(), out.as_mut_ptr(), out.len(), &mut len) };
        if i.is_multiple_of(2) {
            assert_eq!(rc, SUBETHA_E_MAP_KEY_ABSENT, "the peer removed {i}");
        } else {
            assert_eq!(rc, SUBETHA_OK, "the peer left {i}");
            assert_eq!(u64::from_le_bytes(out), u64::from(i) * 2);
        }
    }
    // And inserted its own above them.
    for i in PEER_ITEMS..2 * PEER_ITEMS {
        let key = i.to_be_bytes();
        let rc = unsafe { subetha_btree_get(map, key.as_ptr(), key.len(), out.as_mut_ptr(), out.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's entry {i} is present");
        assert_eq!(u64::from_le_bytes(out), u64::from(i) * 2);
    }
    let mut stats = subetha_btree_stats::default();
    assert_eq!(unsafe { subetha_btree_read_stats(map, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.len, u64::from(PEER_ITEMS + PEER_ITEMS / 2), "the odd half and the peer's own");
    assert_eq!(subetha_handle_destroy(map), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_btree_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the map file");
}

/// The list has one writer at a time, so the two processes take turns:
/// this one pushes indexed nodes and stops, a C peer walks them, pops them
/// and pushes its own, and this one walks what the peer left.
fn list_exchange() {
    let path = scratch_prefix("list");
    let c_path = c_string(&path);
    let mut list: subetha_handle = 0;
    let rc = unsafe { subetha_list_create(c_path.as_ptr(), PEER_ITEMS + 1, &INDEX_LIST_LAYOUT, SUBETHA_MODE_STRICT, &mut list) };
    assert_eq!(rc, SUBETHA_OK);
    for i in 0..PEER_ITEMS {
        let value = map_key(i);
        let mut index = 0u32;
        let rc = unsafe { subetha_list_push_back(list, value.as_ptr(), value.len(), &mut index) };
        assert_eq!(rc, SUBETHA_OK, "list push {i} failed");
    }
    let child = spawn_peer("list", &path);
    wait_for(child, "list");
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut at = 0u32;
    assert_eq!(unsafe { subetha_list_first(list, &mut at) }, SUBETHA_OK);
    for i in PEER_ITEMS..2 * PEER_ITEMS {
        assert_ne!(at, SUBETHA_LIST_HEAD_INDEX, "the peer left a node for index {i}");
        let rc = unsafe { subetha_list_get(list, at, out.as_mut_ptr(), out.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's node {i} reads");
        assert_eq!(index_in(&out), i, "the peer's node {i} holds its index");
        assert_eq!(unsafe { subetha_list_next(list, at, &mut at) }, SUBETHA_OK);
    }
    assert_eq!(at, SUBETHA_LIST_HEAD_INDEX, "the walk comes back to the head");
    let mut stats = subetha_list_stats::default();
    assert_eq!(unsafe { subetha_list_read_stats(list, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.len, u64::from(PEER_ITEMS), "the peer's nodes and none of this process's");
    assert_eq!(subetha_handle_destroy(list), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_list_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the list file");
}

/// Both processes count on one file-backed atomic: this one adds its share
/// while a C peer adds its own, and the flag the peer raises with a
/// release store is acquired here before the total is read.
fn atomic_exchange() {
    let prefix = scratch_prefix("atomic");
    let counter_path = format!("{prefix}.u64");
    let flag_path = format!("{prefix}.flag");
    let c_prefix = c_string(&prefix);
    let c_counter = c_string(&counter_path);
    let c_flag = c_string(&flag_path);
    let mut counter: subetha_handle = 0;
    let mut flag: subetha_handle = 0;
    assert_eq!(unsafe { subetha_atomic_u64_create(c_counter.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut counter) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_bool_create(c_flag.as_ptr(), false, SUBETHA_MODE_STRICT, &mut flag) }, SUBETHA_OK);
    let child = spawn_peer("atomic", &prefix);
    for _ in 0..PEER_ITEMS {
        let rc = unsafe { subetha_atomic_u64_fetch_add_explicit(counter, 1, SUBETHA_ORDER_RELAXED, std::ptr::null_mut()) };
        assert_eq!(rc, SUBETHA_OK);
    }
    wait_for(child, "atomic");
    let mut raised = false;
    assert_eq!(unsafe { subetha_atomic_bool_load_explicit(flag, SUBETHA_ORDER_ACQUIRE, &mut raised) }, SUBETHA_OK);
    assert!(raised, "the peer raised its flag before it exited");
    let mut total = 0u64;
    assert_eq!(unsafe { subetha_atomic_u64_load(counter, &mut total) }, SUBETHA_OK);
    assert_eq!(total, u64::from(2 * PEER_ITEMS), "both processes' additions are in the counter");
    assert_eq!(subetha_handle_destroy(flag), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(counter), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_atomic_unlink(c_counter.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the counter's file");
    assert_eq!(unsafe { subetha_atomic_unlink(c_flag.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the flag's file");
    drop(c_prefix);
}

/// Work units the fleet peer takes before it dies, so this process can
/// count exactly what was abandoned.
const FLEET_IN_FLIGHT: u32 = 5;

/// The recovery a supervisor actually has to do, which needs a process
/// that genuinely goes away. A C peer registers in a heartbeat table,
/// takes leadership of an election, marks work units in flight, beats
/// once, and exits holding all of it.
///
/// This process then does what a supervisor does: ticks the epoch until
/// the grace window has passed, finds the quiet slot, reads the work it
/// abandoned, takes leadership from it, and frees the slot.
fn fleet_recovery_exchange() {
    let path = scratch_prefix("fleet");
    let hb_path = format!("{path}.hb");
    let elect_path = format!("{path}.elect");
    let c_hb = c_string(&hb_path);
    let c_elect = c_string(&elect_path);
    let mut hb: subetha_handle = 0;
    let mut elect: subetha_handle = 0;
    assert_eq!(unsafe { subetha_heartbeat_create(c_hb.as_ptr(), 4, SUBETHA_MODE_STRICT, &mut hb) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_leader_create(c_elect.as_ptr(), SUBETHA_MODE_STRICT, &mut elect) }, SUBETHA_OK);

    let child = spawn_peer("fleet", &path);
    wait_for(child, "fleet");

    // The peer is gone and has left both its slot and the leadership.
    let mut mine = 0u32;
    assert_eq!(unsafe { subetha_current_pid(&mut mine) }, SUBETHA_OK);
    let mut hb_stats = subetha_heartbeat_stats::default();
    assert_eq!(unsafe { subetha_heartbeat_read_stats(hb, &mut hb_stats) }, SUBETHA_OK);
    assert_eq!(hb_stats.registered, 1, "the peer left its slot registered");
    let mut lead = subetha_leader_stats::default();
    assert_eq!(unsafe { subetha_leader_read_stats(elect, &mut lead) }, SUBETHA_OK);
    assert_ne!(lead.leader_pid, SUBETHA_LEADER_NONE, "the peer left the leadership held");
    assert_ne!(lead.leader_pid, mine, "and it was the peer's, not this process's");

    // Find the quiet slot and read what it abandoned. Walking every
    // index, empty ones included, is what a supervisor does.
    let mut departed = None;
    let mut slot = subetha_heartbeat_slot::default();
    for index in 0..hb_stats.capacity {
        assert_eq!(unsafe { subetha_heartbeat_read_slot(hb, index, &mut slot) }, SUBETHA_OK);
        if slot.pid != SUBETHA_HEARTBEAT_EMPTY_PID {
            departed = Some((index, slot));
        }
    }
    let (index, mut slot) = departed.expect("the peer's slot is in the table");
    assert_eq!(slot.pid, lead.leader_pid, "the same process held the slot and the leadership");
    assert_eq!(
        slot.in_flight_bitmap.count_ones(),
        FLEET_IN_FLIGHT,
        "the work the peer was holding when it went is on its slot",
    );
    assert_eq!(slot.last_seen_epoch, 0, "it beat once, at epoch zero");

    // A supervisor with a higher process id cannot take over while the
    // heartbeat still looks fresh.
    let above = slot.pid.saturating_add(1);
    let mut got = true;
    assert_eq!(unsafe { subetha_leader_try_claim(elect, above, 2, &mut got) }, SUBETHA_OK);
    assert!(!got, "a fresh-looking heartbeat holds the leadership");

    // Ticking past the grace window is what marks the peer gone.
    let mut epoch = 0u64;
    for _ in 0..3 {
        assert_eq!(unsafe { subetha_leader_tick_epoch(elect, &mut epoch) }, SUBETHA_OK);
        assert_eq!(unsafe { subetha_heartbeat_tick_epoch(hb, &mut epoch) }, SUBETHA_OK);
    }
    assert_eq!(unsafe { subetha_leader_try_claim(elect, above, 2, &mut got) }, SUBETHA_OK);
    assert!(got, "past the grace window the departed leader is displaced");
    assert_eq!(unsafe { subetha_leader_read_stats(elect, &mut lead) }, SUBETHA_OK);
    assert_eq!(lead.leader_pid, above);
    assert_eq!(lead.election_term, 2, "leadership changed hands twice in all");

    // Reclaiming the slot is the last step, and it frees it for reuse.
    assert_eq!(subetha_heartbeat_unregister(hb, index), SUBETHA_OK);
    assert_eq!(unsafe { subetha_heartbeat_read_stats(hb, &mut hb_stats) }, SUBETHA_OK);
    assert_eq!(hb_stats.registered, 0, "the departed process's slot went back");
    let mut taken = 99u32;
    assert_eq!(unsafe { subetha_heartbeat_register(hb, mine, &mut taken) }, SUBETHA_OK);
    assert_eq!(taken, index, "and the freed slot is the one handed out next");
    assert_eq!(unsafe { subetha_heartbeat_read_slot(hb, index, &mut slot) }, SUBETHA_OK);
    assert_eq!(slot.pid, mine);
    assert_eq!(slot.in_flight_bitmap, 0, "a fresh registration carries no work");

    assert_eq!(subetha_handle_destroy(elect), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(hb), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_heartbeat_unlink(c_hb.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the heartbeat table's file");
    assert_eq!(unsafe { subetha_leader_unlink(c_elect.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the election's file");
}

/// The byte the lease peer leaves in the payload before it exits, so this
/// process can tell what a dead holder wrote from what was there before.
const LEASE_MARK: u32 = 0xAB;

/// A lease exists for the case a lock cannot survive: a holder that dies
/// still holding it. A C peer takes the lease under its own process id,
/// writes a payload, and exits without releasing. Nothing beats for it
/// after that, so ticking the epoch past the grace window is what lets
/// this process take it back - and the payload the dead holder wrote is
/// still there when it does.
///
/// A second process is the only way to show this. Within one process the
/// holder is alive by construction, so the grace window never has a dead
/// holder to expire under.
fn owner_lease_exchange() {
    let path = scratch_prefix("lease");
    let c_path = c_string(&path);
    let mut lease: subetha_handle = 0;
    let initial = [0u8; 8];
    let rc = unsafe {
        subetha_owner_lease_create(c_path.as_ptr(), initial.as_ptr(), initial.len(), 8, SUBETHA_MODE_STRICT, &mut lease)
    };
    assert_eq!(rc, SUBETHA_OK);

    let child = spawn_peer("lease", &path);
    wait_for(child, "lease");

    // The peer is gone and its lease is not released. Its process id is
    // recorded, and it is not this one.
    let mut stats = subetha_owner_lease_stats::default();
    assert_eq!(unsafe { subetha_owner_lease_read_stats(lease, &mut stats) }, SUBETHA_OK);
    let mut mine = 0u32;
    assert_eq!(unsafe { subetha_current_pid(&mut mine) }, SUBETHA_OK);
    assert_ne!(stats.owner_pid, SUBETHA_LEASE_NO_OWNER, "the peer left the lease held");
    assert_ne!(stats.owner_pid, mine, "and it was held by the peer, not by this process");
    assert_eq!(stats.lease_term, 1, "the peer took it once");

    // A pid above the dead holder's cannot take it while the heartbeat
    // still looks fresh, however many times it asks.
    let above = stats.owner_pid.saturating_add(1);
    let mut got = true;
    for _ in 0..4 {
        assert_eq!(unsafe { subetha_owner_lease_try_acquire(lease, above, 2, &mut got) }, SUBETHA_OK);
        assert!(!got, "a fresh-looking heartbeat holds the lease against a higher pid");
    }

    // Ticking the epoch past the grace window is what marks the dead
    // holder gone.
    let mut epoch = 0u64;
    for _ in 0..3 {
        assert_eq!(unsafe { subetha_owner_lease_tick_epoch(lease, &mut epoch) }, SUBETHA_OK);
    }
    assert_eq!(epoch, 3);
    assert_eq!(unsafe { subetha_owner_lease_try_acquire(lease, above, 2, &mut got) }, SUBETHA_OK);
    assert!(got, "past the grace window the dead holder is displaced");
    assert_eq!(unsafe { subetha_owner_lease_read_stats(lease, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.owner_pid, above);
    assert_eq!(stats.lease_term, 2, "the lease changed hands once more");

    // What the dead holder wrote is still there.
    let mut out = [0u8; 8];
    let mut len = 0usize;
    let rc = unsafe { subetha_owner_lease_read(lease, above, out.as_mut_ptr(), out.len(), &mut len) };
    assert_eq!(rc, SUBETHA_OK);
    assert_eq!(len, 8);
    assert_eq!(out, [LEASE_MARK as u8; 8], "the payload survived the holder that wrote it");

    let mut released = false;
    assert_eq!(unsafe { subetha_owner_lease_release(lease, above, &mut released) }, SUBETHA_OK);
    assert!(released);
    assert_eq!(subetha_handle_destroy(lease), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_owner_lease_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the lease's file");
}

/// Rounds of the condvar ping-pong. Enough that a lost notify has many
/// chances to show up as a hang rather than passing by luck.
const CONDVAR_ROUNDS: u32 = 200;

/// A condition variable is a way for one process to park until another
/// says something changed, so a single process cannot show what it is
/// for. This one and a C peer play ping-pong through a shared counter:
/// this side raises it to the next odd number and notifies, the peer
/// waits for that, raises it to the next even one and notifies back.
///
/// Both sides read the generation before checking the counter and hand
/// that reading to the wait. A run that finishes is the evidence: with
/// the lost-notify window open the two deadlock, each parked on a signal
/// the other already sent, and the thirty-second waits turn that into a
/// failure rather than a hang.
fn condvar_exchange() {
    let path = scratch_prefix("cv");
    let c_path = c_string(&path);
    let turn_path = format!("{path}.turn");
    let c_turn = c_string(&turn_path);
    let mut cv: subetha_handle = 0;
    let mut turn: subetha_handle = 0;
    assert_eq!(unsafe { subetha_condvar_create(c_path.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut cv) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_u32_create(c_turn.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut turn) }, SUBETHA_OK);

    let child = spawn_peer("condvar", &path);
    for round in 0..CONDVAR_ROUNDS {
        // Raise the counter to this round's odd number and hand it over.
        assert_eq!(subetha_atomic_u32_store(turn, round * 2 + 1), SUBETHA_OK);
        assert_eq!(unsafe { subetha_condvar_notify_all(cv, std::ptr::null_mut()) }, SUBETHA_OK);

        // Wait for the peer to raise it to the even one.
        let want = round * 2 + 2;
        loop {
            let mut seen = 0u64;
            assert_eq!(unsafe { subetha_condvar_generation(cv, &mut seen) }, SUBETHA_OK);
            let mut at = 0u32;
            assert_eq!(unsafe { subetha_atomic_u32_load(turn, &mut at) }, SUBETHA_OK);
            if at >= want {
                break;
            }
            let rc = subetha_condvar_wait(cv, seen, 30_000);
            assert!(
                rc == SUBETHA_OK || rc == SUBETHA_E_TIMEOUT,
                "round {round}: the wait reported {rc}",
            );
        }
    }
    wait_for(child, "condvar");

    let mut at = 0u32;
    assert_eq!(unsafe { subetha_atomic_u32_load(turn, &mut at) }, SUBETHA_OK);
    assert_eq!(at, CONDVAR_ROUNDS * 2, "both sides took every turn in order");
    let mut stats = subetha_condvar_stats::default();
    assert_eq!(unsafe { subetha_condvar_read_stats(cv, &mut stats) }, SUBETHA_OK);
    assert!(
        stats.generation >= u64::from(CONDVAR_ROUNDS) * 2,
        "every notify from both sides advanced the generation",
    );

    assert_eq!(subetha_handle_destroy(turn), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(cv), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_condvar_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 2, "the condvar's two files");
    assert_eq!(unsafe { subetha_atomic_unlink(c_turn.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the counter's file");
}

/// Permits the semaphore exchange runs with, and rounds each side takes.
/// Two permits against two processes is the smallest shape where the
/// bound and the overlap are both observable.
const SEMAPHORE_PERMITS: u32 = 2;
const SEMAPHORE_ROUNDS: u32 = 200;

/// Limiting how many processes work at once is what a semaphore is for,
/// and a single process cannot show it. Both this process and a C peer
/// take a permit `SEMAPHORE_ROUNDS` times, count themselves in and out of
/// a shared holder count, and raise a shared peak while holding one.
///
/// A peak above `SEMAPHORE_PERMITS` means the semaphore let more
/// processes in than it has permits. A peak of exactly `SEMAPHORE_PERMITS`
/// is also what proves the two ran at the same time, so the bound is
/// evidence rather than an artifact of them never overlapping.
fn semaphore_exchange() {
    let path = scratch_prefix("sem");
    let c_path = c_string(&path);
    let live_path = format!("{path}.live");
    let peak_path = format!("{path}.peak");
    let c_live = c_string(&live_path);
    let c_peak = c_string(&peak_path);
    let mut sem: subetha_handle = 0;
    let mut live: subetha_handle = 0;
    let mut peak: subetha_handle = 0;
    let rc = unsafe {
        subetha_semaphore_create(c_path.as_ptr(), SEMAPHORE_PERMITS, SEMAPHORE_PERMITS, SUBETHA_MODE_STRICT, &mut sem)
    };
    assert_eq!(rc, SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_u32_create(c_live.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut live) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_u32_create(c_peak.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut peak) }, SUBETHA_OK);

    // Raise the peak to `value` if it stands above it. A load then a
    // store loses a raise from the other process; this retries on
    // whatever the swap found instead.
    let raise = |value: u32| {
        let mut current = 0u32;
        assert_eq!(unsafe { subetha_atomic_u32_load(peak, &mut current) }, SUBETHA_OK);
        while value > current {
            let mut swapped = false;
            let rc = unsafe { subetha_atomic_u32_compare_exchange(peak, current, value, &mut current, &mut swapped) };
            assert_eq!(rc, SUBETHA_OK);
            if swapped {
                return;
            }
        }
    };

    let child = spawn_peer("semaphore", &path);
    for round in 0..SEMAPHORE_ROUNDS {
        let mut permit = 0u64;
        let rc = unsafe { subetha_semaphore_acquire(sem, 30_000, &mut permit) };
        assert_eq!(rc, SUBETHA_OK, "round {round}: no permit was taken");
        let mut before = 0u32;
        assert_eq!(unsafe { subetha_atomic_u32_fetch_add(live, 1, &mut before) }, SUBETHA_OK);
        let inside = before + 1;
        assert!(
            inside <= SEMAPHORE_PERMITS,
            "round {round}: {inside} processes were inside a semaphore of {SEMAPHORE_PERMITS}",
        );
        raise(inside);
        // Held long enough that the peer has a real chance to be inside
        // at the same moment.
        std::thread::sleep(std::time::Duration::from_micros(200));
        let mut after = 0u32;
        assert_eq!(unsafe { subetha_atomic_u32_fetch_sub(live, 1, &mut after) }, SUBETHA_OK);
        assert_eq!(subetha_semaphore_release(sem, permit), SUBETHA_OK, "round {round}: the permit did not go back");
    }
    wait_for(child, "semaphore");

    let mut highest = 0u32;
    assert_eq!(unsafe { subetha_atomic_u32_load(peak, &mut highest) }, SUBETHA_OK);
    assert!(
        highest <= SEMAPHORE_PERMITS,
        "the semaphore let {highest} processes in at once against {SEMAPHORE_PERMITS} permits",
    );
    assert_eq!(
        highest, SEMAPHORE_PERMITS,
        "both processes were inside at once at some point, so the bound above is evidence",
    );
    let mut remaining = 0u32;
    assert_eq!(unsafe { subetha_atomic_u32_load(live, &mut remaining) }, SUBETHA_OK);
    assert_eq!(remaining, 0, "every holder counted itself back out");

    let mut stats = subetha_semaphore_stats::default();
    assert_eq!(unsafe { subetha_semaphore_read_stats(sem, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.available, SEMAPHORE_PERMITS, "every permit came back");
    assert_eq!(stats.waiters, 0);
    assert_eq!(stats.release_overflows, 0);

    assert_eq!(subetha_handle_destroy(peak), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(live), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(sem), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_semaphore_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 3, "the semaphore's three files");
    assert_eq!(unsafe { subetha_atomic_unlink(c_live.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the holder count's file");
    assert_eq!(unsafe { subetha_atomic_unlink(c_peak.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the peak's file");
}

/// Rounds the two sides park and wake each other for.
const WAKER_ROUNDS: u32 = 20;

/// Waking a parker in a different process is the only thing this
/// primitive is for, and one process cannot show it: a park woken by the
/// same process exercises the state machine and says nothing about the
/// kernel's cross-process sleep queue.
///
/// The peer parks at each sequence and this process wakes it. The
/// counter is the evidence rather than the wait returning: the peer
/// raises it to `2i - 1` once its park is up and to `2i` once woken, and
/// this side will not wake round `i + 1` until it has seen `2i`. A wait
/// that returned without a wake would run the peer ahead of the counter
/// and this side would time out waiting for it.
fn waker_exchange() {
    let path = scratch_prefix("waker");
    let c_path = c_string(&path);
    let count_path = format!("{path}.count");
    let c_count = c_string(&count_path);

    let mut waker: subetha_handle = 0;
    let mut count: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_waker_create(c_path.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut waker) },
        SUBETHA_OK,
    );
    assert_eq!(
        unsafe { subetha_atomic_u32_create(c_count.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut count) },
        SUBETHA_OK,
    );

    let child = spawn_peer("waker", &path);
    for round in 1..=WAKER_ROUNDS {
        // Wait for the peer's park to be up. Waking before it parks would
        // wake nothing and leave it parked for its whole timeout.
        let began = std::time::Instant::now();
        loop {
            let mut seen = 0u32;
            assert_eq!(unsafe { subetha_atomic_u32_load(count, &mut seen) }, SUBETHA_OK);
            if seen >= round * 2 - 1 {
                break;
            }
            assert!(
                began.elapsed() < std::time::Duration::from_secs(30),
                "round {round}: the peer never parked",
            );
            std::thread::sleep(std::time::Duration::from_micros(200));
        }

        let mut woken = 0u64;
        assert_eq!(unsafe { subetha_waker_wake_up_to(waker, round as u64, &mut woken) }, SUBETHA_OK);

        // Then wait for it to say it woke. A wake that reached nobody
        // leaves the peer parked and this loop times out here.
        let began = std::time::Instant::now();
        loop {
            let mut seen = 0u32;
            assert_eq!(unsafe { subetha_atomic_u32_load(count, &mut seen) }, SUBETHA_OK);
            if seen >= round * 2 {
                break;
            }
            assert!(
                began.elapsed() < std::time::Duration::from_secs(30),
                "round {round}: the peer parked but was not woken from this process",
            );
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
    }
    wait_for(child, "waker");

    let mut total = 0u32;
    assert_eq!(unsafe { subetha_atomic_u32_load(count, &mut total) }, SUBETHA_OK);
    assert_eq!(total, WAKER_ROUNDS * 2, "every round parked and was woken exactly once");

    assert_eq!(subetha_handle_destroy(count), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(waker), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_waker_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the waker's file");
    assert_eq!(unsafe { subetha_atomic_unlink(c_count.as_ptr(), &mut report) }, SUBETHA_OK);
}

/// Bytes the shared value holds in the exchange.
const ARC_BYTES: u32 = 8;

/// A holder table that spans processes is what a shared value is for, and
/// one process cannot show it: a count and a mapping look identical
/// whether the other side exists or is a second copy of the file. This
/// process creates the value, a C peer attaches, and each checks the
/// other is there - the peer requires the count to include the creator and
/// the bytes to be the creator's, and the creator requires the count to
/// include the peer and the bytes to be the peer's reply.
///
/// The peer holds its slot until the creator acknowledges, so the count
/// the creator reads is not racing the peer's exit.
fn shared_arc_exchange() {
    let path = scratch_prefix("arc");
    let c_path = c_string(&path);
    let turn_path = format!("{path}.turn");
    let c_turn = c_string(&turn_path);

    let mut arc: subetha_handle = 0;
    let mut turn: subetha_handle = 0;
    let initial: Vec<u8> = (0..ARC_BYTES as u8).map(|i| i + 1).collect();
    assert_eq!(
        unsafe {
            subetha_shared_arc_create(
                c_path.as_ptr(),
                initial.as_ptr(),
                initial.len(),
                4,
                SUBETHA_ARC_KEEP,
                SUBETHA_MODE_STRICT,
                &mut arc,
            )
        },
        SUBETHA_OK,
    );
    assert_eq!(
        unsafe { subetha_atomic_u32_create(c_turn.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut turn) },
        SUBETHA_OK,
    );

    let child = spawn_peer("arc", &path);

    // The peer reads what is here, writes its reply and says so.
    let began = std::time::Instant::now();
    loop {
        let mut seen = 0u32;
        assert_eq!(unsafe { subetha_atomic_u32_load(turn, &mut seen) }, SUBETHA_OK);
        if seen >= 1 {
            break;
        }
        assert!(
            began.elapsed() < std::time::Duration::from_secs(30),
            "the peer never replied",
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    // It is still holding, so the table counts it.
    let mut count = 0u64;
    assert_eq!(unsafe { subetha_shared_arc_strong_count(arc, &mut count) }, SUBETHA_OK);
    assert_eq!(count, 2, "the peer's slot is counted in this process's table");

    let mut out = [0u8; ARC_BYTES as usize];
    let mut len = 0usize;
    assert_eq!(
        unsafe { subetha_shared_arc_read(arc, 0, out.len(), out.as_mut_ptr(), out.len(), &mut len) },
        SUBETHA_OK,
    );
    assert_eq!(len, out.len());
    let want: Vec<u8> = (0..ARC_BYTES as u8).map(|i| 0xFF - i).collect();
    assert_eq!(out.to_vec(), want, "the peer's write is what this process reads");

    // Acknowledge, so the peer may let go.
    assert_eq!(subetha_atomic_u32_store(turn, 2), SUBETHA_OK);
    wait_for(child, "arc");

    assert_eq!(unsafe { subetha_shared_arc_strong_count(arc, &mut count) }, SUBETHA_OK);
    assert_eq!(count, 1, "the peer released its slot when it exited");

    assert_eq!(subetha_handle_destroy(turn), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(arc), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_shared_arc_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the value's file");
    assert_eq!(unsafe { subetha_atomic_unlink(c_turn.as_ptr(), &mut report) }, SUBETHA_OK);
}

/// Rounds the two sides trade clock readings for.
const FENCE_ROUNDS: u32 = 40;

/// A total order across processes is what a fence clock is for, and one
/// process cannot show it: a clock that only ever ordered its own ticks
/// would look correct from inside. This process and a C peer take turns,
/// this one on even turns and the peer on odd, and each merges the
/// reading the other just produced, requiring the merge to compare
/// strictly above what came in. A merge that dropped its input, or that
/// ordered only within one process, gives back something at or below it.
///
/// The readings therefore climb monotonically across both processes,
/// which this side checks end to end, and the global fence stands at or
/// above every one of them.
fn fence_clock_exchange() {
    let path = scratch_prefix("fence");
    let c_path = c_string(&path);
    let hlc_path = format!("{path}.hlc");
    let c_hlc = c_string(&hlc_path);
    let turn_path = format!("{path}.turn");
    let c_turn = c_string(&turn_path);

    let mut clock: subetha_handle = 0;
    let mut cell: subetha_handle = 0;
    let mut turn: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_fence_clock_create(c_path.as_ptr(), 4, SUBETHA_MODE_STRICT, &mut clock) },
        SUBETHA_OK,
    );
    assert_eq!(unsafe { subetha_cell_create(c_hlc.as_ptr(), 16, SUBETHA_MODE_STRICT, &mut cell) }, SUBETHA_OK);
    assert_eq!(
        unsafe { subetha_atomic_u32_create(c_turn.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut turn) },
        SUBETHA_OK,
    );

    let mut slot = 0u32;
    assert_eq!(unsafe { subetha_fence_clock_register(clock, std::process::id(), &mut slot) }, SUBETHA_OK);

    let child = spawn_peer("fence", &path);
    let mut last = subetha_hlc::default();
    for round in 0..FENCE_ROUNDS {
        let mut ours = subetha_hlc::default();
        assert_eq!(unsafe { subetha_fence_clock_tick(clock, slot, &mut ours) }, SUBETHA_OK);
        assert!(above(ours, last), "round {round}: this side's own clock did not advance");

        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&ours.physical_us.to_ne_bytes());
        bytes[8..].copy_from_slice(&ours.logical.to_ne_bytes());
        assert_eq!(unsafe { subetha_cell_set(cell, bytes.as_ptr(), bytes.len()) }, SUBETHA_OK);
        assert_eq!(subetha_atomic_u32_store(turn, 2 * round + 1), SUBETHA_OK);

        // The peer merges, publishes, and hands the turn back.
        let began = std::time::Instant::now();
        loop {
            let mut seen = 0u32;
            assert_eq!(unsafe { subetha_atomic_u32_load(turn, &mut seen) }, SUBETHA_OK);
            if seen >= 2 * round + 2 {
                break;
            }
            assert!(
                began.elapsed() < std::time::Duration::from_secs(30),
                "round {round}: the peer never took its turn",
            );
            std::thread::sleep(std::time::Duration::from_micros(100));
        }

        let mut len = 0usize;
        assert_eq!(
            unsafe { subetha_cell_get(cell, bytes.as_mut_ptr(), bytes.len(), &mut len) },
            SUBETHA_OK,
        );
        assert_eq!(len, 16);
        let theirs = subetha_hlc {
            physical_us: u64::from_ne_bytes(bytes[..8].try_into().expect("eight bytes")),
            logical: u64::from_ne_bytes(bytes[8..].try_into().expect("eight bytes")),
        };
        assert!(
            above(theirs, ours),
            "round {round}: the peer's merge did not order above what it was sent",
        );

        // Merge it back, so this side's clock stands above what the peer
        // produced. A tick only advances this slot, so without the merge
        // the two clocks would diverge instead of forming one order - and
        // the next round's tick would not outrun the reading just read.
        let mut caught_up = subetha_hlc::default();
        assert_eq!(
            unsafe { subetha_fence_clock_merge(clock, slot, theirs, &mut caught_up) },
            SUBETHA_OK,
        );
        assert!(
            above(caught_up, theirs),
            "round {round}: this side's merge did not order above what it took in",
        );
        last = caught_up;
    }
    wait_for(child, "fence");

    // Every reading either side produced stands at or below the fence.
    let mut fence = subetha_hlc::default();
    assert_eq!(unsafe { subetha_fence_clock_compute_global_fence(clock, &mut fence) }, SUBETHA_OK);
    assert!(
        fence.physical_us > last.physical_us
            || (fence.physical_us == last.physical_us && fence.logical >= last.logical),
        "the fence stands below a reading a participant recorded",
    );

    assert_eq!(subetha_handle_destroy(turn), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(cell), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(clock), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_fence_clock_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_cell_unlink(c_hlc.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_unlink(c_turn.as_ptr(), &mut report) }, SUBETHA_OK);
}

/// Whether `a` stands strictly above `b` in the order every participant
/// agrees on: the physical reading first, the logical counter breaking a
/// tie within it.
fn above(a: subetha_hlc, b: subetha_hlc) -> bool {
    a.physical_us > b.physical_us || (a.physical_us == b.physical_us && a.logical > b.logical)
}

/// Rounds the two sides rendezvous for. Enough that one side running
/// ahead would be caught in an early round rather than by luck.
const BARRIER_ROUNDS: u32 = 40;

/// A rendezvous is the one thing a barrier is for, and a single process
/// cannot show it: threads in one process share a scheduler, so they can
/// pass a broken barrier in step by accident. This process and a C peer
/// each beat in the same heartbeat table and wait at every epoch from
/// zero to `BARRIER_ROUNDS`, incrementing a shared counter after each
/// release.
///
/// The counter is the evidence. Both sides are released at round `i` only
/// once both have arrived, so at least `2 * i` increments from earlier
/// rounds have landed by then. A barrier that let one side run ahead
/// would show a counter still at the previous round's value, and each
/// side checks its own.
fn epoch_barrier_exchange() {
    let path = scratch_prefix("barrier");
    let c_path = c_string(&path);
    let beats_path = format!("{path}-beats.bin");
    let c_beats = c_string(&beats_path);
    let count_path = format!("{path}.count");
    let c_count = c_string(&count_path);

    let mut beats: subetha_handle = 0;
    let mut barrier: subetha_handle = 0;
    let mut count: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_heartbeat_create(c_beats.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut beats) },
        SUBETHA_OK,
    );
    let mut slot = 0u32;
    assert_eq!(unsafe { subetha_heartbeat_register(beats, std::process::id(), &mut slot) }, SUBETHA_OK);
    assert_eq!(
        unsafe { subetha_epoch_barrier_create(c_path.as_ptr(), beats, 0, SUBETHA_MODE_STRICT, &mut barrier) },
        SUBETHA_OK,
    );
    assert_eq!(
        unsafe { subetha_atomic_u32_create(c_count.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut count) },
        SUBETHA_OK,
    );

    let child = spawn_peer("barrier", &path);

    // A wait releases once every LIVE peer has arrived, and the peer is
    // not live until it has registered its own slot. Starting the rounds
    // before then would let this process pass epoch after epoch on its
    // own, as the only peer the table knows about.
    let began = std::time::Instant::now();
    loop {
        let mut live = 0u32;
        assert_eq!(unsafe { subetha_epoch_barrier_live_peers(barrier, &mut live) }, SUBETHA_OK);
        if live >= 2 {
            break;
        }
        assert!(
            began.elapsed() < std::time::Duration::from_secs(30),
            "the peer never registered a slot",
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    for round in 0..BARRIER_ROUNDS {
        let rc = subetha_epoch_barrier_wait_timeout(barrier, round, 30_000);
        assert_eq!(rc, SUBETHA_OK, "round {round}: the barrier did not release");
        let mut before = 0u32;
        assert_eq!(unsafe { subetha_atomic_u32_fetch_add(count, 1, &mut before) }, SUBETHA_OK);
        assert!(
            before + 1 >= 2 * round,
            "round {round}: the counter stood at {}, so the peer had not arrived",
            before + 1,
        );
    }
    wait_for(child, "barrier");

    // Both sides incremented once per round and neither ran ahead.
    let mut total = 0u32;
    assert_eq!(unsafe { subetha_atomic_u32_load(count, &mut total) }, SUBETHA_OK);
    assert_eq!(total, 2 * BARRIER_ROUNDS, "every round was taken by both sides exactly once");

    assert_eq!(subetha_handle_destroy(count), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(barrier), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(beats), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_epoch_barrier_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the barrier's state file");
    assert_eq!(unsafe { subetha_heartbeat_unlink(c_beats.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_atomic_unlink(c_count.as_ptr(), &mut report) }, SUBETHA_OK);
}

/// Rounds each side takes under the lock. Enough that an overlap has many
/// chances to happen, and small enough that the run stays quick with a
/// microsecond of hold time in each.
const LOCK_ROUNDS: u32 = 400;

/// Mutual exclusion is the one thing a lock is for, and a single process
/// cannot show it. Both this process and a C peer take the write hold
/// `LOCK_ROUNDS` times and increment a shared counter under each one. The
/// increment is a read, a pause and a write, so any pair that overlaps
/// loses an update: a total of exactly twice `LOCK_ROUNDS` is the
/// evidence that the two never held it at the same moment.
fn rwlock_exchange() {
    let path = scratch_prefix("rwlock");
    let c_path = c_string(&path);
    let count_path = format!("{path}.count");
    let c_count = c_string(&count_path);
    let mut lock: subetha_handle = 0;
    let mut cell: subetha_handle = 0;
    assert_eq!(unsafe { subetha_rwlock_create(c_path.as_ptr(), SUBETHA_MODE_STRICT, &mut lock) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_cell_create(c_count.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut cell) }, SUBETHA_OK);

    let child = spawn_peer("rwlock", &path);
    let mut out = [0u8; 8];
    let mut len = 0usize;
    for round in 0..LOCK_ROUNDS {
        let mut hold = 0u64;
        let rc = unsafe { subetha_rwlock_write(lock, 30_000, &mut hold) };
        assert_eq!(rc, SUBETHA_OK, "round {round}: the write hold was not taken");
        assert_eq!(unsafe { subetha_cell_get(cell, out.as_mut_ptr(), out.len(), &mut len) }, SUBETHA_OK);
        assert_eq!(len, 8);
        let seen = u64::from_le_bytes(out);
        // The pause is what makes an overlapping pair lose an update, so
        // the counter is evidence about the lock rather than about how
        // fast the two processes happen to run.
        std::thread::sleep(std::time::Duration::from_micros(1));
        let bytes = (seen + 1).to_le_bytes();
        assert_eq!(unsafe { subetha_cell_set(cell, bytes.as_ptr(), bytes.len()) }, SUBETHA_OK);
        assert_eq!(subetha_rwlock_unlock(lock, hold), SUBETHA_OK, "round {round}: the hold did not release");
    }
    wait_for(child, "rwlock");

    assert_eq!(unsafe { subetha_cell_get(cell, out.as_mut_ptr(), out.len(), &mut len) }, SUBETHA_OK);
    assert_eq!(
        u64::from_le_bytes(out),
        u64::from(LOCK_ROUNDS) * 2,
        "every increment from both processes landed, so the two never held the lock at once",
    );
    let mut stats = subetha_rwlock_stats::default();
    assert_eq!(unsafe { subetha_rwlock_read_stats(lock, &mut stats) }, SUBETHA_OK);
    assert!(!stats.has_writer, "every hold was released");
    assert_eq!(stats.readers, 0);
    assert_eq!(stats.timeouts, 0, "no acquire waited out its thirty seconds");

    assert_eq!(subetha_handle_destroy(cell), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(lock), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_rwlock_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the lock's file");
    assert_eq!(unsafe { subetha_cell_unlink(c_count.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the counter's file");
}

/// The two properties an epoch table exists for are the ones a single
/// process cannot show: a pin held in a second process holds the reclaim
/// horizon this one computes, and a ticket open in a second process holds
/// the epoch this one reads as published. A C peer plays the other side.
///
/// Neither side can watch the other through the table itself - an open
/// ticket holds the published epoch down, which is the thing under test -
/// so the two step through a shared cell beside it.
fn epochs_exchange() {
    let path = scratch_prefix("epochs");
    let c_path = c_string(&path);
    let step_path = format!("{path}.step");
    let c_step = c_string(&step_path);
    let mut epochs: subetha_handle = 0;
    let mut cell: subetha_handle = 0;
    assert_eq!(unsafe { subetha_epochs_create(c_path.as_ptr(), 4, SUBETHA_MODE_STRICT, &mut epochs) }, SUBETHA_OK);
    assert_eq!(unsafe { subetha_cell_create(c_step.as_ptr(), 8, SUBETHA_MODE_STRICT, &mut cell) }, SUBETHA_OK);

    let step = |to: u64| {
        let bytes = to.to_le_bytes();
        assert_eq!(unsafe { subetha_cell_set(cell, bytes.as_ptr(), bytes.len()) }, SUBETHA_OK, "step {to}");
    };
    let await_step = |want: u64| {
        let mut out = [0u8; 8];
        let mut len = 0usize;
        for _ in 0..100_000 {
            assert_eq!(unsafe { subetha_cell_get(cell, out.as_mut_ptr(), out.len(), &mut len) }, SUBETHA_OK);
            if u64::from_le_bytes(out) >= want {
                return;
            }
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        panic!("the peer never reached step {want}");
    };

    let child = spawn_peer("epochs", &path);
    let mut before = 0u64;
    assert_eq!(unsafe { subetha_epochs_advance(epochs, &mut before) }, SUBETHA_OK);
    step(1);

    // The peer's pin holds this process's horizon wherever the counter
    // runs to.
    await_step(2);
    let mut horizon = 0u64;
    assert_eq!(unsafe { subetha_epochs_reclaim_horizon(epochs, &mut horizon) }, SUBETHA_OK);
    assert_eq!(horizon, before, "the peer pinned the epoch this process had reached");
    let mut counter = 0u64;
    for _ in 0..PEER_ITEMS {
        assert_eq!(unsafe { subetha_epochs_advance(epochs, &mut counter) }, SUBETHA_OK);
    }
    assert_eq!(unsafe { subetha_epochs_reclaim_horizon(epochs, &mut horizon) }, SUBETHA_OK);
    assert_eq!(horizon, before, "a pin in another process holds this one's horizon");
    let mut stats = subetha_epochs_stats::default();
    assert_eq!(unsafe { subetha_epochs_read_stats(epochs, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.live_pins, 1, "the peer's pin is in the shared table");
    step(3);

    await_step(4);
    assert_eq!(unsafe { subetha_epochs_reclaim_horizon(epochs, &mut horizon) }, SUBETHA_OK);
    assert_eq!(horizon, counter, "the released pin lets this process's horizon run on");
    step(5);

    // The peer's ticket holds this process's published epoch one below
    // its own, however far the counter runs.
    await_step(6);
    let reserved = counter + 1;
    let mut now = 0u64;
    assert_eq!(unsafe { subetha_epochs_now(epochs, &mut now) }, SUBETHA_OK);
    assert_eq!(now, counter, "the peer's open ticket took the next epoch and did not publish it");
    for _ in 0..PEER_ITEMS {
        assert_eq!(unsafe { subetha_epochs_advance(epochs, &mut counter) }, SUBETHA_OK);
    }
    assert_eq!(unsafe { subetha_epochs_now(epochs, &mut now) }, SUBETHA_OK);
    assert_eq!(now, reserved - 1, "a ticket in another process holds this one's published epoch");
    assert_eq!(unsafe { subetha_epochs_read_stats(epochs, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.open_tickets, 1, "the peer's ticket is in the shared table");
    step(7);

    await_step(8);
    assert_eq!(unsafe { subetha_epochs_now(epochs, &mut now) }, SUBETHA_OK);
    assert_eq!(now, counter, "the peer's publish let this process's epoch run on");

    wait_for(child, "epochs");
    assert_eq!(subetha_handle_destroy(cell), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(epochs), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_epochs_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the epoch table's file");
    assert_eq!(unsafe { subetha_cell_unlink(c_step.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the step cell's file");
}

/// This process takes every block of a file-backed frame region and writes
/// each block's own index into it; a C peer opens it, checks them, frees
/// them all and takes them all back. What this process sees afterwards is
/// a region with nothing left to hand out, which it can only be if the
/// peer's frees and the peer's allocations both landed in the same file.
fn frame_region_exchange() {
    let path = scratch_prefix("frames");
    let c_path = c_string(&path);
    let mut region: subetha_handle = 0;
    let rc = unsafe { subetha_frame_region_create(c_path.as_ptr(), 16, PEER_ITEMS, SUBETHA_MODE_STRICT, &mut region) };
    assert_eq!(rc, SUBETHA_OK);
    for i in 0..PEER_ITEMS {
        let mut block = SUBETHA_FRAME_NO_BLOCK;
        assert_eq!(unsafe { subetha_frame_region_alloc(region, &mut block) }, SUBETHA_OK, "block {i} allocates");
        assert_eq!(block, i, "block {i} lands at its own index");
        let payload = map_key(i);
        let rc = unsafe { subetha_frame_region_write(region, block, payload.as_ptr(), payload.len()) };
        assert_eq!(rc, SUBETHA_OK, "block {i} takes its payload");
    }
    let child = spawn_peer("frames", &path);
    wait_for(child, "frames");
    let mut spare = SUBETHA_FRAME_NO_BLOCK;
    assert_eq!(
        unsafe { subetha_frame_region_alloc(region, &mut spare) },
        SUBETHA_E_RING_FULL,
        "the peer took back every block it freed",
    );
    let mut stats = subetha_frame_region_stats::default();
    assert_eq!(unsafe { subetha_frame_region_read_stats(region, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.block_size, 16);
    assert_eq!(stats.block_count, u64::from(PEER_ITEMS));
    assert_eq!(subetha_handle_destroy(region), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_frame_region_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the frame region's file");
}

/// This process allocates indexed slots in a file-backed region; a C peer
/// opens it, checks them, and allocates its own, which this process reads
/// and frees.
fn region_exchange() {
    let path = scratch_prefix("region");
    let c_path = c_string(&path);
    let mut region: subetha_handle = 0;
    let rc = unsafe { subetha_region_create(c_path.as_ptr(), 2 * PEER_ITEMS, &INDEX_LAYOUT, SUBETHA_MODE_STRICT, &mut region) };
    assert_eq!(rc, SUBETHA_OK);
    for i in 0..PEER_ITEMS {
        let element = map_key(i);
        let mut index = 0u32;
        let rc = unsafe { subetha_region_allocate(region, element.as_ptr(), element.len(), &mut index) };
        assert_eq!(rc, SUBETHA_OK, "region allocate {i} failed");
        assert_eq!(index, i, "slot {i} lands at its own index");
    }
    let child = spawn_peer("region", &path);
    wait_for(child, "region");
    let mut out = [0u8; 16];
    let mut len = 0usize;
    for i in PEER_ITEMS..2 * PEER_ITEMS {
        let rc = unsafe { subetha_region_get(region, i, out.as_mut_ptr(), out.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's slot {i} reads");
        assert_eq!(index_in(&out), i, "the peer's slot {i} holds its index");
        let rc = unsafe { subetha_region_free(region, i, out.as_mut_ptr(), out.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's slot {i} frees");
    }
    let mut stats = subetha_region_stats::default();
    assert_eq!(unsafe { subetha_region_read_stats(region, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.len, u64::from(PEER_ITEMS), "this process's slots are still held");
    assert_eq!(stats.free_count, u64::from(PEER_ITEMS), "the peer's slots are on the free list");
    assert_eq!(subetha_handle_destroy(region), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_region_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the region file");
}

/// This process pushes indexed elements into a file-backed vec; a C peer
/// opens it, checks them, and pushes its own, which this process checks
/// at the indexes they landed at.
fn vec_exchange() {
    let path = scratch_prefix("vec");
    let c_path = c_string(&path);
    let mut vec: subetha_handle = 0;
    let rc = unsafe { subetha_vec_create(c_path.as_ptr(), 2 * PEER_ITEMS, &INDEX_LAYOUT, SUBETHA_MODE_STRICT, &mut vec) };
    assert_eq!(rc, SUBETHA_OK);
    for i in 0..PEER_ITEMS {
        let element = map_key(i);
        let mut index = 0u64;
        let rc = unsafe { subetha_vec_push_back(vec, element.as_ptr(), element.len(), &mut index) };
        assert_eq!(rc, SUBETHA_OK, "vec push {i} failed");
        assert_eq!(index, u64::from(i), "element {i} lands at its own index");
    }
    let child = spawn_peer("vec", &path);
    wait_for(child, "vec");
    let mut out = [0u8; 16];
    let mut len = 0usize;
    for i in PEER_ITEMS..2 * PEER_ITEMS {
        let rc = unsafe { subetha_vec_get(vec, u64::from(i), out.as_mut_ptr(), out.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's element {i} is present");
        assert_eq!(len, 16);
        assert_eq!(index_in(&out), i, "the peer's element {i} holds its index");
    }
    let mut stats = subetha_vec_stats::default();
    assert_eq!(unsafe { subetha_vec_read_stats(vec, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.len, u64::from(2 * PEER_ITEMS));
    assert_eq!(subetha_handle_destroy(vec), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_vec_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the vec file");
}

/// This process writes indexed records into the first half of a
/// file-backed slab; a C peer opens it, checks them, and writes the second
/// half, which this process checks along with each slot's version.
fn slab_exchange() {
    let path = scratch_prefix("slab");
    let c_path = c_string(&path);
    let mut slab: subetha_handle = 0;
    let rc = unsafe { subetha_slab_create(c_path.as_ptr(), 2 * PEER_ITEMS, &INDEX_LAYOUT, SUBETHA_MODE_STRICT, &mut slab) };
    assert_eq!(rc, SUBETHA_OK);
    for i in 0..PEER_ITEMS {
        let record = map_key(i);
        let rc = unsafe { subetha_slab_set(slab, u64::from(i), record.as_ptr(), record.len()) };
        assert_eq!(rc, SUBETHA_OK, "slab set {i} failed");
    }
    let child = spawn_peer("slab", &path);
    wait_for(child, "slab");
    let mut out = [0u8; 16];
    let mut len = 0usize;
    let mut version = 0u32;
    for i in PEER_ITEMS..2 * PEER_ITEMS {
        let rc = unsafe { subetha_slab_get(slab, u64::from(i), out.as_mut_ptr(), out.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's record {i} reads");
        assert_eq!(len, 16);
        assert_eq!(index_in(&out), i, "the peer's record {i} holds its index");
        assert_eq!(unsafe { subetha_slab_slot_version(slab, u64::from(i), &mut version) }, SUBETHA_OK);
        assert_eq!(version, 2, "the peer wrote slot {i} once");
    }
    assert_eq!(subetha_handle_destroy(slab), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_slab_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the slab file");
}

/// This process interns indexed values into a file-backed arena; a C peer
/// opens it, resolves them from the offsets their lengths add up to, and
/// interns its own, which this process resolves the same way.
fn arena_exchange() {
    let path = scratch_prefix("arena");
    let c_path = c_string(&path);
    let mut arena: subetha_handle = 0;
    assert_eq!(unsafe { subetha_arena_create(c_path.as_ptr(), 1 << 20, SUBETHA_MODE_STRICT, &mut arena) }, SUBETHA_OK);
    let mut offset = 0u64;
    for i in 0..PEER_ITEMS {
        let text = format!("item-{i}");
        let mut reference = 0u64;
        let rc = unsafe { subetha_arena_intern(arena, text.as_ptr(), text.len(), &mut reference) };
        assert_eq!(rc, SUBETHA_OK, "arena intern {i} failed");
        assert_eq!(subetha_arena_ref_offset(reference), offset, "value {i} lands where the ones before it end");
        assert_eq!(subetha_arena_ref_len(reference), text.len() as u32);
        offset += text.len() as u64;
    }
    let child = spawn_peer("arena", &path);
    wait_for(child, "arena");
    let mut out = [0u8; 32];
    let mut len = 0usize;
    for i in 0..PEER_ITEMS {
        let want = format!("peer-{i}");
        let mut reference = 0u64;
        assert_eq!(unsafe { subetha_arena_ref_pack(offset, want.len() as u32, &mut reference) }, SUBETHA_OK);
        let rc = unsafe { subetha_arena_get(arena, reference, out.as_mut_ptr(), out.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's value {i} resolves");
        assert_eq!(&out[..len], want.as_bytes(), "the peer's value {i}");
        offset += want.len() as u64;
    }
    let mut stats = subetha_arena_stats::default();
    assert_eq!(unsafe { subetha_arena_read_stats(arena, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.used_bytes, offset, "both sides' values account for every byte");
    assert_eq!(subetha_handle_destroy(arena), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_arena_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the arena file");
}

/// A sixteen-byte key holding a decimal index, zero past it.
fn map_key(index: u32) -> [u8; 16] {
    let mut key = [0u8; 16];
    let text = index.to_string();
    key[..text.len()].copy_from_slice(text.as_bytes());
    key
}

/// This process inserts indexed entries into a file-backed map; a C peer
/// opens it, checks them, and inserts its own, which this process checks.
fn hashmap_exchange() {
    let path = scratch_prefix("hashmap");
    let c_path = c_string(&path);
    let mut map: subetha_handle = 0;
    assert_eq!(unsafe { subetha_hashmap_create(c_path.as_ptr(), 4096, 16, 8, SUBETHA_MODE_STRICT, &mut map) }, SUBETHA_OK);
    for i in 0..PEER_ITEMS {
        let key = map_key(i);
        let value = (u64::from(i) * 2).to_le_bytes();
        let rc = unsafe { subetha_hashmap_insert(map, key.as_ptr(), key.len(), value.as_ptr(), value.len(), std::ptr::null_mut()) };
        assert_eq!(rc, SUBETHA_OK, "map insert {i} failed");
    }
    let child = spawn_peer("hashmap", &path);
    wait_for(child, "hashmap");
    let mut value = [0u8; 8];
    let mut len = 0usize;
    for i in PEER_ITEMS..2 * PEER_ITEMS {
        let key = map_key(i);
        let rc = unsafe { subetha_hashmap_get(map, key.as_ptr(), key.len(), value.as_mut_ptr(), value.len(), &mut len) };
        assert_eq!(rc, SUBETHA_OK, "the peer's entry {i} is present");
        assert_eq!(u64::from_le_bytes(value), u64::from(i) * 2, "the peer's entry {i} holds its index doubled");
    }
    let mut stats = subetha_hashmap_stats::default();
    assert_eq!(unsafe { subetha_hashmap_read_stats(map, &mut stats) }, SUBETHA_OK);
    assert_eq!(stats.len, u64::from(2 * PEER_ITEMS));
    assert_eq!(subetha_handle_destroy(map), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_hashmap_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the map file");
}

/// This process watches a notifier on a file-backed ring, through the
/// ABI's wait and through the native object; a C peer in another process
/// pushes, and every item is popped after a signal.
fn notifier_exchange() {
    let prefix = scratch_prefix("notify");
    let c_prefix = c_string(&prefix);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
    let mut ring: subetha_handle = 0;
    assert_eq!(unsafe { subetha_ring_create(c_prefix.as_ptr(), 1, 1, 64, &options, &mut ring) }, SUBETHA_OK);
    let mut cid = 0u32;
    assert_eq!(unsafe { subetha_ring_register_consumer(ring, &mut cid) }, SUBETHA_OK);
    let mut notifier: subetha_handle = 0;
    assert_eq!(unsafe { subetha_ring_notifier(ring, &mut notifier) }, SUBETHA_OK);
    let mut native = 0u64;
    assert_eq!(unsafe { subetha_notifier_native(notifier, &mut native) }, SUBETHA_OK);
    assert_eq!(subetha_notifier_wait(notifier, 20), SUBETHA_E_TIMEOUT, "nothing pushed yet");

    let child = spawn_peer("notify", &prefix);
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    let mut next = 0u32;
    let mut signals = 0u32;
    while next < PEER_ITEMS {
        // Alternate the ABI's wait and the event loop's own wait on the
        // native object; both must see the peer's pushes.
        if signals.is_multiple_of(2) {
            assert_eq!(subetha_notifier_wait(notifier, 10_000), SUBETHA_OK, "the peer's push signals the notifier");
        } else {
            assert_eq!(unsafe { subetha_ctest_wait_native(native, 10_000) }, 1, "the native object is signaled");
        }
        signals += 1;
        assert_eq!(subetha_notifier_drain(notifier), SUBETHA_OK);
        loop {
            let rc = unsafe { subetha_ring_try_pop(ring, cid, out.as_mut_ptr(), out.len(), &mut len) };
            if rc == SUBETHA_E_RING_EMPTY {
                break;
            }
            assert_eq!(rc, SUBETHA_OK);
            assert_eq!(index_of(&out, len), next, "items arrive in order");
            next += 1;
        }
    }
    wait_for(child, "notify");
    println!("notifier exchange: {PEER_ITEMS} items after {signals} signals");
    assert!(signals <= PEER_ITEMS, "never more signals than pushes");

    assert_eq!(subetha_handle_destroy(notifier), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_ring_unlink(c_prefix.as_ptr(), 1, &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert!(report.removed >= 4, "the ring, its wakers and the notifier record: {}", report.removed);
}

/// The layout the stack and deque exchanges declare on both sides: a
/// sixteen-byte element holding a decimal index, zero past it.
fn index_layout() -> subetha_element_layout {
    subetha_element_layout { element_size: 16, alignment: 1, tag: 0x0049_4e44_4558_3136 }
}

/// This process pushes indexed elements onto a file-backed stack; a C peer
/// attaches and pops every one. Capacity 64 against 500 items: the pusher
/// must park on the file waker and be woken by the peer's pops.
fn stack_exchange() {
    let path = scratch_prefix("stack");
    let c_path = c_string(&path);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
    let layout = index_layout();
    let mut stack: subetha_handle = 0;
    assert_eq!(unsafe { subetha_stack_create(c_path.as_ptr(), 64, &layout, &options, &mut stack) }, SUBETHA_OK);

    let child = spawn_peer("stack", &path);
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_stack_push_wait(stack, item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "stack push {i} failed");
    }
    wait_for(child, "stack");

    assert_eq!(subetha_handle_destroy(stack), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_stack_unlink(c_path.as_ptr(), &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 3, "the stack file and its two wakers");
}

/// This process owns a file-backed deque and pushes indexed elements; a C
/// peer opens it as a thief and steals every one, in order. Capacity 64
/// against 500 items: the owner parks on the file waker and the peer's
/// steals wake it.
fn deque_exchange() {
    let path = scratch_prefix("deque");
    let c_path = c_string(&path);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
    let layout = index_layout();
    let mut deque: subetha_handle = 0;
    assert_eq!(unsafe { subetha_deque_create(c_path.as_ptr(), 64, &layout, &options, &mut deque) }, SUBETHA_OK);

    let child = spawn_peer("deque", &path);
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_deque_push_wait(deque, item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "deque push {i} failed");
    }
    wait_for(child, "deque");

    assert_eq!(subetha_handle_destroy(deque), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_deque_unlink(c_path.as_ptr(), &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 3, "the deque file and its two wakers");
}

/// This process produces on a file-backed capacity ring; a C peer attaches
/// under the same base and drains it.
fn capacity_exchange() {
    let base = scratch_prefix("capacity");
    let c_base = c_string(&base);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
    let mut ring: subetha_handle = 0;
    assert_eq!(unsafe { subetha_capacity_create(c_base.as_ptr(), 1, 1, 64, &options, &mut ring) }, SUBETHA_OK);
    let mut pid = 0u32;
    assert_eq!(unsafe { subetha_capacity_register_producer(ring, &mut pid) }, SUBETHA_OK);

    let child = spawn_peer("capacity", &base);
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_capacity_push_wait(ring, pid, item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "capacity push {i} failed");
    }
    wait_for(child, "capacity");

    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_capacity_unlink(c_base.as_ptr(), &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert!(report.removed >= 7, "the backing's files and the two wakers: {}", report.removed);
}

/// This process creates a locale ring, moves it to the file locale and
/// produces; a C peer attaches, finds it there, and drains it.
fn locale_exchange() {
    let base = scratch_prefix("locale");
    let c_base = c_string(&base);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() };
    let mut ring: subetha_handle = 0;
    assert_eq!(unsafe { subetha_locale_ring_create(c_base.as_ptr(), 1, 1, 64, &options, &mut ring) }, SUBETHA_OK);
    let mut pid = 0u32;
    assert_eq!(unsafe { subetha_locale_ring_register_producer(ring, &mut pid) }, SUBETHA_OK);
    assert_eq!(subetha_locale_ring_migrate(ring, SUBETHA_LOCALE_FILE), SUBETHA_OK);

    let child = spawn_peer("locale", &base);
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_locale_ring_push_wait(ring, pid, item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "locale push {i} failed");
    }
    wait_for(child, "locale");

    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_locale_ring_unlink(c_base.as_ptr(), 1, &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
}

/// A C peer produces into a Vyukov ring; this process drains it.
fn vyukov_exchange() {
    let path = scratch_prefix("vyukov");
    let c_path = c_string(&path);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut ring: subetha_handle = 0;
    assert_eq!(unsafe { subetha_vyukov_create(c_path.as_ptr(), 64, &options, &mut ring) }, SUBETHA_OK);

    let child = spawn_peer("vyukov", &path);
    let mut out = [0u8; SUBETHA_RING_PAYLOAD_MAX];
    let mut len = 0usize;
    for i in 0..PEER_ITEMS {
        let rc = unsafe { subetha_vyukov_pop_wait(ring, out.as_mut_ptr(), out.len(), &mut len, 30_000) };
        assert_eq!(rc, SUBETHA_OK, "vyukov pop {i} failed");
        assert_eq!(len, SUBETHA_RING_PAYLOAD_MAX);
        assert_eq!(index_in(&out), i, "a single producer's items arrive in order");
    }
    wait_for(child, "vyukov");

    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_vyukov_unlink(c_path.as_ptr(), &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 3, "the ring and its two wakers");
}

/// This process broadcasts; a C peer registers and reads every item. The
/// pushes start only once the peer is registered, since a consumer starts
/// at the producer's current position.
fn broadcast_exchange() {
    let path = scratch_prefix("broadcast");
    let c_path = c_string(&path);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut ring: subetha_handle = 0;
    assert_eq!(unsafe { subetha_broadcast_create(c_path.as_ptr(), 64, &options, &mut ring) }, SUBETHA_OK);

    let child = spawn_peer("broadcast", &path);
    let started = std::time::Instant::now();
    loop {
        let mut stats = subetha_broadcast_stats::default();
        assert_eq!(unsafe { subetha_broadcast_read_stats(ring, &mut stats) }, SUBETHA_OK);
        if stats.active_consumers >= 1 {
            break;
        }
        assert!(started.elapsed().as_secs() < 30, "the broadcast peer never registered");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_broadcast_push_wait(ring, item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "broadcast push {i} failed");
    }
    wait_for(child, "broadcast");

    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_broadcast_unlink(c_path.as_ptr(), &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 3, "the ring and its two wakers");
}

/// This process publishes; a C peer subscribes with a file position and
/// reads every item, and the position it leaves behind says so.
fn pubsub_exchange() {
    let path = scratch_prefix("pubsub");
    let c_path = c_string(&path);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut ring: subetha_handle = 0;
    // 1024 slots hold every item, so the subscriber's pace loses nothing.
    assert_eq!(unsafe { subetha_pubsub_create(c_path.as_ptr(), 1024, &options, &mut ring) }, SUBETHA_OK);

    let child = spawn_peer("pubsub", &path);
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_pubsub_publish(ring, item.as_ptr(), item.len(), std::ptr::null_mut()) };
        assert_eq!(rc, SUBETHA_OK, "publish {i} failed");
    }
    wait_for(child, "pubsub");

    let pos_path = format!("{path}.pos");
    let c_pos = c_string(&pos_path);
    let mut subscriber: subetha_handle = 0;
    let rc = unsafe { subetha_pubsub_subscribe_file(ring, c_pos.as_ptr(), 0, false, SUBETHA_MODE_STRICT, &mut subscriber) };
    assert_eq!(rc, SUBETHA_OK);
    let mut position = 0u64;
    assert_eq!(unsafe { subetha_subscriber_position(subscriber, &mut position) }, SUBETHA_OK);
    assert_eq!(position, u64::from(PEER_ITEMS), "the peer's file position records what it read");
    assert_eq!(subetha_handle_destroy(subscriber), SUBETHA_OK);
    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_pubsub_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 2, "the ring and its waker");
    assert_eq!(unsafe { subetha_pubsub_unlink_position(c_pos.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1);
}

/// This process produces on an adaptive ring; a C peer drains it.
fn ring_exchange() {
    let prefix = scratch_prefix("peer");
    let c_prefix = c_string(&prefix);
    let options = subetha_ring_options { mode: SUBETHA_MODE_STRICT, max_waiters: 0, scan_interval_us: 0, ..Default::default() };
    let mut ring = 0u64;
    let rc = unsafe { subetha_ring_create(c_prefix.as_ptr(), 1, 1, 64, &options, &mut ring) };
    assert_eq!(rc, SUBETHA_OK);
    let mut pid = 0u32;
    assert_eq!(unsafe { subetha_ring_register_producer(ring, &mut pid) }, SUBETHA_OK);

    let child = spawn_peer("ring", &prefix);
    // Capacity 64 against 500 items: the producer must park on the file
    // waker and be woken by the peer's pops for this to finish.
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_ring_push_wait(ring, pid, item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "push {i} failed");
    }
    wait_for(child, "ring");

    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_ring_unlink(c_prefix.as_ptr(), 1, &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 8, "spsc, vyukov, peers, one mpsc, one mpmc, two wakers, the notifier record");
}

/// This process produces on a blocking SPSC ring; a C peer drains it.
fn spsc_exchange() {
    let base = scratch_prefix("spsc");
    let c_base = c_string(&base);
    let mut ring: subetha_handle = 0;
    let rc = unsafe { subetha_spsc_create(c_base.as_ptr(), 64, SUBETHA_MODE_STRICT, &mut ring) };
    assert_eq!(rc, SUBETHA_OK);

    let child = spawn_peer("spsc", &base);
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_spsc_push_wait(ring, item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "spsc push {i} failed");
    }
    wait_for(child, "spsc");

    assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_spsc_unlink(c_base.as_ptr(), &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 4, "the ring, its two wakers and the notifier record");
}

/// This process plays producer 0 and the consumer of a two-producer pool;
/// a C peer plays producer 1. Each producer's items arrive in its own order.
fn mpsc_exchange() {
    let prefix = scratch_prefix("mpsc");
    let c_prefix = c_string(&prefix);
    let mut producers = [0 as subetha_handle; 2];
    let mut consumer: subetha_handle = 0;
    let rc = unsafe {
        subetha_mpsc_create_pool(c_prefix.as_ptr(), 2, 64, SUBETHA_MODE_STRICT, producers.as_mut_ptr(), &mut consumer)
    };
    assert_eq!(rc, SUBETHA_OK);

    let child = spawn_peer("mpsc", &prefix);
    let drain = std::thread::spawn(move || {
        let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
        let mut len = 0usize;
        let mut next_own = 0u32;
        let mut next_peer = 0u32;
        for _ in 0..2 * PEER_ITEMS {
            let rc = unsafe { subetha_mpsc_pop_wait(consumer, out.as_mut_ptr(), out.len(), &mut len, 30_000) };
            assert_eq!(rc, SUBETHA_OK, "the pool drain failed");
            if out[0] == b'p' {
                let index = index_of(&out[1..], len);
                assert_eq!(index, next_own, "producer 0's items arrive in order");
                next_own += 1;
            } else {
                let index = index_of(&out, len);
                assert_eq!(index, next_peer, "producer 1's items arrive in order");
                next_peer += 1;
            }
        }
        assert_eq!(next_own, PEER_ITEMS);
        assert_eq!(next_peer, PEER_ITEMS);
    });
    for i in 0..PEER_ITEMS {
        let item = format!("p{i}");
        let rc = unsafe { subetha_mpsc_push_wait(producers[0], item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "mpsc push {i} failed");
    }
    drain.join().expect("the drain thread finishes");
    wait_for(child, "mpsc");

    for h in producers {
        assert_eq!(subetha_handle_destroy(h), SUBETHA_OK);
    }
    assert_eq!(subetha_handle_destroy(consumer), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_mpsc_unlink(c_prefix.as_ptr(), 2, &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 5, "two rings, two producer wakers, one consumer waker");
}

/// This process plays producer 1 and consumer 0 of a 2x2 grid; a C peer
/// plays consumer 1 and producer 0. Ring i belongs to consumer i % 2, so
/// each side's pushes land in the other side's subset.
fn mpmc_exchange() {
    let prefix = scratch_prefix("mpmc");
    let c_prefix = c_string(&prefix);
    let mut producers = [0 as subetha_handle; 2];
    let mut consumers = [0 as subetha_handle; 2];
    let rc = unsafe {
        subetha_mpmc_create_grid(
            c_prefix.as_ptr(),
            2,
            2,
            64,
            SUBETHA_MODE_STRICT,
            producers.as_mut_ptr(),
            consumers.as_mut_ptr(),
        )
    };
    assert_eq!(rc, SUBETHA_OK);

    let child = spawn_peer("mpmc", &prefix);
    // The peer drains first and pushes second; this side pushes first and
    // drains second, so neither waits on a ring nobody drains.
    for i in 0..PEER_ITEMS {
        let item = i.to_string();
        let rc = unsafe { subetha_mpmc_push_wait(producers[1], item.as_ptr(), item.len(), PEER_WAIT_MS) };
        assert_eq!(rc, SUBETHA_OK, "mpmc push {i} failed");
    }
    let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
    let mut len = 0usize;
    for i in 0..PEER_ITEMS {
        let rc = unsafe { subetha_mpmc_pop_wait(consumers[0], out.as_mut_ptr(), out.len(), &mut len, 30_000) };
        assert_eq!(rc, SUBETHA_OK, "mpmc pop {i} failed");
        assert_eq!(index_of(&out, len), i, "the peer's items arrive in order");
    }
    wait_for(child, "mpmc");

    for h in producers.into_iter().chain(consumers) {
        assert_eq!(subetha_handle_destroy(h), SUBETHA_OK);
    }
    let mut report = subetha_unlink_report::default();
    let rc = unsafe { subetha_mpmc_unlink(c_prefix.as_ptr(), 2, 2, &mut report) };
    assert_eq!(rc, SUBETHA_OK, "unlink refused {} removal(s)", report.failed);
    assert_eq!(report.removed, 6, "two rings, two producer wakers, two consumer wakers");
}
