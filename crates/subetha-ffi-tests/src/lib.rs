//! The C test suite for the SubEtha C ABI. The suite itself is
//! `c/ctests.c`, compiled by `build.rs`; the entry points below are what
//! `tests/c_abi.rs` calls.

use std::ffi::c_char;

use subetha_ffi::subetha_handle;

// The ABI's symbols come from subetha-ffi; naming the crate makes the
// linker load its library alongside the C objects that reference them.
extern crate subetha_ffi;

/// What a workload in `c/workloads.c` measured; the C struct of the same
/// name, field for field.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default, Debug)]
pub struct subetha_workload_stats {
    /// Requests served, frames received, lines written.
    pub items: u64,
    /// Payload bytes moved.
    pub bytes: u64,
    /// Pushes refused for a full ring, offers dropped.
    pub refusals: u64,
    /// Waits that ended in a timeout and were retried.
    pub retries: u64,
    /// Wall time of the run.
    pub elapsed_ns: u64,
    /// The slowest round trip or wait.
    pub worst_ns: u64,
    /// Round trips summed, for the mean; some workloads carry a count here.
    pub total_ns: u64,
}

unsafe extern "C" {
    /// Run every single-process C test with `scratch_prefix` as the path
    /// prefix for file-backed rings. Returns the number of failed checks,
    /// each of which was printed to stderr as it failed.
    pub fn subetha_ctest_run(scratch_prefix: *const c_char) -> i32;

    /// Attach to the file-backed ring at `prefix` as a consumer and pop
    /// `expect` items, each carrying its own index as a decimal string, in
    /// order, waiting for each. Returns the number of problems found.
    pub fn subetha_ctest_peer(prefix: *const c_char, expect: u32) -> i32;

    /// The same drain against the blocking SPSC ring at `base`.
    pub fn subetha_ctest_peer_spsc(base: *const c_char, expect: u32) -> i32;

    /// Play producer 1 of the two-producer MPSC pool at `prefix`: push
    /// `expect` indexed items, parking when the ring is full.
    pub fn subetha_ctest_peer_mpsc(prefix: *const c_char, expect: u32) -> i32;

    /// Play consumer 1 and then producer 0 of the 2x2 MPMC grid at
    /// `prefix`: drain `expect` indexed items, then push `expect`.
    pub fn subetha_ctest_peer_mpmc(prefix: *const c_char, expect: u32) -> i32;

    /// Produce `expect` indexed items into the Vyukov ring at `path`.
    pub fn subetha_ctest_peer_vyukov(path: *const c_char, expect: u32) -> i32;

    /// Register as a consumer of the broadcast ring at `path` and read
    /// `expect` indexed items in order.
    pub fn subetha_ctest_peer_broadcast(path: *const c_char, expect: u32) -> i32;

    /// Subscribe to the pub/sub ring at `path` with the position kept in
    /// `<path>.pos` and read `expect` indexed items.
    pub fn subetha_ctest_peer_pubsub(path: *const c_char, expect: u32) -> i32;

    /// Attach to the capacity ring under `base` as its consumer and
    /// drain `expect` indexed items.
    pub fn subetha_ctest_peer_capacity(base: *const c_char, expect: u32) -> i32;

    /// Attach to the locale ring under `base`, which the creator moved to
    /// the file locale, as its consumer and drain `expect` indexed items.
    pub fn subetha_ctest_peer_locale(base: *const c_char, expect: u32) -> i32;
    /// Attach to the stack at `path` and pop `expect` indexed elements,
    /// each an index below `expect` and none twice.
    pub fn subetha_ctest_peer_stack(path: *const c_char, expect: u32) -> i32;
    /// Open the deque at `path` as a thief and steal `expect` indexed
    /// elements in order.
    pub fn subetha_ctest_peer_deque(path: *const c_char, expect: u32) -> i32;
    /// Attach to the ring at `prefix` as a producer and push `expect`
    /// indexed items, waiting for room.
    pub fn subetha_ctest_peer_notify(prefix: *const c_char, expect: u32) -> i32;
    /// Open the map at `path`, check the `expect` entries the creator
    /// inserted and insert `expect` more at the indexes above them.
    pub fn subetha_ctest_peer_hashmap(path: *const c_char, expect: u32) -> i32;
    /// Open the arena at `path`, resolve the `expect` values the creator
    /// interned from the offsets their lengths add up to, and intern
    /// `expect` more after them.
    pub fn subetha_ctest_peer_arena(path: *const c_char, expect: u32) -> i32;
    /// Open the vec at `path`, check the `expect` indexed elements the
    /// creator pushed and push `expect` more at the indexes above them.
    pub fn subetha_ctest_peer_vec(path: *const c_char, expect: u32) -> i32;
    /// Open the slab at `path`, check the indexed records in the first
    /// `expect` slots and write the next `expect` slots with theirs.
    pub fn subetha_ctest_peer_slab(path: *const c_char, expect: u32) -> i32;
    /// Open the region at `path`, check the `expect` slots the creator
    /// allocated and allocate `expect` more after them.
    pub fn subetha_ctest_peer_region(path: *const c_char, expect: u32) -> i32;
    /// Add `expect` to the counter at `<path>.u64` one at a time, then
    /// raise the flag at `<path>.flag`.
    pub fn subetha_ctest_peer_atomic(path: *const c_char, expect: u32) -> i32;
    /// Open the list at `path`, walk and pop the `expect` nodes the
    /// creator pushed, then push `expect` of its own.
    pub fn subetha_ctest_peer_list(path: *const c_char, expect: u32) -> i32;
    /// Open the B-tree at `path`, check the `expect` entries the creator
    /// inserted and their order, remove every other one, and insert
    /// `expect` more above them.
    pub fn subetha_ctest_peer_btree(path: *const c_char, expect: u32) -> i32;
    /// Open the cell at `path` and, for each of `expect` rounds, wait for
    /// the creator's value and write it back reversed.
    pub fn subetha_ctest_peer_cell(path: *const c_char, expect: u32) -> i32;
    /// Open the frame region at `path`, check the `expect` blocks the
    /// creator wrote, free every one of them and take every one back.
    pub fn subetha_ctest_peer_frame_region(path: *const c_char, expect: u32) -> i32;
    pub fn subetha_ctest_peer_epoch_barrier(path: *const c_char, expect: u32) -> i32;
    pub fn subetha_ctest_peer_fence_clock(path: *const c_char, expect: u32) -> i32;
    pub fn subetha_ctest_peer_shared_arc(path: *const c_char, expect: u32) -> i32;
    pub fn subetha_ctest_peer_waker(path: *const c_char, expect: u32) -> i32;
    /// Open the epoch table at `path` and hold, in turn, a pin and a
    /// ticket, so the creator can watch each one hold its own reclaim
    /// horizon and published epoch from the other side of the boundary.
    pub fn subetha_ctest_peer_epochs(path: *const c_char, expect: u32) -> i32;
    /// Open the lock at `path` and take the write hold `expect` times,
    /// incrementing the counter at `<path>.count` under each, so the
    /// creator can check that no increment from either side was lost.
    pub fn subetha_ctest_peer_rwlock(path: *const c_char, expect: u32) -> i32;
    /// Open the two-permit semaphore at `path` and take a permit `expect`
    /// times, counting itself in and out of `<path>.live` and raising
    /// `<path>.peak`, so the creator can check the bound was never passed
    /// and that the two really did overlap.
    pub fn subetha_ctest_peer_semaphore(path: *const c_char, expect: u32) -> i32;
    /// Open the condition variable at `path` and play the responder half
    /// of a ping-pong against `<path>.turn` for `expect` rounds, reading
    /// the generation before the counter each time so a notify that
    /// lands in between is not lost.
    pub fn subetha_ctest_peer_condvar(path: *const c_char, expect: u32) -> i32;
    /// Open the lease at `path`, take it under this process's own pid,
    /// write `expect` into every payload byte, and exit still holding
    /// it, so the creator can watch the grace window displace a holder
    /// that is genuinely gone.
    pub fn subetha_ctest_peer_owner_lease(path: *const c_char, expect: u32) -> i32;
    /// Register in the heartbeat table at `<path>.hb`, take leadership
    /// of the election at `<path>.elect`, mark `expect` work units in
    /// flight, beat once, and exit holding all of it, so the creator can
    /// do the recovery a supervisor has to do.
    pub fn subetha_ctest_peer_fleet(path: *const c_char, expect: u32) -> i32;
    /// Wait up to `timeout_ms` on a notifier's native object as an event
    /// loop would: 1 when signaled, 0 on a timeout, -1 on an error.
    pub fn subetha_ctest_wait_native(native: u64, timeout_ms: i32) -> i32;

    /// The request/response server: serves `clients * requests_per_client`
    /// requests on the rings under `base`, then unlinks them.
    pub fn subetha_workload_rr_serve(
        locale: u32,
        base: *const c_char,
        sddl: *const c_char,
        mode: u32,
        clients: u32,
        requests_per_client: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// One request/response client, `requests` round trips.
    pub fn subetha_workload_rr_client(
        locale: u32,
        base: *const c_char,
        sddl: *const c_char,
        mode: u32,
        client: u32,
        requests: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// The fleet host: `rounds` snapshot offers to `workers` workers.
    pub fn subetha_workload_fleet_host(
        locale: u32,
        base: *const c_char,
        mode: u32,
        workers: u32,
        rounds: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Unlink a fleet's rings once the workers have left.
    pub fn subetha_workload_fleet_unlink(locale: u32, base: *const c_char, workers: u32) -> i32;
    /// A fleet worker with index `index`.
    pub fn subetha_workload_fleet_worker(
        locale: u32,
        base: *const c_char,
        mode: u32,
        index: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Explorer `index` on the race ring. `seen` is a map of
    /// `explorer * rounds + round` that records which migrants this
    /// thread adopted, so a caller that comes up short can name the
    /// ones nobody took; pass null to skip it. `slot` receives the
    /// producer slot the explorer published from, which names the ring
    /// its migrants went into.
    pub fn subetha_workload_race_explorer(
        ring: subetha_handle,
        index: u32,
        rounds: u32,
        steps: u32,
        seen: *mut u8,
        seen_len: usize,
        slot: *mut u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// A log producer: `lines` lines of `line_bytes` bytes from index
    /// `first`, `pace_us` apart.
    pub fn subetha_workload_log_producer(
        ring: subetha_handle,
        first: u32,
        lines: u32,
        line_bytes: u32,
        pace_us: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// The log writer, draining until the end slot.
    pub fn subetha_workload_log_writer(ring: subetha_handle, out: *mut subetha_workload_stats) -> i32;
    /// Mark the end of the log once every producer has returned.
    pub fn subetha_workload_log_end(ring: subetha_handle) -> i32;
    /// The command-bus shell, answering `clients * queries` queries.
    pub fn subetha_workload_bus_shell(
        prefix: *const c_char,
        mode: u32,
        clients: u32,
        queries: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// A command-bus client with its own reply ring.
    pub fn subetha_workload_bus_client(
        prefix: *const c_char,
        mode: u32,
        client: u32,
        queries: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// The menu shell, `menus` begin/chunk/show sequences.
    pub fn subetha_workload_menu_shell(path: *const c_char, mode: u32, menus: u32, out: *mut subetha_workload_stats) -> i32;
    /// Unlink a menu ring after the broker has left.
    pub fn subetha_workload_menu_unlink(path: *const c_char) -> i32;
    /// The menu broker, opening with retries and popping without waiting.
    pub fn subetha_workload_menu_broker(path: *const c_char, mode: u32, menus: u32, out: *mut subetha_workload_stats) -> i32;
    /// A pod producer: `items` messages from index `first`, then a sentinel
    /// when `with_sentinel` is non-zero.
    pub fn subetha_workload_pods_producer(
        ring: subetha_handle,
        producer: u32,
        first: u32,
        items: u32,
        with_sentinel: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// A pod consumer in `style` 0, 1 or 2, until `producers` sentinels.
    pub fn subetha_workload_pods_consumer(ring: subetha_handle, style: u32, producers: u32, out: *mut subetha_workload_stats)
        -> i32;
    /// The foreground side of the two-ring mind, `turns` snapshots.
    pub fn subetha_workload_mind_conscious(
        raw: subetha_handle,
        promo: subetha_handle,
        turns: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// The background side of the two-ring mind.
    pub fn subetha_workload_mind_subconscious(raw: subetha_handle, promo: subetha_handle, out: *mut subetha_workload_stats)
        -> i32;
    /// Create a producer's deque at `path`.
    pub fn subetha_workload_deque_create(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32;
    /// Open a producer's deque at `path` as a thief.
    pub fn subetha_workload_deque_open(path: *const c_char, mode: u32, out: *mut subetha_handle) -> i32;
    /// Push `items` packed values, spinning on a full deque.
    pub fn subetha_workload_deque_produce(deque: subetha_handle, producer: u32, items: u32, out: *mut subetha_workload_stats)
        -> i32;
    /// Steal from `count` deques until every one was pushed `items_per_producer`
    /// times and is empty.
    pub fn subetha_workload_deque_consume(
        thieves: *const subetha_handle,
        count: u32,
        items_per_producer: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Lay a blob store out for `writers` writers of `count` blobs each.
    pub fn subetha_workload_blob_create(base: *const c_char, mode: u32, writers: u32, count: u32) -> i32;
    /// Store `count` blobs as writer `writer` of `writers`.
    pub fn subetha_workload_blob_writer(
        base: *const c_char,
        mode: u32,
        writer: u32,
        writers: u32,
        count: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Walk the store on a read-only arena until every distinct blob is seen.
    pub fn subetha_workload_blob_reader(
        base: *const c_char,
        mode: u32,
        writers: u32,
        count: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Lay a content index's lease and generation counter out.
    pub fn subetha_workload_index_create(base: *const c_char, mode: u32) -> i32;
    /// Mark the index finished and report the last generation number used.
    pub fn subetha_workload_index_finish(base: *const c_char, mode: u32, out_last_generation: *mut u32) -> i32;
    /// Build `generations` generations of `entries` records under the lease.
    pub fn subetha_workload_index_writer(
        base: *const c_char,
        mode: u32,
        entries: u32,
        generations: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Check every published generation until the index is finished.
    pub fn subetha_workload_index_reader(base: *const c_char, mode: u32, entries: u32, out: *mut subetha_workload_stats)
        -> i32;
    /// Lay a versioned map and its epoch table out.
    pub fn subetha_workload_mvcc_create(base: *const c_char, mode: u32, writers: u32, keys: u32) -> i32;
    /// `rounds` rounds of updates, removes and re-inserts over one writer's keys.
    pub fn subetha_workload_mvcc_writer(
        base: *const c_char,
        mode: u32,
        writer: u32,
        writers: u32,
        keys: u32,
        rounds: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// `scans` pinned scans over the whole map.
    pub fn subetha_workload_mvcc_reader(
        base: *const c_char,
        mode: u32,
        writers: u32,
        keys: u32,
        scans: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Lay a graph region out with one head page per node.
    pub fn subetha_workload_graph_create(path: *const c_char, mode: u32, nodes: u32, block_count: u32) -> i32;
    /// Append edges to one writer's nodes, then prune each chain's last page.
    pub fn subetha_workload_graph_writer(
        path: *const c_char,
        mode: u32,
        writer: u32,
        writers: u32,
        nodes: u32,
        block_count: u32,
        edges_per_node: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// `walks` walks over every chain while the writers change them.
    pub fn subetha_workload_graph_reader(
        path: *const c_char,
        mode: u32,
        nodes: u32,
        block_count: u32,
        walks: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// One quiet walk that must find exactly `expected_edges`.
    pub fn subetha_workload_graph_verify(
        path: *const c_char,
        mode: u32,
        nodes: u32,
        block_count: u32,
        expected_edges: u64,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Lay a memory store out for `capacity` records.
    pub fn subetha_workload_memory_create(base: *const c_char, mode: u32, capacity: u64) -> i32;
    /// Add `records` records under the write hold.
    pub fn subetha_workload_memory_writer(
        base: *const c_char,
        mode: u32,
        writer: u32,
        writers: u32,
        records: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// `lookups` lookups under the read hold.
    pub fn subetha_workload_memory_reader(
        base: *const c_char,
        mode: u32,
        reader: u32,
        writers: u32,
        records: u32,
        lookups: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// Every record is in both structures and the set finished on the map.
    pub fn subetha_workload_memory_verify(base: *const c_char, mode: u32, writers: u32, records: u32) -> i32;
    /// The sealed receiver, announcing its port and taking every stream to its end.
    pub fn subetha_workload_stream_receiver(
        cert_hex: *const c_char,
        key_hex: *const c_char,
        mode: u32,
        senders: u32,
        streams: u32,
        items: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
    /// One sender process dialing `streams` sealed streams.
    pub fn subetha_workload_stream_sender(
        cert_hex: *const c_char,
        port: u32,
        mode: u32,
        sender: u32,
        streams: u32,
        items: u32,
        out: *mut subetha_workload_stats,
    ) -> i32;
}
