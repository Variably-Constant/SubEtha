---
title: SubEtha FFI
weight: 55
---

# The C ABI (`subetha-ffi`)

`subetha-ffi` exposes the memory-mapped primitives to C, C++, Go, Node and
every other language that binds through C: one header, `subetha.h`, and a
library built as a shared library, a static library and a Rust crate. The
surface ships in tiers in the order stated by
[`C_ABI_TIERS.md`](https://github.com/Variably-Constant/SubEtha/blob/main/C_ABI_TIERS.md)
at the repository root. The crate holds tier 0, the runtime, the handle
table, the error model and the adaptive ring end to end, and tier 1's
data plane: every ring and channel, the shared stack and the work-stealing
deque. The ABI is unstable until its major version reaches 1, and
`subetha_abi_version()` reports what a build carries.

This page is the reference. To install the library, link a program
against it and send your first message, start at
[SubEtha from C and C++](../../tutorial/c-and-cpp/).

## The contract

- `subetha_init(default_mode)` comes before every other call and
  `subetha_shutdown()` before the library is unloaded; shutdown reports,
  on stderr and in its return code, any handle the caller left open.
- Every function returns `int32_t`: `SUBETHA_OK`, or a code
  `subetha_strerror()` names, with the specifics of that failure in a
  thread-local buffer `subetha_last_error_detail()` copies out.
- A handle is a 64-bit index and generation. A destroyed or forged handle
  is refused rather than dereferenced, and the table grows without bound.
- A panic inside any call is caught at the boundary: the call returns
  `SUBETHA_E_PANIC`, the handle it ran on is poisoned until destroyed, and
  `subetha_last_panic_message()` has the text.
- Each object is created in strict mode, which starts no thread and
  writes into caller buffers, or managed mode, which runs the background
  work the Rust API runs; the process default is the one chosen at init.
- Destroying a handle and unlinking a backing are separate calls, since a
  backing outlives every process that mapped it.

## A ring in C

```c
#include "subetha.h"
#include <stdio.h>

int main(void) {
    if (subetha_init(SUBETHA_MODE_STRICT) != SUBETHA_OK) return 1;

    subetha_ring_options opt = {SUBETHA_MODE_DEFAULT, 0, 0};
    subetha_handle ring;
    int32_t rc = subetha_ring_create("/tmp/demo-ring", 1, 1, 1024, &opt, &ring);
    if (rc != SUBETHA_OK) {
        char why[256];
        subetha_last_error_detail(why, sizeof why);
        fprintf(stderr, "create: %s (%s)\n", subetha_strerror(rc), why);
        return 1;
    }

    uint32_t producer, consumer;
    subetha_ring_register_producer(ring, &producer);
    subetha_ring_register_consumer(ring, &consumer);

    subetha_ring_try_push(ring, producer, (const uint8_t *)"hello", 5);

    uint8_t slot[SUBETHA_RING_SLOT_BYTES];
    size_t len;
    rc = subetha_ring_pop_wait(ring, consumer, slot, sizeof slot, &len, 1000);
    printf("%d: %s\n", (int)rc, (const char *)slot);

    subetha_handle_destroy(ring);
    subetha_ring_unlink("/tmp/demo-ring", 1, NULL);
    return subetha_shutdown();
}
```

Another process attaches to the same prefix with `subetha_ring_open`, and a
`subetha_ring_pop_wait` there parks on a waker in the mapping that this
process's push wakes. `subetha_ring_create_shm` and `subetha_ring_open_shm`
do the same in named shared memory.

The ring moves fixed slots of `SUBETHA_RING_SLOT_BYTES` bytes. A push
zero-fills the slot past its payload and a pop yields the whole slot, so
the payload's own length does not survive; a caller that needs it carries
it inside the payload. `SUBETHA_RING_PAYLOAD_MAX` is the largest payload
every shape accepts.

## The families

Every family has a try form and a waiting form of its operations; the
waiting forms park on a consumer waker and a producer waker kept beside
the backing (`<prefix>.cwaker.bin` and `<prefix>.pwaker.bin` for a file,
`{name}_cwaker` and `{name}_pwaker` in shared memory), so a wait in one
process is ended by a push or pop in another. Each family has
`read_stats`, `wake_all` and an `unlink` that removes the backing and its
wakers by name and reports what it removed, what was missing and what the
OS refused.

- `subetha_ring_`: the adaptive ring in the anonymous, file and
  shared-memory locales, with frames past the slot, ordering stamps, the
  merge modes and the producer/consumer contract; `subetha_ordered_` is
  the exact-order receiver on a stamped ring.
- `subetha_spsc_`, `subetha_mpsc_`, `subetha_mpmc_`, `subetha_vyukov_`,
  `subetha_lamport_`: the SPSC ring, the MPSC pool, the MPMC grid, the
  Vyukov ring with its stuck-slot scan and heal, and the Lamport pair.
- `subetha_broadcast_`, `subetha_pubsub_` with `subetha_subscriber_`:
  every consumer sees every item; a subscriber's position lives in memory
  or in a file it names.
- `subetha_capacity_`, `subetha_locale_ring_`,
  `subetha_capacity_broadcast_`, `subetha_capacity_pubsub_` with
  `subetha_capacity_subscriber_`: the capacity-adaptive ring with its
  morphs and prewarm, the locale ring with migration between backings,
  and the capacity-adaptive broadcast and pub/sub rings; a second process
  attaches to a capacity or locale ring with `open`.
- `subetha_stack_`, `subetha_deque_`: the shared Treiber stack and the
  work-stealing deque, at an element size the caller declares in
  `subetha_element_layout` together with the element's alignment and a
  tag naming its type; the region stores the layout and refuses an attach
  that states another one.
- `subetha_hashmap_`, `subetha_arena_`: the shared hash map at key and
  value sizes the caller declares, keys hashed and compared as bytes, and
  the string arena, where an intern returns a 64-bit reference packing
  offset and length that resolves to the same bytes from every process.
- `subetha_vec_`, `subetha_slab_`, `subetha_region_`: the shared vec, the
  shared slab and the shared region at an element layout the caller
  declares in `subetha_element_layout`; the region stores the layout and
  refuses an attach that states another one. The vec and the slab each
  have a read-only open for a process that may not write the file; the
  region names a slot by a 32-bit index every process resolves the same
  way.
- `subetha_atomic_u32_`, `subetha_atomic_u64_`, `subetha_atomic_bool_`:
  a counter or flag in a file every process operates on atomically. Each
  operation has a sequentially-consistent form and an `_explicit` form
  taking a `SUBETHA_ORDER_` value, as `<stdatomic.h>` shapes them.
- `subetha_list_`: a doubly-linked list at an element layout the caller
  declares, where a push returns the node's index and a removal from the
  middle costs a splice rather than a walk.
- `subetha_btree_`: an ordered map at key and value sizes the caller
  declares. Keys compare as unsigned bytes, so a caller wanting numeric
  order stores its integers big-endian; one writer changes the shape
  while readers retry through a version.
- `subetha_cell_`: one value at a size the caller declares, recorded in
  the region so a four-byte cell and a `SharedCell<u32>` are the same
  region. A write bumps a version either side of the copy, so a reader
  never sees half of one value and half of another.
- `subetha_frame_region_`: fixed-size blocks for payloads too large to
  travel inside a ring slot. A producer takes a block, writes it and
  sends the index through whatever ring it is using; a consumer reads the
  block and frees it, and any consumer may free any block. The free list
  runs through the blocks themselves, so a free overwrites the first four
  bytes of the one it takes: read the payload before freeing it.
- `subetha_holders_`: the substrate under the epoch table's pins and
  tickets, reached directly. A fixed array of claimable slots, each
  carrying one `uint64_t` of the caller's own meaning and the process
  that claimed it, so `subetha_holders_reap_dead` can free every slot
  whose process is gone. Claim in one step, or reserve and publish when
  the payload depends on state read after the slot is visible.
- `subetha_leader_`: among the processes attached to one file, exactly
  one leads, and the lowest live process id wins. That converges without
  a vote, since process ids are unique on a host. A follower watching for
  a change in leadership polls the election term rather than the process
  id, which can come back to a value it held before.
- `subetha_heartbeat_`: a whole fleet rather than one holder. Each
  process registers a slot and beats into it, and the slot carries the
  process id, the epoch it last beat at, a bitmap of the work units it
  has taken, and a role. A slot nobody holds reads as empty rather than
  as an error, so a supervisor walks every index to find who has gone
  quiet and what work they left behind.
- `subetha_owner_lease_`: one process at a time holds a resource, and a
  holder that dies loses it. A lower process id preempts outright, and a
  heartbeat that has fallen past the grace window marks a holder gone.
  Nothing advances the epoch on its own, so whatever calls
  `subetha_owner_lease_tick_epoch` sets how quickly a stale holder is
  detected, and a holder keeps its claim by beating faster than that.
  A payload of up to 48 bytes travels with the lease, readable and
  writable only by the holder.
- `subetha_condvar_`: park until another process says something changed.
  The predicate stays with the caller, as it does in C. Read
  `subetha_condvar_generation` first, check your own predicate, and hand
  that generation to `subetha_condvar_wait`; the wait returns at once if
  a notify has already moved past it, which is what stops a notify
  landing between the check and the wait from being lost.
- `subetha_semaphore_`: a bounded number of permits any number of
  processes take and give back, for limiting how many of them work on
  something at once. A permit is a `uint64_t` token, and
  `subetha_semaphore_release(sem, token)` returns it. The waiting acquire
  takes a `timeout_ms` and counts itself among the waiters while it
  waits, so a release from any process advances the wakeup generation.
  Three files carry the state, and `subetha_semaphore_unlink` removes all
  three.
- `subetha_rwlock_`: many readers or one writer across processes, with
  writer priority. A hold is a `uint64_t` token, and
  `subetha_rwlock_unlock(lock, token)` gives it back. The waiting
  acquires take a `timeout_ms` and poll while they wait, since a release
  here signals nobody, and a waiting writer registers for the length of
  its wait so readers arriving after it are refused. Destroying the
  lock's handle releases every caller waiting on it.

  A hold, a permit, a pin and a ticket are tokens rather than handles.
  Everything else in the ABI is named by a handle, but these four are
  taken and given back in a loop, and issuing a handle allocates on the
  heap and takes two mutexes while closing one runs a process-wide
  barrier. A token costs a compare-exchange each way.

  What a token keeps from a handle is the refusal. Each slot carries a
  generation that steps on every release, so a token given back twice is
  refused, and so is one from a hold that ended whose slot has since been
  taken - which a bare index could not tell apart. An all-zeroes token,
  which is what an uninitialized variable holds, names nothing.

  Two consequences a caller has to know. A token belongs to the handle
  that issued it: two handles on one lock share the lock, but a token
  taken through one is refused by the other. And destroying a handle
  gives back every hold outstanding on it, because the object holding
  them goes at the same time - otherwise the lock file would say held
  with nothing able to release it.
- `subetha_epochs_`, with `subetha_pin_` and `subetha_ticket_`: a counter
  and the pins held against it, so a scan reads one fixed view of a store
  while writers keep running. A pin and a ticket are handles of their
  own. Destroying a pin releases it; destroying a ticket publishes the
  write it stamped, which is also what `subetha_ticket_publish` does.
  Both keep the table alive, so a caller may destroy the table's handle
  first. The pins live in the mapping, so a reclaimer in one process sees
  a scan running in another and does not free what it is reading.

The `_many` forms of push and pop carry a run of operations under one
handle lookup and one panic guard, over an array the caller already has:
`items` addresses the first element and `stride` is the distance to the
next. A batch stops at the first operation the object refuses and reports
how many it completed.

Three families take a shape of their own, because their item is not a
uniform payload. `subetha_vec_push_back_many` and
`subetha_arena_intern_many` fill an array of indices or references, one
word per item that landed: a vec push and an arena intern each answer
with something, and another process may append between two batches, so a
caller cannot work the indices out from the length it read beforehand.
`subetha_hashmap_insert_many` and `subetha_hashmap_get_many` walk two
strided arrays, since a caller holds keys and values as two columns
rather than as packed pairs.

- `subetha_ring_notifier`, `subetha_spsc_notifier`, `subetha_notifier_`:
  a pollable notifier for the calling process, a named FIFO on Unix or a
  named manual-reset event on Windows, signaled by every push on the ring
  from any process; `native` hands its file descriptor or event `HANDLE`
  to an event loop and `drain` clears a pending signal.

- `subetha_fence_clock_`: a hybrid logical clock per participant and the
  global fence. A participant ticks as it works and merges the clock that
  traveled beside a message, which is what makes the order total across
  processes: a merge takes the later of the two and steps it, so an event
  that caused another compares below it. The fence is the latest clock
  across live slots, so every event any participant has recorded stands
  at or below it. A slot is a plain `uint32_t` index rather than a
  handle, so a participant costs nothing to register and nothing to
  close; a process that dies holding one leaves it registered, and a
  caller reclaims it by reading its `pid` and unregistering it.

- `subetha_shared_arc_`: a region of shared memory kept alive by the
  processes holding it and released when the last one lets go. Ownership
  is a holder table, one slot per holder stamped with the process that
  took it, so a holder whose process dies never releases and its slot is
  reclaimed by probing whether that process is still there.
  `SUBETHA_ARC_UNLINK` removes the backing when the last holder
  releases; `SUBETHA_ARC_KEEP` leaves it for a process attaching later.

  The bytes are the caller's to interpret and to synchronize: the length
  is fixed at create, nothing here says what they mean, and a write is a
  copy with no ordering against another holder's read, so a caller that
  mutates puts an atomic in the region or shares it under a lock. It is
  the same backing layout the Rust `SharedArc<T>` uses and records the
  region's length, so a Rust holder of a `T` and a C holder of
  `sizeof(T)` bytes share one value, and a length that disagrees is
  refused. What neither side can check for the other is the layout
  inside those bytes.

- `subetha_lazy_`: a value produced once across every process that asks
  for it, so a fleet starting together makes one request against
  whatever serves the value rather than one each. `claim` gives the
  right to produce it to a single caller and refuses the rest; the
  winner produces it however it likes and calls `publish`; the others
  `wait`.

  A claim stands across the caller's own code, so a claimant can die
  holding one. The claim carries the process that took it, so `wait`
  answers `SUBETHA_E_LAZY_CLAIMANT_GONE` once that process is gone
  rather than waiting out its deadline for a publish that will never
  come, and `subetha_lazy_reclaim` frees it for the next caller. A claim
  held by a live process is left alone however slow it is being.
  `publish` takes the claiming process id, so a caller that lost the
  claim cannot overwrite the winner's value.

- `subetha_waker_`: parking and waking between processes, down on the
  platform's own futex - `WaitOnAddress` and the hardware monitor on
  Windows, `futex` on Linux, `_umtx_op` on FreeBSD - so a parked thread
  costs nothing until it is woken. A consumer parks at the sequence
  number it is waiting for and a producer that reaches that number wakes
  it; the sequence is the caller's own, a ring's write position or a job
  counter or anything a producer can compare against.

  `park` reserves a slot and returns a token, `wait` blocks on it and
  gives the slot back before returning - so a caller that waits does not
  release - and `release` is for a parker that decides not to wait. A
  full table answers `SUBETHA_E_RING_WAKER_FULL`, which the caller
  handles by spinning on whatever it wanted, which is what it would do
  with no waker at all.

- `subetha_epoch_barrier_`: a rendezvous, so a round of work cannot begin
  anywhere before it has ended everywhere. What it promises is order, not
  duration: the early arriver does not pass until the late one is there,
  and nothing is said about how long either waits. `_wait` waits for
  every live peer and `_wait_quorum` for a number of them, each with a
  `_timeout` form.

  Two things follow from where it gets its peers. It is built from a
  heartbeat handle rather than a path, so it counts peers in the same
  table its caller beats into rather than a second view of the same file.
  And "every live peer" means every peer the table knows about at that
  moment, so a process must have registered its slot before the round it
  means to join - one that registers late is not waited for, and the
  others pass without it. A grace window, `SUBETHA_BARRIER_GRACE_DEFAULT`
  epochs when the caller passes zero, is how long a slot may go unbeaten
  before it stops counting, which is what stops a process that died
  mid-round from stalling the barrier for good.

Tier 4, the transports:

- `subetha_sens_`: the Sens-O-Matic endpoint. The unified sender and
  receiver switch erasure code on measured loss; `subetha_sens_rlc_` and
  `subetha_sens_rs_` halves pin one code and report zero switches because
  they cannot switch. All six halves share one handle kind and one set of
  `send`, `poll`, `flush`, `local_port` and `read_stats` calls. With the
  `tls` feature, `subetha_sens_sender_tls` and `_receiver_tls` seal every
  item, taking DER bytes because the TLS configuration types cannot cross
  the boundary; without it every entry point exists and answers
  `SUBETHA_E_NOT_SUPPORTED` naming the feature. Strict mode blocks on the
  calling thread; managed mode owns a runtime thread. The stats carry two
  loss counts each with a three-state report - `missed`, what the far end
  never reported receiving, and `kernel_dropped`, what this host's kernel
  dropped for a full receive buffer - because the three hosts genuinely
  differ in what they can say, and a zero paired with anything but
  `SUBETHA_DROPS_EXACT` is not a claim that none were lost.
- `subetha_tcp_bridge_`, `subetha_quic_bridge_`: a client or server half
  carrying items between a ring here and a peer there, over TCP or QUIC.
  Each has `run` with a timeout and `read_stats`; the QUIC bridge takes
  its certificate as DER bytes.
- `subetha_blocking_tcp_bridge_`: the same halves over a blocking SPSC ring
  at each end. The client parks on its ring until an item arrives and
  ships what it drains; the server parks when its ring is full rather than
  dropping, so a consumer that stops draining ends the run with an error.
  Behind the `tcp-bridge` feature with the TCP bridge, with the same `run`
  and `read_stats`.
- `subetha_endpoint_`: the virtual endpoint registry. A target is read
  together with the generation it was read at, and `_still_valid` asks
  whether that generation still stands, because Rust's pinned endpoint
  borrows the registry for its lifetime and C cannot honor a borrow. A
  local target binds a locale-ring handle. An unbound id reads as
  `SUBETHA_ENDPOINT_NONE` rather than an error.
- `subetha_qos_`: a quality-of-service policy a sidecar reads on every
  scan and a caller changes while traffic runs. Each field is its own
  atomic, so there is deliberately no call that writes the whole policy
  at once - it would look atomic and would not be. `subetha_qos_read`
  reads five atomics and can return a mixture of two policies if a writer
  runs during it; every field it returns was real, the combination need
  not have been.

Tier 5, the probabilistic and specialist structures:

- `subetha_bloom_`, `subetha_blocked_bloom_`, `subetha_cms_`,
  `subetha_hll_`: a Bloom filter, a cache-line-blocked Bloom filter, a
  count-min sketch and a HyperLogLog. Each has a `_suggest` that sizes it
  from a target and a `_reset`; the sketch and the HLL answer estimates,
  and the HLL's stats carry the precision it was built with.
- `subetha_histogram_`, `subetha_reservoir_`, `subetha_rate_limiter_`,
  `subetha_topology_`: a histogram with `_percentile` and `_bucket_for`,
  a reservoir sampler with `_snapshot`, a rate limiter with `_try_acquire`
  and `_acquire_wait`, and a topology map recording fan-out and fan-in.
- `subetha_versioned_chain_`, `subetha_versioned_map_`,
  `subetha_versioned_slab_`, `subetha_laned_map_`: the epoch-versioned
  structures, where a reader pins an epoch and sweeps or voids one when
  every reader has left it.
- `subetha_bit_vec_`: a shared bit vector. `toggle`, `set` and `clear`
  each answer what the bit was before.
- `subetha_nan_`: NaN-boxed values, and the only family with no handle
  and nothing shared. A value is a `uint64_t` the caller holds; these
  entry points pack and unpack it, so none can fail on a handle, none
  takes a mode, and `subetha_init` is not required. Reading a value as
  the wrong type answers false and leaves the output untouched.
- `subetha_lru_`: an LRU cache at key and value sizes the caller
  declares. `get` leaves recency alone and `get_and_touch` promotes; a
  miss leaves the value buffer exactly as it was.
- `subetha_graph_`: a directed graph at node and edge value sizes the
  caller declares. Out-edges are walked with `first_edge` and
  `next_edge` to `SUBETHA_GRAPH_NIL` rather than returned as a list. An
  edge to an unallocated node is refused, removing an edge from a node
  that does not own it is refused, and node removal is not offered.
- `subetha_tile_`: the versioned time-point tile. Sixteen lanes, each a
  version and a payload of up to `SUBETHA_TILE_MAX_PAYLOAD` bytes; a
  snapshot sees the lanes at or below its version, and
  `subetha_tile_visible_mask` answers all sixteen in one call. Version 0
  is refused on insert.
- `subetha_umbra_`: the content-prefix pointer, sixteen bytes the caller
  holds rather than a handle, resolved against a `subetha_region_` handle.
  A prefix mismatch rules equality out with no dereference; a match means
  resolve and compare, which is why the comparison is `_prefix_eq` and
  not equality.
- `subetha_universal_`: a set that moves between a vector and a hash map
  while it holds data, on the caller's call to `_migrate` and never on
  its own. The stats carry a `stamp` a reader compares across an
  operation: equal before and after means no migration interleaved.
- `subetha_tower_`: the cascade tower at a depth the caller sets by how
  many intermediate levels it supplies. Paths cross as caller-provided
  arrays of exactly `depth` entries, and `subetha_tower_get` checks every
  level of a path against the tower, refusing at the first level that no
  longer agrees rather than resolving whatever value now sits at its end.

`subetha_ring_options` also carries the adaptive ring's frame region
geometry (`frame_block`, `frame_blocks`), so a frame up to a block travels
past the slot from the first push, and `shm_sddl`, the security descriptor
the object's shared-memory regions are created with, which a region mapped
from another Windows session needs.

## Installing for C consumers

```sh
cargo run -p xtask -- ffi-install --prefix /opt/subetha
```

builds the release library and lays out `include/subetha.h`, the shared
library under its versioned name with an unversioned link (`bin/` and an
import library on Windows), the static library, `lib/pkgconfig/subetha.pc`
for `pkg-config --cflags --libs subetha`, and `lib/cmake/subetha/` for
`find_package(subetha)` with the targets `subetha::subetha` (shared) and
`subetha::static`. The crate's `cmake-consumer/` directory is a CMake
project against that package, and `cargo run -p xtask -- ffi-package-gate`
installs, builds and runs it, reports a missing `cmake` or `pkg-config` as
SKIPPED, and compiles the same consumer directly with the host's C
compiler, shared and static, on every host that has one.

## What gates it

`crates/subetha-ffi-tests` compiles a C suite with the host's own compiler,
every warning an error, and runs it from `cargo test` on Windows, Linux
and FreeBSD, including one C peer per family in a second process,
attaching to a backing the test created and draining what it produces
through the file-backed wakers. The committed header and export
definition are regenerated by the crate's tests, and drift in either
fails them. Nine workloads shaped like the programs that use the rings
run through the ABI across processes and threads, in every locale and
mode their shape allows, and six more shaped like the programs that use
the shared state, the coordination and the transports - a blob store, a
content index, a multi-version index, a graph store, a record store and
a cluster stream - run the same way in both modes; all of them repeat
for a soak when `SUBETHA_FFI_SOAK_SECS` is set. `benches/ffi_overhead.rs` measures the
boundary's cost over the direct Rust call on the same object, per family;
the crate README carries the numbers and what one pass of each workload
moved.
