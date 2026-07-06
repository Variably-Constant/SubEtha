---
title: "Citations and references"
weight: 90
---

# Citations and references

CXC composes algorithms from the published lock-free, probabilistic
data-structure, and distributed-systems literature. This page records
the source of each named algorithm or pattern the codebase uses, the
original publication or canonical specification, and the file under
`crates/` that implements it.

The intent is a single authoritative attribution list: every algorithm
named in source comments has a citation here, and every citation here
points at concrete file paths so the reader can verify the
implementation against the source paper.

## Lock-free queues and stacks

### Vyukov bounded MPMC queue

- **Source**: Dmitry Vyukov, *Bounded MPMC queue*, 1024cores.net, ~2010.
  <https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>
- **Used in**:
  [`crates/subetha-cxc/src/shared_ring.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_ring.rs)
  (`SharedRing`, the MPMC ring),
  and the SPMC sequence-number protocol in
  [`crates/subetha-cxc/src/shared_broadcast_ring.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_broadcast_ring.rs).
- **Role**: producer and consumer coordinate through one atomic
  sequence number per slot, never touching a mutex, at one CAS per
  enqueue / dequeue. The byte layout works the same way whether the
  ring backs a same-process channel, a cross-process MMF, or a
  disk-persistent file.

### Treiber stack

- **Source**: R. Kent Treiber, *Systems Programming: Coping with
  Parallelism*, IBM Almaden Research Center Technical Report RJ 5118,
  April 1986.
- **Used in**:
  [`crates/subetha-cxc/src/shared_treiber_stack.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_treiber_stack.rs)
  (`SharedTreiberStack`, the standalone stack),
  the free-list inside
  [`crates/subetha-cxc/src/shared_handle_table.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_handle_table.rs),
  and the free-list inside
  [`crates/subetha-cxc/src/shared_region.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_region.rs).
- **Role**: lock-free LIFO with ABA defence via a packed
  `(counter, head_index)` u64 head; one CAS per push or pop.

### Bayer-McCreight B-tree

- **Source**: Rudolf Bayer, Edward McCreight, *Organization and
  Maintenance of Large Ordered Indexes*, Acta Informatica 1(3),
  1972, pp. 173-189. <https://doi.org/10.1007/BF00288683>. The
  proactive top-down split/merge variant follows Cormen, Leiserson,
  Rivest, Stein, *Introduction to Algorithms* (B-Trees chapter).
- **Used in**:
  [`crates/subetha-cxc/src/shared_btree_map.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_btree_map.rs).
- **Role**: cross-process ordered map with O(log N) bounds and
  cache-friendly multi-key nodes; a global seqlock makes reads
  lock-free against a single writer, and the contiguous per-node key
  array keeps lookups prefetcher-friendly.

### Chase-Lev work-stealing deque

- **Source**: David Chase and Yossi Lev, *Dynamic Circular Work-Stealing
  Deque*, Proceedings of the 17th Annual ACM Symposium on Parallelism
  in Algorithms and Architectures (SPAA), 2005, pp. 21-28.
  <https://doi.org/10.1145/1073970.1073974>
- **Used in**:
  [`crates/subetha-cxc/src/shared_deque.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_deque.rs)
  (`SharedDeque<T>`).
- **Role**: asymmetric SPMC deque with owner-side `push` and `pop` that
  pay no atomic CAS on the fast path (just Relaxed loads / stores on
  the `bottom` index) and thief-side `steal` that pays exactly one CAS
  on the `top` index. Lifting the protocol into a memory-mapped file
  lets the same primitive serve in-process worker-thread stealing AND
  cross-process work distribution, because the atomics touch physical
  pages whose coherence is identical to the cross-thread case (kernel
  uninvolved on the steal hot path).

### Blumofe-Leiserson work-stealing scheduler

- **Source**: Robert D. Blumofe and Charles E. Leiserson, *Scheduling
  Multithreaded Computations by Work Stealing*, Journal of the ACM
  46(5), 1999.
  <https://doi.org/10.1145/324133.324234>
- **Role**: the architectural foundation SubEtha's `SharedDeque`
  serves. The Chase-Lev deque is the per-worker data structure
  Blumofe-Leiserson schedulers use; this paper proves the time-bound
  and space-bound results that make work-stealing the dominant
  parallel-fork-join scheduling discipline.

### Publication-line cache-line amortization

- **Source**: architectural pattern. The lever - pack K items per
  cache line so one Release-store publishes K items together and one
  cache-line transfer delivers K items to a claiming thief - is a
  direct application of the standard parallel-systems result that
  cross-core coherence cost is per-line, not per-byte. The
  particular three-item-per-64-byte layout used in
  [`SharedDequeKhpd`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_deque_khpd.rs)
  comes from internal research on sub-Chase-Lev MMF deque variants
  benchmarked on Zen+ R7 2700 + EPYC 9B14 Genoa.
- **Used in**:
  [`crates/subetha-cxc/src/shared_deque_khpd.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_deque_khpd.rs)
  (`SharedDequeKhpd`).
- **Role**: a sibling to `SharedDeque` (Chase-Lev) for workloads
  where the producer can batch several items per publication and
  pay one cache-line transfer per batch instead of per item.

### LCRQ + Vyukov bounded MPMC sequence-number protocol (LOH)

- **Source (LCRQ ring)**: Adam Morrison and Yehuda Afek, *Fast
  concurrent queues for x86 processors*, Proceedings of the 18th
  ACM SIGPLAN Symposium on Principles and Practice of Parallel
  Programming (PPoPP), 2013, pp. 103-112.
  <https://doi.org/10.1145/2442516.2442527>
- **Source (per-slot sequence-number protocol)**: Dmitry Vyukov,
  *Bounded MPMC Queue*, 1024cores.net.
  <http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>.
  The same per-slot sequence-number gating (`seq == idx` empty;
  `seq == idx + 1` published; `seq == idx + capacity` consumed) is
  the foundation of `crossbeam-queue::ArrayQueue`, whose
  `array_queue.rs` source header credits Vyukov verbatim.
- **Source (LOH composition)**: architectural pattern. The hybrid -
  a process-private owner-side LIFO that drains a batch into a
  Vyukov-sequenced LCRQ ring via one `tail.fetch_add(N)` plus N
  Release-stores - amortizes the producer-counter atomic over an
  arbitrary batch size while keeping the per-item owner-side push
  at zero atomic cost. Comes from internal research on sub-Chase-
  Lev MMF deque variants.
- **Used in**:
  [`crates/subetha-cxc/src/shared_deque_loh.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_deque_loh.rs)
  (`SharedDequeLoh`).
- **Role**: a sibling to `SharedDeque` (Chase-Lev) and
  `SharedDequeKhpd` (publication-line) for workloads where the
  producer can batch K items per call and pay one
  `tail.fetch_add(K)` plus K Release-stores per call instead of K
  independent Release-stores on `bottom`. The win zone is bursty
  dispatch where the per-burst migration amortizes over many items
  per cache-line bounce.

### Per-thief mailbox + WAITPKG hardware wait

- **Source (ISA)**: Intel WAITPKG extension specification
  (`UMONITOR` / `UMWAIT` / `TPAUSE`). Introduced in the Intel
  64 and IA-32 Architectures Software Developer's Manual, Volume 2;
  shipped on Tremont (2019), Tiger Lake (2020) and later Intel
  cores, and AMD Zen 5 (2024) and later AMD cores.
  <https://www.intel.com/content/www/us/en/developer/articles/technical/software-security-guidance/best-practices/waitpkg-instructions.html>
- **Source (deque shape)**: architectural pattern. Per-thief
  mailbox cache lines eliminate the shared-head CAS contention of
  classical work-stealing deques (Chase-Lev, LCRQ, etc.) by making
  the owner the sole writer to each mailbox and the assigned thief
  the sole reader. Push-based instead of pull-based; the owner picks
  the target by round-robin or by an explicit policy.
- **Used in**:
  [`crates/subetha-cxc/src/shared_deque_urd.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_deque_urd.rs)
  (`SharedDequeUrd`),
  with the runtime wait-strategy dispatch in
  [`crates/subetha-core/src/cpuid.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-core/src/cpuid.rs)
  (`subetha_core::has_waitpkg`).
- **Role**: the right primitive for multi-thief workloads where the
  shared-head CAS becomes the contention bottleneck. URD's per-thief
  mailbox layout gives a zero-CAS-contention steal path; the
  WAITPKG branch additionally lets the thief halt the logical CPU
  via `UMONITOR` + `UMWAIT` so idle thieves do not burn pipeline
  slots polling. Hosts without WAITPKG (most pre-2020 silicon and
  AMD Zen+/2/3/4) fall through to a `PAUSE`-spin path automatically.

## Memory reclamation and consistency

### RCU (read-copy-update) / epoch double-check

- **Source**: Paul E. McKenney and John D. Slingwine, *Read-Copy
  Update: Using Execution History to Solve Concurrency Problems*,
  Proceedings of Parallel and Distributed Computing and Systems
  (PDCS), 1998.
- **Used in**:
  [`crates/subetha-core/src/handshake.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-core/src/handshake.rs)
  (`HandshakeHeader` documents the standard RCU/epoch double-check
  pattern explicitly), and indirectly by `AdaptiveIpc<T>` whenever
  it reads the strategy tag to pick its underlying primitive.
- **Role**: the sidecar swaps a strategy tag while live readers
  continue without taking a lock; readers re-check the tag after the
  operation to detect a mid-flight migration and retry.

### Seqlock

- **Source**: Hans-J. Boehm, *Can Seqlocks Get Along With Programming
  Language Memory Models?*, 4th Workshop on Memory Systems Performance
  and Correctness (MSPC), 2012.
  <https://doi.org/10.1145/2247684.2247688>
  (The pattern itself comes from the Linux kernel; Boehm's paper is
  the formal C++/Rust-style memory-model treatment.)
- **Used in**:
  [`crates/subetha-cxc/src/heartbeat.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/heartbeat.rs)
  (`HeartbeatTable::snapshot`, the SeqLock-protected slot read),
  [`crates/subetha-cxc/src/event_state_log.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/event_state_log.rs)
  (the materialised state cell),
  [`crates/subetha-cxc/src/owner_lease.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/owner_lease.rs)
  (the SeqLock-protected payload cell),
  and the per-slot writes in
  [`crates/subetha-cxc/src/shared_broadcast_ring.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_broadcast_ring.rs).
- **Role**: even-odd version counter around payload writes; multi-word
  reads detect torn updates and retry without locking writers.

## Probabilistic data structures

### Bloom filter

- **Source**: Burton H. Bloom, *Space/Time Trade-offs in Hash Coding
  with Allowable Errors*, Communications of the ACM 13(7), July 1970.
  <https://doi.org/10.1145/362686.362692>
- **Used in**:
  [`crates/subetha-cxc/src/shared_bloom_filter.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_bloom_filter.rs).

### Bloom-filter double-hashing

- **Source**: Adam Kirsch and Michael Mitzenmacher, *Less Hashing,
  Same Performance: Building a Better Bloom Filter*, Random Structures
  and Algorithms 33(2), 2008 (preliminary version at ESA 2006).
  <https://doi.org/10.1002/rsa.20208>
- **Used in**: same file as above. Two seeded FNV-1a hashes form a
  linear-combination basis for the `k` independent indices the Bloom
  filter needs, instead of paying `k` separate hash computations.

### Count-Min Sketch

- **Source**: Graham Cormode and S. Muthukrishnan, *An Improved Data
  Stream Summary: The Count-Min Sketch and Its Applications*,
  Journal of Algorithms 55(1), 2005 (preliminary version at LATIN 2004).
  <https://doi.org/10.1016/j.jalgor.2003.12.001>
- **Used in**:
  [`crates/subetha-cxc/src/shared_count_min_sketch.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_count_min_sketch.rs).

### HyperLogLog

- **Source**: Philippe Flajolet, Eric Fusy, Olivier Gandouet, Frederic
  Meunier, *HyperLogLog: the Analysis of a Near-Optimal Cardinality
  Estimation Algorithm*, AofA Conference on Analysis of Algorithms,
  2007.
- **Used in**:
  [`crates/subetha-cxc/src/shared_hyper_log_log.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_hyper_log_log.rs)
  (2^p AtomicU8 registers, harmonic-mean estimator with bias
  correction).

### Vitter's Algorithm R (reservoir sampling)

- **Source**: Jeffrey Scott Vitter, *Random Sampling with a Reservoir*,
  ACM Transactions on Mathematical Software 11(1), March 1985.
  <https://doi.org/10.1145/3147.3165>
- **Used in**:
  [`crates/subetha-cxc/src/shared_reservoir_sampler.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_reservoir_sampler.rs).

## Hash functions and probing

### FNV-1a

- **Source**: Glenn Fowler, Landon Curt Noll, Phong Vo, *FNV hash
  family specification*, 1991. Public-domain reference at
  <http://www.isthe.com/chongo/tech/comp/fnv/>.
- **Used in**:
  [`crates/subetha-cxc/src/shared_bloom_filter.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_bloom_filter.rs)
  (the double-hashing seed pair),
  and
  [`crates/subetha-cxc/src/shared_hash_map.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_hash_map.rs)
  (`fnv1a_64`, the probe hash for the open-addressed table).

### Linear probing (open-addressed hash table)

- **Source**: Donald E. Knuth, *The Art of Computer Programming, Volume
  3: Sorting and Searching*, section 6.4 (1973; algorithm analysed in
  the 1962-63 working notes that inform this section).
- **Used in**:
  [`crates/subetha-cxc/src/shared_hash_map.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/shared_hash_map.rs)
  (each slot lives in its own cache line; the entire table is a flat
  array in the MMF, so sequential probing dominates probe-variance on
  speculative-prefetch CPUs).

## Operating-system primitives

### POSIX `mmap` with `MAP_SHARED`

- **Source**: IEEE Std 1003.1-2024 (POSIX.1-2024).
  <https://pubs.opengroup.org/onlinepubs/9799919799/functions/mmap.html>
- **Used by**: every `Shared*` type in `subetha-cxc`, through the
  `memmap2` crate.
- **Role**: multiple processes attach the same memory-mapped file and
  observe each other's atomic writes because the atomic operations
  touch physical pages regardless of which page table maps them.

### `CreateFileMapping` (Windows equivalent)

- **Source**: Win32 API documentation.
  <https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-createfilemappingw>
- **Used by**: same set of types as above, on Windows targets.

### `memmap2` crate

- **Source**: <https://crates.io/crates/memmap2>
- **Role**: portable Rust wrapper over POSIX `mmap` and the Windows
  `CreateFileMapping` + `MapViewOfFile` pair. Every cross-process
  primitive in SubEtha goes through this crate.

## Distributed-system patterns

### Closure-id-not-closure-code (closure registry)

- **Source**: Philipp Moritz et al., *Ray: A Distributed Framework for
  Emerging AI Applications*, OSDI 2018, pp. 561-577.
  <https://www.usenix.org/conference/osdi18/presentation/moritz>
  (The same pattern recurs under different names in Akka typed actors
  and Erlang OTP supervised processes.)
- **Used in**:
  [`crates/subetha-cxc/src/pass_registry.rs`](https://github.com/Variably-Constant/SubEtha/blob/main/crates/subetha-cxc/src/pass_registry.rs).
- **Role**: Rust closures cannot safely cross address spaces (function
  pointers are not position-stable; captured environment can hold
  non-portable types). Each process pre-registers a
  `closure_id -> handler` map at startup and the wire carries
  `(closure_id, args_bytes)` records that any peer (including a
  failover target) can execute.

## See also

- [Architecture overview](../architecture/) - where each cited
  algorithm fits into SubEtha's substrate, control plane, and data
  plane.
- [MMF substrate](../mmf-substrate/) - the file-backed memory model
  the cross-process primitives lift the lock-free algorithms into.
- [Frozen-handshake explanation](../frozen-handshake/) - the
  architectural premise the RCU and Seqlock citations support.
