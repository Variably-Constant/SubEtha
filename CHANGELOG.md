# Changelog

All notable changes to SubEtha are recorded here. The five published
crates (`subetha`, `subetha-core`, `subetha-cxc`, `subetha-pointers`,
`subetha-sidecar`) share one version number and release together. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Each version heading links to the commit that cut it.

## [0.2.8] - 2026-09-05

### Added

- `SharedHashMap::insert_if_absent` places a key only when it is absent
  and returns the value found otherwise; `compare_exchange` swaps a value
  only when the stored bytes match, under the slot's seqlock, with
  `KeyAbsent` for a key that has no entry.
- `SharedEpochs` tickets: `begin()` reserves one epoch for a compound
  write across every structure sharing the table, `now()` reports the
  highest epoch every lower ticket has published, and the reclaim horizon
  is bounded by the same value, so entries stamped from one ticket are
  seen by a scan all or none. A ticket whose process died is reported by
  `dead_tickets()`; each structure voids what it stamped there
  (`void_epoch`) and `free_dead_ticket()` releases the slot last.
  `VersionedBTreeMap::insert_at` / `remove_at` stamp a ticket's epoch.
- `SharedVersionedSlab<T, D>`: a `SharedSlab` of epoch-stamped version
  chains sharing an epoch table, so the records an index names stay
  readable at the version a scan pinned (`set_at`, `retire_at`, `get_at`,
  `void_epoch`). A push into a full chain drops only versions no pin can
  reach and refuses with `Pinned` when every version is still reachable.
- `LanedVersionedMap`: one versioned index across n single-writer lanes
  sharing one epoch table, so n statements write at once where one mutex
  used to serialize them. `claim_lane` takes a free lane, `claim_lane_for`
  the lane a key already lives in (`LaneBusy` otherwise), removing a key
  held by another lane reports `KeyInAnotherLane`, and an ordered read
  merges the lanes with a heap trusted up to the smallest lane cursor.
  `HolderTable::try_claim_slot` claims one named slot.
- Counters for every failure the production paths used to discard:
  `send_failures()`, `demux_send_failures()` and `handshake_failures()` on
  the unified endpoint; `sens_rlc::socket_buffer_refusals()` for kernels
  that refuse the 4 MiB socket buffers; `results_dropped()` on the
  scheduler; `permit_release_overflows()` on the semaphore;
  `morph_refusals()` and `mode_refusals()` on the adaptive ring;
  `session_service_errors()` and `trace_sends_skipped()` on the RS bridge;
  `DgramSock::rearm_failures()`; `unattributed_frames()` and
  `malformed_frames()` on the RLC receiver, forwarded by the unified
  receiver as `rlc_unattributed_frames()` / `rlc_malformed_frames()`.
- Benches: a whole-store scan across writer lanes, four writers across
  four lanes against the same four behind a mutex (5.18x ahead), and the
  versioned slab's push and pinned read.

### Changed

- `ShardedUdpSender::send_item` returns `io::Result<()>` and reports
  `BrokenPipe` naming a shard whose thread has ended, where it used to
  drop the item. Callers handle the result.
- The `SharedEpochs` table layout carries a new magic for the ticket
  region; a table laid out by 0.2.7 or earlier is refused rather than read
  with the region missing.
- Every file-backed `create` reports a region that exists at a smaller
  size as its own `LayoutMismatch`, the answer `open` already gave for the
  same file (36 call sites across 35 types).
- The cached clock's readers take the clock directly when its updater
  thread could not be spawned, and the refusal is reported.
- Drain loops in the TCP and QUIC bridges, the locale migration, the
  capacity rings and the event log match `Empty` by name and return or
  record any other pop error; waker waits match their timeout by name and
  propagate the rest; the TCP bridge's closing barrier treats reset,
  abort, broken pipe and unexpected EOF as the receiver's close and
  returns any other read error.

### Fixed

- `mmf_attach::create_or_attach` refuses a non-empty file shorter than the
  requested size at once, with `SizeMismatch` naming the path and both
  sizes; it used to wait five seconds for a creator that would never
  finish. An empty file is still waited on. Reported from PrismLQL.
- The RLC receiver handed any frame without a connection id to the most
  recently admitted session, which rebound its peer address to the
  frame's source without validation, so a one-byte datagram from anyone
  re-pointed that session's control traffic. Such frames are counted and
  reach no session. A `PATH_RESPONSE` answering a migration challenge is
  routed to the window whose id it carries, so an older peer's migration
  validates while a newer peer is live.
- `SharedHashMap` published a claimed slot's hash before its payload,
  letting a prober plant the same key in a second slot; the payload lands
  first and hash 0 marks a slot still forming. A writer whose tombstone
  claim was stolen re-probes instead of claiming the empty slot beyond it,
  and concurrent updates to one key take the seqlock by CAS.
- The scheduler dropped a computed result on a full ring with no trace;
  the LRU cache could lose an entry between its list and its map on a
  refused re-insert; a graph edge or list node whose slot the region
  refused to free was leaked; the RS receiver discarded a session's
  service and feedback errors; an io_uring receive slot that failed to
  re-arm vanished; the net-events watcher could fail to spawn and read as
  a stable path. Each now reports or counts.
- `SharedAsyncPointer`'s speculative races no longer swallow a worker's
  panic: the plain variants continue it on the caller's thread, and the
  resilient variant counts the dead so `AllWorkersDied` is a measured
  statement.
- A listening receiver's RS index never trails its delivery frontier
  (`listen_tls` refuses `CodePolicy::Auto`); that invariant is a debug
  assertion at the branch that relies on it.

### Documentation

- Pages for the versioned slab and the laned map; every new counter on
  its primitive's page; the listener's send-failure accounting; the RLC
  routing rule for frames that name no window.

## [0.2.7] - 2026-08-29

### Added

- `UnifiedSensReceiver::listen_tls(local, cfg, tls, peers)`: a TLS
  listener that is up before any peer dials and serves any number of
  senders, each with its own keys. Handshakes run on the demux thread
  through a per-peer `HandshakeMachine`, and items open with per-peer
  packet numbers bound to the session tag. `handshake_refusals`,
  `handshake_failures`, `tls_preauth_dropped` and `tls_unopened` count
  what is turned away or cannot open. `CodePolicy::Auto` is refused for a
  listener, since the switch boundary is per endpoint.
- `connect_tls_named` asserts a chosen server name,
  `client_config_trusting` accepts a CA root or several leaves, and
  `self_signed_cert_for` issues a certificate for chosen names.

### Fixed

- A single-peer TLS receiver delivered one item and left its sender on a
  full window: opening the session moved the receiver's keys into it, so
  every later sealed frame reached routing still sealed. Receiver and
  session now share the keys.

## [0.2.6] - 2026-08-29

### Fixed

- Windows: an ICMP port-unreachable drawn by a departed peer surfaced as
  `WSAECONNRESET` on the receiver's next receive and displaced a datagram
  belonging to whichever peer was next; one peer delivered 112 of 150
  items under load. `SIO_UDP_CONNRESET` is disabled on every Sens-O-Matic
  socket (`dgram::quiet_icmp_connreset`, a no-op off Windows): 278 socket
  error episodes in one failing run became 0 across 40.
- Tests across the suite wait for the observable event rather than a
  duration, and senders that stop early report what they sent, so a
  loaded host no longer reads as the transport losing data.

### Documentation

- Every lock page states what a holder that dies does to a waiter:
  `SharedRWLock` spins without bound and only `reset` recovers it, the
  blocking primitives point at their bounded variants, and all point at
  `OwnerLease` for a resource whose holder might die.

## [0.2.5] - 2026-08-28

### Added

- `VersionedBTreeMap`: a `SharedBTreeMap` of `Versioned<V>` beside a
  `SharedEpochs`. A delete stamps a death epoch instead of removing the
  entry, a scan pins an epoch and sees what was current then, and an
  entry goes once no pin can reach it. Reinserting a key whose tombstone a
  live pin can still reach is refused as `RebornUnderPin`.
- `HolderTable`: a fixed array of claimable slots stamped with the holding
  process, over memory the caller has mapped; the peer directory's
  consumer slots, the epoch table's pins and a `SharedArc`'s holders are
  the same structure. `SharedEpochs` holds one, byte-for-byte compatible
  with 0.2.4.
- `SharedArc`: a value in shared memory kept alive by the processes
  holding it, bounded by `ShmValue` rather than `Copy` so atomics qualify;
  `LastHolder` decides whether the last release unlinks the backing.
- Benches for all three against the alternatives a caller would reach
  for, with the numbers in the docs including where each primitive loses.

## [0.2.4] - 2026-08-28

### Added

- `ShmFile` takes the creating process's SDDL security descriptor
  (`AdaptiveRing::create_shmfs_secured` / `open_shmfs_secured`), carried
  on `BackingId::Shm` so the peer directory, ordering region and payload
  region take the same one. An unparseable descriptor fails the create;
  the crate supplies no default.
- `SharedSlab`: fixed capacity, a caller-chosen index, and records larger
  than a cache line under the per-slot seqlock. Re-exported from the crate
  root.
- `SharedEpochs`: the counter a writer stamps a superseded version with
  and the pin table a scan holds against reclamation, both in the mapping
  so a reclaimer in another process sees the pins; `reap_dead_pins` frees
  a crashed scanner's slot.
- `SUBETHA_RING_DEBUG` reports every shape transition, the MPMC ownership
  decisions and the table a consumer scanned when it found nothing to
  pop; `ownership_snapshot` exposes ring ownership.

### Fixed

- A single-reader ring shape is served to consumer 0 alone; a second
  consumer registering while the ring was still Mpsc drained the same core
  and took the same items twice. A morph blocked by a stale backlog is held
  as pending and applied by the pop that clears the backlog.
- A pinner reserves its slot before reading the epoch, so a reclaimer's
  horizon cannot pass a pin that appears an instant later.

## [0.2.3] - 2026-08-28

### Added

- `SharedBTreeMap::range`: a bounded ordered query resumed by key, which
  bounds the seqlock retry behind it.

### Documentation

- The `subetha-cxc` crate root states the producer and consumer counts
  each queue primitive supports; `SharedDeque` with two producers and a
  merge-ordered ring consumer without a drainer lease both deadlock by
  contract.

## [0.2.2] - 2026-08-25

### Fixed

- `BackingId::Shm` carries the shm namespace to every region an
  `AdaptiveRing` names (backings, peer directory, per-producer rings,
  ordering region, payload region), so a ring shared between Windows
  sessions reaches all of them rather than each side succeeding against a
  separate copy.

## [0.2.1] - 2026-08-25

### Added

- `ShmNamespace::Machine` names one shared-memory region for every session
  on a Windows host, for a service in session 0 and its interactive
  clients. A create without `SeCreateGlobalPrivilege` fails with the OS
  error rather than falling back.

### Fixed

- Ring takeover fires only on a released claim or a claimed slot whose
  process is gone: a released consumer slot looked like one never claimed,
  and the takeover probe stole rings from a live consumer draining through
  `try_claim_ring`.

## [0.2.0] - 2026-08-23

Breaking: `StringRef` packs its offset in 40 bits and its length in 24
(1 TiB arenas, 16 MiB strings), and an arena region is tagged with the
layout its refs are packed under, so a region written under the previous
layout is refused. A `^0.1` requirement does not resolve to this line.

### Added

- Block-RS telemetry matching the RLC side: per-gate ingest refusal
  counts, armed admission challenges and their age, the block-id ranges
  refused and retransmitted, the last DATA read off the wire, a
  transmit-side probe, the liveness probe's own resend path, the inbound
  demux queue depth and recovery backlog, and counts for feedback,
  challenge answers and code-switch announcements no send path could
  deliver. `EPOCH_OFFSET` is public.
- Both demux readers count datagrams no routing arm claims.
- The RLC receiver counts a session that could not be serviced.
- A bench of the decoder ingest path.

### Changed

- The multi-peer RS receive path drains everything that has arrived per
  poll instead of one datagram, matching the single-peer burst reads; a
  second session no longer slows the receiver below a peer's send rate.

### Fixed

- `SharedVec::push_back` published a slot's index before its payload, so a
  reader could take the zeroed region as a value; a reservation allocator
  publishes `len` only after every earlier reservation has landed.
- Block-RS: a control frame carries no epoch, and the session inferred as
  its owner rebound its peer to the sender, so a completed window acked a
  restarted sender's blocks and delivery stopped one block short for good.
  Only a datagram that names a session may move where that session sends.
- The recovery queue no longer resends datagrams for blocks the peer has
  acked, and a NAK for a block the sender no longer holds is counted and
  named instead of discarded.

## [0.1.12] - 2026-08-22

### Changed

- `SharedStringArena` reports a bad capacity as `LayoutMismatch` from
  `create`, `open` and `reset` alike instead of asserting in two of them.
- `FrameRegion::create` and `SharedAtomicBool::create` obtain an existing
  region rather than truncating it, completing the sweep of every
  file-backed `create` outside the owner-exclusive set.

## [0.1.11] - 2026-08-22

### Added

- `UnifiedSensSender::finish_within(deadline)`; `finish()` keeps its
  default. `demux_stale_for` and `demux_errors` report a reader thread
  that is alive but deaf.

### Changed

- Attach-on-create for `FrameRing`, `SharedUniversal`, `SpscRingCore` and
  the constructors built on it, `DirectFileRing`, `PubSubRing`,
  `SharedVersionedChain` and the hugepage region; `DirectFileRing::open`
  requires the exact file size.

### Fixed

- Linux did not compile from 0.1.6 through 0.1.10: sharing the receiver
  socket behind an `Arc` required `DgramSock: Sync`, which the io_uring
  and wire backends denied. Both hold their state under a mutex.
- `TaskPool::shutdown` set its flag outside the queue lock, so a worker
  between its check and its park missed the notify and `join` never
  returned.
- The unified endpoint fed back to the last speaker only, starving every
  other sender's loss estimate; every peer heard from within the last 30 s
  is fed.
- `net_events` reads the egress MTU on FreeBSD and macOS, so an MTU drop
  registers as a path event there.
- `SharedStringArena::intern_bytes` refuses an offset past `u32::MAX`
  instead of narrowing it into another string's bytes.
- Rustdoc builds with `-Dwarnings` on every platform.

## [0.1.10] - 2026-08-22

### Added

- Telemetry for placing a stall: `tx_probe` / `rlc_tx_probe` (packed, on
  the wire, acked), `route_probe` (active code, RS backlog, items
  accepted, `send_item` entries), `ctrl_probe` / `rlc_ctrl_probe` and
  `session_control` / `rlc_session_control` on both ends of the ARQ
  backchannel, `demux_alive`, `demux_probe`, `queue_seam_probe`,
  `pump_types`, `rlc_path_validations` / `rlc_path_validation_failures`,
  `UnifiedSensSender::local_addr` and `rlc_session_peer`.
  `SUBETHA_SEND_TRACE` and `SUBETHA_WAKE_TRACE` print ledgers on stderr.

### Changed

- Twenty-four more primitives obtain an existing region on `create` and
  truncate only on `reset`: hash map, leader election, once cell, atomics,
  cell, fence clock, handle table, rate limiter, time point, histogram,
  bit vector, Bloom and blocked Bloom filters, count-min sketch,
  HyperLogLog, reservoir sampler, broadcast ring, Treiber stack, vec,
  string arena, topology map, B-tree map and region. The deque family,
  `SharedRing` and the ordering types keep truncate-on-create by design.

### Fixed

- Mesh starvation, three causes: an admission challenge resends at most
  once per 50 ms per candidate, the unified demux thread answers
  `PATH_CHALLENGE` at wire latency, and an ACK goes out when the delivery
  frontier advances and at most once per 10 ms otherwise. Verified 4/4 on
  a four-node quorum gate.
- The reactor parked a future whose ring was already non-empty when items
  were published between its registration and the loop's first read; a
  parked future with a non-empty ring is fired regardless of head history.
  Verified 80/80 under CPU-starved load.

## [0.1.9] - 2026-08-22

### Added

- `session_frontier(cid)` / `rlc_session_frontier(cid)`: one window's
  `(delivered_through, highest_seen)`.

### Changed

- `SharedRWLock::create` attaches to an existing lock and `reset` is the
  explicit truncation, so racing creators cannot clear a writer flag a
  live holder owns. The same contract for `CrossProcessWaker`,
  `OwnerLease`, `HeartbeatTable`, `SharedCondvar`'s generation region and
  `BlockingRWLock`'s wakeup region.

### Fixed

- One session's send error no longer aborts servicing the sessions after
  it or returns the tick's already-decoded items inside an `Err`.

## [0.1.8] - 2026-08-22

### Added

- `SharedRWLock::create_or_open` elects one creator and has the others
  attach; `mmf_attach::create_or_attach` is the shared election, applied
  to the peer directory.
- `live_rlc_sessions`, `live_rs_sessions` and `session_refusals` on the
  unified receiver.

### Fixed

- A latched `ConnectionReset` (Windows, ICMP from a dead peer) ended the
  whole RLC receive drain, so peers queued behind it were never read; the
  drain reads past it, bounded by `MAX_DRAIN_RESETS`.
- The RLC connection id was drawn from the wall clock and repeated (484
  distinct values in 1000 draws); it mixes the invariant TSC, the pid and
  the port.

## [0.1.7] - 2026-08-22

### Fixed

- A unified receiver's block-RS half served one peer, because
  `with_multi_peer` was unreachable through the unified constructor; a
  demux socket routes by session epoch on its own. Found by a consumer's
  four-node quorum gate crossing to RS.

## [0.1.6] - 2026-08-22

Breaking on the wire: the block-RS DATA header grows from 9 to 13 bytes to
carry a session epoch, so 0.1.6 RS endpoints do not interoperate with
earlier ones.

### Added

- A decode window per peer on both codes: RLC sessions keyed by connection
  id and RS sessions keyed by epoch, with `poll_from()` returning
  `(peer, item)` on the RLC, RS and unified receivers. `with_multi_peer()`
  on the RS receiver; the connected single-peer fast path stays the
  default.
- New connection ids are challenged before a window opens; the first id a
  receiver sees is admitted outright, and a TLS receiver refuses a second.
- `with_session_ceiling(max)` bounds live windows and pending challenges,
  and `session_refusals()` counts peers turned away. Unbounded by default.

### Changed

- `AutoIpc::capacity(n)` rounds to a power of two as its docs said;
  `capacity(100).build_channel()` used to panic.

### Fixed

- Block-RS survives a peer restart: every DATA datagram carries the
  session epoch, adoption is gated on a nonce challenge, feedback is
  scoped to its session, the heartbeat announces the epoch, and the
  shared-socket receive path keeps the source address.
- Feedback falls back to an addressed send when a connected socket
  carries a latched `ConnectionReset`.
- `SharedAsyncPointer::get_or_lazy` waits for a publisher mid-initialization
  instead of panicking on the read.

## [0.1.5] - 2026-08-21

### Added

- `take_session_changed()` and `session_adoption_counts()` on the RLC and
  unified receivers.
- The `subetha-e2e` driver binary (workspace member, not published):
  scenarios that cross a real process boundary, including failover of a
  killed child, the ring boundary, flush visibility across processes, a
  scheduler round trip, session restart, a forged session id refused, and
  receiver restart.
- The `iceoryx-bench` feature gates the iceoryx2 bench contender so
  `cargo test` compiles without libclang.

### Fixed

- A restarted RLC peer draws a fresh connection id and was discarded for
  good; the receiver challenges the address with a nonce and adopts the
  session when it returns. The path-validation frames are routed by name
  on the shared socket, which also revives address migration under the
  unified endpoint, inert since that endpoint landed.
- A receiver joining a stream in progress anchors its frontier on the
  first source id observed instead of waiting at zero.

### Removed

- The in-process integration tests the e2e gate subsumes.

## [0.1.4] - 2026-08-02

### Added

- `SharedVec::open_read_only` and `SharedStringArena::open_read_only` map
  without write access; writes return `ReadOnly`.
- `SharedVec::for_each` / `for_each_range` walk the vec without allocating
  a snapshot.

### Documentation

- The `with_frames` path for offset frames above 8 KB, proven across a
  process boundary with matching arguments on both sides.

## [0.1.3] - 2026-07-23

### Fixed

- Offset-class frames (payloads above the inline budget) did not cross a
  process boundary on the file or shm locale, because the payload region
  was a process-private mapping. It rides the ring's own locale
  (`<prefix>.frames.bin` or `<prefix>_frames`) with a CAS-guarded init,
  created lazily on the first offset frame.

## [0.1.2] - 2026-07-23

### Added

- `AdaptiveRing::open_shmfs` attaches to a populated shm region without
  re-laying it out; `create_shmfs` re-initialized the region and wiped
  another process's snapshot.

### Documentation

- The unified Sens-O-Matic endpoint (auto-switching RLC and RS with TLS on
  both codes) positioned as the untrusted-WAN default; the polymorphic
  substrate documented at five axes; README prose passes.

## [0.1.1] - 2026-07-06

### Fixed

- Crate metadata and README URLs point at the renamed `SubEtha` repository
  and Pages site; the published 0.1.0 froze the pre-rename paths. No code
  change.

## [0.1.0] - 2026-07-06

Initial release: MMF-backed cross-process IPC for Rust, one byte layout
serving cross-thread, cross-process, disk-persistent and cross-host
deployment.

- `subetha-cxc`: `Channel<T>`, `AdaptiveIpc<T>`, `AutoIpc`, the MMF
  dispatcher and about forty MMF-backed primitives across the Locale x
  Protocol x Shape x Capacity x Ordering axes (`AdaptiveRing`,
  `OrderingRegion`, the capacity and locale adaptive rings, `PubSubRing`,
  `VirtualEndpoint`, `QosPolicy`, `RingContract`), the Sens-O-Matic
  reliable-UDP transports (block Reed-Solomon and sliding-window RLC, FEC
  plus ARQ, TLS on the RLC code) with the unified auto-switching endpoint,
  the QUIC and TCP bridges and the raw-L2 wire socket behind features,
  and the OS-specific rings (`DirectFileRing`, fd handoff,
  `KernelAsyncRing`, hugepage and superpage regions, vsock).
- `subetha-core`: handshake header, observation ring, marshal trait,
  axis-signature catalog, CPUID helpers.
- `subetha-sidecar`: per-NUMA scan thread, policy, `SidecarBox`,
  `AdaptiveInstance`.
- `subetha-pointers`: Umbra, Bloom, KStep, KTower, SelfDesc, Versioned +
  HLC, Cardinality, CHERI capability and RaspBatch pointer types.
- `subetha`: the umbrella crate re-exporting the four.
- The Hugo wiki and the measured six-platform performance record.

[0.2.8]: https://github.com/Variably-Constant/SubEtha/commit/e5478d8
[0.2.7]: https://github.com/Variably-Constant/SubEtha/commit/91ce2b5
[0.2.6]: https://github.com/Variably-Constant/SubEtha/commit/b38f33a
[0.2.5]: https://github.com/Variably-Constant/SubEtha/commit/cac43e0
[0.2.4]: https://github.com/Variably-Constant/SubEtha/commit/1f6f384
[0.2.3]: https://github.com/Variably-Constant/SubEtha/commit/2154b73
[0.2.2]: https://github.com/Variably-Constant/SubEtha/commit/1e811c9
[0.2.1]: https://github.com/Variably-Constant/SubEtha/commit/1175c79
[0.2.0]: https://github.com/Variably-Constant/SubEtha/commit/152b628
[0.1.12]: https://github.com/Variably-Constant/SubEtha/commit/fdf7ed3
[0.1.11]: https://github.com/Variably-Constant/SubEtha/commit/e90c272
[0.1.10]: https://github.com/Variably-Constant/SubEtha/commit/075cb5c
[0.1.9]: https://github.com/Variably-Constant/SubEtha/commit/0f60be0
[0.1.8]: https://github.com/Variably-Constant/SubEtha/commit/aab02be
[0.1.7]: https://github.com/Variably-Constant/SubEtha/commit/28a5a83
[0.1.6]: https://github.com/Variably-Constant/SubEtha/commit/2e6a84c
[0.1.5]: https://github.com/Variably-Constant/SubEtha/commit/8b1aeb7
[0.1.4]: https://github.com/Variably-Constant/SubEtha/commit/162b8df
[0.1.3]: https://github.com/Variably-Constant/SubEtha/commit/fe82de1
[0.1.2]: https://github.com/Variably-Constant/SubEtha/commit/de5f6d3
[0.1.1]: https://github.com/Variably-Constant/SubEtha/commit/0a5e48e
[0.1.0]: https://github.com/Variably-Constant/SubEtha/commit/9a91f03
