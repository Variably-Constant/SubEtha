---
weight: 90
---

# Coordination primitives

The largest group in the `subetha-cxc` crate. Primitives whose
shape is not a single coordination pattern (mutex, queue, map)
but a distributed-systems-style protocol: liveness, fan-out,
ordering, scheduling. Each is a piece of the coordination
substrate that an MMF-backed multi-process service composes
from.

## Liveness

### `HeartbeatTable`

Per-process liveness slots in a shared table. Each process
periodically writes its current heartbeat-epoch into its slot;
observers read the slot to detect whether a process is alive.

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, HeartbeatError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, HeartbeatError>;

pub fn register(&self, pid: u32) -> Result<usize, HeartbeatError>;  // claim a slot
pub fn beat(&self, idx: usize);                                     // write this slot's epoch
pub fn snapshot(&self, idx: usize) -> Option<HeartbeatSnapshot>;    // read one slot
pub fn tick_global_epoch(&self) -> u64;
```

`IN_FLIGHT_SLOTS` caps the simultaneous registered processes.
Op kinds use the `liveness` module: `OP_BEAT = 1`,
`OP_REGISTER = 2`, `OP_WAIT = 3`, `OP_TICK_EPOCH = 4`,
`OP_SCAN = 5`.

Canonical doc:
[HEARTBEAT.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/HEARTBEAT.md).

### `FailoverWatchdog`

The detection side of the heartbeat shape. Watches a
`HeartbeatTable` and reports which slots have gone stale;
reclaims work-in-progress that belonged to a dead process by
moving it to a healthy process's slot.

```rust,no_run
pub fn create(
    path: impl AsRef<Path>,
    heartbeat_path: impl AsRef<Path>,
) -> Result<Self, /* ... */>;

pub fn scan(&self) -> Result<ReclaimReport, /* ... */>;
```

`DEFAULT_GRACE_EPOCHS` is the default staleness threshold. The
reclaim is a one-shot move; the watchdog does not run as a
separate thread - the application calls `scan` on its own
cadence.

Canonical doc:
[FAILOVER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/FAILOVER.md).

### `EpochBarrier`

Synchronisation barrier across processes. Each participant
calls `wait(epoch)` and blocks until all other participants have
also called `wait(epoch)`. When the count reaches the registered
participant count, the barrier releases and the epoch advances.

```rust,no_run
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, BarrierError>;
pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, BarrierError>;

pub fn wait(&self, my_epoch: u32) -> Result<(), BarrierError>;
pub fn wait_quorum(&self, my_epoch: u32, quorum: u32) -> Result<(), BarrierError>;
pub fn live_peer_count(&self) -> u32;
pub fn current_epoch(&self) -> u32;
```

`DEFAULT_BARRIER_GRACE_EPOCHS` is the grace period after which
a non-responding participant is dropped. Useful for
phased-execution patterns where every participant has to finish
phase N before any starts phase N+1.

Canonical doc:
[EPOCH_BARRIER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/EPOCH_BARRIER.md).

## Event log

### `EventStateLog`

Append-only event log with fold semantics over two concrete type
parameters, `Event` and `State`. Producers call `emit(event)`;
`drain_and_fold(|state, event| ...)` consumes the pending events and
folds them into the log's materialized `State`, which `read_current`
returns.

```rust,no_run
// EventStateLog<Event: Copy + 'static, State: Copy + 'static>
pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, EventLogError>;

pub fn emit(&self, event: Event) -> Result<(), EventLogError>;
pub fn drain_and_fold<F: FnMut(&mut State, &Event)>(&self, fold: F) -> usize;  // events folded
pub fn read_current(&self) -> State;
pub fn set_state(&self, state: State);
```

The shape matches CQRS read-side projections: events are the
source of truth; each observer projects its own state.

Op kinds use the `event_log` module: `OP_EMIT = 1`,
`OP_DRAIN_FOLD = 2`, `OP_READ_CURRENT = 3`.

Canonical doc:
[EVENT_STATE_LOG.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/EVENT_STATE_LOG.md).

### `SharedVersionedChain`

Append-only versioned history. Each `push` adds a new node with
an incremented version; readers can read the value at any past
version via `read_at`.

Used for time-travel debugging, snapshot isolation, and audit
trails across processes. Op kinds use the `versioned` module:
`OP_PUSH = 1`, `OP_READ_AT = 2`, `OP_CURRENT = 3`,
`OP_VISIBLE_MASK = 4`.

Canonical doc:
[SHARED_VERSIONED_CHAIN.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_VERSIONED_CHAIN.md).

### `SharedTimePointTile`

Time-keyed snapshot slots. Each slot records the value at a
specific time point; lookup returns the value as-of that time.
Used for time-series sampling where the exact sample times are
known in advance.

`TILE_CAP` caps the simultaneous time-point slots. Op kinds use
the `versioned` module.

Canonical doc:
[SHARED_TIME_POINT.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TIME_POINT.md).

## Fan-out and scheduling

### `PriorityFanout`

Multi-priority work distribution. Producers submit work units
with a priority level (0 to `MAX_PRIORITIES - 1`); consumers
drain by descending priority. Used for QoS-aware work
distribution where high-priority work must overtake
low-priority work in flight.

Op kinds use the `priority_fanout` module: `OP_SUBMIT = 1`,
`OP_DRAIN_HIGHEST = 2`, `OP_DRAIN_PRIORITY = 3`.

Canonical doc:
[PRIORITY_FANOUT.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/PRIORITY_FANOUT.md).

### `ProgressTask`

Long-running task progress tracker. The worker calls `advance(n)`
to report progress; observers call `read` to see the current
progress without blocking the worker; `complete` marks the task
finished.

Op kinds use the `progress` module: `OP_ADVANCE = 1`,
`OP_READ = 2`, `OP_COMPLETE = 3`.

Canonical doc:
[PROGRESS_TASK.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/PROGRESS_TASK.md).

### `BackgroundScheduler`

Job scheduler with submitter and collector roles. Producers
submit `Submitter`-typed jobs; the scheduler dispatches them to
workers; results are collected by `ResultCollector`. The whole
thing is one MMF region so the producers, the scheduler, and the
collectors can be in different processes.

Op kinds use the `scheduler` module: `OP_SUBMIT = 1`,
`OP_RECV = 2`, `OP_WATCHDOG_SCAN = 3`.

Canonical doc:
[SCHEDULER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SCHEDULER.md).

## Graph and topology

### `SharedGraph`

Cross-process graph with `add_node`, `add_edge`, `neighbors`,
`remove_edge` operations. Nodes and edges are inline (fixed
payload size); links use `OffsetPtr` so the layout works
two-process.

Op kinds use the `graph` module: `OP_ADD_NODE = 1`,
`OP_ADD_EDGE = 2`, `OP_NEIGHBORS = 3`, `OP_REMOVE_EDGE = 4`.

Canonical doc:
[SHARED_GRAPH.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_GRAPH.md).

### `SharedTopologyMap`

Routing topology and fan-in / fan-out recommendations. Records
observed connection patterns; queries return a routing
recommendation based on historical data. Used by adaptive
routers that switch between fan-out and fan-in modes based on
observed downstream load.

Op kinds use the `topology` module: `OP_RECORD = 1`,
`OP_FAN_OUT = 2`, `OP_FAN_IN = 3`, `OP_RECOMMEND = 4`.

Canonical doc:
[SHARED_TOPOLOGY_MAP.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_TOPOLOGY_MAP.md).

## Other coordination shapes

### `KTowerCascade`

K-level resolver cascade. Lookups walk through K resolver
levels in sequence; the first level to return a hit short-
circuits the rest. Used for layered cache hierarchies (memory
to MMF to disk) where each level has different cost and hit
rate characteristics.

Op kinds use the `cascade` module: `OP_INSERT = 1`,
`OP_GET = 2`.

Canonical doc:
[K_TOWER_CASCADE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/K_TOWER_CASCADE.md).

### `SharedAsyncPointer`

Async lazy resolution. The first caller's
`get_or_fetch(fetch_fn)` triggers the fetch; subsequent callers
either wait for the in-flight fetch or read the resolved value.
Cross-process variant of the in-memory `OnceLock<T>` shape, with
async fetch semantics.

Op kinds use the `async_pointer` module: `OP_GET_OR_FETCH = 1`,
`OP_TRY_GET = 2`.

Canonical doc:
[SHARED_ASYNC_POINTER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_ASYNC_POINTER.md).

### `SharedUniversal`

A generic strategy-adaptive store. Inserts / contains_key /
remove plus an explicit `migrate` op that changes the internal
representation. Used when the application wants the
strategy-adaptation hook the adaptive primitives have, but for
data that lives across processes.

Op kinds use the `universal` module: `OP_INSERT = 1`,
`OP_CONTAINS = 2`, `OP_REMOVE = 3`, `OP_MIGRATE = 4`.

Canonical doc:
[SHARED_UNIVERSAL.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_UNIVERSAL.md).

### `SharedNaNValue` and `SharedNaNTaggedValue`

NaN-boxing for cross-process polymorphism. Stores `i32`, `u32`,
`bool`, or pointer values inside the bit pattern of an IEEE-754
double `NaN`, using the unused mantissa bits. The `TAG_*`
constants identify the contained type; the canonical QNAN bit
pattern is the discriminator.

Used in dynamic languages and JIT runtimes where every value
has the same wire size and the type is discriminated inline.
The cross-process variant lets two processes exchange typed
values without an out-of-band type channel.

Canonical docs:
[SHARED_NAN_VALUE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_NAN_VALUE.md),
[SHARED_NAN_TAGGED_VALUE.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_NAN_TAGGED_VALUE.md).

### `SharedUmbraPointer`

Cross-process variant of the Umbra inline-prefix pointer (16 B
prefix inline, fallback to OffsetPtr beyond). The prefix
compare lets equality and prefix queries short-circuit without
deref; the full value is reached via the offset pointer.

Op kinds use the `umbra_pointer` module: `OP_PREFIX_EQ = 1`,
`OP_RESOLVE = 2`.

Canonical doc:
[SHARED_UMBRA_POINTER.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_UMBRA_POINTER.md).

### Vec and linked list

`SharedVec<T>` and `SharedLinkedList<T>` round out the
collection shapes. The vec is indexed access with `OP_PUSH`,
`OP_POP`, `OP_GET`, `OP_SET`, `OP_LEN`, `OP_CLONE`; the linked
list adds both-end ops via `OP_PUSH_HEAD`, `OP_PUSH_TAIL`,
`OP_POP_HEAD`, `OP_POP_TAIL`.

Canonical docs:
[SHARED_VEC.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_VEC.md),
[SHARED_LINKED_LIST.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_LINKED_LIST.md).

### `SharedRegion`

Sub-allocator inside an MMF. Application code allocates and
frees variable-size chunks inside the region; the region tracks
free space via a free-list. Used as the backing for primitives
that need variable-size payloads but want them all inside one
MMF.

Op kinds use the `region` module: `OP_ALLOCATE = 1`,
`OP_FREE = 2`, `OP_GET = 3`, `OP_SET = 4`.

Canonical doc:
[SHARED_REGION.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/SHARED_REGION.md).

### `PassRegistry`

Cross-process closure registry. Application registers handlers
by `Pass` ID; remote processes look up the handler by ID and
invoke it. The function pointers stay in the registering
process's address space; the remote process gets a registered
handle, not the address.

Used to coordinate cross-process dispatch when the work runs
in a specific process (because of resource ownership, security
context, or hardware affinity) but the request can come from
any process. Canonical doc:
[PASS_REGISTRY.md](https://github.com/Variably-Constant/subetha/blob/main/crates/subetha-cxc/docs/pointers/PASS_REGISTRY.md).

## See also

- [Ownership primitives](ownership.md) - `OwnerLease` /
  `SharedLeaderElection` / `LazyConfig` for resource ownership.
- [Architecture](../../explanation/architecture.md) - where
  these coordination primitives sit in the stack.
- [Role-pair selection](../../how-to/role-pair-selection.md) - the
  liveness, owner/lease, event/observer, and submitter/
  scheduler shapes the coordination primitives fit.
