# The C ABI, tier by tier

`subetha-ffi` exposes SubEtha to every language that binds through C. The
surface is organized in the tiers below, numbered in the order they ship.
The order is the commitment; there are no dates. A consumer can see where
the primitive it needs sits.

The ABI carries its own version, reported by `subetha_abi_version()`. Its
major version is 0 through tiers 0 to 3 and becomes 1 once those four tiers
have shipped and been used; from that point the header, the codes, the
handle encoding, and every shipped function signature are frozen. The tier
4 transports keep a 0.x version of their own past that freeze, because the
wire specification work decides what they expose, and the header marks
them.

Both halves of "shipped and been used" are load-bearing. Shipping is what
the table below records; use means a consumer outside this repository has
built against it, which is what finds the awkward signature while it can
still be changed. Every tier in the table below has shipped and none has
been used that way, so the major stays 0.

The minor version is the highest shipped tier plus one, so a consumer
reads it to learn which families it is linked against without probing for
symbols: 1 for tier 0, 2 for tier 1, 3 for tier 2, 4 for tier 3, 5 for
tier 4, 6 for tier 5. The
`abi_version_matches_the_tiers_document` test in
`crates/subetha-ffi/tests/header.rs` holds the constant against this
table, because the version had already fallen four tiers behind before
anything read the two together.

| Tier | Contents | Shipped |
|---|---|---|
| 0 | Foundation: the error model, the handle table, the ABI version query, init and shutdown, panic safety at every boundary, the generated header, packaging, and a C test suite on every gate host, exercised end to end by the adaptive ring | yes |
| 1 | Data plane: the remaining rings and channels, the shared stack and the work-stealing deque, and a pollable notifier an event loop can watch | yes |
| 2 | Shared state: hash map, B-tree, slab, vec, string arena, region, linked list, atomics, cell, frame region | yes |
| 3 | Coordination: locks, condvar, semaphore, owner lease, leader election, heartbeat table, epochs, holder table | yes |
| 4 | Transports: Sens-O-Matic, the TCP and QUIC bridges, and the blocking bridge | yes |
| 5 | Probabilistic and specialist structures: filters, sketches, samplers, rate limiters, histograms, the topology map, the versioned and laned structures | yes |

Five coordination families no tier above names are in the ABI as well:

| family | prefix | kind |
|---|---|---|
| shared fence clock | `subetha_fence_clock_` | 44 |
| epoch barrier | `subetha_epoch_barrier_` | 45 |
| shared value | `subetha_shared_arc_` | 46 |
| lazy value | `subetha_lazy_` | 47 |
| cross-process waker | `subetha_waker_` | 48 |

They are held to everything in "What every tier shares", and the tier
table is not amended for them, because a tier is a unit of shipping
rather than a list of every family.

Three of them reach a Rust type whose shape has no C counterpart, and
each is bridged by a runtime-sized form in `subetha-cxc` rather than by
narrowing the ABI: `SharedArc<T>` by `SharedArcDyn`, and
`SharedOnceCell<T>` with its closure-driven initialization by
`SharedOnceCellDyn` with a claim, a publish and a wait. The
`subetha-pointers` crate, `WakerRing` and `AsyncSpscRing` stay out for
the reasons below.

The `subetha-pointers` crate is not part of the ABI. Its pointer encodings
are a Rust type-system story with no C shape. `WakerRing` and
`AsyncSpscRing` are not in tier 1: they are adapters from a ring to Rust's
`Future` and `Waker` types, and a C caller reaches the same ring through
the waiting forms of the ring's own functions.

## What tier 1 holds

Every ring and channel is reachable through the ABI with a try form and a
waiting form of its operations; the waiting forms park on cross-process
wakers the ABI keeps beside the backing. The adaptive ring carries frames
past the slot, ordering stamps with the three merge modes, the
producer/consumer contract, and an exact-order receiver. The SPSC ring,
the MPSC pool, the MPMC grid, the Vyukov ring, the Lamport pair, the
broadcast ring, pub/sub with anonymous and file-kept positions, the
capacity-adaptive ring with its morphs and prewarm, the locale ring with
migration, the capacity-adaptive broadcast and pub/sub rings, the shared
Treiber stack and the work-stealing deque each have their own prefix. The
stack and the deque take the element size, alignment and a caller-chosen
tag in `subetha_element_layout`; the region stores them and an attach with
another layout is refused. A pollable notifier attaches to an adaptive
ring or an SPSC ring for the calling process: a named FIFO on Unix, a
named manual-reset event on Windows, signaled by every push on the ring
from any process, its native object handed to the event loop. A C peer
crosses a process boundary with each family in the test suite, and
fifteen workloads shaped like the programs that use the library run
through the ABI in every locale and mode their shape allows, ten of them
spawning every role as a child process. Nine are shaped like the programs
that use the rings; six are shaped like the programs that use the shared
state, the coordination and the transports - a blob store, a content
index, a multi-version index, a graph store, a record store and a cluster
stream - each with its writers and readers in processes of their own. The crate
README carries the measured cost of the boundary per family and what one
pass of each workload moved.

## What every tier shares

- Objects are named by generation-checked 64-bit handles; a destroyed or
  forged handle is refused. The table grows without bound.
- Every function returns an integer code; `subetha_strerror` names it and
  `subetha_last_error_detail` carries the specifics.
- A panic inside a call is caught at the boundary and poisons the handle it
  ran on; the process survives.
- Each object is created in strict mode, which starts no thread and writes
  into caller buffers, or managed mode, which runs the background work the
  Rust API runs. The process default is chosen at `subetha_init`.
- Strings are UTF-8. Destroying a handle and unlinking a backing are
  separate calls, since a backing outlives every process that mapped it.
- `subetha_shutdown` is required before the library is unloaded, and
  reports any handle the caller left open.

## What strict mode means per family

Strict mode is one promise everywhere: no thread the caller did not ask for.
What that costs the caller differs by family, and each family's header
comment states its obligation. A ring needs no pumping, since it morphs on
registration and on the pop path. A transport's reader is a pump the caller
drives, and a caller who pumps too slowly loses datagrams in the kernel
buffer.

The ABI reports that loss as far as the host allows, and says which case it
is rather than returning a number that reads the same either way.
`subetha_sens_read_stats` carries two counts, each with its own report:

| Field | What it is | Where it is a number |
|---|---|---|
| `missed` | Datagrams the far end never reported receiving, whatever became of them | A unified sender, which is the only half that learns both counts |
| `kernel_dropped` | Datagrams this host's kernel dropped for a full receive buffer | Where the host counts them per socket |

`missed_report` and `kernel_dropped_report` each take one of three values:
`SUBETHA_DROPS_EXACT`, `SUBETHA_DROPS_OCCURRED` (it happened, this host
will not say how many) or `SUBETHA_DROPS_UNKNOWN`. **A zero paired with
anything but `EXACT` is not a claim that none were lost.** Read the report
before the count.

Three states rather than two because the hosts differ, measured rather than
assumed. Linux carries a per-socket count in the `SO_RXQ_OVFL` ancillary
message. FreeBSD has no such option; `SO_RERROR` turns an overflow into an
error on receive, which says that it happened and not how often, and its
only count is system-wide and shared with every process. Windows moves no
counter at all: its record of a drop is a verbose trace event. macOS is
untested.

`missed` is the portable half and is blind to cause: a slow reader, a full
buffer and a lossy link all reach it the same way. It advances one feedback
window at a time and skips windows too small to trust, so it lags the wire.
Where the two disagree, `kernel_dropped` is the one that names this caller
as the cause.
