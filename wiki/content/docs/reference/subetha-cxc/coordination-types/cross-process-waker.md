---
title: "Cross-Process Waker"
weight: 50
---

# CrossProcessWaker

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Pattern](https://img.shields.io/badge/pattern-userspace_futex-brightgreen)
![Linux](https://img.shields.io/badge/Linux-SHARED_futex-success)
![Windows](https://img.shields.io/badge/Windows-named_event_park-informational)

Cross-process wake / park primitive: a fixed-size array of waiter
slots stored inside a memory-mapped file (MMF). Each slot is the
state atom that a parked consumer waits on and that a remote
producer can flip via a single direct-syscall wake. The pattern
is the userspace `futex`, ported to MMF substrate so it works
across process boundaries.

> **The "futex slot in MMF" primitive.** Two cooperating processes
> map the same waker file. A consumer reserves a slot, writes its
> wake target into it, and parks via the platform wait syscall.
> A producer in another process publishes data, then scans the
> waker slots and calls the platform wake syscall on every slot
> whose target it just passed. Both sides talk through one shared
> 32-bit atom per slot, and on Windows through the id of the event
> a parked waiter sleeps on.

## Constraints

- **MAX_WAITERS slots per waker**, fixed at construction
  ([`MAX_WAITERS_DEFAULT = 32`](#tuning)); `try_park` returns
  `WakerError::Full` if every slot is in use. Caller's fallback
  is to spin via the underlying primitive's non-blocking surface.
- **Anonymous (`create_anon`)**, **file-backed (`create` / `open`)**,
  or **shmfs-backed (`create_from_shm` / `open_from_shm`)**: same
  byte layout, same protocol.
- **Cross-process wake works on Linux/WSL via shared `futex`**
  (no `FUTEX_PRIVATE_FLAG`) **and on FreeBSD via
  `_umtx_op(UMTX_OP_WAIT_UINT / UMTX_OP_WAKE)`** - the non-private
  umtx ops, whose sleep queues the kernel keys by physical address
  precisely so process-shared synchronization works (per
  `_umtx_op(2)`); proven by the cross-process waker sweep on
  FreeBSD 15 (5/5 runs, parks observed and woken across two
  processes sharing an MMF). **On Windows, a cross-process waiter
  parks on a named auto-reset event** past its
  [monitor tier](#the-monitor-wait-tier); `WaitOnAddress` is
  intra-process only and serves anonymous wakers. macOS 14.4+
  parks through `os_sync_wait_on_address` with its shared flag;
  earlier macOS re-checks every millisecond.

## Storage layout

```mermaid
block-beta
  columns 1
  hdr["CrossProcessWakerHeader - 64 B line: magic u64, capacity u32, parked_mask AtomicU64 (one bit per slot below 64), pad"]
  s0["WakerSlot 0 - 64 B line: state AtomicU32 (FREE / RESERVED / PARKED / WOKEN), target_seq AtomicU64, park_event AtomicU64 (Windows event id, 0 outside a kernel park), pad"]
  s1["WakerSlot 1 - same shape"]
  dots["..."]
  sn["WakerSlot MAX_WAITERS - 1"]
  classDef hdrC fill:#1e3a8a,color:#ffffff
  classDef slotC fill:#0f766e,color:#ffffff
  classDef padC fill:#475569,color:#ffffff
  class hdr hdrC
  class s0,s1,sn slotC
  class dots padC
```

One cache line per slot keeps parkers from false-sharing; the
header's `parked_mask` word is what lets a producer's wake scan
answer the nobody-is-parked case from a single line.

Slot `state` is the wait/wake atomic. The platform wait syscall
takes `&slot.state` plus the expected `PARKED` value; the wake
syscall takes the same address. A Windows cross-process park
sleeps on the event `park_event` names instead.

## Slot states

- **FREE (0)**: unused, available for `try_park`.
- **RESERVED (1)**: caller has claimed the slot via CAS and is
  about to publish its target sequence.
- **PARKED (2)**: consumer has published target + is waiting on
  the platform syscall.
- **WOKEN (3)**: producer or another waker fired; the consumer's
  `wait` returns and stores FREE on its way out, as `release` does
  for a parker that decides not to wait.

## Protocol

### Park (consumer side)

1. CAS some slot's `state` from FREE to RESERVED (linear probe
   from slot 0; the first FREE slot wins).
2. Store the target sequence into `slot.target_seq` (Relaxed; the
   Release store in step 3 publishes it).
3. Release-store `state = PARKED`, and set the slot's bit in the
   header's `parked_mask`.
4. Platform wait on `&slot.state` expecting `PARKED`. Every wait
   first runs the bounded MONITOR tier (below); on budget expiry:
   - Linux: `futex(FUTEX_WAIT)` shared, no `FUTEX_PRIVATE_FLAG`;
     crosses process boundaries.
   - FreeBSD: `_umtx_op(UMTX_OP_WAIT_UINT)`, non-private;
     crosses process boundaries.
   - Windows, anon-backed: `WaitOnAddress(state, &PARKED,
     sizeof(u32), timeout_ms)`; intra-process only per Microsoft
     docs.
   - Windows, file/shm-backed: store the id of the slot's park
     event in `slot.park_event` and re-check `state` (both SeqCst),
     then sleep in `WaitForSingleObject` on the event, re-checking
     `state` at least every 20 ms; store 0 on the way out. The
     event is a named auto-reset event the waker instance creates
     per slot on first use: `Local\subetha_park_<id>` beside a
     file, the region's namespace and SDDL beside a shm region. If
     it cannot be created, the wait stays on the monitor tier and
     says so once on stderr.
   - macOS 14.4+: `os_sync_wait_on_address` (the public futex),
     with `OS_SYNC_WAIT_ON_ADDRESS_SHARED` selected for file /
     shm backings so the wake crosses process boundaries.
   - Other: spin-and-recheck loop.

### The monitor-wait tier

`subetha_cxc::monitor_wait` slots hardware MONITOR-class waiting
between spin and park: `MONITORX`/`MWAITX` (AMD, CPUID
`8000_0001` ECX bit 29), `UMONITOR`/`UMWAIT` (WAITPKG, CPUID
`7.0` ECX bit 5), or `LDAXR`+`WFE` on aarch64 (base ISA - a
remote store to the armed line flips the global exclusive
monitor Exclusive-to-Open, which is the wake event, no `SEV`
required; the budget runs on `CNTVCT_EL0` ticks), probed once
and cached. The waiter arms a
monitor on the slot's cache line and light-sleeps (C0.1) with a
hardware deadline; any store to the line wakes it - so the
producer's state-CAS is the wake, no syscall on either side, and
the wake crosses process boundaries because monitors are
physical-address based. The tier takes a bounded budget
(`SUBETHA_MONITOR_WAIT_CYCLES`, default ~90k cycles = tens of
microseconds) and hands off to the kernel park on expiry;
`SUBETHA_NO_MONITOR_WAIT=1` disables it.

Measured on Windows / Zen+ 2700 by
`examples/monitor_wait_probe.rs` (3,000 wake-latency rounds):
kernel park p50 9,321 ns; integrated waker with the monitor tier
p50 **120 ns** - and the tier's p99 (410 ns raw) is tighter than
a PAUSE spin's (1,624 ns). Hypervisors may hide the CPUID bits
from guests (the FreeBSD VM does), in which case the tier
reports unavailable and the ladder skips straight to the kernel
park.

### Wake (producer side)

`wake_up_to(seq)` first loads the header's `parked_mask` word
(one bit per slot index below 64, maintained at park/release):
a zero mask answers the common nobody-is-parked case from one
cache line instead of touching every slot line. For each
candidate slot whose `state == PARKED` and `target_seq <= seq`:

1. CAS `state` from PARKED to WOKEN (SeqCst).
2. Platform wake syscall on `&slot.state`:
   - Linux: `futex(FUTEX_WAKE)` shared.
   - FreeBSD: `_umtx_op(UMTX_OP_WAKE)` shared.
   - Windows: `WakeByAddressSingle(&state)`; for a file/shm
     backing, first a SeqCst load of `slot.park_event` and, if it
     is not 0, `SetEvent` on the event it names.
   - macOS 14.4+: `os_sync_wake_by_address_any` (shared for
     cross-process backings).

On Windows the CAS, the `park_event` load, and the waiter's store
and re-check are all SeqCst, so either the waiter reads WOKEN before
it sleeps or the producer reads its event id. The 20 ms re-check
bounds a wake that sets no event, such as a producer that dies
between its CAS and its set.

Returns the count woken. Two narrower variants share this scan:
`wake_one_up_to(seq)` wakes at most one qualifying slot (the
Mesa-condvar `notify_one` shape, so the other parked waiters stay
parked) and returns 0 or 1; `wake_all()` wakes every PARKED slot
regardless of `target_seq` (shutdown / drain, so blocked consumers
see a terminate signal).

### Wake-before-park race

Classic futex idiom: between the consumer's "ring empty" check
and `try_park`, a producer can publish and call `wake_up_to`
which finds zero PARKED slots. The fix is the caller's
double-check after `try_park`: re-poll the underlying primitive's
non-blocking surface before actually calling `wait`. If data is
now present, `release` the slot and proceed. The combined
unsafe window is the few nanoseconds between `state = PARKED`
and the re-poll load.

The blocking ring wrappers ([`BlockingSpscRing`]({{< ref
"../rings/blocking-spsc-ring" >}}), [`BlockingMpscRing`]({{< ref
"../rings/blocking-mpsc-ring" >}}), [`BlockingMpmcRing`]({{<
ref "../rings/blocking-mpmc-ring" >}})) implement the
double-check pattern correctly so callers do not need to think
about it.

## Operations

```rust
pub struct CrossProcessWaker { /* fields */ }

impl CrossProcessWaker {
    pub fn create_anon(max_waiters: usize) -> Result<Self, WakerError>;
    pub fn create(path: impl AsRef<Path>, max_waiters: usize) -> Result<Self, WakerError>;
    pub fn open(path: impl AsRef<Path>, expected_max_waiters: usize) -> Result<Self, WakerError>;
    pub fn create_from_shm(shm: ShmFile, max_waiters: usize) -> Result<Self, WakerError>;
    pub fn open_from_shm(shm: ShmFile, expected_max_waiters: usize) -> Result<Self, WakerError>;

    pub fn capacity(&self) -> usize;

    pub fn try_park(&self, target_seq: u64) -> Result<WakerToken, WakerError>;
    pub fn wait(&self, token: WakerToken, timeout: Option<Duration>) -> Result<(), WakerError>;
    pub fn release(&self, token: WakerToken);

    pub fn wake_up_to(&self, seq: u64) -> usize;       // all parked with target_seq <= seq
    pub fn wake_one_up_to(&self, seq: u64) -> usize;   // at most one (condvar notify_one)
    pub fn wake_all(&self) -> usize;                   // every parked slot (drain / shutdown)
}
```

`waker_region_size(capacity)` is a public `const fn` for callers sizing a
`ShmFile` by hand, and `WakerToken::slot_index()` exposes the reserved slot for
instrumentation. `WakerError` has four variants: `Full` (every slot in use -
fall back to spinning), `Timeout` (a `wait` deadline elapsed with no wake),
`LayoutMismatch` (an `open` / `open_from_shm` whose magic or capacity
disagrees), and `IoError(std::io::ErrorKind)`, which is also what `wait`
returns when a Windows kernel park's `WaitForSingleObject` fails; it
implements `Display` + `std::error::Error`.

## Worked example (intra-process)

```rust
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use subetha_cxc::CrossProcessWaker;

let waker = Arc::new(CrossProcessWaker::create_anon(32)?);

// Park a consumer waiting for seq >= 5.
let w_consumer = Arc::clone(&waker);
let consumer = thread::spawn(move || {
    let token = w_consumer.try_park(5).expect("park");
    w_consumer.wait(token, Some(Duration::from_secs(1))).expect("wait");
});

// Producer wakes anything parked at seq <= 7.
thread::sleep(Duration::from_millis(10));
let woken = waker.wake_up_to(7);
assert_eq!(woken, 1);

consumer.join().unwrap();
```

For the cross-process two-binary shape, see the file-backed
worked example pair: `examples/waker_xproc_producer.rs` +
`examples/waker_xproc_consumer.rs` in the `subetha-cxc` crate.

## Tuning

- **`MAX_WAITERS_DEFAULT`** is 32 slots per waker. Each slot is
  64 bytes (cache-line padded), so the header plus 32 slots is
  about 2 KB before mmap rounding. Bump for workloads with more
  simultaneous parkers than producers.
- **Pre-park spin** in the blocking ring wrappers retries the
  non-blocking surface 32 times before calling `try_park`, so
  imminent items skip the kernel round-trip entirely. Increase
  for very bursty producers; decrease for steady-rate producers
  that always park.
- **Linux raw-futex feature**: the `linux-futex-raw` Cargo
  feature exposes the direct `FUTEX_WAIT_BITSET` / `FUTEX_REQUEUE`
  surface for callers that need primitives the portable wrapper
  does not expose.

## E2E proof

- Intra-process (Windows + Linux): `examples/waker_intra_process_e2e.rs`
  ships 50000 items under producer-paced cadence, verifies
  FIFO + asserts `parks > 0` so a regression on the wait path
  would fail the test (parks observed: ~3125 / 50000 calls).
- Cross-process (Linux/WSL): `examples/waker_xproc_producer.rs` +
  `examples/waker_xproc_consumer.rs` ship 50000 items between
  two separate binaries through a file-backed MMF; observe
  ~290 to 320 cross-process parks per run (0.6% of recvs), both
  processes exit `rc=0`.
- Cross-process (Windows): the same pair, and
  `examples/condvar_xproc_waiter.rs` + `condvar_xproc_notifier.rs`,
  pass 10 of 10 runs each with the monitor tier on and off; each
  condvar wait (58 to 95 ms) is a kernel park woken from the other
  process.
- Idle cost (Windows): a thread idle 3 s on a file-backed
  two-process link is charged 0.000 of a processor, against 0.953 to
  1.000 on 0.5.1; whole PowerShell 7.6.6 and 5.1 processes read
  0.000 to 0.003, against 0.994 to 1.008. A sleeping control reads
  0.000 and a spinning one 0.995 to 1.000.
- Wake cost (Windows): a round trip whose wait outlasts the
  monitor budget pays one kernel wake. At a 100 us hold, p50 went
  from 100.8 to 104.8 us and p99 from 101.0 to 110.3-111.6 us
  against 0.5.1; with no hold, p50 stayed at 0.4 to 0.5 us.
- Lost wakes: `a_wait_is_woken_from_another_process` parks a
  peer process through 10,000 rounds, half woken anywhere up to
  twice the monitor budget and half only once the peer sleeps on its
  event, with the 20 ms re-check off, so a lost event shows as a 30 s
  timeout. It passes on Windows with the monitor tier on and off,
  and on Linux.
- Sweep: the intra-process e2e (Windows) and the cross-process
  e2e (Linux) were each run N times back-to-back to rule out
  flakiness.

The Windows figures are from Windows 11 / Ryzen 9 7900X, measured
alongside other work (4.1 to 17.8 of 24 cores busy).

## See also

- Source: `crates/subetha-cxc/src/cross_process_waker.rs` (1565
  lines, 12 unit tests, one of them Windows-only; the platform
  wait/wake ladder lives in the in-file `platform_wait` module, and
  the monitor tier in `crate::monitor_wait`) and
  `crates/subetha-cxc/src/park_event.rs` (439 lines, 6 unit tests;
  the Windows park event).
- [`BlockingSpscRing`]({{< ref "../rings/blocking-spsc-ring" >}}):
  single-producer / single-consumer ring with cross-process
  blocking send / recv.
- [`BlockingMpscRing`]({{< ref "../rings/blocking-mpsc-ring" >}}):
  N producers / 1 consumer.
- [`BlockingMpmcRing`]({{< ref "../rings/blocking-mpmc-ring" >}}):
  N producers / M consumers, round-robin partition.
- [Coordination types overview]({{< ref "_index" >}}).
