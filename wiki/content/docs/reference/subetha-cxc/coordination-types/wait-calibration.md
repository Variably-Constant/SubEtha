---
title: "Wait Calibration"
weight: 51
---

# Wait calibration

Every blocking wait in `subetha-cxc` holds its thread in up to three
phases before it sleeps: a spin, a hardware monitor wait where the
processor has one, and the kernel park. That covers the `Blocking*`
rings, [`BlockingSemaphore`]({{< ref "blocking-semaphore" >}}),
[`BlockingRWLock`]({{< ref "blocking-rw-lock" >}}),
[`SharedCondvar`]({{< ref "shared-condvar" >}}) and every other caller
of [`CrossProcessWaker::wait`]({{< ref "cross-process-waker" >}}).
`subetha_cxc::wait_calibration` measures what each phase costs on the
host it runs on, in the background, and gives each wait the lengths
the ski-rental rule derives from those costs. Until it has measured,
a wait keeps 0.5.1's fixed ladder.

## The plan a wait takes

| Plan | Spin | Monitor wait | Then |
|---|---|---|---|
| Fixed ladder: nothing measured yet, or `SUBETHA_WAIT_CALIBRATION=off` | 32 rounds of a try and a spin hint | 90,000 TSC cycles of `MWAITX` at C1 or `UMWAIT` at C0.1; 28 us of `CNTVCT` ticks on aarch64 | kernel park |
| Calibrated, with a monitor | to S0, in spin hints | from S0 to B | kernel park |
| Calibrated, with no monitor | to B, in spin hints | none | kernel park |

- B, the hold, is the park's wake latency minus the spin's. A waiter
  that has held its processor for B has paid what parking would have
  cost.
- S0 is the monitor's wake latency minus the spin's. Where B is no
  longer than S0 the plan has no monitor phase and spins to B.
- The monitor state is the one with the lower measured wake: `MWAITX`
  at C0 or C1, `UMWAIT` at C0.1, or `WFE` on aarch64.
- Without an invariant TSC a calibrated plan has no monitor phase.
- A monitor arm that returns within its guard floor with the line
  unchanged did not hold. The rest of that wait spins, and
  `monitor_wait::monitor_arms_that_did_not_hold()` counts it. A
  remainder of the monitor phase shorter than the guard floor spins
  without arming, since its timer alone would end it that fast.

`wait_active::active_plan(park_kind)` answers the plan a wait of that
park kind takes now, on the calling thread.

## What is measured

The first blocking wait in a process starts one background thread,
`subetha-wait-calibration`. For each class of core it takes, from the
cache or by measuring:

- the intrinsic facts, once: the counter's rate against the monotonic
  clock, its tick, the cycles one spin hint takes, and for each timed
  monitor state the timer's floor and guard floor, from a sweep of 2^0
  to 2^21 units. A unit is one TSC cycle for `MWAITX` and `UMWAIT`
  alike, as their manuals state. The Ryzen 9 7900X's `MWAITX` timer
  counts two units per cycle, so there each arm ends at half the time
  left and the wait re-arms until its deadline;
- a load band's wake set, the first time the machine's load sits in
  that band: the median wake latency of 101 round trips for a spin and
  for each monitor state at a 20 us gap, and for each park kind, an
  anonymous waker and a file-backed one, at a 500 us gap and then again
  with the wake landing as long after the park as the hold that first
  reading gives. A wait parks only once it has spun to its hold, so the
  second reading is the park it meets, and on a host where a park costs
  less the shorter it lasts it is the smaller of the two.

The thread reads the machine's busy logical processors every second.
The four bands are under a quarter busy, a quarter to a half, a half to
three quarters, and the rest. A wait takes the plan of the band the
last reading named, and a wake set is filed under the band the reading
before it named. Each band is measured at most once per process, and a
band with no wake set in force keeps the fixed ladder.

Each fact is the smallest statistic of five windows run back to back,
kept with the busy logical processors across its window. A fact is not
published when its windows' largest statistic minus their smallest
exceeds their median, or when it lies outside its bound:

| Fact | Bound |
|---|---|
| Counter rate | 100 MHz to 10 GHz; 1 MHz to 10 GHz on aarch64 |
| Spin hint | 1 to 100,000 cycles; 1/10,000 of a tick to 100,000 ticks on aarch64 |
| Wake latencies, the hold B, the tick, the timer floors | 1 cycle to 1 ms at the measured rate |
| Monitor timer | at most 64 units per cycle |

One round trip waits at most 500 ms before its window is abandoned and
the fact goes unpublished.

On a Windows or Linux host whose cores fall into more than one class,
each class is measured with the waiter and the storer pinned to two of
its cores, and a wait takes the plan of the class it is running on.
Elsewhere the calibration's threads run where the operating system puts
them. The path for more than one class is tested with synthetic
topologies only; no hybrid processor has run it.

## The cache

The facts are cached per user, so a later process takes them instead of
measuring:

- Windows: `subetha` in the per-user temporary directory.
- Unix: `$XDG_RUNTIME_DIR/subetha` where it is set, else `subetha-<uid>`
  in the temporary directory, created with mode 0700. A path that is
  not a directory, or belongs to another user, is refused and nothing is
  cached.

Each record is one text file, `wait-<class>-intrinsic.txt` or
`wait-<class>-band-<band>.txt`, written under a temporary name and
renamed into place, so a reader finds a whole record. A record names
its schema, the processor's vendor, family, model and stepping, the
logical processor count and the hypervisor. One that names anything
else, holds a line it does not know, or holds a value outside its bound
reads as absent and is measured again, and the reason is kept in the
report's notes.

## Settings

Read once per process, at the first wait.

| Variable | Effect |
|---|---|
| `SUBETHA_WAIT_CALIBRATION=off` | No thread, no measurement and no cache: every wait takes the fixed ladder. `on`, the default, calibrates. |
| `SUBETHA_NO_MONITOR_WAIT=1` | No monitor phase, and no monitor instruction runs anywhere, calibration included: a calibrated wait spins to B and parks. |
| `SUBETHA_MONITOR_WAIT_CYCLES=<n>` | The monitor phase's length, in the calibrated plan and the fixed ladder alike. |
| `SUBETHA_PRE_PARK_SPIN=<rounds>` | The spin phase's rounds in every wait; `0` parks at once, or monitor-waits first where there is a monitor phase. |

A value these cannot use is ignored and named, with the reason, by
`wait_active::ignored_settings()`.

## Report

`wait_calibration::report()` answers the thread's state, the cache
directory, the last load reading and its band, and for each class its
placement, its intrinsic record, and each band's wake set with the plans
in force for a local and a cross-process park. A record in force says
where it came from, the cache or a measurement in this process, and
what a measurement cost: its wall time and the processor time of its
measuring, waiter and storer threads. The notes name each cache file
that was not read, and why.

## Verified

- The calibration's tests pass on Windows 11 / Ryzen 9 7900X, on Ubuntu
  (a 16-vCPU Zen 3 KVM guest with no monitor family) and on FreeBSD
  (amd64, a guest of the same host). On the Windows host they time real
  spin, park and `MWAITX` wakes.
- The aarch64 paths (the `CNTVCT` counter, the `WFE` arm's plan, the
  scaled bounds) compile for `aarch64-apple-darwin`,
  `aarch64-pc-windows-msvc` and `aarch64-unknown-linux-gnu`, by
  `cargo check` of the library on the Windows host. Nothing has run on
  ARM hardware.
- `examples/wait_verdict.rs` runs the comparison the calibrated wait is
  held to: within 2x of the better of a pure spin and a pure park at
  every gap from 1 to 1024 us in each of three rounds, summed below the
  spin, and summed below the fixed ladder where the ladder has a
  monitor tier. On the Ubuntu guest, in four passes (an anonymous ring
  and a file-backed ring across two processes, each with and without a
  compute thread on the waiter's SMT sibling), every gap decided yes in
  every round, the calibrated wait at 0.78 to 1.83 times the better
  fixed arm; on the FreeBSD guest, in two passes (the anonymous and the
  file-backed ring, no co-runner), 0.80 to 1.90. Neither guest's ladder
  has a monitor tier, so the summed comparison against it is not
  applied there. On the Windows host (Ryzen 9 7900X, `MWAITX`), with
  23.85 to 24.00 of its 24 logical processors busy with other work
  throughout the run, both anonymous-ring passes decided yes at every
  gap in every round, the calibrated wait at 0.42 to 1.93 times the
  better fixed arm; the two file-backed passes had no decided miss and
  three undecided cells, where one round of three crossed 2x (256 and
  1024 us without the co-runner, 16 us with it: 2.18 to 2.65 in those
  rounds against 0.96 to 1.06 in the others). Summed over the six gaps
  the calibrated wait cost 0.47 to 0.81 of the fixed ladder in all four
  passes.
