# Changelog

All notable changes to SubEtha are recorded here. The six published
crates (`subetha`, `subetha-core`, `subetha-cxc`, `subetha-ffi`,
`subetha-pointers`, `subetha-sidecar`) share one version number and
release together. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Each version
heading links to the commit that cut it.

## [Unreleased]

### Added

- On Windows a waiter on a file- or shm-backed `CrossProcessWaker`, and
  so every cross-process blocking ring, channel, condvar, lock and
  semaphore wait, sleeps in the kernel once its monitor budget runs out,
  on a named event the waker sets. An idle waiter was charged a
  processor for as long as it waited, 0.953 to 1.008 of a core, and is
  now charged 0.000 to 0.003; a round trip with a 100 microsecond hold
  pays 4.0 microseconds more at p50. Measured on Windows 11 / Ryzen 9
  7900X. A waiter re-checks its slot at least every 20 ms, so a wake
  whose event is never set still ends the wait. The waker's tests pass
  on Windows 11 ARM64 (Azure Cobalt 100) too, with the monitor tier on
  and off.

### Changed

- The PowerShell module is built with PoWerRuSt and cargo-pwrs 0.2.1,
  which compile its PowerShell 7 half against .NET 8 and PowerShell
  7.4's references whichever pwsh does the build, so it imports in
  PowerShell 7.4 and later where 0.5.1 needed 7.6. Built from one commit
  on Windows, Linux and FreeBSD, every file of the module but the native
  is byte-identical. In Windows PowerShell 5.1 the module shares a
  session with another module built by cargo-pwrs 0.2.0 or later that
  declares classes; with one built by 0.1.8 or earlier, whichever of the
  two is imported second fails if it declares classes or enums, as the
  troubleshoot page describes. All 182 Pester tests pass on Windows x64
  in pwsh 7.6.6 and Windows PowerShell 5.1, on Linux x64 in pwsh 7.6.5,
  7.5.11 and 7.4.20, on macOS arm64 in pwsh 7.6.5, and on FreeBSD x64
  in pwsh 7.5.5.

- `Send-SubEthaItem` works out once, when the pipeline begins, whether
  it is sending to a `BroadcastRing`, rather than asking every item's
  target for its type name.

- The workspace crates, the Python package and the PowerShell module
  name Mark Newton as author, and the module's copyright reads the same.

### Fixed

- A blocking ring could lose a wake. A producer stores its ring's head
  and then reads the waker's parked mask; a consumer sets its mask bit
  and then re-checks the ring. x86 and ARM64 both let each of those
  loads complete before the store ahead of it, so the producer could
  find no parked bit while the consumer found no item, and the consumer
  slept beside the item until its timeout. A wake scan now starts with a
  SeqCst fence and a park ends with one. With two processes echoing
  frames over a pair of file-backed `BlockingSpscRing`s, 20 runs of
  2,000 frames each with no hold on Windows 11 / Ryzen 9 7900X, 7 runs
  lost a wake without the fences and none with them. 0.5.1 lost a wake
  in 5 of 15 runs of the same round trip with holds of 0, 100
  microseconds and 1 ms. On the send path the fences cost nothing that
  host resolves: 10.114 ns against 10.118 ns per push-and-pop pair.

- The shared condvar page said a second `open` of the same file in one
  process misses wakes on Windows. No platform's park for a file-backed
  condvar is keyed by virtual address, so it works; it costs a second
  mapping.

- 0.5.1 said the PowerShell module works on FreeBSD x64. Its PowerShell
  7 half was built on PowerShell 7.6 and references .NET 10, so it does
  not import in FreeBSD's PowerShell 7.5.5, nor in PowerShell 7.4.20 or
  7.5.11 on Linux; the FreeBSD figures came from a module built on
  FreeBSD.

- The install, explanation and troubleshoot pages listed the win-x64
  and linux-x64 natives. The published folder has carried osx-arm64 and
  freebsd-x64 as well since 0.5.1.

- `QuicBridgeClient::run` wrote a line of UDP counts to stderr each
  time it finished, so every caller's stderr carried it, the C, Python
  and PowerShell bridges included. It now writes nothing when it
  succeeds.

- `QuicBridgeClient::run` could fail with a lost connection after the
  server had received every item. The server closed the connection as
  soon as it had read the items its header declared, and that close
  could reach the client ahead of the acknowledgment of the client's
  last data. The server now reads the stream to its end, refuses one
  that carries more than its header declared, and waits for the client
  to close; the client closes once its data is acknowledged. Over 200
  rounds of the PowerShell module's round trip on Linux x86-64, 4
  failed that way in PowerShell 7.4.20 and 3 in 7.6.5 before, and none
  in either after. `run` now returns after QUIC's closing period, three
  probe timeouts, which is what delivers the close when the caller's
  runtime ends with the call, as the C API's does. The 200 rounds took
  26 s in place of 4 s in 7.4.20 and 52 s in place of 7 s in 7.6.5.

- The C API's QUIC bridge server could not be made:
  `subetha_quic_bridge_server` bound its endpoint with no runtime
  entered, which quinn refuses, so every call answered
  `SUBETHA_E_RING_IO`. The TCP bridge server could be made but never
  received: it bound its listener on a runtime that ended with the
  constructor and accepted on another. Each server now keeps the runtime
  it bound in and runs every accept on it. The C suite carries 40 items
  end to end through each bridge; on FreeBSD x64 with the transports
  built, it failed 176 checks before and passes after.

- With `wire-locale` on, a shared library linking subetha-cxc failed to
  link on Linux: libxdp-sys's make build compiles libxdp's static
  objects without `-fPIC`, and the link stopped at a `R_X86_64_PC32`
  relocation against `stderr`. subetha-cxc now selects libxdp-sys's cc
  build, which compiles them position-independent. On Linux x86-64,
  `cargo build -p subetha-ffi --features subetha-cxc/wire-locale`
  failed before and links after, and subetha-cxc's 1,502 tests pass
  with the feature on.

- `Sidecar::scan_now` could drain an observation ring while the node's
  own scan thread drained it too, although a ring has one consumer, so
  observations were counted twice or replayed and a slot could be read
  while its producer rewrote it. Each node's scans now take turns. With
  four threads calling `scan_now` beside the node's thread while one
  producer pushed 200,000 observations, the sidecar counted 535,879 to
  627,373 of them before and 200,000 after, on Linux x86-64.

## [0.5.1] - 2026-09-21

### Added

- The Python wheel and the PowerShell module both work on macOS arm64,
  and the module works on FreeBSD x64 as well. Every platform was
  measured rather than assumed: 502 Python tests and 181 Pester tests on
  macOS, and 181 Pester tests on FreeBSD under PowerShell 7.5.5 on
  .NET 9. The module's managed half turns out to be reproducible, since
  building one commit on Windows, Linux and macOS produces a
  byte-identical manifest and shell assembly, so only the native library
  is genuinely per-platform.

  FreeBSD needs two things arranged that SubEtha does not control.
  Building wants `PWRS_TOOLSET=5.3.0`, because the C# toolset the build
  tool fetches by default requires .NET 10 and FreeBSD packages nothing
  past 9. Running the suite wants `$IsLinux` set, because Pester decides
  the platform from three booleans that are all false there and throws
  rather than guessing. Neither is needed to use the module: it imports
  on FreeBSD unaided and all 135 cmdlets work.

### Fixed

- The wheel workflow installs the distribution by its own name. Both of
  its check steps asked pip for `subetha`, which is what the package
  imports as; the distribution is `subetha-ipc`, so pip matched no wheel
  in `dist` and answered "from versions: none" on every platform. The
  workflow had never been run, so a name wrong in both places had never
  failed anything. Running it produced the first Linux wheels SubEtha
  has published.

- The README links the published Guide. Its Documentation section called
  the Guide the canonical reference and then pointed at `wiki/`, the Hugo
  source, so a reader following it got markdown rather than the Guide.
  The site was named nowhere in that file although every crate README
  carries a badge to it. There is now a badge, a line naming it, and
  links to the four sections. GitHub Pages does not appear under a Wiki
  tab, so a README link and the repository's Website field are the only
  ways anyone finds it.

- Three pages stated test counts measured before 0.5.0: the Python
  install page said 494 and 7 skipped for a default build and 500 and 1
  with both bridges, where the measured figures are 502 and 7, and 508
  and 1. Both PowerShell pages said 178 Pester tests, where the measured
  figure is 181 on each of PowerShell 7.6.6, Windows PowerShell 5.1 and
  PowerShell 7.6.5 on Linux. The README badge said 1331, which matched
  no suite, and is now one badge per surface.

- Both PowerShell pages said that identical manifests follow from one
  checkout, which is what `cargo pwrs merge` requires. One checkout is
  not sufficient: the manifest gained fields between releases of the
  tool, so two hosts on different versions of it produce different
  manifests from the same source and the merge is refused. They now say
  the tool version has to match as well, and that `cargo install --list`
  is what settles it, because the newest binary on disk can be the
  oldest version.

## [0.5.0] - 2026-09-20

Three surfaces answer `lag` differently for a consumer that is not
there, which is why this is a minor rather than a patch. Python's
`BroadcastRing.lag` returns `int | None` where it returned `int`,
PowerShell's `Lag` returns `$null` where it returned a number, and
`subetha_broadcast_lag` returns
`SUBETHA_E_BROADCAST_INVALID_CONSUMER` where it returned `SUBETHA_OK`
and wrote a number into `out`. A caller that only ever passes a
registered consumer id sees no change. The `### Changed` entry below
says what the old answers were and why they were wrong.

### Added

- A guide layer for both bindings, under
  `wiki/content/docs/how-to/python/` and
  `wiki/content/docs/how-to/powershell/`: installing, choosing a
  structure, making it fast, what a thread or a runspace does to a
  handle, bridging two hosts, and troubleshooting. An explanation page
  beside each says what the binding is and what crossing it costs. The
  reference says what a call takes; these say which call to reach for,
  and what a symptom means when it was the wrong one.

- A generated reference for both bindings.
  `crates/subetha-pwrs/tools/Export-Reference.ps1` reads the built
  module's own help and type metadata;
  `crates/subetha-py/tools/export_reference.py` pairs the type stub's
  signatures with the doc comments in `crates/subetha-py/src/lib.rs`,
  which is where PyO3 takes `__doc__` from. Between them they cover the
  135 cmdlets, 118 PowerShell classes, 92 Python classes, the enums and
  the module-level values. Neither page is written by hand, so neither
  can describe a surface the build does not have.

- A description on every method of the Python surface. 273 of the 686
  methods carried no doc comment anywhere, so `help()` had nothing to
  say about them. Each was written from the method body and the
  `subetha-cxc` call under it rather than from the name, which is what
  the two disagreeing looks like: `RateLimiter.reset` refills the bucket
  rather than emptying it, `Slab.__len__` and `BitVec.__len__` answer
  the capacity and never change, and `__exit__` on a structure closes
  nothing, flushes nothing and releases no registration.
  `crates/subetha-py/tools/check_docs.py` reads `__doc__` off the
  compiled module for every method the stub declares and exits non-zero
  naming any that is bare; the wheel workflow runs it, so the claim is
  held against the built artifact rather than against the source that
  describes it.

- A values page for each binding, generated by running real scenarios
  against the built module and capturing what they answered
  (`crates/subetha-pwrs/tools/Export-Examples.ps1`,
  `crates/subetha-py/tools/export_values.py`). A type name says what
  comes back and not what it looks like: a ring hands back a whole slot,
  payload then zeros, rather than only what was pushed.

- `SharedBroadcastRing::wait_for_consumers(want, timeout)`, and with it
  `wait_for_consumers` on the Python `BroadcastRing`, `WaitForConsumers`
  on the PowerShell one, and `subetha_broadcast_wait_for_consumers` on
  the C ABI. A consumer registers at the head, so everything published
  before it registered is lost to it and nothing reports that: the push
  succeeds, the ring does not error, and a reader that started late is
  indistinguishable from one that is slow. Publishing only once the
  readers are here is the only thing that closes the window, because
  afterwards there is nothing left to detect. All four answer how many
  consumers are present when the wait ends rather than failing on a
  shortfall, and all four refuse a wait without end.

- `subetha.aio.wait`, which awaits a `Notifier` on an asyncio loop. On
  Unix the loop watches the notifier's descriptor and the wait occupies
  nothing; on Windows the blocking wait runs on a thread from the loop's
  executor, because asyncio has no public way to watch an event handle
  and the default proactor loop does not implement `add_reader` at all.
  What a caller awaits is the same either way. Like `Notifier.wait` it
  does not consume the signal, so a caller that does not drain gets an
  immediate second wake on the signal it already saw.

- `crates/subetha-spec-c`, a second reader of the Sens-O-Matic wire
  specification. It implements section 2's field, the RLC code of
  section 4 and the Reed-Solomon code of section 5 in C, written from
  the document and checked against the frozen vector files. It depends
  on nothing of SubEtha's deliberately: a program built on
  `subetha-ffi` inherits every assumption of the first implementation
  and so cannot disagree with it about anything, and an implementation
  that cannot disagree says nothing about the document. This one
  disagreed with the vectors on the sparsest of the six geometries,
  which is where the specification changes below come from.

- A bench, `crates/subetha-cxc/benches/park_vs_retry.rs`, for where
  parking starts beating spinning on a full ring. `send_blocking` spins
  and then parks; a caller can write a `try_push` retry loop instead,
  and which wins depends on how long the producer waits, which the
  consumer's drain rate sets. Wall time alone flatters the retry arm, so
  it also reports the iterations it burned, and both arms run again with
  every core saturated: that second pass is the one a staged pipeline
  lives in, where the cores a spinning producer occupies are the cores
  its own consumer needs.

- `crates/subetha-cxc/tools/dropped_errors.py`, which separates
  production sites from test ones across the three shapes a dropped
  `Result` takes here: `.ok();` discards it, `unwrap_or_default()`
  substitutes for it, and `if let Ok(` walks past it. A grep for the
  first alone returns 110 hits in `subetha-cxc`, almost all of them a
  test removing a file or joining a thread.

### Changed

- `SENS_O_MATIC_WIRE.md` states the recovery condition for the RLC code
  and says what a conforming decoder must do. Section 4 defined the
  encoder completely and never described a decoder, so the recovery
  condition had to be derived, and the obvious derivation, that exactly
  one symbol of the window is missing, is correct at density 15 and
  wrong at every density below it: a coefficient of zero means the
  symbol does not enter the equation, which the coefficient rule says a
  long way from anything about recovering. A receiver must now derive
  the coefficients as written, refuse a generator it cannot reproduce,
  and recover a symbol when a repair determines it. It may solve the
  linear system that repairs with overlapping windows form, which
  recovers symbols no single repair determines; that is neither required
  nor forbidden, so recovery power is a quality of a receiver rather
  than of the wire.

- The residue code, behind the `residue-fec` feature, no longer divides
  or allocates per column. The moduli are a published constant table, so
  every reciprocal is known at compile time and Barrett reduction with
  `floor(2^34 / m)` replaces the 64-bit hardware divide in Garner's
  inner loop; the per-column `Vec` for the residue list and the second
  one for Garner's digits are gone; and `MODULI[i] mod MODULI[j]`, the
  Horner factor that was constant all along, is a table built once
  rather than a reduction per digit per column. On one host at k=8 with
  a 1 KiB packet, encode fell from 105.02 to 73.59 microseconds and
  recovery from 112.85 to 81.86, against 3 to 4 percent drift on the
  unchanged RLC arm. The allocations rather than the divides were what
  the time was going to: taking out the divide alone bought 9.7 percent.

- `lag` refuses to answer about a consumer that is not there. An index
  past the consumer table and a slot nothing holds both count, because
  `unregister_consumer` clears the active flag and leaves the cursor
  where its last holder stopped, so the distance from the producer to
  that cursor is a reading about nobody and it grows with every push.
  `SharedBroadcastRing::try_lag` answers `None` and `lag` answers
  `u64::MAX`, where `lag` used to answer `0` for an index past the
  table. The Python and PowerShell bindings return `None` where they
  returned an integer. `subetha_broadcast_lag` returns
  `SUBETHA_E_BROADCAST_INVALID_CONSUMER` where it used to write a number
  into `out` and return `SUBETHA_OK`.

## [0.4.1]

### Added

- A PowerShell binding, `crates/subetha-pwrs`, built as the module
  `SubEtha` for PowerShell 7 and Windows PowerShell 5.1. It covers the
  same families as the Python binding: `New-` and `Open-` cmdlets
  obtain a structure, the object each writes carries the operations as
  methods, and `Send-SubEthaItem` and `Receive-SubEthaItem` move items
  through the pipeline. Every cmdlet also answers to a short name with
  the `SE` prefix. Bytes cross as `byte[]` pinned in place, guards come
  back as objects that release on `Release()`, `Dispose()` or
  collection, and the TCP and QUIC bridges are always built in. It is
  built with `cargo pwrs` from the crate. Every cmdlet, class, enum and
  method it exports is reached by a Pester suite, and a gate suite fails
  the run when one is not. `Get-Help` carries a synopsis, a
  description, help on every parameter and one example for each of the
  135 cmdlets, and the reference page names them all.
  All 178 tests pass in pwsh 7.6.6 and Windows PowerShell 5.1 on
  Windows x64 and in pwsh 7.6.5 on Ubuntu 24.04 on Linux x64; the
  native libraries of the two builds fold into one module folder that
  imports on both.

- A gate over the C ABI, `crates/subetha-ffi-tests/tests/surface_gate.rs`.
  It reads the exported functions out of the library's source and fails
  when one is called by no test, no bench and no C program, the way the
  Python and PowerShell bindings are already gated. All 839 exported
  functions are reached; 182 of them were reached by nothing before.

### Fixed

- A region whose builder died while making it can be built again. The
  builder is elected on a marker beside the region rather than by
  creating the region's own name, and the region is built under a
  staging name and published by linking it into place, so that name only
  ever appears over complete bytes. Before this, a process that died
  between winning the election and sizing the file left an empty region
  that every later attacher waited five seconds on and none could
  replace, permanently.

- `SharedHashMap` reports the raw operating-system error before
  `MapError` discards it, so a failure to attach says which system call
  refused and why.

- Three `SharedRateLimiter` tests no longer fail under load. They
  measured their refill window from after the bucket was built, while
  the bucket credits refill from the build itself: an acquire a full
  bucket satisfies spends tokens without restamping the clock, by
  design, so an under-limit caller pays no clock read.

## [0.4.0] - 2026-09-13

### Added

- The Python binding now covers the whole surface a Python caller can
  use. The front door it was missing: `Channel`, a queue between
  processes that can be waited on, `WorkQueue`, work one process owns
  and others steal from, `KvMap`, and `AdaptiveQueue`, which picks its
  shape from the traffic it actually sees rather than from what was
  declared and moves between a ring and a work-stealing deque while
  running.
- Bounded waits throughout. `RWLock.read_for` and `write_for`,
  `Semaphore.acquire_for`, and the channel's `recv_for` and `send_for`
  sleep rather than spin and give up at their deadline. Only the
  bounded forms are exposed: an unbounded park sleeps until something
  signals it, which against a peer that releases without signalling
  would never wake.
- The sensing plane: `LossKind`, `LossBursts`, `Timing`,
  `RoundTripShape`, `Periodicity`, `Capacity`, `Forecast` and
  `PathChanges`. Each is fed measurements and answers what it worked
  out, holds no shared memory and touches no network, and answers
  `None` rather than a number until it has seen enough.
- The value types that ride beside a pointer: `TinyBloom`, a whole
  bloom filter in one machine word whose state crosses as a single
  number, `FineBloom`, `Clock`, which orders two events sharing a
  wall-clock reading, and `CausalClock`, which answers before, after,
  equal, or concurrent.
- `Atomic` gains subtract, the bitwise operations, swap, and compare
  and exchange. It had only add, so a counter could only go up.
- `subetha.aio`, a small pure-Python module letting a coroutine wait
  without blocking its loop. The Rust async engine is not bound and
  cannot be: every entry point takes or returns a Rust future.
- `tests/test_threading.py` runs every call that releases the
  interpreter under eight threads, which is what backs the module's
  declaration that it does not need the interpreter lock.

### Fixed

- `blocking_rw_lock`: `signal_unlock` is public. A caller that hands a
  hold to another language releases it at a moment of its own choosing
  rather than by dropping a guard, and nothing was then telling parked
  waiters the lock had changed hands, so a bounded wait slept to its
  deadline with the lock already free.

## [0.3.3] - 2026-09-13

### Fixed

- The published Python wheel carried instructions the machine that
  built it has and most do not. The build host's own cargo config sets
  `target-cpu=native`, which on that machine means AVX-512, and an
  environment variable replaces such a setting rather than adding to
  it, so nothing on the command line was overriding it. The wheel died
  with an illegal instruction on import on any other x86-64 machine,
  before a single call. Published artifacts now pin
  `-C target-cpu=x86-64`, which costs nothing because the crate
  dispatches its wide kernels at run time, and the wheels workflow sets
  the same so a runner can never inherit a host's choice. The 0.3.2
  wheel on PyPI should be yanked; its source distribution is unaffected
  because it is compiled on the machine that installs it.

## [0.3.2] - 2026-09-12

### Added

- `subetha-py`, the Python binding, bound to the Rust directly rather
  than through the C ABI, covering every family the substrate has: the
  rings including the two that change themselves under traffic, ordered
  delivery through a ring's own stamps, shared state and state with a
  history read through a pin, the locks and the semaphore and the
  condition variable and the owner lease, the probabilistic and
  specialist structures, the quality of service policy, and the
  Sens-O-Matic link that reaches another machine. Measured on the
  AVX-512 Windows host, the same atomic load costs 7.1 ns reached from
  Rust through the C ABI, 30.1 ns through this binding, and 584.4 ns
  through a C shim driven by ctypes, which is what binding the Rust
  directly is worth; batched a thousand at a time it is 1.3 ns an
  operation, and a `Region`'s buffer read whole is 0.3 ns a byte.
  `bench/call_shapes.py` reproduces all of it. It ships as a wheel
  rather than to crates.io.
- Bridges carrying a whole ring to another host over TCP or QUIC, each
  behind a cargo feature and off by default because each brings a
  network stack a process sharing memory on one host does not need. A
  wheel exports `transports`, naming what it was built with, so a name a
  caller cannot find can be told from a feature left out.
- Wheels for free-threaded interpreters. The stable ABI is now a cargo
  feature rather than fixed, since free-threading has none to target
  until 3.15; leaving it out builds for the one interpreter. The module
  declares it does not need the interpreter lock, which is a claim about
  every call in it and is made because `tests/test_threading.py` passes
  on an interpreter that has none: eight threads through the counters,
  both kinds of lock hold, the semaphore's permits, a shared ring, a
  pinned scan running beside writers, and a buffer view held across
  other threads' work. Verified against CPython 3.14.7 free-threaded,
  384 tests with the lock reported off.
- `.github/workflows/python-wheels.yml` builds both kinds of wheel per
  operating system and runs the suite against each one after installing
  it. The free-threaded job additionally asserts that importing the
  module leaves the lock off. Nothing publishes.
- Every SubEtha value held in a `#[pyclass]` is boxed, and a
  compile-time assertion per class enforces it. Python's object
  allocator aligns to sixteen bytes, `HandshakeHeader` is cache-line
  aligned, and 44 of the primitives embed one, so a class holding any of
  them inline compiles and imports and then faults inside its
  constructor on the first aligned store.

### Fixed

- `laned_versioned_map` and `raw_laned_versioned_map`: `sweep` treated a
  lane with nothing to free as a failure of the whole sweep. A lane that
  frees none reports `Full`, and the sweep propagated it, so it
  abandoned every remaining lane and discarded the count of what earlier
  ones had already freed. Any laned map with an idle lane therefore
  swept nothing while reporting it was out of room. Each lane's `Full`
  now means only that this lane freed none; the sweep reports `Full`
  itself only when no lane freed anything, which is the signal an insert
  out of room needs.
- `sens_rlc`: an item too large for a symbol reached the slice copy
  inside `pack_symbol` and panicked, in release as well as debug,
  because the assert guarding it is a debug one. The send path refuses
  it with both sizes named.

### Changed

- `subetha-py`'s adaptive ring is held by a shared handle rather than a
  box, so a network bridge can take one of its own and both name the
  same ring.

- `SENS_O_MATIC_WIRE.md` section 10 gains the literature the two codes
  come from, and two further RFCs, each entry naming the mechanism it
  bears on and where this format departs from it: RFC 9407 (Tetrys)
  against the sender-named coding window of section 4.2, RFC 3393 (IPDV)
  against the unsynchronized clocks section 4.1 differences, and a new
  section 10.3 carrying Reed and Solomon 1960, the Cauchy construction of
  Blömer et al. 1995, random linear coding in Ho et al. 2006, the finite
  sliding window of Wunderlich et al. 2017, tunable sparse coding in Feizi
  et al. 2012, and the SIMD field arithmetic of Plank et al. 2013 that
  section 3.1's log tables have as a conforming alternative.

## [0.3.1] - 2026-09-12

### Changed

- `subetha-ffi`: a handle borrow is a generation check, two acquire
  loads and the epoch guard. The handle table keeps its slots in a
  directory of fixed chunks placed once and never moved, and the object
  behind a slot as a pointer swapped in on insert and out on destroy; the
  two `arc-swap` snapshots a borrow took before, four locked
  read-modify-writes per call for the debt slot each claims and repays,
  are gone, and the epoch guard was already what kept a destroy from
  freeing an object under a call. Per call on the AVX-512 Windows build
  host: 12.4 ns in 0.3.0, 7.1 ns now, the minimum of three sweeps each.
  Every family's measured cost is in the crate README.
- `subetha-ffi`: an entry point takes its handle borrow inside the panic
  guard rather than before it, so a panic while borrowing is caught and
  answered as `SUBETHA_E_PANIC` like any other, and poisoning after a
  panic checks the handle's generation, since the borrow has been given
  back by the time the panic is recorded.

### Added

- `subetha-ffi`: a `test-hooks` cargo feature gating three entry points
  that measure the boundary a layer at a time, the panic guard alone
  (`subetha_test_entry_only`), the borrow alone
  (`subetha_test_borrow_only`) and the atomic family's dispatch alone
  (`subetha_test_atomic_borrow_only`), for `benches/ffi_overhead.rs`,
  which gains those rows and two that pass a memory ordering through.
  No shipped build carries the feature; the header declares the hooks
  under `SUBETHA_TEST_HOOKS`.
- `subetha-ffi`: `benches/ffi_overhead.rs` ships in the published crate,
  since the README describes it. It builds only under `cargo bench`.

### Fixed

- The crate README's "Cost of the boundary" said the difference between
  the direct and the C ABI columns was the same in every row, about
  11 ns; the table under it ran from 9 to 31 ns a call. The text now
  gives the range from the table and says what the ring rows carry
  beyond the boundary. The 0.3.0 entry below repeats the 11 ns figure
  and is superseded by this one.

## [0.3.0] - 2026-09-11

### Added

- `subetha-ffi`, the C ABI: generation-checked 64-bit handles, domain-
  grouped error codes named by `subetha_strerror` with a thread-local
  detail, panics caught at every boundary and reported as
  `SUBETHA_E_PANIC` with the handle poisoned, strict and managed modes per
  object with a process default set at `subetha_init`, and the adaptive
  ring end to end: anonymous, file-backed and shared-memory, registration,
  try and waiting push and pop, stats, wake and unlink. The header
  `include/subetha.h` is generated by cbindgen and committed, `subetha.def`
  lists the exports for the Windows linkers, and drift in either fails
  the test suite.
- The C ABI's data plane: every ring and channel through `subetha-ffi`,
  each with a try and a waiting form of its operations parking on
  cross-process wakers kept beside the backing, stats, wake and unlink.
  Frames past the slot on the adaptive ring (`subetha_ring_send_frame`,
  `subetha_ring_recv_frame`); ordering stamps, merge modes and the
  producer/consumer contract in `subetha_ring_options`, with the stamped
  pop; the SPSC ring (`subetha_spsc_*`); the MPSC pool
  (`subetha_mpsc_*`); the MPMC grid (`subetha_mpmc_*`); the Vyukov ring
  with its stuck-slot scan and heal (`subetha_vyukov_*`); the Lamport
  pair (`subetha_lamport_*`); the broadcast ring (`subetha_broadcast_*`);
  pub/sub with anonymous and file-kept subscriber positions
  (`subetha_pubsub_*`, `subetha_subscriber_*`); the capacity-adaptive
  ring with morph, compound morph and prewarm (`subetha_capacity_*`); the
  locale ring with migrate and the managed request
  (`subetha_locale_ring_*`); the capacity-adaptive broadcast and pub/sub
  rings (`subetha_capacity_broadcast_*`, `subetha_capacity_pubsub_*`,
  `subetha_capacity_subscriber_*`); the exact-order receiver on a stamped
  ring (`subetha_ring_ordered_receiver`, `subetha_ordered_*`); the shared
  Treiber stack (`subetha_stack_*`) and the work-stealing deque
  (`subetha_deque_*`), both at an element size the caller declares in
  `subetha_element_layout`, whose tag and alignment are stored in the
  region and checked on every attach. Handle kinds 2 through 20, codes
  113 through 117.
- `subetha_capacity_open` and `subetha_locale_ring_open` attach a second
  process to a capacity or locale ring without re-creating its backings,
  which on Windows refuses a file another process has mapped. In
  `subetha-cxc`, `CapacityAdaptiveRing::open` and
  `LocaleAdaptiveRing::open`.
- The pollable notifier: `subetha_ring_notifier` and `subetha_spsc_notifier`
  attach, for the calling process, a named FIFO on Unix or a named
  manual-reset event on Windows that every push on the ring from any
  process signals; `subetha_notifier_native` hands out the file descriptor
  or event `HANDLE` for an event loop, `subetha_notifier_drain` clears a
  pending signal, `subetha_notifier_wait` and `subetha_notifier_is_signaled`
  serve a caller without a loop. Handle kind 21. In `subetha-cxc`,
  `cross_process_notifier::{NotifierSet, Notifier}` with a 64-byte record
  beside the ring's backing.
- The C ABI's shared state begins: the shared hash map (`subetha_hashmap_*`,
  handle kind 22) at key and value sizes the caller declares, with
  insert, insert-if-absent, get, contains, remove, compare-exchange, a
  cursor walk, compact, clear, stats, flush and unlink; codes 118
  `SUBETHA_E_MAP_FULL` and 119 `SUBETHA_E_MAP_KEY_ABSENT`. In
  `subetha-cxc`, `RawHashMap`, the same table at run-time sizes, sharing
  the typed map's layout and protocol.
- The shared string arena (`subetha_arena_*`, handle kind 23): intern, get
  by copy and view in place through the 64-bit reference an intern
  returns, the reference helpers, a read-only open, clear, stats, flush
  and unlink; codes 120 `SUBETHA_E_ARENA_FULL`, 121
  `SUBETHA_E_ARENA_INVALID_REF` and 122 `SUBETHA_E_READ_ONLY`.
- The shared vec (`subetha_vec_*`, handle kind 24) and the shared slab
  (`subetha_slab_*`, handle kind 25) at an element layout the caller
  declares in `subetha_element_layout`, each with a read-only open; code
  123 `SUBETHA_E_OUT_OF_BOUNDS`. In `subetha-cxc`, `RawVec` and
  `RawSlab`, the same regions at run-time layouts, and the vec and slab
  headers record the element size, the slot geometry, the alignment and
  a layout tag; an open that states another layout is refused, and an
  element wider than a cache line or aligned wider than 8 gets a slot
  geometry of its own.
- The shared region (`subetha_region_*`, handle kind 26) at an element
  layout the caller declares: allocate, free, get, set, clear, stats,
  flush and unlink, a slot named by a 32-bit index every process resolves
  the same way. In `subetha-cxc`, `RawRegion`, the same arena at a
  run-time layout, and the region header records the element size, the
  alignment, where the slots start and a layout tag.
- Batch entry points: `_try_push_many` and `_try_pop_many` on the adaptive
  ring, the SPSC ring, the MPSC pool, the MPMC grid, the Vyukov ring and
  the Lamport pair, `_try_push_many` with `_try_recv_many` on the
  broadcast ring, `_try_push_many` with `_try_pop_many` on the stack and
  `_try_steal_many` on the deque. One handle lookup and one panic guard
  carry a run of operations over the caller's own array, addressed by a
  base and a stride. A batch stops at the first refusal and reports how
  many it completed.
- A handle borrow raises the object's active count and takes the
  object's addresses, with no reference count cloned per call. The
  boundary costs about 11 ns a call, and every family's measured cost is
  in the crate README.
- The shared B-tree map (`subetha_btree_*`, handle kind 29), ordered by
  unsigned byte comparison of the key so every language agrees on it
  without a comparator crossing the boundary: insert, get, contains,
  remove, first, last, clear, stats, flush and unlink. In `subetha-cxc`,
  `RawBTreeMap` at run-time key and value sizes, with an ordered range
  walk taking both bounds and a limit, which carries its own magic
  because the typed map orders by `K: Ord` and would put the same keys
  elsewhere in the same file.
- The shared fence clock (`subetha_fence_clock_*`, handle kind 44): a
  hybrid logical clock per participant in one file, and the global fence:
  the latest clock across live slots, so every event any participant has
  recorded stands at or below it. Register,
  unregister, tick, merge, the local clock, the fence computed or
  published or read back, a slot's snapshot, stats, flush and unlink.
  `subetha_hlc` carries the `(physical_us, logical)` pair a merge orders
  by. A slot is a plain `uint32_t` index rather than a handle, so nothing
  is allocated per participant and nothing has to be closed. Code 128.
- The cross-process waker (`subetha_waker_*`, handle kind 48): parking
  and waking between processes, down on the platform's own futex -
  `WaitOnAddress` and the hardware monitor on Windows, `futex` on Linux,
  `_umtx_op` on FreeBSD - so a parked thread costs nothing until it is
  woken. A consumer parks at the sequence number it waits for and a
  producer that reaches it wakes it; the sequence is the caller's own.
  Create, open, reset, park, wait, release, the three wakes, stats and
  unlink. A park is a token from the shared hold table, so a second
  release of one is refused rather than freeing a slot its next holder
  relies on, and `wait` gives the park back itself. A full table is
  `SUBETHA_E_RING_WAKER_FULL`, which a caller answers by spinning on
  whatever it wanted.
- The lazy value (`subetha_lazy_*`, handle kind 47): a value produced
  once across every process that asks for it, so a fleet starting
  together makes one request against whatever serves the value rather
  than one each. `claim` hands the right to produce it to a single caller
  and refuses the rest; the winner publishes; the others wait. Codes 132
  and 133.

  In `subetha-cxc`, `SharedOnceCellDyn`: the once cell with its payload
  size given at run time and its initialization split into claim, produce
  and publish, for a caller that cannot pass a closure. A claim stands
  across the caller's own code, so it carries the process that took it:
  `wait` answers `ClaimantGone` once that process is gone rather than
  waiting out a deadline for a publish that will never come, and
  `reclaim` returns the cell to empty for the next caller. `publish`
  takes the claimant's pid, so a caller that lost the claim cannot
  overwrite the winner's value.
- The shared value (`subetha_shared_arc_*`, handle kind 46): a region of
  shared memory kept alive by the processes holding it and released when
  the last one lets go. Create, open, read and write
  by offset, the holder count, dead-holder reaping, stats, flush and
  unlink. `SUBETHA_ARC_UNLINK` removes the backing when the last holder
  releases; `SUBETHA_ARC_KEEP` leaves it for a process attaching later.
  Code 131 when every holder slot is held by a live process.

  In `subetha-cxc`, `SharedArcDyn`: `SharedArc` with the value's size
  given at run time rather than by a type, for callers that cannot name a
  Rust one. It shares the header, the holder table, the reaping and the
  last-holder policy, and because the header already recorded the value's
  size, a dyn arc of `size_of::<T>()` bytes and a `SharedArc<T>` attach to
  each other's backing while a length that disagrees is refused. Two
  obligations are stated rather than implied: the layout inside those
  bytes is the caller's to agree on, and a write is a copy with no
  ordering, so mutation needs an atomic in the region or a lock around it.
- The epoch barrier (`subetha_epoch_barrier_*`, handle kind 45): a
  rendezvous where each process waits at an epoch until the others reach
  it. Create, open, four waits - every live peer or a quorum, each with
  and without a deadline - the current epoch, the live peer count, stats,
  flush and unlink. What it promises is order rather than duration: the
  early arriver does not pass until the late one is there. It counts
  arrivals in one shared word and takes how many peers exist from a
  heartbeat table, so a process that dies between rounds stops being
  waited for once its slot goes stale instead of stalling the round for
  good. It is built from a heartbeat handle rather than a path, so it
  counts peers in the same table its caller beats into rather than a
  second view of the same file. Codes 129 and 130 name the two refusals a
  caller acts on differently: nobody is there to wait for, and the round
  asked for is already over.
- A lock hold, a semaphore permit, an epoch pin and an epoch ticket are
  tokens rather than handles, released by `subetha_rwlock_unlock`,
  `subetha_semaphore_release`, `subetha_pin_release` and
  `subetha_ticket_publish`. A handle is issued with a heap allocation and
  two mutexes and closed with a process-wide barrier, whose price rises
  with how many threads the process runs. That is right for a ring closed
  once at the end of a run and wrong for a lock released in a loop.
  Taking a token is a compare-exchange and giving it back is another,
  with nothing allocated and no lock taken. Measured on the AVX-512
  Windows build host, a lock acquire and release costing 3.0 ns of direct
  work: 42.1 ns through the ABI with eight threads in the borrow guard's
  registry, and 45.0 ns with seven other threads merely running. An epoch
  pin and release reads 52.7 ns and a semaphore acquire and release
  57.4 ns, against 21 to 35 ns of boundary for every other family. A
  registry size belongs with any of these figures rather than being a
  detail, because the registry never shrinks, and
  `subetha_borrow_registry_len` reports it so a bench records what it
  measured against instead of inferring it.
- A token and a handle are told apart by the ABI rather than by the
  caller. Both are 64 bits and both carry an index and a generation, so
  every token carries a tag bit that no handle has:
  `subetha_handle_destroy` refuses a token and names it, and the release
  entry points refuse a handle and name that. Destroying the object that
  issued a token gives that token's hold back, so nothing is stranded.
  Each slot carries a generation that steps on every release, so a token
  given back twice is refused, as is one whose slot has been taken since;
  an all-zeroes token, which is what an uninitialized variable holds,
  names nothing.
- The borrow guard's registry is read through an `ArcSwap`, so a handle
  close takes its snapshot with one reference count operation and no
  lock. The mutex serializes threads joining and leaving.
- Batch forms for the three shared-state families whose item is not a
  uniform payload: `subetha_vec_push_back_many` and
  `subetha_vec_get_many`, `subetha_arena_intern_many`, and
  `subetha_hashmap_insert_many` with `subetha_hashmap_get_many`. A vec
  push answers with the index the element landed at and an arena intern
  with the reference naming the bytes, so those two fill a word per item
  that landed rather than dropping the answer the call exists to give;
  another process may append between two batches, so a caller cannot work
  the indices out from a length it read beforehand. The map's item is a
  key and a value, which a caller already holds as two columns, so its
  forms walk two strided arrays rather than making it pack pairs.
- `subetha_abi_version()` reports 0.6.0: the minor is the highest shipped
  tier plus one, so a consumer reads it to learn which families it is
  linked against rather than which release it holds.
  `abi_version_matches_the_tiers_document` in `tests/header.rs` holds the
  constant against `C_ABI_TIERS.md` so the two cannot drift, and asserts
  the major is 0: the freeze is gated on a consumer outside this
  repository having used the ABI, not on the tiers having shipped.
- The holder table (`subetha_holders_*`, handle kind 43): the substrate
  under the epoch table's pins and tickets, reached directly. A fixed
  array of claimable slots, each carrying one `uint64_t` of the caller's
  own meaning and the process that claimed it, so
  `subetha_holders_reap_dead` can free every slot whose process is
  gone - the question a bare count cannot answer, because a number
  cannot be asked whether it is still running.
  `SUBETHA_HOLDER_FREE` and `SUBETHA_HOLDER_RESERVED` are refused as
  payloads rather than stored, since either makes a held slot read as
  something else. Claim in one step, or reserve and publish when the
  payload depends on state read after the slot is visible. In
  `subetha-cxc`, `SharedHolderTable` gives the bare `HolderTable` view a
  file of its own.
- The heartbeat table (`subetha_heartbeat_*`, handle kind 42): the lease
  and the election each track one holder, and this tracks a whole fleet.
  Each process registers a slot and beats into it; the slot carries the
  process id, the epoch it last beat at, a bitmap of up to
  `SUBETHA_HEARTBEAT_IN_FLIGHT` work units it has taken, and a role the
  caller assigns meaning to. A slot nobody holds reads as empty rather
  than as an error, so a supervisor walks every index to find who has
  gone quiet and what work they were holding when they went. Each slot is
  its own cache line and is written under a version a reader retries on.
- Leader election (`subetha_leader_*`, handle kind 41): among the
  processes attached to one file, exactly one leads, and which one
  converges without a vote. The lowest live process id wins; ids are
  unique on a host, so a set of live processes always agrees, and it
  takes one compare-exchange rather than a quorum. The election term goes
  up on every handover, so a follower watching for a change polls the
  term rather than the process id, which can come back to a value it held
  before.
- The owner lease (`subetha_owner_lease_*`, handle kind 40): one process
  at a time holds a resource, and a holder that dies loses it rather than
  keeping it for good, which is what `subetha_rwlock_*` cannot do. A
  lower process id preempts outright; a heartbeat that has fallen more
  than `grace_epochs` behind the global epoch marks a holder gone. The
  window is counted in epochs and nothing advances them on its own, so
  whatever calls `subetha_owner_lease_tick_epoch` sets how quickly a
  stale holder is detected, and a holder keeps its claim by beating
  faster than that. A payload of up to `SUBETHA_LEASE_PAYLOAD_MAX` bytes
  travels with the lease at a size the region records and checks at every
  attach, readable and writable only by the holder and under a version so
  a torn value is never published. `subetha_current_pid` is there so a
  program using the ABI needs nothing else to work the lease. In
  `subetha-cxc`, `RawOwnerLease` at a run-time payload size, which lays
  out its region through `begin_lease_region` and `publish_lease_magic`,
  as the typed lease does. Code 127.
- The condition variable (`subetha_condvar_*`, handle kind 39): park
  until another process says something changed. The Rust API takes a
  predicate closure, which does not cross the boundary, so the predicate
  stays with the C caller as it does in C generally: read
  `subetha_condvar_generation`, check your own predicate, and pass that
  generation to `subetha_condvar_wait`, which returns at once when a
  notify has already moved past it. That is what stops a notify landing
  between the check and the wait from being lost, and it is the same
  shape as a futex's expected-value argument. `notify_one` and
  `notify_all` report how many they woke, destroying the handle releases
  every parked caller, and `subetha_condvar_unlink` removes both backing
  files.
- The counting semaphore (`subetha_semaphore_*`, handle kind 37): a
  bounded number of permits any number of processes take and give back. A
  permit is a token `subetha_semaphore_release` returns, and
  `subetha_semaphore_held` reports how many are out, which is where a
  leak shows. `subetha_semaphore_acquire` waits up to a `timeout_ms` and
  registers among the waiters while it does, so a release from any
  process still advances the wakeup generation the Rust waiters watch;
  destroying the semaphore's handle releases every caller waiting on it
  and gives back every permit it issued. `subetha_semaphore_unlink`
  removes all three backing files.
- The reader-writer lock (`subetha_rwlock_*`, handle kind 35): many
  readers or one writer across processes, with writer priority. A hold is
  a token `subetha_rwlock_unlock` gives back, and destroying the lock's
  handle gives back every hold it issued. The waiting acquires take a
  `timeout_ms` and poll, since
  a release on this lock signals nobody, and destroying the lock's handle
  releases every caller waiting on it, so a destroy and
  `subetha_shutdown` both complete while an acquire is outstanding. In
  `subetha-cxc`, `SharedRWLock` gains `register_waiting_writer`,
  `unregister_waiting_writer` and `try_write_lock_registered`, which
  `write_lock` is built on; a writer waiting through the ABI
  registers for the length of its wait and so takes the lock ahead of
  readers arriving after it. Code 126.
- The epoch table (`subetha_epochs_*`, handle kind 32), the first of the
  C ABI's coordination tier. A scan takes a pin and reads one fixed view
  of a store while writers keep running; a compound write takes a ticket
  so everything it stamps becomes visible at once. Both are tokens:
  `subetha_pin_release` releases a pin and `subetha_ticket_publish`
  publishes a ticket, which is what the Rust guards do on drop, and
  destroying the table's handle gives back every one it issued.
  `subetha_epochs_dead_tickets` and `_free_dead_ticket` recover an epoch
  whose writer died mid-compound. In `subetha-cxc`, `SharedEpochs` gains
  `claim_pin` / `release_pin` and `claim_ticket` / `publish_ticket`,
  which `PinGuard` and `EpochTicket` are built on, so the claim
  protocol has one implementation rather than a second copy behind the
  ABI. Codes 124 and 125.
- The frame region (`subetha_frame_region_*`, handle kind 31): fixed-size
  blocks in a file for payloads too large to travel inside a ring slot,
  which is the region an adaptive ring builds for itself, reached
  directly. Create, open, reset, alloc, free, write, read, stats and
  unlink. Any process may free any block. The free list runs through the
  blocks themselves, so a free overwrites the first four bytes of the one
  it takes, and both the C test and the cross-process peer are built
  around that. In `subetha-cxc`, `FrameRegion::read_block` copies into a
  caller's slice, beside `read_block_into`, which appends to a `Vec`.
- The shared cell (`subetha_cell_*`, handle kind 30) at a value size the
  caller declares, recorded in the region, so a four-byte cell and a Rust
  `SharedCell<u32>` are one region and a handle declaring another size is
  refused. In `subetha-cxc`, `RawCell`.
- The shared linked list (`subetha_list_*`, handle kind 28) at an element
  layout the caller declares: push and pop at both ends, get, set,
  removal from the middle by the node's index, and a walk from either
  end. In `subetha-cxc`, `RawLinkedList` over `RawRegion`, whose
  `element_ptr`, `allocate_with` and `free_slot` let a caller touch part
  of an element in place.
- The shared atomics (`subetha_atomic_u32_*`, `subetha_atomic_u64_*`,
  `subetha_atomic_bool_*`, handle kind 27): load, store, swap, the fetch
  operations and compare-exchange, each in a sequentially-consistent form
  and an `_explicit` form taking a `SUBETHA_ORDER_` value, with the
  orderings an operation cannot have refused rather than panicked on.
- `subetha_ring_options` carries `frame_block` and `frame_blocks`, the frame
  region geometry an adaptive ring is created with, and `shm_sddl`, the
  security descriptor its shared-memory regions and wakers are created
  with; a shared-memory ring is created and opened through the secured
  constructors.
- `subetha-ffi-tests` carries `c/workloads.c` and `tests/workloads.rs`:
  fifteen workloads shaped like the programs that use the library, driven
  through the C ABI in every locale and mode their shape allows, each
  printing what it measured, with `SUBETHA_FFI_SOAK_SECS` repeating them
  for a soak. Ten spawn every role as a real child process, and nine are
  shaped like the programs that use the rings.
- The other six are shaped like the programs that use the shared state,
  the coordination and the transports, every role in C and a real child
  process: a content-addressed blob store (a hash map from an fnv hash to
  an arena reference, writers storing overlapping sets, readers on the
  arena read-only), a content index rebuilt generation by generation
  under an owner lease (a vec of records over an arena, reset at a fresh
  path, flushed, published through a counter, read read-only), a
  multi-version index (a versioned map changed by writers one at a time
  under a lock, as the tree beneath it requires, while readers scan under
  pins from the shared epoch table and no lock), a graph store on a frame
  region (chains of edge pages with a version word per page, writers
  appending and pruning, readers walking and re-reading a page caught
  mid-write), a record store under one reader-writer lock (ids in the
  strategy-switching set, records in a hash map, the set migrated under
  the write hold), and a cluster stream (sealed Sens-O-Matic streams from
  several sender processes into one receiver, more than one per process,
  the accounting closed against the kernel's own drop report). Twelve
  cells, strict and managed; every cell checks content from seeds and
  removes its backings through the ABI's own unlink calls.
- The line-log workload's writer ends on an end slot the harness pushes
  once every producer has returned, and on nothing else, so a producer
  starved on a loaded box cannot resume after the writer has gone and
  fill the ring with whole lines nobody wrote. Every test opens by
  closing whatever an earlier panicked test left, so one cause is one
  failure rather than the next test's shutdown reporting the leak as its
  own.
- The suite's `sleep_us` waits on a high-resolution waitable timer on
  Windows rather than on `Sleep`, whose granularity is the scheduler
  tick: at the default tick that host charges about 11 ms for any wait
  of a millisecond or less, so a poll asked for in microseconds cost
  tens of times what it asked and a loop bounded by a count of polls
  spanned far longer than the count was chosen for. Fourteen poll loops
  across nine cells reach the wire through that one helper. The stream
  cell's receiver now polls about 240 times a cycle where it polled 19,
  and its strict mode runs 133 soak passes with no operation over a
  second, where before it failed at 36 cycles and 205 of its 240 sender
  operations sat past 100 ms.
- The stream receiver stays past the last stream for as long as a sender
  is allowed to wait on one. It is the only thing that acknowledges a
  stream and it acknowledges only from inside its poll loop, so a
  shorter stay ends the only source of acknowledgement while a sender is
  still owed one, and what reaches that sender is indistinguishable from
  a peer that stopped responding. `STREAM_LINGER_MS` is defined as
  `STREAM_FINISH_MS` rather than as its own number, so the two cannot
  drift apart.
- The stream receiver polls at the rate its decoder is driven: 200
  microseconds in strict mode, where the polling thread drives the
  decoder itself, and 2 milliseconds in managed mode, where a sidecar
  thread drives it and the polling thread only carries away what that
  sidecar has already delivered. A poll faster than the sidecar finds
  nothing and spends the core the sidecar needs, which is why the cell
  passed in strict and failed in managed once the timer above took the
  poll from 11 milliseconds to half a millisecond. The empty-queue wait
  is what changes; a poll that finds an item processes it and continues
  with no wait, so a cycle's 1200 items still cost 1200 fast iterations.
  The cell now passes both modes on that host, 38 strict soak passes and
  39 managed, with no operation over a second, while the box carried
  another agent's build at normal priority and the gate ran at idle.
- `subetha-cxc`: `RawTreiberStack` and `RawDeque`, the Treiber stack and
  the Chase-Lev deque at a slot size chosen at run time, sharing their
  region layout and protocol with the typed `SharedTreiberStack` and
  `SharedDeque`; the headers carry the element alignment and a layout
  tag (`align_of::<T>()` and zero from the typed types).
- `subetha-ffi-tests`: a C suite compiled by the host's own compiler with
  every warning an error and run from `cargo test`, including a C consumer
  in a second process draining a ring through the file-backed wakers, and
  one such peer per family: thirty-two cross-process peers, each a real
  second process.
- `cargo run -p xtask -- ffi-install --prefix <dir>` lays out `include/`, `lib/`,
  `bin/` on Windows, a pkg-config file and a CMake package config for
  `find_package(subetha)`; `cargo run -p xtask -- ffi-package-gate` builds
  and runs a CMake consumer against that tree, reports a missing `cmake`
  or `pkg-config` as SKIPPED, and compiles the same consumer directly with
  the host's C compiler, shared and static. The package config sends the
  options in rustc's `native-static-libs` list, such as MSVC's
  `/defaultlib:msvcrt`, to `INTERFACE_LINK_OPTIONS`, and names as
  libraries only the entries that are libraries, so the static consumer
  links.
- `AdaptiveRing::unlink` removes every backing a prefix names, including
  backings grown past the creation hint, and reports the removed, missing
  and failed counts.
- `C_ABI_TIERS.md` at the repository root states the order in which the C
  surface ships.
- `subetha_ring_options` carries `max_holders` and `last_holder`, an opt-in
  at creation for a file-backed adaptive ring to be removed by whichever
  process lets go of it last: the holders are counted in a region of
  their own beside the peer directory, a holder whose process died is
  reaped by probing the pid, and the last release removes the backings
  together with the wakers and the notifier record the ABI keeps beside
  them, into one report. `SUBETHA_LAST_HOLDER_KEEP` with zero holders is
  a ring that counts no holders and removes nothing of its own; asking to
  unlink with `max_holders` zero is refused rather than given a number
  nobody chose. Anonymous and
  shared-memory rings refuse the option by name. In `subetha-cxc`,
  `AdaptiveRing::with_last_holder` and the `ring_holders` module.
- The C ABI's transports, tier 4. Sens-O-Matic (`subetha_sens_*`, handle
  kind 51): the unified sender and receiver that switch erasure code on
  measured loss, and the `_rlc_` and `_rs_` halves that pin one code and
  report zero switches because they cannot switch; all six share `send`,
  `poll`, `flush`, `local_port` and `read_stats`. Strict mode makes the
  poll the pump, on the calling thread; managed mode owns the thread
  that drives the decoder. With the `tls` feature, `subetha_sens_sender_tls`
  and `subetha_sens_receiver_tls` seal every item under TLS 1.3 and take
  the certificate and key as DER bytes, `subetha_sens_self_signed_cert`
  mints a pair, and `subetha_sens_tls_available` says whether the build
  carries the path; without the feature every entry point exists and
  answers `SUBETHA_E_NOT_SUPPORTED` naming it. The TCP and QUIC bridges
  (`subetha_tcp_bridge_*`, `subetha_quic_bridge_*`, kinds 49 and 50)
  carry items between a ring here and a peer there, a client or a server
  half each, with `run` under a timeout and `read_stats`; the QUIC bridge
  takes its certificate as DER bytes and `subetha_quic_self_signed_cert`
  mints one; `subetha_transports_available` answers for the pair, which a
  build carries or not independently of the sealed Sens path. The blocking
  bridge (`subetha_blocking_tcp_bridge_*`, kind 72) carries a blocking
  SPSC ring the same way, parking on the ring instead of yielding, so an
  idle bridge costs no CPU and a consumer that stops draining ends the
  run with an error rather than a discard; it rides the same feature and
  the same `run` and `read_stats` shape. Virtual
  endpoints (`subetha_endpoint_*`, kind 63): a registry of named targets,
  each a local locale-ring handle or a remote address, read together with
  the generation it was read at, and `_still_valid` in place of the
  lifetime borrow the Rust pinned endpoint takes. The quality-of-service
  policy (`subetha_qos_*`, kind 64), each field its own atomic and no
  call that writes the whole policy at once.
- `subetha_sens_finish(handle, timeout_ms, out_acked)`: wait up to the
  deadline for the far end to acknowledge everything a sending half sent,
  retransmitting what it has not, the way the Rust `finish` does, so a C
  sender ends a stream rather than closing on a tail that no repair
  follows. `subetha_sens_stats` carries `unroutable`,
  `preauth_dropped`, `unopened` and `handshake_failures`, a unified
  receiver's own accounting of what reached the process and was not
  delivered, so a stream that never opens shows where it stopped rather
  than as a zero drop count.
- What a strict-mode transport loses in the kernel is reported as far as
  the host allows, and the report says which case it is.
  `subetha_sens_stats` carries `missed`, the datagrams the far end never
  reported receiving, which only a unified sender learns, and
  `kernel_dropped`, the datagrams this host's kernel dropped for a full
  receive buffer, each paired with a report of `SUBETHA_DROPS_EXACT`,
  `SUBETHA_DROPS_OCCURRED` or `SUBETHA_DROPS_UNKNOWN`. Linux counts per
  socket through the `SO_RXQ_OVFL` ancillary message; FreeBSD's `SO_RERROR`
  turns an overflow into an error on receive, which says that it happened
  and not how often; Windows moves no counter. A zero paired with anything
  but `EXACT` is not a claim that none were lost. In `subetha-cxc`,
  `dgram::DropReport` and `DropTally`, `kernel_drops()` on the demux
  socket and every receiver, and `missed()` on the unified sender.
- The C ABI's probabilistic and specialist structures, tier 5. The Bloom
  filter, the blocked Bloom filter, the count-min sketch and HyperLogLog
  (`subetha_bloom_*`, `subetha_blocked_bloom_*`, `subetha_cms_*`,
  `subetha_hll_*`, kinds 52, 56, 53 and 66), each with a `_suggest` that
  sizes it from a target; the histogram with `_percentile` and
  `_bucket_for`, the reservoir sampler with `_snapshot`, the rate limiter
  with `_try_acquire` and `_acquire_wait`, and the topology map recording
  fan-out and fan-in (kinds 54, 57, 55 and 58); the versioned chain, map
  and slab and the laned map (kinds 59, 61, 60 and 62), epoch-versioned
  structures whose reader pins an epoch and whose writer sweeps or voids
  one once every reader has left it, the map and the slab compiled at
  three size classes named by `SUBETHA_VERSIONED_*` and runtime-sized
  otherwise; the bit vector (kind 65), whose `set`, `clear` and `toggle`
  write what the bit was into `was`, so `set` is a claim between
  processes; NaN-boxed values (`subetha_nan_*`), one `uint64_t` the
  caller holds and the only family with no handle, no mode and no need
  for `subetha_init`, with a two-level tagged form
  (`subetha_nan_from_tagged`, `_as_tagged` and `_is_tagged`) carrying an
  index and a tag at a tag width the caller picks, which
  `subetha_nan_type` reports as `SUBETHA_NAN_RESERVED` because it reads
  only the first level; the LRU cache (kind 67), where `get` leaves recency
  alone and `get_and_touch` promotes and a miss leaves the value buffer
  as it was; the directed graph (kind 68), out-edges walked with
  `first_edge` and `next_edge` to `SUBETHA_GRAPH_NIL` and node removal
  not offered because every edge pointing at a node lives in another
  node's chain; the time-point tile (kind 69), sixteen lanes with
  `visible_mask` answering all of them in one call; the content-prefix
  pointer (`subetha_umbra_*`), sixteen bytes the caller holds and
  resolves against a region handle, with `_prefix_eq` named for what it
  is; the strategy-switching set (kind 70), moved between a vector and a
  hash map by `_migrate` alone, its strategy word kept in a state file
  beside the two backings so a migration through one handle is what every
  process reads, mapped by every handle and read on every operation as
  the typed `SharedUniversal` keeps it, its open taking no strategy and
  keeping the one in force
  (`subetha_universal_open(base_path, capacity, element_size, mode,
  out)`), its migration publishing by compare-exchange over the word it
  read so a word that moved is reported rather than overwritten, and its
  stats carrying the stamp a reader compares across an operation; and the
  cascade tower (kind 71), at a
  depth set by the levels the caller supplies, `get` refusing at the
  first level of a path that no longer agrees rather than resolving
  whatever now sits at its end.
- In `subetha-cxc`, six cores at element sizes chosen at run time, so a
  C caller reaches the generic structures without a fixed instantiation
  per type: `RawLruCache` over `RawHashMap` and `RawLinkedList`, every
  read checking that the slot the map named still holds the key;
  `RawGraph` over two `RawRegion`s; `RawUmbraPointer`, a 16-byte value;
  `RawTimePointTile`, refusing version 0 and clearing a removed lane's
  version before its occupancy; `RawKTower`, its depth taken from the
  levels supplied; and `RawUniversal`, one word carrying strategy,
  generation and version, published after the elements reach the target
  backing, a no-op migration burning no version.
- `subetha-cxc`: `SharedArray`, a flat read-only alternative to
  `SharedSlab` and `SharedVec` for a table written once and read from
  many processes. One 64-byte header carries the magic, the stride, the
  length and a sealed flag; the elements follow at their own stride with
  no per-slot lock or version, so a `u8` costs one byte rather than the
  64 a slab slot takes. `create`, `fill_from`, `set`, `seal`, which
  flushes before it sets the flag, `open_read_only`, which maps the file
  without write access, `get` and `as_slice`. The slab and vec module
  docs state what a slot costs and what that cost buys, with the packing
  that recovers it on the published crate: a slot-sized array such as
  `[u8; 56]` keeps the seqlock at 1.14 times the payload rather than 64.
- `subetha-cxc`: `residue_fec`, a residue-number erasure code recovered
  by the Chinese remainder theorem rather than by a linear system, behind
  the off-by-default `residue-fec` feature. It is correct and its tests
  pass, and it is not what a caller wants: it encodes about 210 times
  slower than the RLC and recovers about 135 times slower, costs slightly
  more on the wire, and is a block code, so a loss waits for the rest of
  its block where the sliding window does not. The one property that sets
  it apart from a random linear code, generating no coefficient at all,
  the published taps have as well. It sits behind a feature rather than
  in the published surface because the optimizations its own notes call
  for would change its moduli and its chunk mapping, which a public
  module is a promise not to do.
- `subetha-cxc`: `region_file`, the one place a file-backed region is
  created, opened, opened read-only and removed, so a removal means the
  same thing on every platform; the ring prefixes, the raw primitives and
  the shared primitives all open through it.
- `subetha-cxc`: `ring_trace`, compiled only into a debug build, records
  what happens to a ring's ownership - the morph a push landed across,
  the peer counts and claimed bitmaps a shape decision was taken on, the
  producers and consumers arriving, leaving and being reaped, and the
  slot events behind each pop - and a failure dumps the whole trace held
  for that ring.
- Interoperability vectors a second implementation checks itself against:
  `crates/subetha-cxc/vectors/rlc.txt` for the sliding-window RLC and
  `vectors/rs.txt` for the block code, each written out by a test that
  fails when the code stops reproducing it.
- Managed mode's shape sidecar scans every
  `SUBETHA_SCAN_INTERVAL_DEFAULT_US` microseconds, 250, when a caller
  names none in `subetha_ring_options::scan_interval_us`. The cadence is
  managed mode's added round-trip latency rather than a tuned optimum:
  measured on Linux and FreeBSD from 250 to 10000 microseconds, a
  request and its response each wait for a scan, so a round trip costs
  exactly one interval and a streaming caller costs nothing, with no knee
  anywhere in that range.

### Changed

- The sliding-window RLC takes its coefficients from a published table
  of sixty-four constants rather than from a generator that derives them
  per repair. This changes the wire and 0.3.0 does not interoperate with
  0.2.x on the RLC repair path. A repair names its generator in the
  high nibble of `dt` - free, because the density occupies 0..=15 - so a
  decoder reads the choice off the stream rather than from its own
  configuration. A repair naming generator 0, which is what 0.2.x sends,
  is counted by `RlcDecoder::refused_repairs` and dropped, because its
  coefficients cannot be reproduced here and an equation with the wrong
  coefficients does not fail to solve, it solves to bytes that were never
  sent. `RlcEncoder` selects no generator: there is one.
  `rlc_fec::refused_repairs()` tallies those refusals for the process
  beside each decoder's own count, so a peer still sending 0.2.x repairs
  is visible without a decoder to hand.

  A fixed generator suffices because the code is convolutional -
  successive repairs cover windows that have shifted, so their equations
  differ though the generator does not, and the per-repair coefficient
  was doing work the window's own motion already did.

  What it costs was measured against the per-repair generator 0.2.x
  sends, on one wire stream per trial with the same dropped positions
  dealt to each: across random loss from 2% to 20% and bursts of 2, 3 and
  5, the taps came within 0.15 percentage points everywhere and were
  ahead in two regimes. What the table can be is bounded too: over four
  coding geometries the shipped table reaches the ceiling set by which
  repairs cover which symbols, so no table of any values recovers more.
  `benches/generator_recovery.rs` keeps the shipped code's recovery
  profile across those regimes, and `benches/tap_search.rs` the ranking
  and the controls that prove the ranking sees a bad table.

- `repair_key` is a repair sequence number: it names the repair in logs
  and traces and does not feed the coefficients, which come from the
  window.

- The published tap table holds one tap per position the widest coding
  window can hold - sixty-four, the controller's own maximum - and the
  density threshold selects a class spread across the window rather than
  a prefix of it: `dt` of 15 takes every position and `dt` of 0 one in
  sixteen, at every depth. A table shorter than the window caps how far a
  repair reaches, however wide the window is, and widening the window is
  the controller's answer to a long burst, so the table length is
  asserted against that maximum and the two constants cannot drift apart
  in silence.

- `AdaptiveRing` walks every backing it has been in, for the life of the
  ring, rather than remembering one previous shape. A producer resolves
  the shape tag and pushes some instructions later, so a backing can
  receive a push after it stops being current, and two morphs in a row
  would otherwise strand whatever landed in the first. There are four
  shapes, so never forgetting one is a bounded walk. A morph is not
  deferred behind an undrained backing and `RingError::StaleBacklog` is
  not returned; the variant stays as a reserved code.

- `HeartbeatTable::open` checks the file's length before mapping, so a
  caller declaring more slots than the file holds gets `LayoutMismatch`
  naming the disagreement rather than an `IoError` from mapping past the
  end of the file, which is how `OwnerLease::open` and the ring families
  answer it.

- The RLC decoder's row scale runs on the GF(2^8) SIMD ladder
  (`fec::gf_mul_auto`), beside the multiply-add sites around it, rather
  than a byte at a time: a symbol is up to a full datagram and a window
  reaches 64 pivots, so the scale is the same order of work as the
  accumulate steps it sits beside. The field and the coefficients are
  untouched, so the interoperability vectors are unchanged.

- A debug build of the SPSC ring claims the tail advance with a
  compare-exchange rather than a plain store, so a second reader on a
  core that admits one panics naming the consumer, the backing and the
  ring's recent history instead of leaving a duplicate to be inferred
  from what a consumer received. A release build keeps the plain store
  the single-reader contract allows.

### Fixed

- The shared hash map's walk (`subetha_hashmap_next`, `RawHashMap::next_entry`,
  `SharedHashMap::snapshot`) handed back a slot on its state alone, and an
  insert claims its slot before it writes the payload and publishes the
  hash, so a walk racing an insert copied the zeroed payload of an entry
  still being written. The blob-store workload's reader met it on every
  host once the cell soaked: a reference of zero, a 0-byte blob that did
  not hash to its key. A walk now passes over a slot claimed and not yet
  published, as a lookup for that key already waited on it.
- A capacity morph on `CapacityAdaptiveRing` and `CapacityBroadcastRing`
  pruned every drained backing from the stale list, while a producer
  pushes into the active backing of the state it loaded: a push delayed
  across two morphs landed in a backing no state named, no consumer's
  stale walk reached it, and a consumer waiting on a count spun forever.
  Seen once in ten parallel runs of the ring tests on a loaded Linux host.
  A drained entry now leaves the list only when the list is its last
  holder, so a backing a loaded snapshot still names stays reachable for
  the push that snapshot carries.
- The peer directory's liveness probe on Unix passed a slot's pid word to
  `kill` as a `pid_t` as it stood, so a word of `u32::MAX` became `-1`,
  which names every process the caller may signal and answers alive, and
  the dead-peer reaper kept such a slot forever. A value that does not
  fit a positive pid is now dead without a probe. The test storing
  `u32::MAX` as a crashed holder's pid had failed on Linux and FreeBSD
  every time and passed on Windows, whose probe refuses the value.
- A peer published its claim to the directory's bitmap before it
  published to the count the shape policy reads, so a shape decision
  taken in that window saw fewer peers than had registered and could
  choose a shape narrower than the peer set - a single-producer shape
  under several producers, which costs one item. A claim now raises the
  count before taking the bit and puts it back when no slot is free, so
  the count can only overstate, and a shape too wide for its peers
  carries them correctly. A contract ceiling checked against this count
  can now refuse a registration that a race would previously have
  admitted past the declared bound; refusing names an error, admitting
  broke the contract silently.
- A consumer arriving into a lower-numbered slot took the single-reader
  role off a consumer that was inside a pop on a Lamport backing. The
  role is checked before the pop and is not atomic with it, so the
  displaced reader kept copying while its replacement started and both
  took the same item. An incumbent that still holds its slot now keeps
  the role, and only a designation naming a slot nobody holds moves; an
  arrival cannot displace a walk in flight, and a departure cannot
  either, because the consumer that leaves is the one that would have
  been walking.
- Two threads could hold one consumer slot at once, putting two readers
  on a Lamport core that admits one and handing both the same item. A
  claim takes the directory's bitmap bit before storing its pid, so a
  recycled slot briefly reads as claimed by the sentinel its release
  wrote; the dead-peer reaper excluded pid zero, which is what a
  producer release writes, but not that sentinel, which is what a
  consumer release writes, and so freed a claim whose holder was
  running.
- A single-reader shape served consumer slot 0 alone, but slots come
  from the directory's lowest-free-bit claim, so a consumer that
  outlived the others kept whatever slot it had claimed and read empty
  on a ring that was not empty. The reader is now the lowest claimed
  consumer slot, re-derived on every topology sync.
- A ring could be left owned by a consumer that had departed: the
  rebalance assigns from a snapshot of the claimed slots and can hand a
  ring to one that is already unregistering, then ask that owner for a
  handoff no absent owner will apply. Recovery existed only through the
  crash takeover, which probes a pid and so runs on every 1024th empty
  scan, and a consumer that drains to the first empty and stops never
  reached it. An owner that released its slot needs no pid probe, so the
  scan reclaims such a ring at once.

### Documentation

- A tutorial for arriving with a C or C++ toolchain: the install layout
  per platform, linking through CMake, pkg-config or the compiler
  directly, a first ring, and the three separate calls a failure is read
  with.
- The cross-process waker and the lazy value carry a direct-Rust figure
  beside their through-the-ABI one, so what is recorded is the cost of
  the boundary rather than the cost of the operation.
- `SENS_O_MATIC_WIRE.md` at the repository root, the normative wire
  specification for the Sens-O-Matic transport, read from the source: the
  packet types, the RLC and Reed-Solomon headers field by field, the
  control frames and their variable-length encoding, connection
  identity, path validation and admission, the published tap table and
  the density nibble, and what version 1 does not interoperate with. Two
  of its hand-written tables are held against the code by tests, and the
  README and the wiki point at it as normative.
- The C ABI acceptance gate in the README: fifteen workloads driven
  through `subetha.h`, ten of them spawning every role as a real child
  process, and the 37 cells `SUBETHA_FFI_SOAK_SECS` soaks. It carries
  what the three legs measured at 240 seconds a cell, and states that
  the three hosts are two machines: Linux and FreeBSD are guests on one
  processor, so three operating systems is what three legs buy and the
  wall times are not independent of one another.
- Every published performance figure is recomputed from the data checked
  in beside it. Eleven stated ranges did not contain their own
  measurements, each because a bound had been rounded the wrong way: a
  range summarizing measurements takes its floor down and its ceiling
  up, and rounding both alike narrows the interval past the data while
  reading like ordinary tidying. The pinned-shape span is 36.8 to 114.9
  ns, where two documents had given 37 to 115 and 49 to 121 for the same
  quantity and neither contained it; the same correction runs through
  the per-platform leaderboard, its summary list, the block
  Reed-Solomon and TCP tail latencies, and the two wiki pages that
  carried them. A bridge document attributed a one-way figure to the
  leaderboard that the leaderboard does not hold, and the conclusion
  resting on it now states the measured relationship rather than an
  equality. A sentence pointing at a crate README that has never existed
  is gone. What was already exact is most of it: every ratio claim, every
  cell of the bridge tables, the ordering-mode table, and the in-process
  comparison including the claims that name a winner.
- The install lines name the version the crates are published at.
- Every comment and document in the workspace states what the code is, in
  US spelling, without emphasis capitals and without narrating what
  changed.

## [0.2.9] - 2026-09-05

### Added

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

### Changed

- `ShardedUdpSender::send_item` returns `io::Result<()>` and reports
  `BrokenPipe` naming a shard whose thread has ended, where it used to
  drop the item. Callers handle the result.
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
  finish. An empty file is still waited on.
- The RLC receiver handed any frame without a connection id to the most
  recently admitted session, which rebound its peer address to the
  frame's source without validation, so a one-byte datagram from anyone
  re-pointed that session's control traffic. Such frames are counted and
  reach no session. A `PATH_RESPONSE` answering a migration challenge is
  routed to the window whose id it carries, so an older peer's migration
  validates while a newer peer is live.
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

- Every new counter on its primitive's page; the listener's send-failure
  accounting; the RLC routing rule for frames that name no window.

## [0.2.8] - 2026-08-29

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
- Benches: a whole-store scan across writer lanes, four writers across
  four lanes against the same four behind a mutex (5.18x ahead), and the
  versioned slab's push and pinned read.

### Changed

- The `SharedEpochs` table layout carries a new magic for the ticket
  region; a table laid out by 0.2.7 or earlier is refused rather than read
  with the region missing.

### Fixed

- `SharedHashMap` published a claimed slot's hash before its payload,
  letting a prober plant the same key in a second slot; the payload lands
  first and hash 0 marks a slot still forming. A writer whose tombstone
  claim was stolen re-probes instead of claiming the empty slot beyond it,
  and concurrent updates to one key take the seqlock by CAS.

### Documentation

- Pages for the versioned slab and the laned map.

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
  demux socket routes by session epoch on its own.

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

[Unreleased]: https://github.com/Variably-Constant/SubEtha/compare/0.5.1...HEAD
[0.5.1]: https://github.com/Variably-Constant/SubEtha/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/Variably-Constant/SubEtha/compare/0.4.1...0.5.0
[0.4.1]: https://github.com/Variably-Constant/SubEtha/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/Variably-Constant/SubEtha/commit/0.4.0
[0.3.3]: https://github.com/Variably-Constant/SubEtha/commit/0.3.3
[0.3.2]: https://github.com/Variably-Constant/SubEtha/commit/0.3.2
[0.3.1]: https://github.com/Variably-Constant/SubEtha/commit/0.3.1
[0.3.0]: https://github.com/Variably-Constant/SubEtha/commit/0.3.0
[0.2.9]: https://github.com/Variably-Constant/SubEtha/commit/38a10f3
[0.2.8]: https://github.com/Variably-Constant/SubEtha/commit/6ca3d22
[0.2.7]: https://github.com/Variably-Constant/SubEtha/commit/aec1f18
[0.2.6]: https://github.com/Variably-Constant/SubEtha/commit/597c23a
[0.2.5]: https://github.com/Variably-Constant/SubEtha/commit/7d1fc25
[0.2.4]: https://github.com/Variably-Constant/SubEtha/commit/e73a185
[0.2.3]: https://github.com/Variably-Constant/SubEtha/commit/0cd7018
[0.2.2]: https://github.com/Variably-Constant/SubEtha/commit/c301474
[0.2.1]: https://github.com/Variably-Constant/SubEtha/commit/a8e96f0
[0.2.0]: https://github.com/Variably-Constant/SubEtha/commit/7a41964
[0.1.12]: https://github.com/Variably-Constant/SubEtha/commit/6827089
[0.1.11]: https://github.com/Variably-Constant/SubEtha/commit/f9fc6e3
[0.1.10]: https://github.com/Variably-Constant/SubEtha/commit/b08e3f8
[0.1.9]: https://github.com/Variably-Constant/SubEtha/commit/234f370
[0.1.8]: https://github.com/Variably-Constant/SubEtha/commit/536a9dc
[0.1.7]: https://github.com/Variably-Constant/SubEtha/commit/16ca524
[0.1.6]: https://github.com/Variably-Constant/SubEtha/commit/64d4c51
[0.1.5]: https://github.com/Variably-Constant/SubEtha/commit/04f5477
[0.1.4]: https://github.com/Variably-Constant/SubEtha/commit/7d6a890
[0.1.3]: https://github.com/Variably-Constant/SubEtha/commit/a7bc859
[0.1.2]: https://github.com/Variably-Constant/SubEtha/commit/de5f6d3
[0.1.1]: https://github.com/Variably-Constant/SubEtha/commit/0a5e48e
[0.1.0]: https://github.com/Variably-Constant/SubEtha/commit/9a91f03
