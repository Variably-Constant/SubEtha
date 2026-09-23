---
title: "Run the release gate"
weight: 80
---

# Run the release gate

The release gate lints and tests every crate in the workspace at its
defaults and with every feature it declares, and runs the Python
package's own suite as it ships and with every feature. A release is cut
from a commit the gate passes on each host that builds it.

```sh
cargo run -p xtask -- gate
```

## What it runs

| Pass | Commands | What it covers |
|---|---|---|
| The workspace at default features | `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace --no-fail-fast` | every crate as a dependent gets it, and the refusal each transport entry point gives when its feature is off |
| Each crate that declares features, at its defaults | the same two, with `-p <crate>` | the crate built alone, so nothing it is tested with was switched on by another crate in the workspace |
| Each crate that declares features, with all of them | the same two, with `-p <crate> --features <every feature it declares>` | its whole feature surface, in a build of its own |
| The Python package | `maturin develop` into a virtual environment made for the run, then `pytest`, once as it ships and once with every feature | the extension module as the wheels build it, and with the bridge classes |

The crates that declare features are `subetha`, `subetha-cxc`,
`subetha-ffi`, `subetha-ffi-tests` and `subetha-py`. The `transports`
feature of `subetha-ffi-tests` runs the C suite against a library that
carries every transport.

## What it leaves off

| Crate | Feature | Hosts | Why |
|---|---|---|---|
| `subetha-py` | `extension-module` | every host | it leaves libpython out of the link, which the crate's test binaries need; the Python pass builds the extension with it |
| `subetha-cxc` | `zmq-bench` | FreeBSD | zmq-sys 0.12 always compiles its vendored libzmq 4.3.4, and zeromq-src has no FreeBSD configuration: it never generates `platform.hpp` |

A host with no Python interpreter builds no `subetha-py` at all, since
pyo3's build script needs one, and the run says so. Every other feature
is built on every host, so one declared later is in the gate without an
edit to the task.

## Reading the result

Each step prints `=== GATE <step> ===` ahead of its own output. The run
ends with `=== GATE SUMMARY on <os> ===` and one line per step: `PASS`,
`FAIL` with the exit status, or `SKIPPED` with the reason. A step that
could not start is a `FAIL`. The task exits non-zero when any step
failed.

## What a host needs

- The Rust toolchain and a C compiler. The C suite and the vendored C
  libraries compile with it, and `zmq-bench` needs a C++ compiler for
  its libzmq.
- On Linux, clang, libelf and zlib for the libxdp that `wire-locale`
  builds.
- Python 3.11 or later with `venv`, and network access for pip to
  install maturin and pytest into the run's environment.
- Disk for one target directory holding every configuration. The gate
  builds without debug info (`CARGO_PROFILE_DEV_DEBUG=0` and
  `CARGO_PROFILE_TEST_DEBUG=0`), and a full run's target directory then
  measured 9.1 GB on Linux x86-64 and 3.3 GB on FreeBSD x86-64, whose
  ZFS compresses it. With debug info, a run outgrew the 15 GB a Linux
  host had free.
