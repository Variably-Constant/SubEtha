---
title: "Fd Handoff"
weight: 72
---

# fd_handoff verb pair

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-Unix%20%28Linux%20%2B%20macOS%29-blue)
![Mechanism](https://img.shields.io/badge/mechanism-SCM__RIGHTS-green)

UNIX domain socket + SCM_RIGHTS file descriptor passing
utilities. Lets one process hand a file descriptor to another
over a UDS; the receiving process gets a duplicated fd referring
to the same kernel file table entry.

For the substrate: a sender process can hand the underlying file
fd of a [`ShmFile`](../../specialized/shm-file/) or file-backed
[`SpscRingCore`](../../rings/shared-ring-spsc/) to a receiving
process; the receiver mmaps the duplicated fd and observes the
same shared region without re-opening the file by path.

This is a VERB pair (send / recv operations), NOT a new
[`Locale`](../../rings/locale-adaptive-ring/) variant. The locale
axis stays at three members (Anon / ShmFs / File); fd-passing
operates on whichever backing exposes a file fd.

## API

| Function | Behavior |
|---|---|
| `send_fd(stream: &UnixStream, fd: RawFd) -> io::Result<()>` | Send one fd over an established UnixStream via libc::sendmsg + SCM_RIGHTS. |
| `recv_fd(stream: &UnixStream) -> io::Result<RawFd>` | Receive one fd. Blocks until peer's sendmsg arrives. |
| `accept_one(uds_path) -> io::Result<UnixStream>` | Bind a UDS at `uds_path` (removing any stale file), accept one connection. |
| `connect(uds_path) -> io::Result<UnixStream>` | Connect to a UDS at `uds_path`. |

## Use cases

- **Privilege drop**: parent process opens MMF, drops
  privileges, hands the fd to a worker that runs without the
  parent's capabilities.
- **Live process restart**: old process hands fd to a fresh
  process before exiting; no data loss in the ring.
- **Supervisor/worker handoff**: supervisor opens MMF, hands
  fds to multiple workers; supervisor doesn't write to the ring.

## Worked sketch

```rust,no_run
use subetha_cxc::fd_handoff::{accept_one, connect, send_fd, recv_fd};
use subetha_cxc::shm_file::ShmFile;
use std::os::unix::io::AsRawFd;

// Sender side:
let mut shm = ShmFile::create_or_open_named("ipc_demo", 4096)?;
// shm has an internal file fd we want to share with another process.
let stream = accept_one("/tmp/handoff.sock")?;
// In a real handoff we'd pass shm's underlying file fd.
// The ShmFile struct keeps that fd private; reach it via a custom
// wrapper or expose it explicitly in your build.

// Receiver side:
let stream = connect("/tmp/handoff.sock")?;
let received_fd = recv_fd(&stream)?;
// Wrap received_fd in a std::fs::File via from_raw_fd and mmap it.
# Ok::<(), std::io::Error>(())
```

## When to reach for this

- The receiving process doesn't have, or shouldn't have,
  filesystem access to the ring's path.
- The sending process needs to relinquish its handle but the
  ring must stay alive.

## When NOT to reach for this

- Both processes already have filesystem access to the ring's
  path: open it by path directly via
  [`LocaleAdaptiveRing::create`](../../rings/locale-adaptive-ring/).
