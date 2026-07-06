//! SubEtha - the Cross-Context Channel (CXC).
//!
//! One byte layout that serves cross-thread, cross-process, disk, and
//! network. After construction every send and recv is a user-space
//! atomic op on a memory-mapped file the kernel page-aliases between
//! participants - no syscalls on the data path, no locks in your code.
//!
//! This is the umbrella crate. It pulls in the whole stack and
//! re-exports each member as a module:
//!
//! - [`core`] (`subetha-core`) - the shared substrate: handshake
//!   header, observation ring, migration generation, `AxisMask`.
//! - [`pointers`] (`subetha-pointers`) - adaptive in-process exotic
//!   pointers (content-prefix, bloom, strided, CHERI-style, ...).
//! - [`sidecar`] (`subetha-sidecar`) - the adaptive control plane.
//! - [`cxc`] (`subetha-cxc`) - the MMF-backed cross-process primitives:
//!   `AdaptiveRing`, `SharedRing`, the Big* of shared data structures,
//!   and the QUIC / TCP / reliable-UDP bridges.
//!
//! Most users reach for [`cxc`]:
//!
//! ```no_run
//! use subetha::cxc::AutoIpc;
//!
//! let chan = AutoIpc::new("/tmp/events.bin")
//!     .capacity(64)
//!     .build_channel::<u64>()?;
//! chan.send(&42)?;
//! let v = chan.recv()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Feature flags
//!
//! The default build is dependency-light and cross-platform. The
//! capability features forward to `subetha-cxc`, and the platform- or
//! C-library-specific dependencies behind each one stay gated:
//!
//! - `quic-bridge` - `QuicBridgeClient` / `QuicBridgeServer` over QUIC.
//! - `tcp-bridge` - `TcpBridgeClient` / `TcpBridgeServer` over TCP.
//! - `tcp-tls-bridge` - the TCP bridge inside a rustls 1.3 record layer.
//! - `tls` - the optional TLS record layer for the reliable-UDP transport.
//! - `wire-locale` - the NIC-bypass datapath (AF_XDP / netmap / BPF / XDP).
//! - `linux-futex-raw` - the raw Linux futex surface on `CrossProcessWaker`.

#![forbid(unsafe_code)]

/// The shared substrate ([`subetha_core`]).
pub use subetha_core as core;

/// Adaptive in-process exotic pointer types ([`subetha_pointers`]).
pub use subetha_pointers as pointers;

/// The adaptive control plane ([`subetha_sidecar`]).
pub use subetha_sidecar as sidecar;

/// MMF-backed cross-process primitives ([`subetha_cxc`]).
pub use subetha_cxc as cxc;
