//! Substrate for adaptive primitives.
//!
//! The four core abstractions actually consumed by the IPC stack:
//!
//! - [`HandshakeHeader`] - per-instance generation counter and in-flight tracker.
//! - [`ObservationRing`] - TLS-local ring buffer for op observations.
//! - [`migration`] - dual-stack migration protocol primitives.
//! - [`Marshal`] - type-system contract for "this value can cross an
//!   address-space boundary byte-identically." Stricter than `Send`;
//!   required by every cross-process primitive in `subetha-cxc` that
//!   stores typed values (e.g. `SharedDeque<T>`).
//!
//! Plus the architecture catalog ([`Axis`] / [`AxisMask`]) for direction
//! signatures and the [`cpuid`] helpers for CPU-feature detection.
//!
//! Every adaptive primitive in `subetha-pointers` and `subetha-cxc`
//! carries a `HandshakeHeader` at a known offset.

#![forbid(unsafe_op_in_unsafe_fn)]

// subetha-core was historically `no_std`, but the per-thread sequential id
// machinery in [`observation::thread_id`] requires `thread_local!`,
// which is std-only. Every dependent already pulls in std, so the
// no_std attribute was removed to keep the substrate consistent.

pub mod axis_signature;
pub mod cpuid;
pub mod handshake;
pub mod marshal;
pub mod migration;
pub mod observation;

pub use axis_signature::{Axis, AxisMask, Fusion};
pub use cpuid::{has_movdir64b, has_waitpkg};
pub use handshake::HandshakeHeader;
pub use marshal::{Marshal, MarshalError};
pub use migration::{Generation, MigrationGuard};
pub use observation::{Observation, ObservationRing, any_observer_armed, thread_id};
