//! Substrate for adaptive primitives.
//!
//! The five core abstractions actually consumed by the IPC stack:
//!
//! - [`HandshakeHeader`] - per-instance generation counter and in-flight tracker.
//! - [`ObservationRing`] - per-instance multi-producer ring of op observations.
//! - [`migration`] - dual-stack migration protocol primitives.
//! - [`Marshal`] - type-system contract for "this value can cross an
//!   address-space boundary byte-identically." Stricter than `Send`;
//!   required by every cross-process primitive in `subetha-cxc` that
//!   stores typed values (e.g. `SharedDeque<T>`).
//! - [`SwapCell`] - an `Arc` swapped atomically and read without
//!   touching its reference count; a replaced value is dropped when its
//!   last reader lets go.
//!
//! Plus the architecture catalog ([`Axis`] / [`AxisMask`]) for direction
//! signatures and the [`cpuid`] helpers for CPU-feature detection.
//!
//! Every adaptive primitive in `subetha-pointers` and `subetha-cxc`
//! carries a `HandshakeHeader` at a known offset.

#![forbid(unsafe_op_in_unsafe_fn)]

// The crate links std: the per-thread sequential id machinery in
// [`observation::thread_id`] needs `thread_local!`.

pub mod axis_signature;
pub mod cpuid;
pub mod handshake;
pub mod marshal;
pub mod migration;
pub mod observation;
pub mod swap_cell;

pub use axis_signature::{Axis, AxisMask, Fusion};
pub use cpuid::{has_movdir64b, has_waitpkg};
pub use swap_cell::{SwapCell, SwapCellOption};
pub use handshake::HandshakeHeader;
pub use marshal::{Marshal, MarshalError};
pub use migration::{Generation, MigrationGuard};
pub use observation::{Observation, ObservationRing, any_observer_armed, thread_id};
