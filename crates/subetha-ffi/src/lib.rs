//! `subetha-ffi`: the C ABI over SubEtha's memory-mapped primitives.
//!
//! The header this crate generates, `include/subetha.h`, is the surface a
//! C, C++, Go, Node or any other consumer builds against. Every entry point
//! returns `int32_t`: `SUBETHA_OK`, or a code `subetha_strerror` names, with
//! the specifics of a failure in a thread-local buffer that
//! `subetha_last_error_detail` copies out.
//!
//! Objects are named by 64-bit handles carrying an index and a generation,
//! so a stale or forged handle is refused rather than dereferenced. Each
//! object is created in strict mode, which starts no thread and writes
//! into caller buffers, or managed mode, which runs the background work
//! the Rust API runs; the process default is chosen at `subetha_init`.
//!
//! A panic inside any entry point is caught at the boundary: the call
//! returns `SUBETHA_E_PANIC` and the handle it ran on is poisoned until it
//! is destroyed. `subetha_init` is required first and `subetha_shutdown`
//! before the library is unloaded.
//!
//! The ABI is unstable until its major version reaches 1. The tiers of the
//! surface and their order are published in `C_ABI_TIERS.md` at the
//! repository root.

#![warn(missing_docs)]

pub mod arena;
pub mod atomic;
mod batch;
pub mod blocking_tcp_bridge;
mod epoch;
pub mod broadcast;
pub mod blocked_bloom;
pub mod bloom;
pub mod btree;
pub mod cell;
pub mod cms;
pub mod condvar;
pub mod epochs;
pub mod frame_region;
pub mod capacity;
pub mod capacity_broadcast;
pub mod capacity_pubsub;
pub mod error;
pub mod handle;
pub mod lamport;
pub mod laned_map;
pub mod leader;
pub mod list;
pub mod locale;
pub mod mpmc;
pub mod bit_vec;
pub mod hyper_log_log;
pub mod graph;
pub mod lru_cache;
pub mod nan_value;
pub mod time_point;
pub mod umbra_pointer;
pub mod universal;
pub mod k_tower;
pub mod qos_policy;
pub mod virtual_endpoint;
pub mod mpsc;
pub mod deque;
pub mod hashmap;
pub mod heartbeat;
pub mod histogram;
pub mod rate_limiter;
pub mod epoch_barrier;
pub mod fence_clock;
pub mod lazy_value;
pub mod shared_arc;
pub mod quic_bridge;
pub mod sens;
pub mod tcp_bridge;
pub mod topology;
pub mod waker;
pub mod holders;
mod holds;
pub mod notifier;
pub mod ordered;
pub mod owner_lease;
pub mod stack;
pub mod pubsub;
pub mod region;
pub mod reservoir;
pub mod ring;
pub mod runtime;
pub mod rwlock;
pub mod semaphore;
pub mod slab;
pub mod spsc;
pub mod vec;
pub mod versioned_chain;
pub mod versioned_map;
pub mod versioned_slab;
pub mod vyukov;
mod wait;

pub use arena::*;
pub use atomic::*;
pub use broadcast::*;
pub use blocked_bloom::*;
pub use blocking_tcp_bridge::*;
pub use bloom::*;
pub use btree::*;
pub use cell::*;
pub use cms::*;
pub use capacity::*;
pub use capacity_broadcast::*;
pub use capacity_pubsub::*;
pub use condvar::*;
pub use epochs::*;
pub use error::*;
pub use frame_region::*;
pub use handle::{
    subetha_handle, SUBETHA_HANDLE_NONE, SUBETHA_KIND_BROADCAST, SUBETHA_KIND_CAPACITY_BROADCAST,
    SUBETHA_KIND_CAPACITY_PUBSUB, SUBETHA_KIND_CAPACITY_RING, SUBETHA_KIND_CAPACITY_SUBSCRIBER,
    SUBETHA_KIND_LAMPORT_CONSUMER, SUBETHA_KIND_LAMPORT_PRODUCER, SUBETHA_KIND_LOCALE_RING,
    SUBETHA_KIND_MPMC_CONSUMER, SUBETHA_KIND_MPMC_PRODUCER, SUBETHA_KIND_MPSC_CONSUMER,
    SUBETHA_KIND_MPSC_PRODUCER, SUBETHA_KIND_ORDERED_RECEIVER, SUBETHA_KIND_PUBSUB, SUBETHA_KIND_RING,
    SUBETHA_KIND_SPSC, SUBETHA_KIND_SUBSCRIBER, SUBETHA_KIND_VYUKOV,
};
pub use lamport::*;
pub use laned_map::*;
pub use leader::*;
pub use list::*;
pub use locale::*;
pub use mpmc::*;
pub use mpsc::*;
pub use deque::*;
pub use hashmap::*;
pub use heartbeat::*;
pub use histogram::*;
pub use rate_limiter::*;
pub use epoch_barrier::*;
pub use fence_clock::*;
pub use lazy_value::*;
pub use shared_arc::*;
pub use quic_bridge::*;
pub use sens::*;
pub use tcp_bridge::*;
pub use topology::*;
pub use waker::*;
pub use holders::*;
pub use notifier::*;
pub use ordered::*;
pub use owner_lease::*;
pub use stack::*;
pub use pubsub::*;
pub use region::*;
pub use reservoir::*;
pub use ring::*;
pub use runtime::*;
pub use rwlock::*;
pub use semaphore::*;
pub use slab::*;
pub use spsc::*;
pub use vec::*;
pub use versioned_chain::*;
pub use versioned_map::*;
pub use versioned_slab::*;
pub use vyukov::*;
