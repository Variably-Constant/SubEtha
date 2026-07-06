//! CXC exotic pointer types.
//!
//! Each pointer type declares a `K_*` direction signature via
//! [`subetha_core::AxisMask`], so CXC's MMF dispatcher can route
//! workloads to the right pointer kind by signature containment.
//!
//! - [`umbra_pointer::UmbraPointer<T>`] for content-prefix
//! - [`bloom_pointer::BloomPointer<T>`] for probabilistic set summary
//! - [`kstep_pointer::KStepPointer<T>`] for log2-strided iteration
//! - [`k_tower_pointer::KTower2<T>`] / [`k_tower_pointer::KTower3<T>`]
//!   for multi-segment address space
//! - [`self_desc_pointer::SelfDescPointer<T>`] for type discriminant
//! - [`versioned_pointer::VersionedPointer<T>`] /
//!   [`versioned_pointer::HlcVersionedPointer<T>`] for version metadata
//! - [`cardinality_pointer::CardinalityPointer<T>`] for log2 cardinality
//! - [`adaptive_cheri_pointer::ReadableCapability<T>`] /
//!   [`adaptive_cheri_pointer::WritableCapability<T>`] for bounds-checked
//!   pointers on capability-aware silicon (CHERI / ARM Morello)
//! - [`adaptive_rasp_batch::RaspBatch<T>`] for SIMD-batched
//!   bounds + permission checks on x86 (the x86 sibling to CHERI;
//!   SoA layout + AVX2 / AVX-512F dispatch)
//!
//! ## Stable-Rust portable
//!
//! No nightly features. The pointer types are pure-Rust structs
//! over `*const T` / `*mut T` with content metadata.

pub mod adaptive_cheri_pointer;
pub mod adaptive_rasp_batch;
pub mod bloom_pointer;
pub mod cardinality_pointer;
pub mod k_tower_pointer;
pub mod kstep_pointer;
pub mod self_desc_pointer;
pub mod umbra_pointer;
pub mod versioned_pointer;
