//! Integration test asserting that every exotic MMF-backed pointer
//! in `subetha-cxc` declares a non-empty `SIGNATURE: AxisMask`.
//!
//! The signature catalog lets a future `MmfDispatcher` route by
//! `provided.satisfies(required)` containment.

use subetha_core::{Axis, AxisMask};
use subetha_cxc::{SharedAsyncPointer, SharedUmbraPointer};

#[test]
fn shared_async_pointer_engages_async_axis() {
    let sig = SharedAsyncPointer::<u8>::SIGNATURE;
    assert_ne!(sig, AxisMask::EMPTY);
    assert!(sig.contains(Axis::Async));
}

#[test]
fn shared_umbra_pointer_engages_content_prefix_axis() {
    let sig = SharedUmbraPointer::<u32>::SIGNATURE;
    assert_ne!(sig, AxisMask::EMPTY);
    assert!(sig.contains(Axis::ContentPrefix));
}

#[test]
fn signatures_distinguish_async_from_umbra() {
    assert_ne!(
        SharedAsyncPointer::<u8>::SIGNATURE,
        SharedUmbraPointer::<u32>::SIGNATURE
    );
}
