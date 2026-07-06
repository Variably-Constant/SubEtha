//! `SharedUmbraPointer<T>` - cross-process content-prefixed pointer.
//!
//! The cross-process lift of `subetha_pointers::UmbraPointer<T>`. The
//! mechanical change is one field swap:
//!
//! ```text
//! in-process:  target: *const T        (8 bytes, address-space-bound)
//! cross-proc:  target: OffsetPtr<T>    (4 bytes, byte-stable)
//! ```
//!
//! Everything else stays identical: 16-byte slot, u32 prefix at the
//! same offset, SIMD-friendly array layout for prefix-shortcircuit
//! scans, prefix derived from content (first 4 bytes or hash).
//!
//! # Why a separate primitive
//!
//! A `*const T` is process-local: it indexes the heap of the
//! constructing process. Writing one into an MMF and reading it from
//! another process gives a wild pointer. `OffsetPtr<T>` is an index
//! into a `SharedRegion<T>` - every process resolves it via its own
//! mapping's base pointer.
//!
//! # Pod-safety
//!
//! `SharedUmbraPointer<T>` is `Copy + repr(C, align(16))` with no
//! Drop side effects. It can live inside any other MMF container
//! (`SharedVec`, `SharedHashMap`, `SharedBTreeMap`, …) and be read
//! in any process holding the matching region.
//!
//! # Composition pattern
//!
//! ```text
//! SharedRegion<T>        owns the underlying T values
//! SharedVec<SharedUmbraPointer<T>>   stores prefix-prefixed handles
//! scan callers           filter by prefix in-register;
//!                         only on prefix match do they resolve
//!                         the OffsetPtr through the region
//! ```
//!
//! The architectural win is identical to the in-process Umbra: 95 %
//! of prefix mismatches reject without paying the cache miss to
//! load the underlying T from the region MMF.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use crate::shared_region::{OffsetPtr, RegionError, SharedRegion};

/// 16-byte cross-process content-prefixed pointer.
///
/// Layout is fixed and PoD so SIMD scans over an array see a stable
/// prefix-byte position.
///
/// ```text
/// offset 0   : OffsetPtr<T>     (u32 index; NIL = u32::MAX)
/// offset 4   : u32 prefix
/// offset 8   : u8 ext_tag       (0 = unset; 1..=255 = registered)
/// offset 9   : [u8; 7] ext_payload  (interpretation per tag)
/// offset 16  : end
/// ```
///
/// # User-addressable extension bytes
///
/// Bytes 8..16 are a TAG (1 byte) + PAYLOAD (7 bytes) that callers
/// can use to attach typed metadata to the pointer. Access via the
/// [`UmbraExtension`] trait + `set_ext` / `ext` methods:
///
/// - **Guard 1 (compile-time size)**: `set_ext<E>` and `ext<E>`
///   both monomorphize a const-assertion that
///   `size_of::<E>() <= 7`. Larger types fail to compile.
/// - **Guard 2 (runtime tag)**: each `UmbraExtension` declares a
///   unique `TAG: u8` constant. `ext<E>()` returns `None` if the
///   pointer's tag does not match `E::TAG`, preventing two
///   consumers from interpreting the same bytes differently.
/// - **Guard 3 (type bound)**: `E: Copy + 'static` ensures no
///   Drop side effects and no lifetimes to manage.
#[repr(C, align(16))]
#[derive(Debug)]
pub struct SharedUmbraPointer<T: Copy + 'static> {
    /// Index of the target slot in some `SharedRegion<T>`. The
    /// region itself is held by the caller; this pointer is just
    /// the cross-process-stable address.
    pub target: OffsetPtr<T>,
    /// 4-byte content prefix derived from the target's bytes (or a
    /// 4-byte hash). Constant for the lifetime of the pointer.
    pub prefix: u32,
    /// User extension tag. 0 means "no extension set"; non-zero
    /// values are caller-defined per `UmbraExtension::TAG`.
    ext_tag: u8,
    /// User extension payload. Interpretation depends on `ext_tag`.
    /// Access via `set_ext` / `ext` for typed safety.
    ext_payload: [u8; 7],
    _phantom: PhantomData<T>,
}

/// Marker trait for user-defined extension types stored in
/// SharedUmbraPointer's reserved bytes. Each implementor declares
/// a unique TAG so different consumers don't misinterpret each
/// other's payloads.
///
/// # Implementor responsibility
///
/// `TAG` MUST be globally unique across all `UmbraExtension`
/// implementations that may be present in the same shared memory.
/// Two implementations sharing a TAG value will silently
/// misinterpret each other's payloads. Reserve TAG values in your
/// application by registering them in a central location (e.g. a
/// doc comment listing claimed tags).
///
/// TAG 0 is reserved for "no extension set".
pub trait UmbraExtension: Copy + 'static {
    const TAG: u8;
}

/// Compile-time size guard. Monomorphization of `CHECK` triggers a
/// `const` assertion that the extension fits in 7 bytes.
struct ExtSizeCheck<E>(PhantomData<E>);
impl<E> ExtSizeCheck<E> {
    const CHECK: () = assert!(
        std::mem::size_of::<E>() <= 7,
        "UmbraExtension type must fit in 7 bytes (1 byte reserved for tag)",
    );
}

impl<T: Copy + 'static> Clone for SharedUmbraPointer<T> {
    fn clone(&self) -> Self { *self }
}
impl<T: Copy + 'static> Copy for SharedUmbraPointer<T> {}

impl<T: Copy + 'static> PartialEq for SharedUmbraPointer<T> {
    /// Full equality: same target AND same prefix. Use
    /// `prefix_eq` for the fast-path prefix-only check.
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target && self.prefix == other.prefix
    }
}
impl<T: Copy + 'static> Eq for SharedUmbraPointer<T> {}

impl<T: Copy + 'static> Default for SharedUmbraPointer<T> {
    /// NIL pointer with zero prefix. Zero-bytes representation,
    /// safe to write into freshly-zeroed MMF storage.
    fn default() -> Self { Self::NIL }
}

impl<T: Copy + 'static> SharedUmbraPointer<T> {
    /// Direction signature of `SharedUmbraPointer<T>`. Engages the
    /// `K_content_prefix` axis (4-byte prefix stored at slot for
    /// short-circuit equality before MMF deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::ContentPrefix],
    );

    /// NIL sentinel: target is `OffsetPtr::NIL` and prefix is 0;
    /// extension tag is 0 (unset). Equivalent to a freshly-zeroed
    /// 16-byte slot.
    pub const NIL: Self = Self {
        target: OffsetPtr::NIL,
        prefix: 0,
        ext_tag: 0,
        ext_payload: [0; 7],
        _phantom: PhantomData,
    };

    /// Construct from an existing region-allocated OffsetPtr and a
    /// caller-computed prefix. Extension is unset (tag=0).
    #[inline]
    pub const fn new(target: OffsetPtr<T>, prefix: u32) -> Self {
        Self {
            target, prefix,
            ext_tag: 0,
            ext_payload: [0; 7],
            _phantom: PhantomData,
        }
    }

    /// Write a typed extension. Sets the tag to `E::TAG` and copies
    /// the value bytes into the payload. Caller guarantees
    /// `E::TAG` is globally unique.
    pub fn set_ext<E: UmbraExtension>(&mut self, value: E) {
        // Monomorphization-time size check: fails to compile if
        // size_of::<E>() exceeds 7.
        let _check: () = ExtSizeCheck::<E>::CHECK;
        self.ext_tag = E::TAG;
        self.ext_payload = [0; 7];
        // SAFETY: E: Copy + 'static (no Drop, no lifetimes); we
        // write size_of::<E>() bytes (<= 7) into the 7-byte
        // payload. The cast to *const u8 is a standard byte-copy.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &value as *const E as *const u8,
                std::mem::size_of::<E>(),
            )
        };
        self.ext_payload[..bytes.len()].copy_from_slice(bytes);
    }

    /// Read a typed extension. Returns `None` if no extension is
    /// set (tag=0) OR if the stored tag does not match `E::TAG`.
    ///
    /// # Safety
    ///
    /// Even with tag validation, this is `unsafe` because the
    /// tag-uniqueness contract is on the caller. Two
    /// `UmbraExtension` implementations sharing a TAG value will
    /// silently misinterpret each other's payloads. The payload
    /// bytes must also be a valid representation of `E` (relevant
    /// for enums with restricted discriminants).
    pub unsafe fn ext<E: UmbraExtension>(&self) -> Option<E> {
        let _check: () = ExtSizeCheck::<E>::CHECK;
        if self.ext_tag == 0 || self.ext_tag != E::TAG {
            return None;
        }
        // SAFETY: tag validated; size compile-time-bounded;
        // E: Copy + 'static. Read first size_of::<E>() bytes from
        // payload as E.
        let mut buf = [0u8; 7];
        buf.copy_from_slice(&self.ext_payload);
        Some(unsafe { std::ptr::read(buf.as_ptr() as *const E) })
    }

    /// Clear the extension. Tag and payload set to 0.
    pub fn clear_ext(&mut self) {
        self.ext_tag = 0;
        self.ext_payload = [0; 7];
    }

    /// The current extension tag (0 = unset).
    #[inline]
    pub fn ext_tag(&self) -> u8 { self.ext_tag }

    /// Raw byte access to the extension payload. Use this for
    /// debugging or when interfacing with untyped consumers.
    #[inline]
    pub fn ext_payload_raw(&self) -> &[u8; 7] { &self.ext_payload }

    /// Allocate `value` in `region` and build a pointer whose prefix
    /// is the first 4 bytes of the in-memory representation of T
    /// (little-endian native). Useful when T's first bytes are a
    /// meaningful key field (row IDs, packet headers).
    pub fn from_region_alloc_content_prefix(
        region: &SharedRegion<T>, value: T,
    ) -> Result<Self, RegionError> {
        let prefix = content_prefix_of(&value);
        let ptr = region.allocate(value)?;
        Ok(Self::new(ptr, prefix))
    }

    /// Allocate `value` in `region` and build a pointer whose prefix
    /// is the low 32 bits of `std::hash::DefaultHasher` applied to
    /// `value`. Near-perfect rejection rate; requires `T: Hash`.
    pub fn from_region_alloc_hash_prefix(
        region: &SharedRegion<T>, value: T,
    ) -> Result<Self, RegionError>
    where T: Hash,
    {
        let prefix = hash_prefix_of(&value);
        let ptr = region.allocate(value)?;
        Ok(Self::new(ptr, prefix))
    }

    /// Allocate `value` in `region` and build a pointer with an
    /// explicit caller-supplied prefix.
    pub fn from_region_alloc(
        region: &SharedRegion<T>, value: T, prefix: u32,
    ) -> Result<Self, RegionError> {
        let ptr = region.allocate(value)?;
        Ok(Self::new(ptr, prefix))
    }

    /// True when target is NIL. Prefix may still be non-zero.
    #[inline]
    pub fn is_nil(&self) -> bool { self.target.is_nil() }

    /// Prefix-only comparison. Single in-register check; does NOT
    /// touch the region MMF. Use as the first step in a staged
    /// equality check.
    #[inline]
    pub fn prefix_eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix
    }

    /// Compare against a literal query prefix. Same semantics as
    /// `prefix_eq` against a constructed SharedUmbraPointer.
    #[inline]
    pub fn matches_prefix(&self, query: u32) -> bool {
        self.prefix == query
    }

    /// Resolve the target through `region`. Costs one MMF read.
    /// Only call after a successful prefix check unless you really
    /// need the value.
    pub fn resolve(&self, region: &SharedRegion<T>) -> Result<T, RegionError> {
        region.get(self.target)
    }
}

/// Compute the content prefix (first 4 bytes of T's in-memory
/// representation, padded with zero if T is smaller than 4 bytes).
#[inline]
fn content_prefix_of<T: Copy>(value: &T) -> u32 {
    let mut buf = [0u8; 4];
    let n = std::mem::size_of::<T>().min(4);
    unsafe {
        std::ptr::copy_nonoverlapping(
            value as *const T as *const u8,
            buf.as_mut_ptr(),
            n,
        );
    }
    u32::from_le_bytes(buf)
}

/// Compute the hash prefix (low 32 bits of DefaultHasher(value)).
#[inline]
fn hash_prefix_of<T: Hash>(value: &T) -> u32 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish() as u32
}

const _: () = {
    assert!(std::mem::size_of::<SharedUmbraPointer<u64>>() == 16);
    assert!(std::mem::align_of::<SharedUmbraPointer<u64>>() == 16);
};

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-umbra-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn layout_is_exactly_16_bytes() {
        assert_eq!(std::mem::size_of::<SharedUmbraPointer<u64>>(), 16);
        assert_eq!(std::mem::align_of::<SharedUmbraPointer<u64>>(), 16);
    }

    #[test]
    fn nil_is_all_zero() {
        let n: SharedUmbraPointer<u64> = SharedUmbraPointer::NIL;
        assert!(n.is_nil());
        assert_eq!(n.prefix, 0);
        let default: SharedUmbraPointer<u64> = SharedUmbraPointer::default();
        assert_eq!(default, n);
    }

    #[test]
    fn prefix_eq_does_not_touch_region() {
        // Two SharedUmbraPointers with the SAME prefix but
        // different (invalid) OffsetPtr indices. prefix_eq returns
        // true without resolving either target. matches_prefix
        // against the same query prefix likewise.
        let a: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(7), 0xDEAD_BEEF,
        );
        let b: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(99), 0xDEAD_BEEF,
        );
        let c: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(0), 0xCAFE_BABE,
        );
        assert!(a.prefix_eq(&b));
        assert!(!a.prefix_eq(&c));
        assert!(a.matches_prefix(0xDEAD_BEEF));
        assert!(!a.matches_prefix(0));
    }

    #[test]
    fn from_region_alloc_content_prefix_round_trip() {
        let p = tmp("content");
        let region: SharedRegion<u64> = SharedRegion::create(&p, 64).unwrap();
        let value: u64 = 0x0000_0000_0000_BEEF;
        let u = SharedUmbraPointer::from_region_alloc_content_prefix(
            &region, value,
        ).unwrap();
        // On little-endian: first 4 bytes of 0xBEEF = 0xEF 0xBE 0x00 0x00.
        assert_eq!(u.prefix, 0x0000_BEEF);
        assert_eq!(u.resolve(&region).unwrap(), value);
        drop(region);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn from_region_alloc_hash_prefix_is_deterministic() {
        let p = tmp("hash");
        let region: SharedRegion<u64> = SharedRegion::create(&p, 64).unwrap();
        let a = SharedUmbraPointer::from_region_alloc_hash_prefix(&region, 42u64).unwrap();
        let b = SharedUmbraPointer::from_region_alloc_hash_prefix(&region, 42u64).unwrap();
        // Same value → same prefix.
        assert_eq!(a.prefix, b.prefix);
        // Targets are different slots though.
        assert_ne!(a.target, b.target);
        let c = SharedUmbraPointer::from_region_alloc_hash_prefix(&region, 43u64).unwrap();
        // Different value → different prefix (with overwhelming probability).
        assert_ne!(a.prefix, c.prefix);
        drop(region);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn explicit_prefix_constructor() {
        let p = tmp("explicit");
        let region: SharedRegion<u64> = SharedRegion::create(&p, 64).unwrap();
        let u = SharedUmbraPointer::from_region_alloc(
            &region, 12345u64, 0x1234_5678,
        ).unwrap();
        assert_eq!(u.prefix, 0x1234_5678);
        assert_eq!(u.resolve(&region).unwrap(), 12345);
        drop(region);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn dedup_scan_via_prefix_zero_region_reads() {
        // Build 100 pointers with distinct prefixes. Scan for a
        // prefix that doesn't match any. The scan must touch only
        // the pointer array, never the region.
        let p = tmp("dedup");
        let region: SharedRegion<u64> = SharedRegion::create(&p, 256).unwrap();
        let pointers: Vec<SharedUmbraPointer<u64>> = (0..100u64)
            .map(|i| SharedUmbraPointer::from_region_alloc(
                &region, i * 1000, (i + 1) as u32,
            ).unwrap())
            .collect();
        let query = 999u32;
        let matches: Vec<_> = pointers.iter()
            .filter(|p| p.matches_prefix(query))
            .collect();
        assert!(matches.is_empty(), "no prefix in 1..=100 should equal 999");
        // Sanity: a prefix that DOES match resolves correctly.
        let hit: &SharedUmbraPointer<u64> = pointers.iter()
            .find(|p| p.matches_prefix(42))
            .expect("prefix 42 should exist (i=41)");
        assert_eq!(hit.resolve(&region).unwrap(), 41 * 1000);
        drop(region);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_process_via_separate_region_handles() {
        // Writer and reader open the same region; pointer values
        // (byte-identical) resolve through either handle.
        let p = tmp("cross");
        let writer_region: SharedRegion<u64> = SharedRegion::create(&p, 32).unwrap();
        let reader_region: SharedRegion<u64> = SharedRegion::open(&p, 32).unwrap();
        let u = SharedUmbraPointer::from_region_alloc(
            &writer_region, 7777u64, 0xABCD_EF01,
        ).unwrap();
        // The SharedUmbraPointer struct is Copy + Pod, so we can
        // pretend we ferried it through shared memory by literal
        // byte-copy. The destination MUST be aligned to align_of
        // SharedUmbraPointer<u64> (16 bytes); a plain `[u8; 16]`
        // has alignment 1 and would produce a misaligned read on
        // architectures that fault on unaligned u64 access. Use
        // MaybeUninit which inherits the destination type's
        // alignment requirement.
        let mut buf: std::mem::MaybeUninit<SharedUmbraPointer<u64>>
            = std::mem::MaybeUninit::uninit();
        // SAFETY: buf is the size_of::<SharedUmbraPointer<u64>>() == 16
        // bytes correctly aligned, fully owned, and writable. Source
        // is a valid SharedUmbraPointer<u64> by construction. The
        // copy initialises every byte of buf.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &u as *const SharedUmbraPointer<u64> as *const u8,
                buf.as_mut_ptr() as *mut u8,
                std::mem::size_of::<SharedUmbraPointer<u64>>(),
            );
        }
        // SAFETY: buf was fully initialised by the copy above; its
        // bytes are a valid SharedUmbraPointer<u64> (the trait is
        // Copy + has no Drop), so assume_init is sound.
        let recovered: SharedUmbraPointer<u64> = unsafe { buf.assume_init() };
        // Reader resolves the byte-recovered pointer through ITS
        // mapping of the same region file.
        assert_eq!(recovered.prefix, 0xABCD_EF01);
        assert_eq!(recovered.resolve(&reader_region).unwrap(), 7777);
        drop(writer_region);
        drop(reader_region);
        std::fs::remove_file(&p).ok();
    }

    // ============== Extension API tests ==============

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(C)]
    struct RegionId(u32);

    impl UmbraExtension for RegionId {
        const TAG: u8 = 1;
    }

    // Note: an 8-byte extension type like `struct MvccEpoch(u64)`
    // would fail the compile-time size guard
    // (ExtSizeCheck::<MvccEpoch>::CHECK fires the const_assert).
    // Tests use the 6-byte Epoch48 variant below to stay within
    // the 7-byte payload budget.

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[repr(C)]
    struct Epoch48([u8; 6]);  // 6 bytes - fits in 7
    impl UmbraExtension for Epoch48 {
        const TAG: u8 = 2;
    }

    #[test]
    fn ext_starts_unset() {
        let p: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(0), 0x1234_5678,
        );
        assert_eq!(p.ext_tag(), 0);
        assert_eq!(p.ext_payload_raw(), &[0u8; 7]);
    }

    #[test]
    fn set_ext_then_ext_round_trip() {
        let mut p: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(7), 0xABCD,
        );
        p.set_ext(RegionId(42));
        assert_eq!(p.ext_tag(), RegionId::TAG);
        let r: Option<RegionId> = unsafe { p.ext::<RegionId>() };
        assert_eq!(r, Some(RegionId(42)));
    }

    #[test]
    fn ext_returns_none_when_tag_mismatch() {
        let mut p: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(7), 0xABCD,
        );
        p.set_ext(RegionId(42));
        // Wrong type for the stored tag.
        let r: Option<Epoch48> = unsafe { p.ext::<Epoch48>() };
        assert_eq!(r, None,
            "ext::<Epoch48>() must return None when stored tag is RegionId::TAG");
    }

    #[test]
    fn ext_returns_none_when_unset() {
        let p: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(7), 0,
        );
        let r: Option<RegionId> = unsafe { p.ext::<RegionId>() };
        assert_eq!(r, None);
    }

    #[test]
    fn clear_ext_zeroes_tag_and_payload() {
        let mut p: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(7), 0,
        );
        p.set_ext(RegionId(123));
        assert_ne!(p.ext_tag(), 0);
        p.clear_ext();
        assert_eq!(p.ext_tag(), 0);
        assert_eq!(p.ext_payload_raw(), &[0u8; 7]);
    }

    #[test]
    fn set_ext_overwrites_previous_extension() {
        let mut p: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(7), 0,
        );
        p.set_ext(RegionId(1));
        p.set_ext(Epoch48([1, 2, 3, 4, 5, 6]));
        assert_eq!(p.ext_tag(), Epoch48::TAG);
        let r: Option<Epoch48> = unsafe { p.ext::<Epoch48>() };
        assert_eq!(r, Some(Epoch48([1, 2, 3, 4, 5, 6])));
        // Old RegionId is gone.
        let old: Option<RegionId> = unsafe { p.ext::<RegionId>() };
        assert_eq!(old, None);
    }

    #[test]
    fn ext_does_not_affect_prefix_or_target() {
        // Verify the extension bytes don't bleed into the
        // target/prefix fields of the layout.
        let mut p: SharedUmbraPointer<u64> = SharedUmbraPointer::new(
            OffsetPtr::new(42), 0xDEAD_BEEF,
        );
        p.set_ext(Epoch48([0xFF; 6]));
        assert_eq!(p.target, OffsetPtr::new(42));
        assert_eq!(p.prefix, 0xDEAD_BEEF);
    }

    #[test]
    fn pointers_fit_inside_shared_vec() {
        // Verify the canonical composition pattern: store an array
        // of SharedUmbraPointer<T> inside SharedVec, scan with
        // prefix filter, resolve only the matches.
        use crate::SharedVec;
        let p_region = tmp("compose-region");
        let p_vec = tmp("compose-vec");
        let region: SharedRegion<u64> = SharedRegion::create(&p_region, 256).unwrap();
        let pointers: SharedVec<SharedUmbraPointer<u64>> =
            SharedVec::create(&p_vec, 256).unwrap();
        for i in 0..50u64 {
            let u = SharedUmbraPointer::from_region_alloc_hash_prefix(
                &region, i * 10,
            ).unwrap();
            pointers.push_back(u).unwrap();
        }
        // Scan: for prefix p17 (the hash of 17 * 10 = 170), how
        // many entries should match?
        let target_prefix = hash_prefix_of(&170u64);
        let snap = pointers.snapshot();
        let hits: Vec<_> = snap.iter()
            .enumerate()
            .filter(|(_, u)| u.matches_prefix(target_prefix))
            .collect();
        // We may have zero or one collision; the resolved values
        // for hits must all be valid u64s from the region.
        for (_, u) in &hits {
            let v = u.resolve(&region).unwrap();
            assert!((0..500u64).step_by(10).any(|x| x == v));
        }
        drop(region);
        drop(pointers);
        std::fs::remove_file(&p_region).ok();
        std::fs::remove_file(&p_vec).ok();
    }
}
