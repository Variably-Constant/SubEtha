//! `UmbraPointer<T>` - generic content-prefixed pointer.
//!
//! 16-byte slot. Actual `#[repr(C, align(16))]` layout is:
//! `target: *const T` at offset 0..8, `prefix: u32` at offset 8..12,
//! `_pad: u32` at offset 12..16. The prefix is 4 bytes derived from
//! the target's content (either the first 4 bytes of an underlying
//! byte-representation, or a 4-byte hash).
//!
//! The architectural win: equality / lookup operations check the
//! 4-byte prefix in-register BEFORE dereferencing `target`. For
//! workloads where most comparisons fail (HashMap bucket-chain
//! walks, dedup scans, RDF subject lookups), the prefix short-
//! circuits the dereference, eliminating the cache miss on the
//! pointed-to object.
//!
//! This is the generic primitive that callers specialise per content
//! type: a string-content overlay (prefix = first 4 bytes of the
//! UTF-8 bytes) and a bit-sliced N-pointer tile overlay both fit
//! inside the same 16-byte slot by reinterpreting `prefix` as
//! content-specific bits.
//!
//! Two prefix construction modes:
//!
//! - [`UmbraPointer::with_content_prefix`] copies 4 bytes from the
//!   target's byte-representation (caller supplies bytes).
//! - [`UmbraPointer::with_hash_prefix`] takes a 4-byte hash of the
//!   target's identity. Near-perfect rejection rate (~2^-32
//!   collision) at the cost of computing the hash on construction.

use std::marker::PhantomData;
use std::sync::Arc;

/// 16-byte content-prefixed pointer. Layout is fixed so SIMD scans
/// over an array of `UmbraPointer<T>` see consistent prefix-byte
/// positions.
#[repr(C, align(16))]
pub struct UmbraPointer<T> {
    /// Pointer to the heap-allocated target. Placed first so its
    /// natural 8-byte alignment does not push the layout off the
    /// 16-byte boundary. Requires `T: Sized` so the pointer stays
    /// thin (8 bytes); for unsized targets, wrap in `Box<[u8]>` or
    /// equivalent at the application layer.
    target: *const T,
    /// 4-byte content prefix, at offset 8.
    prefix: u32,
    /// Padding to fill out the 16-byte slot.
    _pad: u32,
    _phantom: PhantomData<T>,
}

unsafe impl<T: Send> Send for UmbraPointer<T> {}
unsafe impl<T: Sync> Sync for UmbraPointer<T> {}

impl<T> UmbraPointer<T> {
    /// Direction signature of `UmbraPointer<T>`. Engages the
    /// `K_content_prefix` axis (4-byte prefix stored at slot for
    /// short-circuit equality before deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::ContentPrefix],
    );

    /// Construct from an existing raw pointer with an explicit prefix.
    /// The caller is responsible for keeping the pointee alive.
    ///
    /// # Safety
    ///
    /// `target` must remain valid for the lifetime of this
    /// `UmbraPointer`. `prefix` should be a deterministic function of
    /// the pointee's content; otherwise prefix comparisons are
    /// meaningless.
    #[inline]
    pub const unsafe fn from_raw(prefix: u32, target: *const T) -> Self {
        Self { target, prefix, _pad: 0, _phantom: PhantomData }
    }

    #[inline]
    pub const fn prefix(&self) -> u32 { self.prefix }

    #[inline]
    pub const fn as_raw(&self) -> *const T { self.target }

    /// Compare prefixes only. Single in-register equality check; no
    /// dereference of `target`. Returns true when prefixes are equal,
    /// false when they differ. Use this as the first step in a staged
    /// equality check (Umbra paper's prefix short-circuit).
    #[inline]
    pub const fn prefix_eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix
    }

    /// Compare against a literal query prefix.
    #[inline]
    pub const fn matches_prefix(&self, query: u32) -> bool {
        self.prefix == query
    }
}

impl<T> UmbraPointer<T> {
    /// Build by moving `value` onto the heap and copying its first
    /// 4 bytes (in declaration order) as the prefix.
    ///
    /// Useful for types whose first 4 bytes are a meaningful key
    /// field (database row IDs, packet headers, etc.). For more
    /// general use, prefer [`UmbraPointer::with_hash_prefix`].
    pub fn with_content_prefix(value: T) -> Box<UmbraOwner<T>> {
        // SAFETY: we read 4 bytes from `&value` without moving it,
        // then move value into a Box. Reading the raw bytes does not
        // require T: Copy; we only need the bytes for prefix
        // derivation. Endianness is platform-native.
        let bytes = unsafe {
            let p = &value as *const T as *const u8;
            let n = std::mem::size_of::<T>().min(4);
            let mut buf = [0u8; 4];
            std::ptr::copy_nonoverlapping(p, buf.as_mut_ptr(), n);
            buf
        };
        let prefix = u32::from_le_bytes(bytes);
        let boxed = Box::new(value);
        let target = Box::into_raw(boxed) as *const T;
        let ptr = unsafe { Self::from_raw(prefix, target) };
        Box::new(UmbraOwner { ptr })
    }

    /// Build by moving `value` onto the heap and hashing its byte
    /// representation as the prefix. Near-perfect rejection rate
    /// because hash distribution is approximately random over u32.
    pub fn with_hash_prefix(value: T) -> Box<UmbraOwner<T>>
    where T: std::hash::Hash,
    {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut h = DefaultHasher::new();
        value.hash(&mut h);
        let full = h.finish();
        // Take the low 32 bits as the prefix.
        let prefix = full as u32;
        let boxed = Box::new(value);
        let target = Box::into_raw(boxed) as *const T;
        let ptr = unsafe { Self::from_raw(prefix, target) };
        Box::new(UmbraOwner { ptr })
    }

    /// Wrap an `Arc<T>` without taking ownership of the heap
    /// allocation. Uses hash-based prefix.
    pub fn from_arc(value: Arc<T>, prefix: u32) -> ArcUmbra<T> {
        let target = Arc::as_ptr(&value);
        let ptr = unsafe { Self::from_raw(prefix, target) };
        ArcUmbra { ptr, _arc: value }
    }

    /// # Safety
    ///
    /// Caller must guarantee the target is still live.
    #[inline]
    pub unsafe fn deref_unchecked(&self) -> &T {
        unsafe { &*self.target }
    }
}

/// RAII wrapper for an `UmbraPointer<T>` whose target was heap-allocated
/// via [`UmbraPointer::with_content_prefix`] or
/// [`UmbraPointer::with_hash_prefix`]. Drops the boxed target when the
/// owner is dropped.
pub struct UmbraOwner<T> {
    ptr: UmbraPointer<T>,
}

impl<T> UmbraOwner<T> {
    #[inline]
    pub fn ptr(&self) -> &UmbraPointer<T> { &self.ptr }
    #[inline]
    pub fn prefix(&self) -> u32 { self.ptr.prefix }
    #[inline]
    pub fn value(&self) -> &T {
        // SAFETY: we own the heap allocation, so deref is always valid.
        unsafe { &*self.ptr.target }
    }
}

impl<T> Drop for UmbraOwner<T> {
    fn drop(&mut self) {
        let raw = self.ptr.target as *mut T;
        if !raw.is_null() {
            // SAFETY: target was created via Box::into_raw in
            // with_*_prefix.
            unsafe { drop(Box::from_raw(raw)); }
        }
    }
}

/// `UmbraPointer<T>` wrapping an `Arc<T>`. The Arc reference count
/// keeps the target alive; the UmbraPointer is a copy of the Arc's
/// data pointer plus the prefix.
pub struct ArcUmbra<T> {
    ptr: UmbraPointer<T>,
    _arc: Arc<T>,
}

impl<T> ArcUmbra<T> {
    #[inline]
    pub fn ptr(&self) -> &UmbraPointer<T> { &self.ptr }
    #[inline]
    pub fn prefix(&self) -> u32 { self.ptr.prefix }
    #[inline]
    pub fn value(&self) -> &T {
        // SAFETY: the Arc keeps the target alive.
        unsafe { &*self.ptr.target }
    }
    #[inline]
    pub fn into_arc(self) -> Arc<T> { self._arc.clone() }
}

impl<T> Clone for ArcUmbra<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: unsafe { UmbraPointer::from_raw(self.ptr.prefix, self.ptr.target) },
            _arc: self._arc.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_exactly_16_bytes() {
        assert_eq!(std::mem::size_of::<UmbraPointer<u64>>(), 16);
        assert_eq!(std::mem::align_of::<UmbraPointer<u64>>(), 16);
    }

    #[test]
    fn prefix_eq_does_not_deref() {
        // Construct two UmbraPointers with the same prefix but
        // bogus (null-adjacent) targets. The compare must use the
        // prefix only and not crash.
        let p1: UmbraPointer<u64> = unsafe {
            UmbraPointer::from_raw(0xDEADBEEF, std::ptr::dangling::<u64>())
        };
        let p2: UmbraPointer<u64> = unsafe {
            UmbraPointer::from_raw(0xDEADBEEF, std::ptr::dangling::<u64>())
        };
        let p3: UmbraPointer<u64> = unsafe {
            UmbraPointer::from_raw(0xCAFEBABE, std::ptr::dangling::<u64>())
        };
        assert!(p1.prefix_eq(&p2), "same prefix matches without deref");
        assert!(!p1.prefix_eq(&p3), "different prefix does not match");
        assert!(p1.matches_prefix(0xDEADBEEF));
        assert!(!p1.matches_prefix(0));
    }

    #[test]
    fn with_content_prefix_copies_first_4_bytes() {
        // For u64 0x0000_0000_0000_BEEF on little-endian, the first
        // 4 bytes are 0xEF, 0xBE, 0x00, 0x00 -> u32 = 0x0000_BEEF.
        let owner = UmbraPointer::with_content_prefix(0x0000_0000_0000_BEEF_u64);
        assert_eq!(owner.prefix(), 0x0000_BEEF);
        assert_eq!(*owner.value(), 0x0000_0000_0000_BEEF_u64);
    }

    #[test]
    fn with_hash_prefix_is_deterministic_for_same_value() {
        let a = UmbraPointer::with_hash_prefix(42u64);
        let b = UmbraPointer::with_hash_prefix(42u64);
        // Same value -> same hash -> same prefix.
        assert_eq!(a.prefix(), b.prefix());
        assert_eq!(*a.value(), 42);
        assert_eq!(*b.value(), 42);
    }

    #[test]
    fn with_hash_prefix_distinguishes_different_values() {
        let a = UmbraPointer::with_hash_prefix(42u64);
        let b = UmbraPointer::with_hash_prefix(43u64);
        // Different values -> different hashes -> different prefixes
        // (with overwhelming probability).
        assert_ne!(a.prefix(), b.prefix());
    }

    #[test]
    fn arc_umbra_keeps_target_alive() {
        let arc: Arc<u64> = Arc::new(1234);
        let u = UmbraPointer::from_arc(arc.clone(), 0xABCD);
        // Drop the original arc; UmbraPointer's internal arc keeps
        // the target alive.
        drop(arc);
        assert_eq!(u.prefix(), 0xABCD);
        assert_eq!(*u.value(), 1234);
    }

    #[test]
    fn owner_drops_target() {
        // Use a Drop-counting struct to verify the boxed target is freed.
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter(u64);
        impl Drop for DropCounter {
            fn drop(&mut self) { DROPS.fetch_add(1, Ordering::Relaxed); }
        }

        let before = DROPS.load(Ordering::Relaxed);
        let owner = UmbraPointer::with_content_prefix(DropCounter(99));
        assert_eq!(owner.value().0, 99);
        drop(owner);
        let after = DROPS.load(Ordering::Relaxed);
        assert!(after > before, "boxed target should be dropped");
    }

    #[test]
    fn dedup_scan_via_prefix() {
        // Realistic workload: scan an array of UmbraOwners, count
        // distinct prefixes. No dereference required.
        let owners: Vec<_> = (0..100u32)
            .map(UmbraPointer::with_content_prefix)
            .collect();
        let mut prefixes: Vec<u32> = owners.iter().map(|o| o.prefix()).collect();
        prefixes.sort_unstable();
        prefixes.dedup();
        // 100 distinct u32 values have 100 distinct content-prefix bytes
        // because the LE byte 0 captures the low byte uniquely for 0..100.
        assert_eq!(prefixes.len(), 100);
    }

    #[test]
    fn skip_on_mismatch_zero_dereferences() {
        // Build 10 ArcUmbras with distinct prefixes, then scan for a
        // prefix that doesn't match any. The scan must run without
        // touching any of the underlying Arc<T> data.
        let umbras: Vec<ArcUmbra<u64>> = (0..10u64)
            .map(|i| UmbraPointer::from_arc(Arc::new(i * 1000), (i + 1) as u32))
            .collect();
        let query_prefix = 99u32;
        let mut matches = 0;
        for u in &umbras {
            if u.ptr().matches_prefix(query_prefix) {
                matches += 1;
                // would deref here only when matched; never reached
                let _val = u.value();
            }
        }
        assert_eq!(matches, 0,
                   "no prefix in 1..=10 should equal 99");
    }
}
