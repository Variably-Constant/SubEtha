//! `SelfDescPointer<T>` - pointer carrying type ID + layout shape in
//! stolen high bits.
//!
//! Layout: a single `u64` with the high 16 bits stolen:
//! - bits 56..=63: `type_id` (u8)
//! - bits 53..=55: `layout` shape (3 bits, [`LayoutShape`])
//! - bits 0..=52:  the address (53-bit virtual; ample on x86_64 / Apple Silicon)
//!
//! The architectural win: a heterogeneous container of
//! `SelfDescPointer`s can dispatch on type WITHOUT a vtable lookup.
//! When the type universe is small enough to fit in 8 bits (256
//! distinct types), the dispatch is a switch on the high byte; the
//! compiler can compile this to a jump table inline at the call site.
//!
//! # Comparison vs Rust's existing options
//!
//! | Mechanism                    | Per-call cost | Type universe |
//! |------------------------------|---------------|---------------|
//! | `dyn Trait` vtable           | 1 indirect call | unbounded   |
//! | `enum` with variants         | tag-match     | bounded      |
//! | `SelfDescPointer<T>`         | switch on byte | <= 256       |
//!
//! The architectural shape mirrors JVM compressed-klass pointers
//! (where the klass is encoded in the top bits of an object ref)
//! but at a lighter weight: only 8 bits + 3 layout-shape bits
//! stolen, vs JVM's full 32-bit compressed klass.
//!
//! # Bit budget
//!
//! - 8 bits for type ID: 256 distinct types in the universe. For
//!   wider universes use `Box<dyn Trait>` or an enum.
//! - 3 bits for layout shape: 8 shapes covered by [`LayoutShape`].
//! - 53 bits for address: 8 PiB virtual memory; well above any
//!   current process.

use std::marker::PhantomData;

/// Address mask: low 53 bits.
pub const ADDR_MASK: u64 = (1u64 << 53) - 1;
/// Shape field: 3 bits at positions 53..56.
pub const SHAPE_SHIFT: u32 = 53;
pub const SHAPE_MASK: u64 = 0b111 << SHAPE_SHIFT;
/// Type ID field: 8 bits at positions 56..64.
pub const TYPE_SHIFT: u32 = 56;
pub const TYPE_MASK: u64 = 0xFFu64 << TYPE_SHIFT;

/// Layout shape encoded in 3 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LayoutShape {
    /// Single scalar value.
    Scalar = 0,
    /// Fixed-size array; length is implicit in the type ID.
    FixedArray = 1,
    /// Ragged / variable-length array (`Vec`-like).
    RaggedArray = 2,
    /// Tree node (recursive structure).
    Tree = 3,
    /// Graph node (cyclic references).
    Graph = 4,
    /// Hash table bucket.
    HashBucket = 5,
    /// Sparse / null-able slot.
    Sparse = 6,
    /// Reserved for caller-defined extensions.
    UserDefined = 7,
}

impl LayoutShape {
    pub fn from_bits(b: u8) -> Self {
        match b & 0b111 {
            0 => Self::Scalar,
            1 => Self::FixedArray,
            2 => Self::RaggedArray,
            3 => Self::Tree,
            4 => Self::Graph,
            5 => Self::HashBucket,
            6 => Self::Sparse,
            _ => Self::UserDefined,
        }
    }
}

/// 8-byte pointer with (type_id, layout_shape, address) packed.
#[repr(transparent)]
pub struct SelfDescPointer<T> {
    raw: u64,
    _phantom: PhantomData<*const T>,
}

unsafe impl<T: Send> Send for SelfDescPointer<T> {}
unsafe impl<T: Sync> Sync for SelfDescPointer<T> {}

impl<T> SelfDescPointer<T> {
    /// Direction signature of `SelfDescPointer<T>`. Engages the
    /// `K_type_tag` axis (type-id + layout-shape discriminant
    /// stored at slot for runtime type dispatch without a vtable
    /// indirection).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::TypeTag],
    );

    /// Construct from raw pointer + type ID + layout shape.
    ///
    /// # Safety
    ///
    /// `target` must fit in 53 bits (canonical 48-bit address is
    /// always fine; the top 11 bits MUST be zero). Caller must keep
    /// the target alive for the lifetime of this pointer.
    pub unsafe fn from_raw(target: *const T, type_id: u8, shape: LayoutShape) -> Self {
        let addr = target as u64;
        debug_assert!(
            addr & !ADDR_MASK == 0,
            "address {addr:#x} has bits set above 53-bit boundary"
        );
        let raw = ((type_id as u64) << TYPE_SHIFT)
            | ((shape as u64) << SHAPE_SHIFT)
            | (addr & ADDR_MASK);
        Self { raw, _phantom: PhantomData }
    }

    /// The address, with type and layout bits masked off.
    #[inline]
    pub fn as_raw(&self) -> *const T {
        (self.raw & ADDR_MASK) as *const T
    }

    /// Encoded type ID (0..=255).
    #[inline]
    pub const fn type_id(&self) -> u8 {
        (self.raw >> TYPE_SHIFT) as u8
    }

    /// Encoded layout shape.
    #[inline]
    pub fn layout_shape(&self) -> LayoutShape {
        LayoutShape::from_bits(((self.raw >> SHAPE_SHIFT) & 0b111) as u8)
    }

    /// Raw u64 packing for serialization or fast compare.
    #[inline]
    pub const fn raw(&self) -> u64 { self.raw }

    /// Update type_id in place.
    pub fn set_type_id(&mut self, new_id: u8) {
        self.raw = (self.raw & !TYPE_MASK) | ((new_id as u64) << TYPE_SHIFT);
    }

    /// Update layout_shape in place.
    pub fn set_layout_shape(&mut self, new_shape: LayoutShape) {
        self.raw = (self.raw & !SHAPE_MASK) | ((new_shape as u64) << SHAPE_SHIFT);
    }
}

impl<T> Clone for SelfDescPointer<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for SelfDescPointer<T> {}

impl<T> std::fmt::Debug for SelfDescPointer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SelfDescPointer {{ addr: {:#x}, type_id: {}, shape: {:?} }}",
               self.raw & ADDR_MASK, self.type_id(), self.layout_shape())
    }
}

impl<T> PartialEq for SelfDescPointer<T> {
    fn eq(&self, other: &Self) -> bool { self.raw == other.raw }
}
impl<T> Eq for SelfDescPointer<T> {}
impl<T> std::hash::Hash for SelfDescPointer<T> {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) { self.raw.hash(s); }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_8_bytes() {
        assert_eq!(std::mem::size_of::<SelfDescPointer<u64>>(), 8);
        assert_eq!(std::mem::align_of::<SelfDescPointer<u64>>(), 8);
    }

    #[test]
    fn type_id_round_trips() {
        let p = unsafe {
            SelfDescPointer::<u64>::from_raw(0x1FFF_FFFF as *const u64, 42, LayoutShape::Scalar)
        };
        assert_eq!(p.type_id(), 42);
        assert_eq!(p.layout_shape(), LayoutShape::Scalar);
    }

    #[test]
    fn address_round_trips_under_53_bit_mask() {
        let addr: *const u64 = 0x0001_2345_6789_ABCD as *const u64;
        // 0x0001_2345_6789_ABCD = 0b1_0010_0011_0100_0101_0110_0111_1000_1001_1010_1011_1100_1101
        // That is 49 bits set; bit 48 is set; bit 53 is NOT set.
        // Let's verify it fits in 53 bits.
        let addr_u64 = addr as u64;
        assert_eq!(addr_u64 & !ADDR_MASK, 0, "test address fits in 53 bits");
        let p = unsafe { SelfDescPointer::from_raw(addr, 7, LayoutShape::Tree) };
        assert_eq!(p.as_raw(), addr);
        assert_eq!(p.type_id(), 7);
        assert_eq!(p.layout_shape(), LayoutShape::Tree);
    }

    #[test]
    fn each_layout_shape_round_trips() {
        for shape in [
            LayoutShape::Scalar,
            LayoutShape::FixedArray,
            LayoutShape::RaggedArray,
            LayoutShape::Tree,
            LayoutShape::Graph,
            LayoutShape::HashBucket,
            LayoutShape::Sparse,
            LayoutShape::UserDefined,
        ] {
            let p = unsafe {
                SelfDescPointer::<u64>::from_raw(std::ptr::dangling::<u64>(), 0, shape)
            };
            assert_eq!(p.layout_shape(), shape, "shape {shape:?} must round-trip");
        }
    }

    #[test]
    fn set_type_id_preserves_address_and_shape() {
        let mut p = unsafe {
            SelfDescPointer::<u64>::from_raw(0xCAFE as *const u64, 10, LayoutShape::HashBucket)
        };
        p.set_type_id(99);
        assert_eq!(p.type_id(), 99);
        assert_eq!(p.layout_shape(), LayoutShape::HashBucket);
        assert_eq!(p.as_raw() as u64, 0xCAFE);
    }

    #[test]
    fn set_layout_preserves_address_and_type() {
        let mut p = unsafe {
            SelfDescPointer::<u64>::from_raw(0xBEEF as *const u64, 33, LayoutShape::Scalar)
        };
        p.set_layout_shape(LayoutShape::Graph);
        assert_eq!(p.layout_shape(), LayoutShape::Graph);
        assert_eq!(p.type_id(), 33);
        assert_eq!(p.as_raw() as u64, 0xBEEF);
    }

    #[test]
    fn heterogeneous_dispatch_without_vtable() {
        // Build a Vec of SelfDescPointers with varying type IDs.
        // Dispatch on type_id without a vtable lookup.
        let pointers = vec![
            unsafe { SelfDescPointer::<u8>::from_raw(std::ptr::dangling::<u8>(), 1, LayoutShape::Scalar) },
            unsafe { SelfDescPointer::<u8>::from_raw(0x2 as *const u8, 2, LayoutShape::FixedArray) },
            unsafe { SelfDescPointer::<u8>::from_raw(0x3 as *const u8, 1, LayoutShape::Scalar) },
            unsafe { SelfDescPointer::<u8>::from_raw(0x4 as *const u8, 3, LayoutShape::Tree) },
        ];
        let mut t1 = 0;
        let mut t2 = 0;
        let mut t3 = 0;
        for p in &pointers {
            match p.type_id() {
                1 => t1 += 1,
                2 => t2 += 1,
                3 => t3 += 1,
                _ => {}
            }
        }
        assert_eq!((t1, t2, t3), (2, 1, 1));
    }
}
