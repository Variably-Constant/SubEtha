//! The `Marshal` trait - the type-system contract for "this value can
//! cross an address-space boundary byte-identically."
//!
//! `Marshal` is strictly stronger than `Send`. A `Send` value can
//! travel between threads inside one process, where pointers and
//! references mean the same thing in both threads. A `Marshal` value
//! can travel between *processes* (or be serialised to disk and read
//! back), where pointers into the originating process's heap, file
//! descriptors, and any other resource handle that means different
//! things in different address spaces are forbidden.
//!
//! # The contract
//!
//! - [`Marshal::PAYLOAD_BYTES`] is the exact byte width of the
//!   marshalled form.
//! - [`Marshal::marshal`] writes exactly `PAYLOAD_BYTES` into a
//!   caller-supplied buffer.
//! - [`Marshal::unmarshal`] reads exactly `PAYLOAD_BYTES` and
//!   reconstructs a value byte-identical to the original.
//! - Round-tripping: `unmarshal(&buf)` after `marshal(&v, &mut buf)`
//!   produces a value indistinguishable from `v` for every value `v`.
//!
//! # Why `unsafe`
//!
//! Correctness depends on every reachable byte of the value being
//! position-independent across address spaces. The compiler cannot
//! check this for arbitrary user types - a type with an inner
//! `Box<u8>` plus a manual `marshal` impl that copies the box's
//! *raw bytes* compiles cleanly and crashes at runtime. The trait is
//! therefore `unsafe` to implement; the implementer asserts the
//! contract holds.
//!
//! # Auto-impls
//!
//! Safe blanket impls are provided for the primitive integer and
//! floating-point types, `bool`, `()`, and `[T; N]` where `T:
//! Marshal`. These cover the common case (move a `u64` job ID, an
//! `[u8; 48]` argument blob, a `(u32, u32)` pair) without requiring
//! any unsafe code at the call site.
//!
//! # Connection to `pass_registry`
//!
//! `subetha_cxc::pass_registry` solves the same problem (closures
//! that need to execute in another process) at the *runtime* layer,
//! by registering closure handlers by integer ID. `Marshal` is the
//! *compile-time* counterpart: a closure whose captured environment
//! reduces to a `Marshal` payload can be shipped across processes by
//! marshalling the payload and looking up the handler by ID. Both
//! layers cooperate to make cross-process execution byte-safe.

use core::fmt;

/// Error returned when [`Marshal::unmarshal`] cannot reconstruct a
/// value from the source buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarshalError {
    /// The source buffer is shorter than `PAYLOAD_BYTES`.
    ShortBuffer { expected: usize, got: usize },
    /// The source bytes do not encode a valid value of this type
    /// (e.g. a `bool` byte that is neither 0 nor 1).
    InvalidEncoding,
}

impl fmt::Display for MarshalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShortBuffer { expected, got } => {
                write!(f, "Marshal source buffer too short: expected {expected} bytes, got {got}")
            }
            Self::InvalidEncoding => write!(f, "Marshal source bytes do not encode a valid value"),
        }
    }
}

impl std::error::Error for MarshalError {}

/// Type-system contract for "this value can be flattened into a
/// fixed-size byte payload and reconstructed byte-identically in
/// another address space."
///
/// See the [module docs](self) for the full contract.
///
/// # Safety
///
/// Implementer asserts that:
/// - [`marshal`](Self::marshal) writes exactly `PAYLOAD_BYTES` into
///   the destination buffer.
/// - [`unmarshal`](Self::unmarshal) reads exactly `PAYLOAD_BYTES`
///   from the source buffer.
/// - Round-tripping is byte-identical and value-identical for every
///   valid value of the type.
/// - The marshalled bytes contain NO pointers, references, file
///   descriptors, or other handles that mean different things in
///   different address spaces.
pub unsafe trait Marshal: Sized {
    /// Exact byte width of the marshalled form.
    const PAYLOAD_BYTES: usize;

    /// Write the marshalled form of `self` into `dst`.
    ///
    /// `dst.len()` must be at least `PAYLOAD_BYTES`; implementations
    /// write to `dst[..PAYLOAD_BYTES]` and leave any remaining bytes
    /// unmodified. Panics on a short buffer.
    fn marshal(&self, dst: &mut [u8]);

    /// Read the marshalled form from `src` and reconstruct the value.
    ///
    /// `src.len()` must be at least `PAYLOAD_BYTES`; implementations
    /// read from `src[..PAYLOAD_BYTES]`. Returns
    /// [`MarshalError::ShortBuffer`] on a short buffer and
    /// [`MarshalError::InvalidEncoding`] when the bytes do not encode
    /// a valid value (e.g. an out-of-range discriminant).
    fn unmarshal(src: &[u8]) -> Result<Self, MarshalError>;
}

// ---------------------------------------------------------------
// Primitive impls. Each is sound because the type's bytes are
// position-independent: an integer's bit pattern means the same
// thing in every address space.
// ---------------------------------------------------------------

macro_rules! impl_marshal_for_primitive {
    ($t:ty, $bytes:expr) => {
        // SAFETY: $t has no internal pointers or handles; its raw
        // bytes are position-independent across address spaces.
        // Little-endian encoding is canonical and stable.
        unsafe impl Marshal for $t {
            const PAYLOAD_BYTES: usize = $bytes;

            fn marshal(&self, dst: &mut [u8]) {
                let bytes = self.to_le_bytes();
                dst[..$bytes].copy_from_slice(&bytes);
            }

            fn unmarshal(src: &[u8]) -> Result<Self, MarshalError> {
                if src.len() < $bytes {
                    return Err(MarshalError::ShortBuffer {
                        expected: $bytes,
                        got: src.len(),
                    });
                }
                let mut buf = [0u8; $bytes];
                buf.copy_from_slice(&src[..$bytes]);
                Ok(<$t>::from_le_bytes(buf))
            }
        }
    };
}

impl_marshal_for_primitive!(u8, 1);
impl_marshal_for_primitive!(u16, 2);
impl_marshal_for_primitive!(u32, 4);
impl_marshal_for_primitive!(u64, 8);
impl_marshal_for_primitive!(u128, 16);
impl_marshal_for_primitive!(i8, 1);
impl_marshal_for_primitive!(i16, 2);
impl_marshal_for_primitive!(i32, 4);
impl_marshal_for_primitive!(i64, 8);
impl_marshal_for_primitive!(i128, 16);
impl_marshal_for_primitive!(f32, 4);
impl_marshal_for_primitive!(f64, 8);

// SAFETY: bool's two valid bit patterns are 0 and 1; the encoding
// rejects any other byte.
unsafe impl Marshal for bool {
    const PAYLOAD_BYTES: usize = 1;
    fn marshal(&self, dst: &mut [u8]) {
        dst[0] = u8::from(*self);
    }
    fn unmarshal(src: &[u8]) -> Result<Self, MarshalError> {
        if src.is_empty() {
            return Err(MarshalError::ShortBuffer { expected: 1, got: 0 });
        }
        match src[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(MarshalError::InvalidEncoding),
        }
    }
}

// SAFETY: () has no bytes.
unsafe impl Marshal for () {
    const PAYLOAD_BYTES: usize = 0;
    fn marshal(&self, _dst: &mut [u8]) {}
    fn unmarshal(_src: &[u8]) -> Result<Self, MarshalError> { Ok(()) }
}

// SAFETY: an array of Marshal is Marshal: the concatenation of each
// element's bytes is position-independent if every element is.
unsafe impl<T: Marshal + Copy + Default, const N: usize> Marshal for [T; N] {
    const PAYLOAD_BYTES: usize = T::PAYLOAD_BYTES * N;
    fn marshal(&self, dst: &mut [u8]) {
        for (i, item) in self.iter().enumerate() {
            let off = i * T::PAYLOAD_BYTES;
            item.marshal(&mut dst[off..off + T::PAYLOAD_BYTES]);
        }
    }
    fn unmarshal(src: &[u8]) -> Result<Self, MarshalError> {
        let need = T::PAYLOAD_BYTES * N;
        if src.len() < need {
            return Err(MarshalError::ShortBuffer { expected: need, got: src.len() });
        }
        let mut out = [T::default(); N];
        for (i, slot) in out.iter_mut().enumerate() {
            let off = i * T::PAYLOAD_BYTES;
            *slot = T::unmarshal(&src[off..off + T::PAYLOAD_BYTES])?;
        }
        Ok(out)
    }
}

// SAFETY: tuple of two Marshal values is Marshal by component-wise
// concatenation; same argument as the array impl.
unsafe impl<A: Marshal, B: Marshal> Marshal for (A, B) {
    const PAYLOAD_BYTES: usize = A::PAYLOAD_BYTES + B::PAYLOAD_BYTES;
    fn marshal(&self, dst: &mut [u8]) {
        self.0.marshal(&mut dst[..A::PAYLOAD_BYTES]);
        self.1.marshal(&mut dst[A::PAYLOAD_BYTES..A::PAYLOAD_BYTES + B::PAYLOAD_BYTES]);
    }
    fn unmarshal(src: &[u8]) -> Result<Self, MarshalError> {
        let need = A::PAYLOAD_BYTES + B::PAYLOAD_BYTES;
        if src.len() < need {
            return Err(MarshalError::ShortBuffer { expected: need, got: src.len() });
        }
        let a = A::unmarshal(&src[..A::PAYLOAD_BYTES])?;
        let b = B::unmarshal(&src[A::PAYLOAD_BYTES..need])?;
        Ok((a, b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: Marshal + PartialEq + std::fmt::Debug>(v: T) {
        let mut buf = vec![0u8; T::PAYLOAD_BYTES];
        v.marshal(&mut buf);
        let back = T::unmarshal(&buf).unwrap();
        assert_eq!(v, back);
    }

    #[test] fn u8_round_trip()   { round_trip(0u8); round_trip(255u8); }
    #[test] fn u32_round_trip()  { round_trip(0u32); round_trip(u32::MAX); round_trip(0xDEAD_BEEFu32); }
    #[test] fn u64_round_trip()  { round_trip(0u64); round_trip(u64::MAX); round_trip(0xCAFEBABE_DEADBEEFu64); }
    #[test] fn i64_round_trip()  { round_trip(i64::MIN); round_trip(0i64); round_trip(i64::MAX); }
    #[test] fn f64_round_trip()  { round_trip(0.0_f64); round_trip(-1.5_f64); round_trip(f64::INFINITY); }
    #[test] fn bool_round_trip() { round_trip(true); round_trip(false); }
    #[test] fn unit_round_trip() { round_trip(()); }

    #[test]
    fn array_round_trip() {
        round_trip([1u8, 2, 3, 4]);
        round_trip([0u64; 8]);
        round_trip([0xDEAD_BEEF_CAFE_BABE_u64, 0x1234_5678_9ABC_DEF0]);
    }

    #[test]
    fn tuple_round_trip() {
        round_trip((42u32, 7u64));
        round_trip((true, 99i32));
    }

    #[test]
    fn nested_array_in_tuple() {
        let v: (u32, [u8; 16]) = (0xCAFEBABE, [9; 16]);
        round_trip(v);
    }

    #[test]
    fn bool_rejects_invalid_byte() {
        match bool::unmarshal(&[42u8]) {
            Err(MarshalError::InvalidEncoding) => {}
            other => panic!("expected InvalidEncoding, got {other:?}"),
        }
    }

    #[test]
    fn short_buffer_rejected() {
        match u64::unmarshal(&[0u8; 3]) {
            Err(MarshalError::ShortBuffer { expected: 8, got: 3 }) => {}
            other => panic!("expected ShortBuffer{{expected:8,got:3}}, got {other:?}"),
        }
    }

    #[test]
    fn payload_bytes_constants_match_sizes() {
        assert_eq!(u8::PAYLOAD_BYTES, 1);
        assert_eq!(u32::PAYLOAD_BYTES, 4);
        assert_eq!(u64::PAYLOAD_BYTES, 8);
        assert_eq!(u128::PAYLOAD_BYTES, 16);
        assert_eq!(<[u64; 8]>::PAYLOAD_BYTES, 64);
        assert_eq!(<(u32, u64)>::PAYLOAD_BYTES, 12);
    }
}
