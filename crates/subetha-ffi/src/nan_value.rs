//! NaN-boxed values through the C ABI: one 64-bit word holding a double,
//! a 32-bit integer, a boolean, nil, or an offset pointer, told apart by
//! the bit pattern a double reserves for its quiet NaNs.
//!
//! Unlike every other family here there is no handle and nothing shared.
//! A value is a `uint64_t` the caller holds, copies and stores wherever
//! it likes; these entry points only pack and unpack it. So none of them
//! can fail on a handle, none takes a mode, and `subetha_init` is not
//! required before calling them.
//!
//! Reading a value as the wrong type answers false and leaves the output
//! untouched rather than returning a number from the wrong bits, so a
//! caller that forgets to check the type gets no answer instead of a
//! plausible wrong one.

use subetha_cxc::shared_nan_tagged_value::SharedNaNTaggedValue;
use subetha_cxc::shared_nan_value::{NaNValueType, SharedNaNValue};

/// The word holds a double.
pub const SUBETHA_NAN_F64: u32 = 0;
/// The word holds nil.
pub const SUBETHA_NAN_NIL: u32 = 1;
/// The word holds a signed 32-bit integer.
pub const SUBETHA_NAN_I32: u32 = 2;
/// The word holds an unsigned 32-bit integer.
pub const SUBETHA_NAN_U32: u32 = 3;
/// The word holds a boolean.
pub const SUBETHA_NAN_BOOL: u32 = 4;
/// The word holds an offset pointer.
pub const SUBETHA_NAN_OFFSET_PTR: u32 = 5;
/// The word holds a tag this library does not name. A caller reading one
/// is talking to a newer writer and should leave the value alone rather
/// than guess at it.
pub const SUBETHA_NAN_RESERVED: u32 = 6;

/// The nil value.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_nil() -> u64 {
    SharedNaNValue::NIL.raw()
}

/// Pack a double.
///
/// A NaN going in comes back as the one canonical quiet NaN, so its bits
/// can never be mistaken for a boxed value. That means a caller cannot
/// round-trip a particular NaN payload through here: every NaN is the
/// same NaN afterwards.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_from_f64(value: f64) -> u64 {
    SharedNaNValue::from_f64(value).raw()
}

/// Pack a signed 32-bit integer.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_from_i32(value: i32) -> u64 {
    SharedNaNValue::from_i32(value).raw()
}

/// Pack an unsigned 32-bit integer.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_from_u32(value: u32) -> u64 {
    SharedNaNValue::from_u32(value).raw()
}

/// Pack a boolean.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_from_bool(value: bool) -> u64 {
    SharedNaNValue::from_bool(value).raw()
}

/// Which of the `SUBETHA_NAN_` kinds `value` holds.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_type(value: u64) -> u32 {
    match SharedNaNValue::from_raw(value).type_tag() {
        NaNValueType::F64 => SUBETHA_NAN_F64,
        NaNValueType::Nil => SUBETHA_NAN_NIL,
        NaNValueType::I32 => SUBETHA_NAN_I32,
        NaNValueType::U32 => SUBETHA_NAN_U32,
        NaNValueType::Bool => SUBETHA_NAN_BOOL,
        NaNValueType::OffsetPtr => SUBETHA_NAN_OFFSET_PTR,
        NaNValueType::Reserved(_) => SUBETHA_NAN_RESERVED,
    }
}

/// Read `value` as a double, into `out`. Answers false and writes nothing
/// when it holds something else.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_nan_as_f64(value: u64, out: *mut f64) -> bool {
    match SharedNaNValue::from_raw(value).as_f64() {
        Some(v) => {
            if !out.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = v };
            }
            true
        }
        None => false,
    }
}

/// Read `value` as a signed 32-bit integer, into `out`. Answers false and
/// writes nothing when it holds something else.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_nan_as_i32(value: u64, out: *mut i32) -> bool {
    match SharedNaNValue::from_raw(value).as_i32() {
        Some(v) => {
            if !out.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = v };
            }
            true
        }
        None => false,
    }
}

/// Read `value` as an unsigned 32-bit integer, into `out`. Answers false
/// and writes nothing when it holds something else.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_nan_as_u32(value: u64, out: *mut u32) -> bool {
    match SharedNaNValue::from_raw(value).as_u32() {
        Some(v) => {
            if !out.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = v };
            }
            true
        }
        None => false,
    }
}

/// Read `value` as a boolean, into `out`. Answers false and writes
/// nothing when it holds something else.
///
/// Note that a false answer and a stored `false` are different things:
/// the answer says whether the word held a boolean at all, and `out`
/// says which boolean it was.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_nan_as_bool(value: u64, out: *mut bool) -> bool {
    match SharedNaNValue::from_raw(value).as_bool() {
        Some(v) => {
            if !out.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = v };
            }
            true
        }
        None => false,
    }
}

/// Whether `value` is nil.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_is_nil(value: u64) -> bool {
    SharedNaNValue::from_raw(value).is_nil()
}

/// The widest tag the two-level form accepts. Thirty two would leave no
/// index bits.
pub const SUBETHA_NAN_MAX_TAG_BITS: u32 = 31;

// A literal, tied to the layer below, because the header generator copies
// a constant's expression rather than its value.
const _: () = assert!(
    SUBETHA_NAN_MAX_TAG_BITS == subetha_cxc::tagged_offset_ptr::MAX_TAG_BITS
);

/// Pack an index and a tag into one word, as a two-level value: the outer
/// tag says it is a tagged pointer, and `tag_bits` of the payload hold
/// the inner tag with the rest holding the index.
///
/// `tag_bits` is not stored in the word. A caller reading it back must
/// pass the same width it wrote, and nothing here can check that: a wrong
/// width yields a different index and tag rather than a refusal.
///
/// Answers false and writes nothing when `tag_bits` is above
/// `SUBETHA_NAN_MAX_TAG_BITS`, or when either component does not fit the
/// width it leaves. A component is refused rather than truncated, since a
/// silently narrowed index points somewhere else.
///
/// # Safety
/// `out` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_nan_from_tagged(
    index: u32,
    tag: u32,
    tag_bits: u32,
    out: *mut u64,
) -> bool {
    match SharedNaNTaggedValue::from_tagged_parts(index, tag, tag_bits) {
        Some(v) => {
            if !out.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = v.raw() };
            }
            true
        }
        None => false,
    }
}

/// Read `value` as an index and a tag at `tag_bits`.
///
/// Answers false and writes nothing when the word is not a tagged
/// pointer, or `tag_bits` is above `SUBETHA_NAN_MAX_TAG_BITS`.
///
/// # Safety
/// `index_out` and `tag_out` are each null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_nan_as_tagged(
    value: u64,
    tag_bits: u32,
    index_out: *mut u32,
    tag_out: *mut u32,
) -> bool {
    match SharedNaNTaggedValue::from_raw(value).as_tagged_parts(tag_bits) {
        Some((index, tag)) => {
            if !index_out.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *index_out = index };
            }
            if !tag_out.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *tag_out = tag };
            }
            true
        }
        None => false,
    }
}

/// Whether `value` is a tagged pointer.
///
/// A word packed by `subetha_nan_from_tagged` reads as
/// `SUBETHA_NAN_RESERVED` from `subetha_nan_type`, which knows only the
/// one-level form. This is how the two-level form is recognized.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_nan_is_tagged(value: u64) -> bool {
    SharedNaNTaggedValue::from_raw(value).is_tagged_offset_ptr()
}
