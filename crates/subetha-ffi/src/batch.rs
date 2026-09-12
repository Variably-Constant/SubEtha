//! What every batch entry point shares.
//!
//! A batch call takes one handle lookup and one panic guard for a run of
//! operations, which is what the single-call forms spend per item. The
//! items live in the caller's own array: `items` addresses the first,
//! `stride` is the distance to the next, so a caller passes an array of
//! its own structs without repacking, and a packed array is `stride`
//! equal to the item's size.
//!
//! A batch stops at the first operation the object refuses and reports
//! how many it completed. It returns `SUBETHA_OK` when at least one
//! completed, so a caller reads the count and comes back for the rest,
//! and the refusal's own code when the first operation was the one that
//! failed. A count of zero is `SUBETHA_OK` with nothing done.

use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_OK};

/// The arguments a batch reads: a base that is null only when the count
/// is zero, a stride wide enough for one item, and somewhere to report
/// the count.
///
/// # Safety
/// `out_done` is a valid pointer when this returns `Ok`.
pub(crate) fn batch_arguments(base_is_null: bool, stride: usize, item_bytes: usize, count: usize, out_done_is_null: bool) -> Result<(), i32> {
    if out_done_is_null {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out_done is null"));
    }
    if count == 0 {
        return Ok(());
    }
    if base_is_null {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "items is null with a non-zero count"));
    }
    if stride < item_bytes {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("a stride of {stride} does not span an item of {item_bytes} bytes"),
        ));
    }
    if stride.checked_mul(count).is_none() {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("a stride of {stride} over {count} items overruns this platform's address space"),
        ));
    }
    Ok(())
}

/// The code a finished batch returns: what it completed is in `done`, and
/// the first refusal is reported only when nothing was completed.
///
/// # Safety
/// `out_done` is a valid pointer, which `batch_arguments` has checked.
pub(crate) unsafe fn batch_result(done: usize, refusal: Option<i32>, out_done: *mut usize) -> i32 {
    // SAFETY: the caller guarantees it is writable.
    unsafe { *out_done = done };
    match refusal {
        Some(code) if done == 0 => code,
        _ => SUBETHA_OK,
    }
}

/// Hand each item of a strided array to `push` until one is refused.
/// `push` reports `SUBETHA_OK` or the code for its refusal.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_done` is null
/// or a valid pointer.
pub(crate) unsafe fn run_push_many(
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_done: *mut usize,
    mut push: impl FnMut(&[u8]) -> i32,
) -> i32 {
    if let Err(code) = batch_arguments(items.is_null(), stride, len, count, out_done.is_null()) {
        return code;
    }
    let mut done = 0usize;
    let mut refusal = None;
    while done < count {
        // SAFETY: the caller guarantees the array spans this item.
        let payload = unsafe { std::slice::from_raw_parts(items.add(done * stride), len) };
        match push(payload) {
            SUBETHA_OK => done += 1,
            code => {
                refusal = Some(code);
                break;
            }
        }
    }
    // SAFETY: `batch_arguments` refused a null `out_done`.
    unsafe { batch_result(done, refusal, out_done) }
}

/// Fill each slot of a strided array from `pop` until one is refused.
/// `pop` reports `SUBETHA_OK` or the code for its refusal, and `item` is
/// the least a slot may be.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is null or
/// a valid pointer.
pub(crate) unsafe fn run_pop_many(
    out: *mut u8,
    stride: usize,
    item: usize,
    count: usize,
    out_done: *mut usize,
    mut pop: impl FnMut(&mut [u8]) -> i32,
) -> i32 {
    // SAFETY: the caller's guarantees are the indexed form's.
    unsafe { run_pop_many_indexed(out, stride, item, count, out_done, |_, buf| pop(buf)) }
}

/// [`run_pop_many`] with the slot's index handed to `pop`.
///
/// A caller whose source is addressed rather than sequential needs it:
/// the vec reads the element at `indices[i]` and the map the value for
/// `keys[i]`, so the operation depends on which slot is being filled. The
/// index comes from the loop that computes the slot address, so a caller
/// counting alongside cannot fall out of step with it - which is what
/// keeping a second counter in the closure would risk.
///
/// # Safety
/// `out` addresses `count * stride` writable bytes; `out_done` is null or
/// a valid pointer.
pub(crate) unsafe fn run_pop_many_indexed(
    out: *mut u8,
    stride: usize,
    item: usize,
    count: usize,
    out_done: *mut usize,
    mut pop: impl FnMut(usize, &mut [u8]) -> i32,
) -> i32 {
    if let Err(code) = batch_arguments(out.is_null(), stride, item, count, out_done.is_null()) {
        return code;
    }
    let mut done = 0usize;
    let mut refusal = None;
    while done < count {
        // SAFETY: the caller guarantees the array spans this slot.
        let buf = unsafe { std::slice::from_raw_parts_mut(out.add(done * stride), stride) };
        match pop(done, buf) {
            SUBETHA_OK => done += 1,
            code => {
                refusal = Some(code);
                break;
            }
        }
    }
    // SAFETY: `batch_arguments` refused a null `out_done`.
    unsafe { batch_result(done, refusal, out_done) }
}

/// Hand each item of a strided array to `push` until one is refused, and
/// write what each one answered into `out_words`.
///
/// A push does not always answer with nothing. The vec answers with the
/// index the element landed at and the arena with the reference that
/// names the bytes it interned, and a batch that dropped those would
/// leave the caller unable to name what it had just written - which is
/// the whole point of writing it. `out_words` is filled for the items
/// that completed and left alone past that, so a caller reads `out_done`
/// of them.
///
/// # Safety
/// `items` addresses `count * stride` readable bytes; `out_words`
/// addresses `count` writable `uint64_t`s; `out_done` is null or a valid
/// pointer.
pub(crate) unsafe fn run_push_many_reporting(
    items: *const u8,
    stride: usize,
    len: usize,
    count: usize,
    out_words: *mut u64,
    out_done: *mut usize,
    mut push: impl FnMut(&[u8]) -> Result<u64, i32>,
) -> i32 {
    if let Err(code) = batch_arguments(items.is_null(), stride, len, count, out_done.is_null()) {
        return code;
    }
    if count != 0 && out_words.is_null() {
        return fail(SUBETHA_E_INVALID_ARGUMENT, "out_words is null with a non-zero count");
    }
    let mut done = 0usize;
    let mut refusal = None;
    while done < count {
        // SAFETY: the caller guarantees the array spans this item.
        let payload = unsafe { std::slice::from_raw_parts(items.add(done * stride), len) };
        match push(payload) {
            Ok(word) => {
                // SAFETY: `done` is below `count`, which the caller
                // guarantees is writable at `out_words`.
                unsafe { *out_words.add(done) = word };
                done += 1;
            }
            Err(code) => {
                refusal = Some(code);
                break;
            }
        }
    }
    // SAFETY: `batch_arguments` refused a null `out_done`.
    unsafe { batch_result(done, refusal, out_done) }
}

/// Hand each pair from two strided arrays to `insert` until one is
/// refused, for an operation whose item is two runs of bytes rather than
/// one. The map's key and value are that shape, and keeping them in the
/// caller's own two arrays is what saves the repacking a single array of
/// pairs would need.
///
/// # Safety
/// `keys` addresses `count * key_stride` readable bytes and `values`
/// `count * value_stride`; `out_done` is null or a valid pointer.
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn run_pair_many(
    keys: *const u8,
    key_stride: usize,
    key_len: usize,
    values: *const u8,
    value_stride: usize,
    value_len: usize,
    count: usize,
    out_done: *mut usize,
    mut insert: impl FnMut(&[u8], &[u8]) -> i32,
) -> i32 {
    if let Err(code) = batch_arguments(keys.is_null(), key_stride, key_len, count, out_done.is_null()) {
        return code;
    }
    // The value array is checked with the same rule, so a caller passing
    // a stride that spans the key but not the value is told which one.
    if let Err(code) = batch_arguments(values.is_null(), value_stride, value_len, count, out_done.is_null()) {
        return code;
    }
    let mut done = 0usize;
    let mut refusal = None;
    while done < count {
        // SAFETY: the caller guarantees each array spans this item.
        let key = unsafe { std::slice::from_raw_parts(keys.add(done * key_stride), key_len) };
        let value = unsafe { std::slice::from_raw_parts(values.add(done * value_stride), value_len) };
        match insert(key, value) {
            SUBETHA_OK => done += 1,
            code => {
                refusal = Some(code);
                break;
            }
        }
    }
    // SAFETY: `batch_arguments` refused a null `out_done`.
    unsafe { batch_result(done, refusal, out_done) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reporting_batch_fills_a_word_for_each_item_that_landed() {
        let items: [u8; 4] = [1, 2, 3, 4];
        let mut words = [u64::MAX; 4];
        let mut done = 0usize;

        // Every item lands, so every word is filled.
        // SAFETY: the arrays are live locals of the sizes named.
        let rc = unsafe {
            run_push_many_reporting(items.as_ptr(), 1, 1, 4, words.as_mut_ptr(), &mut done, |p| {
                Ok(u64::from(p[0]) * 10)
            })
        };
        assert_eq!(rc, SUBETHA_OK);
        assert_eq!(done, 4);
        assert_eq!(words, [10, 20, 30, 40]);

        // A refusal partway leaves the words past it alone, so a caller
        // reading `done` of them reads only what it wrote.
        let mut words = [u64::MAX; 4];
        // SAFETY: as above.
        let rc = unsafe {
            run_push_many_reporting(items.as_ptr(), 1, 1, 4, words.as_mut_ptr(), &mut done, |p| {
                if p[0] < 3 { Ok(u64::from(p[0])) } else { Err(-7) }
            })
        };
        assert_eq!(rc, SUBETHA_OK, "two landed, so the batch reports what it did");
        assert_eq!(done, 2);
        assert_eq!(words, [1, 2, u64::MAX, u64::MAX]);

        // Nothing lands, so the refusal is what the caller is told.
        // SAFETY: as above.
        let rc = unsafe {
            run_push_many_reporting(items.as_ptr(), 1, 1, 4, words.as_mut_ptr(), &mut done, |_| Err(-7))
        };
        assert_eq!(rc, -7);
        assert_eq!(done, 0);

        // A null word array is refused for a non-zero count, and read for
        // a count of zero it is never touched.
        // SAFETY: as above.
        let rc = unsafe {
            run_push_many_reporting(items.as_ptr(), 1, 1, 4, std::ptr::null_mut(), &mut done, |_| Ok(0))
        };
        assert_eq!(rc, SUBETHA_E_INVALID_ARGUMENT);
        // SAFETY: as above.
        let rc = unsafe {
            run_push_many_reporting(std::ptr::null(), 1, 1, 0, std::ptr::null_mut(), &mut done, |_| Ok(0))
        };
        assert_eq!(rc, SUBETHA_OK);
        assert_eq!(done, 0);
    }

    #[test]
    fn a_pair_batch_checks_both_arrays() {
        let keys: [u8; 4] = [1, 2, 3, 4];
        let values: [u8; 8] = [10, 11, 20, 21, 30, 31, 40, 41];
        let mut seen = Vec::new();
        let mut done = 0usize;

        // SAFETY: the arrays are live locals of the sizes named.
        let rc = unsafe {
            run_pair_many(keys.as_ptr(), 1, 1, values.as_ptr(), 2, 2, 4, &mut done, |k, v| {
                seen.push((k[0], v[0], v[1]));
                SUBETHA_OK
            })
        };
        assert_eq!(rc, SUBETHA_OK);
        assert_eq!(done, 4);
        assert_eq!(seen, vec![(1, 10, 11), (2, 20, 21), (3, 30, 31), (4, 40, 41)]);

        // A value stride that does not span a value is refused, even
        // though the key stride is fine.
        // SAFETY: as above.
        let rc = unsafe {
            run_pair_many(keys.as_ptr(), 1, 1, values.as_ptr(), 1, 2, 4, &mut done, |_, _| SUBETHA_OK)
        };
        assert_eq!(rc, SUBETHA_E_INVALID_ARGUMENT);
    }

    #[test]
    fn the_arguments_a_batch_refuses() {
        let mut done = 0usize;
        assert!(batch_arguments(false, 64, 64, 10, false).is_ok());
        assert!(batch_arguments(true, 0, 64, 0, false).is_ok(), "a count of zero reads no items");
        assert_eq!(batch_arguments(false, 64, 64, 10, true).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(batch_arguments(true, 64, 64, 10, false).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(batch_arguments(false, 32, 64, 10, false).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        assert_eq!(batch_arguments(false, usize::MAX, 1, 2, false).unwrap_err(), SUBETHA_E_INVALID_ARGUMENT);
        // SAFETY: `done` is a live local.
        unsafe {
            assert_eq!(batch_result(3, None, &mut done), SUBETHA_OK);
            assert_eq!(done, 3);
            assert_eq!(batch_result(3, Some(-5), &mut done), SUBETHA_OK, "what landed outranks the refusal that stopped it");
            assert_eq!(done, 3);
            assert_eq!(batch_result(0, Some(-5), &mut done), -5, "a batch that did nothing reports why");
            assert_eq!(done, 0);
        }
    }
}
