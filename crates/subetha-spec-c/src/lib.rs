//! Safe calls into the independent C implementation of the
//! Sens-O-Matic RLC wire format.
//!
//! This crate is a second reader of SENS_O_MATIC_WIRE.md. The C in
//! `c/` was written from that document and links nothing of SubEtha's,
//! which is the only thing that makes it evidence: a program built on
//! `subetha-ffi` would inherit every assumption of the first
//! implementation and could not disagree with it about anything.
//!
//! What is here is the thinnest possible Rust over that C. The
//! declarations below are written against this crate's own header, and
//! the only judgement in this file is about pointers and lengths.
//!
//! The test beside it drives these against
//! `crates/subetha-cxc/vectors/rlc.txt`, which the first implementation
//! generates and is held to. Agreement there is two implementations
//! agreeing about the document; a disagreement is a place the document
//! reads two ways, and that is the result this crate exists to produce.

use std::ffi::c_void;
use std::sync::Once;

unsafe extern "C" {
    fn sens_field_init();
    fn sens_gf_mul(a: u8, b: u8) -> u8;
    fn sens_gf_inv(a: u8) -> u8;
    fn sens_tap(place: u32) -> u8;
    fn sens_coefficient(place: u32, density: u32) -> u8;
    fn sens_symbol_pack(symbol: *mut u8, symbol_len: usize, item: *const u8, item_len: usize) -> i32;
    fn sens_symbol_unpack(
        symbol: *const u8,
        symbol_len: usize,
        out: *mut u8,
        out_cap: usize,
    ) -> usize;
    fn sens_data_write(
        buf: *mut u8,
        cap: usize,
        conn_id: u64,
        source_id: u32,
        send_us: u32,
        symbol: *const u8,
        symbol_len: usize,
    ) -> usize;
    fn sens_data_read(
        buf: *const u8,
        len: usize,
        conn_id: *mut u64,
        source_id: *mut u32,
        send_us: *mut u32,
        symbol: *mut *const u8,
        symbol_len: *mut usize,
    ) -> i32;
    fn sens_repair_write(
        buf: *mut u8,
        cap: usize,
        conn_id: u64,
        repair_key: u32,
        first_source_id: u32,
        window_size: u16,
        dt: u8,
        symbols: *const u8,
        symbol_len: usize,
    ) -> usize;
    fn sens_repair_recover(
        out: *mut u8,
        symbol_len: usize,
        payload: *const u8,
        present: *const *const u8,
        window_size: u16,
        dt: u8,
    ) -> i32;
    fn sens_ack_write(buf: *mut u8, cap: usize, delivered_through: u32, sack: u64) -> usize;
    fn sens_ack_read(
        buf: *const u8,
        len: usize,
        delivered_through: *mut u32,
        sack: *mut u64,
    ) -> i32;
    fn sens_nak_write(buf: *mut u8, cap: usize, ids: *const u32, count: usize) -> usize;
    fn sens_nak_read(buf: *const u8, len: usize, out: *mut u32, out_cap: usize) -> usize;

    fn sens_rs_cauchy(k: u32, j: u32, c: u32) -> u8;
    fn sens_rs_parity(out: *mut u8, shard_len: usize, data: *const u8, k: u32, j: u32) -> i32;
    fn sens_rs_recover(
        out: *mut u8,
        shard_len: usize,
        present: *const *const u8,
        k: u32,
        r: u32,
    ) -> i32;
    fn sens_rs_shard_pack(
        shard: *mut u8,
        shard_len: usize,
        item: *const u8,
        item_len: usize,
    ) -> i32;
    fn sens_rs_data_write(
        buf: *mut u8,
        cap: usize,
        block_id: u32,
        shard_index: u8,
        k: u8,
        r: u8,
        flags: u8,
        epoch: u32,
        shard: *const u8,
        shard_len: usize,
    ) -> usize;
    fn sens_rs_data_read(
        buf: *const u8,
        len: usize,
        block_id: *mut u32,
        shard_index: *mut u8,
        k: *mut u8,
        r: *mut u8,
        flags: *mut u8,
        epoch: *mut u32,
        shard: *mut *const u8,
        shard_len: *mut usize,
    ) -> i32;
    fn sens_rs_outer_id_read(
        id: u32,
        d: *mut u32,
        r_outer: *mut u32,
        segment: *mut u32,
        outer_index: *mut u32,
    ) -> i32;
}

/// Section 5: the block Cauchy Reed-Solomon variant.
pub mod rs {
    use super::ready;

    /// Section 5.2: the Cauchy entry for parity row `j`, data column `c`
    /// of a `(k, r)` code.
    pub fn cauchy(k: usize, j: usize, c: usize) -> u8 {
        ready();
        unsafe { super::sens_rs_cauchy(k as u32, j as u32, c as u32) }
    }

    /// Section 5.2: parity shard `j` over `data`, which is `k` shards of
    /// one fixed size laid out in index order.
    ///
    /// `None` when the shape is outside what this implementation bounds
    /// itself to, which the C header states and which is narrower than
    /// the format allows.
    pub fn parity(data: &[Vec<u8>], k: usize, j: usize) -> Option<Vec<u8>> {
        ready();
        let shard_len = data.first().map_or(0, Vec::len);
        assert!(
            data.iter().all(|s| s.len() == shard_len),
            "a block's shards are one fixed size, which is what lets parity be their combination"
        );
        assert_eq!(data.len(), k, "a (k, r) code takes exactly k data shards");
        let flat: Vec<u8> = data.iter().flatten().copied().collect();
        let mut out = vec![0u8; shard_len];
        // SAFETY: `flat` holds k * shard_len readable bytes and `out`
        // holds shard_len writable ones.
        let rc = unsafe {
            super::sens_rs_parity(out.as_mut_ptr(), shard_len, flat.as_ptr(), k as u32, j as u32)
        };
        (rc == 0).then_some(out)
    }

    /// Section 5.2: rebuild every data shard from any `k` survivors.
    ///
    /// `present` is `k + r` entries in shard-index order, data first
    /// then parity, with `None` where a shard was lost. `None` comes
    /// back when fewer than `k` survive.
    pub fn recover(present: &[Option<Vec<u8>>], k: usize, r: usize) -> Option<Vec<Vec<u8>>> {
        ready();
        let shard_len = present.iter().flatten().next().map_or(0, Vec::len);
        let pointers: Vec<*const u8> = present
            .iter()
            .map(|s| s.as_ref().map_or(std::ptr::null(), |v| v.as_ptr()))
            .collect();
        let mut flat = vec![0u8; k * shard_len];
        // SAFETY: every non-null pointer names a shard that outlives the
        // call, because `present` is borrowed for it, and `flat` holds
        // k * shard_len writable bytes.
        let rc = unsafe {
            super::sens_rs_recover(
                flat.as_mut_ptr(),
                shard_len,
                pointers.as_ptr(),
                k as u32,
                r as u32,
            )
        };
        (rc == 0).then(|| flat.chunks(shard_len).map(<[u8]>::to_vec).collect())
    }

    /// Section 5.1: pack an item into a data shard.
    pub fn shard_pack(item: &[u8], shard_len: usize) -> Option<Vec<u8>> {
        ready();
        let mut shard = vec![0u8; shard_len];
        // SAFETY: both buffers are ours and pass their lengths.
        let rc = unsafe {
            super::sens_rs_shard_pack(shard.as_mut_ptr(), shard_len, item.as_ptr(), item.len())
        };
        (rc == 0).then_some(shard)
    }

    /// Section 5.1: a `DATA` frame carrying one shard.
    #[allow(clippy::too_many_arguments)]
    pub fn data_write(
        block_id: u32,
        shard_index: u8,
        k: u8,
        r: u8,
        flags: u8,
        epoch: u32,
        shard: &[u8],
    ) -> Vec<u8> {
        ready();
        let mut buf = vec![0u8; 13 + shard.len()];
        // SAFETY: the buffer is sized to exactly what the C writes.
        let n = unsafe {
            super::sens_rs_data_write(
                buf.as_mut_ptr(),
                buf.len(),
                block_id,
                shard_index,
                k,
                r,
                flags,
                epoch,
                shard.as_ptr(),
                shard.len(),
            )
        };
        buf.truncate(n);
        buf
    }

    /// What a `DATA` frame carries: block id, shard index, k, r, flags,
    /// epoch, shard.
    pub type Data = (u32, u8, u8, u8, u8, u32, Vec<u8>);

    /// Section 5.1: read a `DATA` frame, or `None` when it is not one.
    pub fn data_read(frame: &[u8]) -> Option<Data> {
        ready();
        let mut block_id = 0u32;
        let mut shard_index = 0u8;
        let mut k = 0u8;
        let mut r = 0u8;
        let mut flags = 0u8;
        let mut epoch = 0u32;
        let mut shard: *const u8 = std::ptr::null();
        let mut shard_len = 0usize;
        // SAFETY: every out pointer is to a live local and `frame` is
        // passed with its length.
        let rc = unsafe {
            super::sens_rs_data_read(
                frame.as_ptr(),
                frame.len(),
                &mut block_id,
                &mut shard_index,
                &mut k,
                &mut r,
                &mut flags,
                &mut epoch,
                &mut shard,
                &mut shard_len,
            )
        };
        if rc != 0 {
            return None;
        }
        // SAFETY: the C set `shard` inside `frame` and `shard_len` to
        // what remains after the header.
        let bytes = unsafe { std::slice::from_raw_parts(shard, shard_len) }.to_vec();
        Some((block_id, shard_index, k, r, flags, epoch, bytes))
    }

    /// Section 5.1: the four fields of an outer-parity block id, or
    /// `None` when the id names nothing decodable and must be discarded.
    pub fn outer_id_read(id: u32) -> Option<(u32, u32, u32, u32)> {
        ready();
        let mut d = 0u32;
        let mut r_outer = 0u32;
        let mut segment = 0u32;
        let mut outer_index = 0u32;
        // SAFETY: four out pointers to live locals.
        let rc = unsafe {
            super::sens_rs_outer_id_read(
                id,
                &mut d,
                &mut r_outer,
                &mut segment,
                &mut outer_index,
            )
        };
        (rc == 0).then_some((d, r_outer, segment, outer_index))
    }
}

/// The field tables are built once. Every entry point below calls this,
/// so a caller cannot forget and get zeros out of the multiply.
fn ready() {
    static ONCE: Once = Once::new();
    // SAFETY: the C builds two static tables and sets a flag; `Once`
    // makes the first call the only one that writes them.
    ONCE.call_once(|| unsafe { sens_field_init() });
}

/// Section 2: GF(2^8) multiply under `x^8+x^4+x^3+x^2+1`.
pub fn gf_mul(a: u8, b: u8) -> u8 {
    ready();
    unsafe { sens_gf_mul(a, b) }
}

/// Section 2: the multiplicative inverse in that field.
pub fn gf_inv(a: u8) -> u8 {
    ready();
    unsafe { sens_gf_inv(a) }
}

/// Section 4.4: the published tap at `place`, or zero past the table.
pub fn tap(place: usize) -> u8 {
    ready();
    unsafe { sens_tap(place as u32) }
}

/// Section 4.4: the coefficient for a symbol `place` from the newest in
/// the window, at `density`.
pub fn coefficient(place: usize, density: u8) -> u8 {
    ready();
    unsafe { sens_coefficient(place as u32, density as u32) }
}

/// Section 4.3: pack `item` into a symbol of `symbol_len` bytes.
///
/// `None` when the item does not satisfy `length + 2 <= symbol_len`.
pub fn symbol_pack(item: &[u8], symbol_len: usize) -> Option<Vec<u8>> {
    ready();
    let mut symbol = vec![0u8; symbol_len];
    // SAFETY: `symbol` holds `symbol_len` writable bytes and `item`
    // holds `item.len()` readable ones.
    let rc = unsafe {
        sens_symbol_pack(symbol.as_mut_ptr(), symbol_len, item.as_ptr(), item.len())
    };
    (rc == 0).then_some(symbol)
}

/// Section 4.3: the item a symbol holds, clamped to what is there.
pub fn symbol_unpack(symbol: &[u8]) -> Vec<u8> {
    ready();
    let mut out = vec![0u8; symbol.len()];
    // SAFETY: both buffers are ours and their lengths are passed with
    // them.
    let n = unsafe {
        sens_symbol_unpack(symbol.as_ptr(), symbol.len(), out.as_mut_ptr(), out.len())
    };
    out.truncate(n);
    out
}

/// Section 4.1: a `DATA` frame carrying `symbol`.
pub fn data_write(conn_id: u64, source_id: u32, send_us: u32, symbol: &[u8]) -> Vec<u8> {
    ready();
    let mut buf = vec![0u8; 17 + symbol.len()];
    // SAFETY: the buffer is sized to exactly what the C writes.
    let n = unsafe {
        sens_data_write(
            buf.as_mut_ptr(),
            buf.len(),
            conn_id,
            source_id,
            send_us,
            symbol.as_ptr(),
            symbol.len(),
        )
    };
    buf.truncate(n);
    buf
}

/// Section 4.1: what a `DATA` frame carries, or `None` when it is not
/// one.
pub fn data_read(frame: &[u8]) -> Option<(u64, u32, u32, Vec<u8>)> {
    ready();
    let mut conn_id = 0u64;
    let mut source_id = 0u32;
    let mut send_us = 0u32;
    let mut symbol: *const u8 = std::ptr::null();
    let mut symbol_len = 0usize;
    // SAFETY: every out pointer is to a live local, and `frame` is
    // passed with its length.
    let rc = unsafe {
        sens_data_read(
            frame.as_ptr(),
            frame.len(),
            &mut conn_id,
            &mut source_id,
            &mut send_us,
            &mut symbol,
            &mut symbol_len,
        )
    };
    if rc != 0 {
        return None;
    }
    // SAFETY: the C set `symbol` to point inside `frame` and
    // `symbol_len` to what remains after the header.
    let bytes = unsafe { std::slice::from_raw_parts(symbol, symbol_len) }.to_vec();
    Some((conn_id, source_id, send_us, bytes))
}

/// Section 4.2: a `REPAIR` frame over `symbols`, laid out oldest first
/// and each `symbol_len` bytes.
///
/// `None` when the generator in `dt`'s high nibble is not 1, which this
/// implementation cannot reproduce and must refuse.
pub fn repair_write(
    conn_id: u64,
    repair_key: u32,
    first_source_id: u32,
    dt: u8,
    symbols: &[Vec<u8>],
) -> Option<Vec<u8>> {
    ready();
    let symbol_len = symbols.first().map_or(0, Vec::len);
    assert!(
        symbols.iter().all(|s| s.len() == symbol_len),
        "a window's symbols are one fixed size, which is what lets a repair be their sum"
    );
    let flat: Vec<u8> = symbols.iter().flatten().copied().collect();
    let mut buf = vec![0u8; 20 + symbol_len];
    // SAFETY: `flat` holds window_size * symbol_len readable bytes and
    // `buf` is sized to the header plus one symbol.
    let n = unsafe {
        sens_repair_write(
            buf.as_mut_ptr(),
            buf.len(),
            conn_id,
            repair_key,
            first_source_id,
            symbols.len() as u16,
            dt,
            flat.as_ptr(),
            symbol_len,
        )
    };
    (n != 0).then(|| {
        buf.truncate(n);
        buf
    })
}

/// Section 4.2 and 4.4: recover the one missing symbol of a window.
///
/// `present` is the window oldest first, with `None` where the symbol
/// was lost. `None` comes back when the generator is not 1, when the
/// number of missing symbols is not one, or when the missing symbol's
/// coefficient is zero, meaning the repair never covered it.
pub fn repair_recover(
    payload: &[u8],
    present: &[Option<Vec<u8>>],
    dt: u8,
) -> Option<Vec<u8>> {
    ready();
    let symbol_len = payload.len();
    let pointers: Vec<*const u8> = present
        .iter()
        .map(|s| s.as_ref().map_or(std::ptr::null(), |v| v.as_ptr()))
        .collect();
    let mut out = vec![0u8; symbol_len];
    // SAFETY: every non-null pointer names a symbol that outlives this
    // call, because `present` is borrowed for it.
    let rc = unsafe {
        sens_repair_recover(
            out.as_mut_ptr(),
            symbol_len,
            payload.as_ptr(),
            pointers.as_ptr(),
            present.len() as u16,
            dt,
        )
    };
    (rc == 0).then_some(out)
}

/// Section 4.6: an `ACK`.
pub fn ack_write(delivered_through: u32, sack: u64) -> Vec<u8> {
    ready();
    let mut buf = vec![0u8; 13];
    // SAFETY: the buffer is the frame's stated size.
    let n = unsafe { sens_ack_write(buf.as_mut_ptr(), buf.len(), delivered_through, sack) };
    buf.truncate(n);
    buf
}

/// Section 4.6: what an `ACK` carries.
pub fn ack_read(frame: &[u8]) -> Option<(u32, u64)> {
    ready();
    let mut delivered_through = 0u32;
    let mut sack = 0u64;
    // SAFETY: both out pointers are to live locals.
    let rc = unsafe {
        sens_ack_read(frame.as_ptr(), frame.len(), &mut delivered_through, &mut sack)
    };
    (rc == 0).then_some((delivered_through, sack))
}

/// Section 4.5: a `NAK` naming missing source ids.
pub fn nak_write(ids: &[u32]) -> Vec<u8> {
    ready();
    let mut buf = vec![0u8; 1 + ids.len() * 4];
    // SAFETY: the buffer is sized to the type byte plus four per id.
    let n = unsafe { sens_nak_write(buf.as_mut_ptr(), buf.len(), ids.as_ptr(), ids.len()) };
    buf.truncate(n);
    buf
}

/// Section 4.5: the ids a `NAK` names. A trailing partial id is ignored.
pub fn nak_read(frame: &[u8]) -> Vec<u32> {
    ready();
    let mut out = vec![0u32; frame.len() / 4 + 1];
    // SAFETY: `out` holds at least as many ids as the frame can carry.
    let n = unsafe { sens_nak_read(frame.as_ptr(), frame.len(), out.as_mut_ptr(), out.len()) };
    out.truncate(n);
    out
}

/// Keeps the linker from dropping the static library when a build
/// happens to inline everything the tests call.
#[doc(hidden)]
pub fn anchor() -> *const c_void {
    sens_field_init as *const c_void
}
