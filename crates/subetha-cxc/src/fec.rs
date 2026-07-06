//! Forward error correction over GF(256): systematic Cauchy
//! Reed-Solomon erasure coding.
//!
//! Packet loss on UDP is an *erasure* - the receiver knows WHICH
//! packet is missing from the gap in the sequence numbers - so the
//! decoder recovers from any K survivors out of K + R coded packets
//! without needing to locate the error. This is the FEC half of the
//! reliable-UDP transport's FEC-primary / ARQ-fallback design: a block
//! of K source packets ships with R parity packets, and up to R losses
//! per block are recovered with no retransmit round-trip.
//!
//! The code is *systematic* (the K source shards are transmitted
//! verbatim; only the R parity shards are computed) and *MDS* (any K of
//! the K + R shards reconstruct the block). The MDS property comes from
//! a Cauchy parity matrix: every square submatrix of a Cauchy matrix is
//! invertible, so any K-row submatrix of the `[I_K ; Cauchy]` encoding
//! matrix is invertible.
//!
//! GF(256) uses the primitive polynomial `0x11D` with log / antilog
//! tables computed at compile time (`const fn`), so there is no runtime
//! initialization.

#![allow(clippy::needless_range_loop)]

/// GF(256) arithmetic with the primitive polynomial `x^8 + x^4 + x^3 +
/// x^2 + 1` (`0x11D`) and generator `2`.
pub mod gf {
    /// `(LOG, EXP)`: `EXP` is doubled to 512 entries so `EXP[log a +
    /// log b]` needs no modular reduction (`log a + log b <= 508`).
    const fn build_tables() -> ([u8; 256], [u8; 512]) {
        let mut log = [0u8; 256];
        let mut exp = [0u8; 512];
        let mut x: u16 = 1;
        let mut i = 0usize;
        while i < 255 {
            exp[i] = x as u8;
            log[x as usize] = i as u8;
            x <<= 1;
            if x & 0x100 != 0 {
                x ^= 0x11D;
            }
            i += 1;
        }
        // Second period for multiply without a modulo.
        let mut j = 255usize;
        while j < 512 {
            exp[j] = exp[j - 255];
            j += 1;
        }
        (log, exp)
    }

    const TABLES: ([u8; 256], [u8; 512]) = build_tables();
    const LOG: [u8; 256] = TABLES.0;
    const EXP: [u8; 512] = TABLES.1;

    /// Addition (and subtraction) in GF(256) is XOR.
    #[inline(always)]
    pub const fn add(a: u8, b: u8) -> u8 {
        a ^ b
    }

    /// Multiplication via the log / antilog tables.
    #[inline(always)]
    pub fn mul(a: u8, b: u8) -> u8 {
        if a == 0 || b == 0 {
            0
        } else {
            EXP[LOG[a as usize] as usize + LOG[b as usize] as usize]
        }
    }

    /// Multiplicative inverse (`a != 0`).
    #[inline(always)]
    pub fn inv(a: u8) -> u8 {
        debug_assert!(a != 0, "GF(256) has no inverse of 0");
        EXP[255 - LOG[a as usize] as usize]
    }

    /// Division `a / b` (`b != 0`).
    #[inline(always)]
    pub fn div(a: u8, b: u8) -> u8 {
        if a == 0 {
            0
        } else {
            EXP[LOG[a as usize] as usize + 255 - LOG[b as usize] as usize]
        }
    }
}

/// GF(256) multiply-add backend, exposed so an A/B bench can compare every
/// SIMD rung against the scalar baseline and so the GFNI / AVX-512 logic can be
/// validated on a host without the silicon via the bit-exact software affine
/// emulation. The production hot loop (`gf_mul_add`) auto-selects the fastest
/// rung this host can run; the ladder always bottoms out at `Scalar`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GfBackend {
    /// Portable table-lookup multiply. Always available; the fallback floor.
    Scalar = 0,
    /// SSSE3 PSHUFB nibble-table multiply, 16 bytes per op.
    Ssse3 = 1,
    /// AVX2 PSHUFB nibble-table multiply, 32 bytes per op.
    Avx2 = 2,
    /// AVX-512BW PSHUFB nibble-table multiply, 64 bytes per op (no GFNI).
    Avx512Pshufb = 3,
    /// GFNI affine GF(2^8) multiply on 256-bit lanes: a hardware field multiply
    /// (no table), broad consumer reach (`gfni` + `avx2`, no AVX-512 needed).
    Gfni256 = 4,
    /// GFNI affine GF(2^8) multiply on 512-bit lanes: 64 bytes per op in one
    /// hardware instruction.
    Gfni512 = 5,
    /// Software emulation of the GFNI affine transform: bit-exact to the GFNI
    /// hardware path, runnable on any host. Validates the GFNI logic where the
    /// silicon is absent, the same emulate-to-verify approach the AVX-512
    /// substrate uses.
    AffineScalar = 6,
    /// ARM NEON TBL nibble-table multiply, 16 bytes per `vqtbl1q_u8`. The
    /// aarch64 byte-shuffle rung: the same nibble-table technique as SSSE3, so
    /// bit-identical to it (and to scalar). NEON is baseline on every aarch64
    /// CPU, so this rung is always available on Apple Silicon / Neoverse.
    Neon = 7,
}

impl GfBackend {
    /// Whether this backend can execute on the current host.
    pub fn available(self) -> bool {
        match self {
            GfBackend::Scalar | GfBackend::AffineScalar => true,
            #[cfg(target_arch = "aarch64")]
            GfBackend::Neon => std::arch::is_aarch64_feature_detected!("neon"),
            #[cfg(not(target_arch = "aarch64"))]
            GfBackend::Neon => false,
            #[cfg(target_arch = "x86_64")]
            GfBackend::Ssse3 => std::is_x86_feature_detected!("ssse3"),
            #[cfg(target_arch = "x86_64")]
            GfBackend::Avx2 => std::is_x86_feature_detected!("avx2"),
            #[cfg(target_arch = "x86_64")]
            GfBackend::Avx512Pshufb => {
                std::is_x86_feature_detected!("avx512f")
                    && std::is_x86_feature_detected!("avx512bw")
            }
            #[cfg(target_arch = "x86_64")]
            GfBackend::Gfni256 => {
                std::is_x86_feature_detected!("gfni") && std::is_x86_feature_detected!("avx2")
            }
            #[cfg(target_arch = "x86_64")]
            GfBackend::Gfni512 => {
                std::is_x86_feature_detected!("gfni")
                    && std::is_x86_feature_detected!("avx512f")
                    && std::is_x86_feature_detected!("avx512bw")
            }
            #[cfg(not(target_arch = "x86_64"))]
            _ => false,
        }
    }

    /// Short identifier for diagnostics / bench output.
    pub fn name(self) -> &'static str {
        match self {
            GfBackend::Scalar => "scalar",
            GfBackend::Ssse3 => "ssse3",
            GfBackend::Avx2 => "avx2",
            GfBackend::Avx512Pshufb => "avx512-pshufb",
            GfBackend::Gfni256 => "gfni256",
            GfBackend::Gfni512 => "gfni512",
            GfBackend::AffineScalar => "affine-emulated",
            GfBackend::Neon => "neon",
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => GfBackend::Ssse3,
            2 => GfBackend::Avx2,
            3 => GfBackend::Avx512Pshufb,
            4 => GfBackend::Gfni256,
            5 => GfBackend::Gfni512,
            6 => GfBackend::AffineScalar,
            7 => GfBackend::Neon,
            _ => GfBackend::Scalar,
        }
    }
}

/// The fastest-first ladder. The production dispatcher walks it and picks the
/// first available rung; the A/B bench confirms each rung beats the next, so
/// the order is empirically grounded rather than assumed.
#[cfg(target_arch = "x86_64")]
const GF_LADDER: [GfBackend; 6] = [
    GfBackend::Gfni512,
    GfBackend::Avx512Pshufb,
    GfBackend::Gfni256,
    GfBackend::Avx2,
    GfBackend::Ssse3,
    GfBackend::Scalar,
];
/// aarch64 rungs: NEON TBL then the scalar floor. NEON is baseline on every
/// aarch64 CPU, so the auto-detector picks NEON on all Apple Silicon /
/// Neoverse hosts and reserves scalar for the (unreachable) no-NEON case.
#[cfg(target_arch = "aarch64")]
const GF_LADDER: [GfBackend; 2] = [GfBackend::Neon, GfBackend::Scalar];
/// Every other architecture has only the portable scalar rung.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const GF_LADDER: [GfBackend; 1] = [GfBackend::Scalar];

fn detect_best_backend() -> GfBackend {
    GF_LADDER
        .into_iter()
        .find(|b| b.available())
        .unwrap_or(GfBackend::Scalar)
}

/// Detection cache: the auto-selected backend, computed once on first use.
/// `BACKEND_UNINIT` until then. This is a pure cache - there is no process-wide
/// override (a global mutable backend would bleed across parallel tests and is
/// the wrong shape); per-call backend selection goes through
/// [`gf_mul_add_backend`] and per-code-instance selection through
/// [`RsCode::with_backend`].
const BACKEND_UNINIT: u8 = 0xFF;
static SELECTED_BACKEND: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(BACKEND_UNINIT);

#[inline]
fn current_backend() -> GfBackend {
    use std::sync::atomic::Ordering;
    let v = SELECTED_BACKEND.load(Ordering::Relaxed);
    if v == BACKEND_UNINIT {
        let b = detect_best_backend();
        SELECTED_BACKEND.store(b as u8, Ordering::Relaxed);
        b
    } else {
        GfBackend::from_u8(v)
    }
}

/// `out[i] ^= gf::mul(coef, src[i])` over the whole slice - the hot inner step
/// of RS encode and decode, run through the given GF(256) `backend` (GFNI /
/// AVX-512 / AVX2 / SSSE3 / scalar). The `coef == 0 / 1` shortcuts skip the
/// multiply entirely.
#[inline]
fn gf_mul_add(backend: GfBackend, out: &mut [u8], src: &[u8], coef: u8) {
    debug_assert_eq!(out.len(), src.len());
    if coef == 0 {
        return;
    }
    if coef == 1 {
        for (o, &s) in out.iter_mut().zip(src) {
            *o ^= s;
        }
        return;
    }
    gf_mul_add_backend(backend, out, src, coef);
}

/// `out[i] ^= gf::mul(coef, src[i])` through the auto-detected fastest backend.
/// The public entry point for callers outside `RsCode` (the sliding-window RLC
/// FEC) that want the SIMD-accelerated GF(2^8) multiply-add without managing a
/// backend. The `coef == 0 / 1` shortcuts skip the multiply.
pub fn gf_mul_add_auto(out: &mut [u8], src: &[u8], coef: u8) {
    gf_mul_add(current_backend(), out, src, coef);
}

/// Run `gf_mul_add` through a specific [`GfBackend`] - the A/B-bench and
/// emulation-validation entry point. The hardware rungs require their ISA
/// feature; call only those `GfBackend::available()` reports (the
/// `debug_assert` catches a mismatch in test builds).
pub fn gf_mul_add_backend(backend: GfBackend, out: &mut [u8], src: &[u8], coef: u8) {
    debug_assert_eq!(out.len(), src.len());
    debug_assert!(
        backend.available(),
        "GF backend {} is not available on this host",
        backend.name()
    );
    match backend {
        GfBackend::Scalar => gf_mul_add_scalar(out, src, coef),
        GfBackend::AffineScalar => gf_mul_add_affine_scalar(out, src, coef),
        #[cfg(target_arch = "aarch64")]
        // SAFETY: reached only for an available() Neon backend; aarch64 has
        // baseline NEON; lengths are matched by the debug_assert.
        GfBackend::Neon => unsafe { gf_mul_add_neon(out, src, coef) },
        #[cfg(not(target_arch = "aarch64"))]
        GfBackend::Neon => gf_mul_add_scalar(out, src, coef),
        #[cfg(target_arch = "x86_64")]
        // SAFETY: each arm is reached only for an available() backend, so its
        // ISA feature is present; lengths are matched by the debug_assert.
        GfBackend::Ssse3 => unsafe { gf_mul_add_ssse3(out, src, coef) },
        #[cfg(target_arch = "x86_64")]
        GfBackend::Avx2 => unsafe { gf_mul_add_avx2(out, src, coef) },
        #[cfg(target_arch = "x86_64")]
        GfBackend::Avx512Pshufb => unsafe { gf_mul_add_avx512(out, src, coef) },
        #[cfg(target_arch = "x86_64")]
        GfBackend::Gfni256 => unsafe { gf_mul_add_gfni256(out, src, coef) },
        #[cfg(target_arch = "x86_64")]
        GfBackend::Gfni512 => unsafe { gf_mul_add_gfni512(out, src, coef) },
        #[cfg(not(target_arch = "x86_64"))]
        _ => gf_mul_add_scalar(out, src, coef),
    }
}

fn gf_mul_add_scalar(out: &mut [u8], src: &[u8], coef: u8) {
    for (o, &s) in out.iter_mut().zip(src) {
        *o ^= gf::mul(coef, s);
    }
}

/// Low / high nibble multiply tables for `coef`: `lo[i] = coef*i`,
/// `hi[i] = coef*(i<<4)` over GF(256). The byte-shuffle backends gather each
/// into place by 16-byte table lookup: x86 PSHUFB (`_mm_shuffle_epi8`) and
/// ARM NEON TBL (`vqtbl1q_u8`). On targets with neither, the scalar path
/// multiplies directly and this table is unused.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[inline]
fn gf_nibble_tables(coef: u8) -> ([u8; 16], [u8; 16]) {
    let mut lo = [0u8; 16];
    let mut hi = [0u8; 16];
    for i in 0..16u8 {
        lo[i as usize] = gf::mul(coef, i);
        hi[i as usize] = gf::mul(coef, i << 4);
    }
    (lo, hi)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn gf_mul_add_ssse3(out: &mut [u8], src: &[u8], coef: u8) {
    use std::arch::x86_64::*;
    let (lo, hi) = gf_nibble_tables(coef);
    let n = out.len();
    // SAFETY: ssse3 enabled at the call site; every load/store is bounded
    // by `i + 16 <= n` or the scalar tail.
    unsafe {
        let lo_v = _mm_loadu_si128(lo.as_ptr() as *const __m128i);
        let hi_v = _mm_loadu_si128(hi.as_ptr() as *const __m128i);
        let mask = _mm_set1_epi8(0x0f);
        let mut i = 0usize;
        while i + 16 <= n {
            let s = _mm_loadu_si128(src.as_ptr().add(i) as *const __m128i);
            let lo_n = _mm_and_si128(s, mask);
            let hi_n = _mm_and_si128(_mm_srli_epi16(s, 4), mask);
            let prod = _mm_xor_si128(_mm_shuffle_epi8(lo_v, lo_n), _mm_shuffle_epi8(hi_v, hi_n));
            let o = _mm_loadu_si128(out.as_ptr().add(i) as *const __m128i);
            _mm_storeu_si128(out.as_mut_ptr().add(i) as *mut __m128i, _mm_xor_si128(o, prod));
            i += 16;
        }
        while i < n {
            out[i] ^= gf::mul(coef, src[i]);
            i += 1;
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn gf_mul_add_avx2(out: &mut [u8], src: &[u8], coef: u8) {
    use std::arch::x86_64::*;
    let (lo, hi) = gf_nibble_tables(coef);
    let n = out.len();
    // SAFETY: avx2 enabled at the call site; every load/store is bounded
    // by `i + 32 <= n` or the scalar tail.
    unsafe {
        let lo_v = _mm256_broadcastsi128_si256(_mm_loadu_si128(lo.as_ptr() as *const __m128i));
        let hi_v = _mm256_broadcastsi128_si256(_mm_loadu_si128(hi.as_ptr() as *const __m128i));
        let mask = _mm256_set1_epi8(0x0f);
        let mut i = 0usize;
        while i + 32 <= n {
            let s = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
            let lo_n = _mm256_and_si256(s, mask);
            let hi_n = _mm256_and_si256(_mm256_srli_epi16(s, 4), mask);
            let prod = _mm256_xor_si256(
                _mm256_shuffle_epi8(lo_v, lo_n),
                _mm256_shuffle_epi8(hi_v, hi_n),
            );
            let o = _mm256_loadu_si256(out.as_ptr().add(i) as *const __m256i);
            _mm256_storeu_si256(out.as_mut_ptr().add(i) as *mut __m256i, _mm256_xor_si256(o, prod));
            i += 32;
        }
        while i < n {
            out[i] ^= gf::mul(coef, src[i]);
            i += 1;
        }
    }
}

/// The 8x8 GF(2) matrix (packed into a u64 in GFNI byte order) that maps
/// `x -> gf::mul(coef, x)` over GF(256) with the field's 0x11D polynomial.
///
/// `GF2P8AFFINEQB` computes output bit `i` as `parity(A.byte[7-i] AND x)`, so
/// `A.byte[7-i]` is the linear form for output bit `i`: its bit `j` is set if
/// and only if bit `i` of `coef * 2^j` is set (multiply-by-`coef` is
/// GF(2)-linear, and the polynomial lives entirely in this precomputed matrix).
/// The matrix feeds both the hardware GFNI instruction and the
/// [`gf2p8affine_byte`] software emulation, so the two are bit-identical by
/// construction.
fn gf_affine_matrix(coef: u8) -> u64 {
    let mut m = 0u64;
    for i in 0..8usize {
        let mut row = 0u8;
        for j in 0..8usize {
            if (gf::mul(coef, 1u8 << j) >> i) & 1 == 1 {
                row |= 1u8 << j;
            }
        }
        let byte_idx = 7 - i;
        m |= (row as u64) << (8 * byte_idx);
    }
    m
}

/// Software emulation of one `GF2P8AFFINEQB` byte (imm = 0): bit-exact to the
/// GFNI hardware instruction, so the affine GF(2^8) multiply can be validated
/// on a host without the silicon. `out.bit[i] = parity(m.byte[7-i] AND x)`.
#[inline]
fn gf2p8affine_byte(x: u8, m: u64) -> u8 {
    let mut out = 0u8;
    for i in 0..8usize {
        let mat_byte = ((m >> (8 * (7 - i))) & 0xff) as u8;
        if (mat_byte & x).count_ones() & 1 == 1 {
            out |= 1u8 << i;
        }
    }
    out
}

/// `gf_mul_add` via the software affine transform: the bit-exact emulation of
/// the GFNI hardware path, runnable on any host. The matrix is built once per
/// coefficient, then applied per byte.
fn gf_mul_add_affine_scalar(out: &mut [u8], src: &[u8], coef: u8) {
    let m = gf_affine_matrix(coef);
    for (o, &s) in out.iter_mut().zip(src) {
        *o ^= gf2p8affine_byte(s, m);
    }
}

/// AVX-512BW PSHUFB nibble-table multiply: the same nibble-table technique as
/// the AVX2 path, 64 bytes per `_mm512_shuffle_epi8`. For AVX-512 hosts without
/// GFNI (Skylake-X / Cascade Lake). Bit-identical to the AVX2 path, which is
/// its emulation on a narrower host.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn gf_mul_add_avx512(out: &mut [u8], src: &[u8], coef: u8) {
    use std::arch::x86_64::*;
    let (lo, hi) = gf_nibble_tables(coef);
    let n = out.len();
    // SAFETY: avx512f+avx512bw enabled at the call site; every load/store is
    // bounded by `i + 64 <= n` or the scalar tail.
    unsafe {
        let lo_v = _mm512_broadcast_i32x4(_mm_loadu_si128(lo.as_ptr() as *const __m128i));
        let hi_v = _mm512_broadcast_i32x4(_mm_loadu_si128(hi.as_ptr() as *const __m128i));
        let mask = _mm512_set1_epi8(0x0f);
        let mut i = 0usize;
        while i + 64 <= n {
            let s = _mm512_loadu_si512(src.as_ptr().add(i) as *const __m512i);
            let lo_n = _mm512_and_si512(s, mask);
            let hi_n = _mm512_and_si512(_mm512_srli_epi16::<4>(s), mask);
            let prod = _mm512_xor_si512(
                _mm512_shuffle_epi8(lo_v, lo_n),
                _mm512_shuffle_epi8(hi_v, hi_n),
            );
            let o = _mm512_loadu_si512(out.as_ptr().add(i) as *const __m512i);
            _mm512_storeu_si512(
                out.as_mut_ptr().add(i) as *mut __m512i,
                _mm512_xor_si512(o, prod),
            );
            i += 64;
        }
        while i < n {
            out[i] ^= gf::mul(coef, src[i]);
            i += 1;
        }
    }
}

/// GFNI affine multiply on 256-bit lanes: one `_mm256_gf2p8affine_epi64_epi8`
/// does the GF(2^8) multiply-by-`coef` in hardware, no nibble table. Needs only
/// `gfni` + `avx2`, so it reaches consumer Zen 4 / Alder Lake+ without AVX-512.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "gfni,avx2")]
unsafe fn gf_mul_add_gfni256(out: &mut [u8], src: &[u8], coef: u8) {
    use std::arch::x86_64::*;
    let matrix = _mm256_set1_epi64x(gf_affine_matrix(coef) as i64);
    let n = out.len();
    // SAFETY: gfni+avx2 enabled at the call site; loads/stores bounded by
    // `i + 32 <= n` or the scalar tail.
    unsafe {
        let mut i = 0usize;
        while i + 32 <= n {
            let s = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
            let prod = _mm256_gf2p8affine_epi64_epi8::<0>(s, matrix);
            let o = _mm256_loadu_si256(out.as_ptr().add(i) as *const __m256i);
            _mm256_storeu_si256(
                out.as_mut_ptr().add(i) as *mut __m256i,
                _mm256_xor_si256(o, prod),
            );
            i += 32;
        }
        while i < n {
            out[i] ^= gf::mul(coef, src[i]);
            i += 1;
        }
    }
}

/// GFNI affine multiply on 512-bit lanes: 64 bytes of GF(2^8) multiply-by-`coef`
/// per `_mm512_gf2p8affine_epi64_epi8`, a hardware field multiply with no table
/// lookup. The top rung (Genoa / Sapphire Rapids / Zen 4+).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "gfni,avx512f,avx512bw")]
unsafe fn gf_mul_add_gfni512(out: &mut [u8], src: &[u8], coef: u8) {
    use std::arch::x86_64::*;
    let matrix = _mm512_set1_epi64(gf_affine_matrix(coef) as i64);
    let n = out.len();
    // SAFETY: gfni+avx512f+avx512bw enabled at the call site; loads/stores
    // bounded by `i + 64 <= n` or the scalar tail.
    unsafe {
        let mut i = 0usize;
        while i + 64 <= n {
            let s = _mm512_loadu_si512(src.as_ptr().add(i) as *const __m512i);
            let prod = _mm512_gf2p8affine_epi64_epi8::<0>(s, matrix);
            let o = _mm512_loadu_si512(out.as_ptr().add(i) as *const __m512i);
            _mm512_storeu_si512(
                out.as_mut_ptr().add(i) as *mut __m512i,
                _mm512_xor_si512(o, prod),
            );
            i += 64;
        }
        while i < n {
            out[i] ^= gf::mul(coef, src[i]);
            i += 1;
        }
    }
}

/// ARM NEON TBL nibble-table multiply: the aarch64 mirror of the SSSE3 path,
/// 16 bytes per `vqtbl1q_u8`. `vshrq_n_u8::<4>` extracts each byte's high
/// nibble in one byte-wise shift (no mask needed, unlike x86's 16-bit shift),
/// and the two table lookups XOR to `coef * byte` over GF(256) - bit-identical
/// to the SSSE3 / scalar result.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn gf_mul_add_neon(out: &mut [u8], src: &[u8], coef: u8) {
    use std::arch::aarch64::*;
    let (lo, hi) = gf_nibble_tables(coef);
    let n = out.len();
    // SAFETY: neon enabled at the call site; every load/store is bounded by
    // `i + 16 <= n` or the scalar tail.
    unsafe {
        let lo_v = vld1q_u8(lo.as_ptr());
        let hi_v = vld1q_u8(hi.as_ptr());
        let mask = vdupq_n_u8(0x0f);
        let mut i = 0usize;
        while i + 16 <= n {
            let s = vld1q_u8(src.as_ptr().add(i));
            let lo_n = vandq_u8(s, mask);
            let hi_n = vshrq_n_u8::<4>(s);
            let prod = veorq_u8(vqtbl1q_u8(lo_v, lo_n), vqtbl1q_u8(hi_v, hi_n));
            let o = vld1q_u8(out.as_ptr().add(i));
            vst1q_u8(out.as_mut_ptr().add(i), veorq_u8(o, prod));
            i += 16;
        }
        while i < n {
            out[i] ^= gf::mul(coef, src[i]);
            i += 1;
        }
    }
}

/// A systematic Cauchy Reed-Solomon erasure code: `k` data shards plus
/// `r` parity shards, recovering any `k` of the `k + r`.
#[derive(Debug, Clone)]
pub struct RsCode {
    k: usize,
    r: usize,
    /// Row-major `r * k` Cauchy parity matrix: `parity[j] = sum_c
    /// cauchy[j*k + c] * data[c]`.
    cauchy: Vec<u8>,
    /// Optional GF(256) backend override for this code (`None` = the auto-
    /// detected fastest rung). Per-instance, not a process global, so an A/B
    /// bench or an emulation-validation test can pin a backend without bleeding
    /// into other code paths.
    backend: Option<GfBackend>,
}

/// Error from constructing or decoding an [`RsCode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FecError {
    /// `k == 0`, `r == 0`, or `k + r > 256` (field size).
    BadParams,
    /// Fewer than `k` shards survived; FEC cannot recover (ARQ falls
    /// back here).
    TooFewShards,
    /// Shards had unequal or zero length.
    BadShardLen,
}

impl RsCode {
    /// Build a `(k, r)` systematic Cauchy-RS code. `k >= 1`, `r >= 1`,
    /// `k + r <= 256`.
    pub fn new(k: usize, r: usize) -> Result<Self, FecError> {
        if k == 0 || r == 0 || k + r > 256 {
            return Err(FecError::BadParams);
        }
        // Cauchy entry C[j][c] = 1 / ((k + j) XOR c). Parity indices
        // {k..k+r} and data indices {0..k} are disjoint, so the XOR is
        // never zero and every square submatrix of `[I_k ; C]` is
        // invertible (MDS).
        let mut cauchy = vec![0u8; r * k];
        for j in 0..r {
            for c in 0..k {
                let x = (k + j) as u8 ^ c as u8;
                cauchy[j * k + c] = gf::inv(x);
            }
        }
        Ok(Self { k, r, cauchy, backend: None })
    }

    /// Pin the GF(256) backend for this code (an A/B-bench / emulation-
    /// validation knob), or `None` to use the auto-detected fastest rung. The
    /// override is per-instance, so it never affects another `RsCode`.
    pub fn with_backend(mut self, backend: Option<GfBackend>) -> Self {
        self.backend = backend;
        self
    }

    /// The backend this code uses: its pinned override, else the auto-detected
    /// fastest available rung.
    #[inline]
    fn effective_backend(&self) -> GfBackend {
        self.backend.unwrap_or_else(current_backend)
    }

    /// Number of data shards.
    pub fn k(&self) -> usize {
        self.k
    }

    /// Number of parity shards.
    pub fn r(&self) -> usize {
        self.r
    }

    /// Compute the `r` parity shards from the `k` data shards. All
    /// shards (data and parity) must have the same length.
    pub fn encode(
        &self,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), FecError> {
        if data.len() != self.k || parity.len() != self.r {
            return Err(FecError::BadParams);
        }
        let len = data[0].len();
        if len == 0
            || data.iter().any(|s| s.len() != len)
            || parity.iter().any(|s| s.len() != len)
        {
            return Err(FecError::BadShardLen);
        }
        let backend = self.effective_backend();
        for j in 0..self.r {
            let row = &self.cauchy[j * self.k..(j + 1) * self.k];
            parity[j].fill(0);
            for c in 0..self.k {
                let coef = row[c];
                if coef != 0 {
                    gf_mul_add(backend, parity[j], data[c], coef);
                }
            }
        }
        Ok(())
    }

    /// Recover the missing data shards in place. `shards` has `k + r`
    /// entries (data shards first, then parity); `Some` = received,
    /// `None` = lost. On success every data-shard slot `0..k` is
    /// `Some`. Returns `TooFewShards` if fewer than `k` survived.
    pub fn decode(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), FecError> {
        let n = self.k + self.r;
        if shards.len() != n {
            return Err(FecError::BadParams);
        }
        // Already-present data shards need nothing.
        if (0..self.k).all(|i| shards[i].is_some()) {
            return Ok(());
        }
        // Pick the first k surviving shard positions.
        let mut surv: Vec<usize> = Vec::with_capacity(self.k);
        let mut len = 0usize;
        for (idx, s) in shards.iter().enumerate() {
            if let Some(v) = s
                && surv.len() < self.k
            {
                if len == 0 {
                    len = v.len();
                } else if v.len() != len {
                    return Err(FecError::BadShardLen);
                }
                surv.push(idx);
            }
        }
        if surv.len() < self.k || len == 0 {
            return Err(FecError::TooFewShards);
        }
        // Build the k x k encoding-matrix submatrix for the survivors,
        // then invert it: data = A^-1 * survivors.
        let mut a = vec![0u8; self.k * self.k];
        for (row, &pos) in surv.iter().enumerate() {
            for c in 0..self.k {
                a[row * self.k + c] = self.enc_entry(pos, c);
            }
        }
        let inv = invert(&a, self.k).ok_or(FecError::TooFewShards)?;
        let backend = self.effective_backend();
        // Recover each missing data shard i: data[i] = sum_j inv[i][j]
        // * survivor_shard[j].
        for i in 0..self.k {
            if shards[i].is_some() {
                continue;
            }
            let mut out = vec![0u8; len];
            for j in 0..self.k {
                let coef = inv[i * self.k + j];
                if coef == 0 {
                    continue;
                }
                let src = shards[surv[j]].as_ref().unwrap();
                gf_mul_add(backend, &mut out, src, coef);
            }
            shards[i] = Some(out);
        }
        Ok(())
    }

    /// Entry `(row, col)` of the full `(k + r) x k` encoding matrix
    /// `[I_k ; Cauchy]`.
    #[inline]
    fn enc_entry(&self, row: usize, col: usize) -> u8 {
        if row < self.k {
            if row == col {
                1
            } else {
                0
            }
        } else {
            self.cauchy[(row - self.k) * self.k + col]
        }
    }
}

/// Invert an `n x n` GF(256) matrix (row-major) by Gauss-Jordan
/// elimination. Returns `None` if singular.
fn invert(m: &[u8], n: usize) -> Option<Vec<u8>> {
    let mut a = m.to_vec();
    let mut inv = vec![0u8; n * n];
    for i in 0..n {
        inv[i * n + i] = 1;
    }
    for col in 0..n {
        // Find a pivot row with a nonzero entry in `col`.
        let mut piv = col;
        while piv < n && a[piv * n + col] == 0 {
            piv += 1;
        }
        if piv == n {
            return None; // singular
        }
        if piv != col {
            for c in 0..n {
                a.swap(col * n + c, piv * n + c);
                inv.swap(col * n + c, piv * n + c);
            }
        }
        // Normalize the pivot row.
        let pv = a[col * n + col];
        let pinv = gf::inv(pv);
        for c in 0..n {
            a[col * n + c] = gf::mul(a[col * n + c], pinv);
            inv[col * n + c] = gf::mul(inv[col * n + c], pinv);
        }
        // Eliminate `col` from every other row.
        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = a[row * n + col];
            if factor == 0 {
                continue;
            }
            for c in 0..n {
                a[row * n + c] ^= gf::mul(factor, a[col * n + c]);
                inv[row * n + c] ^= gf::mul(factor, inv[col * n + c]);
            }
        }
    }
    Some(inv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gf_field_laws() {
        // 0 is additive identity; 1 is multiplicative identity.
        for a in 0u8..=255 {
            assert_eq!(gf::add(a, 0), a);
            assert_eq!(gf::mul(a, 1), a);
            assert_eq!(gf::mul(a, 0), 0);
        }
        // a * inv(a) == 1 for every nonzero a.
        for a in 1u8..=255 {
            assert_eq!(gf::mul(a, gf::inv(a)), 1, "inverse of {a}");
        }
        // mul is commutative and div is its inverse.
        for a in 0u8..=255 {
            for b in 1u8..=255 {
                assert_eq!(gf::mul(a, b), gf::mul(b, a));
                assert_eq!(gf::div(gf::mul(a, b), b), a);
            }
        }
    }

    /// Exhaustively check that EVERY loss pattern dropping up to `r`
    /// shards recovers the original data exactly. This validates the
    /// Cauchy matrix construction and the decoder together.
    fn exhaustive_recovery(k: usize, r: usize, len: usize) {
        exhaustive_recovery_backend(k, r, len, None);
    }

    /// As [`exhaustive_recovery`], but pinning the GF(256) `backend` on the
    /// code instance (no process-global state, so it is safe under the
    /// parallel test runner).
    fn exhaustive_recovery_backend(k: usize, r: usize, len: usize, backend: Option<GfBackend>) {
        let code = RsCode::new(k, r).expect("code").with_backend(backend);
        // Deterministic pseudo-random data shards.
        let data: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                (0..len)
                    .map(|b| ((i * 131 + b * 17 + 7) & 0xFF) as u8)
                    .collect()
            })
            .collect();
        let mut parity: Vec<Vec<u8>> = vec![vec![0u8; len]; r];
        {
            let data_refs: Vec<&[u8]> = data.iter().map(|s| s.as_slice()).collect();
            let mut par_refs: Vec<&mut [u8]> =
                parity.iter_mut().map(|s| s.as_mut_slice()).collect();
            code.encode(&data_refs, &mut par_refs).expect("encode");
        }
        let n = k + r;
        // All shards present.
        let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();
        // Every subset of lost positions of size 1..=r.
        for lost_count in 1..=r {
            // Iterate all combinations via bit masks of n bits with
            // exactly `lost_count` bits set.
            for mask in 0u32..(1 << n) {
                if (mask.count_ones() as usize) != lost_count {
                    continue;
                }
                let mut shards: Vec<Option<Vec<u8>>> = all
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        if mask & (1 << i) != 0 {
                            None
                        } else {
                            Some(s.clone())
                        }
                    })
                    .collect();
                code.decode(&mut shards).expect("decode");
                for i in 0..k {
                    assert_eq!(
                        shards[i].as_ref().unwrap(),
                        &data[i],
                        "k={k} r={r} mask={mask:b}: data shard {i} mismatch"
                    );
                }
            }
        }
    }

    #[test]
    fn recovery_k4_r2() {
        exhaustive_recovery(4, 2, 32);
    }

    #[test]
    fn recovery_k6_r3() {
        exhaustive_recovery(6, 3, 16);
    }

    #[test]
    fn recovery_k8_r4() {
        exhaustive_recovery(8, 4, 8);
    }

    #[test]
    fn recovery_k1_r1() {
        exhaustive_recovery(1, 1, 64);
    }

    #[test]
    fn recovery_k16_r16_high_parity() {
        // r=16 (k+r=32, the per-block bitmap max) is far past the exhaustive
        // tests' r<=4, and 2^32 masks cannot be enumerated. Spot-check the worst
        // cases (all 16 data shards dropped -> reconstruct entirely from parity;
        // all parity dropped) plus a deterministic spread of 16-of-32 erasures,
        // confirming the Cauchy decode is sound at the high parity the lifted
        // r_max allows.
        let (k, r, len) = (16usize, 16usize, 64usize);
        let code = RsCode::new(k, r).expect("code");
        let data: Vec<Vec<u8>> = (0..k)
            .map(|i| (0..len).map(|b| ((i * 131 + b * 17 + 7) & 0xFF) as u8).collect())
            .collect();
        let mut parity: Vec<Vec<u8>> = vec![vec![0u8; len]; r];
        {
            let dr: Vec<&[u8]> = data.iter().map(|s| s.as_slice()).collect();
            let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(|s| s.as_mut_slice()).collect();
            code.encode(&dr, &mut pr).expect("encode");
        }
        let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();
        let n = (k + r) as u32;
        // n == 32 makes `1 << n` overflow u32; build the full n-bit mask safely.
        let full: u32 = if n >= 32 { u32::MAX } else { (1u32 << n) - 1 };
        let mut masks: Vec<u32> = vec![
            (1u32 << k) - 1,                  // all data shards dropped
            full & !((1u32 << k) - 1),        // all parity shards dropped
            0x5555_5555 & full,               // every other shard
        ];
        let mut st: u32 = 0x1234_5678;
        while masks.len() < 30 {
            let (mut m, mut cnt) = (0u32, 0);
            while cnt < 16 {
                st = st.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                let bit = (st >> 16) % n;
                if m & (1 << bit) == 0 {
                    m |= 1 << bit;
                    cnt += 1;
                }
            }
            masks.push(m);
        }
        for mask in masks {
            assert_eq!(mask.count_ones(), 16, "mask must drop exactly r=16");
            let mut shards: Vec<Option<Vec<u8>>> = all
                .iter()
                .enumerate()
                .map(|(i, s)| if mask & (1 << i) != 0 { None } else { Some(s.clone()) })
                .collect();
            code.decode(&mut shards).expect("decode r=16");
            for i in 0..k {
                assert_eq!(shards[i].as_ref().unwrap(), &data[i], "mask={mask:b}: data {i} mismatch");
            }
        }
    }

    #[test]
    fn too_few_shards_is_reported() {
        let code = RsCode::new(4, 2).expect("code");
        let len = 16;
        let data: Vec<Vec<u8>> = (0..4).map(|_| vec![1u8; len]).collect();
        let mut parity: Vec<Vec<u8>> = vec![vec![0u8; len]; 2];
        {
            let dr: Vec<&[u8]> = data.iter().map(|s| s.as_slice()).collect();
            let mut pr: Vec<&mut [u8]> = parity.iter_mut().map(|s| s.as_mut_slice()).collect();
            code.encode(&dr, &mut pr).expect("encode");
        }
        // Drop 3 of 6 (more than r=2): unrecoverable by FEC.
        let mut shards: Vec<Option<Vec<u8>>> = vec![
            None,
            None,
            None,
            Some(data[3].clone()),
            Some(parity[0].clone()),
            Some(parity[1].clone()),
        ];
        assert_eq!(code.decode(&mut shards), Err(FecError::TooFewShards));
    }

    #[test]
    fn gfni_affine_matrix_matches_field_multiply() {
        // Multiply-by-1 is the GF2P8AFFINEQB identity matrix - the anchor that
        // pins the byte/bit convention to the hardware spec.
        assert_eq!(
            gf_affine_matrix(1),
            0x0102_0408_1020_4080,
            "multiply-by-1 must be the GF2P8AFFINEQB identity matrix"
        );
        // For every coefficient and byte, the software affine transform fed the
        // per-coefficient matrix equals the field multiply. Since the hardware
        // GFNI instruction implements the same spec with the same matrix, this
        // validates the GFNI path's correctness without the silicon.
        for coef in 0u16..=255 {
            let m = gf_affine_matrix(coef as u8);
            for x in 0u16..=255 {
                assert_eq!(
                    gf2p8affine_byte(x as u8, m),
                    gf::mul(coef as u8, x as u8),
                    "affine(coef={coef}, x={x}) != gf::mul"
                );
            }
        }
    }

    #[test]
    fn all_available_backends_match_scalar() {
        // Every GF backend this host can run must produce byte-identical output
        // to the scalar reference - the fallback-chain correctness contract.
        // On a GFNI / AVX-512 host this also validates the hardware rungs; here
        // it validates scalar, the affine emulation, SSSE3, and AVX2.
        use GfBackend::*;
        let candidates = [Scalar, AffineScalar, Ssse3, Avx2, Avx512Pshufb, Gfni256, Gfni512, Neon];
        let n = 1000usize;
        let src: Vec<u8> = (0..n).map(|i| ((i * 73 + 11) & 0xff) as u8).collect();
        let init: Vec<u8> = (0..n).map(|i| ((i * 31 + 7) & 0xff) as u8).collect();
        for coef in [2u8, 7, 100, 255] {
            let mut want = init.clone();
            gf_mul_add_backend(Scalar, &mut want, &src, coef);
            for &b in &candidates {
                if !b.available() {
                    continue;
                }
                let mut got = init.clone();
                gf_mul_add_backend(b, &mut got, &src, coef);
                assert_eq!(got, want, "backend {} disagrees at coef {coef}", b.name());
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn neon_backend_matches_scalar_and_recovers() {
        // NEON is baseline on every aarch64 CPU, so the rung must be available
        // and auto-selected here.
        assert!(GfBackend::Neon.available(), "NEON must be available on aarch64");
        assert_eq!(detect_best_backend(), GfBackend::Neon, "aarch64 must pick NEON");
        // Bit-exact vs scalar across coefficients and a length that exercises
        // both the 16-byte NEON body and the scalar tail.
        let n = 1000usize;
        let src: Vec<u8> = (0..n).map(|i| ((i * 73 + 11) & 0xff) as u8).collect();
        let init: Vec<u8> = (0..n).map(|i| ((i * 31 + 7) & 0xff) as u8).collect();
        for coef in [2u8, 7, 100, 255] {
            let mut want = init.clone();
            gf_mul_add_backend(GfBackend::Scalar, &mut want, &src, coef);
            let mut got = init.clone();
            gf_mul_add_backend(GfBackend::Neon, &mut got, &src, coef);
            assert_eq!(got, want, "NEON disagrees with scalar at coef {coef}");
        }
        // Full RS encode/decode-with-loss recovery pinned to the NEON backend.
        exhaustive_recovery_backend(4, 2, 32, Some(GfBackend::Neon));
        exhaustive_recovery_backend(6, 3, 16, Some(GfBackend::Neon));
        exhaustive_recovery_backend(8, 4, 8, Some(GfBackend::Neon));
    }

    #[test]
    fn exhaustive_recovery_via_gfni_emulation() {
        // Run the whole encode/decode path through the GFNI software emulation
        // (pinned per-instance, not via any global), so RS recovery is
        // validated end-to-end through the affine transform - the GFNI logic
        // proven without the silicon. Because the backend is on the code
        // instance, this is safe under the parallel test runner.
        exhaustive_recovery_backend(4, 2, 32, Some(GfBackend::AffineScalar));
        exhaustive_recovery_backend(6, 3, 16, Some(GfBackend::AffineScalar));
        exhaustive_recovery_backend(8, 4, 8, Some(GfBackend::AffineScalar));
    }
}
