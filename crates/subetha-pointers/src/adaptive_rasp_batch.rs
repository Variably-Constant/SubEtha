//! `RaspBatch<T>` - structure-of-arrays (SoA) storage for bounds-
//! checked pointers, designed for high-throughput SIMD batch
//! validation.
//!
//! Where `RaspPointer<T>` is an array-of-structures (AoS) 16-byte
//! pointer that fits in one XMM register, `RaspBatch<T>` flips the
//! layout: instead of storing the four fields (ptr, base, length,
//! perms) packed in 16 bytes per pointer, it stores N pointers as
//! four parallel `Vec`s. Each field's values are contiguous in
//! memory, so SIMD batch validation can load 4 consecutive ptrs
//! into one YMM register via a single `vmovdqu` - no GPR→SIMD
//! domain crossings, no per-call ABI prologue overhead, and the
//! `vpcmpgtq` packed-quadword compare runs at its design speed.
//!
//! # Memory layout
//!
//! ```text
//! ptrs:    [u64, u64, u64, u64, ...]   (8 bytes per slot, contiguous)
//! bases:   [u64, u64, u64, u64, ...]
//! lengths: [u32, u32, u32, u32, ...]   (4 bytes per slot, contiguous)
//! perms:   [u32, u32, u32, u32, ...]   (sealed = high bit of u32)
//! ```
//!
//! All four `Vec`s share the same length; index `i` reads
//! `(ptrs[i], bases[i], lengths[i], perms[i])` as the i-th pointer.
//!
//! # SIMD batch validation
//!
//! `check_read_all_avx2` validates the entire batch by processing 4
//! consecutive entries per loop iteration:
//!
//! 1. `vmovdqu ymm0, [ptrs+offset]`    - 4 u64 ptrs into one YMM
//! 2. `vmovdqu ymm1, [bases+offset]`   - 4 u64 bases into one YMM
//! 3. `vpcmpgtq ymm2, ymm1, ymm0`      - parallel "base > ptr" check
//! 4. `vmovdqu xmm3, [lengths+offset]` - 4 u32 lengths into one XMM
//! 5. `vpmovzxdq ymm3, xmm3`           - zero-extend to 4 u64 lanes
//! 6. `vpaddq ymm4, ymm1, ymm3`        - region_end = base + length
//! 7. `vpaddq ymm5, ymm0, [size_t]`    - `access_end = ptr + size_of::<T>()`
//! 8. `vpcmpgtq ymm6, ymm5, ymm4`      - parallel "access_end > region_end"
//! 9. `vmovdqu xmm7, [perms+offset]`   - 4 u32 perms
//! 10. permission + sealed checks via SIMD masks
//!
//! Total: ~12 SIMD instructions per 4 pointers = 3 instructions per
//! pointer. The AoS path's ~30 instructions per 4 pointers (12
//! `vmovq` GPR→SIMD crossings + 6 `vpunpcklqdq` + 3 `vinserti128` +
//! 2 `vpcmpgtq`) is folded into 12 contiguous-load + arithmetic
//! instructions with zero domain crossings.

use std::marker::PhantomData;

/// Per-pointer permission flags. Multiple permissions OR together;
/// the sealed bit lives at position 31 of the u32 perms field.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaspPermission {
    None    = 0,
    Read    = 1 << 0,
    Write   = 1 << 1,
    Execute = 1 << 2,
}

/// Sealed bit position in the u32 perms field. A sealed RASP returns
/// `Err(Sealed)` from all `check_*` paths regardless of permission
/// bits. Sealing is cooperative: a caller who skips the check and
/// dereferences via the raw pointer bypasses sealing.
const SEALED_BIT_U32: u32 = 1 << 31;

/// Errors returned from RASP bounds / permission checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaspError {
    OutOfBounds,
    PermissionDenied,
    Sealed,
    AddressOverflow,
    /// Length did not fit in the layout's 32-bit length field, or
    /// the batch is already at capacity (`u32::MAX` entries).
    LayoutTooWide,
    /// The current CPU does not support the SIMD feature needed by
    /// a SIMD-only entry point. The scalar entry points are always
    /// available.
    FeatureNotSupported,
}

/// Structure-of-arrays storage for bounds-checked pointers.
///
/// `T` is a phantom type; each entry refers to an externally-owned
/// `[T]` region. The caller is responsible for keeping the regions
/// alive for the lifetime of the batch. Use `push_from_slice` with
/// the returned slice as a borrow anchor.
pub struct RaspBatch<T> {
    ptrs: Vec<u64>,
    bases: Vec<u64>,
    lengths: Vec<u32>,
    perms: Vec<u32>,
    _phantom: PhantomData<*const T>,
}

unsafe impl<T: Send> Send for RaspBatch<T> {}
unsafe impl<T: Sync> Sync for RaspBatch<T> {}

/// Position-independent reference into a `RaspBatch<T>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RaspBatchIndex<T> {
    idx: u32,
    _phantom: PhantomData<T>,
}

impl<T> RaspBatchIndex<T> {
    pub const fn new(idx: u32) -> Self {
        Self { idx, _phantom: PhantomData }
    }
    pub const fn raw(&self) -> u32 { self.idx }
}

impl<T> Default for RaspBatch<T> {
    fn default() -> Self { Self::new() }
}

impl<T> RaspBatch<T> {
    pub fn new() -> Self {
        Self {
            ptrs: Vec::new(),
            bases: Vec::new(),
            lengths: Vec::new(),
            perms: Vec::new(),
            _phantom: PhantomData,
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            ptrs: Vec::with_capacity(n),
            bases: Vec::with_capacity(n),
            lengths: Vec::with_capacity(n),
            perms: Vec::with_capacity(n),
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize { self.ptrs.len() }
    #[inline]
    pub fn is_empty(&self) -> bool { self.ptrs.is_empty() }
    pub fn capacity(&self) -> usize { self.ptrs.capacity() }

    /// Push a new pointer from a borrowed slice. The returned slice
    /// is the lifetime anchor; the batch's pointer is valid only as
    /// long as the anchor is held.
    pub fn push_from_slice<'a>(
        &mut self,
        slice: &'a [T],
        perms: u32,
    ) -> Result<(RaspBatchIndex<T>, &'a [T]), RaspError> {
        let ptr_usize = slice.as_ptr() as usize;
        let length_bytes = std::mem::size_of_val(slice);
        if length_bytes > u32::MAX as usize {
            return Err(RaspError::LayoutTooWide);
        }
        let idx = self.ptrs.len();
        if idx >= u32::MAX as usize {
            return Err(RaspError::LayoutTooWide);
        }
        self.ptrs.push(ptr_usize as u64);
        self.bases.push(ptr_usize as u64);
        self.lengths.push(length_bytes as u32);
        self.perms.push(perms);
        Ok((RaspBatchIndex::new(idx as u32), slice))
    }

    /// Push from raw integer fields (caller manages target lifetime).
    pub fn push_raw(
        &mut self,
        ptr: u64,
        base: u64,
        length: u32,
        perms: u32,
    ) -> Result<RaspBatchIndex<T>, RaspError> {
        let idx = self.ptrs.len();
        if idx >= u32::MAX as usize {
            return Err(RaspError::LayoutTooWide);
        }
        if ptr < base {
            return Err(RaspError::OutOfBounds);
        }
        let access_end = (ptr as usize).checked_add(std::mem::size_of::<T>())
            .ok_or(RaspError::AddressOverflow)?;
        let region_end = (base as usize).checked_add(length as usize)
            .ok_or(RaspError::AddressOverflow)?;
        if access_end > region_end {
            return Err(RaspError::OutOfBounds);
        }
        self.ptrs.push(ptr);
        self.bases.push(base);
        self.lengths.push(length);
        self.perms.push(perms);
        Ok(RaspBatchIndex::new(idx as u32))
    }

    /// Per-element scalar check. Used by per-index call sites and as
    /// the correctness oracle for the SIMD batch path.
    pub fn check_read_scalar(
        &self,
        idx: RaspBatchIndex<T>,
    ) -> Result<(), RaspError> {
        let i = idx.idx as usize;
        if i >= self.ptrs.len() {
            return Err(RaspError::OutOfBounds);
        }
        let p = self.perms[i];
        if p & SEALED_BIT_U32 != 0 {
            return Err(RaspError::Sealed);
        }
        if p & (RaspPermission::Read as u32) == 0 {
            return Err(RaspError::PermissionDenied);
        }
        let ptr_u = self.ptrs[i] as usize;
        let base_u = self.bases[i] as usize;
        if ptr_u < base_u {
            return Err(RaspError::OutOfBounds);
        }
        let access_end = ptr_u.checked_add(std::mem::size_of::<T>())
            .ok_or(RaspError::AddressOverflow)?;
        let region_end = base_u.checked_add(self.lengths[i] as usize)
            .ok_or(RaspError::AddressOverflow)?;
        if access_end > region_end {
            return Err(RaspError::OutOfBounds);
        }
        Ok(())
    }

    /// Raw pointer at the given index. Returns `None` for an
    /// out-of-range index. The pointer is NOT validated; pair with
    /// `check_read_scalar` (or use `read_at` which combines both).
    pub fn raw_ptr(&self, idx: RaspBatchIndex<T>) -> Option<*const T> {
        let i = idx.idx as usize;
        if i >= self.ptrs.len() { return None; }
        Some(self.ptrs[i] as usize as *const T)
    }

    /// Bounds + permission check followed by a dereference. The
    /// caller's responsibility for the original target lifetime
    /// still applies (see `push_from_slice`'s borrow anchor).
    ///
    /// # Safety
    ///
    /// The target of the pointer at `idx` must still be valid
    /// (alive, properly aligned for `T`, and accessible via the
    /// permissions encoded). The check enforces the permissions
    /// recorded at push time; it does NOT prove the target's
    /// allocation has not been freed.
    pub unsafe fn read_at(&self, idx: RaspBatchIndex<T>) -> Result<T, RaspError>
    where T: Copy,
    {
        self.check_read_scalar(idx)?;
        let i = idx.idx as usize;
        let p = self.ptrs[i] as usize as *const T;
        // SAFETY: check_read_scalar succeeded, so bounds + perms are
        // satisfied. The caller is responsible for the target's
        // continued validity per push_from_slice's contract.
        Ok(unsafe { *p })
    }

    /// Batch scalar check across the whole array. Reference path.
    pub fn check_read_all_scalar(&self) -> Vec<Result<(), RaspError>> {
        let n = self.len();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(self.check_read_scalar(RaspBatchIndex::new(i as u32)));
        }
        out
    }

    /// Count of valid entries (Ok results) via scalar path. Avoids
    /// the `Vec<Result>` allocation when only the count is needed.
    pub fn count_valid_scalar(&self) -> u32 {
        let n = self.len();
        let mut count = 0u32;
        for i in 0..n {
            if self.check_read_scalar(RaspBatchIndex::new(i as u32)).is_ok() {
                count += 1;
            }
        }
        count
    }

    /// AVX2-accelerated count of valid entries. Processes 4 elements
    /// per loop iteration using contiguous SIMD loads from each
    /// parallel `Vec`.
    ///
    /// # Safety
    ///
    /// Caller must guarantee AVX2 is supported by the runtime CPU.
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[target_feature(enable = "avx2")]
    #[inline]
    pub unsafe fn count_valid_avx2(&self) -> u32 {
        #[cfg(target_arch = "x86_64")]
        use std::arch::x86_64::*;
        #[cfg(target_arch = "x86")]
        use std::arch::x86::*;

        let n = self.len();
        if n == 0 {
            return 0;
        }
        let chunks = n / 4;
        let size_t = std::mem::size_of::<T>() as u64;
        let size_t_vec = _mm256_set1_epi64x(size_t as i64);
        let read_bit_vec = _mm_set1_epi32(RaspPermission::Read as i32);
        let zero128 = _mm_setzero_si128();

        let mut count_in_simd = 0u32;
        for c in 0..chunks {
            let off = c * 4;
            // Contiguous loads - no GPR→SIMD crossings.
            // SAFETY: c < chunks = n/4, so off+4 <= n; in-bounds.
            let v_ptrs = unsafe {
                _mm256_loadu_si256(self.ptrs.as_ptr().add(off) as *const __m256i)
            };
            let v_bases = unsafe {
                _mm256_loadu_si256(self.bases.as_ptr().add(off) as *const __m256i)
            };
            let lengths_xmm = unsafe {
                _mm_loadu_si128(self.lengths.as_ptr().add(off) as *const __m128i)
            };
            let perms_xmm = unsafe {
                _mm_loadu_si128(self.perms.as_ptr().add(off) as *const __m128i)
            };

            // Lower bound: base > ptr → fail
            let cmp_lower = _mm256_cmpgt_epi64(v_bases, v_ptrs);

            // Upper bound: access_end > region_end → fail
            let v_lengths_u64 = _mm256_cvtepu32_epi64(lengths_xmm);
            let v_region_end = _mm256_add_epi64(v_bases, v_lengths_u64);
            let v_access_end = _mm256_add_epi64(v_ptrs, size_t_vec);
            let cmp_upper = _mm256_cmpgt_epi64(v_access_end, v_region_end);

            // Sealed: high bit of u32 perms (sign bit). _mm_movemask_ps
            // emits one bit per 4-byte lane from the sign bit. So a
            // 4-bit mask, one bit per lane, bit set = sealed.
            let sealed_mask4 =
                _mm_movemask_ps(_mm_castsi128_ps(perms_xmm)) as u32;

            // No-read: AND with Read bit, compare to zero. Set bit per
            // lane means that lane LACKS the read permission.
            let read_anded = _mm_and_si128(perms_xmm, read_bit_vec);
            let read_eq_zero = _mm_cmpeq_epi32(read_anded, zero128);
            let no_read_mask4 =
                _mm_movemask_ps(_mm_castsi128_ps(read_eq_zero)) as u32;

            // VPCMPGTQ produces all-ones in matching 64-bit lanes,
            // including bit 63. _mm256_movemask_pd reads bit 63 of
            // each 64-bit lane and emits a 4-bit mask directly -
            // one VMOVMSKPD instead of VPMOVMSKB + 4 conditional ORs.
            let oob_lower_4 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp_lower)) as u32;
            let oob_upper_4 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp_upper)) as u32;

            // A lane is invalid if ANY of the 4 failure conditions
            // are set.
            let any_fail = sealed_mask4 | no_read_mask4 | oob_lower_4 | oob_upper_4;
            // Valid lanes: bits 0..4 NOT set in any_fail.
            let valid_in_chunk = 4 - (any_fail & 0xF).count_ones();
            count_in_simd += valid_in_chunk;
        }

        // Remainder (n % 4) via scalar.
        let mut count_scalar = 0u32;
        for i in (chunks * 4)..n {
            if self.check_read_scalar(RaspBatchIndex::new(i as u32)).is_ok() {
                count_scalar += 1;
            }
        }
        count_in_simd + count_scalar
    }

    /// AVX-512F count of valid entries. Processes 8 elements per
    /// iteration: ZMM (8x u64) loads for ptrs/bases, YMM (8x u32)
    /// loads for lengths/perms, mask-producing
    /// `_mm512_cmpgt_epi64_mask` for both bounds checks. Doubles the
    /// per-iteration throughput of the AVX2 path.
    ///
    /// # Safety
    /// Caller must guarantee AVX-512F is supported by the runtime CPU.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    #[inline]
    pub unsafe fn count_valid_avx512(&self) -> u32 {
        use std::arch::x86_64::*;

        let n = self.len();
        if n == 0 {
            return 0;
        }
        let chunks = n / 8;
        let size_t = std::mem::size_of::<T>() as u64;
        let size_t_vec = _mm512_set1_epi64(size_t as i64);
        let read_bit_vec = _mm256_set1_epi32(RaspPermission::Read as i32);
        let zero256 = _mm256_setzero_si256();

        let mut count_in_simd = 0u32;
        for c in 0..chunks {
            let off = c * 8;
            // SAFETY: c < chunks = n/8, so off+8 <= n; in-bounds.
            let v_ptrs = unsafe {
                _mm512_loadu_si512(self.ptrs.as_ptr().add(off) as *const __m512i)
            };
            let v_bases = unsafe {
                _mm512_loadu_si512(self.bases.as_ptr().add(off) as *const __m512i)
            };
            let lengths_ymm = unsafe {
                _mm256_loadu_si256(self.lengths.as_ptr().add(off) as *const __m256i)
            };
            let perms_ymm = unsafe {
                _mm256_loadu_si256(self.perms.as_ptr().add(off) as *const __m256i)
            };

            // Lower bound: base > ptr fails. Returns __mmask8 directly,
            // one bit per lane.
            let oob_lower_mask: u8 = _mm512_cmpgt_epi64_mask(v_bases, v_ptrs);

            // Upper bound: access_end > region_end fails.
            let v_lengths_u64 = _mm512_cvtepu32_epi64(lengths_ymm);
            let v_region_end = _mm512_add_epi64(v_bases, v_lengths_u64);
            let v_access_end = _mm512_add_epi64(v_ptrs, size_t_vec);
            let oob_upper_mask: u8 =
                _mm512_cmpgt_epi64_mask(v_access_end, v_region_end);

            // Sealed: high bit of each u32 in perms. movemask_ps reads
            // bit 31 of each 32-bit lane and emits an 8-bit mask for
            // the full 256-bit register.
            let sealed_mask8 =
                _mm256_movemask_ps(_mm256_castsi256_ps(perms_ymm)) as u32 & 0xFF;

            // No-read: AND with Read bit, cmpeq vs zero, movemask.
            let read_anded = _mm256_and_si256(perms_ymm, read_bit_vec);
            let read_eq_zero = _mm256_cmpeq_epi32(read_anded, zero256);
            let no_read_mask8 =
                _mm256_movemask_ps(_mm256_castsi256_ps(read_eq_zero)) as u32 & 0xFF;

            let any_fail = (oob_lower_mask as u32)
                | (oob_upper_mask as u32)
                | sealed_mask8
                | no_read_mask8;
            let valid_in_chunk = 8 - (any_fail & 0xFF).count_ones();
            count_in_simd += valid_in_chunk;
        }

        // Remainder (n % 8) via scalar.
        let mut count_scalar = 0u32;
        for i in (chunks * 8)..n {
            if self.check_read_scalar(RaspBatchIndex::new(i as u32)).is_ok() {
                count_scalar += 1;
            }
        }
        count_in_simd + count_scalar
    }

    /// Cross-platform safe count of valid entries. Dispatches to
    /// `count_valid_avx512` when AVX-512F is present, then
    /// `count_valid_avx2`, then `count_valid_scalar`.
    pub fn count_valid(&self) -> u32 {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                // SAFETY: feature-detected.
                return unsafe { self.count_valid_avx512() };
            }
        }
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        {
            if std::is_x86_feature_detected!("avx2") {
                // SAFETY: feature-detected.
                return unsafe { self.count_valid_avx2() };
            }
        }
        self.count_valid_scalar()
    }

    /// AVX2-accelerated full validation that writes per-index results.
    /// Slower than `count_valid_avx2` because it materialises a
    /// `Vec<Result>` instead of just counting; use this when the
    /// caller needs per-index error attribution.
    ///
    /// # Safety
    ///
    /// Caller must guarantee AVX2 is supported by the runtime CPU.
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[target_feature(enable = "avx2")]
    #[inline]
    pub unsafe fn check_read_all_avx2(&self) -> Vec<Result<(), RaspError>> {
        #[cfg(target_arch = "x86_64")]
        use std::arch::x86_64::*;
        #[cfg(target_arch = "x86")]
        use std::arch::x86::*;

        let n = self.len();
        let mut results: Vec<Result<(), RaspError>> = vec![Ok(()); n];
        if n == 0 {
            return results;
        }
        let chunks = n / 4;
        let size_t = std::mem::size_of::<T>() as u64;
        let size_t_vec = _mm256_set1_epi64x(size_t as i64);
        let read_bit_vec = _mm_set1_epi32(RaspPermission::Read as i32);
        let zero128 = _mm_setzero_si128();

        for c in 0..chunks {
            let off = c * 4;
            // SAFETY: c < chunks = n/4, so off+4 <= n.
            let v_ptrs = unsafe {
                _mm256_loadu_si256(self.ptrs.as_ptr().add(off) as *const __m256i)
            };
            let v_bases = unsafe {
                _mm256_loadu_si256(self.bases.as_ptr().add(off) as *const __m256i)
            };
            let lengths_xmm = unsafe {
                _mm_loadu_si128(self.lengths.as_ptr().add(off) as *const __m128i)
            };
            let perms_xmm = unsafe {
                _mm_loadu_si128(self.perms.as_ptr().add(off) as *const __m128i)
            };

            let cmp_lower = _mm256_cmpgt_epi64(v_bases, v_ptrs);
            let v_lengths_u64 = _mm256_cvtepu32_epi64(lengths_xmm);
            let v_region_end = _mm256_add_epi64(v_bases, v_lengths_u64);
            let v_access_end = _mm256_add_epi64(v_ptrs, size_t_vec);
            let cmp_upper = _mm256_cmpgt_epi64(v_access_end, v_region_end);

            let sealed_mask4 =
                _mm_movemask_ps(_mm_castsi128_ps(perms_xmm)) as u32;
            let read_anded = _mm_and_si128(perms_xmm, read_bit_vec);
            let read_eq_zero = _mm_cmpeq_epi32(read_anded, zero128);
            let no_read_mask4 =
                _mm_movemask_ps(_mm_castsi128_ps(read_eq_zero)) as u32;

            // VPCMPGTQ → bit 63 set in each matching 64-bit lane →
            // _mm256_movemask_pd emits a 4-bit lane mask directly.
            let oob_lower_4 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp_lower)) as u32;
            let oob_upper_4 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp_upper)) as u32;

            for i in 0..4 {
                let bit = 1u32 << i;
                if sealed_mask4 & bit != 0 {
                    results[off + i] = Err(RaspError::Sealed);
                } else if no_read_mask4 & bit != 0 {
                    results[off + i] = Err(RaspError::PermissionDenied);
                } else if (oob_lower_4 | oob_upper_4) & bit != 0 {
                    results[off + i] = Err(RaspError::OutOfBounds);
                }
            }
        }
        // SIMD-tail cleanup: indexed form reads more naturally here than
        // the iterator equivalent (the loop body would still need both
        // `i` and the slot).
        #[allow(clippy::needless_range_loop)]
        for i in (chunks * 4)..n {
            results[i] = self.check_read_scalar(RaspBatchIndex::new(i as u32));
        }
        results
    }

    /// AVX-512F full validation that writes per-index results.
    /// Processes 8 elements per iteration via ZMM loads and
    /// mask-producing `_mm512_cmpgt_epi64_mask`. Bit-exact equivalent
    /// of `check_read_all_avx2` and `check_read_all_scalar`.
    ///
    /// # Safety
    /// Caller must guarantee AVX-512F is supported by the runtime CPU.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    #[inline]
    pub unsafe fn check_read_all_avx512(&self) -> Vec<Result<(), RaspError>> {
        use std::arch::x86_64::*;

        let n = self.len();
        let mut results: Vec<Result<(), RaspError>> = vec![Ok(()); n];
        if n == 0 {
            return results;
        }
        let chunks = n / 8;
        let size_t = std::mem::size_of::<T>() as u64;
        let size_t_vec = _mm512_set1_epi64(size_t as i64);
        let read_bit_vec = _mm256_set1_epi32(RaspPermission::Read as i32);
        let zero256 = _mm256_setzero_si256();

        for c in 0..chunks {
            let off = c * 8;
            // SAFETY: c < chunks = n/8, so off+8 <= n.
            let v_ptrs = unsafe {
                _mm512_loadu_si512(self.ptrs.as_ptr().add(off) as *const __m512i)
            };
            let v_bases = unsafe {
                _mm512_loadu_si512(self.bases.as_ptr().add(off) as *const __m512i)
            };
            let lengths_ymm = unsafe {
                _mm256_loadu_si256(self.lengths.as_ptr().add(off) as *const __m256i)
            };
            let perms_ymm = unsafe {
                _mm256_loadu_si256(self.perms.as_ptr().add(off) as *const __m256i)
            };

            let oob_lower_mask: u8 = _mm512_cmpgt_epi64_mask(v_bases, v_ptrs);
            let v_lengths_u64 = _mm512_cvtepu32_epi64(lengths_ymm);
            let v_region_end = _mm512_add_epi64(v_bases, v_lengths_u64);
            let v_access_end = _mm512_add_epi64(v_ptrs, size_t_vec);
            let oob_upper_mask: u8 =
                _mm512_cmpgt_epi64_mask(v_access_end, v_region_end);

            let sealed_mask8 =
                _mm256_movemask_ps(_mm256_castsi256_ps(perms_ymm)) as u32 & 0xFF;
            let read_anded = _mm256_and_si256(perms_ymm, read_bit_vec);
            let read_eq_zero = _mm256_cmpeq_epi32(read_anded, zero256);
            let no_read_mask8 =
                _mm256_movemask_ps(_mm256_castsi256_ps(read_eq_zero)) as u32 & 0xFF;

            for i in 0..8 {
                let bit = 1u32 << i;
                if sealed_mask8 & bit != 0 {
                    results[off + i] = Err(RaspError::Sealed);
                } else if no_read_mask8 & bit != 0 {
                    results[off + i] = Err(RaspError::PermissionDenied);
                } else if ((oob_lower_mask as u32) | (oob_upper_mask as u32)) & bit
                    != 0
                {
                    results[off + i] = Err(RaspError::OutOfBounds);
                }
            }
        }
        // SIMD-tail cleanup via scalar.
        #[allow(clippy::needless_range_loop)]
        for i in (chunks * 8)..n {
            results[i] = self.check_read_scalar(RaspBatchIndex::new(i as u32));
        }
        results
    }

    /// Cross-platform safe full validation.
    pub fn check_read_all(&self) -> Vec<Result<(), RaspError>> {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f") {
                // SAFETY: feature-detected.
                return unsafe { self.check_read_all_avx512() };
            }
        }
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        {
            if std::is_x86_feature_detected!("avx2") {
                // SAFETY: feature-detected.
                return unsafe { self.check_read_all_avx2() };
            }
        }
        self.check_read_all_scalar()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch_returns_zero_count() {
        let b: RaspBatch<u8> = RaspBatch::new();
        assert!(b.is_empty());
        assert_eq!(b.count_valid(), 0);
        assert_eq!(b.check_read_all().len(), 0);
    }

    #[test]
    fn push_from_slice_and_read_single() {
        let storage: Vec<u64> = vec![42; 8];
        let mut b: RaspBatch<u64> = RaspBatch::new();
        let (idx, _anchor) = b.push_from_slice(
            &storage, RaspPermission::Read as u32,
        ).unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b.check_read_scalar(idx), Ok(()));
    }

    #[test]
    fn count_valid_scalar_matches_check_all() {
        let storages: Vec<Vec<u64>> = (0..8).map(|_| vec![0; 8]).collect();
        let mut b: RaspBatch<u64> = RaspBatch::new();
        for (i, s) in storages.iter().enumerate() {
            let perms = if i % 2 == 0 {
                RaspPermission::Read as u32
            } else {
                RaspPermission::None as u32
            };
            b.push_from_slice(s, perms).unwrap();
        }
        let count = b.count_valid_scalar();
        let all = b.check_read_all_scalar();
        let oks = all.iter().filter(|r| r.is_ok()).count() as u32;
        assert_eq!(count, oks);
        assert_eq!(count, 4);
    }

    #[test]
    fn count_valid_avx2_matches_scalar_oracle() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let storages: Vec<Vec<u64>> = (0..16).map(|_| vec![0; 8]).collect();
        let perms_r = RaspPermission::Read as u32;
        let sealed = perms_r | SEALED_BIT_U32;
        let perms_none = RaspPermission::None as u32;
        let perms_rw =
            (RaspPermission::Read as u32) | (RaspPermission::Write as u32);
        let choices = [perms_r, sealed, perms_none, perms_rw];

        let mut b: RaspBatch<u64> = RaspBatch::new();
        for (i, s) in storages.iter().enumerate() {
            b.push_from_slice(s, choices[i % 4]).unwrap();
        }
        let scalar = b.count_valid_scalar();
        // SAFETY: feature-detected above.
        let avx2 = unsafe { b.count_valid_avx2() };
        let dispatched = b.count_valid();
        assert_eq!(scalar, avx2, "AVX2 count mismatch");
        assert_eq!(scalar, dispatched, "Dispatched count mismatch");
        // Per the choices: lanes 0 and 3 are valid (Read; Read+Write),
        // lanes 1 and 2 are invalid (sealed; none).
        // 16 entries / 4 choices = 4 of each.
        assert_eq!(scalar, 8);
    }

    #[test]
    fn check_read_all_avx2_per_lane_results_match_scalar() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let storages: Vec<Vec<u64>> = (0..12).map(|_| vec![0; 8]).collect();
        let perms_r = RaspPermission::Read as u32;
        let sealed = perms_r | SEALED_BIT_U32;
        let perms_none = RaspPermission::None as u32;
        let choices = [perms_r, sealed, perms_none];

        let mut b: RaspBatch<u64> = RaspBatch::new();
        for (i, s) in storages.iter().enumerate() {
            b.push_from_slice(s, choices[i % 3]).unwrap();
        }
        let scalar = b.check_read_all_scalar();
        let avx2 = unsafe { b.check_read_all_avx2() };
        assert_eq!(scalar.len(), avx2.len());
        for (i, (s, v)) in scalar.iter().zip(avx2.iter()).enumerate() {
            assert_eq!(s, v, "Mismatch at lane {i}");
        }
    }

    #[test]
    fn remainder_lanes_handled_by_scalar_fallback() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        // 13 entries: 3 full chunks of 4, plus 1 remainder lane.
        // The AVX2 path must hand the remainder to scalar.
        let storages: Vec<Vec<u64>> = (0..13).map(|_| vec![0; 8]).collect();
        let mut b: RaspBatch<u64> = RaspBatch::new();
        for s in &storages {
            b.push_from_slice(s, RaspPermission::Read as u32).unwrap();
        }
        let scalar = b.count_valid_scalar();
        let avx2 = unsafe { b.count_valid_avx2() };
        assert_eq!(scalar, avx2);
        assert_eq!(scalar, 13);
    }

    #[test]
    fn push_raw_validates_bounds_at_construction() {
        let mut b: RaspBatch<u8> = RaspBatch::new();
        // ptr below base.
        let r = b.push_raw(0x0FFF, 0x1000, 16, RaspPermission::Read as u32);
        assert_eq!(r.err(), Some(RaspError::OutOfBounds));
        // access_end past region_end.
        let r = b.push_raw(0x1010, 0x1000, 8, RaspPermission::Read as u32);
        assert_eq!(r.err(), Some(RaspError::OutOfBounds));
        // OK case.
        let r = b.push_raw(0x1000, 0x1000, 16, RaspPermission::Read as u32);
        assert!(r.is_ok());
    }

    #[test]
    fn sealed_bit_blocks_read_in_simd_path() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let storages: Vec<Vec<u64>> = (0..4).map(|_| vec![0; 8]).collect();
        let mut b: RaspBatch<u64> = RaspBatch::new();
        let perms_r = RaspPermission::Read as u32;
        let sealed = perms_r | SEALED_BIT_U32;
        for (i, s) in storages.iter().enumerate() {
            let p = if i == 2 { sealed } else { perms_r };
            b.push_from_slice(s, p).unwrap();
        }
        // SAFETY: feature-detected.
        let results = unsafe { b.check_read_all_avx2() };
        assert_eq!(results[0], Ok(()));
        assert_eq!(results[1], Ok(()));
        assert_eq!(results[2], Err(RaspError::Sealed));
        assert_eq!(results[3], Ok(()));
    }

    #[test]
    fn no_read_permission_caught_in_simd_path() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        let storages: Vec<Vec<u64>> = (0..4).map(|_| vec![0; 8]).collect();
        let mut b: RaspBatch<u64> = RaspBatch::new();
        let perms_r = RaspPermission::Read as u32;
        let perms_w_only = RaspPermission::Write as u32;
        for (i, s) in storages.iter().enumerate() {
            let p = if i == 1 { perms_w_only } else { perms_r };
            b.push_from_slice(s, p).unwrap();
        }
        let results = unsafe { b.check_read_all_avx2() };
        assert_eq!(results[0], Ok(()));
        assert_eq!(results[1], Err(RaspError::PermissionDenied));
        assert_eq!(results[2], Ok(()));
        assert_eq!(results[3], Ok(()));
    }

    #[test]
    fn large_batch_simd_vs_scalar_parity() {
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }
        // 1024 entries, varied perms.
        let storages: Vec<Vec<u64>> = (0..1024).map(|_| vec![0; 8]).collect();
        let mut b: RaspBatch<u64> = RaspBatch::new();
        let perms_r = RaspPermission::Read as u32;
        let sealed = perms_r | SEALED_BIT_U32;
        let perms_none = RaspPermission::None as u32;
        let choices = [perms_r, sealed, perms_none, perms_r];
        for (i, s) in storages.iter().enumerate() {
            b.push_from_slice(s, choices[i % 4]).unwrap();
        }
        let scalar = b.count_valid_scalar();
        let avx2 = unsafe { b.count_valid_avx2() };
        assert_eq!(scalar, avx2);
        // 2 of 4 choices are valid (perms_r at indices 0 and 3),
        // so 512 entries valid out of 1024.
        assert_eq!(scalar, 512);
    }
}
