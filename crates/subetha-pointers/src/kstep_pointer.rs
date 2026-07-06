//! `KStepPointer<T>` - pointer with first-class stride encoding.
//!
//! Storage: `(base: *const T, k_step: u8)`. The stride between
//! consecutive elements is `sizeof(T) << k_step`. So:
//!
//! | K_step | Stride                     | Use case                       |
//! |--------|----------------------------|--------------------------------|
//! | 0      | sizeof(T) (tight pack)     | Default, contiguous Vec/array  |
//! | 1      | 2 * sizeof(T)              | Every other element            |
//! | 2      | 4 * sizeof(T)              | Sub-quarter access pattern     |
//! | 3      | 8 * sizeof(T)              | SIMD lane stride               |
//! | 6      | 64 * sizeof(T)             | Cache-line stride              |
//! | 12     | 4096 * sizeof(T)           | Page-aligned stride            |
//!
//! K_step is the pointer-side analog of quartz's `K_inner` axis -
//! it controls the granularity of iteration. The advantage over a
//! runtime `stride: usize` is that K_step is a const-encoded shift
//! amount; the compiler can fold `<< k_step` into address generation
//! and SIMD ops know the stride at codegen time.
//!
//! # Architectural rationale
//!
//! BLAS GEMM iterates over matrix rows AND columns with potentially
//! different strides. NumPy's strided arrays do the same in higher
//! dimensions. Today these are all encoded as runtime `stride: usize`
//! fields - the compiler has to emit IMUL for each step. With KStep
//! the stride is `1 << k_step` so the codegen is SHL (one cycle),
//! and the compiler can hoist the shift amount as an immediate.

use std::marker::PhantomData;

/// Strided pointer to `T`. Stride = `sizeof(T) << k_step` bytes.
#[derive(Debug)]
pub struct KStepPointer<T> {
    base: *const T,
    k_step: u8,
    _phantom: PhantomData<*const T>,
}

unsafe impl<T: Send> Send for KStepPointer<T> {}
unsafe impl<T: Sync> Sync for KStepPointer<T> {}

impl<T> KStepPointer<T> {
    /// Direction signature of `KStepPointer<T>`. Engages the
    /// `K_stride` axis (stride encoded as log2 shift count, no
    /// runtime `stride: usize` field).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Stride],
    );

    /// New strided pointer. `k_step` is the log2 of the stride
    /// multiplier; the actual byte stride is `sizeof(T) << k_step`.
    ///
    /// # Safety
    ///
    /// `base` must point to a valid `T` and the strided sequence
    /// `base + i * stride` must remain valid for all indices the
    /// caller will use.
    pub const unsafe fn new(base: *const T, k_step: u8) -> Self {
        Self { base, k_step, _phantom: PhantomData }
    }

    /// Tight-packed strided pointer (K_step = 0).
    ///
    /// # Safety
    ///
    /// Same as [`Self::new`]: `base` must point to a contiguous run of `T`
    /// for every index the caller will use.
    pub const unsafe fn tight(base: *const T) -> Self {
        unsafe { Self::new(base, 0) }
    }

    /// Cache-line strided pointer (K_step chosen so stride >= 64
    /// bytes). Walks every 64/sizeof(T) elements.
    ///
    /// # Safety
    ///
    /// Same as [`Self::new`]: `base` must remain valid for every index
    /// the caller will use under the chosen `k_step` stride.
    pub const unsafe fn cache_line(base: *const T) -> Self {
        // ceil(log2(64 / sizeof(T))) capped at 6.
        let t_size = std::mem::size_of::<T>();
        let k = if t_size >= 64 { 0 }
            else if t_size >= 32 { 1 }
            else if t_size >= 16 { 2 }
            else if t_size >= 8 { 3 }
            else if t_size >= 4 { 4 }
            else if t_size >= 2 { 5 }
            else { 6 };
        unsafe { Self::new(base, k) }
    }

    #[inline]
    pub const fn base(&self) -> *const T { self.base }

    #[inline]
    pub const fn k_step(&self) -> u8 { self.k_step }

    /// Byte stride between consecutive elements.
    #[inline]
    pub const fn stride(&self) -> usize {
        std::mem::size_of::<T>() << self.k_step
    }

    /// Pointer to the i-th element under the current stride.
    ///
    /// # Safety
    ///
    /// The caller must ensure `base + i * stride` is in bounds.
    #[inline]
    pub unsafe fn at(&self, i: usize) -> *const T {
        let offset_bytes = i * self.stride();
        unsafe { (self.base as *const u8).add(offset_bytes) as *const T }
    }

    /// Borrow the i-th element.
    ///
    /// # Safety
    ///
    /// `i` must be in bounds; the strided sequence at this index
    /// must point to a valid `T`.
    #[inline]
    pub unsafe fn get(&self, i: usize) -> &T {
        unsafe { &*self.at(i) }
    }

    /// Iterator over `count` strided elements.
    ///
    /// # Safety
    ///
    /// All `count` strided positions must be in bounds.
    pub unsafe fn iter(&self, count: usize) -> StridedIter<'_, T> {
        StridedIter { ptr: *self, i: 0, count, _life: PhantomData }
    }
}

impl<T> Clone for KStepPointer<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for KStepPointer<T> {}

/// Iterator yielding strided elements.
pub struct StridedIter<'a, T> {
    ptr: KStepPointer<T>,
    i: usize,
    count: usize,
    _life: PhantomData<&'a T>,
}

impl<'a, T> Iterator for StridedIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> {
        if self.i >= self.count { return None; }
        let r = unsafe { &*self.ptr.at(self.i) };
        self.i += 1;
        Some(r)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.count - self.i;
        (n, Some(n))
    }
}

impl<'a, T> ExactSizeIterator for StridedIter<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tight_stride_walks_contiguous_array() {
        let data: Vec<u64> = (0..10u64).collect();
        let p = unsafe { KStepPointer::tight(data.as_ptr()) };
        assert_eq!(p.k_step(), 0);
        assert_eq!(p.stride(), 8);
        for i in 0..10 {
            assert_eq!(unsafe { *p.get(i) }, i as u64);
        }
    }

    #[test]
    fn k_step_1_skips_every_other() {
        let data: Vec<u64> = (0..10u64).collect();
        let p = unsafe { KStepPointer::new(data.as_ptr(), 1) };
        assert_eq!(p.stride(), 16);
        // i=0 -> data[0]=0; i=1 -> data[2]=2; i=2 -> data[4]=4; ...
        for i in 0..5 {
            assert_eq!(unsafe { *p.get(i) }, (i * 2) as u64);
        }
    }

    #[test]
    fn k_step_2_skips_by_four() {
        let data: Vec<u64> = (0..16u64).collect();
        let p = unsafe { KStepPointer::new(data.as_ptr(), 2) };
        assert_eq!(p.stride(), 32);
        for i in 0..4 {
            assert_eq!(unsafe { *p.get(i) }, (i * 4) as u64);
        }
    }

    #[test]
    fn cache_line_stride_picks_correct_k() {
        // For u64 (8 bytes) cache_line() should pick k=3 -> stride 64.
        let data: Vec<u64> = (0..64u64).collect();
        let p = unsafe { KStepPointer::cache_line(data.as_ptr()) };
        assert_eq!(p.k_step(), 3, "u64 cache_line should be k=3");
        assert_eq!(p.stride(), 64);
        // i=0 -> data[0]; i=1 -> data[8]; i=2 -> data[16]; ...
        for i in 0..8 {
            assert_eq!(unsafe { *p.get(i) }, (i * 8) as u64);
        }
    }

    #[test]
    fn cache_line_stride_for_u8() {
        // For u8 (1 byte) cache_line should pick k=6 -> stride 64.
        let data: Vec<u8> = (0u8..64).collect();
        let p = unsafe { KStepPointer::<u8>::cache_line(data.as_ptr()) };
        assert_eq!(p.k_step(), 6);
        assert_eq!(p.stride(), 64);
    }

    #[test]
    fn strided_iter_yields_correct_elements() {
        let data: Vec<u64> = (0..20u64).collect();
        let p = unsafe { KStepPointer::new(data.as_ptr(), 2) };
        let collected: Vec<u64> = unsafe { p.iter(5) }.copied().collect();
        assert_eq!(collected, vec![0, 4, 8, 12, 16]);
    }

    #[test]
    fn matrix_row_stride_workflow() {
        // 4x4 matrix in row-major, walking col 0 with k_step=2 (stride 4*8=32).
        let matrix: Vec<u64> = (0..16u64).collect();
        let col0 = unsafe { KStepPointer::new(matrix.as_ptr(), 2) };
        let col0_vals: Vec<u64> = unsafe { col0.iter(4) }.copied().collect();
        assert_eq!(col0_vals, vec![0, 4, 8, 12]);
    }

    #[test]
    fn layout_is_16_bytes() {
        // *const T = 8, u8 + PhantomData -> pad to 16 with align(8).
        assert_eq!(std::mem::size_of::<KStepPointer<u64>>(), 16);
    }
}
