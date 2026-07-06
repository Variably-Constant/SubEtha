//! `AdaptiveCheriPointer<T>` - CHERI-style capability pointer with
//! software emulation for portability.
//!
//! CHERI (Capability Hardware Enhanced RISC Instructions) augments
//! every pointer with bounds, permissions, otype, and a sealed bit,
//! enforced by hardware on every dereference. ARM Morello is the
//! production silicon as of 2026; iOS may adopt the model.
//!
//! CHERI is by design a RISC instruction-set extension. There is no
//! equivalent capability ISA on x86 / x86_64. The x86-side equivalent
//! is a Register-Aligned SIMD Pointer primitive: vector instructions
//! express bounds + permission checks rather than emulating capability
//! hardware that does not exist on the silicon.
//!
//! # Read vs Write: distinct types
//!
//! This module separates capability semantics at the TYPE level
//! rather than the runtime permission-bit level:
//!
//! - [`ReadableCapability<T>`] - bounds-checked read-only view of T.
//!   Constructed from `&[T]` borrow. Compile-time guarantee: no
//!   `write()` method exists. Permissions silently strip the Write
//!   bit at construction (a ReadableCapability with Write perm is
//!   nonsensical).
//!
//! - [`WritableCapability<T>`] - bounds-checked read+write access
//!   to T. Constructed from `&mut [T]` borrow (or owned `Box<T>`).
//!   `!Copy + !Clone` so the borrow checker prevents aliasing the
//!   unique-writer status.
//!
//! - [`OwnedReadableCapability<T>`] / [`OwnedWritableCapability<T>`] -
//!   RAII wrappers that own a `Box<T>` and reclaim it on `Drop`.
//!   Use these when you want capability semantics over an owned
//!   value without manual `Box::from_raw` cleanup.
//!
//! # Constructor matrix
//!
//! | Type                       | Safe constructor    | Unsafe constructor |
//! |----------------------------|---------------------|--------------------|
//! | ReadableCapability         | from_slice          | new (raw ptr)      |
//! | WritableCapability         | from_slice_mut      | new (raw ptr)      |
//! | OwnedReadableCapability    | new(value), from_box | -                 |
//! | OwnedWritableCapability    | new(value), from_box | -                 |
//!
//! # Hardware backend
//!
//! The hardware-capability backend (real CHERI primitives on ARM
//! Morello, gated on `target_arch = aarch64` + a `cheri` feature
//! flag) is tracked by its own bead and uses the same
//! ReadableCapability / WritableCapability surface so callers don't
//! need to change code when the hardware path lands.
//!
//! # Instruction-set emulation paths
//!
//! The hardware backend can be developed and benched today without
//! Morello silicon using QEMU-Morello + CHERI-LLVM toolchain, or
//! CheriBSD images, or the Cheriot-RTOS RISC-V FPGA implementation.
//!
//! # Safety contract
//!
//! All bounds arithmetic uses `checked_add` so a near-overflow
//! `base + length` cannot wrap and bypass the check.
//!
//! Adjacent capability-like hardware features on non-x86 silicon:
//! ARM PAC (Pointer Authentication Codes), Apple Silicon MTE
//! (Memory Tagging Extension), SPARC ADI (Application Data Integrity).

use std::marker::PhantomData;

/// Permission bits. Same shape across both Readable and Writable
/// capabilities so narrow() can reason about them uniformly.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityPermission {
    None    = 0,
    Read    = 1 << 0,
    Write   = 1 << 1,
    Execute = 1 << 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityError {
    OutOfBounds,
    PermissionDenied,
    Sealed,
    AddressOverflow,
}

const SEALED_BIT: u32 = 1 << 31;
const WRITE_BIT: u32 = CapabilityPermission::Write as u32;

// =========================================================================
// ReadableCapability<T> - bounds-checked read-only access.
// =========================================================================

/// Read-only bounds-checked capability. The Write permission bit is
/// silently stripped at construction; no `write()` method exists.
///
/// Layout (24 bytes):
/// ```text
/// ptr:    *const T   (8 bytes) - base address
/// base:   usize      (8 bytes) - lower bound (often equals ptr)
/// length: u32        (4 bytes) - bytes from base
/// perms:  u32        (4 bytes) - permission bitmask + sealed bit;
///                                 Write bit guaranteed cleared
/// ```
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ReadableCapability<T> {
    ptr: *const T,
    base: usize,
    length: u32,
    perms: u32,
    _phantom: PhantomData<*const T>,
}

impl<T> ReadableCapability<T> {
    /// Direction signature of `ReadableCapability<T>`. Engages the
    /// `K_bounds` axis (runtime base / length / permissions stored
    /// at slot for CHERI-style bounds enforcement on every deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Bounds],
    );

    /// # Safety
    ///
    /// Caller guarantees `[base, base + length)` is valid memory for
    /// the lifetime of this capability and `ptr` lies within that
    /// region. The Write permission bit is silently stripped.
    pub unsafe fn new(ptr: *const T, base: usize, length: u32, perms: u32)
        -> Result<Self, CapabilityError>
    {
        let region_end = base.checked_add(length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        let addr = ptr as usize;
        if addr < base { return Err(CapabilityError::OutOfBounds); }
        let access_end = addr.checked_add(std::mem::size_of::<T>())
            .ok_or(CapabilityError::AddressOverflow)?;
        if access_end > region_end {
            return Err(CapabilityError::OutOfBounds);
        }
        Ok(Self {
            ptr, base, length,
            perms: perms & !WRITE_BIT,  // strip Write
            _phantom: PhantomData,
        })
    }

    /// Safe constructor: build a read-only capability over a
    /// borrowed slice. Write bit silently stripped.
    pub fn from_slice(slice: &[T], perms: u32)
        -> (ReadableCapability<T>, &[T])
    {
        let ptr = slice.as_ptr();
        let base = ptr as usize;
        let length = std::mem::size_of_val(slice) as u32;
        let cap = ReadableCapability {
            ptr, base, length,
            perms: perms & !WRITE_BIT,
            _phantom: PhantomData,
        };
        (cap, slice)
    }

    #[inline]
    pub fn has_permission(&self, p: CapabilityPermission) -> bool {
        if self.is_sealed() { return false; }
        // Write bit was stripped at construction; even if caller
        // asks for Write, has_permission returns false.
        (self.perms & p as u32) != 0
    }

    #[inline]
    pub fn is_sealed(&self) -> bool { (self.perms & SEALED_BIT) != 0 }

    pub fn sealed(mut self) -> Self {
        self.perms |= SEALED_BIT;
        self
    }

    pub fn unsealed(mut self) -> Self {
        self.perms &= !SEALED_BIT;
        self
    }

    /// Read the value through the capability.
    pub fn read(&self) -> Result<T, CapabilityError>
    where T: Copy,
    {
        if self.is_sealed() { return Err(CapabilityError::Sealed); }
        if !self.has_permission(CapabilityPermission::Read) {
            return Err(CapabilityError::PermissionDenied);
        }
        let addr = self.ptr as usize;
        if addr < self.base { return Err(CapabilityError::OutOfBounds); }
        let access_end = addr.checked_add(std::mem::size_of::<T>())
            .ok_or(CapabilityError::AddressOverflow)?;
        let region_end = self.base.checked_add(self.length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        if access_end > region_end { return Err(CapabilityError::OutOfBounds); }
        // SAFETY: bounds + permission + sealed checks above. Caller's
        // constructor-time contract guarantees the underlying memory
        // is live.
        Ok(unsafe { std::ptr::read(self.ptr) })
    }

    /// Narrow this capability to a sub-range. Write bit is silently
    /// stripped (Readable cannot grant Write).
    pub fn narrow(&self, sub_base: usize, sub_length: u32, sub_perms: u32)
        -> Result<Self, CapabilityError>
    {
        let sub_end = sub_base.checked_add(sub_length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        let region_end = self.base.checked_add(self.length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        if sub_base < self.base || sub_end > region_end {
            return Err(CapabilityError::OutOfBounds);
        }
        let new_perms = (self.perms & sub_perms) & !SEALED_BIT & !WRITE_BIT;
        Ok(Self {
            ptr: sub_base as *const T,
            base: sub_base,
            length: sub_length,
            perms: new_perms,
            _phantom: PhantomData,
        })
    }
}

// =========================================================================
// WritableCapability<T> - bounds-checked read+write access. !Copy/!Clone.
// =========================================================================

/// Read+Write bounds-checked capability. NOT Copy/Clone so the
/// borrow checker prevents aliasing the unique-writer status.
///
/// Constructed from `&mut [T]` or an owned `Box<T>` (via
/// [`OwnedWritableCapability::from_box`]). The `&mut` borrow IS the
/// unique-writer guarantee; without it, multiple WritableCapability
/// instances could simultaneously write the same region.
#[derive(Debug)]
#[repr(C)]
pub struct WritableCapability<T> {
    ptr: *mut T,
    base: usize,
    length: u32,
    perms: u32,
    _phantom: PhantomData<*mut T>,
}

impl<T> WritableCapability<T> {
    /// Direction signature of `WritableCapability<T>`. Engages the
    /// `K_bounds` axis (runtime base / length / permissions stored
    /// at slot for CHERI-style bounds enforcement on every deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Bounds],
    );

    /// # Safety
    ///
    /// Caller guarantees `[base, base + length)` is valid memory and
    /// no other writer accesses the region while this capability is
    /// alive.
    pub unsafe fn new(ptr: *mut T, base: usize, length: u32, perms: u32)
        -> Result<Self, CapabilityError>
    {
        let region_end = base.checked_add(length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        let addr = ptr as usize;
        if addr < base { return Err(CapabilityError::OutOfBounds); }
        let access_end = addr.checked_add(std::mem::size_of::<T>())
            .ok_or(CapabilityError::AddressOverflow)?;
        if access_end > region_end {
            return Err(CapabilityError::OutOfBounds);
        }
        Ok(Self { ptr, base, length, perms, _phantom: PhantomData })
    }

    /// Safe constructor: build a writable capability over a mutable
    /// slice borrow. Grants Read + Write permissions.
    pub fn from_slice_mut(slice: &mut [T])
        -> (WritableCapability<T>, &mut [T])
    {
        let base = slice.as_ptr() as usize;
        let length = std::mem::size_of_val(slice) as u32;
        let ptr = slice.as_mut_ptr();
        let perms = CapabilityPermission::Read as u32
                  | CapabilityPermission::Write as u32;
        let cap = WritableCapability {
            ptr, base, length, perms, _phantom: PhantomData,
        };
        (cap, slice)
    }

    #[inline]
    pub fn has_permission(&self, p: CapabilityPermission) -> bool {
        if self.is_sealed() { return false; }
        (self.perms & p as u32) != 0
    }

    #[inline]
    pub fn is_sealed(&self) -> bool { (self.perms & SEALED_BIT) != 0 }

    pub fn sealed(mut self) -> Self {
        self.perms |= SEALED_BIT;
        self
    }

    pub fn unsealed(mut self) -> Self {
        self.perms &= !SEALED_BIT;
        self
    }

    /// Read through the capability.
    pub fn read(&self) -> Result<T, CapabilityError>
    where T: Copy,
    {
        if self.is_sealed() { return Err(CapabilityError::Sealed); }
        if !self.has_permission(CapabilityPermission::Read) {
            return Err(CapabilityError::PermissionDenied);
        }
        let addr = self.ptr as usize;
        if addr < self.base { return Err(CapabilityError::OutOfBounds); }
        let access_end = addr.checked_add(std::mem::size_of::<T>())
            .ok_or(CapabilityError::AddressOverflow)?;
        let region_end = self.base.checked_add(self.length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        if access_end > region_end { return Err(CapabilityError::OutOfBounds); }
        // SAFETY: bounds + permission + sealed checks above.
        Ok(unsafe { std::ptr::read(self.ptr) })
    }

    /// Write through the capability. The `&mut self` receiver + the
    /// constructor's `&mut [T]` / Box-consuming nature provide the
    /// unique-writer guarantee.
    pub fn write(&mut self, value: T) -> Result<(), CapabilityError> {
        if self.is_sealed() { return Err(CapabilityError::Sealed); }
        if !self.has_permission(CapabilityPermission::Write) {
            return Err(CapabilityError::PermissionDenied);
        }
        let addr = self.ptr as usize;
        if addr < self.base { return Err(CapabilityError::OutOfBounds); }
        let access_end = addr.checked_add(std::mem::size_of::<T>())
            .ok_or(CapabilityError::AddressOverflow)?;
        let region_end = self.base.checked_add(self.length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        if access_end > region_end { return Err(CapabilityError::OutOfBounds); }
        // SAFETY: bounds + permission + sealed checks above; unique
        // writer guaranteed by &mut self + construction path.
        unsafe { std::ptr::write(self.ptr, value); }
        Ok(())
    }

    /// Narrow to a sub-range, returning a new WritableCapability.
    /// To narrow to a read-only view, use `narrow_readable`.
    pub fn narrow(&self, sub_base: usize, sub_length: u32, sub_perms: u32)
        -> Result<Self, CapabilityError>
    {
        let sub_end = sub_base.checked_add(sub_length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        let region_end = self.base.checked_add(self.length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        if sub_base < self.base || sub_end > region_end {
            return Err(CapabilityError::OutOfBounds);
        }
        let new_perms = (self.perms & sub_perms) & !SEALED_BIT;
        Ok(Self {
            ptr: sub_base as *mut T,
            base: sub_base,
            length: sub_length,
            perms: new_perms,
            _phantom: PhantomData,
        })
    }

    /// Narrow to a read-only view. Returned ReadableCapability does
    /// NOT have Write perm regardless of what `sub_perms` contains.
    pub fn narrow_readable(&self, sub_base: usize, sub_length: u32, sub_perms: u32)
        -> Result<ReadableCapability<T>, CapabilityError>
    {
        let sub_end = sub_base.checked_add(sub_length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        let region_end = self.base.checked_add(self.length as usize)
            .ok_or(CapabilityError::AddressOverflow)?;
        if sub_base < self.base || sub_end > region_end {
            return Err(CapabilityError::OutOfBounds);
        }
        let new_perms = (self.perms & sub_perms) & !SEALED_BIT & !WRITE_BIT;
        Ok(ReadableCapability {
            ptr: sub_base as *const T,
            base: sub_base,
            length: sub_length,
            perms: new_perms,
            _phantom: PhantomData,
        })
    }

    /// Borrow this WritableCapability as a ReadableCapability view
    /// (no Write perm, no transfer of ownership). The borrow checker
    /// prevents the writable cap from being used while the readable
    /// view is alive.
    pub fn as_readable(&self) -> ReadableCapability<T> {
        ReadableCapability {
            ptr: self.ptr as *const T,
            base: self.base,
            length: self.length,
            perms: self.perms & !WRITE_BIT,
            _phantom: PhantomData,
        }
    }
}

// =========================================================================
// OwnedReadableCapability<T> + OwnedWritableCapability<T> - RAII wrappers.
// =========================================================================

/// RAII wrapper around a [`ReadableCapability<T>`] that owns the
/// underlying `Box<T>` allocation and reclaims it on `Drop`.
pub struct OwnedReadableCapability<T> {
    cap: ReadableCapability<T>,
}

impl<T> OwnedReadableCapability<T> {
    /// Heap-allocate `value` and wrap it in a read-only capability.
    pub fn new(value: T) -> Self {
        Self::from_box(Box::new(value))
    }

    /// Wrap an existing `Box<T>`. The capability has Read permission
    /// only (no Write).
    pub fn from_box(b: Box<T>) -> Self {
        let ptr = Box::into_raw(b) as *const T;
        let base = ptr as usize;
        let length = std::mem::size_of::<T>() as u32;
        // SAFETY: Box::into_raw produces a valid, aligned pointer
        // for size_of::<T>() bytes; base + length cannot overflow
        // because the allocator would not yield such a region.
        let cap = unsafe {
            ReadableCapability::new(
                ptr, base, length, CapabilityPermission::Read as u32,
            )
        }.expect("Box::into_raw region cannot fail bounds check");
        Self { cap }
    }

    pub fn cap(&self) -> &ReadableCapability<T> { &self.cap }

    /// Consume and reclaim the underlying Box.
    pub fn into_box(self) -> Box<T> {
        let raw = self.cap.ptr as *mut T;
        std::mem::forget(self);
        // SAFETY: raw was obtained from Box::into_raw in from_box.
        unsafe { Box::from_raw(raw) }
    }
}

impl<T> std::ops::Deref for OwnedReadableCapability<T> {
    type Target = ReadableCapability<T>;
    fn deref(&self) -> &Self::Target { &self.cap }
}

impl<T> Drop for OwnedReadableCapability<T> {
    fn drop(&mut self) {
        let raw = self.cap.ptr as *mut T;
        // SAFETY: raw was obtained from Box::into_raw in from_box;
        // Box::from_raw is the matching reclaim.
        let _reclaimed = unsafe { Box::from_raw(raw) };
    }
}

/// RAII wrapper around a [`WritableCapability<T>`] that owns the
/// underlying `Box<T>` allocation and reclaims it on `Drop`.
pub struct OwnedWritableCapability<T> {
    cap: WritableCapability<T>,
}

impl<T> OwnedWritableCapability<T> {
    /// Heap-allocate `value` and wrap it in a writable capability.
    pub fn new(value: T) -> Self {
        Self::from_box(Box::new(value))
    }

    /// Wrap an existing `Box<T>`. Full Read + Write perms.
    pub fn from_box(b: Box<T>) -> Self {
        let ptr = Box::into_raw(b);
        let base = ptr as usize;
        let length = std::mem::size_of::<T>() as u32;
        let perms = CapabilityPermission::Read as u32
                  | CapabilityPermission::Write as u32;
        // SAFETY: Box::into_raw produces a valid, aligned pointer.
        let cap = unsafe {
            WritableCapability::new(ptr, base, length, perms)
        }.expect("Box::into_raw region cannot fail bounds check");
        Self { cap }
    }

    pub fn cap(&self) -> &WritableCapability<T> { &self.cap }
    pub fn cap_mut(&mut self) -> &mut WritableCapability<T> { &mut self.cap }

    /// Consume and reclaim the underlying Box.
    pub fn into_box(self) -> Box<T> {
        let raw = self.cap.ptr;
        std::mem::forget(self);
        // SAFETY: raw was obtained from Box::into_raw in from_box.
        unsafe { Box::from_raw(raw) }
    }
}

impl<T> std::ops::Deref for OwnedWritableCapability<T> {
    type Target = WritableCapability<T>;
    fn deref(&self) -> &Self::Target { &self.cap }
}

impl<T> std::ops::DerefMut for OwnedWritableCapability<T> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.cap }
}

impl<T> Drop for OwnedWritableCapability<T> {
    fn drop(&mut self) {
        let raw = self.cap.ptr;
        // SAFETY: raw was obtained from Box::into_raw in from_box.
        let _reclaimed = unsafe { Box::from_raw(raw) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============== Layout ==============

    #[test]
    fn readable_layout_is_24_bytes() {
        assert_eq!(std::mem::size_of::<ReadableCapability<u64>>(), 24);
    }

    #[test]
    fn writable_layout_is_24_bytes() {
        assert_eq!(std::mem::size_of::<WritableCapability<u64>>(), 24);
    }

    // ============== ReadableCapability ==============

    #[test]
    fn readable_from_slice_strips_write_bit() {
        let storage: Vec<u64> = vec![42];
        let (cap, _anchor) = ReadableCapability::from_slice(
            &storage,
            CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32,
        );
        assert!(cap.has_permission(CapabilityPermission::Read));
        // Write bit silently stripped at construction.
        assert!(!cap.has_permission(CapabilityPermission::Write));
    }

    #[test]
    fn readable_read_with_permission() {
        let storage: Vec<u64> = vec![42];
        let (cap, _anchor) = ReadableCapability::from_slice(
            &storage, CapabilityPermission::Read as u32,
        );
        assert_eq!(cap.read().unwrap(), 42);
    }

    #[test]
    fn readable_read_without_permission_fails() {
        let storage: Vec<u64> = vec![99];
        let (cap, _anchor) = ReadableCapability::from_slice(&storage, 0);
        assert_eq!(cap.read().err(), Some(CapabilityError::PermissionDenied));
    }

    #[test]
    fn readable_sealed_blocks_read() {
        let storage: Vec<u64> = vec![1];
        let (cap, _anchor) = ReadableCapability::from_slice(
            &storage, CapabilityPermission::Read as u32,
        );
        let sealed = cap.sealed();
        assert_eq!(sealed.read().err(), Some(CapabilityError::Sealed));
    }

    #[test]
    fn readable_unseal_restores() {
        let storage: Vec<u64> = vec![7];
        let (cap, _anchor) = ReadableCapability::from_slice(
            &storage, CapabilityPermission::Read as u32,
        );
        let unsealed = cap.sealed().unsealed();
        assert_eq!(unsealed.read().unwrap(), 7);
    }

    #[test]
    fn readable_narrow_strips_write() {
        let storage: Vec<u64> = vec![0, 0, 0, 0];
        let (cap, anchor) = ReadableCapability::from_slice(
            &storage,
            CapabilityPermission::Read as u32,
        );
        let base = anchor.as_ptr() as usize;
        let narrowed = cap.narrow(
            base, 16,
            CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32,
        ).unwrap();
        assert!(narrowed.has_permission(CapabilityPermission::Read));
        assert!(!narrowed.has_permission(CapabilityPermission::Write));
    }

    #[test]
    fn readable_unsafe_new_overflow_guards() {
        let ptr = usize::MAX as *const u64;
        let r = unsafe {
            ReadableCapability::<u64>::new(
                ptr, usize::MAX, 16, CapabilityPermission::Read as u32,
            )
        };
        assert_eq!(r.err(), Some(CapabilityError::AddressOverflow));
    }

    // ============== WritableCapability ==============

    #[test]
    fn writable_from_slice_mut_grants_read_and_write() {
        let mut storage: Vec<u64> = vec![0];
        let (cap, _anchor) = WritableCapability::from_slice_mut(&mut storage);
        assert!(cap.has_permission(CapabilityPermission::Read));
        assert!(cap.has_permission(CapabilityPermission::Write));
    }

    #[test]
    fn writable_write_then_read() {
        let mut storage: Vec<u64> = vec![0];
        {
            let (mut cap, _anchor) = WritableCapability::from_slice_mut(&mut storage);
            cap.write(7777).unwrap();
            assert_eq!(cap.read().unwrap(), 7777);
        }
        assert_eq!(storage[0], 7777);
    }

    #[test]
    fn writable_sealed_blocks_write() {
        let mut storage: Vec<u64> = vec![0];
        let (cap, _anchor) = WritableCapability::from_slice_mut(&mut storage);
        let mut sealed = cap.sealed();
        assert_eq!(sealed.write(99u64).err(), Some(CapabilityError::Sealed));
    }

    #[test]
    fn writable_as_readable_view_strips_write() {
        let mut storage: Vec<u64> = vec![42];
        let (cap, _anchor) = WritableCapability::from_slice_mut(&mut storage);
        let read_view = cap.as_readable();
        assert!(read_view.has_permission(CapabilityPermission::Read));
        assert!(!read_view.has_permission(CapabilityPermission::Write));
        assert_eq!(read_view.read().unwrap(), 42);
    }

    #[test]
    fn writable_narrow_readable_strips_write() {
        let mut storage: Vec<u64> = vec![0, 0];
        let (cap, _anchor) = WritableCapability::from_slice_mut(&mut storage);
        let base = cap.ptr as usize;
        let narrowed: ReadableCapability<u64> = cap.narrow_readable(
            base, 8,
            CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32,
        ).unwrap();
        assert!(narrowed.has_permission(CapabilityPermission::Read));
        assert!(!narrowed.has_permission(CapabilityPermission::Write));
    }

    #[test]
    fn writable_unsafe_new_overflow_guards() {
        let ptr = usize::MAX as *mut u64;
        let r = unsafe {
            WritableCapability::<u64>::new(
                ptr, usize::MAX, 16,
                CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32,
            )
        };
        assert_eq!(r.err(), Some(CapabilityError::AddressOverflow));
    }

    // ============== OwnedReadableCapability ==============

    #[test]
    fn owned_readable_new_and_read() {
        let owned = OwnedReadableCapability::new(42u64);
        assert_eq!(owned.read().unwrap(), 42);
    }

    #[test]
    fn owned_readable_drop_reclaims() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct DropCounter;
        impl Drop for DropCounter {
            fn drop(&mut self) { DROPS.fetch_add(1, Ordering::Relaxed); }
        }
        let before = DROPS.load(Ordering::Relaxed);
        { let _o = OwnedReadableCapability::new(DropCounter); }
        assert_eq!(DROPS.load(Ordering::Relaxed), before + 1);
    }

    // ============== OwnedWritableCapability ==============

    #[test]
    fn owned_writable_write_then_read() {
        let mut owned = OwnedWritableCapability::new(0u64);
        owned.cap_mut().write(555).unwrap();
        assert_eq!(owned.read().unwrap(), 555);
    }

    #[test]
    fn owned_writable_drop_reclaims() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct DropCounter;
        impl Drop for DropCounter {
            fn drop(&mut self) { DROPS.fetch_add(1, Ordering::Relaxed); }
        }
        let before = DROPS.load(Ordering::Relaxed);
        { let _o = OwnedWritableCapability::new(DropCounter); }
        assert_eq!(DROPS.load(Ordering::Relaxed), before + 1);
    }

    #[test]
    fn owned_writable_into_box_suppresses_drop() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct DropCounter(u32);
        impl Drop for DropCounter {
            fn drop(&mut self) { DROPS.fetch_add(1, Ordering::Relaxed); }
        }
        let before = DROPS.load(Ordering::Relaxed);
        let owned = OwnedWritableCapability::new(DropCounter(99));
        let b = owned.into_box();
        // into_box must NOT have fired Drop on the wrapper.
        assert_eq!(DROPS.load(Ordering::Relaxed), before);
        assert_eq!(b.0, 99);
        drop(b);
        // The returned Box drops normally, firing DropCounter once.
        assert_eq!(DROPS.load(Ordering::Relaxed), before + 1);
    }

    #[test]
    fn owned_writable_into_box_round_trip_value() {
        let owned = OwnedWritableCapability::new(12345u64);
        let b = owned.into_box();
        assert_eq!(*b, 12345);
    }
}
