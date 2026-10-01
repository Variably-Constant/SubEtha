//! CHERI-style capability pointers: bounds and permissions checked in
//! software on every access.
//!
//! CHERI (Capability Hardware Enhanced RISC Instructions) augments
//! every pointer with bounds, permissions, otype, and a sealed bit,
//! enforced by hardware on every dereference. Arm's Morello board is a
//! CHERI implementation on Arm silicon.
//!
//! CHERI is by design a RISC instruction-set extension. There is no
//! equivalent capability ISA on x86 / x86_64. The x86-side sibling is
//! [`crate::adaptive_rasp_batch::RaspBatch`]: vector instructions check
//! bounds + permissions for a batch of pointers rather than emulating
//! capability hardware that does not exist on the silicon.
//!
//! # Read vs Write: distinct types
//!
//! This module separates capability semantics at the type level
//! rather than the runtime permission-bit level:
//!
//! - [`ReadableCapability`]`<'a, T>` - bounds-checked read-only view of
//!   memory borrowed for `'a`. Constructed from a `&'a [T]` borrow. No
//!   `write()` method exists, and the Write bit is stripped at
//!   construction.
//!
//! - [`WritableCapability`]`<'a, T>` - bounds-checked read+write access
//!   to memory borrowed uniquely for `'a`. Constructed from a
//!   `&'a mut [T]` borrow, which it holds. Not `Copy` or `Clone`, and
//!   narrowing it or viewing it read-only reborrows it, so one
//!   capability writes a region at a time.
//!
//! - [`OwnedReadableCapability<T>`] / [`OwnedWritableCapability<T>`] -
//!   own a `Box<T>`, free it on drop, and lend capabilities over it
//!   that cannot outlive the owner.
//!
//! A capability cannot outlive the memory it was made from:
//!
//! ```compile_fail
//! use subetha_pointers::adaptive_cheri_pointer::{CapabilityPermission, ReadableCapability};
//! let cap = {
//!     let v = vec![1u64];
//!     let (cap, _) = ReadableCapability::from_slice(&v, CapabilityPermission::Read as u32);
//!     cap
//! };
//! let _ = cap.read();
//! ```
//!
//! ```compile_fail
//! use subetha_pointers::adaptive_cheri_pointer::OwnedReadableCapability;
//! let cap = {
//!     let owned = OwnedReadableCapability::new(1u64);
//!     owned.cap().clone()
//! };
//! let _ = cap.read();
//! ```
//!
//! and two writable capabilities over one region cannot be alive at
//! once:
//!
//! ```compile_fail
//! use subetha_pointers::adaptive_cheri_pointer::{CapabilityPermission, OwnedWritableCapability};
//! let rw = CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32;
//! let mut owned = OwnedWritableCapability::new([0u64; 2]);
//! let mut w = owned.cap_mut();
//! let a = w.narrow(0, 8, rw);
//! let b = w.narrow(0, 8, rw);
//! drop(a);
//! drop(b);
//! ```
//!
//! # Constructor matrix
//!
//! | Type                       | Safe constructor     | Unsafe constructor |
//! |----------------------------|----------------------|--------------------|
//! | ReadableCapability         | from_slice           | new (raw ptr)      |
//! | WritableCapability         | from_slice_mut       | new (raw ptr)      |
//! | OwnedReadableCapability    | new(value), from_box | -                  |
//! | OwnedWritableCapability    | new(value), from_box | -                  |
//!
//! # Backend
//!
//! Bounds and permissions are checked in software on every target. The
//! crate has no hardware CHERI backend and uses no Morello capability
//! instructions.
//!
//! # Safety contract
//!
//! All bounds arithmetic uses `checked_add` so a near-overflow
//! `base + length` cannot wrap and bypass the check, and a
//! capability's address is always aligned for `T`: a constructor or a
//! `narrow` that would misalign it returns
//! [`CapabilityError::Misaligned`].
//!
//! Adjacent capability-like hardware features: Arm PAC (Pointer
//! Authentication Codes), Arm MTE (Memory Tagging Extension), SPARC
//! ADI (Application Data Integrity).

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
    /// The capability's address is not aligned for `T`.
    Misaligned,
}

const SEALED_BIT: u32 = 1 << 31;
const WRITE_BIT: u32 = CapabilityPermission::Write as u32;
const READ_WRITE: u32 = CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32;

/// The region length a capability records for `bytes` bytes: the byte
/// count, or `u32::MAX` for a larger region, which only narrows it.
fn region_len(bytes: usize) -> u32 {
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

/// Checks a capability at `addr` over `[base, base + length)`: the
/// region's end does not overflow, `addr` is at or above `base`,
/// `addr + size_of::<T>()` is at or below the region's end, and `addr`
/// is aligned for `T`.
fn check_access<T>(addr: usize, base: usize, length: u32) -> Result<(), CapabilityError> {
    let region_end = base.checked_add(length as usize)
        .ok_or(CapabilityError::AddressOverflow)?;
    if addr < base { return Err(CapabilityError::OutOfBounds); }
    let access_end = addr.checked_add(std::mem::size_of::<T>())
        .ok_or(CapabilityError::AddressOverflow)?;
    if access_end > region_end { return Err(CapabilityError::OutOfBounds); }
    if !addr.is_multiple_of(std::mem::align_of::<T>()) {
        return Err(CapabilityError::Misaligned);
    }
    Ok(())
}

/// Checks a narrowing of `[base, base + length)` to
/// `[sub_base, sub_base + sub_length)`: the sub-range lies within the
/// region and `sub_base` is aligned for `T`.
fn check_narrow<T>(base: usize, length: u32, sub_base: usize, sub_length: u32)
    -> Result<(), CapabilityError>
{
    let sub_end = sub_base.checked_add(sub_length as usize)
        .ok_or(CapabilityError::AddressOverflow)?;
    let region_end = base.checked_add(length as usize)
        .ok_or(CapabilityError::AddressOverflow)?;
    if sub_base < base || sub_end > region_end {
        return Err(CapabilityError::OutOfBounds);
    }
    if !sub_base.is_multiple_of(std::mem::align_of::<T>()) {
        return Err(CapabilityError::Misaligned);
    }
    Ok(())
}

// =========================================================================
// ReadableCapability<'a, T> - bounds-checked read-only access.
// =========================================================================

/// Read-only bounds-checked capability over memory borrowed for `'a`.
/// The Write permission bit is stripped at construction; no `write()`
/// method exists.
///
/// Layout (24 bytes):
/// ```text
/// ptr:    *const T   (8 bytes) - address, aligned for T
/// base:   usize      (8 bytes) - lower bound (often equals ptr)
/// length: u32        (4 bytes) - bytes from base
/// perms:  u32        (4 bytes) - permission bitmask + sealed bit;
///                                 Write bit guaranteed cleared
/// ```
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ReadableCapability<'a, T> {
    ptr: *const T,
    base: usize,
    length: u32,
    perms: u32,
    _borrow: PhantomData<&'a T>,
}

impl<'a, T> ReadableCapability<'a, T> {
    /// Direction signature of `ReadableCapability<T>`. Engages the
    /// `K_bounds` axis (runtime base / length / permissions stored
    /// at slot for CHERI-style bounds enforcement on every deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Bounds],
    );

    /// Returns `Misaligned` when `ptr` is not aligned for `T`, and
    /// `OutOfBounds` or `AddressOverflow` when `ptr` does not lie in
    /// the region. The Write permission bit is stripped.
    ///
    /// # Safety
    ///
    /// Caller guarantees `[base, base + length)` is valid memory that
    /// nothing writes for `'a`, and that `ptr` carries the provenance
    /// of that memory.
    pub unsafe fn new(ptr: *const T, base: usize, length: u32, perms: u32)
        -> Result<Self, CapabilityError>
    {
        check_access::<T>(ptr as usize, base, length)?;
        Ok(Self {
            ptr, base, length,
            perms: perms & !WRITE_BIT,  // strip Write
            _borrow: PhantomData,
        })
    }

    /// Safe constructor: a read-only capability over a borrowed slice,
    /// valid for as long as the borrow. The Write bit is stripped. The
    /// slice comes back beside it; a slice over `u32::MAX` bytes gives
    /// a capability over its first `u32::MAX` bytes.
    pub fn from_slice(slice: &'a [T], perms: u32)
        -> (ReadableCapability<'a, T>, &'a [T])
    {
        let ptr = slice.as_ptr();
        let cap = ReadableCapability {
            ptr,
            base: ptr as usize,
            length: region_len(std::mem::size_of_val(slice)),
            perms: perms & !WRITE_BIT,
            _borrow: PhantomData,
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
        check_access::<T>(self.ptr as usize, self.base, self.length)?;
        // SAFETY: bounds, alignment, permission and sealed checks
        // above; the borrow held for 'a keeps the memory live and
        // unwritten.
        Ok(unsafe { std::ptr::read(self.ptr) })
    }

    /// Narrow this capability to a sub-range, at the sub-range's start.
    /// The Write bit stays stripped (Readable cannot grant Write).
    pub fn narrow(&self, sub_base: usize, sub_length: u32, sub_perms: u32)
        -> Result<ReadableCapability<'a, T>, CapabilityError>
    {
        check_narrow::<T>(self.base, self.length, sub_base, sub_length)?;
        let new_perms = (self.perms & sub_perms) & !SEALED_BIT & !WRITE_BIT;
        let offset = sub_base.wrapping_sub(self.ptr as usize);
        Ok(ReadableCapability {
            ptr: self.ptr.cast::<u8>().wrapping_add(offset).cast::<T>(),
            base: sub_base,
            length: sub_length,
            perms: new_perms,
            _borrow: PhantomData,
        })
    }
}

// =========================================================================
// WritableCapability<'a, T> - bounds-checked read+write access. !Copy/!Clone.
// =========================================================================

/// Read+Write bounds-checked capability over memory borrowed uniquely
/// for `'a`. Not Copy/Clone, and every capability made from it
/// reborrows it, so one capability writes a region at a time.
///
/// Constructed from `&mut [T]`, or lent by
/// [`OwnedWritableCapability::cap_mut`].
#[derive(Debug)]
#[repr(C)]
pub struct WritableCapability<'a, T> {
    ptr: *mut T,
    base: usize,
    length: u32,
    perms: u32,
    _borrow: PhantomData<&'a mut T>,
}

impl<'a, T> WritableCapability<'a, T> {
    /// Direction signature of `WritableCapability<T>`. Engages the
    /// `K_bounds` axis (runtime base / length / permissions stored
    /// at slot for CHERI-style bounds enforcement on every deref).
    pub const SIGNATURE: subetha_core::AxisMask = subetha_core::AxisMask::from_axes(
        &[subetha_core::Axis::Bounds],
    );

    /// Returns `Misaligned` when `ptr` is not aligned for `T`, and
    /// `OutOfBounds` or `AddressOverflow` when `ptr` does not lie in
    /// the region.
    ///
    /// # Safety
    ///
    /// Caller guarantees `[base, base + length)` is valid memory that
    /// nothing else reads or writes for `'a`, and that `ptr` carries
    /// the provenance of that memory.
    pub unsafe fn new(ptr: *mut T, base: usize, length: u32, perms: u32)
        -> Result<Self, CapabilityError>
    {
        check_access::<T>(ptr as usize, base, length)?;
        Ok(Self { ptr, base, length, perms, _borrow: PhantomData })
    }

    /// Safe constructor: a writable capability over a mutable slice
    /// borrow, which it holds for `'a`. Grants Read + Write
    /// permissions. A slice over `u32::MAX` bytes gives a capability
    /// over its first `u32::MAX` bytes.
    pub fn from_slice_mut(slice: &'a mut [T]) -> WritableCapability<'a, T> {
        let length = region_len(std::mem::size_of_val(slice));
        let ptr = slice.as_mut_ptr();
        WritableCapability {
            ptr, base: ptr as usize, length, perms: READ_WRITE, _borrow: PhantomData,
        }
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
        check_access::<T>(self.ptr as usize, self.base, self.length)?;
        // SAFETY: bounds, alignment, permission and sealed checks
        // above; the unique borrow held for 'a keeps the memory live.
        Ok(unsafe { std::ptr::read(self.ptr) })
    }

    /// Write through the capability. The `&mut self` receiver and the
    /// unique borrow the capability holds make it the only writer.
    pub fn write(&mut self, value: T) -> Result<(), CapabilityError> {
        if self.is_sealed() { return Err(CapabilityError::Sealed); }
        if !self.has_permission(CapabilityPermission::Write) {
            return Err(CapabilityError::PermissionDenied);
        }
        check_access::<T>(self.ptr as usize, self.base, self.length)?;
        // SAFETY: bounds, alignment, permission and sealed checks
        // above; unique writer by &mut self and the held borrow.
        unsafe { std::ptr::write(self.ptr, value); }
        Ok(())
    }

    /// Narrow to a sub-range, at the sub-range's start. The narrowed
    /// capability reborrows this one, which is unusable until the
    /// narrowed one is dropped. To narrow to a read-only view, use
    /// `narrow_readable`.
    pub fn narrow(&mut self, sub_base: usize, sub_length: u32, sub_perms: u32)
        -> Result<WritableCapability<'_, T>, CapabilityError>
    {
        check_narrow::<T>(self.base, self.length, sub_base, sub_length)?;
        let new_perms = (self.perms & sub_perms) & !SEALED_BIT;
        let offset = sub_base.wrapping_sub(self.ptr as usize);
        Ok(WritableCapability {
            ptr: self.ptr.cast::<u8>().wrapping_add(offset).cast::<T>(),
            base: sub_base,
            length: sub_length,
            perms: new_perms,
            _borrow: PhantomData,
        })
    }

    /// Narrow to a read-only view, at the sub-range's start. The view
    /// has no Write perm whatever `sub_perms` contains, and this
    /// capability cannot write while the view is alive.
    pub fn narrow_readable(&self, sub_base: usize, sub_length: u32, sub_perms: u32)
        -> Result<ReadableCapability<'_, T>, CapabilityError>
    {
        check_narrow::<T>(self.base, self.length, sub_base, sub_length)?;
        let new_perms = (self.perms & sub_perms) & !SEALED_BIT & !WRITE_BIT;
        let offset = sub_base.wrapping_sub(self.ptr as usize);
        Ok(ReadableCapability {
            ptr: self.ptr.cast_const().cast::<u8>().wrapping_add(offset).cast::<T>(),
            base: sub_base,
            length: sub_length,
            perms: new_perms,
            _borrow: PhantomData,
        })
    }

    /// Borrow this capability as a read-only view (no Write perm). This
    /// capability cannot write while the view is alive.
    pub fn as_readable(&self) -> ReadableCapability<'_, T> {
        ReadableCapability {
            ptr: self.ptr.cast_const(),
            base: self.base,
            length: self.length,
            perms: self.perms & !WRITE_BIT,
            _borrow: PhantomData,
        }
    }
}

// =========================================================================
// OwnedReadableCapability<T> + OwnedWritableCapability<T> - owners.
// =========================================================================

/// Owns a `Box<T>` and lends read-only capabilities over it that cannot
/// outlive it.
pub struct OwnedReadableCapability<T> {
    value: Box<T>,
}

impl<T> OwnedReadableCapability<T> {
    /// Heap-allocate `value`.
    pub fn new(value: T) -> Self {
        Self::from_box(Box::new(value))
    }

    /// Own an existing `Box<T>`.
    pub fn from_box(b: Box<T>) -> Self {
        Self { value: b }
    }

    /// A capability with Read permission only, over the owned value,
    /// alive while `self` is borrowed.
    pub fn cap(&self) -> ReadableCapability<'_, T> {
        let ptr: *const T = &*self.value;
        ReadableCapability {
            ptr,
            base: ptr as usize,
            length: region_len(std::mem::size_of::<T>()),
            perms: CapabilityPermission::Read as u32,
            _borrow: PhantomData,
        }
    }

    /// Consume and return the underlying Box.
    pub fn into_box(self) -> Box<T> {
        self.value
    }
}

/// Owns a `Box<T>` and lends read and write capabilities over it that
/// cannot outlive it.
pub struct OwnedWritableCapability<T> {
    value: Box<T>,
}

impl<T> OwnedWritableCapability<T> {
    /// Heap-allocate `value`.
    pub fn new(value: T) -> Self {
        Self::from_box(Box::new(value))
    }

    /// Own an existing `Box<T>`.
    pub fn from_box(b: Box<T>) -> Self {
        Self { value: b }
    }

    /// A capability with Read permission only, over the owned value,
    /// alive while `self` is borrowed.
    pub fn cap(&self) -> ReadableCapability<'_, T> {
        let ptr: *const T = &*self.value;
        ReadableCapability {
            ptr,
            base: ptr as usize,
            length: region_len(std::mem::size_of::<T>()),
            perms: CapabilityPermission::Read as u32,
            _borrow: PhantomData,
        }
    }

    /// A capability with Read + Write permissions over the owned
    /// value, alive while `self` is mutably borrowed.
    pub fn cap_mut(&mut self) -> WritableCapability<'_, T> {
        let ptr: *mut T = &mut *self.value;
        WritableCapability {
            ptr,
            base: ptr as usize,
            length: region_len(std::mem::size_of::<T>()),
            perms: READ_WRITE,
            _borrow: PhantomData,
        }
    }

    /// Consume and return the underlying Box.
    pub fn into_box(self) -> Box<T> {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============== Layout ==============

    #[test]
    fn readable_layout_is_24_bytes() {
        assert_eq!(std::mem::size_of::<ReadableCapability<'_, u64>>(), 24);
    }

    #[test]
    fn writable_layout_is_24_bytes() {
        assert_eq!(std::mem::size_of::<WritableCapability<'_, u64>>(), 24);
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
    fn readable_narrow_reads_the_sub_range() {
        let storage: Vec<u64> = vec![10, 20, 30, 40];
        let (cap, anchor) = ReadableCapability::from_slice(
            &storage, CapabilityPermission::Read as u32,
        );
        let base = anchor.as_ptr() as usize;
        let third = cap.narrow(base + 16, 8, CapabilityPermission::Read as u32).unwrap();
        assert_eq!(third.read().unwrap(), 30);
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

    #[test]
    fn readable_misaligned_narrow_is_misaligned() {
        let storage: Vec<u64> = vec![0; 4];
        let (cap, anchor) = ReadableCapability::from_slice(
            &storage, CapabilityPermission::Read as u32,
        );
        let base = anchor.as_ptr() as usize;
        assert_eq!(
            cap.narrow(base + 1, 8, CapabilityPermission::Read as u32).err(),
            Some(CapabilityError::Misaligned)
        );
    }

    // ============== WritableCapability ==============

    #[test]
    fn writable_from_slice_mut_grants_read_and_write() {
        let mut storage: Vec<u64> = vec![0];
        let cap = WritableCapability::from_slice_mut(&mut storage);
        assert!(cap.has_permission(CapabilityPermission::Read));
        assert!(cap.has_permission(CapabilityPermission::Write));
    }

    #[test]
    fn writable_write_then_read() {
        let mut storage: Vec<u64> = vec![0];
        {
            let mut cap = WritableCapability::from_slice_mut(&mut storage);
            cap.write(7777).unwrap();
            assert_eq!(cap.read().unwrap(), 7777);
        }
        assert_eq!(storage[0], 7777);
    }

    #[test]
    fn writable_sealed_blocks_write() {
        let mut storage: Vec<u64> = vec![0];
        let cap = WritableCapability::from_slice_mut(&mut storage);
        let mut sealed = cap.sealed();
        assert_eq!(sealed.write(99u64).err(), Some(CapabilityError::Sealed));
    }

    #[test]
    fn writable_as_readable_view_strips_write() {
        let mut storage: Vec<u64> = vec![42];
        let cap = WritableCapability::from_slice_mut(&mut storage);
        let read_view = cap.as_readable();
        assert!(read_view.has_permission(CapabilityPermission::Read));
        assert!(!read_view.has_permission(CapabilityPermission::Write));
        assert_eq!(read_view.read().unwrap(), 42);
    }

    #[test]
    fn writable_narrow_readable_strips_write() {
        let mut storage: Vec<u64> = vec![0, 0];
        let cap = WritableCapability::from_slice_mut(&mut storage);
        let base = cap.ptr as usize;
        let narrowed: ReadableCapability<'_, u64> = cap.narrow_readable(
            base, 8,
            CapabilityPermission::Read as u32 | CapabilityPermission::Write as u32,
        ).unwrap();
        assert!(narrowed.has_permission(CapabilityPermission::Read));
        assert!(!narrowed.has_permission(CapabilityPermission::Write));
    }

    #[test]
    fn writable_narrow_writes_the_sub_range() {
        let mut storage: Vec<u64> = vec![0, 0];
        {
            let mut cap = WritableCapability::from_slice_mut(&mut storage);
            let base = cap.ptr as usize;
            let mut second = cap.narrow(base + 8, 8, READ_WRITE).unwrap();
            second.write(5).unwrap();
        }
        assert_eq!(storage, vec![0, 5]);
    }

    #[test]
    fn writable_unsafe_new_overflow_guards() {
        let ptr = usize::MAX as *mut u64;
        let r = unsafe {
            WritableCapability::<u64>::new(ptr, usize::MAX, 16, READ_WRITE)
        };
        assert_eq!(r.err(), Some(CapabilityError::AddressOverflow));
    }

    // ============== OwnedReadableCapability ==============

    #[test]
    fn owned_readable_new_and_read() {
        let owned = OwnedReadableCapability::new(42u64);
        assert_eq!(owned.cap().read().unwrap(), 42);
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
        assert_eq!(owned.cap().read().unwrap(), 555);
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
        // into_box hands the value over without dropping it.
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
