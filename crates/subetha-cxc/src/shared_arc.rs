//! `SharedArc<T>` - a value in shared memory kept alive by the
//! processes holding it, and released when the last one lets go.
//!
//! # Holders
//!
//! Ownership is a [`HolderTable`], one slot per holder, each stamped
//! with the process that took it, and
//! [`strong_count`](SharedArc::strong_count) is how many slots are
//! held. A holder whose process dies never releases, so its slot is
//! reclaimed by probing whether that process is still there.
//!
//! # The value is immutable
//!
//! Written once by the call that creates the backing and read-only
//! afterwards, so a reference into the mapping is sound without a lock.
//! Mutable shared state goes inside the value: an atomic, a
//! [`SharedCell`](crate::shared_cell), or a lock such as
//! [`SharedRWLock`](crate::shared_rw_lock::SharedRWLock) or
//! [`OwnerLease`](crate::owner_lease::OwnerLease).
//!
//! # The backing
//!
//! [`LastHolder`] decides what becomes of the file, and the caller sets
//! it at create. `Unlink` removes the backing when the last holder
//! releases; `Keep` leaves it for a process that attaches later.
//!
//! Under `Unlink`, a process opening the path as the last holder
//! releases either attaches before the unlink and gets a live mapping,
//! or finds no file and gets `NotFound`. It never sees a half-torn
//! region.
//!
//! # Layout
//!
//! ```text
//! | ArcHeader (64B) | HolderSlot 0..N (64B each) | value: T |
//! ```

use std::fs::File;
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::holder_table::{holder_table_size, HolderTable};

pub const ARC_MAGIC: u64 = 0x5348_4152_4544_4152; // "SHAREDAR"

/// Payload every holder slot carries. The table needs a value that is
/// neither free nor reserved, and a holder has nothing else to say.
const HOLDER_PRESENT: u64 = 1;

/// A value that may live in a mapping several processes share.
///
/// Broader than `Copy`, which excludes the atomics: `AtomicU64` is not
/// `Copy`, so a `Copy` bound admits no shared counter, no shared flag
/// and no lock word.
///
/// # Safety
///
/// Implementing this asserts all of:
/// - the layout is stable across processes: `#[repr(C)]` or
///   `#[repr(transparent)]`, or a primitive;
/// - no pointers or references, which address one process's memory and
///   mean nothing in another's;
/// - no `Drop`. The value in the mapping is never dropped, so a type
///   that owns anything leaks it;
/// - concurrent access from several processes is sound, which for a
///   plain value means it is only read and for a mutable one means it
///   is an atomic or carries its own synchronization.
pub unsafe trait ShmValue: Send + Sync + 'static {}

macro_rules! shm_value {
    ($($t:ty),* $(,)?) => { $(unsafe impl ShmValue for $t {})* };
}

shm_value!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64, bool, char);
shm_value!(
    std::sync::atomic::AtomicU8,
    std::sync::atomic::AtomicU16,
    std::sync::atomic::AtomicU32,
    std::sync::atomic::AtomicU64,
    std::sync::atomic::AtomicUsize,
    std::sync::atomic::AtomicI8,
    std::sync::atomic::AtomicI16,
    std::sync::atomic::AtomicI32,
    std::sync::atomic::AtomicI64,
    std::sync::atomic::AtomicIsize,
    std::sync::atomic::AtomicBool,
);

unsafe impl<T: ShmValue, const N: usize> ShmValue for [T; N] {}

/// What becomes of the backing when the last holder releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastHolder {
    /// Remove it, the `Arc` shape.
    Unlink,
    /// Leave it for a later process to attach to.
    Keep,
}

#[repr(C, align(64))]
pub struct ArcHeader {
    pub magic: u64,
    /// Holder slots the table carries.
    pub capacity: u64,
    /// `size_of::<T>()` at create, so attaching as the wrong type is
    /// refused rather than reinterpreting the bytes.
    pub value_size: u64,
    _pad: [u8; 40],
}

const _: () = {
    assert!(size_of::<ArcHeader>() == 64);
};

/// Bytes the backing needs for `capacity` holders of a `T`.
pub const fn arc_file_size<T>(capacity: usize) -> usize {
    size_of::<ArcHeader>() + holder_table_size(capacity) + size_of::<T>()
}

const fn value_offset(capacity: usize) -> usize {
    size_of::<ArcHeader>() + holder_table_size(capacity)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArcError {
    /// Every holder slot is held by a live process.
    HoldersExhausted,
    /// The backing was built for a different capacity or a different
    /// value type.
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for ArcError {
    fn from(e: std::io::Error) -> Self {
        ArcError::IoError(e.kind())
    }
}

impl std::fmt::Display for ArcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArcError::HoldersExhausted => write!(f, "every holder slot is held"),
            ArcError::LayoutMismatch => write!(f, "shared arc layout mismatch"),
            ArcError::IoError(k) => write!(f, "shared arc io error: {k:?}"),
        }
    }
}

impl std::error::Error for ArcError {}

/// A value in shared memory and the processes holding it.
pub struct SharedArc<T: ShmValue> {
    _file: File,
    mmap: MmapMut,
    holders: HolderTable,
    slot: usize,
    capacity: usize,
    path: PathBuf,
    on_last: LastHolder,
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: ShmValue> Send for SharedArc<T> {}
unsafe impl<T: ShmValue> Sync for SharedArc<T> {}

impl<T: ShmValue> SharedArc<T> {
    /// Obtain the value at `path`, writing `value` if the path does not
    /// yet exist and attaching to what is there if it does, and take a
    /// holder slot either way.
    ///
    /// `value` is written only by the call that creates the backing; an
    /// attach leaves the value already there, exactly as a second
    /// `Arc::clone` does not overwrite what the first one points at.
    pub fn create(
        path: impl AsRef<Path>,
        value: T,
        max_holders: usize,
        on_last: LastHolder,
    ) -> Result<Self, ArcError> {
        assert!(max_holders >= 1);
        let path = path.as_ref().to_path_buf();
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            &path,
            arc_file_size::<T>(max_holders),
            |ptr| unsafe { Self::init_region(ptr, value, max_holders) },
            |ptr| unsafe { (*(ptr as *const ArcHeader)).magic == ARC_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, ArcError::LayoutMismatch))?;
        Self::attach(file, mmap, path, max_holders, on_last)
    }

    /// Attach to an existing value and take a holder slot.
    pub fn open(
        path: impl AsRef<Path>,
        max_holders: usize,
        on_last: LastHolder,
    ) -> Result<Self, ArcError> {
        let path = path.as_ref().to_path_buf();
        let file = crate::region_file::open_existing(&path)?;
        let total = arc_file_size::<T>(max_holders);
        if file.metadata()?.len() < total as u64 {
            return Err(ArcError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::attach(file, mmap, path, max_holders, on_last)
    }

    fn attach(
        file: File,
        mmap: MmapMut,
        path: PathBuf,
        capacity: usize,
        on_last: LastHolder,
    ) -> Result<Self, ArcError> {
        let header = unsafe { &*(mmap.as_ptr() as *const ArcHeader) };
        if header.magic != ARC_MAGIC
            || header.capacity != capacity as u64
            || header.value_size != size_of::<T>() as u64
        {
            return Err(ArcError::LayoutMismatch);
        }
        let holders = unsafe {
            HolderTable::from_ptr(mmap.as_ptr().add(size_of::<ArcHeader>()), capacity)
        };
        let slot = match holders.claim(HOLDER_PRESENT) {
            Some(s) => s,
            None => {
                holders.reap_dead();
                holders.claim(HOLDER_PRESENT).ok_or(ArcError::HoldersExhausted)?
            }
        };
        Ok(Self {
            _file: file,
            mmap,
            holders,
            slot,
            capacity,
            path,
            on_last,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Lay out the backing: value and sizes first, magic last, because
    /// attachers spin on it and must not see a value that is not there
    /// yet.
    ///
    /// # Safety
    /// `ptr` addresses at least `arc_file_size::<T>(capacity)` writable
    /// zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, value: T, capacity: usize) {
        unsafe {
            std::ptr::write_unaligned(ptr.add(value_offset(capacity)) as *mut T, value);
            let hdr = ptr as *mut ArcHeader;
            (*hdr).capacity = capacity as u64;
            (*hdr).value_size = size_of::<T>() as u64;
            std::ptr::write_volatile(&raw mut (*hdr).magic, ARC_MAGIC);
        }
    }

    /// The shared value.
    #[inline]
    pub fn get(&self) -> &T {
        unsafe { &*(self.mmap.as_ptr().add(value_offset(self.capacity)) as *const T) }
    }

    /// Processes holding this value, this one included.
    ///
    /// A holder whose process has died still counts until something
    /// reaps it; [`reap_dead_holders`](Self::reap_dead_holders) is what
    /// does, and an `open` that would otherwise report the table full
    /// calls it first.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.holders.live()
    }

    /// Holder slots the backing carries.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Free every slot whose holding process is gone, and report how
    /// many went.
    pub fn reap_dead_holders(&self) -> usize {
        self.holders.reap_dead()
    }

    /// The holder table, for a caller that wants it directly.
    #[inline]
    pub fn holders(&self) -> &HolderTable {
        &self.holders
    }

    pub fn flush(&self) -> Result<(), ArcError> {
        self.mmap.flush()?;
        Ok(())
    }
}

impl<T: ShmValue> Deref for SharedArc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        self.get()
    }
}

impl<T: ShmValue + std::fmt::Debug> std::fmt::Debug for SharedArc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedArc")
            .field("value", self.get())
            .field("strong_count", &self.strong_count())
            .finish()
    }
}

impl<T: ShmValue> Drop for SharedArc<T> {
    fn drop(&mut self) {
        self.holders.release(self.slot);
        if self.on_last == LastHolder::Keep {
            return;
        }
        // Reap first, so a table full of corpses does not keep the
        // backing alive after the last live holder has gone.
        self.holders.reap_dead();
        if self.holders.live() != 0 {
            return;
        }
        // Windows refuses to remove a file while a mapping is live, so
        // the mapping and the handle go first. A concurrent open either
        // attached before this or gets NotFound; neither sees a torn
        // region.
        let path = std::mem::take(&mut self.path);
        match MmapOptions::new().len(1).map_anon() {
            Ok(m) => drop(std::mem::replace(&mut self.mmap, m)),
            // The removal below still takes the name away, since a
            // region is opened so that it can be removed while mapped.
            // What is lost is this process's own address space, which
            // nothing else reclaims until it exits.
            Err(e) => eprintln!(
                "subetha: the last holder of {} could not release its mapping: {e}",
                path.display()
            ),
        }
        match crate::region_file::remove(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => eprintln!(
                "subetha: the last holder of {} left its backing file behind: {e}",
                path.display()
            ),
        }
    }
}

/// Bytes the backing needs for `capacity` holders of a value of
/// `value_bytes`.
pub const fn arc_file_size_dyn(capacity: usize, value_bytes: usize) -> usize {
    size_of::<ArcHeader>() + holder_table_size(capacity) + value_bytes
}

/// `SharedArc` with the value's size given at run time instead of by a
/// type: the same header, the same holder table and the same last-holder
/// policy, over a region of `value_bytes` the caller reads and writes by
/// offset.
///
/// This exists for callers that cannot name a Rust type. Prefer
/// [`SharedArc`] wherever the type is known at compile time; it hands
/// back a `&T` rather than bytes, and the type carries what the bytes
/// mean.
///
/// # Attaching across the two shapes
///
/// The header is identical and records the value's size, so a
/// `SharedArcDyn` of `size_of::<T>()` bytes and a `SharedArc<T>` attach
/// to each other's backing, and a size that disagrees is refused. That is
/// what lets a holder that names the type and one that does not share a
/// value.
///
/// It also means the byte side can write anything into a region the other
/// side reads as a `T`, which a size check cannot catch. Both sides
/// agreeing on the layout is the caller's obligation, as it already is
/// for every `ShmValue` implementation.
///
/// # The bytes are not synchronized
///
/// [`SharedArc`]'s value is written once at create and read-only after,
/// so a reference into it needs no lock. A byte region a caller may write
/// carries no such promise: a write is a copy, and a concurrent read can
/// see it half done. Callers that mutate share it under something that
/// orders their access - an atomic laid out inside the region, a
/// [`SharedCell`](crate::shared_cell), or a lock - exactly as the
/// `ShmValue` contract already requires of a mutable value.
pub struct SharedArcDyn {
    _file: File,
    mmap: MmapMut,
    holders: HolderTable,
    slot: usize,
    capacity: usize,
    value_bytes: usize,
    path: PathBuf,
    on_last: LastHolder,
}

unsafe impl Send for SharedArcDyn {}
unsafe impl Sync for SharedArcDyn {}

impl SharedArcDyn {
    /// Obtain the value at `path`, writing `value` if the path does not
    /// yet exist and attaching to what is there if it does, and take a
    /// holder slot either way.
    ///
    /// `value` sets the region's length and its initial contents, and is
    /// written only by the call that creates the backing; an attach
    /// leaves what is there alone.
    pub fn create(
        path: impl AsRef<Path>,
        value: &[u8],
        max_holders: usize,
        on_last: LastHolder,
    ) -> Result<Self, ArcError> {
        assert!(max_holders >= 1);
        let path = path.as_ref().to_path_buf();
        let value_bytes = value.len();
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            &path,
            arc_file_size_dyn(max_holders, value_bytes),
            |ptr| unsafe { Self::init_region(ptr, value, max_holders) },
            |ptr| unsafe { (*(ptr as *const ArcHeader)).magic == ARC_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, ArcError::LayoutMismatch))?;
        Self::attach(file, mmap, path, max_holders, value_bytes, on_last)
    }

    /// Attach to an existing value of `value_bytes` and take a holder
    /// slot. A backing built for another size is a `LayoutMismatch`.
    pub fn open(
        path: impl AsRef<Path>,
        value_bytes: usize,
        max_holders: usize,
        on_last: LastHolder,
    ) -> Result<Self, ArcError> {
        let path = path.as_ref().to_path_buf();
        let file = crate::region_file::open_existing(&path)?;
        let total = arc_file_size_dyn(max_holders, value_bytes);
        if file.metadata()?.len() < total as u64 {
            return Err(ArcError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::attach(file, mmap, path, max_holders, value_bytes, on_last)
    }

    fn attach(
        file: File,
        mmap: MmapMut,
        path: PathBuf,
        capacity: usize,
        value_bytes: usize,
        on_last: LastHolder,
    ) -> Result<Self, ArcError> {
        let header = unsafe { &*(mmap.as_ptr() as *const ArcHeader) };
        if header.magic != ARC_MAGIC
            || header.capacity != capacity as u64
            || header.value_size != value_bytes as u64
        {
            return Err(ArcError::LayoutMismatch);
        }
        let holders =
            unsafe { HolderTable::from_ptr(mmap.as_ptr().add(size_of::<ArcHeader>()), capacity) };
        let slot = match holders.claim(HOLDER_PRESENT) {
            Some(s) => s,
            None => {
                holders.reap_dead();
                holders.claim(HOLDER_PRESENT).ok_or(ArcError::HoldersExhausted)?
            }
        };
        Ok(Self { _file: file, mmap, holders, slot, capacity, value_bytes, path, on_last })
    }

    /// Lay out the backing: value and sizes first, magic last, because
    /// attachers spin on it and must not see a value that is not there
    /// yet.
    ///
    /// # Safety
    /// `ptr` addresses at least `arc_file_size_dyn(capacity, value.len())`
    /// writable zeroed bytes.
    unsafe fn init_region(ptr: *mut u8, value: &[u8], capacity: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                ptr.add(value_offset(capacity)),
                value.len(),
            );
            let hdr = ptr as *mut ArcHeader;
            (*hdr).capacity = capacity as u64;
            (*hdr).value_size = value.len() as u64;
            std::ptr::write_volatile(&raw mut (*hdr).magic, ARC_MAGIC);
        }
    }

    /// The shared region.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.mmap.as_ptr().add(value_offset(self.capacity)),
                self.value_bytes,
            )
        }
    }

    /// Copy `out.len()` bytes from `offset` into `out`. Refuses a range
    /// that runs past the region rather than reading whatever follows it.
    pub fn read_at(&self, offset: usize, out: &mut [u8]) -> Result<(), ArcError> {
        let end = offset.checked_add(out.len()).ok_or(ArcError::LayoutMismatch)?;
        if end > self.value_bytes {
            return Err(ArcError::LayoutMismatch);
        }
        out.copy_from_slice(&self.as_slice()[offset..end]);
        Ok(())
    }

    /// Copy `src` into the region at `offset`. Refuses a range that runs
    /// past the region rather than writing past it.
    ///
    /// Not atomic and not ordered against another holder's read; see this
    /// type's note on synchronizing writes.
    pub fn write_at(&self, offset: usize, src: &[u8]) -> Result<(), ArcError> {
        let end = offset.checked_add(src.len()).ok_or(ArcError::LayoutMismatch)?;
        if end > self.value_bytes {
            return Err(ArcError::LayoutMismatch);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                self.mmap.as_ptr().add(value_offset(self.capacity) + offset) as *mut u8,
                src.len(),
            );
        }
        Ok(())
    }

    /// Bytes the shared region holds.
    #[inline]
    pub fn value_len(&self) -> usize {
        self.value_bytes
    }

    /// Processes holding this value, this one included.
    #[inline]
    pub fn strong_count(&self) -> usize {
        self.holders.live()
    }

    /// Holder slots the backing carries.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Free every slot whose holding process is gone, and report how many
    /// went.
    pub fn reap_dead_holders(&self) -> usize {
        self.holders.reap_dead()
    }

    /// The holder table, for a caller that wants it directly.
    #[inline]
    pub fn holders(&self) -> &HolderTable {
        &self.holders
    }

    pub fn flush(&self) -> Result<(), ArcError> {
        self.mmap.flush()?;
        Ok(())
    }
}

impl std::fmt::Debug for SharedArcDyn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedArcDyn")
            .field("value_len", &self.value_bytes)
            .field("strong_count", &self.strong_count())
            .finish()
    }
}

impl Drop for SharedArcDyn {
    fn drop(&mut self) {
        self.holders.release(self.slot);
        if self.on_last == LastHolder::Keep {
            return;
        }
        // Reap first, so a table full of corpses does not keep the
        // backing alive after the last live holder has gone.
        self.holders.reap_dead();
        if self.holders.live() != 0 {
            return;
        }
        // Windows refuses to remove a file while a mapping is live, so
        // the mapping and the handle go first.
        let path = std::mem::take(&mut self.path);
        match MmapOptions::new().len(1).map_anon() {
            Ok(m) => drop(std::mem::replace(&mut self.mmap, m)),
            Err(e) => eprintln!(
                "subetha: the last holder of {} could not release its mapping: {e}",
                path.display()
            ),
        }
        match crate::region_file::remove(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => eprintln!(
                "subetha: the last holder of {} left its backing file behind: {e}",
                path.display()
            ),
        }
    }
}

/// A counter every holder of a `SharedArc<SharedCounter>` can bump.
///
/// The arc is immutable; the atomic inside it is not.
#[repr(transparent)]
#[derive(Debug)]
pub struct SharedCounter(pub AtomicU64);

unsafe impl ShmValue for SharedCounter {}

impl SharedCounter {
    #[inline]
    pub fn add(&self, n: u64) -> u64 {
        self.0.fetch_add(n, Ordering::AcqRel)
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        dir: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    fn fixture(name: &str) -> (Fixture, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("subetha_arc_{name}_{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("value.bin");
        (Fixture { dir }, path)
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    #[repr(C)]
    struct Config {
        workers: u32,
        budget: u64,
    }

    // The caller asserts what the compiler cannot infer: repr(C), no
    // pointers, no Drop.
    unsafe impl ShmValue for Config {}

    #[test]
    fn a_dyn_arc_shares_its_region_and_counts_its_holders() {
        let (_f, path) = fixture("dyn-share");
        let a = SharedArcDyn::create(&path, &[1, 2, 3, 4], 8, LastHolder::Keep).unwrap();
        assert_eq!(a.as_slice(), &[1, 2, 3, 4]);
        assert_eq!(a.value_len(), 4);
        assert_eq!(a.strong_count(), 1);

        let b = SharedArcDyn::open(&path, 4, 8, LastHolder::Keep).unwrap();
        assert_eq!(b.as_slice(), &[1, 2, 3, 4], "the same region, not a fresh one");
        assert_eq!(a.strong_count(), 2);

        // A write by one holder is what the other reads: one region, not
        // a copy each.
        b.write_at(1, &[9, 9]).unwrap();
        assert_eq!(a.as_slice(), &[1, 9, 9, 4]);
        let mut out = [0u8; 2];
        a.read_at(2, &mut out).unwrap();
        assert_eq!(out, [9, 4]);

        drop(b);
        assert_eq!(a.strong_count(), 1, "the holder that let go is not counted");
    }

    /// Creating over a live region attaches to it, exactly as the typed
    /// arc does: the second value is not written over the first.
    #[test]
    fn creating_a_dyn_arc_over_a_live_one_attaches() {
        let (_f, path) = fixture("dyn-attach");
        let a = SharedArcDyn::create(&path, &[7, 7, 7, 7], 4, LastHolder::Keep).unwrap();
        let b = SharedArcDyn::create(&path, &[9, 9, 9, 9], 4, LastHolder::Keep).unwrap();
        assert_eq!(b.as_slice(), &[7, 7, 7, 7], "the first value stands");
        assert_eq!(a.strong_count(), 2);
    }

    /// The header records the value's size, so a size that disagrees is
    /// refused rather than reinterpreting the bytes.
    #[test]
    fn opening_a_dyn_arc_at_the_wrong_size_is_refused() {
        let (_f, path) = fixture("dyn-size");
        let _a = SharedArcDyn::create(&path, &[0; 8], 4, LastHolder::Keep).unwrap();
        assert_eq!(
            SharedArcDyn::open(&path, 16, 4, LastHolder::Keep).unwrap_err(),
            ArcError::LayoutMismatch,
        );
        assert_eq!(
            SharedArcDyn::open(&path, 8, 9, LastHolder::Keep).unwrap_err(),
            ArcError::LayoutMismatch,
            "a capacity that disagrees is refused too",
        );
    }

    /// The two shapes are one layout, which is what lets a holder that
    /// names the type and one that does not share a value.
    #[test]
    fn a_dyn_arc_and_a_typed_arc_attach_to_each_other() {
        let (_f, path) = fixture("dyn-typed");
        let typed = SharedArc::create(&path, 0x1122_3344_5566_7788u64, 4, LastHolder::Keep).unwrap();
        let raw = SharedArcDyn::open(&path, 8, 4, LastHolder::Keep).unwrap();
        assert_eq!(raw.as_slice(), &0x1122_3344_5566_7788u64.to_ne_bytes());
        assert_eq!(typed.strong_count(), 2, "both shapes hold the same table");

        // And a size that is not the type's is refused from the byte side.
        assert_eq!(
            SharedArcDyn::open(&path, 4, 4, LastHolder::Keep).unwrap_err(),
            ArcError::LayoutMismatch,
        );
    }

    /// A range that runs past the region is refused rather than reading
    /// or writing whatever follows it.
    #[test]
    fn a_range_past_the_region_is_refused() {
        let (_f, path) = fixture("dyn-bounds");
        let a = SharedArcDyn::create(&path, &[0; 4], 4, LastHolder::Keep).unwrap();
        let mut out = [0u8; 2];
        assert_eq!(a.read_at(3, &mut out).unwrap_err(), ArcError::LayoutMismatch);
        assert_eq!(a.write_at(3, &[1, 2]).unwrap_err(), ArcError::LayoutMismatch);
        assert_eq!(a.read_at(usize::MAX, &mut out).unwrap_err(), ArcError::LayoutMismatch);
        // The last byte is still reachable.
        a.write_at(3, &[5]).unwrap();
        assert_eq!(a.as_slice()[3], 5);
    }

    /// Under `Unlink` the backing goes when the last holder releases, so
    /// a later open finds nothing.
    #[test]
    fn the_last_dyn_holder_takes_the_backing_with_it() {
        let (_f, path) = fixture("dyn-unlink");
        {
            let a = SharedArcDyn::create(&path, &[1; 4], 4, LastHolder::Unlink).unwrap();
            let b = SharedArcDyn::open(&path, 4, 4, LastHolder::Unlink).unwrap();
            assert_eq!(a.strong_count(), 2);
            drop(a);
            assert!(path.exists(), "a holder remains, so the backing stands");
            drop(b);
        }
        assert!(!path.exists(), "the last holder removed the backing");
    }

    #[test]
    fn a_second_handle_shares_the_value_and_raises_the_count() {
        let (_f, path) = fixture("share");
        let a = SharedArc::create(&path, 42u64, 8, LastHolder::Keep).unwrap();
        assert_eq!(*a, 42);
        assert_eq!(a.strong_count(), 1);

        let b = SharedArc::<u64>::open(&path, 8, LastHolder::Keep).unwrap();
        assert_eq!(*b, 42, "the same value, not a copy of the default");
        assert_eq!(a.strong_count(), 2);
        assert_eq!(b.strong_count(), 2);

        drop(b);
        assert_eq!(a.strong_count(), 1, "releasing one holder is visible to the other");
    }

    /// `create` on a live backing attaches rather than overwriting, so
    /// racing creators reach one value.
    #[test]
    fn creating_over_a_live_value_attaches_instead_of_overwriting() {
        let (_f, path) = fixture("attach");
        let a = SharedArc::create(&path, 7u64, 8, LastHolder::Keep).unwrap();
        let b = SharedArc::create(&path, 99u64, 8, LastHolder::Keep).unwrap();
        assert_eq!(*b, 7, "the second create did not overwrite the first value");
        assert_eq!(a.strong_count(), 2);
    }

    #[test]
    fn a_struct_value_round_trips() {
        let (_f, path) = fixture("struct");
        let want = Config { workers: 12, budget: 1 << 40 };
        let a = SharedArc::create(&path, want, 4, LastHolder::Keep).unwrap();
        let b = SharedArc::<Config>::open(&path, 4, LastHolder::Keep).unwrap();
        assert_eq!(*b, want);
        drop(a);
        assert_eq!(*b, want, "the value outlives the handle that wrote it");
    }

    #[test]
    fn opening_with_a_different_value_type_is_refused() {
        let (_f, path) = fixture("mismatch");
        let _a = SharedArc::create(&path, 7u64, 4, LastHolder::Keep).unwrap();
        assert_eq!(
            SharedArc::<Config>::open(&path, 4, LastHolder::Keep).unwrap_err(),
            ArcError::LayoutMismatch
        );
        assert_eq!(
            SharedArc::<u64>::open(&path, 8, LastHolder::Keep).unwrap_err(),
            ArcError::LayoutMismatch,
            "and so is a different holder capacity"
        );
    }

    #[test]
    fn a_full_holder_table_refuses() {
        let (_f, path) = fixture("full");
        let a = SharedArc::create(&path, 1u64, 2, LastHolder::Keep).unwrap();
        let b = SharedArc::<u64>::open(&path, 2, LastHolder::Keep).unwrap();
        assert_eq!(
            SharedArc::<u64>::open(&path, 2, LastHolder::Keep).unwrap_err(),
            ArcError::HoldersExhausted
        );
        assert_eq!(a.strong_count(), 2, "the refusal left both holders alone");
        drop(b);
        SharedArc::<u64>::open(&path, 2, LastHolder::Keep)
            .expect("a released slot is reusable");
    }

    /// The reason the count is a table: a holder whose process is gone
    /// must not keep the value alive, and a number cannot be asked
    /// whether it is still running.
    #[test]
    fn a_holder_whose_process_died_stops_counting() {
        let (_f, path) = fixture("dead");
        let a = SharedArc::create(&path, 5u64, 4, LastHolder::Keep).unwrap();
        let ghost = a.holders().slot(1);
        ghost.state.store(HOLDER_PRESENT, Ordering::Release);
        ghost.owner_pid.store(u32::MAX - 1, Ordering::Release);
        assert_eq!(a.strong_count(), 2, "it counts until something asks");
        assert_eq!(a.reap_dead_holders(), 1);
        assert_eq!(a.strong_count(), 1, "and the live holder is untouched");
    }

    #[test]
    fn the_last_holder_unlinks_when_asked_to_and_not_otherwise() {
        let (_f, path) = fixture("unlink");
        let a = SharedArc::create(&path, 1u64, 4, LastHolder::Unlink).unwrap();
        let b = SharedArc::<u64>::open(&path, 4, LastHolder::Unlink).unwrap();
        drop(a);
        assert!(path.exists(), "a holder remains, so the backing stays");
        drop(b);
        assert!(!path.exists(), "the last holder removed it");

        let (_f2, keep) = fixture("keep");
        let c = SharedArc::create(&keep, 1u64, 4, LastHolder::Keep).unwrap();
        drop(c);
        assert!(keep.exists(), "Keep leaves the backing for a later process");
    }

    /// An AtomicU64 is Copy, so the value can be something every holder
    /// mutates through - which is how a shared counter is built without
    /// making the Arc itself mutable.
    #[test]
    fn an_atomic_value_is_shared_state_every_holder_can_bump() {
        let (_f, path) = fixture("counter");
        let a =
            SharedArc::create(&path, SharedCounter(AtomicU64::new(0)), 4, LastHolder::Keep)
                .unwrap();
        let b = SharedArc::<SharedCounter>::open(&path, 4, LastHolder::Keep).unwrap();
        a.add(10);
        b.add(5);
        assert_eq!(a.get_count(), 15, "both handles address one counter");
        assert_eq!(b.get_count(), 15);
    }

    impl SharedArc<SharedCounter> {
        fn get_count(&self) -> u64 {
            self.get().get()
        }
    }

    #[test]
    fn concurrent_handles_never_share_a_holder_slot() {
        let (_f, path) = fixture("concurrent");
        let root = SharedArc::create(&path, 1u64, 64, LastHolder::Keep).unwrap();
        std::thread::scope(|s| {
            for _ in 0..8 {
                let p = path.clone();
                s.spawn(move || {
                    let mut held = Vec::new();
                    for _ in 0..6 {
                        if let Ok(h) = SharedArc::<u64>::open(&p, 64, LastHolder::Keep) {
                            assert_eq!(*h, 1);
                            held.push(h);
                        }
                    }
                });
            }
        });
        assert_eq!(root.strong_count(), 1, "every thread's handles released");
    }
}
