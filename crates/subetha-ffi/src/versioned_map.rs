//! The shared versioned map through the C ABI: a B-tree of keys, each
//! carrying the epochs its value was current between, so a reader pinned
//! at an epoch scans what was current then while a writer moves on.
//!
//! Keys order as unsigned bytes, so a caller that wants numeric order
//! stores its integers big-endian. Key and value sizes come from the
//! caller at create time rather than being fixed by the ABI. Three classes
//! are named for callers with no reason to choose, and
//! `subetha_versioned_map_create_default` takes the middle one; those take
//! a map compiled at fixed sizes, and any other pair the runtime-sized
//! form. The choice follows the two sizes alone, so every process opening
//! one file with the same sizes lands on the same form.
//!
//! One entry per key, which decides two behaviors worth knowing before
//! use. Re-inserting a key whose tombstone a live pin can still reach is
//! refused with `SUBETHA_E_WOULD_BLOCK`, because there is nowhere to put
//! the new version without destroying a row a scan must still see. And an
//! update replaces rather than versioning, so a reader pinned across one
//! finds the key invisible: the old value is gone and the new one is born
//! after the pin. A remove is what leaves something a pin can still reach.
//!
//! One writer at a time changes the map, as for the B-tree beneath it:
//! `insert`, `remove`, `sweep` and `void_epoch` are serialized by the
//! caller against each other across every process, under a lock of the
//! caller's choosing, while `get` and `range` take no lock, the pin being
//! what keeps a reader's view. Two writers changing the shape at once
//! corrupt a node, which the boundary reports as a panic.

use std::ffi::c_char;
use std::ops::Bound;
use std::path::Path;

use subetha_cxc::raw_versioned_btree_map::RawVersionedBTreeMap;
use subetha_cxc::versioned_btree_map::{VersionedBTreeMap, VersionedError};

use crate::error::{
    fail, SUBETHA_E_BUFFER_TOO_SMALL, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_KEY_ABSENT,
    SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WOULD_BLOCK,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_VERSIONED_MAP};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// A key size for identifiers and small integers.
pub const SUBETHA_VERSIONED_MAP_KEY_BYTES_SMALL: usize = 8;
/// The key size `subetha_versioned_map_create_default` uses.
pub const SUBETHA_VERSIONED_MAP_KEY_BYTES_DEFAULT: usize = 16;
/// A key size for compound and textual keys.
pub const SUBETHA_VERSIONED_MAP_KEY_BYTES_LARGE: usize = 32;

/// The value size that pairs with the small key.
pub const SUBETHA_VERSIONED_MAP_VALUE_BYTES_SMALL: usize = 16;
/// The value size `subetha_versioned_map_create_default` uses.
pub const SUBETHA_VERSIONED_MAP_VALUE_BYTES_DEFAULT: usize = 64;
/// The value size that pairs with the large key.
pub const SUBETHA_VERSIONED_MAP_VALUE_BYTES_LARGE: usize = 256;

/// A fixed-size key or value for the compiled classes. `[u8; N]` orders as
/// unsigned bytes, which is the order the runtime-sized map uses, so the
/// two forms agree on what a range means.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct Fixed<const N: usize>([u8; N]);

impl<const N: usize> Default for Fixed<N> {
    fn default() -> Self {
        Self([0u8; N])
    }
}

impl<const N: usize> Fixed<N> {
    fn from_bytes(b: &[u8]) -> Self {
        let mut f = Self::default();
        f.0.copy_from_slice(b);
        f
    }
}

/// A snapshot of a shared versioned map.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_versioned_map_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Nodes the tree addresses.
    pub capacity: u64,
    /// Entries present, tombstones not yet reclaimed included.
    pub len: u64,
    /// Bytes a key takes.
    pub key_size: u64,
    /// Bytes a value takes.
    pub value_size: u64,
    /// The epoch the table stands at now.
    pub epoch: u64,
}

/// One entry a range yields: its key bytes and its value bytes.
type Entry = (Vec<u8>, Vec<u8>);

/// What the entry points call, whichever form is carrying the map.
trait Backing: Send + Sync {
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
    fn key_size(&self) -> usize;
    fn value_size(&self) -> usize;
    fn epoch_now(&self) -> u64;
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), VersionedError>;
    fn get(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError>;
    fn remove(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError>;
    fn range(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Entry>, VersionedError>;
    fn sweep(&self) -> Result<usize, VersionedError>;
    fn void_epoch(&self, epoch: u64) -> Result<usize, VersionedError>;
    fn pin_now(&self) -> Result<u64, VersionedError>;
    fn flush(&self) -> Result<(), VersionedError>;
}

impl Backing for RawVersionedBTreeMap {
    fn capacity(&self) -> usize {
        RawVersionedBTreeMap::capacity(self)
    }
    fn len(&self) -> usize {
        RawVersionedBTreeMap::len(self)
    }
    fn key_size(&self) -> usize {
        RawVersionedBTreeMap::key_size(self)
    }
    fn value_size(&self) -> usize {
        RawVersionedBTreeMap::value_size(self)
    }
    fn epoch_now(&self) -> u64 {
        self.epochs().now()
    }
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), VersionedError> {
        RawVersionedBTreeMap::insert(self, key, value)
    }
    fn get(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError> {
        RawVersionedBTreeMap::get(self, key, out)
    }
    fn remove(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError> {
        RawVersionedBTreeMap::remove(self, key, out)
    }
    fn range(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Entry>, VersionedError> {
        let guard = self.pin()?;
        let low = low.map_or(Bound::Unbounded, Bound::Included);
        let high = high.map_or(Bound::Unbounded, Bound::Included);
        Ok(RawVersionedBTreeMap::range_at(self, low, high, limit, &guard))
    }
    fn sweep(&self) -> Result<usize, VersionedError> {
        RawVersionedBTreeMap::sweep(self)
    }
    fn void_epoch(&self, epoch: u64) -> Result<usize, VersionedError> {
        RawVersionedBTreeMap::void_epoch(self, epoch)
    }
    fn pin_now(&self) -> Result<u64, VersionedError> {
        Ok(self.pin()?.epoch())
    }
    fn flush(&self) -> Result<(), VersionedError> {
        RawVersionedBTreeMap::flush(self)
    }
}

impl<const K: usize, const V: usize> Backing for VersionedBTreeMap<Fixed<K>, Fixed<V>> {
    fn capacity(&self) -> usize {
        VersionedBTreeMap::capacity(self)
    }
    fn len(&self) -> usize {
        VersionedBTreeMap::len(self)
    }
    fn key_size(&self) -> usize {
        K
    }
    fn value_size(&self) -> usize {
        V
    }
    fn epoch_now(&self) -> u64 {
        self.epochs().now()
    }
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), VersionedError> {
        VersionedBTreeMap::insert(self, Fixed::from_bytes(key), Fixed::from_bytes(value))
            .map(|_| ())
    }
    fn get(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError> {
        match VersionedBTreeMap::get(self, &Fixed::from_bytes(key)) {
            Some(v) => {
                out[..V].copy_from_slice(&v.0);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn remove(&self, key: &[u8], out: &mut [u8]) -> Result<bool, VersionedError> {
        match VersionedBTreeMap::remove(self, &Fixed::from_bytes(key))? {
            Some(v) => {
                out[..V].copy_from_slice(&v.0);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn range(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Entry>, VersionedError> {
        let guard = self.pin()?;
        let lo = low.map(Fixed::<K>::from_bytes);
        let hi = high.map(Fixed::<K>::from_bytes);
        let low = lo.as_ref().map_or(Bound::Unbounded, Bound::Included);
        let high = hi.as_ref().map_or(Bound::Unbounded, Bound::Included);
        Ok(VersionedBTreeMap::range_at(self, low, high, limit, &guard)
            .into_iter()
            .map(|(k, v)| (k.0.to_vec(), v.0.to_vec()))
            .collect())
    }
    fn sweep(&self) -> Result<usize, VersionedError> {
        VersionedBTreeMap::sweep(self)
    }
    fn void_epoch(&self, epoch: u64) -> Result<usize, VersionedError> {
        VersionedBTreeMap::void_epoch(self, epoch)
    }
    fn pin_now(&self) -> Result<u64, VersionedError> {
        Ok(self.pin()?.epoch())
    }
    fn flush(&self) -> Result<(), VersionedError> {
        VersionedBTreeMap::flush(self)
    }
}

pub(crate) struct VersionedMapObject {
    map: Box<dyn Backing>,
    mode: u32,
}

impl VersionedMapObject {
    /// A map parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn code_for(e: VersionedError) -> i32 {
    match e {
        VersionedError::RebornUnderPin => fail(
            SUBETHA_E_WOULD_BLOCK,
            "the key's tombstone is still reachable by a pin, so it cannot be re-inserted yet",
        ),
        VersionedError::Full => fail(
            SUBETHA_E_RING_IO,
            "the tree is full and no entry is dead enough to reclaim",
        ),
        VersionedError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the map on disk was built with other sizes",
        ),
        VersionedError::Epochs(e) => fail(SUBETHA_E_RING_IO, format!("epoch table: {e}")),
        VersionedError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_map(handle: subetha_handle, f: impl FnOnce(&VersionedMapObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_VERSIONED_MAP, |object| match object {
        Object::VersionedMap(m) => f(m),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a versioned map"),
    })
}

/// The map one set of sizes names, compiled at fixed sizes when they match
/// a class and runtime-sized otherwise. `attach` opens rather than creates.
fn build(
    attach: bool,
    tree_path: &Path,
    capacity: usize,
    key_size: usize,
    value_size: usize,
    epochs_path: &Path,
    max_pins: usize,
) -> Result<Box<dyn Backing>, VersionedError> {
    macro_rules! fixed {
        ($k:expr, $v:expr) => {{
            let map = if attach {
                VersionedBTreeMap::<Fixed<$k>, Fixed<$v>>::open(
                    tree_path, capacity, epochs_path, max_pins,
                )?
            } else {
                VersionedBTreeMap::<Fixed<$k>, Fixed<$v>>::create(
                    tree_path, capacity, epochs_path, max_pins,
                )?
            };
            Ok(Box::new(map) as Box<dyn Backing>)
        }};
    }
    match (key_size, value_size) {
        (SUBETHA_VERSIONED_MAP_KEY_BYTES_SMALL, SUBETHA_VERSIONED_MAP_VALUE_BYTES_SMALL) => {
            fixed!(8, 16)
        }
        (SUBETHA_VERSIONED_MAP_KEY_BYTES_DEFAULT, SUBETHA_VERSIONED_MAP_VALUE_BYTES_DEFAULT) => {
            fixed!(16, 64)
        }
        (SUBETHA_VERSIONED_MAP_KEY_BYTES_LARGE, SUBETHA_VERSIONED_MAP_VALUE_BYTES_LARGE) => {
            fixed!(32, 256)
        }
        _ => {
            let map = if attach {
                RawVersionedBTreeMap::open(
                    tree_path, capacity, key_size, value_size, epochs_path, max_pins,
                )?
            } else {
                RawVersionedBTreeMap::create(
                    tree_path, capacity, key_size, value_size, epochs_path, max_pins,
                )?
            };
            Ok(Box::new(map) as Box<dyn Backing>)
        }
    }
}

/// The arguments both constructors read, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `tree_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[allow(clippy::too_many_arguments)]
unsafe fn read_arguments<'a>(
    tree_path: *const c_char,
    capacity: u64,
    key_size: usize,
    value_size: usize,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, &'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let tree_path = Path::new(unsafe { text(tree_path, "tree_path") }?);
    if capacity == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "capacity is zero"));
    }
    if key_size == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "key_size is zero"));
    }
    if value_size == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "value_size is zero"));
    }
    let epochs_path = Path::new(unsafe { text(epochs_path, "epochs_path") }?);
    if max_pins == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "max_pins is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((tree_path, capacity as usize, epochs_path, max_pins as usize, mode))
}

/// Obtain the map at `tree_path` with `capacity` nodes for `key_size`-byte
/// keys and `value_size`-byte values, and its epoch table at `epochs_path`
/// holding `max_pins` pins. `epochs_path` may be the table the store's
/// other structures share.
///
/// # Safety
/// `tree_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_versioned_map_create(
    tree_path: *const c_char,
    capacity: u64,
    key_size: usize,
    value_size: usize,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (tree_path, capacity, epochs_path, max_pins, mode) = match unsafe {
            read_arguments(
                tree_path, capacity, key_size, value_size, epochs_path, max_pins, mode, out,
            )
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match build(false, tree_path, capacity, key_size, value_size, epochs_path, max_pins) {
            Ok(map) => unsafe {
                issue(Object::VersionedMap(VersionedMapObject { map, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// `subetha_versioned_map_create` at the default class.
///
/// # Safety
/// `tree_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_create_default(
    tree_path: *const c_char,
    capacity: u64,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    unsafe {
        subetha_versioned_map_create(
            tree_path,
            capacity,
            SUBETHA_VERSIONED_MAP_KEY_BYTES_DEFAULT,
            SUBETHA_VERSIONED_MAP_VALUE_BYTES_DEFAULT,
            epochs_path,
            max_pins,
            mode,
            out,
        )
    }
}

/// Attach to the map and epoch table another process created; both files
/// must exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `tree_path` and `epochs_path` are NUL-terminated UTF-8 strings; `out`
/// is a valid pointer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_versioned_map_open(
    tree_path: *const c_char,
    capacity: u64,
    key_size: usize,
    value_size: usize,
    epochs_path: *const c_char,
    max_pins: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (tree_path, capacity, epochs_path, max_pins, mode) = match unsafe {
            read_arguments(
                tree_path, capacity, key_size, value_size, epochs_path, max_pins, mode, out,
            )
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match build(true, tree_path, capacity, key_size, value_size, epochs_path, max_pins) {
            Ok(map) => unsafe {
                issue(Object::VersionedMap(VersionedMapObject { map, mode }), out)
            },
            Err(e) => code_for(e),
        }
    })
}

/// Make `value` current at `key` at a fresh epoch.
/// `SUBETHA_E_WOULD_BLOCK` when the key's tombstone is still reachable by
/// a pin.
///
/// # Safety
/// `key` points to `key_len` readable bytes and `value` to `value_len`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_insert(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    with_map(handle, |m| {
        if key_len != m.map.key_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key is {} bytes, not {key_len}", m.map.key_size()),
            );
        }
        if value_len != m.map.value_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a value is {} bytes, not {value_len}", m.map.value_size()),
            );
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        let value = match unsafe { bytes(value, value_len) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match m.map.insert(key, value) {
            Ok(()) => SUBETHA_OK,
            Err(e) => code_for(e),
        }
    })
}

/// The value current at `key` into `out`, and its length into `out_len`.
/// `SUBETHA_E_MAP_KEY_ABSENT` when the key is absent or a tombstone.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` points to `cap`
/// writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_get(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_map(handle, |m| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        if key_len != m.map.key_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key is {} bytes, not {key_len}", m.map.key_size()),
            );
        }
        let value_size = m.map.value_size();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = value_size };
        if out.is_null() || cap < value_size {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, value_size) };
        match m.map.get(key, buf) {
            Ok(true) => SUBETHA_OK,
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => code_for(e),
        }
    })
}

/// Stamp `key` as superseded at a fresh epoch, writing what was current
/// into `out`. The entry stays until no pin can reach it.
/// `SUBETHA_E_MAP_KEY_ABSENT` when the key was absent or already a
/// tombstone.
///
/// # Safety
/// `key` points to `key_len` readable bytes; `out` points to `cap`
/// writable bytes; `out_len` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_remove(
    handle: subetha_handle,
    key: *const u8,
    key_len: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    with_map(handle, |m| {
        if out_len.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len is null");
        }
        if key_len != m.map.key_size() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                format!("a key is {} bytes, not {key_len}", m.map.key_size()),
            );
        }
        let value_size = m.map.value_size();
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out_len = value_size };
        if out.is_null() || cap < value_size {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        let key = match unsafe { bytes(key, key_len) } {
            Ok(k) => k,
            Err(code) => return code,
        };
        // The caller guarantees `cap` writable bytes, and cap is enough here.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, value_size) };
        match m.map.remove(key, buf) {
            Ok(true) => SUBETHA_OK,
            Ok(false) => SUBETHA_E_MAP_KEY_ABSENT,
            Err(e) => code_for(e),
        }
    })
}

/// The entries current at one moment, in key order, at most `limit` of
/// them, packed into `out` as key bytes then value bytes per entry.
///
/// `low` and `high` are inclusive bounds, each null for unbounded, and
/// each of the map's key size when given. `out_count` receives the entries
/// written and `out_len` the bytes they take.
/// `SUBETHA_E_BUFFER_TOO_SMALL` leaves `out` untouched with the bytes
/// needed already reported.
///
/// The pin is taken and released inside the call, so the result is one
/// consistent moment. The limit counts entries examined, not entries
/// returned, so a range dense in tombstones can return fewer than `limit`
/// while more remain.
///
/// # Safety
/// `low` and `high` are null or point to `key_size` readable bytes; `out`
/// points to `cap` writable bytes; `out_len` and `out_count` are valid
/// pointers.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_versioned_map_range(
    handle: subetha_handle,
    low: *const u8,
    high: *const u8,
    limit: usize,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_count: *mut usize,
) -> i32 {
    with_map(handle, |m| {
        if out_len.is_null() || out_count.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out_len or out_count is null");
        }
        let key_size = m.map.key_size();
        let stride = key_size + m.map.value_size();
        // A bound is either absent or exactly a key; anything else would
        // silently name a different key than the caller meant.
        let low = if low.is_null() {
            None
        } else {
            match unsafe { bytes(low, key_size) } {
                Ok(b) => Some(b),
                Err(code) => return code,
            }
        };
        let high = if high.is_null() {
            None
        } else {
            match unsafe { bytes(high, key_size) } {
                Ok(b) => Some(b),
                Err(code) => return code,
            }
        };
        let found = match m.map.range(low, high, limit) {
            Ok(f) => f,
            Err(e) => return code_for(e),
        };
        let needed = found.len() * stride;
        // Checked non-null; the caller guarantees they are writable.
        unsafe {
            *out_len = needed;
            *out_count = found.len();
        }
        if out.is_null() || cap < needed {
            return SUBETHA_E_BUFFER_TOO_SMALL;
        }
        // The caller guarantees `cap` writable bytes, and cap >= needed.
        let buf = unsafe { std::slice::from_raw_parts_mut(out, needed) };
        for (i, (k, v)) in found.iter().enumerate() {
            let at = i * stride;
            buf[at..at + key_size].copy_from_slice(k);
            buf[at + key_size..at + stride].copy_from_slice(v);
        }
        SUBETHA_OK
    })
}

/// Drop every tombstone no pin can reach, reporting how many went through
/// `out_freed` when that is not null. `SUBETHA_E_RING_IO` when none could
/// go, which is what a full tree reports.
///
/// # Safety
/// `out_freed` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_sweep(
    handle: subetha_handle,
    out_freed: *mut u64,
) -> i32 {
    with_map(handle, |m| match m.map.sweep() {
        Ok(freed) => {
            if !out_freed.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_freed = freed as u64 };
            }
            SUBETHA_OK
        }
        Err(e) => code_for(e),
    })
}

/// Undo every stamp this map holds at `epoch`: an entry born there goes,
/// and one superseded there is current again. Reports the entries touched
/// through `out_touched` when that is not null.
///
/// # Safety
/// `out_touched` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_void_epoch(
    handle: subetha_handle,
    epoch: u64,
    out_touched: *mut u64,
) -> i32 {
    with_map(handle, |m| match m.map.void_epoch(epoch) {
        Ok(touched) => {
            if !out_touched.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_touched = touched as u64 };
            }
            SUBETHA_OK
        }
        Err(e) => code_for(e),
    })
}

/// The epoch a reader pinning now would see, into `out`. The pin is taken
/// and released inside the call; it names the moment rather than holding
/// it.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_pin_epoch(
    handle: subetha_handle,
    out: *mut u64,
) -> i32 {
    with_map(handle, |m| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        match m.map.pin_now() {
            Ok(epoch) => {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out = epoch };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Push the map's dirty pages to disk, returning when they are durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_versioned_map_flush(handle: subetha_handle) -> i32 {
    with_map(handle, |m| match m.map.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the map into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_versioned_map_read_stats(
    handle: subetha_handle,
    out: *mut subetha_versioned_map_stats,
) -> i32 {
    with_map(handle, |m| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_versioned_map_stats {
            mode: m.mode,
            capacity: m.map.capacity() as u64,
            len: m.map.len() as u64,
            key_size: m.map.key_size() as u64,
            value_size: m.map.value_size() as u64,
            epoch: m.map.epoch_now(),
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
