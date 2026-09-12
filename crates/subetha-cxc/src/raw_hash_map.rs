//! `RawHashMap` - the shared hash map at key and value sizes chosen at
//! run time, for attachers that know their entry as two byte counts
//! rather than two Rust types.
//!
//! It shares the region layout, the header, the slot and the protocol of
//! [`SharedHashMap`](crate::SharedHashMap): FNV-1a over the key bytes, a
//! linear probe, a state byte claimed by CAS, a SeqLock per slot and the
//! hash stored last as the publish. A raw handle and a typed handle on
//! the same file interoperate when `key_size` is `size_of::<K>()` and
//! `value_size` is `size_of::<V>()`, since the typed map stores exactly
//! the bytes of its `K` and `V`. Keys compare as bytes, which is what the
//! typed map's `Eq` means for a plain-data key.
//!
//! Every operation copies bytes: a key or value shorter than its size is
//! refused, never padded, because a padded key would be a different key.

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::Ordering;

use memmap2::{MmapMut, MmapOptions};

use crate::shared_hash_map::{
    fnv1a_64, map_file_size, InsertOutcome, MapError, MapHeader, MapSlot, HASH_UNSET, MAP_MAGIC,
    MAP_PAYLOAD_BYTES, SLOT_EMPTY, SLOT_OCCUPIED, SLOT_TOMBSTONE,
};

/// Where a probe left the key.
enum Placed {
    New,
    /// The key was present; its previous value was copied out.
    Existing,
}

enum Probe {
    Placed(Placed),
    Restart,
    Full,
}

pub struct RawHashMap {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    cap_mask: usize,
    cap_is_pow2: bool,
    key_size: usize,
    value_size: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for RawHashMap {}
unsafe impl Sync for RawHashMap {}

impl subetha_sidecar::AdaptiveInstance for RawHashMap {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl RawHashMap {
    fn check_sizes(key_size: usize, value_size: usize) -> Result<(), MapError> {
        if key_size == 0 || key_size + value_size > MAP_PAYLOAD_BYTES {
            return Err(MapError::PayloadTooLarge);
        }
        Ok(())
    }

    /// Obtain the map at `path` for `key_size`-byte keys and
    /// `value_size`-byte values, initializing an empty one if the path
    /// does not yet exist and attaching to it if it does. A region built
    /// with another capacity or other sizes is a `LayoutMismatch`.
    pub fn create(path: impl AsRef<Path>, capacity: usize, key_size: usize, value_size: usize) -> Result<Self, MapError> {
        Self::check_sizes(key_size, value_size)?;
        assert!(capacity >= 2);
        let total = map_file_size(capacity);
        let (file, mmap) = crate::mmf_attach::create_or_attach(
            path.as_ref(),
            total,
            |ptr| unsafe { Self::init_region(ptr, capacity, key_size, value_size) },
            |ptr| unsafe { (*(ptr as *const MapHeader)).magic == MAP_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, MapError::LayoutMismatch))?;
        Self::from_region(file, mmap, capacity, key_size, value_size)
    }

    /// Truncate the map at `path` and initialize an empty one, discarding
    /// whatever entries a live peer holds.
    pub fn reset(path: impl AsRef<Path>, capacity: usize, key_size: usize, value_size: usize) -> Result<Self, MapError> {
        Self::check_sizes(key_size, value_size)?;
        assert!(capacity >= 2);
        let total = map_file_size(capacity);
        let (file, mmap) = crate::mmf_attach::reset(path.as_ref(), total, |ptr| unsafe {
            Self::init_region(ptr, capacity, key_size, value_size)
        })?;
        Self::from_region(file, mmap, capacity, key_size, value_size)
    }

    /// Attach to an existing map; an absent file is an I/O error and a
    /// region of another shape a `LayoutMismatch`.
    pub fn open(path: impl AsRef<Path>, expected_capacity: usize, key_size: usize, value_size: usize) -> Result<Self, MapError> {
        Self::check_sizes(key_size, value_size)?;
        let total = map_file_size(expected_capacity);
        let file = crate::region_file::open_existing(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(MapError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        Self::from_region(file, mmap, expected_capacity, key_size, value_size)
    }

    /// Lay out an empty map: the layout fields first, magic last, because
    /// attachers spin on the magic.
    ///
    /// # Safety
    /// `ptr` addresses at least `map_file_size(capacity)` writable zeroed
    /// bytes.
    unsafe fn init_region(ptr: *mut u8, capacity: usize, key_size: usize, value_size: usize) {
        let hdr = ptr as *mut MapHeader;
        unsafe {
            (*hdr).capacity = capacity as u32;
            (*hdr).key_size = key_size as u32;
            (*hdr).value_size = value_size as u32;
            std::ptr::write_volatile(&raw mut (*hdr).magic, MAP_MAGIC);
        }
    }

    fn from_region(file: File, mmap: MmapMut, capacity: usize, key_size: usize, value_size: usize) -> Result<Self, MapError> {
        let hdr = unsafe { &*(mmap.as_ptr() as *const MapHeader) };
        if hdr.magic != MAP_MAGIC
            || hdr.capacity != capacity as u32
            || hdr.key_size != key_size as u32
            || hdr.value_size != value_size as u32
        {
            return Err(MapError::LayoutMismatch);
        }
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            cap_mask: capacity.wrapping_sub(1),
            cap_is_pow2: capacity.is_power_of_two(),
            key_size,
            value_size,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    fn wrap(&self, i: usize) -> usize {
        if self.cap_is_pow2 { i & self.cap_mask } else { i % self.capacity }
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

    #[inline]
    pub fn key_size(&self) -> usize { self.key_size }

    #[inline]
    pub fn value_size(&self) -> usize { self.value_size }

    #[inline]
    pub fn len(&self) -> usize {
        self.header().count.load(Ordering::Acquire) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    fn header(&self) -> &MapHeader {
        unsafe { &*(self.mmap.as_ptr() as *const MapHeader) }
    }

    fn slot(&self, idx: usize) -> &MapSlot {
        let base = unsafe { self.mmap.as_ptr().add(size_of::<MapHeader>()) };
        unsafe { &*(base.add(idx * size_of::<MapSlot>()) as *const MapSlot) }
    }

    /// FNV-1a over the key bytes, with 0 remapped to 1: 0 marks a claimed
    /// slot not yet published.
    fn hash_key(key: &[u8]) -> u64 {
        match fnv1a_64(key) {
            HASH_UNSET => 1,
            h => h,
        }
    }

    /// The key and value a caller passes, each exactly its size.
    fn check_entry(&self, key: &[u8], value: &[u8]) -> Result<(), MapError> {
        if key.len() != self.key_size || value.len() != self.value_size {
            return Err(MapError::PayloadTooLarge);
        }
        Ok(())
    }

    fn check_key(&self, key: &[u8]) -> Result<(), MapError> {
        if key.len() != self.key_size {
            return Err(MapError::PayloadTooLarge);
        }
        Ok(())
    }

    /// Take a slot's SeqLock for writing: CAS the version from even to
    /// odd, spinning while another writer holds it.
    #[inline]
    fn lock_slot(&self, slot: &MapSlot) -> u32 {
        loop {
            let v = slot.version.load(Ordering::Acquire);
            if v & 1 == 0
                && slot
                    .version
                    .compare_exchange_weak(v, v + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return v + 1;
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn unlock_slot(&self, slot: &MapSlot, held: u32) {
        slot.version.store(held + 1, Ordering::Release);
    }

    #[inline]
    fn payload_ptr(&self, slot_idx: usize) -> *mut u8 {
        unsafe {
            self.mmap
                .as_ptr()
                .add(size_of::<MapHeader>())
                .add(slot_idx * size_of::<MapSlot>())
                .add(std::mem::offset_of!(MapSlot, payload)) as *mut u8
        }
    }

    /// Copy a key and a value into a slot's payload; the caller holds the
    /// slot's SeqLock.
    unsafe fn copy_payload(&self, slot_idx: usize, key: &[u8], value: &[u8]) {
        let base = self.payload_ptr(slot_idx);
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), base, self.key_size);
            std::ptr::copy_nonoverlapping(value.as_ptr(), base.add(self.key_size), self.value_size);
        }
    }

    fn write_payload(&self, slot_idx: usize, key: &[u8], value: &[u8]) {
        let slot = self.slot(slot_idx);
        let held = self.lock_slot(slot);
        unsafe { self.copy_payload(slot_idx, key, value) };
        self.unlock_slot(slot, held);
    }

    fn publish_claimed(&self, slot_idx: usize, h: u64, key: &[u8], value: &[u8]) {
        self.write_payload(slot_idx, key, value);
        self.slot(slot_idx).hash.store(h, Ordering::Release);
    }

    /// A claimed slot's hash, waiting out a writer that has claimed the
    /// slot and not yet published it.
    #[inline]
    fn published_hash(&self, slot: &MapSlot) -> u64 {
        loop {
            let h = slot.hash.load(Ordering::Acquire);
            if h != HASH_UNSET {
                return h;
            }
            if slot.state.load(Ordering::Acquire) != SLOT_OCCUPIED {
                return HASH_UNSET;
            }
            std::hint::spin_loop();
        }
    }

    /// SeqLock-read a slot's payload into `key_out` and `value_out`, each
    /// at least its size long; the copy is retried while a writer holds
    /// the slot.
    fn read_payload(&self, slot_idx: usize, key_out: &mut [u8], value_out: &mut [u8]) {
        let slot = self.slot(slot_idx);
        loop {
            let v1 = slot.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            unsafe { self.copy_out_locked(slot_idx, key_out, value_out) };
            let v2 = slot.version.load(Ordering::Acquire);
            if v1 == v2 {
                return;
            }
        }
    }

    /// Copy a slot's payload out; the caller holds the slot's SeqLock or
    /// validates the version around the copy.
    unsafe fn copy_out_locked(&self, slot_idx: usize, key_out: &mut [u8], value_out: &mut [u8]) {
        let src = self.payload_ptr(slot_idx);
        unsafe {
            std::ptr::copy_nonoverlapping(src, key_out.as_mut_ptr(), self.key_size);
            std::ptr::copy_nonoverlapping(src.add(self.key_size), value_out.as_mut_ptr(), self.value_size);
        }
    }

    /// Insert or update. `Inserted` for a new key, `Updated` when an
    /// existing key's value was overwritten, `Err(Full)` when the probe
    /// found no slot, `Err(PayloadTooLarge)` when `key` or `value` is not
    /// exactly its size.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<InsertOutcome, MapError> {
        self.check_entry(key, value)?;
        let mut scratch = [0u8; MAP_PAYLOAD_BYTES];
        let r = self.place(key, value, true, &mut scratch);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_INSERT,
            if matches!(r, Err(MapError::Full)) { 1 } else { 0 },
        );
        match r? {
            Placed::New => Ok(InsertOutcome::Inserted),
            Placed::Existing => Ok(InsertOutcome::Updated),
        }
    }

    /// Insert `key` only if it is absent: `Ok(false)` when this call
    /// placed it, `Ok(true)` when a value was already present, copied into
    /// `existing_out` (at least `value_size` long), and nothing written.
    pub fn insert_if_absent(&self, key: &[u8], value: &[u8], existing_out: &mut [u8]) -> Result<bool, MapError> {
        self.check_entry(key, value)?;
        if existing_out.len() < self.value_size {
            return Err(MapError::PayloadTooLarge);
        }
        let r = self.place(key, value, false, existing_out);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_INSERT,
            if matches!(r, Err(MapError::Full)) { 1 } else { 0 },
        );
        match r? {
            Placed::New => Ok(false),
            Placed::Existing => Ok(true),
        }
    }

    fn place(&self, key: &[u8], value: &[u8], overwrite: bool, existing_out: &mut [u8]) -> Result<Placed, MapError> {
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        loop {
            match self.probe_once(h, start, key, value, overwrite, existing_out) {
                Probe::Placed(p) => return Ok(p),
                Probe::Restart => continue,
                Probe::Full => return Err(MapError::Full),
            }
        }
    }

    /// One pass of the linear probe from `start`, tracking the first
    /// tombstone so a probe that ends at an Empty claims the tombstone in
    /// preference to the Empty.
    fn probe_once(&self, h: u64, start: usize, key: &[u8], value: &[u8], overwrite: bool, existing_out: &mut [u8]) -> Probe {
        let mut first_tombstone: Option<usize> = None;
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                if let Some(tomb_idx) = first_tombstone {
                    return self.claim_tombstone_or_restart(tomb_idx, h, key, value);
                }
                if slot
                    .state
                    .compare_exchange(SLOT_EMPTY, SLOT_OCCUPIED, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.publish_claimed(idx, h, key, value);
                    self.header().count.fetch_add(1, Ordering::AcqRel);
                    return Probe::Placed(Placed::New);
                }
                let now = slot.state.load(Ordering::Acquire);
                if now == SLOT_OCCUPIED {
                    if self.present(idx, h, key, value, overwrite, existing_out) {
                        return Probe::Placed(Placed::Existing);
                    }
                } else if now == SLOT_TOMBSTONE && first_tombstone.is_none() {
                    first_tombstone = Some(idx);
                }
                continue;
            }
            if state == SLOT_OCCUPIED {
                if self.present(idx, h, key, value, overwrite, existing_out) {
                    return Probe::Placed(Placed::Existing);
                }
                continue;
            }
            if first_tombstone.is_none() {
                first_tombstone = Some(idx);
            }
        }
        match first_tombstone {
            Some(tomb_idx) => self.claim_tombstone_or_restart(tomb_idx, h, key, value),
            None => Probe::Full,
        }
    }

    /// Claim the tracked tombstone for the key, or restart the probe when
    /// another writer took it first.
    fn claim_tombstone_or_restart(&self, tomb_idx: usize, h: u64, key: &[u8], value: &[u8]) -> Probe {
        if self.try_claim_tombstone(tomb_idx, h, key, value) {
            Probe::Placed(Placed::New)
        } else {
            Probe::Restart
        }
    }

    /// Whether the occupied slot `idx` holds `key`; on a match its value is
    /// copied into `existing_out`, then replaced when `overwrite` is set.
    fn present(&self, idx: usize, h: u64, key: &[u8], value: &[u8], overwrite: bool, existing_out: &mut [u8]) -> bool {
        if self.published_hash(self.slot(idx)) != h {
            return false;
        }
        let mut found_key = [0u8; MAP_PAYLOAD_BYTES];
        self.read_payload(idx, &mut found_key[..self.key_size], existing_out);
        if found_key[..self.key_size] != *key {
            return false;
        }
        if overwrite {
            self.write_payload(idx, key, value);
        }
        true
    }

    /// Claim a tombstone slot for a new entry; false when another writer
    /// took it first.
    #[inline]
    fn try_claim_tombstone(&self, tomb_idx: usize, h: u64, key: &[u8], value: &[u8]) -> bool {
        let tomb_slot = self.slot(tomb_idx);
        let claimed = tomb_slot
            .state
            .compare_exchange(SLOT_TOMBSTONE, SLOT_OCCUPIED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if !claimed {
            return false;
        }
        tomb_slot.hash.store(HASH_UNSET, Ordering::Release);
        self.publish_claimed(tomb_idx, h, key, value);
        self.header().count.fetch_add(1, Ordering::AcqRel);
        loop {
            let cur = self.header().tombstones.load(Ordering::Acquire);
            if cur == 0 {
                break;
            }
            if self
                .header()
                .tombstones
                .compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        true
    }

    /// The slot holding `key`, or `None`.
    fn find(&self, key: &[u8]) -> Option<usize> {
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        let mut found_key = [0u8; MAP_PAYLOAD_BYTES];
        let mut found_value = [0u8; MAP_PAYLOAD_BYTES];
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return None;
            }
            if state == SLOT_OCCUPIED && self.published_hash(slot) == h {
                self.read_payload(idx, &mut found_key[..self.key_size], &mut found_value[..self.value_size]);
                if found_key[..self.key_size] == *key {
                    return Some(idx);
                }
            }
        }
        None
    }

    /// Look up `key`, copying its value into `value_out` (at least
    /// `value_size` long). `Ok(true)` when present, `Ok(false)` when absent.
    pub fn get(&self, key: &[u8], value_out: &mut [u8]) -> Result<bool, MapError> {
        self.check_key(key)?;
        if value_out.len() < self.value_size {
            return Err(MapError::PayloadTooLarge);
        }
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        let mut found_key = [0u8; MAP_PAYLOAD_BYTES];
        let mut found = false;
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                break;
            }
            if state == SLOT_OCCUPIED && self.published_hash(slot) == h {
                self.read_payload(idx, &mut found_key[..self.key_size], value_out);
                if found_key[..self.key_size] == *key {
                    found = true;
                    break;
                }
            }
        }
        self.ring_sidecar.push_op(crate::sidecar_ops::hash_map::OP_GET, if found { 0 } else { 2 });
        Ok(found)
    }

    /// Whether `key` is present.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, MapError> {
        self.check_key(key)?;
        let found = self.find(key).is_some();
        self.ring_sidecar.push_op(crate::sidecar_ops::hash_map::OP_CONTAINS, if found { 0 } else { 2 });
        Ok(found)
    }

    /// Remove `key`, copying the value it had into `value_out` (at least
    /// `value_size` long). `Ok(true)` when it was present.
    pub fn remove(&self, key: &[u8], value_out: &mut [u8]) -> Result<bool, MapError> {
        self.check_key(key)?;
        if value_out.len() < self.value_size {
            return Err(MapError::PayloadTooLarge);
        }
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        let mut found_key = [0u8; MAP_PAYLOAD_BYTES];
        let mut removed = false;
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                break;
            }
            if state == SLOT_OCCUPIED && self.published_hash(slot) == h {
                self.read_payload(idx, &mut found_key[..self.key_size], value_out);
                if found_key[..self.key_size] == *key {
                    let tombstoned = slot
                        .state
                        .compare_exchange(SLOT_OCCUPIED, SLOT_TOMBSTONE, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok();
                    if tombstoned {
                        slot.hash.store(HASH_UNSET, Ordering::Release);
                        self.header().count.fetch_sub(1, Ordering::AcqRel);
                        self.header().tombstones.fetch_add(1, Ordering::AcqRel);
                        removed = true;
                    }
                    break;
                }
            }
        }
        self.ring_sidecar.push_op(crate::sidecar_ops::hash_map::OP_REMOVE, if removed { 0 } else { 2 });
        Ok(removed)
    }

    /// Replace `key`'s value with `new` only if its current value is
    /// byte-for-byte `expected`: `Ok(true)` on the swap, `Ok(false)` when
    /// the value differed, with the current value copied into
    /// `current_out`, `Err(KeyAbsent)` when the key has no entry. The
    /// comparison and the write happen under the slot's SeqLock.
    pub fn compare_exchange(&self, key: &[u8], expected: &[u8], new: &[u8], current_out: &mut [u8]) -> Result<bool, MapError> {
        self.check_entry(key, new)?;
        if expected.len() != self.value_size || current_out.len() < self.value_size {
            return Err(MapError::PayloadTooLarge);
        }
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        let mut found_key = [0u8; MAP_PAYLOAD_BYTES];
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return Err(MapError::KeyAbsent);
            }
            if state != SLOT_OCCUPIED || self.published_hash(slot) != h {
                continue;
            }
            let held = self.lock_slot(slot);
            unsafe { self.copy_out_locked(idx, &mut found_key[..self.key_size], current_out) };
            if found_key[..self.key_size] != *key {
                self.unlock_slot(slot, held);
                continue;
            }
            if slot.state.load(Ordering::Acquire) != SLOT_OCCUPIED {
                self.unlock_slot(slot, held);
                return Err(MapError::KeyAbsent);
            }
            let swapped = current_out[..self.value_size] == *expected;
            if swapped {
                unsafe { self.copy_payload(idx, key, new) };
            }
            self.unlock_slot(slot, held);
            return Ok(swapped);
        }
        Err(MapError::KeyAbsent)
    }

    /// Mark every slot empty and zero both counters. Not safe against a
    /// concurrent insert or remove.
    pub fn clear(&self) {
        for i in 0..self.capacity {
            self.slot(i).state.store(SLOT_EMPTY, Ordering::Release);
        }
        self.header().count.store(0, Ordering::Release);
        self.header().tombstones.store(0, Ordering::Release);
        self.ring_sidecar.push_op(crate::sidecar_ops::hash_map::OP_CLEAR, 0);
    }

    #[inline]
    pub fn tombstone_count(&self) -> usize {
        self.header().tombstones.load(Ordering::Acquire) as usize
    }

    pub fn load_factor(&self) -> f64 {
        self.len() as f64 / self.capacity as f64
    }

    /// Reclaim tombstones by re-inserting every live entry into an
    /// emptied table. Returns the number of tombstones reclaimed. Not
    /// safe against a concurrent writer in any process.
    pub fn compact(&self) -> Result<usize, MapError> {
        self.ring_sidecar.push_op(crate::sidecar_ops::hash_map::OP_COMPACT, 0);
        let entry = self.key_size + self.value_size;
        let mut live: Vec<u8> = Vec::with_capacity(self.len() * entry);
        let mut reclaimed = 0usize;
        let mut key = [0u8; MAP_PAYLOAD_BYTES];
        let mut value = [0u8; MAP_PAYLOAD_BYTES];
        for i in 0..self.capacity {
            let s = self.slot(i).state.load(Ordering::Acquire);
            if s == SLOT_OCCUPIED {
                self.read_payload(i, &mut key[..self.key_size], &mut value[..self.value_size]);
                live.extend_from_slice(&key[..self.key_size]);
                live.extend_from_slice(&value[..self.value_size]);
            } else if s == SLOT_TOMBSTONE {
                reclaimed += 1;
            }
        }
        for i in 0..self.capacity {
            self.slot(i).state.store(SLOT_EMPTY, Ordering::Release);
        }
        self.header().count.store(0, Ordering::Release);
        self.header().tombstones.store(0, Ordering::Release);
        for pair in live.chunks_exact(entry) {
            self.insert(&pair[..self.key_size], &pair[self.key_size..])?;
        }
        Ok(reclaimed)
    }

    /// The next published entry at or after `cursor`, its key and value
    /// copied out; returns the slot index to continue from (one past the
    /// entry), or `None` once the table is walked. An entry is seen once
    /// its insert has published it: an insert claims its slot before it
    /// writes the payload, so a slot claimed and not yet published is
    /// passed over rather than handed back with the zeroed payload of an
    /// insert in flight. Under concurrent writers a walk sees the entries
    /// published before it reached their slots; a walker that must see
    /// every entry walks again once the writers have returned.
    pub fn next_entry(&self, cursor: usize, key_out: &mut [u8], value_out: &mut [u8]) -> Result<Option<usize>, MapError> {
        if key_out.len() < self.key_size || value_out.len() < self.value_size {
            return Err(MapError::PayloadTooLarge);
        }
        let mut i = cursor;
        while i < self.capacity {
            let slot = self.slot(i);
            if slot.state.load(Ordering::Acquire) == SLOT_OCCUPIED && slot.hash.load(Ordering::Acquire) != HASH_UNSET {
                self.read_payload(i, key_out, value_out);
                return Ok(Some(i + 1));
            }
            i += 1;
        }
        Ok(None)
    }

    /// Every entry, best effort under concurrent writers, as
    /// `(key, value)` byte vectors.
    pub fn snapshot(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::with_capacity(self.len());
        let mut key = vec![0u8; self.key_size];
        let mut value = vec![0u8; self.value_size];
        let mut cursor = 0;
        loop {
            match self.next_entry(cursor, &mut key, &mut value) {
                Ok(Some(next)) => {
                    out.push((key.clone(), value.clone()));
                    cursor = next;
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("subetha: raw map snapshot stopped: {e:?}");
                    break;
                }
            }
        }
        out
    }

    pub fn flush(&self) -> Result<(), MapError> {
        self.mmap.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SharedHashMap;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> crate::test_paths::TmpFile {
        crate::test_paths::TmpFile::new(format!("subetha-rawmap-{name}-{}.bin", std::process::id()))
    }

    #[test]
    fn insert_get_remove_at_runtime_sizes() {
        let p = tmp("basic");
        let m = RawHashMap::create(&p, 32, 8, 16).unwrap();
        assert_eq!(m.key_size(), 8);
        assert_eq!(m.value_size(), 16);
        let mut out = [0u8; 16];
        assert!(!m.get(&[1; 8], &mut out).unwrap());
        assert_eq!(m.insert(&[1; 8], &[10; 16]).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(&[2; 8], &[20; 16]).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(&[1; 8], &[11; 16]).unwrap(), InsertOutcome::Updated);
        assert_eq!(m.len(), 2);
        assert!(m.get(&[1; 8], &mut out).unwrap());
        assert_eq!(out, [11; 16]);
        assert!(m.contains_key(&[2; 8]).unwrap());
        assert!(!m.contains_key(&[3; 8]).unwrap());
        assert!(m.remove(&[2; 8], &mut out).unwrap());
        assert_eq!(out, [20; 16]);
        assert!(!m.remove(&[2; 8], &mut out).unwrap());
        assert_eq!(m.len(), 1);
        assert_eq!(m.tombstone_count(), 1);
        assert!(matches!(m.insert(&[1; 7], &[0; 16]), Err(MapError::PayloadTooLarge)), "a short key is refused");
        assert!(matches!(m.get(&[1; 8], &mut out[..8]), Err(MapError::PayloadTooLarge)), "a short buffer is refused");
    }

    #[test]
    fn insert_if_absent_and_compare_exchange() {
        let p = tmp("cas");
        let m = RawHashMap::create(&p, 16, 4, 4).unwrap();
        let mut existing = [0u8; 4];
        assert!(!m.insert_if_absent(&[7, 0, 0, 0], &[1, 0, 0, 0], &mut existing).unwrap());
        assert!(m.insert_if_absent(&[7, 0, 0, 0], &[2, 0, 0, 0], &mut existing).unwrap());
        assert_eq!(existing, [1, 0, 0, 0]);
        let mut current = [0u8; 4];
        assert!(matches!(m.compare_exchange(&[9, 0, 0, 0], &[0; 4], &[5; 4], &mut current), Err(MapError::KeyAbsent)));
        assert!(!m.compare_exchange(&[7, 0, 0, 0], &[9; 4], &[5; 4], &mut current).unwrap());
        assert_eq!(current, [1, 0, 0, 0]);
        assert!(m.compare_exchange(&[7, 0, 0, 0], &[1, 0, 0, 0], &[5; 4], &mut current).unwrap());
        let mut out = [0u8; 4];
        assert!(m.get(&[7, 0, 0, 0], &mut out).unwrap());
        assert_eq!(out, [5; 4]);
    }

    /// An insert claims its slot before it writes the payload and
    /// publishes the hash, so a walk that trusted the state alone would
    /// hand back the zeroed payload of an insert in flight. A claimed,
    /// unpublished slot is passed over; once published it is seen.
    #[test]
    fn a_walk_passes_over_a_slot_claimed_but_not_yet_published() {
        let p = tmp("claimed");
        let m = RawHashMap::create(&p, 8, 8, 8).unwrap();
        assert_eq!(m.insert(&[1; 8], &[10; 8]).unwrap(), InsertOutcome::Inserted);

        // A second insert held between its claim and its publish, as a
        // writer descheduled there leaves it.
        let idx = (0..m.capacity())
            .find(|&i| m.slot(i).state.load(Ordering::Acquire) == SLOT_EMPTY)
            .expect("a free slot");
        m.slot(idx)
            .state
            .compare_exchange(SLOT_EMPTY, SLOT_OCCUPIED, Ordering::AcqRel, Ordering::Acquire)
            .expect("the claim");

        let walk = |m: &RawHashMap| {
            let mut key = [0u8; 8];
            let mut value = [0u8; 8];
            let mut seen = Vec::new();
            let mut cursor = 0;
            while let Some(next) = m.next_entry(cursor, &mut key, &mut value).unwrap() {
                seen.push((key, value));
                cursor = next;
            }
            seen.sort();
            seen
        };
        assert_eq!(
            walk(&m),
            vec![([1; 8], [10; 8])],
            "a slot an insert has claimed and not yet written is not an entry"
        );

        m.publish_claimed(idx, RawHashMap::hash_key(&[2; 8]), &[2; 8], &[20; 8]);
        m.header().count.fetch_add(1, Ordering::AcqRel);
        assert_eq!(walk(&m), vec![([1; 8], [10; 8]), ([2; 8], [20; 8])], "published, it is seen");
    }

    #[test]
    fn full_compact_walk_and_clear() {
        let p = tmp("walk");
        let m = RawHashMap::create(&p, 8, 1, 1).unwrap();
        for k in 0..8u8 {
            assert_eq!(m.insert(&[k], &[k + 100]).unwrap(), InsertOutcome::Inserted);
        }
        assert!(matches!(m.insert(&[9], &[0]), Err(MapError::Full)));
        let mut out = [0u8];
        for k in 0..4u8 {
            assert!(m.remove(&[k], &mut out).unwrap());
        }
        assert_eq!(m.tombstone_count(), 4);
        assert_eq!(m.compact().unwrap(), 4);
        assert_eq!(m.tombstone_count(), 0);
        assert_eq!(m.len(), 4);
        let mut walked = m.snapshot();
        walked.sort();
        assert_eq!(walked, (4..8u8).map(|k| (vec![k], vec![k + 100])).collect::<Vec<_>>());
        let mut key = [0u8];
        let mut value = [0u8];
        let mut cursor = 0;
        let mut seen = 0;
        while let Some(next) = m.next_entry(cursor, &mut key, &mut value).unwrap() {
            assert_eq!(value[0], key[0] + 100);
            seen += 1;
            cursor = next;
        }
        assert_eq!(seen, 4);
        m.clear();
        assert!(m.is_empty());
        assert_eq!(m.load_factor(), 0.0);
    }

    /// A typed map over `(u32, u64)` and a raw map at 4 and 8 bytes
    /// address the same region and see each other's entries.
    #[test]
    fn a_typed_map_and_a_raw_map_share_a_region() {
        let p = tmp("typed");
        let typed: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        typed.insert(5, 500).unwrap();
        let raw = RawHashMap::open(&p, 32, 4, 8).unwrap();
        let mut out = [0u8; 8];
        assert!(raw.get(&5u32.to_ne_bytes(), &mut out).unwrap());
        assert_eq!(u64::from_ne_bytes(out), 500);
        raw.insert(&6u32.to_ne_bytes(), &600u64.to_ne_bytes()).unwrap();
        assert_eq!(typed.get(&6), Some(600));
        assert!(matches!(RawHashMap::open(&p, 32, 8, 4), Err(MapError::LayoutMismatch)));
        assert!(matches!(RawHashMap::open(&p, 16, 4, 8), Err(MapError::LayoutMismatch)));
    }

    #[test]
    fn concurrent_writers_land_every_key_once() {
        let p = tmp("threads");
        let m = Arc::new(RawHashMap::create(&p, 4096, 8, 8).unwrap());
        let writers: Vec<_> = (0..4u64)
            .map(|t| {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    for i in 0..500u64 {
                        let key = ((t << 32) | i).to_ne_bytes();
                        assert_eq!(m.insert(&key, &i.to_ne_bytes()).unwrap(), InsertOutcome::Inserted);
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        assert_eq!(m.len(), 2000);
        let mut out = [0u8; 8];
        for t in 0..4u64 {
            for i in 0..500u64 {
                assert!(m.get(&((t << 32) | i).to_ne_bytes(), &mut out).unwrap());
                assert_eq!(u64::from_ne_bytes(out), i);
            }
        }
    }
}
