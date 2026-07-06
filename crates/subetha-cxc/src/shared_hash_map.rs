//! `SharedHashMap<K, V>` - cross-process open-addressed hash map
//! backed by a single MMF file.
//!
//! # Why open addressing?
//!
//! All storage is inline. No allocator, no pointer indirection. Each
//! slot lives in its own cache line; the entire table is a flat
//! array in the MMF. Robin Hood, linear, and quadratic probing all
//! work; we use **linear probing** because it's the most cache-
//! friendly on modern CPUs (sequential access dominates probe-
//! variance on speculative-prefetch architectures).
//!
//! # Stable hashing
//!
//! `std::hash::BuildHasher` uses a per-process random seed for DoS
//! resistance, which would make keys irreproducible across
//! processes. We use **FNV-1a** over the key bytes - fast, deps-free,
//! and deterministic across processes / runs / OSes.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+
//! | MapHeader (64B)           |
//! |   magic, capacity, count  |
//! |   key_size, value_size    |
//! +---------------------------+
//! | Slot[0]  (64B cache line) |
//! |   state (EMPTY/OCC/TS)    |
//! |   version (SeqLock)       |
//! |   hash (cached)           |
//! |   payload [u8; 48]: K + V |
//! | Slot[1] ...               |
//! +---------------------------+
//! ```
//!
//! # Protocol
//!
//! ## Insert
//! 1. Hash key (FNV-1a).
//! 2. Probe from `hash % capacity`, linearly.
//! 3. At each slot:
//!    - **Empty**: CAS state Empty → Occupied. On success, SeqLock-
//!      write `(K, V)` and store hash; bump `count`. Return Inserted.
//!    - **Occupied & hash matches & key matches**: SeqLock-update V
//!      (state unchanged). Return Updated.
//!    - **Occupied & no match**: probe next slot.
//!    - **Tombstone**: skip (linear probe continues; tombstones do
//!      NOT terminate insert because we want to overwrite them
//!      preferentially - track first tombstone and use it if no
//!      Empty is found earlier than a definitive "not present"
//!      conclusion).
//!
//! Actually the simpler insert: probe until first Empty (insert
//! there) OR find key (update). The tombstone-reuse optimisation
//! costs an extra bookkeeping pass; we skip it and reclaim
//! tombstones via the `compact()` method (single-writer in-place
//! rebuild) instead.
//!
//! ## Get
//! 1. Hash key, probe linearly.
//! 2. **Empty**: key absent (probe always terminates at Empty).
//! 3. **Occupied & hash matches**: SeqLock-read; if K matches, return V.
//! 4. **Tombstone or hash mismatch**: continue probing.
//!
//! ## Remove
//! 1. Find key (same probe).
//! 2. CAS state Occupied → Tombstone. `count.fetch_sub(1)`.
//!
//! # Concurrency
//!
//! All slot writes are SeqLock-protected so readers never observe
//! torn key+value. The state byte's CAS is the serialisation point
//! for who "owns" a slot for write. Two writers racing on the same
//! key both reach the same slot; one wins the Empty→Occupied CAS
//! and writes; the loser falls through to "Occupied + key matches"
//! and updates instead.
//!
//! # Capacity and load factor
//!
//! Fixed at create time. Recommend `capacity = 2 * expected_max`
//! to keep load factor below 0.5; linear probing degrades sharply
//! above 0.7. `insert` returns `MapError::Full` when the probe
//! chain saturates.
//!
//! # If you need dynamic sizing, use `SharedUniversal`
//!
//! `SharedHashMap` deliberately does NOT implement resize-on-grow.
//! Cross-process resize requires the same reader-coordination
//! machinery as MMF-backed migration (atomic file rename, reader
//! re-open signaling). Rather than reinvent that machinery inside
//! `SharedHashMap`, callers who need a dynamically-resizing hash
//! map should use [`crate::shared_universal::SharedUniversal<T>`]
//! configured with hash-map-only backings. The migration mechanism
//! handles cross-process resize correctly, with the reader-side
//! generation-bump protocol that makes wrap-around safe.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const MAP_MAGIC: u32 = 0x4150_484D;
pub const MAP_PAYLOAD_BYTES: usize = 48;

pub const SLOT_EMPTY: u8 = 0;
pub const SLOT_OCCUPIED: u8 = 1;
pub const SLOT_TOMBSTONE: u8 = 2;

#[repr(C, align(64))]
pub struct MapHeader {
    pub magic: u32,
    pub capacity: u32,
    pub count: AtomicU64,
    pub key_size: u32,
    pub value_size: u32,
    /// Monotonic counter of tombstones currently in the table.
    /// Bumped by `remove`, zeroed by `compact`. Used by callers
    /// (e.g. `SharedLRUCache`) to decide when to compact.
    pub tombstones: AtomicU64,
    _pad: [u8; 32],
}

#[repr(C, align(64))]
pub struct MapSlot {
    pub state: AtomicU8,
    _pad1: [u8; 3],
    pub version: AtomicU32,
    pub hash: AtomicU64,
    pub payload: [u8; MAP_PAYLOAD_BYTES],
}

const _: () = {
    assert!(size_of::<MapHeader>() == 64);
    assert!(size_of::<MapSlot>() == 64);
};

pub const fn map_file_size(capacity: usize) -> usize {
    size_of::<MapHeader>() + capacity * size_of::<MapSlot>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapError {
    Full,
    PayloadTooLarge,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for MapError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Updated,
}

/// FNV-1a 64-bit over a byte slice. Deterministic across processes
/// (unlike `std::hash::BuildHasher` which uses per-process random
/// seeds for DoS resistance).
#[inline]
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

pub struct SharedHashMap<K: Copy + Eq + 'static, V: Copy + 'static> {
    _file: File,
    mmap: MmapMut,
    capacity: usize,
    /// `capacity - 1`, valid only when `cap_is_pow2`.
    cap_mask: usize,
    /// True when `capacity` is a power of two, so slot reduction can use
    /// `& cap_mask` instead of a `% capacity` hardware DIV. The probe
    /// loop reduces twice per step (start + each probe), so this removes
    /// the DIV from the hash-map hot path when capacity is pow2.
    cap_is_pow2: bool,
    _phantom: PhantomData<(K, V)>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<K: Copy + Eq + Send + 'static, V: Copy + Send + 'static> Send for SharedHashMap<K, V> {}
unsafe impl<K: Copy + Eq + Sync + 'static, V: Copy + Sync + 'static> Sync for SharedHashMap<K, V> {}

impl<K: Copy + Eq + Send + Sync + 'static, V: Copy + Send + Sync + 'static>
    subetha_sidecar::AdaptiveInstance for SharedHashMap<K, V>
{
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<K: Copy + Eq + 'static, V: Copy + 'static> SharedHashMap<K, V> {
    fn check_layout() -> Result<(), MapError> {
        if size_of::<K>() + size_of::<V>() > MAP_PAYLOAD_BYTES {
            return Err(MapError::PayloadTooLarge);
        }
        Ok(())
    }

    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, MapError> {
        Self::check_layout()?;
        assert!(capacity >= 2);
        let total = map_file_size(capacity);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr_ptr = mmap.as_mut_ptr() as *mut MapHeader;
        unsafe {
            std::ptr::write_bytes(hdr_ptr as *mut u8, 0, size_of::<MapHeader>());
            (*hdr_ptr).magic = MAP_MAGIC;
            (*hdr_ptr).capacity = capacity as u32;
            (*hdr_ptr).key_size = size_of::<K>() as u32;
            (*hdr_ptr).value_size = size_of::<V>() as u32;
            // count and tombstones are AtomicU64; write_bytes zeroed
            // the storage, which is the valid representation of 0.
        }
        for i in 0..capacity {
            let slot_ptr = unsafe {
                mmap.as_mut_ptr()
                    .add(size_of::<MapHeader>())
                    .add(i * size_of::<MapSlot>())
            } as *mut MapSlot;
            unsafe {
                std::ptr::write(slot_ptr, MapSlot {
                    state: AtomicU8::new(SLOT_EMPTY),
                    _pad1: [0; 3],
                    version: AtomicU32::new(0),
                    hash: AtomicU64::new(0),
                    payload: [0u8; MAP_PAYLOAD_BYTES],
                });
            }
        }
        Ok(Self {
            _file: file, mmap, capacity,
            cap_mask: capacity.wrapping_sub(1),
            cap_is_pow2: capacity.is_power_of_two(),
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>, expected_capacity: usize) -> Result<Self, MapError> {
        Self::check_layout()?;
        let total = map_file_size(expected_capacity);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(MapError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const MapHeader) };
        if hdr.magic != MAP_MAGIC
            || hdr.capacity != expected_capacity as u32
            || hdr.key_size != size_of::<K>() as u32
            || hdr.value_size != size_of::<V>() as u32
        {
            return Err(MapError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, capacity: expected_capacity,
            cap_mask: expected_capacity.wrapping_sub(1),
            cap_is_pow2: expected_capacity.is_power_of_two(),
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Reduce an index into `[0, capacity)`. Uses `& cap_mask` when
    /// capacity is a power of two (the common case) - removing the
    /// `% capacity` hardware DIV the linear-probe loop would otherwise
    /// run on every step - and falls back to the modulo otherwise.
    /// A bit-mask is required (not Lemire-style multiply-shift) because
    /// linear probing needs consecutive indices to map to consecutive
    /// slots with wraparound.
    #[inline]
    fn wrap(&self, i: usize) -> usize {
        if self.cap_is_pow2 { i & self.cap_mask } else { i % self.capacity }
    }

    #[inline]
    pub fn capacity(&self) -> usize { self.capacity }

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

    fn hash_key(k: &K) -> u64 {
        let bytes = unsafe {
            std::slice::from_raw_parts(k as *const K as *const u8, size_of::<K>())
        };
        fnv1a_64(bytes)
    }

    /// SeqLock-write a (K, V) pair into a slot's payload region.
    fn write_payload(&self, slot_idx: usize, k: &K, v: &V) {
        let slot = self.slot(slot_idx);
        slot.version.fetch_add(1, Ordering::AcqRel); // odd
        let base = unsafe {
            self.mmap.as_ptr()
                .add(size_of::<MapHeader>())
                .add(slot_idx * size_of::<MapSlot>())
                .add(std::mem::offset_of!(MapSlot, payload))
                as *mut u8
        };
        unsafe {
            // Layout: key bytes then value bytes.
            std::ptr::copy_nonoverlapping(
                k as *const K as *const u8, base, size_of::<K>(),
            );
            std::ptr::copy_nonoverlapping(
                v as *const V as *const u8,
                base.add(size_of::<K>()),
                size_of::<V>(),
            );
        }
        slot.version.fetch_add(1, Ordering::AcqRel); // even
    }

    /// SeqLock-read a (K, V) pair from a slot. Spins on odd version.
    fn read_payload(&self, slot_idx: usize) -> (K, V) {
        let slot = self.slot(slot_idx);
        loop {
            let v1 = slot.version.load(Ordering::Acquire);
            if v1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let mut k = std::mem::MaybeUninit::<K>::uninit();
            let mut v = std::mem::MaybeUninit::<V>::uninit();
            let src = unsafe {
                self.mmap.as_ptr()
                    .add(size_of::<MapHeader>())
                    .add(slot_idx * size_of::<MapSlot>())
                    .add(std::mem::offset_of!(MapSlot, payload))
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src, k.as_mut_ptr() as *mut u8, size_of::<K>(),
                );
                std::ptr::copy_nonoverlapping(
                    src.add(size_of::<K>()),
                    v.as_mut_ptr() as *mut u8,
                    size_of::<V>(),
                );
            }
            let v2 = slot.version.load(Ordering::Acquire);
            if v1 == v2 {
                return unsafe { (k.assume_init(), v.assume_init()) };
            }
        }
    }

    /// Insert or update. Returns `Inserted` for a new key,
    /// `Updated` when an existing key's value was overwritten,
    /// `Err(Full)` if the table has no slot for the key (probed
    /// every slot without finding Empty, a key match, or a
    /// reclaimable tombstone).
    ///
    /// # Tombstone reuse
    ///
    /// Insert tracks the FIRST tombstone seen during the probe.
    /// If the probe terminates at an Empty (key absent) AND a
    /// tombstone was seen, the tombstone slot is reclaimed instead
    /// of consuming the Empty. This eliminates the need for an
    /// explicit `compact()` call in steady-state insert/remove
    /// workloads. `compact()` is still useful for bulk reclamation
    /// in workloads that don't naturally trigger reuse (e.g. a
    /// long insert-only period after heavy removes).
    pub fn insert(&self, key: K, value: V) -> Result<InsertOutcome, MapError> {
        let r = self.insert_inner(key, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_INSERT,
            if matches!(r, Err(MapError::Full)) { 1 } else { 0 },
        );
        r
    }

    fn insert_inner(&self, key: K, value: V) -> Result<InsertOutcome, MapError> {
        let h = Self::hash_key(&key);
        let start = self.wrap(h as usize);
        // Track the first tombstone seen during the probe. If the
        // probe terminates at an Empty without finding the key, we
        // claim this tombstone slot rather than the Empty.
        let mut first_tombstone: Option<usize> = None;
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                // End of probe chain. Key is not present. Either
                // claim the tracked tombstone (reuse path) or this
                // Empty slot.
                if let Some(tomb_idx) = first_tombstone
                    && self.try_claim_tombstone(tomb_idx, h, &key, &value) {
                        return Ok(InsertOutcome::Inserted);
                    }
                    // Tombstone was stolen by another writer; fall
                    // through to claim the Empty slot.
                if slot.state.compare_exchange(
                    SLOT_EMPTY, SLOT_OCCUPIED,
                    Ordering::AcqRel, Ordering::Acquire,
                ).is_ok() {
                    slot.hash.store(h, Ordering::Release);
                    self.write_payload(idx, &key, &value);
                    self.header().count.fetch_add(1, Ordering::AcqRel);
                    return Ok(InsertOutcome::Inserted);
                }
                // CAS on the Empty slot lost; the slot transitioned
                // to Occupied or Tombstone in between. Reread.
                let now = slot.state.load(Ordering::Acquire);
                if now == SLOT_OCCUPIED {
                    let cached = slot.hash.load(Ordering::Acquire);
                    if cached == h {
                        let (k, _) = self.read_payload(idx);
                        if k == key {
                            self.write_payload(idx, &key, &value);
                            return Ok(InsertOutcome::Updated);
                        }
                    }
                } else if now == SLOT_TOMBSTONE && first_tombstone.is_none() {
                    first_tombstone = Some(idx);
                }
                continue;
            }
            if state == SLOT_OCCUPIED {
                let cached = slot.hash.load(Ordering::Acquire);
                if cached == h {
                    let (k, _) = self.read_payload(idx);
                    if k == key {
                        self.write_payload(idx, &key, &value);
                        return Ok(InsertOutcome::Updated);
                    }
                }
                continue;
            }
            // SLOT_TOMBSTONE: track first one, keep probing
            // (subsequent slots may hold the key).
            if first_tombstone.is_none() {
                first_tombstone = Some(idx);
            }
        }
        // Walked every slot. Found no key, no Empty. If we saw a
        // tombstone, try to reuse it; otherwise truly Full.
        if let Some(tomb_idx) = first_tombstone
            && self.try_claim_tombstone(tomb_idx, h, &key, &value) {
                return Ok(InsertOutcome::Inserted);
            }
        Err(MapError::Full)
    }

    /// Claim a tombstone slot for a new insert. Returns true on
    /// success, false if another writer stole the slot via CAS.
    ///
    /// On success: writes hash + payload, increments live count,
    /// decrements tombstone counter via defensive CAS-loop (the
    /// counter cannot underflow under the single-writer contract,
    /// but the loop tolerates concurrent races defensively).
    #[inline]
    fn try_claim_tombstone(&self, tomb_idx: usize, h: u64, key: &K, value: &V) -> bool {
        let tomb_slot = self.slot(tomb_idx);
        if tomb_slot.state.compare_exchange(
            SLOT_TOMBSTONE, SLOT_OCCUPIED,
            Ordering::AcqRel, Ordering::Acquire,
        ).is_err() {
            return false;
        }
        tomb_slot.hash.store(h, Ordering::Release);
        self.write_payload(tomb_idx, key, value);
        self.header().count.fetch_add(1, Ordering::AcqRel);
        // Defensive saturating decrement: bounded retry loop that
        // never underflows past zero. Under the single-writer
        // contract the counter is structurally > 0 here (we just
        // converted a tombstone slot), but the loop tolerates any
        // race that violates that assumption.
        loop {
            let cur = self.header().tombstones.load(Ordering::Acquire);
            if cur == 0 { break; }
            if self.header().tombstones.compare_exchange(
                cur, cur - 1, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                break;
            }
        }
        true
    }

    /// Look up a key. Returns `None` if absent.
    pub fn get(&self, key: &K) -> Option<V> {
        let r = self.get_inner(key);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Internal lookup; no sidecar observation. Used by `get`,
    /// `contains_key`, and `remove` so each public entry point
    /// pushes its own semantic op_kind without double-counting.
    fn get_inner(&self, key: &K) -> Option<V> {
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY {
                return None;
            }
            if state == SLOT_OCCUPIED {
                let cached = slot.hash.load(Ordering::Acquire);
                if cached == h {
                    let (k, v) = self.read_payload(idx);
                    if k == *key {
                        return Some(v);
                    }
                }
            }
            // Tombstone or mismatch: continue probing.
        }
        None
    }

    /// True if `key` is present.
    pub fn contains_key(&self, key: &K) -> bool {
        let r = self.get_inner(key);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_CONTAINS,
            if r.is_none() { 2 } else { 0 },
        );
        r.is_some()
    }

    /// Remove a key. Returns the value if present.
    pub fn remove(&self, key: &K) -> Option<V> {
        let r = self.remove_inner(key);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::hash_map::OP_REMOVE,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    fn remove_inner(&self, key: &K) -> Option<V> {
        let h = Self::hash_key(key);
        let start = self.wrap(h as usize);
        for i in 0..self.capacity {
            let idx = self.wrap(start + i);
            let slot = self.slot(idx);
            let state = slot.state.load(Ordering::Acquire);
            if state == SLOT_EMPTY { return None; }
            if state == SLOT_OCCUPIED {
                let cached = slot.hash.load(Ordering::Acquire);
                if cached == h {
                    let (k, v) = self.read_payload(idx);
                    if k == *key {
                        if slot.state.compare_exchange(
                            SLOT_OCCUPIED, SLOT_TOMBSTONE,
                            Ordering::AcqRel, Ordering::Acquire,
                        ).is_ok() {
                            self.header().count.fetch_sub(1, Ordering::AcqRel);
                            self.header().tombstones.fetch_add(1, Ordering::AcqRel);
                            return Some(v);
                        }
                        // Another remover won; key is gone.
                        return None;
                    }
                }
            }
        }
        None
    }

    /// Clear the entire map. Marks every slot Empty and resets both
    /// the live count and the tombstone counter to 0. Not
    /// concurrency-safe vs concurrent insert/remove - callers should
    /// ensure no other writers are active when calling this.
    pub fn clear(&self) {
        for i in 0..self.capacity {
            let slot = self.slot(i);
            slot.state.store(SLOT_EMPTY, Ordering::Release);
        }
        self.header().count.store(0, Ordering::Release);
        self.header().tombstones.store(0, Ordering::Release);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::hash_map::OP_CLEAR, 0);
    }

    /// Current tombstone count (slots marked dead by `remove` that
    /// have not yet been reclaimed by `compact`).
    #[inline]
    pub fn tombstone_count(&self) -> usize {
        self.header().tombstones.load(Ordering::Acquire) as usize
    }

    /// Heuristic: returns `true` if tombstones occupy at least
    /// `threshold_fraction` of capacity. Callers typically pass
    /// `0.30` (30 %) - past that, linear-probe chains stretch out
    /// and lookup/insert latency degrades sharply. Cheap O(1).
    pub fn should_compact(&self, threshold_fraction: f64) -> bool {
        debug_assert!(
            (0.0..=1.0).contains(&threshold_fraction),
            "threshold_fraction must be in [0, 1]; got {threshold_fraction}",
        );
        let tombs = self.tombstone_count() as f64;
        tombs / self.capacity as f64 >= threshold_fraction
    }

    /// Reclaim tombstones via in-place rebuild. Returns the number
    /// of slots reclaimed.
    ///
    /// # What it does
    ///
    /// Snapshots every Occupied slot into a `Vec<(K, V)>`, resets
    /// every slot to Empty (zeroing both counters), then re-inserts
    /// each snapshotted pair via the normal probe. Since no
    /// tombstones remain, every key lands as close to its ideal
    /// slot as the live keys permit - probe chains shrink back to
    /// the no-deletion baseline.
    ///
    /// # Concurrency
    ///
    /// **NOT concurrency-safe with `insert` / `remove`.** The caller
    /// MUST guarantee no other writer (in any process holding an
    /// MMF handle to the same file) is mutating the map during
    /// `compact`. Readers calling `get` will see a transient empty
    /// state mid-rebuild and may return spurious `None` for keys
    /// that are about to be re-inserted; if that is unacceptable,
    /// serialise readers too.
    ///
    /// # Cost
    ///
    /// O(capacity) for the snapshot + reset, O(live_count *
    /// avg_probe) for re-insert. Allocates a temporary `Vec<(K, V)>`
    /// sized to the live count. For a 1 M-slot map at 50 % load,
    /// expect ~tens of milliseconds.
    pub fn compact(&self) -> Result<usize, MapError> {
        self.ring_sidecar
            .push_op(crate::sidecar_ops::hash_map::OP_COMPACT, 0);
        let mut live: Vec<(K, V)> = Vec::with_capacity(self.len());
        let mut reclaimed = 0usize;
        for i in 0..self.capacity {
            let slot = self.slot(i);
            let s = slot.state.load(Ordering::Acquire);
            if s == SLOT_OCCUPIED {
                live.push(self.read_payload(i));
            } else if s == SLOT_TOMBSTONE {
                reclaimed += 1;
            }
        }
        for i in 0..self.capacity {
            let slot = self.slot(i);
            slot.state.store(SLOT_EMPTY, Ordering::Release);
        }
        self.header().count.store(0, Ordering::Release);
        self.header().tombstones.store(0, Ordering::Release);
        for (k, v) in live {
            // Re-insert under single-writer contract: cannot race,
            // and `Full` is impossible because the live set fit in
            // the table before compaction.
            self.insert(k, v)?;
        }
        Ok(reclaimed)
    }

    /// Walk and collect all (K, V) pairs currently present. Best-
    /// effort snapshot under concurrent writers.
    pub fn snapshot(&self) -> Vec<(K, V)> {
        let mut out = Vec::with_capacity(self.len());
        for i in 0..self.capacity {
            let slot = self.slot(i);
            if slot.state.load(Ordering::Acquire) == SLOT_OCCUPIED {
                out.push(self.read_payload(i));
            }
        }
        out
    }

    /// Current load factor (count / capacity).
    pub fn load_factor(&self) -> f64 {
        self.len() as f64 / self.capacity as f64
    }

    pub fn flush(&self) -> Result<(), MapError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), MapError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-hashmap-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_empty() {
        let p = tmp("init");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 32).unwrap();
        assert_eq!(m.capacity(), 32);
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
        assert_eq!(m.get(&42), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn insert_and_get_round_trip() {
        let p = tmp("rt");
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        assert_eq!(m.insert(1, 100).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(2, 200).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(3, 300).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.len(), 3);
        assert_eq!(m.get(&1), Some(100));
        assert_eq!(m.get(&2), Some(200));
        assert_eq!(m.get(&3), Some(300));
        assert_eq!(m.get(&999), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn duplicate_insert_updates_value() {
        let p = tmp("dup");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.insert(7, 100).unwrap(), InsertOutcome::Inserted);
        assert_eq!(m.insert(7, 200).unwrap(), InsertOutcome::Updated);
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(&7), Some(200));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn remove_returns_value_and_decrements_count() {
        let p = tmp("rm");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        assert_eq!(m.remove(&1), Some(10));
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(&1), None);
        assert_eq!(m.get(&2), Some(20));
        assert_eq!(m.remove(&999), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn tombstone_does_not_break_probing() {
        let p = tmp("tomb");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 8).unwrap();
        // Force collisions by using keys that hash close together.
        // Insert several keys, remove the middle one, verify later
        // keys remain findable past the tombstone.
        for k in 0..6u32 { m.insert(k, k * 10).unwrap(); }
        // Remove a middle key.
        m.remove(&2);
        for k in [0u32, 1, 3, 4, 5] {
            assert_eq!(m.get(&k), Some(k * 10),
                "key {k} should still be findable past the tombstone");
        }
        assert_eq!(m.get(&2), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn full_map_returns_error_on_new_key() {
        let p = tmp("full");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 4).unwrap();
        m.insert(1, 1).unwrap();
        m.insert(2, 2).unwrap();
        m.insert(3, 3).unwrap();
        m.insert(4, 4).unwrap();
        assert_eq!(m.insert(5, 5).err(), Some(MapError::Full));
        // Update of existing key still works.
        assert_eq!(m.insert(1, 100).unwrap(), InsertOutcome::Updated);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn clear_resets_to_empty() {
        let p = tmp("clear");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        for k in 0..5u32 { m.insert(k, k).unwrap(); }
        assert_eq!(m.len(), 5);
        m.clear();
        assert_eq!(m.len(), 0);
        assert_eq!(m.get(&0), None);
        m.insert(99, 99).unwrap();
        assert_eq!(m.get(&99), Some(99));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn snapshot_collects_all_present_pairs() {
        let p = tmp("snap");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        m.insert(1, 10).unwrap();
        m.insert(2, 20).unwrap();
        m.insert(3, 30).unwrap();
        m.remove(&2);
        let mut snap = m.snapshot();
        snap.sort();
        assert_eq!(snap, vec![(1, 10), (3, 30)]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_visibility() {
        let p = tmp("cross-handle");
        let writer: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        let reader: SharedHashMap<u32, u32> = SharedHashMap::open(&p, 16).unwrap();
        writer.insert(42, 4242).unwrap();
        assert_eq!(reader.get(&42), Some(4242));
        reader.insert(7, 77).unwrap();
        assert_eq!(writer.get(&7), Some(77));
        writer.remove(&42);
        assert_eq!(reader.get(&42), None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_inserters_all_keys_present() {
        let p = tmp("concurrent");
        let m: Arc<SharedHashMap<u32, u32>> = Arc::new(SharedHashMap::create(&p, 1024).unwrap());
        let n_threads = 4;
        let per_thread = 100u32;
        let mut handles = vec![];
        for t in 0..n_threads {
            let m = m.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    let key = (t as u32) * per_thread + i;
                    m.insert(key, key * 10).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(m.len(), n_threads * per_thread as usize);
        for t in 0..n_threads as u32 {
            for i in 0..per_thread {
                let key = t * per_thread + i;
                assert_eq!(m.get(&key), Some(key * 10),
                    "key {key} should be present with value {}", key * 10);
            }
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_key_and_value_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(C)]
        struct UserId { realm: u32, user: u32 }
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct Session { token: u64, expires_us: u64 }
        let p = tmp("struct");
        let m: SharedHashMap<UserId, Session> = SharedHashMap::create(&p, 32).unwrap();
        let k = UserId { realm: 1, user: 42 };
        let v = Session { token: 0xDEAD_BEEF, expires_us: 9_999_999_999 };
        m.insert(k, v).unwrap();
        assert_eq!(m.get(&k), Some(v));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn payload_too_large_at_create() {
        #[allow(dead_code)] // sizeof signal only
        struct BigKey([u8; 64]);
        impl Copy for BigKey {}
        impl Clone for BigKey { fn clone(&self) -> Self { *self } }
        impl PartialEq for BigKey { fn eq(&self, _: &Self) -> bool { true } }
        impl Eq for BigKey {}
        let p = tmp("too-large");
        let r = SharedHashMap::<BigKey, u32>::create(&p, 4);
        assert_eq!(r.err(), Some(MapError::PayloadTooLarge));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
            for k in 0..5u32 { m.insert(k, k * 100).unwrap(); }
            m.remove(&2);
            m.flush().unwrap();
        }
        let m2: SharedHashMap<u32, u32> = SharedHashMap::open(&p, 16).unwrap();
        assert_eq!(m2.len(), 4);
        assert_eq!(m2.get(&0), Some(0));
        assert_eq!(m2.get(&1), Some(100));
        assert_eq!(m2.get(&2), None);
        assert_eq!(m2.get(&3), Some(300));
        assert_eq!(m2.get(&4), Some(400));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn fnv1a_64_is_deterministic() {
        // Sanity: same input always produces the same hash.
        let h1 = fnv1a_64(b"adaptive-prims");
        let h2 = fnv1a_64(b"adaptive-prims");
        assert_eq!(h1, h2);
        // And different inputs hash differently.
        let h3 = fnv1a_64(b"ADAPTIVE-PRIMS");
        assert_ne!(h1, h3);
    }

    #[test]
    fn load_factor_reports_correctly() {
        let p = tmp("load");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 10).unwrap();
        for k in 0..3u32 { m.insert(k, k).unwrap(); }
        assert_eq!(m.load_factor(), 0.3);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn remove_bumps_tombstone_counter() {
        let p = tmp("tomb-counter");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.tombstone_count(), 0);
        for k in 0..5u32 { m.insert(k, k).unwrap(); }
        assert_eq!(m.tombstone_count(), 0);
        m.remove(&1);
        m.remove(&3);
        assert_eq!(m.tombstone_count(), 2);
        // Removing absent key does NOT bump tombstone count.
        m.remove(&999);
        assert_eq!(m.tombstone_count(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compact_on_empty_map_is_noop() {
        let p = tmp("compact-empty");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        assert_eq!(m.compact().unwrap(), 0);
        assert_eq!(m.len(), 0);
        assert_eq!(m.tombstone_count(), 0);
        // Map remains fully usable.
        m.insert(7, 70).unwrap();
        assert_eq!(m.get(&7), Some(70));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compact_reclaims_tombstones() {
        let p = tmp("compact-reclaim");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
        for k in 0..10u32 { m.insert(k, k * 10).unwrap(); }
        for k in [0u32, 2, 4, 6, 8] { m.remove(&k); }
        assert_eq!(m.tombstone_count(), 5);
        let reclaimed = m.compact().unwrap();
        assert_eq!(reclaimed, 5);
        assert_eq!(m.tombstone_count(), 0);
        assert_eq!(m.len(), 5);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compact_preserves_all_live_pairs() {
        let p = tmp("compact-preserve");
        let m: SharedHashMap<u32, u64> = SharedHashMap::create(&p, 32).unwrap();
        // Insert 20, remove 10, compact, verify the remaining 10
        // are all present with their original values.
        for k in 0..20u32 { m.insert(k, (k as u64) * 1000).unwrap(); }
        for k in (0..20u32).filter(|k| k % 2 == 0) { m.remove(&k); }
        let pre: Vec<(u32, u64)> = {
            let mut s = m.snapshot();
            s.sort();
            s
        };
        m.compact().unwrap();
        let post: Vec<(u32, u64)> = {
            let mut s = m.snapshot();
            s.sort();
            s
        };
        assert_eq!(pre, post,
            "compact must preserve every live (K, V) pair exactly");
        // And every preserved key is still findable by lookup.
        for (k, v) in &post {
            assert_eq!(m.get(k), Some(*v));
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compact_reclaims_after_heavy_churn() {
        // Many insert/remove cycles accumulate tombstones in the
        // probe path because insert probes PAST tombstones if the
        // tombstone-reuse path is not exercised. Compact must
        // reclaim every dead slot exactly.
        let p = tmp("compact-churn");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 64).unwrap();
        for k in 0..32u32 { m.insert(k, k).unwrap(); }
        for round in 0..16u32 {
            m.remove(&round);
            let new_key = 100 + round;
            m.insert(new_key, new_key).unwrap();
        }
        assert_eq!(m.tombstone_count(), 16);
        let live_before = m.len();
        let reclaimed = m.compact().unwrap();
        assert_eq!(reclaimed, 16);
        assert_eq!(m.tombstone_count(), 0);
        assert_eq!(m.len(), live_before);
        for round in 0..16u32 {
            assert_eq!(m.get(&round), None);
            let new_key = 100 + round;
            assert_eq!(m.get(&new_key), Some(new_key));
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn tombstone_reuse_avoids_full_after_remove() {
        // 8-slot table, fill it, remove one key. The next insert
        // of a NEW key REUSES the tombstone slot instead of
        // returning Full. This validates the tombstone-reuse-on-
        // insert path.
        let p = tmp("reuse-avoids-full");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 8).unwrap();
        for k in 0..8u32 { m.insert(k, k).unwrap(); }
        m.remove(&3);
        assert_eq!(m.tombstone_count(), 1);
        // Without reuse this returns Full (7 live + 1 tombstone
        // in 8 slots). With reuse it succeeds and the tombstone
        // counter drops to 0.
        assert!(m.insert(99, 99).is_ok(),
            "tombstone reuse must let insert succeed");
        assert_eq!(m.get(&99), Some(99));
        assert_eq!(m.tombstone_count(), 0,
            "successful tombstone reuse must decrement the counter");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compact_still_useful_for_remove_heavy_workload() {
        // Insert N, remove most WITHOUT re-inserting. Tombstones
        // accumulate because there is no insert to trigger reuse.
        // compact() bulk-reclaims them. This covers workloads
        // that lack the insert pressure to trigger reuse
        // naturally.
        let p = tmp("compact-still-useful");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 32).unwrap();
        for k in 0..20u32 { m.insert(k, k).unwrap(); }
        for k in 0..15u32 { m.remove(&k); }
        assert_eq!(m.tombstone_count(), 15);
        let reclaimed = m.compact().unwrap();
        assert_eq!(reclaimed, 15);
        assert_eq!(m.tombstone_count(), 0);
        assert_eq!(m.len(), 5);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn should_compact_threshold_logic() {
        let p = tmp("should-compact");
        let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 100).unwrap();
        // 0 tombstones / 100 capacity = 0.0
        assert!(!m.should_compact(0.01));
        // Insert 50, remove 30 → 30 tombstones / 100 = 0.30.
        for k in 0..50u32 { m.insert(k, k).unwrap(); }
        for k in 0..30u32 { m.remove(&k); }
        assert_eq!(m.tombstone_count(), 30);
        assert!(m.should_compact(0.30));
        assert!(m.should_compact(0.29));
        assert!(!m.should_compact(0.31));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn compact_persists_across_reopen() {
        let p = tmp("compact-disk");
        {
            let m: SharedHashMap<u32, u32> = SharedHashMap::create(&p, 16).unwrap();
            for k in 0..6u32 { m.insert(k, k * 10).unwrap(); }
            for k in [0u32, 2, 4] { m.remove(&k); }
            m.compact().unwrap();
            m.flush().unwrap();
        }
        let m2: SharedHashMap<u32, u32> = SharedHashMap::open(&p, 16).unwrap();
        assert_eq!(m2.len(), 3);
        assert_eq!(m2.tombstone_count(), 0);
        for k in [1u32, 3, 5] {
            assert_eq!(m2.get(&k), Some(k * 10));
        }
        for k in [0u32, 2, 4] {
            assert_eq!(m2.get(&k), None);
        }
        std::fs::remove_file(&p).ok();
    }
}
