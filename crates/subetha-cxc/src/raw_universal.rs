//! `RawUniversal` - the strategy-switching set at an element size chosen
//! at run time.
//!
//! [`SharedUniversal`](crate::shared_universal::SharedUniversal) is
//! generic over its element, which a caller binding through C cannot name.
//! This holds the same two backings - a vector scanned linearly, and a
//! hash map - and takes the element size as an argument.
//!
//! # Why two backings
//!
//! A small set is faster to scan than to hash, and a large one is not. The
//! container carries both and can be moved between them while it holds
//! data, so a caller that guessed wrong at creation is not stuck with the
//! guess.
//!
//! Nothing here migrates on its own. [`migrate_to`](RawUniversal::migrate_to)
//! is called or it is not, and the operation counts are published so the
//! caller can decide. A policy that reads those counts belongs above this
//! layer, where the knowledge of the access pattern lives.
//!
//! # How a reader knows the ground moved
//!
//! One 64-bit word holds the strategy, a generation and a version, and a
//! migration rewrites it as a unit. The word lives in a state file beside
//! the two backings, mapped by every handle, so a migration through one
//! handle is what every other process reads on its next operation; a
//! handle carries no belief of its own about which backing is live. A
//! reader that samples the word before and after an operation and sees
//! the same value knows no migration interleaved. That is what makes it
//! safe to read a backing without a lock: not that migrations are
//! prevented, but that they are detectable.
//!
//! The version is bumped per migration and rolls into the generation, so
//! the pair is unique across 2^48 migrations on one base path. Exhausting
//! both is reported rather than wrapped, because a wrapped version makes
//! two different states compare equal, and that comparison is the one
//! every reader depends on.
//!
//! A migration is a write the caller serializes against every writer and
//! every other migrator, as it serializes `clear`. Two migrations racing
//! are not merged: the word is published by compare-exchange from the
//! value the migration read, and one that finds the word moved reports it
//! and leaves the container on the backing the other chose.

use std::fs::File;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

use crate::raw_hash_map::RawHashMap;
use crate::raw_treiber_stack::ElementLayout;
use crate::raw_vec::RawVec;
use crate::shared_hash_map::MapError;
use crate::shared_universal::Strategy;
use crate::shared_vec::VecError;

/// Why an operation on the container could not be carried out.
#[derive(Debug)]
pub enum RawUniversalError {
    /// A value slice was not the size this container was built for.
    WrongSize { expected: usize, found: usize },
    /// The element size asked for is zero.
    ZeroElement,
    /// The backing is full.
    Full,
    /// Both the version and the generation are at their maximum, so no
    /// further migration could be told apart from an earlier one.
    VersionExhausted,
    /// The state file on disk was laid out for another capacity or
    /// element size, or is not a state file.
    StateMismatch,
    /// Another process migrated the container while this migration ran,
    /// so its word was not published and the container stays on the
    /// backing the other chose; `published` is the word that stands.
    MigrationRaced { published: u64 },
    Vec(VecError),
    Map(MapError),
    Io(std::io::ErrorKind),
}

impl From<VecError> for RawUniversalError {
    fn from(e: VecError) -> Self {
        Self::Vec(e)
    }
}
impl From<MapError> for RawUniversalError {
    fn from(e: MapError) -> Self {
        match e {
            MapError::Full => Self::Full,
            other => Self::Map(other),
        }
    }
}
impl From<std::io::Error> for RawUniversalError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.kind())
    }
}

/// The map backing stores the element as a key with a one-byte value: a
/// set is a map whose values carry nothing, and a zero-length value is not
/// a shape the map accepts.
const SET_VALUE_BYTES: usize = 1;

fn pack(version: u32, generation: u16, strategy: u8) -> u64 {
    ((version as u64) << 32) | ((generation as u64) << 16) | (strategy as u64)
}

fn unpack(v: u64) -> (u32, u16, u8) {
    ((v >> 32) as u32, ((v >> 16) & 0xFFFF) as u16, (v & 0xFF) as u8)
}

/// A byte this module never writes means the word was corrupted rather
/// than that a newer writer used it, and the vector is the safe reading
/// because it needs no agreement about hashing.
fn strategy_or_vec(byte: u8) -> Strategy {
    match byte {
        1 => Strategy::Map,
        _ => Strategy::Vec,
    }
}

fn vec_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".uvec.bin");
    PathBuf::from(p)
}

fn map_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".umap.bin");
    PathBuf::from(p)
}

fn state_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".ustate.bin");
    PathBuf::from(p)
}

fn element_layout(element_size: usize) -> ElementLayout {
    ElementLayout { slot_size: element_size, alignment: 8, tag: 0x554E_4956_5241_4C53 }
}

const STATE_MAGIC: u32 = 0x5553_5445;

/// The state file: one cache line every handle maps, carrying the word a
/// migration publishes and the shape the container was created with.
#[repr(C, align(64))]
struct StateHeader {
    magic: u32,
    capacity: u32,
    element_size: u32,
    _reserved: u32,
    state: AtomicU64,
    _pad: [u8; 40],
}

const _: () = assert!(size_of::<StateHeader>() == 64);

/// Lay out a fresh state file: the shape and the word first, the magic
/// last, because an attacher spins on the magic.
///
/// # Safety
/// `ptr` addresses at least `size_of::<StateHeader>()` writable zeroed
/// bytes.
unsafe fn init_state(ptr: *mut u8, capacity: usize, element_size: usize, strategy: Strategy) {
    let hdr = ptr as *mut StateHeader;
    unsafe {
        (*hdr).capacity = capacity as u32;
        (*hdr).element_size = element_size as u32;
        (*hdr).state.store(pack(0, 0, strategy as u8), Ordering::Release);
        std::ptr::write_volatile(&raw mut (*hdr).magic, STATE_MAGIC);
    }
}

/// Whether a mapped state file is one of ours at this shape.
fn state_matches(mmap: &MmapMut, capacity: usize, element_size: usize) -> bool {
    // The mapping is at least a header long by construction.
    let hdr = unsafe { &*(mmap.as_ptr() as *const StateHeader) };
    hdr.magic == STATE_MAGIC && hdr.capacity == capacity as u32 && hdr.element_size == element_size as u32
}

/// The strategy word, and how many operations of each kind have run.
#[derive(Debug, Clone, Copy)]
pub struct RawUniversalState {
    pub strategy: Strategy,
    pub version: u32,
    pub generation: u16,
    pub inserts: u64,
    pub contains: u64,
}

impl RawUniversalState {
    /// The word a reader compares across an operation. Equal before and
    /// after means no migration interleaved.
    pub fn stamp(&self) -> u64 {
        pack(self.version, self.generation, self.strategy as u8)
    }
}

pub struct RawUniversal {
    base: PathBuf,
    capacity: usize,
    element_size: usize,
    _state_file: File,
    state_mmap: MmapMut,
    inserts: AtomicU64,
    contains: AtomicU64,
    vec: RawVec,
    map: RawHashMap,
}

impl RawUniversal {
    /// Obtain the container under `base_path`: a new one starts on
    /// `strategy`, and one that already exists keeps the strategy in
    /// force, whatever this caller would have started it on.
    ///
    /// Both backings are created, because a migration has to happen
    /// without creating a file while callers are attached.
    pub fn create(
        base_path: impl AsRef<Path>,
        capacity: usize,
        element_size: usize,
        strategy: Strategy,
    ) -> Result<Self, RawUniversalError> {
        if element_size == 0 {
            return Err(RawUniversalError::ZeroElement);
        }
        let base = base_path.as_ref().to_path_buf();
        let vec = RawVec::create(vec_path(&base), capacity, element_layout(element_size))?;
        let map = RawHashMap::create(
            map_path(&base),
            (capacity * 2).max(16),
            element_size,
            SET_VALUE_BYTES,
        )?;
        let (state_file, state_mmap) = crate::mmf_attach::create_or_attach(
            &state_path(&base),
            size_of::<StateHeader>(),
            |ptr| unsafe { init_state(ptr, capacity, element_size, strategy) },
            |ptr| unsafe { (*(ptr as *const StateHeader)).magic == STATE_MAGIC },
        )
        .map_err(|e| crate::mmf_attach::attach_error(e, RawUniversalError::StateMismatch))?;
        if !state_matches(&state_mmap, capacity, element_size) {
            return Err(RawUniversalError::StateMismatch);
        }
        Ok(Self {
            base,
            capacity,
            element_size,
            _state_file: state_file,
            state_mmap,
            inserts: AtomicU64::new(0),
            contains: AtomicU64::new(0),
            vec,
            map,
        })
    }

    /// Attach to a container another process created. The strategy in
    /// force is read from the state file rather than declared.
    pub fn open(
        base_path: impl AsRef<Path>,
        capacity: usize,
        element_size: usize,
    ) -> Result<Self, RawUniversalError> {
        if element_size == 0 {
            return Err(RawUniversalError::ZeroElement);
        }
        let base = base_path.as_ref().to_path_buf();
        let vec = RawVec::open(vec_path(&base), capacity, element_layout(element_size))?;
        let map = RawHashMap::open(
            map_path(&base),
            (capacity * 2).max(16),
            element_size,
            SET_VALUE_BYTES,
        )?;
        let state_file = crate::region_file::open_existing(&state_path(&base))?;
        if state_file.metadata()?.len() < size_of::<StateHeader>() as u64 {
            return Err(RawUniversalError::StateMismatch);
        }
        let state_mmap = unsafe { MmapOptions::new().len(size_of::<StateHeader>()).map_mut(&state_file)? };
        if !state_matches(&state_mmap, capacity, element_size) {
            return Err(RawUniversalError::StateMismatch);
        }
        Ok(Self {
            base,
            capacity,
            element_size,
            _state_file: state_file,
            state_mmap,
            inserts: AtomicU64::new(0),
            contains: AtomicU64::new(0),
            vec,
            map,
        })
    }

    fn header(&self) -> &StateHeader {
        // Mapped at exactly a header's length by both constructors.
        unsafe { &*(self.state_mmap.as_ptr() as *const StateHeader) }
    }

    pub fn base(&self) -> &Path {
        &self.base
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn element_size(&self) -> usize {
        self.element_size
    }

    /// The strategy word and the operation counts, read together. The
    /// word is the shared one; the counts are this handle's own.
    pub fn state(&self) -> RawUniversalState {
        let (version, generation, strategy) = unpack(self.header().state.load(Ordering::Acquire));
        RawUniversalState {
            strategy: strategy_or_vec(strategy),
            version,
            generation,
            inserts: self.inserts.load(Ordering::Relaxed),
            contains: self.contains.load(Ordering::Relaxed),
        }
    }

    pub fn strategy(&self) -> Strategy {
        self.state().strategy
    }

    fn sized(&self, value: &[u8]) -> Result<(), RawUniversalError> {
        if value.len() == self.element_size {
            Ok(())
        } else {
            Err(RawUniversalError::WrongSize {
                expected: self.element_size,
                found: value.len(),
            })
        }
    }

    /// Add `value` if it is not already present.
    ///
    /// `true` when it was added, `false` when the set already held it.
    pub fn insert(&self, value: &[u8]) -> Result<bool, RawUniversalError> {
        self.sized(value)?;
        self.inserts.fetch_add(1, Ordering::Relaxed);
        match self.strategy() {
            Strategy::Vec => {
                if self.vec_contains(value)? {
                    return Ok(false);
                }
                self.vec.push_back(value)?;
                Ok(true)
            }
            Strategy::Map => {
                let mut existing = [0u8; SET_VALUE_BYTES];
                // The map answers whether a value was already there, which
                // is the opposite of what this call reports, so it is
                // negated rather than passed through.
                let was_present = self.map.insert_if_absent(value, &[0u8], &mut existing)?;
                Ok(!was_present)
            }
        }
    }

    /// Whether the set holds `value`.
    pub fn contains(&self, value: &[u8]) -> Result<bool, RawUniversalError> {
        self.sized(value)?;
        self.contains.fetch_add(1, Ordering::Relaxed);
        match self.strategy() {
            Strategy::Vec => self.vec_contains(value),
            Strategy::Map => Ok(self.map.contains_key(value)?),
        }
    }

    fn vec_contains(&self, value: &[u8]) -> Result<bool, RawUniversalError> {
        let mut slot = vec![0u8; self.element_size];
        for i in 0..self.vec.len() {
            if self.vec.get(i, &mut slot)? && slot == value {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn len(&self) -> Result<usize, RawUniversalError> {
        Ok(match self.strategy() {
            Strategy::Vec => self.vec.len(),
            Strategy::Map => self.map.len(),
        })
    }

    pub fn is_empty(&self) -> Result<bool, RawUniversalError> {
        Ok(self.len()? == 0)
    }

    /// Every element, in no promised order.
    pub fn snapshot(&self) -> Result<Vec<Vec<u8>>, RawUniversalError> {
        match self.strategy() {
            Strategy::Vec => {
                let mut out = Vec::with_capacity(self.vec.len());
                let mut slot = vec![0u8; self.element_size];
                for i in 0..self.vec.len() {
                    if self.vec.get(i, &mut slot)? {
                        out.push(slot.clone());
                    }
                }
                Ok(out)
            }
            Strategy::Map => Ok(self.map.snapshot().into_iter().map(|(k, _)| k).collect()),
        }
    }

    /// Move to `target`, carrying every element across.
    ///
    /// The elements reach the target backing before the strategy word
    /// changes, so a reader that samples the word and finds it unchanged
    /// has been reading a backing that was complete throughout. A failure
    /// part-way leaves the word untouched and the container on its old
    /// backing, and so does another process's migration landing first,
    /// which is reported as `MigrationRaced`.
    pub fn migrate_to(&self, target: Strategy) -> Result<(), RawUniversalError> {
        let current_state = self.header().state.load(Ordering::Acquire);
        let (version, generation, strategy) = unpack(current_state);
        if strategy_or_vec(strategy) == target {
            // A no-op must not burn a version, or a reader sees a change
            // that did not happen.
            return Ok(());
        }

        let (next_version, next_generation) = match version.checked_add(1) {
            Some(v) => (v, generation),
            // The version rolled. It carries into the generation so the
            // pair stays unique; a wrapped version would make two
            // different states compare equal.
            None => match generation.checked_add(1) {
                Some(g) => (0, g),
                None => return Err(RawUniversalError::VersionExhausted),
            },
        };

        let elements = self.snapshot()?;
        match target {
            Strategy::Vec => {
                self.vec.clear()?;
                for e in &elements {
                    self.vec.push_back(e)?;
                }
            }
            Strategy::Map => {
                self.map.clear();
                let mut existing = [0u8; SET_VALUE_BYTES];
                for e in &elements {
                    self.map.insert_if_absent(e, &[0u8], &mut existing)?;
                }
            }
        }

        // Published last, as one word, and only over the word this
        // migration read: a word that moved meanwhile belongs to another
        // migration, whose backing is now the live one.
        match self.header().state.compare_exchange(
            current_state,
            pack(next_version, next_generation, target as u8),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(published) => Err(RawUniversalError::MigrationRaced { published }),
        }
    }

    /// Empty the live backing.
    ///
    /// The two backings report differently - one can fail, the other
    /// cannot - so each arm is handled rather than coerced to a common
    /// shape that would hide which happened.
    pub fn clear(&self) -> Result<(), RawUniversalError> {
        match self.strategy() {
            Strategy::Vec => self.vec.clear()?,
            Strategy::Map => self.map.clear(),
        }
        Ok(())
    }

    /// Push both backings to disk and wait for them.
    pub fn flush(&self) -> Result<(), RawUniversalError> {
        self.vec.flush()?;
        self.map.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha_raw_universal_{name}_{}", std::process::id()));
        p
    }

    fn cleanup(base: &Path) {
        for p in [vec_path(base), map_path(base), state_path(base)] {
            match std::fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("could not clear {}: {e}", p.display()),
            }
        }
    }

    #[test]
    fn a_set_holds_each_value_once_on_either_backing() {
        for strategy in [Strategy::Vec, Strategy::Map] {
            let base = scratch(&format!("once_{}", strategy as u8));
            cleanup(&base);
            let u = RawUniversal::create(&base, 16, 8, strategy).expect("created");

            assert!(u.insert(&1u64.to_le_bytes()).expect("first"), "newly added");
            assert!(!u.insert(&1u64.to_le_bytes()).expect("again"), "already present");
            assert_eq!(u.len().expect("len"), 1, "{strategy:?} kept one copy");
            assert!(u.contains(&1u64.to_le_bytes()).expect("contains"));
            assert!(!u.contains(&2u64.to_le_bytes()).expect("contains"));

            cleanup(&base);
        }
    }

    #[test]
    fn a_migration_carries_every_element_across() {
        let base = scratch("migrate");
        cleanup(&base);
        let u = RawUniversal::create(&base, 32, 8, Strategy::Vec).expect("created");
        for i in 0u64..10 {
            u.insert(&i.to_le_bytes()).expect("insert");
        }
        assert_eq!(u.strategy(), Strategy::Vec);
        assert_eq!(u.len().expect("len"), 10);

        u.migrate_to(Strategy::Map).expect("to map");
        assert_eq!(u.strategy(), Strategy::Map);
        assert_eq!(u.len().expect("len"), 10, "nothing lost crossing over");
        for i in 0u64..10 {
            assert!(u.contains(&i.to_le_bytes()).expect("contains"), "{i} survived");
        }

        // And back again.
        u.migrate_to(Strategy::Vec).expect("to vec");
        assert_eq!(u.len().expect("len"), 10);
        for i in 0u64..10 {
            assert!(u.contains(&i.to_le_bytes()).expect("contains"), "{i} survived the return");
        }

        cleanup(&base);
    }

    #[test]
    fn the_state_word_changes_on_every_migration_and_not_otherwise() {
        let base = scratch("stamp");
        cleanup(&base);
        let u = RawUniversal::create(&base, 16, 8, Strategy::Vec).expect("created");

        let before = u.state().stamp();
        u.insert(&5u64.to_le_bytes()).expect("insert");
        u.contains(&5u64.to_le_bytes()).expect("contains");
        assert_eq!(u.state().stamp(), before, "ordinary work leaves the word alone");

        u.migrate_to(Strategy::Map).expect("migrate");
        let after = u.state().stamp();
        assert_ne!(after, before, "a migration is visible in the word");
        assert_eq!(u.state().version, 1);

        // Migrating to the strategy already in force changes nothing, so
        // it must not bump the version either.
        u.migrate_to(Strategy::Map).expect("same strategy");
        assert_eq!(u.state().stamp(), after, "no change, no version bump");
        assert_eq!(u.state().version, 1);

        cleanup(&base);
    }

    #[test]
    fn the_operation_counts_are_published_for_a_policy_above_this_layer() {
        let base = scratch("counts");
        cleanup(&base);
        let u = RawUniversal::create(&base, 16, 8, Strategy::Vec).expect("created");

        for i in 0u64..3 {
            u.insert(&i.to_le_bytes()).expect("insert");
        }
        for _ in 0..7 {
            u.contains(&1u64.to_le_bytes()).expect("contains");
        }
        let s = u.state();
        assert_eq!(s.inserts, 3);
        assert_eq!(s.contains, 7);
        // Nothing migrated on its own; the caller decides.
        assert_eq!(s.strategy, Strategy::Vec);
        assert_eq!(s.version, 0);

        cleanup(&base);
    }

    #[test]
    fn a_value_of_the_wrong_size_is_refused_rather_than_padded() {
        let base = scratch("sizes");
        cleanup(&base);
        let u = RawUniversal::create(&base, 16, 8, Strategy::Vec).expect("created");
        assert!(matches!(
            u.insert(&[0u8; 4]),
            Err(RawUniversalError::WrongSize { expected: 8, found: 4 })
        ));
        assert!(matches!(
            u.contains(&[0u8; 16]),
            Err(RawUniversalError::WrongSize { expected: 8, found: 16 })
        ));
        assert_eq!(u.len().expect("len"), 0);
        cleanup(&base);
    }

    #[test]
    fn a_zero_element_size_is_refused() {
        let base = scratch("zeroelem");
        cleanup(&base);
        assert!(matches!(
            RawUniversal::create(&base, 16, 0, Strategy::Vec),
            Err(RawUniversalError::ZeroElement)
        ));
        cleanup(&base);
    }

    #[test]
    fn a_snapshot_holds_the_same_set_on_either_backing() {
        let base = scratch("snapshot");
        cleanup(&base);
        let u = RawUniversal::create(&base, 32, 8, Strategy::Vec).expect("created");
        for i in 0u64..5 {
            u.insert(&i.to_le_bytes()).expect("insert");
        }
        let mut from_vec = u.snapshot().expect("vec snapshot");
        from_vec.sort();

        u.migrate_to(Strategy::Map).expect("migrate");
        let mut from_map = u.snapshot().expect("map snapshot");
        from_map.sort();

        // Order is not promised, so both are sorted before comparing; what
        // is promised is that the same elements are present.
        assert_eq!(from_vec, from_map);
        cleanup(&base);
    }

    #[test]
    fn a_second_handle_reads_what_the_first_stored() {
        let base = scratch("shared");
        cleanup(&base);
        let a = RawUniversal::create(&base, 16, 8, Strategy::Vec).expect("created");
        a.insert(&42u64.to_le_bytes()).expect("insert");

        let b = RawUniversal::open(&base, 16, 8).expect("opened");
        assert!(b.contains(&42u64.to_le_bytes()).expect("contains"));
        assert_eq!(b.len().expect("len"), 1);

        cleanup(&base);
    }

    /// The defect this guards against was found by a workload: a reader
    /// opened before a migration went on scanning the vector while the
    /// writer inserted into the map, and every element added after the
    /// migration was invisible to it.
    #[test]
    fn a_migration_through_one_handle_is_what_every_other_handle_reads() {
        let base = scratch("migrate_shared");
        cleanup(&base);
        let a = RawUniversal::create(&base, 32, 8, Strategy::Vec).expect("created");
        let b = RawUniversal::open(&base, 32, 8).expect("opened");
        for i in 0u64..5 {
            a.insert(&i.to_le_bytes()).expect("insert");
        }
        let seen_before = b.state().stamp();

        a.migrate_to(Strategy::Map).expect("migrate");
        assert_eq!(b.strategy(), Strategy::Map, "the other handle reads the live strategy");
        assert_ne!(b.state().stamp(), seen_before, "and its stamp moved with it");
        for i in 0u64..5 {
            assert!(b.contains(&i.to_le_bytes()).expect("contains"), "{i} is on the map for the other handle");
        }

        // Work after the migration lands where every handle now looks.
        assert!(a.insert(&100u64.to_le_bytes()).expect("insert after"));
        assert!(b.contains(&100u64.to_le_bytes()).expect("the other handle sees it"));
        assert!(b.insert(&200u64.to_le_bytes()).expect("insert through the other"));
        assert!(a.contains(&200u64.to_le_bytes()).expect("and the first sees that"));
        assert_eq!(a.len().expect("len"), 7);
        assert_eq!(b.len().expect("len"), 7);

        cleanup(&base);
    }

    /// Obtaining an existing container keeps the strategy in force rather
    /// than the one the late caller would have started it on.
    #[test]
    fn a_late_create_attaches_to_the_strategy_in_force() {
        let base = scratch("late_create");
        cleanup(&base);
        let a = RawUniversal::create(&base, 16, 8, Strategy::Vec).expect("created");
        a.insert(&7u64.to_le_bytes()).expect("insert");
        a.migrate_to(Strategy::Map).expect("migrate");

        let late = RawUniversal::create(&base, 16, 8, Strategy::Vec).expect("obtained");
        assert_eq!(late.strategy(), Strategy::Map, "the live strategy, not the argument");
        assert_eq!(late.state().version, 1);
        assert!(late.contains(&7u64.to_le_bytes()).expect("contains"));

        // The backings are attached before the state file, so a shape
        // that disagrees is refused by the vector first; the state file's
        // own check stands behind it for a state file that is not ours.
        assert!(
            matches!(
                RawUniversal::create(&base, 16, 4, Strategy::Vec),
                Err(RawUniversalError::Vec(VecError::LayoutMismatch))
            ),
            "another element size is refused"
        );
        assert!(
            matches!(RawUniversal::open(&base, 64, 8), Err(RawUniversalError::Vec(VecError::LayoutMismatch))),
            "another capacity is refused"
        );
        cleanup(&base);
    }

    /// Two migrations racing are not merged: the second finds the word
    /// moved, reports it, and the container stays where the first put it.
    #[test]
    fn a_migration_that_finds_the_word_moved_reports_it_and_changes_nothing() {
        let base = scratch("raced");
        cleanup(&base);
        let a = RawUniversal::create(&base, 16, 8, Strategy::Vec).expect("created");
        let b = RawUniversal::open(&base, 16, 8).expect("opened");
        a.insert(&1u64.to_le_bytes()).expect("insert");

        // b reads the word first, then a migrates under it: b's publish
        // must not overwrite a's.
        let word_b_read = b.header().state.load(Ordering::Acquire);
        a.migrate_to(Strategy::Map).expect("a migrates");
        let published = a.header().state.load(Ordering::Acquire);
        assert_ne!(published, word_b_read);
        assert!(
            matches!(
                b.header().state.compare_exchange(word_b_read, pack(9, 9, 0), Ordering::AcqRel, Ordering::Acquire),
                Err(current) if current == published
            ),
            "a stale word does not publish"
        );
        assert_eq!(b.strategy(), Strategy::Map);
        assert!(b.contains(&1u64.to_le_bytes()).expect("contains"));
        cleanup(&base);
    }
}
