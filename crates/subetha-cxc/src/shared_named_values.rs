//! `SharedNamedValues` - byte values under case-insensitive names, shared
//! by every process that opens the same three files: a
//! [`RawHashMap`] from the hash of each name to the block that holds its
//! value, a [`RawArena`] of blocks, and a [`SharedEpochs`] table that
//! keeps a block readable while a reader who found it is still reading.
//!
//! # Files
//!
//! `<stem>.map`, `<stem>.arena` and `<stem>.epochs` in one directory,
//! each obtained the way its structure obtains a file: created when
//! absent, attached when present.
//!
//! # A value's life
//!
//! [`set`](SharedNamedValues::set) takes a block for the name and the
//! value, writes both, swaps the block's handle into the map under the
//! name's hash, publishes the block, and retires the block the swap
//! replaced. [`get`](SharedNamedValues::get) pins the current epoch,
//! looks the hash up, reads the block, and checks the name stored in it
//! against the one asked for. [`remove`](SharedNamedValues::remove) takes
//! the entry out of the map and retires its block. A block retired at an
//! epoch stays readable until every pin taken before that epoch is
//! released, then comes free through the arena's reclaim.
//!
//! # Names
//!
//! A name is compared without regard to case: its key is the 128-bit
//! FNV-1a hash of its lowercase form. The name as first written is
//! stored ahead of the value, so a listing shows it and so two names
//! whose hashes collide are told apart and refused rather than merged.
//!
//! # Recovery
//!
//! A writer that dies between taking a block and publishing it, or
//! between the swap and the retire, leaves a block the map does not
//! reach, or reaches while the block is still marked as being written.
//! When the arena reports itself exhausted,
//! [`collect`](SharedNamedValues::collect) walks it with the live map as
//! the root set: a block is reached when the map's entry for the name
//! stored in it is that very block. A reader that meets a block a dead
//! writer left reachable collects too, which publishes it.

use std::path::{Path, PathBuf};

use crate::raw_arena::{BlockHandle, CollectReport, RawArena, RawArenaError};
use crate::raw_hash_map::RawHashMap;
use crate::shared_epochs::{EpochError, SharedEpochs};
use crate::shared_hash_map::MapError;

/// Bytes of a map key: the name's 128-bit hash.
pub const KEY_BYTES: usize = 16;

/// Bytes of a map value: a block handle, the offset then the class.
pub const HANDLE_BYTES: usize = 16;

/// The longest name, in UTF-8 bytes: what the length ahead of a value
/// can say.
pub const MAX_NAME_BYTES: usize = u16::MAX as usize;

/// The shape of a store's three files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamedValuesLayout {
    /// Map slots: the most names the store holds.
    pub names: usize,
    /// Bytes of arena block space.
    pub arena_bytes: usize,
    /// The arena's smallest block class.
    pub min_class: u32,
    /// The arena's largest block class.
    pub max_class: u32,
    /// Pin slots: the most readers at once, across every process.
    pub pins: usize,
}

#[derive(Debug)]
pub enum NamedValuesError {
    Map(MapError),
    Arena(RawArenaError),
    Epochs(EpochError),
    /// The name is longer than the length ahead of a value can say.
    NameTooLong { bytes: usize, max: usize },
    /// The map has no slot left for a new name.
    TooManyNames { capacity: usize },
    /// The name and the value together need more than the largest
    /// block holds.
    TooLarge { bytes: usize, max: usize },
    /// No block could be found for the value, after collecting.
    Full { bytes: usize },
    /// Another name with the same hash holds the entry.
    NameCollision { stored: String, asked: String },
    /// A block's stored name is shorter than its length says or is not
    /// UTF-8.
    Corrupt,
}

impl From<MapError> for NamedValuesError {
    fn from(e: MapError) -> Self {
        Self::Map(e)
    }
}

impl From<RawArenaError> for NamedValuesError {
    fn from(e: RawArenaError) -> Self {
        Self::Arena(e)
    }
}

impl From<EpochError> for NamedValuesError {
    fn from(e: EpochError) -> Self {
        Self::Epochs(e)
    }
}

impl std::fmt::Display for NamedValuesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Map(e) => write!(f, "the name map: {e:?}"),
            Self::Arena(e) => write!(f, "the value arena: {e}"),
            Self::Epochs(e) => write!(f, "the epoch table: {e:?}"),
            Self::NameTooLong { bytes, max } => write!(f, "a name of {bytes} bytes; the longest is {max}"),
            Self::TooManyNames { capacity } => write!(f, "the store holds its {capacity} names already"),
            Self::TooLarge { bytes, max } => write!(f, "a name and value of {bytes} bytes; the largest block holds {max}"),
            Self::Full { bytes } => write!(f, "no block of {bytes} bytes is free, after collecting"),
            Self::NameCollision { stored, asked } => {
                write!(f, "the names {stored:?} and {asked:?} hash alike; the store holds {stored:?}")
            }
            Self::Corrupt => write!(f, "a block's stored name cannot be read"),
        }
    }
}

impl std::error::Error for NamedValuesError {}

/// The key a name is stored under: the 128-bit FNV-1a hash of its
/// lowercase form, little-endian.
pub fn name_key(name: &str) -> [u8; KEY_BYTES] {
    const BASIS: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let mut h = BASIS;
    for b in name.to_lowercase().bytes() {
        h ^= u128::from(b);
        h = h.wrapping_mul(PRIME);
    }
    h.to_le_bytes()
}

/// Whether two names are the same name.
fn same_name(a: &str, b: &str) -> bool {
    a.to_lowercase() == b.to_lowercase()
}

fn encode_handle(h: BlockHandle) -> [u8; HANDLE_BYTES] {
    let mut out = [0u8; HANDLE_BYTES];
    out[..8].copy_from_slice(&h.offset.to_le_bytes());
    out[8..12].copy_from_slice(&h.class.to_le_bytes());
    out
}

fn decode_handle(bytes: &[u8; HANDLE_BYTES]) -> BlockHandle {
    let mut offset = [0u8; 8];
    offset.copy_from_slice(&bytes[..8]);
    let mut class = [0u8; 4];
    class.copy_from_slice(&bytes[8..12]);
    BlockHandle { offset: u64::from_le_bytes(offset), class: u32::from_le_bytes(class) }
}

/// The bytes a block holds for a name and its value: the name's length,
/// the name, the value.
fn encode_payload(name: &str, value: &[u8]) -> Result<Vec<u8>, NamedValuesError> {
    let n = name.len();
    if n > MAX_NAME_BYTES {
        return Err(NamedValuesError::NameTooLong { bytes: n, max: MAX_NAME_BYTES });
    }
    let mut payload = Vec::with_capacity(2 + n + value.len());
    payload.extend_from_slice(&(n as u16).to_le_bytes());
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(value);
    Ok(payload)
}

/// The name and the value a block holds, or `None` for a block nothing
/// has been written into yet.
fn split_payload(payload: &[u8]) -> Result<Option<(&str, &[u8])>, NamedValuesError> {
    if payload.is_empty() {
        return Ok(None);
    }
    if payload.len() < 2 {
        return Err(NamedValuesError::Corrupt);
    }
    let n = usize::from(u16::from_le_bytes([payload[0], payload[1]]));
    if payload.len() < 2 + n {
        return Err(NamedValuesError::Corrupt);
    }
    let name = match std::str::from_utf8(&payload[2..2 + n]) {
        Ok(name) => name,
        Err(_not_utf8) => return Err(NamedValuesError::Corrupt),
    };
    Ok(Some((name, &payload[2 + n..])))
}

pub struct SharedNamedValues {
    map: RawHashMap,
    arena: RawArena,
    epochs: SharedEpochs,
    layout: NamedValuesLayout,
    dir: PathBuf,
    stem: String,
}

impl SharedNamedValues {
    /// Obtain the store whose files are `<stem>.map`, `<stem>.arena` and
    /// `<stem>.epochs` in `dir`, creating each that is absent and
    /// attaching to each that is present.
    pub fn create(dir: impl AsRef<Path>, stem: &str, layout: NamedValuesLayout) -> Result<Self, NamedValuesError> {
        let dir = dir.as_ref().to_path_buf();
        let map = RawHashMap::create(dir.join(format!("{stem}.map")), layout.names, KEY_BYTES, HANDLE_BYTES)?;
        let arena = RawArena::create(dir.join(format!("{stem}.arena")), layout.arena_bytes, layout.min_class, layout.max_class)?;
        let epochs = SharedEpochs::create(dir.join(format!("{stem}.epochs")), layout.pins)?;
        Ok(Self { map, arena, epochs, layout, dir, stem: stem.to_string() })
    }

    #[inline]
    pub fn layout(&self) -> NamedValuesLayout {
        self.layout
    }

    /// The directory the three files are in.
    #[inline]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The name the three files share.
    #[inline]
    pub fn stem(&self) -> &str {
        &self.stem
    }

    #[inline]
    pub fn map(&self) -> &RawHashMap {
        &self.map
    }

    #[inline]
    pub fn arena(&self) -> &RawArena {
        &self.arena
    }

    #[inline]
    pub fn epochs(&self) -> &SharedEpochs {
        &self.epochs
    }

    /// Names held.
    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }

    /// The value under `name`, or `None` when the store has no such
    /// name.
    pub fn get(&self, name: &str) -> Result<Option<Vec<u8>>, NamedValuesError> {
        let key = name_key(name);
        let _pin = self.epochs.pin()?;
        let mut handle = [0u8; HANDLE_BYTES];
        let mut payload = Vec::new();
        loop {
            if !self.map.get(&key, &mut handle)? {
                return Ok(None);
            }
            let h = decode_handle(&handle);
            if self.arena.read_into(h, &mut payload)? {
                return match split_payload(&payload)? {
                    Some((stored, value)) if same_name(stored, name) => Ok(Some(value.to_vec())),
                    Some((stored, _value)) => {
                        Err(NamedValuesError::NameCollision { stored: stored.to_string(), asked: name.to_string() })
                    }
                    None => Err(NamedValuesError::Corrupt),
                };
            }
            // The entry names a block still being written: its writer is
            // between the swap and the publish, or died there.
            match self.arena.writer_of(h)? {
                Some(pid) if RawArena::process_alive(pid) => std::thread::yield_now(),
                Some(_dead) => {
                    self.collect()?;
                }
                None => std::thread::yield_now(),
            }
        }
    }

    /// The name the store holds `key` under, as first written, when it
    /// holds one. Another name with the same key is refused.
    fn held_name(&self, key: &[u8; KEY_BYTES], name: &str) -> Result<Option<String>, NamedValuesError> {
        let _pin = self.epochs.pin()?;
        let mut handle = [0u8; HANDLE_BYTES];
        if !self.map.get(key, &mut handle)? {
            return Ok(None);
        }
        let mut payload = Vec::new();
        if !self.arena.peek_payload(decode_handle(&handle), &mut payload)? {
            return Ok(None);
        }
        match split_payload(&payload)? {
            Some((stored, _value)) if same_name(stored, name) => Ok(Some(stored.to_string())),
            Some((stored, _value)) => {
                Err(NamedValuesError::NameCollision { stored: stored.to_string(), asked: name.to_string() })
            }
            None => Ok(None),
        }
    }

    /// A block for `bytes` of payload, collecting once when the arena is
    /// exhausted.
    fn take_block(&self, bytes: usize) -> Result<BlockHandle, NamedValuesError> {
        match self.arena.allocate(bytes, &self.epochs) {
            Ok(h) => return Ok(h),
            Err(RawArenaError::TooLarge { len, max }) => return Err(NamedValuesError::TooLarge { bytes: len, max }),
            Err(RawArenaError::Exhausted) => {}
            Err(e) => return Err(e.into()),
        }
        self.collect()?;
        match self.arena.allocate(bytes, &self.epochs) {
            Ok(h) => Ok(h),
            Err(RawArenaError::Exhausted) => Err(NamedValuesError::Full { bytes }),
            Err(e) => Err(e.into()),
        }
    }

    /// Put `value` under `name`, replacing what was there. A name the
    /// store holds keeps the spelling it was first written with.
    pub fn set(&self, name: &str, value: &[u8]) -> Result<(), NamedValuesError> {
        let key = name_key(name);
        let held = self.held_name(&key, name)?;
        let payload = encode_payload(held.as_deref().unwrap_or(name), value)?;
        let h = self.take_block(payload.len())?;
        if let Err(e) = self.arena.write_payload(h, &payload) {
            self.arena.abandon(h)?;
            return Err(e.into());
        }
        let mut old = [0u8; HANDLE_BYTES];
        let replaced = match self.map.swap(&key, &encode_handle(h), &mut old) {
            Ok(replaced) => replaced,
            Err(MapError::Full) => {
                self.arena.abandon(h)?;
                return Err(NamedValuesError::TooManyNames { capacity: self.layout.names });
            }
            Err(e) => {
                self.arena.abandon(h)?;
                return Err(e.into());
            }
        };
        self.arena.publish(h)?;
        if replaced {
            self.arena.retire(decode_handle(&old), &self.epochs)?;
        }
        Ok(())
    }

    /// Take `name` out, retiring its block. `Ok(false)` when the store
    /// had no such name.
    pub fn remove(&self, name: &str) -> Result<bool, NamedValuesError> {
        let key = name_key(name);
        self.held_name(&key, name)?;
        let mut old = [0u8; HANDLE_BYTES];
        if !self.map.remove(&key, &mut old)? {
            return Ok(false);
        }
        self.arena.retire(decode_handle(&old), &self.epochs)?;
        Ok(true)
    }

    /// Every name, as first written, with its value. A value being
    /// written right now is left out.
    pub fn entries(&self) -> Result<Vec<(String, Vec<u8>)>, NamedValuesError> {
        let _pin = self.epochs.pin()?;
        let mut out = Vec::with_capacity(self.map.len());
        let mut payload = Vec::new();
        for (_key, handle) in self.map.snapshot() {
            let mut bytes = [0u8; HANDLE_BYTES];
            bytes.copy_from_slice(&handle[..HANDLE_BYTES]);
            if !self.arena.read_into(decode_handle(&bytes), &mut payload)? {
                continue;
            }
            if let Some((name, value)) = split_payload(&payload)? {
                out.push((name.to_string(), value.to_vec()));
            }
        }
        Ok(out)
    }

    /// Walk the arena with the live map as the root set, retiring what
    /// the map does not reach and publishing what a dead writer left
    /// reachable.
    pub fn collect(&self) -> Result<CollectReport, NamedValuesError> {
        Ok(self.arena.collect(&self.epochs, |h| self.is_current(h))?)
    }

    /// Whether the map's entry for the name stored in block `h` is `h`.
    /// A block whose bytes cannot be read is treated as reached, since
    /// the cost of retiring a live value is higher than the cost of
    /// leaving a block until the next walk.
    fn is_current(&self, h: BlockHandle) -> bool {
        let mut payload = Vec::new();
        match self.arena.peek_payload(h, &mut payload) {
            Ok(true) => {}
            Ok(false) => return false,
            Err(_unreadable) => return true,
        }
        let name = match split_payload(&payload) {
            Ok(Some((name, _value))) => name,
            Ok(None) => return false,
            Err(_unreadable) => return true,
        };
        let mut handle = [0u8; HANDLE_BYTES];
        match self.map.get(&name_key(name), &mut handle) {
            Ok(true) => decode_handle(&handle) == h,
            Ok(false) => false,
            Err(_unreadable) => true,
        }
    }
}

impl std::fmt::Debug for SharedNamedValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedNamedValues")
            .field("dir", &self.dir)
            .field("stem", &self.stem)
            .field("layout", &self.layout)
            .field("names", &self.map.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_paths::TmpFile;

    const LAYOUT: NamedValuesLayout =
        NamedValuesLayout { names: 16, arena_bytes: 64 * 1024, min_class: 6, max_class: 12, pins: 4 };

    /// The three files of one test's store, removed when the test ends.
    struct Files {
        _map: TmpFile,
        _arena: TmpFile,
        _epochs: TmpFile,
        stem: String,
    }

    fn files(name: &str) -> Files {
        let stem = format!("subetha-named-{name}-{}", std::process::id());
        Files {
            _map: TmpFile::new(format!("{stem}.map")),
            _arena: TmpFile::new(format!("{stem}.arena")),
            _epochs: TmpFile::new(format!("{stem}.epochs")),
            stem,
        }
    }

    fn store(name: &str) -> (Files, SharedNamedValues) {
        let f = files(name);
        let s = SharedNamedValues::create(std::env::temp_dir(), &f.stem, LAYOUT).unwrap();
        (f, s)
    }

    #[test]
    fn a_value_comes_back_under_its_name_in_any_case() {
        let (_f, s) = store("roundtrip");
        assert_eq!(s.get("Greeting").unwrap(), None);
        s.set("Greeting", b"hello").unwrap();
        assert_eq!(s.get("Greeting").unwrap().as_deref(), Some(&b"hello"[..]));
        assert_eq!(s.get("GREETING").unwrap().as_deref(), Some(&b"hello"[..]));
        assert_eq!(s.get("greeting").unwrap().as_deref(), Some(&b"hello"[..]));
        assert_eq!(s.len(), 1);
        s.set("greeting", b"again").unwrap();
        assert_eq!(s.get("Greeting").unwrap().as_deref(), Some(&b"again"[..]));
        assert_eq!(s.len(), 1, "one name in any case");
    }

    #[test]
    fn replacing_a_value_retires_its_block_and_a_pinned_reader_still_reads_it() {
        let (_f, s) = store("replace");
        s.set("n", b"one").unwrap();
        let mut handle = [0u8; HANDLE_BYTES];
        assert!(s.map().get(&name_key("n"), &mut handle).unwrap());
        let first = decode_handle(&handle);
        let pin = s.epochs().pin().unwrap();
        s.set("n", b"two").unwrap();
        assert_eq!(s.get("n").unwrap().as_deref(), Some(&b"two"[..]));
        let mut payload = Vec::new();
        assert!(s.arena().read_into(first, &mut payload).unwrap(), "the pinned reader's block is still there");
        assert_eq!(s.arena().retired_blocks(first.class), 1);
        assert_eq!(s.arena().reclaim(first.class, s.epochs()), 0, "held by the pin");
        drop(pin);
        assert_eq!(s.arena().reclaim(first.class, s.epochs()), 1);
        assert!(!s.arena().read_into(first, &mut payload).unwrap());
    }

    #[test]
    fn removing_a_name_retires_its_block() {
        let (_f, s) = store("remove");
        s.set("n", b"value").unwrap();
        assert!(s.remove("N").unwrap());
        assert!(!s.remove("n").unwrap());
        assert_eq!(s.get("n").unwrap(), None);
        assert_eq!(s.len(), 0);
        let class = s.arena().class_for(2 + 1 + 5).unwrap();
        assert_eq!(s.arena().retired_blocks(class), 1);
    }

    #[test]
    fn entries_list_every_name_as_first_written() {
        let (_f, s) = store("entries");
        s.set("Alpha", b"1").unwrap();
        s.set("beta", b"2").unwrap();
        s.set("ALPHA", b"3").unwrap();
        let mut entries = s.entries().unwrap();
        entries.sort();
        assert_eq!(entries, vec![("Alpha".to_string(), b"3".to_vec()), ("beta".to_string(), b"2".to_vec())]);
    }

    #[test]
    fn a_name_whose_hash_another_name_holds_is_refused() {
        let (_f, s) = store("collision");
        s.set("first", b"value").unwrap();
        let mut handle = [0u8; HANDLE_BYTES];
        assert!(s.map().get(&name_key("first"), &mut handle).unwrap());
        s.map().insert(&name_key("other"), &handle).unwrap();
        for outcome in [s.get("other").err(), s.set("other", b"x").err(), s.remove("other").err()] {
            assert!(
                matches!(&outcome, Some(NamedValuesError::NameCollision { stored, asked }) if stored == "first" && asked == "other"),
                "{outcome:?}"
            );
        }
        assert_eq!(s.get("first").unwrap().as_deref(), Some(&b"value"[..]));
    }

    #[test]
    fn a_store_holds_its_names_and_no_more() {
        let (_f, s) = store("names");
        for i in 0..LAYOUT.names {
            s.set(&format!("name{i}"), b"v").unwrap();
        }
        assert!(matches!(s.set("one-too-many", b"v"), Err(NamedValuesError::TooManyNames { capacity: 16 })));
        assert_eq!(s.len(), LAYOUT.names);
        assert_eq!(s.arena().free_blocks(6), s.arena().free_blocks(6), "the refused write left its block free");
        s.remove("name0").unwrap();
        s.set("one-too-many", b"v").unwrap();
    }

    #[test]
    fn a_full_arena_is_collected_with_the_map_as_its_root_set() {
        let (_f, s) = store("collect");
        let value = vec![9u8; (1 << LAYOUT.max_class) - 40 - 2 - 4];
        let chunks = LAYOUT.arena_bytes >> LAYOUT.max_class;
        let mut old = [0u8; HANDLE_BYTES];
        for i in 0..chunks {
            let name = format!("v{i:02}");
            s.set(&name, &value).unwrap();
            // Taken out of the map without a retire, as a writer that died
            // between its swap and its retire leaves a replaced block.
            assert!(s.map().remove(&name_key(&name), &mut old).unwrap());
        }
        assert_eq!(s.len(), 0);
        assert!(matches!(s.arena().allocate(value.len() + 8, s.epochs()), Err(RawArenaError::Exhausted)));
        s.set("kept", &value).unwrap();
        assert_eq!(s.get("kept").unwrap().as_deref(), Some(&value[..]));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn names_and_values_past_their_bounds_are_refused() {
        let (_f, s) = store("bounds");
        let long = "n".repeat(MAX_NAME_BYTES + 1);
        assert!(matches!(s.set(&long, b"v"), Err(NamedValuesError::NameTooLong { bytes, max }) if bytes == MAX_NAME_BYTES + 1 && max == MAX_NAME_BYTES));
        let max = s.arena().max_payload();
        let big = vec![0u8; max];
        assert!(matches!(s.set("big", &big), Err(NamedValuesError::TooLarge { bytes, .. }) if bytes == max + 2 + 3));
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn a_second_handle_on_the_same_files_sees_the_first_ones_values() {
        let (f, s) = store("second");
        s.set("shared", b"yes").unwrap();
        let t = SharedNamedValues::create(std::env::temp_dir(), &f.stem, LAYOUT).unwrap();
        assert_eq!(t.get("shared").unwrap().as_deref(), Some(&b"yes"[..]));
        t.set("shared", b"no").unwrap();
        assert_eq!(s.get("shared").unwrap().as_deref(), Some(&b"no"[..]));
    }
}
