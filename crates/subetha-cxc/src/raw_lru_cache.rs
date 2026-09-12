//! `RawLruCache` - the shared LRU cache at key and value sizes chosen at
//! run time.
//!
//! [`SharedLRUCache`](crate::shared_lru_cache::SharedLRUCache) is generic
//! over its key and value types, which a caller binding through C cannot
//! name. This holds the same two structures, a map from key to list slot
//! and a doubly-linked list in recency order, and takes those sizes as
//! arguments instead.
//!
//! Keys and values cross as byte slices of exactly the declared size. A
//! slice of any other length is refused rather than padded or truncated,
//! because a key silently changed is a key that names a different entry.
//!
//! # What the two structures promise together
//!
//! The map and the list are separate shared regions and are written one
//! after the other. A reader between the two writes can see a key the
//! map names and the list has already moved. Every read here therefore
//! checks that the list slot it reached still holds the key it asked
//! for, and reports a miss rather than another entry's value when it
//! does not. That check is what makes a torn update read as absent
//! instead of as wrong.

use std::path::{Path, PathBuf};

use crate::raw_hash_map::RawHashMap;
use crate::raw_linked_list::RawLinkedList;
use crate::raw_treiber_stack::ElementLayout;
use crate::shared_hash_map::MapError;
use crate::shared_linked_list::LinkedListError;

/// Why an operation on the cache could not be carried out.
#[derive(Debug)]
pub enum RawLruError {
    /// A key or value slice was not the size this cache was built for.
    WrongSize { expected: usize, found: usize },
    /// The capacity asked for cannot hold anything.
    ZeroCapacity,
    Map(MapError),
    List(LinkedListError),
}

impl From<MapError> for RawLruError {
    fn from(e: MapError) -> Self {
        Self::Map(e)
    }
}
impl From<LinkedListError> for RawLruError {
    fn from(e: LinkedListError) -> Self {
        Self::List(e)
    }
}

/// The list slot index a map value holds. Four bytes, little-endian, so
/// the same bytes mean the same slot on every host that shares the file.
const SLOT_BYTES: usize = 4;

fn map_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".lrumap.bin");
    PathBuf::from(p)
}

fn list_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".lrulist.bin");
    PathBuf::from(p)
}

/// The bytes one list node carries: the key followed by the value, so a
/// node can name its own key when the list is walked without the map.
fn node_layout(key_size: usize, value_size: usize) -> ElementLayout {
    ElementLayout {
        slot_size: key_size + value_size,
        alignment: 8,
        tag: 0x4C52_5543_4143_4845,
    }
}

pub struct RawLruCache {
    map: RawHashMap,
    list: RawLinkedList,
    capacity: u32,
    key_size: usize,
    value_size: usize,
}

impl RawLruCache {
    /// Create the cache under `base_path`, or attach to one already there
    /// with the same shape.
    ///
    /// The map is sized well above `capacity` because an open-addressed
    /// map slows sharply as it fills, and the list holds two slots more
    /// than the capacity so a promotion can allocate before it frees.
    pub fn create(
        base_path: impl AsRef<Path>,
        capacity: u32,
        key_size: usize,
        value_size: usize,
    ) -> Result<Self, RawLruError> {
        let base = base_path.as_ref();
        Self::check_shape(capacity, key_size, value_size)?;
        let map = RawHashMap::create(
            map_path(base),
            (capacity as usize * 8).max(32),
            key_size,
            SLOT_BYTES,
        )?;
        let list = RawLinkedList::create(
            list_path(base),
            capacity as usize + 2,
            node_layout(key_size, value_size),
        )?;
        Ok(Self { map, list, capacity, key_size, value_size })
    }

    /// Attach to a cache another process created under `base_path`.
    pub fn open(
        base_path: impl AsRef<Path>,
        capacity: u32,
        key_size: usize,
        value_size: usize,
    ) -> Result<Self, RawLruError> {
        let base = base_path.as_ref();
        Self::check_shape(capacity, key_size, value_size)?;
        let map = RawHashMap::open(
            map_path(base),
            (capacity as usize * 8).max(32),
            key_size,
            SLOT_BYTES,
        )?;
        let list = RawLinkedList::open(
            list_path(base),
            capacity as usize + 2,
            node_layout(key_size, value_size),
        )?;
        Ok(Self { map, list, capacity, key_size, value_size })
    }

    fn check_shape(capacity: u32, key_size: usize, value_size: usize) -> Result<(), RawLruError> {
        if capacity == 0 {
            return Err(RawLruError::ZeroCapacity);
        }
        if key_size == 0 {
            return Err(RawLruError::WrongSize { expected: 1, found: 0 });
        }
        if value_size == 0 {
            return Err(RawLruError::WrongSize { expected: 1, found: 0 });
        }
        Ok(())
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    pub fn key_size(&self) -> usize {
        self.key_size
    }

    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// Entries the list holds. The map can name a slot the list has
    /// already freed, so the list is what is counted.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn sized(&self, what: &[u8], expected: usize) -> Result<(), RawLruError> {
        if what.len() == expected {
            Ok(())
        } else {
            Err(RawLruError::WrongSize { expected, found: what.len() })
        }
    }

    /// The slot the map names for `key`, or `None`.
    fn slot_of(&self, key: &[u8]) -> Result<Option<u32>, RawLruError> {
        let mut slot = [0u8; SLOT_BYTES];
        if self.map.get(key, &mut slot)? {
            Ok(Some(u32::from_le_bytes(slot)))
        } else {
            Ok(None)
        }
    }

    /// Read the node at `index` into `node`. `false` where the list has
    /// already freed that slot, which is a miss rather than a failure:
    /// the map and list are written separately and a reader can land
    /// between them.
    fn read_node(&self, index: u32, node: &mut [u8]) -> Result<bool, RawLruError> {
        match self.list.get(index, node) {
            Ok(()) => Ok(true),
            Err(LinkedListError::InvalidHandle) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// The value for `key` into `value_out`, without changing its
    /// recency. `false` when the cache does not hold it.
    pub fn get(&self, key: &[u8], value_out: &mut [u8]) -> Result<bool, RawLruError> {
        self.sized(key, self.key_size)?;
        self.sized(value_out, self.value_size)?;
        let Some(index) = self.slot_of(key)? else {
            return Ok(false);
        };
        let mut node = vec![0u8; self.key_size + self.value_size];
        if !self.read_node(index, &mut node)? {
            return Ok(false);
        }
        // The slot the map named must still hold this key. Where it does
        // not, the map and list disagree and the honest answer is a miss
        // rather than whatever key now lives there.
        if &node[..self.key_size] != key {
            return Ok(false);
        }
        value_out.copy_from_slice(&node[self.key_size..]);
        Ok(true)
    }

    /// Whether `key` is in the cache, without changing its recency.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, RawLruError> {
        self.sized(key, self.key_size)?;
        let mut scratch = vec![0u8; self.value_size];
        self.get(key, &mut scratch)
    }

    /// Move `key` to the most recent end. `false` when it is absent.
    pub fn touch(&self, key: &[u8]) -> Result<bool, RawLruError> {
        self.sized(key, self.key_size)?;
        let Some(index) = self.slot_of(key)? else {
            return Ok(false);
        };
        let mut node = vec![0u8; self.key_size + self.value_size];
        match self.list.remove(index, &mut node) {
            Ok(()) => {}
            Err(LinkedListError::InvalidHandle) => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        if &node[..self.key_size] != key {
            // Another key's node, taken out on a stale slot number. It
            // goes back at the least recent end rather than being
            // dropped, and a refusal to put it back is reported: the
            // entry would otherwise be gone from the list while the map
            // still names it.
            self.list.push_back(&node)?;
            return Ok(false);
        }
        let moved = self.list.push_front(&node)?;
        self.map.insert(key, &moved.to_le_bytes())?;
        Ok(true)
    }

    /// The value for `key`, moving it to the most recent end.
    pub fn get_and_touch(&self, key: &[u8], value_out: &mut [u8]) -> Result<bool, RawLruError> {
        if !self.get(key, value_out)? {
            return Ok(false);
        }
        self.touch(key)?;
        Ok(true)
    }

    /// Store `value` under `key` at the most recent end, evicting the
    /// least recent entry first where the cache is full.
    ///
    /// `true` when the key was already present and its value replaced.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<bool, RawLruError> {
        self.sized(key, self.key_size)?;
        self.sized(value, self.value_size)?;
        let mut node = vec![0u8; self.key_size + self.value_size];
        node[..self.key_size].copy_from_slice(key);
        node[self.key_size..].copy_from_slice(value);

        if let Some(index) = self.slot_of(key)? {
            let mut old = vec![0u8; self.key_size + self.value_size];
            let removed = match self.list.remove(index, &mut old) {
                Ok(()) => true,
                Err(LinkedListError::InvalidHandle) => false,
                Err(e) => return Err(e.into()),
            };
            let moved = self.list.push_front(&node)?;
            self.map.insert(key, &moved.to_le_bytes())?;
            return Ok(removed && &old[..self.key_size] == key);
        }

        if self.len() >= self.capacity as usize {
            let mut evicted_key = vec![0u8; self.key_size];
            let mut evicted_value = vec![0u8; self.value_size];
            self.evict_oldest(&mut evicted_key, &mut evicted_value)?;
        }
        let placed = self.list.push_front(&node)?;
        self.map.insert(key, &placed.to_le_bytes())?;
        Ok(false)
    }

    /// Drop `key`, writing its value into `value_out`. `false` when the
    /// cache did not hold it.
    pub fn remove(&self, key: &[u8], value_out: &mut [u8]) -> Result<bool, RawLruError> {
        self.sized(key, self.key_size)?;
        self.sized(value_out, self.value_size)?;
        let mut slot = [0u8; SLOT_BYTES];
        if !self.map.remove(key, &mut slot)? {
            return Ok(false);
        }
        let index = u32::from_le_bytes(slot);
        let mut node = vec![0u8; self.key_size + self.value_size];
        match self.list.remove(index, &mut node) {
            Ok(()) => {}
            Err(LinkedListError::InvalidHandle) => return Ok(false),
            Err(e) => return Err(e.into()),
        }
        if &node[..self.key_size] != key {
            // A stale slot number reached another key's node. Put it
            // back, and report a refusal to do so for the same reason as
            // in `touch`.
            self.list.push_back(&node)?;
            return Ok(false);
        }
        value_out.copy_from_slice(&node[self.key_size..]);
        Ok(true)
    }

    /// Drop the least recently used entry, writing its key and value out.
    /// `false` when the cache is empty.
    pub fn evict_oldest(
        &self,
        key_out: &mut [u8],
        value_out: &mut [u8],
    ) -> Result<bool, RawLruError> {
        self.sized(key_out, self.key_size)?;
        self.sized(value_out, self.value_size)?;
        let mut node = vec![0u8; self.key_size + self.value_size];
        if !self.list.pop_back(&mut node)? {
            return Ok(false);
        }
        key_out.copy_from_slice(&node[..self.key_size]);
        value_out.copy_from_slice(&node[self.key_size..]);
        // The map may already have lost the key to a concurrent remove,
        // so whether it was there is not interesting and is dropped. A
        // failure to reach the map is another matter and propagates.
        let mut discard = [0u8; SLOT_BYTES];
        self.map.remove(key_out, &mut discard)?;
        Ok(true)
    }

    /// Push both regions to disk and wait for them.
    pub fn flush(&self) -> Result<(), RawLruError> {
        self.map.flush()?;
        self.list.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha_raw_lru_{name}_{}", std::process::id()));
        p
    }

    /// Clear both backing files. An absent file is the expected case on
    /// the first run; anything else is reported rather than swallowed,
    /// since a file that cannot be cleared makes the next assertion read
    /// a previous run's entries.
    fn cleanup(base: &Path) {
        for p in [map_path(base), list_path(base)] {
            match std::fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("could not clear {}: {e}", p.display()),
            }
        }
    }

    #[test]
    fn put_and_get_at_runtime_sizes() {
        let base = scratch("basic");
        cleanup(&base);
        let c = RawLruCache::create(&base, 4, 8, 16).expect("created");
        assert_eq!(c.key_size(), 8);
        assert_eq!(c.value_size(), 16);

        let key = [1u8; 8];
        let value = [7u8; 16];
        assert!(!c.put(&key, &value).expect("put"), "the key was not there before");
        assert_eq!(c.len(), 1);

        let mut out = [0u8; 16];
        assert!(c.get(&key, &mut out).expect("get"));
        assert_eq!(out, value);

        // Putting the same key again replaces rather than adds.
        let replacement = [9u8; 16];
        assert!(c.put(&key, &replacement).expect("put"), "the key was there");
        assert_eq!(c.len(), 1);
        assert!(c.get(&key, &mut out).expect("get"));
        assert_eq!(out, replacement);

        cleanup(&base);
    }

    #[test]
    fn a_slice_of_the_wrong_size_is_refused_rather_than_padded() {
        let base = scratch("sizes");
        cleanup(&base);
        let c = RawLruCache::create(&base, 4, 8, 16).expect("created");
        let short = [1u8; 4];
        let value = [0u8; 16];
        assert!(matches!(
            c.put(&short, &value),
            Err(RawLruError::WrongSize { expected: 8, found: 4 })
        ));
        let mut small_out = [0u8; 8];
        assert!(matches!(
            c.get(&[1u8; 8], &mut small_out),
            Err(RawLruError::WrongSize { expected: 16, found: 8 })
        ));
        cleanup(&base);
    }

    #[test]
    fn the_least_recently_used_entry_is_the_one_evicted() {
        let base = scratch("evict");
        cleanup(&base);
        let c = RawLruCache::create(&base, 3, 8, 8).expect("created");
        for i in 0u64..3 {
            c.put(&i.to_le_bytes(), &(i * 10).to_le_bytes()).expect("put");
        }
        assert_eq!(c.len(), 3);

        // Touch the oldest so it is no longer the oldest.
        assert!(c.touch(&0u64.to_le_bytes()).expect("touch"));

        // The fourth entry evicts key 1, which is now the least recent.
        c.put(&3u64.to_le_bytes(), &30u64.to_le_bytes()).expect("put");
        assert_eq!(c.len(), 3);

        let mut out = [0u8; 8];
        assert!(c.get(&0u64.to_le_bytes(), &mut out).expect("get"), "touched, so kept");
        assert!(!c.get(&1u64.to_le_bytes(), &mut out).expect("get"), "evicted");
        assert!(c.get(&2u64.to_le_bytes(), &mut out).expect("get"));
        assert!(c.get(&3u64.to_le_bytes(), &mut out).expect("get"));

        cleanup(&base);
    }

    #[test]
    fn remove_hands_back_the_value_and_forgets_the_key() {
        let base = scratch("remove");
        cleanup(&base);
        let c = RawLruCache::create(&base, 4, 8, 8).expect("created");
        c.put(&1u64.to_le_bytes(), &42u64.to_le_bytes()).expect("put");
        let mut out = [0u8; 8];
        assert!(c.remove(&1u64.to_le_bytes(), &mut out).expect("remove"));
        assert_eq!(u64::from_le_bytes(out), 42);
        assert!(!c.remove(&1u64.to_le_bytes(), &mut out).expect("remove"), "already gone");
        assert_eq!(c.len(), 0);
        cleanup(&base);
    }

    #[test]
    fn evicting_an_empty_cache_says_so_rather_than_failing() {
        let base = scratch("empty");
        cleanup(&base);
        let c = RawLruCache::create(&base, 2, 8, 8).expect("created");
        let mut k = [0u8; 8];
        let mut v = [0u8; 8];
        assert!(!c.evict_oldest(&mut k, &mut v).expect("evict"));
        cleanup(&base);
    }

    #[test]
    fn a_capacity_of_zero_is_refused() {
        let base = scratch("zerocap");
        cleanup(&base);
        assert!(matches!(
            RawLruCache::create(&base, 0, 8, 8),
            Err(RawLruError::ZeroCapacity)
        ));
        cleanup(&base);
    }

    #[test]
    fn a_second_handle_on_the_same_files_sees_the_same_entries() {
        let base = scratch("shared");
        cleanup(&base);
        let a = RawLruCache::create(&base, 4, 8, 8).expect("created");
        a.put(&5u64.to_le_bytes(), &55u64.to_le_bytes()).expect("put");

        let b = RawLruCache::open(&base, 4, 8, 8).expect("opened");
        let mut out = [0u8; 8];
        assert!(b.get(&5u64.to_le_bytes(), &mut out).expect("get"));
        assert_eq!(u64::from_le_bytes(out), 55);

        // And a write through the second is seen by the first.
        b.put(&6u64.to_le_bytes(), &66u64.to_le_bytes()).expect("put");
        assert!(a.get(&6u64.to_le_bytes(), &mut out).expect("get"));
        assert_eq!(u64::from_le_bytes(out), 66);

        cleanup(&base);
    }
}
