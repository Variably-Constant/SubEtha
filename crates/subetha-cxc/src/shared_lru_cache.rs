//! `SharedLRUCache<K, V>` - cross-process LRU cache.
//!
//! Composite primitive demonstrating the layered-composition
//! thesis at full strength: combines
//! [`SharedHashMap<K, u32>`](crate::SharedHashMap) for O(1) lookup
//! with [`SharedLinkedList<(K, V)>`](crate::SharedLinkedList) for
//! O(1) move-to-front and O(1) eviction.
//!
//! # Files (3 per cache, all under a base path)
//!
//! - `<base>.map.bin`        - the SharedHashMap<K, u32>
//! - `<base>.list.<region>`  - SharedLinkedList's underlying region
//! - (the linked list is single-file; uses one MMF for the region)
//!
//! # Concurrency
//!
//! - `get` / `contains_key` / `snapshot_*` / `len`: **lock-free
//!   read paths**. Multi-reader safe at any concurrency. Does NOT
//!   promote MRU order.
//! - `touch` / `get_and_touch` / `put` / `remove` / `evict_oldest`:
//!   **single-writer** operations. Wrap in a SharedSemaphore(1) or
//!   the application's own coordination for cross-process writer
//!   serialisation.
//!
//! # Why split get vs touch
//!
//! Many production caches (tokio::sync MokaCache, Java Caffeine)
//! separate the "look up the value" path from the "promote to MRU"
//! path. Read-heavy workloads where LRU ordering is approximate get
//! the cheap path; strict LRU workloads call `get_and_touch`. This
//! lets the cache be useful in both regimes.
//!
//! # Eviction
//!
//! `put(k, v)` always succeeds when the underlying map has room.
//! If the cache is at capacity AND `k` is not already present, the
//! LRU entry (back of list) is evicted first via pop_back +
//! map.remove.
//!
//! # Long-running workload limit
//!
//! The underlying [`SharedHashMap`] is sized
//! to 8x the cache capacity to absorb tombstone accumulation from
//! eviction. After roughly 7x capacity insert-then-evict cycles,
//! tombstones fill the map and `put` returns `Map(Full)`. For
//! long-running workloads, either size the cache larger or wait for
//! the SharedHashMap.compact() reclamation primitive (separate
//! follow-on). For typical caches that hover near capacity, the
//! tombstone budget is far more than enough.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::shared_hash_map::{MapError, SharedHashMap};
use crate::shared_linked_list::{LinkedListError, NodeHandle, SharedLinkedList};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LRUError {
    Map(MapError),
    LinkedList(LinkedListError),
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<MapError> for LRUError {
    fn from(e: MapError) -> Self { Self::Map(e) }
}
impl From<LinkedListError> for LRUError {
    fn from(e: LinkedListError) -> Self { Self::LinkedList(e) }
}
impl From<std::io::Error> for LRUError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

fn map_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.map.bin"));
    p
}
fn list_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.list.bin"));
    p
}

pub struct SharedLRUCache<
    K: Copy + Eq + Default + 'static,
    V: Copy + Default + 'static,
> {
    map: SharedHashMap<K, u32>,
    list: SharedLinkedList<(K, V)>,
    capacity: u32,
    _phantom: PhantomData<(K, V)>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<
    K: Copy + Eq + Default + Send + Sync + 'static,
    V: Copy + Default + Send + Sync + 'static,
> subetha_sidecar::AdaptiveInstance for SharedLRUCache<K, V> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<
    K: Copy + Eq + Default + 'static,
    V: Copy + Default + 'static,
> SharedLRUCache<K, V> {
    /// Create a new LRU cache with `capacity` entries.
    ///
    /// SIZING: the underlying SharedHashMap is sized to 8x capacity
    /// to absorb tombstone accumulation (open-addressing leaves a
    /// tombstone on every remove; LRU caches do many removes via
    /// eviction). The linked list region is sized to capacity + 2
    /// (one sentinel head + capacity nodes + 1 spare for the
    /// pop-then-push transition during update).
    pub fn create(
        base_path: impl AsRef<Path>, capacity: u32,
    ) -> Result<Self, LRUError> {
        assert!(capacity >= 1);
        let base = base_path.as_ref();
        let map = SharedHashMap::<K, u32>::create(
            map_path(base), (capacity as usize * 8).max(32),
        )?;
        let list = SharedLinkedList::<(K, V)>::create(
            list_path(base), capacity as usize + 2,
        )?;
        Ok(Self {
            map, list, capacity,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        base_path: impl AsRef<Path>, capacity: u32,
    ) -> Result<Self, LRUError> {
        let base = base_path.as_ref();
        let map = SharedHashMap::<K, u32>::open(
            map_path(base), (capacity as usize * 8).max(32),
        )?;
        let list = SharedLinkedList::<(K, V)>::open(
            list_path(base), capacity as usize + 2,
        )?;
        Ok(Self {
            map, list, capacity,
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn capacity(&self) -> u32 { self.capacity }

    /// Current number of entries.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Lock-free lookup. Does NOT promote `k` to MRU position.
    /// Use [`get_and_touch`](Self::get_and_touch) or
    /// [`touch`](Self::touch) for strict LRU semantics.
    pub fn get(&self, key: &K) -> Option<V> {
        let r = (|| {
            let idx = self.map.get(key)?;
            let (k, v) = self.list.get(NodeHandle::new(idx))?;
            // Sanity: the list slot we looked up via the map MUST hold
            // the same key. If not, the cache is corrupted (shouldn't
            // happen since map and list are updated together in writer
            // ops). Return None defensively rather than asserting.
            if k != *key { return None; }
            Some(v)
        })();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lru_cache::OP_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// True if key is present (lock-free).
    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    /// Promote `key` to MRU position. Writer-side. Returns true if
    /// the key was present and was promoted.
    pub fn touch(&self, key: &K) -> bool {
        let promoted = (|| {
            let idx = self.map.get(key)?;
            let (k, v) = self.list.remove(NodeHandle::new(idx))?;
            // Push to front; get the new handle; update the map.
            match self.list.push_front((k, v)) {
                Ok(new_handle) => {
                    // Map insert may collide; map and list updates are
                    // separate writes so the lock-free LRU pattern
                    // already tolerates ordering anomalies here.
                    self.map.insert(k, new_handle.index).ok();
                    Some(true)
                }
                Err(_) => {
                    // Shouldn't happen since we just freed a slot, but
                    // be defensive: re-insert at the back.
                    self.list.push_back((k, v)).ok();
                    Some(false)
                }
            }
        })().unwrap_or(false);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lru_cache::OP_TOUCH,
            if promoted { 0 } else { 2 }, // 2 = key absent
        );
        promoted
    }

    /// Look up and promote in one call. Writer-side.
    pub fn get_and_touch(&self, key: &K) -> Option<V> {
        let v = self.get(key)?;
        self.touch(key);
        Some(v)
    }

    /// Insert / update. Writer-side. If the cache is at capacity
    /// AND `key` is new, evicts the LRU entry first. Returns the
    /// previous value if `key` was present.
    pub fn put(&self, key: K, value: V) -> Result<Option<V>, LRUError> {
        let r = self.put_inner(key, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lru_cache::OP_PUT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn put_inner(&self, key: K, value: V) -> Result<Option<V>, LRUError> {
        // Existing key: update in place + promote.
        if let Some(idx) = self.map.get(&key) {
            let old = self.list.remove(NodeHandle::new(idx))
                .map(|(_, v)| v);
            let new_handle = self.list.push_front((key, value))?;
            self.map.insert(key, new_handle.index)?;
            return Ok(old);
        }
        // New key: maybe evict.
        if self.len() >= self.capacity as usize {
            self.evict_oldest();
        }
        let new_handle = self.list.push_front((key, value))?;
        self.map.insert(key, new_handle.index)?;
        Ok(None)
    }

    /// Remove a key. Writer-side. Returns the value if present.
    pub fn remove(&self, key: &K) -> Option<V> {
        let r = (|| {
            let idx = self.map.remove(key)?;
            let (_, v) = self.list.remove(NodeHandle::new(idx))?;
            Some(v)
        })();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lru_cache::OP_REMOVE,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Evict the LRU (least-recently-used) entry. Writer-side.
    /// Returns (key, value) of the evicted entry, or None if empty.
    pub fn evict_oldest(&self) -> Option<(K, V)> {
        // Pop_back gives the LRU entry.
        let r = (|| {
            let (k, v) = self.list.pop_back()?;
            // Map may already be missing the key under concurrent races;
            // we just want the eviction to commit either way.
            let _removed = self.map.remove(&k);
            Some((k, v))
        })();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lru_cache::OP_EVICT,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// Snapshot all entries from MRU (front) to LRU (back).
    /// Lock-free; not stable under concurrent writers.
    pub fn snapshot_mru_first(&self) -> Vec<(K, V)> {
        self.list.iter_forward()
    }

    /// Snapshot all entries from LRU (back) to MRU (front).
    pub fn snapshot_lru_first(&self) -> Vec<(K, V)> {
        self.list.iter_backward()
    }

    pub fn flush(&self) -> Result<(), LRUError> {
        self.map.flush()?;
        self.list.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), LRUError> {
        self.map.flush_async()?;
        self.list.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-lru-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path) {
        std::fs::remove_file(map_path(base)).ok();
        std::fs::remove_file(list_path(base)).ok();
    }

    #[test]
    fn create_initial_state_is_empty() {
        let base = tmp_base("init");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 16).unwrap();
        assert_eq!(c.capacity(), 16);
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
        assert_eq!(c.get(&1), None);
        cleanup(&base);
    }

    #[test]
    fn put_get_round_trip() {
        let base = tmp_base("rt");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 16).unwrap();
        c.put(1, 100).unwrap();
        c.put(2, 200).unwrap();
        c.put(3, 300).unwrap();
        assert_eq!(c.get(&1), Some(100));
        assert_eq!(c.get(&2), Some(200));
        assert_eq!(c.get(&3), Some(300));
        assert_eq!(c.get(&999), None);
        assert_eq!(c.len(), 3);
        cleanup(&base);
    }

    #[test]
    fn put_existing_key_updates_and_promotes() {
        let base = tmp_base("update");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 16).unwrap();
        c.put(1, 100).unwrap();
        c.put(2, 200).unwrap();
        c.put(3, 300).unwrap();
        // Update key 1.
        let prev = c.put(1, 111).unwrap();
        assert_eq!(prev, Some(100));
        assert_eq!(c.get(&1), Some(111));
        assert_eq!(c.len(), 3);
        // Key 1 should now be at the front (MRU).
        let snap = c.snapshot_mru_first();
        assert_eq!(snap[0], (1, 111));
        cleanup(&base);
    }

    #[test]
    fn get_does_not_promote() {
        let base = tmp_base("get-no-promote");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 16).unwrap();
        c.put(1, 100).unwrap();
        c.put(2, 200).unwrap();
        c.put(3, 300).unwrap();
        // Snapshot order is push-front-order, so MRU = 3.
        let before = c.snapshot_mru_first();
        assert_eq!(before, vec![(3, 300), (2, 200), (1, 100)]);
        // Plain get on key 1 should NOT promote.
        c.get(&1).unwrap();
        let after = c.snapshot_mru_first();
        assert_eq!(after, vec![(3, 300), (2, 200), (1, 100)],
            "plain get must not change order");
        cleanup(&base);
    }

    #[test]
    fn touch_promotes_to_front() {
        let base = tmp_base("touch");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 16).unwrap();
        c.put(1, 100).unwrap();
        c.put(2, 200).unwrap();
        c.put(3, 300).unwrap();
        // Touch key 1: should move it to front.
        assert!(c.touch(&1));
        let snap = c.snapshot_mru_first();
        assert_eq!(snap, vec![(1, 100), (3, 300), (2, 200)]);
        cleanup(&base);
    }

    #[test]
    fn touch_nonexistent_returns_false() {
        let base = tmp_base("touch-none");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 8).unwrap();
        c.put(1, 100).unwrap();
        assert!(!c.touch(&999));
        cleanup(&base);
    }

    #[test]
    fn get_and_touch_combines_both() {
        let base = tmp_base("get-touch");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 8).unwrap();
        c.put(1, 100).unwrap();
        c.put(2, 200).unwrap();
        c.put(3, 300).unwrap();
        let v = c.get_and_touch(&1).unwrap();
        assert_eq!(v, 100);
        let snap = c.snapshot_mru_first();
        assert_eq!(snap[0], (1, 100));
        cleanup(&base);
    }

    #[test]
    fn eviction_at_capacity_drops_oldest() {
        let base = tmp_base("evict");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 3).unwrap();
        c.put(1, 10).unwrap();
        c.put(2, 20).unwrap();
        c.put(3, 30).unwrap();
        // Now full. Insert 4 -> should evict key 1 (LRU).
        let prev = c.put(4, 40).unwrap();
        assert_eq!(prev, None);
        assert_eq!(c.len(), 3);
        assert_eq!(c.get(&1), None, "key 1 should have been evicted");
        assert_eq!(c.get(&2), Some(20));
        assert_eq!(c.get(&3), Some(30));
        assert_eq!(c.get(&4), Some(40));
        cleanup(&base);
    }

    #[test]
    fn touch_prevents_eviction_of_recently_used() {
        let base = tmp_base("touch-evict");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 3).unwrap();
        c.put(1, 10).unwrap();
        c.put(2, 20).unwrap();
        c.put(3, 30).unwrap();
        // Touch key 1 to make it MRU.
        c.touch(&1);
        // Insert 4 -> should evict key 2 (now LRU since 1 was touched).
        c.put(4, 40).unwrap();
        assert_eq!(c.get(&1), Some(10), "touched key 1 should survive");
        assert_eq!(c.get(&2), None, "untouched key 2 should be evicted");
        assert_eq!(c.get(&3), Some(30));
        assert_eq!(c.get(&4), Some(40));
        cleanup(&base);
    }

    #[test]
    fn remove_cleans_both_map_and_list() {
        let base = tmp_base("rm");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 8).unwrap();
        c.put(1, 10).unwrap();
        c.put(2, 20).unwrap();
        c.put(3, 30).unwrap();
        let v = c.remove(&2).unwrap();
        assert_eq!(v, 20);
        assert_eq!(c.len(), 2);
        assert_eq!(c.get(&2), None);
        // Other keys still present.
        assert_eq!(c.get(&1), Some(10));
        assert_eq!(c.get(&3), Some(30));
        // snapshot shouldn't contain key 2.
        let snap = c.snapshot_mru_first();
        let keys: Vec<u32> = snap.iter().map(|(k, _)| *k).collect();
        assert!(!keys.contains(&2));
        cleanup(&base);
    }

    #[test]
    fn evict_oldest_returns_lru() {
        let base = tmp_base("evict-oldest");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 8).unwrap();
        c.put(1, 10).unwrap();
        c.put(2, 20).unwrap();
        c.put(3, 30).unwrap();
        let evicted = c.evict_oldest().unwrap();
        assert_eq!(evicted, (1, 10));
        assert_eq!(c.len(), 2);
        assert_eq!(c.get(&1), None);
        cleanup(&base);
    }

    #[test]
    fn evict_oldest_on_empty_returns_none() {
        let base = tmp_base("evict-empty");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 4).unwrap();
        assert_eq!(c.evict_oldest(), None);
        cleanup(&base);
    }

    #[test]
    fn snapshot_mru_and_lru_first_are_reverses() {
        let base = tmp_base("snap");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 8).unwrap();
        for (k, v) in [(1, 10), (2, 20), (3, 30)] {
            c.put(k, v).unwrap();
        }
        let mru = c.snapshot_mru_first();
        let mut lru = c.snapshot_lru_first();
        lru.reverse();
        assert_eq!(mru, lru);
        cleanup(&base);
    }

    #[test]
    fn cross_handle_visibility() {
        let base = tmp_base("cross-handle");
        let writer: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 8).unwrap();
        let reader: SharedLRUCache<u32, u32> = SharedLRUCache::open(&base, 8).unwrap();
        writer.put(42, 4242).unwrap();
        writer.put(7, 77).unwrap();
        assert_eq!(reader.get(&42), Some(4242));
        assert_eq!(reader.get(&7), Some(77));
        // reader.touch() also works (it's writer-side, but touch
        // is acceptable if the application coordinates writes).
        writer.evict_oldest();
        assert_eq!(reader.get(&42), None);  // 42 was LRU
        cleanup(&base);
    }

    #[test]
    fn struct_key_value_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
        #[repr(C)]
        struct UserKey { realm: u32, user: u32 }
        #[derive(Clone, Copy, Debug, PartialEq, Default)]
        #[repr(C)]
        struct Session { token: u64, expires_us: u64 }
        let base = tmp_base("struct");
        let c: SharedLRUCache<UserKey, Session> = SharedLRUCache::create(&base, 8).unwrap();
        let k = UserKey { realm: 1, user: 42 };
        let v = Session { token: 0xDEAD_BEEF, expires_us: 9_999_999_999 };
        c.put(k, v).unwrap();
        assert_eq!(c.get(&k), Some(v));
        cleanup(&base);
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let base = tmp_base("disk");
        {
            let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 8).unwrap();
            c.put(1, 100).unwrap();
            c.put(2, 200).unwrap();
            c.put(3, 300).unwrap();
            c.flush().unwrap();
        }
        let c2: SharedLRUCache<u32, u32> = SharedLRUCache::open(&base, 8).unwrap();
        assert_eq!(c2.len(), 3);
        assert_eq!(c2.get(&1), Some(100));
        assert_eq!(c2.get(&2), Some(200));
        assert_eq!(c2.get(&3), Some(300));
        cleanup(&base);
    }

    #[test]
    fn many_evictions_maintain_mru_correctness() {
        // Stress within the tombstone budget (8x capacity).
        // capacity=10 -> 80 map slots. 50 puts = 10 active + 40
        // tombstones = 50/80 = 62% load, within probe limit.
        let base = tmp_base("many-evict");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 10).unwrap();
        for k in 0..50u32 {
            c.put(k, k * 10).unwrap();
        }
        // After 50 puts into a 10-slot cache, exactly keys 40..50
        // should be present (the most-recently-inserted 10).
        assert_eq!(c.len(), 10);
        for k in 0..40u32 {
            assert_eq!(c.get(&k), None, "key {k} should have been evicted");
        }
        for k in 40..50u32 {
            assert_eq!(c.get(&k), Some(k * 10), "key {k} should be present");
        }
        cleanup(&base);
    }

    #[test]
    fn touched_keys_survive_subsequent_evictions() {
        // Insert N keys, touch a specific subset, then insert N more.
        // The touched keys should survive; the un-touched original
        // keys should be evicted.
        let base = tmp_base("touch-survival");
        let c: SharedLRUCache<u32, u32> = SharedLRUCache::create(&base, 5).unwrap();
        // Fill: keys 0..5 with 4 = MRU (push_front order).
        for k in 0..5u32 { c.put(k, k * 10).unwrap(); }
        // Touch keys 0 and 1 to make them MRU; order is now 1, 0,
        // then the remaining un-touched ones below.
        c.touch(&0);
        c.touch(&1);
        // Insert 3 new keys; LRU is now whatever wasn't touched.
        c.put(100, 1000).unwrap();
        c.put(101, 1010).unwrap();
        c.put(102, 1020).unwrap();
        // Touched keys 0 and 1 should survive.
        assert_eq!(c.get(&0), Some(0));
        assert_eq!(c.get(&1), Some(10));
        // New keys present.
        assert_eq!(c.get(&100), Some(1000));
        assert_eq!(c.get(&101), Some(1010));
        assert_eq!(c.get(&102), Some(1020));
        cleanup(&base);
    }
}
