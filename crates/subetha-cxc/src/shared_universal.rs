//! `SharedUniversal<T>` - Layer-2 cross-process container that
//! migrates between Shared* backings as the workload shape changes.
//!
//! # The architectural claim
//!
//! A single cross-process container that auto-swaps its backing
//! storage when the observed operation mix favors a different shape.
//! At creation time the container starts in `Vec` mode (cheap pushes,
//! O(N) `contains`). When `contains` calls dominate (the common case
//! for membership / dedup workloads), the container migrates to
//! `HashMap` mode (O(1) `contains`, slightly more expensive insert).
//! Subsequent peer reads observe the migration via a version bump in
//! the shared state header and transparently re-open the new backing.
//!
//! # The MVP scope
//!
//! - **2 backings only**: `SharedVec<T>` and `SharedHashMap<T, ()>`.
//!   The extension to 5 backings (SharedRing, SharedHandleTable,
//!   SharedBTreeMap, SharedTreiberStack) is its own bead.
//! - **Single-writer model**: ONE process holds the writer role and
//!   triggers migrations. Other processes are read-only observers
//!   that follow the strategy tag. Multi-writer voting protocol is
//!   ap-uvj.
//! - **Local policy**: the writer's local op histogram drives
//!   migration decisions. Quorum / cross-process voting is ap-uvj.
//!
//! # File layout
//!
//! Three coordinated files per logical container:
//!
//! ```text
//! <base>.state.bin           always; small header MMF
//! <base>-v{N}-vec.bin        current backing if strategy == Vec
//! <base>-v{N}-map.bin        current backing if strategy == Map
//! ```
//!
//! On migration: writer creates the new `-v{N+1}-{strategy}.bin`,
//! copies the snapshot, then bumps `state.bin`'s version+strategy
//! with a single CAS. Readers see the bump on their next op and
//! re-open transparently.
//!
//! # Concurrency model
//!
//! - Reader / writer ops take an INTERNAL `RwLock<Backing<T>>` on
//!   the handle (process-local; protects against the re-open race
//!   between two ops in the same process).
//! - Re-open is double-checked: re-read state.version under the
//!   write lock; if some other thread already re-opened, drop the
//!   write lock and use the current backing.
//! - Migration is ONLY safe from a single writer process. If two
//!   processes both try to migrate, both will succeed locally but
//!   race on the state CAS; the loser's new backing file is
//!   orphaned (cleanable). The voting protocol (ap-uvj) prevents
//!   this; the MVP documents the single-writer constraint.

use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};
use parking_lot::RwLock;

use crate::shared_hash_map::{MapError, SharedHashMap};
use crate::shared_vec::SharedVec;

pub const UNIVERSAL_MAGIC: u32 = 0x4150_5556;

/// Strategy tag: which backing is currently live.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Vec = 0,
    Map = 1,
}

impl Strategy {
    fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Vec),
            1 => Some(Self::Map),
            _ => None,
        }
    }

    fn file_suffix(self) -> &'static str {
        match self {
            Self::Vec => "vec",
            Self::Map => "map",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniversalError {
    InvalidStrategy,
    IoError(std::io::ErrorKind),
    LayoutMismatch,
    VecError,
    MapError(MapError),
    Full,
    /// `current_version + 1` overflows `u32`. After ~4 billion
    /// migrations on the same base, the container refuses further
    /// migrations rather than wrap version back to 0 (which then
    /// silently overwrites the v=0 backing).
    VersionExhausted,
}

impl From<std::io::Error> for UniversalError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}
impl From<MapError> for UniversalError {
    fn from(e: MapError) -> Self {
        match e {
            MapError::Full => Self::Full,
            other => Self::MapError(other),
        }
    }
}

#[repr(C, align(64))]
pub struct UniversalHeader {
    pub magic: u32,
    pub capacity: u32,
    /// Packed state, single AtomicU64 for atomic update / atomic
    /// reader-load:
    /// - bits 63..32 = `version: u32` (bumps per migration within
    ///   the current generation; wraps to 0 at u32::MAX)
    /// - bits 31..16 = `generation: u16` (bumps when version wraps;
    ///   ensures a reused (generation, version) pair NEVER appears
    ///   in the same lifetime, so readers comparing the full u64
    ///   state always observe wrap-around and re-open)
    /// - bits 15..0  = `strategy: u16` (low byte is the Strategy
    ///   discriminant; high byte reserved for strategy variants)
    ///
    /// True exhaustion: generation u16 AND version u32 both at MAX
    /// (= 2^48 = 281 trillion migrations). Returns VersionExhausted.
    pub state: AtomicU64,
    /// Bumped by every `insert`; consumed by the writer's local
    /// policy to decide when to migrate.
    pub insert_count: AtomicU64,
    /// Bumped by every `contains`; same role as `insert_count`.
    pub contains_count: AtomicU64,
    _pad: [u8; 32],
}

const _: () = {
    assert!(size_of::<UniversalHeader>() == 64);
};

#[inline]
fn pack(version: u32, generation: u16, strategy: u8) -> u64 {
    ((version as u64) << 32) | ((generation as u64) << 16) | (strategy as u64)
}
#[inline]
fn unpack(v: u64) -> (u32, u16, u8) {
    let version = (v >> 32) as u32;
    let generation = ((v >> 16) & 0xFFFF) as u16;
    let strategy = (v & 0xFF) as u8;
    (version, generation, strategy)
}

enum Backing<T: Copy + Eq + 'static> {
    Vec(SharedVec<T>),
    Map(SharedHashMap<T, ()>),
}

pub struct SharedUniversal<T: Copy + Eq + 'static> {
    base: PathBuf,
    capacity: usize,
    _state_file: std::fs::File,
    state_mmap: MmapMut,
    /// Holds `(version, generation, Backing)`. Used to detect when
    /// the shared state's (version, generation) pair has changed and
    /// the local backing handle needs to be re-opened.
    backing: RwLock<(u32, u16, Backing<T>)>,
    _phantom: PhantomData<T>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl<T: Copy + Eq + Send + 'static> Send for SharedUniversal<T> {}
unsafe impl<T: Copy + Eq + Sync + 'static> Sync for SharedUniversal<T> {}

impl<T: Copy + Eq + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for SharedUniversal<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + Eq + 'static> SharedUniversal<T> {
    fn state_path(base: &Path) -> PathBuf {
        let mut p = base.to_path_buf();
        let stem = p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        p.set_file_name(format!("{stem}.state.bin"));
        p
    }

    fn backing_path(base: &Path, generation: u16, version: u32, strategy: Strategy) -> PathBuf {
        let mut p = base.to_path_buf();
        let stem = p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        p.set_file_name(format!(
            "{stem}-g{generation}-v{version}-{}.bin",
            strategy.file_suffix(),
        ));
        p
    }

    /// Create a new container. Starts in Vec strategy at
    /// (generation=0, version=0).
    pub fn create(base: impl AsRef<Path>, capacity: usize) -> Result<Self, UniversalError> {
        let base = base.as_ref().to_path_buf();
        assert!(capacity >= 1);
        let state_p = Self::state_path(&base);
        let state_file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&state_p)?;
        state_file.set_len(size_of::<UniversalHeader>() as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(size_of::<UniversalHeader>()).map_mut(&state_file)? };
        let hdr = mmap.as_mut_ptr() as *mut UniversalHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<UniversalHeader>());
            (*hdr).magic = UNIVERSAL_MAGIC;
            (*hdr).capacity = capacity as u32;
            (*hdr).state.store(pack(0, 0, Strategy::Vec as u8), Ordering::Release);
        }
        let backing_p = Self::backing_path(&base, 0, 0, Strategy::Vec);
        let vec: SharedVec<T> = SharedVec::create(&backing_p, capacity)
            .map_err(|_| UniversalError::VecError)?;
        Ok(Self {
            base, capacity,
            _state_file: state_file,
            state_mmap: mmap,
            backing: RwLock::new((0, 0, Backing::Vec(vec))),
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Open an existing container. Reads the active (generation,
    /// version, strategy) from the state header and opens the
    /// matching backing file.
    pub fn open(base: impl AsRef<Path>, capacity: usize) -> Result<Self, UniversalError> {
        let base = base.as_ref().to_path_buf();
        let state_p = Self::state_path(&base);
        let state_file = OpenOptions::new().read(true).write(true).open(&state_p)?;
        if state_file.metadata()?.len() < size_of::<UniversalHeader>() as u64 {
            return Err(UniversalError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(size_of::<UniversalHeader>()).map_mut(&state_file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const UniversalHeader) };
        if hdr.magic != UNIVERSAL_MAGIC || hdr.capacity != capacity as u32 {
            return Err(UniversalError::LayoutMismatch);
        }
        let (version, generation, strategy_byte) = unpack(hdr.state.load(Ordering::Acquire));
        let strategy = Strategy::from_u8(strategy_byte).ok_or(UniversalError::InvalidStrategy)?;
        let backing = Self::open_backing(&base, generation, version, strategy, capacity)?;
        Ok(Self {
            base, capacity,
            _state_file: state_file,
            state_mmap: mmap,
            backing: RwLock::new((version, generation, backing)),
            _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn open_backing(
        base: &Path, generation: u16, version: u32, strategy: Strategy, capacity: usize,
    ) -> Result<Backing<T>, UniversalError> {
        let p = Self::backing_path(base, generation, version, strategy);
        match strategy {
            Strategy::Vec => {
                let v: SharedVec<T> = SharedVec::open(&p, capacity)
                    .map_err(|_| UniversalError::VecError)?;
                Ok(Backing::Vec(v))
            }
            Strategy::Map => {
                let m: SharedHashMap<T, ()> = SharedHashMap::open(&p, capacity)?;
                Ok(Backing::Map(m))
            }
        }
    }

    fn header(&self) -> &UniversalHeader {
        unsafe { &*(self.state_mmap.as_ptr() as *const UniversalHeader) }
    }

    /// The strategy currently active in shared state. May differ from
    /// the locally-held backing if another writer just migrated; the
    /// next op call will re-open transparently.
    pub fn strategy(&self) -> Strategy {
        let (_, _, s) = unpack(self.header().state.load(Ordering::Acquire));
        Strategy::from_u8(s).expect("invalid strategy byte in state header")
    }

    /// The shared strategy version. Bumps on every migration; wraps
    /// to 0 at u32::MAX with the generation counter incrementing.
    pub fn strategy_version(&self) -> u32 {
        unpack(self.header().state.load(Ordering::Acquire)).0
    }

    /// The shared generation counter. Bumps each time `version`
    /// wraps from u32::MAX back to 0. Together with `version` it
    /// forms the true monotonic migration counter.
    pub fn strategy_generation(&self) -> u16 {
        unpack(self.header().state.load(Ordering::Acquire)).1
    }

    /// Re-open the local backing handle if the shared state's
    /// (version, generation) pair differs from the locally cached
    /// pair. Comparing both fields means a wrap-around (same version
    /// at a new generation) ALSO triggers re-open, preventing the
    /// stale-reader race where a reused version points at new
    /// content. Double-checked so concurrent ops don't trample each
    /// other.
    fn refresh_backing_if_stale(&self) -> Result<(), UniversalError> {
        let (shared_v, shared_g, _) = unpack(self.header().state.load(Ordering::Acquire));
        {
            let g = self.backing.read();
            if g.0 == shared_v && g.1 == shared_g { return Ok(()); }
        }
        let mut g = self.backing.write();
        let (shared_v2, shared_g2, shared_s_byte2) =
            unpack(self.header().state.load(Ordering::Acquire));
        if g.0 == shared_v2 && g.1 == shared_g2 { return Ok(()); }
        let strategy = Strategy::from_u8(shared_s_byte2).ok_or(UniversalError::InvalidStrategy)?;
        let new_backing = Self::open_backing(
            &self.base, shared_g2, shared_v2, strategy, self.capacity,
        )?;
        *g = (shared_v2, shared_g2, new_backing);
        Ok(())
    }

    /// Insert `value`. For Vec strategy this is push_back; for Map
    /// strategy this is insert(value, ()).
    pub fn insert(&self, value: T) -> Result<(), UniversalError>
    where T: std::hash::Hash,
    {
        self.refresh_backing_if_stale()?;
        let g = self.backing.read();
        let r: Result<(), UniversalError> = match &g.2 {
            Backing::Vec(v) => {
                v.push_back(value).map_err(|_| UniversalError::Full).map(|_| ())
            }
            Backing::Map(m) => {
                m.insert(value, ()).map(|_| ()).map_err(Into::into)
            }
        };
        self.header().insert_count.fetch_add(1, Ordering::Relaxed);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::universal::OP_INSERT,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    /// Membership check. Bumps the contains counter so the local
    /// policy can observe contains-heavy workloads.
    pub fn contains(&self, value: &T) -> Result<bool, UniversalError>
    where T: std::hash::Hash,
    {
        self.refresh_backing_if_stale()?;
        let g = self.backing.read();
        let hit = match &g.2 {
            Backing::Vec(v) => v.snapshot().iter().any(|x| x == value),
            Backing::Map(m) => m.contains_key(value),
        };
        self.header().contains_count.fetch_add(1, Ordering::Relaxed);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::universal::OP_CONTAINS,
            if hit { 0 } else { 2 },
        );
        Ok(hit)
    }

    /// Number of live entries.
    pub fn len(&self) -> Result<usize, UniversalError> {
        self.refresh_backing_if_stale()?;
        let g = self.backing.read();
        Ok(match &g.2 {
            Backing::Vec(v) => v.len(),
            Backing::Map(m) => m.len(),
        })
    }

    pub fn is_empty(&self) -> Result<bool, UniversalError> {
        Ok(self.len()? == 0)
    }

    /// Reset the universal to empty: clears whichever backing is
    /// currently live (Vec or Map). Does not change the strategy.
    /// Useful for steady-state benches that need to reset accumulated
    /// state between iterations. Not thread-safe with concurrent
    /// insert/remove from other threads.
    pub fn clear(&self) -> Result<(), UniversalError> {
        self.refresh_backing_if_stale()?;
        let g = self.backing.read();
        match &g.2 {
            Backing::Vec(v) => v.clear(),
            Backing::Map(m) => m.clear(),
        }
        Ok(())
    }

    /// Snapshot all live values into a `Vec<T>`. Best-effort under
    /// concurrent writers.
    pub fn snapshot(&self) -> Result<Vec<T>, UniversalError> {
        self.refresh_backing_if_stale()?;
        let g = self.backing.read();
        Ok(match &g.2 {
            Backing::Vec(v) => v.snapshot(),
            Backing::Map(m) => m.snapshot().into_iter().map(|(k, _)| k).collect(),
        })
    }

    /// Operation counts since creation. The writer's policy code
    /// reads these to decide when to migrate.
    pub fn op_histogram(&self) -> (u64, u64) {
        let hdr = self.header();
        (
            hdr.insert_count.load(Ordering::Acquire),
            hdr.contains_count.load(Ordering::Acquire),
        )
    }

    /// Force a migration to `target`. Snapshots the current backing,
    /// creates a new backing file at version+1, restores the snapshot,
    /// then publishes the new (version, strategy) via Release CAS.
    ///
    /// # Concurrency
    ///
    /// **Single-writer ONLY.** Two processes calling `migrate_to`
    /// concurrently will both build new backings and race on the CAS;
    /// the loser orphans its backing file. Use ap-uvj's voting
    /// protocol to coordinate when multiple writers are involved.
    pub fn migrate_to(&self, target: Strategy) -> Result<(), UniversalError>
    where T: std::hash::Hash,
    {
        let r = self.migrate_to_inner(target);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::universal::OP_MIGRATE,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn migrate_to_inner(&self, target: Strategy) -> Result<(), UniversalError>
    where T: std::hash::Hash,
    {
        let current_state = self.header().state.load(Ordering::Acquire);
        let (current_v, current_g, current_s) = unpack(current_state);
        let current = Strategy::from_u8(current_s).ok_or(UniversalError::InvalidStrategy)?;
        if current == target { return Ok(()); }
        // Bump version; on overflow, bump generation and reset
        // version to 0. True exhaustion (both at max) returns
        // VersionExhausted - that ceiling is 2^48 = 281 trillion
        // migrations on the same base.
        let (new_v, new_g) = match current_v.checked_add(1) {
            Some(v) => (v, current_g),
            None => {
                let next_g = current_g.checked_add(1)
                    .ok_or(UniversalError::VersionExhausted)?;
                (0, next_g)
            }
        };
        let snap = self.snapshot()?;
        let mut g = self.backing.write();
        let new_p = Self::backing_path(&self.base, new_g, new_v, target);
        // Build the new backing inside a closure so any error path
        // can clean up the partially-created file before returning.
        let build_result: Result<Backing<T>, UniversalError> = (|| {
            match target {
                Strategy::Vec => {
                    let v: SharedVec<T> = SharedVec::create(&new_p, self.capacity)
                        .map_err(|_| UniversalError::VecError)?;
                    for x in &snap {
                        v.push_back(*x).map_err(|_| UniversalError::Full)?;
                    }
                    Ok(Backing::Vec(v))
                }
                Strategy::Map => {
                    let m: SharedHashMap<T, ()> = SharedHashMap::create(&new_p, self.capacity)?;
                    for x in &snap { m.insert(*x, ())?; }
                    Ok(Backing::Map(m))
                }
            }
        })();
        let new_backing = match build_result {
            Ok(b) => b,
            Err(e) => {
                std::fs::remove_file(&new_p).ok();
                return Err(e);
            }
        };
        let new_state = pack(new_v, new_g, target as u8);
        match self.header().state.compare_exchange(
            current_state, new_state, Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => {
                *g = (new_v, new_g, new_backing);
                Ok(())
            }
            Err(_) => {
                drop(new_backing);
                std::fs::remove_file(&new_p).ok();
                Err(UniversalError::VecError)
            }
        }
    }

    /// Local-policy migration trigger. If the observed `contains` ops
    /// outnumber `insert` ops by at least `contains_to_insert_ratio`,
    /// AND total ops exceed `min_total_ops`, migrate Vec → Map. If
    /// the inverse holds, migrate Map → Vec.
    ///
    /// Returns `Ok(Some(new_strategy))` if a migration happened,
    /// `Ok(None)` if no policy threshold was crossed.
    pub fn maybe_migrate_by_policy(
        &self,
        contains_to_insert_ratio: f64,
        min_total_ops: u64,
    ) -> Result<Option<Strategy>, UniversalError>
    where T: std::hash::Hash,
    {
        let (ins, cnt) = self.op_histogram();
        if ins + cnt < min_total_ops { return Ok(None); }
        let current = self.strategy();
        let ratio = cnt as f64 / (ins.max(1)) as f64;
        let want = if ratio >= contains_to_insert_ratio { Strategy::Map } else { Strategy::Vec };
        if want == current { return Ok(None); }
        self.migrate_to(want)?;
        Ok(Some(want))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-universal-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path) {
        let stem = base.file_name().unwrap().to_string_lossy().to_string();
        let parent = base.parent().unwrap_or_else(|| Path::new(""));
        if let Ok(entries) = std::fs::read_dir(parent) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if name.starts_with(&stem) {
                    std::fs::remove_file(e.path()).ok();
                }
            }
        }
    }

    #[test]
    fn create_starts_in_vec_strategy() {
        let base = tmp_base("starts-vec");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 64).unwrap();
        assert_eq!(u.strategy(), Strategy::Vec);
        assert_eq!(u.strategy_version(), 0);
        assert_eq!(u.len().unwrap(), 0);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn insert_and_contains_round_trip_in_vec_mode() {
        let base = tmp_base("vec-roundtrip");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 64).unwrap();
        for k in 0..10u64 { u.insert(k).unwrap(); }
        for k in 0..10u64 { assert!(u.contains(&k).unwrap()); }
        assert!(!u.contains(&999).unwrap());
        assert_eq!(u.len().unwrap(), 10);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn explicit_migrate_to_map_preserves_contents() {
        let base = tmp_base("explicit-map");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 64).unwrap();
        for k in 0..10u64 { u.insert(k).unwrap(); }
        u.migrate_to(Strategy::Map).unwrap();
        assert_eq!(u.strategy(), Strategy::Map);
        assert_eq!(u.strategy_version(), 1);
        for k in 0..10u64 { assert!(u.contains(&k).unwrap()); }
        assert_eq!(u.len().unwrap(), 10);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn migrate_back_and_forth_preserves_contents() {
        let base = tmp_base("round-trip");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 64).unwrap();
        for k in 0..5u64 { u.insert(k).unwrap(); }
        u.migrate_to(Strategy::Map).unwrap();
        u.migrate_to(Strategy::Vec).unwrap();
        u.migrate_to(Strategy::Map).unwrap();
        assert_eq!(u.strategy(), Strategy::Map);
        assert_eq!(u.strategy_version(), 3);
        let mut snap = u.snapshot().unwrap();
        snap.sort();
        assert_eq!(snap, vec![0, 1, 2, 3, 4]);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn migrate_to_same_strategy_is_noop() {
        let base = tmp_base("same");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 16).unwrap();
        u.migrate_to(Strategy::Vec).unwrap();
        assert_eq!(u.strategy_version(), 0);
        u.migrate_to(Strategy::Map).unwrap();
        u.migrate_to(Strategy::Map).unwrap();
        assert_eq!(u.strategy_version(), 1);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn local_policy_migrates_to_map_under_contains_load() {
        let base = tmp_base("policy-map");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 64).unwrap();
        for k in 0..5u64 { u.insert(k).unwrap(); }
        // 100 contains, 5 inserts → ratio = 20, well above 0.5
        for _ in 0..100 { u.contains(&3).unwrap(); }
        let migrated = u.maybe_migrate_by_policy(0.5, 100).unwrap();
        assert_eq!(migrated, Some(Strategy::Map));
        assert_eq!(u.strategy(), Strategy::Map);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn local_policy_keeps_vec_under_insert_load() {
        let base = tmp_base("policy-vec");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 256).unwrap();
        for k in 0..200u64 { u.insert(k).unwrap(); }
        for _ in 0..10 { u.contains(&3).unwrap(); }
        let migrated = u.maybe_migrate_by_policy(0.5, 100).unwrap();
        assert_eq!(migrated, None);
        assert_eq!(u.strategy(), Strategy::Vec);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn reader_handle_observes_migration_via_version_bump() {
        // Writer process equivalent: SharedUniversal::create.
        // Reader process equivalent: SharedUniversal::open against
        // the same base. After writer migrates, reader's next op
        // must transparently re-open the new backing.
        let base = tmp_base("cross-handle");
        let writer: SharedUniversal<u64> = SharedUniversal::create(&base, 32).unwrap();
        let reader: SharedUniversal<u64> = SharedUniversal::open(&base, 32).unwrap();
        for k in 0..5u64 { writer.insert(k).unwrap(); }
        // Reader sees the inserts in Vec mode.
        assert_eq!(reader.strategy(), Strategy::Vec);
        assert_eq!(reader.len().unwrap(), 5);
        // Writer migrates.
        writer.migrate_to(Strategy::Map).unwrap();
        // Reader's next op transparently re-opens.
        assert_eq!(reader.strategy(), Strategy::Map);
        assert_eq!(reader.strategy_version(), 1);
        assert_eq!(reader.len().unwrap(), 5);
        for k in 0..5u64 { assert!(reader.contains(&k).unwrap()); }
        drop(writer);
        drop(reader);
        cleanup(&base);
    }

    #[test]
    fn snapshot_preserves_through_migration() {
        let base = tmp_base("snap");
        let u: SharedUniversal<u32> = SharedUniversal::create(&base, 32).unwrap();
        for k in [10u32, 5, 7, 1, 99] { u.insert(k).unwrap(); }
        let mut pre = u.snapshot().unwrap();
        pre.sort();
        u.migrate_to(Strategy::Map).unwrap();
        let mut post = u.snapshot().unwrap();
        post.sort();
        assert_eq!(pre, post);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn op_histogram_tracks_real_ops() {
        let base = tmp_base("hist");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 32).unwrap();
        u.insert(1).unwrap();
        u.insert(2).unwrap();
        u.insert(3).unwrap();
        u.contains(&2).unwrap();
        u.contains(&2).unwrap();
        let (ins, cnt) = u.op_histogram();
        assert_eq!(ins, 3);
        assert_eq!(cnt, 2);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn pack_unpack_round_trips_all_three_fields() {
        // version, generation, strategy round-trip exactly through
        // the u64 state encoding.
        let v: u32 = 0xDEAD_BEEF;
        let g: u16 = 0xCAFE;
        let s: u8 = Strategy::Map as u8;
        let packed = pack(v, g, s);
        let (rv, rg, rs) = unpack(packed);
        assert_eq!(rv, v);
        assert_eq!(rg, g);
        assert_eq!(rs, s);
    }

    #[test]
    fn starts_at_generation_zero() {
        let base = tmp_base("gen-zero");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 16).unwrap();
        assert_eq!(u.strategy_version(), 0);
        assert_eq!(u.strategy_generation(), 0);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn migration_within_generation_keeps_generation_zero() {
        let base = tmp_base("same-gen");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 16).unwrap();
        u.migrate_to(Strategy::Map).unwrap();
        u.migrate_to(Strategy::Vec).unwrap();
        u.migrate_to(Strategy::Map).unwrap();
        assert_eq!(u.strategy_version(), 3);
        assert_eq!(u.strategy_generation(), 0);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn version_wrap_bumps_generation_and_resets_version() {
        // Synthesize a near-wrap state by creating a backing file
        // at (g=0, v=u32::MAX, Vec) on disk, pointing the state
        // header there, and forcing the local backing to re-open
        // at that synthetic state. Then migrate once and verify
        // version wraps to 0 and generation bumps to 1.
        let base = tmp_base("wrap-gen");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 16).unwrap();
        let synth_p = SharedUniversal::<u64>::backing_path(
            u.base.as_path(), 0, u32::MAX, Strategy::Vec,
        );
        // Pre-create the synthetic backing file so refresh_backing
        // can open it.
        let synth: SharedVec<u64> = SharedVec::create(&synth_p, 16).unwrap();
        drop(synth);
        u.header().state.store(
            pack(u32::MAX, 0, Strategy::Vec as u8),
            Ordering::Release,
        );
        u.refresh_backing_if_stale().unwrap();
        // Now migrate: version wraps to 0; generation bumps to 1.
        u.migrate_to(Strategy::Map).unwrap();
        assert_eq!(u.strategy_version(), 0);
        assert_eq!(u.strategy_generation(), 1);
        assert_eq!(u.strategy(), Strategy::Map);
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn true_exhaustion_returns_version_exhausted() {
        // generation = u16::MAX, version = u32::MAX → next migrate
        // can't bump either; returns VersionExhausted.
        let base = tmp_base("exhausted");
        let u: SharedUniversal<u64> = SharedUniversal::create(&base, 16).unwrap();
        u.header().state.store(
            pack(u32::MAX, u16::MAX, Strategy::Vec as u8),
            Ordering::Release,
        );
        u.refresh_backing_if_stale().unwrap_or(());
        let r = u.migrate_to(Strategy::Map);
        assert_eq!(r.err(), Some(UniversalError::VersionExhausted));
        drop(u);
        cleanup(&base);
    }

    #[test]
    fn reader_re_opens_on_generation_change_even_at_same_version() {
        // This is the load-bearing safety property: if a writer
        // wraps version back to 0 (bumping generation), an old
        // reader whose cached (v, g) is (0, 0) MUST re-open when
        // the shared state changes to (0, 1, new_strategy).
        let base = tmp_base("reader-gen");
        let writer: SharedUniversal<u64> = SharedUniversal::create(&base, 16).unwrap();
        let reader: SharedUniversal<u64> = SharedUniversal::open(&base, 16).unwrap();
        // Both at (v=0, g=0). Synthesize a wrap by writing the
        // post-wrap state directly + creating a matching backing.
        writer.insert(11).unwrap();
        writer.insert(22).unwrap();
        // Now wrap: pretend writer just completed a migration that
        // wrapped version to 0 and bumped generation to 1, with
        // strategy Map. Create the new-gen backing file the same
        // way migrate_to does, then publish state.
        let new_p = SharedUniversal::<u64>::backing_path(
            writer.base.as_path(), 1, 0, Strategy::Map,
        );
        let m: SharedHashMap<u64, ()> = SharedHashMap::create(&new_p, 16).unwrap();
        m.insert(99u64, ()).unwrap();
        drop(m);
        writer.header().state.store(
            pack(0, 1, Strategy::Map as u8),
            Ordering::Release,
        );
        // Reader's local backing is at (v=0, g=0). Without the
        // generation check, it sees "v=0 == 0, no re-open
        // needed" and return stale results. With the generation
        // check, refresh_backing_if_stale re-opens at (0, 1, Map).
        assert!(reader.contains(&99u64).unwrap());
        // Old keys are NOT in the new Map backing (it was created
        // fresh with only 99).
        assert!(!reader.contains(&11u64).unwrap());
        assert_eq!(reader.strategy(), Strategy::Map);
        assert_eq!(reader.strategy_version(), 0);
        assert_eq!(reader.strategy_generation(), 1);
        drop(writer);
        drop(reader);
        cleanup(&base);
    }
}
