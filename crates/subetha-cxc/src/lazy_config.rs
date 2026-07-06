//! `LazyConfig<T>` - thundering-herd-proof distributed config fetch.
//!
//! Composite primitive built on `SharedOnceCell<T>` + (optionally)
//! the BackgroundScheduler's Pass dispatch. Guarantees that the
//! config-fetch closure runs EXACTLY ONCE across all participating
//! processes - no matter how many of them call `get_or_fetch`
//! concurrently. The losers of the CAS race spin until the winner
//! publishes, then read the canonical value.
//!
//! # Why this exists
//!
//! Distributed services often want shared config (Consul, etcd,
//! DNS, an internal config service) loaded into every process once.
//! Naive implementations have every process independently fetch:
//! N processes -> N backend requests at startup, a classic
//! thundering herd against the config service. With LazyConfig,
//! one process fetches; all others see the result.
//!
//! # Two access modes
//!
//! 1. **Local fetcher**: `get_or_fetch(|| { ... })` runs the
//!    closure in the calling process if it wins the CAS; losers
//!    block until the winner publishes.
//!
//! 2. **Scheduler-dispatched fetcher** (extension): submit a Pass
//!    via the BackgroundScheduler with a registered closure_id.
//!    Any process registered for that closure_id can serve as the
//!    fetcher; the result is published via the SharedOnceCell. This
//!    allows the fetch to happen in a process dedicated to that
//!    role (e.g., a privileged process with network access) while
//!    other processes only block on the cell.
//!
//! Both modes share the same underlying CAS protocol, so the
//! thundering-herd-prevention property holds either way.

use std::path::Path;
use std::sync::Arc;

use crate::shared_once_cell::{SharedOnceCell, SharedOnceError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyConfigError {
    Once(SharedOnceError),
}

impl From<SharedOnceError> for LazyConfigError {
    fn from(e: SharedOnceError) -> Self { Self::Once(e) }
}

pub struct LazyConfig<T: Copy + Send + Sync + 'static> {
    cell: Arc<SharedOnceCell<T>>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<T: Copy + Send + Sync + 'static> subetha_sidecar::AdaptiveInstance for LazyConfig<T> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<T: Copy + Send + Sync + 'static> LazyConfig<T> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, LazyConfigError> {
        Ok(Self {
            cell: Arc::new(SharedOnceCell::create(path)?),
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, LazyConfigError> {
        Ok(Self {
            cell: Arc::new(SharedOnceCell::open(path)?),
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    /// Fast path: returns the value when loaded, otherwise None.
    pub fn try_get(&self) -> Option<T> {
        let r = self.cell.get();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lazy_config::OP_GET,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    /// True when the config has been loaded.
    pub fn is_loaded(&self) -> bool {
        self.cell.is_initialized()
    }

    /// Get the value if cached. Otherwise, race to be the canonical
    /// fetcher: the CAS winner runs `fetcher` and publishes; CAS
    /// losers block until the winner publishes, then return the
    /// canonical value.
    ///
    /// Across all processes mapping this file, `fetcher` runs at
    /// most once per process AND only one process's result becomes
    /// canonical. (In practice with the CAS-then-fetch protocol
    /// from `SharedOnceCell::get_or_init`, the winner is the
    /// only one to actually run the fetcher.)
    pub fn get_or_fetch<F: FnOnce() -> T>(&self, fetcher: F) -> T {
        let was_loaded = self.cell.is_initialized();
        let v = self.cell.get_or_init(fetcher);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lazy_config::OP_FETCH,
            if was_loaded { 0 } else { 1 }, // cold-fetch path
        );
        v
    }

    /// Force-set the value without going through a fetcher. Useful
    /// for testing or for an admin-set-config workflow. Returns
    /// `true` if this caller's value became canonical (it won the
    /// CAS), `false` when the cell was already initialised.
    pub fn force_set(&self, value: T) -> bool {
        let ok = self.cell.set(value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::lazy_config::OP_FETCH,
            if ok { 0 } else { 1 }, // lost the race
        );
        ok
    }

    pub fn flush(&self) -> Result<(), LazyConfigError> {
        Ok(self.cell.flush()?)
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), LazyConfigError> {
        Ok(self.cell.flush_async()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::thread;
    use std::time::Duration;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-lazyconf-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn unloaded_try_get_returns_none() {
        let p = tmp("unloaded");
        let c: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
        assert_eq!(c.try_get(), None);
        assert!(!c.is_loaded());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn first_fetch_loads_value() {
        let p = tmp("first");
        let c: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
        let v = c.get_or_fetch(|| 12345);
        assert_eq!(v, 12345);
        assert!(c.is_loaded());
        assert_eq!(c.try_get(), Some(12345));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn second_fetch_returns_cached() {
        let p = tmp("second");
        let c: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
        // Prime the cache; this fetcher must run.
        let _primed = c.get_or_fetch(|| 100);
        // Second fetcher must not run; if it ran, the panic fires.
        let v = c.get_or_fetch(|| panic!("must not run on loaded config"));
        assert_eq!(v, 100);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn fetch_runs_at_most_once_under_concurrency() {
        let p = tmp("concurrent");
        let c: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
        let cell = c.cell.clone();
        let runs = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];
        for _ in 0..16 {
            let cell = cell.clone();
            let runs = runs.clone();
            handles.push(thread::spawn(move || {
                cell.get_or_init(|| {
                    runs.fetch_add(1, Ordering::AcqRel);
                    // Simulate slow fetch so workers actually race.
                    thread::sleep(Duration::from_millis(5));
                    9999u64
                })
            }));
        }
        let results: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(results.iter().all(|v| *v == 9999));
        assert_eq!(runs.load(Ordering::Acquire), 1,
                   "fetcher must run EXACTLY once across 16 concurrent callers");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_load_visible() {
        let p = tmp("cross-handle");
        let a: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
        let b: LazyConfig<u64> = LazyConfig::open(&p).unwrap();
        // Process A fetches; B sees the result without running.
        let v_a = a.get_or_fetch(|| 7777);
        let v_b = b.get_or_fetch(|| panic!("must not run after A loaded"));
        assert_eq!(v_a, 7777);
        assert_eq!(v_b, 7777);
        assert!(b.is_loaded());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn force_set_wins_first_then_loses() {
        let p = tmp("force-set");
        let c: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
        assert!(c.force_set(42));
        assert!(!c.force_set(99));
        assert_eq!(c.try_get(), Some(42));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_loaded_config_survives_reopen() {
        let p = tmp("disk-persist");
        {
            let c: LazyConfig<u64> = LazyConfig::create(&p).unwrap();
            let _primed = c.get_or_fetch(|| 8888);
            c.flush().unwrap();
        }
        let c2: LazyConfig<u64> = LazyConfig::open(&p).unwrap();
        assert!(c2.is_loaded());
        assert_eq!(c2.try_get(), Some(8888));
        // Subsequent fetch returns cached value.
        let v = c2.get_or_fetch(|| panic!("must not run on already-loaded config"));
        assert_eq!(v, 8888);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn struct_config_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(C)]
        struct ConfigBytes { max_conns: u32, ttl_ms: u32, debug: u32 }
        let p = tmp("struct-config");
        let c: LazyConfig<ConfigBytes> = LazyConfig::create(&p).unwrap();
        let loaded = c.get_or_fetch(|| ConfigBytes {
            max_conns: 256,
            ttl_ms: 60_000,
            debug: 1,
        });
        assert_eq!(loaded, ConfigBytes { max_conns: 256, ttl_ms: 60_000, debug: 1 });
        std::fs::remove_file(&p).ok();
    }
}
