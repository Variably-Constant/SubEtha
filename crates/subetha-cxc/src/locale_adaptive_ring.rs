//! `LocaleAdaptiveRing`: same-host locale-axis morph for the ring
//! family.
//!
//! Wraps two `AdaptiveRing` instances - one anon-backed (fast, in-
//! process only), one file-backed (persistent, cross-process
//! visible) - behind an MMF-resident locale tag. Callers send/recv
//! through whichever locale is currently active; migration drains
//! one backing and pushes into the other, bumps the pin generation,
//! and flips the tag.
//!
//! The pin protocol mirrors `AdaptiveRing` (shape axis) and
//! `AdaptiveIpc` (protocol axis): one Acquire load on
//! `locale_generation`, captured at pin time, compared on every
//! validity check. `PinnedLocale::as_anon()` /
//! `PinnedLocale::as_file()` return `&AdaptiveRing` so the caller
//! chains directly into the existing shape-axis pin.
//!
//! Locale-axis ordering in the substrate:
//!
//! ```text
//!   LocaleAdaptiveRing                 (locale axis)
//!         |
//!         | pin_current_locale()
//!         v
//!   PinnedLocale { active = Anon | File }
//!         |
//!         | as_anon() / as_file() -> &AdaptiveRing
//!         v
//!   AdaptiveRing                       (shape axis)
//!         |
//!         | pin_current_shape()
//!         v
//!   PinnedRing { shape = Spsc | ... }
//!         |
//!         | spsc_try_push / mpmc_try_push / ...
//!         v
//!   SpscRingCore / SharedRing          (native primitive)
//! ```

use std::cell::Cell;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::adaptive_ring::{AdaptiveRing, ADAPTIVE_SPSC_PAYLOAD_BYTES};
use crate::ordering::{default_stamp_kind, OrderingMode};
use crate::shared_atomic::{SharedAtomicU32, SharedAtomicU64};
use crate::shared_ring::RingError;

/// The three locales a `LocaleAdaptiveRing` can hold its bytes at.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locale {
    /// In-process anonymous memory. No syscalls, no file system,
    /// no cross-process visibility. Cheapest send/recv.
    Anon = 0,
    /// MMF-backed file storage. Persisted to disk via the page
    /// cache, visible to any process that opens the file.
    File = 1,
    /// Named RAM-resident shared memory (ShmFs). Cross-process
    /// visible by name; never touches the page cache or disk.
    /// Sits between Anon (no cross-process) and File (page-cached
    /// + disk-persistent) on the locale axis.
    ShmFs = 2,
}

impl Locale {
    fn from_u32(tag: u32) -> Self {
        match tag {
            0 => Self::Anon,
            1 => Self::File,
            2 => Self::ShmFs,
            _ => panic!("LocaleAdaptiveRing locale_tag corrupted: {tag}"),
        }
    }
}

/// Two-locale ring with cross-process pin invalidation.
///
/// Pre-allocates both an anon backing and a file backing at
/// construction; the active locale is selected by a single
/// MMF-resident `locale_tag` and flipped via `migrate_to`.
pub struct LocaleAdaptiveRing {
    /// MMF-resident locale tag; one Acquire load per dispatched op.
    locale_tag: Arc<SharedAtomicU32>,
    /// MMF-resident generation; bumped on every migrate_to.
    locale_generation: Arc<SharedAtomicU64>,
    /// Anon-backed AdaptiveRing. Active when locale_tag == 0.
    anon: AdaptiveRing,
    /// File-backed AdaptiveRing. Active when locale_tag == 1.
    file: AdaptiveRing,
    /// ShmFs-backed AdaptiveRing. Active when locale_tag == 2.
    shmfs: AdaptiveRing,
    /// Base path for the file backing + the locale-control MMFs.
    /// The ShmFs backing derives its name prefix from this base.
    base_path: PathBuf,
}

unsafe impl Send for LocaleAdaptiveRing {}
unsafe impl Sync for LocaleAdaptiveRing {}

impl LocaleAdaptiveRing {
    /// Construct a two-locale ring. Both backings are pre-allocated.
    /// Initial locale is `Anon`. The file backing lives at
    /// `{base_path}.locale.file.ring.{spsc,mpsc.i,mpmc.i,vyukov}.bin`;
    /// the locale tag at `{base_path}.locale.tag.bin`; the
    /// generation at `{base_path}.locale.gen.bin`.
    pub fn create(
        base_path: impl Into<PathBuf>,
        max_producers: usize,
        max_consumers: usize,
        capacity: usize,
    ) -> Result<Self, RingError> {
        let base_path: PathBuf = base_path.into();

        let tag_path = with_suffix(&base_path, ".locale.tag.bin");
        let gen_path = with_suffix(&base_path, ".locale.gen.bin");
        let file_ring_prefix = with_suffix(&base_path, ".locale.file.ring");

        let locale_tag = Arc::new(
            SharedAtomicU32::create(&tag_path, 0)
                .map_err(|_| RingError::PayloadTooLarge)?,
        );
        let locale_generation = Arc::new(
            SharedAtomicU64::create(&gen_path, 0)
                .map_err(|_| RingError::PayloadTooLarge)?,
        );

        let anon =
            AdaptiveRing::create_anon(max_producers, max_consumers, capacity)?;
        let file = AdaptiveRing::create(
            &file_ring_prefix,
            max_producers,
            max_consumers,
            capacity,
        )?;
        // ShmFs backing: derive a name prefix from the base path so
        // a second process opening the same base_path resolves to
        // the same named shm regions.
        let shmfs_name_prefix = shmfs_name_prefix_for(&base_path);
        let shmfs = AdaptiveRing::create_shmfs(
            &shmfs_name_prefix,
            max_producers,
            max_consumers,
            capacity,
        )?;

        Ok(Self {
            locale_tag,
            locale_generation,
            anon,
            file,
            shmfs,
            base_path,
        })
    }

    /// As [`create`](Self::create) with ordering stamps on ALL
    /// THREE locale backings (one stamp kind picked once so the
    /// backings agree), letting the ordering axis compose with
    /// locale migrations. The ordering mode is applied to all three
    /// backings via [`set_ordering_mode`](Self::set_ordering_mode)
    /// so the discipline follows the ring across migrations.
    ///
    /// Locale migration of a merge-mode ring re-stamps items as the
    /// transfer drains them: the drain order IS stamp order under
    /// the merge, so the destination preserves global order. (Under
    /// `Unordered` the transfer's round-robin drain can reorder
    /// across producers - the same caveat as shape morphs.) The
    /// migrating thread auto-acquires the drainer lease; a live
    /// drainer in another process makes `migrate_to` fail with the
    /// transfer's `NotDrainer` error rather than corrupting order.
    pub fn create_with_ordering_stamps(
        base_path: impl Into<PathBuf>,
        max_producers: usize,
        max_consumers: usize,
        capacity: usize,
    ) -> Result<Self, RingError> {
        let base_path: PathBuf = base_path.into();
        let kind = default_stamp_kind();

        let tag_path = with_suffix(&base_path, ".locale.tag.bin");
        let gen_path = with_suffix(&base_path, ".locale.gen.bin");
        let file_ring_prefix = with_suffix(&base_path, ".locale.file.ring");

        let locale_tag = Arc::new(
            SharedAtomicU32::create(&tag_path, 0)
                .map_err(|_| RingError::PayloadTooLarge)?,
        );
        let locale_generation = Arc::new(
            SharedAtomicU64::create(&gen_path, 0)
                .map_err(|_| RingError::PayloadTooLarge)?,
        );

        let anon = AdaptiveRing::create_anon(max_producers, max_consumers, capacity)?
            .with_ordering_stamps_kind(kind)?;
        let file = AdaptiveRing::create(
            &file_ring_prefix, max_producers, max_consumers, capacity,
        )?
        .with_ordering_stamps_kind(kind)?;
        let shmfs_name_prefix = shmfs_name_prefix_for(&base_path);
        let shmfs = AdaptiveRing::create_shmfs(
            &shmfs_name_prefix, max_producers, max_consumers, capacity,
        )?
        .with_ordering_stamps_kind(kind)?;

        Ok(Self {
            locale_tag,
            locale_generation,
            anon,
            file,
            shmfs,
            base_path,
        })
    }

    /// Whether the backings carry ordering stamps.
    pub fn is_stamped(&self) -> bool {
        self.anon.is_stamped()
    }

    /// Live ordering mode of the ACTIVE locale backing (`None`
    /// when unstamped).
    pub fn ordering_mode(&self) -> Option<OrderingMode> {
        match self.current_locale() {
            Locale::Anon => self.anon.ordering_mode(),
            Locale::File => self.file.ordering_mode(),
            Locale::ShmFs => self.shmfs.ordering_mode(),
        }
    }

    /// Flip the ordering mode on ALL THREE backings so the
    /// discipline follows the ring across locale migrations.
    pub fn set_ordering_mode(&self, mode: OrderingMode) -> Result<(), RingError> {
        self.anon.set_ordering_mode(mode)?;
        self.file.set_ordering_mode(mode)?;
        self.shmfs.set_ordering_mode(mode)
    }

    /// Total cross-producer inversions observed across the three
    /// locale backings (each accumulates while it is the active
    /// locale).
    pub fn inversions(&self) -> u64 {
        self.anon.inversions() + self.file.inversions() + self.shmfs.inversions()
    }

    /// Current locale.
    pub fn current_locale(&self) -> Locale {
        Locale::from_u32(self.locale_tag.load(Ordering::Acquire))
    }

    /// Current pin generation. Pinned-locale handles capture this at
    /// pin time and call `is_still_valid()` to see whether a
    /// `migrate_to` has happened.
    pub fn locale_generation(&self) -> u64 {
        self.locale_generation.load(Ordering::Acquire)
    }

    /// Direct access to the anon backing.
    pub fn anon_ring(&self) -> &AdaptiveRing { &self.anon }

    /// Direct access to the file backing.
    pub fn file_ring(&self) -> &AdaptiveRing { &self.file }

    /// Direct access to the ShmFs backing.
    pub fn shmfs_ring(&self) -> &AdaptiveRing { &self.shmfs }

    /// Register a producer on ALL THREE locale backings so the
    /// active locale always has the registration regardless of which
    /// one is live. Returns the producer_id (same on all backings
    /// since they are sized identically and called in lockstep).
    pub fn register_producer(&self) -> Result<usize, crate::adaptive_ring::AdaptiveError> {
        let anon_id = self.anon.register_producer()?;
        let file_id = self.file.register_producer()?;
        let shmfs_id = self.shmfs.register_producer()?;
        assert_eq!(anon_id, file_id,
                   "LocaleAdaptiveRing producer registrations must stay in lockstep (anon vs file)");
        assert_eq!(anon_id, shmfs_id,
                   "LocaleAdaptiveRing producer registrations must stay in lockstep (anon vs shmfs)");
        Ok(anon_id)
    }

    /// Register a consumer on ALL THREE locale backings.
    pub fn register_consumer(&self) -> Result<usize, crate::adaptive_ring::AdaptiveError> {
        let anon_id = self.anon.register_consumer()?;
        let file_id = self.file.register_consumer()?;
        let shmfs_id = self.shmfs.register_consumer()?;
        assert_eq!(anon_id, file_id,
                   "LocaleAdaptiveRing consumer registrations must stay in lockstep (anon vs file)");
        assert_eq!(anon_id, shmfs_id,
                   "LocaleAdaptiveRing consumer registrations must stay in lockstep (anon vs shmfs)");
        Ok(anon_id)
    }

    /// Send a payload through the currently-active locale.
    pub fn try_send(&self, producer_id: usize, payload: &[u8]) -> Result<(), RingError> {
        let locale = Locale::from_u32(self.locale_tag.load(Ordering::Acquire));
        match locale {
            Locale::Anon => self.anon.try_send(producer_id, payload),
            Locale::File => self.file.try_send(producer_id, payload),
            Locale::ShmFs => self.shmfs.try_send(producer_id, payload),
        }
    }

    /// Receive a payload from the currently-active locale.
    pub fn try_recv(&self, consumer_id: usize, out: &mut [u8]) -> Result<usize, RingError> {
        let locale = Locale::from_u32(self.locale_tag.load(Ordering::Acquire));
        match locale {
            Locale::Anon => self.anon.try_recv(consumer_id, out),
            Locale::File => self.file.try_recv(consumer_id, out),
            Locale::ShmFs => self.shmfs.try_recv(consumer_id, out),
        }
    }

    /// Pin the current locale and return a [`PinnedLocale`] handle.
    pub fn pin_current_locale(&self) -> PinnedLocale<'_> {
        let captured_gen = self.locale_generation.load(Ordering::Acquire);
        let locale = Locale::from_u32(self.locale_tag.load(Ordering::Acquire));
        PinnedLocale {
            parent: self,
            pinned_generation: captured_gen,
            locale,
            _not_sync: PhantomData,
        }
    }

    /// Flip the active locale to `target`. Bumps `locale_generation`
    /// before flipping the tag so pin holders see invalidation on
    /// their next validity check. Transfers in-flight items from the
    /// old locale's backing into the new locale's backing.
    pub fn migrate_to(&self, target: Locale) -> Result<(), RingError> {
        let current = Locale::from_u32(self.locale_tag.load(Ordering::Acquire));
        if current == target {
            return Ok(());
        }
        self.locale_generation.fetch_add(1, Ordering::AcqRel);
        self.transfer_items(current, target)?;
        self.locale_tag.store(target as u32, Ordering::Release);
        Ok(())
    }

    fn transfer_items(&self, from: Locale, to: Locale) -> Result<(), RingError> {
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        // Consumer_id 0 drains across the active shape; producer_id
        // 0 pushes into the destination. For multi-consumer
        // workloads the destination's shape-axis sidecar
        // re-balances post-transfer.
        loop {
            let pop = match from {
                Locale::Anon => self.anon.try_recv(0, &mut buf),
                Locale::File => self.file.try_recv(0, &mut buf),
                Locale::ShmFs => self.shmfs.try_recv(0, &mut buf),
            };
            match pop {
                Ok(n) => {
                    // Push exactly the bytes the pop returned: 64
                    // for unstamped Lamport shapes, 56 for Vyukov
                    // and for stamped rings (whose try_recv strips
                    // the stamp and whose try_send re-stamps in
                    // drain order).
                    let push = match to {
                        Locale::Anon => self.anon.try_send(0, &buf[..n]),
                        Locale::File => self.file.try_send(0, &buf[..n]),
                        Locale::ShmFs => self.shmfs.try_send(0, &buf[..n]),
                    };
                    push?;
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

impl Drop for LocaleAdaptiveRing {
    fn drop(&mut self) {
        let tag_path = with_suffix(&self.base_path, ".locale.tag.bin");
        let gen_path = with_suffix(&self.base_path, ".locale.gen.bin");
        let file_prefix = with_suffix(&self.base_path, ".locale.file.ring");
        let max_p = self.file.max_producers();

        std::fs::remove_file(&tag_path).ok();
        std::fs::remove_file(&gen_path).ok();
        std::fs::remove_file(with_suffix(&file_prefix, ".spsc.bin")).ok();
        std::fs::remove_file(with_suffix(&file_prefix, ".vyukov.bin")).ok();
        std::fs::remove_file(with_suffix(&file_prefix, ".ordering.bin")).ok();
        for i in 0..max_p {
            std::fs::remove_file(
                with_suffix(&file_prefix, &format!(".mpsc.{i}.bin")),
            ).ok();
            std::fs::remove_file(
                with_suffix(&file_prefix, &format!(".mpmc.{i}.bin")),
            ).ok();
        }
    }
}

fn with_suffix(base: &Path, suffix: &str) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

fn shmfs_name_prefix_for(base: &Path) -> String {
    // Derive a logical name from the base path: strip directory
    // separators and use the final component as the shm name root.
    let stem = base.file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "locale_ring".to_string());
    format!("locale_{stem}")
}

/// Handle pinned to one locale of the parent [`LocaleAdaptiveRing`].
///
/// `as_anon()` / `as_file()` return `&AdaptiveRing` so the caller
/// chains directly into the shape-axis pin via
/// `AdaptiveRing::pin_current_shape()`.
pub struct PinnedLocale<'a> {
    parent: &'a LocaleAdaptiveRing,
    pinned_generation: u64,
    locale: Locale,
    _not_sync: PhantomData<Cell<()>>,
}

impl<'a> PinnedLocale<'a> {
    /// Locale this pin was captured at.
    pub fn locale(&self) -> Locale { self.locale }

    /// Pin generation captured at pin time.
    pub fn pinned_generation(&self) -> u64 { self.pinned_generation }

    /// One Acquire load comparing captured generation to the live
    /// one. `false` means a `migrate_to` has happened and the holder
    /// should release + re-acquire via `pin_current_locale()`.
    pub fn is_still_valid(&self) -> bool {
        self.parent.locale_generation.load(Ordering::Acquire)
            == self.pinned_generation
    }

    /// Returns `Some(&AdaptiveRing)` when the pin captured the anon
    /// locale, `None` otherwise.
    pub fn as_anon(&self) -> Option<&AdaptiveRing> {
        match self.locale {
            Locale::Anon => Some(&self.parent.anon),
            _ => None,
        }
    }

    /// Returns `Some(&AdaptiveRing)` when the pin captured the file
    /// locale, `None` otherwise.
    pub fn as_file(&self) -> Option<&AdaptiveRing> {
        match self.locale {
            Locale::File => Some(&self.parent.file),
            _ => None,
        }
    }

    /// Returns `Some(&AdaptiveRing)` when the pin captured the
    /// ShmFs locale, `None` otherwise.
    pub fn as_shmfs(&self) -> Option<&AdaptiveRing> {
        match self.locale {
            Locale::ShmFs => Some(&self.parent.shmfs),
            _ => None,
        }
    }
}

// ===================================================================
// Sidecar locale policy: serialised locale migrations with
// hysteresis. Mirrors the shape-morph
// `AdaptiveRingSidecar` / `DefaultRingShapePolicy` design and the
// capacity-morph `CapacityAdaptiveRingSidecar` /
// `DefaultCapacityPolicy`.
//
// Locale migrations are normally caller-driven: the application
// knows when it needs cross-process visibility (-> File / ShmFs)
// vs in-process only (-> Anon). The sidecar lets the application
// express that intent through a target-locale setter and the
// policy serialises the migration under a cooldown so rapid
// oscillation does not thrash the underlying transfer (every
// migration copies the in-flight items across backings).
// ===================================================================

use std::sync::atomic::{AtomicU32 as StdAtomicU32, AtomicU64 as StdAtomicU64};

/// A snapshot of the locale-adaptive ring's observable state
/// passed to a [`LocalePolicy`] on every sidecar scan.
#[derive(Debug, Clone, Copy)]
pub struct LocalePolicyObservation {
    /// Current active locale.
    pub current_locale: Locale,
    /// Locale the application has asked for. The default sidecar
    /// reads this through a per-sidecar atomic that the
    /// application updates via
    /// [`LocaleAdaptiveRingSidecar::request_locale`].
    pub requested_locale: Locale,
    /// Time since the last successful migration.
    pub since_last_migrate: std::time::Duration,
}

/// Policy that decides when (and to which locale) the sidecar
/// should migrate the [`LocaleAdaptiveRing`]. Returning
/// `Some(locale)` triggers `migrate_to(locale)`. Returning `None`
/// leaves the locale alone.
pub trait LocalePolicy: Send + Sync + 'static {
    fn decide(&self, observation: &LocalePolicyObservation) -> Option<Locale>;
}

/// Default locale policy: honour the application's requested
/// locale once `since_last_migrate >= hysteresis`. Default
/// hysteresis 250 ms (locale migrations cost more than shape
/// morphs because every in-flight item is copied across backings,
/// so the cooldown is longer than the shape-morph default).
pub struct DefaultLocalePolicy {
    pub hysteresis: std::time::Duration,
}

impl Default for DefaultLocalePolicy {
    fn default() -> Self {
        Self { hysteresis: std::time::Duration::from_millis(250) }
    }
}

impl LocalePolicy for DefaultLocalePolicy {
    fn decide(&self, obs: &LocalePolicyObservation) -> Option<Locale> {
        if obs.since_last_migrate < self.hysteresis {
            return None;
        }
        if obs.requested_locale == obs.current_locale {
            None
        } else {
            Some(obs.requested_locale)
        }
    }
}

/// Background scanner thread that drives locale migrations on a
/// [`LocaleAdaptiveRing`] from a [`LocalePolicy`].
///
/// The sidecar exposes a `request_locale(...)` setter that the
/// application calls to express intent ("I want this on shmfs
/// now"). The scanner samples this on every tick, builds a
/// [`LocalePolicyObservation`], asks the policy, and only migrates
/// when the policy returns `Some` AND the hysteresis cooldown has
/// elapsed.
pub struct LocaleAdaptiveRingSidecar {
    handle: Option<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    requested_locale: Arc<StdAtomicU32>,
    migrations_triggered: Arc<StdAtomicU64>,
}

impl LocaleAdaptiveRingSidecar {
    /// Spawn a sidecar thread that migrates `ring` according to
    /// `policy` decisions sampled every `scan_interval`. Initial
    /// requested locale is the ring's current locale (no migration
    /// until the application calls `request_locale`).
    pub fn spawn<P: LocalePolicy>(
        ring: Arc<LocaleAdaptiveRing>,
        policy: P,
        scan_interval: std::time::Duration,
    ) -> Self {
        Self::spawn_gated(
            ring,
            policy,
            scan_interval,
            crate::policy_gate::GateConfig::default(),
        )
    }

    /// As [`spawn`](Self::spawn) with a confidence gate between
    /// the locale policy's recommendation and the migration -
    /// defends against application-side request flapping (every
    /// migration copies the in-flight items across backings, so a
    /// flapping `request_locale` is expensive). Disabled (the
    /// default config) reproduces `spawn` exactly.
    pub fn spawn_gated<P: LocalePolicy>(
        ring: Arc<LocaleAdaptiveRing>,
        policy: P,
        scan_interval: std::time::Duration,
        gate_cfg: crate::policy_gate::GateConfig,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let requested_locale = Arc::new(StdAtomicU32::new(ring.current_locale() as u32));
        let migrations_triggered = Arc::new(StdAtomicU64::new(0));

        let stop_c = Arc::clone(&stop);
        let requested_c = Arc::clone(&requested_locale);
        let migrations_c = Arc::clone(&migrations_triggered);
        let handle = std::thread::spawn(move || {
            let mut last_migrate = std::time::Instant::now();
            let mut gate = crate::policy_gate::ConfidenceGate::new(gate_cfg);
            while !stop_c.load(Ordering::Acquire) {
                let req = Locale::from_u32(requested_c.load(Ordering::Acquire));
                let obs = LocalePolicyObservation {
                    current_locale: ring.current_locale(),
                    requested_locale: req,
                    since_last_migrate: last_migrate.elapsed(),
                };
                if let Some(target) = gate.observe(policy.decide(&obs))
                    && ring.migrate_to(target).is_ok()
                {
                    last_migrate = std::time::Instant::now();
                    migrations_c.fetch_add(1, Ordering::Relaxed);
                }
                std::thread::sleep(scan_interval);
            }
        });

        Self {
            handle: Some(handle),
            stop,
            requested_locale,
            migrations_triggered,
        }
    }

    /// Set the locale the application wants the ring to run at.
    /// The sidecar's policy decides when to actually migrate
    /// (typically: once the hysteresis cooldown elapses).
    pub fn request_locale(&self, target: Locale) {
        self.requested_locale.store(target as u32, Ordering::Release);
    }

    /// Number of successful migrations triggered by this sidecar
    /// since `spawn`.
    pub fn migrations_triggered(&self) -> u64 {
        self.migrations_triggered.load(Ordering::Relaxed)
    }

    /// Stop the sidecar thread and wait for it to exit.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            drop(h.join());
        }
    }
}

impl Drop for LocaleAdaptiveRingSidecar {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            drop(h.join());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RingShape;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("subetha_locale_{pid}_{nonce}_{name}"));
        p
    }

    #[test]
    fn create_starts_in_anon_locale() {
        let path = tmp("init");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        assert_eq!(r.current_locale(), Locale::Anon);
        assert_eq!(r.locale_generation(), 0);
    }

    #[test]
    fn round_trip_in_anon_locale() {
        let path = tmp("rt_anon");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");

        let payload = 0xDEADBEEFu64.to_le_bytes();
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        r.try_send(0, &payload).expect("send");
        let n = r.try_recv(0, &mut buf).expect("recv");
        assert!(n >= 8);
        assert_eq!(&buf[..8], &payload);
    }

    #[test]
    fn migrate_invalidates_outstanding_pin() {
        let path = tmp("migrate_invalidate");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");
        let pin = r.pin_current_locale();
        assert!(pin.is_still_valid());
        assert_eq!(pin.locale(), Locale::Anon);
        assert!(pin.as_anon().is_some());
        assert!(pin.as_file().is_none());

        r.migrate_to(Locale::File).expect("migrate");
        assert!(!pin.is_still_valid(), "pin must invalidate on locale flip");
        assert_eq!(r.current_locale(), Locale::File);

        let pin2 = r.pin_current_locale();
        assert!(pin2.is_still_valid());
        assert_eq!(pin2.locale(), Locale::File);
        assert!(pin2.as_file().is_some());
        assert!(pin2.as_anon().is_none());
    }

    #[test]
    fn migrate_to_same_locale_does_not_bump_generation() {
        let path = tmp("migrate_noop");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        let pin = r.pin_current_locale();
        let gen_before = pin.pinned_generation();
        r.migrate_to(Locale::Anon).expect("noop migrate");
        assert_eq!(r.locale_generation(), gen_before);
        assert!(pin.is_still_valid());
    }

    #[test]
    fn migrate_transfers_in_flight_items() {
        let path = tmp("migrate_transfer");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");

        // Push three items into the anon backing.
        for i in 0u64..3 {
            r.try_send(0, &i.to_le_bytes()).expect("send");
        }

        // Migrate to file - items should transfer.
        r.migrate_to(Locale::File).expect("migrate");
        assert_eq!(r.current_locale(), Locale::File);

        // Drain three items from the file backing.
        let mut got = Vec::new();
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for _ in 0..3 {
            let n = r.try_recv(0, &mut buf).expect("recv");
            assert!(n >= 8);
            got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
        }
        got.sort();
        assert_eq!(got, vec![0, 1, 2]);
    }

    #[test]
    fn pinned_chain_into_shape_axis() {
        let path = tmp("pin_chain");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");

        let pin_locale = r.pin_current_locale();
        let ring = pin_locale.as_anon().expect("pinned at anon");
        let pin_shape = ring.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);
        assert!(pin_locale.is_still_valid() && pin_shape.is_still_valid());

        let payload = 0x12345678u64.to_le_bytes();
        pin_shape.spsc_try_push(&payload).expect("native push");
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        let n = pin_shape.spsc_try_pop(&mut buf).expect("native pop");
        assert!(n >= 8);
        assert_eq!(&buf[..8], &payload);
    }

    #[test]
    fn migrate_to_shmfs_works() {
        let path = tmp("migrate_shmfs");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");
        let pin = r.pin_current_locale();
        assert_eq!(pin.locale(), Locale::Anon);
        assert!(pin.as_shmfs().is_none());

        r.migrate_to(Locale::ShmFs).expect("migrate to shmfs");
        assert!(!pin.is_still_valid());
        assert_eq!(r.current_locale(), Locale::ShmFs);

        let pin2 = r.pin_current_locale();
        assert_eq!(pin2.locale(), Locale::ShmFs);
        assert!(pin2.as_shmfs().is_some());
        assert!(pin2.as_anon().is_none());
        assert!(pin2.as_file().is_none());
    }

    #[test]
    fn round_trip_in_shmfs_locale() {
        let path = tmp("rt_shmfs");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");
        r.migrate_to(Locale::ShmFs).expect("migrate");

        let payload = 0xABCDEF12u64.to_le_bytes();
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        r.try_send(0, &payload).expect("send");
        let n = r.try_recv(0, &mut buf).expect("recv");
        assert!(n >= 8);
        assert_eq!(&buf[..8], &payload);
    }

    #[test]
    fn migrate_anon_to_shmfs_to_file_transfers_items() {
        let path = tmp("triple_morph");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");

        // Push at anon, morph to shmfs, push more, morph to file, drain.
        for i in 0u64..3 {
            r.try_send(0, &i.to_le_bytes()).expect("anon send");
        }
        r.migrate_to(Locale::ShmFs).expect("anon -> shmfs");
        for i in 100u64..103 {
            r.try_send(0, &i.to_le_bytes()).expect("shmfs send");
        }
        r.migrate_to(Locale::File).expect("shmfs -> file");

        // All 6 items should now be in the file backing.
        let mut got = Vec::new();
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        for _ in 0..6 {
            let n = r.try_recv(0, &mut buf).expect("recv");
            assert!(n >= 8);
            got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
        }
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 100, 101, 102]);
    }

    #[test]
    fn stamped_locale_ring_mode_follows_migrations() {
        let path = tmp("stamped_locale");
        let r = LocaleAdaptiveRing::create_with_ordering_stamps(&path, 1, 1, 64)
            .expect("create stamped");
        assert!(r.is_stamped());
        r.register_producer().expect("p");
        r.register_consumer().expect("c");
        r.set_ordering_mode(OrderingMode::MergeByStamp).expect("mode");
        assert_eq!(r.ordering_mode(), Some(OrderingMode::MergeByStamp));

        // Stamped round trip at the anon locale (stamp stripped).
        let payload = 0xFEEDu64.to_le_bytes();
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        r.try_send(0, &payload).expect("send");
        let n = r.try_recv(0, &mut buf).expect("recv");
        assert_eq!(n, crate::ordering::STAMPED_PAYLOAD_BYTES);
        assert_eq!(&buf[..8], &payload);

        // In-flight items survive the locale migration (re-stamped
        // in drain order) and the mode follows the active backing.
        for i in 0u64..3 {
            r.try_send(0, &i.to_le_bytes()).expect("send pre-migrate");
        }
        r.migrate_to(Locale::ShmFs).expect("migrate");
        assert_eq!(r.ordering_mode(), Some(OrderingMode::MergeByStamp),
                   "mode must follow the ring to the new locale");
        let mut got = Vec::new();
        for _ in 0..3 {
            let n = r.try_recv(0, &mut buf).expect("recv post-migrate");
            assert_eq!(n, crate::ordering::STAMPED_PAYLOAD_BYTES);
            got.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
        }
        assert_eq!(got, vec![0, 1, 2],
                   "merge-mode drain order must survive the locale transfer");
    }

    #[test]
    fn four_axis_pin_chain_at_shmfs_locale() {
        let path = tmp("four_axis_shmfs");
        let r = LocaleAdaptiveRing::create(&path, 1, 1, 64).expect("create");
        r.register_producer().expect("p");
        r.register_consumer().expect("c");
        r.migrate_to(Locale::ShmFs).expect("migrate");

        let pin_locale = r.pin_current_locale();
        let adaptive = pin_locale.as_shmfs().expect("pinned at shmfs");
        let pin_shape = adaptive.pin_current_shape();
        assert_eq!(pin_shape.shape(), RingShape::Spsc);
        assert!(pin_locale.is_still_valid() && pin_shape.is_still_valid());

        let payload = 0xCAFEBABEu64.to_le_bytes();
        pin_shape.spsc_try_push(&payload).expect("native push");
        let mut buf = [0u8; ADAPTIVE_SPSC_PAYLOAD_BYTES];
        let n = pin_shape.spsc_try_pop(&mut buf).expect("native pop");
        assert!(n >= 8);
        assert_eq!(&buf[..8], &payload);
    }
}
