//! `SharedRateLimiter` - cross-process token-bucket rate limiter.
//!
//! Tokens accumulate at a configured rate up to a configured
//! capacity; `acquire(n)` atomically deducts n tokens or returns
//! `Err(InsufficientTokens)`. Refill happens lazily on each
//! acquire - no background thread needed.
//!
//! # Layout
//!
//! Single MMF file:
//!
//! ```text
//! +---------------------------+
//! | RateLimiterHeader (64B)   |
//! |   magic, capacity         |
//! |   refill_rate_per_sec     |
//! |   state: AtomicU64        |  // packed (tokens, refill_us_low)
//! +---------------------------+
//! ```
//!
//! # Packed state
//!
//! The hot atomic packs `(tokens_remaining: u32, last_refill_us_low: u32)`
//! into one u64. Updates are CAS-only so multiple processes
//! concurrently acquiring don't race-update either field
//! independently.
//!
//! - `tokens_remaining` (low 32 bits) supports capacities up to
//!   ~4B tokens; well past any realistic rate-limit budget.
//! - `last_refill_us_low` (high 32 bits) holds the low 32 bits of
//!   the wall-clock-microsecond timestamp at the last refill. Low
//!   32 bits give a 4295-second (~71 minute) window before
//!   wrap-around, which is FAR longer than any acquire-to-acquire
//!   gap in practice. Wrap-around handles correctly via wrapping
//!   subtraction.
//!
//! # Refill on acquire
//!
//! Each `acquire(n)` first computes how many tokens should have
//! been refilled since the last refill: `elapsed_us *
//! refill_rate_per_sec / 1_000_000`. The new token count is
//! `min(current + refilled, capacity)`. Then `n` is subtracted; if
//! the result goes negative, the acquire fails without
//! modifying state.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use memmap2::{MmapMut, MmapOptions};

pub const RATE_LIMITER_MAGIC: u64 = 0x4150_5246_4C4D_5452;

#[repr(C, align(64))]
pub struct RateLimiterHeader {
    pub magic: u64,
    pub capacity: u32,
    pub refill_rate_per_sec: u32,
    pub state: AtomicU64,
    _pad: [u8; 40],
}

const _: () = {
    assert!(size_of::<RateLimiterHeader>() == 64);
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimiterError {
    InsufficientTokens { available: u32, requested: u32 },
    Timeout,
    InvalidConfig,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for RateLimiterError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

#[inline]
fn now_us_low() -> u32 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    micros as u32
}

#[inline]
fn pack_state(tokens: u32, refill_us_low: u32) -> u64 {
    ((refill_us_low as u64) << 32) | (tokens as u64)
}
#[inline]
fn unpack_state(state: u64) -> (u32, u32) {
    (state as u32, (state >> 32) as u32)
}

pub struct SharedRateLimiter {
    _file: File,
    mmap: MmapMut,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedRateLimiter {}
unsafe impl Sync for SharedRateLimiter {}

impl subetha_sidecar::AdaptiveInstance for SharedRateLimiter {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedRateLimiter {
    /// Create a rate limiter. Starts with `capacity` tokens (full
    /// bucket). `refill_rate_per_sec` controls the steady-state
    /// rate; both fields must be > 0.
    pub fn create(
        path: impl AsRef<Path>, capacity: u32, refill_rate_per_sec: u32,
    ) -> Result<Self, RateLimiterError> {
        if capacity == 0 || refill_rate_per_sec == 0 {
            return Err(RateLimiterError::InvalidConfig);
        }
        let total = size_of::<RateLimiterHeader>();
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut RateLimiterHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, total);
            (*hdr).magic = RATE_LIMITER_MAGIC;
            (*hdr).capacity = capacity;
            (*hdr).refill_rate_per_sec = refill_rate_per_sec;
            (*hdr).state.store(
                pack_state(capacity, now_us_low()),
                Ordering::Release,
            );
        }
        Ok(Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>, capacity: u32, refill_rate_per_sec: u32,
    ) -> Result<Self, RateLimiterError> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let total = size_of::<RateLimiterHeader>();
        if file.metadata()?.len() < total as u64 {
            return Err(RateLimiterError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const RateLimiterHeader) };
        if hdr.magic != RATE_LIMITER_MAGIC
            || hdr.capacity != capacity
            || hdr.refill_rate_per_sec != refill_rate_per_sec
        {
            return Err(RateLimiterError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    fn header(&self) -> &RateLimiterHeader {
        unsafe { &*(self.mmap.as_ptr() as *const RateLimiterHeader) }
    }

    #[inline]
    pub fn capacity(&self) -> u32 { self.header().capacity }
    #[inline]
    pub fn refill_rate_per_sec(&self) -> u32 { self.header().refill_rate_per_sec }

    /// Compute refilled tokens given an elapsed-microsecond delta
    /// (handles 32-bit wraparound via wrapping_sub).
    #[inline]
    fn refill_amount(&self, prev_refill_us: u32, now_us: u32) -> u32 {
        // wrapping_sub handles the 71-minute wrap correctly.
        let elapsed = now_us.wrapping_sub(prev_refill_us) as u64;
        let rate = self.refill_rate_per_sec() as u64;
        let refilled = (elapsed * rate) / 1_000_000;
        // Clamp to u32 - extremely long gaps overflow.
        refilled.min(u32::MAX as u64) as u32
    }

    /// Read current available tokens (does NOT mutate state).
    /// Returns the count after accounting for refill since the
    /// last update.
    pub fn available(&self) -> u32 {
        let state = self.header().state.load(Ordering::Acquire);
        let (tokens, refill_us) = unpack_state(state);
        let now = now_us_low();
        let refilled = self.refill_amount(refill_us, now);
        let cap = self.capacity();
        let v = (tokens.saturating_add(refilled)).min(cap);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::rate_limiter::OP_AVAILABLE, 0);
        v
    }

    /// Non-blocking acquire. Atomically refills and deducts `n`
    /// tokens. Returns `Err(InsufficientTokens)` immediately if
    /// fewer than `n` tokens are available after refill.
    pub fn try_acquire(&self, n: u32) -> Result<(), RateLimiterError> {
        loop {
            let state = self.header().state.load(Ordering::Acquire);
            let (tokens, refill_us) = unpack_state(state);

            // Fast path: the bucket already holds enough tokens. Refill
            // only ever ADDS, so `tokens >= n` guarantees the post-refill
            // count would also satisfy `n` - we can deduct without reading
            // the clock. `refill_us` is kept unchanged, deferring the
            // refill accounting: the next time the bucket runs short, the
            // clock read credits the entire elapsed interval (capped at
            // capacity), so no tokens are lost and the long-run rate is
            // preserved. This makes under-limit traffic - the common case -
            // free of the `clock_gettime` the slow path pays.
            if tokens >= n {
                let new_state = pack_state(tokens - n, refill_us);
                if self.header().state.compare_exchange(
                    state, new_state, Ordering::AcqRel, Ordering::Acquire,
                ).is_ok() {
                    self.ring_sidecar
                        .push_op(crate::sidecar_ops::rate_limiter::OP_TRY_ACQUIRE, 0);
                    return Ok(());
                }
                continue; // CAS lost; reload and retry.
            }

            // Slow path: short on tokens - read the clock and refill.
            let now = now_us_low();
            let refilled = self.refill_amount(refill_us, now);
            let cap = self.capacity();
            let after_refill = (tokens.saturating_add(refilled)).min(cap);
            if after_refill < n {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::rate_limiter::OP_TRY_ACQUIRE, 1); // insufficient tokens
                return Err(RateLimiterError::InsufficientTokens {
                    available: after_refill, requested: n,
                });
            }
            let new_tokens = after_refill - n;
            let new_state = pack_state(new_tokens, now);
            if self.header().state.compare_exchange(
                state, new_state, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::rate_limiter::OP_TRY_ACQUIRE, 0);
                return Ok(());
            }
            // CAS lost; retry.
        }
    }

    /// Blocking acquire with deadline. Spins with backoff until
    /// enough tokens are available OR the deadline passes.
    pub fn acquire_or_wait(
        &self, n: u32, timeout: Duration,
    ) -> Result<(), RateLimiterError> {
        if n > self.capacity() {
            return Err(RateLimiterError::InsufficientTokens {
                available: self.capacity(), requested: n,
            });
        }
        let deadline = Instant::now() + timeout;
        let mut spins = 0u32;
        loop {
            match self.try_acquire(n) {
                Ok(()) => return Ok(()),
                Err(RateLimiterError::InsufficientTokens { .. }) => {}
                Err(e) => return Err(e),
            }
            if Instant::now() >= deadline {
                return Err(RateLimiterError::Timeout);
            }
            spins += 1;
            if spins < 32 {
                std::hint::spin_loop();
            } else if spins < 256 {
                std::thread::yield_now();
            } else {
                // Compute how long until we expect enough tokens.
                let need = n.saturating_sub(self.available());
                if need == 0 { continue; }
                let micros_needed = (need as u64 * 1_000_000) / self.refill_rate_per_sec() as u64;
                let sleep_us = micros_needed.min(10_000); // cap at 10ms
                std::thread::sleep(Duration::from_micros(sleep_us));
            }
        }
    }

    /// Reset tokens to full capacity. Useful for tests / admin
    /// recovery. Not concurrency-coordinated; expect transient
    /// races with concurrent acquires.
    pub fn reset(&self) {
        self.header().state.store(
            pack_state(self.capacity(), now_us_low()),
            Ordering::Release,
        );
    }

    pub fn flush(&self) -> Result<(), RateLimiterError> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), RateLimiterError> {
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
        p.push(format!("subetha-ratelim-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_starts_with_full_bucket() {
        let p = tmp("init");
        let r = SharedRateLimiter::create(&p, 100, 10).unwrap();
        assert_eq!(r.capacity(), 100);
        assert_eq!(r.refill_rate_per_sec(), 10);
        assert_eq!(r.available(), 100);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn invalid_config_rejected() {
        let p = tmp("invalid");
        assert_eq!(
            SharedRateLimiter::create(&p, 0, 10).err(),
            Some(RateLimiterError::InvalidConfig)
        );
        assert_eq!(
            SharedRateLimiter::create(&p, 10, 0).err(),
            Some(RateLimiterError::InvalidConfig)
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn try_acquire_deducts_tokens() {
        let p = tmp("deduct");
        let r = SharedRateLimiter::create(&p, 100, 1).unwrap();  // slow refill
        r.try_acquire(30).unwrap();
        let avail = r.available();
        // available may include a few refilled tokens (microsecond
        // elapsed at rate=1/s gives < 1 token). Should be near 70.
        assert!((70..=71).contains(&avail), "after 30-token acquire from cap 100, available={avail} should be ~70");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_bucket_rejects() {
        let p = tmp("empty");
        let r = SharedRateLimiter::create(&p, 5, 1).unwrap();  // slow refill
        r.try_acquire(5).unwrap();
        // Immediately try to acquire more (no time has passed).
        match r.try_acquire(1) {
            Err(RateLimiterError::InsufficientTokens { available, requested }) => {
                assert!(available < 1);
                assert_eq!(requested, 1);
            }
            other => panic!("expected InsufficientTokens, got {other:?}"),
        }
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn refill_scales_with_elapsed_time() {
        let p = tmp("refill");
        // 1000 tokens/sec means 1 token per millisecond.
        let r = SharedRateLimiter::create(&p, 100, 1000).unwrap();
        // Drain.
        r.try_acquire(100).unwrap();
        assert!(r.available() < 5, "after full drain, available should be ~0");
        // Wait 30ms; expect ~30 tokens to have refilled.
        thread::sleep(Duration::from_millis(30));
        let after = r.available();
        assert!((25..=40).contains(&after),
            "after 30ms at 1000/s, available={after} should be ~30");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn refill_clamped_to_capacity() {
        let p = tmp("clamp");
        let r = SharedRateLimiter::create(&p, 50, 10_000).unwrap();
        // Drain.
        r.try_acquire(50).unwrap();
        // Wait long enough that uncapped refill exceeds capacity.
        thread::sleep(Duration::from_millis(100));  // refills 1000 uncapped
        assert_eq!(r.available(), 50, "available should clamp to capacity");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn acquire_or_wait_blocks_then_succeeds() {
        let p = tmp("wait");
        // 100 tokens/sec = 1 every 10ms.
        let r = SharedRateLimiter::create(&p, 1, 100).unwrap();
        r.try_acquire(1).unwrap();
        let start = Instant::now();
        // Need 1 more token; should wait ~10ms.
        r.acquire_or_wait(1, Duration::from_millis(500)).unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(5),
            "should have waited some time, got {elapsed:?}");
        assert!(elapsed < Duration::from_millis(100),
            "should have completed quickly, got {elapsed:?}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn acquire_or_wait_returns_timeout() {
        let p = tmp("timeout");
        let r = SharedRateLimiter::create(&p, 1, 1).unwrap();  // 1 per second
        r.try_acquire(1).unwrap();
        let start = Instant::now();
        let result = r.acquire_or_wait(1, Duration::from_millis(50));
        let elapsed = start.elapsed();
        assert!(matches!(result, Err(RateLimiterError::Timeout)));
        assert!(elapsed >= Duration::from_millis(40),
            "should have waited ~50ms, got {elapsed:?}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn acquire_or_wait_oversized_request_fails_fast() {
        let p = tmp("oversize");
        let r = SharedRateLimiter::create(&p, 10, 100).unwrap();
        // Requesting more than capacity can never be satisfied.
        let result = r.acquire_or_wait(100, Duration::from_secs(10));
        assert!(matches!(result, Err(RateLimiterError::InsufficientTokens { .. })));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_acquirers_sum_to_at_most_capacity_no_refill() {
        let p = tmp("concurrent");
        // Slow refill so the test window sees ~no refilled tokens.
        let r = Arc::new(SharedRateLimiter::create(&p, 100, 1).unwrap());
        let n_threads = 8;
        let per_thread = 50;
        let mut handles = vec![];
        for _ in 0..n_threads {
            let r = r.clone();
            handles.push(thread::spawn(move || {
                let mut acquired = 0u32;
                for _ in 0..per_thread {
                    if r.try_acquire(1).is_ok() { acquired += 1; }
                }
                acquired
            }));
        }
        let total: u32 = handles.into_iter()
            .map(|h| h.join().unwrap()).sum();
        // Total acquired across all threads must be <= capacity +
        // very small refill (rate=1/sec, test window << 1 sec).
        assert!(total <= 101, "total acquired {total} should not exceed capacity {} + tiny refill", 100);
        // We should have acquired exactly the capacity (or very close).
        assert!(total >= 95, "total acquired {total} should be near capacity 100");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_state_shared() {
        let p = tmp("cross-handle");
        let writer = SharedRateLimiter::create(&p, 100, 10).unwrap();
        let reader = SharedRateLimiter::open(&p, 100, 10).unwrap();
        writer.try_acquire(40).unwrap();
        let avail = reader.available();
        assert!((59..=60).contains(&avail));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn config_mismatch_at_open_rejected() {
        let p = tmp("mismatch");
        let _w = SharedRateLimiter::create(&p, 100, 10).unwrap();
        assert!(matches!(
            SharedRateLimiter::open(&p, 50, 10),
            Err(RateLimiterError::LayoutMismatch)
        ));
        assert!(matches!(
            SharedRateLimiter::open(&p, 100, 20),
            Err(RateLimiterError::LayoutMismatch)
        ));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn reset_refills_to_capacity() {
        let p = tmp("reset");
        let r = SharedRateLimiter::create(&p, 50, 1).unwrap();
        r.try_acquire(50).unwrap();
        assert!(r.available() < 2);
        r.reset();
        assert_eq!(r.available(), 50);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let r = SharedRateLimiter::create(&p, 50, 1).unwrap();
            r.try_acquire(20).unwrap();
            r.flush().unwrap();
        }
        let r2 = SharedRateLimiter::open(&p, 50, 1).unwrap();
        let avail = r2.available();
        // Should be ~30 plus any time elapsed at rate 1/s (likely 0-1).
        assert!((30..=31).contains(&avail),
            "after reopen available={avail} should be ~30");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn acquire_zero_is_noop() {
        let p = tmp("zero");
        let r = SharedRateLimiter::create(&p, 100, 1).unwrap();
        let before = r.available();
        r.try_acquire(0).unwrap();
        let after = r.available();
        // available may have incremented by 0-1 due to elapsed time,
        // but shouldn't have decreased.
        assert!(after >= before);
        std::fs::remove_file(&p).ok();
    }
}
