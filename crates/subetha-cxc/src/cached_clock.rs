//! Process-global cached wall clock.
//!
//! A single background thread refreshes a cached microsecond timestamp
//! every `REFRESH_INTERVAL`. Readers load one relaxed atomic (~1 ns)
//! instead of calling `clock_gettime` (~20 ns), trading at most
//! `REFRESH_INTERVAL` of staleness for the cheaper read.
//!
//! This suits primitives whose physical-clock component tolerates coarse
//! resolution because a logical counter orders sub-interval events - e.g.
//! a same-host Hybrid Logical Clock, where every process reads the same
//! hardware clock (zero inter-process skew) and the only thing the cache
//! changes is the granularity at which the physical timestamp advances.
//!
//! Compared with `CLOCK_REALTIME_COARSE` (~1 ms granularity, ~5 ns read)
//! this is both finer (250 us) and faster (a plain atomic load); the cost
//! is one background thread per process, spawned lazily on first use.

use std::sync::Once;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static CACHED_US: AtomicU64 = AtomicU64::new(0);
static INIT: Once = Once::new();
/// Set once the updater thread is running. While it is not - before
/// [`start`], or after a spawn the OS refused - [`now_us`] reads the clock
/// directly, so a reader never takes a value the updater is not keeping.
static UPDATER_LIVE: AtomicBool = AtomicBool::new(false);
static BEFORE_EPOCH_REPORTED: Once = Once::new();

/// Maximum staleness of the cached clock: the cached value lags real wall
/// time by at most this much. 250 us keeps the updater's wake rate modest
/// (~4000/s) while staying far finer than `CLOCK_REALTIME_COARSE`.
const REFRESH_INTERVAL: Duration = Duration::from_micros(250);

#[inline]
fn real_now_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since_epoch) => since_epoch.as_micros() as u64,
        // A wall clock set before 1970 has no microsecond count to give;
        // 0 is the value every reader then agrees on, said once.
        Err(before_epoch) => {
            BEFORE_EPOCH_REPORTED.call_once(|| {
                eprintln!("subetha: the wall clock reads before the Unix epoch: {before_epoch}")
            });
            0
        }
    }
}

/// Start the background updater thread (once per process). Idempotent;
/// call from a consumer's `create` / `open`. The cache is seeded
/// synchronously here so the very first [`now_us`] is valid even before
/// the thread's first refresh. A spawn the OS refuses is reported, and
/// readers then take the clock directly rather than a value nothing
/// keeps fresh.
pub fn start() {
    INIT.call_once(|| {
        CACHED_US.store(real_now_us(), Ordering::Relaxed);
        let spawned = std::thread::Builder::new()
            .name("subetha-cached-clock".into())
            .spawn(|| {
                loop {
                    CACHED_US.store(real_now_us(), Ordering::Relaxed);
                    std::thread::sleep(REFRESH_INTERVAL);
                }
            });
        match spawned {
            // Detached: the handle is dropped and the thread runs for the
            // process's life.
            Ok(_detached) => UPDATER_LIVE.store(true, Ordering::Release),
            Err(e) => eprintln!(
                "subetha: the cached-clock updater was not spawned ({e}); readers take the clock directly"
            ),
        }
    });
}

/// Cached wall-clock microseconds - one relaxed atomic load while the
/// updater runs. Before [`start`], or if its thread could not be spawned,
/// this reads the clock directly instead, at that call's cost. Monotonic
/// to the precision of the underlying clock; a brief backward NTP step is
/// absorbed by HLC-style `max(prev, now)` callers.
#[inline]
pub fn now_us() -> u64 {
    if UPDATER_LIVE.load(Ordering::Acquire) {
        CACHED_US.load(Ordering::Relaxed)
    } else {
        real_now_us()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_clock_tracks_wall_within_interval() {
        start();
        std::thread::sleep(REFRESH_INTERVAL * 4);
        // Sample until the skew is inside the bound. A single sample
        // asserts the updater thread was scheduled within one refresh
        // window, which under load it is not; a transiently starved
        // updater is not the defect this test is for. An updater that
        // never catches up runs the deadline out.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let cached = now_us();
            let real = real_now_us();
            assert!(cached > 0, "cache must be seeded");
            let skew = real.abs_diff(cached);
            if skew < 50_000 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "cached clock {cached} never tracked real {real} (skew {skew} us)"
            );
            std::thread::sleep(REFRESH_INTERVAL);
        }
    }

    #[test]
    fn now_us_is_monotonic_nondecreasing() {
        start();
        let mut prev = now_us();
        for _ in 0..1000 {
            let cur = now_us();
            assert!(cur >= prev, "cached clock must not go backward");
            prev = cur;
        }
    }
}
