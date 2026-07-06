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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static CACHED_US: AtomicU64 = AtomicU64::new(0);
static INIT: Once = Once::new();

/// Maximum staleness of the cached clock: the cached value lags real wall
/// time by at most this much. 250 us keeps the updater's wake rate modest
/// (~4000/s) while staying far finer than `CLOCK_REALTIME_COARSE`.
const REFRESH_INTERVAL: Duration = Duration::from_micros(250);

#[inline]
fn real_now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Start the background updater thread (once per process). Idempotent;
/// call from a consumer's `create` / `open`. The cache is seeded
/// synchronously here so the very first [`now_us`] is valid even before
/// the thread's first refresh.
pub fn start() {
    INIT.call_once(|| {
        CACHED_US.store(real_now_us(), Ordering::Relaxed);
        std::thread::Builder::new()
            .name("subetha-cached-clock".into())
            .spawn(|| {
                loop {
                    CACHED_US.store(real_now_us(), Ordering::Relaxed);
                    std::thread::sleep(REFRESH_INTERVAL);
                }
            })
            .ok(); // detached; the JoinHandle is intentionally dropped
    });
}

/// Cached wall-clock microseconds - one relaxed atomic load. Callers must
/// have invoked [`start`] (e.g. at handle create) so the updater is
/// running; before the first refresh this returns the seed taken in
/// `start`. Monotonic to the precision of the underlying clock; a brief
/// backward NTP step is absorbed by HLC-style `max(prev, now)` callers.
#[inline]
pub fn now_us() -> u64 {
    CACHED_US.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_clock_tracks_wall_within_interval() {
        start();
        // Give the updater a couple of refresh cycles to populate.
        std::thread::sleep(REFRESH_INTERVAL * 4);
        let cached = now_us();
        let real = real_now_us();
        assert!(cached > 0, "cache must be seeded");
        // Within a few refresh intervals of real time (generous for CI).
        let skew = real.abs_diff(cached);
        assert!(
            skew < 50_000,
            "cached clock {cached} should track real {real} (skew {skew} us)"
        );
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
