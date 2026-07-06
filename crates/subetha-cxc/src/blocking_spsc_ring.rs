//! `BlockingSpscRing`: SPSC ring with cross-process futex-shaped
//! `recv_blocking(timeout)` / `send_blocking(timeout)`.
//!
//! Wraps `SpscRingCore` + `CrossProcessWaker`. The hot path
//! (`try_send` / `try_recv`) stays the same as the bare SPSC
//! primitive. The blocking calls park the caller on the waker
//! when the ring is empty (recv) or full (send), and the
//! counterparty's post-publish path fires a single-slot wake to
//! release them.
//!
//! See [`crate::cross_process_waker`] for the wake protocol +
//! storage layout. See `examples/blocking_spsc_e2e.rs` for the
//! intra-process worked example and
//! `examples/blocking_spsc_xproc_producer.rs` +
//! `..._consumer.rs` for the cross-process pair.

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::cross_process_waker::{
    CrossProcessWaker, WakerError, MAX_WAITERS_DEFAULT,
};
use crate::phase_estimator::{PhaseConfig, PhaseEstimator};
use crate::shared_ring::RingError;
use crate::spsc_ring::SpscRingCore;

/// Instrumentation for [`BlockingSpscRing::recv_phase_locked`]: how
/// each item was caught. The headline ratio is `spin_catches` (no
/// wake syscall) vs `doorbell_catches` (a park/wake round-trip).
#[derive(Debug, Default, Clone, Copy)]
pub struct PhaseRecvStats {
    /// Item already present at entry (no wait at all).
    pub fast_catches: u64,
    /// Predictive parks: a budgeted wait until just before the
    /// predicted arrival, in engaged mode.
    pub predictive_parks: u64,
    /// Item caught by the guard-band spin after a predictive park -
    /// the syscall-free path the experiment is about.
    pub spin_catches: u64,
    /// Doorbell parks: a park in the fallback (disengaged or
    /// missed-prediction) path.
    pub doorbell_parks: u64,
    /// Item caught in the fallback path.
    pub doorbell_catches: u64,
}

/// Errors returned by the blocking variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockingError {
    /// Wrapped ring error from the underlying SPSC primitive.
    Ring(RingError),
    /// All waker slots in use; caller's fallback is to spin via
    /// `try_send` / `try_recv` directly.
    WakerFull,
    /// `recv_blocking` / `send_blocking` returned because the
    /// caller-supplied timeout elapsed without the counterparty
    /// firing a wake.
    Timeout,
    /// Waker mmap layout did not match expectations on open.
    WakerLayout,
    /// I/O error from the underlying mmap of either the ring or
    /// one of the wakers.
    Io(std::io::ErrorKind),
}

impl From<RingError> for BlockingError {
    fn from(e: RingError) -> Self { Self::Ring(e) }
}

impl From<WakerError> for BlockingError {
    fn from(e: WakerError) -> Self {
        match e {
            WakerError::Full => Self::WakerFull,
            WakerError::Timeout => Self::Timeout,
            WakerError::LayoutMismatch => Self::WakerLayout,
            WakerError::IoError(k) => Self::Io(k),
        }
    }
}

/// Consumer-local adaptive phase-locked-waiting state. SPSC has
/// exactly one consumer, so the ring owns it.
///
/// DEFAULT OFF. The cross-process bench (`phase_lock_xproc`) showed
/// that across the OS process boundary - SubEtha's primary use case -
/// the doorbell wake is already ~400-500 ns and predictive waiting is
/// a LOSS (worse p50 and much worse p99 from prediction jitter). The
/// in-process win it shows (10-50x) came from thread-scheduling
/// contention inflating the in-process doorbell to ~10 us, which does
/// not occur cross-process. So predictive waiting is opt-in for the
/// narrow in-process-contended case, enabled via
/// [`set_phase_locking`](BlockingSpscRing::set_phase_locking).
///
/// When enabled, two nested gates keep it cheap: a **wait-mode gate**
/// (`in_wait_mode`) runs the estimator only while the consumer waits
/// on an empty ring (the fast path reads one relaxed atomic and skips
/// it otherwise), and a **sustained-wait + CV engage gate** inside
/// the estimator predicts only on a regular cadence after consecutive
/// empty-ring waits.
struct PhaseControl {
    enabled: AtomicBool,
    in_wait_mode: AtomicBool,
    /// Consecutive fast-path catches while in wait mode; a long run
    /// means the consumer has caught up and prediction is moot -
    /// leave wait mode. Atomic so the fast path touches it without
    /// the estimator lock or an `Instant::now`.
    consecutive_fast: AtomicU32,
    /// Consecutive empty-ring waits. Prediction fires only after a
    /// sustained run, so a MIXED regime (small backlog, only
    /// occasional empties) never predicts - predicting there mistimes
    /// the park against queued items and adds latency. Reset by any
    /// fast catch.
    consecutive_waits: AtomicU32,
    guard_band: Duration,
    /// Sticky count of items caught by the predictive guard-band spin
    /// (the syscall-free path). Observability - proves the mechanism
    /// fired, surviving the tail-drain estimator reset.
    predictive_catches: AtomicU64,
    /// The arrival estimator. Locked ONLY on the wait path (already
    /// slow), never on the fast path.
    est: Mutex<PhaseEstimator>,
}

impl PhaseControl {
    fn new() -> Self {
        Self {
            // OFF by default: predictive waiting loses cross-process
            // (see the type doc); opt-in via set_phase_locking.
            enabled: AtomicBool::new(false),
            in_wait_mode: AtomicBool::new(false),
            consecutive_fast: AtomicU32::new(0),
            consecutive_waits: AtomicU32::new(0),
            guard_band: Duration::from_micros(3),
            predictive_catches: AtomicU64::new(0),
            est: Mutex::new(PhaseEstimator::new(PhaseConfig::default())),
        }
    }
}

/// SPSC ring with cross-process blocking recv / send.
pub struct BlockingSpscRing {
    inner: Arc<SpscRingCore>,
    /// Wakes a parked CONSUMER when the producer pushes (consumer
    /// is waiting on a non-empty ring).
    consumer_waker: Arc<CrossProcessWaker>,
    /// Wakes a parked PRODUCER when the consumer pops (producer
    /// is waiting on a non-full ring).
    producer_waker: Arc<CrossProcessWaker>,
    /// Adaptive phase-locked waiting, automatic and atomically
    /// toggleable. See [`PhaseControl`].
    phase: PhaseControl,
}

const PRE_PARK_SPIN: u32 = 32;
/// Consecutive fast-path catches that take the consumer out of wait
/// mode (it has caught up; prediction is moot until it waits again).
const PHASE_EXIT_FAST_RUN: u32 = 64;
/// Consecutive empty-ring waits required before prediction fires.
/// Below this the regime is mixed (queued items, not clean waiting)
/// and predicting mistimes the park - so the consumer just doorbells,
/// staying at parity instead of regressing.
const PHASE_MIN_SUSTAINED_WAITS: u32 = 8;

impl BlockingSpscRing {
    /// Anon (in-process) ring + both wakers anon.
    pub fn create_anon(capacity: usize) -> Result<Self, BlockingError> {
        let inner = SpscRingCore::create_anon(capacity).map_err(BlockingError::from)?;
        let consumer_waker = CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT)
            .map_err(BlockingError::from)?;
        let producer_waker = CrossProcessWaker::create_anon(MAX_WAITERS_DEFAULT)
            .map_err(BlockingError::from)?;
        Ok(Self {
            inner: Arc::new(inner),
            consumer_waker: Arc::new(consumer_waker),
            producer_waker: Arc::new(producer_waker),
            phase: PhaseControl::new(),
        })
    }

    /// File-backed ring + both wakers in adjacent files.
    /// Suffixes: `.ring.bin`, `.cw.bin`, `.pw.bin`.
    pub fn create(
        base_path: impl AsRef<Path>,
        capacity: usize,
    ) -> Result<Self, BlockingError> {
        let base = base_path.as_ref();
        let mut ring_path = base.as_os_str().to_owned();
        ring_path.push(".ring.bin");
        let mut cw_path = base.as_os_str().to_owned();
        cw_path.push(".cw.bin");
        let mut pw_path = base.as_os_str().to_owned();
        pw_path.push(".pw.bin");
        let inner = SpscRingCore::create(std::path::PathBuf::from(ring_path), capacity)
            .map_err(BlockingError::from)?;
        let consumer_waker = CrossProcessWaker::create(
            std::path::PathBuf::from(cw_path),
            MAX_WAITERS_DEFAULT,
        ).map_err(BlockingError::from)?;
        let producer_waker = CrossProcessWaker::create(
            std::path::PathBuf::from(pw_path),
            MAX_WAITERS_DEFAULT,
        ).map_err(BlockingError::from)?;
        Ok(Self {
            inner: Arc::new(inner),
            consumer_waker: Arc::new(consumer_waker),
            producer_waker: Arc::new(producer_waker),
            phase: PhaseControl::new(),
        })
    }

    /// Open an existing file-backed ring + wakers.
    pub fn open(
        base_path: impl AsRef<Path>,
        expected_capacity: usize,
    ) -> Result<Self, BlockingError> {
        let base = base_path.as_ref();
        let mut ring_path = base.as_os_str().to_owned();
        ring_path.push(".ring.bin");
        let mut cw_path = base.as_os_str().to_owned();
        cw_path.push(".cw.bin");
        let mut pw_path = base.as_os_str().to_owned();
        pw_path.push(".pw.bin");
        let inner = SpscRingCore::open(std::path::PathBuf::from(ring_path), expected_capacity)
            .map_err(BlockingError::from)?;
        let consumer_waker = CrossProcessWaker::open(
            std::path::PathBuf::from(cw_path),
            MAX_WAITERS_DEFAULT,
        ).map_err(BlockingError::from)?;
        let producer_waker = CrossProcessWaker::open(
            std::path::PathBuf::from(pw_path),
            MAX_WAITERS_DEFAULT,
        ).map_err(BlockingError::from)?;
        Ok(Self {
            inner: Arc::new(inner),
            consumer_waker: Arc::new(consumer_waker),
            producer_waker: Arc::new(producer_waker),
            phase: PhaseControl::new(),
        })
    }

    /// Direct access to the underlying SPSC ring for callers that
    /// want the non-blocking surface.
    pub fn inner(&self) -> &Arc<SpscRingCore> { &self.inner }

    /// Wakers (in case the caller wants to peek wake counts for
    /// instrumentation).
    pub fn consumer_waker(&self) -> &Arc<CrossProcessWaker> { &self.consumer_waker }
    pub fn producer_waker(&self) -> &Arc<CrossProcessWaker> { &self.producer_waker }

    /// Hot-path non-blocking push. On success, fires a single-slot
    /// wake at the consumer_waker so any blocked recv runs.
    #[inline]
    pub fn try_push(&self, payload: &[u8]) -> Result<(), RingError> {
        let r = self.inner.try_push(payload);
        if r.is_ok() {
            self.consumer_waker.wake_up_to(self.inner.head());
        }
        r
    }

    /// Hot-path non-blocking pop. On success, fires a wake at the
    /// producer_waker so any blocked send runs.
    #[inline]
    pub fn try_pop(&self, out: &mut [u8]) -> Result<usize, RingError> {
        let r = self.inner.try_pop(out);
        if r.is_ok() {
            self.producer_waker.wake_up_to(self.inner.tail());
        }
        r
    }

    /// Block until either a push succeeds or `timeout` elapses.
    /// On `Err(Timeout)` the caller's payload is NOT in the ring.
    pub fn send_blocking(
        &self,
        payload: &[u8],
        timeout: Option<Duration>,
    ) -> Result<(), BlockingError> {
        let deadline = timeout.map(|d| Instant::now() + d);
        loop {
            match self.try_push(payload) {
                Ok(()) => return Ok(()),
                Err(RingError::Full) => {}
                Err(e) => return Err(BlockingError::Ring(e)),
            }
            for _ in 0..PRE_PARK_SPIN {
                if self.inner.try_push(payload).is_ok() {
                    self.consumer_waker.wake_up_to(self.inner.head());
                    return Ok(());
                }
                std::hint::spin_loop();
            }
            let current_tail = self.inner.tail();
            let token = self.producer_waker.try_park(current_tail + 1)?;
            // Wake-before-park recovery.
            if self.inner.try_push(payload).is_ok() {
                self.producer_waker.release(token);
                self.consumer_waker.wake_up_to(self.inner.head());
                return Ok(());
            }
            let remaining = match deadline {
                None => None,
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        self.producer_waker.release(token);
                        return Err(BlockingError::Timeout);
                    }
                    Some(d - now)
                }
            };
            match self.producer_waker.wait(token, remaining) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingError::Timeout),
                Err(e) => return Err(BlockingError::from(e)),
            }
        }
    }

    /// Enable or disable predictive (phase-locked) waiting on this
    /// ring's consumer at runtime. Atomic; takes effect on the next
    /// `recv_blocking` call. Default is DISABLED - the bare doorbell
    /// wins cross-process (the primary use case). Enable it only for
    /// an IN-PROCESS consumer whose producer contends for cores
    /// (where the doorbell wake inflates and predictive spinning
    /// shaves it); see the `phase_lock_probe` (in-process win) and
    /// `phase_lock_xproc` (cross-process loss) benches.
    pub fn set_phase_locking(&self, enabled: bool) {
        self.phase.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Whether automatic phase-locked waiting is currently enabled.
    pub fn phase_locking_enabled(&self) -> bool {
        self.phase.enabled.load(Ordering::Relaxed)
    }

    /// Whether the consumer is currently in wait mode (the estimator
    /// is live). Observability; false in steady high-throughput.
    pub fn phase_in_wait_mode(&self) -> bool {
        self.phase.in_wait_mode.load(Ordering::Relaxed)
    }

    /// Whether the arrival predictor is currently engaged (regular
    /// cadence, enough samples). Observability accessor - locks the
    /// estimator, so not for the hot path. Note: the estimator resets
    /// when the consumer leaves wait mode, so this can read false at
    /// the end of a run even after heavy engagement - use
    /// [`phase_predictive_catches`](Self::phase_predictive_catches)
    /// for a sticky "did it fire" signal.
    pub fn phase_engaged(&self) -> bool {
        self.phase.est.lock().unwrap().engaged()
    }

    /// Sticky count of items caught by the predictive guard-band spin
    /// since construction - the syscall-free path. Nonzero proves the
    /// adaptive mechanism engaged and fired.
    pub fn phase_predictive_catches(&self) -> u64 {
        self.phase.predictive_catches.load(Ordering::Relaxed)
    }

    /// Fast-path catch while in wait mode: cheap, lock-free,
    /// `Instant`-free. The consumer caught up; count toward leaving
    /// wait mode. No estimator update - only WAIT arrivals feed the
    /// period estimate.
    #[inline]
    fn phase_on_fast(&self) {
        self.phase.consecutive_waits.store(0, Ordering::Relaxed);
        let cf = self.phase.consecutive_fast.fetch_add(1, Ordering::Relaxed) + 1;
        if cf >= PHASE_EXIT_FAST_RUN {
            // Caught up: leave wait mode and reset the estimator so
            // the next wait re-learns a fresh cadence.
            self.phase.in_wait_mode.store(false, Ordering::Relaxed);
            self.phase.consecutive_fast.store(0, Ordering::Relaxed);
            *self.phase.est.lock().unwrap() =
                PhaseEstimator::new(PhaseConfig::default());
        }
    }

    /// Wait-path catch: feed the estimator (this is already the slow
    /// path, so the lock + `Instant` are free relative to the park).
    fn phase_on_wait(&self, now: Instant) {
        self.phase.consecutive_fast.store(0, Ordering::Relaxed);
        self.phase.consecutive_waits.fetch_add(1, Ordering::Relaxed);
        self.phase.est.lock().unwrap().record(now);
    }

    /// The engaged predictive path: park to just before the predicted
    /// arrival, then spin the guard band. Returns `Ok(Some(n))` on a
    /// catch, `Ok(None)` when disengaged or the prediction missed
    /// (fall through to the doorbell), `Err` on timeout. Never holds
    /// the estimator lock across the park.
    fn phase_predict_and_spin(
        &self,
        out: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<Option<usize>, BlockingError> {
        // Predict only on a regular cadence AND sustained empty-ring
        // waiting - a mixed regime stays on the doorbell.
        if self.phase.consecutive_waits.load(Ordering::Relaxed)
            < PHASE_MIN_SUSTAINED_WAITS
        {
            return Ok(None);
        }
        let (engaged, predicted) = {
            let est = self.phase.est.lock().unwrap();
            (est.engaged(), est.predict_next())
        };
        let Some(predicted) = predicted.filter(|_| engaged) else {
            return Ok(None);
        };
        let now = Instant::now();
        if let Some(wake_at) = predicted.checked_sub(self.phase.guard_band)
            && wake_at > now
        {
            let mut budget = wake_at - now;
            if let Some(d) = deadline {
                budget = budget.min(d.saturating_duration_since(now));
            }
            if !budget.is_zero() {
                let token = self.consumer_waker.try_park(self.inner.head() + 1)?;
                if let Ok(n) = self.try_pop(out) {
                    self.consumer_waker.release(token);
                    return Ok(Some(n));
                }
                self.consumer_waker.wait(token, Some(budget)).ok();
            }
        }
        let spin_end = predicted + self.phase.guard_band;
        loop {
            if let Ok(n) = self.try_pop(out) {
                return Ok(Some(n));
            }
            let now = Instant::now();
            if let Some(d) = deadline
                && now >= d
            {
                return Err(BlockingError::Timeout);
            }
            if now > spin_end {
                return Ok(None); // missed prediction -> doorbell
            }
            std::hint::spin_loop();
        }
    }

    /// Block until either a pop succeeds or `timeout` elapses.
    /// On `Err(Timeout)` `out` is unchanged.
    ///
    /// By default this is the bare doorbell park (the consumer parks
    /// on the cross-process waker until the producer's push wakes it).
    /// Predictive (phase-locked) waiting is OPT-IN via
    /// [`Self::set_phase_locking`] - it wins only for an in-process consumer
    /// whose producer contends for cores, and LOSES cross-process
    /// where the doorbell is already fast. When enabled and the
    /// consumer waits on a regular-cadence producer, it predicts the
    /// arrival and spins a short guard band instead of paying the
    /// wake propagation; the wait-mode gate keeps the fast path at one
    /// relaxed atomic load. Correctness (exactly-once, FIFO) is
    /// identical in every mode.
    pub fn recv_blocking(
        &self,
        out: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<usize, BlockingError> {
        let deadline = timeout.map(|d| Instant::now() + d);
        let adaptive = self.phase.enabled.load(Ordering::Relaxed);
        // Per-call: did this recv park/spin-wait before catching? A
        // catch after a park is a WAIT, even though it surfaces via
        // the fast-path try_pop on the loop-back - classifying it as
        // "fast" would reset the sustained-wait counter and prediction
        // would never accumulate.
        let mut waited = false;

        loop {
            // Fast path: item already present.
            match self.try_pop(out) {
                Ok(n) => {
                    if adaptive && self.phase.in_wait_mode.load(Ordering::Relaxed) {
                        if waited {
                            self.phase_on_wait(Instant::now());
                        } else {
                            self.phase_on_fast();
                        }
                    }
                    return Ok(n);
                }
                Err(RingError::Empty) => {}
                Err(e) => return Err(BlockingError::Ring(e)),
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(n) = self.inner.try_pop(out) {
                    self.producer_waker.wake_up_to(self.inner.tail());
                    if adaptive && self.phase.in_wait_mode.load(Ordering::Relaxed) {
                        if waited {
                            self.phase_on_wait(Instant::now());
                        } else {
                            self.phase_on_fast();
                        }
                    }
                    return Ok(n);
                }
                std::hint::spin_loop();
            }

            // About to wait: enter wait mode and try the predictive
            // path before falling back to the doorbell park.
            waited = true;
            if adaptive {
                self.phase.in_wait_mode.store(true, Ordering::Relaxed);
                if let Some(n) = self.phase_predict_and_spin(out, deadline)? {
                    self.phase.predictive_catches.fetch_add(1, Ordering::Relaxed);
                    self.phase_on_wait(Instant::now());
                    return Ok(n);
                }
            }

            // Doorbell park (today's behavior).
            let current_head = self.inner.head();
            let token = self.consumer_waker.try_park(current_head + 1)?;
            if let Ok(n) = self.inner.try_pop(out) {
                self.consumer_waker.release(token);
                self.producer_waker.wake_up_to(self.inner.tail());
                if adaptive && self.phase.in_wait_mode.load(Ordering::Relaxed) {
                    self.phase_on_wait(Instant::now());
                }
                return Ok(n);
            }
            let remaining = match deadline {
                None => None,
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        self.consumer_waker.release(token);
                        return Err(BlockingError::Timeout);
                    }
                    Some(d - now)
                }
            };
            match self.consumer_waker.wait(token, remaining) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingError::Timeout),
                Err(e) => return Err(BlockingError::from(e)),
            }
        }
    }

    /// Predictive blocking pop. When the `estimator` is engaged (the
    /// producer's cadence is regular enough), this parks only until
    /// `guard_band` before the predicted next arrival, then spins
    /// through the guard band catching the item by polling - skipping
    /// the park/wake syscall round-trip the doorbell pays. When the
    /// estimator is disengaged (irregular cadence) or the prediction
    /// is missed, it falls back to the same doorbell park as
    /// [`Self::recv_blocking`], so correctness is identical in every mode.
    ///
    /// The estimator is consumer-local state the caller owns; pass the
    /// same `&mut` instance across calls so it accumulates cadence.
    /// `stats` accumulates how each item was caught.
    pub fn recv_phase_locked(
        &self,
        out: &mut [u8],
        estimator: &mut PhaseEstimator,
        guard_band: Duration,
        timeout: Option<Duration>,
        stats: &mut PhaseRecvStats,
    ) -> Result<usize, BlockingError> {
        let deadline = timeout.map(|d| Instant::now() + d);

        // Fast path: an item is already waiting.
        if let Ok(n) = self.try_pop(out) {
            estimator.record(Instant::now());
            stats.fast_catches += 1;
            return Ok(n);
        }

        // Engaged predictive path: park to just before the predicted
        // arrival, then spin through the guard band.
        if estimator.engaged()
            && let Some(predicted) = estimator.predict_next()
        {
            let now = Instant::now();
            if let Some(wake_at) = predicted.checked_sub(guard_band)
                && wake_at > now
            {
                let mut budget = wake_at - now;
                if let Some(d) = deadline {
                    budget = budget.min(d.saturating_duration_since(now));
                }
                if !budget.is_zero() {
                    let token = self.consumer_waker.try_park(self.inner.head() + 1)?;
                    // Wake-before-park recovery.
                    if let Ok(n) = self.try_pop(out) {
                        self.consumer_waker.release(token);
                        estimator.record(Instant::now());
                        stats.fast_catches += 1;
                        return Ok(n);
                    }
                    stats.predictive_parks += 1;
                    // Woken by the doorbell or the budget elapsed -
                    // either way, spin the guard band next.
                    self.consumer_waker.wait(token, Some(budget)).ok();
                }
            }

            // Guard-band spin: poll until the item lands or the window
            // past the prediction closes (a missed prediction).
            let spin_end = predicted + guard_band;
            loop {
                if let Ok(n) = self.try_pop(out) {
                    estimator.record(Instant::now());
                    stats.spin_catches += 1;
                    return Ok(n);
                }
                let now = Instant::now();
                if let Some(d) = deadline
                    && now >= d
                {
                    return Err(BlockingError::Timeout);
                }
                if now > spin_end {
                    break; // prediction missed; fall through to the doorbell
                }
                std::hint::spin_loop();
            }
        }

        // Fallback: the doorbell park loop (identical to
        // recv_blocking), recording arrivals so the estimator keeps
        // learning even while disengaged.
        loop {
            if let Ok(n) = self.try_pop(out) {
                estimator.record(Instant::now());
                stats.doorbell_catches += 1;
                return Ok(n);
            }
            for _ in 0..PRE_PARK_SPIN {
                if let Ok(n) = self.try_pop(out) {
                    estimator.record(Instant::now());
                    stats.doorbell_catches += 1;
                    return Ok(n);
                }
                std::hint::spin_loop();
            }
            let current_head = self.inner.head();
            let token = self.consumer_waker.try_park(current_head + 1)?;
            if let Ok(n) = self.try_pop(out) {
                self.consumer_waker.release(token);
                estimator.record(Instant::now());
                stats.doorbell_catches += 1;
                return Ok(n);
            }
            let remaining = match deadline {
                None => None,
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        self.consumer_waker.release(token);
                        return Err(BlockingError::Timeout);
                    }
                    Some(d - now)
                }
            };
            stats.doorbell_parks += 1;
            match self.consumer_waker.wait(token, remaining) {
                Ok(()) => continue,
                Err(WakerError::Timeout) => return Err(BlockingError::Timeout),
                Err(e) => return Err(BlockingError::from(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn round_trip_blocking_anon() {
        let ring = Arc::new(BlockingSpscRing::create_anon(4).expect("create"));
        let r2 = Arc::clone(&ring);
        let producer = thread::spawn(move || {
            for i in 0..10u64 {
                let mut payload = [0u8; 56];
                payload[..8].copy_from_slice(&i.to_le_bytes());
                r2.send_blocking(&payload, Some(Duration::from_secs(2)))
                    .expect("send");
            }
        });
        let r3 = Arc::clone(&ring);
        let consumer = thread::spawn(move || {
            let mut buf = [0u8; 64];
            for expected in 0..10u64 {
                r3.recv_blocking(&mut buf, Some(Duration::from_secs(2)))
                    .expect("recv");
                let got = u64::from_le_bytes(buf[..8].try_into().unwrap());
                assert_eq!(got, expected);
            }
        });
        producer.join().unwrap();
        consumer.join().unwrap();
    }

    #[test]
    fn recv_blocking_returns_timeout() {
        let ring = BlockingSpscRing::create_anon(4).expect("create");
        let mut buf = [0u8; 64];
        let t0 = Instant::now();
        let err = ring.recv_blocking(&mut buf, Some(Duration::from_millis(60)));
        assert_eq!(err, Err(BlockingError::Timeout));
        assert!(t0.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn phase_locked_recv_preserves_order_and_engages() {
        use crate::phase_estimator::{PhaseConfig, PhaseEstimator};

        let ring = Arc::new(BlockingSpscRing::create_anon(256).expect("create"));
        let n = 4_000u64;

        // Producer: a regular ~15us cadence so the estimator engages.
        let r2 = Arc::clone(&ring);
        let producer = thread::spawn(move || {
            for i in 0..n {
                let mut payload = [0u8; 56];
                payload[..8].copy_from_slice(&i.to_le_bytes());
                while r2.try_push(&payload).is_err() {
                    std::hint::spin_loop();
                }
                let t = Instant::now();
                while t.elapsed() < Duration::from_micros(15) {
                    std::hint::spin_loop();
                }
            }
        });

        let mut est = PhaseEstimator::new(PhaseConfig::default());
        let mut stats = PhaseRecvStats::default();
        let mut buf = [0u8; 64];
        for expected in 0..n {
            ring.recv_phase_locked(
                &mut buf,
                &mut est,
                Duration::from_micros(3),
                Some(Duration::from_secs(5)),
                &mut stats,
            ).expect("recv");
            let got = u64::from_le_bytes(buf[..8].try_into().unwrap());
            assert_eq!(got, expected, "phase-locked recv must preserve FIFO");
        }
        producer.join().unwrap();

        // The estimator must have engaged and caught a meaningful
        // share of items via the syscall-free guard-band spin.
        assert!(est.engaged() || stats.spin_catches > 0,
                "a regular cadence must engage the predictor");
        assert!(stats.spin_catches > 0,
                "engaged mode must catch items via the guard-band spin");
    }
}
