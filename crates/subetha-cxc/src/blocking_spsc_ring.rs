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
/// Off by default. The cross-process bench (`phase_lock_xproc`) showed
/// that across the OS process boundary - SubEtha's primary use case -
/// the doorbell wake is already ~400-500 ns and predictive waiting is
/// a loss (worse p50 and much worse p99 from prediction jitter). The
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
    /// sustained run, so a mixed regime (small backlog, only
    /// occasional empties) never predicts - predicting there mistimes
    /// the park against queued items and adds latency. Reset by any
    /// fast catch.
    consecutive_waits: AtomicU32,
    guard_band: Duration,
    /// Sticky count of items caught by the predictive guard-band spin
    /// (the syscall-free path). Observability - proves the mechanism
    /// fired, surviving the tail-drain estimator reset.
    predictive_catches: AtomicU64,
    /// The arrival estimator. Locked only on the wait path (already
    /// slow), never on the fast path.
    est: Mutex<PhaseEstimator>,
}

impl PhaseControl {
    fn new() -> Self {
        Self {
            // Off by default: predictive waiting loses cross-process
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
    /// Wakes a parked consumer when the producer pushes (consumer
    /// is waiting on a non-empty ring).
    consumer_waker: Arc<CrossProcessWaker>,
    /// Wakes a parked producer when the consumer pops (producer
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
    /// On `Err(Timeout)` the caller's payload is not in the ring.
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
    /// an in-process consumer whose producer contends for cores
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
    /// wait mode. No estimator update - only wait-path arrivals feed the
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
        // Predict only on a regular cadence and sustained empty-ring
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
                // The budget elapsing is the predicted arrival, not an
                // error; anything else the waker reports is.
                match self.consumer_waker.wait(token, Some(budget)) {
                    Ok(()) | Err(WakerError::Timeout) => {}
                    Err(e) => return Err(e.into()),
                }
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
    /// Predictive (phase-locked) waiting is opt-in via
    /// [`Self::set_phase_locking`] - it wins only for an in-process consumer
    /// whose producer contends for cores, and loses cross-process
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
        // catch after a park is a wait-path arrival, even though it surfaces via
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
                    // either way, spin the guard band next; anything else
                    // the waker reports is an error.
                    match self.consumer_waker.wait(token, Some(budget)) {
                        Ok(()) | Err(WakerError::Timeout) => {}
                        Err(e) => return Err(e.into()),
                    }
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

    /// Prediction requires a sustained run of empty-ring waits. A
    /// consumer descheduled on a loaded host arrives to a backlog
    /// instead, which is the mixed regime the estimator refuses to
    /// predict in, so the cadence gets several attempts and the
    /// assertion rests on the predictor rather than on the host's
    /// scheduler. FIFO is checked on every attempt.
    #[test]
    fn phase_locked_recv_preserves_order_and_engages() {
        use crate::phase_estimator::{PhaseConfig, PhaseEstimator};

        let mut engaged_once = false;
        for _ in 0..5 {
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

            // The estimator must have engaged and caught items via the
            // syscall-free guard-band spin. An attempt that engaged but
            // whose every predicted wake missed the guard band, as a
            // descheduled consumer's can, is another attempt rather than a
            // verdict.
            if stats.spin_catches > 0 {
                engaged_once = true;
                break;
            }
        }
        assert!(
            engaged_once,
            "a regular cadence must engage the predictor and catch items via the guard-band spin"
        );
    }

    /// Cells of the two-process round trip, each with a fresh pair of
    /// processes.
    const ECHO_CELLS: u64 = 40;
    /// Frames each cell sends and has echoed.
    const ECHO_FRAMES: u64 = 2_000;
    /// How long one receive may wait before its wake counts as lost.
    const ECHO_LOST_WAKE: Duration = Duration::from_secs(30);
    /// Slots in each direction's ring.
    const ECHO_SLOTS: usize = 4;
    /// Bytes in one region block.
    const ECHO_BLOCK_BYTES: usize = 4096;
    /// Blocks in each direction's region: one per ring slot, one the
    /// sender holds before its descriptor is in the ring, and one the
    /// receiver holds between taking a descriptor and freeing its block.
    const ECHO_BLOCKS: usize = ECHO_SLOTS + 2;
    /// The frame the peer sends once it has opened both directions.
    const ECHO_READY: [u8; 1] = [0x52];

    fn with_suffix(base: &Path, suffix: &str) -> std::path::PathBuf {
        let mut name = base.as_os_str().to_owned();
        name.push(suffix);
        std::path::PathBuf::from(name)
    }

    /// The four files one direction at `base` maps, removed when the
    /// guards drop. Declared before the direction, they drop after it.
    fn direction_files(base: &Path) -> [crate::test_paths::TmpFile; 4] {
        let name = base
            .file_name()
            .and_then(|n| n.to_str())
            .expect("the direction's base name is text");
        [".ring.bin", ".cw.bin", ".pw.bin", ".region.bin"]
            .map(|suffix| crate::test_paths::TmpFile::new(format!("{name}{suffix}")))
    }

    /// One direction of the round trip, carrying a frame the way a stage
    /// boundary does: its bytes in a region block, and a descriptor
    /// naming the block, `[len: u32][block: u32]`, through the ring. Each
    /// frame costs a block allocated, written, read and freed, and each
    /// receive a fresh slot buffer.
    struct EchoDirection {
        ring: BlockingSpscRing,
        region: crate::frame_region::FrameRegion,
    }

    impl EchoDirection {
        fn create(base: &Path) -> Self {
            Self {
                ring: BlockingSpscRing::create(base, ECHO_SLOTS).expect("the ring is created"),
                region: crate::frame_region::FrameRegion::create(
                    with_suffix(base, ".region.bin"),
                    ECHO_BLOCK_BYTES,
                    ECHO_BLOCKS,
                )
                .expect("the region is created"),
            }
        }

        fn open(base: &Path) -> Result<Self, String> {
            Ok(Self {
                ring: BlockingSpscRing::open(base, ECHO_SLOTS)
                    .map_err(|e| format!("the ring: {e:?}"))?,
                region: crate::frame_region::FrameRegion::open(
                    with_suffix(base, ".region.bin"),
                    ECHO_BLOCK_BYTES,
                    ECHO_BLOCKS,
                )
                .map_err(|e| format!("the region: {e:?}"))?,
            })
        }

        /// Copies `frame` into a free block and sends the descriptor; a
        /// send that fails gives the block back.
        fn send(&self, frame: &[u8]) -> Result<(), String> {
            let block = self.region.alloc().ok_or("no region block was free")?;
            self.region.write_block(block, frame);
            let len = u32::try_from(frame.len()).expect("a frame fits a block");
            let mut descriptor = [0u8; 8];
            descriptor[..4].copy_from_slice(&len.to_le_bytes());
            descriptor[4..].copy_from_slice(&block.to_le_bytes());
            self.ring
                .send_blocking(&descriptor, Some(ECHO_LOST_WAKE))
                .map_err(|e| {
                    self.region.free(block);
                    format!("{e:?}")
                })
        }

        /// Takes a descriptor, copies its block into `out` and frees the
        /// block.
        fn recv(&self, out: &mut Vec<u8>) -> Result<(), String> {
            let mut slot = vec![0u8; crate::spsc_ring::SPSC_PAYLOAD_BYTES];
            self.ring
                .recv_blocking(&mut slot, Some(ECHO_LOST_WAKE))
                .map_err(|e| format!("{e:?}"))?;
            let len = u32::from_le_bytes(slot[..4].try_into().expect("4 bytes")) as usize;
            let block = u32::from_le_bytes(slot[4..8].try_into().expect("4 bytes"));
            out.clear();
            self.region.read_block_into(block, len, out);
            self.region.free(block);
            Ok(())
        }
    }

    /// The peer half of the round trip: when the test binary is re-run
    /// with `SUBETHA_RING_ECHO_PEER` naming a cell's base path, it opens
    /// both directions, announces itself and echoes every frame, exiting
    /// 1 on the first receive or send that fails; otherwise it passes at
    /// once.
    #[test]
    fn ring_echo_peer_role() {
        let Some(base) = std::env::var_os("SUBETHA_RING_ECHO_PEER") else {
            return;
        };
        fn fail(what: String) -> ! {
            eprintln!("ring echo peer: {what}");
            std::process::exit(1);
        }
        let base = std::path::PathBuf::from(base);
        let to_peer = EchoDirection::open(&with_suffix(&base, ".out"))
            .unwrap_or_else(|e| fail(format!("opening the outbound direction: {e}")));
        let to_coordinator = EchoDirection::open(&with_suffix(&base, ".in"))
            .unwrap_or_else(|e| fail(format!("opening the inbound direction: {e}")));
        if let Err(e) = to_coordinator.send(&ECHO_READY) {
            fail(format!("announcing: {e}"));
        }
        let mut frame = Vec::new();
        for i in 0..ECHO_FRAMES {
            if let Err(e) = to_peer.recv(&mut frame) {
                fail(format!("frame {i}: receiving: {e}"));
            }
            if let Err(e) = to_coordinator.send(&frame) {
                fail(format!("frame {i}: echoing: {e}"));
            }
        }
        std::process::exit(0);
    }

    /// Kills the echo peer when a cell ends before reaping it, so a lost
    /// wake leaves no process waiting on its own.
    struct EchoPeer(Option<std::process::Child>);

    impl EchoPeer {
        fn reap(mut self) -> std::process::ExitStatus {
            let mut child = self.0.take().expect("the peer is reaped once");
            child.wait().expect("the peer is waited on")
        }
    }

    impl Drop for EchoPeer {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                if let Err(e) = child.kill() {
                    eprintln!("the echo peer was not killed: {e}");
                }
                if let Err(e) = child.wait() {
                    eprintln!("the echo peer was not reaped: {e}");
                }
            }
        }
    }

    /// Frames cross between two processes and back with no hold, over a
    /// fresh pair of processes per cell, each frame in a region block
    /// that a descriptor through the ring names. A producer's head store
    /// followed by its wake scan races a consumer's parked-mask set
    /// followed by its re-check of the ring; a receive that waits out its
    /// timeout lost that race.
    #[test]
    fn a_round_trip_between_two_processes_loses_no_wake() {
        let exe = std::env::current_exe().expect("the test binary's own path");
        let mut problems = Vec::new();
        for cell in 0..ECHO_CELLS {
            let base = std::env::temp_dir()
                .join(format!("subetha-ring-echo-{}-{cell}", std::process::id()));
            let _outbound_files = direction_files(&with_suffix(&base, ".out"));
            let _inbound_files = direction_files(&with_suffix(&base, ".in"));
            let to_peer = EchoDirection::create(&with_suffix(&base, ".out"));
            let to_coordinator = EchoDirection::create(&with_suffix(&base, ".in"));
            let peer = EchoPeer(Some(
                std::process::Command::new(&exe)
                    .arg("blocking_spsc_ring::tests::ring_echo_peer_role")
                    .arg("--exact")
                    .arg("--nocapture")
                    .env("SUBETHA_RING_ECHO_PEER", &base)
                    .spawn()
                    .expect("the peer process spawns"),
            ));
            let mut frame = Vec::new();
            match to_coordinator.recv(&mut frame) {
                Ok(()) if frame == ECHO_READY => {}
                Ok(()) => {
                    problems.push(format!("cell {cell}: the peer announced {frame:?}"));
                    continue;
                }
                Err(e) => {
                    problems.push(format!("cell {cell}: the peer's announcement: {e}"));
                    continue;
                }
            }
            let mut lost = None;
            for i in 0..ECHO_FRAMES {
                let sent = i.to_le_bytes();
                if let Err(e) = to_peer.send(&sent) {
                    lost = Some(format!("cell {cell} frame {i}: sending: {e}"));
                    break;
                }
                let start = Instant::now();
                match to_coordinator.recv(&mut frame) {
                    Ok(()) if frame == sent => {}
                    Ok(()) => {
                        lost = Some(format!("cell {cell} frame {i}: the echo was {frame:?}"));
                        break;
                    }
                    Err(e) => {
                        lost = Some(format!(
                            "cell {cell} frame {i}: receiving the echo: {e} after {:?}",
                            start.elapsed()
                        ));
                        break;
                    }
                }
            }
            match lost {
                Some(line) => problems.push(line),
                None => {
                    let status = peer.reap();
                    if !status.success() {
                        problems.push(format!("cell {cell}: the peer ended with {status}"));
                    }
                }
            }
        }
        assert!(
            problems.is_empty(),
            "{} of {ECHO_CELLS} cells failed: {problems:?}",
            problems.len()
        );
    }
}
