//! This host's wait costs, measured on a background thread and cached per
//! user, and the plans [`derive`](fn@derive) makes of them.
//!
//! - [`start`] runs the thread, at most once per process. For each core
//!   class it takes the intrinsic facts from the cache or measures them: the
//!   counter's rate and tick, the cycles one spin hint takes, and each timed
//!   monitor state's floor, guard floor and cycles per unit.
//! - The thread reads the machine's load every second. The first time the
//!   load sits in a band with no wake set, it takes the set from the cache
//!   or measures it: the wake latency of a spin, of each monitor state and
//!   of each park kind, filed under the band that reading named.
//! - [`plan_for`] answers the plan for the calling thread's core class, the
//!   current band and a park kind. Where there is none, a wait keeps
//!   0.5.1's fixed ladder.
//! - A fact is the smallest statistic of five windows run back to back,
//!   kept with the busy logical processors across its window. It is not
//!   published when the largest statistic minus the smallest exceeds their
//!   median, or when it lies outside its bound.
//! - The cache holds one text file per record, written under a temporary
//!   name and renamed into place. A record for another processor, processor
//!   count, hypervisor or schema, or with any value outside its bound, reads
//!   as absent, and the reason is kept in the report's notes.
//! - On Windows and Linux a host with more than one core class is measured
//!   class by class, the waiter and the storer pinned to two cores of the
//!   class. Elsewhere they run where the operating system puts them.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Once, OnceLock};
use std::time::{Duration, Instant};

use subetha_core::{SwapCell, SwapCellOption};
use subetha_core::cpuid::CoreKind;

use crate::cross_process_waker::CrossProcessWaker;
use crate::host_load::CpuTimes;
use crate::monitor_wait::{MonitorWaitKind, monitor_wait_kind, run_monitor_phase_u64};
use crate::ordering::read_tsc;
use crate::wait_instr::{MwaitxHint, UserWait};
use crate::wait_plan::{
    Facts, LoadBand, MonitorFacts, MonitorFamily, MonitorPhase, Overrides, ParkKind, Plan, derive,
};
use crate::wait_topology::{CoreClass, LogicalProcessor, Topology};

/// Windows each fact is taken over, back to back.
const WINDOWS: usize = 5;
/// How long one window times the counter against the monotonic clock.
const RATE_WINDOW: Duration = Duration::from_millis(10);
/// Back-to-back counter reads one window examines for the tick.
const TICK_READS: usize = 100_000;
/// Blocks of spin hints one window times; the window keeps its fastest.
const PAUSE_BLOCKS: usize = 9;
/// Spin hints in one block.
const PAUSE_BLOCK: u32 = 10_000;
/// The sweep requests 2^0 through 2^SWEEP_TOP timer units.
#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
const SWEEP_TOP: u32 = 21;
/// Reads of each sweep request; the request keeps their median.
#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
const SWEEP_READS: usize = 5;
/// Round trips one wake window runs; the window keeps their median.
const TRIPS: u64 = 101;
/// The gap between a spin or monitor waiter reporting ready and the store.
const SHORT_GAP: Duration = Duration::from_micros(20);
/// The gap between a parked waiter reporting ready and its wake, in a
/// park's first window; the second window's gap is the hold the first
/// gives.
const PARK_GAP: Duration = Duration::from_micros(500);
/// The longest one round trip waits before its window is abandoned.
const TRIP_BOUND: Duration = Duration::from_millis(500);
/// How often the background thread reads the machine's load.
const LOAD_PERIOD: Duration = Duration::from_secs(1);
/// The cache's record format.
const SCHEMA: u32 = 3;
/// The value a storer leaves on the line when it abandons its window.
const ABANDONED: u64 = u64::MAX;
/// The band slot that means no load has been read yet.
const NO_BAND: u8 = u8::MAX;

/// Counter cycles per microsecond a rate may read: 100 MHz to 10 GHz, and
/// from 1 MHz on aarch64, whose counter runs at tens of megahertz.
const RATE_BOUNDS: (f64, f64) =
    if cfg!(target_arch = "aarch64") { (1.0, 10_000.0) } else { (100.0, 10_000.0) };
/// Cycles one spin hint may take: 1 to 100,000, and from 1/10,000 of a tick
/// on aarch64, where one hint is a fraction of a counter tick.
const PAUSE_BOUNDS: (f64, f64) =
    if cfg!(target_arch = "aarch64") { (1e-4, 100_000.0) } else { (1.0, 100_000.0) };
/// The fewest cycles one monitor timer unit may take: 64 units per cycle.
const MIN_CYCLES_PER_UNIT: f64 = 1.0 / 64.0;

/// The bounds of a wake latency, a hold, a tick or a floor: 1 cycle to 1 ms
/// at `rate_per_us`.
fn duration_bounds(rate_per_us: f64) -> (f64, f64) {
    (1.0, rate_per_us * 1_000.0)
}

fn within((low, high): (f64, f64), value: f64) -> bool {
    (low..=high).contains(&value)
}

/// `Err` naming `what` when `value` lies outside `bounds`.
fn check(what: &str, value: f64, bounds: (f64, f64)) -> Result<(), String> {
    if within(bounds, value) {
        Ok(())
    } else {
        Err(format!("{what} {value} lies outside {} to {}", bounds.0, bounds.1))
    }
}

/// One measured fact.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Fact {
    /// The smallest window statistic.
    pub value: f64,
    /// Busy logical processors across the window it came from, where the
    /// platform reads the load.
    pub busy: Option<f64>,
}

/// One window's statistic and the busy logical processors across it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Window {
    statistic: f64,
    busy: Option<f64>,
}

/// The fact `windows` give: the smallest statistic, with its window's busy
/// reading. `Err` when a statistic is not finite, or when the largest minus
/// the smallest exceeds their median.
fn take(windows: &[Window]) -> Result<Fact, String> {
    if windows.iter().any(|window| !window.statistic.is_finite()) {
        return Err(format!("a window's statistic is not finite: {windows:?}"));
    }
    let Some(least) = windows.iter().min_by(|a, b| a.statistic.total_cmp(&b.statistic)) else {
        return Err("no windows".to_string());
    };
    let mut sorted: Vec<f64> = windows.iter().map(|window| window.statistic).collect();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[(sorted.len() - 1) / 2];
    let spread = sorted[sorted.len() - 1] - sorted[0];
    if spread > median {
        return Err(format!("the windows spread {spread} past their median {median}: {sorted:?}"));
    }
    Ok(Fact { value: least.statistic, busy: least.busy })
}

/// Runs `window` [`WINDOWS`] times back to back, reading the machine's load
/// across each, and takes one fact per statistic. A window's `Err` ends the
/// facts with it.
fn over_windows_n<const N: usize>(
    mut window: impl FnMut() -> Result<[f64; N], String>,
) -> Result<[Fact; N], String> {
    let mut windows: Vec<([f64; N], Option<f64>)> = Vec::with_capacity(WINDOWS);
    for _ in 0..WINDOWS {
        let before = CpuTimes::read();
        let statistics = window()?;
        let busy = before
            .zip(CpuTimes::read())
            .and_then(|(before, after)| after.busy_since(&before));
        windows.push((statistics, busy));
    }
    let mut facts = [Fact { value: 0.0, busy: None }; N];
    for (index, fact) in facts.iter_mut().enumerate() {
        let column: Vec<Window> = windows
            .iter()
            .map(|(statistics, busy)| Window { statistic: statistics[index], busy: *busy })
            .collect();
        *fact = take(&column)?;
    }
    Ok(facts)
}

/// As [`over_windows_n`] for a window of one statistic.
fn over_windows(mut window: impl FnMut() -> Result<f64, String>) -> Result<Fact, String> {
    over_windows_n(|| window().map(|statistic| [statistic])).map(|[fact]| fact)
}

/// The counter, read after every earlier instruction has completed and
/// before any later one starts.
#[cfg(target_arch = "x86_64")]
fn stamp() -> u64 {
    // SAFETY: `LFENCE` has no precondition.
    unsafe { core::arch::asm!("lfence", options(nostack, preserves_flags)) };
    let now = read_tsc();
    // SAFETY: as above.
    unsafe { core::arch::asm!("lfence", options(nostack, preserves_flags)) };
    now
}

/// The counter, read after every earlier instruction has completed and
/// before any later one starts.
#[cfg(target_arch = "aarch64")]
fn stamp() -> u64 {
    // SAFETY: `ISB` has no precondition.
    unsafe { core::arch::asm!("isb", options(nostack, preserves_flags)) };
    let now = read_tsc();
    // SAFETY: as above.
    unsafe { core::arch::asm!("isb", options(nostack, preserves_flags)) };
    now
}

/// The counter.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn stamp() -> u64 {
    read_tsc()
}

/// Whether the counter reading `now` has reached `deadline`, across a wrap.
fn reached(now: u64, deadline: u64) -> bool {
    now.wrapping_sub(deadline) <= i64::MAX as u64
}

/// Whether a count of counter cycles is a length of time on this host: the
/// invariant TSC on x86_64, and always on aarch64, whose counter runs at one
/// rate by architecture.
fn invariant_counter() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        subetha_core::cpuid::cpuid().invariant_tsc
    }
    #[cfg(target_arch = "aarch64")]
    {
        true
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        false
    }
}

/// One window of the counter's rate: cycles per microsecond of the
/// monotonic clock across [`RATE_WINDOW`].
fn rate_window() -> Result<f64, String> {
    let started = Instant::now();
    let first = stamp();
    while started.elapsed() < RATE_WINDOW {
        std::hint::spin_loop();
    }
    let last = stamp();
    let micros = started.elapsed().as_secs_f64() * 1e6;
    Ok(last.wrapping_sub(first) as f64 / micros)
}

/// One window of the counter's tick: across [`TICK_READS`] back-to-back
/// reads, the commonest nonzero distance between the first reads of
/// successive ticks. The first read of a tick is one that is not exactly one
/// more than the read before it, since a counter can answer one more for
/// each read within a tick.
fn tick_window() -> Result<f64, String> {
    let mut reads = Vec::with_capacity(TICK_READS);
    for _ in 0..TICK_READS {
        reads.push(read_tsc());
    }
    let firsts: Vec<u64> = reads
        .windows(2)
        .filter(|pair| pair[1].wrapping_sub(pair[0]) != 1)
        .map(|pair| pair[1])
        .collect();
    let mut spacing: HashMap<u64, u64> = HashMap::new();
    for pair in firsts.windows(2) {
        let distance = pair[1].wrapping_sub(pair[0]);
        if distance != 0 {
            *spacing.entry(distance).or_insert(0) += 1;
        }
    }
    spacing
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
        .map(|(distance, _)| distance as f64)
        .ok_or_else(|| "the counter did not advance across the reads".to_string())
}

/// One window of a spin hint's cost: the fastest of [`PAUSE_BLOCKS`] blocks
/// of [`PAUSE_BLOCK`] hints, per hint.
fn pause_window() -> Result<f64, String> {
    let mut fastest = u64::MAX;
    for _ in 0..PAUSE_BLOCKS {
        let first = stamp();
        for _ in 0..PAUSE_BLOCK {
            std::hint::spin_loop();
        }
        fastest = fastest.min(stamp().wrapping_sub(first));
    }
    Ok(fastest as f64 / f64::from(PAUSE_BLOCK))
}

/// A value alone on its cache line and on the 128-byte granule a monitor can
/// watch, so a store to a neighbor never ends a wait on it.
#[repr(align(128))]
#[derive(Default)]
struct Line(AtomicU64);

/// The floor, guard floor and cycles per unit of one sweep, from its
/// per-request medians, `medians[k]` being the request of 2^k units. A unit
/// is one cycle, as both families' manuals state; the sweep's own largest
/// requests cannot say otherwise, since an interrupt ends any of them it
/// reaches. The guard floor is the longest any request took whose units
/// ran out within the smallest request's cycles. `Err` when the largest
/// request took no longer than the guard floor, the timer not holding.
#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
fn sweep_statistics(medians: &[u64]) -> Result<[f64; 3], String> {
    let (Some(&floor), Some(&top)) = (medians.first(), medians.last()) else {
        return Err("an empty sweep".to_string());
    };
    let cycles_per_unit = 1.0;
    let guard_floor = medians
        .iter()
        .enumerate()
        .filter(|&(power, _)| (1u64 << power) as f64 * cycles_per_unit < floor as f64)
        .fold(floor, |longest, (_, &median)| longest.max(median));
    if top <= guard_floor {
        return Err(format!(
            "the largest request took {top} cycles, within the guard floor {guard_floor}"
        ));
    }
    Ok([floor as f64, guard_floor as f64, cycles_per_unit])
}

/// One window of `family`'s sweep: requests of 2^0 to 2^[`SWEEP_TOP`] timer
/// units on a line nobody writes, each the median of [`SWEEP_READS`] reads.
/// A `UMWAIT` request is a deadline that many cycles ahead.
#[cfg(target_arch = "x86_64")]
fn sweep_window(family: MonitorFamily) -> Result<[f64; 3], String> {
    use crate::wait_instr::{Monitorx, Waitpkg};
    use core::num::NonZeroU32;

    let line = Line::default();
    let mut medians = Vec::with_capacity(SWEEP_TOP as usize + 1);
    for power in 0..=SWEEP_TOP {
        let request = NonZeroU32::new(1u32 << power).ok_or("a request of no units")?;
        let mut elapsed = [0u64; SWEEP_READS];
        for read in &mut elapsed {
            let first = stamp();
            match family {
                MonitorFamily::Mwaitx(hint) => {
                    let token = Monitorx::on_this_host().ok_or("this host has no MONITORX")?;
                    token.wait(&line.0, || true, hint, request);
                }
                MonitorFamily::Waitpkg(state) => {
                    let token = Waitpkg::on_this_host().ok_or("this host has no WAITPKG")?;
                    token.wait(&line.0, || true, state, first.wrapping_add(u64::from(request.get())));
                }
                MonitorFamily::ArmWfe => return Err("WFE has no timer to sweep".to_string()),
            }
            *read = stamp().wrapping_sub(first);
        }
        elapsed.sort_unstable();
        medians.push(elapsed[SWEEP_READS / 2]);
    }
    sweep_statistics(&medians)
}

/// No target but x86_64 has a timed monitor.
#[cfg(not(target_arch = "x86_64"))]
fn sweep_window(_family: MonitorFamily) -> Result<[f64; 3], String> {
    Err("this target has no timed monitor".to_string())
}

/// A timed monitor state's timer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timer {
    pub family: MonitorFamily,
    /// Cycles the smallest request took.
    pub floor: Fact,
    /// The longest a request took whose units ran out within the smallest
    /// request's cycles. An arm that returns this fast did not hold.
    pub guard_floor: Fact,
    /// Cycles per timer unit: 1 for both families, as their manuals state
    /// (an `MWAITX` count of TSC clocks, a `UMWAIT` deadline in TSC
    /// cycles). A part whose `MWAITX` timer runs faster ends each arm
    /// early, and the phase re-arms until its TSC deadline.
    pub cycles_per_unit: Fact,
}

/// What a core class's intrinsic measurement found.
#[derive(Debug, Clone, PartialEq)]
pub struct Intrinsic {
    /// Counter cycles per microsecond of the monotonic clock.
    pub rate_per_us: Fact,
    /// The counter's tick, in cycles.
    pub tick: Fact,
    /// Cycles one spin hint takes.
    pub pause: Fact,
    /// Each timed monitor state's timer.
    pub timers: Vec<Timer>,
    /// Each timer left out, with the reason.
    pub omitted: Vec<String>,
}

/// A core class's wake latencies in one load band, in counter cycles.
#[derive(Debug, Clone, PartialEq)]
pub struct WakeSet {
    /// A spinning waiter's.
    pub spin: Fact,
    /// A monitor-waiting waiter's, for each state taken.
    pub monitor: Vec<(MonitorFamily, Fact)>,
    /// A parked waiter's, for each park kind taken.
    pub park: Vec<(ParkKind, Fact)>,
    /// Each fact left out, with the reason.
    pub omitted: Vec<String>,
}

fn timer_in_bounds(timer: &Timer, rate_per_us: f64) -> Result<(), String> {
    let durations = duration_bounds(rate_per_us);
    check("the timer floor", timer.floor.value, durations)?;
    check("the guard floor", timer.guard_floor.value, durations)?;
    check("the cycles per timer unit", timer.cycles_per_unit.value, (MIN_CYCLES_PER_UNIT, f64::MAX))
}

fn busy_in_bounds(fact: &Fact, processors: u32) -> Result<(), String> {
    match fact.busy {
        Some(busy) => check("a busy reading", busy, (0.0, f64::from(processors))),
        None => Ok(()),
    }
}

/// `Err` naming the first value of `intrinsic` outside its bound, or busy
/// reading outside 0 to `processors`.
fn intrinsic_in_bounds(intrinsic: &Intrinsic, processors: u32) -> Result<(), String> {
    let rate = intrinsic.rate_per_us.value;
    check("the counter's rate", rate, RATE_BOUNDS)?;
    check("the counter's tick", intrinsic.tick.value, duration_bounds(rate))?;
    check("a spin hint", intrinsic.pause.value, PAUSE_BOUNDS)?;
    for timer in &intrinsic.timers {
        timer_in_bounds(timer, rate)?;
    }
    let facts = [intrinsic.rate_per_us, intrinsic.tick, intrinsic.pause].into_iter().chain(
        intrinsic.timers.iter().flat_map(|timer| [timer.floor, timer.guard_floor, timer.cycles_per_unit]),
    );
    for fact in facts {
        busy_in_bounds(&fact, processors)?;
    }
    Ok(())
}

/// `Err` naming the first wake of `wakes` outside 1 cycle to 1 ms at
/// `rate_per_us`, or busy reading outside 0 to `processors`.
fn wakes_in_bounds(wakes: &WakeSet, rate_per_us: f64, processors: u32) -> Result<(), String> {
    let durations = duration_bounds(rate_per_us);
    let facts = std::iter::once(("the spin wake", wakes.spin))
        .chain(wakes.monitor.iter().map(|(_, fact)| ("a monitor wake", *fact)))
        .chain(wakes.park.iter().map(|(_, fact)| ("a park wake", *fact)));
    for (what, fact) in facts {
        check(what, fact.value, durations)?;
        busy_in_bounds(&fact, processors)?;
    }
    Ok(())
}

/// The monitor states this host measures: `MWAITX` with each hint, `UMWAIT`
/// at C0.1, or `WFE`. None where the host has no family or the environment
/// switched the monitor off.
fn monitor_states() -> Vec<MonitorFamily> {
    match monitor_wait_kind() {
        Some(MonitorWaitKind::Mwaitx) => {
            vec![MonitorFamily::Mwaitx(MwaitxHint::C0), MonitorFamily::Mwaitx(MwaitxHint::C1)]
        }
        Some(MonitorWaitKind::Waitpkg) => vec![MonitorFamily::Waitpkg(UserWait::C01)],
        Some(MonitorWaitKind::ArmWfe) => vec![MonitorFamily::ArmWfe],
        None => Vec::new(),
    }
}

/// Measures a class's intrinsic facts on the calling thread. `Err` when the
/// rate, the tick or the spin hint is not taken or lies outside its bound; a
/// timer that is not is left out.
fn measure_intrinsic() -> Result<Intrinsic, String> {
    let rate_per_us =
        over_windows(rate_window).map_err(|e| format!("the counter's rate: {e}"))?;
    check("the counter's rate", rate_per_us.value, RATE_BOUNDS)?;
    let tick = over_windows(tick_window).map_err(|e| format!("the counter's tick: {e}"))?;
    check("the counter's tick", tick.value, duration_bounds(rate_per_us.value))?;
    let pause = over_windows(pause_window).map_err(|e| format!("a spin hint: {e}"))?;
    check("a spin hint", pause.value, PAUSE_BOUNDS)?;
    let mut timers = Vec::new();
    let mut omitted = Vec::new();
    for family in monitor_states() {
        if family == MonitorFamily::ArmWfe {
            continue;
        }
        let timer = over_windows_n(|| sweep_window(family)).and_then(
            |[floor, guard_floor, cycles_per_unit]| {
                let timer = Timer { family, floor, guard_floor, cycles_per_unit };
                timer_in_bounds(&timer, rate_per_us.value).map(|()| timer)
            },
        );
        match timer {
            Ok(timer) => timers.push(timer),
            Err(e) => omitted.push(format!("the {} timer: {e}", family_name(family))),
        }
    }
    Ok(Intrinsic { rate_per_us, tick, pause, timers, omitted })
}

/// What one measurement cost.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    /// Wall time from its start to its end.
    pub wall: Duration,
    /// Processor time of the thread that ran it.
    pub measuring: Option<Duration>,
    /// Processor time of its waiter threads, summed.
    pub waiters: Option<Duration>,
    /// Processor time of its storer threads, summed.
    pub storers: Option<Duration>,
}

/// Processor time a measurement's waiter and storer threads spent.
#[derive(Debug, Clone, Copy)]
struct Spent {
    waiter: Option<Duration>,
    storer: Option<Duration>,
}

impl Spent {
    fn zero() -> Spent {
        Spent { waiter: Some(Duration::ZERO), storer: Some(Duration::ZERO) }
    }
}

/// The calling thread's processor time, where the platform reports it.
#[cfg(unix)]
fn thread_cpu() -> Option<Duration> {
    let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `now` is a valid timespec for the call to write.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) };
    (rc == 0).then(|| Duration::new(now.tv_sec as u64, now.tv_nsec as u32))
}

/// The calling thread's processor time, where the platform reports it.
#[cfg(windows)]
fn thread_cpu() -> Option<Duration> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};

    let zero = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
    let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
    // SAFETY: the four `FILETIME` values are valid for the call to write,
    // and the pseudo handle names the calling thread.
    let ok = unsafe {
        GetThreadTimes(GetCurrentThread(), &mut created, &mut exited, &mut kernel, &mut user)
    };
    let hundreds =
        |time: FILETIME| (u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime);
    (ok != 0).then(|| Duration::from_nanos((hundreds(kernel) + hundreds(user)) * 100))
}

/// The calling thread's processor time, where the platform reports it.
#[cfg(not(any(unix, windows)))]
fn thread_cpu() -> Option<Duration> {
    None
}

/// The calling thread's processor time since `since`.
fn elapsed_cpu(since: Option<Duration>) -> Option<Duration> {
    Some(thread_cpu()?.saturating_sub(since?))
}

fn add(total: Option<Duration>, more: Option<Duration>) -> Option<Duration> {
    Some(total? + more?)
}

/// What a panicking thread said, where it said it as text.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        return (*text).to_string();
    }
    if let Some(text) = payload.downcast_ref::<String>() {
        return text.clone();
    }
    "a payload with no message".to_string()
}

/// A joined thread's outcome, with a panic as its `Err`.
fn joined<T>(result: std::thread::Result<Result<T, String>>, role: &str) -> Result<T, String> {
    match result {
        Ok(outcome) => outcome,
        Err(panic) => Err(format!("the {role} panicked: {}", panic_text(&*panic))),
    }
}

/// Pins the calling thread to `processor`, or says it could not.
fn pin_here(processor: u32, role: &str) -> Result<(), String> {
    if crate::cpu_affinity::pin_current_thread_to_core(processor as usize) {
        Ok(())
    } else {
        Err(format!("the {role} could not be pinned to logical processor {processor}"))
    }
}

/// Runs `work` on a new thread pinned to `processor`.
fn on_pinned_thread<T: Send>(processor: u32, work: impl FnOnce() -> T + Send) -> Result<T, String> {
    std::thread::scope(|scope| {
        let worker = std::thread::Builder::new()
            .name("subetha-wait-measure".to_string())
            .spawn_scoped(scope, move || pin_here(processor, "measuring thread").map(|()| work()))
            .map_err(|e| format!("the measuring thread did not start: {e}"))?;
        joined(worker.join(), "measuring thread")
    })
}

/// How a wake window's waiter waits for the store.
#[derive(Debug, Clone, Copy)]
enum Arm {
    Spin,
    Monitor(MonitorPhase),
    Park,
}

/// The lines a wake window's two threads share.
#[derive(Default)]
struct Trip {
    /// The trip number the storer last stored, or [`ABANDONED`].
    line: Line,
    /// The trip number the waiter is ready for.
    ready: Line,
    /// The counter as the storer stored.
    stamp: Line,
}

/// The plan a park window's waiter waits with: no spin and no monitor.
const PARK_NOW: Plan = Plan { spin_rounds: 0, monitor: None };

/// The two logical processors, in processor group 0, a pinned class's
/// waiter and storer run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pins {
    pub waiter: u32,
    pub storer: u32,
}

/// Leaves the line abandoned, and wakes every parked waiter, when dropped
/// before the storer has finished, so a waiter never outwaits a storer that
/// stopped.
struct Abandon<'a> {
    trip: &'a Trip,
    waker: Option<&'a CrossProcessWaker>,
    finished: bool,
}

impl Drop for Abandon<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.trip.line.0.store(ABANDONED, Ordering::Release);
            if let Some(waker) = self.waker {
                waker.wake_all();
            }
        }
    }
}

/// The storer's side of a wake window: for each trip, wait up to `bound`
/// until the waiter is ready, let `gap` cycles pass, stamp, store the trip
/// number, and wake `waker`. Answers its processor time.
fn store_trips(
    trip: &Trip,
    gap: u64,
    pin: Option<u32>,
    waker: Option<&CrossProcessWaker>,
    bound: Duration,
) -> Result<Option<Duration>, String> {
    let mut abandon = Abandon { trip, waker, finished: false };
    if let Some(processor) = pin {
        pin_here(processor, "storer")?;
    }
    let started_cpu = thread_cpu();
    for number in 1..=TRIPS {
        let asked = Instant::now();
        while trip.ready.0.load(Ordering::Acquire) != number {
            if asked.elapsed() > bound {
                return Err(format!("the waiter was not ready for trip {number} within {bound:?}"));
            }
            std::hint::spin_loop();
        }
        let due = stamp().wrapping_add(gap);
        while !reached(read_tsc(), due) {
            std::hint::spin_loop();
        }
        trip.stamp.0.store(stamp(), Ordering::Release);
        trip.line.0.store(number, Ordering::Release);
        if let Some(waker) = waker {
            waker.wake_up_to(number);
        }
    }
    abandon.finished = true;
    Ok(elapsed_cpu(started_cpu))
}

/// The waiter's side of a wake window: for each trip, report ready, wait by
/// `arm` until the store (a park waits up to `bound`), and time the return
/// against the storer's stamp. Answers the latencies and its processor time.
fn wait_trips(
    trip: &Trip,
    arm: Arm,
    pin: Option<u32>,
    waker: Option<&CrossProcessWaker>,
    bound: Duration,
) -> Result<(Vec<u64>, Option<Duration>), String> {
    if let Some(processor) = pin {
        pin_here(processor, "waiter")?;
    }
    let started_cpu = thread_cpu();
    let mut latencies = Vec::with_capacity(TRIPS as usize);
    for number in 1..=TRIPS {
        let before = number - 1;
        match arm {
            Arm::Spin => {
                trip.ready.0.store(number, Ordering::Release);
                while trip.line.0.load(Ordering::Acquire) == before {
                    std::hint::spin_loop();
                }
            }
            Arm::Monitor(phase) => {
                trip.ready.0.store(number, Ordering::Release);
                if !run_monitor_phase_u64(&phase, &trip.line.0, before) {
                    return Err(format!("no store ended the monitor wait of trip {number} within its bound"));
                }
            }
            Arm::Park => {
                let waker = waker.ok_or("a park window has no waker")?;
                let token = waker
                    .try_park(number)
                    .map_err(|e| format!("parking for trip {number}: {e:?}"))?;
                trip.ready.0.store(number, Ordering::Release);
                waker
                    .wait_with_plan(token, Some(bound), &PARK_NOW)
                    .map_err(|e| format!("the park of trip {number}: {e:?}"))?;
            }
        }
        let woke = stamp();
        match trip.line.0.load(Ordering::Acquire) {
            value if value == number => {
                latencies.push(woke.wrapping_sub(trip.stamp.0.load(Ordering::Acquire)));
            }
            ABANDONED => return Err("the storer abandoned the window".to_string()),
            value => return Err(format!("trip {number} returned with the line at {value}")),
        }
    }
    Ok((latencies, elapsed_cpu(started_cpu)))
}

/// One wake window: [`TRIPS`] round trips of a stamped store `gap` cycles
/// after the waiter reports ready, each abandoned past `bound`, answering
/// the median cycles from the stamp to the waiter's return. Adds its
/// threads' processor time to `spent`.
fn wake_window(
    arm: Arm,
    gap: u64,
    pins: Option<Pins>,
    waker: Option<&CrossProcessWaker>,
    bound: Duration,
    spent: &mut Spent,
) -> Result<f64, String> {
    let trip = Trip::default();
    #[cfg(test)]
    let hold = crate::test_races::take_waiter_hold();
    std::thread::scope(|scope| {
        let storer = std::thread::Builder::new()
            .name("subetha-wait-storer".to_string())
            .spawn_scoped(scope, || store_trips(&trip, gap, pins.map(|p| p.storer), waker, bound))
            .map_err(|e| format!("the storer did not start: {e}"))?;
        let waiter = std::thread::Builder::new()
            .name("subetha-wait-waiter".to_string())
            .spawn_scoped(scope, || {
                #[cfg(test)]
                if let Some(hold) = hold {
                    std::thread::sleep(hold);
                }
                wait_trips(&trip, arm, pins.map(|p| p.waiter), waker, bound)
            });
        let waited = match waiter {
            Ok(waiter) => joined(waiter.join(), "waiter"),
            Err(e) => Err(format!("the waiter did not start: {e}")),
        };
        let stored = joined(storer.join(), "storer");
        let (mut latencies, waiter_cpu) = waited?;
        let storer_cpu = stored?;
        spent.waiter = add(spent.waiter, waiter_cpu);
        spent.storer = add(spent.storer, storer_cpu);
        latencies.sort_unstable();
        latencies
            .get(latencies.len() / 2)
            .map(|&median| median as f64)
            .ok_or_else(|| "a window of no trips".to_string())
    })
}

/// Measures a class's wake set with `intrinsic`'s counter rate and timers,
/// the waiter and storer at `pins`. `Err` when the spin wake is not taken or
/// lies outside its bound; any other wake that is not is left out. The
/// cross-process park uses a file-backed waker in `dir`, removed afterward.
///
/// A park is timed twice. A wait parks only once it has spun to its hold,
/// so the park it meets is woken soon after it starts, and on some hosts a
/// park costs less the shorter it lasts. The first window, its store
/// [`PARK_GAP`] after the waiter parks, gives a hold; the second lands its
/// store that hold after the park, and its reading is the fact.
fn measure_wakes(
    intrinsic: &Intrinsic,
    pins: Option<Pins>,
    dir: Option<&Path>,
    spent: &mut Spent,
) -> Result<WakeSet, String> {
    let rate = intrinsic.rate_per_us.value;
    let cycles = |span: Duration| (span.as_secs_f64() * 1e6 * rate) as u64;
    let durations = duration_bounds(rate);
    let bounded = |fact: Fact, what: &str| check(what, fact.value, durations).map(|()| fact);

    let spin = over_windows(|| wake_window(Arm::Spin, cycles(SHORT_GAP), pins, None, TRIP_BOUND, spent))
        .map_err(|e| format!("the spin wake: {e}"))?;
    let spin = bounded(spin, "the spin wake")?;

    let mut omitted = Vec::new();
    let mut monitor = Vec::new();
    for family in monitor_states() {
        let timer = intrinsic.timers.iter().find(|timer| timer.family == family);
        let units_per_cycle = match (family, timer) {
            (MonitorFamily::ArmWfe, _) => 1.0,
            (_, Some(timer)) => 1.0 / timer.cycles_per_unit.value,
            (_, None) => {
                omitted.push(format!("the {} wake: its timer was not taken", family_name(family)));
                continue;
            }
        };
        let phase = MonitorPhase {
            family,
            budget_cycles: cycles(TRIP_BOUND),
            units_per_cycle,
            guard_floor_cycles: None,
        };
        let wake = over_windows(|| wake_window(Arm::Monitor(phase), cycles(SHORT_GAP), pins, None, TRIP_BOUND, spent))
            .and_then(|fact| bounded(fact, "the monitor wake"));
        match wake {
            Ok(fact) => monitor.push((family, fact)),
            Err(e) => omitted.push(format!("the {} wake: {e}", family_name(family))),
        }
    }

    let park_at_hold = |waker: &CrossProcessWaker, spent: &mut Spent| -> Result<Fact, String> {
        let first = over_windows(|| wake_window(Arm::Park, cycles(PARK_GAP), pins, Some(waker), TRIP_BOUND, spent))
            .and_then(|fact| bounded(fact, "the park wake"))?;
        let hold = (first.value - spin.value).max(0.0) as u64;
        over_windows(|| wake_window(Arm::Park, hold, pins, Some(waker), TRIP_BOUND, spent))
            .and_then(|fact| bounded(fact, "the park wake at the hold"))
    };

    let mut park = Vec::new();
    let local = CrossProcessWaker::create_anon(1)
        .map_err(|e| format!("the waker: {e:?}"))
        .and_then(|waker| park_at_hold(&waker, spent));
    match local {
        Ok(fact) => park.push((ParkKind::Local, fact)),
        Err(e) => omitted.push(format!("the local park wake: {e}")),
    }
    match dir {
        Some(dir) => {
            let path = dir.join(format!("wait-calibration-{}.waker", std::process::id()));
            let cross = CrossProcessWaker::reset(&path, 1)
                .map_err(|e| format!("the file-backed waker: {e:?}"))
                .and_then(|waker| park_at_hold(&waker, spent));
            if let Err(e) = std::fs::remove_file(&path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                omitted.push(format!("the waker file {} was left: {e}", path.display()));
            }
            match cross {
                Ok(fact) => park.push((ParkKind::CrossProcess, fact)),
                Err(e) => omitted.push(format!("the cross-process park wake: {e}")),
            }
        }
        None => omitted
            .push("the cross-process park wake: there is no cache directory for its waker".to_string()),
    }
    Ok(WakeSet { spin, monitor, park, omitted })
}

/// The monitor facts of `family` from a class's timers, with its wake. `None`
/// for a timed family whose timer was not taken.
fn monitor_facts(intrinsic: &Intrinsic, family: MonitorFamily, wake: f64) -> Option<MonitorFacts> {
    let (floor_cycles, units_per_cycle) = match family {
        MonitorFamily::ArmWfe => (0, 1.0),
        MonitorFamily::Mwaitx(_) | MonitorFamily::Waitpkg(_) => {
            let timer = intrinsic.timers.iter().find(|timer| timer.family == family)?;
            (timer.guard_floor.value as u64, 1.0 / timer.cycles_per_unit.value)
        }
    };
    Some(MonitorFacts { family, wake: wake as u64, floor_cycles, units_per_cycle })
}

/// The plan for a `park` wait from a class's intrinsic facts and one band's
/// wake set, with the monitor state of the lowest wake among those that have
/// their facts. `None` when the set has no park of that kind, or when the
/// hold, the park's wake less the spin's, lies outside 1 cycle to 1 ms.
fn plan_of(intrinsic: &Intrinsic, wakes: &WakeSet, park: ParkKind, overrides: &Overrides) -> Option<Plan> {
    let park_wake = wakes.park.iter().find(|(kind, _)| *kind == park)?.1.value;
    if !within(duration_bounds(intrinsic.rate_per_us.value), park_wake - wakes.spin.value) {
        return None;
    }
    let monitor = wakes
        .monitor
        .iter()
        .filter_map(|(family, wake)| monitor_facts(intrinsic, *family, wake.value))
        .min_by_key(|facts| facts.wake);
    let facts = Facts {
        pause_cycles: intrinsic.pause.value,
        spin_wake: wakes.spin.value as u64,
        park_wake: park_wake as u64,
        monitor,
        invariant_tsc: invariant_counter(),
    };
    Some(derive(&facts, overrides))
}

/// Where a record stands in this process.
#[derive(Debug, Clone, PartialEq)]
pub enum Record<T> {
    /// Not taken.
    Absent,
    /// Being measured.
    Measuring,
    /// In force.
    Ready { record: T, source: Source },
    /// Not published, with the reason.
    Refused(String),
}

/// Where a record in force came from.
#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    /// The cache.
    Cache,
    /// A measurement in this process, with its cost and whether it was
    /// cached.
    Measured { cost: Box<Cost>, cached: Result<(), String> },
}

/// Where a class's measurement threads run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// Where the operating system puts them.
    Unpinned,
    /// On two cores of the class.
    Pinned(Pins),
    /// Nowhere: the class is not measured, for the reason given.
    Unavailable(String),
}

/// One class of core the calibration measures.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Class {
    /// The name its cache files and its report carry.
    label: String,
    placement: Placement,
}

/// The name a class's cache files and report carry.
fn class_label(class: Option<CoreClass>) -> String {
    match class {
        None => "unclassified".to_string(),
        Some(CoreClass::Efficiency(efficiency)) => format!("efficiency-class-{efficiency}"),
        Some(CoreClass::Cpuid(CoreKind::Performance)) => "performance".to_string(),
        Some(CoreClass::Cpuid(CoreKind::Efficiency)) => "efficiency".to_string(),
        Some(CoreClass::Cpuid(CoreKind::Other(kind))) => format!("core-type-{kind}"),
    }
}

/// The classes `topology` holds, in the order their processors first
/// appear, and the class index of each logical processor number in group 0.
/// A topology of fewer than two classes, or none, is the one unpinned class
/// `all`. With `pinning`, each class of a larger one is pinned to its first
/// processor in group 0 and to the first on another core; a class without
/// two such cores, or any class without `pinning`, is unavailable.
fn classes_of(topology: Option<&Topology>, pinning: bool) -> (Vec<Class>, Vec<Option<usize>>) {
    let one = || (vec![Class { label: "all".to_string(), placement: Placement::Unpinned }], Vec::new());
    let Some(topology) = topology else {
        return one();
    };
    let mut kinds: Vec<Option<CoreClass>> = Vec::new();
    for processor in &topology.processors {
        if !kinds.contains(&processor.class) {
            kinds.push(processor.class);
        }
    }
    if kinds.len() < 2 {
        return one();
    }
    let mut classes = Vec::new();
    let mut lookup: Vec<Option<usize>> = Vec::new();
    for (index, kind) in kinds.iter().enumerate() {
        let members: Vec<&LogicalProcessor> = topology
            .processors
            .iter()
            .filter(|processor| processor.class == *kind && processor.group == 0)
            .collect();
        let first = members.first();
        let second = first.and_then(|first| members.iter().find(|p| p.core != first.core));
        let placement = match (pinning, first, second) {
            (false, _, _) => Placement::Unavailable("this platform does not pin threads".to_string()),
            (true, Some(waiter), Some(storer)) => {
                Placement::Pinned(Pins { waiter: waiter.number, storer: storer.number })
            }
            (true, _, _) => Placement::Unavailable(
                "the class has fewer than two cores in processor group 0".to_string(),
            ),
        };
        for member in &members {
            let slot = member.number as usize;
            if lookup.len() <= slot {
                lookup.resize(slot + 1, None);
            }
            lookup[slot] = Some(index);
        }
        classes.push(Class { label: class_label(*kind), placement });
    }
    (classes, lookup)
}

/// The processor group and number the calling thread is running on.
#[cfg(windows)]
fn current_processor() -> Option<(u16, u32)> {
    #[repr(C)]
    struct ProcessorNumber {
        group: u16,
        number: u8,
        _reserved: u8,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcessorNumberEx(number: *mut ProcessorNumber);
    }
    let mut here = ProcessorNumber { group: 0, number: 0, _reserved: 0 };
    // SAFETY: `here` is a valid `PROCESSOR_NUMBER` for the call to write.
    unsafe { GetCurrentProcessorNumberEx(&mut here) };
    Some((here.group, u32::from(here.number)))
}

/// The processor group and number the calling thread is running on.
#[cfg(target_os = "linux")]
fn current_processor() -> Option<(u16, u32)> {
    // SAFETY: sched_getcpu has no precondition; it answers -1 on failure.
    let number = unsafe { libc::sched_getcpu() };
    (number >= 0).then_some((0, number as u32))
}

/// The processor group and number the calling thread is running on.
#[cfg(not(any(windows, target_os = "linux")))]
fn current_processor() -> Option<(u16, u32)> {
    None
}

/// What a cache record must match to be read.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Key {
    /// The processor's vendor, family, model and stepping.
    cpu: String,
    /// The machine's logical processor count.
    processors: u32,
    /// The hypervisor's vendor, or `none`.
    hypervisor: String,
}

impl Key {
    fn host(processors: u32) -> Key {
        let cpuid = subetha_core::cpuid::cpuid();
        let vendor = if cpuid.vendor.is_empty() { "-" } else { cpuid.vendor.as_str() };
        let hypervisor = match &cpuid.hypervisor {
            Some(hypervisor) => hypervisor.vendor.clone(),
            None => "none".to_string(),
        };
        Key {
            cpu: format!("{vendor} {} {} {}", cpuid.family, cpuid.model, cpuid.stepping),
            processors,
            hypervisor,
        }
    }
}

fn family_name(family: MonitorFamily) -> &'static str {
    match family {
        MonitorFamily::Mwaitx(MwaitxHint::C0) => "mwaitx-c0",
        MonitorFamily::Mwaitx(MwaitxHint::C1) => "mwaitx-c1",
        MonitorFamily::Waitpkg(UserWait::C01) => "umwait-c0.1",
        MonitorFamily::Waitpkg(UserWait::C02) => "umwait-c0.2",
        MonitorFamily::ArmWfe => "wfe",
    }
}

fn family_from_name(name: &str) -> Result<MonitorFamily, String> {
    [
        MonitorFamily::Mwaitx(MwaitxHint::C0),
        MonitorFamily::Mwaitx(MwaitxHint::C1),
        MonitorFamily::Waitpkg(UserWait::C01),
        MonitorFamily::Waitpkg(UserWait::C02),
        MonitorFamily::ArmWfe,
    ]
    .into_iter()
    .find(|family| family_name(*family) == name)
    .ok_or_else(|| format!("no monitor state is named {name:?}"))
}

fn park_name(park: ParkKind) -> &'static str {
    match park {
        ParkKind::Local => "local",
        ParkKind::CrossProcess => "cross-process",
    }
}

fn park_from_name(name: &str) -> Result<ParkKind, String> {
    [ParkKind::Local, ParkKind::CrossProcess]
        .into_iter()
        .find(|park| park_name(*park) == name)
        .ok_or_else(|| format!("no park kind is named {name:?}"))
}

fn band_name(band: LoadBand) -> &'static str {
    match band {
        LoadBand::UnderQuarter => "under-25",
        LoadBand::UnderHalf => "25-to-50",
        LoadBand::UnderThreeQuarters => "50-to-75",
        LoadBand::Rest => "75-and-over",
    }
}

/// The band's position in [`LoadBand::ALL`].
fn band_slot(band: LoadBand) -> u8 {
    match band {
        LoadBand::UnderQuarter => 0,
        LoadBand::UnderHalf => 1,
        LoadBand::UnderThreeQuarters => 2,
        LoadBand::Rest => 3,
    }
}

fn intrinsic_file(label: &str) -> String {
    format!("wait-{label}-intrinsic.txt")
}

fn band_file(label: &str, band: LoadBand) -> String {
    format!("wait-{label}-band-{}.txt", band_name(band))
}

fn band_record(band: LoadBand) -> String {
    format!("band {}", band_name(band))
}

/// A fact as the cache writes it: the value, then the busy reading or `-`.
fn fact_words(fact: &Fact) -> String {
    match fact.busy {
        Some(busy) => format!("{} {busy}", fact.value),
        None => format!("{} -", fact.value),
    }
}

fn parse_number(word: &str) -> Result<f64, String> {
    word.parse::<f64>().map_err(|e| format!("{word:?} is not a number: {e}"))
}

fn parse_fact(value: &str, busy: &str) -> Result<Fact, String> {
    let busy = if busy == "-" { None } else { Some(parse_number(busy)?) };
    Ok(Fact { value: parse_number(value)?, busy })
}

/// A record's text: its header, naming the host, the class and the record,
/// then `lines`.
fn record_text(key: &Key, class: &str, record: &str, lines: &[String]) -> String {
    let mut text = format!(
        "subetha-wait {SCHEMA}\ncpu {}\nprocessors {}\nhypervisor {}\nclass {class}\nrecord {record}\n",
        key.cpu, key.processors, key.hypervisor
    );
    for line in lines {
        text.push_str(line);
        text.push('\n');
    }
    text
}

/// The body lines of `text`, each split into words. `Err` unless its header
/// names this schema, `key`, `class` and `record`.
fn record_body<'t>(text: &'t str, key: &Key, class: &str, record: &str) -> Result<Vec<Vec<&'t str>>, String> {
    let header = [
        format!("subetha-wait {SCHEMA}"),
        format!("cpu {}", key.cpu),
        format!("processors {}", key.processors),
        format!("hypervisor {}", key.hypervisor),
        format!("class {class}"),
        format!("record {record}"),
    ];
    let mut lines = text.lines();
    for expected in &header {
        match lines.next() {
            Some(line) if line == expected.as_str() => {}
            Some(line) => return Err(format!("the header reads {line:?} where this host expects {expected:?}")),
            None => return Err("the record ends inside its header".to_string()),
        }
    }
    Ok(lines.map(|line| line.split_whitespace().collect()).collect())
}

fn intrinsic_lines(intrinsic: &Intrinsic) -> Vec<String> {
    let mut lines = vec![
        format!("rate {}", fact_words(&intrinsic.rate_per_us)),
        format!("tick {}", fact_words(&intrinsic.tick)),
        format!("pause {}", fact_words(&intrinsic.pause)),
    ];
    for timer in &intrinsic.timers {
        lines.push(format!(
            "timer {} {} {} {}",
            family_name(timer.family),
            fact_words(&timer.floor),
            fact_words(&timer.guard_floor),
            fact_words(&timer.cycles_per_unit)
        ));
    }
    lines
}

/// An intrinsic record's body. `Err` names a line that is unknown, repeated
/// or malformed, or a required line that is missing.
fn parse_intrinsic(body: &[Vec<&str>]) -> Result<Intrinsic, String> {
    let (mut rate_per_us, mut tick, mut pause) = (None, None, None);
    let mut timers: Vec<Timer> = Vec::new();
    for words in body {
        match words.as_slice() {
            ["rate", value, busy] if rate_per_us.is_none() => rate_per_us = Some(parse_fact(value, busy)?),
            ["tick", value, busy] if tick.is_none() => tick = Some(parse_fact(value, busy)?),
            ["pause", value, busy] if pause.is_none() => pause = Some(parse_fact(value, busy)?),
            ["timer", family, floor, floor_busy, guard, guard_busy, unit, unit_busy] => {
                let family = family_from_name(family)?;
                if timers.iter().any(|timer| timer.family == family) {
                    return Err(format!("a second {} timer", family_name(family)));
                }
                timers.push(Timer {
                    family,
                    floor: parse_fact(floor, floor_busy)?,
                    guard_floor: parse_fact(guard, guard_busy)?,
                    cycles_per_unit: parse_fact(unit, unit_busy)?,
                });
            }
            _ => return Err(format!("an unknown or repeated line: {}", words.join(" "))),
        }
    }
    Ok(Intrinsic {
        rate_per_us: rate_per_us.ok_or("no rate line")?,
        tick: tick.ok_or("no tick line")?,
        pause: pause.ok_or("no pause line")?,
        timers,
        omitted: Vec::new(),
    })
}

fn wake_lines(wakes: &WakeSet) -> Vec<String> {
    let mut lines = vec![format!("spin {}", fact_words(&wakes.spin))];
    for (family, fact) in &wakes.monitor {
        lines.push(format!("monitor {} {}", family_name(*family), fact_words(fact)));
    }
    for (park, fact) in &wakes.park {
        lines.push(format!("park {} {}", park_name(*park), fact_words(fact)));
    }
    lines
}

/// A wake record's body. `Err` names a line that is unknown, repeated or
/// malformed, or the missing spin line.
fn parse_wakes(body: &[Vec<&str>]) -> Result<WakeSet, String> {
    let mut spin = None;
    let mut monitor: Vec<(MonitorFamily, Fact)> = Vec::new();
    let mut park: Vec<(ParkKind, Fact)> = Vec::new();
    for words in body {
        match words.as_slice() {
            ["spin", value, busy] if spin.is_none() => spin = Some(parse_fact(value, busy)?),
            ["monitor", family, value, busy] => {
                let family = family_from_name(family)?;
                if monitor.iter().any(|(taken, _)| *taken == family) {
                    return Err(format!("a second {} wake", family_name(family)));
                }
                monitor.push((family, parse_fact(value, busy)?));
            }
            ["park", kind, value, busy] => {
                let kind = park_from_name(kind)?;
                if park.iter().any(|(taken, _)| *taken == kind) {
                    return Err(format!("a second {} park wake", park_name(kind)));
                }
                park.push((kind, parse_fact(value, busy)?));
            }
            _ => return Err(format!("an unknown or repeated line: {}", words.join(" "))),
        }
    }
    Ok(WakeSet { spin: spin.ok_or("no spin line")?, monitor, park, omitted: Vec::new() })
}

/// Writes `text` to `name` in `dir` under a temporary name, then renames it
/// into place, so a reader finds a whole record.
fn write_record(dir: &Path, name: &str, text: &str) -> Result<(), String> {
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let target = dir.join(name);
    let temporary = dir.join(format!(
        "{name}.{}.{}.tmp",
        std::process::id(),
        WRITES.fetch_add(1, Ordering::Relaxed)
    ));
    let outcome = std::fs::write(&temporary, text)
        .map_err(|e| format!("writing {}: {e}", temporary.display()))
        .and_then(|()| {
            std::fs::rename(&temporary, &target).map_err(|e| {
                format!("renaming {} to {}: {e}", temporary.display(), target.display())
            })
        });
    match outcome {
        Ok(()) => Ok(()),
        Err(reason) => match std::fs::remove_file(&temporary) {
            Ok(()) => Err(reason),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(reason),
            Err(e) => Err(format!("{reason}; {} was left: {e}", temporary.display())),
        },
    }
}

/// This user's cache directory: `subetha` in the per-user temporary
/// directory.
#[cfg(windows)]
pub fn cache_dir() -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join("subetha");
    std::fs::create_dir_all(&dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    Ok(dir)
}

/// This user's cache directory: `subetha` in `$XDG_RUNTIME_DIR` where it is
/// set, else `subetha-<uid>` in the temporary directory. A directory it
/// creates has mode 0700, and one that is not a directory, or belongs to
/// another user, is refused.
#[cfg(unix)]
pub fn cache_dir() -> Result<PathBuf, String> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};

    // SAFETY: geteuid has no precondition and cannot fail.
    let uid = unsafe { libc::geteuid() };
    let dir = match std::env::var_os("XDG_RUNTIME_DIR").filter(|runtime| !runtime.is_empty()) {
        Some(runtime) => PathBuf::from(runtime).join("subetha"),
        None => std::env::temp_dir().join(format!("subetha-{uid}")),
    };
    match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(format!("creating {}: {e}", dir.display())),
    }
    let metadata =
        std::fs::symlink_metadata(&dir).map_err(|e| format!("reading {}: {e}", dir.display()))?;
    if !metadata.is_dir() {
        return Err(format!("{} is not a directory", dir.display()));
    }
    if metadata.uid() != uid {
        return Err(format!("{} belongs to user {}, not {uid}", dir.display(), metadata.uid()));
    }
    Ok(dir)
}

/// This platform has no per-user cache directory.
#[cfg(not(any(unix, windows)))]
pub fn cache_dir() -> Result<PathBuf, String> {
    Err("this platform has no per-user cache directory".to_string())
}

/// One band's plans for a class.
#[derive(Debug, Clone, Copy, PartialEq)]
struct BandPlans {
    local: Option<Plan>,
    cross_process: Option<Plan>,
}

/// A class's records and published plans. The calibration thread
/// replaces a record whole; a reader takes the record as it stands.
struct ClassState {
    class: Class,
    intrinsic: SwapCell<Record<Intrinsic>>,
    /// One per band, as [`LoadBand::ALL`].
    bands: [SwapCell<Record<WakeSet>>; 4],
    /// One per band, as [`LoadBand::ALL`], set once.
    plans: [OnceLock<BandPlans>; 4],
}

impl ClassState {
    fn new(class: Class) -> ClassState {
        ClassState {
            class,
            intrinsic: SwapCell::new(Record::Absent),
            bands: std::array::from_fn(|_band| SwapCell::new(Record::Absent)),
            plans: std::array::from_fn(|_| OnceLock::new()),
        }
    }
}

/// One process's calibration.
struct Calibrator {
    /// The cache directory, where there is one; the reason there is not is
    /// the first note.
    dir: Option<PathBuf>,
    key: Key,
    overrides: Overrides,
    classes: Vec<ClassState>,
    /// The class index of each logical processor number in group 0, where
    /// there is more than one class.
    lookup: Vec<Option<usize>>,
    /// The band of the last load reading, as [`band_slot`], or [`NO_BAND`].
    band: AtomicU8,
    /// The last load reading's busy logical processors.
    busy: SwapCellOption<f64>,
    /// Cache files that were not read, and a missing cache directory, with
    /// the reason. A note is added by copying the list, since notes are few.
    notes: SwapCell<Vec<String>>,
}

impl Calibrator {
    fn new(dir: Result<PathBuf, String>, topology: Option<&Topology>) -> Result<Calibrator, String> {
        let processors = CpuTimes::read()
            .ok_or("this platform has no reading of the machine's load")?
            .processors();
        let mut notes = Vec::new();
        let dir = match dir {
            Ok(dir) => Some(dir),
            Err(reason) => {
                notes.push(format!("no cache directory: {reason}"));
                None
            }
        };
        let (classes, lookup) = classes_of(topology, cfg!(any(windows, target_os = "linux")));
        Ok(Calibrator {
            dir,
            key: Key::host(processors),
            overrides: crate::wait_active::overrides(),
            classes: classes.into_iter().map(ClassState::new).collect(),
            lookup,
            band: AtomicU8::new(NO_BAND),
            busy: SwapCellOption::empty(),
            notes: SwapCell::new(notes),
        })
    }

    /// Adds `note` to the notes.
    fn note(&self, note: String) {
        self.notes.rcu(|notes| {
            let mut next = Vec::clone(notes);
            next.push(note.clone());
            next
        });
    }

    /// The calibration thread's loop: each class's intrinsic facts, then a
    /// load reading every [`LOAD_PERIOD`] and each class's wake set for a
    /// band it has not taken. A reading never spans a measurement.
    fn run(&self) {
        for index in 0..self.classes.len() {
            self.take_intrinsic(index);
        }
        let Some(mut earlier) = CpuTimes::read() else {
            return;
        };
        loop {
            std::thread::sleep(LOAD_PERIOD);
            let Some(now) = CpuTimes::read() else {
                continue;
            };
            let reading = now.busy_since(&earlier);
            earlier = now;
            let Some(busy) = reading else {
                continue;
            };
            let band = LoadBand::of(busy, now.processors());
            self.busy.store(Some(Arc::new(busy)));
            self.band.store(band_slot(band), Ordering::Relaxed);
            let mut measured = false;
            for index in 0..self.classes.len() {
                measured |= self.take_band(index, band);
            }
            if measured && let Some(after) = CpuTimes::read() {
                earlier = after;
            }
        }
    }

    /// Record `name` from the cache, its body parsed by `parse`, when its
    /// header names this host, `class` and `record`. `None` when there is no
    /// cache directory or no such file; any other failure is noted.
    fn cached<T>(
        &self,
        name: &str,
        class: &str,
        record: &str,
        parse: impl Fn(&[Vec<&str>]) -> Result<T, String>,
    ) -> Option<T> {
        let dir = self.dir.as_ref()?;
        let path = dir.join(name);
        let parsed = match std::fs::read_to_string(&path) {
            Ok(text) => record_body(&text, &self.key, class, record).and_then(|body| parse(&body)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => Err(format!("reading it: {e}")),
        };
        match parsed {
            Ok(value) => Some(value),
            Err(reason) => {
                self.note(format!("{}: {reason}", path.display()));
                None
            }
        }
    }

    fn store(&self, name: &str, text: &str) -> Result<(), String> {
        match &self.dir {
            Some(dir) => write_record(dir, name, text),
            None => Err("there is no cache directory".to_string()),
        }
    }

    /// Takes class `index`'s intrinsic facts from the cache, or measures and
    /// caches them, then every band the cache holds for the class.
    fn take_intrinsic(&self, index: usize) {
        let state = &self.classes[index];
        let label = &state.class.label;
        let name = intrinsic_file(label);
        let cached = self.cached(&name, label, "intrinsic", parse_intrinsic).filter(|intrinsic| {
            match intrinsic_in_bounds(intrinsic, self.key.processors) {
                Ok(()) => true,
                Err(reason) => {
                    self.note(format!("{name}: {reason}"));
                    false
                }
            }
        });
        let record = match cached {
            Some(record) => Record::Ready { record, source: Source::Cache },
            None => {
                state.intrinsic.store(Arc::new(Record::Measuring));
                let measured = match &state.class.placement {
                    Placement::Unavailable(reason) => Err(reason.clone()),
                    Placement::Unpinned => Ok(measure_intrinsic_costed()),
                    Placement::Pinned(pins) => on_pinned_thread(pins.waiter, measure_intrinsic_costed),
                };
                match measured {
                    Ok((Ok(intrinsic), cost)) => {
                        let text = record_text(&self.key, label, "intrinsic", &intrinsic_lines(&intrinsic));
                        let cached = self.store(&name, &text);
                        Record::Ready { record: intrinsic, source: Source::Measured { cost: Box::new(cost), cached } }
                    }
                    Ok((Err(reason), _)) | Err(reason) => Record::Refused(reason),
                }
            }
        };
        state.intrinsic.store(Arc::new(record));
        self.load_bands(index);
    }

    /// Takes each band of class `index` that the cache holds within its
    /// bounds, and publishes its plans.
    fn load_bands(&self, index: usize) {
        let state = &self.classes[index];
        let intrinsic = match &*state.intrinsic.load_full() {
            Record::Ready { record, .. } => record.clone(),
            Record::Absent | Record::Measuring | Record::Refused(_) => return,
        };
        let label = &state.class.label;
        for band in LoadBand::ALL {
            let name = band_file(label, band);
            let Some(wakes) = self.cached(&name, label, &band_record(band), parse_wakes) else {
                continue;
            };
            if let Err(reason) = wakes_in_bounds(&wakes, intrinsic.rate_per_us.value, self.key.processors) {
                self.note(format!("{name}: {reason}"));
                continue;
            }
            self.publish(index, band, &intrinsic, &wakes);
            state.bands[usize::from(band_slot(band))]
                .store(Arc::new(Record::Ready { record: wakes, source: Source::Cache }));
        }
    }

    /// Measures, caches and publishes class `index`'s wake set for `band`
    /// when the class has its intrinsic facts and the band has not been
    /// taken. Answers whether it measured.
    fn take_band(&self, index: usize, band: LoadBand) -> bool {
        let state = &self.classes[index];
        let slot = usize::from(band_slot(band));
        let intrinsic = match &*state.intrinsic.load_full() {
            Record::Ready { record, .. } => record.clone(),
            Record::Absent | Record::Measuring | Record::Refused(_) => return false,
        };
        let pins = match &state.class.placement {
            Placement::Pinned(pins) => Some(*pins),
            Placement::Unpinned => None,
            Placement::Unavailable(_) => return false,
        };
        // Claim the band: only a record still absent moves to measuring,
        // and only the caller whose swap replaced it goes on to measure.
        let absent = state.bands[slot].load_full();
        if !matches!(*absent, Record::Absent) {
            return false;
        }
        if state.bands[slot].compare_and_set(&absent, Arc::new(Record::Measuring)).is_err() {
            return false;
        }
        let started = Instant::now();
        let measuring = thread_cpu();
        let mut spent = Spent::zero();
        let outcome = measure_wakes(&intrinsic, pins, self.dir.as_deref(), &mut spent);
        let cost = Cost {
            wall: started.elapsed(),
            measuring: elapsed_cpu(measuring),
            waiters: spent.waiter,
            storers: spent.storer,
        };
        let record = match outcome {
            Ok(wakes) => {
                let label = &state.class.label;
                let text = record_text(&self.key, label, &band_record(band), &wake_lines(&wakes));
                let cached = self.store(&band_file(label, band), &text);
                self.publish(index, band, &intrinsic, &wakes);
                Record::Ready { record: wakes, source: Source::Measured { cost: Box::new(cost), cached } }
            }
            Err(reason) => Record::Refused(reason),
        };
        state.bands[slot].store(Arc::new(record));
        true
    }

    /// Publishes class `index`'s plans for `band` from its intrinsic facts
    /// and the band's wake set.
    fn publish(&self, index: usize, band: LoadBand, intrinsic: &Intrinsic, wakes: &WakeSet) {
        let plans = BandPlans {
            local: plan_of(intrinsic, wakes, ParkKind::Local, &self.overrides),
            cross_process: plan_of(intrinsic, wakes, ParkKind::CrossProcess, &self.overrides),
        };
        self.classes[index].plans[usize::from(band_slot(band))].get_or_init(|| plans);
    }

    /// The index of the calling thread's class.
    fn class_here(&self) -> Option<usize> {
        if self.classes.len() == 1 {
            return Some(0);
        }
        let (group, number) = current_processor()?;
        if group != 0 {
            return None;
        }
        self.lookup.get(number as usize).copied().flatten()
    }
}

/// [`measure_intrinsic`] on the calling thread, with its cost.
fn measure_intrinsic_costed() -> (Result<Intrinsic, String>, Cost) {
    let started = Instant::now();
    let measuring = thread_cpu();
    let outcome = measure_intrinsic();
    let cost = Cost {
        wall: started.elapsed(),
        measuring: elapsed_cpu(measuring),
        waiters: Some(Duration::ZERO),
        storers: Some(Duration::ZERO),
    };
    (outcome, cost)
}

static CALIBRATOR: OnceLock<Calibrator> = OnceLock::new();
static FAILURE: OnceLock<String> = OnceLock::new();
static LAUNCH: Once = Once::new();

/// Starts this process's calibration thread, at most once. Does nothing
/// where `SUBETHA_WAIT_CALIBRATION=off`.
pub fn start() {
    if !crate::wait_active::calibration_enabled() {
        return;
    }
    LAUNCH.call_once(|| {
        let launched = std::thread::Builder::new()
            .name("subetha-wait-calibration".to_string())
            .spawn(background);
        if let Err(e) = launched {
            FAILURE.get_or_init(|| format!("the calibration thread did not start: {e}"));
        }
    });
}

/// The calibration thread's body.
fn background() {
    match Calibrator::new(cache_dir(), crate::wait_topology::topology()) {
        Ok(calibrator) => CALIBRATOR.get_or_init(|| calibrator).run(),
        Err(reason) => {
            FAILURE.get_or_init(|| reason);
        }
    }
}

/// The calibrated plan for a wait of `park` kind on the calling thread's
/// core class in the band of the last load reading. `None` until
/// calibration has published that plan, and where the band or the class
/// cannot be told.
pub fn plan_for(park: ParkKind) -> Option<Plan> {
    let calibrator = CALIBRATOR.get()?;
    let class = calibrator.classes.get(calibrator.class_here()?)?;
    let plans = class.plans.get(usize::from(calibrator.band.load(Ordering::Relaxed)))?.get()?;
    match park {
        ParkKind::Local => plans.local,
        ParkKind::CrossProcess => plans.cross_process,
    }
}

/// Where calibration stands in this process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// `SUBETHA_WAIT_CALIBRATION=off`.
    Off,
    /// No wait has started it, or its thread is starting.
    NotStarted,
    /// It is not running, for the reason given.
    Failed(String),
    /// Its thread is running.
    Running,
}

/// One band of a class: its wake set and the plans in force.
#[derive(Debug, Clone, PartialEq)]
pub struct BandReport {
    pub band: LoadBand,
    pub wakes: Record<WakeSet>,
    pub local: Option<Plan>,
    pub cross_process: Option<Plan>,
}

/// One class of core.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassReport {
    pub label: String,
    pub placement: Placement,
    pub intrinsic: Record<Intrinsic>,
    /// One per band, quietest first.
    pub bands: Vec<BandReport>,
}

/// What calibration has done in this process.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub state: State,
    /// The cache directory, where there is one.
    pub cache: Option<PathBuf>,
    /// The last load reading: busy logical processors and their band.
    pub load: Option<(f64, LoadBand)>,
    pub classes: Vec<ClassReport>,
    /// Cache files that were not read, and a missing cache directory, with
    /// the reason.
    pub notes: Vec<String>,
}

/// What calibration has done in this process.
pub fn report() -> Report {
    let Some(calibrator) = CALIBRATOR.get() else {
        let state = if !crate::wait_active::calibration_enabled() {
            State::Off
        } else if let Some(reason) = FAILURE.get() {
            State::Failed(reason.clone())
        } else {
            State::NotStarted
        };
        return Report { state, cache: None, load: None, classes: Vec::new(), notes: Vec::new() };
    };
    let band = LoadBand::ALL.get(usize::from(calibrator.band.load(Ordering::Relaxed))).copied();
    let classes = calibrator
        .classes
        .iter()
        .map(|state| ClassReport {
            label: state.class.label.clone(),
            placement: state.class.placement.clone(),
            intrinsic: Record::clone(&state.intrinsic.load_full()),
            bands: LoadBand::ALL
                .iter()
                .map(|&band| {
                    let slot = usize::from(band_slot(band));
                    let plans = state.plans[slot].get();
                    BandReport {
                        band,
                        wakes: Record::clone(&state.bands[slot].load_full()),
                        local: plans.and_then(|plans| plans.local),
                        cross_process: plans.and_then(|plans| plans.cross_process),
                    }
                })
                .collect(),
        })
        .collect();
    Report {
        state: State::Running,
        cache: calibrator.dir.clone(),
        load: calibrator.busy.load_full().map(|busy| *busy).zip(band),
        classes,
        notes: Vec::clone(&calibrator.notes.load_full()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// How long a test's round trip may wait before its wake counts as lost,
    /// the bound the waker's cross-process tests put on theirs. A test holds
    /// its windows to this rather than to [`TRIP_BOUND`], at which a
    /// calibration abandons a window, so a host whose load deschedules a
    /// window's thread past that bound delays the test instead of failing it.
    const LOST: Duration = Duration::from_secs(30);

    fn window(statistic: f64, busy: f64) -> Window {
        Window { statistic, busy: Some(busy) }
    }

    fn fact(value: f64) -> Fact {
        Fact { value, busy: Some(0.5) }
    }

    /// The Windows host's figures from the brief (Ryzen 9 7900X): 4,699.9
    /// cycles per microsecond, a 47-cycle tick, 57.9 cycles a pause, and an
    /// `MWAITX` guard floor near 1,500 at about half a cycle per unit.
    fn intrinsic() -> Intrinsic {
        let timer = |hint| Timer {
            family: MonitorFamily::Mwaitx(hint),
            floor: fact(987.0),
            guard_floor: fact(1_504.0),
            cycles_per_unit: fact(0.501),
        };
        Intrinsic {
            rate_per_us: fact(4_699.9),
            tick: fact(47.0),
            pause: fact(57.9),
            timers: vec![timer(MwaitxHint::C0), timer(MwaitxHint::C1)],
            omitted: Vec::new(),
        }
    }

    /// The same host's quiet unpinned wakes: spin 260, monitor 2,069 at C0
    /// and 2,068 at C1, and the local park 19,952.
    fn wakes() -> WakeSet {
        WakeSet {
            spin: fact(260.0),
            monitor: vec![
                (MonitorFamily::Mwaitx(MwaitxHint::C0), fact(2_069.0)),
                (MonitorFamily::Mwaitx(MwaitxHint::C1), fact(2_068.0)),
            ],
            park: vec![(ParkKind::Local, fact(19_952.0)), (ParkKind::CrossProcess, fact(21_000.0))],
            omitted: Vec::new(),
        }
    }

    fn key() -> Key {
        Key { cpu: "AuthenticAMD 25 97 2".to_string(), processors: 24, hypervisor: "Microsoft Hv".to_string() }
    }

    /// A directory of its own under the temporary directory.
    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("subetha-wait-calibration-test-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("creating the test directory");
        dir
    }

    fn processor(number: u32, core: u32, class: Option<CoreClass>) -> LogicalProcessor {
        LogicalProcessor { group: 0, number, core, class }
    }

    #[test]
    fn a_fact_is_the_smallest_window_with_its_busy_reading() {
        let windows =
            [window(12.0, 3.0), window(10.0, 5.0), window(11.0, 4.0), window(13.0, 2.0), window(10.5, 1.0)];
        let fact = take(&windows).expect("the windows agree");
        assert_eq!(fact, Fact { value: 10.0, busy: Some(5.0) });
    }

    #[test]
    fn windows_that_spread_past_their_median_give_no_fact() {
        let spread = |largest| {
            take(&[window(10.0, 0.0), window(11.0, 0.0), window(12.0, 0.0), window(13.0, 0.0), window(largest, 0.0)])
        };
        // Sorted 10, 11, 12, 13, 23: a spread of 13 over a median of 12.
        assert!(spread(23.0).is_err());
        // A spread of 12, equal to the median, is kept.
        assert!(spread(22.0).is_ok());
    }

    /// The Windows host's sweep shape: about 1,000 cycles up to 512 units,
    /// 1,363 at 1,024, then half a cycle per unit plus 2,150.
    fn zen4_medians() -> Vec<u64> {
        (0..=SWEEP_TOP)
            .map(|power| match power {
                0..=9 => 1_000 + u64::from(power) * 3,
                10 => 1_363,
                _ => (1u64 << power) / 2 + 2_150,
            })
            .collect()
    }

    #[test]
    fn the_sweep_gives_the_floor_the_guard_floor_and_the_unit() {
        let [floor, guard_floor, cycles_per_unit] = sweep_statistics(&zen4_medians()).expect("the timer holds");
        assert_eq!(floor, 1_000.0);
        // At one cycle a unit, 512 units run out within the floor; 1,024
        // do not.
        assert_eq!(guard_floor, 1_027.0);
        assert_eq!(cycles_per_unit, 1.0);
    }

    /// The Windows host's sweep with its two largest requests ended by
    /// interrupts at 287,000 cycles, as a busy moment leaves them. The unit
    /// taken from the largest request read 0.137 cycles there, and a plan
    /// that believed it armed 3.65 times the units its budget allowed; the
    /// unit stays one cycle whatever the top of the sweep shows.
    #[test]
    fn interrupts_ending_the_largest_requests_leave_the_unit_at_one_cycle() {
        let mut medians = zen4_medians();
        for cut in &mut medians[SWEEP_TOP as usize - 1..] {
            *cut = 287_000;
        }
        let [floor, guard_floor, cycles_per_unit] = sweep_statistics(&medians).expect("the timer holds");
        assert_eq!(cycles_per_unit, 1.0);
        assert_eq!((floor, guard_floor), (1_000.0, 1_027.0));
    }

    #[test]
    fn a_timer_that_returns_at_once_does_not_hold() {
        let flat = [1_000u64; SWEEP_TOP as usize + 1];
        assert!(sweep_statistics(&flat).is_err());
    }

    #[test]
    fn a_deadline_timer_counts_cycles() {
        // `UMWAIT` returning after at least 300 cycles and at most an
        // operating system limit of 100,000.
        let medians: Vec<u64> = (0..=SWEEP_TOP).map(|power| (1u64 << power).clamp(300, 100_000)).collect();
        let statistics = sweep_statistics(&medians).expect("the timer holds");
        assert_eq!(statistics, [300.0, 300.0, 1.0]);
    }

    #[test]
    fn a_record_reads_back_as_written() {
        let text = record_text(&key(), "all", "intrinsic", &intrinsic_lines(&intrinsic()));
        let body = record_body(&text, &key(), "all", "intrinsic").expect("the header matches");
        assert_eq!(parse_intrinsic(&body).expect("the record parses"), intrinsic());

        let band = band_record(LoadBand::UnderQuarter);
        let text = record_text(&key(), "all", &band, &wake_lines(&wakes()));
        let body = record_body(&text, &key(), "all", &band).expect("the header matches");
        assert_eq!(parse_wakes(&body).expect("the record parses"), wakes());
    }

    #[test]
    fn a_record_for_another_host_class_or_schema_reads_as_absent() {
        let text = record_text(&key(), "all", "intrinsic", &intrinsic_lines(&intrinsic()));
        let fewer = Key { processors: 16, ..key() };
        assert!(record_body(&text, &fewer, "all", "intrinsic").is_err());
        assert!(record_body(&text, &key(), "performance", "intrinsic").is_err());
        let newer = text.replacen(&format!("subetha-wait {SCHEMA}"), &format!("subetha-wait {}", SCHEMA + 1), 1);
        assert!(record_body(&newer, &key(), "all", "intrinsic").is_err());
    }

    #[test]
    fn a_repeated_or_unknown_line_makes_a_record_unreadable() {
        let mut lines = intrinsic_lines(&intrinsic());
        lines.push("pause 60 0.5".to_string());
        let text = record_text(&key(), "all", "intrinsic", &lines);
        let body = record_body(&text, &key(), "all", "intrinsic").expect("the header matches");
        assert!(parse_intrinsic(&body).is_err());

        let mut lines = wake_lines(&wakes());
        lines.push("spun 260 0.5".to_string());
        let text = record_text(&key(), "all", "band under-25", &lines);
        let body = record_body(&text, &key(), "all", "band under-25").expect("the header matches");
        assert!(parse_wakes(&body).is_err());
    }

    #[test]
    fn a_value_outside_its_bound_is_refused() {
        intrinsic_in_bounds(&intrinsic(), 24).expect("the brief's figures lie within their bounds");
        wakes_in_bounds(&wakes(), 4_699.9, 24).expect("the brief's wakes lie within their bounds");

        let slow_pause = Intrinsic { pause: fact(100_001.0), ..intrinsic() };
        assert!(intrinsic_in_bounds(&slow_pause, 24).is_err());
        let slow_counter = Intrinsic { rate_per_us: fact(0.5), ..intrinsic() };
        assert!(intrinsic_in_bounds(&slow_counter, 24).is_err());
        let fast_timer = Intrinsic {
            timers: vec![Timer { cycles_per_unit: fact(1.0 / 65.0), ..intrinsic().timers[0] }],
            ..intrinsic()
        };
        assert!(intrinsic_in_bounds(&fast_timer, 24).is_err());
        let busy = Intrinsic { tick: Fact { value: 47.0, busy: Some(25.0) }, ..intrinsic() };
        assert!(intrinsic_in_bounds(&busy, 24).is_err());

        // One cycle past 1 ms at the rate.
        let slow_park = WakeSet { park: vec![(ParkKind::Local, fact(4_699_901.0))], ..wakes() };
        assert!(wakes_in_bounds(&slow_park, 4_699.9, 24).is_err());
    }

    #[test]
    fn writers_in_parallel_never_leave_a_mixed_record() {
        let dir = scratch("parallel");
        let name = intrinsic_file("all");
        let texts: Vec<String> = (0..4u8)
            .map(|n| {
                let pause = Intrinsic { pause: fact(50.0 + f64::from(n)), ..intrinsic() };
                record_text(&key(), "all", "intrinsic", &intrinsic_lines(&pause))
            })
            .collect();
        // Each thread keeps its own failures and hands them back on join.
        let failures: Vec<String> = std::thread::scope(|scope| {
            let mut threads = Vec::new();
            for text in &texts {
                let (dir, name) = (&dir, &name);
                threads.push(scope.spawn(move || {
                    let mut failed = Vec::new();
                    for _ in 0..200 {
                        if let Err(e) = write_record(dir, name, text) {
                            failed.push(e);
                        }
                    }
                    failed
                }));
            }
            threads.push(scope.spawn(|| {
                let mut failed = Vec::new();
                for _ in 0..800 {
                    match std::fs::read_to_string(dir.join(&name)) {
                        Ok(read) => assert!(texts.contains(&read), "a mixed record: {read:?}"),
                        Err(e) => failed.push(format!("reading: {e}")),
                    }
                }
                failed
            }));
            threads
                .into_iter()
                .flat_map(|thread| thread.join().expect("a writer or the reader ended by panic"))
                .collect()
        });
        let last = std::fs::read_to_string(dir.join(&name))
            .unwrap_or_else(|e| panic!("no record is in place: {e}; failures {failures:?}"));
        assert!(texts.contains(&last), "a mixed record: {last:?}");
        std::fs::remove_file(dir.join(name)).expect("removing the record");
        std::fs::remove_dir(dir).expect("removing the test directory");
    }

    #[test]
    fn a_host_of_one_class_is_one_unpinned_class() {
        let all = vec![Class { label: "all".to_string(), placement: Placement::Unpinned }];
        assert_eq!(classes_of(None, true), (all.clone(), Vec::new()));
        let uniform = Topology {
            processors: (0..4).map(|n| processor(n, n / 2, Some(CoreClass::Efficiency(0)))).collect(),
        };
        assert_eq!(classes_of(Some(&uniform), true), (all, Vec::new()));
    }

    #[test]
    fn a_hybrid_host_pins_each_class_to_two_of_its_cores() {
        // Two faster cores of two threads each, then two slower cores.
        let fast = Some(CoreClass::Efficiency(1));
        let slow = Some(CoreClass::Efficiency(0));
        let topology = Topology {
            processors: vec![
                processor(0, 0, fast),
                processor(1, 0, fast),
                processor(2, 1, fast),
                processor(3, 1, fast),
                processor(4, 2, slow),
                processor(5, 3, slow),
            ],
        };
        let (classes, lookup) = classes_of(Some(&topology), true);
        assert_eq!(
            classes,
            vec![
                Class {
                    label: "efficiency-class-1".to_string(),
                    placement: Placement::Pinned(Pins { waiter: 0, storer: 2 }),
                },
                Class {
                    label: "efficiency-class-0".to_string(),
                    placement: Placement::Pinned(Pins { waiter: 4, storer: 5 }),
                },
            ]
        );
        assert_eq!(lookup, vec![Some(0), Some(0), Some(0), Some(0), Some(1), Some(1)]);
        assert!(
            classes_of(Some(&topology), false)
                .0
                .iter()
                .all(|class| matches!(class.placement, Placement::Unavailable(_)))
        );
    }

    #[test]
    fn a_class_of_one_core_is_not_measured() {
        let performance = Some(CoreClass::Cpuid(CoreKind::Performance));
        let efficiency = Some(CoreClass::Cpuid(CoreKind::Efficiency));
        let topology = Topology {
            processors: vec![
                processor(0, 0, performance),
                processor(1, 1, performance),
                processor(2, 2, efficiency),
                processor(3, 2, efficiency),
            ],
        };
        let (classes, _) = classes_of(Some(&topology), true);
        assert_eq!(classes[0].label, "performance");
        assert_eq!(classes[0].placement, Placement::Pinned(Pins { waiter: 0, storer: 1 }));
        assert_eq!(classes[1].label, "efficiency");
        assert!(matches!(classes[1].placement, Placement::Unavailable(_)));
    }

    #[test]
    fn a_band_plan_is_the_ski_rental_plan_of_its_facts() {
        let none = Overrides::default();
        let expected = derive(
            &Facts {
                pause_cycles: 57.9,
                spin_wake: 260,
                park_wake: 19_952,
                monitor: Some(MonitorFacts {
                    family: MonitorFamily::Mwaitx(MwaitxHint::C1),
                    wake: 2_068,
                    floor_cycles: 1_504,
                    units_per_cycle: 1.0 / 0.501,
                }),
                invariant_tsc: invariant_counter(),
            },
            &none,
        );
        assert_eq!(plan_of(&intrinsic(), &wakes(), ParkKind::Local, &none), Some(expected));

        let local_only = WakeSet { park: vec![(ParkKind::Local, fact(19_952.0))], ..wakes() };
        assert_eq!(plan_of(&intrinsic(), &local_only, ParkKind::CrossProcess, &none), None);

        // A hold one cycle past 1 ms at the rate.
        let slow_park = WakeSet { park: vec![(ParkKind::Local, fact(260.0 + 4_699_901.0))], ..wakes() };
        assert_eq!(plan_of(&intrinsic(), &slow_park, ParkKind::Local, &none), None);

        // Without the timer of either hint, the plan has no monitor phase.
        let untimed = Intrinsic { timers: Vec::new(), ..intrinsic() };
        let plan = plan_of(&untimed, &wakes(), ParkKind::Local, &none).expect("a plan");
        assert_eq!(plan.monitor, None);
    }

    #[test]
    fn a_cached_class_publishes_the_plans_of_its_cached_bands() {
        let dir = scratch("cached-class");
        let calibrator = Calibrator::new(Ok(dir.clone()), None).expect("a calibrator");
        let key = calibrator.key.clone();
        let intrinsic_name = intrinsic_file("all");
        let band_name = band_file("all", LoadBand::UnderHalf);
        write_record(&dir, &intrinsic_name, &record_text(&key, "all", "intrinsic", &intrinsic_lines(&intrinsic())))
            .expect("writing the intrinsic record");
        write_record(
            &dir,
            &band_name,
            &record_text(&key, "all", &band_record(LoadBand::UnderHalf), &wake_lines(&wakes())),
        )
        .expect("writing the band record");

        calibrator.take_intrinsic(0);
        let class = &calibrator.classes[0];
        assert!(matches!(&*class.intrinsic.load_full(), Record::Ready { source: Source::Cache, .. }));
        assert!(matches!(&*class.bands[1].load_full(), Record::Ready { source: Source::Cache, .. }));
        assert!(matches!(&*class.bands[0].load_full(), Record::Absent));
        assert_eq!(
            class.plans[1].get().and_then(|plans| plans.local),
            plan_of(&intrinsic(), &wakes(), ParkKind::Local, &calibrator.overrides)
        );
        assert_eq!(class.plans[0].get(), None);
        assert_eq!(*calibrator.notes.load_full(), Vec::<String>::new());

        std::fs::remove_file(dir.join(intrinsic_name)).expect("removing the intrinsic record");
        std::fs::remove_file(dir.join(band_name)).expect("removing the band record");
        std::fs::remove_dir(dir).expect("removing the test directory");
    }

    #[test]
    fn this_hosts_intrinsic_facts_lie_within_their_bounds() {
        let intrinsic = measure_intrinsic().expect("the intrinsic facts");
        intrinsic_in_bounds(&intrinsic, u32::MAX).expect("this host's facts lie within their bounds");
        let timed = monitor_states().into_iter().filter(|family| *family != MonitorFamily::ArmWfe).count();
        assert_eq!(intrinsic.timers.len() + intrinsic.omitted.len(), timed, "{:?}", intrinsic.omitted);
    }

    /// Counter cycles in `span` on this host.
    fn host_cycles(span: Duration) -> u64 {
        let rate = rate_window().expect("the counter's rate");
        (span.as_secs_f64() * 1e6 * rate) as u64
    }

    #[test]
    fn a_spin_and_each_park_wake_their_waiter() {
        let mut spent = Spent::zero();
        let spin = wake_window(Arm::Spin, host_cycles(SHORT_GAP), None, None, LOST, &mut spent)
            .expect("the spin window");
        assert!(spin > 0.0, "spin {spin}");

        let local = CrossProcessWaker::create_anon(1).expect("an anonymous waker");
        let park = wake_window(Arm::Park, host_cycles(PARK_GAP), None, Some(&local), LOST, &mut spent)
            .expect("the local park window");
        assert!(park > 0.0, "local park {park}");

        let dir = scratch("park");
        let path = dir.join("calibration.waker");
        {
            let file = CrossProcessWaker::reset(&path, 1).expect("a file-backed waker");
            let park = wake_window(Arm::Park, host_cycles(PARK_GAP), None, Some(&file), LOST, &mut spent)
                .expect("the cross-process park window");
            assert!(park > 0.0, "cross-process park {park}");
        }
        std::fs::remove_file(path).expect("removing the waker file");
        std::fs::remove_dir(dir).expect("removing the test directory");
    }

    /// A waiter that a loaded host keeps off the processor for longer than
    /// a calibration's [`TRIP_BOUND`] still completes a test's window, which
    /// is held to [`LOST`] instead. The hold stands in for that load: it
    /// keeps the waiter from reporting ready for twice the bound.
    #[test]
    fn a_waiter_held_past_the_calibration_bound_completes_a_test_window() {
        let mut spent = Spent::zero();
        crate::test_races::hold_next_waiter(TRIP_BOUND * 2);
        let spin = wake_window(Arm::Spin, host_cycles(SHORT_GAP), None, None, LOST, &mut spent)
            .expect("a window whose waiter was held past the calibration bound");
        assert!(spin > 0.0, "spin {spin}");
    }

    #[test]
    fn each_monitor_state_this_host_has_wakes_its_waiter() {
        let mut spent = Spent::zero();
        for family in monitor_states() {
            let phase = MonitorPhase {
                family,
                budget_cycles: host_cycles(LOST),
                units_per_cycle: 1.0,
                guard_floor_cycles: None,
            };
            let wake = wake_window(Arm::Monitor(phase), host_cycles(SHORT_GAP), None, None, LOST, &mut spent)
                .unwrap_or_else(|e| panic!("the {} window: {e}", family_name(family)));
            assert!(wake > 0.0, "{} {wake}", family_name(family));
        }
    }

    #[test]
    fn a_storer_that_stops_releases_a_spinning_waiter() {
        // The storer's pin to processor 1023 fails on every host this runs
        // on, so it stops before its first store while the waiter spins.
        let pins = Pins { waiter: 0, storer: 1_023 };
        let mut spent = Spent::zero();
        assert!(wake_window(Arm::Spin, 1, Some(pins), None, LOST, &mut spent).is_err());
    }
}
