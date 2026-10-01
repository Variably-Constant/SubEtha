//! The D5 verdict on this host's blocking waits: whether the calibrated
//! wait costs at most twice the better of a pure spin and a pure park at
//! every gap, and less than the spin and than the fixed ladder summed over
//! the gaps. The ladder is held to that sum only where it has its monitor
//! tier; without a monitor it is a short spin and a park.
//!
//! - A producer paces items through a `BlockingSpscRing` of 8 slots. For
//!   each item it waits until the consumer has taken the one before, lets
//!   the gap pass on the counter, stamps the counter into the item and
//!   pushes it. The consumer charges each item its latency, from that stamp
//!   to its own on return, plus its share of the processor time the
//!   consumer thread spent across the gap's items, in counter cycles.
//! - The arms, each in a process of its own: `spin` tries and spins, never
//!   parking; `park` parks at once, with no spin and no monitor; `shipped`
//!   has calibration off and waits 0.5.1's fixed ladder; `calibrated` waits
//!   its band's calibrated plan.
//! - Two workloads: an anonymous ring with both threads in the arm's
//!   process, and a file-backed ring whose producer is a second process.
//! - Gaps of 1, 4, 16, 64, 256 and 1024 us, 2,000 items each, over 3
//!   rounds, the arms' order rotated each round.
//! - Each arm's process runs a spin control at 16 us, 2,000 items, before
//!   and after its arm, and reads the machine's load at each boundary. A
//!   gap's comparison in a round is void when that round's controls moved
//!   by more than the calibrated arm differs from the better fixed arm
//!   there.
//! - A gap is read round by round, each round's calibrated cost over that
//!   round's better fixed arm: within 2x when every round is, beyond it
//!   when every round is, and undecided when the rounds fall on both
//!   sides, since its rounds then spread wider than the difference they
//!   would decide. The summed clauses are read the same way.
//! - A co-runner pass repeats every arm with the consumer pinned to the
//!   first logical processor that has an SMT sibling and a compute thread,
//!   a dependent 64-bit multiply-add chain, on that sibling; its iterations
//!   per second print beside each gap.
//! - A warm-up process fills the calibration cache first, and a calibrated
//!   arm waits up to 30 s for its band's plan.
//! - An item whose receive times out after 10 s is counted apart and left
//!   out of the costs.
//!
//! Usage: `wait_verdict` runs every arm and prints the verdict;
//! `wait_verdict --read <file>` prints it again from a run's saved output.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use subetha_cxc::blocking_spsc_ring::{BlockingError, BlockingSpscRing};
use subetha_cxc::host_load::CpuTimes;
use subetha_cxc::ordering::read_tsc;
use subetha_cxc::shared_ring::RingError;
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;
use subetha_cxc::wait_calibration::{self, Record};
use subetha_cxc::wait_plan::ParkKind;

/// The gaps, in microseconds, in the order each arm runs them.
const GAPS_US: [u64; 6] = [1, 4, 16, 64, 256, 1024];
/// Items per gap.
const ITEMS: usize = 2_000;
/// Rounds of every arm.
const ROUNDS: usize = 3;
/// The ring's slots.
const CAPACITY: usize = 8;
/// How long one receive waits before the item counts as timed out.
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// The control's gap, in microseconds.
const CONTROL_GAP_US: u64 = 16;
/// How long a calibrated process waits for its band's plan.
const READY_BOUND: Duration = Duration::from_secs(30);
/// How long the counter's rate is timed against the monotonic clock.
const RATE_WINDOW: Duration = Duration::from_millis(10);
/// How often a process waiting for calibration looks again.
const READY_POLL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Arm {
    Spin,
    Park,
    Shipped,
    Calibrated,
}

impl Arm {
    const ALL: [Arm; 4] = [Arm::Spin, Arm::Park, Arm::Shipped, Arm::Calibrated];

    fn name(self) -> &'static str {
        match self {
            Arm::Spin => "spin",
            Arm::Park => "park",
            Arm::Shipped => "shipped",
            Arm::Calibrated => "calibrated",
        }
    }

    fn from_name(name: &str) -> Result<Arm, String> {
        Arm::ALL
            .into_iter()
            .find(|arm| arm.name() == name)
            .ok_or_else(|| format!("no arm is named {name:?}"))
    }

    /// Sets the environment the arm's process waits under.
    fn configure(self, command: &mut Command) {
        for name in [
            "SUBETHA_WAIT_CALIBRATION",
            "SUBETHA_NO_MONITOR_WAIT",
            "SUBETHA_PRE_PARK_SPIN",
            "SUBETHA_MONITOR_WAIT_CYCLES",
        ] {
            command.env_remove(name);
        }
        match self {
            Arm::Spin | Arm::Shipped => {
                command.env("SUBETHA_WAIT_CALIBRATION", "off");
            }
            Arm::Park => {
                command
                    .env("SUBETHA_WAIT_CALIBRATION", "off")
                    .env("SUBETHA_NO_MONITOR_WAIT", "1")
                    .env("SUBETHA_PRE_PARK_SPIN", "0");
            }
            Arm::Calibrated => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Workload {
    Anon,
    File,
}

impl Workload {
    const ALL: [Workload; 2] = [Workload::Anon, Workload::File];

    fn name(self) -> &'static str {
        match self {
            Workload::Anon => "anon",
            Workload::File => "file",
        }
    }

    fn from_name(name: &str) -> Result<Workload, String> {
        Workload::ALL
            .into_iter()
            .find(|workload| workload.name() == name)
            .ok_or_else(|| format!("no workload is named {name:?}"))
    }

    fn park_kind(self) -> ParkKind {
        match self {
            Workload::Anon => ParkKind::Local,
            Workload::File => ParkKind::CrossProcess,
        }
    }
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

/// Counter cycles per microsecond of the monotonic clock.
fn rate_per_us() -> f64 {
    let started = Instant::now();
    let first = stamp();
    while started.elapsed() < RATE_WINDOW {
        std::hint::spin_loop();
    }
    let last = stamp();
    last.wrapping_sub(first) as f64 / (started.elapsed().as_secs_f64() * 1e6)
}

fn cycles(span_us: f64, rate_per_us: f64) -> u64 {
    (span_us * rate_per_us) as u64
}

/// The calling thread's processor time in counter cycles, where the
/// platform reports it.
#[cfg(windows)]
fn thread_cycles(_rate_per_us: f64) -> Option<f64> {
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::Threading::GetCurrentThread;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn QueryThreadCycleTime(thread: HANDLE, cycles: *mut u64) -> i32;
    }
    let mut spent = 0u64;
    // SAFETY: `spent` is valid for the call to write, and the pseudo handle
    // names the calling thread.
    let ok = unsafe { QueryThreadCycleTime(GetCurrentThread(), &mut spent) };
    (ok != 0).then_some(spent as f64)
}

/// The calling thread's processor time in counter cycles at
/// `rate_per_us`, where the platform reports it.
#[cfg(unix)]
fn thread_cycles(rate_per_us: f64) -> Option<f64> {
    let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `now` is a valid timespec for the call to write.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) };
    (rc == 0).then(|| (now.tv_sec as f64 * 1e6 + now.tv_nsec as f64 / 1e3) * rate_per_us)
}

/// The calling thread's processor time in counter cycles, where the
/// platform reports it.
#[cfg(not(any(unix, windows)))]
fn thread_cycles(_rate_per_us: f64) -> Option<f64> {
    None
}

/// Pins the calling thread to `processor`, or says it could not.
fn pin_here(processor: u32, role: &str) -> Result<(), String> {
    if subetha_cxc::cpu_affinity::pin_current_thread_to_core(processor as usize) {
        Ok(())
    } else {
        Err(format!("the {role} could not be pinned to logical processor {processor}"))
    }
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

/// The first logical processor in group 0 that has an SMT sibling, and
/// that sibling.
fn sibling_pair() -> Result<(u32, u32), String> {
    let topology =
        subetha_cxc::wait_topology::topology().ok_or("this platform reports no topology")?;
    for processor in topology.processors.iter().filter(|processor| processor.group == 0) {
        let sibling = topology
            .siblings(processor)
            .into_iter()
            .find(|sibling| sibling.number != processor.number);
        if let Some(sibling) = sibling {
            return Ok((processor.number, sibling.number));
        }
    }
    Err("no logical processor in group 0 has an SMT sibling".to_string())
}

/// A count alone on its cache line.
#[repr(align(128))]
#[derive(Default)]
struct Counter(AtomicU64);

/// A compute thread pinned to a logical processor of its own, counting the
/// iterations of a dependent 64-bit multiply-add chain.
struct CoRunner {
    count: Arc<Counter>,
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<Result<(), String>>,
}

impl CoRunner {
    /// Starts the chain on `processor` and answers once the thread is
    /// pinned there.
    fn start(processor: u32) -> Result<CoRunner, String> {
        let count = Arc::new(Counter::default());
        let stop = Arc::new(AtomicBool::new(false));
        let (pinned, started) = std::sync::mpsc::channel();
        let thread = {
            let (count, stop) = (Arc::clone(&count), Arc::clone(&stop));
            std::thread::Builder::new()
                .name("wait-verdict-corunner".to_string())
                .spawn(move || {
                    let pin = pin_here(processor, "co-runner");
                    let failed = pin.is_err();
                    pinned.send(pin).map_err(|e| format!("reporting the pin: {e}"))?;
                    if failed {
                        return Ok(());
                    }
                    let mut value: u64 = 1;
                    let mut iterations: u64 = 0;
                    while !stop.load(Ordering::Relaxed) {
                        value = value.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(iterations);
                        iterations += 1;
                        count.0.store(iterations, Ordering::Relaxed);
                    }
                    std::hint::black_box(value);
                    Ok(())
                })
                .map_err(|e| format!("the co-runner did not start: {e}"))?
        };
        match started.recv() {
            Ok(Ok(())) => Ok(CoRunner { count, stop, thread }),
            Ok(Err(reason)) => {
                joined(thread.join(), "co-runner")?;
                Err(reason)
            }
            Err(e) => Err(format!("the co-runner stopped before pinning: {e}")),
        }
    }

    fn iterations(&self) -> u64 {
        self.count.0.load(Ordering::Relaxed)
    }

    fn stop(self) -> Result<(), String> {
        self.stop.store(true, Ordering::Relaxed);
        joined(self.thread.join(), "co-runner")
    }
}

/// Paces `gaps` through `ring`: for each of `items` items per gap, waits
/// until the consumer has taken every item pushed so far, lets the gap
/// pass, stamps the counter into the item and pushes it. Ends early with an
/// `Err` once `stop` is set.
fn produce(ring: &BlockingSpscRing, gaps: &[u64], items: usize, stop: &AtomicBool) -> Result<(), String> {
    let mut pushed = ring.inner().head();
    for &gap in gaps {
        for _ in 0..items {
            while ring.inner().tail() < pushed {
                if stop.load(Ordering::Relaxed) {
                    return Err("the consumer stopped".to_string());
                }
                std::hint::spin_loop();
            }
            let due = stamp().wrapping_add(gap);
            while !reached(read_tsc(), due) {
                std::hint::spin_loop();
            }
            let sent = stamp();
            ring.try_push(&sent.to_le_bytes())
                .map_err(|e| format!("pushing item {pushed}: {e:?}"))?;
            pushed += 1;
        }
    }
    Ok(())
}

/// Why a receive gave no item.
enum Missed {
    TimedOut,
    Failed(String),
}

/// Takes one item from `ring` into `buffer` the way `arm` waits.
fn receive(ring: &BlockingSpscRing, arm: Arm, buffer: &mut [u8], rate: f64) -> Result<(), Missed> {
    match arm {
        Arm::Spin => {
            let deadline = read_tsc().wrapping_add(cycles(RECEIVE_TIMEOUT.as_secs_f64() * 1e6, rate));
            loop {
                match ring.try_pop(buffer) {
                    Ok(_length) => return Ok(()),
                    Err(RingError::Empty) => {}
                    Err(e) => return Err(Missed::Failed(format!("{e:?}"))),
                }
                if reached(read_tsc(), deadline) {
                    return Err(Missed::TimedOut);
                }
                std::hint::spin_loop();
            }
        }
        Arm::Park | Arm::Shipped | Arm::Calibrated => match ring.recv_blocking(buffer, Some(RECEIVE_TIMEOUT)) {
            Ok(_length) => Ok(()),
            Err(BlockingError::Timeout) => Err(Missed::TimedOut),
            Err(e) => Err(Missed::Failed(format!("{e:?}"))),
        },
    }
}

/// What the consumer measured over one gap, in counter cycles.
struct Gap {
    gap_us: u64,
    latencies: Vec<u64>,
    timeouts: u64,
    processor: Option<f64>,
    corunner_per_s: Option<f64>,
}

/// Takes `items` items per gap from `ring`, waiting as `arm` does, and
/// measures each gap. `corunner` is read at each gap's ends.
fn consume(
    ring: &BlockingSpscRing,
    arm: Arm,
    rate: f64,
    gaps_us: &[u64],
    items: usize,
    corunner: Option<&CoRunner>,
) -> Result<Vec<Gap>, String> {
    let mut buffer = [0u8; SPSC_PAYLOAD_BYTES];
    let mut gaps = Vec::with_capacity(gaps_us.len());
    for &gap_us in gaps_us {
        let processor_start = thread_cycles(rate);
        let counter_start = stamp();
        let corunner_start = corunner.map(CoRunner::iterations);
        let mut latencies = Vec::with_capacity(items);
        let mut timeouts = 0u64;
        for item in 0..items {
            let mut timed_out = false;
            loop {
                match receive(ring, arm, &mut buffer, rate) {
                    Ok(()) => break,
                    Err(Missed::TimedOut) => timed_out = true,
                    Err(Missed::Failed(reason)) => {
                        return Err(format!("item {item} at {gap_us} us: {reason}"));
                    }
                }
            }
            let now = stamp();
            let mut sent = [0u8; 8];
            sent.copy_from_slice(&buffer[..8]);
            if timed_out {
                timeouts += 1;
            } else {
                latencies.push(now.wrapping_sub(u64::from_le_bytes(sent)));
            }
        }
        let counter_end = stamp();
        let processor = thread_cycles(rate).zip(processor_start).map(|(end, start)| end - start);
        let corunner_per_s = corunner.map(CoRunner::iterations).zip(corunner_start).map(|(end, start)| {
            (end - start) as f64 / (counter_end.wrapping_sub(counter_start) as f64 / rate / 1e6)
        });
        gaps.push(Gap { gap_us, latencies, timeouts, processor, corunner_per_s });
    }
    Ok(gaps)
}

/// Runs the producer and the consumer on threads of this process over one
/// anonymous ring. The consumer is pinned to `pin` where one is given.
fn run_anon(
    arm: Arm,
    rate: f64,
    gaps_us: &[u64],
    corunner: Option<&CoRunner>,
    pin: Option<u32>,
) -> Result<Vec<Gap>, String> {
    let ring = BlockingSpscRing::create_anon(CAPACITY).map_err(|e| format!("the ring: {e:?}"))?;
    let gaps: Vec<u64> = gaps_us.iter().map(|&us| cycles(us as f64, rate)).collect();
    let stop = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let producer = std::thread::Builder::new()
            .name("wait-verdict-producer".to_string())
            .spawn_scoped(scope, || produce(&ring, &gaps, ITEMS, &stop))
            .map_err(|e| format!("the producer did not start: {e}"))?;
        let consumer = std::thread::Builder::new()
            .name("wait-verdict-consumer".to_string())
            .spawn_scoped(scope, || {
                if let Some(processor) = pin {
                    pin_here(processor, "consumer")?;
                }
                consume(&ring, arm, rate, gaps_us, ITEMS, corunner)
            });
        let consumed = match consumer {
            Ok(consumer) => joined(consumer.join(), "consumer"),
            Err(e) => Err(format!("the consumer did not start: {e}")),
        };
        if consumed.is_err() {
            stop.store(true, Ordering::Relaxed);
        }
        let produced = joined(producer.join(), "producer");
        let gaps = consumed?;
        produced?;
        Ok(gaps)
    })
}

/// The files a file-backed ring at `base` lays out.
fn ring_files(base: &Path) -> Vec<PathBuf> {
    [".ring.bin", ".cw.bin", ".pw.bin"]
        .into_iter()
        .map(|suffix| {
            let mut path = base.as_os_str().to_owned();
            path.push(suffix);
            PathBuf::from(path)
        })
        .collect()
}

/// Runs the consumer on a thread of this process over a file-backed ring at
/// `base` whose producer is a second process.
fn measure_file(
    arm: Arm,
    rate: f64,
    base: &Path,
    corunner: Option<&CoRunner>,
    pin: Option<u32>,
) -> Result<Vec<Gap>, String> {
    let ring = BlockingSpscRing::create(base, CAPACITY).map_err(|e| format!("the ring: {e:?}"))?;
    let exe = std::env::current_exe().map_err(|e| format!("this program's path: {e}"))?;
    let mut producer = Command::new(exe)
        .arg("--producer")
        .arg(base)
        .arg(rate.to_string())
        .stdout(Stdio::null())
        .spawn()
        .map_err(|e| format!("the producer process did not start: {e}"))?;
    let consumed = std::thread::scope(|scope| {
        let consumer = std::thread::Builder::new()
            .name("wait-verdict-consumer".to_string())
            .spawn_scoped(scope, || {
                if let Some(processor) = pin {
                    pin_here(processor, "consumer")?;
                }
                consume(&ring, arm, rate, &GAPS_US, ITEMS, corunner)
            })
            .map_err(|e| format!("the consumer did not start: {e}"))?;
        joined(consumer.join(), "consumer")
    });
    let stopped = if consumed.is_err() {
        producer.kill().map_err(|e| format!("stopping the producer process: {e}"))
    } else {
        Ok(())
    };
    let exit = producer.wait().map_err(|e| format!("the producer process: {e}"))?;
    stopped?;
    let gaps = consumed?;
    if exit.success() {
        Ok(gaps)
    } else {
        Err(format!("the producer process ended with {exit}"))
    }
}

/// [`measure_file`] over a ring in the temporary directory, whose files are
/// removed afterward whatever the outcome.
fn run_file(arm: Arm, rate: f64, corunner: Option<&CoRunner>, pin: Option<u32>) -> Result<Vec<Gap>, String> {
    let base = std::env::temp_dir().join(format!("subetha-wait-verdict-{}", std::process::id()));
    let outcome = measure_file(arm, rate, &base, corunner, pin);
    let mut left = Vec::new();
    for file in ring_files(&base) {
        if let Err(e) = std::fs::remove_file(&file)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            left.push(format!("{}: {e}", file.display()));
        }
    }
    match outcome {
        Ok(gaps) if left.is_empty() => Ok(gaps),
        Ok(_gaps) => Err(format!("ring files were left: {}", left.join("; "))),
        Err(reason) if left.is_empty() => Err(reason),
        Err(reason) => Err(format!("{reason}; ring files were left: {}", left.join("; "))),
    }
}

/// The producer process of the file workload.
fn run_producer(base: &Path, rate: f64) -> Result<(), String> {
    let ring = BlockingSpscRing::open(base, CAPACITY).map_err(|e| format!("the ring: {e:?}"))?;
    let gaps: Vec<u64> = GAPS_US.iter().map(|&us| cycles(us as f64, rate)).collect();
    produce(&ring, &gaps, ITEMS, &AtomicBool::new(false))
}

/// The median latency, in counter cycles, of the spin control.
fn control(rate: f64) -> Result<u64, String> {
    let mut gaps = run_anon(Arm::Spin, rate, &[CONTROL_GAP_US], None, None)?;
    let mut latencies = gaps.pop().ok_or("the control measured no gap")?.latencies;
    latencies.sort_unstable();
    latencies.get(latencies.len() / 2).copied().ok_or_else(|| "the control took no item".to_string())
}

/// Prints the machine's busy logical processors since `since`, and answers
/// the new reading.
fn busy_line(tag: &str, when: &str, since: Option<CpuTimes>) -> Option<CpuTimes> {
    let now = CpuTimes::read();
    match now.zip(since).and_then(|(now, since)| now.busy_since(&since).map(|busy| (busy, now.processors()))) {
        Some((busy, processors)) => println!("BUSY {tag} when={when} busy={busy:.2} processors={processors}"),
        None => println!("BUSY {tag} when={when} busy=-"),
    }
    now
}

/// Waits until calibration has published the plan for `kind` on this
/// thread's class and band, up to [`READY_BOUND`].
fn wait_for_plan(kind: ParkKind) -> Result<(), String> {
    wait_calibration::start();
    let started = Instant::now();
    while wait_calibration::plan_for(kind).is_none() {
        if started.elapsed() > READY_BOUND {
            return Err(format!("no calibrated plan within {READY_BOUND:?}: {:?}", wait_calibration::report()));
        }
        std::thread::sleep(READY_POLL);
    }
    Ok(())
}

fn statistics(gap: &Gap) -> String {
    let mut latencies = gap.latencies.clone();
    latencies.sort_unstable();
    let taken = latencies.len();
    if taken == 0 {
        return format!("items=0 timeouts={}", gap.timeouts);
    }
    let mean = latencies.iter().map(|&latency| latency as f64).sum::<f64>() / taken as f64;
    let at = |fraction: f64| latencies[((taken - 1) as f64 * fraction) as usize];
    let processor = gap.processor.map(|spent| spent / (taken as u64 + gap.timeouts) as f64);
    let cost = processor.map(|processor| mean + processor);
    let show = |value: Option<f64>| value.map_or_else(|| "-".to_string(), |v| format!("{v:.0}"));
    format!(
        "items={taken} timeouts={} lat_mean={mean:.0} lat_p50={} lat_p99={} cpu_per_item={} cost={} corunner_per_s={}",
        gap.timeouts,
        at(0.5),
        at(0.99),
        show(processor),
        show(cost),
        show(gap.corunner_per_s)
    )
}

/// One arm's process: its controls, its load readings and its gaps.
fn run_arm(arm: Arm, workload: Workload, corunner_pass: bool, round: usize) -> Result<(), String> {
    let rate = rate_per_us();
    let tag = format!(
        "arm={} workload={} corunner={} round={round}",
        arm.name(),
        workload.name(),
        u8::from(corunner_pass)
    );
    if arm == Arm::Calibrated {
        wait_for_plan(workload.park_kind())?;
    }
    let start = CpuTimes::read();
    println!("CONTROL {tag} when=before lat_p50={}", control(rate)?);
    let before = busy_line(&tag, "control-before", start);
    let (corunner, pin) = if corunner_pass {
        let (waiter, sibling) = sibling_pair()?;
        (Some(CoRunner::start(sibling)?), Some(waiter))
    } else {
        (None, None)
    };
    let gaps = match workload {
        Workload::Anon => run_anon(arm, rate, &GAPS_US, corunner.as_ref(), pin),
        Workload::File => run_file(arm, rate, corunner.as_ref(), pin),
    };
    let stopped = corunner.map_or(Ok(()), CoRunner::stop);
    let gaps = gaps?;
    stopped?;
    let after = busy_line(&tag, "arm", before);
    for gap in &gaps {
        println!("RESULT {tag} gap_us={} {}", gap.gap_us, statistics(gap));
    }
    println!("CONTROL {tag} when=after lat_p50={}", control(rate)?);
    busy_line(&tag, "control-after", after);
    Ok(())
}

/// The warm-up process: calibrates until the band of the last load
/// reading has a wake set in force or refused for every class, up to
/// [`READY_BOUND`], so the cache holds it for the arms.
fn run_warm() -> Result<(), String> {
    wait_calibration::start();
    let started = Instant::now();
    loop {
        let report = wait_calibration::report();
        let settled = report.load.is_some_and(|(_, band)| {
            report.classes.iter().all(|class| {
                class
                    .bands
                    .iter()
                    .any(|entry| entry.band == band && matches!(entry.wakes, Record::Ready { .. } | Record::Refused(_)))
            })
        });
        if settled {
            println!("WARM {report:?}");
            return Ok(());
        }
        if started.elapsed() > READY_BOUND {
            return Err(format!("calibration did not settle within {READY_BOUND:?}: {report:?}"));
        }
        std::thread::sleep(READY_POLL);
    }
}

/// One line an arm's process printed: its kind and its `key=value` words.
struct Output {
    kind: String,
    fields: BTreeMap<String, String>,
}

impl Output {
    fn parse(line: &str) -> Option<Output> {
        let mut words = line.split_whitespace();
        let kind = words.next()?.to_string();
        let fields = words
            .filter_map(|word| word.split_once('='))
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        Some(Output { kind, fields })
    }

    fn is(&self, workload: Workload, corunner: bool) -> bool {
        self.fields.get("workload").map(String::as_str) == Some(workload.name())
            && self.fields.get("corunner").map(String::as_str) == Some(if corunner { "1" } else { "0" })
    }

    /// The number under `key`: `None` where the line has no such field or
    /// printed `-` for it.
    fn number(&self, key: &str) -> Result<Option<f64>, String> {
        match self.fields.get(key).map(String::as_str) {
            None | Some("-") => Ok(None),
            Some(text) => text.parse::<f64>().map(Some).map_err(|e| format!("{key}={text}: {e}")),
        }
    }

    fn required(&self, key: &str) -> Result<f64, String> {
        self.number(key)?.ok_or_else(|| format!("no {key}"))
    }
}

/// Runs this program with `args` under `arm`'s environment, echoing its
/// output, and answers its lines.
fn run_child(exe: &Path, args: &[String], arm: Arm) -> Result<Vec<Output>, String> {
    let mut command = Command::new(exe);
    command.args(args).stderr(Stdio::inherit());
    arm.configure(&mut command);
    let output = command.output().map_err(|e| format!("running {args:?}: {e}"))?;
    let text = String::from_utf8_lossy(&output.stdout);
    for line in text.lines() {
        println!("  {line}");
    }
    if !output.status.success() {
        println!("  FAILED {args:?}: {}", output.status);
    }
    Ok(text.lines().filter_map(Output::parse).collect())
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied()
}

/// One workload and pass's costs per round, arm and gap, its controls per
/// round, and the lines it could not read.
#[derive(Default)]
struct Cells {
    costs: BTreeMap<(usize, Arm, u64), f64>,
    controls: BTreeMap<usize, Vec<f64>>,
    timeouts: f64,
    unreadable: Vec<String>,
}

impl Cells {
    fn gather(lines: &[Output], workload: Workload, corunner: bool) -> Cells {
        let mut cells = Cells::default();
        for line in lines.iter().filter(|line| line.is(workload, corunner)) {
            if let Err(reason) = cells.add(line) {
                cells.unreadable.push(format!("{} {:?}: {reason}", line.kind, line.fields));
            }
        }
        cells
    }

    fn add(&mut self, line: &Output) -> Result<(), String> {
        match line.kind.as_str() {
            "RESULT" => {
                let round = line.required("round")? as usize;
                let arm = Arm::from_name(line.fields.get("arm").ok_or("no arm")?)?;
                let gap = line.required("gap_us")? as u64;
                self.timeouts += line.required("timeouts")?;
                if let Some(cost) = line.number("cost")? {
                    self.costs.insert((round, arm, gap), cost);
                }
            }
            "CONTROL" => {
                let round = line.required("round")? as usize;
                let latency = line.required("lat_p50")?;
                self.controls.entry(round).or_default().push(latency);
            }
            _ => {}
        }
        Ok(())
    }

    fn cost(&self, round: usize, arm: Arm, gap: u64) -> Option<f64> {
        self.costs.get(&(round, arm, gap)).copied()
    }

    /// Whether `gap`'s comparison in `round` is void: its controls moved by
    /// more than the calibrated arm differs from the better fixed arm, or a
    /// cost is missing.
    fn void(&self, round: usize, gap: u64) -> bool {
        let movement = match self.controls.get(&round) {
            Some(values) if !values.is_empty() => {
                let low = values.iter().copied().fold(f64::INFINITY, f64::min);
                let high = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                high - low
            }
            Some(_) | None => return true,
        };
        match (self.cost(round, Arm::Spin, gap), self.cost(round, Arm::Park, gap), self.cost(round, Arm::Calibrated, gap)) {
            (Some(spin), Some(park), Some(calibrated)) => movement > (calibrated - spin.min(park)).abs(),
            _ => true,
        }
    }
}

fn show(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{v:.0}"))
}

/// Where a clause's rounds fall against its bound: every round within it,
/// every round beyond it, rounds on both sides, or no round to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reading {
    Within,
    Beyond,
    Undecided,
    Missing,
}

impl Reading {
    /// The reading of per-round `values` against `holds`.
    fn of(values: &[f64], holds: impl Fn(f64) -> bool) -> Reading {
        let held = values.iter().filter(|&&value| holds(value)).count();
        if values.is_empty() {
            Reading::Missing
        } else if held == values.len() {
            Reading::Within
        } else if held == 0 {
            Reading::Beyond
        } else {
            Reading::Undecided
        }
    }

    fn word(self) -> &'static str {
        match self {
            Reading::Within => "yes",
            Reading::Beyond => "NO",
            Reading::Undecided => "undecided",
            Reading::Missing => "-",
        }
    }
}

/// Prints the verdict for one workload and pass from the arms' lines. Each
/// arm's cost at a gap prints as the median of its rounds, and each clause
/// is read round by round. `ladder_tier` says whether the fixed ladder ran
/// its monitor tier, which is what the summed clause against it compares.
fn verdict(lines: &[Output], workload: Workload, corunner: bool, ladder_tier: bool) {
    let cells = Cells::gather(lines, workload, corunner);
    println!();
    println!("VERDICT workload={} corunner={}", workload.name(), u8::from(corunner));
    for line in &cells.unreadable {
        println!("  UNREADABLE {line}");
    }
    println!("  gap_us   spin   park shipped calibrated  cal/best  within-2x  void-rounds  rounds");
    let mut sums: BTreeMap<Arm, f64> = BTreeMap::new();
    let mut gaps = Vec::new();
    let mut complete = true;
    for gap in GAPS_US {
        let [spin, park, shipped, calibrated] =
            Arm::ALL.map(|arm| median((0..ROUNDS).filter_map(|round| cells.cost(round, arm, gap)).collect()));
        let void_rounds = (0..ROUNDS).filter(|&round| cells.void(round, gap)).count();
        let ratio = calibrated.zip(spin.zip(park)).map(|(calibrated, (spin, park))| calibrated / spin.min(park));
        // Each round's calibrated cost over that round's better fixed arm.
        let rounds: Vec<f64> = (0..ROUNDS)
            .filter_map(|round| {
                let best = cells.cost(round, Arm::Spin, gap)?.min(cells.cost(round, Arm::Park, gap)?);
                Some(cells.cost(round, Arm::Calibrated, gap)? / best)
            })
            .collect();
        if rounds.len() < ROUNDS {
            complete = false;
        }
        let within = Reading::of(&rounds, |ratio| ratio <= 2.0);
        gaps.push((gap, within));
        for (arm, value) in Arm::ALL.into_iter().zip([spin, park, shipped, calibrated]) {
            match value {
                Some(value) => *sums.entry(arm).or_default() += value,
                None => complete = false,
            }
        }
        println!(
            "  {gap:>6} {:>6} {:>6} {:>7} {:>10} {:>9} {:>10} {:>12}  {}",
            show(spin),
            show(park),
            show(shipped),
            show(calibrated),
            ratio.map_or_else(|| "-".to_string(), |r| format!("{r:.2}")),
            within.word(),
            void_rounds,
            rounds.iter().map(|ratio| format!("{ratio:.2}")).collect::<Vec<_>>().join("/")
        );
    }
    let sum = |arm: Arm| sums.get(&arm).copied();
    // Each round's calibrated total over the gaps against another arm's.
    let summed_below = |other: Arm| {
        let rounds: Vec<f64> = (0..ROUNDS)
            .filter_map(|round| {
                let total = |arm: Arm| GAPS_US.iter().map(|&gap| cells.cost(round, arm, gap)).sum::<Option<f64>>();
                Some(total(Arm::Calibrated)? / total(other)?)
            })
            .collect();
        Reading::of(&rounds, |ratio| ratio < 1.0)
    };
    println!(
        "  summed: spin {} park {} shipped {} calibrated {}; timed-out items {:.0}",
        show(sum(Arm::Spin)),
        show(sum(Arm::Park)),
        show(sum(Arm::Shipped)),
        show(sum(Arm::Calibrated)),
        cells.timeouts
    );
    let every_gap = if gaps.iter().any(|&(_, reading)| reading == Reading::Beyond) {
        "NO"
    } else if gaps.iter().all(|&(_, reading)| reading == Reading::Within) {
        "yes"
    } else {
        "undecided"
    };
    let undecided: Vec<String> = gaps
        .iter()
        .filter(|&&(_, reading)| reading == Reading::Undecided)
        .map(|(gap, _)| format!("{gap} us"))
        .collect();
    println!(
        "  within 2x of the better fixed arm at every gap: {every_gap}{}; summed below spin: {}; summed below shipped: {}{}",
        if undecided.is_empty() { String::new() } else { format!(" (undecided at {})", undecided.join(", ")) },
        summed_below(Arm::Spin).word(),
        if ladder_tier { summed_below(Arm::Shipped).word() } else { "not applied, the ladder has no monitor tier" },
        if complete { "" } else { "; INCOMPLETE: an arm has no cost at some gap" }
    );
}

/// Runs the warm-up, every arm of every round, and prints the verdicts.
fn drive() -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| format!("this program's path: {e}"))?;
    let cpuid = subetha_core::cpuid::cpuid();
    println!(
        "HOST vendor={} family={:#x} model={:#x} stepping={} invariant_tsc={} monitor={:?} rate_per_us={:.1} processors={}",
        cpuid.vendor,
        cpuid.family,
        cpuid.model,
        cpuid.stepping,
        cpuid.invariant_tsc,
        subetha_cxc::monitor_wait::monitor_wait_kind(),
        rate_per_us(),
        CpuTimes::read().map_or(0, |times| times.processors())
    );
    println!("WARM-UP");
    run_child(&exe, &["--warm".to_string()], Arm::Calibrated)?;
    let passes: Vec<bool> = match sibling_pair() {
        Ok((waiter, sibling)) => {
            println!("CORUNNER waiter={waiter} sibling={sibling}");
            vec![false, true]
        }
        Err(reason) => {
            println!("CORUNNER unavailable: {reason}");
            vec![false]
        }
    };
    let mut lines = Vec::new();
    for round in 0..ROUNDS {
        for workload in Workload::ALL {
            for &corunner in &passes {
                for index in 0..Arm::ALL.len() {
                    let arm = Arm::ALL[(index + round) % Arm::ALL.len()];
                    let mut args = vec![
                        "--arm".to_string(),
                        arm.name().to_string(),
                        "--workload".to_string(),
                        workload.name().to_string(),
                        "--round".to_string(),
                        round.to_string(),
                    ];
                    if corunner {
                        args.push("--corunner".to_string());
                    }
                    lines.extend(run_child(&exe, &args, arm)?);
                }
            }
        }
    }
    let ladder_tier = subetha_cxc::monitor_wait::monitor_wait_kind().is_some();
    for workload in Workload::ALL {
        for &corunner in &passes {
            verdict(&lines, workload, corunner, ladder_tier);
        }
    }
    Ok(())
}

/// Prints the verdicts again from a run's output saved at `path`, with the
/// co-runner pass where the run had one.
fn read_saved(path: &Path) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let lines: Vec<Output> = text.lines().filter_map(Output::parse).collect();
    let corunner_pass = lines
        .iter()
        .any(|line| line.kind == "RESULT" && line.fields.get("corunner").map(String::as_str) == Some("1"));
    let monitor = lines
        .iter()
        .find(|line| line.kind == "HOST")
        .and_then(|line| line.fields.get("monitor"))
        .ok_or("the saved run has no HOST line naming its monitor")?;
    let ladder_tier = monitor != "None";
    for workload in Workload::ALL {
        for corunner in [false, true] {
            if !corunner || corunner_pass {
                verdict(&lines, workload, corunner, ladder_tier);
            }
        }
    }
    Ok(())
}

/// The value after `flag` in `args`.
fn flag_value<'a>(args: &'a [String], flag: &str) -> Result<&'a str, String> {
    let at = args.iter().position(|arg| arg == flag).ok_or_else(|| format!("{flag} is missing"))?;
    args.get(at + 1).map(String::as_str).ok_or_else(|| format!("{flag} has no value"))
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => drive(),
        Some("--warm") => run_warm(),
        Some("--read") => read_saved(Path::new(args.get(1).ok_or("--read needs a saved run's path")?)),
        Some("--producer") => {
            let base = PathBuf::from(args.get(1).ok_or("--producer needs the ring's path")?);
            let rate = args
                .get(2)
                .ok_or("--producer needs the counter's rate")?
                .parse::<f64>()
                .map_err(|e| format!("the counter's rate: {e}"))?;
            run_producer(&base, rate)
        }
        Some("--arm") => {
            let arm = Arm::from_name(flag_value(&args, "--arm")?)?;
            let workload = Workload::from_name(flag_value(&args, "--workload")?)?;
            let round = flag_value(&args, "--round")?.parse::<usize>().map_err(|e| format!("--round: {e}"))?;
            run_arm(arm, workload, args.iter().any(|arg| arg == "--corunner"), round)
        }
        Some(other) => Err(format!("unknown argument {other:?}")),
    }
}

fn main() {
    if let Err(reason) = run() {
        println!("ERROR {reason}");
        std::process::exit(1);
    }
}
