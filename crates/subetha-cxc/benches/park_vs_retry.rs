//! Bench: where parking starts beating spinning on a full ring.
//!
//! `BlockingSpscRing::send_blocking` spins `PRE_PARK_SPIN` times and
//! then parks on a waker. The alternative a caller can always write
//! instead is a `try_push` retry loop, which never parks. Which one
//! wins depends on how long the producer has to wait, which is set by
//! how fast the consumer drains.
//!
//! The consumer is paced by work done rather than by time waited. A
//! first version of this paced with `thread::sleep` across 1 to 500
//! microseconds and measured nothing: a zero-length sleep costs about
//! 232 microseconds on the box it ran on, so every interval in that
//! range collapsed onto one floor and five of six points were the same
//! regime wearing different labels. Iterations of a cheap computation
//! resolve at any scale and cost what they say on any host.
//!
//! Two contenders against one `BlockingSpscRing`, capacity 8:
//!
//! - **park**: `send_blocking(payload, None)`, the unbounded park.
//! - **retry**: `try_push` in a loop with `spin_loop`, never parking.
//!
//! Wall time alone flatters retry, because a spin that burns a core
//! still returns the instant space appears. So the retry arm reports
//! the iterations it burned, and both arms run again with every core
//! saturated. That second pass is the one a staged pipeline lives in,
//! where the cores a spinning producer occupies are the cores its own
//! consumer needs.
//!
//! Every cell is the median of `REPEATS`, and the first cell is
//! repeated at the end as a control, because position within a run is
//! worth several percent on its own and a difference smaller than the
//! control's own drift is not a difference.
//!
//! Results go to stderr: run over ssh through a measurement-lease
//! wrapper, this binary's stdout does not survive the trip.
//!
//! What it established on a 24-core Windows host, so the next reader
//! does not rediscover it: the contended arm does not resolve. Its
//! control drift stays at 51 to 67 percent, because the load that is
//! the condition under test is also what makes the timing unstable, and
//! every wall-time gap in that half sits inside that floor. The idle
//! arm resolves at 19 to 31 percent, and there two cells clear it, both
//! favoring retry: a slow consumer on an idle box makes spinning 1.5
//! to 1.9 times faster on wall time, bought with 418,307 and 5,392,065
//! burned iterations.
//!
//! So the spin counts, not the times, are what this bench answers with.
//! They are counts rather than timings, they held across every run, and
//! they rise monotonically from about 24,000 at a fast consumer to
//! about 7.85 million at a slow one. Resolving the wall-time question
//! would need thread pinning and producer CPU time rather than wall
//! time, which is a different bench.
//!
//! Run:  cargo bench --bench park_vs_retry

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::blocking_spsc_ring::BlockingSpscRing;

const CAPACITY: usize = 8;
/// Enough items that the measured window dwarfs the cost of starting
/// and joining the consumer thread. At 2_000 the idle cells measured
/// about 58 microseconds of work each, which is the same order as
/// thread setup, and the control cell drifted 205 percent: the table
/// was unreadable and said so.
const ITEMS: usize = 100_000;
const REPEATS: usize = 5;
const PAYLOAD: &[u8] = b"0123456789abcdef";

/// How much work the consumer does between drains, in iterations of a
/// multiply-add. Zero is "drain as fast as possible", where the
/// producer barely waits; the top of the range is a stage slow enough
/// that the producer waits almost all the time.
const DRAIN_WORK: &[u64] = &[0, 100, 1_000, 10_000, 100_000];

/// A unit of consumer work. Deterministic, cheap, and impossible for
/// the optimiser to remove because the result is handed to `black_box`.
#[inline(never)]
fn burn(iters: u64) -> u64 {
    let mut x = 1u64;
    for _ in 0..iters {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
    }
    black_box(x)
}

struct Outcome {
    wall: Duration,
    spins: u64,
}

/// One arm. `park` chooses the producer strategy; the consumer is
/// identical either way, so the only difference is how the producer
/// waits for space.
fn run(park: bool, drain_work: u64) -> Outcome {
    let ring = Arc::new(BlockingSpscRing::create_anon(CAPACITY).expect("ring"));
    // Sized past any slot width rather than guessed at: a buffer too
    // small makes every try_pop fail, which stalls the consumer and
    // parks the producer forever.
    let slot = 4096;
    let progress = Arc::new(AtomicUsize::new(0));

    let consumer = {
        let ring = Arc::clone(&ring);
        let progress = Arc::clone(&progress);
        thread::spawn(move || {
            let mut out = vec![0u8; slot];
            let mut taken = 0usize;
            while taken < ITEMS {
                if drain_work > 0 {
                    burn(drain_work);
                }
                while ring.try_pop(&mut out).is_ok() {
                    taken += 1;
                    progress.store(taken, Ordering::Release);
                    if taken >= ITEMS {
                        break;
                    }
                }
            }
        })
    };

    // A watchdog. A cell that stops making progress names itself and
    // exits non-zero, rather than printing nothing and exiting zero.
    {
        let progress = Arc::clone(&progress);
        thread::spawn(move || {
            let mut last = 0usize;
            let mut stuck = 0;
            loop {
                thread::sleep(Duration::from_secs(5));
                let now = progress.load(Ordering::Acquire);
                if now >= ITEMS {
                    return;
                }
                if now == last {
                    stuck += 1;
                    if stuck >= 4 {
                        eprintln!("STALLED at {now}/{ITEMS}, park={park} drain_work={drain_work}");
                        std::process::exit(3);
                    }
                } else {
                    stuck = 0;
                }
                last = now;
            }
        });
    }

    let mut spins = 0u64;
    let started = Instant::now();
    if park {
        for _ in 0..ITEMS {
            ring.send_blocking(black_box(PAYLOAD), None).expect("park send");
        }
    } else {
        for _ in 0..ITEMS {
            loop {
                if ring.try_push(black_box(PAYLOAD)).is_ok() {
                    break;
                }
                spins += 1;
                std::hint::spin_loop();
            }
        }
    }
    let wall = started.elapsed();
    consumer.join().expect("consumer thread");

    Outcome { wall, spins }
}

/// The median of `REPEATS` runs of one arm, with the spin count from
/// the same run that produced the median time.
fn median(park: bool, drain_work: u64) -> Outcome {
    let mut got: Vec<Outcome> = (0..REPEATS).map(|_| run(park, drain_work)).collect();
    got.sort_by_key(|o| o.wall);
    got.swap_remove(REPEATS / 2)
}

/// Saturate every core, so a spinning producer competes for the cores
/// its own consumer needs rather than running on a free one.
fn with_load<T>(on: bool, f: impl FnOnce() -> T) -> T {
    if !on {
        return f();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let hogs: Vec<_> = (0..cores)
        .map(|_| {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    burn(1_000);
                }
            })
        })
        .collect();
    let out = f();
    stop.store(true, Ordering::Relaxed);
    for h in hogs {
        h.join().expect("load thread");
    }
    out
}

fn cell(loaded: bool, drain_work: u64) -> (f64, f64, u64) {
    let p = with_load(loaded, || median(true, drain_work));
    let r = with_load(loaded, || median(false, drain_work));
    (
        p.wall.as_nanos() as f64 / ITEMS as f64,
        r.wall.as_nanos() as f64 / ITEMS as f64,
        r.spins,
    )
}

fn main() {
    let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    eprintln!("park_vs_retry: {ITEMS} items, capacity {CAPACITY}, {cores} cores, median of {REPEATS}");
    eprintln!("wall is ns per item from the producer's side; spins is the retry arm's burned iterations\n");

    for &loaded in &[false, true] {
        eprintln!("{}", if loaded { "== every core saturated ==" } else { "== idle machine ==" });
        eprintln!("{:>11} {:>12} {:>12} {:>10} {:>16}", "drain work", "park ns", "retry ns", "winner", "retry spins");

        let first = DRAIN_WORK[0];
        let (p0, r0, _) = cell(loaded, first);

        for &w in DRAIN_WORK {
            let (pn, rn, spins) = cell(loaded, w);
            let winner = if pn < rn { "park" } else { "retry" };
            eprintln!("{w:>11} {pn:>12.0} {rn:>12.0} {winner:>10} {spins:>16}");
        }

        // The control: the first cell again, at the end. Its drift from
        // its own earlier reading is the floor below which nothing in
        // this table is a difference.
        let (pc, rc, _) = cell(loaded, first);
        let drift = |a: f64, b: f64| ((b - a) / a * 100.0).abs();
        eprintln!("{:>11} {pc:>12.0} {rc:>12.0}", "control", );
        eprintln!(
            "  control drift: park {:.1}%, retry {:.1}% - read nothing smaller than this as a difference\n",
            drift(p0, pc),
            drift(r0, rc)
        );
    }
}
