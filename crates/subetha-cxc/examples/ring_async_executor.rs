//! The ready queue IS the ring. An async executor where every task's
//! handle is scheduled by pushing it into a SubEtha Vyukov MPMC ring
//! and run by popping it back out - the ring is the scheduler, not a
//! data channel beside one.
//!
//! The point it proves: WORKERS are capped at hardware (one per logical
//! core), but TASKS are not capped at all. An unbounded task population
//! is multiplexed onto the fixed worker pool through the ring. A
//! 44-thread host (e.g. EPYC 9B14 Genoa) would drive this same task set
//! on 44 workers; here it runs on whatever `available_parallelism`
//! reports, and the tasks/worker ratio shows the decoupling.
//!
//! Each task suspends and resumes several times (a yield re-pushes its
//! handle through the ready ring), so the run counts thousands of real
//! schedule -> run -> reschedule cycles. Integrity is a sum-of-ids
//! checksum: every task must complete exactly once.
//!
//! Run:
//!     cargo run --release --example ring_async_executor -p subetha-cxc

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use subetha_cxc::ring_executor::RingExecutor;

const N_TASKS: u64 = 200_000;
const YIELDS_PER_TASK: u32 = 10;

/// Cooperative yield: re-schedules the current task through the ready
/// ring once, then resolves. Each `.await` of this is one full
/// handle round-trip (push on wake, pop on run) through the SubEtha
/// ring acting as the scheduler.
struct YieldNow {
    left: u32,
}

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.left == 0 {
            Poll::Ready(())
        } else {
            self.left -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn main() {
    // The executor sizes its worker pool and its per-worker ready-ring
    // shards to whatever host it lands on - no core count baked in.
    let exec = RingExecutor::with_available_parallelism(N_TASKS as usize);
    let workers = exec.worker_count();

    // Every task completes once and adds its id; the summed ids must
    // equal the closed form only when none was lost or run twice.
    let expected_sum: u64 = (0..N_TASKS).sum();
    let sum = Arc::new(AtomicU64::new(0));

    let total_schedule_ops = N_TASKS * (YIELDS_PER_TASK as u64 + 1);

    println!("ring-as-scheduler async executor (shape-adaptive)");
    println!("{N_TASKS} tasks, {YIELDS_PER_TASK} yields each");
    println!("detected {workers} logical cores -> {} ready-ring shards, \
              {} pinned to a core", exec.shard_count(), exec.pinned_workers());
    println!("per-shard capacity: {} slots", exec.shard_capacity());
    println!("tasks per worker: {} (workers are hardware-capped; tasks are not)",
             N_TASKS / workers as u64);
    println!("a thread-per-task design would need {N_TASKS} OS threads; this uses {workers}.\n");

    let t0 = Instant::now();
    for id in 0..N_TASKS {
        let sum = Arc::clone(&sum);
        exec.spawn(async move {
            YieldNow { left: YIELDS_PER_TASK }.await;
            sum.fetch_add(id, Ordering::AcqRel);
        });
    }
    exec.wait_idle();
    let elapsed = t0.elapsed();

    let got = sum.load(Ordering::Acquire);
    assert_eq!(got, expected_sum, "every task completed exactly once");

    exec.shutdown();

    println!("completed {N_TASKS} tasks ({total_schedule_ops} schedule/run cycles \
              through the shards) in {elapsed:?}");
    println!("{:.2} M task-schedules/s through the SubEtha ready shards",
             total_schedule_ops as f64 / elapsed.as_secs_f64() / 1e6);
    println!("{:.2} M tasks/s end to end, integrity OK (sum-of-ids checksum)",
             N_TASKS as f64 / elapsed.as_secs_f64() / 1e6);
    println!("served by {workers} OS threads, not {N_TASKS} - the rings multiplexed \
              an uncapped task set onto a hardware-shaped worker pool.");
}
