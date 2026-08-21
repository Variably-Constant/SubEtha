//! Work submitted here is executed by another process.
//!
//! The parent creates the submit and result rings and pushes one
//! `Pass` - a closure id plus its arguments, never the closure itself.
//! It deliberately registers no handler for that id, so nothing in the
//! parent is capable of running the work.
//!
//! A worker child registers the handler under the same id and attaches
//! a `BackgroundScheduler` to the rings the parent already created. Its
//! worker thread drains the submit ring, executes against its own
//! registry, and pushes the result back.
//!
//! The parent then collects a result it could not have produced: the
//! closure-id-not-closure-code contract, with the id and the bytes
//! crossing but not the code, correlated back by token.

use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use subetha_cxc::message_transport::MessageTransport;
use subetha_cxc::pass_registry;
use subetha_cxc::{
    register_handler, BackgroundScheduler, Pass, ResultCollector, SharedRing, Submitter,
};

use crate::harness::{arg_path, require, BoxErr, Harness};

const RING_CAPACITY: usize = 64;
const HEARTBEAT_CAPACITY: usize = 8;

/// Closure id both sides agree on. Only the worker registers code for it.
const CLOSURE_ID: u32 = 0x3000_0001;

const ARGS: &[u8] = b"abc";
const EXPECTED: &[u8] = b"cba";

/// How long the parent waits for the worker process to answer.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(20);

pub fn parent(h: &Harness) -> Result<(), BoxErr> {
    let submit_path = h.path("submit.bin");
    let result_path = h.path("result.bin");
    let heartbeat_path = h.path("scheduler-hb.bin");

    let submit: Arc<dyn MessageTransport> = Arc::new(
        SharedRing::create(&submit_path, RING_CAPACITY)
            .map_err(|e| format!("create submit ring: {e:?}"))?,
    );
    let result: Arc<dyn MessageTransport> = Arc::new(
        SharedRing::create(&result_path, RING_CAPACITY)
            .map_err(|e| format!("create result ring: {e:?}"))?,
    );

    require(
        !pass_registry::is_registered(CLOSURE_ID),
        format!("parent has a handler for {CLOSURE_ID:#x}; the work could be done locally"),
    )?;

    let submitter = Submitter::new(submit);
    let collector = ResultCollector::new(result);

    let token = submitter
        .submit(&Pass { closure_id: CLOSURE_ID, args: ARGS.to_vec() })
        .map_err(|e| format!("submit: {e:?}"))?;
    println!("   parent: submitted closure {CLOSURE_ID:#x} as token {token}, no local handler");

    let mut worker = h.spawn(
        "worker",
        &[
            submit_path.to_string_lossy().as_ref(),
            result_path.to_string_lossy().as_ref(),
            heartbeat_path.to_string_lossy().as_ref(),
        ],
    )?;

    let answer = collect_until(&collector, ANSWER_TIMEOUT);
    worker.kill().ok();
    worker.wait().ok();

    let answer = answer.ok_or_else(|| {
        format!("no result within {ANSWER_TIMEOUT:?}; the worker process never answered")
    })?;

    require(
        answer.token == token,
        format!("result token {} does not correlate with submitted {token}", answer.token),
    )?;
    let data = answer
        .result
        .map_err(|e| format!("worker reported execution failure: {e:?}"))?;
    require(
        data == EXPECTED,
        format!("worker returned {data:?}, expected {EXPECTED:?}"),
    )?;
    println!("   parent: collected {:?} from the worker process", String::from_utf8_lossy(&data));
    Ok(())
}

fn collect_until(
    collector: &ResultCollector,
    timeout: Duration,
) -> Option<subetha_cxc::SubmittedResult> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(r) = collector.try_recv() {
            return Some(r);
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(5));
    }
}

pub fn child(role: &str, args: &[String]) -> Result<(), BoxErr> {
    match role {
        "worker" => worker(args),
        other => Err(format!("scheduler: unknown child role {other:?}").into()),
    }
}

/// Register the code for the id, attach to the parent's rings, serve
/// until killed.
fn worker(args: &[String]) -> Result<(), BoxErr> {
    let submit_path = arg_path(args, 0, "submit ring path")?;
    let result_path = arg_path(args, 1, "result ring path")?;
    let heartbeat_path = arg_path(args, 2, "heartbeat path")?;

    register_handler(CLOSURE_ID, |args| Ok(args.iter().rev().copied().collect()));
    println!("   worker: registered closure {CLOSURE_ID:#x}");

    let _scheduler = BackgroundScheduler::start(
        submit_path,
        result_path,
        heartbeat_path,
        RING_CAPACITY,
        HEARTBEAT_CAPACITY,
    )
    .map_err(|e| format!("worker start scheduler: {e:?}"))?;

    // The parent ends this process once it has its answer.
    loop {
        sleep(Duration::from_millis(25));
    }
}
