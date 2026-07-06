//! `BackgroundScheduler` - autonomous Pass executor backed by
//! [`SharedRing`] + [`HeartbeatTable`] + [`FailoverWatchdog`] +
//! the [`pass_registry`](crate::pass_registry) closure table.
//!
//! Each participating process:
//! 1. Opens the shared submit-ring and shared result-ring.
//! 2. Registers itself in the heartbeat table.
//! 3. Drives one worker thread that drains the submit-ring,
//!    executes the Pass via the closure registry, and pushes the
//!    result onto the result-ring.
//! 4. Optionally drives the FailoverWatchdog (one process per
//!    cluster, typically the coordinator).
//!
//! The same MMF backing gives cross-thread + cross-process + disk
//! durability. The submit-ring file IS the persistent queue; a
//! process that died holding work in the ring loses nothing; when
//! it restarts, those slots are still there.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::failover::FailoverWatchdog;
use crate::heartbeat::{HeartbeatError, HeartbeatTable};
use crate::message_transport::{MessageTransport, TransportError};
use crate::pass_registry::{execute as exec_pass, Pass, PassResult};
use crate::shared_ring::{RingError, SharedRing, PAYLOAD_BYTES};

/// Submit-ring payload encoding: a Pass serialised into the slot.
///
/// Wire format (fits in PAYLOAD_BYTES = 56 bytes):
/// ```text
/// [closure_id: u32 LE][result_token: u32 LE][arg_len: u16 LE][args: [u8; ...]]
/// ```
///
/// The result_token is a caller-supplied correlation ID echoed in
/// the result-ring payload so the originator can match results to
/// submissions. The args slice is bounded by what fits in a single
/// slot; oversized passes need a side-channel for the args.
const RESULT_TOKEN_OFFSET: usize = 4;
const ARG_LEN_OFFSET: usize = 8;
const ARGS_OFFSET: usize = 10;
const MAX_ARG_LEN: usize = PAYLOAD_BYTES - ARGS_OFFSET;

/// Result-ring payload encoding:
/// ```text
/// [result_token: u32 LE][status: u8][result_len: u16 LE][result: [u8; ...]]
/// ```
const RESULT_TOKEN_R_OFFSET: usize = 0;
const STATUS_OFFSET: usize = 4;
const RESULT_LEN_OFFSET: usize = 5;
const RESULT_DATA_OFFSET: usize = 7;
const MAX_RESULT_LEN: usize = PAYLOAD_BYTES - RESULT_DATA_OFFSET;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedError {
    Ring(RingError),
    Transport(TransportError),
    Heartbeat(HeartbeatError),
    ArgsTooLarge,
    ResultTooLarge,
    /// The MmfDispatcher selected a family the scheduler cannot use
    /// for its push / pop wire format (`SharedHashMap`, which is
    /// key-value, not streaming).
    UnsupportedTransportFamily(crate::mmf_dispatcher::MmfFamily),
}

impl From<RingError> for SchedError {
    fn from(e: RingError) -> Self { Self::Ring(e) }
}
impl From<TransportError> for SchedError {
    fn from(e: TransportError) -> Self { Self::Transport(e) }
}
impl From<HeartbeatError> for SchedError {
    fn from(e: HeartbeatError) -> Self { Self::Heartbeat(e) }
}

/// One submit-side handle: the producer's view of the scheduler.
///
/// Generic over any transport implementing [`MessageTransport`], so
/// the same `Submitter` API works with both the canonical MPMC
/// [`SharedRing`] and the SPMC `SharedDeque<PassSlot>` transport.
pub struct Submitter {
    submit_ring: Arc<dyn MessageTransport>,
    next_token: AtomicU64,
}

impl Submitter {
    pub fn new(submit_ring: Arc<dyn MessageTransport>) -> Self {
        Self { submit_ring, next_token: AtomicU64::new(1) }
    }

    /// Submit a Pass. Returns the result_token correlation id.
    pub fn submit(&self, pass: &Pass) -> Result<u32, SchedError> {
        if pass.args.len() > MAX_ARG_LEN {
            return Err(SchedError::ArgsTooLarge);
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed) as u32;
        let mut slot = [0u8; PAYLOAD_BYTES];
        slot[0..4].copy_from_slice(&pass.closure_id.to_le_bytes());
        slot[RESULT_TOKEN_OFFSET..ARG_LEN_OFFSET]
            .copy_from_slice(&token.to_le_bytes());
        slot[ARG_LEN_OFFSET..ARGS_OFFSET]
            .copy_from_slice(&(pass.args.len() as u16).to_le_bytes());
        slot[ARGS_OFFSET..ARGS_OFFSET + pass.args.len()]
            .copy_from_slice(&pass.args);
        self.submit_ring.try_push(&slot)?;
        Ok(token)
    }
}

/// Result-side handle: drains the result-ring.
pub struct ResultCollector {
    result_ring: Arc<dyn MessageTransport>,
}

#[derive(Debug, Clone)]
pub struct SubmittedResult {
    pub token: u32,
    pub result: PassResult,
}

impl ResultCollector {
    pub fn new(result_ring: Arc<dyn MessageTransport>) -> Self {
        Self { result_ring }
    }

    /// Drain one result. Returns `Err(Transport(Empty))` when
    /// there's nothing pending.
    pub fn try_recv(&self) -> Result<SubmittedResult, SchedError> {
        let mut slot = [0u8; PAYLOAD_BYTES];
        self.result_ring.try_pop(&mut slot)?;
        Ok(parse_result_slot(&slot))
    }
}

fn parse_result_slot(slot: &[u8; PAYLOAD_BYTES]) -> SubmittedResult {
    let token = u32::from_le_bytes(
        slot[RESULT_TOKEN_R_OFFSET..STATUS_OFFSET].try_into().unwrap()
    );
    let status = slot[STATUS_OFFSET];
    let len = u16::from_le_bytes(
        slot[RESULT_LEN_OFFSET..RESULT_DATA_OFFSET].try_into().unwrap()
    ) as usize;
    let data = slot[RESULT_DATA_OFFSET..RESULT_DATA_OFFSET + len.min(MAX_RESULT_LEN)].to_vec();
    let result = if status == 0 {
        Ok(data)
    } else {
        let msg = String::from_utf8_lossy(&data).into_owned();
        Err(crate::pass_registry::PassError::ExecutionError(msg))
    };
    SubmittedResult { token, result }
}

/// The autonomous executor running in this process.
///
/// Submit and result transports are abstracted behind
/// [`MessageTransport`] so the scheduler can ride either the
/// canonical MPMC [`SharedRing`] or the SPMC
/// `SharedDeque<PassSlot>` work-stealing transport. The constructor
/// picks per workload topology.
pub struct BackgroundScheduler {
    submit_ring: Arc<dyn MessageTransport>,
    result_ring: Arc<dyn MessageTransport>,
    heartbeat: Arc<HeartbeatTable>,
    slot_idx: usize,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl subetha_sidecar::AdaptiveInstance for BackgroundScheduler {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl BackgroundScheduler {
    /// Create a new scheduler. Opens the submit + result rings and
    /// the heartbeat table; registers this process; spawns the
    /// worker thread that drains the submit ring.
    pub fn start(
        submit_path: impl AsRef<Path>,
        result_path: impl AsRef<Path>,
        heartbeat_path: impl AsRef<Path>,
        capacity: usize,
        heartbeat_capacity: usize,
    ) -> Result<Self, SchedError> {
        let submit_ring: Arc<dyn MessageTransport> = Arc::new(
            SharedRing::open(submit_path.as_ref(), capacity)
                .or_else(|_| SharedRing::create(submit_path.as_ref(), capacity))?
        );
        let result_ring: Arc<dyn MessageTransport> = Arc::new(
            SharedRing::open(result_path.as_ref(), capacity)
                .or_else(|_| SharedRing::create(result_path.as_ref(), capacity))?
        );
        Self::start_with_transports(
            submit_ring,
            result_ring,
            heartbeat_path,
            heartbeat_capacity,
        )
    }

    /// Construct a scheduler from caller-supplied transports.
    /// Lets the caller pick the wire-format primitive at the
    /// transport layer (canonical [`SharedRing`] MPMC, SPMC
    /// `SharedDeque<PassSlot>`, or any other future
    /// [`MessageTransport`] impl).
    pub fn start_with_transports(
        submit_ring: Arc<dyn MessageTransport>,
        result_ring: Arc<dyn MessageTransport>,
        heartbeat_path: impl AsRef<Path>,
        heartbeat_capacity: usize,
    ) -> Result<Self, SchedError> {
        let heartbeat = Arc::new(
            HeartbeatTable::open(heartbeat_path.as_ref(), heartbeat_capacity)
                .or_else(|_| HeartbeatTable::create(heartbeat_path.as_ref(), heartbeat_capacity))?
        );
        let pid = std::process::id();
        let slot_idx = heartbeat.register(pid)?;

        let stop = Arc::new(AtomicBool::new(false));
        let stop_w = stop.clone();
        let submit_w = submit_ring.clone();
        let result_w = result_ring.clone();
        let hb_w = heartbeat.clone();
        let worker = std::thread::Builder::new()
            .name(format!("subetha-sched-worker-pid{pid}"))
            .spawn(move || {
                Self::worker_loop(submit_w, result_w, hb_w, slot_idx, stop_w);
            })
            .expect("spawn scheduler worker thread");

        Ok(Self {
            submit_ring,
            result_ring,
            heartbeat,
            slot_idx,
            stop,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
            worker: Some(worker),
        })
    }

    /// Construct a scheduler with transports picked by
    /// [`MmfDispatcher`](crate::MmfDispatcher) from caller-supplied
    /// workload shapes. The submit-side and result-side shapes can
    /// differ (the canonical pair is `StreamingMpmc` for submit and
    /// `WorkStealing(producer_fast(K))` for result). Returns the
    /// scheduler plus the picked families so the caller can confirm
    /// the routing decision matched expectations.
    ///
    /// `SharedRing` and `SharedDeque<PassSlot>` are the two
    /// supported transport families;
    /// [`MmfFamily::SharedHashMap`](crate::MmfFamily::SharedHashMap)
    /// is rejected with
    /// [`SchedError::UnsupportedTransportFamily`] because the
    /// scheduler's wire format is push / pop, not key / value.
    pub fn start_by_workload_shape(
        submit_path: impl AsRef<Path>,
        submit_shape: crate::mmf_dispatcher::MmfWorkloadShape,
        result_path: impl AsRef<Path>,
        result_shape: crate::mmf_dispatcher::MmfWorkloadShape,
        heartbeat_path: impl AsRef<Path>,
        capacity: usize,
        heartbeat_capacity: usize,
    ) -> Result<
        (
            Self,
            crate::mmf_dispatcher::MmfFamily,
            crate::mmf_dispatcher::MmfFamily,
        ),
        SchedError,
    > {
        let submit_family = crate::mmf_dispatcher::MmfDispatcher::pick(submit_shape);
        let result_family = crate::mmf_dispatcher::MmfDispatcher::pick(result_shape);
        let submit_transport =
            Self::build_transport_for(submit_family, submit_path.as_ref(), capacity)?;
        let result_transport =
            Self::build_transport_for(result_family, result_path.as_ref(), capacity)?;
        let sched = Self::start_with_transports(
            submit_transport,
            result_transport,
            heartbeat_path,
            heartbeat_capacity,
        )?;
        Ok((sched, submit_family, result_family))
    }

    /// Build a [`MessageTransport`] for the given family at `path`
    /// with `capacity` slots. `SharedRing` and `SharedDeque<PassSlot>`
    /// are supported; `SharedHashMap` returns
    /// `UnsupportedTransportFamily`.
    fn build_transport_for(
        family: crate::mmf_dispatcher::MmfFamily,
        path: &Path,
        capacity: usize,
    ) -> Result<Arc<dyn MessageTransport>, SchedError> {
        use crate::mmf_dispatcher::MmfFamily;
        use crate::shared_deque::SharedDeque;
        use crate::message_transport::PassSlot;
        match family {
            MmfFamily::SharedRing => {
                let ring = SharedRing::open(path, capacity)
                    .or_else(|_| SharedRing::create(path, capacity))?;
                Ok(Arc::new(ring))
            }
            MmfFamily::SharedDeque(_) => {
                let deque = SharedDeque::<PassSlot>::create(path, capacity)
                    .map_err(|_| {
                        SchedError::UnsupportedTransportFamily(family)
                    })?;
                Ok(Arc::new(deque))
            }
            MmfFamily::SharedHashMap => {
                Err(SchedError::UnsupportedTransportFamily(family))
            }
        }
    }

    /// Get a submitter handle (cheap clone-friendly).
    pub fn submitter(&self) -> Submitter {
        Submitter::new(self.submit_ring.clone())
    }

    /// Get a result collector handle.
    pub fn collector(&self) -> ResultCollector {
        ResultCollector::new(self.result_ring.clone())
    }

    /// Access the heartbeat table for failover-watchdog usage.
    pub fn heartbeat(&self) -> Arc<HeartbeatTable> { self.heartbeat.clone() }

    /// The slot index this scheduler claimed in the heartbeat table.
    pub fn slot_idx(&self) -> usize { self.slot_idx }

    /// One scan of the watchdog (typically called from the
    /// coordinator process's main loop). Returns reclaim report.
    pub fn watchdog_scan(&self) -> crate::failover::ReclaimReport {
        let r = FailoverWatchdog::new(&self.heartbeat).scan();
        self.ring_sidecar.push_op(
            crate::sidecar_ops::scheduler::OP_WATCHDOG_SCAN,
            if !r.is_empty() { 1 } else { 0 }, // 1 = reclaim required
        );
        r
    }

    fn worker_loop(
        submit: Arc<dyn MessageTransport>,
        result: Arc<dyn MessageTransport>,
        heartbeat: Arc<HeartbeatTable>,
        slot_idx: usize,
        stop: Arc<AtomicBool>,
    ) {
        let mut slot_buf = [0u8; PAYLOAD_BYTES];
        let mut beat_counter = 0u64;
        while !stop.load(Ordering::Acquire) {
            beat_counter += 1;
            if beat_counter.is_multiple_of(100) {
                heartbeat.beat(slot_idx);
            }
            match submit.try_pop(&mut slot_buf) {
                Ok(_) => {
                    let (pass, token) = parse_submit_slot(&slot_buf);
                    // Mark one in_flight bit (use the low byte of the token
                    // mod 64 as the bit position).
                    let bit = (token & 0x3F) as u8;
                    heartbeat.mark_in_flight(slot_idx, bit);
                    let r = exec_pass(&pass);
                    let result_payload = encode_result_slot(token, &r);
                    // Result ring may be full if no consumer is
                    // reading; we drop the result rather than blocking.
                    result.try_push(&result_payload).ok();
                    heartbeat.clear_in_flight(slot_idx, bit);
                }
                Err(TransportError::Empty) => {
                    // Idle: short sleep to avoid burning CPU.
                    std::thread::sleep(Duration::from_micros(100));
                }
                Err(_) => break,
            }
        }
        heartbeat.unregister(slot_idx);
    }
}

impl Drop for BackgroundScheduler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(w) = self.worker.take() {
            // Worker panic on shutdown is non-fatal; ignore join error.
            w.join().ok();
        }
    }
}

fn parse_submit_slot(slot: &[u8; PAYLOAD_BYTES]) -> (Pass, u32) {
    let closure_id = u32::from_le_bytes(slot[0..4].try_into().unwrap());
    let token = u32::from_le_bytes(slot[RESULT_TOKEN_OFFSET..ARG_LEN_OFFSET].try_into().unwrap());
    let arg_len = u16::from_le_bytes(
        slot[ARG_LEN_OFFSET..ARGS_OFFSET].try_into().unwrap()
    ) as usize;
    let args = slot[ARGS_OFFSET..ARGS_OFFSET + arg_len.min(MAX_ARG_LEN)].to_vec();
    (Pass { closure_id, args }, token)
}

fn encode_result_slot(token: u32, r: &PassResult) -> [u8; PAYLOAD_BYTES] {
    let mut buf = [0u8; PAYLOAD_BYTES];
    buf[RESULT_TOKEN_R_OFFSET..STATUS_OFFSET].copy_from_slice(&token.to_le_bytes());
    match r {
        Ok(data) => {
            buf[STATUS_OFFSET] = 0;
            let len = data.len().min(MAX_RESULT_LEN);
            buf[RESULT_LEN_OFFSET..RESULT_DATA_OFFSET]
                .copy_from_slice(&(len as u16).to_le_bytes());
            buf[RESULT_DATA_OFFSET..RESULT_DATA_OFFSET + len]
                .copy_from_slice(&data[..len]);
        }
        Err(e) => {
            buf[STATUS_OFFSET] = 1;
            let msg = format!("{e:?}");
            let bytes = msg.as_bytes();
            let len = bytes.len().min(MAX_RESULT_LEN);
            buf[RESULT_LEN_OFFSET..RESULT_DATA_OFFSET]
                .copy_from_slice(&(len as u16).to_le_bytes());
            buf[RESULT_DATA_OFFSET..RESULT_DATA_OFFSET + len]
                .copy_from_slice(&bytes[..len]);
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pass_registry;

    fn tmp(name: &str) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let mut s = std::env::temp_dir(); let pid = std::process::id();
        s.push(format!("subetha-sched-{name}-{pid}-submit.bin"));
        let mut r = std::env::temp_dir();
        r.push(format!("subetha-sched-{name}-{pid}-result.bin"));
        let mut h = std::env::temp_dir();
        h.push(format!("subetha-sched-{name}-{pid}-hb.bin"));
        (s, r, h)
    }

    fn cleanup(paths: &(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)) {
        std::fs::remove_file(&paths.0).ok();
        std::fs::remove_file(&paths.1).ok();
        std::fs::remove_file(&paths.2).ok();
    }

    #[test]
    fn deque_backed_scheduler_round_trips_pass_end_to_end() {
        // Wire the scheduler through `SharedDeque<PassSlot>` instead
        // of the canonical `SharedRing`. Same Pass round-trip; same
        // semantics; different transport primitive at the MMF layer.
        use crate::message_transport::PassSlot;
        use crate::shared_deque::SharedDeque;

        let id = 0x2100_0002;
        pass_registry::register(id, |args| {
            Ok(args.iter().map(|b| b.wrapping_add(1)).collect())
        });
        let paths = tmp("deque_rt");
        let submit_deque: Arc<dyn MessageTransport> = Arc::new(
            SharedDeque::<PassSlot>::create(&paths.0, 64).expect("submit create"),
        );
        let result_deque: Arc<dyn MessageTransport> = Arc::new(
            SharedDeque::<PassSlot>::create(&paths.1, 64).expect("result create"),
        );
        let sched = BackgroundScheduler::start_with_transports(
            submit_deque,
            result_deque,
            &paths.2,
            8,
        )
        .unwrap();
        let submitter = sched.submitter();
        let collector = sched.collector();
        let token = submitter
            .submit(&Pass {
                closure_id: id,
                args: vec![10, 20, 30],
            })
            .unwrap();
        let mut got = None;
        for _ in 0..200 {
            if let Ok(r) = collector.try_recv() {
                got = Some(r);
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let r = got.expect("result must arrive within 400ms");
        assert_eq!(r.token, token);
        match r.result {
            Ok(data) => assert_eq!(data, vec![11, 21, 31]),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
        pass_registry::unregister(id);
        drop(sched);
        cleanup(&paths);
    }

    #[test]
    fn workload_shape_routed_scheduler_round_trips_pass_end_to_end() {
        // Wire the scheduler through MmfDispatcher: submit-side is
        // StreamingMpmc (-> SharedRing), result-side is WorkStealing
        // (-> SharedDeque<PassSlot> via the deque family). Same Pass
        // round-trip as the canonical path.
        use crate::dispatch_deque::WorkloadShape;
        use crate::mmf_dispatcher::{MmfFamily, MmfWorkloadShape};

        let id = 0x2100_0003;
        pass_registry::register(id, |args| {
            Ok(args.iter().map(|b| b.wrapping_mul(3)).collect())
        });
        let paths = tmp("by_shape_rt");
        let submit_shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let result_shape =
            MmfWorkloadShape::WorkStealing(WorkloadShape::producer_fast(8));
        let (sched, submit_family, result_family) =
            BackgroundScheduler::start_by_workload_shape(
                &paths.0,
                submit_shape,
                &paths.1,
                result_shape,
                &paths.2,
                64,
                8,
            )
            .expect("by_workload_shape");
        // Submit side: SharedRing for streaming MPMC.
        assert_eq!(submit_family, MmfFamily::SharedRing);
        // Result side: SharedDeque family.
        assert!(matches!(result_family, MmfFamily::SharedDeque(_)));

        let submitter = sched.submitter();
        let collector = sched.collector();
        let token = submitter
            .submit(&Pass {
                closure_id: id,
                args: vec![5, 6, 7],
            })
            .unwrap();
        let mut got = None;
        for _ in 0..200 {
            if let Ok(r) = collector.try_recv() {
                got = Some(r);
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let r = got.expect("result must arrive within 400ms");
        assert_eq!(r.token, token);
        match r.result {
            Ok(data) => assert_eq!(data, vec![15, 18, 21]),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
        pass_registry::unregister(id);
        drop(sched);
        cleanup(&paths);
    }

    #[test]
    fn workload_shape_routed_scheduler_rejects_key_value_family() {
        // KeyValueLookup -> SharedHashMap, which the scheduler's
        // push/pop wire format cannot transport. Construction must
        // fail with UnsupportedTransportFamily.
        use crate::mmf_dispatcher::{MmfFamily, MmfWorkloadShape};

        let paths = tmp("by_shape_reject_kv");
        let bad_shape = MmfWorkloadShape::KeyValueLookup {
            n_readers: 1,
            n_writers: 1,
        };
        let good_shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: 1,
            n_consumers: 1,
        };
        let result = BackgroundScheduler::start_by_workload_shape(
            &paths.0,
            bad_shape,
            &paths.1,
            good_shape,
            &paths.2,
            64,
            8,
        );
        match result {
            Err(SchedError::UnsupportedTransportFamily(MmfFamily::SharedHashMap)) => {
                // expected
            }
            Err(other) => panic!("expected UnsupportedTransportFamily(SharedHashMap), got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        cleanup(&paths);
    }

    #[test]
    fn submit_execute_result_round_trip() {
        let id = 0x2000_0001;
        pass_registry::register(id, |args| {
            // echo doubled
            Ok(args.iter().map(|b| b.wrapping_mul(2)).collect())
        });
        let paths = tmp("rt");
        let sched = BackgroundScheduler::start(
            &paths.0, &paths.1, &paths.2, 64, 8,
        ).unwrap();
        let submitter = sched.submitter();
        let collector = sched.collector();
        let token = submitter.submit(&Pass {
            closure_id: id, args: vec![1, 2, 3, 4],
        }).unwrap();
        // Wait briefly for the worker to drain + produce.
        let mut got = None;
        for _ in 0..200 {
            if let Ok(r) = collector.try_recv() {
                got = Some(r);
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let r = got.expect("result must arrive within 400ms");
        assert_eq!(r.token, token);
        match r.result {
            Ok(data) => assert_eq!(data, vec![2, 4, 6, 8]),
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
        pass_registry::unregister(id);
        drop(sched);
        cleanup(&paths);
    }

    #[test]
    fn submit_unknown_closure_returns_error() {
        let paths = tmp("unknown");
        let sched = BackgroundScheduler::start(
            &paths.0, &paths.1, &paths.2, 64, 8,
        ).unwrap();
        let submitter = sched.submitter();
        let collector = sched.collector();
        let token = submitter.submit(&Pass {
            closure_id: 0x9999_FFFF, args: vec![],
        }).unwrap();
        let mut got = None;
        for _ in 0..200 {
            if let Ok(r) = collector.try_recv() {
                got = Some(r);
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let r = got.expect("error result must arrive");
        assert_eq!(r.token, token);
        assert!(r.result.is_err());
        drop(sched);
        cleanup(&paths);
    }

    #[test]
    fn args_too_large_rejected_at_submit() {
        let paths = tmp("oversized");
        let sched = BackgroundScheduler::start(
            &paths.0, &paths.1, &paths.2, 8, 4,
        ).unwrap();
        let submitter = sched.submitter();
        let big = vec![0u8; MAX_ARG_LEN + 1];
        assert_eq!(
            submitter.submit(&Pass { closure_id: 1, args: big }).unwrap_err(),
            SchedError::ArgsTooLarge,
        );
        drop(sched);
        cleanup(&paths);
    }

    #[test]
    fn scheduler_registers_in_heartbeat_table() {
        let paths = tmp("hb-reg");
        let sched = BackgroundScheduler::start(
            &paths.0, &paths.1, &paths.2, 8, 4,
        ).unwrap();
        let snap = sched.heartbeat().snapshot(sched.slot_idx()).unwrap();
        assert_eq!(snap.pid, std::process::id());
        drop(sched);
        cleanup(&paths);
    }

    #[test]
    fn many_passes_round_trip_in_order_of_submission() {
        let id = 0x2000_0002;
        pass_registry::register(id, |args| Ok(args.to_vec()));
        let paths = tmp("many");
        let sched = BackgroundScheduler::start(
            &paths.0, &paths.1, &paths.2, 64, 4,
        ).unwrap();
        let submitter = sched.submitter();
        let collector = sched.collector();
        let mut tokens = Vec::new();
        for i in 0..16u8 {
            let t = submitter.submit(&Pass {
                closure_id: id, args: vec![i; 4],
            }).unwrap();
            tokens.push(t);
        }
        let mut got = std::collections::HashMap::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while got.len() < tokens.len() && std::time::Instant::now() < deadline {
            if let Ok(r) = collector.try_recv() {
                got.insert(r.token, r.result);
            } else {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        assert_eq!(got.len(), tokens.len());
        for (i, t) in tokens.iter().enumerate() {
            let r = got.get(t).expect("result for every token");
            let data = r.as_ref().expect("Ok result");
            assert_eq!(data, &vec![i as u8; 4]);
        }
        pass_registry::unregister(id);
        drop(sched);
        cleanup(&paths);
    }
}
