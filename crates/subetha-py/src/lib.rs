//! Python bindings for SubEtha, bound to the Rust directly.
//!
//! A Python call costs between eighty and a hundred and twenty times what
//! the C ABI boundary costs, so the shapes that earn their place here are
//! the ones that do not pay per item: a batch call carrying many items,
//! and a buffer the interpreter reads with no call at all. The per-call
//! surface exists because some operations are genuinely one call, not
//! because it is the way to move data.
//!
//! Objects own their mapping and release it when Python drops them or
//! when a `with` block ends. Failures raise rather than return a code.
//!
//! # Alignment
//!
//! Every SubEtha value held here is boxed, and that is load-bearing
//! rather than a style choice. A `#[pyclass]` instance is allocated by
//! Python's object allocator, which aligns to a pointer pair; several
//! SubEtha types are cache-line aligned, `HandshakeHeader` among them,
//! and a good many embed it. Storing one inline in a pyclass puts a
//! 64-byte-aligned type at a 16-byte-aligned address, and a release
//! build writing that header through an aligned SSE store faults on it.
//! A box is allocated by Rust, which honors the alignment; the pyclass
//! then holds only a pointer. The assertions below fail the build if a
//! field ever reintroduces the requirement.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
/// The range bound of the standard library, named apart from PyO3's own
/// `Bound`, which is a handle on a Python object and is what every other
/// mention of that word here means.
use std::ops::Bound as RangeEnd;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::{ffi, PyResult};

use subetha_cxc::raw_region::RawRegion;
use subetha_cxc::raw_treiber_stack::ElementLayout;
use subetha_cxc::shared_atomic::SharedAtomicU64;
use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::capacity_adaptive_ring::CapacityAdaptiveRing;
use subetha_cxc::ordering::OrderingMode;
use subetha_cxc::locale_adaptive_ring::{Locale, LocaleAdaptiveRing};
use subetha_cxc::ordering::{StampKind, STAMPED_PAYLOAD_BYTES};
use subetha_cxc::owner_lease::{LeaseError, OwnerLease as SubethaOwnerLease};
use subetha_cxc::shared_blocked_bloom_filter::SharedBlockedBloomFilter;
use subetha_cxc::shared_handle_table::{
    Handle as SubethaHandle, HandleTableError, SharedHandleTable,
};
use subetha_cxc::shared_epochs::PinGuard as SubethaPinGuard;
use subetha_cxc::api::{
    ApiError, AutoIpc, Channel as SubethaChannel, KvMap as SubethaKvMap,
    WorkStealQueue as SubethaWorkStealQueue,
};
use subetha_pointers::bloom_pointer::{Bloom64, BloomFine};
use subetha_pointers::versioned_pointer::{HybridLogicalClock, VectorClock};
use subetha_cxc::blocking_rw_lock::{BlockingRWLock, BlockingRWLockError};
use subetha_cxc::blocking_semaphore::{BlockingSemaphore, BlockingSemaphoreError};
use subetha_cxc::adaptive_ipc::AdaptiveIpc as SubethaAdaptiveIpc;
use subetha_cxc::burst_model_sensor::BurstModel;
use subetha_cxc::forecast_sensor::ArrivalForecast;
use subetha_cxc::loss_class_sensor::{LossClass, LossClassSensor};
use subetha_cxc::path_sensor::PathSensor;
use subetha_cxc::periodicity_sensor::PeriodicitySensor;
use subetha_cxc::rtt_shape_sensor::RttShape;
use subetha_cxc::temporal_sensor::TemporalSensor;
use subetha_cxc::wbest_sensor::WBestEstimator;
use subetha_cxc::dispatch_deque::DequeVariant;
use subetha_cxc::message_transport::TransportError;
use subetha_cxc::mmf_dispatcher::{MmfFamily, MmfWorkloadShape};
use subetha_cxc::qos_policy::{
    Durability, History, Ordering as OrderingNeed, QosPolicy as SubethaQosPolicy,
    QosSnapshot as SubethaQosSnapshot, Reliability,
};
use subetha_cxc::raw_k_tower::{RawKTower, RawTowerError};
#[cfg(feature = "quic-bridge")]
use subetha_cxc::quic_bridge::{
    self, QuicBridgeClient as SubethaQuicClient, QuicBridgeServer as SubethaQuicServer,
};
#[cfg(feature = "tcp-bridge")]
use subetha_cxc::tcp_bridge::{
    TcpBridgeClient as SubethaTcpClient, TcpBridgeServer as SubethaTcpServer,
};
use subetha_cxc::sens_unified::{
    CodePolicy, SensCode, UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender,
};
use subetha_cxc::shared_graph::{EdgeIndex, NodeIndex, SharedGraph};
use subetha_cxc::shared_reservoir_sampler::SharedReservoirSampler;
use subetha_cxc::shared_topology_map::{
    SharedTopologyMap, TopologyKind, DEFAULT_FAN_IN_THRESHOLD, DEFAULT_FAN_OUT_THRESHOLD,
};
use subetha_cxc::shared_universal::{SharedUniversal, Strategy};
use subetha_cxc::shared_versioned_chain::SharedVersionedChain;
use subetha_cxc::shared_versioned_slab::SharedVersionedSlab;
use subetha_cxc::laned_versioned_map::{
    LanedError, LanedVersionedMap, LaneGuard as SubethaLaneGuard,
};
use subetha_cxc::versioned_btree_map::{VersionedBTreeMap, VersionedError};
use subetha_cxc::shared_time_point::{SharedTimePointTile, TILE_CAP as SUBETHA_TILE_CAP};
use subetha_cxc::reorder::{
    AdaptiveOrderedReceiver, ReorderBuffer, DEFAULT_CAP as REORDER_DEFAULT_CAP,
    DEFAULT_FLOOR as REORDER_DEFAULT_FLOOR,
};
use subetha_cxc::frame_region::FrameRegion as SubetnaFrameRegion;
use subetha_cxc::protocol_pubsub::{PubSubReadError, PubSubRing, PUBSUB_PAYLOAD_BYTES};
use subetha_cxc::raw_btree_map::RawBTreeMap;
use subetha_cxc::raw_cell::RawCell;
use subetha_cxc::raw_hash_map::RawHashMap;
use subetha_cxc::raw_linked_list::RawLinkedList;
use subetha_cxc::raw_lru_cache::RawLruCache;
use subetha_cxc::raw_deque::RawDeque;
use subetha_cxc::raw_slab::RawSlab;
use subetha_cxc::raw_treiber_stack::RawTreiberStack;
use subetha_cxc::shared_deque::DequeError;
use subetha_cxc::shared_treiber_stack::StackError;
use subetha_cxc::shared_string_arena::{ArenaError, SharedStringArena, StringRef};
use subetha_cxc::raw_vec::RawVec;
use subetha_cxc::shared_btree_map::BTreeError;
use subetha_cxc::shared_arc::{LastHolder, SharedArcDyn};
use subetha_cxc::shared_bit_vec::SharedBitVec;
use subetha_cxc::shared_bloom_filter::SharedBloomFilter;
use subetha_cxc::cross_process_notifier::{
    Notifier as SubethaNotifier, NotifierSet as SubethaNotifierSet,
};
use subetha_cxc::epoch_barrier::{BarrierError, EpochBarrier as SubethaEpochBarrier};
use subetha_cxc::heartbeat::HeartbeatTable;
use subetha_cxc::shared_condvar::{CondvarError, SharedCondvar};
use subetha_cxc::shared_epochs::SharedEpochs;
use subetha_cxc::shared_count_min_sketch::SharedCountMinSketch;
use subetha_cxc::shared_fence_clock::{Hlc, SharedFenceClock};
use subetha_cxc::shared_once_cell::SharedOnceCellDyn;
use subetha_cxc::shared_holder_table::SharedHolderTable;
use subetha_cxc::shared_leader_election::SharedLeaderElection;
use subetha_cxc::shared_hyper_log_log::{
    SharedHyperLogLog, MAX_PRECISION as HLL_MAX_PRECISION, MIN_PRECISION as HLL_MIN_PRECISION,
};
use subetha_cxc::shared_broadcast_ring::{SharedBroadcastRing, BROADCAST_PAYLOAD_BYTES};
use subetha_cxc::shared_histogram::SharedHistogram;
use subetha_cxc::shared_rate_limiter::{RateLimiterError, SharedRateLimiter};
use subetha_cxc::shared_rw_lock::{RWLockError, SharedRWLock};
use subetha_cxc::shared_semaphore::{SemaphoreError, SharedSemaphore};
use subetha_cxc::shared_hash_map::{InsertOutcome, MapError};
use subetha_cxc::shared_vec::VecError;
use subetha_cxc::shared_ring::{Consumer, Producer, RingError, SharedRingSpsc};
use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

/// Map a memory ordering named in Python onto the Rust one. Names rather
/// than integers, so a caller reads what it asked for.
fn ordering(name: &str) -> PyResult<Ordering> {
    match name {
        "relaxed" => Ok(Ordering::Relaxed),
        "acquire" => Ok(Ordering::Acquire),
        "release" => Ok(Ordering::Release),
        "acq_rel" => Ok(Ordering::AcqRel),
        "seq_cst" => Ok(Ordering::SeqCst),
        other => Err(PyValueError::new_err(format!(
            "{other} is not a memory ordering; use relaxed, acquire, release, acq_rel or seq_cst"
        ))),
    }
}

fn os_err(what: &str, e: impl std::fmt::Debug) -> PyErr {
    PyOSError::new_err(format!("{what}: {e:?}"))
}

/// A 64-bit integer in a file every process maps.
///
/// The single-call surface, and the one to measure against: a `load` here
/// is the same work the C ABI does in about 7 ns, so what this costs over
/// that is what Python costs.
#[pyclass(module = "subetha")]
struct Atomic {
    inner: Box<SharedAtomicU64>,
}

#[pymethods]
impl Atomic {
    /// Obtain the atomic at `path`, creating it holding `init` when the
    /// file does not exist and attaching to its live value when it does.
    #[new]
    #[pyo3(signature = (path, init = 0))]
    fn new(path: &str, init: u64) -> PyResult<Self> {
        SharedAtomicU64::create(path, init)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the atomic", e))
    }

    /// Attach to an atomic that already exists, leaving its value alone.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        SharedAtomicU64::open(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the atomic", e))
    }

    /// Read the value.
    ///
    /// `order` names how this read is ordered against the rest of the
    /// calling thread's work: `relaxed`, `acquire`, `release`, `acq_rel`
    /// or `seq_cst`. Any other name is a `ValueError`.
    #[pyo3(signature = (order = "seq_cst"))]
    fn load(&self, order: &str) -> PyResult<u64> {
        Ok(self.inner.load(ordering(order)?))
    }

    /// Write the value, discarding whatever was there without reading it.
    /// Use `swap` when the previous value matters, or `compare_exchange`
    /// when the write should land only if nobody else got there first.
    #[pyo3(signature = (value, order = "seq_cst"))]
    fn store(&self, value: u64, order: &str) -> PyResult<()> {
        self.inner.store(value, ordering(order)?);
        Ok(())
    }

    /// Add, and answer what the value was before. Adding past what a
    /// sixty-four bit number holds wraps round, as it does in Rust and
    /// in C.
    #[pyo3(signature = (value = 1, order = "seq_cst"))]
    fn fetch_add(&self, value: u64, order: &str) -> PyResult<u64> {
        Ok(self.inner.fetch_add(value, ordering(order)?))
    }

    /// Subtract, and answer what the value was before. Taking more than
    /// the value holds wraps round to the top, which is what a counter
    /// going below zero means here.
    #[pyo3(signature = (value = 1, order = "seq_cst"))]
    fn fetch_sub(&self, value: u64, order: &str) -> PyResult<u64> {
        Ok(self.inner.fetch_sub(value, ordering(order)?))
    }

    /// Set every bit that is set in `value`, and answer what the value
    /// was before.
    #[pyo3(signature = (value, order = "seq_cst"))]
    fn fetch_or(&self, value: u64, order: &str) -> PyResult<u64> {
        Ok(self.inner.fetch_or(value, ordering(order)?))
    }

    /// Clear every bit that is not set in `value`, and answer what the
    /// value was before.
    #[pyo3(signature = (value, order = "seq_cst"))]
    fn fetch_and(&self, value: u64, order: &str) -> PyResult<u64> {
        Ok(self.inner.fetch_and(value, ordering(order)?))
    }

    /// Flip every bit that is set in `value`, and answer what the value
    /// was before.
    #[pyo3(signature = (value, order = "seq_cst"))]
    fn fetch_xor(&self, value: u64, order: &str) -> PyResult<u64> {
        Ok(self.inner.fetch_xor(value, ordering(order)?))
    }

    /// Put `value` in and answer what was there, in one step nothing
    /// else can get between.
    #[pyo3(signature = (value, order = "seq_cst"))]
    fn swap(&self, value: u64, order: &str) -> PyResult<u64> {
        Ok(self.inner.swap(value, ordering(order)?))
    }

    /// Put `new` in only if the value is still `expected`, and answer
    /// what was there either way.
    ///
    /// The answer being `expected` is how a caller knows it won. This is
    /// the one operation that lets several processes agree on who
    /// changes something, and a caller that needs to retry loops on it.
    #[pyo3(signature = (expected, new, order = "seq_cst"))]
    fn compare_exchange(&self, expected: u64, new: u64, order: &str) -> PyResult<u64> {
        let ord = ordering(order)?;
        match self.inner.compare_exchange(expected, new, ord, ordering("acquire")?) {
            Ok(won) => Ok(won),
            // A refusal is an ordinary answer here, and the value it
            // carries is the whole point: it is what the caller compares
            // against next time round.
            Err(found) => Ok(found),
        }
    }

    /// Add `value` `count` times and return the value before the run.
    ///
    /// The batch shape, present so the cost of one Python call can be
    /// spread over many operations rather than paid on each.
    #[pyo3(signature = (count, value = 1, order = "seq_cst"))]
    fn fetch_add_many(&self, count: usize, value: u64, order: &str) -> PyResult<u64> {
        let ord = ordering(order)?;
        let mut first = 0;
        for i in 0..count {
            let seen = self.inner.fetch_add(value, ord);
            if i == 0 {
                first = seen;
            }
        }
        Ok(first)
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The mapping goes when the last reference to this object
    /// goes, not at the end of the block, and the file outlives the
    /// process either way.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!("<subetha.Atomic value={}>", self.inner.load(Ordering::Relaxed))
    }
}

/// A fixed-capacity arena of equal-sized slots in a file every process
/// maps, exposed to Python as a buffer.
///
/// This is the shape the measurement argues for: `memoryview(region)` is
/// a view over the mapping itself, so numpy or anything else reading it
/// makes no call into Rust per element and copies nothing.
#[pyclass(module = "subetha")]
struct Region {
    inner: Box<RawRegion>,
    /// Bytes from the first slot to the end of the last, which is what
    /// the buffer covers. Taken from the region's own addresses rather
    /// than assumed, so a slot geometry wider than the element is right.
    span: usize,
    /// Live buffer exports. Python may take several views, and the
    /// mapping must outlive all of them.
    exports: usize,
}

#[pymethods]
impl Region {
    /// Obtain the region at `path` holding `capacity` slots of
    /// `slot_size` bytes, creating it when the file does not exist.
    #[new]
    #[pyo3(signature = (path, capacity, slot_size, alignment = 1, tag = 0))]
    fn new(path: &str, capacity: usize, slot_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size, alignment, tag };
        let inner = Box::new(
            RawRegion::create(path, capacity, layout).map_err(|e| os_err("opening the region", e))?,
        );
        let span = Self::measure_span(&inner, capacity, slot_size)?;
        Ok(Self { inner, span, exports: 0 })
    }

    /// How many slots the region holds.
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// How many slots are allocated.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Take a slot and write `value` into it, returning its index.
    fn allocate(&self, value: &[u8]) -> PyResult<u32> {
        self.inner
            .allocate(value)
            .map_err(|e| os_err("allocating a slot", e))
    }

    /// Read slot `index`.
    fn get(&self, index: u32) -> PyResult<Vec<u8>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner
            .get(index, &mut out)
            .map_err(|e| os_err("reading a slot", e))?;
        Ok(out)
    }

    /// Write slot `index`.
    fn set(&self, index: u32, value: &[u8]) -> PyResult<()> {
        self.inner
            .set(index, value)
            .map_err(|e| os_err("writing a slot", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. A `memoryview` taken inside the block stays valid after
    /// it, because the view holds its own reference to the region and the
    /// export count is what keeps the mapping under it.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Region capacity={} slot_size={} bytes={}>",
            self.inner.capacity(),
            self.inner.layout().slot_size,
            self.span
        )
    }

    /// Hand Python a view over the mapping itself. The region must outlive
    /// every view, which the export count enforces.
    unsafe fn __getbuffer__(
        mut slf: PyRefMut<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: std::os::raw::c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyValueError::new_err("a buffer request with no view"));
        }
        let base = slf
            .inner
            .element_ptr(0)
            .map_err(|e| os_err("addressing the first slot", e))?;
        let readonly = if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE { 0 } else { 1 };
        let span = slf.span;
        // Counted before the reference is handed to the view, which
        // consumes it.
        slf.exports += 1;
        unsafe {
            (*view).buf = base as *mut std::os::raw::c_void;
            (*view).len = span as ffi::Py_ssize_t;
            (*view).readonly = readonly;
            (*view).itemsize = 1;
            (*view).format = if (flags & ffi::PyBUF_FORMAT) == ffi::PyBUF_FORMAT {
                c"B".as_ptr() as *mut std::os::raw::c_char
            } else {
                std::ptr::null_mut()
            };
            (*view).ndim = 1;
            (*view).shape = std::ptr::null_mut();
            (*view).strides = std::ptr::null_mut();
            (*view).suboffsets = std::ptr::null_mut();
            (*view).internal = std::ptr::null_mut();
            // The view owns a reference to the region for as long as it
            // lives, which is what keeps the mapping under it.
            (*view).obj = slf.into_ptr();
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(mut slf: PyRefMut<'_, Self>, _view: *mut ffi::Py_buffer) {
        slf.exports = slf.exports.saturating_sub(1);
    }
}

impl Region {
    /// Bytes from the first slot to the end of the last, read from the
    /// region's own slot addresses so a stride wider than the element is
    /// accounted for rather than assumed away.
    fn measure_span(region: &RawRegion, capacity: usize, slot_size: usize) -> PyResult<usize> {
        if capacity == 0 {
            return Ok(0);
        }
        let first = region
            .element_ptr(0)
            .map_err(|e| os_err("addressing the first slot", e))? as usize;
        let last = region
            .element_ptr((capacity - 1) as u32)
            .map_err(|e| os_err("addressing the last slot", e))? as usize;
        Ok(last + slot_size - first)
    }
}

/// A single-producer single-consumer ring in a file two processes map.
///
/// Slots are a fixed 64 bytes. `push` and `pop` are the single-item
/// calls; `push_many` and `pop_many` carry a run of items across one
/// boundary crossing, and `push_buffer` carries them without building a
/// Python object per item at all, which is the fastest shape here.
#[pyclass(module = "subetha")]
struct SpscRing {
    inner: Box<SpscRingCore>,
}

#[pymethods]
impl SpscRing {
    /// Obtain the ring at `path` holding `capacity` slots, creating it
    /// when the file does not exist.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        SpscRingCore::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the ring", e))
    }

    /// Attach to a ring that already exists. `capacity` must be the one
    /// it was created with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SpscRingCore::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the ring", e))
    }

    /// How many slots the ring holds.
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// The bytes one slot carries. A longer item is refused.
    #[getter]
    fn payload_size(&self) -> usize {
        SPSC_PAYLOAD_BYTES
    }

    /// Push one item. `False` means the ring was full, which is an
    /// answer rather than a failure; anything else raises.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("pushing", e)),
        }
    }

    /// Pop one item, or `None` when the ring is empty.
    fn pop(&self) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.inner.try_pop(&mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("popping", e)),
        }
    }

    /// Push a run of items, stopping at the first the ring refuses.
    /// Returns how many went in, so the caller keeps the rest.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            match self.inner.try_push(item) {
                Ok(()) => pushed += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// Push items packed end to end in one buffer, `item_len` bytes
    /// each. No Python object is built per item, which is what makes
    /// this the fastest way in: the caller's own array goes straight
    /// across.
    fn push_buffer(&self, data: &[u8], item_len: usize) -> PyResult<usize> {
        if item_len == 0 {
            return Err(PyValueError::new_err("item_len must not be zero"));
        }
        let mut pushed = 0;
        for chunk in data.chunks(item_len) {
            match self.inner.try_push(chunk) {
                Ok(()) => pushed += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// Pop up to `max_items`, stopping when the ring runs empty.
    fn pop_many(&self, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_pop(&mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("popping", e)),
            }
        }
        Ok(taken)
    }

    /// Pop up to `max_items` into one buffer packed end to end, and
    /// return how many were written. Nothing is allocated per item.
    fn pop_buffer(&self, max_items: usize) -> PyResult<(Vec<u8>, usize)> {
        let mut packed = vec![0u8; max_items * SPSC_PAYLOAD_BYTES];
        let mut taken = 0;
        for i in 0..max_items {
            let at = i * SPSC_PAYLOAD_BYTES;
            let slice = &mut packed[at..at + SPSC_PAYLOAD_BYTES];
            match self.inner.try_pop(slice) {
                Ok(_) => taken += 1,
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("popping", e)),
            }
        }
        packed.truncate(taken * SPSC_PAYLOAD_BYTES);
        Ok((packed, taken))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Nothing is drained: items still in the ring stay there
    /// for whoever attaches next, because the file outlives the process.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!("<subetha.SpscRing capacity={}>", self.inner.capacity())
    }
}

/// A broadcast ring: one producer, many consumers, each reading every
/// item at its own pace.
///
/// A consumer registers to get its own position, and reads with that.
#[pyclass(module = "subetha")]
struct BroadcastRing {
    inner: Box<SharedBroadcastRing>,
}

#[pymethods]
impl BroadcastRing {
    /// Obtain the broadcast ring at `path` holding `capacity` slots,
    /// creating it when the file does not exist and attaching to what is
    /// already there when it does.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        SharedBroadcastRing::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the broadcast ring", e))
    }

    /// Attach to a broadcast ring that already exists, raising `OSError`
    /// when it does not. `capacity` must be the one it was created with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedBroadcastRing::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the broadcast ring", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn payload_size(&self) -> usize {
        BROADCAST_PAYLOAD_BYTES
    }

    /// Take a consumer position. Every registered consumer sees every
    /// item published after it registered.
    fn register_consumer(&self) -> PyResult<usize> {
        self.inner
            .register_consumer()
            .map_err(|e| os_err("registering a consumer", e))
    }

    /// Give up a consumer position, so the producer stops holding slots
    /// for it.
    ///
    /// This matters more than it looks: a consumer that goes away without
    /// calling it keeps its position registered, and once the producer is
    /// a full lap ahead of that stalled position every `push` answers
    /// `False` forever. A position outside the range is ignored rather
    /// than raising.
    fn unregister_consumer(&self, consumer: usize) {
        self.inner.unregister_consumer(consumer);
    }

    /// How many items are waiting for a consumer, or `None` when the id
    /// names no live consumer of this ring.
    ///
    /// `None` covers an id past the consumer table and an id that has
    /// been given back, because a slot nobody holds keeps the cursor its
    /// last holder left and the distance to the producer is then a
    /// number about nobody.
    ///
    /// Zero is not the same as "nothing was lost". A consumer starts at
    /// the head, so one registered after a burst was published is
    /// caught up by this measure and saw none of it. Read
    /// `producer_position` at the moment you register to learn how much
    /// you will never see.
    fn lag(&self, consumer: usize) -> Option<u64> {
        self.inner.try_lag(consumer)
    }

    #[getter]
    fn producer_position(&self) -> u64 {
        self.inner.producer_position()
    }

    #[getter]
    fn active_consumers(&self) -> usize {
        self.inner.active_consumer_count()
    }

    /// Wait until `want` consumers have registered, and answer how many
    /// there are when the wait ends.
    ///
    /// An answer below `want` means the timeout ran out first, and the
    /// number says how many of the readers you are about to publish for
    /// actually arrived.
    ///
    /// This is what a producer does about the loss `lag` cannot report.
    /// A consumer starts at the head, so everything published before it
    /// registered is lost to it and nothing says so; across processes
    /// the window is however long starting a worker takes. Publishing
    /// only once the readers are here closes it.
    ///
    /// It promises nothing about afterwards. A consumer counted here can
    /// unregister, or its process can die, the moment this returns. It
    /// is a starting gun, not a register of attendance.
    ///
    /// The interpreter is detached while waiting, so other threads run.
    #[pyo3(signature = (want, timeout = 5.0))]
    fn wait_for_consumers(&self, py: Python<'_>, want: usize, timeout: f64) -> PyResult<usize> {
        if timeout.is_nan() || timeout <= 0.0 {
            return Err(PyValueError::new_err("the timeout must be positive"));
        }
        let ring: &SharedBroadcastRing = &self.inner;
        let wait = std::time::Duration::from_secs_f64(timeout);
        Ok(py.detach(|| ring.wait_for_consumers(want, wait)))
    }

    /// Publish one item, which every registered consumer will see.
    ///
    /// `False` means the slowest registered consumer is a full lap
    /// behind, so the slot about to be overwritten still holds something
    /// it has not read. That is an answer rather than a failure. A ring
    /// nobody has registered against never fills, because a broadcast to
    /// nobody has nothing to hold back for.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(e) if is_broadcast_full(&e) => Ok(false),
            Err(e) => Err(os_err("publishing", e)),
        }
    }

    /// Publish a run of items, stopping at the first that will not fit,
    /// and answer how many landed. A count below what was handed in means
    /// the rest were not published and are still the caller's to keep.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            match self.inner.try_push(item) {
                Ok(()) => pushed += 1,
                Err(e) if is_broadcast_full(&e) => break,
                Err(e) => return Err(os_err("publishing", e)),
            }
        }
        Ok(pushed)
    }

    /// Read the next item for `consumer`, or `None` when it has caught
    /// up with the producer.
    fn recv(&self, consumer: usize) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; BROADCAST_PAYLOAD_BYTES];
        match self.inner.try_recv(consumer, &mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(e) if is_broadcast_empty(&e) => Ok(None),
            Err(e) => Err(os_err("receiving", e)),
        }
    }

    /// Read up to `max_items` for `consumer` in one call.
    fn recv_many(&self, consumer: usize, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; BROADCAST_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_recv(consumer, &mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(e) if is_broadcast_empty(&e) => break,
                Err(e) => return Err(os_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. A consumer position taken inside the block is still
    /// registered after it: give it up with `unregister_consumer` rather
    /// than relying on the block to do it.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.BroadcastRing capacity={} consumers={}>",
            self.inner.capacity(),
            self.inner.active_consumer_count()
        )
    }
}

/// One fixed-size value in a file every process maps, with a version
/// that steps on each write so a reader can tell a change from a repeat.
#[pyclass(module = "subetha")]
struct Cell {
    inner: Box<RawCell>,
}

#[pymethods]
impl Cell {
    /// Obtain the cell at `path` holding `value_size` bytes, creating it
    /// when the file does not exist and attaching to the value already
    /// there when it does.
    #[new]
    fn new(path: &str, value_size: usize) -> PyResult<Self> {
        RawCell::create(path, value_size)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the cell", e))
    }

    /// Attach to a cell that already exists, raising `OSError` when it
    /// does not. `value_size` must be the one it was created with,
    /// because it is part of the layout rather than a hint.
    #[staticmethod]
    fn open(path: &str, value_size: usize) -> PyResult<Self> {
        RawCell::open(path, value_size)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the cell", e))
    }

    #[getter]
    fn value_size(&self) -> usize {
        self.inner.value_size()
    }

    /// Steps on every write. A reader that sees the same version twice
    /// saw no write between.
    #[getter]
    fn version(&self) -> u32 {
        self.inner.version()
    }

    /// Read the value, always `value_size` bytes.
    ///
    /// Read `version` either side when it matters whether what you got
    /// was written since you last looked: the same version twice means
    /// nothing was written between.
    fn get(&self) -> PyResult<Vec<u8>> {
        let mut out = vec![0u8; self.inner.value_size()];
        self.inner
            .get(&mut out)
            .map_err(|e| os_err("reading the cell", e))?;
        Ok(out)
    }

    /// Replace the value and step `version`, so a reader watching that
    /// number sees this write happened.
    fn set(&self, value: &[u8]) -> PyResult<()> {
        self.inner
            .set(value)
            .map_err(|e| os_err("writing the cell", e))
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it.
    ///
    /// Another process mapping the same file sees a write without this.
    /// Flushing is about what survives the machine stopping, not about
    /// what other processes can see.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing the cell", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Nothing is flushed on the way out: call `flush` where
    /// durability matters.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Cell value_size={} version={}>",
            self.inner.value_size(),
            self.inner.version()
        )
    }
}

/// A growable-to-capacity sequence of equal-sized elements in a mapped
/// file.
///
/// # Why this has no buffer and `Region` does
///
/// Each element here sits under a seqlock: the slot carries a version,
/// a writer steps it either side of the copy, and a reader retries while
/// it is odd or has moved. Handing Python a `memoryview` over the span
/// would hand it a pointer past that protocol, and a read racing a write
/// would return a torn element rather than retrying. So the bulk path
/// here is `read_range`, which crosses the boundary once and does the
/// reading in Rust, where the seqlock is honored. A `Region` has no such
/// protocol over its slots, which is why a view of one is sound.
#[pyclass(module = "subetha", name = "Vec")]
struct Vec_ {
    inner: Box<RawVec>,
}

#[pymethods]
impl Vec_ {
    /// Obtain the vec at `path` holding up to `capacity` elements of
    /// `element_size` bytes, creating it when the file does not exist and
    /// attaching to what is already there when it does.
    ///
    /// The capacity is fixed at creation. This grows up to it and never
    /// past it, which is what lets every process work out where an
    /// element lives without asking anyone. `alignment` and `tag` are
    /// part of the layout written into the file, so attaching to an
    /// existing vec means matching them rather than requesting them.
    #[new]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn new(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        let inner = Box::new(
            RawVec::create(path, capacity, layout).map_err(|e| os_err("opening the vec", e))?,
        );
        Ok(Self { inner })
    }

    /// Attach to a vec that already exists, raising `OSError` when it
    /// does not. Every layout argument must be the one it was created
    /// with: they describe the file rather than asking anything of it.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn open(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        let inner = Box::new(
            RawVec::open(path, capacity, layout).map_err(|e| os_err("attaching to the vec", e))?,
        );
        Ok(Self { inner })
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn element_size(&self) -> usize {
        self.inner.layout().slot_size
    }

    #[getter]
    fn writable(&self) -> bool {
        self.inner.is_writable()
    }

    /// How many elements are live, which is not the capacity. A vec that
    /// has never been pushed to is empty, and `bool(vec)` is `False`.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Append one element, or `None` when the vec is full.
    fn push(&self, value: &[u8]) -> PyResult<Option<usize>> {
        match self.inner.push_back(value) {
            Ok(index) => Ok(Some(index)),
            Err(VecError::Full) => Ok(None),
            Err(e) => Err(os_err("appending", e)),
        }
    }

    /// Append a run of elements, stopping at the first refusal, and say
    /// how many landed.
    fn push_many(&self, values: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for value in &values {
            match self.inner.push_back(value) {
                Ok(_) => pushed += 1,
                Err(VecError::Full) => break,
                Err(e) => return Err(os_err("appending", e)),
            }
        }
        Ok(pushed)
    }

    /// Take the last element, or `None` when the vec is empty.
    fn pop(&self) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.pop_back(&mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("popping", e)),
        }
    }

    /// Read element `index`, or `None` when it is past what is live.
    ///
    /// The read goes through the slot's seqlock, so a writer racing it
    /// loses to a retry rather than handing back a torn element. Use
    /// `read_range` for a run: it crosses the boundary once instead of
    /// once per element.
    fn get(&self, index: usize) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.get(index, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("reading", e)),
        }
    }

    /// Overwrite element `index`, stepping its seqlock either side so a
    /// reader racing this retries instead of seeing half of each value.
    ///
    /// The index must be below the length, not merely below the capacity:
    /// this writes over an element that is already there and cannot
    /// append. An index past what is live is an `OSError`; `push` is what
    /// adds.
    fn set(&self, index: usize, value: &[u8]) -> PyResult<()> {
        self.inner
            .set(index, value)
            .map_err(|e| os_err("writing", e))
    }

    /// Drop every element, so the length goes to zero. The capacity and
    /// the file are left as they are, and the space is reused by the next
    /// `push`.
    fn clear(&self) -> PyResult<()> {
        self.inner.clear().map_err(|e| os_err("clearing", e))
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees a
    /// write without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The elements stay where they are for whoever attaches
    /// next; `clear` is what empties a vec, not the end of a block.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Vec len={} capacity={} element_size={}>",
            self.inner.len(),
            self.inner.capacity(),
            self.inner.layout().slot_size
        )
    }

    /// Read `count` elements from `start`, packed end to end into one
    /// object. Stops at the end of what is live.
    ///
    /// This is the bulk path: one crossing, no Python object per
    /// element, and each element read through the seqlock, so a writer
    /// racing this loses to a retry rather than handing over a torn
    /// element.
    fn read_range(&self, start: usize, count: usize) -> PyResult<Vec<u8>> {
        let size = self.inner.layout().slot_size;
        let mut packed = vec![0u8; count * size];
        let mut taken = 0;
        for i in 0..count {
            let at = taken * size;
            match self.inner.get(start + i, &mut packed[at..at + size]) {
                Ok(true) => taken += 1,
                Ok(false) => break,
                Err(e) => return Err(os_err("reading a range", e)),
            }
        }
        packed.truncate(taken * size);
        Ok(packed)
    }

    /// Write elements packed end to end into consecutive slots from
    /// `start`, and return how many landed.
    fn write_range(&self, start: usize, data: &[u8]) -> PyResult<usize> {
        let size = self.inner.layout().slot_size;
        if size == 0 || !data.len().is_multiple_of(size) {
            return Err(PyValueError::new_err(
                "the data must be a whole number of elements",
            ));
        }
        let mut written = 0;
        for (i, chunk) in data.chunks(size).enumerate() {
            match self.inner.set(start + i, chunk) {
                Ok(()) => written += 1,
                Err(VecError::OutOfBounds) => break,
                Err(e) => return Err(os_err("writing a range", e)),
            }
        }
        Ok(written)
    }
}

/// A set of notifiers on one file: any process can signal it, and every
/// attached notifier wakes.
#[pyclass(module = "subetha")]
struct NotifierSet {
    inner: Box<SubethaNotifierSet>,
}

#[pymethods]
impl NotifierSet {
    /// Obtain the notifier set at `path`, creating it when the file does
    /// not exist. Attaching does not by itself give this process a
    /// notifier: call `attach` for that.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        SubethaNotifierSet::file(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the notifier set", e))
    }

    /// How many notifiers are attached.
    #[getter]
    fn attached(&self) -> u32 {
        self.inner.attached()
    }

    /// Wake every attached notifier, and say how many were signaled.
    fn signal(&self) -> usize {
        self.inner.signal()
    }

    /// Attach a notifier of this process's own.
    fn attach(&self) -> PyResult<Notifier> {
        self.inner
            .attach()
            .map(|inner| Notifier { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching a notifier", e))
    }

    fn __repr__(&self) -> String {
        format!("<subetha.NotifierSet {} attached>", self.inner.attached())
    }
}

/// One process's end of a notifier set: it waits, and a signal from any
/// process wakes it.
///
/// # Reaching an event loop
///
/// `subetha.aio.wait` is the awaitable form and works on every platform.
/// What it costs differs, because `native` hands out a different object
/// on each: a file descriptor on Unix, which `asyncio`'s
/// `loop.add_reader` takes, so waiting occupies nothing; and an event
/// `HANDLE` on Windows, which nothing in asyncio's public surface can
/// watch, so the wait runs on a worker thread there.
///
/// Reach for `native` only to hand the notifier to something else that
/// knows what to do with a descriptor. `wait` blocks, and
/// `subetha.aio.wait` does not.
#[pyclass(module = "subetha")]
struct Notifier {
    inner: Box<SubethaNotifier>,
}

#[pymethods]
impl Notifier {
    /// This notifier's place in the set.
    #[getter]
    fn index(&self) -> u32 {
        self.inner.index()
    }

    /// The native object as an integer: a file descriptor on Unix, an
    /// event `HANDLE` on Windows. See the class note before handing it
    /// to an event loop.
    #[getter]
    fn native(&self) -> u64 {
        self.inner.native()
    }

    /// Whether a signal is pending right now.
    #[getter]
    fn is_signaled(&self) -> bool {
        self.inner.is_signaled()
    }

    /// Wait for a signal, up to `timeout` seconds, and say whether one
    /// arrived. The interpreter is detached while waiting, so other
    /// Python threads keep running.
    #[pyo3(signature = (timeout = None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<bool> {
        let timeout_ms = match timeout {
            None => -1,
            Some(t) if t >= 0.0 => {
                let ms = (t * 1000.0).round();
                if ms > i32::MAX as f64 {
                    i32::MAX
                } else {
                    ms as i32
                }
            }
            Some(_) => {
                return Err(PyValueError::new_err("the timeout must not be negative"));
            }
        };
        let notifier: &SubethaNotifier = &self.inner;
        Ok(py.detach(|| notifier.wait(timeout_ms)))
    }

    /// Clear a pending signal, so the next wait blocks rather than
    /// returning at once on a signal already consumed.
    fn drain(&self) {
        self.inner.drain();
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Notifier {} {}>",
            self.inner.index(),
            if self.inner.is_signaled() { "signaled" } else { "quiet" }
        )
    }
}

/// A value in a mapped file that several processes hold at once, counted
/// like a reference count: the file goes when the last holder does, or
/// stays for a later process, depending on `on_last`.
#[pyclass(module = "subetha")]
struct SharedArc {
    inner: Box<SharedArcDyn>,
}

#[pymethods]
impl SharedArc {
    /// The constructor asserts at least one holder, so that is refused
    /// here first. `keep_on_last` decides what happens when the last
    /// holder lets go: keep the file for a later process, or unlink it.
    #[new]
    #[pyo3(signature = (path, value, max_holders = 16, keep_on_last = false))]
    fn new(path: &str, value: &[u8], max_holders: usize, keep_on_last: bool) -> PyResult<Self> {
        if max_holders < 1 {
            return Err(PyValueError::new_err("max_holders must be at least one"));
        }
        SharedArcDyn::create(path, value, max_holders, Self::policy(keep_on_last))
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the shared value", e))
    }

    /// Attach to a shared value that already exists, counting this
    /// process as another holder.
    ///
    /// `value_bytes` must be the size it was created with. `keep_on_last`
    /// is this holder's own answer to what should become of the file once
    /// the last holder lets go. A `max_holders` below one is a
    /// `ValueError`, and a file that is not there is an `OSError`.
    #[staticmethod]
    #[pyo3(signature = (path, value_bytes, max_holders = 16, keep_on_last = false))]
    fn open(path: &str, value_bytes: usize, max_holders: usize, keep_on_last: bool) -> PyResult<Self> {
        if max_holders < 1 {
            return Err(PyValueError::new_err("max_holders must be at least one"));
        }
        SharedArcDyn::open(path, value_bytes, max_holders, Self::policy(keep_on_last))
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the shared value", e))
    }

    /// How many processes hold it right now.
    #[getter]
    fn holders(&self) -> usize {
        self.inner.strong_count()
    }

    #[getter]
    fn value_size(&self) -> usize {
        self.inner.value_len()
    }

    /// The whole value.
    fn get(&self) -> Vec<u8> {
        self.inner.as_slice().to_vec()
    }

    /// Part of the value, for a caller that wants a field rather than
    /// the whole of a large record.
    fn read_at(&self, offset: usize, length: usize) -> PyResult<Vec<u8>> {
        let mut out = vec![0u8; length];
        self.inner
            .read_at(offset, &mut out)
            .map_err(|e| os_err("reading", e))?;
        Ok(out)
    }

    /// Overwrite as many bytes as `value` carries, starting at `offset`.
    /// A range running past the end of the value is an `OSError` rather
    /// than a short write. The partner of `read_at`, for a caller that
    /// wants one field of a large record rather than all of it.
    fn write_at(&self, offset: usize, value: &[u8]) -> PyResult<()> {
        self.inner
            .write_at(offset, value)
            .map_err(|e| os_err("writing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without letting go of the value, and let an
    /// exception through.
    ///
    /// This is worth knowing here more than elsewhere: the holder count
    /// falls when the last reference to this object is collected, not at
    /// the end of the block, so a `with` statement does not decide when
    /// the file goes. Drop the name to let go of it.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.SharedArc {} bytes, {} holders>",
            self.inner.value_len(),
            self.inner.strong_count()
        )
    }
}

impl SharedArc {
    fn policy(keep_on_last: bool) -> LastHolder {
        if keep_on_last {
            LastHolder::Keep
        } else {
            LastHolder::Unlink
        }
    }
}

/// Leader election in a mapped file: exactly one process holds the role,
/// and it is taken back if the holder stops beating.
#[pyclass(module = "subetha")]
struct LeaderElection {
    inner: Box<SharedLeaderElection>,
}

#[pymethods]
impl LeaderElection {
    /// Obtain the election at `path`, creating it when the file does not
    /// exist. Creating one claims nothing: `try_claim` is what takes the
    /// role.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        SharedLeaderElection::create(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the election", e))
    }

    /// Attach to an election that already exists, raising `OSError` when
    /// it does not.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        SharedLeaderElection::open(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the election", e))
    }

    /// Try to take the role. `True` means this process now holds it and
    /// must keep beating; `False` means someone else holds it and is
    /// still alive.
    #[pyo3(signature = (pid = None, grace_epochs = 3))]
    fn try_claim(&self, pid: Option<u32>, grace_epochs: u64) -> bool {
        self.inner
            .try_claim_leadership(pid.unwrap_or_else(std::process::id), grace_epochs)
    }

    /// Say the leader is still alive. `False` means this process is not
    /// the leader any more, which is the answer a former leader needs.
    #[pyo3(signature = (pid = None))]
    fn beat(&self, pid: Option<u32>) -> bool {
        self.inner.beat_as_leader(pid.unwrap_or_else(std::process::id))
    }

    /// Give the role up so another process can take it without waiting
    /// out the grace period.
    #[pyo3(signature = (pid = None))]
    fn step_down(&self, pid: Option<u32>) -> bool {
        self.inner.step_down(pid.unwrap_or_else(std::process::id))
    }

    /// The pid holding the role, or `None` when nobody does.
    #[getter]
    fn leader(&self) -> Option<u32> {
        self.inner.current_leader()
    }

    /// Whether `pid` holds the role right now, defaulting to this
    /// process. This only asks. `beat` is what keeps a claim alive, and
    /// asking never renews one.
    #[pyo3(signature = (pid = None))]
    fn am_i_leader(&self, pid: Option<u32>) -> bool {
        self.inner.am_i_leader(pid.unwrap_or_else(std::process::id))
    }

    /// Steps each time the role changes hands, so a follower can tell a
    /// new leader from the same one.
    #[getter]
    fn term(&self) -> u32 {
        self.inner.election_term()
    }

    #[getter]
    fn global_epoch(&self) -> u64 {
        self.inner.global_epoch()
    }

    /// Step the global epoch and answer the new value, not the old one.
    ///
    /// The grace period a stalled leader is given is counted in epochs,
    /// so something has to tick them or a dead leader's claim never ages
    /// out. The leader usually does it once per scan.
    fn tick_epoch(&self) -> u64 {
        self.inner.tick_epoch()
    }

    fn __repr__(&self) -> String {
        match self.inner.current_leader() {
            Some(pid) => format!(
                "<subetha.LeaderElection leader={pid} term={}>",
                self.inner.election_term()
            ),
            None => "<subetha.LeaderElection no leader>".to_string(),
        }
    }
}

/// A table of slots each holding a 64-bit payload, taken and given back
/// by any process that maps it.
#[pyclass(module = "subetha")]
struct HolderTable {
    inner: Box<SharedHolderTable>,
}

#[pymethods]
impl HolderTable {
    /// Obtain the holder table at `path` with `capacity` slots, creating
    /// it when the file does not exist. A capacity of zero is a
    /// `ValueError`, because a table with no slots can serve nobody.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity == 0 {
            return Err(PyValueError::new_err("capacity must be at least one"));
        }
        SharedHolderTable::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the holder table", e))
    }

    /// Attach to a holder table that already exists, raising `OSError`
    /// when it does not. `capacity` must be the one it was created with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedHolderTable::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the holder table", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// How many slots are held right now.
    #[getter]
    fn live(&self) -> usize {
        self.inner.live()
    }

    /// Take a slot and put `payload` in it, or `None` when every slot is
    /// taken.
    fn claim(&self, payload: u64) -> Option<usize> {
        self.inner.claim(payload)
    }

    /// Take a slot without publishing anything into it yet.
    fn reserve(&self) -> Option<usize> {
        self.inner.reserve()
    }

    /// Put a payload in a slot this caller already holds.
    fn publish(&self, slot: usize, payload: u64) {
        self.inner.publish(slot, payload);
    }

    /// What a slot holds, or `None` when nobody holds it.
    fn payload(&self, slot: usize) -> Option<u64> {
        self.inner.payload(slot)
    }

    /// Give a slot back, so the next `claim` or `reserve` can hand it to
    /// somebody else. A process that exits without releasing leaves its
    /// slot held, and nothing here reclaims it.
    fn release(&self, slot: usize) {
        self.inner.release(slot);
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.HolderTable {} of {} held>",
            self.inner.live(),
            self.inner.capacity()
        )
    }
}

/// A table of live participants in a mapped file. Each beats its own
/// slot; a slot that stops beating is how everyone else learns the
/// process behind it is gone.
#[pyclass(module = "subetha", frozen)]
struct Heartbeat {
    /// Shared rather than owned, because an `EpochBarrier` holds the
    /// same table to count who is still alive.
    inner: Arc<HeartbeatTable>,
}

#[pymethods]
impl Heartbeat {
    /// Obtain the heartbeat table at `path` with `capacity` slots,
    /// creating it when the file does not exist. A capacity of zero is a
    /// `ValueError`.
    ///
    /// Creating the table registers nothing. Each participant takes its
    /// own slot with `register` and keeps it alive with `beat`.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity == 0 {
            return Err(PyValueError::new_err("capacity must be at least one"));
        }
        HeartbeatTable::create(path, capacity)
            .map(|inner| Self { inner: Arc::new(inner) })
            .map_err(|e| os_err("opening the heartbeat table", e))
    }

    /// Attach to a heartbeat table that already exists, raising `OSError`
    /// when it does not. `capacity` must be the one it was created with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        HeartbeatTable::open(path, capacity)
            .map(|inner| Self { inner: Arc::new(inner) })
            .map_err(|e| os_err("attaching to the heartbeat table", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Take a slot. A full table raises rather than answering `None`:
    /// it is a configuration that cannot serve this process, not an
    /// answer to a question.
    #[pyo3(signature = (pid = None))]
    fn register(&self, pid: Option<u32>) -> PyResult<usize> {
        self.inner
            .register(pid.unwrap_or_else(std::process::id))
            .map_err(|e| os_err("registering", e))
    }

    /// Give a slot back, so another process can register into it.
    ///
    /// A process that exits without this leaves its slot registered, and
    /// that is the mechanism rather than a leak: the slot stops beating,
    /// and once its last beat is further back than the grace period
    /// everyone else reads it as dead. Unregistering is the tidy exit
    /// that skips the wait.
    fn unregister(&self, slot: usize) {
        self.inner.unregister(slot);
    }

    /// Say this slot is still alive. A slot that stops beating for
    /// longer than the grace period counts as dead.
    fn beat(&self, slot: usize) {
        self.inner.beat(slot);
    }

    #[getter]
    fn global_epoch(&self) -> u64 {
        self.inner.global_epoch()
    }

    /// Step the global epoch and answer the new value, not the old one.
    ///
    /// Liveness here is measured in epochs rather than seconds, so a
    /// watchdog ticks this once per scan and a slot whose last beat is
    /// too many epochs back reads as dead. Nothing ticks on its own.
    fn tick_global_epoch(&self) -> u64 {
        self.inner.tick_global_epoch()
    }

    /// What a slot currently says about itself, or `None` when nothing
    /// holds it.
    fn snapshot<'py>(
        &self,
        slot: usize,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, pyo3::types::PyDict>>> {
        let Some(s) = self.inner.snapshot(slot) else {
            return Ok(None);
        };
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("pid", s.pid)?;
        dict.set_item("last_seen_epoch", s.last_seen_epoch)?;
        dict.set_item("in_flight", s.in_flight_bitmap)?;
        dict.set_item("role", s.role)?;
        Ok(Some(dict))
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Heartbeat capacity={} epoch={}>",
            self.inner.capacity(),
            self.inner.global_epoch()
        )
    }
}

/// A barrier every participant reaches before any of them goes on.
///
/// It counts live peers through a `Heartbeat` table, so a process that
/// dies while others wait stops being counted rather than holding the
/// barrier shut for ever.
#[pyclass(module = "subetha")]
struct EpochBarrier {
    inner: Box<SubethaEpochBarrier>,
}

#[pymethods]
impl EpochBarrier {
    /// Obtain the barrier at `path`, creating it when the file does not
    /// exist.
    ///
    /// It counts live peers through `heartbeat`, and `grace_epochs` is
    /// how many epochs a participant may go without beating before it
    /// stops being counted. That is what keeps a process dying mid-wait
    /// from holding the barrier shut for the others.
    #[new]
    #[pyo3(signature = (path, heartbeat, grace_epochs = 3))]
    fn new(path: &str, heartbeat: PyRef<'_, Heartbeat>, grace_epochs: u64) -> PyResult<Self> {
        SubethaEpochBarrier::create(path, Arc::clone(&heartbeat.inner), grace_epochs)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the barrier", e))
    }

    /// Attach to a barrier that already exists, raising `OSError` when it
    /// does not. The heartbeat table and the grace period are this
    /// participant's own, given here the same way they are at creation.
    #[staticmethod]
    #[pyo3(signature = (path, heartbeat, grace_epochs = 3))]
    fn open(path: &str, heartbeat: PyRef<'_, Heartbeat>, grace_epochs: u64) -> PyResult<Self> {
        SubethaEpochBarrier::open(path, Arc::clone(&heartbeat.inner), grace_epochs)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the barrier", e))
    }

    /// How many participants are still beating.
    #[getter]
    fn live_peers(&self) -> u32 {
        self.inner.live_peer_count()
    }

    #[getter]
    fn current_epoch(&self) -> u32 {
        self.inner.current_epoch()
    }

    #[getter]
    fn arrived(&self) -> u32 {
        self.inner.arrived_count()
    }

    /// The epoch and how many have arrived at it, read together so the
    /// two cannot disagree.
    fn snapshot(&self) -> (u32, u32) {
        self.inner.snapshot()
    }

    /// Arrive at `epoch` and wait for everyone else, or for `timeout`
    /// seconds. Returns `True` when the barrier opened and `False` on a
    /// timeout. The interpreter is detached while waiting.
    #[pyo3(signature = (epoch, timeout = None, quorum = None))]
    fn wait(
        &self,
        py: Python<'_>,
        epoch: u32,
        timeout: Option<f64>,
        quorum: Option<u32>,
    ) -> PyResult<bool> {
        if let Some(t) = timeout
            && (t.is_nan() || t <= 0.0)
        {
            return Err(PyValueError::new_err("the timeout must be positive"));
        }
        let barrier: &SubethaEpochBarrier = &self.inner;
        let outcome = py.detach(|| match (timeout, quorum) {
            (Some(t), Some(q)) => {
                barrier.wait_quorum_timeout(epoch, q, std::time::Duration::from_secs_f64(t))
            }
            (Some(t), None) => barrier.wait_timeout(epoch, std::time::Duration::from_secs_f64(t)),
            (None, Some(q)) => barrier.wait_quorum(epoch, q),
            (None, None) => barrier.wait(epoch),
        });
        match outcome {
            Ok(()) => Ok(true),
            Err(BarrierError::Timeout) => Ok(false),
            Err(e) => Err(os_err("waiting at the barrier", e)),
        }
    }

    fn __repr__(&self) -> String {
        let (epoch, arrived) = self.inner.snapshot();
        format!(
            "<subetha.EpochBarrier epoch={epoch} arrived={arrived} live={}>",
            self.inner.live_peer_count()
        )
    }
}

/// A condition variable shared between processes: a waiter parks until
/// another process says the thing it is waiting for has happened.
///
/// The predicate is a Python callable and is genuinely called from
/// inside the wait, three times per round: before parking, again after
/// the park slot is taken, and after each wake. That last pair is what
/// closes the gap where a notify lands between the check and the park,
/// so Python cannot own the loop without losing it. The interpreter is
/// detached while parked and re-attached for each check.
#[pyclass(module = "subetha")]
struct Condvar {
    inner: Box<SharedCondvar>,
}

#[pymethods]
impl Condvar {
    /// Obtain the condition at `path`, creating it when the file does not
    /// exist and attaching to the live one when it does.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        SharedCondvar::create(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the condition", e))
    }

    /// Attach to a condition that already exists, raising `OSError` when
    /// it does not.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        SharedCondvar::open(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the condition", e))
    }

    /// Steps on every notify. A waiter that sees the same value twice
    /// knows nothing was announced between the two readings.
    #[getter]
    fn generation(&self) -> u64 {
        self.inner.generation()
    }

    /// Wait until `predicate` is true, or until `timeout` seconds pass.
    ///
    /// Returns `True` when the predicate became true and `False` when
    /// the timeout ran out first. An exception raised by the predicate
    /// ends the wait and is re-raised here rather than being swallowed
    /// into a false answer.
    #[pyo3(signature = (predicate, timeout = None))]
    fn wait_for(&self, py: Python<'_>, predicate: Py<pyo3::PyAny>, timeout: Option<f64>) -> PyResult<bool> {
        if let Some(t) = timeout
            && (t.is_nan() || t <= 0.0)
        {
            return Err(PyValueError::new_err("the timeout must be positive"));
        }
        // A predicate that raises must not leave the waiter parked, so
        // its failure is recorded and the check answers true to end the
        // wait; the error is raised below rather than reported as a
        // satisfied condition.
        let failure: std::sync::Mutex<Option<PyErr>> = std::sync::Mutex::new(None);
        let condition: &SharedCondvar = &self.inner;
        let check = || {
            Python::attach(|inner_py| match predicate.call0(inner_py) {
                Ok(value) => match value.bind(inner_py).is_truthy() {
                    Ok(truth) => truth,
                    Err(e) => {
                        Self::record(&failure, e);
                        true
                    }
                },
                Err(e) => {
                    Self::record(&failure, e);
                    true
                }
            })
        };
        let outcome = py.detach(|| match timeout {
            Some(t) => condition.wait_timeout(check, std::time::Duration::from_secs_f64(t)),
            None => condition.wait(check),
        });
        if let Some(e) = Self::take(&failure) {
            return Err(e);
        }
        match outcome {
            Ok(()) => Ok(true),
            Err(CondvarError::Timeout) => Ok(false),
            Err(e) => Err(os_err("waiting on the condition", e)),
        }
    }

    /// Wake at most one waiter. The caller is responsible for having
    /// made the predicate true first, which is the contract every
    /// condition variable has.
    fn notify_one(&self) -> usize {
        self.inner.notify_one()
    }

    /// Wake every waiter.
    fn notify_all(&self) -> usize {
        self.inner.notify_all()
    }

    fn __repr__(&self) -> String {
        format!("<subetha.Condvar generation={}>", self.inner.generation())
    }
}

impl Condvar {
    /// Keep the first failure: a later one is a consequence of the wait
    /// already ending, and the first is the one that explains it.
    fn record(slot: &std::sync::Mutex<Option<PyErr>>, e: PyErr) {
        let mut held = slot
            .lock()
            .expect("the predicate's failure slot is never held across a panic");
        if held.is_none() {
            *held = Some(e);
        }
    }

    fn take(slot: &std::sync::Mutex<Option<PyErr>>) -> Option<PyErr> {
        slot.lock()
            .expect("the predicate's failure slot is never held across a panic")
            .take()
    }
}

/// A value computed once, by whichever process gets there first, and
/// read by every other.
///
/// The protocol has three steps rather than one, because "compute this
/// once across processes" cannot be a single call: a process `claim`s
/// the right to initialize, computes, and `publish`es. Everyone else
/// reads, or waits.
#[pyclass(module = "subetha")]
struct LazyValue {
    inner: Box<SharedOnceCellDyn>,
}

#[pymethods]
impl LazyValue {
    /// Obtain the lazy value at `path` holding `value_bytes` bytes,
    /// creating it when the file does not exist. Zero bytes is a
    /// `ValueError`.
    ///
    /// Creating it publishes nothing. The value arrives when some process
    /// wins `claim` and calls `publish`; everyone else reads `get` or
    /// blocks in `wait`.
    #[new]
    fn new(path: &str, value_bytes: usize) -> PyResult<Self> {
        if value_bytes == 0 {
            return Err(PyValueError::new_err("value_bytes must be at least one"));
        }
        SharedOnceCellDyn::create(path, value_bytes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the lazy value", e))
    }

    /// Attach to a lazy value that already exists, raising `OSError` when
    /// it does not. `value_bytes` must be the size it was created with.
    /// Attaching says nothing about whether anything has been published
    /// yet: `ready` answers that.
    #[staticmethod]
    fn open(path: &str, value_bytes: usize) -> PyResult<Self> {
        SharedOnceCellDyn::open(path, value_bytes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the lazy value", e))
    }

    #[getter]
    fn value_size(&self) -> usize {
        self.inner.value_len()
    }

    /// Whether a value has been published yet.
    #[getter]
    fn ready(&self) -> bool {
        let mut out = vec![0u8; self.inner.value_len()];
        self.inner.try_get(&mut out)
    }

    /// The value, or `None` when nobody has published one yet.
    fn get(&self) -> Option<Vec<u8>> {
        let mut out = vec![0u8; self.inner.value_len()];
        if self.inner.try_get(&mut out) {
            Some(out)
        } else {
            None
        }
    }

    /// Claim the right to compute the value. `True` means this caller
    /// won and must publish; `False` means someone else is doing it and
    /// this caller should wait.
    #[pyo3(signature = (pid = None))]
    fn claim(&self, pid: Option<u32>) -> bool {
        self.inner.claim(pid.unwrap_or_else(std::process::id))
    }

    /// Publish the value this caller claimed. `False` means it was not
    /// this caller's to publish.
    #[pyo3(signature = (value, pid = None))]
    fn publish(&self, value: &[u8], pid: Option<u32>) -> PyResult<bool> {
        if value.len() != self.inner.value_len() {
            return Err(PyValueError::new_err(format!(
                "the value must be exactly {} bytes",
                self.inner.value_len()
            )));
        }
        Ok(self.inner.publish(pid.unwrap_or_else(std::process::id), value))
    }

    /// Wait for whoever claimed it to publish, up to `timeout` seconds.
    /// The interpreter is detached while waiting.
    #[pyo3(signature = (timeout = 30.0))]
    fn wait(&self, py: Python<'_>, timeout: f64) -> PyResult<Vec<u8>> {
        if timeout.is_nan() || timeout <= 0.0 {
            return Err(PyValueError::new_err("the timeout must be positive"));
        }
        let len = self.inner.value_len();
        let cell: &SharedOnceCellDyn = &self.inner;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout);
        let mut out = vec![0u8; len];
        py.detach(|| cell.wait(&mut out, deadline))
            .map_err(|e| os_err("waiting for the value", e))?;
        Ok(out)
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.LazyValue {} of {} bytes>",
            if self.ready() { "ready" } else { "not yet published" },
            self.inner.value_len()
        )
    }
}

/// A hybrid logical clock shared between processes: each participant
/// keeps its own clock in one file, and the global fence is the latest
/// reading across all of them.
///
/// A reading is a pair: the physical microseconds and a logical counter
/// that breaks ties when two events share a microsecond.
#[pyclass(module = "subetha")]
struct FenceClock {
    inner: Box<SharedFenceClock>,
}

#[pymethods]
impl FenceClock {
    /// Obtain the clock at `path` with room for `capacity` participants,
    /// creating it when the file does not exist. A capacity of zero is a
    /// `ValueError`.
    ///
    /// One slot per participant is what makes `global_fence` a scan over
    /// the slots rather than a lock every process contends on. Creating
    /// the clock takes no slot: `register` does that.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity == 0 {
            return Err(PyValueError::new_err("capacity must be at least one"));
        }
        SharedFenceClock::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the clock", e))
    }

    /// Attach to a clock that already exists, raising `OSError` when it
    /// does not. `capacity` must be the one it was created with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedFenceClock::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the clock", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn shared_clock_us(&self) -> u64 {
        self.inner.shared_clock_us()
    }

    /// Take a participant slot. Every tick and merge names one.
    #[pyo3(signature = (pid = None))]
    fn register(&self, pid: Option<u32>) -> PyResult<usize> {
        self.inner
            .register(pid.unwrap_or_else(std::process::id))
            .map_err(|e| os_err("registering", e))
    }

    /// Give a participant slot back, so another process can take it. The
    /// readings it contributed stay in the shared clock: giving up the
    /// slot stops this participant advancing, it does not rewind what it
    /// already published.
    fn unregister(&self, slot: usize) {
        self.inner.unregister(slot);
    }

    /// Move this participant's clock on and return the new reading.
    fn tick(&self, slot: usize) -> (u64, u64) {
        let hlc = self.inner.tick(slot);
        (hlc.physical_us, hlc.logical)
    }

    /// Fold a reading received from elsewhere into this participant's
    /// clock, which is what makes the ordering hold across processes.
    fn merge(&self, slot: usize, physical_us: u64, logical: u64) -> (u64, u64) {
        let merged = self.inner.merge(slot, Hlc { physical_us, logical });
        (merged.physical_us, merged.logical)
    }

    /// This participant's current reading, without moving it.
    fn get_local(&self, slot: usize) -> (u64, u64) {
        let hlc = self.inner.get_local(slot);
        (hlc.physical_us, hlc.logical)
    }

    /// The latest reading across every live participant: every event any
    /// of them has recorded is at or before this.
    fn global_fence(&self) -> (u64, u64) {
        let hlc = self.inner.compute_global_fence();
        (hlc.physical_us, hlc.logical)
    }

    fn __repr__(&self) -> String {
        let fence = self.inner.compute_global_fence();
        format!(
            "<subetha.FenceClock fence at {}us/{}>",
            fence.physical_us, fence.logical
        )
    }
}

/// A Bloom filter in a mapped file: it answers "definitely not there" or
/// "probably there", and never the other way round.
#[pyclass(module = "subetha")]
struct BloomFilter {
    inner: Box<SharedBloomFilter>,
}

#[pymethods]
impl BloomFilter {
    /// Obtain the filter at `path` with `n_bits` bits and `n_hashes` hash
    /// functions, creating it when the file does not exist. Either being
    /// zero is a `ValueError`.
    ///
    /// The pair decides both the false-positive rate and which bits an
    /// item touches, so it is part of the layout rather than a tuning
    /// knob. `suggest_config` works it out from an item count and the
    /// rate you are willing to accept.
    #[new]
    fn new(path: &str, n_bits: usize, n_hashes: u32) -> PyResult<Self> {
        if n_bits == 0 || n_hashes == 0 {
            return Err(PyValueError::new_err(
                "n_bits and n_hashes must both be at least one",
            ));
        }
        SharedBloomFilter::create(path, n_bits, n_hashes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the filter", e))
    }

    /// Attach to a filter that already exists, raising `OSError` when it
    /// does not. `n_bits` and `n_hashes` must be the ones it was created
    /// with, because together they decide which bits an item touches.
    #[staticmethod]
    fn open(path: &str, n_bits: usize, n_hashes: u32) -> PyResult<Self> {
        SharedBloomFilter::open(path, n_bits, n_hashes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the filter", e))
    }

    /// The bits and hash count for `n_items` at a false-positive rate of
    /// `p`, so a caller sizes the filter from what it means rather than
    /// from arithmetic it has to do itself.
    #[staticmethod]
    fn suggest_config(n_items: usize, false_positive_rate: f64) -> PyResult<(usize, u32)> {
        if n_items == 0 {
            return Err(PyValueError::new_err("n_items must be at least one"));
        }
        if !(false_positive_rate > 0.0 && false_positive_rate < 1.0) {
            return Err(PyValueError::new_err(
                "the false positive rate must lie between zero and one",
            ));
        }
        Ok(SharedBloomFilter::suggest_config(n_items, false_positive_rate))
    }

    #[getter]
    fn n_bits(&self) -> u64 {
        self.inner.n_bits()
    }

    #[getter]
    fn n_hashes(&self) -> u32 {
        self.inner.n_hashes()
    }

    /// What the filter's false-positive rate has drifted to, given what
    /// has actually been put in it.
    #[getter]
    fn false_positive_rate(&self) -> f64 {
        self.inner.estimated_false_positive_rate()
    }

    /// Add an item.
    ///
    /// Nothing can be taken out again. A Bloom filter only ever gains
    /// bits, so `false_positive_rate` climbs with every insert and
    /// `clear` is the only way back.
    fn insert(&self, item: &[u8]) -> PyResult<()> {
        self.inner
            .insert(item)
            .map_err(|e| os_err("inserting", e))
    }

    /// Insert a run of items in one crossing.
    fn insert_many(&self, items: Vec<Vec<u8>>) -> PyResult<()> {
        for item in &items {
            self.inner
                .insert(item)
                .map_err(|e| os_err("inserting", e))?;
        }
        Ok(())
    }

    /// `False` means it is definitely absent. `True` means it is
    /// probably present, at about `false_positive_rate`.
    fn __contains__(&self, item: &[u8]) -> PyResult<bool> {
        self.inner
            .contains(item)
            .map_err(|e| os_err("looking up", e))
    }

    /// `False` means definitely absent, `True` means probably present at
    /// about `false_positive_rate`. The same answer `in` gives, named so
    /// a caller can pass it around rather than write the operator.
    fn contains(&self, item: &[u8]) -> PyResult<bool> {
        self.inner
            .contains(item)
            .map_err(|e| os_err("looking up", e))
    }

    /// Look a run of items up in one crossing.
    fn contains_many(&self, items: Vec<Vec<u8>>) -> PyResult<Vec<bool>> {
        let mut answers = Vec::with_capacity(items.len());
        for item in &items {
            answers.push(
                self.inner
                    .contains(item)
                    .map_err(|e| os_err("looking up", e))?,
            );
        }
        Ok(answers)
    }

    /// Clear every bit, so the filter is empty again and
    /// `false_positive_rate` goes back to nothing. The size and the hash
    /// count are unchanged, and every process mapping the file sees it.
    fn clear(&self) {
        self.inner.clear();
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.BloomFilter {} bits, {} hashes, about {:.4} false positive>",
            self.inner.n_bits(),
            self.inner.n_hashes(),
            self.inner.estimated_false_positive_rate()
        )
    }
}

/// A ring that can be resized while it is in use.
///
/// A morph leaves the old backing in place until its items have been
/// drained, so nothing in flight is lost; the consumer reads the stale
/// backings oldest first and only then the new one. That is why a resize
/// is a call rather than a rebuild.
#[pyclass(module = "subetha")]
struct CapacityRing {
    inner: Box<CapacityAdaptiveRing>,
}

#[pymethods]
impl CapacityRing {
    /// The capacity must be a power of two and at least two, which the
    /// ring arithmetic relies on.
    ///
    /// `stamped` builds a ring whose items carry ordering stamps, which
    /// is what `ordering_mode` and `inversions` read. Stamps cost space
    /// and a write per item, so they are off by default.
    #[new]
    #[pyo3(signature = (path, capacity, max_producers = 1, max_consumers = 1, stamped = false))]
    fn new(
        path: &str,
        capacity: usize,
        max_producers: usize,
        max_consumers: usize,
        stamped: bool,
    ) -> PyResult<Self> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PyValueError::new_err(
                "capacity must be a power of two and at least two",
            ));
        }
        if max_producers < 1 || max_consumers < 1 {
            return Err(PyValueError::new_err(
                "a ring needs at least one producer and one consumer",
            ));
        }
        let built = if stamped {
            CapacityAdaptiveRing::create_stamped(path, max_producers, max_consumers, capacity)
        } else {
            CapacityAdaptiveRing::create(path, max_producers, max_consumers, capacity)
        };
        built
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the ring", e))
    }

    /// `stamped` must match how the ring was created: a stamped ring
    /// carries ordering stamps and attaching to one as unstamped reads
    /// the wrong shape.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, max_producers = 1, max_consumers = 1, stamped = false))]
    fn open(
        path: &str,
        capacity: usize,
        max_producers: usize,
        max_consumers: usize,
        stamped: bool,
    ) -> PyResult<Self> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PyValueError::new_err(
                "capacity must be a power of two and at least two",
            ));
        }
        CapacityAdaptiveRing::open(path, max_producers, max_consumers, capacity, stamped)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the ring", e))
    }

    /// The capacity right now, which a morph changes.
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.current_capacity()
    }

    /// Steps when the backing changes, so a holder of a pin can tell
    /// that what it pinned has been superseded.
    #[getter]
    fn pin_generation(&self) -> u64 {
        self.inner.pin_generation()
    }

    /// Take a producer position, which every `send` names. A ring built
    /// with one producer has exactly one to take, and asking past
    /// `max_producers` raises rather than answering.
    fn register_producer(&self) -> PyResult<usize> {
        self.inner
            .register_producer()
            .map_err(|e| os_err("registering a producer", e))
    }

    /// Take a consumer position, which every `recv` names.
    ///
    /// Register before anything is sent. A consumer starts where the ring
    /// is when it registers, so one registered after a burst has already
    /// gone by sees none of it, and nothing reports that.
    fn register_consumer(&self) -> PyResult<usize> {
        self.inner
            .register_consumer()
            .map_err(|e| os_err("registering a consumer", e))
    }

    /// Send one item from `producer`. `False` means the ring is full,
    /// which is an answer rather than a failure; an item too long for a
    /// slot raises.
    fn send(&self, producer: usize, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_send(producer, item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("sending", e)),
        }
    }

    /// Send a run of items from one producer, stopping at the first that
    /// will not fit, and answer how many landed. A count short of what
    /// was handed in means the rest were not sent and are still the
    /// caller's to hold.
    fn send_many(&self, producer: usize, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut sent = 0;
        for item in &items {
            match self.inner.try_send(producer, item) {
                Ok(()) => sent += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("sending", e)),
            }
        }
        Ok(sent)
    }

    /// Take the next item for `consumer`, or `None` when there is nothing
    /// to take.
    ///
    /// A morph does not interrupt this. Backings a resize has superseded
    /// are drained oldest first, so an item sent before the resize still
    /// arrives, and `stale_pops` counts how many came that way.
    fn recv(&self, consumer: usize) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.inner.try_recv(consumer, &mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("receiving", e)),
        }
    }

    /// Take up to `max_items` for `consumer` in one crossing, stopping
    /// early when the ring runs dry. An empty list means there was
    /// nothing, which is the same answer `recv` gives as `None`.
    fn recv_many(&self, consumer: usize, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_recv(consumer, &mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Resize. Items already in the ring stay readable through the old
    /// backing until they have been taken, so nothing in flight is lost.
    fn morph_to(&self, capacity: usize) -> PyResult<()> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PyValueError::new_err(
                "capacity must be a power of two and at least two",
            ));
        }
        self.inner
            .morph_capacity_to(capacity)
            .map_err(|e| os_err("resizing", e))
    }

    /// Build a backing of `capacity` ahead of needing it, so the morph
    /// that switches to it does not pay for the allocation.
    fn prewarm(&self, capacity: usize) -> PyResult<()> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PyValueError::new_err(
                "capacity must be a power of two and at least two",
            ));
        }
        self.inner
            .prewarm(capacity)
            .map_err(|e| os_err("prewarming", e))
    }

    /// The capacity held ready by `prewarm`, or None when nothing is
    /// cached.
    #[getter]
    fn warm_capacity(&self) -> Option<usize> {
        self.inner.warm_capacity()
    }

    /// Morphs that took the prewarmed backing instead of building one.
    #[getter]
    fn warm_hits(&self) -> u64 {
        self.inner.warm_hits()
    }

    /// Items taken from a backing a morph has superseded rather than
    /// from the current one, which is what a morph costs a reader.
    #[getter]
    fn stale_pops(&self) -> u64 {
        self.inner.stale_pops()
    }

    /// Drop the prewarmed backing, giving back its memory and its file.
    fn clear_warm(&self) {
        self.inner.clear_warm();
    }

    /// Whether this ring's items carry ordering stamps, which is fixed
    /// when the ring is built.
    #[getter]
    fn stamped(&self) -> bool {
        self.inner.is_stamped()
    }

    /// The ordering discipline in force, one of `unordered`,
    /// `merge_by_stamp` or `merge_strict`. None on an unstamped ring.
    #[getter]
    fn ordering_mode(&self) -> Option<&'static str> {
        self.inner.ordering_mode().map(ordering_mode_name)
    }

    /// Set the ordering discipline across the current backing and every
    /// superseded one still draining, so a reader walking both applies
    /// one discipline.
    fn set_ordering_mode(&self, mode: &str) -> PyResult<()> {
        let mode = ordering_mode_from_name(mode)?;
        self.inner
            .set_ordering_mode(mode)
            .map_err(|e| os_err("setting the ordering mode", e))
    }

    /// Cross-producer inversions seen so far, carried across morphs.
    #[getter]
    fn inversions(&self) -> u64 {
        self.inner.inversions()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. A prewarmed backing is still held afterwards, and a
    /// producer or consumer position taken inside is still registered:
    /// `clear_warm` gives back the first, and nothing gives back the
    /// second.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.CapacityRing capacity={}>",
            self.inner.current_capacity()
        )
    }
}

fn ordering_mode_name(mode: OrderingMode) -> &'static str {
    match mode {
        OrderingMode::Unordered => "unordered",
        OrderingMode::MergeByStamp => "merge_by_stamp",
        OrderingMode::MergeStrict => "merge_strict",
    }
}

fn ordering_mode_from_name(name: &str) -> PyResult<OrderingMode> {
    match name {
        "unordered" => Ok(OrderingMode::Unordered),
        "merge_by_stamp" => Ok(OrderingMode::MergeByStamp),
        "merge_strict" => Ok(OrderingMode::MergeStrict),
        other => Err(PyValueError::new_err(format!(
            "unknown ordering mode {other}, expected unordered, merge_by_stamp or merge_strict"
        ))),
    }
}

/// A ring that can move the bytes it holds between three places
/// without the senders and readers having to reconnect.
///
/// The three locales are `anon`, memory private to one process and the
/// cheapest to send through; `file`, a mapped file that survives the
/// process and reaches any other one that opens it; and `shmfs`, named
/// memory that other processes can reach but which never goes to disk.
/// A ring starts at `anon`; `migrate_to` moves it, carrying whatever is
/// already in it.
#[pyclass(module = "subetha")]
struct LocaleRing {
    inner: Box<LocaleAdaptiveRing>,
}

#[pymethods]
impl LocaleRing {
    /// The capacity must be a power of two and at least two, which the
    /// ring arithmetic relies on. All three backings are built here, so
    /// a later migration has somewhere to go without allocating.
    ///
    /// `stamped` builds a ring whose items carry ordering stamps, which
    /// is what `ordering_mode` and `inversions` read, and which is what
    /// lets a migration preserve order across every sender.
    #[new]
    #[pyo3(signature = (path, capacity, max_producers = 1, max_consumers = 1, stamped = false))]
    fn new(
        path: &str,
        capacity: usize,
        max_producers: usize,
        max_consumers: usize,
        stamped: bool,
    ) -> PyResult<Self> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PyValueError::new_err(
                "capacity must be a power of two and at least two",
            ));
        }
        if max_producers < 1 || max_consumers < 1 {
            return Err(PyValueError::new_err(
                "a ring needs at least one producer and one consumer",
            ));
        }
        let built = if stamped {
            LocaleAdaptiveRing::create_with_ordering_stamps(
                path,
                max_producers,
                max_consumers,
                capacity,
            )
        } else {
            LocaleAdaptiveRing::create(path, max_producers, max_consumers, capacity)
        };
        built
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the ring", e))
    }

    /// Attach to a ring another holder created. The counts, the
    /// capacity and `stamped` must all be the ones it was created with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, max_producers = 1, max_consumers = 1, stamped = false))]
    fn open(
        path: &str,
        capacity: usize,
        max_producers: usize,
        max_consumers: usize,
        stamped: bool,
    ) -> PyResult<Self> {
        if !capacity.is_power_of_two() || capacity < 2 {
            return Err(PyValueError::new_err(
                "capacity must be a power of two and at least two",
            ));
        }
        LocaleAdaptiveRing::open(path, max_producers, max_consumers, capacity, stamped)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the ring", e))
    }

    /// Where the bytes are right now, one of `anon`, `file` or `shmfs`.
    #[getter]
    fn locale(&self) -> &'static str {
        locale_name(self.inner.current_locale())
    }

    /// Steps on every migration, so a holder that captured the locale
    /// can tell that what it captured has been superseded.
    #[getter]
    fn locale_generation(&self) -> u64 {
        self.inner.locale_generation()
    }

    /// Register on all three backings at once, so the registration is
    /// there whichever locale is live.
    fn register_producer(&self) -> PyResult<usize> {
        self.inner
            .register_producer()
            .map_err(|e| os_err("registering a producer", e))
    }

    /// Take a consumer position, which every `recv` names. Registered on
    /// all three backings at once, like a producer, so a migration does
    /// not lose it.
    ///
    /// Register before anything is sent. A consumer starts where the ring
    /// is when it registers, so one registered after a burst has already
    /// gone by sees none of it, and nothing reports that.
    fn register_consumer(&self) -> PyResult<usize> {
        self.inner
            .register_consumer()
            .map_err(|e| os_err("registering a consumer", e))
    }

    /// Send one item from `producer` into whichever locale is live.
    /// `False` means the ring is full, which is an answer rather than a
    /// failure; an item too long for a slot raises.
    fn send(&self, producer: usize, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_send(producer, item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("sending", e)),
        }
    }

    /// Send a run of items from one producer, stopping at the first that
    /// will not fit, and answer how many landed. A count short of what
    /// was handed in means the rest were not sent and are still the
    /// caller's to hold.
    fn send_many(&self, producer: usize, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut sent = 0;
        for item in &items {
            match self.inner.try_send(producer, item) {
                Ok(()) => sent += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("sending", e)),
            }
        }
        Ok(sent)
    }

    /// Take the next item for `consumer`, or `None` when there is nothing
    /// to take.
    ///
    /// A migration does not interrupt this and does not need the reader
    /// to reconnect: `migrate_to` carries what is already in the ring to
    /// the new locale, and this keeps reading.
    fn recv(&self, consumer: usize) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.inner.try_recv(consumer, &mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("receiving", e)),
        }
    }

    /// Take up to `max_items` for `consumer` in one crossing, stopping
    /// early when the ring runs dry. An empty list means there was
    /// nothing, which is the same answer `recv` gives as `None`.
    fn recv_many(&self, consumer: usize, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_recv(consumer, &mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Move the ring to another locale, carrying what is already in it.
    /// On a stamped ring the transfer keeps the order every sender saw;
    /// on an unstamped one the drain can interleave senders, as a shape
    /// change can.
    fn migrate_to(&self, locale: &str) -> PyResult<()> {
        let target = locale_from_name(locale)?;
        self.inner
            .migrate_to(target)
            .map_err(|e| os_err("migrating", e))
    }

    /// Whether this ring's items carry ordering stamps, which is fixed
    /// when the ring is built.
    #[getter]
    fn stamped(&self) -> bool {
        self.inner.is_stamped()
    }

    /// The ordering discipline in force, one of `unordered`,
    /// `merge_by_stamp` or `merge_strict`. None on an unstamped ring.
    #[getter]
    fn ordering_mode(&self) -> Option<&'static str> {
        self.inner.ordering_mode().map(ordering_mode_name)
    }

    /// Set the ordering discipline on all three backings, so a
    /// migration does not change the discipline underneath a reader.
    fn set_ordering_mode(&self, mode: &str) -> PyResult<()> {
        let mode = ordering_mode_from_name(mode)?;
        self.inner
            .set_ordering_mode(mode)
            .map_err(|e| os_err("setting the ordering mode", e))
    }

    /// Cross-producer inversions seen across all three backings.
    #[getter]
    fn inversions(&self) -> u64 {
        self.inner.inversions()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The ring stays in whichever locale it was migrated to,
    /// and a position taken inside the block is still registered.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.LocaleRing locale={}>",
            locale_name(self.inner.current_locale())
        )
    }
}

fn locale_name(locale: Locale) -> &'static str {
    match locale {
        Locale::Anon => "anon",
        Locale::File => "file",
        Locale::ShmFs => "shmfs",
    }
}

fn locale_from_name(name: &str) -> PyResult<Locale> {
    match name {
        "anon" => Ok(Locale::Anon),
        "file" => Ok(Locale::File),
        "shmfs" => Ok(Locale::ShmFs),
        other => Err(PyValueError::new_err(format!(
            "unknown locale {other}, expected anon, file or shmfs"
        ))),
    }
}

/// Epoch-based reclamation shared between processes: readers take a
/// ticket at the current epoch, and memory is only reused once every
/// ticket that could still see it has gone.
///
/// # What is not here
///
/// The Rust `pin` returns a guard that owns its slot privately, and the
/// only release takes a slot number. A `with`-block pin would therefore
/// have to reimplement the pin protocol out here to know which slot to
/// give back, which is precisely the sort of duplication that goes
/// wrong quietly. Tickets, which hand back their slot, are bound
/// instead; a pin belongs here once the guard exposes its slot upstream.
#[pyclass(module = "subetha")]
struct Epochs {
    inner: Box<SharedEpochs>,
}

#[pymethods]
impl Epochs {
    /// Obtain the epoch table at `path` with room for `capacity` open
    /// tickets, creating it when the file does not exist. A capacity of
    /// zero is a `ValueError`.
    ///
    /// The capacity is how many tickets may be outstanding at once, so
    /// `claim_ticket` raises rather than queueing once they are all
    /// taken. Size it by how many compound writes can overlap.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity == 0 {
            return Err(PyValueError::new_err("capacity must be at least one"));
        }
        SharedEpochs::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the epochs", e))
    }

    /// Attach to an epoch table that already exists, raising `OSError`
    /// when it does not. `capacity` must be the one it was created with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedEpochs::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the epochs", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// The PUBLISHED epoch, which is not the same as the latest one.
    ///
    /// An open ticket holds this below its own epoch, because the write
    /// that ticket is stamping is not visible yet. So a claim followed
    /// by a read of this gives one less than the ticket's epoch, and it
    /// stays there until the ticket publishes however far `advance` has
    /// moved the counter. That is the mechanism working, not a lag.
    #[getter]
    fn now(&self) -> u64 {
        self.inner.now()
    }

    /// Take the next epoch and return it, for a writer stamping a
    /// version as it supersedes the last.
    ///
    /// The returned epoch is not necessarily what `now` then reports:
    /// an older open ticket holds the published epoch below it.
    fn advance(&self) -> u64 {
        self.inner.advance()
    }

    /// How many tickets are outstanding.
    #[getter]
    fn open_tickets(&self) -> usize {
        self.inner.open_tickets()
    }

    /// Reserve the next epoch for a compound write, returning the slot
    /// holding the ticket and the epoch it took.
    ///
    /// Nothing stamped with that epoch is visible until the ticket is
    /// published, and `now` stays one below it meanwhile. The slot is
    /// what gives it back; a caller that never publishes holds every
    /// reader's view down until its process ends.
    fn claim_ticket(&self) -> PyResult<(usize, u64)> {
        self.inner
            .claim_ticket()
            .map_err(|e| os_err("claiming a ticket", e))
    }

    /// Give a ticket back.
    fn publish_ticket(&self, slot: usize) {
        self.inner.publish_ticket(slot);
    }

    /// The epochs of tickets whose holders are gone, which is what makes
    /// a crashed reader stop holding reclamation up for ever.
    fn dead_tickets(&self) -> Vec<u64> {
        self.inner.dead_tickets()
    }

    /// Free a ticket whose holder died. `True` when one was freed.
    fn free_dead_ticket(&self, epoch: u64) -> bool {
        self.inner.free_dead_ticket(epoch)
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Epochs at {} with {} open tickets>",
            self.inner.now(),
            self.inner.open_tickets()
        )
    }
}

/// A least-recently-used cache in a mapped file, shared by every process
/// that opens it.
///
/// Reading through `get` does not count as use; `get_and_touch` does.
/// The two are separate because a cache that is inspected by a monitor
/// should not have its eviction order rearranged by being looked at.
#[pyclass(module = "subetha")]
struct LruCache {
    inner: Box<RawLruCache>,
}

#[pymethods]
impl LruCache {
    /// Obtain the cache at `path` holding `capacity` entries of
    /// `key_size` and `value_size` bytes, creating it when the file does
    /// not exist. A capacity of zero is a `ValueError`.
    ///
    /// Both widths are fixed: they are part of the layout every process
    /// reads out of the file, not a maximum this one is willing to store.
    /// The cache never grows past the capacity, so a `put` into a full
    /// one evicts the least recently used entry.
    #[new]
    fn new(path: &str, capacity: u32, key_size: usize, value_size: usize) -> PyResult<Self> {
        if capacity == 0 {
            return Err(PyValueError::new_err("capacity must be at least one"));
        }
        RawLruCache::create(path, capacity, key_size, value_size)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the cache", e))
    }

    /// Attach to a cache that already exists, raising `OSError` when it
    /// does not. The capacity and both widths must be the ones it was
    /// created with.
    #[staticmethod]
    fn open(path: &str, capacity: u32, key_size: usize, value_size: usize) -> PyResult<Self> {
        RawLruCache::open(path, capacity, key_size, value_size)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the cache", e))
    }

    #[getter]
    fn capacity(&self) -> u32 {
        self.inner.capacity()
    }

    #[getter]
    fn key_size(&self) -> usize {
        self.inner.key_size()
    }

    #[getter]
    fn value_size(&self) -> usize {
        self.inner.value_size()
    }

    /// How many entries the cache holds, which is at most the capacity
    /// because a full cache evicts rather than growing.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Read without changing the eviction order.
    fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.get(key, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("reading", e)),
        }
    }

    /// Read and count it as use, which is what a cache client wants.
    fn get_and_touch(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.get_and_touch(key, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("reading", e)),
        }
    }

    /// Read a run of keys without disturbing the order.
    fn get_many(&self, keys: Vec<Vec<u8>>) -> PyResult<Vec<Option<Vec<u8>>>> {
        let mut answers = Vec::with_capacity(keys.len());
        for key in &keys {
            let mut out = vec![0u8; self.inner.value_size()];
            match self.inner.get(key, &mut out) {
                Ok(true) => answers.push(Some(out)),
                Ok(false) => answers.push(None),
                Err(e) => return Err(os_err("reading", e)),
            }
        }
        Ok(answers)
    }

    /// Count a key as used without reading it.
    fn touch(&self, key: &[u8]) -> PyResult<bool> {
        self.inner.touch(key).map_err(|e| os_err("touching", e))
    }

    /// Whether the key is present, without counting as use. Asking does
    /// not save an entry from eviction; `touch` is what does that.
    fn __contains__(&self, key: &[u8]) -> PyResult<bool> {
        self.inner
            .contains_key(key)
            .map_err(|e| os_err("looking up", e))
    }

    /// Put a key in at the most recent end, evicting the least recently
    /// used first if the cache is full. `True` means the key was already
    /// present and its value was replaced.
    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<bool> {
        self.inner
            .put(key, value)
            .map_err(|e| os_err("inserting", e))
    }

    /// Put a run of pairs in, and answer how many.
    ///
    /// Unlike the batch forms on the rings, this does not stop early: a
    /// cache always has room because a full one evicts instead of
    /// refusing, so the answer is always the length of what was handed
    /// in. Put more pairs than the capacity and the earlier ones in the
    /// same call are the ones evicted.
    fn put_many(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<usize> {
        for (key, value) in &pairs {
            self.inner
                .put(key, value)
                .map_err(|e| os_err("inserting", e))?;
        }
        Ok(pairs.len())
    }

    /// Take a key out and answer the value it held, or `None` when it was
    /// not there. The freed room goes to the next `put`.
    fn remove(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.remove(key, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("removing", e)),
        }
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The entries stay in the file for whoever attaches next,
    /// which is the point of a cache that outlives the process.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.LruCache {} of {}>",
            self.inner.len(),
            self.inner.capacity()
        )
    }
}

/// A HyperLogLog in a mapped file: it counts how many distinct items it
/// has seen, in a fixed amount of memory, without keeping any of them.
#[pyclass(module = "subetha")]
struct HyperLogLog {
    inner: Box<SharedHyperLogLog>,
}

#[pymethods]
impl HyperLogLog {
    /// `precision` sets the trade between memory and accuracy: 4 is the
    /// smallest the format allows and 16 the largest.
    #[new]
    #[pyo3(signature = (path, precision = 14))]
    fn new(path: &str, precision: u8) -> PyResult<Self> {
        if !(HLL_MIN_PRECISION..=HLL_MAX_PRECISION).contains(&precision) {
            return Err(PyValueError::new_err(format!(
                "precision must lie between {HLL_MIN_PRECISION} and {HLL_MAX_PRECISION}"
            )));
        }
        SharedHyperLogLog::create(path, precision)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the counter", e))
    }

    /// Attach to a counter that already exists, raising `OSError` when it
    /// does not. `precision` must be the one it was created with, because
    /// it sets how many registers the file holds.
    #[staticmethod]
    #[pyo3(signature = (path, precision = 14))]
    fn open(path: &str, precision: u8) -> PyResult<Self> {
        SharedHyperLogLog::open(path, precision)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the counter", e))
    }

    #[getter]
    fn precision(&self) -> u8 {
        self.inner.precision()
    }

    #[getter]
    fn n_registers(&self) -> u32 {
        self.inner.n_registers()
    }

    /// Record having seen `item`.
    ///
    /// Nothing is kept: the item is hashed into a register and thrown
    /// away, which is why the memory does not grow with what it has seen
    /// and why it can only ever estimate. Inserting the same item twice
    /// changes nothing.
    fn insert(&self, item: &[u8]) {
        self.inner.insert(item);
    }

    /// Insert a run of items in one crossing, which is what this
    /// structure is usually fed: a stream rather than single items.
    fn insert_many(&self, items: Vec<Vec<u8>>) -> usize {
        for item in &items {
            self.inner.insert(item);
        }
        items.len()
    }

    /// How many distinct items it estimates it has seen. An estimate,
    /// not a count: that is the bargain the structure makes.
    fn estimate(&self) -> u64 {
        self.inner.estimate()
    }

    /// Zero every register, so the counter is empty again and `estimate`
    /// answers nothing seen. Every process mapping the file sees it.
    fn reset(&self) {
        self.inner.reset();
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// registers without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.HyperLogLog precision={} about {} distinct>",
            self.inner.precision(),
            self.inner.estimate()
        )
    }
}

/// A count-min sketch in a mapped file: it estimates how often it has
/// seen each item, never undercounting and sometimes overcounting.
#[pyclass(module = "subetha")]
struct CountMinSketch {
    inner: Box<SharedCountMinSketch>,
}

#[pymethods]
impl CountMinSketch {
    /// Obtain the sketch at `path` with `depth` rows of `width` cells,
    /// creating it when the file does not exist. Either being zero is a
    /// `ValueError`.
    ///
    /// The pair is the layout: it sets which cells an item hashes to, so
    /// two processes disagreeing about it read different sketches out of
    /// one file. `suggest_config` works the pair out from the error and
    /// the confidence you want instead.
    #[new]
    fn new(path: &str, depth: u32, width: u32) -> PyResult<Self> {
        if depth == 0 || width == 0 {
            return Err(PyValueError::new_err(
                "depth and width must both be at least one",
            ));
        }
        SharedCountMinSketch::create(path, depth, width)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the sketch", e))
    }

    /// Attach to a sketch that already exists, raising `OSError` when it
    /// does not. `depth` and `width` must be the ones it was created
    /// with, because together they decide which cells an item touches.
    #[staticmethod]
    fn open(path: &str, depth: u32, width: u32) -> PyResult<Self> {
        SharedCountMinSketch::open(path, depth, width)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the sketch", e))
    }

    /// The depth and width for an error of `epsilon` with confidence
    /// `delta`, so a caller sizes the sketch from what it needs.
    #[staticmethod]
    fn suggest_config(epsilon: f64, delta: f64) -> PyResult<(u32, u32)> {
        if !(epsilon > 0.0 && epsilon < 1.0) {
            return Err(PyValueError::new_err("epsilon must lie between zero and one"));
        }
        if !(delta > 0.0 && delta < 1.0) {
            return Err(PyValueError::new_err("delta must lie between zero and one"));
        }
        Ok(SharedCountMinSketch::suggest_config(epsilon, delta))
    }

    #[getter]
    fn depth(&self) -> u32 {
        self.inner.d()
    }

    #[getter]
    fn width(&self) -> u32 {
        self.inner.w()
    }

    #[getter]
    fn total_inserts(&self) -> u64 {
        self.inner.total_inserts()
    }

    /// Record one occurrence of `item`.
    ///
    /// The item itself is not kept, only counts in the cells it hashes
    /// to, which is why the memory is fixed and the answer can only ever
    /// be an over-estimate.
    fn insert(&self, item: &[u8]) {
        self.inner.insert(item);
    }

    /// Add `count` occurrences at once rather than calling `insert` that
    /// many times.
    fn insert_n(&self, item: &[u8], count: u64) {
        self.inner.insert_n(item, count);
    }

    /// Record one occurrence of each item in one crossing, and answer how
    /// many were handed in. Nothing here can refuse, so the count is
    /// always the length of the sequence.
    fn insert_many(&self, items: Vec<Vec<u8>>) -> usize {
        for item in &items {
            self.inner.insert(item);
        }
        items.len()
    }

    /// How often it thinks it has seen `item`. Never less than the true
    /// count, sometimes more.
    fn estimate_count(&self, item: &[u8]) -> u64 {
        self.inner.estimate_count(item)
    }

    /// One estimate per item, in the order they were given, from a single
    /// crossing rather than one per item. Each answer has the same
    /// never-under, sometimes-over property as `estimate_count`.
    fn estimate_many(&self, items: Vec<Vec<u8>>) -> Vec<u64> {
        items.iter().map(|i| self.inner.estimate_count(i)).collect()
    }

    /// Zero every cell and the insert total, so the sketch is empty
    /// again. Every process mapping the file sees it.
    fn reset(&self) {
        self.inner.reset();
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.CountMinSketch {}x{}, {} inserts>",
            self.inner.d(),
            self.inner.w(),
            self.inner.total_inserts()
        )
    }
}

/// A bit vector in a mapped file, addressed by bit index.
#[pyclass(module = "subetha")]
struct BitVec {
    inner: Box<SharedBitVec>,
}

#[pymethods]
impl BitVec {
    /// The constructor asserts a capacity of at least one bit, so that
    /// is refused here first.
    #[new]
    fn new(path: &str, capacity_bits: usize) -> PyResult<Self> {
        if capacity_bits < 1 {
            return Err(PyValueError::new_err("capacity_bits must be at least one"));
        }
        SharedBitVec::create(path, capacity_bits)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the bit vector", e))
    }

    /// Attach to a bit vector that already exists, raising `OSError` when
    /// it does not. `capacity_bits` must be the one it was created with.
    #[staticmethod]
    fn open(path: &str, capacity_bits: usize) -> PyResult<Self> {
        SharedBitVec::open(path, capacity_bits)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the bit vector", e))
    }

    #[getter]
    fn capacity_bits(&self) -> usize {
        self.inner.capacity_bits()
    }

    /// How many bits the vector addresses, which is its capacity and not
    /// a count of the bits that are set. It never changes, so `len` on a
    /// bit vector is a constant rather than a measurement.
    fn __len__(&self) -> usize {
        self.inner.capacity_bits()
    }

    /// Set bit `index` and report what it was.
    fn set(&self, index: usize) -> PyResult<bool> {
        self.inner.set(index).map_err(|e| os_err("setting a bit", e))
    }

    /// Clear bit `index` and report what it was.
    fn clear(&self, index: usize) -> PyResult<bool> {
        self.inner
            .clear(index)
            .map_err(|e| os_err("clearing a bit", e))
    }

    /// Flip bit `index` and report what it now is.
    ///
    /// Note the asymmetry with `set` and `clear`, which report the value
    /// they replaced: a toggle's interesting answer is the value it
    /// landed on, and the Rust API is written that way.
    fn toggle(&self, index: usize) -> PyResult<bool> {
        self.inner
            .toggle(index)
            .map_err(|e| os_err("toggling a bit", e))
    }

    /// Whether bit `index` is set, without changing it. An index past the
    /// capacity is an `OSError`.
    fn get(&self, index: usize) -> PyResult<bool> {
        self.inner.get(index).map_err(|e| os_err("reading a bit", e))
    }

    /// The same answer `get` gives, so `bits[3]` reads bit three. There
    /// is no slicing and no negative indexing: an index is a bit number.
    fn __getitem__(&self, index: usize) -> PyResult<bool> {
        self.get(index)
    }

    /// Set every bit from `lo` up to but not including `hi`, in one
    /// call rather than one per bit.
    fn set_range(&self, lo: usize, hi: usize) -> PyResult<()> {
        self.inner
            .set_range(lo, hi)
            .map_err(|e| os_err("setting a range", e))
    }

    fn __repr__(&self) -> String {
        format!("<subetha.BitVec {} bits>", self.inner.capacity_bits())
    }
}

/// A histogram in a mapped file, with bucket boundaries the caller
/// chooses, that every process records into.
#[pyclass(module = "subetha")]
struct Histogram {
    inner: Box<SharedHistogram>,
}

#[pymethods]
impl Histogram {
    /// `boundaries` must rise and must not be empty, which is checked
    /// here so a bad one names itself.
    #[new]
    fn new(path: &str, boundaries: Vec<u64>) -> PyResult<Self> {
        if boundaries.is_empty() {
            return Err(PyValueError::new_err("a histogram needs at least one boundary"));
        }
        if boundaries.windows(2).any(|w| w[1] <= w[0]) {
            return Err(PyValueError::new_err("the boundaries must rise"));
        }
        SharedHistogram::create(path, &boundaries)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the histogram", e))
    }

    /// Attach to a histogram that already exists, raising `OSError` when
    /// it does not. `boundaries` must be the ones it was created with:
    /// they are the bucket edges written into the file, so a different
    /// list reads the counts against the wrong edges.
    #[staticmethod]
    fn open(path: &str, boundaries: Vec<u64>) -> PyResult<Self> {
        SharedHistogram::open(path, &boundaries)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the histogram", e))
    }

    #[getter]
    fn n_buckets(&self) -> usize {
        self.inner.n_buckets()
    }

    #[getter]
    fn total_count(&self) -> u64 {
        self.inner.total_count()
    }

    #[getter]
    fn boundaries(&self) -> Vec<u64> {
        self.inner.boundaries_vec()
    }

    /// Every bucket's count in one call, which is what a reader wants:
    /// the alternative is a call per bucket.
    #[getter]
    fn counts(&self) -> Vec<u64> {
        self.inner.counts()
    }

    /// Record one value and return the bucket it fell in.
    fn record(&self, value: u64) -> usize {
        self.inner.record(value)
    }

    /// Record a run of values in one crossing.
    fn record_many(&self, values: Vec<u64>) -> usize {
        for value in &values {
            self.inner.record(*value);
        }
        values.len()
    }

    /// How many values fell in one bucket, by its index. A bucket past
    /// `n_buckets` is an `OSError`. Use `counts` when you want them all:
    /// it costs one crossing rather than one per bucket.
    fn count(&self, bucket: usize) -> PyResult<u64> {
        self.inner
            .count(bucket)
            .map_err(|e| os_err("reading a bucket", e))
    }

    /// The value at percentile `p`, from the bucket boundaries, so it is
    /// as precise as the buckets are.
    fn percentile(&self, p: f64) -> PyResult<u64> {
        if !(0.0..=100.0).contains(&p) {
            return Err(PyValueError::new_err("a percentile lies between 0 and 100"));
        }
        Ok(self.inner.percentile(p))
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Histogram {} buckets, {} recorded>",
            self.inner.n_buckets(),
            self.inner.total_count()
        )
    }
}

/// A token-bucket rate limiter in a mapped file, shared by every process
/// that opens it, so a rate is enforced across all of them rather than
/// per process.
#[pyclass(module = "subetha")]
struct RateLimiter {
    inner: Box<SharedRateLimiter>,
}

#[pymethods]
impl RateLimiter {
    /// Obtain the limiter at `path` holding at most `capacity` tokens and
    /// refilling `refill_per_second` of them each second, creating it
    /// when the file does not exist. Either being zero is a `ValueError`.
    ///
    /// There is one bucket in one file, so the rate holds across every
    /// process that opens it rather than once per process. The capacity
    /// is the burst a caller can take at once; the refill is the rate it
    /// settles to.
    #[new]
    fn new(path: &str, capacity: u32, refill_per_second: u32) -> PyResult<Self> {
        if capacity == 0 || refill_per_second == 0 {
            return Err(PyValueError::new_err(
                "capacity and refill_per_second must both be at least one",
            ));
        }
        SharedRateLimiter::create(path, capacity, refill_per_second)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the limiter", e))
    }

    /// Attach to a limiter that already exists, raising `OSError` when it
    /// does not. The capacity and refill rate must be the ones it was
    /// created with.
    #[staticmethod]
    fn open(path: &str, capacity: u32, refill_per_second: u32) -> PyResult<Self> {
        SharedRateLimiter::open(path, capacity, refill_per_second)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the limiter", e))
    }

    #[getter]
    fn capacity(&self) -> u32 {
        self.inner.capacity()
    }

    #[getter]
    fn refill_per_second(&self) -> u32 {
        self.inner.refill_rate_per_sec()
    }

    /// Tokens available right now, which the refill moves on its own.
    #[getter]
    fn available(&self) -> u32 {
        self.inner.available()
    }

    /// Take `n` tokens if they are there. `False` means they were not,
    /// which is an answer rather than a failure.
    #[pyo3(signature = (n = 1))]
    fn try_acquire(&self, n: u32) -> PyResult<bool> {
        match self.inner.try_acquire(n) {
            Ok(()) => Ok(true),
            Err(RateLimiterError::InsufficientTokens { .. }) => Ok(false),
            Err(e) => Err(os_err("taking tokens", e)),
        }
    }

    /// Refill the bucket to full, which is the opposite of what the name
    /// suggests: this hands out capacity rather than taking it away.
    ///
    /// It is for tests and for an operator clearing a jam, not for the
    /// request path. Nothing coordinates it with `try_acquire`, so a
    /// reset landing beside concurrent takes can let through more than
    /// the capacity in that instant.
    fn reset(&self) {
        self.inner.reset();
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// token count without this.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.RateLimiter {} of {} tokens, {}/s>",
            self.inner.available(),
            self.inner.capacity(),
            self.inner.refill_rate_per_sec()
        )
    }
}

/// The adaptive ring: the family the C ABI is built around, which
/// changes its own shape as the traffic through it changes.
///
/// Producers and consumers register to get an id, and every send and
/// receive names the id it belongs to. The ring can morph between
/// shapes underneath without the caller doing anything.
/// The ring is held by a shared handle rather than a box because a
/// network bridge takes one of its own, and both then name the same
/// ring rather than two copies of it.
#[pyclass(module = "subetha")]
struct Ring {
    inner: Arc<AdaptiveRing>,
}

#[pymethods]
impl Ring {
    /// The constructor asserts that there is at least one producer and
    /// one consumer, so both are refused here first.
    ///
    /// `stamps` marks every item with the order its sender made it in,
    /// which is what `ordered_receiver` reads. Pass `tsc` for the
    /// processor's own clock, `monotonic` for the system clock, or
    /// `counter` for a count shared between the senders, which is the
    /// only one that gives a total order at any speed. None, the
    /// default, leaves the items unmarked and costs nothing.
    #[new]
    #[pyo3(signature = (path, capacity, max_producers = 1, max_consumers = 1, stamps = None))]
    fn new(
        path: &str,
        capacity: usize,
        max_producers: usize,
        max_consumers: usize,
        stamps: Option<&str>,
    ) -> PyResult<Self> {
        if max_producers < 1 || max_consumers < 1 {
            return Err(PyValueError::new_err(
                "a ring needs at least one producer and one consumer",
            ));
        }
        let ring = AdaptiveRing::create(path, max_producers, max_consumers, capacity)
            .map_err(|e| os_err("opening the ring", e))?;
        let ring = apply_stamps(ring, stamps)?;
        Ok(Self { inner: Arc::new(ring) })
    }

    /// Attach to a ring another holder created. `stamps` must be the
    /// kind it was created with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, max_producers = 1, max_consumers = 1, stamps = None))]
    fn open(
        path: &str,
        capacity: usize,
        max_producers: usize,
        max_consumers: usize,
        stamps: Option<&str>,
    ) -> PyResult<Self> {
        if max_producers < 1 || max_consumers < 1 {
            return Err(PyValueError::new_err(
                "a ring needs at least one producer and one consumer",
            ));
        }
        let ring = AdaptiveRing::open(path, max_producers, max_consumers, capacity)
            .map_err(|e| os_err("attaching to the ring", e))?;
        let ring = apply_stamps(ring, stamps)?;
        Ok(Self { inner: Arc::new(ring) })
    }

    /// Whether this ring marks its items with the order their senders
    /// made them, which is fixed when the ring is built.
    #[getter]
    fn stamped(&self) -> bool {
        self.inner.is_stamped()
    }

    /// The kind of mark the items carry, one of `tsc`, `counter` or
    /// `monotonic`. None on a ring whose items are unmarked.
    #[getter]
    fn stamps(&self) -> Option<&'static str> {
        self.inner.stamp_kind().map(stamp_kind_name)
    }

    /// Take a producer id. Every send names one.
    fn register_producer(&self) -> PyResult<usize> {
        self.inner
            .register_producer()
            .map_err(|e| os_err("registering a producer", e))
    }

    /// Take a consumer id. Every receive names one.
    fn register_consumer(&self) -> PyResult<usize> {
        self.inner
            .register_consumer()
            .map_err(|e| os_err("registering a consumer", e))
    }

    /// The shape the ring is in right now, as a name. It can change
    /// under the caller; that is what adaptive means.
    #[getter]
    fn shape(&self) -> String {
        format!("{:?}", self.inner.current_shape())
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.sub_ring_capacity()
    }

    #[getter]
    fn total_capacity(&self) -> usize {
        self.inner.total_slot_capacity()
    }

    #[getter]
    fn max_producers(&self) -> usize {
        self.inner.max_producers()
    }

    #[getter]
    fn max_consumers(&self) -> usize {
        self.inner.max_consumers()
    }

    /// How many items are in it, read without stopping anyone.
    #[getter]
    fn approx_len(&self) -> usize {
        self.inner.approx_len()
    }

    /// Whether the ring looked empty when asked. Read without stopping
    /// the producers, so it is a sighting rather than a promise: a send
    /// can land the instant after. Treat `None` from `recv` as the real
    /// answer, and use this for reporting.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Send one item as `producer`. `False` means the ring was full.
    fn send(&self, producer: usize, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_send(producer, item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("sending", e)),
        }
    }

    /// Send a run of items, stopping at the first refusal.
    fn send_many(&self, producer: usize, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut sent = 0;
        for item in &items {
            match self.inner.try_send(producer, item) {
                Ok(()) => sent += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("sending", e)),
            }
        }
        Ok(sent)
    }

    /// Send items packed end to end, `item_len` bytes each, with no
    /// Python object built per item.
    fn send_buffer(&self, producer: usize, data: &[u8], item_len: usize) -> PyResult<usize> {
        if item_len == 0 {
            return Err(PyValueError::new_err("item_len must not be zero"));
        }
        let mut sent = 0;
        for chunk in data.chunks(item_len) {
            match self.inner.try_send(producer, chunk) {
                Ok(()) => sent += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("sending", e)),
            }
        }
        Ok(sent)
    }

    /// Receive one item as `consumer`, or `None` when the ring is empty.
    fn recv(&self, consumer: usize) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.inner.try_recv(consumer, &mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("receiving", e)),
        }
    }

    /// Receive up to `max_items` in one crossing.
    fn recv_many(&self, consumer: usize, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_recv(consumer, &mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Send a payload longer than a slot, carried in frames.
    fn send_frame(&self, producer: usize, payload: &[u8]) -> PyResult<bool> {
        match self.inner.send_frame(producer, payload) {
            Ok(_) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("sending a frame", e)),
        }
    }

    /// Receive a framed payload, or `None` when there is none waiting.
    fn recv_frame(&self, consumer: usize) -> PyResult<Option<Vec<u8>>> {
        let mut out = Vec::new();
        match self.inner.recv_frame(consumer, &mut out) {
            Ok(_) => Ok(Some(out)),
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("receiving a frame", e)),
        }
    }

    /// How many times the ring refused to change shape.
    #[getter]
    fn morph_refusals(&self) -> u64 {
        self.inner.morph_refusals()
    }

    /// A reader that hands items back in the order their senders made
    /// them, rather than the order they happened to arrive in.
    ///
    /// It picks its own strategy from how the ring is built, and
    /// `strategy` says which one it picked. On a ring whose stamps come
    /// from a counter it buffers a small window and releases the
    /// smallest stamp in it; past a few hundred senders it asks the ring
    /// to wait on every sender instead. On a ring stamped from the clock,
    /// or one with no stamps at all, the arrival order is already the
    /// right one and nothing is buffered.
    /// The ring must have been built with `stamps`: the order a
    /// receiver delivers in is the order those marks record, and
    /// without them it would have nothing to read and would hand back
    /// nothing at all.
    fn ordered_receiver(slf: Bound<'_, Self>, consumer: usize) -> PyResult<OrderedReceiver> {
        // The ring sits in a box the Ring object owns, so its address
        // does not change for as long as that object lives, and the
        // handle held here keeps it alive at least as long as the
        // receiver. That is what makes the borrow the receiver takes
        // good for the receiver's whole life.
        let held = slf.borrow();
        let ring: *const AdaptiveRing = &*held.inner;
        let ring: &'static AdaptiveRing = unsafe { &*ring };
        drop(held);
        if !ring.is_stamped() {
            return Err(PyValueError::new_err(
                "an ordered receiver needs a ring built with stamps, \
                 such as Ring(path, capacity, stamps='counter')",
            ));
        }
        Ok(OrderedReceiver {
            inner: AdaptiveOrderedReceiver::new(ring, consumer),
            _owner: slf.unbind(),
        })
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Producer and consumer positions taken inside the block
    /// are still registered after it, and items still in the ring stay
    /// there for whoever attaches next.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Ring {:?} about {} of {}>",
            self.inner.current_shape(),
            self.inner.approx_len(),
            self.inner.sub_ring_capacity()
        )
    }
}

fn stamp_kind_name(kind: StampKind) -> &'static str {
    match kind {
        StampKind::Tsc => "tsc",
        StampKind::SharedCounter => "counter",
        StampKind::Monotonic => "monotonic",
    }
}

/// Put marks of the named kind on a ring's items, leaving it unmarked
/// when no kind is named.
fn apply_stamps(ring: AdaptiveRing, stamps: Option<&str>) -> PyResult<AdaptiveRing> {
    let Some(name) = stamps else {
        return Ok(ring);
    };
    let kind = match name {
        "tsc" => StampKind::Tsc,
        "counter" => StampKind::SharedCounter,
        "monotonic" => StampKind::Monotonic,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown stamp kind {other}, expected tsc, counter or monotonic"
            )))
        }
    };
    ring.with_ordering_stamps_kind(kind)
        .map_err(|e| os_err("putting stamps on the ring", e))
}

/// A reader that delivers a ring's items in the order their senders made
/// them. Built by `Ring.ordered_receiver`.
///
/// Under the buffering strategy an item is held back until the window
/// behind it has filled, so at the end of a stream there is a tail still
/// inside the receiver. `flush` is what releases it, and a reader that
/// stops at the first None from `recv` loses that tail.
#[pyclass(module = "subetha", unsendable)]
struct OrderedReceiver {
    /// Declared before the handle below so it is dropped first, because
    /// it borrows from the ring that handle keeps alive.
    inner: AdaptiveOrderedReceiver<'static>,
    /// The ring this came from, kept alive for as long as the receiver.
    _owner: Py<Ring>,
}

#[pymethods]
impl OrderedReceiver {
    /// Everything the ring holds now plus everything the window still
    /// holds back, in order, in one crossing.
    ///
    /// This is the whole of a stream and the shape to reach for. `recv`
    /// alone is not: it answers None while the window is filling as well
    /// as when the ring is empty, so a loop that stops at the first None
    /// leaves items behind in both.
    ///
    /// `max_items` bounds how many times the ring is read, so a sender
    /// that keeps writing cannot hold the call open.
    #[pyo3(signature = (max_items = 4096))]
    fn drain(&mut self, max_items: usize) -> Vec<(Vec<u8>, u64)> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        for _ in 0..max_items {
            if let Some((len, stamp)) = self.inner.try_recv(&mut out) {
                taken.push((out[..len].to_vec(), stamp));
            }
        }
        while let Some((len, stamp)) = self.inner.flush(&mut out) {
            taken.push((out[..len].to_vec(), stamp));
        }
        taken
    }

    /// The next item and its stamp, or None while the window is still
    /// filling or nothing is waiting.
    ///
    /// The two Nones are not distinguishable from here, which is why a
    /// stream that ends must finish with `flush_all` and why `drain` is
    /// the easier shape.
    fn recv(&mut self) -> Option<(Vec<u8>, u64)> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        self.inner.try_recv(&mut out).map(|(len, stamp)| {
            out.truncate(len);
            (out, stamp)
        })
    }

    /// The next item held back in the window, or None once the window is
    /// empty. Call this in a loop at the end of a stream.
    fn flush(&mut self) -> Option<(Vec<u8>, u64)> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        self.inner.flush(&mut out).map(|(len, stamp)| {
            out.truncate(len);
            (out, stamp)
        })
    }

    /// Everything still held, in order, for a caller that would rather
    /// end a stream in one crossing than a loop of them.
    fn flush_all(&mut self) -> Vec<(Vec<u8>, u64)> {
        let mut drained = Vec::new();
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        while let Some((len, stamp)) = self.inner.flush(&mut out) {
            drained.push((out[..len].to_vec(), stamp));
        }
        drained
    }

    /// Which strategy this receiver picked: `reorder`, `strict` or
    /// `direct`.
    #[getter]
    fn strategy(&self) -> &'static str {
        self.inner.strategy()
    }

    /// How many times the window had to grow because an item arrived
    /// further out of order than the window covered. Zero means the
    /// window was wide enough throughout.
    #[getter]
    fn corrections(&self) -> u64 {
        self.inner.corrections()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.OrderedReceiver {} corrections={}>",
            self.inner.strategy(),
            self.inner.corrections()
        )
    }
}

/// The window a reader uses to put items back in order, on its own.
///
/// Items go in with the stamp their sender gave them, and come out
/// smallest stamp first once more than `window` of them are held. The
/// window grows by itself when an item turns up further out of order
/// than it covered, and `corrections` counts how often that happened:
/// zero means the starting window was wide enough all along.
///
/// `Ring.ordered_receiver` uses one of these against a ring. This class
/// is the same window over items from anywhere else.
#[pyclass(module = "subetha")]
struct ReorderWindow {
    inner: Box<ReorderBuffer>,
}

#[pymethods]
impl ReorderWindow {
    /// `floor` is the window to start at and `cap` the widest it may
    /// grow to. A window at least as wide as the number of senders puts
    /// every item back in order.
    #[new]
    #[pyo3(signature = (floor = REORDER_DEFAULT_FLOOR, cap = REORDER_DEFAULT_CAP))]
    fn new(floor: usize, cap: usize) -> Self {
        Self {
            inner: Box::new(ReorderBuffer::with_window(floor, cap)),
        }
    }

    /// Hold one item, with the stamp its sender gave it.
    fn push(&mut self, stamp: u64, payload: &[u8]) {
        self.inner.push(stamp, payload);
    }

    /// Hold a run of items in one crossing. Each is a stamp and a
    /// payload.
    fn push_many(&mut self, items: Vec<(u64, Vec<u8>)>) -> usize {
        for (stamp, payload) in &items {
            self.inner.push(*stamp, payload);
        }
        items.len()
    }

    /// The next item and its stamp, or None while fewer than `window`
    /// items are held.
    fn take(&mut self) -> Option<(Vec<u8>, u64)> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        self.inner.try_take(&mut out).map(|(stamp, len)| {
            out.truncate(len);
            (out, stamp)
        })
    }

    /// The next item regardless of how full the window is, or None once
    /// nothing is held. This is how a stream ends.
    fn flush(&mut self) -> Option<(Vec<u8>, u64)> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        self.inner.flush_one(&mut out).map(|(stamp, len)| {
            out.truncate(len);
            (out, stamp)
        })
    }

    /// Everything still held, in order, in one crossing.
    fn flush_all(&mut self) -> Vec<(Vec<u8>, u64)> {
        let mut drained = Vec::new();
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        while let Some((stamp, len)) = self.inner.flush_one(&mut out) {
            drained.push((out[..len].to_vec(), stamp));
        }
        drained
    }

    /// Widen the window to at least this, which is what a caller does
    /// when the number of senders grows.
    fn widen_to(&mut self, window: usize) {
        self.inner.widen_to(window);
    }

    /// The window right now, which growth changes.
    #[getter]
    fn window(&self) -> usize {
        self.inner.window()
    }

    /// Times an item arrived further out of order than the window
    /// covered, each of which widened it.
    #[getter]
    fn corrections(&self) -> u64 {
        self.inner.corrections()
    }

    /// How many items are held in the window waiting for a gap to fill,
    /// which is not how many have passed through. An empty window is
    /// falsy, so `if not window:` works.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.ReorderWindow holding {} window={}>",
            self.inner.len(),
            self.inner.window()
        )
    }
}

/// Check an element layout against what the constructors assert, so a
/// bad one raises here instead of panicking inside Rust.
fn check_layout(element_size: usize, alignment: usize) -> PyResult<()> {
    if element_size < 1 {
        return Err(PyValueError::new_err("element_size must be at least one byte"));
    }
    if element_size > u32::MAX as usize {
        return Err(PyValueError::new_err("element_size must fit in 32 bits"));
    }
    if alignment < 1 || !alignment.is_power_of_two() {
        return Err(PyValueError::new_err("alignment must be a power of two"));
    }
    Ok(())
}

/// A last-in first-out stack in a mapped file, shared between processes.
#[pyclass(module = "subetha")]
struct Stack {
    inner: Box<RawTreiberStack>,
}

#[pymethods]
impl Stack {
    /// Obtain the stack at `path` with room for `capacity` items of
    /// `element_size` bytes, creating it when the file does not exist.
    /// A capacity below one is a `ValueError`, and so is a layout the
    /// alignment cannot satisfy.
    ///
    /// Any number of processes may push and pop at once. The order is
    /// last in, first out, so this is the wrong shape when the oldest
    /// item should be served first.
    #[new]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn new(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("capacity must be at least one"));
        }
        check_layout(element_size, alignment)?;
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawTreiberStack::create(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the stack", e))
    }

    /// Attach to a stack that already exists, raising `OSError` when it
    /// does not. Every layout argument describes the file rather than
    /// asking anything of it, so all of them must match what it was
    /// created with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn open(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("capacity must be at least one"));
        }
        check_layout(element_size, alignment)?;
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawTreiberStack::open(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the stack", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn element_size(&self) -> usize {
        self.inner.slot_size()
    }

    /// How many items are on it, read without stopping anyone, so it is
    /// a sighting rather than a promise.
    #[getter]
    fn approx_len(&self) -> usize {
        self.inner.approx_len()
    }

    /// Whether the stack looked empty when asked. Read without stopping
    /// anyone else, so it is a sighting rather than a promise: a push can
    /// land the instant after. Treat `None` from `pop` as the real
    /// answer, and use this for reporting.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Push one item. `False` means the stack is full.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        match self.inner.push(item) {
            Ok(()) => Ok(true),
            Err(StackError::Full) => Ok(false),
            Err(e) => Err(os_err("pushing", e)),
        }
    }

    /// Push a run of items in one crossing, stopping at the first that
    /// will not fit, and answer how many landed. They go on in the order
    /// given, so the last one handed in is the first one `pop` returns.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            match self.inner.push(item) {
                Ok(()) => pushed += 1,
                Err(StackError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// Take the top item, or `None` when it is empty.
    fn pop(&self) -> Option<Vec<u8>> {
        let mut out = vec![0u8; self.inner.slot_size()];
        self.inner.pop(&mut out).map(|n| {
            out.truncate(n);
            out
        })
    }

    /// Take up to `max_items` in one crossing, stopping early when the
    /// stack runs dry, newest first. An empty list means there was
    /// nothing, and nothing here raises.
    fn pop_many(&self, max_items: usize) -> Vec<Vec<u8>> {
        let mut taken = Vec::new();
        for _ in 0..max_items {
            match self.pop() {
                Some(item) => taken.push(item),
                None => break,
            }
        }
        taken
    }

    /// Look at the top without taking it. The bytes are a snapshot: a
    /// concurrent pop can retire the slot while this reads it.
    fn peek(&self) -> Option<Vec<u8>> {
        let mut out = vec![0u8; self.inner.slot_size()];
        self.inner.peek(&mut out).map(|n| {
            out.truncate(n);
            out
        })
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// writes without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Nothing is popped on the way out: whatever is on the
    /// stack stays there for whoever attaches next.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Stack about {} of {}>",
            self.inner.approx_len(),
            self.inner.capacity()
        )
    }
}

/// A work-stealing deque in a mapped file: its owner pushes and pops one
/// end, and other processes steal from the other.
#[pyclass(module = "subetha")]
struct Deque {
    inner: Box<RawDeque>,
}

#[pymethods]
impl Deque {
    /// The capacity must be a power of two, which the ring arithmetic
    /// relies on; it is checked here so a bad one reads as an argument
    /// error rather than a refusal from deeper down.
    #[new]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn new(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(PyValueError::new_err("capacity must be a power of two"));
        }
        check_layout(element_size, alignment)?;
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawDeque::create(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the deque", e))
    }

    /// Attach as a thief: this handle steals from the deque another
    /// process owns, and does not push or pop.
    ///
    /// No capacity is asked for, because the file states it and being
    /// told a second time only creates a way to be told wrong.
    #[staticmethod]
    #[pyo3(signature = (path, element_size, alignment = 1, tag = 0))]
    fn open_as_thief(path: &str, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        check_layout(element_size, alignment)?;
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawDeque::open_as_thief(path, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the deque", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn element_size(&self) -> usize {
        self.inner.layout().slot_size
    }

    #[getter]
    fn approx_len(&self) -> usize {
        self.inner.approx_len()
    }

    /// Push at the owner's end. `False` means it is full.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        match self.inner.push(item) {
            Ok(()) => Ok(true),
            Err(DequeError::Full) => Ok(false),
            Err(e) => Err(os_err("pushing", e)),
        }
    }

    /// Push a run of items in one crossing, stopping at the first that
    /// will not fit, and answer how many landed. They go on the owner's
    /// end, the end `pop` takes from and `steal` does not.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            match self.inner.push(item) {
                Ok(()) => pushed += 1,
                Err(DequeError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// Take from the owner's end, or `None`.
    fn pop(&self) -> Option<Vec<u8>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner.pop(&mut out).map(|n| {
            out.truncate(n);
            out
        })
    }

    /// Take from the other end, which is what another process does.
    /// `None` means there was nothing to take, or that another thief won
    /// the race for it.
    fn steal(&self) -> Option<Vec<u8>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner.steal(&mut out).map(|n| {
            out.truncate(n);
            out
        })
    }

    /// Steal up to `max_items` from the far end in one crossing, stopping
    /// early when there is nothing left to take. An empty list means the
    /// deque was empty or the owner won every race for the last items,
    /// which a thief cannot tell apart and does not need to.
    fn steal_many(&self, max_items: usize) -> Vec<Vec<u8>> {
        let mut taken = Vec::new();
        for _ in 0..max_items {
            match self.steal() {
                Some(item) => taken.push(item),
                None => break,
            }
        }
        taken
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// writes without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Nothing is drained on the way out: whatever is in the
    /// deque stays there for whoever attaches next, including for a
    /// thief still stealing from the other end.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Deque about {} of {}>",
            self.inner.approx_len(),
            self.inner.capacity()
        )
    }
}

pyo3::create_exception!(
    subetha,
    Lagged,
    pyo3::exceptions::PyException,
    "A subscriber fell so far behind that what it asked for had already \
     been overwritten. Raised rather than answered with `None`, because \
     losing items is not the same as having none yet."
);

/// A publish/subscribe ring: one publisher, any number of subscribers,
/// none of which hold the publisher up.
///
/// The ring keeps the last N items and wraps, so a subscriber that falls
/// behind loses the items it missed. That loss is reported as `Lagged`
/// rather than as an empty read, because the two mean opposite things.
#[pyclass(module = "subetha", frozen)]
struct PubSub {
    inner: Box<PubSubRing>,
}

#[pymethods]
impl PubSub {
    /// Obtain the pubsub ring at `path` holding `capacity` items,
    /// creating it when the file does not exist.
    ///
    /// Creating it subscribes nobody. A subscriber starts from where the
    /// ring is when it subscribes, so anything published before that is
    /// not waiting for it.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        PubSubRing::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the pubsub ring", e))
    }

    /// Attach to a pubsub ring that already exists, raising `OSError`
    /// when it does not. `capacity` must be the one it was created with.
    /// Attaching does not subscribe: take a subscription of your own.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        PubSubRing::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the pubsub ring", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// The position the publisher has reached. A subscriber at this
    /// position has seen everything.
    #[getter]
    fn head(&self) -> u64 {
        self.inner.head()
    }

    #[getter]
    fn payload_size(&self) -> usize {
        PUBSUB_PAYLOAD_BYTES
    }

    /// Publish one item and return the position it landed at. The
    /// publisher never waits for a subscriber.
    fn publish(&self, item: &[u8]) -> PyResult<u64> {
        if item.len() > PUBSUB_PAYLOAD_BYTES {
            return Err(PyValueError::new_err(format!(
                "an item of {} bytes does not fit a {PUBSUB_PAYLOAD_BYTES}-byte slot",
                item.len()
            )));
        }
        Ok(self.inner.publish(item))
    }

    /// Publish a run of items in one crossing, returning the position of
    /// the last.
    fn publish_many(&self, items: Vec<Vec<u8>>) -> PyResult<Option<u64>> {
        let mut last = None;
        for item in &items {
            if item.len() > PUBSUB_PAYLOAD_BYTES {
                return Err(PyValueError::new_err(format!(
                    "an item of {} bytes does not fit a {PUBSUB_PAYLOAD_BYTES}-byte slot",
                    item.len()
                )));
            }
            last = Some(self.inner.publish(item));
        }
        Ok(last)
    }

    /// Read the item at `position`, `None` when nothing has been
    /// published there yet, and `Lagged` when it has already been
    /// overwritten.
    fn read_at(&self, position: u64) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; PUBSUB_PAYLOAD_BYTES];
        match self.inner.read_at(position, &mut out) {
            Ok(()) => Ok(Some(out)),
            Err(PubSubReadError::Pending) => Ok(None),
            Err(PubSubReadError::Lost) => Err(Lagged::new_err(format!(
                "position {position} has been overwritten; the publisher is at {}",
                self.inner.head()
            ))),
        }
    }

    /// A subscriber starting where the publisher is now, so it sees what
    /// follows and nothing that came before.
    fn subscribe(slf: Bound<'_, Self>) -> Subscriber {
        let position = slf.get().inner.head();
        Subscriber { ring: slf.unbind(), position }
    }

    /// A subscriber starting at `position`, for one replaying from a
    /// place it recorded earlier.
    fn subscribe_from(slf: Bound<'_, Self>, position: u64) -> Subscriber {
        Subscriber { ring: slf.unbind(), position }
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.PubSub head={} capacity={}>",
            self.inner.head(),
            self.inner.capacity()
        )
    }
}

/// One subscriber's place in a `PubSub` ring, which it advances as it
/// reads.
#[pyclass(module = "subetha")]
struct Subscriber {
    ring: Py<PubSub>,
    position: u64,
}

#[pymethods]
impl Subscriber {
    /// Where this subscriber has read up to.
    #[getter]
    fn position(&self) -> u64 {
        self.position
    }

    /// How far behind the publisher this subscriber is.
    fn lag(&self, py: Python<'_>) -> u64 {
        self.ring.bind(py).get().inner.head().saturating_sub(self.position)
    }

    /// The next item, or `None` when this subscriber has caught up.
    /// Raises `Lagged` when the next item was overwritten before it was
    /// read.
    fn next(&mut self, py: Python<'_>) -> PyResult<Option<Vec<u8>>> {
        let ring = self.ring.bind(py).get();
        let mut out = vec![0u8; PUBSUB_PAYLOAD_BYTES];
        match ring.inner.read_at(self.position, &mut out) {
            Ok(()) => {
                self.position += 1;
                Ok(Some(out))
            }
            Err(PubSubReadError::Pending) => Ok(None),
            Err(PubSubReadError::Lost) => {
                let head = ring.inner.head();
                let missed = head.saturating_sub(self.position);
                self.position = head;
                Err(Lagged::new_err(format!(
                    "fell behind by {missed} items; skipped to {head}"
                )))
            }
        }
    }

    /// Up to `max_items` in one crossing, stopping when caught up.
    fn next_many(&mut self, py: Python<'_>, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let ring = self.ring.bind(py).get();
        let mut taken = Vec::new();
        let mut out = vec![0u8; PUBSUB_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match ring.inner.read_at(self.position, &mut out) {
                Ok(()) => {
                    self.position += 1;
                    taken.push(out.clone());
                }
                Err(PubSubReadError::Pending) => break,
                Err(PubSubReadError::Lost) => {
                    let head = ring.inner.head();
                    let missed = head.saturating_sub(self.position);
                    self.position = head;
                    return Err(Lagged::new_err(format!(
                        "fell behind by {missed} items; skipped to {head}"
                    )));
                }
            }
        }
        Ok(taken)
    }

    fn __repr__(&self) -> String {
        format!("<subetha.Subscriber at {}>", self.position)
    }
}

/// The producer end of a Lamport pair: one writer, one reader, sharing a
/// ring in a mapped file.
///
/// The pair is handed out together by `lamport_pair`, because the whole
/// point is that exactly one of each exists.
#[pyclass(module = "subetha", unsendable)]
struct LamportProducer {
    inner: Box<Producer>,
}

#[pymethods]
impl LamportProducer {
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn payload_size(&self) -> usize {
        SPSC_PAYLOAD_BYTES
    }

    /// Push one item. `False` means the ring is full and the consumer has
    /// not caught up, which is an answer rather than a failure; an item
    /// longer than `payload_size` raises.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("pushing", e)),
        }
    }

    /// Push a run of items in one crossing, stopping at the first that
    /// will not fit, and answer how many landed. A count short of what
    /// was handed in leaves the rest with the caller.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            match self.inner.try_push(item) {
                Ok(()) => pushed += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// Push items cut out of one buffer, `item_len` bytes each, and
    /// answer how many landed.
    ///
    /// This is the cheapest shape on this class: the caller hands over
    /// one object, and no Python object is built per item on either side.
    /// A trailing piece shorter than `item_len` is pushed as it is. Stops
    /// at the first that will not fit, and an `item_len` of zero is a
    /// `ValueError`.
    fn push_buffer(&self, data: &[u8], item_len: usize) -> PyResult<usize> {
        if item_len == 0 {
            return Err(PyValueError::new_err("item_len must not be zero"));
        }
        let mut pushed = 0;
        for chunk in data.chunks(item_len) {
            match self.inner.try_push(chunk) {
                Ok(()) => pushed += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    fn __repr__(&self) -> String {
        format!("<subetha.LamportProducer capacity={}>", self.inner.capacity())
    }
}

/// The consumer end of a Lamport pair.
#[pyclass(module = "subetha", unsendable)]
struct LamportConsumer {
    inner: Box<Consumer>,
}

#[pymethods]
impl LamportConsumer {
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Take the next item, or `None` when the ring is empty. Items arrive
    /// in the order the producer sent them, because there is exactly one
    /// producer.
    fn pop(&self) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.inner.try_pop(&mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("popping", e)),
        }
    }

    /// Take up to `max_items` in one crossing, stopping early when the
    /// ring runs dry. An empty list means there was nothing, which is the
    /// same answer `pop` gives as `None`.
    fn pop_many(&self, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_pop(&mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("popping", e)),
            }
        }
        Ok(taken)
    }

    fn __repr__(&self) -> String {
        format!("<subetha.LamportConsumer capacity={}>", self.inner.capacity())
    }
}

/// Make a Lamport pair at `path`: one producer and one consumer over one
/// ring.
///
/// Exactly one of each may exist per ring. Within a process the types
/// enforce that; across processes it is the caller's undertaking, which
/// is why the pair is handed out rather than opened twice.
#[pyfunction]
fn lamport_pair(path: &str, capacity: usize) -> PyResult<(LamportProducer, LamportConsumer)> {
    let (p, c) = SharedRingSpsc::create_pair(path, capacity)
        .map_err(|e| os_err("building the pair", e))?;
    Ok((
        LamportProducer { inner: Box::new(p) },
        LamportConsumer { inner: Box::new(c) },
    ))
}

/// Attach to a Lamport ring that already exists.
#[pyfunction]
fn lamport_pair_open(path: &str, capacity: usize) -> PyResult<(LamportProducer, LamportConsumer)> {
    let (p, c) = SharedRingSpsc::open_pair(path, capacity)
        .map_err(|e| os_err("attaching to the pair", e))?;
    Ok((
        LamportProducer { inner: Box::new(p) },
        LamportConsumer { inner: Box::new(c) },
    ))
}

/// A pool of fixed-size blocks in a mapped file, for payloads too large
/// to sit in a ring slot.
///
/// A producer takes a block, writes the payload into it, and puts the
/// block's index in the ring; the consumer reads the block and frees it.
/// The allocator is safe for many producers taking and many consumers
/// freeing at once.
#[pyclass(module = "subetha")]
struct FrameRegion {
    inner: Box<SubetnaFrameRegion>,
}

#[pymethods]
impl FrameRegion {
    /// Obtain the frame region at `path` holding `block_count` blocks of
    /// `block_size` bytes, creating it when the file does not exist.
    ///
    /// Blocks are addressed rather than queued: every one exists from
    /// creation and any index below `block_count` can be written or read
    /// straight away. The size is a whole block, so a shorter payload
    /// leaves the rest of its block as it was.
    #[new]
    fn new(path: &str, block_size: usize, block_count: usize) -> PyResult<Self> {
        SubetnaFrameRegion::create(path, block_size, block_count)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the frame region", e))
    }

    /// Attach to a frame region that already exists, raising `OSError`
    /// when it does not. The block size and count must be the ones it was
    /// created with: they are the layout, so a mismatch reads the blocks
    /// at the wrong offsets.
    #[staticmethod]
    fn open(path: &str, block_size: usize, block_count: usize) -> PyResult<Self> {
        SubetnaFrameRegion::open(path, block_size, block_count)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the frame region", e))
    }

    #[getter]
    fn block_size(&self) -> usize {
        self.inner.block_size()
    }

    #[getter]
    fn block_count(&self) -> usize {
        self.inner.block_count()
    }

    /// Take a block, or `None` when every block is in use.
    fn allocate(&self) -> Option<u32> {
        self.inner.alloc()
    }

    /// Give a block back. Freeing one nobody holds corrupts the pool, so
    /// only free what this process allocated and has finished with.
    fn free(&self, index: u32) {
        self.inner.free(index);
    }

    /// Take a block and write `payload` into it in one call, or `None`
    /// when the pool is exhausted. This is the shape a producer wants:
    /// one crossing rather than an allocate and a write.
    fn write_new(&self, payload: &[u8]) -> PyResult<Option<u32>> {
        if payload.len() > self.inner.block_size() {
            return Err(PyValueError::new_err(
                "the payload is longer than a block",
            ));
        }
        match self.inner.alloc() {
            Some(index) => {
                self.inner.write_block(index, payload);
                Ok(Some(index))
            }
            None => Ok(None),
        }
    }

    /// Write `payload` into block `index`, checking first that it fits.
    ///
    /// A payload longer than `block_size` is a `ValueError` rather than a
    /// truncated write, so a caller cannot lose the tail of a frame
    /// without being told. A shorter one leaves the rest of the block as
    /// it was, so a reader has to know how much of the block is its own.
    fn write_block(&self, index: u32, payload: &[u8]) -> PyResult<()> {
        if payload.len() > self.inner.block_size() {
            return Err(PyValueError::new_err(
                "the payload is longer than a block",
            ));
        }
        self.inner.write_block(index, payload);
        Ok(())
    }

    /// Read `length` bytes from a block.
    fn read_block(&self, index: u32, length: usize) -> PyResult<Vec<u8>> {
        if length > self.inner.block_size() {
            return Err(PyValueError::new_err(
                "the length asked for is longer than a block",
            ));
        }
        let mut out = vec![0u8; length];
        let read = self.inner.read_block(index, length, &mut out);
        out.truncate(read);
        Ok(out)
    }

    /// Read a block and give it back in one call, which is the shape a
    /// consumer wants.
    fn take_block(&self, index: u32, length: usize) -> PyResult<Vec<u8>> {
        let out = self.read_block(index, length)?;
        self.inner.free(index);
        Ok(out)
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The blocks stay as they were written for whoever attaches
    /// next.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.FrameRegion {} blocks of {} bytes>",
            self.inner.block_count(),
            self.inner.block_size()
        )
    }
}

/// A reader-writer lock in a mapped file, held across processes.
///
/// The C ABI hands a caller a 64-bit token and trusts it to give the
/// token back. Python has a better answer for the same problem, so this
/// does not mirror that: `read()` and `write()` return an object that
/// releases when its `with` block ends, on the way out of an exception
/// as well as on the way out of a return.
/// Frozen because every method takes `&self`, which is what lets a hold
/// reach the lock through `get` without borrowing it from the
/// interpreter, including from `Drop`.
/// The lock is held through the parking wrapper, which is the same lock
/// file with two small ones beside it carrying the wakeups. `inner`
/// reaches the lock itself, so the waiting forms that spin and the ones
/// that sleep are two ways at one lock rather than two locks.
#[pyclass(module = "subetha", frozen)]
struct RWLock {
    parked: Box<BlockingRWLock>,
}

impl RWLock {
    /// The lock underneath the parking wrapper.
    fn inner(&self) -> &SharedRWLock {
        self.parked.inner()
    }
}

#[pymethods]
impl RWLock {
    /// Obtain the lock at `path`, creating it when the file does not
    /// exist and attaching to the live one when it does.
    ///
    /// Creating it takes nothing. A hold comes from `read` or `write`,
    /// and belongs to the thread that took it.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        BlockingRWLock::create(path)
            .map(|parked| Self { parked: Box::new(parked) })
            .map_err(|e| PyOSError::new_err(format!("opening the lock: {e:?}")))
    }

    /// Attach to a lock that already exists, raising `OSError` when it
    /// does not. Attaching takes nothing: `read` and `write` are what
    /// take a hold.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        BlockingRWLock::open(path)
            .map(|parked| Self { parked: Box::new(parked) })
            .map_err(|e| PyOSError::new_err(format!("attaching to the lock: {e:?}")))
    }

    /// How many readers hold it right now.
    #[getter]
    fn readers(&self) -> u32 {
        self.inner().reader_count()
    }

    /// Take the read hold, waiting for it. Other readers may hold it at
    /// the same time; a writer may not.
    ///
    /// The interpreter is detached while this waits, so other Python
    /// threads keep running rather than being held behind a lock this
    /// thread is only queueing for.
    fn read(slf: Bound<'_, Self>, py: Python<'_>) -> Hold {
        let owner = slf.clone().unbind();
        // Only the lock itself crosses into the detached closure: a
        // `Bound` carries an interpreter marker and cannot. The guard
        // borrows the lock and the hold outlives this call, so it is
        // forgotten on purpose and `Hold` releases instead, which is what
        // the C ABI does with the same guards.
        let lock: &SharedRWLock = slf.get().inner();
        py.detach(|| {
            let guard = lock.read_lock();
            std::mem::forget(guard);
        });
        Hold { owner, write: false, released: false }
    }

    /// Take the read hold, giving up after `timeout` seconds and
    /// answering None.
    ///
    /// This one sleeps rather than spinning, so a long wait costs no
    /// processor. It never waits past the deadline even if whoever holds
    /// the lock never says it has finished, which is why the waiting
    /// forms here are the bounded ones only.
    fn read_for(slf: Bound<'_, Self>, py: Python<'_>, timeout: f64) -> PyResult<Option<Hold>> {
        let owner = slf.clone().unbind();
        let wait = duration_from(timeout)?;
        let parked: &BlockingRWLock = &slf.get().parked;
        match py.detach(|| parked.read_park_timeout(wait)) {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(Hold { owner, write: false, released: false }))
            }
            Err(BlockingRWLockError::Timeout) => Ok(None),
            Err(e) => Err(PyOSError::new_err(format!("taking the read hold: {e:?}"))),
        }
    }

    /// Take the write hold, giving up after `timeout` seconds and
    /// answering None.
    fn write_for(slf: Bound<'_, Self>, py: Python<'_>, timeout: f64) -> PyResult<Option<Hold>> {
        let owner = slf.clone().unbind();
        let wait = duration_from(timeout)?;
        let parked: &BlockingRWLock = &slf.get().parked;
        match py.detach(|| parked.write_park_timeout(wait)) {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(Hold { owner, write: true, released: false }))
            }
            Err(BlockingRWLockError::Timeout) => Ok(None),
            Err(e) => Err(PyOSError::new_err(format!("taking the write hold: {e:?}"))),
        }
    }

    /// Take the read hold if it is free, or answer `None`. Only a
    /// contended lock answers `None`; anything else raises, so a broken
    /// lock never reads as a busy one.
    fn try_read(slf: Bound<'_, Self>) -> PyResult<Option<Hold>> {
        let owner = slf.clone().unbind();
        match slf.get().inner().try_read_lock() {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(Hold { owner, write: false, released: false }))
            }
            Err(RWLockError::WouldBlock) => Ok(None),
            Err(e) => Err(os_err("taking the read hold", e)),
        }
    }

    /// Take the write hold, waiting for it. Nobody else holds it while
    /// this does.
    fn write(slf: Bound<'_, Self>, py: Python<'_>) -> Hold {
        let owner = slf.clone().unbind();
        let lock: &SharedRWLock = slf.get().inner();
        py.detach(|| {
            let guard = lock.write_lock();
            std::mem::forget(guard);
        });
        Hold { owner, write: true, released: false }
    }

    /// Take the write hold if it is free, or answer `None`.
    fn try_write(slf: Bound<'_, Self>) -> PyResult<Option<Hold>> {
        let owner = slf.clone().unbind();
        match slf.get().inner().try_write_lock() {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(Hold { owner, write: true, released: false }))
            }
            Err(RWLockError::WouldBlock) => Ok(None),
            Err(e) => Err(os_err("taking the write hold", e)),
        }
    }

    fn __repr__(&self) -> String {
        format!("<subetha.RWLock readers={}>", self.inner().reader_count())
    }
}

/// A hold on a lock, given back when its `with` block ends.
///
/// It keeps the lock object alive, so the box it points into outlives
/// it, and it gives the hold back when dropped even if nobody wrote a
/// `with` block. A hold belongs to the thread that took it, which is
/// why this is unsendable rather than pretending otherwise.
#[pyclass(module = "subetha", unsendable)]
struct Hold {
    /// The lock this came from, kept alive for as long as the hold is.
    owner: Py<RWLock>,
    write: bool,
    released: bool,
}

#[pymethods]
impl Hold {
    /// Answer the same object, which is why a hold is normally taken as
    /// `with lock.write() as held:` rather than bound to a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Give the lock back, and let an exception through.
    ///
    /// This one really does release, and releasing on the way out is the
    /// point of taking a hold in a `with` block: an exception inside it
    /// still gives the lock back, where a hold left to be collected keeps
    /// every other process waiting until then. The hold belongs to the
    /// thread that took it, so it must be given back on that thread.
    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.give_back();
        false
    }

    /// Give the hold back now rather than at the end of a block. Calling
    /// it twice is harmless.
    fn release(&mut self) {
        self.give_back();
    }

    #[getter]
    fn held(&self) -> bool {
        !self.released
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Hold {} {}>",
            if self.write { "write" } else { "read" },
            if self.released { "released" } else { "held" }
        )
    }
}

impl Hold {
    fn give_back(&mut self) {
        if self.released {
            return;
        }
        // `attach` costs nothing when the interpreter is already held,
        // which it is on every path here including `Drop`, where Python
        // is the one destroying this object.
        Python::attach(|py| {
            let lock = self.owner.bind(py).get();
            if self.write {
                lock.inner().release_write_for_blocking();
            } else {
                lock.inner().release_read_for_blocking();
            }
            // The hold was taken without keeping the guard that would
            // do this on the way out, so it is done here. Without it a
            // thread waiting in `read_for` or `write_for` sleeps to its
            // deadline with the lock already free.
            lock.parked.signal_unlock();
        });
        self.released = true;
    }
}

impl Drop for Hold {
    /// A hold nobody released is released here, so forgetting a `with`
    /// block costs nothing worse than a later release.
    fn drop(&mut self) {
        self.give_back();
    }
}

pyo3::create_exception!(
    subetha,
    Contended,
    pyo3::exceptions::PyException,
    "Another live process holds the lease and its grace period has not \
     run out, so this process cannot take it."
);

/// The most a lease's value can be. The lease's own region gives 48
/// bytes to the value; four of them record how long the value is, so
/// that a value ending in zero bytes comes back as it went in.
const LEASE_VALUE_BYTES: usize = 44;

/// What the families holding a fixed-size value put in one of their
/// slots: a length and the bytes.
///
/// These families are generic over any value a Rust caller can copy,
/// and Python has no such type to offer. Bytes are the one thing every
/// caller has, and the length travels with them because a slot is a
/// fixed size: without it a value ending in zero bytes would come back
/// indistinguishable from a shorter one padded out.
#[repr(C)]
#[derive(Clone, Copy)]
struct Payload<const N: usize> {
    len: u32,
    bytes: [u8; N],
}

impl<const N: usize> Default for Payload<N> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<const N: usize> Payload<N> {
    fn empty() -> Self {
        Self { len: 0, bytes: [0; N] }
    }

    fn from_bytes(value: &[u8]) -> PyResult<Self> {
        if value.len() > N {
            return Err(PyValueError::new_err(format!(
                "a value here is at most {N} bytes, got {}",
                value.len()
            )));
        }
        let mut held = Self::empty();
        held.len = value.len() as u32;
        held.bytes[..value.len()].copy_from_slice(value);
        Ok(held)
    }

    fn as_bytes(&self) -> Vec<u8> {
        let len = (self.len as usize).min(N);
        self.bytes[..len].to_vec()
    }
}

/// A payload travels between processes as its length and then its
/// bytes, little-endian, which is the same in every address space.
///
/// # Safety
///
/// `marshal` writes exactly `4 + N` bytes and `unmarshal` reads
/// exactly that many. There are no pointers, handles or descriptors in
/// it: a length and a run of bytes mean the same thing wherever they
/// are read. A length larger than `N` cannot name real bytes, so it is
/// refused as an invalid encoding rather than trusted.
unsafe impl<const N: usize> subetha_core::Marshal for Payload<N> {
    const PAYLOAD_BYTES: usize = 4 + N;

    fn marshal(&self, dst: &mut [u8]) {
        dst[..4].copy_from_slice(&self.len.to_le_bytes());
        dst[4..4 + N].copy_from_slice(&self.bytes);
    }

    fn unmarshal(src: &[u8]) -> Result<Self, subetha_core::MarshalError> {
        if src.len() < 4 + N {
            return Err(subetha_core::MarshalError::ShortBuffer {
                expected: 4 + N,
                got: src.len(),
            });
        }
        let len = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
        if len as usize > N {
            return Err(subetha_core::MarshalError::InvalidEncoding);
        }
        let mut held = Self::empty();
        held.len = len;
        held.bytes.copy_from_slice(&src[4..4 + N]);
        Ok(held)
    }
}

/// What a lease holds, sized to the lease's own region.
type LeaseValue = Payload<LEASE_VALUE_BYTES>;

/// The most a value can be in the families whose slot is 56 bytes,
/// four of which record the length.
const SLOT_VALUE_BYTES: usize = 52;

/// What those families hold.
type SlotValue = Payload<SLOT_VALUE_BYTES>;

/// One slot's versions, newest first, each as its value and the epochs
/// it was born at and died at. A died of `None` is the current version.
type SlotHistory = Vec<(Vec<u8>, u64, Option<u64>)>;

/// The entries a scan walked, as key and value, and the key to carry on
/// from, which is `None` when the walk ran out of entries.
type ScanPage = (Vec<(u64, u64)>, Option<u64>);

/// One process at a time owns a small shared value, and if that process
/// dies another takes it over rather than the value being stranded.
///
/// Ownership is by process id, and there are two ways to get it, which
/// a caller has to know apart.
///
/// The first is that a lower process id takes the lease from a higher
/// one on the spot, whether or not that owner is alive or has just
/// beaten. This is what settles which process leads when several start
/// at once and all want the same job, and it is deliberate: the answer
/// is the same whoever asks, so the processes agree without talking. It
/// also means a claim is not a lock against every other process, only
/// against the ones with higher ids.
///
/// The second is the takeover of a quiet owner. A live owner says it is
/// still here by calling `beat`. Time is counted in epochs the holders
/// step themselves with `tick_epoch`, not in seconds: an owner that has
/// not beaten for more than `grace_epochs` of them is treated as gone
/// and any process may take over. Nothing steps the epoch on its own, so
/// a lease whose holders never call `tick_epoch` never expires this way.
///
/// Taking a lease this process already holds succeeds and changes
/// nothing, so a claim is safe to repeat.
///
/// The value is at most 44 bytes. It is a token saying who is doing what,
/// not a place to put data; a `Region` is that.
#[pyclass(module = "subetha")]
struct OwnerLease {
    inner: Box<SubethaOwnerLease<LeaseValue>>,
}

#[pymethods]
impl OwnerLease {
    /// Take the lease at `path`, creating it with `value` when it is not
    /// there yet. Attaching to one that exists leaves its owner, its
    /// term and its value alone, and `value` is then unused.
    #[new]
    #[pyo3(signature = (path, value = None))]
    fn new(path: &str, value: Option<&[u8]>) -> PyResult<Self> {
        let initial = LeaseValue::from_bytes(value.unwrap_or(&[]))?;
        SubethaOwnerLease::create(path, initial)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| lease_err("opening the lease", e))
    }

    /// Attach to a lease that already exists, failing if it does not.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        SubethaOwnerLease::open(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| lease_err("attaching to the lease", e))
    }

    /// Strip the lease at `path` back to no owner and this value. This
    /// throws away a claim another process may still believe it has, so
    /// it is for a lease known to be wedged rather than for ordinary use.
    #[staticmethod]
    #[pyo3(signature = (path, value = None))]
    fn reset(path: &str, value: Option<&[u8]>) -> PyResult<Self> {
        let initial = LeaseValue::from_bytes(value.unwrap_or(&[]))?;
        SubethaOwnerLease::reset(path, initial)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| lease_err("resetting the lease", e))
    }

    /// Take the lease, answering False when a process with a lower id
    /// holds it and has beaten within `grace_epochs`.
    ///
    /// It answers True when nobody holds it, when this process already
    /// does, when the holder's id is higher than this one's, or when the
    /// holder has been quiet for longer than the grace period.
    ///
    /// `pid` names the process claiming it, and defaults to this one.
    /// Pass another only to stand in for a process that is not here,
    /// which is what a test of the takeover does.
    #[pyo3(signature = (grace_epochs = 0, pid = None))]
    fn try_acquire(&self, grace_epochs: u64, pid: Option<u32>) -> bool {
        self.inner.try_acquire(pid.unwrap_or_else(std::process::id), grace_epochs)
    }

    /// Give the lease back, answering False if this process did not hold
    /// it.
    #[pyo3(signature = (pid = None))]
    fn release(&self, pid: Option<u32>) -> bool {
        self.inner.release(pid.unwrap_or_else(std::process::id))
    }

    /// Take the lease for the length of a block, giving it back when the
    /// block ends including on the way out of an exception.
    ///
    /// Raises `Contended` when a process with a lower id holds it and
    /// has beaten within the grace period, because a block has to have
    /// something to enter. `try_acquire` is the same question asked
    /// without raising.
    #[pyo3(signature = (grace_epochs = 0, pid = None))]
    fn hold(slf: Bound<'_, Self>, grace_epochs: u64, pid: Option<u32>) -> PyResult<LeaseHold> {
        let claimant = pid.unwrap_or_else(std::process::id);
        if !slf.borrow().inner.try_acquire(claimant, grace_epochs) {
            let holder = slf.borrow().inner.current_owner();
            return Err(Contended::new_err(match holder {
                Some(other) => format!("process {other} holds this lease"),
                None => "the lease could not be taken".to_string(),
            }));
        }
        Ok(LeaseHold {
            owner: slf.unbind(),
            pid: claimant,
            released: false,
        })
    }

    /// The value, readable only by the process holding the lease. None
    /// when this process does not hold it.
    #[pyo3(signature = (pid = None))]
    fn read(&self, pid: Option<u32>) -> Option<Vec<u8>> {
        self.inner
            .read_as_owner(pid.unwrap_or_else(std::process::id))
            .map(|held| held.as_bytes())
    }

    /// Write the value, answering False when this process does not hold
    /// the lease.
    #[pyo3(signature = (value, pid = None))]
    fn write(&self, value: &[u8], pid: Option<u32>) -> PyResult<bool> {
        let held = LeaseValue::from_bytes(value)?;
        Ok(self
            .inner
            .write_as_owner(pid.unwrap_or_else(std::process::id), held))
    }

    /// Say this process is still here, so its claim does not lapse.
    /// Answers False once it is no longer the owner.
    #[pyo3(signature = (pid = None))]
    fn beat(&self, pid: Option<u32>) -> bool {
        self.inner.beat(pid.unwrap_or_else(std::process::id))
    }

    /// Step the epoch every holder measures the grace period in, and
    /// give back the epoch this reached. Nothing steps it on its own.
    fn tick_epoch(&self) -> u64 {
        self.inner.tick_epoch()
    }

    /// The process holding the lease, or None when nobody does.
    #[getter]
    fn owner(&self) -> Option<u32> {
        self.inner.current_owner()
    }

    /// Whether this process holds it.
    #[pyo3(signature = (pid = None))]
    fn held_by_me(&self, pid: Option<u32>) -> bool {
        self.inner.am_i_owner(pid.unwrap_or_else(std::process::id))
    }

    /// Steps every time the lease changes hands, so a holder can tell a
    /// takeover from an uninterrupted claim.
    #[getter]
    fn term(&self) -> u32 {
        self.inner.lease_term()
    }

    /// Put the lease's file on the disk, and wait for it.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| lease_err("flushing", e))
    }

    /// Ask for the lease's file to reach the disk without waiting.
    fn flush_async(&self) -> PyResult<()> {
        self.inner
            .flush_async()
            .map_err(|e| lease_err("flushing", e))
    }

    /// The most a value may be, in bytes.
    #[classattr]
    fn max_value_bytes() -> usize {
        LEASE_VALUE_BYTES
    }

    fn __repr__(&self) -> String {
        match self.inner.current_owner() {
            Some(pid) => format!("<subetha.OwnerLease held by {pid} term={}>", self.inner.lease_term()),
            None => "<subetha.OwnerLease unheld>".to_string(),
        }
    }
}

fn lease_err(doing: &str, e: LeaseError) -> PyErr {
    match e {
        LeaseError::IoError(kind) => {
            PyOSError::new_err(format!("{doing}: {}", std::io::Error::from(kind)))
        }
        LeaseError::LayoutMismatch => PyValueError::new_err(format!(
            "{doing}: the file is a lease of another shape"
        )),
        LeaseError::PayloadTooLarge => {
            PyValueError::new_err(format!("{doing}: the value does not fit a lease"))
        }
        LeaseError::NotOwner => {
            PyOSError::new_err(format!("{doing}: this process does not hold the lease"))
        }
        LeaseError::Contention => Contended::new_err(format!("{doing}: the lease is contended")),
    }
}

/// A lease held for the length of a block. It gives the lease back when
/// the block ends, and when dropped even if nobody wrote a block.
#[pyclass(module = "subetha", unsendable)]
struct LeaseHold {
    /// The lease this came from, kept alive for as long as the hold is.
    owner: Py<OwnerLease>,
    pid: u32,
    released: bool,
}

#[pymethods]
impl LeaseHold {
    /// Answer the same object, which is why a hold is normally taken as
    /// `with lease.acquire() as held:` rather than bound to a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Give the lease back, and let an exception through.
    ///
    /// This one really does release, and that is the point of taking it
    /// in a `with` block: an exception inside still hands the lease on,
    /// where a hold left to be collected keeps the next owner waiting for
    /// the lease to expire instead.
    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.give_back();
        false
    }

    /// The value under the lease this hold has.
    fn read(&self, py: Python<'_>) -> Option<Vec<u8>> {
        self.owner
            .bind(py)
            .borrow()
            .inner
            .read_as_owner(self.pid)
            .map(|held| held.as_bytes())
    }

    /// Write the value under the lease this hold has.
    fn write(&self, py: Python<'_>, value: &[u8]) -> PyResult<bool> {
        let held = LeaseValue::from_bytes(value)?;
        Ok(self
            .owner
            .bind(py)
            .borrow()
            .inner
            .write_as_owner(self.pid, held))
    }

    /// Say this process is still here, so the claim does not lapse
    /// during a long block.
    fn beat(&self, py: Python<'_>) -> bool {
        self.owner.bind(py).borrow().inner.beat(self.pid)
    }

    /// Give the lease back now rather than at the end of a block.
    /// Calling it twice is harmless.
    fn release(&mut self) {
        self.give_back();
    }

    #[getter]
    fn held(&self) -> bool {
        !self.released
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.LeaseHold pid={} {}>",
            self.pid,
            if self.released { "released" } else { "held" }
        )
    }
}

impl LeaseHold {
    fn give_back(&mut self) {
        if self.released {
            return;
        }
        Python::attach(|py| {
            self.owner.bind(py).borrow().inner.release(self.pid);
        });
        self.released = true;
    }
}

impl Drop for LeaseHold {
    /// A lease nobody gave back is given back here, so forgetting a
    /// `with` block does not strand it until the grace period.
    fn drop(&mut self) {
        self.give_back();
    }
}

/// A fixed number of items kept from a stream of any length, each one
/// as likely to be kept as any other.
///
/// Every value offered has the same chance of being in the sample, and
/// the sample never grows past `capacity`, so a stream of any size costs
/// the same memory. This is the shape for keeping an unbiased handful of
/// a firehose: a sample of requests, of errors, of anything there is too
/// much of to keep.
#[pyclass(module = "subetha")]
struct Reservoir {
    inner: Box<SharedReservoirSampler<SlotValue>>,
}

#[pymethods]
impl Reservoir {
    /// How many items the sample holds at most. A value is at most 52
    /// bytes.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("a reservoir holds at least one item"));
        }
        SharedReservoirSampler::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the reservoir", e))
    }

    /// Attach to a reservoir another holder made. The capacity must be
    /// the one it was made with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedReservoirSampler::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the reservoir", e))
    }

    /// Offer one value. Answers the place it was kept in, or None when
    /// the sample kept what it already had there instead. Neither answer
    /// is a failure: refusing is how the sample stays unbiased.
    fn record(&self, value: &[u8]) -> PyResult<Option<usize>> {
        let held = SlotValue::from_bytes(value)?;
        Ok(self.inner.record(held))
    }

    /// Offer a run of values in one crossing, and say how many were
    /// kept.
    fn record_many(&self, values: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut kept = 0;
        for value in &values {
            let held = SlotValue::from_bytes(value)?;
            if self.inner.record(held).is_some() {
                kept += 1;
            }
        }
        Ok(kept)
    }

    /// Everything in the sample right now, in one crossing.
    fn snapshot(&self) -> Vec<Vec<u8>> {
        self.inner
            .snapshot()
            .iter()
            .map(|held| held.as_bytes())
            .collect()
    }

    /// How many values have been offered, which is not how many are
    /// kept. The ratio of the two is what a count drawn from the sample
    /// has to be scaled by.
    #[getter]
    fn total_seen(&self) -> u64 {
        self.inner.total_seen()
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Empty the sample and forget how much has been offered.
    fn reset(&self) {
        self.inner.reset();
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// sample without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Start writing the mapping back to its file and return at once,
    /// without waiting for the write to land. Nothing is on disk yet when
    /// this returns; `flush` is the form that waits.
    fn flush_async(&self) -> PyResult<()> {
        self.inner.flush_async().map_err(|e| os_err("flushing", e))
    }

    /// The most a value may be, in bytes.
    #[classattr]
    fn max_value_bytes() -> usize {
        SLOT_VALUE_BYTES
    }

    /// How many values are held in the sample right now, which is at most
    /// the capacity and is not `total_seen`. A reservoir that has been
    /// offered a million values still holds only its capacity.
    fn __len__(&self) -> usize {
        self.inner.snapshot().len()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The sample and the total seen both stay for whoever
    /// attaches next; `reset` is what empties a reservoir.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Reservoir {} of {} seen {}>",
            self.inner.snapshot().len(),
            self.inner.capacity(),
            self.inner.total_seen()
        )
    }
}

/// A set that answers whether something has been added, where the
/// bits for one item are kept together in a single cache line.
///
/// Like any bloom filter it can say yes about something never added, and
/// never says no about something that was. What is different here is the
/// layout: an ordinary bloom filter touches scattered bits across the
/// whole filter, so one lookup is several cache misses, while this one
/// puts every bit for an item in one 64-byte block and pays for a single
/// miss. Nothing can be removed; `clear` empties the whole filter.
#[pyclass(module = "subetha")]
struct BlockedBloomFilter {
    inner: Box<SharedBlockedBloomFilter>,
}

#[pymethods]
impl BlockedBloomFilter {
    /// `bits` is the size of the filter and `hashes` how many bits each
    /// item sets. `suggest` works both out from the number of items and
    /// the rate of wrong yeses that can be lived with.
    #[new]
    fn new(path: &str, bits: usize, hashes: u32) -> PyResult<Self> {
        SharedBlockedBloomFilter::create(path, bits, hashes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the filter", e))
    }

    /// Attach to a filter another holder made, with the size and hash
    /// count it was made with.
    #[staticmethod]
    fn open(path: &str, bits: usize, hashes: u32) -> PyResult<Self> {
        SharedBlockedBloomFilter::open(path, bits, hashes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the filter", e))
    }

    /// Empty the filter and remake it at this size, throwing away
    /// everything in it.
    #[staticmethod]
    fn reset(path: &str, bits: usize, hashes: u32) -> PyResult<Self> {
        SharedBlockedBloomFilter::reset(path, bits, hashes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("resetting the filter", e))
    }

    /// The size and hash count for holding `items` with no more than
    /// `false_positive_rate` of wrong yeses. Hand both to the
    /// constructor.
    #[staticmethod]
    fn suggest(items: usize, false_positive_rate: f64) -> PyResult<(usize, u32)> {
        if !(0.0..1.0).contains(&false_positive_rate) || false_positive_rate <= 0.0 {
            return Err(PyValueError::new_err(
                "the false positive rate must be above zero and below one",
            ));
        }
        Ok(SharedBlockedBloomFilter::suggest_config(
            items,
            false_positive_rate,
        ))
    }

    /// Add an item, setting its bits inside a single cache-line block.
    ///
    /// Confining a key to one block is what makes this faster than the
    /// plain filter, which touches bits spread across the whole bit
    /// array. Nothing can be taken out again, so the false-positive rate
    /// only climbs.
    fn insert(&self, item: &[u8]) {
        self.inner.insert(item);
    }

    /// Add a run of items in one crossing.
    fn insert_many(&self, items: Vec<Vec<u8>>) -> usize {
        for item in &items {
            self.inner.insert(item);
        }
        items.len()
    }

    /// False means the item was definitely never added. True means it
    /// probably was.
    fn __contains__(&self, item: &[u8]) -> bool {
        self.inner.contains(item)
    }

    /// As `in`, spelled out.
    fn contains(&self, item: &[u8]) -> bool {
        self.inner.contains(item)
    }

    /// Ask about a run of items in one crossing.
    fn contains_many(&self, items: Vec<Vec<u8>>) -> Vec<bool> {
        items.iter().map(|item| self.inner.contains(item)).collect()
    }

    /// Empty the filter.
    fn clear(&self) {
        self.inner.clear();
    }

    /// How many bits each item sets.
    #[getter]
    fn hashes(&self) -> u32 {
        self.inner.n_hashes()
    }

    /// How many cache-line blocks the filter is made of.
    #[getter]
    fn blocks(&self) -> u64 {
        self.inner.n_blocks()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// bits without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The bits stay set for whoever attaches next, and nothing
    /// is flushed on the way out.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.BlockedBloomFilter {} blocks, {} hashes>",
            self.inner.n_blocks(),
            self.inner.n_hashes()
        )
    }
}

/// A handle crosses the boundary as a plain number, which is the whole
/// of what it is: the place in the table in its low half and the number
/// of times that place has been reused in its high half.
fn handle_from_raw(raw: u64) -> SubethaHandle {
    SubethaHandle::from_parts((raw >> 32) as u32, raw as u32)
}

/// Values reached by a handle rather than by a place, so a slot reused
/// after a value is removed cannot be mistaken for the old one.
///
/// A handle carries the place and the number of times that place has
/// been reused. Looking up a handle whose place has since been given to
/// something else answers None instead of the new occupant, which is the
/// mistake a bare index invites.
#[pyclass(module = "subetha")]
struct HandleTable {
    inner: Box<SharedHandleTable<LeaseValue>>,
}

#[pymethods]
impl HandleTable {
    /// How many values the table holds at most. A value is at most 44
    /// bytes.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("a table holds at least one value"));
        }
        SharedHandleTable::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the table", e))
    }

    /// Attach to a table another holder made, with the capacity it was
    /// made with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedHandleTable::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the table", e))
    }

    /// Empty the table and remake it at this capacity.
    #[staticmethod]
    fn reset(path: &str, capacity: usize) -> PyResult<Self> {
        SharedHandleTable::reset(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("resetting the table", e))
    }

    /// Put a value in and get its handle. Raises when the table is full.
    fn insert(&self, value: &[u8]) -> PyResult<u64> {
        let held = LeaseValue::from_bytes(value)?;
        self.inner
            .insert(held)
            .map(|handle| handle.raw())
            .map_err(|e| os_err("inserting", e))
    }

    /// Put a run of values in, answering their handles in order. Stops
    /// at the first one that does not fit, so a short answer means the
    /// table filled.
    fn insert_many(&self, values: Vec<Vec<u8>>) -> PyResult<Vec<u64>> {
        let mut handles = Vec::with_capacity(values.len());
        for value in &values {
            let held = LeaseValue::from_bytes(value)?;
            match self.inner.insert(held) {
                Ok(handle) => handles.push(handle.raw()),
                Err(HandleTableError::Full) => break,
                Err(e) => return Err(os_err("inserting", e)),
            }
        }
        Ok(handles)
    }

    /// The value behind a handle, or None when it has been removed or
    /// its place given to something else.
    fn get(&self, handle: u64) -> Option<Vec<u8>> {
        self.inner
            .get(handle_from_raw(handle))
            .map(|held| held.as_bytes())
    }

    /// Several values in one crossing, each None where the handle no
    /// longer names anything.
    fn get_many(&self, handles: Vec<u64>) -> Vec<Option<Vec<u8>>> {
        handles
            .into_iter()
            .map(|handle| {
                self.inner
                    .get(handle_from_raw(handle))
                    .map(|held| held.as_bytes())
            })
            .collect()
    }

    /// Whether the handle is still live. A handle that has been given
    /// back answers `False`, and so does one from an earlier generation
    /// of the same slot, which is what the generation in a handle is for.
    fn __contains__(&self, handle: u64) -> bool {
        self.inner.contains(handle_from_raw(handle))
    }

    /// As `in`, spelled out.
    fn contains(&self, handle: u64) -> bool {
        self.inner.contains(handle_from_raw(handle))
    }

    /// Take a value out, answering it, or None when the handle no longer
    /// names anything.
    fn remove(&self, handle: u64) -> Option<Vec<u8>> {
        self.inner
            .remove(handle_from_raw(handle))
            .map(|held| held.as_bytes())
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// entries without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Start writing the mapping back to its file and return at once,
    /// without waiting for the write to land. Nothing is on disk yet when
    /// this returns; `flush` is the form that waits.
    fn flush_async(&self) -> PyResult<()> {
        self.inner.flush_async().map_err(|e| os_err("flushing", e))
    }

    /// The most a value may be, in bytes.
    #[classattr]
    fn max_value_bytes() -> usize {
        LEASE_VALUE_BYTES
    }

    /// How many handles are live, which is not the capacity. A handle
    /// given back stops being counted at once.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Handles taken inside the block are still live after it:
    /// they are named by the table, not owned by this object.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.HandleTable {} of {}>",
            self.inner.len(),
            self.inner.capacity()
        )
    }
}

/// Sixteen values, each stamped with the version it was written at, so a
/// reader holding an older version sees only what existed then.
///
/// A reader takes a version number and asks which places were written at
/// or before it. Everything written after stays invisible to it, and
/// stays visible to a reader that comes later. The sixteen are compared
/// against the reader's version in a handful of vector instructions
/// rather than one at a time, which is why the tile is this size.
#[pyclass(module = "subetha")]
struct TimePointTile {
    inner: Box<SharedTimePointTile<SlotValue>>,
}

#[pymethods]
impl TimePointTile {
    /// A value is at most 52 bytes.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        SharedTimePointTile::create(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the tile", e))
    }

    /// Attach to a tile another holder made.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        SharedTimePointTile::open(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the tile", e))
    }

    /// Empty the tile and remake it.
    #[staticmethod]
    fn reset(path: &str) -> PyResult<Self> {
        SharedTimePointTile::reset(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("resetting the tile", e))
    }

    /// Write a value at a version and get the place it went in. Raises
    /// when all sixteen places are taken.
    fn insert(&self, version: u64, value: &[u8]) -> PyResult<usize> {
        let held = SlotValue::from_bytes(value)?;
        self.inner
            .insert(version, held)
            .map_err(|e| os_err("inserting", e))
    }

    /// Empty one place.
    fn remove(&self, lane: usize) -> PyResult<()> {
        if lane >= SUBETHA_TILE_CAP {
            return Err(PyValueError::new_err(format!(
                "a tile has {SUBETHA_TILE_CAP} places, numbered from zero"
            )));
        }
        self.inner.remove(lane);
        Ok(())
    }

    /// The version and value in one place, or None when it is empty.
    fn at(&self, lane: usize) -> PyResult<Option<(u64, Vec<u8>)>> {
        if lane >= SUBETHA_TILE_CAP {
            return Err(PyValueError::new_err(format!(
                "a tile has {SUBETHA_TILE_CAP} places, numbered from zero"
            )));
        }
        Ok(self
            .inner
            .at(lane)
            .map(|(version, held)| (version, held.as_bytes())))
    }

    /// Which places a reader at `version` can see, as sixteen bits with
    /// the lowest standing for the first place.
    fn visible_mask(&self, version: u64) -> u16 {
        self.inner.visible_mask(version)
    }

    /// How many places a reader at `version` can see.
    fn visible_count(&self, version: u64) -> u32 {
        self.inner.visible_count(version)
    }

    /// Everything a reader at `version` can see, as version and value
    /// pairs, in one crossing.
    fn visible(&self, version: u64) -> Vec<(u64, Vec<u8>)> {
        let mask = self.inner.visible_mask(version);
        (0..SUBETHA_TILE_CAP)
            .filter(|lane| mask & (1 << lane) != 0)
            .filter_map(|lane| {
                self.inner
                    .at(lane)
                    .map(|(at, held)| (at, held.as_bytes()))
            })
            .collect()
    }

    /// Whether all sixteen places are taken.
    #[getter]
    fn full(&self) -> bool {
        self.inner.is_full()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// points without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Start writing the mapping back to its file and return at once,
    /// without waiting for the write to land. Nothing is on disk yet when
    /// this returns; `flush` is the form that waits.
    fn flush_async(&self) -> PyResult<()> {
        self.inner.flush_async().map_err(|e| os_err("flushing", e))
    }

    /// How many places a tile has.
    #[classattr]
    fn lanes() -> usize {
        SUBETHA_TILE_CAP
    }

    /// The most a value may be, in bytes.
    #[classattr]
    fn max_value_bytes() -> usize {
        SLOT_VALUE_BYTES
    }

    /// How many of the tile's sixteen places are taken. `full` is the
    /// same question asked the other way round.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The points recorded stay for whoever attaches next, and
    /// nothing is flushed on the way out.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.TimePointTile {} of {}>",
            self.inner.len(),
            SUBETHA_TILE_CAP
        )
    }
}

/// One value's history: every version it has had, each stamped with the
/// version number it became current at.
///
/// A reader holding a version number gets the value as it stood then,
/// whatever has been written since. Writing does not overwrite; it adds
/// to the front of the chain, which is what lets an old reader keep
/// reading. The chain is bounded, so a long-lived value eventually needs
/// its old versions cleared.
#[pyclass(module = "subetha")]
struct VersionChain {
    inner: Box<SharedVersionedChain<LeaseValue>>,
}

#[pymethods]
impl VersionChain {
    /// How many versions the chain holds at most. A value is at most 44
    /// bytes.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("a chain holds at least one version"));
        }
        SharedVersionedChain::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the chain", e))
    }

    /// Attach to a chain another holder made, with the capacity it was
    /// made with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedVersionedChain::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the chain", e))
    }

    /// Empty the chain and remake it at this capacity.
    #[staticmethod]
    fn reset(path: &str, capacity: usize) -> PyResult<Self> {
        SharedVersionedChain::reset(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("resetting the chain", e))
    }

    /// Write a new version. The version number must be above the one
    /// already at the front, which is what keeps the history in order.
    fn push(&self, version: u64, value: &[u8]) -> PyResult<()> {
        let held = LeaseValue::from_bytes(value)?;
        self.inner
            .push(version, held)
            .map_err(|e| os_err("writing a version", e))
    }

    /// The value as it stood at `version`, or None when nothing had been
    /// written by then.
    fn read_at(&self, version: u64) -> Option<Vec<u8>> {
        self.inner.read_at(version).map(|held| held.as_bytes())
    }

    /// The version number and value at the front of the chain, or None
    /// when nothing has been written.
    #[getter]
    fn current(&self) -> Option<(u64, Vec<u8>)> {
        self.inner
            .current()
            .map(|(version, held)| (version, held.as_bytes()))
    }

    /// Throw away every version.
    fn clear(&self) {
        self.inner.clear();
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// versions without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Start writing the mapping back to its file and return at once,
    /// without waiting for the write to land. Nothing is on disk yet when
    /// this returns; `flush` is the form that waits.
    fn flush_async(&self) -> PyResult<()> {
        self.inner.flush_async().map_err(|e| os_err("flushing", e))
    }

    /// The most a value may be, in bytes.
    #[classattr]
    fn max_value_bytes() -> usize {
        LEASE_VALUE_BYTES
    }

    /// How many versions the chain holds, which is not the capacity and
    /// not the number of distinct values: one value written three times
    /// is three versions until something reclaims the older two.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Every version written inside the block is still in the
    /// chain after it, and nothing is reclaimed on the way out.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.VersionChain {} of {}>",
            self.inner.len(),
            self.inner.capacity()
        )
    }
}

/// How many versions a slot of `VersionedSlab` keeps. Past this, the
/// oldest goes when a new one is written, so a reader that has held a
/// pin through four writes to one slot may find its version gone.
const VERSIONED_SLAB_DEPTH: usize = 4;

/// Numbered slots, each keeping its recent history, so a reader holding
/// an epoch sees every slot as it stood at that epoch.
///
/// Epochs here come from a shared table rather than from the caller: a
/// reader takes a pin, which fixes the epoch it reads at, and a writer
/// advances the epoch when it writes. A version stays until every pin
/// that could still see it has gone, which is what `sweep` and `void`
/// act on.
#[pyclass(module = "subetha")]
struct VersionedSlab {
    inner: Box<SharedVersionedSlab<LeaseValue, VERSIONED_SLAB_DEPTH>>,
}

#[pymethods]
impl VersionedSlab {
    /// `capacity` is how many slots there are and `max_pins` how many
    /// readers may hold a pin at once. The epochs live in their own
    /// file, which other structures may share. A value is at most 44
    /// bytes.
    #[new]
    #[pyo3(signature = (path, capacity, epochs_path, max_pins = 16))]
    fn new(path: &str, capacity: usize, epochs_path: &str, max_pins: usize) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("a slab holds at least one slot"));
        }
        if max_pins < 1 {
            return Err(PyValueError::new_err("at least one reader must be able to pin"));
        }
        SharedVersionedSlab::create(path, capacity, epochs_path, max_pins)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the slab", e))
    }

    /// Attach to a slab another holder made, with the capacity and pin
    /// count it was made with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, epochs_path, max_pins = 16))]
    fn open(path: &str, capacity: usize, epochs_path: &str, max_pins: usize) -> PyResult<Self> {
        SharedVersionedSlab::open(path, capacity, epochs_path, max_pins)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the slab", e))
    }

    /// Write a slot, at the next epoch.
    fn set(&self, slot: usize, value: &[u8]) -> PyResult<()> {
        let held = LeaseValue::from_bytes(value)?;
        self.inner
            .set(slot, held)
            .map_err(|e| os_err("writing a slot", e))
    }

    /// Write a slot at a named epoch, for a caller stepping the epochs
    /// itself.
    fn set_at(&self, slot: usize, value: &[u8], born: u64) -> PyResult<()> {
        let held = LeaseValue::from_bytes(value)?;
        self.inner
            .set_at(slot, held, born)
            .map_err(|e| os_err("writing a slot", e))
    }

    /// The value in a slot now, or None when nothing live is there.
    fn get(&self, slot: usize) -> PyResult<Option<Vec<u8>>> {
        self.inner
            .get(slot)
            .map(|found| found.map(|held| held.as_bytes()))
            .map_err(|e| os_err("reading a slot", e))
    }

    /// A slot's history, newest first, as value, born and died. A died
    /// of None means the version is the current one.
    fn history(&self, slot: usize) -> PyResult<SlotHistory> {
        let chain = self
            .inner
            .chain(slot)
            .map_err(|e| os_err("reading a slot", e))?;
        Ok(chain
            .into_iter()
            .map(|version| {
                let died = if version.is_live() { None } else { Some(version.died) };
                (version.value.as_bytes(), version.born, died)
            })
            .collect())
    }

    /// Mark a slot's current value as no longer current, at the next
    /// epoch, answering what it was. Readers pinned earlier still see it.
    fn retire(&self, slot: usize) -> PyResult<Option<Vec<u8>>> {
        self.inner
            .retire(slot)
            .map(|gone| gone.map(|held| held.as_bytes()))
            .map_err(|e| os_err("retiring a slot", e))
    }

    /// As `retire`, at a named epoch.
    fn retire_at(&self, slot: usize, died: u64) -> PyResult<Option<Vec<u8>>> {
        self.inner
            .retire_at(slot, died)
            .map(|gone| gone.map(|held| held.as_bytes()))
            .map_err(|e| os_err("retiring a slot", e))
    }

    /// Take a pin, fixing one epoch to read the whole slab at. Give it
    /// back when the block ends, or reclaiming cannot move past it.
    fn pin(slf: Bound<'_, Self>) -> PyResult<SlabPin> {
        // The slab sits in a box this object owns, so its address does
        // not change while the object lives, and the handle held here
        // keeps it alive at least as long as the pin.
        let held = slf.borrow();
        let slab: *const SharedVersionedSlab<LeaseValue, VERSIONED_SLAB_DEPTH> = &*held.inner;
        let slab = unsafe { &*slab };
        drop(held);
        let guard = slab
            .pin()
            .map_err(|e| os_err("taking a pin", e))?;
        Ok(SlabPin {
            guard: Some(guard),
            slab,
            _owner: slf.unbind(),
        })
    }

    /// Throw away every version of a slot that nothing can still see,
    /// answering how many went.
    fn sweep_slot(&self, slot: usize) -> PyResult<usize> {
        self.inner
            .sweep_slot(slot)
            .map_err(|e| os_err("sweeping a slot", e))
    }

    /// Undo every write stamped at exactly this epoch, across the whole
    /// slab, answering how many versions were touched.
    ///
    /// This is not reclaiming: a version written at the epoch is taken
    /// away, and one that stopped being current at the epoch is current
    /// again. It is for an epoch whose writer died partway through, so
    /// the half-finished change is rolled back rather than left. To
    /// reclaim, use `sweep_slot`.
    fn void_epoch(&self, epoch: u64) -> PyResult<usize> {
        self.inner
            .void_epoch(epoch)
            .map_err(|e| os_err("voiding an epoch", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// versions without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// How many versions one slot keeps.
    #[classattr]
    fn depth() -> usize {
        VERSIONED_SLAB_DEPTH
    }

    /// The most a value may be, in bytes.
    #[classattr]
    fn max_value_bytes() -> usize {
        LEASE_VALUE_BYTES
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. A `SlabPin` taken inside the block is not given back
    /// here: pin it in its own `with` block, or the epoch it holds keeps
    /// reclamation waiting.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!("<subetha.VersionedSlab capacity={}>", self.inner.capacity())
    }
}

/// One fixed epoch to read a slab at. Everything read through it is the
/// slab as it stood when the pin was taken.
///
/// A pin held open stops reclaiming, so give it back when done. The
/// block ending does that, and so does dropping it.
#[pyclass(module = "subetha", unsendable)]
struct SlabPin {
    /// Declared first so it is dropped first. Giving the pin back
    /// reaches into the slab's epoch table, and the handle below is
    /// what keeps that table alive; released the other way round, the
    /// last handle could go while the guard still had to use it.
    guard: Option<SubethaPinGuard<'static>>,
    slab: &'static SharedVersionedSlab<LeaseValue, VERSIONED_SLAB_DEPTH>,
    /// The slab this came from, kept alive for as long as the pin is.
    _owner: Py<VersionedSlab>,
}

#[pymethods]
impl SlabPin {
    /// The epoch this pin fixed.
    #[getter]
    fn epoch(&self) -> PyResult<u64> {
        self.guard
            .as_ref()
            .map(|guard| guard.epoch())
            .ok_or_else(|| PyValueError::new_err("this pin has been given back"))
    }

    /// The value in a slot as it stood when the pin was taken, or None
    /// when nothing was there then.
    fn get(&self, slot: usize) -> PyResult<Option<Vec<u8>>> {
        let guard = self
            .guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("this pin has been given back"))?;
        self.slab
            .get_at(slot, guard)
            .map(|found| found.map(|held| held.as_bytes()))
            .map_err(|e| os_err("reading a slot", e))
    }

    /// Several slots in one crossing, each None where nothing was there
    /// when the pin was taken.
    fn get_many(&self, slots: Vec<usize>) -> PyResult<Vec<Option<Vec<u8>>>> {
        let guard = self
            .guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("this pin has been given back"))?;
        let mut found = Vec::with_capacity(slots.len());
        for slot in slots {
            found.push(
                self.slab
                    .get_at(slot, guard)
                    .map(|value| value.map(|held| held.as_bytes()))
                    .map_err(|e| os_err("reading a slot", e))?,
            );
        }
        Ok(found)
    }

    /// Give the pin back now rather than at the end of a block. Calling
    /// it twice is harmless.
    fn release(&mut self) {
        self.guard = None;
    }

    #[getter]
    fn held(&self) -> bool {
        self.guard.is_some()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Give the pin back, and let an exception through.
    ///
    /// This one really does release: the epoch stops being held, so the
    /// slab can reclaim versions no one can still see. Reading through
    /// the pin after the block answers nothing, and `repr` says it was
    /// given back. This is the reason to take a pin in a `with` block
    /// rather than leaving it to be collected.
    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.guard = None;
        false
    }

    fn __repr__(&self) -> String {
        match &self.guard {
            Some(guard) => format!("<subetha.SlabPin at epoch {}>", guard.epoch()),
            None => "<subetha.SlabPin given back>".to_string(),
        }
    }
}

/// An ordered map whose entries carry the epochs they were current
/// between, so a reader holding a pin scans one unchanging view of it
/// while writers carry on.
///
/// Keys and values are both plain unsigned integers here. A versioned
/// map at this level is an index, mapping an identifier to another one
/// or to an offset, and keeping both sides to a machine word is what
/// makes a scan cost what it should.
///
/// Removing does not take an entry away; it marks it as no longer
/// current, so a reader pinned earlier still sees it. `sweep` is what
/// takes the marked ones away once nothing can reach them.
#[pyclass(module = "subetha")]
struct VersionedMap {
    inner: Box<VersionedBTreeMap<u64, u64>>,
}

#[pymethods]
impl VersionedMap {
    /// `capacity` is how many entries the map holds, counting the ones
    /// marked as no longer current, and `max_pins` how many readers may
    /// scan at once. The epochs live in their own file, which other
    /// structures may share.
    #[new]
    #[pyo3(signature = (path, capacity, epochs_path, max_pins = 16))]
    fn new(path: &str, capacity: usize, epochs_path: &str, max_pins: usize) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("a map holds at least one entry"));
        }
        if max_pins < 1 {
            return Err(PyValueError::new_err("at least one reader must be able to pin"));
        }
        VersionedBTreeMap::create(path, capacity, epochs_path, max_pins)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the map", e))
    }

    /// Attach to a map another holder made, with the capacity and pin
    /// count it was made with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, epochs_path, max_pins = 16))]
    fn open(path: &str, capacity: usize, epochs_path: &str, max_pins: usize) -> PyResult<Self> {
        VersionedBTreeMap::open(path, capacity, epochs_path, max_pins)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the map", e))
    }

    /// Put an entry in at the next epoch, answering what the key held
    /// before, or None when it held nothing.
    fn insert(&self, key: u64, value: u64) -> PyResult<Option<u64>> {
        self.inner
            .insert(key, value)
            .map_err(|e| os_err("inserting", e))
    }

    /// Put an entry in at a named epoch, for a caller stepping the
    /// epochs itself.
    fn insert_at(&self, key: u64, value: u64, born: u64) -> PyResult<Option<u64>> {
        self.inner
            .insert_at(key, value, born)
            .map_err(|e| os_err("inserting", e))
    }

    /// Put a run of entries in, in one crossing, answering what each key
    /// held before.
    fn insert_many(&self, entries: Vec<(u64, u64)>) -> PyResult<Vec<Option<u64>>> {
        let mut before = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            before.push(
                self.inner
                    .insert(key, value)
                    .map_err(|e| os_err("inserting", e))?,
            );
        }
        Ok(before)
    }

    /// What a key holds now, or None when it holds nothing.
    fn get(&self, key: u64) -> Option<u64> {
        self.inner.get(&key)
    }

    /// Several keys in one crossing, each None where the key holds
    /// nothing.
    fn get_many(&self, keys: Vec<u64>) -> Vec<Option<u64>> {
        keys.into_iter().map(|key| self.inner.get(&key)).collect()
    }

    /// Mark a key as no longer current at the next epoch, answering what
    /// it held. Readers pinned earlier still see it.
    fn remove(&self, key: u64) -> PyResult<Option<u64>> {
        self.inner
            .remove(&key)
            .map_err(|e| os_err("removing", e))
    }

    /// As `remove`, at a named epoch.
    fn remove_at(&self, key: u64, died: u64) -> PyResult<Option<u64>> {
        self.inner
            .remove_at(&key, died)
            .map_err(|e| os_err("removing", e))
    }

    /// Take a pin, fixing one epoch to scan the whole map at. Give it
    /// back when the block ends, or reclaiming cannot move past it.
    fn pin(slf: Bound<'_, Self>) -> PyResult<MapPin> {
        // The map sits in a box this object owns, so its address does
        // not change while the object lives, and the handle held here
        // keeps it alive at least as long as the pin.
        let held = slf.borrow();
        let map: *const VersionedBTreeMap<u64, u64> = &*held.inner;
        let map = unsafe { &*map };
        drop(held);
        let guard = map.pin().map_err(|e| os_err("taking a pin", e))?;
        Ok(MapPin {
            guard: Some(guard),
            map,
            _owner: slf.unbind(),
        })
    }

    /// Take away every entry marked as no longer current that nothing
    /// can still reach, answering how many went.
    ///
    /// Zero is an ordinary answer and means nothing could be taken: a
    /// live pin holds the horizon where it is, so a sweep run during a
    /// scan frees only what was already unreachable when that scan
    /// began.
    fn sweep(&self) -> PyResult<usize> {
        match self.inner.sweep() {
            Ok(freed) => Ok(freed),
            // The Rust reports finding nothing to free as Full, because
            // its own caller is an insert that has run out of room. Here
            // the question is how many went, and the answer is none.
            Err(VersionedError::Full) => Ok(0),
            Err(e) => Err(os_err("sweeping", e)),
        }
    }

    /// Undo every write stamped at exactly this epoch, answering how
    /// many entries were touched.
    ///
    /// This is not reclaiming: an entry written at the epoch is taken
    /// away, and one marked as no longer current at the epoch is current
    /// again. It is for an epoch whose writer died partway through. To
    /// reclaim, use `sweep`.
    fn void_epoch(&self, epoch: u64) -> PyResult<usize> {
        self.inner
            .void_epoch(epoch)
            .map_err(|e| os_err("voiding an epoch", e))
    }

    /// Entries the map holds, counting the ones marked as no longer
    /// current.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// entries without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. A `MapPin` taken inside the block is not given back here:
    /// pin it in its own `with` block, or the epoch it holds keeps
    /// `sweep` from reclaiming anything.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.VersionedMap {} of {}>",
            self.inner.len(),
            self.inner.capacity()
        )
    }
}

/// One fixed epoch to scan a map at. Everything read through it is the
/// map as it stood when the pin was taken.
#[pyclass(module = "subetha", unsendable)]
struct MapPin {
    /// Declared first so it is dropped first, for the reason `SlabPin`
    /// gives.
    guard: Option<SubethaPinGuard<'static>>,
    map: &'static VersionedBTreeMap<u64, u64>,
    /// The map this came from, kept alive for as long as the pin is.
    _owner: Py<VersionedMap>,
}

#[pymethods]
impl MapPin {
    /// The epoch this pin fixed.
    #[getter]
    fn epoch(&self) -> PyResult<u64> {
        self.guard
            .as_ref()
            .map(|guard| guard.epoch())
            .ok_or_else(|| PyValueError::new_err("this pin has been given back"))
    }

    /// What a key held when the pin was taken, or None when it held
    /// nothing then.
    fn get(&self, key: u64) -> PyResult<Option<u64>> {
        let guard = self.guard()?;
        Ok(self.map.get_at(&key, guard))
    }

    /// Several keys in one crossing.
    fn get_many(&self, keys: Vec<u64>) -> PyResult<Vec<Option<u64>>> {
        let guard = self.guard()?;
        Ok(keys
            .into_iter()
            .map(|key| self.map.get_at(&key, guard))
            .collect())
    }

    /// The entries between two keys, in key order, at most `limit` of
    /// them, as they stood when the pin was taken. `low` and `high` are
    /// inclusive; None at either end means no bound there.
    ///
    /// The limit counts entries walked rather than entries answered, so
    /// a stretch thick with entries marked as no longer current can
    /// answer fewer than the limit while more remain. `scan_from` is the
    /// form that says where to carry on from.
    #[pyo3(signature = (low = None, high = None, limit = 1024))]
    fn scan(&self, low: Option<u64>, high: Option<u64>, limit: usize) -> PyResult<Vec<(u64, u64)>> {
        let guard = self.guard()?;
        Ok(self
            .map
            .range_at(bound_of(&low), bound_of(&high), limit, guard))
    }

    /// As `scan`, and also the last key the walk reached, to carry on
    /// from. A None there means the walk ran out of entries.
    #[pyo3(signature = (low = None, high = None, limit = 1024))]
    fn scan_from(
        &self,
        low: Option<u64>,
        high: Option<u64>,
        limit: usize,
    ) -> PyResult<ScanPage> {
        let guard = self.guard()?;
        Ok(self
            .map
            .range_at_with_cursor(bound_of(&low), bound_of(&high), limit, guard))
    }

    /// Give the pin back now rather than at the end of a block. Calling
    /// it twice is harmless.
    fn release(&mut self) {
        self.guard = None;
    }

    #[getter]
    fn held(&self) -> bool {
        self.guard.is_some()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Give the pin back, and let an exception through.
    ///
    /// This one really does release: the epoch stops being held, so
    /// `sweep` can reclaim what no reader can still see. Reading through
    /// the pin after the block answers nothing, and `repr` says it was
    /// given back. A pin left to be collected instead holds reclamation
    /// up for as long as the object lives.
    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.guard = None;
        false
    }

    fn __repr__(&self) -> String {
        match &self.guard {
            Some(guard) => format!("<subetha.MapPin at epoch {}>", guard.epoch()),
            None => "<subetha.MapPin given back>".to_string(),
        }
    }
}

impl MapPin {
    fn guard(&self) -> PyResult<&SubethaPinGuard<'static>> {
        self.guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("this pin has been given back"))
    }
}

/// A key at one end of a scan, or no bound there. Both ends are
/// inclusive, which is the reading a caller naming two keys expects.
fn bound_of(key: &Option<u64>) -> RangeEnd<&u64> {
    match key {
        Some(key) => RangeEnd::Included(key),
        None => RangeEnd::Unbounded,
    }
}

pyo3::create_exception!(
    subetha,
    WrongLane,
    pyo3::exceptions::PyException,
    "The key belongs to a different lane of the laned map, and must be \
     written through that lane rather than this one."
);

/// The same versioned map split across several trees, so several writers
/// work at once instead of queueing behind one.
///
/// Each writer claims a lane and writes only through it. A key belongs
/// to the lane it was first written in for the rest of its life, because
/// a lane is a separate tree: removing a key through the wrong lane
/// would report a row gone that no reader has stopped seeing. Writing a
/// key through the wrong lane raises `WrongLane`, naming the right one,
/// rather than answering that there was nothing there.
///
/// Reading takes no lane. A read looks across every lane, and a scan
/// through a pin merges them in key order.
#[pyclass(module = "subetha")]
struct LanedMap {
    inner: Box<LanedVersionedMap<u64, u64>>,
}

#[pymethods]
impl LanedMap {
    /// The map lives in a directory of its own, one file per lane plus
    /// the shared epochs and the claims. `nodes_per_lane` is how many
    /// entries each lane holds and `max_pins` how many readers may scan
    /// at once across all of them.
    #[new]
    #[pyo3(signature = (directory, lanes = 4, nodes_per_lane = 256, max_pins = 16))]
    fn new(directory: &str, lanes: usize, nodes_per_lane: usize, max_pins: usize) -> PyResult<Self> {
        if lanes < 1 {
            return Err(PyValueError::new_err("a laned map has at least one lane"));
        }
        if nodes_per_lane < 1 {
            return Err(PyValueError::new_err("a lane holds at least one entry"));
        }
        if max_pins < 1 {
            return Err(PyValueError::new_err("at least one reader must be able to pin"));
        }
        LanedVersionedMap::create(directory, lanes, nodes_per_lane, max_pins)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(laned_err)
    }

    /// Attach to a laned map another holder made, with the lane count,
    /// lane size and pin count it was made with.
    #[staticmethod]
    #[pyo3(signature = (directory, lanes = 4, nodes_per_lane = 256, max_pins = 16))]
    fn open(directory: &str, lanes: usize, nodes_per_lane: usize, max_pins: usize) -> PyResult<Self> {
        LanedVersionedMap::open(directory, lanes, nodes_per_lane, max_pins)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(laned_err)
    }

    /// Claim a free lane to write keys that are not in the map yet.
    /// Raises `Contended` when every lane is held.
    fn claim_lane(slf: Bound<'_, Self>) -> PyResult<LaneClaim> {
        let map = laned_map_of(&slf);
        let guard = map.claim_lane().map_err(laned_err)?;
        Ok(LaneClaim {
            guard: Some(guard),
            _owner: slf.unbind(),
        })
    }

    /// Claim the lane a key already lives in, to rewrite or remove it.
    /// Raises `KeyError` when no lane holds the key, and `Contended`
    /// when its lane is held by someone else. Retry rather than writing
    /// elsewhere: elsewhere is a different tree.
    fn claim_lane_for(slf: Bound<'_, Self>, key: u64) -> PyResult<LaneClaim> {
        let map = laned_map_of(&slf);
        let guard = map.claim_lane_for(&key).map_err(laned_err)?;
        Ok(LaneClaim {
            guard: Some(guard),
            _owner: slf.unbind(),
        })
    }

    /// Which lane holds a key, or None when no lane does.
    fn lane_of(&self, key: u64) -> Option<usize> {
        self.inner.lane_of(&key)
    }

    /// What a key holds now, from whichever lane holds it.
    fn get(&self, key: u64) -> Option<u64> {
        self.inner.get(&key)
    }

    /// Several keys in one crossing.
    fn get_many(&self, keys: Vec<u64>) -> Vec<Option<u64>> {
        keys.into_iter().map(|key| self.inner.get(&key)).collect()
    }

    /// Take a pin, fixing one epoch to scan every lane at.
    fn pin(slf: Bound<'_, Self>) -> PyResult<LanedPin> {
        let map = laned_map_of(&slf);
        let guard = map.pin().map_err(laned_err)?;
        Ok(LanedPin {
            guard: Some(guard),
            map,
            _owner: slf.unbind(),
        })
    }

    /// How many lanes are held by a writer right now.
    #[getter]
    fn held_lanes(&self) -> usize {
        self.inner.held_lanes()
    }

    /// Give back the lanes of writers whose process has gone, answering
    /// how many came back. Without this a process that died holding a
    /// lane keeps it forever.
    fn reap_dead_claims(&self) -> usize {
        self.inner.reap_dead_claims()
    }

    /// Take away every entry marked as no longer current that nothing
    /// can still reach, across every lane. Zero means nothing could be
    /// taken.
    fn sweep(&self) -> PyResult<usize> {
        match self.inner.sweep() {
            Ok(freed) => Ok(freed),
            Err(LanedError::Versioned(VersionedError::Full)) => Ok(0),
            Err(e) => Err(laned_err(e)),
        }
    }

    /// Undo every write stamped at exactly this epoch across every lane,
    /// answering how many entries were touched. For a writer that died
    /// partway through a change spanning several lanes.
    fn void_epoch(&self, epoch: u64) -> PyResult<usize> {
        self.inner.void_epoch(epoch).map_err(laned_err)
    }

    /// How many lanes there are.
    #[getter]
    fn lanes(&self) -> usize {
        self.inner.lanes()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Every lane goes at once, not only the ones this
    /// process has claimed. Another process mapping the same file sees
    /// the entries without this; flushing is about surviving a machine
    /// that stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(laned_err)
    }

    /// Entries across every lane, counting the ones marked as no longer
    /// current.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. A `LaneClaim` or `LanedPin` taken inside the block is not
    /// given back here; each belongs in a `with` block of its own.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.LanedMap {} entries across {} lanes>",
            self.inner.len(),
            self.inner.lanes()
        )
    }
}

/// The laned map inside a Python object, borrowed for as long as that
/// object lives. It sits in a box the object owns, so its address does
/// not change, and every holder of this reference also holds a handle on
/// the object.
fn laned_map_of(slf: &Bound<'_, LanedMap>) -> &'static LanedVersionedMap<u64, u64> {
    let held = slf.borrow();
    let map: *const LanedVersionedMap<u64, u64> = &*held.inner;
    drop(held);
    unsafe { &*map }
}

fn laned_err(e: LanedError) -> PyErr {
    match e {
        LanedError::NoFreeLane => {
            Contended::new_err("every lane is held by another writer".to_string())
        }
        LanedError::LaneBusy(lane) => {
            Contended::new_err(format!("lane {lane} is held by another writer"))
        }
        LanedError::KeyAbsent => {
            pyo3::exceptions::PyKeyError::new_err("no lane holds this key".to_string())
        }
        LanedError::KeyInAnotherLane(lane) => WrongLane::new_err(format!(
            "this key belongs to lane {lane} and must be written through that lane"
        )),
        other => PyOSError::new_err(format!("{other}")),
    }
}

/// A held lane of a laned map. Writes go through it, and it gives the
/// lane back when its block ends, so another writer can take it.
#[pyclass(module = "subetha", unsendable)]
struct LaneClaim {
    /// Declared first so it is dropped first, for the reason `SlabPin`
    /// gives.
    guard: Option<SubethaLaneGuard<'static, u64, u64>>,
    /// The map this came from, kept alive for as long as the claim is.
    _owner: Py<LanedMap>,
}

#[pymethods]
impl LaneClaim {
    /// Which lane this claim holds.
    #[getter]
    fn index(&self) -> PyResult<usize> {
        self.guard().map(|guard| guard.index())
    }

    /// Put an entry in this lane at the next epoch, answering what the
    /// key held before.
    fn insert(&self, key: u64, value: u64) -> PyResult<Option<u64>> {
        self.guard()?.insert(key, value).map_err(laned_err)
    }

    /// Put an entry in at a named epoch, so every write of one change
    /// becomes visible together.
    fn insert_at(&self, key: u64, value: u64, born: u64) -> PyResult<Option<u64>> {
        self.guard()?.insert_at(key, value, born).map_err(laned_err)
    }

    /// Put a run of entries in this lane in one crossing.
    fn insert_many(&self, entries: Vec<(u64, u64)>) -> PyResult<Vec<Option<u64>>> {
        let guard = self.guard()?;
        let mut before = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            before.push(guard.insert(key, value).map_err(laned_err)?);
        }
        Ok(before)
    }

    /// Mark a key in this lane as no longer current, answering what it
    /// held. Raises `WrongLane` when the key lives in another lane.
    fn remove(&self, key: u64) -> PyResult<Option<u64>> {
        self.guard()?.remove(&key).map_err(laned_err)
    }

    /// As `remove`, at a named epoch.
    fn remove_at(&self, key: u64, died: u64) -> PyResult<Option<u64>> {
        self.guard()?.remove_at(&key, died).map_err(laned_err)
    }

    /// Give the lane back now rather than at the end of a block. Calling
    /// it twice is harmless.
    fn release(&mut self) {
        self.guard = None;
    }

    #[getter]
    fn held(&self) -> bool {
        self.guard.is_some()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Give the lane back, and let an exception through.
    ///
    /// This one really does release: another process can claim the lane
    /// straight away. A claim left to be collected instead holds the lane
    /// until then, and `reap_dead_claims` on the map is what recovers one
    /// whose process died without giving it back.
    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.guard = None;
        false
    }

    fn __repr__(&self) -> String {
        match &self.guard {
            Some(guard) => format!("<subetha.LaneClaim on lane {}>", guard.index()),
            None => "<subetha.LaneClaim given back>".to_string(),
        }
    }
}

impl LaneClaim {
    fn guard(&self) -> PyResult<&SubethaLaneGuard<'static, u64, u64>> {
        self.guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("this lane claim has been given back"))
    }
}

/// One fixed epoch to scan a laned map at, across every lane.
#[pyclass(module = "subetha", unsendable)]
struct LanedPin {
    /// Declared first so it is dropped first, for the reason `SlabPin`
    /// gives.
    guard: Option<SubethaPinGuard<'static>>,
    map: &'static LanedVersionedMap<u64, u64>,
    /// The map this came from, kept alive for as long as the pin is.
    _owner: Py<LanedMap>,
}

#[pymethods]
impl LanedPin {
    /// The epoch this pin fixed.
    #[getter]
    fn epoch(&self) -> PyResult<u64> {
        self.guard().map(|guard| guard.epoch())
    }

    /// What a key held when the pin was taken, from whichever lane holds
    /// it.
    fn get(&self, key: u64) -> PyResult<Option<u64>> {
        Ok(self.map.get_at(&key, self.guard()?))
    }

    /// Several keys in one crossing.
    fn get_many(&self, keys: Vec<u64>) -> PyResult<Vec<Option<u64>>> {
        let guard = self.guard()?;
        Ok(keys
            .into_iter()
            .map(|key| self.map.get_at(&key, guard))
            .collect())
    }

    /// The entries between two keys, in key order, merged across every
    /// lane, as they stood when the pin was taken. Both ends are
    /// inclusive; None at either means no bound there. The limit counts
    /// entries walked per lane.
    #[pyo3(signature = (low = None, high = None, limit = 1024))]
    fn scan(&self, low: Option<u64>, high: Option<u64>, limit: usize) -> PyResult<Vec<(u64, u64)>> {
        let guard = self.guard()?;
        Ok(self
            .map
            .range_at(bound_of(&low), bound_of(&high), limit, guard))
    }

    /// As `scan`, and also the last key the walk reached in each lane,
    /// to carry on from.
    #[pyo3(signature = (low = None, high = None, limit = 1024))]
    fn scan_from(
        &self,
        low: Option<u64>,
        high: Option<u64>,
        limit: usize,
    ) -> PyResult<ScanPage> {
        let guard = self.guard()?;
        Ok(self
            .map
            .range_at_with_cursor(bound_of(&low), bound_of(&high), limit, guard))
    }

    /// Give the pin back now rather than at the end of a block.
    fn release(&mut self) {
        self.guard = None;
    }

    #[getter]
    fn held(&self) -> bool {
        self.guard.is_some()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Give the pin back, and let an exception through.
    ///
    /// This one really does release: the epoch stops being held, so the
    /// map can reclaim versions no reader can still see. Reading through
    /// the pin after the block answers nothing, and `repr` says it was
    /// given back.
    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.guard = None;
        false
    }

    fn __repr__(&self) -> String {
        match &self.guard {
            Some(guard) => format!("<subetha.LanedPin at epoch {}>", guard.epoch()),
            None => "<subetha.LanedPin given back>".to_string(),
        }
    }
}

impl LanedPin {
    fn guard(&self) -> PyResult<&SubethaPinGuard<'static>> {
        self.guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("this pin has been given back"))
    }
}

/// Who sends to whom, counted, so the shape of the traffic can be read
/// off it rather than guessed at.
///
/// Every send is recorded as a pair of numbered participants. From those
/// counts it reports how many different places each one sends to and
/// receives from, and recommends the shape that fits: `point_to_point`
/// when the traffic is between pairs, `broadcast_tree` when one
/// participant reaches many, and `all_to_all_mesh` when many reach many.
/// The recommendation can be published so every process reads the same
/// one.
#[pyclass(module = "subetha")]
struct TopologyMap {
    inner: Box<SharedTopologyMap>,
}

#[pymethods]
impl TopologyMap {
    /// `participants` is how many there are, numbered from zero.
    ///
    /// The thresholds are how many different places a participant has to
    /// reach, or be reached from, before the shape counts as one to many
    /// or many to one.
    #[new]
    #[pyo3(signature = (path, participants, fan_out_threshold = None, fan_in_threshold = None))]
    fn new(
        path: &str,
        participants: usize,
        fan_out_threshold: Option<u32>,
        fan_in_threshold: Option<u32>,
    ) -> PyResult<Self> {
        if participants < 1 {
            return Err(PyValueError::new_err("a topology has at least one participant"));
        }
        let built = match (fan_out_threshold, fan_in_threshold) {
            (None, None) => SharedTopologyMap::create(path, participants),
            (out, in_) => SharedTopologyMap::create_with_thresholds(
                path,
                participants,
                out.unwrap_or(DEFAULT_FAN_OUT_THRESHOLD),
                in_.unwrap_or(DEFAULT_FAN_IN_THRESHOLD),
            ),
        };
        built
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the topology", e))
    }

    /// Attach to a topology another holder made, with the participant
    /// count it was made with.
    #[staticmethod]
    fn open(path: &str, participants: usize) -> PyResult<Self> {
        SharedTopologyMap::open(path, participants)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the topology", e))
    }

    /// Empty the counts and remake the map at this size, throwing away
    /// what every other holder has recorded.
    #[staticmethod]
    #[pyo3(signature = (path, participants, fan_out_threshold = None, fan_in_threshold = None))]
    fn reset(
        path: &str,
        participants: usize,
        fan_out_threshold: Option<u32>,
        fan_in_threshold: Option<u32>,
    ) -> PyResult<Self> {
        SharedTopologyMap::reset(
            path,
            participants,
            fan_out_threshold.unwrap_or(DEFAULT_FAN_OUT_THRESHOLD),
            fan_in_threshold.unwrap_or(DEFAULT_FAN_IN_THRESHOLD),
        )
        .map(|inner| Self { inner: Box::new(inner) })
        .map_err(|e| os_err("resetting the topology", e))
    }

    /// Record one send, answering how many have gone that way.
    fn record_send(&self, sender: u32, receiver: u32) -> PyResult<u64> {
        self.inner
            .record_send(sender, receiver)
            .map_err(|e| os_err("recording a send", e))
    }

    /// Record a run of sends in one crossing.
    fn record_many(&self, sends: Vec<(u32, u32)>) -> PyResult<usize> {
        for (sender, receiver) in &sends {
            self.inner
                .record_send(*sender, *receiver)
                .map_err(|e| os_err("recording a send", e))?;
        }
        Ok(sends.len())
    }

    /// How many different places this one sends to.
    fn fan_out(&self, sender: u32) -> u32 {
        self.inner.fan_out(sender)
    }

    /// How many different places send to this one.
    fn fan_in(&self, receiver: u32) -> u32 {
        self.inner.fan_in(receiver)
    }

    /// The participant sending to the most places, and how many places
    /// that is. The Rust answers these the other way round; here the
    /// participant comes first, matching the name.
    #[getter]
    fn busiest_sender(&self) -> (u32, u32) {
        let (reach, who) = self.inner.max_fan_out();
        (who, reach)
    }

    /// The participant reached from the most places, and how many places
    /// that is.
    #[getter]
    fn busiest_receiver(&self) -> (u32, u32) {
        let (reached_from, who) = self.inner.max_fan_in();
        (who, reached_from)
    }

    /// The shape the counts suggest, one of `point_to_point`,
    /// `broadcast_tree` or `all_to_all_mesh`. Reading this does not
    /// publish it.
    fn recommend(&self) -> &'static str {
        topology_name(self.inner.recommend())
    }

    /// Work the shape out and write it down, so every process reads the
    /// same one. Answers what was published.
    fn publish_recommendation(&self) -> &'static str {
        topology_name(self.inner.publish_recommendation())
    }

    /// The shape last published, which may not be what the counts
    /// suggest now.
    fn published_recommendation(&self) -> &'static str {
        topology_name(self.inner.read_recommendation())
    }

    /// The participant at the center when the shape is one to many.
    #[getter]
    fn broadcast_root(&self) -> u32 {
        self.inner.broadcast_root()
    }

    /// Steps each time a recommendation is published, so a reader can
    /// tell a new one from the one it already acted on.
    #[getter]
    fn recommendation_epoch(&self) -> u64 {
        self.inner.recommendation_epoch()
    }

    /// Every send recorded so far.
    #[getter]
    fn total_sends(&self) -> u64 {
        self.inner.total_msgs()
    }

    #[getter]
    fn participants(&self) -> usize {
        self.inner.n_nodes()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Everything recorded stays counted for whoever attaches
    /// next; `reset` is what clears the observations.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.TopologyMap {} participants, {} sends, looks {}>",
            self.inner.n_nodes(),
            self.inner.total_msgs(),
            topology_name(self.inner.recommend())
        )
    }
}

fn topology_name(kind: TopologyKind) -> &'static str {
    match kind {
        TopologyKind::PointToPoint => "point_to_point",
        TopologyKind::BroadcastTree => "broadcast_tree",
        TopologyKind::AllToAllMesh => "all_to_all_mesh",
    }
}

/// A directed graph of numbered nodes and the edges between them, in
/// mapped files other processes can read.
///
/// A node and an edge each carry one unsigned integer, which is enough
/// to name a record elsewhere. Adding gives back an index, and that
/// index is how everything else refers to it.
#[pyclass(module = "subetha")]
struct Graph {
    inner: Box<SharedGraph<u64, u64>>,
}

#[pymethods]
impl Graph {
    /// The graph lives in two files beside the path given, one for the
    /// nodes and one for the edges.
    #[new]
    fn new(path: &str, max_nodes: usize, max_edges: usize) -> PyResult<Self> {
        if max_nodes < 1 || max_edges < 1 {
            return Err(PyValueError::new_err(
                "a graph holds at least one node and one edge",
            ));
        }
        SharedGraph::create(path, max_nodes, max_edges)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the graph", e))
    }

    /// Attach to a graph another holder made, with the sizes it was made
    /// with.
    #[staticmethod]
    fn open(path: &str, max_nodes: usize, max_edges: usize) -> PyResult<Self> {
        SharedGraph::open(path, max_nodes, max_edges)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the graph", e))
    }

    /// Add a node carrying `value`, answering its index.
    fn add_node(&self, value: u64) -> PyResult<u32> {
        self.inner
            .add_node(value)
            .map(|node| node.index)
            .map_err(|e| os_err("adding a node", e))
    }

    /// Add a run of nodes in one crossing, answering their indexes.
    fn add_nodes(&self, values: Vec<u64>) -> PyResult<Vec<u32>> {
        let mut added = Vec::with_capacity(values.len());
        for value in values {
            added.push(
                self.inner
                    .add_node(value)
                    .map(|node| node.index)
                    .map_err(|e| os_err("adding a node", e))?,
            );
        }
        Ok(added)
    }

    /// Add an edge from one node to another, carrying `value`, and
    /// answer its index.
    fn add_edge(&self, source: u32, target: u32, value: u64) -> PyResult<u32> {
        self.inner
            .add_edge(NodeIndex::new(source), NodeIndex::new(target), value)
            .map(|edge| edge.index)
            .map_err(|e| os_err("adding an edge", e))
    }

    /// Add a run of edges in one crossing, each a source, a target and a
    /// value.
    fn add_edges(&self, edges: Vec<(u32, u32, u64)>) -> PyResult<Vec<u32>> {
        let mut added = Vec::with_capacity(edges.len());
        for (source, target, value) in edges {
            added.push(
                self.inner
                    .add_edge(NodeIndex::new(source), NodeIndex::new(target), value)
                    .map(|edge| edge.index)
                    .map_err(|e| os_err("adding an edge", e))?,
            );
        }
        Ok(added)
    }

    /// What a node carries, or None when there is no such node.
    fn node_value(&self, node: u32) -> Option<u64> {
        self.inner.node_value(NodeIndex::new(node))
    }

    /// Everything reachable in one step from a node, as the edge index,
    /// the node it leads to and what the edge carries.
    fn neighbors(&self, source: u32) -> Vec<(u32, u32, u64)> {
        self.inner
            .neighbors(NodeIndex::new(source))
            .into_iter()
            .map(|(edge, target, value)| (edge.index, target.index, value))
            .collect()
    }

    /// How many edges lead out of a node, or None when there is no such
    /// node.
    fn out_degree(&self, source: u32) -> Option<u32> {
        self.inner.out_degree(NodeIndex::new(source))
    }

    /// Take an edge out of a node's edges, answering what it carried, or
    /// None when that node has no such edge.
    fn remove_edge(&self, source: u32, edge: u32) -> Option<u64> {
        self.inner
            .remove_edge(NodeIndex::new(source), EdgeIndex::new(edge))
    }

    #[getter]
    fn node_count(&self) -> usize {
        self.inner.node_count()
    }

    #[getter]
    fn edge_count(&self) -> usize {
        self.inner.edge_count()
    }

    #[getter]
    fn max_nodes(&self) -> usize {
        self.inner.max_nodes()
    }

    #[getter]
    fn max_edges(&self) -> usize {
        self.inner.max_edges()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// nodes and edges without this; flushing is about surviving a
    /// machine that stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Start writing the mapping back to its file and return at once,
    /// without waiting for the write to land. Nothing is on disk yet when
    /// this returns; `flush` is the form that waits.
    fn flush_async(&self) -> PyResult<()> {
        self.inner.flush_async().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Node and edge indices handed out inside the block still
    /// name the same nodes and edges after it, and nothing is flushed on
    /// the way out.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Graph {} nodes, {} edges>",
            self.inner.node_count(),
            self.inner.edge_count()
        )
    }
}

/// A set that changes how it stores itself as it grows.
///
/// A small set is a plain list, which is the fastest thing to walk when
/// there is little to walk. Past a point walking costs more than hashing
/// does, and the set moves itself to a map. `strategy` says which it is
/// using now, and `migrate_to` moves it by hand.
#[pyclass(module = "subetha")]
struct Universal {
    inner: Box<SharedUniversal<u64>>,
}

#[pymethods]
impl Universal {
    /// The set lives in files beside the path given, one per way of
    /// storing it.
    #[new]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("a set holds at least one value"));
        }
        SharedUniversal::create(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the set", e))
    }

    /// Attach to a set another holder made, with the capacity it was
    /// made with.
    #[staticmethod]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SharedUniversal::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the set", e))
    }

    /// Empty the set and remake it at this capacity.
    #[staticmethod]
    fn reset(path: &str, capacity: usize) -> PyResult<Self> {
        SharedUniversal::reset(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("resetting the set", e))
    }

    /// Add a value to the set.
    ///
    /// This is a set, so adding a value already there changes nothing and
    /// is not an error. The insert may be what tips the set from one
    /// storage strategy to the other, which `strategy` and `migrations`
    /// report; that happens underneath and nothing about the call says
    /// so.
    fn insert(&self, value: u64) -> PyResult<()> {
        self.inner
            .insert(value)
            .map_err(|e| os_err("inserting", e))
    }

    /// Add a run of values in one crossing.
    fn insert_many(&self, values: Vec<u64>) -> PyResult<usize> {
        for value in &values {
            self.inner
                .insert(*value)
                .map_err(|e| os_err("inserting", e))?;
        }
        Ok(values.len())
    }

    /// Whether the value is in the set. Exact, not probabilistic: unlike
    /// the filters, a `True` here is a fact.
    fn __contains__(&self, value: u64) -> PyResult<bool> {
        self.inner
            .contains(&value)
            .map_err(|e| os_err("looking up", e))
    }

    /// As `in`, spelled out.
    fn contains(&self, value: u64) -> PyResult<bool> {
        self.inner
            .contains(&value)
            .map_err(|e| os_err("looking up", e))
    }

    /// Ask about a run of values in one crossing.
    fn contains_many(&self, values: Vec<u64>) -> PyResult<Vec<bool>> {
        let mut found = Vec::with_capacity(values.len());
        for value in &values {
            found.push(
                self.inner
                    .contains(value)
                    .map_err(|e| os_err("looking up", e))?,
            );
        }
        Ok(found)
    }

    /// Everything in the set, in one crossing.
    fn snapshot(&self) -> PyResult<Vec<u64>> {
        self.inner.snapshot().map_err(|e| os_err("reading", e))
    }

    /// Take every value out, so the set is empty again. The storage
    /// strategy it had migrated to is kept rather than reset, and
    /// `migrations` goes on counting from where it was.
    fn clear(&self) -> PyResult<()> {
        self.inner.clear().map_err(|e| os_err("clearing", e))
    }

    /// How the set is stored right now, `list` or `map`.
    #[getter]
    fn strategy(&self) -> &'static str {
        strategy_name(self.inner.strategy())
    }

    /// Steps each time the set moves to another way of storing itself,
    /// so a holder can tell that what it read the shape of has changed.
    #[getter]
    fn migrations(&self) -> u32 {
        self.inner.strategy_version()
    }

    /// Steps only when `migrations` runs past what a thirty-two bit
    /// count holds and starts again. A holder comparing what it saw
    /// before has to compare this and `migrations` together, because on
    /// its own `migrations` can come back round to a number already
    /// seen.
    #[getter]
    fn generation(&self) -> u16 {
        self.inner.strategy_generation()
    }

    /// Move the set to `list` or `map` by hand. Moving it to where it
    /// already is does nothing.
    fn migrate_to(&self, strategy: &str) -> PyResult<()> {
        let target = match strategy {
            "list" => Strategy::Vec,
            "map" => Strategy::Map,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown strategy {other}, expected list or map"
                )))
            }
        };
        self.inner
            .migrate_to(target)
            .map_err(|e| os_err("migrating", e))
    }

    /// How many inserts and how many lookups have been made, in that
    /// order, which is what the set weighs when deciding to move itself.
    #[getter]
    fn op_counts(&self) -> (u64, u64) {
        self.inner.op_histogram()
    }

    /// How many values the set holds. It can raise, unlike most `len`
    /// implementations, because reading the count means reading the
    /// mapping and that can fail.
    fn __len__(&self) -> PyResult<usize> {
        self.inner.len().map_err(|e| os_err("reading", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Whatever strategy it migrated to inside the block is the
    /// one it is still in afterwards.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Universal stored as a {}>",
            strategy_name(self.inner.strategy())
        )
    }
}

fn strategy_name(strategy: Strategy) -> &'static str {
    match strategy {
        Strategy::Vec => "list",
        Strategy::Map => "map",
    }
}

/// Values reached by a path down through levels, where the path checks
/// itself on the way.
///
/// The bottom level holds the values. Every level above holds one link
/// per place, naming a place on the level below. So a value is reached
/// by a path of as many numbers as there are levels.
///
/// Reading does not simply follow the numbers it is given. At each level
/// it checks that the place named there really does point at the next
/// number in the path, and refuses at the first level where it does not,
/// saying which. That is the whole reason to use a tower rather than a
/// bare index into the bottom level: a path kept across a change that
/// rewrote a level in the middle comes back as a named refusal rather
/// than quietly resolving to whatever now sits at the end of it.
#[pyclass(module = "subetha")]
struct Tower {
    inner: Box<RawKTower>,
}

#[pymethods]
impl Tower {
    /// `path` is where the bottom level lives and `levels` the levels
    /// above it, each a file and how many places it holds. The levels
    /// are given top first. With no levels the tower is one deep, which
    /// is a plain region reached by a path of one number.
    #[new]
    fn new(
        path: &str,
        capacity: usize,
        value_size: usize,
        levels: Vec<(String, usize)>,
    ) -> PyResult<Self> {
        let levels = tower_levels(levels);
        RawKTower::create(path, capacity, value_size, &levels)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| tower_err("opening the tower", e))
    }

    /// Attach to a tower another holder made, with the shape it was made
    /// with.
    #[staticmethod]
    fn open(
        path: &str,
        capacity: usize,
        value_size: usize,
        levels: Vec<(String, usize)>,
    ) -> PyResult<Self> {
        let levels = tower_levels(levels);
        RawKTower::open(path, capacity, value_size, &levels)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| tower_err("attaching to the tower", e))
    }

    /// Store a value, taking a fresh place on the top level, and answer
    /// the path that reaches it.
    fn append(&self, value: &[u8]) -> PyResult<Vec<u32>> {
        let mut path = vec![0u32; self.inner.depth()];
        self.inner
            .append(value, &mut path)
            .map_err(|e| tower_err("storing a value", e))?;
        Ok(path)
    }

    /// Store a run of values in one crossing, answering a path for each.
    fn append_many(&self, values: Vec<Vec<u8>>) -> PyResult<Vec<Vec<u32>>> {
        let depth = self.inner.depth();
        let mut paths = Vec::with_capacity(values.len());
        for value in &values {
            let mut path = vec![0u32; depth];
            self.inner
                .append(value, &mut path)
                .map_err(|e| tower_err("storing a value", e))?;
            paths.push(path);
        }
        Ok(paths)
    }

    /// Store a value under a named place on the top level, rather than a
    /// fresh one, and answer the path that reaches it.
    ///
    /// The value is written first and the top link last, so another
    /// process walking down never reaches a level that does not yet name
    /// a live place below it.
    fn insert_at_top(&self, top: u32, value: &[u8]) -> PyResult<Vec<u32>> {
        let mut path = vec![0u32; self.inner.depth()];
        self.inner
            .insert_at_top(top, value, &mut path)
            .map_err(|e| tower_err("storing a value", e))?;
        Ok(path)
    }

    /// The value a path reaches. Raises when a level along the way no
    /// longer agrees with the path, naming that level.
    fn get(&self, path: Vec<u32>) -> PyResult<Vec<u8>> {
        let mut out = vec![0u8; self.inner.value_size()];
        self.inner
            .get(&path, &mut out)
            .map_err(|e| tower_err("reading a value", e))?;
        Ok(out)
    }

    /// Several paths in one crossing.
    ///
    /// A path the tower no longer agrees with stops the batch and is
    /// raised, naming which of the paths it was. Answering None for it
    /// instead would put the one thing this class exists to report back
    /// among the ordinary results.
    fn get_many(&self, paths: Vec<Vec<u32>>) -> PyResult<Vec<Vec<u8>>> {
        let size = self.inner.value_size();
        let mut found = Vec::with_capacity(paths.len());
        for (nth, path) in paths.iter().enumerate() {
            let mut out = vec![0u8; size];
            self.inner
                .get(path, &mut out)
                .map_err(|e| tower_err(&format!("reading path {nth}"), e))?;
            found.push(out);
        }
        Ok(found)
    }

    /// How many levels, which is how many numbers a path has.
    #[getter]
    fn depth(&self) -> usize {
        self.inner.depth()
    }

    /// How many bytes a value is. Every value is this size exactly.
    #[getter]
    fn value_size(&self) -> usize {
        self.inner.value_size()
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Every level goes, not just the bottom one.
    /// Another process mapping the same file sees the values without
    /// this; flushing is about surviving a machine that stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| tower_err("flushing", e))
    }

    /// Values stored at the bottom level.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Paths handed out inside the block still resolve to the
    /// same values after it, and nothing is flushed on the way out.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Tower {} deep, {} values of {} bytes>",
            self.inner.depth(),
            self.inner.len(),
            self.inner.value_size()
        )
    }
}

fn tower_levels(levels: Vec<(String, usize)>) -> Vec<(PathBuf, usize)> {
    levels
        .into_iter()
        .map(|(path, capacity)| (PathBuf::from(path), capacity))
        .collect()
}

fn tower_err(doing: &str, e: RawTowerError) -> PyErr {
    match e {
        RawTowerError::WrongSize { expected, found } => PyValueError::new_err(format!(
            "{doing}: this tower wants {expected} where {found} was given"
        )),
        RawTowerError::ZeroDepth => {
            PyValueError::new_err(format!("{doing}: a tower has at least one level"))
        }
        other => PyOSError::new_err(format!("{doing}: {other:?}")),
    }
}

/// What a stream needs, written down, so the shape underneath it can be
/// chosen to match rather than guessed at.
///
/// Four settings. `durability` is how long the bytes must outlive the
/// process that wrote them, and the place they should live follows from
/// it. `reliability` is whether a full ring may drop or must make the
/// sender wait. `keep_last` is how many items to hold, or None to hold
/// everything there is room for. `max_latency` is how long an item may
/// take, in seconds.
///
/// `ordering` is different from the other four and is set on its own.
/// Whether a reader needs one sender's order or every sender's order is
/// something only the application knows, so nothing here ever changes it
/// from watching the traffic.
///
/// This one is not shared between processes. It is the settings a
/// process holds while it decides what to build.
#[pyclass(module = "subetha")]
struct QosPolicy {
    inner: Box<SubethaQosPolicy>,
}

#[pymethods]
impl QosPolicy {
    /// `durability` is `volatile`, `transient` or `persistent`.
    /// `reliability` is `best_effort` or `reliable`.
    #[new]
    #[pyo3(signature = (
        durability = "volatile",
        reliability = "best_effort",
        keep_last = 1024,
        max_latency = 0.1,
    ))]
    fn new(
        durability: &str,
        reliability: &str,
        keep_last: Option<u32>,
        max_latency: f64,
    ) -> PyResult<Self> {
        let inner = SubethaQosPolicy::new(
            durability_from_name(durability)?,
            reliability_from_name(reliability)?,
            history_from(keep_last),
            duration_from(max_latency)?,
        );
        Ok(Self { inner: Box::new(inner) })
    }

    /// The settings for a stream that would rather lose an item than
    /// hold its sender up: in-process memory, dropping when full, the
    /// last thousand or so items, a tenth of a second.
    #[staticmethod]
    fn streaming() -> Self {
        Self { inner: Box::new(SubethaQosPolicy::streaming_default()) }
    }

    /// The settings for a stream nothing may fall out of: named memory
    /// other processes can reach, senders waiting rather than dropping.
    #[staticmethod]
    fn reliable_pubsub() -> Self {
        Self { inner: Box::new(SubethaQosPolicy::reliable_pubsub_default()) }
    }

    /// The settings for a stream that must survive the process: a
    /// mapped file, senders waiting, everything kept.
    #[staticmethod]
    fn persistent_log() -> Self {
        Self { inner: Box::new(SubethaQosPolicy::persistent_log_default()) }
    }

    #[getter]
    fn durability(&self) -> &'static str {
        durability_name(self.inner.durability())
    }

    #[setter]
    fn set_durability(&self, durability: &str) -> PyResult<()> {
        self.inner.set_durability(durability_from_name(durability)?);
        Ok(())
    }

    #[getter]
    fn reliability(&self) -> &'static str {
        reliability_name(self.inner.reliability())
    }

    #[setter]
    fn set_reliability(&self, reliability: &str) -> PyResult<()> {
        self.inner
            .set_reliability(reliability_from_name(reliability)?);
        Ok(())
    }

    /// How many items to hold, or None to hold everything there is room
    /// for.
    #[getter]
    fn keep_last(&self) -> Option<u32> {
        match self.inner.history() {
            History::KeepLastN(n) => Some(n),
            History::KeepAll => None,
        }
    }

    #[setter]
    fn set_keep_last(&self, keep_last: Option<u32>) {
        self.inner.set_history(history_from(keep_last));
    }

    /// How long an item may take, in seconds.
    #[getter]
    fn max_latency(&self) -> f64 {
        self.inner.max_latency().as_secs_f64()
    }

    #[setter]
    fn set_max_latency(&self, seconds: f64) -> PyResult<()> {
        self.inner.set_max_latency(duration_from(seconds)?);
        Ok(())
    }

    /// Whose order a reader needs: `per_producer` for each sender's own
    /// order, `global_fifo` for the order across all of them.
    #[getter]
    fn ordering(&self) -> &'static str {
        ordering_need_name(self.inner.ordering())
    }

    #[setter]
    fn set_ordering(&self, ordering: &str) -> PyResult<()> {
        self.inner.set_ordering(ordering_need_from_name(ordering)?);
        Ok(())
    }

    /// Every setting read together, so a decision is made against one
    /// consistent set rather than five separate reads.
    fn snapshot(&self) -> QosSnapshot {
        QosSnapshot { inner: self.inner.snapshot() }
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.QosPolicy {} {} ordering={}>",
            durability_name(self.inner.durability()),
            reliability_name(self.inner.reliability()),
            ordering_need_name(self.inner.ordering())
        )
    }
}

/// Every QoS setting as it stood at one moment, and what follows from
/// them.
#[pyclass(module = "subetha", frozen)]
struct QosSnapshot {
    inner: SubethaQosSnapshot,
}

#[pymethods]
impl QosSnapshot {
    #[getter]
    fn durability(&self) -> &'static str {
        durability_name(self.inner.durability)
    }

    #[getter]
    fn reliability(&self) -> &'static str {
        reliability_name(self.inner.reliability)
    }

    #[getter]
    fn keep_last(&self) -> Option<u32> {
        match self.inner.history {
            History::KeepLastN(n) => Some(n),
            History::KeepAll => None,
        }
    }

    #[getter]
    fn max_latency(&self) -> f64 {
        self.inner.max_latency.as_secs_f64()
    }

    #[getter]
    fn ordering(&self) -> &'static str {
        ordering_need_name(self.inner.ordering)
    }

    /// Where the bytes should live given how long they must last, or
    /// None when that is where they already are. The answer is one of
    /// `anon`, `file` or `shmfs`, which is what `LocaleRing.migrate_to`
    /// takes.
    fn recommends_locale_change(&self, current: &str) -> PyResult<Option<&'static str>> {
        let current = locale_from_name(current)?;
        Ok(self
            .inner
            .recommends_locale_change(current)
            .map(locale_name))
    }

    /// The ordering this asks for, when it is not the one already in
    /// force, and None when it is.
    fn recommends_ordering_change(&self, current: &str) -> PyResult<Option<&'static str>> {
        let current = ordering_need_from_name(current)?;
        Ok(self
            .inner
            .recommends_ordering_change(current)
            .map(ordering_need_name))
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.QosSnapshot {} {} ordering={}>",
            durability_name(self.inner.durability),
            reliability_name(self.inner.reliability),
            ordering_need_name(self.inner.ordering)
        )
    }
}

fn durability_name(durability: Durability) -> &'static str {
    match durability {
        Durability::Volatile => "volatile",
        Durability::Transient => "transient",
        Durability::Persistent => "persistent",
    }
}

fn durability_from_name(name: &str) -> PyResult<Durability> {
    match name {
        "volatile" => Ok(Durability::Volatile),
        "transient" => Ok(Durability::Transient),
        "persistent" => Ok(Durability::Persistent),
        other => Err(PyValueError::new_err(format!(
            "unknown durability {other}, expected volatile, transient or persistent"
        ))),
    }
}

fn reliability_name(reliability: Reliability) -> &'static str {
    match reliability {
        Reliability::BestEffort => "best_effort",
        Reliability::Reliable => "reliable",
    }
}

fn reliability_from_name(name: &str) -> PyResult<Reliability> {
    match name {
        "best_effort" => Ok(Reliability::BestEffort),
        "reliable" => Ok(Reliability::Reliable),
        other => Err(PyValueError::new_err(format!(
            "unknown reliability {other}, expected best_effort or reliable"
        ))),
    }
}

fn ordering_need_name(ordering: OrderingNeed) -> &'static str {
    match ordering {
        OrderingNeed::PerProducer => "per_producer",
        OrderingNeed::GlobalFifo => "global_fifo",
    }
}

fn ordering_need_from_name(name: &str) -> PyResult<OrderingNeed> {
    match name {
        "per_producer" => Ok(OrderingNeed::PerProducer),
        "global_fifo" => Ok(OrderingNeed::GlobalFifo),
        other => Err(PyValueError::new_err(format!(
            "unknown ordering {other}, expected per_producer or global_fifo"
        ))),
    }
}

fn history_from(keep_last: Option<u32>) -> History {
    match keep_last {
        Some(n) => History::KeepLastN(n),
        None => History::KeepAll,
    }
}

fn duration_from(seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(PyValueError::new_err(
            "a latency is a number of seconds that is not negative",
        ));
    }
    Ok(Duration::from_secs_f64(seconds))
}

/// The sending end of a link across a network that keeps working as the
/// network gets worse.
///
/// It sends more than the items themselves, so a reader can rebuild what
/// the network lost without asking for it again. There are two ways of
/// working out that extra, and which one is better depends on how much
/// is being lost: a sliding one is quicker while losses are light, and a
/// block one carries heavy sustained loss for less extra traffic. The
/// link measures the loss the reader reports and changes between them
/// while running, without either end reconnecting. `code` says which it
/// is on now and `switches` how many times it has changed.
///
/// The two differ in when a reader sees an item. Under the sliding code
/// an item can come out as soon as it arrives. Under the block code
/// items are grouped in eights, and a reader sees none of a group until
/// enough of it has arrived, so a run shorter than a group waits for the
/// rest of the group before any of it is delivered.
///
/// An item may be anything up to `max_item_size`, which is fixed when
/// the link is made and must match at both ends. The length travels with
/// each item, so a short one arrives as the short one it was.
#[pyclass(module = "subetha", unsendable)]
struct SensSender {
    inner: Box<UnifiedSensSender>,
    max_item: usize,
}

#[pymethods]
impl SensSender {
    /// `local` is the address to send from and `peer` the address to
    /// send to, each as host and port. `max_item_size` is the largest an
    /// item may be, and must be the same at both ends.
    ///
    /// `code` pins the way of working out the extra traffic: `rlc` for
    /// the sliding one, `rs` for the block one, or None, the default, to
    /// let the link choose from the loss it measures.
    #[new]
    #[pyo3(signature = (local, peer, max_item_size, code = None))]
    fn new(
        local: (String, u16),
        peer: (String, u16),
        max_item_size: usize,
        code: Option<&str>,
    ) -> PyResult<Self> {
        let peer = socket_addr_of(&peer)?;
        let mut config = UnifiedConfig::new(symbol_len_for(max_item_size)?);
        config.policy = code_policy_from_name(code)?;
        UnifiedSensSender::connect((local.0.as_str(), local.1), peer, config)
            .map(|inner| Self { inner: Box::new(inner), max_item: max_item_size })
            .map_err(|e| PyOSError::new_err(format!("opening the link: {e}")))
    }

    /// Send one item, of anything up to `max_item_size` bytes.
    fn send(&mut self, item: &[u8]) -> PyResult<()> {
        self.check_fits(item)?;
        self.inner
            .send_item(item)
            .map_err(|e| PyOSError::new_err(format!("sending: {e}")))
    }

    /// Send a run of items in one crossing, which is the shape to reach
    /// for: the work per item is small enough that the crossing would
    /// otherwise be most of the cost.
    ///
    /// Every item is checked for size before any is sent, so a run with
    /// one item too large sends none of them rather than stopping
    /// partway.
    fn send_many(&mut self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        for item in &items {
            self.check_fits(item)?;
        }
        for item in &items {
            self.inner
                .send_item(item)
                .map_err(|e| PyOSError::new_err(format!("sending: {e}")))?;
        }
        Ok(items.len())
    }

    /// The largest an item may be on this link.
    #[getter]
    fn max_item_size(&self) -> usize {
        self.max_item
    }

    /// The address this sends from, which is worth reading when the port
    /// was left to the system to pick.
    #[getter]
    fn local_addr(&self) -> PyResult<(String, u16)> {
        self.inner
            .local_addr()
            .map(|addr| (addr.ip().to_string(), addr.port()))
            .map_err(|e| PyOSError::new_err(format!("reading the address: {e}")))
    }

    /// Which way of working out the extra traffic is in use now, `rlc`
    /// or `rs`.
    #[getter]
    fn code(&self) -> &'static str {
        sens_code_name(self.inner.active_code())
    }

    /// How many times the link has changed between the two.
    #[getter]
    fn switches(&self) -> u64 {
        self.inner.switches()
    }

    /// The share of what was sent that the reader reports never
    /// arrived, between zero and one, or None before the reader has
    /// reported anything at all.
    ///
    /// None and zero are different answers: None is nothing measured
    /// yet, zero is measured and nothing lost. This is the number the
    /// link watches when deciding to change code.
    #[getter]
    fn loss(&self) -> Option<f64> {
        let measured = self.inner.raw_loss_estimate();
        // The Rust reports "no sample yet" as a negative share, which
        // read as a number would be a loss rate that cannot happen.
        (measured >= 0.0).then_some(measured)
    }

    /// Datagrams sent and datagrams received on this link, in that
    /// order. These count what went on the wire, which is more than the
    /// items, because the extra traffic is on it too.
    #[getter]
    fn datagrams(&self) -> (u64, u64) {
        self.inner.raw_sent_recv()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.SensSender on {} after {} switches>",
            sens_code_name(self.inner.active_code()),
            self.inner.switches()
        )
    }
}

/// The reading end of a link across a network that keeps working as the
/// network gets worse.
///
/// Items do not arrive one at a time. `poll` drives the link and answers
/// everything it could rebuild this time round, which may be nothing,
/// one item, or a run of them held back while a lost piece was being
/// recovered. A reader calls it in a loop.
#[pyclass(module = "subetha", unsendable)]
struct SensReceiver {
    inner: Box<UnifiedSensReceiver>,
    max_item: usize,
}

#[pymethods]
impl SensReceiver {
    /// `local` is the address to listen on, as host and port.
    /// `max_item_size` must be the one the sender was made with.
    #[new]
    #[pyo3(signature = (local, max_item_size, code = None))]
    fn new(local: (String, u16), max_item_size: usize, code: Option<&str>) -> PyResult<Self> {
        let mut config = UnifiedConfig::new(symbol_len_for(max_item_size)?);
        config.policy = code_policy_from_name(code)?;
        UnifiedSensReceiver::bind((local.0.as_str(), local.1), config)
            .map(|inner| Self { inner: Box::new(inner), max_item: max_item_size })
            .map_err(|e| PyOSError::new_err(format!("opening the link: {e}")))
    }

    /// The largest an item may be on this link.
    #[getter]
    fn max_item_size(&self) -> usize {
        self.max_item
    }

    /// Drive the link and answer every item it could rebuild this time
    /// round. An empty answer is ordinary and means nothing was ready,
    /// not that anything is wrong.
    fn poll(&mut self) -> PyResult<Vec<Vec<u8>>> {
        self.inner
            .poll()
            .map_err(|e| PyOSError::new_err(format!("reading: {e}")))
    }

    /// As `poll`, and also which sender each item came from, for a
    /// reader taking from several at once.
    fn poll_from(&mut self) -> PyResult<Vec<(u64, Vec<u8>)>> {
        self.inner
            .poll_from()
            .map_err(|e| PyOSError::new_err(format!("reading: {e}")))
    }

    /// The address this listens on, which is worth reading when the port
    /// was left to the system to pick.
    #[getter]
    fn local_addr(&self) -> PyResult<(String, u16)> {
        self.inner
            .local_addr()
            .map(|addr| (addr.ip().to_string(), addr.port()))
            .map_err(|e| PyOSError::new_err(format!("reading the address: {e}")))
    }

    /// Which way of working out the extra traffic is in use now, `rlc`
    /// or `rs`.
    #[getter]
    fn code(&self) -> &'static str {
        sens_code_name(self.inner.active_code())
    }

    /// How many times the link has changed between the two.
    #[getter]
    fn switches(&self) -> u64 {
        self.inner.switches()
    }

    /// Answers this end owed a sender and could not send: feedback, path
    /// checks, handshakes. Each one leaves a sender waiting, so a link
    /// that has gone quiet is worth reading this on.
    #[getter]
    fn send_failures(&self) -> u64 {
        self.inner.send_failures()
    }

    /// Whether the thread reading the socket is still running. A reader
    /// whose thread has stopped looks exactly like a healthy process
    /// nobody is sending to, which is why this is worth asking.
    #[getter]
    fn alive(&self) -> bool {
        self.inner.demux_alive()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.SensReceiver on {} after {} switches>",
            sens_code_name(self.inner.active_code()),
            self.inner.switches()
        )
    }
}

impl SensSender {
    fn check_fits(&self, item: &[u8]) -> PyResult<()> {
        if item.len() > self.max_item {
            return Err(PyValueError::new_err(format!(
                "an item on this link is at most {} bytes, got {}",
                self.max_item,
                item.len()
            )));
        }
        Ok(())
    }
}

/// The buffer one item travels in: the item itself plus the two bytes
/// that record how long it is, so a short item arrives short rather than
/// padded out to the full size.
fn symbol_len_for(max_item_size: usize) -> PyResult<usize> {
    if max_item_size < 1 {
        return Err(PyValueError::new_err("an item is at least one byte"));
    }
    Ok(max_item_size + 2)
}

fn sens_code_name(code: SensCode) -> &'static str {
    match code {
        SensCode::Rlc => "rlc",
        SensCode::Rs => "rs",
    }
}

fn code_policy_from_name(code: Option<&str>) -> PyResult<CodePolicy> {
    match code {
        None => Ok(CodePolicy::default_auto()),
        Some("rlc") => Ok(CodePolicy::ForceRlc),
        Some("rs") => Ok(CodePolicy::ForceRs),
        Some(other) => Err(PyValueError::new_err(format!(
            "unknown code {other}, expected rlc or rs, or None to choose from the loss"
        ))),
    }
}

/// One address from a host and a port, resolved here so a caller never
/// has to spell a socket address as one string.
fn socket_addr_of(addr: &(String, u16)) -> PyResult<SocketAddr> {
    (addr.0.as_str(), addr.1)
        .to_socket_addrs()
        .map_err(|e| PyValueError::new_err(format!("{}:{} is not an address: {e}", addr.0, addr.1)))?
        .next()
        .ok_or_else(|| PyValueError::new_err(format!("{}:{} named no address", addr.0, addr.1)))
}

/// One runtime for every bridge in the process, started the first time
/// a bridge needs it.
///
/// The bridges are written against an async runtime and the calls here
/// are ordinary blocking ones, so each waits on this. One runtime rather
/// than one per bridge, because a runtime owns threads and a process
/// that makes several bridges should not pay for several sets of them.
#[cfg(any(feature = "tcp-bridge", feature = "quic-bridge"))]
static BRIDGE_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

#[cfg(any(feature = "tcp-bridge", feature = "quic-bridge"))]
fn bridge_runtime() -> PyResult<&'static tokio::runtime::Runtime> {
    if let Some(running) = BRIDGE_RUNTIME.get() {
        return Ok(running);
    }
    let built = tokio::runtime::Runtime::new()
        .map_err(|e| PyOSError::new_err(format!("starting the bridge runtime: {e}")))?;
    // A set that fails hands back the runtime it could not store,
    // because another thread published one first. Both are equally
    // good, so the one that lost the race is shut down here and every
    // caller goes on to use the winner's.
    if let Err(surplus) = BRIDGE_RUNTIME.set(built) {
        surplus.shutdown_background();
    }
    BRIDGE_RUNTIME
        .get()
        .ok_or_else(|| PyOSError::new_err("the bridge runtime could not be started"))
}

/// The sending end of a bridge carrying a ring's items over a TCP
/// connection to another host.
///
/// The ring is the one this process already writes to. The bridge takes
/// what is in it and ships it, so a process that fills a ring locally
/// reaches a reader on another machine without changing how it writes.
#[cfg(feature = "tcp-bridge")]
#[pyclass(module = "subetha")]
struct TcpBridgeClient {
    ring: Arc<AdaptiveRing>,
    server: SocketAddr,
}

#[cfg(feature = "tcp-bridge")]
#[pymethods]
impl TcpBridgeClient {
    /// `ring` is the ring to take items from and `server` the address
    /// of the reading end, as host and port.
    #[new]
    fn new(ring: &Ring, server: (String, u16)) -> PyResult<Self> {
        Ok(Self {
            ring: Arc::clone(&ring.inner),
            server: socket_addr_of(&server)?,
        })
    }

    /// Connect and ship `items` of them, waiting until all have gone.
    ///
    /// Other Python threads run while this waits, so a reader in the
    /// same process is not held up by it.
    fn run(&self, py: Python<'_>, items: u64) -> PyResult<()> {
        let bridge = SubethaTcpClient::new(Arc::clone(&self.ring), self.server);
        let runtime = bridge_runtime()?;
        py.detach(|| runtime.block_on(bridge.run(items)))
            .map_err(|e| PyOSError::new_err(format!("shipping: {e:?}")))
    }

    fn __repr__(&self) -> String {
        format!("<subetha.TcpBridgeClient to {}>", self.server)
    }
}

/// The reading end of a bridge, putting what arrives into a ring this
/// process reads from.
#[cfg(feature = "tcp-bridge")]
#[pyclass(module = "subetha")]
struct TcpBridgeServer {
    inner: Arc<SubethaTcpServer>,
}

#[cfg(feature = "tcp-bridge")]
#[pymethods]
impl TcpBridgeServer {
    /// `ring` is the ring to put arriving items into and `local` the
    /// address to listen on, as host and port. A port of zero lets the
    /// system pick one, which `local_addr` then reports.
    #[new]
    fn new(py: Python<'_>, ring: &Ring, local: (String, u16)) -> PyResult<Self> {
        let addr = socket_addr_of(&local)?;
        let shared = Arc::clone(&ring.inner);
        let runtime = bridge_runtime()?;
        let inner = py
            .detach(|| runtime.block_on(SubethaTcpServer::bind(shared, addr)))
            .map_err(|e| PyOSError::new_err(format!("listening: {e:?}")))?;
        Ok(Self { inner: Arc::new(inner) })
    }

    /// The address this listens on, worth reading when the port was
    /// left to the system to pick.
    #[getter]
    fn local_addr(&self) -> PyResult<(String, u16)> {
        self.inner
            .local_addr()
            .map(|addr| (addr.ip().to_string(), addr.port()))
            .map_err(|e| PyOSError::new_err(format!("reading the address: {e}")))
    }

    /// Take one connection, read it to its end, and answer how many
    /// items arrived. Waits until the sending end has finished.
    ///
    /// Other Python threads run while this waits, which is what lets the
    /// sending end live in the same process.
    fn accept_one(&self, py: Python<'_>) -> PyResult<u64> {
        let listening = Arc::clone(&self.inner);
        let runtime = bridge_runtime()?;
        py.detach(|| runtime.block_on(listening.accept_one()))
            .map_err(|e| PyOSError::new_err(format!("reading: {e:?}")))
    }

    fn __repr__(&self) -> String {
        match self.inner.local_addr() {
            Ok(addr) => format!("<subetha.TcpBridgeServer on {addr}>"),
            Err(e) => format!("<subetha.TcpBridgeServer address unreadable: {e}>"),
        }
    }
}

/// Make a certificate and its key for a QUIC bridge, both as bytes.
///
/// `name` is what the certificate is issued for and what a client
/// passes as `server_name`. It names the certificate rather than the
/// address, so any address the reading end is reachable at works.
///
/// The certificate is signed by nobody, so the reading end holds both
/// of these and the sending end holds the certificate alone, which is
/// what it checks the reading end against. Carrying it between hosts is
/// the caller's to arrange.
#[cfg(feature = "quic-bridge")]
#[pyfunction]
fn generate_self_signed_cert(name: &str) -> PyResult<(Vec<u8>, Vec<u8>)> {
    quic_bridge::generate_self_signed_cert(name)
        .map_err(|e| PyOSError::new_err(format!("making a certificate: {e:?}")))
}

/// The sending end of a bridge carrying a ring's items over QUIC.
#[cfg(feature = "quic-bridge")]
#[pyclass(module = "subetha")]
struct QuicBridgeClient {
    ring: Arc<AdaptiveRing>,
    server: SocketAddr,
    /// The certificate bytes rather than the config built from them, so
    /// the QUIC library's types stay inside the crate that owns them.
    /// Building the config again per run costs nothing beside opening a
    /// connection, which is what a run does.
    cert: Vec<u8>,
    server_name: String,
    bind: SocketAddr,
}

#[cfg(feature = "quic-bridge")]
#[pymethods]
impl QuicBridgeClient {
    /// `ring` is the ring to take items from, `server` the address of
    /// the reading end, and `cert` the certificate bytes that end was
    /// made with. `server_name` must be the name the certificate was
    /// issued for.
    ///
    /// `local` is the address to send from, and a port of zero lets the
    /// system pick one.
    #[new]
    #[pyo3(signature = (ring, server, cert, server_name, local = None))]
    fn new(
        ring: &Ring,
        server: (String, u16),
        cert: &[u8],
        server_name: &str,
        local: Option<(String, u16)>,
    ) -> PyResult<Self> {
        quic_bridge::install_default_crypto_provider();
        // Built and thrown away here so a certificate that cannot be
        // read is refused when the client is made, rather than on the
        // first attempt to send.
        quic_bridge::make_client_config_from_der(cert)
            .map_err(|e| PyOSError::new_err(format!("trusting the certificate: {e:?}")))?;
        let bind = match local {
            Some(addr) => socket_addr_of(&addr)?,
            None => SocketAddr::from(([0, 0, 0, 0], 0)),
        };
        Ok(Self {
            ring: Arc::clone(&ring.inner),
            server: socket_addr_of(&server)?,
            cert: cert.to_vec(),
            server_name: server_name.to_string(),
            bind,
        })
    }

    /// Connect and ship `items` of them, waiting until the reading end
    /// has acknowledged the last of them.
    fn run(&self, py: Python<'_>, items: u64) -> PyResult<()> {
        let config = quic_bridge::make_client_config_from_der(&self.cert)
            .map_err(|e| PyOSError::new_err(format!("trusting the certificate: {e:?}")))?;
        let bridge = SubethaQuicClient::new(
            Arc::clone(&self.ring),
            self.server,
            config,
            self.bind,
        );
        let runtime = bridge_runtime()?;
        let name = self.server_name.clone();
        py.detach(|| runtime.block_on(bridge.run(items, &name)))
            .map_err(|e| PyOSError::new_err(format!("shipping: {e:?}")))
    }

    fn __repr__(&self) -> String {
        format!("<subetha.QuicBridgeClient to {}>", self.server)
    }
}

/// The reading end of a QUIC bridge, putting what arrives into a ring
/// this process reads from.
#[cfg(feature = "quic-bridge")]
#[pyclass(module = "subetha")]
struct QuicBridgeServer {
    inner: Arc<SubethaQuicServer>,
}

#[cfg(feature = "quic-bridge")]
#[pymethods]
impl QuicBridgeServer {
    /// `ring` is the ring to put arriving items into, `local` the
    /// address to listen on, and `cert` and `key` the certificate and
    /// key this end proves itself with. A port of zero lets the system
    /// pick one, which `local_addr` then reports.
    #[new]
    fn new(ring: &Ring, local: (String, u16), cert: &[u8], key: &[u8]) -> PyResult<Self> {
        quic_bridge::install_default_crypto_provider();
        let config = quic_bridge::make_server_config_from_der(cert, key)
            .map_err(|e| PyOSError::new_err(format!("reading the certificate: {e:?}")))?;
        let addr = socket_addr_of(&local)?;
        // Binding is not an async call but it builds an endpoint that
        // registers with the runtime, so it has to happen inside one.
        let runtime = bridge_runtime()?;
        let entered = runtime.enter();
        let bound = SubethaQuicServer::bind(Arc::clone(&ring.inner), addr, config)
            .map_err(|e| PyOSError::new_err(format!("listening: {e:?}")));
        drop(entered);
        Ok(Self { inner: Arc::new(bound?) })
    }

    /// The address this listens on.
    #[getter]
    fn local_addr(&self) -> PyResult<(String, u16)> {
        self.inner
            .local_addr()
            .map(|addr| (addr.ip().to_string(), addr.port()))
            .map_err(|e| PyOSError::new_err(format!("reading the address: {e}")))
    }

    /// Take one connection, read it to its end, and answer how many
    /// items arrived.
    fn accept_one(&self, py: Python<'_>) -> PyResult<u64> {
        let listening = Arc::clone(&self.inner);
        let runtime = bridge_runtime()?;
        py.detach(|| runtime.block_on(listening.accept_one()))
            .map_err(|e| PyOSError::new_err(format!("reading: {e:?}")))
    }

    fn __repr__(&self) -> String {
        match self.inner.local_addr() {
            Ok(addr) => format!("<subetha.QuicBridgeServer on {addr}>"),
            Err(e) => format!("<subetha.QuicBridgeServer address unreadable: {e}>"),
        }
    }
}

/// The transports this wheel was built with, as names.
///
/// The Sens-O-Matic link is always here. The bridges are left out by
/// default because each brings a network stack with it, and a process
/// sharing memory with another on the same host needs none of it. A
/// caller looking for one it cannot find reads this to tell a wheel
/// built without it from a name that never existed.
fn transports_built() -> Vec<&'static str> {
    let mut built = vec!["sens"];
    if cfg!(feature = "tcp-bridge") {
        built.push("tcp");
    }
    if cfg!(feature = "quic-bridge") {
        built.push("quic");
    }
    built
}

/// The front door: a queue between processes that can be waited on.
///
/// This is the shape to reach for when what is wanted is simply to send
/// things to another process and read them back. `Ring` and its
/// relations underneath give more control over the shape; this one
/// picks the shape from the number of senders and readers named here,
/// and adds the one thing they do not have, which is waiting.
///
/// `recv` answers None the moment there is nothing there. `recv_for`
/// waits, and while it waits the interpreter is free, so other Python
/// threads run. That is what makes it usable as the only thing a
/// reader does.
///
/// An item may be anything up to `max_item_size` bytes.
#[pyclass(module = "subetha")]
struct Channel {
    inner: Box<SubethaChannel<SlotValue>>,
}

#[pymethods]
impl Channel {
    /// `capacity` is the number of items in flight, rounded up to a
    /// power of two. `senders` and `readers` are how many are expected
    /// at once, which is what the shape underneath is picked from.
    #[new]
    #[pyo3(signature = (path, capacity = 1024, senders = 1, readers = 1))]
    fn new(path: &str, capacity: usize, senders: usize, readers: usize) -> PyResult<Self> {
        if senders < 1 || readers < 1 {
            return Err(PyValueError::new_err(
                "a channel needs at least one sender and one reader",
            ));
        }
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: senders,
            n_consumers: readers,
        };
        SubethaChannel::create(path, shape, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| api_err("opening the channel", e))
    }

    /// Attach to a channel another holder made, with the capacity it
    /// was made with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity = 1024))]
    fn open(path: &str, capacity: usize) -> PyResult<Self> {
        SubethaChannel::open(path, capacity)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| api_err("attaching to the channel", e))
    }

    /// Send one item, answering False when the channel is full.
    fn send(&self, item: &[u8]) -> PyResult<bool> {
        let held = SlotValue::from_bytes(item)?;
        match self.inner.send(&held) {
            Ok(()) => Ok(true),
            Err(e) if is_full(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// Send a run of items in one crossing, answering how many went.
    /// A short answer means the channel filled.
    fn send_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut sent = 0;
        for item in &items {
            let held = SlotValue::from_bytes(item)?;
            match self.inner.send(&held) {
                Ok(()) => sent += 1,
                Err(e) if is_full(&e) => break,
                Err(e) => return Err(api_err("sending", e)),
            }
        }
        Ok(sent)
    }

    /// Wait until the item can be sent, or until `timeout` seconds have
    /// passed. None waits as long as it takes. Answers False on a
    /// timeout.
    ///
    /// Other Python threads run while this waits.
    #[pyo3(signature = (item, timeout = None))]
    fn send_for(&self, py: Python<'_>, item: &[u8], timeout: Option<f64>) -> PyResult<bool> {
        let held = SlotValue::from_bytes(item)?;
        let wait = optional_duration(timeout)?;
        match py.detach(|| self.inner.send_blocking(&held, wait)) {
            Ok(()) => Ok(true),
            Err(e) if is_timeout(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// The next item, or None when there is nothing there.
    fn recv(&self) -> PyResult<Option<Vec<u8>>> {
        match self.inner.recv() {
            Ok(held) => Ok(Some(held.as_bytes())),
            Err(e) if is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }

    /// Everything waiting, up to `max_items`, in one crossing.
    #[pyo3(signature = (max_items = 256))]
    fn recv_many(&self, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        for _ in 0..max_items {
            match self.inner.recv() {
                Ok(held) => taken.push(held.as_bytes()),
                Err(e) if is_empty(&e) => break,
                Err(e) => return Err(api_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Wait for the next item, or until `timeout` seconds have passed.
    /// None waits as long as it takes. Answers None on a timeout.
    ///
    /// Other Python threads run while this waits, which is what lets a
    /// reader do nothing else.
    #[pyo3(signature = (timeout = None))]
    fn recv_for(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Vec<u8>>> {
        let wait = optional_duration(timeout)?;
        match py.detach(|| self.inner.recv_blocking(wait)) {
            Ok(held) => Ok(Some(held.as_bytes())),
            Err(e) if is_timeout(&e) || is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }

    /// The most an item may be, in bytes.
    #[classattr]
    fn max_item_size() -> usize {
        SLOT_VALUE_BYTES
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing the channel, and let an exception
    /// through. Nothing is signaled to the other end: a reader waiting
    /// on it goes on waiting, so a sender that means to say it is done
    /// has to say so in the messages it sends.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        "<subetha.Channel>".to_string()
    }
}

/// A queue that changes what it is underneath as the traffic changes.
///
/// The other front-door classes pick a shape once, from what the caller
/// says it expects. This one picks from what actually happens: it
/// watches the sizes it is sent and moves between a ring and a
/// work-stealing deque while running, without either end reconnecting.
/// `shape` says which it is on now and `promotions` how many times it
/// has moved.
///
/// Worth it when the traffic is not known in advance or changes over a
/// run. When it is known, saying so through `Channel` or `WorkQueue` is
/// cheaper, because this one pays a counter on every send.
#[pyclass(module = "subetha")]
struct AdaptiveQueue {
    inner: Box<SubethaAdaptiveIpc<SlotValue>>,
}

#[pymethods]
impl AdaptiveQueue {
    /// `capacity` is the number of items in flight, rounded up to a
    /// power of two. `senders` and `readers` are where it starts; what
    /// it becomes is decided by the traffic.
    ///
    /// `ordering` is `per_producer`, each sender's own order, or
    /// `global_fifo`, the order across all of them. It is a setting a
    /// caller makes, never inferred from the traffic, because only the
    /// application knows which its readers need.
    ///
    /// `auto_order` is the one exception, and it is a pre-authorization
    /// rather than an inference: give it a number of cross-sender
    /// inversions per second and the queue is allowed to turn global
    /// ordering on by itself once it sees that many. None, the default,
    /// means it never does.
    #[new]
    #[pyo3(signature = (
        path,
        capacity = 1024,
        senders = 1,
        readers = 1,
        ordering = "per_producer",
        auto_order = None,
    ))]
    fn new(
        path: &str,
        capacity: usize,
        senders: usize,
        readers: usize,
        ordering: &str,
        auto_order: Option<f64>,
    ) -> PyResult<Self> {
        if senders < 1 || readers < 1 {
            return Err(PyValueError::new_err(
                "a queue needs at least one sender and one reader",
            ));
        }
        if let Some(rate) = auto_order
            && (!rate.is_finite() || rate < 0.0)
        {
            return Err(PyValueError::new_err(
                "auto_order is a number of inversions a second that is not negative",
            ));
        }
        let shape = MmfWorkloadShape::StreamingMpmc {
            n_producers: senders,
            n_consumers: readers,
        };
        SubethaAdaptiveIpc::create_with_ordering(
            path,
            shape,
            capacity,
            readers,
            ordering_need_from_name(ordering)?,
            auto_order,
        )
        .map(|inner| Self { inner: Box::new(inner) })
        .map_err(|e| api_err("opening the queue", e))
    }

    /// Send one item, answering False when it is full.
    fn send(&self, item: &[u8]) -> PyResult<bool> {
        let held = SlotValue::from_bytes(item)?;
        match self.inner.send(&held) {
            Ok(()) => Ok(true),
            Err(e) if is_full(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// Send a run of items as one batch, which is also what tells the
    /// queue the traffic comes in batches and may be worth a different
    /// shape.
    fn send_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut held = Vec::with_capacity(items.len());
        for item in &items {
            held.push(SlotValue::from_bytes(item)?);
        }
        match self.inner.send_batch(&held) {
            Ok(()) => Ok(held.len()),
            Err(e) if is_full(&e) => Ok(0),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// The next item, or None when there is nothing there.
    fn recv(&self) -> PyResult<Option<Vec<u8>>> {
        match self.inner.recv() {
            Ok(held) => Ok(Some(held.as_bytes())),
            Err(e) if is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }

    /// Everything waiting, up to `max_items`, in one crossing.
    #[pyo3(signature = (max_items = 256))]
    fn recv_many(&self, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        for _ in 0..max_items {
            match self.inner.recv() {
                Ok(held) => taken.push(held.as_bytes()),
                Err(e) if is_empty(&e) => break,
                Err(e) => return Err(api_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Wait until the item can be sent, or until `timeout` seconds have
    /// passed. Answers False on a timeout.
    #[pyo3(signature = (item, timeout = None))]
    fn send_for(&self, py: Python<'_>, item: &[u8], timeout: Option<f64>) -> PyResult<bool> {
        let held = SlotValue::from_bytes(item)?;
        let wait = optional_duration(timeout)?;
        match py.detach(|| self.inner.send_blocking(&held, wait)) {
            Ok(()) => Ok(true),
            Err(e) if is_timeout(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// Wait for the next item, or until `timeout` seconds have passed.
    /// Answers None on a timeout.
    #[pyo3(signature = (timeout = None))]
    fn recv_for(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Vec<u8>>> {
        let wait = optional_duration(timeout)?;
        match py.detach(|| self.inner.recv_blocking(wait)) {
            Ok(held) => Ok(Some(held.as_bytes())),
            Err(e) if is_timeout(&e) || is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }

    /// What the queue is right now, `ring` or `work_stealing`.
    #[getter]
    fn shape(&self) -> &'static str {
        family_name(self.inner.active_family())
    }

    /// Look at the traffic so far and move to the shape that fits it,
    /// answering the new shape or None when the one it has already
    /// fits.
    fn maybe_change_shape(&self) -> PyResult<Option<&'static str>> {
        self.inner
            .maybe_promote()
            .map(|moved| moved.map(family_name))
            .map_err(|e| api_err("changing shape", e))
    }

    /// Move to a named shape, whatever the traffic says.
    fn change_shape_to(&self, shape: &str) -> PyResult<()> {
        let target = family_from_name(shape)?;
        self.inner
            .migrate_to(target)
            .map_err(|e| api_err("changing shape", e))
    }

    /// Steps every time the shape changes, so a holder can tell that
    /// what it looked at has been superseded.
    #[getter]
    fn shape_generation(&self) -> u64 {
        self.inner.pin_generation()
    }

    /// The average number of items a send carried, and the share of
    /// sends that were batches. This is what the queue weighs when
    /// deciding to change shape.
    #[getter]
    fn traffic(&self) -> (u64, f64) {
        let seen = self.inner.profile_snapshot();
        (seen.avg_batch_size(), seen.batch_ratio())
    }

    /// Whose order a reader gets, `per_producer` or `global_fifo`.
    #[getter]
    fn ordering(&self) -> &'static str {
        ordering_need_name(self.inner.ordering())
    }

    #[setter]
    fn set_ordering(&self, ordering: &str) -> PyResult<()> {
        self.inner
            .set_ordering(ordering_need_from_name(ordering)?)
            .map_err(|e| api_err("setting the ordering", e))
    }

    /// Cross-sender inversions seen so far, which is what says whether
    /// the ordering asked for is being met.
    #[getter]
    fn inversions(&self) -> u64 {
        self.inner.inversions()
    }

    /// The most an item may be, in bytes.
    #[classattr]
    fn max_item_size() -> usize {
        SLOT_VALUE_BYTES
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Whatever shape it changed into inside the block is the
    /// one it is still in afterwards, and anything queued stays queued.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.AdaptiveQueue shaped as a {}>",
            family_name(self.inner.active_family())
        )
    }
}

fn family_name(family: MmfFamily) -> &'static str {
    match family {
        MmfFamily::SharedRing => "ring",
        MmfFamily::SharedDeque(_) => "work_stealing",
        MmfFamily::SharedHashMap => "map",
    }
}

fn family_from_name(name: &str) -> PyResult<MmfFamily> {
    match name {
        "ring" => Ok(MmfFamily::SharedRing),
        "work_stealing" => Ok(MmfFamily::SharedDeque(DequeVariant::ChaseLev)),
        other => Err(PyValueError::new_err(format!(
            "unknown shape {other}, expected ring or work_stealing"
        ))),
    }
}

/// Work one process owns and others take from when they are idle.
///
/// The owner pushes and pops at one end, which is the cheap end and the
/// one it gets to itself. Everybody else steals from the other end. The
/// two ends are what make it worth using over a ring: the owner's own
/// work costs nothing to hand out, and a thief only pays when it
/// actually takes something.
///
/// The owner makes the queue. A thief attaches with `steal_from`.
///
/// An item may be anything up to `max_item_size` bytes.
#[pyclass(module = "subetha")]
struct WorkQueue {
    inner: Box<SubethaWorkStealQueue<SlotValue>>,
}

#[pymethods]
impl WorkQueue {
    /// `capacity` is the number of items in flight, rounded up to a
    /// power of two. `thieves` is how many are expected to take from
    /// it, which is what the shape underneath is picked from.
    #[new]
    #[pyo3(signature = (path, capacity = 1024, thieves = 1))]
    fn new(path: &str, capacity: usize, thieves: usize) -> PyResult<Self> {
        if thieves < 1 {
            return Err(PyValueError::new_err("a queue needs at least one thief"));
        }
        AutoIpc::new(path)
            .consumers(thieves)
            // A batch hint is what turns the inference toward
            // work-stealing rather than streaming; without it the
            // dispatcher picks a ring and the build is refused.
            .batch_size(2)
            .capacity(capacity)
            .build_work_steal_queue::<SlotValue>()
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| api_err("opening the queue", e))
    }

    /// Attach to a queue somebody else owns, to take from it.
    #[staticmethod]
    fn steal_from(path: &str) -> PyResult<Self> {
        SubethaWorkStealQueue::open_as_thief(path)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| api_err("attaching to the queue", e))
    }

    /// Add work, at the owner's end.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        let held = SlotValue::from_bytes(item)?;
        match self.inner.push(&held) {
            Ok(()) => Ok(true),
            Err(e) if is_full(&e) => Ok(false),
            Err(e) => Err(api_err("pushing", e)),
        }
    }

    /// Add a run of work in one crossing, answering how many went in.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            let held = SlotValue::from_bytes(item)?;
            match self.inner.push(&held) {
                Ok(()) => pushed += 1,
                Err(e) if is_full(&e) => break,
                Err(e) => return Err(api_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// Take the owner's own next piece of work, the most recent one it
    /// added, or None when there is none.
    fn pop(&self) -> Option<Vec<u8>> {
        self.inner.pop().map(|held| held.as_bytes())
    }

    /// Take a piece of work from the far end, which is what a thief
    /// does, or None when there is none to take.
    fn steal(&self) -> Option<Vec<u8>> {
        self.inner.steal().map(|held| held.as_bytes())
    }

    /// Take up to `max_items` by stealing, in one crossing.
    #[pyo3(signature = (max_items = 64))]
    fn steal_many(&self, max_items: usize) -> Vec<Vec<u8>> {
        let mut taken = Vec::new();
        for _ in 0..max_items {
            match self.inner.steal() {
                Some(held) => taken.push(held.as_bytes()),
                None => break,
            }
        }
        taken
    }

    /// The most an item may be, in bytes.
    #[classattr]
    fn max_item_size() -> usize {
        SLOT_VALUE_BYTES
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Work still in the queue stays there for whoever attaches
    /// next, and no worker is told to stop.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        "<subetha.WorkQueue>".to_string()
    }
}

/// A map between processes, reached without naming a shape.
///
/// The companion to `Channel` in the front door: keys and values are
/// both unsigned integers, and the shape underneath is picked from how
/// many readers and writers are expected.
#[pyclass(module = "subetha")]
struct KvMap {
    inner: Box<SubethaKvMap<u64, u64>>,
}

#[pymethods]
impl KvMap {
    /// Obtain the map at `path`, creating it when the file does not
    /// exist. Keys and values are both sixty-four bit integers.
    ///
    /// `readers` and `writers` are how many of each will take part, and
    /// the shape underneath is chosen from them rather than named: this
    /// is the surface for a caller who wants a map between processes
    /// without picking one. Either below one is a `ValueError`.
    #[new]
    #[pyo3(signature = (path, capacity = 1024, readers = 1, writers = 1))]
    fn new(path: &str, capacity: usize, readers: usize, writers: usize) -> PyResult<Self> {
        if readers < 1 || writers < 1 {
            return Err(PyValueError::new_err(
                "a map needs at least one reader and one writer",
            ));
        }
        AutoIpc::new(path)
            .consumers(readers)
            .producers(writers)
            .capacity(capacity)
            .build_kv_map::<u64, u64>()
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| api_err("opening the map", e))
    }

    /// Put an entry in, answering True when the key was not there
    /// before and False when this replaced what it held.
    ///
    /// It does not answer the old value. The map underneath reports
    /// only which of the two happened, and inventing a read to go with
    /// it would be a second look the caller did not ask for.
    fn insert(&self, key: u64, value: u64) -> PyResult<bool> {
        self.inner
            .insert(key, value)
            .map(|outcome| matches!(outcome, InsertOutcome::Inserted))
            .map_err(|e| api_err("inserting", e))
    }

    /// Put a run of entries in, in one crossing, answering True for
    /// each key that was not there before.
    fn insert_many(&self, entries: Vec<(u64, u64)>) -> PyResult<Vec<bool>> {
        let mut fresh = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            fresh.push(
                self.inner
                    .insert(key, value)
                    .map(|outcome| matches!(outcome, InsertOutcome::Inserted))
                    .map_err(|e| api_err("inserting", e))?,
            );
        }
        Ok(fresh)
    }

    /// What a key holds, or None when it holds nothing.
    fn get(&self, key: u64) -> Option<u64> {
        self.inner.get(&key)
    }

    /// Several keys in one crossing.
    fn get_many(&self, keys: Vec<u64>) -> Vec<Option<u64>> {
        keys.into_iter().map(|key| self.inner.get(&key)).collect()
    }

    /// Whether the key has a value. Reads the value and throws it away,
    /// so it costs what `get` costs; call `get` when you want the value
    /// as well rather than asking twice.
    fn __contains__(&self, key: u64) -> bool {
        self.inner.get(&key).is_some()
    }

    /// How many keys the map holds.
    ///
    /// There is no way to take a key out. The map underneath has no
    /// removal, and a key can only be written over. `HashMap` is the
    /// one to reach for when entries have to go away.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The entries stay in the file for whoever attaches next.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!("<subetha.KvMap holding {}>", self.inner.len())
    }
}

/// Seconds as a duration, or None to wait as long as it takes.
fn optional_duration(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    match seconds {
        None => Ok(None),
        Some(s) => Ok(Some(duration_from(s)?)),
    }
}

/// Whether this failure is the channel being full, which is an answer
/// rather than a fault.
fn is_full(e: &ApiError) -> bool {
    matches!(e, ApiError::Transport(TransportError::Full))
}

/// Whether this failure is the channel being empty.
fn is_empty(e: &ApiError) -> bool {
    matches!(e, ApiError::Transport(TransportError::Empty))
}

/// Whether this failure is the wait running out rather than anything
/// going wrong.
fn is_timeout(e: &ApiError) -> bool {
    matches!(e, ApiError::Timeout)
}

fn api_err(doing: &str, e: ApiError) -> PyErr {
    match e {
        ApiError::PayloadTooLarge => {
            PyValueError::new_err(format!("{doing}: the item does not fit a slot"))
        }
        other => PyOSError::new_err(format!("{doing}: {other}")),
    }
}

/// A whole bloom filter in a single sixty-four bit word.
///
/// Four bits per key in one machine word, which is small enough to sit
/// beside a pointer and be read in the same cache line. That is what it
/// is for: reject a lookup before following the pointer at all. False
/// means the key was definitely never added; true means it probably was.
///
/// About eight keys before the rate of wrong yeses climbs past a few
/// percent. `TinyBloom.suggested_capacity` says so, and `false_positive_rate`
/// works out the rate for any number of keys.
#[pyclass(module = "subetha", from_py_object)]
#[derive(Clone)]
struct TinyBloom {
    inner: Bloom64,
}

#[pymethods]
impl TinyBloom {
    /// An empty filter, or one already holding `keys`.
    #[new]
    #[pyo3(signature = (keys = None))]
    fn new(keys: Option<Vec<Vec<u8>>>) -> Self {
        let mut inner = Bloom64::ZERO;
        if let Some(keys) = keys {
            for key in &keys {
                inner.insert(key.as_slice());
            }
        }
        Self { inner }
    }

    /// Add a key, setting its bits. Nothing can be taken out again, and
    /// the filter is only sixty-four bits wide, so the rate of wrong
    /// yeses climbs quickly past `suggested_capacity` keys.
    fn insert(&mut self, key: &[u8]) {
        self.inner.insert(key);
    }

    /// Add a run of keys in one crossing.
    fn insert_many(&mut self, keys: Vec<Vec<u8>>) -> usize {
        for key in &keys {
            self.inner.insert(key.as_slice());
        }
        keys.len()
    }

    /// `False` means the key is definitely absent, `True` means it is
    /// probably present. A filter this small says yes wrongly often once
    /// it holds more than a handful of keys.
    fn __contains__(&self, key: &[u8]) -> bool {
        self.inner.might_contain(key)
    }

    /// As `in`, spelled out.
    fn contains(&self, key: &[u8]) -> bool {
        self.inner.might_contain(key)
    }

    /// Ask about a run of keys in one crossing.
    fn contains_many(&self, keys: Vec<Vec<u8>>) -> Vec<bool> {
        keys.iter()
            .map(|key| self.inner.might_contain(key.as_slice()))
            .collect()
    }

    /// The whole filter as one number, which is how it travels beside a
    /// value or through anything that carries an integer.
    #[getter]
    fn bits(&self) -> u64 {
        self.inner.0
    }

    /// Rebuild a filter from the number `bits` gave.
    #[staticmethod]
    fn from_bits(bits: u64) -> Self {
        Self { inner: Bloom64(bits) }
    }

    /// How many bits are set, which is how full it is.
    #[getter]
    fn set_bits(&self) -> u32 {
        self.inner.popcount()
    }

    /// The share of wrong yeses to expect once `keys` keys are in,
    /// between zero and one.
    #[staticmethod]
    fn false_positive_rate(keys: usize) -> f64 {
        Bloom64::estimated_fpr(keys)
    }

    /// How many keys this size holds before the rate of wrong yeses
    /// climbs past a few percent.
    #[classattr]
    fn suggested_capacity() -> usize {
        Bloom64::SUGGESTED_CAPACITY
    }

    fn __repr__(&self) -> String {
        format!("<subetha.TinyBloom {} of 64 bits set>", self.inner.popcount())
    }
}

/// The same idea in four words rather than one, for about sixty-four
/// keys instead of eight.
#[pyclass(module = "subetha", from_py_object)]
#[derive(Clone)]
struct FineBloom {
    inner: BloomFine,
}

#[pymethods]
impl FineBloom {
    /// An empty filter, or one already holding `keys`.
    ///
    /// Unlike `BloomFilter`, this one lives in the process rather than in
    /// a mapped file: it has no path, and another process cannot see it.
    /// It is a few words of bits meant to travel beside a value, which is
    /// what makes it worth having next to the file-backed filter.
    #[new]
    #[pyo3(signature = (keys = None))]
    fn new(keys: Option<Vec<Vec<u8>>>) -> Self {
        let mut inner = BloomFine::ZERO;
        if let Some(keys) = keys {
            for key in &keys {
                inner.insert(key.as_slice());
            }
        }
        Self { inner }
    }

    /// Add a key, setting its bits. Nothing can be taken out again, and
    /// the rate of wrong yeses climbs past `suggested_capacity` keys.
    fn insert(&mut self, key: &[u8]) {
        self.inner.insert(key);
    }

    /// Add a run of keys in one crossing, and answer how many were handed
    /// in. Nothing here can refuse, so the count is always the length of
    /// the sequence.
    fn insert_many(&mut self, keys: Vec<Vec<u8>>) -> usize {
        for key in &keys {
            self.inner.insert(key.as_slice());
        }
        keys.len()
    }

    /// `False` means the key is definitely absent, `True` means it is
    /// probably present.
    fn __contains__(&self, key: &[u8]) -> bool {
        self.inner.might_contain(key)
    }

    /// As `in`, spelled out.
    fn contains(&self, key: &[u8]) -> bool {
        self.inner.might_contain(key)
    }

    /// Ask about a run of keys in one crossing, answering one `True` or
    /// `False` per key in the order they were given.
    fn contains_many(&self, keys: Vec<Vec<u8>>) -> Vec<bool> {
        keys.iter()
            .map(|key| self.inner.might_contain(key.as_slice()))
            .collect()
    }

    /// How many keys this size holds before the rate of wrong yeses
    /// climbs past a few percent.
    #[classattr]
    fn suggested_capacity() -> usize {
        BloomFine::SUGGESTED_CAPACITY
    }

    fn __repr__(&self) -> String {
        "<subetha.FineBloom>".to_string()
    }
}

/// A clock that keeps wall-clock time and still orders two events that
/// share a reading.
///
/// Two parts: the physical time, and a count that steps when two events
/// land on the same physical reading. Comparing two of these orders
/// them even when the machines' clocks disagree slightly, which a bare
/// timestamp cannot do.
///
/// `merge` is what a receiver does with a sender's clock: it takes the
/// later of the two and steps past it, so the received event orders
/// after the one that caused it.
#[pyclass(module = "subetha", frozen, from_py_object)]
#[derive(Clone)]
struct Clock {
    inner: HybridLogicalClock,
}

#[pymethods]
impl Clock {
    /// A reading built from the two parts given, both defaulting to zero.
    ///
    /// This does not read the machine's clock: it is for rebuilding a
    /// reading that arrived from somewhere else, so that `merge` can fold
    /// it in. `now` is the one that takes the time.
    #[new]
    #[pyo3(signature = (physical = 0, logical = 0))]
    fn new(physical: u64, logical: u64) -> Self {
        Self { inner: HybridLogicalClock::new(physical, logical) }
    }

    /// A reading taken now, in microseconds since the epoch, with the
    /// count at zero.
    #[staticmethod]
    fn now() -> Self {
        Self { inner: HybridLogicalClock::now() }
    }

    #[getter]
    fn physical(&self) -> u64 {
        self.inner.physical
    }

    #[getter]
    fn logical(&self) -> u64 {
        self.inner.logical
    }

    /// The next reading given a new physical time. The count steps
    /// rather than resetting when the physical time has not moved.
    fn advance(&self, physical: u64) -> Self {
        Self { inner: self.inner.advance(physical) }
    }

    /// The reading a receiver should take, given what arrived and what
    /// its own clock says. Orders the received event after whatever
    /// caused it.
    fn merge(&self, received: &Clock, physical: u64) -> Self {
        Self { inner: self.inner.merge(&received.inner, physical) }
    }

    fn __lt__(&self, other: &Clock) -> bool {
        (self.inner.physical, self.inner.logical)
            < (other.inner.physical, other.inner.logical)
    }

    fn __le__(&self, other: &Clock) -> bool {
        (self.inner.physical, self.inner.logical)
            <= (other.inner.physical, other.inner.logical)
    }

    fn __gt__(&self, other: &Clock) -> bool {
        (self.inner.physical, self.inner.logical)
            > (other.inner.physical, other.inner.logical)
    }

    fn __ge__(&self, other: &Clock) -> bool {
        (self.inner.physical, self.inner.logical)
            >= (other.inner.physical, other.inner.logical)
    }

    fn __eq__(&self, other: &Clock) -> bool {
        (self.inner.physical, self.inner.logical)
            == (other.inner.physical, other.inner.logical)
    }

    fn __hash__(&self) -> u64 {
        self.inner.physical ^ self.inner.logical.rotate_left(32)
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Clock {}.{}>",
            self.inner.physical, self.inner.logical
        )
    }
}

/// How many participants a `CausalClock` counts for. Fixed, because the
/// clock is an array rather than a map, which is what makes comparing
/// two of them a handful of instructions.
const CAUSAL_CLOCK_NODES: usize = 16;

/// One count per participant, which answers whether one event caused
/// another or whether the two happened independently.
///
/// A bare timestamp cannot tell "before" from "at the same time on
/// another machine". This can: comparing two of these answers before,
/// after, equal, or neither, and neither means the two events are
/// genuinely concurrent.
#[pyclass(module = "subetha", frozen, from_py_object)]
#[derive(Clone)]
struct CausalClock {
    inner: VectorClock<CAUSAL_CLOCK_NODES>,
}

#[pymethods]
impl CausalClock {
    /// All counts at zero.
    #[new]
    fn new() -> Self {
        Self { inner: VectorClock::zero() }
    }

    /// Step this participant's own count, which is what it does when
    /// something happens to it.
    fn tick(&self, node: usize) -> PyResult<Self> {
        check_causal_node(node)?;
        let mut stepped = self.inner;
        stepped.increment(node);
        Ok(Self { inner: stepped })
    }

    /// One participant's count.
    fn count(&self, node: usize) -> PyResult<u64> {
        check_causal_node(node)?;
        Ok(self.inner.clock[node])
    }

    /// Every count, in participant order.
    #[getter]
    fn counts(&self) -> Vec<u64> {
        self.inner.clock.to_vec()
    }

    /// The clock a receiver should hold after taking `other` in: the
    /// higher of each count.
    fn merge(&self, other: &CausalClock) -> Self {
        Self { inner: self.inner.merge(&other.inner) }
    }

    /// How this stands to another: `before`, `after`, `equal`, or
    /// `concurrent` when neither caused the other.
    fn compare(&self, other: &CausalClock) -> &'static str {
        match self.inner.causal_cmp(&other.inner) {
            Some(std::cmp::Ordering::Less) => "before",
            Some(std::cmp::Ordering::Greater) => "after",
            Some(std::cmp::Ordering::Equal) => "equal",
            None => "concurrent",
        }
    }

    /// Whether this happened before the other, which is the same as
    /// `compare` answering `before`.
    fn happened_before(&self, other: &CausalClock) -> bool {
        matches!(self.inner.causal_cmp(&other.inner), Some(std::cmp::Ordering::Less))
    }

    /// Whether neither caused the other.
    fn concurrent_with(&self, other: &CausalClock) -> bool {
        self.inner.causal_cmp(&other.inner).is_none()
    }

    /// How many participants a clock counts for.
    #[classattr]
    fn nodes() -> usize {
        CAUSAL_CLOCK_NODES
    }

    fn __repr__(&self) -> String {
        format!("<subetha.CausalClock {:?}>", self.inner.clock)
    }
}

fn check_causal_node(node: usize) -> PyResult<()> {
    if node >= CAUSAL_CLOCK_NODES {
        return Err(PyValueError::new_err(format!(
            "a causal clock counts for {CAUSAL_CLOCK_NODES} participants, numbered from zero"
        )));
    }
    Ok(())
}

// The sensors. Each is fed measurements and answers what it has worked
// out from them. They hold no shared memory and touch no network: they
// are the arithmetic a transport does on its own numbers, which is why
// they are worth having from Python whether or not a SubEtha link is
// what produced the numbers.
//
// Every one of them answers None rather than a number until it has seen
// enough to say anything, which is a different answer from zero.

/// Tells a loss that happened because the air was noisy from one that
/// happened because a queue overflowed.
///
/// The two want opposite responses. A wireless drop should be recovered
/// locally and must not be read as congestion, or the sender slows down
/// for no reason. A congestion drop should raise redundancy and slow the
/// sender. Telling them apart is done from the spacing between arrivals:
/// a gap at or above a quarter more than the smallest spacing seen is
/// queuing, and anything narrower is noise.
#[pyclass(module = "subetha")]
struct LossKind {
    inner: Box<LossClassSensor>,
}

#[pymethods]
impl LossKind {
    /// A sensor with nothing observed yet. It lives in this process and
    /// has no file behind it, so two processes each keep their own.
    /// Feed it with `observe_spacing` and `observe_delay` before asking
    /// it anything.
    #[new]
    fn new() -> Self {
        Self { inner: Box::new(LossClassSensor::new()) }
    }

    /// Feed the spacing between two arrivals, in microseconds.
    fn observe_spacing(&mut self, microseconds: f64) -> PyResult<()> {
        check_measurement(microseconds, "a spacing")?;
        self.inner.observe_interarrival(microseconds);
        Ok(())
    }

    /// Feed a one-way delay, in microseconds.
    fn observe_delay(&mut self, microseconds: f64) -> PyResult<()> {
        check_measurement(microseconds, "a delay")?;
        self.inner.observe_owd(microseconds);
        Ok(())
    }

    /// What a loss of `gap` items with this spacing was: `wireless` or
    /// `congestion`.
    fn classify(&mut self, gap: u32, spacing_microseconds: f64) -> PyResult<&'static str> {
        check_measurement(spacing_microseconds, "a spacing")?;
        Ok(match self.inner.classify(gap, spacing_microseconds) {
            LossClass::Wireless => "wireless",
            LossClass::Congestion => "congestion",
        })
    }

    /// The share of recent losses that were congestion, between zero
    /// and one.
    #[getter]
    fn congestion_share(&self) -> f32 {
        self.inner.congestion_fraction()
    }

    /// The spread of recent delays, in microseconds.
    #[getter]
    fn delay_spread(&self) -> f64 {
        self.inner.recent_owd_spread_us()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.LossKind congestion share {:.2}>",
            self.inner.congestion_fraction()
        )
    }
}

/// Whether losses arrive alone or in runs, and how long a run lasts.
///
/// A link that loses one item at a time and a link that loses twenty
/// together can lose the same share overall and need entirely different
/// redundancy. This fits a two-state model to what it is fed and reports
/// the average length of a run.
#[pyclass(module = "subetha")]
struct LossBursts {
    inner: Box<BurstModel>,
}

#[pymethods]
impl LossBursts {
    /// A model with nothing observed yet. It lives in this process and
    /// has no file behind it. Feed it with `observe` or `observe_many`,
    /// one call per item, saying whether that item was lost.
    #[new]
    fn new() -> Self {
        Self { inner: Box::new(BurstModel::new()) }
    }

    /// Feed one item: True when it was lost.
    fn observe(&mut self, lost: bool) {
        self.inner.observe(lost);
    }

    /// Feed a run of items in one crossing.
    fn observe_many(&mut self, losses: Vec<bool>) -> usize {
        for lost in &losses {
            self.inner.observe(*lost);
        }
        losses.len()
    }

    /// How many items a run of losses lasts on average, or None before
    /// there is enough to say.
    #[getter]
    fn mean_run_length(&self) -> Option<f64> {
        self.inner.mean_burst_len()
    }

    /// The share lost once the runs are accounted for, or None before
    /// there is enough to say.
    #[getter]
    fn steady_loss(&self) -> Option<f64> {
        self.inner.steady_loss()
    }

    /// The two rates the model fits, entering a run and leaving it, or
    /// None before there is enough to say.
    #[getter]
    fn transition_rates(&self) -> Option<(f64, f64)> {
        self.inner.fit()
    }

    /// How many items have been fed.
    #[getter]
    fn samples(&self) -> u64 {
        self.inner.samples()
    }

    fn __repr__(&self) -> String {
        match self.inner.mean_burst_len() {
            Some(run) => format!("<subetha.LossBursts runs of {run:.1}>"),
            None => "<subetha.LossBursts not enough yet>".to_string(),
        }
    }
}

/// Jitter, spacing and whether delay is trending up.
///
/// Fed a send time and a receive time for each item, it reports the
/// variation between arrivals, the average spacing, and whether the
/// one-way delay is climbing, which is a queue filling before it
/// overflows.
///
/// The two clocks need not agree. A trend is a change over time, and a
/// constant difference between two clocks cancels out of it, which is
/// what `trend_debiased` makes explicit.
#[pyclass(module = "subetha")]
struct Timing {
    inner: Box<TemporalSensor>,
}

#[pymethods]
impl Timing {
    /// `window` is how many recent items the answers are taken over.
    #[new]
    #[pyo3(signature = (window = 64))]
    fn new(window: usize) -> PyResult<Self> {
        if window < 2 {
            return Err(PyValueError::new_err(
                "a window covers at least two items",
            ));
        }
        Ok(Self { inner: Box::new(TemporalSensor::new(window)) })
    }

    /// Feed one item's send and receive times, in microseconds. The two
    /// clocks need not agree with each other.
    fn observe(&mut self, sent: u64, received: u64) {
        self.inner.observe(sent, received);
    }

    /// The variation between arrivals, in microseconds.
    #[getter]
    fn jitter(&self) -> f64 {
        self.inner.jitter_micros()
    }

    /// The average spacing between arrivals, in microseconds.
    #[getter]
    fn spacing(&self) -> f64 {
        self.inner.interarrival_micros()
    }

    /// Whether one-way delay is climbing. Above zero is a queue
    /// filling.
    #[getter]
    fn trend(&self) -> f64 {
        self.inner.owd_trend()
    }

    /// The same trend with a steady difference between the two clocks
    /// taken out, which is the one to read when the clocks are not
    /// synchronized.
    #[getter]
    fn trend_debiased(&self) -> f64 {
        self.inner.owd_trend_debiased()
    }

    /// How far the two clocks differ, in microseconds.
    #[getter]
    fn clock_skew(&self) -> f64 {
        self.inner.skew()
    }

    #[getter]
    fn samples(&self) -> usize {
        self.inner.samples()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Timing jitter {:.1}us over {} samples>",
            self.inner.jitter_micros(),
            self.inner.samples()
        )
    }
}

/// Whether round trips fall into two groups, which is what a wireless
/// link looks like.
///
/// A wired link's round trips cluster around one value. A wireless one
/// often has two: the quick ones and the ones that waited for a retry
/// in the radio. Two groups is evidence of the second.
#[pyclass(module = "subetha")]
struct RoundTripShape {
    inner: Box<RttShape>,
}

#[pymethods]
impl RoundTripShape {
    /// A shape with nothing observed yet. It lives in this process and
    /// has no file behind it. Feed it with `observe` or `observe_many`,
    /// in microseconds, before asking it anything.
    #[new]
    fn new() -> Self {
        Self { inner: Box::new(RttShape::new()) }
    }

    /// Feed one round trip, in microseconds.
    fn observe(&mut self, microseconds: f64) -> PyResult<()> {
        check_measurement(microseconds, "a round trip")?;
        self.inner.observe(microseconds);
        Ok(())
    }

    /// Feed a run of round trips in one crossing.
    fn observe_many(&mut self, microseconds: Vec<f64>) -> PyResult<usize> {
        for one in &microseconds {
            check_measurement(*one, "a round trip")?;
        }
        for one in &microseconds {
            self.inner.observe(*one);
        }
        Ok(microseconds.len())
    }

    /// How strongly the round trips fall into two groups, or None
    /// before there is enough to say.
    #[getter]
    fn two_groups(&self) -> Option<f64> {
        self.inner.bimodality()
    }

    /// How much this looks like a wireless link, between zero and one.
    #[getter]
    fn wireless_confidence(&self) -> f32 {
        self.inner.wifi_confidence()
    }

    #[getter]
    fn samples(&self) -> u64 {
        self.inner.samples()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.RoundTripShape wireless confidence {:.2}>",
            self.inner.wifi_confidence()
        )
    }
}

/// Whether delay spikes on a regular beat, and when the next one is
/// due.
///
/// Some interference is periodic: a radio that scans on a schedule, a
/// neighbour's traffic that arrives in a rhythm. Finding the beat means
/// a sender can raise redundancy just before the next spike rather than
/// reacting after it.
#[pyclass(module = "subetha")]
struct Periodicity {
    inner: Box<PeriodicitySensor>,
}

#[pymethods]
impl Periodicity {
    /// A sensor with nothing observed yet. It lives in this process and
    /// has no file behind it. Each `observe` needs both the delay and
    /// when it was taken, because a beat can only be found against time.
    #[new]
    fn new() -> Self {
        Self { inner: Box::new(PeriodicitySensor::new()) }
    }

    /// Feed one delay, in microseconds, and when it was taken, also in
    /// microseconds.
    fn observe(&mut self, delay_microseconds: f64, at_microseconds: u64) -> PyResult<()> {
        check_measurement(delay_microseconds, "a delay")?;
        self.inner.observe(delay_microseconds, at_microseconds);
        Ok(())
    }

    /// The beat found, as its length in seconds and how strong it is,
    /// or None when there is no beat to find.
    #[getter]
    fn period(&self) -> Option<(f64, f64)> {
        self.inner.detected_period()
    }

    /// Seconds until the next spike is due, or None when there is no
    /// beat to go on.
    #[getter]
    fn seconds_to_next(&self) -> Option<f64> {
        self.inner.secs_to_next_spike()
    }

    fn __repr__(&self) -> String {
        match self.inner.detected_period() {
            Some((length, _)) => format!("<subetha.Periodicity every {length:.3}s>"),
            None => "<subetha.Periodicity no beat found>".to_string(),
        }
    }
}

/// How much of the path's capacity is free, worked out from probes sent
/// in pairs and in trains.
///
/// Two probes sent back to back arrive spread apart by the narrowest
/// link on the path, which gives its capacity. A longer train arrives
/// spread by what is left after the traffic already there, which gives
/// what is available.
#[pyclass(module = "subetha")]
struct Capacity {
    inner: Box<WBestEstimator>,
}

#[pymethods]
impl Capacity {
    /// `probe_bytes` is how big each probe is.
    #[new]
    #[pyo3(signature = (probe_bytes = 1400))]
    fn new(probe_bytes: usize) -> PyResult<Self> {
        if probe_bytes < 1 {
            return Err(PyValueError::new_err("a probe is at least one byte"));
        }
        Ok(Self { inner: Box::new(WBestEstimator::new(probe_bytes)) })
    }

    /// Feed the arrival of one probe of a pair, numbered within the
    /// pair, in microseconds.
    fn observe_pair(&mut self, index: u8, arrived_microseconds: f64) -> PyResult<()> {
        check_measurement(arrived_microseconds, "an arrival")?;
        self.inner.on_pair_probe(index, arrived_microseconds);
        Ok(())
    }

    /// Feed the arrival of one probe of a train, in microseconds.
    fn observe_train(&mut self, arrived_microseconds: f64) -> PyResult<()> {
        check_measurement(arrived_microseconds, "an arrival")?;
        self.inner.on_train_probe(arrived_microseconds);
        Ok(())
    }

    /// The narrowest link's capacity, in bits a second, or None before
    /// there is enough to say.
    #[getter]
    fn link_capacity(&self) -> Option<f64> {
        self.inner.effective_capacity_bps()
    }

    /// What is left after the traffic already on the path, in bits a
    /// second, or None before there is enough to say.
    #[getter]
    fn available(&self) -> Option<f64> {
        self.inner.available_bps()
    }

    /// The rate the train arrived at, in bits a second.
    #[getter]
    fn train_rate(&self) -> Option<f64> {
        self.inner.train_rate_bps()
    }

    /// How many pairs and how many train probes have been fed.
    #[getter]
    fn samples(&self) -> (usize, u32) {
        self.inner.samples()
    }

    /// Forget everything and start again, which is what a caller does
    /// when the path may have changed.
    fn reset(&mut self) {
        self.inner.reset();
    }

    fn __repr__(&self) -> String {
        match self.inner.available_bps() {
            Some(free) => format!("<subetha.Capacity {:.0} bits a second free>", free),
            None => "<subetha.Capacity not enough yet>".to_string(),
        }
    }
}

/// What the next interval's traffic is likely to be, from what the last
/// ones were.
#[pyclass(module = "subetha")]
struct Forecast {
    inner: Box<ArrivalForecast>,
}

#[pymethods]
impl Forecast {
    /// A forecast with nothing observed yet. It lives in this process and
    /// has no file behind it. Feed it whole intervals with `observe`,
    /// each one a byte count and how long it covered.
    #[new]
    fn new() -> Self {
        Self { inner: Box::new(ArrivalForecast::new()) }
    }

    /// Feed one interval: how many bytes arrived and how long it was,
    /// in seconds.
    fn observe(&mut self, bytes: u64, seconds: f64) -> PyResult<()> {
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err(PyValueError::new_err(
                "an interval is a number of seconds above zero",
            ));
        }
        self.inner.observe(bytes, seconds);
        Ok(())
    }

    /// The average rate so far, in bits a second.
    #[getter]
    fn mean_rate(&self) -> f64 {
        self.inner.mean_bps()
    }

    /// What the next interval is likely to carry, in bits a second.
    #[getter]
    fn next_rate(&self) -> f64 {
        self.inner.forecast_bps()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Forecast next {:.0} bits a second>",
            self.inner.forecast_bps()
        )
    }
}

/// Whether the route changed under the traffic.
///
/// Every item carries how many hops it took and whether anything on the
/// way marked it as congested. A change in the hop count means the route
/// moved, which explains a sudden change in delay that would otherwise
/// look like congestion.
#[pyclass(module = "subetha")]
struct PathChanges {
    inner: Box<PathSensor>,
}

#[pymethods]
impl PathChanges {
    /// A sensor with nothing observed yet. It lives in this process and
    /// has no file behind it. Feed it with `observe`, one call per item
    /// received, and it infers from the spread of what it is told.
    #[new]
    fn new() -> Self {
        Self { inner: Box::new(PathSensor::new()) }
    }

    /// Feed one item: its remaining time to live, its congestion
    /// marking, and how many hops it took.
    fn observe(&mut self, ttl: u8, congestion_mark: u8, hops: u8) {
        self.inner.observe(ttl, congestion_mark, hops);
    }

    /// How much the route has been moving, between zero and one.
    #[getter]
    fn route_movement(&self) -> f32 {
        self.inner.path_shift()
    }

    /// The share of items something on the way marked as congested,
    /// between zero and one.
    #[getter]
    fn marked_share(&self) -> f32 {
        self.inner.ecn_ce()
    }

    /// The last item's time to live, marking and hop count, or None
    /// when nothing has been fed.
    #[getter]
    fn last(&self) -> Option<(u8, u8, u8)> {
        self.inner.last()
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.PathChanges movement {:.2}>",
            self.inner.path_shift()
        )
    }
}

/// A measurement that a sensor can use: a real number, not negative.
fn check_measurement(value: f64, what: &str) -> PyResult<()> {
    if !value.is_finite() || value < 0.0 {
        return Err(PyValueError::new_err(format!(
            "{what} is a number of microseconds that is not negative"
        )));
    }
    Ok(())
}

/// A counting semaphore in a mapped file, limiting how many processes
/// work at once.
/// Frozen for the same reason `RWLock` is: a permit reaches it through
/// `get`, including from `Drop`.
#[pyclass(module = "subetha", frozen)]
/// Held through the parking wrapper, which is the same semaphore file
/// with a small one beside it carrying the wakeups. `inner` reaches the
/// semaphore itself, so the waiting form that spins and the one that
/// sleeps are two ways at one semaphore rather than two.
struct Semaphore {
    parked: Box<BlockingSemaphore>,
}

impl Semaphore {
    /// The semaphore underneath the parking wrapper.
    fn inner(&self) -> &SharedSemaphore {
        self.parked.inner()
    }
}

#[pymethods]
impl Semaphore {
    /// The constructor asserts that the initial count fits the maximum,
    /// so that is refused here first rather than reaching Python as a
    /// panic.
    #[new]
    #[pyo3(signature = (path, initial, max_permits = None))]
    fn new(path: &str, initial: u32, max_permits: Option<u32>) -> PyResult<Self> {
        let max = max_permits.unwrap_or(initial);
        if initial > max {
            return Err(PyValueError::new_err(
                "the initial count cannot exceed max_permits",
            ));
        }
        BlockingSemaphore::create(path, max, initial)
            .map(|parked| Self { parked: Box::new(parked) })
            .map_err(|e| PyOSError::new_err(format!("opening the semaphore: {e:?}")))
    }

    /// Attach to a semaphore that already exists, raising `OSError` when
    /// it does not. `max_permits` must be the one it was created with,
    /// and attaching takes no permit: `acquire` is what does.
    #[staticmethod]
    fn open(path: &str, max_permits: u32) -> PyResult<Self> {
        BlockingSemaphore::open(path, max_permits)
            .map(|parked| Self { parked: Box::new(parked) })
            .map_err(|e| PyOSError::new_err(format!("attaching to the semaphore: {e:?}")))
    }

    #[getter]
    fn available(&self) -> u32 {
        self.inner().available()
    }

    #[getter]
    fn waiters(&self) -> u32 {
        self.inner().waiters()
    }

    #[getter]
    fn max_permits(&self) -> u32 {
        self.inner().max_permits()
    }

    /// Take a permit, waiting for one. The interpreter is detached while
    /// this waits.
    fn acquire(slf: Bound<'_, Self>, py: Python<'_>) -> PermitHold {
        let owner = slf.clone().unbind();
        let semaphore: &SharedSemaphore = slf.get().inner();
        py.detach(|| {
            let permit = semaphore.acquire();
            std::mem::forget(permit);
        });
        PermitHold { owner, released: false }
    }

    /// Take a permit, giving up after `timeout` seconds and answering
    /// None.
    ///
    /// This one sleeps rather than spinning, so a long wait costs no
    /// processor, and it never waits past the deadline even if whoever
    /// holds the permits never gives one back.
    fn acquire_for(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        timeout: f64,
    ) -> PyResult<Option<PermitHold>> {
        let owner = slf.clone().unbind();
        let wait = duration_from(timeout)?;
        let parked: &BlockingSemaphore = &slf.get().parked;
        match py.detach(|| parked.acquire_park_timeout(wait)) {
            Ok(permit) => {
                std::mem::forget(permit);
                Ok(Some(PermitHold { owner, released: false }))
            }
            Err(BlockingSemaphoreError::Timeout) => Ok(None),
            Err(e) => Err(PyOSError::new_err(format!("taking a permit: {e:?}"))),
        }
    }

    /// Take a permit if one is free, or answer `None`. Only an exhausted
    /// semaphore answers `None`; anything else raises.
    fn try_acquire(slf: Bound<'_, Self>) -> PyResult<Option<PermitHold>> {
        let owner = slf.clone().unbind();
        match slf.get().inner().try_acquire() {
            Ok(permit) => {
                std::mem::forget(permit);
                Ok(Some(PermitHold { owner, released: false }))
            }
            Err(SemaphoreError::WouldBlock) => Ok(None),
            Err(e) => Err(os_err("taking a permit", e)),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Semaphore available={} of {}>",
            self.inner().available(),
            self.inner().max_permits()
        )
    }
}

/// A permit, given back when its `with` block ends.
#[pyclass(module = "subetha", unsendable)]
struct PermitHold {
    /// The semaphore this came from, kept alive while the permit is.
    owner: Py<Semaphore>,
    released: bool,
}

#[pymethods]
impl PermitHold {
    /// Answer the same object, which is why a permit is normally taken as
    /// `with semaphore.acquire() as permit:` rather than bound to a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Give the permit back. A refusal here is reported rather than
    /// dropped: releasing more permits than the semaphore allows is a
    /// real fault in the caller's bookkeeping.
    #[pyo3(signature = (*_args))]
    fn __exit__(&mut self, _args: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<bool> {
        self.give_back()
            .map_err(|e| PyOSError::new_err(format!("releasing the permit: {e:?}")))?;
        Ok(false)
    }

    /// Give the permit back now rather than at the end of a block.
    /// Calling it twice is harmless, and `held` says whether it is still
    /// out. A genuine failure is raised rather than dropped: releasing
    /// more permits than the semaphore allows is a fault in the caller's
    /// bookkeeping, not a condition to ignore.
    fn release(&mut self) -> PyResult<()> {
        self.give_back()
            .map_err(|e| PyOSError::new_err(format!("releasing the permit: {e:?}")))
    }

    #[getter]
    fn held(&self) -> bool {
        !self.released
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Permit {}>",
            if self.released { "released" } else { "held" }
        )
    }
}

impl PermitHold {
    fn give_back(&mut self) -> Result<(), BlockingSemaphoreError> {
        if self.released {
            return Ok(());
        }
        // Released through the parking wrapper rather than the
        // semaphore itself, because that is what wakes a thread waiting
        // in `acquire_for`. Releasing underneath it would leave that
        // thread asleep to its deadline with a permit free.
        let outcome = Python::attach(|py| self.owner.bind(py).get().parked.release());
        self.released = true;
        outcome
    }
}

impl Drop for PermitHold {
    /// A permit that reaches here unreleased is given back. Drop cannot
    /// raise, so a refusal is reported on stderr rather than discarded:
    /// it means the count is wrong, which the next acquirer will feel.
    fn drop(&mut self) {
        match self.give_back() {
            Ok(()) => {}
            Err(e) => eprintln!("subetha: releasing a permit on drop failed: {e:?}"),
        }
    }
}

/// An append-only arena of interned strings in a mapped file.
///
/// Interning returns a 64-bit reference that every process resolves the
/// same way, so a string crosses between processes as eight bytes rather
/// than as its own bytes each time. Nothing is ever moved or freed
/// individually; `clear` reclaims the whole arena.
#[pyclass(module = "subetha")]
struct Arena {
    inner: Box<SharedStringArena>,
}

#[pymethods]
impl Arena {
    /// Obtain the arena at `path` holding `capacity_bytes` of interned
    /// text, creating it when the file does not exist.
    ///
    /// The capacity is bytes of storage, not a count of strings, and the
    /// arena only ever grows: nothing is freed, so `intern` answers
    /// `None` once the space is gone.
    #[new]
    fn new(path: &str, capacity_bytes: usize) -> PyResult<Self> {
        SharedStringArena::create(path, capacity_bytes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the arena", e))
    }

    /// Attach to an arena that already exists, able to intern into it.
    /// Raises `OSError` when it is not there. `capacity_bytes` must be
    /// the one it was created with.
    #[staticmethod]
    fn open(path: &str, capacity_bytes: usize) -> PyResult<Self> {
        SharedStringArena::open(path, capacity_bytes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the arena", e))
    }

    /// Attach for reading only, so `intern` cannot be called and
    /// `writable` answers `False`.
    ///
    /// This is the shape for a process that resolves references somebody
    /// else interned: it takes a read-only mapping, so a bug in it cannot
    /// write over the arena everyone shares.
    #[staticmethod]
    fn open_read_only(path: &str, capacity_bytes: usize) -> PyResult<Self> {
        SharedStringArena::open_read_only(path, capacity_bytes)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the arena", e))
    }

    #[getter]
    fn capacity_bytes(&self) -> usize {
        self.inner.capacity_bytes()
    }

    #[getter]
    fn used_bytes(&self) -> usize {
        self.inner.used_bytes()
    }

    #[getter]
    fn remaining_bytes(&self) -> usize {
        self.inner.remaining_bytes()
    }

    #[getter]
    fn writable(&self) -> bool {
        self.inner.is_writable()
    }

    /// Intern a string and return the reference that names it, or `None`
    /// when the arena is full.
    fn intern(&self, value: &str) -> PyResult<Option<u64>> {
        match self.inner.intern(value) {
            Ok(r) => Ok(Some(r.to_u64())),
            Err(ArenaError::Full) => Ok(None),
            Err(e) => Err(os_err("interning", e)),
        }
    }

    /// Intern bytes that need not be text.
    fn intern_bytes(&self, value: &[u8]) -> PyResult<Option<u64>> {
        match self.inner.intern_bytes(value) {
            Ok(r) => Ok(Some(r.to_u64())),
            Err(ArenaError::Full) => Ok(None),
            Err(e) => Err(os_err("interning", e)),
        }
    }

    /// Intern a run of strings in one crossing, stopping at the first
    /// the arena cannot take. Returns the references it managed.
    fn intern_many(&self, values: Vec<String>) -> PyResult<Vec<u64>> {
        let mut refs = Vec::with_capacity(values.len());
        for value in &values {
            match self.inner.intern(value) {
                Ok(r) => refs.push(r.to_u64()),
                Err(ArenaError::Full) => break,
                Err(e) => return Err(os_err("interning", e)),
            }
        }
        Ok(refs)
    }

    /// Resolve a reference to its string.
    fn get(&self, reference: u64) -> PyResult<String> {
        self.inner
            .get(StringRef::from_u64(reference))
            .map(|s| s.to_owned())
            .map_err(|e| os_err("resolving", e))
    }

    /// Resolve a reference to its bytes, for anything that is not text.
    fn get_bytes(&self, reference: u64) -> PyResult<Vec<u8>> {
        self.inner
            .get_bytes(StringRef::from_u64(reference))
            .map(|b| b.to_vec())
            .map_err(|e| os_err("resolving", e))
    }

    /// Resolve a run of references in one crossing.
    fn get_many(&self, references: Vec<u64>) -> PyResult<Vec<String>> {
        let mut out = Vec::with_capacity(references.len());
        for reference in references {
            out.push(
                self.inner
                    .get(StringRef::from_u64(reference))
                    .map(|s| s.to_owned())
                    .map_err(|e| os_err("resolving", e))?,
            );
        }
        Ok(out)
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. References handed out inside the block stay valid after
    /// it: they name bytes in the file, not anything this object owns.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Arena used={} of {} bytes>",
            self.inner.used_bytes(),
            self.inner.capacity_bytes()
        )
    }
}

/// A doubly linked list of equal-sized elements in a mapped file, where
/// a node is named by an index that stays valid until it is removed.
#[pyclass(module = "subetha")]
struct LinkedList {
    inner: Box<RawLinkedList>,
}

#[pymethods]
impl LinkedList {
    /// Obtain the list at `path` with room for `capacity` nodes of
    /// `element_size` bytes, creating it when the file does not exist.
    ///
    /// The capacity counts nodes and is fixed at creation. A node index
    /// is a position in that fixed array rather than a pointer, which is
    /// what makes an index handed to another process mean the same node
    /// there.
    #[new]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn new(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawLinkedList::create(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the list", e))
    }

    /// Attach to a list that already exists, raising `OSError` when it
    /// does not. Every layout argument describes the file rather than
    /// asking anything of it, so all of them must match.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn open(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawLinkedList::open(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the list", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn element_size(&self) -> usize {
        self.inner.layout().slot_size
    }

    /// How many nodes the list holds, which is not the capacity. An empty
    /// list is falsy, so `if not list:` works.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Add at the front and return the index of the node holding it.
    fn push_front(&self, value: &[u8]) -> PyResult<u32> {
        self.inner
            .push_front(value)
            .map_err(|e| os_err("adding at the front", e))
    }

    /// Add at the back and return the index of the node holding it.
    fn push_back(&self, value: &[u8]) -> PyResult<u32> {
        self.inner
            .push_back(value)
            .map_err(|e| os_err("adding at the back", e))
    }

    /// Add a run of values at the back in one crossing, and answer the
    /// node index of each, in order.
    ///
    /// Unlike the ring batch forms this does not stop early and answer a
    /// count: a full list raises, so a short list of indices never
    /// happens and every value handed in is either placed or the call
    /// fails.
    fn push_back_many(&self, values: Vec<Vec<u8>>) -> PyResult<Vec<u32>> {
        let mut indices = Vec::with_capacity(values.len());
        for value in &values {
            indices.push(
                self.inner
                    .push_back(value)
                    .map_err(|e| os_err("adding at the back", e))?,
            );
        }
        Ok(indices)
    }

    /// Take from the front, or `None` when the list is empty.
    fn pop_front(&self) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.pop_front(&mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("taking from the front", e)),
        }
    }

    /// Take from the back, or `None` when the list is empty.
    fn pop_back(&self) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.pop_back(&mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("taking from the back", e)),
        }
    }

    /// Read the node at `index`.
    fn get(&self, index: u32) -> PyResult<Vec<u8>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner
            .get(index, &mut out)
            .map_err(|e| os_err("reading a node", e))?;
        Ok(out)
    }

    /// Unlink the node at `index` and return what it held.
    fn remove(&self, index: u32) -> PyResult<Vec<u8>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner
            .remove(index, &mut out)
            .map_err(|e| os_err("removing a node", e))?;
        Ok(out)
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The nodes stay linked for whoever attaches next, and a
    /// node index taken inside the block is still valid after it.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.LinkedList len={} capacity={}>",
            self.inner.len(),
            self.inner.capacity()
        )
    }
}

/// A fixed-capacity array of equal-sized slots addressed by index, in a
/// mapped file.
///
/// Like `Vec`, each slot is under a seqlock, so the bulk path is
/// `read_range` rather than a buffer; `slot_version` is the seqlock's
/// own counter, which a reader can use to tell a change from a repeat.
#[pyclass(module = "subetha")]
struct Slab {
    inner: Box<RawSlab>,
}

#[pymethods]
impl Slab {
    /// Obtain the slab at `path` with `capacity` slots of `element_size`
    /// bytes, creating it when the file does not exist.
    ///
    /// Every slot exists from the moment the file does, unlike a `Vec`
    /// where a slot has to be pushed before it is there. So a slab is
    /// addressed rather than appended to, and any index below the
    /// capacity is readable straight away.
    #[new]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn new(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawSlab::create(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the slab", e))
    }

    /// Attach to a slab that already exists, able to write into it.
    /// Raises `OSError` when it is not there, and every layout argument
    /// must be the one it was created with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn open(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawSlab::open(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the slab", e))
    }

    /// Attach for reading only, so `set` and `write_range` raise and
    /// `writable` answers `False`.
    ///
    /// The mapping itself is read-only, so this is a guarantee about the
    /// process rather than a convention it agrees to keep: a bug in a
    /// reader cannot write over a slab the writers share.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, element_size, alignment = 1, tag = 0))]
    fn open_read_only(path: &str, capacity: usize, element_size: usize, alignment: usize, tag: u64) -> PyResult<Self> {
        let layout = ElementLayout { slot_size: element_size, alignment, tag };
        RawSlab::open_read_only(path, capacity, layout)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the slab", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn element_size(&self) -> usize {
        self.inner.layout().slot_size
    }

    #[getter]
    fn writable(&self) -> bool {
        self.inner.is_writable()
    }

    /// How many slots the slab has, which is its capacity and not a count
    /// of the ones written to. Every slot exists from creation, so this
    /// never changes and a slab is never empty.
    fn __len__(&self) -> usize {
        self.inner.capacity()
    }

    /// Read slot `index`, always `element_size` bytes. A slot nobody has
    /// written to reads as zeros rather than raising, because it exists
    /// from creation. An index past the capacity is an `OSError`.
    ///
    /// The read goes through the slot's seqlock, so a writer racing it
    /// loses to a retry rather than handing back a torn element.
    fn get(&self, index: usize) -> PyResult<Vec<u8>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner
            .get(index, &mut out)
            .map_err(|e| os_err("reading a slot", e))?;
        Ok(out)
    }

    /// Overwrite slot `index`, stepping its seqlock either side so a
    /// reader racing this retries instead of seeing half of each value.
    /// An index past the capacity, or a slab opened read-only, is an
    /// `OSError`.
    ///
    /// One writer per slot. The seqlock makes a torn read detectable but
    /// does nothing to make a torn write safe, so two processes writing
    /// the same slot at once corrupt it and nothing reports that. Give
    /// each writer its own slots, or put a lock over the shared ones.
    fn set(&self, index: usize, value: &[u8]) -> PyResult<()> {
        self.inner
            .set(index, value)
            .map_err(|e| os_err("writing a slot", e))
    }

    /// The seqlock counter for a slot. Even means nobody is writing; the
    /// same value twice means no write landed between the two reads.
    fn slot_version(&self, index: usize) -> PyResult<u32> {
        self.inner
            .slot_version(index)
            .map_err(|e| os_err("reading a slot version", e))
    }

    /// Read `count` slots from `start` packed end to end, one crossing
    /// and one object, each slot read through its seqlock.
    fn read_range(&self, start: usize, count: usize) -> PyResult<Vec<u8>> {
        let size = self.inner.layout().slot_size;
        let mut packed = vec![0u8; count * size];
        for i in 0..count {
            let at = i * size;
            self.inner
                .get(start + i, &mut packed[at..at + size])
                .map_err(|e| os_err("reading a range", e))?;
        }
        Ok(packed)
    }

    /// Write slots packed end to end from `start`.
    fn write_range(&self, start: usize, data: &[u8]) -> PyResult<usize> {
        let size = self.inner.layout().slot_size;
        if size == 0 || !data.len().is_multiple_of(size) {
            return Err(PyValueError::new_err(
                "the data must be a whole number of elements",
            ));
        }
        for (i, chunk) in data.chunks(size).enumerate() {
            self.inner
                .set(start + i, chunk)
                .map_err(|e| os_err("writing a range", e))?;
        }
        Ok(data.len() / size)
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// slots without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. Nothing is flushed on the way out: call `flush` where
    /// durability matters.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.Slab capacity={} element_size={}>",
            self.inner.capacity(),
            self.inner.layout().slot_size
        )
    }
}

/// An ordered map in a mapped file, sorted by unsigned byte comparison
/// of the key so every language agrees on the order without a comparator
/// crossing the boundary.
#[pyclass(module = "subetha", name = "BTreeMap")]
struct BTreeMap_ {
    inner: Box<RawBTreeMap>,
}

#[pymethods]
impl BTreeMap_ {
    /// Obtain the map at `path` holding `capacity` entries of `key_size`
    /// and `value_size` bytes, creating it when the file does not exist.
    /// A capacity below one is a `ValueError`.
    ///
    /// Keys are ordered by unsigned byte comparison, so `first` and
    /// `last` mean the same thing in every language reading the file and
    /// no comparator has to cross the boundary. Both widths are fixed:
    /// they are the layout, not a maximum.
    #[new]
    #[pyo3(signature = (path, capacity, key_size, value_size, tag = 0))]
    fn new(path: &str, capacity: usize, key_size: usize, value_size: usize, tag: u64) -> PyResult<Self> {
        if capacity < 1 {
            return Err(PyValueError::new_err("a map needs a capacity of at least one"));
        }
        RawBTreeMap::create(path, capacity, key_size, value_size, tag)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the map", e))
    }

    /// Attach to a map that already exists, raising `OSError` when it
    /// does not. The capacity, both widths and the tag must be the ones
    /// it was created with.
    #[staticmethod]
    #[pyo3(signature = (path, capacity, key_size, value_size, tag = 0))]
    fn open(path: &str, capacity: usize, key_size: usize, value_size: usize, tag: u64) -> PyResult<Self> {
        RawBTreeMap::open(path, capacity, key_size, value_size, tag)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the map", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn key_size(&self) -> usize {
        self.inner.key_size()
    }

    #[getter]
    fn value_size(&self) -> usize {
        self.inner.value_size()
    }

    #[getter]
    fn nodes(&self) -> usize {
        self.inner.node_count()
    }

    /// How many entries the map holds, which is not the capacity and not
    /// `nodes`: a node is a block of the tree and holds several entries.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Insert or replace. Returns what the key held before, or `None`
    /// when it held nothing; `False` is never returned, a full map
    /// raises, because a map that cannot take a key has failed rather
    /// than answered.
    fn insert(&self, key: &[u8], value: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut previous = vec![0u8; self.inner.value_size()];
        match self.inner.insert(key, value, Some(&mut previous)) {
            Ok(true) => Ok(Some(previous)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("inserting", e)),
        }
    }

    /// Insert or replace a run of pairs, stopping at the first the map
    /// has no room for, and answer how many landed.
    ///
    /// This is where the batch form differs from `insert`: a full map
    /// stops the run and shortens the count, where `insert` on its own
    /// raises. The previous value is not collected, so a caller who needs
    /// what a key held has to call `insert` per pair.
    fn insert_many(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<usize> {
        let mut done = 0;
        for (key, value) in &pairs {
            match self.inner.insert(key, value, None) {
                Ok(_) => done += 1,
                Err(BTreeError::Full) => break,
                Err(e) => return Err(os_err("inserting", e)),
            }
        }
        Ok(done)
    }

    /// What `key` holds, or `None` when it holds nothing. The value is
    /// always `value_size` bytes.
    fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.get(key, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("reading", e)),
        }
    }

    /// Whether the key is present, without bringing its value back over
    /// the boundary. Cheaper than `get` when the value is not wanted.
    fn __contains__(&self, key: &[u8]) -> PyResult<bool> {
        self.inner
            .contains_key(key)
            .map_err(|e| os_err("looking up", e))
    }

    /// Take a key out and answer what it held, or `None` when it held
    /// nothing. The room it used goes back to the map.
    fn remove(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.remove(key, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("removing", e)),
        }
    }

    /// The smallest key and its value, or `None` when the map is empty.
    fn first(&self) -> PyResult<Option<(Vec<u8>, Vec<u8>)>> {
        let mut key = vec![0u8; self.inner.key_size()];
        let mut value = vec![0u8; self.inner.value_size()];
        match self.inner.first(&mut key, &mut value) {
            Ok(true) => Ok(Some((key, value))),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("reading the first entry", e)),
        }
    }

    /// The largest key and its value, or `None` when the map is empty.
    fn last(&self) -> PyResult<Option<(Vec<u8>, Vec<u8>)>> {
        let mut key = vec![0u8; self.inner.key_size()];
        let mut value = vec![0u8; self.inner.value_size()];
        match self.inner.last(&mut key, &mut value) {
            Ok(true) => Ok(Some((key, value))),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("reading the last entry", e)),
        }
    }

    /// Remove every entry, so the map is empty and its room is free
    /// again. The capacity and both widths are unchanged, and every
    /// process mapping the file sees it.
    fn clear(&self) {
        self.inner.clear();
    }

    /// Ask the operating system to write the mapping back to its file,
    /// and wait for it. Another process mapping the same file sees the
    /// entries without this; flushing is about surviving a machine that
    /// stops.
    fn flush(&self) -> PyResult<()> {
        self.inner.flush().map_err(|e| os_err("flushing", e))
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The entries stay in the file for whoever attaches next,
    /// and nothing is flushed: call `flush` where durability matters.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.BTreeMap len={} capacity={}>",
            self.inner.len(),
            self.inner.capacity()
        )
    }
}

/// A hash map in a mapped file, at key and value sizes the caller
/// declares, shared by every process that opens it.
#[pyclass(module = "subetha", name = "HashMap")]
struct HashMap_ {
    inner: Box<RawHashMap>,
}

#[pymethods]
impl HashMap_ {
    /// Obtain the map at `path` holding `capacity` entries of `key_size`
    /// and `value_size` bytes, creating it when the file does not exist.
    ///
    /// Both widths are fixed: they are the layout every process reads out
    /// of the file, not a maximum this one will store. The capacity never
    /// grows, so a full map refuses an insert rather than rehashing into
    /// something bigger.
    #[new]
    fn new(path: &str, capacity: usize, key_size: usize, value_size: usize) -> PyResult<Self> {
        RawHashMap::create(path, capacity, key_size, value_size)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("opening the map", e))
    }

    /// Attach to a map that already exists, raising `OSError` when it
    /// does not. The capacity and both widths must be the ones it was
    /// created with.
    #[staticmethod]
    fn open(path: &str, capacity: usize, key_size: usize, value_size: usize) -> PyResult<Self> {
        RawHashMap::open(path, capacity, key_size, value_size)
            .map(|inner| Self { inner: Box::new(inner) })
            .map_err(|e| os_err("attaching to the map", e))
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn key_size(&self) -> usize {
        self.inner.key_size()
    }

    #[getter]
    fn value_size(&self) -> usize {
        self.inner.value_size()
    }

    /// How many entries the map holds, which is not the capacity and does
    /// not count tombstones: a removed key stops being counted here while
    /// its marker still occupies a slot. Read `tombstones` for those.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Insert or replace. Returns `"inserted"` or `"updated"`, and
    /// `None` when the map is full.
    fn insert(&self, key: &[u8], value: &[u8]) -> PyResult<Option<&'static str>> {
        match self.inner.insert(key, value) {
            Ok(InsertOutcome::Inserted) => Ok(Some("inserted")),
            Ok(InsertOutcome::Updated) => Ok(Some("updated")),
            Err(MapError::Full) => Ok(None),
            Err(e) => Err(os_err("inserting", e)),
        }
    }

    /// Insert a run of pairs, stopping at the first the map refuses.
    fn insert_many(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<usize> {
        let mut done = 0;
        for (key, value) in &pairs {
            match self.inner.insert(key, value) {
                Ok(_) => done += 1,
                Err(MapError::Full) => break,
                Err(e) => return Err(os_err("inserting", e)),
            }
        }
        Ok(done)
    }

    /// What `key` holds, or `None` when it holds nothing. The value is
    /// always `value_size` bytes. Use `get_many` for a run of keys: it
    /// costs one crossing rather than one per key.
    fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.get(key, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("reading", e)),
        }
    }

    /// Look a run of keys up in one call, answering `None` per key that
    /// is absent.
    fn get_many(&self, keys: Vec<Vec<u8>>) -> PyResult<Vec<Option<Vec<u8>>>> {
        let mut answers = Vec::with_capacity(keys.len());
        for key in &keys {
            let mut out = vec![0u8; self.inner.value_size()];
            match self.inner.get(key, &mut out) {
                Ok(true) => answers.push(Some(out)),
                Ok(false) => answers.push(None),
                Err(e) => return Err(os_err("reading", e)),
            }
        }
        Ok(answers)
    }

    /// Whether the key is present, without bringing its value back over
    /// the boundary. Cheaper than `get` when the value is not wanted.
    fn __contains__(&self, key: &[u8]) -> PyResult<bool> {
        self.inner
            .contains_key(key)
            .map_err(|e| os_err("looking up", e))
    }

    /// Remove a key and return what it held, or `None` if it held
    /// nothing.
    fn remove(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.remove(key, &mut out) {
            Ok(true) => Ok(Some(out)),
            Ok(false) => Ok(None),
            Err(e) => Err(os_err("removing", e)),
        }
    }

    /// Replace `expected` with `new` only if that is what is there.
    /// Returns whether it swapped and what was found.
    fn compare_exchange(
        &self,
        key: &[u8],
        expected: &[u8],
        new: &[u8],
    ) -> PyResult<(bool, Vec<u8>)> {
        let mut current = vec![0u8; self.inner.value_size()];
        let swapped = self
            .inner
            .compare_exchange(key, expected, new, &mut current)
            .map_err(|e| os_err("comparing and exchanging", e))?;
        Ok((swapped, current))
    }

    /// Remove every entry and every tombstone, so both `len` and
    /// `tombstones` go to zero and the whole capacity is usable again.
    /// The widths are unchanged, and every process mapping the file sees
    /// it. This is the only thing that clears tombstones.
    fn clear(&self) {
        self.inner.clear();
    }

    #[getter]
    fn tombstones(&self) -> usize {
        self.inner.tombstone_count()
    }

    /// Answer the same object, so a `with` block can give it a name.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Leave the block without closing anything, and let an exception
    /// through. The entries stay in the file for whoever attaches next.
    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<subetha.HashMap len={} capacity={}>",
            self.inner.len(),
            self.inner.capacity()
        )
    }
}

/// One producer's end of an MPSC pool.
///
/// The Rust type is deliberately not `Sync`: a producer belongs to one
/// thread, and the class carries that through rather than hiding it, so
/// handing one to another thread is refused rather than silently wrong.
#[pyclass(module = "subetha", unsendable)]
struct MpscProducer {
    inner: Box<subetha_cxc::mpsc_ring::MpscProducer>,
}

#[pymethods]
impl MpscProducer {
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[getter]
    fn payload_size(&self) -> usize {
        SPSC_PAYLOAD_BYTES
    }

    /// Push one item into this producer's own ring.
    ///
    /// `False` means that ring is full, which is an answer rather than a
    /// failure. A full ring here does not mean the pool is full: the
    /// other producers have rings of their own, and only the consumer
    /// drains this one.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("pushing", e)),
        }
    }

    /// Push a run of items in one crossing, stopping at the first that
    /// will not fit, and answer how many landed. A count short of what
    /// was handed in leaves the rest with the caller.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            match self.inner.try_push(item) {
                Ok(()) => pushed += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// Push items cut out of one buffer, `item_len` bytes each, and
    /// answer how many landed.
    ///
    /// This is the cheapest shape on this class: the caller hands over
    /// one object, and no Python object is built per item on either side.
    /// A trailing piece shorter than `item_len` is pushed as it is. Stops
    /// at the first that will not fit, and an `item_len` of zero is a
    /// `ValueError`.
    fn push_buffer(&self, data: &[u8], item_len: usize) -> PyResult<usize> {
        if item_len == 0 {
            return Err(PyValueError::new_err("item_len must not be zero"));
        }
        let mut pushed = 0;
        for chunk in data.chunks(item_len) {
            match self.inner.try_push(chunk) {
                Ok(()) => pushed += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    fn __repr__(&self) -> String {
        format!("<subetha.MpscProducer capacity={}>", self.inner.capacity())
    }
}

/// The single consumer of an MPSC pool, draining every producer's ring
/// in turn.
#[pyclass(module = "subetha", unsendable)]
struct MpscConsumer {
    inner: Box<subetha_cxc::mpsc_ring::MpscConsumer>,
}

#[pymethods]
impl MpscConsumer {
    #[getter]
    fn producers(&self) -> usize {
        self.inner.n_producers()
    }

    /// How many items are waiting across every producer's ring. Read
    /// without stopping the producers, so it is a sighting rather than a
    /// promise.
    #[getter]
    fn approx_len(&self) -> usize {
        self.inner.approx_total_len()
    }

    /// Take the next item from whichever producer's ring has one, or
    /// `None` when they are all empty.
    ///
    /// What one producer sent arrives in the order it sent it, because
    /// that producer has a ring to itself. Across producers there is no
    /// order at all: the consumer walks the rings in turn, so two items
    /// sent at the same moment by different producers can arrive either
    /// way round.
    fn pop(&self) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.inner.try_pop(&mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("popping", e)),
        }
    }

    /// Take up to `max_items` in one crossing, stopping early once every
    /// ring is empty. An empty list means there was nothing, which is the
    /// same answer `pop` gives as `None`.
    fn pop_many(&self, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_pop(&mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("popping", e)),
            }
        }
        Ok(taken)
    }

    fn __repr__(&self) -> String {
        format!("<subetha.MpscConsumer producers={}>", self.inner.n_producers())
    }
}

/// Build an MPSC pool: one ring per producer, drained by one consumer.
///
/// Returns the producers and the consumer. The underlying constructor
/// asserts on a producer count below one, so that is refused here first:
/// a Rust assertion reaching Python as a panic is not an error message.
#[pyfunction]
#[pyo3(signature = (path, producers, capacity))]
fn mpsc_pool(path: &str, producers: usize, capacity: usize) -> PyResult<(Vec<MpscProducer>, MpscConsumer)> {
    if producers < 1 {
        return Err(PyValueError::new_err("a pool needs at least one producer"));
    }
    let (ps, c) = subetha_cxc::mpsc_ring::SharedRingMpsc::create_pool(path, producers, capacity)
        .map_err(|e| os_err("building the pool", e))?;
    Ok((
        ps.into_iter().map(|p| MpscProducer { inner: Box::new(p) }).collect(),
        MpscConsumer { inner: Box::new(c) },
    ))
}

/// Attach to a pool that already exists, with the shape it was built at.
#[pyfunction]
#[pyo3(signature = (path, producers, capacity))]
fn mpsc_pool_open(path: &str, producers: usize, capacity: usize) -> PyResult<(Vec<MpscProducer>, MpscConsumer)> {
    if producers < 1 {
        return Err(PyValueError::new_err("a pool needs at least one producer"));
    }
    let (ps, c) = subetha_cxc::mpsc_ring::SharedRingMpsc::open_pool(path, producers, capacity)
        .map_err(|e| os_err("attaching to the pool", e))?;
    Ok((
        ps.into_iter().map(|p| MpscProducer { inner: Box::new(p) }).collect(),
        MpscConsumer { inner: Box::new(c) },
    ))
}

/// One producer's end of an MPMC grid.
#[pyclass(module = "subetha", unsendable)]
struct MpmcProducer {
    inner: Box<subetha_cxc::mpmc_ring::MpmcProducer>,
}

#[pymethods]
impl MpmcProducer {
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Push one item into this producer's own ring.
    ///
    /// `False` means that ring is full, which is an answer rather than a
    /// failure. A full ring here does not mean the grid is full: the
    /// other producers have rings of their own.
    fn push(&self, item: &[u8]) -> PyResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(os_err("pushing", e)),
        }
    }

    /// Push a run of items in one crossing, stopping at the first that
    /// will not fit, and answer how many landed. A count short of what
    /// was handed in leaves the rest with the caller.
    fn push_many(&self, items: Vec<Vec<u8>>) -> PyResult<usize> {
        let mut pushed = 0;
        for item in &items {
            match self.inner.try_push(item) {
                Ok(()) => pushed += 1,
                Err(RingError::Full) => break,
                Err(e) => return Err(os_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    fn __repr__(&self) -> String {
        format!("<subetha.MpmcProducer capacity={}>", self.inner.capacity())
    }
}

/// One consumer's end of an MPMC grid, draining the subset of rings it
/// was given.
#[pyclass(module = "subetha", unsendable)]
struct MpmcConsumer {
    inner: Box<subetha_cxc::mpmc_ring::MpmcConsumer>,
}

#[pymethods]
impl MpmcConsumer {
    #[getter]
    fn rings(&self) -> usize {
        self.inner.n_rings()
    }

    #[getter]
    fn approx_len(&self) -> usize {
        self.inner.approx_subset_len()
    }

    /// Take the next item from one of the rings this consumer was given,
    /// or `None` when they are all empty.
    ///
    /// It never takes from a ring another consumer was given, which is
    /// what lets several consumers drain the grid at once without
    /// agreeing anything between them. So `None` here means this
    /// consumer's own rings are empty, not that the grid is.
    fn pop(&self) -> PyResult<Option<Vec<u8>>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.inner.try_pop(&mut out) {
            Ok(n) => {
                out.truncate(n);
                Ok(Some(out))
            }
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(os_err("popping", e)),
        }
    }

    /// Take up to `max_items` in one crossing from this consumer's own
    /// rings, stopping early once they are empty. An empty list means
    /// there was nothing, which is the same answer `pop` gives as `None`.
    fn pop_many(&self, max_items: usize) -> PyResult<Vec<Vec<u8>>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.inner.try_pop(&mut out) {
                Ok(n) => taken.push(out[..n].to_vec()),
                Err(RingError::Empty) => break,
                Err(e) => return Err(os_err("popping", e)),
            }
        }
        Ok(taken)
    }

    fn __repr__(&self) -> String {
        format!("<subetha.MpmcConsumer rings={}>", self.inner.n_rings())
    }
}

/// Build an MPMC grid: a ring per producer, shared out among consumers.
///
/// The constructor asserts that there is at least one consumer and that
/// producers are not outnumbered by them, so both are refused here first
/// rather than reaching Python as a panic.
#[pyfunction]
#[pyo3(signature = (path, producers, consumers, capacity))]
fn mpmc_grid(
    path: &str,
    producers: usize,
    consumers: usize,
    capacity: usize,
) -> PyResult<(Vec<MpmcProducer>, Vec<MpmcConsumer>)> {
    if consumers < 1 {
        return Err(PyValueError::new_err("a grid needs at least one consumer"));
    }
    if producers < consumers {
        return Err(PyValueError::new_err(
            "a grid needs at least as many producers as consumers",
        ));
    }
    let (ps, cs) =
        subetha_cxc::mpmc_ring::SharedRingMpmc::create_grid(path, producers, consumers, capacity)
            .map_err(|e| os_err("building the grid", e))?;
    Ok((
        ps.into_iter().map(|p| MpmcProducer { inner: Box::new(p) }).collect(),
        cs.into_iter().map(|c| MpmcConsumer { inner: Box::new(c) }).collect(),
    ))
}

/// Attach to a grid that already exists, with the shape it was built at.
#[pyfunction]
#[pyo3(signature = (path, producers, consumers, capacity))]
fn mpmc_grid_open(
    path: &str,
    producers: usize,
    consumers: usize,
    capacity: usize,
) -> PyResult<(Vec<MpmcProducer>, Vec<MpmcConsumer>)> {
    if consumers < 1 {
        return Err(PyValueError::new_err("a grid needs at least one consumer"));
    }
    if producers < consumers {
        return Err(PyValueError::new_err(
            "a grid needs at least as many producers as consumers",
        ));
    }
    let (ps, cs) =
        subetha_cxc::mpmc_ring::SharedRingMpmc::open_grid(path, producers, consumers, capacity)
            .map_err(|e| os_err("attaching to the grid", e))?;
    Ok((
        ps.into_iter().map(|p| MpmcProducer { inner: Box::new(p) }).collect(),
        cs.into_iter().map(|c| MpmcConsumer { inner: Box::new(c) }).collect(),
    ))
}

/// Whether a broadcast error means the ring was full, which is an answer
/// rather than a failure. Matched on the name so a new variant does not
/// silently become a success.
fn is_broadcast_full(e: &subetha_cxc::shared_broadcast_ring::BroadcastError) -> bool {
    matches!(e, subetha_cxc::shared_broadcast_ring::BroadcastError::Full)
}

/// Whether a broadcast error means the consumer has caught up.
fn is_broadcast_empty(e: &subetha_cxc::shared_broadcast_ring::BroadcastError) -> bool {
    matches!(e, subetha_cxc::shared_broadcast_ring::BroadcastError::Empty)
}

/// What Python's object allocator guarantees. A `#[pyclass]` whose
/// alignment exceeds this is placed at an address that does not satisfy
/// it, and a release build faults on the first aligned store.
const PY_OBJECT_ALIGNMENT: usize = 16;

/// Fail the build for any class that asks for more alignment than Python
/// gives it.
///
/// Nearly every SubEtha primitive embeds a cache-line aligned
/// `HandshakeHeader`, so holding one inline rather than boxed is the easy
/// mistake, and it is not one the compiler or the test suite catches on
/// its own: such a class compiles, imports, and then faults inside its
/// constructor. Every class this module adds belongs in this list.
macro_rules! classes_fit_python_allocation {
    ($($class:ty),+ $(,)?) => {
        $(
            const _: () = assert!(
                std::mem::align_of::<$class>() <= PY_OBJECT_ALIGNMENT,
                concat!(
                    stringify!($class),
                    " needs more alignment than Python's allocator gives. Box the field that \
                     asks for it: a Box comes from Rust's allocator, which honors it."
                ),
            );
        )+
    };
}

classes_fit_python_allocation!(
    Atomic,
    Region,
    SpscRing,
    BroadcastRing,
    MpscProducer,
    MpscConsumer,
    MpmcProducer,
    MpmcConsumer,
    Cell,
    Vec_,
    HashMap_,
    Slab,
    BTreeMap_,
    Arena,
    LinkedList,
    RWLock,
    Hold,
    Semaphore,
    PermitHold,
    FrameRegion,
    LamportProducer,
    LamportConsumer,
    PubSub,
    Subscriber,
    Stack,
    Deque,
    Ring,
    BloomFilter,
    Histogram,
    RateLimiter,
    HyperLogLog,
    CountMinSketch,
    BitVec,
    LazyValue,
    FenceClock,
    Condvar,
    Heartbeat,
    EpochBarrier,
    LeaderElection,
    HolderTable,
    SharedArc,
    NotifierSet,
    Notifier,
    LruCache,
    Epochs,
    CapacityRing,
    LocaleRing,
    OrderedReceiver,
    OwnerLease,
    Reservoir,
    BlockedBloomFilter,
    HandleTable,
    TimePointTile,
    VersionChain,
    VersionedSlab,
    VersionedMap,
    LanedMap,
    TopologyMap,
    Graph,
    Universal,
    Tower,
    QosPolicy,
    Channel,
    KvMap,
    WorkQueue,
    AdaptiveQueue,
    LossKind,
    LossBursts,
    Timing,
    RoundTripShape,
    Periodicity,
    Capacity,
    Forecast,
    PathChanges,
    ReorderWindow,
);

/// What this binding costs, reported by the binding itself so a caller
/// can check the claim on its own machine rather than take the number in
/// the documentation.
#[pyfunction]
fn boundary_note() -> &'static str {
    "a call from Python costs tens to hundreds of nanoseconds; the C ABI boundary underneath costs about 7"
}

/// `gil_used = false` tells a free-threaded interpreter not to switch
/// the lock back on when this module is imported.
///
/// It is a claim that every call here is safe with several threads
/// inside it at once, and it is made because tests/test_threading.py
/// passes on an interpreter with no lock: every call that releases the
/// interpreter, both kinds of lock hold, the semaphore's permits, a
/// shared ring, a pinned scan running beside writers, and a buffer view
/// held across other threads' work. Anything added here that is not safe
/// that way makes this declaration wrong, so add the threading test
/// with it.
#[pymodule(gil_used = false)]
fn _subetha(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Atomic>()?;
    m.add_class::<Region>()?;
    m.add_class::<SpscRing>()?;
    m.add_class::<BroadcastRing>()?;
    m.add_class::<MpscProducer>()?;
    m.add_class::<MpscConsumer>()?;
    m.add_class::<MpmcProducer>()?;
    m.add_class::<MpmcConsumer>()?;
    m.add_class::<Cell>()?;
    m.add_class::<Vec_>()?;
    m.add_class::<HashMap_>()?;
    m.add_class::<Slab>()?;
    m.add_class::<BTreeMap_>()?;
    m.add_class::<Arena>()?;
    m.add_class::<LinkedList>()?;
    m.add_class::<RWLock>()?;
    m.add_class::<Hold>()?;
    m.add_class::<Semaphore>()?;
    m.add_class::<PermitHold>()?;
    m.add_class::<FrameRegion>()?;
    m.add_class::<LamportProducer>()?;
    m.add_class::<LamportConsumer>()?;
    m.add_class::<PubSub>()?;
    m.add_class::<Subscriber>()?;
    m.add_class::<Stack>()?;
    m.add_class::<Deque>()?;
    m.add_class::<Ring>()?;
    m.add_class::<BloomFilter>()?;
    m.add_class::<Histogram>()?;
    m.add_class::<RateLimiter>()?;
    m.add_class::<HyperLogLog>()?;
    m.add_class::<CountMinSketch>()?;
    m.add_class::<BitVec>()?;
    m.add_class::<LazyValue>()?;
    m.add_class::<FenceClock>()?;
    m.add_class::<Condvar>()?;
    m.add_class::<Heartbeat>()?;
    m.add_class::<EpochBarrier>()?;
    m.add_class::<LeaderElection>()?;
    m.add_class::<HolderTable>()?;
    m.add_class::<SharedArc>()?;
    m.add_class::<NotifierSet>()?;
    m.add_class::<Notifier>()?;
    m.add_class::<LruCache>()?;
    m.add_class::<Epochs>()?;
    m.add_class::<CapacityRing>()?;
    m.add_class::<LocaleRing>()?;
    m.add_class::<OrderedReceiver>()?;
    m.add_class::<ReorderWindow>()?;
    m.add_class::<OwnerLease>()?;
    m.add_class::<LeaseHold>()?;
    m.add_class::<Reservoir>()?;
    m.add_class::<BlockedBloomFilter>()?;
    m.add_class::<HandleTable>()?;
    m.add_class::<TimePointTile>()?;
    m.add_class::<VersionChain>()?;
    m.add_class::<VersionedSlab>()?;
    m.add_class::<SlabPin>()?;
    m.add_class::<VersionedMap>()?;
    m.add_class::<MapPin>()?;
    m.add_class::<LanedMap>()?;
    m.add_class::<LaneClaim>()?;
    m.add_class::<LanedPin>()?;
    m.add("WrongLane", m.py().get_type::<WrongLane>())?;
    m.add_class::<TopologyMap>()?;
    m.add_class::<Graph>()?;
    m.add_class::<Universal>()?;
    m.add_class::<Tower>()?;
    m.add_class::<QosPolicy>()?;
    m.add_class::<QosSnapshot>()?;
    m.add_class::<Channel>()?;
    m.add_class::<KvMap>()?;
    m.add_class::<WorkQueue>()?;
    m.add_class::<AdaptiveQueue>()?;
    m.add_class::<TinyBloom>()?;
    m.add_class::<FineBloom>()?;
    m.add_class::<Clock>()?;
    m.add_class::<CausalClock>()?;
    m.add_class::<LossKind>()?;
    m.add_class::<LossBursts>()?;
    m.add_class::<Timing>()?;
    m.add_class::<RoundTripShape>()?;
    m.add_class::<Periodicity>()?;
    m.add_class::<Capacity>()?;
    m.add_class::<Forecast>()?;
    m.add_class::<PathChanges>()?;
    m.add_class::<SensSender>()?;
    m.add_class::<SensReceiver>()?;
    #[cfg(feature = "tcp-bridge")]
    {
        m.add_class::<TcpBridgeClient>()?;
        m.add_class::<TcpBridgeServer>()?;
    }
    #[cfg(feature = "quic-bridge")]
    {
        m.add_class::<QuicBridgeClient>()?;
        m.add_class::<QuicBridgeServer>()?;
        m.add_function(wrap_pyfunction!(generate_self_signed_cert, m)?)?;
    }
    // What a wheel was built with, so a caller can tell a name that is
    // absent because the feature was left out from one that never
    // existed.
    m.add("transports", transports_built())?;
    // Whether this extension was built against an interpreter with no
    // lock. A caller writing threaded code needs to know which it has,
    // and so does the threading suite, which only means what it says on
    // the build that runs its threads at the same time.
    m.add("free_threaded", cfg!(Py_GIL_DISABLED))?;
    m.add("Lagged", m.py().get_type::<Lagged>())?;
    m.add("Contended", m.py().get_type::<Contended>())?;
    m.add_function(wrap_pyfunction!(lamport_pair, m)?)?;
    m.add_function(wrap_pyfunction!(lamport_pair_open, m)?)?;
    m.add_function(wrap_pyfunction!(mpsc_pool, m)?)?;
    m.add_function(wrap_pyfunction!(mpsc_pool_open, m)?)?;
    m.add_function(wrap_pyfunction!(mpmc_grid, m)?)?;
    m.add_function(wrap_pyfunction!(mpmc_grid_open, m)?)?;
    m.add_function(wrap_pyfunction!(boundary_note, m)?)?;
    m.add("__doc__", "SubEtha: shared memory between processes, bound to the Rust directly.")?;
    Ok(())
}
