//! `subetha-cxc` - CXC (Cross-Context Channel): memory-mapped file
//! primitives for cross-thread, cross-process, and disk-persistent
//! communication.
//!
//! The name comes from Douglas Adams's *Hitchhiker's Guide to the
//! Galaxy*: the Sub-Etha Sens-O-Matic uses sub-etheric waves to
//! communicate **around** conventional channels. CXC does the same
//! thing - it skips the kernel's conventional channel (named pipes,
//! sockets, IPC handles) and writes directly to user-space memory
//! the kernel page-aliases between participants.
//!
//! Four cooperating modules:
//! - [`shared_ring`]: MPMC lock-free ring backed by an MMF
//! - [`heartbeat`]: per-process liveness slots in an MMF table
//! - [`failover`]: watchdog that reclaims work from dead processes
//! - [`pass_registry`]: closure registry for cross-process Pass dispatch
//! - [`scheduler`]: BackgroundScheduler tying the above into an
//!   autonomous executor
//!
//! # The unifying mechanism
//!
//! Every primitive in this crate uses one mechanism: a file-mapped
//! MMF region. That single mechanism gives:
//!
//! 1. **Cross-thread** communication (threads in one process map the
//!    same file; lock-free CAS handles concurrency).
//! 2. **Cross-process** communication (different processes open the
//!    same file; the OS page-cache aliases them onto the same
//!    physical pages).
//! 3. **Disk persistence** (the MMF is backed by a real file; data
//!    survives process death and can be reopened later).
//!
//! The same byte layout serves all three. There is no separate
//! "shared memory" vs "disk" abstraction; the MMF is both at once.
//!
//! # Architectural pattern
//!
//! Bare OS IPC primitives (named pipes, sockets, anonymous shared
//! memory) hide useful metadata: which logical stream a message
//! belongs to, what priority it has, whether the peer is alive,
//! when the data needs to hit disk. The protocol layer here names
//! each of those as a first-class field so the application can
//! reason about (and direct) the substrate's behaviour.
//!
//! The win over OS pipes/sockets is the same shape as QUIC over
//! TCP: userspace transport eliminates the kernel from the hot
//! path. A pipe write costs ~20us (CreateFile + WriteFile
//! syscalls); a SharedRing slot publish is ~50-200ns (atomic CAS +
//! fence). That's a ~100x improvement that vanishes the moment a
//! Mutex enters the picture.

pub mod sidecar_ops;

pub mod cached_clock;
pub mod epoch_barrier;
pub mod event_state_log;
pub mod failover;
pub mod k_tower_cascade;
pub mod heartbeat;
pub mod lazy_config;
pub mod owner_lease;
pub mod pass_registry;
pub mod priority_fanout;
pub mod progress_task;
pub mod reorder;
pub mod scheduler;
pub mod shared_async_pointer;
pub mod shared_atomic;
pub mod shared_bit_vec;
pub mod shared_blocked_bloom_filter;
pub mod shared_bloom_filter;
pub mod shared_btree_map;
pub mod shared_broadcast_ring;
pub mod shared_cell;
pub mod shared_count_min_sketch;
pub mod adaptive_ipc;
pub mod api;
pub mod dispatch_deque;
pub mod message_transport;
pub mod mmf_dispatcher;
pub mod shared_deque;
pub mod shared_deque_fcl;
pub mod shared_deque_khl;
pub mod shared_deque_khpd;
pub mod shared_deque_loh;
pub mod shared_deque_urd;
pub mod shared_fence_clock;
pub mod shared_graph;
pub mod shared_handle_table;
pub mod shared_hash_map;
pub mod shared_histogram;
pub mod shared_hyper_log_log;
pub mod shared_leader_election;
pub mod shared_linked_list;
pub mod shared_lru_cache;
pub mod shared_nan_tagged_value;
pub mod shared_nan_value;
pub mod shared_once_cell;
pub mod shared_rate_limiter;
pub mod shared_region;
pub mod shared_reservoir_sampler;
pub mod cpu_affinity;
pub mod task_pool;
pub mod waker_ring;
pub mod ring_executor;
pub mod reactor;
pub mod net_bridge;
pub mod shared_ring;
pub mod spsc_ring;
pub mod frame_ring;
pub mod frame_region;
pub mod mpsc_ring;
pub mod mpmc_ring;
pub mod adaptive_ring;
pub mod ring_contract;
pub mod capacity_adaptive_ring;
pub mod policy_gate;
pub mod unified_policy;
pub mod phase_estimator;
pub mod capacity_broadcast_ring;
pub mod capacity_pubsub_ring;
pub mod async_ring;
pub mod blocking_spsc_ring;
pub mod blocking_mpsc_ring;
pub mod blocking_mpmc_ring;
pub mod blocking_rw_lock;
pub mod blocking_semaphore;
#[cfg(feature = "tcp-bridge")]
pub mod blocking_tcp_bridge;
pub mod bbr;
pub mod cache_ops;
pub mod control_frame;
pub mod control_table;
pub mod fec;
pub mod rlc_fec;
pub mod rlc_control;
#[cfg(feature = "tls")]
pub mod rlc_crypto;
pub mod dgram;
/// Sens-O-Matic transport carrying the sliding-window RLC erasure code (the
/// adaptive, optionally-TLS code; the block Reed-Solomon code lives in
/// [`udp_bridge`]). The RLC coding internals are in [`rlc_fec`] / [`rlc_control`].
pub mod sens_rlc;
/// Unified Sens-O-Matic endpoint: one transport carrying both erasure codes,
/// switching RLC <-> RS mid-stream on the loss the receiver feeds back (the
/// loss-driven auto-switch, with operator override).
pub mod sens_unified;
pub mod fusion;
pub mod interleave;
pub mod link_sensor;
pub mod compressed_udp;
pub mod reliable_udp;
pub mod salvage;
pub mod schema_codec;
pub mod sharded_udp;
pub mod path_sensor;
pub mod path_model_sensor;
pub mod net_events;
pub mod stream_mux;
pub mod wbest_sensor;
pub mod trace_sensor;
pub mod forecast_sensor;
pub mod periodicity_sensor;
pub mod rtt_shape_sensor;
pub mod burst_model_sensor;
pub mod loss_class_sensor;
pub mod temporal_sensor;
pub mod tower;
pub mod udp_bridge;
pub mod cross_process_waker;
pub mod shared_condvar;
pub mod locale_adaptive_ring;
pub mod mmf_warm;
pub mod monitor_wait;
pub mod net_tune;
pub mod ordering;
pub mod peer_directory;
pub mod protocol_pubsub;
pub mod qos_policy;
pub mod replay_positions;
pub mod shm_file;
pub mod virtual_endpoint;
#[cfg(target_os = "linux")]
pub mod hugepages;
#[cfg(windows)]
pub mod large_pages;
#[cfg(any(target_os = "freebsd", target_os = "macos"))]
pub mod super_pages;
#[cfg(any(target_os = "linux", windows))]
pub mod locale_vsock;
#[cfg(any(unix, windows))]
pub mod protocol_direct_file;
#[cfg(any(unix, windows))]
pub mod fd_handoff;
#[cfg(any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"))]
pub mod kernel_async_ring;
#[cfg(all(
    any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"),
    feature = "wire-locale"
))]
pub mod locale_wire;
#[cfg(feature = "quic-bridge")]
pub mod quic_bridge;
#[cfg(feature = "quic-bridge")]
pub mod sens_quic;
#[cfg(feature = "tcp-bridge")]
pub mod tcp_bridge;
#[cfg(feature = "tcp-tls-bridge")]
pub mod tcp_tls_bridge;
pub mod shared_rw_lock;
pub mod shared_semaphore;
pub mod shared_string_arena;
pub mod shared_time_point;
pub mod shared_topology_map;
pub mod shared_treiber_stack;
pub mod shared_umbra_pointer;
pub mod shared_universal;
pub mod shared_vec;
pub mod shared_versioned_chain;
pub mod tagged_offset_ptr;

pub use epoch_barrier::{BarrierError, EpochBarrier, DEFAULT_BARRIER_GRACE_EPOCHS};
pub use event_state_log::{EventLogError, EventStateLog};
pub use failover::{FailoverWatchdog, ReclaimReport, DEFAULT_GRACE_EPOCHS};
pub use k_tower_cascade::{
    CascadeError, CascadeResolver2, CascadeResolverN, KTowerCascade,
    NIL_INDEX as CASCADE_NIL_INDEX,
};
pub use lazy_config::{LazyConfig, LazyConfigError};
pub use owner_lease::{
    LeaseError, LeaseHeader, LeasePayload, OwnerLease,
    LEASE_FILE_SIZE, LEASE_MAGIC, NO_OWNER,
    PAYLOAD_BYTES as LEASE_PAYLOAD_BYTES,
};
pub use heartbeat::{
    HeartbeatError, HeartbeatHeader, HeartbeatSlot, HeartbeatSnapshot,
    HeartbeatTable, EMPTY_PID, HEARTBEAT_MAGIC, IN_FLIGHT_SLOTS,
};
pub use pass_registry::{
    execute as execute_pass, is_registered, register as register_handler,
    registered_count, unregister as unregister_handler,
    Pass, PassError, PassHandler, PassResult,
};
pub use priority_fanout::{FanoutError, PriorityFanout, MAX_PRIORITIES};
pub use progress_task::{ProgressReporter, ProgressTask, ProgressTaskError};
pub use scheduler::{
    BackgroundScheduler, ResultCollector, SchedError, SubmittedResult,
    Submitter,
};
pub use shared_async_pointer::{SharedAsyncError, SharedAsyncPointer};
pub use shared_atomic::{
    SharedAtomicBool, SharedAtomicError, SharedAtomicU32, SharedAtomicU64,
};
pub use shared_bit_vec::{
    bit_vec_file_size, BitVecError, BitVecHeader, SharedBitVec,
    BITS_PER_WORD, BITVEC_MAGIC,
};
pub use shared_blocked_bloom_filter::{
    BlockedBloomError, SharedBlockedBloomFilter,
};
pub use shared_bloom_filter::{
    BloomError, BloomHeader, SharedBloomFilter, BLOOM_MAGIC,
};
pub use shared_broadcast_ring::{
    broadcast_file_size, BroadcastError, BroadcastHeader, BroadcastSlot,
    SharedBroadcastRing, BROADCAST_MAGIC, BROADCAST_PAYLOAD_BYTES,
    MAX_CONSUMERS,
};
pub use shared_cell::{
    CellHeader, SharedCell, SharedCellError, CELL_FILE_SIZE, CELL_MAGIC,
    PAYLOAD_BYTES as CELL_PAYLOAD_BYTES,
};
pub use shared_count_min_sketch::{
    cms_file_size, CMSError, CMSHeader, SharedCountMinSketch, CMS_MAGIC,
};
pub use dispatch_deque::{
    DequeDispatcher, DequeVariant, DispatchError, DispatcherBuilder, WorkloadShape,
};
pub use message_transport::{MessageTransport, PassSlot, TransportError};
pub use mmf_dispatcher::{MmfDispatcher, MmfFamily, MmfWorkloadShape};
pub use api::{ApiError, AutoIpc, Channel, KvMap, WorkStealQueue};
pub use adaptive_ipc::{
    AdaptiveIpc, AdaptiveIpcSidecar, PinnedIpc, ProfileSnapshot,
};
pub use shared_deque::{
    deque_file_size, slot_bytes_for, DequeError, DequeHeader, SharedDeque, DEQUE_MAGIC,
};
pub use shared_deque_fcl::SharedDequeFcl;
pub use shared_deque_khl::{
    khl_file_size, KhlHeader, KhlSlot, PublishRadius as KhlPublishRadius,
    PushError as KhlPushError, SharedDequeKhl, Steal as KhlSteal,
    StealResult as KhlStealResult, KHL_ITEMS_PER_SLOT, KHL_MAGIC, KHL_SLOT_SIZE,
};
pub use shared_deque_khpd::{
    khpd_file_size, FatLineItem, KhpdHeader, LineItem, PublicationLine,
    PushError as KhpdPushError, SharedDequeKhpd, Steal as KhpdSteal,
    StealResult as KhpdStealResult, KHPD_ITEM_BYTES, KHPD_LINE_SIZE, KHPD_MAGIC,
    LINE_ITEMS,
};
pub use shared_deque_loh::{
    loh_file_size, LcrqJobSlot, LohHeader, PushError as LohPushError,
    SharedDequeLoh, Steal as LohSteal, StealResult as LohStealResult,
    DEFAULT_LIFO_CAP as LOH_DEFAULT_LIFO_CAP, LOH_MAGIC, LOH_SLOT_SIZE,
};
pub use shared_deque_urd::{
    urd_file_size, Drain as UrdDrain, DrainResult as UrdDrainResult, Mailbox,
    PublishError as UrdPublishError, PublishStrategy as UrdPublishStrategy,
    SharedDequeUrd, UrdHeader, WaitStrategy, MAILBOX_ITEMS, URD_MAGIC, URD_MAILBOX_SIZE,
};
pub use shared_fence_clock::{
    fence_clock_file_size, FenceClockError, Hlc, HlcHeader, HlcSlot,
    HlcSlotSnapshot, SharedFenceClock, FENCE_CLOCK_MAGIC,
};
pub use shared_graph::{
    EdgeIndex, GraphEdge, GraphError, GraphNode, NodeIndex, SharedGraph,
    NIL_INDEX as GRAPH_NIL_INDEX,
};
pub use shared_handle_table::{
    handle_table_file_size, slot_offset, Handle, HandleHeader, HandleTableError,
    SharedHandleTable, SharedSlot, HANDLE_TABLE_MAGIC, NIL_SLOT, SLOT_PAYLOAD_BYTES,
};
pub use shared_hash_map::{
    fnv1a_64, map_file_size, InsertOutcome, MapError, MapHeader, MapSlot,
    SharedHashMap, MAP_MAGIC, MAP_PAYLOAD_BYTES,
    SLOT_EMPTY, SLOT_OCCUPIED, SLOT_TOMBSTONE,
};
pub use shared_histogram::{
    histogram_file_size, HistogramError, HistogramHeader, SharedHistogram,
    HISTOGRAM_MAGIC,
};
pub use shared_hyper_log_log::{
    hll_file_size, HLLError, HLLHeader, SharedHyperLogLog,
    HLL_MAGIC, MAX_PRECISION as HLL_MAX_PRECISION,
    MIN_PRECISION as HLL_MIN_PRECISION,
};
pub use shared_leader_election::{
    LeaderError, LeaderHeader, SharedLeaderElection,
    DEFAULT_GRACE_EPOCHS as LEADER_DEFAULT_GRACE_EPOCHS,
    LEADER_FILE_SIZE, LEADER_MAGIC, NO_LEADER,
};
pub use shared_linked_list::{
    LinkedListError, Node as LinkedListNode, NodeHandle, SharedLinkedList,
    HEAD_INDEX as LINKED_LIST_HEAD_INDEX, NIL_INDEX as LINKED_LIST_NIL_INDEX,
};
pub use shared_lru_cache::{LRUError, SharedLRUCache};
pub use shared_nan_tagged_value::{NaNTaggedType, SharedNaNTaggedValue};
pub use shared_nan_value::{
    NaNValueType, SharedNaNValue,
    BOXED_MASK, BOXED_PREFIX, CANONICAL_QNAN, PAYLOAD_MASK,
    TAG_BOOL, TAG_I32, TAG_MASK, TAG_NIL, TAG_OFFSET_PTR, TAG_SHIFT,
    TAG_TAGGED_OFFSET_PTR, TAG_U32,
};
pub use shared_once_cell::{
    OnceHeader, SharedOnceCell, SharedOnceError, ONCE_FILE_SIZE, ONCE_MAGIC,
    ONCE_PAYLOAD_BYTES, STATE_EMPTY, STATE_INITIALIZED, STATE_INITIALIZING,
};
pub use shared_rate_limiter::{
    RateLimiterError, RateLimiterHeader, SharedRateLimiter, RATE_LIMITER_MAGIC,
};
pub use shared_region::{
    region_file_size, OffsetPtr, RegionError, RegionHeader, SharedRegion,
    NIL_INDEX, REGION_MAGIC,
};
pub use shared_reservoir_sampler::{
    reservoir_file_size, ReservoirError, ReservoirHeader, ReservoirSlot,
    SharedReservoirSampler, RESERVOIR_MAGIC, RESERVOIR_SLOT_PAYLOAD,
};
pub use shared_ring::{
    ring_file_size, Consumer as SpscConsumer, LazySharedRing, Producer as SpscProducer,
    RingError, RingHeader, SharedRing, SharedRingSpsc, Slot, PAYLOAD_BYTES,
    RING_MAGIC, SLOT_SIZE,
};
pub use frame_ring::{
    frame_ring_file_size, FrameClass, FrameRing, LayoutHint,
    DESC_HEADER_BYTES, FRAME_MAGIC, MIN_SLOT_SIZE,
};
pub use frame_region::{
    frame_region_file_size, FrameRegion, FRAME_REGION_MAGIC, MIN_BLOCK_SIZE,
};
pub use mpsc_ring::{
    MpscConsumer, MpscFifoConsumer, MpscFifoProducer, MpscProducer,
    SharedRingMpsc, SharedRingMpscFifo,
};
pub use mpmc_ring::{MpmcConsumer, MpmcProducer, SharedRingMpmc};
pub use adaptive_ring::{
    AdaptiveError, AdaptiveRing, AdaptiveRingSidecar, DefaultOrderingPolicy,
    DefaultRingShapePolicy, OrderingPolicy, OrderingPolicyObservation,
    PinnedRing, PolicyObservation, QosRingShapePolicy, RingShape,
    RingShapePolicy, ADAPTIVE_SPSC_PAYLOAD_BYTES,
    ADAPTIVE_VYUKOV_PAYLOAD_BYTES, DRAINER_GRACE_EPOCHS,
};
pub use cache_ops::{cldemote, has_cldemote, prefetchw, sfence};
pub use mmf_warm::{warm_mmap, warm_region};
pub use monitor_wait::{
    monitor_wait_budget_cycles, monitor_wait_kind, monitor_wait_u32,
    monitor_wait_u32_with, monitor_wait_u64, monitor_wait_u64_with,
    MonitorWaitKind, DEFAULT_MONITOR_BUDGET_CYCLES,
};
pub use ordering::{
    default_stamp_kind, has_invariant_tsc, ordering_region_size,
    OrderingHeader, OrderingMode, OrderingRegion, StampKind,
    MONOTONIC_FRESHNESS_GUARD_NANOS, ORDERING_MAGIC, STAMPED_PAYLOAD_BYTES,
    STAMP_BYTES, TSC_FRESHNESS_GUARD_CYCLES,
};
pub use qos_policy::{
    Durability, History, Ordering as QosOrdering, QosPolicy, QosSnapshot,
    Reliability,
};
pub use capacity_adaptive_ring::{
    BackingTarget, CapacityAdaptiveRing, CapacityAdaptiveRingSidecar,
    CapacityMorphError, CapacityPolicy, CapacityPolicyObservation,
    DefaultCapacityPolicy, PinnedCapacity, RingConfig,
};
pub use policy_gate::{min_samples_for_arity, ConfidenceGate, GateConfig};
pub use unified_policy::{
    UnifiedObservation, UnifiedPolicy, UnifiedSidecar, UnifiedWeights,
};
pub use phase_estimator::{PhaseConfig, PhaseEstimator};
pub use capacity_broadcast_ring::{
    BroadcastCapacityMorphError, CapacityBroadcastRing, PinnedBroadcastCapacity,
};
pub use capacity_pubsub_ring::{
    CapacityPubSubRing, CapacityPubSubSubscriber, PubSubCapacityMorphError,
};
pub use blocking_spsc_ring::{BlockingError, BlockingSpscRing, PhaseRecvStats};
pub use blocking_mpsc_ring::{
    BlockingMpscConsumer, BlockingMpscProducer, BlockingMpscRing,
};
pub use blocking_mpmc_ring::{
    BlockingMpmcConsumer, BlockingMpmcProducer, BlockingMpmcRing,
};
pub use cross_process_waker::{
    CrossProcessWaker, WakerError, WakerToken, MAX_WAITERS_DEFAULT, WAKER_MAGIC,
    waker_region_size,
};
pub use shared_condvar::{CondvarError, SharedCondvar};
pub use async_ring::{AsyncRecv, AsyncSend, AsyncSpscRing};
pub use blocking_semaphore::{
    BlockingPermit, BlockingSemaphore, BlockingSemaphoreError,
};
pub use blocking_rw_lock::{
    BlockingReadGuard, BlockingRWLock, BlockingRWLockError, BlockingWriteGuard,
};
pub use locale_adaptive_ring::{
    DefaultLocalePolicy, Locale, LocaleAdaptiveRing, LocaleAdaptiveRingSidecar,
    LocalePolicy, LocalePolicyObservation, PinnedLocale,
};
pub use shared_rw_lock::{
    ReadGuard, RWLockError, RWLockHeader, SharedRWLock, WriteGuard, RWLOCK_MAGIC,
};
pub use shared_semaphore::{Permit, SemaphoreError, SharedSemaphore};
pub use shared_btree_map::{BTreeError, SharedBTreeMap};
pub use shared_string_arena::{
    arena_file_size, ArenaError, ArenaHeader, SharedStringArena, StringRef,
    ARENA_MAGIC,
};
pub use shared_time_point::{
    tile_file_size, SharedTimePointTile, TileError, TileHeader, VersionedSlot,
    SLOT_PAYLOAD, TILE_CAP, TIME_POINT_MAGIC,
};
pub use shared_topology_map::{
    topology_file_size, SharedTopologyMap, TopologyError, TopologyHeader,
    TopologyKind, TopologyStats, DEFAULT_FAN_IN_THRESHOLD,
    DEFAULT_FAN_OUT_THRESHOLD, TOPOLOGY_MAGIC,
};
pub use shared_treiber_stack::{
    stack_file_size, SharedTreiberStack, StackError, StackHeader,
    STACK_MAGIC, STACK_NIL,
};
pub use shared_umbra_pointer::SharedUmbraPointer;
pub use shared_universal::{
    SharedUniversal, Strategy as UniversalStrategy, UniversalError,
    UniversalHeader, UNIVERSAL_MAGIC,
};
pub use shared_vec::{
    vec_file_size, SharedVec, VecError, VecHeader, VecSlot,
    VEC_MAGIC, VEC_PAYLOAD_BYTES,
};
pub use shared_versioned_chain::{
    versioned_chain_file_size, ChainError, ChainHeader, SharedVersionedChain,
    VersionNode, NIL_NODE, NODE_PAYLOAD_BYTES, VERSIONED_CHAIN_MAGIC,
};
pub use tagged_offset_ptr::{TaggedOffsetPtr, TaggedPtrError};
