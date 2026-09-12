//! The handle table: a 64-bit handle names one live object through an index
//! and a generation, so a stale or forged handle is refused rather than
//! dereferenced.
//!
//! The table grows without bound; there is no capacity to run out of.
//! Readers take a snapshot of the slot vector through an `ArcSwap` and
//! never lock. A slot is one `Arc`, so a snapshot taken before a growth
//! keeps its slots valid after it.
//!
//! A call publishes the slot it is inside, in a word of its own thread's;
//! `crate::epoch` holds that protocol. `destroy` marks the slot closing,
//! interrupts any waiter, and waits out every call that began before it
//! did, so a call in flight on another thread can never see freed memory.
//! A panic caught inside a call poisons the slot: every later call on that
//! handle fails until it is destroyed.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use parking_lot::Mutex;

use crate::error::{
    fail, SUBETHA_E_HANDLE_POISONED, SUBETHA_E_INVALID_HANDLE, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};

/// A handle: zero is never issued.
#[allow(non_camel_case_types)]
pub type subetha_handle = u64;

/// The handle no object has.
pub const SUBETHA_HANDLE_NONE: subetha_handle = 0;

/// The handle names an adaptive ring.
pub const SUBETHA_KIND_RING: u32 = 1;
/// The handle names a blocking single-producer single-consumer ring.
pub const SUBETHA_KIND_SPSC: u32 = 2;
/// The handle names one producer of a blocking MPSC pool.
pub const SUBETHA_KIND_MPSC_PRODUCER: u32 = 3;
/// The handle names the consumer of a blocking MPSC pool.
pub const SUBETHA_KIND_MPSC_CONSUMER: u32 = 4;
/// The handle names one producer of a blocking MPMC grid.
pub const SUBETHA_KIND_MPMC_PRODUCER: u32 = 5;
/// The handle names one consumer of a blocking MPMC grid.
pub const SUBETHA_KIND_MPMC_CONSUMER: u32 = 6;
/// The handle names a Vyukov MPMC ring.
pub const SUBETHA_KIND_VYUKOV: u32 = 7;
/// The handle names the producer side of a Lamport SPSC pair.
pub const SUBETHA_KIND_LAMPORT_PRODUCER: u32 = 8;
/// The handle names the consumer side of a Lamport SPSC pair.
pub const SUBETHA_KIND_LAMPORT_CONSUMER: u32 = 9;
/// The handle names a broadcast ring.
pub const SUBETHA_KIND_BROADCAST: u32 = 10;
/// The handle names a pub/sub ring.
pub const SUBETHA_KIND_PUBSUB: u32 = 11;
/// The handle names a subscriber of a pub/sub ring.
pub const SUBETHA_KIND_SUBSCRIBER: u32 = 12;
/// The handle names a capacity-adaptive ring.
pub const SUBETHA_KIND_CAPACITY_RING: u32 = 13;
/// The handle names a locale-adaptive ring.
pub const SUBETHA_KIND_LOCALE_RING: u32 = 14;
/// The handle names a capacity-adaptive broadcast ring.
pub const SUBETHA_KIND_CAPACITY_BROADCAST: u32 = 15;
/// The handle names a capacity-adaptive pub/sub ring.
pub const SUBETHA_KIND_CAPACITY_PUBSUB: u32 = 16;
/// The handle names a subscriber of a capacity-adaptive pub/sub ring.
pub const SUBETHA_KIND_CAPACITY_SUBSCRIBER: u32 = 17;
/// The handle names an exact-delivery receiver on a stamped ring.
pub const SUBETHA_KIND_ORDERED_RECEIVER: u32 = 18;
/// The handle names a shared Treiber stack.
pub const SUBETHA_KIND_STACK: u32 = 19;
/// The handle names a work-stealing deque, as its owner or as a thief.
pub const SUBETHA_KIND_DEQUE: u32 = 20;
/// The handle names a pollable notifier attached to a ring.
pub const SUBETHA_KIND_NOTIFIER: u32 = 21;
/// The handle names a shared hash map.
pub const SUBETHA_KIND_HASHMAP: u32 = 22;
/// The handle names a shared string arena.
pub const SUBETHA_KIND_ARENA: u32 = 23;
/// The handle names a shared vec.
pub const SUBETHA_KIND_VEC: u32 = 24;
/// The handle names a shared slab.
pub const SUBETHA_KIND_SLAB: u32 = 25;
/// The handle names a shared region.
pub const SUBETHA_KIND_REGION: u32 = 26;
/// The handle names a shared atomic of one of the three widths.
pub const SUBETHA_KIND_ATOMIC: u32 = 27;
/// The handle names a shared doubly-linked list.
pub const SUBETHA_KIND_LIST: u32 = 28;
/// The handle names a shared B-tree map.
pub const SUBETHA_KIND_BTREE: u32 = 29;
/// The handle names a shared cell.
pub const SUBETHA_KIND_CELL: u32 = 30;
/// The handle names a frame region of fixed-size blocks.
pub const SUBETHA_KIND_FRAME_REGION: u32 = 31;
/// The handle names an epoch table.
pub const SUBETHA_KIND_EPOCHS: u32 = 32;
/// The handle names a pin held against an epoch table.
/// Retired: a pin is a token now, not a handle. The number is not
/// reused, so an old pin handle is refused rather than resolved.
pub const SUBETHA_KIND_PIN: u32 = 33;
/// The handle names an open ticket on an epoch table.
/// Retired, like `SUBETHA_KIND_PIN`: a ticket is a token now. The number
/// is not reused.
pub const SUBETHA_KIND_TICKET: u32 = 34;
/// The handle names a reader-writer lock.
pub const SUBETHA_KIND_RWLOCK: u32 = 35;
/// Retired. A hold on a reader-writer lock is a token now, not a handle,
/// so no object has this kind. The number is not reused: a caller that
/// still passes an old hold handle should be refused rather than
/// resolved to whatever took the number.
pub const SUBETHA_KIND_LOCK_HOLD: u32 = 36;
/// The handle names a counting semaphore.
pub const SUBETHA_KIND_SEMAPHORE: u32 = 37;
/// Retired, like `SUBETHA_KIND_LOCK_HOLD`: a permit is a token now, not
/// a handle. The number is not reused, so an old permit handle is
/// refused rather than resolved to whatever took the number.
pub const SUBETHA_KIND_PERMIT: u32 = 38;
/// The handle names a condition variable.
pub const SUBETHA_KIND_CONDVAR: u32 = 39;
/// The handle names an owner lease.
pub const SUBETHA_KIND_OWNER_LEASE: u32 = 40;
/// The handle names a leader election.
pub const SUBETHA_KIND_LEADER: u32 = 41;
/// The handle names a heartbeat table.
pub const SUBETHA_KIND_HEARTBEAT: u32 = 42;
/// The handle names a holder table.
pub const SUBETHA_KIND_HOLDERS: u32 = 43;
/// The handle names a shared fence clock.
pub const SUBETHA_KIND_FENCE_CLOCK: u32 = 44;
/// The handle names an epoch barrier.
pub const SUBETHA_KIND_EPOCH_BARRIER: u32 = 45;
/// The handle names a shared value kept alive by its holders.
pub const SUBETHA_KIND_SHARED_ARC: u32 = 46;
/// The handle names a value produced once across every process.
pub const SUBETHA_KIND_LAZY_VALUE: u32 = 47;
/// The handle names a cross-process waker.
pub const SUBETHA_KIND_WAKER: u32 = 48;
/// The handle names one half of a TCP bridge.
pub const SUBETHA_KIND_TCP_BRIDGE: u32 = 49;
/// The handle names one half of a QUIC bridge.
pub const SUBETHA_KIND_QUIC_BRIDGE: u32 = 50;
/// The handle names one half of a Sens-O-Matic link.
pub const SUBETHA_KIND_SENS: u32 = 51;
/// The handle names a shared Bloom filter.
pub const SUBETHA_KIND_BLOOM: u32 = 52;
/// The handle names a shared count-min sketch.
pub const SUBETHA_KIND_CMS: u32 = 53;
/// The handle names a shared histogram.
pub const SUBETHA_KIND_HISTOGRAM: u32 = 54;
/// The handle names a shared rate limiter.
pub const SUBETHA_KIND_RATE_LIMITER: u32 = 55;
/// The handle names a shared blocked Bloom filter.
pub const SUBETHA_KIND_BLOCKED_BLOOM: u32 = 56;
/// The handle names a shared reservoir sampler.
pub const SUBETHA_KIND_RESERVOIR: u32 = 57;
/// The handle names a shared topology map.
pub const SUBETHA_KIND_TOPOLOGY: u32 = 58;
/// The handle names a shared versioned chain.
pub const SUBETHA_KIND_VERSIONED_CHAIN: u32 = 59;
/// The handle names a shared versioned slab.
pub const SUBETHA_KIND_VERSIONED_SLAB: u32 = 60;
/// The handle names a shared versioned map.
pub const SUBETHA_KIND_VERSIONED_MAP: u32 = 61;
/// The handle names a shared laned versioned map.
pub const SUBETHA_KIND_LANED_MAP: u32 = 62;
/// The handle names a virtual endpoint registry.
pub const SUBETHA_KIND_ENDPOINT_REGISTRY: u32 = 63;
/// The handle names a runtime-mutable quality-of-service policy.
pub const SUBETHA_KIND_QOS_POLICY: u32 = 64;
/// The handle names a shared bit vector.
pub const SUBETHA_KIND_BIT_VEC: u32 = 65;
/// The handle names a shared HyperLogLog.
pub const SUBETHA_KIND_HLL: u32 = 66;
/// The handle names a least-recently-used cache.
pub const SUBETHA_KIND_LRU_CACHE: u32 = 67;
/// The handle names a directed graph.
pub const SUBETHA_KIND_GRAPH: u32 = 68;
/// The handle names a versioned time-point tile.
pub const SUBETHA_KIND_TIME_POINT: u32 = 69;
/// The handle names a strategy-switching set.
pub const SUBETHA_KIND_UNIVERSAL: u32 = 70;
/// The handle names a cascade tower.
pub const SUBETHA_KIND_K_TOWER: u32 = 71;
/// The handle names a blocking bridge: a blocking SPSC ring carried
/// across a socket connection.
pub const SUBETHA_KIND_BLOCKING_TCP_BRIDGE: u32 = 72;

const STATE_FREE: u8 = 0;
const STATE_LIVE: u8 = 1;
const STATE_POISONED: u8 = 2;
const STATE_CLOSING: u8 = 3;

/// Everything a handle can name.
pub(crate) enum Object {
    Ring(crate::ring::RingObject),
    Spsc(crate::spsc::SpscObject),
    MpscProducer(crate::mpsc::MpscProducerObject),
    MpscConsumer(crate::mpsc::MpscConsumerObject),
    MpmcProducer(crate::mpmc::MpmcProducerObject),
    MpmcConsumer(crate::mpmc::MpmcConsumerObject),
    Vyukov(crate::vyukov::VyukovObject),
    LamportProducer(crate::lamport::LamportProducerObject),
    LamportConsumer(crate::lamport::LamportConsumerObject),
    Broadcast(crate::broadcast::BroadcastObject),
    PubSub(crate::pubsub::PubSubObject),
    Subscriber(crate::pubsub::SubscriberObject),
    Capacity(crate::capacity::CapacityObject),
    LocaleRing(crate::locale::LocaleObject),
    EndpointRegistry(crate::virtual_endpoint::EndpointRegistryObject),
    QosPolicy(crate::qos_policy::QosPolicyObject),
    Lru(crate::lru_cache::LruObject),
    Graph(crate::graph::GraphObject),
    Tile(crate::time_point::TileObject),
    Universal(crate::universal::UniversalObject),
    Tower(crate::k_tower::TowerObject),
    BitVec(crate::bit_vec::BitVecObject),
    Hll(crate::hyper_log_log::HllObject),
    CapacityBroadcast(crate::capacity_broadcast::CapacityBroadcastObject),
    CapacityPubSub(crate::capacity_pubsub::CapacityPubSubObject),
    CapacitySubscriber(crate::capacity_pubsub::CapacitySubscriberObject),
    Ordered(crate::ordered::OrderedObject),
    Stack(crate::stack::StackObject),
    Deque(crate::deque::DequeObject),
    Notifier(crate::notifier::NotifierObject),
    HashMap(crate::hashmap::HashMapObject),
    Arena(crate::arena::ArenaObject),
    Vec(crate::vec::VecObject),
    Slab(crate::slab::SlabObject),
    Region(crate::region::RegionObject),
    Atomic(crate::atomic::AtomicObject),
    List(crate::list::ListObject),
    BTree(crate::btree::BTreeObject),
    Cell(crate::cell::CellObject),
    FrameRegion(crate::frame_region::FrameRegionObject),
    Epochs(crate::epochs::EpochsObject),
    RwLock(crate::rwlock::RwLockObject),
    Semaphore(crate::semaphore::SemaphoreObject),
    Condvar(crate::condvar::CondvarObject),
    OwnerLease(crate::owner_lease::OwnerLeaseObject),
    Leader(crate::leader::LeaderObject),
    Heartbeat(crate::heartbeat::HeartbeatObject),
    Holders(crate::holders::HoldersObject),
    FenceClock(crate::fence_clock::FenceClockObject),
    EpochBarrier(crate::epoch_barrier::EpochBarrierObject),
    SharedArc(crate::shared_arc::SharedArcObject),
    LazyValue(crate::lazy_value::LazyValueObject),
    Waker(crate::waker::WakerObject),
    /// In a build without the transports the type this carries holds an
    /// `Infallible`, so the variant cannot be constructed. It stays in the
    /// enum regardless, so `kind` and the borrow path have one shape in
    /// every build and a handle means the same thing either way.
    #[cfg_attr(not(feature = "tcp-bridge"), allow(dead_code))]
    TcpBridge(crate::tcp_bridge::TcpBridgeObject),
    /// As with `TcpBridge`, unconstructable without its feature and
    /// present in every build so a handle means the same thing either way.
    #[cfg_attr(not(feature = "quic-bridge"), allow(dead_code))]
    QuicBridge(crate::quic_bridge::QuicBridgeObject),
    /// As with `TcpBridge`: the blocking bridge rides the same feature and
    /// is unconstructable without it.
    #[cfg_attr(not(feature = "tcp-bridge"), allow(dead_code))]
    BlockingTcpBridge(crate::blocking_tcp_bridge::BlockingTcpBridgeObject),
    Sens(crate::sens::SensObject),
    Bloom(crate::bloom::BloomObject),
    Cms(crate::cms::CmsObject),
    Histogram(crate::histogram::HistogramObject),
    RateLimiter(crate::rate_limiter::RateLimiterObject),
    BlockedBloom(crate::blocked_bloom::BlockedBloomObject),
    Reservoir(crate::reservoir::ReservoirObject),
    Topology(crate::topology::TopologyObject),
    VersionedChain(crate::versioned_chain::VersionedChainObject),
    VersionedSlab(crate::versioned_slab::VersionedSlabObject),
    VersionedMap(crate::versioned_map::VersionedMapObject),
    LanedMap(crate::laned_map::LanedMapObject),
}

impl Object {
    pub(crate) fn kind(&self) -> u32 {
        match self {
            Object::Ring(_) => SUBETHA_KIND_RING,
            Object::Spsc(_) => SUBETHA_KIND_SPSC,
            Object::MpscProducer(_) => SUBETHA_KIND_MPSC_PRODUCER,
            Object::MpscConsumer(_) => SUBETHA_KIND_MPSC_CONSUMER,
            Object::MpmcProducer(_) => SUBETHA_KIND_MPMC_PRODUCER,
            Object::MpmcConsumer(_) => SUBETHA_KIND_MPMC_CONSUMER,
            Object::Vyukov(_) => SUBETHA_KIND_VYUKOV,
            Object::LamportProducer(_) => SUBETHA_KIND_LAMPORT_PRODUCER,
            Object::LamportConsumer(_) => SUBETHA_KIND_LAMPORT_CONSUMER,
            Object::Broadcast(_) => SUBETHA_KIND_BROADCAST,
            Object::PubSub(_) => SUBETHA_KIND_PUBSUB,
            Object::Subscriber(_) => SUBETHA_KIND_SUBSCRIBER,
            Object::Capacity(_) => SUBETHA_KIND_CAPACITY_RING,
            Object::LocaleRing(_) => SUBETHA_KIND_LOCALE_RING,
            Object::EndpointRegistry(_) => SUBETHA_KIND_ENDPOINT_REGISTRY,
            Object::QosPolicy(_) => SUBETHA_KIND_QOS_POLICY,
            Object::Lru(_) => SUBETHA_KIND_LRU_CACHE,
            Object::Graph(_) => SUBETHA_KIND_GRAPH,
            Object::Tile(_) => SUBETHA_KIND_TIME_POINT,
            Object::Universal(_) => SUBETHA_KIND_UNIVERSAL,
            Object::Tower(_) => SUBETHA_KIND_K_TOWER,
            Object::BitVec(_) => SUBETHA_KIND_BIT_VEC,
            Object::Hll(_) => SUBETHA_KIND_HLL,
            Object::CapacityBroadcast(_) => SUBETHA_KIND_CAPACITY_BROADCAST,
            Object::CapacityPubSub(_) => SUBETHA_KIND_CAPACITY_PUBSUB,
            Object::CapacitySubscriber(_) => SUBETHA_KIND_CAPACITY_SUBSCRIBER,
            Object::Ordered(_) => SUBETHA_KIND_ORDERED_RECEIVER,
            Object::Stack(_) => SUBETHA_KIND_STACK,
            Object::Deque(_) => SUBETHA_KIND_DEQUE,
            Object::Notifier(_) => SUBETHA_KIND_NOTIFIER,
            Object::HashMap(_) => SUBETHA_KIND_HASHMAP,
            Object::Arena(_) => SUBETHA_KIND_ARENA,
            Object::Vec(_) => SUBETHA_KIND_VEC,
            Object::Slab(_) => SUBETHA_KIND_SLAB,
            Object::Region(_) => SUBETHA_KIND_REGION,
            Object::Atomic(_) => SUBETHA_KIND_ATOMIC,
            Object::List(_) => SUBETHA_KIND_LIST,
            Object::BTree(_) => SUBETHA_KIND_BTREE,
            Object::Cell(_) => SUBETHA_KIND_CELL,
            Object::FrameRegion(_) => SUBETHA_KIND_FRAME_REGION,
            Object::Epochs(_) => SUBETHA_KIND_EPOCHS,
            Object::RwLock(_) => SUBETHA_KIND_RWLOCK,
            Object::Semaphore(_) => SUBETHA_KIND_SEMAPHORE,
            Object::Condvar(_) => SUBETHA_KIND_CONDVAR,
            Object::OwnerLease(_) => SUBETHA_KIND_OWNER_LEASE,
            Object::Leader(_) => SUBETHA_KIND_LEADER,
            Object::Heartbeat(_) => SUBETHA_KIND_HEARTBEAT,
            Object::Holders(_) => SUBETHA_KIND_HOLDERS,
            Object::FenceClock(_) => SUBETHA_KIND_FENCE_CLOCK,
            Object::EpochBarrier(_) => SUBETHA_KIND_EPOCH_BARRIER,
            Object::SharedArc(_) => SUBETHA_KIND_SHARED_ARC,
            Object::LazyValue(_) => SUBETHA_KIND_LAZY_VALUE,
            Object::Waker(_) => SUBETHA_KIND_WAKER,
            Object::TcpBridge(_) => SUBETHA_KIND_TCP_BRIDGE,
            Object::QuicBridge(_) => SUBETHA_KIND_QUIC_BRIDGE,
            Object::BlockingTcpBridge(_) => SUBETHA_KIND_BLOCKING_TCP_BRIDGE,
            Object::Sens(_) => SUBETHA_KIND_SENS,
            Object::Bloom(_) => SUBETHA_KIND_BLOOM,
            Object::Cms(_) => SUBETHA_KIND_CMS,
            Object::Histogram(_) => SUBETHA_KIND_HISTOGRAM,
            Object::RateLimiter(_) => SUBETHA_KIND_RATE_LIMITER,
            Object::BlockedBloom(_) => SUBETHA_KIND_BLOCKED_BLOOM,
            Object::Reservoir(_) => SUBETHA_KIND_RESERVOIR,
            Object::Topology(_) => SUBETHA_KIND_TOPOLOGY,
            Object::VersionedChain(_) => SUBETHA_KIND_VERSIONED_CHAIN,
            Object::VersionedSlab(_) => SUBETHA_KIND_VERSIONED_SLAB,
            Object::VersionedMap(_) => SUBETHA_KIND_VERSIONED_MAP,
            Object::LanedMap(_) => SUBETHA_KIND_LANED_MAP,
        }
    }

    /// Wake every thread parked inside a call on this object, so a destroy
    /// can wait for them to leave.
    fn interrupt(&self) {
        match self {
            Object::Ring(r) => r.interrupt(),
            Object::Spsc(s) => s.interrupt(),
            Object::MpscProducer(p) => p.interrupt(),
            Object::MpscConsumer(c) => c.interrupt(),
            Object::MpmcProducer(p) => p.interrupt(),
            Object::MpmcConsumer(c) => c.interrupt(),
            Object::Vyukov(v) => v.interrupt(),
            Object::LamportProducer(p) => p.interrupt(),
            Object::LamportConsumer(c) => c.interrupt(),
            Object::Broadcast(b) => b.interrupt(),
            Object::PubSub(p) => p.interrupt(),
            Object::Subscriber(s) => s.interrupt(),
            Object::Capacity(c) => c.interrupt(),
            Object::LocaleRing(l) => l.interrupt(),
            Object::EndpointRegistry(r) => r.interrupt(),
            Object::QosPolicy(q) => q.interrupt(),
            Object::Lru(c) => c.interrupt(),
            Object::Graph(g) => g.interrupt(),
            Object::Tile(t) => t.interrupt(),
            Object::Universal(u) => u.interrupt(),
            Object::Tower(t) => t.interrupt(),
            Object::BitVec(b) => b.interrupt(),
            Object::Hll(h) => h.interrupt(),
            Object::CapacityBroadcast(b) => b.interrupt(),
            Object::CapacityPubSub(p) => p.interrupt(),
            Object::CapacitySubscriber(s) => s.interrupt(),
            Object::Ordered(o) => o.interrupt(),
            Object::Stack(s) => s.interrupt(),
            Object::Deque(d) => d.interrupt(),
            Object::Notifier(n) => n.interrupt(),
            Object::HashMap(m) => m.interrupt(),
            Object::Arena(a) => a.interrupt(),
            Object::Vec(v) => v.interrupt(),
            Object::Slab(s) => s.interrupt(),
            Object::Region(r) => r.interrupt(),
            Object::Atomic(a) => a.interrupt(),
            Object::List(l) => l.interrupt(),
            Object::BTree(b) => b.interrupt(),
            Object::Cell(c) => c.interrupt(),
            Object::FrameRegion(r) => r.interrupt(),
            Object::Epochs(e) => e.interrupt(),
            Object::RwLock(l) => l.interrupt(),
            Object::Semaphore(s) => s.interrupt(),
            Object::Condvar(c) => c.interrupt(),
            Object::OwnerLease(l) => l.interrupt(),
            Object::Leader(l) => l.interrupt(),
            Object::Heartbeat(h) => h.interrupt(),
            Object::Holders(h) => h.interrupt(),
            Object::FenceClock(c) => c.interrupt(),
            Object::EpochBarrier(b) => b.interrupt(),
            Object::SharedArc(a) => a.interrupt(),
            Object::LazyValue(l) => l.interrupt(),
            Object::Waker(w) => w.interrupt(),
            Object::TcpBridge(b) => b.interrupt(),
            Object::QuicBridge(b) => b.interrupt(),
            Object::BlockingTcpBridge(b) => b.interrupt(),
            Object::Sens(s) => s.interrupt(),
            Object::Bloom(b) => b.interrupt(),
            Object::Cms(c) => c.interrupt(),
            Object::Histogram(h) => h.interrupt(),
            Object::RateLimiter(l) => l.interrupt(),
            Object::BlockedBloom(b) => b.interrupt(),
            Object::Reservoir(r) => r.interrupt(),
            Object::Topology(t) => t.interrupt(),
            Object::VersionedChain(c) => c.interrupt(),
            Object::VersionedSlab(s) => s.interrupt(),
            Object::VersionedMap(m) => m.interrupt(),
            Object::LanedMap(m) => m.interrupt(),
        }
    }
}

pub(crate) struct Slot {
    generation: AtomicU32,
    state: AtomicU8,
    object: ArcSwapOption<Object>,
    panic_message: Mutex<Option<String>>,
}

impl Slot {
    fn new() -> Self {
        Self {
            generation: AtomicU32::new(1),
            state: AtomicU8::new(STATE_FREE),
            object: ArcSwapOption::from(None),
            panic_message: Mutex::new(None),
        }
    }
}

/// A live object borrowed for the duration of one call. Dropping it lets a
/// pending destroy proceed.
///
/// Both pointers are raised out of the table rather than cloned, which is
/// what keeps a call off the reference counts:
///
/// - A slot, once in the table, is never dropped. Growth clones every
///   `Arc<Slot>` into the longer vector and a destroy only recycles the
///   index, so the address stays live for as long as the table does, which
///   is the process.
/// - The object is held by the call this thread published before it read
///   the slot's state. A destroy marks the slot closing, waits out every
///   call that began before that, and swaps the object out only
///   afterwards, so a borrow that published and then read the state as
///   live holds an object no destroy can take away underneath it.
pub(crate) struct Borrowed {
    slot: *const Slot,
    object: *const Object,
    /// Published for as long as this borrow lives, and cleared when it
    /// drops. Declared last so it is cleared after the pointers are done
    /// with.
    _inside: crate::epoch::Inside,
}

impl std::fmt::Debug for Borrowed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Borrowed(kind {})", self.object().kind())
    }
}

impl Borrowed {
    pub(crate) fn object(&self) -> &Object {
        // SAFETY: the slot this borrow published keeps the object in place
        // for the borrow's lifetime.
        unsafe { &*self.object }
    }

    fn slot(&self) -> &Slot {
        // SAFETY: a slot in the table outlives every borrow of it.
        unsafe { &*self.slot }
    }

    /// Mark the slot poisoned and keep the panic's text: every later call on
    /// the handle fails until it is destroyed.
    pub(crate) fn poison(&self, message: String) {
        self.slot().state.store(STATE_POISONED, Ordering::Release);
        *self.slot().panic_message.lock() = Some(message);
    }
}

pub(crate) struct Table {
    slots: ArcSwap<Vec<Arc<Slot>>>,
    free: Mutex<Vec<u32>>,
    live: AtomicU64,
}

fn encode(index: u32, generation: u32) -> subetha_handle {
    (u64::from(index) << 32) | u64::from(generation)
}

fn decode(handle: subetha_handle) -> (u32, u32) {
    ((handle >> 32) as u32, handle as u32)
}

impl Table {
    pub(crate) fn new() -> Self {
        Self {
            slots: ArcSwap::from_pointee(Vec::new()),
            free: Mutex::new(Vec::new()),
            live: AtomicU64::new(0),
        }
    }

    /// Objects currently held by a handle, poisoned ones included.
    pub(crate) fn live(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// Place an object and issue its handle.
    pub(crate) fn insert(&self, object: Object) -> subetha_handle {
        let mut free = self.free.lock();
        let index = match free.pop() {
            Some(i) => i,
            None => {
                // Grow by doubling under the free-list lock, publishing the
                // longer vector before any new index is handed out.
                let current = self.slots.load_full();
                let old_len = current.len();
                let new_len = old_len.max(1) * 2;
                let mut grown = Vec::with_capacity(new_len);
                grown.extend(current.iter().cloned());
                for _ in old_len..new_len {
                    grown.push(Arc::new(Slot::new()));
                }
                self.slots.store(Arc::new(grown));
                for i in (old_len + 1..new_len).rev() {
                    free.push(i as u32);
                }
                old_len as u32
            }
        };
        let slots = self.slots.load();
        let slot = &slots[index as usize];
        let generation = slot.generation.load(Ordering::Acquire);
        slot.object.store(Some(Arc::new(object)));
        *slot.panic_message.lock() = None;
        slot.state.store(STATE_LIVE, Ordering::Release);
        self.live.fetch_add(1, Ordering::AcqRel);
        encode(index, generation)
    }

    /// Borrow the object a handle names for one call. Refuses a handle that
    /// was never issued, has been destroyed, is being destroyed, or is
    /// poisoned; refuses a live handle of another kind.
    pub(crate) fn borrow(&self, handle: subetha_handle, kind: u32) -> Result<Borrowed, i32> {
        if crate::holds::is_token(handle) {
            return Err(fail(
                SUBETHA_E_INVALID_HANDLE,
                format!("handle {handle:#x}: that is a hold token, not a handle"),
            ));
        }
        let (index, generation) = decode(handle);
        let slots = self.slots.load();
        let Some(slot) = slots.get(index as usize) else {
            return Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: no such slot")));
        };
        if slot.generation.load(Ordering::Acquire) != generation {
            return Err(fail(
                SUBETHA_E_INVALID_HANDLE,
                format!("handle {handle:#x}: its object was destroyed"),
            ));
        }
        // Published before the state is read, so a destroy either waits for
        // this call or this call sees the slot closing.
        let inside = crate::epoch::enter(index)?;
        let admitted = match slot.state.load(Ordering::Acquire) {
            STATE_LIVE => Ok(()),
            STATE_POISONED => Err(fail(
                SUBETHA_E_HANDLE_POISONED,
                match slot.panic_message.lock().as_deref() {
                    Some(m) => format!("handle {handle:#x} poisoned by: {m}"),
                    None => format!("handle {handle:#x} poisoned"),
                },
            )),
            _ => Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: not live"))),
        };
        let object = admitted.and_then(|()| {
            // A destroy that raced this borrow has bumped the generation;
            // a borrow that came in after that must not run.
            if slot.generation.load(Ordering::Acquire) != generation {
                return Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: destroyed")));
            }
            // The address, not a clone of the reference count: the slot
            // published above is what holds the object for this call.
            match slot.object.load().as_deref() {
                Some(o) => Ok(o as *const Object),
                None => Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: empty"))),
            }
        });
        match object {
            Ok(object) => {
                // SAFETY: this call is published and the slot was read as
                // live, so the object stays in place for this borrow.
                let kind_of = unsafe { (*object).kind() };
                if kind_of != kind {
                    return Err(fail(
                        SUBETHA_E_WRONG_KIND,
                        format!("handle {handle:#x} is kind {kind_of}, not {kind}"),
                    ));
                }
                Ok(Borrowed { slot: Arc::as_ptr(slot), object, _inside: inside })
            }
            Err(code) => Err(code),
        }
    }

    /// The kind a live or poisoned handle names.
    pub(crate) fn kind_of(&self, handle: subetha_handle) -> Result<u32, i32> {
        let (index, generation) = decode(handle);
        let slots = self.slots.load();
        let Some(slot) = slots.get(index as usize) else {
            return Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: no such slot")));
        };
        if slot.generation.load(Ordering::Acquire) != generation {
            return Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: destroyed")));
        }
        match slot.state.load(Ordering::Acquire) {
            STATE_LIVE | STATE_POISONED => match slot.object.load_full() {
                Some(o) => Ok(o.kind()),
                None => Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: empty"))),
            },
            _ => Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: not live"))),
        }
    }

    /// Whether a live handle is poisoned.
    pub(crate) fn is_poisoned(&self, handle: subetha_handle) -> Result<bool, i32> {
        let (index, generation) = decode(handle);
        let slots = self.slots.load();
        let Some(slot) = slots.get(index as usize) else {
            return Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: no such slot")));
        };
        if slot.generation.load(Ordering::Acquire) != generation {
            return Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: destroyed")));
        }
        match slot.state.load(Ordering::Acquire) {
            STATE_LIVE => Ok(false),
            STATE_POISONED => Ok(true),
            _ => Err(fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: not live"))),
        }
    }

    /// Release a handle: refuse new calls, interrupt any waiter, wait out
    /// every call that began before the refusal, drop the object, and
    /// retire the generation so the handle can never name anything again.
    /// Poisoned handles are destroyed the same way; that is how they are
    /// recovered.
    pub(crate) fn destroy(&self, handle: subetha_handle) -> i32 {
        if crate::holds::is_token(handle) {
            return fail(
                SUBETHA_E_INVALID_HANDLE,
                format!("handle {handle:#x}: that is a hold token, not a handle - release it on the object that issued it"),
            );
        }
        let (index, generation) = decode(handle);
        let slots = self.slots.load();
        let Some(slot) = slots.get(index as usize) else {
            return fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: no such slot"));
        };
        if slot.generation.load(Ordering::Acquire) != generation {
            return fail(SUBETHA_E_INVALID_HANDLE, format!("handle {handle:#x}: already destroyed"));
        }
        let claimed = slot
            .state
            .compare_exchange(STATE_LIVE, STATE_CLOSING, Ordering::AcqRel, Ordering::Acquire)
            .or_else(|_| {
                slot.state.compare_exchange(
                    STATE_POISONED,
                    STATE_CLOSING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
            });
        if claimed.is_err() {
            return fail(
                SUBETHA_E_INVALID_HANDLE,
                format!("handle {handle:#x}: not live, or being destroyed by another thread"),
            );
        }
        if let Some(object) = slot.object.load_full() {
            object.interrupt();
        }
        // The slot is closing, so a call that begins from here on is
        // refused; this waits out the ones that began before it was.
        crate::epoch::wait_for_calls_on(index);
        // No call can start and none is inside, so this is the last
        // reference: dropping it runs the object's own cleanup, including
        // joining any thread it owns.
        let object = slot.object.swap(None);
        drop(object);
        *slot.panic_message.lock() = None;
        let next = match generation.wrapping_add(1) {
            0 => 1,
            g => g,
        };
        slot.generation.store(next, Ordering::Release);
        slot.state.store(STATE_FREE, Ordering::Release);
        self.live.fetch_sub(1, Ordering::AcqRel);
        self.free.lock().push(index);
        SUBETHA_OK
    }

    /// Destroy every handle still held. Returns how many there were.
    pub(crate) fn destroy_all(&self) -> u64 {
        let slots = self.slots.load_full();
        let mut closed = 0u64;
        for (index, slot) in slots.iter().enumerate() {
            let state = slot.state.load(Ordering::Acquire);
            if state == STATE_LIVE || state == STATE_POISONED {
                let handle = encode(index as u32, slot.generation.load(Ordering::Acquire));
                if self.destroy(handle) == SUBETHA_OK {
                    closed += 1;
                }
            }
        }
        closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring() -> Object {
        Object::Ring(crate::ring::RingObject::anon_for_tests())
    }

    #[test]
    fn a_handle_is_refused_after_destroy_and_its_slot_is_reissued_with_a_new_generation() {
        let table = Table::new();
        let a = table.insert(ring());
        assert!(table.borrow(a, SUBETHA_KIND_RING).is_ok());
        assert_eq!(table.destroy(a), SUBETHA_OK);
        assert_eq!(table.borrow(a, SUBETHA_KIND_RING).unwrap_err(), SUBETHA_E_INVALID_HANDLE);
        assert_eq!(table.destroy(a), SUBETHA_E_INVALID_HANDLE, "a second destroy is refused");
        let b = table.insert(ring());
        assert_eq!(decode(b).0, decode(a).0, "the slot is reused");
        assert_ne!(decode(b).1, decode(a).1, "under a new generation");
        assert!(table.borrow(a, SUBETHA_KIND_RING).is_err(), "the old handle still names nothing");
        assert!(table.borrow(b, SUBETHA_KIND_RING).is_ok());
        assert_eq!(table.destroy(b), SUBETHA_OK);
    }

    #[test]
    fn zero_and_forged_handles_name_nothing() {
        let table = Table::new();
        assert_eq!(table.borrow(SUBETHA_HANDLE_NONE, SUBETHA_KIND_RING).unwrap_err(), SUBETHA_E_INVALID_HANDLE);
        assert_eq!(table.borrow(u64::MAX, SUBETHA_KIND_RING).unwrap_err(), SUBETHA_E_INVALID_HANDLE);
        let a = table.insert(ring());
        let forged = encode(decode(a).0, decode(a).1.wrapping_add(7));
        assert_eq!(table.borrow(forged, SUBETHA_KIND_RING).unwrap_err(), SUBETHA_E_INVALID_HANDLE);
        assert_eq!(table.destroy(a), SUBETHA_OK);
    }

    #[test]
    fn a_poisoned_handle_refuses_calls_until_it_is_destroyed() {
        let table = Table::new();
        let a = table.insert(ring());
        table.borrow(a, SUBETHA_KIND_RING).unwrap().poison("boom".into());
        assert_eq!(table.borrow(a, SUBETHA_KIND_RING).unwrap_err(), SUBETHA_E_HANDLE_POISONED);
        assert!(table.is_poisoned(a).unwrap());
        assert_eq!(table.kind_of(a).unwrap(), SUBETHA_KIND_RING, "the kind is still readable");
        assert_eq!(table.destroy(a), SUBETHA_OK);
        assert_eq!(table.live(), 0);
    }

    #[test]
    fn the_table_grows_without_a_ceiling_and_every_handle_stays_distinct() {
        let table = Table::new();
        let handles: Vec<_> = (0..1000).map(|_| table.insert(ring())).collect();
        let mut sorted = handles.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), handles.len());
        assert_eq!(table.live(), 1000);
        for h in &handles {
            assert!(table.borrow(*h, SUBETHA_KIND_RING).is_ok());
        }
        assert_eq!(table.destroy_all(), 1000);
        assert_eq!(table.live(), 0);
    }

    #[test]
    fn destroy_waits_for_a_call_in_flight_and_that_call_sees_the_object_alive() {
        let table = Arc::new(Table::new());
        let a = table.insert(ring());
        let borrowed = table.borrow(a, SUBETHA_KIND_RING).unwrap();
        let t = {
            let table = Arc::clone(&table);
            std::thread::spawn(move || table.destroy(a))
        };
        // The destroyer is waiting out this borrow; the object must still
        // be reachable through it.
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(!t.is_finished(), "destroy must not complete while a borrow is live");
        assert_eq!(borrowed.object().kind(), SUBETHA_KIND_RING);
        drop(borrowed);
        assert_eq!(t.join().unwrap(), SUBETHA_OK);
    }
}
