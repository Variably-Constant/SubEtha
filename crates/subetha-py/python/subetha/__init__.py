"""Shared memory between processes: rings, channels and shared state.

Every name here comes from the compiled extension beside this file. The
package exists so the extension can be shipped with type stubs and a
py.typed marker, which a bare extension module cannot carry.

What a caller should know before reaching for any of it:

- One call from Python into this library costs about thirty nanoseconds,
  against seven for the same operation reached from C and six hundred
  through a C shim. The batched forms (`push_many`, `send_buffer`,
  `insert_many` and their kin) carry a run of items across one crossing
  and cost about one nanosecond an item; a `Region`'s buffer, read whole,
  costs a fraction of one per byte. A per-item Python loop is the shape
  to avoid.
- Full and empty are answers rather than failures: a push that does not
  fit returns False and a pop with nothing to take returns None. Only a
  genuine fault raises.
- Anything holding a resource is a context manager, and gives the
  resource back when its block ends, including on the way out of an
  exception.
- Most of this shares memory between processes on one machine. The
  `Sens` pair reaches another machine, and the bridges carry a whole
  ring there. The bridges are optional, so `transports` says which ones
  this build has.
"""

from . import _subetha
from ._subetha import (
    Arena,
    Atomic,
    BTreeMap,
    BitVec,
    BlockedBloomFilter,
    BloomFilter,
    BroadcastRing,
    CapacityRing,
    Cell,
    Channel,
    Condvar,
    Contended,
    CountMinSketch,
    Deque,
    EpochBarrier,
    Epochs,
    FenceClock,
    FrameRegion,
    Graph,
    HandleTable,
    HashMap,
    Heartbeat,
    Histogram,
    Hold,
    HolderTable,
    HyperLogLog,
    KvMap,
    Lagged,
    LamportConsumer,
    LamportProducer,
    LaneClaim,
    LanedMap,
    LanedPin,
    LazyValue,
    LeaderElection,
    LeaseHold,
    LinkedList,
    LocaleRing,
    LruCache,
    MapPin,
    MpmcConsumer,
    MpmcProducer,
    MpscConsumer,
    MpscProducer,
    Notifier,
    NotifierSet,
    OrderedReceiver,
    OwnerLease,
    PermitHold,
    PubSub,
    QosPolicy,
    QosSnapshot,
    RWLock,
    RateLimiter,
    Region,
    ReorderWindow,
    Reservoir,
    Ring,
    Semaphore,
    SensReceiver,
    SensSender,
    SharedArc,
    Slab,
    SpscRing,
    Stack,
    SlabPin,
    Subscriber,
    TimePointTile,
    TopologyMap,
    Tower,
    Universal,
    Vec,
    VersionChain,
    VersionedMap,
    VersionedSlab,
    WorkQueue,
    WrongLane,
    boundary_note,
    free_threaded,
    transports,
    lamport_pair,
    lamport_pair_open,
    mpmc_grid,
    mpmc_grid_open,
    mpsc_pool,
    mpsc_pool_open,
)

__all__ = [
    "Arena",
    "Atomic",
    "BTreeMap",
    "BitVec",
    "BlockedBloomFilter",
    "BloomFilter",
    "BroadcastRing",
    "CapacityRing",
    "Cell",
    "Channel",
    "Condvar",
    "Contended",
    "CountMinSketch",
    "Deque",
    "EpochBarrier",
    "Epochs",
    "FenceClock",
    "FrameRegion",
    "Graph",
    "HandleTable",
    "HashMap",
    "Heartbeat",
    "Histogram",
    "Hold",
    "HolderTable",
    "HyperLogLog",
    "KvMap",
    "Lagged",
    "LamportConsumer",
    "LamportProducer",
    "LaneClaim",
    "LanedMap",
    "LanedPin",
    "LazyValue",
    "LeaderElection",
    "LeaseHold",
    "LinkedList",
    "LocaleRing",
    "LruCache",
    "MapPin",
    "MpmcConsumer",
    "MpmcProducer",
    "MpscConsumer",
    "MpscProducer",
    "Notifier",
    "NotifierSet",
    "OrderedReceiver",
    "OwnerLease",
    "PermitHold",
    "PubSub",
    "QosPolicy",
    "QosSnapshot",
    "RWLock",
    "RateLimiter",
    "Region",
    "ReorderWindow",
    "Reservoir",
    "Ring",
    "Semaphore",
    "SensReceiver",
    "SensSender",
    "SharedArc",
    "Slab",
    "SpscRing",
    "Stack",
    "SlabPin",
    "Subscriber",
    "TimePointTile",
    "TopologyMap",
    "Tower",
    "Universal",
    "Vec",
    "VersionChain",
    "VersionedMap",
    "VersionedSlab",
    "WorkQueue",
    "WrongLane",
    "boundary_note",
    "free_threaded",
    "transports",
    "lamport_pair",
    "lamport_pair_open",
    "mpmc_grid",
    "mpmc_grid_open",
    "mpsc_pool",
    "mpsc_pool_open",
]

# The classes each optional transport brings with it. The Sens-O-Matic
# link is always built, so it is not here; the bridges each carry a
# network stack and are left out of the default wheel, which is why a
# caller has to be able to tell a wheel built without one from a name
# that never existed. `transports` says which were built.
OPTIONAL_BY_TRANSPORT = {
    "tcp": ("TcpBridgeClient", "TcpBridgeServer"),
    "quic": ("QuicBridgeClient", "QuicBridgeServer", "generate_self_signed_cert"),
}

for _transport, _classes in OPTIONAL_BY_TRANSPORT.items():
    if _transport in transports:
        for _name in _classes:
            globals()[_name] = getattr(_subetha, _name)
            __all__.append(_name)

__all__.append("OPTIONAL_BY_TRANSPORT")
__all__.sort()
