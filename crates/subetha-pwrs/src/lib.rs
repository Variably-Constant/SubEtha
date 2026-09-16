//! PowerShell bindings for SubEtha, bound to the Rust directly.
//!
//! A method call on an object the module returned costs one native
//! call into the library plus the host's own method invocation, about
//! two microseconds in PowerShell 7 and two thirds of one in Windows
//! PowerShell against seven nanoseconds for the Rust underneath, and a
//! record through the pipeline costs about as much as a call in
//! PowerShell 7 and several times that in Windows PowerShell. So the
//! module has two layers: `New-` and `Open-` cmdlets that obtain a
//! structure, and objects whose methods are the operations the Rust
//! type offers, one method per operation. `Send-` and `Receive-`
//! cmdlets carry items through the pipeline for the structures that
//! move items, so a script can pipe into and out of them.
//!
//! An object owns its mapping and releases it when disposed or when the
//! garbage collector finalizes it. A failure from a method is an
//! exception; from a cmdlet it is an error record, non-terminating
//! unless the cmdlet cannot go on.
//!
//! Bytes cross as `byte[]`: an argument is pinned where it lies and
//! copied nowhere, and a string is taken as its UTF-8; a result is one
//! array filled through one pin. A method that moves many items takes
//! or returns them in one call, so the cost of a call is spread over
//! the run rather than paid per item: a thousand atomic adds in one
//! call cost a few nanoseconds each, and 256 ring items packed in one
//! `byte[]` cost tens of nanoseconds each, where one item per call
//! costs microseconds. `bench/CallShapes.ps1` measures all of it.
//!
//! # Alignment
//!
//! Several SubEtha types are cache-line aligned and a good many embed
//! one. An object's Rust value is boxed by the framework when the object
//! is written, and a box honors the type's alignment, so a value sits
//! inline in its object type here and is never at an address the
//! allocator did not align for it.

mod common;
mod coordination;
mod frontdoor;
mod locks;
mod pipeline;
mod primitives;
mod rings;
mod sensing;
mod sketches;
mod structures;
mod transports;
mod values;
mod versioned;

pwrs::export_module! {
    name: "SubEtha",
    cmdlets: [
        primitives::NewSubEthaAtomic, primitives::OpenSubEthaAtomic,
        primitives::NewSubEthaRegion, primitives::OpenSubEthaRegion,
        primitives::NewSubEthaCell, primitives::OpenSubEthaCell,
        primitives::NewSubEthaVec, primitives::OpenSubEthaVec,
        primitives::NewSubEthaSharedArc, primitives::OpenSubEthaSharedArc,
        primitives::NewSubEthaLazyValue, primitives::OpenSubEthaLazyValue,
        primitives::NewSubEthaBitVec, primitives::OpenSubEthaBitVec,
        rings::NewSubEthaSpscRing, rings::OpenSubEthaSpscRing,
        rings::NewSubEthaBroadcastRing, rings::OpenSubEthaBroadcastRing,
        rings::NewSubEthaCapacityRing, rings::OpenSubEthaCapacityRing,
        rings::NewSubEthaLocaleRing, rings::OpenSubEthaLocaleRing,
        rings::NewSubEthaRing, rings::OpenSubEthaRing,
        rings::NewSubEthaReorderWindow,
        rings::NewSubEthaStack, rings::OpenSubEthaStack,
        rings::NewSubEthaDeque, rings::OpenSubEthaDeque,
        rings::NewSubEthaPubSub, rings::OpenSubEthaPubSub,
        rings::NewSubEthaLamportPair,
        rings::NewSubEthaFrameRegion, rings::OpenSubEthaFrameRegion,
        structures::NewSubEthaArena, structures::OpenSubEthaArena,
        structures::NewSubEthaLinkedList, structures::OpenSubEthaLinkedList,
        structures::NewSubEthaSlab, structures::OpenSubEthaSlab,
        structures::NewSubEthaBTreeMap, structures::OpenSubEthaBTreeMap,
        structures::NewSubEthaHashMap, structures::OpenSubEthaHashMap,
        structures::NewSubEthaMpscPool, structures::NewSubEthaMpmcGrid,
        coordination::NewSubEthaNotifierSet,
        coordination::NewSubEthaLeaderElection, coordination::OpenSubEthaLeaderElection,
        coordination::NewSubEthaHolderTable, coordination::OpenSubEthaHolderTable,
        coordination::NewSubEthaHeartbeat, coordination::OpenSubEthaHeartbeat,
        coordination::NewSubEthaEpochBarrier, coordination::OpenSubEthaEpochBarrier,
        coordination::NewSubEthaCondvar, coordination::OpenSubEthaCondvar, coordination::WaitSubEthaCondition,
        coordination::NewSubEthaFenceClock, coordination::OpenSubEthaFenceClock,
        coordination::NewSubEthaEpochs, coordination::OpenSubEthaEpochs,
        locks::NewSubEthaRWLock, locks::OpenSubEthaRWLock,
        locks::NewSubEthaSemaphore, locks::OpenSubEthaSemaphore,
        locks::NewSubEthaOwnerLease, locks::OpenSubEthaOwnerLease,
        sketches::NewSubEthaBloomFilter, sketches::OpenSubEthaBloomFilter, sketches::MeasureSubEthaBloomSize,
        sketches::NewSubEthaBlockedBloomFilter, sketches::OpenSubEthaBlockedBloomFilter,
        sketches::NewSubEthaHyperLogLog, sketches::OpenSubEthaHyperLogLog,
        sketches::NewSubEthaCountMinSketch, sketches::OpenSubEthaCountMinSketch, sketches::MeasureSubEthaSketchSize,
        sketches::NewSubEthaHistogram, sketches::OpenSubEthaHistogram,
        sketches::NewSubEthaRateLimiter, sketches::OpenSubEthaRateLimiter,
        sketches::NewSubEthaLruCache, sketches::OpenSubEthaLruCache,
        versioned::NewSubEthaReservoir, versioned::OpenSubEthaReservoir,
        versioned::NewSubEthaHandleTable, versioned::OpenSubEthaHandleTable,
        versioned::NewSubEthaTimePointTile, versioned::OpenSubEthaTimePointTile,
        versioned::NewSubEthaVersionChain, versioned::OpenSubEthaVersionChain,
        versioned::NewSubEthaVersionedSlab, versioned::OpenSubEthaVersionedSlab,
        versioned::NewSubEthaVersionedMap, versioned::OpenSubEthaVersionedMap,
        versioned::NewSubEthaLanedMap, versioned::OpenSubEthaLanedMap,
        versioned::NewSubEthaTopologyMap, versioned::OpenSubEthaTopologyMap,
        versioned::NewSubEthaGraph, versioned::OpenSubEthaGraph,
        versioned::NewSubEthaUniversal, versioned::OpenSubEthaUniversal,
        versioned::NewSubEthaTower, versioned::OpenSubEthaTower,
        frontdoor::NewSubEthaChannel, frontdoor::OpenSubEthaChannel,
        frontdoor::NewSubEthaAdaptiveQueue,
        frontdoor::NewSubEthaWorkQueue, frontdoor::OpenSubEthaWorkQueue,
        frontdoor::NewSubEthaKvMap,
        frontdoor::NewSubEthaQosPolicy,
        values::NewSubEthaTinyBloom, values::NewSubEthaFineBloom, values::NewSubEthaClock, values::NewSubEthaCausalClock,
        sensing::NewSubEthaSensSender, sensing::NewSubEthaSensReceiver,
        sensing::NewSubEthaLossKind, sensing::NewSubEthaLossBursts, sensing::NewSubEthaTiming, sensing::NewSubEthaRoundTripShape,
        sensing::NewSubEthaPeriodicity, sensing::NewSubEthaCapacity, sensing::NewSubEthaForecast, sensing::NewSubEthaPathChanges,
        transports::NewSubEthaTcpBridgeClient, transports::NewSubEthaTcpBridgeServer,
        transports::NewSubEthaSelfSignedCert, transports::NewSubEthaQuicBridgeClient, transports::NewSubEthaQuicBridgeServer,
        transports::GetSubEthaTransport,
        pipeline::SendSubEthaItem, pipeline::ReceiveSubEthaItem,
    ],
    classes: [
        common::StampedItem, common::ClockReading,
        primitives::Atomic, primitives::Region, primitives::Cell, primitives::SharedVec, primitives::SharedArc,
        primitives::LazyValue, primitives::BitVec,
        rings::PackedItems, rings::SpscRing, rings::BroadcastRing, rings::CapacityRing, rings::LocaleRing, rings::Ring,
        rings::OrderedReceiver, rings::ReorderWindow, rings::Stack, rings::Deque, rings::PubSub, rings::Subscriber,
        rings::LamportProducer, rings::LamportConsumer, rings::FrameRegion,
        structures::Arena, structures::LinkedList, structures::Slab, structures::Pair, structures::BTreeMap,
        structures::Exchange, structures::HashMap,
        structures::MpscProducer, structures::MpscConsumer, structures::MpscPool,
        structures::MpmcProducer, structures::MpmcConsumer, structures::MpmcGrid,
        coordination::NotifierSet, coordination::Notifier, coordination::LeaderElection, coordination::HolderTable,
        coordination::HeartbeatSlot, coordination::Heartbeat, coordination::EpochBarrier, coordination::Condvar,
        coordination::FenceClock, coordination::EpochTicket, coordination::Epochs,
        locks::RWLock, locks::Hold, locks::Semaphore, locks::PermitHold, locks::OwnerLease, locks::LeaseHold,
        sketches::BloomSize, sketches::SketchSize, sketches::BloomFilter, sketches::BlockedBloomFilter,
        sketches::HyperLogLog, sketches::CountMinSketch, sketches::Histogram, sketches::RateLimiter, sketches::LruCache,
        versioned::Reservoir, versioned::HandleTable, versioned::TileEntry, versioned::TimePointTile,
        versioned::Versioned, versioned::VersionChain, versioned::SlotVersion, versioned::VersionedSlab, versioned::SlabPin,
        versioned::Entry, versioned::Scan, versioned::VersionedMap, versioned::MapPin,
        versioned::LanedMap, versioned::LaneClaim, versioned::LanedPin,
        versioned::FanCount, versioned::TopologyMap, versioned::Neighbor, versioned::Graph,
        versioned::OpCounts, versioned::Universal, versioned::Tower,
        frontdoor::Channel, frontdoor::Traffic, frontdoor::AdaptiveQueue, frontdoor::WorkQueue, frontdoor::KvMap,
        frontdoor::QosSnapshot, frontdoor::QosPolicy,
        values::TinyBloom, values::FineBloom, values::Clock, values::CausalClock,
        sensing::Endpoint, sensing::SourcedItem, sensing::SensSender, sensing::SensReceiver,
        sensing::LossKind, sensing::BurstRates, sensing::LossBursts, sensing::Timing, sensing::RoundTripShape,
        sensing::Beat, sensing::Periodicity, sensing::Capacity, sensing::Forecast, sensing::PathMark, sensing::PathChanges,
        transports::TcpBridgeClient, transports::TcpBridgeServer, transports::Certificate,
        transports::QuicBridgeClient, transports::QuicBridgeServer,
    ],
    enums: [
        primitives::MemoryOrder, rings::OrderingMode, rings::Locale, rings::StampKind,
        structures::ArenaAccess, structures::Inserted,
        frontdoor::Durability, frontdoor::Reliability, frontdoor::OrderingNeed, frontdoor::QueueShape, frontdoor::QosPreset,
        versioned::Topology, versioned::SetStrategy,
        sensing::SensCodeKind, sensing::LossClassKind,
    ],
}
