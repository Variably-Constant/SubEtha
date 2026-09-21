---
title: "Every object, in full"
weight: 20
---

# Every object, in full

Every one of the 117 object types a cmdlet or a method
can write, with each property and each method signature. Generated
from the built module by
`crates/subetha-pwrs/tools/Export-Reference.ps1`.

A method that answers `?` may answer `$null`, which the surface
uses for an ordinary absent answer rather than a fault.

## Contents

[`SubEtha.AdaptiveQueue`](#subetha-adaptivequeue), [`SubEtha.Arena`](#subetha-arena), [`SubEtha.Atomic`](#subetha-atomic), [`SubEtha.Beat`](#subetha-beat), [`SubEtha.BitVec`](#subetha-bitvec), [`SubEtha.BlockedBloomFilter`](#subetha-blockedbloomfilter), [`SubEtha.BloomFilter`](#subetha-bloomfilter), [`SubEtha.BloomSize`](#subetha-bloomsize), [`SubEtha.BroadcastRing`](#subetha-broadcastring), [`SubEtha.BTreeMap`](#subetha-btreemap), [`SubEtha.BurstRates`](#subetha-burstrates), [`SubEtha.Capacity`](#subetha-capacity), [`SubEtha.CapacityRing`](#subetha-capacityring), [`SubEtha.CausalClock`](#subetha-causalclock), [`SubEtha.Cell`](#subetha-cell), [`SubEtha.Certificate`](#subetha-certificate), [`SubEtha.Channel`](#subetha-channel), [`SubEtha.Clock`](#subetha-clock), [`SubEtha.ClockReading`](#subetha-clockreading), [`SubEtha.Condvar`](#subetha-condvar), [`SubEtha.CountMinSketch`](#subetha-countminsketch), [`SubEtha.Deque`](#subetha-deque), [`SubEtha.Endpoint`](#subetha-endpoint), [`SubEtha.Entry`](#subetha-entry), [`SubEtha.EpochBarrier`](#subetha-epochbarrier), [`SubEtha.Epochs`](#subetha-epochs), [`SubEtha.EpochTicket`](#subetha-epochticket), [`SubEtha.Exchange`](#subetha-exchange), [`SubEtha.FanCount`](#subetha-fancount), [`SubEtha.FenceClock`](#subetha-fenceclock), [`SubEtha.FineBloom`](#subetha-finebloom), [`SubEtha.Forecast`](#subetha-forecast), [`SubEtha.FrameRegion`](#subetha-frameregion), [`SubEtha.Graph`](#subetha-graph), [`SubEtha.HandleTable`](#subetha-handletable), [`SubEtha.HashMap`](#subetha-hashmap), [`SubEtha.Heartbeat`](#subetha-heartbeat), [`SubEtha.HeartbeatSlot`](#subetha-heartbeatslot), [`SubEtha.Histogram`](#subetha-histogram), [`SubEtha.Hold`](#subetha-hold), [`SubEtha.HolderTable`](#subetha-holdertable), [`SubEtha.HyperLogLog`](#subetha-hyperloglog), [`SubEtha.KvMap`](#subetha-kvmap), [`SubEtha.LamportConsumer`](#subetha-lamportconsumer), [`SubEtha.LamportProducer`](#subetha-lamportproducer), [`SubEtha.LaneClaim`](#subetha-laneclaim), [`SubEtha.LanedMap`](#subetha-lanedmap), [`SubEtha.LanedPin`](#subetha-lanedpin), [`SubEtha.LazyValue`](#subetha-lazyvalue), [`SubEtha.LeaderElection`](#subetha-leaderelection), [`SubEtha.LeaseHold`](#subetha-leasehold), [`SubEtha.LinkedList`](#subetha-linkedlist), [`SubEtha.LocaleRing`](#subetha-localering), [`SubEtha.LossBursts`](#subetha-lossbursts), [`SubEtha.LossKind`](#subetha-losskind), [`SubEtha.LruCache`](#subetha-lrucache), [`SubEtha.MapPin`](#subetha-mappin), [`SubEtha.MpmcConsumer`](#subetha-mpmcconsumer), [`SubEtha.MpmcGrid`](#subetha-mpmcgrid), [`SubEtha.MpmcProducer`](#subetha-mpmcproducer), [`SubEtha.MpscConsumer`](#subetha-mpscconsumer), [`SubEtha.MpscPool`](#subetha-mpscpool), [`SubEtha.MpscProducer`](#subetha-mpscproducer), [`SubEtha.Neighbor`](#subetha-neighbor), [`SubEtha.Notifier`](#subetha-notifier), [`SubEtha.NotifierSet`](#subetha-notifierset), [`SubEtha.OpCounts`](#subetha-opcounts), [`SubEtha.OrderedReceiver`](#subetha-orderedreceiver), [`SubEtha.OwnerLease`](#subetha-ownerlease), [`SubEtha.PackedItems`](#subetha-packeditems), [`SubEtha.Pair`](#subetha-pair), [`SubEtha.PathChanges`](#subetha-pathchanges), [`SubEtha.PathMark`](#subetha-pathmark), [`SubEtha.Periodicity`](#subetha-periodicity), [`SubEtha.PermitHold`](#subetha-permithold), [`SubEtha.PubSub`](#subetha-pubsub), [`SubEtha.QosPolicy`](#subetha-qospolicy), [`SubEtha.QosSnapshot`](#subetha-qossnapshot), [`SubEtha.QuicBridgeClient`](#subetha-quicbridgeclient), [`SubEtha.QuicBridgeServer`](#subetha-quicbridgeserver), [`SubEtha.RateLimiter`](#subetha-ratelimiter), [`SubEtha.Region`](#subetha-region), [`SubEtha.ReorderWindow`](#subetha-reorderwindow), [`SubEtha.Reservoir`](#subetha-reservoir), [`SubEtha.Ring`](#subetha-ring), [`SubEtha.RoundTripShape`](#subetha-roundtripshape), [`SubEtha.RWLock`](#subetha-rwlock), [`SubEtha.Scan`](#subetha-scan), [`SubEtha.Semaphore`](#subetha-semaphore), [`SubEtha.SensReceiver`](#subetha-sensreceiver), [`SubEtha.SensSender`](#subetha-senssender), [`SubEtha.SharedArc`](#subetha-sharedarc), [`SubEtha.SketchSize`](#subetha-sketchsize), [`SubEtha.Slab`](#subetha-slab), [`SubEtha.SlabPin`](#subetha-slabpin), [`SubEtha.SlotVersion`](#subetha-slotversion), [`SubEtha.SourcedItem`](#subetha-sourceditem), [`SubEtha.SpscRing`](#subetha-spscring), [`SubEtha.Stack`](#subetha-stack), [`SubEtha.StampedItem`](#subetha-stampeditem), [`SubEtha.Subscriber`](#subetha-subscriber), [`SubEtha.TcpBridgeClient`](#subetha-tcpbridgeclient), [`SubEtha.TcpBridgeServer`](#subetha-tcpbridgeserver), [`SubEtha.TileEntry`](#subetha-tileentry), [`SubEtha.TimePointTile`](#subetha-timepointtile), [`SubEtha.Timing`](#subetha-timing), [`SubEtha.TinyBloom`](#subetha-tinybloom), [`SubEtha.TopologyMap`](#subetha-topologymap), [`SubEtha.Tower`](#subetha-tower), [`SubEtha.Traffic`](#subetha-traffic), [`SubEtha.Universal`](#subetha-universal), [`SubEtha.Vec`](#subetha-vec), [`SubEtha.VersionChain`](#subetha-versionchain), [`SubEtha.Versioned`](#subetha-versioned), [`SubEtha.VersionedMap`](#subetha-versionedmap), [`SubEtha.VersionedSlab`](#subetha-versionedslab), [`SubEtha.WorkQueue`](#subetha-workqueue)

## SubEtha.AdaptiveQueue

| Property | Type |
|---|---|
| `MaxItemSize` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `ChangeShapeTo(SubEtha.QueueShape shape)` | `void` |
| `Inversions()` | `ulong` |
| `MaybeChangeShape()` | `SubEtha.QueueShape?` |
| `Ordering()` | `SubEtha.OrderingNeed` |
| `Recv()` | `object` |
| `RecvFor(double? timeout)` | `object` |
| `RecvMany(ulong? maxItems)` | `object[]` |
| `Send(object item)` | `bool` |
| `SendFor(object item, double? timeout)` | `bool` |
| `SendMany(object[] items)` | `ulong` |
| `SetOrdering(SubEtha.OrderingNeed ordering)` | `void` |
| `Shape()` | `SubEtha.QueueShape` |
| `ShapeGeneration()` | `ulong` |
| `Traffic()` | `SubEtha.Traffic` |

## SubEtha.Arena

| Property | Type |
|---|---|
| `CapacityBytes` | `ulong` |
| `Path` | `string` |
| `Writable` | `bool` |

| Method | Answers |
|---|---|
| `Get(ulong reference)` | `string` |
| `GetBytes(ulong reference)` | `object` |
| `GetMany(ulong[] references)` | `string[]` |
| `Intern(string value)` | `ulong?` |
| `InternBytes(object value)` | `ulong?` |
| `InternMany(string[] values)` | `ulong[]` |
| `RemainingBytes()` | `ulong` |
| `UsedBytes()` | `ulong` |

## SubEtha.Atomic

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `CompareExchange(ulong expected, ulong desired, SubEtha.MemoryOrder? order)` | `ulong` |
| `FetchAdd(ulong? value, SubEtha.MemoryOrder? order)` | `ulong` |
| `FetchAddMany(ulong count, ulong? value, SubEtha.MemoryOrder? order)` | `ulong` |
| `FetchAnd(ulong value, SubEtha.MemoryOrder? order)` | `ulong` |
| `FetchOr(ulong value, SubEtha.MemoryOrder? order)` | `ulong` |
| `FetchSub(ulong? value, SubEtha.MemoryOrder? order)` | `ulong` |
| `FetchXor(ulong value, SubEtha.MemoryOrder? order)` | `ulong` |
| `Load(SubEtha.MemoryOrder? order)` | `ulong` |
| `Store(ulong value, SubEtha.MemoryOrder? order)` | `void` |
| `Swap(ulong value, SubEtha.MemoryOrder? order)` | `ulong` |

## SubEtha.Beat

| Property | Type |
|---|---|
| `Seconds` | `double` |
| `Strength` | `double` |

## SubEtha.BitVec

| Property | Type |
|---|---|
| `CapacityBits` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Clear(ulong index)` | `bool` |
| `Get(ulong index)` | `bool` |
| `Set(ulong index)` | `bool` |
| `SetRange(ulong lo, ulong hi)` | `void` |
| `Toggle(ulong index)` | `bool` |

## SubEtha.BlockedBloomFilter

| Property | Type |
|---|---|
| `Blocks` | `ulong` |
| `Hashes` | `uint` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Clear()` | `void` |
| `Contains(object item)` | `bool` |
| `ContainsMany(object[] items)` | `bool[]` |
| `Flush()` | `void` |
| `Insert(object item)` | `void` |
| `InsertMany(object[] items)` | `ulong` |

## SubEtha.BloomFilter

| Property | Type |
|---|---|
| `Bits` | `ulong` |
| `Hashes` | `uint` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Clear()` | `void` |
| `Contains(object item)` | `bool` |
| `ContainsMany(object[] items)` | `bool[]` |
| `FalsePositiveRate()` | `double` |
| `Insert(object item)` | `void` |
| `InsertMany(object[] items)` | `ulong` |

## SubEtha.BloomSize

| Property | Type |
|---|---|
| `Bits` | `ulong` |
| `Hashes` | `uint` |

## SubEtha.BroadcastRing

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |
| `PayloadSize` | `ulong` |

| Method | Answers |
|---|---|
| `ActiveConsumers()` | `ulong` |
| `Lag(ulong consumer)` | `ulong?` |
| `ProducerPosition()` | `ulong` |
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |
| `Recv(ulong consumer)` | `object` |
| `RecvMany(ulong consumer, ulong maxItems)` | `object[]` |
| `RegisterConsumer()` | `ulong` |
| `UnregisterConsumer(ulong consumer)` | `void` |
| `WaitForConsumers(ulong want, double timeoutSeconds)` | `ulong` |

## SubEtha.BTreeMap

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `KeySize` | `ulong` |
| `Path` | `string` |
| `ValueSize` | `ulong` |

| Method | Answers |
|---|---|
| `Clear()` | `void` |
| `Contains(object key)` | `bool` |
| `Count()` | `ulong` |
| `First()` | `SubEtha.Pair` |
| `Flush()` | `void` |
| `Get(object key)` | `object` |
| `Insert(object key, object value)` | `object` |
| `InsertMany(object[] keys, object[] values)` | `ulong` |
| `Last()` | `SubEtha.Pair` |
| `Nodes()` | `ulong` |
| `Remove(object key)` | `object` |

## SubEtha.BurstRates

| Property | Type |
|---|---|
| `Entering` | `double` |
| `Leaving` | `double` |

## SubEtha.Capacity

| Property | Type |
|---|---|
| `ProbeBytes` | `ulong` |

| Method | Answers |
|---|---|
| `Available()` | `double?` |
| `LinkCapacity()` | `double?` |
| `ObservePair(byte index, double arrivedMicroseconds)` | `void` |
| `ObserveTrain(double arrivedMicroseconds)` | `void` |
| `PairSamples()` | `ulong` |
| `Reset()` | `void` |
| `TrainRate()` | `double?` |
| `TrainSamples()` | `uint` |

## SubEtha.CapacityRing

| Property | Type |
|---|---|
| `MaxConsumers` | `ulong` |
| `MaxProducers` | `ulong` |
| `Path` | `string` |
| `Stamped` | `bool` |

| Method | Answers |
|---|---|
| `Capacity()` | `ulong` |
| `ClearWarm()` | `void` |
| `Inversions()` | `ulong` |
| `MorphTo(ulong capacity)` | `void` |
| `OrderingMode()` | `SubEtha.OrderingMode?` |
| `PinGeneration()` | `ulong` |
| `Prewarm(ulong capacity)` | `void` |
| `Recv(ulong consumer)` | `object` |
| `RecvMany(ulong consumer, ulong maxItems)` | `object[]` |
| `RegisterConsumer()` | `ulong` |
| `RegisterProducer()` | `ulong` |
| `Send(ulong producer, object item)` | `bool` |
| `SendMany(ulong producer, object[] items)` | `ulong` |
| `SetOrderingMode(SubEtha.OrderingMode mode)` | `void` |
| `StalePops()` | `ulong` |
| `WarmCapacity()` | `ulong?` |
| `WarmHits()` | `ulong` |

## SubEtha.CausalClock

| Property | Type |
|---|---|
| `Counts` | `ulong[]` |

| Method | Answers |
|---|---|
| `Compare(SubEtha.CausalClock other)` | `string` |
| `ConcurrentWith(SubEtha.CausalClock other)` | `bool` |
| `Count(ulong node)` | `ulong` |
| `HappenedBefore(SubEtha.CausalClock other)` | `bool` |
| `Merge(SubEtha.CausalClock other)` | `SubEtha.CausalClock` |
| `Nodes()` | `ulong` |
| `Tick(ulong node)` | `SubEtha.CausalClock` |

## SubEtha.Cell

| Property | Type |
|---|---|
| `Path` | `string` |
| `ValueSize` | `ulong` |

| Method | Answers |
|---|---|
| `Flush()` | `void` |
| `Get()` | `object` |
| `Set(object value)` | `void` |
| `Version()` | `uint` |

## SubEtha.Certificate

| Property | Type |
|---|---|
| `Cert` | `object` |
| `Key` | `object` |
| `Name` | `string` |

## SubEtha.Channel

| Property | Type |
|---|---|
| `MaxItemSize` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Recv()` | `object` |
| `RecvFor(double? timeout)` | `object` |
| `RecvMany(ulong? maxItems)` | `object[]` |
| `Send(object item)` | `bool` |
| `SendFor(object item, double? timeout)` | `bool` |
| `SendMany(object[] items)` | `ulong` |

## SubEtha.Clock

| Property | Type |
|---|---|
| `Logical` | `ulong` |
| `Physical` | `ulong` |

| Method | Answers |
|---|---|
| `Advance(ulong physical)` | `SubEtha.Clock` |
| `After(SubEtha.Clock other)` | `bool` |
| `Before(SubEtha.Clock other)` | `bool` |
| `CompareTo(SubEtha.Clock other)` | `int` |
| `Merge(SubEtha.Clock received, ulong physical)` | `SubEtha.Clock` |
| `SameAs(SubEtha.Clock other)` | `bool` |

## SubEtha.ClockReading

| Property | Type |
|---|---|
| `Logical` | `ulong` |
| `PhysicalUs` | `ulong` |

## SubEtha.Condvar

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Generation()` | `ulong` |
| `NotifyAll()` | `ulong` |
| `NotifyOne()` | `ulong` |

## SubEtha.CountMinSketch

| Property | Type |
|---|---|
| `Depth` | `uint` |
| `Path` | `string` |
| `Width` | `uint` |

| Method | Answers |
|---|---|
| `EstimateCount(object item)` | `ulong` |
| `EstimateMany(object[] items)` | `ulong[]` |
| `Insert(object item)` | `void` |
| `InsertMany(object[] items)` | `ulong` |
| `InsertN(object item, ulong count)` | `void` |
| `Reset()` | `void` |
| `TotalInserts()` | `ulong` |

## SubEtha.Deque

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `ElementSize` | `ulong` |
| `Path` | `string` |
| `Thief` | `bool` |

| Method | Answers |
|---|---|
| `ApproxLen()` | `ulong` |
| `Flush()` | `void` |
| `Pop()` | `object` |
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |
| `Steal()` | `object` |
| `StealMany(ulong maxItems)` | `object[]` |

## SubEtha.Endpoint

| Property | Type |
|---|---|
| `Host` | `string` |
| `Port` | `ushort` |

## SubEtha.Entry

| Property | Type |
|---|---|
| `Key` | `ulong` |
| `Value` | `ulong` |

## SubEtha.EpochBarrier

| Property | Type |
|---|---|
| `GraceEpochs` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Arrived()` | `uint` |
| `CurrentEpoch()` | `uint` |
| `LivePeers()` | `uint` |
| `Wait(uint epoch, double? timeout, uint? quorum)` | `bool` |

## SubEtha.Epochs

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Advance()` | `ulong` |
| `ClaimTicket()` | `SubEtha.EpochTicket` |
| `DeadTickets()` | `ulong[]` |
| `FreeDeadTicket(ulong epoch)` | `bool` |
| `Now()` | `ulong` |
| `OpenTickets()` | `ulong` |
| `PublishTicket(ulong slot)` | `void` |

## SubEtha.EpochTicket

| Property | Type |
|---|---|
| `Epoch` | `ulong` |
| `Slot` | `ulong` |

## SubEtha.Exchange

| Property | Type |
|---|---|
| `Found` | `object` |
| `Swapped` | `bool` |

## SubEtha.FanCount

| Property | Type |
|---|---|
| `Participant` | `uint` |
| `Places` | `uint` |

## SubEtha.FenceClock

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `GetLocal(ulong slot)` | `SubEtha.ClockReading` |
| `GlobalFence()` | `SubEtha.ClockReading` |
| `Merge(ulong slot, ulong physicalUs, ulong logical)` | `SubEtha.ClockReading` |
| `Register(uint? pid)` | `ulong` |
| `SharedClockUs()` | `ulong` |
| `Tick(ulong slot)` | `SubEtha.ClockReading` |
| `Unregister(ulong slot)` | `void` |

## SubEtha.FineBloom

| Property | Type |
|---|---|
| `SetBits` | `uint` |

| Method | Answers |
|---|---|
| `Contains(object key)` | `bool` |
| `ContainsMany(object[] keys)` | `bool[]` |
| `Insert(object key)` | `void` |
| `InsertMany(object[] keys)` | `ulong` |
| `SuggestedCapacity()` | `ulong` |

## SubEtha.Forecast

| Method | Answers |
|---|---|
| `MeanRate()` | `double` |
| `NextRate()` | `double` |
| `Observe(ulong bytes, double seconds)` | `void` |

## SubEtha.FrameRegion

| Property | Type |
|---|---|
| `BlockCount` | `ulong` |
| `BlockSize` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Allocate()` | `uint?` |
| `Free(uint index)` | `void` |
| `ReadBlock(uint index, ulong length)` | `object` |
| `TakeBlock(uint index, ulong length)` | `object` |
| `WriteBlock(uint index, object payload)` | `void` |
| `WriteNew(object payload)` | `uint?` |

## SubEtha.Graph

| Property | Type |
|---|---|
| `MaxEdges` | `ulong` |
| `MaxNodes` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `AddEdge(uint source, uint target, ulong value)` | `uint` |
| `AddEdges(uint[] sources, uint[] targets, ulong[] values)` | `uint[]` |
| `AddNode(ulong value)` | `uint` |
| `AddNodes(ulong[] values)` | `uint[]` |
| `EdgeCount()` | `ulong` |
| `Flush()` | `void` |
| `FlushAsync()` | `void` |
| `Neighbors(uint source)` | `SubEtha.Neighbor[]` |
| `NodeCount()` | `ulong` |
| `NodeValue(uint node)` | `ulong?` |
| `OutDegree(uint source)` | `uint?` |
| `RemoveEdge(uint source, uint edge)` | `ulong?` |

## SubEtha.HandleTable

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `MaxValueBytes` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Contains(ulong handle)` | `bool` |
| `Count()` | `ulong` |
| `Flush()` | `void` |
| `FlushAsync()` | `void` |
| `Get(ulong handle)` | `object` |
| `GetMany(ulong[] handles)` | `object[]` |
| `Insert(object value)` | `ulong` |
| `InsertMany(object[] values)` | `ulong[]` |
| `Remove(ulong handle)` | `object` |

## SubEtha.HashMap

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `KeySize` | `ulong` |
| `Path` | `string` |
| `ValueSize` | `ulong` |

| Method | Answers |
|---|---|
| `Clear()` | `void` |
| `CompareExchange(object key, object expected, object desired)` | `SubEtha.Exchange` |
| `Contains(object key)` | `bool` |
| `Count()` | `ulong` |
| `Get(object key)` | `object` |
| `GetMany(object[] keys)` | `object[]` |
| `Insert(object key, object value)` | `SubEtha.InsertOutcome` |
| `InsertMany(object[] keys, object[] values)` | `ulong` |
| `Remove(object key)` | `object` |
| `Tombstones()` | `ulong` |

## SubEtha.Heartbeat

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Barrier(string path, ulong? graceEpochs, bool? open)` | `SubEtha.EpochBarrier` |
| `Beat(ulong slot)` | `void` |
| `GlobalEpoch()` | `ulong` |
| `Register(uint? pid)` | `ulong` |
| `Snapshot(ulong slot)` | `SubEtha.HeartbeatSlot` |
| `TickGlobalEpoch()` | `ulong` |
| `Unregister(ulong slot)` | `void` |

## SubEtha.HeartbeatSlot

| Property | Type |
|---|---|
| `InFlight` | `ulong` |
| `LastSeenEpoch` | `ulong` |
| `Pid` | `uint` |
| `Role` | `uint` |

## SubEtha.Histogram

| Property | Type |
|---|---|
| `Boundaries` | `ulong[]` |
| `Buckets` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Count(ulong bucket)` | `ulong` |
| `Counts()` | `ulong[]` |
| `Percentile(double p)` | `ulong` |
| `Record(ulong value)` | `ulong` |
| `RecordMany(ulong[] values)` | `ulong` |
| `TotalCount()` | `ulong` |

## SubEtha.Hold

| Property | Type |
|---|---|
| `Held` | `bool` |
| `Write` | `bool` |

| Method | Answers |
|---|---|
| `Release()` | `void` |

## SubEtha.HolderTable

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Claim(ulong payload)` | `ulong?` |
| `Live()` | `ulong` |
| `Payload(ulong slot)` | `ulong?` |
| `Publish(ulong slot, ulong payload)` | `void` |
| `Release(ulong slot)` | `void` |
| `Reserve()` | `ulong?` |

## SubEtha.HyperLogLog

| Property | Type |
|---|---|
| `Path` | `string` |
| `Precision` | `byte` |
| `Registers` | `uint` |

| Method | Answers |
|---|---|
| `Estimate()` | `ulong` |
| `Flush()` | `void` |
| `Insert(object item)` | `void` |
| `InsertMany(object[] items)` | `ulong` |
| `Reset()` | `void` |

## SubEtha.KvMap

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Contains(ulong key)` | `bool` |
| `Count()` | `ulong` |
| `Get(ulong key)` | `ulong?` |
| `GetMany(ulong[] keys)` | `object[]` |
| `Insert(ulong key, ulong value)` | `bool` |
| `InsertMany(ulong[] keys, ulong[] values)` | `bool[]` |

## SubEtha.LamportConsumer

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Pop()` | `object` |
| `PopMany(ulong maxItems)` | `object[]` |
| `PopPacked(ulong maxItems)` | `SubEtha.PackedItems` |

## SubEtha.LamportProducer

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |
| `PayloadSize` | `ulong` |

| Method | Answers |
|---|---|
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |
| `PushPacked(object data, ulong itemLen)` | `ulong` |

## SubEtha.LaneClaim

| Method | Answers |
|---|---|
| `Held()` | `bool` |
| `Index()` | `ulong` |
| `Insert(ulong key, ulong value)` | `ulong?` |
| `InsertAt(ulong key, ulong value, ulong born)` | `ulong?` |
| `InsertMany(ulong[] keys, ulong[] values)` | `object[]` |
| `Lanes()` | `ulong` |
| `Release()` | `void` |
| `Remove(ulong key)` | `ulong?` |
| `RemoveAt(ulong key, ulong died)` | `ulong?` |

## SubEtha.LanedMap

| Property | Type |
|---|---|
| `Directory` | `string` |
| `Lanes` | `ulong` |

| Method | Answers |
|---|---|
| `ClaimLane()` | `SubEtha.LaneClaim` |
| `ClaimLaneFor(ulong key)` | `SubEtha.LaneClaim` |
| `Count()` | `ulong` |
| `Flush()` | `void` |
| `Get(ulong key)` | `ulong?` |
| `GetMany(ulong[] keys)` | `object[]` |
| `HeldLanes()` | `ulong` |
| `LaneOf(ulong key)` | `ulong?` |
| `Pin()` | `SubEtha.LanedPin` |
| `ReapDeadClaims()` | `ulong` |
| `Sweep()` | `ulong` |
| `VoidEpoch(ulong epoch)` | `ulong` |

## SubEtha.LanedPin

| Method | Answers |
|---|---|
| `Epoch()` | `ulong` |
| `Get(ulong key)` | `ulong?` |
| `GetMany(ulong[] keys)` | `object[]` |
| `Held()` | `bool` |
| `Release()` | `void` |
| `Scan(ulong? low, ulong? high, ulong? limit)` | `SubEtha.Entry[]` |
| `ScanFrom(ulong? low, ulong? high, ulong? limit)` | `SubEtha.Scan` |

## SubEtha.LazyValue

| Property | Type |
|---|---|
| `Path` | `string` |
| `ValueSize` | `ulong` |

| Method | Answers |
|---|---|
| `Claim(uint? pid)` | `bool` |
| `Get()` | `object` |
| `Publish(object value, uint? pid)` | `bool` |
| `Ready()` | `bool` |
| `Wait(double? timeout)` | `object` |

## SubEtha.LeaderElection

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Beat(uint? pid)` | `bool` |
| `GlobalEpoch()` | `ulong` |
| `IsLeader(uint? pid)` | `bool` |
| `Leader()` | `uint?` |
| `StepDown(uint? pid)` | `bool` |
| `Term()` | `uint` |
| `TickEpoch()` | `ulong` |
| `TryClaim(ulong? graceEpochs, uint? pid)` | `bool` |

## SubEtha.LeaseHold

| Property | Type |
|---|---|
| `Held` | `bool` |
| `Pid` | `uint` |

| Method | Answers |
|---|---|
| `Beat()` | `bool` |
| `Read()` | `object` |
| `Release()` | `void` |
| `Write(object value)` | `bool` |

## SubEtha.LinkedList

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `ElementSize` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Count()` | `ulong` |
| `Get(uint index)` | `object` |
| `PopBack()` | `object` |
| `PopFront()` | `object` |
| `PushBack(object value)` | `uint` |
| `PushBackMany(object[] values)` | `uint[]` |
| `PushFront(object value)` | `uint` |
| `Remove(uint index)` | `object` |

## SubEtha.LocaleRing

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `MaxConsumers` | `ulong` |
| `MaxProducers` | `ulong` |
| `Path` | `string` |
| `Stamped` | `bool` |

| Method | Answers |
|---|---|
| `Inversions()` | `ulong` |
| `Locale()` | `SubEtha.Locale` |
| `LocaleGeneration()` | `ulong` |
| `MigrateTo(SubEtha.Locale locale)` | `void` |
| `OrderingMode()` | `SubEtha.OrderingMode?` |
| `Recv(ulong consumer)` | `object` |
| `RecvMany(ulong consumer, ulong maxItems)` | `object[]` |
| `RegisterConsumer()` | `ulong` |
| `RegisterProducer()` | `ulong` |
| `Send(ulong producer, object item)` | `bool` |
| `SendMany(ulong producer, object[] items)` | `ulong` |
| `SetOrderingMode(SubEtha.OrderingMode mode)` | `void` |

## SubEtha.LossBursts

| Method | Answers |
|---|---|
| `MeanRunLength()` | `double?` |
| `Observe(bool lost)` | `void` |
| `ObserveMany(bool[] losses)` | `ulong` |
| `Samples()` | `ulong` |
| `SteadyLoss()` | `double?` |
| `TransitionRates()` | `SubEtha.BurstRates` |

## SubEtha.LossKind

| Method | Answers |
|---|---|
| `Classify(uint gap, double spacingMicroseconds)` | `SubEtha.LossClass` |
| `CongestionShare()` | `Single` |
| `DelaySpread()` | `double` |
| `ObserveDelay(double microseconds)` | `void` |
| `ObserveSpacing(double microseconds)` | `void` |

## SubEtha.LruCache

| Property | Type |
|---|---|
| `Capacity` | `uint` |
| `KeySize` | `ulong` |
| `Path` | `string` |
| `ValueSize` | `ulong` |

| Method | Answers |
|---|---|
| `Contains(object key)` | `bool` |
| `Count()` | `ulong` |
| `Get(object key)` | `object` |
| `GetAndTouch(object key)` | `object` |
| `GetMany(object[] keys)` | `object[]` |
| `Put(object key, object value)` | `bool` |
| `PutMany(object[] keys, object[] values)` | `ulong` |
| `Remove(object key)` | `object` |
| `Touch(object key)` | `bool` |

## SubEtha.MapPin

| Method | Answers |
|---|---|
| `Epoch()` | `ulong` |
| `Get(ulong key)` | `ulong?` |
| `GetMany(ulong[] keys)` | `object[]` |
| `Held()` | `bool` |
| `Release()` | `void` |
| `Scan(ulong? low, ulong? high, ulong? limit)` | `SubEtha.Entry[]` |
| `ScanFrom(ulong? low, ulong? high, ulong? limit)` | `SubEtha.Scan` |

## SubEtha.MpmcConsumer

| Property | Type |
|---|---|
| `Index` | `ulong` |
| `Rings` | `ulong` |

| Method | Answers |
|---|---|
| `ApproxLen()` | `ulong` |
| `Pop()` | `object` |
| `PopMany(ulong maxItems)` | `object[]` |

## SubEtha.MpmcGrid

| Property | Type |
|---|---|
| `Consumers` | `SubEtha.MpmcConsumer[]` |
| `Producers` | `SubEtha.MpmcProducer[]` |

## SubEtha.MpmcProducer

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Index` | `ulong` |

| Method | Answers |
|---|---|
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |

## SubEtha.MpscConsumer

| Property | Type |
|---|---|
| `Producers` | `ulong` |

| Method | Answers |
|---|---|
| `ApproxLen()` | `ulong` |
| `Pop()` | `object` |
| `PopMany(ulong maxItems)` | `object[]` |

## SubEtha.MpscPool

| Property | Type |
|---|---|
| `Consumer` | `SubEtha.MpscConsumer` |
| `Producers` | `SubEtha.MpscProducer[]` |

## SubEtha.MpscProducer

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Index` | `ulong` |

| Method | Answers |
|---|---|
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |

## SubEtha.Neighbor

| Property | Type |
|---|---|
| `Edge` | `uint` |
| `Target` | `uint` |
| `Value` | `ulong` |

## SubEtha.Notifier

| Property | Type |
|---|---|
| `Index` | `uint` |
| `Native` | `ulong` |

| Method | Answers |
|---|---|
| `Drain()` | `void` |
| `IsSignaled()` | `bool` |
| `Wait(double? timeout)` | `bool` |

## SubEtha.NotifierSet

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Attach()` | `SubEtha.Notifier` |
| `Attached()` | `uint` |
| `Signal()` | `ulong` |

## SubEtha.OpCounts

| Property | Type |
|---|---|
| `Inserts` | `ulong` |
| `Lookups` | `ulong` |

## SubEtha.OrderedReceiver

| Method | Answers |
|---|---|
| `Corrections()` | `ulong` |
| `Drain(ulong? maxItems)` | `SubEtha.StampedItem[]` |
| `Flush()` | `SubEtha.StampedItem` |
| `FlushAll()` | `SubEtha.StampedItem[]` |
| `Recv()` | `SubEtha.StampedItem` |
| `RingShared()` | `bool` |
| `Strategy()` | `string` |

## SubEtha.OwnerLease

| Property | Type |
|---|---|
| `MaxValueBytes` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Beat(uint? pid)` | `bool` |
| `Flush()` | `void` |
| `FlushAsync()` | `void` |
| `HeldBy(uint? pid)` | `bool` |
| `Hold(ulong? graceEpochs, uint? pid)` | `SubEtha.LeaseHold` |
| `Owner()` | `uint?` |
| `Read(uint? pid)` | `object` |
| `Release(uint? pid)` | `bool` |
| `Term()` | `uint` |
| `TickEpoch()` | `ulong` |
| `TryAcquire(ulong? graceEpochs, uint? pid)` | `bool` |
| `Write(object value, uint? pid)` | `bool` |

## SubEtha.PackedItems

| Property | Type |
|---|---|
| `Bytes` | `object` |
| `Count` | `ulong` |

## SubEtha.Pair

| Property | Type |
|---|---|
| `Key` | `object` |
| `Value` | `object` |

## SubEtha.PathChanges

| Method | Answers |
|---|---|
| `Last()` | `SubEtha.PathMark` |
| `MarkedShare()` | `Single` |
| `Observe(byte ttl, byte congestionMark, byte hops)` | `void` |
| `RouteMovement()` | `Single` |

## SubEtha.PathMark

| Property | Type |
|---|---|
| `CongestionMark` | `byte` |
| `Hops` | `byte` |
| `Ttl` | `byte` |

## SubEtha.Periodicity

| Method | Answers |
|---|---|
| `Observe(double delayMicroseconds, ulong atMicroseconds)` | `void` |
| `Period()` | `SubEtha.Beat` |
| `SecondsToNext()` | `double?` |

## SubEtha.PermitHold

| Property | Type |
|---|---|
| `Held` | `bool` |

| Method | Answers |
|---|---|
| `Release()` | `void` |

## SubEtha.PubSub

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |
| `PayloadSize` | `ulong` |

| Method | Answers |
|---|---|
| `Head()` | `ulong` |
| `Publish(object item)` | `ulong` |
| `PublishMany(object[] items)` | `ulong?` |
| `ReadAt(ulong position)` | `object` |
| `Subscribe()` | `SubEtha.Subscriber` |
| `SubscribeFrom(ulong position)` | `SubEtha.Subscriber` |

## SubEtha.QosPolicy

| Method | Answers |
|---|---|
| `Durability()` | `SubEtha.Durability` |
| `KeepLast()` | `uint?` |
| `MaxLatency()` | `double` |
| `Ordering()` | `SubEtha.OrderingNeed` |
| `Reliability()` | `SubEtha.Reliability` |
| `SetDurability(SubEtha.Durability durability)` | `void` |
| `SetKeepLast(uint? keepLast)` | `void` |
| `SetMaxLatency(double seconds)` | `void` |
| `SetOrdering(SubEtha.OrderingNeed ordering)` | `void` |
| `SetReliability(SubEtha.Reliability reliability)` | `void` |
| `Snapshot()` | `SubEtha.QosSnapshot` |

## SubEtha.QosSnapshot

| Property | Type |
|---|---|
| `Durability` | `SubEtha.Durability` |
| `KeepLast` | `uint?` |
| `MaxLatency` | `double` |
| `Ordering` | `SubEtha.OrderingNeed` |
| `Reliability` | `SubEtha.Reliability` |

| Method | Answers |
|---|---|
| `RecommendsLocaleChange(SubEtha.Locale current)` | `SubEtha.Locale?` |
| `RecommendsOrderingChange(SubEtha.OrderingNeed current)` | `SubEtha.OrderingNeed?` |

## SubEtha.QuicBridgeClient

| Property | Type |
|---|---|
| `RingPath` | `string` |
| `Server` | `SubEtha.Endpoint` |
| `ServerName` | `string` |

| Method | Answers |
|---|---|
| `Run(ulong items)` | `void` |

## SubEtha.QuicBridgeServer

| Property | Type |
|---|---|
| `RingPath` | `string` |

| Method | Answers |
|---|---|
| `AcceptOne()` | `ulong` |
| `LocalAddr()` | `SubEtha.Endpoint` |

## SubEtha.RateLimiter

| Property | Type |
|---|---|
| `Capacity` | `uint` |
| `Path` | `string` |
| `RefillPerSecond` | `uint` |

| Method | Answers |
|---|---|
| `Available()` | `uint` |
| `Flush()` | `void` |
| `Reset()` | `void` |
| `TryAcquire(uint? n)` | `bool` |

## SubEtha.Region

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |
| `SlotSize` | `ulong` |

| Method | Answers |
|---|---|
| `Allocate(object value)` | `uint` |
| `Count()` | `ulong` |
| `Get(uint index)` | `object` |
| `Set(uint index, object value)` | `void` |
| `Snapshot()` | `object` |

## SubEtha.ReorderWindow

| Property | Type |
|---|---|
| `Cap` | `ulong` |
| `Floor` | `ulong` |

| Method | Answers |
|---|---|
| `Corrections()` | `ulong` |
| `Count()` | `ulong` |
| `Flush()` | `SubEtha.StampedItem` |
| `FlushAll()` | `SubEtha.StampedItem[]` |
| `Push(ulong stamp, object payload)` | `void` |
| `PushMany(ulong[] stamps, object[] payloads)` | `ulong` |
| `Take()` | `SubEtha.StampedItem` |
| `WidenTo(ulong window)` | `void` |
| `Window()` | `ulong` |

## SubEtha.Reservoir

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `MaxValueBytes` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Count()` | `ulong` |
| `Flush()` | `void` |
| `FlushAsync()` | `void` |
| `Record(object value)` | `ulong?` |
| `RecordMany(object[] values)` | `ulong` |
| `Reset()` | `void` |
| `Snapshot()` | `object[]` |
| `TotalSeen()` | `ulong` |

## SubEtha.Ring

| Property | Type |
|---|---|
| `MaxConsumers` | `ulong` |
| `MaxProducers` | `ulong` |
| `Path` | `string` |
| `Stamps` | `SubEtha.StampKind?` |

| Method | Answers |
|---|---|
| `ApproxLen()` | `ulong` |
| `Capacity()` | `ulong` |
| `IsEmpty()` | `bool` |
| `MorphRefusals()` | `ulong` |
| `OrderedReceiver(ulong consumer)` | `SubEtha.OrderedReceiver` |
| `Recv(ulong consumer)` | `object` |
| `RecvFrame(ulong consumer)` | `object` |
| `RecvMany(ulong consumer, ulong maxItems)` | `object[]` |
| `RegisterConsumer()` | `ulong` |
| `RegisterProducer()` | `ulong` |
| `Send(ulong producer, object item)` | `bool` |
| `SendFrame(ulong producer, object payload)` | `bool` |
| `SendMany(ulong producer, object[] items)` | `ulong` |
| `SendPacked(ulong producer, object data, ulong itemLen)` | `ulong` |
| `Shape()` | `string` |
| `Stamped()` | `bool` |
| `TotalCapacity()` | `ulong` |

## SubEtha.RoundTripShape

| Method | Answers |
|---|---|
| `Observe(double microseconds)` | `void` |
| `ObserveMany(double[] microseconds)` | `ulong` |
| `Samples()` | `ulong` |
| `TwoGroups()` | `double?` |
| `WirelessConfidence()` | `Single` |

## SubEtha.RWLock

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Read()` | `SubEtha.Hold` |
| `Readers()` | `uint` |
| `ReadFor(double timeout)` | `SubEtha.Hold` |
| `TryRead()` | `SubEtha.Hold` |
| `TryWrite()` | `SubEtha.Hold` |
| `Write()` | `SubEtha.Hold` |
| `WriteFor(double timeout)` | `SubEtha.Hold` |

## SubEtha.Scan

| Property | Type |
|---|---|
| `Entries` | `SubEtha.Entry[]` |
| `ResumeFrom` | `ulong?` |

## SubEtha.Semaphore

| Property | Type |
|---|---|
| `MaxPermits` | `uint` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Acquire()` | `SubEtha.PermitHold` |
| `AcquireFor(double timeout)` | `SubEtha.PermitHold` |
| `Available()` | `uint` |
| `TryAcquire()` | `SubEtha.PermitHold` |
| `Waiters()` | `uint` |

## SubEtha.SensReceiver

| Property | Type |
|---|---|
| `MaxItemSize` | `ulong` |

| Method | Answers |
|---|---|
| `Alive()` | `bool` |
| `Code()` | `SubEtha.SensCode` |
| `LocalAddr()` | `SubEtha.Endpoint` |
| `Poll()` | `object[]` |
| `PollFrom()` | `SubEtha.SourcedItem[]` |
| `SendFailures()` | `ulong` |
| `Switches()` | `ulong` |

## SubEtha.SensSender

| Property | Type |
|---|---|
| `MaxItemSize` | `ulong` |
| `Peer` | `SubEtha.Endpoint` |

| Method | Answers |
|---|---|
| `Code()` | `SubEtha.SensCode` |
| `DatagramsReceived()` | `ulong` |
| `DatagramsSent()` | `ulong` |
| `LocalAddr()` | `SubEtha.Endpoint` |
| `Loss()` | `double?` |
| `Send(object item)` | `void` |
| `SendMany(object[] items)` | `ulong` |
| `Switches()` | `ulong` |

## SubEtha.SharedArc

| Property | Type |
|---|---|
| `KeepOnLast` | `bool` |
| `Path` | `string` |
| `ValueSize` | `ulong` |

| Method | Answers |
|---|---|
| `Get()` | `object` |
| `Holders()` | `ulong` |
| `ReadAt(ulong offset, ulong length)` | `object` |
| `WriteAt(ulong offset, object value)` | `void` |

## SubEtha.SketchSize

| Property | Type |
|---|---|
| `Depth` | `uint` |
| `Width` | `uint` |

## SubEtha.Slab

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `ElementSize` | `ulong` |
| `Path` | `string` |
| `Writable` | `bool` |

| Method | Answers |
|---|---|
| `Flush()` | `void` |
| `Get(ulong index)` | `object` |
| `ReadRange(ulong start, ulong count)` | `object` |
| `Set(ulong index, object value)` | `void` |
| `SlotVersion(ulong index)` | `uint` |
| `WriteRange(ulong start, object data)` | `ulong` |

## SubEtha.SlabPin

| Method | Answers |
|---|---|
| `Epoch()` | `ulong` |
| `Get(ulong slot)` | `object` |
| `GetMany(ulong[] slots)` | `object[]` |
| `Held()` | `bool` |
| `Release()` | `void` |

## SubEtha.SlotVersion

| Property | Type |
|---|---|
| `Born` | `ulong` |
| `Bytes` | `object` |
| `Died` | `ulong?` |

## SubEtha.SourcedItem

| Property | Type |
|---|---|
| `Bytes` | `object` |
| `Source` | `ulong` |

## SubEtha.SpscRing

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |
| `PayloadSize` | `ulong` |

| Method | Answers |
|---|---|
| `Pop()` | `object` |
| `PopMany(ulong maxItems)` | `object[]` |
| `PopPacked(ulong maxItems)` | `SubEtha.PackedItems` |
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |
| `PushPacked(object data, ulong itemLen)` | `ulong` |

## SubEtha.Stack

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `ElementSize` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `ApproxLen()` | `ulong` |
| `Flush()` | `void` |
| `IsEmpty()` | `bool` |
| `Peek()` | `object` |
| `Pop()` | `object` |
| `PopMany(ulong maxItems)` | `object[]` |
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |

## SubEtha.StampedItem

| Property | Type |
|---|---|
| `Bytes` | `object` |
| `Stamp` | `ulong` |

## SubEtha.Subscriber

| Method | Answers |
|---|---|
| `Lag()` | `ulong` |
| `Next()` | `object` |
| `NextMany(ulong maxItems)` | `object[]` |
| `Position()` | `ulong` |

## SubEtha.TcpBridgeClient

| Property | Type |
|---|---|
| `RingPath` | `string` |
| `Server` | `SubEtha.Endpoint` |

| Method | Answers |
|---|---|
| `Run(ulong items)` | `void` |

## SubEtha.TcpBridgeServer

| Property | Type |
|---|---|
| `RingPath` | `string` |

| Method | Answers |
|---|---|
| `AcceptOne()` | `ulong` |
| `LocalAddr()` | `SubEtha.Endpoint` |

## SubEtha.TileEntry

| Property | Type |
|---|---|
| `Bytes` | `object` |
| `Lane` | `uint` |
| `Version` | `ulong` |

## SubEtha.TimePointTile

| Property | Type |
|---|---|
| `Lanes` | `ulong` |
| `MaxValueBytes` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `At(uint lane)` | `SubEtha.TileEntry` |
| `Count()` | `ulong` |
| `Flush()` | `void` |
| `FlushAsync()` | `void` |
| `Insert(ulong version, object value)` | `uint` |
| `IsFull()` | `bool` |
| `Remove(uint lane)` | `void` |
| `Visible(ulong version)` | `SubEtha.TileEntry[]` |
| `VisibleCount(ulong version)` | `uint` |
| `VisibleMask(ulong version)` | `ushort` |

## SubEtha.Timing

| Property | Type |
|---|---|
| `Window` | `ulong` |

| Method | Answers |
|---|---|
| `ClockSkew()` | `double` |
| `Jitter()` | `double` |
| `Observe(ulong sent, ulong received)` | `void` |
| `Samples()` | `ulong` |
| `Spacing()` | `double` |
| `Trend()` | `double` |
| `TrendDebiased()` | `double` |

## SubEtha.TinyBloom

| Property | Type |
|---|---|
| `Bits` | `ulong` |

| Method | Answers |
|---|---|
| `Contains(object key)` | `bool` |
| `ContainsMany(object[] keys)` | `bool[]` |
| `FalsePositiveRate(ulong keys)` | `double` |
| `Insert(object key)` | `void` |
| `InsertMany(object[] keys)` | `ulong` |
| `SetBits()` | `uint` |
| `SuggestedCapacity()` | `ulong` |

## SubEtha.TopologyMap

| Property | Type |
|---|---|
| `Participants` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `BroadcastRoot()` | `uint` |
| `BusiestReceiver()` | `SubEtha.FanCount` |
| `BusiestSender()` | `SubEtha.FanCount` |
| `FanIn(uint receiver)` | `uint` |
| `FanOut(uint sender)` | `uint` |
| `PublishedRecommendation()` | `SubEtha.Topology` |
| `PublishRecommendation()` | `SubEtha.Topology` |
| `Recommend()` | `SubEtha.Topology` |
| `RecommendationEpoch()` | `ulong` |
| `RecordMany(uint[] senders, uint[] receivers)` | `ulong` |
| `RecordSend(uint sender, uint receiver)` | `ulong` |
| `TotalSends()` | `ulong` |

## SubEtha.Tower

| Property | Type |
|---|---|
| `Depth` | `ulong` |
| `Path` | `string` |
| `ValueSize` | `ulong` |

| Method | Answers |
|---|---|
| `Append(object value)` | `uint[]` |
| `AppendMany(object[] values)` | `object[]` |
| `Count()` | `ulong` |
| `Flush()` | `void` |
| `Get(uint[] path)` | `object` |
| `GetMany(object[] paths)` | `object[]` |
| `InsertAtTop(uint top, object value)` | `uint[]` |

## SubEtha.Traffic

| Property | Type |
|---|---|
| `AverageBatch` | `ulong` |
| `BatchShare` | `double` |

## SubEtha.Universal

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Clear()` | `void` |
| `Contains(ulong value)` | `bool` |
| `ContainsMany(ulong[] values)` | `bool[]` |
| `Count()` | `ulong` |
| `Generation()` | `ushort` |
| `Insert(ulong value)` | `void` |
| `InsertMany(ulong[] values)` | `ulong` |
| `MigrateTo(SubEtha.SetStrategy strategy)` | `void` |
| `Migrations()` | `uint` |
| `OpCounts()` | `SubEtha.OpCounts` |
| `Snapshot()` | `ulong[]` |
| `Strategy()` | `SubEtha.SetStrategy` |

## SubEtha.Vec

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `ElementSize` | `ulong` |
| `Path` | `string` |
| `Writable` | `bool` |

| Method | Answers |
|---|---|
| `Clear()` | `void` |
| `Count()` | `ulong` |
| `Flush()` | `void` |
| `Get(ulong index)` | `object` |
| `Pop()` | `object` |
| `Push(object value)` | `ulong?` |
| `PushMany(object[] values)` | `ulong` |
| `ReadRange(ulong start, ulong count)` | `object` |
| `Set(ulong index, object value)` | `void` |
| `WriteRange(ulong start, object data)` | `ulong` |

## SubEtha.VersionChain

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `MaxValueBytes` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Clear()` | `void` |
| `Count()` | `ulong` |
| `Current()` | `SubEtha.Versioned` |
| `Flush()` | `void` |
| `FlushAsync()` | `void` |
| `Push(ulong version, object value)` | `void` |
| `ReadAt(ulong version)` | `object` |

## SubEtha.Versioned

| Property | Type |
|---|---|
| `Bytes` | `object` |
| `Version` | `ulong` |

## SubEtha.VersionedMap

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Count()` | `ulong` |
| `Flush()` | `void` |
| `Get(ulong key)` | `ulong?` |
| `GetMany(ulong[] keys)` | `object[]` |
| `Insert(ulong key, ulong value)` | `ulong?` |
| `InsertAt(ulong key, ulong value, ulong born)` | `ulong?` |
| `InsertMany(ulong[] keys, ulong[] values)` | `object[]` |
| `Pin()` | `SubEtha.MapPin` |
| `Remove(ulong key)` | `ulong?` |
| `RemoveAt(ulong key, ulong died)` | `ulong?` |
| `Sweep()` | `ulong` |
| `VoidEpoch(ulong epoch)` | `ulong` |

## SubEtha.VersionedSlab

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Depth` | `ulong` |
| `MaxValueBytes` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Flush()` | `void` |
| `Get(ulong slot)` | `object` |
| `History(ulong slot)` | `SubEtha.SlotVersion[]` |
| `Pin()` | `SubEtha.SlabPin` |
| `Retire(ulong slot)` | `object` |
| `RetireAt(ulong slot, ulong died)` | `object` |
| `Set(ulong slot, object value)` | `void` |
| `SetAt(ulong slot, object value, ulong born)` | `void` |
| `SweepSlot(ulong slot)` | `ulong` |
| `VoidEpoch(ulong epoch)` | `ulong` |

## SubEtha.WorkQueue

| Property | Type |
|---|---|
| `MaxItemSize` | `ulong` |
| `Path` | `string` |
| `Thief` | `bool` |

| Method | Answers |
|---|---|
| `Pop()` | `object` |
| `Push(object item)` | `bool` |
| `PushMany(object[] items)` | `ulong` |
| `Steal()` | `object` |
| `StealMany(ulong? maxItems)` | `object[]` |

