---
title: "Every cmdlet, in full"
weight: 10
---

# Every cmdlet, in full

Every one of the 135 cmdlets the module exports, with each
parameter, its type, whether it is required, and what the cmdlet
writes, followed by the properties and methods of whatever came
back. Generated from the built module by
`crates/subetha-pwrs/tools/Export-Reference.ps1`, so it cannot
disagree with the surface it describes.

For the shape of an actual returned value rather than its type,
see [What the values look like](../values/). In a shell,
`Get-Help <name> -Full` adds an example to any of these.

## Contents

**[The front door](#the-front-door)** (7) - [`New-SubEthaAdaptiveQueue`](#new-subethaadaptivequeue), [`New-SubEthaChannel`](#new-subethachannel), [`New-SubEthaKvMap`](#new-subethakvmap), [`New-SubEthaQosPolicy`](#new-subethaqospolicy), [`New-SubEthaWorkQueue`](#new-subethaworkqueue), [`Open-SubEthaChannel`](#open-subethachannel), [`Open-SubEthaWorkQueue`](#open-subethaworkqueue)

**[Rings and channels](#rings-and-channels)** (11) - [`New-SubEthaBroadcastRing`](#new-subethabroadcastring), [`New-SubEthaLamportPair`](#new-subethalamportpair), [`New-SubEthaMpmcGrid`](#new-subethampmcgrid), [`New-SubEthaMpscPool`](#new-subethampscpool), [`New-SubEthaPubSub`](#new-subethapubsub), [`New-SubEthaRing`](#new-subetharing), [`New-SubEthaSpscRing`](#new-subethaspscring), [`Open-SubEthaBroadcastRing`](#open-subethabroadcastring), [`Open-SubEthaPubSub`](#open-subethapubsub), [`Open-SubEthaRing`](#open-subetharing), [`Open-SubEthaSpscRing`](#open-subethaspscring)

**[Rings that change themselves](#rings-that-change-themselves)** (4) - [`New-SubEthaCapacityRing`](#new-subethacapacityring), [`New-SubEthaLocaleRing`](#new-subethalocalering), [`Open-SubEthaCapacityRing`](#open-subethacapacityring), [`Open-SubEthaLocaleRing`](#open-subethalocalering)

**[Order](#order)** (1) - [`New-SubEthaReorderWindow`](#new-subethareorderwindow)

**[Shared state](#shared-state)** (40) - [`New-SubEthaArena`](#new-subethaarena), [`New-SubEthaAtomic`](#new-subethaatomic), [`New-SubEthaBitVec`](#new-subethabitvec), [`New-SubEthaBTreeMap`](#new-subethabtreemap), [`New-SubEthaCell`](#new-subethacell), [`New-SubEthaDeque`](#new-subethadeque), [`New-SubEthaFrameRegion`](#new-subethaframeregion), [`New-SubEthaGraph`](#new-subethagraph), [`New-SubEthaHandleTable`](#new-subethahandletable), [`New-SubEthaHashMap`](#new-subethahashmap), [`New-SubEthaLazyValue`](#new-subethalazyvalue), [`New-SubEthaLinkedList`](#new-subethalinkedlist), [`New-SubEthaRegion`](#new-subetharegion), [`New-SubEthaSharedArc`](#new-subethasharedarc), [`New-SubEthaSlab`](#new-subethaslab), [`New-SubEthaStack`](#new-subethastack), [`New-SubEthaTopologyMap`](#new-subethatopologymap), [`New-SubEthaTower`](#new-subethatower), [`New-SubEthaUniversal`](#new-subethauniversal), [`New-SubEthaVec`](#new-subethavec), [`Open-SubEthaArena`](#open-subethaarena), [`Open-SubEthaAtomic`](#open-subethaatomic), [`Open-SubEthaBitVec`](#open-subethabitvec), [`Open-SubEthaBTreeMap`](#open-subethabtreemap), [`Open-SubEthaCell`](#open-subethacell), [`Open-SubEthaDeque`](#open-subethadeque), [`Open-SubEthaFrameRegion`](#open-subethaframeregion), [`Open-SubEthaGraph`](#open-subethagraph), [`Open-SubEthaHandleTable`](#open-subethahandletable), [`Open-SubEthaHashMap`](#open-subethahashmap), [`Open-SubEthaLazyValue`](#open-subethalazyvalue), [`Open-SubEthaLinkedList`](#open-subethalinkedlist), [`Open-SubEthaRegion`](#open-subetharegion), [`Open-SubEthaSharedArc`](#open-subethasharedarc), [`Open-SubEthaSlab`](#open-subethaslab), [`Open-SubEthaStack`](#open-subethastack), [`Open-SubEthaTopologyMap`](#open-subethatopologymap), [`Open-SubEthaTower`](#open-subethatower), [`Open-SubEthaUniversal`](#open-subethauniversal), [`Open-SubEthaVec`](#open-subethavec)

**[State with a history](#state-with-a-history)** (12) - [`New-SubEthaEpochs`](#new-subethaepochs), [`New-SubEthaLanedMap`](#new-subethalanedmap), [`New-SubEthaTimePointTile`](#new-subethatimepointtile), [`New-SubEthaVersionChain`](#new-subethaversionchain), [`New-SubEthaVersionedMap`](#new-subethaversionedmap), [`New-SubEthaVersionedSlab`](#new-subethaversionedslab), [`Open-SubEthaEpochs`](#open-subethaepochs), [`Open-SubEthaLanedMap`](#open-subethalanedmap), [`Open-SubEthaTimePointTile`](#open-subethatimepointtile), [`Open-SubEthaVersionChain`](#open-subethaversionchain), [`Open-SubEthaVersionedMap`](#open-subethaversionedmap), [`Open-SubEthaVersionedSlab`](#open-subethaversionedslab)

**[Coordination](#coordination)** (20) - [`New-SubEthaCondvar`](#new-subethacondvar), [`New-SubEthaEpochBarrier`](#new-subethaepochbarrier), [`New-SubEthaFenceClock`](#new-subethafenceclock), [`New-SubEthaHeartbeat`](#new-subethaheartbeat), [`New-SubEthaHolderTable`](#new-subethaholdertable), [`New-SubEthaLeaderElection`](#new-subethaleaderelection), [`New-SubEthaNotifierSet`](#new-subethanotifierset), [`New-SubEthaOwnerLease`](#new-subethaownerlease), [`New-SubEthaRWLock`](#new-subetharwlock), [`New-SubEthaSemaphore`](#new-subethasemaphore), [`Open-SubEthaCondvar`](#open-subethacondvar), [`Open-SubEthaEpochBarrier`](#open-subethaepochbarrier), [`Open-SubEthaFenceClock`](#open-subethafenceclock), [`Open-SubEthaHeartbeat`](#open-subethaheartbeat), [`Open-SubEthaHolderTable`](#open-subethaholdertable), [`Open-SubEthaLeaderElection`](#open-subethaleaderelection), [`Open-SubEthaOwnerLease`](#open-subethaownerlease), [`Open-SubEthaRWLock`](#open-subetharwlock), [`Open-SubEthaSemaphore`](#open-subethasemaphore), [`Wait-SubEthaCondition`](#wait-subethacondition)

**[Probabilistic](#probabilistic)** (20) - [`Measure-SubEthaBloomSize`](#measure-subethabloomsize), [`Measure-SubEthaSketchSize`](#measure-subethasketchsize), [`New-SubEthaBlockedBloomFilter`](#new-subethablockedbloomfilter), [`New-SubEthaBloomFilter`](#new-subethabloomfilter), [`New-SubEthaCountMinSketch`](#new-subethacountminsketch), [`New-SubEthaFineBloom`](#new-subethafinebloom), [`New-SubEthaHistogram`](#new-subethahistogram), [`New-SubEthaHyperLogLog`](#new-subethahyperloglog), [`New-SubEthaLruCache`](#new-subethalrucache), [`New-SubEthaRateLimiter`](#new-subetharatelimiter), [`New-SubEthaReservoir`](#new-subethareservoir), [`New-SubEthaTinyBloom`](#new-subethatinybloom), [`Open-SubEthaBlockedBloomFilter`](#open-subethablockedbloomfilter), [`Open-SubEthaBloomFilter`](#open-subethabloomfilter), [`Open-SubEthaCountMinSketch`](#open-subethacountminsketch), [`Open-SubEthaHistogram`](#open-subethahistogram), [`Open-SubEthaHyperLogLog`](#open-subethahyperloglog), [`Open-SubEthaLruCache`](#open-subethalrucache), [`Open-SubEthaRateLimiter`](#open-subetharatelimiter), [`Open-SubEthaReservoir`](#open-subethareservoir)

**[Clocks](#clocks)** (2) - [`New-SubEthaCausalClock`](#new-subethacausalclock), [`New-SubEthaClock`](#new-subethaclock)

**[Sensing](#sensing)** (10) - [`New-SubEthaCapacity`](#new-subethacapacity), [`New-SubEthaForecast`](#new-subethaforecast), [`New-SubEthaLossBursts`](#new-subethalossbursts), [`New-SubEthaLossKind`](#new-subethalosskind), [`New-SubEthaPathChanges`](#new-subethapathchanges), [`New-SubEthaPeriodicity`](#new-subethaperiodicity), [`New-SubEthaRoundTripShape`](#new-subetharoundtripshape), [`New-SubEthaSensReceiver`](#new-subethasensreceiver), [`New-SubEthaSensSender`](#new-subethasenssender), [`New-SubEthaTiming`](#new-subethatiming)

**[Bridges](#bridges)** (6) - [`Get-SubEthaTransport`](#get-subethatransport), [`New-SubEthaQuicBridgeClient`](#new-subethaquicbridgeclient), [`New-SubEthaQuicBridgeServer`](#new-subethaquicbridgeserver), [`New-SubEthaSelfSignedCert`](#new-subethaselfsignedcert), [`New-SubEthaTcpBridgeClient`](#new-subethatcpbridgeclient), [`New-SubEthaTcpBridgeServer`](#new-subethatcpbridgeserver)

**[The pipeline](#the-pipeline)** (2) - [`Receive-SubEthaItem`](#receive-subethaitem), [`Send-SubEthaItem`](#send-subethaitem)

## The front door

### New-SubEthaAdaptiveQueue

Creates the adaptive queue at Path with Capacity items in flight (1024 when absent), starting as a ring for Senders and Readers and changing shape as the traffic says.

Also `New-SEAdaptiveQueue`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | - | named | - |
| `Senders` | `ulong` | - | named | - |
| `Readers` | `ulong` | - | named | - |
| `Ordering` | `SubEtha.OrderingNeed` | - | named | - |
| `AutoOrder` | `double` | - | named | - |

**Writes** `SubEtha.AdaptiveQueue`.

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

### New-SubEthaChannel

Obtains the channel at Path with Capacity items in flight (1024 when absent, rounded up to a power of two), creating it for Senders and Readers when the file does not exist.

Also `New-SEChannel`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | - | named | - |
| `Senders` | `ulong` | - | named | - |
| `Readers` | `ulong` | - | named | - |

**Writes** `SubEtha.Channel`.

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

### New-SubEthaKvMap

Creates the map at Path with Capacity entries (1024 when absent), shaped for Readers and Writers.

Also `New-SEKvMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | - | named | - |
| `Readers` | `ulong` | - | named | - |
| `Writers` | `ulong` | - | named | - |

**Writes** `SubEtha.KvMap`.

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

### New-SubEthaQosPolicy

Builds a policy from its settings, or from a Preset with any of the settings overriding it.

Also `New-SEQosPolicy`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Preset` | `SubEtha.QosPreset` | - | named | - |
| `Durability` | `SubEtha.Durability` | - | named | - |
| `Reliability` | `SubEtha.Reliability` | - | named | - |
| `KeepLast` | `uint` | - | named | - |
| `KeepAll` | `switch` | - | named | - |
| `MaxLatency` | `double` | - | named | - |
| `Ordering` | `SubEtha.OrderingNeed` | - | named | - |

**Writes** `SubEtha.QosPolicy`.

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

### New-SubEthaWorkQueue

Creates the work queue at Path as its owner, with Capacity items in flight (1024 when absent) and Thieves expected to take from it.

Also `New-SEWorkQueue`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | - | named | - |
| `Thieves` | `ulong` | - | named | - |

**Writes** `SubEtha.WorkQueue`.

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

### Open-SubEthaChannel

Attaches to the channel at Path, which must exist with the Capacity it was created with.

Also `Open-SEChannel`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | - | named | - |

**Writes** `SubEtha.Channel`.

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

### Open-SubEthaWorkQueue

Attaches to the work queue at Path as a thief, to take from it.

Also `Open-SEWorkQueue`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.WorkQueue`.

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

## Rings and channels

### New-SubEthaBroadcastRing

Obtains the broadcast ring at Path holding Capacity slots, creating it when the file does not exist.

Also `New-SEBroadcastRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.BroadcastRing`.

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

### New-SubEthaLamportPair

Makes a Lamport pair at Path, one producer and one consumer over one ring of Capacity slots, and writes the two in that order. Exactly one of each may exist per ring: within a process the types enforce that, and across processes it is the caller's undertaking, which is why the pair is handed out rather than opened twice.

Also `New-SELamportPair`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `Open` | `switch` | - | named | - |

**Writes** `SubEtha.LamportConsumer`, `SubEtha.LamportProducer`.

On the `SubEtha.LamportConsumer`:

| Property | Type |
|---|---|
| `Capacity` | `ulong` |
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Pop()` | `object` |
| `PopMany(ulong maxItems)` | `object[]` |
| `PopPacked(ulong maxItems)` | `SubEtha.PackedItems` |

On the `SubEtha.LamportProducer`:

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

### New-SubEthaMpmcGrid

Makes a grid at Path of Producers rings of Capacity slots shared out among Consumers, and writes the producers and the consumers together. Open attaches to a grid that exists with the shape it was built at.

Also `New-SEMpmcGrid`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Producers` | `ulong` | yes | 1 | - |
| `Consumers` | `ulong` | yes | 2 | - |
| `Capacity` | `ulong` | yes | 3 | - |
| `Open` | `switch` | - | named | - |

**Writes** `SubEtha.MpmcGrid`.

| Property | Type |
|---|---|
| `Consumers` | `SubEtha.MpmcConsumer[]` |
| `Producers` | `SubEtha.MpmcProducer[]` |

### New-SubEthaMpscPool

Makes a pool at Path of Producers rings of Capacity slots, drained by one consumer, and writes the producers and the consumer together. Open attaches to a pool that exists with the shape it was built at.

Also `New-SEMpscPool`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Producers` | `ulong` | yes | 1 | - |
| `Capacity` | `ulong` | yes | 2 | - |
| `Open` | `switch` | - | named | - |

**Writes** `SubEtha.MpscPool`.

| Property | Type |
|---|---|
| `Consumer` | `SubEtha.MpscConsumer` |
| `Producers` | `SubEtha.MpscProducer[]` |

### New-SubEthaPubSub

Obtains the publish/subscribe ring at Path keeping Capacity items, creating it when the file does not exist.

Also `New-SEPubSub`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.PubSub`.

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

### New-SubEthaRing

Obtains the adaptive ring at Path holding Capacity slots, creating it when the file does not exist.

Also `New-SERing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |
| `Stamps` | `SubEtha.StampKind` | - | named | - |

**Writes** `SubEtha.Ring`.

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

### New-SubEthaSpscRing

Obtains the single-producer ring at Path holding Capacity slots, creating it when the file does not exist.

Also `New-SESpscRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.SpscRing`.

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

### Open-SubEthaBroadcastRing

Attaches to the broadcast ring at Path, which must exist with the Capacity it was created with.

Also `Open-SEBroadcastRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.BroadcastRing`.

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

### Open-SubEthaPubSub

Attaches to the publish/subscribe ring at Path, which must exist with the Capacity it was created with.

Also `Open-SEPubSub`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.PubSub`.

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

### Open-SubEthaRing

Attaches to the adaptive ring at Path, which must exist with the capacity, counts and stamps it was created with.

Also `Open-SERing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |
| `Stamps` | `SubEtha.StampKind` | - | named | - |

**Writes** `SubEtha.Ring`.

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

### Open-SubEthaSpscRing

Attaches to the single-producer ring at Path, which must exist with the Capacity it was created with.

Also `Open-SESpscRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.SpscRing`.

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

## Rings that change themselves

### New-SubEthaCapacityRing

Obtains the resizable ring at Path holding Capacity slots, a power of two, creating it when the file does not exist.

Also `New-SECapacityRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |
| `Stamped` | `switch` | - | named | - |

**Writes** `SubEtha.CapacityRing`.

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

### New-SubEthaLocaleRing

Obtains the relocatable ring at Path holding Capacity slots, a power of two, creating it when the file does not exist. All three backings are built, so a later migration has somewhere to go without allocating.

Also `New-SELocaleRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |
| `Stamped` | `switch` | - | named | - |

**Writes** `SubEtha.LocaleRing`.

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

### Open-SubEthaCapacityRing

Attaches to the resizable ring at Path, which must exist with the capacity, counts and stamping it was created with.

Also `Open-SECapacityRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |
| `Stamped` | `switch` | - | named | - |

**Writes** `SubEtha.CapacityRing`.

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

### Open-SubEthaLocaleRing

Attaches to the relocatable ring at Path, which must exist with the capacity, counts and stamping it was created with.

Also `Open-SELocaleRing`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |
| `Stamped` | `switch` | - | named | - |

**Writes** `SubEtha.LocaleRing`.

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

## Order

### New-SubEthaReorderWindow

Builds a reorder window. Floor is the window to start at and Cap the widest it may grow to; a window at least as wide as the number of senders puts every item back in order.

Also `New-SEReorderWindow`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Floor` | `ulong` | - | named | - |
| `Cap` | `ulong` | - | named | - |

**Writes** `SubEtha.ReorderWindow`.

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

## Shared state

### New-SubEthaArena

Obtains the arena at Path holding CapacityBytes, creating it when the file does not exist.

Also `New-SEArena`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `CapacityBytes` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Arena`.

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

### New-SubEthaAtomic

Obtains the atomic at Path, creating it holding Init when the file does not exist and attaching to its live value when it does.

Also `New-SEAtomic`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Init` | `ulong` | - | named | - |

**Writes** `SubEtha.Atomic`.

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

### New-SubEthaBitVec

Obtains the bit vector at Path holding CapacityBits bits, creating it cleared when the file does not exist.

Also `New-SEBitVec`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `CapacityBits` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.BitVec`.

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

### New-SubEthaBTreeMap

Obtains the ordered map at Path holding Capacity entries of KeySize-byte keys and ValueSize-byte values, creating it when the file does not exist.

Also `New-SEBTreeMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `KeySize` | `ulong` | yes | 2 | - |
| `ValueSize` | `ulong` | yes | 3 | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.BTreeMap`.

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

### New-SubEthaCell

Obtains the cell at Path holding a value of ValueSize bytes, creating it when the file does not exist.

Also `New-SECell`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `ValueSize` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Cell`.

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

### New-SubEthaDeque

Obtains the deque at Path as its owner, holding Capacity items, a power of two, of ElementSize bytes, creating it when the file does not exist.

Also `New-SEDeque`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Deque`.

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

### New-SubEthaFrameRegion

Obtains the frame region at Path holding BlockCount blocks of BlockSize bytes, creating it when the file does not exist.

Also `New-SEFrameRegion`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `BlockSize` | `ulong` | yes | 1 | - |
| `BlockCount` | `ulong` | yes | 2 | - |

**Writes** `SubEtha.FrameRegion`.

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

### New-SubEthaGraph

Obtains the graph beside Path holding up to MaxNodes nodes and MaxEdges edges, creating it when the files do not exist.

Also `New-SEGraph`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `MaxNodes` | `ulong` | yes | 1 | - |
| `MaxEdges` | `ulong` | yes | 2 | - |

**Writes** `SubEtha.Graph`.

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

### New-SubEthaHandleTable

Obtains the handle table at Path holding Capacity values, creating it when the file does not exist; with Reset, empties it and remakes it at that capacity.

Also `New-SEHandleTable`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `Reset` | `switch` | - | named | - |

**Writes** `SubEtha.HandleTable`.

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

### New-SubEthaHashMap

Obtains the hash map at Path holding Capacity entries of KeySize-byte keys and ValueSize-byte values, creating it when the file does not exist.

Also `New-SEHashMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `KeySize` | `ulong` | yes | 2 | - |
| `ValueSize` | `ulong` | yes | 3 | - |

**Writes** `SubEtha.HashMap`.

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

### New-SubEthaLazyValue

Obtains the lazy value at Path holding ValueSize bytes, creating it unpublished when the file does not exist.

Also `New-SELazyValue`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `ValueSize` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.LazyValue`.

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

### New-SubEthaLinkedList

Obtains the list at Path holding up to Capacity elements of ElementSize bytes, creating it when the file does not exist.

Also `New-SELinkedList`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.LinkedList`.

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

### New-SubEthaRegion

Obtains the region at Path holding Capacity slots of SlotSize bytes, creating it when the file does not exist.

Also `New-SERegion`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `SlotSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Region`.

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

### New-SubEthaSharedArc

Obtains the shared value at Path, creating it holding Value when the file does not exist.

Also `New-SESharedArc`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Value` | `object` | yes | 1 | - |
| `MaxHolders` | `ulong` | - | named | - |
| `KeepOnLast` | `switch` | - | named | - |

**Writes** `SubEtha.SharedArc`.

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

### New-SubEthaSlab

Obtains the slab at Path holding Capacity slots of ElementSize bytes, creating it when the file does not exist.

Also `New-SESlab`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Slab`.

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

### New-SubEthaStack

Obtains the stack at Path holding up to Capacity items of ElementSize bytes, creating it when the file does not exist.

Also `New-SEStack`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Stack`.

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

### New-SubEthaTopologyMap

Obtains the topology map at Path for Participants, creating it when the file does not exist; with Reset, empties the counts and remakes it. The thresholds are how many different places a participant has to reach, or be reached from, before the shape counts as one to many or many to one.

Also `New-SETopologyMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Participants` | `ulong` | yes | 1 | - |
| `FanOutThreshold` | `uint` | - | named | - |
| `FanInThreshold` | `uint` | - | named | - |
| `Reset` | `switch` | - | named | - |

**Writes** `SubEtha.TopologyMap`.

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

### New-SubEthaTower

Obtains the tower whose bottom level lives at Path holding Capacity values of ValueSize bytes, with the levels above it at LevelPath holding LevelCapacity places each, given top first; creating them when the files do not exist. With no levels the tower is one deep, which is a plain region reached by a path of one number.

Also `New-SETower`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ValueSize` | `ulong` | yes | 2 | - |
| `LevelPath` | `string[]` | - | named | - |
| `LevelCapacity` | `ulong[]` | - | named | - |

**Writes** `SubEtha.Tower`.

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

### New-SubEthaUniversal

Obtains the self-storing set beside Path holding Capacity values, creating it when the files do not exist; with Reset, empties it and remakes it at that capacity.

Also `New-SEUniversal`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `Reset` | `switch` | - | named | - |

**Writes** `SubEtha.Universal`.

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

### New-SubEthaVec

Obtains the vector at Path holding up to Capacity elements of ElementSize bytes, creating it when the file does not exist.

Also `New-SEVec`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Vec`.

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

### Open-SubEthaArena

Attaches to the arena at Path, which must exist with the CapacityBytes it was created with; ReadOnly opens it to resolve without interning.

Also `Open-SEArena`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `CapacityBytes` | `ulong` | yes | 1 | - |
| `ReadOnly` | `switch` | - | named | - |

**Writes** `SubEtha.Arena`.

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

### Open-SubEthaAtomic

Attaches to the atomic at Path, which must exist, leaving its value alone.

Also `Open-SEAtomic`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.Atomic`.

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

### Open-SubEthaBitVec

Attaches to the bit vector at Path, which must exist holding CapacityBits bits.

Also `Open-SEBitVec`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `CapacityBits` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.BitVec`.

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

### Open-SubEthaBTreeMap

Attaches to the ordered map at Path, which must exist with the capacity and sizes it was created with.

Also `Open-SEBTreeMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `KeySize` | `ulong` | yes | 2 | - |
| `ValueSize` | `ulong` | yes | 3 | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.BTreeMap`.

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

### Open-SubEthaCell

Attaches to the cell at Path, which must exist with the same value size it was created with.

Also `Open-SECell`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `ValueSize` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Cell`.

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

### Open-SubEthaDeque

Attaches to the deque at Path as a thief: the handle steals from the deque another process owns, and neither pushes nor pops. No capacity is asked for, because the file states it.

Also `Open-SEDeque`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `ElementSize` | `ulong` | yes | 1 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Deque`.

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

### Open-SubEthaFrameRegion

Attaches to the frame region at Path, which must exist with the block size and count it was created with.

Also `Open-SEFrameRegion`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `BlockSize` | `ulong` | yes | 1 | - |
| `BlockCount` | `ulong` | yes | 2 | - |

**Writes** `SubEtha.FrameRegion`.

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

### Open-SubEthaGraph

Attaches to the graph beside Path, which must exist with the sizes it was made with.

Also `Open-SEGraph`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `MaxNodes` | `ulong` | yes | 1 | - |
| `MaxEdges` | `ulong` | yes | 2 | - |

**Writes** `SubEtha.Graph`.

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

### Open-SubEthaHandleTable

Attaches to the handle table at Path, which must exist with the Capacity it was made with.

Also `Open-SEHandleTable`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.HandleTable`.

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

### Open-SubEthaHashMap

Attaches to the hash map at Path, which must exist with the capacity and sizes it was created with.

Also `Open-SEHashMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `KeySize` | `ulong` | yes | 2 | - |
| `ValueSize` | `ulong` | yes | 3 | - |

**Writes** `SubEtha.HashMap`.

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

### Open-SubEthaLazyValue

Attaches to the lazy value at Path, which must exist holding ValueSize bytes.

Also `Open-SELazyValue`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `ValueSize` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.LazyValue`.

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

### Open-SubEthaLinkedList

Attaches to the list at Path, which must exist with the capacity and layout it was created with.

Also `Open-SELinkedList`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.LinkedList`.

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

### Open-SubEthaRegion

Attaches to the region at Path, which must exist with the same capacity and layout it was created with.

Also `Open-SERegion`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `SlotSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Region`.

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

### Open-SubEthaSharedArc

Attaches to the shared value at Path, which must exist holding ValueSize bytes.

Also `Open-SESharedArc`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `ValueSize` | `ulong` | yes | 1 | - |
| `MaxHolders` | `ulong` | - | named | - |
| `KeepOnLast` | `switch` | - | named | - |

**Writes** `SubEtha.SharedArc`.

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

### Open-SubEthaSlab

Attaches to the slab at Path, which must exist with the capacity and layout it was created with; ReadOnly opens it without write access.

Also `Open-SESlab`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |
| `ReadOnly` | `switch` | - | named | - |

**Writes** `SubEtha.Slab`.

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

### Open-SubEthaStack

Attaches to the stack at Path, which must exist with the capacity and layout it was created with.

Also `Open-SEStack`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Stack`.

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

### Open-SubEthaTopologyMap

Attaches to the topology map at Path, which must exist with the Participants it was made with.

Also `Open-SETopologyMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Participants` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.TopologyMap`.

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

### Open-SubEthaTower

Attaches to the tower at Path, which must exist with the shape it was made with.

Also `Open-SETower`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ValueSize` | `ulong` | yes | 2 | - |
| `LevelPath` | `string[]` | - | named | - |
| `LevelCapacity` | `ulong[]` | - | named | - |

**Writes** `SubEtha.Tower`.

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

### Open-SubEthaUniversal

Attaches to the self-storing set beside Path, which must exist with the Capacity it was made with.

Also `Open-SEUniversal`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Universal`.

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

### Open-SubEthaVec

Attaches to the vector at Path, which must exist with the same capacity and layout it was created with.

Also `Open-SEVec`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ElementSize` | `ulong` | yes | 2 | - |
| `Alignment` | `ulong` | - | named | - |
| `Tag` | `ulong` | - | named | - |

**Writes** `SubEtha.Vec`.

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

## State with a history

### New-SubEthaEpochs

Obtains the epochs at Path holding Capacity tickets, creating them when the file does not exist.

Also `New-SEEpochs`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Epochs`.

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

### New-SubEthaLanedMap

Obtains the laned map in Directory with Lanes lanes of NodesPerLane entries, creating it when the directory does not hold one.

Also `New-SELanedMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Directory` | `string` | yes | 0 | - |
| `Lanes` | `ulong` | - | named | - |
| `NodesPerLane` | `ulong` | - | named | - |
| `MaxPins` | `ulong` | - | named | - |

**Writes** `SubEtha.LanedMap`.

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

### New-SubEthaTimePointTile

Obtains the time-point tile at Path, creating it when the file does not exist; with Reset, empties it and remakes it.

Also `New-SETimePointTile`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Reset` | `switch` | - | named | - |

**Writes** `SubEtha.TimePointTile`.

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

### New-SubEthaVersionChain

Obtains the version chain at Path holding Capacity versions, creating it when the file does not exist; with Reset, empties it and remakes it at that capacity.

Also `New-SEVersionChain`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `Reset` | `switch` | - | named | - |

**Writes** `SubEtha.VersionChain`.

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

### New-SubEthaVersionedMap

Obtains the versioned map at Path holding Capacity entries, with its epochs at EpochsPath, creating both when the files do not exist.

Also `New-SEVersionedMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `EpochsPath` | `string` | yes | 2 | - |
| `MaxPins` | `ulong` | - | named | - |

**Writes** `SubEtha.VersionedMap`.

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

### New-SubEthaVersionedSlab

Obtains the versioned slab at Path holding Capacity slots, with its epochs at EpochsPath, creating both when the files do not exist.

Also `New-SEVersionedSlab`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `EpochsPath` | `string` | yes | 2 | - |
| `MaxPins` | `ulong` | - | named | - |

**Writes** `SubEtha.VersionedSlab`.

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

### Open-SubEthaEpochs

Attaches to the epochs at Path, which must exist with the Capacity they were created with.

Also `Open-SEEpochs`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Epochs`.

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

### Open-SubEthaLanedMap

Attaches to the laned map in Directory, which must exist with the lane count, lane size and pin count it was made with.

Also `Open-SELanedMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Directory` | `string` | yes | 0 | - |
| `Lanes` | `ulong` | - | named | - |
| `NodesPerLane` | `ulong` | - | named | - |
| `MaxPins` | `ulong` | - | named | - |

**Writes** `SubEtha.LanedMap`.

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

### Open-SubEthaTimePointTile

Attaches to the time-point tile at Path, which must exist.

Also `Open-SETimePointTile`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.TimePointTile`.

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

### Open-SubEthaVersionChain

Attaches to the version chain at Path, which must exist with the Capacity it was made with.

Also `Open-SEVersionChain`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.VersionChain`.

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

### Open-SubEthaVersionedMap

Attaches to the versioned map at Path, which must exist with the capacity and pin count it was made with.

Also `Open-SEVersionedMap`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `EpochsPath` | `string` | yes | 2 | - |
| `MaxPins` | `ulong` | - | named | - |

**Writes** `SubEtha.VersionedMap`.

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

### Open-SubEthaVersionedSlab

Attaches to the versioned slab at Path, which must exist with the capacity and pin count it was made with.

Also `Open-SEVersionedSlab`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `EpochsPath` | `string` | yes | 2 | - |
| `MaxPins` | `ulong` | - | named | - |

**Writes** `SubEtha.VersionedSlab`.

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

## Coordination

### New-SubEthaCondvar

Obtains the condition at Path, creating it when the file does not exist.

Also `New-SECondvar`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.Condvar`.

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Generation()` | `ulong` |
| `NotifyAll()` | `ulong` |
| `NotifyOne()` | `ulong` |

### New-SubEthaEpochBarrier

Obtains the barrier at Path over the heartbeat table at HeartbeatPath, creating it when the file does not exist.

Also `New-SEEpochBarrier`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `HeartbeatPath` | `string` | yes | 1 | - |
| `Capacity` | `ulong` | yes | 2 | - |
| `GraceEpochs` | `ulong` | - | named | - |

**Writes** `SubEtha.EpochBarrier`.

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

### New-SubEthaFenceClock

Obtains the fence clock at Path holding Capacity participants, creating it when the file does not exist.

Also `New-SEFenceClock`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.FenceClock`.

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

### New-SubEthaHeartbeat

Obtains the heartbeat table at Path holding Capacity slots, creating it when the file does not exist.

Also `New-SEHeartbeat`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Heartbeat`.

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

### New-SubEthaHolderTable

Obtains the holder table at Path holding Capacity slots, creating it when the file does not exist.

Also `New-SEHolderTable`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.HolderTable`.

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

### New-SubEthaLeaderElection

Obtains the election at Path, creating it with no leader when the file does not exist.

Also `New-SELeaderElection`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.LeaderElection`.

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

### New-SubEthaNotifierSet

Obtains the notifier set at Path, creating it when the file does not exist.

Also `New-SENotifierSet`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.NotifierSet`.

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Attach()` | `SubEtha.Notifier` |
| `Attached()` | `uint` |
| `Signal()` | `ulong` |

### New-SubEthaOwnerLease

Obtains the lease at Path, creating it with Value when the file does not exist; attaching to one that exists leaves its owner, its term and its value alone. With Reset, strips the lease back to no owner and Value, which throws away a claim another process may still believe it has, so it is for a lease known to be wedged.

Also `New-SEOwnerLease`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Value` | `object` | - | named | - |
| `Reset` | `switch` | - | named | - |

**Writes** `SubEtha.OwnerLease`.

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

### New-SubEthaRWLock

Obtains the lock at Path, creating it when the file does not exist.

Also `New-SERWLock`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.RWLock`.

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

### New-SubEthaSemaphore

Obtains the semaphore at Path holding Initial permits of at most MaxPermits (Initial when absent), creating it when the file does not exist.

Also `New-SESemaphore`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Initial` | `uint` | yes | 1 | - |
| `MaxPermits` | `uint` | - | named | - |

**Writes** `SubEtha.Semaphore`.

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

### Open-SubEthaCondvar

Attaches to the condition at Path, which must exist.

Also `Open-SECondvar`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.Condvar`.

| Property | Type |
|---|---|
| `Path` | `string` |

| Method | Answers |
|---|---|
| `Generation()` | `ulong` |
| `NotifyAll()` | `ulong` |
| `NotifyOne()` | `ulong` |

### Open-SubEthaEpochBarrier

Attaches to the barrier at Path, which must exist, over the heartbeat table at HeartbeatPath.

Also `Open-SEEpochBarrier`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `HeartbeatPath` | `string` | yes | 1 | - |
| `Capacity` | `ulong` | yes | 2 | - |
| `GraceEpochs` | `ulong` | - | named | - |

**Writes** `SubEtha.EpochBarrier`.

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

### Open-SubEthaFenceClock

Attaches to the fence clock at Path, which must exist with the Capacity it was created with.

Also `Open-SEFenceClock`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.FenceClock`.

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

### Open-SubEthaHeartbeat

Attaches to the heartbeat table at Path, which must exist with the Capacity it was created with.

Also `Open-SEHeartbeat`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Heartbeat`.

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

### Open-SubEthaHolderTable

Attaches to the holder table at Path, which must exist with the Capacity it was created with.

Also `Open-SEHolderTable`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.HolderTable`.

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

### Open-SubEthaLeaderElection

Attaches to the election at Path, which must exist.

Also `Open-SELeaderElection`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.LeaderElection`.

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

### Open-SubEthaOwnerLease

Attaches to the lease at Path, which must exist.

Also `Open-SEOwnerLease`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.OwnerLease`.

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

### Open-SubEthaRWLock

Attaches to the lock at Path, which must exist.

Also `Open-SERWLock`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |

**Writes** `SubEtha.RWLock`.

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

### Open-SubEthaSemaphore

Attaches to the semaphore at Path, which must exist with the MaxPermits it was created with.

Also `Open-SESemaphore`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `MaxPermits` | `uint` | yes | 1 | - |

**Writes** `SubEtha.Semaphore`.

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

### Wait-SubEthaCondition

Waits on the condition at Path until Until answers true, or until Timeout seconds pass, and writes whether the condition became true. Until runs from inside the wait on this thread; an error it raises ends the wait and is reported rather than read as a false answer.

Also `Wait-SECondition`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Until` | `scriptblock` | yes | 1 | - |
| `Timeout` | `double` | - | named | - |

**Writes** `bool`.

## Probabilistic

### Measure-SubEthaBloomSize

The bits and hash count for holding Items with no more than FalsePositiveRate of wrong yeses, so a caller sizes a filter from what it means rather than from arithmetic. Blocked sizes for the blocked filter.

Also `Measure-SEBloomSize`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Items` | `ulong` | yes | 0 | - |
| `FalsePositiveRate` | `double` | yes | 1 | - |
| `Blocked` | `switch` | - | named | - |

**Writes** `SubEtha.BloomSize`.

| Property | Type |
|---|---|
| `Bits` | `ulong` |
| `Hashes` | `uint` |

### Measure-SubEthaSketchSize

The depth and width for a count-min sketch with an error of Epsilon at confidence Delta, so a caller sizes the sketch from what it needs.

Also `Measure-SESketchSize`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Epsilon` | `double` | yes | 0 | - |
| `Delta` | `double` | yes | 1 | - |

**Writes** `SubEtha.SketchSize`.

| Property | Type |
|---|---|
| `Depth` | `uint` |
| `Width` | `uint` |

### New-SubEthaBlockedBloomFilter

Obtains the blocked Bloom filter at Path with Bits bits and Hashes hash functions, creating it when the file does not exist; with Reset, empties it and remakes it at that size.

Also `New-SEBlockedBloomFilter`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Bits` | `ulong` | yes | 1 | - |
| `Hashes` | `uint` | yes | 2 | - |
| `Reset` | `switch` | - | named | - |

**Writes** `SubEtha.BlockedBloomFilter`.

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

### New-SubEthaBloomFilter

Obtains the Bloom filter at Path with Bits bits and Hashes hash functions, creating it when the file does not exist.

Also `New-SEBloomFilter`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Bits` | `ulong` | yes | 1 | - |
| `Hashes` | `uint` | yes | 2 | - |

**Writes** `SubEtha.BloomFilter`.

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

### New-SubEthaCountMinSketch

Obtains the count-min sketch at Path of Depth rows by Width counters, creating it when the file does not exist.

Also `New-SECountMinSketch`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Depth` | `uint` | yes | 1 | - |
| `Width` | `uint` | yes | 2 | - |

**Writes** `SubEtha.CountMinSketch`.

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

### New-SubEthaFineBloom

Builds a four-word Bloom filter, empty or holding Key.

Also `New-SEFineBloom`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Key` | `object[]` | - | named | - |

**Writes** `SubEtha.FineBloom`.

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

### New-SubEthaHistogram

Obtains the histogram at Path with the rising bucket Boundaries, creating it when the file does not exist.

Also `New-SEHistogram`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Boundaries` | `ulong[]` | yes | 1 | - |

**Writes** `SubEtha.Histogram`.

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

### New-SubEthaHyperLogLog

Obtains the distinct counter at Path at Precision (fourteen when absent; four is the smallest the format allows and sixteen the largest), creating it when the file does not exist.

Also `New-SEHyperLogLog`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Precision` | `byte` | - | named | - |

**Writes** `SubEtha.HyperLogLog`.

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

### New-SubEthaLruCache

Obtains the cache at Path holding Capacity entries of KeySize-byte keys and ValueSize-byte values, creating it when the file does not exist.

Also `New-SELruCache`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `uint` | yes | 1 | - |
| `KeySize` | `ulong` | yes | 2 | - |
| `ValueSize` | `ulong` | yes | 3 | - |

**Writes** `SubEtha.LruCache`.

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

### New-SubEthaRateLimiter

Obtains the rate limiter at Path holding Capacity tokens that come back at RefillPerSecond, creating it when the file does not exist.

Also `New-SERateLimiter`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `uint` | yes | 1 | - |
| `RefillPerSecond` | `uint` | yes | 2 | - |

**Writes** `SubEtha.RateLimiter`.

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

### New-SubEthaReservoir

Obtains the reservoir at Path holding Capacity items, creating it when the file does not exist.

Also `New-SEReservoir`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Reservoir`.

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

### New-SubEthaTinyBloom

Builds a word-sized Bloom filter, empty, holding Key, or rebuilt from the number Bits an earlier one gave.

Also `New-SETinyBloom`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Key` | `object[]` | - | named | - |
| `Bits` | `ulong` | - | named | - |

**Writes** `SubEtha.TinyBloom`.

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

### Open-SubEthaBlockedBloomFilter

Attaches to the blocked Bloom filter at Path, which must exist with the size it was created with.

Also `Open-SEBlockedBloomFilter`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Bits` | `ulong` | yes | 1 | - |
| `Hashes` | `uint` | yes | 2 | - |

**Writes** `SubEtha.BlockedBloomFilter`.

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

### Open-SubEthaBloomFilter

Attaches to the Bloom filter at Path, which must exist with the size it was created with.

Also `Open-SEBloomFilter`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Bits` | `ulong` | yes | 1 | - |
| `Hashes` | `uint` | yes | 2 | - |

**Writes** `SubEtha.BloomFilter`.

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

### Open-SubEthaCountMinSketch

Attaches to the count-min sketch at Path, which must exist with the depth and width it was created with.

Also `Open-SECountMinSketch`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Depth` | `uint` | yes | 1 | - |
| `Width` | `uint` | yes | 2 | - |

**Writes** `SubEtha.CountMinSketch`.

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

### Open-SubEthaHistogram

Attaches to the histogram at Path, which must exist with the Boundaries it was created with.

Also `Open-SEHistogram`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Boundaries` | `ulong[]` | yes | 1 | - |

**Writes** `SubEtha.Histogram`.

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

### Open-SubEthaHyperLogLog

Attaches to the distinct counter at Path, which must exist at the Precision it was created with.

Also `Open-SEHyperLogLog`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Precision` | `byte` | - | named | - |

**Writes** `SubEtha.HyperLogLog`.

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

### Open-SubEthaLruCache

Attaches to the cache at Path, which must exist with the capacity and sizes it was created with.

Also `Open-SELruCache`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `uint` | yes | 1 | - |
| `KeySize` | `ulong` | yes | 2 | - |
| `ValueSize` | `ulong` | yes | 3 | - |

**Writes** `SubEtha.LruCache`.

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

### Open-SubEthaRateLimiter

Attaches to the rate limiter at Path, which must exist with the capacity and rate it was created with.

Also `Open-SERateLimiter`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `uint` | yes | 1 | - |
| `RefillPerSecond` | `uint` | yes | 2 | - |

**Writes** `SubEtha.RateLimiter`.

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

### Open-SubEthaReservoir

Attaches to the reservoir at Path, which must exist with the Capacity it was made with.

Also `Open-SEReservoir`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Path` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |

**Writes** `SubEtha.Reservoir`.

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

## Clocks

### New-SubEthaCausalClock

Builds a causal clock with every count at zero, or at Counts.

Also `New-SECausalClock`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Counts` | `ulong[]` | - | named | - |

**Writes** `SubEtha.CausalClock`.

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

### New-SubEthaClock

Builds a hybrid logical clock reading: Physical and Logical as given, or with Now the moment this runs, in microseconds since the epoch, with the count at zero.

Also `New-SEClock`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Physical` | `ulong` | - | named | - |
| `Logical` | `ulong` | - | named | - |
| `Now` | `switch` | - | named | - |

**Writes** `SubEtha.Clock`.

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

## Sensing

### New-SubEthaCapacity

Builds a capacity estimator over probes of ProbeBytes, 1400 when absent.

Also `New-SECapacity`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `ProbeBytes` | `ulong` | - | named | - |

**Writes** `SubEtha.Capacity`.

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

### New-SubEthaForecast

Builds an arrival forecast.

Also `New-SEForecast`.

_No parameters._

**Writes** `SubEtha.Forecast`.

| Method | Answers |
|---|---|
| `MeanRate()` | `double` |
| `NextRate()` | `double` |
| `Observe(ulong bytes, double seconds)` | `void` |

### New-SubEthaLossBursts

Builds a burst model.

Also `New-SELossBursts`.

_No parameters._

**Writes** `SubEtha.LossBursts`.

| Method | Answers |
|---|---|
| `MeanRunLength()` | `double?` |
| `Observe(bool lost)` | `void` |
| `ObserveMany(bool[] losses)` | `ulong` |
| `Samples()` | `ulong` |
| `SteadyLoss()` | `double?` |
| `TransitionRates()` | `SubEtha.BurstRates` |

### New-SubEthaLossKind

Builds a loss classifier.

Also `New-SELossKind`.

_No parameters._

**Writes** `SubEtha.LossKind`.

| Method | Answers |
|---|---|
| `Classify(uint gap, double spacingMicroseconds)` | `SubEtha.LossClass` |
| `CongestionShare()` | `Single` |
| `DelaySpread()` | `double` |
| `ObserveDelay(double microseconds)` | `void` |
| `ObserveSpacing(double microseconds)` | `void` |

### New-SubEthaPathChanges

Builds a path sensor.

Also `New-SEPathChanges`.

_No parameters._

**Writes** `SubEtha.PathChanges`.

| Method | Answers |
|---|---|
| `Last()` | `SubEtha.PathMark` |
| `MarkedShare()` | `Single` |
| `Observe(byte ttl, byte congestionMark, byte hops)` | `void` |
| `RouteMovement()` | `Single` |

### New-SubEthaPeriodicity

Builds a periodicity sensor.

Also `New-SEPeriodicity`.

_No parameters._

**Writes** `SubEtha.Periodicity`.

| Method | Answers |
|---|---|
| `Observe(double delayMicroseconds, ulong atMicroseconds)` | `void` |
| `Period()` | `SubEtha.Beat` |
| `SecondsToNext()` | `double?` |

### New-SubEthaRoundTripShape

Builds a round-trip shape sensor.

Also `New-SERoundTripShape`.

_No parameters._

**Writes** `SubEtha.RoundTripShape`.

| Method | Answers |
|---|---|
| `Observe(double microseconds)` | `void` |
| `ObserveMany(double[] microseconds)` | `ulong` |
| `Samples()` | `ulong` |
| `TwoGroups()` | `double?` |
| `WirelessConfidence()` | `Single` |

### New-SubEthaSensReceiver

Opens the reading end of a link on LocalHost and LocalPort, carrying items of up to MaxItemSize bytes, which must be what the sender was made with.

Also `New-SESensReceiver`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `LocalPort` | `ushort` | yes | 0 | - |
| `MaxItemSize` | `ulong` | yes | 1 | - |
| `LocalHost` | `string` | - | named | - |
| `Code` | `SubEtha.SensCode` | - | named | - |

**Writes** `SubEtha.SensReceiver`.

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

### New-SubEthaSensSender

Opens the sending end of a link from LocalHost and LocalPort to PeerHost and PeerPort, carrying items of up to MaxItemSize bytes.

Also `New-SESensSender`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `PeerHost` | `string` | yes | 0 | - |
| `PeerPort` | `ushort` | yes | 1 | - |
| `MaxItemSize` | `ulong` | yes | 2 | - |
| `LocalHost` | `string` | - | named | - |
| `LocalPort` | `ushort` | - | named | - |
| `Code` | `SubEtha.SensCode` | - | named | - |

**Writes** `SubEtha.SensSender`.

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

### New-SubEthaTiming

Builds a timing sensor over a Window of recent items, sixty-four when absent.

Also `New-SETiming`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Window` | `ulong` | - | named | - |

**Writes** `SubEtha.Timing`.

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

## Bridges

### Get-SubEthaTransport

Writes the names of the transports this module carries: sens, the link across a lossy network, and the tcp and quic bridges.

Also `Get-SETransport`.

_No parameters._

**Writes** `string`.

### New-SubEthaQuicBridgeClient

Makes the sending end of a QUIC bridge from the ring at RingPath to the reading end at ServerHost and ServerPort, trusting Cert, the certificate that end was made with, issued for ServerName.

Also `New-SEQuicBridgeClient`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `RingPath` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ServerHost` | `string` | yes | 2 | - |
| `ServerPort` | `ushort` | yes | 3 | - |
| `Cert` | `object` | yes | named | - |
| `ServerName` | `string` | yes | named | - |
| `LocalHost` | `string` | - | named | - |
| `LocalPort` | `ushort` | - | named | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |

**Writes** `SubEtha.QuicBridgeClient`.

| Property | Type |
|---|---|
| `RingPath` | `string` |
| `Server` | `SubEtha.Endpoint` |
| `ServerName` | `string` |

| Method | Answers |
|---|---|
| `Run(ulong items)` | `void` |

### New-SubEthaQuicBridgeServer

Makes the reading end of a QUIC bridge on LocalHost and LocalPort, proving itself with Cert and Key, putting arriving items into the ring at RingPath. A port of zero lets the system pick one, which LocalAddr then reports.

Also `New-SEQuicBridgeServer`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `RingPath` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `LocalPort` | `ushort` | yes | 2 | - |
| `Cert` | `object` | yes | named | - |
| `Key` | `object` | yes | named | - |
| `LocalHost` | `string` | - | named | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |

**Writes** `SubEtha.QuicBridgeServer`.

| Property | Type |
|---|---|
| `RingPath` | `string` |

| Method | Answers |
|---|---|
| `AcceptOne()` | `ulong` |
| `LocalAddr()` | `SubEtha.Endpoint` |

### New-SubEthaSelfSignedCert

Makes a certificate and its key for a QUIC bridge. Name is what the certificate is issued for and what a client passes as ServerName; it names the certificate rather than the address, so any address the reading end is reachable at works. The certificate is signed by nobody, so the reading end holds both and the sending end holds the certificate alone, which is what it checks the reading end against.

Also `New-SESelfSignedCert`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Name` | `string` | yes | 0 | - |

**Writes** `SubEtha.Certificate`.

| Property | Type |
|---|---|
| `Cert` | `object` |
| `Key` | `object` |
| `Name` | `string` |

### New-SubEthaTcpBridgeClient

Makes the sending end of a TCP bridge from the ring at RingPath to the reading end at ServerHost and ServerPort.

Also `New-SETcpBridgeClient`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `RingPath` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `ServerHost` | `string` | yes | 2 | - |
| `ServerPort` | `ushort` | yes | 3 | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |

**Writes** `SubEtha.TcpBridgeClient`.

| Property | Type |
|---|---|
| `RingPath` | `string` |
| `Server` | `SubEtha.Endpoint` |

| Method | Answers |
|---|---|
| `Run(ulong items)` | `void` |

### New-SubEthaTcpBridgeServer

Makes the reading end of a TCP bridge on LocalHost and LocalPort, putting arriving items into the ring at RingPath. A port of zero lets the system pick one, which LocalAddr then reports.

Also `New-SETcpBridgeServer`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `RingPath` | `string` | yes | 0 | - |
| `Capacity` | `ulong` | yes | 1 | - |
| `LocalPort` | `ushort` | yes | 2 | - |
| `LocalHost` | `string` | - | named | - |
| `MaxProducers` | `ulong` | - | named | - |
| `MaxConsumers` | `ulong` | - | named | - |

**Writes** `SubEtha.TcpBridgeServer`.

| Property | Type |
|---|---|
| `RingPath` | `string` |

| Method | Answers |
|---|---|
| `AcceptOne()` | `ulong` |
| `LocalAddr()` | `SubEtha.Endpoint` |

## The pipeline

### Receive-SubEthaItem

Reads items out of From, a structure the module returned, into the pipeline: everything waiting, or up to Count of them, stopping when the structure runs empty. A ring that numbers its consumers takes the id in Consumer; a deque is stolen from unless Owner is given.

Also `Receive-SEItem`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `From` | `object` | yes | 0 | - |
| `Count` | `ulong` | - | named | - |
| `Consumer` | `ulong` | - | named | - |
| `Owner` | `switch` | - | named | - |

**Writes** `byte[]`.

### Send-SubEthaItem

Sends each item piped in to To, a structure the module returned, and writes each item the structure refused, so a full structure hands the item back rather than dropping it. A ring that numbers its producers takes the id in Producer.

Also `Send-SEItem`.

| Parameter | Type | Required | Position | Pipeline |
|---|---|---|---|---|
| `Item` | `object` | yes | 0 | value |
| `To` | `object` | yes | named | - |
| `Producer` | `ulong` | - | named | - |

**Writes** `object`.

