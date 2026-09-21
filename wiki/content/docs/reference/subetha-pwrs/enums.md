---
title: "Every enum"
weight: 30
---

# Every enum

The 14 enums the surface takes and answers. Each is
accepted as its type or as its name in a string, and the string
form reads the same in both hosts:

```powershell
$atomic.FetchAdd(1, [SubEtha.MemoryOrder]::Relaxed)
$atomic.FetchAdd(1, 'Relaxed')
```

| Enum | Values |
|---|---|
| `SubEtha.Durability` | `Volatile`, `Transient`, `Persistent` |
| `SubEtha.InsertOutcome` | `Inserted`, `Updated`, `Full` |
| `SubEtha.Locale` | `Anon`, `File`, `ShmFs` |
| `SubEtha.LossClass` | `Wireless`, `Congestion` |
| `SubEtha.MemoryOrder` | `Relaxed`, `Acquire`, `Release`, `AcqRel`, `SeqCst` |
| `SubEtha.OrderingMode` | `Unordered`, `MergeByStamp`, `MergeStrict` |
| `SubEtha.OrderingNeed` | `PerProducer`, `GlobalFifo` |
| `SubEtha.QosPreset` | `Streaming`, `ReliablePubSub`, `PersistentLog` |
| `SubEtha.QueueShape` | `Ring`, `WorkStealing`, `Map` |
| `SubEtha.Reliability` | `BestEffort`, `Reliable` |
| `SubEtha.SensCode` | `Rlc`, `Rs` |
| `SubEtha.SetStrategy` | `List`, `Map` |
| `SubEtha.StampKind` | `Tsc`, `Counter`, `Monotonic` |
| `SubEtha.Topology` | `PointToPoint`, `BroadcastTree`, `AllToAllMesh` |

