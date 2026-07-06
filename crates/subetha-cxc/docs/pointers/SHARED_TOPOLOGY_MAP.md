# SharedTopologyMap

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/N*N_atomic_grid-lock_free-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

K_process axis observer + recommendation substrate. Flat
`N * N` `AtomicU64` grid where each cell tracks the message
count from src to dst. Reads fan-in / fan-out / total flow as
linear scans over a row or column. Recommends transport
topology (PointToPoint / BroadcastTree / AllToAllMesh) based on
observed fan statistics.

> **The "cross-process message-flow topology observer"
> primitive.** fan_out at **11.48 ns** vs `Mutex<HashMap>`
> 26.49 ns (**2.31x faster** - row scan over atomics vs mutex
> iter). recommend at 390 ns vs 556 ns (**1.42x faster**).
> read_recommendation at **1.47 ns** (one atomic load).
> Architectural lever: cross-process flow observation + low-cost
> topology recommendation for transport choice.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Capacity N fixed at create**: `N * N` AtomicU64 cells.
- **`record_send(src, dst)` is one `fetch_add`**: lock-free.
- **`fan_out(src)` / `fan_in(dst)`** are linear scans over one
  row / column.
- **`recommend()`** computes max fan-in/out against the default
  thresholds and returns a `TopologyKind`.
- **Pre-published recommendation**: stored in header for O(1)
  `read_recommendation`.
- **Cross-process backed by MMF.**

---

## Bench evidence

| Op | `SharedTopologyMap` N=16 (mmf) | `Mutex<HashMap<(u32,u32), u64>>` | mmf relative |
|---|---:|---:|---|
| fan_out (16-node scan) | **11.48 ns** | 26.49 ns | **2.31x faster** |
| recommend (max fan calc) | **390 ns** | 556 ns | **1.42x faster** |
| read_recommendation (O(1) header) | **1.47 ns** | n/a | one atomic load |

### Reading the trade-offs

1. **fan_out 2.31x faster**: row scan over 16 atomic loads vs
   Mutex lock + HashMap iter + filter + unlock.
2. **recommend 1.42x faster**: aggregates fan stats over the
   whole grid; mutex baseline pays per-call lock.
3. **read_recommendation at 1.47 ns**: one atomic load of the
   pre-published Topology enum.

### Rule 3b bench audit

- **Fair contender**: `Mutex<HashMap<(u32, u32), u64>>` is
  the textbook in-process edge-counter shape. Same
  semantics.
- **No `thread::spawn` inside `b.iter`**: single-threaded;
  multi-thread record correctness in source unit tests.
- **Sizing**: N=16 topology (representative for cluster-scale
  observation).
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process flow observation**: any process records
  sends; coordinator reads recommendations.
- **Distinct edges fully concurrent**: each cell has its own
  AtomicU64; concurrent record_send on distinct edges never
  contend. Mutex baseline serializes ALL records.

---

## Worked examples

### Record flow + recommend topology

```rust
use subetha_cxc::SharedTopologyMap;

let t = SharedTopologyMap::create("/tmp/topo.bin", 16).unwrap();
for (src, dst) in observed_messages() {
    t.record_send(src, dst).unwrap();
}
let topo = t.recommend();  // default fan_in/out thresholds
match topo {
    TopologyKind::PointToPoint => use_shared_ring(),
    TopologyKind::BroadcastTree => use_shared_broadcast_ring(),
    TopologyKind::AllToAllMesh => use_n_squared_rings(),
}
```

### Cross-process dashboard

```rust
let t = SharedTopologyMap::open("/tmp/topo.bin", 16).unwrap();
println!("Current topology: {:?}", t.read_recommendation());   // 1 ns
```

---

## Use case patterns

### Pattern: dynamic transport selection

Observe message flow; recommend P2P / Broadcast / Mesh
transport based on fan-in/fan-out shape; pick the matching
primitive at startup.

### Pattern: flow visualization dashboard

A monitor process reads fan-out/fan-in counts and renders the
communication graph live.

### Pattern: cluster-shape adaptation

The orchestrator observes which processes talk to which and
adapts the transport choice in production.

---

## Known limitations

- **N fixed at create**: capacity bounded.
- **`N * N` storage**: 16-node = 2 KB; 256-node = 512 KB.
- **`fan_out` / `fan_in` are O(N) scans**: practical for
  N <= ~256.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Sizing N too small for the cluster.** record_send on
  out-of-bounds src/dst returns Err; over-provision.

- **Treating `recommend` as authoritative without
  `publish_recommendation`.** The header stores the last
  published value for cheap reads.

- **Wrapping in a Mutex.** Pointless; per-cell fetch_add is
  already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/shared_topology_map.rs` (676
  lines, 16 unit tests covering record + fan_out/in,
  recommendation policy, publish + read recommendation,
  out-of-bounds rejection, cross-handle visibility).
- Bench: `crates/subetha-cxc/benches/shared_topology_map.rs`
  (record_send, fan_out, recommend, read_recommendation vs
  `Mutex<HashMap>`).
- Sibling primitive: [SHARED_RING.md](./SHARED_RING.md) -
  PointToPoint transport.
- Sibling primitive:
  [SHARED_BROADCAST_RING.md](./SHARED_BROADCAST_RING.md) -
  BroadcastTree transport.
- Sibling primitive:
  [SHARED_LEADER_ELECTION.md](./SHARED_LEADER_ELECTION.md) -
  coordinator selection for the recommendation engine.
