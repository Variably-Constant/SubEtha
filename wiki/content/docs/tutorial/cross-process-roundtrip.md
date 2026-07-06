---
weight: 30
---

# Cross-process round-trip in 30 lines

This chapter demonstrates the headline capability of the `subetha-cxc`
crate: two independent processes mapping the same MMF file and
sharing a primitive end-to-end, with sidecar observation already
wired natively into the primitive.

## The two binaries

Create two binaries in the same workspace, `producer` and `consumer`.

### `producer/src/main.rs`

```rust,no_run
use subetha_cxc::SharedHashMap;

fn main() {
    let path = "/tmp/subetha-roundtrip.bin";

    // Create the MMF file. SharedHashMap needs capacity >= 2
    // (it probes with hash % capacity, so any size works).
    let m = SharedHashMap::<u32, u64>::create(path, 1024)
        .expect("create");

    for k in 0..100u32 {
        m.insert(k, (k as u64) * 1000)
            .expect("insert");
    }
    m.flush().expect("flush");

    println!("producer: 100 entries written to {path}");
}
```

### `consumer/src/main.rs`

```rust,no_run
use subetha_cxc::SharedHashMap;

fn main() {
    let path = "/tmp/subetha-roundtrip.bin";

    // Open the existing MMF. The capacity argument MUST match what
    // the producer used; mismatch returns LayoutMismatch.
    let m = SharedHashMap::<u32, u64>::open(path, 1024)
        .expect("open");

    let mut sum = 0u64;
    for k in 0..100u32 {
        if let Some(v) = m.get(&k) {
            sum += v;
        }
    }
    println!("consumer: sum = {sum}");
}
```

## Running them

In two terminals:

```bash
# terminal 1
cargo run --release --bin producer

# terminal 2 (after producer exits)
cargo run --release --bin consumer
```

You should see:

```text
producer: 100 entries written to /tmp/subetha-roundtrip.bin
consumer: sum = 4950000
```

(0+1000+2000+...+99000 = 4,950,000.)

## What just happened

> [!NOTE]
> **No serialisation, no IPC channel.** Both processes mapped the
> same MMF file. The OS page cache aliases the two virtual mappings
> onto the same physical pages. Reads in `consumer` go to the
> exact bytes that `producer`'s `insert()` calls wrote.

> [!TIP]
> **Disk persistence is free.** The producer's `flush()` call
> forces dirty pages to disk via `msync()`. If you stop here and
> reboot, the data is still in `/tmp/subetha-roundtrip.bin`. The
> next `consumer` run picks up where the previous left off without
> any explicit reload step.

> [!IMPORTANT]
> **The hash is FNV-1a, not the default `std::hash::BuildHasher`**.
> `std`'s hasher uses a per-process random seed for DoS resistance,
> which makes keys irreproducible across processes. `SharedHashMap`
> uses FNV-1a so the same key produces the same slot index in
> every process.

## Live cross-process: two processes hitting the map concurrently

The above example ran producer and consumer serially. The MPMC
shape works concurrently too - launch both binaries while running,
and the consumer sees the producer's inserts as they happen.

This pattern composes with the sidecar control plane. Wrap either
end in a `SidecarBox::new(SharedHashMap::open(...))` and the
sidecar in that process drains the local observation ring; each
process has its own sidecar with its own stats, observing the
local op-stream while the underlying MMF holds the shared bytes.

## What to do next

You have seen the substrate, the sidecar, and the cross-process
MMF substrate end-to-end. From here:

- The [role-pair selection how-to](../how-to/role-pair-selection.md)
  walks through which primitive answers your concurrency shape.
- The [architecture explanation](../explanation/architecture.md)
  explains *why* the substrate is shaped the way it is.
- The [reference section](../reference/subetha-cxc/) lists
  every MMF-backed primitive with its layout and op-kind table.
