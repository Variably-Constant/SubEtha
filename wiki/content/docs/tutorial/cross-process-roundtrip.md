---
weight: 30
---

# Cross-process round-trip in 30 lines

This chapter demonstrates the headline capability of the `subetha-cxc`
crate: two independent processes mapping the same MMF file and
sharing a primitive end-to-end, with sidecar observation already
wired natively into the primitive.

## The producer and the consumer

Write a producer and a consumer on the surface you use. In Rust they
are two binaries in the same workspace; in Python and PowerShell, two
scripts. Rust types the map's keys and values; the Python and
PowerShell maps take keys and values as bytes of the widths the map
was created with (4-byte keys, 8-byte values here).

### The producer

{{< tabs >}}

{{< tab name="Rust" >}}
`producer/src/main.rs`:

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
{{< /tab >}}

{{< tab name="Python" >}}
`producer.py`:

```python
import os
import tempfile

import subetha

path = os.path.join(tempfile.gettempdir(), "subetha-roundtrip.bin")

# 1024 entries of 4-byte keys and 8-byte values, created when the
# file does not exist.
with subetha.HashMap(path, 1024, 4, 8) as m:
    for k in range(100):
        # insert answers "inserted" or "updated", and None when full.
        if m.insert(k.to_bytes(4, "little"), (k * 1000).to_bytes(8, "little")) is None:
            raise RuntimeError("the map is full")

print(f"producer: 100 entries written to {path}")
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
`producer.ps1`:

```powershell
Import-Module SubEtha
$path = Join-Path ([IO.Path]::GetTempPath()) 'subetha-roundtrip.bin'

# 1024 entries of 4-byte keys and 8-byte values, created when the
# file does not exist.
$m = New-SubEthaHashMap -Path $path -Capacity 1024 -KeySize 4 -ValueSize 8
foreach ($k in 0..99) {
    $key = [BitConverter]::GetBytes([uint32]$k)
    $value = [BitConverter]::GetBytes([uint64]($k * 1000))
    # Insert answers Inserted, Updated, or Full.
    if ($m.Insert($key, $value) -eq 'Full') { throw 'the map is full' }
}
$m.Dispose()
"producer: 100 entries written to $path"
```
{{< /tab >}}

{{< /tabs >}}

### The consumer

{{< tabs >}}

{{< tab name="Rust" >}}
`consumer/src/main.rs`:

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
{{< /tab >}}

{{< tab name="Python" >}}
`consumer.py`:

```python
import os
import tempfile

import subetha

path = os.path.join(tempfile.gettempdir(), "subetha-roundtrip.bin")

# Attach to the producer's map. The capacity and both widths must be
# the ones it was created with, or open raises OSError.
total = 0
with subetha.HashMap.open(path, 1024, 4, 8) as m:
    for k in range(100):
        v = m.get(k.to_bytes(4, "little"))
        if v is not None:
            total += int.from_bytes(v, "little")

print(f"consumer: sum = {total}")
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
`consumer.ps1`:

```powershell
Import-Module SubEtha
$path = Join-Path ([IO.Path]::GetTempPath()) 'subetha-roundtrip.bin'

# Attach to the producer's map. The capacity and both sizes must be
# the ones it was created with.
$m = Open-SubEthaHashMap -Path $path -Capacity 1024 -KeySize 4 -ValueSize 8
$sum = [uint64]0
foreach ($k in 0..99) {
    $v = $m.Get([BitConverter]::GetBytes([uint32]$k))
    if ($null -ne $v) { $sum += [BitConverter]::ToUInt64($v, 0) }
}
$m.Dispose()
"consumer: sum = $sum"
```
{{< /tab >}}

{{< /tabs >}}

## Running them

In two terminals, the producer first:

{{< tabs >}}

{{< tab name="Rust" >}}
```bash
# terminal 1
cargo run --release --bin producer

# terminal 2 (after producer exits)
cargo run --release --bin consumer
```
{{< /tab >}}

{{< tab name="Python" >}}
```bash
# terminal 1
python producer.py

# terminal 2 (after producer exits)
python consumer.py
```
{{< /tab >}}

{{< tab name="PowerShell" >}}
```powershell
# terminal 1
pwsh ./producer.ps1

# terminal 2 (after producer exits)
pwsh ./consumer.ps1
```
{{< /tab >}}

{{< /tabs >}}

You should see, with your temp directory in the path on Python and
PowerShell:

```text
producer: 100 entries written to /tmp/subetha-roundtrip.bin
consumer: sum = 4950000
```

(0+1000+2000+...+99000 = 4,950,000.)

## What just happened

> [!NOTE]
> **No serialization, no IPC channel.** Both processes mapped the
> same MMF file. The OS page cache aliases the two virtual mappings
> onto the same physical pages. Reads in `consumer` go to the
> exact bytes that `producer`'s `insert()` calls wrote.

> [!TIP]
> **Disk persistence is free.** The Rust producer's `flush()` call
> forces dirty pages to disk via `msync()`. If you stop here and
> reboot, the data is still in `/tmp/subetha-roundtrip.bin`. The
> next `consumer` run picks up where the previous left off without
> any explicit reload step. The Python and PowerShell maps have no
> `flush`: the operating system writes their dirty pages back on its
> own schedule.

> [!IMPORTANT]
> **The hash is FNV-1a, not the default `std::hash::BuildHasher`**.
> `std`'s hasher uses a per-process random seed for DoS resistance,
> which makes keys irreproducible across processes. `SharedHashMap`,
> and the byte-keyed map Python and PowerShell use, hash with FNV-1a
> so the same key produces the same slot index in every process.

## Live cross-process: two processes hitting the map concurrently

The above example ran producer and consumer serially. The MPMC
shape works concurrently too - launch both binaries while running,
and the consumer sees the producer's inserts as they happen.

In Rust this pattern composes with the sidecar control plane. Wrap
either end in a `SidecarBox::new(SharedHashMap::open(...))` and the
sidecar in that process drains the local observation ring; each
process has its own sidecar with its own stats, observing the
local op-stream while the underlying MMF holds the shared bytes.
The Python package and the PowerShell module do not wrap their maps
in a sidecar.

## What to do next

You have seen the substrate, the sidecar, and the cross-process
MMF substrate end-to-end. From here:

- The [role-pair selection how-to](../how-to/role-pair-selection.md)
  walks through which primitive answers your concurrency shape.
- The [architecture explanation](../explanation/architecture.md)
  explains *why* the substrate is shaped the way it is.
- The [reference section](../reference/subetha-cxc/) lists
  every MMF-backed primitive with its layout and op-kind table.
