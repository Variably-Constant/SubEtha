# Structural Compression Layer (throughput axis)

Sens-O-Matic's loss axis (adaptive Cauchy-RS FEC, the segment tower,
interleaving, the four sensors with fusion, ARQ, bad-FCS salvage) is the
subject of [`SENS_O_MATIC_PERFORMANCE.md`](SENS_O_MATIC_PERFORMANCE.md)
and is built and cross-host-validated. This document covers the
**orthogonal throughput axis**: shipping fewer bytes on the wire for the
same delivered data, and distributing the path across cores. Both are
sized by measurement, and the measurements also confirm where the loss
axis correctly stops.

## What the measurements settled

Two measurements drive every decision here. Both ran on real binaries
over the real LAN.

### Keystone 1 - payload structure (`examples/payload_entropy.rs`)

The bridge ships fixed-width `repr(C)` slots through their marshal paths.
Measured on the real slot types:

| slot type | content | constant | derivable | free slack |
|---|---|---:|---:|---:|
| `PassSlot` (56B) | typical | 28 | 0 | 68.8% |
| `FatLineItem` (64B) | typical | 27 | 0 | 62.7% |
| `FatLineItem` | random | 15 | 0 | 44.1% |
| hash-map op (32B) | seq keys | 14 | 8 ✓ | 75.2% |

Structured traffic is 44-75% free slack (padding, reserved fields, count
fields, FNV hashes the receiver recomputes). `delta_H >= static_H` in
every case: temporal or general-purpose compression adds nothing beyond
removing the structural redundancy a known schema already exposes. So the
right compressor here is schema-aware elision, not an LZ/zstd pass, and
not a separate dictionary stage.

### Keystone 2 - loss burst structure (`examples/loss_burst_probe.rs`)

A seq-stamped UDP probe, validated against known netem models (uniform
10% gives burst_ratio 1.0, gemodel gives mean_run = 1/r exactly), then run
on the real Wi-Fi link including a 1.5x-overload congestion test:

- At every offered rate up to the sender's self-paced ceiling, real
  application-visible loss is **0%**. The OS qdisc backpressures and
  802.11 MAC retransmission absorbs frame loss; under 267 Mbit/s offered
  onto a ~173 Mbit/s link the probe **slowed** (lost bandwidth) but lost
  zero packets.
- The only real loss observed was a single contiguous **262-packet burst**
  (a ~52 ms outage), burst_ratio 380 - an outage event, not scattered
  loss, not multi-scale.

Real loss on these links is **bimodal**: paced-to-zero in the common case,
or a rare long outage burst. An outage burst is too large for any
economical FEC (recovering 262 losses needs 262 parity) - it is ARQ
territory, recovered by the selective NAK already built. There is no
middle regime of moderate multi-scale bursts where a deeper FEC tower or a
cross-scale iterative decoder would earn its complexity.

## Loss-axis scope (confirmed, not changed)

The data confirms the loss axis is correctly scoped as-built:

- The single within-block RS plus one cross-block segment tower rung
  (built) covers the scattered and meso-burst regimes. The super-segment
  rung and a cross-scale iterative decoder are **not built**, because no
  measured loss regime needs them: loss is either absorbed below the
  socket or arrives as an outage burst that ARQ handles.
- A cross-scale iterative (turbo/LDPC-style) decoder is doubly wrong here:
  the UDP erasure channel delivers hard intact-or-missing shards with no
  soft log-likelihood information, and the loss is not multi-scale. The
  segment tower path stays gated on a loss regime the LAN/Wi-Fi links do
  not produce; it earns its keep only on a link without link-layer ARQ
  (satellite, raw radio, bit-error fiber), which is not this transport's
  target.

This is the data following its own conclusion: the throughput axis below
is where the new wins are.

## The structural compression layer

A fixed-width slot carries a large fraction of bytes that never vary
(padding, reserved, stable enum high-bytes, counts) or are derivable
(a stored FNV hash equals the hash of the key bytes also present). The
codec learns which byte positions are constant across the stream (a
template), then ships only the bytes at the varying positions; the
receiver scatters them back into the template.

### Built - `src/schema_codec.rs`

`SchemaTemplate::learn` finds the constant positions from a sample;
`encode` ships one flag byte plus the varying bytes; `decode` rebuilds
from the template. It is **exact, not lossy**: a slot that differs at a
supposedly-constant position is escaped in full under an escape flag, so
round-trip is byte-identical for any input and a stale template can never
corrupt, only compress less. It is cache-resident (linear gather/scatter
over `u16` position lists, no per-slot allocation). `serialize` /
`deserialize` carry the template in one handshake message.

Proven E2E (`examples/schema_bridge_demo.rs`): 50k real `FatLineItem`
slots over the real reliable-UDP transport, template negotiated in-band,
every slot recovered byte-exact, the compressed payload 41% smaller than
the raw slot, zero escapes. That 41% is the **payload** reduction (the
pre-FEC item); with per-block adaptive shard length (the TX egress-gate
note below) it becomes a 34% reduction in actual UDP wire bytes and a
1.47x goodput gain, audited cross-host by packet capture. A learning-sample
caveat is also recorded: a head-only sample
mislearns a monotonic counter's high bytes as constant and escapes later;
sampling across the stream fixes it, and the production template learns
from a strided sample or re-learns on an escape-rate threshold.

### Integration points (the existing architecture already has them)

The adaptive-FEC design already defines the two transform points this
layer slots into, so no new architecture is introduced:

- **TX egress-gate**: compress each slot with the active template before
  it enters the FEC encoder. The payload reduction reaches the wire only
  when the FEC shard length is sized per block to the block's actual
  items. The encoder sizes `shard_len` per block to the block's largest
  staged item (`seal_block`), and the decoder reads each block's length
  from the datagram size, so no header field is added. The tower's
  cross-block outer code needs uniform blocks across a segment, so it
  forces the fixed maximum when enabled. Audited ground truth (tcpdump,
  cross-host Win to VM, 60k slots, shards x3): raw 6.74 MB of UDP vs
  compressed 4.42 MB - **34% fewer actual wire bytes** (the 41% payload
  reduction diluted by the fixed 9-byte header + 2-byte length prefix per
  datagram) - and goodput rose from 19 to **28 Mbit/s (1.47x)**, byte-exact.
- **RX read-modifier**: decompress after FEC reassembly and before
  delivery into the consumer ring.
- **In-band template negotiation**: the template descriptor is too large
  for the 16-byte wire heartbeat, so it ships as its own tagged
  (`TAG_TEMPLATE`), FEC/ARQ-reliable, in-order control item rather than on
  the heartbeat plane. The sender ships it as the first item of the stream;
  a mid-stream re-learn (`with_relearn`) ships a fresh tagged template the
  same way, and the receiver re-points its decoder (`poll` swaps the
  template on a `TAG_TEMPLATE` item) without a delivery gap.

### Composition with sharding (`src/sharded_udp.rs`, built)

Sharding runs N independent streams across N threads (one whole transport
each via `ShardedSender` / `ShardedReceiver`, item `i` riding shard
`i % N`), distributing the FEC encode / send / recv / decode / deliver path
over cores. The schema codec is an item-bytes transform independent of which
shard carries an item, so the two compose. The built cross-host example
[`examples/compressed_sharded_lan.rs`](../crates/subetha-cxc/examples/compressed_sharded_lan.rs)
(`--shards S --compress 1`) takes the simplest form: one template learned
over the stream, each slot compressed before the sharded sender round-robins
the compact items across shard threads - fewer bytes per item AND the
recovery path spread across cores at once, real `FatLineItem` slots delivered
byte-exact against the `--compress 0` baseline. (Because each
`CompressedSender` carries its own template, a per-shard-template variant -
one `CompressedSender` per shard - is equally available.) This is the run
that produced the cross-host 34% / 1.47x figures above.

## Honest limits

- Structural compression only removes the slack a known schema exposes.
  High-entropy payloads (already-compressed or encrypted content) measure
  near 0% slack, and the escape path keeps them exact at one flag byte of
  overhead per slot. The win is throughput, not loss resilience.
- The template assumes a stable slot schema. A schema that drifts (a new
  field pattern) raises the escape rate; the re-learn threshold bounds the
  cost, and the floor is the uncompressed slot plus one flag byte.
- Compression lowers wire bytes, which lowers loss exposure (fewer
  datagrams) but does not change the loss regime. The loss axis remains
  the authority on recovery.
