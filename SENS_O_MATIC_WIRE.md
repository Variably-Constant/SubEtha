# The Sens-O-Matic Wire Format

Version 1, corresponding to SubEtha 0.3.0.

Sens-O-Matic is a reliable, forward-correcting datagram protocol over UDP.
This document specifies what goes on the wire, in enough detail to write an
interoperating implementation without reading the reference source.

Every layout here was read from the code that writes or parses those bytes.
Where a comment in the reference implementation and its code disagree,
this document records the code.

## 1. Scope and conformance

This document is normative. It is versioned with the reference
implementation and lives beside it, so a reader takes the specification
and the code that satisfies it from one place at one revision. Where any
other document in this repository describes the transport, this one
governs.

It covers both erasure codes the protocol carries, their datagram
layouts, their control planes, and the coefficient constants that make a
decoder reproducible.

It does **not** specify the sender's adaptive control law. Section 3
explains why that is a completeness statement rather than an omission.

An implementation conforms if it:

- produces datagrams matching sections 4 and 5,
- decodes any parameters a peer sends, whether or not it would have chosen
  them,
- reproduces the test vectors of section 8 byte for byte,
- follows the compatibility rules of section 7.

"Must" is a requirement for interoperation. Byte offsets count from zero.
All multi-byte integers are little-endian unless stated otherwise. `u16-le`
means an unsigned 16-bit little-endian integer.

## 2. Two codes, one protocol

The erasure code is a swappable detail of the protocol, in the way a cipher
suite is a swappable detail of a secure channel. Two are defined:

| Code | Shape | Recovers | Section |
|---|---|---|---|
| RLC | Sliding window, convolutional | From the next repair | 4 |
| RS | Block, Cauchy Reed-Solomon, systematic, MDS | Any `k` of `k + r` shards | 5 |

Both deliver every item in order and both compute over GF(2^8) (section
3.1). They differ in latency: the block code must wait for the rest of a
block before it can reconstruct, which often means a retransmit has already
been requested by the time recovery is possible; the sliding window recovers
from the next repair that covers the gap.

The two occupy disjoint packet-type numbers, so a single socket can carry
both and demultiplex on the first byte. This is the complete assignment;
every value below is taken.

| Type | Meaning | Code |
|---|---|---|
| `0x01` | Data shard | RS |
| `0x04` | Control container | RS |
| `0x08` | Raw-loss feedback | either |
| `0x09` | Code switch | either |
| `0x0A` | Source symbol | RLC |
| `0x0B` | Repair symbol | RLC |
| `0x0C` | Negative acknowledgment | RLC |
| `0x0D` | Acknowledgment | RLC |
| `0x0E` | Feedback | RLC |
| `0x0F` | Handshake flight | RLC, secured builds |
| `0x10` | Handshake flight acknowledgment | RLC, secured builds |
| `0x11` | Sealed envelope | RLC, secured builds |
| `0x12` | Path challenge | RLC |
| `0x13` | Path response | RLC |

`0x0F` to `0x11` are present only in a build with the record layer
compiled in. They are listed here because the number is spent either way:
an implementation without a record layer must still not reuse them.

The RLC path-validation frames sit outside the contiguous RLC data range
deliberately, so a shared-socket demultiplexer can route them by type rather
than by a range test.

**Every assigned type keeps bit `0x40` clear.** A QUIC packet always has
that bit set in its first byte, so a socket can carry QUIC alongside both
codes and separate them on the first byte alone. A new type must preserve
this, which bounds the space a future assignment may use.

### 2.1 Every datagram is self-describing

An RS data shard carries its block id, shard index, `k`, `r` and session
epoch. An RLC repair carries the first source id of its window, the window
size and its coefficient density. Neither receiver needs prior negotiation,
a handshake exchange of coding parameters, or any knowledge of how the
sender chose them.

This is the single most important property of the format, and section 3
follows from it.

### 2.2 The two cross-code frames

`0x08` and `0x09` belong to neither code and are carried whichever is
active.

**Raw-loss feedback**, 9 bytes:

```
[0x08] [received u64-le]
```

`received` is the receiver's cumulative count of forward data and repair
datagrams it has seen. The sender pairs it with its own sent count to
measure channel loss directly, independent of what either code recovered.
A code that repairs a loss hides it from every other estimator, which is
what this frame exists to see past.

**Code switch**, 10 bytes:

```
[0x09] [boundary u64-le] [to_code u8]
```

`to_code` is `0` for RLC and `1` for RS. `boundary` is the number of items
the sender has delivered across both codes up to the switch. The receiver
keeps draining the old decoder until its own cumulative delivery reaches
`boundary`, then activates `to_code`. Delivery order is preserved across
the switch because the boundary is stated in items delivered, not in
datagrams sent.

## 3. The control law is sender policy, not protocol

The reference sender adapts its coding to measured channel conditions: it
raises redundancy as loss rises, widens the window to span the fitted burst
length, leans denser under bursty loss, and disables coding entirely on a
link it can prove is clean. The receiver feeds it the measurements to do so
(sections 4.7 and 5.3).

None of that is specified here, and an implementation is not required to
reproduce any of it, because section 2.1 makes it unnecessary: a conforming
receiver decodes whatever parameters arrive. A sender that picks its
parameters by any means, adaptive or fixed or arbitrary, produces a stream
any conforming receiver can read.

An implementation **must** therefore decode any legal parameter combination.
It **may** choose parameters however it likes.

### 3.1 The field

Both codes compute in GF(2^8) with the primitive polynomial
`x^8 + x^4 + x^3 + x^2 + 1` (`0x11D`) and generator `2`.

Addition is `XOR`. Multiplication is conventionally implemented with
logarithm and antilogarithm tables over that polynomial; any implementation
giving the same products conforms.

## 4. The RLC variant

### 4.1 DATA (type `0x0A`)

Header is 17 bytes, then the symbol.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `0x0A` |
| 1 | 8 | `conn_id`, u64-le |
| 9 | 4 | `source_id`, u32-le |
| 13 | 4 | `send_us`, u32-le |
| 17 | `symbol_len` | symbol (section 4.3) |

`source_id` counts up from zero per sender and names the symbol both for
retransmission and for the coding window.

`send_us` is the sender's microseconds-since-start. A receiver differences
it against its own arrival clock to recover a *relative* one-way trip time;
a constant clock offset cancels in the difference, so the two clocks need no
relationship. It rides the header rather than the symbol, so it is not part
of any repair's linear combination.

### 4.2 REPAIR (type `0x0B`)

Header is 20 bytes, then the repair payload, which is `symbol_len` bytes.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `0x0B` |
| 1 | 8 | `conn_id`, u64-le |
| 9 | 4 | `repair_key`, u32-le |
| 13 | 4 | `first_source_id`, u32-le |
| 17 | 2 | `window_size`, u16-le |
| 19 | 1 | `dt` |
| 20 | `symbol_len` | payload |

`repair_key` is a sequence number counting up per sender. **It takes no part
in decoding.** It names a repair in a log or a trace. An implementation must
not derive coefficients from it.

`first_source_id` and `window_size` name the window this repair covers: the
source symbols `first_source_id` up to but not including
`first_source_id + window_size`.

`dt` splits into two nibbles:

- low nibble (`dt & 0x0F`): the coefficient density, 0 to 15.
- high nibble (`dt >> 4`): the generator id.

The payload is the GF(2^8) sum of each covered source symbol multiplied by
its coefficient (section 4.4).

#### The connection id

`conn_id` appears on `DATA`, `REPAIR`, `PATH_CHALLENGE` and `PATH_RESPONSE`,
and on no other frame. A session is routed by it rather than by the UDP
4-tuple, which is what lets a session survive a peer address change. The
reverse-path frames `NAK`, `ACK` and `FEEDBACK` carry none: they are matched
to a session by the socket they arrive on.

### 4.3 Symbol packing

A symbol is a fixed `symbol_len` buffer:

| Offset | Size | Field |
|---|---|---|
| 0 | 2 | item length, u16-le |
| 2 | length | item bytes |
| 2 + length | rest | zero padding |

An item must satisfy `length + 2 <= symbol_len`. The fixed size is what lets
a repair be a linear combination of symbols; the padding is zero so it
contributes nothing to the sum.

A decoder must clamp the item length to the buffer it actually holds, so a
corrupt length yields a short item rather than a read past the end.

### 4.4 The coefficients

The generator id in `dt`'s high nibble selects how coefficients are derived.

| Id | Generator | Status |
|---|---|---|
| 0 | Per-repair generated coefficients | Not defined here. Must be refused. |
| 1 | Published taps | The only generator defined by this document. |

A receiver that cannot reproduce a repair's generator **must drop that
repair and count it**. It must not attempt to decode with different
coefficients. An equation with the wrong coefficients does not fail to
solve; it solves, to bytes that were never sent.

Generator 1 derives the coefficient for a source symbol from its **place in
the window** and the density, and from nothing else. Nothing is seeded,
keyed, or generated per repair.

Let `place` be the symbol's distance from the newest symbol in the window,
so the newest is 0, and let `density` be `dt & 0x0F`. Then the coefficient
is:

```
if place >= 64:                  0
else if place mod 16 <= density: TAPS[place]
else:                            0
```

A coefficient of zero means the symbol does not enter the equation.

Density is a **fraction of the window, not a reach into it**. A density of 0
takes one position in every sixteen, at places 0, 16, 32 and 48, spread
across the whole window rather than filling its newest end. A density of 15
takes every position. Reading density as a reach caps protection at the
newest 16 symbols however wide the window is opened, which silently
unprotects the oldest symbols of a wide window.

The table has 64 entries because the window may be opened to 64 symbols. A
table shorter than the window leaves its oldest symbols multiplied by
nothing.

`TAPS`, in order, place 0 first:

```
01 02 03 05 07 0b 0d 11  13 17 1d 1f 25 29 2b 2f
35 3b 3d 43 47 49 4f 53  59 61 65 67 6b 6d 71 7f
83 89 8b 95 97 9d a3 a7  ad b3 b5 bf c1 c5 c7 d3
df e3 e5 e9 ef f1 f5 f7  fb fd 04 08 0e 16 1a 22
```

These are constants of the format. The values are distinct and nonzero, and
those are the only properties they need: what limits recovery is which
repairs cover which symbols, not the values chosen.

### 4.5 NAK (type `0x0C`), receiver to sender

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `0x0C` |
| 1 | 4 each | missing `source_id`, u32-le, repeated |

Zero or more ids, read until fewer than four bytes remain.

### 4.6 ACK (type `0x0D`), receiver to sender

13 bytes.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `0x0D` |
| 1 | 4 | `delivered_through`, u32-le |
| 5 | 8 | `sack`, u64-le |

`delivered_through` is the cumulative in-order frontier: the first id **not**
yet delivered.

`sack` is a selective-acknowledgment bitmap of ids received above that
frontier. Bit `i` set means `delivered_through + 1 + i` has been received. A
sender releases each acknowledged id from its retransmit buffer, which is
what stops one hole holding the whole outstanding window.

### 4.7 FEEDBACK (type `0x0E`), receiver to sender

8 bytes.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `0x0E` |
| 1 | 1 | `loss_q8` |
| 2 | 1 | `burst_q8` |
| 3 | 1 | `cong_q8` |
| 4 | 2 | `rate_q16`, u16-le, megabits per second |
| 6 | 2 | `cap_q16`, u16-le, megabits per second |

The three quantized bytes are the receiver's fitted loss rate, mean burst
length, and congestion share of loss. `rate_q16` is the delivered goodput.
`cap_q16` is a packet-pair bottleneck capacity estimate, where zero means
"not measured yet" and must be ignored rather than treated as a capacity of
zero.

A conforming sender may ignore all of it; see section 3.

### 4.8 PATH_CHALLENGE (`0x12`) and PATH_RESPONSE (`0x13`)

17 bytes each.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `0x12` or `0x13` |
| 1 | 8 | `conn_id`, u64-le |
| 9 | 8 | nonce |

A receiver seeing a session's traffic arrive from a new address challenges
that address with an unpredictable nonce; the sender echoes the nonce in a
response, proving it can receive there. An off-path attacker cannot forge a
response to a challenge it never saw.

**Until an address answers, a receiver must send it at most three times
the bytes it has received from that address** (the anti-amplification
limit of RFC 9000 section 8). Without this an implementation is a
reflector: a spoofed source address turns it into an amplifier pointed at
whoever the address belongs to. This is a requirement, not a tuning
choice, and it is the one part of path validation an implementation
cannot decide for itself.

The reference implementation gives an address 500 ms to answer. A session
whose traffic moved to an address that never answers reverts to the
address it had before, and its challenge is resent on every receive pass
within the amplification limit. A challenge that would admit a new session
is resent at most every 50 ms, so a peer servicing its socket on a coarse
cadence drains a few frames before answering rather than a poll-rate
flood. These values are policy, and an implementation may choose its own.

## 5. The RS variant

### 5.1 DATA (type `0x01`)

Header is 13 bytes, then one shard.

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | `0x01` |
| 1 | 4 | `block_id`, u32-le |
| 5 | 1 | `shard_index` |
| 6 | 1 | `k` |
| 7 | 1 | `r` |
| 8 | 1 | `flags` |
| 9 | 4 | `epoch`, u32-le |
| 13 | `shard_len` | shard |

Shard indices `0` up to but not including `k` are data shards, carried
verbatim; `k` up to `k + r` are parity. Every datagram carries `k` and `r`,
so a receiver sizes the block from any shard of it.

`epoch` is a non-zero session identifier, distinct across restarts of a
sender, so a datagram from a previous session is recognizable rather than
mistaken for a current one.

A **data** shard's payload carries the item the same way an RLC symbol does:

| Offset in shard | Size | Field |
|---|---|---|
| 0 | 2 | item length, u16-le |
| 2 | length | item bytes |
| 2 + length | rest | zero padding |

so the largest item a block carries is `shard_len - 2`. A **parity** shard's
payload is the computed combination of the data shards' payloads, prefix
bytes included, and carries no prefix of its own.

`flags` bits:

| Bit | Name | Meaning |
|---|---|---|
| `0x01` | parity | This shard is parity (`shard_index >= k`) |
| `0x02` | outer | A cross-block outer-parity block, used opportunistically and never retransmission-tracked |
| `0x04` | retransmit | This datagram is a retransmission |

The retransmit bit matters beyond bookkeeping: a data shard arriving with it
set, for the first time, means its original was lost. Without it a receiver
cannot see the losses that retransmission repaired, and a loss estimate
built only on gaps would read a lossy link as clean.

#### Outer-parity block ids

A block carrying the outer flag also carries a `block_id` from a separate
number space. The id is self-describing: it names the segment of data
blocks it protects, its own index within that segment's parity, and the
shape of the segment, so a receiver learns the tower structure from the
wire and needs no out-of-band configuration.

| Bits | Width | Field | Range |
|---|---|---|---|
| 31 | 1 | Set on an outer-parity block, clear on a data block | 1 |
| 27-30 | 4 | `d`, data blocks in this segment | 1-15 |
| 24-26 | 3 | `r_outer`, outer-parity blocks for this segment | 1-7 |
| 8-23 | 16 | `segment` | 0-65535 |
| 0-7 | 8 | `outer_index` within this segment's parity | 0-255 |

A receiver decodes them as `d = (id >> 27) & 0xF`, `r_outer = (id >> 24)
& 0x7`, `segment = (id >> 8) & 0xFFFF`, `outer_index = id & 0xFF`.

**A receiver must discard an outer block whose `d` or `r_outer` is zero**,
and a sender must never emit one: those values describe a segment with no
data blocks or no parity, which names nothing decodable. Both fields are
therefore one-based on the wire.

Keeping bit 31 set is what stops an outer block colliding with the
sequential data-block ids.

A receiver that does not implement outer parity ignores these blocks
entirely and loses nothing it is entitled to: they are opportunistic
cross-block redundancy, never retransmission-tracked, and retransmission on
the data blocks is the correctness floor.

### 5.2 The code

A block of `k` data shards is extended with `r` parity shards over GF(2^8),
systematic, so the data shards ship unchanged, and MDS, so any `k` of the
`k + r` shards reconstruct the block.

The parity matrix is Cauchy. For parity row `j` and data column `c`:

```
C[j][c] = inverse( (k + j) XOR c )
```

where `inverse` is the GF(2^8) multiplicative inverse. Data indices below
`k` and parity indices at or above `k` are disjoint, so the `XOR` is never
zero and the inverse always exists. Every square submatrix of a Cauchy
matrix is invertible, which is what makes any `k` shards sufficient.

Parity shard `j` is the sum over `c` of `C[j][c]` times data shard `c`.

`k` and `r` must each be at least 1, and `k + r` must not exceed 256: a
block with no data shards or no parity shards is not a code, and past 256
the field has no distinct indices left to build the matrix from. The
reference implementation additionally bounds `k + r` to 32 per block,
because it tracks arrival in a 32-bit bitmap; an implementation with
different bookkeeping may carry more.

### 5.3 CONTROL (type `0x04`)

The RS control plane is one container carrying a sequence of type-tagged,
length-prefixed frames:

```
[0x04] ( [frame_type: u8] [length: varint] [payload: length bytes] )*
```

Both endpoints emit control datagrams holding whatever frames they have to
report, so the channel is symmetric: an acknowledgment from the receiver
and a timing beat from the sender are the same packet shape.

Integers wider than a byte use the QUIC variable-length encoding (RFC 9000
section 16): the top two bits of the first byte select a 1, 2, 4 or 8-byte
form. Byte-sized fields are written raw. Values must fit 62 bits; a wider
value is clamped to the 62-bit maximum rather than corrupting the stream,
so a clamped value never compares equal to the original it came from and
an echo of it is recognizable as clamped.

Frame types:

| Tag | Name | Meaning |
|---|---|---|
| `0x01` | Ack | Cumulative acknowledgment frontier |
| `0x02` | Nak | A block and its missing-shard bitmap |
| `0x03` | Loss | Fused loss, burstiness and delay-trend readings |
| `0x04` | Timing | Sender clock beat plus the peer beat echoed |
| `0x05` | Ring | Source-ring shape telemetry |
| `0x06` | Path | Observed TTL, ECN and hop count |
| `0x07` | Link | Peer link class and normalized quality |
| `0x08` | LossAcct | Highest peer sequence seen, for directional loss accounting |
| `0x09` | Pmtu | Observed path MTU |
| `0x0A` | BwProbe | One member of a bandwidth probe train |
| `0x0B` | Trace | A traceroute marker at a chosen TTL |
| `0x0C` | AvailBw | Receiver's available-bandwidth estimate |
| `0x0D` | Forecast | Receiver's forecast of the next-tick deliverable rate |
| `0x0E` | Periodicity | Detected handover cadence and time to the next spike |
| `0x0F` | SessionChallenge | Prove you can receive at this address |
| `0x10` | SessionResponse | The challenge echoed back |
| `0x11` | SessionAnnounce | The epoch this endpoint sends under |

#### Padding, and why rule 2 is not theoretical

A sender growing a control datagram to a target size appends a single
frame of type `0x7F` carrying zero bytes, sized so the datagram lands
exactly on the target. The type is deliberately one no receiver
implements: it exists to be length-skipped, which is the behavior
section 7 rule 2 requires.

An active bandwidth probe rides a known, large datagram, because its
inter-arrival dispersion only measures capacity at a stated packet size.
Padding is how it reaches that size without inventing a payload the peer
must understand.

Padding is appended only when the remaining gap holds the 3-byte header
and at least 64 bytes of body; below that the datagram is left short. The
64-byte floor keeps the length varint at exactly two bytes, which is what
makes the final size exact.

An implementation that rejects unknown frame types rather than skipping
them will fail against any peer that probes bandwidth.

## 6. Ordering and reliability

Both variants deliver every item, in the order the sender submitted them.

Forward error correction is primary: a loss the code covers is reconstructed
with no round trip. Retransmission is the floor: a loss the code cannot
cover is requested explicitly and resent. An implementation must provide
both, because the code alone cannot bound worst-case loss and retransmission
alone cannot avoid the round trip.

## 7. Compatibility rules

These are requirements, and each exists because violating it produces a
failure that appears only against a peer of a different vintage.

1. **Read the reverse-path frames by length, not by equality.** An `ACK` is
   acted on from 5 bytes and its `sack` read only if 13 are present. A
   `FEEDBACK` is acted on from 4 bytes, its `rate_q16` read only if 6 are
   present, and its `cap_q16` only if 8. A shorter frame from an older peer
   is a frame with its tail absent, not a malformed one. An implementation
   that rejects on length will interoperate until it meets an older peer.

2. **Skip unknown control frames by their length prefix.** A frame type an
   implementation does not recognize must be skipped, not treated as an
   error, and not used to abandon the rest of the packet. This is what
   allows a new signal to be added without a version bump.

3. **Never renumber a frame type or a packet type.** Append.

4. **Refuse a generator you cannot reproduce.** Count it, drop the repair,
   and continue. See section 4.4.

5. **Ignore a zero `cap_q16`.** Zero means unmeasured.

## 8. Test vectors

Two files accompany this specification. Both are generated from the
reference implementation and compared against it by a test, so the format
cannot change without them changing.

- `crates/subetha-cxc/vectors/rlc.txt` carries the generator ids, the full
  tap table, the complete coefficient grid for every place and density, and
  for each of six geometries the repairs a stated source stream produces and
  the symbols a stated loss pattern recovers. The geometries span windows of
  8, 16, 48 and 64 and densities of 0, 7 and 15.

- `crates/subetha-cxc/vectors/rs.txt` carries, for each of five geometries,
  the Cauchy matrix rows, the parity shards a stated data rule produces, and
  an erasure pattern withholding exactly `r` shards. The geometries span `r`
  from 1 to 6 and a high-parity case at `r = 16`.

Both state their source data as arithmetic on the symbol or shard index, so
an implementation reproduces them without a random source.

The geometries sweep deliberately. A single geometry admits an
implementation that agrees at that point and disagrees everywhere else, so
each parameter is exercised at more than one value. The window sweep is what
separates the two readings of density: a window of 8 and a density of 7
select the same coefficients whether density is read as a fraction of the
window or as a count of symbols reached into it, and only a wider window
tells them apart.

## 9. Stability

This is version 1 of the format, shipped in SubEtha 0.3.0. It does not
interoperate with the transport in earlier SubEtha releases: their `DATA`
and `REPAIR` headers carry no connection id, so every field after the type
byte sits eight bytes early, and their repairs name generator 0. A
version 1 receiver can diagnose only the generator, which section 4.4 has
it refuse and count. Version 1 must therefore not be spoken to a peer that
has not declared it.

Within version 1, the layouts in sections 4 and 5 will not change. New
signals will arrive as new control frame types (section 5.3) or new
generator ids (section 4.4), both of which existing implementations handle
by the rules of section 7.

## 10. References

### 10.1 Normative

An implementation needs these to be correct on the wire.

- **RFC 9000**, *QUIC: A UDP-Based Multiplexed and Secure Transport*.
  Section 16 defines the variable-length integer encoding used by every
  control frame field wider than a byte (section 5.3). Section 8 defines
  the anti-amplification limit that section 4.8 requires during path
  validation. Sections 17.2 and 17.3 define the first-byte bit that is
  set on every QUIC packet, long header and short, which is the bit this
  format keeps clear so a socket can carry QUIC alongside both codes
  (section 2).

### 10.2 Informative

These explain why parts of this format are shaped as they are. An
implementation conforms without reading them.

- **RFC 8681**, *Sliding Window Random Linear Code (RLC) Forward Erasure
  Correction (FEC) Schemes for FECFRAME*. Its density threshold carries
  the same meaning as the low nibble of `dt` in section 4.2: values 0 to
  15, with the average probability of a nonzero coefficient equal to
  `(density + 1) / 16`. Density is a proportion of the window, not a
  count of symbols reached into it, and the interoperability vectors of
  section 8 sweep window sizes specifically to distinguish the two
  readings. The coefficient generator differs: section 4.4 derives a
  coefficient from a symbol's place in the window and the density alone,
  where RFC 8681 draws its coefficients from a seeded pseudorandom
  generator.

- **RFC 9407**, *Tetrys: An On-the-Fly Network Coding Protocol*. Its
  elastic encoding window is the same object as the coding window of
  section 4.2, managed the other way round: a Tetrys sender widens and
  narrows its window from receiver feedback naming what has been received
  or rebuilt, so what a coded packet covers is a negotiated quantity. Here
  every repair names its own window outright in `first_source_id` and
  `window_size`, so a receiver that has seen none of the feedback still
  knows what a repair covers, and the feedback of sections 4.7 and 5.3
  informs sender policy alone (section 3).

- **RFC 9265**, *Forward Erasure Correction (FEC) Coding and Congestion
  Control in Transport*. Erasure coding repairs loss, and a repaired loss
  is invisible to anything downstream that counts gaps, which can leave a
  sender's congestion control blind to a link it is congesting. The
  raw-loss feedback frame of section 2.2 and the retransmit flag of
  section 5.1 exist to keep that measurement available: the first reports
  datagrams seen before any code is applied, the second marks a datagram
  whose original was lost even though retransmission recovered it.

- **RFC 3393**, *IP Packet Delay Variation Metric for IP Performance
  Metrics (IPPM)*. Section 4.1's `send_us` is read the way that metric is
  defined, as a differential measurement: a constant offset between two
  unsynchronized clocks cancels in the difference, which is why the two
  hosts need no common time base. What survives the difference is the
  variation, which is what a sender's policy can use; the absolute one-way
  delay does not survive it, and this format does not claim it.

### 10.3 The codes

These are where the two codes and their arithmetic come from. An
implementation conforms without reading them. Each entry says what this
format takes from the work and where it departs from it, because the
departures are the parts an implementer is most likely to assume.

- **J. S. Plank, K. M. Greenan and E. L. Miller**, *Screaming Fast Galois
  Field Arithmetic Using Intel SIMD Instructions*, 11th USENIX Conference
  on File and Storage Technologies (FAST '13), February 2013. Section 3.1
  names logarithm and antilogarithm tables as the
  conventional implementation and requires only that the products agree.
  This is the standard alternative: the same field arithmetic through SIMD
  shuffles over split nibble tables. It is faster and gives identical
  products, so it conforms.

- **T. Ho, M. Médard, R. Koetter, D. R. Karger, M. Effros, J. Shi and
  B. Leong**, *A Random Linear Network Coding Approach to Multicast*, IEEE
  Transactions on Information Theory, volume 52, number 10, October 2006,
  pages 4413 to 4430. A repair in section 4.2 is a linear combination of a
  window of source symbols, which is the object this paper analyzes. The
  difference is where the coefficients come from. There they are drawn at
  random, so a coded packet must carry the vector or a seed for it; here
  generator 1 derives them from a symbol's place in the window and the
  density alone (section 4.4), so a repair carries the one `dt` byte and
  two implementations agree without exchanging any coefficient at all.

- **S. Wunderlich, F. Gabriel, S. Pandi, F. H. P. Fitzek and
  M. Reisslein**, *Caterpillar RLNC (CRLNC): A Practical Finite Sliding
  Window RLNC Approach*, IEEE Access, volume 5, 2017, pages 20183 to
  20197. The window of section 4.2 is finite and it slides, which is the
  regime this measures, and for the reason section 4.2 serves: a sliding
  window lowers in-order delay against a block code, and holding it finite
  bounds what a decoder must keep. How wide to open it is sender policy
  here (section 3) and reaches the wire only as the `window_size` each
  repair names.

- **S. Feizi, D. E. Lucani and M. Médard**, *Tunable Sparse Network
  Coding*, International Zurich Seminar on Communications, 2012, pages 107
  to 110. The low nibble of `dt` is a density in this sense: the fraction
  of the window entering a repair with a nonzero coefficient, trading a
  cheaper decode against less information carried per repair. Where that
  work varies density across a session as a receiver accumulates packets,
  here it is a per-repair field and a sender may vary it or not
  (section 3); what section 4.4 fixes is that the density selects
  positions spread across the whole window rather than a reach into its
  newest end.

- **I. S. Reed and G. Solomon**, *Polynomial Codes Over Certain Finite
  Fields*, Journal of the Society for Industrial and Applied Mathematics,
  volume 8, number 2, 1960, pages 300 to 304. The block code of section
  5.2 is one of these: `k` data shards extended with `r` parity shards
  over a finite field so that any `k` of the `k + r` reconstruct the
  block. What section 5.2 fixes beyond the code is the field (section
  3.1), the particular matrix, and the systematic form that leaves the
  data shards unchanged on the wire.

- **J. Blömer, M. Kalfane, R. Karp, M. Karpinski, M. Luby and
  D. Zuckerman**, *An XOR-Based Erasure-Resilient Coding Scheme*,
  International Computer Science Institute, Berkeley, Technical Report
  TR-95-048, 1995. The parity matrix of section
  5.2 is the Cauchy construction this introduced for erasure coding, and
  the property section 5.2 rests on is the one it establishes: every
  square submatrix of a Cauchy matrix is invertible, which is what makes
  any `k` shards sufficient. The report then expands the field
  multiplications into XORs over a bit matrix; section 5.2 does not, and
  multiplies in GF(2^8) directly.
