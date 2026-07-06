//! Consumer-side exact-delivery reorder buffer for the best-effort
//! `MergeByStamp` (merge_tsc) ordering mode.
//!
//! `MergeByStamp` merges producer ring heads by stamp cheaply, but on a
//! host without an invariant TSC (stamps fall back to `SharedCounter`)
//! it has no watermark gate, so under producer lag it can hand the
//! consumer a smaller stamp late - a cross-core reservation-store race
//! ([`crate::ordering`]). `MergeStrict` closes that by waiting on every
//! producer's watermark: exact, but it couples release latency to the
//! slowest producer and scans every producer line per pop.
//!
//! This buffer takes the other route: pop best-effort and reorder on the
//! consumer side. It holds a bounded min-by-stamp window; once more than
//! `window` items are buffered it releases the minimum. A stamp that
//! arrives up to `window` positions late is still reordered ahead of the
//! items buffered after it, so delivery is stamp-monotone as long as
//! `window` covers the host's out-of-order displacement. It decouples
//! from the slowest producer (the cost is a bounded buffering latency,
//! not a watermark wait) and never touches a second CAS. Holes (a
//! `Full`-failed push consumes a `SharedCounter` value that never lands
//! in a ring) do not stall it: it releases the minimum of what it holds,
//! so a missing stamp is simply skipped, delivery stays monotone.
//!
//! Measured on a 16-vCPU KVM guest (SharedCounter, 4 producers -> 1
//! consumer): exact delivery at ~113 ns/item (window 8) vs ~138 ns for
//! `MergeStrict` and ~105 ns for the raw best-effort merge.
//!
//! # Adaptive window
//! The window starts at `floor` and grows (up to `cap`) whenever the
//! buffer catches a stamp below the last one it released - i.e. whenever
//! the current window was too small for the observed displacement.
//! [`corrections`](ReorderBuffer::corrections) reports how many times
//! that happened: it staying at zero means `floor` already covered the
//! host; a value that rises then stops means the window found the right
//! size; a value that keeps rising means displacement exceeds `cap`.
//!
//! # Guarantee (read this)
//! Delivery is exactly stamp-monotone **while `window >= max
//! displacement`**. This is NOT an unconditional, host-independent
//! guarantee: if a displacement spike exceeds the current window, one
//! item can be released out of order *before* the window grows to
//! absorb the next spike. Start `floor` at or above the host's expected
//! displacement for exact delivery from the first item. For an
//! unconditional guarantee regardless of host, use `MergeStrict`.

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::adaptive_ring::{AdaptiveRing, PinnedRing};
use crate::ordering::{OrderingMode, StampKind, STAMPED_PAYLOAD_BYTES};

/// Default starting window. The observed max out-of-order displacement
/// on a 16-vCPU KVM guest was <= 4; 8 leaves margin for exact delivery
/// from item zero on comparable hosts.
pub const DEFAULT_FLOOR: usize = 8;
/// Default window ceiling. Past this the buffering latency outweighs the
/// reorder benefit; a stream needing more should use `MergeStrict`.
pub const DEFAULT_CAP: usize = 1024;

#[derive(Clone)]
struct Entry {
    stamp: u64,
    seq: u64,
    len: usize,
    payload: [u8; STAMPED_PAYLOAD_BYTES],
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        (self.stamp, self.seq) == (other.stamp, other.seq)
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // (stamp, seq): seq breaks ties so equal stamps keep arrival
        // (FIFO) order and the heap is a total order.
        (self.stamp, self.seq).cmp(&(other.stamp, other.seq))
    }
}

/// Bounded min-by-stamp reorder buffer with an adaptive window. See the
/// module docs for the guarantee and the `MergeStrict` trade-off.
pub struct ReorderBuffer {
    heap: BinaryHeap<Reverse<Entry>>,
    window: usize,
    cap: usize,
    last_emitted: u64,
    have_emitted: bool,
    next_seq: u64,
    corrections: u64,
}

impl Default for ReorderBuffer {
    fn default() -> Self {
        Self::with_window(DEFAULT_FLOOR, DEFAULT_CAP)
    }
}

impl ReorderBuffer {
    /// A reorder buffer with the default floor/cap window.
    pub fn new() -> Self {
        Self::default()
    }

    /// A reorder buffer whose window starts at `floor` and grows (on a
    /// caught late stamp) up to `cap`. `cap` is clamped to at least
    /// `floor`.
    pub fn with_window(floor: usize, cap: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            window: floor,
            cap: cap.max(floor),
            last_emitted: 0,
            have_emitted: false,
            next_seq: 0,
            corrections: 0,
        }
    }

    /// Proactively raise the window (and, if needed, the cap) to at
    /// least `min_window`. Called when the producer count GROWS at
    /// runtime: displacement is bounded by the concurrent producer
    /// count, so widening on growth keeps delivery provably exact
    /// instead of waiting for a caught late stamp (which admits one
    /// out-of-order release before the reactive growth kicks in).
    pub fn widen_to(&mut self, min_window: usize) {
        if min_window > self.window {
            self.window = min_window;
            self.cap = self.cap.max(min_window);
        }
    }

    /// Buffer one popped item (its `stamp` and `payload`). `payload`
    /// must fit in `STAMPED_PAYLOAD_BYTES`; longer input is truncated to
    /// that bound (the ring never delivers more than a stamped slot
    /// holds).
    pub fn push(&mut self, stamp: u64, payload: &[u8]) {
        let len = payload.len().min(STAMPED_PAYLOAD_BYTES);
        let mut buf = [0u8; STAMPED_PAYLOAD_BYTES];
        buf[..len].copy_from_slice(&payload[..len]);
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(Reverse(Entry { stamp, seq, len, payload: buf }));
    }

    /// Release the next in-order item into `out` if the buffer holds more
    /// than `window` items, returning its stamp and payload length.
    /// Returns `None` while the buffer is still filling the window (the
    /// steady-state call: push a pop, then `try_take`).
    pub fn try_take(&mut self, out: &mut [u8]) -> Option<(u64, usize)> {
        if self.heap.len() > self.window {
            self.release(out)
        } else {
            None
        }
    }

    /// Drain-time release: pop the next in-order item regardless of the
    /// window. Call in a loop after the source is exhausted to flush the
    /// tail in stamp order.
    pub fn flush_one(&mut self, out: &mut [u8]) -> Option<(u64, usize)> {
        self.release(out)
    }

    fn release(&mut self, out: &mut [u8]) -> Option<(u64, usize)> {
        let Reverse(entry) = self.heap.pop()?;
        // A stamp below the last released one is a real out-of-order
        // delivery: the window was smaller than the displacement. Record
        // it and grow the window so the next spike is absorbed.
        if self.have_emitted && entry.stamp < self.last_emitted {
            self.corrections += 1;
            self.window = self.window.saturating_mul(2).min(self.cap).max(1);
        }
        self.last_emitted = entry.stamp;
        self.have_emitted = true;
        let n = entry.len.min(out.len());
        out[..n].copy_from_slice(&entry.payload[..n]);
        Some((entry.stamp, n))
    }

    /// Items currently buffered.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Whether the buffer holds no items.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Current adaptive window size.
    pub fn window(&self) -> usize {
        self.window
    }

    /// How many times a late stamp forced the window to grow. Zero means
    /// the starting floor covered every observed displacement; see the
    /// module docs on reading this.
    pub fn corrections(&self) -> u64 {
        self.corrections
    }
}

/// Ergonomic exact-delivery wrapper: pairs a stamped-ring consumer
/// handle ([`PinnedRing`]) with a [`ReorderBuffer`] so a `GlobalFifo`
/// (`MergeByStamp`) consumer receives items in exact stamp order without
/// the `MergeStrict` watermark coupling or a Vyukov second CAS.
///
/// ```ignore
/// let pin = ring.pin_current_shape();
/// let mut rx = ReorderingReceiver::new(&pin, 0);
/// let mut out = [0u8; 56];
/// // steady state:
/// while let Some((len, stamp)) = rx.try_recv(&mut out) {
///     deliver(&out[..len], stamp);
/// }
/// // end of stream - drain the buffered tail in order:
/// while let Some((len, stamp)) = rx.flush(&mut out) {
///     deliver(&out[..len], stamp);
/// }
/// ```
pub struct ReorderingReceiver<'a> {
    pin: &'a PinnedRing<'a>,
    consumer_id: usize,
    buf: ReorderBuffer,
    scratch: [u8; STAMPED_PAYLOAD_BYTES],
}

impl<'a> ReorderingReceiver<'a> {
    /// Wrap `pin` (a stamped-ring consumer handle) with the default
    /// adaptive window.
    pub fn new(pin: &'a PinnedRing<'a>, consumer_id: usize) -> Self {
        Self::with_window(pin, consumer_id, DEFAULT_FLOOR, DEFAULT_CAP)
    }

    /// Wrap `pin` with an explicit floor/cap window (see
    /// [`ReorderBuffer::with_window`]).
    pub fn with_window(
        pin: &'a PinnedRing<'a>,
        consumer_id: usize,
        floor: usize,
        cap: usize,
    ) -> Self {
        Self {
            pin,
            consumer_id,
            buf: ReorderBuffer::with_window(floor, cap),
            scratch: [0u8; STAMPED_PAYLOAD_BYTES],
        }
    }

    /// Pop one item from the ring into the buffer (if available), then
    /// release the next in-order item once the window is full. Returns
    /// `(payload_len, stamp)`, or `None` while the window is still
    /// filling or the ring is momentarily empty. On end of stream,
    /// finish with [`flush`](Self::flush).
    pub fn try_recv(&mut self, out: &mut [u8]) -> Option<(usize, u64)> {
        if let Ok((n, stamp)) =
            self.pin.ordered_try_pop_with_stamp(self.consumer_id, &mut self.scratch)
        {
            self.buf.push(stamp, &self.scratch[..n]);
        }
        self.buf.try_take(out).map(|(stamp, len)| (len, stamp))
    }

    /// Drain the buffered tail in stamp order. Call in a loop after the
    /// producers have finished to release the last `window` items.
    pub fn flush(&mut self, out: &mut [u8]) -> Option<(usize, u64)> {
        self.buf.flush_one(out).map(|(stamp, len)| (len, stamp))
    }

    /// Times the adaptive window had to grow (see
    /// [`ReorderBuffer::corrections`]).
    pub fn corrections(&self) -> u64 {
        self.buf.corrections()
    }

    /// Current adaptive window.
    pub fn window(&self) -> usize {
        self.buf.window()
    }
}

/// Producer-count ceiling for the reorder-buffer strategy. The
/// best-effort merge's out-of-order displacement is bounded by the
/// concurrent producer count, so a reorder window `>= producers` is
/// provably exact. Above this many producers the window (and its
/// buffering latency) would be too large, so the adaptive receiver
/// morphs the ring to `MergeStrict` instead.
pub const REORDER_PRODUCER_CAP: usize = 256;

enum ExactMode {
    /// SharedCounter, producers <= cap: keep `MergeByStamp` and reorder
    /// on the consumer with a window sized to the producer count.
    Reorder(ReorderBuffer),
    /// SharedCounter, producers > cap: ring morphed to `MergeStrict`;
    /// the pop is already exact (watermark gate), no buffering.
    Strict,
    /// Time-based stamps (freshness-guarded merge) or unstamped: the pop
    /// order needs no consumer-side correction.
    Direct,
}

/// Automatic exact-delivery consumer for a stamped `GlobalFifo` ring.
///
/// Picks the cheapest strategy that is exact for the ring's
/// configuration, mirroring the adaptive-ring philosophy (use the fast
/// path while it is correct, morph when it is not):
///
/// - **SharedCounter stamps** (the config whose cheap `MergeByStamp`
///   merge can deliver out of order under producer lag): if the
///   producer count fits a bounded reorder window
///   (`<= REORDER_PRODUCER_CAP`), keep `MergeByStamp` and correct on the
///   consumer with a [`ReorderBuffer`] sized `>= producers` (provably
///   exact, ~9% over the raw merge). Otherwise morph the ring to
///   `MergeStrict` (exact at any scale via the watermark wait).
/// - **Time-based stamps** (`Tsc`/`Monotonic`, freshness-guarded) or an
///   unstamped ring: deliver directly, no correction needed.
///
/// ```ignore
/// let mut rx = AdaptiveOrderedReceiver::new(&ring, 0);
/// let mut out = [0u8; 56];
/// while let Some((len, stamp)) = rx.try_recv(&mut out) { deliver(&out[..len], stamp); }
/// while let Some((len, stamp)) = rx.flush(&mut out) { deliver(&out[..len], stamp); } // end of stream
/// ```
pub struct AdaptiveOrderedReceiver<'a> {
    ring: &'a AdaptiveRing,
    consumer_id: usize,
    mode: ExactMode,
}

impl<'a> AdaptiveOrderedReceiver<'a> {
    /// Set up exact delivery for `ring`, auto-selecting the strategy and
    /// setting the ring's merge mode to match. Call once per consumer.
    ///
    /// The reorder window sizes to the PUBLISHED producer-slot count
    /// (pre-allocated + grown), so a ring that grew past its
    /// construction hint still gets the provably-exact window.
    pub fn new(ring: &'a AdaptiveRing, consumer_id: usize) -> Self {
        let producers = ring.published_producers().max(ring.max_producers());
        let mode = match ring.stamp_kind() {
            Some(StampKind::SharedCounter) => {
                if producers <= REORDER_PRODUCER_CAP {
                    ring.set_ordering_mode(OrderingMode::MergeByStamp).ok();
                    let window = producers.max(DEFAULT_FLOOR);
                    ExactMode::Reorder(ReorderBuffer::with_window(
                        window,
                        window.max(DEFAULT_CAP),
                    ))
                } else {
                    ring.set_ordering_mode(OrderingMode::MergeStrict).ok();
                    ExactMode::Strict
                }
            }
            _ => ExactMode::Direct,
        };
        Self { ring, consumer_id, mode }
    }

    /// Deliver the next in-order item, or `None` if none is ready.
    /// Under the reorder strategy this holds the window during
    /// streaming; drain the tail with [`flush`](Self::flush) at end of
    /// stream.
    pub fn try_recv(&mut self, out: &mut [u8]) -> Option<(usize, u64)> {
        let pin = self.ring.pin_current_shape();
        let cid = self.consumer_id;
        match &mut self.mode {
            ExactMode::Reorder(rb) => {
                // Producers can GROW mid-stream: widen the window with
                // them (displacement is bounded by producer count).
                // Past the reorder cap, flip the ring to the strict
                // watermark wait; the buffer then sees monotone input
                // and delivery stays exact.
                let producers = self.ring.published_producers();
                if producers > rb.window() {
                    if producers > REORDER_PRODUCER_CAP {
                        self.ring
                            .set_ordering_mode(OrderingMode::MergeStrict)
                            .ok();
                    }
                    rb.widen_to(producers.min(REORDER_PRODUCER_CAP));
                }
                let mut scratch = [0u8; STAMPED_PAYLOAD_BYTES];
                if let Ok((n, stamp)) = pin.ordered_try_pop_with_stamp(cid, &mut scratch) {
                    rb.push(stamp, &scratch[..n]);
                }
                rb.try_take(out).map(|(stamp, len)| (len, stamp))
            }
            ExactMode::Strict | ExactMode::Direct => {
                match pin.ordered_try_pop_with_stamp(cid, out) {
                    Ok((n, stamp)) => Some((n, stamp)),
                    Err(_) => None,
                }
            }
        }
    }

    /// Drain the buffered tail in order (reorder strategy only); `None`
    /// once drained or under the strict/direct strategies.
    pub fn flush(&mut self, out: &mut [u8]) -> Option<(usize, u64)> {
        match &mut self.mode {
            ExactMode::Reorder(rb) => rb.flush_one(out).map(|(stamp, len)| (len, stamp)),
            _ => None,
        }
    }

    /// Which exact-delivery strategy was auto-selected: `"reorder"`,
    /// `"strict"`, or `"direct"`.
    pub fn strategy(&self) -> &'static str {
        match self.mode {
            ExactMode::Reorder(_) => "reorder",
            ExactMode::Strict => "strict",
            ExactMode::Direct => "direct",
        }
    }

    /// Times the reorder window had to grow (reorder strategy only); a
    /// nonzero value means the observed displacement exceeded the
    /// producer-count-derived window.
    pub fn corrections(&self) -> u64 {
        match &self.mode {
            ExactMode::Reorder(rb) => rb.corrections(),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A permutation of `1..=n` where each value is displaced from its
    /// sorted index by at most `d` (blocks of `d+1` reversed).
    fn displaced(n: u64, d: usize) -> Vec<u64> {
        let mut v = Vec::with_capacity(n as usize);
        let block = d as u64 + 1;
        let mut base = 1u64;
        while base <= n {
            let end = (base + block - 1).min(n);
            for s in (base..=end).rev() {
                v.push(s);
            }
            base = end + 1;
        }
        v
    }

    /// Drive a stream through the buffer at a fixed window (no growth):
    /// returns (emitted_stamps, corrections).
    fn drive(stream: &[u64], floor: usize) -> (Vec<u64>, u64) {
        let mut rb = ReorderBuffer::with_window(floor, floor); // cap==floor: no growth
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        let mut emitted = Vec::new();
        for &s in stream {
            rb.push(s, &s.to_le_bytes());
            if let Some((stamp, _)) = rb.try_take(&mut out) {
                emitted.push(stamp);
            }
        }
        while let Some((stamp, _)) = rb.flush_one(&mut out) {
            emitted.push(stamp);
        }
        (emitted, rb.corrections())
    }

    fn is_monotone(v: &[u64]) -> bool {
        v.windows(2).all(|w| w[1] >= w[0])
    }

    #[test]
    fn in_order_stream_is_untouched() {
        let stream: Vec<u64> = (1..=1000).collect();
        let (emitted, corr) = drive(&stream, 8);
        assert_eq!(emitted, stream);
        assert_eq!(corr, 0);
    }

    #[test]
    fn window_at_or_above_displacement_is_exact() {
        for d in [1usize, 3, 8] {
            let stream = displaced(2000, d);
            let (emitted, corr) = drive(&stream, d); // window == displacement
            assert!(is_monotone(&emitted), "d={d}: not monotone");
            assert_eq!(corr, 0, "d={d}: unexpected corrections at window==d");
            assert_eq!(emitted.len(), stream.len());
        }
    }

    #[test]
    fn window_below_displacement_slips_and_is_flagged() {
        // window 1 cannot cover displacement 4: some items slip and the
        // corrections counter records it.
        let stream = displaced(2000, 4);
        let (emitted, corr) = drive(&stream, 1);
        assert!(corr > 0, "expected corrections when window < displacement");
        assert_eq!(emitted.len(), stream.len());
    }

    #[test]
    fn adaptive_window_grows_toward_displacement() {
        // Start below the displacement with headroom to grow: the window
        // climbs and corrections are what drive the growth.
        let stream = displaced(4000, 8);
        let mut rb = ReorderBuffer::with_window(2, 64);
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        for &s in &stream {
            rb.push(s, &s.to_le_bytes());
            while rb.try_take(&mut out).is_some() {}
        }
        while rb.flush_one(&mut out).is_some() {}
        assert!(rb.window() > 2, "window should have grown, got {}", rb.window());
        assert!(rb.corrections() > 0, "growth is driven by caught slips");
        assert!(rb.window() >= 8, "window should reach the displacement, got {}", rb.window());
    }

    #[test]
    fn holes_do_not_stall_and_stay_monotone() {
        // Stamps 1,2,4,5,7,8... (every third is a hole) arriving with a
        // small local swap; delivery must stay monotone and skip holes.
        let mut stream = Vec::new();
        let mut s = 1u64;
        while s < 300 {
            // swap a pair to force reorder within the window
            stream.push(s + 1);
            stream.push(s);
            s += 3; // skip s+2 -> a hole
        }
        let (emitted, _) = drive(&stream, 8);
        assert!(is_monotone(&emitted), "delivery must be monotone across holes");
        assert_eq!(emitted.len(), stream.len(), "no item dropped");
    }

    #[test]
    fn payload_round_trips() {
        let mut rb = ReorderBuffer::with_window(1, 1);
        rb.push(10, b"hello");
        rb.push(9, b"world");
        let mut out = [0u8; STAMPED_PAYLOAD_BYTES];
        // window 1, 2 buffered -> release min (stamp 9, "world")
        let (stamp, n) = rb.try_take(&mut out).expect("release");
        assert_eq!(stamp, 9);
        assert_eq!(&out[..n], b"world");
        let (stamp, n) = rb.flush_one(&mut out).expect("flush");
        assert_eq!(stamp, 10);
        assert_eq!(&out[..n], b"hello");
    }

    #[test]
    fn adaptive_receiver_selects_strategy_by_config() {
        use crate::adaptive_ring::{AdaptiveRing, RingShape};

        // SharedCounter, few producers -> reorder (window covers them).
        let ring = AdaptiveRing::create_anon(4, 1, 256)
            .unwrap()
            .with_ordering_stamps_kind(StampKind::SharedCounter)
            .unwrap();
        ring.morph_to(RingShape::Mpsc).unwrap();
        let rx = AdaptiveOrderedReceiver::new(&ring, 0);
        assert_eq!(rx.strategy(), "reorder");
        assert_eq!(ring.ordering_mode(), Some(OrderingMode::MergeByStamp));

        // SharedCounter, producers above the cap -> morph to strict.
        let big = AdaptiveRing::create_anon(REORDER_PRODUCER_CAP + 1, 1, 64)
            .unwrap()
            .with_ordering_stamps_kind(StampKind::SharedCounter)
            .unwrap();
        big.morph_to(RingShape::Mpsc).unwrap();
        let rx = AdaptiveOrderedReceiver::new(&big, 0);
        assert_eq!(rx.strategy(), "strict");
        assert_eq!(big.ordering_mode(), Some(OrderingMode::MergeStrict));

        // Time-based stamp (freshness-guarded merge) -> direct.
        let tsc = AdaptiveRing::create_anon(4, 1, 256)
            .unwrap()
            .with_ordering_stamps_kind(StampKind::Tsc)
            .unwrap();
        tsc.morph_to(RingShape::Mpsc).unwrap();
        let rx = AdaptiveOrderedReceiver::new(&tsc, 0);
        assert_eq!(rx.strategy(), "direct");
    }
}
