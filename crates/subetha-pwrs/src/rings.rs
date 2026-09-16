//! The rings and the other structures that move items: single-producer,
//! broadcast, resizable, relocatable and adaptive rings, the ordered
//! reader and its window, the stack, the deque, publish and subscribe,
//! the Lamport pair, and the frame region for payloads too large for a
//! slot.

use std::sync::Arc;

use pwrs::prelude::*;

use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::capacity_adaptive_ring::CapacityAdaptiveRing;
use subetha_cxc::frame_region::FrameRegion as SubethaFrameRegion;
use subetha_cxc::locale_adaptive_ring::{Locale as SubethaLocale, LocaleAdaptiveRing};
use subetha_cxc::ordering::{OrderingMode as SubethaOrderingMode, StampKind as SubethaStampKind, STAMPED_PAYLOAD_BYTES};
use subetha_cxc::protocol_pubsub::{PubSubReadError, PubSubRing, PUBSUB_PAYLOAD_BYTES};
use subetha_cxc::raw_deque::RawDeque;
use subetha_cxc::raw_treiber_stack::RawTreiberStack;
use subetha_cxc::reorder::{AdaptiveOrderedReceiver, ReorderBuffer, DEFAULT_CAP as REORDER_DEFAULT_CAP, DEFAULT_FLOOR as REORDER_DEFAULT_FLOOR};
use subetha_cxc::shared_broadcast_ring::{BroadcastError, SharedBroadcastRing, BROADCAST_PAYLOAD_BYTES};
use subetha_cxc::shared_deque::DequeError;
use subetha_cxc::shared_ring::{Consumer, Producer, RingError, SharedRingSpsc};
use subetha_cxc::shared_treiber_stack::StackError;
use subetha_cxc::spsc_ring::{SpscRingCore, SPSC_PAYLOAD_BYTES};

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, open_err, out_bytes, power_of_two, size, StampedItem};
use crate::primitives::layout;

assert_send!(
    SpscRing, BroadcastRing, CapacityRing, LocaleRing, Ring, OrderedReceiver, ReorderWindow, Stack, Deque, PubSub, Subscriber,
    LamportProducer, LamportConsumer, FrameRegion
);

/// The discipline a stamped ring's reader applies to the order items
/// come out in.
#[psenum(name = "SubEtha.OrderingMode")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OrderingMode {
    /// Items come out as they arrived.
    #[default]
    Unordered,
    /// Items are merged by their stamps where the stamps allow.
    MergeByStamp,
    /// Items come out in stamp order and nothing is released early.
    MergeStrict,
}

impl OrderingMode {
    fn rust(self) -> SubethaOrderingMode {
        match self {
            OrderingMode::Unordered => SubethaOrderingMode::Unordered,
            OrderingMode::MergeByStamp => SubethaOrderingMode::MergeByStamp,
            OrderingMode::MergeStrict => SubethaOrderingMode::MergeStrict,
        }
    }

    fn from_rust(mode: SubethaOrderingMode) -> Self {
        match mode {
            SubethaOrderingMode::Unordered => OrderingMode::Unordered,
            SubethaOrderingMode::MergeByStamp => OrderingMode::MergeByStamp,
            SubethaOrderingMode::MergeStrict => OrderingMode::MergeStrict,
        }
    }
}

/// Where a relocatable ring keeps its bytes.
#[psenum(name = "SubEtha.Locale")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Locale {
    /// Memory private to one process, the cheapest to send through.
    #[default]
    Anon,
    /// A mapped file that survives the process and reaches any other
    /// one that opens it.
    File,
    /// Named memory other processes can reach but which never goes to
    /// disk.
    ShmFs,
}

impl Locale {
    pub(crate) fn rust(self) -> SubethaLocale {
        match self {
            Locale::Anon => SubethaLocale::Anon,
            Locale::File => SubethaLocale::File,
            Locale::ShmFs => SubethaLocale::ShmFs,
        }
    }

    pub(crate) fn from_rust(locale: SubethaLocale) -> Self {
        match locale {
            SubethaLocale::Anon => Locale::Anon,
            SubethaLocale::File => Locale::File,
            SubethaLocale::ShmFs => Locale::ShmFs,
        }
    }
}

/// The mark an adaptive ring puts on each item so a reader can put the
/// items back in the order their senders made them.
#[psenum(name = "SubEtha.StampKind")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum StampKind {
    /// The processor's own cycle counter.
    Tsc,
    /// A count shared between the senders, the only kind that gives a
    /// total order at any speed.
    #[default]
    Counter,
    /// The system's monotonic clock.
    Monotonic,
}

impl StampKind {
    fn rust(self) -> SubethaStampKind {
        match self {
            StampKind::Tsc => SubethaStampKind::Tsc,
            StampKind::Counter => SubethaStampKind::SharedCounter,
            StampKind::Monotonic => SubethaStampKind::Monotonic,
        }
    }

    fn from_rust(kind: SubethaStampKind) -> Self {
        match kind {
            SubethaStampKind::Tsc => StampKind::Tsc,
            SubethaStampKind::SharedCounter => StampKind::Counter,
            SubethaStampKind::Monotonic => StampKind::Monotonic,
        }
    }
}

/// Pushes each of `items` through `push` until one is refused, and
/// returns how many went in, so the caller keeps the rest.
fn push_each(items: &[PsObject], mut push: impl FnMut(&[u8]) -> PsResult<bool>) -> PsResult<u64> {
    let mut pushed = 0;
    for item in items {
        let item = bytes(item)?;
        if !push(&item)? {
            break;
        }
        pushed += 1;
    }
    Ok(pushed)
}

/// Pushes the `item_len`-byte chunks of `data` through `push` until one
/// is refused, and returns how many went in.
fn push_packed(data: &PsObject, item_len: u64, mut push: impl FnMut(&[u8]) -> PsResult<bool>) -> PsResult<u64> {
    let item_len = size(item_len, "the item length")?;
    if item_len == 0 {
        return Err(arg_err("the item length must not be zero"));
    }
    let data = bytes(data)?;
    let mut pushed = 0;
    for chunk in data.chunks(item_len) {
        if !push(chunk)? {
            break;
        }
        pushed += 1;
    }
    Ok(pushed)
}

/// Pops through `pop`, which fills a buffer and answers the length or
/// nothing, up to `max_items` times, and returns a `byte[]` per item.
fn pop_each(max_items: u64, slot: usize, mut pop: impl FnMut(&mut [u8]) -> PsResult<Option<usize>>) -> PsResult<Vec<PsObject>> {
    let mut taken = Vec::new();
    let mut out = vec![0u8; slot];
    for _ in 0..max_items {
        match pop(&mut out)? {
            Some(n) => taken.push(out_bytes(&out[..n])?),
            None => break,
        }
    }
    Ok(taken)
}

/// Pops through `pop` up to `max_items` times into one `byte[]` packed
/// end to end at `slot` bytes per item, and returns it with how many
/// items it holds.
fn pop_packed(max_items: u64, slot: usize, mut pop: impl FnMut(&mut [u8]) -> PsResult<Option<usize>>) -> PsResult<PackedItems> {
    let max_items = size(max_items, "the item count")?;
    let mut packed = vec![0u8; max_items.saturating_mul(slot)];
    let mut taken = 0;
    for i in 0..max_items {
        let at = i * slot;
        match pop(&mut packed[at..at + slot])? {
            Some(_) => taken += 1,
            None => break,
        }
    }
    packed.truncate(taken * slot);
    Ok(PackedItems { count: taken as u64, bytes: out_bytes(&packed)? })
}

/// Items packed end to end in one `byte[]`, each the slot's full width.
#[psclass(name = "SubEtha.PackedItems")]
#[derive(Clone, Default)]
pub struct PackedItems {
    /// How many items the bytes hold.
    pub count: u64,
    /// The items, one slot width each, a `byte[]`.
    pub bytes: PsObject,
}

/// A single-producer single-consumer ring in a file two processes map.
///
/// Slots are a fixed 64 bytes. `Push` and `Pop` are the single-item
/// calls; `PushMany` and `PopMany` carry a run of items across one
/// boundary crossing, and `PushPacked` and `PopPacked` carry them in
/// one `byte[]` without an object per item, which is the fastest shape.
#[psclass(name = "SubEtha.SpscRing", mode = proxy)]
pub struct SpscRing {
    /// The file the ring lives in.
    pub path: String,
    /// How many slots the ring holds.
    pub capacity: u64,
    /// The bytes one slot carries; a longer item is refused.
    pub payload_size: u64,
    #[psfield(skip)]
    inner: SpscRingCore,
}

impl SpscRing {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        let slots = size(capacity, "the capacity")?;
        let inner = if open { SpscRingCore::open(&path, slots) } else { SpscRingCore::create(&path, slots) }
            .map_err(|e| open_err("the ring", &path, e))?;
        Ok(Self { path, capacity, payload_size: SPSC_PAYLOAD_BYTES as u64, inner })
    }

    fn try_push(&self, item: &[u8]) -> PsResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(op_err("pushing", e)),
        }
    }

    fn try_pop(&self, out: &mut [u8]) -> PsResult<Option<usize>> {
        match self.inner.try_pop(out) {
            Ok(n) => Ok(Some(n)),
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(op_err("popping", e)),
        }
    }
}

/// The operations of a `SubEtha.SpscRing`.
#[psmethods]
impl SpscRing {
    /// Pushes one item. False means the ring was full, which is an
    /// answer rather than a failure.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_push(&item)
    }

    /// Pops one item, or `$null` when the ring is empty.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.try_pop(&mut out)? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Pushes a run of items, stopping at the first the ring refuses,
    /// and returns how many went in, so the caller keeps the rest.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        push_each(&items, |item| self.try_push(item))
    }

    /// Pushes items packed end to end in one `byte[]`, `itemLen` bytes
    /// each, and returns how many went in.
    pub fn push_packed(&self, data: PsObject, item_len: u64) -> PsResult<u64> {
        push_packed(&data, item_len, |item| self.try_push(item))
    }

    /// Pops up to `maxItems`, stopping when the ring runs empty.
    pub fn pop_many(&self, max_items: u64) -> PsResult<Vec<PsObject>> {
        pop_each(max_items, SPSC_PAYLOAD_BYTES, |out| self.try_pop(out))
    }

    /// Pops up to `maxItems` into one `byte[]` packed end to end, a
    /// slot's width each.
    pub fn pop_packed(&self, max_items: u64) -> PsResult<PackedItems> {
        pop_packed(max_items, SPSC_PAYLOAD_BYTES, |out| self.try_pop(out))
    }
}

/// Obtains the single-producer ring at Path holding Capacity slots,
/// creating it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaSpscRing", alias = "New-SESpscRing", output = ["SubEtha.SpscRing"])]
#[derive(Default)]
pub struct NewSubEthaSpscRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaSpscRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(SpscRing::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the single-producer ring at Path, which must exist with
/// the Capacity it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaSpscRing", alias = "Open-SESpscRing", output = ["SubEtha.SpscRing"])]
#[derive(Default)]
pub struct OpenSubEthaSpscRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaSpscRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(SpscRing::obtain(path, self.capacity, true)?)
    }
}

/// A broadcast ring: one producer, many consumers, each reading every
/// item at its own pace. A consumer registers to get its own position
/// and reads with that.
#[psclass(name = "SubEtha.BroadcastRing", mode = proxy)]
pub struct BroadcastRing {
    /// The file the ring lives in.
    pub path: String,
    /// How many slots the ring holds.
    pub capacity: u64,
    /// The bytes one slot carries; a longer item is refused.
    pub payload_size: u64,
    #[psfield(skip)]
    inner: SharedBroadcastRing,
}

impl BroadcastRing {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        let slots = size(capacity, "the capacity")?;
        let inner = if open { SharedBroadcastRing::open(&path, slots) } else { SharedBroadcastRing::create(&path, slots) }
            .map_err(|e| open_err("the broadcast ring", &path, e))?;
        Ok(Self { path, capacity, payload_size: BROADCAST_PAYLOAD_BYTES as u64, inner })
    }

    fn try_push(&self, item: &[u8]) -> PsResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(BroadcastError::Full) => Ok(false),
            Err(e) => Err(op_err("publishing", e)),
        }
    }

    fn try_recv(&self, consumer: usize, out: &mut [u8]) -> PsResult<Option<usize>> {
        match self.inner.try_recv(consumer, out) {
            Ok(n) => Ok(Some(n)),
            Err(BroadcastError::Empty) => Ok(None),
            Err(e) => Err(op_err("receiving", e)),
        }
    }
}

/// The operations of a `SubEtha.BroadcastRing`.
#[psmethods]
impl BroadcastRing {
    /// Takes a consumer position. Every registered consumer sees every
    /// item published after it registered.
    pub fn register_consumer(&self) -> PsResult<u64> {
        self.inner.register_consumer().map(|c| c as u64).map_err(|e| op_err("registering a consumer", e))
    }

    /// Gives a consumer position back.
    pub fn unregister_consumer(&self, consumer: u64) -> PsResult<()> {
        self.inner.unregister_consumer(size(consumer, "the consumer")?);
        Ok(())
    }

    /// How far behind the producer a consumer is.
    pub fn lag(&self, consumer: u64) -> PsResult<u64> {
        Ok(self.inner.lag(size(consumer, "the consumer")?))
    }

    /// The position the producer has reached.
    pub fn producer_position(&self) -> PsResult<u64> {
        Ok(self.inner.producer_position())
    }

    /// How many consumers are registered.
    pub fn active_consumers(&self) -> PsResult<u64> {
        Ok(self.inner.active_consumer_count() as u64)
    }

    /// Publishes one item. False means the ring was full.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_push(&item)
    }

    /// Publishes a run of items, stopping at the first the ring
    /// refuses, and returns how many went in.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        push_each(&items, |item| self.try_push(item))
    }

    /// The next item for `consumer`, or `$null` when it has caught up
    /// with the producer.
    pub fn recv(&self, consumer: u64) -> PsResult<Option<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        let mut out = vec![0u8; BROADCAST_PAYLOAD_BYTES];
        match self.try_recv(consumer, &mut out)? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Up to `maxItems` for `consumer` in one call.
    pub fn recv_many(&self, consumer: u64, max_items: u64) -> PsResult<Vec<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        pop_each(max_items, BROADCAST_PAYLOAD_BYTES, |out| self.try_recv(consumer, out))
    }
}

/// Obtains the broadcast ring at Path holding Capacity slots, creating
/// it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaBroadcastRing", alias = "New-SEBroadcastRing", output = ["SubEtha.BroadcastRing"])]
#[derive(Default)]
pub struct NewSubEthaBroadcastRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaBroadcastRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BroadcastRing::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the broadcast ring at Path, which must exist with the
/// Capacity it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaBroadcastRing", alias = "Open-SEBroadcastRing", output = ["SubEtha.BroadcastRing"])]
#[derive(Default)]
pub struct OpenSubEthaBroadcastRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaBroadcastRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BroadcastRing::obtain(path, self.capacity, true)?)
    }
}

/// A ring that can be resized while it is in use.
///
/// A morph leaves the old backing in place until its items have been
/// drained, so nothing in flight is lost; the consumer reads the stale
/// backings oldest first and only then the new one. That is why a
/// resize is a call rather than a rebuild.
#[psclass(name = "SubEtha.CapacityRing", mode = proxy)]
pub struct CapacityRing {
    /// The file the ring lives in.
    pub path: String,
    /// How many producers may register.
    pub max_producers: u64,
    /// How many consumers may register.
    pub max_consumers: u64,
    /// Whether the items carry ordering stamps, fixed when the ring is
    /// built.
    pub stamped: bool,
    #[psfield(skip)]
    inner: CapacityAdaptiveRing,
}

impl CapacityRing {
    fn obtain(path: String, capacity: u64, producers: u64, consumers: u64, stamped: bool, open: bool) -> PsResult<Self> {
        let slots = power_of_two(capacity)?;
        if producers < 1 || consumers < 1 {
            return Err(arg_err("a ring needs at least one producer and one consumer"));
        }
        let p = size(producers, "the producer count")?;
        let c = size(consumers, "the consumer count")?;
        let inner = if open {
            CapacityAdaptiveRing::open(&path, p, c, slots, stamped)
        } else if stamped {
            CapacityAdaptiveRing::create_stamped(&path, p, c, slots)
        } else {
            CapacityAdaptiveRing::create(&path, p, c, slots)
        }
        .map_err(|e| open_err("the ring", &path, e))?;
        Ok(Self { path, max_producers: producers, max_consumers: consumers, stamped, inner })
    }

    fn try_send(&self, producer: usize, item: &[u8]) -> PsResult<bool> {
        match self.inner.try_send(producer, item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(op_err("sending", e)),
        }
    }

    fn try_recv(&self, consumer: usize, out: &mut [u8]) -> PsResult<Option<usize>> {
        match self.inner.try_recv(consumer, out) {
            Ok(n) => Ok(Some(n)),
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(op_err("receiving", e)),
        }
    }
}

/// The operations of a `SubEtha.CapacityRing`.
#[psmethods]
impl CapacityRing {
    /// The capacity right now, which a morph changes.
    pub fn capacity(&self) -> PsResult<u64> {
        Ok(self.inner.current_capacity() as u64)
    }

    /// Steps when the backing changes, so a holder of a pin can tell
    /// that what it pinned has been superseded.
    pub fn pin_generation(&self) -> PsResult<u64> {
        Ok(self.inner.pin_generation())
    }

    /// Takes a producer id. Every send names one.
    pub fn register_producer(&self) -> PsResult<u64> {
        self.inner.register_producer().map(|p| p as u64).map_err(|e| op_err("registering a producer", e))
    }

    /// Takes a consumer id. Every receive names one.
    pub fn register_consumer(&self) -> PsResult<u64> {
        self.inner.register_consumer().map(|c| c as u64).map_err(|e| op_err("registering a consumer", e))
    }

    /// Sends one item as `producer`. False means the ring was full.
    pub fn send(&self, producer: u64, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_send(size(producer, "the producer")?, &item)
    }

    /// Sends a run of items as `producer`, stopping at the first
    /// refusal, and returns how many went in.
    pub fn send_many(&self, producer: u64, items: Vec<PsObject>) -> PsResult<u64> {
        let producer = size(producer, "the producer")?;
        push_each(&items, |item| self.try_send(producer, item))
    }

    /// The next item for `consumer`, or `$null` when the ring is
    /// empty.
    pub fn recv(&self, consumer: u64) -> PsResult<Option<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.try_recv(consumer, &mut out)? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Up to `maxItems` for `consumer` in one call.
    pub fn recv_many(&self, consumer: u64, max_items: u64) -> PsResult<Vec<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        pop_each(max_items, SPSC_PAYLOAD_BYTES, |out| self.try_recv(consumer, out))
    }

    /// Resizes to `capacity`, a power of two. Items already in the ring
    /// stay readable through the old backing until they have been
    /// taken, so nothing in flight is lost.
    pub fn morph_to(&self, capacity: u64) -> PsResult<()> {
        self.inner.morph_capacity_to(power_of_two(capacity)?).map_err(|e| op_err("resizing", e))
    }

    /// Builds a backing of `capacity` ahead of needing it, so the morph
    /// that switches to it does not pay for the allocation.
    pub fn prewarm(&self, capacity: u64) -> PsResult<()> {
        self.inner.prewarm(power_of_two(capacity)?).map_err(|e| op_err("prewarming", e))
    }

    /// The capacity held ready by Prewarm, or `$null` when nothing is
    /// cached.
    pub fn warm_capacity(&self) -> PsResult<Option<u64>> {
        Ok(self.inner.warm_capacity().map(|c| c as u64))
    }

    /// Morphs that took the prewarmed backing instead of building one.
    pub fn warm_hits(&self) -> PsResult<u64> {
        Ok(self.inner.warm_hits())
    }

    /// Items taken from a backing a morph has superseded rather than
    /// from the current one, which is what a morph costs a reader.
    pub fn stale_pops(&self) -> PsResult<u64> {
        Ok(self.inner.stale_pops())
    }

    /// Drops the prewarmed backing, giving back its memory and its
    /// file.
    pub fn clear_warm(&self) -> PsResult<()> {
        self.inner.clear_warm();
        Ok(())
    }

    /// The ordering discipline in force, or `$null` on an unstamped
    /// ring.
    pub fn ordering_mode(&self) -> PsResult<Option<OrderingMode>> {
        Ok(self.inner.ordering_mode().map(OrderingMode::from_rust))
    }

    /// Sets the ordering discipline across the current backing and
    /// every superseded one still draining, so a reader walking both
    /// applies one discipline.
    pub fn set_ordering_mode(&self, mode: OrderingMode) -> PsResult<()> {
        self.inner.set_ordering_mode(mode.rust()).map_err(|e| op_err("setting the ordering mode", e))
    }

    /// Cross-producer inversions seen so far, carried across morphs.
    pub fn inversions(&self) -> PsResult<u64> {
        Ok(self.inner.inversions())
    }
}

/// Obtains the resizable ring at Path holding Capacity slots, a power of
/// two, creating it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaCapacityRing", alias = "New-SECapacityRing", output = ["SubEtha.CapacityRing"])]
#[derive(Default)]
pub struct NewSubEthaCapacityRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds, a power of two and at least two.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many producers may register; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers may register; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
    /// Put ordering stamps on the items, which is what OrderingMode and
    /// Inversions read. Stamps cost space and a write per item.
    #[param]
    pub stamped: bool,
}

impl Cmdlet for NewSubEthaCapacityRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(CapacityRing::obtain(path, self.capacity, self.max_producers.unwrap_or(1), self.max_consumers.unwrap_or(1), self.stamped, false)?)
    }
}

/// Attaches to the resizable ring at Path, which must exist with the
/// capacity, counts and stamping it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaCapacityRing", alias = "Open-SECapacityRing", output = ["SubEtha.CapacityRing"])]
#[derive(Default)]
pub struct OpenSubEthaCapacityRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds, a power of two and at least two.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many producers may register; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers may register; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
    /// The ring was created with ordering stamps.
    #[param]
    pub stamped: bool,
}

impl Cmdlet for OpenSubEthaCapacityRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(CapacityRing::obtain(path, self.capacity, self.max_producers.unwrap_or(1), self.max_consumers.unwrap_or(1), self.stamped, true)?)
    }
}

/// A ring that can move the bytes it holds between three places without
/// the senders and readers having to reconnect.
///
/// The three locales are `Anon`, memory private to one process and the
/// cheapest to send through; `File`, a mapped file that survives the
/// process and reaches any other one that opens it; and `ShmFs`, named
/// memory that other processes can reach but which never goes to disk.
/// A ring starts at `Anon`; `MigrateTo` moves it, carrying whatever is
/// already in it.
#[psclass(name = "SubEtha.LocaleRing", mode = proxy)]
pub struct LocaleRing {
    /// The file the ring lives in.
    pub path: String,
    /// How many slots the ring holds.
    pub capacity: u64,
    /// How many producers may register.
    pub max_producers: u64,
    /// How many consumers may register.
    pub max_consumers: u64,
    /// Whether the items carry ordering stamps, fixed when the ring is
    /// built.
    pub stamped: bool,
    #[psfield(skip)]
    inner: LocaleAdaptiveRing,
}

impl LocaleRing {
    fn obtain(path: String, capacity: u64, producers: u64, consumers: u64, stamped: bool, open: bool) -> PsResult<Self> {
        let slots = power_of_two(capacity)?;
        if producers < 1 || consumers < 1 {
            return Err(arg_err("a ring needs at least one producer and one consumer"));
        }
        let p = size(producers, "the producer count")?;
        let c = size(consumers, "the consumer count")?;
        let inner = if open {
            LocaleAdaptiveRing::open(&path, p, c, slots, stamped)
        } else if stamped {
            LocaleAdaptiveRing::create_with_ordering_stamps(&path, p, c, slots)
        } else {
            LocaleAdaptiveRing::create(&path, p, c, slots)
        }
        .map_err(|e| open_err("the ring", &path, e))?;
        Ok(Self { path, capacity, max_producers: producers, max_consumers: consumers, stamped, inner })
    }

    fn try_send(&self, producer: usize, item: &[u8]) -> PsResult<bool> {
        match self.inner.try_send(producer, item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(op_err("sending", e)),
        }
    }

    fn try_recv(&self, consumer: usize, out: &mut [u8]) -> PsResult<Option<usize>> {
        match self.inner.try_recv(consumer, out) {
            Ok(n) => Ok(Some(n)),
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(op_err("receiving", e)),
        }
    }
}

/// The operations of a `SubEtha.LocaleRing`.
#[psmethods]
impl LocaleRing {
    /// Where the bytes are right now.
    pub fn locale(&self) -> PsResult<Locale> {
        Ok(Locale::from_rust(self.inner.current_locale()))
    }

    /// Steps on every migration, so a holder that captured the locale
    /// can tell that what it captured has been superseded.
    pub fn locale_generation(&self) -> PsResult<u64> {
        Ok(self.inner.locale_generation())
    }

    /// Takes a producer id on all three backings at once, so the
    /// registration is there whichever locale is live.
    pub fn register_producer(&self) -> PsResult<u64> {
        self.inner.register_producer().map(|p| p as u64).map_err(|e| op_err("registering a producer", e))
    }

    /// Takes a consumer id on all three backings at once.
    pub fn register_consumer(&self) -> PsResult<u64> {
        self.inner.register_consumer().map(|c| c as u64).map_err(|e| op_err("registering a consumer", e))
    }

    /// Sends one item as `producer`. False means the ring was full.
    pub fn send(&self, producer: u64, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_send(size(producer, "the producer")?, &item)
    }

    /// Sends a run of items as `producer`, stopping at the first
    /// refusal, and returns how many went in.
    pub fn send_many(&self, producer: u64, items: Vec<PsObject>) -> PsResult<u64> {
        let producer = size(producer, "the producer")?;
        push_each(&items, |item| self.try_send(producer, item))
    }

    /// The next item for `consumer`, or `$null` when the ring is
    /// empty.
    pub fn recv(&self, consumer: u64) -> PsResult<Option<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.try_recv(consumer, &mut out)? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Up to `maxItems` for `consumer` in one call.
    pub fn recv_many(&self, consumer: u64, max_items: u64) -> PsResult<Vec<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        pop_each(max_items, SPSC_PAYLOAD_BYTES, |out| self.try_recv(consumer, out))
    }

    /// Moves the ring to another locale, carrying what is already in
    /// it. On a stamped ring the transfer keeps the order every sender
    /// saw; on an unstamped one the drain can interleave senders, as a
    /// shape change can.
    pub fn migrate_to(&self, locale: Locale) -> PsResult<()> {
        self.inner.migrate_to(locale.rust()).map_err(|e| op_err("migrating", e))
    }

    /// The ordering discipline in force, or `$null` on an unstamped
    /// ring.
    pub fn ordering_mode(&self) -> PsResult<Option<OrderingMode>> {
        Ok(self.inner.ordering_mode().map(OrderingMode::from_rust))
    }

    /// Sets the ordering discipline on all three backings, so a
    /// migration does not change the discipline underneath a reader.
    pub fn set_ordering_mode(&self, mode: OrderingMode) -> PsResult<()> {
        self.inner.set_ordering_mode(mode.rust()).map_err(|e| op_err("setting the ordering mode", e))
    }

    /// Cross-producer inversions seen across all three backings.
    pub fn inversions(&self) -> PsResult<u64> {
        Ok(self.inner.inversions())
    }
}

/// Obtains the relocatable ring at Path holding Capacity slots, a power
/// of two, creating it when the file does not exist. All three backings
/// are built, so a later migration has somewhere to go without
/// allocating.
#[cmdlet(verb = "New", noun = "SubEthaLocaleRing", alias = "New-SELocaleRing", output = ["SubEtha.LocaleRing"])]
#[derive(Default)]
pub struct NewSubEthaLocaleRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds, a power of two and at least two.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many producers may register; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers may register; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
    /// Put ordering stamps on the items, which is what lets a
    /// migration preserve order across every sender.
    #[param]
    pub stamped: bool,
}

impl Cmdlet for NewSubEthaLocaleRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LocaleRing::obtain(path, self.capacity, self.max_producers.unwrap_or(1), self.max_consumers.unwrap_or(1), self.stamped, false)?)
    }
}

/// Attaches to the relocatable ring at Path, which must exist with the
/// capacity, counts and stamping it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaLocaleRing", alias = "Open-SELocaleRing", output = ["SubEtha.LocaleRing"])]
#[derive(Default)]
pub struct OpenSubEthaLocaleRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds, a power of two and at least two.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many producers may register; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers may register; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
    /// The ring was created with ordering stamps.
    #[param]
    pub stamped: bool,
}

impl Cmdlet for OpenSubEthaLocaleRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LocaleRing::obtain(path, self.capacity, self.max_producers.unwrap_or(1), self.max_consumers.unwrap_or(1), self.stamped, true)?)
    }
}

/// The adaptive ring: the family the C ABI is built around, which
/// changes its own shape as the traffic through it changes.
///
/// Producers and consumers register to get an id, and every send and
/// receive names the id it belongs to. The ring can morph between
/// shapes underneath without the caller doing anything. The ring sits
/// behind a shared handle rather than being owned outright, because an
/// ordered receiver and a network bridge each take a handle of their
/// own, and all of them then name the same ring rather than copies.
#[psclass(name = "SubEtha.Ring", mode = proxy)]
pub struct Ring {
    /// The file the ring lives in.
    pub path: String,
    /// How many producers may register.
    pub max_producers: u64,
    /// How many consumers may register.
    pub max_consumers: u64,
    /// The kind of mark the items carry, or `$null` on a ring whose
    /// items are unmarked.
    pub stamps: Option<StampKind>,
    #[psfield(skip)]
    inner: Arc<AdaptiveRing>,
}

impl Ring {
    fn obtain(path: String, capacity: u64, producers: u64, consumers: u64, stamps: Option<StampKind>, open: bool) -> PsResult<Self> {
        if producers < 1 || consumers < 1 {
            return Err(arg_err("a ring needs at least one producer and one consumer"));
        }
        let slots = size(capacity, "the capacity")?;
        let p = size(producers, "the producer count")?;
        let c = size(consumers, "the consumer count")?;
        let ring = if open { AdaptiveRing::open(&path, p, c, slots) } else { AdaptiveRing::create(&path, p, c, slots) }
            .map_err(|e| open_err("the ring", &path, e))?;
        let ring = match stamps {
            Some(kind) => ring.with_ordering_stamps_kind(kind.rust()).map_err(|e| op_err("putting stamps on the ring", e))?,
            None => ring,
        };
        let stamps = ring.stamp_kind().map(StampKind::from_rust);
        Ok(Self { path, max_producers: producers, max_consumers: consumers, stamps, inner: Arc::new(ring) })
    }

    /// The shared handle, for the bridges and readers that take one of
    /// their own.
    pub(crate) fn handle(&self) -> Arc<AdaptiveRing> {
        Arc::clone(&self.inner)
    }

    fn try_send(&self, producer: usize, item: &[u8]) -> PsResult<bool> {
        match self.inner.try_send(producer, item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(op_err("sending", e)),
        }
    }

    fn try_recv(&self, consumer: usize, out: &mut [u8]) -> PsResult<Option<usize>> {
        match self.inner.try_recv(consumer, out) {
            Ok(n) => Ok(Some(n)),
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(op_err("receiving", e)),
        }
    }
}

/// The operations of a `SubEtha.Ring`.
#[psmethods]
impl Ring {
    /// Whether this ring marks its items with the order their senders
    /// made them, which is fixed when the ring is built.
    pub fn stamped(&self) -> PsResult<bool> {
        Ok(self.inner.is_stamped())
    }

    /// Takes a producer id. Every send names one.
    pub fn register_producer(&self) -> PsResult<u64> {
        self.inner.register_producer().map(|p| p as u64).map_err(|e| op_err("registering a producer", e))
    }

    /// Takes a consumer id. Every receive names one.
    pub fn register_consumer(&self) -> PsResult<u64> {
        self.inner.register_consumer().map(|c| c as u64).map_err(|e| op_err("registering a consumer", e))
    }

    /// The shape the ring is in right now, by name. It can change under
    /// the caller; that is what adaptive means.
    pub fn shape(&self) -> PsResult<String> {
        Ok(format!("{:?}", self.inner.current_shape()))
    }

    /// The capacity of one sub-ring in the current shape.
    pub fn capacity(&self) -> PsResult<u64> {
        Ok(self.inner.sub_ring_capacity() as u64)
    }

    /// The slots across every sub-ring in the current shape.
    pub fn total_capacity(&self) -> PsResult<u64> {
        Ok(self.inner.total_slot_capacity() as u64)
    }

    /// How many items are in it, read without stopping anyone, so a
    /// sighting rather than a promise.
    pub fn approx_len(&self) -> PsResult<u64> {
        Ok(self.inner.approx_len() as u64)
    }

    /// Whether nothing is in it.
    pub fn is_empty(&self) -> PsResult<bool> {
        Ok(self.inner.is_empty())
    }

    /// Sends one item as `producer`. False means the ring was full.
    pub fn send(&self, producer: u64, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_send(size(producer, "the producer")?, &item)
    }

    /// Sends a run of items as `producer`, stopping at the first
    /// refusal, and returns how many went in.
    pub fn send_many(&self, producer: u64, items: Vec<PsObject>) -> PsResult<u64> {
        let producer = size(producer, "the producer")?;
        push_each(&items, |item| self.try_send(producer, item))
    }

    /// Sends items packed end to end in one `byte[]`, `itemLen` bytes
    /// each, with no object per item.
    pub fn send_packed(&self, producer: u64, data: PsObject, item_len: u64) -> PsResult<u64> {
        let producer = size(producer, "the producer")?;
        push_packed(&data, item_len, |item| self.try_send(producer, item))
    }

    /// The next item for `consumer`, or `$null` when the ring is
    /// empty.
    pub fn recv(&self, consumer: u64) -> PsResult<Option<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.try_recv(consumer, &mut out)? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Up to `maxItems` for `consumer` in one call.
    pub fn recv_many(&self, consumer: u64, max_items: u64) -> PsResult<Vec<PsObject>> {
        let consumer = size(consumer, "the consumer")?;
        pop_each(max_items, SPSC_PAYLOAD_BYTES, |out| self.try_recv(consumer, out))
    }

    /// Sends a payload longer than a slot as `producer`, carried in
    /// frames. False means the ring was full.
    pub fn send_frame(&self, producer: u64, payload: PsObject) -> PsResult<bool> {
        let payload = bytes(&payload)?;
        match self.inner.send_frame(size(producer, "the producer")?, &payload) {
            Ok(_) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(op_err("sending a frame", e)),
        }
    }

    /// The next framed payload for `consumer`, or `$null` when none is
    /// waiting.
    pub fn recv_frame(&self, consumer: u64) -> PsResult<Option<PsObject>> {
        let mut out = Vec::new();
        match self.inner.recv_frame(size(consumer, "the consumer")?, &mut out) {
            Ok(_) => Ok(Some(out_bytes(&out)?)),
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(op_err("receiving a frame", e)),
        }
    }

    /// How many times the ring refused to change shape.
    pub fn morph_refusals(&self) -> PsResult<u64> {
        Ok(self.inner.morph_refusals())
    }

    /// A reader for `consumer` that hands items back in the order their
    /// senders made them, rather than the order they happened to arrive
    /// in. The ring must have been built with stamps.
    ///
    /// It picks its own strategy from how the ring is built, and its
    /// `Strategy` says which one it picked. On a ring whose stamps come
    /// from a counter it buffers a small window and releases the
    /// smallest stamp in it; past a few hundred senders it asks the ring
    /// to wait on every sender instead. On a ring stamped from the
    /// clock the arrival order is already the right one and nothing is
    /// buffered.
    pub fn ordered_receiver(&self, consumer: u64) -> PsResult<OrderedReceiver> {
        if !self.inner.is_stamped() {
            return Err(arg_err("an ordered receiver needs a ring built with stamps, such as New-SubEthaRing -Stamps Counter"));
        }
        let ring = Arc::clone(&self.inner);
        // The ring lives on the heap behind the shared handle, so its
        // address does not change for as long as any handle lives, and
        // the receiver holds one; that is what makes the borrow it
        // takes good for the receiver's whole life.
        let borrowed: &'static AdaptiveRing = unsafe { &*Arc::as_ptr(&ring) };
        Ok(OrderedReceiver { inner: AdaptiveOrderedReceiver::new(borrowed, size(consumer, "the consumer")?), ring })
    }
}

/// Obtains the adaptive ring at Path holding Capacity slots, creating
/// it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaRing", alias = "New-SERing", output = ["SubEtha.Ring"])]
#[derive(Default)]
pub struct NewSubEthaRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many producers may register; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers may register; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
    /// Mark every item with the order its sender made it in, which is
    /// what an ordered receiver reads. Absent, the items are unmarked
    /// and it costs nothing.
    #[param]
    pub stamps: Option<StampKind>,
}

impl Cmdlet for NewSubEthaRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Ring::obtain(path, self.capacity, self.max_producers.unwrap_or(1), self.max_consumers.unwrap_or(1), self.stamps, false)?)
    }
}

/// Attaches to the adaptive ring at Path, which must exist with the
/// capacity, counts and stamps it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaRing", alias = "Open-SERing", output = ["SubEtha.Ring"])]
#[derive(Default)]
pub struct OpenSubEthaRing {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many producers may register; one when absent.
    #[param]
    pub max_producers: Option<u64>,
    /// How many consumers may register; one when absent.
    #[param]
    pub max_consumers: Option<u64>,
    /// The kind of stamps the ring was created with.
    #[param]
    pub stamps: Option<StampKind>,
}

impl Cmdlet for OpenSubEthaRing {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Ring::obtain(path, self.capacity, self.max_producers.unwrap_or(1), self.max_consumers.unwrap_or(1), self.stamps, true)?)
    }
}

/// A reader that delivers a ring's items in the order their senders
/// made them. Built by a `SubEtha.Ring`'s OrderedReceiver.
///
/// Under the buffering strategy an item is held back until the window
/// behind it has filled, so at the end of a stream there is a tail
/// still inside the receiver. Flush is what releases it, and a reader
/// that stops at the first `$null` from Recv loses that tail; Drain is
/// the shape that takes everything.
#[psclass(name = "SubEtha.OrderedReceiver", mode = proxy)]
pub struct OrderedReceiver {
    /// Declared before the handle below so it is dropped first, because
    /// it borrows from the ring that handle keeps alive.
    #[psfield(skip)]
    inner: AdaptiveOrderedReceiver<'static>,
    #[psfield(skip)]
    ring: Arc<AdaptiveRing>,
}

/// The operations of a `SubEtha.OrderedReceiver`.
#[psmethods]
impl OrderedReceiver {
    /// Everything the ring holds now plus everything the window still
    /// holds back, in order, in one call. `maxItems`, 4096 when absent,
    /// bounds how many times the ring is read, so a sender that keeps
    /// writing cannot hold the call open.
    pub fn drain(&mut self, max_items: Option<u64>) -> PsResult<Vec<StampedItem>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        for _ in 0..max_items.unwrap_or(4096) {
            if let Some((len, stamp)) = self.inner.try_recv(&mut out) {
                taken.push(StampedItem::new(stamp, &out[..len])?);
            }
        }
        while let Some((len, stamp)) = self.inner.flush(&mut out) {
            taken.push(StampedItem::new(stamp, &out[..len])?);
        }
        Ok(taken)
    }

    /// The next item and its stamp, or `$null` while the window is
    /// still filling or nothing is waiting. The two are not
    /// distinguishable from here, which is why a stream that ends must
    /// finish with FlushAll and why Drain is the easier shape.
    pub fn recv(&mut self) -> PsResult<Option<StampedItem>> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        match self.inner.try_recv(&mut out) {
            Some((len, stamp)) => Ok(Some(StampedItem::new(stamp, &out[..len])?)),
            None => Ok(None),
        }
    }

    /// The next item held back in the window, or `$null` once the
    /// window is empty. Called in a loop at the end of a stream.
    pub fn flush(&mut self) -> PsResult<Option<StampedItem>> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        match self.inner.flush(&mut out) {
            Some((len, stamp)) => Ok(Some(StampedItem::new(stamp, &out[..len])?)),
            None => Ok(None),
        }
    }

    /// Everything still held, in order, in one call.
    pub fn flush_all(&mut self) -> PsResult<Vec<StampedItem>> {
        let mut drained = Vec::new();
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        while let Some((len, stamp)) = self.inner.flush(&mut out) {
            drained.push(StampedItem::new(stamp, &out[..len])?);
        }
        Ok(drained)
    }

    /// Which strategy this receiver picked: reorder, strict or direct.
    pub fn strategy(&self) -> PsResult<String> {
        Ok(self.inner.strategy().to_string())
    }

    /// How many times the window had to grow because an item arrived
    /// further out of order than the window covered. Zero means the
    /// window was wide enough throughout.
    pub fn corrections(&self) -> PsResult<u64> {
        Ok(self.inner.corrections())
    }

    /// Whether the ring this reads from still has other handles open in
    /// this process.
    pub fn ring_shared(&self) -> PsResult<bool> {
        Ok(Arc::strong_count(&self.ring) > 1)
    }
}

/// The window a reader uses to put items back in order, on its own.
///
/// Items go in with the stamp their sender gave them, and come out
/// smallest stamp first once more than the window's worth are held.
/// The window grows by itself when an item turns up further out of
/// order than it covered, and Corrections counts how often that
/// happened: zero means the starting window was wide enough all along.
/// An ordered receiver uses one of these against a ring; this is the
/// same window over items from anywhere else.
#[psclass(name = "SubEtha.ReorderWindow", mode = proxy)]
pub struct ReorderWindow {
    /// The window it started at.
    pub floor: u64,
    /// The widest it may grow to.
    pub cap: u64,
    #[psfield(skip)]
    inner: ReorderBuffer,
}

/// The operations of a `SubEtha.ReorderWindow`.
#[psmethods]
impl ReorderWindow {
    /// Holds one item, with the stamp its sender gave it.
    pub fn push(&mut self, stamp: u64, payload: PsObject) -> PsResult<()> {
        let payload = bytes(&payload)?;
        self.inner.push(stamp, &payload);
        Ok(())
    }

    /// Holds a run of items in one call: `stamps` and `payloads` side
    /// by side.
    pub fn push_many(&mut self, stamps: Vec<u64>, payloads: Vec<PsObject>) -> PsResult<u64> {
        if stamps.len() != payloads.len() {
            return Err(arg_err("the stamps and the payloads must be the same length"));
        }
        for (stamp, payload) in stamps.iter().zip(&payloads) {
            let payload = bytes(payload)?;
            self.inner.push(*stamp, &payload);
        }
        Ok(stamps.len() as u64)
    }

    /// The next item and its stamp, or `$null` while fewer than a
    /// window's worth are held.
    pub fn take(&mut self) -> PsResult<Option<StampedItem>> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        match self.inner.try_take(&mut out) {
            Some((stamp, len)) => Ok(Some(StampedItem::new(stamp, &out[..len])?)),
            None => Ok(None),
        }
    }

    /// The next item regardless of how full the window is, or `$null`
    /// once nothing is held. This is how a stream ends.
    pub fn flush(&mut self) -> PsResult<Option<StampedItem>> {
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        match self.inner.flush_one(&mut out) {
            Some((stamp, len)) => Ok(Some(StampedItem::new(stamp, &out[..len])?)),
            None => Ok(None),
        }
    }

    /// Everything still held, in order, in one call.
    pub fn flush_all(&mut self) -> PsResult<Vec<StampedItem>> {
        let mut drained = Vec::new();
        let mut out = vec![0u8; STAMPED_PAYLOAD_BYTES];
        while let Some((stamp, len)) = self.inner.flush_one(&mut out) {
            drained.push(StampedItem::new(stamp, &out[..len])?);
        }
        Ok(drained)
    }

    /// Widens the window to at least `window`, which is what a caller
    /// does when the number of senders grows.
    pub fn widen_to(&mut self, window: u64) -> PsResult<()> {
        self.inner.widen_to(size(window, "the window")?);
        Ok(())
    }

    /// The window right now, which growth changes.
    pub fn window(&self) -> PsResult<u64> {
        Ok(self.inner.window() as u64)
    }

    /// Times an item arrived further out of order than the window
    /// covered, each of which widened it.
    pub fn corrections(&self) -> PsResult<u64> {
        Ok(self.inner.corrections())
    }

    /// How many items are held.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }
}

/// Builds a reorder window. Floor is the window to start at and Cap the
/// widest it may grow to; a window at least as wide as the number of
/// senders puts every item back in order.
#[cmdlet(verb = "New", noun = "SubEthaReorderWindow", alias = "New-SEReorderWindow", output = ["SubEtha.ReorderWindow"])]
#[derive(Default)]
pub struct NewSubEthaReorderWindow {
    /// The window to start at.
    #[param]
    pub floor: Option<u64>,
    /// The widest the window may grow to.
    #[param]
    pub cap: Option<u64>,
}

impl Cmdlet for NewSubEthaReorderWindow {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let floor = match self.floor {
            Some(f) => size(f, "the floor")?,
            None => REORDER_DEFAULT_FLOOR,
        };
        let cap = match self.cap {
            Some(c) => size(c, "the cap")?,
            None => REORDER_DEFAULT_CAP,
        };
        ps.write(ReorderWindow { floor: floor as u64, cap: cap as u64, inner: ReorderBuffer::with_window(floor, cap) })
    }
}

/// A last-in first-out stack in a mapped file, shared between
/// processes.
#[psclass(name = "SubEtha.Stack", mode = proxy)]
pub struct Stack {
    /// The file the stack lives in.
    pub path: String,
    /// How many items it can hold.
    pub capacity: u64,
    /// The bytes one item holds.
    pub element_size: u64,
    #[psfield(skip)]
    inner: RawTreiberStack,
}

impl Stack {
    fn obtain(path: String, capacity: u64, element_size: u64, alignment: Option<u64>, tag: Option<u64>, open: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("the capacity must be at least one"));
        }
        let layout = layout(element_size, alignment, tag)?;
        let slots = size(capacity, "the capacity")?;
        let inner = if open { RawTreiberStack::open(&path, slots, layout) } else { RawTreiberStack::create(&path, slots, layout) }
            .map_err(|e| open_err("the stack", &path, e))?;
        Ok(Self { path, capacity, element_size, inner })
    }

    fn try_push(&self, item: &[u8]) -> PsResult<bool> {
        match self.inner.push(item) {
            Ok(()) => Ok(true),
            Err(StackError::Full) => Ok(false),
            Err(e) => Err(op_err("pushing", e)),
        }
    }
}

/// The operations of a `SubEtha.Stack`.
#[psmethods]
impl Stack {
    /// How many items are on it, read without stopping anyone, so a
    /// sighting rather than a promise.
    pub fn approx_len(&self) -> PsResult<u64> {
        Ok(self.inner.approx_len() as u64)
    }

    /// Whether nothing is on it.
    pub fn is_empty(&self) -> PsResult<bool> {
        Ok(self.inner.is_empty())
    }

    /// Pushes one item. False means the stack is full.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_push(&item)
    }

    /// Pushes a run of items, stopping at the first refusal, and
    /// returns how many went on.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        push_each(&items, |item| self.try_push(item))
    }

    /// Takes the top item, or `$null` when the stack is empty.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.slot_size()];
        match self.inner.pop(&mut out) {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Takes up to `maxItems` from the top.
    pub fn pop_many(&self, max_items: u64) -> PsResult<Vec<PsObject>> {
        pop_each(max_items, self.inner.slot_size(), |out| Ok(self.inner.pop(out)))
    }

    /// The top item without taking it, or `$null`. The bytes are a
    /// snapshot: a concurrent pop can retire the slot while this reads
    /// it.
    pub fn peek(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.slot_size()];
        match self.inner.peek(&mut out) {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the stack at Path holding up to Capacity items of ElementSize
/// bytes, creating it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaStack", alias = "New-SEStack", output = ["SubEtha.Stack"])]
#[derive(Default)]
pub struct NewSubEthaStack {
    /// The file the stack lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items it can hold.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one item holds.
    #[param(mandatory, position = 2)]
    pub element_size: u64,
    /// The alignment of each item, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for NewSubEthaStack {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Stack::obtain(path, self.capacity, self.element_size, self.alignment, self.tag, false)?)
    }
}

/// Attaches to the stack at Path, which must exist with the capacity and
/// layout it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaStack", alias = "Open-SEStack", output = ["SubEtha.Stack"])]
#[derive(Default)]
pub struct OpenSubEthaStack {
    /// The file the stack lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items it can hold.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one item holds.
    #[param(mandatory, position = 2)]
    pub element_size: u64,
    /// The alignment of each item, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for OpenSubEthaStack {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Stack::obtain(path, self.capacity, self.element_size, self.alignment, self.tag, true)?)
    }
}

/// A work-stealing deque in a mapped file: its owner pushes and pops
/// one end, and other processes steal from the other.
#[psclass(name = "SubEtha.Deque", mode = proxy)]
pub struct Deque {
    /// The file the deque lives in.
    pub path: String,
    /// How many items it can hold.
    pub capacity: u64,
    /// The bytes one item holds.
    pub element_size: u64,
    /// Whether this handle is a thief, which steals and neither pushes
    /// nor pops.
    pub thief: bool,
    #[psfield(skip)]
    inner: RawDeque,
}

impl Deque {
    fn try_push(&self, item: &[u8]) -> PsResult<bool> {
        match self.inner.push(item) {
            Ok(()) => Ok(true),
            Err(DequeError::Full) => Ok(false),
            Err(e) => Err(op_err("pushing", e)),
        }
    }
}

/// The operations of a `SubEtha.Deque`.
#[psmethods]
impl Deque {
    /// How many items are in it, read without stopping anyone.
    pub fn approx_len(&self) -> PsResult<u64> {
        Ok(self.inner.approx_len() as u64)
    }

    /// Pushes at the owner's end. False means it is full.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_push(&item)
    }

    /// Pushes a run of items at the owner's end, stopping at the first
    /// refusal, and returns how many went in.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        push_each(&items, |item| self.try_push(item))
    }

    /// Takes from the owner's end, or `$null`.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.pop(&mut out) {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Takes from the other end, which is what another process does.
    /// `$null` means there was nothing to take, or that another thief
    /// won the race for it.
    pub fn steal(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.steal(&mut out) {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Steals up to `maxItems`.
    pub fn steal_many(&self, max_items: u64) -> PsResult<Vec<PsObject>> {
        pop_each(max_items, self.inner.layout().slot_size, |out| Ok(self.inner.steal(out)))
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the deque at Path as its owner, holding Capacity items, a
/// power of two, of ElementSize bytes, creating it when the file does
/// not exist.
#[cmdlet(verb = "New", noun = "SubEthaDeque", alias = "New-SEDeque", output = ["SubEtha.Deque"])]
#[derive(Default)]
pub struct NewSubEthaDeque {
    /// The file the deque lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items it can hold, a power of two.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one item holds.
    #[param(mandatory, position = 2)]
    pub element_size: u64,
    /// The alignment of each item, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for NewSubEthaDeque {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        if self.capacity == 0 || !self.capacity.is_power_of_two() {
            return Err(arg_err("the capacity must be a power of two"));
        }
        let layout = layout(self.element_size, self.alignment, self.tag)?;
        let inner = RawDeque::create(&path, size(self.capacity, "the capacity")?, layout).map_err(|e| open_err("the deque", &path, e))?;
        ps.write(Deque { path, capacity: self.capacity, element_size: self.element_size, thief: false, inner })
    }
}

/// Attaches to the deque at Path as a thief: the handle steals from the
/// deque another process owns, and neither pushes nor pops. No capacity
/// is asked for, because the file states it.
#[cmdlet(verb = "Open", noun = "SubEthaDeque", alias = "Open-SEDeque", output = ["SubEtha.Deque"])]
#[derive(Default)]
pub struct OpenSubEthaDeque {
    /// The file the deque lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes one item holds.
    #[param(mandatory, position = 1)]
    pub element_size: u64,
    /// The alignment of each item, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for OpenSubEthaDeque {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let layout = layout(self.element_size, self.alignment, self.tag)?;
        let inner = RawDeque::open_as_thief(&path, layout).map_err(|e| open_err("the deque", &path, e))?;
        let capacity = inner.capacity() as u64;
        ps.write(Deque { path, capacity, element_size: self.element_size, thief: true, inner })
    }
}

/// The error a subscriber raises when it fell so far behind that what
/// it asked for had already been overwritten: raised rather than
/// answered with `$null`, because losing items is not the same as
/// having none yet.
fn lagged(message: String) -> PsError {
    PsError::new(ErrorCategory::LimitsExceeded, "SubEthaLagged", message)
}

/// A publish/subscribe ring: one publisher, any number of subscribers,
/// none of which hold the publisher up.
///
/// The ring keeps the last N items and wraps, so a subscriber that falls
/// behind loses the items it missed. That loss is reported as an error
/// named SubEthaLagged rather than as an empty read, because the two
/// mean opposite things.
#[psclass(name = "SubEtha.PubSub", mode = proxy)]
pub struct PubSub {
    /// The file the ring lives in.
    pub path: String,
    /// How many items the ring keeps.
    pub capacity: u64,
    /// The bytes one item holds; a longer item is refused.
    pub payload_size: u64,
    #[psfield(skip)]
    inner: Arc<PubSubRing>,
}

impl PubSub {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        let slots = size(capacity, "the capacity")?;
        let inner = if open { PubSubRing::open(&path, slots) } else { PubSubRing::create(&path, slots) }
            .map_err(|e| open_err("the pubsub ring", &path, e))?;
        Ok(Self { path, capacity, payload_size: PUBSUB_PAYLOAD_BYTES as u64, inner: Arc::new(inner) })
    }

    fn fits(item: &[u8]) -> PsResult<()> {
        if item.len() > PUBSUB_PAYLOAD_BYTES {
            return Err(arg_err(format!("an item of {} bytes does not fit a {PUBSUB_PAYLOAD_BYTES}-byte slot", item.len())));
        }
        Ok(())
    }
}

/// The operations of a `SubEtha.PubSub`.
#[psmethods]
impl PubSub {
    /// The position the publisher has reached. A subscriber at this
    /// position has seen everything.
    pub fn head(&self) -> PsResult<u64> {
        Ok(self.inner.head())
    }

    /// Publishes one item and returns the position it landed at. The
    /// publisher never waits for a subscriber.
    pub fn publish(&self, item: PsObject) -> PsResult<u64> {
        let item = bytes(&item)?;
        Self::fits(&item)?;
        Ok(self.inner.publish(&item))
    }

    /// Publishes a run of items in one call and returns the position of
    /// the last, or `$null` when there were none.
    pub fn publish_many(&self, items: Vec<PsObject>) -> PsResult<Option<u64>> {
        let mut last = None;
        for item in &items {
            let item = bytes(item)?;
            Self::fits(&item)?;
            last = Some(self.inner.publish(&item));
        }
        Ok(last)
    }

    /// The item at `position`: `$null` when nothing has been published
    /// there yet, and an error named SubEthaLagged when it has already
    /// been overwritten.
    pub fn read_at(&self, position: u64) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; PUBSUB_PAYLOAD_BYTES];
        match self.inner.read_at(position, &mut out) {
            Ok(()) => Ok(Some(out_bytes(&out)?)),
            Err(PubSubReadError::Pending) => Ok(None),
            Err(PubSubReadError::Lost) => Err(lagged(format!("position {position} has been overwritten; the publisher is at {}", self.inner.head()))),
        }
    }

    /// A subscriber starting where the publisher is now, so it sees
    /// what follows and nothing that came before.
    pub fn subscribe(&self) -> PsResult<Subscriber> {
        Ok(Subscriber { ring: Arc::clone(&self.inner), position: self.inner.head() })
    }

    /// A subscriber starting at `position`, for one replaying from a
    /// place it recorded earlier.
    pub fn subscribe_from(&self, position: u64) -> PsResult<Subscriber> {
        Ok(Subscriber { ring: Arc::clone(&self.inner), position })
    }
}

/// Obtains the publish/subscribe ring at Path keeping Capacity items,
/// creating it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaPubSub", alias = "New-SEPubSub", output = ["SubEtha.PubSub"])]
#[derive(Default)]
pub struct NewSubEthaPubSub {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items the ring keeps.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaPubSub {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(PubSub::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the publish/subscribe ring at Path, which must exist
/// with the Capacity it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaPubSub", alias = "Open-SEPubSub", output = ["SubEtha.PubSub"])]
#[derive(Default)]
pub struct OpenSubEthaPubSub {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items the ring keeps.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaPubSub {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(PubSub::obtain(path, self.capacity, true)?)
    }
}

/// One subscriber's place in a `SubEtha.PubSub` ring, which it advances
/// as it reads.
#[psclass(name = "SubEtha.Subscriber", mode = proxy)]
pub struct Subscriber {
    #[psfield(skip)]
    ring: Arc<PubSubRing>,
    #[psfield(skip)]
    position: u64,
}

impl Subscriber {
    /// The message for a subscriber that fell behind, after skipping it
    /// to the publisher's position.
    fn fell_behind(&mut self) -> PsError {
        let head = self.ring.head();
        let missed = head.saturating_sub(self.position);
        self.position = head;
        lagged(format!("fell behind by {missed} items; skipped to {head}"))
    }
}

/// The operations of a `SubEtha.Subscriber`.
#[psmethods]
impl Subscriber {
    /// Where this subscriber has read up to.
    pub fn position(&self) -> PsResult<u64> {
        Ok(self.position)
    }

    /// How far behind the publisher this subscriber is.
    pub fn lag(&self) -> PsResult<u64> {
        Ok(self.ring.head().saturating_sub(self.position))
    }

    /// The next item, or `$null` when this subscriber has caught up.
    /// An error named SubEthaLagged means the next item was overwritten
    /// before it was read, and the subscriber has skipped to the
    /// publisher's position.
    pub fn next(&mut self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; PUBSUB_PAYLOAD_BYTES];
        match self.ring.read_at(self.position, &mut out) {
            Ok(()) => {
                self.position += 1;
                Ok(Some(out_bytes(&out)?))
            }
            Err(PubSubReadError::Pending) => Ok(None),
            Err(PubSubReadError::Lost) => Err(self.fell_behind()),
        }
    }

    /// Up to `maxItems` in one call, stopping when caught up.
    pub fn next_many(&mut self, max_items: u64) -> PsResult<Vec<PsObject>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; PUBSUB_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match self.ring.read_at(self.position, &mut out) {
                Ok(()) => {
                    self.position += 1;
                    taken.push(out_bytes(&out)?);
                }
                Err(PubSubReadError::Pending) => break,
                Err(PubSubReadError::Lost) => return Err(self.fell_behind()),
            }
        }
        Ok(taken)
    }
}

/// The producer end of a Lamport pair: one writer, one reader, sharing
/// a ring in a mapped file. The pair is handed out together, because
/// the whole point is that exactly one of each exists.
#[psclass(name = "SubEtha.LamportProducer", mode = proxy)]
pub struct LamportProducer {
    /// The file the ring lives in.
    pub path: String,
    /// How many slots the ring holds.
    pub capacity: u64,
    /// The bytes one slot carries; a longer item is refused.
    pub payload_size: u64,
    #[psfield(skip)]
    inner: Producer,
}

impl LamportProducer {
    fn try_push(&self, item: &[u8]) -> PsResult<bool> {
        match self.inner.try_push(item) {
            Ok(()) => Ok(true),
            Err(RingError::Full) => Ok(false),
            Err(e) => Err(op_err("pushing", e)),
        }
    }
}

/// The operations of a `SubEtha.LamportProducer`.
#[psmethods]
impl LamportProducer {
    /// Pushes one item. False means the ring was full.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.try_push(&item)
    }

    /// Pushes a run of items, stopping at the first refusal, and
    /// returns how many went in.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        push_each(&items, |item| self.try_push(item))
    }

    /// Pushes items packed end to end in one `byte[]`, `itemLen` bytes
    /// each, and returns how many went in.
    pub fn push_packed(&self, data: PsObject, item_len: u64) -> PsResult<u64> {
        push_packed(&data, item_len, |item| self.try_push(item))
    }
}

/// The consumer end of a Lamport pair.
#[psclass(name = "SubEtha.LamportConsumer", mode = proxy)]
pub struct LamportConsumer {
    /// The file the ring lives in.
    pub path: String,
    /// How many slots the ring holds.
    pub capacity: u64,
    #[psfield(skip)]
    inner: Consumer,
}

impl LamportConsumer {
    fn try_pop(&self, out: &mut [u8]) -> PsResult<Option<usize>> {
        match self.inner.try_pop(out) {
            Ok(n) => Ok(Some(n)),
            Err(RingError::Empty) => Ok(None),
            Err(e) => Err(op_err("popping", e)),
        }
    }
}

/// The operations of a `SubEtha.LamportConsumer`.
#[psmethods]
impl LamportConsumer {
    /// Pops one item, or `$null` when the ring is empty.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match self.try_pop(&mut out)? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Pops up to `maxItems`, stopping when the ring runs empty.
    pub fn pop_many(&self, max_items: u64) -> PsResult<Vec<PsObject>> {
        pop_each(max_items, SPSC_PAYLOAD_BYTES, |out| self.try_pop(out))
    }

    /// Pops up to `maxItems` into one `byte[]` packed end to end, a
    /// slot's width each.
    pub fn pop_packed(&self, max_items: u64) -> PsResult<PackedItems> {
        pop_packed(max_items, SPSC_PAYLOAD_BYTES, |out| self.try_pop(out))
    }
}

/// Makes a Lamport pair at Path, one producer and one consumer over one
/// ring of Capacity slots, and writes the two in that order. Exactly
/// one of each may exist per ring: within a process the types enforce
/// that, and across processes it is the caller's undertaking, which is
/// why the pair is handed out rather than opened twice.
#[cmdlet(verb = "New", noun = "SubEthaLamportPair", alias = "New-SELamportPair", output = ["SubEtha.LamportProducer", "SubEtha.LamportConsumer"])]
#[derive(Default)]
pub struct NewSubEthaLamportPair {
    /// The file the ring lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the ring holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// Attach to a ring that already exists instead of creating one.
    #[param]
    pub open: bool,
}

impl Cmdlet for NewSubEthaLamportPair {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let slots = size(self.capacity, "the capacity")?;
        let (p, c) = if self.open { SharedRingSpsc::open_pair(&path, slots) } else { SharedRingSpsc::create_pair(&path, slots) }
            .map_err(|e| open_err("the Lamport pair", &path, e))?;
        ps.write(LamportProducer { path: path.clone(), capacity: self.capacity, payload_size: SPSC_PAYLOAD_BYTES as u64, inner: p })?;
        ps.write(LamportConsumer { path, capacity: self.capacity, inner: c })
    }
}

/// A pool of fixed-size blocks in a mapped file, for payloads too large
/// to sit in a ring slot.
///
/// A producer takes a block, writes the payload into it, and puts the
/// block's index in the ring; the consumer reads the block and frees it.
/// The allocator is safe for many producers taking and many consumers
/// freeing at once.
#[psclass(name = "SubEtha.FrameRegion", mode = proxy)]
pub struct FrameRegion {
    /// The file the pool lives in.
    pub path: String,
    /// The bytes one block holds.
    pub block_size: u64,
    /// How many blocks the pool holds.
    pub block_count: u64,
    #[psfield(skip)]
    inner: SubethaFrameRegion,
}

impl FrameRegion {
    fn obtain(path: String, block_size: u64, block_count: u64, open: bool) -> PsResult<Self> {
        let bs = size(block_size, "the block size")?;
        let bc = size(block_count, "the block count")?;
        let inner = if open { SubethaFrameRegion::open(&path, bs, bc) } else { SubethaFrameRegion::create(&path, bs, bc) }
            .map_err(|e| open_err("the frame region", &path, e))?;
        Ok(Self { path, block_size, block_count, inner })
    }

    fn fits(&self, payload: &[u8]) -> PsResult<()> {
        if payload.len() > self.inner.block_size() {
            return Err(arg_err("the payload is longer than a block"));
        }
        Ok(())
    }

    fn read(&self, index: u32, length: u64) -> PsResult<Vec<u8>> {
        let length = size(length, "the length")?;
        if length > self.inner.block_size() {
            return Err(arg_err("the length asked for is longer than a block"));
        }
        let mut out = vec![0u8; length];
        let read = self.inner.read_block(index, length, &mut out);
        out.truncate(read);
        Ok(out)
    }
}

/// The operations of a `SubEtha.FrameRegion`.
#[psmethods]
impl FrameRegion {
    /// Takes a block, or `$null` when every block is in use.
    pub fn allocate(&self) -> PsResult<Option<u32>> {
        Ok(self.inner.alloc())
    }

    /// Gives a block back. Freeing one nobody holds corrupts the pool,
    /// so only free what this process allocated and has finished with.
    pub fn free(&self, index: u32) -> PsResult<()> {
        self.inner.free(index);
        Ok(())
    }

    /// Takes a block and writes `payload` into it in one call, or
    /// `$null` when the pool is exhausted: the shape a producer wants,
    /// one call rather than an allocate and a write.
    pub fn write_new(&self, payload: PsObject) -> PsResult<Option<u32>> {
        let payload = bytes(&payload)?;
        self.fits(&payload)?;
        match self.inner.alloc() {
            Some(index) => {
                self.inner.write_block(index, &payload);
                Ok(Some(index))
            }
            None => Ok(None),
        }
    }

    /// Writes `payload` into block `index`.
    pub fn write_block(&self, index: u32, payload: PsObject) -> PsResult<()> {
        let payload = bytes(&payload)?;
        self.fits(&payload)?;
        self.inner.write_block(index, &payload);
        Ok(())
    }

    /// `length` bytes from block `index`.
    pub fn read_block(&self, index: u32, length: u64) -> PsResult<PsObject> {
        out_bytes(&self.read(index, length)?)
    }

    /// Reads `length` bytes from block `index` and gives the block back
    /// in one call, which is the shape a consumer wants.
    pub fn take_block(&self, index: u32, length: u64) -> PsResult<PsObject> {
        let out = self.read(index, length)?;
        self.inner.free(index);
        out_bytes(&out)
    }
}

/// Obtains the frame region at Path holding BlockCount blocks of
/// BlockSize bytes, creating it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaFrameRegion", alias = "New-SEFrameRegion", output = ["SubEtha.FrameRegion"])]
#[derive(Default)]
pub struct NewSubEthaFrameRegion {
    /// The file the pool lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes one block holds.
    #[param(mandatory, position = 1)]
    pub block_size: u64,
    /// How many blocks the pool holds.
    #[param(mandatory, position = 2)]
    pub block_count: u64,
}

impl Cmdlet for NewSubEthaFrameRegion {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(FrameRegion::obtain(path, self.block_size, self.block_count, false)?)
    }
}

/// Attaches to the frame region at Path, which must exist with the
/// block size and count it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaFrameRegion", alias = "Open-SEFrameRegion", output = ["SubEtha.FrameRegion"])]
#[derive(Default)]
pub struct OpenSubEthaFrameRegion {
    /// The file the pool lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes one block holds.
    #[param(mandatory, position = 1)]
    pub block_size: u64,
    /// How many blocks the pool holds.
    #[param(mandatory, position = 2)]
    pub block_count: u64,
}

impl Cmdlet for OpenSubEthaFrameRegion {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(FrameRegion::obtain(path, self.block_size, self.block_count, true)?)
    }
}
