//! The front door: the shapes reached without naming a structure. A
//! channel, an adaptive queue, a work queue and a map are picked from
//! what the caller says it expects, and the policy records what a
//! stream needs so the shape can be chosen to match.

use std::time::Duration;

use pwrs::prelude::*;

use subetha_cxc::adaptive_ipc::AdaptiveIpc as SubethaAdaptiveIpc;
use subetha_cxc::api::{ApiError, AutoIpc, Channel as SubethaChannel, KvMap as SubethaKvMap, WorkStealQueue as SubethaWorkStealQueue};
use subetha_cxc::dispatch_deque::DequeVariant;
use subetha_cxc::message_transport::TransportError;
use subetha_cxc::mmf_dispatcher::{MmfFamily, MmfWorkloadShape};
use subetha_cxc::qos_policy::{Durability as SubethaDurability, History, Ordering as SubethaOrderingNeed, QosPolicy as SubethaQosPolicy, Reliability as SubethaReliability};
use subetha_cxc::shared_hash_map::InsertOutcome;

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, seconds, size, SlotValue, SLOT_VALUE_BYTES};
use crate::rings::Locale;

assert_send!(Channel, AdaptiveQueue, WorkQueue, KvMap, QosPolicy, QosSnapshot);

/// How long a stream's bytes must outlive the process that wrote them.
#[psenum(name = "SubEtha.Durability")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Durability {
    /// In-process memory; gone with the process.
    #[default]
    Volatile,
    /// Named memory other processes can reach, gone with the host.
    Transient,
    /// A mapped file that survives the host.
    Persistent,
}

impl Durability {
    fn rust(self) -> SubethaDurability {
        match self {
            Durability::Volatile => SubethaDurability::Volatile,
            Durability::Transient => SubethaDurability::Transient,
            Durability::Persistent => SubethaDurability::Persistent,
        }
    }

    fn from_rust(d: SubethaDurability) -> Self {
        match d {
            SubethaDurability::Volatile => Durability::Volatile,
            SubethaDurability::Transient => Durability::Transient,
            SubethaDurability::Persistent => Durability::Persistent,
        }
    }
}

/// Whether a full ring may drop or must make the sender wait.
#[psenum(name = "SubEtha.Reliability")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Reliability {
    /// A full ring drops.
    #[default]
    BestEffort,
    /// A full ring makes the sender wait.
    Reliable,
}

impl Reliability {
    fn rust(self) -> SubethaReliability {
        match self {
            Reliability::BestEffort => SubethaReliability::BestEffort,
            Reliability::Reliable => SubethaReliability::Reliable,
        }
    }

    fn from_rust(r: SubethaReliability) -> Self {
        match r {
            SubethaReliability::BestEffort => Reliability::BestEffort,
            SubethaReliability::Reliable => Reliability::Reliable,
        }
    }
}

/// Whose order a reader needs.
#[psenum(name = "SubEtha.OrderingNeed")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OrderingNeed {
    /// Each sender's own order.
    #[default]
    PerProducer,
    /// The order across every sender.
    GlobalFifo,
}

impl OrderingNeed {
    fn rust(self) -> SubethaOrderingNeed {
        match self {
            OrderingNeed::PerProducer => SubethaOrderingNeed::PerProducer,
            OrderingNeed::GlobalFifo => SubethaOrderingNeed::GlobalFifo,
        }
    }

    fn from_rust(o: SubethaOrderingNeed) -> Self {
        match o {
            SubethaOrderingNeed::PerProducer => OrderingNeed::PerProducer,
            SubethaOrderingNeed::GlobalFifo => OrderingNeed::GlobalFifo,
        }
    }
}

/// What an adaptive queue is underneath right now.
#[psenum(name = "SubEtha.QueueShape")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QueueShape {
    /// A ring.
    #[default]
    Ring,
    /// A work-stealing deque.
    WorkStealing,
    /// A hash map.
    Map,
}

impl QueueShape {
    fn from_rust(family: MmfFamily) -> Self {
        match family {
            MmfFamily::SharedRing => QueueShape::Ring,
            MmfFamily::SharedDeque(_) => QueueShape::WorkStealing,
            MmfFamily::SharedHashMap => QueueShape::Map,
        }
    }

    fn rust(self) -> PsResult<MmfFamily> {
        match self {
            QueueShape::Ring => Ok(MmfFamily::SharedRing),
            QueueShape::WorkStealing => Ok(MmfFamily::SharedDeque(DequeVariant::ChaseLev)),
            QueueShape::Map => Err(arg_err("a queue cannot be moved to a map; the shapes are Ring and WorkStealing")),
        }
    }
}

/// Whether this failure is the channel being full, which is an answer
/// rather than a fault.
fn is_full(e: &ApiError) -> bool {
    matches!(e, ApiError::Transport(TransportError::Full))
}

/// Whether this failure is the channel being empty.
fn is_empty(e: &ApiError) -> bool {
    matches!(e, ApiError::Transport(TransportError::Empty))
}

/// Whether this failure is the wait running out rather than anything
/// going wrong.
fn is_timeout(e: &ApiError) -> bool {
    matches!(e, ApiError::Timeout)
}

fn api_err(doing: &str, e: ApiError) -> PsError {
    match e {
        ApiError::PayloadTooLarge => arg_err(format!("{doing}: the item does not fit a slot")),
        other => op_err(doing, other),
    }
}

/// Seconds as a duration, or nothing to wait as long as it takes.
fn optional_duration(timeout: Option<f64>) -> PsResult<Option<Duration>> {
    match timeout {
        None => Ok(None),
        Some(t) => Ok(Some(seconds(t)?)),
    }
}

/// The front door: a queue between processes that can be waited on.
///
/// This is the shape to reach for when what is wanted is simply to send
/// things to another process and read them back. The rings underneath
/// give more control over the shape; this one picks the shape from the
/// number of senders and readers named, and adds the one thing they do
/// not have, which is waiting. Recv answers `$null` the moment there is
/// nothing there; RecvFor waits.
#[psclass(name = "SubEtha.Channel", mode = proxy)]
pub struct Channel {
    /// The file the channel lives in.
    pub path: String,
    /// The most an item may be, in bytes.
    pub max_item_size: u64,
    #[psfield(skip)]
    inner: SubethaChannel<SlotValue>,
}

/// The operations of a `SubEtha.Channel`.
#[psmethods]
impl Channel {
    /// Sends one item. False means the channel is full.
    pub fn send(&self, item: PsObject) -> PsResult<bool> {
        let held = SlotValue::from_bytes(&bytes(&item)?)?;
        match self.inner.send(&held) {
            Ok(()) => Ok(true),
            Err(e) if is_full(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// Sends a run of items in one call and returns how many went. A
    /// short answer means the channel filled.
    pub fn send_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        let mut sent = 0;
        for item in &items {
            let held = SlotValue::from_bytes(&bytes(item)?)?;
            match self.inner.send(&held) {
                Ok(()) => sent += 1,
                Err(e) if is_full(&e) => break,
                Err(e) => return Err(api_err("sending", e)),
            }
        }
        Ok(sent)
    }

    /// Waits until the item can be sent, or until `timeout` seconds
    /// have passed; absent waits as long as it takes. False on a
    /// timeout.
    pub fn send_for(&self, item: PsObject, timeout: Option<f64>) -> PsResult<bool> {
        let held = SlotValue::from_bytes(&bytes(&item)?)?;
        match self.inner.send_blocking(&held, optional_duration(timeout)?) {
            Ok(()) => Ok(true),
            Err(e) if is_timeout(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// The next item, or `$null` when there is nothing there.
    pub fn recv(&self) -> PsResult<Option<PsObject>> {
        match self.inner.recv() {
            Ok(held) => Ok(Some(held.to_ps()?)),
            Err(e) if is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }

    /// Everything waiting, up to `maxItems` (256 when absent), in one
    /// call.
    pub fn recv_many(&self, max_items: Option<u64>) -> PsResult<Vec<PsObject>> {
        let mut taken = Vec::new();
        for _ in 0..max_items.unwrap_or(256) {
            match self.inner.recv() {
                Ok(held) => taken.push(held.to_ps()?),
                Err(e) if is_empty(&e) => break,
                Err(e) => return Err(api_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Waits for the next item, or until `timeout` seconds have passed;
    /// absent waits as long as it takes. `$null` on a timeout.
    pub fn recv_for(&self, timeout: Option<f64>) -> PsResult<Option<PsObject>> {
        match self.inner.recv_blocking(optional_duration(timeout)?) {
            Ok(held) => Ok(Some(held.to_ps()?)),
            Err(e) if is_timeout(&e) || is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }
}

/// Obtains the channel at Path with Capacity items in flight (1024 when
/// absent, rounded up to a power of two), creating it for Senders and
/// Readers when the file does not exist.
///
/// # Examples
///
/// `$channel = New-SubEthaChannel -Path C:\ipc\channel -Capacity 64`
#[cmdlet(verb = "New", noun = "SubEthaChannel", alias = "New-SEChannel", output = ["SubEtha.Channel"])]
#[derive(Default)]
pub struct NewSubEthaChannel {
    /// The file the channel lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items may be in flight; 1024 when absent.
    #[param]
    pub capacity: Option<u64>,
    /// How many senders are expected at once; one when absent.
    #[param]
    pub senders: Option<u64>,
    /// How many readers are expected at once; one when absent.
    #[param]
    pub readers: Option<u64>,
}

impl Cmdlet for NewSubEthaChannel {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let senders = size(self.senders.unwrap_or(1), "the sender count")?;
        let readers = size(self.readers.unwrap_or(1), "the reader count")?;
        if senders < 1 || readers < 1 {
            return Err(arg_err("a channel needs at least one sender and one reader"));
        }
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: senders, n_consumers: readers };
        let inner = SubethaChannel::create(&path, shape, size(self.capacity.unwrap_or(1024), "the capacity")?).map_err(|e| api_err("opening the channel", e))?;
        ps.write(Channel { path, max_item_size: SLOT_VALUE_BYTES as u64, inner })
    }
}

/// Attaches to the channel at Path, which must exist with the Capacity
/// it was created with.
///
/// # Examples
///
/// `$channel = Open-SubEthaChannel -Path C:\ipc\channel -Capacity 64`
#[cmdlet(verb = "Open", noun = "SubEthaChannel", alias = "Open-SEChannel", output = ["SubEtha.Channel"])]
#[derive(Default)]
pub struct OpenSubEthaChannel {
    /// The file the channel lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items may be in flight; 1024 when absent.
    #[param]
    pub capacity: Option<u64>,
}

impl Cmdlet for OpenSubEthaChannel {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let inner = SubethaChannel::open(&path, size(self.capacity.unwrap_or(1024), "the capacity")?).map_err(|e| api_err("attaching to the channel", e))?;
        ps.write(Channel { path, max_item_size: SLOT_VALUE_BYTES as u64, inner })
    }
}

/// What an adaptive queue weighs when deciding to change shape.
#[psclass(name = "SubEtha.Traffic")]
#[derive(Clone, Default)]
pub struct Traffic {
    /// The average number of items a send carried.
    pub average_batch: u64,
    /// The share of sends that were batches.
    pub batch_share: f64,
}

/// A queue that changes what it is underneath as the traffic changes.
///
/// The other front-door classes pick a shape once, from what the caller
/// says it expects. This one picks from what actually happens: it
/// watches the sizes it is sent and moves between a ring and a
/// work-stealing deque while running, without either end reconnecting.
/// Worth it when the traffic is not known in advance or changes over a
/// run; when it is known, a channel or a work queue is cheaper, because
/// this one pays a counter on every send.
#[psclass(name = "SubEtha.AdaptiveQueue", mode = proxy)]
pub struct AdaptiveQueue {
    /// The file the queue lives in.
    pub path: String,
    /// The most an item may be, in bytes.
    pub max_item_size: u64,
    #[psfield(skip)]
    inner: SubethaAdaptiveIpc<SlotValue>,
}

/// The operations of a `SubEtha.AdaptiveQueue`.
#[psmethods]
impl AdaptiveQueue {
    /// Sends one item. False means the queue is full.
    pub fn send(&self, item: PsObject) -> PsResult<bool> {
        let held = SlotValue::from_bytes(&bytes(&item)?)?;
        match self.inner.send(&held) {
            Ok(()) => Ok(true),
            Err(e) if is_full(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// Sends a run of items as one batch, which is also what tells the
    /// queue the traffic comes in batches and may be worth a different
    /// shape. Returns how many went: all of them, or none when the
    /// queue was full.
    pub fn send_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        let mut held = Vec::with_capacity(items.len());
        for item in &items {
            held.push(SlotValue::from_bytes(&bytes(item)?)?);
        }
        match self.inner.send_batch(&held) {
            Ok(()) => Ok(held.len() as u64),
            Err(e) if is_full(&e) => Ok(0),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// The next item, or `$null` when there is nothing there.
    pub fn recv(&self) -> PsResult<Option<PsObject>> {
        match self.inner.recv() {
            Ok(held) => Ok(Some(held.to_ps()?)),
            Err(e) if is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }

    /// Everything waiting, up to `maxItems` (256 when absent), in one
    /// call.
    pub fn recv_many(&self, max_items: Option<u64>) -> PsResult<Vec<PsObject>> {
        let mut taken = Vec::new();
        for _ in 0..max_items.unwrap_or(256) {
            match self.inner.recv() {
                Ok(held) => taken.push(held.to_ps()?),
                Err(e) if is_empty(&e) => break,
                Err(e) => return Err(api_err("receiving", e)),
            }
        }
        Ok(taken)
    }

    /// Waits until the item can be sent, or until `timeout` seconds
    /// have passed; absent waits as long as it takes. False on a
    /// timeout.
    pub fn send_for(&self, item: PsObject, timeout: Option<f64>) -> PsResult<bool> {
        let held = SlotValue::from_bytes(&bytes(&item)?)?;
        match self.inner.send_blocking(&held, optional_duration(timeout)?) {
            Ok(()) => Ok(true),
            Err(e) if is_timeout(&e) => Ok(false),
            Err(e) => Err(api_err("sending", e)),
        }
    }

    /// Waits for the next item, or until `timeout` seconds have passed;
    /// absent waits as long as it takes. `$null` on a timeout.
    pub fn recv_for(&self, timeout: Option<f64>) -> PsResult<Option<PsObject>> {
        match self.inner.recv_blocking(optional_duration(timeout)?) {
            Ok(held) => Ok(Some(held.to_ps()?)),
            Err(e) if is_timeout(&e) || is_empty(&e) => Ok(None),
            Err(e) => Err(api_err("receiving", e)),
        }
    }

    /// What the queue is right now.
    pub fn shape(&self) -> PsResult<QueueShape> {
        Ok(QueueShape::from_rust(self.inner.active_family()))
    }

    /// Looks at the traffic so far and moves to the shape that fits it,
    /// answering the new shape or `$null` when the one it has already
    /// fits.
    pub fn maybe_change_shape(&self) -> PsResult<Option<QueueShape>> {
        self.inner.maybe_promote().map(|moved| moved.map(QueueShape::from_rust)).map_err(|e| api_err("changing shape", e))
    }

    /// Moves to a named shape, whatever the traffic says.
    pub fn change_shape_to(&self, shape: QueueShape) -> PsResult<()> {
        self.inner.migrate_to(shape.rust()?).map_err(|e| api_err("changing shape", e))
    }

    /// Steps every time the shape changes, so a holder can tell that
    /// what it looked at has been superseded.
    pub fn shape_generation(&self) -> PsResult<u64> {
        Ok(self.inner.pin_generation())
    }

    /// What the queue weighs when deciding to change shape.
    pub fn traffic(&self) -> PsResult<Traffic> {
        let seen = self.inner.profile_snapshot();
        Ok(Traffic { average_batch: seen.avg_batch_size(), batch_share: seen.batch_ratio() })
    }

    /// Whose order a reader gets.
    pub fn ordering(&self) -> PsResult<OrderingNeed> {
        Ok(OrderingNeed::from_rust(self.inner.ordering()))
    }

    /// Sets whose order a reader gets.
    pub fn set_ordering(&self, ordering: OrderingNeed) -> PsResult<()> {
        self.inner.set_ordering(ordering.rust()).map_err(|e| api_err("setting the ordering", e))
    }

    /// Cross-sender inversions seen so far, which is what says whether
    /// the ordering asked for is being met.
    pub fn inversions(&self) -> PsResult<u64> {
        Ok(self.inner.inversions())
    }
}

/// Creates the adaptive queue at Path with Capacity items in flight
/// (1024 when absent), starting as a ring for Senders and Readers and
/// changing shape as the traffic says.
///
/// # Examples
///
/// `$queue = New-SubEthaAdaptiveQueue -Path C:\ipc\adaptivequeue -Capacity 64`
#[cmdlet(verb = "New", noun = "SubEthaAdaptiveQueue", alias = "New-SEAdaptiveQueue", output = ["SubEtha.AdaptiveQueue"])]
#[derive(Default)]
pub struct NewSubEthaAdaptiveQueue {
    /// The file the queue lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items may be in flight; 1024 when absent.
    #[param]
    pub capacity: Option<u64>,
    /// How many senders it starts with; one when absent.
    #[param]
    pub senders: Option<u64>,
    /// How many readers it starts with; one when absent.
    #[param]
    pub readers: Option<u64>,
    /// Whose order a reader needs, a setting the caller makes and never
    /// inferred from the traffic; each sender's own when absent.
    #[param]
    pub ordering: Option<OrderingNeed>,
    /// Cross-sender inversions per second past which the queue may
    /// turn global ordering on by itself; absent means it never does.
    #[param]
    pub auto_order: Option<f64>,
}

impl Cmdlet for NewSubEthaAdaptiveQueue {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let senders = size(self.senders.unwrap_or(1), "the sender count")?;
        let readers = size(self.readers.unwrap_or(1), "the reader count")?;
        if senders < 1 || readers < 1 {
            return Err(arg_err("a queue needs at least one sender and one reader"));
        }
        if let Some(rate) = self.auto_order {
            if !rate.is_finite() || rate < 0.0 {
                return Err(arg_err("AutoOrder is a number of inversions a second that is not negative"));
            }
        }
        let shape = MmfWorkloadShape::StreamingMpmc { n_producers: senders, n_consumers: readers };
        let inner = SubethaAdaptiveIpc::create_with_ordering(
            &path,
            shape,
            size(self.capacity.unwrap_or(1024), "the capacity")?,
            readers,
            self.ordering.unwrap_or_default().rust(),
            self.auto_order,
        )
        .map_err(|e| api_err("opening the queue", e))?;
        ps.write(AdaptiveQueue { path, max_item_size: SLOT_VALUE_BYTES as u64, inner })
    }
}

/// Work one process owns and others take from when they are idle.
///
/// The owner pushes and pops at one end, which is the cheap end and the
/// one it gets to itself. Everybody else steals from the other end. The
/// two ends are what make it worth using over a ring: the owner's own
/// work costs nothing to hand out, and a thief only pays when it
/// actually takes something. The owner creates the queue; a thief
/// opens it.
#[psclass(name = "SubEtha.WorkQueue", mode = proxy)]
pub struct WorkQueue {
    /// The file the queue lives in.
    pub path: String,
    /// The most an item may be, in bytes.
    pub max_item_size: u64,
    /// Whether this handle is a thief, which steals and neither pushes
    /// nor pops.
    pub thief: bool,
    #[psfield(skip)]
    inner: SubethaWorkStealQueue<SlotValue>,
}

/// The operations of a `SubEtha.WorkQueue`.
#[psmethods]
impl WorkQueue {
    /// Adds work at the owner's end. False means the queue is full.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let held = SlotValue::from_bytes(&bytes(&item)?)?;
        match self.inner.push(&held) {
            Ok(()) => Ok(true),
            Err(e) if is_full(&e) => Ok(false),
            Err(e) => Err(api_err("pushing", e)),
        }
    }

    /// Adds a run of work in one call and returns how many went in.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        let mut pushed = 0;
        for item in &items {
            let held = SlotValue::from_bytes(&bytes(item)?)?;
            match self.inner.push(&held) {
                Ok(()) => pushed += 1,
                Err(e) if is_full(&e) => break,
                Err(e) => return Err(api_err("pushing", e)),
            }
        }
        Ok(pushed)
    }

    /// The owner's own next piece of work, the most recent one it
    /// added, or `$null` when there is none.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        match self.inner.pop() {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// A piece of work from the far end, which is what a thief does, or
    /// `$null` when there is none to take.
    pub fn steal(&self) -> PsResult<Option<PsObject>> {
        match self.inner.steal() {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// Up to `maxItems` (64 when absent) by stealing, in one call.
    pub fn steal_many(&self, max_items: Option<u64>) -> PsResult<Vec<PsObject>> {
        let mut taken = Vec::new();
        for _ in 0..max_items.unwrap_or(64) {
            match self.inner.steal() {
                Some(held) => taken.push(held.to_ps()?),
                None => break,
            }
        }
        Ok(taken)
    }
}

/// Creates the work queue at Path as its owner, with Capacity items in
/// flight (1024 when absent) and Thieves expected to take from it.
///
/// # Examples
///
/// `$owner = New-SubEthaWorkQueue -Path C:\ipc\workqueue -Capacity 64 -Thieves 2`
#[cmdlet(verb = "New", noun = "SubEthaWorkQueue", alias = "New-SEWorkQueue", output = ["SubEtha.WorkQueue"])]
#[derive(Default)]
pub struct NewSubEthaWorkQueue {
    /// The file the queue lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items may be in flight; 1024 when absent.
    #[param]
    pub capacity: Option<u64>,
    /// How many thieves are expected; one when absent.
    #[param]
    pub thieves: Option<u64>,
}

impl Cmdlet for NewSubEthaWorkQueue {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let thieves = size(self.thieves.unwrap_or(1), "the thief count")?;
        if thieves < 1 {
            return Err(arg_err("a queue needs at least one thief"));
        }
        // A batch hint is what turns the inference toward work-stealing
        // rather than streaming; without it the dispatcher picks a ring
        // and the build is refused.
        let inner = AutoIpc::new(&path)
            .consumers(thieves)
            .batch_size(2)
            .capacity(size(self.capacity.unwrap_or(1024), "the capacity")?)
            .build_work_steal_queue::<SlotValue>()
            .map_err(|e| api_err("opening the queue", e))?;
        ps.write(WorkQueue { path, max_item_size: SLOT_VALUE_BYTES as u64, thief: false, inner })
    }
}

/// Attaches to the work queue at Path as a thief, to take from it.
///
/// # Examples
///
/// `$thief = Open-SubEthaWorkQueue -Path C:\ipc\workqueue`
#[cmdlet(verb = "Open", noun = "SubEthaWorkQueue", alias = "Open-SEWorkQueue", output = ["SubEtha.WorkQueue"])]
#[derive(Default)]
pub struct OpenSubEthaWorkQueue {
    /// The file the queue lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for OpenSubEthaWorkQueue {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let inner = SubethaWorkStealQueue::open_as_thief(&path).map_err(|e| api_err("attaching to the queue", e))?;
        ps.write(WorkQueue { path, max_item_size: SLOT_VALUE_BYTES as u64, thief: true, inner })
    }
}

/// A map between processes, reached without naming a shape. The
/// companion to the channel in the front door: keys and values are
/// both unsigned integers, and the shape underneath is picked from how
/// many readers and writers are expected. There is no way to take a key
/// out; a key can only be written over.
#[psclass(name = "SubEtha.KvMap", mode = proxy)]
pub struct KvMap {
    /// The file the map lives in.
    pub path: String,
    #[psfield(skip)]
    inner: SubethaKvMap<u64, u64>,
}

/// The operations of a `SubEtha.KvMap`.
#[psmethods]
impl KvMap {
    /// Puts an entry in. True when the key was not there before and
    /// false when this replaced what it held.
    pub fn insert(&self, key: u64, value: u64) -> PsResult<bool> {
        self.inner.insert(key, value).map(|outcome| matches!(outcome, InsertOutcome::Inserted)).map_err(|e| api_err("inserting", e))
    }

    /// Puts each of `values` in under the key beside it in `keys`, and
    /// answers true for each key that was not there before.
    pub fn insert_many(&self, keys: Vec<u64>, values: Vec<u64>) -> PsResult<Vec<bool>> {
        if keys.len() != values.len() {
            return Err(arg_err("the keys and the values must be the same length"));
        }
        let mut fresh = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(&values) {
            fresh.push(self.inner.insert(*key, *value).map(|outcome| matches!(outcome, InsertOutcome::Inserted)).map_err(|e| api_err("inserting", e))?);
        }
        Ok(fresh)
    }

    /// What `key` holds, or `$null` when it holds nothing.
    pub fn get(&self, key: u64) -> PsResult<Option<u64>> {
        Ok(self.inner.get(&key))
    }

    /// What each of `keys` holds, `$null` where a key holds nothing.
    pub fn get_many(&self, keys: Vec<u64>) -> PsResult<Vec<PsObject>> {
        keys.into_iter().map(|key| self.inner.get(&key).into_ps()).collect()
    }

    /// Whether `key` holds anything.
    pub fn contains(&self, key: u64) -> PsResult<bool> {
        Ok(self.inner.get(&key).is_some())
    }

    /// How many keys the map holds.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }
}

/// Creates the map at Path with Capacity entries (1024 when absent),
/// shaped for Readers and Writers.
///
/// # Examples
///
/// `$map = New-SubEthaKvMap -Path C:\ipc\kvmap -Capacity 64`
#[cmdlet(verb = "New", noun = "SubEthaKvMap", alias = "New-SEKvMap", output = ["SubEtha.KvMap"])]
#[derive(Default)]
pub struct NewSubEthaKvMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries it holds; 1024 when absent.
    #[param]
    pub capacity: Option<u64>,
    /// How many readers are expected; one when absent.
    #[param]
    pub readers: Option<u64>,
    /// How many writers are expected; one when absent.
    #[param]
    pub writers: Option<u64>,
}

impl Cmdlet for NewSubEthaKvMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let readers = size(self.readers.unwrap_or(1), "the reader count")?;
        let writers = size(self.writers.unwrap_or(1), "the writer count")?;
        if readers < 1 || writers < 1 {
            return Err(arg_err("a map needs at least one reader and one writer"));
        }
        let inner = AutoIpc::new(&path)
            .consumers(readers)
            .producers(writers)
            .capacity(size(self.capacity.unwrap_or(1024), "the capacity")?)
            .build_kv_map::<u64, u64>()
            .map_err(|e| api_err("opening the map", e))?;
        ps.write(KvMap { path, inner })
    }
}

/// Every QoS setting as it stood at one moment, and what follows from
/// them.
#[psclass(name = "SubEtha.QosSnapshot", mode = proxy)]
#[derive(Clone, Default)]
pub struct QosSnapshot {
    /// How long the bytes must outlive the process that wrote them.
    pub durability: Durability,
    /// Whether a full ring may drop or must make the sender wait.
    pub reliability: Reliability,
    /// How many items to hold, or `$null` to hold everything there is
    /// room for.
    pub keep_last: Option<u32>,
    /// How long an item may take, in seconds.
    pub max_latency: f64,
    /// Whose order a reader needs.
    pub ordering: OrderingNeed,
}

impl QosSnapshot {
    fn rust(&self) -> subetha_cxc::qos_policy::QosSnapshot {
        subetha_cxc::qos_policy::QosSnapshot {
            durability: self.durability.rust(),
            reliability: self.reliability.rust(),
            history: history_from(self.keep_last),
            max_latency: Duration::from_secs_f64(self.max_latency),
            ordering: self.ordering.rust(),
        }
    }
}

/// The operations of a `SubEtha.QosSnapshot`.
#[psmethods]
impl QosSnapshot {
    /// Where the bytes should live given how long they must last, or
    /// `$null` when `current` is where they already are. The answer is
    /// what a relocatable ring's MigrateTo takes.
    pub fn recommends_locale_change(&self, current: Locale) -> PsResult<Option<Locale>> {
        Ok(self.rust().recommends_locale_change(current.rust()).map(Locale::from_rust))
    }

    /// The ordering this asks for, when it is not the one already in
    /// force, and `$null` when it is.
    pub fn recommends_ordering_change(&self, current: OrderingNeed) -> PsResult<Option<OrderingNeed>> {
        Ok(self.rust().recommends_ordering_change(current.rust()).map(OrderingNeed::from_rust))
    }
}

fn history_from(keep_last: Option<u32>) -> History {
    match keep_last {
        Some(n) => History::KeepLastN(n),
        None => History::KeepAll,
    }
}

fn keep_last_of(history: History) -> Option<u32> {
    match history {
        History::KeepLastN(n) => Some(n),
        History::KeepAll => None,
    }
}

/// What a stream needs, written down, so the shape underneath it can be
/// chosen to match rather than guessed at.
///
/// Four settings. Durability is how long the bytes must outlive the
/// process that wrote them, and the place they should live follows from
/// it. Reliability is whether a full ring may drop or must make the
/// sender wait. KeepLast is how many items to hold, or `$null` to hold
/// everything there is room for. MaxLatency is how long an item may
/// take, in seconds. Ordering is different from the other four and is
/// set on its own: whether a reader needs one sender's order or every
/// sender's order is something only the application knows, so nothing
/// here ever changes it from watching the traffic. This one is not
/// shared between processes; it is the settings a process holds while
/// it decides what to build.
#[psclass(name = "SubEtha.QosPolicy", mode = proxy)]
pub struct QosPolicy {
    #[psfield(skip)]
    inner: SubethaQosPolicy,
}

/// The operations of a `SubEtha.QosPolicy`.
#[psmethods]
impl QosPolicy {
    /// How long the bytes must outlive the process that wrote them.
    pub fn durability(&self) -> PsResult<Durability> {
        Ok(Durability::from_rust(self.inner.durability()))
    }

    /// Sets the durability.
    pub fn set_durability(&self, durability: Durability) -> PsResult<()> {
        self.inner.set_durability(durability.rust());
        Ok(())
    }

    /// Whether a full ring may drop or must make the sender wait.
    pub fn reliability(&self) -> PsResult<Reliability> {
        Ok(Reliability::from_rust(self.inner.reliability()))
    }

    /// Sets the reliability.
    pub fn set_reliability(&self, reliability: Reliability) -> PsResult<()> {
        self.inner.set_reliability(reliability.rust());
        Ok(())
    }

    /// How many items to hold, or `$null` to hold everything there is
    /// room for.
    pub fn keep_last(&self) -> PsResult<Option<u32>> {
        Ok(keep_last_of(self.inner.history()))
    }

    /// Sets how many items to hold; absent holds everything there is
    /// room for.
    pub fn set_keep_last(&self, keep_last: Option<u32>) -> PsResult<()> {
        self.inner.set_history(history_from(keep_last));
        Ok(())
    }

    /// How long an item may take, in seconds.
    pub fn max_latency(&self) -> PsResult<f64> {
        Ok(self.inner.max_latency().as_secs_f64())
    }

    /// Sets how long an item may take, in seconds.
    pub fn set_max_latency(&self, seconds: f64) -> PsResult<()> {
        self.inner.set_max_latency(crate::common::seconds(seconds)?);
        Ok(())
    }

    /// Whose order a reader needs.
    pub fn ordering(&self) -> PsResult<OrderingNeed> {
        Ok(OrderingNeed::from_rust(self.inner.ordering()))
    }

    /// Sets whose order a reader needs.
    pub fn set_ordering(&self, ordering: OrderingNeed) -> PsResult<()> {
        self.inner.set_ordering(ordering.rust());
        Ok(())
    }

    /// Every setting read together, so a decision is made against one
    /// consistent set rather than five separate reads.
    pub fn snapshot(&self) -> PsResult<QosSnapshot> {
        let s = self.inner.snapshot();
        Ok(QosSnapshot {
            durability: Durability::from_rust(s.durability),
            reliability: Reliability::from_rust(s.reliability),
            keep_last: keep_last_of(s.history),
            max_latency: s.max_latency.as_secs_f64(),
            ordering: OrderingNeed::from_rust(s.ordering),
        })
    }
}

/// A named starting point for a policy.
#[psenum(name = "SubEtha.QosPreset")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QosPreset {
    /// A stream that would rather lose an item than hold its sender
    /// up: in-process memory, dropping when full, the last thousand or
    /// so items, a tenth of a second.
    #[default]
    Streaming,
    /// A stream nothing may fall out of: named memory other processes
    /// can reach, senders waiting rather than dropping.
    ReliablePubSub,
    /// A stream that must survive the process: a mapped file, senders
    /// waiting, everything kept.
    PersistentLog,
}

/// Builds a policy from its settings, or from a Preset with any of the
/// settings overriding it.
///
/// # Examples
///
/// `$policy = New-SubEthaQosPolicy -Preset Streaming`
#[cmdlet(verb = "New", noun = "SubEthaQosPolicy", alias = "New-SEQosPolicy", output = ["SubEtha.QosPolicy"])]
#[derive(Default)]
pub struct NewSubEthaQosPolicy {
    /// A named starting point.
    #[param]
    pub preset: Option<QosPreset>,
    /// How long the bytes must outlive the process that wrote them;
    /// Volatile when absent.
    #[param]
    pub durability: Option<Durability>,
    /// Whether a full ring may drop or must make the sender wait;
    /// BestEffort when absent.
    #[param]
    pub reliability: Option<Reliability>,
    /// How many items to hold; 1024 when absent, and KeepAll holds
    /// everything there is room for.
    #[param]
    pub keep_last: Option<u32>,
    /// Hold everything there is room for.
    #[param]
    pub keep_all: bool,
    /// How long an item may take, in seconds; a tenth when absent.
    #[param]
    pub max_latency: Option<f64>,
    /// Whose order a reader needs; each sender's own when absent.
    #[param]
    pub ordering: Option<OrderingNeed>,
}

impl Cmdlet for NewSubEthaQosPolicy {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        if self.keep_all && self.keep_last.is_some() {
            return Err(arg_err("KeepAll and KeepLast cannot both be given"));
        }
        let inner = match self.preset {
            Some(QosPreset::Streaming) => SubethaQosPolicy::streaming_default(),
            Some(QosPreset::ReliablePubSub) => SubethaQosPolicy::reliable_pubsub_default(),
            Some(QosPreset::PersistentLog) => SubethaQosPolicy::persistent_log_default(),
            None => SubethaQosPolicy::new(
                self.durability.unwrap_or_default().rust(),
                self.reliability.unwrap_or_default().rust(),
                if self.keep_all { History::KeepAll } else { History::KeepLastN(self.keep_last.unwrap_or(1024)) },
                crate::common::seconds(self.max_latency.unwrap_or(0.1))?,
            ),
        };
        if self.preset.is_some() {
            if let Some(d) = self.durability {
                inner.set_durability(d.rust());
            }
            if let Some(r) = self.reliability {
                inner.set_reliability(r.rust());
            }
            if self.keep_all {
                inner.set_history(History::KeepAll);
            } else if let Some(n) = self.keep_last {
                inner.set_history(History::KeepLastN(n));
            }
            if let Some(l) = self.max_latency {
                inner.set_max_latency(crate::common::seconds(l)?);
            }
        }
        if let Some(o) = self.ordering {
            inner.set_ordering(o.rust());
        }
        ps.write(QosPolicy { inner })
    }
}
