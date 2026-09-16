//! The structures processes agree through: notifiers, leader election,
//! holder tables, heartbeats, the epoch barrier, the condition variable,
//! the fence clock and epoch reclamation.

use std::sync::Arc;

use pwrs::prelude::*;

use subetha_cxc::cross_process_notifier::{Notifier as SubethaNotifier, NotifierSet as SubethaNotifierSet};
use subetha_cxc::epoch_barrier::{BarrierError, EpochBarrier as SubethaEpochBarrier};
use subetha_cxc::heartbeat::HeartbeatTable;
use subetha_cxc::shared_condvar::{CondvarError, SharedCondvar};
use subetha_cxc::shared_epochs::SharedEpochs;
use subetha_cxc::shared_fence_clock::{Hlc, SharedFenceClock};
use subetha_cxc::shared_holder_table::SharedHolderTable;
use subetha_cxc::shared_leader_election::SharedLeaderElection;

use crate::common::{arg_err, assert_send, full_path, op_err, open_err, seconds, size, ClockReading};

assert_send!(NotifierSet, Notifier, LeaderElection, HolderTable, Heartbeat, EpochBarrier, Condvar, FenceClock, Epochs);

/// A set of notifiers on one file: any process can signal it, and every
/// attached notifier wakes.
#[psclass(name = "SubEtha.NotifierSet", mode = proxy)]
pub struct NotifierSet {
    /// The file the set lives in.
    pub path: String,
    #[psfield(skip)]
    inner: SubethaNotifierSet,
}

/// The operations of a `SubEtha.NotifierSet`.
#[psmethods]
impl NotifierSet {
    /// How many notifiers are attached.
    pub fn attached(&self) -> PsResult<u32> {
        Ok(self.inner.attached())
    }

    /// Wakes every attached notifier and returns how many were
    /// signaled.
    pub fn signal(&self) -> PsResult<u64> {
        Ok(self.inner.signal() as u64)
    }

    /// Attaches a notifier of this process's own.
    pub fn attach(&self) -> PsResult<Notifier> {
        let inner = self.inner.attach().map_err(|e| op_err("attaching a notifier", e))?;
        Ok(Notifier { index: inner.index(), native: inner.native(), inner })
    }
}

/// Obtains the notifier set at Path, creating it when the file does not
/// exist.
///
/// # Examples
///
/// `$set = New-SubEthaNotifierSet -Path C:\ipc\notifierset`
#[cmdlet(verb = "New", noun = "SubEthaNotifierSet", alias = "New-SENotifierSet", output = ["SubEtha.NotifierSet"])]
#[derive(Default)]
pub struct NewSubEthaNotifierSet {
    /// The file the set lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for NewSubEthaNotifierSet {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let inner = SubethaNotifierSet::file(&path).map_err(|e| open_err("the notifier set", &path, e))?;
        ps.write(NotifierSet { path, inner })
    }
}

/// One process's end of a notifier set: it waits, and a signal from any
/// process wakes it.
#[psclass(name = "SubEtha.Notifier", mode = proxy)]
pub struct Notifier {
    /// This notifier's place in the set.
    pub index: u32,
    /// The native object as an integer: a file descriptor on Unix, an
    /// event handle on Windows.
    pub native: u64,
    #[psfield(skip)]
    inner: SubethaNotifier,
}

/// The operations of a `SubEtha.Notifier`.
#[psmethods]
impl Notifier {
    /// Whether a signal is pending right now.
    pub fn is_signaled(&self) -> PsResult<bool> {
        Ok(self.inner.is_signaled())
    }

    /// Waits for a signal, up to `timeout` seconds or for ever when
    /// absent, and returns whether one arrived.
    pub fn wait(&self, timeout: Option<f64>) -> PsResult<bool> {
        let timeout_ms = match timeout {
            None => -1,
            Some(t) => {
                let ms = (seconds(t)?.as_secs_f64() * 1000.0).round();
                if ms > i32::MAX as f64 { i32::MAX } else { ms as i32 }
            }
        };
        Ok(self.inner.wait(timeout_ms))
    }

    /// Clears a pending signal, so the next wait blocks rather than
    /// returning at once on a signal already consumed.
    pub fn drain(&self) -> PsResult<()> {
        self.inner.drain();
        Ok(())
    }
}

/// Leader election in a mapped file: exactly one process holds the
/// role, and it is taken back if the holder stops beating.
#[psclass(name = "SubEtha.LeaderElection", mode = proxy)]
pub struct LeaderElection {
    /// The file the election lives in.
    pub path: String,
    #[psfield(skip)]
    inner: SharedLeaderElection,
}

impl LeaderElection {
    fn obtain(path: String, open: bool) -> PsResult<Self> {
        let inner = if open { SharedLeaderElection::open(&path) } else { SharedLeaderElection::create(&path) }
            .map_err(|e| open_err("the election", &path, e))?;
        Ok(Self { path, inner })
    }
}

/// The operations of a `SubEtha.LeaderElection`. Every `pid` argument
/// names the process acting, this one when absent.
#[psmethods]
impl LeaderElection {
    /// Tries to take the role, treating a holder quiet for more than
    /// `graceEpochs` epochs (three when absent) as gone. True means
    /// this process now holds it and must keep beating; false means
    /// someone else holds it and is still alive.
    pub fn try_claim(&self, grace_epochs: Option<u64>, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.try_claim_leadership(crate::common::pid(pid), grace_epochs.unwrap_or(3)))
    }

    /// Says the leader is still alive. False means this process is not
    /// the leader any more, which is the answer a former leader needs.
    pub fn beat(&self, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.beat_as_leader(crate::common::pid(pid)))
    }

    /// Gives the role up so another process can take it without
    /// waiting out the grace period.
    pub fn step_down(&self, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.step_down(crate::common::pid(pid)))
    }

    /// The process holding the role, or `$null` when nobody does.
    pub fn leader(&self) -> PsResult<Option<u32>> {
        Ok(self.inner.current_leader())
    }

    /// Whether the process holds the role.
    pub fn is_leader(&self, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.am_i_leader(crate::common::pid(pid)))
    }

    /// Steps each time the role changes hands, so a follower can tell
    /// a new leader from the same one.
    pub fn term(&self) -> PsResult<u32> {
        Ok(self.inner.election_term())
    }

    /// The epoch the holders measure the grace period in.
    pub fn global_epoch(&self) -> PsResult<u64> {
        Ok(self.inner.global_epoch())
    }

    /// Steps the epoch and returns the epoch it reached. Nothing steps
    /// it on its own.
    pub fn tick_epoch(&self) -> PsResult<u64> {
        Ok(self.inner.tick_epoch())
    }
}

/// Obtains the election at Path, creating it with no leader when the
/// file does not exist.
///
/// # Examples
///
/// `$election = New-SubEthaLeaderElection -Path C:\ipc\leaderelection`
#[cmdlet(verb = "New", noun = "SubEthaLeaderElection", alias = "New-SELeaderElection", output = ["SubEtha.LeaderElection"])]
#[derive(Default)]
pub struct NewSubEthaLeaderElection {
    /// The file the election lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for NewSubEthaLeaderElection {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LeaderElection::obtain(path, false)?)
    }
}

/// Attaches to the election at Path, which must exist.
///
/// # Examples
///
/// `$election = Open-SubEthaLeaderElection -Path C:\ipc\leaderelection`
#[cmdlet(verb = "Open", noun = "SubEthaLeaderElection", alias = "Open-SELeaderElection", output = ["SubEtha.LeaderElection"])]
#[derive(Default)]
pub struct OpenSubEthaLeaderElection {
    /// The file the election lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for OpenSubEthaLeaderElection {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LeaderElection::obtain(path, true)?)
    }
}

/// A table of slots each holding a 64-bit payload, taken and given back
/// by any process that maps it.
#[psclass(name = "SubEtha.HolderTable", mode = proxy)]
pub struct HolderTable {
    /// The file the table lives in.
    pub path: String,
    /// How many slots the table holds.
    pub capacity: u64,
    #[psfield(skip)]
    inner: SharedHolderTable,
}

impl HolderTable {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        if capacity == 0 {
            return Err(arg_err("the capacity must be at least one"));
        }
        let slots = size(capacity, "the capacity")?;
        let inner = if open { SharedHolderTable::open(&path, slots) } else { SharedHolderTable::create(&path, slots) }
            .map_err(|e| open_err("the holder table", &path, e))?;
        Ok(Self { path, capacity, inner })
    }
}

/// The operations of a `SubEtha.HolderTable`.
#[psmethods]
impl HolderTable {
    /// How many slots are held right now.
    pub fn live(&self) -> PsResult<u64> {
        Ok(self.inner.live() as u64)
    }

    /// Takes a slot and puts `payload` in it, or `$null` when every
    /// slot is taken.
    pub fn claim(&self, payload: u64) -> PsResult<Option<u64>> {
        Ok(self.inner.claim(payload).map(|s| s as u64))
    }

    /// Takes a slot without publishing anything into it yet.
    pub fn reserve(&self) -> PsResult<Option<u64>> {
        Ok(self.inner.reserve().map(|s| s as u64))
    }

    /// Puts `payload` in a slot this caller already holds.
    pub fn publish(&self, slot: u64, payload: u64) -> PsResult<()> {
        self.inner.publish(size(slot, "the slot")?, payload);
        Ok(())
    }

    /// What a slot holds, or `$null` when nobody holds it.
    pub fn payload(&self, slot: u64) -> PsResult<Option<u64>> {
        Ok(self.inner.payload(size(slot, "the slot")?))
    }

    /// Gives a slot back.
    pub fn release(&self, slot: u64) -> PsResult<()> {
        self.inner.release(size(slot, "the slot")?);
        Ok(())
    }
}

/// Obtains the holder table at Path holding Capacity slots, creating it
/// when the file does not exist.
///
/// # Examples
///
/// `$holders = New-SubEthaHolderTable -Path C:\ipc\holdertable -Capacity 8`
#[cmdlet(verb = "New", noun = "SubEthaHolderTable", alias = "New-SEHolderTable", output = ["SubEtha.HolderTable"])]
#[derive(Default)]
pub struct NewSubEthaHolderTable {
    /// The file the table lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the table holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaHolderTable {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HolderTable::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the holder table at Path, which must exist with the
/// Capacity it was created with.
///
/// # Examples
///
/// `$holders = Open-SubEthaHolderTable -Path C:\ipc\holdertable -Capacity 8`
#[cmdlet(verb = "Open", noun = "SubEthaHolderTable", alias = "Open-SEHolderTable", output = ["SubEtha.HolderTable"])]
#[derive(Default)]
pub struct OpenSubEthaHolderTable {
    /// The file the table lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the table holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaHolderTable {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HolderTable::obtain(path, self.capacity, true)?)
    }
}

/// What a heartbeat slot says about itself.
#[psclass(name = "SubEtha.HeartbeatSlot")]
#[derive(Clone, Default)]
pub struct HeartbeatSlot {
    /// The process behind the slot.
    pub pid: u32,
    /// The epoch it last beat in.
    pub last_seen_epoch: u64,
    /// The bitmap of work it reports in flight.
    pub in_flight: u64,
    /// The role it reports.
    pub role: u32,
}

/// A table of live participants in a mapped file. Each beats its own
/// slot; a slot that stops beating is how everyone else learns the
/// process behind it is gone.
#[psclass(name = "SubEtha.Heartbeat", mode = proxy)]
pub struct Heartbeat {
    /// The file the table lives in.
    pub path: String,
    /// How many slots the table holds.
    pub capacity: u64,
    #[psfield(skip)]
    inner: Arc<HeartbeatTable>,
}

impl Heartbeat {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        if capacity == 0 {
            return Err(arg_err("the capacity must be at least one"));
        }
        let slots = size(capacity, "the capacity")?;
        let inner = if open { HeartbeatTable::open(&path, slots) } else { HeartbeatTable::create(&path, slots) }
            .map_err(|e| open_err("the heartbeat table", &path, e))?;
        Ok(Self { path, capacity, inner: Arc::new(inner) })
    }
}

/// The operations of a `SubEtha.Heartbeat`.
#[psmethods]
impl Heartbeat {
    /// Takes a slot for `pid`, this process when absent. A full table
    /// is an error rather than `$null`: it is a configuration that
    /// cannot serve this process, not an answer to a question.
    pub fn register(&self, pid: Option<u32>) -> PsResult<u64> {
        self.inner.register(crate::common::pid(pid)).map(|s| s as u64).map_err(|e| op_err("registering", e))
    }

    /// Gives a slot back.
    pub fn unregister(&self, slot: u64) -> PsResult<()> {
        self.inner.unregister(size(slot, "the slot")?);
        Ok(())
    }

    /// Says this slot is still alive. A slot that stops beating for
    /// longer than the grace period counts as dead.
    pub fn beat(&self, slot: u64) -> PsResult<()> {
        self.inner.beat(size(slot, "the slot")?);
        Ok(())
    }

    /// The epoch the table measures the grace period in.
    pub fn global_epoch(&self) -> PsResult<u64> {
        Ok(self.inner.global_epoch())
    }

    /// Steps the epoch and returns the epoch it reached.
    pub fn tick_global_epoch(&self) -> PsResult<u64> {
        Ok(self.inner.tick_global_epoch())
    }

    /// What a slot currently says about itself, or `$null` when nothing
    /// holds it.
    pub fn snapshot(&self, slot: u64) -> PsResult<Option<HeartbeatSlot>> {
        Ok(self.inner.snapshot(size(slot, "the slot")?).map(|s| HeartbeatSlot {
            pid: s.pid,
            last_seen_epoch: s.last_seen_epoch,
            in_flight: s.in_flight_bitmap,
            role: s.role,
        }))
    }

    /// A barrier over this table at `path`, which every participant
    /// reaches before any goes on, treating a participant quiet for
    /// more than `graceEpochs` epochs (three when absent) as gone.
    /// `open` attaches to a barrier that exists instead of creating
    /// one.
    pub fn barrier(&self, path: String, grace_epochs: Option<u64>, open: Option<bool>) -> PsResult<EpochBarrier> {
        EpochBarrier::obtain(path, Arc::clone(&self.inner), grace_epochs.unwrap_or(3), open.unwrap_or(false))
    }
}

/// Obtains the heartbeat table at Path holding Capacity slots, creating
/// it when the file does not exist.
///
/// # Examples
///
/// `$heartbeat = New-SubEthaHeartbeat -Path C:\ipc\heartbeat -Capacity 8`
#[cmdlet(verb = "New", noun = "SubEthaHeartbeat", alias = "New-SEHeartbeat", output = ["SubEtha.Heartbeat"])]
#[derive(Default)]
pub struct NewSubEthaHeartbeat {
    /// The file the table lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the table holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaHeartbeat {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Heartbeat::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the heartbeat table at Path, which must exist with the
/// Capacity it was created with.
///
/// # Examples
///
/// `$heartbeat = Open-SubEthaHeartbeat -Path C:\ipc\heartbeat -Capacity 8`
#[cmdlet(verb = "Open", noun = "SubEthaHeartbeat", alias = "Open-SEHeartbeat", output = ["SubEtha.Heartbeat"])]
#[derive(Default)]
pub struct OpenSubEthaHeartbeat {
    /// The file the table lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the table holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaHeartbeat {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Heartbeat::obtain(path, self.capacity, true)?)
    }
}

/// A barrier every participant reaches before any of them goes on.
///
/// It counts live peers through a heartbeat table, so a process that
/// dies while others wait stops being counted rather than holding the
/// barrier shut for ever. Built by a `SubEtha.Heartbeat`'s Barrier or
/// by the barrier cmdlets, which open the table themselves.
#[psclass(name = "SubEtha.EpochBarrier", mode = proxy)]
pub struct EpochBarrier {
    /// The file the barrier lives in.
    pub path: String,
    /// Epochs a participant may stay quiet before it stops counting.
    pub grace_epochs: u64,
    #[psfield(skip)]
    inner: SubethaEpochBarrier,
}

impl EpochBarrier {
    fn obtain(path: String, table: Arc<HeartbeatTable>, grace_epochs: u64, open: bool) -> PsResult<Self> {
        let inner = if open { SubethaEpochBarrier::open(&path, table, grace_epochs) } else { SubethaEpochBarrier::create(&path, table, grace_epochs) }
            .map_err(|e| open_err("the barrier", &path, e))?;
        Ok(Self { path, grace_epochs, inner })
    }
}

/// The operations of a `SubEtha.EpochBarrier`.
#[psmethods]
impl EpochBarrier {
    /// How many participants are still beating.
    pub fn live_peers(&self) -> PsResult<u32> {
        Ok(self.inner.live_peer_count())
    }

    /// The epoch the barrier is at.
    pub fn current_epoch(&self) -> PsResult<u32> {
        Ok(self.inner.current_epoch())
    }

    /// How many have arrived at the current epoch.
    pub fn arrived(&self) -> PsResult<u32> {
        Ok(self.inner.arrived_count())
    }

    /// Arrives at `epoch` and waits for everyone else, or for `quorum`
    /// of them, or until `timeout` seconds pass. True when the barrier
    /// opened and false on a timeout.
    pub fn wait(&self, epoch: u32, timeout: Option<f64>, quorum: Option<u32>) -> PsResult<bool> {
        let timeout = match timeout {
            Some(t) => {
                let d = seconds(t)?;
                if d.is_zero() {
                    return Err(arg_err("the timeout must be positive"));
                }
                Some(d)
            }
            None => None,
        };
        let outcome = match (timeout, quorum) {
            (Some(t), Some(q)) => self.inner.wait_quorum_timeout(epoch, q, t),
            (Some(t), None) => self.inner.wait_timeout(epoch, t),
            (None, Some(q)) => self.inner.wait_quorum(epoch, q),
            (None, None) => self.inner.wait(epoch),
        };
        match outcome {
            Ok(()) => Ok(true),
            Err(BarrierError::Timeout) => Ok(false),
            Err(e) => Err(op_err("waiting at the barrier", e)),
        }
    }
}

/// Obtains the barrier at Path over the heartbeat table at
/// HeartbeatPath, creating it when the file does not exist.
///
/// # Examples
///
/// `$barrier = New-SubEthaEpochBarrier -Path C:\ipc\barrier -HeartbeatPath C:\ipc\heartbeat -Capacity 8`
#[cmdlet(verb = "New", noun = "SubEthaEpochBarrier", alias = "New-SEEpochBarrier", output = ["SubEtha.EpochBarrier"])]
#[derive(Default)]
pub struct NewSubEthaEpochBarrier {
    /// The file the barrier lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The file of the heartbeat table that counts the participants.
    #[param(mandatory, position = 1)]
    pub heartbeat_path: String,
    /// How many slots the heartbeat table holds.
    #[param(mandatory, position = 2)]
    pub capacity: u64,
    /// Epochs a participant may stay quiet before it stops counting;
    /// three when absent.
    #[param]
    pub grace_epochs: Option<u64>,
}

impl Cmdlet for NewSubEthaEpochBarrier {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let table = Heartbeat::obtain(full_path(ps, &self.heartbeat_path)?, self.capacity, false)?;
        ps.write(EpochBarrier::obtain(path, table.inner, self.grace_epochs.unwrap_or(3), false)?)
    }
}

/// Attaches to the barrier at Path, which must exist, over the heartbeat
/// table at HeartbeatPath.
///
/// # Examples
///
/// `$barrier = Open-SubEthaEpochBarrier -Path C:\ipc\barrier -HeartbeatPath C:\ipc\heartbeat -Capacity 8`
#[cmdlet(verb = "Open", noun = "SubEthaEpochBarrier", alias = "Open-SEEpochBarrier", output = ["SubEtha.EpochBarrier"])]
#[derive(Default)]
pub struct OpenSubEthaEpochBarrier {
    /// The file the barrier lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The file of the heartbeat table that counts the participants.
    #[param(mandatory, position = 1)]
    pub heartbeat_path: String,
    /// How many slots the heartbeat table holds.
    #[param(mandatory, position = 2)]
    pub capacity: u64,
    /// Epochs a participant may stay quiet before it stops counting;
    /// three when absent.
    #[param]
    pub grace_epochs: Option<u64>,
}

impl Cmdlet for OpenSubEthaEpochBarrier {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let table = Heartbeat::obtain(full_path(ps, &self.heartbeat_path)?, self.capacity, true)?;
        ps.write(EpochBarrier::obtain(path, table.inner, self.grace_epochs.unwrap_or(3), true)?)
    }
}

/// A condition variable shared between processes: a waiter parks until
/// another process says the thing it is waiting for has happened.
///
/// The wait takes a script block and runs it from inside the wait,
/// three times per round: before parking, again after the park slot is
/// taken, and after each wake. That last pair is what closes the gap
/// where a notify lands between the check and the park, so a script
/// cannot own the loop without losing it. A script block runs only on
/// the pipeline thread, so the wait is the Wait-SubEthaCondition cmdlet
/// rather than a method.
#[psclass(name = "SubEtha.Condvar", mode = proxy)]
pub struct Condvar {
    /// The file the condition lives in.
    pub path: String,
    #[psfield(skip)]
    inner: Arc<SharedCondvar>,
}

impl Condvar {
    fn obtain(path: String, open: bool) -> PsResult<Self> {
        let inner = if open { SharedCondvar::open(&path) } else { SharedCondvar::create(&path) }.map_err(|e| open_err("the condition", &path, e))?;
        Ok(Self { path, inner: Arc::new(inner) })
    }
}

/// The operations of a `SubEtha.Condvar`.
#[psmethods]
impl Condvar {
    /// Steps on every notify. A waiter that sees the same value twice
    /// knows nothing was announced between the two readings.
    pub fn generation(&self) -> PsResult<u64> {
        Ok(self.inner.generation())
    }

    /// Wakes at most one waiter and returns how many were woken. The
    /// caller is responsible for having made the condition true first,
    /// which is the contract every condition variable has.
    pub fn notify_one(&self) -> PsResult<u64> {
        Ok(self.inner.notify_one() as u64)
    }

    /// Wakes every waiter and returns how many were woken.
    pub fn notify_all(&self) -> PsResult<u64> {
        Ok(self.inner.notify_all() as u64)
    }
}

/// Obtains the condition at Path, creating it when the file does not
/// exist.
///
/// # Examples
///
/// `$condvar = New-SubEthaCondvar -Path C:\ipc\condvar`
#[cmdlet(verb = "New", noun = "SubEthaCondvar", alias = "New-SECondvar", output = ["SubEtha.Condvar"])]
#[derive(Default)]
pub struct NewSubEthaCondvar {
    /// The file the condition lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for NewSubEthaCondvar {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Condvar::obtain(path, false)?)
    }
}

/// Attaches to the condition at Path, which must exist.
///
/// # Examples
///
/// `$condvar = Open-SubEthaCondvar -Path C:\ipc\condvar`
#[cmdlet(verb = "Open", noun = "SubEthaCondvar", alias = "Open-SECondvar", output = ["SubEtha.Condvar"])]
#[derive(Default)]
pub struct OpenSubEthaCondvar {
    /// The file the condition lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for OpenSubEthaCondvar {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Condvar::obtain(path, true)?)
    }
}

/// Waits on the condition at Path until Until answers true, or until
/// Timeout seconds pass, and writes whether the condition became true.
/// Until runs from inside the wait on this thread; an error it raises
/// ends the wait and is reported rather than read as a false answer.
///
/// # Examples
///
/// `Wait-SubEthaCondition -Path C:\ipc\condvar -Until { $counter.Load() -gt 0 } -Timeout 5`
#[cmdlet(verb = "Wait", noun = "SubEthaCondition", alias = "Wait-SECondition", output = ["System.Boolean"])]
#[derive(Default)]
pub struct WaitSubEthaCondition {
    /// The file the condition lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The condition, a script block whose last output is the answer.
    #[param(mandatory, position = 1)]
    pub until: PsScriptBlock,
    /// How many seconds to wait at most; for ever when absent.
    #[param]
    pub timeout: Option<f64>,
}

impl Cmdlet for WaitSubEthaCondition {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let condition = SharedCondvar::open(&path).map_err(|e| open_err("the condition", &path, e))?;
        let timeout = match self.timeout {
            Some(t) => {
                let d = seconds(t)?;
                if d.is_zero() {
                    return Err(arg_err("the timeout must be positive"));
                }
                Some(d)
            }
            None => None,
        };
        // A check that fails must not leave the waiter parked, so its
        // failure is kept and the check answers true to end the wait;
        // the error is reported below rather than as a satisfied
        // condition.
        let mut failure: Option<PsError> = None;
        let until = &self.until;
        let mut check = || match until.call(ps, &[]) {
            Ok(outputs) => match outputs.last() {
                Some(last) => match bool::from_ps(last) {
                    Ok(truth) => truth,
                    Err(e) => {
                        failure.get_or_insert(e);
                        true
                    }
                },
                None => false,
            },
            Err(e) => {
                failure.get_or_insert(e);
                true
            }
        };
        let outcome = match timeout {
            Some(t) => condition.wait_timeout(&mut check, t),
            None => condition.wait(&mut check),
        };
        if let Some(e) = failure {
            return Err(e);
        }
        match outcome {
            Ok(()) => ps.write(true),
            Err(CondvarError::Timeout) => ps.write(false),
            Err(e) => Err(op_err("waiting on the condition", e)),
        }
    }
}

/// A hybrid logical clock shared between processes: each participant
/// keeps its own clock in one file, and the global fence is the latest
/// reading across all of them. A reading is the physical microseconds
/// and a logical counter that breaks ties when two events share a
/// microsecond.
#[psclass(name = "SubEtha.FenceClock", mode = proxy)]
pub struct FenceClock {
    /// The file the clocks live in.
    pub path: String,
    /// How many participants the clock holds.
    pub capacity: u64,
    #[psfield(skip)]
    inner: SharedFenceClock,
}

impl FenceClock {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        if capacity == 0 {
            return Err(arg_err("the capacity must be at least one"));
        }
        let slots = size(capacity, "the capacity")?;
        let inner = if open { SharedFenceClock::open(&path, slots) } else { SharedFenceClock::create(&path, slots) }
            .map_err(|e| open_err("the clock", &path, e))?;
        Ok(Self { path, capacity, inner })
    }

    fn reading(hlc: Hlc) -> ClockReading {
        ClockReading { physical_us: hlc.physical_us, logical: hlc.logical }
    }
}

/// The operations of a `SubEtha.FenceClock`.
#[psmethods]
impl FenceClock {
    /// The shared physical clock, in microseconds.
    pub fn shared_clock_us(&self) -> PsResult<u64> {
        Ok(self.inner.shared_clock_us())
    }

    /// Takes a participant slot for `pid`, this process when absent.
    /// Every tick and merge names one.
    pub fn register(&self, pid: Option<u32>) -> PsResult<u64> {
        self.inner.register(crate::common::pid(pid)).map(|s| s as u64).map_err(|e| op_err("registering", e))
    }

    /// Gives a participant slot back.
    pub fn unregister(&self, slot: u64) -> PsResult<()> {
        self.inner.unregister(size(slot, "the slot")?);
        Ok(())
    }

    /// Moves this participant's clock on and returns the new reading.
    pub fn tick(&self, slot: u64) -> PsResult<ClockReading> {
        Ok(Self::reading(self.inner.tick(size(slot, "the slot")?)))
    }

    /// Folds a reading received from elsewhere into this participant's
    /// clock, which is what makes the ordering hold across processes.
    pub fn merge(&self, slot: u64, physical_us: u64, logical: u64) -> PsResult<ClockReading> {
        Ok(Self::reading(self.inner.merge(size(slot, "the slot")?, Hlc { physical_us, logical })))
    }

    /// This participant's current reading, without moving it.
    pub fn get_local(&self, slot: u64) -> PsResult<ClockReading> {
        Ok(Self::reading(self.inner.get_local(size(slot, "the slot")?)))
    }

    /// The latest reading across every live participant: every event
    /// any of them has recorded is at or before this.
    pub fn global_fence(&self) -> PsResult<ClockReading> {
        Ok(Self::reading(self.inner.compute_global_fence()))
    }
}

/// Obtains the fence clock at Path holding Capacity participants,
/// creating it when the file does not exist.
///
/// # Examples
///
/// `$clock = New-SubEthaFenceClock -Path C:\ipc\fenceclock -Capacity 8`
#[cmdlet(verb = "New", noun = "SubEthaFenceClock", alias = "New-SEFenceClock", output = ["SubEtha.FenceClock"])]
#[derive(Default)]
pub struct NewSubEthaFenceClock {
    /// The file the clocks live in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many participants the clock holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaFenceClock {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(FenceClock::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the fence clock at Path, which must exist with the
/// Capacity it was created with.
///
/// # Examples
///
/// `$clock = Open-SubEthaFenceClock -Path C:\ipc\fenceclock -Capacity 8`
#[cmdlet(verb = "Open", noun = "SubEthaFenceClock", alias = "Open-SEFenceClock", output = ["SubEtha.FenceClock"])]
#[derive(Default)]
pub struct OpenSubEthaFenceClock {
    /// The file the clocks live in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many participants the clock holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaFenceClock {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(FenceClock::obtain(path, self.capacity, true)?)
    }
}

/// A claimed epoch ticket: the slot holding it, which gives it back, and
/// the epoch it took.
#[psclass(name = "SubEtha.EpochTicket")]
#[derive(Clone, Default)]
pub struct EpochTicket {
    /// The slot holding the ticket.
    pub slot: u64,
    /// The epoch the ticket reserved.
    pub epoch: u64,
}

/// Epoch-based reclamation shared between processes: readers take a
/// ticket at the current epoch, and memory is only reused once every
/// ticket that could still see it has gone.
#[psclass(name = "SubEtha.Epochs", mode = proxy)]
pub struct Epochs {
    /// The file the epochs live in.
    pub path: String,
    /// How many tickets may be open at once.
    pub capacity: u64,
    #[psfield(skip)]
    inner: SharedEpochs,
}

impl Epochs {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        if capacity == 0 {
            return Err(arg_err("the capacity must be at least one"));
        }
        let slots = size(capacity, "the capacity")?;
        let inner = if open { SharedEpochs::open(&path, slots) } else { SharedEpochs::create(&path, slots) }.map_err(|e| open_err("the epochs", &path, e))?;
        Ok(Self { path, capacity, inner })
    }
}

/// The operations of a `SubEtha.Epochs`.
#[psmethods]
impl Epochs {
    /// The published epoch, which is not the same as the latest one. An
    /// open ticket holds this below its own epoch, because the write
    /// that ticket is stamping is not visible yet; that is the
    /// mechanism working, not a lag.
    pub fn now(&self) -> PsResult<u64> {
        Ok(self.inner.now())
    }

    /// Takes the next epoch and returns it, for a writer stamping a
    /// version as it supersedes the last. The returned epoch is not
    /// necessarily what Now then reports: an older open ticket holds
    /// the published epoch below it.
    pub fn advance(&self) -> PsResult<u64> {
        Ok(self.inner.advance())
    }

    /// How many tickets are outstanding.
    pub fn open_tickets(&self) -> PsResult<u64> {
        Ok(self.inner.open_tickets() as u64)
    }

    /// Reserves the next epoch for a compound write. Nothing stamped
    /// with that epoch is visible until the ticket is published, and
    /// Now stays one below it meanwhile; a caller that never publishes
    /// holds every reader's view down until its process ends.
    pub fn claim_ticket(&self) -> PsResult<EpochTicket> {
        let (slot, epoch) = self.inner.claim_ticket().map_err(|e| op_err("claiming a ticket", e))?;
        Ok(EpochTicket { slot: slot as u64, epoch })
    }

    /// Gives a ticket back by its slot.
    pub fn publish_ticket(&self, slot: u64) -> PsResult<()> {
        self.inner.publish_ticket(size(slot, "the slot")?);
        Ok(())
    }

    /// The epochs of tickets whose holders are gone, which is what
    /// makes a crashed reader stop holding reclamation up for ever.
    pub fn dead_tickets(&self) -> PsResult<Vec<u64>> {
        Ok(self.inner.dead_tickets())
    }

    /// Frees a ticket whose holder died. True when one was freed.
    pub fn free_dead_ticket(&self, epoch: u64) -> PsResult<bool> {
        Ok(self.inner.free_dead_ticket(epoch))
    }
}

/// Obtains the epochs at Path holding Capacity tickets, creating them
/// when the file does not exist.
///
/// # Examples
///
/// `$epochs = New-SubEthaEpochs -Path C:\ipc\epochs -Capacity 8`
#[cmdlet(verb = "New", noun = "SubEthaEpochs", alias = "New-SEEpochs", output = ["SubEtha.Epochs"])]
#[derive(Default)]
pub struct NewSubEthaEpochs {
    /// The file the epochs live in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many tickets may be open at once.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaEpochs {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Epochs::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the epochs at Path, which must exist with the Capacity
/// they were created with.
///
/// # Examples
///
/// `$epochs = Open-SubEthaEpochs -Path C:\ipc\epochs -Capacity 8`
#[cmdlet(verb = "Open", noun = "SubEthaEpochs", alias = "Open-SEEpochs", output = ["SubEtha.Epochs"])]
#[derive(Default)]
pub struct OpenSubEthaEpochs {
    /// The file the epochs live in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many tickets may be open at once.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaEpochs {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Epochs::obtain(path, self.capacity, true)?)
    }
}

