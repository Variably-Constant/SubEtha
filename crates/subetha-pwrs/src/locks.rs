//! The holds: a reader-writer lock, a counting semaphore and an owner
//! lease, each handing back an object that gives the hold back when
//! disposed or collected.

use std::sync::Arc;

use pwrs::prelude::*;

use subetha_cxc::blocking_rw_lock::{BlockingRWLock, BlockingRWLockError};
use subetha_cxc::blocking_semaphore::{BlockingSemaphore, BlockingSemaphoreError};
use subetha_cxc::owner_lease::{LeaseError, OwnerLease as SubethaOwnerLease};
use subetha_cxc::shared_rw_lock::RWLockError;
use subetha_cxc::shared_semaphore::SemaphoreError;

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, open_err, pid, seconds, LeaseValue, LEASE_VALUE_BYTES};

assert_send!(RWLock, Hold, Semaphore, PermitHold, OwnerLease, LeaseHold);

/// A bounded wait's duration: positive seconds.
fn bounded(timeout: f64) -> PsResult<std::time::Duration> {
    let d = seconds(timeout)?;
    if d.is_zero() {
        return Err(arg_err("the timeout must be positive"));
    }
    Ok(d)
}

/// A reader-writer lock in a mapped file, held across processes.
///
/// The C ABI hands a caller a 64-bit token and trusts it to give the
/// token back. Here Read and Write return a `SubEtha.Hold` that gives
/// the hold back when disposed or collected, so a script that leaves a
/// block early does not strand the lock. The waiting forms that spin
/// and the ones that sleep are two ways at one lock rather than two
/// locks.
#[psclass(name = "SubEtha.RWLock", mode = proxy)]
pub struct RWLock {
    /// The file the lock lives in.
    pub path: String,
    #[psfield(skip)]
    parked: Arc<BlockingRWLock>,
}

impl RWLock {
    fn obtain(path: String, open: bool) -> PsResult<Self> {
        let parked = if open { BlockingRWLock::open(&path) } else { BlockingRWLock::create(&path) }.map_err(|e| open_err("the lock", &path, e))?;
        Ok(Self { path, parked: Arc::new(parked) })
    }

    fn hold(&self, write: bool) -> Hold {
        Hold { write, held: true, lock: Arc::clone(&self.parked) }
    }
}

/// The operations of a `SubEtha.RWLock`.
#[psmethods]
impl RWLock {
    /// How many readers hold it right now.
    pub fn readers(&self) -> PsResult<u32> {
        Ok(self.parked.inner().reader_count())
    }

    /// Takes the read hold, waiting for it. Other readers may hold it
    /// at the same time; a writer may not.
    pub fn read(&self) -> PsResult<Hold> {
        // The guard borrows the lock and the hold outlives this call,
        // so it is forgotten on purpose and the hold releases instead,
        // which is what the C ABI does with the same guards.
        let guard = self.parked.inner().read_lock();
        std::mem::forget(guard);
        Ok(self.hold(false))
    }

    /// Takes the read hold, giving up after `timeout` seconds and
    /// answering `$null`. This one sleeps rather than spinning, so a
    /// long wait costs no processor, and it never waits past the
    /// deadline even if whoever holds the lock never says it has
    /// finished.
    pub fn read_for(&self, timeout: f64) -> PsResult<Option<Hold>> {
        match self.parked.read_park_timeout(bounded(timeout)?) {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(self.hold(false)))
            }
            Err(BlockingRWLockError::Timeout) => Ok(None),
            Err(e) => Err(op_err("taking the read hold", e)),
        }
    }

    /// Takes the read hold if it is free, or answers `$null`. Only a
    /// contended lock answers `$null`; anything else is an error, so a
    /// broken lock never reads as a busy one.
    pub fn try_read(&self) -> PsResult<Option<Hold>> {
        match self.parked.inner().try_read_lock() {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(self.hold(false)))
            }
            Err(RWLockError::WouldBlock) => Ok(None),
            Err(e) => Err(op_err("taking the read hold", e)),
        }
    }

    /// Takes the write hold, waiting for it. Nobody else holds it while
    /// this does.
    pub fn write(&self) -> PsResult<Hold> {
        let guard = self.parked.inner().write_lock();
        std::mem::forget(guard);
        Ok(self.hold(true))
    }

    /// Takes the write hold, giving up after `timeout` seconds and
    /// answering `$null`.
    pub fn write_for(&self, timeout: f64) -> PsResult<Option<Hold>> {
        match self.parked.write_park_timeout(bounded(timeout)?) {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(self.hold(true)))
            }
            Err(BlockingRWLockError::Timeout) => Ok(None),
            Err(e) => Err(op_err("taking the write hold", e)),
        }
    }

    /// Takes the write hold if it is free, or answers `$null`.
    pub fn try_write(&self) -> PsResult<Option<Hold>> {
        match self.parked.inner().try_write_lock() {
            Ok(guard) => {
                std::mem::forget(guard);
                Ok(Some(self.hold(true)))
            }
            Err(RWLockError::WouldBlock) => Ok(None),
            Err(e) => Err(op_err("taking the write hold", e)),
        }
    }
}

/// Obtains the lock at Path, creating it when the file does not exist.
#[cmdlet(verb = "New", noun = "SubEthaRWLock", alias = "New-SERWLock", output = ["SubEtha.RWLock"])]
#[derive(Default)]
pub struct NewSubEthaRWLock {
    /// The file the lock lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for NewSubEthaRWLock {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(RWLock::obtain(path, false)?)
    }
}

/// Attaches to the lock at Path, which must exist.
#[cmdlet(verb = "Open", noun = "SubEthaRWLock", alias = "Open-SERWLock", output = ["SubEtha.RWLock"])]
#[derive(Default)]
pub struct OpenSubEthaRWLock {
    /// The file the lock lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for OpenSubEthaRWLock {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(RWLock::obtain(path, true)?)
    }
}

/// A hold on a lock, given back when released, disposed or collected.
/// It keeps the lock alive for as long as it is held.
#[psclass(name = "SubEtha.Hold", mode = proxy)]
pub struct Hold {
    /// Whether this is the write hold rather than a read hold.
    pub write: bool,
    /// Whether the hold is still held.
    pub held: bool,
    #[psfield(skip)]
    lock: Arc<BlockingRWLock>,
}

impl Hold {
    fn give_back(&mut self) {
        if !self.held {
            return;
        }
        if self.write {
            self.lock.inner().release_write_for_blocking();
        } else {
            self.lock.inner().release_read_for_blocking();
        }
        // The hold was taken without keeping the guard that would do
        // this on the way out, so it is done here. Without it a thread
        // waiting in ReadFor or WriteFor sleeps to its deadline with the
        // lock already free.
        self.lock.signal_unlock();
        self.held = false;
    }
}

/// The operations of a `SubEtha.Hold`.
#[psmethods]
impl Hold {
    /// Gives the hold back now rather than when the object goes.
    /// Calling it twice is harmless.
    pub fn release(&mut self) -> PsResult<()> {
        self.give_back();
        Ok(())
    }
}

impl Drop for Hold {
    /// A hold nobody released is released here, so a script that never
    /// called Release costs nothing worse than a later release.
    fn drop(&mut self) {
        self.give_back();
    }
}

/// A counting semaphore in a mapped file, limiting how many processes
/// work at once. Acquire returns a `SubEtha.PermitHold` that gives the
/// permit back when disposed or collected.
#[psclass(name = "SubEtha.Semaphore", mode = proxy)]
pub struct Semaphore {
    /// The file the semaphore lives in.
    pub path: String,
    /// The most permits it can hold.
    pub max_permits: u32,
    #[psfield(skip)]
    parked: Arc<BlockingSemaphore>,
}

impl Semaphore {
    fn permit(&self) -> PermitHold {
        PermitHold { held: true, semaphore: Arc::clone(&self.parked) }
    }
}

/// The operations of a `SubEtha.Semaphore`.
#[psmethods]
impl Semaphore {
    /// Permits available right now.
    pub fn available(&self) -> PsResult<u32> {
        Ok(self.parked.inner().available())
    }

    /// How many are waiting for a permit.
    pub fn waiters(&self) -> PsResult<u32> {
        Ok(self.parked.inner().waiters())
    }

    /// Takes a permit, waiting for one.
    pub fn acquire(&self) -> PsResult<PermitHold> {
        let permit = self.parked.inner().acquire();
        std::mem::forget(permit);
        Ok(self.permit())
    }

    /// Takes a permit, giving up after `timeout` seconds and answering
    /// `$null`. This one sleeps rather than spinning, so a long wait
    /// costs no processor, and it never waits past the deadline even if
    /// whoever holds the permits never gives one back.
    pub fn acquire_for(&self, timeout: f64) -> PsResult<Option<PermitHold>> {
        match self.parked.acquire_park_timeout(bounded(timeout)?) {
            Ok(permit) => {
                std::mem::forget(permit);
                Ok(Some(self.permit()))
            }
            Err(BlockingSemaphoreError::Timeout) => Ok(None),
            Err(e) => Err(op_err("taking a permit", e)),
        }
    }

    /// Takes a permit if one is free, or answers `$null`. Only an
    /// exhausted semaphore answers `$null`; anything else is an error.
    pub fn try_acquire(&self) -> PsResult<Option<PermitHold>> {
        match self.parked.inner().try_acquire() {
            Ok(permit) => {
                std::mem::forget(permit);
                Ok(Some(self.permit()))
            }
            Err(SemaphoreError::WouldBlock) => Ok(None),
            Err(e) => Err(op_err("taking a permit", e)),
        }
    }
}

/// Obtains the semaphore at Path holding Initial permits of at most
/// MaxPermits (Initial when absent), creating it when the file does not
/// exist.
#[cmdlet(verb = "New", noun = "SubEthaSemaphore", alias = "New-SESemaphore", output = ["SubEtha.Semaphore"])]
#[derive(Default)]
pub struct NewSubEthaSemaphore {
    /// The file the semaphore lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many permits it starts with.
    #[param(mandatory, position = 1)]
    pub initial: u32,
    /// The most permits it can hold; Initial when absent.
    #[param]
    pub max_permits: Option<u32>,
}

impl Cmdlet for NewSubEthaSemaphore {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let max = self.max_permits.unwrap_or(self.initial);
        if self.initial > max {
            return Err(arg_err("the initial count cannot exceed MaxPermits"));
        }
        let parked = BlockingSemaphore::create(&path, max, self.initial).map_err(|e| open_err("the semaphore", &path, e))?;
        ps.write(Semaphore { path, max_permits: max, parked: Arc::new(parked) })
    }
}

/// Attaches to the semaphore at Path, which must exist with the
/// MaxPermits it was created with.
#[cmdlet(verb = "Open", noun = "SubEthaSemaphore", alias = "Open-SESemaphore", output = ["SubEtha.Semaphore"])]
#[derive(Default)]
pub struct OpenSubEthaSemaphore {
    /// The file the semaphore lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The most permits it can hold.
    #[param(mandatory, position = 1)]
    pub max_permits: u32,
}

impl Cmdlet for OpenSubEthaSemaphore {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let parked = BlockingSemaphore::open(&path, self.max_permits).map_err(|e| open_err("the semaphore", &path, e))?;
        ps.write(Semaphore { path, max_permits: self.max_permits, parked: Arc::new(parked) })
    }
}

/// A permit, given back when released, disposed or collected.
#[psclass(name = "SubEtha.PermitHold", mode = proxy)]
pub struct PermitHold {
    /// Whether the permit is still held.
    pub held: bool,
    #[psfield(skip)]
    semaphore: Arc<BlockingSemaphore>,
}

impl PermitHold {
    fn give_back(&mut self) -> Result<(), BlockingSemaphoreError> {
        if !self.held {
            return Ok(());
        }
        // Released through the parking wrapper rather than the
        // semaphore itself, because that is what wakes a thread waiting
        // in AcquireFor. Releasing underneath it would leave that thread
        // asleep to its deadline with a permit free.
        let outcome = self.semaphore.release();
        self.held = false;
        outcome
    }
}

/// The operations of a `SubEtha.PermitHold`.
#[psmethods]
impl PermitHold {
    /// Gives the permit back now rather than when the object goes. A
    /// refusal is reported rather than dropped: releasing more permits
    /// than the semaphore allows is a real fault in the caller's
    /// bookkeeping.
    pub fn release(&mut self) -> PsResult<()> {
        self.give_back().map_err(|e| op_err("releasing the permit", e))
    }
}

impl Drop for PermitHold {
    /// A permit that reaches here unreleased is given back. Drop cannot
    /// raise, so a refusal is reported on stderr rather than discarded:
    /// it means the count is wrong, which the next acquirer will feel.
    fn drop(&mut self) {
        match self.give_back() {
            Ok(()) => {}
            Err(e) => eprintln!("subetha: releasing a permit on drop failed: {e:?}"),
        }
    }
}

fn lease_err(doing: &str, e: LeaseError) -> PsError {
    match e {
        LeaseError::IoError(kind) => PsError::new(ErrorCategory::ResourceUnavailable, "SubEthaLease", format!("{doing}: {}", std::io::Error::from(kind))),
        LeaseError::LayoutMismatch => arg_err(format!("{doing}: the file is a lease of another shape")),
        LeaseError::PayloadTooLarge => arg_err(format!("{doing}: the value does not fit a lease")),
        LeaseError::NotOwner => PsError::new(ErrorCategory::PermissionDenied, "SubEthaNotOwner", format!("{doing}: this process does not hold the lease")),
        LeaseError::Contention => PsError::new(ErrorCategory::ResourceBusy, "SubEthaContended", format!("{doing}: the lease is contended")),
    }
}

/// One process at a time owns a small shared value, and if that process
/// dies another takes it over rather than the value being stranded.
///
/// Ownership is by process id, and there are two ways to get it, which
/// a caller has to know apart. The first is that a lower process id
/// takes the lease from a higher one on the spot, whether or not that
/// owner is alive or has just beaten. This is what settles which process
/// leads when several start at once and all want the same job, and it
/// is deliberate: the answer is the same whoever asks, so the processes
/// agree without talking. It also means a claim is not a lock against
/// every other process, only against the ones with higher ids.
///
/// The second is the takeover of a quiet owner. A live owner says it is
/// still here by calling Beat. Time is counted in epochs the holders
/// step themselves with TickEpoch, not in seconds: an owner that has not
/// beaten for more than the grace epochs is treated as gone and any
/// process may take over. Nothing steps the epoch on its own.
///
/// Taking a lease this process already holds succeeds and changes
/// nothing, so a claim is safe to repeat. The value is at most 44
/// bytes: a token saying who is doing what, not a place to put data.
#[psclass(name = "SubEtha.OwnerLease", mode = proxy)]
pub struct OwnerLease {
    /// The file the lease lives in.
    pub path: String,
    /// The most the value may be, in bytes.
    pub max_value_bytes: u64,
    #[psfield(skip)]
    inner: Arc<SubethaOwnerLease<LeaseValue>>,
}

/// The operations of a `SubEtha.OwnerLease`. Every `pid` argument names
/// the process acting, this one when absent; pass another only to stand
/// in for a process that is not here, which is what a test of the
/// takeover does.
#[psmethods]
impl OwnerLease {
    /// Takes the lease, treating a holder quiet for more than
    /// `graceEpochs` epochs (zero when absent) as gone. False when a
    /// process with a lower id holds it and has beaten within the grace
    /// period; true when nobody holds it, when this process already
    /// does, when the holder's id is higher, or when the holder has
    /// been quiet for longer than the grace period.
    pub fn try_acquire(&self, grace_epochs: Option<u64>, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.try_acquire(crate::common::pid(pid), grace_epochs.unwrap_or(0)))
    }

    /// Gives the lease back. False if this process did not hold it.
    pub fn release(&self, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.release(crate::common::pid(pid)))
    }

    /// Takes the lease and returns a `SubEtha.LeaseHold` that gives it
    /// back when released, disposed or collected. An error named
    /// SubEthaContended means a process with a lower id holds it and has
    /// beaten within the grace period; TryAcquire is the same question
    /// asked without an error.
    pub fn hold(&self, grace_epochs: Option<u64>, pid: Option<u32>) -> PsResult<LeaseHold> {
        let claimant = crate::common::pid(pid);
        if !self.inner.try_acquire(claimant, grace_epochs.unwrap_or(0)) {
            let message = match self.inner.current_owner() {
                Some(other) => format!("process {other} holds this lease"),
                None => "the lease could not be taken".to_string(),
            };
            return Err(PsError::new(ErrorCategory::ResourceBusy, "SubEthaContended", message));
        }
        Ok(LeaseHold { pid: claimant, held: true, lease: Arc::clone(&self.inner) })
    }

    /// The value, readable only by the process holding the lease.
    /// `$null` when this process does not hold it.
    pub fn read(&self, pid: Option<u32>) -> PsResult<Option<PsObject>> {
        match self.inner.read_as_owner(crate::common::pid(pid)) {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// Writes the value. False when this process does not hold the
    /// lease.
    pub fn write(&self, value: PsObject, pid: Option<u32>) -> PsResult<bool> {
        let held = LeaseValue::from_bytes(&bytes(&value)?)?;
        Ok(self.inner.write_as_owner(crate::common::pid(pid), held))
    }

    /// Says this process is still here, so its claim does not lapse.
    /// False once it is no longer the owner.
    pub fn beat(&self, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.beat(crate::common::pid(pid)))
    }

    /// Steps the epoch every holder measures the grace period in, and
    /// returns the epoch this reached. Nothing steps it on its own.
    pub fn tick_epoch(&self) -> PsResult<u64> {
        Ok(self.inner.tick_epoch())
    }

    /// The process holding the lease, or `$null` when nobody does.
    pub fn owner(&self) -> PsResult<Option<u32>> {
        Ok(self.inner.current_owner())
    }

    /// Whether the process holds it.
    pub fn held_by(&self, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.am_i_owner(crate::common::pid(pid)))
    }

    /// Steps every time the lease changes hands, so a holder can tell a
    /// takeover from an uninterrupted claim.
    pub fn term(&self) -> PsResult<u32> {
        Ok(self.inner.lease_term())
    }

    /// Puts the lease's file on the disk, and waits for it.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| lease_err("flushing", e))
    }

    /// Asks for the lease's file to reach the disk without waiting.
    pub fn flush_async(&self) -> PsResult<()> {
        self.inner.flush_async().map_err(|e| lease_err("flushing", e))
    }
}

/// Obtains the lease at Path, creating it with Value when the file does
/// not exist; attaching to one that exists leaves its owner, its term
/// and its value alone. With Reset, strips the lease back to no owner
/// and Value, which throws away a claim another process may still
/// believe it has, so it is for a lease known to be wedged.
#[cmdlet(verb = "New", noun = "SubEthaOwnerLease", alias = "New-SEOwnerLease", output = ["SubEtha.OwnerLease"])]
#[derive(Default)]
pub struct NewSubEthaOwnerLease {
    /// The file the lease lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The value the lease starts with, at most 44 bytes; empty when
    /// absent.
    #[param]
    pub value: Option<PsObject>,
    /// Strip the lease back to no owner and Value.
    #[param]
    pub reset: bool,
}

impl Cmdlet for NewSubEthaOwnerLease {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let initial = match &self.value {
            Some(v) => LeaseValue::from_bytes(&bytes(v)?)?,
            None => LeaseValue::empty(),
        };
        let inner = if self.reset { SubethaOwnerLease::reset(&path, initial) } else { SubethaOwnerLease::create(&path, initial) }
            .map_err(|e| lease_err("opening the lease", e))?;
        ps.write(OwnerLease { path, max_value_bytes: LEASE_VALUE_BYTES as u64, inner: Arc::new(inner) })
    }
}

/// Attaches to the lease at Path, which must exist.
#[cmdlet(verb = "Open", noun = "SubEthaOwnerLease", alias = "Open-SEOwnerLease", output = ["SubEtha.OwnerLease"])]
#[derive(Default)]
pub struct OpenSubEthaOwnerLease {
    /// The file the lease lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for OpenSubEthaOwnerLease {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let inner = SubethaOwnerLease::open(&path).map_err(|e| lease_err("attaching to the lease", e))?;
        ps.write(OwnerLease { path, max_value_bytes: LEASE_VALUE_BYTES as u64, inner: Arc::new(inner) })
    }
}

/// A lease held by one process, given back when released, disposed or
/// collected.
#[psclass(name = "SubEtha.LeaseHold", mode = proxy)]
pub struct LeaseHold {
    /// The process holding the lease through this hold.
    pub pid: u32,
    /// Whether the lease is still held.
    pub held: bool,
    #[psfield(skip)]
    lease: Arc<SubethaOwnerLease<LeaseValue>>,
}

impl LeaseHold {
    fn give_back(&mut self) {
        if !self.held {
            return;
        }
        self.lease.release(self.pid);
        self.held = false;
    }
}

/// The operations of a `SubEtha.LeaseHold`.
#[psmethods]
impl LeaseHold {
    /// The value under the lease this hold has, or `$null` once the
    /// hold is gone.
    pub fn read(&self) -> PsResult<Option<PsObject>> {
        match self.lease.read_as_owner(self.pid) {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// Writes the value under the lease this hold has. False once the
    /// hold is gone.
    pub fn write(&self, value: PsObject) -> PsResult<bool> {
        let held = LeaseValue::from_bytes(&bytes(&value)?)?;
        Ok(self.lease.write_as_owner(self.pid, held))
    }

    /// Says this process is still here, so the claim does not lapse
    /// during a long hold.
    pub fn beat(&self) -> PsResult<bool> {
        Ok(self.lease.beat(self.pid))
    }

    /// Gives the lease back now rather than when the object goes.
    /// Calling it twice is harmless.
    pub fn release(&mut self) -> PsResult<()> {
        self.give_back();
        Ok(())
    }
}

impl Drop for LeaseHold {
    /// A lease nobody gave back is given back here, so a script that
    /// never called Release does not strand it until the grace period.
    fn drop(&mut self) {
        self.give_back();
    }
}

/// Keeps the process id helper in use by the holds that name one.
#[allow(dead_code)]
fn this_process() -> u32 {
    pid(None)
}
