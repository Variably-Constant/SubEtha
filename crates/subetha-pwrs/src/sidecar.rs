//! The process's sidecar from PowerShell: registering an object with it,
//! what it drains into stats, policies written as ScriptBlocks, and an
//! adaptive object of the script's own.
//!
//! One sidecar serves the process: a scan thread per NUMA node drains
//! every registered object's observation ring and, when the object was
//! registered with a policy, asks that policy which tag the object should
//! run at. A ScriptBlock policy runs on that scan thread, in a runspace
//! its registration opens for it, so it never waits on the session that
//! registered it.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use pwrs::prelude::*;
use pwrs::{attach_current_thread, PsType};

use subetha_core::{HandshakeHeader, Observation, ObservationRing, SwapCell, SwapCellOption};
use subetha_sidecar::{
    global as sidecar_global, AdaptiveInstance, InstanceId, InstanceStats as SidecarStats,
    NoMigrationPolicy, Policy,
};

use crate::common::{arg_err, assert_send};

assert_send!(InstanceStats, Registration, Adaptive, SidecarStatus);

/// The cadence a ring's own sidecar scans at: none for a strict ring,
/// and for a managed one the caller's interval, else `default_us`. A
/// strict ring given an interval is refused, since its caller most likely
/// meant a managed one, and so is a managed ring with neither an interval
/// nor a default.
pub(crate) fn scan_cadence(
    managed: bool,
    scan_interval_us: Option<u64>,
    default_us: Option<u64>,
    ring: &str,
) -> PsResult<Option<std::time::Duration>> {
    if !managed {
        return match scan_interval_us {
            Some(_) => Err(arg_err("ScanIntervalUs is a managed ring's cadence; pass -Managed with it")),
            None => Ok(None),
        };
    }
    match scan_interval_us.or(default_us) {
        None => Err(arg_err(format!("a managed {ring} needs -ScanIntervalUs; the library names no default"))),
        Some(0) => Err(arg_err("ScanIntervalUs must be at least one microsecond")),
        Some(us) => Ok(Some(std::time::Duration::from_micros(us))),
    }
}

/// Observed rings by address, each with its registration's id once it has
/// one.
type ObservedRings = SwapCell<Vec<(usize, Option<InstanceId>)>>;

/// The observation rings a live `SubEtha.Registration` holds. The sidecar
/// drains a ring from one thread, so Observe refuses a ring already here.
fn observed_rings() -> &'static ObservedRings {
    static RINGS: OnceLock<ObservedRings> = OnceLock::new();
    RINGS.get_or_init(|| SwapCell::new(Vec::new()))
}

/// Take `ring` for a registration being made. `Err` carries the id of
/// the live registration holding it, or `None` while another Observe is
/// still making one.
fn claim_ring(ring: usize) -> Result<(), Option<InstanceId>> {
    let rings = observed_rings();
    loop {
        let held = rings.load_full();
        if let Some(&(_, id)) = held.iter().find(|(address, _)| *address == ring) {
            return Err(id);
        }
        let mut next = Vec::with_capacity(held.len() + 1);
        next.extend_from_slice(&held);
        next.push((ring, None));
        if rings.compare_and_set(&held, Arc::new(next)).is_ok() {
            return Ok(());
        }
    }
}

/// Record the id the registration holding `ring` was given.
fn settle_ring(ring: usize, id: InstanceId) {
    observed_rings().rcu(|held| {
        held.iter()
            .map(|&(address, held_id)| (address, if address == ring { Some(id) } else { held_id }))
            .collect()
    });
}

/// Give `ring` back once its registration has left the sidecar.
fn release_ring(ring: usize) {
    observed_rings().rcu(|held| held.iter().copied().filter(|&(address, _)| address != ring).collect());
}

/// A ring claimed for a registration, given back on drop unless the
/// registration was made.
struct RingClaim {
    ring: usize,
    made: bool,
}

impl Drop for RingClaim {
    fn drop(&mut self) {
        if !self.made {
            release_ring(self.ring);
        }
    }
}

/// How a policy's asking ended when it gave no tag and no "stay".
enum Failure {
    /// The error record the block raised, or the one its answer earned.
    Raised(PsObject),
    /// PowerShell could not ask the block at all, and why.
    Asking(String),
}

/// What a registration keeps of its policy's failures.
struct PolicyFailures {
    count: AtomicU64,
    last: SwapCellOption<Failure>,
}

impl PolicyFailures {
    fn record(&self, failure: Failure) {
        self.count.fetch_add(1, Ordering::AcqRel);
        self.last.store(Some(Arc::new(failure)));
    }
}

/// The script a policy runs inside. It calls the block with the stats and
/// the tag, holds the block to one answer, and hands back one object
/// saying what came of it: the tag, nothing, or the error record.
const ASK_POLICY: &str = r#"
param($policy, $stats, $tag)
$ErrorActionPreference = 'Stop'
try {
    $answer = @(& $policy $stats $tag)
    if ($answer.Count -gt 1) {
        throw [System.Management.Automation.PSInvalidCastException]::new(
            "a policy answers one tag or nothing; it answered $($answer.Count) objects")
    }
    if ($answer.Count -eq 0 -or $null -eq $answer[0]) {
        return [pscustomobject]@{ ok = $true; tag = $null; err = $null }
    }
    $value = $answer[0]
    $integral = $value -is [byte] -or $value -is [sbyte] -or $value -is [int16] -or
        $value -is [uint16] -or $value -is [int] -or $value -is [uint32] -or
        $value -is [long] -or $value -is [uint64]
    if (-not $integral -or $value -lt 0 -or $value -gt [uint32]::MaxValue) {
        throw [System.Management.Automation.PSInvalidCastException]::new(
            "a policy answers a tag, an integer from 0 to 4294967295, or nothing; it answered '$value'")
    }
    [pscustomobject]@{ ok = $true; tag = [uint32]$value; err = $null }
} catch {
    [pscustomobject]@{ ok = $false; tag = $null; err = $_ }
}
"#;

/// What a policy's block answered.
enum Answer {
    Tag(u32),
    Stay,
    Failed(PsObject),
}

/// A ScriptBlock standing as a registration's Policy. The sidecar asks it
/// on its own scan thread after every scan that drained something new,
/// through a PowerShell bound to the registration's own runspace.
struct ScriptPolicy {
    block: PsObject,
    shell: PsObject,
    failures: Arc<PolicyFailures>,
}

impl ScriptPolicy {
    fn ask(&self, stats: &SidecarStats, current_tag: u32) -> PsResult<Answer> {
        self.shell.get("Commands")?.call("Clear", &[])?;
        self.shell.call("AddScript", &[ASK_POLICY.to_string().into_ps()?])?;
        self.shell.call("AddArgument", std::slice::from_ref(&self.block))?;
        self.shell.call("AddArgument", &[InstanceStats::from_rust(*stats).into_ps()?])?;
        self.shell.call("AddArgument", &[current_tag.into_ps()?])?;
        let results = Vec::<PsObject>::from_ps(&self.shell.call("Invoke", &[])?)?;
        let [outcome] = results.as_slice() else {
            return Err(PsError::new(
                ErrorCategory::InvalidResult,
                "SubEthaPolicy",
                format!("the policy's wrapper answered {} objects", results.len()),
            ));
        };
        if !bool::from_ps(&outcome.get("ok")?)? {
            return Ok(Answer::Failed(outcome.get("err")?));
        }
        let tag = outcome.get("tag")?;
        if tag.is_null() {
            Ok(Answer::Stay)
        } else {
            Ok(Answer::Tag(u32::from_ps(&tag)?))
        }
    }
}

impl Policy for ScriptPolicy {
    fn decide(&self, stats: &SidecarStats, current_tag: u32) -> Option<u32> {
        let _attached = attach_current_thread();
        match self.ask(stats, current_tag) {
            Ok(Answer::Tag(tag)) => Some(tag),
            Ok(Answer::Stay) => None,
            Ok(Answer::Failed(record)) => {
                self.failures.record(Failure::Raised(record));
                None
            }
            Err(e) => {
                self.failures.record(Failure::Asking(e.message));
                None
            }
        }
    }
}

/// The runspace a policy runs in and the PowerShell bound to it, closed
/// once the registration has left the sidecar.
struct PolicyRunspace {
    shell: PsObject,
    runspace: PsObject,
}

impl PolicyRunspace {
    /// Open a runspace for `block` and bind a PowerShell to it, with the
    /// block rebuilt from its syntax tree so it runs in that runspace
    /// rather than in the session that wrote it. The runspace runs each
    /// invocation on the thread that asks, which is the scan thread.
    fn open(block: &PsObject) -> PsResult<(Self, PsObject)> {
        if block.type_name()? != "System.Management.Automation.ScriptBlock" {
            return Err(arg_err("a policy is a ScriptBlock taking the stats and the tag"));
        }
        let unbound = block.get("Ast")?.call("GetScriptBlock", &[])?;
        let runspace = PsType::from_name("System.Management.Automation.Runspaces.RunspaceFactory")
            .call_static("CreateRunspace", &[])?;
        runspace.set("ThreadOptions", &"UseCurrentThread".into_ps()?)?;
        runspace.call("Open", &[])?;
        let shell = PsType::from_name("System.Management.Automation.PowerShell").call_static("Create", &[])?;
        shell.set("Runspace", &runspace)?;
        Ok((Self { shell, runspace }, unbound))
    }

    /// Dispose the PowerShell and its runspace.
    fn close(&self) -> PsResult<()> {
        self.shell.call("Dispose", &[])?;
        self.runspace.call("Dispose", &[])?;
        Ok(())
    }
}

/// What the process's sidecar has drained from one registered object, as
/// its last scan left it. A Registration's Stats takes one, and a policy
/// is handed one after every scan that drained something new.
#[psclass(name = "SubEtha.InstanceStats", mode = proxy)]
pub struct InstanceStats {
    /// Observations drained from the object's ring.
    pub ops_observed: u64,
    /// The latencies those observations carried, summed.
    pub total_latency_ticks: u64,
    /// Observations that reported contention.
    pub contention_ops: u64,
    /// Microseconds from registration to the last scan that drained an
    /// observation.
    pub last_drain_us: u64,
    /// Scans whose policy answered a tag other than the current one.
    pub migrations_triggered: u64,
    #[psfield(skip)]
    inner: SidecarStats,
}

impl InstanceStats {
    fn from_rust(inner: SidecarStats) -> Self {
        Self {
            ops_observed: inner.ops_observed,
            total_latency_ticks: inner.total_latency_ticks,
            contention_ops: inner.contention_ops,
            last_drain_us: inner.last_drain_us,
            migrations_triggered: inner.migrations_triggered,
            inner,
        }
    }
}

/// An op kind as the sidecar indexes it.
fn op_kind(kind: u32) -> PsResult<u16> {
    u16::try_from(kind).map_err(|e| arg_err(format!("op kind {kind} is past 65535: {e}")))
}

/// The operations of a `SubEtha.InstanceStats`.
#[psmethods]
impl InstanceStats {
    /// Observations per op kind, indexed by kind: kinds 1 to 6 are counted
    /// apart, 7 and above share the last count, and 0 is unspecified.
    pub fn op_kind_counts(&self) -> PsResult<Vec<u64>> {
        Ok(self.inner.op_kind_counts.to_vec())
    }

    /// Per op kind, the ids of the first producer threads seen for it, in
    /// arrival order; 0 marks a slot no thread has filled.
    pub fn per_op_kind_distinct_threads(&self) -> PsResult<Vec<PsObject>> {
        self.inner.per_op_kind_distinct_threads.iter().map(|slots| slots.to_vec().into_ps()).collect()
    }

    /// Per op kind, how many distinct producer threads were seen, up to
    /// MaxTrackedThreadsPerKind plus one.
    pub fn per_op_kind_distinct_count(&self) -> PsResult<Vec<u32>> {
        Ok(self.inner.per_op_kind_distinct_count.iter().map(|&count| u32::from(count)).collect())
    }

    /// The op kinds the per-kind counts hold.
    pub fn n_op_kinds(&self) -> PsResult<u32> {
        Ok(subetha_sidecar::N_OP_KINDS as u32)
    }

    /// Producer threads remembered per op kind; a count one past this
    /// means more threads than that.
    pub fn max_tracked_threads_per_kind(&self) -> PsResult<u32> {
        Ok(subetha_sidecar::MAX_TRACKED_THREADS_PER_KIND as u32)
    }

    /// TotalLatencyTicks over OpsObserved, and 0 before any observation.
    pub fn average_latency_ticks(&self) -> PsResult<u64> {
        Ok(self.inner.average_latency_ticks())
    }

    /// ContentionOps over OpsObserved, and 0 before any observation.
    pub fn contention_rate(&self) -> PsResult<f64> {
        Ok(self.inner.contention_rate())
    }

    /// The per-kind counts summed.
    pub fn op_kind_total(&self) -> PsResult<u64> {
        Ok(self.inner.op_kind_total())
    }

    /// The count of `kind` over the counts of `totalKinds` summed, and 0
    /// while those are all zero.
    pub fn ratio_of(&self, kind: u32, total_kinds: Vec<u32>) -> PsResult<f64> {
        let kinds = total_kinds.into_iter().map(op_kind).collect::<PsResult<Vec<u16>>>()?;
        Ok(self.inner.ratio_of(op_kind(kind)?, &kinds))
    }

    /// Distinct producer threads seen for `kind`.
    pub fn distinct_threads_for(&self, kind: u32) -> PsResult<u32> {
        Ok(u32::from(self.inner.distinct_threads_for(op_kind(kind)?)))
    }

    /// Whether `kind` was seen from two producer threads or more.
    pub fn is_multi_thread_for(&self, kind: u32) -> PsResult<bool> {
        Ok(self.inner.is_multi_thread_for(op_kind(kind)?))
    }
}

/// One object registered with the process's sidecar. The sidecar drains
/// what the object records into its Stats, and when Observe was given a
/// policy it asks that ScriptBlock after every scan that drained something
/// new which tag the object should run at.
///
/// Close, Dispose or collection unregisters it, and the object can then be
/// observed again. An object has one registration at a time, because the
/// sidecar drains its ring from one thread.
///
/// A policy runs on the sidecar's own thread, in a runspace of its own, so
/// it sees none of the registering session's variables: it works from the
/// stats and the tag it is handed. A policy that never returns stalls the
/// sidecar, and a policy must not close its own registration.
#[psclass(name = "SubEtha.Registration", mode = proxy)]
pub struct Registration {
    /// The sidecar's id for this registration.
    pub id: u32,
    #[psfield(skip)]
    ring: usize,
    #[psfield(skip)]
    closed: AtomicBool,
    #[psfield(skip)]
    failures: Arc<PolicyFailures>,
    #[psfield(skip)]
    runspace: Option<PolicyRunspace>,
    #[psfield(skip)]
    owner: Arc<dyn AdaptiveInstance>,
}

impl Registration {
    /// Unregister, once, then close the policy's runspace, which no scan
    /// can be using once the sidecar has let the registration go.
    fn close_now(&self) -> PsResult<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        sidecar_global().unregister(self.id);
        release_ring(self.ring);
        match &self.runspace {
            Some(runspace) => runspace.close(),
            None => Ok(()),
        }
    }

    fn live(&self) -> PsResult<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(PsError::new(ErrorCategory::InvalidOperation, "SubEthaClosed", "the registration is closed"))
        } else {
            Ok(())
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Err(e) = self.close_now() {
            // A Drop has no caller to hand this to.
            eprintln!("subetha: closing registration {}'s policy runspace failed: {}", self.id, e.message);
        }
    }
}

/// The operations of a `SubEtha.Registration`.
#[psmethods]
impl Registration {
    /// Whether Close has run.
    pub fn closed(&self) -> PsResult<bool> {
        Ok(self.closed.load(Ordering::Acquire))
    }

    /// The strategy tag the object runs at, which a policy's answer sets.
    pub fn tag(&self) -> PsResult<u32> {
        self.live()?;
        Ok(self.owner.header().tag())
    }

    /// What the sidecar has drained from the object so far, as its last
    /// scan left it. Invoke-SubEthaSidecarScan drains what is waiting.
    pub fn stats(&self) -> PsResult<InstanceStats> {
        self.live()?;
        match sidecar_global().stats(self.id) {
            Some(inner) => Ok(InstanceStats::from_rust(inner)),
            None => Err(PsError::new(
                ErrorCategory::InvalidOperation,
                "SubEthaSidecar",
                format!("the sidecar holds no registration {}", self.id),
            )),
        }
    }

    /// Times the policy failed: it threw, wrote an error, or answered
    /// something other than one tag or nothing. Each such scan left the
    /// tag where it was.
    pub fn policy_errors(&self) -> PsResult<u64> {
        Ok(self.failures.count.load(Ordering::Acquire))
    }

    /// The last such failure as its error record, or `$null` before the
    /// first. When PowerShell could not run the policy at all, the reason
    /// comes back as an InvalidOperationException.
    pub fn last_policy_error(&self) -> PsResult<Option<PsObject>> {
        match self.failures.last.load_full() {
            None => Ok(None),
            Some(failure) => match &*failure {
                Failure::Raised(record) => Ok(Some(record.clone())),
                Failure::Asking(why) => Ok(Some(
                    PsType::from_name("System.InvalidOperationException").new(&[why.clone().into_ps()?])?,
                )),
            },
        }
    }

    /// Unregisters from the sidecar and closes the policy's runspace.
    /// Returns once no scan is inside the registration, and the object
    /// can then be observed again.
    pub fn close(&self) -> PsResult<()> {
        self.close_now()
    }
}

/// The error Observe refuses with once the sidecar holds as many objects
/// as it allows.
fn over_cap(cap: usize) -> PsError {
    PsError::new(
        ErrorCategory::LimitsExceeded,
        "SubEthaSidecarCap",
        format!(
            "subetha-sidecar: instance cap ({cap}) exceeded. Likely cause: Observe() is being called \
             inside a loop, registering an object on every pass. Register once and reuse the \
             registration, or run Set-SubEthaSidecar -MaxInstances if the load is intentional."
        ),
    )
}

/// Register `owner` with the process's sidecar, under `policy` or,
/// without one, the object's own.
pub(crate) fn observe<T: AdaptiveInstance>(owner: Arc<T>, policy: Option<PsObject>) -> PsResult<Registration> {
    let owner: Arc<dyn AdaptiveInstance> = owner;
    let failures = Arc::new(PolicyFailures { count: AtomicU64::new(0), last: SwapCellOption::empty() });
    let (policy, runspace): (Box<dyn Policy>, Option<PolicyRunspace>) = match policy.filter(|p| !p.is_null()) {
        None => (owner.make_policy(), None),
        Some(block) => {
            let (runspace, unbound) = PolicyRunspace::open(&block)?;
            let asked = ScriptPolicy { block: unbound, shell: runspace.shell.clone(), failures: Arc::clone(&failures) };
            (Box::new(asked), Some(runspace))
        }
    };
    let ring = owner.ring() as *const ObservationRing as usize;
    let mut claim = match claim_ring(ring) {
        Ok(()) => RingClaim { ring, made: false },
        Err(Some(id)) => {
            return Err(arg_err(format!("this object is already observed by registration {id}; close that one first")))
        }
        Err(None) => return Err(arg_err("this object is being observed by another call right now")),
    };
    let sidecar = sidecar_global();
    let cap = sidecar.max_instances();
    if sidecar.instance_count() >= cap {
        if let Some(opened) = &runspace {
            opened.close()?;
        }
        return Err(over_cap(cap));
    }
    // SAFETY: the registration returned holds `owner`, so the header, the
    // ring and the instance stay where they are for as long as it lives,
    // and it unregisters before it lets `owner` go. The claim above means
    // the ring has no other registration, so the sidecar is its one
    // consumer.
    let id = unsafe {
        sidecar.register_raw(
            std::ptr::NonNull::from(owner.header()),
            std::ptr::NonNull::from(owner.ring()),
            Some(std::ptr::NonNull::from(&*owner)),
            policy,
        )
    };
    settle_ring(ring, id);
    claim.made = true;
    Ok(Registration { id, ring, closed: AtomicBool::new(false), failures, runspace, owner })
}

/// The strategy tag and observation ring a `SubEtha.Adaptive` carries.
struct OwnInstance {
    header: HandshakeHeader,
    ring: ObservationRing,
}

impl AdaptiveInstance for OwnInstance {
    fn header(&self) -> &HandshakeHeader {
        &self.header
    }

    fn ring(&self) -> &ObservationRing {
        &self.ring
    }

    fn make_policy(&self) -> Box<dyn Policy> {
        Box::new(NoMigrationPolicy)
    }
}

/// An adaptive object of the script's own: a strategy tag, and a ring the
/// script records its operations into. Observed with a policy, it has the
/// process's sidecar drain the ring into stats after each scan and move
/// the tag to whatever the policy answers, and the script reads Tag to
/// choose how it works. It is what a Rust type implementing
/// `AdaptiveInstance` is, and like the sidecar it lives in this process.
#[psclass(name = "SubEtha.Adaptive", mode = proxy)]
pub struct Adaptive {
    #[psfield(skip)]
    inner: Arc<OwnInstance>,
}

/// The operations of a `SubEtha.Adaptive`.
#[psmethods]
impl Adaptive {
    /// Records one operation of kind `opKind` that took `latencyTicks`, in
    /// whatever unit the policy reads. Kinds 1 to 6 are counted apart, 7
    /// and above share one count, and 0 is unspecified. `contended` marks
    /// an operation that took a slow path, which is what ContentionRate
    /// counts, and `empty` one that found nothing. False when the ring did
    /// not take it: before the object is observed, or while the ring is
    /// full.
    pub fn record(&self, op_kind: u32, latency_ticks: Option<u64>, contended: Option<bool>, empty: Option<bool>) -> PsResult<bool> {
        let flags = u16::from(contended == Some(true)) | (u16::from(empty == Some(true)) << 1);
        Ok(self.inner.ring.push(Observation {
            op_kind: crate::sidecar::op_kind(op_kind)?,
            flags,
            latency_ticks: latency_ticks.unwrap_or(0),
            ..Observation::ZERO
        }))
    }

    /// The strategy tag, which a policy's answer moves.
    pub fn tag(&self) -> PsResult<u32> {
        Ok(self.inner.header.tag())
    }

    /// Moves the tag from the script's own code.
    pub fn set_tag(&self, tag: u32) -> PsResult<()> {
        self.inner.header.set_tag(tag);
        Ok(())
    }

    /// Registers this object with the process's sidecar, under `policy`
    /// when given one; see SubEtha.Registration.
    pub fn observe(&self, policy: Option<PsObject>) -> PsResult<Registration> {
        observe(Arc::clone(&self.inner), policy)
    }
}

/// Makes an adaptive object of the script's own, at tag 0.
///
/// # Examples
///
/// `$adaptive = New-SubEthaAdaptive`
#[cmdlet(verb = "New", noun = "SubEthaAdaptive", alias = "New-SEAdaptive", output = ["SubEtha.Adaptive"])]
#[derive(Default)]
pub struct NewSubEthaAdaptive {}

impl Cmdlet for NewSubEthaAdaptive {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(Adaptive {
            inner: Arc::new(OwnInstance { header: HandshakeHeader::new(), ring: ObservationRing::new() }),
        })
    }
}

/// The process's sidecar as a whole.
#[psclass(name = "SubEtha.SidecarStatus")]
#[derive(Clone, Default)]
pub struct SidecarStatus {
    /// Objects registered with the sidecar now.
    pub instance_count: u64,
    /// The most objects it holds at once; Observe past it is refused.
    pub max_instances: u64,
    /// Its scan threads, one per NUMA node.
    pub node_count: u64,
}

/// Reports the process's sidecar: how many objects it holds, the most it
/// allows, and its scan threads.
///
/// # Examples
///
/// `Get-SubEthaSidecar`
#[cmdlet(verb = "Get", noun = "SubEthaSidecar", alias = "Get-SESidecar", output = ["SubEtha.SidecarStatus"])]
#[derive(Default)]
pub struct GetSubEthaSidecar {}

/// The process's sidecar as a `SubEtha.SidecarStatus`, as it stands now.
fn status() -> SidecarStatus {
    let sidecar = sidecar_global();
    SidecarStatus {
        instance_count: sidecar.instance_count() as u64,
        max_instances: sidecar.max_instances() as u64,
        node_count: sidecar.node_count() as u64,
    }
}

impl Cmdlet for GetSubEthaSidecar {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        ps.write(status())
    }
}

/// Sets the most objects the process's sidecar holds at once. What one
/// scan can cost grows with the number registered, so raise it for a
/// count you have measured and have room to scan. With PassThru it writes
/// the sidecar's status once the cap is set; without it, nothing.
///
/// # Examples
///
/// `Set-SubEthaSidecar -MaxInstances 20000`
#[cmdlet(verb = "Set", noun = "SubEthaSidecar", alias = "Set-SESidecar", output = ["SubEtha.SidecarStatus"])]
#[derive(Default)]
pub struct SetSubEthaSidecar {
    /// The most objects the sidecar holds at once.
    #[param(mandatory)]
    pub max_instances: u64,
    /// Write the sidecar's status once the cap is set.
    #[param]
    pub pass_thru: bool,
}

impl Cmdlet for SetSubEthaSidecar {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let cap = usize::try_from(self.max_instances)
            .map_err(|e| arg_err(format!("MaxInstances is more than this platform addresses: {e}")))?;
        sidecar_global().set_max_instances(cap);
        if self.pass_thru {
            ps.write(status())
        } else {
            Ok(())
        }
    }
}

/// Has every scan thread of the process's sidecar scan now, and waits for
/// it: an observation recorded before the call has been drained, and its
/// policy asked, when it returns. With PassThru it writes the sidecar's
/// status once the scan is done; without it, nothing.
///
/// # Examples
///
/// `Invoke-SubEthaSidecarScan`
#[cmdlet(verb = "Invoke", noun = "SubEthaSidecarScan", alias = "Invoke-SESidecarScan", output = ["SubEtha.SidecarStatus"])]
#[derive(Default)]
pub struct InvokeSubEthaSidecarScan {
    /// Write the sidecar's status once the scan is done.
    #[param]
    pub pass_thru: bool,
}

impl Cmdlet for InvokeSubEthaSidecarScan {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        sidecar_global().scan_now();
        if self.pass_thru {
            ps.write(status())
        } else {
            Ok(())
        }
    }
}
