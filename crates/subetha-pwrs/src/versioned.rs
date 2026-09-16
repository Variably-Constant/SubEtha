//! The structures that keep history or reach values by something other
//! than a place: the reservoir, the handle table, the time-point tile,
//! the version chain, the versioned slab and maps with their pins, the
//! topology map, the graph, the self-storing set and the tower.

use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

use pwrs::prelude::*;

use subetha_cxc::laned_versioned_map::{LaneGuard as SubethaLaneGuard, LanedError, LanedVersionedMap};
use subetha_cxc::raw_k_tower::{RawKTower, RawTowerError};
use subetha_cxc::shared_epochs::PinGuard as SubethaPinGuard;
use subetha_cxc::shared_graph::{EdgeIndex, NodeIndex, SharedGraph};
use subetha_cxc::shared_handle_table::{Handle as SubethaHandle, HandleTableError, SharedHandleTable};
use subetha_cxc::shared_reservoir_sampler::SharedReservoirSampler;
use subetha_cxc::shared_time_point::{SharedTimePointTile, TILE_CAP};
use subetha_cxc::shared_topology_map::{SharedTopologyMap, TopologyKind, DEFAULT_FAN_IN_THRESHOLD, DEFAULT_FAN_OUT_THRESHOLD};
use subetha_cxc::shared_universal::{SharedUniversal, Strategy};
use subetha_cxc::shared_versioned_chain::SharedVersionedChain;
use subetha_cxc::shared_versioned_slab::SharedVersionedSlab;
use subetha_cxc::versioned_btree_map::{VersionedBTreeMap, VersionedError};

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, open_err, out_bytes, size, LeaseValue, SlotValue, LEASE_VALUE_BYTES, SLOT_VALUE_BYTES};

assert_send!(
    Reservoir, HandleTable, TimePointTile, VersionChain, VersionedSlab, SlabPin, VersionedMap, MapPin, LanedMap, LaneClaim, LanedPin,
    TopologyMap, Graph, Universal, Tower
);

/// A fixed number of items kept from a stream of any length, each one
/// as likely to be kept as any other.
///
/// Every value offered has the same chance of being in the sample, and
/// the sample never grows past its capacity, so a stream of any size
/// costs the same memory. This is the shape for keeping an unbiased
/// handful of a firehose: a sample of requests, of errors, of anything
/// there is too much of to keep. A value is at most 52 bytes.
#[psclass(name = "SubEtha.Reservoir", mode = proxy)]
pub struct Reservoir {
    /// The file the sample lives in.
    pub path: String,
    /// How many items the sample holds at most.
    pub capacity: u64,
    /// The most a value may be, in bytes.
    pub max_value_bytes: u64,
    #[psfield(skip)]
    inner: SharedReservoirSampler<SlotValue>,
}

impl Reservoir {
    fn obtain(path: String, capacity: u64, open: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("a reservoir holds at least one item"));
        }
        let cap = size(capacity, "the capacity")?;
        let inner = if open { SharedReservoirSampler::open(&path, cap) } else { SharedReservoirSampler::create(&path, cap) }
            .map_err(|e| open_err("the reservoir", &path, e))?;
        Ok(Self { path, capacity, max_value_bytes: SLOT_VALUE_BYTES as u64, inner })
    }
}

/// The operations of a `SubEtha.Reservoir`.
#[psmethods]
impl Reservoir {
    /// Offers one value. Answers the place it was kept in, or `$null`
    /// when the sample kept what it already had there instead. Neither
    /// answer is a failure: refusing is how the sample stays unbiased.
    pub fn record(&self, value: PsObject) -> PsResult<Option<u64>> {
        let held = SlotValue::from_bytes(&bytes(&value)?)?;
        Ok(self.inner.record(held).map(|p| p as u64))
    }

    /// Offers a run of values in one call, and says how many were kept.
    pub fn record_many(&self, values: Vec<PsObject>) -> PsResult<u64> {
        let mut kept = 0;
        for value in &values {
            let held = SlotValue::from_bytes(&bytes(value)?)?;
            if self.inner.record(held).is_some() {
                kept += 1;
            }
        }
        Ok(kept)
    }

    /// Everything in the sample right now, in one call.
    pub fn snapshot(&self) -> PsResult<Vec<PsObject>> {
        self.inner.snapshot().iter().map(|held| held.to_ps()).collect()
    }

    /// How many items the sample holds right now.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.snapshot().len() as u64)
    }

    /// How many values have been offered, which is not how many are
    /// kept. The ratio of the two is what a count drawn from the sample
    /// has to be scaled by.
    pub fn total_seen(&self) -> PsResult<u64> {
        Ok(self.inner.total_seen())
    }

    /// Empties the sample and forgets how much has been offered.
    pub fn reset(&self) -> PsResult<()> {
        self.inner.reset();
        Ok(())
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }

    /// Asks for the mapping to reach the disk without waiting.
    pub fn flush_async(&self) -> PsResult<()> {
        self.inner.flush_async().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the reservoir at Path holding Capacity items, creating it
/// when the file does not exist.
///
/// # Examples
///
/// `$sample = New-SubEthaReservoir -Path C:\ipc\reservoir -Capacity 256`
#[cmdlet(verb = "New", noun = "SubEthaReservoir", alias = "New-SEReservoir", output = ["SubEtha.Reservoir"])]
#[derive(Default)]
pub struct NewSubEthaReservoir {
    /// The file the sample lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items the sample holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for NewSubEthaReservoir {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Reservoir::obtain(path, self.capacity, false)?)
    }
}

/// Attaches to the reservoir at Path, which must exist with the
/// Capacity it was made with.
///
/// # Examples
///
/// `$sample = Open-SubEthaReservoir -Path C:\ipc\reservoir -Capacity 256`
#[cmdlet(verb = "Open", noun = "SubEthaReservoir", alias = "Open-SEReservoir", output = ["SubEtha.Reservoir"])]
#[derive(Default)]
pub struct OpenSubEthaReservoir {
    /// The file the sample lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many items the sample holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaReservoir {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Reservoir::obtain(path, self.capacity, true)?)
    }
}

/// A handle crosses the boundary as a plain number, which is the whole
/// of what it is: the place in the table in its low half and the number
/// of times that place has been reused in its high half.
fn handle_from_raw(raw: u64) -> SubethaHandle {
    SubethaHandle::from_parts((raw >> 32) as u32, raw as u32)
}

/// Values reached by a handle rather than by a place, so a slot reused
/// after a value is removed cannot be mistaken for the old one.
///
/// A handle carries the place and the number of times that place has
/// been reused. Looking up a handle whose place has since been given to
/// something else answers `$null` instead of the new occupant, which is
/// the mistake a bare index invites. A value is at most 44 bytes.
#[psclass(name = "SubEtha.HandleTable", mode = proxy)]
pub struct HandleTable {
    /// The file the table lives in.
    pub path: String,
    /// How many values the table holds at most.
    pub capacity: u64,
    /// The most a value may be, in bytes.
    pub max_value_bytes: u64,
    #[psfield(skip)]
    inner: SharedHandleTable<LeaseValue>,
}

impl HandleTable {
    fn obtain(path: String, capacity: u64, open: bool, reset: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("a table holds at least one value"));
        }
        let cap = size(capacity, "the capacity")?;
        let inner = if reset {
            SharedHandleTable::reset(&path, cap)
        } else if open {
            SharedHandleTable::open(&path, cap)
        } else {
            SharedHandleTable::create(&path, cap)
        }
        .map_err(|e| open_err("the table", &path, e))?;
        Ok(Self { path, capacity, max_value_bytes: LEASE_VALUE_BYTES as u64, inner })
    }
}

/// The operations of a `SubEtha.HandleTable`.
#[psmethods]
impl HandleTable {
    /// How many values the table holds.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Puts a value in and returns its handle. A full table is an
    /// error.
    pub fn insert(&self, value: PsObject) -> PsResult<u64> {
        let held = LeaseValue::from_bytes(&bytes(&value)?)?;
        self.inner.insert(held).map(|handle| handle.raw()).map_err(|e| op_err("inserting", e))
    }

    /// Puts a run of values in and returns their handles in order,
    /// stopping at the first one that does not fit, so a short answer
    /// means the table filled.
    pub fn insert_many(&self, values: Vec<PsObject>) -> PsResult<Vec<u64>> {
        let mut handles = Vec::with_capacity(values.len());
        for value in &values {
            let held = LeaseValue::from_bytes(&bytes(value)?)?;
            match self.inner.insert(held) {
                Ok(handle) => handles.push(handle.raw()),
                Err(HandleTableError::Full) => break,
                Err(e) => return Err(op_err("inserting", e)),
            }
        }
        Ok(handles)
    }

    /// The value behind `handle`, or `$null` when it has been removed
    /// or its place given to something else.
    pub fn get(&self, handle: u64) -> PsResult<Option<PsObject>> {
        match self.inner.get(handle_from_raw(handle)) {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// Several values in one call, `$null` where a handle no longer
    /// names anything.
    pub fn get_many(&self, handles: Vec<u64>) -> PsResult<Vec<PsObject>> {
        handles
            .into_iter()
            .map(|handle| match self.inner.get(handle_from_raw(handle)) {
                Some(held) => held.to_ps(),
                None => Ok(PsObject::null()),
            })
            .collect()
    }

    /// Whether `handle` still names a value.
    pub fn contains(&self, handle: u64) -> PsResult<bool> {
        Ok(self.inner.contains(handle_from_raw(handle)))
    }

    /// Takes a value out and returns it, or `$null` when the handle no
    /// longer names anything.
    pub fn remove(&self, handle: u64) -> PsResult<Option<PsObject>> {
        match self.inner.remove(handle_from_raw(handle)) {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }

    /// Asks for the mapping to reach the disk without waiting.
    pub fn flush_async(&self) -> PsResult<()> {
        self.inner.flush_async().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the handle table at Path holding Capacity values, creating
/// it when the file does not exist; with Reset, empties it and remakes
/// it at that capacity.
///
/// # Examples
///
/// `$table = New-SubEthaHandleTable -Path C:\ipc\handletable -Capacity 256`
#[cmdlet(verb = "New", noun = "SubEthaHandleTable", alias = "New-SEHandleTable", output = ["SubEtha.HandleTable"])]
#[derive(Default)]
pub struct NewSubEthaHandleTable {
    /// The file the table lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many values the table holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// Empty the table and remake it at this capacity.
    #[param]
    pub reset: bool,
}

impl Cmdlet for NewSubEthaHandleTable {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HandleTable::obtain(path, self.capacity, false, self.reset)?)
    }
}

/// Attaches to the handle table at Path, which must exist with the
/// Capacity it was made with.
///
/// # Examples
///
/// `$table = Open-SubEthaHandleTable -Path C:\ipc\handletable -Capacity 256`
#[cmdlet(verb = "Open", noun = "SubEthaHandleTable", alias = "Open-SEHandleTable", output = ["SubEtha.HandleTable"])]
#[derive(Default)]
pub struct OpenSubEthaHandleTable {
    /// The file the table lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many values the table holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaHandleTable {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HandleTable::obtain(path, self.capacity, true, false)?)
    }
}

/// A place in a time-point tile: the version it was written at and its
/// value.
#[psclass(name = "SubEtha.TileEntry")]
#[derive(Clone, Default)]
pub struct TileEntry {
    /// The place, numbered from zero.
    pub lane: u32,
    /// The version it was written at.
    pub version: u64,
    /// The value, a `byte[]`.
    pub bytes: PsObject,
}

/// Sixteen values, each stamped with the version it was written at, so
/// a reader holding an older version sees only what existed then.
///
/// A reader takes a version number and asks which places were written
/// at or before it. Everything written after stays invisible to it, and
/// stays visible to a reader that comes later. The sixteen are compared
/// against the reader's version in a handful of vector instructions
/// rather than one at a time, which is why the tile is this size. A
/// value is at most 52 bytes.
#[psclass(name = "SubEtha.TimePointTile", mode = proxy)]
pub struct TimePointTile {
    /// The file the tile lives in.
    pub path: String,
    /// How many places the tile has.
    pub lanes: u64,
    /// The most a value may be, in bytes.
    pub max_value_bytes: u64,
    #[psfield(skip)]
    inner: SharedTimePointTile<SlotValue>,
}

impl TimePointTile {
    fn obtain(path: String, open: bool, reset: bool) -> PsResult<Self> {
        let inner = if reset {
            SharedTimePointTile::reset(&path)
        } else if open {
            SharedTimePointTile::open(&path)
        } else {
            SharedTimePointTile::create(&path)
        }
        .map_err(|e| open_err("the tile", &path, e))?;
        Ok(Self { path, lanes: TILE_CAP as u64, max_value_bytes: SLOT_VALUE_BYTES as u64, inner })
    }

    fn lane(lane: u32) -> PsResult<usize> {
        let lane = lane as usize;
        if lane >= TILE_CAP {
            return Err(arg_err(format!("a tile has {TILE_CAP} places, numbered from zero")));
        }
        Ok(lane)
    }
}

/// The operations of a `SubEtha.TimePointTile`.
#[psmethods]
impl TimePointTile {
    /// How many places are taken.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Writes a value at `version` and returns the place it went in. A
    /// full tile is an error.
    pub fn insert(&self, version: u64, value: PsObject) -> PsResult<u32> {
        let held = SlotValue::from_bytes(&bytes(&value)?)?;
        self.inner.insert(version, held).map(|lane| lane as u32).map_err(|e| op_err("inserting", e))
    }

    /// Empties one place.
    pub fn remove(&self, lane: u32) -> PsResult<()> {
        self.inner.remove(Self::lane(lane)?);
        Ok(())
    }

    /// The version and value in one place, or `$null` when it is empty.
    pub fn at(&self, lane: u32) -> PsResult<Option<TileEntry>> {
        match self.inner.at(Self::lane(lane)?) {
            Some((version, held)) => Ok(Some(TileEntry { lane, version, bytes: held.to_ps()? })),
            None => Ok(None),
        }
    }

    /// Which places a reader at `version` can see, as sixteen bits with
    /// the lowest standing for the first place.
    pub fn visible_mask(&self, version: u64) -> PsResult<u16> {
        Ok(self.inner.visible_mask(version))
    }

    /// How many places a reader at `version` can see.
    pub fn visible_count(&self, version: u64) -> PsResult<u32> {
        Ok(self.inner.visible_count(version))
    }

    /// Everything a reader at `version` can see, in one call.
    pub fn visible(&self, version: u64) -> PsResult<Vec<TileEntry>> {
        let mask = self.inner.visible_mask(version);
        let mut seen = Vec::new();
        for lane in 0..TILE_CAP {
            if mask & (1 << lane) == 0 {
                continue;
            }
            if let Some((at, held)) = self.inner.at(lane) {
                seen.push(TileEntry { lane: lane as u32, version: at, bytes: held.to_ps()? });
            }
        }
        Ok(seen)
    }

    /// Whether all sixteen places are taken.
    pub fn is_full(&self) -> PsResult<bool> {
        Ok(self.inner.is_full())
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }

    /// Asks for the mapping to reach the disk without waiting.
    pub fn flush_async(&self) -> PsResult<()> {
        self.inner.flush_async().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the time-point tile at Path, creating it when the file does
/// not exist; with Reset, empties it and remakes it.
///
/// # Examples
///
/// `$tile = New-SubEthaTimePointTile -Path C:\ipc\timepointtile`
#[cmdlet(verb = "New", noun = "SubEthaTimePointTile", alias = "New-SETimePointTile", output = ["SubEtha.TimePointTile"])]
#[derive(Default)]
pub struct NewSubEthaTimePointTile {
    /// The file the tile lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// Empty the tile and remake it.
    #[param]
    pub reset: bool,
}

impl Cmdlet for NewSubEthaTimePointTile {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(TimePointTile::obtain(path, false, self.reset)?)
    }
}

/// Attaches to the time-point tile at Path, which must exist.
///
/// # Examples
///
/// `$tile = Open-SubEthaTimePointTile -Path C:\ipc\timepointtile`
#[cmdlet(verb = "Open", noun = "SubEthaTimePointTile", alias = "Open-SETimePointTile", output = ["SubEtha.TimePointTile"])]
#[derive(Default)]
pub struct OpenSubEthaTimePointTile {
    /// The file the tile lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for OpenSubEthaTimePointTile {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(TimePointTile::obtain(path, true, false)?)
    }
}

/// A version of a value: its bytes and the version number it became
/// current at.
#[psclass(name = "SubEtha.Versioned")]
#[derive(Clone, Default)]
pub struct Versioned {
    /// The version number.
    pub version: u64,
    /// The value, a `byte[]`.
    pub bytes: PsObject,
}

/// One value's history: every version it has had, each stamped with
/// the version number it became current at.
///
/// A reader holding a version number gets the value as it stood then,
/// whatever has been written since. Writing does not overwrite; it adds
/// to the front of the chain, which is what lets an old reader keep
/// reading. The chain is bounded, so a long-lived value eventually
/// needs its old versions cleared. A value is at most 44 bytes.
#[psclass(name = "SubEtha.VersionChain", mode = proxy)]
pub struct VersionChain {
    /// The file the chain lives in.
    pub path: String,
    /// How many versions the chain holds at most.
    pub capacity: u64,
    /// The most a value may be, in bytes.
    pub max_value_bytes: u64,
    #[psfield(skip)]
    inner: SharedVersionedChain<LeaseValue>,
}

impl VersionChain {
    fn obtain(path: String, capacity: u64, open: bool, reset: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("a chain holds at least one version"));
        }
        let cap = size(capacity, "the capacity")?;
        let inner = if reset {
            SharedVersionedChain::reset(&path, cap)
        } else if open {
            SharedVersionedChain::open(&path, cap)
        } else {
            SharedVersionedChain::create(&path, cap)
        }
        .map_err(|e| open_err("the chain", &path, e))?;
        Ok(Self { path, capacity, max_value_bytes: LEASE_VALUE_BYTES as u64, inner })
    }
}

/// The operations of a `SubEtha.VersionChain`.
#[psmethods]
impl VersionChain {
    /// How many versions the chain holds.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Writes a new version. The version number must be above the one
    /// already at the front, which is what keeps the history in order.
    pub fn push(&self, version: u64, value: PsObject) -> PsResult<()> {
        let held = LeaseValue::from_bytes(&bytes(&value)?)?;
        self.inner.push(version, held).map_err(|e| op_err("writing a version", e))
    }

    /// The value as it stood at `version`, or `$null` when nothing had
    /// been written by then.
    pub fn read_at(&self, version: u64) -> PsResult<Option<PsObject>> {
        match self.inner.read_at(version) {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// The version at the front of the chain, or `$null` when nothing
    /// has been written.
    pub fn current(&self) -> PsResult<Option<Versioned>> {
        match self.inner.current() {
            Some((version, held)) => Ok(Some(Versioned { version, bytes: held.to_ps()? })),
            None => Ok(None),
        }
    }

    /// Throws away every version.
    pub fn clear(&self) -> PsResult<()> {
        self.inner.clear();
        Ok(())
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }

    /// Asks for the mapping to reach the disk without waiting.
    pub fn flush_async(&self) -> PsResult<()> {
        self.inner.flush_async().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the version chain at Path holding Capacity versions,
/// creating it when the file does not exist; with Reset, empties it and
/// remakes it at that capacity.
///
/// # Examples
///
/// `$chain = New-SubEthaVersionChain -Path C:\ipc\versionchain -Capacity 64`
#[cmdlet(verb = "New", noun = "SubEthaVersionChain", alias = "New-SEVersionChain", output = ["SubEtha.VersionChain"])]
#[derive(Default)]
pub struct NewSubEthaVersionChain {
    /// The file the chain lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many versions the chain holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// Empty the chain and remake it at this capacity.
    #[param]
    pub reset: bool,
}

impl Cmdlet for NewSubEthaVersionChain {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(VersionChain::obtain(path, self.capacity, false, self.reset)?)
    }
}

/// Attaches to the version chain at Path, which must exist with the
/// Capacity it was made with.
///
/// # Examples
///
/// `$chain = Open-SubEthaVersionChain -Path C:\ipc\versionchain -Capacity 64`
#[cmdlet(verb = "Open", noun = "SubEthaVersionChain", alias = "Open-SEVersionChain", output = ["SubEtha.VersionChain"])]
#[derive(Default)]
pub struct OpenSubEthaVersionChain {
    /// The file the chain lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many versions the chain holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaVersionChain {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(VersionChain::obtain(path, self.capacity, true, false)?)
    }
}

/// How many versions a slot of a versioned slab keeps. Past this, the
/// oldest goes when a new one is written, so a reader that has held a
/// pin through four writes to one slot may find its version gone.
const VERSIONED_SLAB_DEPTH: usize = 4;

/// One version in a slot's history: its value, the epoch it was born
/// at, and the epoch it died at, or `$null` while it is current.
#[psclass(name = "SubEtha.SlotVersion")]
#[derive(Clone, Default)]
pub struct SlotVersion {
    /// The value, a `byte[]`.
    pub bytes: PsObject,
    /// The epoch the version became current at.
    pub born: u64,
    /// The epoch it stopped being current at, or `$null` while it is.
    pub died: Option<u64>,
}

/// Numbered slots, each keeping its recent history, so a reader holding
/// an epoch sees every slot as it stood at that epoch.
///
/// Epochs here come from a shared table rather than from the caller: a
/// reader takes a pin, which fixes the epoch it reads at, and a writer
/// advances the epoch when it writes. A version stays until every pin
/// that could still see it has gone, which is what SweepSlot and
/// VoidEpoch act on. A value is at most 44 bytes.
#[psclass(name = "SubEtha.VersionedSlab", mode = proxy)]
pub struct VersionedSlab {
    /// The file the slab lives in.
    pub path: String,
    /// How many slots there are.
    pub capacity: u64,
    /// How many versions one slot keeps.
    pub depth: u64,
    /// The most a value may be, in bytes.
    pub max_value_bytes: u64,
    #[psfield(skip)]
    inner: Arc<SharedVersionedSlab<LeaseValue, VERSIONED_SLAB_DEPTH>>,
}

impl VersionedSlab {
    fn obtain(path: String, capacity: u64, epochs_path: String, max_pins: u64, open: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("a slab holds at least one slot"));
        }
        if max_pins < 1 {
            return Err(arg_err("at least one reader must be able to pin"));
        }
        let cap = size(capacity, "the capacity")?;
        let pins = size(max_pins, "the pin count")?;
        let inner = if open { SharedVersionedSlab::open(&path, cap, &epochs_path, pins) } else { SharedVersionedSlab::create(&path, cap, &epochs_path, pins) }
            .map_err(|e| open_err("the slab", &path, e))?;
        Ok(Self { path, capacity, depth: VERSIONED_SLAB_DEPTH as u64, max_value_bytes: LEASE_VALUE_BYTES as u64, inner: Arc::new(inner) })
    }
}

/// The operations of a `SubEtha.VersionedSlab`.
#[psmethods]
impl VersionedSlab {
    /// Writes `slot` at the next epoch.
    pub fn set(&self, slot: u64, value: PsObject) -> PsResult<()> {
        let held = LeaseValue::from_bytes(&bytes(&value)?)?;
        self.inner.set(size(slot, "the slot")?, held).map_err(|e| op_err("writing a slot", e))
    }

    /// Writes `slot` at the named epoch `born`, for a caller stepping
    /// the epochs itself.
    pub fn set_at(&self, slot: u64, value: PsObject, born: u64) -> PsResult<()> {
        let held = LeaseValue::from_bytes(&bytes(&value)?)?;
        self.inner.set_at(size(slot, "the slot")?, held, born).map_err(|e| op_err("writing a slot", e))
    }

    /// The value in `slot` now, or `$null` when nothing live is there.
    pub fn get(&self, slot: u64) -> PsResult<Option<PsObject>> {
        match self.inner.get(size(slot, "the slot")?).map_err(|e| op_err("reading a slot", e))? {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// A slot's history, newest first.
    pub fn history(&self, slot: u64) -> PsResult<Vec<SlotVersion>> {
        let chain = self.inner.chain(size(slot, "the slot")?).map_err(|e| op_err("reading a slot", e))?;
        chain
            .into_iter()
            .map(|version| {
                let died = if version.is_live() { None } else { Some(version.died) };
                Ok(SlotVersion { bytes: version.value.to_ps()?, born: version.born, died })
            })
            .collect()
    }

    /// Marks a slot's current value as no longer current, at the next
    /// epoch, and returns what it was. Readers pinned earlier still see
    /// it.
    pub fn retire(&self, slot: u64) -> PsResult<Option<PsObject>> {
        match self.inner.retire(size(slot, "the slot")?).map_err(|e| op_err("retiring a slot", e))? {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// As Retire, at the named epoch `died`.
    pub fn retire_at(&self, slot: u64, died: u64) -> PsResult<Option<PsObject>> {
        match self.inner.retire_at(size(slot, "the slot")?, died).map_err(|e| op_err("retiring a slot", e))? {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// Takes a pin, fixing one epoch to read the whole slab at. Release
    /// it when done, or reclaiming cannot move past it.
    pub fn pin(&self) -> PsResult<SlabPin> {
        let slab = Arc::clone(&self.inner);
        // The slab lives on the heap behind the shared handle, so its
        // address does not change for as long as any handle lives, and
        // the pin holds one; that is what makes the borrow the guard
        // takes good for the pin's whole life.
        let borrowed: &'static SharedVersionedSlab<LeaseValue, VERSIONED_SLAB_DEPTH> = unsafe { &*Arc::as_ptr(&slab) };
        let guard = borrowed.pin().map_err(|e| op_err("taking a pin", e))?;
        Ok(SlabPin { guard: Some(guard), slab })
    }

    /// Throws away every version of `slot` that nothing can still see,
    /// and returns how many went.
    pub fn sweep_slot(&self, slot: u64) -> PsResult<u64> {
        self.inner.sweep_slot(size(slot, "the slot")?).map(|n| n as u64).map_err(|e| op_err("sweeping a slot", e))
    }

    /// Undoes every write stamped at exactly `epoch`, across the whole
    /// slab, and returns how many versions were touched. This is not
    /// reclaiming: a version written at the epoch is taken away, and
    /// one that stopped being current at the epoch is current again. It
    /// is for an epoch whose writer died partway through.
    pub fn void_epoch(&self, epoch: u64) -> PsResult<u64> {
        self.inner.void_epoch(epoch).map(|n| n as u64).map_err(|e| op_err("voiding an epoch", e))
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the versioned slab at Path holding Capacity slots, with its
/// epochs at EpochsPath, creating both when the files do not exist.
///
/// # Examples
///
/// `$slab = New-SubEthaVersionedSlab -Path C:\ipc\vslab -Capacity 64 -EpochsPath C:\ipc\epochs`
#[cmdlet(verb = "New", noun = "SubEthaVersionedSlab", alias = "New-SEVersionedSlab", output = ["SubEtha.VersionedSlab"])]
#[derive(Default)]
pub struct NewSubEthaVersionedSlab {
    /// The file the slab lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots there are.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The file the epochs live in, which other structures may share.
    #[param(mandatory, position = 2)]
    pub epochs_path: String,
    /// How many readers may hold a pin at once; sixteen when absent.
    #[param]
    pub max_pins: Option<u64>,
}

impl Cmdlet for NewSubEthaVersionedSlab {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let epochs = full_path(ps, &self.epochs_path)?;
        ps.write(VersionedSlab::obtain(path, self.capacity, epochs, self.max_pins.unwrap_or(16), false)?)
    }
}

/// Attaches to the versioned slab at Path, which must exist with the
/// capacity and pin count it was made with.
///
/// # Examples
///
/// `$slab = Open-SubEthaVersionedSlab -Path C:\ipc\vslab -Capacity 64 -EpochsPath C:\ipc\epochs`
#[cmdlet(verb = "Open", noun = "SubEthaVersionedSlab", alias = "Open-SEVersionedSlab", output = ["SubEtha.VersionedSlab"])]
#[derive(Default)]
pub struct OpenSubEthaVersionedSlab {
    /// The file the slab lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots there are.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The file the epochs live in.
    #[param(mandatory, position = 2)]
    pub epochs_path: String,
    /// How many readers may hold a pin at once; sixteen when absent.
    #[param]
    pub max_pins: Option<u64>,
}

impl Cmdlet for OpenSubEthaVersionedSlab {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let epochs = full_path(ps, &self.epochs_path)?;
        ps.write(VersionedSlab::obtain(path, self.capacity, epochs, self.max_pins.unwrap_or(16), true)?)
    }
}

/// The error for a pin, claim or guard that has been given back.
fn given_back(what: &str) -> PsError {
    PsError::new(ErrorCategory::InvalidOperation, "SubEthaReleased", format!("this {what} has been given back"))
}

/// One fixed epoch to read a slab at. Everything read through it is
/// the slab as it stood when the pin was taken. A pin held open stops
/// reclaiming, so release it when done; disposing or collecting the
/// object does that too.
#[psclass(name = "SubEtha.SlabPin", mode = proxy)]
pub struct SlabPin {
    /// Declared first so it is dropped first. Giving the pin back
    /// reaches into the slab's epoch table, and the handle below is
    /// what keeps that table alive; released the other way round, the
    /// last handle could go while the guard still had to use it.
    #[psfield(skip)]
    guard: Option<SubethaPinGuard<'static>>,
    #[psfield(skip)]
    slab: Arc<SharedVersionedSlab<LeaseValue, VERSIONED_SLAB_DEPTH>>,
}

impl SlabPin {
    fn guard(&self) -> PsResult<&SubethaPinGuard<'static>> {
        self.guard.as_ref().ok_or_else(|| given_back("pin"))
    }
}

/// The operations of a `SubEtha.SlabPin`.
#[psmethods]
impl SlabPin {
    /// The epoch this pin fixed.
    pub fn epoch(&self) -> PsResult<u64> {
        Ok(self.guard()?.epoch())
    }

    /// Whether the pin is still held.
    pub fn held(&self) -> PsResult<bool> {
        Ok(self.guard.is_some())
    }

    /// The value in `slot` as it stood when the pin was taken, or
    /// `$null` when nothing was there then.
    pub fn get(&self, slot: u64) -> PsResult<Option<PsObject>> {
        let guard = self.guard()?;
        match self.slab.get_at(size(slot, "the slot")?, guard).map_err(|e| op_err("reading a slot", e))? {
            Some(held) => Ok(Some(held.to_ps()?)),
            None => Ok(None),
        }
    }

    /// Several slots in one call, `$null` where nothing was there when
    /// the pin was taken.
    pub fn get_many(&self, slots: Vec<u64>) -> PsResult<Vec<PsObject>> {
        let guard = self.guard()?;
        let mut found = Vec::with_capacity(slots.len());
        for slot in slots {
            match self.slab.get_at(size(slot, "the slot")?, guard).map_err(|e| op_err("reading a slot", e))? {
                Some(held) => found.push(held.to_ps()?),
                None => found.push(PsObject::null()),
            }
        }
        Ok(found)
    }

    /// Gives the pin back now rather than when the object goes. Calling
    /// it twice is harmless.
    pub fn release(&mut self) -> PsResult<()> {
        self.guard = None;
        Ok(())
    }
}

/// A key and what it holds.
#[psclass(name = "SubEtha.Entry")]
#[derive(Clone, Default)]
pub struct Entry {
    /// The key.
    pub key: u64,
    /// What it holds.
    pub value: u64,
}

/// The entries a scan walked and where to carry on from.
#[psclass(name = "SubEtha.Scan")]
#[derive(Clone, Default)]
pub struct Scan {
    /// The entries, in key order.
    pub entries: Vec<Entry>,
    /// The last key the walk reached, to carry on from, or `$null` when
    /// the walk ran out of entries.
    pub resume_from: Option<u64>,
}

fn entries(pairs: Vec<(u64, u64)>) -> Vec<Entry> {
    pairs.into_iter().map(|(key, value)| Entry { key, value }).collect()
}

/// A key at one end of a scan, or no bound there. Both ends are
/// inclusive, which is the reading a caller naming two keys expects.
fn bound_of(key: &Option<u64>) -> Bound<&u64> {
    match key {
        Some(key) => Bound::Included(key),
        None => Bound::Unbounded,
    }
}

/// An ordered map whose entries carry the epochs they were current
/// between, so a reader holding a pin scans one unchanging view of it
/// while writers carry on.
///
/// Keys and values are both unsigned integers here. A versioned map at
/// this level is an index, mapping an identifier to another one or to
/// an offset, and keeping both sides to a machine word is what makes a
/// scan cost what it should. Removing does not take an entry away; it
/// marks it as no longer current, so a reader pinned earlier still sees
/// it. Sweep is what takes the marked ones away once nothing can reach
/// them.
#[psclass(name = "SubEtha.VersionedMap", mode = proxy)]
pub struct VersionedMap {
    /// The file the map lives in.
    pub path: String,
    /// How many entries the map holds, counting the ones marked as no
    /// longer current.
    pub capacity: u64,
    #[psfield(skip)]
    inner: Arc<VersionedBTreeMap<u64, u64>>,
}

impl VersionedMap {
    fn obtain(path: String, capacity: u64, epochs_path: String, max_pins: u64, open: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("a map holds at least one entry"));
        }
        if max_pins < 1 {
            return Err(arg_err("at least one reader must be able to pin"));
        }
        let cap = size(capacity, "the capacity")?;
        let pins = size(max_pins, "the pin count")?;
        let inner = if open { VersionedBTreeMap::open(&path, cap, &epochs_path, pins) } else { VersionedBTreeMap::create(&path, cap, &epochs_path, pins) }
            .map_err(|e| open_err("the map", &path, e))?;
        Ok(Self { path, capacity, inner: Arc::new(inner) })
    }
}

/// The operations of a `SubEtha.VersionedMap`.
#[psmethods]
impl VersionedMap {
    /// Entries the map holds, counting the ones marked as no longer
    /// current.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Puts an entry in at the next epoch, and returns what the key
    /// held before, or `$null` when it held nothing.
    pub fn insert(&self, key: u64, value: u64) -> PsResult<Option<u64>> {
        self.inner.insert(key, value).map_err(|e| op_err("inserting", e))
    }

    /// Puts an entry in at the named epoch `born`, for a caller
    /// stepping the epochs itself.
    pub fn insert_at(&self, key: u64, value: u64, born: u64) -> PsResult<Option<u64>> {
        self.inner.insert_at(key, value, born).map_err(|e| op_err("inserting", e))
    }

    /// Puts each of `values` in under the key beside it in `keys`, in
    /// one call, and answers what each key held before, `$null` where
    /// it held nothing.
    pub fn insert_many(&self, keys: Vec<u64>, values: Vec<u64>) -> PsResult<Vec<PsObject>> {
        if keys.len() != values.len() {
            return Err(arg_err("the keys and the values must be the same length"));
        }
        let mut before = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(&values) {
            before.push(self.inner.insert(*key, *value).map_err(|e| op_err("inserting", e))?.into_ps()?);
        }
        Ok(before)
    }

    /// What `key` holds now, or `$null` when it holds nothing.
    pub fn get(&self, key: u64) -> PsResult<Option<u64>> {
        Ok(self.inner.get(&key))
    }

    /// Several keys in one call, `$null` where a key holds nothing.
    pub fn get_many(&self, keys: Vec<u64>) -> PsResult<Vec<PsObject>> {
        keys.into_iter().map(|key| self.inner.get(&key).into_ps()).collect()
    }

    /// Marks `key` as no longer current at the next epoch, and returns
    /// what it held. Readers pinned earlier still see it.
    pub fn remove(&self, key: u64) -> PsResult<Option<u64>> {
        self.inner.remove(&key).map_err(|e| op_err("removing", e))
    }

    /// As Remove, at the named epoch `died`.
    pub fn remove_at(&self, key: u64, died: u64) -> PsResult<Option<u64>> {
        self.inner.remove_at(&key, died).map_err(|e| op_err("removing", e))
    }

    /// Takes a pin, fixing one epoch to scan the whole map at. Release
    /// it when done, or reclaiming cannot move past it.
    pub fn pin(&self) -> PsResult<MapPin> {
        let map = Arc::clone(&self.inner);
        // The map lives on the heap behind the shared handle, so its
        // address does not change for as long as any handle lives, and
        // the pin holds one.
        let borrowed: &'static VersionedBTreeMap<u64, u64> = unsafe { &*Arc::as_ptr(&map) };
        let guard = borrowed.pin().map_err(|e| op_err("taking a pin", e))?;
        Ok(MapPin { guard: Some(guard), map })
    }

    /// Takes away every entry marked as no longer current that nothing
    /// can still reach, and returns how many went. Zero is an ordinary
    /// answer and means nothing could be taken: a live pin holds the
    /// horizon where it is, so a sweep run during a scan frees only
    /// what was already unreachable when that scan began.
    pub fn sweep(&self) -> PsResult<u64> {
        match self.inner.sweep() {
            Ok(freed) => Ok(freed as u64),
            // The Rust reports finding nothing to free as Full, because
            // its own caller is an insert that has run out of room. Here
            // the question is how many went, and the answer is none.
            Err(VersionedError::Full) => Ok(0),
            Err(e) => Err(op_err("sweeping", e)),
        }
    }

    /// Undoes every write stamped at exactly `epoch`, and returns how
    /// many entries were touched. This is not reclaiming: an entry
    /// written at the epoch is taken away, and one marked as no longer
    /// current at the epoch is current again. It is for an epoch whose
    /// writer died partway through.
    pub fn void_epoch(&self, epoch: u64) -> PsResult<u64> {
        self.inner.void_epoch(epoch).map(|n| n as u64).map_err(|e| op_err("voiding an epoch", e))
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the versioned map at Path holding Capacity entries, with its
/// epochs at EpochsPath, creating both when the files do not exist.
///
/// # Examples
///
/// `$map = New-SubEthaVersionedMap -Path C:\ipc\vmap -Capacity 256 -EpochsPath C:\ipc\epochs`
#[cmdlet(verb = "New", noun = "SubEthaVersionedMap", alias = "New-SEVersionedMap", output = ["SubEtha.VersionedMap"])]
#[derive(Default)]
pub struct NewSubEthaVersionedMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries the map holds, counting the ones marked as no
    /// longer current.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The file the epochs live in, which other structures may share.
    #[param(mandatory, position = 2)]
    pub epochs_path: String,
    /// How many readers may scan at once; sixteen when absent.
    #[param]
    pub max_pins: Option<u64>,
}

impl Cmdlet for NewSubEthaVersionedMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let epochs = full_path(ps, &self.epochs_path)?;
        ps.write(VersionedMap::obtain(path, self.capacity, epochs, self.max_pins.unwrap_or(16), false)?)
    }
}

/// Attaches to the versioned map at Path, which must exist with the
/// capacity and pin count it was made with.
///
/// # Examples
///
/// `$map = Open-SubEthaVersionedMap -Path C:\ipc\vmap -Capacity 256 -EpochsPath C:\ipc\epochs`
#[cmdlet(verb = "Open", noun = "SubEthaVersionedMap", alias = "Open-SEVersionedMap", output = ["SubEtha.VersionedMap"])]
#[derive(Default)]
pub struct OpenSubEthaVersionedMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries the map holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The file the epochs live in.
    #[param(mandatory, position = 2)]
    pub epochs_path: String,
    /// How many readers may scan at once; sixteen when absent.
    #[param]
    pub max_pins: Option<u64>,
}

impl Cmdlet for OpenSubEthaVersionedMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let epochs = full_path(ps, &self.epochs_path)?;
        ps.write(VersionedMap::obtain(path, self.capacity, epochs, self.max_pins.unwrap_or(16), true)?)
    }
}

/// One fixed epoch to scan a map at. Everything read through it is the
/// map as it stood when the pin was taken.
#[psclass(name = "SubEtha.MapPin", mode = proxy)]
pub struct MapPin {
    /// Declared first so it is dropped first, for the reason the slab
    /// pin gives.
    #[psfield(skip)]
    guard: Option<SubethaPinGuard<'static>>,
    #[psfield(skip)]
    map: Arc<VersionedBTreeMap<u64, u64>>,
}

impl MapPin {
    fn guard(&self) -> PsResult<&SubethaPinGuard<'static>> {
        self.guard.as_ref().ok_or_else(|| given_back("pin"))
    }
}

/// The operations of a `SubEtha.MapPin`.
#[psmethods]
impl MapPin {
    /// The epoch this pin fixed.
    pub fn epoch(&self) -> PsResult<u64> {
        Ok(self.guard()?.epoch())
    }

    /// Whether the pin is still held.
    pub fn held(&self) -> PsResult<bool> {
        Ok(self.guard.is_some())
    }

    /// What `key` held when the pin was taken, or `$null` when it held
    /// nothing then.
    pub fn get(&self, key: u64) -> PsResult<Option<u64>> {
        Ok(self.map.get_at(&key, self.guard()?))
    }

    /// Several keys in one call, `$null` where a key held nothing.
    pub fn get_many(&self, keys: Vec<u64>) -> PsResult<Vec<PsObject>> {
        let guard = self.guard()?;
        keys.into_iter().map(|key| self.map.get_at(&key, guard).into_ps()).collect()
    }

    /// The entries between `low` and `high`, in key order, at most
    /// `limit` (1024 when absent) of them, as they stood when the pin
    /// was taken. Both ends are inclusive; `$null` at either means no
    /// bound there. The limit counts entries walked rather than entries
    /// answered, so a stretch thick with entries marked as no longer
    /// current can answer fewer than the limit while more remain;
    /// ScanFrom is the form that says where to carry on from.
    pub fn scan(&self, low: Option<u64>, high: Option<u64>, limit: Option<u64>) -> PsResult<Vec<Entry>> {
        let guard = self.guard()?;
        let limit = size(limit.unwrap_or(1024), "the limit")?;
        Ok(entries(self.map.range_at(bound_of(&low), bound_of(&high), limit, guard)))
    }

    /// As Scan, and also the last key the walk reached, to carry on
    /// from.
    pub fn scan_from(&self, low: Option<u64>, high: Option<u64>, limit: Option<u64>) -> PsResult<Scan> {
        let guard = self.guard()?;
        let limit = size(limit.unwrap_or(1024), "the limit")?;
        let (pairs, resume_from) = self.map.range_at_with_cursor(bound_of(&low), bound_of(&high), limit, guard);
        Ok(Scan { entries: entries(pairs), resume_from })
    }

    /// Gives the pin back now rather than when the object goes. Calling
    /// it twice is harmless.
    pub fn release(&mut self) -> PsResult<()> {
        self.guard = None;
        Ok(())
    }
}

fn laned_err(e: LanedError) -> PsError {
    match e {
        LanedError::NoFreeLane => PsError::new(ErrorCategory::ResourceBusy, "SubEthaContended", "every lane is held by another writer".to_string()),
        LanedError::LaneBusy(lane) => PsError::new(ErrorCategory::ResourceBusy, "SubEthaContended", format!("lane {lane} is held by another writer")),
        LanedError::KeyAbsent => PsError::new(ErrorCategory::ObjectNotFound, "SubEthaKeyAbsent", "no lane holds this key".to_string()),
        LanedError::KeyInAnotherLane(lane) => {
            PsError::new(ErrorCategory::InvalidOperation, "SubEthaWrongLane", format!("this key belongs to lane {lane} and must be written through that lane"))
        }
        other => op_err("the laned map", other),
    }
}

/// The same versioned map split across several trees, so several
/// writers work at once instead of queueing behind one.
///
/// Each writer claims a lane and writes only through it. A key belongs
/// to the lane it was first written in for the rest of its life,
/// because a lane is a separate tree: removing a key through the wrong
/// lane would report a row gone that no reader has stopped seeing.
/// Writing a key through the wrong lane is an error named
/// SubEthaWrongLane, naming the right one, rather than an answer that
/// there was nothing there. Reading takes no lane: a read looks across
/// every lane, and a scan through a pin merges them in key order.
#[psclass(name = "SubEtha.LanedMap", mode = proxy)]
pub struct LanedMap {
    /// The directory the map lives in, one file per lane plus the
    /// shared epochs and the claims.
    pub directory: String,
    /// How many lanes there are.
    pub lanes: u64,
    #[psfield(skip)]
    inner: Arc<LanedVersionedMap<u64, u64>>,
}

impl LanedMap {
    fn obtain(directory: String, lanes: u64, nodes_per_lane: u64, max_pins: u64, open: bool) -> PsResult<Self> {
        if lanes < 1 {
            return Err(arg_err("a laned map has at least one lane"));
        }
        if nodes_per_lane < 1 {
            return Err(arg_err("a lane holds at least one entry"));
        }
        if max_pins < 1 {
            return Err(arg_err("at least one reader must be able to pin"));
        }
        let l = size(lanes, "the lane count")?;
        let n = size(nodes_per_lane, "the lane size")?;
        let p = size(max_pins, "the pin count")?;
        let inner = if open { LanedVersionedMap::open(&directory, l, n, p) } else { LanedVersionedMap::create(&directory, l, n, p) }.map_err(laned_err)?;
        Ok(Self { directory, lanes, inner: Arc::new(inner) })
    }

    /// The map borrowed for as long as the shared handle lives, which
    /// every holder of the borrow also holds.
    fn borrowed(&self) -> &'static LanedVersionedMap<u64, u64> {
        unsafe { &*Arc::as_ptr(&self.inner) }
    }
}

/// The operations of a `SubEtha.LanedMap`.
#[psmethods]
impl LanedMap {
    /// Entries across every lane, counting the ones marked as no
    /// longer current.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Claims a free lane to write keys that are not in the map yet. An
    /// error named SubEthaContended means every lane is held.
    pub fn claim_lane(&self) -> PsResult<LaneClaim> {
        let guard = self.borrowed().claim_lane().map_err(laned_err)?;
        Ok(LaneClaim { guard: Some(guard), map: Arc::clone(&self.inner) })
    }

    /// Claims the lane `key` already lives in, to rewrite or remove it.
    /// An error named SubEthaKeyAbsent means no lane holds the key, and
    /// SubEthaContended that its lane is held by someone else: retry
    /// rather than writing elsewhere, because elsewhere is a different
    /// tree.
    pub fn claim_lane_for(&self, key: u64) -> PsResult<LaneClaim> {
        let guard = self.borrowed().claim_lane_for(&key).map_err(laned_err)?;
        Ok(LaneClaim { guard: Some(guard), map: Arc::clone(&self.inner) })
    }

    /// Which lane holds `key`, or `$null` when no lane does.
    pub fn lane_of(&self, key: u64) -> PsResult<Option<u64>> {
        Ok(self.inner.lane_of(&key).map(|l| l as u64))
    }

    /// What `key` holds now, from whichever lane holds it, or `$null`.
    pub fn get(&self, key: u64) -> PsResult<Option<u64>> {
        Ok(self.inner.get(&key))
    }

    /// Several keys in one call, `$null` where a key holds nothing.
    pub fn get_many(&self, keys: Vec<u64>) -> PsResult<Vec<PsObject>> {
        keys.into_iter().map(|key| self.inner.get(&key).into_ps()).collect()
    }

    /// Takes a pin, fixing one epoch to scan every lane at.
    pub fn pin(&self) -> PsResult<LanedPin> {
        let guard = self.borrowed().pin().map_err(laned_err)?;
        Ok(LanedPin { guard: Some(guard), map: Arc::clone(&self.inner) })
    }

    /// How many lanes are held by a writer right now.
    pub fn held_lanes(&self) -> PsResult<u64> {
        Ok(self.inner.held_lanes() as u64)
    }

    /// Gives back the lanes of writers whose process has gone, and
    /// returns how many came back. Without this a process that died
    /// holding a lane keeps it forever.
    pub fn reap_dead_claims(&self) -> PsResult<u64> {
        Ok(self.inner.reap_dead_claims() as u64)
    }

    /// Takes away every entry marked as no longer current that nothing
    /// can still reach, across every lane. Zero means nothing could be
    /// taken.
    pub fn sweep(&self) -> PsResult<u64> {
        match self.inner.sweep() {
            Ok(freed) => Ok(freed as u64),
            Err(LanedError::Versioned(VersionedError::Full)) => Ok(0),
            Err(e) => Err(laned_err(e)),
        }
    }

    /// Undoes every write stamped at exactly `epoch` across every lane,
    /// and returns how many entries were touched. For a writer that
    /// died partway through a change spanning several lanes.
    pub fn void_epoch(&self, epoch: u64) -> PsResult<u64> {
        self.inner.void_epoch(epoch).map(|n| n as u64).map_err(laned_err)
    }

    /// Writes the mappings through to their files.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(laned_err)
    }
}

/// Obtains the laned map in Directory with Lanes lanes of NodesPerLane
/// entries, creating it when the directory does not hold one.
///
/// # Examples
///
/// `$map = New-SubEthaLanedMap -Directory C:\ipc\lanedmap -Lanes 4`
#[cmdlet(verb = "New", noun = "SubEthaLanedMap", alias = "New-SELanedMap", output = ["SubEtha.LanedMap"])]
#[derive(Default)]
pub struct NewSubEthaLanedMap {
    /// The directory the map lives in.
    #[param(mandatory, position = 0)]
    pub directory: String,
    /// How many lanes; four when absent.
    #[param]
    pub lanes: Option<u64>,
    /// How many entries each lane holds; 256 when absent.
    #[param]
    pub nodes_per_lane: Option<u64>,
    /// How many readers may scan at once across every lane; sixteen
    /// when absent.
    #[param]
    pub max_pins: Option<u64>,
}

impl Cmdlet for NewSubEthaLanedMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let directory = full_path(ps, &self.directory)?;
        ps.write(LanedMap::obtain(directory, self.lanes.unwrap_or(4), self.nodes_per_lane.unwrap_or(256), self.max_pins.unwrap_or(16), false)?)
    }
}

/// Attaches to the laned map in Directory, which must exist with the
/// lane count, lane size and pin count it was made with.
///
/// # Examples
///
/// `$map = Open-SubEthaLanedMap -Directory C:\ipc\lanedmap -Lanes 4`
#[cmdlet(verb = "Open", noun = "SubEthaLanedMap", alias = "Open-SELanedMap", output = ["SubEtha.LanedMap"])]
#[derive(Default)]
pub struct OpenSubEthaLanedMap {
    /// The directory the map lives in.
    #[param(mandatory, position = 0)]
    pub directory: String,
    /// How many lanes; four when absent.
    #[param]
    pub lanes: Option<u64>,
    /// How many entries each lane holds; 256 when absent.
    #[param]
    pub nodes_per_lane: Option<u64>,
    /// How many readers may scan at once; sixteen when absent.
    #[param]
    pub max_pins: Option<u64>,
}

impl Cmdlet for OpenSubEthaLanedMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let directory = full_path(ps, &self.directory)?;
        ps.write(LanedMap::obtain(directory, self.lanes.unwrap_or(4), self.nodes_per_lane.unwrap_or(256), self.max_pins.unwrap_or(16), true)?)
    }
}

/// A held lane of a laned map. Writes go through it, and it gives the
/// lane back when released, disposed or collected, so another writer
/// can take it.
#[psclass(name = "SubEtha.LaneClaim", mode = proxy)]
pub struct LaneClaim {
    /// Declared first so it is dropped first, for the reason the slab
    /// pin gives.
    #[psfield(skip)]
    guard: Option<SubethaLaneGuard<'static, u64, u64>>,
    #[psfield(skip)]
    map: Arc<LanedVersionedMap<u64, u64>>,
}

impl LaneClaim {
    fn guard(&self) -> PsResult<&SubethaLaneGuard<'static, u64, u64>> {
        self.guard.as_ref().ok_or_else(|| given_back("lane claim"))
    }
}

/// The operations of a `SubEtha.LaneClaim`.
#[psmethods]
impl LaneClaim {
    /// Which lane this claim holds.
    pub fn index(&self) -> PsResult<u64> {
        Ok(self.guard()?.index() as u64)
    }

    /// Whether the claim is still held.
    pub fn held(&self) -> PsResult<bool> {
        Ok(self.guard.is_some())
    }

    /// Puts an entry in this lane at the next epoch, and returns what
    /// the key held before, or `$null`.
    pub fn insert(&self, key: u64, value: u64) -> PsResult<Option<u64>> {
        self.guard()?.insert(key, value).map_err(laned_err)
    }

    /// Puts an entry in at the named epoch `born`, so every write of
    /// one change becomes visible together.
    pub fn insert_at(&self, key: u64, value: u64, born: u64) -> PsResult<Option<u64>> {
        self.guard()?.insert_at(key, value, born).map_err(laned_err)
    }

    /// Puts each of `values` in this lane under the key beside it in
    /// `keys`, in one call, and answers what each key held before.
    pub fn insert_many(&self, keys: Vec<u64>, values: Vec<u64>) -> PsResult<Vec<PsObject>> {
        if keys.len() != values.len() {
            return Err(arg_err("the keys and the values must be the same length"));
        }
        let guard = self.guard()?;
        let mut before = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(&values) {
            before.push(guard.insert(*key, *value).map_err(laned_err)?.into_ps()?);
        }
        Ok(before)
    }

    /// Marks `key` in this lane as no longer current, and returns what
    /// it held. An error named SubEthaWrongLane means the key lives in
    /// another lane.
    pub fn remove(&self, key: u64) -> PsResult<Option<u64>> {
        self.guard()?.remove(&key).map_err(laned_err)
    }

    /// As Remove, at the named epoch `died`.
    pub fn remove_at(&self, key: u64, died: u64) -> PsResult<Option<u64>> {
        self.guard()?.remove_at(&key, died).map_err(laned_err)
    }

    /// Gives the lane back now rather than when the object goes.
    /// Calling it twice is harmless.
    pub fn release(&mut self) -> PsResult<()> {
        self.guard = None;
        Ok(())
    }

    /// How many lanes the map this claim belongs to has.
    pub fn lanes(&self) -> PsResult<u64> {
        Ok(self.map.lanes() as u64)
    }
}

/// One fixed epoch to scan a laned map at, across every lane.
#[psclass(name = "SubEtha.LanedPin", mode = proxy)]
pub struct LanedPin {
    /// Declared first so it is dropped first, for the reason the slab
    /// pin gives.
    #[psfield(skip)]
    guard: Option<SubethaPinGuard<'static>>,
    #[psfield(skip)]
    map: Arc<LanedVersionedMap<u64, u64>>,
}

impl LanedPin {
    fn guard(&self) -> PsResult<&SubethaPinGuard<'static>> {
        self.guard.as_ref().ok_or_else(|| given_back("pin"))
    }
}

/// The operations of a `SubEtha.LanedPin`.
#[psmethods]
impl LanedPin {
    /// The epoch this pin fixed.
    pub fn epoch(&self) -> PsResult<u64> {
        Ok(self.guard()?.epoch())
    }

    /// Whether the pin is still held.
    pub fn held(&self) -> PsResult<bool> {
        Ok(self.guard.is_some())
    }

    /// What `key` held when the pin was taken, from whichever lane
    /// holds it, or `$null`.
    pub fn get(&self, key: u64) -> PsResult<Option<u64>> {
        Ok(self.map.get_at(&key, self.guard()?))
    }

    /// Several keys in one call, `$null` where a key held nothing.
    pub fn get_many(&self, keys: Vec<u64>) -> PsResult<Vec<PsObject>> {
        let guard = self.guard()?;
        keys.into_iter().map(|key| self.map.get_at(&key, guard).into_ps()).collect()
    }

    /// The entries between `low` and `high`, in key order, merged across
    /// every lane, as they stood when the pin was taken. Both ends are
    /// inclusive; `$null` at either means no bound there. The limit
    /// (1024 when absent) counts entries walked per lane.
    pub fn scan(&self, low: Option<u64>, high: Option<u64>, limit: Option<u64>) -> PsResult<Vec<Entry>> {
        let guard = self.guard()?;
        let limit = size(limit.unwrap_or(1024), "the limit")?;
        Ok(entries(self.map.range_at(bound_of(&low), bound_of(&high), limit, guard)))
    }

    /// As Scan, and also the last key the walk reached, to carry on
    /// from.
    pub fn scan_from(&self, low: Option<u64>, high: Option<u64>, limit: Option<u64>) -> PsResult<Scan> {
        let guard = self.guard()?;
        let limit = size(limit.unwrap_or(1024), "the limit")?;
        let (pairs, resume_from) = self.map.range_at_with_cursor(bound_of(&low), bound_of(&high), limit, guard);
        Ok(Scan { entries: entries(pairs), resume_from })
    }

    /// Gives the pin back now rather than when the object goes.
    pub fn release(&mut self) -> PsResult<()> {
        self.guard = None;
        Ok(())
    }
}

/// The shape traffic between participants takes.
#[psenum(name = "SubEtha.Topology")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Topology {
    /// Traffic between pairs.
    #[default]
    PointToPoint,
    /// One participant reaches many.
    BroadcastTree,
    /// Many reach many.
    AllToAllMesh,
}

impl Topology {
    fn from_rust(kind: TopologyKind) -> Self {
        match kind {
            TopologyKind::PointToPoint => Topology::PointToPoint,
            TopologyKind::BroadcastTree => Topology::BroadcastTree,
            TopologyKind::AllToAllMesh => Topology::AllToAllMesh,
        }
    }
}

/// A participant and how many places it reaches or is reached from.
#[psclass(name = "SubEtha.FanCount")]
#[derive(Clone, Default)]
pub struct FanCount {
    /// The participant.
    pub participant: u32,
    /// How many different places.
    pub places: u32,
}

/// Who sends to whom, counted, so the shape of the traffic can be read
/// off it rather than guessed at.
///
/// Every send is recorded as a pair of numbered participants. From
/// those counts it reports how many different places each one sends to
/// and receives from, and recommends the shape that fits: point to
/// point when the traffic is between pairs, a broadcast tree when one
/// participant reaches many, and an all-to-all mesh when many reach
/// many. The recommendation can be published so every process reads
/// the same one.
#[psclass(name = "SubEtha.TopologyMap", mode = proxy)]
pub struct TopologyMap {
    /// The file the map lives in.
    pub path: String,
    /// How many participants there are, numbered from zero.
    pub participants: u64,
    #[psfield(skip)]
    inner: SharedTopologyMap,
}

impl TopologyMap {
    fn obtain(path: String, participants: u64, fan_out: Option<u32>, fan_in: Option<u32>, open: bool, reset: bool) -> PsResult<Self> {
        if participants < 1 {
            return Err(arg_err("a topology has at least one participant"));
        }
        let n = size(participants, "the participant count")?;
        let out = fan_out.unwrap_or(DEFAULT_FAN_OUT_THRESHOLD);
        let in_ = fan_in.unwrap_or(DEFAULT_FAN_IN_THRESHOLD);
        let inner = if reset {
            SharedTopologyMap::reset(&path, n, out, in_)
        } else if open {
            SharedTopologyMap::open(&path, n)
        } else if fan_out.is_none() && fan_in.is_none() {
            SharedTopologyMap::create(&path, n)
        } else {
            SharedTopologyMap::create_with_thresholds(&path, n, out, in_)
        }
        .map_err(|e| open_err("the topology", &path, e))?;
        Ok(Self { path, participants, inner })
    }
}

/// The operations of a `SubEtha.TopologyMap`.
#[psmethods]
impl TopologyMap {
    /// Records one send and returns how many have gone that way.
    pub fn record_send(&self, sender: u32, receiver: u32) -> PsResult<u64> {
        self.inner.record_send(sender, receiver).map_err(|e| op_err("recording a send", e))
    }

    /// Records a run of sends in one call: `senders` and `receivers`
    /// side by side.
    pub fn record_many(&self, senders: Vec<u32>, receivers: Vec<u32>) -> PsResult<u64> {
        if senders.len() != receivers.len() {
            return Err(arg_err("the senders and the receivers must be the same length"));
        }
        for (sender, receiver) in senders.iter().zip(&receivers) {
            self.inner.record_send(*sender, *receiver).map_err(|e| op_err("recording a send", e))?;
        }
        Ok(senders.len() as u64)
    }

    /// How many different places `sender` sends to.
    pub fn fan_out(&self, sender: u32) -> PsResult<u32> {
        Ok(self.inner.fan_out(sender))
    }

    /// How many different places send to `receiver`.
    pub fn fan_in(&self, receiver: u32) -> PsResult<u32> {
        Ok(self.inner.fan_in(receiver))
    }

    /// The participant sending to the most places, and how many places
    /// that is.
    pub fn busiest_sender(&self) -> PsResult<FanCount> {
        let (places, participant) = self.inner.max_fan_out();
        Ok(FanCount { participant, places })
    }

    /// The participant reached from the most places, and how many
    /// places that is.
    pub fn busiest_receiver(&self) -> PsResult<FanCount> {
        let (places, participant) = self.inner.max_fan_in();
        Ok(FanCount { participant, places })
    }

    /// The shape the counts suggest. Reading this does not publish it.
    pub fn recommend(&self) -> PsResult<Topology> {
        Ok(Topology::from_rust(self.inner.recommend()))
    }

    /// Works the shape out and writes it down, so every process reads
    /// the same one, and returns what was published.
    pub fn publish_recommendation(&self) -> PsResult<Topology> {
        Ok(Topology::from_rust(self.inner.publish_recommendation()))
    }

    /// The shape last published, which may not be what the counts
    /// suggest now.
    pub fn published_recommendation(&self) -> PsResult<Topology> {
        Ok(Topology::from_rust(self.inner.read_recommendation()))
    }

    /// The participant at the center when the shape is one to many.
    pub fn broadcast_root(&self) -> PsResult<u32> {
        Ok(self.inner.broadcast_root())
    }

    /// Steps each time a recommendation is published, so a reader can
    /// tell a new one from the one it already acted on.
    pub fn recommendation_epoch(&self) -> PsResult<u64> {
        Ok(self.inner.recommendation_epoch())
    }

    /// Every send recorded so far.
    pub fn total_sends(&self) -> PsResult<u64> {
        Ok(self.inner.total_msgs())
    }
}

/// Obtains the topology map at Path for Participants, creating it when
/// the file does not exist; with Reset, empties the counts and remakes
/// it. The thresholds are how many different places a participant has
/// to reach, or be reached from, before the shape counts as one to
/// many or many to one.
///
/// # Examples
///
/// `$topology = New-SubEthaTopologyMap -Path C:\ipc\topologymap -Participants 8`
#[cmdlet(verb = "New", noun = "SubEthaTopologyMap", alias = "New-SETopologyMap", output = ["SubEtha.TopologyMap"])]
#[derive(Default)]
pub struct NewSubEthaTopologyMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many participants there are, numbered from zero.
    #[param(mandatory, position = 1)]
    pub participants: u64,
    /// Places a participant must send to before it counts as one to
    /// many.
    #[param]
    pub fan_out_threshold: Option<u32>,
    /// Places a participant must be reached from before it counts as
    /// many to one.
    #[param]
    pub fan_in_threshold: Option<u32>,
    /// Empty the counts and remake the map, throwing away what every
    /// other holder has recorded.
    #[param]
    pub reset: bool,
}

impl Cmdlet for NewSubEthaTopologyMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(TopologyMap::obtain(path, self.participants, self.fan_out_threshold, self.fan_in_threshold, false, self.reset)?)
    }
}

/// Attaches to the topology map at Path, which must exist with the
/// Participants it was made with.
///
/// # Examples
///
/// `$topology = Open-SubEthaTopologyMap -Path C:\ipc\topologymap -Participants 8`
#[cmdlet(verb = "Open", noun = "SubEthaTopologyMap", alias = "Open-SETopologyMap", output = ["SubEtha.TopologyMap"])]
#[derive(Default)]
pub struct OpenSubEthaTopologyMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many participants there are.
    #[param(mandatory, position = 1)]
    pub participants: u64,
}

impl Cmdlet for OpenSubEthaTopologyMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(TopologyMap::obtain(path, self.participants, None, None, true, false)?)
    }
}

/// One step out of a node: the edge, the node it leads to, and what the
/// edge carries.
#[psclass(name = "SubEtha.Neighbor")]
#[derive(Clone, Default)]
pub struct Neighbor {
    /// The edge's index.
    pub edge: u32,
    /// The node the edge leads to.
    pub target: u32,
    /// What the edge carries.
    pub value: u64,
}

/// A directed graph of numbered nodes and the edges between them, in
/// mapped files other processes can read.
///
/// A node and an edge each carry one unsigned integer, which is enough
/// to name a record elsewhere. Adding gives back an index, and that
/// index is how everything else refers to it. The graph lives in two
/// files beside the path given, one for the nodes and one for the
/// edges.
#[psclass(name = "SubEtha.Graph", mode = proxy)]
pub struct Graph {
    /// The path the graph's files sit beside.
    pub path: String,
    /// How many nodes it can hold.
    pub max_nodes: u64,
    /// How many edges it can hold.
    pub max_edges: u64,
    #[psfield(skip)]
    inner: SharedGraph<u64, u64>,
}

impl Graph {
    fn obtain(path: String, max_nodes: u64, max_edges: u64, open: bool) -> PsResult<Self> {
        if max_nodes < 1 || max_edges < 1 {
            return Err(arg_err("a graph holds at least one node and one edge"));
        }
        let n = size(max_nodes, "the node count")?;
        let e = size(max_edges, "the edge count")?;
        let inner = if open { SharedGraph::open(&path, n, e) } else { SharedGraph::create(&path, n, e) }.map_err(|e| open_err("the graph", &path, e))?;
        Ok(Self { path, max_nodes, max_edges, inner })
    }
}

/// The operations of a `SubEtha.Graph`.
#[psmethods]
impl Graph {
    /// Adds a node carrying `value` and returns its index.
    pub fn add_node(&self, value: u64) -> PsResult<u32> {
        self.inner.add_node(value).map(|node| node.index).map_err(|e| op_err("adding a node", e))
    }

    /// Adds a run of nodes in one call and returns their indexes.
    pub fn add_nodes(&self, values: Vec<u64>) -> PsResult<Vec<u32>> {
        let mut added = Vec::with_capacity(values.len());
        for value in values {
            added.push(self.inner.add_node(value).map(|node| node.index).map_err(|e| op_err("adding a node", e))?);
        }
        Ok(added)
    }

    /// Adds an edge from `source` to `target` carrying `value`, and
    /// returns its index.
    pub fn add_edge(&self, source: u32, target: u32, value: u64) -> PsResult<u32> {
        self.inner.add_edge(NodeIndex::new(source), NodeIndex::new(target), value).map(|edge| edge.index).map_err(|e| op_err("adding an edge", e))
    }

    /// Adds a run of edges in one call: `sources`, `targets` and
    /// `values` side by side.
    pub fn add_edges(&self, sources: Vec<u32>, targets: Vec<u32>, values: Vec<u64>) -> PsResult<Vec<u32>> {
        if sources.len() != targets.len() || sources.len() != values.len() {
            return Err(arg_err("the sources, the targets and the values must be the same length"));
        }
        let mut added = Vec::with_capacity(sources.len());
        for ((source, target), value) in sources.iter().zip(&targets).zip(&values) {
            added.push(
                self.inner
                    .add_edge(NodeIndex::new(*source), NodeIndex::new(*target), *value)
                    .map(|edge| edge.index)
                    .map_err(|e| op_err("adding an edge", e))?,
            );
        }
        Ok(added)
    }

    /// What `node` carries, or `$null` when there is no such node.
    pub fn node_value(&self, node: u32) -> PsResult<Option<u64>> {
        Ok(self.inner.node_value(NodeIndex::new(node)))
    }

    /// Everything reachable in one step from `source`.
    pub fn neighbors(&self, source: u32) -> PsResult<Vec<Neighbor>> {
        Ok(self.inner.neighbors(NodeIndex::new(source)).into_iter().map(|(edge, target, value)| Neighbor { edge: edge.index, target: target.index, value }).collect())
    }

    /// How many edges lead out of `source`, or `$null` when there is no
    /// such node.
    pub fn out_degree(&self, source: u32) -> PsResult<Option<u32>> {
        Ok(self.inner.out_degree(NodeIndex::new(source)))
    }

    /// Takes `edge` out of `source`'s edges and returns what it
    /// carried, or `$null` when that node has no such edge.
    pub fn remove_edge(&self, source: u32, edge: u32) -> PsResult<Option<u64>> {
        Ok(self.inner.remove_edge(NodeIndex::new(source), EdgeIndex::new(edge)))
    }

    /// How many nodes there are.
    pub fn node_count(&self) -> PsResult<u64> {
        Ok(self.inner.node_count() as u64)
    }

    /// How many edges there are.
    pub fn edge_count(&self) -> PsResult<u64> {
        Ok(self.inner.edge_count() as u64)
    }

    /// Writes the mappings through to their files.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }

    /// Asks for the mappings to reach the disk without waiting.
    pub fn flush_async(&self) -> PsResult<()> {
        self.inner.flush_async().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the graph beside Path holding up to MaxNodes nodes and
/// MaxEdges edges, creating it when the files do not exist.
///
/// # Examples
///
/// `$graph = New-SubEthaGraph -Path C:\ipc\graph -MaxNodes 256 -MaxEdges 1024`
#[cmdlet(verb = "New", noun = "SubEthaGraph", alias = "New-SEGraph", output = ["SubEtha.Graph"])]
#[derive(Default)]
pub struct NewSubEthaGraph {
    /// The path the graph's files sit beside.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many nodes it can hold.
    #[param(mandatory, position = 1)]
    pub max_nodes: u64,
    /// How many edges it can hold.
    #[param(mandatory, position = 2)]
    pub max_edges: u64,
}

impl Cmdlet for NewSubEthaGraph {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Graph::obtain(path, self.max_nodes, self.max_edges, false)?)
    }
}

/// Attaches to the graph beside Path, which must exist with the sizes
/// it was made with.
///
/// # Examples
///
/// `$graph = Open-SubEthaGraph -Path C:\ipc\graph -MaxNodes 256 -MaxEdges 1024`
#[cmdlet(verb = "Open", noun = "SubEthaGraph", alias = "Open-SEGraph", output = ["SubEtha.Graph"])]
#[derive(Default)]
pub struct OpenSubEthaGraph {
    /// The path the graph's files sit beside.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many nodes it can hold.
    #[param(mandatory, position = 1)]
    pub max_nodes: u64,
    /// How many edges it can hold.
    #[param(mandatory, position = 2)]
    pub max_edges: u64,
}

impl Cmdlet for OpenSubEthaGraph {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Graph::obtain(path, self.max_nodes, self.max_edges, true)?)
    }
}

/// How a self-storing set keeps itself.
#[psenum(name = "SubEtha.SetStrategy")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SetStrategy {
    /// A plain list, the fastest thing to walk when there is little to
    /// walk.
    #[default]
    List,
    /// A map, once walking costs more than hashing does.
    Map,
}

impl SetStrategy {
    fn rust(self) -> Strategy {
        match self {
            SetStrategy::List => Strategy::Vec,
            SetStrategy::Map => Strategy::Map,
        }
    }

    fn from_rust(strategy: Strategy) -> Self {
        match strategy {
            Strategy::Vec => SetStrategy::List,
            Strategy::Map => SetStrategy::Map,
        }
    }
}

/// How many inserts and lookups a set has made.
#[psclass(name = "SubEtha.OpCounts")]
#[derive(Clone, Default)]
pub struct OpCounts {
    /// How many inserts.
    pub inserts: u64,
    /// How many lookups.
    pub lookups: u64,
}

/// A set that changes how it stores itself as it grows.
///
/// A small set is a plain list, which is the fastest thing to walk when
/// there is little to walk. Past a point walking costs more than
/// hashing does, and the set moves itself to a map. Strategy says which
/// it is using now, and MigrateTo moves it by hand. The set lives in
/// files beside the path given, one per way of storing it.
#[psclass(name = "SubEtha.Universal", mode = proxy)]
pub struct Universal {
    /// The path the set's files sit beside.
    pub path: String,
    /// How many values it holds at most.
    pub capacity: u64,
    #[psfield(skip)]
    inner: SharedUniversal<u64>,
}

impl Universal {
    fn obtain(path: String, capacity: u64, open: bool, reset: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("a set holds at least one value"));
        }
        let cap = size(capacity, "the capacity")?;
        let inner = if reset {
            SharedUniversal::reset(&path, cap)
        } else if open {
            SharedUniversal::open(&path, cap)
        } else {
            SharedUniversal::create(&path, cap)
        }
        .map_err(|e| open_err("the set", &path, e))?;
        Ok(Self { path, capacity, inner })
    }
}

/// The operations of a `SubEtha.Universal`.
#[psmethods]
impl Universal {
    /// Adds `value`.
    pub fn insert(&self, value: u64) -> PsResult<()> {
        self.inner.insert(value).map_err(|e| op_err("inserting", e))
    }

    /// Adds a run of values in one call.
    pub fn insert_many(&self, values: Vec<u64>) -> PsResult<u64> {
        for value in &values {
            self.inner.insert(*value).map_err(|e| op_err("inserting", e))?;
        }
        Ok(values.len() as u64)
    }

    /// Whether `value` is in the set.
    pub fn contains(&self, value: u64) -> PsResult<bool> {
        self.inner.contains(&value).map_err(|e| op_err("looking up", e))
    }

    /// Asks about a run of values in one call.
    pub fn contains_many(&self, values: Vec<u64>) -> PsResult<Vec<bool>> {
        let mut found = Vec::with_capacity(values.len());
        for value in &values {
            found.push(self.inner.contains(value).map_err(|e| op_err("looking up", e))?);
        }
        Ok(found)
    }

    /// Everything in the set, in one call.
    pub fn snapshot(&self) -> PsResult<Vec<u64>> {
        self.inner.snapshot().map_err(|e| op_err("reading", e))
    }

    /// How many values are in it.
    pub fn count(&self) -> PsResult<u64> {
        self.inner.len().map(|n| n as u64).map_err(|e| op_err("reading", e))
    }

    /// Removes every value.
    pub fn clear(&self) -> PsResult<()> {
        self.inner.clear().map_err(|e| op_err("clearing", e))
    }

    /// How the set is stored right now.
    pub fn strategy(&self) -> PsResult<SetStrategy> {
        Ok(SetStrategy::from_rust(self.inner.strategy()))
    }

    /// Steps each time the set moves to another way of storing itself,
    /// so a holder can tell that what it read the shape of has changed.
    pub fn migrations(&self) -> PsResult<u32> {
        Ok(self.inner.strategy_version())
    }

    /// Steps only when Migrations runs past what a thirty-two bit count
    /// holds and starts again. A holder comparing what it saw before
    /// has to compare this and Migrations together.
    pub fn generation(&self) -> PsResult<u16> {
        Ok(self.inner.strategy_generation())
    }

    /// Moves the set to `strategy` by hand. Moving it to where it
    /// already is does nothing.
    pub fn migrate_to(&self, strategy: SetStrategy) -> PsResult<()> {
        self.inner.migrate_to(strategy.rust()).map_err(|e| op_err("migrating", e))
    }

    /// How many inserts and lookups have been made, which is what the
    /// set weighs when deciding to move itself.
    pub fn op_counts(&self) -> PsResult<OpCounts> {
        let (inserts, lookups) = self.inner.op_histogram();
        Ok(OpCounts { inserts, lookups })
    }
}

/// Obtains the self-storing set beside Path holding Capacity values,
/// creating it when the files do not exist; with Reset, empties it and
/// remakes it at that capacity.
///
/// # Examples
///
/// `$set = New-SubEthaUniversal -Path C:\ipc\universal -Capacity 256`
#[cmdlet(verb = "New", noun = "SubEthaUniversal", alias = "New-SEUniversal", output = ["SubEtha.Universal"])]
#[derive(Default)]
pub struct NewSubEthaUniversal {
    /// The path the set's files sit beside.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many values it holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// Empty the set and remake it at this capacity.
    #[param]
    pub reset: bool,
}

impl Cmdlet for NewSubEthaUniversal {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Universal::obtain(path, self.capacity, false, self.reset)?)
    }
}

/// Attaches to the self-storing set beside Path, which must exist with
/// the Capacity it was made with.
///
/// # Examples
///
/// `$set = Open-SubEthaUniversal -Path C:\ipc\universal -Capacity 256`
#[cmdlet(verb = "Open", noun = "SubEthaUniversal", alias = "Open-SEUniversal", output = ["SubEtha.Universal"])]
#[derive(Default)]
pub struct OpenSubEthaUniversal {
    /// The path the set's files sit beside.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many values it holds at most.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
}

impl Cmdlet for OpenSubEthaUniversal {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Universal::obtain(path, self.capacity, true, false)?)
    }
}

fn tower_err(doing: &str, e: RawTowerError) -> PsError {
    match e {
        RawTowerError::WrongSize { expected, found } => arg_err(format!("{doing}: this tower wants {expected} where {found} was given")),
        RawTowerError::ZeroDepth => arg_err(format!("{doing}: a tower has at least one level")),
        other => op_err(doing, other),
    }
}

/// Values reached by a path down through levels, where the path checks
/// itself on the way.
///
/// The bottom level holds the values. Every level above holds one link
/// per place, naming a place on the level below. So a value is reached
/// by a path of as many numbers as there are levels. Reading does not
/// simply follow the numbers it is given: at each level it checks that
/// the place named there really does point at the next number in the
/// path, and refuses at the first level where it does not, saying
/// which. That is the whole reason to use a tower rather than a bare
/// index into the bottom level: a path kept across a change that
/// rewrote a level in the middle comes back as a named refusal rather
/// than quietly resolving to whatever now sits at the end of it.
#[psclass(name = "SubEtha.Tower", mode = proxy)]
pub struct Tower {
    /// The file the bottom level lives in.
    pub path: String,
    /// How many levels, which is how many numbers a path has.
    pub depth: u64,
    /// How many bytes a value is. Every value is this size exactly.
    pub value_size: u64,
    #[psfield(skip)]
    inner: RawKTower,
}

impl Tower {
    fn obtain(path: String, capacity: u64, value_size: u64, levels: Vec<(PathBuf, usize)>, open: bool) -> PsResult<Self> {
        let cap = size(capacity, "the capacity")?;
        let vs = size(value_size, "the value size")?;
        let inner = if open { RawKTower::open(&path, cap, vs, &levels) } else { RawKTower::create(&path, cap, vs, &levels) }
            .map_err(|e| tower_err("opening the tower", e))?;
        Ok(Self { path, depth: inner.depth() as u64, value_size, inner })
    }
}

/// The operations of a `SubEtha.Tower`.
#[psmethods]
impl Tower {
    /// How many values are stored at the bottom level.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Stores a value, taking a fresh place on the top level, and
    /// returns the path that reaches it.
    pub fn append(&self, value: PsObject) -> PsResult<Vec<u32>> {
        let value = bytes(&value)?;
        let mut path = vec![0u32; self.inner.depth()];
        self.inner.append(&value, &mut path).map_err(|e| tower_err("storing a value", e))?;
        Ok(path)
    }

    /// Stores a run of values in one call and returns a path for each,
    /// as one `uint[]` per value.
    pub fn append_many(&self, values: Vec<PsObject>) -> PsResult<Vec<PsObject>> {
        let depth = self.inner.depth();
        let mut paths = Vec::with_capacity(values.len());
        for value in &values {
            let value = bytes(value)?;
            let mut path = vec![0u32; depth];
            self.inner.append(&value, &mut path).map_err(|e| tower_err("storing a value", e))?;
            paths.push(PsObject::from_slice(&path)?);
        }
        Ok(paths)
    }

    /// Stores a value under the named place `top` on the top level,
    /// rather than a fresh one, and returns the path that reaches it.
    /// The value is written first and the top link last, so another
    /// process walking down never reaches a level that does not yet
    /// name a live place below it.
    pub fn insert_at_top(&self, top: u32, value: PsObject) -> PsResult<Vec<u32>> {
        let value = bytes(&value)?;
        let mut path = vec![0u32; self.inner.depth()];
        self.inner.insert_at_top(top, &value, &mut path).map_err(|e| tower_err("storing a value", e))?;
        Ok(path)
    }

    /// The value `path` reaches. An error names the level at which the
    /// tower no longer agrees with the path.
    pub fn get(&self, path: Vec<u32>) -> PsResult<PsObject> {
        let mut out = vec![0u8; self.inner.value_size()];
        self.inner.get(&path, &mut out).map_err(|e| tower_err("reading a value", e))?;
        out_bytes(&out)
    }

    /// Several paths in one call, each a `uint[]`. A path the tower no
    /// longer agrees with stops the batch and is an error naming which
    /// of the paths it was: answering `$null` for it instead would put
    /// the one thing this class exists to report back among the
    /// ordinary results.
    pub fn get_many(&self, paths: Vec<PsObject>) -> PsResult<Vec<PsObject>> {
        let vs = self.inner.value_size();
        let mut found = Vec::with_capacity(paths.len());
        for (nth, path) in paths.iter().enumerate() {
            let path = Vec::<u32>::from_ps(path)?;
            let mut out = vec![0u8; vs];
            self.inner.get(&path, &mut out).map_err(|e| tower_err(&format!("reading path {nth}"), e))?;
            found.push(out_bytes(&out)?);
        }
        Ok(found)
    }

    /// Writes the mappings through to their files.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| tower_err("flushing", e))
    }
}

/// The levels above a tower's bottom, from `paths` and `capacities`
/// side by side, top first.
fn tower_levels(ps: &Pipeline<'_>, paths: &[String], capacities: &[u64]) -> PsResult<Vec<(PathBuf, usize)>> {
    if paths.len() != capacities.len() {
        return Err(arg_err("LevelPath and LevelCapacity must be the same length"));
    }
    let mut levels = Vec::with_capacity(paths.len());
    for (path, capacity) in paths.iter().zip(capacities) {
        levels.push((PathBuf::from(full_path(ps, path)?), size(*capacity, "a level's capacity")?));
    }
    Ok(levels)
}

/// Obtains the tower whose bottom level lives at Path holding Capacity
/// values of ValueSize bytes, with the levels above it at LevelPath
/// holding LevelCapacity places each, given top first; creating them
/// when the files do not exist. With no levels the tower is one deep,
/// which is a plain region reached by a path of one number.
///
/// # Examples
///
/// `$tower = New-SubEthaTower -Path C:\ipc\tower -Capacity 256 -ValueSize 8 -LevelPath C:\ipc\tower-top -LevelCapacity 256`
#[cmdlet(verb = "New", noun = "SubEthaTower", alias = "New-SETower", output = ["SubEtha.Tower"])]
#[derive(Default)]
pub struct NewSubEthaTower {
    /// The file the bottom level lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many values the bottom level holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many bytes a value is.
    #[param(mandatory, position = 2)]
    pub value_size: u64,
    /// The files of the levels above the bottom, top first.
    #[param]
    pub level_path: Vec<String>,
    /// How many places each level above the bottom holds, in the same
    /// order.
    #[param]
    pub level_capacity: Vec<u64>,
}

impl Cmdlet for NewSubEthaTower {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let levels = tower_levels(ps, &self.level_path, &self.level_capacity)?;
        ps.write(Tower::obtain(path, self.capacity, self.value_size, levels, false)?)
    }
}

/// Attaches to the tower at Path, which must exist with the shape it
/// was made with.
///
/// # Examples
///
/// `$tower = Open-SubEthaTower -Path C:\ipc\tower -Capacity 256 -ValueSize 8 -LevelPath C:\ipc\tower-top -LevelCapacity 256`
#[cmdlet(verb = "Open", noun = "SubEthaTower", alias = "Open-SETower", output = ["SubEtha.Tower"])]
#[derive(Default)]
pub struct OpenSubEthaTower {
    /// The file the bottom level lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many values the bottom level holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// How many bytes a value is.
    #[param(mandatory, position = 2)]
    pub value_size: u64,
    /// The files of the levels above the bottom, top first.
    #[param]
    pub level_path: Vec<String>,
    /// How many places each level above the bottom holds, in the same
    /// order.
    #[param]
    pub level_capacity: Vec<u64>,
}

impl Cmdlet for OpenSubEthaTower {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let levels = tower_levels(ps, &self.level_path, &self.level_capacity)?;
        ps.write(Tower::obtain(path, self.capacity, self.value_size, levels, true)?)
    }
}
