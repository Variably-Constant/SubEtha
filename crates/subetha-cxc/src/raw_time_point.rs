//! `RawTimePointTile` - the versioned tile at a payload size chosen at
//! run time.
//!
//! [`SharedTimePointTile`](crate::shared_time_point::SharedTimePointTile)
//! is generic over what a lane carries, which a caller binding through C
//! cannot name. The file layout never depended on that type: a lane is a
//! version word and a fixed 56-byte payload, and the type only decided
//! how many of those bytes were used. This takes that count as an
//! argument.
//!
//! # What a snapshot sees
//!
//! Every lane carries the version at which its value became visible. A
//! reader holding snapshot `s` sees a lane when its version is non-zero
//! and no greater than `s`, so a writer publishing at a version above
//! every live snapshot is invisible until those readers move on. That is
//! the whole of the concurrency contract; there is no lock and no reader
//! registration.
//!
//! Version 0 means "no value here", so it is refused on insert rather
//! than stored: a lane written at version 0 would be occupied and
//! invisible to every snapshot at once, which no reader could tell from
//! an empty lane and no writer could clear.
//!
//! # Ordering
//!
//! Claiming a lane and publishing its version are separate steps. Between
//! them the lane is occupied and its version is still 0, so it is
//! invisible to every snapshot - a reader sees the value appear only when
//! the version is stored, never a half-written payload. That is why the
//! payload is written before the version and why the version store is a
//! release.

use std::path::Path;

use crate::shared_time_point::{
    tile_file_size, TileError, TileHeader, VersionedSlot, SLOT_PAYLOAD, TILE_CAP,
    TIME_POINT_MAGIC,
};

use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::sync::atomic::Ordering;

/// Why an operation on the tile could not be carried out.
#[derive(Debug)]
pub enum RawTileError {
    /// A payload slice was not the size this tile was built for.
    WrongSize { expected: usize, found: usize },
    /// The payload size asked for is zero, or above [`SLOT_PAYLOAD`].
    BadPayloadSize { asked: usize, max: usize },
    /// Version 0 means "no value", so it cannot be published.
    ZeroVersion,
    /// Every lane is taken.
    Full,
    Tile(TileError),
}

impl From<TileError> for RawTileError {
    fn from(e: TileError) -> Self {
        Self::Tile(e)
    }
}

pub struct RawTimePointTile {
    _file: std::fs::File,
    mmap: MmapMut,
    payload_size: usize,
}

impl RawTimePointTile {
    fn map(path: &Path, create: bool, payload_size: usize) -> Result<Self, RawTileError> {
        if payload_size == 0 || payload_size > SLOT_PAYLOAD {
            return Err(RawTileError::BadPayloadSize { asked: payload_size, max: SLOT_PAYLOAD });
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(path)
            .map_err(|e| RawTileError::Tile(TileError::IoError(e.kind())))?;
        file.set_len(tile_file_size() as u64)
            .map_err(|e| RawTileError::Tile(TileError::IoError(e.kind())))?;
        // SAFETY: the file is sized to the tile layout above, and every
        // access below stays inside it.
        let mmap = unsafe { MmapMut::map_mut(&file) }
            .map_err(|e| RawTileError::Tile(TileError::IoError(e.kind())))?;
        Ok(Self { _file: file, mmap, payload_size })
    }

    /// Create the tile at `path`, or attach to one already there.
    ///
    /// A tile whose header says a different payload size is refused: the
    /// two callers would disagree about how many of a lane's bytes mean
    /// anything, and neither could tell from the bytes alone.
    pub fn create(path: impl AsRef<Path>, payload_size: usize) -> Result<Self, RawTileError> {
        let tile = Self::map(path.as_ref(), true, payload_size)?;
        let header = tile.header_mut();
        if header.magic == TIME_POINT_MAGIC {
            if header.payload_size as usize != payload_size {
                return Err(RawTileError::BadPayloadSize {
                    asked: payload_size,
                    max: header.payload_size as usize,
                });
            }
        } else {
            header.magic = TIME_POINT_MAGIC;
            header.capacity = TILE_CAP as u32;
            header.payload_size = payload_size as u32;
            header.occupied.store(0, Ordering::Release);
        }
        Ok(tile)
    }

    /// Attach to a tile another process created.
    pub fn open(path: impl AsRef<Path>, payload_size: usize) -> Result<Self, RawTileError> {
        let tile = Self::map(path.as_ref(), false, payload_size)?;
        let header = tile.header();
        if header.magic != TIME_POINT_MAGIC {
            // No magic means this file was never a tile, which is the
            // same class of mistake as a shape disagreement: the bytes
            // are not what the caller believes they are.
            return Err(RawTileError::Tile(TileError::LayoutMismatch));
        }
        if header.payload_size as usize != payload_size {
            return Err(RawTileError::BadPayloadSize {
                asked: payload_size,
                max: header.payload_size as usize,
            });
        }
        Ok(tile)
    }

    fn header(&self) -> &TileHeader {
        // SAFETY: the mapping is at least tile_file_size() bytes and the
        // header sits at offset 0.
        unsafe { &*(self.mmap.as_ptr() as *const TileHeader) }
    }

    #[allow(clippy::mut_from_ref)]
    fn header_mut(&self) -> &mut TileHeader {
        // SAFETY: as above. Callers write only during create, before the
        // tile is shared.
        unsafe { &mut *(self.mmap.as_ptr() as *mut TileHeader) }
    }

    fn slot(&self, lane: usize) -> &VersionedSlot {
        let offset = size_of::<TileHeader>() + lane * size_of::<VersionedSlot>();
        // SAFETY: lane < TILE_CAP is checked by every caller, and the
        // mapping covers the header plus TILE_CAP slots.
        unsafe { &*(self.mmap.as_ptr().add(offset) as *const VersionedSlot) }
    }

    pub fn payload_size(&self) -> usize {
        self.payload_size
    }

    pub fn capacity(&self) -> usize {
        TILE_CAP
    }

    pub fn len(&self) -> usize {
        self.header().occupied.load(Ordering::Acquire).count_ones() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.header().occupied.load(Ordering::Acquire) == ((1u32 << TILE_CAP) - 1)
    }

    /// Publish `value` at `version`, answering the lane it took.
    ///
    /// The payload is written before the version, and the version with a
    /// release, so a reader that sees the version sees the whole payload.
    pub fn insert(&self, version: u64, value: &[u8]) -> Result<usize, RawTileError> {
        if value.len() != self.payload_size {
            return Err(RawTileError::WrongSize {
                expected: self.payload_size,
                found: value.len(),
            });
        }
        if version == 0 {
            return Err(RawTileError::ZeroVersion);
        }
        let header = self.header();
        loop {
            let cur = header.occupied.load(Ordering::Acquire);
            let free = !cur & ((1u32 << TILE_CAP) - 1);
            if free == 0 {
                return Err(RawTileError::Full);
            }
            let lane = free.trailing_zeros() as usize;
            if header
                .occupied
                .compare_exchange_weak(
                    cur,
                    cur | (1u32 << lane),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                let slot = self.slot(lane);
                // SAFETY: the compare-exchange above makes this lane ours
                // alone, and payload_size is bounded by SLOT_PAYLOAD.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr(),
                        slot.payload.as_ptr() as *mut u8,
                        self.payload_size,
                    );
                }
                slot.version.store(version, Ordering::Release);
                return Ok(lane);
            }
            std::hint::spin_loop();
        }
    }

    /// Free `lane`, and clear its version so a later reader cannot see the
    /// value through a stale version word.
    pub fn remove(&self, lane: usize) {
        if lane >= TILE_CAP {
            return;
        }
        // The version goes first. Clearing occupancy alone would leave a
        // lane free to be claimed while its old version still reads as
        // visible, so a snapshot could see a value the writer had not
        // published yet.
        self.slot(lane).version.store(0, Ordering::Release);
        self.header().occupied.fetch_and(!(1u32 << lane), Ordering::AcqRel);
    }

    /// The version at `lane`, and its payload into `out`. `false` where
    /// the lane holds nothing.
    pub fn at(&self, lane: usize, out: &mut [u8]) -> Result<Option<u64>, RawTileError> {
        if out.len() != self.payload_size {
            return Err(RawTileError::WrongSize {
                expected: self.payload_size,
                found: out.len(),
            });
        }
        if lane >= TILE_CAP {
            return Ok(None);
        }
        let occupied = self.header().occupied.load(Ordering::Acquire);
        if occupied & (1u32 << lane) == 0 {
            return Ok(None);
        }
        let slot = self.slot(lane);
        let version = slot.version.load(Ordering::Acquire);
        if version == 0 {
            // Claimed but not yet published.
            return Ok(None);
        }
        // SAFETY: the lane is occupied and published, and payload_size is
        // bounded by SLOT_PAYLOAD.
        unsafe {
            std::ptr::copy_nonoverlapping(
                slot.payload.as_ptr(),
                out.as_mut_ptr(),
                self.payload_size,
            );
        }
        Ok(Some(version))
    }

    /// A bit per lane visible at `snapshot`: occupied, published, and at a
    /// version no greater than the snapshot.
    pub fn visible_mask(&self, snapshot: u64) -> u16 {
        let occupied = self.header().occupied.load(Ordering::Acquire) as u16;
        if occupied == 0 {
            return 0;
        }
        let mut mask = 0u16;
        for lane in 0..TILE_CAP {
            if occupied & (1u16 << lane) == 0 {
                continue;
            }
            let version = self.slot(lane).version.load(Ordering::Acquire);
            if version != 0 && version <= snapshot {
                mask |= 1u16 << lane;
            }
        }
        mask
    }

    /// How many lanes `snapshot` sees.
    pub fn visible_count(&self, snapshot: u64) -> u32 {
        self.visible_mask(snapshot).count_ones()
    }

    /// Push the tile to disk and wait for it.
    pub fn flush(&self) -> Result<(), RawTileError> {
        self.mmap
            .flush()
            .map_err(|e| RawTileError::Tile(TileError::IoError(e.kind())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn scratch(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha_raw_tile_{name}_{}.bin", std::process::id()));
        p
    }

    fn cleanup(p: &Path) {
        match std::fs::remove_file(p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("could not clear {}: {e}", p.display()),
        }
    }

    #[test]
    fn a_value_comes_back_at_the_lane_it_took() {
        let path = scratch("basic");
        cleanup(&path);
        let t = RawTimePointTile::create(&path, 8).expect("created");
        assert_eq!(t.payload_size(), 8);
        assert_eq!(t.capacity(), TILE_CAP);
        assert!(t.is_empty());

        let lane = t.insert(5, &42u64.to_le_bytes()).expect("insert");
        assert_eq!(t.len(), 1);

        let mut out = [0u8; 8];
        assert_eq!(t.at(lane, &mut out).expect("at"), Some(5));
        assert_eq!(u64::from_le_bytes(out), 42);

        cleanup(&path);
    }

    #[test]
    fn a_snapshot_sees_only_versions_at_or_below_it() {
        let path = scratch("visibility");
        cleanup(&path);
        let t = RawTimePointTile::create(&path, 8).expect("created");

        let early = t.insert(10, &1u64.to_le_bytes()).expect("early");
        let late = t.insert(20, &2u64.to_le_bytes()).expect("late");

        // A reader from before either write sees neither.
        assert_eq!(t.visible_mask(5), 0);
        assert_eq!(t.visible_count(5), 0);

        // One from between them sees only the earlier.
        let mid = t.visible_mask(15);
        assert_eq!(mid & (1 << early), 1 << early);
        assert_eq!(mid & (1 << late), 0);
        assert_eq!(t.visible_count(15), 1);

        // The boundary is inclusive: a snapshot at a version sees it.
        assert_eq!(t.visible_mask(10) & (1 << early), 1 << early);

        // And one after both sees both.
        assert_eq!(t.visible_count(20), 2);

        cleanup(&path);
    }

    #[test]
    fn version_zero_is_refused_rather_than_stored() {
        let path = scratch("zerover");
        cleanup(&path);
        let t = RawTimePointTile::create(&path, 8).expect("created");
        // A lane at version 0 would be occupied and invisible to every
        // snapshot, which no reader could tell from an empty lane.
        assert!(matches!(t.insert(0, &[0u8; 8]), Err(RawTileError::ZeroVersion)));
        assert!(t.is_empty(), "and it claimed no lane");
        cleanup(&path);
    }

    #[test]
    fn a_removed_lane_is_invisible_even_at_a_later_snapshot() {
        let path = scratch("remove");
        cleanup(&path);
        let t = RawTimePointTile::create(&path, 8).expect("created");
        let lane = t.insert(7, &99u64.to_le_bytes()).expect("insert");
        assert_eq!(t.visible_count(100), 1);

        t.remove(lane);
        assert_eq!(t.len(), 0);
        assert_eq!(t.visible_count(100), 0, "a very late snapshot still sees nothing");
        let mut out = [0u8; 8];
        assert_eq!(t.at(lane, &mut out).expect("at"), None);

        // The lane is reusable and its old version does not leak into the
        // new occupant's visibility.
        let again = t.insert(50, &7u64.to_le_bytes()).expect("reuse");
        assert_eq!(t.visible_count(49), 0, "the new value is not visible before its version");
        assert_eq!(t.visible_count(50), 1);
        assert_eq!(again, lane, "and it took the freed lane");

        cleanup(&path);
    }

    #[test]
    fn a_full_tile_refuses_rather_than_evicting() {
        let path = scratch("full");
        cleanup(&path);
        let t = RawTimePointTile::create(&path, 8).expect("created");
        for i in 0..TILE_CAP {
            t.insert(i as u64 + 1, &(i as u64).to_le_bytes()).expect("fill");
        }
        assert!(t.is_full());
        assert!(matches!(t.insert(99, &[0u8; 8]), Err(RawTileError::Full)));
        assert_eq!(t.len(), TILE_CAP, "and nothing was displaced");
        cleanup(&path);
    }

    #[test]
    fn a_payload_of_the_wrong_size_is_refused_rather_than_padded() {
        let path = scratch("sizes");
        cleanup(&path);
        let t = RawTimePointTile::create(&path, 8).expect("created");
        assert!(matches!(
            t.insert(1, &[0u8; 4]),
            Err(RawTileError::WrongSize { expected: 8, found: 4 })
        ));
        let lane = t.insert(1, &[0u8; 8]).expect("insert");
        let mut small = [0u8; 4];
        assert!(matches!(
            t.at(lane, &mut small),
            Err(RawTileError::WrongSize { expected: 8, found: 4 })
        ));
        cleanup(&path);
    }

    #[test]
    fn a_payload_larger_than_a_lane_is_refused_at_creation() {
        let path = scratch("toobig");
        cleanup(&path);
        assert!(matches!(
            RawTimePointTile::create(&path, SLOT_PAYLOAD + 1),
            Err(RawTileError::BadPayloadSize { .. })
        ));
        assert!(matches!(
            RawTimePointTile::create(&path, 0),
            Err(RawTileError::BadPayloadSize { .. })
        ));
        // The largest a lane can hold is accepted.
        RawTimePointTile::create(&path, SLOT_PAYLOAD).expect("the full lane");
        cleanup(&path);
    }

    #[test]
    fn attaching_with_a_different_payload_size_is_refused() {
        let path = scratch("disagree");
        cleanup(&path);
        let first = RawTimePointTile::create(&path, 8).expect("created");
        first.insert(1, &1u64.to_le_bytes()).expect("insert");

        // The two callers would disagree about how many of a lane's bytes
        // mean anything, and neither could tell from the bytes.
        assert!(matches!(
            RawTimePointTile::open(&path, 16),
            Err(RawTileError::BadPayloadSize { .. })
        ));
        let same = RawTimePointTile::open(&path, 8).expect("same shape opens");
        assert_eq!(same.len(), 1);

        cleanup(&path);
    }

    #[test]
    fn a_second_handle_sees_what_the_first_published() {
        let path = scratch("shared");
        cleanup(&path);
        let a = RawTimePointTile::create(&path, 8).expect("created");
        let lane = a.insert(3, &77u64.to_le_bytes()).expect("insert");

        let b = RawTimePointTile::open(&path, 8).expect("opened");
        let mut out = [0u8; 8];
        assert_eq!(b.at(lane, &mut out).expect("at"), Some(3));
        assert_eq!(u64::from_le_bytes(out), 77);
        assert_eq!(b.visible_count(3), 1);
        assert_eq!(b.visible_count(2), 0);

        cleanup(&path);
    }
}
