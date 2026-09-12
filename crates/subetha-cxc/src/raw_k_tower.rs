//! `RawKTower` - the cascade tower at a depth and leaf size chosen at run
//! time.
//!
//! [`CascadeResolverN`](crate::k_tower_cascade::CascadeResolverN) is
//! generic over both the value type and a const `DEPTH`, and C can express
//! neither. Here the depth is however many intermediate regions the caller
//! supplied, and the leaf value size is an argument.
//!
//! # The shape
//!
//! A tower of `depth` levels. The last level holds values; every level
//! above it holds one `u32` per slot, naming a slot on the level below. A
//! cascade is therefore a path of `depth` indices, one per level.
//!
//! No maximum depth is imposed. The path crosses as a caller-provided
//! slice of exactly `depth` entries, so nothing here has to guess how deep
//! a caller will go or allocate on their behalf.
//!
//! # A path validates itself
//!
//! [`get`](RawKTower::get) does not simply follow the indices it is given.
//! At every level it checks that the slot named there actually points at
//! the next index in the path, and refuses at the first level where it
//! does not. So a path kept across a change that rewrote an intermediate
//! slot reports the level that stopped agreeing, rather than resolving to
//! whatever value now sits at the end of it.
//!
//! That check is the reason to use a tower rather than a bare leaf index:
//! it costs one read per level and turns a stale path from a wrong answer
//! into a named refusal.

use std::path::{Path, PathBuf};

use crate::raw_region::RawRegion;
use crate::raw_treiber_stack::ElementLayout;
use crate::shared_region::RegionError;

/// The index that means "nothing here".
pub const RAW_TOWER_NIL: u32 = u32::MAX;

/// One intermediate slot: a `u32` naming a slot on the level below.
const LINK_BYTES: usize = 4;

/// Why an operation on the tower could not be carried out.
#[derive(Debug)]
pub enum RawTowerError {
    /// A value or path slice was not the length this tower needs.
    WrongSize { expected: usize, found: usize },
    /// A tower needs at least one level.
    ZeroDepth,
    /// The path holds [`RAW_TOWER_NIL`] at this level.
    NilAtLevel(usize),
    /// The slot named at this level does not point at the next index in
    /// the path, so the path no longer describes the tower.
    BrokenAtLevel(usize),
    Region(RegionError),
}

impl From<RegionError> for RawTowerError {
    fn from(e: RegionError) -> Self {
        Self::Region(e)
    }
}

fn link_layout() -> ElementLayout {
    ElementLayout { slot_size: LINK_BYTES, alignment: 8, tag: 0x4B54_574F_524C_4E4B }
}

fn leaf_layout(value_size: usize) -> ElementLayout {
    ElementLayout { slot_size: value_size, alignment: 8, tag: 0x4B54_574F_524C_4541 }
}

pub struct RawKTower {
    /// One per level above the leaf, top first. Empty for a depth-1 tower.
    intermediate: Vec<RawRegion>,
    leaf: RawRegion,
    value_size: usize,
}

impl RawKTower {
    /// Create a tower whose leaf holds `leaf_capacity` values of
    /// `value_size` bytes, with one intermediate level per entry in
    /// `levels`, top first.
    ///
    /// Depth is `levels.len() + 1`. Passing no levels makes a depth-1
    /// tower, which is a bare region with a one-entry path.
    pub fn create(
        leaf_path: impl AsRef<Path>,
        leaf_capacity: usize,
        value_size: usize,
        levels: &[(PathBuf, usize)],
    ) -> Result<Self, RawTowerError> {
        if value_size == 0 {
            return Err(RawTowerError::WrongSize { expected: 1, found: 0 });
        }
        let leaf = RawRegion::create(leaf_path, leaf_capacity, leaf_layout(value_size))?;
        let mut intermediate = Vec::with_capacity(levels.len());
        for (path, capacity) in levels {
            intermediate.push(RawRegion::create(path, *capacity, link_layout())?);
        }
        Ok(Self { intermediate, leaf, value_size })
    }

    /// Attach to a tower another process created, with the same shape.
    pub fn open(
        leaf_path: impl AsRef<Path>,
        leaf_capacity: usize,
        value_size: usize,
        levels: &[(PathBuf, usize)],
    ) -> Result<Self, RawTowerError> {
        if value_size == 0 {
            return Err(RawTowerError::WrongSize { expected: 1, found: 0 });
        }
        let leaf = RawRegion::open(leaf_path, leaf_capacity, leaf_layout(value_size))?;
        let mut intermediate = Vec::with_capacity(levels.len());
        for (path, capacity) in levels {
            intermediate.push(RawRegion::open(path, *capacity, link_layout())?);
        }
        Ok(Self { intermediate, leaf, value_size })
    }

    /// Levels in this tower, including the leaf. A path has this many
    /// entries.
    pub fn depth(&self) -> usize {
        self.intermediate.len() + 1
    }

    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// Values stored at the leaf.
    pub fn len(&self) -> usize {
        self.leaf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read_link(&self, level: usize, slot: u32) -> Result<u32, RawTowerError> {
        let mut buf = [0u8; LINK_BYTES];
        self.intermediate[level].get(slot, &mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn write_link(&self, level: usize, slot: u32, value: u32) -> Result<(), RawTowerError> {
        self.intermediate[level].set(slot, &value.to_le_bytes())?;
        Ok(())
    }

    /// Store `value` at the leaf and build the chain of links down to it
    /// from `top_slot` on the top level, writing the path into `path_out`.
    ///
    /// The leaf is written first and the top link last, so a reader
    /// walking from the top never reaches a level that does not yet name
    /// a live slot below it.
    pub fn insert_at_top(
        &self,
        top_slot: u32,
        value: &[u8],
        path_out: &mut [u32],
    ) -> Result<(), RawTowerError> {
        if value.len() != self.value_size {
            return Err(RawTowerError::WrongSize {
                expected: self.value_size,
                found: value.len(),
            });
        }
        let depth = self.depth();
        if path_out.len() != depth {
            return Err(RawTowerError::WrongSize { expected: depth, found: path_out.len() });
        }

        path_out[depth - 1] = self.leaf.allocate(value)?;
        // Each level from just above the leaf up to just below the top
        // takes a fresh slot naming the level below it.
        for level in (1..depth - 1).rev() {
            let below = path_out[level + 1];
            path_out[level] = self.intermediate[level].allocate(&below.to_le_bytes())?;
        }
        if depth > 1 {
            self.write_link(0, top_slot, path_out[1])?;
        }
        path_out[0] = top_slot;
        Ok(())
    }

    /// Store `value` and hang it off the next free slot on the top level.
    pub fn append(&self, value: &[u8], path_out: &mut [u32]) -> Result<(), RawTowerError> {
        let depth = self.depth();
        if depth == 1 {
            if value.len() != self.value_size {
                return Err(RawTowerError::WrongSize {
                    expected: self.value_size,
                    found: value.len(),
                });
            }
            if path_out.len() != 1 {
                return Err(RawTowerError::WrongSize { expected: 1, found: path_out.len() });
            }
            path_out[0] = self.leaf.allocate(value)?;
            return Ok(());
        }
        // A fresh top slot, so appends do not overwrite each other's
        // links. Allocating it rather than counting means a top level
        // with freed slots reuses them.
        let top = self.intermediate[0].allocate(&RAW_TOWER_NIL.to_le_bytes())?;
        self.insert_at_top(top, value, path_out)
    }

    /// Read the value `path` names into `out`, checking every level.
    ///
    /// Refuses with the level that failed rather than resolving a path
    /// the tower no longer agrees with.
    pub fn get(&self, path: &[u32], out: &mut [u8]) -> Result<(), RawTowerError> {
        let depth = self.depth();
        if path.len() != depth {
            return Err(RawTowerError::WrongSize { expected: depth, found: path.len() });
        }
        if out.len() != self.value_size {
            return Err(RawTowerError::WrongSize {
                expected: self.value_size,
                found: out.len(),
            });
        }
        for (level, index) in path.iter().enumerate() {
            if *index == RAW_TOWER_NIL {
                return Err(RawTowerError::NilAtLevel(level));
            }
        }
        for level in 0..depth - 1 {
            let stored = self.read_link(level, path[level])?;
            if stored != path[level + 1] {
                // The tower and the path disagree from here down. The
                // level is named so a caller can tell a rewritten link
                // from a freed leaf.
                return Err(RawTowerError::BrokenAtLevel(level + 1));
            }
        }
        self.leaf.get(path[depth - 1], out)?;
        Ok(())
    }

    /// Push every region to disk and wait for them.
    pub fn flush(&self) -> Result<(), RawTowerError> {
        self.leaf.flush()?;
        for region in &self.intermediate {
            region.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Paths {
        leaf: PathBuf,
        levels: Vec<(PathBuf, usize)>,
    }

    fn scratch(name: &str, n_levels: usize) -> Paths {
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        let mut leaf = dir.clone();
        leaf.push(format!("subetha_raw_tower_{name}_{pid}_leaf.bin"));
        let levels = (0..n_levels)
            .map(|i| {
                let mut p = dir.clone();
                p.push(format!("subetha_raw_tower_{name}_{pid}_l{i}.bin"));
                (p, 32usize)
            })
            .collect();
        Paths { leaf, levels }
    }

    fn cleanup(p: &Paths) {
        let mut all = vec![p.leaf.clone()];
        all.extend(p.levels.iter().map(|(path, _)| path.clone()));
        for path in all {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("could not clear {}: {e}", path.display()),
            }
        }
    }

    #[test]
    fn a_value_comes_back_through_the_path_that_stored_it() {
        let p = scratch("basic", 2);
        cleanup(&p);
        let t = RawKTower::create(&p.leaf, 32, 8, &p.levels).expect("created");
        assert_eq!(t.depth(), 3, "two intermediate levels plus the leaf");
        assert_eq!(t.value_size(), 8);

        let mut path = vec![0u32; t.depth()];
        t.append(&1234u64.to_le_bytes(), &mut path).expect("append");
        assert_eq!(t.len(), 1);

        let mut out = [0u8; 8];
        t.get(&path, &mut out).expect("get");
        assert_eq!(u64::from_le_bytes(out), 1234);

        cleanup(&p);
    }

    #[test]
    fn a_depth_one_tower_is_a_bare_region_with_a_one_entry_path() {
        let p = scratch("flat", 0);
        cleanup(&p);
        let t = RawKTower::create(&p.leaf, 8, 8, &p.levels).expect("created");
        assert_eq!(t.depth(), 1);

        let mut path = vec![0u32; 1];
        t.append(&7u64.to_le_bytes(), &mut path).expect("append");
        let mut out = [0u8; 8];
        t.get(&path, &mut out).expect("get");
        assert_eq!(u64::from_le_bytes(out), 7);

        cleanup(&p);
    }

    #[test]
    fn several_values_keep_their_own_paths() {
        let p = scratch("several", 2);
        cleanup(&p);
        let t = RawKTower::create(&p.leaf, 32, 8, &p.levels).expect("created");

        let mut paths = Vec::new();
        for i in 0u64..5 {
            let mut path = vec![0u32; t.depth()];
            t.append(&i.to_le_bytes(), &mut path).expect("append");
            paths.push(path);
        }
        assert_eq!(t.len(), 5);

        let mut out = [0u8; 8];
        for (i, path) in paths.iter().enumerate() {
            t.get(path, &mut out).expect("get");
            assert_eq!(u64::from_le_bytes(out), i as u64, "path {i} still names its own value");
        }

        cleanup(&p);
    }

    #[test]
    fn a_path_whose_link_was_rewritten_is_refused_at_that_level() {
        let p = scratch("stale", 2);
        cleanup(&p);
        let t = RawKTower::create(&p.leaf, 32, 8, &p.levels).expect("created");

        let mut first = vec![0u32; t.depth()];
        t.append(&111u64.to_le_bytes(), &mut first).expect("first");
        let mut second = vec![0u32; t.depth()];
        t.append(&222u64.to_le_bytes(), &mut second).expect("second");

        // Point the first path's top slot at the second path's level-1
        // slot. The old path now describes a route the tower does not
        // have; without the check it would resolve to 222.
        t.write_link(0, first[0], second[1]).expect("rewrite");

        let mut out = [0u8; 8];
        match t.get(&first, &mut out) {
            Err(RawTowerError::BrokenAtLevel(1)) => {}
            other => panic!("expected a refusal naming level 1, got {other:?}"),
        }
        assert_eq!(out, [0u8; 8], "and it wrote nothing");

        // The second path is untouched and still resolves.
        t.get(&second, &mut out).expect("second still good");
        assert_eq!(u64::from_le_bytes(out), 222);

        cleanup(&p);
    }

    #[test]
    fn a_nil_anywhere_in_a_path_is_refused_naming_its_level() {
        let p = scratch("nil", 2);
        cleanup(&p);
        let t = RawKTower::create(&p.leaf, 32, 8, &p.levels).expect("created");
        let mut path = vec![0u32; t.depth()];
        t.append(&5u64.to_le_bytes(), &mut path).expect("append");

        let mut out = [0u8; 8];
        for level in 0..t.depth() {
            let mut broken = path.clone();
            broken[level] = RAW_TOWER_NIL;
            match t.get(&broken, &mut out) {
                Err(RawTowerError::NilAtLevel(l)) if l == level => {}
                other => panic!("level {level}: expected NilAtLevel({level}), got {other:?}"),
            }
        }

        cleanup(&p);
    }

    #[test]
    fn a_path_of_the_wrong_length_is_refused_rather_than_walked_partly() {
        let p = scratch("pathlen", 2);
        cleanup(&p);
        let t = RawKTower::create(&p.leaf, 32, 8, &p.levels).expect("created");
        let mut out = [0u8; 8];
        // Two entries where the tower has three levels: walking it would
        // read one level and call the second index a leaf.
        assert!(matches!(
            t.get(&[0, 0], &mut out),
            Err(RawTowerError::WrongSize { expected: 3, found: 2 })
        ));
        let mut short = vec![0u32; 2];
        assert!(matches!(
            t.append(&[0u8; 8], &mut short),
            Err(RawTowerError::WrongSize { expected: 3, found: 2 })
        ));
        cleanup(&p);
    }

    #[test]
    fn a_value_of_the_wrong_size_is_refused_rather_than_padded() {
        let p = scratch("sizes", 1);
        cleanup(&p);
        let t = RawKTower::create(&p.leaf, 32, 8, &p.levels).expect("created");
        let mut path = vec![0u32; t.depth()];
        assert!(matches!(
            t.append(&[0u8; 4], &mut path),
            Err(RawTowerError::WrongSize { expected: 8, found: 4 })
        ));
        t.append(&[0u8; 8], &mut path).expect("right size");
        let mut small = [0u8; 4];
        assert!(matches!(
            t.get(&path, &mut small),
            Err(RawTowerError::WrongSize { expected: 8, found: 4 })
        ));
        cleanup(&p);
    }

    #[test]
    fn a_second_handle_walks_the_same_paths() {
        let p = scratch("shared", 2);
        cleanup(&p);
        let a = RawKTower::create(&p.leaf, 32, 8, &p.levels).expect("created");
        let mut path = vec![0u32; a.depth()];
        a.append(&909u64.to_le_bytes(), &mut path).expect("append");

        let b = RawKTower::open(&p.leaf, 32, 8, &p.levels).expect("opened");
        assert_eq!(b.depth(), a.depth());
        let mut out = [0u8; 8];
        b.get(&path, &mut out).expect("the path crosses processes");
        assert_eq!(u64::from_le_bytes(out), 909);

        cleanup(&p);
    }
}
