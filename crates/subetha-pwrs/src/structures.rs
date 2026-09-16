//! The plain containers at declared sizes: the string arena, the linked
//! list, the slab, the ordered map and the hash map, and the pooled
//! rings for many producers.

use pwrs::prelude::*;

use subetha_cxc::mpmc_ring::{MpmcConsumer as SubethaMpmcConsumer, MpmcProducer as SubethaMpmcProducer, SharedRingMpmc};
use subetha_cxc::mpsc_ring::{MpscConsumer as SubethaMpscConsumer, MpscProducer as SubethaMpscProducer, SharedRingMpsc};
use subetha_cxc::raw_btree_map::RawBTreeMap;
use subetha_cxc::raw_hash_map::RawHashMap;
use subetha_cxc::raw_linked_list::RawLinkedList;
use subetha_cxc::raw_slab::RawSlab;
use subetha_cxc::shared_btree_map::BTreeError;
use subetha_cxc::shared_hash_map::{InsertOutcome, MapError};
use subetha_cxc::shared_ring::RingError;
use subetha_cxc::shared_string_arena::{ArenaError, SharedStringArena, StringRef};
use subetha_cxc::spsc_ring::SPSC_PAYLOAD_BYTES;

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, open_err, out_bytes, size};
use crate::primitives::layout;

assert_send!(Arena, LinkedList, Slab, BTreeMap, HashMap, MpscProducer, MpscConsumer, MpmcProducer, MpmcConsumer);

/// An append-only arena of interned strings in a mapped file.
///
/// Interning returns a 64-bit reference that every process resolves the
/// same way, so a string crosses between processes as eight bytes
/// rather than as its own bytes each time. Nothing is ever moved or
/// freed individually.
#[psclass(name = "SubEtha.Arena", mode = proxy)]
pub struct Arena {
    /// The file the arena lives in.
    pub path: String,
    /// The bytes the arena holds in all.
    pub capacity_bytes: u64,
    /// Whether this handle may intern.
    pub writable: bool,
    #[psfield(skip)]
    inner: SharedStringArena,
}

/// How an arena is opened.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ArenaAccess {
    /// Interning and resolving.
    ReadWrite,
    /// Resolving only.
    ReadOnly,
}

impl Arena {
    fn obtain(path: String, capacity_bytes: u64, access: ArenaAccess, open: bool) -> PsResult<Self> {
        let cap = size(capacity_bytes, "the capacity")?;
        let inner = match (open, access) {
            (false, _) => SharedStringArena::create(&path, cap),
            (true, ArenaAccess::ReadWrite) => SharedStringArena::open(&path, cap),
            (true, ArenaAccess::ReadOnly) => SharedStringArena::open_read_only(&path, cap),
        }
        .map_err(|e| open_err("the arena", &path, e))?;
        let writable = inner.is_writable();
        Ok(Self { path, capacity_bytes, writable, inner })
    }
}

/// The operations of a `SubEtha.Arena`.
#[psmethods]
impl Arena {
    /// The bytes interned so far.
    pub fn used_bytes(&self) -> PsResult<u64> {
        Ok(self.inner.used_bytes() as u64)
    }

    /// The bytes still free.
    pub fn remaining_bytes(&self) -> PsResult<u64> {
        Ok(self.inner.remaining_bytes() as u64)
    }

    /// Interns a string and returns the reference that names it, or
    /// `$null` when the arena is full.
    pub fn intern(&self, value: String) -> PsResult<Option<u64>> {
        match self.inner.intern(&value) {
            Ok(r) => Ok(Some(r.to_u64())),
            Err(ArenaError::Full) => Ok(None),
            Err(e) => Err(op_err("interning", e)),
        }
    }

    /// Interns bytes that need not be text.
    pub fn intern_bytes(&self, value: PsObject) -> PsResult<Option<u64>> {
        let value = bytes(&value)?;
        match self.inner.intern_bytes(&value) {
            Ok(r) => Ok(Some(r.to_u64())),
            Err(ArenaError::Full) => Ok(None),
            Err(e) => Err(op_err("interning", e)),
        }
    }

    /// Interns a run of strings in one call, stopping at the first the
    /// arena cannot take, and returns the references it managed.
    pub fn intern_many(&self, values: Vec<String>) -> PsResult<Vec<u64>> {
        let mut refs = Vec::with_capacity(values.len());
        for value in &values {
            match self.inner.intern(value) {
                Ok(r) => refs.push(r.to_u64()),
                Err(ArenaError::Full) => break,
                Err(e) => return Err(op_err("interning", e)),
            }
        }
        Ok(refs)
    }

    /// The string a reference names.
    pub fn get(&self, reference: u64) -> PsResult<String> {
        self.inner.get(StringRef::from_u64(reference)).map(|s| s.to_owned()).map_err(|e| op_err("resolving", e))
    }

    /// The bytes a reference names, for anything that is not text.
    pub fn get_bytes(&self, reference: u64) -> PsResult<PsObject> {
        let found = self.inner.get_bytes(StringRef::from_u64(reference)).map_err(|e| op_err("resolving", e))?;
        out_bytes(found)
    }

    /// The strings a run of references name, in one call.
    pub fn get_many(&self, references: Vec<u64>) -> PsResult<Vec<String>> {
        let mut out = Vec::with_capacity(references.len());
        for reference in references {
            out.push(self.inner.get(StringRef::from_u64(reference)).map(|s| s.to_owned()).map_err(|e| op_err("resolving", e))?);
        }
        Ok(out)
    }
}

/// Obtains the arena at Path holding CapacityBytes, creating it when the
/// file does not exist.
///
/// # Examples
///
/// `$arena = New-SubEthaArena -Path C:\ipc\arena -CapacityBytes 4096`
#[cmdlet(verb = "New", noun = "SubEthaArena", alias = "New-SEArena", output = ["SubEtha.Arena"])]
#[derive(Default)]
pub struct NewSubEthaArena {
    /// The file the arena lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes the arena holds in all.
    #[param(mandatory, position = 1)]
    pub capacity_bytes: u64,
}

impl Cmdlet for NewSubEthaArena {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Arena::obtain(path, self.capacity_bytes, ArenaAccess::ReadWrite, false)?)
    }
}

/// Attaches to the arena at Path, which must exist with the
/// CapacityBytes it was created with; ReadOnly opens it to resolve
/// without interning.
///
/// # Examples
///
/// `$arena = Open-SubEthaArena -Path C:\ipc\arena -CapacityBytes 4096 -ReadOnly`
#[cmdlet(verb = "Open", noun = "SubEthaArena", alias = "Open-SEArena", output = ["SubEtha.Arena"])]
#[derive(Default)]
pub struct OpenSubEthaArena {
    /// The file the arena lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes the arena holds in all.
    #[param(mandatory, position = 1)]
    pub capacity_bytes: u64,
    /// Resolve only, without interning.
    #[param]
    pub read_only: bool,
}

impl Cmdlet for OpenSubEthaArena {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let access = if self.read_only { ArenaAccess::ReadOnly } else { ArenaAccess::ReadWrite };
        ps.write(Arena::obtain(path, self.capacity_bytes, access, true)?)
    }
}

/// A doubly linked list of equal-sized elements in a mapped file, where
/// a node is named by an index that stays valid until it is removed.
#[psclass(name = "SubEtha.LinkedList", mode = proxy)]
pub struct LinkedList {
    /// The file the list lives in.
    pub path: String,
    /// How many nodes it can hold.
    pub capacity: u64,
    /// The bytes one element holds.
    pub element_size: u64,
    #[psfield(skip)]
    inner: RawLinkedList,
}

impl LinkedList {
    fn obtain(path: String, capacity: u64, element_size: u64, alignment: Option<u64>, tag: Option<u64>, open: bool) -> PsResult<Self> {
        let layout = layout(element_size, alignment, tag)?;
        let slots = size(capacity, "the capacity")?;
        let inner = if open { RawLinkedList::open(&path, slots, layout) } else { RawLinkedList::create(&path, slots, layout) }
            .map_err(|e| open_err("the list", &path, e))?;
        Ok(Self { path, capacity, element_size, inner })
    }

    fn take(&self, front: bool) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        let taken = if front { self.inner.pop_front(&mut out) } else { self.inner.pop_back(&mut out) };
        match taken {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("taking from the list", e)),
        }
    }
}

/// The operations of a `SubEtha.LinkedList`.
#[psmethods]
impl LinkedList {
    /// How many nodes are in it.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Adds at the front and returns the index of the node holding it.
    pub fn push_front(&self, value: PsObject) -> PsResult<u32> {
        let value = bytes(&value)?;
        self.inner.push_front(&value).map_err(|e| op_err("adding at the front", e))
    }

    /// Adds at the back and returns the index of the node holding it.
    pub fn push_back(&self, value: PsObject) -> PsResult<u32> {
        let value = bytes(&value)?;
        self.inner.push_back(&value).map_err(|e| op_err("adding at the back", e))
    }

    /// Adds a run of elements at the back in one call and returns their
    /// indexes.
    pub fn push_back_many(&self, values: Vec<PsObject>) -> PsResult<Vec<u32>> {
        let mut indexes = Vec::with_capacity(values.len());
        for value in &values {
            let value = bytes(value)?;
            indexes.push(self.inner.push_back(&value).map_err(|e| op_err("adding at the back", e))?);
        }
        Ok(indexes)
    }

    /// Takes from the front, or `$null` when the list is empty.
    pub fn pop_front(&self) -> PsResult<Option<PsObject>> {
        self.take(true)
    }

    /// Takes from the back, or `$null` when the list is empty.
    pub fn pop_back(&self) -> PsResult<Option<PsObject>> {
        self.take(false)
    }

    /// The element in the node at `index`.
    pub fn get(&self, index: u32) -> PsResult<PsObject> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner.get(index, &mut out).map_err(|e| op_err("reading a node", e))?;
        out_bytes(&out)
    }

    /// Unlinks the node at `index` and returns what it held.
    pub fn remove(&self, index: u32) -> PsResult<PsObject> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner.remove(index, &mut out).map_err(|e| op_err("removing a node", e))?;
        out_bytes(&out)
    }
}

/// Obtains the list at Path holding up to Capacity elements of
/// ElementSize bytes, creating it when the file does not exist.
///
/// # Examples
///
/// `$list = New-SubEthaLinkedList -Path C:\ipc\linkedlist -Capacity 64 -ElementSize 8`
#[cmdlet(verb = "New", noun = "SubEthaLinkedList", alias = "New-SELinkedList", output = ["SubEtha.LinkedList"])]
#[derive(Default)]
pub struct NewSubEthaLinkedList {
    /// The file the list lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many nodes it can hold.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one element holds.
    #[param(mandatory, position = 2)]
    pub element_size: u64,
    /// The alignment of each element, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for NewSubEthaLinkedList {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LinkedList::obtain(path, self.capacity, self.element_size, self.alignment, self.tag, false)?)
    }
}

/// Attaches to the list at Path, which must exist with the capacity and
/// layout it was created with.
///
/// # Examples
///
/// `$list = Open-SubEthaLinkedList -Path C:\ipc\linkedlist -Capacity 64 -ElementSize 8`
#[cmdlet(verb = "Open", noun = "SubEthaLinkedList", alias = "Open-SELinkedList", output = ["SubEtha.LinkedList"])]
#[derive(Default)]
pub struct OpenSubEthaLinkedList {
    /// The file the list lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many nodes it can hold.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one element holds.
    #[param(mandatory, position = 2)]
    pub element_size: u64,
    /// The alignment of each element, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for OpenSubEthaLinkedList {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LinkedList::obtain(path, self.capacity, self.element_size, self.alignment, self.tag, true)?)
    }
}

/// A fixed-capacity array of equal-sized slots addressed by index, in a
/// mapped file. Each slot is under a seqlock, so ReadRange is the bulk
/// path; SlotVersion is the seqlock's own counter, which a reader can
/// use to tell a change from a repeat.
#[psclass(name = "SubEtha.Slab", mode = proxy)]
pub struct Slab {
    /// The file the slab lives in.
    pub path: String,
    /// How many slots it holds.
    pub capacity: u64,
    /// The bytes one slot holds.
    pub element_size: u64,
    /// Whether this handle may write.
    pub writable: bool,
    #[psfield(skip)]
    inner: RawSlab,
}

impl Slab {
    fn obtain(path: String, capacity: u64, element_size: u64, alignment: Option<u64>, tag: Option<u64>, open: bool, read_only: bool) -> PsResult<Self> {
        let layout = layout(element_size, alignment, tag)?;
        let slots = size(capacity, "the capacity")?;
        let inner = match (open, read_only) {
            (false, _) => RawSlab::create(&path, slots, layout),
            (true, false) => RawSlab::open(&path, slots, layout),
            (true, true) => RawSlab::open_read_only(&path, slots, layout),
        }
        .map_err(|e| open_err("the slab", &path, e))?;
        let writable = inner.is_writable();
        Ok(Self { path, capacity, element_size, writable, inner })
    }
}

/// The operations of a `SubEtha.Slab`.
#[psmethods]
impl Slab {
    /// The bytes of slot `index`.
    pub fn get(&self, index: u64) -> PsResult<PsObject> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner.get(size(index, "the index")?, &mut out).map_err(|e| op_err("reading a slot", e))?;
        out_bytes(&out)
    }

    /// Writes slot `index`.
    pub fn set(&self, index: u64, value: PsObject) -> PsResult<()> {
        let value = bytes(&value)?;
        self.inner.set(size(index, "the index")?, &value).map_err(|e| op_err("writing a slot", e))
    }

    /// The seqlock counter for a slot. Even means nobody is writing;
    /// the same value twice means no write landed between the two
    /// reads.
    pub fn slot_version(&self, index: u64) -> PsResult<u32> {
        self.inner.slot_version(size(index, "the index")?).map_err(|e| op_err("reading a slot version", e))
    }

    /// `count` slots from `start`, packed end to end in one `byte[]`,
    /// each read through its seqlock.
    pub fn read_range(&self, start: u64, count: u64) -> PsResult<PsObject> {
        let element = self.inner.layout().slot_size;
        let start = size(start, "the start")?;
        let count = size(count, "the count")?;
        let mut packed = vec![0u8; count.saturating_mul(element)];
        for i in 0..count {
            let at = i * element;
            self.inner.get(start + i, &mut packed[at..at + element]).map_err(|e| op_err("reading a range", e))?;
        }
        out_bytes(&packed)
    }

    /// Writes slots packed end to end in `data` from `start`, and
    /// returns how many were written.
    pub fn write_range(&self, start: u64, data: PsObject) -> PsResult<u64> {
        let element = self.inner.layout().slot_size;
        let data = bytes(&data)?;
        if element == 0 || data.len() % element != 0 {
            return Err(arg_err("the data must be a whole number of elements"));
        }
        let start = size(start, "the start")?;
        for (i, chunk) in data.chunks(element).enumerate() {
            self.inner.set(start + i, chunk).map_err(|e| op_err("writing a range", e))?;
        }
        Ok((data.len() / element) as u64)
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the slab at Path holding Capacity slots of ElementSize bytes,
/// creating it when the file does not exist.
///
/// # Examples
///
/// `$slab = New-SubEthaSlab -Path C:\ipc\slab -Capacity 64 -ElementSize 16`
#[cmdlet(verb = "New", noun = "SubEthaSlab", alias = "New-SESlab", output = ["SubEtha.Slab"])]
#[derive(Default)]
pub struct NewSubEthaSlab {
    /// The file the slab lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one slot holds.
    #[param(mandatory, position = 2)]
    pub element_size: u64,
    /// The alignment of each slot, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for NewSubEthaSlab {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Slab::obtain(path, self.capacity, self.element_size, self.alignment, self.tag, false, false)?)
    }
}

/// Attaches to the slab at Path, which must exist with the capacity and
/// layout it was created with; ReadOnly opens it without write access.
///
/// # Examples
///
/// `$slab = Open-SubEthaSlab -Path C:\ipc\slab -Capacity 64 -ElementSize 16 -ReadOnly`
#[cmdlet(verb = "Open", noun = "SubEthaSlab", alias = "Open-SESlab", output = ["SubEtha.Slab"])]
#[derive(Default)]
pub struct OpenSubEthaSlab {
    /// The file the slab lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one slot holds.
    #[param(mandatory, position = 2)]
    pub element_size: u64,
    /// The alignment of each slot, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
    /// Open without write access.
    #[param]
    pub read_only: bool,
}

impl Cmdlet for OpenSubEthaSlab {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Slab::obtain(path, self.capacity, self.element_size, self.alignment, self.tag, true, self.read_only)?)
    }
}

/// A key and a value, both `byte[]`.
#[psclass(name = "SubEtha.Pair")]
#[derive(Clone, Default)]
pub struct Pair {
    /// The key.
    pub key: PsObject,
    /// The value.
    pub value: PsObject,
}

/// An ordered map in a mapped file, sorted by unsigned byte comparison
/// of the key so every language agrees on the order without a
/// comparator crossing the boundary.
#[psclass(name = "SubEtha.BTreeMap", mode = proxy)]
pub struct BTreeMap {
    /// The file the map lives in.
    pub path: String,
    /// How many entries it holds.
    pub capacity: u64,
    /// The bytes one key holds.
    pub key_size: u64,
    /// The bytes one value holds.
    pub value_size: u64,
    #[psfield(skip)]
    inner: RawBTreeMap,
}

impl BTreeMap {
    fn obtain(path: String, capacity: u64, key_size: u64, value_size: u64, tag: Option<u64>, open: bool) -> PsResult<Self> {
        if capacity < 1 {
            return Err(arg_err("the capacity must be at least one"));
        }
        let cap = size(capacity, "the capacity")?;
        let ks = size(key_size, "the key size")?;
        let vs = size(value_size, "the value size")?;
        let tag = tag.unwrap_or(0);
        let inner = if open { RawBTreeMap::open(&path, cap, ks, vs, tag) } else { RawBTreeMap::create(&path, cap, ks, vs, tag) }
            .map_err(|e| open_err("the map", &path, e))?;
        Ok(Self { path, capacity, key_size, value_size, inner })
    }

    fn end(&self, first: bool) -> PsResult<Option<Pair>> {
        let mut key = vec![0u8; self.inner.key_size()];
        let mut value = vec![0u8; self.inner.value_size()];
        let found = if first { self.inner.first(&mut key, &mut value) } else { self.inner.last(&mut key, &mut value) };
        match found {
            Ok(true) => Ok(Some(Pair { key: out_bytes(&key)?, value: out_bytes(&value)? })),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("reading an end of the map", e)),
        }
    }
}

/// The operations of a `SubEtha.BTreeMap`.
#[psmethods]
impl BTreeMap {
    /// How many entries are in it.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// How many nodes the tree uses.
    pub fn nodes(&self) -> PsResult<u64> {
        Ok(self.inner.node_count() as u64)
    }

    /// Inserts or replaces, and returns what the key held before, or
    /// `$null` when it held nothing. A full map is an error, because a
    /// map that cannot take a key has failed rather than answered.
    pub fn insert(&self, key: PsObject, value: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        let value = bytes(&value)?;
        let mut previous = vec![0u8; self.inner.value_size()];
        match self.inner.insert(&key, &value, Some(&mut previous)) {
            Ok(true) => Ok(Some(out_bytes(&previous)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("inserting", e)),
        }
    }

    /// Inserts each of `values` under the key beside it in `keys`,
    /// stopping when the map fills, and returns how many landed.
    pub fn insert_many(&self, keys: Vec<PsObject>, values: Vec<PsObject>) -> PsResult<u64> {
        if keys.len() != values.len() {
            return Err(arg_err("the keys and the values must be the same length"));
        }
        let mut done = 0;
        for (key, value) in keys.iter().zip(&values) {
            let key = bytes(key)?;
            let value = bytes(value)?;
            match self.inner.insert(&key, &value, None) {
                Ok(_) => done += 1,
                Err(BTreeError::Full) => break,
                Err(e) => return Err(op_err("inserting", e)),
            }
        }
        Ok(done)
    }

    /// The value under `key`, or `$null`.
    pub fn get(&self, key: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.get(&key, &mut out) {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("reading", e)),
        }
    }

    /// Whether `key` is in the map.
    pub fn contains(&self, key: PsObject) -> PsResult<bool> {
        let key = bytes(&key)?;
        self.inner.contains_key(&key).map_err(|e| op_err("looking up", e))
    }

    /// Removes `key` and returns what it held, or `$null` when it held
    /// nothing.
    pub fn remove(&self, key: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.remove(&key, &mut out) {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("removing", e)),
        }
    }

    /// The smallest key and its value, or `$null` when the map is
    /// empty.
    pub fn first(&self) -> PsResult<Option<Pair>> {
        self.end(true)
    }

    /// The largest key and its value, or `$null` when the map is empty.
    pub fn last(&self) -> PsResult<Option<Pair>> {
        self.end(false)
    }

    /// Removes every entry.
    pub fn clear(&self) -> PsResult<()> {
        self.inner.clear();
        Ok(())
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the ordered map at Path holding Capacity entries of
/// KeySize-byte keys and ValueSize-byte values, creating it when the
/// file does not exist.
///
/// # Examples
///
/// `$map = New-SubEthaBTreeMap -Path C:\ipc\btreemap -Capacity 64 -KeySize 8 -ValueSize 8`
#[cmdlet(verb = "New", noun = "SubEthaBTreeMap", alias = "New-SEBTreeMap", output = ["SubEtha.BTreeMap"])]
#[derive(Default)]
pub struct NewSubEthaBTreeMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one key holds.
    #[param(mandatory, position = 2)]
    pub key_size: u64,
    /// The bytes one value holds.
    #[param(mandatory, position = 3)]
    pub value_size: u64,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for NewSubEthaBTreeMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BTreeMap::obtain(path, self.capacity, self.key_size, self.value_size, self.tag, false)?)
    }
}

/// Attaches to the ordered map at Path, which must exist with the
/// capacity and sizes it was created with.
///
/// # Examples
///
/// `$map = Open-SubEthaBTreeMap -Path C:\ipc\btreemap -Capacity 64 -KeySize 8 -ValueSize 8`
#[cmdlet(verb = "Open", noun = "SubEthaBTreeMap", alias = "Open-SEBTreeMap", output = ["SubEtha.BTreeMap"])]
#[derive(Default)]
pub struct OpenSubEthaBTreeMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one key holds.
    #[param(mandatory, position = 2)]
    pub key_size: u64,
    /// The bytes one value holds.
    #[param(mandatory, position = 3)]
    pub value_size: u64,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for OpenSubEthaBTreeMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BTreeMap::obtain(path, self.capacity, self.key_size, self.value_size, self.tag, true)?)
    }
}

/// What an insert into a hash map did.
#[psenum(name = "SubEtha.InsertOutcome")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Inserted {
    /// The key was not there and is now.
    #[default]
    Inserted,
    /// The key was there and its value was replaced.
    Updated,
    /// The map was full and nothing changed.
    Full,
}

/// An exchange's answer: whether it swapped, and what was found.
#[psclass(name = "SubEtha.Exchange")]
#[derive(Clone, Default)]
pub struct Exchange {
    /// Whether the value was replaced.
    pub swapped: bool,
    /// The value found under the key, a `byte[]`.
    pub found: PsObject,
}

/// A hash map in a mapped file, at key and value sizes the caller
/// declares, shared by every process that opens it.
#[psclass(name = "SubEtha.HashMap", mode = proxy)]
pub struct HashMap {
    /// The file the map lives in.
    pub path: String,
    /// How many entries it holds.
    pub capacity: u64,
    /// The bytes one key holds.
    pub key_size: u64,
    /// The bytes one value holds.
    pub value_size: u64,
    #[psfield(skip)]
    inner: RawHashMap,
}

impl HashMap {
    fn obtain(path: String, capacity: u64, key_size: u64, value_size: u64, open: bool) -> PsResult<Self> {
        let cap = size(capacity, "the capacity")?;
        let ks = size(key_size, "the key size")?;
        let vs = size(value_size, "the value size")?;
        let inner = if open { RawHashMap::open(&path, cap, ks, vs) } else { RawHashMap::create(&path, cap, ks, vs) }.map_err(|e| open_err("the map", &path, e))?;
        Ok(Self { path, capacity, key_size, value_size, inner })
    }

    fn read(&self, key: &[u8]) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.get(key, &mut out) {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("reading", e)),
        }
    }
}

/// The operations of a `SubEtha.HashMap`.
#[psmethods]
impl HashMap {
    /// How many entries are in it.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Inserts or replaces, and says which it did, or that the map was
    /// full.
    pub fn insert(&self, key: PsObject, value: PsObject) -> PsResult<Inserted> {
        let key = bytes(&key)?;
        let value = bytes(&value)?;
        match self.inner.insert(&key, &value) {
            Ok(InsertOutcome::Inserted) => Ok(Inserted::Inserted),
            Ok(InsertOutcome::Updated) => Ok(Inserted::Updated),
            Err(MapError::Full) => Ok(Inserted::Full),
            Err(e) => Err(op_err("inserting", e)),
        }
    }

    /// Inserts each of `values` under the key beside it in `keys`,
    /// stopping at the first the map refuses, and returns how many
    /// landed.
    pub fn insert_many(&self, keys: Vec<PsObject>, values: Vec<PsObject>) -> PsResult<u64> {
        if keys.len() != values.len() {
            return Err(arg_err("the keys and the values must be the same length"));
        }
        let mut done = 0;
        for (key, value) in keys.iter().zip(&values) {
            let key = bytes(key)?;
            let value = bytes(value)?;
            match self.inner.insert(&key, &value) {
                Ok(_) => done += 1,
                Err(MapError::Full) => break,
                Err(e) => return Err(op_err("inserting", e)),
            }
        }
        Ok(done)
    }

    /// The value under `key`, or `$null`.
    pub fn get(&self, key: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        self.read(&key)
    }

    /// The values under a run of keys, `$null` where a key is absent.
    pub fn get_many(&self, keys: Vec<PsObject>) -> PsResult<Vec<PsObject>> {
        let mut answers = Vec::with_capacity(keys.len());
        for key in &keys {
            let key = bytes(key)?;
            answers.push(self.read(&key)?.unwrap_or_default());
        }
        Ok(answers)
    }

    /// Whether `key` is in the map.
    pub fn contains(&self, key: PsObject) -> PsResult<bool> {
        let key = bytes(&key)?;
        self.inner.contains_key(&key).map_err(|e| op_err("looking up", e))
    }

    /// Removes `key` and returns what it held, or `$null` when it held
    /// nothing.
    pub fn remove(&self, key: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.remove(&key, &mut out) {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("removing", e)),
        }
    }

    /// Replaces `expected` with `desired` under `key` only if that is
    /// what is there, and returns whether it swapped and what was
    /// found.
    pub fn compare_exchange(&self, key: PsObject, expected: PsObject, desired: PsObject) -> PsResult<Exchange> {
        let key = bytes(&key)?;
        let expected = bytes(&expected)?;
        let desired = bytes(&desired)?;
        let mut current = vec![0u8; self.inner.value_size()];
        let swapped = self.inner.compare_exchange(&key, &expected, &desired, &mut current).map_err(|e| op_err("comparing and exchanging", e))?;
        Ok(Exchange { swapped, found: out_bytes(&current)? })
    }

    /// Removes every entry.
    pub fn clear(&self) -> PsResult<()> {
        self.inner.clear();
        Ok(())
    }

    /// How many removed entries still occupy their slots.
    pub fn tombstones(&self) -> PsResult<u64> {
        Ok(self.inner.tombstone_count() as u64)
    }
}

/// Obtains the hash map at Path holding Capacity entries of
/// KeySize-byte keys and ValueSize-byte values, creating it when the
/// file does not exist.
///
/// # Examples
///
/// `$map = New-SubEthaHashMap -Path C:\ipc\hashmap -Capacity 64 -KeySize 8 -ValueSize 8`
#[cmdlet(verb = "New", noun = "SubEthaHashMap", alias = "New-SEHashMap", output = ["SubEtha.HashMap"])]
#[derive(Default)]
pub struct NewSubEthaHashMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one key holds.
    #[param(mandatory, position = 2)]
    pub key_size: u64,
    /// The bytes one value holds.
    #[param(mandatory, position = 3)]
    pub value_size: u64,
}

impl Cmdlet for NewSubEthaHashMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HashMap::obtain(path, self.capacity, self.key_size, self.value_size, false)?)
    }
}

/// Attaches to the hash map at Path, which must exist with the capacity
/// and sizes it was created with.
///
/// # Examples
///
/// `$map = Open-SubEthaHashMap -Path C:\ipc\hashmap -Capacity 64 -KeySize 8 -ValueSize 8`
#[cmdlet(verb = "Open", noun = "SubEthaHashMap", alias = "Open-SEHashMap", output = ["SubEtha.HashMap"])]
#[derive(Default)]
pub struct OpenSubEthaHashMap {
    /// The file the map lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one key holds.
    #[param(mandatory, position = 2)]
    pub key_size: u64,
    /// The bytes one value holds.
    #[param(mandatory, position = 3)]
    pub value_size: u64,
}

impl Cmdlet for OpenSubEthaHashMap {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HashMap::obtain(path, self.capacity, self.key_size, self.value_size, true)?)
    }
}

/// A push's answer: true for pushed, false for a full ring.
fn ring_push(outcome: Result<(), RingError>) -> PsResult<bool> {
    match outcome {
        Ok(()) => Ok(true),
        Err(RingError::Full) => Ok(false),
        Err(e) => Err(op_err("pushing", e)),
    }
}

/// A pop's answer: the length popped, or nothing for an empty ring.
fn ring_pop(outcome: Result<usize, RingError>) -> PsResult<Option<usize>> {
    match outcome {
        Ok(n) => Ok(Some(n)),
        Err(RingError::Empty) => Ok(None),
        Err(e) => Err(op_err("popping", e)),
    }
}

/// One producer's end of a pool: its own ring, drained by the one
/// consumer.
#[psclass(name = "SubEtha.MpscProducer", mode = proxy)]
pub struct MpscProducer {
    /// Which producer of the pool this is.
    pub index: u64,
    /// How many slots the ring holds.
    pub capacity: u64,
    #[psfield(skip)]
    inner: SubethaMpscProducer,
}

/// The operations of a `SubEtha.MpscProducer`.
#[psmethods]
impl MpscProducer {
    /// Pushes one item. False means the ring was full.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        ring_push(self.inner.try_push(&item))
    }

    /// Pushes a run of items, stopping at the first refusal, and
    /// returns how many went in.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        let mut pushed = 0;
        for item in &items {
            let item = bytes(item)?;
            if !ring_push(self.inner.try_push(&item))? {
                break;
            }
            pushed += 1;
        }
        Ok(pushed)
    }
}

/// The single consumer of a pool, draining every producer's ring in
/// turn.
#[psclass(name = "SubEtha.MpscConsumer", mode = proxy)]
pub struct MpscConsumer {
    /// How many producers feed it.
    pub producers: u64,
    #[psfield(skip)]
    inner: SubethaMpscConsumer,
}

/// The operations of a `SubEtha.MpscConsumer`.
#[psmethods]
impl MpscConsumer {
    /// How many items are waiting across every producer's ring, read
    /// without stopping the producers, so a sighting rather than a
    /// promise.
    pub fn approx_len(&self) -> PsResult<u64> {
        Ok(self.inner.approx_total_len() as u64)
    }

    /// The next item from any producer, or `$null` when every ring is
    /// empty.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match ring_pop(self.inner.try_pop(&mut out))? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Up to `maxItems` in one call, stopping when every ring is empty.
    pub fn pop_many(&self, max_items: u64) -> PsResult<Vec<PsObject>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match ring_pop(self.inner.try_pop(&mut out))? {
                Some(n) => taken.push(out_bytes(&out[..n])?),
                None => break,
            }
        }
        Ok(taken)
    }
}

/// A pool: one ring per producer, drained by one consumer.
#[psclass(name = "SubEtha.MpscPool")]
pub struct MpscPool {
    /// The producers, one per ring.
    pub producers: Vec<MpscProducer>,
    /// The one consumer.
    pub consumer: MpscConsumer,
}

/// Makes a pool at Path of Producers rings of Capacity slots, drained
/// by one consumer, and writes the producers and the consumer together.
/// Open attaches to a pool that exists with the shape it was built at.
///
/// # Examples
///
/// `$pool = New-SubEthaMpscPool -Path C:\ipc\mpscpool -Producers 4 -Capacity 64`
#[cmdlet(verb = "New", noun = "SubEthaMpscPool", alias = "New-SEMpscPool", output = ["SubEtha.MpscPool"])]
#[derive(Default)]
pub struct NewSubEthaMpscPool {
    /// The file the pool lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many producers, each with a ring of its own.
    #[param(mandatory, position = 1)]
    pub producers: u64,
    /// How many slots each ring holds.
    #[param(mandatory, position = 2)]
    pub capacity: u64,
    /// Attach to a pool that already exists instead of creating one.
    #[param]
    pub open: bool,
}

impl Cmdlet for NewSubEthaMpscPool {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        if self.producers < 1 {
            return Err(arg_err("a pool needs at least one producer"));
        }
        let n = size(self.producers, "the producer count")?;
        let slots = size(self.capacity, "the capacity")?;
        let (producers, consumer) = if self.open { SharedRingMpsc::open_pool(&path, n, slots) } else { SharedRingMpsc::create_pool(&path, n, slots) }
            .map_err(|e| open_err("the pool", &path, e))?;
        let producers = producers
            .into_iter()
            .enumerate()
            .map(|(index, inner)| MpscProducer { index: index as u64, capacity: inner.capacity() as u64, inner })
            .collect();
        ps.write(MpscPool { producers, consumer: MpscConsumer { producers: consumer.n_producers() as u64, inner: consumer } })
    }
}

/// One producer's end of a grid.
#[psclass(name = "SubEtha.MpmcProducer", mode = proxy)]
pub struct MpmcProducer {
    /// Which producer of the grid this is.
    pub index: u64,
    /// How many slots the ring holds.
    pub capacity: u64,
    #[psfield(skip)]
    inner: SubethaMpmcProducer,
}

/// The operations of a `SubEtha.MpmcProducer`.
#[psmethods]
impl MpmcProducer {
    /// Pushes one item. False means the ring was full.
    pub fn push(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        ring_push(self.inner.try_push(&item))
    }

    /// Pushes a run of items, stopping at the first refusal, and
    /// returns how many went in.
    pub fn push_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        let mut pushed = 0;
        for item in &items {
            let item = bytes(item)?;
            if !ring_push(self.inner.try_push(&item))? {
                break;
            }
            pushed += 1;
        }
        Ok(pushed)
    }
}

/// One consumer's end of a grid, draining the subset of rings it was
/// given.
#[psclass(name = "SubEtha.MpmcConsumer", mode = proxy)]
pub struct MpmcConsumer {
    /// Which consumer of the grid this is.
    pub index: u64,
    /// How many rings it drains.
    pub rings: u64,
    #[psfield(skip)]
    inner: SubethaMpmcConsumer,
}

/// The operations of a `SubEtha.MpmcConsumer`.
#[psmethods]
impl MpmcConsumer {
    /// How many items are waiting across its rings, read without
    /// stopping the producers.
    pub fn approx_len(&self) -> PsResult<u64> {
        Ok(self.inner.approx_subset_len() as u64)
    }

    /// The next item from any of its rings, or `$null` when they are
    /// all empty.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        match ring_pop(self.inner.try_pop(&mut out))? {
            Some(n) => Ok(Some(out_bytes(&out[..n])?)),
            None => Ok(None),
        }
    }

    /// Up to `maxItems` in one call, stopping when its rings are empty.
    pub fn pop_many(&self, max_items: u64) -> PsResult<Vec<PsObject>> {
        let mut taken = Vec::new();
        let mut out = vec![0u8; SPSC_PAYLOAD_BYTES];
        for _ in 0..max_items {
            match ring_pop(self.inner.try_pop(&mut out))? {
                Some(n) => taken.push(out_bytes(&out[..n])?),
                None => break,
            }
        }
        Ok(taken)
    }
}

/// A grid: a ring per producer, shared out among consumers.
#[psclass(name = "SubEtha.MpmcGrid")]
pub struct MpmcGrid {
    /// The producers, one per ring.
    pub producers: Vec<MpmcProducer>,
    /// The consumers, each draining a share of the rings.
    pub consumers: Vec<MpmcConsumer>,
}

/// Makes a grid at Path of Producers rings of Capacity slots shared out
/// among Consumers, and writes the producers and the consumers
/// together. Open attaches to a grid that exists with the shape it was
/// built at.
///
/// # Examples
///
/// `$grid = New-SubEthaMpmcGrid -Path C:\ipc\mpmcgrid -Producers 4 -Consumers 2 -Capacity 64`
#[cmdlet(verb = "New", noun = "SubEthaMpmcGrid", alias = "New-SEMpmcGrid", output = ["SubEtha.MpmcGrid"])]
#[derive(Default)]
pub struct NewSubEthaMpmcGrid {
    /// The file the grid lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many producers, each with a ring of its own.
    #[param(mandatory, position = 1)]
    pub producers: u64,
    /// How many consumers share the rings out; no more than the
    /// producers.
    #[param(mandatory, position = 2)]
    pub consumers: u64,
    /// How many slots each ring holds.
    #[param(mandatory, position = 3)]
    pub capacity: u64,
    /// Attach to a grid that already exists instead of creating one.
    #[param]
    pub open: bool,
}

impl Cmdlet for NewSubEthaMpmcGrid {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        if self.consumers < 1 {
            return Err(arg_err("a grid needs at least one consumer"));
        }
        if self.producers < self.consumers {
            return Err(arg_err("a grid needs at least as many producers as consumers"));
        }
        let p = size(self.producers, "the producer count")?;
        let c = size(self.consumers, "the consumer count")?;
        let slots = size(self.capacity, "the capacity")?;
        let (producers, consumers) = if self.open { SharedRingMpmc::open_grid(&path, p, c, slots) } else { SharedRingMpmc::create_grid(&path, p, c, slots) }
            .map_err(|e| open_err("the grid", &path, e))?;
        let producers = producers
            .into_iter()
            .enumerate()
            .map(|(index, inner)| MpmcProducer { index: index as u64, capacity: inner.capacity() as u64, inner })
            .collect();
        let consumers = consumers
            .into_iter()
            .enumerate()
            .map(|(index, inner)| MpmcConsumer { index: index as u64, rings: inner.n_rings() as u64, inner })
            .collect();
        ps.write(MpmcGrid { producers, consumers })
    }
}
