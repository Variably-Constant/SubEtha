//! The single values and plain containers: an atomic, a region of
//! slots, a versioned cell, a vector, a reference-counted value, a value
//! computed once, and a bit vector.

use std::sync::atomic::Ordering;

use pwrs::prelude::*;

use subetha_cxc::raw_cell::RawCell;
use subetha_cxc::raw_region::RawRegion;
use subetha_cxc::raw_treiber_stack::ElementLayout;
use subetha_cxc::raw_vec::RawVec;
use subetha_cxc::shared_arc::{LastHolder, SharedArcDyn};
use subetha_cxc::shared_atomic::SharedAtomicU64;
use subetha_cxc::shared_bit_vec::SharedBitVec;
use subetha_cxc::shared_once_cell::SharedOnceCellDyn;
use subetha_cxc::shared_vec::VecError;

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, open_err, out_bytes, size};

assert_send!(Atomic, Region, Cell, SharedVec, SharedArc, LazyValue, BitVec);

/// The memory ordering a load, a store or a read-modify-write uses: the
/// Rust orderings under their own names.
#[psenum(name = "SubEtha.MemoryOrder")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MemoryOrder {
    /// No ordering beyond the operation itself.
    Relaxed,
    /// Reads after this one see what the matching release made visible.
    Acquire,
    /// Writes before this one are visible to a matching acquire.
    Release,
    /// Acquire and release together.
    AcqRel,
    /// One total order every thread agrees on.
    #[default]
    SeqCst,
}

impl MemoryOrder {
    fn rust(self) -> Ordering {
        match self {
            MemoryOrder::Relaxed => Ordering::Relaxed,
            MemoryOrder::Acquire => Ordering::Acquire,
            MemoryOrder::Release => Ordering::Release,
            MemoryOrder::AcqRel => Ordering::AcqRel,
            MemoryOrder::SeqCst => Ordering::SeqCst,
        }
    }
}

/// The ordering a method argument names, sequentially consistent when
/// the argument is absent.
fn ordering(order: Option<MemoryOrder>) -> Ordering {
    order.unwrap_or_default().rust()
}

/// The layout of one element of a region, a vector or a stack, checked
/// against what the constructors assert so a bad one is refused by
/// name rather than panicking inside Rust.
pub(crate) fn layout(element_size: u64, alignment: Option<u64>, tag: Option<u64>) -> PsResult<ElementLayout> {
    if element_size < 1 {
        return Err(arg_err("the element size must be at least one byte"));
    }
    if element_size > u32::MAX as u64 {
        return Err(arg_err("the element size must fit in 32 bits"));
    }
    let alignment = alignment.unwrap_or(1);
    if alignment < 1 || !alignment.is_power_of_two() {
        return Err(arg_err("the alignment must be a power of two"));
    }
    Ok(ElementLayout {
        slot_size: size(element_size, "the element size")?,
        alignment: size(alignment, "the alignment")?,
        tag: tag.unwrap_or(0),
    })
}

/// A 64-bit integer in a file every process maps.
///
/// The single-call surface, and the one to measure against: a `Load`
/// is the same work the C ABI does in about seven nanoseconds, so what
/// it costs over that is what the method call costs.
#[psclass(name = "SubEtha.Atomic", mode = proxy)]
pub struct Atomic {
    /// The file the atomic lives in.
    pub path: String,
    #[psfield(skip)]
    inner: SharedAtomicU64,
}

/// The operations of a `SubEtha.Atomic`. Every `order` argument is a
/// `SubEtha.MemoryOrder`, sequentially consistent when absent.
#[psmethods]
impl Atomic {
    /// The value.
    pub fn load(&self, order: Option<MemoryOrder>) -> PsResult<u64> {
        Ok(self.inner.load(ordering(order)))
    }

    /// Writes `value`.
    pub fn store(&self, value: u64, order: Option<MemoryOrder>) -> PsResult<()> {
        self.inner.store(value, ordering(order));
        Ok(())
    }

    /// Adds `value`, one when absent, and returns what the value was
    /// before. Adding past what sixty-four bits hold wraps round, as it
    /// does in Rust and in C.
    pub fn fetch_add(&self, value: Option<u64>, order: Option<MemoryOrder>) -> PsResult<u64> {
        Ok(self.inner.fetch_add(value.unwrap_or(1), ordering(order)))
    }

    /// Subtracts `value`, one when absent, and returns what the value
    /// was before. Taking more than the value holds wraps round to the
    /// top, which is what a counter going below zero means here.
    pub fn fetch_sub(&self, value: Option<u64>, order: Option<MemoryOrder>) -> PsResult<u64> {
        Ok(self.inner.fetch_sub(value.unwrap_or(1), ordering(order)))
    }

    /// Sets every bit that is set in `value` and returns what the value
    /// was before.
    pub fn fetch_or(&self, value: u64, order: Option<MemoryOrder>) -> PsResult<u64> {
        Ok(self.inner.fetch_or(value, ordering(order)))
    }

    /// Clears every bit that is not set in `value` and returns what the
    /// value was before.
    pub fn fetch_and(&self, value: u64, order: Option<MemoryOrder>) -> PsResult<u64> {
        Ok(self.inner.fetch_and(value, ordering(order)))
    }

    /// Flips every bit that is set in `value` and returns what the
    /// value was before.
    pub fn fetch_xor(&self, value: u64, order: Option<MemoryOrder>) -> PsResult<u64> {
        Ok(self.inner.fetch_xor(value, ordering(order)))
    }

    /// Puts `value` in and returns what was there, in one step nothing
    /// else can get between.
    pub fn swap(&self, value: u64, order: Option<MemoryOrder>) -> PsResult<u64> {
        Ok(self.inner.swap(value, ordering(order)))
    }

    /// Puts `desired` in only if the value is still `expected`, and
    /// returns what was there either way; the answer being `expected`
    /// is how a caller knows it won. This is the one operation that
    /// lets several processes agree on who changes something, and a
    /// caller that needs to retry loops on it.
    pub fn compare_exchange(&self, expected: u64, desired: u64, order: Option<MemoryOrder>) -> PsResult<u64> {
        match self.inner.compare_exchange(expected, desired, ordering(order), Ordering::Acquire) {
            Ok(won) => Ok(won),
            // A refusal is an ordinary answer here, and the value it
            // carries is the whole point: it is what the caller compares
            // against next time round.
            Err(found) => Ok(found),
        }
    }

    /// Adds `value`, one when absent, `count` times and returns the
    /// value before the run: the batch shape, so the cost of one call
    /// is spread over many operations.
    pub fn fetch_add_many(&self, count: u64, value: Option<u64>, order: Option<MemoryOrder>) -> PsResult<u64> {
        let ord = ordering(order);
        let step = value.unwrap_or(1);
        let mut first = 0;
        for i in 0..count {
            let seen = self.inner.fetch_add(step, ord);
            if i == 0 {
                first = seen;
            }
        }
        Ok(first)
    }
}

/// Obtains the atomic at Path, creating it holding Init when the file
/// does not exist and attaching to its live value when it does.
///
/// # Examples
///
/// `$counter = New-SubEthaAtomic -Path C:\ipc\counter -Init 0`
#[cmdlet(verb = "New", noun = "SubEthaAtomic", alias = "New-SEAtomic", output = ["SubEtha.Atomic"])]
#[derive(Default)]
pub struct NewSubEthaAtomic {
    /// The file the atomic lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The value the atomic starts at when the file is created; zero
    /// when absent. Ignored when the file exists.
    #[param]
    pub init: Option<u64>,
}

impl Cmdlet for NewSubEthaAtomic {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let inner = SharedAtomicU64::create(&path, self.init.unwrap_or(0)).map_err(|e| open_err("the atomic", &path, e))?;
        ps.write(Atomic { path, inner })
    }
}

/// Attaches to the atomic at Path, which must exist, leaving its value
/// alone.
///
/// # Examples
///
/// `$counter = Open-SubEthaAtomic -Path C:\ipc\counter`
#[cmdlet(verb = "Open", noun = "SubEthaAtomic", alias = "Open-SEAtomic", output = ["SubEtha.Atomic"])]
#[derive(Default)]
pub struct OpenSubEthaAtomic {
    /// The file the atomic lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
}

impl Cmdlet for OpenSubEthaAtomic {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let inner = SharedAtomicU64::open(&path).map_err(|e| open_err("the atomic", &path, e))?;
        ps.write(Atomic { path, inner })
    }
}

/// A fixed-capacity arena of equal-sized slots in a file every process
/// maps.
#[psclass(name = "SubEtha.Region", mode = proxy)]
pub struct Region {
    /// The file the region lives in.
    pub path: String,
    /// How many slots the region holds.
    pub capacity: u64,
    /// The bytes one slot holds.
    pub slot_size: u64,
    #[psfield(skip)]
    inner: RawRegion,
    /// Bytes from the first slot to the end of the last, which is what
    /// a snapshot covers. Taken from the region's own addresses rather
    /// than assumed, so a slot geometry wider than the element is right.
    #[psfield(skip)]
    span: usize,
}

impl Region {
    fn obtain(path: String, capacity: u64, layout: ElementLayout, open: bool) -> PsResult<Self> {
        let slots = size(capacity, "the capacity")?;
        let inner = if open { RawRegion::open(&path, slots, layout) } else { RawRegion::create(&path, slots, layout) }
            .map_err(|e| open_err("the region", &path, e))?;
        let span = Self::measure_span(&inner, slots, layout.slot_size)?;
        Ok(Self { path, capacity, slot_size: layout.slot_size as u64, inner, span })
    }

    /// Bytes from the first slot to the end of the last, read from the
    /// region's own slot addresses so a stride wider than the element is
    /// accounted for rather than assumed away.
    fn measure_span(region: &RawRegion, capacity: usize, slot_size: usize) -> PsResult<usize> {
        if capacity == 0 {
            return Ok(0);
        }
        let first = region.element_ptr(0).map_err(|e| op_err("addressing the first slot", e))? as usize;
        let last = region.element_ptr((capacity - 1) as u32).map_err(|e| op_err("addressing the last slot", e))? as usize;
        Ok(last + slot_size - first)
    }
}

/// The operations of a `SubEtha.Region`.
#[psmethods]
impl Region {
    /// How many slots are allocated.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Takes a slot, writes `value`, which must be exactly the slot
    /// size, into it and returns its index.
    pub fn allocate(&self, value: PsObject) -> PsResult<u32> {
        let value = bytes(&value)?;
        self.inner.allocate(&value).map_err(|e| op_err("allocating a slot", e))
    }

    /// The bytes of slot `index`.
    pub fn get(&self, index: u32) -> PsResult<PsObject> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        self.inner.get(index, &mut out).map_err(|e| op_err("reading a slot", e))?;
        out_bytes(&out)
    }

    /// Writes slot `index` with `value`, which must be exactly the slot
    /// size.
    pub fn set(&self, index: u32, value: PsObject) -> PsResult<()> {
        let value = bytes(&value)?;
        self.inner.set(index, &value).map_err(|e| op_err("writing a slot", e))
    }

    /// Every slot's bytes from the first to the end of the last, as one
    /// `byte[]`, read from the mapping in one pass: the bulk path, which
    /// crosses the boundary once for the whole region.
    pub fn snapshot(&self) -> PsResult<PsObject> {
        if self.span == 0 {
            return out_bytes(&[]);
        }
        let base = self.inner.element_ptr(0).map_err(|e| op_err("addressing the first slot", e))? as *const u8;
        // The mapping lives for as long as this object does, and the
        // span was measured from the region's own slot addresses, so
        // every byte in it is mapped memory of this region.
        let view = unsafe { std::slice::from_raw_parts(base, self.span) };
        out_bytes(view)
    }
}

/// Obtains the region at Path holding Capacity slots of SlotSize bytes,
/// creating it when the file does not exist.
///
/// # Examples
///
/// `$region = New-SubEthaRegion -Path C:\ipc\region -Capacity 64 -SlotSize 64`
#[cmdlet(verb = "New", noun = "SubEthaRegion", alias = "New-SERegion", output = ["SubEtha.Region"])]
#[derive(Default)]
pub struct NewSubEthaRegion {
    /// The file the region lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the region holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one slot holds.
    #[param(mandatory, position = 2)]
    pub slot_size: u64,
    /// The alignment of each slot, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for NewSubEthaRegion {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let layout = layout(self.slot_size, self.alignment, self.tag)?;
        ps.write(Region::obtain(path, self.capacity, layout, false)?)
    }
}

/// Attaches to the region at Path, which must exist with the same
/// capacity and layout it was created with.
///
/// # Examples
///
/// `$region = Open-SubEthaRegion -Path C:\ipc\region -Capacity 64 -SlotSize 64`
#[cmdlet(verb = "Open", noun = "SubEthaRegion", alias = "Open-SERegion", output = ["SubEtha.Region"])]
#[derive(Default)]
pub struct OpenSubEthaRegion {
    /// The file the region lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many slots the region holds.
    #[param(mandatory, position = 1)]
    pub capacity: u64,
    /// The bytes one slot holds.
    #[param(mandatory, position = 2)]
    pub slot_size: u64,
    /// The alignment of each slot, a power of two; one when absent.
    #[param]
    pub alignment: Option<u64>,
    /// A tag stored with the layout; zero when absent.
    #[param]
    pub tag: Option<u64>,
}

impl Cmdlet for OpenSubEthaRegion {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let layout = layout(self.slot_size, self.alignment, self.tag)?;
        ps.write(Region::obtain(path, self.capacity, layout, true)?)
    }
}

/// One fixed-size value in a file every process maps, with a version
/// that steps on each write so a reader can tell a change from a
/// repeat.
#[psclass(name = "SubEtha.Cell", mode = proxy)]
pub struct Cell {
    /// The file the cell lives in.
    pub path: String,
    /// The bytes the value holds.
    pub value_size: u64,
    #[psfield(skip)]
    inner: RawCell,
}

impl Cell {
    fn obtain(path: String, value_size: u64, open: bool) -> PsResult<Self> {
        let len = size(value_size, "the value size")?;
        let inner = if open { RawCell::open(&path, len) } else { RawCell::create(&path, len) }.map_err(|e| open_err("the cell", &path, e))?;
        Ok(Self { path, value_size, inner })
    }
}

/// The operations of a `SubEtha.Cell`.
#[psmethods]
impl Cell {
    /// The version, which steps on every write. A reader that sees the
    /// same version twice saw no write between.
    pub fn version(&self) -> PsResult<u32> {
        Ok(self.inner.version())
    }

    /// The value.
    pub fn get(&self) -> PsResult<PsObject> {
        let mut out = vec![0u8; self.inner.value_size()];
        self.inner.get(&mut out).map_err(|e| op_err("reading the cell", e))?;
        out_bytes(&out)
    }

    /// Writes `value`, which must be exactly the value size.
    pub fn set(&self, value: PsObject) -> PsResult<()> {
        let value = bytes(&value)?;
        self.inner.set(&value).map_err(|e| op_err("writing the cell", e))
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing the cell", e))
    }
}

/// Obtains the cell at Path holding a value of ValueSize bytes, creating
/// it when the file does not exist.
///
/// # Examples
///
/// `$cell = New-SubEthaCell -Path C:\ipc\cell -ValueSize 8`
#[cmdlet(verb = "New", noun = "SubEthaCell", alias = "New-SECell", output = ["SubEtha.Cell"])]
#[derive(Default)]
pub struct NewSubEthaCell {
    /// The file the cell lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes the value holds.
    #[param(mandatory, position = 1)]
    pub value_size: u64,
}

impl Cmdlet for NewSubEthaCell {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Cell::obtain(path, self.value_size, false)?)
    }
}

/// Attaches to the cell at Path, which must exist with the same value
/// size it was created with.
///
/// # Examples
///
/// `$cell = Open-SubEthaCell -Path C:\ipc\cell -ValueSize 8`
#[cmdlet(verb = "Open", noun = "SubEthaCell", alias = "Open-SECell", output = ["SubEtha.Cell"])]
#[derive(Default)]
pub struct OpenSubEthaCell {
    /// The file the cell lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes the value holds.
    #[param(mandatory, position = 1)]
    pub value_size: u64,
}

impl Cmdlet for OpenSubEthaCell {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Cell::obtain(path, self.value_size, true)?)
    }
}

/// A growable-to-capacity sequence of equal-sized elements in a mapped
/// file.
///
/// Each element sits under a seqlock: the slot carries a version, a
/// writer steps it either side of the copy, and a reader retries while
/// it is odd or has moved. Every read here goes through that protocol,
/// so a writer racing a read loses to a retry rather than handing over
/// a torn element; `ReadRange` is the bulk path, one crossing for a run
/// of elements.
#[psclass(name = "SubEtha.Vec", mode = proxy)]
pub struct SharedVec {
    /// The file the vector lives in.
    pub path: String,
    /// How many elements it can hold.
    pub capacity: u64,
    /// The bytes one element holds.
    pub element_size: u64,
    /// Whether this handle may write.
    pub writable: bool,
    #[psfield(skip)]
    inner: RawVec,
}

impl SharedVec {
    fn obtain(path: String, capacity: u64, layout: ElementLayout, open: bool) -> PsResult<Self> {
        let slots = size(capacity, "the capacity")?;
        let inner = if open { RawVec::open(&path, slots, layout) } else { RawVec::create(&path, slots, layout) }
            .map_err(|e| open_err("the vector", &path, e))?;
        let writable = inner.is_writable();
        Ok(Self { path, capacity, element_size: layout.slot_size as u64, writable, inner })
    }
}

/// The operations of a `SubEtha.Vec`.
#[psmethods]
impl SharedVec {
    /// How many elements are live.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// Appends one element and returns its index, or `$null` when the
    /// vector is full.
    pub fn push(&self, value: PsObject) -> PsResult<Option<u64>> {
        let value = bytes(&value)?;
        match self.inner.push_back(&value) {
            Ok(index) => Ok(Some(index as u64)),
            Err(VecError::Full) => Ok(None),
            Err(e) => Err(op_err("appending", e)),
        }
    }

    /// Appends a run of elements, stopping at the first refusal, and
    /// returns how many landed.
    pub fn push_many(&self, values: Vec<PsObject>) -> PsResult<u64> {
        let mut pushed = 0;
        for value in &values {
            let value = bytes(value)?;
            match self.inner.push_back(&value) {
                Ok(_) => pushed += 1,
                Err(VecError::Full) => break,
                Err(e) => return Err(op_err("appending", e)),
            }
        }
        Ok(pushed)
    }

    /// Takes the last element, or `$null` when the vector is empty.
    pub fn pop(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.pop_back(&mut out) {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("popping", e)),
        }
    }

    /// The element at `index`, or `$null` past the live ones.
    pub fn get(&self, index: u64) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.layout().slot_size];
        match self.inner.get(size(index, "the index")?, &mut out) {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("reading", e)),
        }
    }

    /// Writes the element at `index`.
    pub fn set(&self, index: u64, value: PsObject) -> PsResult<()> {
        let value = bytes(&value)?;
        self.inner.set(size(index, "the index")?, &value).map_err(|e| op_err("writing", e))
    }

    /// Removes every element.
    pub fn clear(&self) -> PsResult<()> {
        self.inner.clear().map_err(|e| op_err("clearing", e))
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }

    /// `count` elements from `start`, packed end to end in one
    /// `byte[]`, stopping at the end of what is live.
    pub fn read_range(&self, start: u64, count: u64) -> PsResult<PsObject> {
        let element = self.inner.layout().slot_size;
        let start = size(start, "the start")?;
        let count = size(count, "the count")?;
        let mut packed = vec![0u8; count.saturating_mul(element)];
        let mut taken = 0;
        for i in 0..count {
            let at = taken * element;
            match self.inner.get(start + i, &mut packed[at..at + element]) {
                Ok(true) => taken += 1,
                Ok(false) => break,
                Err(e) => return Err(op_err("reading a range", e)),
            }
        }
        packed.truncate(taken * element);
        out_bytes(&packed)
    }

    /// Writes elements packed end to end in `data` into consecutive
    /// slots from `start`, and returns how many landed.
    pub fn write_range(&self, start: u64, data: PsObject) -> PsResult<u64> {
        let element = self.inner.layout().slot_size;
        let data = bytes(&data)?;
        if element == 0 || data.len() % element != 0 {
            return Err(arg_err("the data must be a whole number of elements"));
        }
        let start = size(start, "the start")?;
        let mut written = 0;
        for (i, chunk) in data.chunks(element).enumerate() {
            match self.inner.set(start + i, chunk) {
                Ok(()) => written += 1,
                Err(VecError::OutOfBounds) => break,
                Err(e) => return Err(op_err("writing a range", e)),
            }
        }
        Ok(written)
    }
}

/// Obtains the vector at Path holding up to Capacity elements of
/// ElementSize bytes, creating it when the file does not exist.
///
/// # Examples
///
/// `$vec = New-SubEthaVec -Path C:\ipc\vec -Capacity 64 -ElementSize 8`
#[cmdlet(verb = "New", noun = "SubEthaVec", alias = "New-SEVec", output = ["SubEtha.Vec"])]
#[derive(Default)]
pub struct NewSubEthaVec {
    /// The file the vector lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many elements it can hold.
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

impl Cmdlet for NewSubEthaVec {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let layout = layout(self.element_size, self.alignment, self.tag)?;
        ps.write(SharedVec::obtain(path, self.capacity, layout, false)?)
    }
}

/// Attaches to the vector at Path, which must exist with the same
/// capacity and layout it was created with.
///
/// # Examples
///
/// `$vec = Open-SubEthaVec -Path C:\ipc\vec -Capacity 64 -ElementSize 8`
#[cmdlet(verb = "Open", noun = "SubEthaVec", alias = "Open-SEVec", output = ["SubEtha.Vec"])]
#[derive(Default)]
pub struct OpenSubEthaVec {
    /// The file the vector lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many elements it can hold.
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

impl Cmdlet for OpenSubEthaVec {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let layout = layout(self.element_size, self.alignment, self.tag)?;
        ps.write(SharedVec::obtain(path, self.capacity, layout, true)?)
    }
}

/// A value in a mapped file that several processes hold at once,
/// counted like a reference count: the file goes when the last holder
/// does, or stays for a later process, as the holder chose.
#[psclass(name = "SubEtha.SharedArc", mode = proxy)]
pub struct SharedArc {
    /// The file the value lives in.
    pub path: String,
    /// The bytes the value holds.
    pub value_size: u64,
    /// Whether the file stays when the last holder lets go.
    pub keep_on_last: bool,
    #[psfield(skip)]
    inner: SharedArcDyn,
}

impl SharedArc {
    fn policy(keep_on_last: bool) -> LastHolder {
        if keep_on_last { LastHolder::Keep } else { LastHolder::Unlink }
    }
}

/// The operations of a `SubEtha.SharedArc`.
#[psmethods]
impl SharedArc {
    /// How many processes hold it right now.
    pub fn holders(&self) -> PsResult<u64> {
        Ok(self.inner.strong_count() as u64)
    }

    /// The whole value.
    pub fn get(&self) -> PsResult<PsObject> {
        out_bytes(self.inner.as_slice())
    }

    /// `length` bytes of the value from `offset`, for a caller that
    /// wants a field rather than the whole of a large record.
    pub fn read_at(&self, offset: u64, length: u64) -> PsResult<PsObject> {
        let mut out = vec![0u8; size(length, "the length")?];
        self.inner.read_at(size(offset, "the offset")?, &mut out).map_err(|e| op_err("reading", e))?;
        out_bytes(&out)
    }

    /// Writes `value` into the value at `offset`.
    pub fn write_at(&self, offset: u64, value: PsObject) -> PsResult<()> {
        let value = bytes(&value)?;
        self.inner.write_at(size(offset, "the offset")?, &value).map_err(|e| op_err("writing", e))
    }
}

/// Obtains the shared value at Path, creating it holding Value when the
/// file does not exist.
///
/// # Examples
///
/// `$shared = New-SubEthaSharedArc -Path C:\ipc\sharedarc -Value 'held'`
#[cmdlet(verb = "New", noun = "SubEthaSharedArc", alias = "New-SESharedArc", output = ["SubEtha.SharedArc"])]
#[derive(Default)]
pub struct NewSubEthaSharedArc {
    /// The file the value lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The value, a `byte[]` or a string, whose length fixes the size.
    #[param(mandatory, position = 1)]
    pub value: PsObject,
    /// How many processes may hold it at once; sixteen when absent.
    #[param]
    pub max_holders: Option<u64>,
    /// Keep the file when the last holder lets go, instead of unlinking
    /// it.
    #[param]
    pub keep_on_last: bool,
}

impl Cmdlet for NewSubEthaSharedArc {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let holders = size(self.max_holders.unwrap_or(16), "the holder count")?;
        if holders < 1 {
            return Err(arg_err("the holder count must be at least one"));
        }
        let value = bytes(&self.value)?;
        let inner = SharedArcDyn::create(&path, &value, holders, SharedArc::policy(self.keep_on_last))
            .map_err(|e| open_err("the shared value", &path, e))?;
        let value_size = value.len() as u64;
        ps.write(SharedArc { path, value_size, keep_on_last: self.keep_on_last, inner })
    }
}

/// Attaches to the shared value at Path, which must exist holding
/// ValueSize bytes.
///
/// # Examples
///
/// `$shared = Open-SubEthaSharedArc -Path C:\ipc\sharedarc -ValueSize 8`
#[cmdlet(verb = "Open", noun = "SubEthaSharedArc", alias = "Open-SESharedArc", output = ["SubEtha.SharedArc"])]
#[derive(Default)]
pub struct OpenSubEthaSharedArc {
    /// The file the value lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes the value holds.
    #[param(mandatory, position = 1)]
    pub value_size: u64,
    /// How many processes may hold it at once; sixteen when absent.
    #[param]
    pub max_holders: Option<u64>,
    /// Keep the file when the last holder lets go, instead of unlinking
    /// it.
    #[param]
    pub keep_on_last: bool,
}

impl Cmdlet for OpenSubEthaSharedArc {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        let holders = size(self.max_holders.unwrap_or(16), "the holder count")?;
        if holders < 1 {
            return Err(arg_err("the holder count must be at least one"));
        }
        let inner = SharedArcDyn::open(&path, size(self.value_size, "the value size")?, holders, SharedArc::policy(self.keep_on_last))
            .map_err(|e| open_err("the shared value", &path, e))?;
        ps.write(SharedArc { path, value_size: self.value_size, keep_on_last: self.keep_on_last, inner })
    }
}

/// A value computed once, by whichever process gets there first, and
/// read by every other.
///
/// The protocol has three steps rather than one, because computing
/// something once across processes cannot be a single call: a process
/// claims the right to initialize, computes, and publishes. Everyone
/// else reads, or waits.
#[psclass(name = "SubEtha.LazyValue", mode = proxy)]
pub struct LazyValue {
    /// The file the value lives in.
    pub path: String,
    /// The bytes the value holds.
    pub value_size: u64,
    #[psfield(skip)]
    inner: SharedOnceCellDyn,
}

impl LazyValue {
    fn obtain(path: String, value_size: u64, open: bool) -> PsResult<Self> {
        if value_size == 0 {
            return Err(arg_err("the value size must be at least one byte"));
        }
        let len = size(value_size, "the value size")?;
        let inner = if open { SharedOnceCellDyn::open(&path, len) } else { SharedOnceCellDyn::create(&path, len) }
            .map_err(|e| open_err("the lazy value", &path, e))?;
        Ok(Self { path, value_size, inner })
    }
}

/// The operations of a `SubEtha.LazyValue`.
#[psmethods]
impl LazyValue {
    /// Whether a value has been published yet.
    pub fn ready(&self) -> PsResult<bool> {
        let mut out = vec![0u8; self.inner.value_len()];
        Ok(self.inner.try_get(&mut out))
    }

    /// The value, or `$null` when nobody has published one yet.
    pub fn get(&self) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.value_len()];
        if self.inner.try_get(&mut out) { Ok(Some(out_bytes(&out)?)) } else { Ok(None) }
    }

    /// Claims the right to compute the value, as `pid` or as this
    /// process. True means this caller won and must publish; false
    /// means someone else is doing it and this caller should wait.
    pub fn claim(&self, pid: Option<u32>) -> PsResult<bool> {
        Ok(self.inner.claim(crate::common::pid(pid)))
    }

    /// Publishes the value this caller claimed, which must be exactly
    /// the value size. False means it was not this caller's to publish.
    pub fn publish(&self, value: PsObject, pid: Option<u32>) -> PsResult<bool> {
        let value = bytes(&value)?;
        if value.len() != self.inner.value_len() {
            return Err(arg_err(format!("the value must be exactly {} bytes", self.inner.value_len())));
        }
        Ok(self.inner.publish(crate::common::pid(pid), &value))
    }

    /// Waits for whoever claimed it to publish, up to `timeout` seconds
    /// (thirty when absent), and returns the value.
    pub fn wait(&self, timeout: Option<f64>) -> PsResult<PsObject> {
        let timeout = crate::common::seconds(timeout.unwrap_or(30.0))?;
        let deadline = std::time::Instant::now() + timeout;
        let mut out = vec![0u8; self.inner.value_len()];
        self.inner.wait(&mut out, deadline).map_err(|e| op_err("waiting for the value", e))?;
        out_bytes(&out)
    }
}

/// Obtains the lazy value at Path holding ValueSize bytes, creating it
/// unpublished when the file does not exist.
///
/// # Examples
///
/// `$lazy = New-SubEthaLazyValue -Path C:\ipc\lazyvalue -ValueSize 8`
#[cmdlet(verb = "New", noun = "SubEthaLazyValue", alias = "New-SELazyValue", output = ["SubEtha.LazyValue"])]
#[derive(Default)]
pub struct NewSubEthaLazyValue {
    /// The file the value lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes the value holds.
    #[param(mandatory, position = 1)]
    pub value_size: u64,
}

impl Cmdlet for NewSubEthaLazyValue {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LazyValue::obtain(path, self.value_size, false)?)
    }
}

/// Attaches to the lazy value at Path, which must exist holding
/// ValueSize bytes.
///
/// # Examples
///
/// `$lazy = Open-SubEthaLazyValue -Path C:\ipc\lazyvalue -ValueSize 8`
#[cmdlet(verb = "Open", noun = "SubEthaLazyValue", alias = "Open-SELazyValue", output = ["SubEtha.LazyValue"])]
#[derive(Default)]
pub struct OpenSubEthaLazyValue {
    /// The file the value lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bytes the value holds.
    #[param(mandatory, position = 1)]
    pub value_size: u64,
}

impl Cmdlet for OpenSubEthaLazyValue {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LazyValue::obtain(path, self.value_size, true)?)
    }
}

/// A bit vector in a mapped file, addressed by bit index.
#[psclass(name = "SubEtha.BitVec", mode = proxy)]
pub struct BitVec {
    /// The file the bits live in.
    pub path: String,
    /// How many bits it holds.
    pub capacity_bits: u64,
    #[psfield(skip)]
    inner: SharedBitVec,
}

impl BitVec {
    fn obtain(path: String, capacity_bits: u64, open: bool) -> PsResult<Self> {
        if capacity_bits < 1 {
            return Err(arg_err("the capacity must be at least one bit"));
        }
        let bits = size(capacity_bits, "the capacity")?;
        let inner = if open { SharedBitVec::open(&path, bits) } else { SharedBitVec::create(&path, bits) }
            .map_err(|e| open_err("the bit vector", &path, e))?;
        Ok(Self { path, capacity_bits, inner })
    }
}

/// The operations of a `SubEtha.BitVec`.
#[psmethods]
impl BitVec {
    /// Sets bit `index` and returns what it was.
    pub fn set(&self, index: u64) -> PsResult<bool> {
        self.inner.set(size(index, "the index")?).map_err(|e| op_err("setting a bit", e))
    }

    /// Clears bit `index` and returns what it was.
    pub fn clear(&self, index: u64) -> PsResult<bool> {
        self.inner.clear(size(index, "the index")?).map_err(|e| op_err("clearing a bit", e))
    }

    /// Flips bit `index` and returns what it now is. Set and Clear
    /// return the value they replaced; a toggle's interesting answer is
    /// the value it landed on, and the Rust is written that way.
    pub fn toggle(&self, index: u64) -> PsResult<bool> {
        self.inner.toggle(size(index, "the index")?).map_err(|e| op_err("toggling a bit", e))
    }

    /// Bit `index`.
    pub fn get(&self, index: u64) -> PsResult<bool> {
        self.inner.get(size(index, "the index")?).map_err(|e| op_err("reading a bit", e))
    }

    /// Sets every bit from `lo` up to but not including `hi`, in one
    /// call rather than one per bit.
    pub fn set_range(&self, lo: u64, hi: u64) -> PsResult<()> {
        self.inner.set_range(size(lo, "the low bound")?, size(hi, "the high bound")?).map_err(|e| op_err("setting a range", e))
    }
}

/// Obtains the bit vector at Path holding CapacityBits bits, creating
/// it cleared when the file does not exist.
///
/// # Examples
///
/// `$bits = New-SubEthaBitVec -Path C:\ipc\bitvec -CapacityBits 1024`
#[cmdlet(verb = "New", noun = "SubEthaBitVec", alias = "New-SEBitVec", output = ["SubEtha.BitVec"])]
#[derive(Default)]
pub struct NewSubEthaBitVec {
    /// The file the bits live in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many bits it holds.
    #[param(mandatory, position = 1)]
    pub capacity_bits: u64,
}

impl Cmdlet for NewSubEthaBitVec {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BitVec::obtain(path, self.capacity_bits, false)?)
    }
}

/// Attaches to the bit vector at Path, which must exist holding
/// CapacityBits bits.
///
/// # Examples
///
/// `$bits = Open-SubEthaBitVec -Path C:\ipc\bitvec -CapacityBits 1024`
#[cmdlet(verb = "Open", noun = "SubEthaBitVec", alias = "Open-SEBitVec", output = ["SubEtha.BitVec"])]
#[derive(Default)]
pub struct OpenSubEthaBitVec {
    /// The file the bits live in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many bits it holds.
    #[param(mandatory, position = 1)]
    pub capacity_bits: u64,
}

impl Cmdlet for OpenSubEthaBitVec {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BitVec::obtain(path, self.capacity_bits, true)?)
    }
}

