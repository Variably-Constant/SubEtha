//! `SwapCell`: an `Arc` swapped atomically and read without touching its
//! reference count.
//!
//! A cell holds one `Arc<T>`. Any thread reads the value through a
//! [`Guard`], replaces it, or replaces it only while the cell still holds
//! the one it expects.
//!
//! A read records the pointer it read in a slot of its own thread's and
//! checks the cell still holds that pointer: the recorded read is a
//! reference the reader owes. A write that replaces a value pays every debt
//! on it, turning each recorded read into a reference its reader owns, and
//! only then gives up the cell's own reference. A replaced value is dropped
//! when its last reader lets go, exactly as a plain `Arc` would be. A
//! guard's drop gives its slot back with one compare-and-swap, or drops the
//! reference a writer paid it.
//!
//! Each thread's slots come in blocks one cache line in size, chained and
//! never moved or freed. A thread holding more guards at once than its
//! blocks have slots appends a block, and a finished thread's record,
//! blocks and all, goes to the next thread that needs one. Reads touch only
//! the reading thread's own slots; a write walks every thread's.
//!
//! [`SwapCellOption`] is the same cell over an `Option<Arc<T>>`.

use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Deref;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

/// A slot that holds no debt. Odd, so it never equals the address of an
/// `Arc`'s value, which is aligned at least as its reference counts are.
const FREE: usize = 1;

/// Bytes in a cache line: the size of a block of slots.
const CACHE_LINE: usize = 64;

/// Slots in a block: those that fill its cache line after the link to the
/// next block, which is one pointer wide.
const BLOCK_SLOTS: usize = (CACHE_LINE - size_of::<usize>()) / size_of::<usize>();

/// One cache line of a thread's slots, and the link to its next block.
#[repr(C, align(64))]
struct Block {
    slots: [AtomicUsize; BLOCK_SLOTS],
    next: AtomicPtr<Block>,
}

const _: () = assert!(size_of::<Block>() == CACHE_LINE);

impl Block {
    const fn new() -> Self {
        Self { slots: [const { AtomicUsize::new(FREE) }; BLOCK_SLOTS], next: AtomicPtr::new(ptr::null_mut()) }
    }
}

/// Head of the list of records, one per thread that has read a cell.
/// Records are never freed; a record a finished thread gave back is taken
/// by the next thread that needs one.
static RECORDS: AtomicPtr<Record> = AtomicPtr::new(ptr::null_mut());

/// One reading thread's slots: its first block, and more chained behind it.
struct Record {
    first: Block,
    /// Held by a thread.
    owned: AtomicBool,
    /// No thread end will give the record back: its last guard does.
    orphaned: AtomicBool,
    next: AtomicPtr<Record>,
}

impl Record {
    /// Call `f` on every slot of every block.
    fn each_slot(&self, mut f: impl FnMut(&AtomicUsize)) {
        let mut block: &Block = &self.first;
        loop {
            for slot in &block.slots {
                f(slot);
            }
            let next = block.next.load(Ordering::Acquire);
            if next.is_null() {
                return;
            }
            // SAFETY: blocks are never freed.
            block = unsafe { &*next };
        }
    }

    /// A slot holding no debt, found by the owning thread; a block is
    /// appended when every slot it has is in use.
    fn free_slot(&'static self) -> &'static AtomicUsize {
        let mut block: &'static Block = &self.first;
        loop {
            for slot in &block.slots {
                if slot.load(Ordering::Relaxed) == FREE {
                    return slot;
                }
            }
            let next = block.next.load(Ordering::Acquire);
            if next.is_null() {
                let grown: &'static Block = Box::leak(Box::new(Block::new()));
                block.next.store((grown as *const Block).cast_mut(), Ordering::Release);
                return &grown.slots[0];
            }
            // SAFETY: blocks are never freed.
            block = unsafe { &*next };
        }
    }

    /// Whether no slot holds a debt: no guard of the owning thread is alive.
    fn all_free(&self) -> bool {
        let mut free = true;
        self.each_slot(|slot| free &= slot.load(Ordering::Acquire) == FREE);
        free
    }

    /// Hand the record back for another thread to take, now or, while a
    /// guard still holds one of its slots, when the last such guard goes.
    fn release(&self) {
        self.orphaned.store(true, Ordering::Relaxed);
        if self.all_free() {
            self.owned.store(false, Ordering::Release);
        }
    }
}

/// Take a record no thread holds, or add one.
fn acquire_record() -> &'static Record {
    let mut cursor = RECORDS.load(Ordering::Acquire);
    while !cursor.is_null() {
        // SAFETY: records are never freed.
        let record = unsafe { &*cursor };
        if !record.owned.load(Ordering::Relaxed)
            && record.owned.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
        {
            record.orphaned.store(false, Ordering::Relaxed);
            return record;
        }
        cursor = record.next.load(Ordering::Acquire);
    }
    let record: &'static Record = Box::leak(Box::new(Record {
        first: Block::new(),
        owned: AtomicBool::new(true),
        orphaned: AtomicBool::new(false),
        next: AtomicPtr::new(ptr::null_mut()),
    }));
    let published = (record as *const Record).cast_mut();
    let mut head = RECORDS.load(Ordering::Relaxed);
    loop {
        record.next.store(head, Ordering::Relaxed);
        match RECORDS.compare_exchange_weak(head, published, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return record,
            Err(current) => head = current,
        }
    }
}

/// Gives the thread's record back when the thread ends.
struct GiveBack;

impl GiveBack {
    /// Reaching the thread-local is what schedules its drop at thread end.
    fn arm(&self) {}
}

impl Drop for GiveBack {
    fn drop(&mut self) {
        let record = RECORD.with(Cell::get);
        if !record.is_null() {
            // SAFETY: records are never freed.
            unsafe { &*record }.release();
            RECORD.with(|slot| slot.set(ptr::null()));
        }
    }
}

thread_local! {
    /// The thread's record, null until the thread first reads a cell. No
    /// destructor, so it reads the same while the thread is torn down.
    static RECORD: Cell<*const Record> = const { Cell::new(ptr::null()) };
    static GIVE_BACK: GiveBack = const { GiveBack };
}

/// The calling thread's record, taken on its first read.
fn thread_record() -> &'static Record {
    let current = RECORD.with(Cell::get);
    if !current.is_null() {
        // SAFETY: records are never freed.
        return unsafe { &*current };
    }
    let record = acquire_record();
    RECORD.with(|slot| slot.set(record));
    // A thread already being torn down has no thread end left to give the
    // record back at: its last guard gives it back instead, and a later
    // read on the thread takes a record of its own.
    if GIVE_BACK.try_with(GiveBack::arm).is_err() {
        record.orphaned.store(true, Ordering::Relaxed);
        RECORD.with(|slot| slot.set(ptr::null()));
    }
    record
}

/// Pay every recorded read of `value`, which a write has just taken out of
/// a cell: each becomes a reference its reader owns. The reference is made
/// before the slot is freed, so a reader never finds its debt paid ahead of
/// the count that pays it.
///
/// # Safety
/// `value` came from `Arc::<T>::into_raw`, and the caller still holds the
/// cell's reference to it.
unsafe fn pay_debts<T>(value: *const T) {
    let address = value as usize;
    let mut cursor = RECORDS.load(Ordering::Acquire);
    while !cursor.is_null() {
        // SAFETY: records are never freed.
        let record = unsafe { &*cursor };
        record.each_slot(|slot| {
            if slot.load(Ordering::SeqCst) == address {
                // SAFETY: the caller's reference keeps the count above zero.
                unsafe { Arc::increment_strong_count(value) };
                if slot.compare_exchange(address, FREE, Ordering::SeqCst, Ordering::Relaxed).is_err() {
                    // The reader took its debt back first; the reference
                    // made for it goes.
                    // SAFETY: the reference incremented just above.
                    unsafe { Arc::decrement_strong_count(value) };
                }
            }
        });
        cursor = record.next.load(Ordering::Acquire);
    }
}

/// What a guard holds its value by.
enum Hold {
    /// A recorded read in this slot, owed until a writer pays it.
    Owed(&'static AtomicUsize),
    /// A reference of its own, paid before the read was confirmed.
    Owned,
}

/// A read of a cell's value. Holding it keeps the value alive; it is
/// released on the thread that took it.
pub struct Guard<'a, T> {
    value: *const T,
    hold: Hold,
    record: &'static Record,
    _cell: PhantomData<(&'a T, *const ())>,
}

impl<T> Guard<'_, T> {
    /// A reference-counted handle on the value, which outlives the guard.
    pub fn to_arc(&self) -> Arc<T> {
        // SAFETY: the guard's debt or its own reference keeps the value's
        // count above zero.
        unsafe {
            Arc::increment_strong_count(self.value);
            Arc::from_raw(self.value)
        }
    }

    /// Whether `other` is this very value.
    pub fn ptr_eq(&self, other: &Arc<T>) -> bool {
        ptr::eq(self.value, Arc::as_ptr(other))
    }
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: the guard keeps the value alive while it is held.
        unsafe { &*self.value }
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        let paid = match self.hold {
            Hold::Owed(slot) => {
                slot.compare_exchange(self.value as usize, FREE, Ordering::AcqRel, Ordering::Acquire).is_err()
            }
            Hold::Owned => true,
        };
        if paid {
            // SAFETY: a writer paid this read a reference, or it owned one.
            drop(unsafe { Arc::from_raw(self.value) });
        }
        if self.record.orphaned.load(Ordering::Relaxed) && self.record.all_free() {
            self.record.owned.store(false, Ordering::Release);
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Guard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// The pointer both cell types keep: an `Arc<T>` turned into a raw
/// pointer, null for an empty option.
struct Raw<T> {
    ptr: AtomicPtr<T>,
    _owns: PhantomData<Arc<T>>,
}

impl<T> Raw<T> {
    fn from_option(value: Option<Arc<T>>) -> Self {
        Self { ptr: AtomicPtr::new(into_raw(value)), _owns: PhantomData }
    }

    fn load(&self) -> Option<Guard<'_, T>> {
        let record = thread_record();
        let mut value = self.ptr.load(Ordering::Acquire);
        loop {
            if value.is_null() {
                return None;
            }
            let slot = record.free_slot();
            slot.store(value as usize, Ordering::SeqCst);
            let current = self.ptr.load(Ordering::SeqCst);
            if current == value {
                return Some(Guard { value, hold: Hold::Owed(slot), record, _cell: PhantomData });
            }
            // The cell moved on before the read was recorded where a writer
            // would see it: take the debt back and read again, unless a
            // writer paid it, which makes the value this read's to keep.
            if slot.compare_exchange(value as usize, FREE, Ordering::AcqRel, Ordering::Acquire).is_err() {
                return Some(Guard { value, hold: Hold::Owned, record, _cell: PhantomData });
            }
            value = current;
        }
    }

    fn swap(&self, value: Option<Arc<T>>) -> Option<Arc<T>> {
        let old = self.ptr.swap(into_raw(value), Ordering::SeqCst);
        if old.is_null() {
            return None;
        }
        // SAFETY: `old` carries the cell's reference, which goes to the
        // caller once its readers own theirs.
        unsafe {
            pay_debts(old);
            Some(Arc::from_raw(old))
        }
    }

    fn compare_and_set(&self, current: *const T, new: Option<Arc<T>>) -> Result<Option<Arc<T>>, Option<Arc<T>>> {
        let new = into_raw(new);
        match self.ptr.compare_exchange(current.cast_mut(), new, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(old) => {
                if old.is_null() {
                    return Ok(None);
                }
                // SAFETY: `old` carries the cell's reference.
                unsafe {
                    pay_debts(old);
                    Ok(Some(Arc::from_raw(old)))
                }
            }
            // The cell holds another value, so `new` was never published
            // and goes back to the caller.
            // SAFETY: `new` came from `into_raw` above.
            Err(_in_place) => Err(unsafe { from_raw(new) }),
        }
    }
}

impl<T> Drop for Raw<T> {
    fn drop(&mut self) {
        // No guard outlives the cell it borrows, so no debt on this cell's
        // value is left to pay.
        // SAFETY: the pointer holds the cell's reference.
        drop(unsafe { from_raw(*self.ptr.get_mut()) });
    }
}

fn into_raw<T>(value: Option<Arc<T>>) -> *mut T {
    match value {
        Some(value) => Arc::into_raw(value).cast_mut(),
        None => ptr::null_mut(),
    }
}

/// # Safety
/// `value` is null or came from `Arc::<T>::into_raw` with a reference the
/// caller hands over.
unsafe fn from_raw<T>(value: *mut T) -> Option<Arc<T>> {
    if value.is_null() { None } else { Some(unsafe { Arc::from_raw(value) }) }
}

/// An `Arc<T>` swapped atomically and read without touching its reference
/// count. See the module docs.
pub struct SwapCell<T> {
    raw: Raw<T>,
}

// SAFETY: the cell hands its `Arc<T>` between threads, and a write pays or
// drops references on whichever thread makes it, as an `Arc<T>` shared
// between threads does. A cell of any other value type is neither, so
// every write to it is made on the one thread that has it.
unsafe impl<T: Send + Sync> Send for SwapCell<T> {}
unsafe impl<T: Send + Sync> Sync for SwapCell<T> {}

impl<T> SwapCell<T> {
    /// A cell holding `value`.
    pub fn new(value: T) -> Self {
        Self::from_arc(Arc::new(value))
    }

    /// A cell holding `value`.
    pub fn from_arc(value: Arc<T>) -> Self {
        Self { raw: Raw::from_option(Some(value)) }
    }

    /// Read the value: a slot of this thread's, no reference count change.
    pub fn load(&self) -> Guard<'_, T> {
        match self.raw.load() {
            Some(guard) => guard,
            None => unreachable!("a SwapCell always holds a value"),
        }
    }

    /// The value as an `Arc` of its own, kept past any guard.
    pub fn load_full(&self) -> Arc<T> {
        self.load().to_arc()
    }

    /// Put `value` in the cell. The value it replaces is dropped when its
    /// last reader lets go.
    pub fn store(&self, value: Arc<T>) {
        drop(self.swap(value));
    }

    /// Put `value` in the cell and return the value it replaces, with the
    /// cell's reference: once every reader of it has let go, the returned
    /// `Arc` is its last.
    pub fn swap(&self, value: Arc<T>) -> Arc<T> {
        match self.raw.swap(Some(value)) {
            Some(old) => old,
            None => unreachable!("a SwapCell always holds a value"),
        }
    }

    /// Put `new` in the cell only while it holds `current`, the very value
    /// and not an equal one. Hands `new` back when the cell holds something
    /// else.
    pub fn compare_and_set(&self, current: &Arc<T>, new: Arc<T>) -> Result<(), Arc<T>> {
        match self.raw.compare_and_set(Arc::as_ptr(current), Some(new)) {
            Ok(_replaced) => Ok(()),
            Err(Some(new)) => Err(new),
            Err(None) => unreachable!("the value handed back is the one passed in"),
        }
    }

    /// Replace the value with `update` applied to it, applying it again to
    /// the newer value whenever another write lands between the read and
    /// the replacement. Returns the value replaced.
    pub fn rcu(&self, mut update: impl FnMut(&T) -> T) -> Arc<T> {
        loop {
            let current = self.load_full();
            if self.compare_and_set(&current, Arc::new(update(&current))).is_ok() {
                return current;
            }
        }
    }
}

impl<T: Default> Default for SwapCell<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<Arc<T>> for SwapCell<T> {
    fn from(value: Arc<T>) -> Self {
        Self::from_arc(value)
    }
}

impl<T: fmt::Debug> fmt::Debug for SwapCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SwapCell").field(&*self.load()).finish()
    }
}

/// An `Option<Arc<T>>` swapped atomically, read as [`SwapCell`] reads.
pub struct SwapCellOption<T> {
    raw: Raw<T>,
}

// SAFETY: as for `SwapCell`.
unsafe impl<T: Send + Sync> Send for SwapCellOption<T> {}
unsafe impl<T: Send + Sync> Sync for SwapCellOption<T> {}

impl<T> SwapCellOption<T> {
    /// A cell holding `value`.
    pub fn new(value: Option<Arc<T>>) -> Self {
        Self { raw: Raw::from_option(value) }
    }

    /// A cell holding nothing.
    pub fn empty() -> Self {
        Self::new(None)
    }

    /// Read the value, if the cell holds one.
    pub fn load(&self) -> Option<Guard<'_, T>> {
        self.raw.load()
    }

    /// The value as an `Arc` of its own, if the cell holds one.
    pub fn load_full(&self) -> Option<Arc<T>> {
        self.raw.load().map(|guard| guard.to_arc())
    }

    /// Put `value` in the cell.
    pub fn store(&self, value: Option<Arc<T>>) {
        drop(self.raw.swap(value));
    }

    /// Put `value` in the cell and return the value it replaces, with the
    /// cell's reference.
    pub fn swap(&self, value: Option<Arc<T>>) -> Option<Arc<T>> {
        self.raw.swap(value)
    }

    /// Empty the cell and return what it held.
    pub fn take(&self) -> Option<Arc<T>> {
        self.raw.swap(None)
    }

    /// Put `new` in the cell only while it holds `current`: the very value,
    /// or nothing for `None`. Hands `new` back otherwise.
    pub fn compare_and_set(&self, current: Option<&Arc<T>>, new: Option<Arc<T>>) -> Result<(), Option<Arc<T>>> {
        let current = match current {
            Some(value) => Arc::as_ptr(value),
            None => ptr::null(),
        };
        match self.raw.compare_and_set(current, new) {
            Ok(_replaced) => Ok(()),
            Err(new) => Err(new),
        }
    }
}

impl<T> Default for SwapCellOption<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: fmt::Debug> fmt::Debug for SwapCellOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.load() {
            Some(value) => f.debug_tuple("SwapCellOption").field(&*value).finish(),
            None => f.write_str("SwapCellOption(None)"),
        }
    }
}

/// How many records exist, held or free. For tests that check a finished
/// thread's record is reused.
#[cfg(test)]
fn record_count() -> usize {
    let mut count = 0;
    let mut cursor = RECORDS.load(Ordering::Acquire);
    while !cursor.is_null() {
        count += 1;
        // SAFETY: records are never freed.
        cursor = unsafe { (*cursor).next.load(Ordering::Acquire) };
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// A value that counts its drops.
    struct Counted(Arc<AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counted(drops: &Arc<AtomicUsize>) -> Arc<Counted> {
        Arc::new(Counted(Arc::clone(drops)))
    }

    #[test]
    fn a_read_sees_the_value_written_last() {
        let cell = SwapCell::new(1u32);
        assert_eq!(*cell.load(), 1);
        cell.store(Arc::new(2));
        assert_eq!(*cell.load(), 2);
        assert_eq!(*cell.swap(Arc::new(3)), 2, "swap returns the value it replaced");
        assert_eq!(*cell.load_full(), 3);
        let current = cell.load_full();
        assert!(cell.compare_and_set(&current, Arc::new(4)).is_ok(), "the expected value was in place");
        match cell.compare_and_set(&current, Arc::new(5)) {
            Ok(()) => panic!("a value no longer in the cell was taken as current"),
            Err(back) => assert_eq!(*back, 5, "the refused value is handed back"),
        }
        assert_eq!(*cell.load(), 4);
        assert_eq!(*cell.rcu(|value| value + 10), 4, "rcu returns the value it replaced");
        assert_eq!(*cell.load(), 14);

        let option = SwapCellOption::empty();
        assert!(option.load().is_none());
        assert!(option.compare_and_set(None, Some(Arc::new(7u32))).is_ok());
        assert_eq!(option.load_full().as_deref(), Some(&7));
        assert!(option.compare_and_set(None, Some(Arc::new(8))).is_err(), "the cell was not empty");
        assert_eq!(option.take().as_deref(), Some(&7));
        assert!(option.load().is_none());
    }

    /// A replaced value a guard on this thread reads is dropped the moment
    /// that guard lets go, not before and not later.
    #[test]
    fn a_replaced_value_is_dropped_when_its_last_reader_lets_go() {
        let drops = Arc::new(AtomicUsize::new(0));
        let cell = SwapCell::from_arc(counted(&drops));
        let guard = cell.load();
        cell.store(Arc::new(Counted(Arc::new(AtomicUsize::new(0)))));
        assert_eq!(drops.load(Ordering::SeqCst), 0, "the value was dropped under the guard reading it");
        drop(guard);
        assert_eq!(drops.load(Ordering::SeqCst), 1, "the value outlived its last reader");
    }

    /// The same across threads: a guard on another thread keeps the
    /// replaced value alive until it goes, and no longer.
    #[test]
    fn a_reader_on_another_thread_keeps_a_replaced_value_until_it_lets_go() {
        let drops = Arc::new(AtomicUsize::new(0));
        let cell = Arc::new(SwapCell::from_arc(counted(&drops)));
        let (pinned_tx, pinned_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let reading = Arc::clone(&cell);
        let reader = std::thread::spawn(move || {
            let guard = reading.load();
            pinned_tx.send(()).expect("the test is waiting");
            release_rx.recv().expect("the test releases the reader");
            drop(guard);
        });
        pinned_rx.recv().expect("the reader reads");
        cell.store(Arc::new(Counted(Arc::new(AtomicUsize::new(0)))));
        assert_eq!(drops.load(Ordering::SeqCst), 0, "the value was dropped under another thread's guard");
        release_tx.send(()).expect("the reader waits");
        reader.join().expect("the reader");
        assert_eq!(drops.load(Ordering::SeqCst), 1, "the value outlived its last reader");
    }

    /// A swap returns the cell's own reference: with no reader holding the
    /// old value it is the only one, and with one holding it the reader's
    /// guard owns the other until it lets go.
    #[test]
    fn a_swap_hands_back_the_cells_reference() {
        let cell = Arc::new(SwapCell::new(1u32));
        let old = cell.swap(Arc::new(2));
        assert_eq!(Arc::strong_count(&old), 1, "the old value has a reference beside the caller's");

        let (pinned_tx, pinned_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let reading = Arc::clone(&cell);
        let reader = std::thread::spawn(move || {
            let guard = reading.load();
            pinned_tx.send(()).expect("the test is waiting");
            release_rx.recv().expect("the test releases the reader");
            drop(guard);
        });
        pinned_rx.recv().expect("the reader reads");
        let old = cell.swap(Arc::new(3));
        assert_eq!(Arc::strong_count(&old), 2, "the reader's guard was not paid a reference of its own");
        release_tx.send(()).expect("the reader waits");
        reader.join().expect("the reader");
        assert_eq!(Arc::strong_count(&old), 1, "the reader's guard kept its reference after letting go");
    }

    /// One thread holding far more guards than one block of slots: every
    /// value stays alive while its guard is held and goes once it is not.
    #[test]
    fn a_thread_holding_more_guards_than_a_block_keeps_every_value() {
        let held = BLOCK_SLOTS * 3;
        let drops = Arc::new(AtomicUsize::new(0));
        let cells: Vec<SwapCell<Counted>> = (0..held).map(|_| SwapCell::from_arc(counted(&drops))).collect();
        let guards: Vec<_> = cells.iter().map(|cell| cell.load()).collect();
        for cell in &cells {
            cell.store(Arc::new(Counted(Arc::new(AtomicUsize::new(0)))));
        }
        assert_eq!(drops.load(Ordering::SeqCst), 0, "a value was dropped under a guard holding it");
        drop(guards);
        assert_eq!(drops.load(Ordering::SeqCst), held, "a value outlived its last reader");
    }

    /// Readers on several threads while a writer replaces the value: every
    /// value written is dropped exactly once, the last with the cell.
    #[test]
    fn every_value_written_is_dropped_once() {
        const WRITES: usize = 2_000;
        let drops = Arc::new(AtomicUsize::new(0));
        let cell = SwapCell::from_arc(counted(&drops));
        let done = AtomicBool::new(false);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    while !done.load(Ordering::Relaxed) {
                        let guard = cell.load();
                        assert!(guard.0.load(Ordering::SeqCst) <= WRITES + 1);
                        let owned = cell.load_full();
                        drop(guard);
                        drop(owned);
                    }
                });
            }
            for _ in 0..WRITES {
                cell.store(counted(&drops));
            }
            done.store(true, Ordering::Relaxed);
        });
        assert_eq!(drops.load(Ordering::SeqCst), WRITES, "a replaced value was dropped twice or not at all");
        drop(cell);
        assert_eq!(drops.load(Ordering::SeqCst), WRITES + 1, "the cell's last value was not dropped with it");
    }

    /// A thread that read a cell and ended gives its record back for the
    /// next thread to take.
    #[test]
    fn a_finished_thread_gives_its_record_back() {
        let cell = Arc::new(SwapCell::new(0u32));
        let warm = Arc::clone(&cell);
        std::thread::spawn(move || drop(warm.load())).join().expect("a reader");
        let before = record_count();
        for _ in 0..64 {
            let reading = Arc::clone(&cell);
            std::thread::spawn(move || drop(reading.load())).join().expect("a reader");
        }
        let grown = record_count() - before;
        assert!(grown < 64, "{grown} records were added for 64 threads run one after another");
    }
}
