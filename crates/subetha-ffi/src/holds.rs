//! The slot table behind every hold a caller takes and gives back: a
//! lock's read or write hold, a semaphore's permit, an epoch pin, an
//! epoch ticket.
//!
//! # Why these are not handles
//!
//! A handle is the right shape for a ring or a map, made once and closed
//! at the end of a run. It is the wrong shape for a lock, taken and given
//! back in a loop, and the cost is not small: issuing one allocates on
//! the heap and takes two mutexes, closing one takes a third and runs a
//! process-wide barrier to prove no call is inside it. Measured on the
//! lock, that is about 563 ns against 3 ns of direct work, where every
//! other family in the ABI pays about 22 ns of boundary.
//!
//! A token costs a compare-exchange each way and allocates nothing. What
//! it must not cost is the guarantee the handle gave: a handle refused a
//! double close exactly, and a caller that unlocks twice must still be
//! refused rather than releasing a hold someone else is relying on.
//!
//! # How a token stays exact
//!
//! Each slot carries a generation that steps on every release. A token
//! names a slot and the generation it was claimed at, so a token released
//! twice fails its second check against a generation that has moved on,
//! and a token from an earlier hold of the same slot fails the same way.
//! The check is one atomic load and one compare-exchange; nothing is
//! searched and nothing is allocated.
//!
//! # Why it grows
//!
//! Some of the primitives beneath cap what a table would need - one
//! writer, `max_permits` permits, the epoch table's pin capacity - and
//! some do not: a read lock counts its readers and sets no ceiling. A
//! fixed table would therefore invent a limit that is not the lock's, and
//! refusing a read a caller is entitled to is a worse failure than the
//! cost this exists to remove.
//!
//! So it grows, in blocks that are appended and never moved. A token
//! names an absolute slot index, and appending leaves every existing
//! index meaning what it did, so a hold taken before a growth is
//! unaffected by one. Nothing is copied, which matters because copying a
//! slot's word while another thread was mid-compare-exchange on it would
//! lose that claim. Growth takes a lock and claiming does not; growth
//! happens at most once per doubling of the high-water mark of concurrent
//! holds, so a steady state never reaches it.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

/// A slot's word: the generation in the high half, and in the low half
/// the kind the hold was taken as, with bit 0 saying it is claimed.
///
/// The generation lives in the same word as the claimed bit so a release
/// can free the slot and step the generation in one compare-exchange. Two
/// writes would let a second release land between them and see a slot
/// that is free at the generation it was expecting.
const CLAIMED: u64 = 1;
const KIND_SHIFT: u32 = 1;
const KIND_MASK: u64 = 0x7F << KIND_SHIFT;
const GENERATION_SHIFT: u32 = 32;

#[inline]
fn generation_of(word: u64) -> u32 {
    (word >> GENERATION_SHIFT) as u32
}

#[inline]
fn is_claimed(word: u64) -> bool {
    word & CLAIMED != 0
}

#[inline]
fn kind_of(word: u64) -> u32 {
    ((word & KIND_MASK) >> KIND_SHIFT) as u32
}

#[inline]
fn word(generation: u32, kind: u32, claimed: bool) -> u64 {
    ((generation as u64) << GENERATION_SHIFT)
        | ((u64::from(kind) << KIND_SHIFT) & KIND_MASK)
        | u64::from(claimed)
}

/// Set on every token, and never on a handle: a handle's index counts up
/// from zero, so the top bit of one is always clear. A value handed to
/// the wrong one of the two is refused on this bit rather than decoded as
/// a plausible member of the other family.
pub(crate) const TOKEN_TAG: u64 = 1 << 63;

/// Bit 62 belongs to the family that issued the token; the lock says read
/// or write with it. The slot index has the thirty bits below that, and a
/// table refuses to grow past `MAX_SLOTS` rather than wrap into them.
pub(crate) const MAX_SLOTS: usize = 1 << 30;
const SLOT_SHIFT: u32 = 32;
const SLOT_MASK: u64 = ((MAX_SLOTS as u64) - 1) << SLOT_SHIFT;

/// Whether `value` is a hold token rather than a handle.
#[inline]
pub(crate) fn is_token(value: u64) -> bool {
    value & TOKEN_TAG != 0
}

/// The slot a token names, for a family carrying per-hold state of its own
/// in an array beside the table. The tag and the family bit are not part
/// of the index and are masked off here rather than at each caller.
#[inline]
pub(crate) fn slot_of(token: u64) -> usize {
    ((token & SLOT_MASK) >> SLOT_SHIFT) as usize
}

/// The value a caller carries between taking a hold and giving it back.
/// It names a slot and the generation that slot stood at, which is what
/// makes a stale one recognizable rather than merely unlikely.
#[inline]
pub(crate) fn token(slot: usize, generation: u32) -> u64 {
    TOKEN_TAG | (((slot as u64) << SLOT_SHIFT) & SLOT_MASK) | u64::from(generation)
}

#[inline]
fn split(token: u64) -> (usize, u32) {
    (((token & SLOT_MASK) >> SLOT_SHIFT) as usize, token as u32)
}

/// Why a release was refused, so each family can say it in its own words
/// rather than every one of them inventing the same three codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReleaseError {
    /// The token names no slot in this table.
    NoSuchSlot,
    /// The slot is not held: either it never was, or this token has
    /// already been given back once.
    NotHeld,
    /// The slot is held, but at a later generation - so this token
    /// belongs to a hold that ended and the slot has been taken since.
    Stale,
    /// The value carries no token tag, so it is a handle or a forgery
    /// rather than a hold this table ever issued.
    NotAToken,
}

/// Slots per block. Blocks are appended and never moved, so a slot's
/// index means the same thing for the life of the table.
const BLOCK: usize = 64;

pub(crate) struct HoldTable {
    blocks: ArcSwap<Vec<Arc<Box<[AtomicU64]>>>>,
    /// Serializes growth. Claiming and releasing never take it.
    growing: Mutex<()>,
    /// The most slots this table may ever hold, when the primitive
    /// beneath has a limit of its own - one writer, `max_permits`
    /// permits, the epoch table's pin capacity. `None` where it has none,
    /// as a read lock does: it counts its readers and sets no ceiling, so
    /// a table that stopped growing would refuse a read the lock itself
    /// would have allowed.
    ceiling: Option<usize>,
    /// Where the next claim starts looking. Only a hint: a claim is
    /// correct from any starting point, and this keeps a table whose
    /// early slots are held from walking them every time.
    hint: AtomicUsize,
}

fn fresh_block() -> Arc<Box<[AtomicU64]>> {
    let mut slots = Vec::with_capacity(BLOCK);
    for _ in 0..BLOCK {
        // Generation 1 rather than 0, so a token is never all zeroes and
        // a caller passing an uninitialized variable is refused rather
        // than resolving to slot 0's first hold.
        slots.push(AtomicU64::new(word(1, 0, false)));
    }
    Arc::new(slots.into_boxed_slice())
}

impl HoldTable {
    /// A table for a primitive that limits how many holds can stand at
    /// once. It never grows past `ceiling`, because reaching it means the
    /// primitive would have refused anyway.
    pub(crate) fn bounded(ceiling: usize) -> Self {
        Self::build(Some(ceiling.max(1)))
    }

    /// A table for a primitive that sets no such limit, which grows to
    /// whatever the caller actually holds at once.
    pub(crate) fn unbounded() -> Self {
        Self::build(None)
    }

    fn build(ceiling: Option<usize>) -> Self {
        Self {
            blocks: ArcSwap::from_pointee(vec![fresh_block()]),
            growing: Mutex::new(()),
            ceiling,
            hint: AtomicUsize::new(0),
        }
    }

    /// Slots a caller may hold at once: the primitive's own ceiling when
    /// it has one, and otherwise what the table has grown to, which is
    /// the high-water mark of concurrent holds rounded up to a block.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        let len = self.blocks.load().len() * BLOCK;
        self.ceiling.map_or(len, |c| c.min(len))
    }

    /// Holds outstanding right now. A diagnostic: it races every claim
    /// and release in this process.
    pub(crate) fn live(&self) -> usize {
        self.blocks
            .load()
            .iter()
            .flat_map(|b| b.iter())
            .filter(|s| is_claimed(s.load(Ordering::Acquire)))
            .count()
    }

    /// Take a slot for a hold of `kind`, or `None` when a bounded table
    /// is at its ceiling - which means the primitive beneath is at its
    /// own limit and would have refused too.
    pub(crate) fn claim(&self, kind: u32) -> Option<u64> {
        loop {
            let blocks = self.blocks.load();
            let len = blocks.len() * BLOCK;
            // A block holds sixty-four slots however small the ceiling
            // is, so the ceiling bounds the scan: a table built for one
            // writer offers one slot, not a block's worth.
            let usable = self.ceiling.map_or(len, |c| c.min(len));
            let start = self.hint.load(Ordering::Relaxed) % usable;
            for step in 0..usable {
                let i = (start + step) % usable;
                let cell = &blocks[i / BLOCK][i % BLOCK];
                let mut seen = cell.load(Ordering::Acquire);
                // Retry this slot while it is free and someone else keeps
                // winning it; move on as soon as it is genuinely held.
                while !is_claimed(seen) {
                    let generation = generation_of(seen);
                    match cell.compare_exchange_weak(
                        seen,
                        word(generation, kind, true),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            self.hint.store(i + 1, Ordering::Relaxed);
                            return Some(token(i, generation));
                        }
                        Err(again) => seen = again,
                    }
                }
            }
            if let Some(ceiling) = self.ceiling
                && len >= ceiling
            {
                return None;
            }
            // A token carries the slot index in thirty bits, so growing
            // past this would issue an index that decodes as another one.
            if len >= MAX_SLOTS {
                return None;
            }
            self.grow(len);
        }
    }

    /// Append a block, unless another thread already did while this one
    /// was scanning. `seen` is the length that was full, so a table that
    /// has grown since is left alone and the caller rescans it.
    fn grow(&self, seen: usize) {
        let _serialized = self.growing.lock();
        let blocks = self.blocks.load_full();
        if blocks.len() * BLOCK > seen {
            return;
        }
        let mut grown = Vec::with_capacity(blocks.len() + 1);
        // The existing blocks come across by pointer, so every slot keeps
        // its address and its word: a hold taken before this growth is
        // untouched by it, and a claim racing it cannot lose its
        // compare-exchange to a copy.
        grown.extend(blocks.iter().cloned());
        grown.push(fresh_block());
        self.blocks.store(Arc::new(grown));
    }

    /// Give `token`'s slot back, and report the kind it was held as so
    /// the caller knows which release to run on the primitive.
    ///
    /// The generation steps here, which is what makes this token unusable
    /// afterwards.
    pub(crate) fn release(&self, token: u64) -> Result<u32, ReleaseError> {
        if !is_token(token) {
            return Err(ReleaseError::NotAToken);
        }
        let (slot, generation) = split(token);
        let blocks = self.blocks.load();
        let Some(block) = blocks.get(slot / BLOCK) else {
            return Err(ReleaseError::NoSuchSlot);
        };
        let cell = &block[slot % BLOCK];
        let mut seen = cell.load(Ordering::Acquire);
        loop {
            if !is_claimed(seen) {
                return Err(ReleaseError::NotHeld);
            }
            if generation_of(seen) != generation {
                return Err(ReleaseError::Stale);
            }
            let kind = kind_of(seen);
            match cell.compare_exchange_weak(
                seen,
                word(generation.wrapping_add(1), 0, false),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(kind),
                Err(again) => seen = again,
            }
        }
    }

    /// Take every live hold and report what each was held as, leaving the
    /// table empty.
    ///
    /// A handle being destroyed with holds outstanding is what this is
    /// for. The primitive beneath outlives nothing here: dropping the
    /// object drops the lock or the semaphore with it, so a hold nobody
    /// gave back would leave the file saying it is held with nothing left
    /// able to release it - a wedge every other process would see. The
    /// caller gives each one back before it lets the primitive go.
    /// Each entry is the slot's index and what it was held as: the index
    /// because a caller may keep state of its own beside the table, keyed
    /// the way a token's slot keys it.
    pub(crate) fn drain(&self) -> Vec<(usize, u32)> {
        let blocks = self.blocks.load();
        let mut taken = Vec::new();
        for (b, block) in blocks.iter().enumerate() {
            for (i, cell) in block.iter().enumerate() {
                let mut seen = cell.load(Ordering::Acquire);
                while is_claimed(seen) {
                    let next = word(generation_of(seen).wrapping_add(1), 0, false);
                    match cell.compare_exchange_weak(seen, next, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            taken.push((b * BLOCK + i, kind_of(seen)));
                            break;
                        }
                        Err(again) => seen = again,
                    }
                }
            }
        }
        taken
    }

    /// What `token` is held as, without giving it back. It refuses for the
    /// same reasons `release` does and says which, so a caller that passed
    /// a handle is told that rather than that its hold has ended.
    pub(crate) fn kind(&self, token: u64) -> Result<u32, ReleaseError> {
        if !is_token(token) {
            return Err(ReleaseError::NotAToken);
        }
        let (slot, generation) = split(token);
        let blocks = self.blocks.load();
        let Some(block) = blocks.get(slot / BLOCK) else {
            return Err(ReleaseError::NoSuchSlot);
        };
        let seen = block[slot % BLOCK].load(Ordering::Acquire);
        if !is_claimed(seen) {
            return Err(ReleaseError::NotHeld);
        }
        if generation_of(seen) != generation {
            return Err(ReleaseError::Stale);
        }
        Ok(kind_of(seen))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_is_good_once_and_names_the_kind_it_was_taken_as() {
        let table = HoldTable::bounded(2);
        let a = table.claim(3).expect("a free slot");
        assert_eq!(table.kind(a).expect("the hold is live"), 3, "the kind comes back");
        assert_eq!(table.live(), 1);

        assert_eq!(table.release(a), Ok(3));
        assert_eq!(table.live(), 0);
        // The second release is refused rather than freeing a slot the
        // next holder may already be relying on.
        assert_eq!(table.release(a), Err(ReleaseError::NotHeld));
        assert_eq!(table.kind(a).unwrap_err(), ReleaseError::NotHeld, "a spent token names no hold");
    }

    /// A handle encodes an index and a generation in the same two halves a
    /// token does, so without the tag every handle would name a plausible
    /// slot here - handle 0x1 is index 0 generation 1, and slot 0 is held
    /// at generation 1 while this runs.
    #[test]
    fn a_handle_shaped_value_is_refused_rather_than_naming_a_slot() {
        let table = HoldTable::bounded(2);
        let held = table.claim(5).expect("a free slot");
        assert!(is_token(held), "a token carries the tag");

        for handle in [0u64, 1, 0x0000_0001_0000_0001, !TOKEN_TAG] {
            assert!(!is_token(handle), "{handle:#x} is not tagged");
            assert_eq!(
                table.release(handle).unwrap_err(),
                ReleaseError::NotAToken,
                "release({handle:#x}) must refuse an untagged value",
            );
            assert_eq!(
                table.kind(handle).unwrap_err(),
                ReleaseError::NotAToken,
                "kind({handle:#x}) must refuse an untagged value",
            );
        }
        assert_eq!(table.live(), 1, "none of them released the live hold");
        assert_eq!(table.release(held).expect("the real token still works"), 5);
    }

    #[test]
    fn a_token_from_an_earlier_hold_of_the_same_slot_is_refused() {
        let table = HoldTable::bounded(1);
        let first = table.claim(1).expect("the only slot");
        table.release(first).expect("released");
        let second = table.claim(1).expect("the slot comes back");
        assert_ne!(first, second, "the generation moved, so the token differs");

        // This is the case a bare index could not catch: the slot is held
        // again, by someone else, and the old token names it.
        assert_eq!(table.release(first), Err(ReleaseError::Stale));
        assert_eq!(table.kind(first).unwrap_err(), ReleaseError::Stale);
        assert_eq!(table.release(second), Ok(1), "the live token still works");
    }

    /// A bounded table stops at the ceiling the primitive gave it: one
    /// writer means one hold, and the refusal stands in for a refusal the
    /// lock itself would have made.
    #[test]
    fn a_bounded_table_stops_at_its_ceiling() {
        let table = HoldTable::bounded(1);
        let only = table.claim(7).expect("the one slot");
        assert!(table.claim(7).is_none(), "a ceiling of one holds one");
        table.release(only).expect("back");
        assert!(table.claim(7).is_some(), "and hands it out again");
    }

    /// An unbounded table grows instead of refusing, because a read lock
    /// counts its readers and sets no ceiling - a table that stopped
    /// would refuse a read the lock itself allows.
    #[test]
    fn an_unbounded_table_grows_past_a_block_and_keeps_every_token_good() {
        let table = HoldTable::unbounded();
        let first_capacity = table.capacity();
        let mut held = Vec::new();
        for _ in 0..first_capacity * 3 {
            held.push(table.claim(1).expect("an unbounded table never refuses"));
        }
        assert!(table.capacity() > first_capacity, "it grew rather than refusing");
        assert_eq!(table.live(), held.len());
        // Tokens taken before a growth still name their own slots: the
        // blocks were appended, so no index changed meaning.
        for t in &held {
            assert_eq!(table.kind(*t).expect("the hold survived the growth"), 1);
        }
        for t in held {
            table.release(t).expect("every token is still good");
        }
        assert_eq!(table.live(), 0);
    }

    #[test]
    fn a_full_table_refuses_and_a_forged_token_names_nothing() {
        let table = HoldTable::bounded(2);
        let a = table.claim(0).expect("first");
        let b = table.claim(0).expect("second");
        assert!(table.claim(0).is_none(), "no third slot");
        assert_eq!(table.release(token(9999, 1)), Err(ReleaseError::NoSuchSlot));
        // An all-zeroes token - a caller's uninitialized variable - carries
        // no tag, so it is refused before any slot is read and whatever
        // slot 0 happens to hold is irrelevant.
        assert_eq!(
            table.release(0).unwrap_err(),
            ReleaseError::NotAToken,
            "an unset token releases nothing",
        );
        table.release(a).expect("first back");
        table.release(b).expect("second back");
        assert!(table.claim(0).is_some(), "the slots are reusable");
    }

    /// Many threads claiming and releasing at once: a slot must never be
    /// held by two of them, which a claim that raced its compare-exchange
    /// would allow.
    #[test]
    fn concurrent_claims_never_hand_one_slot_to_two_holders() {
        use std::sync::atomic::AtomicU32;
        use std::sync::Arc;

        let table = Arc::new(HoldTable::bounded(8));
        assert_eq!(table.capacity(), 8, "a ceiling of eight is eight, not a block");
        let held: Arc<Vec<AtomicU32>> = Arc::new((0..8).map(|_| AtomicU32::new(0)).collect());
        let mut threads = Vec::new();
        for _ in 0..8 {
            let table = Arc::clone(&table);
            let held = Arc::clone(&held);
            threads.push(std::thread::spawn(move || {
                for _ in 0..20_000 {
                    if let Some(t) = table.claim(1) {
                        let (slot, _) = split(t);
                        let before = held[slot].fetch_add(1, Ordering::AcqRel);
                        assert_eq!(before, 0, "slot {slot} was handed to two holders");
                        held[slot].fetch_sub(1, Ordering::AcqRel);
                        table.release(t).expect("the token this thread holds");
                    }
                }
            }));
        }
        for t in threads {
            t.join().expect("a claiming thread finishes");
        }
        assert_eq!(table.live(), 0, "every hold was given back");
    }
}
