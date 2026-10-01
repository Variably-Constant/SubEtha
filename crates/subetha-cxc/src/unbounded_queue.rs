//! `UnboundedQueue`: a first-in first-out queue of values, with no bound on
//! its length, that any number of threads push to and pop from with no
//! lock.
//!
//! Values sit in blocks of `BLOCK_SLOTS` slots linked front to back. Every
//! position in the queue has a number. A push takes the next number at the
//! tail with a compare-and-swap, writes its value into that position's slot
//! and marks the slot written; a pop takes the next number at the head the
//! same way and reads the value out, first waiting for the mark when the
//! push that took the position is still writing. The numbering runs one
//! past each block's slots: the push that takes a block's last slot links
//! the next block and moves the tail over that extra number, and a pop that
//! meets it waits for the link.
//!
//! A block is freed once every value in it has been read. The pop that
//! reads its last slot sets out to free it and checks the slots before it;
//! a slot still being read is marked instead, and the reader of that slot
//! frees the block when it finishes, so no reader is left in memory that
//! has been freed.
//!
//! The head's position word carries one flag beside the number: set, it
//! says the head's block already has a block after it, so a pop there
//! need not read the tail to rule out an empty queue.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};

/// Slots in one block.
const BLOCK_SLOTS: usize = 31;
/// Position numbers per block: its slots, and one more standing for the
/// link to the next block. A power of two.
const LAP: usize = BLOCK_SLOTS + 1;
/// A position word holds its number shifted past the head's flag bit.
const SHIFT: usize = 1;
/// Head flag: the head's block has a block after it.
const HAS_NEXT: usize = 1;

/// Slot state: the value is written.
const WRITTEN: usize = 1;
/// Slot state: the value has been read.
const READ: usize = 2;
/// Slot state: the block is being freed, and this slot's reader finishes it.
const FREE_AFTER_READ: usize = 4;

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicUsize,
}

impl<T> Slot<T> {
    /// Wait until the push that took this slot has written it.
    fn wait_written(&self) {
        while self.state.load(Ordering::Acquire) & WRITTEN == 0 {
            std::hint::spin_loop();
        }
    }
}

struct Block<T> {
    next: AtomicPtr<Block<T>>,
    slots: [Slot<T>; BLOCK_SLOTS],
}

impl<T> Block<T> {
    fn new() -> Box<Self> {
        Box::new(Self {
            next: AtomicPtr::new(ptr::null_mut()),
            slots: std::array::from_fn(|_| Slot {
                value: UnsafeCell::new(MaybeUninit::uninit()),
                state: AtomicUsize::new(0),
            }),
        })
    }

    /// Wait until the push that took this block's last slot has linked the
    /// next block, and return it.
    fn wait_next(&self) -> *mut Block<T> {
        loop {
            let next = self.next.load(Ordering::Acquire);
            if !next.is_null() {
                return next;
            }
            std::hint::spin_loop();
        }
    }

    /// Free `this` once every slot from `start` on has been read. A slot
    /// still being read is marked instead, and its reader calls this again
    /// from the slot after it. The last slot is not checked: its reader is
    /// the one that starts the freeing.
    ///
    /// # Safety
    /// `this` came from `Block::new`, the queue's head and tail have both
    /// moved past it, and every slot before `start`, and the last slot,
    /// have been read.
    unsafe fn free(this: *mut Self, start: usize) {
        for i in start..BLOCK_SLOTS - 1 {
            // SAFETY: the block stays allocated until the drop below, which
            // only a call that finds every slot read reaches.
            let slot = unsafe { &(*this).slots[i] };
            if slot.state.load(Ordering::Acquire) & READ == 0
                && slot.state.fetch_or(FREE_AFTER_READ, Ordering::AcqRel) & READ == 0
            {
                return;
            }
        }
        // SAFETY: every slot has been read, and no reader is left in it.
        drop(unsafe { Box::from_raw(this) });
    }
}

/// One end of the queue: the number of its next position, and the block
/// that position is in.
struct End<T> {
    index: AtomicUsize,
    block: AtomicPtr<Block<T>>,
}

/// Keeps the head and the tail on cache lines of their own, so pushes and
/// pops do not contend over one line.
#[repr(align(128))]
struct Padded<T>(T);

/// An unbounded lock-free first-in first-out queue of `T`.
pub(crate) struct UnboundedQueue<T> {
    head: Padded<End<T>>,
    tail: Padded<End<T>>,
    _values: PhantomData<T>,
}

// SAFETY: a value moves from the thread that pushes it to the thread that
// pops it and is never shared, so `T: Send` is all either side needs.
unsafe impl<T: Send> Send for UnboundedQueue<T> {}
unsafe impl<T: Send> Sync for UnboundedQueue<T> {}

impl<T> UnboundedQueue<T> {
    /// An empty queue. Its first block is made by its first push.
    pub(crate) const fn new() -> Self {
        Self {
            head: Padded(End { index: AtomicUsize::new(0), block: AtomicPtr::new(ptr::null_mut()) }),
            tail: Padded(End { index: AtomicUsize::new(0), block: AtomicPtr::new(ptr::null_mut()) }),
            _values: PhantomData,
        }
    }

    /// Put `value` at the back.
    pub(crate) fn push(&self, value: T) {
        let mut tail = self.tail.0.index.load(Ordering::Acquire);
        let mut block = self.tail.0.block.load(Ordering::Acquire);
        let mut next_block: Option<Box<Block<T>>> = None;
        loop {
            let offset = (tail >> SHIFT) % LAP;
            // The block is full and the push that took its last slot is
            // linking the next one.
            if offset == BLOCK_SLOTS {
                std::hint::spin_loop();
                tail = self.tail.0.index.load(Ordering::Acquire);
                block = self.tail.0.block.load(Ordering::Acquire);
                continue;
            }
            // Taking the last slot means linking the next block, which is
            // made before the slot is taken.
            if offset + 1 == BLOCK_SLOTS && next_block.is_none() {
                next_block = Some(Block::new());
            }
            if block.is_null() {
                let first = Box::into_raw(Block::new());
                if self
                    .tail
                    .0
                    .block
                    .compare_exchange(block, first, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    self.head.0.block.store(first, Ordering::Release);
                    block = first;
                } else {
                    // Another push made the first block; this one is kept
                    // for the next block this push may link.
                    // SAFETY: `first` came from Box::into_raw above and was
                    // never published.
                    next_block = Some(unsafe { Box::from_raw(first) });
                    tail = self.tail.0.index.load(Ordering::Acquire);
                    block = self.tail.0.block.load(Ordering::Acquire);
                    continue;
                }
            }
            let new_tail = tail + (1 << SHIFT);
            match self.tail.0.index.compare_exchange_weak(tail, new_tail, Ordering::SeqCst, Ordering::Acquire) {
                Ok(_) => {
                    // SAFETY: the position is this push's alone, and its
                    // block stays allocated until the slot is read.
                    unsafe {
                        if offset + 1 == BLOCK_SLOTS {
                            let next = Box::into_raw(next_block.take().unwrap_or_else(Block::new));
                            self.tail.0.block.store(next, Ordering::Release);
                            self.tail.0.index.store(new_tail.wrapping_add(1 << SHIFT), Ordering::Release);
                            (*block).next.store(next, Ordering::Release);
                        }
                        #[cfg(test)]
                        crate::test_races::pause_point();
                        let slot = &(*block).slots[offset];
                        slot.value.get().write(MaybeUninit::new(value));
                        slot.state.fetch_or(WRITTEN, Ordering::Release);
                    }
                    return;
                }
                Err(current) => {
                    tail = current;
                    block = self.tail.0.block.load(Ordering::Acquire);
                }
            }
        }
    }

    /// Take the value at the front, or `None` when the queue is empty.
    pub(crate) fn pop(&self) -> Option<T> {
        let mut head = self.head.0.index.load(Ordering::Acquire);
        let mut block = self.head.0.block.load(Ordering::Acquire);
        loop {
            let offset = (head >> SHIFT) % LAP;
            // The pop that took the block's last slot is moving the head
            // to the next block.
            if offset == BLOCK_SLOTS {
                std::hint::spin_loop();
                head = self.head.0.index.load(Ordering::Acquire);
                block = self.head.0.block.load(Ordering::Acquire);
                continue;
            }
            let mut new_head = head + (1 << SHIFT);
            if new_head & HAS_NEXT == 0 {
                fence(Ordering::SeqCst);
                let tail = self.tail.0.index.load(Ordering::Relaxed);
                if head >> SHIFT == tail >> SHIFT {
                    return None;
                }
                if (head >> SHIFT) / LAP != (tail >> SHIFT) / LAP {
                    new_head |= HAS_NEXT;
                }
            }
            // The first push is still making the first block.
            if block.is_null() {
                std::hint::spin_loop();
                head = self.head.0.index.load(Ordering::Acquire);
                block = self.head.0.block.load(Ordering::Acquire);
                continue;
            }
            match self.head.0.index.compare_exchange_weak(head, new_head, Ordering::SeqCst, Ordering::Acquire) {
                Ok(_) => {
                    // SAFETY: the position is this pop's alone; its block is
                    // freed only after this slot is marked read.
                    unsafe {
                        if offset + 1 == BLOCK_SLOTS {
                            let next = (*block).wait_next();
                            let mut next_index = (new_head & !HAS_NEXT).wrapping_add(1 << SHIFT);
                            if !(*next).next.load(Ordering::Relaxed).is_null() {
                                next_index |= HAS_NEXT;
                            }
                            self.head.0.block.store(next, Ordering::Release);
                            self.head.0.index.store(next_index, Ordering::Release);
                        }
                        let slot = &(*block).slots[offset];
                        slot.wait_written();
                        let value = slot.value.get().read().assume_init();
                        if offset + 1 == BLOCK_SLOTS {
                            Block::free(block, 0);
                        } else if slot.state.fetch_or(READ, Ordering::AcqRel) & FREE_AFTER_READ != 0 {
                            Block::free(block, offset + 1);
                        }
                        return Some(value);
                    }
                }
                Err(current) => {
                    head = current;
                    block = self.head.0.block.load(Ordering::Acquire);
                }
            }
        }
    }

    /// Whether the queue held no value at the moment of reading.
    pub(crate) fn is_empty(&self) -> bool {
        let head = self.head.0.index.load(Ordering::SeqCst);
        let tail = self.tail.0.index.load(Ordering::SeqCst);
        head >> SHIFT == tail >> SHIFT
    }

    /// How many values the queue held at one moment while this ran.
    pub(crate) fn len(&self) -> usize {
        loop {
            let mut tail = self.tail.0.index.load(Ordering::SeqCst);
            let mut head = self.head.0.index.load(Ordering::SeqCst);
            // The two were read at one moment only if the tail had not
            // moved by the time the head was read.
            if self.tail.0.index.load(Ordering::SeqCst) != tail {
                continue;
            }
            tail >>= SHIFT;
            head >>= SHIFT;
            // A position resting on a block's link number counts as the
            // start of the next block.
            if tail % LAP == LAP - 1 {
                tail = tail.wrapping_add(1);
            }
            if head % LAP == LAP - 1 {
                head = head.wrapping_add(1);
            }
            // Count from the start of the head's block, then take away one
            // link number for every block boundary between the two.
            let base = head / LAP * LAP;
            let tail = tail.wrapping_sub(base);
            let head = head.wrapping_sub(base);
            return tail - head - tail / LAP;
        }
    }
}

impl<T> Default for UnboundedQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for UnboundedQueue<T> {
    fn drop(&mut self) {
        let mut head = *self.head.0.index.get_mut() & !HAS_NEXT;
        let tail = *self.tail.0.index.get_mut();
        let mut block = *self.head.0.block.get_mut();
        // SAFETY: `&mut self` means no push or pop is running, so every slot
        // from the head to the tail holds a written value no one has read,
        // and every block from the head's on is this queue's alone.
        unsafe {
            while head != tail {
                let offset = (head >> SHIFT) % LAP;
                if offset < BLOCK_SLOTS {
                    (*(*block).slots[offset].value.get()).assume_init_drop();
                } else {
                    let next = *(*block).next.get_mut();
                    drop(Box::from_raw(block));
                    block = next;
                }
                head = head.wrapping_add(1 << SHIFT);
            }
            if !block.is_null() {
                drop(Box::from_raw(block));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;

    /// Values come out in the order they went in, across several block
    /// boundaries, and the length counts values rather than positions.
    #[test]
    fn values_come_out_in_push_order_across_blocks() {
        let queue = UnboundedQueue::new();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        let count = BLOCK_SLOTS * 4 + 5;
        for value in 0..count {
            queue.push(value);
        }
        assert!(!queue.is_empty());
        assert_eq!(queue.len(), count, "the length counted the block links as values");
        for expected in 0..count {
            assert_eq!(queue.pop(), Some(expected), "a value came out of order");
            assert_eq!(queue.len(), count - expected - 1);
        }
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }

    /// Four threads pushing and four popping at once: every value pushed is
    /// popped exactly once.
    #[test]
    fn every_value_pushed_from_many_threads_is_popped_once() {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        const EACH: usize = 20_000;
        const TOTAL: usize = PRODUCERS * EACH;
        let queue = UnboundedQueue::new();
        let popped = AtomicUsize::new(0);
        let seen: Vec<AtomicUsize> = (0..TOTAL).map(|_| AtomicUsize::new(0)).collect();
        let deadline = Instant::now() + crate::test_races::LOST;
        std::thread::scope(|scope| {
            for producer in 0..PRODUCERS {
                let queue = &queue;
                scope.spawn(move || {
                    for i in 0..EACH {
                        queue.push(producer * EACH + i);
                    }
                });
            }
            for _ in 0..CONSUMERS {
                let (queue, popped, seen) = (&queue, &popped, &seen);
                scope.spawn(move || {
                    while popped.load(Ordering::SeqCst) < TOTAL && Instant::now() < deadline {
                        match queue.pop() {
                            Some(value) => {
                                seen[value].fetch_add(1, Ordering::SeqCst);
                                popped.fetch_add(1, Ordering::SeqCst);
                            }
                            None => std::hint::spin_loop(),
                        }
                    }
                });
            }
        });
        let wrong = seen.iter().filter(|times| times.load(Ordering::SeqCst) != 1).count();
        assert_eq!(wrong, 0, "{wrong} values were popped twice or not at all");
        assert_eq!(queue.pop(), None);
    }

    /// A pop that reaches a position whose push has taken it but not yet
    /// written it waits for the value rather than reporting the queue empty
    /// or reading the slot unwritten.
    #[test]
    fn a_pop_waits_for_the_push_that_took_its_slot() {
        let queue = Arc::new(UnboundedQueue::new());
        let pushing = Arc::clone(&queue);
        let (pause, pusher) = crate::test_races::stopped(move || pushing.push(7u64));
        let popping = Arc::clone(&queue);
        let popper = std::thread::spawn(move || popping.pop());
        crate::test_races::settle(&popper);
        assert!(!popper.is_finished(), "the pop did not wait for the value the stopped push is writing");
        pause.release();
        pusher.join().expect("the pusher");
        assert_eq!(popper.join().expect("the popper"), Some(7), "the pop missed the value");
    }

    /// Values still in the queue when it drops are dropped with it.
    #[test]
    fn values_left_in_the_queue_are_dropped_with_it() {
        let alive = Arc::new(());
        let queue = UnboundedQueue::new();
        for _ in 0..BLOCK_SLOTS * 2 + 3 {
            queue.push(Arc::clone(&alive));
        }
        for _ in 0..BLOCK_SLOTS {
            assert!(queue.pop().is_some());
        }
        drop(queue);
        assert_eq!(Arc::strong_count(&alive), 1, "values left in the queue outlived it");
    }
}
