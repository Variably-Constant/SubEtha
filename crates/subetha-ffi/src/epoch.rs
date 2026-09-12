//! Telling a destroy when every call that might be inside an object has
//! left, without the call paying an atomic read-modify-write for it.
//!
//! A call publishes that it is inside one, into a word of its own thread's,
//! and clears it on the way out. A destroy marks the object closing, then
//! looks at every thread's word and waits for the ones that were already
//! inside. The two sides need to agree on an order, since the call must not
//! read the object as live after the destroy has closed it and seen the
//! thread quiet, and there are two ways to get that:
//!
//! - Where the host can fence every thread from one of them, the destroy
//!   pays for it and a call publishes with a plain store.
//!   `FlushProcessWriteBuffers` does it on Windows and
//!   `membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED)` on Linux.
//! - Everywhere else the call fences itself with a sequentially consistent
//!   store, which is one atomic instruction where the reference counts it
//!   replaced were two.
//!
//! Either way the promise is what it was: a call in flight blocks a
//! destroy, and a handle whose object is closing is refused.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use crate::error::{fail, SUBETHA_E_NOT_SUPPORTED};

/// The word a thread publishes into. Its own cache line, so one thread's
/// publish never disturbs another's.
///
/// Zero while the thread is inside no call. Otherwise the low half names
/// the slot it is inside, one past the slot's index so that zero can mean
/// none, and the high half carries the epoch it read on the way in. Both
/// travel in one word because a destroy has to read them together: a
/// thread inside another slot is none of its business, and a thread that
/// came in after the destroy published cannot have seen the object live.
#[repr(C, align(64))]
struct ThreadEpoch {
    word: AtomicU64,
    _pad: [u8; 56],
}

/// The slot a published word names, and the epoch it was published in.
fn unpack(word: u64) -> (u64, u32) {
    (word & 0xFFFF_FFFF, (word >> 32) as u32)
}

/// The word naming `slot` in `epoch`.
fn pack(slot: u32, epoch: u32) -> u64 {
    ((epoch as u64) << 32) | (u64::from(slot) + 1)
}

const _: () = assert!(std::mem::size_of::<ThreadEpoch>() == 64);

impl ThreadEpoch {
    fn new() -> Self {
        Self { word: AtomicU64::new(0), _pad: [0; 56] }
    }
}

/// Every word ever handed to a thread, and the ones a finished thread gave
/// back. A word is never dropped: the next thread to arrive takes a
/// returned one, so the table stands at the high-water mark of threads
/// that have been inside a call at once, and a word's address is good for
/// as long as the process is.
struct Registry {
    all: Vec<Arc<ThreadEpoch>>,
    free: Vec<usize>,
}

/// The mutex is what threads joining and leaving serialize on, and it is
/// still required for that after [`PUBLISHED`] took over the reads.
/// Registering against the snapshot alone would be load, clone, push,
/// store, and two threads arriving at once would lose one of them.
static REGISTRY: Mutex<Registry> = Mutex::new(Registry { all: Vec::new(), free: Vec::new() });

/// The same words as `REGISTRY.all`, published for the walk to read
/// without taking the lock.
///
/// The walk cannot hold `REGISTRY` while it runs, because it spins
/// waiting for threads to leave their calls and a thread on its way out
/// takes that same lock to give its word back - so holding it would wait
/// on something that is waiting on the lock. The previous shape solved
/// that by cloning the `Vec<Arc<..>>` under the lock, which costs an
/// allocation and two atomic read-modify-writes per registered thread on
/// a path that runs whenever a handle closes. This is the same snapshot
/// for one refcount operation and no lock.
///
/// It is republished only when a thread arrives and finds no free word,
/// so it changes at most once per thread of the process's high-water
/// mark, never on the hot path. A thread that registers during a walk is
/// absent from the snapshot in hand, which is correct for the same reason
/// it was before: it published its word after the walk's barrier, so its
/// epoch is already at or past the target and the walk would not have
/// waited for it.
static PUBLISHED: LazyLock<ArcSwap<Vec<Arc<ThreadEpoch>>>> =
    LazyLock::new(|| ArcSwap::from_pointee(Vec::new()));

/// Bumped by every destroy, so a call that publishes afterwards is
/// recognizable as one that cannot have read the object as live. It wraps,
/// and is only ever compared as a difference, so a wrap costs nothing.
static GLOBAL: AtomicU32 = AtomicU32::new(1);

/// How the two sides agree on an order, decided once.
static BARRIER: AtomicUsize = AtomicUsize::new(BARRIER_UNKNOWN);
const BARRIER_UNKNOWN: usize = 0;
/// The destroy fences every thread; a call publishes with a plain store.
const BARRIER_PROCESS_WIDE: usize = 1;
/// The call fences itself.
const BARRIER_PER_CALL: usize = 2;

/// A thread's word, taken on its first call and given back when it ends.
struct Local {
    index: usize,
    epoch: Arc<ThreadEpoch>,
}

impl Drop for Local {
    fn drop(&mut self) {
        self.epoch.word.store(0, Ordering::Release);
        REGISTRY.lock().free.push(self.index);
    }
}

thread_local! {
    static LOCAL: Local = {
        let mut registry = REGISTRY.lock();
        match registry.free.pop() {
            Some(index) => {
                let epoch = Arc::clone(&registry.all[index]);
                epoch.word.store(0, Ordering::Release);
                Local { index, epoch }
            }
            None => {
                let epoch = Arc::new(ThreadEpoch::new());
                registry.all.push(Arc::clone(&epoch));
                // Republish for the lock-free walk. Only a new
                // high-water thread reaches here, so this is rare and the
                // clone it costs is not on any hot path.
                PUBLISHED.store(Arc::new(registry.all.clone()));
                Local { index: registry.all.len() - 1, epoch }
            }
        }
    };
}

/// Whether the host can fence every thread from one of them, asked once
/// and remembered.
fn barrier_kind() -> usize {
    match BARRIER.load(Ordering::Relaxed) {
        BARRIER_UNKNOWN => {
            let kind = if register_process_wide_barrier() { BARRIER_PROCESS_WIDE } else { BARRIER_PER_CALL };
            BARRIER.store(kind, Ordering::Relaxed);
            kind
        }
        kind => kind,
    }
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    /// Drains every other thread's store buffer, which gives this thread's
    /// preceding stores the effect of a fence in all of them.
    fn FlushProcessWriteBuffers();
}

/// Ask the host for the barrier, and say whether it has one.
#[cfg(windows)]
fn register_process_wide_barrier() -> bool {
    true
}

#[cfg(windows)]
fn process_wide_barrier() {
    // SAFETY: the call takes no argument and returns nothing.
    unsafe { FlushProcessWriteBuffers() };
}

/// The expedited private command fences the process's own threads without
/// waiting for a grace period, and is available only to a process that
/// registered its intent to use it.
#[cfg(target_os = "linux")]
const MEMBARRIER_CMD_PRIVATE_EXPEDITED: libc::c_int = 1 << 3;
#[cfg(target_os = "linux")]
const MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED: libc::c_int = 1 << 4;

#[cfg(target_os = "linux")]
fn membarrier(command: libc::c_int) -> bool {
    // SAFETY: the syscall reads no memory of ours; it takes the command, a
    // flags word and a cpu id, and reports failure as a negative return.
    unsafe { libc::syscall(libc::SYS_membarrier, command, 0, 0) == 0 }
}

#[cfg(target_os = "linux")]
fn register_process_wide_barrier() -> bool {
    membarrier(MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED)
}

#[cfg(target_os = "linux")]
fn process_wide_barrier() {
    if !membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED) {
        // The kernel took the registration and then refused the barrier,
        // which leaves a call that published with a plain store unordered
        // against this one. A fence of our own is the strongest thing left.
        std::sync::atomic::fence(Ordering::SeqCst);
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
fn register_process_wide_barrier() -> bool {
    false
}

#[cfg(not(any(windows, target_os = "linux")))]
fn process_wide_barrier() {
    std::sync::atomic::fence(Ordering::SeqCst);
}

/// A call in flight. Dropping it lets a destroy that is waiting proceed.
///
/// It holds the address of its own thread's word rather than a share of
/// it, which is what keeps the call off the reference counts. The word
/// outlives the guard: the registry never drops one, and the thread hands
/// its word back only after every guard on that thread is gone. The
/// `PhantomData` keeps the guard on the thread that made it, so the word
/// it clears is always the one it published into.
pub(crate) struct Inside {
    word: *const ThreadEpoch,
    _not_send: PhantomData<*const ()>,
}

impl Drop for Inside {
    fn drop(&mut self) {
        // SAFETY: the registry keeps every word for the life of the
        // process, and this guard cannot outlive its thread's hold on it.
        unsafe { (*self.word).word.store(0, Ordering::Release) };
    }
}

/// Publish that this thread is inside a call on `slot`, before it reads
/// whether that slot's object is live.
///
/// Refused on a thread whose locals have already been torn down, which is
/// a thread on its way out and so not one that should be starting a call,
/// and refused while this thread is already inside one: the word holds a
/// single slot, and a call that borrowed a second handle inside the first
/// would unpublish the one a destroy is waiting on.
pub(crate) fn enter(slot: u32) -> Result<Inside, i32> {
    LOCAL
        .try_with(|local| {
            if local.epoch.word.load(Ordering::Relaxed) != 0 {
                return Err(fail(
                    SUBETHA_E_NOT_SUPPORTED,
                    "this thread is already inside a call; an entry point cannot borrow a second handle",
                ));
            }
            let mark = pack(slot, GLOBAL.load(Ordering::Relaxed));
            local.epoch.word.store(mark, Ordering::Relaxed);
            if barrier_kind() != BARRIER_PROCESS_WIDE {
                // Nothing else will fence this thread, so it fences itself:
                // the publish above must land before the load of the slot's
                // state below, and a store followed by a load of another
                // word is exactly what a processor is free to reorder.
                std::sync::atomic::fence(Ordering::SeqCst);
            }
            Ok(Inside { word: Arc::as_ptr(&local.epoch), _not_send: PhantomData })
        })
        .unwrap_or_else(|gone| {
            Err(fail(
                SUBETHA_E_NOT_SUPPORTED,
                format!("the calling thread is being torn down and has no call word: {gone}"),
            ))
        })
}

/// Wait until no thread is inside a call on `slot` that began before the
/// caller closed it. The caller has already marked the slot closing; this
/// makes that visible and waits out the calls that could have missed it.
pub(crate) fn wait_for_calls_on(slot: u32) {
    wait_for_calls_on_all(&[slot]);
}

/// How many thread words the registry holds: the high-water mark of
/// threads that have been inside a call at once, since a word is never
/// dropped.
///
/// This is the length of the walk every reclamation does after its
/// barrier, so it is the second term in what a destroy costs. A bench
/// that quotes a per-word cost has to read this rather than reason about
/// it from the order its rows ran in - the walk is cheap precisely when
/// the registry happens to be empty, and how full it is at the moment of
/// a measurement is a property of what ran before, not of the design.
pub(crate) fn registry_len() -> usize {
    REGISTRY.lock().all.len()
}

/// [`wait_for_calls_on`] for a batch of slots under a single barrier.
///
/// The barrier is what this costs, and it costs the same whether it
/// covers one slot or a hundred: it interrupts every running thread in
/// the process, so its price rises with how busy the process is rather
/// than with how much is being reclaimed. Measured on the AVX-512 Windows
/// build host at 331 ns with the process otherwise idle and 5.90 us with
/// seven threads running, so a caller that reclaims often batches rather
/// than paying it per slot.
///
/// The registry is read once for the whole batch, so a sweep also takes
/// the global lock once rather than once per slot.
pub(crate) fn wait_for_calls_on_all(slots: &[u32]) {
    if slots.is_empty() {
        return;
    }
    let target = GLOBAL.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
    process_wide_barrier();
    let threads = PUBLISHED.load();
    for thread in threads.iter() {
        loop {
            let (published_slot, published_epoch) = unpack(thread.word.load(Ordering::Acquire));
            // A call that began after the barrier cannot have read any of
            // these slots as live, whichever one it names.
            if published_epoch.wrapping_sub(target) as i32 >= 0 {
                break;
            }
            // Quiescent, or inside a call on a slot outside this batch.
            if !slots.iter().any(|s| published_slot == u64::from(*s) + 1) {
                break;
            }
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn a_thread_publishes_the_slot_it_is_inside_and_clears_on_the_way_out() {
        let word = LOCAL.with(|local| Arc::clone(&local.epoch));
        assert_eq!(word.word.load(Ordering::Acquire), 0, "quiet before the call");
        {
            let _inside = enter(7).expect("a live thread takes its word");
            let (slot, _) = unpack(word.word.load(Ordering::Acquire));
            assert_eq!(slot, 8, "the slot one past its index, so zero can mean none");
        }
        assert_eq!(word.word.load(Ordering::Acquire), 0, "quiet again");
    }

    #[test]
    fn a_second_borrow_on_one_thread_is_refused_rather_than_unpublishing_the_first() {
        let _inside = enter(1).expect("a live thread takes its word");
        match enter(2) {
            Ok(_) => panic!("a second borrow on one thread must be refused"),
            Err(code) => assert_eq!(code, SUBETHA_E_NOT_SUPPORTED),
        }
    }

    #[test]
    fn a_wait_returns_at_once_when_every_thread_is_quiet() {
        wait_for_calls_on(3);
    }

    #[test]
    fn a_wait_outlasts_a_call_on_its_own_slot_and_ignores_another() {
        let inside = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let (i, r, d) = (Arc::clone(&inside), Arc::clone(&release), Arc::clone(&done));
        let worker = std::thread::spawn(move || {
            let _guard = enter(11).expect("a live thread takes its word");
            i.store(true, Ordering::Release);
            while !r.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            d.store(true, Ordering::Release);
        });
        while !inside.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        // Another slot's destroy does not wait for that call at all.
        wait_for_calls_on(12);
        assert!(!done.load(Ordering::Acquire), "a call on slot 11 is not slot 12's business");
        let waiter = std::thread::spawn(move || {
            wait_for_calls_on(11);
            assert!(done.load(Ordering::Acquire), "the wait outlasted the call");
        });
        std::thread::yield_now();
        release.store(true, Ordering::Release);
        worker.join().expect("the worker finishes");
        waiter.join().expect("the waiter finishes");
    }

    /// The table stands at the high-water mark of threads that have been
    /// inside a call at once, rather than growing by one for every thread
    /// that has ever run.
    ///
    /// The count is what this asserts, and not that the free list is
    /// non-empty at any instant: the registry is process-wide and every
    /// other test in this binary shares it, so a word returned here is
    /// one another thread may take before the next line runs. Sixteen
    /// threads one after another need one word between them; if a word
    /// were leaked per thread the table would grow by sixteen, and the
    /// handful of concurrent arrivals from elsewhere in the suite cannot
    /// account for that.
    #[test]
    fn a_thread_that_ends_gives_its_word_back() {
        const ROUNDS: usize = 16;
        let before = REGISTRY.lock().all.len();
        for _ in 0..ROUNDS {
            std::thread::spawn(|| {
                let _inside = enter(5).expect("a live thread takes its word");
            })
            .join()
            .expect("the thread finishes");
        }
        let after = REGISTRY.lock().all.len();
        assert!(after >= before, "the table never shrinks");
        assert!(
            after - before < ROUNDS,
            "{ROUNDS} threads ran one at a time and the table grew by {}, \
             so a word was not given back when its thread ended",
            after - before,
        );
    }
}
