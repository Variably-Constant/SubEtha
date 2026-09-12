//! A debug-build trace of what happened to a ring's ownership and its
//! pops, kept so a failure can say what the other thread did.
//!
//! A two-reader panic on a single-reader core names the thread that lost
//! the race. That thread's own history explains nothing: the question is
//! always what someone else did to that ring in the instructions before.
//! This is a fixed ring of packed events every thread appends to, which
//! the panic dumps filtered to the ring at fault.
//!
//! Each event is one `u64` in one atomic store, so a thread adds an
//! event without taking a lock, allocating, or waiting on another
//! thread. That matters more here than it usually would: the window this
//! records is a few instructions wide, and an instrument that
//! synchronizes threads closes it.
//!
//! Nothing here is compiled into a release build.

#![cfg(debug_assertions)]

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Events kept, which has to cover a whole workload run. Every push and
/// every pop is one, so a run emits them in the thousands, and a buffer
/// that wraps drops the registrations, transfers and morphs first - the
/// events a loss is explained by. At eight bytes each this is 8 MB of
/// static, in debug builds only.
const CAPACITY: usize = 1 << 20;

/// What a recorded event was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum What {
    /// A consumer read a ring's owner while scanning.
    SawOwner,
    /// A consumer popped from a ring it believed it owned.
    Popped,
    /// A consumer claimed a ring that had no owner.
    ClaimedUnowned,
    /// A leaving consumer handed a ring to another.
    Transferred,
    /// A consumer applied a handoff on its own scan.
    AppliedHandoff,
    /// A consumer asked the owner to hand a ring over.
    RequestedHandoff,
    /// A consumer took a ring from an owner whose slot had gone.
    TookOver,
    /// A consumer claimed its slot.
    Registered,
    /// A consumer released its slot.
    Unregistered,
    /// The reaper released a slot whose process it judged gone.
    Reaped,
    /// The shape tag flipped, making the old backing the stale one.
    Morphed,
    /// A push landed in a backing.
    Sent,
    /// The peer counts a shape decision was taken on, recorded at the
    /// moment of the decision. A morph to a single-producer shape while
    /// several producers are registered cannot be explained by the
    /// morph event alone: the question is what the deciding thread
    /// read, and only that thread can say.
    Counts,
    /// How many slots the directory's bitmaps say are claimed, recorded
    /// beside the counts the same decision read. If the two disagree the
    /// counter is wrong; if they agree, the claims had genuinely not
    /// landed for the reading thread, which is a different defect.
    Claimed,
    /// The reaper released a producer slot whose process it judged gone,
    /// which drops the producer count the shape policy reads. [`Reaped`]
    /// names the consumer side; the two carry separate codes because the
    /// columns hold a slot and a pid either way, so nothing else in the
    /// event tells a dropped producer from a dropped consumer.
    ///
    /// [`Reaped`]: What::Reaped
    ReapedProducer,
    /// A producer claimed its slot. The producer side registers and
    /// leaves without touching a ring, so nothing else in a dump places
    /// it, and the producer count is half of what the shape policy
    /// reads.
    RegisteredProducer,
    /// A producer released its slot. A departure that lands between a
    /// peer's last two pushes lowers the producer count under them,
    /// which is a different fault from a count that was never right.
    UnregisteredProducer,
}

impl What {
    fn code(self) -> u64 {
        match self {
            What::SawOwner => 0,
            What::Popped => 1,
            What::ClaimedUnowned => 2,
            What::Transferred => 3,
            What::AppliedHandoff => 4,
            What::RequestedHandoff => 5,
            What::TookOver => 6,
            What::Registered => 7,
            What::Unregistered => 8,
            What::Reaped => 9,
            What::Morphed => 10,
            What::Sent => 11,
            What::Counts => 12,
            What::Claimed => 13,
            What::ReapedProducer => 14,
            What::RegisteredProducer => 15,
            What::UnregisteredProducer => 16,
        }
    }

    fn name(code: u64) -> &'static str {
        match code {
            0 => "saw-owner",
            1 => "popped",
            2 => "claimed-unowned",
            3 => "transferred",
            4 => "applied-handoff",
            5 => "requested-handoff",
            6 => "took-over",
            7 => "registered",
            8 => "unregistered",
            9 => "reaped",
            10 => "morphed",
            11 => "sent",
            12 => "counts",
            13 => "claimed",
            14 => "reaped-producer",
            15 => "registered-producer",
            16 => "unregistered-producer",
            _ => "?",
        }
    }
}

/// Zero is "no event", so a slot never written reads as absent rather
/// than as a `saw-owner` on ring 0 by thread 0.
const EMPTY: u64 = 0;

static EVENTS: [AtomicU64; CAPACITY] = [const { AtomicU64::new(EMPTY) }; CAPACITY];
static NEXT: AtomicUsize = AtomicUsize::new(0);

/// Where the live ring's events begin.
///
/// The trace is process-wide, and a workload that builds and destroys a
/// ring per pass puts every pass's events in it. A dump that spans them
/// reads as though threads from different passes touched one ring,
/// which is a false picture rather than a noisy one:
/// consumer slot 0 of one pass and of the next are different consumers
/// wearing the same number. [`restart`] moves this mark so a dump shows
/// only the ring being asked about.
static EPOCH: AtomicUsize = AtomicUsize::new(0);

/// Begin a fresh span: events already recorded stay in the buffer but
/// fall out of every later dump. Called when a ring is built, so a dump
/// covers one ring rather than a run.
pub fn restart() {
    EPOCH.store(NEXT.load(Ordering::Relaxed), Ordering::Relaxed);
}

/// A short thread identity: the low bits of the thread's id, which is
/// enough to tell two threads apart in one dump and fits the packing.
fn thread_tag() -> u64 {
    // A ThreadId is opaque, so its hash stands in for it. Collisions
    // only make two threads look alike in a dump, which the consumer
    // and ring fields still separate.
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut h);
    (h.finish() & 0x7FF) | 1
}

/// Record one event. Never blocks and never allocates.
///
/// `ring` is the ring index, `consumer` the slot acting, and `other` the
/// owner or target the event concerns, or `u16::MAX` for none.
///
/// Several events spend the columns differently, because a column that
/// would repeat what another already says is worth more carrying
/// something else. [`What::Morphed`] puts the old shape in `consumer`
/// and the new one in `other`; [`What::Sent`] puts the payload's first
/// two bytes in `consumer` and the shape it was pushed into in `other`;
/// [`What::Counts`] and [`What::Claimed`] each put a producer count in
/// `consumer` and a consumer count in `other`; [`What::Reaped`] and
/// [`What::ReapedProducer`] put the slot in `consumer` and the low
/// sixteen bits of the departed process id in `other`.
pub fn note(what: What, ring: usize, consumer: usize, other: usize) {
    // Five bits hold the kind, so a thirty-third variant would alias
    // onto an existing one and print a dump that reads as sound.
    debug_assert!(what.code() <= 0x1F, "the packed event has five bits for the kind");
    let packed = (thread_tag() << 53)
        | ((what.code() & 0x1F) << 48)
        | (((ring as u64) & 0xFFFF) << 32)
        | (((consumer as u64) & 0xFFFF) << 16)
        | ((other as u64) & 0xFFFF);
    let at = NEXT.fetch_add(1, Ordering::Relaxed) % CAPACITY;
    EVENTS[at].store(packed, Ordering::Relaxed);
}

/// The recent events touching `ring`, oldest first, as printable lines.
/// Reads the whole buffer, so it is for a failure path only.
pub fn recent_for(ring: usize, limit: usize) -> Vec<String> {
    let end = NEXT.load(Ordering::Relaxed);
    // Never reach behind the live ring's first event, and never behind
    // what the buffer still holds.
    let start = end.saturating_sub(CAPACITY).max(EPOCH.load(Ordering::Relaxed));
    let mut out = Vec::new();
    for i in start..end {
        let packed = EVENTS[i % CAPACITY].load(Ordering::Relaxed);
        if packed == EMPTY {
            continue;
        }
        let at_ring = ((packed >> 32) & 0xFFFF) as usize;
        // 0xFFFF is an event about a consumer rather than a ring -
        // taking or releasing a slot. Those belong in every ring's dump:
        // without them the dump cannot show whether two threads held one
        // slot at once or merely took it in turn.
        if at_ring != ring && at_ring != 0xFFFF {
            continue;
        }
        let thread = packed >> 53;
        let what = What::name((packed >> 48) & 0x1F);
        let consumer = ((packed >> 16) & 0xFFFF) as usize;
        let other = (packed & 0xFFFF) as usize;
        out.push(format!(
            "thread {thread:#05x} consumer {consumer:<5} {what:<17} ring {at_ring} other {other}"
        ));
    }
    if out.len() > limit {
        out.drain(..out.len() - limit);
    }
    out
}
