//! The value types that ride beside a pointer: the two word-sized Bloom
//! filters and the two clocks. None of them maps a file; each is a
//! small value a script builds, passes around and compares.

use std::cmp::Ordering;

use pwrs::prelude::*;

use subetha_pointers::bloom_pointer::{Bloom64, BloomFine};
use subetha_pointers::versioned_pointer::{HybridLogicalClock, VectorClock};

use crate::common::{arg_err, assert_send, bytes, size};

assert_send!(TinyBloom, FineBloom, Clock, CausalClock);

/// A whole Bloom filter in a single sixty-four bit word.
///
/// Four bits per key in one machine word, which is small enough to sit
/// beside a pointer and be read in the same cache line. That is what it
/// is for: reject a lookup before following the pointer at all. False
/// means the key was definitely never added; true means it probably
/// was. About eight keys before the rate of wrong yeses climbs past a
/// few percent; FalsePositiveRate works the rate out for any number.
#[psclass(name = "SubEtha.TinyBloom", mode = proxy)]
pub struct TinyBloom {
    /// The whole filter as one number, which is how it travels beside a
    /// value or through anything that carries an integer.
    pub bits: u64,
}

impl TinyBloom {
    fn filter(&self) -> Bloom64 {
        Bloom64(self.bits)
    }
}

/// The operations of a `SubEtha.TinyBloom`.
#[psmethods]
impl TinyBloom {
    /// Adds `key`.
    pub fn insert(&mut self, key: PsObject) -> PsResult<()> {
        let key = bytes(&key)?;
        let mut filter = self.filter();
        filter.insert(&key[..]);
        self.bits = filter.0;
        Ok(())
    }

    /// Adds a run of keys in one call.
    pub fn insert_many(&mut self, keys: Vec<PsObject>) -> PsResult<u64> {
        let mut filter = self.filter();
        for key in &keys {
            let key = bytes(key)?;
            filter.insert(&key[..]);
        }
        self.bits = filter.0;
        Ok(keys.len() as u64)
    }

    /// False means `key` was definitely never added; true means it
    /// probably was.
    pub fn contains(&self, key: PsObject) -> PsResult<bool> {
        let key = bytes(&key)?;
        Ok(self.filter().might_contain(&key[..]))
    }

    /// Asks about a run of keys in one call.
    pub fn contains_many(&self, keys: Vec<PsObject>) -> PsResult<Vec<bool>> {
        let filter = self.filter();
        let mut answers = Vec::with_capacity(keys.len());
        for key in &keys {
            let key = bytes(key)?;
            answers.push(filter.might_contain(&key[..]));
        }
        Ok(answers)
    }

    /// How many bits are set, which is how full it is.
    pub fn set_bits(&self) -> PsResult<u32> {
        Ok(self.filter().popcount())
    }

    /// The share of wrong yeses to expect once `keys` keys are in,
    /// between zero and one.
    pub fn false_positive_rate(&self, keys: u64) -> PsResult<f64> {
        Ok(Bloom64::estimated_fpr(size(keys, "the key count")?))
    }

    /// How many keys this size holds before the rate of wrong yeses
    /// climbs past a few percent.
    pub fn suggested_capacity(&self) -> PsResult<u64> {
        Ok(Bloom64::SUGGESTED_CAPACITY as u64)
    }
}

/// Builds a word-sized Bloom filter, empty, holding Key, or rebuilt from
/// the number Bits an earlier one gave.
///
/// # Examples
///
/// `$bloom = New-SubEthaTinyBloom`
#[cmdlet(verb = "New", noun = "SubEthaTinyBloom", alias = "New-SETinyBloom", output = ["SubEtha.TinyBloom"])]
#[derive(Default)]
pub struct NewSubEthaTinyBloom {
    /// Keys to add, each a `byte[]` or a string.
    #[param]
    pub key: Vec<PsObject>,
    /// The number an earlier filter's Bits gave, to rebuild it.
    #[param]
    pub bits: Option<u64>,
}

impl Cmdlet for NewSubEthaTinyBloom {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let mut filter = Bloom64(self.bits.unwrap_or(0));
        for key in &self.key {
            let key = bytes(key)?;
            filter.insert(&key[..]);
        }
        ps.write(TinyBloom { bits: filter.0 })
    }
}

/// The same idea in four words rather than one, for about sixty-four
/// keys instead of eight.
#[psclass(name = "SubEtha.FineBloom", mode = proxy)]
pub struct FineBloom {
    /// How many bits are set, which is how full it is.
    pub set_bits: u32,
    #[psfield(skip)]
    inner: BloomFine,
}

/// The operations of a `SubEtha.FineBloom`.
#[psmethods]
impl FineBloom {
    /// Adds `key`.
    pub fn insert(&mut self, key: PsObject) -> PsResult<()> {
        let key = bytes(&key)?;
        self.inner.insert(&key[..]);
        self.set_bits = self.inner.popcount();
        Ok(())
    }

    /// Adds a run of keys in one call.
    pub fn insert_many(&mut self, keys: Vec<PsObject>) -> PsResult<u64> {
        for key in &keys {
            let key = bytes(key)?;
            self.inner.insert(&key[..]);
        }
        self.set_bits = self.inner.popcount();
        Ok(keys.len() as u64)
    }

    /// False means `key` was definitely never added; true means it
    /// probably was.
    pub fn contains(&self, key: PsObject) -> PsResult<bool> {
        let key = bytes(&key)?;
        Ok(self.inner.might_contain(&key[..]))
    }

    /// Asks about a run of keys in one call.
    pub fn contains_many(&self, keys: Vec<PsObject>) -> PsResult<Vec<bool>> {
        let mut answers = Vec::with_capacity(keys.len());
        for key in &keys {
            let key = bytes(key)?;
            answers.push(self.inner.might_contain(&key[..]));
        }
        Ok(answers)
    }

    /// How many keys this size holds before the rate of wrong yeses
    /// climbs past a few percent.
    pub fn suggested_capacity(&self) -> PsResult<u64> {
        Ok(BloomFine::SUGGESTED_CAPACITY as u64)
    }
}

/// Builds a four-word Bloom filter, empty or holding Key.
///
/// # Examples
///
/// `$bloom = New-SubEthaFineBloom -Key 'a'`
#[cmdlet(verb = "New", noun = "SubEthaFineBloom", alias = "New-SEFineBloom", output = ["SubEtha.FineBloom"])]
#[derive(Default)]
pub struct NewSubEthaFineBloom {
    /// Keys to add, each a `byte[]` or a string.
    #[param]
    pub key: Vec<PsObject>,
}

impl Cmdlet for NewSubEthaFineBloom {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let mut inner = BloomFine::ZERO;
        for key in &self.key {
            let key = bytes(key)?;
            inner.insert(&key[..]);
        }
        ps.write(FineBloom { set_bits: inner.popcount(), inner })
    }
}

/// A clock that keeps wall-clock time and still orders two events that
/// share a reading.
///
/// Two parts: the physical time, and a count that steps when two events
/// land on the same physical reading. Comparing two of these orders
/// them even when the machines' clocks disagree slightly, which a bare
/// timestamp cannot do. Merge is what a receiver does with a sender's
/// clock: it takes the later of the two and steps past it, so the
/// received event orders after the one that caused it. A clock never
/// changes; every operation answers a new one.
#[psclass(name = "SubEtha.Clock", mode = proxy)]
#[derive(Clone, Default)]
pub struct Clock {
    /// The physical time, in microseconds since the epoch.
    pub physical: u64,
    /// The count that orders events sharing a physical reading.
    pub logical: u64,
}

impl Clock {
    fn rust(&self) -> HybridLogicalClock {
        HybridLogicalClock::new(self.physical, self.logical)
    }

    fn from_rust(clock: HybridLogicalClock) -> Self {
        Self { physical: clock.physical, logical: clock.logical }
    }

    fn pair(&self) -> (u64, u64) {
        (self.physical, self.logical)
    }
}

/// The operations of a `SubEtha.Clock`.
#[psmethods]
impl Clock {
    /// The next reading given a new physical time. The count steps
    /// rather than resetting when the physical time has not moved.
    pub fn advance(&self, physical: u64) -> PsResult<Clock> {
        Ok(Clock::from_rust(self.rust().advance(physical)))
    }

    /// The reading a receiver should take, given what arrived and what
    /// its own clock says. Orders the received event after whatever
    /// caused it.
    pub fn merge(&self, received: Clock, physical: u64) -> PsResult<Clock> {
        Ok(Clock::from_rust(self.rust().merge(&received.rust(), physical)))
    }

    /// Minus one when this reads before `other`, one when after, zero
    /// when the two are equal.
    pub fn compare_to(&self, other: Clock) -> PsResult<i32> {
        Ok(match self.pair().cmp(&other.pair()) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        })
    }

    /// Whether this reads before `other`.
    pub fn before(&self, other: Clock) -> PsResult<bool> {
        Ok(self.pair() < other.pair())
    }

    /// Whether this reads after `other`.
    pub fn after(&self, other: Clock) -> PsResult<bool> {
        Ok(self.pair() > other.pair())
    }

    /// Whether this and `other` read the same.
    pub fn same_as(&self, other: Clock) -> PsResult<bool> {
        Ok(self.pair() == other.pair())
    }
}

/// Builds a hybrid logical clock reading: Physical and Logical as given,
/// or with Now the moment this runs, in microseconds since the epoch,
/// with the count at zero.
///
/// # Examples
///
/// `$clock = New-SubEthaClock -Physical 100 -Logical 0`
#[cmdlet(verb = "New", noun = "SubEthaClock", alias = "New-SEClock", output = ["SubEtha.Clock"])]
#[derive(Default)]
pub struct NewSubEthaClock {
    /// The physical time, in microseconds since the epoch; zero when
    /// absent.
    #[param]
    pub physical: Option<u64>,
    /// The count; zero when absent.
    #[param]
    pub logical: Option<u64>,
    /// Read the physical time from the system clock now.
    #[param]
    pub now: bool,
}

impl Cmdlet for NewSubEthaClock {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let clock = if self.now {
            if self.physical.is_some() || self.logical.is_some() {
                return Err(arg_err("Now reads the clock itself; Physical and Logical cannot be given with it"));
            }
            HybridLogicalClock::now()
        } else {
            HybridLogicalClock::new(self.physical.unwrap_or(0), self.logical.unwrap_or(0))
        };
        ps.write(Clock::from_rust(clock))
    }
}

/// How many participants a `SubEtha.CausalClock` counts for. Fixed,
/// because the clock is an array rather than a map, which is what makes
/// comparing two of them a handful of instructions.
const CAUSAL_CLOCK_NODES: usize = 16;

/// One count per participant, which answers whether one event caused
/// another or whether the two happened independently.
///
/// A bare timestamp cannot tell before from at the same time on another
/// machine. This can: comparing two of these answers before, after,
/// equal, or neither, and neither means the two events are genuinely
/// concurrent. A clock never changes; every operation answers a new
/// one.
#[psclass(name = "SubEtha.CausalClock", mode = proxy)]
#[derive(Clone, Default)]
pub struct CausalClock {
    /// Every count, in participant order.
    pub counts: Vec<u64>,
}

impl CausalClock {
    fn rust(&self) -> PsResult<VectorClock<CAUSAL_CLOCK_NODES>> {
        if self.counts.len() != CAUSAL_CLOCK_NODES {
            return Err(arg_err(format!("a causal clock holds {CAUSAL_CLOCK_NODES} counts, got {}", self.counts.len())));
        }
        let mut clock = VectorClock::zero();
        clock.clock.copy_from_slice(&self.counts);
        Ok(clock)
    }

    fn from_rust(clock: VectorClock<CAUSAL_CLOCK_NODES>) -> Self {
        Self { counts: clock.clock.to_vec() }
    }

    fn node(node: u64) -> PsResult<usize> {
        let node = size(node, "the participant")?;
        if node >= CAUSAL_CLOCK_NODES {
            return Err(arg_err(format!("a causal clock counts for {CAUSAL_CLOCK_NODES} participants, numbered from zero")));
        }
        Ok(node)
    }
}

/// The operations of a `SubEtha.CausalClock`.
#[psmethods]
impl CausalClock {
    /// The clock after `node`'s own count steps, which is what a
    /// participant does when something happens to it.
    pub fn tick(&self, node: u64) -> PsResult<CausalClock> {
        let mut stepped = self.rust()?;
        stepped.increment(Self::node(node)?);
        Ok(CausalClock::from_rust(stepped))
    }

    /// One participant's count.
    pub fn count(&self, node: u64) -> PsResult<u64> {
        Ok(self.rust()?.clock[Self::node(node)?])
    }

    /// The clock a receiver should hold after taking `other` in: the
    /// higher of each count.
    pub fn merge(&self, other: CausalClock) -> PsResult<CausalClock> {
        Ok(CausalClock::from_rust(self.rust()?.merge(&other.rust()?)))
    }

    /// How this stands to `other`: before, after, equal, or concurrent
    /// when neither caused the other.
    pub fn compare(&self, other: CausalClock) -> PsResult<String> {
        Ok(match self.rust()?.causal_cmp(&other.rust()?) {
            Some(Ordering::Less) => "before",
            Some(Ordering::Greater) => "after",
            Some(Ordering::Equal) => "equal",
            None => "concurrent",
        }
        .to_string())
    }

    /// Whether this happened before `other`, which is the same as
    /// Compare answering before.
    pub fn happened_before(&self, other: CausalClock) -> PsResult<bool> {
        Ok(matches!(self.rust()?.causal_cmp(&other.rust()?), Some(Ordering::Less)))
    }

    /// Whether neither this nor `other` caused the other.
    pub fn concurrent_with(&self, other: CausalClock) -> PsResult<bool> {
        Ok(self.rust()?.causal_cmp(&other.rust()?).is_none())
    }

    /// How many participants a clock counts for.
    pub fn nodes(&self) -> PsResult<u64> {
        Ok(CAUSAL_CLOCK_NODES as u64)
    }
}

/// Builds a causal clock with every count at zero, or at Counts.
///
/// # Examples
///
/// `$clock = New-SubEthaCausalClock`
#[cmdlet(verb = "New", noun = "SubEthaCausalClock", alias = "New-SECausalClock", output = ["SubEtha.CausalClock"])]
#[derive(Default)]
pub struct NewSubEthaCausalClock {
    /// The sixteen counts, in participant order; all zero when absent.
    #[param]
    pub counts: Vec<u64>,
}

impl Cmdlet for NewSubEthaCausalClock {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let clock = if self.counts.is_empty() {
            CausalClock::from_rust(VectorClock::zero())
        } else {
            let given = CausalClock { counts: std::mem::take(&mut self.counts) };
            CausalClock::from_rust(given.rust()?)
        };
        ps.write(clock)
    }
}
