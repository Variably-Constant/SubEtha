//! The structures that summarize a stream in fixed memory: the two
//! Bloom filters, the distinct-count and frequency sketches, the
//! histogram, the rate limiter and the cache.

use pwrs::prelude::*;

use subetha_cxc::raw_lru_cache::RawLruCache;
use subetha_cxc::shared_blocked_bloom_filter::SharedBlockedBloomFilter;
use subetha_cxc::shared_bloom_filter::SharedBloomFilter;
use subetha_cxc::shared_count_min_sketch::SharedCountMinSketch;
use subetha_cxc::shared_histogram::SharedHistogram;
use subetha_cxc::shared_hyper_log_log::{SharedHyperLogLog, MAX_PRECISION as HLL_MAX_PRECISION, MIN_PRECISION as HLL_MIN_PRECISION};
use subetha_cxc::shared_rate_limiter::{RateLimiterError, SharedRateLimiter};

use crate::common::{arg_err, assert_send, bytes, full_path, op_err, open_err, out_bytes, size};

assert_send!(BloomFilter, BlockedBloomFilter, HyperLogLog, CountMinSketch, Histogram, RateLimiter, LruCache);

/// A Bloom filter's size: the bits and the hash count.
#[psclass(name = "SubEtha.BloomSize")]
#[derive(Clone, Default)]
pub struct BloomSize {
    /// How many bits the filter holds.
    pub bits: u64,
    /// How many bits each item sets.
    pub hashes: u32,
}

/// A count-min sketch's size: its depth and width.
#[psclass(name = "SubEtha.SketchSize")]
#[derive(Clone, Default)]
pub struct SketchSize {
    /// How many hash rows the sketch has.
    pub depth: u32,
    /// How many counters each row has.
    pub width: u32,
}

/// A Bloom filter in a mapped file: it answers definitely not there or
/// probably there, and never the other way round.
#[psclass(name = "SubEtha.BloomFilter", mode = proxy)]
pub struct BloomFilter {
    /// The file the filter lives in.
    pub path: String,
    /// How many bits the filter holds.
    pub bits: u64,
    /// How many bits each item sets.
    pub hashes: u32,
    #[psfield(skip)]
    inner: SharedBloomFilter,
}

impl BloomFilter {
    fn obtain(path: String, bits: u64, hashes: u32, open: bool) -> PsResult<Self> {
        if bits == 0 || hashes == 0 {
            return Err(arg_err("the bits and the hash count must both be at least one"));
        }
        let n = size(bits, "the bit count")?;
        let inner = if open { SharedBloomFilter::open(&path, n, hashes) } else { SharedBloomFilter::create(&path, n, hashes) }
            .map_err(|e| open_err("the filter", &path, e))?;
        Ok(Self { path, bits: inner.n_bits(), hashes: inner.n_hashes(), inner })
    }
}

/// The operations of a `SubEtha.BloomFilter`.
#[psmethods]
impl BloomFilter {
    /// What the filter's false-positive rate has drifted to, given what
    /// has actually been put in it.
    pub fn false_positive_rate(&self) -> PsResult<f64> {
        Ok(self.inner.estimated_false_positive_rate())
    }

    /// Adds `item`.
    pub fn insert(&self, item: PsObject) -> PsResult<()> {
        let item = bytes(&item)?;
        self.inner.insert(&item).map_err(|e| op_err("inserting", e))
    }

    /// Adds a run of items in one call.
    pub fn insert_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        for item in &items {
            let item = bytes(item)?;
            self.inner.insert(&item).map_err(|e| op_err("inserting", e))?;
        }
        Ok(items.len() as u64)
    }

    /// False means `item` is definitely absent. True means it is
    /// probably present, at about the false-positive rate.
    pub fn contains(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        self.inner.contains(&item).map_err(|e| op_err("looking up", e))
    }

    /// Looks a run of items up in one call.
    pub fn contains_many(&self, items: Vec<PsObject>) -> PsResult<Vec<bool>> {
        let mut answers = Vec::with_capacity(items.len());
        for item in &items {
            let item = bytes(item)?;
            answers.push(self.inner.contains(&item).map_err(|e| op_err("looking up", e))?);
        }
        Ok(answers)
    }

    /// Empties the filter.
    pub fn clear(&self) -> PsResult<()> {
        self.inner.clear();
        Ok(())
    }
}

/// Obtains the Bloom filter at Path with Bits bits and Hashes hash
/// functions, creating it when the file does not exist.
///
/// # Examples
///
/// `$filter = New-SubEthaBloomFilter -Path C:\ipc\bloomfilter -Bits 98304 -Hashes 7`
#[cmdlet(verb = "New", noun = "SubEthaBloomFilter", alias = "New-SEBloomFilter", output = ["SubEtha.BloomFilter"])]
#[derive(Default)]
pub struct NewSubEthaBloomFilter {
    /// The file the filter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many bits the filter holds.
    #[param(mandatory, position = 1)]
    pub bits: u64,
    /// How many bits each item sets.
    #[param(mandatory, position = 2)]
    pub hashes: u32,
}

impl Cmdlet for NewSubEthaBloomFilter {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BloomFilter::obtain(path, self.bits, self.hashes, false)?)
    }
}

/// Attaches to the Bloom filter at Path, which must exist with the size
/// it was created with.
///
/// # Examples
///
/// `$filter = Open-SubEthaBloomFilter -Path C:\ipc\bloomfilter -Bits 98304 -Hashes 7`
#[cmdlet(verb = "Open", noun = "SubEthaBloomFilter", alias = "Open-SEBloomFilter", output = ["SubEtha.BloomFilter"])]
#[derive(Default)]
pub struct OpenSubEthaBloomFilter {
    /// The file the filter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many bits the filter holds.
    #[param(mandatory, position = 1)]
    pub bits: u64,
    /// How many bits each item sets.
    #[param(mandatory, position = 2)]
    pub hashes: u32,
}

impl Cmdlet for OpenSubEthaBloomFilter {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BloomFilter::obtain(path, self.bits, self.hashes, true)?)
    }
}

/// The bits and hash count for holding Items with no more than
/// FalsePositiveRate of wrong yeses, so a caller sizes a filter from
/// what it means rather than from arithmetic. Blocked sizes for the
/// blocked filter.
///
/// # Examples
///
/// `$size = Measure-SubEthaBloomSize -Items 10000 -FalsePositiveRate 0.01`
#[cmdlet(verb = "Measure", noun = "SubEthaBloomSize", alias = "Measure-SEBloomSize", output = ["SubEtha.BloomSize"])]
#[derive(Default)]
pub struct MeasureSubEthaBloomSize {
    /// How many items the filter will hold.
    #[param(mandatory, position = 0)]
    pub items: u64,
    /// The rate of wrong yeses that can be lived with, above zero and
    /// below one.
    #[param(mandatory, position = 1)]
    pub false_positive_rate: f64,
    /// Size a blocked filter, whose bits for one item share a cache
    /// line, instead of a plain one.
    #[param]
    pub blocked: bool,
}

impl Cmdlet for MeasureSubEthaBloomSize {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        if self.items == 0 {
            return Err(arg_err("the item count must be at least one"));
        }
        let rate = self.false_positive_rate;
        if !(rate > 0.0 && rate < 1.0) {
            return Err(arg_err("the false positive rate must lie between zero and one"));
        }
        let items = size(self.items, "the item count")?;
        let (bits, hashes) = if self.blocked { SharedBlockedBloomFilter::suggest_config(items, rate) } else { SharedBloomFilter::suggest_config(items, rate) };
        ps.write(BloomSize { bits: bits as u64, hashes })
    }
}

/// A set that answers whether something has been added, where the bits
/// for one item are kept together in a single cache line.
///
/// Like any Bloom filter it can say yes about something never added,
/// and never says no about something that was. An ordinary Bloom filter
/// touches scattered bits across the whole filter, so one lookup is
/// several cache misses; this one puts every bit for an item in one
/// 64-byte block and pays for a single miss. Nothing can be removed;
/// Clear empties the whole filter.
#[psclass(name = "SubEtha.BlockedBloomFilter", mode = proxy)]
pub struct BlockedBloomFilter {
    /// The file the filter lives in.
    pub path: String,
    /// How many cache-line blocks the filter is made of.
    pub blocks: u64,
    /// How many bits each item sets.
    pub hashes: u32,
    #[psfield(skip)]
    inner: SharedBlockedBloomFilter,
}

impl BlockedBloomFilter {
    fn obtain(path: String, bits: u64, hashes: u32, open: bool, reset: bool) -> PsResult<Self> {
        let n = size(bits, "the bit count")?;
        let inner = if reset {
            SharedBlockedBloomFilter::reset(&path, n, hashes)
        } else if open {
            SharedBlockedBloomFilter::open(&path, n, hashes)
        } else {
            SharedBlockedBloomFilter::create(&path, n, hashes)
        }
        .map_err(|e| open_err("the filter", &path, e))?;
        Ok(Self { path, blocks: inner.n_blocks(), hashes: inner.n_hashes(), inner })
    }
}

/// The operations of a `SubEtha.BlockedBloomFilter`.
#[psmethods]
impl BlockedBloomFilter {
    /// Adds `item`.
    pub fn insert(&self, item: PsObject) -> PsResult<()> {
        let item = bytes(&item)?;
        self.inner.insert(&item);
        Ok(())
    }

    /// Adds a run of items in one call.
    pub fn insert_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        for item in &items {
            let item = bytes(item)?;
            self.inner.insert(&item);
        }
        Ok(items.len() as u64)
    }

    /// False means `item` was definitely never added. True means it
    /// probably was.
    pub fn contains(&self, item: PsObject) -> PsResult<bool> {
        let item = bytes(&item)?;
        Ok(self.inner.contains(&item))
    }

    /// Asks about a run of items in one call.
    pub fn contains_many(&self, items: Vec<PsObject>) -> PsResult<Vec<bool>> {
        let mut answers = Vec::with_capacity(items.len());
        for item in &items {
            let item = bytes(item)?;
            answers.push(self.inner.contains(&item));
        }
        Ok(answers)
    }

    /// Empties the filter.
    pub fn clear(&self) -> PsResult<()> {
        self.inner.clear();
        Ok(())
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the blocked Bloom filter at Path with Bits bits and Hashes
/// hash functions, creating it when the file does not exist; with Reset,
/// empties it and remakes it at that size.
///
/// # Examples
///
/// `$filter = New-SubEthaBlockedBloomFilter -Path C:\ipc\blocked -Bits 98304 -Hashes 7`
#[cmdlet(verb = "New", noun = "SubEthaBlockedBloomFilter", alias = "New-SEBlockedBloomFilter", output = ["SubEtha.BlockedBloomFilter"])]
#[derive(Default)]
pub struct NewSubEthaBlockedBloomFilter {
    /// The file the filter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many bits the filter holds.
    #[param(mandatory, position = 1)]
    pub bits: u64,
    /// How many bits each item sets.
    #[param(mandatory, position = 2)]
    pub hashes: u32,
    /// Empty the filter and remake it at this size, throwing away
    /// everything in it.
    #[param]
    pub reset: bool,
}

impl Cmdlet for NewSubEthaBlockedBloomFilter {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BlockedBloomFilter::obtain(path, self.bits, self.hashes, false, self.reset)?)
    }
}

/// Attaches to the blocked Bloom filter at Path, which must exist with
/// the size it was created with.
///
/// # Examples
///
/// `$filter = Open-SubEthaBlockedBloomFilter -Path C:\ipc\blocked -Bits 98304 -Hashes 7`
#[cmdlet(verb = "Open", noun = "SubEthaBlockedBloomFilter", alias = "Open-SEBlockedBloomFilter", output = ["SubEtha.BlockedBloomFilter"])]
#[derive(Default)]
pub struct OpenSubEthaBlockedBloomFilter {
    /// The file the filter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many bits the filter holds.
    #[param(mandatory, position = 1)]
    pub bits: u64,
    /// How many bits each item sets.
    #[param(mandatory, position = 2)]
    pub hashes: u32,
}

impl Cmdlet for OpenSubEthaBlockedBloomFilter {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(BlockedBloomFilter::obtain(path, self.bits, self.hashes, true, false)?)
    }
}

/// A HyperLogLog in a mapped file: it counts how many distinct items it
/// has seen, in a fixed amount of memory, without keeping any of them.
#[psclass(name = "SubEtha.HyperLogLog", mode = proxy)]
pub struct HyperLogLog {
    /// The file the counter lives in.
    pub path: String,
    /// The precision, which trades memory for accuracy.
    pub precision: u8,
    /// How many registers the precision gives.
    pub registers: u32,
    #[psfield(skip)]
    inner: SharedHyperLogLog,
}

impl HyperLogLog {
    fn obtain(path: String, precision: u8, open: bool) -> PsResult<Self> {
        if !(HLL_MIN_PRECISION..=HLL_MAX_PRECISION).contains(&precision) {
            return Err(arg_err(format!("the precision must lie between {HLL_MIN_PRECISION} and {HLL_MAX_PRECISION}")));
        }
        let inner = if open { SharedHyperLogLog::open(&path, precision) } else { SharedHyperLogLog::create(&path, precision) }
            .map_err(|e| open_err("the counter", &path, e))?;
        Ok(Self { path, precision: inner.precision(), registers: inner.n_registers(), inner })
    }
}

/// The operations of a `SubEtha.HyperLogLog`.
#[psmethods]
impl HyperLogLog {
    /// Counts `item`.
    pub fn insert(&self, item: PsObject) -> PsResult<()> {
        let item = bytes(&item)?;
        self.inner.insert(&item);
        Ok(())
    }

    /// Counts a run of items in one call, which is what this structure
    /// is usually fed: a stream rather than single items.
    pub fn insert_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        for item in &items {
            let item = bytes(item)?;
            self.inner.insert(&item);
        }
        Ok(items.len() as u64)
    }

    /// How many distinct items it estimates it has seen. An estimate,
    /// not a count: that is the bargain the structure makes.
    pub fn estimate(&self) -> PsResult<u64> {
        Ok(self.inner.estimate())
    }

    /// Forgets everything.
    pub fn reset(&self) -> PsResult<()> {
        self.inner.reset();
        Ok(())
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the distinct counter at Path at Precision (fourteen when
/// absent; four is the smallest the format allows and sixteen the
/// largest), creating it when the file does not exist.
///
/// # Examples
///
/// `$counter = New-SubEthaHyperLogLog -Path C:\ipc\hyperloglog`
#[cmdlet(verb = "New", noun = "SubEthaHyperLogLog", alias = "New-SEHyperLogLog", output = ["SubEtha.HyperLogLog"])]
#[derive(Default)]
pub struct NewSubEthaHyperLogLog {
    /// The file the counter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The precision, between four and sixteen; fourteen when absent.
    #[param]
    pub precision: Option<u8>,
}

impl Cmdlet for NewSubEthaHyperLogLog {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HyperLogLog::obtain(path, self.precision.unwrap_or(14), false)?)
    }
}

/// Attaches to the distinct counter at Path, which must exist at the
/// Precision it was created with.
///
/// # Examples
///
/// `$counter = Open-SubEthaHyperLogLog -Path C:\ipc\hyperloglog`
#[cmdlet(verb = "Open", noun = "SubEthaHyperLogLog", alias = "Open-SEHyperLogLog", output = ["SubEtha.HyperLogLog"])]
#[derive(Default)]
pub struct OpenSubEthaHyperLogLog {
    /// The file the counter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The precision it was created with; fourteen when absent.
    #[param]
    pub precision: Option<u8>,
}

impl Cmdlet for OpenSubEthaHyperLogLog {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(HyperLogLog::obtain(path, self.precision.unwrap_or(14), true)?)
    }
}

/// A count-min sketch in a mapped file: it estimates how often it has
/// seen each item, never undercounting and sometimes overcounting.
#[psclass(name = "SubEtha.CountMinSketch", mode = proxy)]
pub struct CountMinSketch {
    /// The file the sketch lives in.
    pub path: String,
    /// How many hash rows the sketch has.
    pub depth: u32,
    /// How many counters each row has.
    pub width: u32,
    #[psfield(skip)]
    inner: SharedCountMinSketch,
}

impl CountMinSketch {
    fn obtain(path: String, depth: u32, width: u32, open: bool) -> PsResult<Self> {
        if depth == 0 || width == 0 {
            return Err(arg_err("the depth and the width must both be at least one"));
        }
        let inner = if open { SharedCountMinSketch::open(&path, depth, width) } else { SharedCountMinSketch::create(&path, depth, width) }
            .map_err(|e| open_err("the sketch", &path, e))?;
        Ok(Self { path, depth: inner.d(), width: inner.w(), inner })
    }
}

/// The operations of a `SubEtha.CountMinSketch`.
#[psmethods]
impl CountMinSketch {
    /// How many occurrences have been counted in all.
    pub fn total_inserts(&self) -> PsResult<u64> {
        Ok(self.inner.total_inserts())
    }

    /// Counts one occurrence of `item`.
    pub fn insert(&self, item: PsObject) -> PsResult<()> {
        let item = bytes(&item)?;
        self.inner.insert(&item);
        Ok(())
    }

    /// Counts `count` occurrences of `item` at once rather than
    /// calling Insert that many times.
    pub fn insert_n(&self, item: PsObject, count: u64) -> PsResult<()> {
        let item = bytes(&item)?;
        self.inner.insert_n(&item, count);
        Ok(())
    }

    /// Counts one occurrence of each of a run of items in one call.
    pub fn insert_many(&self, items: Vec<PsObject>) -> PsResult<u64> {
        for item in &items {
            let item = bytes(item)?;
            self.inner.insert(&item);
        }
        Ok(items.len() as u64)
    }

    /// How often it thinks it has seen `item`: never less than the true
    /// count, sometimes more.
    pub fn estimate_count(&self, item: PsObject) -> PsResult<u64> {
        let item = bytes(&item)?;
        Ok(self.inner.estimate_count(&item))
    }

    /// The estimate for each of a run of items in one call.
    pub fn estimate_many(&self, items: Vec<PsObject>) -> PsResult<Vec<u64>> {
        let mut answers = Vec::with_capacity(items.len());
        for item in &items {
            let item = bytes(item)?;
            answers.push(self.inner.estimate_count(&item));
        }
        Ok(answers)
    }

    /// Forgets everything.
    pub fn reset(&self) -> PsResult<()> {
        self.inner.reset();
        Ok(())
    }
}

/// Obtains the count-min sketch at Path of Depth rows by Width counters,
/// creating it when the file does not exist.
///
/// # Examples
///
/// `$sketch = New-SubEthaCountMinSketch -Path C:\ipc\countminsketch -Depth 5 -Width 272`
#[cmdlet(verb = "New", noun = "SubEthaCountMinSketch", alias = "New-SECountMinSketch", output = ["SubEtha.CountMinSketch"])]
#[derive(Default)]
pub struct NewSubEthaCountMinSketch {
    /// The file the sketch lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many hash rows the sketch has.
    #[param(mandatory, position = 1)]
    pub depth: u32,
    /// How many counters each row has.
    #[param(mandatory, position = 2)]
    pub width: u32,
}

impl Cmdlet for NewSubEthaCountMinSketch {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(CountMinSketch::obtain(path, self.depth, self.width, false)?)
    }
}

/// Attaches to the count-min sketch at Path, which must exist with the
/// depth and width it was created with.
///
/// # Examples
///
/// `$sketch = Open-SubEthaCountMinSketch -Path C:\ipc\countminsketch -Depth 5 -Width 272`
#[cmdlet(verb = "Open", noun = "SubEthaCountMinSketch", alias = "Open-SECountMinSketch", output = ["SubEtha.CountMinSketch"])]
#[derive(Default)]
pub struct OpenSubEthaCountMinSketch {
    /// The file the sketch lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many hash rows the sketch has.
    #[param(mandatory, position = 1)]
    pub depth: u32,
    /// How many counters each row has.
    #[param(mandatory, position = 2)]
    pub width: u32,
}

impl Cmdlet for OpenSubEthaCountMinSketch {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(CountMinSketch::obtain(path, self.depth, self.width, true)?)
    }
}

/// The depth and width for a count-min sketch with an error of Epsilon
/// at confidence Delta, so a caller sizes the sketch from what it
/// needs.
///
/// # Examples
///
/// `$size = Measure-SubEthaSketchSize -Epsilon 0.01 -Delta 0.01`
#[cmdlet(verb = "Measure", noun = "SubEthaSketchSize", alias = "Measure-SESketchSize", output = ["SubEtha.SketchSize"])]
#[derive(Default)]
pub struct MeasureSubEthaSketchSize {
    /// The error, as a fraction of the total count, above zero and
    /// below one.
    #[param(mandatory, position = 0)]
    pub epsilon: f64,
    /// The chance the error is exceeded, above zero and below one.
    #[param(mandatory, position = 1)]
    pub delta: f64,
}

impl Cmdlet for MeasureSubEthaSketchSize {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        if !(self.epsilon > 0.0 && self.epsilon < 1.0) {
            return Err(arg_err("epsilon must lie between zero and one"));
        }
        if !(self.delta > 0.0 && self.delta < 1.0) {
            return Err(arg_err("delta must lie between zero and one"));
        }
        let (depth, width) = SharedCountMinSketch::suggest_config(self.epsilon, self.delta);
        ps.write(SketchSize { depth, width })
    }
}

/// A histogram in a mapped file, with bucket boundaries the caller
/// chooses, that every process records into.
#[psclass(name = "SubEtha.Histogram", mode = proxy)]
pub struct Histogram {
    /// The file the histogram lives in.
    pub path: String,
    /// The bucket boundaries, rising.
    pub boundaries: Vec<u64>,
    /// How many buckets there are.
    pub buckets: u64,
    #[psfield(skip)]
    inner: SharedHistogram,
}

impl Histogram {
    fn obtain(path: String, boundaries: Vec<u64>, open: bool) -> PsResult<Self> {
        if boundaries.is_empty() {
            return Err(arg_err("a histogram needs at least one boundary"));
        }
        if boundaries.windows(2).any(|w| w[1] <= w[0]) {
            return Err(arg_err("the boundaries must rise"));
        }
        let inner = if open { SharedHistogram::open(&path, &boundaries) } else { SharedHistogram::create(&path, &boundaries) }
            .map_err(|e| open_err("the histogram", &path, e))?;
        Ok(Self { path, boundaries: inner.boundaries_vec(), buckets: inner.n_buckets() as u64, inner })
    }
}

/// The operations of a `SubEtha.Histogram`.
#[psmethods]
impl Histogram {
    /// How many values have been recorded in all.
    pub fn total_count(&self) -> PsResult<u64> {
        Ok(self.inner.total_count())
    }

    /// Every bucket's count in one call, which is what a reader wants;
    /// the alternative is a call per bucket.
    pub fn counts(&self) -> PsResult<Vec<u64>> {
        Ok(self.inner.counts())
    }

    /// Records one value and returns the bucket it fell in.
    pub fn record(&self, value: u64) -> PsResult<u64> {
        Ok(self.inner.record(value) as u64)
    }

    /// Records a run of values in one call.
    pub fn record_many(&self, values: Vec<u64>) -> PsResult<u64> {
        for value in &values {
            self.inner.record(*value);
        }
        Ok(values.len() as u64)
    }

    /// The count in `bucket`.
    pub fn count(&self, bucket: u64) -> PsResult<u64> {
        self.inner.count(size(bucket, "the bucket")?).map_err(|e| op_err("reading a bucket", e))
    }

    /// The value at percentile `p`, from the bucket boundaries, so it
    /// is as precise as the buckets are.
    pub fn percentile(&self, p: f64) -> PsResult<u64> {
        if !(0.0..=100.0).contains(&p) {
            return Err(arg_err("a percentile lies between 0 and 100"));
        }
        Ok(self.inner.percentile(p))
    }
}

/// Obtains the histogram at Path with the rising bucket Boundaries,
/// creating it when the file does not exist.
///
/// # Examples
///
/// `$histogram = New-SubEthaHistogram -Path C:\ipc\histogram -Boundaries @(10, 100, 1000)`
#[cmdlet(verb = "New", noun = "SubEthaHistogram", alias = "New-SEHistogram", output = ["SubEtha.Histogram"])]
#[derive(Default)]
pub struct NewSubEthaHistogram {
    /// The file the histogram lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bucket boundaries, which must rise.
    #[param(mandatory, position = 1)]
    pub boundaries: Vec<u64>,
}

impl Cmdlet for NewSubEthaHistogram {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Histogram::obtain(path, std::mem::take(&mut self.boundaries), false)?)
    }
}

/// Attaches to the histogram at Path, which must exist with the
/// Boundaries it was created with.
///
/// # Examples
///
/// `$histogram = Open-SubEthaHistogram -Path C:\ipc\histogram -Boundaries @(10, 100, 1000)`
#[cmdlet(verb = "Open", noun = "SubEthaHistogram", alias = "Open-SEHistogram", output = ["SubEtha.Histogram"])]
#[derive(Default)]
pub struct OpenSubEthaHistogram {
    /// The file the histogram lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// The bucket boundaries it was created with.
    #[param(mandatory, position = 1)]
    pub boundaries: Vec<u64>,
}

impl Cmdlet for OpenSubEthaHistogram {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(Histogram::obtain(path, std::mem::take(&mut self.boundaries), true)?)
    }
}

/// A token-bucket rate limiter in a mapped file, shared by every process
/// that opens it, so a rate is enforced across all of them rather than
/// per process.
#[psclass(name = "SubEtha.RateLimiter", mode = proxy)]
pub struct RateLimiter {
    /// The file the limiter lives in.
    pub path: String,
    /// How many tokens the bucket holds.
    pub capacity: u32,
    /// How many tokens come back each second.
    pub refill_per_second: u32,
    #[psfield(skip)]
    inner: SharedRateLimiter,
}

impl RateLimiter {
    fn obtain(path: String, capacity: u32, refill: u32, open: bool) -> PsResult<Self> {
        if capacity == 0 || refill == 0 {
            return Err(arg_err("the capacity and the refill rate must both be at least one"));
        }
        let inner = if open { SharedRateLimiter::open(&path, capacity, refill) } else { SharedRateLimiter::create(&path, capacity, refill) }
            .map_err(|e| open_err("the limiter", &path, e))?;
        Ok(Self { path, capacity: inner.capacity(), refill_per_second: inner.refill_rate_per_sec(), inner })
    }
}

/// The operations of a `SubEtha.RateLimiter`.
#[psmethods]
impl RateLimiter {
    /// Tokens available right now, which the refill moves on its own.
    pub fn available(&self) -> PsResult<u32> {
        Ok(self.inner.available())
    }

    /// Takes `n` tokens (one when absent) if they are there. False
    /// means they were not, which is an answer rather than a failure.
    pub fn try_acquire(&self, n: Option<u32>) -> PsResult<bool> {
        match self.inner.try_acquire(n.unwrap_or(1)) {
            Ok(()) => Ok(true),
            Err(RateLimiterError::InsufficientTokens { .. }) => Ok(false),
            Err(e) => Err(op_err("taking tokens", e)),
        }
    }

    /// Refills the bucket to its capacity.
    pub fn reset(&self) -> PsResult<()> {
        self.inner.reset();
        Ok(())
    }

    /// Writes the mapping through to the file.
    pub fn flush(&self) -> PsResult<()> {
        self.inner.flush().map_err(|e| op_err("flushing", e))
    }
}

/// Obtains the rate limiter at Path holding Capacity tokens that come
/// back at RefillPerSecond, creating it when the file does not exist.
///
/// # Examples
///
/// `$limiter = New-SubEthaRateLimiter -Path C:\ipc\ratelimiter -Capacity 100 -RefillPerSecond 10`
#[cmdlet(verb = "New", noun = "SubEthaRateLimiter", alias = "New-SERateLimiter", output = ["SubEtha.RateLimiter"])]
#[derive(Default)]
pub struct NewSubEthaRateLimiter {
    /// The file the limiter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many tokens the bucket holds.
    #[param(mandatory, position = 1)]
    pub capacity: u32,
    /// How many tokens come back each second.
    #[param(mandatory, position = 2)]
    pub refill_per_second: u32,
}

impl Cmdlet for NewSubEthaRateLimiter {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(RateLimiter::obtain(path, self.capacity, self.refill_per_second, false)?)
    }
}

/// Attaches to the rate limiter at Path, which must exist with the
/// capacity and rate it was created with.
///
/// # Examples
///
/// `$limiter = Open-SubEthaRateLimiter -Path C:\ipc\ratelimiter -Capacity 100 -RefillPerSecond 10`
#[cmdlet(verb = "Open", noun = "SubEthaRateLimiter", alias = "Open-SERateLimiter", output = ["SubEtha.RateLimiter"])]
#[derive(Default)]
pub struct OpenSubEthaRateLimiter {
    /// The file the limiter lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many tokens the bucket holds.
    #[param(mandatory, position = 1)]
    pub capacity: u32,
    /// How many tokens come back each second.
    #[param(mandatory, position = 2)]
    pub refill_per_second: u32,
}

impl Cmdlet for OpenSubEthaRateLimiter {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(RateLimiter::obtain(path, self.capacity, self.refill_per_second, true)?)
    }
}

/// A least-recently-used cache in a mapped file, shared by every process
/// that opens it.
///
/// Reading through Get does not count as use; GetAndTouch does. The two
/// are separate because a cache that is inspected by a monitor should
/// not have its eviction order rearranged by being looked at.
#[psclass(name = "SubEtha.LruCache", mode = proxy)]
pub struct LruCache {
    /// The file the cache lives in.
    pub path: String,
    /// How many entries it holds.
    pub capacity: u32,
    /// The bytes one key holds.
    pub key_size: u64,
    /// The bytes one value holds.
    pub value_size: u64,
    #[psfield(skip)]
    inner: RawLruCache,
}

impl LruCache {
    fn obtain(path: String, capacity: u32, key_size: u64, value_size: u64, open: bool) -> PsResult<Self> {
        if capacity == 0 {
            return Err(arg_err("the capacity must be at least one"));
        }
        let ks = size(key_size, "the key size")?;
        let vs = size(value_size, "the value size")?;
        let inner = if open { RawLruCache::open(&path, capacity, ks, vs) } else { RawLruCache::create(&path, capacity, ks, vs) }
            .map_err(|e| open_err("the cache", &path, e))?;
        Ok(Self { path, capacity: inner.capacity(), key_size, value_size, inner })
    }

    fn read(&self, key: &[u8], touch: bool) -> PsResult<Option<PsObject>> {
        let mut out = vec![0u8; self.inner.value_size()];
        let found = if touch { self.inner.get_and_touch(key, &mut out) } else { self.inner.get(key, &mut out) };
        match found {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("reading", e)),
        }
    }
}

/// The operations of a `SubEtha.LruCache`.
#[psmethods]
impl LruCache {
    /// How many entries are in it.
    pub fn count(&self) -> PsResult<u64> {
        Ok(self.inner.len() as u64)
    }

    /// The value under `key`, or `$null`, without changing the eviction
    /// order.
    pub fn get(&self, key: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        self.read(&key, false)
    }

    /// The value under `key`, or `$null`, counting it as use, which is
    /// what a cache client wants.
    pub fn get_and_touch(&self, key: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        self.read(&key, true)
    }

    /// The values under a run of keys, `$null` where a key is absent,
    /// without disturbing the order.
    pub fn get_many(&self, keys: Vec<PsObject>) -> PsResult<Vec<PsObject>> {
        let mut answers = Vec::with_capacity(keys.len());
        for key in &keys {
            let key = bytes(key)?;
            answers.push(self.read(&key, false)?.unwrap_or_default());
        }
        Ok(answers)
    }

    /// Counts `key` as used without reading it. False when it is
    /// absent.
    pub fn touch(&self, key: PsObject) -> PsResult<bool> {
        let key = bytes(&key)?;
        self.inner.touch(&key).map_err(|e| op_err("touching", e))
    }

    /// Whether `key` is in the cache.
    pub fn contains(&self, key: PsObject) -> PsResult<bool> {
        let key = bytes(&key)?;
        self.inner.contains_key(&key).map_err(|e| op_err("looking up", e))
    }

    /// Puts `value` under `key` at the most recent end, evicting the
    /// least recently used entry first when the cache is full. True
    /// means the key was already present and its value was replaced.
    pub fn put(&self, key: PsObject, value: PsObject) -> PsResult<bool> {
        let key = bytes(&key)?;
        let value = bytes(&value)?;
        self.inner.put(&key, &value).map_err(|e| op_err("inserting", e))
    }

    /// Puts each of `values` under the key beside it in `keys`, and
    /// returns how many were put.
    pub fn put_many(&self, keys: Vec<PsObject>, values: Vec<PsObject>) -> PsResult<u64> {
        if keys.len() != values.len() {
            return Err(arg_err("the keys and the values must be the same length"));
        }
        for (key, value) in keys.iter().zip(&values) {
            let key = bytes(key)?;
            let value = bytes(value)?;
            self.inner.put(&key, &value).map_err(|e| op_err("inserting", e))?;
        }
        Ok(keys.len() as u64)
    }

    /// Takes `key` out and returns its value, or `$null` when it was
    /// absent.
    pub fn remove(&self, key: PsObject) -> PsResult<Option<PsObject>> {
        let key = bytes(&key)?;
        let mut out = vec![0u8; self.inner.value_size()];
        match self.inner.remove(&key, &mut out) {
            Ok(true) => Ok(Some(out_bytes(&out)?)),
            Ok(false) => Ok(None),
            Err(e) => Err(op_err("removing", e)),
        }
    }
}

/// Obtains the cache at Path holding Capacity entries of KeySize-byte
/// keys and ValueSize-byte values, creating it when the file does not
/// exist.
///
/// # Examples
///
/// `$cache = New-SubEthaLruCache -Path C:\ipc\lrucache -Capacity 256 -KeySize 8 -ValueSize 64`
#[cmdlet(verb = "New", noun = "SubEthaLruCache", alias = "New-SELruCache", output = ["SubEtha.LruCache"])]
#[derive(Default)]
pub struct NewSubEthaLruCache {
    /// The file the cache lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u32,
    /// The bytes one key holds.
    #[param(mandatory, position = 2)]
    pub key_size: u64,
    /// The bytes one value holds.
    #[param(mandatory, position = 3)]
    pub value_size: u64,
}

impl Cmdlet for NewSubEthaLruCache {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LruCache::obtain(path, self.capacity, self.key_size, self.value_size, false)?)
    }
}

/// Attaches to the cache at Path, which must exist with the capacity
/// and sizes it was created with.
///
/// # Examples
///
/// `$cache = Open-SubEthaLruCache -Path C:\ipc\lrucache -Capacity 256 -KeySize 8 -ValueSize 64`
#[cmdlet(verb = "Open", noun = "SubEthaLruCache", alias = "Open-SELruCache", output = ["SubEtha.LruCache"])]
#[derive(Default)]
pub struct OpenSubEthaLruCache {
    /// The file the cache lives in.
    #[param(mandatory, position = 0)]
    pub path: String,
    /// How many entries it holds.
    #[param(mandatory, position = 1)]
    pub capacity: u32,
    /// The bytes one key holds.
    #[param(mandatory, position = 2)]
    pub key_size: u64,
    /// The bytes one value holds.
    #[param(mandatory, position = 3)]
    pub value_size: u64,
}

impl Cmdlet for OpenSubEthaLruCache {
    fn process(&mut self, ps: &Pipeline<'_>) -> PsResult<()> {
        let path = full_path(ps, &self.path)?;
        ps.write(LruCache::obtain(path, self.capacity, self.key_size, self.value_size, true)?)
    }
}
