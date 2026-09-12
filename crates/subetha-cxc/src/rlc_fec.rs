//! Sliding-window Random Linear Code (RLC) forward erasure correction: a
//! convolutional erasure code that interleaves repair symbols with the source
//! symbols over a sliding window, so an isolated loss is recovered from the
//! next repair without waiting for a block boundary.
//!
//! This is the convolutional counterpart of the block Cauchy Reed-Solomon code
//! in [`crate::fec`]. A block code sends `k` source shards then `r` parity
//! shards; to recover a loss the decoder must wait for the rest of the block,
//! by which time the loss detector has often already fired a wasted retransmit.
//! A sliding-window RLC, by contrast, emits one repair symbol every few source
//! symbols, each a random linear combination of the source symbols currently in
//! the window, so a single loss is recovered as soon as the next repair
//! arrives - an RTT-independent, near-instant recovery that suits low-latency
//! streams.
//!
//! The repair symbol over a window of source symbols `s_i` is
//! `sum_i coef_i * s_i` over GF(2^8) with the field's `0x11D` polynomial - the
//! *same* field as the block RS code, so the linear combination rides the
//! GF(2^8) SIMD ladder ([`crate::fec::gf_mul_add_auto`]) with no new kernel.
//! The coefficients come from `TAPS`, a table of sixty-four constants the
//! specification prints and both ends read: a symbol is multiplied by the tap
//! for its place in the window, so the decoder reconstructs the coefficients
//! from the repair's window alone.
//!
//! A fixed generator is enough because the code is convolutional. Successive
//! repairs cover windows that have shifted, so their equations differ even
//! though the generator does not, and a coefficient generated per repair adds
//! nothing the window's own motion was not already providing.
//!
//! The decoder maintains a linear system over GF(2^8) of the received source
//! and repair symbols and solves it by Gaussian elimination; a lost symbol is
//! recovered the moment the received equations determine it. The heavy work -
//! combining symbol-vectors during elimination - is `gf_mul_add` over the
//! payload bytes (SIMD-accelerated); the small coefficient matrix (at most one
//! column per source symbol in the window) uses scalar GF(2^8) arithmetic.

use crate::fec::{gf, gf_mul_add_auto};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

/// Default density threshold: every coefficient nonzero (maximum density),
/// which maximizes the recovery probability for small windows.
pub const DEFAULT_DT: u8 = 15;

/// The published generator taps, most recent symbol first.
///
/// A repair multiplies the newest source symbol in its window by
/// `TAPS[0]`, the one before it by `TAPS[1]`, and so on.
///
/// There is one tap per position the encoder's widest window can hold,
/// because the table length is a ceiling on how far a repair reaches: a
/// position with no tap takes a zero and does not enter the equation at
/// all. A shorter table than the window silently leaves the oldest
/// symbols unprotected, and the controller widens the window to
/// [`crate::rlc_control::WINDOW_MAX`] under bursty loss - exactly the
/// case the width is for.
///
/// These are constants of the format, not values either end works out.
/// The encoder and decoder agree because they read the same table, which
/// is what a specification can print and a second implementation can
/// copy. Nothing is seeded and nothing is derived from a key.
const TAPS: [u8; 64] = [
    0x01, 0x02, 0x03, 0x05, 0x07, 0x0b, 0x0d, 0x11,
    0x13, 0x17, 0x1d, 0x1f, 0x25, 0x29, 0x2b, 0x2f,
    0x35, 0x3b, 0x3d, 0x43, 0x47, 0x49, 0x4f, 0x53,
    0x59, 0x61, 0x65, 0x67, 0x6b, 0x6d, 0x71, 0x7f,
    0x83, 0x89, 0x8b, 0x95, 0x97, 0x9d, 0xa3, 0xa7,
    0xad, 0xb3, 0xb5, 0xbf, 0xc1, 0xc5, 0xc7, 0xd3,
    0xdf, 0xe3, 0xe5, 0xe9, 0xef, 0xf1, 0xf5, 0xf7,
    0xfb, 0xfd, 0x04, 0x08, 0x0e, 0x16, 0x1a, 0x22,
];

/// How many window positions one density step covers. The density
/// threshold runs 0..=15 because it occupies a nibble on the wire, so a
/// position's density class is its place modulo this.
const DENSITY_CLASSES: usize = 16;

/// The GF(2^8) coefficient a fixed-generator repair gives the source
/// symbol sitting `tap` places back from the newest in its window.
///
/// Successive repairs cover windows that have shifted, so their
/// equations differ even though the generator does not - which is what
/// a convolutional code has always done, and why it needs no per-repair
/// coefficient at all.
fn fixed_coef(tap: usize, dt: u8) -> u8 {
    if tap >= TAPS.len() {
        return 0;
    }
    // `dt` is a density, not a reach: it says what fraction of the
    // window carries a coefficient, and that fraction is spread across
    // the whole window rather than filling the newest end of it. A
    // position is in if its density class is at or below the threshold,
    // so `dt` of 15 takes every position and `dt` of 0 takes one in
    // sixteen, at every depth.
    //
    // Reading `dt` as a reach instead caps protection at sixteen
    // symbols however wide the window is, which makes the controller's
    // answer to a long burst - widening the window - do nothing.
    if tap % DENSITY_CLASSES <= dt as usize {
        TAPS[tap]
    } else {
        0
    }
}

/// A repair symbol: a linear combination of the source symbols in the
/// sliding window at the moment it was generated. The metadata lets the decoder
/// reconstruct the exact coefficients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairSymbol {
    /// This repair's sequence number, counting up from zero per encoder.
    /// It names the repair in a log or a trace and takes no part in
    /// decoding: the coefficients come from the window, not from here.
    pub repair_key: u32,
    /// Lowest source id in the covered window.
    pub first_source_id: u32,
    /// Number of source symbols in the covered window.
    pub window_size: u16,
    /// Density threshold in the low nibble, generator id in the high
    /// one. Read them with [`density`](Self::density) and
    /// [`generator`](Self::generator) rather than as a whole byte.
    ///
    /// The density takes 0..=15, which leaves the high nibble for the
    /// generator id without the header growing a byte. Generator id 0 in
    /// the high nibble names [`GENERATOR_PER_REPAIR`].
    pub dt: u8,
    /// `sum_i coef_i * source_i` over the window.
    pub payload: Vec<u8>,
}

/// Generator id 0: a coefficient drawn per (repair, source) from a
/// seeded hash. This build neither produces nor decodes it. The id stays
/// reserved, so a repair carrying it is dropped and counted as one this
/// build cannot read rather than decoded as another generator.
pub const GENERATOR_PER_REPAIR: u8 = 0;

/// Coefficients from the published `TAPS`, keyed by a symbol's place
/// in the window.
pub const GENERATOR_PUBLISHED_TAPS: u8 = 1;

/// Repairs dropped for naming a generator this build cannot read,
/// summed over the process.
static REFUSED_REPAIRS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many repairs this process has dropped for naming a generator it
/// cannot read.
///
/// A count above zero on a link that then reports no recovery is a peer
/// naming generator 0, not a bad channel. Without this count the two
/// look identical from the outside: repairs arrive, nothing is
/// recovered, and nothing says why.
pub fn refused_repairs() -> u64 {
    REFUSED_REPAIRS.load(std::sync::atomic::Ordering::Relaxed)
}

impl RepairSymbol {
    /// The density threshold, 0..=15.
    pub fn density(&self) -> u8 {
        self.dt & 0x0f
    }

    /// Which generator produced this repair's coefficients: one of the
    /// `GENERATOR_` constants.
    pub fn generator(&self) -> u8 {
        self.dt >> 4
    }

    /// The coefficient this repair gives the source symbol `off` places
    /// after the first in its window.
    ///
    /// Only [`GENERATOR_PUBLISHED_TAPS`] is produced or read; a repair
    /// naming any other generator is refused by
    /// [`RlcDecoder::add_repair`] before it reaches here.
    fn coefficient(&self, off: u32) -> u8 {
        let newest = (self.window_size as u32).saturating_sub(1);
        fixed_coef((newest - off) as usize, self.density())
    }

    /// Whether this build can read this repair's coefficients.
    pub fn generator_is_readable(&self) -> bool {
        self.generator() == GENERATOR_PUBLISHED_TAPS
    }
}

/// Sliding-window RLC encoder: holds the last `window_max` source symbols and
/// emits one repair symbol every `step` source symbols.
#[derive(Debug)]
pub struct RlcEncoder {
    window: VecDeque<(u32, Vec<u8>)>,
    window_max: usize,
    step: usize,
    dt: u8,
    symbol_len: usize,
    next_source_id: u32,
    since_last_repair: usize,
    next_repair_key: u32,
    /// Whether repairs are emitted at all. `false` is the disable-on-clean
    /// state: source symbols still flow and the window is still maintained (so
    /// re-arming is instant), but no repair rides the wire.
    coding_on: bool,
}

impl RlcEncoder {
    /// Build an encoder over `symbol_len`-byte symbols with a window of up to
    /// `window_max` source symbols, emitting one repair every `step` source
    /// symbols at density threshold `dt`. The code rate is `step / (step + 1)`.
    pub fn new(window_max: usize, step: usize, dt: u8, symbol_len: usize) -> Self {
        Self {
            window: VecDeque::new(),
            window_max: window_max.max(1),
            step: step.max(1),
            dt: dt.min(15),
            symbol_len,
            next_source_id: 0,
            since_last_repair: 0,
            next_repair_key: 0,
            coding_on: true,
        }
    }

    /// Retune the coding parameters at runtime (the adaptive control path): the
    /// window size, the repair cadence `step` (code rate `step / (step + 1)`),
    /// and the coefficient density `dt`. Shrinking the window trims the oldest
    /// source symbols immediately so the next repair spans only the new window.
    pub fn set_params(&mut self, window_max: usize, step: usize, dt: u8) {
        self.window_max = window_max.max(1);
        self.step = step.max(1);
        self.dt = dt.min(15);
        while self.window.len() > self.window_max {
            self.window.pop_front();
        }
    }

    /// Turn repair emission on or off (disable-on-clean). The window keeps
    /// filling either way, so re-enabling protects the in-flight symbols at once.
    pub fn set_coding(&mut self, on: bool) {
        self.coding_on = on;
    }

    /// The live `(window_max, step, dt)` parameters (telemetry).
    pub fn params(&self) -> (usize, usize, u8) {
        (self.window_max, self.step, self.dt)
    }

    /// Whether repair emission is currently active.
    pub fn coding_on(&self) -> bool {
        self.coding_on
    }

    /// Add one source symbol. Returns its assigned source id and, every `step`
    /// symbols (while coding is on), a repair symbol to interleave onto the wire
    /// after it.
    pub fn push_source(&mut self, payload: &[u8]) -> (u32, Option<RepairSymbol>) {
        debug_assert_eq!(payload.len(), self.symbol_len);
        let sid = self.next_source_id;
        self.next_source_id = self.next_source_id.wrapping_add(1);
        self.window.push_back((sid, payload.to_vec()));
        while self.window.len() > self.window_max {
            self.window.pop_front();
        }
        self.since_last_repair += 1;
        let repair = if self.coding_on && self.since_last_repair >= self.step {
            self.since_last_repair = 0;
            Some(self.emit_repair())
        } else {
            None
        };
        (sid, repair)
    }

    /// Drop acknowledged-or-recovered source symbols below `floor` from the
    /// window (the elastic-window feedback path); the window never protects
    /// data the peer already has.
    pub fn forget_below(&mut self, floor: u32) {
        while let Some((sid, _)) = self.window.front() {
            if *sid < floor {
                self.window.pop_front();
            } else {
                break;
            }
        }
    }

    /// Re-base the source-id stream to `base` for a cross-code resync: the next
    /// source symbol is assigned id `base` and the coding window starts empty, so
    /// repairs reference only post-rebase symbols. Used when another code carried
    /// the ids between this code's old running id and `base`, so it must resume at
    /// `base` rather than its own (now-diverged) counter. The repair-key counter
    /// keeps running (keys are matched to ids by coefficient, not by equality).
    pub fn rebase_to(&mut self, base: u32) {
        self.next_source_id = base;
        self.window.clear();
        self.since_last_repair = 0;
    }

    fn emit_repair(&mut self) -> RepairSymbol {
        let repair_key = self.next_repair_key;
        self.next_repair_key = self.next_repair_key.wrapping_add(1);
        let first = self.window.front().map(|(id, _)| *id).unwrap_or(0);
        let mut payload = vec![0u8; self.symbol_len];
        let newest = self.window.len().saturating_sub(1);
        for (i, (_, sym)) in self.window.iter().enumerate() {
            let c = fixed_coef(newest - i, self.dt);
            if c != 0 {
                gf_mul_add_auto(&mut payload, sym, c);
            }
        }
        RepairSymbol {
            repair_key,
            first_source_id: first,
            window_size: self.window.len() as u16,
            dt: (GENERATOR_PUBLISHED_TAPS << 4) | (self.dt & 0x0f),
            payload,
        }
    }

    /// The next source id that will be assigned.
    pub fn next_source_id(&self) -> u32 {
        self.next_source_id
    }
}

/// Sliding-window RLC decoder: stores received source and repair symbols and
/// recovers lost source symbols by Gaussian elimination over GF(2^8).
#[derive(Debug)]
pub struct RlcDecoder {
    symbol_len: usize,
    source: BTreeMap<u32, Vec<u8>>,
    repairs: Vec<RepairSymbol>,
    /// Highest source id seen on any source or repair, for the recovery horizon.
    highest: u32,
    /// RLC solving is bounded to source ids within `horizon` of `highest`: a
    /// sliding-window code can only recover within its window, so a gap older
    /// than this is the ARQ floor's job, not RLC's. Bounding the solve keeps
    /// the per-packet cost constant instead of growing with history.
    horizon: u32,
    /// Repairs refused because they named a generator this build does not
    /// read - a peer still speaking the 0.2.x per-repair coefficients.
    /// Counted rather than discarded quietly, so a stream that recovers
    /// nothing can be told from one that had nothing to recover.
    refused_repairs: u64,
}

impl RlcDecoder {
    /// Build a decoder over `symbol_len`-byte symbols.
    pub fn new(symbol_len: usize) -> Self {
        Self {
            symbol_len,
            source: BTreeMap::new(),
            repairs: Vec::new(),
            highest: 0,
            horizon: 1024,
            refused_repairs: 0,
        }
    }

    /// Set the RLC recovery horizon (source ids back from the newest that the
    /// solver considers). Should comfortably exceed the encoder's window so a
    /// repair's whole window is in scope; gaps older than this fall to ARQ.
    pub fn with_horizon(mut self, horizon: u32) -> Self {
        self.horizon = horizon.max(1);
        self
    }

    /// Record a received source symbol. A source arrival fills its own slot
    /// directly; recovery of other symbols is driven by repair arrivals
    /// ([`on_repair`](Self::on_repair)), so this skips the (potentially
    /// expensive) Gaussian solve - that would otherwise fire on every single
    /// packet under loss, re-solving the whole in-horizon system each time, when
    /// a new source adds no equation. A late source that completes a pending
    /// system is recovered on the next repair (one arrives every `step`
    /// symbols), and anything that slips through is caught by the ARQ floor.
    pub fn on_source(&mut self, source_id: u32, payload: &[u8]) -> Vec<u32> {
        debug_assert_eq!(payload.len(), self.symbol_len);
        self.highest = self.highest.max(source_id);
        self.source
            .entry(source_id)
            .or_insert_with(|| payload.to_vec());
        Vec::new()
    }

    /// Record a received repair symbol. Returns any source ids newly recovered.
    pub fn on_repair(&mut self, r: RepairSymbol) -> Vec<u32> {
        self.add_repair(r);
        self.try_recover()
    }

    /// Store a repair without solving, for a caller that drains a batch of
    /// datagrams first (stamping their arrival before any decode) and then runs
    /// one [`recover`](Self::recover) over the whole batch - keeping the
    /// expensive Gaussian solve out of the receive/timing path.
    /// A repair naming a generator this build does not read is counted
    /// and dropped rather than stored. Storing it would put an equation
    /// into the system whose coefficients this decoder cannot reproduce,
    /// and a wrong equation does not fail to solve - it solves to the
    /// wrong bytes.
    pub fn add_repair(&mut self, r: RepairSymbol) {
        if !r.generator_is_readable() {
            self.refused_repairs += 1;
            REFUSED_REPAIRS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
        self.highest = self
            .highest
            .max(r.first_source_id.wrapping_add(r.window_size as u32).saturating_sub(1));
        self.repairs.push(r);
    }

    /// Repairs refused so far for naming an unreadable generator. Nonzero
    /// means a peer is still sending the 0.2.x per-repair coefficients.
    pub fn refused_repairs(&self) -> u64 {
        self.refused_repairs
    }

    /// Run one recovery pass over the currently-stored source and repair symbols,
    /// returning any source ids newly recovered. Pairs with [`add_repair`](Self::add_repair).
    pub fn recover(&mut self) -> Vec<u32> {
        self.try_recover()
    }

    /// The bytes of source symbol `source_id`, if received or recovered.
    pub fn get(&self, source_id: u32) -> Option<&[u8]> {
        self.source.get(&source_id).map(|v| v.as_slice())
    }

    /// Whether source symbol `source_id` is present (received or recovered).
    pub fn has(&self, source_id: u32) -> bool {
        self.source.contains_key(&source_id)
    }

    /// Drop delivered source symbols and spent repairs below `floor`, to bound
    /// memory on a long-lived flow. A source symbol a remaining repair still
    /// references is kept regardless (the decoder must subtract its known
    /// contribution when solving), so the source floor is capped at the oldest
    /// remaining repair's window start - forgetting it otherwise would make a
    /// received symbol look unknown and corrupt the linear system.
    pub fn forget_below(&mut self, floor: u32) {
        self.repairs
            .retain(|r| r.first_source_id.wrapping_add(r.window_size as u32) > floor);
        let safe = match self.repairs.iter().map(|r| r.first_source_id).min() {
            Some(oldest) => floor.min(oldest),
            None => floor,
        };
        self.source.retain(|&sid, _| sid >= safe);
    }

    /// Re-base the decoder to deliver from `base`: drop all stored source and
    /// repair symbols (they belong to the pre-rebase id range another code now
    /// owns) and anchor the recovery horizon at `base`. The receiver moves its
    /// delivery frontier to `base` in lockstep, so nothing below `base` is ever
    /// looked up again.
    pub fn rebase_to(&mut self, base: u32) {
        self.source.clear();
        self.repairs.clear();
        self.highest = base;
    }

    fn try_recover(&mut self) -> Vec<u32> {
        // Recovery is scoped to the horizon: a sliding-window code cannot use a
        // repair whose window has aged out, so drop those and only treat
        // in-horizon gaps as RLC unknowns (older gaps fall to the ARQ floor).
        // This keeps the solve bounded instead of growing with history.
        let lo = self.highest.saturating_sub(self.horizon);
        self.repairs
            .retain(|r| r.first_source_id.wrapping_add(r.window_size as u32) > lo);
        let mut unknown_set: BTreeSet<u32> = BTreeSet::new();
        for r in &self.repairs {
            for off in 0..r.window_size as u32 {
                let sid = r.first_source_id.wrapping_add(off);
                if sid >= lo && !self.source.contains_key(&sid) {
                    unknown_set.insert(sid);
                }
            }
        }
        if unknown_set.is_empty() {
            self.prune();
            return Vec::new();
        }
        let unknowns: Vec<u32> = unknown_set.into_iter().collect();
        let idx: HashMap<u32, usize> = unknowns.iter().enumerate().map(|(i, &s)| (s, i)).collect();
        let ncols = unknowns.len();

        // One row per repair covering at least one unknown: a coefficient
        // vector over the unknowns and an rhs symbol-vector with the known
        // source contributions already moved across (rhs ^= c * known_source).
        struct Row {
            coefs: Vec<u8>,
            rhs: Vec<u8>,
        }
        let mut rows: Vec<Row> = Vec::new();
        for r in &self.repairs {
            let mut coefs = vec![0u8; ncols];
            let mut rhs = r.payload.clone();
            let mut covers_unknown = false;
            let mut usable = true;
            for off in 0..r.window_size as u32 {
                let sid = r.first_source_id.wrapping_add(off);
                let c = r.coefficient(off);
                if c == 0 {
                    continue;
                }
                if let Some(sym) = self.source.get(&sid) {
                    gf_mul_add_auto(&mut rhs, sym, c);
                } else if sid >= lo {
                    coefs[idx[&sid]] = c;
                    covers_unknown = true;
                } else {
                    // An unknown below the horizon is out of RLC scope; this
                    // repair cannot be used here (the ARQ floor recovers that
                    // older gap).
                    usable = false;
                    break;
                }
            }
            if usable && covers_unknown {
                rows.push(Row { coefs, rhs });
            }
        }

        // Reduced row echelon over GF(2^8).
        let mut pivot = 0usize;
        for col in 0..ncols {
            let sel = (pivot..rows.len()).find(|&r| rows[r].coefs[col] != 0);
            let Some(sel) = sel else { continue };
            rows.swap(pivot, sel);
            let inv = gf::inv(rows[pivot].coefs[col]);
            for cf in rows[pivot].coefs.iter_mut() {
                *cf = gf::mul(*cf, inv);
            }
            gf_scale(&mut rows[pivot].rhs, inv);
            // Snapshot the pivot row so the elimination loop can borrow `rows`
            // mutably for every other row without aliasing.
            let pivot_coefs = rows[pivot].coefs.clone();
            let pivot_rhs = rows[pivot].rhs.clone();
            for (r, row) in rows.iter_mut().enumerate() {
                if r == pivot {
                    continue;
                }
                let f = row.coefs[col];
                if f == 0 {
                    continue;
                }
                for (rc, &pc) in row.coefs.iter_mut().zip(&pivot_coefs) {
                    *rc ^= gf::mul(f, pc);
                }
                gf_mul_add_auto(&mut row.rhs, &pivot_rhs, f);
            }
            pivot += 1;
        }

        // A row that reduced to a single unit coefficient determines that
        // unknown: x = rhs.
        let mut recovered = Vec::new();
        for row in &rows {
            let nz: Vec<usize> = (0..ncols).filter(|&c| row.coefs[c] != 0).collect();
            if nz.len() == 1 && row.coefs[nz[0]] == 1 {
                let sid = unknowns[nz[0]];
                if let std::collections::btree_map::Entry::Vacant(e) = self.source.entry(sid) {
                    e.insert(row.rhs.clone());
                    recovered.push(sid);
                }
            }
        }
        self.prune();
        recovered
    }

    /// Drop repairs whose covered source symbols are all known: they carry no
    /// further information and keeping them only grows the linear system.
    fn prune(&mut self) {
        let source = &self.source;
        self.repairs.retain(|r| {
            (0..r.window_size as u32)
                .any(|off| !source.contains_key(&r.first_source_id.wrapping_add(off)))
        });
    }
}

/// `v[i] = gf::mul(v[i], coef)` in place - a GF(2^8) scale of one
/// symbol-vector, used once per pivot to normalize the pivot row's rhs.
///
/// Runs on the same SIMD ladder as the multiply-add sites around it. A
/// symbol is up to a full datagram and a window reaches 64 pivots, so a
/// byte-at-a-time scale here is the same order of work as the accumulate
/// steps it sits beside.
fn gf_scale(v: &mut [u8], coef: u8) {
    crate::fec::gf_mul_auto(v, coef);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic source symbol of `len` bytes for source id `sid`.
    fn make_symbol(sid: u32, len: usize) -> Vec<u8> {
        (0..len)
            .map(|b| ((sid as usize * 131 + b * 17 + 7) & 0xff) as u8)
            .collect()
    }

    /// The geometries the interoperability vectors are emitted at.
    ///
    /// One geometry would let an implementation agree with us at that
    /// point and disagree everywhere else, which is how the density was
    /// read as a reach for as long as it was: the case that exposed it
    /// lives at a window wider than one density class, and nothing
    /// narrower can see it. So the set spans what the controller can
    /// actually ask for - the window from a few symbols to
    /// [`crate::rlc_control::WINDOW_MAX`], and the density across the
    /// nibble it occupies.
    const VECTOR_GEOMETRIES: &[(&str, usize, usize, u8)] = &[
        ("narrow-window-full-density", 8, 4, 15),
        ("one-density-class-wide", 16, 4, 15),
        ("past-one-density-class", 48, 8, 15),
        ("widest-window", 64, 8, 15),
        ("wide-window-sparsest-density", 48, 8, 0),
        ("mid-density", 16, 4, 7),
    ];

    /// Interoperability vectors for a second implementation of the wire
    /// format, regenerated here and compared against the committed file
    /// so a change to the format has to change the vectors with it.
    ///
    /// Set `SUBETHA_REGENERATE_VECTORS=1` to rewrite the file. Every
    /// value in it is a constant of the format: the tap table, the
    /// coefficient each (place, density) pair selects, the repairs a
    /// stated source stream produces, and which symbols a stated loss
    /// pattern gets back. Nothing is seeded, so an implementation that
    /// has only the specification can produce the same bytes.
    #[test]
    fn the_interoperability_vectors_match_the_committed_file() {
        let mut out = String::new();
        out.push_str(
            "# SubEtha RLC interoperability vectors\n\
             #\n\
             # Regenerate with SUBETHA_REGENERATE_VECTORS=1; the test that\n\
             # writes this file also compares it, so the format cannot move\n\
             # without these moving.\n\
             #\n\
             # Source symbol `sid` of `len` bytes is, for byte b in 0..len:\n\
             #     (sid * 131 + b * 17 + 7) & 0xff\n\
             # so an implementation needs no random source to reproduce it.\n\
             #\n\
             # A repair line is: seq, the lowest source id its window covers,\n\
             # the number of symbols in that window, the dt byte whole, and\n\
             # the payload. dt carries the density in the low nibble and the\n\
             # generator id in the high one.\n\n",
        );

        out.push_str(&format!("[generators]\nper_repair={GENERATOR_PER_REPAIR} (refused)\npublished_taps={GENERATOR_PUBLISHED_TAPS}\n\n"));

        out.push_str(&format!("[taps] {} entries\n", TAPS.len()));
        for (i, chunk) in TAPS.chunks(8).enumerate() {
            let row: Vec<String> = chunk.iter().map(|t| format!("{t:02x}")).collect();
            out.push_str(&format!("{:3}: {}\n", i * 8, row.join(" ")));
        }
        out.push('\n');

        out.push_str(&format!(
            "[coefficients] fixed_coef(place, density), place 0..{} across the row\n",
            TAPS.len()
        ));
        for dt in 0..=15u8 {
            let row: Vec<String> =
                (0..TAPS.len()).map(|t| format!("{:02x}", fixed_coef(t, dt))).collect();
            out.push_str(&format!("density {dt:2}: {}\n", row.join(" ")));
        }
        out.push('\n');

        const SOURCES: u32 = 96;
        const SYMBOL_LEN: usize = 16;
        // Every seventh symbol is dropped: isolated losses, which is what
        // a repair covering its whole window is meant to carry.
        const LOSS_EVERY: u32 = 7;

        for (name, window, step, dt) in VECTOR_GEOMETRIES {
            out.push_str(&format!(
                "[stream {name}] window_max={window} step={step} density={dt} \
                 symbol_len={SYMBOL_LEN} sources={SOURCES}\n"
            ));
            let mut enc = RlcEncoder::new(*window, *step, *dt, SYMBOL_LEN);
            let mut dec = RlcDecoder::new(SYMBOL_LEN).with_horizon(SOURCES);
            let mut lost = Vec::new();
            let mut recovered = Vec::new();

            for sid in 0..SOURCES {
                let symbol = make_symbol(sid, SYMBOL_LEN);
                let (id, repair) = enc.push_source(&symbol);
                assert_eq!(id, sid, "source ids count up from zero");
                if sid % LOSS_EVERY == 0 {
                    lost.push(sid);
                } else {
                    dec.on_source(sid, &symbol);
                }
                if let Some(r) = repair {
                    let payload: Vec<String> =
                        r.payload.iter().map(|b| format!("{b:02x}")).collect();
                    out.push_str(&format!(
                        "repair seq={} first={} window={} dt=0x{:02x} payload={}\n",
                        r.repair_key,
                        r.first_source_id,
                        r.window_size,
                        r.dt,
                        payload.join(""),
                    ));
                    assert_eq!(
                        r.generator(),
                        GENERATOR_PUBLISHED_TAPS,
                        "every repair names the only generator that ships",
                    );
                    recovered.extend(dec.on_repair(r));
                }
            }
            recovered.sort_unstable();
            recovered.dedup();

            // The ids are recorded rather than their bytes, because the
            // bytes are the source rule and printing them again would
            // only restate it. That they equal it is asserted instead,
            // so a recovery that returns the wrong bytes fails here.
            for id in &recovered {
                assert_eq!(
                    dec.get(*id),
                    Some(make_symbol(*id, SYMBOL_LEN).as_slice()),
                    "recovered symbol {id} is the symbol that was sent",
                );
            }
            let ids: Vec<String> = recovered.iter().map(|i| i.to_string()).collect();
            out.push_str(&format!(
                "lost {} of {SOURCES}: every {LOSS_EVERY}th id\nrecovered {} of {}: {}\n\n",
                lost.len(),
                recovered.len(),
                lost.len(),
                ids.join(" "),
            ));
        }

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("vectors")
            .join("rlc.txt");
        if std::env::var_os("SUBETHA_REGENERATE_VECTORS").is_some() {
            std::fs::create_dir_all(path.parent().expect("the vectors directory has a parent"))
                .expect("create the vectors directory");
            std::fs::write(&path, &out).expect("write the vectors file");
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "the vectors file at {} could not be read ({e}); regenerate it with \
                 SUBETHA_REGENERATE_VECTORS=1",
                path.display()
            )
        });
        assert_eq!(
            committed.replace("\r\n", "\n"),
            out,
            "the wire format moved and {} did not follow; regenerate it with \
             SUBETHA_REGENERATE_VECTORS=1 and read the diff before committing, \
             because a change here is a change a second implementation has to make too",
            path.display(),
        );
    }

    /// After a cross-code resync, the encoder re-bases to the new id and its
    /// repairs reference only post-rebase symbols, and a re-based decoder recovers
    /// a post-rebase loss without ever touching the abandoned pre-rebase ids.
    #[test]
    fn rebase_resumes_a_clean_recoverable_stream() {
        let len = 32;
        let mut enc = RlcEncoder::new(8, 4, 15, len);
        for sid in 0..10u32 {
            enc.push_source(&make_symbol(sid, len));
        }
        // Resync: another code carried [10, 5000); RLC resumes at 5000.
        enc.rebase_to(5000);
        assert_eq!(enc.next_source_id(), 5000);

        let mut dec = RlcDecoder::new(len).with_horizon(64);
        dec.rebase_to(5000);
        // Push four post-rebase symbols, dropping the first (5000), keeping its
        // repair, and verify RLC recovers it - proving the re-based window is a
        // self-contained linear system anchored at the new base.
        let mut repair = None;
        for sid in 5000..5004u32 {
            let (id, rep) = enc.push_source(&make_symbol(sid, len));
            assert_eq!(id, sid);
            if sid != 5000 {
                dec.on_source(sid, &make_symbol(sid, len));
            }
            if let Some(r) = rep {
                repair = Some(r);
            }
        }
        let recovered = dec.on_repair(repair.expect("a repair fires every step=4"));
        assert!(recovered.contains(&5000), "re-based loss recovers: {recovered:?}");
        assert_eq!(dec.get(5000), Some(make_symbol(5000, len).as_slice()));
        // Nothing below the rebase base is present (the old range was abandoned).
        assert!(!dec.has(9), "pre-rebase ids must not linger in the decoder");
    }

    /// Every repair names the published taps, and the density still
    /// occupies the low nibble beside the generator id.
    #[test]
    fn a_repair_names_its_own_generator() {
        let len = 64;
        let mut enc = RlcEncoder::new(8, 4, 15, len);
        let mut emitted = None;
        for sid in 0..4u32 {
            if let (_, Some(r)) = enc.push_source(&make_symbol(sid, len)) {
                emitted = Some(r);
            }
        }
        let r = emitted.expect("a repair every step=4");
        assert_eq!(r.generator(), GENERATOR_PUBLISHED_TAPS);
        assert_eq!(r.density(), 15);
        assert!(r.generator_is_readable());
    }

    /// A repair from the generator 0.2.x used is refused rather than
    /// decoded with the taps. Its coefficients cannot be reproduced here,
    /// and an equation whose coefficients are wrong does not fail to
    /// solve - it solves to bytes that were never sent.
    #[test]
    fn a_repair_from_the_retired_generator_is_refused() {
        let len = 64;
        let mut enc = RlcEncoder::new(8, 4, 15, len);
        let mut dec = RlcDecoder::new(len).with_horizon(64);

        let mut repair = None;
        for sid in 0..4u32 {
            let (id, rep) = enc.push_source(&make_symbol(sid, len));
            if sid != 1 {
                dec.on_source(id, &make_symbol(sid, len));
            }
            if let Some(r) = rep {
                repair = Some(r);
            }
        }
        // Restamp it as the retired generator, exactly as a 0.2.x peer
        // would have sent it.
        let mut legacy = repair.expect("a repair every step=4");
        legacy.dt = (GENERATOR_PER_REPAIR << 4) | legacy.density();

        let before = refused_repairs();
        let recovered = dec.on_repair(legacy);
        assert!(recovered.is_empty(), "a refused repair recovers nothing");
        assert_eq!(dec.refused_repairs(), 1);
        assert_eq!(dec.get(1), None, "the lost symbol stays lost rather than wrong");
        // The process-wide tally is what an operator reads, so it has to
        // move too: a link that recovers nothing because the peer speaks
        // the retired generator otherwise looks exactly like a bad
        // channel.
        assert_eq!(refused_repairs(), before + 1);
    }

    /// The published taps recover an isolated loss, which is the property
    /// that decides whether a fixed generator is usable at all: a
    /// generator that does not recover is no cheaper for being published.
    #[test]
    fn the_published_taps_recover_an_isolated_loss() {
        let len = 64;
        let mut enc = RlcEncoder::new(8, 4, 15, len);
        let mut dec = RlcDecoder::new(len).with_horizon(64);

        let mut repair = None;
        for sid in 0..4u32 {
            let (id, rep) = enc.push_source(&make_symbol(sid, len));
            assert_eq!(id, sid);
            // Drop symbol 1 on the way to the decoder.
            if sid != 1 {
                dec.on_source(sid, &make_symbol(sid, len));
            }
            if let Some(r) = rep {
                repair = Some(r);
            }
        }
        let recovered = dec.on_repair(repair.expect("a repair fires every step=4"));
        assert!(recovered.contains(&1), "published taps recover: {recovered:?}");
        assert_eq!(dec.get(1), Some(make_symbol(1, len).as_slice()));
    }

    /// Across a run of isolated losses the taps recover essentially all
    /// of them. One repair covers every symbol in its window, so a single
    /// loss between two repairs is always determined; anything materially
    /// short of that would mean the taps are not carrying the code.
    #[test]
    fn the_published_taps_hold_up_across_a_loss_trace() {
        let len = 64;
        const SYMBOLS: u32 = 400;
        // Every seventh symbol is lost: isolated single losses, which is
        // what a sliding-window code exists to catch.
        let lost = |sid: u32| sid % 7 == 3;

        let mut enc = RlcEncoder::new(8, 4, 15, len);
        let mut dec = RlcDecoder::new(len).with_horizon(256);
        let mut healed = 0usize;
        let mut dropped = 0usize;
        for sid in 0..SYMBOLS {
            let (id, rep) = enc.push_source(&make_symbol(sid, len));
            if lost(id) {
                dropped += 1;
            } else {
                dec.on_source(id, &make_symbol(sid, len));
            }
            if let Some(r) = rep {
                healed += dec.on_repair(r).len();
            }
        }

        assert!(dropped > 0, "the trace drops something to recover");
        assert!(
            healed * 10 >= dropped * 9,
            "the taps recovered {healed} of {dropped} isolated losses; a sliding-window \
             code that misses one-in-seven singles is not carrying the code",
        );
        // Every recovery is the bytes that were sent, not merely a
        // symbol appearing in the map.
        for sid in (0..SYMBOLS).filter(|s| lost(*s)) {
            if let Some(bytes) = dec.get(sid) {
                assert_eq!(bytes, make_symbol(sid, len).as_slice());
            }
        }
    }

    /// The taps are a table both ends read, so the properties worth
    /// pinning are the ones a second implementation must reproduce: the
    /// values are distinct and nonzero, there is one for every position
    /// the widest window can hold, and the density spreads across the
    /// window instead of capping how far back it reaches.
    #[test]
    fn the_taps_are_distinct_nonzero_and_cover_the_widest_window() {
        let mut seen = std::collections::BTreeSet::new();
        for (i, &t) in TAPS.iter().enumerate() {
            assert_ne!(t, 0, "tap {i} is zero, so that place in the window is skipped");
            assert!(seen.insert(t), "tap {i} repeats a value already in the table");
        }
        assert!(
            TAPS.len() >= crate::rlc_control::WINDOW_MAX as usize,
            "the table is {} long against a controller that widens the window to {}; the \
             positions past the table take a zero and are not protected at all",
            TAPS.len(),
            crate::rlc_control::WINDOW_MAX,
        );

        // Density, not reach: dt of 15 takes every position, dt of 0
        // takes one in sixteen - and both at every depth, not only near
        // the newest symbol.
        for dt in 0..=15u8 {
            for (tap, &want) in TAPS.iter().enumerate() {
                let c = fixed_coef(tap, dt);
                if tap % DENSITY_CLASSES <= dt as usize {
                    assert_eq!(c, want, "tap {tap} is in density class {dt}");
                } else {
                    assert_eq!(c, 0, "tap {tap} is outside density class {dt}");
                }
            }
        }
        for tap in 0..TAPS.len() {
            assert_ne!(fixed_coef(tap, 15), 0, "full density leaves position {tap} out");
        }
    }

    /// A window wider than one density class still protects its oldest
    /// symbols. This is what a reach-shaped density silently broke: the
    /// controller widens the window towards `WINDOW_MAX` to span a long
    /// burst, and a repair that reaches only sixteen back makes that
    /// widening do nothing.
    #[test]
    fn a_wide_window_protects_its_oldest_symbols() {
        let len = 64;
        let window = 48;
        let mut enc = RlcEncoder::new(window, window, 15, len);
        let mut dec = RlcDecoder::new(len).with_horizon(256);

        // Fill the window, dropping the oldest symbol on the way - the
        // one a sixteen-deep reach would never cover.
        let lost = 0u32;
        let mut repair = None;
        for sid in 0..window as u32 {
            let (id, rep) = enc.push_source(&make_symbol(sid, len));
            if id != lost {
                dec.on_source(id, &make_symbol(sid, len));
            }
            if let Some(r) = rep {
                repair = Some(r);
            }
        }
        let r = repair.expect("a repair every step=window");
        assert_eq!(r.window_size as usize, window, "the repair spans the whole window");
        assert_ne!(
            r.coefficient(0),
            0,
            "the oldest symbol in a {window}-wide window carries no coefficient, so no \
             repair can recover it",
        );

        let recovered = dec.on_repair(r);
        assert!(recovered.contains(&lost), "the oldest symbol recovers: {recovered:?}");
        assert_eq!(dec.get(lost), Some(make_symbol(lost, len).as_slice()));
    }

    /// An isolated loss is recovered from the very next repair that covers it,
    /// without waiting for a block to complete.
    #[test]
    fn isolated_loss_recovers_immediately() {
        let len = 64;
        let mut enc = RlcEncoder::new(8, 2, DEFAULT_DT, len);
        let mut dec = RlcDecoder::new(len);
        let drop_sid = 5u32;
        let n = 12u32;
        let mut recovered_at: Option<u32> = None;
        let mut emitted = 0u32; // count of wire symbols fed after the drop
        for i in 0..n {
            let sym = make_symbol(i, len);
            let (sid, repair) = enc.push_source(&sym);
            if sid != drop_sid {
                dec.on_source(sid, &sym);
            }
            if sid > drop_sid {
                emitted += 1;
            }
            if let Some(r) = repair {
                let rec = dec.on_repair(r);
                if rec.contains(&drop_sid) && recovered_at.is_none() {
                    recovered_at = Some(emitted);
                }
            }
        }
        assert!(dec.has(drop_sid), "isolated loss must recover");
        assert_eq!(
            dec.get(drop_sid),
            Some(make_symbol(drop_sid, len).as_slice()),
            "recovered bytes must match the original"
        );
        // Recovered within a couple of symbols of the loss - not after a whole
        // block (a block of this rate would need ~the full window first).
        assert!(
            recovered_at.is_some_and(|e| e <= 2),
            "must recover within ~2 wire symbols of the loss, got {recovered_at:?}"
        );
    }

    /// A burst of consecutive losses recovers once enough repairs span them.
    #[test]
    fn burst_within_capability_recovers() {
        let len = 48;
        let mut enc = RlcEncoder::new(16, 2, DEFAULT_DT, len);
        let mut dec = RlcDecoder::new(len);
        let drops: BTreeSet<u32> = [4, 5].into_iter().collect();
        let originals: Vec<Vec<u8>> = (0..16).map(|i| make_symbol(i, len)).collect();
        for i in 0..16u32 {
            let (sid, repair) = enc.push_source(&originals[i as usize]);
            if !drops.contains(&sid) {
                dec.on_source(sid, &originals[i as usize]);
            }
            if let Some(r) = repair {
                dec.on_repair(r);
            }
        }
        for &sid in &drops {
            assert!(dec.has(sid), "burst symbol {sid} must recover");
            assert_eq!(dec.get(sid), Some(originals[sid as usize].as_slice()));
        }
    }

    /// A loss whose repairs are also lost is reported missing, never recovered
    /// as wrong data.
    #[test]
    fn unrecoverable_loss_is_not_misrecovered() {
        let len = 32;
        let mut enc = RlcEncoder::new(8, 2, DEFAULT_DT, len);
        let mut dec = RlcDecoder::new(len);
        let drop_sid = 5u32;
        for i in 0..12u32 {
            let sym = make_symbol(i, len);
            let (sid, repair) = enc.push_source(&sym);
            if sid != drop_sid {
                dec.on_source(sid, &sym);
            }
            // Drop every repair whose window still contains the lost symbol, so
            // it can never be recovered.
            if let Some(r) = repair {
                let covers = (0..r.window_size as u32)
                    .any(|off| r.first_source_id.wrapping_add(off) == drop_sid);
                if !covers {
                    dec.on_repair(r);
                }
            }
        }
        assert!(!dec.has(drop_sid), "no repair covered it -> must stay missing");
        assert_eq!(dec.get(drop_sid), None, "must not fabricate wrong data");
    }

    /// A long stream under scattered isolated losses delivers every symbol
    /// exactly (received or recovered).
    #[test]
    fn long_stream_scattered_losses_all_recover() {
        let len = 40;
        let n = 300u32;
        let mut enc = RlcEncoder::new(16, 2, DEFAULT_DT, len);
        let mut dec = RlcDecoder::new(len);
        let originals: Vec<Vec<u8>> = (0..n).map(|i| make_symbol(i, len)).collect();
        // Deterministic ~8% isolated drops (never two in a row, so each is
        // within the repair capability at this rate).
        let mut rng = 0x1234_5678u32;
        let mut prev_dropped = false;
        for i in 0..n {
            rng = rng.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let drop = !prev_dropped && (rng >> 24) % 100 < 8;
            prev_dropped = drop;
            let (sid, repair) = enc.push_source(&originals[i as usize]);
            if !drop {
                dec.on_source(sid, &originals[i as usize]);
            }
            if let Some(r) = repair {
                dec.on_repair(r);
            }
        }
        for i in 0..n {
            assert!(dec.has(i), "symbol {i} must be delivered");
            assert_eq!(dec.get(i), Some(originals[i as usize].as_slice()));
        }
    }
}
