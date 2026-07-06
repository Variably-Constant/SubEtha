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
//! Each coefficient is drawn from a deterministic, seedable generator and is
//! nonzero with probability `(DT + 1) / 16` (the density threshold `DT`), so
//! the decoder reconstructs the exact coefficients from the repair's metadata
//! (the repair key, the first source id, the window size, and `DT`).
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

/// Deterministic GF(2^8) coefficient for `source_id` under repair `repair_key`
/// at density threshold `dt`. Nonzero with probability `(dt + 1) / 16`; the two
/// independent draws (a density nibble and a value byte) come from a splitmix
/// hash of `(repair_key, source_id)`, so encoder and decoder agree exactly.
fn coef(repair_key: u32, source_id: u32, dt: u8) -> u8 {
    // splitmix64 finaliser over the (key, id) pair.
    let mut x = ((repair_key as u64) << 32) | source_id as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    // Low nibble decides presence (uniform 0..15); next byte is the value.
    if (x as u8 & 0x0f) <= dt {
        let v = (x >> 8) as u8;
        if v == 0 { 1 } else { v }
    } else {
        0
    }
}

/// A repair symbol: a random linear combination of the source symbols in the
/// sliding window at the moment it was generated. The metadata lets the decoder
/// reconstruct the exact coefficients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairSymbol {
    /// Seed for the coefficient generator.
    pub repair_key: u32,
    /// Lowest source id in the covered window.
    pub first_source_id: u32,
    /// Number of source symbols in the covered window.
    pub window_size: u16,
    /// Density threshold (0..=15).
    pub dt: u8,
    /// `sum_i coef_i * source_i` over the window.
    pub payload: Vec<u8>,
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
        for (sid, sym) in &self.window {
            let c = coef(repair_key, *sid, self.dt);
            if c != 0 {
                gf_mul_add_auto(&mut payload, sym, c);
            }
        }
        RepairSymbol {
            repair_key,
            first_source_id: first,
            window_size: self.window.len() as u16,
            dt: self.dt,
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
    /// directly; recovery of OTHER symbols is driven by repair arrivals
    /// ([`on_repair`](Self::on_repair)), so this does NOT run the (potentially
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

    /// Store a repair WITHOUT solving, for a caller that drains a batch of
    /// datagrams first (stamping their arrival before any decode) and then runs
    /// one [`recover`](Self::recover) over the whole batch - keeping the
    /// expensive Gaussian solve out of the receive/timing path.
    pub fn add_repair(&mut self, r: RepairSymbol) {
        self.highest = self
            .highest
            .max(r.first_source_id.wrapping_add(r.window_size as u32).saturating_sub(1));
        self.repairs.push(r);
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
                let c = coef(r.repair_key, sid, r.dt);
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

/// `v[i] = gf::mul(v[i], coef)` in place - a scalar GF(2^8) scale of one
/// symbol-vector (used once per pivot to normalize the pivot row's rhs).
fn gf_scale(v: &mut [u8], coef: u8) {
    if coef == 1 {
        return;
    }
    for b in v.iter_mut() {
        *b = gf::mul(*b, coef);
    }
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

    #[test]
    fn coefficient_generator_is_deterministic_and_honors_density() {
        // Same inputs -> same coefficient.
        for key in 0..8u32 {
            for sid in 0..8u32 {
                assert_eq!(coef(key, sid, 15), coef(key, sid, 15));
            }
        }
        // DT = 15 -> every coefficient nonzero.
        for sid in 0..256u32 {
            assert_ne!(coef(7, sid, 15), 0, "DT=15 must be fully dense at sid {sid}");
        }
        // DT = 0 -> roughly 1/16 nonzero (sample a population).
        let nz = (0..4096u32).filter(|&s| coef(3, s, 0) != 0).count();
        assert!(
            (150..420).contains(&nz),
            "DT=0 density ~1/16 of 4096 (~256); got {nz}"
        );
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
