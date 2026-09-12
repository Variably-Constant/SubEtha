//! Search the published-tap table for the distance properties a
//! convolutional generator is normally chosen for, and prove the search
//! can tell a good table from a bad one before believing its answer.
//!
//! The shipped table's sixty-four values were picked to be small and
//! distinct, not searched. This scores a candidate table the way an
//! erasure code is scored: over a span of source symbols carrying the
//! repairs the encoder would emit, take every loss pattern of weight
//! `t`, and ask whether the surviving repairs determine all `t` lost
//! symbols. With `t` unknowns and the repair rows that cover them, they
//! are all determined exactly when that matrix has rank `t` over
//! GF(2^8), so the score is the fraction of weight-`t` patterns whose
//! matrix is full rank.
//!
//! The enumeration is exhaustive rather than sampled at every weight it
//! reports, so two tables are compared on the same complete set of
//! patterns and a difference between them is a property of the tables,
//! not of a draw.
//!
//! # Why the controls come first
//!
//! A scoring function that returns the same number for every table
//! would rank the shipped table first as readily as any other, and the
//! search would look like it had worked. So the run begins with two
//! controls whose answers are known: the all-ones table, where every
//! repair is the same unweighted sum and any two losses inside one
//! window are indistinguishable, must score far worse than the shipped
//! table at t >= 2; and a table of distinct values must not.
//! If the controls do not separate, the search is not measuring
//! anything and its ranking is not reported.
//!
//! Run: cargo bench -p subetha-cxc --bench tap_search

use subetha_cxc::fec::gf;

/// One coding geometry: the window the encoder keeps, the repair
/// cadence, the span of source ids modeled, and the id at or above
/// which losses are drawn (so a pattern is covered by repairs holding a
/// full window rather than by the short windows at the start of a
/// stream).
#[derive(Clone, Copy)]
struct Geometry {
    window: usize,
    step: usize,
    span: u32,
    floor: u32,
}

/// The geometries the conclusion is checked at. The shipped parameters
/// are the first; the rest vary the window and the cadence, because a
/// claim about the tap values that only held at one shape would be a
/// claim about that shape.
const GEOMETRIES: [Geometry; 4] = [
    Geometry { window: 16, step: 8, span: 40, floor: 8 },
    Geometry { window: 16, step: 4, span: 40, floor: 8 },
    Geometry { window: 8, step: 4, span: 32, floor: 8 },
    Geometry { window: 32, step: 8, span: 64, floor: 16 },
];

/// One repair as the encoder would emit it: the window it covers.
#[derive(Clone, Copy)]
struct Repair {
    first: u32,
    size: u32,
}

/// The repairs the encoder emits across the span. `emit_repair` takes the
/// window after the source is pushed, so the repair following source `sid`
/// covers the newest `min(WINDOW, sid + 1)` ids ending at `sid`.
fn repairs(g: Geometry) -> Vec<Repair> {
    let mut out = Vec::new();
    for sid in 0..g.span {
        if (sid + 1) % g.step as u32 == 0 {
            let size = ((sid + 1) as usize).min(g.window) as u32;
            out.push(Repair { first: sid + 1 - size, size });
        }
    }
    out
}

/// The coefficient a tap table gives the source `off` places after the
/// first in a window of `size`: the newest symbol takes `taps[0]`, the
/// one before it `taps[1]`, matching `RepairSymbol::coefficient`.
fn coef(taps: &[u8; TAPS_LEN], size: u32, off: u32) -> u8 {
    let idx = (size - 1 - off) as usize;
    if idx < taps.len() { taps[idx] } else { 0 }
}

/// Rank over GF(2^8) of the `rows x cols` matrix, by Gaussian elimination.
fn rank(mut m: Vec<Vec<u8>>, cols: usize) -> usize {
    let rows = m.len();
    let mut rank = 0;
    for col in 0..cols {
        let pivot = (rank..rows).find(|&r| m[r][col] != 0);
        let Some(p) = pivot else { continue };
        m.swap(rank, p);
        let inv = gf::inv(m[rank][col]);
        for v in m[rank].iter_mut().skip(col) {
            *v = gf::mul(*v, inv);
        }
        // The pivot row is read while every other row is written, so it
        // is taken out of the borrow rather than indexed alongside them.
        let pivot = m[rank].clone();
        for (r, row) in m.iter_mut().enumerate() {
            if r == rank || row[col] == 0 {
                continue;
            }
            let factor = row[col];
            for (c, v) in row.iter_mut().enumerate().skip(col) {
                *v ^= gf::mul(factor, pivot[c]);
            }
        }
        rank += 1;
        if rank == rows {
            break;
        }
    }
    rank
}

/// Whether every id in `lost` is determined by the repairs, which for
/// `t` unknowns is exactly the covering matrix having rank `t`.
fn all_recovered(taps: &[u8; TAPS_LEN], reps: &[Repair], lost: &[u32]) -> bool {
    let mut m = Vec::new();
    for r in reps {
        let mut row = vec![0u8; lost.len()];
        let mut any = false;
        for (j, &id) in lost.iter().enumerate() {
            if id >= r.first && id < r.first + r.size {
                let c = coef(taps, r.size, id - r.first);
                row[j] = c;
                any |= c != 0;
            }
        }
        if any {
            m.push(row);
        }
    }
    if m.len() < lost.len() {
        return false;
    }
    rank(m, lost.len()) == lost.len()
}

/// Fraction of weight-`t` loss patterns this table recovers in full,
/// over every such pattern in the span.
fn score_at(taps: &[u8; TAPS_LEN], reps: &[Repair], t: usize, g: Geometry) -> f64 {
    let ids: Vec<u32> = (g.floor..g.span).collect();
    let mut total = 0u64;
    let mut ok = 0u64;
    let mut idx: Vec<usize> = (0..t).collect();
    loop {
        let lost: Vec<u32> = idx.iter().map(|&i| ids[i]).collect();
        total += 1;
        if all_recovered(taps, reps, &lost) {
            ok += 1;
        }
        // Next combination in lexicographic order.
        let mut i = t;
        loop {
            if i == 0 {
                return ok as f64 / total as f64;
            }
            i -= 1;
            if idx[i] != i + ids.len() - t {
                break;
            }
        }
        idx[i] += 1;
        for j in i + 1..t {
            idx[j] = idx[j - 1] + 1;
        }
    }
}

/// The largest set of unknowns that can be matched one-to-one to
/// distinct repairs covering them. Over a field this size a table of
/// unrelated values realizes that matching as rank with overwhelming
/// probability, so the matching size is the rank no table can beat: it
/// is a property of which repairs cover which symbols, and no choice of
/// coefficients adds an equation that does not exist.
fn max_matching(reps: &[Repair], lost: &[u32]) -> usize {
    let mut paired: Vec<Option<usize>> = vec![None; reps.len()];
    let mut size = 0;
    for j in 0..lost.len() {
        let mut seen = vec![false; reps.len()];
        if augment(j, lost, reps, &mut paired, &mut seen) {
            size += 1;
        }
    }
    size
}

/// One augmenting-path step of the bipartite matching.
fn augment(
    j: usize,
    lost: &[u32],
    reps: &[Repair],
    paired: &mut [Option<usize>],
    seen: &mut [bool],
) -> bool {
    for (ri, r) in reps.iter().enumerate() {
        if seen[ri] || !covers(r, lost[j]) {
            continue;
        }
        seen[ri] = true;
        let ok = match paired[ri] {
            None => true,
            Some(other) => augment(other, lost, reps, paired, seen),
        };
        if ok {
            paired[ri] = Some(j);
            return true;
        }
    }
    false
}

/// Whether a repair can give this source id a nonzero coefficient.
///
/// Being inside the window is not enough. The table holds sixteen taps,
/// so in a window wider than sixteen the oldest symbols are multiplied
/// by nothing and take no part in that repair's equation. A ceiling that
/// counted them would sit above what any table can reach and would read
/// as a table worth improving.
fn covers(r: &Repair, id: u32) -> bool {
    if id < r.first || id >= r.first + r.size {
        return false;
    }
    let idx = (r.size - 1 - (id - r.first)) as usize;
    idx < TAPS_LEN
}

/// Taps in the table, and so the furthest back any repair reaches.
const TAPS_LEN: usize = 64;

/// The fraction of weight-`t` patterns that any table at all could
/// recover: the ceiling the search is working under.
fn bound_at(reps: &[Repair], t: usize, g: Geometry) -> f64 {
    let ids: Vec<u32> = (g.floor..g.span).collect();
    let mut total = 0u64;
    let mut ok = 0u64;
    let mut idx: Vec<usize> = (0..t).collect();
    loop {
        let lost: Vec<u32> = idx.iter().map(|&i| ids[i]).collect();
        total += 1;
        if max_matching(reps, &lost) == t {
            ok += 1;
        }
        let mut i = t;
        loop {
            if i == 0 {
                return ok as f64 / total as f64;
            }
            i -= 1;
            if idx[i] != i + ids.len() - t {
                break;
            }
        }
        idx[i] += 1;
        for j in i + 1..t {
            idx[j] = idx[j - 1] + 1;
        }
    }
}

/// The weights a table is screened on, and then ranked on.
const SCREEN_WEIGHTS: [usize; 3] = [2, 3, 4];

/// A single figure for ranking: the mean over the screened weights. The
/// higher weights are where tables separate, and weight 1 is excluded
/// because any table with a nonzero newest tap recovers every isolated
/// loss and it therefore ranks nothing.
fn score(taps: &[u8; TAPS_LEN], reps: &[Repair], g: Geometry) -> f64 {
    SCREEN_WEIGHTS.iter().map(|&t| score_at(taps, reps, t, g)).sum::<f64>()
        / SCREEN_WEIGHTS.len() as f64
}

/// xorshift64*, so a run's candidate tables are reproducible.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

/// A candidate table of sixteen distinct nonzero field elements.
fn candidate(rng: &mut Rng) -> [u8; TAPS_LEN] {
    let mut taps = [0u8; TAPS_LEN];
    let mut used = [false; 256];
    for slot in taps.iter_mut() {
        loop {
            let v = (rng.next_u64() & 0xff) as u8;
            if v != 0 && !used[v as usize] {
                used[v as usize] = true;
                *slot = v;
                break;
            }
        }
    }
    taps
}

/// The table rlc_fec ships today: one tap per position the widest
/// window can hold.
const SHIPPED: [u8; TAPS_LEN] = [
    0x01, 0x02, 0x03, 0x05, 0x07, 0x0b, 0x0d, 0x11,
    0x13, 0x17, 0x1d, 0x1f, 0x25, 0x29, 0x2b, 0x2f,
    0x35, 0x3b, 0x3d, 0x43, 0x47, 0x49, 0x4f, 0x53,
    0x59, 0x61, 0x65, 0x67, 0x6b, 0x6d, 0x71, 0x7f,
    0x83, 0x89, 0x8b, 0x95, 0x97, 0x9d, 0xa3, 0xa7,
    0xad, 0xb3, 0xb5, 0xbf, 0xc1, 0xc5, 0xc7, 0xd3,
    0xdf, 0xe3, 0xe5, 0xe9, 0xef, 0xf1, 0xf5, 0xf7,
    0xfb, 0xfd, 0x04, 0x08, 0x0e, 0x16, 0x1a, 0x22,
];

/// Every repair the same unweighted sum: two losses in one window are
/// indistinguishable, so this must score far below anything usable.
const ALL_ONES: [u8; TAPS_LEN] = [1; TAPS_LEN];

/// The shipped table with its last value repeating its first. If
/// distinctness is the only property of the values that matters, this
/// scores no higher than the shipped table.
const ONE_COLLISION: [u8; TAPS_LEN] = {
    let mut t = SHIPPED;
    t[TAPS_LEN - 1] = t[0];
    t
};

fn show(label: &str, taps: &[u8; TAPS_LEN], reps: &[Repair], g: Geometry) -> f64 {
    let per: Vec<String> = SCREEN_WEIGHTS
        .iter()
        .map(|&t| format!("t{t} {:>6.2}%", score_at(taps, reps, t, g) * 100.0))
        .collect();
    let s = score(taps, reps, g);
    println!("{label:<22} {}   mean {:>6.2}%", per.join("  "), s * 100.0);
    s
}

/// Run the controls, the ceiling and the search at one geometry.
/// Returns whether the shipped table reached the ceiling there.
fn examine(g: Geometry) -> bool {
    let reps = repairs(g);
    println!(
        "== window {} step {} over {} symbols, {} repairs ==",
        g.window,
        g.step,
        g.span,
        reps.len()
    );

    // The ceiling first, so every score below is read against what is
    // reachable rather than against 100%.
    let bound: Vec<f64> = SCREEN_WEIGHTS.iter().map(|&t| bound_at(&reps, t, g)).collect();
    let bound_mean = bound.iter().sum::<f64>() / bound.len() as f64;
    let bound_cells: Vec<String> = SCREEN_WEIGHTS
        .iter()
        .zip(&bound)
        .map(|(&t, b)| format!("t{t} {:>6.2}%", b * 100.0))
        .collect();
    println!(
        "{:<22} {}   mean {:>6.2}%",
        "ceiling (any table)",
        bound_cells.join("  "),
        bound_mean * 100.0
    );
    println!(
        "the ceiling is set by which repairs cover which symbols; a pattern above it has \
         no equation to solve, whatever the coefficients\n"
    );

    println!("controls");
    let ones = show("all-ones (degenerate)", &ALL_ONES, &reps, g);
    let collision = show("one repeated value", &ONE_COLLISION, &reps, g);
    let shipped = show("shipped table", &SHIPPED, &reps, g);
    assert!(
        collision <= shipped,
        "a table repeating a value scored {collision:.4} against the shipped table's \
         {shipped:.4}; repeating a value cannot add an independent equation, so the \
         score is not measuring rank"
    );

    if shipped <= ones + 0.05 {
        println!(
            "\nCONTROLS DID NOT SEPARATE: the degenerate table scores {:.2}% against the \
             shipped table's {:.2}%. The scoring function is not measuring the thing it \
             claims to, so no ranking is reported.",
            ones * 100.0,
            shipped * 100.0
        );
        std::process::exit(1);
    }
    println!(
        "controls separate by {:.2} points, so the score distinguishes a good \
         generator from a bad one\n",
        (shipped - ones) * 100.0
    );

    let mut rng = Rng(0x243F_6A88_85A3_08D3);
    let mut best: Vec<([u8; TAPS_LEN], f64)> = Vec::new();
    let candidates = 4000;
    for _ in 0..candidates {
        let taps = candidate(&mut rng);
        let s = score(&taps, &reps, g);
        best.push((taps, s));
    }
    best.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    best.truncate(5);

    println!("best of {candidates} random tables of distinct nonzero elements");
    for (i, (taps, _)) in best.iter().enumerate() {
        let label = format!("candidate {}", i + 1);
        show(&label, taps, &reps, g);
        let hex: Vec<String> = taps.iter().map(|b| format!("0x{b:02x}")).collect();
        println!("{:22} [{}]", "", hex.join(", "));
    }

    let top = best[0].1;
    println!(
        "\nbest candidate {:.2}% against the shipped table's {:.2}%: {:+.2} points",
        top * 100.0,
        shipped * 100.0,
        (top - shipped) * 100.0
    );
    let at_ceiling = (shipped - bound_mean).abs() < 1e-9;
    if at_ceiling {
        println!(
            "the shipped table is at the ceiling here, so no table of any values scores \
             higher and there is nothing for a longer search to find\n"
        );
    } else {
        println!(
            "the shipped table sits {:.2} points under the ceiling, so a better table exists\n",
            (bound_mean - shipped) * 100.0
        );
    }
    at_ceiling
}

fn main() {
    println!("score is the fraction of weight-t loss patterns recovered in full");
    println!(
        "the first geometry is the shipped one; the rest are there so a conclusion about \
         the tap values is not a conclusion about one shape\n"
    );
    let mut all_at_ceiling = true;
    for g in GEOMETRIES {
        all_at_ceiling &= examine(g);
    }
    if all_at_ceiling {
        println!(
            "The shipped table reaches the ceiling at every geometry tried. The values \
             cannot be improved on: what limits recovery is which repairs cover which \
             symbols, and the only property the values need is that they are distinct and \
             nonzero. Freeze the table."
        );
    } else {
        println!(
            "The shipped table is under the ceiling at one or more geometries; the tap \
             values are worth searching after all."
        );
    }
}
