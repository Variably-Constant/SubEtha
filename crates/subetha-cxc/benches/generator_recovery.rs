//! What the sliding-window code recovers, across loss regimes.
//!
//! The recovery profile of the code as it ships: a change that quietly
//! costs recovery shows up here as a row that moved.
//!
//! Timing would be the wrong instrument: the coefficients ride the same
//! GF(2^8) ladder whatever they are, so a throughput bench measures the
//! ladder and not the code.
//!
//! Recovered payloads are compared byte-for-byte with the originals, so
//! a "recovery" that returns the wrong bytes counts as a failure rather
//! than a success.
//!
//! Run: cargo bench -p subetha-cxc --bench generator_recovery

use subetha_cxc::rlc_fec::{RepairSymbol, RlcDecoder, RlcEncoder};

/// Source symbols per trial.
const SYMBOLS: u32 = 3000;
/// Payload bytes per symbol.
const SYMBOL_LEN: usize = 256;
/// Source symbols the encoder keeps, and so the furthest back a repair reaches.
const WINDOW: usize = 16;
/// One repair every this many source symbols; the code rate is step/(step+1).
const STEP: usize = 8;
/// Density threshold: every position in the window carries a tap, which
/// is the density the code ships at.
const DT: u8 = 15;
/// Independent trials per cell, each with its own loss draw.
const TRIALS: u32 = 24;

/// xorshift64*, so a trial's loss pattern is reproducible from its seed
/// and identical across the two arms.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    /// Uniform in 0.0..1.0.
    fn next_unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// One item as it goes on the wire, in send order.
enum Wire {
    Source(u32, Vec<u8>),
    Repair(RepairSymbol),
}

/// How a trial decides what to drop.
#[derive(Clone, Copy)]
enum Regime {
    /// Each wire item dropped independently with this probability.
    Random(f64),
    /// Bursts of `len` consecutive items, entered with probability `p`.
    Burst { p: f64, len: usize },
}

impl Regime {
    fn label(self) -> String {
        match self {
            Regime::Random(p) => format!("random {:>4.1}%", p * 100.0),
            Regime::Burst { p, len } => format!("burst {len} @ {:>4.1}%", p * 100.0),
        }
    }

    /// The dropped positions for one trial. Computed from the seed alone,
    /// so both arms lose exactly the same wire positions.
    fn drops(self, count: usize, seed: u64) -> Vec<bool> {
        let mut rng = Rng::new(seed);
        let mut out = vec![false; count];
        match self {
            Regime::Random(p) => {
                for slot in out.iter_mut() {
                    *slot = rng.next_unit() < p;
                }
            }
            Regime::Burst { p, len } => {
                let mut i = 0;
                while i < count {
                    if rng.next_unit() < p {
                        for slot in out.iter_mut().skip(i).take(len) {
                            *slot = true;
                        }
                        i += len;
                    } else {
                        i += 1;
                    }
                }
            }
        }
        out
    }
}

/// Build the wire stream for one trial. The payload of source `sid` is
/// derived from `sid`, so a recovered symbol can be checked against what
/// was sent without keeping the originals alive separately.
fn payload_for(sid: u32) -> Vec<u8> {
    let mut rng = Rng::new(0x9E37_79B9_7F4A_7C15 ^ u64::from(sid));
    (0..SYMBOL_LEN).map(|_| rng.next_u64() as u8).collect()
}

fn wire_stream() -> Vec<Wire> {
    let mut enc = RlcEncoder::new(WINDOW, STEP, DT, SYMBOL_LEN);
    let mut wire = Vec::new();
    for sid in 0..SYMBOLS {
        let payload = payload_for(sid);
        let (id, repair) = enc.push_source(&payload);
        wire.push(Wire::Source(id, payload));
        if let Some(r) = repair {
            wire.push(Wire::Repair(r));
        }
    }
    wire
}

/// Lost source symbols, and how many of them came back correct.
struct Outcome {
    lost: u64,
    recovered: u64,
    /// Recoveries whose bytes did not match what was sent. Any nonzero
    /// value here invalidates the corresponding recovery figure.
    corrupt: u64,
}

fn run_trial(regime: Regime, seed: u64) -> Outcome {
    let wire = wire_stream();
    let drops = regime.drops(wire.len(), seed);

    let mut dec = RlcDecoder::new(SYMBOL_LEN).with_horizon(1024);
    let mut lost_ids = Vec::new();
    for (item, dropped) in wire.iter().zip(drops.iter()) {
        match item {
            Wire::Source(sid, payload) => {
                if *dropped {
                    lost_ids.push(*sid);
                } else {
                    dec.on_source(*sid, payload);
                }
            }
            Wire::Repair(r) => {
                if !*dropped {
                    dec.on_repair(r.clone());
                }
            }
        }
    }
    // One final pass, so a symbol determined by the last repair in the
    // stream is not counted as lost purely for arriving last.
    dec.recover();

    let mut recovered = 0;
    let mut corrupt = 0;
    for sid in &lost_ids {
        match dec.get(*sid) {
            Some(bytes) if bytes == payload_for(*sid).as_slice() => recovered += 1,
            Some(_) => corrupt += 1,
            None => {}
        }
    }
    Outcome { lost: lost_ids.len() as u64, recovered, corrupt }
}

fn main() {
    let regimes = [
        Regime::Random(0.02),
        Regime::Random(0.05),
        Regime::Random(0.10),
        Regime::Random(0.15),
        Regime::Random(0.20),
        Regime::Burst { p: 0.01, len: 2 },
        Regime::Burst { p: 0.01, len: 3 },
        Regime::Burst { p: 0.01, len: 5 },
        Regime::Burst { p: 0.02, len: 3 },
    ];

    println!(
        "sliding-window RLC recovery, window {WINDOW}, step {STEP}, dt {DT}, \
         {SYMBOLS} symbols x {TRIALS} trials\n"
    );
    println!("{:<18} {:>10} {:>12}", "regime", "lost", "recovered");

    for regime in regimes {
        let mut lost_total = 0u64;
        let mut recovered = 0u64;
        let mut corrupt = 0u64;
        for trial in 0..TRIALS {
            let seed = 0xD1B5_4A32_D192_ED03 ^ (u64::from(trial) << 17);
            let o = run_trial(regime, seed);
            lost_total += o.lost;
            recovered += o.recovered;
            corrupt += o.corrupt;
        }
        let pct = if lost_total == 0 {
            0.0
        } else {
            recovered as f64 * 100.0 / lost_total as f64
        };
        println!("{:<18} {:>10} {:>11.2}%", regime.label(), lost_total, pct);
        assert_eq!(corrupt, 0, "a recovery returned bytes that were never sent");
    }
}
