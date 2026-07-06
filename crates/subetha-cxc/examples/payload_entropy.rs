//! Payload entropy / structural-slack measurement for the bridge wire
//! format.
//!
//! For each real slot type and a content spectrum (structured to random)
//! it reports, per marshaled slot, how many bytes are constant /
//! temporally redundant / derivable / irreducible entropy. That split is
//! the number that sizes the schema-elision and embedded-parity layers:
//! their budget is the same `slot_size - H`.
//!
//! Method is dep-free and CONSERVATIVE: per-byte-position order-0 Shannon
//! entropy is a lower bound on the slack (a real compressor exploits
//! higher-order and cross-field structure this misses, and arithmetic
//! prediction of counter fields shows up as entropy here), so the free
//! fraction reported is a FLOOR - the true slack is at least this.
//!
//! Layouts mirror the real marshal paths:
//! - `PassSlot` (56 B): `closure_id: u32 @0`, `token: u32 @4`,
//!   `arg_len: u16 @8`, args, zero pad (scheduler.rs).
//! - `FatLineItem` (64 B): real `Marshal` impl, 12 zero bytes by layout.
//! - hash-map insert op: `opcode + key + value + FNV1a(key) + pad`, the
//!   FNV hash derivable at the receiver.

use subetha_core::Marshal;
use subetha_cxc::shared_deque_khpd::{FatLineItem, LineItem};

const N: usize = 40_000;

/// xorshift64 - dep-free, reproducible.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn byte(&mut self) -> u8 {
        (self.next() >> 24) as u8
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Order-0 Shannon entropy (bits) of a 256-bin histogram over `n` samples.
fn entropy_bits(hist: &[u32; 256], n: usize) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let n = n as f64;
    let mut h = 0.0;
    for &c in hist.iter() {
        if c > 0 {
            let p = c as f64 / n;
            h -= p * p.log2();
        }
    }
    h
}

/// Returns (width, const_positions, static_entropy_bytes,
/// delta_entropy_bytes). `const` = byte positions identical across every
/// slot (schema-elidable with certainty); entropy figures are per-position
/// order-0 sums over 8 (a conservative compressed-size floor); static =
/// each slot independent, delta = XOR against the previous slot (temporal
/// redundancy).
fn analyze(stream: &[Vec<u8>]) -> (usize, usize, f64, f64) {
    let n = stream.len();
    let w = stream[0].len();
    let mut const_count = 0usize;
    let mut static_bits = 0.0;
    let mut delta_bits = 0.0;
    for p in 0..w {
        let mut hist = [0u32; 256];
        for s in stream {
            hist[s[p] as usize] += 1;
        }
        let hs = entropy_bits(&hist, n);
        if hs == 0.0 {
            const_count += 1;
        }
        static_bits += hs;

        let mut dhist = [0u32; 256];
        for i in 1..n {
            dhist[(stream[i][p] ^ stream[i - 1][p]) as usize] += 1;
        }
        delta_bits += entropy_bits(&dhist, n - 1);
    }
    (w, const_count, static_bits / 8.0, delta_bits / 8.0)
}

fn report(name: &str, stream: &[Vec<u8>], derivable: usize) {
    let (w, c, sh, dh) = analyze(stream);
    // Effective irreducible entropy = the better of the static or temporal
    // order-0 model, minus bytes the receiver can recompute. The rest of
    // the slot is free slack (constant + temporal + derivable).
    let eff_entropy = (sh.min(dh) - derivable as f64).max(0.0);
    let free_frac = ((w as f64 - eff_entropy) / w as f64) * 100.0;
    println!(
        "  {name:<26} W={w:<3} const={c:<3} derivable={derivable:<2} \
         static_H={sh:5.1}B delta_H={dh:5.1}B  eff_entropy~{eff_entropy:4.1}B  free>={free_frac:4.1}%"
    );
}

// ---- generators (real layouts) ----

fn passslot(n: usize, rng: &mut Rng, random: bool) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    let mut token = 0u32;
    for _ in 0..n {
        let mut s = vec![0u8; 56];
        if random {
            for b in s.iter_mut() {
                *b = rng.byte();
            }
        } else {
            let closure = rng.below(64) as u32;
            s[0..4].copy_from_slice(&closure.to_le_bytes());
            s[4..8].copy_from_slice(&token.to_le_bytes());
            token = token.wrapping_add(1);
            let arg_len = rng.below(25) as u16;
            s[8..10].copy_from_slice(&arg_len.to_le_bytes());
            for j in 0..arg_len as usize {
                s[10 + j] = rng.byte();
            }
        }
        out.push(s);
    }
    out
}

fn fatline(n: usize, rng: &mut Rng, random: bool) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    let mut id = 0u32;
    for _ in 0..n {
        let cnt = 1 + rng.below(3) as usize;
        let mut items = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            let mut b = [0u8; 16];
            if random {
                for x in b.iter_mut() {
                    *x = rng.byte();
                }
            } else {
                b[0] = rng.below(16) as u8; // opcode
                b[4..8].copy_from_slice(&id.to_le_bytes()); // sequential id
                id = id.wrapping_add(1);
                for x in b.iter_mut().skip(8) {
                    *x = rng.byte(); // 8-byte value payload
                }
            }
            items.push(LineItem::new(&b).unwrap());
        }
        let fat = FatLineItem::from_items(&items).unwrap();
        let mut s = vec![0u8; 64];
        fat.marshal(&mut s);
        out.push(s);
    }
    out
}

/// Hash-map insert op: opcode @0, key @1..9, value @9..17,
/// FNV1a(key) @17..25 (derivable), pad @25..32. The 8 hash bytes are
/// reported as derivable.
fn hashmap_op(n: usize, rng: &mut Rng, random_keys: bool) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    let mut key = 0u64;
    for _ in 0..n {
        let mut s = vec![0u8; 32];
        s[0] = 1; // insert opcode
        let k = if random_keys { rng.next() } else { key };
        key = key.wrapping_add(1);
        s[1..9].copy_from_slice(&k.to_le_bytes());
        s[9..17].copy_from_slice(&rng.next().to_le_bytes()); // value
        s[17..25].copy_from_slice(&fnv1a(&k.to_le_bytes()).to_le_bytes()); // derivable
        out.push(s);
    }
    out
}

fn verify_derivable_hash(stream: &[Vec<u8>]) -> bool {
    stream
        .iter()
        .all(|s| fnv1a(&s[1..9]).to_le_bytes() == s[17..25])
}

fn main() {
    println!("Bridge payload entropy / structural slack (N={N} slots/case)");
    println!("free>= is the CONSERVATIVE schema+temporal slack floor; true slack is at least this.\n");

    let mut rng = Rng::new(0x5eed_1234);

    println!("PassSlot (56B scheduler work descriptor):");
    report("typical", &passslot(N, &mut rng, false), 0);
    report("random (worst case)", &passslot(N, &mut rng, true), 0);

    println!("\nFatLineItem (64B deque batch, real marshal):");
    report("typical", &fatline(N, &mut rng, false), 0);
    report("random (worst case)", &fatline(N, &mut rng, true), 0);

    println!("\nHashMap insert op (32B, FNV hash derivable = 8B free):");
    let seq = hashmap_op(N, &mut rng, false);
    let rnd = hashmap_op(N, &mut rng, true);
    assert!(
        verify_derivable_hash(&seq) && verify_derivable_hash(&rnd),
        "hash must recompute"
    );
    report("sequential keys", &seq, 8);
    report("random keys (worst case)", &rnd, 8);

    println!("\n(const = bytes identical across ALL slots; derivable = recomputed at receiver;");
    println!(" static_H = order-0 size; delta_H = order-0 size of XOR-vs-previous = temporal redundancy.)");
}
