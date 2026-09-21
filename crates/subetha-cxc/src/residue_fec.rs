//! Residue erasure coding: recovery from redundant residues and the
//! Chinese Remainder Theorem, rather than from a linear system over a
//! finite field.
//!
//! A value is carried as its residues modulo a set of pairwise-coprime
//! moduli. More residues are sent than the value needs, so any
//! sufficient subset reconstructs it and the rest may be lost. The
//! moduli are published constants: nothing is chosen at run time,
//! nothing is seeded, and the encoder and decoder agree because the
//! table is part of the format rather than because they generate the
//! same numbers.
//!
//! The moduli are primes just above `2^16`, so every 16-bit chunk of
//! payload is a legal residue of every modulus and no value is
//! unrepresentable. The `k` information moduli are the `k` smallest of
//! the set, which is what makes any `k` surviving residues sufficient:
//! the value is below the product of the smallest `k`, so it is below
//! the product of any `k`.
//!
//! Reconstruction is Garner's mixed-radix algorithm, which needs only
//! arithmetic on single words. The mixed-radix digits are evaluated
//! modulo whichever modulus is missing, so a lost chunk is recovered
//! without the whole value ever being formed.

/// Primes immediately above `2^16`. Every 16-bit chunk is less than
/// each of them, so a chunk is always a legal residue.
const MODULI: [u32; 32] = [
    65537, 65539, 65543, 65551, 65557, 65563, 65579, 65581,
    65587, 65599, 65609, 65617, 65629, 65633, 65647, 65651,
    65657, 65677, 65687, 65699, 65701, 65707, 65713, 65717,
    65719, 65729, 65731, 65761, 65777, 65789, 65809, 65827,
];

/// Chunks a value is split into, and residues sent for it, at most.
pub const MAX_MODULI: usize = MODULI.len();

/// `decode` tracks which residues arrived as one bit per modulus in a
/// `u32`, so the table may not outgrow that word without the shift
/// becoming undefined. Stated here rather than left for whoever adds the
/// thirty-third prime to discover.
const _: () = assert!(MAX_MODULI <= u32::BITS as usize);

/// The shift Barrett reduction is taken at.
///
/// Every modulus lies in `[2^16, 2^17)`, and every value reduced here is
/// a product of two residues, so it is below `2^34`. Barrett's bound
/// wants the shift at twice the modulus width, which is that.
const BARRETT_SHIFT: u32 = 34;

/// `floor(2^34 / m)` for each modulus.
///
/// This is what turns `x % m` into a multiply and a shift. The moduli
/// are a published constant table, so the reciprocals are known at
/// compile time and cost nothing at run time.
const BARRETT: [u64; MAX_MODULI] = {
    let mut out = [0u64; MAX_MODULI];
    let mut i = 0;
    while i < MAX_MODULI {
        out[i] = (1u64 << BARRETT_SHIFT) / MODULI[i] as u64;
        i += 1;
    }
    out
};

/// `x mod m`, for `x` below `2^34` and `m` the modulus at `idx`.
///
/// Garner's inner loop is a modular multiply-subtract per pair of
/// residues, so the reduction runs as often as the multiply beside it.
/// A 64-bit `div` is twenty to thirty cycles against that multiply's
/// three, which is why the moduli being a constant table earns its
/// keep: every reciprocal is known at compile time.
///
/// The estimate can fall at most two multiples of `m` short, never over,
/// so two conditional subtractions land it exactly. Written as
/// subtractions rather than a loop because the count is a proof rather
/// than a condition to discover.
#[inline(always)]
fn reduce(x: u64, idx: usize) -> u32 {
    let m = MODULI[idx] as u64;
    let q = (x * BARRETT[idx]) >> BARRETT_SHIFT;
    let mut r = x - q * m;
    if r >= m {
        r -= m;
    }
    if r >= m {
        r -= m;
    }
    r as u32
}

/// What went wrong encoding or decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResidueError {
    /// `k + r` exceeds the published modulus table.
    TooManyModuli,
    /// Fewer than `k` residues survived, so the value is unrecoverable.
    NotEnoughResidues,
    /// A residue names a modulus outside the code's set.
    BadResidueIndex,
    /// A chunk or buffer length disagrees with the code's `k`.
    LengthMismatch,
}

impl std::fmt::Display for ResidueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResidueError::TooManyModuli => write!(f, "more moduli than the table holds"),
            ResidueError::NotEnoughResidues => write!(f, "fewer residues than the value needs"),
            ResidueError::BadResidueIndex => write!(f, "residue names no modulus of this code"),
            ResidueError::LengthMismatch => write!(f, "chunk count disagrees with the code"),
        }
    }
}

impl std::error::Error for ResidueError {}

/// `a^-1 mod m` for prime `m`, by Fermat's little theorem.
fn mod_inv(a: u32, m: u32) -> u32 {
    let mut result: u64 = 1;
    let mut base = (a % m) as u64;
    let mut exp = m - 2;
    let m64 = m as u64;
    while exp > 0 {
        if exp & 1 == 1 {
            result = result * base % m64;
        }
        base = base * base % m64;
        exp >>= 1;
    }
    result as u32
}

/// An erasure code over `k` information moduli and `r` redundant ones.
///
/// The value is `k` chunks of 16 bits. `encode` produces `r` redundant
/// residues; any `k` of the `k + r` residues recover every chunk, so up
/// to `r` may be lost.
#[derive(Debug, Clone)]
pub struct ResidueCode {
    k: usize,
    n: usize,
    /// `inv[i * n + j]` is `moduli[i]^-1 mod moduli[j]`, which is what
    /// Garner's algorithm consumes. Flat so a lookup is one index.
    inv: Vec<u32>,
    /// `residual[i * n + j]` is `moduli[i] mod moduli[j]`, the factor
    /// Horner multiplies by when evaluating the digits at another
    /// modulus.
    ///
    /// Both operands are constants of the code, and the loop that wants
    /// this runs once per digit per column: four thousand lookups for a
    /// 1 KiB packet, over the same handful of values.
    residual: Vec<u32>,
}

impl ResidueCode {
    /// A code carrying `k` chunks with `r` residues of redundancy.
    pub fn new(k: usize, r: usize) -> Result<Self, ResidueError> {
        let n = k + r;
        if k == 0 || n > MAX_MODULI {
            return Err(ResidueError::TooManyModuli);
        }
        let mut inv = vec![0u32; n * n];
        let mut residual = vec![0u32; n * n];
        for i in 0..n {
            for j in 0..n {
                residual[i * n + j] = MODULI[i] % MODULI[j];
                if i != j {
                    inv[i * n + j] = mod_inv(MODULI[i] % MODULI[j], MODULI[j]);
                }
            }
        }
        Ok(Self { k, n, inv, residual })
    }

    /// Chunks the code carries.
    pub fn k(&self) -> usize {
        self.k
    }

    /// Residues the code sends in total.
    pub fn n(&self) -> usize {
        self.n
    }

    /// Residues that may be lost with the value still recoverable.
    pub fn redundancy(&self) -> usize {
        self.n - self.k
    }

    /// The modulus a residue index names.
    pub fn modulus(&self, i: usize) -> u32 {
        MODULI[i]
    }

    /// Garner's mixed-radix digits for `have`, which names distinct
    /// moduli of this code in ascending order, written into `digits` in
    /// the same order.
    ///
    /// The caller owns the buffer because this runs once per 16-bit
    /// column of a packet, 512 times for a 1 KiB one, and a heap
    /// allocation on that path costs more than the arithmetic it would
    /// carry.
    fn mixed_radix(
        &self,
        have: &[(usize, u32)],
        digits: &mut [u32],
    ) -> Result<(), ResidueError> {
        if digits.len() != have.len() {
            return Err(ResidueError::LengthMismatch);
        }
        for (slot, &(idx, residue)) in have.iter().enumerate() {
            if idx >= self.n {
                return Err(ResidueError::BadResidueIndex);
            }
            let m = MODULI[idx];
            let mut value = reduce(residue as u64, idx);
            // Take out the contribution of every earlier digit, then
            // divide by the modulus that digit carries.
            for (earlier, &(prev_idx, _)) in have[..slot].iter().enumerate() {
                let d = reduce(digits[earlier] as u64, idx);
                // Both terms are below `m`, so the difference taken this
                // way is below `2m` and one subtraction settles it.
                value += m - d;
                if value >= m {
                    value -= m;
                }
                value = reduce(value as u64 * self.inv[prev_idx * self.n + idx] as u64, idx);
            }
            digits[slot] = value;
        }
        Ok(())
    }

    /// The digits evaluated modulo the modulus at `target`, by Horner
    /// from the most significant down, so the value they represent is
    /// reduced without being formed.
    ///
    /// `target` names the modulus rather than carrying it, because the
    /// reduction is by a precomputed reciprocal and that is looked up by
    /// index.
    fn evaluate_mod(&self, have: &[(usize, u32)], digits: &[u32], target: usize) -> u32 {
        let t = MODULI[target];
        let mut acc: u32 = 0;
        for slot in (0..digits.len()).rev() {
            // Both below `t`, so the sum is below `2t`.
            acc += reduce(digits[slot] as u64, target);
            if acc >= t {
                acc -= t;
            }
            if slot > 0 {
                let step = self.residual[have[slot - 1].0 * self.n + target];
                acc = reduce(acc as u64 * step as u64, target);
            }
        }
        acc
    }

    /// The `r` redundant residues for `chunks`, which is `k` values each
    /// below every modulus, as any `u16` is.
    pub fn encode(&self, chunks: &[u16], repair: &mut [u32]) -> Result<(), ResidueError> {
        if chunks.len() != self.k || repair.len() != self.redundancy() {
            return Err(ResidueError::LengthMismatch);
        }
        // On the stack, not the heap. `MAX_MODULI` bounds `k`, so the
        // worst case is a few hundred bytes of frame, and this runs once
        // per column of every packet.
        let mut have = [(0usize, 0u32); MAX_MODULI];
        for (i, slot) in have[..self.k].iter_mut().enumerate() {
            *slot = (i, chunks[i] as u32);
        }
        let have = &have[..self.k];

        let mut digits = [0u32; MAX_MODULI];
        let digits = &mut digits[..self.k];
        self.mixed_radix(have, digits)?;

        for (slot, out) in repair.iter_mut().enumerate() {
            *out = self.evaluate_mod(have, digits, self.k + slot);
        }
        Ok(())
    }

    /// Recover every chunk from any `k` surviving residues.
    ///
    /// `have` is `(index, residue)` for the residues that arrived, where
    /// an index below `k` names an information chunk and one at or above
    /// it names a redundant residue. Order does not matter, and residues
    /// past the `k` needed are ignored.
    pub fn decode(&self, have: &[(usize, u32)], out: &mut [u16]) -> Result<(), ResidueError> {
        if out.len() != self.k {
            return Err(ResidueError::LengthMismatch);
        }
        if have.len() < self.k {
            return Err(ResidueError::NotEnoughResidues);
        }
        // One slot per modulus the code has, so a residue placed at its
        // own index arrives in the ascending order Garner wants, a
        // repeated index loses to the first one, and nothing is sorted
        // or allocated. The k lowest indices carry the smallest moduli,
        // whose product is the bound the value was encoded under.
        let mut residues = [0u32; MAX_MODULI];
        let mut present: u32 = 0;
        for &(idx, residue) in have {
            if idx >= self.n {
                return Err(ResidueError::BadResidueIndex);
            }
            if present & (1 << idx) == 0 {
                present |= 1 << idx;
                residues[idx] = residue;
            }
        }
        if (present.count_ones() as usize) < self.k {
            return Err(ResidueError::NotEnoughResidues);
        }

        let mut used = [(0usize, 0u32); MAX_MODULI];
        let mut taken = 0usize;
        for idx in 0..self.n {
            if taken == self.k {
                break;
            }
            if present & (1 << idx) != 0 {
                used[taken] = (idx, residues[idx]);
                taken += 1;
            }
        }
        let used = &used[..self.k];

        let mut digits = [0u32; MAX_MODULI];
        let digits = &mut digits[..self.k];
        self.mixed_radix(used, digits)?;
        for (i, slot) in out.iter_mut().enumerate() {
            // A chunk that arrived is already known; one that did not is
            // the digits evaluated at its own modulus.
            *slot = match used.iter().find(|(idx, _)| *idx == i) {
                Some(&(_, residue)) => residue as u16,
                None => self.evaluate_mod(used, digits, i) as u16,
            };
        }
        Ok(())
    }
}

/// The code striped across whole packets: column `j` of the stripe is
/// the 16-bit value at byte offset `2j` of every source packet, so `k`
/// source packets of `len` bytes yield `r` repair packets that recover
/// any `r` lost packets of the group.
///
/// A residue needs seventeen bits where the chunk it protects needs
/// sixteen, and paying that on every column would cost a sixteenth of
/// the wire. The values that need the extra bit are those at or above
/// `2^16`, which for these moduli are a handful out of sixty-five
/// thousand, so a repair packet carries the low sixteen bits of each
/// residue and then a short list naming the columns that overflowed.
/// The expected cost of that list is a couple of bytes per packet
/// rather than a sixteenth of one.
#[derive(Debug, Clone)]
pub struct PacketResidueCode {
    code: ResidueCode,
}

/// Bytes of header a repair packet spends on its overflow list before
/// any column is named: the count itself.
const OVERFLOW_COUNT_BYTES: usize = 2;

impl PacketResidueCode {
    /// A code recovering any `r` lost packets from a group of `k`.
    pub fn new(k: usize, r: usize) -> Result<Self, ResidueError> {
        Ok(Self { code: ResidueCode::new(k, r)? })
    }

    /// The underlying residue code.
    pub fn code(&self) -> &ResidueCode {
        &self.code
    }

    /// Repair packets for `sources`, which is `k` packets of one even
    /// length. Each repair packet is that length plus its overflow
    /// list.
    pub fn encode_packets(
        &self,
        sources: &[&[u8]],
        repairs: &mut Vec<Vec<u8>>,
    ) -> Result<(), ResidueError> {
        let k = self.code.k();
        let r = self.code.redundancy();
        if sources.len() != k {
            return Err(ResidueError::LengthMismatch);
        }
        let len = sources[0].len();
        if !len.is_multiple_of(2) || sources.iter().any(|s| s.len() != len) {
            return Err(ResidueError::LengthMismatch);
        }
        let columns = len / 2;

        let mut bodies = vec![vec![0u8; len]; r];
        let mut overflows: Vec<Vec<u16>> = vec![Vec::new(); r];
        let mut chunks = vec![0u16; k];
        let mut residues = vec![0u32; r];

        for col in 0..columns {
            let at = col * 2;
            for (chunk, src) in chunks.iter_mut().zip(sources.iter()) {
                *chunk = u16::from_le_bytes([src[at], src[at + 1]]);
            }
            self.code.encode(&chunks, &mut residues)?;
            for j in 0..r {
                bodies[j][at..at + 2]
                    .copy_from_slice(&(residues[j] as u16).to_le_bytes());
                if residues[j] > u16::MAX as u32 {
                    overflows[j].push(col as u16);
                }
            }
        }

        repairs.clear();
        for (body, overflow) in bodies.into_iter().zip(overflows) {
            let mut packet = body;
            packet.extend_from_slice(&(overflow.len() as u16).to_le_bytes());
            for col in overflow {
                packet.extend_from_slice(&col.to_le_bytes());
            }
            repairs.push(packet);
        }
        Ok(())
    }

    /// Recover the `k` source packets from any `k` that arrived.
    ///
    /// `have` names each surviving packet by its index in the group:
    /// below `k` a source packet, at or above it a repair packet as
    /// `encode_packets` produced it, overflow list and all.
    pub fn decode_packets(
        &self,
        have: &[(usize, &[u8])],
        len: usize,
        out: &mut [Vec<u8>],
    ) -> Result<(), ResidueError> {
        let k = self.code.k();
        if out.len() != k || !len.is_multiple_of(2) {
            return Err(ResidueError::LengthMismatch);
        }
        if have.len() < k {
            return Err(ResidueError::NotEnoughResidues);
        }
        let columns = len / 2;

        // Read each repair packet's overflow list once, so a column
        // lookup during the walk is a set membership rather than a
        // reparse.
        let mut overflow_sets: Vec<(usize, Vec<u16>)> = Vec::new();
        for &(idx, packet) in have {
            if idx < k {
                continue;
            }
            if packet.len() < len + OVERFLOW_COUNT_BYTES {
                return Err(ResidueError::LengthMismatch);
            }
            let count =
                u16::from_le_bytes([packet[len], packet[len + 1]]) as usize;
            let need = len + OVERFLOW_COUNT_BYTES + count * 2;
            if packet.len() < need {
                return Err(ResidueError::LengthMismatch);
            }
            let mut cols = Vec::with_capacity(count);
            for c in 0..count {
                let at = len + OVERFLOW_COUNT_BYTES + c * 2;
                cols.push(u16::from_le_bytes([packet[at], packet[at + 1]]));
            }
            overflow_sets.push((idx, cols));
        }

        for slot in out.iter_mut() {
            slot.clear();
            slot.resize(len, 0);
        }

        let mut column = Vec::with_capacity(have.len());
        let mut recovered = vec![0u16; k];
        for col in 0..columns {
            let at = col * 2;
            column.clear();
            for &(idx, packet) in have {
                let low = u16::from_le_bytes([packet[at], packet[at + 1]]) as u32;
                let value = if idx < k {
                    low
                } else {
                    let overflowed = overflow_sets
                        .iter()
                        .find(|(i, _)| *i == idx)
                        .is_some_and(|(_, cols)| cols.contains(&(col as u16)));
                    if overflowed { low + (1 << 16) } else { low }
                };
                column.push((idx, value));
            }
            self.code.decode(&column, &mut recovered)?;
            for (i, slot) in out.iter_mut().enumerate() {
                slot[at..at + 2].copy_from_slice(&recovered[i].to_le_bytes());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reduction that replaced the divide gives the same answer the
    /// divide gave, for every modulus.
    ///
    /// This is the one place where being wrong is silent: a reduction
    /// off by a multiple of `m` produces a repair packet that decodes to
    /// plausible bytes rather than an error, so the round-trip tests
    /// below would pass on data that had been quietly corrupted. It is
    /// checked directly instead.
    ///
    /// Two sweeps. The small multiples of `m` are where the conditional
    /// subtractions decide, so every value up to `4m` is tried
    /// exhaustively. The rest of the range is walked with a fixed
    /// deterministic stride, so the test measures the code and not the
    /// day it ran.
    #[test]
    fn the_barrett_reduction_agrees_with_the_divide_it_replaced() {
        for idx in 0..MAX_MODULI {
            let m = MODULI[idx] as u64;
            for x in 0..=(4 * m) {
                assert_eq!(
                    reduce(x, idx) as u64,
                    x % m,
                    "modulus {m} at {x}, near a correction boundary"
                );
            }

            let mut state: u64 = 0x2545_F491_4F6C_DD1D ^ idx as u64;
            for _ in 0..200_000 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                // The bound the reduction is proved under: every value
                // it sees is a product of two residues.
                let x = state % (1 << BARRETT_SHIFT);
                assert_eq!(reduce(x, idx) as u64, x % m, "modulus {m} at {x}");
            }

            // The largest value that can reach it, which is where an
            // overflow in the estimate would show.
            let top = (1u64 << BARRETT_SHIFT) - 1;
            assert_eq!(reduce(top, idx) as u64, top % m);
        }
    }

    /// Every product of two residues stays inside the bound the
    /// reduction is proved under.
    ///
    /// The proof rests on the input being below `2^34`. The largest
    /// input is a residue times a modular inverse, both below the
    /// largest modulus, so this checks that the table cannot grow past
    /// the point where that stops holding.
    #[test]
    fn a_product_of_two_residues_stays_inside_the_barrett_bound() {
        let largest = MODULI[MAX_MODULI - 1] as u64;
        assert!(
            (largest - 1) * (largest - 1) < (1u64 << BARRETT_SHIFT),
            "the modulus table has outgrown the shift the reduction uses"
        );
    }

    /// Every chunk comes back when nothing was lost.
    #[test]
    fn round_trips_with_no_loss() {
        let code = ResidueCode::new(8, 4).unwrap();
        let chunks: Vec<u16> = (0..8).map(|i| (i * 7919 + 13) as u16).collect();
        let mut repair = vec![0u32; code.redundancy()];
        code.encode(&chunks, &mut repair).unwrap();

        let have: Vec<(usize, u32)> = (0..8).map(|i| (i, chunks[i] as u32)).collect();
        let mut out = vec![0u16; 8];
        code.decode(&have, &mut out).unwrap();
        assert_eq!(out, chunks);
    }

    /// Losing any single chunk still recovers it from one redundant
    /// residue.
    #[test]
    fn one_lost_chunk_is_recovered() {
        let code = ResidueCode::new(8, 4).unwrap();
        let chunks: Vec<u16> = (0..8).map(|i| (i * 30011 + 7) as u16).collect();
        let mut repair = vec![0u32; code.redundancy()];
        code.encode(&chunks, &mut repair).unwrap();

        for lost in 0..8 {
            let mut have: Vec<(usize, u32)> = (0..8)
                .filter(|i| *i != lost)
                .map(|i| (i, chunks[i] as u32))
                .collect();
            have.push((8, repair[0]));
            let mut out = vec![0u16; 8];
            code.decode(&have, &mut out).unwrap();
            assert_eq!(out, chunks, "chunk {lost} lost");
        }
    }

    /// The code recovers its full redundancy under every pattern of that
    /// many losses, which is what makes the overhead worth sending.
    #[test]
    fn every_pattern_up_to_the_redundancy_recovers() {
        let code = ResidueCode::new(6, 3).unwrap();
        let chunks: Vec<u16> = (0..6).map(|i| (i * 12345 + 999) as u16).collect();
        let mut repair = vec![0u32; code.redundancy()];
        code.encode(&chunks, &mut repair).unwrap();

        let all: Vec<(usize, u32)> = (0..6)
            .map(|i| (i, chunks[i] as u32))
            .chain((0..3).map(|j| (6 + j, repair[j])))
            .collect();

        for a in 0..9 {
            for b in (a + 1)..9 {
                for c in (b + 1)..9 {
                    let have: Vec<(usize, u32)> = all
                        .iter()
                        .copied()
                        .filter(|(i, _)| *i != a && *i != b && *i != c)
                        .collect();
                    let mut out = vec![0u16; 6];
                    code.decode(&have, &mut out)
                        .unwrap_or_else(|e| panic!("dropping {a},{b},{c}: {e}"));
                    assert_eq!(out, chunks, "dropping {a},{b},{c}");
                }
            }
        }
    }

    /// One loss past the redundancy is refused rather than answered with
    /// wrong chunks.
    #[test]
    fn past_the_redundancy_is_refused() {
        let code = ResidueCode::new(6, 2).unwrap();
        let chunks: Vec<u16> = (0..6).map(|i| (i * 4096 + 1) as u16).collect();
        let mut repair = vec![0u32; code.redundancy()];
        code.encode(&chunks, &mut repair).unwrap();

        let have: Vec<(usize, u32)> = vec![(0, chunks[0] as u32), (1, chunks[1] as u32)];
        let mut out = vec![0u16; 6];
        assert_eq!(
            code.decode(&have, &mut out),
            Err(ResidueError::NotEnoughResidues),
        );
    }

    /// Packets round-trip and any `r` lost ones come back, which is the
    /// property the wire cares about rather than the chunk property.
    #[test]
    fn any_r_lost_packets_are_recovered() {
        const LEN: usize = 256;
        let code = PacketResidueCode::new(6, 3).unwrap();
        let sources: Vec<Vec<u8>> = (0..6)
            .map(|i| {
                (0..LEN)
                    .map(|b| ((i * 31 + b * 17 + 5) % 256) as u8)
                    .collect()
            })
            .collect();
        let refs: Vec<&[u8]> = sources.iter().map(|s| s.as_slice()).collect();
        let mut repairs = Vec::new();
        code.encode_packets(&refs, &mut repairs).unwrap();
        assert_eq!(repairs.len(), 3);

        // Drop every combination of three of the nine packets sent.
        for a in 0..9 {
            for b in (a + 1)..9 {
                for c in (b + 1)..9 {
                    let mut have: Vec<(usize, &[u8])> = Vec::new();
                    for (i, src) in sources.iter().enumerate() {
                        if i != a && i != b && i != c {
                            have.push((i, src.as_slice()));
                        }
                    }
                    for (j, rep) in repairs.iter().enumerate() {
                        let idx = 6 + j;
                        if idx != a && idx != b && idx != c {
                            have.push((idx, rep.as_slice()));
                        }
                    }
                    let mut out = vec![Vec::new(); 6];
                    code.decode_packets(&have, LEN, &mut out)
                        .unwrap_or_else(|e| panic!("dropping {a},{b},{c}: {e}"));
                    for (i, (got, want)) in out.iter().zip(sources.iter()).enumerate() {
                        assert_eq!(got, want, "dropping {a},{b},{c}, packet {i}");
                    }
                }
            }
        }
    }

    /// What the redundancy actually costs on the wire. A repair packet
    /// carries the low sixteen bits of each residue plus the columns
    /// that needed a seventeenth, and that list stays near empty, so a
    /// repair packet is a source packet plus a couple of bytes rather
    /// than a source packet plus a sixteenth.
    #[test]
    fn a_repair_packet_costs_about_what_a_source_packet_does() {
        const LEN: usize = 1024;
        let code = PacketResidueCode::new(8, 2).unwrap();
        // Pseudo-random payload, so the overflow columns are whatever
        // the moduli make them rather than whatever a pattern makes.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let sources: Vec<Vec<u8>> = (0..8)
            .map(|_| {
                (0..LEN)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        (state >> 24) as u8
                    })
                    .collect()
            })
            .collect();
        let refs: Vec<&[u8]> = sources.iter().map(|s| s.as_slice()).collect();
        let mut repairs = Vec::new();
        code.encode_packets(&refs, &mut repairs).unwrap();

        for packet in &repairs {
            let extra = packet.len() - LEN;
            assert!(
                extra <= LEN / 64,
                "repair packet spent {extra} bytes past the {LEN}-byte body; \
                 the overflow list was meant to stay near empty",
            );
        }
    }

    /// The whole 16-bit range is representable, which is what choosing
    /// moduli above `2^16` buys.
    #[test]
    fn the_whole_chunk_range_survives() {
        let code = ResidueCode::new(4, 2).unwrap();
        for &value in &[0u16, 1, 32768, 65534, 65535] {
            let chunks = vec![value; 4];
            let mut repair = vec![0u32; code.redundancy()];
            code.encode(&chunks, &mut repair).unwrap();
            let have: Vec<(usize, u32)> = vec![
                (0, chunks[0] as u32),
                (1, chunks[1] as u32),
                (4, repair[0]),
                (5, repair[1]),
            ];
            let mut out = vec![0u16; 4];
            code.decode(&have, &mut out).unwrap();
            assert_eq!(out, chunks, "value {value}");
        }
    }
}
