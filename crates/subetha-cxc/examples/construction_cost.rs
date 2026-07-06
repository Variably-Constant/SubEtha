//! Decompose end-to-end construction cost across the three layers:
//!
//!   1. SharedRing::create(path, capacity)
//!         - raw MMF primitive, no dispatcher, no shape inference
//!   2. Channel::create(path, MmfWorkloadShape::StreamingMpmc{..}, capacity)
//!         - typed Channel<T> wrapper, manual shape selection
//!   3. AutoIpc::new(path).capacity(N).build_channel::<T>()
//!         - full builder: shape inference + dispatcher pick + wrap
//!
//! Each is timed for N_ITERS rounds; the file is removed between
//! iterations so the OS sees a fresh path each time. Reports per-build
//! averages and the break-even N (vs the 58.2 ns/item steady-state
//! SPSC rate captured in SHARED_RING.md).
//!
//! Run with:
//!     cargo run --release --example construction_cost

use std::time::Instant;

use subetha_cxc::{
    AutoIpc, Channel, LazySharedRing, MmfWorkloadShape, SharedRing,
};

const N_ITERS: u32 = 500;
const STEADY_STATE_NS_PER_SEND: f64 = 58.2;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmpdir = std::env::temp_dir();
    let base = tmpdir.join("subetha_construction_cost");

    // Warm: page-cache canonicalisation + first-time-init.
    {
        let path = base.with_extension("warm.bin");
        let _ring = SharedRing::create(&path, 64)
            .map_err(|e| format!("warm SharedRing::create: {e:?}"))?;
        drop(_ring);
        std::fs::remove_file(&path).ok();
    }

    // Anon mode does not need a path, but we keep the same closure
    // shape so the bench harness reuses one code path.
    let anon_ring_ns = bench("SharedRing::create_anon (in-mem)", &base, N_ITERS, |_p| {
        let r = SharedRing::create_anon(64)
            .map_err(|e| format!("SharedRing::create_anon: {e:?}"))?;
        std::hint::black_box(&r);
        drop(r);
        Ok(())
    })?;

    // Lazy mode: construction does no syscalls. We never call .get()
    // here so the file create never fires - this measures the "channel
    // was speculatively created but never used" case.
    let lazy_unused_ns = bench("LazySharedRing::new (never used)", &base, N_ITERS, |p| {
        let lazy = LazySharedRing::new(p, 64);
        std::hint::black_box(&lazy);
        drop(lazy);
        Ok(())
    })?;

    // Lazy mode + first use: construction + first .try_push() pays the
    // setup cost on the first call. This is the "deferred but used"
    // case; total cost equals create() but moves it to first-use time.
    let lazy_used_ns = bench("LazySharedRing::new + first try_push", &base, N_ITERS, |p| {
        let lazy = LazySharedRing::new(p, 64);
        lazy.try_push(&[1u8; 8])
            .map_err(|e| format!("LazySharedRing::try_push: {e:?}"))?;
        std::hint::black_box(&lazy);
        drop(lazy);
        Ok(())
    })?;

    let raw_ring_ns = bench("SharedRing::create (file)", &base, N_ITERS, |p| {
        let r = SharedRing::create(p, 64)
            .map_err(|e| format!("SharedRing::create: {e:?}"))?;
        std::hint::black_box(&r);
        drop(r);
        Ok(())
    })?;

    let channel_manual_ns = bench("Channel::create (manual shape)", &base, N_ITERS, |p| {
        let chan: Channel<u64> = Channel::create(
            p,
            MmfWorkloadShape::StreamingMpmc { n_producers: 1, n_consumers: 1 },
            64,
        )?;
        std::hint::black_box(&chan);
        drop(chan);
        Ok(())
    })?;

    let autoipc_ns = bench("AutoIpc::new(..).build_channel", &base, N_ITERS, |p| {
        let chan: Channel<u64> = AutoIpc::new(p).capacity(64).build_channel()?;
        std::hint::black_box(&chan);
        drop(chan);
        Ok(())
    })?;

    println!();
    println!("=== Construction cost decomposition ===");
    println!("Iterations per measurement: {N_ITERS}");
    println!();
    println!("{:<42} {:>10}  {:>14}", "Path", "per build", "break-even N");
    println!("{:-<42}-{:->10}--{:->14}", "", "", "");
    print_row("LazySharedRing::new (never used)", lazy_unused_ns);
    print_row("SharedRing::create_anon (in-mem)", anon_ring_ns);
    print_row("LazySharedRing + first try_push", lazy_used_ns);
    print_row("SharedRing::create (file)", raw_ring_ns);
    print_row("Channel::create (manual shape)", channel_manual_ns);
    print_row("AutoIpc::new(..).build_channel", autoipc_ns);
    println!();
    println!("Steady-state SPSC: {STEADY_STATE_NS_PER_SEND:.1} ns/send (SHARED_RING.md)");
    println!();
    let anon_vs_file_speedup = raw_ring_ns / anon_ring_ns;
    println!("Anon vs file-backed speedup: {anon_vs_file_speedup:.1}x");
    println!();
    println!("Interpretation:");
    println!("  - create_anon skips file create + ftruncate + first-page-fault.");
    println!("    For one-shot scripts that do not need cross-process or disk,");
    println!("    this is the constructor to reach for; the break-even N drops");
    println!("    by the speedup factor.");
    println!("  - The cost ladder (raw -> manual -> auto) on the file-backed");
    println!("    side shows where dispatcher shape inference + Channel<T>");
    println!("    wrapping land vs the bare MMF primitive.");

    Ok(())
}

fn bench<F>(
    label: &str,
    base: &std::path::Path,
    n_iters: u32,
    mut f: F,
) -> Result<f64, Box<dyn std::error::Error>>
where
    F: FnMut(&std::path::Path) -> Result<(), Box<dyn std::error::Error>>,
{
    // Sanitize label for Windows: no colons or other invalid chars.
    let safe = label.replace([':', ' ', '(', ')', '.'], "_");
    let t0 = Instant::now();
    for i in 0..n_iters {
        let path = base.with_extension(format!("{safe}.{i}.bin"));
        f(&path)?;
        std::fs::remove_file(&path).ok();
    }
    let elapsed = t0.elapsed();
    let total_ns = elapsed.as_secs_f64() * 1e9;
    let per_ns = total_ns / f64::from(n_iters);
    println!(
        "{label:<42} total={elapsed:?}, per build = {:.1} us",
        per_ns / 1_000.0
    );
    Ok(per_ns)
}

fn print_row(label: &str, per_ns: f64) {
    let per_us = per_ns / 1_000.0;
    let break_even = per_ns / STEADY_STATE_NS_PER_SEND;
    println!("{label:<42} {per_us:>8.1} us  {break_even:>12.0} sends");
}
