//! Multi-process proof that OFFSET-class frames cross a shmfs process
//! boundary - the gap my earlier `open_shmfs` proof missed because it
//! used 8-byte inline items that never touched the payload region.
//!
//! A `send_frame` payload above the tiny inline budget spills to the
//! frame payload region and the slot carries only an offset descriptor.
//! Before the fix that region was a PRIVATE anon mmap, so the descriptor
//! crossed but the payload did not; now a shm-backed ring names a SHARED
//! `{prefix}_frames` region both processes map.
//!
//! The parent enqueues a mix of inline + offset frames (including the
//! 4670-byte DeusARC snapshot size and an 8000-byte near-block payload);
//! the worker attaches via `open_shmfs` and must recover every frame's
//! exact bytes AND its class.
//!
//! Run: cargo run --release -p subetha-cxc --example offset_frame_xproc_e2e

use std::error::Error;
use subetha_cxc::adaptive_ring::AdaptiveRing;
use subetha_cxc::frame_ring::FrameClass;

type BoxErr = Box<dyn Error + Send + Sync>;

const CAPACITY: usize = 256;
// (size, expected class) - mixes inline and offset, repeats offset to
// exercise alloc/free of multiple blocks across the boundary.
const FRAMES: &[(usize, FrameClass)] = &[
    (30, FrameClass::Inline),
    (4670, FrameClass::Offset), // the DeusARC snapshot size, exactly
    (8000, FrameClass::Offset), // near the 8192 block ceiling
    (4670, FrameClass::Offset),
    (12, FrameClass::Inline),
    (4670, FrameClass::Offset),
];

/// Deterministic payload for frame `seq` so both sides agree byte-exact.
fn payload(seq: usize, len: usize) -> Vec<u8> {
    (0..len).map(|j| (seq.wrapping_mul(31).wrapping_add(j)) as u8).collect()
}

// --big: a single frame ABOVE the default 8192 block, carried by a
// with_frames-sized region on BOTH sides. Exercises the DeusARC concern
// that a real snapshot can exceed 8 KB.
const BIG_BLOCK: usize = 16384;
const BIG_COUNT: usize = 64;
const BIG_PAYLOAD: usize = 12000;

fn main() -> Result<(), BoxErr> {
    let argv: Vec<String> = std::env::args().collect();
    let big = argv.iter().any(|a| a == "--big");
    if let Some(pos) = argv.iter().position(|a| a == "worker") {
        let name = argv.get(pos + 1).cloned().unwrap_or_default();
        return worker(&name, big);
    }

    let name = format!("subetha_offset_frame_e2e_{}", std::process::id());
    let mut ring = AdaptiveRing::create_shmfs(&name, 1, 1, CAPACITY)
        .map_err(|e| format!("create_shmfs: {e:?}"))?;
    if big {
        ring = ring.with_frames(BIG_BLOCK, BIG_COUNT);
        let p = payload(0, BIG_PAYLOAD);
        let got = ring.send_frame(0, &p).map_err(|e| format!("big send_frame: {e:?}"))?;
        if got != FrameClass::Offset {
            return Err(format!("big frame sent class {got:?}, expected Offset").into());
        }
        println!("parent: enqueued one {BIG_PAYLOAD}-byte offset frame (with_frames {BIG_BLOCK}) into '{name}'");
        let self_exe = std::env::current_exe()?;
        let status = std::process::Command::new(&self_exe)
            .arg("worker").arg(&name).arg("--big").status()?;
        drop(ring);
        let ok = status.success();
        println!("\nRESULT offset_frame_xproc --big: worker_exit={} -> {}",
            status.code().unwrap_or(-1),
            if ok { "PASS: >8 KB offset frame crossed via with_frames" } else { "FAIL" });
        if !ok { std::process::exit(1); }
        return Ok(());
    }

    for (seq, &(len, want)) in FRAMES.iter().enumerate() {
        let p = payload(seq, len);
        let got = ring.send_frame(0, &p).map_err(|e| format!("send_frame {seq}: {e:?}"))?;
        if got != want {
            return Err(format!("frame {seq}: sent class {got:?}, expected {want:?}").into());
        }
    }
    println!(
        "parent: enqueued {} frames ({} offset) into '{name}'",
        FRAMES.len(),
        FRAMES.iter().filter(|(_, c)| *c == FrameClass::Offset).count()
    );

    let self_exe = std::env::current_exe()?;
    let status = std::process::Command::new(&self_exe)
        .arg("worker").arg(&name).status()?;
    drop(ring);

    let ok = status.success();
    println!(
        "\nRESULT offset_frame_xproc: worker_exit={} -> {}",
        status.code().unwrap_or(-1),
        if ok { "PASS: offset frames crossed the shmfs boundary byte-exact" }
        else { "FAIL: offset payload did not cross" }
    );
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

fn worker(name: &str, big: bool) -> Result<(), BoxErr> {
    let mut ring = AdaptiveRing::open_shmfs(name, 1, 1, CAPACITY)
        .map_err(|e| format!("worker open_shmfs: {e:?}"))?;

    let mut out = Vec::new();

    if big {
        // Same with_frames geometry as the parent, then recover the one
        // >8 KB frame byte-exact.
        ring = ring.with_frames(BIG_BLOCK, BIG_COUNT);
        let mut tries = 0;
        loop {
            match ring.recv_frame(0, &mut out) {
                Ok(FrameClass::Offset) => break,
                Ok(c) => { eprintln!("worker: big frame class {c:?}"); std::process::exit(1); }
                Err(_) if tries < 5_000_000 => { tries += 1; std::hint::spin_loop(); }
                Err(e) => { eprintln!("worker: big frame never arrived: {e:?}"); std::process::exit(1); }
            }
        }
        if out != payload(0, BIG_PAYLOAD) {
            eprintln!("worker: big frame mismatch (len {})", out.len());
            std::process::exit(1);
        }
        println!("worker: recovered the {BIG_PAYLOAD}-byte offset frame byte-exact");
        return Ok(());
    }
    for (seq, &(len, want)) in FRAMES.iter().enumerate() {
        // Spin briefly for the frame to arrive (producer + consumer race).
        let mut tries = 0;
        let class = loop {
            match ring.recv_frame(0, &mut out) {
                Ok(c) => break c,
                Err(_) if tries < 5_000_000 => {
                    tries += 1;
                    std::hint::spin_loop();
                }
                Err(e) => {
                    eprintln!("worker: frame {seq} never arrived: {e:?}");
                    std::process::exit(1);
                }
            }
        };
        if class != want {
            eprintln!("worker: frame {seq} class {class:?}, expected {want:?}");
            std::process::exit(1);
        }
        let expect = payload(seq, len);
        if out != expect {
            eprintln!(
                "worker: frame {seq} payload mismatch (len got {} want {len})",
                out.len()
            );
            std::process::exit(1);
        }
    }
    println!("worker: recovered all {} frames byte-exact (offset payloads crossed)", FRAMES.len());
    Ok(())
}
