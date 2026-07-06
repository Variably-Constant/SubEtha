//! E2E: the AdaptiveRing carries variable-payload frames at every shape.
//!
//! Ships a mixed-size record stream through each shape - SPSC, then
//! morphing live into MPSC, MPMC, and Vyukov - recovers every record
//! byte-for-byte, and reports the inline/offset split. This is the
//! running-binary proof that `send_frame` / `recv_frame` work across
//! all four shapes with the payload size chosen per record.

use subetha_cxc::{AdaptiveRing, FrameClass, RingShape};

/// Send each size, recover it immediately, verify byte-for-byte.
/// Returns (inline_count, offset_count).
fn run_shape(ring: &AdaptiveRing, shape: RingShape) -> (usize, usize) {
    // Sizes straddle the 51-byte inline budget so both paths fire.
    let sizes = [8usize, 40, 51, 52, 200, 4000];
    let mut inline = 0;
    let mut offset = 0;
    let mut out = Vec::new();
    for (i, &len) in sizes.iter().enumerate() {
        let payload: Vec<u8> = (0..len).map(|k| (i + k) as u8).collect();
        let sent = ring.send_frame(0, &payload).expect("send_frame");
        match sent {
            FrameClass::Inline => inline += 1,
            FrameClass::Offset => offset += 1,
        }
        let got = ring.recv_frame(0, &mut out).expect("recv_frame");
        assert_eq!(got, sent, "{shape:?} size {len} class mismatch");
        assert_eq!(out, payload, "{shape:?} size {len} payload mismatch");
    }
    (inline, offset)
}

fn main() {
    let ring = AdaptiveRing::create_anon(4, 4, 64).expect("create_anon");
    println!("AdaptiveRing variable-payload frames at every shape:\n");

    let mut total_inline = 0;
    let mut total_offset = 0;
    for shape in [RingShape::Spsc, RingShape::Mpsc, RingShape::Mpmc, RingShape::Vyukov] {
        if shape != RingShape::Spsc {
            ring.morph_to(shape).expect("morph_to");
        }
        let (inline, offset) = run_shape(&ring, shape);
        total_inline += inline;
        total_offset += offset;
        println!(
            "  {shape:<7?}: {inline} inline + {offset} offset records, all recovered byte-for-byte",
        );
    }

    let total = total_inline + total_offset;
    println!(
        "\ntotal: {total_inline} inline + {total_offset} offset = {total} records across 4 shapes",
    );
    println!("integrity: PASS (every record recovered byte-for-byte through 3 live morphs)");
}
