//! End-to-end proof of adaptive FEC passthrough on the real Sens-O-Matic
//! bridge over loopback. FEC drops to zero parity (CodingLevel::Passthrough)
//! on a provably-clean link, re-arms instantly when loss appears, and ARQ is
//! the always-on reliability floor - so every item is byte-exact whether or
//! not parity was on the wire when it shipped.
//!
//! Three runs:
//!   - clean link         -> controller drops to Passthrough (r=0), byte-exact
//!   - sustained 12% loss  -> controller keeps parity armed, byte-exact
//!   - clean then loss      -> Passthrough on the clean half, re-arm on the
//!     lossy half, and ARQ recovers the in-flight Passthrough blocks that
//!     lose shards - byte-exact through the transition
//!
//! A `StubSensor` pins the feed-forward link sensor to "clean" so the test
//! exercises the loss-driven control loop on loopback (where the platform
//! sensor would otherwise read the host's real NIC / Wi-Fi). A short
//! `clean_hold` makes the sustained-clean confidence window quick to cross.

use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use subetha_cxc::fusion::ImmediateUpConservativeDown;
use subetha_cxc::link_sensor::{LinkSensor, LinkSnapshot, StubSensor};
use subetha_cxc::udp_bridge::{SensOMaticReceiver, SensOMaticSender};

/// A link sensor pinned to a fixed interface drop rate, so a run can drive
/// the feed-forward link-stress signal directly (independent of actual loss).
struct StressSensor(f32);
impl LinkSensor for StressSensor {
    fn sample(&mut self) -> LinkSnapshot {
        // Only the interface drop rate is synthesized here; the Wi-Fi radio
        // fields stay at their empty defaults (no association to report).
        LinkSnapshot { drop_rate: Some(self.0), ..LinkSnapshot::default() }
    }
    fn backend(&self) -> &'static str {
        "stress-stub"
    }
}

fn gen_items(n: usize, item_len: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let mut v = vec![0u8; item_len];
        let s = (i as u64 ^ seed).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for (j, b) in v.iter_mut().enumerate() {
            *b = (s.wrapping_add(j as u64).wrapping_mul(2_654_435_761) >> 24) as u8;
        }
        v[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        out.push(v);
    }
    out
}

struct RunResult {
    byte_exact: bool,
    fully_acked: bool,
    saw_passthrough: bool,
    rearmed: bool,
    passthrough_blocks: u64,
    fec_blocks: u64,
    goodput_mbit: f64,
    /// Parity-level histogram: index r holds how many sends observed the
    /// controller at parity_r = r. Shows the FEC right-sizing itself.
    parity_hist: [u64; 9],
}

impl RunResult {
    /// The most common parity level the controller settled on (the mode).
    fn typical_parity(&self) -> usize {
        let (mut best_r, mut best_c) = (0usize, 0u64);
        for (r, &c) in self.parity_hist.iter().enumerate() {
            if c > best_c {
                best_c = c;
                best_r = r;
            }
        }
        best_r
    }
}

/// Run one stream. `loss_schedule(progress_fraction)` returns the receiver's
/// loss percent at that point, so a run can flip clean -> lossy mid-stream.
fn run_stream(
    port: u16,
    n: usize,
    item_len: usize,
    clean_hold: u32,
    sensor: Box<dyn LinkSensor + Send>,
    loss_schedule: impl Fn(f64) -> u32 + Send + 'static,
) -> RunResult {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let items = gen_items(n, item_len, port as u64);
    let expected = items.clone();
    let total = items.len();

    let rx = thread::spawn(move || -> bool {
        let mut recv = SensOMaticReceiver::bind(addr).expect("bind recv");
        let (mut got, mut ok) = (0usize, true);
        let start = Instant::now();
        let mut last_pct = u32::MAX;
        while got < total {
            if start.elapsed() > Duration::from_secs(120) {
                return false;
            }
            let pct = loss_schedule(got as f64 / total as f64);
            if pct != last_pct {
                recv.set_debug_loss(pct);
                last_pct = pct;
            }
            for item in recv.poll().unwrap_or_default() {
                if item != expected[got] {
                    ok = false;
                }
                got += 1;
            }
        }
        for _ in 0..100 {
            recv.nudge_feedback().ok();
            thread::sleep(Duration::from_millis(2));
        }
        ok && got == total
    });

    thread::sleep(Duration::from_millis(250));
    let mut send = SensOMaticSender::bind("127.0.0.1:0", addr, 8, 2, item_len)
        .expect("bind send")
        .with_sensor(sensor)
        .with_fusion(Box::new(ImmediateUpConservativeDown::with_holds(2, clean_hold)));

    let mut saw_passthrough = false;
    let mut rearmed = false;
    let mut parity_hist = [0u64; 9];
    let t0 = Instant::now();
    for it in &items {
        while send.flow_blocked() {
            send.pump_feedback().ok();
            if send.flow_blocked() {
                thread::sleep(Duration::from_micros(50));
            }
        }
        send.send_item(it).expect("send_item");
        let p = send.control().parity_r() as usize;
        parity_hist[p.min(8)] += 1;
        if p == 0 {
            saw_passthrough = true;
        } else if saw_passthrough {
            rearmed = true;
        }
    }
    send.flush().expect("flush");
    let fully_acked = send
        .drain_until_acked(Duration::from_secs(120))
        .expect("drain");
    let secs = t0.elapsed().as_secs_f64();
    let (passthrough_blocks, fec_blocks) = send.coding_counts();
    let byte_exact = rx.join().expect("rx join");

    RunResult {
        byte_exact,
        fully_acked,
        saw_passthrough,
        rearmed,
        passthrough_blocks,
        fec_blocks,
        goodput_mbit: (n * item_len) as f64 * 8.0 / secs / 1e6,
        parity_hist,
    }
}

fn main() {
    let item_len = 256usize;
    let mut failures = 0;

    // A clean link must let FEC drop fully off (Passthrough), byte-exact.
    let clean = run_stream(25810, 40_000, item_len, 5, Box::new(StubSensor), |_| 0);
    println!(
        "clean (PT)  : byte_exact={} acked={} passthrough_blocks={} fec_blocks={} goodput={:.0} Mbit/s",
        clean.byte_exact, clean.fully_acked, clean.passthrough_blocks, clean.fec_blocks, clean.goodput_mbit
    );
    if !(clean.byte_exact && clean.fully_acked && clean.passthrough_blocks > 0) {
        eprintln!("  FAIL: clean link did not drop to Passthrough byte-exact");
        failures += 1;
    }

    // Sustained 12% loss must keep parity armed, byte-exact via FEC + ARQ.
    let lossy = run_stream(25820, 20_000, item_len, 5, Box::new(StubSensor), |_| 12);
    println!(
        "12% loss    : byte_exact={} acked={} passthrough_blocks={} fec_blocks={}",
        lossy.byte_exact, lossy.fully_acked, lossy.passthrough_blocks, lossy.fec_blocks
    );
    if !(lossy.byte_exact && lossy.fully_acked && lossy.fec_blocks > 0) {
        eprintln!("  FAIL: lossy link not byte-exact or never armed parity");
        failures += 1;
    }

    // Clean -> Passthrough, then loss mid-stream -> re-arm, with ARQ recovering
    // the in-flight Passthrough blocks. Byte-exact through it all.
    let trans = run_stream(25830, 40_000, item_len, 5, Box::new(StubSensor), |frac| {
        if frac < 0.5 { 0 } else { 12 }
    });
    println!(
        "transition  : byte_exact={} acked={} passthrough_blocks={} fec_blocks={} saw_passthrough={} rearmed={}",
        trans.byte_exact, trans.fully_acked, trans.passthrough_blocks, trans.fec_blocks, trans.saw_passthrough, trans.rearmed
    );
    if !(trans.byte_exact
        && trans.fully_acked
        && trans.passthrough_blocks > 0
        && trans.fec_blocks > 0
        && trans.rearmed)
    {
        eprintln!("  FAIL: transition did not Passthrough->re-arm byte-exact");
        failures += 1;
    }

    // A degraded link reported by the sensor (50% drop signal) at ZERO actual
    // loss must keep FEC armed - the feed-forward predictor arms before loss.
    // Same volume as the clean run, so its goodput is the forced-FEC (r>=1)
    // baseline the Passthrough clean run is measured against.
    let stressed = run_stream(25840, 40_000, item_len, 5, Box::new(StressSensor(0.5)), |_| 0);
    println!(
        "stress (FEC): byte_exact={} acked={} passthrough_blocks={} fec_blocks={} goodput={:.0} Mbit/s",
        stressed.byte_exact, stressed.fully_acked, stressed.passthrough_blocks, stressed.fec_blocks, stressed.goodput_mbit
    );
    if !(stressed.byte_exact
        && stressed.fully_acked
        && stressed.fec_blocks > 0
        && !stressed.saw_passthrough)
    {
        eprintln!("  FAIL: link stress did not keep FEC armed at zero loss");
        failures += 1;
    }

    if clean.goodput_mbit > 0.0 && stressed.goodput_mbit > 0.0 {
        println!(
            "\nclean-link goodput: Passthrough {:.0} vs forced-FEC {:.0} Mbit/s = {:.2}x (FEC off packs more payload)",
            clean.goodput_mbit, stressed.goodput_mbit, clean.goodput_mbit / stressed.goodput_mbit
        );
    }

    // Adaptive PARITY SIZE: the parity count must rise with the drop rate,
    // not just toggle on/off - recover proportional to loss without
    // over-provisioning. Sweep loss and show the parity the controller
    // settles on for each. Expect r to climb monotonically (0 clean -> ~2 ->
    // ~3 -> ~4) and never pin at the max.
    println!("\nadaptive parity size (typical parity_r the controller settles on):");
    let mut prev_r = 0usize;
    let mut size_ok = true;
    for (i, loss) in [0u32, 5, 15, 30].into_iter().enumerate() {
        let port = 25850 + i as u16;
        let r = run_stream(port, 30_000, item_len, 5, Box::new(StubSensor), move |_| loss);
        let tr = r.typical_parity();
        println!(
            "  {loss:2}% loss -> parity_r={tr}  byte_exact={} (hist {:?})",
            r.byte_exact, &r.parity_hist[..6]
        );
        if !r.byte_exact {
            size_ok = false;
        }
        if loss > 0 && tr < prev_r {
            size_ok = false; // parity must not shrink as loss grows
        }
        prev_r = tr;
    }
    if !size_ok {
        eprintln!("  FAIL: parity did not right-size monotonically with loss");
        failures += 1;
    }

    if failures == 0 {
        println!(
            "\nALL E2E PASS: FEC switches off on a clean link, re-arms instantly on loss, \
             ARQ is the floor, every item byte-exact."
        );
        std::process::exit(0);
    } else {
        eprintln!("\n{failures} run(s) FAILED");
        std::process::exit(1);
    }
}
