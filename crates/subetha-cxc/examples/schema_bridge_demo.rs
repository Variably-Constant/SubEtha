//! End-to-end demo of the production `CompressedSender` /
//! `CompressedReceiver` wrapper over the real reliable-UDP FEC transport
//! on loopback. Real `FatLineItem` slots, schema-compressed, recovered
//! byte-exact. Runs the stream twice (one slot per datagram, then
//! coalesced into MTU items the way the stream bridges batch) so the
//! datagram-rate effect is visible, then demonstrates mid-stream re-learn
//! across a schema drift.

use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use subetha_core::Marshal;
use subetha_cxc::compressed_udp::{CompressedReceiver, CompressedSender};
use subetha_cxc::schema_codec::SchemaTemplate;
use subetha_cxc::shared_deque_khpd::{FatLineItem, LineItem};

struct Rng(u64);
impl Rng {
    fn new(s: u64) -> Self {
        Self(s | 1)
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

fn gen_slots(n: usize, seed: u64, phase: u8) -> Vec<[u8; 64]> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::with_capacity(n);
    let mut id = 0u32;
    for _ in 0..n {
        let cnt = 1 + rng.below(3) as usize;
        let mut items = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            let mut b = [0u8; 16];
            b[0] = rng.below(16) as u8;
            b[1] = phase; // constant within a phase; flips on schema drift
            b[4..8].copy_from_slice(&id.to_le_bytes());
            id = id.wrapping_add(1);
            for x in b.iter_mut().skip(8) {
                *x = rng.byte();
            }
            items.push(LineItem::new(&b).unwrap());
        }
        let fat = FatLineItem::from_items(&items).unwrap();
        let mut s = [0u8; 64];
        fat.marshal(&mut s);
        out.push(s);
    }
    out
}

/// Ship `slots` once over the real transport, returning (goodput Mbit/s of
/// raw slot data, byte-exact). `coalesce` packs many compressed slots per
/// MTU item.
fn run_stream(
    port: u16,
    slots: &[[u8; 64]],
    tpl: SchemaTemplate,
    coalesce: bool,
) -> Result<(f64, bool), Box<dyn std::error::Error>> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let expected: Vec<[u8; 64]> = slots.to_vec();
    let total = expected.len();
    let rx = thread::spawn(move || -> bool {
        let mut recv = CompressedReceiver::bind(addr).expect("bind recv");
        let (mut got, mut ok) = (0usize, true);
        let start = Instant::now();
        while got < total {
            if start.elapsed() > Duration::from_secs(120) {
                return false;
            }
            for slot in recv.poll().unwrap_or_default() {
                if slot.as_slice() != &expected[got][..] {
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
    let max_item = if coalesce { 1400 } else { 256 };
    let mut send = CompressedSender::bind(addr, 8, 2, max_item, tpl)?;
    if coalesce {
        send = send.with_coalesce(1200);
    }
    let t0 = Instant::now();
    for s in slots {
        while send.flow_blocked() {
            send.pump_feedback().ok();
            if send.flow_blocked() {
                thread::sleep(Duration::from_micros(50));
            }
        }
        send.send_item(s)?;
    }
    send.flush()?;
    let acked = send.drain_until_acked(Duration::from_secs(120))?;
    let secs = t0.elapsed().as_secs_f64();
    let ok = rx.join().expect("rx join") && acked;
    let mbit = (slots.len() * 64) as f64 * 8.0 / secs / 1e6;
    Ok((mbit, ok))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n = 50_000usize;
    let slots = gen_slots(n, 0x1234_5678, 0);
    let stride = (slots.len() / 2000).max(1);
    let sample: Vec<&[u8]> = slots.iter().step_by(stride).map(|s| s.as_slice()).collect();
    let tpl = SchemaTemplate::learn(&sample, 64);
    println!(
        "template: {} of 64 bytes constant, compact slot ~{} bytes",
        tpl.constant(),
        tpl.compact_len()
    );

    let (g1, ok1) = run_stream(24650, &slots, tpl.clone(), false)?;
    let (g2, ok2) = run_stream(24655, &slots, tpl.clone(), true)?;
    println!("{n} real slots, byte-exact over the real FEC transport:");
    println!("  one slot / datagram : {g1:6.0} Mbit/s  byte-exact={ok1}");
    println!("  coalesced (MTU items): {g2:6.0} Mbit/s  byte-exact={ok2}  ({:.1}x)", g2 / g1.max(0.001));
    if !ok1 || !ok2 {
        return Err("stream byte-exact failed".into());
    }

    // --- re-learn across a mid-stream schema drift ---
    let half = 15_000usize;
    let mut drift = gen_slots(half, 0xa1, 0);
    drift.extend(gen_slots(half, 0xa2, 7)); // phase byte flips mid-stream
    let dtotal = drift.len();
    let dsample: Vec<&[u8]> = drift[..half].iter().step_by(8).map(|s| s.as_slice()).collect();
    let dtpl = SchemaTemplate::learn(&dsample, 64);
    let daddr: SocketAddr = "127.0.0.1:24660".parse().unwrap();
    let dexpect = drift.clone();
    let drx = thread::spawn(move || -> bool {
        let mut recv = CompressedReceiver::bind(daddr).expect("bind drift recv");
        let (mut got, mut ok) = (0usize, true);
        let start = Instant::now();
        while got < dexpect.len() {
            if start.elapsed() > Duration::from_secs(120) {
                return false;
            }
            for slot in recv.poll().unwrap_or_default() {
                if slot.as_slice() != &dexpect[got][..] {
                    ok = false;
                }
                got += 1;
            }
        }
        for _ in 0..100 {
            recv.nudge_feedback().ok();
            thread::sleep(Duration::from_millis(2));
        }
        ok && got == dexpect.len()
    });
    thread::sleep(Duration::from_millis(300));
    let mut dsend = CompressedSender::bind(daddr, 8, 2, 256, dtpl)?.with_relearn(512, 20);
    for s in &drift {
        while dsend.flow_blocked() {
            dsend.pump_feedback().ok();
            if dsend.flow_blocked() {
                thread::sleep(Duration::from_micros(50));
            }
        }
        dsend.send_item(s)?;
    }
    dsend.flush()?;
    let dacked = dsend.drain_until_acked(Duration::from_secs(120))?;
    let relearns = dsend.relearns();
    let dok = drx.join().expect("drift rx join");
    println!("\ndrift demo: {dtotal} slots, schema flips at the midpoint");
    println!("  re-learns triggered: {relearns}   byte-exact: {dok}   fully_acked: {dacked}");
    if !dok || !dacked || relearns == 0 {
        return Err("re-learn drift E2E failed".into());
    }
    println!("ALL E2E PASS: coalescing + re-learn, every slot byte-exact");
    Ok(())
}
