//! Cross-host E2E of the full throughput stack: real `FatLineItem` slots,
//! schema-compressed, shipped through the sharded reliable-UDP FEC
//! transport (N independent streams across cores), recovered byte-exact,
//! with the wire-byte and goodput effect measured against the
//! uncompressed baseline.
//!
//! The template is shared deterministically (both ends generate the same
//! slot stream from `--seed` and learn the same template); in-band
//! negotiation over the heartbeat plane is a separate concern. `--compress
//! 0` sends raw 64-byte slots through the same sharded path as the
//! baseline; `--compress 1` sends the compact form.
//!
//! ```text
//! server: compressed_sharded_lan --role server --bind 0.0.0.0:PORT --items N --shards S --compress 1 --loss L
//! client: compressed_sharded_lan --role client --connect HOST:PORT --items N --shards S --compress 1 --seed 0x99
//! ```

use std::net::SocketAddr;
use std::time::Instant;

use subetha_core::Marshal;
use subetha_cxc::schema_codec::SchemaTemplate;
use subetha_cxc::shared_deque_khpd::{FatLineItem, LineItem};
use subetha_cxc::sharded_udp::{ShardedReceiver, ShardedSender};

struct Args {
    role: String,
    addr: SocketAddr,
    items: u64,
    shards: usize,
    compress: bool,
    loss: u32,
    seed: u64,
}

fn parse() -> Args {
    let mut role = String::new();
    let mut addr: SocketAddr = "127.0.0.1:24700".parse().unwrap();
    let (mut items, mut shards, mut compress, mut loss, mut seed) =
        (100_000u64, 3usize, true, 0u32, 0x99u64);
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        let val = argv[i + 1].clone();
        match argv[i].as_str() {
            "--role" => role = val,
            "--bind" | "--connect" => addr = val.parse().expect("addr"),
            "--items" => items = val.parse().expect("items"),
            "--shards" => shards = val.parse().expect("shards"),
            "--compress" => compress = val != "0",
            "--loss" => loss = val.parse().expect("loss"),
            "--seed" => {
                let h = val.trim_start_matches("0x");
                seed = u64::from_str_radix(h, 16).or_else(|_| h.parse()).unwrap_or(0x99);
            }
            other => panic!("unknown arg {other}"),
        }
        i += 2;
    }
    Args {
        role,
        addr,
        items,
        shards,
        compress,
        loss,
        seed,
    }
}

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

fn gen_slots(n: u64, seed: u64) -> Vec<[u8; 64]> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::with_capacity(n as usize);
    let mut id = 0u32;
    for _ in 0..n {
        let cnt = 1 + rng.below(3) as usize;
        let mut items = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            let mut b = [0u8; 16];
            b[0] = rng.below(16) as u8;
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

fn template(slots: &[[u8; 64]]) -> SchemaTemplate {
    let stride = (slots.len() / 2000).max(1);
    let sample: Vec<&[u8]> = slots.iter().step_by(stride).map(|s| s.as_slice()).collect();
    SchemaTemplate::learn(&sample, 64)
}

fn server(a: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let slots = gen_slots(a.items, a.seed);
    let tpl = template(&slots);
    let mut recv =
        ShardedReceiver::bind(a.addr.ip(), a.addr.port(), a.shards, a.items, a.loss, a.seed)?;
    println!("BOUND {} (sharded x{} compress={})", a.addr, a.shards, a.compress);
    let (mut got, mut ok) = (0u64, true);
    let mut t_first: Option<Instant> = None;
    let mut slot = [0u8; 64];
    while got < a.items {
        match recv.recv_item() {
            Some(item) => {
                t_first.get_or_insert_with(Instant::now);
                let decoded: &[u8] = if a.compress {
                    tpl.decode(&item, &mut slot);
                    &slot
                } else {
                    &item
                };
                if decoded != &slots[got as usize][..] {
                    ok = false;
                }
                got += 1;
            }
            None => return Err("a shard ended before delivering all items".into()),
        }
    }
    recv.finish();
    let secs = t_first.unwrap().elapsed().as_secs_f64();
    // Goodput is delivered slot data (raw 64B/slot), so compression shows
    // as a higher rate at the same wire capacity.
    let mbit = a.items as f64 * 64.0 * 8.0 / secs / 1e6;
    println!(
        "RESULT role=server shards={} compress={} loss={} items={} goodput_mbit={:.0} byte_exact={ok}",
        a.shards, a.compress, a.loss, a.items, mbit
    );
    if !ok {
        return Err("byte-exact verification failed".into());
    }
    Ok(())
}

fn client(a: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let slots = gen_slots(a.items, a.seed);
    let tpl = template(&slots);
    let mut send = ShardedSender::bind(a.addr.ip(), a.addr.port(), a.shards, 8, 2, 256)?;
    let mut cbuf = Vec::with_capacity(80);
    let mut wire = 0u64;
    for s in &slots {
        if a.compress {
            cbuf.clear();
            tpl.encode(s, &mut cbuf);
            wire += cbuf.len() as u64;
            send.send_item(&cbuf);
        } else {
            wire += 64;
            send.send_item(s);
        }
    }
    let acked = send.finish();
    // This counts the pre-FEC payload, NOT the datagrams. The actual wire
    // bytes are the FEC datagrams (header + per-block shard length); they
    // are measured externally by capturing the UDP flow (tcpdump).
    println!(
        "RESULT role=client shards={} compress={} payload_bytes={} ({:.0}% of raw payload) fully_acked={acked}",
        a.shards,
        a.compress,
        wire,
        wire as f64 / (a.items * 64) as f64 * 100.0
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = parse();
    match a.role.as_str() {
        "server" => server(&a),
        "client" => client(&a),
        other => Err(format!("--role must be server|client, got {other}").into()),
    }
}
