//! Loss-burst-length probe.
//!
//! The sender ships `count` sequence-stamped UDP datagrams at a paced
//! rate; the receiver records which arrived and histograms the lengths of
//! consecutive-missing runs. That run-length distribution sizes the FEC
//! hierarchy: if loss is dominated by length-1 runs (isolated drops), one
//! code scale plus interleaving suffices and the iterative decoder buys
//! nothing; if there is real mass at long runs, the segment /
//! super-segment hierarchy and the cross-scale iterative decoder earn
//! their complexity.
//!
//! Pacing is below link capacity on purpose: this measures the link's own
//! loss burstiness (interference, contention), not congestion drop from
//! overrunning a buffer. It also reports the Gilbert-Elliott signature
//! `P(lost | prev lost)` vs the marginal `P(lost)` - when the conditional
//! is much higher, loss is bursty (correlated), which is exactly what the
//! hierarchy targets.
//!
//! ```text
//! server: loss_burst_probe --role server --bind 0.0.0.0:9100 --count 100000 --bytes 1400
//! client: loss_burst_probe --role client --connect HOST:9100 --count 100000 --pps 5000 --bytes 1400
//! ```

use std::net::UdpSocket;
use std::time::{Duration, Instant};

struct Args {
    role: String,
    addr: String,
    count: usize,
    pps: u64,
    bytes: usize,
    secs: u64,
    threads: usize,
}

fn parse() -> Args {
    let mut a = Args {
        role: String::new(),
        addr: String::new(),
        count: 100_000,
        pps: 5_000,
        bytes: 1400,
        secs: 20,
        threads: 4,
    };
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--role" => {
                a.role = argv[i + 1].clone();
                i += 2;
            }
            "--bind" | "--connect" => {
                a.addr = argv[i + 1].clone();
                i += 2;
            }
            "--count" => {
                a.count = argv[i + 1].parse().unwrap();
                i += 2;
            }
            "--pps" => {
                a.pps = argv[i + 1].parse().unwrap();
                i += 2;
            }
            "--bytes" => {
                a.bytes = argv[i + 1].parse().unwrap();
                i += 2;
            }
            "--secs" => {
                a.secs = argv[i + 1].parse().unwrap();
                i += 2;
            }
            "--threads" => {
                a.threads = argv[i + 1].parse().unwrap();
                i += 2;
            }
            other => panic!("unknown arg {other}"),
        }
    }
    a
}

const SENTINEL: u32 = u32::MAX;

fn client(a: &Args) {
    let sock = UdpSocket::bind("0.0.0.0:0").expect("bind");
    sock.connect(&a.addr).expect("connect");
    let mut buf = vec![0u8; a.bytes];
    let gap = Duration::from_secs_f64(1.0 / a.pps as f64);
    let start = Instant::now();
    let mut next = start;
    for seq in 0..a.count {
        buf[0..4].copy_from_slice(&(seq as u32).to_le_bytes());
        sock.send(&buf).ok();
        next += gap;
        while Instant::now() < next {
            std::hint::spin_loop();
        }
    }
    // Sentinels (spaced) so the receiver ends promptly even if the tail is
    // lost.
    buf[0..4].copy_from_slice(&SENTINEL.to_le_bytes());
    for _ in 0..40 {
        sock.send(&buf).ok();
        std::thread::sleep(Duration::from_millis(5));
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "CLIENT sent {} pkts in {:.1}s ({:.0} pps, {:.1} Mbit/s)",
        a.count,
        secs,
        a.count as f64 / secs,
        a.count as f64 * a.bytes as f64 * 8.0 / secs / 1e6
    );
}

fn server(a: &Args) {
    let sock = UdpSocket::bind(&a.addr).expect("bind");
    sock.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let mut recd = vec![false; a.count];
    let mut buf = vec![0u8; a.bytes + 64];
    println!("BOUND {}", a.addr);
    // A failed recv (the read timeout) ends the loop: the sender is done
    // and any tail is lost.
    while sock.recv(&mut buf).is_ok() {
        let seq = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if seq == SENTINEL {
            break;
        }
        if (seq as usize) < a.count {
            recd[seq as usize] = true;
        }
    }
    report(&recd);
}

fn report(recd: &[bool]) {
    let n = recd.len();
    let lost: usize = recd.iter().filter(|&&r| !r).count();
    // Consecutive-missing run lengths.
    let mut runs: Vec<usize> = Vec::new();
    let mut cur = 0usize;
    for &r in recd {
        if r {
            if cur > 0 {
                runs.push(cur);
                cur = 0;
            }
        } else {
            cur += 1;
        }
    }
    if cur > 0 {
        runs.push(cur);
    }
    // Gilbert-Elliott signature: P(lost) vs P(lost | prev lost).
    let mut prev_lost_and_lost = 0usize;
    let mut prev_lost = 0usize;
    for w in recd.windows(2) {
        if !w[0] {
            prev_lost += 1;
            if !w[1] {
                prev_lost_and_lost += 1;
            }
        }
    }
    let p_lost = lost as f64 / n as f64;
    let p_cond = if prev_lost > 0 {
        prev_lost_and_lost as f64 / prev_lost as f64
    } else {
        0.0
    };
    // Bucket the run lengths.
    let buckets = [1usize, 2, 4, 8, 16, 32, 64, usize::MAX];
    let labels = ["1", "2", "3-4", "5-8", "9-16", "17-32", "33-64", "65+"];
    let mut counts = [0usize; 8];
    let mut lost_in = [0usize; 8];
    for &len in &runs {
        let mut bi = 0;
        while len > buckets[bi] {
            bi += 1;
        }
        counts[bi] += 1;
        lost_in[bi] += len;
    }
    let max_run = runs.iter().copied().max().unwrap_or(0);
    let n_runs = runs.len();
    let mean_run = if n_runs > 0 {
        lost as f64 / n_runs as f64
    } else {
        0.0
    };

    println!(
        "RESULT count={n} lost={lost} loss={:.2}% runs={n_runs} mean_run={mean_run:.2} max_run={max_run} \
         p_lost={p_lost:.4} p_lost_given_prev_lost={p_cond:.4} burst_ratio={:.1}",
        p_lost * 100.0,
        if p_lost > 0.0 { p_cond / p_lost } else { 0.0 }
    );
    println!("run-length histogram (runs | lost pkts in bucket | % of all lost):");
    for i in 0..8 {
        if counts[i] > 0 {
            println!(
                "  len {:<5} runs={:<6} lost={:<7} ({:.1}% of loss)",
                labels[i],
                counts[i],
                lost_in[i],
                if lost > 0 {
                    lost_in[i] as f64 / lost as f64 * 100.0
                } else {
                    0.0
                }
            );
        }
    }
}

/// Greedy congester: `threads` non-blocking senders flood `addr` for
/// `secs` seconds. The point is to fill the shared egress qdisc so it
/// tail-drops / AQM-drops, inducing real contention loss on the path that
/// a concurrent paced `client` then measures.
fn blast(a: &Args) {
    let total: u64 = (0..a.threads)
        .map(|_| {
            let addr = a.addr.clone();
            let bytes = a.bytes;
            let secs = a.secs;
            std::thread::spawn(move || -> u64 {
                let sock = UdpSocket::bind("0.0.0.0:0").expect("bind");
                sock.connect(&addr).expect("connect");
                sock.set_nonblocking(true).ok();
                let buf = vec![0xa5u8; bytes];
                let until = Instant::now() + Duration::from_secs(secs);
                let mut sent = 0u64;
                while Instant::now() < until {
                    // Ignore WouldBlock: the buffer being full IS the
                    // congestion we want to create.
                    if sock.send(&buf).is_ok() {
                        sent += 1;
                    }
                }
                sent
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap_or(0))
        .sum();
    println!(
        "BLAST {} threads, {} pkts in {}s ({:.0} Mbit/s offered)",
        a.threads,
        total,
        a.secs,
        total as f64 * a.bytes as f64 * 8.0 / a.secs as f64 / 1e6
    );
}

/// Sink for the blaster: receive and discard until idle.
fn drain(a: &Args) {
    let sock = UdpSocket::bind(&a.addr).expect("bind");
    sock.set_read_timeout(Some(Duration::from_secs(a.secs + 10))).ok();
    let mut buf = vec![0u8; a.bytes + 64];
    println!("DRAIN {}", a.addr);
    let mut got = 0u64;
    while sock.recv(&mut buf).is_ok() {
        got += 1;
    }
    println!("DRAIN received {got} pkts");
}

fn main() {
    let a = parse();
    match a.role.as_str() {
        "server" => server(&a),
        "client" => client(&a),
        "blast" => blast(&a),
        "drain" => drain(&a),
        other => panic!("--role must be server|client|blast|drain, got {other}"),
    }
}
