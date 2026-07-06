//! Comprehensive cross-process IPC comparison.
//!
//! Benches the four SubEtha pinned channel shapes against every
//! canonical local IPC mechanism + every reasonable Rust crate,
//! and outputs machine-readable JSON.
//!
//! Contenders:
//!
//! - SubEtha pinned SPSC / MPSC / MPMC / Vyukov: a file-backed
//!   `AdaptiveRing` in each direction, morphed to the target shape
//!   and pinned via `pin_current_shape()` in BOTH processes, with
//!   the ping-pong running on the pinned native-primitive calls.
//!   This is the production hot path of the adaptive system - not
//!   a hand-rolled single-purpose MMF.
//! - TCP loopback (`std::net::TcpStream`)
//! - UDP loopback (`std::net::UdpSocket`)
//! - Named pipe / Local socket (`interprocess` crate)
//! - Anonymous stdio pipe (`std::process::Command`)
//! - ipc-channel (Mozilla, used in Servo / Firefox)
//! - iceoryx2 (Eclipse zero-copy IPC, Rust port)
//!
//! Every SubEtha contender declares `max_producers = 1,
//! max_consumers = 1` so the four shapes run the SAME 1P/1C
//! workload through four different dispatch paths; the deltas
//! between the four rows are pure shape-dispatch cost, not
//! peer-count scan overhead.
//!
//! Output:
//! - `docs/cross_process_ipc_results.json` - machine-readable results
//!
//! Save the JSON per host as `docs/cross_process_ipc_results-<platform>.json`;
//! the committed multi-host dot plots are rendered from that per-host set.
//!
//! Run: `cargo run --release --example cross_process_compare -p subetha-cxc`

use std::env;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use interprocess::local_socket::{
    prelude::*, GenericNamespaced, ListenerOptions, Stream as LocalStream,
};
use serde::{Deserialize, Serialize};
use subetha_cxc::adaptive_ring::{AdaptiveRing, PinnedRing, RingShape};

const N_ITEMS: u64 = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchResult {
    name: String,
    transport_kind: String,
    n_roundtrips: u64,
    total_ns: u64,
    rt_ns: f64,
    one_way_ns: f64,
    rank: usize,
    vs_fastest: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ComparisonReport {
    machine: String,
    timestamp: u64,
    payload_bytes: u32,
    results: Vec<BenchResult>,
}

fn tmp(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_compare_{pid}_{nonce}_{name}.bin"));
    p
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("--child-adaptive") => {
            return run_child_adaptive(&args[2], &args[3], &args[4])
        }
        Some("--child-tcp") => return run_child_tcp(args[2].parse()?),
        Some("--child-udp") => return run_child_udp(args[2].parse()?),
        Some("--child-localsock") => return run_child_localsock(&args[2]),
        Some("--child-stdio") => return run_child_stdio(),
        Some("--child-ipcchan") => return run_child_ipcchan(&args[2]),
        #[cfg(not(any(target_os = "freebsd", target_os = "macos")))]
        Some("--child-iceoryx2") => return run_child_iceoryx2(&args[2]),
        #[cfg(feature = "zmq-bench")]
        Some("--child-zmq") => return run_child_zmq(&args[2]),
        _ => {}
    }
    run_parent()
}

/// Shape name <-> RingShape mapping shared by parent + child arms.
fn shape_from_name(name: &str) -> RingShape {
    match name {
        "spsc" => RingShape::Spsc,
        "mpsc" => RingShape::Mpsc,
        "mpmc" => RingShape::Mpmc,
        "vyukov" => RingShape::Vyukov,
        other => panic!("unknown shape arg: {other}"),
    }
}

fn shape_arg(shape: RingShape) -> &'static str {
    match shape {
        RingShape::Spsc => "spsc",
        RingShape::Mpsc => "mpsc",
        RingShape::Mpmc => "mpmc",
        RingShape::Vyukov => "vyukov",
    }
}

/// Push through the pinned handle using the pinned shape's native
/// call. Producer id / consumer id are 0: every contender declares
/// max 1 producer + 1 consumer.
/// The adaptive contenders default to named anonymous
/// shared-memory sections (pagefile-backed on Windows, shmfs on
/// unix). This is the apples-to-apples backing across platforms:
/// Linux temp directories are tmpfs (memory) already, while a
/// real NTFS file behind the mapping costs the Windows ping-pong
/// 1.5-2.7 us one-way against ~100-240 ns section-backed - the
/// filesystem's dirty-page machinery, not the ring protocol.
/// SUBETHA_COMPARE_FILE=1 selects real-file backing to measure
/// exactly that effect.
fn use_shm_backing() -> bool {
    std::env::var_os("SUBETHA_COMPARE_FILE").is_none_or(|v| v != "1")
}

/// Flat section name derived from a temp-path prefix (section
/// names cannot carry path separators).
fn shm_name(prefix: &std::path::Path) -> String {
    format!(
        "subetha_cmp_{}",
        prefix.file_name().map(|f| f.to_string_lossy()).unwrap_or_default()
    )
}

#[inline]
fn pinned_push(pin: &PinnedRing<'_>, shape: RingShape, payload: &[u8]) -> bool {
    let r = match shape {
        RingShape::Spsc => pin.spsc_try_push(payload),
        RingShape::Mpsc => pin.mpsc_try_push(0, payload),
        RingShape::Mpmc => pin.mpmc_try_push(0, payload),
        RingShape::Vyukov => pin.vyukov_try_push(payload),
    };
    r.is_ok()
}

#[inline]
fn pinned_pop(pin: &PinnedRing<'_>, shape: RingShape, out: &mut [u8]) -> bool {
    let r = match shape {
        RingShape::Spsc => pin.spsc_try_pop(out),
        RingShape::Mpsc => pin.mpsc_try_pop(out),
        RingShape::Mpmc => pin.mpmc_try_pop(0, out),
        RingShape::Vyukov => pin.vyukov_try_pop(out),
    };
    r.is_ok()
}

/// Pop with the production wait discipline: short bounded spin,
/// then a budgeted hardware monitor-wait armed on the shape's
/// publish signal. Raw unbounded PAUSE spinning measures fine on
/// Linux/FreeBSD but gets descheduled and migrated by the Windows
/// scheduler (measured 1.7-2.7 us one-way vs ~100-300 ns); the
/// armed monitor wakes on the producer's store itself. Hosts
/// without a monitor family fall back to pure spin (the wait call
/// returns immediately).
///
/// Lost-wake-free: the signal value is sampled BEFORE the last
/// pop attempt, so a publish that lands after the sample makes
/// the wait return instantly (value != sampled).
#[inline]
fn pinned_pop_wait(pin: &PinnedRing<'_>, shape: RingShape, out: &mut [u8]) -> bool {
    const SPINS: usize = 128;
    const MONITOR_STEP_CYCLES: u64 = 20_000;
    for _ in 0..SPINS {
        if pinned_pop(pin, shape, out) {
            return true;
        }
        std::hint::spin_loop();
    }
    let sig = pin.recv_signal(shape);
    let cur = sig.load(std::sync::atomic::Ordering::Relaxed);
    if pinned_pop(pin, shape, out) {
        return true;
    }
    subetha_cxc::monitor_wait_u64(sig, cur, MONITOR_STEP_CYCLES);
    pinned_pop(pin, shape, out)
}

/// Remove every backing file an AdaptiveRing created under `prefix`
/// with max_producers = 1.
fn cleanup_adaptive_files(prefix: &std::path::Path) {
    for suffix in [".spsc.bin", ".mpsc.0.bin", ".mpmc.0.bin", ".vyukov.bin"] {
        let mut p = prefix.as_os_str().to_owned();
        p.push(suffix);
        std::fs::remove_file(PathBuf::from(p)).ok();
    }
}

/// Number of round-trip runs per contender; the minimum is kept.
const REPEATS: usize = 5;

/// Run a contender's round-trip bench `REPEATS` times and keep the fastest
/// (minimum) total. Cross-process round-trip latency is scheduling-sensitive:
/// any single run can be inflated several-fold by background load on the
/// host. The minimum over repeats is the run least perturbed by that load -
/// the stable, comparable latency - and the first (cold) run is discarded by
/// construction since it is never the minimum once the machine is warm.
fn best_of<F>(mut f: F) -> Result<Duration, Box<dyn std::error::Error>>
where
    F: FnMut() -> Result<Duration, Box<dyn std::error::Error>>,
{
    let mut best: Option<Duration> = None;
    for _ in 0..REPEATS {
        let d = f()?;
        best = Some(best.map_or(d, |b: Duration| b.min(d)));
    }
    best.ok_or_else(|| "REPEATS must be > 0".into())
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Cross-process IPC comprehensive comparison ===");
    println!("Payload: 8 bytes (u64). N = {N_ITEMS} round-trips per scenario.");
    println!();

    let mut results: Vec<(String, String, Duration)> = Vec::new();

    let shapes = [
        (RingShape::Spsc, "SubEtha pinned SPSC (MMF)"),
        (RingShape::Mpsc, "SubEtha pinned MPSC (MMF)"),
        (RingShape::Mpmc, "SubEtha pinned MPMC (MMF)"),
        (RingShape::Vyukov, "SubEtha pinned Vyukov MPMC (MMF)"),
    ];
    for (i, (shape, label)) in shapes.iter().enumerate() {
        println!("[{}/10] {label} (best of {REPEATS}) ...", i + 1);
        let d = best_of(|| bench_adaptive_pinned(*shape))?;
        println!("       {:?}", d);
        results.push((label.to_string(), "MMF (kernel-bypass)".to_string(), d));
    }

    println!("[5/10] TCP loopback (best of {REPEATS}) ...");
    let d = best_of(bench_tcp)?;
    println!("       {:?}", d);
    results.push(("TCP loopback".to_string(), "kernel TCP/IP stack".to_string(), d));

    println!("[6/10] UDP loopback (best of {REPEATS}) ...");
    let d = best_of(bench_udp)?;
    println!("       {:?}", d);
    results.push(("UDP loopback".to_string(), "kernel UDP/IP stack".to_string(), d));

    println!("[7/10] Local socket (interprocess crate) (best of {REPEATS}) ...");
    let d = best_of(bench_localsock)?;
    println!("       {:?}", d);
    results.push(("Named pipe (interprocess)".to_string(), "kernel named pipe / UDS".to_string(), d));

    println!("[8/10] Anonymous stdio pipe (best of {REPEATS}) ...");
    let d = best_of(bench_stdio)?;
    println!("       {:?}", d);
    results.push(("Anonymous stdio pipe".to_string(), "kernel anonymous pipe".to_string(), d));

    println!("[9/10] ipc-channel (Mozilla) (best of {REPEATS}) ...");
    match best_of(bench_ipcchan) {
        Ok(d) => {
            println!("       {:?}", d);
            results.push(("ipc-channel (Mozilla)".to_string(), "kernel + bincode serialize".to_string(), d));
        }
        Err(e) => println!("       SKIPPED: {e}"),
    }

    // iceoryx2 is excluded from the FreeBSD build: its platform
    // layer binds via bindgen there (libclang at build time), the
    // wrong cost for a dev-only bench peer whose comparison numbers
    // are produced on Linux.
    #[cfg(not(any(target_os = "freebsd", target_os = "macos")))]
    {
        println!("[10/10] iceoryx2 (Eclipse zero-copy) (best of {REPEATS}) ...");
        match best_of(bench_iceoryx2) {
            Ok(d) => {
                println!("       {:?}", d);
                results.push(("iceoryx2 (Eclipse)".to_string(), "shared memory zero-copy".to_string(), d));
            }
            Err(e) => println!("       SKIPPED: {e}"),
        }
    }
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    println!("[10/10] iceoryx2: not built on this platform (FreeBSD bindgen / macOS MSG_NOSIGNAL)");

    // ZeroMQ (ipc:// REQ/REP) - opt-in behind the `zmq-bench` feature, since
    // it links the C libzmq. Same round-trip ping-pong as the other contenders.
    #[cfg(feature = "zmq-bench")]
    {
        println!("[zmq] ZeroMQ (ipc:// REQ/REP) (best of {REPEATS}) ...");
        match best_of(bench_zmq) {
            Ok(d) => {
                println!("       {:?}", d);
                results.push(("ZeroMQ (ipc://)".to_string(), "C libzmq REQ/REP".to_string(), d));
            }
            Err(e) => println!("       SKIPPED: {e}"),
        }
    }

    println!();
    println!("=== LEADERBOARD ===");
    let mut sorted = results.clone();
    sorted.sort_by_key(|(_, _, d)| *d);
    let fastest = sorted[0].2.as_nanos() as f64 / N_ITEMS as f64;

    let mut bench_results: Vec<BenchResult> = Vec::new();
    println!("{:<35} {:<35} {:>12} {:>12} {:>10}",
        "Method", "Kind", "RT (ns)", "1-way (ns)", "vs fastest");
    println!("{:-<35} {:-<35} {:->12} {:->12} {:->10}", "", "", "", "", "");
    for (i, (name, kind, dur)) in sorted.iter().enumerate() {
        let rt_ns = dur.as_nanos() as f64 / N_ITEMS as f64;
        let one_way_ns = rt_ns / 2.0;
        let ratio = rt_ns / fastest;
        println!("{:<35} {:<35} {:>12.0} {:>12.0} {:>9.2}x",
            name, kind, rt_ns, one_way_ns, ratio);
        bench_results.push(BenchResult {
            name: name.clone(),
            transport_kind: kind.clone(),
            n_roundtrips: N_ITEMS,
            total_ns: dur.as_nanos() as u64,
            rt_ns,
            one_way_ns,
            rank: i + 1,
            vs_fastest: ratio,
        });
    }

    let report = ComparisonReport {
        machine: format!("{} ({})", std::env::consts::OS, std::env::consts::ARCH),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        payload_bytes: 8,
        results: bench_results.clone(),
    };

    // Write JSON to docs/
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let docs_dir = std::path::Path::new(manifest_dir)
        .parent().unwrap().parent().unwrap()
        .join("docs");
    std::fs::create_dir_all(&docs_dir).ok();
    let json_path = docs_dir.join("cross_process_ipc_results.json");
    let json_str = serde_json::to_string_pretty(&report)?;
    std::fs::write(&json_path, &json_str)?;
    println!();
    println!("[json] wrote {} ({} bytes)", json_path.display(), json_str.len());
    println!("[save] keep per host as docs/cross_process_ipc_results-<platform>.json");

    Ok(())
}

// ==========================================================================
// Bench functions
// ==========================================================================

/// Ping-pong through a pair of file-backed AdaptiveRings, both
/// sides morphed to `shape` and running on PINNED handles. This is
/// the adaptive system's production hot path: one Acquire-load
/// generation check amortized across the run, native-primitive
/// calls per op.
fn bench_adaptive_pinned(shape: RingShape) -> Result<Duration, Box<dyn std::error::Error>> {
    let p2c_prefix = tmp(&format!("{}_p2c", shape_arg(shape)));
    let c2p_prefix = tmp(&format!("{}_c2p", shape_arg(shape)));

    // SUBETHA_COMPARE_SHM=1 backs the rings with named anonymous
    // shared-memory sections (pagefile-backed on Windows, shmfs on
    // unix) instead of real files in the temp directory - the A/B
    // that isolates filesystem-backed-mapping effects from the
    // ring protocol itself.
    let (p2c, c2p) = if use_shm_backing() {
        (
            AdaptiveRing::create_shmfs(&shm_name(&p2c_prefix), 1, 1, 16384)
                .map_err(|e| format!("{e:?}"))?,
            AdaptiveRing::create_shmfs(&shm_name(&c2p_prefix), 1, 1, 16384)
                .map_err(|e| format!("{e:?}"))?,
        )
    } else {
        (
            AdaptiveRing::create(&p2c_prefix, 1, 1, 16384)
                .map_err(|e| format!("{e:?}"))?,
            AdaptiveRing::create(&c2p_prefix, 1, 1, 16384)
                .map_err(|e| format!("{e:?}"))?,
        )
    };
    p2c.morph_to(shape).map_err(|e| format!("{e:?}"))?;
    c2p.morph_to(shape).map_err(|e| format!("{e:?}"))?;
    let send_pin = p2c.pin_current_shape();
    let recv_pin = c2p.pin_current_shape();

    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-adaptive")
        .arg(shape_arg(shape))
        .arg(format!("{}", p2c_prefix.display()))
        .arg(format!("{}", c2p_prefix.display()))
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    thread::sleep(Duration::from_millis(100));

    let mut buf = [0u8; 64];
    let t0 = Instant::now();
    for i in 0..N_ITEMS {
        let payload = i.to_le_bytes();
        while !pinned_push(&send_pin, shape, &payload) { std::hint::spin_loop(); }
        loop {
            if pinned_pop_wait(&recv_pin, shape, &mut buf) {
                let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
                assert_eq!(v, i.wrapping_add(1));
                break;
            }
        }
    }
    let total = t0.elapsed();

    let sentinel = u64::MAX.to_le_bytes();
    while !pinned_push(&send_pin, shape, &sentinel) { std::hint::spin_loop(); }
    child.wait().ok();

    drop(p2c); drop(c2p);
    cleanup_adaptive_files(&p2c_prefix);
    cleanup_adaptive_files(&c2p_prefix);
    Ok(total)
}

fn bench_tcp() -> Result<Duration, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let port = listener.local_addr()?.port();
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-tcp").arg(port.to_string())
        .stdout(Stdio::null()).stderr(Stdio::inherit())
        .spawn()?;
    let (mut sock, _) = listener.accept()?;
    sock.set_nodelay(true)?;
    let t0 = Instant::now();
    let mut buf = [0u8; 8];
    for i in 0..N_ITEMS {
        sock.write_all(&i.to_le_bytes())?;
        sock.read_exact(&mut buf)?;
        assert_eq!(u64::from_le_bytes(buf), i.wrapping_add(1));
    }
    let total = t0.elapsed();
    drop(sock); child.wait().ok();
    Ok(total)
}

fn bench_udp() -> Result<Duration, Box<dyn std::error::Error>> {
    let parent_sock = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let parent_port = parent_sock.local_addr()?.port();
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-udp").arg(parent_port.to_string())
        .stdout(Stdio::piped()).stderr(Stdio::inherit())
        .spawn()?;
    // Read child's port from its stdout
    let mut child_stdout = BufReader::new(child.stdout.take().ok_or("no child stdout")?);
    let mut child_port_line = String::new();
    child_stdout.read_line(&mut child_port_line)?;
    let child_port: u16 = child_port_line.trim().parse()?;
    let child_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, child_port));
    let t0 = Instant::now();
    let mut buf = [0u8; 8];
    for i in 0..N_ITEMS {
        parent_sock.send_to(&i.to_le_bytes(), child_addr)?;
        let (n, _) = parent_sock.recv_from(&mut buf)?;
        assert_eq!(n, 8);
        assert_eq!(u64::from_le_bytes(buf), i.wrapping_add(1));
    }
    let total = t0.elapsed();
    parent_sock.send_to(&u64::MAX.to_le_bytes(), child_addr).ok();
    child.wait().ok();
    Ok(total)
}

fn bench_localsock() -> Result<Duration, Box<dyn std::error::Error>> {
    let sock_name = format!("subetha-cmp-{}", std::process::id());
    let name = sock_name.clone().to_ns_name::<GenericNamespaced>()?;
    let opts = ListenerOptions::new().name(name);
    let listener = opts.create_sync()?;
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-localsock").arg(&sock_name)
        .stdout(Stdio::null()).stderr(Stdio::inherit())
        .spawn()?;
    let mut conn = listener.accept()?;
    let t0 = Instant::now();
    let mut buf = [0u8; 8];
    for i in 0..N_ITEMS {
        conn.write_all(&i.to_le_bytes())?;
        conn.read_exact(&mut buf)?;
        assert_eq!(u64::from_le_bytes(buf), i.wrapping_add(1));
    }
    let total = t0.elapsed();
    drop(conn); child.wait().ok();
    Ok(total)
}

fn bench_stdio() -> Result<Duration, Box<dyn std::error::Error>> {
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-stdio")
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit())
        .spawn()?;
    let mut child_stdin = child.stdin.take().ok_or("no child stdin")?;
    let mut child_stdout = BufReader::new(child.stdout.take().ok_or("no child stdout")?);
    let t0 = Instant::now();
    let mut buf = String::new();
    for i in 0..N_ITEMS {
        writeln!(child_stdin, "{i}")?;
        child_stdin.flush()?;
        buf.clear();
        child_stdout.read_line(&mut buf)?;
        let echoed: u64 = buf.trim().parse()?;
        assert_eq!(echoed, i.wrapping_add(1));
    }
    let total = t0.elapsed();
    drop(child_stdin); child.wait().ok();
    Ok(total)
}

fn bench_ipcchan() -> Result<Duration, Box<dyn std::error::Error>> {
    use ipc_channel::ipc::{IpcOneShotServer, IpcSender, IpcReceiver};
    // Two one-shot servers: one for parent->child, one for child->reply tx
    let (server_a, server_a_name) = IpcOneShotServer::<(IpcSender<u64>, IpcReceiver<u64>)>::new()?;
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-ipcchan").arg(&server_a_name)
        .stdout(Stdio::null()).stderr(Stdio::inherit())
        .spawn()?;
    let (_, (tx_to_child, rx_from_child)): (_, (IpcSender<u64>, IpcReceiver<u64>)) =
        server_a.accept()?;
    let t0 = Instant::now();
    for i in 0..N_ITEMS {
        tx_to_child.send(i)?;
        let v = rx_from_child.recv()?;
        assert_eq!(v, i.wrapping_add(1));
    }
    let total = t0.elapsed();
    tx_to_child.send(u64::MAX).ok();
    child.wait().ok();
    Ok(total)
}

#[cfg(not(any(target_os = "freebsd", target_os = "macos")))]
fn bench_iceoryx2() -> Result<Duration, Box<dyn std::error::Error>> {
    use iceoryx2::prelude::*;
    let service_name = format!("subetha_cmp_{}", std::process::id());
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-iceoryx2").arg(&service_name)
        .stdout(Stdio::null()).stderr(Stdio::inherit())
        .spawn()?;

    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let p2c_service = node
        .service_builder(&format!("{service_name}_p2c").as_str().try_into()?)
        .publish_subscribe::<u64>()
        .open_or_create()?;
    let c2p_service = node
        .service_builder(&format!("{service_name}_c2p").as_str().try_into()?)
        .publish_subscribe::<u64>()
        .open_or_create()?;
    let publisher = p2c_service.publisher_builder().create()?;
    let subscriber = c2p_service.subscriber_builder().create()?;

    thread::sleep(Duration::from_millis(200)); // child has time to attach

    let t0 = Instant::now();
    for i in 0..N_ITEMS {
        publisher.send_copy(i)?;
        loop {
            if let Some(sample) = subscriber.receive()? {
                assert_eq!(*sample, i.wrapping_add(1));
                break;
            }
            std::hint::spin_loop();
        }
    }
    let total = t0.elapsed();
    publisher.send_copy(u64::MAX).ok();
    child.wait().ok();
    Ok(total)
}

// ZeroMQ (ipc:// REQ/REP), opt-in behind the `zmq-bench` feature (links the C
// libzmq). Parent is the REQ driver (binds), child is the REP echo (connects):
// the same round-trip ping-pong (send i, receive i+1) as every other contender.
#[cfg(feature = "zmq-bench")]
fn bench_zmq() -> Result<Duration, Box<dyn std::error::Error>> {
    let endpoint = format!("ipc:///tmp/subetha-zmq-{}.ipc", std::process::id());
    let ctx = zmq::Context::new();
    let req = ctx.socket(zmq::REQ)?;
    req.bind(&endpoint)?;
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-zmq")
        .arg(&endpoint)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let t0 = Instant::now();
    for i in 0..N_ITEMS {
        req.send(&i.to_le_bytes()[..], 0)?;
        let reply = req.recv_bytes(0)?;
        assert_eq!(u64::from_le_bytes(reply[..8].try_into()?), i.wrapping_add(1));
    }
    let total = t0.elapsed();
    // Sentinel stops the child; REQ/REP alternation needs the matching recv.
    req.send(&u64::MAX.to_le_bytes()[..], 0)?;
    drop(req.recv_bytes(0)?);
    child.wait().ok();
    Ok(total)
}

#[cfg(feature = "zmq-bench")]
fn run_child_zmq(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = zmq::Context::new();
    let rep = ctx.socket(zmq::REP)?;
    rep.connect(endpoint)?;
    loop {
        let msg = rep.recv_bytes(0)?;
        let i = u64::from_le_bytes(msg[..8].try_into()?);
        if i == u64::MAX {
            rep.send(&u64::MAX.to_le_bytes()[..], 0)?;
            break;
        }
        rep.send(&i.wrapping_add(1).to_le_bytes()[..], 0)?;
    }
    Ok(())
}

// ==========================================================================
// Child functions
// ==========================================================================

fn run_child_adaptive(
    shape_name: &str,
    p2c_prefix: &str,
    c2p_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    thread::sleep(Duration::from_millis(50));
    let shape = shape_from_name(shape_name);
    let (from_parent, to_parent) = if use_shm_backing() {
        (
            AdaptiveRing::create_shmfs(
                &shm_name(std::path::Path::new(p2c_prefix)), 1, 1, 16384,
            )
            .map_err(|e| format!("{e:?}"))?,
            AdaptiveRing::create_shmfs(
                &shm_name(std::path::Path::new(c2p_prefix)), 1, 1, 16384,
            )
            .map_err(|e| format!("{e:?}"))?,
        )
    } else {
        (
            AdaptiveRing::open(p2c_prefix, 1, 1, 16384)
                .map_err(|e| format!("{e:?}"))?,
            AdaptiveRing::open(c2p_prefix, 1, 1, 16384)
                .map_err(|e| format!("{e:?}"))?,
        )
    };
    // The shape tag is process-local; mirror the parent's morph so
    // this side's pinned handles run the same backing.
    from_parent.morph_to(shape).map_err(|e| format!("{e:?}"))?;
    to_parent.morph_to(shape).map_err(|e| format!("{e:?}"))?;
    let recv_pin = from_parent.pin_current_shape();
    let send_pin = to_parent.pin_current_shape();

    let mut buf = [0u8; 64];
    loop {
        if pinned_pop_wait(&recv_pin, shape, &mut buf) {
            let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
            if v == u64::MAX { break; }
            let reply = v.wrapping_add(1).to_le_bytes();
            while !pinned_push(&send_pin, shape, &reply) { std::hint::spin_loop(); }
        }
    }
    Ok(())
}

fn run_child_tcp(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut sock = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))?;
    sock.set_nodelay(true)?;
    let mut buf = [0u8; 8];
    while sock.read_exact(&mut buf).is_ok() {
        let v = u64::from_le_bytes(buf);
        if sock.write_all(&v.wrapping_add(1).to_le_bytes()).is_err() { break; }
    }
    Ok(())
}

fn run_child_udp(parent_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let child_sock = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let child_port = child_sock.local_addr()?.port();
    // Print port for parent to read
    println!("{child_port}");
    std::io::stdout().flush()?;
    let parent_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, parent_port));
    let mut buf = [0u8; 8];
    loop {
        let (n, _) = child_sock.recv_from(&mut buf)?;
        if n != 8 { break; }
        let v = u64::from_le_bytes(buf);
        if v == u64::MAX { break; }
        child_sock.send_to(&v.wrapping_add(1).to_le_bytes(), parent_addr)?;
    }
    Ok(())
}

fn run_child_localsock(sock_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let name = sock_name.to_string().to_ns_name::<GenericNamespaced>()?;
    let mut conn = LocalStream::connect(name)?;
    let mut buf = [0u8; 8];
    while conn.read_exact(&mut buf).is_ok() {
        let v = u64::from_le_bytes(buf);
        if conn.write_all(&v.wrapping_add(1).to_le_bytes()).is_err() { break; }
    }
    Ok(())
}

fn run_child_stdio() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut buf = String::new();
    let mut reader = stdin.lock();
    loop {
        buf.clear();
        if reader.read_line(&mut buf)? == 0 { break; }
        let v: u64 = buf.trim().parse()?;
        writeln!(stdout, "{}", v.wrapping_add(1))?;
        stdout.flush()?;
    }
    Ok(())
}

fn run_child_ipcchan(server_a_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use ipc_channel::ipc::{channel, IpcSender};
    let bootstrap: IpcSender<(IpcSender<u64>, ipc_channel::ipc::IpcReceiver<u64>)> =
        IpcSender::connect(server_a_name.to_string())?;
    let (tx_to_parent, rx_from_parent_inner) = channel::<u64>()?;
    let (tx_to_child, rx_from_parent) = channel::<u64>()?;
    // Send (tx_to_child, rx_from_child_side) so parent can talk to us
    bootstrap.send((tx_to_child, rx_from_parent_inner))?;
    loop {
        let v = rx_from_parent.recv()?;
        if v == u64::MAX { break; }
        tx_to_parent.send(v.wrapping_add(1))?;
    }
    Ok(())
}

#[cfg(not(any(target_os = "freebsd", target_os = "macos")))]
fn run_child_iceoryx2(service_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use iceoryx2::prelude::*;
    // iceoryx2 0.9 on Windows requires POSIX user-database lookup
    // (`/etc/passwd`) that does not exist; the node create fails. Exit
    // 0 silently so the parent's child.wait() does not surface the
    // child's stderr noise as a benchmark failure.
    let node = match NodeBuilder::new().create::<ipc::Service>() {
        Ok(n) => n,
        Err(_) => return Ok(()),
    };
    let p2c_service = node
        .service_builder(&format!("{service_name}_p2c").as_str().try_into()?)
        .publish_subscribe::<u64>()
        .open_or_create()?;
    let c2p_service = node
        .service_builder(&format!("{service_name}_c2p").as_str().try_into()?)
        .publish_subscribe::<u64>()
        .open_or_create()?;
    let publisher = c2p_service.publisher_builder().create()?;
    let subscriber = p2c_service.subscriber_builder().create()?;
    loop {
        if let Some(sample) = subscriber.receive()? {
            let v = *sample;
            if v == u64::MAX { break; }
            publisher.send_copy(v.wrapping_add(1))?;
        } else {
            std::hint::spin_loop();
        }
    }
    Ok(())
}
