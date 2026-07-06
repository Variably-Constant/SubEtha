//! Cross-process IPC throughput E2E demo: comprehensive comparison
//! across all canonical local-IPC mechanisms users actually pick:
//!
//! 1. **SubEtha Channel<u64>** (MMF, kernel-bypass on data path)
//! 2. **TCP loopback** (`std::net::TcpStream`, every byte through kernel)
//! 3. **Named pipe / Local socket** (`interprocess::local_socket`,
//!    Windows named pipe; Unix abstract UDS) - the canonical Rust
//!    cross-platform local-IPC abstraction
//! 4. **Anonymous stdio pipe** (`std::process::Command` stdin/stdout) -
//!    the simplest kernel-mediated local-IPC mechanism
//!
//! Parent + child process, 8-byte payload, N round-trips. Parent
//! times push -> wait-for-echo. Numbers are directly comparable:
//! same machine, same payload size, same N.
//!
//! Run with: `cargo run --release --example cross_process_throughput -p subetha-cxc`

use std::env;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use interprocess::local_socket::{
    prelude::*, GenericNamespaced, ListenerOptions, Stream as LocalStream,
};
use subetha_cxc::{Channel, MmfWorkloadShape};

const N_ITEMS: u64 = 10_000;

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    p.push(format!("subetha_cross_proc_{pid}_{nonce}_{name}.bin"));
    p
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("--child-mmf") => {
            return run_child_mmf(&args[2], &args[3]);
        }
        Some("--child-tcp") => {
            return run_child_tcp(args[2].parse()?);
        }
        Some("--child-localsock") => {
            return run_child_localsock(&args[2]);
        }
        Some("--child-stdio") => {
            return run_child_stdio();
        }
        _ => {}
    }
    run_parent()
}

#[derive(Debug, Clone)]
struct Result1 {
    name: &'static str,
    total: Duration,
    rt_ns: f64,
    one_way_ns: f64,
}

impl Result1 {
    fn new(name: &'static str, total: Duration) -> Self {
        let rt_ns = total.as_nanos() as f64 / N_ITEMS as f64;
        let one_way_ns = rt_ns / 2.0;
        Self { name, total, rt_ns, one_way_ns }
    }
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Cross-process IPC throughput comparison ===");
    println!("Payload: 8 bytes (u64). N = {N_ITEMS} round-trips per scenario.");
    println!();

    let mut results: Vec<Result1> = Vec::new();

    // === SubEtha Channel<u64> (MMF) ===
    println!("[1] SubEtha Channel<u64> (MMF, kernel-bypass on data path)");
    let r = bench_subetha_channel()?;
    println!("    {:?}", r);
    results.push(r);
    println!();

    // === TCP loopback ===
    println!("[2] TCP loopback (std::net::TcpStream)");
    let r = bench_tcp()?;
    println!("    {:?}", r);
    results.push(r);
    println!();

    // === Local socket / Named pipe via interprocess ===
    println!("[3] Named pipe / Local socket (interprocess crate)");
    let r = bench_localsock()?;
    println!("    {:?}", r);
    results.push(r);
    println!();

    // === Anonymous stdio pipe ===
    println!("[4] Anonymous stdio pipe (std::process::Command stdin/stdout)");
    let r = bench_stdio()?;
    println!("    {:?}", r);
    results.push(r);
    println!();

    // === Sorted leaderboard ===
    let mut sorted = results.clone();
    sorted.sort_by_key(|r| r.total);
    let fastest = sorted[0].rt_ns;

    println!("=== LEADERBOARD (fastest first) ===");
    println!(
        "{:<60}  {:>12}  {:>12}  {:>10}",
        "Method", "RT (ns)", "1-way (ns)", "vs fastest"
    );
    println!("{:-<60}  {:->12}  {:->12}  {:->10}", "", "", "", "");
    for r in &sorted {
        let ratio = r.rt_ns / fastest;
        println!(
            "{:<60}  {:>12.0}  {:>12.0}  {:>9.2}x",
            r.name, r.rt_ns, r.one_way_ns, ratio
        );
    }

    Ok(())
}

fn bench_subetha_channel() -> Result<Result1, Box<dyn std::error::Error>> {
    let p2c_path = tmp("p2c");
    let c2p_path = tmp("c2p");
    let shape = MmfWorkloadShape::StreamingMpmc {
        n_producers: 1,
        n_consumers: 1,
    };
    let parent_send: Channel<u64> = Channel::create(&p2c_path, shape, 16384)
        .map_err(|e| format!("create send: {e:?}"))?;
    let parent_recv: Channel<u64> = Channel::create(&c2p_path, shape, 16384)
        .map_err(|e| format!("create recv: {e:?}"))?;

    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-mmf")
        .arg(format!("{}", p2c_path.display()))
        .arg(format!("{}", c2p_path.display()))
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;

    thread::sleep(Duration::from_millis(100));

    let t0 = Instant::now();
    for i in 0..N_ITEMS {
        while parent_send.send(&i).is_err() {
            std::hint::spin_loop();
        }
        loop {
            match parent_recv.recv() {
                Ok(v) => {
                    assert_eq!(v, i.wrapping_add(1));
                    break;
                }
                Err(_) => std::hint::spin_loop(),
            }
        }
    }
    let total = t0.elapsed();
    parent_send.send(&u64::MAX).ok();
    child.wait().ok();

    drop(parent_send);
    drop(parent_recv);
    std::fs::remove_file(&p2c_path).ok();
    std::fs::remove_file(&c2p_path).ok();

    Ok(Result1::new("SubEtha Channel<u64> (MMF)", total))
}

fn bench_tcp() -> Result<Result1, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let port = listener.local_addr()?.port();

    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-tcp")
        .arg(port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
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
    drop(sock);
    child.wait().ok();

    Ok(Result1::new("TCP loopback (std::net::TcpStream)", total))
}

fn bench_localsock() -> Result<Result1, Box<dyn std::error::Error>> {
    let sock_name = format!("subetha-bench-{}", std::process::id());
    let name = sock_name.clone().to_ns_name::<GenericNamespaced>()?;
    let opts = ListenerOptions::new().name(name);
    let listener = opts.create_sync()?;

    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-localsock")
        .arg(&sock_name)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
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
    drop(conn);
    child.wait().ok();

    Ok(Result1::new("Local socket (interprocess; Windows named pipe / Unix UDS)", total))
}

fn bench_stdio() -> Result<Result1, Box<dyn std::error::Error>> {
    let self_exe = std::env::current_exe()?;
    let mut child = Command::new(self_exe)
        .arg("--child-stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
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
    drop(child_stdin);
    child.wait().ok();

    Ok(Result1::new("Anonymous stdio pipe (process stdin/stdout)", total))
}

fn run_child_mmf(send_path: &str, recv_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    thread::sleep(Duration::from_millis(50));
    let from_parent: Channel<u64> = Channel::open(send_path, 16384)
        .map_err(|e| format!("child from_parent open: {e:?}"))?;
    let to_parent: Channel<u64> = Channel::open(recv_path, 16384)
        .map_err(|e| format!("child to_parent open: {e:?}"))?;
    loop {
        match from_parent.recv() {
            Ok(v) => {
                if v == u64::MAX { break; }
                while to_parent.send(&v.wrapping_add(1)).is_err() {
                    std::hint::spin_loop();
                }
            }
            Err(_) => std::hint::spin_loop(),
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
        if sock.write_all(&v.wrapping_add(1).to_le_bytes()).is_err() {
            break;
        }
    }
    Ok(())
}

fn run_child_localsock(sock_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let name = sock_name.to_string().to_ns_name::<GenericNamespaced>()?;
    let mut conn = LocalStream::connect(name)?;
    let mut buf = [0u8; 8];
    while conn.read_exact(&mut buf).is_ok() {
        let v = u64::from_le_bytes(buf);
        if conn.write_all(&v.wrapping_add(1).to_le_bytes()).is_err() {
            break;
        }
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
