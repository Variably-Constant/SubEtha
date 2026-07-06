//! Standalone `SO_TXTIME` (EDT) kernel-pacing probe over real UDP.
//!
//! Validates - in isolation, before touching the transport - whether stamping
//! each datagram's earliest-departure time and letting the `fq` qdisc release
//! it on schedule paces UDP cleanly *above* the bursty buffer cliff in this VM.
//! It A/Bs against a bursty blast (`--no-txtime`) at the same offered rate, so
//! the empirical delta is unambiguous. Full debug logging throughout.
//!
//! The sender needs the `fq` qdisc on its egress:
//!     sudo tc qdisc replace dev <iface> root fq
//!
//! Run:
//!     # receiver (VPS):
//!     txtime_pace_probe recv 0.0.0.0:9500 <n_packets>
//!     # sender (home VM):
//!     txtime_pace_probe send <vps_ip>:9500 <n_packets> <mbps> [--no-txtime]

#[cfg(target_os = "linux")]
fn main() {
    linux::main()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("txtime_pace_probe is Linux-only (SO_TXTIME + fq).");
}

#[cfg(target_os = "linux")]
mod linux {
    use std::net::UdpSocket;
    use std::os::unix::io::AsRawFd;
    use std::time::{Duration, Instant};

    /// Datagram size matching the RLC transport's symbol (1024 payload + header).
    const PKT: usize = 1041;
    /// SO_TXTIME / SCM_TXTIME share the value 61 on Linux.
    const SO_TXTIME: libc::c_int = 61;
    const SCM_TXTIME: libc::c_int = 61;
    const EOF: u64 = u64::MAX;

    /// The clock the txtimes + the SO_TXTIME socket use. `etf` is conventionally
    /// driven from CLOCK_TAI; `SUBETHA_TXTIME_CLOCK=monotonic` overrides it (for
    /// the fq path). The qdisc's clockid MUST match this.
    fn pace_clock() -> libc::clockid_t {
        match std::env::var("SUBETHA_TXTIME_CLOCK").as_deref() {
            Ok("monotonic") => libc::CLOCK_MONOTONIC,
            _ => libc::CLOCK_TAI,
        }
    }

    #[repr(C)]
    struct SockTxtime {
        clockid: libc::clockid_t,
        flags: u32,
    }

    fn mono_ns() -> u64 {
        let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
        unsafe {
            libc::clock_gettime(pace_clock(), &mut ts);
        }
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }

    pub fn main() {
        let a: Vec<String> = std::env::args().collect();
        match a.get(1).map(|s| s.as_str()) {
            Some("recv") => recv(&a[2], a[3].parse().expect("n")),
            Some("send") => send(
                &a[2],
                a[3].parse().expect("n"),
                a[4].parse().expect("mbps"),
                a.iter().any(|x| x == "--no-txtime"),
            ),
            _ => eprintln!("usage: txtime_pace_probe recv <bind> <n> | send <dst> <n> <mbps> [--no-txtime]"),
        }
    }

    fn recv(bind: &str, n: u64) {
        let s = UdpSocket::bind(bind).expect("bind");
        socket2::SockRef::from(&s)
            .set_recv_buffer_size(16 * 1024 * 1024)
            .ok();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        println!("[recv] bound {bind}, expecting {n} packets x {PKT}B");
        // EDT diagnostic: for a tiny run, print each packet's arrival time
        // relative to the first, so a fixed sender-side txtime gap shows up (or
        // not) as a receiver-side inter-arrival gap.
        let trace = n <= 32;
        let mut buf = [0u8; 2048];
        let (mut got, mut maxseq) = (0u64, 0u64);
        let mut t_first: Option<Instant> = None;
        let mut t_last = Instant::now();
        let mut last_log = Instant::now();
        loop {
            match s.recv_from(&mut buf) {
                Ok((sz, _)) if sz >= 8 => {
                    let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    if seq == EOF {
                        if got > 0 {
                            break;
                        }
                        continue;
                    }
                    let now = Instant::now();
                    let first = *t_first.get_or_insert(now);
                    t_last = now;
                    got += 1;
                    if seq > maxseq {
                        maxseq = seq;
                    }
                    if trace {
                        println!(
                            "[recv] seq={seq} arrived +{:.1}ms",
                            now.duration_since(first).as_secs_f64() * 1e3
                        );
                    }
                    if last_log.elapsed() >= Duration::from_millis(500) {
                        let secs = t_first.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
                        let mbit = got as f64 * PKT as f64 * 8.0 / secs.max(1e-6) / 1e6;
                        println!("[recv] t={secs:.1}s got={got} maxseq={maxseq} inst_mbit={mbit:.1}");
                        last_log = Instant::now();
                    }
                    if got >= n {
                        break;
                    }
                }
                Ok(_) => {}
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    if got > 0 {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[recv] error {e}");
                    break;
                }
            }
        }
        let secs = t_first
            .map(|t| t_last.duration_since(t).as_secs_f64().max(1e-6))
            .unwrap_or(1.0);
        let mbit = got as f64 * PKT as f64 * 8.0 / secs / 1e6;
        let loss = 1.0 - got as f64 / (maxseq + 1).max(1) as f64;
        println!(
            "[recv] RESULT got={got}/{n} maxseq={maxseq} loss={loss:.4} secs={secs:.3} delivered_mbit={mbit:.1}"
        );
    }

    fn send(dst: &str, n: u64, mbps: u64, no_txtime: bool) {
        let s = UdpSocket::bind("0.0.0.0:0").expect("bind");
        socket2::SockRef::from(&s)
            .set_send_buffer_size(16 * 1024 * 1024)
            .ok();
        s.connect(dst).expect("connect");
        let fd = s.as_raw_fd();
        if !no_txtime {
            let st = SockTxtime {
                clockid: pace_clock(),
                flags: 0,
            };
            let r = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    SO_TXTIME,
                    &st as *const SockTxtime as *const libc::c_void,
                    std::mem::size_of::<SockTxtime>() as libc::socklen_t,
                )
            };
            let clk = if pace_clock() == libc::CLOCK_TAI {
                "TAI"
            } else {
                "MONOTONIC"
            };
            println!(
                "[send] SO_TXTIME setsockopt -> {r} ({}), clock={clk}",
                if r == 0 { "ok" } else { "FAILED" }
            );
        }
        // EDT diagnostic: a fixed per-packet gap (ms) via txtime, with NO app
        // throttle, so any spacing at the receiver came purely from fq honoring
        // the departure time.
        let edt_gap_ms: u64 = std::env::var("SUBETHA_EDT_GAP_MS")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        let interval_ns = if edt_gap_ms > 0 {
            edt_gap_ms * 1_000_000
        } else {
            (PKT as u64 * 8 * 1_000_000_000) / (mbps * 1_000_000)
        };
        println!(
            "[send] dst={dst} n={n} target={mbps}Mbit interval={interval_ns}ns paced={} edt_gap_ms={edt_gap_ms}",
            !no_txtime
        );
        let mut pkt = vec![0u8; PKT];
        let base = mono_ns() + 5_000_000; // first departure 5ms out
        let t0 = Instant::now();
        let mut last_log = Instant::now();
        let mut sent = 0u64;
        for seq in 0..n {
            pkt[..8].copy_from_slice(&seq.to_le_bytes());
            if no_txtime {
                loop {
                    match s.send(&pkt) {
                        Ok(_) => {
                            sent += 1;
                            break;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                        Err(_) => break,
                    }
                }
            } else {
                let txtime = base + seq * interval_ns;
                send_at(fd, &pkt, txtime);
                sent += 1;
                // Coarse submission throttle so the fq queue (10000p) does not
                // overflow: if we are far ahead of the schedule, sleep most of
                // the gap. fq still does the precise per-packet release. The EDT
                // diagnostic skips this so fq alone determines the spacing.
                if edt_gap_ms == 0 {
                    let now = mono_ns();
                    if txtime > now + 8_000_000 {
                        std::thread::sleep(Duration::from_nanos(txtime - now - 4_000_000));
                    }
                }
            }
            if last_log.elapsed() >= Duration::from_millis(500) {
                println!("[send] t={:.1}s seq={seq}", t0.elapsed().as_secs_f64());
                last_log = Instant::now();
            }
        }
        pkt[..8].copy_from_slice(&EOF.to_le_bytes());
        for _ in 0..64 {
            s.send(&pkt).ok();
            std::thread::sleep(Duration::from_millis(2));
        }
        let secs = t0.elapsed().as_secs_f64();
        println!(
            "[send] done sent={sent}/{n} in {secs:.3}s offered_mbit={:.1}",
            n as f64 * PKT as f64 * 8.0 / secs / 1e6
        );
    }

    /// `sendmsg` one datagram carrying an `SCM_TXTIME` control message that
    /// names its earliest departure time (CLOCK_MONOTONIC ns).
    fn send_at(fd: libc::c_int, pkt: &[u8], txtime: u64) {
        let mut iov = libc::iovec {
            iov_base: pkt.as_ptr() as *mut libc::c_void,
            iov_len: pkt.len(),
        };
        let cmsg_space = unsafe { libc::CMSG_SPACE(8) } as usize;
        let mut cbuf = vec![0u8; cmsg_space];
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;
        unsafe {
            let c = libc::CMSG_FIRSTHDR(&msg);
            (*c).cmsg_level = libc::SOL_SOCKET;
            (*c).cmsg_type = SCM_TXTIME;
            (*c).cmsg_len = libc::CMSG_LEN(8) as _;
            std::ptr::copy_nonoverlapping(
                &txtime as *const u64 as *const u8,
                libc::CMSG_DATA(c),
                8,
            );
            loop {
                let r = libc::sendmsg(fd, &msg, 0);
                if r >= 0 {
                    break;
                }
                let e = std::io::Error::last_os_error();
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    continue;
                }
                eprintln!("[send] sendmsg error {e}");
                break;
            }
        }
    }
}
