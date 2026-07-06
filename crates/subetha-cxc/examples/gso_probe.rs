//! Standalone UDP GSO (`UDP_SEGMENT`) evidence probe.
//!
//! Proves - before wiring GSO into the transport - how much the syscall +
//! stack-traversal cost drops when fixed-size datagrams are shipped as one
//! GSO super-buffer (up to 64 segments/syscall) vs one `send` per datagram.
//! A/Bs plain vs `--gso` at the same payload, counting sender syscalls and
//! measuring receiver goodput. Full debug logging.
//!
//! Run (loopback or cross-host):
//!     gso_probe recv 0.0.0.0:7406 <n>
//!     gso_probe send <dst> <n> <payload_bytes> [--gso]

#[cfg(target_os = "linux")]
fn main() {
    linux::main()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("gso_probe is Linux-only (UDP_SEGMENT).");
}

#[cfg(target_os = "linux")]
mod linux {
    use std::net::UdpSocket;
    use std::os::unix::io::AsRawFd;
    use std::time::Instant;

    /// UDP_SEGMENT (GSO) socket-option / cmsg type; SOL_UDP = 17.
    const UDP_SEGMENT: libc::c_int = 103;
    const SOL_UDP: libc::c_int = 17;
    /// Max segments the kernel slices from one GSO buffer.
    const MAX_SEGS: usize = 64;
    const EOF: u64 = u64::MAX;

    pub fn main() {
        let a: Vec<String> = std::env::args().collect();
        match a.get(1).map(|s| s.as_str()) {
            Some("recv") => recv(&a[2], a[3].parse().expect("n")),
            Some("send") => send(
                &a[2],
                a[3].parse().expect("n"),
                a[4].parse().expect("payload"),
                a.iter().any(|x| x == "--gso"),
            ),
            _ => eprintln!("usage: gso_probe recv <bind> <n> | send <dst> <n> <payload> [--gso]"),
        }
    }

    fn recv(bind: &str, n: u64) {
        let s = UdpSocket::bind(bind).expect("bind");
        socket2::SockRef::from(&s)
            .set_recv_buffer_size(32 * 1024 * 1024)
            .ok();
        s.set_read_timeout(Some(std::time::Duration::from_secs(3))).unwrap();
        println!("[recv] bound {bind}, expecting {n}");
        let mut buf = [0u8; 2048];
        let (mut got, mut bytes) = (0u64, 0u64);
        let mut t_first: Option<Instant> = None;
        let mut t_last = Instant::now();
        loop {
            match s.recv_from(&mut buf) {
                Ok((sz, _)) if sz >= 8 => {
                    if u64::from_le_bytes(buf[..8].try_into().unwrap()) == EOF {
                        if got > 0 {
                            break;
                        }
                        continue;
                    }
                    t_first.get_or_insert_with(Instant::now);
                    t_last = Instant::now();
                    got += 1;
                    bytes += sz as u64;
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
                    eprintln!("[recv] {e}");
                    break;
                }
            }
        }
        let secs = t_first
            .map(|t| t_last.duration_since(t).as_secs_f64().max(1e-6))
            .unwrap_or(1.0);
        println!(
            "[recv] RESULT got={got}/{n} bytes={bytes} secs={secs:.3} mbit={:.1} mpps={:.3}",
            bytes as f64 * 8.0 / secs / 1e6,
            got as f64 / secs / 1e6
        );
    }

    fn send(dst: &str, n: u64, payload: usize, gso: bool) {
        let s = UdpSocket::bind("0.0.0.0:0").expect("bind");
        socket2::SockRef::from(&s)
            .set_send_buffer_size(32 * 1024 * 1024)
            .ok();
        s.connect(dst).expect("connect");
        let fd = s.as_raw_fd();
        let mut pkt = vec![0u8; payload];
        let mut syscalls = 0u64;
        let t0 = Instant::now();
        if gso {
            // Pack up to `max_segs` payloads into one buffer, ship with one
            // sendmsg carrying a UDP_SEGMENT cmsg = payload; the kernel slices.
            // The super-buffer must fit a single IP datagram (<= 65535 B), so
            // cap segments at floor(65535/payload) as well as UDP's 64-seg max.
            let max_segs = (65535 / payload).min(MAX_SEGS).max(1);
            let mut big = vec![0u8; payload * max_segs];
            let mut seq = 0u64;
            while seq < n {
                let segs = ((n - seq) as usize).min(max_segs);
                for i in 0..segs {
                    big[i * payload..i * payload + 8].copy_from_slice(&seq.to_le_bytes());
                    seq += 1;
                }
                gso_send(fd, &big[..segs * payload], payload as u16);
                syscalls += 1;
            }
        } else {
            for seq in 0..n {
                pkt[..8].copy_from_slice(&seq.to_le_bytes());
                loop {
                    match s.send(&pkt) {
                        Ok(_) => {
                            syscalls += 1;
                            break;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                        Err(_) => break,
                    }
                }
            }
        }
        let secs = t0.elapsed().as_secs_f64();
        // EOF markers
        pkt[..8].copy_from_slice(&EOF.to_le_bytes());
        for _ in 0..64 {
            s.send(&pkt).ok();
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        println!(
            "[send] mode={} n={n} payload={payload} syscalls={syscalls} ({:.1} pkts/syscall) \
             secs={secs:.3} offered_mbit={:.1} send_mpps={:.3}",
            if gso { "GSO" } else { "plain" },
            n as f64 / syscalls.max(1) as f64,
            n as f64 * payload as f64 * 8.0 / secs / 1e6,
            n as f64 / secs / 1e6,
        );
    }

    /// One `sendmsg` shipping `buf` (k*gso_size bytes) as k datagrams of
    /// `gso_size` via a `UDP_SEGMENT` control message.
    fn gso_send(fd: libc::c_int, buf: &[u8], gso_size: u16) {
        let mut iov = libc::iovec {
            iov_base: buf.as_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        let cmsg_space = unsafe { libc::CMSG_SPACE(2) } as usize;
        let mut cbuf = vec![0u8; cmsg_space];
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cbuf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;
        unsafe {
            let c = libc::CMSG_FIRSTHDR(&msg);
            (*c).cmsg_level = SOL_UDP;
            (*c).cmsg_type = UDP_SEGMENT;
            (*c).cmsg_len = libc::CMSG_LEN(2) as _;
            std::ptr::copy_nonoverlapping(
                &gso_size as *const u16 as *const u8,
                libc::CMSG_DATA(c),
                2,
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
                eprintln!("[send] sendmsg(GSO) {e}");
                break;
            }
        }
    }
}
