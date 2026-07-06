//! Linux TCP socket tuning for the bridge data paths. Each knob is
//! a direct `setsockopt`; all are advisory (failures are ignored -
//! the socket works untuned) and the whole module is a no-op off
//! Linux.
//!
//! | knob | why |
//! |---|---|
//! | `TCP_QUICKACK` | the bridges' request/echo traffic is ACK-clocked; delayed ACKs add up to 40 ms per quiet round |
//! | `TCP_NOTSENT_LOWAT` | bounds unsent bytes queued below the egress batch size, keeping write-side latency flat under backlog |
//! | `SO_BUSY_POLL` | kernel busy-polls the NIC queue for the configured microseconds before sleeping; opt-in via `SUBETHA_BUSY_POLL_US` because it trades CPU for latency and needs NIC/NAPI support |

/// Apply the bridge tuning set to a connected TCP socket.
#[cfg(target_os = "linux")]
pub fn tune_tcp_socket(fd: std::os::unix::io::RawFd) {
    unsafe {
        let one: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_QUICKACK,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        // Keep at most one egress batch unsent in the kernel.
        let lowat: libc::c_int = 16 * 1024;
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NOTSENT_LOWAT,
            &lowat as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        if let Some(us) = std::env::var("SUBETHA_BUSY_POLL_US")
            .ok()
            .and_then(|v| v.parse::<libc::c_int>().ok())
        {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                &us as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

/// No-op on platforms without these knobs.
#[cfg(not(target_os = "linux"))]
pub fn tune_tcp_socket<T>(_fd: T) {}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(target_os = "linux")]
    fn tuning_a_live_socket_does_not_break_it() {
        use std::io::{Read, Write};
        use std::os::unix::io::AsRawFd;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let mut client = std::net::TcpStream::connect(addr).expect("connect");
        let (mut server, _) = listener.accept().expect("accept");
        super::tune_tcp_socket(client.as_raw_fd());
        super::tune_tcp_socket(server.as_raw_fd());
        client.write_all(b"ping").expect("write");
        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).expect("read");
        assert_eq!(&buf, b"ping");
    }
}
