//! Cross-process E2E for live handle handoff ([`subetha_cxc::fd_handoff`]).
//!
//! The sender process creates an ANONYMOUS shared region with NO name /
//! path (a Linux memfd, a FreeBSD `SHM_ANON` object, a Windows
//! pagefile-backed file mapping), maps it, and writes a marker; it then
//! hands the live handle to a separate receiver process - via SCM_RIGHTS
//! over a UNIX socket on every Unix, via `DuplicateHandle` into the
//! target + an 8-byte value over a TCP loopback stream on Windows. The
//! receiver maps the handed-over handle
//! and reads the marker, which it could only reach through the passed
//! handle since the region has no name to open. The sender then writes a
//! SECOND marker AFTER the handoff and the receiver reads it too,
//! proving both processes hold the SAME live kernel object, not a copy.
//!
//! Only the handle-transport syscall differs per OS; the anonymous-
//! region + two-marker liveness proof is shared.
//!
//! Run:
//!     cargo run --release --example fd_handoff_xproc

#[cfg(unix)]
fn main() {
    use std::io::{Read, Write};
    use std::os::unix::io::RawFd;
    use subetha_cxc::fd_handoff::{accept_one, connect, recv_fd, send_fd};

    const REGION: usize = 4096;
    const MARK1: &[u8] = b"FD-PASSED-ANON-REGION";
    const OFF2: usize = 256;
    const MARK2: &[u8] = b"WRITTEN-AFTER-HANDOFF";

    // Map a fd MAP_SHARED and return the base pointer.
    fn map_shared(fd: RawFd) -> *mut u8 {
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                REGION,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        assert!(p != libc::MAP_FAILED, "mmap: {}", std::io::Error::last_os_error());
        p as *mut u8
    }

    // Create an ANONYMOUS (no-name) shared fd of REGION bytes. Only this
    // syscall is per-OS; the SCM_RIGHTS handoff + two-marker liveness proof
    // below are shared across every Unix.
    fn create_anon_fd() -> RawFd {
        #[cfg(target_os = "linux")]
        let fd = {
            // memfd: a nameless file living only in RAM.
            let name = std::ffi::CString::new("subetha-fd-handoff").unwrap();
            let fd = unsafe { libc::memfd_create(name.as_ptr(), 0) };
            assert!(fd >= 0, "memfd_create: {}", std::io::Error::last_os_error());
            fd
        };
        #[cfg(target_os = "freebsd")]
        let fd = {
            // SHM_ANON: an anonymous shared object with no name, explicitly
            // passable via sendmsg(2)/SCM_RIGHTS - the FreeBSD analogue of a
            // Linux memfd.
            let fd =
                unsafe { libc::shm_open(libc::SHM_ANON, libc::O_RDWR | libc::O_CREAT, 0o600) };
            assert!(fd >= 0, "shm_open(SHM_ANON): {}", std::io::Error::last_os_error());
            fd
        };
        #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
        let fd = {
            // Generic POSIX (e.g. macOS): a uniquely-named shm object,
            // unlinked immediately so it is nameless yet live - the fd alone
            // reaches it.
            let nm = std::ffi::CString::new(format!("/subetha_fdh_{}", std::process::id()))
                .unwrap();
            let fd = unsafe {
                libc::shm_open(nm.as_ptr(), libc::O_RDWR | libc::O_CREAT | libc::O_EXCL, 0o600)
            };
            assert!(fd >= 0, "shm_open: {}", std::io::Error::last_os_error());
            unsafe { libc::shm_unlink(nm.as_ptr()) };
            fd
        };
        assert_eq!(unsafe { libc::ftruncate(fd, REGION as libc::off_t) }, 0, "ftruncate");
        fd
    }

    let args: Vec<String> = std::env::args().collect();
    let uds = std::env::temp_dir().join(format!("fdhandoff_xproc_{}.sock", std::process::id()));

    // --- receiver role: connect, recv the fd, mmap it, verify both marks ---
    if args.get(1).map(String::as_str) == Some("recv") {
        let uds_path = &args[2];
        let mut stream = connect(uds_path).expect("connect");
        let fd = recv_fd(&stream).expect("recv_fd");
        // Anonymous memfd: there is NO path; reaching the data proves the
        // duplicated fd is what gives access.
        let base = map_shared(fd);
        let got1 = unsafe { std::slice::from_raw_parts(base, MARK1.len()) };
        assert_eq!(got1, MARK1, "receiver did not see marker1 via passed fd");
        // Tell the sender we are mapped; it then writes marker2 post-handoff.
        stream.write_all(b"1").expect("ack1");
        let mut go = [0u8; 1];
        stream.read_exact(&mut go).expect("await go");
        let got2 = unsafe { std::slice::from_raw_parts(base.add(OFF2), MARK2.len()) };
        assert_eq!(got2, MARK2, "receiver did not see the POST-HANDOFF write");
        stream.write_all(b"2").expect("ack2");
        unsafe { libc::munmap(base as *mut libc::c_void, REGION) };
        unsafe { libc::close(fd) };
        return;
    }

    // --- sender role (parent): anon fd, mmap, write, hand off, prove live ---
    println!("=== SCM_RIGHTS fd-handoff CROSS-PROCESS E2E (anonymous shared fd) ===");
    let anon_fd = create_anon_fd();
    let base = map_shared(anon_fd);
    unsafe { std::ptr::copy_nonoverlapping(MARK1.as_ptr(), base, MARK1.len()) };
    println!("[sender] anon fd={anon_fd} mapped, marker1 written (no path - anonymous)");

    let uds_str = uds.to_string_lossy().to_string();
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = std::process::Command::new(exe)
        .args(["recv", &uds_str])
        .spawn()
        .expect("spawn receiver");
    println!("[sender] spawned receiver pid={}", child.id());

    let mut stream = accept_one(&uds).expect("accept");
    send_fd(&stream, anon_fd).expect("send_fd");
    println!("[sender] handed the anon fd to receiver via SCM_RIGHTS");

    // Wait for the receiver to confirm it mapped + saw marker1, THEN write
    // marker2 so its visibility proves a live shared object.
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).expect("ack1");
    unsafe { std::ptr::copy_nonoverlapping(MARK2.as_ptr(), base.add(OFF2), MARK2.len()) };
    println!("[sender] wrote marker2 AFTER the handoff");
    stream.write_all(b"g").expect("go");
    stream.read_exact(&mut ack).expect("ack2");

    let status = child.wait().expect("reap receiver");
    unsafe { libc::munmap(base as *mut libc::c_void, REGION) };
    unsafe { libc::close(anon_fd) };
    std::fs::remove_file(&uds).ok();

    println!();
    println!("  receiver exit: {status}");
    assert!(status.success(), "receiver process failed");
    println!("  integrity:     PASS");
    println!("    an independent process read an ANONYMOUS fd (no path) only");
    println!("    reachable through the SCM_RIGHTS-passed fd, AND saw a write made");
    println!("    AFTER the handoff - the same live kernel object, zero-copy.");
}

#[cfg(windows)]
fn main() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use subetha_cxc::fd_handoff::{
        close_handle, create_anon_mapping, map_handle, recv_handle, send_handle, unmap,
    };

    const REGION: usize = 4096;
    const MARK1: &[u8] = b"HANDLE-PASSED-ANON-REGION";
    const OFF2: usize = 256;
    const MARK2: &[u8] = b"WRITTEN-AFTER-HANDOFF";

    let args: Vec<String> = std::env::args().collect();

    // --- receiver role: connect, recv the handle, map it, verify marks ---
    if args.get(1).map(String::as_str) == Some("recv") {
        let port: u16 = args[2].parse().expect("port");
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        let handle = recv_handle(&mut stream).expect("recv_handle");
        // Anonymous mapping: there is NO name; reaching the data proves
        // the duplicated handle is what gives access.
        let base = map_handle(handle, REGION).expect("map passed handle");
        let got1 = unsafe { std::slice::from_raw_parts(base, MARK1.len()) };
        assert_eq!(got1, MARK1, "receiver did not see marker1 via passed handle");
        // Tell the sender we mapped; it then writes marker2 post-handoff.
        stream.write_all(b"1").expect("ack1");
        let mut go = [0u8; 1];
        stream.read_exact(&mut go).expect("await go");
        let got2 = unsafe { std::slice::from_raw_parts(base.add(OFF2), MARK2.len()) };
        assert_eq!(got2, MARK2, "receiver did not see the POST-HANDOFF write");
        stream.write_all(b"2").expect("ack2");
        unmap(base);
        close_handle(handle);
        return;
    }

    // --- sender role (parent): anon mapping, write, hand off, prove live ---
    println!("=== DuplicateHandle handoff CROSS-PROCESS E2E (anonymous mapping) ===");
    let handle = create_anon_mapping(REGION).expect("create_anon_mapping");
    let base = map_handle(handle, REGION).expect("map");
    unsafe { std::ptr::copy_nonoverlapping(MARK1.as_ptr(), base, MARK1.len()) };
    println!("[sender] anon mapping created, marker1 written (no name - anonymous)");

    // TCP loopback is just the byte channel that ferries the handle value
    // + the handshake; the handle itself rides DuplicateHandle, not TCP.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let exe = std::env::current_exe().expect("current_exe");
    let mut child = std::process::Command::new(exe)
        .args(["recv", &port.to_string()])
        .spawn()
        .expect("spawn receiver");
    println!("[sender] spawned receiver pid={}", child.id());

    let (mut stream, _) = listener.accept().expect("accept");
    send_handle(&mut stream, handle, child.id()).expect("send_handle");
    println!("[sender] duplicated the mapping handle into the receiver");

    // Wait for the receiver to confirm it mapped + saw marker1, THEN write
    // marker2 so its visibility proves a live shared object.
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).expect("ack1");
    unsafe { std::ptr::copy_nonoverlapping(MARK2.as_ptr(), base.add(OFF2), MARK2.len()) };
    println!("[sender] wrote marker2 AFTER the handoff");
    stream.write_all(b"g").expect("go");
    stream.read_exact(&mut ack).expect("ack2");

    let status = child.wait().expect("reap receiver");
    unmap(base);
    close_handle(handle);

    println!();
    println!("  receiver exit: {status}");
    assert!(status.success(), "receiver process failed");
    println!("  integrity:     PASS");
    println!("    an independent process mapped an ANONYMOUS region (no name) only");
    println!("    reachable through the DuplicateHandle-passed handle, AND saw a write");
    println!("    made AFTER the handoff - the same live kernel object, zero-copy.");
}

#[cfg(not(any(unix, windows)))]
fn main() {
    eprintln!("fd_handoff_xproc needs SCM_RIGHTS (Unix) or DuplicateHandle (Windows).");
}
