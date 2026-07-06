//! `Locale::Wire`: userspace NIC access (cross-platform).
//!
//! The lowest byte locale, whose "other side" is the NIC hardware: bytes
//! flow producer -> TX ring -> NIC, or NIC -> RX ring -> consumer, with
//! the socket stack bypassed. Both OSes use the same architecture - a
//! UMEM frame area plus FILL / COMPLETION / RX / TX rings shared with the
//! kernel, and an XDP redirect program steering matching ingress frames
//! into the RX ring. Only the OS plumbing is gated; the [`WireSocket`]
//! `send_frame` / `recv_frame` surface is shared:
//!
//! - Linux (`#[cfg(target_os = "linux")]`): AF_XDP via the mainline
//!   libxdp datapath (xsk-rs), SKB (generic) mode, libxdp's redirect
//!   program. Needs root / `CAP_NET_RAW` + `CAP_BPF`.
//! - Windows (`#[cfg(windows)]`): XDP-for-Windows - the `xdpapi.dll`
//!   AF_XDP-equivalent (`XskCreate` / `XskBind` / `XskActivate` / the
//!   UMEM + 4-ring `XskGetSockopt(RING_INFO)` layout) plus an
//!   `XdpCreateProgram` redirect rule. Generic mode runs on any NIC;
//!   needs the signed XDP runtime driver installed + admin.
//!
//! # Platform & privileges
//!
//! - FreeBSD (`#[cfg(target_os = "freebsd")]`): netmap via `libnetmap`
//!   (`nmport_open` / `nmport_inject`) for open + TX, with the RX path
//!   reading the mmap'd netmap rings directly. The kernel networking
//!   stack is bypassed; raw Ethernet frames ride the netmap rings.
//! - macOS (`#[cfg(target_os = "macos")]`): BPF (`/dev/bpf*`). Darwin has
//!   neither AF_XDP nor netmap, so the raw-packet path is BPF: `bind`
//!   opens a free BPF node and attaches it to the NIC (`BIOCSETIF` +
//!   `BIOCIMMEDIATE`), `send_frame` is a `write`, and `recv_frame` is a
//!   `read` that returns one-or-more `bpf_hdr`-prefixed frames per call.
//!
//! Gated behind `cfg(any(target_os = "linux", windows, target_os =
//! "freebsd", target_os = "macos"))` AND the `wire-locale` Cargo feature
//! (which pulls the libxdp datapath dependency on Linux; the Windows side
//! dynamically loads the in-box `xdpapi.dll`, FreeBSD links the in-base
//! `libnetmap`, and macOS uses in-libc BPF - no extra crate on any of the
//! three), so callers without NIC access pay nothing.

#![cfg(all(
    any(target_os = "linux", windows, target_os = "freebsd", target_os = "macos"),
    feature = "wire-locale"
))]

#[cfg(target_os = "linux")]
use std::io::{self, Write};

#[cfg(target_os = "linux")]
use xsk_rs::{
    config::{BindFlags, Interface, SocketConfig, UmemConfig, XdpFlags},
    CompQueue, FillQueue, FrameDesc, RxQueue, Socket, TxQueue, Umem,
};

/// AF_XDP socket constant (linux/if_xdp.h), exposed for callers that
/// want to inspect the family.
#[cfg(target_os = "linux")]
pub const AF_XDP: libc::c_int = 44;

/// Frames reserved for the RX (fill) side of the datapath.
#[cfg(target_os = "linux")]
const RX_FRAMES: usize = 16;
/// Frames reserved for the TX side of the datapath.
#[cfg(target_os = "linux")]
const TX_FRAMES: usize = 16;

/// A `Locale::Wire` endpoint: an AF_XDP socket bound to one NIC (or
/// `veth`) queue, with the UMEM + four rings + libxdp redirect program
/// wired up. Transmits and receives raw Ethernet frames with the socket
/// stack bypassed.
#[cfg(target_os = "linux")]
pub struct WireSocket {
    umem: Umem,
    tx_q: TxQueue,
    rx_q: RxQueue,
    fq: FillQueue,
    cq: CompQueue,
    rx_descs: Vec<FrameDesc>,
    /// TX frames available for a new transmit. A frame leaves this pool
    /// when produced to the TX ring and returns once the kernel reports
    /// its completion - so a frame is never reused while still in flight.
    tx_free: Vec<FrameDesc>,
    /// Scratch buffer the completion queue drains into.
    cq_scratch: Vec<FrameDesc>,
}

#[cfg(target_os = "linux")]
impl WireSocket {
    /// Bind an AF_XDP socket to `if_name` queue `queue_id` and stand up
    /// the full datapath. libxdp attaches its default redirect program so
    /// RX frames land in this socket's ring. Needs root / `CAP_BPF`.
    pub fn bind(if_name: &str, queue_id: u32) -> io::Result<Self> {
        let iface: Interface = if_name
            .parse()
            .map_err(|e| io::Error::other(format!("interface '{if_name}': {e:?}")))?;

        let frame_count = std::num::NonZeroU32::new((RX_FRAMES + TX_FRAMES) as u32).unwrap();
        let (umem, mut descs) = Umem::new(UmemConfig::default(), frame_count, false)
            .map_err(|e| io::Error::other(format!("UMEM: {e}")))?;

        // First RX_FRAMES go to the RX/fill side, the rest to TX.
        let tx_descs: Vec<FrameDesc> = descs.split_off(RX_FRAMES);
        let rx_descs: Vec<FrameDesc> = descs;

        // SKB (generic) mode + copy: runs on veth and on any NIC without
        // native-XDP support; the bypass datapath is identical.
        let config = SocketConfig::builder()
            .xdp_flags(XdpFlags::XDP_FLAGS_SKB_MODE)
            .bind_flags(BindFlags::XDP_COPY)
            .build();

        let (tx_q, rx_q, fq_cq) = unsafe { Socket::new(config, &umem, &iface, queue_id) }
            .map_err(|e| io::Error::other(format!("AF_XDP socket on {if_name}: {e}")))?;
        let (mut fq, cq) = fq_cq
            .ok_or_else(|| io::Error::other("socket created without fill/comp queue"))?;

        // Hand the RX frames to the kernel so it can deliver packets.
        unsafe {
            fq.produce(&rx_descs);
        }

        // All TX frames start free; the completion scratch is sized to
        // hold every TX frame so one drain reclaims them all.
        let cq_scratch = tx_descs.clone();
        let tx_free = tx_descs;
        Ok(Self {
            umem,
            tx_q,
            rx_q,
            fq,
            cq,
            rx_descs,
            tx_free,
            cq_scratch,
        })
    }

    /// Drain finished TX frames from the completion ring back into the
    /// free pool so they can be reused.
    fn reap_tx(&mut self) {
        loop {
            let n = unsafe { self.cq.consume(&mut self.cq_scratch) };
            if n == 0 {
                break;
            }
            self.tx_free.extend_from_slice(&self.cq_scratch[..n]);
        }
    }

    /// Transmit one raw Ethernet frame through the AF_XDP TX ring,
    /// bypassing the socket send path. Reclaims completed TX frames first
    /// so a frame is never reused while the kernel still owns it.
    pub fn send_frame(&mut self, data: &[u8]) -> io::Result<()> {
        // Reclaim completed frames, then wait for one if the pool is
        // momentarily empty (kicking the kernel to flush completions).
        self.reap_tx();
        let mut spins = 0u32;
        while self.tx_free.is_empty() {
            if self.tx_q.needs_wakeup() {
                self.tx_q.wakeup()?;
            }
            self.reap_tx();
            spins += 1;
            if spins > 1_000_000 {
                return Err(io::Error::other("TX completion ring stalled"));
            }
            std::hint::spin_loop();
        }

        // FrameDesc is Copy, so this leaves a usable copy in `desc`.
        let mut desc = self.tx_free.pop().unwrap();
        unsafe {
            self.umem.data_mut(&mut desc).cursor().write_all(data)?;
        }
        let descs = [desc];
        let produced = unsafe { self.tx_q.produce_and_wakeup(&descs)? };
        if produced == 0 {
            // TX ring momentarily full; return the frame to the pool.
            self.tx_free.push(desc);
            return Err(io::Error::other("TX ring full"));
        }
        Ok(())
    }

    /// Receive one raw Ethernet frame from the AF_XDP RX ring into `out`,
    /// bypassing the socket recv path. Blocks up to `timeout_ms`; returns
    /// `Ok(0)` on timeout. Recycles consumed frames back to the FILL ring.
    pub fn recv_frame(&mut self, out: &mut [u8], timeout_ms: i32) -> io::Result<usize> {
        let n = unsafe { self.rx_q.poll_and_consume(&mut self.rx_descs, timeout_ms)? };
        if n == 0 {
            return Ok(0);
        }
        let len = {
            let data = unsafe { self.umem.data(&self.rx_descs[0]) };
            let contents = data.contents();
            let len = contents.len().min(out.len());
            out[..len].copy_from_slice(&contents[..len]);
            len
        };
        // Return the consumed frames to the kernel for re-use.
        unsafe {
            self.fq.produce(&self.rx_descs[..n]);
        }
        Ok(len)
    }
}

// ---------------------------------------------------------------------
// Windows: XDP-for-Windows (xdpapi.dll), dynamically loaded so the crate
// builds without the SDK and degrades to Err when the runtime driver is
// absent. AF_XDP-equivalent: XskCreate/Bind/Activate + UMEM + 4 rings via
// XskGetSockopt(RING_INFO) + an XdpCreateProgram redirect rule.
// ---------------------------------------------------------------------

#[cfg(windows)]
mod windows_impl {
    use std::io;
    use std::sync::atomic::{AtomicU32, Ordering};
    use windows_sys::Win32::Foundation::{CloseHandle, FreeLibrary, HANDLE, HMODULE};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetAdaptersAddresses, IP_ADAPTER_ADDRESSES_LH,
    };
    use windows_sys::Win32::System::LibraryLoader::{GetProcAddress, LoadLibraryW};
    use windows_sys::Win32::System::Memory::{
        VirtualAlloc, VirtualFree, MEM_COMMIT, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE,
    };

    type XdpStatus = i32;
    fn check(status: XdpStatus, what: &str) -> io::Result<()> {
        if status < 0 {
            Err(io::Error::other(format!("{what} failed: {status:#x}")))
        } else {
            Ok(())
        }
    }

    // --- FFI structs (repr C, exact layout) ---

    #[repr(C)]
    struct XskUmemReg {
        total_size: u64,
        chunk_size: u32,
        headroom: u32,
        address: *mut core::ffi::c_void,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XskBufferDescriptor {
        address: u64,
        length: u32,
        reserved: u32,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XskRingInfo {
        ring: *mut u8,
        descriptors_offset: u32,
        producer_index_offset: u32,
        consumer_index_offset: u32,
        flags_offset: u32,
        size: u32,
        element_stride: u32,
        reserved: u32,
    }

    #[repr(C)]
    struct XskRingInfoSet {
        fill: XskRingInfo,
        completion: XskRingInfo,
        rx: XskRingInfo,
        tx: XskRingInfo,
    }

    #[repr(C)]
    struct XdpHookId {
        layer: i32,
        direction: i32,
        sublayer: i32,
    }

    // XDP_RULE and its nested match-pattern union, laid out field-for-field
    // so repr(C) reproduces the C ABI exactly. Only MATCH_UDP_DST + REDIRECT
    // are used, but the whole shape must match for the offsets to be right.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct In6Addr {
        word: [u16; 8],
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    union XdpInetAddr {
        ipv4: [u8; 4],
        ipv6: In6Addr,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XdpIpAddressMask {
        mask: XdpInetAddr,
        address: XdpInetAddr,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XdpTuple {
        source_address: XdpInetAddr,
        destination_address: XdpInetAddr,
        source_port: u16,
        destination_port: u16,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XdpQuicFlow {
        udp_port: u16,
        cid_length: u8,
        cid_offset: u8,
        cid_data: [u8; 20],
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XdpPortSet {
        port_set: *const u8,
        reserved: *mut core::ffi::c_void,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XdpIpPortSet {
        address: XdpInetAddr,
        port_set: XdpPortSet,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    union XdpMatchPattern {
        port: u16,
        ip_mask: XdpIpAddressMask,
        tuple: XdpTuple,
        quic_flow: XdpQuicFlow,
        port_set: XdpPortSet,
        ip_port_set: XdpIpPortSet,
        next_header: u8,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XdpRedirectParams {
        target_type: i32,
        target: HANDLE,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct XdpEbpfParams {
        target: HANDLE,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    union XdpRuleUnion {
        redirect: XdpRedirectParams,
        ebpf: XdpEbpfParams,
    }
    #[repr(C)]
    struct XdpRule {
        match_type: i32,
        pattern: XdpMatchPattern,
        action: i32,
        u: XdpRuleUnion,
    }

    // --- constants (afxdp.h / program.h / hookid.h) ---
    const XSK_SOCKOPT_UMEM_REG: u32 = 1;
    const XSK_SOCKOPT_RX_RING_SIZE: u32 = 2;
    const XSK_SOCKOPT_RX_FILL_RING_SIZE: u32 = 3;
    const XSK_SOCKOPT_TX_RING_SIZE: u32 = 4;
    const XSK_SOCKOPT_TX_COMPLETION_RING_SIZE: u32 = 5;
    const XSK_SOCKOPT_RING_INFO: u32 = 6;
    const XSK_BIND_FLAG_RX: u32 = 0x1;
    const XSK_BIND_FLAG_TX: u32 = 0x2;
    const XSK_BIND_FLAG_GENERIC: u32 = 0x4;
    const XSK_NOTIFY_FLAG_POKE_TX: u32 = 0x2;
    const XSK_NOTIFY_FLAG_WAIT_RX: u32 = 0x4;
    const XSK_ACTIVATE_FLAG_NONE: u32 = 0;
    const XDP_HOOK_L2: i32 = 0;
    const XDP_HOOK_RX: i32 = 0;
    const XDP_HOOK_INSPECT: i32 = 0;
    const XDP_MATCH_UDP_DST: i32 = 2;
    const XDP_PROGRAM_ACTION_REDIRECT: i32 = 2;
    const XDP_REDIRECT_TARGET_TYPE_XSK: i32 = 0;
    const XDP_CREATE_PROGRAM_FLAG_GENERIC: u32 = 0x1;

    // --- xdpapi.dll function pointers ---
    type FnXskCreate = unsafe extern "system" fn(*mut HANDLE) -> XdpStatus;
    type FnXskBind = unsafe extern "system" fn(HANDLE, u32, u32, u32) -> XdpStatus;
    type FnXskActivate = unsafe extern "system" fn(HANDLE, u32) -> XdpStatus;
    type FnXskSetSockopt =
        unsafe extern "system" fn(HANDLE, u32, *const core::ffi::c_void, u32) -> XdpStatus;
    type FnXskGetSockopt =
        unsafe extern "system" fn(HANDLE, u32, *mut core::ffi::c_void, *mut u32) -> XdpStatus;
    type FnXskNotifySocket = unsafe extern "system" fn(HANDLE, u32, u32, *mut u32) -> XdpStatus;
    type FnXdpCreateProgram = unsafe extern "system" fn(
        u32,
        *const XdpHookId,
        u32,
        u32,
        *const XdpRule,
        u32,
        *mut HANDLE,
    ) -> XdpStatus;

    /// Dynamically-loaded handle to `xdpapi.dll`. Loading fails (returns
    /// Err) when the XDP runtime driver is not installed - the runtime
    /// capability check that complements the compile-time cfg gate.
    /// Function-pointer table returned by `XdpOpenApi` (the only stable
    /// export of `xdpapi.dll` for the modern API). Fields are in the exact
    /// order of the C `XDP_API_TABLE`; entries this code does not use are
    /// kept opaque so the layout - and thus the offsets of the entries it
    /// does use - is reproduced exactly.
    #[repr(C)]
    struct XdpApiTable {
        open_api: *const core::ffi::c_void,
        close_api: FnXdpCloseApi,
        get_routine: *const core::ffi::c_void,
        create_program: FnXdpCreateProgram,
        interface_open: *const core::ffi::c_void,
        create: FnXskCreate,
        bind: FnXskBind,
        activate: FnXskActivate,
        notify_socket: FnXskNotifySocket,
        notify_async: *const core::ffi::c_void,
        get_notify_async_result: *const core::ffi::c_void,
        set_sockopt: FnXskSetSockopt,
        get_sockopt: FnXskGetSockopt,
        ioctl: *const core::ffi::c_void,
    }

    type FnXdpOpenApi = unsafe extern "system" fn(u32, *mut *const XdpApiTable) -> XdpStatus;
    type FnXdpCloseApi = unsafe extern "system" fn(*const XdpApiTable);

    /// Dynamically-loaded XDP-for-Windows API. `xdpapi.dll` exports only
    /// `XdpOpenApi`, which yields the function table; loading fails (returns
    /// Err) when the runtime driver is not installed - the runtime
    /// capability check that complements the compile-time cfg gate.
    struct XdpApi {
        lib: HMODULE,
        table: *const XdpApiTable,
        create: FnXskCreate,
        bind: FnXskBind,
        activate: FnXskActivate,
        set_sockopt: FnXskSetSockopt,
        get_sockopt: FnXskGetSockopt,
        notify: FnXskNotifySocket,
        create_program: FnXdpCreateProgram,
    }

    impl XdpApi {
        fn load() -> io::Result<Self> {
            const XDP_API_VERSION_1: u32 = 1;
            let name: Vec<u16> = "xdpapi.dll".encode_utf16().chain(Some(0)).collect();
            let lib = unsafe { LoadLibraryW(name.as_ptr()) };
            if lib.is_null() {
                return Err(io::Error::other(
                    "LoadLibrary(xdpapi.dll) failed; install the XDP-for-Windows runtime",
                ));
            }
            let proc = unsafe { GetProcAddress(lib, c"XdpOpenApi".as_ptr() as *const u8) };
            let open_api: FnXdpOpenApi = match proc {
                Some(f) => unsafe {
                    std::mem::transmute::<unsafe extern "system" fn() -> isize, FnXdpOpenApi>(f)
                },
                None => {
                    unsafe { FreeLibrary(lib) };
                    return Err(io::Error::other("xdpapi.dll missing XdpOpenApi"));
                }
            };
            let mut table: *const XdpApiTable = std::ptr::null();
            let st = unsafe { open_api(XDP_API_VERSION_1, &mut table) };
            if st < 0 || table.is_null() {
                unsafe { FreeLibrary(lib) };
                return Err(io::Error::other(format!("XdpOpenApi failed: {st:#x}")));
            }
            let t = unsafe { &*table };
            Ok(Self {
                lib,
                table,
                create: t.create,
                bind: t.bind,
                activate: t.activate,
                set_sockopt: t.set_sockopt,
                get_sockopt: t.get_sockopt,
                notify: t.notify_socket,
                create_program: t.create_program,
            })
        }
    }

    impl Drop for XdpApi {
        fn drop(&mut self) {
            unsafe {
                ((*self.table).close_api)(self.table);
                FreeLibrary(self.lib);
            }
        }
    }

    /// The shared-memory ring (port of `afxdp_helper.h`'s `XSK_RING`):
    /// single-producer or single-consumer index management over the
    /// kernel-shared ring, with a cached opposite-index for lock-free
    /// reservation.
    struct XskRing {
        producer: *mut AtomicU32,
        consumer: *mut AtomicU32,
        elements: *mut u8,
        mask: u32,
        size: u32,
        stride: u32,
        cached_producer: u32,
        cached_consumer: u32,
    }

    unsafe impl Send for XskRing {}

    impl XskRing {
        unsafe fn new(info: &XskRingInfo) -> Self {
            unsafe {
                let producer =
                    info.ring.add(info.producer_index_offset as usize) as *mut AtomicU32;
                let consumer =
                    info.ring.add(info.consumer_index_offset as usize) as *mut AtomicU32;
                let elements = info.ring.add(info.descriptors_offset as usize);
                let cached_producer = (*producer).load(Ordering::Acquire);
                let cached_consumer = (*consumer).load(Ordering::Acquire);
                Self {
                    producer,
                    consumer,
                    elements,
                    mask: info.size - 1,
                    size: info.size,
                    stride: info.element_stride,
                    cached_producer,
                    cached_consumer,
                }
            }
        }

        unsafe fn element(&self, index: u32) -> *mut u8 {
            unsafe {
                self.elements
                    .add((index & self.mask) as usize * self.stride as usize)
            }
        }

        /// Producer side (fill / tx): how many slots can be filled, and the
        /// producer index to start at.
        unsafe fn producer_reserve(&mut self, max: u32) -> (u32, u32) {
            unsafe {
                let producer = (*self.producer).load(Ordering::Relaxed);
                let mut avail = self.size - producer.wrapping_sub(self.cached_consumer);
                if avail < max {
                    self.cached_consumer = (*self.consumer).load(Ordering::Acquire);
                    avail = self.size - producer.wrapping_sub(self.cached_consumer);
                }
                (avail.min(max), producer)
            }
        }

        unsafe fn producer_submit(&mut self, count: u32) {
            unsafe {
                let cur = (*self.producer).load(Ordering::Relaxed);
                (*self.producer).store(cur.wrapping_add(count), Ordering::Release);
            }
        }

        /// Consumer side (rx / completion): how many slots are ready, and the
        /// consumer index to start at.
        unsafe fn consumer_reserve(&mut self, max: u32) -> (u32, u32) {
            unsafe {
                let consumer = (*self.consumer).load(Ordering::Relaxed);
                let mut avail = self.cached_producer.wrapping_sub(consumer);
                if avail < max {
                    self.cached_producer = (*self.producer).load(Ordering::Acquire);
                    avail = self.cached_producer.wrapping_sub(consumer);
                }
                (avail.min(max), consumer)
            }
        }

        unsafe fn consumer_release(&mut self, count: u32) {
            unsafe {
                let cur = (*self.consumer).load(Ordering::Relaxed);
                (*self.consumer).store(cur.wrapping_add(count), Ordering::Release);
            }
        }
    }

    const N_FRAMES: u32 = 64;
    const FRAME_SIZE: u32 = 2048;
    const RING_SIZE: u32 = 32;

    /// A `Locale::Wire` endpoint on Windows: an XDP-for-Windows AF_XDP
    /// socket bound to one NIC queue, with the UMEM + four rings + an
    /// `XdpCreateProgram` redirect rule. Same `send_frame` / `recv_frame`
    /// surface as the Linux side.
    pub struct WireSocket {
        api: XdpApi,
        socket: HANDLE,
        program: HANDLE,
        umem: *mut u8,
        fill: XskRing,
        comp: XskRing,
        rx: XskRing,
        tx: XskRing,
        tx_free: Vec<u64>,
    }

    unsafe impl Send for WireSocket {}

    impl WireSocket {
        /// Bind an XDP socket to `if_index` queue `queue_id`, stand up the
        /// UMEM + rings, and attach a redirect program steering ingress UDP
        /// frames destined to `udp_dst_port` into this socket's RX ring.
        /// Generic mode, so it runs on any NIC. Returns Err when the XDP
        /// runtime is absent or the NIC rejects the bind.
        pub fn bind(if_index: u32, queue_id: u32, udp_dst_port: u16) -> io::Result<Self> {
            let api = XdpApi::load()?;

            let umem_size = (N_FRAMES * FRAME_SIZE) as usize;
            let umem = unsafe {
                VirtualAlloc(
                    std::ptr::null(),
                    umem_size,
                    MEM_COMMIT | MEM_RESERVE,
                    PAGE_READWRITE,
                )
            } as *mut u8;
            if umem.is_null() {
                return Err(io::Error::last_os_error());
            }

            // Build everything, unwinding the UMEM allocation on any failure.
            match Self::build(api, umem, umem_size, if_index, queue_id, udp_dst_port) {
                Ok(s) => Ok(s),
                Err(e) => {
                    unsafe {
                        VirtualFree(umem as *mut core::ffi::c_void, 0, MEM_RELEASE);
                    }
                    Err(e)
                }
            }
        }

        fn build(
            api: XdpApi,
            umem: *mut u8,
            umem_size: usize,
            if_index: u32,
            queue_id: u32,
            udp_dst_port: u16,
        ) -> io::Result<Self> {
            let mut socket: HANDLE = std::ptr::null_mut();
            check(unsafe { (api.create)(&mut socket) }, "XskCreate")?;

            let umem_reg = XskUmemReg {
                total_size: umem_size as u64,
                chunk_size: FRAME_SIZE,
                headroom: 0,
                address: umem as *mut core::ffi::c_void,
            };
            check(
                unsafe {
                    (api.set_sockopt)(
                        socket,
                        XSK_SOCKOPT_UMEM_REG,
                        &umem_reg as *const _ as *const core::ffi::c_void,
                        std::mem::size_of::<XskUmemReg>() as u32,
                    )
                },
                "XSK_SOCKOPT_UMEM_REG",
            )?;

            check(
                unsafe {
                    (api.bind)(
                        socket,
                        if_index,
                        queue_id,
                        XSK_BIND_FLAG_RX | XSK_BIND_FLAG_TX | XSK_BIND_FLAG_GENERIC,
                    )
                },
                "XskBind",
            )?;

            for (opt, name) in [
                (XSK_SOCKOPT_RX_RING_SIZE, "RX_RING_SIZE"),
                (XSK_SOCKOPT_RX_FILL_RING_SIZE, "RX_FILL_RING_SIZE"),
                (XSK_SOCKOPT_TX_RING_SIZE, "TX_RING_SIZE"),
                (XSK_SOCKOPT_TX_COMPLETION_RING_SIZE, "TX_COMPLETION_RING_SIZE"),
            ] {
                let sz = RING_SIZE;
                check(
                    unsafe {
                        (api.set_sockopt)(
                            socket,
                            opt,
                            &sz as *const u32 as *const core::ffi::c_void,
                            4,
                        )
                    },
                    name,
                )?;
            }

            check(
                unsafe { (api.activate)(socket, XSK_ACTIVATE_FLAG_NONE) },
                "XskActivate",
            )?;

            let mut info: XskRingInfoSet = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of::<XskRingInfoSet>() as u32;
            check(
                unsafe {
                    (api.get_sockopt)(
                        socket,
                        XSK_SOCKOPT_RING_INFO,
                        &mut info as *mut _ as *mut core::ffi::c_void,
                        &mut len,
                    )
                },
                "XSK_SOCKOPT_RING_INFO",
            )?;

            let mut fill = unsafe { XskRing::new(&info.fill) };
            let comp = unsafe { XskRing::new(&info.completion) };
            let rx = unsafe { XskRing::new(&info.rx) };
            let tx = unsafe { XskRing::new(&info.tx) };

            // First half of the UMEM frames seed the RX fill ring; the rest
            // form the TX free pool. Fill / completion elements are bare u64
            // UMEM offsets; rx / tx elements are XSK_BUFFER_DESCRIPTOR.
            let rx_count = N_FRAMES / 2;
            let mut tx_free = Vec::new();
            unsafe {
                let (n, idx) = fill.producer_reserve(rx_count);
                for j in 0..n {
                    *(fill.element(idx + j) as *mut u64) = (j * FRAME_SIZE) as u64;
                }
                fill.producer_submit(n);
            }
            for i in rx_count..N_FRAMES {
                tx_free.push((i * FRAME_SIZE) as u64);
            }

            // Attach the redirect program: ingress UDP frames to udp_dst_port
            // are redirected into this XSK (the Windows analogue of libxdp's
            // redirect program).
            let hook = XdpHookId {
                layer: XDP_HOOK_L2,
                direction: XDP_HOOK_RX,
                sublayer: XDP_HOOK_INSPECT,
            };
            let mut rule: XdpRule = unsafe { std::mem::zeroed() };
            rule.match_type = XDP_MATCH_UDP_DST;
            rule.pattern.port = udp_dst_port.to_be();
            rule.action = XDP_PROGRAM_ACTION_REDIRECT;
            rule.u.redirect = XdpRedirectParams {
                target_type: XDP_REDIRECT_TARGET_TYPE_XSK,
                target: socket,
            };
            let mut program: HANDLE = std::ptr::null_mut();
            check(
                unsafe {
                    (api.create_program)(
                        if_index,
                        &hook,
                        queue_id,
                        XDP_CREATE_PROGRAM_FLAG_GENERIC,
                        &rule,
                        1,
                        &mut program,
                    )
                },
                "XdpCreateProgram",
            )?;

            Ok(Self {
                api,
                socket,
                program,
                umem,
                fill,
                comp,
                rx,
                tx,
                tx_free,
            })
        }

        /// Reclaim completed TX frames (bare u64 offsets) into the free pool.
        fn reap_tx(&mut self) {
            unsafe {
                loop {
                    let (n, idx) = self.comp.consumer_reserve(RING_SIZE);
                    if n == 0 {
                        break;
                    }
                    for j in 0..n {
                        let off = *(self.comp.element(idx + j) as *const u64);
                        self.tx_free.push(off);
                    }
                    self.comp.consumer_release(n);
                }
            }
        }

        /// Transmit one raw Ethernet frame via the XSK TX ring, socket stack
        /// bypassed. Recycles completed frames first so none is reused while
        /// the kernel still owns it.
        pub fn send_frame(&mut self, data: &[u8]) -> io::Result<()> {
            self.reap_tx();
            let mut spins = 0u32;
            while self.tx_free.is_empty() {
                self.reap_tx();
                spins += 1;
                if spins > 1_000_000 {
                    return Err(io::Error::other("TX completion ring stalled"));
                }
                std::hint::spin_loop();
            }
            let off = self.tx_free.pop().unwrap();
            assert!(data.len() <= FRAME_SIZE as usize, "frame exceeds UMEM chunk");
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    self.umem.add(off as usize),
                    data.len(),
                );
                let (n, idx) = self.tx.producer_reserve(1);
                if n == 0 {
                    self.tx_free.push(off);
                    return Err(io::Error::other("TX ring full"));
                }
                let desc = self.tx.element(idx) as *mut XskBufferDescriptor;
                (*desc).address = off;
                (*desc).length = data.len() as u32;
                (*desc).reserved = 0;
                self.tx.producer_submit(1);
            }
            let mut result: u32 = 0;
            check(
                unsafe { (self.api.notify)(self.socket, XSK_NOTIFY_FLAG_POKE_TX, 0, &mut result) },
                "XskNotifySocket(POKE_TX)",
            )?;
            Ok(())
        }

        /// Receive one raw Ethernet frame from the XSK RX ring into `out`,
        /// socket stack bypassed. Blocks up to `timeout_ms`; `Ok(0)` on
        /// timeout. Recycles consumed frames back to the fill ring.
        pub fn recv_frame(&mut self, out: &mut [u8], timeout_ms: u32) -> io::Result<usize> {
            let (mut n, mut idx) = unsafe { self.rx.consumer_reserve(1) };
            if n == 0 && timeout_ms > 0 {
                let mut result: u32 = 0;
                // Wait for RX; ignore a timeout status, just re-check the ring.
                unsafe {
                    (self.api.notify)(self.socket, XSK_NOTIFY_FLAG_WAIT_RX, timeout_ms, &mut result);
                }
                let r = unsafe { self.rx.consumer_reserve(1) };
                n = r.0;
                idx = r.1;
            }
            if n == 0 {
                return Ok(0);
            }
            let copied = unsafe {
                let desc = self.rx.element(idx) as *const XskBufferDescriptor;
                let addr = (*desc).address;
                let base = (addr & 0xFFFF_FFFF_FFFF) as usize; // BaseAddress (48 bits)
                let off = ((addr >> 48) & 0xFFFF) as usize; // Offset (16 bits)
                let len = (*desc).length as usize;
                let copy = len.min(out.len());
                std::ptr::copy_nonoverlapping(self.umem.add(base + off), out.as_mut_ptr(), copy);
                self.rx.consumer_release(1);
                // Recycle the frame (its base offset) back to the fill ring.
                let (fn_avail, fidx) = self.fill.producer_reserve(1);
                if fn_avail == 1 {
                    *(self.fill.element(fidx) as *mut u64) = base as u64;
                    self.fill.producer_submit(1);
                }
                copy
            };
            Ok(copied)
        }

        /// Read the socket's RX/TX drop + invalid-descriptor counters
        /// (XSK_SOCKOPT_STATISTICS). Useful for diagnosing whether frames
        /// are arriving-but-dropped versus never arriving.
        pub fn stats(&self) -> io::Result<XskStats> {
            const XSK_SOCKOPT_STATISTICS: u32 = 7;
            let mut s = XskStats::default();
            let mut len = std::mem::size_of::<XskStats>() as u32;
            check(
                unsafe {
                    (self.api.get_sockopt)(
                        self.socket,
                        XSK_SOCKOPT_STATISTICS,
                        &mut s as *mut _ as *mut core::ffi::c_void,
                        &mut len,
                    )
                },
                "XSK_SOCKOPT_STATISTICS",
            )?;
            Ok(s)
        }
    }

    /// AF_XDP socket statistics (`XSK_STATISTICS`).
    #[repr(C)]
    #[derive(Debug, Default, Clone, Copy)]
    pub struct XskStats {
        pub rx_dropped: u64,
        pub rx_truncated: u64,
        pub rx_invalid_descriptors: u64,
        pub tx_invalid_descriptors: u64,
    }

    impl Drop for WireSocket {
        fn drop(&mut self) {
            unsafe {
                if !self.program.is_null() {
                    CloseHandle(self.program);
                }
                if !self.socket.is_null() {
                    CloseHandle(self.socket);
                }
                if !self.umem.is_null() {
                    VirtualFree(self.umem as *mut core::ffi::c_void, 0, MEM_RELEASE);
                }
            }
        }
    }

    /// A discovered network interface: its kernel interface index, MAC, and
    /// human description (so callers can filter out virtual adapters).
    #[derive(Clone, Debug)]
    pub struct NicInfo {
        pub if_index: u32,
        pub mac: [u8; 6],
        pub description: String,
    }

    fn pwstr_to_string(p: *const u16) -> String {
        if p.is_null() {
            return String::new();
        }
        let mut len = 0;
        while unsafe { *p.add(len) } != 0 {
            len += 1;
        }
        String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(p, len) })
    }

    /// Enumerate the Up Ethernet NICs (`if_index` + MAC) so a caller can
    /// pick which interface(s) to bind a [`WireSocket`] to without
    /// hardcoding indices - the runtime hardware-detection counterpart to
    /// the compile-time cfg gate.
    pub fn list_ethernet_nics() -> io::Result<Vec<NicInfo>> {
        const AF_UNSPEC: u32 = 0;
        // Skip the unicast / anycast / multicast / DNS address lists; we
        // only need the adapter's index, type, status and MAC.
        const GAA_SKIP: u32 = 0xF;
        const IF_TYPE_ETHERNET_CSMACD: u32 = 6;
        const IF_OPER_STATUS_UP: i32 = 1;

        // First call sizes the buffer (returns ERROR_BUFFER_OVERFLOW).
        let mut size: u32 = 0;
        unsafe {
            GetAdaptersAddresses(
                AF_UNSPEC,
                GAA_SKIP,
                std::ptr::null(),
                std::ptr::null_mut(),
                &mut size,
            );
        }
        if size == 0 {
            return Err(io::Error::other("GetAdaptersAddresses returned no size"));
        }

        // 8-aligned backing for the linked list of adapter structs.
        let mut buf = vec![0u64; (size as usize).div_ceil(8)];
        let head = buf.as_mut_ptr() as *mut IP_ADAPTER_ADDRESSES_LH;
        let rc = unsafe {
            GetAdaptersAddresses(AF_UNSPEC, GAA_SKIP, std::ptr::null(), head, &mut size)
        };
        if rc != 0 {
            return Err(io::Error::other(format!("GetAdaptersAddresses failed: {rc}")));
        }

        let mut nics = Vec::new();
        let mut cur = head;
        while !cur.is_null() {
            let a = unsafe { &*cur };
            if a.IfType == IF_TYPE_ETHERNET_CSMACD
                && a.OperStatus == IF_OPER_STATUS_UP
                && a.PhysicalAddressLength >= 6
            {
                let if_index = unsafe { a.Anonymous1.Anonymous.IfIndex };
                let mut mac = [0u8; 6];
                mac.copy_from_slice(&a.PhysicalAddress[..6]);
                let description = pwstr_to_string(a.Description);
                nics.push(NicInfo { if_index, mac, description });
            }
            cur = a.Next;
        }
        Ok(nics)
    }
}

#[cfg(windows)]
pub use windows_impl::{list_ethernet_nics, NicInfo, WireSocket, XskStats};

// ---------------------------------------------------------------------
// FreeBSD: netmap via libnetmap (nmport_open / nmport_inject) for open +
// TX, with the RX path reading the mmap'd netmap rings directly. Only the
// field offsets into the netmap structs are platform-specific; they are
// the verified offsetof() values from <net/netmap.h> + <libnetmap.h> on
// FreeBSD 15 (amd64, NM_CACHE_ALIGN = 128). A wrong offset would corrupt
// the RX path, so the wire_netmap_e2e round-trip is the real check.
// ---------------------------------------------------------------------

#[cfg(target_os = "freebsd")]
mod freebsd_impl {
    use std::ffi::CString;
    use std::io;
    use std::os::raw::{c_char, c_int, c_void};

    #[link(name = "netmap")]
    unsafe extern "C" {
        /// Open a netmap port (NIC, `vale*:*`, pipe, ...), registering its
        /// rings and mmap-ing the netmap memory. Returns a `nmport_d *`.
        fn nmport_open(portspec: *const c_char) -> *mut c_void;
        /// Copy one frame into a free TX slot. Returns the size on success,
        /// 0 when no TX slot is free.
        fn nmport_inject(d: *mut c_void, buf: *const c_void, size: usize) -> c_int;
        /// Unregister + munmap + close the port.
        fn nmport_close(d: *mut c_void);
    }

    // Verified field offsets - offsetof() on FreeBSD 15 amd64
    // <net/netmap.h> + <libnetmap.h>, NM_CACHE_ALIGN = 128.
    // struct nmport_d:
    const NMPORT_FD: usize = 176; // int fd
    const NMPORT_NIFP: usize = 184; // struct netmap_if *nifp
    const NMPORT_FIRST_RX: usize = 196; // uint16_t first_rx_ring
    const NMPORT_LAST_RX: usize = 198; // uint16_t last_rx_ring
    // struct netmap_if:
    const NIF_NI_TX_RINGS: usize = 24; // uint32_t ni_tx_rings
    const NIF_NI_HOST_TX_RINGS: usize = 36; // uint32_t ni_host_tx_rings
    const NIF_RING_OFS: usize = 56; // ssize_t ring_ofs[]
    // struct netmap_ring:
    const RING_BUF_OFS: usize = 0; // int64_t buf_ofs
    const RING_NUM_SLOTS: usize = 8; // uint32_t num_slots
    const RING_NR_BUF_SIZE: usize = 12; // uint32_t nr_buf_size
    const RING_HEAD: usize = 20; // uint32_t head
    const RING_CUR: usize = 24; // uint32_t cur
    const RING_TAIL: usize = 28; // uint32_t tail
    const RING_SLOT: usize = 256; // struct netmap_slot slot[]
    // struct netmap_slot:
    const SLOT_SIZE: usize = 16;
    const SLOT_BUF_IDX: usize = 0; // uint32_t buf_idx
    const SLOT_LEN: usize = 4; // uint16_t len
    // ioctl(fd, NIOCTXSYNC): flush injected TX frames to the wire / switch.
    const NIOCTXSYNC: libc::c_ulong = 0x2000_6994;

    #[inline]
    unsafe fn rd<T: Copy>(base: *const u8, off: usize) -> T {
        unsafe { std::ptr::read_unaligned(base.add(off) as *const T) }
    }
    #[inline]
    unsafe fn wr<T>(base: *mut u8, off: usize, v: T) {
        unsafe { std::ptr::write_unaligned(base.add(off) as *mut T, v) }
    }

    /// A `Locale::Wire` endpoint backed by a netmap port. Transmits and
    /// receives raw Ethernet frames with the socket stack bypassed.
    pub struct WireSocket {
        d: *mut c_void, // struct nmport_d *
        fd: c_int,
    }

    // The nmport_d is owned solely by this handle; moving the WireSocket
    // across threads moves only the pointer, exactly as for the Linux /
    // Windows endpoints.
    unsafe impl Send for WireSocket {}

    impl WireSocket {
        /// Open a netmap port and bind its rings. `if_name` may be a bare
        /// NIC name (bound as `netmap:<if>-<queue_id>`) or a full netmap
        /// port spec - anything containing `:` (e.g. `vale0:a`) is used
        /// verbatim. Needs the netmap device (`/dev/netmap`); returns Err
        /// otherwise so the caller can fall back to a socket path.
        pub fn bind(if_name: &str, queue_id: u32) -> io::Result<Self> {
            let portspec = if if_name.contains(':') {
                if_name.to_string()
            } else {
                format!("netmap:{if_name}-{queue_id}")
            };
            let c = CString::new(portspec)
                .map_err(|_| io::Error::other("interface name has interior NUL"))?;
            let d = unsafe { nmport_open(c.as_ptr()) };
            if d.is_null() {
                return Err(io::Error::last_os_error());
            }
            let fd = unsafe { rd::<c_int>(d as *const u8, NMPORT_FD) };
            Ok(Self { d, fd })
        }

        /// Transmit one raw Ethernet frame through a netmap TX ring,
        /// bypassing the socket send path. Flushes via `NIOCTXSYNC` so the
        /// frame reaches the wire / VALE switch immediately; reclaims sent
        /// slots and retries once if the ring is momentarily full.
        pub fn send_frame(&mut self, data: &[u8]) -> io::Result<()> {
            let inject = |d: *mut c_void| -> c_int {
                unsafe { nmport_inject(d, data.as_ptr() as *const c_void, data.len()) }
            };
            if inject(self.d) == 0 {
                // No free TX slot: flush completed frames and retry once.
                unsafe { libc::ioctl(self.fd, NIOCTXSYNC) };
                if inject(self.d) == 0 {
                    return Err(io::Error::other("netmap TX ring full"));
                }
            }
            if unsafe { libc::ioctl(self.fd, NIOCTXSYNC) } < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        /// Receive one raw Ethernet frame from a netmap RX ring into `out`,
        /// bypassing the socket recv path. Blocks up to `timeout_ms`;
        /// returns `Ok(0)` on timeout. Reads the first non-empty RX ring
        /// and advances its head past the consumed slot.
        pub fn recv_frame(&mut self, out: &mut [u8], timeout_ms: i32) -> io::Result<usize> {
            let mut pfd = libc::pollfd { fd: self.fd, events: libc::POLLIN, revents: 0 };
            // poll with POLLIN performs the RX sync and blocks until frames
            // are available or the timeout elapses.
            let r = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            if r < 0 {
                return Err(io::Error::last_os_error());
            }
            if r == 0 {
                return Ok(0);
            }
            // SAFETY: nmport_open mmap'd the netmap memory; nifp and every
            // ring/slot/buffer offset below is within that region, and the
            // offsets are the verified <net/netmap.h> layout for this ABI.
            unsafe {
                let nifp = rd::<*mut u8>(self.d as *const u8, NMPORT_NIFP);
                let first_rx = rd::<u16>(self.d as *const u8, NMPORT_FIRST_RX);
                let last_rx = rd::<u16>(self.d as *const u8, NMPORT_LAST_RX);
                let ni_tx = rd::<u32>(nifp, NIF_NI_TX_RINGS) as usize;
                let ni_htx = rd::<u32>(nifp, NIF_NI_HOST_TX_RINGS) as usize;
                for i in first_rx..=last_rx {
                    // ring_ofs order is: NIC tx, host tx, then NIC rx rings.
                    let ring_idx = i as usize + ni_tx + ni_htx;
                    let ofs = rd::<i64>(nifp, NIF_RING_OFS + ring_idx * 8);
                    let ring = nifp.offset(ofs as isize);
                    let head = rd::<u32>(ring, RING_HEAD);
                    let tail = rd::<u32>(ring, RING_TAIL);
                    if head == tail {
                        continue; // this ring is empty
                    }
                    let num_slots = rd::<u32>(ring, RING_NUM_SLOTS);
                    let buf_ofs = rd::<i64>(ring, RING_BUF_OFS);
                    let nr_buf_size = rd::<u32>(ring, RING_NR_BUF_SIZE) as usize;
                    let slot = ring.add(RING_SLOT + head as usize * SLOT_SIZE);
                    let buf_idx = rd::<u32>(slot, SLOT_BUF_IDX) as usize;
                    let slen = rd::<u16>(slot, SLOT_LEN) as usize;
                    let bufp = ring.offset(buf_ofs as isize).add(buf_idx * nr_buf_size);
                    let n = slen.min(out.len());
                    std::ptr::copy_nonoverlapping(bufp, out.as_mut_ptr(), n);
                    // Advance head + cur past the consumed slot so the slot
                    // returns to the kernel on the next sync.
                    let next = if head + 1 == num_slots { 0 } else { head + 1 };
                    wr::<u32>(ring, RING_HEAD, next);
                    wr::<u32>(ring, RING_CUR, next);
                    return Ok(n);
                }
            }
            // poll reported readable but none of our NIC rx rings held a
            // frame (e.g. a host ring did); no data for the caller this round.
            Ok(0)
        }
    }

    impl Drop for WireSocket {
        fn drop(&mut self) {
            unsafe { nmport_close(self.d) };
        }
    }
}

#[cfg(target_os = "freebsd")]
pub use freebsd_impl::WireSocket;

// ---------------------------------------------------------------------
// macOS: raw L2 frames via BPF (/dev/bpf*). Darwin has neither AF_XDP nor
// netmap; BPF is the userspace raw-packet path. `bind` opens a free BPF
// node and attaches it to the NIC; `send_frame` is a `write`, `recv_frame`
// is a `read` whose buffer holds one-or-more `bpf_hdr`-prefixed frames that
// are handed out one per call.
// ---------------------------------------------------------------------

#[cfg(target_os = "macos")]
mod macos_impl {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::io::RawFd;

    // BSD ioctl encoding (sys/ioccom.h): direction | (len << 16) |
    // (group << 8) | num. BPF commands use group 'B'.
    const IOC_OUT: libc::c_ulong = 0x4000_0000;
    const IOC_IN: libc::c_ulong = 0x8000_0000;
    const IOCPARM_MASK: libc::c_ulong = 0x1fff;
    const B: libc::c_ulong = b'B' as libc::c_ulong;
    const U_INT: libc::c_ulong = 4; // sizeof(u_int)

    const fn ioc(dir: libc::c_ulong, num: libc::c_ulong, len: libc::c_ulong) -> libc::c_ulong {
        dir | ((len & IOCPARM_MASK) << 16) | (B << 8) | num
    }
    // BIOCGBLEN=_IOR('B',102,u_int)  BIOCSETIF=_IOW('B',108,ifreq)
    // BIOCIMMEDIATE=_IOW('B',112,u_int)  BIOCSHDRCMPLT=_IOW('B',117,u_int)
    const BIOCGBLEN: libc::c_ulong = ioc(IOC_OUT, 102, U_INT);
    const BIOCSETIF: libc::c_ulong =
        ioc(IOC_IN, 108, std::mem::size_of::<libc::ifreq>() as libc::c_ulong);
    const BIOCIMMEDIATE: libc::c_ulong = ioc(IOC_IN, 112, U_INT);
    const BIOCSHDRCMPLT: libc::c_ulong = ioc(IOC_IN, 117, U_INT);

    /// `BPF_WORDALIGN`: BPF packs frames at `BPF_ALIGNMENT` (`sizeof(int32_t)`
    /// = 4) boundaries within a read buffer.
    #[inline]
    fn bpf_wordalign(x: usize) -> usize {
        (x + 3) & !3
    }

    /// A `Locale::Wire` endpoint backed by a BPF device bound to one NIC.
    /// Transmits raw Ethernet frames via `write` and receives them via
    /// `read` (the kernel returns one-or-more `bpf_hdr`-prefixed frames per
    /// read), bypassing the socket stack.
    pub struct WireSocket {
        fd: RawFd,
        // Kernel-chosen read buffer length (BIOCGBLEN); BPF requires reads of
        // exactly this size. The buffer is drained frame-by-frame across
        // calls: `filled` bytes are valid, `cursor` is the next frame.
        blen: usize,
        rbuf: Vec<u8>,
        filled: usize,
        cursor: usize,
    }

    // The BPF fd is owned solely by this handle; moving the WireSocket across
    // threads moves only the descriptor + buffer, as for the other endpoints.
    unsafe impl Send for WireSocket {}

    impl WireSocket {
        /// Open a BPF device and attach it to `if_name` (e.g. `en0`,
        /// `feth0`). `queue_id` is ignored - BPF has no per-queue binding.
        /// Needs root or `/dev/bpf*` access; returns Err otherwise so the
        /// caller can fall back to a socket path.
        pub fn bind(if_name: &str, _queue_id: u32) -> io::Result<Self> {
            let fd = Self::open_bpf()?;
            match Self::setup(fd, if_name) {
                Ok(blen) => Ok(Self { fd, blen, rbuf: vec![0u8; blen], filled: 0, cursor: 0 }),
                Err(e) => {
                    // SAFETY: `fd` is the just-opened BPF descriptor.
                    unsafe { libc::close(fd) };
                    Err(e)
                }
            }
        }

        /// Open the first free `/dev/bpfN`. EBUSY means that node is taken,
        /// try the next; EACCES (not privileged) and ENOENT (no more nodes)
        /// are surfaced to the caller.
        fn open_bpf() -> io::Result<RawFd> {
            for n in 0..256 {
                let path = CString::new(format!("/dev/bpf{n}")).unwrap();
                // SAFETY: `path` is a valid NUL-terminated device path.
                let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDWR) };
                if fd >= 0 {
                    return Ok(fd);
                }
                let e = io::Error::last_os_error();
                if e.raw_os_error() != Some(libc::EBUSY) {
                    return Err(e);
                }
            }
            Err(io::Error::other("no free /dev/bpf device"))
        }

        /// Attach `fd` to `if_name`, enable immediate mode + complete-header
        /// writes, and return the kernel read-buffer length.
        fn setup(fd: RawFd, if_name: &str) -> io::Result<usize> {
            // SAFETY: zeroed ifreq is a valid all-zero struct; only ifr_name
            // is read by BIOCSETIF.
            let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
            let name = if_name.as_bytes();
            if name.len() >= ifr.ifr_name.len() {
                return Err(io::Error::other("interface name too long"));
            }
            for (slot, &b) in ifr.ifr_name.iter_mut().zip(name) {
                *slot = b as libc::c_char;
            }
            // SAFETY: each ioctl matches its documented arg type; `fd` is the
            // open BPF descriptor and the pointers reference live locals.
            unsafe {
                if libc::ioctl(fd, BIOCSETIF, &mut ifr) < 0 {
                    return Err(io::Error::last_os_error());
                }
                let one: libc::c_uint = 1;
                if libc::ioctl(fd, BIOCIMMEDIATE, &one) < 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::ioctl(fd, BIOCSHDRCMPLT, &one) < 0 {
                    return Err(io::Error::last_os_error());
                }
                let mut blen: libc::c_uint = 0;
                if libc::ioctl(fd, BIOCGBLEN, &mut blen) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(blen as usize)
            }
        }

        /// Transmit one raw Ethernet frame by writing it to the BPF device.
        pub fn send_frame(&mut self, data: &[u8]) -> io::Result<()> {
            // SAFETY: `data` is a valid slice; `fd` is the open BPF device.
            let n =
                unsafe { libc::write(self.fd, data.as_ptr() as *const libc::c_void, data.len()) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            if n as usize != data.len() {
                return Err(io::Error::other("short BPF write"));
            }
            Ok(())
        }

        /// Receive one raw Ethernet frame into `out`, blocking up to
        /// `timeout_ms` (`Ok(0)` on timeout). Each BPF `read` returns several
        /// frames, each prefixed by a `bpf_hdr` and padded to a 4-byte
        /// boundary; this hands them out one per call, refilling via `read`
        /// when the buffer drains.
        pub fn recv_frame(&mut self, out: &mut [u8], timeout_ms: i32) -> io::Result<usize> {
            loop {
                if self.cursor < self.filled {
                    // SAFETY: `cursor` indexes a kernel-written bpf_hdr within
                    // `filled` bytes of `rbuf`; read unaligned (BPF packs them
                    // at 4-byte boundaries, not the struct's natural align).
                    let hdr = unsafe {
                        std::ptr::read_unaligned(
                            self.rbuf.as_ptr().add(self.cursor) as *const libc::bpf_hdr
                        )
                    };
                    let hdrlen = hdr.bh_hdrlen as usize;
                    let caplen = hdr.bh_caplen as usize;
                    let data_off = self.cursor + hdrlen;
                    let avail = self.filled.saturating_sub(data_off);
                    let n = caplen.min(out.len()).min(avail);
                    out[..n].copy_from_slice(&self.rbuf[data_off..data_off + n]);
                    self.cursor += bpf_wordalign(hdrlen + caplen);
                    return Ok(n);
                }
                self.cursor = 0;
                self.filled = 0;
                let mut pfd =
                    libc::pollfd { fd: self.fd, events: libc::POLLIN, revents: 0 };
                // SAFETY: single valid pollfd for the lifetime of the call.
                let r = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
                if r < 0 {
                    let e = io::Error::last_os_error();
                    if e.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(e);
                }
                if r == 0 {
                    return Ok(0);
                }
                // SAFETY: read into `rbuf` of capacity `blen`; BPF requires the
                // read length be exactly the configured buffer length.
                let got = unsafe {
                    libc::read(self.fd, self.rbuf.as_mut_ptr() as *mut libc::c_void, self.blen)
                };
                if got < 0 {
                    let e = io::Error::last_os_error();
                    if e.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(e);
                }
                self.filled = got as usize;
                if self.filled == 0 {
                    return Ok(0);
                }
            }
        }
    }

    impl Drop for WireSocket {
        fn drop(&mut self) {
            // SAFETY: `fd` is the open BPF descriptor owned by this handle.
            unsafe { libc::close(self.fd) };
        }
    }
}

#[cfg(target_os = "macos")]
pub use macos_impl::WireSocket;
