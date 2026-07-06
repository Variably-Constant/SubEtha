//! Active OS path-event observer: a background watcher that fires the instant
//! the kernel's route table, an interface carrier, or the path MTU changes -
//! *ahead of any loss*.
//!
//! This is the active dual of [`crate::path_sensor`]. The path sensor is
//! passive: it learns of a path change only after a received datagram's TTL
//! reveals a new hop count, which is one round trip late. A re-route or a
//! Wi-Fi roam, by contrast, is announced by the OS the moment it happens - on
//! a netlink multicast group (Linux), a `PF_ROUTE` socket (the BSDs), or an
//! IP-helper change callback (Windows). Subscribing to that announcement lets
//! the controller pre-arm protection a full round trip before the first
//! datagram even reflects the new path.
//!
//! Two signals come out:
//!
//!  - **Path shift** (0..=1): spikes to 1.0 on a route / carrier / MTU event
//!    and decays with a fixed half-life, exactly like the hop-count shift, so
//!    the fusion controller treats an OS-announced path change the same way it
//!    treats a hop-count change. The sender fuses it as a third `path_shift`
//!    source alongside the passive hop-count shift and the link-class shift.
//!  - **Path MTU**: the egress interface MTU. A drop (1500 -> ~1280) is the
//!    tell of a lower-MTU link engaging - a cellular handoff, a tunnel coming
//!    up - and is itself a path event. Each endpoint reports its own MTU to
//!    the peer in a [`PmtuFrame`], so a receiver-side MTU drop rides its
//!    feedback to the sender and pre-arms that end too.
//!
//! The watcher runs on its own thread (Linux / BSD) or as an OS change
//! callback (Windows); the controller reads the two signals through cheap
//! lock-free atomics on its normal cadence. Per platform:
//!
//!  - **Linux**: an `AF_NETLINK` / `NETLINK_ROUTE` socket bound to the link,
//!    address, and route multicast groups; the egress MTU from
//!    `/sys/class/net/<iface>/mtu`.
//!  - **FreeBSD / macOS**: a `PF_ROUTE` raw socket (every routing-table
//!    change is delivered). The MTU read is a Linux / Windows capability; on
//!    the BSDs the observer reports route and carrier events and `pmtu`
//!    stays `None`.
//!  - **Windows**: `NotifyRouteChange2` + `NotifyIpInterfaceChange` callbacks;
//!    the egress MTU from the best up, non-loopback `GetIfTable2` row.
//!  - **Other**: a stub that never fires (always `path_shift = 0`, `pmtu`
//!    `None`).
//!
//! [`PmtuFrame`]: crate::control_frame::PmtuFrame

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Half-life of the path-shift spike, in seconds: the signal is 1.0 at the
/// event and halves every `SHIFT_HALF_LIFE_SECS` thereafter, so it stays above
/// the fusion controller's 0.5 pre-arm threshold for about this long.
const SHIFT_HALF_LIFE_SECS: f32 = 2.0;

/// The decaying path-shift value `secs_since_event` after the most recent
/// event: 1.0 at the event, halving every [`SHIFT_HALF_LIFE_SECS`]. A pure
/// function of elapsed time, so the decay is deterministically testable.
fn decayed_shift(secs_since_event: f32) -> f32 {
    if secs_since_event <= 0.0 {
        return 1.0;
    }
    0.5f32.powf(secs_since_event / SHIFT_HALF_LIFE_SECS)
}

/// Lock-free state shared between the OS watcher (thread or callback) and the
/// controller that reads it. Every field is an atomic so the reader never
/// blocks the watcher and the watcher never blocks the reader.
struct NetEventState {
    /// Total path events observed (monotonic). A nonzero value is the durable
    /// proof the watcher fired, surviving the path-shift decay.
    event_count: AtomicU64,
    /// `start.elapsed()` nanos at the most recent event; meaningful only once
    /// `have_event` is set.
    last_event_nanos: AtomicU64,
    /// Whether any event has been recorded yet (so a fresh observer reports a
    /// path shift of 0, not the decayed-from-zero 1.0).
    have_event: AtomicBool,
    /// Current egress-interface MTU in bytes; 0 means unknown / unavailable.
    pmtu: AtomicU32,
    /// Monotonic origin for the event timestamps.
    start: Instant,
}

impl NetEventState {
    fn new() -> Self {
        Self {
            event_count: AtomicU64::new(0),
            last_event_nanos: AtomicU64::new(0),
            have_event: AtomicBool::new(false),
            pmtu: AtomicU32::new(0),
            start: Instant::now(),
        }
    }

    /// Record a path event: bump the count and stamp the time, spiking the
    /// path shift to 1.0.
    fn record_event(&self) {
        self.event_count.fetch_add(1, Ordering::Relaxed);
        let t = self.start.elapsed().as_nanos() as u64;
        self.last_event_nanos.store(t, Ordering::Relaxed);
        self.have_event.store(true, Ordering::Relaxed);
    }

    /// Store the current MTU without treating it as an event. Used by the OS
    /// watcher, which already records the event for the netlink / callback
    /// message that delivered the change, so re-reading the MTU here must not
    /// double-count.
    fn set_pmtu(&self, mtu: u16) {
        if mtu != 0 {
            self.pmtu.store(mtu as u32, Ordering::Relaxed);
        }
    }

    /// Store the MTU and, if it dropped below the last known value, record a
    /// path event - a path-MTU decrease is a path change in its own right.
    /// Used by the synthetic inject path and exercised by the unit tests; the
    /// real OS watcher uses [`set_pmtu`](Self::set_pmtu) because the kernel
    /// message that carried the change already recorded the event.
    fn note_pmtu(&self, mtu: u16) {
        if mtu == 0 {
            return;
        }
        let prev = self.pmtu.swap(mtu as u32, Ordering::Relaxed);
        if prev != 0 && (mtu as u32) < prev {
            self.record_event();
        }
    }

    fn path_shift(&self) -> f32 {
        if !self.have_event.load(Ordering::Relaxed) {
            return 0.0;
        }
        let last = self.last_event_nanos.load(Ordering::Relaxed);
        let now = self.start.elapsed().as_nanos() as u64;
        let secs = now.saturating_sub(last) as f32 / 1e9;
        decayed_shift(secs)
    }

    fn pmtu(&self) -> Option<u16> {
        let v = self.pmtu.load(Ordering::Relaxed);
        (v != 0).then_some(v as u16)
    }

    fn event_count(&self) -> u64 {
        self.event_count.load(Ordering::Relaxed)
    }
}

/// A running path-event observer. Construct one with [`start`](Self::start);
/// the OS watcher runs until the observer is dropped, which stops the thread
/// (Linux / BSD) or cancels the change callbacks (Windows).
pub struct NetEventObserver {
    state: Arc<NetEventState>,
    backend: &'static str,
    /// Platform watcher handle whose `Drop` stops the thread / cancels the
    /// callbacks. Absent on the stub platform, which has nothing to stop.
    #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
    _watcher: unix_watch::Watcher,
    #[cfg(target_os = "windows")]
    _watcher: windows_watch::Watcher,
}

impl NetEventObserver {
    /// Start watching for path events. `iface` names the interface to read the
    /// MTU from; `None` auto-detects the first non-loopback up interface (the
    /// usual single-uplink case). Watcher startup is best-effort: if the OS
    /// notification source cannot be opened the observer still constructs and
    /// simply never fires, so a caller need not handle a failure.
    pub fn start(iface: Option<String>) -> Self {
        let state = Arc::new(NetEventState::new());
        // Seed the MTU once so `pmtu()` is populated from the start, before any
        // event re-reads it.
        if let Some(mtu) = read_iface_mtu(iface.as_deref()) {
            state.set_pmtu(mtu);
        }
        #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
        {
            let (watcher, backend) = unix_watch::Watcher::start(Arc::clone(&state), iface);
            Self { state, backend, _watcher: watcher }
        }
        #[cfg(target_os = "windows")]
        {
            drop(iface);
            let (watcher, backend) = windows_watch::Watcher::start(Arc::clone(&state));
            Self { state, backend, _watcher: watcher }
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos",
            target_os = "windows"
        )))]
        {
            drop(iface);
            Self { state, backend: "stub" }
        }
    }

    /// Decaying path-shift signal (0..=1): high just after a route / carrier /
    /// MTU event, fading with a fixed half-life. Fused as a `path_shift` source.
    pub fn path_shift(&self) -> f32 {
        self.state.path_shift()
    }

    /// Current egress-interface MTU in bytes, or `None` if unknown / the
    /// platform does not read it. Reported to the peer in a [`PmtuFrame`].
    ///
    /// [`PmtuFrame`]: crate::control_frame::PmtuFrame
    pub fn pmtu(&self) -> Option<u16> {
        self.state.pmtu()
    }

    /// Total path events observed since start (monotonic). A nonzero value is
    /// the durable proof the watcher fired, independent of the shift decay.
    pub fn event_count(&self) -> u64 {
        self.state.event_count()
    }

    /// Backend identifier (for diagnostics).
    pub fn backend(&self) -> &'static str {
        self.backend
    }

    /// Synthetically record a path event, as if the OS had announced a route /
    /// carrier change. Drives the `--sim-path-event` demo and the unit tests on
    /// a host where flapping a real interface is impractical; the production
    /// path is the OS watcher.
    pub fn inject_event(&self) {
        self.state.record_event();
    }

    /// Synthetically report a path MTU, recording an event if it is a drop -
    /// the same path a polled MTU decrease would take. For tests / demos.
    pub fn inject_pmtu(&self, mtu: u16) {
        self.state.note_pmtu(mtu);
    }
}

/// Read the egress-interface MTU for this platform, or `None` if unavailable.
fn read_iface_mtu(iface: Option<&str>) -> Option<u16> {
    #[cfg(target_os = "linux")]
    {
        unix_watch::read_iface_mtu_linux(iface)
    }
    #[cfg(target_os = "windows")]
    {
        let _iface = iface; // Windows reads the MTU via GetIfTable2, not by name.
        windows_watch::read_iface_mtu_win()
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        let _iface = iface; // No MTU source on this platform.
        None
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
mod unix_watch {
    use super::NetEventState;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread::JoinHandle;

    /// The first non-loopback interface whose `operstate` is `up` (Linux). The
    /// egress MTU is read from this interface unless the caller named one.
    #[cfg(target_os = "linux")]
    fn detect_iface() -> Option<String> {
        let entries = std::fs::read_dir("/sys/class/net").ok()?;
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name == "lo" {
                continue;
            }
            let up = std::fs::read_to_string(e.path().join("operstate"))
                .map(|s| s.trim() == "up")
                .unwrap_or(false);
            if up {
                return Some(name);
            }
        }
        None
    }

    /// Read `/sys/class/net/<iface>/mtu` (Linux), auto-detecting the interface
    /// when none is named.
    #[cfg(target_os = "linux")]
    pub fn read_iface_mtu_linux(iface: Option<&str>) -> Option<u16> {
        let name = iface.map(str::to_owned).or_else(detect_iface)?;
        let p = format!("/sys/class/net/{name}/mtu");
        std::fs::read_to_string(p).ok()?.trim().parse().ok()
    }

    /// The watcher handle: its `Drop` signals the thread to stop and joins it,
    /// so the netlink / route socket is closed and no thread leaks.
    pub struct Watcher {
        stop: Arc<AtomicBool>,
        join: Option<JoinHandle<()>>,
    }

    impl Watcher {
        pub fn start(state: Arc<NetEventState>, iface: Option<String>) -> (Self, &'static str) {
            let stop = Arc::new(AtomicBool::new(false));
            let join = spawn(Arc::clone(&state), Arc::clone(&stop), iface);
            (Self { stop, join }, BACKEND)
        }
    }

    impl Drop for Watcher {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(j) = self.join.take() {
                // The thread polls the stop flag on a sub-second receive
                // timeout, so the join completes within one timeout window.
                j.join().ok();
            }
        }
    }

    #[cfg(target_os = "linux")]
    const BACKEND: &str = "linux-netlink";
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    const BACKEND: &str = "bsd-pf-route";

    /// 250 ms receive timeout: long enough that the blocking `recv` spends
    /// almost all its time parked, short enough that a stop request is honored
    /// promptly.
    const RECV_TIMEOUT_US: i64 = 250_000;

    /// Set `SO_RCVTIMEO` so the blocking `recv` wakes periodically to check the
    /// stop flag instead of blocking forever.
    ///
    /// # Safety
    /// `fd` must be a valid socket file descriptor.
    unsafe fn set_recv_timeout(fd: i32) {
        let tv = libc::timeval {
            tv_sec: 0,
            tv_usec: RECV_TIMEOUT_US as libc::suseconds_t,
        };
        // SAFETY: `tv` is a valid timeval that outlives the call; `fd` is a
        // valid socket.
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVTIMEO,
                &tv as *const libc::timeval as *const libc::c_void,
                size_of::<libc::timeval>() as libc::socklen_t,
            );
        }
    }

    /// Open the OS path-notification socket: a bound `NETLINK_ROUTE` socket on
    /// Linux. `None` on any error.
    ///
    /// # Safety
    /// The returned fd is owned by the caller, which must close it.
    #[cfg(target_os = "linux")]
    unsafe fn open_event_socket() -> Option<i32> {
        // Subscribe to link (carrier), address, and route changes for IPv4 and
        // IPv6: every path event the controller cares about flows through one
        // of these multicast groups.
        const RTMGRP_LINK: u32 = 1;
        const RTMGRP_IPV4_IFADDR: u32 = 0x10;
        const RTMGRP_IPV4_ROUTE: u32 = 0x40;
        const RTMGRP_IPV6_IFADDR: u32 = 0x100;
        const RTMGRP_IPV6_ROUTE: u32 = 0x400;
        // SAFETY: a zeroed sockaddr_nl is a valid bind address; the socket is
        // closed by the caller on every error path.
        unsafe {
            let fd = libc::socket(libc::AF_NETLINK, libc::SOCK_RAW, libc::NETLINK_ROUTE);
            if fd < 0 {
                return None;
            }
            let mut addr: libc::sockaddr_nl = std::mem::zeroed();
            addr.nl_family = libc::AF_NETLINK as u16;
            addr.nl_groups = RTMGRP_LINK
                | RTMGRP_IPV4_IFADDR
                | RTMGRP_IPV4_ROUTE
                | RTMGRP_IPV6_IFADDR
                | RTMGRP_IPV6_ROUTE;
            if libc::bind(
                fd,
                &addr as *const _ as *const libc::sockaddr,
                size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            ) < 0
            {
                libc::close(fd);
                return None;
            }
            set_recv_timeout(fd);
            Some(fd)
        }
    }

    /// A `PF_ROUTE` raw socket delivers every routing-table change to a
    /// listener with no explicit subscription (the BSDs / macOS).
    ///
    /// # Safety
    /// The returned fd is owned by the caller, which must close it.
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    unsafe fn open_event_socket() -> Option<i32> {
        // SAFETY: a PF_ROUTE raw socket needs no bind; closed by the caller.
        unsafe {
            let fd = libc::socket(libc::PF_ROUTE, libc::SOCK_RAW, 0);
            if fd < 0 {
                return None;
            }
            set_recv_timeout(fd);
            Some(fd)
        }
    }

    /// The BSDs report route / carrier events here; the MTU read is a Linux /
    /// Windows capability, so `pmtu` stays `None` on these targets.
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    fn read_iface_mtu_after_event(_iface: &Option<String>) -> Option<u16> {
        None
    }

    #[cfg(target_os = "linux")]
    fn read_iface_mtu_after_event(iface: &Option<String>) -> Option<u16> {
        read_iface_mtu_linux(iface.as_deref())
    }

    /// Spawn the watcher thread: block on the OS notification socket and, on
    /// each delivered change, record one path event and refresh the MTU. On a
    /// receive timeout it checks the stop flag and loops. Returns `None` if the
    /// socket could not be opened (the observer then simply never fires).
    fn spawn(
        state: Arc<NetEventState>,
        stop: Arc<AtomicBool>,
        iface: Option<String>,
    ) -> Option<JoinHandle<()>> {
        // SAFETY: open_event_socket returns an owned fd; the thread closes it.
        let fd = unsafe { open_event_socket() }?;
        std::thread::Builder::new()
            .name("net-events".into())
            .spawn(move || {
                let mut buf = vec![0u8; 8192];
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    // SAFETY: `buf` is a valid, owned 8192-byte buffer; `fd` is
                    // the socket opened above and not closed until after the
                    // loop.
                    let n = unsafe {
                        libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0)
                    };
                    if n > 0 {
                        // Any message on these groups is a path change. We do
                        // not parse it: the controller wants "the path moved",
                        // not which route. Record one event per delivered batch
                        // and refresh the MTU (a decrease is implicit in the
                        // event already counted).
                        state.record_event();
                        if let Some(mtu) = read_iface_mtu_after_event(&iface) {
                            state.set_pmtu(mtu);
                        }
                    }
                    // n <= 0 is a timeout (SO_RCVTIMEO) or a transient error;
                    // either way, loop back and re-check the stop flag.
                }
                // SAFETY: `fd` was opened above and is closed exactly once here,
                // after the receive loop has finished using it.
                unsafe {
                    libc::close(fd);
                }
            })
            .ok()
    }
}

#[cfg(target_os = "windows")]
mod windows_watch {
    use super::NetEventState;
    use std::ffi::c_void;
    use std::ptr;
    use std::sync::Arc;
    use windows_sys::Win32::Foundation::{BOOLEAN, HANDLE};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        CancelMibChangeNotify2, FreeMibTable, GetIfTable2, NotifyIpInterfaceChange,
        NotifyRouteChange2, MIB_IF_TABLE2, MIB_IPFORWARD_ROW2, MIB_IPINTERFACE_ROW,
        MIB_NOTIFICATION_TYPE,
    };
    use windows_sys::Win32::Networking::WinSock::AF_UNSPEC;

    /// `IF_OPER_STATUS` value for an interface that is up.
    const IF_OPER_STATUS_UP: i32 = 1;
    /// `IFTYPE` value for a software loopback interface (skipped).
    const IF_TYPE_SOFTWARE_LOOPBACK: u32 = 24;

    /// Read the MTU of the busiest up, non-loopback adapter via `GetIfTable2`.
    pub fn read_iface_mtu_win() -> Option<u16> {
        // SAFETY: GetIfTable2 allocates the table; every row is read within
        // `NumEntries`, the table is freed exactly once, and no pointer
        // outlives the call.
        unsafe {
            let mut table: *mut MIB_IF_TABLE2 = ptr::null_mut();
            if GetIfTable2(&mut table) != 0 || table.is_null() {
                return None;
            }
            let n = (*table).NumEntries as usize;
            let rows = &raw const (*table).Table[0];
            let (mut best_pkts, mut best_mtu, mut found) = (0u64, 0u32, false);
            for i in 0..n {
                let row = &*rows.add(i);
                if row.OperStatus != IF_OPER_STATUS_UP || row.Type == IF_TYPE_SOFTWARE_LOOPBACK {
                    continue;
                }
                let pkts = row.InUcastPkts.saturating_add(row.OutUcastPkts);
                if !found || pkts > best_pkts {
                    best_pkts = pkts;
                    best_mtu = row.Mtu;
                    found = true;
                }
            }
            FreeMibTable(table as *const c_void);
            (found && best_mtu != 0).then_some(best_mtu.min(u16::MAX as u32) as u16)
        }
    }

    /// Handle a route or interface change: record one path event and refresh
    /// the MTU. `ctx` is the `Arc<NetEventState>` pointer handed to the OS at
    /// registration; the observer keeps that `Arc` alive and cancels the
    /// callbacks before dropping it, so the pointer is valid for every call.
    /// Does only atomic stores and a `GetIfTable2` read, so it cannot unwind
    /// across the FFI boundary.
    ///
    /// # Safety
    /// `ctx` must be the live `*const NetEventState` passed to the notify call.
    unsafe fn on_change(ctx: *const c_void) {
        if ctx.is_null() {
            return;
        }
        // SAFETY: the observer holds the Arc and cancels notifications before
        // releasing it, so the state outlives every callback.
        let state = unsafe { &*(ctx as *const NetEventState) };
        state.record_event();
        if let Some(mtu) = read_iface_mtu_win() {
            state.set_pmtu(mtu);
        }
    }

    /// `NotifyRouteChange2` callback: a route-table entry changed.
    ///
    /// # Safety
    /// Invoked by the OS with the context registered below.
    unsafe extern "system" fn route_cb(
        ctx: *const c_void,
        _row: *const MIB_IPFORWARD_ROW2,
        _ty: MIB_NOTIFICATION_TYPE,
    ) {
        // SAFETY: `ctx` is the registered NetEventState pointer.
        unsafe { on_change(ctx) }
    }

    /// `NotifyIpInterfaceChange` callback: an interface property (carrier,
    /// MTU) changed.
    ///
    /// # Safety
    /// Invoked by the OS with the context registered below.
    unsafe extern "system" fn iface_cb(
        ctx: *const c_void,
        _row: *const MIB_IPINTERFACE_ROW,
        _ty: MIB_NOTIFICATION_TYPE,
    ) {
        // SAFETY: `ctx` is the registered NetEventState pointer.
        unsafe { on_change(ctx) }
    }

    /// The watcher handle: keeps the `Arc` alive for the callbacks and, on
    /// `Drop`, cancels both notifications before the `Arc` is released so no
    /// callback can fire against freed state.
    pub struct Watcher {
        _state: Arc<NetEventState>,
        route_handle: HANDLE,
        iface_handle: HANDLE,
    }

    // The handles are opaque OS tokens used only to cancel; sending the watcher
    // across threads is sound because the callbacks reference the Arc'd state,
    // not the handle.
    unsafe impl Send for Watcher {}
    unsafe impl Sync for Watcher {}

    impl Watcher {
        pub fn start(state: Arc<NetEventState>) -> (Self, &'static str) {
            // The context is the stable heap address of the shared state; the
            // Arc kept in `_state` below keeps it alive, and Drop cancels the
            // callbacks before that Arc is released.
            let ctx = Arc::as_ptr(&state) as *const c_void;
            let mut route_handle: HANDLE = ptr::null_mut();
            let mut iface_handle: HANDLE = ptr::null_mut();
            // SAFETY: valid callback pointers and a stable context; the output
            // handles are owned by this Watcher and cancelled in Drop. The
            // `FALSE` initial-notification flag means no callback fires before
            // a real change.
            unsafe {
                NotifyRouteChange2(
                    AF_UNSPEC,
                    Some(route_cb),
                    ctx,
                    0 as BOOLEAN,
                    &mut route_handle,
                );
                NotifyIpInterfaceChange(
                    AF_UNSPEC,
                    Some(iface_cb),
                    ctx,
                    0 as BOOLEAN,
                    &mut iface_handle,
                );
            }
            (
                Self {
                    _state: state,
                    route_handle,
                    iface_handle,
                },
                "windows-notify",
            )
        }
    }

    impl Drop for Watcher {
        fn drop(&mut self) {
            // Cancel both notifications FIRST, so no in-flight callback can run
            // after the Arc'd state is released. CancelMibChangeNotify2 blocks
            // until any running callback returns. `_state` then drops once the
            // callbacks are guaranteed quiesced.
            // SAFETY: each handle was produced by the matching notify call.
            unsafe {
                if !self.route_handle.is_null() {
                    CancelMibChangeNotify2(self.route_handle);
                }
                if !self.iface_handle.is_null() {
                    CancelMibChangeNotify2(self.iface_handle);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decay_halves_each_half_life() {
        assert!((decayed_shift(0.0) - 1.0).abs() < 1e-6, "spike at the event");
        assert!(
            (decayed_shift(SHIFT_HALF_LIFE_SECS) - 0.5).abs() < 1e-6,
            "halves after one half-life"
        );
        assert!(
            decayed_shift(3.0 * SHIFT_HALF_LIFE_SECS) < 0.15,
            "well decayed after three half-lives"
        );
    }

    #[test]
    fn fresh_observer_reports_no_shift() {
        let st = NetEventState::new();
        assert_eq!(st.event_count(), 0);
        assert_eq!(st.path_shift(), 0.0, "no event -> no shift");
        assert_eq!(st.pmtu(), None);
    }

    #[test]
    fn an_event_spikes_the_shift() {
        let st = NetEventState::new();
        st.record_event();
        assert_eq!(st.event_count(), 1);
        assert!(st.path_shift() > 0.9, "shift spikes right after an event");
    }

    #[test]
    fn a_pmtu_drop_is_a_path_event() {
        let st = NetEventState::new();
        // First reading: just establishes the baseline, not an event.
        st.note_pmtu(1500);
        assert_eq!(st.event_count(), 0, "first MTU reading is not an event");
        assert_eq!(st.pmtu(), Some(1500));
        // A drop is a path event.
        st.note_pmtu(1280);
        assert_eq!(st.event_count(), 1, "an MTU drop records an event");
        assert_eq!(st.pmtu(), Some(1280));
        assert!(st.path_shift() > 0.9, "the drop spikes the shift");
        // A rise (back to a higher MTU) is not a fresh path-degradation event.
        st.note_pmtu(1500);
        assert_eq!(st.event_count(), 1, "an MTU rise is not a new drop event");
        assert_eq!(st.pmtu(), Some(1500));
    }

    #[test]
    fn set_pmtu_never_records_an_event() {
        // The OS watcher path: the kernel message already counted the event, so
        // refreshing the MTU value must not double-count.
        let st = NetEventState::new();
        st.set_pmtu(1500);
        st.set_pmtu(1280);
        st.set_pmtu(1500);
        assert_eq!(st.event_count(), 0, "set_pmtu is value-only");
        assert_eq!(st.pmtu(), Some(1500), "last value wins, no event");
    }

    #[test]
    fn observer_starts_and_stops_without_panicking() {
        // Whatever backend this platform builds, starting and dropping the
        // observer must be safe, and an injected event must register.
        let obs = NetEventObserver::start(None);
        assert!(!obs.backend().is_empty());
        assert_eq!(obs.event_count(), 0);
        obs.inject_event();
        assert_eq!(obs.event_count(), 1);
        assert!(obs.path_shift() > 0.9);
        // Dropping joins the watcher thread / cancels the callbacks cleanly.
        drop(obs);
    }
}
