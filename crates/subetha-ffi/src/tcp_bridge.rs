//! A ring carried across a TCP connection, through the C ABI.
//!
//! The client half pulls slots from a local producer ring and ships them;
//! the server half accepts one connection and pushes what arrives into a
//! local consumer ring. Between them a ring on one machine feeds a ring
//! on another, and the caller on each side goes on using the ring's own
//! entry points.
//!
//! # These entry points exist in every build
//!
//! The transports pull tokio, quinn and rustls, which an embedder that
//! only wants the memory-mapped primitives has no use for, so they sit
//! behind the `tcp-bridge` feature. The symbols are here either way: a
//! library built without it answers `SUBETHA_E_NOT_SUPPORTED` naming the
//! feature, so `include/subetha.h` and `subetha.def` stay one artifact
//! and a C program links against any build.
//! `subetha_transports_available` reports which kind of build this is.
//!
//! # Strict blocks, managed owns a thread
//!
//! The primitive underneath is asynchronous, and strict mode promises no
//! thread the caller did not ask for. In strict mode a run drives a
//! current-thread runtime on the calling thread: it blocks, and the
//! caller asked for that by calling it. In managed mode the object owns a
//! runtime thread, the call returns at once, and `read_stats` says how
//! far it has got.

use std::ffi::c_char;

use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK};
#[cfg(not(feature = "tcp-bridge"))]
use crate::error::SUBETHA_E_NOT_SUPPORTED;
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_TCP_BRIDGE};
use crate::runtime::{entry, require_initialized, with_kind};

/// The half that ships from a local producer ring.
pub const SUBETHA_BRIDGE_CLIENT: u32 = 0;
/// The half that binds, accepts one connection and receives.
pub const SUBETHA_BRIDGE_SERVER: u32 = 1;

/// A snapshot of a bridge.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_tcp_bridge_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// One of `SUBETHA_BRIDGE_CLIENT` or `SUBETHA_BRIDGE_SERVER`.
    pub role: u32,
    /// Items shipped or received so far.
    pub items: u64,
    /// Whether a run is under way right now.
    pub running: bool,
    /// Whether the last run finished.
    pub finished: bool,
    /// The code the last run ended with, or `SUBETHA_OK`.
    pub last_code: i32,
}

/// What a build without the feature answers, naming the feature so the
/// caller is told what to turn on rather than that the call is unknown.
#[cfg(not(feature = "tcp-bridge"))]
fn unavailable() -> i32 {
    fail(SUBETHA_E_NOT_SUPPORTED, "this library was built without the tcp-bridge feature")
}

#[cfg(feature = "tcp-bridge")]
pub(crate) use built::TcpBridgeObject;
#[cfg(not(feature = "tcp-bridge"))]
pub(crate) use stub::TcpBridgeObject;

/// The object a handle would name in a build with no transports. Nothing
/// constructs it, since every constructor refuses first, and its field
/// makes that unconstructable rather than merely unused.
#[cfg(not(feature = "tcp-bridge"))]
mod stub {
    pub(crate) struct TcpBridgeObject {
        never: std::convert::Infallible,
    }

    impl TcpBridgeObject {
        pub(crate) fn interrupt(&self) {
            match self.never {}
        }
    }
}

fn with_bridge(handle: subetha_handle, f: impl FnOnce(&TcpBridgeObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_TCP_BRIDGE, |object| match object {
        Object::TcpBridge(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a TCP bridge"),
    })
}

/// Make the client half: it pulls from the ring `ring` names and ships to
/// `addr`, a host and port such as `127.0.0.1:9000`.
///
/// # Safety
/// `addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tcp_bridge_client(
    ring: subetha_handle,
    addr: *const c_char,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        #[cfg(feature = "tcp-bridge")]
        {
            unsafe { built::client(ring, addr, mode, out) }
        }
        #[cfg(not(feature = "tcp-bridge"))]
        {
            let _unused = (ring, addr, mode, out);
            unavailable()
        }
    })
}

/// Make the server half: it binds `addr`, accepts one connection and
/// pushes what arrives into the ring `ring` names. Port zero binds a port
/// the system chooses, which `subetha_tcp_bridge_local_port` reports.
///
/// # Safety
/// `addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tcp_bridge_server(
    ring: subetha_handle,
    addr: *const c_char,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        #[cfg(feature = "tcp-bridge")]
        {
            unsafe { built::server(ring, addr, mode, out) }
        }
        #[cfg(not(feature = "tcp-bridge"))]
        {
            let _unused = (ring, addr, mode, out);
            unavailable()
        }
    })
}

/// The port the server half listens on, into `out`. For a server bound to
/// port zero this is the port the system chose.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tcp_bridge_local_port(handle: subetha_handle, out: *mut u16) -> i32 {
    with_bridge(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        #[cfg(feature = "tcp-bridge")]
        {
            unsafe { built::local_port(b, out) }
        }
        #[cfg(not(feature = "tcp-bridge"))]
        {
            let _unused = (b, out);
            unavailable()
        }
    })
}

/// Carry `items` slots across the connection.
///
/// In strict mode this blocks the calling thread until the transfer ends
/// or `timeout_ms` elapses, driving the work on that thread. In managed
/// mode it returns at once and the object's own thread carries the
/// transfer, which `subetha_tcp_bridge_read_stats` reports on.
///
/// A negative `timeout_ms` waits without a deadline.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_tcp_bridge_run(handle: subetha_handle, items: u64, timeout_ms: i64) -> i32 {
    with_bridge(handle, |b| {
        #[cfg(feature = "tcp-bridge")]
        {
            built::run(b, items, timeout_ms)
        }
        #[cfg(not(feature = "tcp-bridge"))]
        {
            let _unused = (b, items, timeout_ms);
            unavailable()
        }
    })
}

/// A snapshot of the bridge into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_tcp_bridge_read_stats(
    handle: subetha_handle,
    out: *mut subetha_tcp_bridge_stats,
) -> i32 {
    with_bridge(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        #[cfg(feature = "tcp-bridge")]
        {
            unsafe { built::read_stats(b, out) }
        }
        #[cfg(not(feature = "tcp-bridge"))]
        {
            let _unused = (b, out);
            unavailable()
        }
    })
}

#[cfg(feature = "tcp-bridge")]
mod built {
    use std::ffi::c_char;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::Mutex;
    use subetha_cxc::tcp_bridge::{TcpBridgeClient, TcpBridgeError, TcpBridgeServer};

    use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_TIMEOUT, SUBETHA_OK};
    use crate::handle::{subetha_handle, Object};
    use crate::ring::text;
    use crate::runtime::{issue, resolve_mode, with_ring, SUBETHA_MODE_MANAGED};

    use super::{subetha_tcp_bridge_stats, SUBETHA_BRIDGE_CLIENT, SUBETHA_BRIDGE_SERVER};

    enum Half {
        Client(TcpBridgeClient),
        /// The listener and the runtime it is registered with, the only
        /// one that can drive its accept. The listener is declared first
        /// so it is dropped while the driver it is registered with is
        /// still there.
        Server {
            bridge: TcpBridgeServer,
            runtime: tokio::runtime::Runtime,
        },
    }

    /// What a run has done so far, shared with the thread carrying it in
    /// managed mode so a snapshot reads the same words either way.
    #[derive(Default)]
    struct Progress {
        items: AtomicU64,
        running: AtomicBool,
        finished: AtomicBool,
        last_code: AtomicI32,
    }

    pub(crate) struct TcpBridgeObject {
        half: Arc<Half>,
        mode: u32,
        role: u32,
        progress: Arc<Progress>,
        /// The thread a managed run is on, joined when the handle is
        /// destroyed so a run never outlives the object it reports to.
        worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    }

    impl TcpBridgeObject {
        /// A run in flight is on a socket this cannot interrupt, so the
        /// destroy joins it rather than abandoning it.
        pub(crate) fn interrupt(&self) {
            if let Some(handle) = self.worker.lock().take()
                && handle.join().is_err()
            {
                // The run's own code is already in `last_code`; a panic
                // leaves none, so it is said here rather than nowhere.
                eprintln!("subetha: a TCP bridge run panicked and left no code");
            }
        }
    }

    impl Drop for TcpBridgeObject {
        fn drop(&mut self) {
            self.interrupt();
        }
    }

    fn code_for(e: &TcpBridgeError) -> i32 {
        match e {
            TcpBridgeError::Io(io) => fail(SUBETHA_E_RING_IO, format!("io error: {}", io.kind())),
            TcpBridgeError::Closed => fail(SUBETHA_E_RING_IO, "the connection closed"),
        }
    }

    /// A runtime that runs on the calling thread. Strict mode drives every
    /// await on the thread that called, so the library starts none.
    fn here() -> Result<tokio::runtime::Runtime, i32> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| fail(SUBETHA_E_RING_IO, format!("no runtime: {e}")))
    }

    /// # Safety
    /// `addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
    pub(crate) unsafe fn client(
        ring: subetha_handle,
        addr: *const c_char,
        mode: u32,
        out: *mut subetha_handle,
    ) -> i32 {
        let (socket, mode) = match unsafe { arguments(addr, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let mut taken = None;
        let code = with_ring(ring, |r| {
            taken = Some(Arc::clone(&r.ring));
            SUBETHA_OK
        });
        let Some(producer) = taken else { return code };
        let object = TcpBridgeObject {
            half: Arc::new(Half::Client(TcpBridgeClient::new(producer, socket))),
            mode,
            role: SUBETHA_BRIDGE_CLIENT,
            progress: Arc::new(Progress::default()),
            worker: Mutex::new(None),
        };
        unsafe { issue(Object::TcpBridge(object), out) }
    }

    /// # Safety
    /// `addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
    pub(crate) unsafe fn server(
        ring: subetha_handle,
        addr: *const c_char,
        mode: u32,
        out: *mut subetha_handle,
    ) -> i32 {
        let (socket, mode) = match unsafe { arguments(addr, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let mut taken = None;
        let code = with_ring(ring, |r| {
            taken = Some(Arc::clone(&r.ring));
            SUBETHA_OK
        });
        let Some(consumer) = taken else { return code };
        // The bind registers the listener with this runtime's driver, so
        // the runtime stays with the listener and the accept is driven on
        // it by whichever thread carries the run. It is driven only while
        // a run is, so between runs the object holds no thread.
        let runtime = match here() {
            Ok(rt) => rt,
            Err(code) => return code,
        };
        let bridge = match runtime.block_on(TcpBridgeServer::bind(consumer, socket)) {
            Ok(s) => s,
            Err(e) => return code_for(&e),
        };
        let object = TcpBridgeObject {
            half: Arc::new(Half::Server { bridge, runtime }),
            mode,
            role: SUBETHA_BRIDGE_SERVER,
            progress: Arc::new(Progress::default()),
            worker: Mutex::new(None),
        };
        unsafe { issue(Object::TcpBridge(object), out) }
    }

    /// # Safety
    /// `addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
    unsafe fn arguments(
        addr: *const c_char,
        mode: u32,
        out: *mut subetha_handle,
    ) -> Result<(SocketAddr, u32), i32> {
        if out.is_null() {
            return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
        }
        let text = unsafe { text(addr, "addr") }?;
        let socket: SocketAddr = text
            .parse()
            .map_err(|e| fail(SUBETHA_E_INVALID_ARGUMENT, format!("addr {text}: {e}")))?;
        let mode = resolve_mode(mode)?;
        Ok((socket, mode))
    }

    /// # Safety
    /// `out` is a valid pointer, checked by the caller.
    pub(crate) unsafe fn local_port(b: &TcpBridgeObject, out: *mut u16) -> i32 {
        match &*b.half {
            Half::Server { bridge, .. } => match bridge.local_addr() {
                Ok(addr) => {
                    // SAFETY: the caller checked it is non-null and writable.
                    unsafe { *out = addr.port() };
                    SUBETHA_OK
                }
                Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
            },
            Half::Client(_) => {
                fail(SUBETHA_E_INVALID_ARGUMENT, "a client half listens on no port")
            }
        }
    }

    /// One transfer, on this thread or on the object's own. A client
    /// connects inside the run, so a runtime of the call's own carries it;
    /// a server's accept runs on the runtime its listener was bound in.
    fn carry(half: &Half, items: u64, timeout: Option<Duration>) -> i32 {
        match half {
            Half::Client(c) => match here() {
                Ok(rt) => rt.block_on(bounded(
                    async { c.run(items).await.map(|()| items) },
                    timeout,
                )),
                Err(code) => code,
            },
            Half::Server { bridge, runtime } => {
                runtime.block_on(bounded(bridge.accept_one(), timeout))
            }
        }
    }

    /// `work` under the caller's deadline, as a code.
    async fn bounded(
        work: impl std::future::Future<Output = Result<u64, TcpBridgeError>>,
        timeout: Option<Duration>,
    ) -> i32 {
        match timeout {
            Some(d) => match tokio::time::timeout(d, work).await {
                Ok(Ok(_)) => SUBETHA_OK,
                Ok(Err(e)) => code_for(&e),
                Err(_elapsed) => SUBETHA_E_TIMEOUT,
            },
            None => match work.await {
                Ok(_) => SUBETHA_OK,
                Err(e) => code_for(&e),
            },
        }
    }

    pub(crate) fn run(b: &TcpBridgeObject, items: u64, timeout_ms: i64) -> i32 {
        if b.progress.running.load(Ordering::Acquire) {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "a run is already under way on this bridge");
        }
        let timeout = if timeout_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_ms as u64))
        };
        b.progress.running.store(true, Ordering::Release);
        b.progress.finished.store(false, Ordering::Release);

        if b.mode != SUBETHA_MODE_MANAGED {
            let code = carry(&b.half, items, timeout);
            b.progress.items.store(items, Ordering::Release);
            b.progress.last_code.store(code, Ordering::Release);
            b.progress.finished.store(true, Ordering::Release);
            b.progress.running.store(false, Ordering::Release);
            return code;
        }

        let half = Arc::clone(&b.half);
        let progress = Arc::clone(&b.progress);
        let worker = std::thread::Builder::new()
            .name("subetha-tcp-bridge".to_owned())
            .spawn(move || {
                let code = carry(&half, items, timeout);
                progress.items.store(items, Ordering::Release);
                progress.last_code.store(code, Ordering::Release);
                progress.finished.store(true, Ordering::Release);
                progress.running.store(false, Ordering::Release);
            });
        match worker {
            Ok(handle) => {
                *b.worker.lock() = Some(handle);
                SUBETHA_OK
            }
            Err(e) => {
                b.progress.running.store(false, Ordering::Release);
                fail(SUBETHA_E_RING_IO, format!("no thread for the run: {e}"))
            }
        }
    }

    /// # Safety
    /// `out` is a valid pointer, checked by the caller.
    pub(crate) unsafe fn read_stats(b: &TcpBridgeObject, out: *mut subetha_tcp_bridge_stats) -> i32 {
        let stats = subetha_tcp_bridge_stats {
            mode: b.mode,
            role: b.role,
            items: b.progress.items.load(Ordering::Acquire),
            running: b.progress.running.load(Ordering::Acquire),
            finished: b.progress.finished.load(Ordering::Acquire),
            last_code: b.progress.last_code.load(Ordering::Acquire),
        };
        // SAFETY: the caller checked it is non-null and writable.
        unsafe { *out = stats };
        SUBETHA_OK
    }

}

/// Whether this library carries the transports, into `out`. A caller
/// choosing a path at run time asks this rather than making a call and
/// reading the refusal.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_transports_available(out: *mut bool) -> i32 {
    entry(|| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let available = cfg!(feature = "tcp-bridge");
        // SAFETY: checked non-null; the caller guarantees it is writable.
        unsafe { *out = available };
        SUBETHA_OK
    })
}
