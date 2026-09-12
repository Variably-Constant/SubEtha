//! A blocking single-producer single-consumer ring carried across a
//! socket connection, through the C ABI.
//!
//! The client half parks on a local blocking ring until an item arrives,
//! drains what is there and ships it; the server half accepts one
//! connection and pushes what arrives into a local blocking ring, parking
//! when that ring is full. Nothing is dropped: a consumer that stops
//! draining ends the run with an error rather than a discard, which is
//! what sets this family apart from `subetha_tcp_bridge_`, whose halves
//! ride an adaptive ring and yield rather than park.
//!
//! # These entry points exist in every build
//!
//! The transports sit behind the `tcp-bridge` feature, and the symbols
//! are here either way: a library built without it answers
//! `SUBETHA_E_NOT_SUPPORTED` naming the feature, so `include/subetha.h`
//! and `subetha.def` stay one artifact and a C program links against any
//! build. `subetha_transports_available` reports which kind of build this
//! is.
//!
//! # Strict blocks, managed owns a thread
//!
//! The primitive underneath is asynchronous, and strict mode promises no
//! thread the caller did not ask for. In strict mode a run drives a
//! current-thread runtime on the calling thread: it blocks, and the
//! caller asked for that by calling it. In managed mode the object owns a
//! runtime thread, the call returns at once, and `read_stats` says how
//! far it has got.
//!
//! The client half connects inside the run, so each run builds a runtime
//! and ends with it. The server half binds its listener when it is made,
//! and a socket is readable only from the runtime whose driver it is
//! registered with, so the server keeps that runtime for its life and
//! every accept is driven on it by whichever thread carries the run. At
//! rest it holds no thread: a thread its blocking pool starts to park on
//! a full ring exits as soon as that wait is over.

use std::ffi::c_char;

use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND};
#[cfg(not(feature = "tcp-bridge"))]
use crate::error::SUBETHA_E_NOT_SUPPORTED;
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_BLOCKING_TCP_BRIDGE};
use crate::runtime::{entry, require_initialized, with_kind};

/// A snapshot of a blocking bridge.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_blocking_tcp_bridge_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// One of `SUBETHA_BRIDGE_CLIENT` or `SUBETHA_BRIDGE_SERVER`.
    pub role: u32,
    /// Items the last finished run carried. A run that ended in an error
    /// reports zero, since the primitive counts no partial transfer.
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
pub(crate) use built::BlockingTcpBridgeObject;
#[cfg(not(feature = "tcp-bridge"))]
pub(crate) use stub::BlockingTcpBridgeObject;

/// The object a handle would name in a build with no transports. Nothing
/// constructs it, since every constructor refuses first, and its field
/// makes that unconstructable rather than merely unused.
#[cfg(not(feature = "tcp-bridge"))]
mod stub {
    pub(crate) struct BlockingTcpBridgeObject {
        never: std::convert::Infallible,
    }

    impl BlockingTcpBridgeObject {
        pub(crate) fn interrupt(&self) {
            match self.never {}
        }
    }
}

fn with_bridge(handle: subetha_handle, f: impl FnOnce(&BlockingTcpBridgeObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_BLOCKING_TCP_BRIDGE, |object| match object {
        Object::BlockingTcpBridge(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a blocking bridge"),
    })
}

/// Make the client half: it parks on the blocking SPSC ring `ring` names
/// until an item arrives, drains what is there, and ships it to `addr`, a
/// host and port such as `127.0.0.1:9000`.
///
/// # Safety
/// `addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocking_tcp_bridge_client(
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
/// pushes what arrives into the blocking SPSC ring `ring` names, parking
/// while that ring is full. Port zero binds a port the system chooses,
/// which `subetha_blocking_tcp_bridge_local_port` reports.
///
/// # Safety
/// `addr` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_blocking_tcp_bridge_server(
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
pub unsafe extern "C" fn subetha_blocking_tcp_bridge_local_port(handle: subetha_handle, out: *mut u16) -> i32 {
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
/// transfer, which `subetha_blocking_tcp_bridge_read_stats` reports on.
///
/// A negative `timeout_ms` waits without a deadline.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_blocking_tcp_bridge_run(handle: subetha_handle, items: u64, timeout_ms: i64) -> i32 {
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
pub unsafe extern "C" fn subetha_blocking_tcp_bridge_read_stats(
    handle: subetha_handle,
    out: *mut subetha_blocking_tcp_bridge_stats,
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
    use subetha_cxc::blocking_tcp_bridge::{
        BlockingTcpBridgeClient, BlockingTcpBridgeError, BlockingTcpBridgeServer,
    };

    use crate::error::{
        blocking_code, fail, ring_code, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_TIMEOUT,
        SUBETHA_OK,
    };
    use crate::handle::{subetha_handle, Object};
    use crate::ring::text;
    use crate::runtime::{issue, resolve_mode, SUBETHA_MODE_MANAGED};
    use crate::spsc::with_spsc;
    use crate::tcp_bridge::{SUBETHA_BRIDGE_CLIENT, SUBETHA_BRIDGE_SERVER};

    use super::subetha_blocking_tcp_bridge_stats;

    enum Half {
        Client(BlockingTcpBridgeClient),
        /// The listener and the runtime it is registered with, the only
        /// one that can drive its accept. The listener is declared first
        /// so it is dropped while the driver it is registered with is
        /// still there.
        Server {
            bridge: BlockingTcpBridgeServer,
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

    pub(crate) struct BlockingTcpBridgeObject {
        half: Arc<Half>,
        mode: u32,
        role: u32,
        progress: Arc<Progress>,
        /// The thread a managed run is on, joined when the handle is
        /// destroyed so a run never outlives the object it reports to.
        worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    }

    impl BlockingTcpBridgeObject {
        /// A run in flight is on a socket this cannot interrupt, so the
        /// destroy joins it rather than abandoning it.
        pub(crate) fn interrupt(&self) {
            if let Some(handle) = self.worker.lock().take()
                && handle.join().is_err()
            {
                // The run's own code is already in `last_code`; a panic
                // leaves none, so it is said here rather than nowhere.
                eprintln!("subetha: a blocking bridge run panicked and left no code");
            }
        }
    }

    impl Drop for BlockingTcpBridgeObject {
        fn drop(&mut self) {
            self.interrupt();
        }
    }

    /// The code that names what ended a run. A ring error and a blocking
    /// error keep the codes their own families use, so a full ring under
    /// a stopped consumer reads as the timeout it is.
    fn code_for(e: BlockingTcpBridgeError) -> i32 {
        match e {
            BlockingTcpBridgeError::Io(io) => fail(SUBETHA_E_RING_IO, format!("io error: {}", io.kind())),
            BlockingTcpBridgeError::Blocking(e) => blocking_code(e),
            BlockingTcpBridgeError::Ring(e) => ring_code(e),
            BlockingTcpBridgeError::Closed => fail(SUBETHA_E_RING_IO, "the connection closed"),
        }
    }

    /// A runtime that runs on the calling thread. Strict mode drives every
    /// await on the thread that called, so the library starts none.
    fn here() -> Result<tokio::runtime::Runtime, i32> {
        build(tokio::runtime::Builder::new_current_thread().enable_all())
    }

    /// The runtime a server keeps for its life. Its blocking pool parks on
    /// a full ring, and a thread it started for that exits as soon as the
    /// wait is over, so between runs the object holds no thread.
    fn kept() -> Result<tokio::runtime::Runtime, i32> {
        build(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_keep_alive(Duration::ZERO),
        )
    }

    fn build(builder: &mut tokio::runtime::Builder) -> Result<tokio::runtime::Runtime, i32> {
        builder
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
        // The ring is taken before the bridge object exists: a borrow of
        // the handle table does not nest, so nothing else may be held
        // while it is asked for.
        let mut taken = None;
        let code = with_spsc(ring, |s| {
            taken = Some(s.ring());
            SUBETHA_OK
        });
        let Some(producer) = taken else { return code };
        let object = BlockingTcpBridgeObject {
            half: Arc::new(Half::Client(BlockingTcpBridgeClient::new(producer, socket))),
            mode,
            role: SUBETHA_BRIDGE_CLIENT,
            progress: Arc::new(Progress::default()),
            worker: Mutex::new(None),
        };
        unsafe { issue(Object::BlockingTcpBridge(object), out) }
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
        let code = with_spsc(ring, |s| {
            taken = Some(s.ring());
            SUBETHA_OK
        });
        let Some(consumer) = taken else { return code };
        // The bind registers the listener with this runtime's driver, so
        // the runtime stays with the listener and the accept is driven on
        // it by whichever thread carries the run.
        let runtime = match kept() {
            Ok(rt) => rt,
            Err(code) => return code,
        };
        let bridge = match runtime.block_on(BlockingTcpBridgeServer::bind(consumer, socket)) {
            Ok(s) => s,
            Err(e) => return code_for(e),
        };
        let object = BlockingTcpBridgeObject {
            half: Arc::new(Half::Server { bridge, runtime }),
            mode,
            role: SUBETHA_BRIDGE_SERVER,
            progress: Arc::new(Progress::default()),
            worker: Mutex::new(None),
        };
        unsafe { issue(Object::BlockingTcpBridge(object), out) }
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
    pub(crate) unsafe fn local_port(b: &BlockingTcpBridgeObject, out: *mut u16) -> i32 {
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

    /// One transfer, on this thread or on the object's own: the code it
    /// ended with and the items it carried, which is zero unless it ended
    /// well.
    fn carry(half: &Half, items: u64, timeout: Option<Duration>) -> (i32, u64) {
        match half {
            Half::Client(c) => match here() {
                Ok(rt) => rt.block_on(bounded(async { c.run(items).await.map(|()| items) }, timeout)),
                Err(code) => (code, 0),
            },
            Half::Server { bridge, runtime } => runtime.block_on(bounded(bridge.accept_one(), timeout)),
        }
    }

    /// `work` under the caller's deadline, as a code and a count.
    async fn bounded(
        work: impl std::future::Future<Output = Result<u64, BlockingTcpBridgeError>>,
        timeout: Option<Duration>,
    ) -> (i32, u64) {
        let outcome = match timeout {
            Some(d) => match tokio::time::timeout(d, work).await {
                Ok(outcome) => outcome,
                Err(_elapsed) => return (SUBETHA_E_TIMEOUT, 0),
            },
            None => work.await,
        };
        match outcome {
            Ok(carried) => (SUBETHA_OK, carried),
            Err(e) => (code_for(e), 0),
        }
    }

    pub(crate) fn run(b: &BlockingTcpBridgeObject, items: u64, timeout_ms: i64) -> i32 {
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
            let (code, carried) = carry(&b.half, items, timeout);
            b.progress.items.store(carried, Ordering::Release);
            b.progress.last_code.store(code, Ordering::Release);
            b.progress.finished.store(true, Ordering::Release);
            b.progress.running.store(false, Ordering::Release);
            return code;
        }

        let half = Arc::clone(&b.half);
        let progress = Arc::clone(&b.progress);
        let worker = std::thread::Builder::new()
            .name("subetha-blocking-tcp-bridge".to_owned())
            .spawn(move || {
                let (code, carried) = carry(&half, items, timeout);
                progress.items.store(carried, Ordering::Release);
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
    pub(crate) unsafe fn read_stats(
        b: &BlockingTcpBridgeObject,
        out: *mut subetha_blocking_tcp_bridge_stats,
    ) -> i32 {
        let stats = subetha_blocking_tcp_bridge_stats {
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
