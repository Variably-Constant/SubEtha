//! The QUIC bridge: one half ships a ring's contents to a remote peer
//! over QUIC, the other receives into a ring.
//!
//! Shaped like the TCP bridge, with the one thing QUIC adds: the peers
//! authenticate, so a client has to be told which certificate to trust.
//! `subetha_quic_self_signed_cert` mints a certificate and its key as
//! DER bytes; the server is built from both, the client from the
//! certificate alone. The bytes travel between hosts however the caller
//! already moves configuration.
//!
//! In strict mode a run drives the transfer on the calling thread and
//! returns when it ends. In managed mode the object owns a runtime
//! thread, the call returns at once, and `read_stats` says how far it
//! has got.

use std::ffi::c_char;

#[cfg(not(feature = "quic-bridge"))]
use crate::error::SUBETHA_E_NOT_SUPPORTED;
use crate::error::{fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_WRONG_KIND};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_QUIC_BRIDGE};
use crate::runtime::{entry, require_initialized, with_kind};

/// A snapshot of a QUIC bridge.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_quic_bridge_stats {
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
#[cfg(not(feature = "quic-bridge"))]
fn unavailable() -> i32 {
    fail(SUBETHA_E_NOT_SUPPORTED, "this library was built without the quic-bridge feature")
}

#[cfg(feature = "quic-bridge")]
pub(crate) use built::QuicBridgeObject;
#[cfg(not(feature = "quic-bridge"))]
pub(crate) use stub::QuicBridgeObject;

/// The object a handle would name in a build with no transports. Nothing
/// constructs it, since every constructor refuses first, and its field
/// makes that unconstructable rather than merely unused.
#[cfg(not(feature = "quic-bridge"))]
mod stub {
    pub(crate) struct QuicBridgeObject {
        never: std::convert::Infallible,
    }

    impl QuicBridgeObject {
        pub(crate) fn interrupt(&self) {
            match self.never {}
        }
    }
}

fn with_bridge(handle: subetha_handle, f: impl FnOnce(&QuicBridgeObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_QUIC_BRIDGE, |object| match object {
        Object::QuicBridge(b) => f(b),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a QUIC bridge"),
    })
}

/// Mint a self-signed certificate for `sni` and copy it and its private
/// key out as DER bytes. `cert_len` and `key_len` receive the byte counts
/// the call produced, or the counts it would need when a buffer is too
/// small, in which case the answer is `SUBETHA_E_BUFFER_TOO_SMALL` and
/// nothing is copied.
///
/// The server half needs both; a client half needs the certificate alone,
/// and trusts exactly that one. `sni` names the certificate rather than
/// the wire address, so a peer reached at any address presents it.
///
/// # Safety
/// `sni` is a NUL-terminated UTF-8 string. `cert_out` points to
/// `cert_cap` writable bytes and `key_out` to `key_cap`; both lengths are
/// valid pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_quic_self_signed_cert(
    sni: *const c_char,
    cert_out: *mut u8,
    cert_cap: usize,
    cert_len: *mut usize,
    key_out: *mut u8,
    key_cap: usize,
    key_len: *mut usize,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        #[cfg(feature = "quic-bridge")]
        {
            unsafe {
                built::self_signed(sni, cert_out, cert_cap, cert_len, key_out, key_cap, key_len)
            }
        }
        #[cfg(not(feature = "quic-bridge"))]
        {
            let _unused = (sni, cert_out, cert_cap, cert_len, key_out, key_cap, key_len);
            unavailable()
        }
    })
}

/// Make the client half: it pulls from the ring `ring` names and ships to
/// `server_addr`, presenting `sni` and trusting the certificate in
/// `cert`. `bind_addr` is the local address to send from; `0.0.0.0:0`
/// takes any port the system offers.
///
/// # Safety
/// `server_addr`, `bind_addr` and `sni` are NUL-terminated UTF-8 strings;
/// `cert` points to `cert_len` readable bytes; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_quic_bridge_client(
    ring: subetha_handle,
    server_addr: *const c_char,
    bind_addr: *const c_char,
    sni: *const c_char,
    cert: *const u8,
    cert_len: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        #[cfg(feature = "quic-bridge")]
        {
            unsafe { built::client(ring, server_addr, bind_addr, sni, cert, cert_len, mode, out) }
        }
        #[cfg(not(feature = "quic-bridge"))]
        {
            let _unused = (ring, server_addr, bind_addr, sni, cert, cert_len, mode, out);
            unavailable()
        }
    })
}

/// Make the server half: it binds `addr`, accepts one connection and
/// pushes what arrives into the ring `ring` names, presenting the
/// certificate in `cert` with the key in `key`. Port zero binds a port
/// the system chooses, which `subetha_quic_bridge_local_port` reports.
///
/// # Safety
/// `addr` is a NUL-terminated UTF-8 string; `cert` points to `cert_len`
/// readable bytes and `key` to `key_len`; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_quic_bridge_server(
    ring: subetha_handle,
    addr: *const c_char,
    cert: *const u8,
    cert_len: usize,
    key: *const u8,
    key_len: usize,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        #[cfg(feature = "quic-bridge")]
        {
            unsafe { built::server(ring, addr, cert, cert_len, key, key_len, mode, out) }
        }
        #[cfg(not(feature = "quic-bridge"))]
        {
            let _unused = (ring, addr, cert, cert_len, key, key_len, mode, out);
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
pub unsafe extern "C" fn subetha_quic_bridge_local_port(
    handle: subetha_handle,
    out: *mut u16,
) -> i32 {
    with_bridge(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        #[cfg(feature = "quic-bridge")]
        {
            unsafe { built::local_port(b, out) }
        }
        #[cfg(not(feature = "quic-bridge"))]
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
/// transfer, which `subetha_quic_bridge_read_stats` reports on.
///
/// A negative `timeout_ms` waits without a deadline.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_quic_bridge_run(
    handle: subetha_handle,
    items: u64,
    timeout_ms: i64,
) -> i32 {
    with_bridge(handle, |b| {
        #[cfg(feature = "quic-bridge")]
        {
            built::run(b, items, timeout_ms)
        }
        #[cfg(not(feature = "quic-bridge"))]
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
pub unsafe extern "C" fn subetha_quic_bridge_read_stats(
    handle: subetha_handle,
    out: *mut subetha_quic_bridge_stats,
) -> i32 {
    with_bridge(handle, |b| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        #[cfg(feature = "quic-bridge")]
        {
            unsafe { built::read_stats(b, out) }
        }
        #[cfg(not(feature = "quic-bridge"))]
        {
            let _unused = (b, out);
            unavailable()
        }
    })
}

#[cfg(feature = "quic-bridge")]
mod built {
    use std::ffi::c_char;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::Mutex;
    use subetha_cxc::quic_bridge::{
        generate_self_signed_cert, install_default_crypto_provider, make_client_config_from_der,
        make_server_config_from_der, QuicBridgeClient, QuicBridgeError, QuicBridgeServer,
    };

    use crate::error::{
        fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_TIMEOUT, SUBETHA_OK,
    };
    use crate::handle::{subetha_handle, Object};
    use crate::ring::{copy_out, text};
    use crate::runtime::{issue, resolve_mode, with_ring, SUBETHA_MODE_MANAGED};
    use crate::tcp_bridge::{SUBETHA_BRIDGE_CLIENT, SUBETHA_BRIDGE_SERVER};

    use super::subetha_quic_bridge_stats;

    /// The client half keeps the name it presents: the run needs it and
    /// the config it was built from does not give it back.
    enum Half {
        Client(QuicBridgeClient, String),
        /// The endpoint and the runtime its driver runs on, the only one
        /// that can drive its accept. The endpoint is declared first so it
        /// is dropped while that driver is still there.
        Server {
            bridge: QuicBridgeServer,
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

    pub(crate) struct QuicBridgeObject {
        half: Arc<Half>,
        mode: u32,
        role: u32,
        progress: Arc<Progress>,
        /// The thread a managed run is on, joined when the handle is
        /// destroyed so a run never outlives the object it reports to.
        worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    }

    impl QuicBridgeObject {
        /// A run in flight is on a socket this cannot interrupt, so the
        /// destroy joins it rather than abandoning it.
        pub(crate) fn interrupt(&self) {
            if let Some(handle) = self.worker.lock().take()
                && handle.join().is_err()
            {
                eprintln!("subetha: a QUIC bridge run panicked and left no code");
            }
        }
    }

    impl Drop for QuicBridgeObject {
        fn drop(&mut self) {
            self.interrupt();
        }
    }

    fn code_for(e: &QuicBridgeError) -> i32 {
        match e {
            QuicBridgeError::Tls(s) => fail(SUBETHA_E_RING_IO, format!("tls: {s}")),
            QuicBridgeError::Quic(s) => fail(SUBETHA_E_RING_IO, format!("quic: {s}")),
            QuicBridgeError::Io(io) => fail(SUBETHA_E_RING_IO, format!("io error: {}", io.kind())),
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
    /// `bytes` points to `len` readable bytes.
    unsafe fn borrowed<'a>(bytes: *const u8, len: usize, what: &str) -> Result<&'a [u8], i32> {
        if bytes.is_null() {
            return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("{what} is null")));
        }
        if len == 0 {
            return Err(fail(SUBETHA_E_INVALID_ARGUMENT, format!("{what} is empty")));
        }
        // Checked non-null; the caller guarantees the length.
        Ok(unsafe { std::slice::from_raw_parts(bytes, len) })
    }

    /// Report `source`'s length through `len`, and copy it to `out` when
    /// there is room. A buffer too small leaves `out` untouched with the
    /// needed length already reported.
    ///
    /// # Safety
    /// `out` points to `cap` writable bytes and `len` is a valid pointer.
    /// # Safety
    /// The pointer contract of `subetha_quic_self_signed_cert`.
    pub(crate) unsafe fn self_signed(
        sni: *const c_char,
        cert_out: *mut u8,
        cert_cap: usize,
        cert_len: *mut usize,
        key_out: *mut u8,
        key_cap: usize,
        key_len: *mut usize,
    ) -> i32 {
        let name = match unsafe { text(sni, "sni") } {
            Ok(t) => t,
            Err(code) => return code,
        };
        let (cert, key) = match generate_self_signed_cert(name) {
            Ok(pair) => pair,
            Err(e) => return code_for(&e),
        };
        // Both copies are attempted before either result is read, so a
        // caller that sized one buffer right and the other wrong learns
        // both counts from one call. The certificate's code is reported
        // first when neither fits.
        let cert_fit = unsafe { copy_out(&cert, cert_out, cert_cap, cert_len) };
        let key_fit = unsafe { copy_out(&key, key_out, key_cap, key_len) };
        if let Err(code) = cert_fit {
            return code;
        }
        if let Err(code) = key_fit {
            return code;
        }
        SUBETHA_OK
    }

    /// # Safety
    /// The pointer contract of `subetha_quic_bridge_client`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) unsafe fn client(
        ring: subetha_handle,
        server_addr: *const c_char,
        bind_addr: *const c_char,
        sni: *const c_char,
        cert: *const u8,
        cert_len: usize,
        mode: u32,
        out: *mut subetha_handle,
    ) -> i32 {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let server = match unsafe { address(server_addr, "server_addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let bind = match unsafe { address(bind_addr, "bind_addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let name = match unsafe { text(sni, "sni") } {
            Ok(t) => t,
            Err(code) => return code,
        };
        let trusted = match unsafe { borrowed(cert, cert_len, "cert") } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        install_default_crypto_provider();
        let config = match make_client_config_from_der(trusted) {
            Ok(c) => c,
            Err(e) => return code_for(&e),
        };
        let mut taken = None;
        let code = with_ring(ring, |r| {
            taken = Some(Arc::clone(&r.ring));
            SUBETHA_OK
        });
        let Some(producer) = taken else { return code };
        let object = QuicBridgeObject {
            half: Arc::new(Half::Client(
                QuicBridgeClient::new(producer, server, config, bind),
                name.to_owned(),
            )),
            mode,
            role: SUBETHA_BRIDGE_CLIENT,
            progress: Arc::new(Progress::default()),
            worker: Mutex::new(None),
        };
        unsafe { issue(Object::QuicBridge(object), out) }
    }

    /// # Safety
    /// The pointer contract of `subetha_quic_bridge_server`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) unsafe fn server(
        ring: subetha_handle,
        addr: *const c_char,
        cert: *const u8,
        cert_len: usize,
        key: *const u8,
        key_len: usize,
        mode: u32,
        out: *mut subetha_handle,
    ) -> i32 {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let socket = match unsafe { address(addr, "addr") } {
            Ok(a) => a,
            Err(code) => return code,
        };
        let cert_bytes = match unsafe { borrowed(cert, cert_len, "cert") } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let key_bytes = match unsafe { borrowed(key, key_len, "key") } {
            Ok(b) => b,
            Err(code) => return code,
        };
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        install_default_crypto_provider();
        let config = match make_server_config_from_der(cert_bytes, key_bytes) {
            Ok(c) => c,
            Err(e) => return code_for(&e),
        };
        let mut taken = None;
        let code = with_ring(ring, |r| {
            taken = Some(Arc::clone(&r.ring));
            SUBETHA_OK
        });
        let Some(consumer) = taken else { return code };
        // Binding spawns the endpoint's driver on the runtime it is
        // entered in, and only that runtime can drive it afterwards, so
        // the runtime stays with the endpoint and every accept runs on it.
        // It is driven only while a run is, so between runs the object
        // holds no thread.
        let runtime = match here() {
            Ok(rt) => rt,
            Err(code) => return code,
        };
        let bound = {
            let _entered = runtime.enter();
            QuicBridgeServer::bind(consumer, socket, config)
        };
        let bridge = match bound {
            Ok(s) => s,
            Err(e) => return code_for(&e),
        };
        let object = QuicBridgeObject {
            half: Arc::new(Half::Server { bridge, runtime }),
            mode,
            role: SUBETHA_BRIDGE_SERVER,
            progress: Arc::new(Progress::default()),
            worker: Mutex::new(None),
        };
        unsafe { issue(Object::QuicBridge(object), out) }
    }

    /// # Safety
    /// `addr` is a NUL-terminated UTF-8 string.
    unsafe fn address(addr: *const c_char, what: &str) -> Result<SocketAddr, i32> {
        let text = unsafe { text(addr, what) }?;
        text.parse()
            .map_err(|e| fail(SUBETHA_E_INVALID_ARGUMENT, format!("{what} {text}: {e}")))
    }

    /// # Safety
    /// `out` is a valid pointer, checked by the caller.
    pub(crate) unsafe fn local_port(b: &QuicBridgeObject, out: *mut u16) -> i32 {
        match &*b.half {
            Half::Server { bridge, .. } => match bridge.local_addr() {
                Ok(addr) => {
                    // Checked non-null and writable by the caller.
                    unsafe { *out = addr.port() };
                    SUBETHA_OK
                }
                Err(e) => fail(SUBETHA_E_RING_IO, format!("io error: {}", e.kind())),
            },
            Half::Client(_, _) => {
                fail(SUBETHA_E_INVALID_ARGUMENT, "a client half listens on no port")
            }
        }
    }

    /// One transfer, on this thread or on the object's own. A client
    /// makes its endpoint inside the run, so a runtime of the call's own
    /// carries it; a server's accept runs on the runtime it was bound in.
    fn carry(half: &Half, items: u64, timeout: Option<Duration>) -> i32 {
        match half {
            Half::Client(c, sni) => match here() {
                Ok(rt) => rt.block_on(bounded(
                    async { c.run(items, sni).await.map(|()| items) },
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
        work: impl std::future::Future<Output = Result<u64, QuicBridgeError>>,
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

    pub(crate) fn run(b: &QuicBridgeObject, items: u64, timeout_ms: i64) -> i32 {
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
            .name("subetha-quic-bridge".to_owned())
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
    pub(crate) unsafe fn read_stats(
        b: &QuicBridgeObject,
        out: *mut subetha_quic_bridge_stats,
    ) -> i32 {
        let stats = subetha_quic_bridge_stats {
            mode: b.mode,
            role: b.role,
            items: b.progress.items.load(Ordering::Acquire),
            running: b.progress.running.load(Ordering::Acquire),
            finished: b.progress.finished.load(Ordering::Acquire),
            last_code: b.progress.last_code.load(Ordering::Acquire),
        };
        // Checked non-null and writable by the caller.
        unsafe { *out = stats };
        SUBETHA_OK
    }
}
