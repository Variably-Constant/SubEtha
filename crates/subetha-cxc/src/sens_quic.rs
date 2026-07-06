//! One-port QUIC + Sens-O-Matic demux endpoint.
//!
//! QUIC v1 sets the fixed bit `0x40` in every packet's first byte; Sens-O-Matic
//! and the unified transport keep their first bytes' high bits clear (RS 1 / 4,
//! RLC 10..=14, CODE_SWITCH 9, UNIFIED_FB 8). So one UDP socket can serve BOTH
//! protocols, routed by the first wire byte: a vanilla QUIC peer connects and
//! gets real QUIC (quinn); a SubEtha peer gets the unified FEC transport with
//! its loss-driven RLC <-> RS switch. The "which is the default" question
//! dissolves: the endpoint speaks both.
//!
//! `DemuxQuicSocket` is a quinn [`AsyncUdpSocket`] mirroring quinn's own tokio
//! socket; only `poll_recv` differs - it returns QUIC datagrams to quinn and
//! routes everything else into the unified Sens receiver's queues via
//! `sens_unified::route_sens_inbound`. [`one_port_server`] wires it to a quinn
//! server [`Endpoint`] and a [`UnifiedSensReceiver`] sharing the socket.

#![cfg(feature = "quic-bridge")]

use std::fmt;
use std::io;
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use quinn::udp::{RecvMeta, Transmit, UdpSocketState};
use quinn::{
    AsyncUdpSocket, Endpoint, EndpointConfig, ServerConfig, TokioRuntime, UdpPoller,
};
use tokio::io::Interest;

use std::sync::atomic::Ordering;

use crate::dgram::{new_demux_queue, DemuxQueue};
use crate::sens_unified::{route_sens_inbound, SwitchSignal, UnifiedConfig, UnifiedSensReceiver};

/// splitmix64 constant for the demux loss injector's state advance.
const SPLITMIX_GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

/// One splitmix64 draw over an atomic state cell (the QUIC socket's `poll_recv`
/// holds `&self`, so the loss-injector RNG needs interior mutability).
fn next_rand(state: &AtomicU64) -> u64 {
    let mut z = state
        .fetch_add(SPLITMIX_GOLDEN, Ordering::Relaxed)
        .wrapping_add(SPLITMIX_GOLDEN);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The demux state the one-port endpoint shares between the QUIC socket (which
/// routes Sens datagrams IN) and the Sens receiver (which reads them OUT). The
/// queues are the same `Arc`s both sides hold.
pub struct SensDemux {
    rlc_q: DemuxQueue,
    rs_q: DemuxQueue,
    /// TLS handshake frames (PKT_RLC_CRYPTO 15 / ACK 16) for the one-port Sens
    /// handshake driver. The QUIC endpoint owns the socket, so the Sens handshake
    /// cannot run a recv loop; the demux routes its frames here and the
    /// `from_shared_tls` thread reads them.
    hs_q: DemuxQueue,
    switch_signal: SwitchSignal,
    recv_counter: Arc<AtomicU64>,
    sens_peer: Arc<Mutex<Option<SocketAddr>>>,
    /// Injected forward-loss percentage (0 = none), mirroring the standalone
    /// demux thread so the one-port path can drive the auto-switch under test.
    debug_loss: u32,
    /// splitmix64 state for the loss injector.
    rng: AtomicU64,
}

impl SensDemux {
    fn new(debug_loss: u32, seed: u64) -> Self {
        Self {
            rlc_q: new_demux_queue(),
            rs_q: new_demux_queue(),
            hs_q: new_demux_queue(),
            switch_signal: Arc::new(Mutex::new(None)),
            recv_counter: Arc::new(AtomicU64::new(0)),
            sens_peer: Arc::new(Mutex::new(None)),
            debug_loss,
            rng: AtomicU64::new(seed ^ 0x5f3a_c001_d00d_1234),
        }
    }
}

/// A quinn [`AsyncUdpSocket`] that demultiplexes one UDP port by first wire
/// byte: QUIC datagrams (`0x40` bit set) are returned to quinn; everything else
/// is routed to the Sens receiver's queues. Mirrors quinn's tokio socket; only
/// `poll_recv` differs.
struct DemuxQuicSocket {
    io: tokio::net::UdpSocket,
    inner: UdpSocketState,
    demux: Arc<SensDemux>,
}

impl fmt::Debug for DemuxQuicSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DemuxQuicSocket").finish_non_exhaustive()
    }
}

/// Write-readiness poller for [`DemuxQuicSocket`] (quinn's own `UdpPollHelper`
/// is not exported, so this wraps the tokio socket's send-readiness directly).
struct QuicPoller {
    sock: Arc<DemuxQuicSocket>,
}

impl fmt::Debug for QuicPoller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicPoller").finish_non_exhaustive()
    }
}

impl UdpPoller for QuicPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        self.sock.io.poll_send_ready(cx)
    }
}

impl AsyncUdpSocket for DemuxQuicSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(QuicPoller { sock: self })
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        self.io
            .try_io(Interest::WRITABLE, || self.inner.send((&self.io).into(), transmit))
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        // Drain the socket aggressively so the kernel receive buffer does not
        // overflow and silently drop Sens datagrams (extra loss on top of the
        // channel's own would push RLC past its repair budget and deadlock its
        // sliding window). Process all-Sens batches back to back until the socket
        // empties (try_io WouldBlock -> park) or a QUIC datagram appears; yield to
        // quinn only after a BOUNDED run of all-Sens batches, so a sustained Sens
        // flood can neither drop datagrams nor monopolize the QUIC driver.
        const SENS_BATCH_BUDGET: u32 = 32;
        let mut sens_batches = 0u32;
        loop {
            ready!(self.io.poll_recv_ready(cx))?;
            let res = self
                .io
                .try_io(Interest::READABLE, || self.inner.recv((&self.io).into(), bufs, meta));
            let Ok(n) = res else { continue };
            // Partition ONE batch: QUIC datagrams compact to the front for quinn;
            // Sens datagrams route to the unified receiver's queues.
            let mut keep = 0;
            for i in 0..n {
                let len = meta[i].len;
                let b0 = if len > 0 { bufs[i][0] } else { 0 };
                if b0 & 0x40 != 0 {
                    // QUIC (possibly GRO-coalesced; quinn splits by stride): keep
                    // the whole buffer + meta, compacting toward the front.
                    if keep != i {
                        let m = meta[i];
                        let (head, tail) = bufs.split_at_mut(i);
                        head[keep][..len].copy_from_slice(&tail[0][..len]);
                        meta[keep] = m;
                    }
                    keep += 1;
                } else {
                    // Sens. A GRO-coalesced buffer holds several same-flow
                    // datagrams back to back at `stride` boundaries; split and
                    // route each. (QUIC and Sens never coalesce together: GRO only
                    // merges datagrams from one source, and a peer speaks one
                    // protocol.) Inject forward loss per datagram (data/repair
                    // only; control always survives) to drive the auto-switch.
                    let addr = meta[i].addr;
                    let stride = meta[i].stride.max(1);
                    let mut off = 0;
                    while off < len {
                        let end = (off + stride).min(len);
                        let seg = &bufs[i][off..end];
                        let sb0 = seg.first().copied().unwrap_or(0);
                        let is_fwd = matches!(sb0, 1 | 10 | 11);
                        let drop = self.demux.debug_loss > 0
                            && is_fwd
                            && (next_rand(&self.demux.rng) % 100) < self.demux.debug_loss as u64;
                        if !drop {
                            let datagram = seg.to_vec();
                            *self.demux.sens_peer.lock().unwrap() = Some(addr);
                            route_sens_inbound(
                                datagram,
                                addr,
                                None,
                                &self.demux.rlc_q,
                                &self.demux.rs_q,
                                Some(&self.demux.switch_signal),
                                None,
                                Some(&self.demux.recv_counter),
                                Some(&self.demux.hs_q),
                            );
                        }
                        off += stride;
                    }
                }
            }
            if keep > 0 {
                return Poll::Ready(Ok(keep));
            }
            // Whole batch was Sens. Loop to drain the next batch immediately (no
            // scheduler round-trip) so the kernel buffer stays empty; only after a
            // bounded run of all-Sens batches do we yield, re-arming our own wake,
            // so quinn's driver gets a turn instead of being monopolized.
            sens_batches += 1;
            if sens_batches >= SENS_BATCH_BUDGET {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    // One datagram per buffer (no GRO): keeps the first-byte demux unambiguous.
    fn max_receive_segments(&self) -> usize {
        1
    }

    fn max_transmit_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

/// Bind one UDP port and bring up a quinn server [`Endpoint`] that shares it
/// with a [`UnifiedSensReceiver`]: QUIC peers get QUIC, SubEtha peers get the
/// unified FEC transport, demultiplexed by first wire byte. Must be called
/// inside a tokio runtime (quinn spawns its endpoint driver). The returned
/// receiver is driven by `poll()` like any unified receiver; the QUIC socket
/// feeds its queues.
pub fn one_port_server(
    sock: StdUdpSocket,
    server_config: ServerConfig,
    cfg: UnifiedConfig,
) -> io::Result<(Endpoint, UnifiedSensReceiver)> {
    let (endpoint, send_clone, demux) = build_endpoint(sock, server_config, &cfg)?;
    let recv = UnifiedSensReceiver::from_shared(
        send_clone,
        Arc::clone(&demux.rlc_q),
        Arc::clone(&demux.rs_q),
        Arc::clone(&demux.switch_signal),
        Arc::clone(&demux.recv_counter),
        Arc::clone(&demux.sens_peer),
        cfg,
        0,
    )?;
    Ok((endpoint, recv))
}

/// Like [`one_port_server`] but the Sens half runs a TLS 1.3 server handshake and
/// AEAD-seals every item, so the WHOLE one-port endpoint - QUIC and Sens - is
/// confidential over an untrusted WAN. The QUIC endpoint owns the socket, so the
/// Sens handshake cannot run its own recv loop; it rides the demux (the handshake
/// frames route to a queue the driver thread reads) and the receiver's `poll()`
/// withholds delivery until the 1-RTT keys are published. Must be called inside a
/// tokio runtime; returns immediately so the caller can start the clients that
/// drive the handshake.
#[cfg(feature = "tls")]
pub fn one_port_server_tls(
    sock: StdUdpSocket,
    server_config: ServerConfig,
    cfg: UnifiedConfig,
    sens_tls: std::sync::Arc<rustls::ServerConfig>,
) -> io::Result<(Endpoint, UnifiedSensReceiver)> {
    let (endpoint, send_clone, demux) = build_endpoint(sock, server_config, &cfg)?;
    let recv = UnifiedSensReceiver::from_shared_tls(
        send_clone,
        Arc::clone(&demux.rlc_q),
        Arc::clone(&demux.rs_q),
        Arc::clone(&demux.hs_q),
        Arc::clone(&demux.switch_signal),
        Arc::clone(&demux.recv_counter),
        Arc::clone(&demux.sens_peer),
        cfg,
        sens_tls,
    )?;
    Ok((endpoint, recv))
}

/// Shared one-port setup: enlarge the kernel UDP buffers, disable GRO + QUIC bit
/// greasing (both would break the first-byte demux), build the quinn server
/// [`Endpoint`] over the [`DemuxQuicSocket`], and return it with a send-clone of
/// the shared socket and the [`SensDemux`] both halves hold.
fn build_endpoint(
    sock: StdUdpSocket,
    server_config: ServerConfig,
    cfg: &UnifiedConfig,
) -> io::Result<(Endpoint, Arc<StdUdpSocket>, Arc<SensDemux>)> {
    sock.set_nonblocking(true)?;
    // Enlarge the kernel UDP buffers so a Sens send burst (plus the QUIC traffic
    // sharing the port) cannot overflow the receive queue and manufacture loss on
    // top of the channel's own - extra loss would push RLC past its repair budget
    // and deadlock its window. Matches the standalone Sens transport's 4 MiB.
    {
        let s = socket2::SockRef::from(&sock);
        s.set_recv_buffer_size(4 * 1024 * 1024).ok();
        s.set_send_buffer_size(4 * 1024 * 1024).ok();
    }
    let send_clone = sock.try_clone()?;
    send_clone.set_nonblocking(true)?;
    let demux = Arc::new(SensDemux::new(cfg.debug_loss, cfg.seed));

    let inner = UdpSocketState::new((&sock).into())?;
    // Disable kernel UDP GRO on the shared socket. quinn enables it for QUIC recv
    // throughput, but GRO coalesces a Sens send-burst into one super-buffer whose
    // per-datagram boundaries the first-byte demux must split by `stride` - and a
    // mis-split hands the RS decoder mis-framed shards (it then reconstructs zero
    // blocks). With GRO off every recv returns exactly one datagram (stride==len),
    // so the demux split is the identity and shards reach the decoder intact. QUIC
    // still works, just without the GRO batching speedup.
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        const UDP_GRO: libc::c_int = 104;
        let off: libc::c_int = 0;
        unsafe {
            libc::setsockopt(
                sock.as_raw_fd(),
                libc::SOL_UDP,
                UDP_GRO,
                &off as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    let io = tokio::net::UdpSocket::from_std(sock)?;
    let quic_sock: Arc<dyn AsyncUdpSocket> = Arc::new(DemuxQuicSocket {
        io,
        inner,
        demux: Arc::clone(&demux),
    });
    // Disable QUIC bit greasing (RFC 9287). Greasing randomly CLEARS the fixed
    // bit (0x40) on packets; the one-port demux uses that bit to tell QUIC from
    // Sens-O-Matic by the first wire byte, so a greased packet (0x40 clear) would
    // mis-route into the Sens path. A peer only greases toward us if WE advertise
    // support, so not advertising it keeps every inbound QUIC packet's fixed bit
    // set and the first-byte demux unambiguous.
    let mut endpoint_config = EndpointConfig::default();
    endpoint_config.grease_quic_bit(false);
    let endpoint = Endpoint::new_with_abstract_socket(
        endpoint_config,
        Some(server_config),
        quic_sock,
        Arc::new(TokioRuntime),
    )?;
    Ok((endpoint, Arc::new(send_clone), demux))
}
