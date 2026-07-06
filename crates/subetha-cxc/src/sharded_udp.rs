//! Sharded reliable-UDP: N independent Sens-O-Matic streams, each on its
//! own thread, distributing the WHOLE data path (encode + flow + send on
//! the way out, recv + decode + deliver on the way in) across N cores.
//!
//! Each stream is the single-threaded [`ReliableUdpSender`] /
//! [`ReliableUdpReceiver`] pair, unchanged - every property it carries
//! (FEC, selective NAK, hold-time, loss resilience) holds per shard. The
//! parallelism is at stream granularity: application item `i` rides shard
//! `i % shards`, and because each shard delivers its own items in order,
//! the receiver reassembles the global order by reading the shards
//! round-robin. With per-thread throughput already above a single QUIC
//! worker, a few shards match or pass it.
//!
//! Shard `s` uses UDP port `base_port + s`; the transport is
//! point-to-point per shard, so no demultiplexing is needed.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::udp_bridge::{ReliableUdpReceiver, ReliableUdpSender};

/// Per-shard hand-off queue depth between the application thread and a
/// shard's stream thread. Bounded so a fast producer paces to the
/// slowest shard rather than growing unboundedly.
const HANDOFF_CAP: usize = 2048;

/// How long a shard's drain / receive may run before giving up.
const SHARD_DEADLINE: Duration = Duration::from_secs(180);

/// Sending half: `shards` independent senders, one stream thread each.
pub struct ShardedSender {
    txs: Vec<SyncSender<Vec<u8>>>,
    handles: Vec<JoinHandle<bool>>,
    next: u64,
}

impl ShardedSender {
    /// Bind `shards` senders; shard `s` targets `peer:(base_port + s)`
    /// with `k` data + `r` parity shards and a `max_item`-byte payload.
    /// Each shard runs its send + flow-control + drain loop on its own
    /// thread.
    pub fn bind(
        peer: IpAddr,
        base_port: u16,
        shards: usize,
        k: usize,
        r: usize,
        max_item: usize,
    ) -> io::Result<Self> {
        let shards = shards.max(1);
        let mut txs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for s in 0..shards {
            let (tx, rx) = sync_channel::<Vec<u8>>(HANDOFF_CAP);
            let peer_addr = SocketAddr::new(peer, base_port + s as u16);
            let mut sender = ReliableUdpSender::bind("0.0.0.0:0", peer_addr, k, r, max_item)?;
            let handle = thread::spawn(move || -> bool {
                // Drain the hand-off queue, sending each item under flow
                // control. The channel closing (all senders dropped) is
                // the end-of-stream signal.
                while let Ok(item) = rx.recv() {
                    while sender.flow_blocked() {
                        sender.pump_feedback().ok();
                        if sender.flow_blocked() {
                            thread::sleep(Duration::from_micros(50));
                        }
                    }
                    if sender.send_item(&item).is_err() {
                        return false;
                    }
                }
                sender.flush().ok();
                sender
                    .drain_until_acked(SHARD_DEADLINE)
                    .unwrap_or(false)
            });
            txs.push(tx);
            handles.push(handle);
        }
        Ok(Self {
            txs,
            handles,
            next: 0,
        })
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.txs.len()
    }

    /// Hand `item` to its shard (round-robin). Blocks if that shard's
    /// queue is full, pacing the producer to the slowest shard.
    pub fn send_item(&mut self, item: &[u8]) {
        let shard = (self.next % self.txs.len() as u64) as usize;
        self.next += 1;
        // A send error means the shard thread already exited; the join in
        // `finish` surfaces it as not-fully-acked.
        self.txs[shard].send(item.to_vec()).ok();
    }

    /// Close every shard's queue, join the stream threads, and report
    /// whether all shards fully acked.
    pub fn finish(self) -> bool {
        let Self { txs, handles, .. } = self;
        drop(txs);
        // Join EVERY shard (collect forces all joins; `all` alone would
        // short-circuit on the first non-acked shard and strand threads),
        // then report whether all acked.
        let acked: Vec<bool> = handles
            .into_iter()
            .map(|h| h.join().unwrap_or(false))
            .collect();
        acked.into_iter().all(|x| x)
    }
}

/// Receiving half: `shards` independent receivers, one stream thread
/// each, reassembled round-robin into the global item order.
pub struct ShardedReceiver {
    rxs: Vec<Receiver<Vec<u8>>>,
    handles: Vec<JoinHandle<()>>,
    next: u64,
}

impl ShardedReceiver {
    /// Bind `shards` receivers; shard `s` binds `bind_ip:(base_port + s)`
    /// and delivers its slice of `total_items`. `loss` (>0) injects
    /// per-shard diagnostic loss with a per-shard seed.
    pub fn bind(
        bind_ip: IpAddr,
        base_port: u16,
        shards: usize,
        total_items: u64,
        loss: u32,
        seed: u64,
    ) -> io::Result<Self> {
        let shards = shards.max(1);
        let mut rxs = Vec::with_capacity(shards);
        let mut handles = Vec::with_capacity(shards);
        for s in 0..shards {
            let (tx, rx) = sync_channel::<Vec<u8>>(HANDOFF_CAP);
            // Item `i` rides shard `i % shards`, so shard `s` receives
            // `floor(total/shards)` plus one if `s` is below the
            // remainder.
            let expected =
                total_items / shards as u64 + u64::from((s as u64) < total_items % shards as u64);
            let addr = SocketAddr::new(bind_ip, base_port + s as u16);
            let mut receiver = ReliableUdpReceiver::bind(addr)?;
            if loss > 0 {
                receiver = receiver.with_debug_loss(loss, seed.wrapping_add(s as u64 + 1));
            }
            let handle = thread::spawn(move || {
                let started = Instant::now();
                let mut delivered = 0u64;
                while delivered < expected {
                    if started.elapsed() > SHARD_DEADLINE {
                        return;
                    }
                    for item in receiver.poll().unwrap_or_default() {
                        if tx.send(item).is_err() {
                            return;
                        }
                        delivered += 1;
                    }
                }
                // Grace: keep feeding feedback so the sender learns the
                // final ack on this shard.
                for _ in 0..100 {
                    receiver.nudge_feedback().ok();
                    thread::sleep(Duration::from_millis(2));
                }
            });
            rxs.push(rx);
            handles.push(handle);
        }
        Ok(Self {
            rxs,
            handles,
            next: 0,
        })
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.rxs.len()
    }

    /// Receive the next item in GLOBAL order (round-robin across shards;
    /// each shard delivers its own items in order, so the round-robin is
    /// the global order). Blocks until that shard delivers it; `None`
    /// when the shard's stream ended.
    pub fn recv_item(&mut self) -> Option<Vec<u8>> {
        let shard = (self.next % self.rxs.len() as u64) as usize;
        self.next += 1;
        self.rxs[shard].recv().ok()
    }

    /// Join the shard threads (after the caller has read every item).
    pub fn finish(self) {
        let Self { rxs, handles, .. } = self;
        drop(rxs);
        for handle in handles {
            handle.join().ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    /// End-to-end loopback across 4 shards: striped out, reassembled in
    /// global order, exact count + order + sum.
    #[test]
    fn sharded_loopback_in_order_exact() {
        let (shards, total) = (4usize, 4000u64);
        let base = 21000u16;
        let mut recv =
            ShardedReceiver::bind(IpAddr::V4(Ipv4Addr::LOCALHOST), base, shards, total, 0, 1)
                .expect("bind receiver");
        let rx = std::thread::spawn(move || -> Vec<u64> {
            let mut got = Vec::with_capacity(total as usize);
            for _ in 0..total {
                let item = recv.recv_item().expect("item");
                got.push(u64::from_le_bytes(item[..8].try_into().unwrap()));
            }
            recv.finish();
            got
        });
        let mut send =
            ShardedSender::bind(IpAddr::V4(Ipv4Addr::LOCALHOST), base, shards, 8, 2, 64)
                .expect("bind sender");
        let mut buf = [0u8; 64];
        for i in 0..total {
            buf[..8].copy_from_slice(&i.to_le_bytes());
            send.send_item(&buf);
        }
        assert!(send.finish(), "all shards fully acked");
        let got = rx.join().expect("rx thread");
        assert_eq!(got, (0..total).collect::<Vec<_>>(), "global order exact");
    }
}
