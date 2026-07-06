//! `SharedTopologyMap` - K_process axis observer + recommendation
//! substrate for cross-process message-flow topology selection.
//!
//! # Architectural role
//!
//! The K_process equivalent of intra-process Layer-2 adaptation:
//! observes the flow pattern between N participating processes
//! (per-edge message counts) and recommends one of three
//! transport topologies based on the observed fan-in / fan-out
//! statistics:
//!
//! - **PointToPoint** - single producer, single consumer. Use
//!   [`SharedRing`](crate::SharedRing) directly.
//! - **BroadcastTree** - one producer, many consumers each
//!   receiving every message. Use
//!   [`SharedBroadcastRing`](crate::SharedBroadcastRing).
//! - **AllToAllMesh** - N peers, all-to-all routing. Use a grid of
//!   `N*N` SharedRings indexed by `(src, dst)`.
//!
//! # Policy
//!
//! From the bead specification:
//! - `max_fan_in >= fan_in_threshold` AND `max_fan_out >=
//!    fan_out_threshold` → `AllToAllMesh`
//! - `max_fan_out >= fan_out_threshold` → `BroadcastTree`
//! - otherwise → `PointToPoint`
//!
//! Defaults: both thresholds = 3.
//!
//! # Layout
//!
//! ```text
//! +---------------------------+
//! | TopologyHeader (64B)      |
//! |   magic, n_nodes          |
//! |   total_msgs              |
//! |   recommendation cell     |
//! |   recommendation_epoch    |
//! |   thresholds              |
//! +---------------------------+
//! | edge_counts [N*N AtomicU64] |
//! +---------------------------+
//! ```
//!
//! # Why separate observer from transport
//!
//! Each process picks its OWN role in a topology (publisher /
//! subscriber for BroadcastTree; node index for Mesh), which is
//! intrinsically per-process. The observer is the SHARED part
//! (everyone reads the same recommendation). The transport
//! instantiation is the per-process part. Keeping them separate
//! avoids forcing all processes to share a single transport-
//! switching state machine they can't all participate in
//! symmetrically.

use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::{MmapMut, MmapOptions};

pub const TOPOLOGY_MAGIC: u64 = 0x4150_544F_504F_3031;

/// Default thresholds: 3 fan-out for BroadcastTree, 3 fan-in for
/// AllToAllMesh. Match the bead specification.
pub const DEFAULT_FAN_OUT_THRESHOLD: u32 = 3;
pub const DEFAULT_FAN_IN_THRESHOLD: u32 = 3;

/// Topology kind, encoded as a u32 in shared memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum TopologyKind {
    PointToPoint = 0,
    BroadcastTree = 1,
    AllToAllMesh = 2,
}

impl TopologyKind {
    pub fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::BroadcastTree,
            2 => Self::AllToAllMesh,
            _ => Self::PointToPoint,
        }
    }
}

#[repr(C, align(64))]
pub struct TopologyHeader {
    pub magic: u64,
    pub n_nodes: u32,
    pub fan_out_threshold: AtomicU32,
    pub fan_in_threshold: AtomicU32,
    _pad1: u32,
    pub total_msgs: AtomicU64,
    pub recommendation: AtomicU32,
    pub recommendation_epoch: AtomicU64,
    pub broadcast_root: AtomicU32,
    _pad2: u32,
    _pad3: [u8; 8],
}

const _: () = {
    assert!(size_of::<TopologyHeader>() == 64);
};

pub const fn topology_file_size(n_nodes: usize) -> usize {
    size_of::<TopologyHeader>() + n_nodes * n_nodes * size_of::<AtomicU64>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyError {
    NodeIndexOutOfBounds,
    LayoutMismatch,
    IoError(std::io::ErrorKind),
}

impl From<std::io::Error> for TopologyError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopologyStats {
    pub total_msgs: u64,
    pub max_fan_out: u32,
    pub max_fan_in: u32,
    pub max_fan_out_src: u32,
    pub max_fan_in_dst: u32,
    pub current_recommendation: TopologyKind,
    pub recommendation_epoch: u64,
}

pub struct SharedTopologyMap {
    _file: File,
    mmap: MmapMut,
    n_nodes: usize,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

unsafe impl Send for SharedTopologyMap {}
unsafe impl Sync for SharedTopologyMap {}

impl subetha_sidecar::AdaptiveInstance for SharedTopologyMap {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl SharedTopologyMap {
    pub fn create(
        path: impl AsRef<Path>,
        n_nodes: usize,
    ) -> Result<Self, TopologyError> {
        Self::create_with_thresholds(
            path, n_nodes,
            DEFAULT_FAN_OUT_THRESHOLD, DEFAULT_FAN_IN_THRESHOLD,
        )
    }

    pub fn create_with_thresholds(
        path: impl AsRef<Path>,
        n_nodes: usize,
        fan_out_threshold: u32,
        fan_in_threshold: u32,
    ) -> Result<Self, TopologyError> {
        assert!(n_nodes >= 1);
        assert!(n_nodes <= u32::MAX as usize);
        let total = topology_file_size(n_nodes);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path.as_ref())?;
        file.set_len(total as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = mmap.as_mut_ptr() as *mut TopologyHeader;
        unsafe {
            std::ptr::write_bytes(hdr as *mut u8, 0, size_of::<TopologyHeader>());
            (*hdr).magic = TOPOLOGY_MAGIC;
            (*hdr).n_nodes = n_nodes as u32;
            (*hdr).fan_out_threshold.store(fan_out_threshold, Ordering::Release);
            (*hdr).fan_in_threshold.store(fan_in_threshold, Ordering::Release);
            (*hdr).recommendation.store(
                TopologyKind::PointToPoint as u32, Ordering::Release,
            );
        }
        // Edge counters are zero-filled by set_len + map_mut.
        Ok(Self {
            _file: file, mmap, n_nodes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        path: impl AsRef<Path>,
        expected_n_nodes: usize,
    ) -> Result<Self, TopologyError> {
        let total = topology_file_size(expected_n_nodes);
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        if file.metadata()?.len() < total as u64 {
            return Err(TopologyError::LayoutMismatch);
        }
        let mmap = unsafe { MmapOptions::new().len(total).map_mut(&file)? };
        let hdr = unsafe { &*(mmap.as_ptr() as *const TopologyHeader) };
        if hdr.magic != TOPOLOGY_MAGIC || hdr.n_nodes != expected_n_nodes as u32 {
            return Err(TopologyError::LayoutMismatch);
        }
        Ok(Self {
            _file: file, mmap, n_nodes: expected_n_nodes,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    #[inline]
    pub fn n_nodes(&self) -> usize { self.n_nodes }

    fn header(&self) -> &TopologyHeader {
        unsafe { &*(self.mmap.as_ptr() as *const TopologyHeader) }
    }

    fn edge(&self, src: u32, dst: u32) -> &AtomicU64 {
        let idx = (src as usize) * self.n_nodes + (dst as usize);
        let base = unsafe { self.mmap.as_ptr().add(size_of::<TopologyHeader>()) };
        unsafe { &*(base.add(idx * size_of::<AtomicU64>()) as *const AtomicU64) }
    }

    /// Record one message from `src` to `dst`. Increments the edge
    /// counter and the total. Returns the new edge count.
    pub fn record_send(&self, src: u32, dst: u32) -> Result<u64, TopologyError> {
        if src as usize >= self.n_nodes || dst as usize >= self.n_nodes {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::topology::OP_RECORD, 1);
            return Err(TopologyError::NodeIndexOutOfBounds);
        }
        let prev = self.edge(src, dst).fetch_add(1, Ordering::AcqRel);
        self.header().total_msgs.fetch_add(1, Ordering::AcqRel);
        self.ring_sidecar
            .push_op(crate::sidecar_ops::topology::OP_RECORD, 0);
        Ok(prev + 1)
    }

    /// Fan-out for `src`: count of destinations with non-zero edge.
    pub fn fan_out(&self, src: u32) -> u32 {
        if src as usize >= self.n_nodes {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::topology::OP_FAN_OUT, 1);
            return 0;
        }
        let mut count = 0u32;
        for d in 0..self.n_nodes as u32 {
            if self.edge(src, d).load(Ordering::Acquire) > 0 {
                count += 1;
            }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::topology::OP_FAN_OUT, 0);
        count
    }

    /// Fan-in for `dst`: count of sources with non-zero edge.
    pub fn fan_in(&self, dst: u32) -> u32 {
        if dst as usize >= self.n_nodes {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::topology::OP_FAN_IN, 1);
            return 0;
        }
        let mut count = 0u32;
        for s in 0..self.n_nodes as u32 {
            if self.edge(s, dst).load(Ordering::Acquire) > 0 {
                count += 1;
            }
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::topology::OP_FAN_IN, 0);
        count
    }

    /// Max fan-out across all sources, plus the source index.
    pub fn max_fan_out(&self) -> (u32, u32) {
        let mut max = 0u32;
        let mut who = 0u32;
        for s in 0..self.n_nodes as u32 {
            let fo = self.fan_out(s);
            if fo > max { max = fo; who = s; }
        }
        (max, who)
    }

    /// Max fan-in across all destinations, plus the dst index.
    pub fn max_fan_in(&self) -> (u32, u32) {
        let mut max = 0u32;
        let mut who = 0u32;
        for d in 0..self.n_nodes as u32 {
            let fi = self.fan_in(d);
            if fi > max { max = fi; who = d; }
        }
        (max, who)
    }

    /// Compute the recommended topology from observed stats. Pure
    /// function over the current edge-count snapshot; does NOT
    /// mutate the published recommendation. Use
    /// `publish_recommendation` to cache it for O(1) reads.
    pub fn recommend(&self) -> TopologyKind {
        let (max_fan_out, _) = self.max_fan_out();
        let (max_fan_in, _) = self.max_fan_in();
        let fo_threshold = self.header().fan_out_threshold.load(Ordering::Acquire);
        let fi_threshold = self.header().fan_in_threshold.load(Ordering::Acquire);
        let kind = if max_fan_out >= fo_threshold && max_fan_in >= fi_threshold {
            TopologyKind::AllToAllMesh
        } else if max_fan_out >= fo_threshold {
            TopologyKind::BroadcastTree
        } else {
            TopologyKind::PointToPoint
        };
        self.ring_sidecar
            .push_op(crate::sidecar_ops::topology::OP_RECOMMEND, 0);
        kind
    }

    /// Compute the recommendation AND publish it to the header so
    /// other processes can read it at O(1) via
    /// `read_recommendation`. Bumps `recommendation_epoch`.
    /// Returns the published recommendation.
    pub fn publish_recommendation(&self) -> TopologyKind {
        let kind = self.recommend();
        let hdr = self.header();
        hdr.recommendation.store(kind as u32, Ordering::Release);
        hdr.recommendation_epoch.fetch_add(1, Ordering::Release);
        // If recommending BroadcastTree, also record the broadcast
        // root (the source with the highest fan-out).
        if kind == TopologyKind::BroadcastTree {
            let (_, root) = self.max_fan_out();
            hdr.broadcast_root.store(root, Ordering::Release);
        }
        kind
    }

    /// Read the most-recently-published recommendation. O(1).
    pub fn read_recommendation(&self) -> TopologyKind {
        TopologyKind::from_u32(
            self.header().recommendation.load(Ordering::Acquire)
        )
    }

    /// Read the recommended broadcast root (the highest-fan-out
    /// source at the most recent `publish_recommendation`). Only
    /// meaningful when `read_recommendation` == BroadcastTree.
    pub fn broadcast_root(&self) -> u32 {
        self.header().broadcast_root.load(Ordering::Acquire)
    }

    /// Returns the recommendation epoch counter (bumped every
    /// `publish_recommendation`). Observers can subscribe to
    /// changes by comparing successive reads.
    pub fn recommendation_epoch(&self) -> u64 {
        self.header().recommendation_epoch.load(Ordering::Acquire)
    }

    /// Read total messages observed across all edges.
    pub fn total_msgs(&self) -> u64 {
        self.header().total_msgs.load(Ordering::Acquire)
    }

    /// Snapshot all stats in one O(N²) pass.
    pub fn stats(&self) -> TopologyStats {
        let (max_fan_out, max_fan_out_src) = self.max_fan_out();
        let (max_fan_in, max_fan_in_dst) = self.max_fan_in();
        TopologyStats {
            total_msgs: self.total_msgs(),
            max_fan_out, max_fan_out_src,
            max_fan_in, max_fan_in_dst,
            current_recommendation: self.read_recommendation(),
            recommendation_epoch: self.recommendation_epoch(),
        }
    }

    /// Reset all edge counters to zero (new observation window).
    /// Total_msgs is also reset. The recommendation cell is left
    /// untouched (use publish_recommendation to refresh after a new
    /// observation epoch).
    pub fn reset_observations(&self) {
        for s in 0..self.n_nodes as u32 {
            for d in 0..self.n_nodes as u32 {
                self.edge(s, d).store(0, Ordering::Release);
            }
        }
        self.header().total_msgs.store(0, Ordering::Release);
    }

    /// Update the policy thresholds. Useful for tuning per workload
    /// without re-creating the map.
    pub fn set_thresholds(&self, fan_out: u32, fan_in: u32) {
        self.header().fan_out_threshold.store(fan_out, Ordering::Release);
        self.header().fan_in_threshold.store(fan_in, Ordering::Release);
    }

    pub fn flush(&self) -> Result<(), TopologyError> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Non-blocking flush: schedules a writeback via the OS.
    /// Note: Windows is only partially async (sync to page cache,
    /// not to disk).
    pub fn flush_async(&self) -> Result<(), TopologyError> {
        self.mmap.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-topology-{name}-{pid}.bin"));
        p
    }

    #[test]
    fn create_initial_state() {
        let p = tmp("init");
        let t = SharedTopologyMap::create(&p, 4).unwrap();
        assert_eq!(t.n_nodes(), 4);
        assert_eq!(t.total_msgs(), 0);
        assert_eq!(t.max_fan_out(), (0, 0));
        assert_eq!(t.max_fan_in(), (0, 0));
        assert_eq!(t.recommend(), TopologyKind::PointToPoint);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn record_send_increments_edge_and_total() {
        let p = tmp("record");
        let t = SharedTopologyMap::create(&p, 4).unwrap();
        let new_count = t.record_send(0, 1).unwrap();
        assert_eq!(new_count, 1);
        assert_eq!(t.total_msgs(), 1);
        t.record_send(0, 1).unwrap();
        assert_eq!(t.total_msgs(), 2);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn fan_out_in_count_distinct_edges() {
        let p = tmp("fan");
        let t = SharedTopologyMap::create(&p, 4).unwrap();
        // Node 0 sends to nodes 1, 2, 3 → fan-out = 3.
        t.record_send(0, 1).unwrap();
        t.record_send(0, 2).unwrap();
        t.record_send(0, 3).unwrap();
        assert_eq!(t.fan_out(0), 3);
        assert_eq!(t.fan_in(0), 0);
        // Multiple sends to the same dst still count as one edge.
        t.record_send(0, 1).unwrap();
        t.record_send(0, 1).unwrap();
        assert_eq!(t.fan_out(0), 3);
        // Node 2 receives from no one yet... wait, node 0 sent to 2.
        assert_eq!(t.fan_in(2), 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn out_of_bounds_record_returns_error() {
        let p = tmp("oob");
        let t = SharedTopologyMap::create(&p, 4).unwrap();
        assert_eq!(t.record_send(4, 0).err(), Some(TopologyError::NodeIndexOutOfBounds));
        assert_eq!(t.record_send(0, 99).err(), Some(TopologyError::NodeIndexOutOfBounds));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn recommend_point_to_point_when_fan_low() {
        let p = tmp("rec-p2p");
        let t = SharedTopologyMap::create(&p, 4).unwrap();
        // 1:1 flow: node 0 → node 1.
        for _ in 0..100 { t.record_send(0, 1).unwrap(); }
        assert_eq!(t.recommend(), TopologyKind::PointToPoint);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn recommend_broadcast_when_fan_out_high_only() {
        let p = tmp("rec-bcast");
        let t = SharedTopologyMap::create(&p, 5).unwrap();
        // Node 0 broadcasts to 1, 2, 3, 4 - high fan-out, low fan-in.
        for d in 1..5 { t.record_send(0, d).unwrap(); }
        assert_eq!(t.fan_out(0), 4);
        assert_eq!(t.max_fan_in(), (1, 1));  // each receives 1 from one source
        assert_eq!(t.recommend(), TopologyKind::BroadcastTree);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn recommend_all_to_all_when_both_high() {
        let p = tmp("rec-mesh");
        let t = SharedTopologyMap::create(&p, 5).unwrap();
        // Every node sends to every other node.
        for s in 0..5u32 {
            for d in 0..5u32 {
                if s != d { t.record_send(s, d).unwrap(); }
            }
        }
        assert_eq!(t.max_fan_out().0, 4);
        assert_eq!(t.max_fan_in().0, 4);
        assert_eq!(t.recommend(), TopologyKind::AllToAllMesh);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn publish_recommendation_caches_for_o1_reads() {
        let p = tmp("publish");
        let t = SharedTopologyMap::create(&p, 5).unwrap();
        for d in 1..5 { t.record_send(0, d).unwrap(); }
        assert_eq!(t.recommendation_epoch(), 0);
        let published = t.publish_recommendation();
        assert_eq!(published, TopologyKind::BroadcastTree);
        assert_eq!(t.read_recommendation(), TopologyKind::BroadcastTree);
        assert_eq!(t.recommendation_epoch(), 1);
        // Broadcast root is the highest-fan-out source.
        assert_eq!(t.broadcast_root(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn cross_handle_observation_visible() {
        let p = tmp("cross-handle");
        let writer = SharedTopologyMap::create(&p, 4).unwrap();
        let observer = SharedTopologyMap::open(&p, 4).unwrap();
        writer.record_send(0, 1).unwrap();
        writer.record_send(0, 2).unwrap();
        writer.record_send(0, 3).unwrap();
        assert_eq!(observer.fan_out(0), 3);
        writer.publish_recommendation();
        assert_eq!(observer.read_recommendation(), TopologyKind::BroadcastTree);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn reset_observations_clears_all_edges() {
        let p = tmp("reset");
        let t = SharedTopologyMap::create(&p, 4).unwrap();
        for s in 0..4u32 {
            for d in 0..4u32 {
                t.record_send(s, d).unwrap();
            }
        }
        assert_eq!(t.total_msgs(), 16);
        t.reset_observations();
        assert_eq!(t.total_msgs(), 0);
        assert_eq!(t.fan_out(0), 0);
        assert_eq!(t.fan_in(0), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_record_sends_count_correctly() {
        let p = tmp("concurrent");
        let t = Arc::new(SharedTopologyMap::create(&p, 4).unwrap());
        let n_threads = 4;
        let per_thread = 100;
        let mut handles = vec![];
        for src in 0..n_threads as u32 {
            let t = t.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..per_thread {
                    for dst in 0..4u32 {
                        if src != dst { t.record_send(src, dst).unwrap(); }
                    }
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        // Each thread sent (4-1) edges * per_thread times.
        assert_eq!(t.total_msgs(), (n_threads * 3 * per_thread) as u64);
        // Every source has fan_out = 3 (all dst != self).
        for s in 0..n_threads as u32 {
            assert_eq!(t.fan_out(s), 3, "src {s} should have fan_out 3");
        }
        // The full N-to-N pattern triggers AllToAllMesh.
        assert_eq!(t.recommend(), TopologyKind::AllToAllMesh);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn set_thresholds_adjusts_recommendation_policy() {
        let p = tmp("thresholds");
        let t = SharedTopologyMap::create(&p, 5).unwrap();
        // Node 0 sends to 1, 2 - fan_out=2 (below default 3, recommends P2P).
        t.record_send(0, 1).unwrap();
        t.record_send(0, 2).unwrap();
        assert_eq!(t.recommend(), TopologyKind::PointToPoint);
        // Lower fan_out threshold to 2 → now BroadcastTree applies.
        t.set_thresholds(2, 3);
        assert_eq!(t.recommend(), TopologyKind::BroadcastTree);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn stats_snapshot_returns_full_picture() {
        let p = tmp("stats");
        let t = SharedTopologyMap::create(&p, 5).unwrap();
        for d in 1..5u32 { t.record_send(0, d).unwrap(); }
        t.publish_recommendation();
        let s = t.stats();
        assert_eq!(s.total_msgs, 4);
        assert_eq!(s.max_fan_out, 4);
        assert_eq!(s.max_fan_out_src, 0);
        assert_eq!(s.max_fan_in, 1);
        assert_eq!(s.current_recommendation, TopologyKind::BroadcastTree);
        assert_eq!(s.recommendation_epoch, 1);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn topology_kind_from_u32_round_trip() {
        assert_eq!(TopologyKind::from_u32(0), TopologyKind::PointToPoint);
        assert_eq!(TopologyKind::from_u32(1), TopologyKind::BroadcastTree);
        assert_eq!(TopologyKind::from_u32(2), TopologyKind::AllToAllMesh);
        // Unknown values default to PointToPoint (defensive).
        assert_eq!(TopologyKind::from_u32(999), TopologyKind::PointToPoint);
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let p = tmp("disk");
        {
            let t = SharedTopologyMap::create(&p, 4).unwrap();
            for d in 1..4u32 { t.record_send(0, d).unwrap(); }
            t.publish_recommendation();
            t.flush().unwrap();
        }
        let t2 = SharedTopologyMap::open(&p, 4).unwrap();
        assert_eq!(t2.total_msgs(), 3);
        assert_eq!(t2.fan_out(0), 3);
        assert_eq!(t2.read_recommendation(), TopologyKind::BroadcastTree);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn recommendation_demotes_when_observations_drop() {
        let p = tmp("demote");
        let t = SharedTopologyMap::create(&p, 5).unwrap();
        // Start with broadcast pattern.
        for d in 1..5u32 { t.record_send(0, d).unwrap(); }
        assert_eq!(t.recommend(), TopologyKind::BroadcastTree);
        // New observation epoch: only 1:1 traffic.
        t.reset_observations();
        for _ in 0..10 { t.record_send(0, 1).unwrap(); }
        assert_eq!(t.recommend(), TopologyKind::PointToPoint);
        std::fs::remove_file(&p).ok();
    }
}
