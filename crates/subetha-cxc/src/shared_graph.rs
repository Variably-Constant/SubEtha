//! `SharedGraph<N, E>` - cross-process directed graph with
//! arbitrary out-degree.
//!
//! Nodes carry `N` values; edges carry `E` values + destination
//! index. Adjacency stored as per-node linked lists of edges (each
//! edge has a `next_in_src_list` link to the next edge from the
//! same source).
//!
//! # Files
//!
//! - `<base>.nodes.bin` - `SharedRegion<GraphNode<N>>`
//! - `<base>.edges.bin` - `SharedRegion<GraphEdge<E>>`
//!
//! # Concurrency
//!
//! SINGLE-WRITER, MULTI-READER. Reads (neighbors, node_value,
//! edge_value, iter) are lock-free. Writes (add_node, add_edge,
//! remove_edge) require external serialisation.
//!
//! # Safety
//!
//! - Bounded capacity at create (both regions).
//! - SharedRegion's ABA-safe free list backs slot reuse.
//! - No spin loops, no Drop guards, no atomic underflow risk.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::shared_region::{OffsetPtr, RegionError, SharedRegion};

pub const NIL_INDEX: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct NodeIndex<N> {
    pub index: u32,
    _phantom: PhantomData<N>,
}

impl<N> NodeIndex<N> {
    pub const NIL: Self = Self { index: NIL_INDEX, _phantom: PhantomData };
    #[inline]
    pub fn new(index: u32) -> Self { Self { index, _phantom: PhantomData } }
    #[inline]
    pub fn is_nil(self) -> bool { self.index == NIL_INDEX }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct EdgeIndex<E> {
    pub index: u32,
    _phantom: PhantomData<E>,
}

impl<E> EdgeIndex<E> {
    pub const NIL: Self = Self { index: NIL_INDEX, _phantom: PhantomData };
    #[inline]
    pub fn new(index: u32) -> Self { Self { index, _phantom: PhantomData } }
    #[inline]
    pub fn is_nil(self) -> bool { self.index == NIL_INDEX }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct GraphNode<N: Copy + Default + 'static> {
    pub value: N,
    pub first_out_edge: u32,
    pub n_out_edges: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct GraphEdge<E: Copy + Default + 'static> {
    pub value: E,
    pub dst: u32,
    pub next_in_src_list: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphError {
    Region(RegionError),
    InvalidNode,
    InvalidEdge,
    IoError(std::io::ErrorKind),
}

impl From<RegionError> for GraphError {
    fn from(e: RegionError) -> Self { Self::Region(e) }
}
impl From<std::io::Error> for GraphError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.kind()) }
}

fn nodes_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.nodes.bin"));
    p
}
fn edges_path(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    let stem = p.file_name().unwrap().to_string_lossy().to_string();
    p.set_file_name(format!("{stem}.edges.bin"));
    p
}

pub struct SharedGraph<
    N: Copy + Default + 'static,
    E: Copy + Default + 'static,
> {
    nodes: SharedRegion<GraphNode<N>>,
    edges: SharedRegion<GraphEdge<E>>,
    _phantom: PhantomData<(N, E)>,
    header_sidecar: subetha_core::HandshakeHeader,
    ring_sidecar: Box<subetha_core::ObservationRing>,
}

impl<
    N: Copy + Default + Send + Sync + 'static,
    E: Copy + Default + Send + Sync + 'static,
> subetha_sidecar::AdaptiveInstance for SharedGraph<N, E> {
    fn header(&self) -> &subetha_core::HandshakeHeader { &self.header_sidecar }
    fn ring(&self) -> &subetha_core::ObservationRing { &self.ring_sidecar }
    fn make_policy(&self) -> Box<dyn subetha_sidecar::Policy> {
        Box::new(subetha_sidecar::NoMigrationPolicy)
    }
}

impl<N: Copy + Default + 'static, E: Copy + Default + 'static>
    SharedGraph<N, E>
{
    pub fn create(
        base_path: impl AsRef<Path>,
        max_nodes: usize,
        max_edges: usize,
    ) -> Result<Self, GraphError> {
        let base = base_path.as_ref();
        let nodes = SharedRegion::create(nodes_path(base), max_nodes)?;
        let edges = SharedRegion::create(edges_path(base), max_edges)?;
        Ok(Self {
            nodes, edges, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn open(
        base_path: impl AsRef<Path>,
        max_nodes: usize,
        max_edges: usize,
    ) -> Result<Self, GraphError> {
        let base = base_path.as_ref();
        let nodes = SharedRegion::open(nodes_path(base), max_nodes)?;
        let edges = SharedRegion::open(edges_path(base), max_edges)?;
        Ok(Self {
            nodes, edges, _phantom: PhantomData,
            header_sidecar: subetha_core::HandshakeHeader::new(),
            ring_sidecar: Box::new(subetha_core::ObservationRing::new()),
        })
    }

    pub fn node_count(&self) -> usize { self.nodes.len() }
    pub fn edge_count(&self) -> usize { self.edges.len() }
    pub fn max_nodes(&self) -> usize { self.nodes.capacity() }
    pub fn max_edges(&self) -> usize { self.edges.capacity() }

    /// Add a new node with `value`. Returns its index.
    pub fn add_node(&self, value: N) -> Result<NodeIndex<N>, GraphError> {
        let r = self.nodes.allocate(GraphNode {
            value,
            first_out_edge: NIL_INDEX,
            n_out_edges: 0,
        });
        self.ring_sidecar.push_op(
            crate::sidecar_ops::graph::OP_ADD_NODE,
            if r.is_err() { 1 } else { 0 },
        );
        Ok(NodeIndex::new(r?.index))
    }

    /// Add an edge from `src` to `dst` carrying `value`. Returns
    /// its index. Single-writer per src node (the linked-list head
    /// update isn't synchronised internally).
    pub fn add_edge(
        &self, src: NodeIndex<N>, dst: NodeIndex<N>, value: E,
    ) -> Result<EdgeIndex<E>, GraphError> {
        let r = self.add_edge_inner(src, dst, value);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::graph::OP_ADD_EDGE,
            if r.is_err() { 1 } else { 0 },
        );
        r
    }

    fn add_edge_inner(
        &self, src: NodeIndex<N>, dst: NodeIndex<N>, value: E,
    ) -> Result<EdgeIndex<E>, GraphError> {
        if src.is_nil() || dst.is_nil()
            || src.index as usize >= self.nodes.capacity()
            || dst.index as usize >= self.nodes.capacity()
        {
            return Err(GraphError::InvalidNode);
        }
        let mut src_node = self.nodes.get(OffsetPtr::new(src.index))?;
        let edge_ptr = self.edges.allocate(GraphEdge {
            value,
            dst: dst.index,
            next_in_src_list: src_node.first_out_edge,
        })?;
        src_node.first_out_edge = edge_ptr.index;
        src_node.n_out_edges = src_node.n_out_edges.wrapping_add(1);
        self.nodes.set(OffsetPtr::new(src.index), src_node)?;
        Ok(EdgeIndex::new(edge_ptr.index))
    }

    /// Read the value at a node index.
    pub fn node_value(&self, idx: NodeIndex<N>) -> Option<N> {
        if idx.is_nil() { return None; }
        self.nodes.get(OffsetPtr::new(idx.index)).ok().map(|n| n.value)
    }

    /// Read the (value, dst) for an edge index.
    pub fn edge_endpoints(&self, idx: EdgeIndex<E>) -> Option<(E, NodeIndex<N>)> {
        if idx.is_nil() { return None; }
        let e = self.edges.get(OffsetPtr::new(idx.index)).ok()?;
        Some((e.value, NodeIndex::new(e.dst)))
    }

    /// Out-degree of a node.
    pub fn out_degree(&self, src: NodeIndex<N>) -> Option<u32> {
        if src.is_nil() { return None; }
        self.nodes.get(OffsetPtr::new(src.index)).ok().map(|n| n.n_out_edges)
    }

    /// Enumerate outgoing edges from `src` as (EdgeIndex, dst, edge_value).
    /// Snapshot at call time; not stable under concurrent writes to src.
    pub fn neighbors(&self, src: NodeIndex<N>) -> Vec<(EdgeIndex<E>, NodeIndex<N>, E)> {
        if src.is_nil() {
            self.ring_sidecar
                .push_op(crate::sidecar_ops::graph::OP_NEIGHBORS, 2);
            return Vec::new();
        }
        let src_node = match self.nodes.get(OffsetPtr::new(src.index)) {
            Ok(n) => n,
            Err(_) => {
                self.ring_sidecar
                    .push_op(crate::sidecar_ops::graph::OP_NEIGHBORS, 2);
                return Vec::new();
            }
        };
        let mut out = Vec::with_capacity(src_node.n_out_edges as usize);
        let mut cur = src_node.first_out_edge;
        let mut visited = 0u32;
        let max_iter = self.edges.capacity() as u32;
        while cur != NIL_INDEX && visited < max_iter {
            let e = match self.edges.get(OffsetPtr::new(cur)) {
                Ok(e) => e, Err(_) => break,
            };
            out.push((EdgeIndex::new(cur), NodeIndex::new(e.dst), e.value));
            cur = e.next_in_src_list;
            visited += 1;
        }
        self.ring_sidecar
            .push_op(crate::sidecar_ops::graph::OP_NEIGHBORS, 0);
        out
    }

    /// Remove an edge from `src`'s out-list. Returns the removed
    /// edge's value if found.
    pub fn remove_edge(
        &self, src: NodeIndex<N>, edge_idx: EdgeIndex<E>,
    ) -> Option<E> {
        let r = self.remove_edge_inner(src, edge_idx);
        self.ring_sidecar.push_op(
            crate::sidecar_ops::graph::OP_REMOVE_EDGE,
            if r.is_none() { 2 } else { 0 },
        );
        r
    }

    fn remove_edge_inner(
        &self, src: NodeIndex<N>, edge_idx: EdgeIndex<E>,
    ) -> Option<E> {
        if src.is_nil() || edge_idx.is_nil() { return None; }
        let mut src_node = self.nodes.get(OffsetPtr::new(src.index)).ok()?;
        let target_value;
        let target_next;
        // Find and unlink.
        if src_node.first_out_edge == edge_idx.index {
            let e = self.edges.get(OffsetPtr::new(edge_idx.index)).ok()?;
            target_value = e.value;
            target_next = e.next_in_src_list;
            src_node.first_out_edge = target_next;
        } else {
            let mut prev_idx = src_node.first_out_edge;
            loop {
                if prev_idx == NIL_INDEX { return None; }
                let mut prev_edge = self.edges.get(OffsetPtr::new(prev_idx)).ok()?;
                if prev_edge.next_in_src_list == edge_idx.index {
                    let removed = self.edges.get(OffsetPtr::new(edge_idx.index)).ok()?;
                    target_value = removed.value;
                    target_next = removed.next_in_src_list;
                    prev_edge.next_in_src_list = target_next;
                    self.edges.set(OffsetPtr::new(prev_idx), prev_edge).ok()?;
                    break;
                }
                prev_idx = prev_edge.next_in_src_list;
            }
        }
        src_node.n_out_edges = src_node.n_out_edges.saturating_sub(1);
        self.nodes.set(OffsetPtr::new(src.index), src_node).ok()?;
        // Free returns the prior value; we don't need it here.
        self.edges.free(OffsetPtr::new(edge_idx.index)).ok();
        Some(target_value)
    }

    pub fn flush(&self) -> Result<(), GraphError> {
        self.nodes.flush()?;
        self.edges.flush()?;
        Ok(())
    }
    pub fn flush_async(&self) -> Result<(), GraphError> {
        self.nodes.flush_async()?;
        self.edges.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_base(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        p.push(format!("subetha-graph-{name}-{pid}"));
        p
    }

    fn cleanup(base: &Path) {
        std::fs::remove_file(nodes_path(base)).ok();
        std::fs::remove_file(edges_path(base)).ok();
    }

    #[test]
    fn create_initial_state_is_empty() {
        let base = tmp_base("init");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 16, 32).unwrap();
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);
        cleanup(&base);
    }

    #[test]
    fn add_node_returns_distinct_indices() {
        let base = tmp_base("add-node");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 16, 32).unwrap();
        let a = g.add_node(10).unwrap();
        let b = g.add_node(20).unwrap();
        let c = g.add_node(30).unwrap();
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_eq!(g.node_value(a), Some(10));
        assert_eq!(g.node_value(b), Some(20));
        assert_eq!(g.node_value(c), Some(30));
        assert_eq!(g.node_count(), 3);
        cleanup(&base);
    }

    #[test]
    fn add_edge_links_correctly() {
        let base = tmp_base("add-edge");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 16, 32).unwrap();
        let a = g.add_node(1).unwrap();
        let b = g.add_node(2).unwrap();
        let c = g.add_node(3).unwrap();
        let _e1 = g.add_edge(a, b, 100).unwrap();
        let _e2 = g.add_edge(a, c, 200).unwrap();
        assert_eq!(g.out_degree(a), Some(2));
        assert_eq!(g.out_degree(b), Some(0));
        let nbrs = g.neighbors(a);
        let mut dsts: Vec<u32> = nbrs.iter().map(|(_, d, _)| d.index).collect();
        let mut vals: Vec<u32> = nbrs.iter().map(|(_, _, v)| *v).collect();
        dsts.sort();
        vals.sort();
        assert_eq!(dsts, vec![b.index, c.index]);
        assert_eq!(vals, vec![100, 200]);
        cleanup(&base);
    }

    #[test]
    fn multiple_edges_from_one_source() {
        let base = tmp_base("multi-edge");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 16, 32).unwrap();
        let src = g.add_node(0).unwrap();
        let dsts: Vec<NodeIndex<u32>> = (1..=5).map(|i| g.add_node(i).unwrap()).collect();
        for (i, &d) in dsts.iter().enumerate() {
            g.add_edge(src, d, (i as u32) * 10).unwrap();
        }
        assert_eq!(g.out_degree(src), Some(5));
        let nbrs = g.neighbors(src);
        assert_eq!(nbrs.len(), 5);
        cleanup(&base);
    }

    #[test]
    fn remove_edge_unlinks_head() {
        let base = tmp_base("remove-head");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 16, 32).unwrap();
        let a = g.add_node(0).unwrap();
        let b = g.add_node(1).unwrap();
        let c = g.add_node(2).unwrap();
        let e1 = g.add_edge(a, b, 100).unwrap();
        let e2 = g.add_edge(a, c, 200).unwrap();
        // e2 was added last so it's at head of the linked list.
        let removed = g.remove_edge(a, e2);
        assert_eq!(removed, Some(200));
        assert_eq!(g.out_degree(a), Some(1));
        let nbrs = g.neighbors(a);
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].0, e1);
        cleanup(&base);
    }

    #[test]
    fn remove_edge_unlinks_middle() {
        let base = tmp_base("remove-middle");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 16, 32).unwrap();
        let src = g.add_node(0).unwrap();
        let dsts: Vec<NodeIndex<u32>> = (1..=4).map(|i| g.add_node(i).unwrap()).collect();
        let edges: Vec<EdgeIndex<u32>> = dsts.iter().enumerate()
            .map(|(i, &d)| g.add_edge(src, d, (i as u32) * 10).unwrap())
            .collect();
        // Remove edges[1] (an arbitrary middle one).
        let removed = g.remove_edge(src, edges[1]);
        assert_eq!(removed, Some(10));
        assert_eq!(g.out_degree(src), Some(3));
        let nbrs = g.neighbors(src);
        assert_eq!(nbrs.len(), 3);
        // The removed edge index isn't in the neighbor list anymore.
        let edge_idxs: Vec<EdgeIndex<u32>> = nbrs.iter().map(|(e, _, _)| *e).collect();
        assert!(!edge_idxs.contains(&edges[1]));
        cleanup(&base);
    }

    #[test]
    fn invalid_node_index_rejected_on_add_edge() {
        let base = tmp_base("invalid-node");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 4, 8).unwrap();
        let a = g.add_node(0).unwrap();
        let bogus = NodeIndex::<u32>::new(999);
        assert_eq!(
            g.add_edge(a, bogus, 1).err(),
            Some(GraphError::InvalidNode)
        );
        assert_eq!(g.add_edge(NodeIndex::NIL, a, 1).err(), Some(GraphError::InvalidNode));
        cleanup(&base);
    }

    #[test]
    fn cross_handle_visibility() {
        let base = tmp_base("cross-handle");
        let w: SharedGraph<u32, u32> = SharedGraph::create(&base, 8, 16).unwrap();
        let r: SharedGraph<u32, u32> = SharedGraph::open(&base, 8, 16).unwrap();
        let a = w.add_node(1).unwrap();
        let b = w.add_node(2).unwrap();
        let e = w.add_edge(a, b, 42).unwrap();
        // Reader sees the same graph.
        assert_eq!(r.node_value(a), Some(1));
        assert_eq!(r.node_value(b), Some(2));
        let nbrs = r.neighbors(a);
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].0, e);
        cleanup(&base);
    }

    #[test]
    fn struct_payload_round_trip() {
        #[derive(Clone, Copy, Debug, PartialEq, Default)]
        #[repr(C)]
        struct Page { url_hash: u64, depth: u32 }
        #[derive(Clone, Copy, Debug, PartialEq, Default)]
        #[repr(C)]
        struct Link { weight: f32, rel: u32 }
        let base = tmp_base("struct");
        let g: SharedGraph<Page, Link> = SharedGraph::create(&base, 8, 16).unwrap();
        let p1 = Page { url_hash: 0xAAAA, depth: 0 };
        let p2 = Page { url_hash: 0xBBBB, depth: 1 };
        let a = g.add_node(p1).unwrap();
        let b = g.add_node(p2).unwrap();
        let _e = g.add_edge(a, b, Link { weight: 0.75, rel: 1 }).unwrap();
        assert_eq!(g.node_value(a), Some(p1));
        let nbrs = g.neighbors(a);
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].1, b);
        assert_eq!(nbrs[0].2.weight, 0.75);
        cleanup(&base);
    }

    #[test]
    fn capacity_exhaustion_returns_error() {
        let base = tmp_base("exhaust");
        let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 3, 2).unwrap();
        let _a = g.add_node(0).unwrap();
        let _b = g.add_node(1).unwrap();
        let _c = g.add_node(2).unwrap();
        // Node region full.
        assert!(g.add_node(3).is_err());
        cleanup(&base);
    }

    #[test]
    fn disk_persistence_survives_reopen() {
        let base = tmp_base("disk");
        let saved_a;
        let saved_b;
        {
            let g: SharedGraph<u32, u32> = SharedGraph::create(&base, 8, 16).unwrap();
            saved_a = g.add_node(100).unwrap();
            saved_b = g.add_node(200).unwrap();
            g.add_edge(saved_a, saved_b, 42).unwrap();
            g.flush().unwrap();
        }
        let g2: SharedGraph<u32, u32> = SharedGraph::open(&base, 8, 16).unwrap();
        assert_eq!(g2.node_count(), 2);
        assert_eq!(g2.node_value(saved_a), Some(100));
        let nbrs = g2.neighbors(saved_a);
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0].2, 42);
        cleanup(&base);
    }
}
