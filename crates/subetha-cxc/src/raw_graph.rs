//! `RawGraph` - the shared directed graph at node and edge value sizes
//! chosen at run time.
//!
//! [`SharedGraph`](crate::shared_graph::SharedGraph) is generic over the
//! values its nodes and edges carry, which a caller binding through C
//! cannot name. This holds the same two regions and takes those sizes as
//! arguments instead.
//!
//! # Shape
//!
//! A node slot is its value followed by the head of its out-edge list and
//! the length of that list. An edge slot is its value followed by the
//! node it points at and the next edge in the same source's list. So the
//! out-edges of a node are a singly-linked chain through the edge region,
//! and walking them touches only the edges that node owns.
//!
//! Values cross as byte slices of exactly the declared size. A slice of
//! any other length is refused rather than padded or truncated.
//!
//! # What it does not do
//!
//! Removing a node is not offered. Its slot could be freed, but every
//! edge pointing at it lives in another node's chain, and finding those
//! means walking every chain in the graph. A caller that needs it can
//! remove the edges it knows about and leave the node unreachable; a
//! sweep that does it properly belongs above this layer, where the
//! caller's own index of who-points-at-what lives.

use std::path::{Path, PathBuf};

use crate::raw_region::RawRegion;
use crate::raw_treiber_stack::ElementLayout;
use crate::shared_region::RegionError;

/// The index that means "no node" or "no edge".
pub const RAW_NIL_INDEX: u32 = u32::MAX;

/// The two `u32` fields a node slot carries after its value: the head of
/// its out-edge chain and how many edges are on it.
const NODE_TRAILER: usize = 8;
/// The two an edge slot carries: the node it points at and the next edge
/// in the same source's chain.
const EDGE_TRAILER: usize = 8;

/// Why an operation on the graph could not be carried out.
#[derive(Debug)]
pub enum RawGraphError {
    /// A value slice was not the size this graph was built for.
    WrongSize { expected: usize, found: usize },
    /// The index names no live node.
    InvalidNode,
    /// The index names no live edge.
    InvalidEdge,
    Region(RegionError),
}

impl From<RegionError> for RawGraphError {
    fn from(e: RegionError) -> Self {
        Self::Region(e)
    }
}

/// An index the region rejects is a dead slot; anything else it says is a
/// real failure and keeps its own words. Collapsing the two would make a
/// full region or a bad layout read as "that node is not there".
fn as_node_error(e: RegionError) -> RawGraphError {
    match e {
        RegionError::InvalidPtr => RawGraphError::InvalidNode,
        other => RawGraphError::Region(other),
    }
}

fn as_edge_error(e: RegionError) -> RawGraphError {
    match e {
        RegionError::InvalidPtr => RawGraphError::InvalidEdge,
        other => RawGraphError::Region(other),
    }
}

fn nodes_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".nodes.bin");
    PathBuf::from(p)
}

fn edges_path(base: &Path) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".edges.bin");
    PathBuf::from(p)
}

fn node_layout(node_value_size: usize) -> ElementLayout {
    ElementLayout {
        slot_size: node_value_size + NODE_TRAILER,
        alignment: 8,
        tag: 0x4752_4150_484E_4F44,
    }
}

fn edge_layout(edge_value_size: usize) -> ElementLayout {
    ElementLayout {
        slot_size: edge_value_size + EDGE_TRAILER,
        alignment: 8,
        tag: 0x4752_4150_4845_4447,
    }
}

pub struct RawGraph {
    nodes: RawRegion,
    edges: RawRegion,
    node_value_size: usize,
    edge_value_size: usize,
}

impl RawGraph {
    /// Create the graph under `base_path`, or attach to one already there
    /// with the same shape.
    pub fn create(
        base_path: impl AsRef<Path>,
        max_nodes: usize,
        max_edges: usize,
        node_value_size: usize,
        edge_value_size: usize,
    ) -> Result<Self, RawGraphError> {
        let base = base_path.as_ref();
        let nodes = RawRegion::create(nodes_path(base), max_nodes, node_layout(node_value_size))?;
        let edges = RawRegion::create(edges_path(base), max_edges, edge_layout(edge_value_size))?;
        Ok(Self { nodes, edges, node_value_size, edge_value_size })
    }

    /// Attach to a graph another process created under `base_path`.
    pub fn open(
        base_path: impl AsRef<Path>,
        max_nodes: usize,
        max_edges: usize,
        node_value_size: usize,
        edge_value_size: usize,
    ) -> Result<Self, RawGraphError> {
        let base = base_path.as_ref();
        let nodes = RawRegion::open(nodes_path(base), max_nodes, node_layout(node_value_size))?;
        let edges = RawRegion::open(edges_path(base), max_edges, edge_layout(edge_value_size))?;
        Ok(Self { nodes, edges, node_value_size, edge_value_size })
    }

    pub fn node_value_size(&self) -> usize {
        self.node_value_size
    }

    pub fn edge_value_size(&self) -> usize {
        self.edge_value_size
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn max_nodes(&self) -> usize {
        self.nodes.capacity()
    }

    pub fn max_edges(&self) -> usize {
        self.edges.capacity()
    }

    fn sized(what: &[u8], expected: usize) -> Result<(), RawGraphError> {
        if what.len() == expected {
            Ok(())
        } else {
            Err(RawGraphError::WrongSize { expected, found: what.len() })
        }
    }

    fn read_node(&self, node: u32) -> Result<Vec<u8>, RawGraphError> {
        let mut slot = vec![0u8; self.node_value_size + NODE_TRAILER];
        self.nodes.get(node, &mut slot).map_err(as_node_error)?;
        Ok(slot)
    }

    fn read_edge(&self, edge: u32) -> Result<Vec<u8>, RawGraphError> {
        let mut slot = vec![0u8; self.edge_value_size + EDGE_TRAILER];
        self.edges.get(edge, &mut slot).map_err(as_edge_error)?;
        Ok(slot)
    }

    fn node_head(slot: &[u8], value_size: usize) -> u32 {
        u32::from_le_bytes(slot[value_size..value_size + 4].try_into().expect("four bytes"))
    }

    fn node_degree(slot: &[u8], value_size: usize) -> u32 {
        u32::from_le_bytes(slot[value_size + 4..value_size + 8].try_into().expect("four bytes"))
    }

    fn edge_dst(slot: &[u8], value_size: usize) -> u32 {
        u32::from_le_bytes(slot[value_size..value_size + 4].try_into().expect("four bytes"))
    }

    fn edge_next(slot: &[u8], value_size: usize) -> u32 {
        u32::from_le_bytes(slot[value_size + 4..value_size + 8].try_into().expect("four bytes"))
    }

    /// Add a node carrying `value`, answering the index it took.
    pub fn add_node(&self, value: &[u8]) -> Result<u32, RawGraphError> {
        Self::sized(value, self.node_value_size)?;
        let mut slot = vec![0u8; self.node_value_size + NODE_TRAILER];
        slot[..self.node_value_size].copy_from_slice(value);
        slot[self.node_value_size..self.node_value_size + 4]
            .copy_from_slice(&RAW_NIL_INDEX.to_le_bytes());
        slot[self.node_value_size + 4..].copy_from_slice(&0u32.to_le_bytes());
        Ok(self.nodes.allocate(&slot)?)
    }

    /// Add an edge from `src` to `dst` carrying `value`, answering the
    /// index it took. Both nodes must be live.
    ///
    /// The edge goes at the front of the source's chain, so the order
    /// a walk sees is the reverse of the order edges were added. Nothing
    /// here promises an order; a caller that needs one sorts.
    pub fn add_edge(&self, src: u32, dst: u32, value: &[u8]) -> Result<u32, RawGraphError> {
        Self::sized(value, self.edge_value_size)?;
        let src_slot = self.read_node(src)?;
        // The destination is read only to prove it is live: an edge to a
        // freed slot is a dangling edge nothing later can detect.
        self.read_node(dst)?;

        let old_head = Self::node_head(&src_slot, self.node_value_size);
        let degree = Self::node_degree(&src_slot, self.node_value_size);

        let mut edge = vec![0u8; self.edge_value_size + EDGE_TRAILER];
        edge[..self.edge_value_size].copy_from_slice(value);
        edge[self.edge_value_size..self.edge_value_size + 4]
            .copy_from_slice(&dst.to_le_bytes());
        edge[self.edge_value_size + 4..].copy_from_slice(&old_head.to_le_bytes());
        let index = self.edges.allocate(&edge)?;

        // The node's head and degree are rewritten together, so a reader
        // never sees a chain whose length disagrees with what is on it.
        let mut updated = src_slot;
        updated[self.node_value_size..self.node_value_size + 4]
            .copy_from_slice(&index.to_le_bytes());
        updated[self.node_value_size + 4..]
            .copy_from_slice(&(degree + 1).to_le_bytes());
        self.nodes.set(src, &updated).map_err(as_node_error)?;
        Ok(index)
    }

    /// The value a node carries, into `value_out`.
    pub fn node_value(&self, node: u32, value_out: &mut [u8]) -> Result<(), RawGraphError> {
        Self::sized(value_out, self.node_value_size)?;
        let slot = self.read_node(node)?;
        value_out.copy_from_slice(&slot[..self.node_value_size]);
        Ok(())
    }

    /// The value an edge carries and the node it points at.
    pub fn edge_target(&self, edge: u32, value_out: &mut [u8]) -> Result<u32, RawGraphError> {
        Self::sized(value_out, self.edge_value_size)?;
        let slot = self.read_edge(edge)?;
        value_out.copy_from_slice(&slot[..self.edge_value_size]);
        Ok(Self::edge_dst(&slot, self.edge_value_size))
    }

    /// How many edges leave `node`.
    pub fn out_degree(&self, node: u32) -> Result<u32, RawGraphError> {
        let slot = self.read_node(node)?;
        Ok(Self::node_degree(&slot, self.node_value_size))
    }

    /// The first edge leaving `node`, or [`RAW_NIL_INDEX`] where it has
    /// none. Walk the rest with [`next_edge`](Self::next_edge).
    pub fn first_edge(&self, node: u32) -> Result<u32, RawGraphError> {
        let slot = self.read_node(node)?;
        Ok(Self::node_head(&slot, self.node_value_size))
    }

    /// The edge after `edge` in the same source's chain, or
    /// [`RAW_NIL_INDEX`] at the end.
    ///
    /// Walking with this rather than collecting a list is what lets a
    /// caller in another language read a graph of any size without the
    /// two sides agreeing on how to hand a vector across.
    pub fn next_edge(&self, edge: u32) -> Result<u32, RawGraphError> {
        let slot = self.read_edge(edge)?;
        Ok(Self::edge_next(&slot, self.edge_value_size))
    }

    /// Remove the edge at `edge` from `src`'s chain and free its slot.
    ///
    /// The chain is walked from the head, so this costs the length of
    /// that one node's chain and touches no other node.
    pub fn remove_edge(&self, src: u32, edge: u32) -> Result<(), RawGraphError> {
        let src_slot = self.read_node(src)?;
        let head = Self::node_head(&src_slot, self.node_value_size);
        let degree = Self::node_degree(&src_slot, self.node_value_size);
        if head == RAW_NIL_INDEX {
            return Err(RawGraphError::InvalidEdge);
        }

        let target = self.read_edge(edge)?;
        let after = Self::edge_next(&target, self.edge_value_size);

        if head == edge {
            let mut updated = src_slot;
            updated[self.node_value_size..self.node_value_size + 4]
                .copy_from_slice(&after.to_le_bytes());
            updated[self.node_value_size + 4..]
                .copy_from_slice(&degree.saturating_sub(1).to_le_bytes());
            self.nodes.set(src, &updated).map_err(as_node_error)?;
            self.edges.free_slot(edge)?;
            return Ok(());
        }

        let mut previous = head;
        loop {
            let slot = self.read_edge(previous)?;
            let next = Self::edge_next(&slot, self.edge_value_size);
            if next == RAW_NIL_INDEX {
                // The edge is not on this node's chain. Freeing it here
                // would leave whichever chain does hold it pointing at a
                // free slot, so it is refused instead.
                return Err(RawGraphError::InvalidEdge);
            }
            if next == edge {
                let mut updated = slot;
                updated[self.edge_value_size + 4..].copy_from_slice(&after.to_le_bytes());
                self.edges.set(previous, &updated).map_err(as_edge_error)?;
                let mut node_updated = src_slot;
                node_updated[self.node_value_size + 4..]
                    .copy_from_slice(&degree.saturating_sub(1).to_le_bytes());
                self.nodes.set(src, &node_updated).map_err(as_node_error)?;
                self.edges.free_slot(edge)?;
                return Ok(());
            }
            previous = next;
        }
    }

    /// Push both regions to disk and wait for them.
    pub fn flush(&self) -> Result<(), RawGraphError> {
        self.nodes.flush()?;
        self.edges.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("subetha_raw_graph_{name}_{}", std::process::id()));
        p
    }

    fn cleanup(base: &Path) {
        for p in [nodes_path(base), edges_path(base)] {
            match std::fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("could not clear {}: {e}", p.display()),
            }
        }
    }

    /// Every edge leaving `node`, as (edge index, destination).
    fn walk(g: &RawGraph, node: u32) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        let mut e = g.first_edge(node).expect("first edge");
        let mut value = vec![0u8; g.edge_value_size()];
        while e != RAW_NIL_INDEX {
            let dst = g.edge_target(e, &mut value).expect("target");
            out.push((e, dst));
            e = g.next_edge(e).expect("next");
        }
        out
    }

    #[test]
    fn nodes_and_edges_at_runtime_sizes() {
        let base = scratch("basic");
        cleanup(&base);
        let g = RawGraph::create(&base, 16, 32, 8, 4).expect("created");
        assert_eq!(g.node_value_size(), 8);
        assert_eq!(g.edge_value_size(), 4);

        let a = g.add_node(&1u64.to_le_bytes()).expect("node a");
        let b = g.add_node(&2u64.to_le_bytes()).expect("node b");
        assert_eq!(g.node_count(), 2);

        let mut value = [0u8; 8];
        g.node_value(a, &mut value).expect("value");
        assert_eq!(u64::from_le_bytes(value), 1);

        let e = g.add_edge(a, b, &7u32.to_le_bytes()).expect("edge");
        assert_eq!(g.edge_count(), 1);
        assert_eq!(g.out_degree(a).expect("degree"), 1);
        assert_eq!(g.out_degree(b).expect("degree"), 0);

        let mut edge_value = [0u8; 4];
        assert_eq!(g.edge_target(e, &mut edge_value).expect("target"), b);
        assert_eq!(u32::from_le_bytes(edge_value), 7);

        cleanup(&base);
    }

    #[test]
    fn the_out_edges_of_a_node_are_walkable_and_belong_only_to_it() {
        let base = scratch("walk");
        cleanup(&base);
        let g = RawGraph::create(&base, 16, 32, 8, 4).expect("created");
        let a = g.add_node(&0u64.to_le_bytes()).expect("a");
        let b = g.add_node(&1u64.to_le_bytes()).expect("b");
        let c = g.add_node(&2u64.to_le_bytes()).expect("c");

        g.add_edge(a, b, &10u32.to_le_bytes()).expect("a-b");
        g.add_edge(a, c, &11u32.to_le_bytes()).expect("a-c");
        g.add_edge(b, c, &12u32.to_le_bytes()).expect("b-c");

        let from_a = walk(&g, a);
        assert_eq!(from_a.len(), 2);
        assert_eq!(g.out_degree(a).expect("degree") as usize, from_a.len());
        let mut targets: Vec<u32> = from_a.iter().map(|(_, d)| *d).collect();
        targets.sort_unstable();
        assert_eq!(targets, vec![b, c]);

        // b's chain holds only its own edge, not a's.
        let from_b = walk(&g, b);
        assert_eq!(from_b.len(), 1);
        assert_eq!(from_b[0].1, c);

        assert_eq!(walk(&g, c).len(), 0);

        cleanup(&base);
    }

    #[test]
    fn a_value_of_the_wrong_size_is_refused_rather_than_padded() {
        let base = scratch("sizes");
        cleanup(&base);
        let g = RawGraph::create(&base, 8, 8, 8, 4).expect("created");
        assert!(matches!(
            g.add_node(&[0u8; 4]),
            Err(RawGraphError::WrongSize { expected: 8, found: 4 })
        ));
        let a = g.add_node(&[0u8; 8]).expect("a");
        assert!(matches!(
            g.add_edge(a, a, &[0u8; 8]),
            Err(RawGraphError::WrongSize { expected: 4, found: 8 })
        ));
        cleanup(&base);
    }

    #[test]
    fn an_edge_to_a_node_that_does_not_exist_is_refused() {
        let base = scratch("dangling");
        cleanup(&base);
        let g = RawGraph::create(&base, 8, 8, 8, 4).expect("created");
        let a = g.add_node(&[0u8; 8]).expect("a");
        // An edge to a slot nothing allocated would dangle, and nothing
        // later could tell it apart from a live one.
        assert!(matches!(g.add_edge(a, 999, &[0u8; 4]), Err(RawGraphError::InvalidNode)));
        assert!(matches!(g.add_edge(999, a, &[0u8; 4]), Err(RawGraphError::InvalidNode)));
        assert_eq!(g.edge_count(), 0);
        cleanup(&base);
    }

    #[test]
    fn removing_an_edge_unlinks_it_from_the_middle_of_a_chain() {
        let base = scratch("remove");
        cleanup(&base);
        let g = RawGraph::create(&base, 8, 8, 8, 4).expect("created");
        let a = g.add_node(&[0u8; 8]).expect("a");
        let b = g.add_node(&[1u8; 8]).expect("b");

        let first = g.add_edge(a, b, &1u32.to_le_bytes()).expect("e1");
        let second = g.add_edge(a, b, &2u32.to_le_bytes()).expect("e2");
        let third = g.add_edge(a, b, &3u32.to_le_bytes()).expect("e3");
        assert_eq!(g.out_degree(a).expect("degree"), 3);

        // The chain is most-recent first, so `second` sits in the middle.
        g.remove_edge(a, second).expect("remove the middle");
        assert_eq!(g.out_degree(a).expect("degree"), 2);
        let left: Vec<u32> = walk(&g, a).iter().map(|(e, _)| *e).collect();
        assert_eq!(left, vec![third, first]);

        // And the head.
        g.remove_edge(a, third).expect("remove the head");
        assert_eq!(g.out_degree(a).expect("degree"), 1);
        assert_eq!(walk(&g, a).iter().map(|(e, _)| *e).collect::<Vec<_>>(), vec![first]);

        cleanup(&base);
    }

    #[test]
    fn removing_an_edge_from_a_node_that_does_not_own_it_is_refused() {
        let base = scratch("wrongowner");
        cleanup(&base);
        let g = RawGraph::create(&base, 8, 8, 8, 4).expect("created");
        let a = g.add_node(&[0u8; 8]).expect("a");
        let b = g.add_node(&[1u8; 8]).expect("b");
        let from_a = g.add_edge(a, b, &1u32.to_le_bytes()).expect("edge");
        g.add_edge(b, a, &2u32.to_le_bytes()).expect("edge");

        // Freeing it here would leave a's chain pointing at a free slot.
        assert!(matches!(g.remove_edge(b, from_a), Err(RawGraphError::InvalidEdge)));
        assert_eq!(g.out_degree(a).expect("degree"), 1);
        assert_eq!(g.out_degree(b).expect("degree"), 1);

        cleanup(&base);
    }

    #[test]
    fn a_second_handle_on_the_same_files_sees_the_same_graph() {
        let base = scratch("shared");
        cleanup(&base);
        let g = RawGraph::create(&base, 8, 8, 8, 4).expect("created");
        let a = g.add_node(&5u64.to_le_bytes()).expect("a");
        let b = g.add_node(&6u64.to_le_bytes()).expect("b");
        g.add_edge(a, b, &9u32.to_le_bytes()).expect("edge");

        let other = RawGraph::open(&base, 8, 8, 8, 4).expect("opened");
        assert_eq!(other.node_count(), 2);
        assert_eq!(other.out_degree(a).expect("degree"), 1);
        let mut value = [0u8; 8];
        other.node_value(b, &mut value).expect("value");
        assert_eq!(u64::from_le_bytes(value), 6);

        cleanup(&base);
    }
}
