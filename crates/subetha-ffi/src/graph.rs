//! The shared directed graph through the C ABI.
//!
//! Node and edge values cross as byte buffers of exactly the sizes the
//! graph was created with. A buffer of any other length is refused rather
//! than padded.
//!
//! # Walking a node's edges
//!
//! Out-edges are read one at a time, with `subetha_graph_first_edge` and
//! then `subetha_graph_next_edge` until the answer is
//! `SUBETHA_GRAPH_NIL`. Nothing hands back a list, deliberately: a graph
//! of any size can be read without the two sides agreeing how a
//! variable-length array crosses the boundary, and without this library
//! allocating on the caller's behalf.
//!
//! # What it refuses, and why each refusal keeps the graph sound
//!
//! An edge to a node index nothing allocated is refused, because a
//! dangling edge is indistinguishable from a live one afterwards.
//! Removing an edge from a node that does not own it is refused, because
//! freeing it would leave the chain that does own it pointing at a free
//! slot. Removing a node is not offered at all: every edge pointing at it
//! lives in some other node's chain, so finding them means walking the
//! whole graph, and that sweep belongs where the caller's own index of
//! who-points-at-what lives.

use std::ffi::c_char;

use subetha_cxc::raw_graph::{RawGraph, RawGraphError, RAW_NIL_INDEX};
use subetha_cxc::shared_region::RegionError;

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_FULL, SUBETHA_E_RING_IO,
    SUBETHA_E_RING_LAYOUT_MISMATCH, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_GRAPH};
use crate::ring::{bytes, text};
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The index that means no node or no edge. A walk ends when
/// `subetha_graph_next_edge` answers this.
pub const SUBETHA_GRAPH_NIL: u32 = u32::MAX;

// A literal, because the header generator copies a constant's expression
// rather than its value. The assertion is what keeps it honest.
const _: () = assert!(SUBETHA_GRAPH_NIL == RAW_NIL_INDEX);

pub(crate) struct GraphObject {
    graph: RawGraph,
    mode: u32,
}

impl GraphObject {
    /// Nothing parks on a graph, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

/// A snapshot of a shared graph.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_graph_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Nodes it holds now.
    pub node_count: u64,
    /// Edges it holds now.
    pub edge_count: u64,
    /// Nodes it can hold.
    pub max_nodes: u64,
    /// Edges it can hold.
    pub max_edges: u64,
    /// Bytes in a node's value.
    pub node_value_size: u64,
    /// Bytes in an edge's value.
    pub edge_value_size: u64,
}

fn code_for(e: RawGraphError) -> i32 {
    match e {
        RawGraphError::WrongSize { expected, found } => fail(
            SUBETHA_E_INVALID_ARGUMENT,
            format!("this graph uses {expected}-byte values here, and {found} was given"),
        ),
        RawGraphError::InvalidNode => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "that index names no live node")
        }
        RawGraphError::InvalidEdge => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "that index names no live edge on this node")
        }
        RawGraphError::Region(RegionError::Full) => {
            fail(SUBETHA_E_MAP_FULL, "the graph has no free slot")
        }
        RawGraphError::Region(RegionError::LayoutMismatch) => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the graph on disk was built with different value sizes",
        ),
        RawGraphError::Region(RegionError::IoError(k)) => {
            fail(SUBETHA_E_RING_IO, format!("the graph could not be reached: {k:?}"))
        }
        RawGraphError::Region(other) => {
            fail(SUBETHA_E_RING_IO, format!("the graph refused: {other:?}"))
        }
    }
}

fn with_graph(handle: subetha_handle, f: impl FnOnce(&GraphObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_GRAPH, |object| match object {
        Object::Graph(g) => f(g),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a graph"),
    })
}

/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments(
    base_path: *const c_char,
    node_value_size: u64,
    edge_value_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'static str, usize, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = unsafe { text(base_path, "base_path") }?;
    let mode = resolve_mode(mode)?;
    if node_value_size == 0 || edge_value_size == 0 {
        return Err(fail(
            SUBETHA_E_INVALID_ARGUMENT,
            "a node or edge value of zero bytes carries nothing",
        ));
    }
    Ok((path, node_value_size as usize, edge_value_size as usize, mode))
}

/// Create a graph under `base_path`, holding up to `max_nodes` nodes and
/// `max_edges` edges whose values are the given sizes.
///
/// Two files are made beside that prefix: the nodes and the edges.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_create(
    base_path: *const c_char,
    max_nodes: u64,
    max_edges: u64,
    node_value_size: u64,
    edge_value_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, node_value_size, edge_value_size, mode) = match unsafe {
            read_arguments(base_path, node_value_size, edge_value_size, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawGraph::create(
            path,
            max_nodes as usize,
            max_edges as usize,
            node_value_size,
            edge_value_size,
        ) {
            Ok(g) => unsafe { issue(Object::Graph(GraphObject { graph: g, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to a graph another process created under `base_path`, with the
/// same shape.
///
/// # Safety
/// `base_path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_open(
    base_path: *const c_char,
    max_nodes: u64,
    max_edges: u64,
    node_value_size: u64,
    edge_value_size: u64,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, node_value_size, edge_value_size, mode) = match unsafe {
            read_arguments(base_path, node_value_size, edge_value_size, mode, out)
        } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match RawGraph::open(
            path,
            max_nodes as usize,
            max_edges as usize,
            node_value_size,
            edge_value_size,
        ) {
            Ok(g) => unsafe { issue(Object::Graph(GraphObject { graph: g, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Add a node carrying `value`, writing the index it took into `node_out`.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `node_out` is valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_add_node(
    handle: subetha_handle,
    value: *const u8,
    value_len: u64,
    node_out: *mut u32,
) -> i32 {
    with_graph(handle, |g| {
        if node_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "node_out is null");
        }
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match g.graph.add_node(value) {
            Ok(index) => {
                // Checked non-null above.
                unsafe { *node_out = index };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Add an edge from `src` to `dst` carrying `value`, writing the index it
/// took into `edge_out`. Both nodes must be live.
///
/// The edge goes at the front of the source's chain, so a walk sees the
/// reverse of the order edges were added. No order is promised.
///
/// # Safety
/// `value` points to at least `value_len` bytes; `edge_out` is valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_add_edge(
    handle: subetha_handle,
    src: u32,
    dst: u32,
    value: *const u8,
    value_len: u64,
    edge_out: *mut u32,
) -> i32 {
    with_graph(handle, |g| {
        if edge_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "edge_out is null");
        }
        let value = match unsafe { bytes(value, value_len as usize) } {
            Ok(v) => v,
            Err(code) => return code,
        };
        match g.graph.add_edge(src, dst, value) {
            Ok(index) => {
                // Checked non-null above.
                unsafe { *edge_out = index };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Read the value node `node` carries into `value_out`.
///
/// # Safety
/// `value_out` points to at least `value_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_node_value(
    handle: subetha_handle,
    node: u32,
    value_out: *mut u8,
    value_len: u64,
) -> i32 {
    with_graph(handle, |g| {
        if value_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "value_out is null");
        }
        let mut scratch = vec![0u8; value_len as usize];
        match g.graph.node_value(node, &mut scratch) {
            Ok(()) => {
                // Checked non-null above; the length is the caller's own.
                unsafe {
                    std::ptr::copy_nonoverlapping(scratch.as_ptr(), value_out, scratch.len());
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Read the value edge `edge` carries into `value_out`, and write the node
/// it points at into `dst_out`.
///
/// # Safety
/// `value_out` points to at least `value_len` bytes; `dst_out` is valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_edge_target(
    handle: subetha_handle,
    edge: u32,
    value_out: *mut u8,
    value_len: u64,
    dst_out: *mut u32,
) -> i32 {
    with_graph(handle, |g| {
        if value_out.is_null() || dst_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "value_out or dst_out is null");
        }
        let mut scratch = vec![0u8; value_len as usize];
        match g.graph.edge_target(edge, &mut scratch) {
            Ok(dst) => {
                // Checked non-null above.
                unsafe {
                    std::ptr::copy_nonoverlapping(scratch.as_ptr(), value_out, scratch.len());
                    *dst_out = dst;
                }
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// How many edges leave `node`, into `degree_out`.
///
/// # Safety
/// `degree_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_out_degree(
    handle: subetha_handle,
    node: u32,
    degree_out: *mut u32,
) -> i32 {
    with_graph(handle, |g| {
        if degree_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "degree_out is null");
        }
        match g.graph.out_degree(node) {
            Ok(degree) => {
                // Checked non-null above.
                unsafe { *degree_out = degree };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// The first edge leaving `node`, into `edge_out`, or `SUBETHA_GRAPH_NIL`
/// where it has none.
///
/// # Safety
/// `edge_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_first_edge(
    handle: subetha_handle,
    node: u32,
    edge_out: *mut u32,
) -> i32 {
    with_graph(handle, |g| {
        if edge_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "edge_out is null");
        }
        match g.graph.first_edge(node) {
            Ok(edge) => {
                // Checked non-null above.
                unsafe { *edge_out = edge };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// The edge after `edge` in the same source's chain, into `edge_out`, or
/// `SUBETHA_GRAPH_NIL` at the end.
///
/// # Safety
/// `edge_out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_next_edge(
    handle: subetha_handle,
    edge: u32,
    edge_out: *mut u32,
) -> i32 {
    with_graph(handle, |g| {
        if edge_out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "edge_out is null");
        }
        match g.graph.next_edge(edge) {
            Ok(next) => {
                // Checked non-null above.
                unsafe { *edge_out = next };
                SUBETHA_OK
            }
            Err(e) => code_for(e),
        }
    })
}

/// Remove `edge` from `src`'s chain and free its slot.
///
/// Refused where `src` does not own that edge, since freeing it would
/// leave whichever chain does own it pointing at a free slot.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_graph_remove_edge(
    handle: subetha_handle,
    src: u32,
    edge: u32,
) -> i32 {
    with_graph(handle, |g| match g.graph.remove_edge(src, edge) {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Read the graph's shape and occupancy into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_graph_read_stats(
    handle: subetha_handle,
    out: *mut subetha_graph_stats,
) -> i32 {
    with_graph(handle, |g| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let stats = subetha_graph_stats {
            mode: g.mode,
            node_count: g.graph.node_count() as u64,
            edge_count: g.graph.edge_count() as u64,
            max_nodes: g.graph.max_nodes() as u64,
            max_edges: g.graph.max_edges() as u64,
            node_value_size: g.graph.node_value_size() as u64,
            edge_value_size: g.graph.edge_value_size() as u64,
        };
        // Checked non-null above.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}

/// Push both of the graph's files to disk and wait for them.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_graph_flush(handle: subetha_handle) -> i32 {
    with_graph(handle, |g| match g.graph.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}
