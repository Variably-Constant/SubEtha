//! The shared topology map through the C ABI: an N-by-N matrix of message
//! counts in a file, and the shape it implies.
//!
//! Peers record who sent to whom; the map counts the edges and reads a
//! topology off the fan-out and fan-in against thresholds the caller sets.
//! The recommendation is advisory: it names the shape the traffic looks
//! like, and a caller routing on it decides what to do about that. The map
//! runs no background work, so strict and managed modes are the same.

use std::ffi::c_char;
use std::path::Path;

use subetha_cxc::shared_topology_map::{
    SharedTopologyMap, TopologyError, TopologyKind, DEFAULT_FAN_IN_THRESHOLD,
    DEFAULT_FAN_OUT_THRESHOLD,
};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_RING_IO, SUBETHA_E_RING_LAYOUT_MISMATCH,
    SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_TOPOLOGY};
use crate::ring::text;
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// One producer to one consumer.
pub const SUBETHA_TOPOLOGY_POINT_TO_POINT: u32 = 0;
/// One producer to many consumers.
pub const SUBETHA_TOPOLOGY_BROADCAST_TREE: u32 = 1;
/// Peers routing to one another.
pub const SUBETHA_TOPOLOGY_ALL_TO_ALL_MESH: u32 = 2;

/// The fan-out at or above which the traffic reads as a broadcast tree.
pub const SUBETHA_TOPOLOGY_FAN_OUT_DEFAULT: u32 = 3;
/// The fan-in at or above which the traffic reads as an all-to-all mesh.
pub const SUBETHA_TOPOLOGY_FAN_IN_DEFAULT: u32 = 3;

const _: () = assert!(SUBETHA_TOPOLOGY_FAN_OUT_DEFAULT == DEFAULT_FAN_OUT_THRESHOLD);
const _: () = assert!(SUBETHA_TOPOLOGY_FAN_IN_DEFAULT == DEFAULT_FAN_IN_THRESHOLD);

/// A snapshot of a shared topology map.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_topology_stats {
    /// The mode this handle was created in.
    pub mode: u32,
    /// Nodes the matrix covers.
    pub n_nodes: u32,
    /// Messages recorded across every edge.
    pub total_msgs: u64,
    /// The largest number of distinct destinations any one node sent to.
    pub max_fan_out: u32,
    /// The node that sent to that many.
    pub max_fan_out_src: u32,
    /// The largest number of distinct sources any one node received from.
    pub max_fan_in: u32,
    /// The node that received from that many.
    pub max_fan_in_dst: u32,
    /// The shape the traffic reads as: one of the `SUBETHA_TOPOLOGY_`
    /// constants.
    pub recommendation: u32,
    /// Bumped whenever the recommendation changes, so a caller can tell a
    /// re-read from a re-decision.
    pub recommendation_epoch: u64,
}

pub(crate) struct TopologyObject {
    map: SharedTopologyMap,
    mode: u32,
}

impl TopologyObject {
    /// A map parks nothing inside a call, so a destroy has nobody to wake.
    pub(crate) fn interrupt(&self) {}
}

fn kind_of(kind: TopologyKind) -> u32 {
    match kind {
        TopologyKind::PointToPoint => SUBETHA_TOPOLOGY_POINT_TO_POINT,
        TopologyKind::BroadcastTree => SUBETHA_TOPOLOGY_BROADCAST_TREE,
        TopologyKind::AllToAllMesh => SUBETHA_TOPOLOGY_ALL_TO_ALL_MESH,
    }
}

fn code_for(e: TopologyError) -> i32 {
    match e {
        TopologyError::NodeIndexOutOfBounds => {
            fail(SUBETHA_E_INVALID_ARGUMENT, "that node index is past the last node")
        }
        TopologyError::LayoutMismatch => fail(
            SUBETHA_E_RING_LAYOUT_MISMATCH,
            "the map on disk was built for another node count",
        ),
        TopologyError::IoError(kind) => fail(SUBETHA_E_RING_IO, format!("io error: {kind}")),
    }
}

fn with_topology(handle: subetha_handle, f: impl FnOnce(&TopologyObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_TOPOLOGY, |object| match object {
        Object::Topology(t) => f(t),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name a topology map"),
    })
}

/// The arguments every constructor reads, in order, so the first refusal
/// names the argument at fault.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
unsafe fn read_arguments<'a>(
    path: *const c_char,
    n_nodes: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> Result<(&'a Path, usize, u32), i32> {
    require_initialized()?;
    if out.is_null() {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "out is null"));
    }
    let path = Path::new(unsafe { text(path, "path") }?);
    if n_nodes == 0 {
        return Err(fail(SUBETHA_E_INVALID_ARGUMENT, "n_nodes is zero"));
    }
    let mode = resolve_mode(mode)?;
    Ok((path, n_nodes as usize, mode))
}

/// Obtain the map at `path` covering `n_nodes` nodes, classifying at
/// `fan_out_threshold` and `fan_in_threshold`: an empty one is initialized
/// when the file does not exist, an existing one is attached with its
/// counts in place. A map built for another node count is a
/// `SUBETHA_E_RING_LAYOUT_MISMATCH`.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_topology_create(
    path: *const c_char,
    n_nodes: u32,
    fan_out_threshold: u32,
    fan_in_threshold: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, n_nodes, mode) = match unsafe { read_arguments(path, n_nodes, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedTopologyMap::create_with_thresholds(
            path,
            n_nodes,
            fan_out_threshold,
            fan_in_threshold,
        ) {
            Ok(map) => unsafe { issue(Object::Topology(TopologyObject { map, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Truncate the map's file at `path` and initialize an empty one,
/// discarding every count live peers share.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_topology_reset(
    path: *const c_char,
    n_nodes: u32,
    fan_out_threshold: u32,
    fan_in_threshold: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, n_nodes, mode) = match unsafe { read_arguments(path, n_nodes, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedTopologyMap::reset(path, n_nodes, fan_out_threshold, fan_in_threshold) {
            Ok(map) => unsafe { issue(Object::Topology(TopologyObject { map, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Attach to the map another process created at `path`; the file must
/// exist. `SUBETHA_E_RING_IO` names an absent one.
///
/// # Safety
/// `path` is a NUL-terminated UTF-8 string; `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_topology_open(
    path: *const c_char,
    n_nodes: u32,
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        let (path, n_nodes, mode) = match unsafe { read_arguments(path, n_nodes, mode, out) } {
            Ok(a) => a,
            Err(code) => return code,
        };
        match SharedTopologyMap::open(path, n_nodes) {
            Ok(map) => unsafe { issue(Object::Topology(TopologyObject { map, mode }), out) },
            Err(e) => code_for(e),
        }
    })
}

/// Record one message from `src` to `dst`, and report that edge's running
/// count through `out_count` when that is not null.
///
/// # Safety
/// `out_count` is null or a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_topology_record_send(
    handle: subetha_handle,
    src: u32,
    dst: u32,
    out_count: *mut u64,
) -> i32 {
    with_topology(handle, |t| match t.map.record_send(src, dst) {
        Ok(count) => {
            if !out_count.is_null() {
                // Checked non-null; the caller guarantees it is writable.
                unsafe { *out_count = count };
            }
            SUBETHA_OK
        }
        Err(e) => code_for(e),
    })
}

/// The number of distinct destinations `src` has sent to, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_topology_fan_out(
    handle: subetha_handle,
    src: u32,
    out: *mut u32,
) -> i32 {
    with_topology(handle, |t| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if src as usize >= t.map.n_nodes() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "that node index is past the last node");
        }
        let fan_out = t.map.fan_out(src);
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = fan_out };
        SUBETHA_OK
    })
}

/// The number of distinct sources `dst` has received from, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_topology_fan_in(
    handle: subetha_handle,
    dst: u32,
    out: *mut u32,
) -> i32 {
    with_topology(handle, |t| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        if dst as usize >= t.map.n_nodes() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "that node index is past the last node");
        }
        let fan_in = t.map.fan_in(dst);
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = fan_in };
        SUBETHA_OK
    })
}

/// Push the map's dirty pages to disk, returning when they are durable.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_topology_flush(handle: subetha_handle) -> i32 {
    with_topology(handle, |t| match t.map.flush() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// Start pushing the map's dirty pages to disk and return at once.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_topology_flush_async(handle: subetha_handle) -> i32 {
    with_topology(handle, |t| match t.map.flush_async() {
        Ok(()) => SUBETHA_OK,
        Err(e) => code_for(e),
    })
}

/// A snapshot of the map into `out`, which walks every edge.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_topology_read_stats(
    handle: subetha_handle,
    out: *mut subetha_topology_stats,
) -> i32 {
    with_topology(handle, |t| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let s = t.map.stats();
        let stats = subetha_topology_stats {
            mode: t.mode,
            n_nodes: t.map.n_nodes() as u32,
            total_msgs: s.total_msgs,
            max_fan_out: s.max_fan_out,
            max_fan_out_src: s.max_fan_out_src,
            max_fan_in: s.max_fan_in,
            max_fan_in_dst: s.max_fan_in_dst,
            recommendation: kind_of(s.current_recommendation),
            recommendation_epoch: s.recommendation_epoch,
        };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = stats };
        SUBETHA_OK
    })
}
