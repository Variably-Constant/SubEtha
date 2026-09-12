//! The workloads in `c/workloads.c`, each the shape of a program that uses
//! the Rust API in production, driven through the C ABI: several
//! processes on one backing, frames past the slot, the waiting forms under
//! contention, every locale a workload's shape allows, strict and managed
//! modes, and a soak that repeats a workload for as long as
//! `SUBETHA_FFI_SOAK_SECS` says. A process role is this test binary run
//! again with `SUBETHA_FFI_WORKLOAD` naming the role; a ready marker on
//! the child's stdout is what the parent waits for before it spawns the
//! peers.
//!
//! Every run prints one `WL` line per role with what the C side measured,
//! so a `--nocapture` run is the record.

use std::env::VarError;
use std::ffi::CString;
use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use subetha_ffi::{
    subetha_arena_unlink, subetha_atomic_unlink, subetha_btree_unlink, subetha_epochs_unlink,
    subetha_frame_region_unlink, subetha_handle, subetha_handle_destroy, subetha_hashmap_unlink, subetha_init,
    subetha_owner_lease_unlink, subetha_ring_create_anon, subetha_ring_options, subetha_ring_recv_frame,
    subetha_ring_register_consumer, subetha_rwlock_unlink, subetha_sens_self_signed_cert, subetha_sens_tls_available,
    subetha_shutdown, subetha_spsc_create_anon, subetha_unlink_report, subetha_vec_unlink,
    SUBETHA_E_HANDLES_WERE_LIVE, SUBETHA_E_NOT_INITIALIZED, SUBETHA_E_RING_EMPTY, SUBETHA_E_RING_IO,
    SUBETHA_E_SHUT_DOWN, SUBETHA_MODE_MANAGED, SUBETHA_MODE_STRICT, SUBETHA_OK, SUBETHA_RING_FRAME_DEFAULT_BLOCK,
};
use subetha_ffi_tests::{
    subetha_workload_blob_create, subetha_workload_blob_reader, subetha_workload_blob_writer,
    subetha_workload_bus_client, subetha_workload_bus_shell, subetha_workload_deque_consume,
    subetha_workload_deque_create, subetha_workload_deque_open, subetha_workload_deque_produce,
    subetha_workload_fleet_host, subetha_workload_fleet_unlink, subetha_workload_fleet_worker,
    subetha_workload_graph_create, subetha_workload_graph_reader, subetha_workload_graph_verify,
    subetha_workload_graph_writer, subetha_workload_index_create, subetha_workload_index_finish,
    subetha_workload_index_reader, subetha_workload_index_writer, subetha_workload_log_end,
    subetha_workload_log_producer, subetha_workload_log_writer, subetha_workload_memory_create,
    subetha_workload_memory_reader,
    subetha_workload_memory_verify, subetha_workload_memory_writer, subetha_workload_menu_broker,
    subetha_workload_menu_shell, subetha_workload_menu_unlink, subetha_workload_mind_conscious,
    subetha_workload_mind_subconscious, subetha_workload_mvcc_create, subetha_workload_mvcc_reader,
    subetha_workload_mvcc_writer, subetha_workload_pods_consumer, subetha_workload_pods_producer,
    subetha_workload_race_explorer, subetha_workload_rr_client, subetha_workload_rr_serve, subetha_workload_stats,
    subetha_workload_stream_receiver, subetha_workload_stream_sender,
};

/// Every workload initializes the library and shuts it down, and a
/// shutdown closes every handle in the process, so the workloads run one
/// at a time whatever the test thread count. A test that failed while
/// holding the lock poisons it; the next test takes the guard anyway,
/// since the library state is what shutdown left, not the lock's.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    match SERIAL.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Take the serial lock and initialize the library for one test. A test
/// that panicked left the library initialized with its handles open, and
/// the next test's own shutdown would report that leak as its failure;
/// closing it here keeps one cause to one failure. What is closed here
/// has already failed the test that leaked it.
fn begin() -> std::sync::MutexGuard<'static, ()> {
    let guard = serial();
    let rc = subetha_shutdown();
    assert!(
        rc == SUBETHA_OK
            || rc == SUBETHA_E_NOT_INITIALIZED
            || rc == SUBETHA_E_SHUT_DOWN
            || rc == SUBETHA_E_HANDLES_WERE_LIVE,
        "the library is in a state no earlier test could have left it in: code {rc}"
    );
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    guard
}

/// The locales the ring workloads run in, as `c/workloads.c` numbers them.
const LOCALE_FILE: u32 = 0;
const LOCALE_SHM_SESSION: u32 = 1;
const LOCALE_SHM_MACHINE: u32 = 2;

/// The descriptor a machine-namespace region is created with on Windows:
/// authenticated users may map and query it, the creating account and
/// administrators have everything. Ignored elsewhere.
const MACHINE_SDDL: &str = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GRGW;;;AU)";

/// Counts per workload, one pass. The soak repeats passes.
const RR_CLIENTS: u32 = 3;
const RR_REQUESTS_PER_CLIENT: u32 = 200;
const FLEET_WORKERS: u32 = 2;
const FLEET_ROUNDS: u32 = 200;
const RACE_THREADS: u32 = 4;
const RACE_ROUNDS: u32 = 500;
const RACE_STEPS: u32 = 8;
const LOG_PRODUCERS: u32 = 2;
const LOG_LINES: u32 = 5000;
const LOG_LINE_BYTES: u32 = 200;
const LOG_PACE_US: u32 = 100;
const BUS_CLIENTS: u32 = 2;
const BUS_QUERIES: u32 = 50;
const MENUS: u32 = 100;
const PODS_PRODUCERS: u32 = 3;
const PODS_ITEMS: u32 = 2000;
const PODS_BATCH: u32 = 50;
const MIND_TURNS: u32 = 2000;
const DEQUE_PRODUCERS: u32 = 2;
const DEQUE_CONSUMERS: u32 = 2;
const DEQUE_ITEMS: u32 = 100_000;
const BLOB_WRITERS: u32 = 2;
const BLOB_READERS: u32 = 2;
const BLOBS: u32 = 300;
const INDEX_WRITERS: u32 = 2;
const INDEX_READERS: u32 = 2;
const INDEX_ENTRIES: u32 = 500;
const INDEX_GENERATIONS: u32 = 6;
const MVCC_WRITERS: u32 = 2;
const MVCC_READERS: u32 = 2;
const MVCC_KEYS: u32 = 400;
const MVCC_ROUNDS: u32 = 6;
const MVCC_SCANS: u32 = 200;
const GRAPH_WRITERS: u32 = 2;
const GRAPH_READERS: u32 = 2;
const GRAPH_NODES: u32 = 256;
const GRAPH_EDGES_PER_NODE: u32 = 150;
const GRAPH_BLOCKS: u32 = 1024;
const GRAPH_WALKS: u32 = 20;
const MEMORY_WRITERS: u32 = 2;
const MEMORY_READERS: u32 = 2;
const MEMORY_RECORDS: u32 = 2000;
const MEMORY_LOOKUPS: u32 = 4000;
const STREAM_SENDERS: u32 = 2;
const STREAM_STREAMS: u32 = 2;
const STREAM_ITEMS: u32 = 300;

fn c_string(s: &str) -> CString {
    CString::new(s).expect("no NUL in a test string")
}

fn scratch(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is past 1970")
        .as_nanos();
    let dir = std::env::temp_dir();
    let name = format!("subetha-wl-{tag}-{}-{nanos}", std::process::id());
    dir.join(name).to_str().expect("the temp directory is UTF-8").to_owned()
}

/// A shared-memory name: no directory, short, and distinct from every
/// other name this process asks for. The sequence number carries that
/// last property on its own. A name has to stay short for the platforms
/// with a tight limit on it, so the clock reading in it is truncated to
/// its low 32 bits, and a truncated clock repeats - every 4.295 seconds
/// of real time. A repeated name hands a fresh pass the region an
/// earlier pass left behind, with that pass's peers still claimed in its
/// directory and its rings holding that pass's items.
fn shm_name(tag: &str) -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is past 1970")
        .as_nanos();
    let seq = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("subetha_wl_{tag}_{}_{:x}_{seq:x}", std::process::id(), nanos & 0xffff_ffff)
}

/// The managed-mode sidecar's scan cadence, in microseconds. The library
/// names no default and refuses managed mode without one, so the value a
/// workload passes is a choice this harness makes rather than a library
/// constant. `SUBETHA_FFI_SCAN_US` overrides it, which is what lets a
/// sweep measure the cost and the responsiveness of the cadence itself
/// against a workload that is otherwise unchanged.
fn scan_interval_us() -> u64 {
    match std::env::var("SUBETHA_FFI_SCAN_US") {
        Ok(s) => s.parse().expect("SUBETHA_FFI_SCAN_US is a number of microseconds"),
        Err(VarError::NotPresent) => 1000,
        Err(e) => panic!("SUBETHA_FFI_SCAN_US is set but unreadable: {e}"),
    }
}

fn soak() -> Option<Duration> {
    match std::env::var("SUBETHA_FFI_SOAK_SECS") {
        Ok(s) => Some(Duration::from_secs(s.parse().expect("SUBETHA_FFI_SOAK_SECS is a number of seconds"))),
        Err(VarError::NotPresent) => None,
        Err(e) => panic!("SUBETHA_FFI_SOAK_SECS is set but unreadable: {e}"),
    }
}

fn mode_name(mode: u32) -> &'static str {
    if mode == SUBETHA_MODE_MANAGED {
        "managed"
    } else {
        "strict"
    }
}

fn locale_name(locale: u32) -> &'static str {
    match locale {
        LOCALE_FILE => "file",
        LOCALE_SHM_SESSION => "shm-session",
        _ => "shm-machine",
    }
}

fn report(workload: &str, role: &str, detail: &str, s: &subetha_workload_stats) {
    let mean_us = if s.items == 0 { 0.0 } else { s.total_ns as f64 / s.items as f64 / 1e3 };
    println!(
        "WL {workload} role={role} {detail} items={} bytes={} refusals={} retries={} elapsed_ms={:.1} worst_us={:.1} mean_us={mean_us:.2}",
        s.items,
        s.bytes,
        s.refusals,
        s.retries,
        s.elapsed_ns as f64 / 1e6,
        s.worst_ns as f64 / 1e3,
    );
}

fn env_u32(name: &str) -> u32 {
    std::env::var(name)
        .unwrap_or_else(|e| panic!("{name} is required for this role: {e}"))
        .parse()
        .unwrap_or_else(|e| panic!("{name} is a number: {e}"))
}

fn env_string(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|e| panic!("{name} is required for this role: {e}"))
}

/// The child side: one role, chosen by `SUBETHA_FFI_WORKLOAD`, exiting
/// with the number of problems the C side found.
#[test]
fn workload_peer() {
    let role = match std::env::var("SUBETHA_FFI_WORKLOAD") {
        Ok(r) => r,
        Err(VarError::NotPresent) => return,
        Err(e) => panic!("SUBETHA_FFI_WORKLOAD is set but unreadable: {e}"),
    };
    let _serial = begin();
    let mode = env_u32("SUBETHA_FFI_WL_MODE");
    let mut stats = subetha_workload_stats::default();
    let problems = match role.as_str() {
        "rr_server" => {
            let locale = env_u32("SUBETHA_FFI_WL_LOCALE");
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let sddl = sddl_for(locale);
            let rc = unsafe {
                subetha_workload_rr_serve(
                    locale,
                    base.as_ptr(),
                    sddl.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
                    mode,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("rr", "server", &format!("locale={} mode={}", locale_name(locale), mode_name(mode)), &stats);
            rc
        }
        "rr_client" => {
            let locale = env_u32("SUBETHA_FFI_WL_LOCALE");
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let sddl = sddl_for(locale);
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_rr_client(
                    locale,
                    base.as_ptr(),
                    sddl.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
                    mode,
                    index,
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("rr", &format!("client{index}"), &format!("locale={} mode={}", locale_name(locale), mode_name(mode)), &stats);
            rc
        }
        "fleet_host" => {
            let locale = env_u32("SUBETHA_FFI_WL_LOCALE");
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let rc = unsafe {
                subetha_workload_fleet_host(
                    locale,
                    base.as_ptr(),
                    mode,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("fleet", "host", &format!("locale={} mode={}", locale_name(locale), mode_name(mode)), &stats);
            rc
        }
        "fleet_worker" => {
            let locale = env_u32("SUBETHA_FFI_WL_LOCALE");
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe { subetha_workload_fleet_worker(locale, base.as_ptr(), mode, index, &mut stats) };
            report("fleet", &format!("worker{index}"), &format!("locale={} mode={}", locale_name(locale), mode_name(mode)), &stats);
            rc
        }
        "bus_shell" => {
            let prefix = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let rc = unsafe {
                subetha_workload_bus_shell(
                    prefix.as_ptr(),
                    mode,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("bus", "shell", &format!("mode={}", mode_name(mode)), &stats);
            rc
        }
        "bus_client" => {
            let prefix = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe { subetha_workload_bus_client(prefix.as_ptr(), mode, index, env_u32("SUBETHA_FFI_WL_COUNT"), &mut stats) };
            report("bus", &format!("client{index}"), &format!("mode={}", mode_name(mode)), &stats);
            rc
        }
        "menu_shell" => {
            let path = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let rc = unsafe { subetha_workload_menu_shell(path.as_ptr(), mode, env_u32("SUBETHA_FFI_WL_COUNT"), &mut stats) };
            report("menu", "shell", &format!("mode={}", mode_name(mode)), &stats);
            rc
        }
        "menu_broker" => {
            let path = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let rc = unsafe { subetha_workload_menu_broker(path.as_ptr(), mode, env_u32("SUBETHA_FFI_WL_COUNT"), &mut stats) };
            report("menu", "broker", &format!("mode={}", mode_name(mode)), &stats);
            rc
        }
        "blob_writer" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_blob_writer(
                    base.as_ptr(),
                    mode,
                    index,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("blob", &format!("writer{index}"), &format!("mode={} already_stored={} lost_races={}", mode_name(mode), stats.refusals, stats.retries), &stats);
            rc
        }
        "blob_reader" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_blob_reader(
                    base.as_ptr(),
                    mode,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("blob", &format!("reader{index}"), &format!("mode={} walks={}", mode_name(mode), stats.retries), &stats);
            rc
        }
        "index_writer" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_index_writer(
                    base.as_ptr(),
                    mode,
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    env_u32("SUBETHA_FFI_WL_ROUNDS"),
                    &mut stats,
                )
            };
            report("index", &format!("writer{index}"), &format!("mode={} lease_waits={} abandoned={}", mode_name(mode), stats.retries, stats.refusals), &stats);
            rc
        }
        "index_reader" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe { subetha_workload_index_reader(base.as_ptr(), mode, env_u32("SUBETHA_FFI_WL_COUNT"), &mut stats) };
            report("index", &format!("reader{index}"), &format!("mode={} generations={} idle_polls={}", mode_name(mode), stats.total_ns, stats.retries), &stats);
            rc
        }
        "mvcc_writer" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_mvcc_writer(
                    base.as_ptr(),
                    mode,
                    index,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    env_u32("SUBETHA_FFI_WL_ROUNDS"),
                    &mut stats,
                )
            };
            report("mvcc", &format!("writer{index}"), &format!("mode={} removes={} refused_then_swept={}", mode_name(mode), stats.refusals, stats.retries), &stats);
            rc
        }
        "mvcc_reader" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_mvcc_reader(
                    base.as_ptr(),
                    mode,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    env_u32("SUBETHA_FFI_WL_ROUNDS"),
                    &mut stats,
                )
            };
            report("mvcc", &format!("reader{index}"), &format!("mode={} pages={}", mode_name(mode), stats.retries), &stats);
            rc
        }
        "graph_writer" => {
            let path = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_graph_writer(
                    path.as_ptr(),
                    mode,
                    index,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_NODES"),
                    env_u32("SUBETHA_FFI_WL_BLOCKS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("graph", &format!("writer{index}"), &format!("mode={} overflow_pages={} pruned_edges={}", mode_name(mode), stats.retries, stats.total_ns), &stats);
            // The pruned count rides in total_ns; the parent reads it
            // from the exit line rather than the mean.
            println!("GRAPH-WRITER-EDGES added={} pruned={}", stats.items, stats.total_ns);
            rc
        }
        "graph_reader" => {
            let path = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_graph_reader(
                    path.as_ptr(),
                    mode,
                    env_u32("SUBETHA_FFI_WL_NODES"),
                    env_u32("SUBETHA_FFI_WL_BLOCKS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("graph", &format!("reader{index}"), &format!("mode={} edges_seen={} rereads={}", mode_name(mode), stats.bytes, stats.retries), &stats);
            rc
        }
        "memory_writer" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_memory_writer(
                    base.as_ptr(),
                    mode,
                    index,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("memory", &format!("writer{index}"), &format!("mode={} lock_timeouts={}", mode_name(mode), stats.retries), &stats);
            rc
        }
        "memory_reader" => {
            let base = c_string(&env_string("SUBETHA_FFI_WL_BASE"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_memory_reader(
                    base.as_ptr(),
                    mode,
                    index,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    env_u32("SUBETHA_FFI_WL_ROUNDS"),
                    &mut stats,
                )
            };
            report("memory", &format!("reader{index}"), &format!("mode={} not_yet_stored={} lock_timeouts={}", mode_name(mode), stats.refusals, stats.retries), &stats);
            rc
        }
        "stream_receiver" => {
            let cert = c_string(&env_string("SUBETHA_FFI_WL_CERT"));
            let key = c_string(&env_string("SUBETHA_FFI_WL_KEY"));
            let rc = unsafe {
                subetha_workload_stream_receiver(
                    cert.as_ptr(),
                    key.as_ptr(),
                    mode,
                    env_u32("SUBETHA_FFI_WL_CLIENTS"),
                    env_u32("SUBETHA_FFI_WL_STREAMS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("stream", "receiver", &format!("mode={} gaps={} empty_polls={} streams_ended={}", mode_name(mode), stats.refusals, stats.retries, stats.total_ns), &stats);
            println!("STREAM-RECEIVER-ITEMS delivered={} gaps={} ended={}", stats.items, stats.refusals, stats.total_ns);
            rc
        }
        "stream_sender" => {
            let cert = c_string(&env_string("SUBETHA_FFI_WL_CERT"));
            let index = env_u32("SUBETHA_FFI_WL_INDEX");
            let rc = unsafe {
                subetha_workload_stream_sender(
                    cert.as_ptr(),
                    env_u32("SUBETHA_FFI_WL_PORT"),
                    mode,
                    index,
                    env_u32("SUBETHA_FFI_WL_STREAMS"),
                    env_u32("SUBETHA_FFI_WL_COUNT"),
                    &mut stats,
                )
            };
            report("stream", &format!("sender{index}"), &format!("mode={} missed_by_sender={}", mode_name(mode), stats.total_ns), &stats);
            rc
        }
        other => panic!("SUBETHA_FFI_WORKLOAD {other} names no role"),
    };
    let rc = subetha_shutdown();
    assert_eq!(rc, SUBETHA_OK, "the {role} role left a handle open");
    std::process::exit(problems);
}

fn sddl_for(locale: u32) -> Option<CString> {
    if locale == LOCALE_SHM_MACHINE {
        Some(c_string(MACHINE_SDDL))
    } else {
        None
    }
}

struct Peer {
    child: Child,
    stdout: BufReader<ChildStdout>,
    role: String,
}

fn spawn_role(role: &str, env: &[(&str, String)]) -> Peer {
    let mut cmd = Command::new(std::env::current_exe().expect("the test binary's own path"));
    cmd.arg("workload_peer").arg("--exact").arg("--nocapture").env("SUBETHA_FFI_WORKLOAD", role).stdout(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("the role spawns");
    let stdout = BufReader::new(child.stdout.take().expect("the child's stdout is piped"));
    Peer { child, stdout, role: role.to_owned() }
}

/// Read the child's stdout until a line starting with `marker` or with
/// `failed_marker`; every line is relayed. Returns what followed the
/// marker on its line, or the failure code when the failed marker came
/// first.
fn wait_for_marker(peer: &mut Peer, marker: &str, failed_marker: &str) -> Result<String, i32> {
    let mut line = String::new();
    loop {
        line.clear();
        let n = peer.stdout.read_line(&mut line).expect("the child's stdout is readable");
        if n == 0 {
            panic!("the {} role ended before printing {marker}", peer.role);
        }
        let text = line.trim_end();
        println!("[{}] {text}", peer.role);
        if let Some(rest) = text.strip_prefix(marker) {
            return Ok(rest.trim().to_owned());
        }
        if let Some(rest) = text.strip_prefix(failed_marker) {
            return Err(rest.trim().parse().expect("the failure marker carries a code"));
        }
    }
}

/// Relay the rest of a child's stdout and wait for it; a non-zero exit
/// is the number of problems the C side found. Lines starting with
/// `keep` are collected for the caller, which is how a role hands a
/// count back past its exit code.
fn finish_keeping(mut peer: Peer, keep: Option<&str>) -> (i32, Vec<String>) {
    let mut line = String::new();
    let mut kept = Vec::new();
    loop {
        line.clear();
        let n = peer.stdout.read_line(&mut line).expect("the child's stdout is readable");
        if n == 0 {
            break;
        }
        let text = line.trim_end();
        println!("[{}] {text}", peer.role);
        if let Some(rest) = keep.and_then(|prefix| text.strip_prefix(prefix)) {
            kept.push(rest.trim().to_owned());
        }
    }
    let status = peer.child.wait().expect("the child is waited on");
    match status.code() {
        Some(code) => (code, kept),
        None => panic!("the {} role was killed by a signal: {status}", peer.role),
    }
}

fn finish(peer: Peer) -> i32 {
    finish_keeping(peer, None).0
}

/// `count` processes in `role`, each told its index.
fn spawn_indexed(role: &str, count: u32, common: &[(&str, String)]) -> Vec<Peer> {
    (0..count)
        .map(|i| {
            let mut env = common.to_vec();
            env.push(("SUBETHA_FFI_WL_INDEX", i.to_string()));
            spawn_role(role, &env)
        })
        .collect()
}

/// Every role exits clean.
fn finish_all(label: &str, peers: Vec<Peer>) {
    for peer in peers {
        let role = peer.role.clone();
        assert_eq!(finish(peer), 0, "{label}: the {role} role reported problems");
    }
}

/// `name=value` fields of a kept line, by name.
fn field(line: &str, name: &str) -> u64 {
    line.split_whitespace()
        .find_map(|f| f.strip_prefix(name).and_then(|rest| rest.strip_prefix('=')))
        .unwrap_or_else(|| panic!("the line {line:?} carries no {name}"))
        .parse()
        .unwrap_or_else(|e| panic!("{name} in {line:?} is a number: {e}"))
}

/// Remove a backing at `path` and insist it went.
fn unlinked(label: &str, what: &str, rc: i32, report: &subetha_unlink_report) {
    assert_eq!(rc, SUBETHA_OK, "{label}: unlink the {what}");
    assert_eq!(report.failed, 0, "{label}: the {what} files are removed");
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn run_passes(name: &str, mut one_pass: impl FnMut(u32)) {
    match soak() {
        None => one_pass(0),
        Some(duration) => {
            let start = Instant::now();
            let mut pass = 0;
            while start.elapsed() < duration {
                one_pass(pass);
                pass += 1;
            }
            println!("WL {name} soak passes={pass} seconds={:.0}", start.elapsed().as_secs_f64());
        }
    }
}

/// A request/response service in every locale and mode: the server in
/// one process, the clients in one each. In the machine namespace a
/// create the OS refuses for want of the privilege is reported as a skip
/// and nothing else is.
#[test]
fn request_response_service() {
    let _serial = begin();
    let mut skips = Vec::new();
    for locale in [LOCALE_FILE, LOCALE_SHM_SESSION, LOCALE_SHM_MACHINE] {
        for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
            let label = format!("rr locale={} mode={}", locale_name(locale), mode_name(mode));
            run_passes(&label, |pass| {
                let base = if locale == LOCALE_FILE { scratch("rr") } else { shm_name("rr") };
                let common = vec![
                    ("SUBETHA_FFI_WL_LOCALE", locale.to_string()),
                    ("SUBETHA_FFI_WL_BASE", base.clone()),
                    ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                    ("SUBETHA_FFI_WL_COUNT", RR_REQUESTS_PER_CLIENT.to_string()),
                ];
                let mut server_env = common.clone();
                server_env.push(("SUBETHA_FFI_WL_CLIENTS", RR_CLIENTS.to_string()));
                let mut server = spawn_role("rr_server", &server_env);
                match wait_for_marker(&mut server, "RR-SERVER-READY", "RR-SERVER-FAILED") {
                    Ok(_) => {}
                    Err(code) if locale == LOCALE_SHM_MACHINE && code == SUBETHA_E_RING_IO && pass == 0 => {
                        finish(server);
                        println!("SKIP {label}: the OS refused the machine-namespace create (code {code}); this host lacks the privilege to create global regions");
                        skips.push(label.clone());
                        return;
                    }
                    Err(code) => panic!("{label}: the server failed to create its rings with code {code}"),
                }
                let clients: Vec<Peer> = (0..RR_CLIENTS)
                    .map(|i| {
                        let mut env = common.clone();
                        env.push(("SUBETHA_FFI_WL_INDEX", i.to_string()));
                        spawn_role("rr_client", &env)
                    })
                    .collect();
                for client in clients {
                    let role = client.role.clone();
                    assert_eq!(finish(client), 0, "{label}: the {role} role reported problems");
                }
                assert_eq!(finish(server), 0, "{label}: the server reported problems");
            });
        }
    }
    if !skips.is_empty() {
        println!("SKIPPED {} of 6 request/response runs: {}", skips.len(), skips.join(", "));
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A host and its workers on two rings each, every locale and mode.
#[test]
fn snapshot_fleet() {
    let _serial = begin();
    let mut skips = Vec::new();
    for locale in [LOCALE_FILE, LOCALE_SHM_SESSION, LOCALE_SHM_MACHINE] {
        for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
            let label = format!("fleet locale={} mode={}", locale_name(locale), mode_name(mode));
            run_passes(&label, |pass| {
                let base = if locale == LOCALE_FILE { scratch("fleet") } else { shm_name("fleet") };
                let common = vec![
                    ("SUBETHA_FFI_WL_LOCALE", locale.to_string()),
                    ("SUBETHA_FFI_WL_BASE", base.clone()),
                    ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                ];
                let mut host_env = common.clone();
                host_env.push(("SUBETHA_FFI_WL_CLIENTS", FLEET_WORKERS.to_string()));
                host_env.push(("SUBETHA_FFI_WL_COUNT", FLEET_ROUNDS.to_string()));
                let mut host = spawn_role("fleet_host", &host_env);
                match wait_for_marker(&mut host, "FLEET-HOST-READY", "FLEET-HOST-FAILED") {
                    Ok(_) => {}
                    Err(code) if locale == LOCALE_SHM_MACHINE && code == SUBETHA_E_RING_IO && pass == 0 => {
                        finish(host);
                        println!("SKIP {label}: the OS refused the machine-namespace create (code {code}); this host lacks the privilege to create global regions");
                        skips.push(label.clone());
                        return;
                    }
                    Err(code) => panic!("{label}: the host failed to create its rings with code {code}"),
                }
                let workers: Vec<Peer> = (0..FLEET_WORKERS)
                    .map(|i| {
                        let mut env = common.clone();
                        env.push(("SUBETHA_FFI_WL_INDEX", i.to_string()));
                        spawn_role("fleet_worker", &env)
                    })
                    .collect();
                wait_for_marker(&mut host, "FLEET-HOST-DONE", "FLEET-HOST-FAILED").expect("the host runs to its done marker");
                for worker in workers {
                    let role = worker.role.clone();
                    assert_eq!(finish(worker), 0, "{label}: the {role} role reported problems");
                }
                assert_eq!(finish(host), 0, "{label}: the host reported problems");
                let c_base = c_string(&base);
                assert_eq!(unsafe { subetha_workload_fleet_unlink(locale, c_base.as_ptr(), FLEET_WORKERS) }, 0, "{label}: unlink");
            });
        }
    }
    if !skips.is_empty() {
        println!("SKIPPED {} of 6 fleet runs: {}", skips.len(), skips.join(", "));
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// T explorer threads on one anonymous ring, every one producer and
/// consumer, in both modes.
#[test]
fn race_bus() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("race mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let options = subetha_ring_options {
                mode,
                scan_interval_us: if mode == SUBETHA_MODE_MANAGED { scan_interval_us() } else { 0 },
                ..Default::default()
            };
            let mut ring: subetha_handle = 0;
            assert_eq!(unsafe { subetha_ring_create_anon(RACE_THREADS, RACE_THREADS, 512, &options, &mut ring) }, SUBETHA_OK);
            let mut totals = Vec::new();
            // The producer slot each explorer published from, so a loss
            // can be traced to the ring its migrants went into.
            let mut slots = Vec::new();
            // One byte per (explorer, round). A migrant is adopted by
            // exactly one consumer, so each byte has a single writer and
            // the threads share it without atomics; the address is
            // carried as an integer because a raw pointer is not Send.
            let mut seen = vec![0u8; RACE_THREADS as usize * RACE_ROUNDS as usize];
            let seen_len = seen.len();
            let seen_at = seen.as_mut_ptr() as usize;
            std::thread::scope(|scope| {
                let handles: Vec<_> = (0..RACE_THREADS)
                    .map(|index| {
                        scope.spawn(move || {
                            let mut stats = subetha_workload_stats::default();
                            let mut slot = u32::MAX;
                            let rc = unsafe {
                                subetha_workload_race_explorer(
                                    ring,
                                    index,
                                    RACE_ROUNDS,
                                    RACE_STEPS,
                                    seen_at as *mut u8,
                                    seen_len,
                                    &mut slot,
                                    &mut stats,
                                )
                            };
                            (rc, slot, stats)
                        })
                    })
                    .collect();
                for (i, h) in handles.into_iter().enumerate() {
                    let (rc, slot, stats) = h.join().expect("an explorer thread finishes");
                    report("race", &format!("explorer{i}"), &format!("mode={}", mode_name(mode)), &stats);
                    assert_eq!(rc, 0, "{label}: explorer {i} reported problems");
                    totals.push(stats);
                    slots.push(slot as usize);
                }
            });
            let published: u64 = totals.iter().map(|s| s.bytes / (10 + 3 * RACE_STEPS as u64)).sum();
            let adopted: u64 = totals.iter().map(|s| s.items).sum();
            // Migrants published after another explorer's last drain are
            // still in the ring when the race ends; a last consumer takes
            // them so the accounting closes.
            let mut cid = 0u32;
            assert_eq!(unsafe { subetha_ring_register_consumer(ring, &mut cid) }, SUBETHA_OK);
            let mut leftovers = 0u64;
            let mut frame = vec![0u8; SUBETHA_RING_FRAME_DEFAULT_BLOCK];
            let mut len = 0usize;
            loop {
                let rc = unsafe { subetha_ring_recv_frame(ring, cid, frame.as_mut_ptr(), frame.len(), &mut len) };
                if rc == SUBETHA_E_RING_EMPTY {
                    break;
                }
                assert_eq!(rc, SUBETHA_OK, "{label}: draining the leftovers");
                // A leftover is accounted for as surely as an adopted
                // migrant; both mark the same map, so what stays clear
                // at the end is what nobody could reach.
                let round = u64::from_le_bytes(frame[..8].try_into().expect("eight bytes of round"));
                let slot = frame[12] as usize * RACE_ROUNDS as usize + round as usize;
                if slot < seen.len() {
                    seen[slot] = 1;
                }
                leftovers += 1;
            }
            println!("WL race leftovers={leftovers} mode={}", mode_name(mode));
            if adopted + leftovers != published {
                let lost: Vec<String> = seen
                    .iter()
                    .enumerate()
                    .filter(|(_, taken)| **taken == 0)
                    .map(|(slot, _)| {
                        format!(
                            "explorer {} round {}",
                            slot / RACE_ROUNDS as usize,
                            slot % RACE_ROUNDS as usize
                        )
                    })
                    .collect();
                // A lost migrant is one no consumer could reach, and the
                // ring its explorer published into is where the answer
                // is. The trace is a debug-build record, so a release run
                // prints the tally alone.
                let explorer = seen
                    .iter()
                    .position(|taken| *taken == 0)
                    .map_or(0, |slot| slot / RACE_ROUNDS as usize);
                let ring = slots[explorer];
                // Everything the trace still holds for that ring. A cut
                // here reads exactly like a ring nothing else touched,
                // and the events that explain a loss are the ones a
                // recent-only window drops first.
                let history = subetha_ffi::subetha_test_ring_trace(ring, usize::MAX).join("\n  ");
                panic!(
                    "{label}: every published migrant is adopted by someone or left in the \
                     ring; {published} published, {adopted} adopted, {leftovers} left over, \
                     unaccounted for: {lost:?}\n  \
                     explorer {explorer} published from producer slot {ring}; what happened \
                     to that ring before this, oldest first:\n  {history}"
                );
            }
            assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A chunked line log: producers serialized by a lock, a writer draining
/// to empty, in both modes.
#[test]
fn line_log() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("log mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let mut ring: subetha_handle = 0;
            assert_eq!(unsafe { subetha_spsc_create_anon(1024, mode, &mut ring) }, SUBETHA_OK);
            let lock = Mutex::new(());
            let mut pushed = 0u64;
            let mut dropped = 0u64;
            let mut written = subetha_workload_stats::default();
            std::thread::scope(|scope| {
                let writer = scope.spawn(|| {
                    let mut stats = subetha_workload_stats::default();
                    let rc = unsafe { subetha_workload_log_writer(ring, &mut stats) };
                    (rc, stats)
                });
                let producers: Vec<_> = (0..LOG_PRODUCERS)
                    .map(|_| {
                        let lock = &lock;
                        scope.spawn(move || {
                            let mut whole = 0u64;
                            let mut lost = 0u64;
                            let mut elapsed = 0u64;
                            let mut done = 0;
                            while done < LOG_LINES {
                                let batch = (LOG_LINES - done).min(50);
                                let mut stats = subetha_workload_stats::default();
                                let rc = {
                                    let _held = lock.lock().expect("the producer lock is not poisoned");
                                    unsafe { subetha_workload_log_producer(ring, done, batch, LOG_LINE_BYTES, LOG_PACE_US, &mut stats) }
                                };
                                assert_eq!(rc, 0, "a log producer reported problems");
                                whole += stats.items;
                                lost += stats.refusals + stats.retries;
                                elapsed += stats.elapsed_ns;
                                done += batch;
                            }
                            subetha_workload_stats { items: whole, refusals: lost, elapsed_ns: elapsed, ..Default::default() }
                        })
                    })
                    .collect();
                for (i, p) in producers.into_iter().enumerate() {
                    let stats = p.join().expect("a producer thread finishes");
                    report("log", &format!("producer{i}"), &format!("mode={}", mode_name(mode)), &stats);
                    pushed += stats.items;
                    dropped += stats.refusals;
                }
                // The end is a slot the harness pushes once the producers
                // have returned, never a stretch of silence: a producer
                // starved of its core is silent too, and a writer that
                // left on silence left a ring of whole lines behind.
                assert_eq!(unsafe { subetha_workload_log_end(ring) }, 0, "{label}: the end slot is pushed");
                let (rc, stats) = writer.join().expect("the writer thread finishes");
                report("log", "writer", &format!("mode={} cut_short={} idle_passes={}", mode_name(mode), stats.refusals, stats.total_ns), &stats);
                assert_eq!(rc, 0, "{label}: the writer reported problems");
                written = stats;
            });
            assert_eq!(written.items, pushed, "{label}: every whole line reaches the writer; {dropped} were dropped by producers");
            assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A command ring and a reply ring per client, file-backed, both modes.
#[test]
fn command_bus() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("bus mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let prefix = scratch("bus");
            let common = vec![("SUBETHA_FFI_WL_BASE", prefix.clone()), ("SUBETHA_FFI_WL_MODE", mode.to_string()), ("SUBETHA_FFI_WL_COUNT", BUS_QUERIES.to_string())];
            let mut shell_env = common.clone();
            shell_env.push(("SUBETHA_FFI_WL_CLIENTS", BUS_CLIENTS.to_string()));
            let mut shell = spawn_role("bus_shell", &shell_env);
            wait_for_marker(&mut shell, "BUS-SHELL-READY", "BUS-SHELL-FAILED").expect("the shell creates its ring");
            let clients: Vec<Peer> = (0..BUS_CLIENTS)
                .map(|i| {
                    let mut env = common.clone();
                    env.push(("SUBETHA_FFI_WL_INDEX", i.to_string()));
                    spawn_role("bus_client", &env)
                })
                .collect();
            for client in clients {
                let role = client.role.clone();
                assert_eq!(finish(client), 0, "{label}: the {role} role reported problems");
            }
            assert_eq!(finish(shell), 0, "{label}: the shell reported problems");
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// One direction over a file-backed single-producer ring with a
/// begin/chunk/show protocol, the broker opening with retries.
#[test]
fn context_menu() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("menu mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let path = scratch("menu");
            let env = vec![("SUBETHA_FFI_WL_BASE", path.clone()), ("SUBETHA_FFI_WL_MODE", mode.to_string()), ("SUBETHA_FFI_WL_COUNT", MENUS.to_string())];
            // The broker starts first and retries its open, as the real one does.
            let broker = spawn_role("menu_broker", &env);
            let mut shell = spawn_role("menu_shell", &env);
            wait_for_marker(&mut shell, "MENU-SHELL-READY", "MENU-SHELL-FAILED").expect("the shell creates its ring");
            assert_eq!(finish(broker), 0, "{label}: the broker reported problems");
            assert_eq!(finish(shell), 0, "{label}: the shell reported problems");
            let c_path = c_string(&path);
            assert_eq!(unsafe { subetha_workload_menu_unlink(c_path.as_ptr()) }, 0, "{label}: unlink");
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// Three anonymous single-producer rings with lock-serialized producers
/// and a consumer in each of the three waiting styles.
#[test]
fn pod_rings() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("pods mode={}", mode_name(mode));
        run_passes(&label, |_| {
            for style in 0..3u32 {
                let mut ring: subetha_handle = 0;
                assert_eq!(unsafe { subetha_spsc_create_anon(256, mode, &mut ring) }, SUBETHA_OK);
                let lock = Mutex::new(());
                let mut pushed = 0u64;
                std::thread::scope(|scope| {
                    let consumer = scope.spawn(move || {
                        let mut stats = subetha_workload_stats::default();
                        let rc = unsafe { subetha_workload_pods_consumer(ring, style, PODS_PRODUCERS, &mut stats) };
                        (rc, stats)
                    });
                    let producers: Vec<_> = (0..PODS_PRODUCERS)
                        .map(|p| {
                            let lock = &lock;
                            scope.spawn(move || {
                                let mut total = subetha_workload_stats::default();
                                let mut first = 0;
                                while first < PODS_ITEMS {
                                    let batch = (PODS_ITEMS - first).min(PODS_BATCH);
                                    let last = first + batch >= PODS_ITEMS;
                                    let mut stats = subetha_workload_stats::default();
                                    let rc = {
                                        let _held = lock.lock().expect("the producer lock is not poisoned");
                                        unsafe { subetha_workload_pods_producer(ring, p, first, batch, u32::from(last), &mut stats) }
                                    };
                                    assert_eq!(rc, 0, "a pod producer reported problems");
                                    total.items += stats.items;
                                    total.bytes += stats.bytes;
                                    total.refusals += stats.refusals;
                                    total.elapsed_ns += stats.elapsed_ns;
                                    first += batch;
                                }
                                total
                            })
                        })
                        .collect();
                    for (i, p) in producers.into_iter().enumerate() {
                        let stats = p.join().expect("a producer thread finishes");
                        report("pods", &format!("producer{i}"), &format!("mode={} style={style}", mode_name(mode)), &stats);
                        pushed += stats.items;
                    }
                    let (rc, stats) = consumer.join().expect("the consumer thread finishes");
                    report("pods", "consumer", &format!("mode={} style={style}", mode_name(mode)), &stats);
                    assert_eq!(rc, 0, "{label}: the consumer reported problems");
                    assert_eq!(stats.items, pushed, "{label} style {style}: every pushed message is consumed");
                });
                assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
            }
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// Two anonymous rings between a foreground and a background thread.
#[test]
fn two_ring_mind() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("mind mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let options = subetha_ring_options {
                mode,
                scan_interval_us: if mode == SUBETHA_MODE_MANAGED { scan_interval_us() } else { 0 },
                ..Default::default()
            };
            let mut raw: subetha_handle = 0;
            let mut promo: subetha_handle = 0;
            assert_eq!(unsafe { subetha_ring_create_anon(1, 1, 4096, &options, &mut raw) }, SUBETHA_OK);
            assert_eq!(unsafe { subetha_ring_create_anon(1, 1, 4096, &options, &mut promo) }, SUBETHA_OK);
            std::thread::scope(|scope| {
                let background = scope.spawn(move || {
                    let mut stats = subetha_workload_stats::default();
                    let rc = unsafe { subetha_workload_mind_subconscious(raw, promo, &mut stats) };
                    (rc, stats)
                });
                let mut stats = subetha_workload_stats::default();
                let rc = unsafe { subetha_workload_mind_conscious(raw, promo, MIND_TURNS, &mut stats) };
                report("mind", "conscious", &format!("mode={} promotions={}", mode_name(mode), stats.total_ns), &stats);
                assert_eq!(rc, 0, "{label}: the foreground reported problems");
                let (brc, bstats) = background.join().expect("the background thread finishes");
                report("mind", "subconscious", &format!("mode={} verdicts={}", mode_name(mode), bstats.total_ns), &bstats);
                assert_eq!(brc, 0, "{label}: the background reported problems");
                assert_eq!(bstats.items, stats.items, "{label}: every snapshot reaches the background");
                assert_eq!(bstats.total_ns, stats.total_ns, "{label}: every promotion gets a verdict");
            });
            assert_eq!(subetha_handle_destroy(promo), SUBETHA_OK);
            assert_eq!(subetha_handle_destroy(raw), SUBETHA_OK);
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// One file-backed deque per producer, every consumer stealing from all,
/// both sides spinning.
#[test]
fn dispatch_deques() {
    let _serial = begin();
    let label = "deque".to_owned();
    run_passes(&label, |_| {
        let paths: Vec<String> = (0..DEQUE_PRODUCERS).map(|i| scratch(&format!("deque{i}"))).collect();
        let c_paths: Vec<CString> = paths.iter().map(|p| c_string(p)).collect();
        let owners: Vec<subetha_handle> = c_paths
            .iter()
            .map(|p| {
                let mut h: subetha_handle = 0;
                assert_eq!(unsafe { subetha_workload_deque_create(p.as_ptr(), SUBETHA_MODE_STRICT, &mut h) }, 0);
                h
            })
            .collect();
        let thieves: Vec<Vec<subetha_handle>> = (0..DEQUE_CONSUMERS)
            .map(|_| {
                c_paths
                    .iter()
                    .map(|p| {
                        let mut h: subetha_handle = 0;
                        assert_eq!(unsafe { subetha_workload_deque_open(p.as_ptr(), SUBETHA_MODE_STRICT, &mut h) }, 0);
                        h
                    })
                    .collect()
            })
            .collect();
        let mut stolen = 0u64;
        std::thread::scope(|scope| {
            let consumers: Vec<_> = thieves
                .iter()
                .map(|set| {
                    let set = set.clone();
                    scope.spawn(move || {
                        let mut stats = subetha_workload_stats::default();
                        let rc = unsafe { subetha_workload_deque_consume(set.as_ptr(), set.len() as u32, DEQUE_ITEMS, &mut stats) };
                        (rc, stats)
                    })
                })
                .collect();
            let producers: Vec<_> = owners
                .iter()
                .enumerate()
                .map(|(i, &deque)| {
                    scope.spawn(move || {
                        let mut stats = subetha_workload_stats::default();
                        let rc = unsafe { subetha_workload_deque_produce(deque, i as u32, DEQUE_ITEMS, &mut stats) };
                        (rc, stats)
                    })
                })
                .collect();
            for (i, p) in producers.into_iter().enumerate() {
                let (rc, stats) = p.join().expect("a producer thread finishes");
                report("deque", &format!("producer{i}"), "spin", &stats);
                assert_eq!(rc, 0, "{label}: producer {i} reported problems");
            }
            for (i, c) in consumers.into_iter().enumerate() {
                let (rc, stats) = c.join().expect("a consumer thread finishes");
                report("deque", &format!("consumer{i}"), "spin", &stats);
                assert_eq!(rc, 0, "{label}: consumer {i} reported problems");
                stolen += stats.items;
            }
        });
        assert_eq!(stolen, u64::from(DEQUE_PRODUCERS) * u64::from(DEQUE_ITEMS), "{label}: every pushed value is stolen once");
        for set in thieves {
            for h in set {
                assert_eq!(subetha_handle_destroy(h), SUBETHA_OK);
            }
        }
        for h in owners {
            assert_eq!(subetha_handle_destroy(h), SUBETHA_OK);
        }
        for path in &paths {
            let c_path = c_string(path);
            let mut report_out = subetha_ffi::subetha_unlink_report::default();
            assert_eq!(unsafe { subetha_ffi::subetha_deque_unlink(c_path.as_ptr(), &mut report_out) }, SUBETHA_OK);
            assert_eq!(report_out.failed, 0, "{label}: the deque files are removed");
        }
    });
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A content-addressed blob store: writers storing overlapping blob sets
/// in a process each, readers on a read-only arena in a process each,
/// both modes.
#[test]
fn blob_store() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("blob mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let base = scratch("blob");
            let c_base = c_string(&base);
            assert_eq!(unsafe { subetha_workload_blob_create(c_base.as_ptr(), mode, BLOB_WRITERS, BLOBS) }, 0, "{label}: create");
            let common = vec![
                ("SUBETHA_FFI_WL_BASE", base.clone()),
                ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                ("SUBETHA_FFI_WL_CLIENTS", BLOB_WRITERS.to_string()),
                ("SUBETHA_FFI_WL_COUNT", BLOBS.to_string()),
            ];
            // Readers first: they wait for the set to fill.
            let readers = spawn_indexed("blob_reader", BLOB_READERS, &common);
            let writers = spawn_indexed("blob_writer", BLOB_WRITERS, &common);
            finish_all(&label, writers);
            finish_all(&label, readers);
            let mut report = subetha_unlink_report::default();
            let map = c_string(&format!("{base}_map"));
            unlinked(&label, "map", unsafe { subetha_hashmap_unlink(map.as_ptr(), &mut report) }, &report);
            let arena = c_string(&format!("{base}_arena"));
            unlinked(&label, "arena", unsafe { subetha_arena_unlink(arena.as_ptr(), &mut report) }, &report);
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A content index rebuilt generation by generation under an owner lease
/// by competing indexer processes, read read-only by reader processes as
/// each generation is published, both modes.
#[test]
fn content_index() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("index mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let base = scratch("index");
            let c_base = c_string(&base);
            assert_eq!(unsafe { subetha_workload_index_create(c_base.as_ptr(), mode) }, 0, "{label}: create");
            let common = vec![
                ("SUBETHA_FFI_WL_BASE", base.clone()),
                ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                ("SUBETHA_FFI_WL_COUNT", INDEX_ENTRIES.to_string()),
                ("SUBETHA_FFI_WL_ROUNDS", INDEX_GENERATIONS.to_string()),
            ];
            let readers = spawn_indexed("index_reader", INDEX_READERS, &common);
            let writers = spawn_indexed("index_writer", INDEX_WRITERS, &common);
            finish_all(&label, writers);
            let mut last = 0u32;
            assert_eq!(unsafe { subetha_workload_index_finish(c_base.as_ptr(), mode, &mut last) }, 0, "{label}: finish");
            assert!(
                last >= INDEX_WRITERS * INDEX_GENERATIONS,
                "{label}: {last} lease terms for {} builds",
                INDEX_WRITERS * INDEX_GENERATIONS
            );
            for reader in readers {
                let role = reader.role.clone();
                let (code, kept) = finish_keeping(reader, Some("WL index role=reader"));
                assert_eq!(code, 0, "{label}: the {role} role reported problems");
                let records: u64 = kept.iter().map(|line| field(line, "items")).sum();
                assert!(records >= u64::from(INDEX_ENTRIES), "{label}: {role} verified {records} records");
            }
            let mut report = subetha_unlink_report::default();
            let lease = c_string(&format!("{base}.lease"));
            unlinked(&label, "lease", unsafe { subetha_owner_lease_unlink(lease.as_ptr(), &mut report) }, &report);
            let counter = c_string(&format!("{base}.gen"));
            unlinked(&label, "counter", unsafe { subetha_atomic_unlink(counter.as_ptr(), &mut report) }, &report);
            for generation in 1..=last {
                let vec = c_string(&format!("{base}_g{generation}.vec"));
                unlinked(&label, "generation vec", unsafe { subetha_vec_unlink(vec.as_ptr(), &mut report) }, &report);
                let arena = c_string(&format!("{base}_g{generation}.arena"));
                unlinked(&label, "generation arena", unsafe { subetha_arena_unlink(arena.as_ptr(), &mut report) }, &report);
            }
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A versioned map changed by writer processes, one at a time under the
/// write side of a shared lock, while reader processes scan it under
/// pins from the shared epoch table and no lock, both modes.
#[test]
fn mvcc_index() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("mvcc mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let base = scratch("mvcc");
            let c_base = c_string(&base);
            assert_eq!(unsafe { subetha_workload_mvcc_create(c_base.as_ptr(), mode, MVCC_WRITERS, MVCC_KEYS) }, 0, "{label}: create");
            let common = vec![
                ("SUBETHA_FFI_WL_BASE", base.clone()),
                ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                ("SUBETHA_FFI_WL_CLIENTS", MVCC_WRITERS.to_string()),
                ("SUBETHA_FFI_WL_COUNT", MVCC_KEYS.to_string()),
            ];
            let mut reader_env = common.clone();
            reader_env.push(("SUBETHA_FFI_WL_ROUNDS", MVCC_SCANS.to_string()));
            let mut writer_env = common.clone();
            writer_env.push(("SUBETHA_FFI_WL_ROUNDS", MVCC_ROUNDS.to_string()));
            let readers = spawn_indexed("mvcc_reader", MVCC_READERS, &reader_env);
            let writers = spawn_indexed("mvcc_writer", MVCC_WRITERS, &writer_env);
            finish_all(&label, writers);
            finish_all(&label, readers);
            let mut report = subetha_unlink_report::default();
            let tree = c_string(&format!("{base}_tree"));
            unlinked(&label, "tree", unsafe { subetha_btree_unlink(tree.as_ptr(), &mut report) }, &report);
            let epochs = c_string(&format!("{base}_epochs"));
            unlinked(&label, "epoch table", unsafe { subetha_epochs_unlink(epochs.as_ptr(), &mut report) }, &report);
            let lock = c_string(&format!("{base}_wlock"));
            unlinked(&label, "writers' lock", unsafe { subetha_rwlock_unlink(lock.as_ptr(), &mut report) }, &report);
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A graph store on a frame region: writer processes appending edges to
/// their own nodes and pruning, reader processes walking every chain
/// meanwhile, a final quiet walk counting what the writers say is there.
#[test]
fn graph_store() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("graph mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let path = scratch("graph");
            let c_path = c_string(&path);
            assert_eq!(unsafe { subetha_workload_graph_create(c_path.as_ptr(), mode, GRAPH_NODES, GRAPH_BLOCKS) }, 0, "{label}: create");
            let common = vec![
                ("SUBETHA_FFI_WL_BASE", path.clone()),
                ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                ("SUBETHA_FFI_WL_CLIENTS", GRAPH_WRITERS.to_string()),
                ("SUBETHA_FFI_WL_NODES", GRAPH_NODES.to_string()),
                ("SUBETHA_FFI_WL_BLOCKS", GRAPH_BLOCKS.to_string()),
            ];
            let mut reader_env = common.clone();
            reader_env.push(("SUBETHA_FFI_WL_COUNT", GRAPH_WALKS.to_string()));
            let mut writer_env = common.clone();
            writer_env.push(("SUBETHA_FFI_WL_COUNT", GRAPH_EDGES_PER_NODE.to_string()));
            let readers = spawn_indexed("graph_reader", GRAPH_READERS, &reader_env);
            let writers = spawn_indexed("graph_writer", GRAPH_WRITERS, &writer_env);
            let mut added = 0u64;
            let mut pruned = 0u64;
            for writer in writers {
                let role = writer.role.clone();
                let (code, kept) = finish_keeping(writer, Some("GRAPH-WRITER-EDGES"));
                assert_eq!(code, 0, "{label}: the {role} role reported problems");
                assert_eq!(kept.len(), 1, "{label}: {role} reports its edges once");
                added += field(&kept[0], "added");
                pruned += field(&kept[0], "pruned");
            }
            finish_all(&label, readers);
            assert_eq!(added, u64::from(GRAPH_WRITERS) * u64::from(GRAPH_NODES / GRAPH_WRITERS) * u64::from(GRAPH_EDGES_PER_NODE), "{label}: every edge was added");
            let mut stats = subetha_workload_stats::default();
            let rc = unsafe { subetha_workload_graph_verify(c_path.as_ptr(), mode, GRAPH_NODES, GRAPH_BLOCKS, added - pruned, &mut stats) };
            report("graph", "verify", &format!("mode={} edges={}", mode_name(mode), stats.bytes), &stats);
            assert_eq!(rc, 0, "{label}: the graph holds every edge added and not pruned");
            let mut report_out = subetha_unlink_report::default();
            unlinked(&label, "region", unsafe { subetha_frame_region_unlink(c_path.as_ptr(), &mut report_out) }, &report_out);
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A record store under one reader-writer lock: writer processes adding
/// records to a set and a map, reader processes looking ids up under read
/// holds and expecting the two to agree, both modes.
#[test]
fn memory_store() {
    let _serial = begin();
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("memory mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let base = scratch("memory");
            let c_base = c_string(&base);
            let capacity = u64::from(MEMORY_WRITERS) * u64::from(MEMORY_RECORDS);
            assert_eq!(unsafe { subetha_workload_memory_create(c_base.as_ptr(), mode, capacity) }, 0, "{label}: create");
            let common = vec![
                ("SUBETHA_FFI_WL_BASE", base.clone()),
                ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                ("SUBETHA_FFI_WL_CLIENTS", MEMORY_WRITERS.to_string()),
                ("SUBETHA_FFI_WL_COUNT", MEMORY_RECORDS.to_string()),
                ("SUBETHA_FFI_WL_ROUNDS", MEMORY_LOOKUPS.to_string()),
            ];
            let readers = spawn_indexed("memory_reader", MEMORY_READERS, &common);
            let writers = spawn_indexed("memory_writer", MEMORY_WRITERS, &common);
            finish_all(&label, writers);
            finish_all(&label, readers);
            assert_eq!(unsafe { subetha_workload_memory_verify(c_base.as_ptr(), mode, MEMORY_WRITERS, MEMORY_RECORDS) }, 0, "{label}: verify");
            let mut report = subetha_unlink_report::default();
            let set_vec = c_string(&format!("{base}_ids.uvec.bin"));
            unlinked(&label, "set vec", unsafe { subetha_vec_unlink(set_vec.as_ptr(), &mut report) }, &report);
            let set_map = c_string(&format!("{base}_ids.umap.bin"));
            unlinked(&label, "set map", unsafe { subetha_hashmap_unlink(set_map.as_ptr(), &mut report) }, &report);
            let rows = c_string(&format!("{base}_rows"));
            unlinked(&label, "rows", unsafe { subetha_hashmap_unlink(rows.as_ptr(), &mut report) }, &report);
            let lock = c_string(&format!("{base}_lock"));
            unlinked(&label, "lock", unsafe { subetha_rwlock_unlink(lock.as_ptr(), &mut report) }, &report);
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

/// A cluster stream: sender processes each dialing the sealed receiver
/// over more than one stream, the receiver taking every stream to its
/// end and closing the accounting, both modes. A build without the sealed
/// path skips.
#[test]
fn cluster_stream() {
    let _serial = begin();
    let mut sealed = false;
    assert_eq!(unsafe { subetha_sens_tls_available(&mut sealed) }, SUBETHA_OK);
    if !sealed {
        println!("SKIP stream: this build carries no sealed Sens-O-Matic path");
        assert_eq!(subetha_shutdown(), SUBETHA_OK);
        return;
    }
    let mut cert = vec![0u8; 4096];
    let mut key = vec![0u8; 4096];
    let mut cert_len = 0usize;
    let mut key_len = 0usize;
    assert_eq!(
        unsafe {
            subetha_sens_self_signed_cert(
                std::ptr::null(),
                cert.as_mut_ptr(),
                cert.len(),
                &mut cert_len,
                key.as_mut_ptr(),
                key.len(),
                &mut key_len,
            )
        },
        SUBETHA_OK
    );
    let cert_hex = hex(&cert[..cert_len]);
    let key_hex = hex(&key[..key_len]);
    for mode in [SUBETHA_MODE_STRICT, SUBETHA_MODE_MANAGED] {
        let label = format!("stream mode={}", mode_name(mode));
        run_passes(&label, |_| {
            let common = vec![
                ("SUBETHA_FFI_WL_CERT", cert_hex.clone()),
                ("SUBETHA_FFI_WL_MODE", mode.to_string()),
                ("SUBETHA_FFI_WL_STREAMS", STREAM_STREAMS.to_string()),
                ("SUBETHA_FFI_WL_COUNT", STREAM_ITEMS.to_string()),
            ];
            let mut receiver_env = common.clone();
            receiver_env.push(("SUBETHA_FFI_WL_KEY", key_hex.clone()));
            receiver_env.push(("SUBETHA_FFI_WL_CLIENTS", STREAM_SENDERS.to_string()));
            let mut receiver = spawn_role("stream_receiver", &receiver_env);
            let port = wait_for_marker(&mut receiver, "STREAM-RECEIVER-READY", "STREAM-RECEIVER-FAILED")
                .expect("the receiver stands up");
            let mut sender_env = common.clone();
            sender_env.push(("SUBETHA_FFI_WL_PORT", port));
            let senders = spawn_indexed("stream_sender", STREAM_SENDERS, &sender_env);
            finish_all(&label, senders);
            let (code, kept) = finish_keeping(receiver, Some("STREAM-RECEIVER-ITEMS"));
            assert_eq!(code, 0, "{label}: the receiver reported problems");
            assert_eq!(kept.len(), 1, "{label}: the receiver reports its items once");
            let delivered = field(&kept[0], "delivered");
            let gaps = field(&kept[0], "gaps");
            let sent = u64::from(STREAM_SENDERS) * u64::from(STREAM_STREAMS) * u64::from(STREAM_ITEMS);
            assert_eq!(delivered + gaps, sent, "{label}: every item sent is delivered or counted missing");
            assert!(delivered > 0, "{label}: nothing was delivered");
        });
    }
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}
