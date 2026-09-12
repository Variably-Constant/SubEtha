//! A destroy has to wait out the calls already inside its object and
//! refuse the ones that arrive after, while calls on other objects carry
//! on untouched. The borrow protocol is what makes that true, so this
//! leans on it: many threads calling as fast as they can while another
//! destroys and rebuilds underneath them.
//!
//! A handle that has been destroyed is refused, never dereferenced, so the
//! only outcomes a caller may see are the call's own codes and
//! `SUBETHA_E_INVALID_HANDLE`. Anything else, or a crash, is the failure
//! this test is for.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use subetha_ffi::{
    subetha_atomic_u64_create, subetha_atomic_u64_fetch_add, subetha_atomic_u64_load, subetha_atomic_unlink,
    subetha_handle, subetha_handle_destroy, subetha_init, subetha_ring_create_anon, subetha_ring_options,
    subetha_ring_register_consumer, subetha_ring_register_producer, subetha_ring_try_pop, subetha_ring_try_push,
    subetha_shutdown, subetha_unlink_report, SUBETHA_E_INVALID_HANDLE, SUBETHA_E_RING_EMPTY, SUBETHA_E_RING_FULL,
    SUBETHA_MODE_STRICT, SUBETHA_OK, SUBETHA_RING_SLOT_BYTES,
};

/// The library's init and shutdown are process-wide, so a test that shuts
/// down while another is calling would have that other see
/// `SUBETHA_E_SHUT_DOWN`. One at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    match SERIAL.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// How long each phase hammers the handles.
const RUN: Duration = Duration::from_secs(2);

/// Threads calling while the destroys go on.
const CALLERS: usize = 4;

fn options() -> subetha_ring_options {
    subetha_ring_options { mode: SUBETHA_MODE_STRICT, ..Default::default() }
}

/// Claim one producer and one consumer slot per caller, in order, so
/// caller `i` owns id `i` on every handle it is handed. Slots are claimed
/// lowest-free-first, so the ids come out `0..CALLERS` and a caller needs
/// no handshake to learn its own.
fn register_slots(ring: subetha_handle) {
    for expected in 0..CALLERS as u32 {
        let mut p = u32::MAX;
        let mut c = u32::MAX;
        assert_eq!(unsafe { subetha_ring_register_producer(ring, &mut p) }, SUBETHA_OK);
        assert_eq!(unsafe { subetha_ring_register_consumer(ring, &mut c) }, SUBETHA_OK);
        assert_eq!(p, expected, "producer slots are handed out in order");
        assert_eq!(c, expected, "consumer slots are handed out in order");
    }
}

fn scratch(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the wall clock is after the epoch")
        .as_nanos();
    std::env::temp_dir()
        .join(format!("subetha-ffi-lifecycle-{tag}-{}-{nanos}.bin", std::process::id()))
        .to_str()
        .expect("the temp dir is UTF-8")
        .to_owned()
}

/// A handle every thread reads, replaced by the destroyer. A `u64` rather
/// than a lock, because a caller must be free to hold a handle that the
/// destroyer retires under it: that is the race being tested.
struct Shared {
    handle: AtomicU64,
    stop: AtomicBool,
    calls: AtomicU64,
    refused: AtomicU64,
}

#[test]
fn a_destroy_waits_out_the_calls_inside_it_and_refuses_the_ones_after() {
    let _serial = serial();
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let options = options();
    // A slot per caller, not one slot shared by all of them. What this
    // test is after is a destroy landing on calls that are in flight;
    // the ring's own contract still holds during those calls, and a
    // consumer id is served by one thread at a time. Pointing every
    // caller at id 0 put four readers on a single-reader backing, which
    // a debug build now refuses rather than quietly handing two of them
    // the same item.
    let mut first: subetha_handle = 0;
    assert_eq!(
        unsafe { subetha_ring_create_anon(CALLERS as u32, CALLERS as u32, 64, &options, &mut first) },
        SUBETHA_OK
    );
    register_slots(first);

    let shared = Arc::new(Shared {
        handle: AtomicU64::new(first),
        stop: AtomicBool::new(false),
        calls: AtomicU64::new(0),
        refused: AtomicU64::new(0),
    });

    std::thread::scope(|scope| {
        for slot in 0..CALLERS {
            let shared = Arc::clone(&shared);
            scope.spawn(move || {
                let slot = slot as u32;
                let payload = [7u8; 16];
                let mut out = [0u8; SUBETHA_RING_SLOT_BYTES];
                let mut len = 0usize;
                while !shared.stop.load(Ordering::Acquire) {
                    let handle = shared.handle.load(Ordering::Acquire);
                    let rc = unsafe { subetha_ring_try_push(handle, slot, payload.as_ptr(), payload.len()) };
                    match rc {
                        SUBETHA_OK | SUBETHA_E_RING_FULL => {
                            shared.calls.fetch_add(1, Ordering::Relaxed);
                        }
                        SUBETHA_E_INVALID_HANDLE => {
                            shared.refused.fetch_add(1, Ordering::Relaxed);
                        }
                        other => panic!("a push on a live or destroyed handle returned {other}"),
                    }
                    let rc = unsafe { subetha_ring_try_pop(handle, slot, out.as_mut_ptr(), out.len(), &mut len) };
                    match rc {
                        SUBETHA_OK | SUBETHA_E_RING_EMPTY => {
                            shared.calls.fetch_add(1, Ordering::Relaxed);
                        }
                        SUBETHA_E_INVALID_HANDLE => {
                            shared.refused.fetch_add(1, Ordering::Relaxed);
                        }
                        other => panic!("a pop on a live or destroyed handle returned {other}"),
                    }
                }
            });
        }

        // The destroyer: retire the handle every caller is using and put a
        // fresh one in its place, over and over.
        let deadline = Instant::now() + RUN;
        let mut cycles = 0u64;
        while Instant::now() < deadline {
            let mut next: subetha_handle = 0;
            assert_eq!(
                unsafe { subetha_ring_create_anon(CALLERS as u32, CALLERS as u32, 64, &options, &mut next) },
                SUBETHA_OK
            );
            // Every slot is registered before the handle goes live, so a
            // caller that picks the new handle up finds its own id there.
            register_slots(next);
            let previous = shared.handle.swap(next, Ordering::AcqRel);
            assert_eq!(subetha_handle_destroy(previous), SUBETHA_OK, "the retired handle is destroyed once");
            cycles += 1;
        }
        shared.stop.store(true, Ordering::Release);

        assert!(cycles > 0, "the destroyer ran at least one cycle");
        assert!(shared.calls.load(Ordering::Relaxed) > 0, "the callers got through");
    });

    let last = shared.handle.load(Ordering::Acquire);
    assert_eq!(subetha_handle_destroy(last), SUBETHA_OK);
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}

#[test]
fn a_destroy_does_not_wait_for_a_call_on_another_object() {
    let _serial = serial();
    assert_eq!(subetha_init(SUBETHA_MODE_STRICT), SUBETHA_OK);
    let path = scratch("elsewhere");
    let c_path = std::ffi::CString::new(path.clone()).expect("the path has no NUL");
    let mut counter: subetha_handle = 0;
    assert_eq!(unsafe { subetha_atomic_u64_create(c_path.as_ptr(), 0, SUBETHA_MODE_STRICT, &mut counter) }, SUBETHA_OK);

    let options = options();
    let mut ring: subetha_handle = 0;
    assert_eq!(unsafe { subetha_ring_create_anon(1, 1, 64, &options, &mut ring) }, SUBETHA_OK);

    let stop = Arc::new(AtomicBool::new(false));
    let busy = Arc::new(AtomicU64::new(0));
    std::thread::scope(|scope| {
        let stop_for_worker = Arc::clone(&stop);
        let busy_for_worker = Arc::clone(&busy);
        scope.spawn(move || {
            // A thread that is almost always inside a call on the counter.
            while !stop_for_worker.load(Ordering::Acquire) {
                let rc = unsafe { subetha_atomic_u64_fetch_add(counter, 1, std::ptr::null_mut()) };
                assert_eq!(rc, SUBETHA_OK);
                busy_for_worker.fetch_add(1, Ordering::Relaxed);
            }
        });

        // Wait until the worker is actually running, then time a destroy of
        // the unrelated ring. It must not wait on the counter's calls.
        while busy.load(Ordering::Relaxed) < 1000 {
            std::hint::spin_loop();
        }
        let started = Instant::now();
        assert_eq!(subetha_handle_destroy(ring), SUBETHA_OK);
        let took = started.elapsed();
        stop.store(true, Ordering::Release);
        assert!(
            took < Duration::from_millis(250),
            "destroying a ring waited {took:?} on calls that were not its own"
        );
    });

    let mut total = 0u64;
    assert_eq!(unsafe { subetha_atomic_u64_load(counter, &mut total) }, SUBETHA_OK);
    assert!(total > 0, "the worker was inside calls throughout");
    assert_eq!(subetha_handle_destroy(counter), SUBETHA_OK);
    let mut report = subetha_unlink_report::default();
    assert_eq!(unsafe { subetha_atomic_unlink(c_path.as_ptr(), &mut report) }, SUBETHA_OK);
    assert_eq!(report.removed, 1, "the counter's file");
    assert_eq!(subetha_shutdown(), SUBETHA_OK);
}
