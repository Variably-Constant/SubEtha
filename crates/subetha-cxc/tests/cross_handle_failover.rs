//! Integration test: cross-handle "process" simulation demonstrating
//! failover within 1 epoch.
//!
//! True cross-process testing requires spawning a child binary which
//! is environment-specific. Instead we simulate the two participants
//! via TWO HeartbeatTable handles to the SAME backing file - which
//! is byte-for-byte the same as two processes opening the same
//! shared MMF (the OS aliases them onto the same physical pages).
//! The behavioural guarantees (heartbeat visibility, failover
//! detection within 1 epoch) are identical.

use std::time::Duration;

use subetha_cxc::{
    register_handler, unregister_handler,
    BackgroundScheduler, FailoverWatchdog, HeartbeatTable, Pass, SharedRing,
};

fn tmp(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    p.push(format!("subetha-it-{name}-{pid}.bin"));
    p
}

#[test]
fn failover_detected_within_one_epoch() {
    // Two handles to the same heartbeat table - the "two processes".
    let path = tmp("failover-1-epoch");
    let proc_a = HeartbeatTable::create(&path, 4).unwrap();
    let proc_b = HeartbeatTable::open(&path, 4).unwrap();

    let slot_a = proc_a.register(1001).unwrap();
    let _slot_b = proc_b.register(1002).unwrap();

    // Process A marks two work units in-flight then "crashes" (stops beating).
    proc_a.mark_in_flight(slot_a, 3);
    proc_a.mark_in_flight(slot_a, 7);
    proc_a.beat(slot_a);

    // Watchdog (could be either process; pick B since B is alive).
    let w = FailoverWatchdog::with_grace(&proc_b, 1);

    // Tick 1: B beats; A does not.
    proc_b.beat(proc_b.register(1003).unwrap_or(1));  // self-keep-alive
    let r1 = w.scan();
    // Still within grace.
    assert!(r1.is_empty(), "no reclaim within grace; got {:?}", r1.dead_slots);

    // Tick 2: B beats; A still does not. Lag for A now > grace.
    let r2 = w.scan();
    assert!(
        r2.dead_slots.iter().any(|(idx, snap)| {
            *idx == slot_a && snap.pid == 1001 && snap.in_flight_bitmap == (1u64 << 3) | (1u64 << 7)
        }),
        "expected A's slot to be reclaimed; got {:?}", r2.dead_slots
    );

    std::fs::remove_file(&path).ok();
}

#[test]
fn cross_handle_ring_carries_messages() {
    // Two handles to one ring; one pushes, the other pops.
    let path = tmp("cross-handle-ring");
    let producer = SharedRing::create(&path, 16).unwrap();
    let consumer = SharedRing::open(&path, 16).unwrap();

    producer.try_push(b"hello").unwrap();
    producer.try_push(b"world").unwrap();
    let mut buf = [0u8; subetha_cxc::PAYLOAD_BYTES];
    let _val = consumer.try_pop(&mut buf).unwrap();
    assert_eq!(&buf[..5], b"hello");
    let _val = consumer.try_pop(&mut buf).unwrap();
    assert_eq!(&buf[..5], b"world");

    std::fs::remove_file(&path).ok();
}

#[test]
fn ring_survives_close_and_reopen_with_data_intact() {
    // The "disk persistence" property: data written then flushed
    // remains when the file is reopened as a fresh ring instance.
    let path = tmp("disk-survive");
    {
        let r = SharedRing::create(&path, 8).unwrap();
        r.try_push(b"persistent-1").unwrap();
        r.try_push(b"persistent-2").unwrap();
        r.flush().unwrap();
    }
    // Reopen; previous data still there.
    let r2 = SharedRing::open(&path, 8).unwrap();
    let mut buf = [0u8; subetha_cxc::PAYLOAD_BYTES];
    let _val = r2.try_pop(&mut buf).unwrap();
    assert_eq!(&buf[..12], b"persistent-1");
    let _val = r2.try_pop(&mut buf).unwrap();
    assert_eq!(&buf[..12], b"persistent-2");
    std::fs::remove_file(&path).ok();
}

#[test]
fn scheduler_end_to_end_with_disk_backing() {
    let id = 0x3000_0001;
    register_handler(id, |args| {
        Ok(args.iter().rev().copied().collect())
    });

    let s = tmp("e2e-submit");
    let r = tmp("e2e-result");
    let h = tmp("e2e-hb");
    let sched = BackgroundScheduler::start(&s, &r, &h, 64, 8).unwrap();
    let sub = sched.submitter();
    let col = sched.collector();
    let token = sub.submit(&Pass {
        closure_id: id, args: b"abc".to_vec(),
    }).unwrap();

    let mut got = None;
    for _ in 0..500 {
        if let Ok(rr) = col.try_recv() {
            got = Some(rr);
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let got = got.expect("must receive within 1s");
    assert_eq!(got.token, token);
    let data = got.result.expect("Ok");
    assert_eq!(data, b"cba");

    unregister_handler(id);
    drop(sched);
    std::fs::remove_file(&s).ok();
    std::fs::remove_file(&r).ok();
    std::fs::remove_file(&h).ok();
}

#[test]
fn watchdog_scan_advances_global_epoch() {
    let path = tmp("watchdog-epoch");
    let t = HeartbeatTable::create(&path, 2).unwrap();
    let w = FailoverWatchdog::new(&t);
    let before = t.global_epoch();
    let r = w.scan();
    assert_eq!(r.new_global_epoch, before + 1);
    assert_eq!(t.global_epoch(), before + 1);
    std::fs::remove_file(&path).ok();
}

#[test]
fn two_processes_simulated_via_two_handles_share_state() {
    // Demonstrates the bytes-on-disk-are-bytes-in-process property.
    let path = tmp("two-handle-share");
    let t_a = HeartbeatTable::create(&path, 4).unwrap();
    let t_b = HeartbeatTable::open(&path, 4).unwrap();

    let slot_a = t_a.register(101).unwrap();
    let slot_b = t_b.register(102).unwrap();
    assert_ne!(slot_a, slot_b);

    // Process A marks bit; process B sees it via separate snapshot.
    t_a.mark_in_flight(slot_a, 5);
    let snap_from_b = t_b.snapshot(slot_a).unwrap();
    assert_eq!(snap_from_b.in_flight_bitmap, 1u64 << 5);
    assert_eq!(snap_from_b.pid, 101);

    // Process B advances global epoch; process A sees the increment.
    let new = t_b.tick_global_epoch();
    assert_eq!(t_a.global_epoch(), new);

    std::fs::remove_file(&path).ok();
}
