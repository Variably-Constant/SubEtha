//! A process dies holding work; the watchdog reclaims it.
//!
//! The holder is a child process that registers in the shared heartbeat
//! table under its own pid, marks two work units in flight, beats once
//! and blocks. The parent kills it and runs the watchdog.
//!
//! Asserts two things: that the parent reads the holder's pid and
//! in-flight bitmap out of the mapped file, and that the watchdog
//! reports that slot with the dead pid and the exact bitmap on the first
//! scan past the grace window.
//!
//! The kill is load-bearing. It leaves the file as the dead process last
//! wrote it - no unwind, no destructor, no cooperative deregistration -
//! which is the state the watchdog has to be correct against.

use std::thread::sleep;
use std::time::{Duration, Instant};

use subetha_cxc::{FailoverWatchdog, HeartbeatTable};

use crate::harness::{arg_path, require, BoxErr, Harness};

/// Slots in the table. Larger than the one holder so the scan has to
/// pick the right slot rather than the only slot.
const CAPACITY: usize = 8;

/// Work units the holder claims before it dies.
const WORK_BITS: [u8; 2] = [3, 7];

/// Scans a slot may lag before the watchdog calls it dead.
const GRACE: u64 = 1;

/// How long the parent waits for the holder's registration to appear.
const VISIBLE_TIMEOUT: Duration = Duration::from_secs(20);

fn work_bitmap() -> u64 {
    WORK_BITS.iter().fold(0u64, |acc, b| acc | (1u64 << b))
}

pub fn parent(h: &Harness) -> Result<(), BoxErr> {
    let hb = h.path("heartbeat.bin");
    let table = HeartbeatTable::create(&hb, CAPACITY)
        .map_err(|e| format!("create heartbeat table: {e:?}"))?;

    let mut holder = h.spawn("holder", &[hb.to_string_lossy().as_ref()])?;
    let holder_pid = holder.id();

    let slot = match wait_for_pid(&table, holder_pid, VISIBLE_TIMEOUT) {
        Some(slot) => slot,
        None => {
            holder.kill().ok();
            holder.wait().ok();
            return Err(format!(
                "holder pid {holder_pid} never became visible in the parent within {VISIBLE_TIMEOUT:?}"
            )
            .into());
        }
    };

    let live = table
        .snapshot(slot)
        .ok_or_else(|| format!("slot {slot} vanished between scan and read"))?;
    require(
        live.in_flight_bitmap == work_bitmap(),
        format!(
            "holder's in-flight bitmap read as {:#x}, expected {:#x}",
            live.in_flight_bitmap,
            work_bitmap()
        ),
    )?;
    println!(
        "   parent: holder pid {holder_pid} visible in slot {slot}, bitmap {:#x}",
        live.in_flight_bitmap
    );

    holder.kill()?;
    let status = holder.wait()?;
    println!("   parent: holder killed ({status})");

    let watchdog = FailoverWatchdog::with_grace(&table, GRACE);

    let epoch_before = table.global_epoch();
    let within_grace = watchdog.scan();
    require(
        within_grace.new_global_epoch == epoch_before + 1,
        format!(
            "scan did not advance the global epoch: {} -> {}",
            epoch_before, within_grace.new_global_epoch
        ),
    )?;
    require(
        within_grace.is_empty(),
        format!(
            "slot reclaimed while still inside the grace window: {:?}",
            within_grace.dead_slots
        ),
    )?;
    println!("   parent: scan 1 inside grace, nothing reclaimed");

    let past_grace = watchdog.scan();
    let reclaimed = past_grace
        .dead_slots
        .iter()
        .find(|(idx, _)| *idx == slot)
        .ok_or_else(|| {
            format!(
                "holder's slot {slot} not reclaimed past the grace window; got {:?}",
                past_grace.dead_slots
            )
        })?;

    let snap = reclaimed.1;
    require(
        snap.pid == holder_pid,
        format!("reclaimed slot carries pid {}, expected {holder_pid}", snap.pid),
    )?;
    require(
        snap.in_flight_bitmap == work_bitmap(),
        format!(
            "reclaimed work bitmap {:#x}, expected {:#x}",
            snap.in_flight_bitmap,
            work_bitmap()
        ),
    )?;

    let recovered: Vec<u8> = FailoverWatchdog::iter_in_flight_bits(snap.in_flight_bitmap).collect();
    require(
        recovered == WORK_BITS,
        format!("recovered work units {recovered:?}, expected {WORK_BITS:?}"),
    )?;
    println!("   parent: dead pid {holder_pid} reclaimed, work units {recovered:?} recoverable");

    Ok(())
}

/// Poll the table until some slot reports `pid`, or time out.
fn wait_for_pid(table: &HeartbeatTable, pid: u32, timeout: Duration) -> Option<usize> {
    let deadline = Instant::now() + timeout;
    loop {
        for idx in 0..table.capacity() {
            if let Some(snap) = table.snapshot(idx) {
                // A slot only counts once the holder has claimed its
                // work: pid lands before the bitmap does, and the
                // parent must not read the gap between the two.
                if snap.pid == pid && snap.in_flight_bitmap != 0 {
                    return Some(idx);
                }
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(10));
    }
}

pub fn child(role: &str, args: &[String]) -> Result<(), BoxErr> {
    match role {
        "holder" => holder(args),
        other => Err(format!("failover: unknown child role {other:?}").into()),
    }
}

/// Register, claim work, beat once, then wait to be killed.
fn holder(args: &[String]) -> Result<(), BoxErr> {
    let hb = arg_path(args, 0, "heartbeat path")?;
    let table = HeartbeatTable::open(hb, CAPACITY)
        .map_err(|e| format!("holder open heartbeat table: {e:?}"))?;

    let pid = std::process::id();
    let slot = table
        .register(pid)
        .map_err(|e| format!("holder register: {e:?}"))?;

    for bit in WORK_BITS {
        table.mark_in_flight(slot, bit);
    }
    table.beat(slot);
    println!("   holder: pid {pid} registered in slot {slot}, {} units in flight", WORK_BITS.len());

    // The parent ends this process. Sleeping rather than spinning
    // keeps the holder off the CPU while the parent reads its state.
    loop {
        sleep(Duration::from_millis(50));
    }
}
