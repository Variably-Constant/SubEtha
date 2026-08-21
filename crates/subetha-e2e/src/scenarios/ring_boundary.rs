//! Ring payloads across a real boundary, and across process death.
//!
//! **Carry.** The parent pushes into a mapped ring; a child process
//! opens the same file and pops, sharing nothing but the file.
//!
//! **Persistence.** A writer child creates a second ring, pushes,
//! flushes and exits; the parent then opens that file and pops, so the
//! process that wrote the bytes is gone when they are read.
//!
//! Both are checked against the fixed-slot contract: a push zero-fills
//! the rest of the slot and a pop yields the whole slot, so the payload
//! length does not survive the round trip. `AdaptiveRing::send_frame` is
//! the variable-length path.

use subetha_cxc::{SharedRing, PAYLOAD_BYTES};

use crate::harness::{arg_path, require, BoxErr, Harness};

const CAPACITY: usize = 16;

/// Pushed by the parent, popped by the child.
const CARRIED: [&[u8]; 3] = [b"hello", b"world", b"subetha-crosses-the-boundary"];

/// Pushed by a child that then exits; popped by the parent.
const PERSISTED: [&[u8]; 2] = [b"persistent-1", b"persistent-2"];

pub fn parent(h: &Harness) -> Result<(), BoxErr> {
    carry(h)?;
    persistence(h)
}

/// Parent pushes, a child pops and verifies.
fn carry(h: &Harness) -> Result<(), BoxErr> {
    let path = h.path("carry.bin");
    let ring = SharedRing::create(&path, CAPACITY)
        .map_err(|e| format!("create carry ring: {e:?}"))?;

    for payload in CARRIED {
        ring.try_push(payload)
            .map_err(|e| format!("push {:?}: {e:?}", show(payload), ))?;
    }
    ring.flush().map_err(|e| format!("flush carry ring: {e:?}"))?;
    println!("   parent: pushed {} payloads into the carry ring", CARRIED.len());

    h.run("drain", &[path.to_string_lossy().as_ref()])?;
    println!("   parent: child drained the carry ring byte-exact");
    Ok(())
}

/// A child writes and dies; the parent reads what it left behind.
fn persistence(h: &Harness) -> Result<(), BoxErr> {
    let path = h.path("persist.bin");
    h.run("writer", &[path.to_string_lossy().as_ref()])?;
    println!("   parent: writer process has exited");

    let ring = SharedRing::open(&path, CAPACITY)
        .map_err(|e| format!("open persisted ring: {e:?}"))?;
    let mut buf = [0u8; PAYLOAD_BYTES];
    for expected in PERSISTED {
        let n = ring
            .try_pop(&mut buf)
            .map_err(|e| format!("pop persisted {:?}: {e:?}", show(expected)))?;
        check_slot(&buf[..n], expected, "persisted")?;
    }
    println!("   parent: recovered {} payloads written by the dead process", PERSISTED.len());
    Ok(())
}

pub fn child(role: &str, args: &[String]) -> Result<(), BoxErr> {
    match role {
        "drain" => drain(args),
        "writer" => writer(args),
        other => Err(format!("ring-boundary: unknown child role {other:?}").into()),
    }
}

fn drain(args: &[String]) -> Result<(), BoxErr> {
    let path = arg_path(args, 0, "carry ring path")?;
    let ring = SharedRing::open(path, CAPACITY)
        .map_err(|e| format!("drain open: {e:?}"))?;

    let mut buf = [0u8; PAYLOAD_BYTES];
    for expected in CARRIED {
        let n = ring
            .try_pop(&mut buf)
            .map_err(|e| format!("drain pop {:?}: {e:?}", show(expected)))?;
        check_slot(&buf[..n], expected, "drain")?;
    }
    println!("   drain: {} payloads recovered in push order", CARRIED.len());
    Ok(())
}

fn writer(args: &[String]) -> Result<(), BoxErr> {
    let path = arg_path(args, 0, "persist ring path")?;
    let ring = SharedRing::create(path, CAPACITY)
        .map_err(|e| format!("writer create: {e:?}"))?;
    for payload in PERSISTED {
        ring.try_push(payload)
            .map_err(|e| format!("writer push {:?}: {e:?}", show(payload)))?;
    }
    ring.flush().map_err(|e| format!("writer flush: {e:?}"))?;
    println!("   writer: wrote and flushed {} payloads, exiting", PERSISTED.len());
    Ok(())
}

/// Bytes as text where they are text, so a mismatch is readable.
fn show(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// A popped slot carries `expected` at its head and zeroes behind it.
fn check_slot(slot: &[u8], expected: &[u8], whose: &str) -> Result<(), BoxErr> {
    require(
        slot.len() == PAYLOAD_BYTES,
        format!("{whose}: pop yielded {} bytes, expected the {PAYLOAD_BYTES}-byte slot", slot.len()),
    )?;
    require(
        &slot[..expected.len()] == expected,
        format!(
            "{whose}: slot head is {:?}, expected {:?} - payload or FIFO order not preserved",
            show(&slot[..expected.len()]),
            show(expected)
        ),
    )?;
    require(
        slot[expected.len()..].iter().all(|b| *b == 0),
        format!("{whose}: slot tail behind {:?} is not zero-filled", show(expected)),
    )
}
