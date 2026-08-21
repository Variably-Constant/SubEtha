//! A peer restarts; its fresh session must still be delivered.
//!
//! The receiver runs in the parent and takes a first batch from a sender
//! child, which advances its decoder state. That child is KILLED, and a
//! second sender process starts against the same receiver. The second
//! process is a NEW session: it draws its own connection id and its
//! sequence numbering starts from the bottom again.
//!
//! The claim under test is that the receiver delivers the second
//! session's items. A receiver that latched its session identity at
//! first contact, or that judges freshness by a sequence number it has
//! already passed, discards them instead - and discards them silently,
//! which is what makes the failure hard to see from the application: it
//! observes only that nothing arrives.
//!
//! The restart must be a KILL rather than a clean shutdown. A peer that
//! announces its own departure is the easy case; the case that matters
//! is the one where the transport learns of the restart only from the
//! frames of the session that replaces it.
//!
//! The scenario also reports which erasure code the endpoint is on when
//! the restart happens, because the two codes reject a new session by
//! different mechanisms and the answer decides which one this exercises.

use std::thread::sleep;
use std::time::{Duration, Instant};

use subetha_cxc::sens_unified::{UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender};

use crate::harness::{arg_u64, require, BoxErr, Harness};

/// Symbol size the endpoint codes over.
const SYMBOL_LEN: usize = 1200;

/// Items each session sends. Enough to advance the decoder well past its
/// starting state, so the second session's low sequence numbers land
/// behind the frontier rather than near it.
const ITEMS_PER_SESSION: usize = 64;

/// How long the parent waits for a session's items before calling it.
const DELIVER_TIMEOUT: Duration = Duration::from_secs(20);

/// Payload for `session`, item `i`: the tag identifies which process
/// sent it, so the parent can tell the two sessions apart.
fn payload(session: u8, i: usize) -> Vec<u8> {
    let mut v = vec![session; 16];
    v[1..9].copy_from_slice(&(i as u64).to_le_bytes());
    v
}

fn tag_of(item: &[u8]) -> Option<u8> {
    item.first().copied()
}

pub fn parent(h: &Harness) -> Result<(), BoxErr> {
    let mut rx = UnifiedSensReceiver::bind("127.0.0.1:0", UnifiedConfig::new(SYMBOL_LEN))
        .map_err(|e| format!("bind receiver: {e}"))?;
    let port = rx
        .local_addr()
        .map_err(|e| format!("receiver local_addr: {e}"))?
        .port();
    println!("   parent: receiver on 127.0.0.1:{port}, code {:?}", rx.active_code());

    // Session A: establishes the connection and advances the decoder.
    let mut first = h.spawn("sender", &[port.to_string(), "1".to_string()])?;
    let got_a = collect_tag(&mut rx, 1, ITEMS_PER_SESSION, DELIVER_TIMEOUT);
    require(
        got_a == ITEMS_PER_SESSION,
        format!("session A delivered {got_a}/{ITEMS_PER_SESSION} items - the scenario cannot test a restart it never established"),
    )?;
    println!(
        "   parent: session A delivered {got_a} items, receiver code {:?}, switches {}",
        rx.active_code(),
        rx.switches()
    );

    // The restart. A kill, so the receiver is told nothing.
    first.kill()?;
    first.wait()?;
    println!("   parent: session A killed without announcing departure");

    // Session B: a new process, so a new connection id and sequence
    // numbering from the bottom.
    let mut second = h.spawn("sender", &[port.to_string(), "2".to_string()])?;
    let got_b = collect_tag(&mut rx, 2, ITEMS_PER_SESSION, DELIVER_TIMEOUT);
    second.kill().ok();
    second.wait().ok();

    let code = rx.active_code();
    let switches = rx.switches();
    println!(
        "   parent: session B delivered {got_b}/{ITEMS_PER_SESSION}, receiver code {code:?}, switches {switches}"
    );
    println!(
        "   parent: CODE AT RESTART = {code:?} ({} code switch(es) over the run)",
        switches
    );

    require(
        got_b == ITEMS_PER_SESSION,
        format!(
            "restarted peer delivered {got_b}/{ITEMS_PER_SESSION} items - the receiver is \
             discarding the new session (code {code:?}, {switches} switch(es))"
        ),
    )?;
    Ok(())
}

/// Poll until `want` items carrying `tag` have arrived, or time out.
/// Items from other tags are counted out but not returned - a late
/// arrival from the dead session is not evidence about the new one.
fn collect_tag(
    rx: &mut UnifiedSensReceiver,
    tag: u8,
    want: usize,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    let mut seen = 0usize;
    while seen < want && Instant::now() < deadline {
        match rx.poll() {
            Ok(items) => {
                for item in items {
                    if tag_of(&item) == Some(tag) {
                        seen += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("   parent: receiver poll error: {e}");
                break;
            }
        }
        sleep(Duration::from_millis(2));
    }
    seen
}

pub fn child(role: &str, args: &[String]) -> Result<(), BoxErr> {
    match role {
        "sender" => sender(args),
        other => Err(format!("session-restart: unknown child role {other:?}").into()),
    }
}

/// Send one session's worth of items, then stay alive so the transport
/// can service repair and ARQ for what it sent. The parent ends it.
fn sender(args: &[String]) -> Result<(), BoxErr> {
    let port = arg_u64(args, 0, "receiver port")? as u16;
    let tag = arg_u64(args, 1, "session tag")? as u8;
    let peer = format!("127.0.0.1:{port}")
        .parse()
        .map_err(|e| format!("parse peer: {e}"))?;

    let mut tx = UnifiedSensSender::connect("127.0.0.1:0", peer, UnifiedConfig::new(SYMBOL_LEN))
        .map_err(|e| format!("connect sender: {e}"))?;
    println!(
        "   sender {tag}: pid {} connected, code {:?}",
        std::process::id(),
        tx.active_code()
    );

    for i in 0..ITEMS_PER_SESSION {
        tx.send_item(&payload(tag, i))
            .map_err(|e| format!("send item {i}: {e}"))?;
    }
    let code = tx.active_code();
    println!(
        "   sender {tag}: {ITEMS_PER_SESSION} items sent, code {code:?}, switches {}",
        tx.switches()
    );

    // Keep servicing the transport until the parent ends this process.
    loop {
        tx.finish().ok();
        sleep(Duration::from_millis(20));
    }
}
