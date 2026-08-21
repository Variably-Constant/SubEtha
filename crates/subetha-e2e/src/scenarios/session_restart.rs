//! A peer restarts; its fresh session must still be delivered.
//!
//! The receiver runs in the parent. A sender child delivers a first
//! batch, advancing the decoder, and is KILLED - so the transport learns
//! of the restart only from the frames of the session that replaces it.
//! A second sender process then runs against the same receiver, drawing
//! its own connection id and numbering from the bottom again.
//!
//! Asserts that the second session's items are delivered, and that a
//! forged connection id from a socket that answers no challenge is
//! refused without disturbing the established session.
//!
//! Reports the active erasure code at the restart: the two codes reject
//! a new session by different mechanisms.

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

/// How long the parent keeps polling after a forgery, so the challenge it
/// provokes has time to go unanswered and be retired.
const FORGERY_SETTLE: Duration = Duration::from_millis(1500);

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

/// Wire layout of an RLC DATA datagram, restated because the forgery case
/// builds one from outside the crate. Drift here makes the forged datagram
/// implausible, which is why the case also asserts a challenge was issued.
const PKT_RLC_DATA: u8 = 10;
const RLC_DATA_HDR: usize = 1 + 8 + 4 + 4;

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

    // A forgery, before the genuine restart: a datagram announcing an
    // unknown connection id, from a socket that then refuses to answer the
    // challenge it provokes. This is the reset an off-path attacker would
    // want, and the receiver must decline it.
    let (adopted_before, failed_before) = rx.session_adoption_counts();
    forge_unknown_session(port)?;
    let deadline = Instant::now() + FORGERY_SETTLE;
    while Instant::now() < deadline {
        rx.poll().ok();
        sleep(Duration::from_millis(10));
    }
    let (adopted_after, failed_after) = rx.session_adoption_counts();
    require(
        adopted_after == adopted_before,
        format!(
            "a forged connection id was ADOPTED ({adopted_before} -> {adopted_after}); \
             an unauthenticated peer can reset this receiver's window"
        ),
    )?;
    require(
        failed_after > failed_before,
        format!(
            "the forgery was not even challenged ({failed_before} -> {failed_after} \
             unanswered); the receiver is not exercising the path check"
        ),
    )?;
    println!(
        "   parent: forged session refused - adoptions {adopted_after}, unanswered challenges {failed_after}"
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

/// Send datagrams announcing a connection id the receiver has never seen,
/// from a socket that is then dropped without answering anything.
///
/// Covers an UNANSWERED challenge yielding no adoption, not source-address
/// spoofing: the datagrams carry this process's real source address, since
/// forging that needs a raw socket.
fn forge_unknown_session(port: u16) -> Result<(), BoxErr> {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let target = format!("127.0.0.1:{port}");
    let mut pkt = vec![0u8; RLC_DATA_HDR + SYMBOL_LEN];
    pkt[0] = PKT_RLC_DATA;
    // A connection id no genuine peer here holds.
    pkt[1..9].copy_from_slice(&0xDEAD_BEEF_FEED_FACEu64.to_le_bytes());
    for attempt in 0..4u32 {
        pkt[9..13].copy_from_slice(&attempt.to_le_bytes());
        sock.send_to(&pkt, &target)?;
        sleep(Duration::from_millis(5));
    }
    println!("   parent: sent 4 forged datagrams under an unknown connection id");
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
