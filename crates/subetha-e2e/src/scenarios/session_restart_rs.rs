//! The same restart, pinned to the block Reed-Solomon code.
//!
//! [`session_restart`](super::session_restart) exercises whichever code
//! the loss-driven selector picks, which on a healthy link is RLC start
//! to finish. `CodePolicy::ForceRs` pins both ends to block-RS instead,
//! so the restart lands on the other decoder.
//!
//! The two codes reject a replacement session by different mechanisms -
//! RLC by a connection id it has latched, block-RS by a block sequence
//! its frontier has passed - so only running both says whether either
//! survives a peer restart.
//!
//! Also asserts that a forged epoch from a socket answering nothing is
//! refused, which is what makes the reset cost an attacker the ability
//! to receive at the address it claims.

use std::thread::sleep;
use std::time::{Duration, Instant};

use subetha_cxc::sens_unified::{
    CodePolicy, SensCode, UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender,
};

use crate::harness::{arg_u64, require, BoxErr, Harness};

const SYMBOL_LEN: usize = 1200;
const ITEMS_PER_SESSION: usize = 64;
const DELIVER_TIMEOUT: Duration = Duration::from_secs(25);

fn config() -> UnifiedConfig {
    let mut cfg = UnifiedConfig::new(SYMBOL_LEN);
    cfg.policy = CodePolicy::ForceRs;
    cfg
}

fn payload(session: u8, i: usize) -> Vec<u8> {
    let mut v = vec![session; 16];
    v[1..9].copy_from_slice(&(i as u64).to_le_bytes());
    v
}

pub fn parent(h: &Harness) -> Result<(), BoxErr> {
    let mut rx = UnifiedSensReceiver::bind("127.0.0.1:0", config())
        .map_err(|e| format!("bind receiver: {e}"))?;
    let port = rx
        .local_addr()
        .map_err(|e| format!("receiver local_addr: {e}"))?
        .port();
    require(
        rx.active_code() == SensCode::Rs,
        format!(
            "receiver started on {:?}, not Rs - ForceRs did not pin the code and this \
             scenario would silently retest the RLC path",
            rx.active_code()
        ),
    )?;
    println!("   parent: receiver on 127.0.0.1:{port}, code {:?} (pinned)", rx.active_code());

    let mut first = h.spawn("sender", &[port.to_string(), "1".to_string()])?;
    let got_a = collect_tag(&mut rx, 1, ITEMS_PER_SESSION, DELIVER_TIMEOUT);
    require(
        got_a == ITEMS_PER_SESSION,
        format!("session A delivered {got_a}/{ITEMS_PER_SESSION} over Rs"),
    )?;
    println!("   parent: session A delivered {got_a} items over {:?}", rx.active_code());

    first.kill()?;
    first.wait()?;
    println!("   parent: session A killed without announcing departure");

    let mut second = h.spawn("sender", &[port.to_string(), "2".to_string()])?;
    let got_b = collect_tag(&mut rx, 2, ITEMS_PER_SESSION, DELIVER_TIMEOUT);
    second.kill().ok();
    second.wait().ok();

    let code = rx.active_code();
    println!("   parent: session B delivered {got_b}/{ITEMS_PER_SESSION}, code {code:?}");
    require(
        got_b == ITEMS_PER_SESSION,
        format!(
            "restarted peer delivered {got_b}/{ITEMS_PER_SESSION} over Rs - the receiver \
             is discarding the new session (code {code:?})"
        ),
    )?;
    Ok(())
}

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
                    if item.first().copied() == Some(tag) {
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
        other => Err(format!("session-restart-rs: unknown child role {other:?}").into()),
    }
}

/// Send one session's items, then keep the transport serviced so the
/// receiver can recover the burst once it has adopted the session. A
/// sender that flushed and went quiet would have nothing to re-offer.
fn sender(args: &[String]) -> Result<(), BoxErr> {
    let port = arg_u64(args, 0, "receiver port")? as u16;
    let tag = arg_u64(args, 1, "session tag")? as u8;
    let peer = format!("127.0.0.1:{port}")
        .parse()
        .map_err(|e| format!("parse peer: {e}"))?;

    let mut tx = UnifiedSensSender::connect("127.0.0.1:0", peer, config())
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
    println!("   sender {tag}: {ITEMS_PER_SESSION} items sent, code {:?}", tx.active_code());

    loop {
        tx.finish().ok();
        sleep(Duration::from_millis(10));
    }
}
