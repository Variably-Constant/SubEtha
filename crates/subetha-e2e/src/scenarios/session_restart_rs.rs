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

/// How long the parent polls after a forgery, so the challenge it
/// provokes has time to go unanswered and be retired.
const FORGERY_SETTLE: Duration = Duration::from_millis(1500);

/// Wire layout of a block-RS DATA datagram, restated because the forgery
/// case builds one from outside the crate. Drift here makes the forged
/// datagram implausible, which is why the case also asserts a challenge
/// was issued rather than only that nothing was adopted.
/// One window's `(next_needed, highest_seen, datagrams_in)` at an instant,
/// so two samples a second apart give the rate rather than the lifetime
/// total.
type WindowSample = (u32, u32, u64);

const PKT_DATA: u8 = 1;
const RS_DATA_HEADER: usize = 13;
const RS_EPOCH_OFFSET: usize = 9;

/// Send datagrams under an epoch no peer here holds, from a socket that
/// is dropped without answering anything.
///
/// Covers an UNANSWERED challenge yielding no adoption, not source-address
/// spoofing: the datagrams carry this process's real source address, since
/// forging that needs a raw socket.
fn forge_unknown_session(port: u16) -> Result<(), BoxErr> {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let target = format!("127.0.0.1:{port}");
    let mut pkt = vec![0u8; RS_DATA_HEADER + SYMBOL_LEN];
    pkt[0] = PKT_DATA;
    pkt[5] = 0; // shard index
    pkt[6] = 4; // k
    pkt[7] = 2; // r
    pkt[RS_EPOCH_OFFSET..RS_EPOCH_OFFSET + 4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    for block in 0..4u32 {
        pkt[1..5].copy_from_slice(&block.to_le_bytes());
        sock.send_to(&pkt, &target)?;
        sleep(Duration::from_millis(5));
    }
    println!("   parent: sent 4 forged datagrams under an unknown epoch");
    Ok(())
}

/// Service the transport for one round, reporting any error the drain
/// returns. Discarding it here would hide an egress failure, which the
/// receiver cannot tell from a datagram lost in the network.
fn drain(tx: &mut UnifiedSensSender, tag: u8, round: u32) {
    if let Err(e) = tx.finish_within(Duration::from_millis(500)) {
        eprintln!("   sender {tag}: round {round}, drain error: {e}");
    }
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

    // A forged epoch, before the genuine restart: a datagram announcing a
    // session no peer here holds, from a socket that answers nothing. This
    // is the reset an off-path attacker would want.
    let (adopted_before, failed_before) = rx.session_adoption_counts();
    let armed_before = rx.rs_session_challenges_armed();
    forge_unknown_session(port)?;
    let deadline = Instant::now() + FORGERY_SETTLE;
    while Instant::now() < deadline {
        rx.poll().ok();
        sleep(Duration::from_millis(10));
    }
    let (adopted_after, failed_after) = rx.session_adoption_counts();
    // Whether a challenge was ever armed for the forged epoch. Without
    // this, a receiver that never noticed the forgery and one that
    // challenged it and is still inside the answer window report the
    // same way, and only the first is a defect.
    let armed_after = rx.rs_session_challenges_armed();
    require(
        adopted_after == adopted_before,
        format!(
            "a forged epoch was ADOPTED ({adopted_before} -> {adopted_after}); an \
             unauthenticated peer can reset this receiver's window"
        ),
    )?;
    // The security property is that an unanswered epoch is CHALLENGED and
    // never adopted. Whether the challenge has additionally been retired
    // as failed by now is bookkeeping on a timer, and asserting it here
    // made the gate fail on a busy host while the property itself held.
    // Retirement is reported below, and guarded on its own terms by
    // `a_challenge_retires_on_its_timeout`.
    require(
        armed_after > armed_before,
        format!(
            "the forgery was not challenged (challenges armed {armed_before} -> \
             {armed_after}, {failed_before} -> {failed_after} unanswered); the \
             receiver is not exercising the path check"
        ),
    )?;
    let still_pending = rx.rs_pending_admissions();
    println!(
        "   parent: forged epoch refused - adoptions {adopted_after}, challenges \
         armed {armed_after}, unanswered {failed_after}, still pending \
         {still_pending:?}"
    );

    first.kill()?;
    first.wait()?;
    println!("   parent: session A killed without announcing departure");

    let mut second = h.spawn("sender", &[port.to_string(), "2".to_string()])?;
    let got_b = collect_tag(&mut rx, 2, ITEMS_PER_SESSION, DELIVER_TIMEOUT);
    second.kill().ok();
    second.wait().ok();

    let code = rx.active_code();
    println!("   parent: session B delivered {got_b}/{ITEMS_PER_SESSION}, code {code:?}");
    // Where the window actually stopped, so a shortfall names the block it
    // is waiting on instead of only the count that did not arrive.
    let live = rx.live_rs_sessions();
    let frontiers: Vec<String> = live
        .iter()
        .map(|e| match rx.rs_session_frontier(*e) {
            // `peer` is the address this window aims its ACKs and NAKs at.
            // A completed window pointing at the RESTARTED sender is the
            // rebinding that would free that sender's pending blocks.
            // A window that ingested datagrams without advancing either
            // took them and could not use them, or refused them at a gate.
            // The reject tally names which gate, so a stall separates a
            // shard that never arrived from one turned away on arrival.
            Some((next, high, recv, peer)) => format!(
                "epoch {e}: next_needed {next}, highest_seen {high}, \
                 datagrams_in {recv}, peer {peer:?}, rejects {:?}",
                rx.rs_session_rejects(*e)
            ),
            None => format!("epoch {e}: no window"),
        })
        .collect();
    // The counters above are lifetime totals, which cannot tell a window
    // still taking traffic from one that went quiet an instant after it
    // stalled. Sample the same windows again after a pause and report the
    // delta, which is the steady state rather than the accumulation.
    let first_sample: Vec<(u32, Option<WindowSample>)> = live
        .iter()
        .map(|e| (*e, rx.rs_session_frontier(*e).map(|(n, h, r, _)| (n, h, r))))
        .collect();
    let settle = Instant::now() + Duration::from_millis(1000);
    while Instant::now() < settle {
        rx.poll().ok();
        sleep(Duration::from_millis(2));
    }
    let deltas: Vec<String> = first_sample
        .iter()
        .map(|(e, before)| match (before, rx.rs_session_frontier(*e)) {
            (Some((n0, h0, r0)), Some((n1, h1, r1, _))) => format!(
                "epoch {e}: next_needed {n0}->{n1}, highest_seen {h0}->{h1}, \
                 datagrams_in +{}",
                r1.saturating_sub(*r0)
            ),
            _ => format!("epoch {e}: window went away"),
        })
        .collect();
    println!("   parent: over 1s of continued polling [{}]", deltas.join("; "));
    println!(
        "   parent: demux unroutable {:?}, rejects now {:?}",
        rx.demux_unroutable(),
        live.iter().map(|e| rx.rs_session_rejects(*e)).collect::<Vec<_>>(),
    );

    let pending = rx.rs_pending_admissions();
    println!(
        "   parent: rs windows [{}], pending admissions {pending:?}, adoptions {:?}",
        frontiers.join("; "),
        rx.session_adoption_counts(),
    );
    // Whether the datagrams reached this process at all. recv_ok climbing
    // while the window never saw the blocks puts the loss inside the
    // routing or the queue; recv_ok flat puts it below us, in the socket.
    println!(
        "   parent: demux probe {:?}, errors {:?}, stale_for {:?}, alive {}",
        rx.demux_probe(),
        rx.demux_errors(),
        rx.demux_stale_for(),
        rx.demux_alive(),
    );
    // Two windows may never aim at the same peer: each session's acks and
    // naks belong to the sender that owns it, and a completed window
    // pointing at a live sender acks blocks that sender still has to
    // deliver, freeing them from its retransmit buffer for good. This is
    // an invariant, so it catches the fault even on a run whose delivery
    // happens to survive it.
    // Recorded rather than returned, so a run reports BOTH the delivery
    // result and this property. Returning here would abort before the
    // delivery check and make the shortfall unobservable whenever the
    // rebinding fires, which is exactly how one comparison of the two
    // was misread.
    let mut shared_peer: Option<String> = None;
    let mut seen: Vec<(u32, std::net::SocketAddr)> = Vec::new();
    for e in &live {
        if let Some((_, _, _, Some(addr))) = rx.rs_session_frontier(*e) {
            if let Some((other, _)) = seen.iter().find(|(_, a)| *a == addr) {
                shared_peer = Some(format!(
                    "windows {other} and {e} both aim at {addr} - one sender's \
                     control plane is pointed at another's peer"
                ));
            }
            seen.push((*e, addr));
        }
    }

    if got_b != ITEMS_PER_SESSION {
        return Err(format!(
            "restarted peer delivered {got_b}/{ITEMS_PER_SESSION} over Rs - the receiver \
             is discarding the new session (code {code:?}); windows [{}], pending {pending:?}{}",
            frontiers.join("; "),
            match &shared_peer {
                Some(s) => format!("; ALSO {s}"),
                None => String::new(),
            },
        )
        .into());
    }
    if let Some(s) = shared_peer {
        return Err(format!("{s}; windows [{}]", frontiers.join("; ")).into());
    }
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

    // Whether the tail ever left this process, reported while the drain
    // runs: `sent` is forward datagrams put on the wire and `fed_back` is
    // what the receiver said it got. A shortfall the receiver blames on a
    // block it never saw is a different defect depending on whether the
    // sender transmitted it at all.
    for round in 0..40 {
        let (sent, fed_back) = tx.raw_sent_recv();
        // Which side a stall is on. `oldest_pending` naming the block the
        // receiver is waiting for means this sender still holds it and is
        // answering NAKs for it, so the loss is on the wire or at ingest;
        // a `next_block_id` at or below that block means it was never
        // produced and no NAK can ever be served.
        println!(
            "   sender {tag}: round {round}, datagrams sent {sent}, fed back \
             {fed_back}, tx probe {:?}, egress {:?}, last (nak, retx) {:?}",
            tx.rs_tx_probe(),
            tx.rs_egress_counts(),
            tx.rs_last_nak_and_retx(),
        );
        drain(&mut tx, tag, round);
        sleep(Duration::from_millis(10));
    }
    let mut round = 40;
    loop {
        drain(&mut tx, tag, round);
        round += 1;
        sleep(Duration::from_millis(10));
    }
}
