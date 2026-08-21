//! The mirror: the RECEIVING end restarts, mid-stream.
//!
//! [`session_restart`](super::session_restart) kills the sender, so the
//! receiver meets a session whose source ids start from the bottom. This
//! kills the other end, which is the opposite problem: a replacement
//! receiver binds the same port and meets a sender already well into its
//! id space. Its decode window starts at the bottom; the traffic arrives
//! at the top.
//!
//! Both directions have to hold, because either end can be the one that
//! restarts and neither knows which case it is in. A transport that only
//! survives a sender restart leaves a peer-bounce failure to be found in
//! production by whoever bounced the wrong process.
//!
//! # Why the parent only supervises
//!
//! Every blocking call lives in a child process. `send_item` on the
//! unified sender owns RLC's flow-window wait internally, so with nothing
//! draining the stream it does not return for as long as its deadlock
//! detection takes to escape - and a deadline checked around such a call
//! is never reached. A wedged child is killable; a wedged parent thread is
//! not. So the parent here spawns, watches for marker files, and kills:
//! its own worst case is bounded by its own clock no matter what the
//! transport does.

use std::path::{Path, PathBuf};
use std::process::Child;
use std::thread::sleep;
use std::time::{Duration, Instant};

use subetha_cxc::sens_unified::{UnifiedConfig, UnifiedSensReceiver, UnifiedSensSender};

use crate::harness::{arg_path, arg_u64, require, BoxErr, Harness};

const SYMBOL_LEN: usize = 1200;

/// Items a receiver must take before it reports.
const ITEMS_PER_RECEIVER: usize = 32;

/// Tag every item carries, so a receiver counts this scenario's traffic
/// and not a stray datagram.
const TAG: u8 = 0xA5;

/// How long the parent waits for a receiver to report.
const REPORT_TIMEOUT: Duration = Duration::from_secs(25);

/// Gap between sends in the sender child, slow enough that a receiver
/// which never binds cannot be starved by a burst alone.
const SEND_GAP: Duration = Duration::from_millis(4);

pub fn parent(h: &Harness) -> Result<(), BoxErr> {
    let port = reserve_port()?;
    let first = h.path("r1.done");
    let second = h.path("r2.done");

    // Receiver first, so the stream has somewhere to land from the start.
    let mut r1 = spawn_receiver(h, port, &first)?;
    let mut tx = h.spawn("sender", &[port.to_string()])?;

    let established = wait_for_marker(&first, REPORT_TIMEOUT);
    if !established {
        kill_all(&mut [&mut r1, &mut tx]);
        return Err(format!(
            "the first receiver never took {ITEMS_PER_RECEIVER} items - the scenario \
             cannot test a restart it never established"
        )
        .into());
    }
    println!("   parent: first receiver took its batch");

    r1.kill()?;
    r1.wait()?;
    println!("   parent: first receiver killed mid-stream, sender still running");

    // The replacement binds the same port against a sender that is already
    // past the bottom of its id space.
    let mut r2 = spawn_receiver(h, port, &second)?;
    let recovered = wait_for_marker(&second, REPORT_TIMEOUT);
    kill_all(&mut [&mut r2, &mut tx]);

    require(
        recovered,
        format!(
            "the replacement receiver never took {ITEMS_PER_RECEIVER} items within \
             {REPORT_TIMEOUT:?} - a fresh decoder is not accepting a stream already \
             in progress"
        ),
    )?;
    println!("   parent: replacement receiver took its batch from a mid-stream sender");
    Ok(())
}

fn spawn_receiver(h: &Harness, port: u16, marker: &Path) -> Result<Child, BoxErr> {
    h.spawn(
        "receiver",
        &[port.to_string(), marker.to_string_lossy().into_owned()],
    )
}

/// Bind and drop a socket to learn a free port. The replacement receiver
/// must rebind the SAME port, so it cannot be chosen by the OS at bind.
fn reserve_port() -> Result<u16, BoxErr> {
    let s = std::net::UdpSocket::bind("127.0.0.1:0")?;
    Ok(s.local_addr()?.port())
}

/// Watch for a child's marker file. The parent blocks on nothing but its
/// own clock, so a wedged child costs this deadline and no more.
fn wait_for_marker(marker: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if marker.exists() {
            return true;
        }
        sleep(Duration::from_millis(20));
    }
    false
}

fn kill_all(children: &mut [&mut Child]) {
    for c in children.iter_mut() {
        c.kill().ok();
        c.wait().ok();
    }
}

pub fn child(role: &str, args: &[String]) -> Result<(), BoxErr> {
    match role {
        "sender" => sender(args),
        "receiver" => receiver(args),
        other => Err(format!("receiver-restart: unknown child role {other:?}").into()),
    }
}

/// Stream tagged items until killed. Runs in its own process precisely
/// because `send_item` can sit in RLC's flow-window wait once nothing is
/// draining, which is the state this scenario deliberately creates.
fn sender(args: &[String]) -> Result<(), BoxErr> {
    let port = arg_u64(args, 0, "receiver port")? as u16;
    let peer = format!("127.0.0.1:{port}")
        .parse()
        .map_err(|e| format!("parse peer: {e}"))?;
    let mut tx = UnifiedSensSender::connect("127.0.0.1:0", peer, UnifiedConfig::new(SYMBOL_LEN))
        .map_err(|e| format!("connect sender: {e}"))?;
    println!("   sender: pid {} streaming, code {:?}", std::process::id(), tx.active_code());

    let mut i = 0usize;
    loop {
        let mut item = vec![TAG; 16];
        item[1..9].copy_from_slice(&(i as u64).to_le_bytes());
        tx.send_item(&item).ok();
        i += 1;
        sleep(SEND_GAP);
    }
}

/// Take `ITEMS_PER_RECEIVER` tagged items, report by creating the marker,
/// then keep draining so the sender is never starved by this end going
/// quiet after it has reported.
fn receiver(args: &[String]) -> Result<(), BoxErr> {
    let port = arg_u64(args, 0, "listen port")? as u16;
    let marker: PathBuf = arg_path(args, 1, "marker path")?.to_path_buf();

    let mut rx = UnifiedSensReceiver::bind(
        format!("127.0.0.1:{port}"),
        UnifiedConfig::new(SYMBOL_LEN),
    )
    .map_err(|e| format!("receiver bind on {port}: {e}"))?;
    println!("   receiver: pid {} bound on {port}", std::process::id());

    let mut seen = 0usize;
    let mut adopted = false;
    while seen < ITEMS_PER_RECEIVER {
        match rx.poll() {
            Ok(items) => {
                for item in items {
                    if item.first().copied() == Some(TAG) {
                        seen += 1;
                    }
                }
            }
            Err(e) => return Err(format!("receiver poll: {e}").into()),
        }
        adopted |= rx.take_session_changed();
        sleep(Duration::from_millis(2));
    }
    std::fs::write(&marker, seen.to_string())
        .map_err(|e| format!("receiver marker: {e}"))?;
    let (adoptions, unanswered) = rx.session_adoption_counts();
    println!(
        "   receiver: took {seen} items (session_changed {adopted}, adoptions {adoptions}, \
         unanswered {unanswered})"
    );

    loop {
        rx.poll().ok();
        sleep(Duration::from_millis(5));
    }
}
