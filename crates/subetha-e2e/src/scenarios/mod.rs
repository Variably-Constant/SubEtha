//! Scenario registry.
//!
//! A scenario is two function pointers over one boundary: `parent`
//! drives and asserts, `child` runs in the spawned process and exits
//! non-zero when its half of the claim fails. Both live in this
//! binary, so adding a scenario costs one entry in [`ALL`] and no new
//! compile target.

use crate::harness::{BoxErr, Harness};

pub mod failover;
pub mod flush_visibility;
pub mod receiver_restart;
pub mod ring_boundary;
pub mod scheduler;
pub mod session_restart;
pub mod session_restart_rs;

/// One end-to-end claim, in its parent and child halves.
pub struct Scenario {
    /// Selector on the command line.
    pub name: &'static str,
    /// One line for `subetha-e2e list`.
    pub about: &'static str,
    /// Runs in the driving process; spawns children through the harness.
    pub parent: fn(&Harness) -> Result<(), BoxErr>,
    /// Runs in a spawned process, dispatched on the role name.
    pub child: fn(&str, &[String]) -> Result<(), BoxErr>,
}

pub static ALL: &[Scenario] = &[
    Scenario {
        name: "failover",
        about: "a KILLED process's in-flight work is reclaimed by the watchdog",
        parent: failover::parent,
        child: failover::child,
    },
    Scenario {
        name: "ring-boundary",
        about: "ring payloads cross a real boundary and survive process death",
        parent: ring_boundary::parent,
        child: ring_boundary::child,
    },
    Scenario {
        name: "flush-visibility",
        about: "every primitive's flush_async state is readable from a second process",
        parent: flush_visibility::parent,
        child: flush_visibility::child,
    },
    Scenario {
        name: "session-restart",
        about: "a KILLED peer's replacement session is delivered, not discarded",
        parent: session_restart::parent,
        child: session_restart::child,
    },
    Scenario {
        name: "session-restart-rs",
        about: "the same restart pinned to block-RS, the code with no session id of its own",
        parent: session_restart_rs::parent,
        child: session_restart_rs::child,
    },
    Scenario {
        name: "receiver-restart",
        about: "the mirror: a replacement RECEIVER joins a stream already in progress",
        parent: receiver_restart::parent,
        child: receiver_restart::child,
    },
    Scenario {
        name: "scheduler",
        about: "a Pass submitted here is executed by a worker process and collected here",
        parent: scheduler::parent,
        child: scheduler::child,
    },
];
