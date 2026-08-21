//! `subetha-e2e` - the end-to-end gate.
//!
//! One binary holding every scenario that has to cross a process
//! boundary to mean anything. The parent half of a scenario spawns
//! this same executable through `std::env::current_exe()` as the
//! child half, so the boundary under test is a real one: two address
//! spaces, two page tables, communicating only through the mapped
//! file or the wire.
//!
//! ```text
//! subetha-e2e                     run every scenario
//! subetha-e2e run [name...]       run all, or only the named ones
//! subetha-e2e list                one line per scenario
//! subetha-e2e child <sc> <role>   the spawned half; not for direct use
//! ```
//!
//! Exit status is 0 only when every scenario asked for passed.

mod harness;
mod scenarios;

use std::process::ExitCode;
use std::time::Instant;

use harness::{BoxErr, Harness};
use scenarios::{Scenario, ALL};

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let result = match argv.first().map(String::as_str) {
        Some("list") => {
            list();
            Ok(())
        }
        Some("child") => run_child(&argv[1..]),
        Some("run") => run_parent(&argv[1..]),
        None => run_parent(&[]),
        Some(other) if other.starts_with('-') => {
            eprintln!("unknown flag {other:?}\n");
            list();
            Err("bad invocation".into())
        }
        // Bare scenario names, so `subetha-e2e failover` works.
        Some(_) => run_parent(&argv),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("subetha-e2e: {e}");
            ExitCode::FAILURE
        }
    }
}

fn list() {
    println!("scenarios ({}):", ALL.len());
    for s in ALL {
        println!("  {:<22} {}", s.name, s.about);
    }
}

fn find(name: &str) -> Result<&'static Scenario, BoxErr> {
    ALL.iter()
        .find(|s| s.name == name)
        .ok_or_else(|| format!("no scenario named {name:?} (try `subetha-e2e list`)").into())
}

/// The spawned half: `child <scenario> <role> [args...]`.
fn run_child(argv: &[String]) -> Result<(), BoxErr> {
    let name = argv
        .first()
        .ok_or("child needs a scenario name")?;
    let role = argv
        .get(1)
        .ok_or("child needs a role")?;
    let scenario = find(name)?;
    (scenario.child)(role, &argv[2..])
}

/// The driving half. Runs the named scenarios (or all of them), one
/// per line, and reports a matrix at the end.
fn run_parent(names: &[String]) -> Result<(), BoxErr> {
    let selected: Vec<&Scenario> = if names.is_empty() {
        ALL.iter().collect()
    } else {
        names.iter().map(|n| find(n)).collect::<Result<_, _>>()?
    };

    let mut failures = Vec::new();
    for s in &selected {
        print!("== {} ... ", s.name);
        // The child inherits stdout, so flush before it writes.
        use std::io::Write;
        std::io::stdout().flush().ok();
        println!();

        let started = Instant::now();
        let outcome = Harness::new(s.name).and_then(|h| {
            let r = (s.parent)(&h);
            h.cleanup();
            r
        });
        let ms = started.elapsed().as_millis();

        match outcome {
            Ok(()) => println!("== {} PASS ({ms} ms)\n", s.name),
            Err(e) => {
                println!("== {} FAIL ({ms} ms): {e}\n", s.name);
                failures.push((s.name, e.to_string()));
            }
        }
    }

    println!("---");
    println!(
        "{} scenario(s): {} passed, {} failed",
        selected.len(),
        selected.len() - failures.len(),
        failures.len()
    );
    if failures.is_empty() {
        return Ok(());
    }
    for (name, why) in &failures {
        println!("  FAIL {name}: {why}");
    }
    Err(format!("{} scenario(s) failed", failures.len()).into())
}
