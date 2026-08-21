//! Process-spawning harness shared by every scenario.
//!
//! A scenario runs in two halves that live in different address
//! spaces. The parent half receives a [`Harness`] and uses it to
//! launch the child half; the child half is this same executable,
//! re-entered through the `child` subcommand, so both sides are
//! compiled from one source and shipped as one artifact.
//!
//! Paths handed out by [`Harness::path`] are unique per run (they
//! carry the parent pid), so concurrent runs of the same scenario on
//! one host do not collide.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

/// Boxed error every scenario half returns.
pub type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Per-run scratch directory plus the means to spawn the child half.
pub struct Harness {
    exe: PathBuf,
    scenario: &'static str,
    dir: PathBuf,
}

impl Harness {
    /// Build a harness for `scenario`, creating its scratch directory.
    pub fn new(scenario: &'static str) -> Result<Self, BoxErr> {
        let exe = std::env::current_exe()?;
        let mut dir = std::env::temp_dir();
        dir.push(format!("subetha-e2e-{scenario}-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        Ok(Self { exe, scenario, dir })
    }

    /// A path inside this run's scratch directory.
    pub fn path(&self, stem: &str) -> PathBuf {
        self.dir.join(stem)
    }

    /// This run's scratch directory, for handing to a child that opens
    /// many files by stem.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Launch the child half in `role`, inheriting stdout and stderr so
    /// its diagnostics interleave with the parent's.
    pub fn spawn<S: AsRef<str>>(&self, role: &str, args: &[S]) -> Result<Child, BoxErr> {
        let mut cmd = Command::new(&self.exe);
        cmd.arg("child").arg(self.scenario).arg(role);
        for a in args {
            cmd.arg(OsString::from(a.as_ref()));
        }
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        Ok(cmd.spawn()?)
    }

    /// Launch the child half and require it to exit zero.
    pub fn run<S: AsRef<str>>(&self, role: &str, args: &[S]) -> Result<(), BoxErr> {
        let status = self.spawn(role, args)?.wait()?;
        if status.success() {
            return Ok(());
        }
        Err(format!(
            "child role '{role}' exited {}",
            status.code().map_or_else(|| "by signal".to_string(), |c| c.to_string())
        )
        .into())
    }

    /// Remove the scratch directory and everything under it.
    pub fn cleanup(&self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

/// `Err` carrying `msg` unless `cond` holds. The scenario bodies read
/// as a list of claims rather than a ladder of `if` blocks.
pub fn require(cond: bool, msg: impl Into<String>) -> Result<(), BoxErr> {
    if cond { Ok(()) } else { Err(msg.into().into()) }
}

/// Parse a child argument as `u64`, naming the position when it is
/// missing or malformed.
pub fn arg_u64(args: &[String], idx: usize, what: &str) -> Result<u64, BoxErr> {
    let raw = args
        .get(idx)
        .ok_or_else(|| format!("missing argument {idx} ({what})"))?;
    raw.parse::<u64>()
        .map_err(|e| format!("argument {idx} ({what}) = {raw:?}: {e}").into())
}

/// Parse a child argument as a path.
pub fn arg_path<'a>(args: &'a [String], idx: usize, what: &str) -> Result<&'a Path, BoxErr> {
    let raw = args
        .get(idx)
        .ok_or_else(|| format!("missing argument {idx} ({what})"))?;
    Ok(Path::new(raw))
}
