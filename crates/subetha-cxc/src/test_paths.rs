//! Paths a test backs a primitive with, removed when the test ends.
//!
//! A guard declared before the primitive that maps its file drops after
//! it, so the removal runs against an unmapped file. A removal the OS
//! refuses is then a defect in the test - a mapping still alive - and
//! fails it; during an unwind it is reported instead, because a second
//! panic there would abort the run in place of the failure that started
//! it.

use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};

/// A file path under the temp dir, unique to this process.
pub(crate) struct TmpFile(PathBuf);

impl TmpFile {
    /// Whatever an earlier run left at `name` is removed first; a removal
    /// refused for any reason but absence fails the test rather than
    /// gating it on a stale file.
    pub(crate) fn new(name: impl AsRef<str>) -> Self {
        let p = std::env::temp_dir().join(name.as_ref());
        match std::fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => panic!("stale test file {} not removed: {e}", p.display()),
        }
        Self(p)
    }
}

impl AsRef<Path> for TmpFile {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

/// `PathBuf: From<&T>` needs this, for constructors that take
/// `impl Into<PathBuf>`.
impl AsRef<std::ffi::OsStr> for TmpFile {
    fn as_ref(&self) -> &std::ffi::OsStr {
        self.0.as_os_str()
    }
}

impl Deref for TmpFile {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpFile {
    fn drop(&mut self) {
        report(std::fs::remove_file(&self.0), "test file", &self.0);
    }
}

/// A removal that the test itself already performed reads as absent and
/// is fine; any other refusal fails the test, or is reported when the
/// test is already unwinding.
fn report(r: io::Result<()>, what: &str, p: &Path) {
    match r {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) if std::thread::panicking() => {
            eprintln!("{what} {} not removed: {e}", p.display());
        }
        Err(e) => panic!("{what} {} not removed, a mapping is still alive: {e}", p.display()),
    }
}
