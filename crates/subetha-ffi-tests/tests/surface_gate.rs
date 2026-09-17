//! Every function the library exports must be called by something that
//! goes through the C ABI.
//!
//! No lint can report an export nothing reaches: a `pub extern "C"`
//! function is part of the API whether or not anyone calls it, and its
//! caller is a C program the compiler cannot see.
//!
//! The corpus is the C test programs, this crate's Rust tests, the header
//! test beside the library, and the boundary bench. Reads only.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// This test, which is left out of its own corpus.
const GATE: &str = "surface_gate.rs";

/// The crate holding the exports, beside this one.
fn ffi_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("this crate sits under crates/")
        .join("subetha-ffi")
}

fn this_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The files directly in `dir` whose name ends with `ext`. A directory
/// that cannot be read fails the test naming itself: a corpus that
/// quietly lost a folder reports everything called only from there as
/// unreached.
fn files_with(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{} is readable: {e}", dir.display()));
    let mut out: Vec<PathBuf> = entries
        .map(|e| e.expect("the entry is readable").path())
        .filter(|p| p.to_string_lossy().ends_with(ext))
        .collect();
    out.sort();
    out
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{} is readable: {e}", path.display()))
}

/// `text` with its comments taken out, so a function named in prose next
/// to a parenthesis does not read as a call. String literals are stepped
/// over, since a `//` inside one opens no comment; a literal opened with
/// `r` takes no escape, so a trailing backslash does not swallow its
/// closing quote.
fn without_comments(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        let pair = if i + 1 < bytes.len() { &bytes[i..i + 2] } else { &bytes[i..i + 1] };
        if pair == b"//" {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if pair == b"/*" {
            i += 2;
            while i + 1 < bytes.len() && &bytes[i..i + 2] != b"*/" {
                if bytes[i] == b'\n' {
                    out.push('\n');
                }
                i += 1;
            }
            i = (i + 2).min(bytes.len());
        } else if bytes[i] == b'"' {
            let raw = i > 0 && (bytes[i - 1] == b'r' || bytes[i - 1] == b'#');
            out.push('"');
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                if !raw && bytes[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            i = (i + 1).min(bytes.len());
            out.push('"');
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

fn is_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Whether `corpus` calls `name`: a whole word with a `(` after it. A
/// `use` writes the name without one, so importing a function and never
/// calling it does not count as reaching it.
fn calls(corpus: &str, name: &str) -> bool {
    let bytes = corpus.as_bytes();
    let mut from = 0;
    while let Some(found) = corpus[from..].find(name) {
        let at = from + found;
        from = at + name.len();
        if at > 0 && is_name_byte(bytes[at - 1]) {
            continue;
        }
        let mut after = at + name.len();
        while after < bytes.len() && bytes[after].is_ascii_whitespace() {
            after += 1;
        }
        if after < bytes.len() && bytes[after] == b'(' {
            return true;
        }
    }
    false
}

/// Every function the library exports, read from its source.
fn exported(ffi: &Path) -> BTreeSet<String> {
    const MARK: &str = "extern \"C\" fn ";
    let mut names = BTreeSet::new();
    for file in files_with(&ffi.join("src"), ".rs") {
        for line in read(&file).lines() {
            let trimmed = line.trim_start();
            if !trimmed.starts_with("pub ") {
                continue;
            }
            let Some(at) = trimmed.find(MARK) else {
                continue;
            };
            let rest = &trimmed[at + MARK.len()..];
            let end =
                rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(rest.len());
            names.insert(rest[..end].to_string());
        }
    }
    names
}

#[test]
fn every_exported_function_is_called_through_the_abi() {
    let ffi = ffi_dir();
    let exports = exported(&ffi);

    // The list is read out of the source, so reading it wrongly fails
    // here rather than passing with nothing to check. A pattern missing
    // `pub unsafe extern "C"` counts this surface at 166 against 839.
    assert!(
        exports.len() > 800,
        "the exported functions are read from the library's source, and {} of them is too few to \
         be the whole surface",
        exports.len()
    );

    let mut corpus = String::new();
    for (dir, ext) in [
        (this_dir().join("c"), ".c"),
        (this_dir().join("src"), ".rs"),
        (this_dir().join("tests"), ".rs"),
        (ffi.join("tests"), ".rs"),
        (ffi.join("benches"), ".rs"),
    ] {
        for file in files_with(&dir, ext) {
            if file.file_name() == Some(OsStr::new(GATE)) {
                continue;
            }
            corpus.push_str(&without_comments(&read(&file)));
            corpus.push('\n');
        }
    }

    let unreached: Vec<&str> =
        exports.iter().filter(|name| !calls(&corpus, name)).map(String::as_str).collect();
    assert!(
        unreached.is_empty(),
        "{} of the {} exported functions are called by no test, no bench and no C program, so \
         nothing would notice if they stopped working:\n    {}",
        unreached.len(),
        exports.len(),
        unreached.join("\n    ")
    );
}

#[test]
fn a_name_only_imported_does_not_count_as_reached() {
    // The matcher's edges: a name is reached by a call and not by a
    // mention, and is itself rather than the start of a longer one.
    assert!(calls("let rc = subetha_ring_flush(handle);", "subetha_ring_flush"));
    assert!(calls(
        "use subetha_ffi::{subetha_ring_flush};\nsubetha_ring_flush (h);",
        "subetha_ring_flush"
    ));
    assert!(!calls(
        "use subetha_ffi::{subetha_ring_flush, subetha_ring_open};",
        "subetha_ring_flush"
    ));
    assert!(!calls("subetha_ring_flush_async(handle);", "subetha_ring_flush"));
    assert!(!calls("nothing_here(handle);", "subetha_ring_flush"));
}

#[test]
fn a_function_named_in_a_comment_does_not_count_as_reached() {
    let text = "// subetha_ring_flush(handle) is what a writer calls.\n\
                /* subetha_ring_open(p) too. */\n\
                let path = \"a // b\";\n";
    let stripped = without_comments(text);
    assert!(!calls(&stripped, "subetha_ring_flush"), "a line comment is not a call");
    assert!(!calls(&stripped, "subetha_ring_open"), "a block comment is not a call");
    assert!(stripped.contains("let path"), "the code around the comments is kept");
}
