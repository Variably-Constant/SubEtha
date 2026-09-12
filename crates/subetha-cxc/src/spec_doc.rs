//! The wire specification's packet-type table, read back from the
//! document so the modules that own those numbers can assert against it.
//!
//! The interoperability vectors pin the coefficient and matrix layers,
//! and nothing pinned the frame layer: the type table is written by hand
//! and the constants are not. A second implementation is told to append
//! when it adds a type, so a number missing from the table is a number
//! it appends onto.
//!
//! Each module owning packet-type constants asserts them here, which
//! keeps every constant checked by the module that can see it without
//! widening any visibility for a test.
//!
//! The parse is strict. A row it cannot read is a failure, never a row
//! it skips: a lenient parser reports a table smaller than the document
//! states, which is indistinguishable from a document that is correct.

use std::collections::BTreeMap;
use std::path::PathBuf;

/// Where the specification lives, relative to this crate.
fn spec_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("SENS_O_MATIC_WIRE.md")
}

/// The line preceding the packet-type table. Anchoring on it rather than
/// on a position keeps this from reading a different table: section 5.3
/// also tabulates `0x01` upward, for control frames.
const ANCHOR: &str = "This is the complete assignment";

/// The packet-type table's header, recognized so it is not mistaken for a
/// row that failed to parse.
const HEADER: [&str; 3] = ["Type", "Meaning", "Code"];

/// The line preceding the control-plane frame-type table in section 5.3.
const FRAME_ANCHOR: &str = "Frame types:";

/// That table's header.
const FRAME_HEADER: [&str; 3] = ["Tag", "Name", "Meaning"];

/// Split a markdown table line into trimmed cells.
fn cells(line: &str) -> Vec<&str> {
    line.trim().trim_matches('|').split('|').map(str::trim).collect()
}

/// True for the `|---|---|---|` rule under a table header.
fn is_rule(row: &[&str]) -> bool {
    row.iter().all(|c| !c.is_empty() && c.chars().all(|ch| ch == '-' || ch == ':'))
}

/// The packet-type table as the specification states it: type byte to the
/// meaning column.
pub(crate) fn packet_types() -> BTreeMap<u8, String> {
    table_after(ANCHOR, &HEADER)
}

/// The control-plane frame-type table of section 5.3: tag to name.
pub(crate) fn control_frame_types() -> BTreeMap<u8, String> {
    table_after(FRAME_ANCHOR, &FRAME_HEADER)
}

/// The first three-column table following `anchor`, keyed on its leading
/// `` `0xNN` `` cell.
fn table_after(anchor: &str, header: &[&str; 3]) -> BTreeMap<u8, String> {
    let path = spec_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("the specification is readable at {}: {e}", path.display()));

    let mut lines = text.lines();
    lines
        .by_ref()
        .find(|l| l.contains(anchor))
        .unwrap_or_else(|| panic!("{} no longer contains {anchor:?}", path.display()));

    let mut found: BTreeMap<u8, String> = BTreeMap::new();
    for line in lines {
        if !line.trim().starts_with('|') {
            if found.is_empty() {
                // Prose between the anchor and the table.
                continue;
            }
            break;
        }
        let row = cells(line);
        assert_eq!(
            row.len(),
            3,
            "{}: a row under {anchor:?} has {} cells, expected 3: {line:?}",
            path.display(),
            row.len()
        );
        if row[..] == header[..] || is_rule(&row) {
            continue;
        }
        let hex = row[0]
            .strip_prefix("`0x")
            .and_then(|s| s.strip_suffix('`'))
            .unwrap_or_else(|| {
                panic!(
                    "{}: a row under {anchor:?} has {:?} as its first cell, which is not \
                     a `0xNN` code. Every row of this table names a byte.",
                    path.display(),
                    row[0]
                )
            });
        let byte = u8::from_str_radix(hex, 16).unwrap_or_else(|e| {
            panic!("{}: {:?} is not a hex byte: {e}", path.display(), row[0])
        });
        if let Some(prior) = found.insert(byte, row[1].to_owned()) {
            panic!("{}: 0x{byte:02X} is listed twice, as {prior:?} and {:?}",
                   path.display(), row[1]);
        }
    }
    assert!(!found.is_empty(), "no rows under {anchor:?} in {}", path.display());
    found
}

/// Assert every `(constant, name)` pair appears in the specification's
/// packet-type table. `owner` names the caller in the failure.
pub(crate) fn assert_listed(owner: &str, constants: &[(u8, &str)]) {
    let listed = packet_types();
    let missing: Vec<String> = constants
        .iter()
        .filter(|(byte, _)| !listed.contains_key(byte))
        .map(|(byte, name)| format!("0x{byte:02X} ({name})"))
        .collect();
    assert!(
        missing.is_empty(),
        "{owner}: the wire specification's packet-type table does not list {}. \
         An implementation reading that table is told to append when it adds a \
         type, so an unlisted number is one it appends onto. Add the row to \
         SENS_O_MATIC_WIRE.md section 2.",
        missing.join(", ")
    );
}
