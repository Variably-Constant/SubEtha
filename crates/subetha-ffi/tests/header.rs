//! The committed header is the one cbindgen generates from this crate, and
//! the committed export definition lists the functions that header
//! declares. These tests regenerate both and fail on any difference,
//! naming the first line that differs; with `SUBETHA_REGENERATE_HEADER=1`
//! they write the generated text over the committed files instead, which
//! is how the files are updated.

use std::path::{Path, PathBuf};

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn generate() -> String {
    let config = cbindgen::Config::from_file(crate_dir().join("cbindgen.toml"))
        .expect("cbindgen.toml parses");
    let bindings = cbindgen::Builder::new()
        .with_crate(crate_dir())
        .with_config(config)
        .generate()
        .expect("cbindgen generates the header");
    let mut out = Vec::new();
    bindings.write(&mut out);
    String::from_utf8(out).expect("the header is UTF-8")
}

fn normalize(s: &str) -> String {
    s.replace("\r\n", "\n")
}

/// The names the shared library exports: every function the header
/// declares outside its test-hook blocks, sorted, as a module definition
/// file for the Windows linkers.
fn export_definition(header: &str) -> String {
    let mut names = Vec::new();
    let mut in_test_hooks = false;
    for line in header.lines() {
        if line.starts_with("#if defined(SUBETHA_TEST_HOOKS)") {
            in_test_hooks = true;
            continue;
        }
        if line.starts_with("#endif") {
            in_test_hooks = false;
            continue;
        }
        // A declaration starts at column 0 with its return type; field,
        // comment, directive and typedef lines never do.
        if in_test_hooks
            || line.starts_with(|c: char| "# */}".contains(c))
            || line.starts_with("typedef")
        {
            continue;
        }
        let Some(open) = line.find('(') else {
            continue;
        };
        let head = &line[..open];
        let name_start = head
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .map_or(0, |i| i + 1);
        let name = &head[name_start..];
        if name.starts_with("subetha_") {
            names.push(name.to_string());
        }
    }
    names.sort();
    let mut out = String::from("LIBRARY subetha_ffi\nEXPORTS\n");
    for name in &names {
        out.push_str("    ");
        out.push_str(name);
        out.push('\n');
    }
    out
}

fn check_or_write(path: &Path, generated: &str) {
    if std::env::var_os("SUBETHA_REGENERATE_HEADER").is_some() {
        std::fs::create_dir_all(path.parent().expect("the file has a parent directory"))
            .expect("the parent directory can be created");
        std::fs::write(path, generated).expect("the file can be written");
        return;
    }
    let committed = match std::fs::read_to_string(path) {
        Ok(s) => normalize(&s),
        Err(e) => panic!(
            "{} cannot be read ({e}); run with SUBETHA_REGENERATE_HEADER=1 to write it",
            path.display()
        ),
    };
    if committed != generated {
        let first = committed
            .lines()
            .zip(generated.lines())
            .position(|(a, b)| a != b)
            .map(|i| i + 1)
            .unwrap_or_else(|| committed.lines().count().min(generated.lines().count()) + 1);
        panic!(
            "{} has drifted from the Rust it describes; first difference at line {first}. \
             Run with SUBETHA_REGENERATE_HEADER=1 to rewrite it and commit the result.",
            path.display()
        );
    }
}

#[test]
fn the_committed_header_is_the_generated_one() {
    let generated = normalize(&generate());
    check_or_write(&crate_dir().join("include").join("subetha.h"), &generated);
}

#[test]
fn the_committed_export_definition_lists_the_header_functions() {
    let generated = export_definition(&normalize(&generate()));
    check_or_write(&crate_dir().join("subetha.def"), &generated);
}

/// The tier number and shipped column of every row in the tiers table.
/// A row is a tier when its first cell is a number; the header row and
/// the separator are how a table says the rest of itself.
fn tier_rows(document: &str) -> Vec<(u32, String)> {
    let mut rows = Vec::new();
    for line in document.lines() {
        let line = line.trim();
        if !line.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        if cells.len() != 3 {
            continue;
        }
        if cells[0].is_empty() || !cells[0].chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let tier: u32 = cells[0].parse().expect("a cell of ascii digits parses");
        rows.push((tier, cells[2].to_owned()));
    }
    rows
}

/// `subetha_abi_version`'s minor is the highest shipped tier plus one,
/// so a consumer reads it to learn which families it is linked against.
///
/// The version fell four tiers behind before this test existed: it was
/// set at tier 0 and never moved while tiers 1, 2 and 3 shipped, so the
/// query answered the same number for a library with the ring alone and
/// one with every family. Nothing else in the suite notices that, because
/// nothing else reads the two together.
#[test]
fn abi_version_matches_the_tiers_document() {
    let path = crate_dir().join("..").join("..").join("C_ABI_TIERS.md");
    let document = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => panic!("{} cannot be read ({e}); it is the tier list this version tracks", path.display()),
    };
    let rows = tier_rows(&document);
    assert!(rows.len() >= 4, "the tiers table has {} rows, which is not the document", rows.len());

    // The shipped tiers are a prefix: a later tier marked shipped over an
    // earlier one that is not means the table disagrees with itself, and
    // then "the highest shipped tier" names nothing.
    let shipped: Vec<u32> = rows.iter().filter(|(_, s)| s == "yes").map(|(t, _)| *t).collect();
    for (i, tier) in shipped.iter().enumerate() {
        assert_eq!(
            *tier, i as u32,
            "tier {tier} is marked shipped while an earlier tier is not; the tiers ship in order",
        );
    }
    let highest = shipped.last().copied().expect("at least tier 0 has shipped");

    assert_eq!(
        subetha_ffi::SUBETHA_ABI_VERSION_MINOR,
        highest + 1,
        "C_ABI_TIERS.md has tier {highest} shipped, so the ABI minor is {}, not {}. \
         Move SUBETHA_ABI_VERSION_MINOR and the string beside it in runtime.rs.",
        highest + 1,
        subetha_ffi::SUBETHA_ABI_VERSION_MINOR,
    );

    // The freeze is gated on use as well as on shipping, so the major
    // does not follow from the table and is not asserted against it.
    assert_eq!(
        subetha_ffi::SUBETHA_ABI_VERSION_MAJOR,
        0,
        "the major reached 1, so the header, the codes, the handle encoding and every \
         shipped signature are frozen; this test is where that decision is recorded",
    );
}

#[test]
fn the_export_definition_skips_the_test_hooks_and_non_functions() {
    let header = "typedef uint64_t subetha_handle;\n\
                  #define SUBETHA_OK 0\n\
                  typedef struct subetha_ring_options {\n\
                  \x20 uint32_t mode;\n\
                  } subetha_ring_options;\n\
                  int32_t subetha_init(uint32_t default_mode);\n\
                  const char *subetha_strerror(int32_t code);\n\
                  int32_t subetha_ring_create(const char *path_prefix,\n\
                  \x20                          subetha_handle *out);\n\
                  #if defined(SUBETHA_TEST_HOOKS)\n\
                  int32_t subetha_test_panic_free(void);\n\
                  #endif\n";
    assert_eq!(
        export_definition(header),
        "LIBRARY subetha_ffi\nEXPORTS\n    subetha_init\n    subetha_ring_create\n    subetha_strerror\n"
    );
}
