//! Compile the independent C implementation into a static library the
//! tests link.
//!
//! Warnings are errors here. This code exists to be read against a
//! specification, and a warning in it is either a mistake or a thing
//! the specification left unclear enough that the compiler noticed.

fn main() {
    let mut build = cc::Build::new();
    build
        .file("c/sens_field.c")
        .file("c/sens_rlc.c")
        .file("c/sens_rs.c")
        .include("c");

    if build.get_compiler().is_like_msvc() {
        build.flag("/W4").flag("/WX");
    } else {
        build.flag("-Wall").flag("-Wextra").flag("-Werror");
    }

    build.compile("sens_rlc");

    for f in [
        "c/sens_field.c",
        "c/sens_field.h",
        "c/sens_rlc.c",
        "c/sens_rlc.h",
        "c/sens_rs.c",
        "c/sens_rs.h",
    ] {
        println!("cargo:rerun-if-changed={f}");
    }
}
