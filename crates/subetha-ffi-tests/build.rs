//! Compiles the C test suite against the committed header with the host's
//! own C compiler, into a static library the Rust tests call. Every C
//! warning is an error, the same bar the Rust side holds.

fn main() {
    println!("cargo:rerun-if-changed=c/ctests.c");
    println!("cargo:rerun-if-changed=c/workloads.c");
    println!("cargo:rerun-if-changed=../subetha-ffi/include/subetha.h");
    cc::Build::new()
        .file("c/ctests.c")
        .file("c/workloads.c")
        .include("../subetha-ffi/include")
        .define("SUBETHA_TEST_HOOKS", None)
        .warnings(true)
        .extra_warnings(true)
        .warnings_into_errors(true)
        .compile("subetha_ctests");
}
