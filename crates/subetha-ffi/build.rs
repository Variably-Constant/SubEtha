//! Link arguments for the shared library: a versioned soname on the ELF
//! platforms and an install name on macOS, so a consumer's binary records
//! the ABI's major version rather than a bare file name.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")
        .expect("cargo sets CARGO_CFG_TARGET_OS for every build script");
    match target_os.as_str() {
        "linux" | "freebsd" | "android" | "netbsd" | "openbsd" | "dragonfly" => {
            println!("cargo:rustc-cdylib-link-arg=-Wl,-soname,libsubetha_ffi.so.0");
        }
        "macos" | "ios" => {
            println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libsubetha_ffi.0.dylib");
        }
        _ => {}
    }
}
