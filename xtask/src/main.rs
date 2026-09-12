//! Workspace tasks that cargo alone does not cover: the C ABI's install
//! layout, and the packaging gate that proves a C consumer can find and
//! link it.
//!
//! ```text
//! cargo run -p xtask -- ffi-install --prefix <dir> [--debug]
//! cargo run -p xtask -- ffi-package-gate
//! ```
//!
//! `ffi-install` builds `subetha-ffi` and lays out `include/`, `lib/`,
//! `bin/` on Windows, `lib/pkgconfig/subetha.pc` and `lib/cmake/subetha/`
//! under the prefix. `ffi-package-gate` installs into `target/ffi-prefix`,
//! checks the pkg-config file where `pkg-config` is on the path, builds and
//! runs the CMake consumer example against the prefix where `cmake` is, and
//! compiles the same consumer directly with the host's C compiler, shared
//! and static, on every host that has one. A tool that is absent is
//! reported as SKIPPED on stdout, never silently.
//!
//! The crate's one dependency, `cc`, is the one the C suite's build script
//! already needs, so the task builds on every gate host as it is.

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const USAGE: &str = "usage: cargo run -p xtask -- ffi-install --prefix <dir> [--debug]\n       cargo run -p xtask -- ffi-package-gate";

/// The line a consumer's run prints when the ABI worked end to end.
const CONSUMER_OK: &str = "subetha consumer: ok";

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let outcome = match args.first().map(String::as_str) {
        Some("ffi-install") => ffi_install_command(&args[1..]),
        Some("ffi-package-gate") => ffi_package_gate(&args[1..]),
        _ => Err(USAGE.to_string()),
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(why) => {
            eprintln!("xtask: {why}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Copy)]
enum Profile {
    Release,
    Debug,
}

impl Profile {
    fn dir_name(self) -> &'static str {
        match self {
            Profile::Release => "release",
            Profile::Debug => "debug",
        }
    }
}

/// Where the installed tree keeps what a consumer needs at run time.
struct Installed {
    /// The directory holding the shared library a program loads: `bin/`
    /// on Windows, `lib/` elsewhere.
    runtime_dir: PathBuf,
    /// The file beside the built artifacts holding the system libraries a
    /// static link needs, as rustc reported them.
    native_libs: PathBuf,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the xtask crate sits one level below the workspace root")
        .to_path_buf()
}

fn target_dir(root: &Path) -> PathBuf {
    match env::var_os("CARGO_TARGET_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => root.join("target"),
    }
}

fn ffi_install_command(args: &[String]) -> Result<(), String> {
    let mut prefix = None;
    let mut profile = Profile::Release;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--prefix" => {
                let dir = it.next().ok_or("--prefix needs a directory")?;
                prefix = Some(PathBuf::from(dir));
            }
            "--debug" => profile = Profile::Debug,
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    let prefix = prefix.ok_or(format!("--prefix is required\n{USAGE}"))?;
    let root = workspace_root();
    ffi_install(&root, &absolute(&prefix)?, profile).map(|_| ())
}

/// Builds `subetha-ffi` at `profile` and lays it out under `prefix`.
fn ffi_install(root: &Path, prefix: &Path, profile: Profile) -> Result<Installed, String> {
    let native_libs = build_and_read_native_libs(root, profile)?;
    let version = crate_version(root)?;
    let built = target_dir(root).join(profile.dir_name());
    let ffi_crate = root.join("crates").join("subetha-ffi");

    let include = prefix.join("include");
    let lib = prefix.join("lib");
    make_dir(&include)?;
    make_dir(&lib)?;
    install(&ffi_crate.join("include").join("subetha.h"), &include.join("subetha.h"))?;

    let layout = if cfg!(windows) {
        let bin = prefix.join("bin");
        make_dir(&bin)?;
        install(&built.join("subetha_ffi.dll"), &bin.join("subetha_ffi.dll"))?;
        // The import library takes the plain name, the static library a
        // suffixed one, so `-lsubetha_ffi` and `subetha_ffi.lib` both mean
        // the shared build.
        install(&built.join("subetha_ffi.dll.lib"), &lib.join("subetha_ffi.lib"))?;
        install(&built.join("subetha_ffi.lib"), &lib.join("subetha_ffi_static.lib"))?;
        install(&ffi_crate.join("subetha.def"), &lib.join("subetha.def"))?;
        Layout {
            runtime_dir: bin,
            shared: "bin/subetha_ffi.dll".to_string(),
            implib: Some("lib/subetha_ffi.lib".to_string()),
            soname: None,
            static_lib: "lib/subetha_ffi_static.lib".to_string(),
        }
    } else if cfg!(target_os = "macos") {
        install(&built.join("libsubetha_ffi.dylib"), &lib.join("libsubetha_ffi.0.dylib"))?;
        link_or_copy(&lib, "libsubetha_ffi.0.dylib", "libsubetha_ffi.dylib")?;
        install(&built.join("libsubetha_ffi.a"), &lib.join("libsubetha_ffi.a"))?;
        Layout {
            runtime_dir: lib.clone(),
            shared: "lib/libsubetha_ffi.0.dylib".to_string(),
            implib: None,
            soname: Some("@rpath/libsubetha_ffi.0.dylib"),
            static_lib: "lib/libsubetha_ffi.a".to_string(),
        }
    } else {
        install(&built.join("libsubetha_ffi.so"), &lib.join("libsubetha_ffi.so.0"))?;
        link_or_copy(&lib, "libsubetha_ffi.so.0", "libsubetha_ffi.so")?;
        install(&built.join("libsubetha_ffi.a"), &lib.join("libsubetha_ffi.a"))?;
        Layout {
            runtime_dir: lib.clone(),
            shared: "lib/libsubetha_ffi.so.0".to_string(),
            implib: None,
            soname: Some("libsubetha_ffi.so.0"),
            static_lib: "lib/libsubetha_ffi.a".to_string(),
        }
    };

    let search_dirs = link_search_dirs(&built)?;
    let libs = resolve_native_libs(&native_libs, &search_dirs, &lib)?;

    let pkgconfig = lib.join("pkgconfig");
    make_dir(&pkgconfig)?;
    write_file(
        &pkgconfig.join("subetha.pc"),
        &pkg_config_text(prefix, &version, &libs),
    )?;

    let cmake = lib.join("cmake").join("subetha");
    make_dir(&cmake)?;
    write_file(
        &cmake.join("subethaConfig.cmake"),
        &cmake_config_text(&layout, &version, &libs),
    )?;
    write_file(
        &cmake.join("subethaConfigVersion.cmake"),
        &cmake_config_version_text(&version)?,
    )?;

    Ok(Installed {
        runtime_dir: layout.runtime_dir,
        native_libs: native_libs_cache(root, profile),
    })
}

/// Where the system-library list rustc printed is kept between runs.
fn native_libs_cache(root: &Path, profile: Profile) -> PathBuf {
    target_dir(root)
        .join(profile.dir_name())
        .join("subetha_ffi.native-static-libs")
}

/// The per-platform file names inside the prefix, as the CMake config
/// states them.
struct Layout {
    runtime_dir: PathBuf,
    /// Prefix-relative path of the shared library a program loads.
    shared: String,
    /// Prefix-relative path of the import library, on Windows.
    implib: Option<String>,
    /// The soname or install name the shared library carries.
    soname: Option<&'static str>,
    /// Prefix-relative path of the static library.
    static_lib: String,
}

/// Builds the crate and returns the system libraries a static link of it
/// needs, as rustc reports them.
///
/// rustc prints the list only when it runs; cargo folds the `--print`
/// flag into the crate's fingerprint, so a second call with an unchanged
/// tree is fresh and prints nothing. The list is therefore kept beside
/// the artifacts and read back on a fresh build.
fn build_and_read_native_libs(root: &Path, profile: Profile) -> Result<String, String> {
    let mut cmd = Command::new("cargo");
    cmd.current_dir(root).args(["rustc", "-p", "subetha-ffi", "--lib"]);
    if let Profile::Release = profile {
        cmd.arg("--release");
    }
    cmd.args(["--", "--print", "native-static-libs"]);
    let out = cmd
        .output()
        .map_err(|e| format!("cargo could not be run: {e}"))?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        return Err(format!("cargo rustc failed ({}):\n{stderr}", out.status));
    }
    let cache = native_libs_cache(root, profile);
    let printed = stderr
        .lines()
        .find_map(|line| line.split("native-static-libs:").nth(1))
        .map(|libs| libs.trim().to_string());
    match printed {
        Some(libs) => {
            write_file(&cache, &libs)?;
            Ok(libs)
        }
        None => fs::read_to_string(&cache).map_err(|e| {
            format!(
                "rustc printed no native-static-libs line and {} cannot be read ({e}); \
                 the static library's system libraries are unknown",
                cache.display()
            )
        }),
    }
}

fn crate_version(root: &Path) -> Result<String, String> {
    let out = Command::new("cargo")
        .current_dir(root)
        .args(["pkgid", "-p", "subetha-ffi"])
        .output()
        .map_err(|e| format!("cargo could not be run: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "cargo pkgid failed ({}):\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    // path+file:///.../crates/subetha-ffi#<version>, or ...#subetha-ffi@<version>
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let after_hash = id
        .rsplit('#')
        .next()
        .ok_or_else(|| format!("cargo pkgid printed no version: {id}"))?;
    let version = after_hash
        .rsplit('@')
        .next()
        .ok_or_else(|| format!("cargo pkgid printed no version: {id}"))?;
    if version.is_empty() || !version.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(format!("cargo pkgid printed no version: {id}"));
    }
    Ok(version.to_string())
}

/// One entry of the list rustc prints for a static link: a system library
/// named as rustc named it, or a library a crate's build script shipped,
/// which the install copies into `lib/` under `bundled`'s file name.
struct NativeLib {
    token: String,
    bundled: Option<String>,
}

impl NativeLib {
    /// The flag a pkg-config consumer passes: a shipped file by its stem,
    /// an MSVC system library by its stem, a `-l` flag as it is.
    fn pc_flag(&self) -> String {
        match &self.bundled {
            Some(file) => format!("-l{}", stem(file)),
            None => match self.token.strip_suffix(".lib") {
                Some(name) => format!("-l{name}"),
                None => self.token.clone(),
            },
        }
    }

    /// The item the CMake static target links: a shipped file by its full
    /// path under the prefix, a system library as rustc named it.
    fn cmake_item(&self) -> String {
        match &self.bundled {
            Some(file) => format!("${{SUBETHA_PREFIX}}/lib/{file}"),
            None => self.token.clone(),
        }
    }

    /// Whether this entry is a linker option rather than a library.
    ///
    /// rustc names MSVC's runtime choice as `/defaultlib:msvcrt`, which
    /// is an option the linker reads and not a file it opens. CMake
    /// takes anything in `INTERFACE_LINK_LIBRARIES` for a library name
    /// and appends an object suffix, so an option landing there is
    /// asked for as `\defaultlib:msvcrt.obj` and the link fails on a
    /// file that was never meant to exist.
    ///
    /// An absolute Unix path also starts with a slash, so the colon is
    /// what separates the two: `/defaultlib:msvcrt` carries one and
    /// `/usr/lib/libfoo.a` does not.
    fn is_linker_option(&self) -> bool {
        self.bundled.is_none()
            && self.token.starts_with('/')
            && self.token.contains(':')
    }
}

/// `libfoo.a` and `foo.lib` are both `foo`.
fn stem(file: &str) -> &str {
    let without_extension = file
        .strip_suffix(".lib")
        .or_else(|| file.strip_suffix(".a"))
        .unwrap_or(file);
    without_extension
        .strip_prefix("lib")
        .unwrap_or(without_extension)
}

/// The directories build scripts added to the link search, read from the
/// `output` file cargo keeps in each script's build directory.
fn link_search_dirs(built: &Path) -> Result<Vec<PathBuf>, String> {
    let build = built.join("build");
    let mut dirs = Vec::new();
    let entries = match fs::read_dir(&build) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(dirs),
        Err(e) => return Err(format!("{} cannot be listed: {e}", build.display())),
    };
    for entry in entries {
        let entry = entry.map_err(|e| format!("{} cannot be listed: {e}", build.display()))?;
        let output = entry.path().join("output");
        let text = match fs::read_to_string(&output) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("{} cannot be read: {e}", output.display())),
        };
        for line in text.lines() {
            let Some(value) = line
                .strip_prefix("cargo:rustc-link-search=")
                .or_else(|| line.strip_prefix("cargo::rustc-link-search="))
            else {
                continue;
            };
            let path = match value.split_once('=') {
                Some(("native" | "all" | "crate" | "dependency" | "framework", path)) => path,
                _ => value,
            };
            dirs.push(PathBuf::from(path));
        }
    }
    Ok(dirs)
}

/// Resolves rustc's list: a name found as a file in one of the build
/// scripts' link-search directories is copied into `lib_dir` and marked
/// bundled; the rest are the system's.
fn resolve_native_libs(
    list: &str,
    dirs: &[PathBuf],
    lib_dir: &Path,
) -> Result<Vec<NativeLib>, String> {
    let mut libs = Vec::new();
    for token in list.split_whitespace() {
        let name = token.strip_prefix("-l").unwrap_or(token);
        let candidates: Vec<String> = if name.ends_with(".lib") || name.ends_with(".a") {
            vec![name.to_string()]
        } else {
            vec![format!("lib{name}.a"), format!("{name}.lib")]
        };
        let mut bundled = None;
        'search: for dir in dirs {
            for file in &candidates {
                let path = dir.join(file);
                if path.is_file() {
                    install(&path, &lib_dir.join(file))?;
                    bundled = Some(file.clone());
                    break 'search;
                }
            }
        }
        libs.push(NativeLib {
            token: token.to_string(),
            bundled,
        });
    }
    Ok(libs)
}

fn pkg_config_text(prefix: &Path, version: &str, libs: &[NativeLib]) -> String {
    let private = libs.iter().map(NativeLib::pc_flag).collect::<Vec<_>>().join(" ");
    format!(
        "prefix={}\n\
         exec_prefix=${{prefix}}\n\
         libdir=${{exec_prefix}}/lib\n\
         includedir=${{prefix}}/include\n\
         \n\
         Name: subetha\n\
         Description: SubEtha cross-process IPC, C ABI\n\
         URL: https://github.com/Variably-Constant/SubEtha\n\
         Version: {version}\n\
         Cflags: -I${{includedir}}\n\
         Libs: -L${{libdir}} -lsubetha_ffi\n\
         Libs.private: -L${{libdir}} {private}\n",
        forward_slashes(prefix),
    )
}

fn cmake_config_text(layout: &Layout, version: &str, libs: &[NativeLib]) -> String {
    let mut shared_props = format!(
        "    INTERFACE_INCLUDE_DIRECTORIES \"${{SUBETHA_PREFIX}}/include\"\n\
         \x20   IMPORTED_LOCATION \"${{SUBETHA_PREFIX}}/{}\"\n",
        layout.shared
    );
    if let Some(implib) = &layout.implib {
        shared_props.push_str(&format!(
            "    IMPORTED_IMPLIB \"${{SUBETHA_PREFIX}}/{implib}\"\n"
        ));
    }
    if let Some(soname) = layout.soname {
        shared_props.push_str(&format!("    IMPORTED_SONAME \"{soname}\"\n"));
    }
    let link_libs = libs
        .iter()
        .filter(|lib| !lib.is_linker_option())
        .map(NativeLib::cmake_item)
        .collect::<Vec<_>>()
        .join(";");
    let options = libs
        .iter()
        .filter(|lib| lib.is_linker_option())
        .map(NativeLib::cmake_item)
        .collect::<Vec<_>>()
        .join(";");
    let link_options = if options.is_empty() {
        String::new()
    } else {
        format!("\x20   INTERFACE_LINK_OPTIONS \"{options}\"\n")
    };
    format!(
        "# Written by `cargo xtask ffi-install`; describes the tree it sits in.\n\
         get_filename_component(SUBETHA_PREFIX \"${{CMAKE_CURRENT_LIST_DIR}}/../../..\" ABSOLUTE)\n\
         set(SUBETHA_VERSION \"{version}\")\n\
         set(SUBETHA_INCLUDE_DIRS \"${{SUBETHA_PREFIX}}/include\")\n\
         \n\
         if(NOT TARGET subetha::subetha)\n\
         \x20 add_library(subetha::subetha SHARED IMPORTED)\n\
         \x20 set_target_properties(subetha::subetha PROPERTIES\n\
         {shared_props}\
         \x20 )\n\
         \n\
         \x20 add_library(subetha::static STATIC IMPORTED)\n\
         \x20 set_target_properties(subetha::static PROPERTIES\n\
         \x20   INTERFACE_INCLUDE_DIRECTORIES \"${{SUBETHA_PREFIX}}/include\"\n\
         \x20   IMPORTED_LOCATION \"${{SUBETHA_PREFIX}}/{}\"\n\
         \x20   INTERFACE_LINK_LIBRARIES \"{link_libs}\"\n\
         {link_options}\
         \x20 )\n\
         endif()\n\
         \n\
         set(subetha_FOUND TRUE)\n",
        layout.static_lib
    )
}

fn cmake_config_version_text(version: &str) -> Result<String, String> {
    let major = version
        .split('.')
        .next()
        .ok_or_else(|| format!("version {version} has no major component"))?;
    Ok(format!(
        "# Written by `cargo xtask ffi-install`.\n\
         set(PACKAGE_VERSION \"{version}\")\n\
         set(PACKAGE_VERSION_COMPATIBLE FALSE)\n\
         set(PACKAGE_VERSION_EXACT FALSE)\n\
         if(NOT PACKAGE_FIND_VERSION)\n\
         \x20 set(PACKAGE_VERSION_COMPATIBLE TRUE)\n\
         elseif(PACKAGE_FIND_VERSION_MAJOR STREQUAL \"{major}\"\n\
         \x20      AND NOT PACKAGE_FIND_VERSION VERSION_GREATER PACKAGE_VERSION)\n\
         \x20 set(PACKAGE_VERSION_COMPATIBLE TRUE)\n\
         \x20 if(PACKAGE_FIND_VERSION VERSION_EQUAL PACKAGE_VERSION)\n\
         \x20   set(PACKAGE_VERSION_EXACT TRUE)\n\
         \x20 endif()\n\
         endif()\n"
    ))
}

fn ffi_package_gate(args: &[String]) -> Result<(), String> {
    if !args.is_empty() {
        return Err(format!("ffi-package-gate takes no arguments\n{USAGE}"));
    }
    let root = workspace_root();
    let prefix = target_dir(&root).join("ffi-prefix");
    remove_tree(&prefix)?;
    let installed = ffi_install(&root, &prefix, Profile::Release)?;
    println!("FFI-INSTALL: PASS {}", prefix.display());

    let mut failed = 0;
    match pkg_config_check(&prefix) {
        Ok(report) => println!("FFI-PKGCONFIG: {report}"),
        Err(why) => {
            failed += 1;
            println!("FFI-PKGCONFIG: FAIL {why}");
        }
    }
    match cmake_consumer_check(&root, &prefix, &installed) {
        Ok(report) => println!("FFI-PACKAGE: {report}"),
        Err(why) => {
            failed += 1;
            println!("FFI-PACKAGE: FAIL {why}");
        }
    }
    match direct_consumer_check(&root, &prefix, &installed) {
        Ok(report) => println!("FFI-DIRECT: {report}"),
        Err(why) => {
            failed += 1;
            println!("FFI-DIRECT: FAIL {why}");
        }
    }
    if failed == 0 {
        Ok(())
    } else {
        Err(format!("{failed} packaging check(s) failed"))
    }
}

fn pkg_config_check(prefix: &Path) -> Result<String, String> {
    let Some(pkg_config) = find_on_path("pkg-config") else {
        return Ok("SKIPPED pkg-config is not on PATH".to_string());
    };
    let flags = run(
        Command::new(pkg_config)
            .env("PKG_CONFIG_PATH", prefix.join("lib").join("pkgconfig"))
            .args(["--cflags", "--libs", "subetha"]),
        "pkg-config --cflags --libs subetha",
    )?;
    let flags = flags.trim().to_string();
    if !flags.contains("-lsubetha_ffi") {
        return Err(format!("pkg-config answered without -lsubetha_ffi: {flags}"));
    }
    Ok(format!("PASS {flags}"))
}

fn cmake_consumer_check(root: &Path, prefix: &Path, installed: &Installed) -> Result<String, String> {
    let Some(cmake) = find_on_path("cmake") else {
        return Ok("SKIPPED cmake is not on PATH".to_string());
    };
    let source = root.join("crates").join("subetha-ffi").join("cmake-consumer");
    let build = target_dir(root).join("ffi-consumer-build");
    remove_tree(&build)?;
    run(
        Command::new(&cmake)
            .arg("-S")
            .arg(&source)
            .arg("-B")
            .arg(&build)
            .arg("-DCMAKE_BUILD_TYPE=Release")
            .arg(format!("-DCMAKE_PREFIX_PATH={}", forward_slashes(prefix))),
        "cmake configure of the consumer example",
    )?;
    run(
        Command::new(&cmake)
            .arg("--build")
            .arg(&build)
            .args(["--config", "Release"]),
        "cmake build of the consumer example",
    )?;

    for name in ["subetha_consumer_shared", "subetha_consumer_static"] {
        let exe = find_built(&build, name)?;
        let mut consumer = Command::new(&exe);
        let output = run(with_runtime_dir(&mut consumer, &installed.runtime_dir), name)?;
        if !output.contains(CONSUMER_OK) {
            return Err(format!("{name} ran without printing '{CONSUMER_OK}': {}", output.trim()));
        }
        println!("  {name}: {}", output.trim());
    }
    Ok("PASS the shared and static consumers built against the prefix and ran".to_string())
}

/// Compiles the consumer straight from the prefix with the host's C
/// compiler, once against the shared library and once against the static
/// one with the system libraries rustc named, and runs both. This is what
/// proves the artifacts on a host without cmake.
fn direct_consumer_check(root: &Path, prefix: &Path, installed: &Installed) -> Result<String, String> {
    let triple = host_triple()?;
    let tool = match cc::Build::new()
        .target(&triple)
        .host(&triple)
        .opt_level(2)
        .debug(false)
        .cargo_metadata(false)
        .cargo_warnings(false)
        .try_get_compiler()
    {
        Ok(tool) => tool,
        Err(why) => return Ok(format!("SKIPPED no C compiler for {triple}: {why}")),
    };
    let native_libs = fs::read_to_string(&installed.native_libs).map_err(|e| {
        format!("{} cannot be read: {e}", installed.native_libs.display())
    })?;
    let source = root
        .join("crates")
        .join("subetha-ffi")
        .join("cmake-consumer")
        .join("main.c");
    let build = target_dir(root).join("ffi-direct-build");
    remove_tree(&build)?;
    make_dir(&build)?;
    let include = prefix.join("include");
    let lib = prefix.join("lib");

    for (name, statically) in [("subetha_consumer_shared", false), ("subetha_consumer_static", true)] {
        let exe = build.join(if cfg!(windows) { format!("{name}.exe") } else { name.to_string() });
        let mut cmd = tool.to_command();
        if tool.is_like_msvc() {
            cmd.args(["/W4", "/WX"])
                .arg(format!("/I{}", include.display()))
                .arg(&source)
                .arg(format!("/Fe{}", exe.display()))
                .arg(format!("/Fo{}\\", build.display()))
                .arg("/link")
                .arg(format!("/LIBPATH:{}", lib.display()));
            if statically {
                cmd.arg("subetha_ffi_static.lib");
                cmd.args(native_libs.split_whitespace());
            } else {
                cmd.arg("subetha_ffi.lib");
            }
        } else {
            cmd.args(["-Wall", "-Wextra", "-Werror"])
                .arg(format!("-I{}", include.display()))
                .arg(&source)
                .arg("-o")
                .arg(&exe);
            cmd.arg(format!("-L{}", lib.display()));
            if statically {
                cmd.arg(lib.join("libsubetha_ffi.a"));
                cmd.args(native_libs.split_whitespace());
            } else {
                cmd.arg("-lsubetha_ffi");
            }
        }
        run(&mut cmd, &format!("direct compile of {name}"))?;
        let mut consumer = Command::new(&exe);
        let output = run(with_runtime_dir(&mut consumer, &installed.runtime_dir), name)?;
        if !output.contains(CONSUMER_OK) {
            return Err(format!("{name} ran without printing '{CONSUMER_OK}': {}", output.trim()));
        }
        println!("  {name}: {}", output.trim());
    }
    Ok(format!(
        "PASS both consumers compiled with {} against the prefix and ran",
        tool.path().display()
    ))
}

fn host_triple() -> Result<String, String> {
    let out = Command::new("rustc")
        .arg("-vV")
        .output()
        .map_err(|e| format!("rustc could not be run: {e}"))?;
    let text = String::from_utf8_lossy(&out.stdout);
    text.lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(|host| host.trim().to_string())
        .ok_or_else(|| format!("rustc -vV printed no host line:\n{text}"))
}

/// A consumer built by a multi-config generator lands under `Release/`;
/// a single-config one at the build root.
fn find_built(build: &Path, name: &str) -> Result<PathBuf, String> {
    let file = if cfg!(windows) { format!("{name}.exe") } else { name.to_string() };
    for dir in [build.to_path_buf(), build.join("Release")] {
        let candidate = dir.join(&file);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(format!("{file} was not produced under {}", build.display()))
}

/// Puts the installed shared library where the platform's loader looks.
fn with_runtime_dir<'a>(cmd: &'a mut Command, runtime_dir: &Path) -> &'a mut Command {
    let var = if cfg!(windows) {
        "PATH"
    } else if cfg!(target_os = "macos") {
        "DYLD_LIBRARY_PATH"
    } else {
        "LD_LIBRARY_PATH"
    };
    let mut paths = vec![runtime_dir.to_path_buf()];
    if let Some(existing) = env::var_os(var) {
        paths.extend(env::split_paths(&existing));
    }
    let joined = env::join_paths(paths)
        .expect("the runtime directory and the existing search path join into one variable");
    cmd.env(var, joined)
}

fn run(cmd: &mut Command, what: &str) -> Result<String, String> {
    let out = cmd
        .output()
        .map_err(|e| format!("{what} could not be started: {e}"))?;
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    if !out.status.success() {
        return Err(format!("{what} failed ({}):\n{text}", out.status));
    }
    Ok(text)
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    let names: Vec<String> = if cfg!(windows) {
        vec![format!("{name}.exe"), name.to_string()]
    } else {
        vec![name.to_string()]
    };
    env::split_paths(&path)
        .flat_map(|dir| names.iter().map(move |n| dir.join(n)))
        .find(|candidate| candidate.is_file())
}

fn absolute(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = env::current_dir().map_err(|e| format!("the current directory is unknown: {e}"))?;
    Ok(cwd.join(path))
}

fn forward_slashes(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn make_dir(dir: &Path) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|e| format!("{} cannot be created: {e}", dir.display()))
}

fn install(from: &Path, to: &Path) -> Result<(), String> {
    fs::copy(from, to)
        .map_err(|e| format!("{} cannot be copied to {}: {e}", from.display(), to.display()))?;
    println!("installed: {}", to.display());
    Ok(())
}

fn write_file(path: &Path, text: &str) -> Result<(), String> {
    fs::write(path, text).map_err(|e| format!("{} cannot be written: {e}", path.display()))?;
    println!("installed: {}", path.display());
    Ok(())
}

/// Makes `link` in `dir` point at `target` there. A file system that
/// refuses symbolic links gets a copy, and the run says so.
fn link_or_copy(dir: &Path, target: &str, link: &str) -> Result<(), String> {
    let link_path = dir.join(link);
    match fs::remove_file(&link_path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("{} cannot be replaced: {e}", link_path.display())),
    }
    match symlink(target, &link_path) {
        Ok(()) => {
            println!("installed: {} -> {target}", link_path.display());
            Ok(())
        }
        Err(e) => {
            println!(
                "symbolic link {} refused ({e}); copying {target} instead",
                link_path.display()
            );
            install(&dir.join(target), &link_path)
        }
    }
}

#[cfg(unix)]
fn symlink(target: &str, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn symlink(target: &str, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

fn remove_tree(dir: &Path) -> Result<(), String> {
    match fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("{} cannot be removed: {e}", dir.display())),
    }
}
