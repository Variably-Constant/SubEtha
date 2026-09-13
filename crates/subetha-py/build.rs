//! Carry the interpreter's build settings into this crate's own `cfg`.
//!
//! PyO3 works out what the target interpreter is and sets flags such as
//! `Py_GIL_DISABLED` from it, but it sets them for itself. Without this,
//! `cfg!(Py_GIL_DISABLED)` here is false however the interpreter was
//! built, so the module would tell a caller on a free-threaded
//! interpreter that it was not on one.

fn main() {
    pyo3_build_config::use_pyo3_cfgs();
}
