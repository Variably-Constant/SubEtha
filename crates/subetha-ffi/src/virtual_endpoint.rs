//! Virtual endpoints through the C ABI: a name bound to a local ring or
//! to a remote host, rebindable without the caller relearning where the
//! bytes go.
//!
//! A pin cannot cross this boundary. The Rust `PinnedEndpoint` borrows the
//! registry for as long as it lives, and C has no way to honor that
//! borrow, so a caller here reads a target and the generation it was read
//! at, then asks whether that generation still stands before acting on
//! what it read. That is the same comparison `is_still_valid` makes; only
//! the holding is different.
//!
//! The registry lives in one process. Two processes that both bind the
//! same id have two registries and two answers.
//!
//! Strict and managed modes are the same here: no background work.

use std::ffi::c_char;
use std::sync::Arc;

use subetha_cxc::virtual_endpoint::{
    EndpointId, EndpointTarget, RemoteEndpoint, VirtualEndpointRegistry,
};

use crate::error::{
    fail, SUBETHA_E_INVALID_ARGUMENT, SUBETHA_E_MAP_KEY_ABSENT, SUBETHA_E_WRONG_KIND, SUBETHA_OK,
};
use crate::handle::{subetha_handle, Object, SUBETHA_KIND_ENDPOINT_REGISTRY};
use crate::locale::with_locale_ring;
use crate::ring::text;
use crate::runtime::{entry, issue, require_initialized, resolve_mode, with_kind};

/// The id names no binding.
pub const SUBETHA_ENDPOINT_NONE: u32 = 0;
/// The id names a ring in this process.
pub const SUBETHA_ENDPOINT_LOCAL: u32 = 1;
/// The id names a remote host.
pub const SUBETHA_ENDPOINT_REMOTE: u32 = 2;

/// What one id resolves to right now.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Default)]
pub struct subetha_endpoint_target {
    /// One of the `SUBETHA_ENDPOINT_` constants.
    pub kind: u32,
    /// The registry generation this was read at. Pass it to
    /// `subetha_endpoint_still_valid` before acting on what was read: a
    /// rebind between the two calls makes it stale, and nothing else
    /// reveals that.
    pub generation: u64,
}

pub(crate) struct EndpointRegistryObject {
    registry: Arc<VirtualEndpointRegistry>,
    mode: u32,
}

impl EndpointRegistryObject {
    /// Nothing parks on a registry, so a destroy has nothing to wake.
    pub(crate) fn interrupt(&self) {}
}

fn with_registry(handle: subetha_handle, f: impl FnOnce(&EndpointRegistryObject) -> i32) -> i32 {
    with_kind(handle, SUBETHA_KIND_ENDPOINT_REGISTRY, |object| match object {
        Object::EndpointRegistry(r) => f(r),
        _ => fail(SUBETHA_E_WRONG_KIND, "the handle does not name an endpoint registry"),
    })
}

/// Make a registry. `mode` is one of the `SUBETHA_MODE_` constants and is
/// recorded rather than acted on, since a registry runs nothing.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_endpoint_registry_create(
    mode: u32,
    out: *mut subetha_handle,
) -> i32 {
    entry(|| {
        if let Err(code) = require_initialized() {
            return code;
        }
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        let mode = match resolve_mode(mode) {
            Ok(m) => m,
            Err(code) => return code,
        };
        let object = EndpointRegistryObject {
            registry: Arc::new(VirtualEndpointRegistry::new()),
            mode,
        };
        unsafe { issue(Object::EndpointRegistry(object), out) }
    })
}

/// Bind `id` to the locale-adaptive ring `ring` names. Binding an id that
/// is already bound replaces it and steps the generation, which is how a
/// rebind reaches a caller holding an older reading.
/// The ring is taken before the registry is borrowed, and not inside that
/// borrow: a borrow of the handle table does not nest, so holding one
/// while asking for another refuses the second.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_endpoint_bind_local(
    registry: subetha_handle,
    id: u64,
    ring: subetha_handle,
) -> i32 {
    entry(|| {
        let mut taken = None;
        let code = with_locale_ring(ring, |ring| {
            taken = Some(ring);
            SUBETHA_OK
        });
        let Some(ring) = taken else { return code };
        with_registry(registry, |r| {
            r.registry.bind(EndpointId(id), EndpointTarget::Local(ring));
            SUBETHA_OK
        })
    })
}

/// Bind `id` to a remote host: `server_addr` is where its bridge listens
/// and `server_name` is the identity its certificate must carry.
///
/// Binding a remote target needs no transport built into this library. It
/// records where the bytes would go; carrying them is the bridge's job and
/// its feature's.
///
/// # Safety
/// `server_addr` and `server_name` are NUL-terminated UTF-8 strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_endpoint_bind_remote(
    registry: subetha_handle,
    id: u64,
    server_addr: *const c_char,
    server_name: *const c_char,
) -> i32 {
    with_registry(registry, |r| {
        let addr = match unsafe { text(server_addr, "server_addr") } {
            Ok(t) => match t.parse() {
                Ok(a) => a,
                Err(e) => {
                    return fail(SUBETHA_E_INVALID_ARGUMENT, format!("server_addr {t}: {e}"))
                }
            },
            Err(code) => return code,
        };
        let name = match unsafe { text(server_name, "server_name") } {
            Ok(t) => t.to_owned(),
            Err(code) => return code,
        };
        if name.is_empty() {
            return fail(
                SUBETHA_E_INVALID_ARGUMENT,
                "server_name is empty, so no certificate could ever match it",
            );
        }
        r.registry.bind(
            EndpointId(id),
            EndpointTarget::Remote(RemoteEndpoint { server_addr: addr, server_name: name }),
        );
        SUBETHA_OK
    })
}

/// Remove `id`. `SUBETHA_E_MAP_KEY_ABSENT` when it named nothing, so a caller
/// can tell an unbind that did something from one that did not.
#[unsafe(no_mangle)]
pub extern "C" fn subetha_endpoint_unbind(registry: subetha_handle, id: u64) -> i32 {
    with_registry(registry, |r| match r.registry.unbind(EndpointId(id)) {
        Some(_) => SUBETHA_OK,
        None => fail(SUBETHA_E_MAP_KEY_ABSENT, format!("endpoint {id} is not bound")),
    })
}

/// Read what `id` resolves to, and the generation it was read at, into
/// `out`.
///
/// An unbound id is not an error: it reports `SUBETHA_ENDPOINT_NONE` with
/// a generation, because "nothing is bound here right now" is an answer a
/// caller acts on and can also go stale.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_endpoint_read(
    registry: subetha_handle,
    id: u64,
    out: *mut subetha_endpoint_target,
) -> i32 {
    with_registry(registry, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // The generation is taken after the lookup. A rebind between the
        // two makes the pair look stale, which is the safe direction: a
        // caller re-reads. Taken before, the pair could look current while
        // naming a target that had already been replaced.
        let kind = match r.registry.lookup(EndpointId(id)) {
            None => SUBETHA_ENDPOINT_NONE,
            Some(EndpointTarget::Local(_)) => SUBETHA_ENDPOINT_LOCAL,
            Some(EndpointTarget::Remote(_)) => SUBETHA_ENDPOINT_REMOTE,
        };
        let target = subetha_endpoint_target { kind, generation: r.registry.generation() };
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = target };
        SUBETHA_OK
    })
}

/// Copy the address and name of a remote binding out as text.
///
/// `SUBETHA_E_MAP_KEY_ABSENT` when `id` is unbound, `SUBETHA_E_WRONG_KIND` when
/// it names a local ring, and `SUBETHA_E_BUFFER_TOO_SMALL` when a buffer
/// is short, with the needed length written so a caller can ask again.
///
/// # Safety
/// `addr_out` points to `addr_cap` writable bytes and `name_out` to
/// `name_cap`; both length pointers are valid.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn subetha_endpoint_read_remote(
    registry: subetha_handle,
    id: u64,
    addr_out: *mut u8,
    addr_cap: usize,
    addr_len: *mut usize,
    name_out: *mut u8,
    name_cap: usize,
    name_len: *mut usize,
) -> i32 {
    with_registry(registry, |r| {
        let remote = match r.registry.lookup(EndpointId(id)) {
            Some(EndpointTarget::Remote(remote)) => remote,
            Some(EndpointTarget::Local(_)) => {
                return fail(SUBETHA_E_WRONG_KIND, format!("endpoint {id} names a local ring"))
            }
            None => return fail(SUBETHA_E_MAP_KEY_ABSENT, format!("endpoint {id} is not bound")),
        };
        let addr = remote.server_addr.to_string();
        // Both copies are attempted before either result is read, so a
        // caller that sized one buffer right and the other wrong learns
        // both counts from one call.
        let addr_fit = unsafe { crate::ring::copy_out(addr.as_bytes(), addr_out, addr_cap, addr_len) };
        let name_fit = unsafe {
            crate::ring::copy_out(remote.server_name.as_bytes(), name_out, name_cap, name_len)
        };
        if let Err(code) = addr_fit {
            return code;
        }
        if let Err(code) = name_fit {
            return code;
        }
        SUBETHA_OK
    })
}

/// Whether a generation read earlier still stands, into `out`.
///
/// False means something was bound or unbound since, so anything read at
/// that generation is to be read again. It does not mean this id changed:
/// the counter is registry-wide, so a rebind of another id also moves it.
/// Re-reading is cheap and being wrong is not.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_endpoint_still_valid(
    registry: subetha_handle,
    generation: u64,
    out: *mut bool,
) -> i32 {
    with_registry(registry, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = r.registry.generation() == generation };
        SUBETHA_OK
    })
}

/// The registry's current generation into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_endpoint_generation(
    registry: subetha_handle,
    out: *mut u64,
) -> i32 {
    with_registry(registry, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = r.registry.generation() };
        SUBETHA_OK
    })
}

/// How many ids are bound, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_endpoint_count(registry: subetha_handle, out: *mut u64) -> i32 {
    with_registry(registry, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = r.registry.len() as u64 };
        SUBETHA_OK
    })
}

/// The mode this registry was created in, into `out`.
///
/// # Safety
/// `out` is a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn subetha_endpoint_registry_mode(
    registry: subetha_handle,
    out: *mut u32,
) -> i32 {
    with_registry(registry, |r| {
        if out.is_null() {
            return fail(SUBETHA_E_INVALID_ARGUMENT, "out is null");
        }
        // Checked non-null; the caller guarantees it is writable.
        unsafe { *out = r.mode };
        SUBETHA_OK
    })
}
