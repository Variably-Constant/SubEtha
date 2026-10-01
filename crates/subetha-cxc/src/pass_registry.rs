//! Closure registry for cross-process `Pass<F>` dispatch.
//!
//! Rust closures cannot be safely serialized across process
//! boundaries; they reference function pointers that are not
//! position-stable, and they may capture variables of arbitrary
//! types. The PSC / Ray / Akka pattern is to register closures by
//! ID at startup; the wire protocol carries the ID + serialized
//! args, not the closure code.
//!
//! Each process must register the same ID -> closure mapping at
//! startup (typically via a macro that all participating binaries
//! call). A `Pass { id, args }` can then be dispatched by any
//! process, including failover targets.
//!
//! The map is copied on write: a registration builds a new map and
//! swaps it in, so a dispatch reads one whole map and takes nothing a
//! registration waits on.

use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;
use subetha_core::SwapCell;

/// A passable unit of work: a closure ID plus its serialized args.
#[derive(Debug, Clone)]
pub struct Pass {
    pub closure_id: u32,
    pub args: Vec<u8>,
}

/// Outcome of running a Pass; arbitrary bytes that the originator
/// can deserialize.
pub type PassResult = Result<Vec<u8>, PassError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassError {
    UnknownClosureId(u32),
    ExecutionError(String),
}

/// Closure handler signature. Args are raw bytes; result is raw
/// bytes. Caller-supplied (de)serialization. Shared, since a map a
/// registration replaces may still be read by a dispatch in flight.
pub type PassHandler = Arc<dyn Fn(&[u8]) -> PassResult + Send + Sync + 'static>;

static REGISTRY: Lazy<SwapCell<HashMap<u32, PassHandler>>> =
    Lazy::new(|| SwapCell::new(HashMap::new()));

/// Register a closure under `id`. Subsequent calls with the same id
/// overwrite. Returns the previous handler if any.
pub fn register<F>(id: u32, f: F) -> Option<PassHandler>
where F: Fn(&[u8]) -> PassResult + Send + Sync + 'static,
{
    let handler: PassHandler = Arc::new(f);
    let replaced = REGISTRY.rcu(|map| {
        let mut next = HashMap::clone(map);
        next.insert(id, Arc::clone(&handler));
        next
    });
    replaced.get(&id).cloned()
}

/// Unregister a closure. Returns the handler if any.
pub fn unregister(id: u32) -> Option<PassHandler> {
    let replaced = REGISTRY.rcu(|map| {
        let mut next = HashMap::clone(map);
        next.remove(&id);
        next
    });
    replaced.get(&id).cloned()
}

/// True when `id` is registered in this process.
pub fn is_registered(id: u32) -> bool {
    REGISTRY.load().contains_key(&id)
}

/// Number of registered handlers in this process.
pub fn registered_count() -> usize {
    REGISTRY.load().len()
}

/// Execute a Pass. Returns the closure's result, or
/// `PassError::UnknownClosureId` when the id is not registered. The
/// handler runs outside the registry: a registration during the call
/// swaps in a new map and this call keeps the handler it found.
pub fn execute(pass: &Pass) -> PassResult {
    let handler = REGISTRY.load().get(&pass.closure_id).cloned();
    match handler {
        Some(handler) => handler(&pass.args),
        None => Err(PassError::UnknownClosureId(pass.closure_id)),
    }
}

/// Macro helper for static-registry style registration. Each
/// participating binary should call `register_pass!(ID, "name",
/// |args| { ... })` at startup.
#[macro_export]
macro_rules! register_pass {
    ($id:expr, $name:expr, $handler:expr) => {{
        // Registration returns Option<PassHandler>: Some = replaced
        // prior handler, None = first registration. The prior must
        // be dropped immediately (not held for the macro scope) so
        // a `let _prior =` binding is wrong - use explicit drop.
        drop($crate::pass_registry::register($id, $handler));
        let _name = $name;  // name is for documentation; not stored
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_then_execute_round_trips_bytes() {
        // Use distinct ids per test to avoid registry contention
        // across parallel runs.
        let id = 0x1000_0001;
        register(id, |args| {
            let mut out = args.to_vec();
            out.reverse();
            Ok(out)
        });
        let pass = Pass { closure_id: id, args: b"hello".to_vec() };
        let r = execute(&pass).unwrap();
        assert_eq!(r, b"olleh");
        unregister(id);
    }

    #[test]
    fn unknown_closure_id_returns_error() {
        let pass = Pass { closure_id: 0xDEAD_BEEF, args: vec![] };
        assert_eq!(execute(&pass), Err(PassError::UnknownClosureId(0xDEAD_BEEF)));
    }

    #[test]
    fn re_register_overwrites_previous() {
        let id = 0x1000_0002;
        register(id, |_| Ok(b"v1".to_vec()));
        register(id, |_| Ok(b"v2".to_vec()));
        let pass = Pass { closure_id: id, args: vec![] };
        assert_eq!(execute(&pass).unwrap(), b"v2");
        unregister(id);
    }

    #[test]
    fn handler_can_return_execution_error() {
        let id = 0x1000_0003;
        register(id, |_| Err(PassError::ExecutionError("nope".to_string())));
        let pass = Pass { closure_id: id, args: vec![] };
        match execute(&pass) {
            Err(PassError::ExecutionError(msg)) => assert_eq!(msg, "nope"),
            other => panic!("expected ExecutionError, got {other:?}"),
        }
        unregister(id);
    }

    #[test]
    fn is_registered_and_count_accurate() {
        let id_a = 0x1000_0004;
        let id_b = 0x1000_0005;
        register(id_a, |_| Ok(vec![]));
        register(id_b, |_| Ok(vec![]));
        assert!(is_registered(id_a));
        assert!(is_registered(id_b));
        // The registry is global and sibling tests register and
        // unregister concurrently, so a before/after count delta is
        // not a stable property. What must hold: the count includes
        // the two registrations this test owns right now.
        assert!(registered_count() >= 2);
        unregister(id_a);
        unregister(id_b);
        assert!(!is_registered(id_a));
    }

    #[test]
    fn macro_registration_works() {
        let id = 0x1000_0006;
        register_pass!(id, "test_pass", |args: &[u8]| {
            Ok(args.iter().map(|b| b.wrapping_add(1)).collect())
        });
        assert!(is_registered(id));
        let pass = Pass { closure_id: id, args: vec![1, 2, 3] };
        assert_eq!(execute(&pass).unwrap(), vec![2, 3, 4]);
        unregister(id);
    }
}
