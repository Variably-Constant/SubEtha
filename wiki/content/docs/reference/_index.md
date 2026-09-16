---
title: Reference
linkTitle: Reference
weight: 3
sidebar:
  open: true
---

Per-crate type / trait / op-kind reference.

- [`subetha-cxc`](subetha-cxc/) - the principal user-facing crate: `Channel<T>`, `AdaptiveIpc<T>`, `AutoIpc`, the MMF dispatcher, and more than sixty MMF-backed primitives.
- [`subetha-pointers`](subetha-pointers/) - exotic pointer types for CXC payloads.
- [`subetha-core`](subetha-core/) - the substrate (handshake, observation ring, migration, `Marshal`).
- [`subetha-sidecar`](subetha-sidecar/) - the control plane (policy, scan thread, `SidecarBox`).
- [`subetha-ffi`](subetha-ffi/) - the C ABI: `subetha.h` over generation-checked handles, for every language that binds through C.
- [`subetha-py`](subetha-py/) - the Python binding, bound to the Rust directly rather than through the C ABI, covering every family above.
- [`subetha-pwrs`](subetha-pwrs/) - the PowerShell binding: the module `SubEtha`, whose cmdlets obtain a structure and whose objects operate on it, covering the same families.

For machine-generated API docs (every signature, every type), the
canonical source is the per-crate `docs.rs` page:
[docs.rs/subetha-cxc](https://docs.rs/subetha-cxc) /
[docs.rs/subetha-pointers](https://docs.rs/subetha-pointers) /
[docs.rs/subetha-core](https://docs.rs/subetha-core) /
[docs.rs/subetha-sidecar](https://docs.rs/subetha-sidecar).
