---
title: Reference
linkTitle: Reference
weight: 3
sidebar:
  open: true
---

Per-crate type / trait / op-kind reference.

- [`subetha-cxc`](subetha-cxc/) - the principal user-facing crate: `Channel<T>`, `AdaptiveIpc<T>`, `AutoIpc`, the MMF dispatcher, and ~40 MMF-backed primitives.
- [`subetha-pointers`](subetha-pointers/) - exotic pointer types for CXC payloads.
- [`subetha-core`](subetha-core/) - the substrate (handshake, observation ring, migration, `Marshal`).
- [`subetha-sidecar`](subetha-sidecar/) - the control plane (policy, scan thread, `SidecarBox`).

For machine-generated API docs (every signature, every type), the
canonical source is the per-crate `docs.rs` page:
[docs.rs/subetha-cxc](https://docs.rs/subetha-cxc) /
[docs.rs/subetha-pointers](https://docs.rs/subetha-pointers) /
[docs.rs/subetha-core](https://docs.rs/subetha-core) /
[docs.rs/subetha-sidecar](https://docs.rs/subetha-sidecar).
