---
title: "Shared String Arena"
weight: 10
---

# SharedStringArena

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Edition](https://img.shields.io/badge/Edition-2024-blue)
![Layout](https://img.shields.io/badge/Layout-MMF--backed-green)
![Protocol](https://img.shields.io/badge/bump_pointer-append_only-brightgreen)
![Cross-Process](https://img.shields.io/badge/Cross--Process-yes-success)

Cross-process string-interning arena. Append-only bump-pointer
allocation; `intern(s)` writes the bytes into the arena and
returns a position-independent `StringRef` (offset + length).
Any process resolves the same StringRef to the same bytes.

> **The "cross-process string interning at lock-free cost"
> primitive.** intern 16-byte string at **9.37 ns** vs
> `Mutex<Vec<String>>` 96.45 ns (**10.3x faster** - bump-pointer
> + memcpy vs Mutex + heap-alloc + String construction).
> get_bytes at **9.76 ns** vs 77.27 ns (**7.9x faster**).
> Architectural lever: bytes live in MMF, cross-process by
> default, no per-string heap allocation.

**Constraints (read first):**

- **Native sidecar integration**: the struct carries a `HandshakeHeader` + `ObservationRing` and implements `subetha_sidecar::AdaptiveInstance`. Wrap in `SidecarBox::new` to register with the global sidecar; raw `create()` / `open()` return the unregistered type unchanged.

- **Append-only**: no deletion; `clear()` resets the whole
  arena.
- **Bump-pointer allocation**: `fetch_add` on offset; one
  memcpy per intern.
- **StringRef = (offset: u32, len: u32)**: position-independent;
  `to_u64()` / `from_u64()` pack it into one word for transport.
- **Bounded capacity at create**: arena size in bytes.
- **Cross-process backed by MMF.**

---

## Bench evidence

| Op | `SharedStringArena` (mmf) | `Mutex<Vec<String>>` | Relative |
|---|---:|---:|---|
| intern (16-byte string) | **9.37 ns** | 96.45 ns | **10.3x faster** |
| get_bytes (resolve known ref) | **9.76 ns** | 77.27 ns | **7.9x faster** |

### Reading the trade-offs

1. **intern 10.3x faster.** Bump-pointer `fetch_add` + memcpy
   vs Mutex lock + String::from + Vec::push + unlock. No
   per-string heap allocation.
2. **get_bytes 7.9x faster.** Position-independent slice via
   offset + len vs Mutex lock + Vec[idx] + as_bytes + clone.
3. **Cross-process visibility**: StringRef bits resolve
   identically across all processes mapping the arena.

### Rule 3b bench audit

- **Fair contender**: `Mutex<Vec<String>>` is the textbook
  in-process string-table baseline. Same intern + lookup
  semantics.
- **No `thread::spawn` inside `b.iter`**: single-threaded.
- **Sizing**: 16 MB arena; pre-cleared between iters via the
  full-arena overflow branch.
- **MMF lifecycle managed**: create + ops + drop + remove_file.

### What the numbers do NOT show

- **Cross-process string interning**: any process interns; any
  process resolves the StringRef. Mutex<Vec<String>> cannot.
- **Memory locality**: bytes are cache-contiguous in the
  arena; Mutex<Vec<String>> scatters heap-allocated Strings.

---

## Worked examples

### Basic intern + resolve

```rust
use subetha_cxc::SharedStringArena;

let a = SharedStringArena::create("/tmp/arena.bin", 1024).unwrap();
let r = a.intern("hello, world").unwrap();
let bytes = a.get_bytes(r).unwrap();
assert_eq!(bytes, b"hello, world");
```

### Cross-process string table

```rust
// Writer:
let a = SharedStringArena::create("/tmp/strings.bin", 1 << 20).unwrap();
let r = a.intern("shared-string").unwrap();
let raw_ref: u64 = r.to_u64();   // pack (offset, len) into one u64 to ship

// Reader (any process):
let a = SharedStringArena::open("/tmp/strings.bin", 1 << 20).unwrap();
let r = subetha_cxc::StringRef::from_u64(raw_ref);
let bytes = a.get_bytes(r).unwrap();
```

---

## Use case patterns

### Pattern: cross-process symbol table

Compilers / interpreters intern identifiers once and share
StringRef tokens across processes.

### Pattern: log message dictionary

A logger interns common message strings; downstream analyzers
resolve refs without copying.

### Pattern: append-only event metadata

Event records carry StringRefs to interned metadata; the arena
grows linearly with unique metadata, not with event count.

---

## Known limitations

- **No deletion**: the whole arena is reclaimed via `clear`.
- **Bounded capacity at create**.
- **StringRef len is u32**: a single interned string is bounded
  only by the arena capacity, not a 64 KB per-string ceiling.
- **Cross-process backed by MMF.**

---

## Common pitfalls

- **Using a StringRef after `clear`.** The bytes may be
  overwritten by subsequent interns. Treat clear as
  invalidating all outstanding refs.

- **Sizing the arena too small.** `intern` returns `Full`
  once capacity is exhausted. Plan for unique-string-byte
  total, not event count.

- **Wrapping in a Mutex.** Pointless; the bump-pointer
  fetch_add is already concurrency-safe.

---

## References

- Source: `crates/subetha-cxc/src/shared_string_arena.rs` (564
  lines, unit tests covering intern + resolve, cross-handle
  visibility, arena-full handling, and clear semantics).
- Bench: `crates/subetha-cxc/benches/shared_string_arena.rs`
  (intern short, intern 16-byte, get_bytes vs
  `Mutex<Vec<String>>` and `RwLock<Vec<String>>`).
- Underlying primitive: [SHARED_ATOMIC.md](../atomics/shared-atomic/) -
  the bump-pointer counter.
- Sibling primitive: [SHARED_REGION.md](shared-region/) -
  typed slot allocator with reuse; StringArena is the
  byte-stream variant without reuse.
