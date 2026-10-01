---
title: "Shm File"
weight: 50
---

# ShmFile

![Rust](https://img.shields.io/badge/Rust-1.96+-orange?logo=rust)
![Platform](https://img.shields.io/badge/platform-cross--platform-blue)
![Layout](https://img.shields.io/badge/Layout-shared--memory-green)

Cross-platform RAM-resident named shared-memory backing. Wraps the
platform's named shared-memory primitive so the rest of the
substrate can treat ShmFs the same way it treats anon and file
backings.

| Platform | Backend |
|---|---|
| Linux + macOS | `libc::shm_open` + `ftruncate` + memmap2 via `File::from_raw_fd` |
| Windows | `windows_sys::CreateFileMappingW(INVALID_HANDLE_VALUE, ...)`, or `OpenFileMappingW` for a region that must exist, + `MapViewOfFile` |

Naming: the caller's logical name is prefixed with `/subetha_` on
Unix (POSIX shm names must start with `/`) and, on Windows, with the
object directory of the `ShmNamespace` asked for: `Local\subetha_`,
`Global\subetha_`, or an AppContainer's named-object path followed by
`\subetha_`. Embedded slashes in the caller's name become underscores.
On Apple targets, where POSIX shm names are capped at 31 chars
(`PSHMNAMLEN`), a prefixed name that would overrun the cap collapses to
a deterministic short `/se_{hash}` form so a create here and an open in
a peer process still resolve to the same region. macOS also only honors
`ftruncate` once at creation, so a create sizes the region only when it
is not already at least `size` (later openers map it as-is); the
mapping is prefaulted on construction.

## API

| Call | Behavior |
|---|---|
| `ShmFile::create_or_open_named(name: &str, size: usize) -> io::Result<Self>` | Create or open a named shared-memory region of `size` bytes in the per-session namespace. Asserts `size > 0`. |
| `ShmFile::create_or_open_named_in(name: &str, size: usize, ns: ShmNamespace) -> io::Result<Self>` | The same, naming the region in `ns`. `ShmNamespace::Machine` resolves one name to one region for every Windows session, which is what a service in session 0 and its interactive clients need; creating one requires `SeCreateGlobalPrivilege` and a caller without it gets the OS error rather than a per-session region. On Unix a POSIX name is machine-wide either way and the choice changes nothing. |
| `ShmFile::create_or_open_named_secured(name: &str, size: usize, ns: ShmNamespace, sddl: Option<&str>) -> io::Result<Self>` | The same, with `sddl` as the security descriptor a create applies to the region. See [Access control](#access-control). For a region either side may reach first; the handle owns the name only if this call made the region. |
| `ShmFile::create_named_secured(name: &str, size: usize, ns: ShmNamespace, sddl: Option<&str>) -> io::Result<Self>` | The creator's call: as `create_or_open_named_secured`, and the handle owns the name even when a region of that name was already there, such as one a process that died left behind, which the creator then lays out afresh. |
| `ShmFile::open_named_in(name: &str, size: usize, ns: ShmNamespace) -> io::Result<Self>` | Open a region that must already exist. One nobody made is `io::ErrorKind::NotFound`, never a fresh empty region; one smaller than `size` is refused. The handle never owns the name. |
| `ShmFile::open_named_secured(name: &str, size: usize, ns: ShmNamespace, sddl: Option<&str>) -> io::Result<Self>` | The same, keeping `sddl` for the objects made beside the region, such as a waker's park events. The region itself keeps the descriptor it was made with. |
| `shm.as_mut_slice() -> &mut [u8]` | Cross-platform mutable byte slice into the mapped region. |
| `shm.len() -> usize` | Region size in bytes. |
| `shm.is_empty() -> bool` | Always false for a valid region. |
| `shm.logical_name() -> &str` | The substrate-prefixed safe name. |
| `shm.owns_name() -> bool` | Whether this handle owns the region's name; see [Cleanup](#cleanup). |
| `shm.keep_name()` | Give the name up, so this handle's drop leaves it for whatever removes it by name. For a structure whose regions different handles make and which removes them together; see [Cleanup](#cleanup). |
| `"S-1-15-2-...".parse::<ContainerSid>()` | An AppContainer's SID from its string form, held inline so `ShmNamespace` stays `Copy`. Anything outside the AppContainer range (`S-1-15-2-` and at least one more sub-authority) is refused with `InvalidInput`. Prints back as the same string. |
| `ContainerSid::from_container_name(name: &str) -> io::Result<ContainerSid>` | Windows: the SID Windows gives the container whose profile was created under `name`, as `DeriveAppContainerSidFromAppContainerName` computes it. |

## AppContainers

`ShmNamespace::AppContainer(sid)` names regions in an AppContainer's
named-object directory, `AppContainerNamedObjects\<SID>`, from outside
the container. A process inside the container reaches the same regions
under `ShmNamespace::Session`, because inside a container `Local\` is
that directory. So the outside process names the container and the
process inside names nothing.

Measured on Windows 11 with a probe that created a profile, started a
child in the container and exchanged a mapping and events with it:

- **The directory exists only while a process of the container runs.**
  A create under it before one has started fails with the path not
  found. The creator therefore starts its container process suspended,
  creates what the container will open, and then lets it run.
- **Both directions work.** The child opened the outside's mapping as
  `Local\<name>`, and a create-or-open of that name returned the
  existing mapping; writes and event signals crossed both ways; and an
  event the child created as `Local\<name>` opened from outside under
  the directory's path.
- **The descriptor must admit the container.** The probe's regions
  carried `D:P(A;;GA;;;BA)(A;;GA;;;AU)(A;;GA;;;<container SID>)S:(ML;;NW;;;LW)`:
  administrators, authenticated users and the container itself, with a
  low mandatory label so the container's low integrity may write.

An `AdaptiveRing` and its `NotifierSet` created with
`ShmNamespace::AppContainer` put every region and event in the
directory, including the ones made later: a producer slot grown past
the creation hint, the payload region of the first offset frame, and
the notifier events.

On Unix the variant, like the other two, changes nothing.

## Access control

The namespace decides which name resolves; the security descriptor
decides who may map it. They are separate, and a region reachable
across Windows sessions needs both.

A section created with no descriptor carries the creator's default,
which admits that user's own session. A service in session 0 creating
in `ShmNamespace::Machine` therefore makes a region whose name an
interactive client resolves and whose contents it is refused. `sddl`
is the descriptor that names who may map it, in SDDL form.

- **Applied only by the call that creates the region.** Opening one
  that already exists uses the descriptor already on it.
- **The mapping asks for `FILE_MAP_ALL_ACCESS`.** A descriptor
  granting only read is refused at the map, so a caller admitting
  authenticated users to map and query writes `"D:(A;;0x000F001F;;;AU)"`.
- **An unparseable descriptor fails the create.** The region is not
  built with the creator's default in its place.
- **The descriptor is the caller's.** The crate applies what it is
  given and supplies no default of its own; who may reach a region is
  the calling program's decision.
- **Unix ignores it.** A POSIX shared-memory object carries mode bits
  rather than an ACL, and the caller sets those on the object itself.

## Cross-process visibility

Two handles opened with the same logical name map onto the same
underlying memory region. This is the property that distinguishes
ShmFile from `MmapOptions::map_anon` (which is in-process only).

## Cleanup

A handle owns the region's name when it made the region, or when it
was created with `create_named_secured`, unless it gave the name up with
`keep_name`. Drop:

- Unix: drops the inner `File` (closes the fd), and a handle that owns
  the name calls `shm_unlink` on it, so a later create with the same
  name starts fresh. A handle that opened a region another made leaves
  the name alone, so peers attaching and leaving never stop a later
  process from attaching.
- Windows: `UnmapViewOfFile` + `CloseHandle`. Windows refcounts
  handles; the named object goes away when the last handle closes,
  whoever owns it.

A structure built from several regions keeps every name it makes, since
different handles make them: an `AdaptiveRing`'s names, a peer's grown
backings and payload region included, and a `NotifierSet`'s record stay
until the structure's own removal takes them, as their file-backed twins'
files do. For a ring that is `AdaptiveRing::unlink_shmfs` or its last
holder; see [the ring's Lifetime](../../rings/shared-ring-adaptive/#lifetime).

## Worked example

```rust,no_run
use subetha_cxc::shm_file::{ShmFile, ShmNamespace};

let mut a = ShmFile::create_named_secured("ipc_demo", 4096, ShmNamespace::Session, None)?;
a.as_mut_slice()[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

// Process B (or same process, different handle):
let mut b = ShmFile::open_named_in("ipc_demo", 4096, ShmNamespace::Session)?;
assert_eq!(&b.as_mut_slice()[0..4], &[0xDE, 0xAD, 0xBE, 0xEF]);
# Ok::<(), std::io::Error>(())
```

## When to reach for this primitive

- Building a custom cross-process primitive that needs raw shared
  memory (the substrate's standard rings use this internally via
  `SpscRingCore::create_from_shm` / `SharedRing::create_from_shm`).
- Interop with non-SubEtha processes that speak POSIX shm.

## When not to reach for this

- You want a ring or a hash map: use the substrate's typed
  primitives ([`AdaptiveRing`](../../rings/shared-ring-adaptive/),
  [`SharedHashMap`](../../shared-hash-map/), etc.) which carry their
  own slot layouts.

## References

- Source: `crates/subetha-cxc/src/shm_file.rs` (1,151 lines, 16 unit
  tests: create+read/write, two-handles-same-memory, deterministic
  names, namespace prefix selection, an AppContainer name in the
  container's directory, a SID's string form round trip, SIDs outside
  the AppContainer range refused, a container name deriving its SID
  (Windows), a machine-namespace create that reaches the OS and never
  silently falls back to a per-session region, a descriptor that is
  either applied or fails the create, drop-then-recreate-fresh, an open
  of a missing region reporting not found, an open larger than the
  region refused, who owns a name, an opener's drop leaving the name
  for later openers, and an Apple-gated name-length test). `ShmFile`
  lives in the `pub mod shm_file` module path.
- [`LocaleAdaptiveRing`](../../rings/locale-adaptive-ring/) -
  uses `ShmFile` for the `Locale::ShmFs` backing.
- [`SpscRingCore::create_from_shm`](../../rings/shared-ring-spsc/),
  [`SharedRing::create_from_shm`](../../rings/shared-ring/) -
  direct construction on a ShmFile.
