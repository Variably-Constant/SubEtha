---
title: "Arenas & Region Storage"
weight: 270
sidebar:
  open: true
---

# Arenas and region storage

Pool allocators with position-independent addressing inside an MMF.

| Primitive | Allocation shape |
|---|---|
| [Shared String Arena](shared-string-arena/) | Append-only; refer to strings by offset |
| [Raw Arena](raw-arena/) | Power-of-two blocks published, retired under an epoch and reused; recovered from their headers after a crash |
| [Shared Handle Table](shared-handle-table/) | ECS-style slotmap with generational handles |
| [Shared Region](shared-region/) | Typed `T` arena with position-independent offset pointers between regions |
