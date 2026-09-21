---
title: "Every class, in full"
weight: 10
---

# Every class, in full

All 91 classes the extension exports, with every method,
its full signature and what it answers. Generated from the type
stub by `crates/subetha-py/tools/export_reference.py`; the stub is
held against the compiled module by `tests/test_surface.py`, so a
name here is a name that ships.

A method answering `T | None` uses `None` for an ordinary absent
answer rather than a fault, the same way the other bindings use
their empty value.

Signatures come from the stub and descriptions from the Rust doc
comments. `help()` in a REPL shows both, because PyO3 derives
`__text_signature__` from the `#[pyo3(signature = ...)]`
attribute; this page exists to read the surface whole rather
than a name at a time.

Each description here is the opening paragraph of the method's
doc comment. Many carry more than that, on what an answer means
or what the call does not do, and `help()` shows all of it.

All 687 methods carry a description.

## Contents

[`AdaptiveQueue`](#adaptivequeue), [`Arena`](#arena), [`Atomic`](#atomic), [`BTreeMap`](#btreemap), [`BitVec`](#bitvec), [`BlockedBloomFilter`](#blockedbloomfilter), [`BloomFilter`](#bloomfilter), [`BroadcastRing`](#broadcastring), [`Capacity`](#capacity), [`CapacityRing`](#capacityring), [`CausalClock`](#causalclock), [`Cell`](#cell), [`Channel`](#channel), [`Clock`](#clock), [`Condvar`](#condvar), [`CountMinSketch`](#countminsketch), [`Deque`](#deque), [`EpochBarrier`](#epochbarrier), [`Epochs`](#epochs), [`FenceClock`](#fenceclock), [`FineBloom`](#finebloom), [`Forecast`](#forecast), [`FrameRegion`](#frameregion), [`Graph`](#graph), [`HandleTable`](#handletable), [`HashMap`](#hashmap), [`Heartbeat`](#heartbeat), [`Histogram`](#histogram), [`Hold`](#hold), [`HolderTable`](#holdertable), [`HyperLogLog`](#hyperloglog), [`KvMap`](#kvmap), [`LamportConsumer`](#lamportconsumer), [`LamportProducer`](#lamportproducer), [`LaneClaim`](#laneclaim), [`LanedMap`](#lanedmap), [`LanedPin`](#lanedpin), [`LazyValue`](#lazyvalue), [`LeaderElection`](#leaderelection), [`LeaseHold`](#leasehold), [`LinkedList`](#linkedlist), [`LocaleRing`](#localering), [`LossBursts`](#lossbursts), [`LossKind`](#losskind), [`LruCache`](#lrucache), [`MapPin`](#mappin), [`MpmcConsumer`](#mpmcconsumer), [`MpmcProducer`](#mpmcproducer), [`MpscConsumer`](#mpscconsumer), [`MpscProducer`](#mpscproducer), [`Notifier`](#notifier), [`NotifierSet`](#notifierset), [`OrderedReceiver`](#orderedreceiver), [`OwnerLease`](#ownerlease), [`PathChanges`](#pathchanges), [`Periodicity`](#periodicity), [`PermitHold`](#permithold), [`PubSub`](#pubsub), [`QosPolicy`](#qospolicy), [`QosSnapshot`](#qossnapshot), [`QuicBridgeClient`](#quicbridgeclient), [`QuicBridgeServer`](#quicbridgeserver), [`RWLock`](#rwlock), [`RateLimiter`](#ratelimiter), [`Region`](#region), [`ReorderWindow`](#reorderwindow), [`Reservoir`](#reservoir), [`Ring`](#ring), [`RoundTripShape`](#roundtripshape), [`Semaphore`](#semaphore), [`SensReceiver`](#sensreceiver), [`SensSender`](#senssender), [`SharedArc`](#sharedarc), [`Slab`](#slab), [`SlabPin`](#slabpin), [`SpscRing`](#spscring), [`Stack`](#stack), [`Subscriber`](#subscriber), [`TcpBridgeClient`](#tcpbridgeclient), [`TcpBridgeServer`](#tcpbridgeserver), [`TimePointTile`](#timepointtile), [`Timing`](#timing), [`TinyBloom`](#tinybloom), [`TopologyMap`](#topologymap), [`Tower`](#tower), [`Universal`](#universal), [`Vec`](#vec), [`VersionChain`](#versionchain), [`VersionedMap`](#versionedmap), [`VersionedSlab`](#versionedslab), [`WorkQueue`](#workqueue)

## AdaptiveQueue

| Attribute | Type |
|---|---|
| `max_item_size` | `int` |
| `ordering` | `str` |
| `inversions` | `int` |
| `shape` | `str` |
| `shape_generation` | `int` |
| `traffic` | `tuple[int, float]` |

| Method | Kind | What it does |
|---|---|---|
| `change_shape_to(self, shape: str) -> None` | method | Move to a named shape, whatever the traffic says. |
| `maybe_change_shape(self) -> str | None` | method | Look at the traffic so far and move to the shape that fits it, answering the new shape or None when the one it has already fits. |
| `recv(self) -> bytes | None` | method | The next item, or None when there is nothing there. |
| `recv_for(self, timeout: float | None=None) -> bytes | None` | method | Wait for the next item, or until `timeout` seconds have passed. Answers None on a timeout. |
| `recv_many(self, max_items: int=256) -> list[bytes]` | method | Everything waiting, up to `max_items`, in one crossing. |
| `send(self, item: bytes) -> bool` | method | Send one item, answering False when it is full. |
| `send_for(self, item: bytes, timeout: float | None=None) -> bool` | method | Wait until the item can be sent, or until `timeout` seconds have passed. Answers False on a timeout. |
| `send_many(self, items: Sequence[bytes]) -> int` | method | Send a run of items as one batch, which is also what tells the queue the traffic comes in batches and may be worth a different shape. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Whatever shape it changed into inside the block is the one it is still in afterwards, and anything queued stays queued. |
| `__init__(self, path: str, capacity: int=1024, senders: int=1, readers: int=1, ordering: str='per_producer', auto_order: float | None=None) -> None` | method | `capacity` is the number of items in flight, rounded up to a power of two. `senders` and `readers` are where it starts; what it becomes is decided by the traffic. |

## Arena

| Attribute | Type |
|---|---|
| `capacity_bytes` | `int` |
| `remaining_bytes` | `int` |
| `used_bytes` | `int` |
| `writable` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `get(self, reference: int) -> str` | method | Resolve a reference to its string. |
| `get_bytes(self, reference: int) -> bytes` | method | Resolve a reference to its bytes, for anything that is not text. |
| `get_many(self, references: Sequence[int]) -> list[str]` | method | Resolve a run of references in one crossing. |
| `intern(self, value: str) -> int | None` | method | Intern a string and return the reference that names it, or `None` when the arena is full. |
| `intern_bytes(self, value: bytes) -> int | None` | method | Intern bytes that need not be text. |
| `intern_many(self, values: Sequence[str]) -> list[int]` | method | Intern a run of strings in one crossing, stopping at the first the arena cannot take. Returns the references it managed. |
| `open(path: str, capacity_bytes: int) -> Arena` | staticmethod | Attach to an arena that already exists, able to intern into it. Raises `OSError` when it is not there. `capacity_bytes` must be the one it was created with. |
| `open_read_only(path: str, capacity_bytes: int) -> Arena` | staticmethod | Attach for reading only, so `intern` cannot be called and `writable` answers `False`. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. References handed out inside the block stay valid after it: they name bytes in the file, not anything this object owns. |
| `__init__(self, path: str, capacity_bytes: int) -> None` | method | Obtain the arena at `path` holding `capacity_bytes` of interned text, creating it when the file does not exist. |

## Atomic

| Method | Kind | What it does |
|---|---|---|
| `compare_exchange(self, expected: int, new: int, order: str='seq_cst') -> int` | method | Put `new` in only if the value is still `expected`, and answer what was there either way. |
| `fetch_add(self, value: int=1, order: str='seq_cst') -> int` | method | Add, and answer what the value was before. Adding past what a sixty-four bit number holds wraps round, as it does in Rust and in C. |
| `fetch_add_many(self, count: int, value: int=1, order: str='seq_cst') -> int` | method | Add `value` `count` times and return the value before the run. |
| `fetch_and(self, value: int, order: str='seq_cst') -> int` | method | Clear every bit that is not set in `value`, and answer what the value was before. |
| `fetch_or(self, value: int, order: str='seq_cst') -> int` | method | Set every bit that is set in `value`, and answer what the value was before. |
| `fetch_sub(self, value: int=1, order: str='seq_cst') -> int` | method | Subtract, and answer what the value was before. Taking more than the value holds wraps round to the top, which is what a counter going below zero means here. |
| `fetch_xor(self, value: int, order: str='seq_cst') -> int` | method | Flip every bit that is set in `value`, and answer what the value was before. |
| `load(self, order: str='seq_cst') -> int` | method | Read the value. |
| `open(path: str) -> Atomic` | staticmethod | Attach to an atomic that already exists, leaving its value alone. |
| `store(self, value: int, order: str='seq_cst') -> None` | method | Write the value, discarding whatever was there without reading it. Use `swap` when the previous value matters, or `compare_exchange` when the write should land only if nobody else got there first. |
| `swap(self, value: int, order: str='seq_cst') -> int` | method | Put `value` in and answer what was there, in one step nothing else can get between. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The mapping goes when the last reference to this object goes, not at the end of the block, and the file outlives the process either way. |
| `__init__(self, path: str, init: int=0) -> None` | method | Obtain the atomic at `path`, creating it holding `init` when the file does not exist and attaching to its live value when it does. |

## BTreeMap

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `key_size` | `int` |
| `nodes` | `int` |
| `value_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self) -> None` | method | Remove every entry, so the map is empty and its room is free again. The capacity and both widths are unchanged, and every process mapping the file sees it. |
| `first(self) -> tuple[bytes, bytes] | None` | method | The smallest key and its value, or `None` when the map is empty. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the entries without this; flushing is about surviving a machine that stops. |
| `get(self, key: bytes) -> bytes | None` | method | What `key` holds, or `None` when it holds nothing. The value is always `value_size` bytes. |
| `insert(self, key: bytes, value: bytes) -> bytes | None` | method | Insert or replace. Returns what the key held before, or `None` when it held nothing; `False` is never returned, a full map raises, because a map that cannot take a key has failed rather than answered. |
| `insert_many(self, pairs: Sequence[tuple[bytes, bytes]]) -> int` | method | Insert or replace a run of pairs, stopping at the first the map has no room for, and answer how many landed. |
| `last(self) -> tuple[bytes, bytes] | None` | method | The largest key and its value, or `None` when the map is empty. |
| `open(path: str, capacity: int, key_size: int, value_size: int, tag: int=0) -> BTreeMap` | staticmethod | Attach to a map that already exists, raising `OSError` when it does not. The capacity, both widths and the tag must be the ones it was created with. |
| `remove(self, key: bytes) -> bytes | None` | method | Take a key out and answer what it held, or `None` when it held nothing. The room it used goes back to the map. |
| `__contains__(self, key: bytes) -> bool` | method | Whether the key is present, without bringing its value back over the boundary. Cheaper than `get` when the value is not wanted. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The entries stay in the file for whoever attaches next, and nothing is flushed: call `flush` where durability matters. |
| `__init__(self, path: str, capacity: int, key_size: int, value_size: int, tag: int=0) -> None` | method | Obtain the map at `path` holding `capacity` entries of `key_size` and `value_size` bytes, creating it when the file does not exist. A capacity below one is a `ValueError`. |
| `__len__(self) -> int` | method | How many entries the map holds, which is not the capacity and not `nodes`: a node is a block of the tree and holds several entries. |

## BitVec

| Attribute | Type |
|---|---|
| `capacity_bits` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self, index: int) -> bool` | method | Clear bit `index` and report what it was. |
| `get(self, index: int) -> bool` | method | Whether bit `index` is set, without changing it. An index past the capacity is an `OSError`. |
| `open(path: str, capacity_bits: int) -> BitVec` | staticmethod | Attach to a bit vector that already exists, raising `OSError` when it does not. `capacity_bits` must be the one it was created with. |
| `set(self, index: int) -> bool` | method | Set bit `index` and report what it was. |
| `set_range(self, lo: int, hi: int) -> None` | method | Set every bit from `lo` up to but not including `hi`, in one call rather than one per bit. |
| `toggle(self, index: int) -> bool` | method | Flip bit `index` and report what it now is. |
| `__getitem__(self, index: int) -> bool` | method | The same answer `get` gives, so `bits[3]` reads bit three. There is no slicing and no negative indexing: an index is a bit number. |
| `__init__(self, path: str, capacity_bits: int) -> None` | method | The constructor asserts a capacity of at least one bit, so that is refused here first. |
| `__len__(self) -> int` | method | How many bits the vector addresses, which is its capacity and not a count of the bits that are set. It never changes, so `len` on a bit vector is a constant rather than a measurement. |

## BlockedBloomFilter

| Attribute | Type |
|---|---|
| `blocks` | `int` |
| `hashes` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self) -> None` | method | Empty the filter. |
| `contains(self, item: bytes) -> bool` | method | As `in`, spelled out. |
| `contains_many(self, items: Sequence[bytes]) -> list[bool]` | method | Ask about a run of items in one crossing. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the bits without this; flushing is about surviving a machine that stops. |
| `insert(self, item: bytes) -> None` | method | Add an item, setting its bits inside a single cache-line block. |
| `insert_many(self, items: Sequence[bytes]) -> int` | method | Add a run of items in one crossing. |
| `open(path: str, bits: int, hashes: int) -> BlockedBloomFilter` | staticmethod | Attach to a filter another holder made, with the size and hash count it was made with. |
| `reset(path: str, bits: int, hashes: int) -> BlockedBloomFilter` | staticmethod | Empty the filter and remake it at this size, throwing away everything in it. |
| `suggest(items: int, false_positive_rate: float) -> tuple[int, int]` | staticmethod | The size and hash count for holding `items` with no more than `false_positive_rate` of wrong yeses. Hand both to the constructor. |
| `__contains__(self, item: bytes) -> bool` | method | False means the item was definitely never added. True means it probably was. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The bits stay set for whoever attaches next, and nothing is flushed on the way out. |
| `__init__(self, path: str, bits: int, hashes: int) -> None` | method | `bits` is the size of the filter and `hashes` how many bits each item sets. `suggest` works both out from the number of items and the rate of wrong yeses that can be lived with. |

## BloomFilter

| Attribute | Type |
|---|---|
| `false_positive_rate` | `float` |
| `n_bits` | `int` |
| `n_hashes` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self) -> None` | method | Clear every bit, so the filter is empty again and `false_positive_rate` goes back to nothing. The size and the hash count are unchanged, and every process mapping the file sees it. |
| `contains(self, item: bytes) -> bool` | method | `False` means definitely absent, `True` means probably present at about `false_positive_rate`. The same answer `in` gives, named so a caller can pass it around rather than write the operator. |
| `contains_many(self, items: Sequence[bytes]) -> list[bool]` | method | Look a run of items up in one crossing. |
| `insert(self, item: bytes) -> None` | method | Add an item. |
| `insert_many(self, items: Sequence[bytes]) -> None` | method | Insert a run of items in one crossing. |
| `open(path: str, n_bits: int, n_hashes: int) -> BloomFilter` | staticmethod | Attach to a filter that already exists, raising `OSError` when it does not. `n_bits` and `n_hashes` must be the ones it was created with, because together they decide which bits an item touches. |
| `suggest_config(n_items: int, false_positive_rate: float) -> tuple[int, int]` | staticmethod | The bits and hash count for `n_items` at a false-positive rate of `p`, so a caller sizes the filter from what it means rather than from arithmetic it has to do itself. |
| `__contains__(self, item: bytes) -> bool` | method | `False` means it is definitely absent. `True` means it is probably present, at about `false_positive_rate`. |
| `__init__(self, path: str, n_bits: int, n_hashes: int) -> None` | method | Obtain the filter at `path` with `n_bits` bits and `n_hashes` hash functions, creating it when the file does not exist. Either being zero is a `ValueError`. |

## BroadcastRing

| Attribute | Type |
|---|---|
| `active_consumers` | `int` |
| `capacity` | `int` |
| `payload_size` | `int` |
| `producer_position` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `lag(self, consumer: int) -> int | None` | method | How many items are waiting for a consumer, or `None` when the id names no live consumer of this ring. |
| `open(path: str, capacity: int) -> BroadcastRing` | staticmethod | Attach to a broadcast ring that already exists, raising `OSError` when it does not. `capacity` must be the one it was created with. |
| `push(self, item: bytes) -> bool` | method | Publish one item, which every registered consumer will see. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Publish a run of items, stopping at the first that will not fit, and answer how many landed. A count below what was handed in means the rest were not published and are still the caller's to keep. |
| `recv(self, consumer: int) -> bytes | None` | method | Read the next item for `consumer`, or `None` when it has caught up with the producer. |
| `recv_many(self, consumer: int, max_items: int) -> list[bytes]` | method | Read up to `max_items` for `consumer` in one call. |
| `register_consumer(self) -> int` | method | Take a consumer position. Every registered consumer sees every item published after it registered. |
| `unregister_consumer(self, consumer: int) -> None` | method | Give up a consumer position, so the producer stops holding slots for it. |
| `wait_for_consumers(self, want: int, timeout: float=5.0) -> int` | method | Wait until `want` consumers have registered, and answer how many there are when the wait ends. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. A consumer position taken inside the block is still registered after it: give it up with `unregister_consumer` rather than relying on the block to do it. |
| `__init__(self, path: str, capacity: int) -> None` | method | Obtain the broadcast ring at `path` holding `capacity` slots, creating it when the file does not exist and attaching to what is already there when it does. |

## Capacity

| Attribute | Type |
|---|---|
| `available` | `float | None` |
| `link_capacity` | `float | None` |
| `samples` | `tuple[int, int]` |
| `train_rate` | `float | None` |

| Method | Kind | What it does |
|---|---|---|
| `observe_pair(self, index: int, arrived_microseconds: float) -> None` | method | Feed the arrival of one probe of a pair, numbered within the pair, in microseconds. |
| `observe_train(self, arrived_microseconds: float) -> None` | method | Feed the arrival of one probe of a train, in microseconds. |
| `reset(self) -> None` | method | Forget everything and start again, which is what a caller does when the path may have changed. |
| `__init__(self, probe_bytes: int=1400) -> None` | method | `probe_bytes` is how big each probe is. |

## CapacityRing

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `inversions` | `int` |
| `ordering_mode` | `str | None` |
| `pin_generation` | `int` |
| `stale_pops` | `int` |
| `stamped` | `bool` |
| `warm_capacity` | `int | None` |
| `warm_hits` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `clear_warm(self) -> None` | method | Drop the prewarmed backing, giving back its memory and its file. |
| `morph_to(self, capacity: int) -> None` | method | Resize. Items already in the ring stay readable through the old backing until they have been taken, so nothing in flight is lost. |
| `open(path: str, capacity: int, max_producers: int=1, max_consumers: int=1, stamped: bool=False) -> CapacityRing` | staticmethod | `stamped` must match how the ring was created: a stamped ring carries ordering stamps and attaching to one as unstamped reads the wrong shape. |
| `prewarm(self, capacity: int) -> None` | method | Build a backing of `capacity` ahead of needing it, so the morph that switches to it does not pay for the allocation. |
| `recv(self, consumer: int) -> bytes | None` | method | Take the next item for `consumer`, or `None` when there is nothing to take. |
| `recv_many(self, consumer: int, max_items: int) -> list[bytes]` | method | Take up to `max_items` for `consumer` in one crossing, stopping early when the ring runs dry. An empty list means there was nothing, which is the same answer `recv` gives as `None`. |
| `register_consumer(self) -> int` | method | Take a consumer position, which every `recv` names. |
| `register_producer(self) -> int` | method | Take a producer position, which every `send` names. A ring built with one producer has exactly one to take, and asking past `max_producers` raises rather than answering. |
| `send(self, producer: int, item: bytes) -> bool` | method | Send one item from `producer`. `False` means the ring is full, which is an answer rather than a failure; an item too long for a slot raises. |
| `send_many(self, producer: int, items: Sequence[bytes]) -> int` | method | Send a run of items from one producer, stopping at the first that will not fit, and answer how many landed. A count short of what was handed in means the rest were not sent and are still the caller's to hold. |
| `set_ordering_mode(self, mode: str) -> None` | method | Set the ordering discipline across the current backing and every superseded one still draining, so a reader walking both applies one discipline. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. A prewarmed backing is still held afterwards, and a producer or consumer position taken inside is still registered: `clear_warm` gives back the first, and nothing gives back the second. |
| `__init__(self, path: str, capacity: int, max_producers: int=1, max_consumers: int=1, stamped: bool=False) -> None` | method | The capacity must be a power of two and at least two, which the ring arithmetic relies on. |

## CausalClock

| Attribute | Type |
|---|---|
| `nodes` | `int` |
| `counts` | `list[int]` |

| Method | Kind | What it does |
|---|---|---|
| `compare(self, other: CausalClock) -> str` | method | How this stands to another: `before`, `after`, `equal`, or `concurrent` when neither caused the other. |
| `concurrent_with(self, other: CausalClock) -> bool` | method | Whether neither caused the other. |
| `count(self, node: int) -> int` | method | One participant's count. |
| `happened_before(self, other: CausalClock) -> bool` | method | Whether this happened before the other, which is the same as `compare` answering `before`. |
| `merge(self, other: CausalClock) -> CausalClock` | method | The clock a receiver should hold after taking `other` in: the higher of each count. |
| `tick(self, node: int) -> CausalClock` | method | Step this participant's own count, which is what it does when something happens to it. |
| `__init__(self) -> None` | method | All counts at zero. |

## Cell

| Attribute | Type |
|---|---|
| `value_size` | `int` |
| `version` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. |
| `get(self) -> bytes` | method | Read the value, always `value_size` bytes. |
| `open(path: str, value_size: int) -> Cell` | staticmethod | Attach to a cell that already exists, raising `OSError` when it does not. `value_size` must be the one it was created with, because it is part of the layout rather than a hint. |
| `set(self, value: bytes) -> None` | method | Replace the value and step `version`, so a reader watching that number sees this write happened. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Nothing is flushed on the way out: call `flush` where durability matters. |
| `__init__(self, path: str, value_size: int) -> None` | method | Obtain the cell at `path` holding `value_size` bytes, creating it when the file does not exist and attaching to the value already there when it does. |

## Channel

| Attribute | Type |
|---|---|
| `max_item_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `open(path: str, capacity: int=1024) -> Channel` | staticmethod | Attach to a channel another holder made, with the capacity it was made with. |
| `recv(self) -> bytes | None` | method | The next item, or None when there is nothing there. |
| `recv_for(self, timeout: float | None=None) -> bytes | None` | method | Wait for the next item, or until `timeout` seconds have passed. None waits as long as it takes. Answers None on a timeout. |
| `recv_many(self, max_items: int=256) -> list[bytes]` | method | Everything waiting, up to `max_items`, in one crossing. |
| `send(self, item: bytes) -> bool` | method | Send one item, answering False when the channel is full. |
| `send_for(self, item: bytes, timeout: float | None=None) -> bool` | method | Wait until the item can be sent, or until `timeout` seconds have passed. None waits as long as it takes. Answers False on a timeout. |
| `send_many(self, items: Sequence[bytes]) -> int` | method | Send a run of items in one crossing, answering how many went. A short answer means the channel filled. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing the channel, and let an exception through. Nothing is signaled to the other end: a reader waiting on it goes on waiting, so a sender that means to say it is done has to say so in the messages it sends. |
| `__init__(self, path: str, capacity: int=1024, senders: int=1, readers: int=1) -> None` | method | `capacity` is the number of items in flight, rounded up to a power of two. `senders` and `readers` are how many are expected at once, which is what the shape underneath is picked from. |

## Clock

| Attribute | Type |
|---|---|
| `logical` | `int` |
| `physical` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `advance(self, physical: int) -> Clock` | method | The next reading given a new physical time. The count steps rather than resetting when the physical time has not moved. |
| `merge(self, received: Clock, physical: int) -> Clock` | method | The reading a receiver should take, given what arrived and what its own clock says. Orders the received event after whatever caused it. |
| `now() -> Clock` | staticmethod | A reading taken now, in microseconds since the epoch, with the count at zero. |
| `__init__(self, physical: int=0, logical: int=0) -> None` | method | A reading built from the two parts given, both defaulting to zero. |

## Condvar

| Attribute | Type |
|---|---|
| `generation` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `notify_all(self) -> int` | method | Wake every waiter. |
| `notify_one(self) -> int` | method | Wake at most one waiter. The caller is responsible for having made the predicate true first, which is the contract every condition variable has. |
| `open(path: str) -> Condvar` | staticmethod | Attach to a condition that already exists, raising `OSError` when it does not. |
| `wait_for(self, predicate: Callable[[], object], timeout: float | None=None) -> bool` | method | Wait until `predicate` is true, or until `timeout` seconds pass. |
| `__init__(self, path: str) -> None` | method | Obtain the condition at `path`, creating it when the file does not exist and attaching to the live one when it does. |

## CountMinSketch

| Attribute | Type |
|---|---|
| `depth` | `int` |
| `total_inserts` | `int` |
| `width` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `estimate_count(self, item: bytes) -> int` | method | How often it thinks it has seen `item`. Never less than the true count, sometimes more. |
| `estimate_many(self, items: Sequence[bytes]) -> list[int]` | method | One estimate per item, in the order they were given, from a single crossing rather than one per item. Each answer has the same never-under, sometimes-over property as `estimate_count`. |
| `insert(self, item: bytes) -> None` | method | Record one occurrence of `item`. |
| `insert_many(self, items: Sequence[bytes]) -> int` | method | Record one occurrence of each item in one crossing, and answer how many were handed in. Nothing here can refuse, so the count is always the length of the sequence. |
| `insert_n(self, item: bytes, count: int) -> None` | method | Add `count` occurrences at once rather than calling `insert` that many times. |
| `open(path: str, depth: int, width: int) -> CountMinSketch` | staticmethod | Attach to a sketch that already exists, raising `OSError` when it does not. `depth` and `width` must be the ones it was created with, because together they decide which cells an item touches. |
| `reset(self) -> None` | method | Zero every cell and the insert total, so the sketch is empty again. Every process mapping the file sees it. |
| `suggest_config(epsilon: float, delta: float) -> tuple[int, int]` | staticmethod | The depth and width for an error of `epsilon` with confidence `delta`, so a caller sizes the sketch from what it needs. |
| `__init__(self, path: str, depth: int, width: int) -> None` | method | Obtain the sketch at `path` with `depth` rows of `width` cells, creating it when the file does not exist. Either being zero is a `ValueError`. |

## Deque

| Attribute | Type |
|---|---|
| `approx_len` | `int` |
| `capacity` | `int` |
| `element_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the writes without this; flushing is about surviving a machine that stops. |
| `open_as_thief(path: str, element_size: int, alignment: int=1, tag: int=0) -> Deque` | staticmethod | Attach as a thief: this handle steals from the deque another process owns, and does not push or pop. |
| `pop(self) -> bytes | None` | method | Take from the owner's end, or `None`. |
| `push(self, item: bytes) -> bool` | method | Push at the owner's end. `False` means it is full. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Push a run of items in one crossing, stopping at the first that will not fit, and answer how many landed. They go on the owner's end, the end `pop` takes from and `steal` does not. |
| `steal(self) -> bytes | None` | method | Take from the other end, which is what another process does. `None` means there was nothing to take, or that another thief won the race for it. |
| `steal_many(self, max_items: int) -> list[bytes]` | method | Steal up to `max_items` from the far end in one crossing, stopping early when there is nothing left to take. An empty list means the deque was empty or the owner won every race for the last items, which a thief cannot tell apart and does not need to. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Nothing is drained on the way out: whatever is in the deque stays there for whoever attaches next, including for a thief still stealing from the other end. |
| `__init__(self, path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> None` | method | The capacity must be a power of two, which the ring arithmetic relies on; it is checked here so a bad one reads as an argument error rather than a refusal from deeper down. |

## EpochBarrier

| Attribute | Type |
|---|---|
| `arrived` | `int` |
| `current_epoch` | `int` |
| `live_peers` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `open(path: str, heartbeat: Heartbeat, grace_epochs: int=3) -> EpochBarrier` | staticmethod | Attach to a barrier that already exists, raising `OSError` when it does not. The heartbeat table and the grace period are this participant's own, given here the same way they are at creation. |
| `snapshot(self) -> tuple[int, int]` | method | The epoch and how many have arrived at it, read together so the two cannot disagree. |
| `wait(self, epoch: int, timeout: float | None=None, quorum: int | None=None) -> bool` | method | Arrive at `epoch` and wait for everyone else, or for `timeout` seconds. Returns `True` when the barrier opened and `False` on a timeout. The interpreter is detached while waiting. |
| `__init__(self, path: str, heartbeat: Heartbeat, grace_epochs: int=3) -> None` | method | Obtain the barrier at `path`, creating it when the file does not exist. |

## Epochs

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `now` | `int` |
| `open_tickets` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `advance(self) -> int` | method | Take the next epoch and return it, for a writer stamping a version as it supersedes the last. |
| `claim_ticket(self) -> tuple[int, int]` | method | Reserve the next epoch for a compound write, returning the slot holding the ticket and the epoch it took. |
| `dead_tickets(self) -> list[int]` | method | The epochs of tickets whose holders are gone, which is what makes a crashed reader stop holding reclamation up for ever. |
| `free_dead_ticket(self, epoch: int) -> bool` | method | Free a ticket whose holder died. `True` when one was freed. |
| `open(path: str, capacity: int) -> Epochs` | staticmethod | Attach to an epoch table that already exists, raising `OSError` when it does not. `capacity` must be the one it was created with. |
| `publish_ticket(self, slot: int) -> None` | method | Give a ticket back. |
| `__init__(self, path: str, capacity: int) -> None` | method | Obtain the epoch table at `path` with room for `capacity` open tickets, creating it when the file does not exist. A capacity of zero is a `ValueError`. |

## FenceClock

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `shared_clock_us` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `get_local(self, slot: int) -> tuple[int, int]` | method | This participant's current reading, without moving it. |
| `global_fence(self) -> tuple[int, int]` | method | The latest reading across every live participant: every event any of them has recorded is at or before this. |
| `merge(self, slot: int, physical_us: int, logical: int) -> tuple[int, int]` | method | Fold a reading received from elsewhere into this participant's clock, which is what makes the ordering hold across processes. |
| `open(path: str, capacity: int) -> FenceClock` | staticmethod | Attach to a clock that already exists, raising `OSError` when it does not. `capacity` must be the one it was created with. |
| `register(self, pid: int | None=None) -> int` | method | Take a participant slot. Every tick and merge names one. |
| `tick(self, slot: int) -> tuple[int, int]` | method | Move this participant's clock on and return the new reading. |
| `unregister(self, slot: int) -> None` | method | Give a participant slot back, so another process can take it. The readings it contributed stay in the shared clock: giving up the slot stops this participant advancing, it does not rewind what it already published. |
| `__init__(self, path: str, capacity: int) -> None` | method | Obtain the clock at `path` with room for `capacity` participants, creating it when the file does not exist. A capacity of zero is a `ValueError`. |

## FineBloom

| Attribute | Type |
|---|---|
| `suggested_capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `contains(self, key: bytes) -> bool` | method | As `in`, spelled out. |
| `contains_many(self, keys: Sequence[bytes]) -> list[bool]` | method | Ask about a run of keys in one crossing, answering one `True` or `False` per key in the order they were given. |
| `insert(self, key: bytes) -> None` | method | Add a key, setting its bits. Nothing can be taken out again, and the rate of wrong yeses climbs past `suggested_capacity` keys. |
| `insert_many(self, keys: Sequence[bytes]) -> int` | method | Add a run of keys in one crossing, and answer how many were handed in. Nothing here can refuse, so the count is always the length of the sequence. |
| `__contains__(self, key: bytes) -> bool` | method | `False` means the key is definitely absent, `True` means it is probably present. |
| `__init__(self, keys: Sequence[bytes] | None=None) -> None` | method | An empty filter, or one already holding `keys`. |

## Forecast

| Attribute | Type |
|---|---|
| `mean_rate` | `float` |
| `next_rate` | `float` |

| Method | Kind | What it does |
|---|---|---|
| `observe(self, bytes: int, seconds: float) -> None` | method | Feed one interval: how many bytes arrived and how long it was, in seconds. |
| `__init__(self) -> None` | method | A forecast with nothing observed yet. It lives in this process and has no file behind it. Feed it whole intervals with `observe`, each one a byte count and how long it covered. |

## FrameRegion

| Attribute | Type |
|---|---|
| `block_count` | `int` |
| `block_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `allocate(self) -> int | None` | method | Take a block, or `None` when every block is in use. |
| `free(self, index: int) -> None` | method | Give a block back. Freeing one nobody holds corrupts the pool, so only free what this process allocated and has finished with. |
| `open(path: str, block_size: int, block_count: int) -> FrameRegion` | staticmethod | Attach to a frame region that already exists, raising `OSError` when it does not. The block size and count must be the ones it was created with: they are the layout, so a mismatch reads the blocks at the wrong offsets. |
| `read_block(self, index: int, length: int) -> bytes` | method | Read `length` bytes from a block. |
| `take_block(self, index: int, length: int) -> bytes` | method | Read a block and give it back in one call, which is the shape a consumer wants. |
| `write_block(self, index: int, payload: bytes) -> None` | method | Write `payload` into block `index`, checking first that it fits. |
| `write_new(self, payload: bytes) -> int | None` | method | Take a block and write `payload` into it in one call, or `None` when the pool is exhausted. This is the shape a producer wants: one crossing rather than an allocate and a write. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The blocks stay as they were written for whoever attaches next. |
| `__init__(self, path: str, block_size: int, block_count: int) -> None` | method | Obtain the frame region at `path` holding `block_count` blocks of `block_size` bytes, creating it when the file does not exist. |

## Graph

| Attribute | Type |
|---|---|
| `edge_count` | `int` |
| `max_edges` | `int` |
| `max_nodes` | `int` |
| `node_count` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `add_edge(self, source: int, target: int, value: int) -> int` | method | Add an edge from one node to another, carrying `value`, and answer its index. |
| `add_edges(self, edges: Sequence[tuple[int, int, int]]) -> list[int]` | method | Add a run of edges in one crossing, each a source, a target and a value. |
| `add_node(self, value: int) -> int` | method | Add a node carrying `value`, answering its index. |
| `add_nodes(self, values: Sequence[int]) -> list[int]` | method | Add a run of nodes in one crossing, answering their indexes. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the nodes and edges without this; flushing is about surviving a machine that stops. |
| `flush_async(self) -> None` | method | Start writing the mapping back to its file and return at once, without waiting for the write to land. Nothing is on disk yet when this returns; `flush` is the form that waits. |
| `neighbors(self, source: int) -> list[tuple[int, int, int]]` | method | Everything reachable in one step from a node, as the edge index, the node it leads to and what the edge carries. |
| `node_value(self, node: int) -> int | None` | method | What a node carries, or None when there is no such node. |
| `open(path: str, max_nodes: int, max_edges: int) -> Graph` | staticmethod | Attach to a graph another holder made, with the sizes it was made with. |
| `out_degree(self, source: int) -> int | None` | method | How many edges lead out of a node, or None when there is no such node. |
| `remove_edge(self, source: int, edge: int) -> int | None` | method | Take an edge out of a node's edges, answering what it carried, or None when that node has no such edge. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Node and edge indices handed out inside the block still name the same nodes and edges after it, and nothing is flushed on the way out. |
| `__init__(self, path: str, max_nodes: int, max_edges: int) -> None` | method | The graph lives in two files beside the path given, one for the nodes and one for the edges. |

## HandleTable

| Attribute | Type |
|---|---|
| `max_value_bytes` | `int` |
| `capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `contains(self, handle: int) -> bool` | method | As `in`, spelled out. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the entries without this; flushing is about surviving a machine that stops. |
| `flush_async(self) -> None` | method | Start writing the mapping back to its file and return at once, without waiting for the write to land. Nothing is on disk yet when this returns; `flush` is the form that waits. |
| `get(self, handle: int) -> bytes | None` | method | The value behind a handle, or None when it has been removed or its place given to something else. |
| `get_many(self, handles: Sequence[int]) -> list[bytes | None]` | method | Several values in one crossing, each None where the handle no longer names anything. |
| `insert(self, value: bytes) -> int` | method | Put a value in and get its handle. Raises when the table is full. |
| `insert_many(self, values: Sequence[bytes]) -> list[int]` | method | Put a run of values in, answering their handles in order. Stops at the first one that does not fit, so a short answer means the table filled. |
| `open(path: str, capacity: int) -> HandleTable` | staticmethod | Attach to a table another holder made, with the capacity it was made with. |
| `remove(self, handle: int) -> bytes | None` | method | Take a value out, answering it, or None when the handle no longer names anything. |
| `reset(path: str, capacity: int) -> HandleTable` | staticmethod | Empty the table and remake it at this capacity. |
| `__contains__(self, handle: int) -> bool` | method | Whether the handle is still live. A handle that has been given back answers `False`, and so does one from an earlier generation of the same slot, which is what the generation in a handle is for. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Handles taken inside the block are still live after it: they are named by the table, not owned by this object. |
| `__init__(self, path: str, capacity: int) -> None` | method | How many values the table holds at most. A value is at most 44 bytes. |
| `__len__(self) -> int` | method | How many handles are live, which is not the capacity. A handle given back stops being counted at once. |

## HashMap

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `key_size` | `int` |
| `tombstones` | `int` |
| `value_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self) -> None` | method | Remove every entry and every tombstone, so both `len` and `tombstones` go to zero and the whole capacity is usable again. The widths are unchanged, and every process mapping the file sees it. This is the only thing that clears tombstones. |
| `compare_exchange(self, key: bytes, expected: bytes, new: bytes) -> tuple[bool, bytes]` | method | Replace `expected` with `new` only if that is what is there. Returns whether it swapped and what was found. |
| `get(self, key: bytes) -> bytes | None` | method | What `key` holds, or `None` when it holds nothing. The value is always `value_size` bytes. Use `get_many` for a run of keys: it costs one crossing rather than one per key. |
| `get_many(self, keys: Sequence[bytes]) -> list[bytes | None]` | method | Look a run of keys up in one call, answering `None` per key that is absent. |
| `insert(self, key: bytes, value: bytes) -> str | None` | method | Insert or replace. Returns `"inserted"` or `"updated"`, and `None` when the map is full. |
| `insert_many(self, pairs: Sequence[tuple[bytes, bytes]]) -> int` | method | Insert a run of pairs, stopping at the first the map refuses. |
| `open(path: str, capacity: int, key_size: int, value_size: int) -> HashMap` | staticmethod | Attach to a map that already exists, raising `OSError` when it does not. The capacity and both widths must be the ones it was created with. |
| `remove(self, key: bytes) -> bytes | None` | method | Remove a key and return what it held, or `None` if it held nothing. |
| `__contains__(self, key: bytes) -> bool` | method | Whether the key is present, without bringing its value back over the boundary. Cheaper than `get` when the value is not wanted. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The entries stay in the file for whoever attaches next. |
| `__init__(self, path: str, capacity: int, key_size: int, value_size: int) -> None` | method | Obtain the map at `path` holding `capacity` entries of `key_size` and `value_size` bytes, creating it when the file does not exist. |
| `__len__(self) -> int` | method | How many entries the map holds, which is not the capacity and does not count tombstones: a removed key stops being counted here while its marker still occupies a slot. Read `tombstones` for those. |

## Heartbeat

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `global_epoch` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `beat(self, slot: int) -> None` | method | Say this slot is still alive. A slot that stops beating for longer than the grace period counts as dead. |
| `open(path: str, capacity: int) -> Heartbeat` | staticmethod | Attach to a heartbeat table that already exists, raising `OSError` when it does not. `capacity` must be the one it was created with. |
| `register(self, pid: int | None=None) -> int` | method | Take a slot. A full table raises rather than answering `None`: it is a configuration that cannot serve this process, not an answer to a question. |
| `snapshot(self, slot: int) -> dict[str, int] | None` | method | What a slot currently says about itself, or `None` when nothing holds it. |
| `tick_global_epoch(self) -> int` | method | Step the global epoch and answer the new value, not the old one. |
| `unregister(self, slot: int) -> None` | method | Give a slot back, so another process can register into it. |
| `__init__(self, path: str, capacity: int) -> None` | method | Obtain the heartbeat table at `path` with `capacity` slots, creating it when the file does not exist. A capacity of zero is a `ValueError`. |

## Histogram

| Attribute | Type |
|---|---|
| `boundaries` | `list[int]` |
| `counts` | `list[int]` |
| `n_buckets` | `int` |
| `total_count` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `count(self, bucket: int) -> int` | method | How many values fell in one bucket, by its index. A bucket past `n_buckets` is an `OSError`. Use `counts` when you want them all: it costs one crossing rather than one per bucket. |
| `open(path: str, boundaries: Sequence[int]) -> Histogram` | staticmethod | Attach to a histogram that already exists, raising `OSError` when it does not. `boundaries` must be the ones it was created with: they are the bucket edges written into the file, so a different list reads the counts against the wrong edges. |
| `percentile(self, p: float) -> int` | method | The value at percentile `p`, from the bucket boundaries, so it is as precise as the buckets are. |
| `record(self, value: int) -> int` | method | Record one value and return the bucket it fell in. |
| `record_many(self, values: Sequence[int]) -> int` | method | Record a run of values in one crossing. |
| `__init__(self, path: str, boundaries: Sequence[int]) -> None` | method | `boundaries` must rise and must not be empty, which is checked here so a bad one names itself. |

## Hold

| Attribute | Type |
|---|---|
| `held` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `release(self) -> None` | method | Give the hold back now rather than at the end of a block. Calling it twice is harmless. |
| `__enter__(self) -> Self` | method | Answer the same object, which is why a hold is normally taken as `with lock.write() as held:` rather than bound to a name. |
| `__exit__(self, *args: object) -> bool` | method | Give the lock back, and let an exception through. |

## HolderTable

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `live` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `claim(self, payload: int) -> int | None` | method | Take a slot and put `payload` in it, or `None` when every slot is taken. |
| `open(path: str, capacity: int) -> HolderTable` | staticmethod | Attach to a holder table that already exists, raising `OSError` when it does not. `capacity` must be the one it was created with. |
| `payload(self, slot: int) -> int | None` | method | What a slot holds, or `None` when nobody holds it. |
| `publish(self, slot: int, payload: int) -> None` | method | Put a payload in a slot this caller already holds. |
| `release(self, slot: int) -> None` | method | Give a slot back, so the next `claim` or `reserve` can hand it to somebody else. A process that exits without releasing leaves its slot held, and nothing here reclaims it. |
| `reserve(self) -> int | None` | method | Take a slot without publishing anything into it yet. |
| `__init__(self, path: str, capacity: int) -> None` | method | Obtain the holder table at `path` with `capacity` slots, creating it when the file does not exist. A capacity of zero is a `ValueError`, because a table with no slots can serve nobody. |

## HyperLogLog

| Attribute | Type |
|---|---|
| `n_registers` | `int` |
| `precision` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `estimate(self) -> int` | method | How many distinct items it estimates it has seen. An estimate, not a count: that is the bargain the structure makes. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the registers without this; flushing is about surviving a machine that stops. |
| `insert(self, item: bytes) -> None` | method | Record having seen `item`. |
| `insert_many(self, items: Sequence[bytes]) -> int` | method | Insert a run of items in one crossing, which is what this structure is usually fed: a stream rather than single items. |
| `open(path: str, precision: int=14) -> HyperLogLog` | staticmethod | Attach to a counter that already exists, raising `OSError` when it does not. `precision` must be the one it was created with, because it sets how many registers the file holds. |
| `reset(self) -> None` | method | Zero every register, so the counter is empty again and `estimate` answers nothing seen. Every process mapping the file sees it. |
| `__init__(self, path: str, precision: int=14) -> None` | method | `precision` sets the trade between memory and accuracy: 4 is the smallest the format allows and 16 the largest. |

## KvMap

| Method | Kind | What it does |
|---|---|---|
| `get(self, key: int) -> int | None` | method | What a key holds, or None when it holds nothing. |
| `get_many(self, keys: Sequence[int]) -> list[int | None]` | method | Several keys in one crossing. |
| `insert(self, key: int, value: int) -> bool` | method | Put an entry in, answering True when the key was not there before and False when this replaced what it held. |
| `insert_many(self, entries: Sequence[tuple[int, int]]) -> list[bool]` | method | Put a run of entries in, in one crossing, answering True for each key that was not there before. |
| `__contains__(self, key: int) -> bool` | method | Whether the key has a value. Reads the value and throws it away, so it costs what `get` costs; call `get` when you want the value as well rather than asking twice. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The entries stay in the file for whoever attaches next. |
| `__init__(self, path: str, capacity: int=1024, readers: int=1, writers: int=1) -> None` | method | Obtain the map at `path`, creating it when the file does not exist. Keys and values are both sixty-four bit integers. |
| `__len__(self) -> int` | method | How many keys the map holds. |

## LamportConsumer

| Attribute | Type |
|---|---|
| `capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `pop(self) -> bytes | None` | method | Take the next item, or `None` when the ring is empty. Items arrive in the order the producer sent them, because there is exactly one producer. |
| `pop_many(self, max_items: int) -> list[bytes]` | method | Take up to `max_items` in one crossing, stopping early when the ring runs dry. An empty list means there was nothing, which is the same answer `pop` gives as `None`. |

## LamportProducer

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `payload_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `push(self, item: bytes) -> bool` | method | Push one item. `False` means the ring is full and the consumer has not caught up, which is an answer rather than a failure; an item longer than `payload_size` raises. |
| `push_buffer(self, data: bytes, item_len: int) -> int` | method | Push items cut out of one buffer, `item_len` bytes each, and answer how many landed. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Push a run of items in one crossing, stopping at the first that will not fit, and answer how many landed. A count short of what was handed in leaves the rest with the caller. |

## LaneClaim

| Attribute | Type |
|---|---|
| `held` | `bool` |
| `index` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `insert(self, key: int, value: int) -> int | None` | method | Put an entry in this lane at the next epoch, answering what the key held before. |
| `insert_at(self, key: int, value: int, born: int) -> int | None` | method | Put an entry in at a named epoch, so every write of one change becomes visible together. |
| `insert_many(self, entries: Sequence[tuple[int, int]]) -> list[int | None]` | method | Put a run of entries in this lane in one crossing. |
| `release(self) -> None` | method | Give the lane back now rather than at the end of a block. Calling it twice is harmless. |
| `remove(self, key: int) -> int | None` | method | Mark a key in this lane as no longer current, answering what it held. Raises `WrongLane` when the key lives in another lane. |
| `remove_at(self, key: int, died: int) -> int | None` | method | As `remove`, at a named epoch. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Give the lane back, and let an exception through. |

## LanedMap

| Attribute | Type |
|---|---|
| `held_lanes` | `int` |
| `lanes` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `claim_lane(self) -> LaneClaim` | method | Claim a free lane to write keys that are not in the map yet. Raises `Contended` when every lane is held. |
| `claim_lane_for(self, key: int) -> LaneClaim` | method | Claim the lane a key already lives in, to rewrite or remove it. Raises `KeyError` when no lane holds the key, and `Contended` when its lane is held by someone else. Retry rather than writing elsewhere: elsewhere is a different tree. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Every lane goes at once, not only the ones this process has claimed. Another process mapping the same file sees the entries without this; flushing is about surviving a machine that stops. |
| `get(self, key: int) -> int | None` | method | What a key holds now, from whichever lane holds it. |
| `get_many(self, keys: Sequence[int]) -> list[int | None]` | method | Several keys in one crossing. |
| `lane_of(self, key: int) -> int | None` | method | Which lane holds a key, or None when no lane does. |
| `open(directory: str, lanes: int=4, nodes_per_lane: int=256, max_pins: int=16) -> LanedMap` | staticmethod | Attach to a laned map another holder made, with the lane count, lane size and pin count it was made with. |
| `pin(self) -> LanedPin` | method | Take a pin, fixing one epoch to scan every lane at. |
| `reap_dead_claims(self) -> int` | method | Give back the lanes of writers whose process has gone, answering how many came back. Without this a process that died holding a lane keeps it forever. |
| `sweep(self) -> int` | method | Take away every entry marked as no longer current that nothing can still reach, across every lane. Zero means nothing could be taken. |
| `void_epoch(self, epoch: int) -> int` | method | Undo every write stamped at exactly this epoch across every lane, answering how many entries were touched. For a writer that died partway through a change spanning several lanes. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. A `LaneClaim` or `LanedPin` taken inside the block is not given back here; each belongs in a `with` block of its own. |
| `__init__(self, directory: str, lanes: int=4, nodes_per_lane: int=256, max_pins: int=16) -> None` | method | The map lives in a directory of its own, one file per lane plus the shared epochs and the claims. `nodes_per_lane` is how many entries each lane holds and `max_pins` how many readers may scan at once across all of them. |
| `__len__(self) -> int` | method | Entries across every lane, counting the ones marked as no longer current. |

## LanedPin

| Attribute | Type |
|---|---|
| `epoch` | `int` |
| `held` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `get(self, key: int) -> int | None` | method | What a key held when the pin was taken, from whichever lane holds it. |
| `get_many(self, keys: Sequence[int]) -> list[int | None]` | method | Several keys in one crossing. |
| `release(self) -> None` | method | Give the pin back now rather than at the end of a block. |
| `scan(self, low: int | None=None, high: int | None=None, limit: int=1024) -> list[tuple[int, int]]` | method | The entries between two keys, in key order, merged across every lane, as they stood when the pin was taken. Both ends are inclusive; None at either means no bound there. The limit counts entries walked per lane. |
| `scan_from(self, low: int | None=None, high: int | None=None, limit: int=1024) -> tuple[list[tuple[int, int]], int | None]` | method | As `scan`, and also the last key the walk reached in each lane, to carry on from. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Give the pin back, and let an exception through. |

## LazyValue

| Attribute | Type |
|---|---|
| `ready` | `bool` |
| `value_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `claim(self, pid: int | None=None) -> bool` | method | Claim the right to compute the value. `True` means this caller won and must publish; `False` means someone else is doing it and this caller should wait. |
| `get(self) -> bytes | None` | method | The value, or `None` when nobody has published one yet. |
| `open(path: str, value_bytes: int) -> LazyValue` | staticmethod | Attach to a lazy value that already exists, raising `OSError` when it does not. `value_bytes` must be the size it was created with. Attaching says nothing about whether anything has been published yet: `ready` answers that. |
| `publish(self, value: bytes, pid: int | None=None) -> bool` | method | Publish the value this caller claimed. `False` means it was not this caller's to publish. |
| `wait(self, timeout: float=30.0) -> bytes` | method | Wait for whoever claimed it to publish, up to `timeout` seconds. The interpreter is detached while waiting. |
| `__init__(self, path: str, value_bytes: int) -> None` | method | Obtain the lazy value at `path` holding `value_bytes` bytes, creating it when the file does not exist. Zero bytes is a `ValueError`. |

## LeaderElection

| Attribute | Type |
|---|---|
| `global_epoch` | `int` |
| `leader` | `int | None` |
| `term` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `am_i_leader(self, pid: int | None=None) -> bool` | method | Whether `pid` holds the role right now, defaulting to this process. This only asks. `beat` is what keeps a claim alive, and asking never renews one. |
| `beat(self, pid: int | None=None) -> bool` | method | Say the leader is still alive. `False` means this process is not the leader any more, which is the answer a former leader needs. |
| `open(path: str) -> LeaderElection` | staticmethod | Attach to an election that already exists, raising `OSError` when it does not. |
| `step_down(self, pid: int | None=None) -> bool` | method | Give the role up so another process can take it without waiting out the grace period. |
| `tick_epoch(self) -> int` | method | Step the global epoch and answer the new value, not the old one. |
| `try_claim(self, pid: int | None=None, grace_epochs: int=3) -> bool` | method | Try to take the role. `True` means this process now holds it and must keep beating; `False` means someone else holds it and is still alive. |
| `__init__(self, path: str) -> None` | method | Obtain the election at `path`, creating it when the file does not exist. Creating one claims nothing: `try_claim` is what takes the role. |

## LeaseHold

| Attribute | Type |
|---|---|
| `held` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `beat(self) -> bool` | method | Say this process is still here, so the claim does not lapse during a long block. |
| `read(self) -> bytes | None` | method | The value under the lease this hold has. |
| `release(self) -> None` | method | Give the lease back now rather than at the end of a block. Calling it twice is harmless. |
| `write(self, value: bytes) -> bool` | method | Write the value under the lease this hold has. |
| `__enter__(self) -> Self` | method | Answer the same object, which is why a hold is normally taken as `with lease.acquire() as held:` rather than bound to a name. |
| `__exit__(self, *args: object) -> bool` | method | Give the lease back, and let an exception through. |

## LinkedList

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `element_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `get(self, index: int) -> bytes` | method | Read the node at `index`. |
| `open(path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> LinkedList` | staticmethod | Attach to a list that already exists, raising `OSError` when it does not. Every layout argument describes the file rather than asking anything of it, so all of them must match. |
| `pop_back(self) -> bytes | None` | method | Take from the back, or `None` when the list is empty. |
| `pop_front(self) -> bytes | None` | method | Take from the front, or `None` when the list is empty. |
| `push_back(self, value: bytes) -> int` | method | Add at the back and return the index of the node holding it. |
| `push_back_many(self, values: Sequence[bytes]) -> list[int]` | method | Add a run of values at the back in one crossing, and answer the node index of each, in order. |
| `push_front(self, value: bytes) -> int` | method | Add at the front and return the index of the node holding it. |
| `remove(self, index: int) -> bytes` | method | Unlink the node at `index` and return what it held. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The nodes stay linked for whoever attaches next, and a node index taken inside the block is still valid after it. |
| `__init__(self, path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> None` | method | Obtain the list at `path` with room for `capacity` nodes of `element_size` bytes, creating it when the file does not exist. |
| `__len__(self) -> int` | method | How many nodes the list holds, which is not the capacity. An empty list is falsy, so `if not list:` works. |

## LocaleRing

| Attribute | Type |
|---|---|
| `inversions` | `int` |
| `locale` | `str` |
| `locale_generation` | `int` |
| `ordering_mode` | `str | None` |
| `stamped` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `migrate_to(self, locale: str) -> None` | method | Move the ring to another locale, carrying what is already in it. On a stamped ring the transfer keeps the order every sender saw; on an unstamped one the drain can interleave senders, as a shape change can. |
| `open(path: str, capacity: int, max_producers: int=1, max_consumers: int=1, stamped: bool=False) -> LocaleRing` | staticmethod | Attach to a ring another holder created. The counts, the capacity and `stamped` must all be the ones it was created with. |
| `recv(self, consumer: int) -> bytes | None` | method | Take the next item for `consumer`, or `None` when there is nothing to take. |
| `recv_many(self, consumer: int, max_items: int) -> list[bytes]` | method | Take up to `max_items` for `consumer` in one crossing, stopping early when the ring runs dry. An empty list means there was nothing, which is the same answer `recv` gives as `None`. |
| `register_consumer(self) -> int` | method | Take a consumer position, which every `recv` names. Registered on all three backings at once, like a producer, so a migration does not lose it. |
| `register_producer(self) -> int` | method | Register on all three backings at once, so the registration is there whichever locale is live. |
| `send(self, producer: int, item: bytes) -> bool` | method | Send one item from `producer` into whichever locale is live. `False` means the ring is full, which is an answer rather than a failure; an item too long for a slot raises. |
| `send_many(self, producer: int, items: Sequence[bytes]) -> int` | method | Send a run of items from one producer, stopping at the first that will not fit, and answer how many landed. A count short of what was handed in means the rest were not sent and are still the caller's to hold. |
| `set_ordering_mode(self, mode: str) -> None` | method | Set the ordering discipline on all three backings, so a migration does not change the discipline underneath a reader. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The ring stays in whichever locale it was migrated to, and a position taken inside the block is still registered. |
| `__init__(self, path: str, capacity: int, max_producers: int=1, max_consumers: int=1, stamped: bool=False) -> None` | method | The capacity must be a power of two and at least two, which the ring arithmetic relies on. All three backings are built here, so a later migration has somewhere to go without allocating. |

## LossBursts

| Attribute | Type |
|---|---|
| `mean_run_length` | `float | None` |
| `samples` | `int` |
| `steady_loss` | `float | None` |
| `transition_rates` | `tuple[float, float] | None` |

| Method | Kind | What it does |
|---|---|---|
| `observe(self, lost: bool) -> None` | method | Feed one item: True when it was lost. |
| `observe_many(self, losses: Sequence[bool]) -> int` | method | Feed a run of items in one crossing. |
| `__init__(self) -> None` | method | A model with nothing observed yet. It lives in this process and has no file behind it. Feed it with `observe` or `observe_many`, one call per item, saying whether that item was lost. |

## LossKind

| Attribute | Type |
|---|---|
| `congestion_share` | `float` |
| `delay_spread` | `float` |

| Method | Kind | What it does |
|---|---|---|
| `classify(self, gap: int, spacing_microseconds: float) -> str` | method | What a loss of `gap` items with this spacing was: `wireless` or `congestion`. |
| `observe_delay(self, microseconds: float) -> None` | method | Feed a one-way delay, in microseconds. |
| `observe_spacing(self, microseconds: float) -> None` | method | Feed the spacing between two arrivals, in microseconds. |
| `__init__(self) -> None` | method | A sensor with nothing observed yet. It lives in this process and has no file behind it, so two processes each keep their own. Feed it with `observe_spacing` and `observe_delay` before asking it anything. |

## LruCache

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `key_size` | `int` |
| `value_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `get(self, key: bytes) -> bytes | None` | method | Read without changing the eviction order. |
| `get_and_touch(self, key: bytes) -> bytes | None` | method | Read and count it as use, which is what a cache client wants. |
| `get_many(self, keys: Sequence[bytes]) -> list[bytes | None]` | method | Read a run of keys without disturbing the order. |
| `open(path: str, capacity: int, key_size: int, value_size: int) -> LruCache` | staticmethod | Attach to a cache that already exists, raising `OSError` when it does not. The capacity and both widths must be the ones it was created with. |
| `put(self, key: bytes, value: bytes) -> bool` | method | Put a key in at the most recent end, evicting the least recently used first if the cache is full. `True` means the key was already present and its value was replaced. |
| `put_many(self, pairs: Sequence[tuple[bytes, bytes]]) -> int` | method | Put a run of pairs in, and answer how many. |
| `remove(self, key: bytes) -> bytes | None` | method | Take a key out and answer the value it held, or `None` when it was not there. The freed room goes to the next `put`. |
| `touch(self, key: bytes) -> bool` | method | Count a key as used without reading it. |
| `__contains__(self, key: bytes) -> bool` | method | Whether the key is present, without counting as use. Asking does not save an entry from eviction; `touch` is what does that. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The entries stay in the file for whoever attaches next, which is the point of a cache that outlives the process. |
| `__init__(self, path: str, capacity: int, key_size: int, value_size: int) -> None` | method | Obtain the cache at `path` holding `capacity` entries of `key_size` and `value_size` bytes, creating it when the file does not exist. A capacity of zero is a `ValueError`. |
| `__len__(self) -> int` | method | How many entries the cache holds, which is at most the capacity because a full cache evicts rather than growing. |

## MapPin

| Attribute | Type |
|---|---|
| `epoch` | `int` |
| `held` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `get(self, key: int) -> int | None` | method | What a key held when the pin was taken, or None when it held nothing then. |
| `get_many(self, keys: Sequence[int]) -> list[int | None]` | method | Several keys in one crossing. |
| `release(self) -> None` | method | Give the pin back now rather than at the end of a block. Calling it twice is harmless. |
| `scan(self, low: int | None=None, high: int | None=None, limit: int=1024) -> list[tuple[int, int]]` | method | The entries between two keys, in key order, at most `limit` of them, as they stood when the pin was taken. `low` and `high` are inclusive; None at either end means no bound there. |
| `scan_from(self, low: int | None=None, high: int | None=None, limit: int=1024) -> tuple[list[tuple[int, int]], int | None]` | method | As `scan`, and also the last key the walk reached, to carry on from. A None there means the walk ran out of entries. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Give the pin back, and let an exception through. |

## MpmcConsumer

| Attribute | Type |
|---|---|
| `approx_len` | `int` |
| `rings` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `pop(self) -> bytes | None` | method | Take the next item from one of the rings this consumer was given, or `None` when they are all empty. |
| `pop_many(self, max_items: int) -> list[bytes]` | method | Take up to `max_items` in one crossing from this consumer's own rings, stopping early once they are empty. An empty list means there was nothing, which is the same answer `pop` gives as `None`. |

## MpmcProducer

| Attribute | Type |
|---|---|
| `capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `push(self, item: bytes) -> bool` | method | Push one item into this producer's own ring. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Push a run of items in one crossing, stopping at the first that will not fit, and answer how many landed. A count short of what was handed in leaves the rest with the caller. |

## MpscConsumer

| Attribute | Type |
|---|---|
| `approx_len` | `int` |
| `producers` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `pop(self) -> bytes | None` | method | Take the next item from whichever producer's ring has one, or `None` when they are all empty. |
| `pop_many(self, max_items: int) -> list[bytes]` | method | Take up to `max_items` in one crossing, stopping early once every ring is empty. An empty list means there was nothing, which is the same answer `pop` gives as `None`. |

## MpscProducer

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `payload_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `push(self, item: bytes) -> bool` | method | Push one item into this producer's own ring. |
| `push_buffer(self, data: bytes, item_len: int) -> int` | method | Push items cut out of one buffer, `item_len` bytes each, and answer how many landed. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Push a run of items in one crossing, stopping at the first that will not fit, and answer how many landed. A count short of what was handed in leaves the rest with the caller. |

## Notifier

| Attribute | Type |
|---|---|
| `index` | `int` |
| `is_signaled` | `bool` |
| `native` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `drain(self) -> None` | method | Clear a pending signal, so the next wait blocks rather than returning at once on a signal already consumed. |
| `wait(self, timeout: float | None=None) -> bool` | method | Wait for a signal, up to `timeout` seconds, and say whether one arrived. The interpreter is detached while waiting, so other Python threads keep running. |

## NotifierSet

| Attribute | Type |
|---|---|
| `attached` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `attach(self) -> Notifier` | method | Attach a notifier of this process's own. |
| `signal(self) -> int` | method | Wake every attached notifier, and say how many were signaled. |
| `__init__(self, path: str) -> None` | method | Obtain the notifier set at `path`, creating it when the file does not exist. Attaching does not by itself give this process a notifier: call `attach` for that. |

## OrderedReceiver

| Attribute | Type |
|---|---|
| `corrections` | `int` |
| `strategy` | `str` |

| Method | Kind | What it does |
|---|---|---|
| `drain(self, max_items: int=4096) -> list[tuple[bytes, int]]` | method | Everything the ring holds now plus everything the window still holds back, in order, in one crossing. |
| `flush(self) -> tuple[bytes, int] | None` | method | The next item held back in the window, or None once the window is empty. Call this in a loop at the end of a stream. |
| `flush_all(self) -> list[tuple[bytes, int]]` | method | Everything still held, in order, for a caller that would rather end a stream in one crossing than a loop of them. |
| `recv(self) -> tuple[bytes, int] | None` | method | The next item and its stamp, or None while the window is still filling or nothing is waiting. |

## OwnerLease

| Attribute | Type |
|---|---|
| `max_value_bytes` | `int` |
| `owner` | `int | None` |
| `term` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `beat(self, pid: int | None=None) -> bool` | method | Say this process is still here, so its claim does not lapse. Answers False once it is no longer the owner. |
| `flush(self) -> None` | method | Put the lease's file on the disk, and wait for it. |
| `flush_async(self) -> None` | method | Ask for the lease's file to reach the disk without waiting. |
| `held_by_me(self, pid: int | None=None) -> bool` | method | Whether this process holds it. |
| `hold(self, grace_epochs: int=0, pid: int | None=None) -> LeaseHold` | method | Take the lease for the length of a block, giving it back when the block ends including on the way out of an exception. |
| `open(path: str) -> OwnerLease` | staticmethod | Attach to a lease that already exists, failing if it does not. |
| `read(self, pid: int | None=None) -> bytes | None` | method | The value, readable only by the process holding the lease. None when this process does not hold it. |
| `release(self, pid: int | None=None) -> bool` | method | Give the lease back, answering False if this process did not hold it. |
| `reset(path: str, value: bytes | None=None) -> OwnerLease` | staticmethod | Strip the lease at `path` back to no owner and this value. This throws away a claim another process may still believe it has, so it is for a lease known to be wedged rather than for ordinary use. |
| `tick_epoch(self) -> int` | method | Step the epoch every holder measures the grace period in, and give back the epoch this reached. Nothing steps it on its own. |
| `try_acquire(self, grace_epochs: int=0, pid: int | None=None) -> bool` | method | Take the lease, answering False when a process with a lower id holds it and has beaten within `grace_epochs`. |
| `write(self, value: bytes, pid: int | None=None) -> bool` | method | Write the value, answering False when this process does not hold the lease. |
| `__init__(self, path: str, value: bytes | None=None) -> None` | method | Take the lease at `path`, creating it with `value` when it is not there yet. Attaching to one that exists leaves its owner, its term and its value alone, and `value` is then unused. |

## PathChanges

| Attribute | Type |
|---|---|
| `last` | `tuple[int, int, int] | None` |
| `marked_share` | `float` |
| `route_movement` | `float` |

| Method | Kind | What it does |
|---|---|---|
| `observe(self, ttl: int, congestion_mark: int, hops: int) -> None` | method | Feed one item: its remaining time to live, its congestion marking, and how many hops it took. |
| `__init__(self) -> None` | method | A sensor with nothing observed yet. It lives in this process and has no file behind it. Feed it with `observe`, one call per item received, and it infers from the spread of what it is told. |

## Periodicity

| Attribute | Type |
|---|---|
| `period` | `tuple[float, float] | None` |
| `seconds_to_next` | `float | None` |

| Method | Kind | What it does |
|---|---|---|
| `observe(self, delay_microseconds: float, at_microseconds: int) -> None` | method | Feed one delay, in microseconds, and when it was taken, also in microseconds. |
| `__init__(self) -> None` | method | A sensor with nothing observed yet. It lives in this process and has no file behind it. Each `observe` needs both the delay and when it was taken, because a beat can only be found against time. |

## PermitHold

| Attribute | Type |
|---|---|
| `held` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `release(self) -> None` | method | Give the permit back now rather than at the end of a block. Calling it twice is harmless, and `held` says whether it is still out. A genuine failure is raised rather than dropped: releasing more permits than the semaphore allows is a fault in the caller's bookkeeping, not a condition to ignore. |
| `__enter__(self) -> Self` | method | Answer the same object, which is why a permit is normally taken as `with semaphore.acquire() as permit:` rather than bound to a name. |
| `__exit__(self, *args: object) -> bool` | method | Give the permit back. A refusal here is reported rather than dropped: releasing more permits than the semaphore allows is a real fault in the caller's bookkeeping. |

## PubSub

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `head` | `int` |
| `payload_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `open(path: str, capacity: int) -> PubSub` | staticmethod | Attach to a pubsub ring that already exists, raising `OSError` when it does not. `capacity` must be the one it was created with. Attaching does not subscribe: take a subscription of your own. |
| `publish(self, item: bytes) -> int` | method | Publish one item and return the position it landed at. The publisher never waits for a subscriber. |
| `publish_many(self, items: Sequence[bytes]) -> int | None` | method | Publish a run of items in one crossing, returning the position of the last. |
| `read_at(self, position: int) -> bytes | None` | method | Read the item at `position`, `None` when nothing has been published there yet, and `Lagged` when it has already been overwritten. |
| `subscribe(self) -> Subscriber` | method | A subscriber starting where the publisher is now, so it sees what follows and nothing that came before. |
| `subscribe_from(self, position: int) -> Subscriber` | method | A subscriber starting at `position`, for one replaying from a place it recorded earlier. |
| `__init__(self, path: str, capacity: int) -> None` | method | Obtain the pubsub ring at `path` holding `capacity` items, creating it when the file does not exist. |

## QosPolicy

| Attribute | Type |
|---|---|
| `durability` | `str` |
| `reliability` | `str` |
| `keep_last` | `int | None` |
| `max_latency` | `float` |
| `ordering` | `str` |

| Method | Kind | What it does |
|---|---|---|
| `persistent_log() -> QosPolicy` | staticmethod | The settings for a stream that must survive the process: a mapped file, senders waiting, everything kept. |
| `reliable_pubsub() -> QosPolicy` | staticmethod | The settings for a stream nothing may fall out of: named memory other processes can reach, senders waiting rather than dropping. |
| `snapshot(self) -> QosSnapshot` | method | Every setting read together, so a decision is made against one consistent set rather than five separate reads. |
| `streaming() -> QosPolicy` | staticmethod | The settings for a stream that would rather lose an item than hold its sender up: in-process memory, dropping when full, the last thousand or so items, a tenth of a second. |
| `__init__(self, durability: str='volatile', reliability: str='best_effort', keep_last: int | None=1024, max_latency: float=0.1) -> None` | method | `durability` is `volatile`, `transient` or `persistent`. `reliability` is `best_effort` or `reliable`. |

## QosSnapshot

| Attribute | Type |
|---|---|
| `durability` | `str` |
| `keep_last` | `int | None` |
| `max_latency` | `float` |
| `ordering` | `str` |
| `reliability` | `str` |

| Method | Kind | What it does |
|---|---|---|
| `recommends_locale_change(self, current: str) -> str | None` | method | Where the bytes should live given how long they must last, or None when that is where they already are. The answer is one of `anon`, `file` or `shmfs`, which is what `LocaleRing.migrate_to` takes. |
| `recommends_ordering_change(self, current: str) -> str | None` | method | The ordering this asks for, when it is not the one already in force, and None when it is. |

## QuicBridgeClient

| Method | Kind | What it does |
|---|---|---|
| `run(self, items: int) -> None` | method | Connect and ship `items` of them, waiting until the reading end has acknowledged the last of them. |
| `__init__(self, ring: Ring, server: tuple[str, int], cert: bytes, server_name: str, local: tuple[str, int] | None=None) -> None` | method | `ring` is the ring to take items from, `server` the address of the reading end, and `cert` the certificate bytes that end was made with. `server_name` must be the name the certificate was issued for. |

## QuicBridgeServer

| Attribute | Type |
|---|---|
| `local_addr` | `tuple[str, int]` |

| Method | Kind | What it does |
|---|---|---|
| `accept_one(self) -> int` | method | Take one connection, read it to its end, and answer how many items arrived. |
| `__init__(self, ring: Ring, local: tuple[str, int], cert: bytes, key: bytes) -> None` | method | `ring` is the ring to put arriving items into, `local` the address to listen on, and `cert` and `key` the certificate and key this end proves itself with. A port of zero lets the system pick one, which `local_addr` then reports. |

## RWLock

| Attribute | Type |
|---|---|
| `readers` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `open(path: str) -> RWLock` | staticmethod | Attach to a lock that already exists, raising `OSError` when it does not. Attaching takes nothing: `read` and `write` are what take a hold. |
| `read(self) -> Hold` | method | Take the read hold, waiting for it. Other readers may hold it at the same time; a writer may not. |
| `read_for(self, timeout: float) -> Hold | None` | method | Take the read hold, giving up after `timeout` seconds and answering None. |
| `try_read(self) -> Hold | None` | method | Take the read hold if it is free, or answer `None`. Only a contended lock answers `None`; anything else raises, so a broken lock never reads as a busy one. |
| `try_write(self) -> Hold | None` | method | Take the write hold if it is free, or answer `None`. |
| `write(self) -> Hold` | method | Take the write hold, waiting for it. Nobody else holds it while this does. |
| `write_for(self, timeout: float) -> Hold | None` | method | Take the write hold, giving up after `timeout` seconds and answering None. |
| `__init__(self, path: str) -> None` | method | Obtain the lock at `path`, creating it when the file does not exist and attaching to the live one when it does. |

## RateLimiter

| Attribute | Type |
|---|---|
| `available` | `int` |
| `capacity` | `int` |
| `refill_per_second` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the token count without this. |
| `open(path: str, capacity: int, refill_per_second: int) -> RateLimiter` | staticmethod | Attach to a limiter that already exists, raising `OSError` when it does not. The capacity and refill rate must be the ones it was created with. |
| `reset(self) -> None` | method | Refill the bucket to full, which is the opposite of what the name suggests: this hands out capacity rather than taking it away. |
| `try_acquire(self, n: int=1) -> bool` | method | Take `n` tokens if they are there. `False` means they were not, which is an answer rather than a failure. |
| `__init__(self, path: str, capacity: int, refill_per_second: int) -> None` | method | Obtain the limiter at `path` holding at most `capacity` tokens and refilling `refill_per_second` of them each second, creating it when the file does not exist. Either being zero is a `ValueError`. |

## Region

| Attribute | Type |
|---|---|
| `capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `allocate(self, value: bytes) -> int` | method | Take a slot and write `value` into it, returning its index. |
| `get(self, index: int) -> bytes` | method | Read slot `index`. |
| `set(self, index: int, value: bytes) -> None` | method | Write slot `index`. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. A `memoryview` taken inside the block stays valid after it, because the view holds its own reference to the region and the export count is what keeps the mapping under it. |
| `__init__(self, path: str, capacity: int, slot_size: int, alignment: int=1, tag: int=0) -> None` | method | Obtain the region at `path` holding `capacity` slots of `slot_size` bytes, creating it when the file does not exist. |
| `__len__(self) -> int` | method | How many slots are allocated. |

## ReorderWindow

| Attribute | Type |
|---|---|
| `corrections` | `int` |
| `window` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> tuple[bytes, int] | None` | method | The next item regardless of how full the window is, or None once nothing is held. This is how a stream ends. |
| `flush_all(self) -> list[tuple[bytes, int]]` | method | Everything still held, in order, in one crossing. |
| `push(self, stamp: int, payload: bytes) -> None` | method | Hold one item, with the stamp its sender gave it. |
| `push_many(self, items: Sequence[tuple[int, bytes]]) -> int` | method | Hold a run of items in one crossing. Each is a stamp and a payload. |
| `take(self) -> tuple[bytes, int] | None` | method | The next item and its stamp, or None while fewer than `window` items are held. |
| `widen_to(self, window: int) -> None` | method | Widen the window to at least this, which is what a caller does when the number of senders grows. |
| `__init__(self, floor: int=8, cap: int=1024) -> None` | method | `floor` is the window to start at and `cap` the widest it may grow to. A window at least as wide as the number of senders puts every item back in order. |
| `__len__(self) -> int` | method | How many items are held in the window waiting for a gap to fill, which is not how many have passed through. An empty window is falsy, so `if not window:` works. |

## Reservoir

| Attribute | Type |
|---|---|
| `max_value_bytes` | `int` |
| `capacity` | `int` |
| `total_seen` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the sample without this; flushing is about surviving a machine that stops. |
| `flush_async(self) -> None` | method | Start writing the mapping back to its file and return at once, without waiting for the write to land. Nothing is on disk yet when this returns; `flush` is the form that waits. |
| `open(path: str, capacity: int) -> Reservoir` | staticmethod | Attach to a reservoir another holder made. The capacity must be the one it was made with. |
| `record(self, value: bytes) -> int | None` | method | Offer one value. Answers the place it was kept in, or None when the sample kept what it already had there instead. Neither answer is a failure: refusing is how the sample stays unbiased. |
| `record_many(self, values: Sequence[bytes]) -> int` | method | Offer a run of values in one crossing, and say how many were kept. |
| `reset(self) -> None` | method | Empty the sample and forget how much has been offered. |
| `snapshot(self) -> list[bytes]` | method | Everything in the sample right now, in one crossing. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The sample and the total seen both stay for whoever attaches next; `reset` is what empties a reservoir. |
| `__init__(self, path: str, capacity: int) -> None` | method | How many items the sample holds at most. A value is at most 52 bytes. |
| `__len__(self) -> int` | method | How many values are held in the sample right now, which is at most the capacity and is not `total_seen`. A reservoir that has been offered a million values still holds only its capacity. |

## Ring

| Attribute | Type |
|---|---|
| `approx_len` | `int` |
| `capacity` | `int` |
| `max_consumers` | `int` |
| `max_producers` | `int` |
| `morph_refusals` | `int` |
| `shape` | `str` |
| `stamped` | `bool` |
| `stamps` | `str | None` |
| `total_capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `is_empty(self) -> bool` | method | Whether the ring looked empty when asked. Read without stopping the producers, so it is a sighting rather than a promise: a send can land the instant after. Treat `None` from `recv` as the real answer, and use this for reporting. |
| `open(path: str, capacity: int, max_producers: int=1, max_consumers: int=1, stamps: str | None=None) -> Ring` | staticmethod | Attach to a ring another holder created. `stamps` must be the kind it was created with. |
| `ordered_receiver(self, consumer: int) -> OrderedReceiver` | method | A reader that hands items back in the order their senders made them, rather than the order they happened to arrive in. |
| `recv(self, consumer: int) -> bytes | None` | method | Receive one item as `consumer`, or `None` when the ring is empty. |
| `recv_frame(self, consumer: int) -> bytes | None` | method | Receive a framed payload, or `None` when there is none waiting. |
| `recv_many(self, consumer: int, max_items: int) -> list[bytes]` | method | Receive up to `max_items` in one crossing. |
| `register_consumer(self) -> int` | method | Take a consumer id. Every receive names one. |
| `register_producer(self) -> int` | method | Take a producer id. Every send names one. |
| `send(self, producer: int, item: bytes) -> bool` | method | Send one item as `producer`. `False` means the ring was full. |
| `send_buffer(self, producer: int, data: bytes, item_len: int) -> int` | method | Send items packed end to end, `item_len` bytes each, with no Python object built per item. |
| `send_frame(self, producer: int, payload: bytes) -> bool` | method | Send a payload longer than a slot, carried in frames. |
| `send_many(self, producer: int, items: Sequence[bytes]) -> int` | method | Send a run of items, stopping at the first refusal. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Producer and consumer positions taken inside the block are still registered after it, and items still in the ring stay there for whoever attaches next. |
| `__init__(self, path: str, capacity: int, max_producers: int=1, max_consumers: int=1, stamps: str | None=None) -> None` | method | The constructor asserts that there is at least one producer and one consumer, so both are refused here first. |

## RoundTripShape

| Attribute | Type |
|---|---|
| `samples` | `int` |
| `two_groups` | `float | None` |
| `wireless_confidence` | `float` |

| Method | Kind | What it does |
|---|---|---|
| `observe(self, microseconds: float) -> None` | method | Feed one round trip, in microseconds. |
| `observe_many(self, microseconds: Sequence[float]) -> int` | method | Feed a run of round trips in one crossing. |
| `__init__(self) -> None` | method | A shape with nothing observed yet. It lives in this process and has no file behind it. Feed it with `observe` or `observe_many`, in microseconds, before asking it anything. |

## Semaphore

| Attribute | Type |
|---|---|
| `available` | `int` |
| `max_permits` | `int` |
| `waiters` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `acquire(self) -> PermitHold` | method | Take a permit, waiting for one. The interpreter is detached while this waits. |
| `acquire_for(self, timeout: float) -> PermitHold | None` | method | Take a permit, giving up after `timeout` seconds and answering None. |
| `open(path: str, max_permits: int) -> Semaphore` | staticmethod | Attach to a semaphore that already exists, raising `OSError` when it does not. `max_permits` must be the one it was created with, and attaching takes no permit: `acquire` is what does. |
| `try_acquire(self) -> PermitHold | None` | method | Take a permit if one is free, or answer `None`. Only an exhausted semaphore answers `None`; anything else raises. |
| `__init__(self, path: str, initial: int, max_permits: int | None=None) -> None` | method | The constructor asserts that the initial count fits the maximum, so that is refused here first rather than reaching Python as a panic. |

## SensReceiver

| Attribute | Type |
|---|---|
| `alive` | `bool` |
| `code` | `str` |
| `local_addr` | `tuple[str, int]` |
| `max_item_size` | `int` |
| `send_failures` | `int` |
| `switches` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `poll(self) -> list[bytes]` | method | Drive the link and answer every item it could rebuild this time round. An empty answer is ordinary and means nothing was ready, not that anything is wrong. |
| `poll_from(self) -> list[tuple[int, bytes]]` | method | As `poll`, and also which sender each item came from, for a reader taking from several at once. |
| `__init__(self, local: tuple[str, int], max_item_size: int, code: str | None=None) -> None` | method | `local` is the address to listen on, as host and port. `max_item_size` must be the one the sender was made with. |

## SensSender

| Attribute | Type |
|---|---|
| `code` | `str` |
| `datagrams` | `tuple[int, int]` |
| `local_addr` | `tuple[str, int]` |
| `loss` | `float | None` |
| `max_item_size` | `int` |
| `switches` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `send(self, item: bytes) -> None` | method | Send one item, of anything up to `max_item_size` bytes. |
| `send_many(self, items: Sequence[bytes]) -> int` | method | Send a run of items in one crossing, which is the shape to reach for: the work per item is small enough that the crossing would otherwise be most of the cost. |
| `__init__(self, local: tuple[str, int], peer: tuple[str, int], max_item_size: int, code: str | None=None) -> None` | method | `local` is the address to send from and `peer` the address to send to, each as host and port. `max_item_size` is the largest an item may be, and must be the same at both ends. |

## SharedArc

| Attribute | Type |
|---|---|
| `holders` | `int` |
| `value_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `get(self) -> bytes` | method | The whole value. |
| `open(path: str, value_bytes: int, max_holders: int=16, keep_on_last: bool=False) -> SharedArc` | staticmethod | Attach to a shared value that already exists, counting this process as another holder. |
| `read_at(self, offset: int, length: int) -> bytes` | method | Part of the value, for a caller that wants a field rather than the whole of a large record. |
| `write_at(self, offset: int, value: bytes) -> None` | method | Overwrite as many bytes as `value` carries, starting at `offset`. A range running past the end of the value is an `OSError` rather than a short write. The partner of `read_at`, for a caller that wants one field of a large record rather than all of it. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without letting go of the value, and let an exception through. |
| `__init__(self, path: str, value: bytes, max_holders: int=16, keep_on_last: bool=False) -> None` | method | The constructor asserts at least one holder, so that is refused here first. `keep_on_last` decides what happens when the last holder lets go: keep the file for a later process, or unlink it. |

## Slab

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `element_size` | `int` |
| `writable` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the slots without this; flushing is about surviving a machine that stops. |
| `get(self, index: int) -> bytes` | method | Read slot `index`, always `element_size` bytes. A slot nobody has written to reads as zeros rather than raising, because it exists from creation. An index past the capacity is an `OSError`. |
| `open(path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> Slab` | staticmethod | Attach to a slab that already exists, able to write into it. Raises `OSError` when it is not there, and every layout argument must be the one it was created with. |
| `open_read_only(path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> Slab` | staticmethod | Attach for reading only, so `set` and `write_range` raise and `writable` answers `False`. |
| `read_range(self, start: int, count: int) -> bytes` | method | Read `count` slots from `start` packed end to end, one crossing and one object, each slot read through its seqlock. |
| `set(self, index: int, value: bytes) -> None` | method | Overwrite slot `index`, stepping its seqlock either side so a reader racing this retries instead of seeing half of each value. An index past the capacity, or a slab opened read-only, is an `OSError`. |
| `slot_version(self, index: int) -> int` | method | The seqlock counter for a slot. Even means nobody is writing; the same value twice means no write landed between the two reads. |
| `write_range(self, start: int, data: bytes) -> int` | method | Write slots packed end to end from `start`. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Nothing is flushed on the way out: call `flush` where durability matters. |
| `__init__(self, path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> None` | method | Obtain the slab at `path` with `capacity` slots of `element_size` bytes, creating it when the file does not exist. |
| `__len__(self) -> int` | method | How many slots the slab has, which is its capacity and not a count of the ones written to. Every slot exists from creation, so this never changes and a slab is never empty. |

## SlabPin

| Attribute | Type |
|---|---|
| `epoch` | `int` |
| `held` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `get(self, slot: int) -> bytes | None` | method | The value in a slot as it stood when the pin was taken, or None when nothing was there then. |
| `get_many(self, slots: Sequence[int]) -> list[bytes | None]` | method | Several slots in one crossing, each None where nothing was there when the pin was taken. |
| `release(self) -> None` | method | Give the pin back now rather than at the end of a block. Calling it twice is harmless. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Give the pin back, and let an exception through. |

## SpscRing

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `payload_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `open(path: str, capacity: int) -> SpscRing` | staticmethod | Attach to a ring that already exists. `capacity` must be the one it was created with. |
| `pop(self) -> bytes | None` | method | Pop one item, or `None` when the ring is empty. |
| `pop_buffer(self, max_items: int) -> tuple[bytes, int]` | method | Pop up to `max_items` into one buffer packed end to end, and return how many were written. Nothing is allocated per item. |
| `pop_many(self, max_items: int) -> list[bytes]` | method | Pop up to `max_items`, stopping when the ring runs empty. |
| `push(self, item: bytes) -> bool` | method | Push one item. `False` means the ring was full, which is an answer rather than a failure; anything else raises. |
| `push_buffer(self, data: bytes, item_len: int) -> int` | method | Push items packed end to end in one buffer, `item_len` bytes each. No Python object is built per item, which is what makes this the fastest way in: the caller's own array goes straight across. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Push a run of items, stopping at the first the ring refuses. Returns how many went in, so the caller keeps the rest. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Nothing is drained: items still in the ring stay there for whoever attaches next, because the file outlives the process. |
| `__init__(self, path: str, capacity: int) -> None` | method | Obtain the ring at `path` holding `capacity` slots, creating it when the file does not exist. |

## Stack

| Attribute | Type |
|---|---|
| `approx_len` | `int` |
| `capacity` | `int` |
| `element_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the writes without this; flushing is about surviving a machine that stops. |
| `is_empty(self) -> bool` | method | Whether the stack looked empty when asked. Read without stopping anyone else, so it is a sighting rather than a promise: a push can land the instant after. Treat `None` from `pop` as the real answer, and use this for reporting. |
| `open(path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> Stack` | staticmethod | Attach to a stack that already exists, raising `OSError` when it does not. Every layout argument describes the file rather than asking anything of it, so all of them must match what it was created with. |
| `peek(self) -> bytes | None` | method | Look at the top without taking it. The bytes are a snapshot: a concurrent pop can retire the slot while this reads it. |
| `pop(self) -> bytes | None` | method | Take the top item, or `None` when it is empty. |
| `pop_many(self, max_items: int) -> list[bytes]` | method | Take up to `max_items` in one crossing, stopping early when the stack runs dry, newest first. An empty list means there was nothing, and nothing here raises. |
| `push(self, item: bytes) -> bool` | method | Push one item. `False` means the stack is full. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Push a run of items in one crossing, stopping at the first that will not fit, and answer how many landed. They go on in the order given, so the last one handed in is the first one `pop` returns. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Nothing is popped on the way out: whatever is on the stack stays there for whoever attaches next. |
| `__init__(self, path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> None` | method | Obtain the stack at `path` with room for `capacity` items of `element_size` bytes, creating it when the file does not exist. A capacity below one is a `ValueError`, and so is a layout the alignment cannot satisfy. |

## Subscriber

| Attribute | Type |
|---|---|
| `position` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `lag(self) -> int` | method | How far behind the publisher this subscriber is. |
| `next(self) -> bytes | None` | method | The next item, or `None` when this subscriber has caught up. Raises `Lagged` when the next item was overwritten before it was read. |
| `next_many(self, max_items: int) -> list[bytes]` | method | Up to `max_items` in one crossing, stopping when caught up. |

## TcpBridgeClient

| Method | Kind | What it does |
|---|---|---|
| `run(self, items: int) -> None` | method | Connect and ship `items` of them, waiting until all have gone. |
| `__init__(self, ring: Ring, server: tuple[str, int]) -> None` | method | `ring` is the ring to take items from and `server` the address of the reading end, as host and port. |

## TcpBridgeServer

| Attribute | Type |
|---|---|
| `local_addr` | `tuple[str, int]` |

| Method | Kind | What it does |
|---|---|---|
| `accept_one(self) -> int` | method | Take one connection, read it to its end, and answer how many items arrived. Waits until the sending end has finished. |
| `__init__(self, ring: Ring, local: tuple[str, int]) -> None` | method | `ring` is the ring to put arriving items into and `local` the address to listen on, as host and port. A port of zero lets the system pick one, which `local_addr` then reports. |

## TimePointTile

| Attribute | Type |
|---|---|
| `lanes` | `int` |
| `max_value_bytes` | `int` |
| `full` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `at(self, lane: int) -> tuple[int, bytes] | None` | method | The version and value in one place, or None when it is empty. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the points without this; flushing is about surviving a machine that stops. |
| `flush_async(self) -> None` | method | Start writing the mapping back to its file and return at once, without waiting for the write to land. Nothing is on disk yet when this returns; `flush` is the form that waits. |
| `insert(self, version: int, value: bytes) -> int` | method | Write a value at a version and get the place it went in. Raises when all sixteen places are taken. |
| `open(path: str) -> TimePointTile` | staticmethod | Attach to a tile another holder made. |
| `remove(self, lane: int) -> None` | method | Empty one place. |
| `reset(path: str) -> TimePointTile` | staticmethod | Empty the tile and remake it. |
| `visible(self, version: int) -> list[tuple[int, bytes]]` | method | Everything a reader at `version` can see, as version and value pairs, in one crossing. |
| `visible_count(self, version: int) -> int` | method | How many places a reader at `version` can see. |
| `visible_mask(self, version: int) -> int` | method | Which places a reader at `version` can see, as sixteen bits with the lowest standing for the first place. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The points recorded stay for whoever attaches next, and nothing is flushed on the way out. |
| `__init__(self, path: str) -> None` | method | A value is at most 52 bytes. |
| `__len__(self) -> int` | method | How many of the tile's sixteen places are taken. `full` is the same question asked the other way round. |

## Timing

| Attribute | Type |
|---|---|
| `clock_skew` | `float` |
| `jitter` | `float` |
| `samples` | `int` |
| `spacing` | `float` |
| `trend` | `float` |
| `trend_debiased` | `float` |

| Method | Kind | What it does |
|---|---|---|
| `observe(self, sent: int, received: int) -> None` | method | Feed one item's send and receive times, in microseconds. The two clocks need not agree with each other. |
| `__init__(self, window: int=64) -> None` | method | `window` is how many recent items the answers are taken over. |

## TinyBloom

| Attribute | Type |
|---|---|
| `suggested_capacity` | `int` |
| `bits` | `int` |
| `set_bits` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `contains(self, key: bytes) -> bool` | method | As `in`, spelled out. |
| `contains_many(self, keys: Sequence[bytes]) -> list[bool]` | method | Ask about a run of keys in one crossing. |
| `false_positive_rate(keys: int) -> float` | staticmethod | The share of wrong yeses to expect once `keys` keys are in, between zero and one. |
| `from_bits(bits: int) -> TinyBloom` | staticmethod | Rebuild a filter from the number `bits` gave. |
| `insert(self, key: bytes) -> None` | method | Add a key, setting its bits. Nothing can be taken out again, and the filter is only sixty-four bits wide, so the rate of wrong yeses climbs quickly past `suggested_capacity` keys. |
| `insert_many(self, keys: Sequence[bytes]) -> int` | method | Add a run of keys in one crossing. |
| `__contains__(self, key: bytes) -> bool` | method | `False` means the key is definitely absent, `True` means it is probably present. A filter this small says yes wrongly often once it holds more than a handful of keys. |
| `__init__(self, keys: Sequence[bytes] | None=None) -> None` | method | An empty filter, or one already holding `keys`. |

## TopologyMap

| Attribute | Type |
|---|---|
| `broadcast_root` | `int` |
| `busiest_receiver` | `tuple[int, int]` |
| `busiest_sender` | `tuple[int, int]` |
| `participants` | `int` |
| `recommendation_epoch` | `int` |
| `total_sends` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `fan_in(self, receiver: int) -> int` | method | How many different places send to this one. |
| `fan_out(self, sender: int) -> int` | method | How many different places this one sends to. |
| `open(path: str, participants: int) -> TopologyMap` | staticmethod | Attach to a topology another holder made, with the participant count it was made with. |
| `publish_recommendation(self) -> str` | method | Work the shape out and write it down, so every process reads the same one. Answers what was published. |
| `published_recommendation(self) -> str` | method | The shape last published, which may not be what the counts suggest now. |
| `recommend(self) -> str` | method | The shape the counts suggest, one of `point_to_point`, `broadcast_tree` or `all_to_all_mesh`. Reading this does not publish it. |
| `record_many(self, sends: Sequence[tuple[int, int]]) -> int` | method | Record a run of sends in one crossing. |
| `record_send(self, sender: int, receiver: int) -> int` | method | Record one send, answering how many have gone that way. |
| `reset(path: str, participants: int, fan_out_threshold: int | None=None, fan_in_threshold: int | None=None) -> TopologyMap` | staticmethod | Empty the counts and remake the map at this size, throwing away what every other holder has recorded. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Everything recorded stays counted for whoever attaches next; `reset` is what clears the observations. |
| `__init__(self, path: str, participants: int, fan_out_threshold: int | None=None, fan_in_threshold: int | None=None) -> None` | method | `participants` is how many there are, numbered from zero. |

## Tower

| Attribute | Type |
|---|---|
| `depth` | `int` |
| `value_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `append(self, value: bytes) -> list[int]` | method | Store a value, taking a fresh place on the top level, and answer the path that reaches it. |
| `append_many(self, values: Sequence[bytes]) -> list[list[int]]` | method | Store a run of values in one crossing, answering a path for each. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Every level goes, not just the bottom one. Another process mapping the same file sees the values without this; flushing is about surviving a machine that stops. |
| `get(self, path: Sequence[int]) -> bytes` | method | The value a path reaches. Raises when a level along the way no longer agrees with the path, naming that level. |
| `get_many(self, paths: Sequence[Sequence[int]]) -> list[bytes]` | method | Several paths in one crossing. |
| `insert_at_top(self, top: int, value: bytes) -> list[int]` | method | Store a value under a named place on the top level, rather than a fresh one, and answer the path that reaches it. |
| `open(path: str, capacity: int, value_size: int, levels: Sequence[tuple[str, int]]) -> Tower` | staticmethod | Attach to a tower another holder made, with the shape it was made with. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Paths handed out inside the block still resolve to the same values after it, and nothing is flushed on the way out. |
| `__init__(self, path: str, capacity: int, value_size: int, levels: Sequence[tuple[str, int]]) -> None` | method | `path` is where the bottom level lives and `levels` the levels above it, each a file and how many places it holds. The levels are given top first. With no levels the tower is one deep, which is a plain region reached by a path of one number. |
| `__len__(self) -> int` | method | Values stored at the bottom level. |

## Universal

| Attribute | Type |
|---|---|
| `generation` | `int` |
| `migrations` | `int` |
| `op_counts` | `tuple[int, int]` |
| `strategy` | `str` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self) -> None` | method | Take every value out, so the set is empty again. The storage strategy it had migrated to is kept rather than reset, and `migrations` goes on counting from where it was. |
| `contains(self, value: int) -> bool` | method | As `in`, spelled out. |
| `contains_many(self, values: Sequence[int]) -> list[bool]` | method | Ask about a run of values in one crossing. |
| `insert(self, value: int) -> None` | method | Add a value to the set. |
| `insert_many(self, values: Sequence[int]) -> int` | method | Add a run of values in one crossing. |
| `migrate_to(self, strategy: str) -> None` | method | Move the set to `list` or `map` by hand. Moving it to where it already is does nothing. |
| `open(path: str, capacity: int) -> Universal` | staticmethod | Attach to a set another holder made, with the capacity it was made with. |
| `reset(path: str, capacity: int) -> Universal` | staticmethod | Empty the set and remake it at this capacity. |
| `snapshot(self) -> list[int]` | method | Everything in the set, in one crossing. |
| `__contains__(self, value: int) -> bool` | method | Whether the value is in the set. Exact, not probabilistic: unlike the filters, a `True` here is a fact. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Whatever strategy it migrated to inside the block is the one it is still in afterwards. |
| `__init__(self, path: str, capacity: int) -> None` | method | The set lives in files beside the path given, one per way of storing it. |
| `__len__(self) -> int` | method | How many values the set holds. It can raise, unlike most `len` implementations, because reading the count means reading the mapping and that can fail. |

## Vec

| Attribute | Type |
|---|---|
| `capacity` | `int` |
| `element_size` | `int` |
| `writable` | `bool` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self) -> None` | method | Drop every element, so the length goes to zero. The capacity and the file are left as they are, and the space is reused by the next `push`. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees a write without this; flushing is about surviving a machine that stops. |
| `get(self, index: int) -> bytes | None` | method | Read element `index`, or `None` when it is past what is live. |
| `open(path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> Vec` | staticmethod | Attach to a vec that already exists, raising `OSError` when it does not. Every layout argument must be the one it was created with: they describe the file rather than asking anything of it. |
| `pop(self) -> bytes | None` | method | Take the last element, or `None` when the vec is empty. |
| `push(self, value: bytes) -> int | None` | method | Append one element, or `None` when the vec is full. |
| `push_many(self, values: Sequence[bytes]) -> int` | method | Append a run of elements, stopping at the first refusal, and say how many landed. |
| `read_range(self, start: int, count: int) -> bytes` | method | Read `count` elements from `start`, packed end to end into one object. Stops at the end of what is live. |
| `set(self, index: int, value: bytes) -> None` | method | Overwrite element `index`, stepping its seqlock either side so a reader racing this retries instead of seeing half of each value. |
| `write_range(self, start: int, data: bytes) -> int` | method | Write elements packed end to end into consecutive slots from `start`, and return how many landed. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. The elements stay where they are for whoever attaches next; `clear` is what empties a vec, not the end of a block. |
| `__init__(self, path: str, capacity: int, element_size: int, alignment: int=1, tag: int=0) -> None` | method | Obtain the vec at `path` holding up to `capacity` elements of `element_size` bytes, creating it when the file does not exist and attaching to what is already there when it does. |
| `__len__(self) -> int` | method | How many elements are live, which is not the capacity. A vec that has never been pushed to is empty, and `bool(vec)` is `False`. |

## VersionChain

| Attribute | Type |
|---|---|
| `max_value_bytes` | `int` |
| `capacity` | `int` |
| `current` | `tuple[int, bytes] | None` |

| Method | Kind | What it does |
|---|---|---|
| `clear(self) -> None` | method | Throw away every version. |
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the versions without this; flushing is about surviving a machine that stops. |
| `flush_async(self) -> None` | method | Start writing the mapping back to its file and return at once, without waiting for the write to land. Nothing is on disk yet when this returns; `flush` is the form that waits. |
| `open(path: str, capacity: int) -> VersionChain` | staticmethod | Attach to a chain another holder made, with the capacity it was made with. |
| `push(self, version: int, value: bytes) -> None` | method | Write a new version. The version number must be above the one already at the front, which is what keeps the history in order. |
| `read_at(self, version: int) -> bytes | None` | method | The value as it stood at `version`, or None when nothing had been written by then. |
| `reset(path: str, capacity: int) -> VersionChain` | staticmethod | Empty the chain and remake it at this capacity. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Every version written inside the block is still in the chain after it, and nothing is reclaimed on the way out. |
| `__init__(self, path: str, capacity: int) -> None` | method | How many versions the chain holds at most. A value is at most 44 bytes. |
| `__len__(self) -> int` | method | How many versions the chain holds, which is not the capacity and not the number of distinct values: one value written three times is three versions until something reclaims the older two. |

## VersionedMap

| Attribute | Type |
|---|---|
| `capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the entries without this; flushing is about surviving a machine that stops. |
| `get(self, key: int) -> int | None` | method | What a key holds now, or None when it holds nothing. |
| `get_many(self, keys: Sequence[int]) -> list[int | None]` | method | Several keys in one crossing, each None where the key holds nothing. |
| `insert(self, key: int, value: int) -> int | None` | method | Put an entry in at the next epoch, answering what the key held before, or None when it held nothing. |
| `insert_at(self, key: int, value: int, born: int) -> int | None` | method | Put an entry in at a named epoch, for a caller stepping the epochs itself. |
| `insert_many(self, entries: Sequence[tuple[int, int]]) -> list[int | None]` | method | Put a run of entries in, in one crossing, answering what each key held before. |
| `open(path: str, capacity: int, epochs_path: str, max_pins: int=16) -> VersionedMap` | staticmethod | Attach to a map another holder made, with the capacity and pin count it was made with. |
| `pin(self) -> MapPin` | method | Take a pin, fixing one epoch to scan the whole map at. Give it back when the block ends, or reclaiming cannot move past it. |
| `remove(self, key: int) -> int | None` | method | Mark a key as no longer current at the next epoch, answering what it held. Readers pinned earlier still see it. |
| `remove_at(self, key: int, died: int) -> int | None` | method | As `remove`, at a named epoch. |
| `sweep(self) -> int` | method | Take away every entry marked as no longer current that nothing can still reach, answering how many went. |
| `void_epoch(self, epoch: int) -> int` | method | Undo every write stamped at exactly this epoch, answering how many entries were touched. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. A `MapPin` taken inside the block is not given back here: pin it in its own `with` block, or the epoch it holds keeps `sweep` from reclaiming anything. |
| `__init__(self, path: str, capacity: int, epochs_path: str, max_pins: int=16) -> None` | method | `capacity` is how many entries the map holds, counting the ones marked as no longer current, and `max_pins` how many readers may scan at once. The epochs live in their own file, which other structures may share. |
| `__len__(self) -> int` | method | Entries the map holds, counting the ones marked as no longer current. |

## VersionedSlab

| Attribute | Type |
|---|---|
| `depth` | `int` |
| `max_value_bytes` | `int` |
| `capacity` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `flush(self) -> None` | method | Ask the operating system to write the mapping back to its file, and wait for it. Another process mapping the same file sees the versions without this; flushing is about surviving a machine that stops. |
| `get(self, slot: int) -> bytes | None` | method | The value in a slot now, or None when nothing live is there. |
| `history(self, slot: int) -> list[tuple[bytes, int, int | None]]` | method | A slot's history, newest first, as value, born and died. A died of None means the version is the current one. |
| `open(path: str, capacity: int, epochs_path: str, max_pins: int=16) -> VersionedSlab` | staticmethod | Attach to a slab another holder made, with the capacity and pin count it was made with. |
| `pin(self) -> SlabPin` | method | Take a pin, fixing one epoch to read the whole slab at. Give it back when the block ends, or reclaiming cannot move past it. |
| `retire(self, slot: int) -> bytes | None` | method | Mark a slot's current value as no longer current, at the next epoch, answering what it was. Readers pinned earlier still see it. |
| `retire_at(self, slot: int, died: int) -> bytes | None` | method | As `retire`, at a named epoch. |
| `set(self, slot: int, value: bytes) -> None` | method | Write a slot, at the next epoch. |
| `set_at(self, slot: int, value: bytes, born: int) -> None` | method | Write a slot at a named epoch, for a caller stepping the epochs itself. |
| `sweep_slot(self, slot: int) -> int` | method | Throw away every version of a slot that nothing can still see, answering how many went. |
| `void_epoch(self, epoch: int) -> int` | method | Undo every write stamped at exactly this epoch, across the whole slab, answering how many versions were touched. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. A `SlabPin` taken inside the block is not given back here: pin it in its own `with` block, or the epoch it holds keeps reclamation waiting. |
| `__init__(self, path: str, capacity: int, epochs_path: str, max_pins: int=16) -> None` | method | `capacity` is how many slots there are and `max_pins` how many readers may hold a pin at once. The epochs live in their own file, which other structures may share. A value is at most 44 bytes. |

## WorkQueue

| Attribute | Type |
|---|---|
| `max_item_size` | `int` |

| Method | Kind | What it does |
|---|---|---|
| `pop(self) -> bytes | None` | method | Take the owner's own next piece of work, the most recent one it added, or None when there is none. |
| `push(self, item: bytes) -> bool` | method | Add work, at the owner's end. |
| `push_many(self, items: Sequence[bytes]) -> int` | method | Add a run of work in one crossing, answering how many went in. |
| `steal(self) -> bytes | None` | method | Take a piece of work from the far end, which is what a thief does, or None when there is none to take. |
| `steal_from(path: str) -> WorkQueue` | staticmethod | Attach to a queue somebody else owns, to take from it. |
| `steal_many(self, max_items: int=64) -> list[bytes]` | method | Take up to `max_items` by stealing, in one crossing. |
| `__enter__(self) -> Self` | method | Answer the same object, so a `with` block can give it a name. |
| `__exit__(self, *args: object) -> bool` | method | Leave the block without closing anything, and let an exception through. Work still in the queue stays there for whoever attaches next, and no worker is told to stop. |
| `__init__(self, path: str, capacity: int=1024, thieves: int=1) -> None` | method | `capacity` is the number of items in flight, rounded up to a power of two. `thieves` is how many are expected to take from it, which is what the shape underneath is picked from. |

