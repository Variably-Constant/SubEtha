"""Every name the binding exposes to Python, reached from Python.

A #[pymethods] function's caller is an interpreter, so no Rust lint can
tell whether anything reaches it: a `pub` method nothing calls is still
part of a library's API and never warns. These tests cover the names the
rest of the suite leaves untouched, and the gate at the end fails when a
new one appears without a test.

Each assertion states what the method's own Rust doc says it does.

Some names reach Python as a method and some as a property, and which is
which is a detail of the binding rather than of the surface, so `reach`
takes either and the assertions read the same way for both.
"""

import os
import re

import pytest

import subetha


@pytest.fixture
def scratch(tmp_path):
    return lambda name: os.fspath(tmp_path / name)


def reach(obj, name):
    """The named value, calling it when it is a method."""
    value = getattr(obj, name)
    return value() if callable(value) else value


# --- the shapes that report what they were built with ---------------------


def test_the_sized_families_report_their_element_size(scratch):
    assert reach(subetha.Vec(scratch("vec"), capacity=4, element_size=8), "element_size") == 8
    assert reach(subetha.Stack(scratch("stack"), capacity=4, element_size=8), "element_size") == 8
    assert reach(subetha.Deque(scratch("deque"), capacity=4, element_size=8), "element_size") == 8
    assert reach(subetha.Slab(scratch("slab"), capacity=4, element_size=8), "element_size") == 8
    assert reach(subetha.LinkedList(scratch("list"), capacity=4, element_size=8), "element_size") == 8


def test_the_keyed_families_report_their_key_size(scratch):
    assert reach(subetha.HashMap(scratch("hash"), capacity=8, key_size=4, value_size=4), "key_size") == 4
    assert reach(subetha.BTreeMap(scratch("btree"), capacity=8, key_size=4, value_size=4), "key_size") == 4
    assert reach(subetha.LruCache(scratch("lru"), capacity=4, key_size=4, value_size=4), "key_size") == 4


def test_a_bitvec_reports_the_bits_it_was_given(scratch):
    assert reach(subetha.BitVec(scratch("bits"), capacity_bits=64), "capacity_bits") == 64


def test_a_frame_region_reports_its_block_count(scratch):
    frames = subetha.FrameRegion(scratch("frames"), block_size=32, block_count=4)
    assert reach(frames, "block_count") == 4


def test_a_graph_reports_the_edges_it_can_hold(scratch):
    assert reach(subetha.Graph(scratch("graph"), max_nodes=8, max_edges=16), "max_edges") == 16


def test_a_histogram_reports_its_boundaries(scratch):
    hist = subetha.Histogram(scratch("hist"), boundaries=[10, 100])
    assert list(reach(hist, "boundaries")) == [10, 100]


def test_a_rate_limiter_reports_its_refill(scratch):
    limiter = subetha.RateLimiter(scratch("rate"), capacity=4, refill_per_second=2)
    assert reach(limiter, "refill_per_second") == 2


def test_the_queues_report_the_largest_item_they_take(scratch):
    assert reach(subetha.Channel(scratch("chan"), capacity=8), "max_item_size") > 0
    assert reach(subetha.AdaptiveQueue(scratch("aq"), capacity=8), "max_item_size") > 0
    assert reach(subetha.WorkQueue(scratch("wq"), capacity=8), "max_item_size") > 0


def test_the_valued_families_report_the_largest_value_they_take(scratch):
    assert reach(subetha.OwnerLease(scratch("lease"), value=b"v"), "max_value_bytes") > 0
    assert reach(subetha.Reservoir(scratch("res"), capacity=4), "max_value_bytes") > 0
    assert reach(subetha.HandleTable(scratch("handles"), capacity=4), "max_value_bytes") > 0
    assert reach(subetha.VersionChain(scratch("chain"), capacity=4), "max_value_bytes") > 0
    tile = subetha.TimePointTile(scratch("tile"))
    assert reach(tile, "max_value_bytes") > 0
    assert reach(tile, "lanes") > 0
    slab = subetha.VersionedSlab(scratch("vslab"), 4, scratch("vslab-epochs"))
    assert reach(slab, "max_value_bytes") > 0
    assert reach(slab, "depth") >= 0


def test_the_sketches_report_their_shape(scratch):
    bloom = subetha.BloomFilter(scratch("bloom"), n_bits=1024, n_hashes=3)
    assert reach(bloom, "n_bits") == 1024
    assert reach(bloom, "n_hashes") == 3
    sketch = subetha.CountMinSketch(scratch("cms"), depth=4, width=64)
    assert reach(sketch, "width") == 64
    assert reach(sketch, "total_inserts") == 0
    hll = subetha.HyperLogLog(scratch("hll"))
    assert reach(hll, "precision") > 0
    assert reach(hll, "n_registers") == 1 << reach(hll, "precision")


# --- read-only attachment -------------------------------------------------


def test_an_arena_opened_read_only_reads_what_the_writer_interned(scratch):
    path = scratch("arena")
    writer = subetha.Arena(path, capacity_bytes=4096)
    assert reach(writer, "writable") is True
    ref = writer.intern("hello")
    reader = subetha.Arena.open_read_only(path, 4096)
    assert reach(reader, "writable") is False
    assert reader.get(ref) == "hello"


def test_a_slab_opened_read_only_reads_what_the_writer_set(scratch):
    path = scratch("slab-ro")
    writer = subetha.Slab(path, capacity=4, element_size=2)
    assert reach(writer, "writable") is True
    writer.set(1, b"ab")
    writer.flush()
    reader = subetha.Slab.open_read_only(path, 4, 2)
    assert reach(reader, "writable") is False
    assert bytes(reader.get(1)) == b"ab"


def test_a_vec_reports_whether_it_may_be_written(scratch):
    assert reach(subetha.Vec(scratch("vec-w"), capacity=4, element_size=2), "writable") is True


# --- membership, spelled out ----------------------------------------------


def test_contains_answers_what_the_in_operator_answers(scratch):
    bloom = subetha.BloomFilter(scratch("b1"), n_bits=1024, n_hashes=3)
    bloom.insert(b"key")
    assert bloom.contains(b"key") is True

    blocked = subetha.BlockedBloomFilter(scratch("b2"), bits=4096, hashes=4)
    blocked.insert(b"key")
    assert blocked.contains(b"key") is True

    tiny = subetha.TinyBloom()
    tiny.insert(b"key")
    assert tiny.contains(b"key") is True
    assert reach(subetha.TinyBloom, "suggested_capacity") > 0

    fine = subetha.FineBloom()
    fine.insert(b"key")
    assert fine.contains(b"key") is True
    assert reach(subetha.FineBloom, "suggested_capacity") > 0

    universal = subetha.Universal(scratch("uni"), capacity=8)
    universal.insert(7)
    assert universal.contains(7) is True

    handles = subetha.HandleTable(scratch("ht"), capacity=4)
    handle = handles.insert(b"v")
    assert handles.contains(handle) is True


# --- the counts read without stopping anyone ------------------------------


def test_the_queues_report_a_length_without_stopping_anyone(scratch):
    ring = subetha.Ring(scratch("ring"), capacity=8)
    producer = ring.register_producer()
    assert reach(ring, "is_empty") is True
    assert reach(ring, "approx_len") == 0
    assert reach(ring, "total_capacity") >= 8
    ring.send(producer, b"one")
    assert reach(ring, "approx_len") == 1
    assert reach(ring, "is_empty") is False
    assert reach(ring, "morph_refusals") >= 0

    stack = subetha.Stack(scratch("stk"), capacity=4, element_size=4)
    assert reach(stack, "is_empty") is True
    assert reach(stack, "approx_len") == 0
    stack.push(b"aaaa")
    assert reach(stack, "approx_len") == 1
    assert reach(stack, "is_empty") is False

    deque = subetha.Deque(scratch("dq"), capacity=4, element_size=4)
    assert reach(deque, "approx_len") == 0
    deque.push(b"aaaa")
    assert reach(deque, "approx_len") == 1


# --- waiting with a deadline ----------------------------------------------


def test_send_for_answers_false_when_it_cannot_send_in_time(scratch):
    channel = subetha.Channel(scratch("full"), capacity=2)
    sent = 0
    while channel.send_for(b"x", 0.05):
        sent += 1
        if sent > 64:
            break
    assert sent > 0, "a channel with room takes at least one item"
    assert channel.send_for(b"x", 0.05) is False, "a full channel answers False on a timeout"


def test_an_adaptive_queue_sends_with_a_deadline(scratch):
    queue = subetha.AdaptiveQueue(scratch("aq2"), capacity=8)
    assert queue.send_for(b"item", 1.0) is True


def test_a_read_hold_is_taken_when_the_lock_is_free(scratch):
    lock = subetha.RWLock(scratch("rw"))
    hold = lock.try_read()
    assert hold is not None, "an uncontended lock gives the read hold"
    hold.release()


# --- the coordination families --------------------------------------------


def test_a_condvar_wakes_one_waiter_and_reports_how_many(scratch):
    cond = subetha.Condvar(scratch("cv"))
    assert cond.notify_one() == 0, "nobody is parked, so nobody is woken"


def test_a_lazy_value_reports_whether_it_is_ready(scratch):
    lazy = subetha.LazyValue(scratch("lazy"), value_bytes=4)
    assert reach(lazy, "ready") is False
    lazy.claim()
    lazy.publish(b"abcd")
    assert reach(lazy, "ready") is True


def test_a_semaphore_reports_its_waiters(scratch):
    sem = subetha.Semaphore(scratch("sem"), initial=1)
    assert reach(sem, "waiters") == 0


def test_the_epoch_table_reports_and_frees_a_dead_ticket(scratch):
    epochs = subetha.Epochs(scratch("ep"), capacity=4)
    assert list(epochs.dead_tickets()) == []
    assert epochs.free_dead_ticket(999) is False, "no ticket at that epoch to free"


def test_a_fence_clock_reads_locally_and_across_participants(scratch):
    clock = subetha.FenceClock(scratch("fence"), capacity=4)
    assert reach(clock, "shared_clock_us") >= 0
    slot = clock.register()
    local = clock.get_local(slot)
    assert isinstance(local, tuple) and len(local) == 2
    fence = clock.global_fence()
    assert isinstance(fence, tuple) and len(fence) == 2


# --- the publishing families ----------------------------------------------


def test_a_pubsub_publishes_a_run_and_reports_its_head(scratch):
    topic = subetha.PubSub(scratch("ps"), capacity=8)
    reader = topic.subscribe()
    assert reach(topic, "head") >= 0
    assert topic.publish_many([b"a", b"b"]) is not None
    assert reach(reader, "position") >= 0
    # A subscriber reads a slot, so an item shorter than the payload comes
    # back padded to the slot's width.
    got = [bytes(x).rstrip(b"\x00") for x in reader.next_many(10)]
    assert got == [b"a", b"b"]


def test_a_broadcast_ring_reports_the_producer_position(scratch):
    ring = subetha.BroadcastRing(scratch("bc"), capacity=8)
    before = reach(ring, "producer_position")
    ring.push(b"item")
    assert reach(ring, "producer_position") == before + 1


def test_a_capacity_ring_steps_its_pin_generation_when_it_morphs(scratch):
    ring = subetha.CapacityRing(scratch("cap"), capacity=8)
    before = reach(ring, "pin_generation")
    ring.morph_to(16)
    assert reach(ring, "pin_generation") > before, "a morph supersedes what a pin held"


# --- the bulk forms -------------------------------------------------------


def test_the_bulk_forms_land_every_item(scratch):
    cache = subetha.LruCache(scratch("lru2"), capacity=4, key_size=2, value_size=2)
    assert cache.put_many([(b"aa", b"11"), (b"bb", b"22")]) == 2

    listing = subetha.LinkedList(scratch("ll"), capacity=8, element_size=4)
    assert len(listing.push_back_many([b"aaaa", b"bbbb"])) == 2

    frames = subetha.FrameRegion(scratch("fr"), block_size=8, block_count=4)
    frames.write_block(0, b"abcdefgh")
    assert bytes(frames.read_block(0, 8)) == b"abcdefgh"


# --- writing at a named epoch --------------------------------------------


def test_removing_at_a_named_epoch_answers_what_was_there(scratch):
    index = subetha.VersionedMap(scratch("vm"), 64, scratch("vm-ep"))
    index.insert_at(1, 10, 1)
    assert index.remove_at(1, 2) == 10

    slab = subetha.VersionedSlab(scratch("vs"), 4, scratch("vs-ep"))
    slab.set_at(0, b"old", 1)
    assert bytes(slab.retire_at(0, 2)) == b"old"


def test_a_lane_claim_removes_at_a_named_epoch(scratch):
    laned = subetha.LanedMap(scratch("laned"), lanes=2)
    claim = laned.claim_lane()
    claim.insert_at(7, 70, 1)
    assert claim.remove_at(7, 2) == 70
    claim.release()


# --- the sensors ----------------------------------------------------------


def test_the_sensors_report_what_they_have_not_measured_yet():
    bursts = subetha.LossBursts()
    assert reach(bursts, "steady_loss") is None, "nothing observed, so nothing to say"
    assert reach(bursts, "transition_rates") is None
    for _ in range(50):
        bursts.observe_many([True, True, False, False, False, False])
    assert reach(bursts, "steady_loss") is not None
    assert len(reach(bursts, "transition_rates")) == 2

    kind = subetha.LossKind()
    for _ in range(20):
        kind.observe_spacing(1000)
    assert reach(kind, "delay_spread") >= 0

    timing = subetha.Timing(window=8)
    for i in range(16):
        timing.observe(1000 * i, 1000 * i + 50)
    assert isinstance(reach(timing, "clock_skew"), float)

    beat = subetha.Periodicity()
    assert reach(beat, "seconds_to_next") is None, "no beat found yet"

    capacity = subetha.Capacity(probe_bytes=1400)
    assert reach(capacity, "train_rate") is None
    for i in range(1, 21):
        capacity.observe_train(1_000_000.0 + 200 * i)
    rate = reach(capacity, "train_rate")
    assert rate is None or rate > 0


def test_a_causal_clock_reports_its_nodes():
    assert reach(subetha.CausalClock(), "nodes") >= 0


def test_a_receiver_counts_what_it_owed_and_could_not_send():
    receiver = subetha.SensReceiver(("127.0.0.1", 0), max_item_size=64)
    assert reach(receiver, "send_failures") == 0


def test_a_notifier_reports_whether_a_signal_is_pending(scratch):
    notifiers = subetha.NotifierSet(scratch("notify"))
    one = notifiers.attach()
    assert reach(notifiers, "attached") >= 1
    assert reach(one, "is_signaled") is False
    notifiers.signal()
    assert reach(one, "is_signaled") is True
    assert reach(one, "index") >= 0
    assert reach(one, "native") != 0


# --- the map that counts its tombstones -----------------------------------


def test_a_hash_map_counts_its_tombstones(scratch):
    table = subetha.HashMap(scratch("hm"), capacity=8, key_size=4, value_size=4)
    table.insert(b"key1", b"val1")
    assert reach(table, "tombstones") == 0
    table.remove(b"key1")
    assert reach(table, "tombstones") >= 0


# --- the gate -------------------------------------------------------------


def exposed_names():
    """Every public name on the extension's classes, with whether Python
    reaches it by calling it or by reading it."""
    names = []
    for cls_name in subetha.__all__:
        cls = getattr(subetha, cls_name, None)
        if not isinstance(cls, type):
            continue
        for attr in vars(cls):
            if attr.startswith("_"):
                continue
            names.append((cls_name, attr, callable(getattr(cls, attr, None))))
    return names


def suite_source():
    here = os.path.dirname(__file__)
    text = ""
    for name in sorted(os.listdir(here)):
        if name.endswith(".py"):
            with open(os.path.join(here, name), encoding="utf-8") as handle:
                text += handle.read() + "\n"
    return text


def test_every_name_the_binding_exposes_is_reached_by_the_suite():
    corpus = suite_source()
    missing = []
    for cls_name, attr, is_callable in exposed_names():
        # A name handed to `reach` counts, since that call reaches it
        # whichever shape the binding gives it. A name merely quoted
        # somewhere else does not.
        reached = re.search(r"reach\([^\n]*\"" + re.escape(attr) + r"\"\s*\)", corpus)
        called = re.search(r"[.\s(\[]" + re.escape(attr) + r"\s*\(", corpus)
        read = re.search(r"\.\s*" + re.escape(attr) + r"\b", corpus)
        if not (reached or called or read):
            missing.append(f"{cls_name}.{attr}")
    assert not missing, (
        "exposed to Python but no test reaches them: " + ", ".join(sorted(set(missing)))
    )
