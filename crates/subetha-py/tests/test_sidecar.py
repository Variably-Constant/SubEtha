"""The process's sidecar, reached from Python.

Three things are checked here. A managed ring runs a sidecar of its own
that resizes or moves it. An observed object has the process's sidecar
drain what it records into stats a caller can read. And a policy written
in Python decides, on the sidecar's own thread, which tag an object runs
at.

Where a sidecar thread has to get round to something, the test polls
for the outcome and gives up only at a generous deadline, so a busy
machine slows these tests rather than failing them. `scan_now` is used
wherever a test needs what was recorded counted before it goes on.
"""

import gc
import inspect
import os
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import subetha
from subetha import sidecar


@pytest.fixture
def scratch(tmp_path):
    return lambda name: os.fspath(tmp_path / name)


def eventually(check, seconds=10.0):
    """Whether `check()` came true before the deadline, polling for it."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(0.005)
    return check()


# --- managed rings ---------------------------------------------------------


def test_a_managed_ring_runs_a_shape_sidecar_and_a_strict_one_does_not(scratch):
    managed = subetha.Ring(scratch("managed"), 64, managed=True)
    assert managed.managed is True, "managed=True starts the ring's shape sidecar"
    opened = subetha.Ring.open(scratch("managed"), 64, managed=True, scan_interval_us=500)
    assert opened.managed is True, "an attaching handle runs a sidecar of its own"
    strict = subetha.Ring(scratch("strict"), 64)
    assert strict.managed is False
    assert strict.sidecar_morphs == 0, "a strict ring has no sidecar to morph it"


def test_a_stray_or_zero_interval_is_refused(scratch):
    with pytest.raises(ValueError, match="managed=True"):
        subetha.Ring(scratch("stray"), 64, scan_interval_us=250)
    with pytest.raises(ValueError, match="managed=True"):
        subetha.CapacityRing(scratch("stray-cap"), 64, scan_interval_us=250)
    with pytest.raises(ValueError, match="at least one microsecond"):
        subetha.Ring(scratch("zero"), 64, managed=True, scan_interval_us=0)


def test_a_managed_capacity_or_locale_ring_needs_an_interval(scratch):
    with pytest.raises(ValueError, match="names no default"):
        subetha.CapacityRing(scratch("cap"), 64, managed=True)
    with pytest.raises(ValueError, match="names no default"):
        subetha.LocaleRing(scratch("loc"), 64, managed=True)


def test_a_managed_capacity_ring_grows_once_it_fills(scratch):
    ring = subetha.CapacityRing(scratch("grow"), 64, managed=True, scan_interval_us=1000)
    assert ring.managed is True
    producer = ring.register_producer()
    ring.register_consumer()
    sent = ring.send_many(producer, [b"x"] * 64)
    assert sent >= 55, "the ring is filled past the 85 percent it grows at"
    assert eventually(lambda: ring.capacity > 64), "the sidecar never grew a full ring"
    assert ring.sidecar_morphs >= 1
    assert eventually(lambda: ring.sidecar_prewarms >= 1), (
        "a fill ratio held across scans has the sidecar build the next backing early"
    )


def test_a_strict_capacity_ring_changes_size_only_when_told(scratch):
    ring = subetha.CapacityRing(scratch("strict-grow"), 64)
    assert ring.managed is False
    producer = ring.register_producer()
    ring.register_consumer()
    ring.send_many(producer, [b"x"] * 64)
    # Five times the 100 ms a capacity sidecar waits between resizes: long
    # enough for one to have grown the ring, had one been running.
    time.sleep(0.5)
    assert ring.capacity == 64
    assert ring.sidecar_morphs == 0
    assert ring.sidecar_prewarms == 0


def test_a_managed_locale_ring_moves_where_it_is_asked(scratch):
    ring = subetha.LocaleRing(scratch("ask"), 64, managed=True, scan_interval_us=1000)
    assert ring.managed is True
    assert ring.locale == "anon"
    ring.request_locale("file")
    assert eventually(lambda: ring.locale == "file"), "the sidecar never moved the ring"
    assert ring.sidecar_migrations >= 1


def test_a_strict_locale_ring_has_no_sidecar_to_ask(scratch):
    ring = subetha.LocaleRing(scratch("strict-ask"), 64)
    assert ring.managed is False
    with pytest.raises(ValueError, match="migrate_to"):
        ring.request_locale("file")
    assert ring.sidecar_migrations == 0


def test_a_managed_ring_moved_directly_stays_where_it_was_moved(scratch):
    ring = subetha.LocaleRing(scratch("stay"), 64, managed=True, scan_interval_us=1000)
    ring.migrate_to("file")
    # Four times the 250 ms the sidecar waits between moves: long enough
    # for it to move the ring back, had it been left asking for anon.
    time.sleep(1.0)
    assert ring.locale == "file", "the ring's own sidecar moved it back"
    assert ring.sidecar_migrations == 0


# --- observing an object ---------------------------------------------------


def observable_objects(path):
    """One object of every class that can be observed, each built small."""
    heartbeat = subetha.Heartbeat(path("heartbeat"), 4)
    bits, hashes = subetha.BlockedBloomFilter.suggest(100, 0.01)
    return {
        "Adaptive": subetha.Adaptive(),
        "Arena": subetha.Arena(path("arena"), 4096),
        "Atomic": subetha.Atomic(path("atomic")),
        "BitVec": subetha.BitVec(path("bits"), capacity_bits=64),
        "BlockedBloomFilter": subetha.BlockedBloomFilter(path("blocked"), bits, hashes),
        "BloomFilter": subetha.BloomFilter(path("bloom"), n_bits=1024, n_hashes=3),
        "BroadcastRing": subetha.BroadcastRing(path("broadcast"), 8),
        "CountMinSketch": subetha.CountMinSketch(path("sketch"), depth=4, width=64),
        "EpochBarrier": subetha.EpochBarrier(path("barrier"), heartbeat),
        "FenceClock": subetha.FenceClock(path("clock"), 4),
        "Graph": subetha.Graph(path("graph"), max_nodes=8, max_edges=16),
        "HandleTable": subetha.HandleTable(path("handles"), 4),
        "HashMap": subetha.HashMap(path("map"), 8, 4, 4),
        "Heartbeat": heartbeat,
        "Histogram": subetha.Histogram(path("histogram"), boundaries=[10, 100]),
        "HyperLogLog": subetha.HyperLogLog(path("hll")),
        "LeaderElection": subetha.LeaderElection(path("leader")),
        "OwnerLease": subetha.OwnerLease(path("lease"), value=b"v"),
        "RWLock": subetha.RWLock(path("rwlock")),
        "RateLimiter": subetha.RateLimiter(path("rate"), capacity=4, refill_per_second=2),
        "Reservoir": subetha.Reservoir(path("reservoir"), 4),
        "Ring": subetha.Ring(path("ring"), 8),
        "Semaphore": subetha.Semaphore(path("semaphore"), 1),
        "TimePointTile": subetha.TimePointTile(path("tile")),
        "TopologyMap": subetha.TopologyMap(path("topology"), 4),
        "Universal": subetha.Universal(path("universal"), 8),
        "VersionChain": subetha.VersionChain(path("chain"), 4),
    }


def registers_with_the_sidecar(cls):
    """Whether `cls` has the observe that registers it, which takes a
    policy, rather than a sensor's observe, which takes a sample."""
    observe = getattr(cls, "observe", None)
    return observe is not None and "policy" in inspect.signature(observe).parameters


def test_every_class_that_carries_an_observation_ring_can_be_observed(scratch):
    objects = observable_objects(scratch)
    observable = sorted(
        name
        for name in subetha.__all__
        if isinstance(getattr(subetha, name, None), type)
        and registers_with_the_sidecar(getattr(subetha, name))
    )
    assert sorted(objects) == observable, "the table here covers every class that registers"
    for name, obj in objects.items():
        registration = obj.observe()
        try:
            assert registration.closed is False, name
            assert registration.stats().migrations_triggered == 0, name
        finally:
            registration.close()


def test_an_observed_map_counts_what_is_done_to_it(scratch):
    table = subetha.HashMap(scratch("table"), 64, 4, 8)
    with table.observe() as registration:
        table.insert(b"key1", b"value001")
        table.get(b"key1")
        table.get(b"none")
        sidecar.scan_now()
        stats = registration.stats()
    assert stats.ops_observed == 3
    kinds = stats.op_kind_counts
    assert len(kinds) == subetha.InstanceStats.N_OP_KINDS
    assert kinds[1] == 1, "one insert, the map's op kind 1"
    assert kinds[2] == 2, "two gets, its op kind 2"
    assert stats.op_kind_total() == 3
    assert stats.ratio_of(2, [1, 2]) == pytest.approx(2 / 3)
    assert stats.migrations_triggered == 0, "a map's own policy never migrates it"


def test_closing_a_registration_takes_the_object_out_of_the_sidecar(scratch):
    table = subetha.HashMap(scratch("closing"), 64, 4, 8)
    gc.collect()
    before = sidecar.instance_count()
    with table.observe() as registration:
        assert sidecar.instance_count() == before + 1
        assert registration.id >= 0
        assert registration.tag == 0
        assert f"id={registration.id}" in repr(registration)
    assert registration.closed is True
    assert sidecar.instance_count() == before
    with pytest.raises(ValueError, match="closed"):
        registration.stats()
    with pytest.raises(ValueError, match="closed"):
        registration.tag  # noqa: B018
    registration.close()  # closing twice is closing once


def test_an_object_has_one_registration_at_a_time(scratch):
    table = subetha.HashMap(scratch("once"), 64, 4, 8)
    first = table.observe()
    with pytest.raises(ValueError, match=f"registration {first.id}"):
        table.observe()
    first.close()
    second = table.observe()
    assert second.closed is False, "a closed registration frees the object for another"
    second.close()


def test_a_collected_registration_frees_the_object(scratch):
    table = subetha.HashMap(scratch("collected"), 64, 4, 8)
    table.observe()  # dropped at once
    gc.collect()
    table.observe().close()


def test_observing_past_the_cap_raises_runtime_error(scratch):
    table = subetha.HashMap(scratch("cap"), 64, 4, 8)
    cap = sidecar.max_instances()
    sidecar.set_max_instances(sidecar.instance_count())
    try:
        with pytest.raises(RuntimeError, match="set_max_instances"):
            table.observe()
    finally:
        sidecar.set_max_instances(cap)
    assert sidecar.max_instances() == cap
    table.observe().close()  # the refused call left the object free


def test_the_sidecar_reports_its_scan_threads():
    assert sidecar.node_count() >= 1


# --- an adaptive object of the caller's own ---------------------------------


def test_an_adaptive_object_records_only_once_it_is_observed():
    obj = subetha.Adaptive()
    assert obj.record(1) is False, "nothing drains an object nobody observes"
    with obj.observe():
        assert obj.record(1) is True


def test_the_caller_moves_its_own_tag():
    obj = subetha.Adaptive()
    assert obj.tag == 0
    obj.set_tag(7)
    assert obj.tag == 7
    assert "tag=7" in repr(obj)


def test_a_python_policy_moves_the_tag_of_an_adaptive_object():
    asked = []

    def policy(stats, tag):
        asked.append((stats.ops_observed, tag))
        return 1 if stats.ops_observed >= 3 else None

    obj = subetha.Adaptive()
    with obj.observe(policy) as registration:
        for _ in range(3):
            assert obj.record(1, latency_ticks=100) is True
        sidecar.scan_now()
        assert obj.tag == 1, "the policy's answer is the object's tag"
        assert registration.tag == 1
        stats = registration.stats()
        assert stats.migrations_triggered == 1
        assert stats.total_latency_ticks == 300
        assert stats.average_latency_ticks() == 100
        assert asked[-1] == (3, 0), "the policy saw every record, at the tag before its answer"
        assert registration.policy_errors == 0
        assert registration.last_policy_error is None


def test_a_policy_that_raises_is_counted_and_its_exception_kept():
    def policy(stats, tag):
        raise ZeroDivisionError("no tag today")

    obj = subetha.Adaptive()
    with obj.observe(policy) as registration:
        obj.record(1)
        sidecar.scan_now()
        assert registration.policy_errors == 1
        failure = registration.last_policy_error
        assert isinstance(failure, ZeroDivisionError)
        assert str(failure) == "no tag today"
        assert failure.__traceback__ is not None, "the exception comes back whole"
        assert obj.tag == 0, "a failed policy leaves the tag where it was"


def test_an_answer_that_is_not_a_tag_is_kept_as_a_type_error():
    obj = subetha.Adaptive()
    with obj.observe(lambda stats, tag: "fast") as registration:
        obj.record(1)
        sidecar.scan_now()
        assert registration.policy_errors == 1
        failure = registration.last_policy_error
        assert isinstance(failure, TypeError)
        assert "'fast'" in str(failure), "the error names what came back"
        assert failure.__cause__ is not None, "and carries the conversion's own error"
        assert obj.tag == 0


def test_a_policy_must_be_callable():
    with pytest.raises(TypeError, match="callable"):
        subetha.Adaptive().observe(42)


def test_stats_tell_one_recording_thread_from_several():
    obj = subetha.Adaptive()
    # perf_counter, since monotonic ticks every 15.6 ms on Windows before
    # Python 3.13 and this whole block can take less than one tick.
    started = time.perf_counter()
    with obj.observe() as registration:
        obj.record(2, contended=True)
        worker = threading.Thread(target=lambda: obj.record(2))
        worker.start()
        worker.join()
        obj.record(3, empty=True)
        sidecar.scan_now()
        stats = registration.stats()
    elapsed_us = (time.perf_counter() - started) * 1_000_000
    assert stats.distinct_threads_for(2) == 2
    assert stats.is_multi_thread_for(2) is True
    assert stats.is_multi_thread_for(3) is False
    assert stats.per_op_kind_distinct_count[2] == 2
    threads = stats.per_op_kind_distinct_threads
    assert len(threads) == subetha.InstanceStats.N_OP_KINDS
    assert len(threads[2]) == subetha.InstanceStats.MAX_TRACKED_THREADS_PER_KIND
    assert len({tid for tid in threads[2] if tid}) == 2, "two distinct thread ids, the rest empty"
    assert stats.contention_ops == 1, "contended sets the flag contention_rate counts"
    assert stats.contention_rate() == pytest.approx(1 / 3)
    assert stats.last_drain_us <= elapsed_us, "the last drain happened inside the test"
    assert "InstanceStats" in repr(stats)


# --- the interpreter's exit -------------------------------------------------


def test_a_process_exits_while_its_policy_is_being_asked(tmp_path):
    # The sidecar's thread asks the policy after each scan that drained
    # something; the records below leave it asking as the interpreter
    # exits. It must not attach to an interpreter that is finalizing.
    child = textwrap.dedent(
        """
        import time
        import subetha

        def policy(stats, tag):
            time.sleep(0.002)
            return None

        obj = subetha.Adaptive()
        registration = obj.observe(policy)
        for _ in range(4000):
            obj.record(1)
        """
    )
    done = subprocess.run(
        [sys.executable, "-c", child],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=os.fspath(tmp_path),
    )
    assert done.returncode == 0, done.stderr
    assert done.stderr == "", done.stderr
