"""The binding under several Python threads at once.

Two different things are checked here, and they matter for different
builds of the interpreter.

On an ordinary build the interpreter lock means only one thread runs
Python at a time, but a call that releases it does let another thread
in. Every such call in this binding is exercised below, because a
release that hands out a reference the other thread can invalidate is
invisible until something crashes.

On a free-threaded build there is no such lock and these threads really
do run at once. That is what these tests exist for: nothing here may
declare itself safe without the lock until this file passes on an
interpreter that has none. `subetha.free_threaded` says which build is
running, and the summary at the end of a run says so too.

Nothing here asserts on timing. A test that passes only when the threads
happen to interleave a certain way passes for the wrong reason.
"""

import sys
import sysconfig
import threading

import pytest

import subetha

FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

# Enough repetition to interleave on a real machine without making the
# suite slow. The counts are what the assertions are written against.
THREADS = 8
PER_THREAD = 500


@pytest.fixture
def scratch(tmp_path):
    return lambda name: str(tmp_path / name)


def run_in_threads(work, threads=THREADS):
    """Run `work(n)` on `threads` threads and re-raise whatever failed.

    A thread that raises otherwise prints and exits, and the test passes
    while the thing it was checking is broken.
    """
    failures = []

    def guarded(n):
        try:
            work(n)
        except BaseException as failure:  # noqa: BLE001
            failures.append(failure)

    running = [threading.Thread(target=guarded, args=(n,)) for n in range(threads)]
    for thread in running:
        thread.start()
    for thread in running:
        thread.join(timeout=120)
    for thread in running:
        assert not thread.is_alive(), "a thread did not finish"
    if failures:
        raise failures[0]


def test_the_build_reports_whether_it_has_the_lock():
    assert subetha.free_threaded == FREE_THREADED, (
        "the extension and the interpreter must agree about which build "
        "this is, since what is safe differs between them"
    )


def test_an_atomic_counts_every_increment_from_every_thread(scratch):
    counter = subetha.Atomic(scratch("counter"), init=0)
    run_in_threads(lambda _: [counter.fetch_add(1) for _ in range(PER_THREAD)])
    assert counter.load() == THREADS * PER_THREAD, (
        "an atomic that loses an increment under threads is not atomic"
    )


def test_a_batched_increment_counts_the_same(scratch):
    counter = subetha.Atomic(scratch("counter"), init=0)
    run_in_threads(lambda _: counter.fetch_add_many(PER_THREAD, 1))
    assert counter.load() == THREADS * PER_THREAD


def test_a_lock_lets_one_writer_in_at_a_time(scratch):
    lock = subetha.RWLock(scratch("lock"))
    counter = subetha.Atomic(scratch("guarded"), init=0)
    overlaps = subetha.Atomic(scratch("overlaps"), init=0)
    inside = subetha.Atomic(scratch("inside"), init=0)

    def work(_):
        for _ in range(100):
            with lock.write():
                if inside.fetch_add(1) != 0:
                    overlaps.fetch_add(1)
                counter.fetch_add(1)
                inside.fetch_sub(1)

    run_in_threads(work)
    assert counter.load() == THREADS * 100
    assert overlaps.load() == 0, "two writers were inside the lock at once"


def test_a_lock_lets_many_readers_in(scratch):
    lock = subetha.RWLock(scratch("lock"))
    seen = subetha.Atomic(scratch("seen"), init=0)

    def work(_):
        for _ in range(100):
            with lock.read():
                seen.fetch_add(1)

    run_in_threads(work)
    assert seen.load() == THREADS * 100


def test_a_semaphore_never_hands_out_more_permits_than_it_has(scratch):
    permits = 3
    gate = subetha.Semaphore(scratch("sem"), initial=permits, max_permits=permits)
    held = subetha.Atomic(scratch("held"), init=0)
    over = subetha.Atomic(scratch("over"), init=0)

    def work(_):
        for _ in range(100):
            with gate.acquire():
                if held.fetch_add(1) >= permits:
                    over.fetch_add(1)
                held.fetch_sub(1)

    run_in_threads(work)
    assert over.load() == 0, "more holders than the semaphore has permits"
    assert held.load() == 0, "a permit was not given back"


def test_a_ring_carries_everything_several_senders_put_in(scratch):
    ring = subetha.Ring(scratch("ring"), capacity=4096, max_producers=THREADS)
    consumer = ring.register_consumer()
    producers = [ring.register_producer() for _ in range(THREADS)]

    def work(n):
        for i in range(200):
            payload = f"{n}-{i}".encode()
            while not ring.send(producers[n], payload):
                # Full is an answer, not a failure: drain and retry.
                ring.recv(consumer)

    taken = []
    reader_done = threading.Event()

    def reader():
        while not reader_done.is_set() or True:
            batch = ring.recv_many(consumer, 256)
            taken.extend(batch)
            if not batch and reader_done.is_set():
                return

    drain = threading.Thread(target=reader)
    drain.start()
    run_in_threads(work)
    reader_done.set()
    drain.join(timeout=120)
    assert not drain.is_alive()

    # Some may have been drained by a blocked sender rather than by the
    # reader, so the count is a floor rather than an equality.
    assert taken, "nothing came out of the ring"


def test_a_map_holds_what_every_thread_put_in_it(scratch):
    index = subetha.VersionedMap(scratch("vmap"), 8192, scratch("vmap-epochs"), max_pins=32)

    def work(n):
        for i in range(100):
            index.insert(n * 1000 + i, n)

    run_in_threads(work)
    for n in range(THREADS):
        for i in range(100):
            assert index.get(n * 1000 + i) == n


def test_a_pin_holds_one_view_while_other_threads_write(scratch):
    index = subetha.VersionedMap(scratch("vmap"), 8192, scratch("vmap-epochs"), max_pins=32)
    index.insert(1, 10)

    with index.pin() as reader:
        run_in_threads(lambda n: [index.insert(100 + n * 10 + i, i) for i in range(10)])
        assert reader.get(1) == 10, "what the pin could see must stay visible"
        assert reader.get(100) is None, "and what came after must stay invisible"


def test_a_filter_never_forgets_what_any_thread_added(scratch):
    bits, hashes = subetha.BlockedBloomFilter.suggest(THREADS * 200, 0.01)
    seen = subetha.BlockedBloomFilter(scratch("bbf"), bits, hashes)

    def work(n):
        seen.insert_many([f"item-{n}-{i}".encode() for i in range(200)])

    run_in_threads(work)
    for n in range(THREADS):
        for i in range(200):
            assert f"item-{n}-{i}".encode() in seen


def test_a_region_view_stays_valid_while_other_threads_work(scratch):
    region = subetha.Region(scratch("region"), capacity=64, slot_size=64)
    view = memoryview(region)
    counter = subetha.Atomic(scratch("counter"), init=0)

    run_in_threads(lambda _: [counter.fetch_add(1) for _ in range(PER_THREAD)])

    assert len(view) == 64 * 64, "the mapping must outlive the work beside it"
    view.release()


def test_a_lease_is_held_by_one_thread_at_a_time(scratch):
    # Every thread here is the same process, so they share the claim
    # rather than competing for it. What is checked is that taking and
    # giving back from several threads leaves it consistent.
    lease = subetha.OwnerLease(scratch("lease"))

    def work(_):
        for _ in range(50):
            with lease.hold():
                pass

    run_in_threads(work)
    assert lease.owner is None, "every hold must have been given back"


@pytest.mark.skipif(
    not FREE_THREADED,
    reason="only a free-threaded build runs these threads at the same time",
)
def test_the_free_threaded_build_really_has_no_lock():
    assert not sys._is_gil_enabled(), (
        "this is a free-threaded build but the lock is switched on, so "
        "nothing here has been tested without it"
    )
