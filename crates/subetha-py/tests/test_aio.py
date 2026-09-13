"""Awaiting SubEtha from asyncio.

What these check is that a wait really does leave the event loop free.
A test that only checked the value came back would pass just as well if
the call blocked the loop outright, which is the one thing this module
exists to avoid.
"""

import asyncio
import time

import pytest

import subetha
from subetha import aio


@pytest.fixture
def scratch(tmp_path):
    return lambda name: str(tmp_path / name)


def test_waiting_for_an_item_leaves_the_loop_running(scratch):
    chan = subetha.Channel(scratch("chan"), capacity=64)

    async def main():
        ticks = 0

        async def tick():
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.01)

        async def send_shortly():
            await asyncio.sleep(0.15)
            chan.send(b"late")

        ticker = asyncio.create_task(tick())
        sender = asyncio.create_task(send_shortly())
        got = await aio.recv(chan, timeout=10)
        ticker.cancel()
        await sender
        return got, ticks

    got, ticks = asyncio.run(main())
    assert got == b"late"
    assert ticks > 2, "the loop must keep running while the wait is in progress"


def test_waiting_for_nothing_answers_none(scratch):
    chan = subetha.Channel(scratch("chan"), capacity=64)

    async def main():
        started = time.monotonic()
        got = await aio.recv(chan, timeout=0.2)
        return got, time.monotonic() - started

    got, took = asyncio.run(main())
    assert got is None, "a timeout answers None"
    assert took >= 0.1, "and it really waited"


def test_sending_answers_true_when_it_goes(scratch):
    chan = subetha.Channel(scratch("chan"), capacity=64)

    async def main():
        return await aio.send(chan, b"item", timeout=5)

    assert asyncio.run(main()) is True
    assert chan.recv() == b"item"


def test_a_wait_can_be_cancelled(scratch):
    chan = subetha.Channel(scratch("chan"), capacity=64)

    async def main():
        waiting = asyncio.create_task(aio.recv(chan))
        await asyncio.sleep(0.05)
        waiting.cancel()
        try:
            await waiting
        except asyncio.CancelledError:
            return "cancelled"
        return "finished"

    assert asyncio.run(main()) == "cancelled"


def test_work_runs_under_a_permit(scratch):
    gate = subetha.Semaphore(scratch("sem"), initial=1, max_permits=1)

    async def main():
        # The permit is held while this runs and given back after, all
        # on the thread that took it.
        answer = await aio.with_permit(gate, lambda: gate.available, timeout=5)
        return answer, gate.available

    while_held, after = asyncio.run(main())
    assert while_held == 0, "the permit was held while the work ran"
    assert after == 1, "and given back after"


def test_work_under_a_permit_gives_up_when_none_is_free(scratch):
    gate = subetha.Semaphore(scratch("sem"), initial=1, max_permits=1)
    taken = gate.acquire()

    async def main():
        return await aio.with_permit(gate, lambda: "ran", timeout=0.2)

    assert asyncio.run(main()) is None, "a timeout answers None, and the work never ran"
    taken.release()


def test_work_runs_under_a_write_hold(scratch):
    lock = subetha.RWLock(scratch("lock"))

    async def main():
        answer = await aio.with_write_lock(lock, lambda: "done", timeout=5)
        return answer, lock.readers

    answer, readers = asyncio.run(main())
    assert answer == "done"
    assert readers == 0, "the hold was given back"


def test_work_runs_under_a_read_hold(scratch):
    lock = subetha.RWLock(scratch("lock"))

    async def main():
        return await aio.with_read_lock(lock, lambda: lock.readers, timeout=5)

    assert asyncio.run(main()) >= 1, "the read hold was in place while the work ran"


def test_a_hold_is_given_back_when_the_work_raises(scratch):
    lock = subetha.RWLock(scratch("lock"))

    def explode():
        raise RuntimeError("the work went wrong")

    async def main():
        with pytest.raises(RuntimeError):
            await aio.with_write_lock(lock, explode, timeout=5)
        # If the hold had leaked, this would time out instead.
        return await aio.with_write_lock(lock, lambda: "second", timeout=2)

    assert asyncio.run(main()) == "second"


def test_polling_a_link_answers_when_something_arrives(scratch):
    reader = subetha.SensReceiver(("127.0.0.1", 0), max_item_size=64)
    writer = subetha.SensSender(("127.0.0.1", 0), reader.local_addr, max_item_size=64)

    async def main():
        async def send_shortly():
            await asyncio.sleep(0.05)
            writer.send(b"across")

        sender = asyncio.create_task(send_shortly())
        taken = await aio.poll(reader, timeout=10)
        await sender
        return taken

    taken = asyncio.run(main())
    assert taken == [b"across"]


def test_polling_a_quiet_link_answers_empty(scratch):
    reader = subetha.SensReceiver(("127.0.0.1", 0), max_item_size=64)

    async def main():
        return await aio.poll(reader, timeout=0.2)

    assert asyncio.run(main()) == []
