"""Awaiting SubEtha from asyncio.

The Rust side has an async engine of its own: a reactor, an executor, a
task pool. None of it is bound and none of it could be, because every
entry point takes or returns a Rust future and Python has no way to make
one. Python has asyncio instead, and what asyncio needs is not Rust's
executor but a way to wait without blocking its loop.

That is what this module is, and it is deliberately small, because only
some of the surface can be awaited soundly.

**A queue can.** `recv` and `send` take the binding's own bounded wait,
which sleeps rather than spins and releases the interpreter while it
sleeps, and run it on a worker thread. What comes back is bytes, which
belong to nobody.

**A hold cannot be handed back.** A lock hold and a semaphore permit
belong to the thread that took them, and the binding refuses a release
from anywhere else rather than quietly releasing somebody else's. So
there is no `acquire` here that answers a permit. `with_write_lock`,
`with_read_lock` and `with_permit` take the hold and run the caller's
work on the same worker thread instead, which is sound and is what the
hold is for.

**A link has no wait to borrow.** `SensReceiver` cannot leave the thread
that made it, and it answers with whatever it could rebuild each time it
is asked rather than waiting. `poll` therefore asks on this thread and
yields to the loop between tries, which is cheap because each ask is
non-blocking.

    import asyncio
    import subetha
    from subetha import aio

    async def main():
        chan = subetha.Channel("/tmp/work", capacity=1024)
        item = await aio.recv(chan, timeout=5)
        if item is not None:
            handle(item)

Nothing here is faster than calling the binding directly. Reach for it
when a coroutine must not block its loop, and call the binding directly
everywhere else.
"""

import asyncio

__all__ = [
    "recv",
    "send",
    "with_permit",
    "with_read_lock",
    "with_write_lock",
    "poll",
]

# How long each underlying wait runs before it comes back to be retried.
# A cancelled await cannot interrupt a wait already in progress, so this
# also bounds how long cancellation takes to take effect.
_SLICE_SECONDS = 0.25


async def _until(attempt, timeout):
    """Run `attempt(slice)` on a thread until it answers, or time runs out.

    `attempt` returns None to mean "nothing yet, ask again". Splitting
    the wait into slices is what lets a cancelled await stop within a
    slice rather than at the far end of the caller's whole timeout.
    """
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while True:
        if deadline is None:
            slice_seconds = _SLICE_SECONDS
        else:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            slice_seconds = min(_SLICE_SECONDS, remaining)
        got = await asyncio.to_thread(attempt, slice_seconds)
        if got is not None:
            return got


async def recv(channel, timeout=None):
    """The next item from a `Channel` or `AdaptiveQueue`.

    Waits without blocking the event loop. Answers None when `timeout`
    seconds pass with nothing arriving; None with no timeout waits as
    long as it takes.
    """
    return await _until(lambda seconds: channel.recv_for(timeout=seconds), timeout)


async def send(channel, item, timeout=None):
    """Send one item, waiting for room without blocking the event loop.

    Answers True once it has gone, False when `timeout` seconds pass
    with no room.
    """

    def attempt(seconds):
        # False means no room this slice, which is "ask again" rather
        # than an answer, so it maps to None for the loop above.
        return True if channel.send_for(item, timeout=seconds) else None

    return bool(await _until(attempt, timeout))


async def _holding(take, work, timeout):
    """Take a hold and run `work` under it, all on one worker thread.

    The hold never crosses a thread, which is the whole point: the
    binding refuses a release from a thread other than the one that
    took it, so a hold handed back to a coroutine could not be given
    back at all.
    """
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout

    def attempt(seconds):
        held = take(seconds)
        if held is None:
            return None
        try:
            # Both the work and the release happen here, on the thread
            # that took the hold. The tuple is so a work result of None
            # is still an answer rather than "ask again".
            return (work(),)
        finally:
            held.release()
            # Drop the name as well as the hold. Work that raises puts
            # this frame in the exception's traceback, which would carry
            # the hold back to the event loop's thread and have it
            # collected there, and a hold may not be collected anywhere
            # but the thread that took it.
            del held

    while True:
        if deadline is None:
            slice_seconds = _SLICE_SECONDS
        else:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            slice_seconds = min(_SLICE_SECONDS, remaining)
        got = await asyncio.to_thread(attempt, slice_seconds)
        if got is not None:
            return got


async def with_permit(semaphore, work, timeout=None):
    """Run `work()` holding a permit, and answer what it returned.

    The permit is taken and given back on one worker thread, so `work`
    runs there too and must not touch the event loop. Answers None when
    `timeout` seconds pass without a permit, which is why `work` should
    answer something other than None when it needs telling apart.
    """
    got = await _holding(
        lambda seconds: semaphore.acquire_for(timeout=seconds), work, timeout
    )
    return None if got is None else got[0]


async def with_read_lock(lock, work, timeout=None):
    """Run `work()` holding the read side of an `RWLock`."""
    got = await _holding(
        lambda seconds: lock.read_for(timeout=seconds), work, timeout
    )
    return None if got is None else got[0]


async def with_write_lock(lock, work, timeout=None):
    """Run `work()` holding the write side of an `RWLock`."""
    got = await _holding(
        lambda seconds: lock.write_for(timeout=seconds), work, timeout
    )
    return None if got is None else got[0]


async def poll(receiver, timeout=None, every=0.005):
    """Items from a `SensReceiver`, waiting for the first to arrive.

    The receiver cannot leave the thread that made it, so this asks on
    this thread rather than a worker one. That is cheap: each ask is
    non-blocking and answers with whatever the link could rebuild, and
    the sleep between tries is what yields to the loop.

    An empty list means the timeout passed with nothing arriving.
    """
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while True:
        taken = receiver.poll()
        if taken:
            return taken
        if deadline is not None and loop.time() >= deadline:
            return []
        await asyncio.sleep(every)
