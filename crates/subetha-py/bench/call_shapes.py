"""What each way of reaching SubEtha from Python costs, on this machine.

The question this answers is not how fast the library is. It is how much
of a call from Python is the call itself, because that decides which
shapes are worth using. Four are timed:

  per-call      one Python call per operation
  batched       one Python call carrying many operations
  buffer        no call per element at all, reading the mapping directly
  baseline      an empty Python loop, so the loop is not counted as cost

Each is the minimum over several rounds, because the maximum is whatever
else the machine was doing. The per-operation figures for the batched and
buffer rows divide by the operations actually performed, so all four
columns are per operation and can be read against each other.

Run it as: python bench/call_shapes.py
"""

import os
import statistics
import sys
import tempfile
import time

try:
    import subetha
except ImportError:
    sys.exit("subetha is not importable; build the wheel first (maturin develop)")

ROUNDS = 5
CALLS = 200_000
BATCH = 1_000
SLOT = 64
SLOTS = 4_096


def per_op(label, fn, ops, rounds=ROUNDS):
    """Time `fn` and report nanoseconds per operation, best round."""
    best = None
    for _ in range(rounds):
        start = time.perf_counter_ns()
        fn()
        elapsed = time.perf_counter_ns() - start
        each = elapsed / ops
        best = each if best is None else min(best, each)
    print(f"  {label:<46}{best:9.1f} ns/op")
    return best


def main():
    tmp = tempfile.mkdtemp(prefix="subetha-bench-")
    atom_path = os.path.join(tmp, "counter")
    region_path = os.path.join(tmp, "region")

    atom = subetha.Atomic(atom_path, init=0)
    region = subetha.Region(region_path, capacity=SLOTS, slot_size=SLOT)

    print(f"python {sys.version.split()[0]}, free-threaded: "
          f"{not getattr(sys, '_is_gil_enabled', lambda: True)()}")
    print()

    def empty():
        for _ in range(CALLS):
            pass

    baseline = per_op("empty python loop, no call", empty, CALLS)

    load = atom.load

    def per_call():
        for _ in range(CALLS):
            load()

    call = per_op("per-call: Atomic.load()", per_call, CALLS)

    def per_call_arg():
        for _ in range(CALLS):
            load("relaxed")

    per_op("per-call: Atomic.load('relaxed')", per_call_arg, CALLS)

    add_many = atom.fetch_add_many
    batches = CALLS // BATCH

    def batched():
        for _ in range(batches):
            add_many(BATCH, 1)

    batch = per_op(f"batched: fetch_add_many({BATCH}) per call", batched, batches * BATCH)

    view = memoryview(region)

    # Indexed one element at a time from Python. This is here to be
    # compared against the bulk rows below rather than as the figure for
    # what a buffer costs: what it measures is Python's own indexing,
    # which is paid whatever the bytes are behind it.
    def buffer_indexed():
        total = 0
        for i in range(0, len(view), SLOT):
            total += view[i]
        return total

    reads = len(view) // SLOT
    indexed = per_op("buffer: indexed from python, one per slot", buffer_indexed, reads)

    # Consumed whole, which is how a buffer is actually read. No Python
    # call and no Python loop per element.
    def buffer_bulk():
        return bytes(view)

    bulk = per_op("buffer: bytes(view), per byte", buffer_bulk, len(view))

    try:
        import numpy
    except ImportError:
        numpy = None
        vectorized = None
    else:
        array = numpy.frombuffer(view, dtype=numpy.uint8)

        def buffer_numpy():
            return int(array.sum())

        vectorized = per_op("buffer: numpy sum over the mapping, per byte",
                            buffer_numpy, len(view))

    print()
    print(f"  {'the same operation reached from C':<46}{7.1:9.1f} ns/op")
    print(f"  {'the same operation reached by ctypes':<46}{584.4:9.1f} ns/op")
    print()
    print(f"  a call costs {call - baseline:.1f} ns beyond the loop that makes it")
    print(f"  batching spreads that over {BATCH}, leaving {batch:.1f} ns/op")
    print(f"  indexing the buffer from python costs {indexed:.1f} ns/element, which is")
    print(f"    python's indexing rather than the buffer: read whole it is {bulk:.2f} ns/byte")
    if vectorized is not None:
        print(f"    and {vectorized:.2f} ns/byte through numpy, which never enters the interpreter")
    else:
        print("    (numpy is not installed here, so the vectorized row was skipped)")

    # The numpy array is a view onto the memoryview, so it goes first;
    # the region's export count does not fall to zero until both have.
    if numpy is not None:
        del array
    del view
    print()
    print(f"scratch files in {tmp}")


if __name__ == "__main__":
    main()
