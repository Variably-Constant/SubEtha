"""The process's sidecar.

One sidecar serves the whole process: a scan thread per NUMA node that
drains the observation ring of every registered object into its stats
and, when the object was registered with a policy, asks that policy which
tag the object should run at. An object is registered with its own
`observe` method, which returns a `Registration`; this module holds what
belongs to the sidecar as a whole.

    import subetha
    from subetha import sidecar

    table = subetha.HashMap("/tmp/table", 1024, 4, 8)
    with table.observe() as registration:
        table.insert(b"key1", b"value001")
        table.get(b"key1")
        sidecar.scan_now()
        print(registration.stats().ops_observed)  # 2

The scan threads wake every 200 microseconds on their own. `scan_now` is
for a caller that needs what was recorded counted before it goes on, a
test above all.
"""

from . import _subetha

__all__ = [
    "instance_count",
    "max_instances",
    "node_count",
    "scan_now",
    "set_max_instances",
]


def scan_now() -> None:
    """Have every scan thread scan now, and wait for it.

    An observation recorded before the call has been drained, and its
    object's policy asked, by the time this returns. The interpreter is
    released while it waits, since a scan may be running a Python policy.
    """
    _subetha.sidecar_scan_now()


def instance_count() -> int:
    """How many objects are registered with the sidecar now."""
    return _subetha.sidecar_instance_count()


def max_instances() -> int:
    """The most objects the sidecar holds at once, 10,000 unless changed.

    `observe` past it raises RuntimeError.
    """
    return _subetha.sidecar_max_instances()


def set_max_instances(cap: int) -> None:
    """Raise or lower the most objects the sidecar holds at once.

    What one scan can cost grows with the number registered, so raise it
    for a count you have measured and have room to scan.
    """
    _subetha.sidecar_set_max_instances(cap)


def node_count() -> int:
    """The sidecar's scan threads, one per NUMA node."""
    return _subetha.sidecar_node_count()
