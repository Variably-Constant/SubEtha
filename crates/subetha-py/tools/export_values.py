"""Run real scenarios against the built extension and write down what
they actually answered.

A type name says what comes back; it does not say what it looks like.
Every output this produces is captured from the run rather than
transcribed, so the page cannot describe a shape the module does not
have. It needs the compiled module, so it runs where the wheel was
built rather than on the authoring host.

    python tools/export_values.py --out values.md

Each scenario works in its own throwaway directory and the paths are
rewritten to an ordinary one on the way out, so the page does not carry
a temp directory that changes every run.
"""
from __future__ import annotations

import argparse
import pathlib
import shutil
import tempfile
import traceback

import subetha


def show(value: object, depth: int = 0) -> str:
    """How a captured value is written down."""
    pad = " " * depth
    if value is None:
        return "None"
    if isinstance(value, (bytes, bytearray)):
        head = ", ".join(str(b) for b in value[:16])
        tail = ", ..." if len(value) > 16 else ""
        return f"bytes[{len(value)}] : {head}{tail}"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float, str)):
        return repr(value)
    if isinstance(value, tuple):
        return "(" + ", ".join(show(v, depth) for v in value) + ")"
    if isinstance(value, list):
        if not value:
            return "[]  (empty)"
        lines = [f"list, {len(value)} item(s):"]
        for item in value[:4]:
            lines.append(f"{pad}  " + show(item, depth + 2))
        if len(value) > 4:
            lines.append(f"{pad}  ...")
        return "\n".join(lines)
    name = type(value).__name__
    props = [
        p
        for p in dir(type(value))
        if not p.startswith("_") and isinstance(getattr(type(value), p, None), property)
    ]
    if props:
        lines = [f"<{name}>"]
        for p in props:
            try:
                lines.append(f"{pad}  {p} = " + show(getattr(value, p), depth + 2))
            except Exception:
                lines.append(f"{pad}  {p} = <raised>")
        return "\n".join(lines)
    return f"<{name}>"


SECTIONS: list[tuple[str, str, str]] = []


def scenario(group: str, title: str, code: str) -> None:
    SECTIONS.append((group, title, code.strip()))


scenario(
    "Shared state",
    "An atomic, and what each operation answers",
    """
a = subetha.Atomic(f"{root}/atom", init=10)
out("store(40)           ", a.store(40))
out("fetch_add(2)        ", a.fetch_add(2))
out("load()              ", a.load())
out("swap(99)            ", a.swap(99))
out("compare_exchange    ", a.compare_exchange(99, 1))
out("load()              ", a.load())
""",
)

scenario(
    "Shared state",
    "A map: keys and values are bytes of the declared size",
    """
m = subetha.HashMap(f"{root}/map", capacity=64, key_size=8, value_size=8)
k = (7).to_bytes(8, "little")
v = (70).to_bytes(8, "little")
absent = (999).to_bytes(8, "little")
out("key_size/value_size ", (m.key_size, m.value_size))
out("insert(k, v)        ", m.insert(k, v))
out("insert(k, v) again  ", m.insert(k, v))
out("get(k)              ", m.get(k))
out("get(absent)         ", m.get(absent))
out("k in m              ", k in m)
out("len(m)              ", len(m))
out("remove(k)           ", m.remove(k))
out("get(k) after remove ", m.get(k))
""",
)

scenario(
    "Rings",
    "A ring: what recv actually hands back",
    """
r = subetha.BroadcastRing(f"{root}/bcast", capacity=8)
cid = r.register_consumer()
r.push(b"hello")
out("payload_size        ", r.payload_size)
out("recv(cid)           ", r.recv(cid))
out("recv(cid) when empty", r.recv(cid))
out("producer_position   ", r.producer_position)
out("active_consumers    ", r.active_consumers)
r.unregister_consumer(cid)
""",
)

scenario(
    "Versioned",
    "A pin, and the entries a scan answers",
    """
vm = subetha.VersionedMap(f"{root}/vmap", capacity=64, epochs_path=f"{root}/vepochs")
vm.insert(7, 70)
vm.insert(9, 90)
pin = vm.pin()
out("pin()               ", pin)
out("pin.epoch           ", pin.epoch)
out("pin.get(7)          ", pin.get(7))
out("pin.scan(0, 100, 10)", pin.scan(0, 100, 10))
out("pin.scan_from(...,1)", pin.scan_from(0, 100, 1))
pin.release()
""",
)

scenario(
    "Coordination",
    "A lock hold, and what a refused one looks like",
    """
lock = subetha.RWLock(f"{root}/lock")
held = lock.write()
out("write()             ", held)
out("readers             ", lock.readers)
out("try_write() held    ", lock.try_write())
out("write_for(0.2) held ", lock.write_for(0.2))
held.release()
out("try_write() free    ", lock.try_write() is not None)
""",
)

scenario(
    "Coordination",
    "A hold as a context manager",
    """
lock = subetha.RWLock(f"{root}/lock2")
with lock.write() as held:
    out("inside the block    ", (held, lock.readers))
out("after the block     ", lock.try_write() is not None)
""",
)

scenario(
    "Probabilistic",
    "Sizing a filter, then asking it about membership",
    """
bits, hashes = subetha.BloomFilter.suggest_config(1000, 0.01)
out("suggest_config      ", (bits, hashes))
b = subetha.BloomFilter(f"{root}/bloom", n_bits=bits, n_hashes=hashes)
b.insert(b"alice")
out("contains(b'alice')  ", b.contains(b"alice"))
out("contains(b'bob')    ", b.contains(b"bob"))
out("false_positive_rate ", b.false_positive_rate)
""",
)

scenario(
    "The module",
    "What the wheel reports about itself",
    """
out("transports          ", subetha.transports)
out("free_threaded       ", subetha.free_threaded)
out("boundary_note()     ", subetha.boundary_note())
""",
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    lines: list[str] = [
        "---",
        'title: "What the values look like"',
        "weight: 5",
        "---",
        "",
        "# What the values look like",
        "",
        "A type name says what comes back; it does not say what it looks",
        "like. Every block below was run against the built extension and the",
        "output is what it answered, captured rather than written down.",
        "Generated by `crates/subetha-py/tools/export_values.py`.",
        "",
        "A `bytes` value is shown as its length and its leading bytes,",
        "because the length is the part a reader cannot guess: a ring hands",
        "back a whole slot, payload then zeros, rather than only what was",
        "pushed.",
        "",
        f"Captured from a wheel carrying {subetha.transports}, on a "
        f"{'free-threaded' if subetha.free_threaded else 'standard'} interpreter.",
        "",
        "Which transports a wheel carries is a build-time choice: the",
        "Sens-O-Matic link is always there and each bridge is present only",
        "when its feature was built, which `OPTIONAL_BY_TRANSPORT` spells",
        "out. Everything else on this page is the same whichever way it was",
        "built.",
        "",
    ]

    groups: dict[str, list[tuple[str, str, str]]] = {}
    for group, title, code in SECTIONS:
        groups.setdefault(group, []).append((group, title, code))

    for group, entries in groups.items():
        lines += [f"## {group}", ""]
        for _g, title, code in entries:
            root = tempfile.mkdtemp(prefix="subetha-values-")
            captured: list[str] = []

            def out(label: str, value: object) -> None:
                captured.append(f"{label}-> " + show(value))

            # `root` is in the environment, so the scenario's own f-strings
            # resolve against it and the code runs exactly as it is printed.
            env = {"subetha": subetha, "root": root, "out": out}
            try:
                exec(code, env)  # noqa: S102
            except Exception:
                captured.append("RAISED:")
                captured += ["  " + ln for ln in traceback.format_exc().strip().split("\n")[-3:]]
            finally:
                shutil.rmtree(root, ignore_errors=True)

            # The scratch root never reaches the page. It carries the
            # temp directory and so whoever ran this, and it changes
            # every run. The code prints `{root}` unexpanded, so it is
            # the captured answers that would carry it: a scenario
            # asking a structure where it lives gets a real path back.
            def redact(s: str, at: str = root) -> str:
                return s.replace(at, "/ipc")

            lines += [f"### {title}", "", "```python"]
            lines += [redact(ln) for ln in code.split("\n")]
            lines += ["```", "", "Answers:", "", "```"]
            lines += [redact(ln) for ln in captured]
            lines += ["```", ""]

    text = "\n".join(lines) + "\n"
    dest = pathlib.Path(args.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text, encoding="utf-8")
    print(f"wrote {len(SECTIONS)} scenarios to {dest} ({len(lines)} lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
