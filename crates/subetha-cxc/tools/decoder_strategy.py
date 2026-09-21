"""Does the vectors' `recovered` line describe the format or the decoder?

The wire specification defines which symbols a repair covers. It does
not say what a decoder does with several overlapping repairs, and the
vectors state a recovered set for each geometry. If a stronger decoder
recovers more than the vectors state, then that line is a property of
the reference decoder rather than of the format, and a conformance suite
built from it rejects implementations that are better.

Two decoders, over the repairs the vectors actually state:

  peeling      repeatedly take any repair with exactly one covered hole
  elimination  Gaussian elimination over GF(2^8) on the whole system

Reads the vectors for the geometry, so nothing here assumes a schedule.
"""
from __future__ import annotations

import pathlib
import re
import sys

VECTORS = pathlib.Path(
    r"E:\Projects\SubEtha\crates\subetha-cxc\vectors\rlc.txt"
)

TAPS = [
    0x01, 0x02, 0x03, 0x05, 0x07, 0x0B, 0x0D, 0x11,
    0x13, 0x17, 0x1D, 0x1F, 0x25, 0x29, 0x2B, 0x2F,
    0x35, 0x3B, 0x3D, 0x43, 0x47, 0x49, 0x4F, 0x53,
    0x59, 0x61, 0x65, 0x67, 0x6B, 0x6D, 0x71, 0x7F,
    0x83, 0x89, 0x8B, 0x95, 0x97, 0x9D, 0xA3, 0xA7,
    0xAD, 0xB3, 0xB5, 0xBF, 0xC1, 0xC5, 0xC7, 0xD3,
    0xDF, 0xE3, 0xE5, 0xE9, 0xEF, 0xF1, 0xF5, 0xF7,
    0xFB, 0xFD, 0x04, 0x08, 0x0E, 0x16, 0x1A, 0x22,
]

# GF(2^8), polynomial 0x11D, generator 2, as section 2 names it.
EXP = [0] * 512
LOG = [0] * 256
_x = 1
for _i in range(255):
    EXP[_i] = _x
    LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= 0x11D
for _i in range(255, 512):
    EXP[_i] = EXP[_i - 255]


def mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return EXP[LOG[a] + LOG[b]]


def inv(a: int) -> int:
    if a == 0:
        raise ZeroDivisionError("zero has no inverse in the field")
    return EXP[255 - LOG[a]]


def coefficient(place: int, density: int) -> int:
    """Section 4.4, the rule as written."""
    if place >= 64:
        return 0
    if place % 16 <= density:
        return TAPS[place]
    return 0


class Stream:
    def __init__(self, name: str, density: int, sources: int) -> None:
        self.name = name
        self.density = density
        self.sources = sources
        self.repairs: list[tuple[int, int, int]] = []  # first, window, dt
        self.lost: set[int] = set()
        self.recovered: set[int] = set()


def parse(path: pathlib.Path) -> list[Stream]:
    streams: list[Stream] = []
    section = ""
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("["):
            head = line[1 : line.index("]")]
            section = head.split()[0]
            if section == "stream":
                fields = dict(
                    tok.split("=", 1) for tok in line.split() if "=" in tok
                )
                streams.append(
                    Stream(
                        head.split()[1],
                        int(fields["density"]),
                        int(fields["sources"]),
                    )
                )
            continue
        if section != "stream" or not streams:
            continue
        s = streams[-1]
        if line.startswith("repair "):
            f = dict(tok.split("=", 1) for tok in line.split() if "=" in tok)
            s.repairs.append((int(f["first"]), int(f["window"]), int(f["dt"], 16)))
        elif line.startswith("lost "):
            m = re.search(r"every (\d+)th id", line)
            if not m:
                raise SystemExit(f"loss pattern not understood: {line!r}")
            s.lost = set(range(0, s.sources, int(m.group(1))))
        elif line.startswith("recovered "):
            tail = line.split(":", 1)[1] if ":" in line else ""
            s.recovered = {int(t) for t in tail.split()}
    return streams


def covered(first: int, window: int, density: int) -> dict[int, int]:
    """Source id to its nonzero coefficient, for one repair."""
    out = {}
    for i in range(window):
        place = window - 1 - i
        c = coefficient(place, density)
        if c:
            out[first + i] = c
    return out


def peel(stream: Stream) -> set[int]:
    """Take any repair with exactly one covered hole, repeatedly."""
    holes = set(stream.lost)
    got: set[int] = set()
    changed = True
    while changed:
        changed = False
        for first, window, dt in stream.repairs:
            cov = covered(first, window, dt & 0x0F)
            missing = [sid for sid in cov if sid in holes]
            if len(missing) == 1:
                holes.discard(missing[0])
                got.add(missing[0])
                changed = True
    return got


def eliminate(stream: Stream) -> set[int]:
    """Gaussian elimination over the whole system, in GF(2^8).

    One row per repair, one column per lost id. A row that reduces to a
    single nonzero entry determines that unknown; nothing here needs the
    payloads, because which unknowns are determined is a property of the
    coefficient matrix alone.
    """
    unknowns = sorted(stream.lost)
    index = {sid: i for i, sid in enumerate(unknowns)}
    rows: list[list[int]] = []
    for first, window, dt in stream.repairs:
        cov = covered(first, window, dt & 0x0F)
        row = [0] * len(unknowns)
        touched = False
        for sid, c in cov.items():
            if sid in index:
                row[index[sid]] = c
                touched = True
        if touched:
            rows.append(row)

    pivot_row = 0
    pivot_of_col: dict[int, int] = {}
    for col in range(len(unknowns)):
        pick = None
        for r in range(pivot_row, len(rows)):
            if rows[r][col]:
                pick = r
                break
        if pick is None:
            continue
        rows[pivot_row], rows[pick] = rows[pick], rows[pivot_row]
        scale = inv(rows[pivot_row][col])
        rows[pivot_row] = [mul(v, scale) for v in rows[pivot_row]]
        for r in range(len(rows)):
            if r != pivot_row and rows[r][col]:
                factor = rows[r][col]
                rows[r] = [
                    a ^ mul(factor, b) for a, b in zip(rows[r], rows[pivot_row])
                ]
        pivot_of_col[col] = pivot_row
        pivot_row += 1

    got: set[int] = set()
    for col, r in pivot_of_col.items():
        if sum(1 for v in rows[r] if v) == 1:
            got.add(unknowns[col])
    return got


def main() -> int:
    streams = parse(VECTORS)
    print(f"{'geometry':<32} {'stated':>7} {'peel':>6} {'elim':>6}  verdict")
    disagreements = 0
    for s in streams:
        if not s.lost:
            continue
        p = peel(s)
        e = eliminate(s)
        note = "peeling is what the vectors describe"
        if e > p:
            note = f"elimination recovers {len(e - p)} MORE than the vectors state"
            disagreements += 1
        elif p != s.recovered:
            note = "peeling does not reproduce the stated set"
            disagreements += 1
        print(
            f"{s.name:<32} {len(s.recovered):>7} {len(p):>6} {len(e):>6}  {note}"
        )
        if e - p:
            extra = sorted(e - p)
            print(f"{'':<32} extra ids: {extra[:12]}{' ...' if len(extra) > 12 else ''}")

    print()
    if disagreements:
        print(
            f"{disagreements} geometry(ies) where the stated set is the "
            f"reference DECODER's, not the format's."
        )
    else:
        print("the stated sets are decoder-independent across these geometries.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
