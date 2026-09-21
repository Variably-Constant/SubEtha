"""Separate the dropped errors that matter from the test cleanup.

A grep for `.ok();` in this crate returns about a hundred and ten hits,
and almost all of them are a test removing a file or joining a thread.
Reading that list by hand is how a survey takes an afternoon and still
gets an answer wrong, so this does the separation instead:

    python tools/dropped_errors.py            # the production sites
    python tools/dropped_errors.py --all      # every site, grouped

Three patterns, because each drops a Result in a different disguise:
`.ok();` discards it outright, `unwrap_or_default()` substitutes a
value for it, and `if let Ok(` walks past it.

TWO THINGS THE OBVIOUS VERSION GETS WRONG, both of which have:

  A test module is not always at column zero and is not always spelled
  `#[cfg(test)]`. fd_handoff.rs gates per platform with `mod tests_unix`
  and `mod tests_windows`, so a site inside one reads as production
  until somebody opens the file. Both spellings are matched, anywhere on
  the line.

  `std::env::var("NAME").ok()` is not a dropped error. It is the
  Result-to-Option conversion for an optional environment variable, and
  a regex-driven rewrite that does not exclude it breaks working code.

Report only: it exits zero whatever it finds, unless `--fail-over` is
given a number the production count must not exceed.
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
SRC = HERE.parent / "src"

PATTERNS = {
    ".ok();": re.compile(r"\.ok\(\);"),
    "unwrap_or_default()": re.compile(r"\.unwrap_or_default\(\)"),
    "if let Ok(": re.compile(r"if let Ok\("),
}

# The Result-to-Option conversion for an optional environment variable.
# It reads as a dropped error and is not one.
ENV_VAR = re.compile(r"env::var\([^)]*\)\.ok\(\)")

TEST_REGION = re.compile(r"cfg\(test\)|\bmod\s+tests")


def first_test_line(lines: list[str]) -> int:
    """Where this file stops being production code.

    Everything from the first test gate onward is test code. A file with
    no gate is production throughout, which is what the length gives.
    """
    for i, line in enumerate(lines):
        if TEST_REGION.search(line):
            return i
    return len(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=str(SRC))
    ap.add_argument(
        "--all",
        action="store_true",
        help="list the test sites too, not only the production ones",
    )
    ap.add_argument(
        "--fail-over",
        type=int,
        default=None,
        help="exit non-zero when more production sites than this are found",
    )
    args = ap.parse_args()

    root = pathlib.Path(args.src)
    if not root.is_dir():
        print(f"no source directory at {root}")
        return 2

    production: list[tuple[str, str, int, str]] = []
    excluded: list[tuple[str, int, str]] = []
    test_counts: dict[str, dict[str, int]] = {}
    totals = dict.fromkeys(PATTERNS, 0)

    for path in sorted(root.rglob("*.rs")):
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        boundary = first_test_line(lines)
        rel = path.relative_to(root).as_posix()
        for i, line in enumerate(lines):
            for label, pattern in PATTERNS.items():
                if not pattern.search(line):
                    continue
                totals[label] += 1
                if i >= boundary:
                    test_counts.setdefault(rel, dict.fromkeys(PATTERNS, 0))
                    test_counts[rel][label] += 1
                elif label == ".ok();" and ENV_VAR.search(line):
                    excluded.append((rel, i + 1, line.strip()))
                else:
                    production.append((label, rel, i + 1, line.strip()))

    print("counted, not estimated:")
    for label, n in totals.items():
        print(f"  {label:<22} {n}")
    print()

    print(f"PRODUCTION SITES: {len(production)}")
    for label, rel, line_no, text in production:
        print(f"  [{label}] {rel}:{line_no}")
        print(f"      {text}")
    if not production:
        print("  none")
    print()

    if excluded:
        print(f"NOT DROPPED ERRORS, excluded: {len(excluded)}")
        for rel, line_no, text in excluded:
            print(f"  {rel}:{line_no}  {text}")
        print()

    in_tests = sum(sum(v.values()) for v in test_counts.values())
    print(f"IN TEST CODE: {in_tests} across {len(test_counts)} files")
    if args.all:
        for rel, counts in sorted(
            test_counts.items(), key=lambda kv: -sum(kv[1].values())
        ):
            parts = ", ".join(f"{k} {n}" for k, n in counts.items() if n)
            print(f"  {rel}: {parts}")

    if args.fail_over is not None and len(production) > args.fail_over:
        print()
        print(
            f"{len(production)} production sites, more than the "
            f"{args.fail_over} this was run against"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
