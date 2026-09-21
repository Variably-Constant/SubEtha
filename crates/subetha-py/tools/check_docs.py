"""Fail when a method on the compiled module has nothing for help().

The reference page counts descriptions by reading the Rust source. That
is a claim about what a user sees, and it can be wrong in both
directions: a parser gap reports a documented method as bare, and a doc
comment in a position PyO3 does not carry never reaches `__doc__` at
all. Neither shows up until somebody types help() and gets nothing.

So this asks the built module instead. It takes the set of methods from
the type stub, which is the same set the reference counts and which
tests/test_surface.py holds against the module, then reads `__doc__` off
the compiled object for each one.

Run it against an installed module, not a source tree:

    python -m maturin develop --release
    python tools/check_docs.py

Exits non-zero and names every bare method, so a run that passes is
evidence and a run that fails is a list of work.
"""
from __future__ import annotations

import argparse
import ast
import importlib
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
STUB = HERE.parent / "python" / "subetha" / "__init__.pyi"


def declared(stub: pathlib.Path) -> dict[str, list[str]]:
    """Class name mapped to the methods the stub declares for it.

    Properties are left out: a property is read rather than called, so
    `help()` reaches it through the class and not through a call
    signature. Exception classes are left out too; what they carry is
    the class docstring, which the module page prints.
    """
    tree = ast.parse(stub.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if any(
            ast.unparse(base).endswith(("Exception", "Error")) for base in node.bases
        ):
            continue
        names = []
        for member in node.body:
            if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if any(
                ast.unparse(d) == "property" for d in member.decorator_list
            ):
                continue
            names.append(member.name)
        if names:
            out[node.name] = names
    return out


def described(cls: type, name: str) -> bool:
    """Whether `help()` would print prose for this method.

    `__init__` is the constructor's doc, which PyO3 puts on the class
    rather than on the function, so the class docstring is what a caller
    sees for it. An attribute the module does not carry is not this
    tool's business: tests/test_surface.py is what holds the stub and
    the module to each other, and reporting it here as undocumented
    would name the wrong defect.
    """
    if name == "__init__":
        text = (cls.__doc__ or "").strip()
        own = getattr(cls, "__init__", None)
        return bool(text) or bool((getattr(own, "__doc__", "") or "").strip())
    member = getattr(cls, name, None)
    if member is None:
        return True
    return bool((getattr(member, "__doc__", "") or "").strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--module", default="subetha")
    ap.add_argument("--stub", default=str(STUB))
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="print the tally only, not the per-class lines",
    )
    args = ap.parse_args()

    module = importlib.import_module(args.module)
    print(f"reading {module.__file__}")

    total = bare_count = 0
    bare: list[tuple[str, str]] = []
    for class_name, methods in sorted(declared(pathlib.Path(args.stub)).items()):
        cls = getattr(module, class_name, None)
        if cls is None:
            # The stub names a class the module does not export. That is
            # a surface mismatch and test_surface.py is what reports it.
            continue
        for method in methods:
            total += 1
            if not described(cls, method):
                bare_count += 1
                bare.append((class_name, method))

    if bare and not args.quiet:
        for class_name, method in bare:
            print(f"  {class_name}.{method} has no description")

    if bare_count:
        print(f"{bare_count} of {total} methods have nothing for help()")
        return 1
    print(f"all {total} methods carry a description")
    return 0


if __name__ == "__main__":
    sys.exit(main())
