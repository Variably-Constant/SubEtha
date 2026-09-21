"""Generate the complete Python reference from the type stub.

The stub at python/subetha/__init__.pyi declares every class, method and
signature the extension exports, and tests/test_surface.py holds it
against the compiled module, so a name in one and not the other fails
the suite. That makes it the right source for a reference: it carries
the annotations the compiled module does not, and it cannot drift
without the tests saying so.

    python tools/export_reference.py --section classes --out ../../wiki/content/docs/reference/subetha-py/classes.md
    python tools/export_reference.py --section module  --out ../../wiki/content/docs/reference/subetha-py/module.md

Reads only; writes one section per run.
"""
from __future__ import annotations

import argparse
import ast
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
STUB = HERE.parent / "python" / "subetha" / "__init__.pyi"
RUST = HERE.parent / "src" / "lib.rs"


PYCLASS_NAME_RE = re.compile(r'#\[pyclass\([^)]*name\s*=\s*"([^"]+)"')
STRUCT_RE = re.compile(r"^\s*(?:pub\s+)?struct\s+([A-Za-z_][A-Za-z0-9_]*)")


def pyclass_renames(lines: list[str]) -> dict[str, str]:
    """Rust struct name mapped onto the name Python sees.

    `#[pyclass(name = "Vec")]` over `struct Vec_` means the stub declares
    `class Vec`, so a doc comment found under `impl Vec_` has to be filed
    under `Vec` or it never reaches the method it belongs to. Without this
    every method of a renamed class reads as undocumented, which is a
    claim about the surface rather than a cosmetic miss.
    """
    renames: dict[str, str] = {}
    pending: str | None = None
    for raw in lines:
        found = PYCLASS_NAME_RE.search(raw)
        if found:
            pending = found.group(1)
            continue
        if pending is None:
            continue
        declared = STRUCT_RE.match(raw)
        if declared:
            renames[declared.group(1)] = pending
            pending = None
        elif raw.strip() and not raw.strip().startswith("#["):
            # Anything between the attribute and its struct that is
            # neither means the two are unrelated; forget the name rather
            # than attach it to whatever declares itself next.
            pending = None
    return renames


def rust_docs(path: pathlib.Path) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """What each method does, read from the Rust doc comments.

    PyO3 turns a `///` comment into the object's __doc__, so the prose a
    user sees from help() lives in the Rust source and not in the type
    stub. Reading it here is what lets the generated page carry both the
    signature and the description.

    Returns (class, python name) -> first line, and a second map for the
    module-level #[pyfunction]s.
    """
    if not path.is_file():
        return {}, {}
    lines = path.read_text(encoding="utf-8").splitlines()
    renames = pyclass_renames(lines)

    methods: dict[tuple[str, str], str] = {}
    functions: dict[str, str] = {}
    current: str | None = None
    depth = 0
    pending: list[str] = []
    attrs: list[str] = []
    in_pymethods = False
    # An attribute may run over several lines. Its continuation lines
    # look like ordinary code, so without counting the brackets they
    # reset the pending doc comment and every method carrying a wrapped
    # `#[pyo3(signature = ...)]` reads as undocumented.
    attr_open = 0

    fn_re = re.compile(r"^\s*(?:pub\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)")
    impl_re = re.compile(r"^\s*impl\s+([A-Za-z_][A-Za-z0-9_]*)")
    name_re = re.compile(r'name\s*=\s*"([^"]+)"')

    for raw in lines:
        line = raw.rstrip()
        stripped = line.strip()

        if stripped == "#[pymethods]":
            in_pymethods = True
            pending, attrs = [], []
            continue

        if in_pymethods and current is None:
            m = impl_re.match(line)
            if m:
                current = renames.get(m.group(1), m.group(1))
                depth = line.count("{") - line.count("}")
                pending, attrs = [], []
                continue

        if current is not None:
            depth += line.count("{") - line.count("}")
            if depth <= 0:
                current, in_pymethods = None, False
                pending, attrs = [], []
                continue

        if attr_open > 0:
            attrs.append(stripped)
            attr_open += line.count("(") - line.count(")")
            continue

        if stripped.startswith("///"):
            pending.append(stripped[3:].strip())
            continue
        if stripped.startswith("#["):
            attrs.append(stripped)
            attr_open = max(0, line.count("(") - line.count(")"))
            continue

        m = fn_re.match(line)
        if m and pending:
            # The first paragraph only. A Rust doc comment's opening
            # paragraph is its summary by convention, and the rest is
            # detail that belongs in `help()` rather than in a table
            # cell: pasting all of it in makes rows hundreds of
            # characters wide and the table unreadable.
            summary: list[str] = []
            for part in pending:
                if not part:
                    break
                summary.append(part)
            doc = " ".join(" ".join(summary).split()).replace("|", r"\|")
            attr_text = " ".join(attrs)
            rust_name = m.group(1)
            renamed = name_re.search(attr_text)
            if "#[new]" in attr_text:
                py_name = "__init__"
            elif renamed:
                py_name = renamed.group(1)
            else:
                py_name = rust_name
            if current:
                methods[(current, py_name)] = doc
            elif "#[pyfunction]" in attr_text:
                functions[py_name] = doc
        if stripped and not stripped.startswith("//"):
            pending, attrs = [], []

    return methods, functions


def first_line(node: ast.AST) -> str:
    """The docstring's first line, flattened for a table cell."""
    doc = ast.get_docstring(node)
    if not doc:
        return ""
    return " ".join(doc.strip().split("\n")[0].split()).replace("|", r"\|")


def signature(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """The def line as written, without the body."""
    args = ast.unparse(fn.args)
    ret = f" -> {ast.unparse(fn.returns)}" if fn.returns else ""
    prefix = "async " if isinstance(fn, ast.AsyncFunctionDef) else ""
    return f"{prefix}{fn.name}({args}){ret}"


def decorators(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    names = []
    for d in fn.decorator_list:
        text = ast.unparse(d)
        if text in ("staticmethod", "classmethod", "property"):
            names.append(text)
    return ", ".join(names)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--section", required=True, choices=["classes", "module"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--stub", default=str(STUB))
    args = ap.parse_args()

    source = pathlib.Path(args.stub).read_text(encoding="utf-8")
    tree = ast.parse(source)
    rust_methods, rust_functions = rust_docs(RUST)

    classes = [n for n in tree.body if isinstance(n, ast.ClassDef)]
    functions = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    assignments = [n for n in tree.body if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name)]

    def is_exception(c: ast.ClassDef) -> bool:
        return any(ast.unparse(b).endswith("Exception") or ast.unparse(b).endswith("Error") for b in c.bases)

    out: list[str] = []

    if args.section == "classes":
        concrete = [c for c in classes if not is_exception(c)]
        out += [
            "---",
            'title: "Every class, in full"',
            "weight: 10",
            "---",
            "",
            "# Every class, in full",
            "",
            f"All {len(concrete)} classes the extension exports, with every method,",
            "its full signature and what it answers. Generated from the type",
            "stub by `crates/subetha-py/tools/export_reference.py`; the stub is",
            "held against the compiled module by `tests/test_surface.py`, so a",
            "name here is a name that ships.",
            "",
            "A method answering `T | None` uses `None` for an ordinary absent",
            "answer rather than a fault, the same way the other bindings use",
            "their empty value.",
            "",
            "Signatures come from the stub and descriptions from the Rust doc",
            "comments. `help()` in a REPL shows both, because PyO3 derives",
            "`__text_signature__` from the `#[pyo3(signature = ...)]`",
            "attribute; this page exists to read the surface whole rather",
            "than a name at a time.",
            "",
            "Each description here is the opening paragraph of the method's",
            "doc comment. Many carry more than that, on what an answer means",
            "or what the call does not do, and `help()` shows all of it.",
            "",
            "@@COVERAGE@@",
            "",
            "## Contents",
            "",
            ", ".join(
                f"[`{c.name}`](#{c.name.lower()})"
                for c in sorted(concrete, key=lambda n: n.name)
            ),
            "",
        ]
        for c in sorted(concrete, key=lambda n: n.name):
            out.append(f"## {c.name}")
            out.append("")
            doc = first_line(c)
            if doc:
                out += [doc, ""]
            everything = [m for m in c.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))]
            # A @property is read as an attribute, not called, so it does
            # not belong in a table of call signatures.
            props = [m for m in everything if "property" in decorators(m)]
            members = [m for m in everything if "property" not in decorators(m)]
            fields = [m for m in c.body if isinstance(m, ast.AnnAssign) and isinstance(m.target, ast.Name)]
            if fields or props:
                out += ["| Attribute | Type |", "|---|---|"]
                for f in fields:
                    out.append(f"| `{f.target.id}` | `{ast.unparse(f.annotation)}` |")
                for p in sorted(props, key=lambda n: n.name):
                    ptype = ast.unparse(p.returns) if p.returns else ""
                    out.append(f"| `{p.name}` | `{ptype}` |")
                out.append("")
            if members:
                def describe(m):
                    return first_line(m) or rust_methods.get((c.name, m.name), "")

                described = any(describe(m) for m in members)
                if described:
                    out += ["| Method | Kind | What it does |", "|---|---|---|"]
                else:
                    out += ["| Method | Kind |", "|---|---|"]
                for m in sorted(members, key=lambda n: (n.name.startswith("__"), n.name)):
                    kind = decorators(m) or "method"
                    if described:
                        out.append(f"| `{signature(m)}` | {kind} | {describe(m)} |")
                    else:
                        out.append(f"| `{signature(m)}` | {kind} |")
                out.append("")
            if not members and not fields and not props:
                out += ["_Carries no members of its own._", ""]

    if args.section == "module":
        exceptions = [c for c in classes if is_exception(c)]
        out += [
            "---",
            'title: "Module functions, attributes and exceptions"',
            "weight: 20",
            "---",
            "",
            "# Module functions, attributes and exceptions",
            "",
            "What `import subetha` gives you besides the classes. Generated",
            "from the type stub by",
            "`crates/subetha-py/tools/export_reference.py`.",
            "",
        ]
        if functions:
            out += ["## Functions", "", "| Function | What it does |", "|---|---|"]
            for f in sorted(functions, key=lambda n: n.name):
                desc = first_line(f) or rust_functions.get(f.name, "")
                out.append(f"| `{signature(f)}` | {desc} |")
            out.append("")
        if assignments:
            out += ["## Attributes", "", "| Attribute | Type |", "|---|---|"]
            for a in assignments:
                out.append(f"| `{a.target.id}` | `{ast.unparse(a.annotation)}` |")
            out.append("")
        if exceptions:
            out += ["## Exceptions", "", "| Exception | Raised when |", "|---|---|"]
            for c in sorted(exceptions, key=lambda n: n.name):
                out.append(f"| `{c.name}` | {first_line(c)} |")
            out.append("")

    if "@@COVERAGE@@" in out:
        total = described_count = 0
        for c in classes:
            if is_exception(c):
                continue
            for m in c.body:
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and "property" not in decorators(m):
                    total += 1
                    if first_line(m) or rust_methods.get((c.name, m.name), ""):
                        described_count += 1
        missing = total - described_count
        pct = round(100 * described_count / total) if total else 0
        out[out.index("@@COVERAGE@@")] = (
            f"All {total} methods carry a description."
            if missing == 0
            else (
                f"Of {total} methods, {described_count} carry a description "
                f"and {missing} do not ({pct}% covered)."
            )
        )

    text = "\n".join(out) + "\n"
    dest = pathlib.Path(args.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text, encoding="utf-8")
    print(f"wrote {args.section} to {dest} ({len(out)} lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
