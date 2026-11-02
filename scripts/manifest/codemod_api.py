"""Point callers of the light Column API at the generated Python API (a migration tool).

For the given SQL names: `<alias>.<name>(` where <alias> is bound to pyrx/pygx/pyvx `functions` becomes
`<new alias>.<name>(` with the generated module imported under that alias; `from ...pyrx.functions import x`
of a moved name is reported for a hand edit. When the new alias is already bound to something else in the
file (often the light module itself, `rx` for pyrx), the import uses `<alias>_api` instead, so the light
module's other calls keep their meaning. Notebooks (.ipynb) are edited cell by cell, kept at indent 1, the
file's own ensure_ascii, and a trailing newline; a notebook with no change is never rewritten, and one
whose JSON layout does not round-trip is reported instead. Anything else
is printed as UNRESOLVED for a hand edit. `--dry-run` prints what would change and writes nothing.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

LIGHT = r"databricks\.labs\.gbx\.(pyrx|pygx|pyvx)"
ALIAS = {
    "rasterx": "rx",
    "gridx.bng": "bx",
    "gridx.quadbin": "qx",
    "gridx.custom": "cx",
    "gridx.h3": "hx",
    "vectorx": "vx",
    "pmtiles": "pmx",
}


def _targets(names) -> Dict[str, str]:
    from databricks.labs.gbx import manifest

    man = manifest.load()
    return {
        man.functions[n].python.name: man.functions[n].python.module
        for n in names
        if man.functions[n].python
    }


def _import_line(module: str, alias: str) -> str:
    return f"from databricks.labs.gbx.{module} import functions as {alias}"


def _alias_for(text: str, module: str) -> Tuple[str, bool]:
    """(alias, needs an import): the module's short alias when it is free (or already this module's)."""
    for alias in (ALIAS[module], ALIAS[module] + "_api"):
        same = rf"^\s*import databricks\.labs\.gbx\.{re.escape(module)}\.functions as {alias}\b"
        if _import_line(module, alias) in text or re.search(same, text, re.M):
            return alias, False
        bound = re.search(rf"\bas {alias}\b", text) or re.search(
            rf"^\s*{alias}\s*=", text, re.M
        )
        if not bound:
            return alias, True
    return "", False


def _after_docstring(text: str) -> int:
    """The offset after a module's docstring and __future__ imports (0 when it has neither)."""
    import ast

    try:
        body = ast.parse(text).body
    except SyntaxError:  # a notebook cell with magics: the top is the place
        return 0
    line = 0
    for node in body:
        doc = isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
        future = isinstance(node, ast.ImportFrom) and node.module == "__future__"
        if not (doc and line == 0) and not future:
            break
        line = node.end_lineno
    lines = text.splitlines(keepends=True)
    return sum(len(x) for x in lines[:line])


def rewrite(
    text: str, targets: Dict[str, str], context: str = ""
) -> Tuple[str, List[str]]:
    """``context``: the text that binds names for ``text`` too (a notebook's other cells)."""
    unresolved: List[str] = []
    scope = context + "\n" + text
    aliases = set(re.findall(rf"from {LIGHT} import functions as (\w+)", scope))
    aliases |= set(re.findall(rf"import {LIGHT}\.functions as (\w+)", scope))
    aliases = {a[1] if isinstance(a, tuple) else a for a in aliases}
    chosen: Dict[str, Tuple[str, bool]] = {}
    for alias in sorted(aliases):
        for name, module in targets.items():
            pat = re.compile(rf"\b{re.escape(alias)}\.{re.escape(name)}\(")
            if not pat.search(text):
                continue
            if module not in chosen:
                chosen[module] = _alias_for(scope, module)
            new, _ = chosen[module]
            if not new:
                unresolved.append(
                    f"no free alias for {module} ({ALIAS[module]}, {ALIAS[module]}_api)"
                )
                continue
            text = pat.sub(f"{new}.{name}(", text)
    imported = set(re.findall(r"\bimport [\w.]+ as (\w+)", scope))
    imported |= set(
        re.findall(r"\bfrom [\w.]+ import (?:\w+ as )?(\w+)\s*$", scope, re.M)
    )
    imported |= {alias for alias, _ in chosen.values()}
    for name in targets:
        for m in re.finditer(rf"(?<![\w.])(\w+)\.{re.escape(name)}\(", text):
            if m.group(1) not in imported:
                line = text.count("\n", 0, m.start()) + 1
                unresolved.append(
                    f"line {line}: {m.group(1)}.{name}( calls through a name this file does not "
                    "import (a fixture, a parameter, or a %run notebook's binding): check by hand"
                )
    for m in re.finditer(rf"from {LIGHT}\.functions import (\([^)]*\)|[^\n(]+)", text):
        listed = re.split(r"[\s,()]+", m.group(2))
        moved = sorted({n.split(" as ")[0] for n in listed if n in targets})
        if moved:
            unresolved.append(
                f"direct import of {moved}: move to the generated module by hand"
            )
    for module, (alias, needs) in sorted(chosen.items()):
        imp = _import_line(module, alias) + "\n"
        if needs and imp.strip() not in scope:
            # the header's import block: top-level imports before the first def, class or decorator
            body = re.search(r"^(def|class|async def|@)\b", text, re.M)
            head = text[: body.start()] if body else text
            anchor = list(
                re.finditer(r"^(from|import) databricks\.labs\.gbx[^\n]*\n", head, re.M)
            )
            anchor = anchor or list(re.finditer(r"^(from|import) [^\n]*\n", head, re.M))
            at = anchor[-1].end() if anchor else _after_docstring(text)
            if anchor and "noqa: E402" in anchor[-1].group(0):
                imp = (
                    imp.rstrip("\n") + "  # noqa: E402\n"
                )  # the header's imports follow code
            text = text[:at] + imp + text[at:]
    return text, unresolved


def _cell_text(cell) -> str:
    src = cell["source"]
    return "".join(src) if isinstance(src, list) else src


def _notebook(path: Path, targets) -> Tuple[str, str, List[str]]:
    raw = path.read_text()
    nb = json.loads(raw)
    issues, changed = [], False
    code = [c for c in nb.get("cells", []) if c.get("cell_type") == "code"]
    for i, cell in enumerate(code):
        src = _cell_text(cell)
        # earlier cells bind names for this one (later cells bind nothing yet)
        new, un = rewrite(src, targets, "\n".join(_cell_text(c) for c in code[:i]))
        issues += un
        if new != src:
            changed = True
            cell["source"] = (
                new.splitlines(keepends=True)
                if isinstance(cell["source"], list)
                else new
            )
    if not changed:
        return raw, raw, issues
    ascii_only = not any(ord(ch) > 127 for ch in raw)
    if json.dumps(json.loads(raw), indent=1, ensure_ascii=ascii_only) + "\n" != raw:
        # rewriting would reformat cells it did not change: report it for a hand edit instead
        return (
            raw,
            raw,
            issues
            + [
                "the notebook's JSON layout does not round-trip (indent 1); edit its cells by hand"
            ],
        )
    return raw, json.dumps(nb, indent=1, ensure_ascii=ascii_only) + "\n", issues


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--names", nargs="+", default=[])
    ap.add_argument("--names-from-family")
    ap.add_argument(
        "--dry-run", action="store_true", help="print what would change; write nothing"
    )
    ap.add_argument("paths", nargs="*")
    a = ap.parse_args(argv)
    from databricks.labs.gbx import manifest

    # `--names a b path1 path2`: argparse gives the paths to --names; a SQL name starts with gbx_.
    names = [n for n in a.names if n.startswith("gbx_")]
    paths = [n for n in a.names if not n.startswith("gbx_")] + list(a.paths)
    if not paths:
        ap.error("give at least one path")
    if a.names_from_family:
        names += [
            f.sql_name
            for f in manifest.load().functions.values()
            if f.family == a.names_from_family
        ]
    targets = _targets(names)
    files = [
        p
        for root in paths
        for p in (Path(root).rglob("*") if Path(root).is_dir() else [Path(root)])
    ]
    for path in sorted(f for f in files if f.suffix in (".py", ".ipynb")):
        if path.suffix == ".ipynb":
            old, new, issues = _notebook(path, targets)
        else:
            old = path.read_text()
            new, issues = rewrite(old, targets)
        if new != old:
            print(f"{'WOULD CHANGE' if a.dry_run else 'CHANGED'} {path}")
            if not a.dry_run:
                path.write_text(new)
        for issue in issues:
            print(f"UNRESOLVED {path}: {issue}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
