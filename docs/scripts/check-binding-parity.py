#!/usr/bin/env python3
"""Verify every registered GeoBrix function exists across all bindings.

Single canonical name per function (no aliases). The source of truth for the registered
SQL surface is the function manifest,
``python/geobrix/src/databricks/labs/gbx/manifest/functions.json`` (one row per function,
written by ``gbx:manifest:gen``); the legacy list
``docs/tests-function-info/registered_functions.txt`` is read too, so a name left in that
list without a manifest row is caught as stale. Every stable name must also appear as:

  * a Scala expression companion  -> ``override def name: String = "gbx_..."``, required
                                     when the manifest row has an L1 (JVM) level
  * a Python binding              -> a ``"gbx_..."`` string literal in a
                                     ``python/geobrix/src/.../functions.py``
  * the heavy Python module       -> for an L1 row, a literal in the ``functions.py`` of
                                     the row's own package (``python.module`` or the
                                     package of its SQL prefix)
  * a function-info.json entry    -> a top-level key under ``functions``

A function missing from any binding is a hard failure (it surfaces at runtime as
``UNRESOLVED_ROUTINE`` or as an undocumented/uncallable function). Names that appear in a
binding but not in the canonical set are reported as warnings (e.g. an expression whose
registration is intentionally commented out), not failures.

An empty source is a failure, never a pass: zero canonical functions, or a binding scan that
finds nothing, means a source moved and this check would otherwise verify nothing.

Exit code: 0 when every canonical function is present in every binding, 1 otherwise.

Run via ``gbx:test:bindings`` (or directly). Pure stdlib; runs on the host, no Docker.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _manifest_names import stable_rows  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_TXT = REPO_ROOT / "docs/tests-function-info/registered_functions.txt"
FUNCTION_INFO_JSON = REPO_ROOT / "src/main/resources/com/databricks/labs/gbx/function-info.json"
SCALA_ROOT = REPO_ROOT / "src/main/scala"
PYTHON_ROOT = REPO_ROOT / "python/geobrix/src"

# `override def name: String = "gbx_..."` — the canonical SQL name a companion registers under.
SCALA_NAME_RE = re.compile(r'override\s+def\s+name\s*:\s*String\s*=\s*"(gbx_[a-z0-9_]+)"')
# A quoted gbx_ literal (call_function("gbx_..."))/('gbx_...'); quoting excludes docstring
# fragments like `gbx_rst_*` that would otherwise match a bare token.
PY_NAME_RE = re.compile(r"""["'](gbx_[a-z0-9_]+)["']""")

# The Python API module of a SQL prefix (gbx_<prefix>_...); mirrors manifest.PACKAGE_OF_PREFIX
# (a test pins the two together). A row's own ``python.module`` overrides it.
PACKAGE_OF_PREFIX = {
    "rst": "rasterx",
    "bng": "gridx.bng",
    "quadbin": "gridx.quadbin",
    "custom": "gridx.custom",
    "h3": "gridx.h3",
    "st": "vectorx",
    "pmtiles": "pmtiles",
}


def legacy_sql() -> set[str]:
    """Names in the legacy registered_functions.txt (kept byte-identical to the bench wheel's copy)."""
    names = set()
    for line in REGISTERED_TXT.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.add(line)
    return names


def manifest_rows() -> dict[str, dict]:
    """Stable manifest rows by SQL name: the registered SQL surface."""
    return stable_rows()


def canonical_sql() -> set[str]:
    return set(manifest_rows())


def function_info_keys() -> set[str]:
    data = json.loads(FUNCTION_INFO_JSON.read_text())
    funcs = data.get("functions", {})
    return {k for k in funcs if k.startswith("gbx_")}


def scala_names() -> set[str]:
    names = set()
    for path in SCALA_ROOT.rglob("*.scala"):
        names.update(SCALA_NAME_RE.findall(path.read_text()))
    return names


def python_names() -> set[str]:
    names = set()
    for path in PYTHON_ROOT.rglob("functions.py"):
        names.update(PY_NAME_RE.findall(path.read_text()))
    return names


def heavy_module_path(sql_name: str, row: dict, root: Path = PYTHON_ROOT) -> Path | None:
    """The ``functions.py`` of the row's own package, or None for a row with no Python entry (a probe)."""
    py = row.get("python", "derive")
    if py is None:
        return None
    module = py.get("module") if isinstance(py, dict) else None
    module = module or PACKAGE_OF_PREFIX[sql_name.split("_")[1]]
    return root / "databricks/labs/gbx" / module.replace(".", "/") / "functions.py"


def has_scala_level(row: dict) -> bool:
    return "L1" in row.get("levels", {})


def check_heavy_module_coverage(rows: dict[str, dict], root: Path = PYTHON_ROOT) -> dict[Path, list[str]]:
    """Check that each row with a JVM level (L1) is wrapped in ITS package's heavy module.

    A name defined only in another module (e.g. the light ``pyrx``/``pyvx`` registration
    tables) does not count. Rows without an L1 level are light-only and exempt.

    Returns a dict mapping each heavy module path to a list of missing function names
    that should be defined there.
    """
    module_names: dict[Path, set[str]] = {}
    missing_per_module: dict[Path, list[str]] = {}
    for name in sorted(rows):
        row = rows[name]
        if not has_scala_level(row):
            continue
        path = heavy_module_path(name, row, root)
        if path is None:
            continue
        if path not in module_names:
            module_names[path] = set(PY_NAME_RE.findall(path.read_text())) if path.exists() else set()
        if name not in module_names[path]:
            missing_per_module.setdefault(path, []).append(name)
    return missing_per_module


def _rel(path: Path) -> Path:
    return path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path


def main() -> int:
    for required in (REGISTERED_TXT, FUNCTION_INFO_JSON):
        if not required.exists():
            print(f"❌ missing required file: {required}", file=sys.stderr)
            return 1

    rows = manifest_rows()
    sql = canonical_sql()
    listed = legacy_sql()
    bindings = {
        "Scala (override def name)": scala_names(),
        "Python (functions.py)": python_names(),
        "function-info.json": function_info_keys(),
    }
    scala_required = {n for n in sql if n not in rows or has_scala_level(rows[n])}

    print(f"Canonical registered functions (manifest, stable): {len(sql)}")
    print(f"  of which with a Scala (L1) level: {len(scala_required)}")
    for label, found in bindings.items():
        print(f"  {label}: {len(found)}")
    print()

    failed = False

    # A silent zero must never pass: an empty source means a path moved, and nothing was checked.
    if not sql:
        failed = True
        print("❌ 0 canonical functions: the manifest has no stable rows (did "
              "python/geobrix/src/databricks/labs/gbx/manifest/functions.json move?) -- nothing was checked.")
    for label, found in bindings.items():
        if not found:
            failed = True
            print(f"❌ {label} found 0 names: its source moved or is unreadable -- nothing was checked.")

    # Stale legacy list: a name listed with no manifest row has no source of truth.
    stale = sorted(listed - sql)
    if stale:
        failed = True
        print(f"❌ {len(stale)} name(s) in docs/tests-function-info/registered_functions.txt have no stable "
              "manifest row (add the row, or remove the name):")
        for name in stale:
            print(f"     - {name}")

    # Drift guard: the bench wheel ships a copy of registered_functions.txt (so
    # spec.registered_rst works on a cluster with no repo tree). It MUST stay
    # byte-identical to the canonical file here.
    packaged = PYTHON_ROOT / "databricks/labs/gbx/bench/registered_functions.txt"
    if not packaged.exists():
        failed = True
        print(f"❌ packaged copy missing: {_rel(packaged)} "
              "(spec.registered_rst's cluster fallback needs it)")
    elif packaged.read_text() != REGISTERED_TXT.read_text():
        failed = True
        print(f"❌ packaged copy drifted from canonical: {_rel(packaged)} "
              f"!= {_rel(REGISTERED_TXT)} -- re-copy the canonical file.")

    # Each JVM-level function has a wrapper in its own package's heavy module.
    heavy_missing = check_heavy_module_coverage(rows)
    for module_path in sorted(heavy_missing):
        failed = True
        missing_names = heavy_missing[module_path]
        print(f"❌ {len(missing_names)} canonical function(s) missing from heavy module "
              f"{_rel(module_path)}:")
        for name in missing_names:
            print(f"     - {name}")

    for label, found in bindings.items():
        # A row without an L1 level has no Scala expression by design (light-only, or a Python
        # registration); it still needs a Python binding and a function-info.json entry.
        required = scala_required if label == "Scala (override def name)" else sql
        missing = sorted(required - found)
        if missing:
            failed = True
            print(f"❌ {len(missing)} canonical function(s) missing from {label}:")
            for name in missing:
                print(f"     - {name}")

    # Extras are informational: a binding name not in the canonical set (e.g. an
    # expression whose rd.register(...) is commented out). Not a failure.
    for label, found in bindings.items():
        extra = sorted(found - sql - listed)
        if extra:
            print(f"ℹ️  {len(extra)} name(s) in {label} not in the canonical list (ignored): "
                  f"{', '.join(extra)}")

    print()
    if failed:
        print("❌ binding parity FAILED — every registered function must exist in all bindings.")
        print("   Fix the missing binding(s) or the manifest row (gbx:manifest:gen), or remove the "
              "name from docs/tests-function-info/registered_functions.txt if it was withdrawn.")
        return 1
    print(f"✅ binding parity OK — all {len(sql)} manifest functions exist in Python and "
          f"function-info.json, and the {len(scala_required)} with a JVM level in Scala and their heavy module.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
