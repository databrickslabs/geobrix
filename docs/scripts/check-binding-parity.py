#!/usr/bin/env python3
"""Verify every registered GeoBrix function exists across all bindings.

Single canonical name per function (no aliases). The source of truth for the
registered SQL surface is ``docs/tests-function-info/registered_functions.txt``.
Every name listed there must also appear as:

  * a Scala expression companion  -> ``override def name: String = "gbx_..."``
  * a Python binding              -> a ``"gbx_..."`` string literal in a
                                     ``python/geobrix/src/.../functions.py``
  * a function-info.json entry    -> a top-level key under ``functions``

A function missing from any binding is a hard failure (it surfaces at runtime as
``UNRESOLVED_ROUTINE`` or as an undocumented/uncallable function). Extra names
that appear in a binding but not in the canonical list are reported as warnings
(e.g. an expression whose registration is intentionally commented out), not
failures.

Exit code: 0 when every canonical function is present in every binding, 1 otherwise.

Run via ``gbx:test:bindings`` (or directly). Pure stdlib; runs on the host, no Docker.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_TXT = REPO_ROOT / "docs/tests-function-info/registered_functions.txt"
FUNCTION_INFO_JSON = REPO_ROOT / "src/main/resources/com/databricks/labs/gbx/function-info.json"
SCALA_ROOT = REPO_ROOT / "src/main/scala"
PYTHON_ROOT = REPO_ROOT / "python/geobrix/src"

# Functions registered as a Python UDF (via spark.udf.register in the Python `register`), NOT a
# Scala expression companion, because they cannot be implemented in the JVM tier. gbx_rst_fromfile
# reads a file path, and on Databricks only the credentialed Python worker can read a UC Volume
# (/Volumes) FUSE path -- the executor JVM cannot. These are EXEMPT from the Scala-expression
# requirement; they still require a Python binding and a function-info.json entry. Issue #34.
PYTHON_REGISTERED = {
    "gbx_rst_fromfile",
    "gbx_st_explainvalidity",
    "gbx_st_makevalid",
    "gbx_st_node",
    "gbx_st_reduceprecision",
    "gbx_st_removerepeatedpoints",
    "gbx_st_shiftlongitude",
    "gbx_st_simplifypreservetopology",
    "gbx_st_snap",
    "gbx_st_split",
    "gbx_st_wrapx",
    # coverage validity (light-only)
    "gbx_st_coverageisvalid",
    "gbx_st_coverageinvalidedges",
    # H3 geom-aware kring/kloop — light-only (no Scala/heavy equivalent).
    # These are PySpark UDF/UDTF registrations in pygx; the geometry work
    # (cover/core arrays) is supplied by product h3_* SQL functions at runtime.
    "gbx_h3_geomkring",
    "gbx_h3_geomkloop",
    "gbx_h3_geomkringexplode",
    "gbx_h3_geomkloopexplode",
    # rst_ light-only classifier (no Scala/heavy equivalent)
    "gbx_rst_land_cover",
}

# `override def name: String = "gbx_..."` — the canonical SQL name a companion registers under.
SCALA_NAME_RE = re.compile(r'override\s+def\s+name\s*:\s*String\s*=\s*"(gbx_[a-z0-9_]+)"')
# A quoted gbx_ literal (call_function("gbx_..."))/('gbx_...'); quoting excludes docstring
# fragments like `gbx_rst_*` that would otherwise match a bare token.
PY_NAME_RE = re.compile(r"""["'](gbx_[a-z0-9_]+)["']""")

# Map each gbx_ function prefix to its heavy (non-light-only) Python module.
# Light-only modules (pyrx, pyvx, pygx, and vectorx/jts/legacy) are excluded —
# those functions may have no heavy equivalent and are listed in PYTHON_REGISTERED.
PREFIX_TO_HEAVY_MODULE = {
    "gbx_rst_": PYTHON_ROOT / "databricks/labs/gbx/rasterx/functions.py",
    # Legacy Mosaic migration ships in its own heavy module (documented import path).
    "gbx_st_legacy": PYTHON_ROOT / "databricks/labs/gbx/vectorx/jts/legacy/functions.py",
    "gbx_st_": PYTHON_ROOT / "databricks/labs/gbx/vectorx/functions.py",
    "gbx_bng_": PYTHON_ROOT / "databricks/labs/gbx/gridx/bng/functions.py",
    "gbx_custom_": PYTHON_ROOT / "databricks/labs/gbx/gridx/custom/functions.py",
    "gbx_h3_": PYTHON_ROOT / "databricks/labs/gbx/gridx/h3/functions.py",
    # RasterX grid helper with an h3 name: Scala RST_H3_CellBBox, heavy wrapper in rasterx.
    "gbx_h3_cell_bbox": PYTHON_ROOT / "databricks/labs/gbx/rasterx/functions.py",
    "gbx_quadbin_": PYTHON_ROOT / "databricks/labs/gbx/gridx/quadbin/functions.py",
    "gbx_pmtiles_": PYTHON_ROOT / "databricks/labs/gbx/pmtiles/functions.py",
}


def canonical_sql() -> set[str]:
    names = set()
    for line in REGISTERED_TXT.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.add(line)
    return names


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


def check_heavy_module_coverage(canonical: set[str]) -> dict[str, list[str]]:
    """Check that each canonical function (except light-only) is wrapped in ITS package's heavy module.

    A name defined only in another module (e.g. the light ``pyrx``/``pyvx`` registration
    tables) does not count. The longest matching prefix picks the module, so
    ``gbx_st_legacy*`` resolves to the legacy module rather than ``vectorx``.

    Returns a dict mapping each heavy module path to a list of missing function names
    that should be defined there.
    """
    module_names = {
        path: set(PY_NAME_RE.findall(path.read_text())) if path.exists() else set()
        for path in PREFIX_TO_HEAVY_MODULE.values()
    }
    missing_per_module = {}

    for name in sorted(canonical):
        # Light-only functions don't need a heavy wrapper
        if name in PYTHON_REGISTERED:
            continue
        prefixes = [p for p in PREFIX_TO_HEAVY_MODULE if name.startswith(p)]
        if not prefixes:
            continue
        module_path = PREFIX_TO_HEAVY_MODULE[max(prefixes, key=len)]
        if name not in module_names[module_path]:
            missing_per_module.setdefault(module_path, []).append(name)

    return missing_per_module


def main() -> int:
    for required in (REGISTERED_TXT, FUNCTION_INFO_JSON):
        if not required.exists():
            print(f"❌ missing required file: {required}", file=sys.stderr)
            return 1

    sql = canonical_sql()
    bindings = {
        "Scala (override def name)": scala_names(),
        "Python (functions.py)": python_names(),
        "function-info.json": function_info_keys(),
    }

    print(f"Canonical registered functions (SQL): {len(sql)}")
    for label, found in bindings.items():
        print(f"  {label}: {len(found)}")
    print()

    failed = False

    # Drift guard: the bench wheel ships a copy of registered_functions.txt (so
    # spec.registered_rst works on a cluster with no repo tree). It MUST stay
    # byte-identical to the canonical file here.
    packaged = REPO_ROOT / "python/geobrix/src/databricks/labs/gbx/bench/registered_functions.txt"
    if not packaged.exists():
        failed = True
        print(f"❌ packaged copy missing: {packaged.relative_to(REPO_ROOT)} "
              "(spec.registered_rst's cluster fallback needs it)")
    elif packaged.read_text() != REGISTERED_TXT.read_text():
        failed = True
        print(f"❌ packaged copy drifted from canonical: {packaged.relative_to(REPO_ROOT)} "
              f"!= {REGISTERED_TXT.relative_to(REPO_ROOT)} -- re-copy the canonical file.")

    # Check that each canonical function (except light-only) has a heavy module wrapper
    heavy_missing = check_heavy_module_coverage(sql)
    if heavy_missing:
        failed = True
        for module_path in sorted(heavy_missing.keys()):
            missing_names = heavy_missing[module_path]
            print(f"❌ {len(missing_names)} canonical function(s) missing from heavy module "
                  f"{module_path.relative_to(REPO_ROOT)}:")
            for name in sorted(missing_names):
                print(f"     - {name}")

    for label, found in bindings.items():
        # Python-registered functions (e.g. gbx_rst_fromfile) have no Scala expression by design;
        # exempt them from the Scala arm. Python binding + function-info are still required.
        required = sql - PYTHON_REGISTERED if label == "Scala (override def name)" else sql
        missing = sorted(required - found)
        if missing:
            failed = True
            print(f"❌ {len(missing)} canonical function(s) missing from {label}:")
            for name in missing:
                print(f"     - {name}")

    # Extras are informational: a binding name not in the canonical list (e.g. an
    # expression whose rd.register(...) is commented out). Not a failure.
    for label, found in bindings.items():
        extra = sorted(found - sql)
        if extra:
            print(f"ℹ️  {len(extra)} name(s) in {label} not in the canonical list (ignored): "
                  f"{', '.join(extra)}")

    print()
    if failed:
        print("❌ binding parity FAILED — every registered function must exist in all bindings.")
        print("   Fix the missing binding(s), or remove the function from "
              "docs/tests-function-info/registered_functions.txt if it was withdrawn.")
        return 1
    print(f"✅ binding parity OK — all {len(sql)} registered functions exist in Scala, Python, "
          "and function-info.json.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
