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
    "gbx_st_shiftlongitude",
    "gbx_st_split",
    "gbx_st_wrapx",
}

# `override def name: String = "gbx_..."` — the canonical SQL name a companion registers under.
SCALA_NAME_RE = re.compile(r'override\s+def\s+name\s*:\s*String\s*=\s*"(gbx_[a-z0-9_]+)"')
# A quoted gbx_ literal (call_function("gbx_..."))/('gbx_...'); quoting excludes docstring
# fragments like `gbx_rst_*` that would otherwise match a bare token.
PY_NAME_RE = re.compile(r"""["'](gbx_[a-z0-9_]+)["']""")


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
