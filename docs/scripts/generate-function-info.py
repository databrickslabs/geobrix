#!/usr/bin/env python3
"""
Generate function-info.json from doc SQL examples (single source for DESCRIBE FUNCTION EXTENDED).

- Feeder is the SQL API function ref in docs (docs/tests/python/api/*_functions_sql.py).
  Empty or missing examples are not allowed; fix upstream by adding the *_sql_example()
  in the docs before this script will succeed.
- Full overwrite from registered_functions.txt; output only includes functions that have
  non-empty examples from the doc modules. No placeholders.
- Organized by package (rasterx, gridx, vectorx), sorted by function name. Section keys
  _package_<name> separate packages (loaders skip _ keys).

Usage (from repo root):
  python docs/scripts/generate-function-info.py
  # Fails if any registered function has no doc SQL example (lists them; fix in docs first).
"""

import json
import os
import re
import sys
from typing import List, Optional

# Script lives in docs/scripts/; repo root is two levels up.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DOCS_ROOT = os.path.join(REPO_ROOT, "docs")
RESOURCE_DIR = os.path.join(
    REPO_ROOT, "src", "main", "resources", "com", "databricks", "labs", "gbx"
)
RESOURCE_FILE = os.path.join(RESOURCE_DIR, "function-info.json")

# Map (module, name_prefix) -> (spark_prefix,)
# e.g. (rasterx_functions_sql, "rst_") -> "gbx_rst_"
MODULES = [
    ("tests.python.api.rasterx_functions_sql", "rst_", "gbx_rst_"),
    ("tests.python.api.gridx_functions_sql", "bng_", "gbx_bng_"),
]
# VectorX: optional module (st_*_sql_example -> gbx_st_*)
VECTORX_MODULE = ("tests.python.api.vectorx_functions_sql", "st_", "gbx_st_")
REGISTERED_FUNCTIONS_TXT = os.path.join(
    REPO_ROOT, "docs", "tests-function-info", "registered_functions.txt"
)


def first_statement_containing(sql: str, func_name: str) -> str:
    """Extract first SQL statement that contains func_name (e.g. gbx_rst_width)."""
    sql = sql.strip()
    statements = re.split(r";\s*", sql)
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        if func_name not in stmt:
            continue
        # Only lines that are SQL (no leading comments); keep from first SELECT onward
        lines = []
        started = False
        for ln in stmt.splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("--"):
                continue
            if ln.upper().startswith("SELECT"):
                started = True
            if started:
                lines.append(ln)
        if not lines:
            continue
        one = " ".join(lines).strip()
        return one + (";" if not one.endswith(";") else "")
    return ""


def format_examples_block(sql_line: str) -> str:
    """Format one SQL line for Spark ExpressionInfo examples (DESCRIBE FUNCTION EXTENDED)."""
    if not sql_line:
        return ""
    # Spark typically shows: "    Examples:\n      > SELECT ..."
    return "\n    Examples:\n      > " + sql_line.replace("\n", "\n      ") + "\n"


def _collect_from_module(
    mod, local_prefix: str, spark_prefix: str, registered_for_package: Optional[List[str]] = None
) -> dict:
    """
    Collect examples from one doc module.

    When registered_for_package is provided: for each *_sql_example() we take the first
    SELECT statement that contains the package prefix, then assign that example to
    every registered function name that appears in that statement. So one doc example
    (e.g. rst_upperleft_sql_example showing gbx_rst_upperleftx and gbx_rst_upperlefty)
    fills entries for all matching registered names.
    When registered_for_package is None (legacy): one Python function maps to one
    derived spark name as before.
    """
    result = {}
    for attr in dir(mod):
        if not attr.endswith("_sql_example"):
            continue
        if not attr.startswith(local_prefix):
            continue
        fn = getattr(mod, attr)
        if not callable(fn):
            continue
        try:
            sql = fn()
        except Exception as e:
            print(f"  skip {attr}: {e}", file=sys.stderr)
            continue
        if not sql or not isinstance(sql, str):
            continue

        if registered_for_package:
            # First SELECT that contains the package prefix (e.g. gbx_rst_)
            stmt = first_statement_containing(sql, spark_prefix)
            if not stmt:
                continue
            # Assign this example to every registered function that appears in the statement
            for name in registered_for_package:
                if name in stmt and name not in result:
                    result[name] = {"examples": format_examples_block(stmt).strip()}
        else:
            middle = attr[: -len("_sql_example")]
            spark_name = spark_prefix + middle[len(local_prefix) :]
            stmt = first_statement_containing(sql, spark_name)
            if not stmt:
                continue
            result[spark_name] = {"examples": format_examples_block(stmt).strip()}
    return result


def discover_and_collect(registered: Optional[List[str]] = None) -> dict:
    """
    Import doc modules and collect examples keyed by Spark function name.

    If registered is provided, each doc example is applied to every registered
    function whose name appears in the example's SQL (so combined examples
    like upperleftx/upperlefty are picked up for both).
    """
    sys.path.insert(0, DOCS_ROOT)
    result = {}
    try:
        for module_path, local_prefix, spark_prefix in MODULES:
            mod = __import__(module_path, fromlist=[""])
            reg_for_pkg = (
                [n for n in registered if n.startswith(spark_prefix)]
                if registered
                else None
            )
            collected = _collect_from_module(mod, local_prefix, spark_prefix, reg_for_pkg)
            # First example wins for each name
            for k, v in collected.items():
                if k not in result:
                    result[k] = v
        # Optional VectorX module
        try:
            mod = __import__(VECTORX_MODULE[0], fromlist=[""])
            reg_for_pkg = (
                [n for n in registered if n.startswith(VECTORX_MODULE[2])]
                if registered
                else None
            )
            collected = _collect_from_module(
                mod, VECTORX_MODULE[1], VECTORX_MODULE[2], reg_for_pkg
            )
            for k, v in collected.items():
                if k not in result:
                    result[k] = v
        except ImportError:
            pass
        return result
    finally:
        if DOCS_ROOT in sys.path:
            sys.path.remove(DOCS_ROOT)


def load_registered_functions_txt() -> list:
    """Load registered function names from docs/tests-function-info/registered_functions.txt."""
    if not os.path.isfile(REGISTERED_FUNCTIONS_TXT):
        return []
    names = []
    with open(REGISTERED_FUNCTIONS_TXT) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                names.append(line)
    return sorted(set(names))


# Package prefixes for grouping (section keys in JSON are _package_<name>; loader skips them)
PACKAGE_PREFIXES = [
    ("rasterx", "gbx_rst_"),
    ("gridx", "gbx_bng_"),
    ("vectorx", "gbx_st_"),
]


def _package_for(name: str) -> str:
    """Return package label for a function name (rasterx, gridx, vectorx)."""
    for pkg, prefix in PACKAGE_PREFIXES:
        if name.startswith(prefix):
            return pkg
    return "other"


def build_functions_object(registered: list, doc_examples: dict) -> dict:
    """
    Build the "functions" object: only functions with non-empty examples from docs.
    Ordered by package, then sorted by function name. Section markers _package_<name>
    separate packages. Empty usage is not allowed; only doc-derived entries are included.
    """
    by_package = {}
    for name in registered:
        pkg = _package_for(name)
        by_package.setdefault(pkg, []).append(name)
    for pkg in by_package:
        by_package[pkg] = sorted(by_package[pkg])

    out = {}
    for pkg_label, _ in PACKAGE_PREFIXES:
        names = by_package.get(pkg_label, [])
        if not names:
            continue
        out[f"_package_{pkg_label}"] = f"--- {pkg_label} ---"
        for name in names:
            entry = doc_examples.get(name) or {}
            examples = (entry.get("examples") or "").strip()
            if examples:
                out[name] = {"examples": examples}
    other_names = by_package.get("other", [])
    if other_names:
        out["_package_other"] = "--- other ---"
        for name in sorted(other_names):
            entry = doc_examples.get(name) or {}
            examples = (entry.get("examples") or "").strip()
            if examples:
                out[name] = {"examples": examples}
    return out


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate function-info.json from doc SQL examples (no empty usage; fix missing upstream in docs)"
    )
    args = parser.parse_args()

    os.makedirs(RESOURCE_DIR, exist_ok=True)
    registered = load_registered_functions_txt()
    doc_examples = discover_and_collect(registered)

    if not registered:
        # No registered list: output only doc-derived (legacy)
        functions = build_functions_object(sorted(doc_examples.keys()), doc_examples)
        with open(RESOURCE_FILE, "w") as f:
            json.dump({"functions": functions}, f, indent=2)
        count = len([k for k in functions if not k.startswith("_")])
        print(f"Wrote {count} function entries to {RESOURCE_FILE}")
        return

    # Full overwrite from registered; only include entries with non-empty examples.
    functions = build_functions_object(registered, doc_examples)
    included = {k for k in functions if not k.startswith("_")}
    missing_or_empty = [n for n in registered if n not in included]
    if missing_or_empty:
        print("ERROR: Empty or missing usage is not allowed. Fix upstream: add SQL examples in docs.", file=sys.stderr)
        print("Functions missing a doc SQL example (add *_sql_example() in the API function ref):", file=sys.stderr)
        for name in sorted(missing_or_empty):
            pkg = _package_for(name)
            if pkg == "rasterx":
                path = "docs/tests/python/api/rasterx_functions_sql.py"
            elif pkg == "gridx":
                path = "docs/tests/python/api/gridx_functions_sql.py"
            elif pkg == "vectorx":
                path = "docs/tests/python/api/vectorx_functions_sql.py"
            else:
                path = "docs/tests/python/api/*_functions_sql.py"
            print(f"  {name}  -> {path}", file=sys.stderr)
        print(f"\nTotal: {len(missing_or_empty)} function(s) need a doc SQL example.", file=sys.stderr)
        sys.exit(1)

    with open(RESOURCE_FILE, "w") as f:
        json.dump({"functions": functions}, f, indent=2)
    count = len(included)
    print(f"Wrote {count} function entries to {RESOURCE_FILE}")


if __name__ == "__main__":
    main()
