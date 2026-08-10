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
from pathlib import Path
from typing import Dict, List, Optional, Set

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
    ("tests.python.api.rasterx_functions_sql", "h3_", "gbx_h3_"),
    ("tests.python.api.gridx_functions_sql", "bng_", "gbx_bng_"),
    ("tests.python.api.gridx_functions_sql", "quadbin_", "gbx_quadbin_"),
    ("tests.python.api.gridx_functions_sql", "custom_", "gbx_custom_"),
]
# VectorX: optional module (st_*_sql_example -> gbx_st_*)
VECTORX_MODULE = ("tests.python.api.vectorx_functions_sql", "st_", "gbx_st_")
# PMTiles: optional module (pmtiles_*_sql_example -> gbx_pmtiles_*)
PMTILES_MODULE = ("tests.python.api.pmtiles_functions_sql", "pmtiles_", "gbx_pmtiles_")
REGISTERED_FUNCTIONS_TXT = os.path.join(
    REPO_ROOT, "docs", "tests-function-info", "registered_functions.txt"
)


# Load parsed builder metadata (usage_args from Scala case classes)
def _load_parsed_builders() -> Dict[str, dict]:
    """Load parsed builder metadata from extend_function_metadata.py."""
    try:
        # Execute the extend_function_metadata script as a subprocess
        import subprocess
        import json
        script_path = os.path.join(os.path.dirname(__file__), "extend-function-metadata.py")
        result = subprocess.run(
            [sys.executable, script_path, REPO_ROOT],
            capture_output=True,
            text=True,
            timeout=30
        )
        # Fail LOUDLY, never silently. Returning {} on error looks like "no functions have
        # signatures" and would quietly wipe usage_args out of the JSON on the next run.
        if result.returncode != 0:
            raise SystemExit(
                "generate-function-info: signature parser FAILED — refusing to write "
                "metadata that would silently drop usage_args.\n"
                f"{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
            )
        # Find the first '{' and parse from there (skip diagnostic output)
        output = result.stdout
        json_start = output.find('{')
        if json_start < 0:
            raise SystemExit(
                "generate-function-info: signature parser produced no JSON.\n"
                f"{output[-2000:]}"
            )
        try:
            return json.loads(output[json_start:])
        except json.JSONDecodeError as e:
            raise SystemExit(f"generate-function-info: parser JSON is invalid: {e}")
    except SystemExit:
        raise
    except Exception as e:
        raise SystemExit(f"generate-function-info: could not run signature parser: {e}")


def _split_sql_statements(sql: str) -> List[str]:
    """Split SQL on ';' outside single-quoted string literals and -- line comments."""
    stmts: List[str] = []
    buf: List[str] = []
    in_str = False
    in_line_comment = False
    chars = list(sql)
    i = 0
    while i < len(chars):
        ch = chars[i]
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
        elif in_str:
            if ch == "'":
                in_str = False
            buf.append(ch)
            i += 1
        elif ch == "-" and i + 1 < len(chars) and chars[i + 1] == "-":
            in_line_comment = True
            buf.append(ch)
            i += 1
        elif ch == "'":
            in_str = True
            buf.append(ch)
            i += 1
        elif ch == ";":
            stmts.append("".join(buf))
            buf = []
            i += 1
        else:
            buf.append(ch)
            i += 1
    if "".join(buf).strip():
        stmts.append("".join(buf))
    return stmts


def first_statement_containing(sql: str, func_name: str) -> str:
    """Extract first SQL statement that contains func_name (e.g. gbx_rst_width)."""
    sql = sql.strip()
    statements = _split_sql_statements(sql)
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

    Pre-pass: determine which registered names have a *dedicated* example function
    (Python `<name>_sql_example` whose derived spark name equals the registered name).
    Substring fallback during the main pass NEVER overrides those — so e.g.
    `gbx_st_asmvt` and `gbx_st_asmvt_pyramid` each bind to their own example.
    """
    # Pre-pass: collect the set of exact spark targets each *_sql_example function aims at.
    dedicated_targets = set()
    for attr in dir(mod):
        if not attr.endswith("_sql_example") or not attr.startswith(local_prefix):
            continue
        if not callable(getattr(mod, attr)):
            continue
        middle = attr[: -len("_sql_example")]
        dedicated_targets.add(spark_prefix + middle[len(local_prefix):])

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
            # Determine this example function's "exact target" spark name (e.g.
            # st_asmvt_pyramid_sql_example -> gbx_st_asmvt_pyramid). Substring matches
            # against OTHER registered names are tolerated as a fallback (e.g.
            # gbx_bng_cellunion inherits the gbx_bng_cellunion_agg example because there
            # is no dedicated cellunion_sql_example), but a name that DOES have its own
            # dedicated example function never picks up another's example as substring.
            middle = attr[: -len("_sql_example")]
            exact_target = spark_prefix + middle[len(local_prefix):]
            for name in registered_for_package:
                if name not in stmt or name in result:
                    continue
                if name != exact_target and name in dedicated_targets:
                    # `name` has its own *_sql_example — skip this substring spillover.
                    continue
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
    # Examples in rasterx_functions_sql.py import `path_config` from docs/tests/python/
    sys.path.insert(0, os.path.join(DOCS_ROOT, "tests", "python"))
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
        # Optional PMTiles module
        try:
            mod = __import__(PMTILES_MODULE[0], fromlist=[""])
            reg_for_pkg = (
                [n for n in registered if n.startswith(PMTILES_MODULE[2])]
                if registered
                else None
            )
            collected = _collect_from_module(
                mod, PMTILES_MODULE[1], PMTILES_MODULE[2], reg_for_pkg
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
    ("rasterx_h3", "gbx_h3_"),
    ("gridx", "gbx_bng_"),
    ("gridx_custom", "gbx_custom_"),
    ("vectorx", "gbx_st_"),
    ("pmtiles", "gbx_pmtiles_"),
]


def _package_for(name: str) -> str:
    """Return package label for a function name (rasterx, gridx, vectorx)."""
    for pkg, prefix in PACKAGE_PREFIXES:
        if name.startswith(prefix):
            return pkg
    return "other"


# All MODULES entries (including optional ones) used for base derivation.
# Each tuple: (spark_prefix, local_prefix)
_ALL_MODULE_PREFIXES = [
    (spark_prefix, local_prefix)
    for (_mod, local_prefix, spark_prefix) in MODULES
] + [
    (VECTORX_MODULE[2], VECTORX_MODULE[1]),
    (PMTILES_MODULE[2], PMTILES_MODULE[1]),
]


def _base_for_spark_name(spark_name: str) -> str:
    """
    Derive the 'base' name for tier example symbol lookup.

    Mirrors the SQL convention: strip spark_prefix, prepend local_prefix.
    E.g. gbx_rst_avg (spark_prefix='gbx_rst_', local_prefix='rst_') -> 'rst_avg'.
    Falls back to stripping 'gbx_' if no prefix matches.
    """
    for spark_prefix, local_prefix in _ALL_MODULE_PREFIXES:
        if spark_name.startswith(spark_prefix):
            suffix = spark_name[len(spark_prefix):]
            return local_prefix + suffix
    # Fallback: strip leading 'gbx_'
    if spark_name.startswith("gbx_"):
        return spark_name[4:]
    return spark_name


# tier -> (doc-test source path relative to docs/, symbol template, binding label)
# Missing files are tolerated; their tier is simply absent from bindings for all functions.
_TIER_SCANS = [
    (
        "tests/python/api/rasterx_functions_python_light.py",
        "def {base}_python_light_example",
        "python-light",
    ),
    (
        "tests/python/api/rasterx_functions.py",
        "def {base}_python_heavy_example",
        "python-heavy",
    ),
    (
        "tests/scala/api/ScalaApiExamples.scala",
        "val {base}_scala_example",
        "scala",
    ),
]


def _scan_tier_bindings(docs_root: str, spark_names: List[str]) -> Dict[str, Set[str]]:
    """
    Return spark_name -> set of tier labels whose example symbol is present in source text.

    Detection is TEXT-SCAN only (no import/execution). Missing files are tolerated
    (that tier is simply absent from bindings for all functions).
    """
    found: Dict[str, Set[str]] = {name: set() for name in spark_names}
    docs_path = Path(docs_root)
    for rel, template, label in _TIER_SCANS:
        path = docs_path / rel
        text = path.read_text() if path.exists() else ""
        for spark_name in spark_names:
            base = _base_for_spark_name(spark_name)
            symbol = template.format(base=base)
            # Match symbol followed by '(' (Python def) or ':'/whitespace (Scala val).
            if re.search(re.escape(symbol) + r"\s*[(:]", text):
                found[spark_name].add(label)
    return found


def build_functions_object(
    registered: list,
    doc_examples: dict,
    parsed_builders: Optional[Dict] = None,
    docs_root: Optional[str] = None,
) -> dict:
    """
    Build the "functions" object: only functions with non-empty examples from docs.
    Ordered by package, then sorted by function name. Section markers _package_<name>
    separate packages. Optionally merge in parsed builder metadata (usage_args).
    Empty usage is not allowed; only doc-derived entries are included.

    Each function entry gains a 'bindings' list (subset of
    ["sql","python-light","python-heavy","scala"]) recording which tiers have an example.
    'sql' is present whenever the entry has a non-empty examples value.
    The other three are present when a text-scan of the tier's doc-test source file finds
    the corresponding example symbol (e.g. def rst_avg_python_light_example).
    """
    if parsed_builders is None:
        parsed_builders = {}

    # Pre-compute per-tier bindings via text scan (missing files -> empty set).
    tier_bindings: Dict[str, Set[str]] = _scan_tier_bindings(
        docs_root or DOCS_ROOT, list(registered)
    )

    # Fixed order for the 'bindings' list.
    _BINDING_ORDER = ["sql", "python-light", "python-heavy", "scala"]

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
                func_entry = {"examples": examples}
                # Add parsed builder metadata if available
                if name in parsed_builders:
                    parsed = parsed_builders[name]
                    if "usage_args" in parsed and parsed["usage_args"]:
                        func_entry["usage_args"] = parsed["usage_args"]
                # Build bindings list in fixed order
                present = tier_bindings.get(name, set())
                bindings = []
                if examples:
                    bindings.append("sql")
                for b in _BINDING_ORDER[1:]:
                    if b in present:
                        bindings.append(b)
                func_entry["bindings"] = bindings
                out[name] = func_entry
    other_names = by_package.get("other", [])
    if other_names:
        out["_package_other"] = "--- other ---"
        for name in sorted(other_names):
            entry = doc_examples.get(name) or {}
            examples = (entry.get("examples") or "").strip()
            if examples:
                func_entry = {"examples": examples}
                # Add parsed builder metadata if available
                if name in parsed_builders:
                    parsed = parsed_builders[name]
                    if "usage_args" in parsed and parsed["usage_args"]:
                        func_entry["usage_args"] = parsed["usage_args"]
                # Build bindings list in fixed order
                present = tier_bindings.get(name, set())
                bindings = []
                if examples:
                    bindings.append("sql")
                for b in _BINDING_ORDER[1:]:
                    if b in present:
                        bindings.append(b)
                func_entry["bindings"] = bindings
                out[name] = func_entry
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
    parsed_builders = _load_parsed_builders()

    if not registered:
        # No registered list: output only doc-derived (legacy)
        functions = build_functions_object(
            sorted(doc_examples.keys()), doc_examples, parsed_builders
        )
        with open(RESOURCE_FILE, "w") as f:
            json.dump({"functions": functions}, f, indent=2)
        count = len([k for k in functions if not k.startswith("_")])
        print(f"Wrote {count} function entries to {RESOURCE_FILE}")
        return

    # Full overwrite from registered; only include entries with non-empty examples.
    functions = build_functions_object(registered, doc_examples, parsed_builders)
    included = {k for k in functions if not k.startswith("_")}
    missing_or_empty = [n for n in registered if n not in included]
    if missing_or_empty:
        print("ERROR: Empty or missing usage is not allowed. Fix upstream: add SQL examples in docs.", file=sys.stderr)
        print("Functions missing a doc SQL example (add *_sql_example() in the API function ref):", file=sys.stderr)
        for name in sorted(missing_or_empty):
            pkg = _package_for(name)
            if pkg in ("rasterx", "rasterx_h3"):
                path = "docs/tests/python/api/rasterx_functions_sql.py"
            elif pkg in ("gridx", "gridx_custom"):
                path = "docs/tests/python/api/gridx_functions_sql.py"
            elif pkg == "vectorx":
                path = "docs/tests/python/api/vectorx_functions_sql.py"
            elif pkg == "pmtiles":
                path = "docs/tests/python/api/pmtiles_functions_sql.py"
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
