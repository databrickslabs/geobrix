"""
Function-info JSON coverage: assert every registered GeoBrix function has an entry
in function-info.json (so DESCRIBE FUNCTION EXTENDED can show doc-derived examples).
"""

import json
from pathlib import Path

import pytest

# Path to function-info.json (from repo root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FUNCTION_INFO_JSON = (
    PROJECT_ROOT / "src" / "main" / "resources" / "com" / "databricks" / "labs" / "gbx" / "function-info.json"
)
REGISTERED_TXT = Path(__file__).resolve().parent / "registered_functions.txt"


def load_function_info():
    """Load function-info.json; return dict of func_name -> info."""
    if not FUNCTION_INFO_JSON.exists():
        return {}
    with open(FUNCTION_INFO_JSON) as f:
        data = json.load(f)
    return data.get("functions", {})


def load_registered_from_txt():
    """Load registered function names from registered_functions.txt."""
    names = []
    with open(REGISTERED_TXT) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                names.append(line)
    return sorted(set(names))


def test_function_info_json_exists():
    """function-info.json must exist (run gbx:docs:function-info or docs/scripts/generate-function-info.py)."""
    assert FUNCTION_INFO_JSON.exists(), (
        f"Missing {FUNCTION_INFO_JSON}. Run: python docs/scripts/generate-function-info.py"
    )


def test_full_coverage_against_registered_list():
    """
    Every function in registered_functions.txt must have an entry in function-info.json
    with non-empty examples. Empty usage is not allowed; fix upstream by adding the
    SQL example in docs/tests/python/api/*_functions_sql.py.
    """
    assert FUNCTION_INFO_JSON.exists(), f"Missing {FUNCTION_INFO_JSON}"
    assert REGISTERED_TXT.exists(), f"Missing {REGISTERED_TXT}"
    info = load_function_info()
    registered = load_registered_from_txt()
    missing = [n for n in registered if n not in info]
    assert not missing, (
        f"function-info.json is missing {len(missing)} registered function(s). "
        f"Fix upstream: add *_sql_example() in docs/tests/python/api (rasterx_functions_sql.py, gridx_functions_sql.py, or vectorx_functions_sql.py). Missing: {missing}"
    )
    empty = [n for n in registered if n in info and not (info.get(n) or {}).get("examples", "").strip()]
    assert not empty, (
        f"Empty usage is not allowed. {len(empty)} function(s) have empty examples. "
        f"Fix upstream: add/repair the SQL example in the API function ref docs. Empty: {empty}"
    )


def test_coverage_report(registered_gbx_functions):
    """
    Report coverage: how many registered functions (from Spark) have an entry in function-info.json,
    and how many of those have non-empty examples. Skip if Spark not available.
    """
    if not registered_gbx_functions:
        pytest.skip("No registered functions (Spark/GeoBrix not available)")
    info = load_function_info()
    if not info:
        pytest.skip("function-info.json missing or empty")
    # No aliases; empty usage is not allowed (fix upstream in docs)
    in_json = [f for f in registered_gbx_functions if f in info]
    with_examples = [f for f in in_json if (info.get(f) or {}).get("examples", "").strip()]
    missing = [f for f in registered_gbx_functions if f not in info]
    empty_examples = [f for f in in_json if not (info.get(f) or {}).get("examples", "").strip()]
    total = len(registered_gbx_functions)
    covered = len(in_json)
    with_ex_count = len(with_examples)
    print(f"\nFunction-info coverage: {covered}/{total} in JSON, {with_ex_count}/{total} with non-empty examples")
    if missing:
        print(f"  Missing from JSON (fix upstream: add *_sql_example in docs): {len(missing)} -> {missing[:5]}{'...' if len(missing) > 5 else ''}")
    if empty_examples:
        print(f"  Empty usage not allowed (fix upstream): {len(empty_examples)} -> {empty_examples[:5]}{'...' if len(empty_examples) > 5 else ''}")
    assert not missing, f"Coverage {covered}/{total}; missing: {missing}"
    assert not empty_examples, f"Empty usage not allowed; fix upstream in docs. Empty: {empty_examples}"
