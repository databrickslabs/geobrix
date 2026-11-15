"""
Guard: no documented SQL puts a routing SQL function directly under explode or inline.

Spark refuses a SQL function inside a generator (UNSUPPORTED_SQL_UDF_USAGE), so the array is computed in a
subquery and the column is exploded. The names allowed below are the ones that resolve there; an existing
doc test executes each of them.
"""

import re
from pathlib import Path

DOCS = Path(__file__).resolve().parents[3]  # docs/
CALL = re.compile(r"\b(?:explode|posexplode|inline)(?:_outer)?\(\s*(gbx_\w+)", re.I)

# Run by test_bng_chip_array_sql_examples and test_rastertogrid_partial_sql_examples.
ALLOWED = {
    "gbx_bng_tessellate",
    "gbx_rst_h3_rastertogrid_partial",
    "gbx_rst_quadbin_rastertogrid_partial",
    "gbx_rst_bng_rastertogrid_partial",
    "gbx_rst_custom_rastertogrid_partial",
}


def test_no_documented_sql_calls_a_routing_function_under_a_generator():
    files = [*(DOCS / "docs").rglob("*.mdx"), *(DOCS / "tests" / "python" / "api").glob("*.py")]
    assert files
    bad = []
    for path in files:
        if path.name == Path(__file__).name:
            continue
        for m in CALL.finditer(path.read_text(errors="ignore")):
            if m.group(1) not in ALLOWED:
                bad.append(f"{path.relative_to(DOCS)}: {m.group(0)}")
    assert not bad, "compute the array in a subquery, then explode the column:\n" + "\n".join(bad)
