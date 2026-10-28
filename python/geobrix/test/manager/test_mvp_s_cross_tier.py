"""The MVP-S oracle: every session row of every manifest function, at every light variant,
through SQL and through the Python Column API."""

import importlib

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles

CASES = [
    (fn.sql_name, variant)
    for fn in manifest.load().functions.values()
    if fn.oracle == "MVP_S"
    for variant in (fn.levels["L3"].get("variants") or {"-": None})
]
_API_PACKAGE = {"bng": "pygx", "rst": "pyrx", "st": "pyvx"}


def _column_function(sql_name):
    package = _API_PACKAGE[sql_name.split("_")[1]]
    module = importlib.import_module(f"databricks.labs.gbx.{package}.functions")
    return getattr(module, sql_name[len("gbx_") :])


@pytest.mark.parametrize("api", ["sql", "column"])
@pytest.mark.parametrize("fn,variant", CASES, ids=lambda v: v)
def test_mvp_s_oracle_session(spark, tmp_path, fn, variant, api):
    register(spark, only=[fn], _levels={fn: ("L3", variant)})
    rows = [c for c in oracles.MVP_S if c.fn == fn]
    results = oracles.run_session(
        spark,
        rows,
        tmpdir=str(tmp_path),
        column_of=_column_function if api == "column" else None,
    )
    assert [msg for _, msg in results if msg] == []
