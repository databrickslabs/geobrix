"""The MVP-S oracle: every session row of every manifest function, at every light variant."""

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles

CASES = [
    (fn.sql_name, variant)
    for fn in manifest.load().functions.values()
    if fn.oracle == "MVP_S"
    for variant in (fn.levels["L3"].get("variants") or {"-": None})
]


@pytest.mark.parametrize("fn,variant", CASES, ids=lambda v: v)
def test_mvp_s_oracle_session(spark, tmp_path, fn, variant):
    register(spark, only=[fn], _levels={fn: ("L3", variant)})
    rows = [c for c in oracles.MVP_S if c.fn == fn]
    failures = [
        msg for _, msg in oracles.run_session(spark, rows, tmpdir=str(tmp_path)) if msg
    ]
    assert failures == []
