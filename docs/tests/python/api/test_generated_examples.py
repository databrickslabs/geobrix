"""Every function's generated example runs and meets the oracle row it was generated from."""

import pytest
from generated_examples import EXAMPLES

from databricks.labs.gbx import register
from databricks.labs.gbx.bench import oracles


@pytest.mark.parametrize("sql_name", sorted(EXAMPLES))
def test_generated_example(spark, tmp_path, sql_name):
    table, cid, sql = EXAMPLES[sql_name]
    case = next(c for c in oracles.TABLES[table] if c.cid == cid)
    register(spark, only=[sql_name])
    rows = oracles.argument_rows(case, str(tmp_path))
    view = spark.createDataFrame(
        [tuple(r) for r in rows], oracles.argument_schema(case, rows)
    )
    view.createOrReplaceTempView("gbx_example")
    got = spark.sql(sql).collect()
    outs = [r["out"] for r in got] if " AS out" in sql else got
    assert oracles.verdict(case, outs, rows, str(tmp_path)) is None, sql
