"""The MVP-H oracle: the DataFrame helpers (retile, tessellate, merge, combine) on local Spark."""

import pytest

from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.manifest import TILE_SQL


def _red(spark, tmp_path):
    ran = oracles.run_helpers(spark, oracles.MVP_H, tmpdir=str(tmp_path))
    assert len(ran) == len(oracles.MVP_H)
    return [m for _, m in ran if m]


def test_mvp_h_oracle_session(spark, tmp_path):
    assert _red(spark, tmp_path) == []


def test_mvp_h_oracle_connect(spark_connect_local, tmp_path):
    """The same rows through a Spark Connect client: the helpers touch no JVM-only API."""
    assert _red(spark_connect_local, tmp_path) == []


def test_a_virtual_member_over_the_cap_stops_the_fold(spark, tmp_path, monkeypatch):
    """Decoding a virtual member into a row goes through the materialize cap, and the error says what to do."""
    from databricks.labs.gbx.pyrx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")  # the cap is read on the driver
    tile = oracles.resolve_arg(oracles.Build("tile_virtual"), str(tmp_path))
    df = spark.createDataFrame([("a", tile)], f"key string, tile {TILE_SQL}")
    with pytest.raises(Exception, match="method='vrt'"):
        helpers.merge_by_key(df, ["key"], method="fold").collect()


def _frame(spark, tmp_path, name):
    tiles = oracles.resolve_arg(oracles.Build(name), str(tmp_path))
    return spark.createDataFrame(
        [("a", t) for t in tiles], f"key string, tile {TILE_SQL}"
    )


def test_a_group_over_the_tile_budget_stops_the_fold(spark, tmp_path, monkeypatch):
    """The members a key holds are bounded by the canonical tile budget, before the fold copies them."""
    from databricks.labs.gbx.pyrx import helpers

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.core.budget.decoded_budget_bytes",
        lambda strategy: 100,
    )
    with pytest.raises(Exception, match="split the key"):
        helpers.merge_by_key(_frame(spark, tmp_path, "quad_tiles"), ["key"]).collect()


def test_a_mosaic_over_the_cap_is_refused_before_it_is_read(
    spark, tmp_path, monkeypatch
):
    """The vrt merge checks the size of its COG on disk; reading it whole first would be the OOM."""
    from databricks.labs.gbx.pyrx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")
    with pytest.raises(Exception, match="merged mosaic is"):
        helpers.merge_by_key(
            _frame(spark, tmp_path, "quad_paths"), ["key"], method="vrt"
        ).collect()
