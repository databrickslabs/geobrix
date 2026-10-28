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


TOWERS_ROWS = [c for c in oracles.MVP_H if c.fn == "rst_viewshed_towers"]
TOWERS_ROW = TOWERS_ROWS[0]


@pytest.mark.integration
@pytest.mark.parametrize("lv", ["L1-steps", "JVM-DF"])
@pytest.mark.parametrize("case", TOWERS_ROWS, ids=lambda c: c.cid)
def test_mvp_h_towers(jvm_spark, tmp_path, lv, case):
    """The rows the host runs at L3 (test_mvp_h_oracle_session), at the two heavy levels: the same counts within
    0.5% of total_px, no row for a tower on no tile, a NULL mast read as 0.0, and an error, not a row, for a tile
    that cannot be read."""
    ((_, msg),) = oracles.run_helpers(jvm_spark, [case], tmpdir=str(tmp_path), level=lv)
    assert msg is None


@pytest.mark.integration
def test_the_heavy_towers_levels_agree(jvm_spark, tmp_path):
    """Heavy steps and the one Scala step run the same GDAL, so they give the same counts, tower by tower (no
    tolerance), and the window of a flat 200 x 200 tile at 80 m is the 161 x 161 GDAL writes with a disc of
    pi * 80^2 = 20106 visible pixels."""

    def run(lv):
        return oracles.run_towers(jvm_spark, TOWERS_ROW, str(tmp_path), lv)

    steps, one = run("L1-steps"), run("JVM-DF")
    assert steps == one
    assert steps["t1"] == (20081, 161 * 161)


def test_a_session_without_the_jar_runs_light_and_says_why(
    spark, tmp_path, monkeypatch, capsys
):
    """A heavy level asked of a session with no matching JAR gives the light answer, and one line gives the reason."""
    import weakref

    from databricks.labs.gbx.manager import capabilities, levels

    why = "no geobrix JAR on classpath (towers test)"
    no_jar = capabilities.Capabilities(False, False, None, why, why, False)
    monkeypatch.setattr(capabilities, "detect", lambda spark, need_jvm: no_jar)
    # as a session that has set nothing up (an earlier row may have set up a heavy level on this one)
    monkeypatch.setattr(levels, "_READY", weakref.WeakKeyDictionary())
    got = oracles.run_towers(spark, TOWERS_ROW, str(tmp_path), "JVM-DF")
    assert got == oracles.run_towers(spark, TOWERS_ROW, str(tmp_path), "L3")
    assert f"geobrix: heavy unavailable: {why}; using light" in capsys.readouterr().out


def test_the_levels_knob_refuses_what_it_cannot_run(spark, tmp_path):
    """A level that does not exist, and JVM-DF with a reprojection (its step has none), are errors, not fallbacks."""
    from databricks.labs.gbx.pyrx.functions import (
        rst_viewshed_towers,  # not at import: see the Connect oracle
    )

    with pytest.raises(ValueError, match="_levels must be one of L3, L1-steps, JVM-DF"):
        oracles.run_towers(spark, TOWERS_ROW, str(tmp_path), "L2")
    tiles = spark.createDataFrame(
        oracles.dsm_tile_rows(str(tmp_path)), "tx int, ty int, path string"
    )
    towers = spark.createDataFrame(
        [("a", 100.0, 100.0)], "tower_cellid string, x double, y double"
    )
    with pytest.raises(ValueError, match="does not support to_crs"):
        rst_viewshed_towers(
            towers, tiles, 80.0, 20.0, x0=0.0, y0=0.0, tile_m=200.0,
            to_crs=4326, _levels="JVM-DF",
        )  # fmt: skip


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


def test_a_group_over_the_materialize_cap_stops_the_fold(spark, tmp_path, monkeypatch):
    """A key's members (their encoded bytes, 2.2 KB here) are bounded by the materialize cap, before
    the fold copies them; each member alone (about 0.6 KB) fits."""
    from databricks.labs.gbx.pyrx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1500")  # the cap is read on the driver
    with pytest.raises(Exception, match="split the key"):
        helpers.merge_by_key(_frame(spark, tmp_path, "quad_tiles"), ["key"]).collect()


def test_a_member_that_decodes_over_the_tile_budget_stops_the_fold(
    spark, tmp_path, monkeypatch
):
    """A member's decoded size comes from its header, also when its bytes are already in the row."""
    from databricks.labs.gbx.pyrx import helpers

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.core.budget.decoded_budget_bytes",
        lambda strategy: 100,  # a 16x16 float32 member decodes to 1 KB
    )
    with pytest.raises(Exception, match="MiB decoded, over the"):
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
