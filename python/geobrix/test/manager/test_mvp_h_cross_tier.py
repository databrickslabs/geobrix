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


TOWERS_ROW = next(c for c in oracles.MVP_H if c.fn == "rst_viewshed_towers")


# The heavy levels run the JAR's GDAL JNI (3.11) and L3 runs the light libgdal (3.12): 3.11 clips a viewshed to its
# max-distance window and keeps its disc, 3.12 returns the whole DEM with most of it visible whatever the distance
# (a flat 200 x 200 DEM, 80 m: 20081 of 25921 against 30967 of 40000). The counts agree again when the JNI is
# swapped for the light libgdal; the strict xfail then fails and these marks come off.
GDAL_VERSIONS = pytest.mark.xfail(
    strict=True, reason="GDAL 3.11 JNI vs 3.12 libgdal: max_distance clips differently"
)


@pytest.mark.parametrize(
    "lv",
    [
        "L3",
        pytest.param("L1-steps", marks=[pytest.mark.integration, GDAL_VERSIONS]),
        pytest.param("JVM-DF", marks=[pytest.mark.integration, GDAL_VERSIONS]),
    ],
)
def test_mvp_h_towers(request, tmp_path, lv):
    """Every level gives the L3 counts within 0.5% of total_px, and the same towers: a tower in no tile's reach
    has no row, one on the corner of four tiles sees across them, one on a tile of NoData still has a row.
    """
    spark = request.getfixturevalue("spark" if lv == "L3" else "jvm_spark")
    ref = oracles.run_towers(spark, TOWERS_ROW, str(tmp_path), "L3")
    got = oracles.run_towers(spark, TOWERS_ROW, str(tmp_path), lv)
    want = {k: ref.get(k) for k in TOWERS_ROW.expect}
    assert oracles.towers_differ(want, got) is None


@pytest.mark.integration
def test_the_heavy_towers_levels_agree(jvm_spark, tmp_path):
    """Heavy steps and the one Scala step run the same GDAL, so they give the same counts, tower by tower, and
    the same towers as L3 (a row for the NoData tile and for the four-tile corner, none for the tower out of reach).
    """

    def run(lv):
        return oracles.run_towers(jvm_spark, TOWERS_ROW, str(tmp_path), lv)

    steps, one = run("L1-steps"), run("JVM-DF")
    assert steps == one
    assert (
        set(steps) == set(run("L3")) == {k for k, v in TOWERS_ROW.expect.items() if v}
    )


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
