"""The DataFrame helpers oracle: retile, tessellate, merge and combine on local Spark."""

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


def test_a_virtual_member_is_not_decoded_into_a_row(spark, tmp_path, monkeypatch):
    """A virtual member goes to the merge by reference, never decoded into a row: only the result is bounded by the cap
    for bytes in a row, and the error says what to do with a result that does not fit.
    """
    from databricks.labs.gbx.rasterx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1000")  # the cap is read on the driver
    tile = oracles.resolve_arg(oracles.Build("tile_virtual"), str(tmp_path))
    df = spark.createDataFrame([("a", tile)], f"key string, tile {TILE_SQL}")
    with pytest.raises(Exception, match="cap for bytes in a row"):
        helpers.merge_by_key(df, ["key"], method="fold").collect()


def _frame(spark, tmp_path, name):
    tiles = oracles.resolve_arg(oracles.Build(name), str(tmp_path))
    return spark.createDataFrame(
        [("a", t) for t in tiles], f"key string, tile {TILE_SQL}"
    )


def test_a_group_over_the_materialize_cap_is_merged_not_held(
    spark, tmp_path, monkeypatch
):
    """A key's members are not held: their encoded bytes (2.2 KB here) pass the cap for bytes in a row (1.5 KB), and the
    merge still runs, its result (0.8 KB) being what the cap bounds. The group had a cap of its own once.
    """
    from databricks.labs.gbx.rasterx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1500")  # the cap is read on the driver
    for frame in ("quad_tiles", "quad_paths"):
        rows = helpers.merge_by_key(_frame(spark, tmp_path, frame), ["key"]).collect()
        assert len(rows) == 1 and 0 < len(rows[0]["raster"]) < 1500


def test_a_result_over_the_cap_is_refused_with_the_mosaic_route(
    spark, tmp_path, monkeypatch
):
    """The merged result (0.8 KB here) is bounded by the cap for bytes in a row (0.5 KB), for tiles with bytes and for
    paths, and the Fix names the files route: `merge_by_key` is bounded by the same cap.
    """
    from databricks.labs.gbx.rasterx import helpers

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "500")
    for frame in ("quad_tiles", "quad_paths"):
        with pytest.raises(Exception, match=r"mint_vrt|cog_gbx writer") as err:
            helpers.merge_by_key(_frame(spark, tmp_path, frame), ["key"]).collect()
        assert "cap" in str(err.value)


def test_towers_to_crs_adds_the_counts_of_the_reprojected_viewshed(spark, tmp_path):
    """`to_crs` reprojects each tower's viewshed and counts it again: both counts are there and, on flat ground
    at the equator, within a few percent of the counts before the reprojection."""
    from databricks.labs.gbx.rasterx.helpers import rst_viewshed_towers

    manifest, towers, radius, height = (
        oracles.resolve_arg(oracles.Build("dsm_tiles_edge"), str(tmp_path)),
        (("t1", 100.0, 100.0),),
        80.0,
        20.0,
    )
    tiles = spark.createDataFrame(manifest, "tx int, ty int, path string")
    sites = spark.createDataFrame(
        list(towers), "tower_cellid string, x double, y double"
    )
    row = rst_viewshed_towers(
        sites,
        tiles,
        radius,
        height,
        x0=0.0,
        y0=0.0,
        tile_m=oracles.DSM_TILE_M,
        to_crs=4326,
    ).collect()[0]
    assert row["total_px_reproj"] > 0 and row["visible_px_reproj"] > 0
    assert row["visible_px_reproj"] <= row["total_px_reproj"]
    for after, before in (
        ("total_px_reproj", "total_px"),
        ("visible_px_reproj", "visible_px"),
    ):
        assert abs(row[after] - row[before]) <= 0.05 * row[before], (
            after,
            dict(row.asDict()),
        )
