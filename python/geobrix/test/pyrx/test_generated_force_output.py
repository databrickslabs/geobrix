"""The generated Python API's force-output path (``rasterx.functions`` -> ``_api.force_output``): every SQL
argument arrives with its default, the output lands where asked, and placement never changes the pixels.
"""

from test.pyrx.test_virtual_aware_family import _slope_array_from_row, _v2_df

import numpy as np
from pyspark.sql import functions as f

from databricks.labs.gbx.pyrx import _serde


def test_slope_virtualize_dir_writes_under_the_dir_and_matches_unforced(
    spark, tmp_path
):
    from databricks.labs.gbx.rasterx import functions as rx

    df = _v2_df(spark, tmp_path)
    out_dir = tmp_path / "gen_vout"
    forced = df.select(rx.rst_slope("tile", virtualize_dir=str(out_dir)).alias("t"))
    row = forced.first()["t"].asDict()
    assert row["raster"] is None and row["path"].startswith(str(out_dir))
    assert list(out_dir.glob("*.tif"))
    # the unforced call sends all four arguments, so it runs whichever level the session lands on
    nul = f.lit(None).cast("double")
    plain = df.select(rx.rst_slope("tile", "degrees", nul, nul).alias("t")).first()["t"]
    # an output-placement flag must not change the computed pixels
    assert np.array_equal(
        _slope_array_from_row(row), _slope_array_from_row(plain.asDict())
    )


def test_mapalgebra_materialize_returns_raster_bytes(spark, tmp_path):
    from databricks.labs.gbx.rasterx import functions as rx

    df = _v2_df(spark, tmp_path)
    row = (
        df.select(
            rx.rst_mapalgebra(f.array("tile"), "A*2", materialize=True).alias("t")
        )
        .first()["t"]
        .asDict()
    )
    assert row["raster"] is not None and row["path"] is None
    with _serde.open_tile(bytes(row["raster"])) as ds:
        assert np.array_equal(
            ds.read(1), np.arange(64, dtype="float32").reshape(8, 8) * 2
        )


def test_dem_and_calc_force_output_keywords(spark, tmp_path):
    """The DEM and band-math entries take the force-output keywords: `virtualize_dir` writes the tile under the
    directory, `materialize=True` returns the raster bytes."""
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.rasterx import functions as rx

    dem = oracles._build("dem_3857", str(tmp_path))
    bands = oracles._build("calc_bands", str(tmp_path))
    df = spark.createDataFrame([(dem, bands)], f"dem {TILE_SQL}, bands {TILE_SQL}")
    out_dir = tmp_path / "dem_vout"
    row = df.select(
        rx.rst_aspect("dem", virtualize_dir=str(out_dir)).alias("a"),
        rx.rst_ndvi("bands", 1, 2, materialize=True).alias("n"),
    ).first()
    assert row["a"]["raster"] is None and row["a"]["path"].startswith(str(out_dir))
    assert list(out_dir.glob("*.tif"))
    assert row["n"]["raster"] is not None and row["n"]["path"] is None


def test_clip_virtualize_dir_with_optional_crs_before_the_literals(spark, tmp_path):
    """`rst_clip` has an optional `clip_crs` before the force-output literals: left out, the SQL default fills it,
    and the clip lands under the directory with the pixels of the unforced call."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = _v2_df(spark, tmp_path)
    # the left half of the 8 x 8 tile (columns 0 to 3), in the tile's own CRS
    cutline = f.lit("POLYGON((-1 -1, 4 -1, 4 9, -1 9, -1 -1))")
    out_dir = tmp_path / "clip_vout"
    forced = df.select(
        rx.rst_clip("tile", cutline, False, virtualize_dir=str(out_dir)).alias("t")
    ).first()["t"]
    assert forced["raster"] is None and forced["path"].startswith(str(out_dir))
    assert list(out_dir.glob("*.tif"))
    plain = df.select(rx.rst_clip("tile", cutline, False).alias("t")).first()["t"]
    got = _slope_array_from_row(forced.asDict())
    assert got.shape == (8, 4)
    assert np.array_equal(got, _slope_array_from_row(plain.asDict()))
