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
