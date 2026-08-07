"""Test that rst_xyzpyramid accepts a 7th rescale arg and that rescale alters output.

The registered UDTF _RstXyzPyramidUDTF.eval already accepts rescale as its 7th
positional parameter.  This test guards the complete SQL surface:

  - A 7-arg LATERAL call is accepted without error.
  - rescale='auto' and rescale='none' produce different PNG bytes on a uint16
    narrow-range source (contrast mapping differs).

Pattern: same fixture construction as test_sql_registration._rgb_tile_view, but
using a uint16 narrow-range raster so that auto vs none meaningfully diverge.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import functions as prx


pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (geobrix[light] or [test] required)",
)


def _uint16_tile_view(spark, name="t"):
    """One-row DF with a narrow-range uint16 tile; extent lon 10..12, lat 48..50."""
    transform = from_origin(10.0, 50.0, 0.03125, 0.03125)
    profile = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=transform,
    )
    ramp = np.linspace(8000, 12000, 64 * 64).astype("uint16").reshape(64, 64)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(ramp, 1)
        raw = mf.read()

    from pyspark.sql import functions as f

    df = spark.createDataFrame([(raw,)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView(name)


def test_xyzpyramid_accepts_rescale_and_alters_output(spark):
    """7-arg SQL call is accepted; auto and none produce different PNG bytes."""
    prx.register(spark)
    _uint16_tile_view(spark)

    auto_rows = spark.sql(
        "SELECT x.bytes FROM t, "
        "LATERAL gbx_rst_xyzpyramid(t.tile, 7, 8, 'PNG', 256, 'bilinear', 'auto') x"
    ).collect()

    none_rows = spark.sql(
        "SELECT x.bytes FROM t, "
        "LATERAL gbx_rst_xyzpyramid(t.tile, 7, 8, 'PNG', 256, 'bilinear', 'none') x"
    ).collect()

    # Both rescale modes must yield at least one tile row.
    assert auto_rows, "rescale='auto' yielded no rows"
    assert none_rows, "rescale='none' yielded no rows"

    # rescale changes 8-bit contrast encoding — byte payloads must differ on
    # a uint16 narrow-range source ([8000, 12000]).
    auto_bytes = [bytes(r.bytes) for r in auto_rows]
    none_bytes = [bytes(r.bytes) for r in none_rows]
    assert auto_bytes != none_bytes, (
        "auto and none produced identical bytes on a uint16 narrow-range source; "
        "rescale is not being applied to the rendered output"
    )
