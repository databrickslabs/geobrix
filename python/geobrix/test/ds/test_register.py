"""register(spark) makes all light raster formats resolvable."""

import numpy as np
import rasterio
from rasterio.transform import from_origin


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_register_makes_both_formats_loadable(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    f = tmp_path / "s.tif"
    _write_sample(str(f))
    gbx_manager.register_sources(spark)
    assert spark.read.format("raster_gbx").load(str(f)).count() == 1
    assert spark.read.format("gtiff_gbx").load(str(f)).count() == 1
