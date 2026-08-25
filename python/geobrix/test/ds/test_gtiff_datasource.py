"""gtiff_gbx named reader: same output as raster_gbx with driver preset."""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource


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


def test_name_is_gtiff_gbx():
    assert GTiffGbxDataSource.name() == "gtiff_gbx"


def test_driver_preset_injected():
    ds = GTiffGbxDataSource(options={"path": "/tmp/x"})
    reader = ds.reader(ds.schema())
    assert reader.driver == "GTiff"


def test_reads_geotiff_like_catch_all(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("gtiff_gbx").load(str(f))
    rows = df.collect()
    assert len(rows) == 1
    # Task 2: whole-file virtual tiles defer the rasterio.open, so 'driver' is
    # no longer eagerly emitted.  'format' is inferred from the file extension.
    assert rows[0]["tile"]["metadata"]["format"] == "gtiff"
    assert rows[0]["tile"]["cellid"] == -1
