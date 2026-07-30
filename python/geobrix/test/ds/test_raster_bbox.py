import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.core.cog import GBX_FORMAT, sniff_header


def _write_sample(path, width=4, height=3, epsg=4326):
    # extent: origin (10.0, 50.0), 0.5 px -> x[10,12], y[48.5,50]
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)


def _tile_bounds(row):
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        b = out.bounds
        return (b.left, b.bottom, b.right, b.top), (out.width, out.height)


def test_bbox_windows_to_aoi(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("bbox", "10.5,49.0,11.5,50.0")
        .load(str(f))
    )
    rows = df.collect()
    assert len(rows) == 1
    bounds, (w, h) = _tile_bounds(rows[0])
    assert bounds == (10.5, 49.0, 11.5, 50.0)
    assert (w, h) == (2, 2)  # 1.0 deg / 0.5 px


def test_bbox_north_overhang_clips(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("bbox", "10.5,49.0,11.5,51.0")
        .load(str(f))
    )
    bounds, _ = _tile_bounds(df.collect()[0])
    assert bounds[3] == 50.0  # top clipped to dataset top, not 51.0


def test_non_overlapping_file_is_skipped(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("bbox", "20,20,21,21").load(str(f))
    assert df.collect() == []


def test_gtiff_gbx_parity(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    opt = ("bbox", "10.5,49.0,11.5,50.0")
    r1 = spark.read.format("raster_gbx").option(*opt).load(str(f)).collect()[0]
    r2 = spark.read.format("gtiff_gbx").option(*opt).load(str(f)).collect()[0]
    assert _tile_bounds(r1) == _tile_bounds(r2)


def test_malformed_bbox_raises(spark, tmp_path):
    f = tmp_path / "s.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    import pytest

    with pytest.raises(Exception):
        spark.read.format("raster_gbx").option("bbox", "1,2,3").load(str(f)).collect()


def _write_large_sample(path, width=512, height=512, epsg=4326):
    """Write a 512x512 raster so an AOI window of 256x256 exceeds a 64px COG blocksize.

    extent: origin (0.0, 1.0), 1/512 px/deg -> x[0,1], y[0,1]
    """
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(0.0, 1.0, 1.0 / width, 1.0 / height),
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)


def test_bbox_read_emits_gtiff(spark, tmp_path):
    """bbox read always emits a plain GTiff tile (COG is a cog_gbx writer concern).

    The reader no longer accepts tileFormat/cogBlockSize options. AOI windowed
    reads emit plain GTiff regardless of any passed tileFormat option.
    """
    f = tmp_path / "large.tif"
    _write_large_sample(str(f), width=512, height=512)
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("bbox", "0.0,0.0,0.5,0.5")  # AOI = 256x256 px window
        .load(str(f))
    )
    rows = df.collect()
    assert len(rows) == 1, "expected exactly one tile for the AOI"
    tile = rows[0]["tile"]
    raster_bytes = bytes(tile["raster"])
    metadata = tile["metadata"]
    # Reader always emits plain GTiff — COG creation is a writer concern.
    assert metadata.get(GBX_FORMAT) == "gtiff", (
        f"metadata gbx_format was '{metadata.get(GBX_FORMAT)}', expected 'gtiff'; "
        "bbox read must emit plain GTiff (use cog_gbx writer for COG creation)"
    )
    info = sniff_header(raster_bytes)
    assert (
        not info.is_cog
    ), f"bytes sniff says is_cog={info.is_cog}; expected plain GTiff, not COG"
