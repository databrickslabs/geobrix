"""Tests for GeoTIFF FILE read+write bench legs.

FUSE legs (fuse tier, local[2]):
  - run_gtiff_file_read with file_mode="fuse" → status="ok"
  - run_gtiff_file_write with file_mode="fuse" → status="ok"

External/managed legs on FUSE tier:
  - run_gtiff_file_read with file_mode="external" → status="na_by_design"
    (gbx_file_read raises ValueError when FILE is unavailable)
"""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin


def _write_n_geotiffs(tmp_path, n):
    """Write n tiny single-tile GeoTIFFs under tmp_path via rasterio."""
    for i in range(n):
        p = str(tmp_path / f"t{i}.tif")
        transform = from_origin(10.0 + i * 0.1, 50.0, 0.5, 0.5)
        profile = dict(
            driver="GTiff",
            width=4,
            height=3,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=transform,
        )
        data = np.arange(12, dtype="float32").reshape(3, 4)
        with rasterio.open(p, "w", **profile) as ds:
            ds.write(data, 1)


def _one_tile_df(spark):
    """Return a one-row (source, tile) DataFrame with a materialized GeoTIFF tile.

    Builds GeoTIFF bytes in-memory via rasterio, then wraps them in a V2-schema
    tile row.  ``tile.path`` is None and ``tile.raster`` carries the bytes —
    a materialized tile ({tile:{path:None, raster:<bytes>}}).
    """
    from databricks.labs.gbx.ds.raster import reader_schema_v2

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    )
    data = np.arange(12, dtype="float32").reshape(3, 4)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        gtiff_bytes = bytearray(mf.read())

    schema = reader_schema_v2()
    row = (
        "test_src",
        {
            "cellid": 0,
            "raster": gtiff_bytes,
            "path": None,
            "path_mode": None,
            "window": None,
            "clip_polygon": None,
            "clip_crs": None,
            "crs": None,
            "metadata": {},
        },
    )
    return spark.createDataFrame([row], schema=schema)


def test_gtiff_file_read_fuse_ok(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gtiff_file_read

    # Stage 3 tiny single-tile GeoTIFFs under tmp_path
    _write_n_geotiffs(tmp_path, 3)
    r = run_gtiff_file_read(
        spark, str(tmp_path), "t", 0, 1, file_mode="fuse", where="venv"
    )
    assert r.status == "ok"
    assert r.file_mode == "fuse"
    assert r.rows == 3
    assert r.slots_available >= 1


def test_gtiff_file_read_external_skips_cleanly_on_fuse_tier(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gtiff_file_read

    _write_n_geotiffs(tmp_path, 2)
    r = run_gtiff_file_read(
        spark, str(tmp_path), "t", 0, 1, file_mode="external", where="venv"
    )
    assert r.status == "na_by_design"  # FILE not available on local fuse tier
    assert "FILE" in r.note


def test_gtiff_file_write_fuse_ok(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gtiff_file_write

    tile_df = _one_tile_df(spark)  # {tile:{path:None, raster:<bytes>}}
    r = run_gtiff_file_write(
        spark,
        tile_df,
        str(tmp_path / "out"),
        "t",
        0,
        1,
        file_mode="fuse",
        where="venv",
    )
    assert r.status == "ok"
    assert r.layout in ("order", "na")
