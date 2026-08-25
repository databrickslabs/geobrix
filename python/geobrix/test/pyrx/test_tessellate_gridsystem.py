"""Tests: gridSystem key in tile.metadata for all three light tessellate UDTFs.

Mirrors the test_tessellate_bng.py::test_spark_struct_no_rasterx_cell_id_in_metadata
Spark-UDTF pattern. Each UDTF is driven through a real PySpark session (local[2], no JAR)
to exercise the actual yielded struct row including the metadata map.
"""

import numpy as np
import rasterio
from rasterio.io import MemoryFile


def _small_4326_bytes(size=32, origin=(-0.12, 51.52), res_deg=0.01):
    """Return GTiff bytes: small EPSG:4326 raster over London."""
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(
            origin[0], origin[1], res_deg, res_deg
        ),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def _small_27700_bytes(size=8, origin=(530000.0, 182000.0), res_m=1000.0):
    """Return GTiff bytes: small EPSG:27700 raster over London (BNG easting/northing)."""
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=rasterio.transform.from_origin(origin[0], origin[1], res_m, res_m),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# H3 tessellate: gridSystem="h3"
# ---------------------------------------------------------------------------


def test_h3_tessellate_tile_carries_gridsystem_h3(spark):
    """Each tile emitted by rst_h3_tessellate must have metadata['gridSystem']='h3'."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    tile_bytes = _small_4326_bytes()
    df = spark.createDataFrame([(bytearray(tile_bytes),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_gs_h3")
    rows = spark.sql(
        "SELECT t.metadata FROM _gs_h3, LATERAL gbx_rst_h3_tessellate(tile, 5, 'covering') t"
    ).collect()

    assert rows, "rst_h3_tessellate must yield >=1 tile for the test raster"
    for row in rows:
        metadata = dict(row["metadata"]) if row["metadata"] else {}
        assert metadata.get("gridSystem") == "h3", (
            f"h3 tessellate tile must have gridSystem='h3' in metadata; "
            f"got {metadata.get('gridSystem')!r}. Metadata: {metadata}"
        )


def test_h3_tessellate_centroid_tile_carries_gridsystem_h3(spark):
    """Centroid mode tiles must also carry gridSystem='h3'."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    tile_bytes = _small_4326_bytes()
    df = spark.createDataFrame([(bytearray(tile_bytes),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_gs_h3_centroid")
    rows = spark.sql(
        "SELECT t.metadata FROM _gs_h3_centroid, LATERAL gbx_rst_h3_tessellate(tile, 5, 'centroid') t"
    ).collect()

    assert rows, "rst_h3_tessellate centroid must yield >=1 tile"
    for row in rows:
        metadata = dict(row["metadata"]) if row["metadata"] else {}
        assert (
            metadata.get("gridSystem") == "h3"
        ), f"h3 tessellate centroid tile must have gridSystem='h3'; got {metadata.get('gridSystem')!r}"


# ---------------------------------------------------------------------------
# Quadbin tessellate: gridSystem="quadbin"
# ---------------------------------------------------------------------------


def test_quadbin_tessellate_tile_carries_gridsystem_quadbin(spark):
    """Each tile emitted by rst_quadbin_tessellate must have metadata['gridSystem']='quadbin'."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    tile_bytes = _small_4326_bytes()
    df = spark.createDataFrame([(bytearray(tile_bytes),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_gs_qb")
    rows = spark.sql(
        "SELECT t.metadata FROM _gs_qb, LATERAL gbx_rst_quadbin_tessellate(tile, 12, 'covering') t"
    ).collect()

    assert rows, "rst_quadbin_tessellate must yield >=1 tile"
    for row in rows:
        metadata = dict(row["metadata"]) if row["metadata"] else {}
        assert (
            metadata.get("gridSystem") == "quadbin"
        ), f"quadbin tessellate tile must have gridSystem='quadbin'; got {metadata.get('gridSystem')!r}"


def test_quadbin_tessellate_centroid_tile_carries_gridsystem_quadbin(spark):
    """Centroid mode tiles must also carry gridSystem='quadbin'."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    tile_bytes = _small_4326_bytes()
    df = spark.createDataFrame([(bytearray(tile_bytes),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_gs_qb_centroid")
    rows = spark.sql(
        "SELECT t.metadata FROM _gs_qb_centroid, LATERAL gbx_rst_quadbin_tessellate(tile, 12, 'centroid') t"
    ).collect()

    assert rows, "rst_quadbin_tessellate centroid must yield >=1 tile"
    for row in rows:
        metadata = dict(row["metadata"]) if row["metadata"] else {}
        assert (
            metadata.get("gridSystem") == "quadbin"
        ), f"quadbin tessellate centroid tile must have gridSystem='quadbin'; got {metadata.get('gridSystem')!r}"


# ---------------------------------------------------------------------------
# BNG tessellate: gridSystem="bng"
# ---------------------------------------------------------------------------


def test_bng_tessellate_tile_carries_gridsystem_bng(spark):
    """Each tile emitted by rst_bng_tessellate must have metadata['gridSystem']='bng'."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    tile_bytes = _small_27700_bytes()
    df = spark.createDataFrame([(bytearray(tile_bytes),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_gs_bng")
    rows = spark.sql(
        "SELECT t.metadata FROM _gs_bng, LATERAL gbx_rst_bng_tessellate(tile, 3, 'covering') t"
    ).collect()

    assert rows, "rst_bng_tessellate must yield >=1 tile"
    for row in rows:
        metadata = dict(row["metadata"]) if row["metadata"] else {}
        assert (
            metadata.get("gridSystem") == "bng"
        ), f"bng tessellate tile must have gridSystem='bng'; got {metadata.get('gridSystem')!r}"


def test_bng_tessellate_centroid_tile_carries_gridsystem_bng(spark):
    """Centroid mode BNG tiles must also carry gridSystem='bng'."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    tile_bytes = _small_27700_bytes()
    df = spark.createDataFrame([(bytearray(tile_bytes),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_gs_bng_centroid")
    rows = spark.sql(
        "SELECT t.metadata FROM _gs_bng_centroid, LATERAL gbx_rst_bng_tessellate(tile, 3, 'centroid') t"
    ).collect()

    assert rows, "rst_bng_tessellate centroid must yield >=1 tile"
    for row in rows:
        metadata = dict(row["metadata"]) if row["metadata"] else {}
        assert (
            metadata.get("gridSystem") == "bng"
        ), f"bng tessellate centroid tile must have gridSystem='bng'; got {metadata.get('gridSystem')!r}"


# ---------------------------------------------------------------------------
# Regression guard: rasterize_agg path must NOT gain gridSystem
# ---------------------------------------------------------------------------


def test_build_tile_rasterize_agg_path_no_gridsystem():
    """_serde.build_tile called with no grid_system (rasterize_agg pattern: cellid=0)
    must NOT include gridSystem in the tile metadata. This guards that the
    _as_tile_udf caller at functions.py:6730 stays unchanged after the retrofit.
    """
    from databricks.labs.gbx.pyrx import _serde

    from .conftest import make_geotiff_bytes

    tile = _serde.build_tile(make_geotiff_bytes(), "GTiff", 0)
    metadata = tile["metadata"] or {}
    assert "gridSystem" not in metadata, (
        f"rasterize_agg path (grid_system omitted) must NOT include gridSystem; "
        f"found in metadata: {metadata}"
    )
