"""Drivers that GeoBrix reaches in its own way: a VRT a user writes (read under the driver policy), and MEM, which
the GDAL engine uses for the in-memory feature output of ``gbx_rst_contour``.
"""

import os
from test.drivers_extended._asserts import assert_tile_is, tile_dict
from test.drivers_extended._raster import RasterCase, reference, write_sample

import numpy as np
import pytest

pytestmark = pytest.mark.filterwarnings("ignore")

_BASE = RasterCase("GTiff", ".tif", "int16", 2, name="base.tif")


@pytest.fixture(scope="module")
def vrt(gb, tmp_path_factory):
    """A GeoTIFF and a VRT beside it that names it by a relative path, as a user writes one."""
    d = tmp_path_factory.mktemp("vrt")
    tif = write_sample(gb, _BASE, str(d))
    path = os.path.join(str(d), "mosaic.vrt")
    ds = gb.gdal.BuildVRT(path, [tif])
    ds.FlushCache()
    ds = None
    return path


def test_a_user_written_vrt_reads_with_raster_gbx(gb, spark, vrt):
    rows = spark.read.format("raster_gbx").load(vrt).collect()
    assert len(rows) == 1
    assert_tile_is(tile_dict(rows[0]["tile"]), reference(gb, vrt), "raster_gbx VRT")


def test_a_user_written_vrt_reads_with_rst_fromfile(gb, spark, vrt):
    from databricks.labs.gbx.rasterx import functions as rx

    row = (
        spark.createDataFrame([(vrt,)], ["path"])
        .select(rx.rst_fromfile("path", "VRT").alias("tile"))
        .collect()[0]
    )
    assert_tile_is(tile_dict(row["tile"]), reference(gb, vrt), "rst_fromfile VRT")


def test_the_sql_rst_fromfile_renders_a_vrt_into_a_tile_of_its_own(gb, spark, vrt):
    """The SQL function reads the pixels now: the VRT's XML is no tile, so the tile holds the rendered pixels."""
    spark.createDataFrame([(vrt,)], ["path"]).createOrReplaceTempView("vrts")
    tile = tile_dict(
        spark.sql("SELECT gbx_rst_fromfile(path, 'VRT') AS tile FROM vrts").collect()[
            0
        ]["tile"]
    )
    assert tile["raster"] is not None
    assert_tile_is(tile, reference(gb, vrt), "gbx_rst_fromfile VRT")


def test_the_bytes_of_a_vrt_are_refused_with_a_reason(spark, vrt):
    """A VRT file's bytes name other files, so the policy refuses them as a tile (VRT has tile_bytes false)."""
    with open(vrt, "rb") as fh:
        content = fh.read()
    row = (
        spark.createDataFrame([(bytearray(content),)], ["content"])
        .selectExpr("gbx_rst_fromcontent(content, 'VRT') AS tile")
        .collect()[0]
    )
    tile = tile_dict(row["tile"])
    assert tile["raster"] is None and tile["metadata"].get("last_error")


def test_mem_is_the_in_memory_output_of_the_engines_feature_functions(
    gb, spark, tmp_path
):
    """``gbx_rst_contour`` runs GDAL's contour algorithm and takes its lines from a MEM dataset (the engine's feature
    output format). A surface that rises by one per pixel to the east has its line of value v half way between the
    centres of the columns v - 0.5 and v + 0.5."""
    from databricks.labs.gbx.engine import spec

    assert spec.OUTPUT_FORMAT_OF["features"] == "MEM"
    z = np.tile(np.arange(10, dtype="float32"), (10, 1))
    gdal = gb.gdal
    mem = gdal.GetDriverByName("MEM").Create("", 10, 10, 1, gdal.GDT_Float32)
    mem.SetGeoTransform((400000.0, 10.0, 0.0, 6000000.0, 0.0, -10.0))
    srs = gb.osr.SpatialReference()
    srs.ImportFromEPSG(3857)
    mem.SetProjection(srs.ExportToWkt())
    mem.GetRasterBand(1).WriteArray(z)
    path = str(tmp_path / "ramp.tif")
    gdal.GetDriverByName("GTiff").CreateCopy(path, mem)
    mem = None

    from shapely import from_wkb

    from databricks.labs.gbx.rasterx import functions as rx

    row = (
        spark.createDataFrame([(path,)], ["path"])
        .select(rx.rst_contour(rx.rst_fromfile("path"), [1.5, 3.5]).alias("lines"))
        .collect()[0]
    )
    lines = {round(f["value"], 1): from_wkb(bytes(f["geom_wkb"])) for f in row["lines"]}
    assert set(lines) == {1.5, 3.5}
    assert {round(g.bounds[0], 3) for g in lines.values()} == {400020.0, 400040.0}
    assert all(g.geom_type == "LineString" for g in lines.values())
