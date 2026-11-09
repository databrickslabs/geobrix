"""A materialized read keeps what a file's sidecars hold: ``raster_gbx`` with ``virtualTiles=false``, the SQL
``gbx_rst_fromfile`` and ``rst_fromfile(materialize=True)`` return the CRS, NoData and pixels that the path read gives.

A sidecar is a file beside the main one: a GeoTIFF's ``.aux.xml`` (NoData or CRS GDAL's PAM keeps there), an ESRI
``.hdr`` and ``.prj``, an ENVI ``.hdr``, a SAGA ``.sgrd`` and ``.prj``. The tile's bytes are one file, so the read has to
bake the sidecars' metadata into those bytes.
"""

import os
from test.drivers_extended._asserts import assert_tile_is, tile_dict
from test.drivers_extended._raster import (
    BY_DRIVER,
    RasterCase,
    _srs,
    reference,
    source_arrays,
    write_sample,
)

import pytest

pytestmark = pytest.mark.filterwarnings("ignore")


def _pam_geotiff(gb, directory: str) -> str:
    """A GeoTIFF with no CRS and no NoData of its own, whose ``.aux.xml`` holds both."""
    gdal = gb.gdal
    case = RasterCase(
        "GTiff", ".tif", "int16", 1, nodata=None, epsg=None, name="pam.tif"
    )
    path = write_sample(gb, case, directory)
    arr = source_arrays(
        RasterCase("GTiff", ".tif", "int16", 1, nodata=-9999, epsg=None)
    )[0]
    ds = gdal.Open(path, gdal.GA_Update)
    ds.GetRasterBand(1).WriteArray(arr)
    ds = None
    ds = gdal.Open(path)  # read-only: PAM writes the sidecar
    ds.SetProjection(_srs(gb, 32633).ExportToWkt())
    ds.GetRasterBand(1).SetNoDataValue(-9999.0)
    ds.FlushCache()
    ds = None
    assert os.path.exists(path + ".aux.xml"), "GDAL wrote no .aux.xml"
    return path


def _sidecar_case(driver: str, idx: int = 0) -> RasterCase:
    return BY_DRIVER[driver][idx]


_SIDECAR = [("EHdr", 1), ("ENVI", 0), ("SAGA", 0)]


def _sample(gb, kind, tmp_path_factory):
    d = str(tmp_path_factory.mktemp(f"mat_{kind}"))
    if kind == "GTiff+aux":
        return _pam_geotiff(gb, d)
    driver, idx = kind, 0
    for k, i in _SIDECAR:
        if k == kind:
            idx = i
    return write_sample(gb, _sidecar_case(driver, idx), d)


KINDS = ["GTiff+aux", "EHdr", "ENVI", "SAGA"]


@pytest.fixture(scope="module")
def samples(gb, tmp_path_factory):
    out = {}
    for k in KINDS:
        p = _sample(gb, k, tmp_path_factory)
        out[k] = (p, reference(gb, p))
    return out


@pytest.mark.parametrize("kind", KINDS)
def test_the_path_reference_holds_the_sidecars(samples, kind):
    """The reference is the file read by path: it has a CRS and a NoData value from the sidecars."""
    _, ref = samples[kind]
    assert ref.crs, f"{kind}: the reference has no CRS"
    assert ref.nodata[0] is not None


@pytest.mark.parametrize("kind", KINDS)
def test_raster_gbx_with_virtual_tiles_off_keeps_the_sidecars(spark, samples, kind):
    path, ref = samples[kind]
    rows = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "false")
        .load(path)
        .collect()
    )
    assert len(rows) == 1
    assert rows[0]["tile"]["raster"] is not None, "a materialized read has bytes"
    assert_tile_is(
        tile_dict(rows[0]["tile"]), ref, f"raster_gbx virtualTiles=false {kind}"
    )


@pytest.mark.parametrize("kind", KINDS)
def test_the_sql_rst_fromfile_keeps_the_sidecars(spark, samples, kind):
    path, ref = samples[kind]
    driver = "GTiff" if kind == "GTiff+aux" else kind
    spark.createDataFrame([(path,)], ["path"]).createOrReplaceTempView("mat_files")
    row = spark.sql(
        f"SELECT gbx_rst_fromfile(path, '{driver}') AS tile FROM mat_files"
    ).collect()[0]
    assert row["tile"] is not None, f"gbx_rst_fromfile returned NULL for {kind}"
    assert row["tile"]["raster"] is not None
    assert_tile_is(tile_dict(row["tile"]), ref, f"gbx_rst_fromfile {kind}")


@pytest.mark.parametrize("kind", KINDS)
def test_rst_fromfile_materialized_keeps_the_sidecars(spark, samples, kind):
    from databricks.labs.gbx.rasterx import functions as rx

    path, ref = samples[kind]
    driver = "GTiff" if kind == "GTiff+aux" else kind
    row = (
        spark.createDataFrame([(path,)], ["path"])
        .select(rx.rst_fromfile("path", driver, materialize=True).alias("tile"))
        .collect()[0]
    )
    assert row["tile"] is not None, f"rst_fromfile returned NULL for {kind}"
    assert row["tile"]["raster"] is not None
    assert_tile_is(tile_dict(row["tile"]), ref, f"rst_fromfile materialize {kind}")
