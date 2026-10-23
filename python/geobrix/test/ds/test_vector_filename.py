"""Tests for _canonical_ext, _complete_ext, _resolve_single_file_output (pure-unit),
and end-to-end round-trip tests for the fileName/adaptive-naming behavior of the
light vector writers (gpkg_gbx, geojson_gbx, shapefile_gbx+zip, file_gdb_gbx).

Pure-unit tests (no Spark, no Docker needed) are at the top.
Round-trip tests require a local SparkSession and pyogrio.
"""

import os

import pytest
from shapely import Point, to_wkb

from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.ds.vector import (
    _canonical_ext,
    _complete_ext,
    _resolve_single_file_output,
)


def test_canonical_ext():
    assert _canonical_ext("GPKG", False) == ".gpkg"
    assert _canonical_ext("GeoJSON", False) == ".geojson"
    assert _canonical_ext("ESRI Shapefile", True) == ".shp.zip"
    assert _canonical_ext("OpenFileGDB", False) == ".gdb"
    assert _canonical_ext("OpenFileGDB", True) == ".gdb.zip"


def test_complete_ext_appends_when_missing():
    assert _complete_ext("roads", ".shp.zip") == "roads.shp.zip"
    assert (
        _complete_ext("roads.shp", ".shp.zip") == "roads.shp.zip"
    )  # partial -> complete
    assert (
        _complete_ext("roads.shp.zip", ".shp.zip") == "roads.shp.zip"
    )  # already complete
    assert _complete_ext("city", ".gpkg") == "city.gpkg"
    assert _complete_ext("city.gpkg", ".gpkg") == "city.gpkg"


def test_complete_ext_rejects_wrong_geo_ext():
    with pytest.raises(ValueError, match="expected .shp.zip"):
        _complete_ext("roads.gpkg", ".shp.zip")


# ---- Task A2: _resolve_single_file_output ----


def test_case1_filename_given(tmp_path):
    out = _resolve_single_file_output(str(tmp_path / "newdir"), "roads", ".shp.zip")
    assert out == str(tmp_path / "newdir" / "roads.shp.zip")
    assert os.path.isdir(tmp_path / "newdir")  # parent created


def test_case2_existing_dir_no_filename(tmp_path):
    d = tmp_path / "roads_dir"
    d.mkdir()
    out = _resolve_single_file_output(str(d), None, ".shp.zip")
    assert out == str(d / "roads_dir.shp.zip")  # named after the dir, under it


def test_case3_stem_path_no_filename(tmp_path):
    out = _resolve_single_file_output(str(tmp_path / "sub" / "roads"), None, ".gpkg")
    assert out == str(tmp_path / "sub" / "roads.gpkg")  # complete ext on the stem
    assert os.path.isdir(tmp_path / "sub")  # parent created


def test_filename_extension_completed(tmp_path):
    out = _resolve_single_file_output(str(tmp_path), "roads.shp", ".shp.zip")
    assert out == str(tmp_path / "roads.shp.zip")


# ---- Task A3: VectorGbxWriter path resolution ----


def test_writer_resolves_gpkg_stem(tmp_path):
    from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    sch = StructType(
        [StructField("geom", BinaryType()), StructField("geom_srid", IntegerType())]
    )
    w = VectorGbxWriter(str(tmp_path / "city"), sch, "GPKG", {}, overwrite=True)
    assert w.path == str(tmp_path / "city.gpkg")


# ---------------------------------------------------------------------------
# Task A4: End-to-end round-trip tests for fileName/adaptive-naming
# ---------------------------------------------------------------------------
# Three naming cases per single-file writer:
#   (a) stem path  -- .save("<dir>/<stem>")         -> <dir>/<stem>.<ext>
#   (b) existing dir -- .save("<existing dir>")     -> <dir>/<dirname>.<ext>
#   (c) fileName   -- .option("fileName","x").save("<dir>")  -> <dir>/x.<ext>
#
# Each case asserts:
#   1. The output file exists at the contract-resolved path.
#   2. Reading back that path gives the same row count.
# ---------------------------------------------------------------------------

# Minimal helper: 3-row Point DataFrame with the standard geom+srid schema.


def _make_df(spark):
    rows = [
        (str(i), bytearray(to_wkb(Point(float(i), 51.0))), "4326", "") for i in range(3)
    ]
    return spark.createDataFrame(
        rows,
        schema="name string, geom_0 binary, geom_0_srid string, geom_0_srid_proj string",
    ).coalesce(1)


# ---- gpkg_gbx ---------------------------------------------------------------


def test_gpkg_filename_case_a_stem(spark, tmp_path):
    """gpkg_gbx: save to stem path resolves to <stem>.gpkg."""
    register(spark)
    out_stem = str(tmp_path / "cities")
    _make_df(spark).write.format("gpkg_gbx").mode("overwrite").save(out_stem)
    expected = str(tmp_path / "cities.gpkg")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("gpkg_gbx").load(expected).count() == 3


def test_gpkg_filename_case_b_existing_dir(spark, tmp_path):
    """gpkg_gbx: save to existing dir names the output after the directory."""
    register(spark)
    d = tmp_path / "cities_dir"
    d.mkdir()
    _make_df(spark).write.format("gpkg_gbx").mode("overwrite").save(str(d))
    expected = str(d / "cities_dir.gpkg")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("gpkg_gbx").load(expected).count() == 3


def test_gpkg_filename_case_c_option(spark, tmp_path):
    """gpkg_gbx: .option('fileName', 'cities').save(dir) -> dir/cities.gpkg."""
    register(spark)
    out_dir = str(tmp_path / "outdir")
    (
        _make_df(spark)
        .write.format("gpkg_gbx")
        .mode("overwrite")
        .option("fileName", "cities")
        .save(out_dir)
    )
    expected = str(tmp_path / "outdir" / "cities.gpkg")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("gpkg_gbx").load(expected).count() == 3


# ---- geojson_gbx ------------------------------------------------------------


def test_geojson_filename_case_a_stem(spark, tmp_path):
    """geojson_gbx: save to stem path resolves to <stem>.geojson."""
    register(spark)
    out_stem = str(tmp_path / "places")
    _make_df(spark).write.format("geojson_gbx").mode("overwrite").save(out_stem)
    expected = str(tmp_path / "places.geojson")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("geojson_gbx").load(expected).count() == 3


def test_geojson_filename_case_b_existing_dir(spark, tmp_path):
    """geojson_gbx: save to existing dir names the output after the directory."""
    register(spark)
    d = tmp_path / "places_dir"
    d.mkdir()
    _make_df(spark).write.format("geojson_gbx").mode("overwrite").save(str(d))
    expected = str(d / "places_dir.geojson")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("geojson_gbx").load(expected).count() == 3


def test_geojson_filename_case_c_option(spark, tmp_path):
    """geojson_gbx: .option('fileName', 'places').save(dir) -> dir/places.geojson."""
    register(spark)
    out_dir = str(tmp_path / "outdir")
    (
        _make_df(spark)
        .write.format("geojson_gbx")
        .mode("overwrite")
        .option("fileName", "places")
        .save(out_dir)
    )
    expected = str(tmp_path / "outdir" / "places.geojson")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("geojson_gbx").load(expected).count() == 3


# ---- shapefile_gbx (zip=true, single-file) ----------------------------------


def test_shapefile_zip_filename_case_a_stem(spark, tmp_path):
    """shapefile_gbx zip=true: save to stem path resolves to <stem>.shp.zip."""
    register(spark)
    out_stem = str(tmp_path / "roads")
    (
        _make_df(spark)
        .write.format("shapefile_gbx")
        .mode("overwrite")
        .option("zip", "true")
        .save(out_stem)
    )
    expected = str(tmp_path / "roads.shp.zip")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("shapefile_gbx").load(expected).count() == 3


def test_shapefile_zip_filename_case_b_existing_dir(spark, tmp_path):
    """shapefile_gbx zip=true: save to existing dir names output after the directory."""
    register(spark)
    d = tmp_path / "roads_dir"
    d.mkdir()
    (
        _make_df(spark)
        .write.format("shapefile_gbx")
        .mode("overwrite")
        .option("zip", "true")
        .save(str(d))
    )
    expected = str(d / "roads_dir.shp.zip")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("shapefile_gbx").load(expected).count() == 3


def test_shapefile_zip_filename_case_c_option(spark, tmp_path):
    """shapefile_gbx zip=true: .option('fileName','roads').save(dir) -> dir/roads.shp.zip."""
    register(spark)
    out_dir = str(tmp_path / "outdir")
    (
        _make_df(spark)
        .write.format("shapefile_gbx")
        .mode("overwrite")
        .option("zip", "true")
        .option("fileName", "roads")
        .save(out_dir)
    )
    expected = str(tmp_path / "outdir" / "roads.shp.zip")
    assert os.path.isfile(expected), f"Expected {expected}"
    assert spark.read.format("shapefile_gbx").load(expected).count() == 3


# ---- file_gdb_gbx -----------------------------------------------------------


def test_filegdb_filename_case_a_stem(spark, tmp_path):
    """file_gdb_gbx: save to stem path resolves to <stem>.gdb."""
    register(spark)
    out_stem = str(tmp_path / "parcels")
    _make_df(spark).write.format("file_gdb_gbx").mode("overwrite").save(out_stem)
    expected = str(tmp_path / "parcels.gdb")
    assert os.path.isdir(expected), f"Expected .gdb dir at {expected}"
    assert spark.read.format("file_gdb_gbx").load(expected).count() == 3


def test_filegdb_filename_case_b_existing_dir(spark, tmp_path):
    """file_gdb_gbx: save to existing dir names the .gdb after the directory."""
    register(spark)
    d = tmp_path / "parcels_dir"
    d.mkdir()
    _make_df(spark).write.format("file_gdb_gbx").mode("overwrite").save(str(d))
    expected = str(d / "parcels_dir.gdb")
    assert os.path.isdir(expected), f"Expected .gdb dir at {expected}"
    assert spark.read.format("file_gdb_gbx").load(expected).count() == 3


def test_filegdb_filename_case_c_option(spark, tmp_path):
    """file_gdb_gbx: .option('fileName','parcels').save(dir) -> dir/parcels.gdb."""
    register(spark)
    out_dir = str(tmp_path / "outdir")
    (
        _make_df(spark)
        .write.format("file_gdb_gbx")
        .mode("overwrite")
        .option("fileName", "parcels")
        .save(out_dir)
    )
    expected = str(tmp_path / "outdir" / "parcels.gdb")
    assert os.path.isdir(expected), f"Expected .gdb dir at {expected}"
    assert spark.read.format("file_gdb_gbx").load(expected).count() == 3
