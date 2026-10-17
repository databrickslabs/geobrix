"""Executes the read_paths doc examples and holds the shared Reference to the code
(Docker; real sample data).

Validates docs/docs/ingestion/read-paths.mdx.
"""

import json
import math
import os
import re
import shutil
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import mdx_tables  # noqa: E402
import read_paths_examples as ex  # noqa: E402

pytestmark = pytest.mark.deadline(360)  # past the 300 s limit of the stream helper

PAGE = "read-paths"


def _expected_borough_rows(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    return spark.read.format("geojson_gbx").load(ex.SAMPLE_BOROUGHS_DIR).count()


def test_read_paths_batch(spark):
    df = ex.read_paths_batch(spark)
    rows = df.select("_source_path", "_source_mtime").collect()
    assert len(rows) == _expected_borough_rows(spark) > 0
    assert all(r["_source_path"].endswith("nyc_boroughs.geojson") for r in rows)
    assert all(r["_source_path"].startswith("/Volumes/") for r in rows)
    assert all(r["_source_mtime"] is not None for r in rows)
    assert {"geom_0", "geom_0_srid", "geom_0_srid_proj"} <= set(df.columns)
    assert "boroname" in df.columns  # the column READ_PATHS_BATCH selects


def test_read_paths_stream(spark, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copy(f"{ex.SAMPLE_BOROUGHS_DIR}/nyc_boroughs.geojson", landing)
    # Sorts first but does not match the format's file pattern: the folder
    # schema_path skips it and takes the schema from the GeoJSON file.
    (landing / "0_readme.txt").write_text("not a GeoJSON file")
    out = ex.read_paths_stream(spark, str(landing), str(tmp_path / "checkpoint"))
    assert out.count() == _expected_borough_rows(spark)
    assert out.select("_source_path").distinct().count() == 1


def test_read_paths_stream_needs_schema_path(spark, tmp_path):
    from databricks.labs.gbx.ds.paths import read_paths

    stream = (
        spark.readStream.format("binaryFile")
        .schema("path string, modificationTime timestamp, length long, content binary")
        .load(str(tmp_path))
        .select("path", "modificationTime")
    )
    with pytest.raises(ValueError, match="schema_path"):
        read_paths(stream, "geojson_gbx")


def _dims(path):
    import rasterio

    with rasterio.open(path) as src:
        return src.width, src.height


def _grid(path, tile):
    w, h = _dims(path)
    return math.ceil(w / tile) * math.ceil(h / tile)


def _windows_per_file(tiles):
    rows = tiles.select("tile.window", "_source_path").collect()
    return rows, Counter(r["_source_path"] for r in rows)


def test_read_paths_raster_windows(spark):
    """``tileSize`` cuts each raster into a grid of ceil(w/ts) * ceil(h/ts) windows,
    spread over ``num_partitions`` tasks. Every count comes from the files'
    dimensions, so this holds on the minimal and the complete sample bundle."""
    folder = ex.SAMPLE_SENTINEL_DIR
    files = sorted(os.path.join(folder, n) for n in os.listdir(folder) if n.endswith(".tif"))
    assert files

    # The displayed size (512) over the whole folder.
    tiles = ex.read_paths_raster(spark)
    assert tiles.rdd.getNumPartitions() == 8  # test-only RDD use
    rows, per_file = _windows_per_file(tiles)
    assert per_file == {f: _grid(f, 512) for f in files}
    assert all(0 < r.window.width <= 512 and 0 < r.window.height <= 512 for r in rows)

    # A smaller grid on the smallest raster: several windows (64 px on the sample
    # rasters), at most 8 x 8 however large the bundle's rasters are.
    smallest = min(files, key=lambda f: (math.prod(_dims(f)), f))
    tile = max(64, math.ceil(max(_dims(smallest)) / 8))
    rows, per_file = _windows_per_file(
        ex.read_paths_raster(spark, smallest, tile_size=str(tile))
    )
    assert per_file == {smallest: _grid(smallest, tile)}
    assert len(rows) > 1
    assert all(0 < r.window.width <= tile and 0 < r.window.height <= tile for r in rows)


def test_displayed_snippets_name_real_api():
    import ast

    from databricks.labs.gbx.ds.paths import read_paths  # noqa: F401

    for snippet in (ex.READ_PATHS_BATCH, ex.READ_PATHS_STREAM, ex.READ_PATHS_RASTER):
        assert "from databricks.labs.gbx.ds.paths import read_paths" in snippet
        ast.parse(snippet)
    assert "read_paths(files" in ex.READ_PATHS_BATCH


def test_snippet_calls_bind_to_real_signatures():
    """Every displayed read_paths call binds to the real signature."""
    from databricks.labs.gbx.ds.paths import read_paths

    for snippet in (ex.READ_PATHS_BATCH, ex.READ_PATHS_STREAM, ex.READ_PATHS_RASTER):
        calls = mdx_tables.calls(snippet, "read_paths")
        assert calls
        for call in calls:
            mdx_tables.bind(read_paths, call)


def test_object_store_uri_is_rejected(spark):
    """Paths section: an object-store URI or GDAL virtual path fails with an error
    naming it."""
    from databricks.labs.gbx.ds.paths import read_paths

    files = ex._files(spark, ex.SAMPLE_BOROUGHS_DIR, "*.geojson")
    for uri in ("s3://bucket/landing", "/vsis3/bucket/landing", "/vsimem/landing"):
        with pytest.raises(ValueError, match=f"Unity Catalog Volume.*{uri}"):
            read_paths(files, "geojson_gbx", schema_path=uri)


def test_default_file_patterns_table_matches_the_code():
    """The Reference's default-pattern table lists exactly DEFAULT_GLOBS, with
    pmtiles_gbx once per `source` (its pattern follows that option)."""
    from databricks.labs.gbx.ds.paths import DEFAULT_GLOBS, default_glob

    shown = {}
    for formats, glob in mdx_tables.section_rows(PAGE, "Default file patterns"):
        source = re.search(r'`source="(\w+)"`', formats)
        for fmt in re.findall(r"`(\w+_gbx)`", formats):
            shown[(fmt, source.group(1) if source else None)] = glob.strip("`")
    sources = ("archive", "raster")
    want = {(f, None): g for f, g in DEFAULT_GLOBS.items() if f != "pmtiles_gbx"}
    want.update(
        {("pmtiles_gbx", s): default_glob("pmtiles", {"source": s}) for s in sources}
    )
    assert shown == want
    assert shown[("pmtiles_gbx", "archive")] == DEFAULT_GLOBS["pmtiles_gbx"]


def _formats_in_first_column(heading):
    return [
        fmt
        for formats, *_ in mdx_tables.section_rows(PAGE, heading)
        for fmt in re.findall(r"`(\w+_gbx)`", formats)
    ]


def test_supported_formats_table_lists_every_reader():
    """The Reference's formats table names every reader read_paths supports, once,
    and links each to its reader page."""
    from databricks.labs.gbx.ds.paths import SUPPORTED_FORMATS

    assert sorted(_formats_in_first_column("Supported formats")) == sorted(SUPPORTED_FORMATS)
    for formats, reader, _ in mdx_tables.section_rows(PAGE, "Supported formats"):
        assert re.search(r"\]\(\.\./(readers|writers)/[\w-]+\)", reader), formats


def test_unreadable_files_table_lists_every_reader():
    """Each supported reader appears exactly once in the unreadable-files table."""
    from databricks.labs.gbx.ds.paths import SUPPORTED_FORMATS

    assert sorted(_formats_in_first_column("Unreadable files")) == sorted(SUPPORTED_FORMATS)


# --- the unreadable-files table, format by format ----------------------------

GARBAGE = b"\x00not a geospatial file\x13" * 40


def _listing(spark, path):
    when = datetime.fromtimestamp(os.stat(path).st_mtime, timezone.utc)
    return spark.createDataFrame(
        [(path, when)], "path string, modificationTime timestamp"
    )


def _garbage(tmp_path, name):
    path = tmp_path / name
    path.write_bytes(GARBAGE)
    return str(path)


def _good_las(tmp_path):
    import laspy

    las = laspy.LasData(laspy.LasHeader(point_format=3, version="1.2"))
    las.x, las.y, las.z = [1.0, 2.0], [1.0, 2.0], [1.0, 2.0]
    path = tmp_path / "good.las"
    las.write(str(path))
    return str(path)


def _good_jpg(tmp_path):
    from PIL import Image

    path = tmp_path / "good.jpg"
    Image.new("RGB", (8, 8)).save(path)
    return str(path)


def _good_nc(tmp_path):
    import numpy as np
    import xarray as xr

    path = tmp_path / "good.nc"
    xr.Dataset(
        {"t": (("y", "x"), np.ones((4, 4), "f4"))},
        coords={"y": np.arange(4.0), "x": np.arange(4.0)},
    ).to_netcdf(path)
    return str(path)


def _rows_of(spark, path, fmt, options=None, schema_path=None):
    """read_paths over one file. The schema is resolved here, on the driver, so a
    failure of the returned DataFrame's collect() is a failure to read the file."""
    from databricks.labs.gbx.ds.paths import read_paths

    return read_paths(_listing(spark, path), fmt, options, schema_path=schema_path)


def _read(spark, path, fmt, options=None, schema_path=None):
    return _rows_of(spark, path, fmt, options, schema_path).collect()


SAMPLE_GPKG = f"{ex.SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"
SAMPLE_TIF = f"{ex.SAMPLE_SENTINEL_DIR}/nyc_sentinel2_red.tif"


@pytest.mark.parametrize(
    "fmt, name, options, schema_file",
    [
        ("geojson_gbx", "bad.geojson", None, f"{ex.SAMPLE_BOROUGHS_DIR}/nyc_boroughs.geojson"),
        ("gpkg_gbx", "bad.gpkg", None, SAMPLE_GPKG),
        ("netcdf_gbx", "bad.nc", None, "good.nc"),
        # PMTiles' schema is fixed, so the bad archive itself can define it.
        ("pmtiles_gbx", "bad.pmtiles", {"source": "archive"}, None),
        ("gtiff_gbx", "bad_tiled.tif", {"tileSize": "256"}, SAMPLE_TIF),
        ("gtiff_gbx", "bad_bytes.tif", {"virtualTiles": "false"}, SAMPLE_TIF),
    ],
)
def test_unreadable_file_fails_the_read(spark, tmp_path, fmt, name, options, schema_file):
    """Rows that say "Fails the read": reading the file stops the query."""
    if schema_file == "good.nc":
        schema_file = _good_nc(tmp_path)
    bad = _garbage(tmp_path, name)
    rows = _rows_of(spark, bad, fmt, options, schema_path=schema_file or bad)
    with pytest.raises(Exception):
        rows.collect()


def test_unreadable_raster_read_as_a_virtual_tile_is_not_opened(spark, tmp_path):
    """Raster row: without tileSize (virtual tiles, the default) the file is not
    opened at ingest; its row refers to the file, and reading the pixels fails."""
    bad = _garbage(tmp_path, "bad.tif")
    rows = _read(spark, bad, "gtiff_gbx", schema_path=SAMPLE_TIF)
    assert len(rows) == 1
    assert rows[0]["tile"]["path"] == bad and rows[0]["tile"]["raster"] is None
    assert rows[0]["_source_path"] == bad


def test_unreadable_lidar_file_is_skipped(spark, tmp_path):
    rows = _read(
        spark, _garbage(tmp_path, "bad.las"), "lidar_gbx", schema_path=_good_las(tmp_path)
    )
    assert rows == []


def test_unreadable_exif_file_gives_a_row_of_null_metadata(spark, tmp_path):
    bad = _garbage(tmp_path, "bad.jpg")
    (row,) = _read(spark, bad, "exif_gbx", schema_path=_good_jpg(tmp_path))
    assert row["_source_path"] == bad
    for column in ("camera_make", "camera_model", "timestamp", "latitude", "longitude"):
        assert row[column] is None


# --- on_error="skip": the third column of the unreadable-files table ----------


def _feature_collection(path, geometries):
    feats = [
        {"type": "Feature", "properties": {"id": i}, "geometry": g}
        for i, g in enumerate(geometries)
    ]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    return str(path)


def _point(i):
    return {"type": "Point", "coordinates": [i, i]}


def _skipped(spark, tmp_path, path, fmt, options=None, schema_path=None):
    """``(rows, bad records)`` of read_paths(on_error="skip") over one file; the
    records are written to a folder (as declare_ingest does) to inspect them."""
    import pyarrow.parquet as pq

    from databricks.labs.gbx.ds.paths import read_paths

    folder = tmp_path / "bad_records"
    rows = read_paths(
        _listing(spark, path),
        fmt,
        options,
        schema_path=schema_path or path,
        on_error="skip",
        bad_records_dir=str(folder),
    ).collect()
    recs = pq.read_table(str(folder)).to_pylist() if folder.exists() else []
    return rows, [(r["record_index"], r["error_class"]) for r in recs]


def test_skip_sets_a_feature_aside(spark, tmp_path):
    path = _feature_collection(tmp_path / "a.geojson", [_point(0), None, _point(2)])
    rows, recs = _skipped(spark, tmp_path, path, "geojson_gbx")
    assert sorted(r["id"] for r in rows) == [0, 2]
    assert recs == [("feature=1", "NullGeometry")]


def test_skip_sets_a_geojson_document_aside_whole(spark, tmp_path):
    good = _feature_collection(tmp_path / "good.geojson", [_point(0)])
    bad = tmp_path / "bad.geojson"
    bad.write_text(open(good).read()[:-10])  # truncated
    rows, recs = _skipped(spark, tmp_path, str(bad), "geojson_gbx", schema_path=good)
    assert rows == [] and recs == [(None, "pyogrio.errors.DataSourceError")]


def test_geojsonseq_line_that_is_not_json(spark, tmp_path):
    """fail: the reader drops the line; skip: the line is a bad record."""
    from databricks.labs.gbx.ds.paths import read_paths

    lines = [
        json.dumps({"type": "Feature", "properties": {"id": i}, "geometry": _point(i)})
        for i in range(5)
    ]
    lines[2] = "not json"
    path = tmp_path / "a.geojsonl"
    path.write_text("\n".join(lines) + "\n")
    assert len(read_paths(_listing(spark, str(path)), "geojsonl_gbx").collect()) == 4
    rows, recs = _skipped(spark, tmp_path, str(path), "geojsonl_gbx")
    assert len(rows) == 4 and recs == [("line=3", "RecordsNotRead")]


def test_skip_reads_the_complete_points_of_a_truncated_las_file(spark, tmp_path):
    path = _cut_las(tmp_path)
    rows, recs = _skipped(spark, tmp_path, path, "lidar_gbx", schema_path=_good_las(tmp_path))
    assert len(rows) == 6 and recs == [(None, "RecordsNotRead")]


def test_skip_sets_a_file_that_is_not_a_jpeg_aside(spark, tmp_path):
    """The file that gives a row of NULL metadata with on_error="fail"."""
    bad = _garbage(tmp_path, "bad.jpg")
    rows, recs = _skipped(spark, tmp_path, bad, "exif_gbx", schema_path=_good_jpg(tmp_path))
    assert rows == [] and recs == [(None, "NotJpeg")]


def _cut_las(tmp_path, keep=6):
    import laspy

    las = laspy.LasData(laspy.LasHeader(point_format=3, version="1.2"))
    las.x, las.y, las.z = [float(i) for i in range(10)], [1.0] * 10, [1.0] * 10
    path = str(tmp_path / "cut.las")
    las.write(path)
    with laspy.open(path) as r:
        offset, reclen = r.header.offset_to_point_data, r.header.point_format.size
    with open(path, "r+b") as fh:
        fh.truncate(offset + keep * reclen + 5)
    return path


def test_truncated_las_fails_in_points_mode_and_gives_its_row_in_metadata_mode(
    spark, tmp_path
):
    cut = _cut_las(tmp_path)
    with pytest.raises(Exception):
        _read(spark, cut, "lidar_gbx", schema_path=_good_las(tmp_path))
    (row,) = _read(spark, cut, "lidar_gbx", {"mode": "metadata"})
    assert row["point_count"] == 10  # the header's count, not what the file holds


def test_empty_exif_file_is_skipped(spark, tmp_path):
    empty = tmp_path / "empty.jpg"
    empty.write_bytes(b"")
    assert _read(spark, str(empty), "exif_gbx", schema_path=_good_jpg(tmp_path)) == []
