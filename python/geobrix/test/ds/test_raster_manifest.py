"""Tests for manifest/tilesTable tile-row input helpers (Approach 1)."""

import json

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin


def _write_sample(path, width=4, height=3, epsg=4326):
    """Write a minimal single-band float32 GeoTIFF to *path*."""
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    with rasterio.open(str(path), "w", **profile) as ds:
        ds.write(data, 1)


# ---------------------------------------------------------------------------
# _resolved_budget
# ---------------------------------------------------------------------------


def test_resolved_budget_size_mib_wins():
    from databricks.labs.gbx.ds.raster import _resolved_budget

    assert _resolved_budget(size_mib=2, strategy="none") == 2 * 1024 * 1024


def test_resolved_budget_strategy_used_when_no_size_mib():
    from databricks.labs.gbx.ds.raster import _resolved_budget

    # strategy="none" → decoded_budget_bytes returns 0 (no split)
    result = _resolved_budget(size_mib=-1, strategy="none")
    assert result == 0


# ---------------------------------------------------------------------------
# _read_manifest_rows (JSON path only — Parquet needs Spark; tested in Task 4)
# ---------------------------------------------------------------------------


def test_read_manifest_rows_json(tmp_path):
    from databricks.labs.gbx.ds.raster import _read_manifest_rows

    manifest = [
        {"path": "/Volumes/x/a.tif", "window": [0, 0, 256, 256]},
        {"path": "/Volumes/x/b.tif"},
    ]
    manifest_file = str(tmp_path / "tiles.json")
    with open(manifest_file, "w") as f:
        json.dump(manifest, f)

    rows = _read_manifest_rows(manifest_file)
    assert len(rows) == 2
    assert rows[0]["path"] == "/Volumes/x/a.tif"
    assert rows[0]["window"] == [0, 0, 256, 256]
    assert rows[1].get("window") is None


def test_read_manifest_rows_json_not_found():
    from databricks.labs.gbx.ds.raster import _read_manifest_rows

    with pytest.raises(FileNotFoundError):
        _read_manifest_rows("/nonexistent/manifest.json")


# ---------------------------------------------------------------------------
# _partitions_from_tile_rows
# ---------------------------------------------------------------------------


def test_partitions_from_tile_rows_window_and_dims_no_rasterio_open(
    tmp_path, monkeypatch
):
    """Row with path + window + dims → _TilePartition built without rasterio.open."""
    _write_sample(tmp_path / "a.tif", width=4, height=3)
    _write_sample(tmp_path / "b.tif", width=8, height=6)

    open_calls = []
    real_open = rasterio.open

    def _mock_open(p, *a, **kw):
        open_calls.append(p)
        return real_open(p, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [
        {
            "path": str(tmp_path / "a.tif"),
            "window": [0, 0, 4, 3],
            "width": 4,
            "height": 3,
        },
        {
            "path": str(tmp_path / "b.tif"),
            "window": [0, 0, 8, 6],
            "width": 8,
            "height": 6,
        },
    ]
    parts = _partitions_from_tile_rows(
        rows,
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    assert (
        len(open_calls) == 0
    ), f"rasterio.open called {len(open_calls)} time(s) during planning"
    assert len(parts) == 2
    assert parts[0].file_path == str(tmp_path / "a.tif")
    assert parts[0].window == (0, 0, 4, 3)
    assert parts[0].emit_virtual is True
    assert parts[1].file_path == str(tmp_path / "b.tif")
    assert parts[1].window == (0, 0, 8, 6)


def test_partitions_from_tile_rows_window_only_no_rasterio_open(tmp_path, monkeypatch):
    """Row with path + window but no dims → still no rasterio.open (window is enough)."""
    _write_sample(tmp_path / "a.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open

    def _mock_open(p, *a, **kw):
        open_calls.append(p)
        return real_open(p, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [{"path": str(tmp_path / "a.tif"), "window": [0, 0, 4, 3]}]
    parts = _partitions_from_tile_rows(
        rows,
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    assert len(open_calls) == 0
    assert parts[0].window == (0, 0, 4, 3)


def test_partitions_from_tile_rows_path_only_opens_header(tmp_path, monkeypatch):
    """Row with only path (no window, no dims), materialized case → header read for that file only.

    Note: with emit_virtual=True, Task 5 (lazy planning) makes path-only rows return
    window=None without opening the header at plan time — the window is resolved lazily
    in read(). This test uses emit_virtual=False (materialized) to verify the path-only
    header-open behaviour that still applies for non-virtual tiles.
    """
    _write_sample(tmp_path / "a.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open

    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [{"path": str(tmp_path / "a.tif")}]
    parts = _partitions_from_tile_rows(
        rows,
        emit_virtual=False,  # materialized: header still read at plan time
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    # Header was opened for the listed file only (materialized path)
    assert any(
        str(tmp_path / "a.tif") in c for c in open_calls
    ), f"Expected rasterio.open for a.tif; calls={open_calls}"
    assert len(parts) >= 1


def test_partitions_from_tile_rows_unlisted_file_never_opened(tmp_path, monkeypatch):
    """A file on disk that is NOT in the manifest is never opened."""
    _write_sample(tmp_path / "in_manifest.tif", width=4, height=3)
    _write_sample(tmp_path / "not_in_manifest.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open

    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _partitions_from_tile_rows

    rows = [{"path": str(tmp_path / "in_manifest.tif"), "window": [0, 0, 4, 3]}]
    _partitions_from_tile_rows(
        rows,
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    for c in open_calls:
        assert "not_in_manifest" not in c, f"Unlisted file was opened: {c}"


# ---------------------------------------------------------------------------
# Integration tests — full partitions() routing via RasterGbxReader
# ---------------------------------------------------------------------------


def test_partitions_manifest_json_end_to_end(tmp_path, monkeypatch):
    """RasterGbxReader with manifest= option → correct partitions, 0 rasterio.open."""
    _write_sample(tmp_path / "r0.tif", width=4, height=3)
    _write_sample(tmp_path / "r1.tif", width=8, height=6)

    manifest = [
        {"path": str(tmp_path / "r0.tif"), "window": [0, 0, 4, 3]},
        {"path": str(tmp_path / "r1.tif"), "window": [0, 0, 8, 6]},
    ]
    manifest_file = str(tmp_path / "tiles.json")
    with open(manifest_file, "w") as f:
        json.dump(manifest, f)

    open_calls = []
    real_open = rasterio.open

    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    reader = RasterGbxReader(
        {
            "path": str(tmp_path),
            "manifest": manifest_file,
            "virtualTiles": "true",
        }
    )
    parts = reader.partitions()

    assert (
        len(open_calls) == 0
    ), f"Expected 0 rasterio.open during partitions(); got {open_calls}"
    assert len(parts) == 2
    assert parts[0].file_path == str(tmp_path / "r0.tif")
    assert parts[0].window == (0, 0, 4, 3)
    assert parts[1].file_path == str(tmp_path / "r1.tif")
    assert parts[1].window == (0, 0, 8, 6)


def test_partitions_manifest_mutual_exclusion_error():
    """Supplying both manifest and tilesTable raises ValueError."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    with pytest.raises(ValueError, match="mutually exclusive"):
        RasterGbxReader(
            {
                "path": "/tmp/x",
                "manifest": "/tmp/m.json",
                "tilesTable": "catalog.schema.tiles",
            }
        )


def test_partitions_tiles_table_flat_columns(tmp_path, spark):
    """tilesTable with flat col_off/row_off/width/height columns (not nested window).

    This exercises the flat-column layout branch in _partitions_from_tile_rows:
    when a tilesTable query result carries window fields as separate top-level
    columns (common for Delta tables built by ingest pipelines), partitions()
    must build _TilePartition objects without any rasterio.open calls.
    """
    _write_sample(tmp_path / "raster.tif", width=4, height=3)

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("path", StringType(), False),
            StructField("col_off", IntegerType(), True),
            StructField("row_off", IntegerType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ]
    )
    tile_data = [(str(tmp_path / "raster.tif"), 0, 0, 4, 3)]
    spark.createDataFrame(tile_data, schema=schema).createOrReplaceTempView(
        "_test_tile_rows_flat_gbx"
    )

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    reader = RasterGbxReader(
        {
            "path": str(tmp_path),
            "tilesTable": "_test_tile_rows_flat_gbx",
            "virtualTiles": "true",
        }
    )
    parts = reader.partitions()

    assert len(parts) == 1
    assert parts[0].file_path == str(tmp_path / "raster.tif")
    # Flat cols col_off/row_off/width/height → whole-file window (0,0,4,3)
    assert parts[0].window == (0, 0, 4, 3)
    assert parts[0].emit_virtual is True


def test_partitions_tiles_table_end_to_end(tmp_path, spark):
    """RasterGbxReader with tilesTable= option reads tile rows from a Spark temp view."""
    _write_sample(tmp_path / "raster.tif", width=4, height=3)

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("path", StringType(), False),
            StructField("col_off", IntegerType(), True),
            StructField("row_off", IntegerType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ]
    )
    tile_data = [(str(tmp_path / "raster.tif"), 0, 0, 4, 3)]
    spark.createDataFrame(tile_data, schema=schema).createOrReplaceTempView(
        "_test_tile_rows_gbx"
    )

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    reader = RasterGbxReader(
        {
            "path": str(tmp_path),
            "tilesTable": "_test_tile_rows_gbx",
            "virtualTiles": "true",
        }
    )
    parts = reader.partitions()

    assert len(parts) == 1
    assert parts[0].file_path == str(tmp_path / "raster.tif")
    assert parts[0].window == (0, 0, 4, 3)


def test_partitions_manifest_full_spark_read(tmp_path, spark):
    """End-to-end Spark read with manifest option emits correct virtual tiles."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    _write_sample(tmp_path / "raster.tif", width=4, height=3)
    manifest = [{"path": str(tmp_path / "raster.tif"), "window": [0, 0, 4, 3]}]
    manifest_file = str(tmp_path / "manifest.json")
    with open(manifest_file, "w") as f:
        json.dump(manifest, f)

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx")
        .option("manifest", manifest_file)
        .option("virtualTiles", "true")
        .load(str(tmp_path))
        .collect()
    )

    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert tile["raster"] is None  # virtual: no bytes
    assert tile["path"] == str(tmp_path / "raster.tif")
    assert tile["window"]["width"] == 4
    assert tile["window"]["height"] == 3


# ---------------------------------------------------------------------------
# Task 3: skipOrdering DataSource option
# ---------------------------------------------------------------------------


def test_raster_tilestable_skip_ordering_option_manifest_path(tmp_path):
    """skipOrdering='true' on the manifest/tilesTable path suppresses the T8 sort.

    Uses a manifest (no Spark table needed) since both manifest and tilesTable
    share the same sort gate at the end of the Approach-1 branch.

    Checks:
    - Default (no skipOrdering): partitions sorted by file_path asc.
    - skipOrdering='true': partitions preserve the manifest input order (unsorted).
    """
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    # Manifest with paths in reverse-alpha order — unsorted input.
    manifest = [
        {"path": "/z/z.tif", "window": [0, 0, 4, 4]},
        {"path": "/a/a.tif", "window": [0, 0, 4, 4]},
        {"path": "/m/m.tif", "window": [0, 0, 4, 4]},
    ]
    mf = str(tmp_path / "tiles.json")
    with open(mf, "w") as fh:
        json.dump(manifest, fh)

    # Default: must be sorted.
    r_sorted = RasterGbxReader({"path": "/x", "manifest": mf})
    parts_sorted = r_sorted.partitions()
    paths_sorted = [p.file_path for p in parts_sorted]
    assert paths_sorted == sorted(
        paths_sorted
    ), f"Default must sort by file_path; got {paths_sorted}"

    # skipOrdering='true': must preserve input order (unsorted).
    r_skip = RasterGbxReader({"path": "/x", "manifest": mf, "skipOrdering": "true"})
    parts_skip = r_skip.partitions()
    paths_skip = [p.file_path for p in parts_skip]
    # Precondition: input IS unsorted.
    assert paths_skip != sorted(
        paths_skip
    ), f"Precondition: manifest order must be unsorted; got {paths_skip}"
    assert paths_skip == [
        "/z/z.tif",
        "/a/a.tif",
        "/m/m.tif",
    ], f"skipOrdering=true must preserve manifest order; got {paths_skip}"


def test_raster_dir_walk_skip_ordering_option():
    """skipOrdering='true' on directory-walk path preserves walk order (unsorted)."""
    from unittest.mock import patch

    from databricks.labs.gbx.ds.raster import RasterGbxReader, _TilePartition

    with (
        # _list_source_files calls list_local_files from file_gbx; patch at source.
        patch(
            "databricks.labs.gbx.ds.file_gbx.list_local_files",
            return_value=["/z/z.tif", "/a/a.tif", "/m/m.tif"],
        ),
        patch("databricks.labs.gbx.ds.raster._plan_partitions_for_file") as mock_ppf,
    ):
        # Non-sorted input to prove the sort gate is bypassed.
        mock_ppf.side_effect = lambda file_path, **kw: [
            _TilePartition(file_path=file_path, window=(0, 0, 4, 4))
        ]
        r = RasterGbxReader({"path": "/some/dir", "skipOrdering": "true"})
        parts = r.partitions()

    paths = [p.file_path for p in parts]
    assert paths == [
        "/z/z.tif",
        "/a/a.tif",
        "/m/m.tif",
    ], f"skipOrdering=true on walk path must preserve walk order; got {paths}"
    assert paths != sorted(
        paths
    ), "Precondition: walk order must be unsorted to prove the sort gate was bypassed"
