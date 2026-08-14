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

def test_partitions_from_tile_rows_window_and_dims_no_rasterio_open(tmp_path, monkeypatch):
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
        {"path": str(tmp_path / "a.tif"), "window": [0, 0, 4, 3], "width": 4, "height": 3},
        {"path": str(tmp_path / "b.tif"), "window": [0, 0, 8, 6], "width": 8, "height": 6},
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

    assert len(open_calls) == 0, f"rasterio.open called {len(open_calls)} time(s) during planning"
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
    """Row with only path (no window, no dims) → header read for that file only."""
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
        emit_virtual=True,
        budget_bytes=0,
        clip_polygons=[],
        clip_crs=None,
        windows=[],
        tile_size=None,
        overlap_percent=0,
    )

    # Header was opened for the listed file only
    assert any(str(tmp_path / "a.tif") in c for c in open_calls), (
        f"Expected rasterio.open for a.tif; calls={open_calls}"
    )
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
        assert "not_in_manifest" not in c, (
            f"Unlisted file was opened: {c}"
        )
