"""Tests for Approach 3: lazy planning (no rasterio.open at plan time for virtual passthrough)."""

import numpy as np
import rasterio
from rasterio.transform import from_origin


def _write_sample(path, width=4, height=3, epsg=4326):
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
# _plan_partitions_for_file: virtual passthrough case should NOT open header
# ---------------------------------------------------------------------------


def test_plan_partitions_virtual_passthrough_no_rasterio_open(tmp_path, monkeypatch):
    """virtualTiles=True, no split, no AOI → rasterio.open NOT called at plan time."""
    _write_sample(tmp_path / "raster.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open

    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import _plan_partitions_for_file

    parts = _plan_partitions_for_file(
        file_path=str(tmp_path / "raster.tif"),
        budget_bytes=0,
        emit_virtual=True,
    )

    assert (
        len(open_calls) == 0
    ), f"rasterio.open called {len(open_calls)} time(s) during planning: {open_calls}"
    assert len(parts) == 1
    assert parts[0].window is None  # lazy: filled in by read()
    assert parts[0].emit_virtual is True
    assert parts[0].is_passthrough is False


def test_plan_partitions_virtual_three_files_zero_opens(tmp_path, monkeypatch):
    """Three files in a directory → zero header opens during partitions()."""
    for i in range(3):
        _write_sample(tmp_path / f"raster_{i}.tif", width=4, height=3)

    open_calls = []
    real_open = rasterio.open

    def _mock_open(p, *a, **kw):
        open_calls.append(str(p))
        return real_open(p, *a, **kw)

    monkeypatch.setattr(rasterio, "open", _mock_open)

    from databricks.labs.gbx.ds.raster import RasterGbxReader

    reader = RasterGbxReader({"path": str(tmp_path), "virtualTiles": "true"})
    parts = reader.partitions()

    assert (
        len(open_calls) == 0
    ), f"Expected 0 rasterio.open during partitions(); got {len(open_calls)}: {open_calls}"
    assert len(parts) == 3
    for p in parts:
        assert p.window is None
        assert p.emit_virtual is True


# ---------------------------------------------------------------------------
# tileSize case must STILL read header at plan (correctness regression guard)
# ---------------------------------------------------------------------------


def test_plan_partitions_tilesize_still_reads_header_at_plan(tmp_path, monkeypatch):
    """tileSize requires dims at plan time → rasterio.open IS called."""
    _write_sample(tmp_path / "raster.tif", width=8, height=6)

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
            "virtualTiles": "true",
            "tileSize": "4",
        }
    )
    parts = reader.partitions()

    # Header opened for grid-window planning
    assert len(open_calls) >= 1, "Expected rasterio.open for tileSize grid planning"
    # 8×6 with 4×4 tiles: ceil(8/4) × ceil(6/4) = 2 × 2 = 4 partitions
    assert len(parts) == 4
    for p in parts:
        assert p.window is not None  # tileSize fills window at plan


# ---------------------------------------------------------------------------
# End-to-end: read() fills the lazy window; emitted tile has correct dims
# ---------------------------------------------------------------------------


def test_read_fills_lazy_window_via_spark(tmp_path, spark):
    """Virtual passthrough read via Spark → tile.window matches actual raster dims."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    _write_sample(tmp_path / "raster.tif", width=4, height=3)

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .load(str(tmp_path / "raster.tif"))
        .collect()
    )

    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert tile["raster"] is None, "Virtual tile must have no raster bytes"
    assert tile["window"] is not None, "read() must fill the lazy window"
    assert tile["window"]["col_off"] == 0
    assert tile["window"]["row_off"] == 0
    assert tile["window"]["width"] == 4
    assert tile["window"]["height"] == 3


def test_read_lazy_window_three_files(tmp_path, spark):
    """Three files: each emitted virtual tile has the correct per-file window."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    sizes = [(4, 3), (8, 6), (2, 2)]
    for i, (w, h) in enumerate(sizes):
        _write_sample(tmp_path / f"raster_{i}.tif", width=w, height=h)

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .load(str(tmp_path))
        .collect()
    )

    assert len(rows) == 3
    by_path = {r["tile"]["path"]: r["tile"] for r in rows}
    for i, (w, h) in enumerate(sizes):
        key = str(tmp_path / f"raster_{i}.tif")
        tile = by_path[key]
        assert tile["window"]["width"] == w, f"width mismatch for raster_{i}.tif"
        assert tile["window"]["height"] == h, f"height mismatch for raster_{i}.tif"
