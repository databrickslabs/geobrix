"""Tests for open_header — metadata access without pixel materialization.

open_header(tile) opens a rasterio dataset for header/profile inspection:
  - virtual tile (raster None):
      * whole-file window (None or == full extent) -> rasterio.open(staged_path),
        full-source header, NO window read.
      * sub-window (present AND not full-extent) -> a read-free header view whose
        width/height/transform/bounds reflect the WINDOW (consistent with the
        pixel path and the materialized-equivalent tile), NO window read.
  - bytes/v1 tile (raster set)  -> bytes open via _open (header accessible, read
    never forced by open_header itself, but bytes path is allowed to call read
    if the caller does so; this test asserts the accessor calls work, not that
    read is blocked at the bytes level since the bytes ARE the result).

Discriminating design for virtual tests:
  - Source is 2048x2048. Whole-file tests use a full-extent window and expect
    ds.width == 2048. The windowed test uses a (100,50,512,384) sub-window and
    expects ds.width == 512 / height == 384 with a window-shifted transform.
  - rasterio.io.DatasetReader.read is patched at class level to raise
    AssertionError, wrapping the entire open_header call + accessor calls.
    Any .read() at any point — inside open_header before yield, after yield,
    or inside the accessor calls — trips the patch immediately.
"""

import unittest.mock

import numpy as np
import rasterio
import rasterio.io
from rasterio.transform import from_origin
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import open_tile as ot

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _big_virtual(tmp_path):
    """Write a 2048x2048 EPSG:4326 GTiff; return a WHOLE-FILE virtual tile.

    The window covers the full extent (0,0,2048,2048), so ``open_header`` yields
    the full-source header (width=2048) — the whole-file case. Windowed tests
    override ``window`` with a sub-region.
    """
    p = str(tmp_path / "big.tif")
    prof = dict(
        driver="GTiff",
        width=2048,
        height=2048,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10, 50, 0.001, 0.001),
        nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.zeros((2048, 2048), "float32"), 1)
    return {
        "cellid": 0,
        "raster": None,
        "path": p,
        "window": {"col_off": 0, "row_off": 0, "width": 2048, "height": 2048},
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }


def _bytes_tile(tmp_path):
    """Return a small materialized (bytes) tile dict."""
    from rasterio.io import MemoryFile

    prof = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=1,
        dtype="float32",
        crs="EPSG:32632",
        transform=from_origin(500000, 5600000, 10.0, 10.0),
        nodata=0.0,
    )
    arr = np.ones((1, 64, 64), "float32")
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(arr)
        raw = mf.read()
    return {"cellid": 1, "raster": raw, "metadata": {}}


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_open_header_windowed_virtual_reports_window_dims(tmp_path):
    """Sub-windowed virtual tile: header reports the WINDOW's dims/extent.

    Source is 2048x2048; window is (col=100, row=50, w=512, h=384). open_header
    must yield a header view whose width==512, height==384, transform ==
    src.window_transform(Window(100,50,512,384)), and bounds derived from that
    window — WITHOUT reading pixels (class-level read patch). CRS/count/nodata/
    dtypes proxy the source (window-invariant).
    """
    tile = _big_virtual(tmp_path)
    tile["window"] = {"col_off": 100, "row_off": 50, "width": 512, "height": 384}

    # Compute the expected window transform / bounds from the source header only.
    src_path = tile["path"]
    with rasterio.open(src_path) as src:
        win = Window(100, 50, 512, 384)
        expected_transform = src.window_transform(win)
        expected_bounds = rasterio.windows.bounds(win, src.transform)

    no_read = unittest.mock.patch.object(
        rasterio.io.DatasetReader,
        "read",
        side_effect=AssertionError("open_header must not read pixels"),
    )
    with no_read:
        with ot.open_header(tile) as ds:
            assert ds.width == 512, f"expected window width 512, got {ds.width}"
            assert ds.height == 384, f"expected window height 384, got {ds.height}"
            assert ds.transform == expected_transform
            b = ds.bounds
            assert b.left == expected_bounds[0]
            assert b.bottom == expected_bounds[1]
            assert b.right == expected_bounds[2]
            assert b.top == expected_bounds[3]
            # Window-invariant fields proxy the source.
            assert ds.crs.to_epsg() == 4326
            assert ds.count == 1
            assert ds.nodata == -9999.0
            assert ds.dtypes[0] == "float32"
            # Header accessors read window-correct values.
            assert accessors.width(ds) == 512
            assert accessors.height(ds) == 384
            assert accessors.upperleftx(ds) == expected_transform.c
            assert accessors.scalex(ds) == expected_transform.a
            bb = accessors.boundingbox(ds)
            assert bb is not None
            assert accessors.srid(ds) == 4326


def test_open_header_virtual_metadata_without_read(tmp_path):
    """Whole-file virtual tile: full-source header accessible; zero reads.

    Window == full extent (0,0,2048,2048), so:
    (a) ds.width == 2048 (full source) — the whole-file case yields the plain
        source header, not a windowed view.
    (b) DatasetReader.read patched at class level to raise AssertionError —
        catches any .read() during open_header setup, yield, OR accessor calls.
    """
    tile = _big_virtual(tmp_path)
    no_read = unittest.mock.patch.object(
        rasterio.io.DatasetReader,
        "read",
        side_effect=AssertionError("open_header must not read pixels"),
    )
    with no_read:
        with ot.open_header(tile) as ds:
            w = accessors.width(ds)
            s = accessors.srid(ds)
            bb = accessors.boundingbox(ds)

    # (a) Full source dimensions — NOT the 512 window.
    assert w == 2048, f"expected full-source width 2048, got {w}"
    # (b) If we reach here, no AssertionError was raised → ds.read never called.
    assert s == 4326, f"expected srid 4326, got {s}"
    assert bb is not None, "boundingbox returned None"


def test_open_header_virtual_crs_and_transform(tmp_path):
    """Whole-file virtual tile: CRS and full-source dims (window == full extent)."""
    tile = _big_virtual(tmp_path)
    with ot.open_header(tile) as ds:
        assert ds.crs is not None
        assert ds.crs.to_epsg() == 4326
        assert ds.transform is not None
        # Whole-file window -> full source dimensions.
        assert ds.width == 2048, f"expected 2048 (full source), got {ds.width}"
        assert ds.height == 2048, f"expected 2048 (full source), got {ds.height}"


def test_open_header_bytes_tile_metadata(tmp_path):
    """Bytes/v1 tile: header accessors work correctly via open_header."""
    tile = _bytes_tile(tmp_path)
    with ot.open_header(tile) as ds:
        assert accessors.width(ds) == 64
        assert accessors.height(ds) == 64
        assert accessors.srid(ds) == 32632
        bb = accessors.boundingbox(ds)
        assert bb is not None
