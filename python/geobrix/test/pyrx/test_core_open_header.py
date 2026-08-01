"""Tests for open_header — metadata access without pixel materialization.

open_header(tile) opens a rasterio dataset for header/profile inspection:
  - virtual tile (raster None)  -> rasterio.open(staged_path), NO window read.
  - bytes/v1 tile (raster set)  -> bytes open via _open (header accessible, read
    never forced by open_header itself, but bytes path is allowed to call read
    if the caller does so; this test asserts the accessor calls work, not that
    read is blocked at the bytes level since the bytes ARE the result).

Discriminating design for virtual tests:
  - Source is 2048x2048; tile window is 512x512 (a sub-window).
    open_header must yield ds.width == 2048 (full source header).
    If it regressed to open_tile (windowed materialise), ds.width would be 512.
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

from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import open_tile as ot

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _big_virtual(tmp_path):
    """Write a 2048x2048 EPSG:4326 GTiff; tile window is 512x512 sub-window."""
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
    # Window is a 512x512 sub-region — intentionally smaller than the full
    # source.  open_header must expose the FULL source (width=2048), not this
    # window.  open_tile (windowed) would yield width=512 — wrong for a header
    # accessor.
    return {
        "cellid": 0,
        "raster": None,
        "path": p,
        "window": {"col_off": 0, "row_off": 0, "width": 512, "height": 512},
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


def test_open_header_virtual_metadata_without_read(tmp_path):
    """Virtual tile: full-source header accessible; zero pixel reads anywhere.

    Discriminates on two axes:
    (a) ds.width == 2048 (full source, not the 512 window) — proves open_header
        opened the source header, not a materialised windowed slice.
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
    """Virtual tile: CRS and transform reflect the full source, not the window."""
    tile = _big_virtual(tmp_path)
    with ot.open_header(tile) as ds:
        assert ds.crs is not None
        assert ds.crs.to_epsg() == 4326
        assert ds.transform is not None
        # Full source dimensions — window is 512x512 but header must show 2048x2048.
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
