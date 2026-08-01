"""Tests for open_header — metadata access without pixel materialization.

open_header(tile) opens a rasterio dataset for header/profile inspection:
  - virtual tile (raster None)  -> rasterio.open(staged_path), NO window read.
  - bytes/v1 tile (raster set)  -> bytes open via _open (header accessible, read
    never forced by open_header itself, but bytes path is allowed to call read
    if the caller does so; this test asserts the accessor calls work, not that
    read is blocked at the bytes level since the bytes ARE the result).
"""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import open_tile as ot

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _big_virtual(tmp_path):
    """Write a 2048x2048 EPSG:4326 GTiff and return a virtual-tile dict."""
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


def test_open_header_virtual_metadata_without_read(tmp_path):
    """Virtual tile: header accessors work and ds.read is never called."""
    tile = _big_virtual(tmp_path)
    with ot.open_header(tile) as ds:
        # Spy: wrap ds.read to detect any call.
        orig = ds.read
        calls = []

        def spy_read(*a, **k):
            calls.append((a, k))
            return orig(*a, **k)

        ds.read = spy_read

        w = accessors.width(ds)
        s = accessors.srid(ds)
        bb = accessors.boundingbox(ds)

        # Correctness: header values must match what we wrote.
        assert w == 2048, f"expected width 2048, got {w}"
        assert s == 4326, f"expected srid 4326, got {s}"
        assert bb is not None, "boundingbox returned None"

        # No-read guarantee: none of width/srid/boundingbox called ds.read.
        assert calls == [], f"ds.read was called {len(calls)} time(s) — not header-only"


def test_open_header_virtual_crs_and_transform(tmp_path):
    """Virtual tile: CRS and transform are accessible without pixel reads."""
    tile = _big_virtual(tmp_path)
    with ot.open_header(tile) as ds:
        assert ds.crs is not None
        assert ds.crs.to_epsg() == 4326
        assert ds.transform is not None
        # Full source dimensions are visible — not a windowed subset.
        assert ds.width == 2048
        assert ds.height == 2048


def test_open_header_bytes_tile_metadata(tmp_path):
    """Bytes/v1 tile: header accessors work correctly via open_header."""
    tile = _bytes_tile(tmp_path)
    with ot.open_header(tile) as ds:
        assert accessors.width(ds) == 64
        assert accessors.height(ds) == 64
        assert accessors.srid(ds) == 32632
        bb = accessors.boundingbox(ds)
        assert bb is not None
