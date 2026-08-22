"""RasterGbxWriter size-gate: small tiles materialise fully; large tiles stream block-wise.

Unit tests (no Spark, no JAR, no /Volumes data required):

1. ``test_windowed_pixels_profile_equiv`` — _windowed_materialize_bytes is pixel-array-equal
   AND profile-equal to materialize_to_bytes on the same no-warp/no-clip virtual tile.
2. ``test_small_tile_uses_materialize_path`` — with GBX_STREAM_MAX_BYTES unset (default 256 MiB
   cap), a tile whose path_file_size is well below the cap routes to materialize_to_bytes.
3. ``test_large_tile_uses_windowed_path`` — with GBX_STREAM_MAX_BYTES=1 (forces "fuse" for any
   file), the writer routes to _windowed_materialize_bytes; materialize_to_bytes is NOT called.
4. ``test_large_tile_round_trip`` — write via the large branch then read back; pixels match.
5. ``test_large_tile_with_pending_bands`` — windowed path correctly applies band-select.
6. ``test_large_tile_subwindow`` — windowed path reads the correct sub-window of the source.
"""

from __future__ import annotations

import os
from typing import Iterator
from unittest.mock import patch

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.raster import reader_schema_v2
from databricks.labs.gbx.ds.writer import RasterGbxWriter
from databricks.labs.gbx.pyrx.core.open_tile import (
    _windowed_materialize_bytes,
    materialize_to_bytes,
)
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_geotiff(path: str, width: int = 8, height: int = 6, count: int = 1) -> None:
    """Write a small Float32 GTiff to *path* with known pixels."""
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(path, "w", **profile) as ds:
        for b in range(1, count + 1):
            ds.write(data + (b - 1) * 100.0, b)


def _virtual_tile(path: str, window=None, metadata: dict | None = None) -> VirtualTile:
    """Build a whole-file (or windowed) virtual tile pointing at *path*."""
    file_size = os.path.getsize(path)
    meta = {"path_file_size": str(file_size)}
    if metadata:
        meta.update(metadata)
    return VirtualTile(cellid=1, path=path, window=window, metadata=meta)


def _row(vt: VirtualTile) -> dict:
    """Pack *vt* into a row dict the writer iterator expects."""
    return {"tile": vt.to_row(), "source": vt.path or ""}


def _write_one(out_dir: str, vt: VirtualTile) -> str:
    """Write a single virtual tile via RasterGbxWriter; return the output path."""
    schema = reader_schema_v2()
    writer = RasterGbxWriter(out_dir, schema, overwrite=True)

    def _iter() -> Iterator[dict]:
        yield _row(vt)

    writer.write(_iter())

    written = sorted(f for f in os.listdir(out_dir) if f.endswith(".tif"))
    assert len(written) == 1, f"expected one .tif output, got {written}"
    return os.path.join(out_dir, written[0])


def _profile_key(ds) -> dict:
    """Extract the profile keys relevant to equivalence checks."""
    return {
        "width": ds.width,
        "height": ds.height,
        "count": ds.count,
        "dtype": ds.dtypes[0],
        "crs": str(ds.crs) if ds.crs else None,
        "nodata": ds.nodata,
        "transform": ds.transform,
    }


# ---------------------------------------------------------------------------
# 1. Pixel + profile equivalence between windowed and full-materialize paths
# ---------------------------------------------------------------------------


def test_windowed_pixels_profile_equiv(tmp_path):
    """_windowed_materialize_bytes output is pixel-array-equal and profile-equal
    to materialize_to_bytes for a no-warp / no-clip virtual tile."""
    src = str(tmp_path / "in.tif")
    _write_geotiff(src)

    vt = _virtual_tile(src)

    # Reference: full-materialize path.
    ref_bytes = materialize_to_bytes(vt).raster
    with MemoryFile(ref_bytes) as mf:
        with mf.open() as ref_ds:
            ref_arr = ref_ds.read()
            ref_prof = _profile_key(ref_ds)

    # Test: windowed-streaming path.
    win_bytes = _windowed_materialize_bytes(vt)
    with MemoryFile(win_bytes) as mf:
        with mf.open() as win_ds:
            win_arr = win_ds.read()
            win_prof = _profile_key(win_ds)

    np.testing.assert_array_equal(win_arr, ref_arr, err_msg="pixel arrays differ")
    assert (
        win_prof == ref_prof
    ), f"profile mismatch:\n  ref={ref_prof}\n  win={win_prof}"


# ---------------------------------------------------------------------------
# 2. Small tile → materialize_to_bytes path taken
# ---------------------------------------------------------------------------


def test_small_tile_uses_materialize_path(tmp_path):
    """With default cap (256 MiB), a small tile routes to materialize_to_bytes."""
    src = str(tmp_path / "in.tif")
    _write_geotiff(src)
    out_dir = str(tmp_path / "out_small")
    os.makedirs(out_dir)

    vt = _virtual_tile(src)  # path_file_size = real file size (a few hundred bytes)

    schema = reader_schema_v2()
    writer = RasterGbxWriter(out_dir, schema, overwrite=True)

    from databricks.labs.gbx.pyrx.core.open_tile import materialize_to_bytes as _mtb

    with patch(
        "databricks.labs.gbx.pyrx.core.open_tile.materialize_to_bytes",
        wraps=_mtb,
    ) as spy:

        def _iter() -> Iterator[dict]:
            yield _row(vt)

        writer.write(_iter())
        assert spy.call_count == 1, (
            f"expected materialize_to_bytes called once for small tile, "
            f"got call_count={spy.call_count}"
        )


# ---------------------------------------------------------------------------
# 3. Large tile → windowed path taken; materialize_to_bytes NOT called
# ---------------------------------------------------------------------------


def test_large_tile_uses_windowed_path(tmp_path, monkeypatch):
    """With GBX_STREAM_MAX_BYTES=1, any file triggers the windowed path;
    materialize_to_bytes must NOT be called."""
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

    src = str(tmp_path / "in.tif")
    _write_geotiff(src)
    out_dir = str(tmp_path / "out_large")
    os.makedirs(out_dir)

    vt = _virtual_tile(src)  # path_file_size > 1 byte → "fuse" decision

    schema = reader_schema_v2()
    writer = RasterGbxWriter(out_dir, schema, overwrite=True)

    with patch(
        "databricks.labs.gbx.pyrx.core.open_tile.materialize_to_bytes"
    ) as mock_mat:
        # _windowed_materialize_bytes must not call materialize_to_bytes in the
        # no-warp / no-clip branch.
        mock_mat.side_effect = AssertionError(
            "materialize_to_bytes must NOT be called on the large (windowed) path"
        )

        def _iter() -> Iterator[dict]:
            yield _row(vt)

        writer.write(_iter())  # must succeed without triggering mock_mat

    # Verify mock was never called.
    assert mock_mat.call_count == 0, (
        "windowed path must not delegate to materialize_to_bytes for a "
        "no-warp / no-clip tile"
    )

    # Verify the output file exists and has the right pixels.
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        actual = ds.read()
    with rasterio.open(src) as orig:
        expected = orig.read()
    np.testing.assert_array_equal(actual, expected)


# ---------------------------------------------------------------------------
# 4. Round-trip: write (large branch) then read back; correct pixels
# ---------------------------------------------------------------------------


def test_large_tile_round_trip(tmp_path, monkeypatch):
    """Write via the windowed path and re-read; pixels must match the source."""
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

    src = str(tmp_path / "in.tif")
    _write_geotiff(src, width=16, height=12, count=2)  # 2-band raster
    out_dir = str(tmp_path / "out_rt")

    vt = _virtual_tile(src)
    out_path = _write_one(out_dir, vt)

    with rasterio.open(out_path) as ds:
        actual = ds.read()
    with rasterio.open(src) as orig:
        expected = orig.read()

    assert actual.shape == expected.shape, "shape mismatch after round-trip"
    np.testing.assert_array_equal(actual, expected, err_msg="round-trip pixel mismatch")


# ---------------------------------------------------------------------------
# 5. Windowed path correctly applies pending band-select
# ---------------------------------------------------------------------------


def test_large_tile_with_pending_bands(tmp_path, monkeypatch):
    """Windowed path correctly applies pending band-select (PENDING_BANDS)."""
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

    src = str(tmp_path / "in3.tif")
    _write_geotiff(src, count=3)  # 3-band raster

    # Virtual tile with pending band-select: only band 2
    vt = VirtualTile(
        cellid=1,
        path=src,
        metadata={
            "path_file_size": str(os.path.getsize(src)),
            "pending_bands": "2",
        },
    )

    win_bytes = _windowed_materialize_bytes(vt)
    with MemoryFile(win_bytes) as mf:
        with mf.open() as ds:
            assert ds.count == 1, f"expected 1 band after band-select, got {ds.count}"
            actual = ds.read(1)

    # Expected: band 2 of the original raster
    with rasterio.open(src) as orig:
        expected = orig.read(2)

    np.testing.assert_array_equal(
        actual, expected, err_msg="band-select pixel mismatch"
    )


# ---------------------------------------------------------------------------
# 6. Windowed path reads the correct sub-window of the source
# ---------------------------------------------------------------------------


def test_large_tile_subwindow(tmp_path, monkeypatch):
    """Windowed path reads only the specified sub-window."""
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

    src = str(tmp_path / "in.tif")
    _write_geotiff(src, width=8, height=6)

    # Sub-window: top-left 4x3 block
    window = (0, 0, 4, 3)  # (col_off, row_off, width, height)
    vt = VirtualTile(
        cellid=1,
        path=src,
        window=window,
        metadata={"path_file_size": str(os.path.getsize(src))},
    )

    win_bytes = _windowed_materialize_bytes(vt)
    with MemoryFile(win_bytes) as mf:
        with mf.open() as ds:
            assert ds.width == 4
            assert ds.height == 3
            actual = ds.read(1)

    # Expected: same sub-window from materialize_to_bytes (reference)
    ref_bytes = materialize_to_bytes(vt).raster
    with MemoryFile(ref_bytes) as mf:
        with mf.open() as ds:
            expected = ds.read(1)

    np.testing.assert_array_equal(actual, expected, err_msg="subwindow pixel mismatch")
