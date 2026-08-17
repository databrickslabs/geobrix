"""Unit tests for open_tile._tile_to_bytes — the single-encode path.

These tests prove that _tile_to_bytes produces semantically identical output
to materialize_to_bytes(vt).raster for the same input, covering:
  - materialized tile (passthrough)
  - virtual tile, no warp, no clip (the common path)
  - virtual tile with pending_nodata instruction
  - virtual tile with clip_polygon (the clip path)

The goal is to confirm the optimization (skip the double ZSTD encode/decode) is
correct: the output pixels + georeference are identical to the reference path.
"""

import numpy as np
import rasterio
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.open_tile import PENDING_NODATA
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

W, H = 64, 64
WINDOW = (8, 8, 32, 24)  # col_off, row_off, width, height


def _write_src(tmp_path, name="src.tif", w=W, h=H, epsg=4326, declare_nodata=True):
    path = str(tmp_path / name)
    data = np.arange(w * h, dtype="float32").reshape(h, w)
    transform = from_origin(10.0, 50.0, 0.01, 0.01)
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
    )
    if declare_nodata:
        profile["nodata"] = -9999.0
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)
    return path


def _pixels(tile_bytes):
    with _serde.open_tile(tile_bytes) as ds:
        return ds.read(1)


def _meta(tile_bytes):
    with _serde.open_tile(tile_bytes) as ds:
        return ds.crs, ds.transform, ds.width, ds.height


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_materialized_returns_same_bytes():
    """_tile_to_bytes returns bytes(vt.raster) for a materialized tile."""
    data = np.arange(4 * 3, dtype="float32").reshape(3, 4)
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        raw = mf.read()

    vt = VirtualTile(cellid=1, raster=raw)
    assert ot._tile_to_bytes(vt) == raw


def test_virtual_pixels_identical_to_materialize(tmp_path):
    """Pixels from _tile_to_bytes match materialize_to_bytes for a plain virtual tile."""
    path = _write_src(tmp_path, "plain.tif")
    vt = VirtualTile(cellid=0, path=path, window=WINDOW)

    ref = ot.materialize_to_bytes(vt).raster
    opt = ot._tile_to_bytes(vt)

    assert opt is not None
    assert np.array_equal(_pixels(ref), _pixels(opt))


def test_virtual_georeference_identical_to_materialize(tmp_path):
    """CRS + transform + size from _tile_to_bytes match materialize_to_bytes."""
    path = _write_src(tmp_path, "plain.tif")
    vt = VirtualTile(cellid=0, path=path, window=WINDOW)

    ref_crs, ref_tr, ref_w, ref_h = _meta(ot.materialize_to_bytes(vt).raster)
    opt_crs, opt_tr, opt_w, opt_h = _meta(ot._tile_to_bytes(vt))

    assert ref_crs == opt_crs
    assert ref_tr == opt_tr
    assert ref_w == opt_w
    assert ref_h == opt_h


def test_virtual_pending_nodata_pixels_identical(tmp_path):
    """Pending nodata instruction is applied the same in both paths."""
    # Source has no nodata declared; pending_nodata should be applied.
    path = _write_src(tmp_path, "nonodata.tif", declare_nodata=False)
    vt = VirtualTile(
        cellid=0,
        path=path,
        window=WINDOW,
        metadata={PENDING_NODATA: "-9999"},
    )

    ref = ot.materialize_to_bytes(vt).raster
    opt = ot._tile_to_bytes(vt)

    assert opt is not None
    assert np.array_equal(_pixels(ref), _pixels(opt))


def test_virtual_clip_pixels_identical(tmp_path):
    """Clip path returns pixels identical to materialize_to_bytes."""
    path = _write_src(tmp_path, "clip.tif")

    # Compute a geographic clip box covering the left half of the window.
    transform = from_origin(10.0, 50.0, 0.01, 0.01)
    c, r, w, h = WINDOW
    # Pixel origin of the window in geographic space
    win_ulx = transform.c + c * transform.a
    win_uly = transform.f + r * transform.e  # negative step so uly < 50
    minx = win_ulx
    maxy = win_uly
    midx = win_ulx + (w // 2) * abs(transform.a)
    miny = win_uly + h * transform.e  # transform.e is negative

    poly = box(minx, miny, midx, maxy)
    clip_wkb = shapely.wkb.dumps(poly)

    vt = VirtualTile(
        cellid=0,
        path=path,
        window=WINDOW,
        clip_polygon=clip_wkb,
        clip_crs="EPSG:4326",
    )

    ref = ot.materialize_to_bytes(vt).raster
    opt = ot._tile_to_bytes(vt)

    assert opt is not None
    ref_pix = _pixels(ref)
    opt_pix = _pixels(opt)
    # Shape and pixel values must match.
    assert (
        ref_pix.shape == opt_pix.shape
    ), f"clip shape mismatch: ref={ref_pix.shape} opt={opt_pix.shape}"
    assert np.array_equal(ref_pix, opt_pix)


def test_mapalgebra_bytes_virtual_matches_materialized(tmp_path):
    """_mapalgebra_bytes with virtual inputs gives the same pixel result as with
    materialized inputs — regression guard for the _tile_to_bytes optimization."""
    from databricks.labs.gbx.pyrx.functions import _mapalgebra_bytes

    path_a = _write_src(tmp_path, "a.tif")
    path_b = str(tmp_path / "b.tif")

    # b values = a values * 2
    with rasterio.open(path_a) as ds:
        data_a = ds.read(1)
        prof = ds.profile.copy()
    with rasterio.open(path_b, "w", **prof) as ds:
        ds.write((data_a * 2).astype("float32"), 1)

    # Build both virtual and materialized tile inputs (full extent).
    vt_a = VirtualTile(cellid=0, path=path_a, window=(0, 0, W, H))
    vt_b = VirtualTile(cellid=0, path=path_b, window=(0, 0, W, H))

    mat_a = ot.materialize_to_bytes(vt_a).raster
    mat_b = ot.materialize_to_bytes(vt_b).raster
    mat_ta = VirtualTile(cellid=0, raster=mat_a)
    mat_tb = VirtualTile(cellid=0, raster=mat_b)

    out_virtual = _mapalgebra_bytes([vt_a.to_row(), vt_b.to_row()], "A + B")
    out_mat = _mapalgebra_bytes([mat_ta.to_row(), mat_tb.to_row()], "A + B")

    assert out_virtual is not None
    assert out_mat is not None
    with _serde.open_tile(out_virtual) as ov, _serde.open_tile(out_mat) as om:
        assert np.allclose(
            ov.read(1), om.read(1), atol=1e-5
        ), "mapalgebra_bytes virtual != materialized"
