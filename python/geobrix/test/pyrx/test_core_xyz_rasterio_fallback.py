"""Tests for the rasterio-warp fallback backend in pyrx/core/xyz.py.

The dev environment HAS rio-tiler, so tests force the fallback in two ways:
1. Call the private backend functions directly (_render_tile_rasterio,
   _transparent_png_rasterio).
2. Monkeypatch _riotiler_available to False and exercise the public API
   (render_tile, transparent_png) to confirm correct dispatch.

Coverage mirrors test_core_xyz.py so both backends meet the same contract.
"""

import io

import numpy as np
from PIL import Image
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import xyz

# --- shared fixtures (copied from test_core_xyz.py) -------------------------


def _make_rgb(width=64, height=64, epsg=4326, ulx=10.0, uly=50.0, res=0.03125):
    """A small RGB GTiff over a European extent (default ~lon 10..12, lat 48..50)."""
    transform = from_origin(ulx, uly, res, res)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=3,
        dtype="uint8",
        crs=f"EPSG:{epsg}",
        transform=transform,
    )
    data = (np.arange(width * height) % 256).astype("uint8").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, 4):
                ds.write(data, b)
        return mf.read()


def _make_uint16_narrow(width=64, height=64, epsg=4326, lo=8000, hi=12000):
    """Single-band uint16 raster with values spread across [lo, hi] (narrow band)."""
    transform = from_origin(10.0, 50.0, 0.03125, 0.03125)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="uint16",
        crs=f"EPSG:{epsg}",
        transform=transform,
    )
    ramp = np.linspace(lo, hi, width * height).astype("uint16").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(ramp, 1)
        return mf.read()


def _open(raster_bytes):
    mf = MemoryFile(raster_bytes)
    return mf, mf.open()


def _decode(png_bytes):
    """Decode image bytes to a (bands, height, width) numpy array via rasterio."""
    with MemoryFile(png_bytes) as mf, mf.open() as ds:
        return ds.read()


def _center_tile_zxy(ds):
    """z=8 tile covering the geographic centre of the fixture (~lon 10-12, lat 48-50)."""
    import morecantile

    tms = morecantile.tms.get("WebMercatorQuad")
    west, south, east, north = xyz._wgs84_bounds(ds)
    mid_lon = (west + east) / 2
    mid_lat = (south + north) / 2
    t = tms.tile(mid_lon, mid_lat, 8)
    return t.z, t.x, t.y


def _covered_pixels(png_bytes):
    """Return (N, 3) uint8 array of RGB values where alpha > 0."""
    img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    a = np.asarray(img)
    mask = a[..., 3] > 0
    return a[..., :3][mask]


def _spread(png_bytes):
    """max - min over covered pixels (0 if none)."""
    rgb = _covered_pixels(png_bytes)
    return 0 if rgb.size == 0 else int(rgb.max()) - int(rgb.min())


# --- _transparent_png_rasterio ----------------------------------------------


def test_transparent_png_rasterio_shape_and_alpha():
    out = xyz._transparent_png_rasterio(200)
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr.shape == (4, 200, 200)
    assert arr[3].max() == 0  # alpha fully transparent


def test_transparent_png_rasterio_default_size():
    out = xyz._transparent_png_rasterio(128)
    arr = _decode(out)
    assert arr.shape == (4, 128, 128)


# --- _render_tile_rasterio: in-extent ---------------------------------------


def test_render_tile_rasterio_in_extent_png():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
        out = xyz._render_tile_rasterio(ds, z, x, y, "PNG", 256, "bilinear", None)
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr.shape == (4, 256, 256)  # RGBA


def test_render_tile_rasterio_in_extent_jpeg():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
        out = xyz._render_tile_rasterio(ds, z, x, y, "JPEG", 256, "bilinear", None)
    finally:
        ds.close()
        mf.close()
    assert out[:3] == b"\xff\xd8\xff"


def test_render_tile_rasterio_in_extent_webp():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
        out = xyz._render_tile_rasterio(ds, z, x, y, "WEBP", 256, "bilinear", None)
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"RIFF"


# --- _render_tile_rasterio: out-of-extent -----------------------------------


def test_render_tile_rasterio_out_of_extent_transparent_png():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        # z=2 (0,0) is the NW world quadrant — opposite the European fixture.
        out = xyz._render_tile_rasterio(ds, 2, 0, 0, "PNG", 128, "bilinear", None)
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr.shape == (4, 128, 128)
    assert arr[3].max() == 0  # fully transparent


def test_render_tile_rasterio_out_of_extent_returns_png_for_jpeg_request():
    """Out-of-extent always returns a transparent PNG, even for a JPEG request."""
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        out = xyz._render_tile_rasterio(ds, 2, 0, 0, "JPEG", 64, "bilinear", None)
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr[3].max() == 0


# --- _render_tile_rasterio: rescale parity ----------------------------------


def test_render_tile_rasterio_uint16_auto_spans_full_range():
    """auto: [8000,12000] → full 8-bit spread (contrast recovered, spread > 100)."""
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        z, x, y = _center_tile_zxy(ds)
        in_range = xyz._resolve_in_range(ds, "auto")
        png = xyz._render_tile_rasterio(ds, z, x, y, "PNG", 256, "bilinear", in_range)
    finally:
        ds.close()
        mf.close()
    assert _spread(png) > 100


def test_render_tile_rasterio_uint16_none_stays_crushed():
    """none (in_range=None): full-dtype-range map keeps values crushed (max < 80)."""
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        z, x, y = _center_tile_zxy(ds)
        # rescale="none" → _resolve_in_range returns None → _to_uint8 maps [0,65535]
        png = xyz._render_tile_rasterio(ds, z, x, y, "PNG", 256, "bilinear", None)
    finally:
        ds.close()
        mf.close()
    assert _spread(png) < 80


def test_render_tile_rasterio_uint8_auto_equals_none():
    """uint8 source: auto and none both pass through unchanged → byte-identical."""
    mf, ds = _open(_make_rgb())
    try:
        z, x, y = _center_tile_zxy(ds)
        auto_in_range = xyz._resolve_in_range(ds, "auto")
        none_in_range = None
        auto = xyz._render_tile_rasterio(
            ds, z, x, y, "PNG", 256, "bilinear", auto_in_range
        )
        none = xyz._render_tile_rasterio(
            ds, z, x, y, "PNG", 256, "bilinear", none_in_range
        )
    finally:
        ds.close()
        mf.close()
    # Both should be identical (uint8 pass-through for both modes)
    assert auto == none


# --- dispatch (monkeypatch _riotiler_available = False) ---------------------


def test_dispatch_render_tile_routes_to_fallback(monkeypatch):
    """With rio-tiler unavailable, render_tile uses the rasterio fallback."""
    monkeypatch.setattr(xyz, "_riotiler_available", lambda: False)
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
        out = xyz.render_tile(ds, z, x, y, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr.shape == (4, 256, 256)


def test_dispatch_transparent_png_routes_to_fallback(monkeypatch):
    """With rio-tiler unavailable, transparent_png uses the rasterio fallback."""
    monkeypatch.setattr(xyz, "_riotiler_available", lambda: False)
    out = xyz.transparent_png(128)
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr.shape == (4, 128, 128)
    assert arr[3].max() == 0


# --- cross-backend sanity (optional) ----------------------------------------


def test_cross_backend_both_have_covered_pixels():
    """Both backends produce an in-extent tile with >0 covered pixels for uint8 RGB."""
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
        riotiler_png = xyz._render_tile_riotiler(
            ds, z, x, y, "PNG", 256, "bilinear", None
        )
        rasterio_png = xyz._render_tile_rasterio(
            ds, z, x, y, "PNG", 256, "bilinear", None
        )
    finally:
        ds.close()
        mf.close()
    # Both should decode to RGBA tiles
    rt_arr = _decode(riotiler_png)
    rs_arr = _decode(rasterio_png)
    assert rt_arr.shape == (4, 256, 256)
    assert rs_arr.shape == (4, 256, 256)
    # Both have covered (non-transparent) pixels
    assert rt_arr[3].max() > 0
    assert rs_arr[3].max() > 0
    # Byte outputs need not be identical across backends
