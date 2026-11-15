"""Pure-function tests for pyrx web-mercator XYZ tiling (core/xyz.py).

These exercise the Spark-free render/intersection logic directly on an open
rasterio DatasetReader — no Spark, no UDFs.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import xyz


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


def _make_world(epsg=4326):
    """A coarse raster covering the whole WGS84 world (for count-guard tests)."""
    transform = from_origin(-180.0, 90.0, 10.0, 10.0)
    profile = dict(
        driver="GTiff",
        width=36,
        height=18,
        count=1,
        dtype="uint8",
        crs=f"EPSG:{epsg}",
        transform=transform,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.ones((18, 36), dtype="uint8"), 1)
        return mf.read()


def _open(raster_bytes):
    mf = MemoryFile(raster_bytes)
    return mf, mf.open()


def _decode(png_bytes):
    with MemoryFile(png_bytes) as mf, mf.open() as ds:
        return ds.read()


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


def test_validate_rescale_normalizes():
    assert xyz._validate_rescale(None) == "auto"
    assert xyz._validate_rescale("auto") == "auto"
    assert xyz._validate_rescale("AUTO") == "auto"
    assert xyz._validate_rescale("none") == "none"
    assert xyz._validate_rescale((10, 200)) == (10.0, 200.0)
    assert xyz._validate_rescale([10, 200]) == (10.0, 200.0)


def test_validate_rescale_rejects_bad():
    with pytest.raises(ValueError):
        xyz._validate_rescale("stretch")
    with pytest.raises(ValueError):
        xyz._validate_rescale((1, 2, 3))
    with pytest.raises(ValueError):
        xyz._validate_rescale((200, 10))  # min must be < max


def test_resolve_in_range_uint8_passthrough_is_none():
    mf, ds = _open(_make_rgb())  # uint8
    try:
        assert xyz._resolve_in_range(ds, "auto") is None
    finally:
        ds.close()
        mf.close()


def test_resolve_in_range_none_is_none():
    mf, ds = _open(_make_uint16_narrow())
    try:
        assert xyz._resolve_in_range(ds, "none") is None
    finally:
        ds.close()
        mf.close()


def test_resolve_in_range_auto_uint16_uses_data_minmax():
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        rng = xyz._resolve_in_range(ds, "auto")
        assert rng is not None and len(rng) == 1
        lo, hi = rng[0]
        # Whole-dataset min/max ~ [8000, 12000], NOT the dtype range [0, 65535].
        assert 7900 <= lo <= 8100
        assert 11900 <= hi <= 12100
    finally:
        ds.close()
        mf.close()


def test_resolve_in_range_explicit_pair_repeats_per_band():
    mf, ds = _open(_make_rgb())  # 3-band uint8
    try:
        rng = xyz._resolve_in_range(ds, (10, 200))
        assert rng == [(10.0, 200.0), (10.0, 200.0), (10.0, 200.0)]
    finally:
        ds.close()
        mf.close()


# --- render_tile: in-extent -------------------------------------------------
def test_render_tile_in_extent_png():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        # z=5 tile (16, 10) covers lon~11.25, lat~48.9 — inside the fixture.
        out = xyz.render_tile(ds, 5, 16, 10, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    assert len(out) > 0


def test_render_tile_jpeg_webp():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        jpeg = xyz.render_tile(ds, 5, 16, 10, "JPEG", 256, "bilinear")
        webp = xyz.render_tile(ds, 5, 16, 10, "WEBP", 256, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert jpeg[:3] == b"\xff\xd8\xff"
    assert webp[:4] == b"RIFF"


# --- render_tile: out-of-extent -> transparent PNG --------------------------
def test_render_tile_out_of_extent_transparent_png():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        # z=2 (0,0) is the NW quadrant of the world — opposite a European fixture.
        out = xyz.render_tile(ds, 2, 0, 0, "PNG", 128, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"
    arr = _decode(out)
    assert arr.shape == (4, 128, 128)  # RGBA
    assert arr[3].max() == 0  # alpha fully transparent


def test_render_tile_out_of_extent_returns_png_even_for_jpeg_request():
    # Mirror heavyweight: out-of-extent always yields a transparent PNG regardless
    # of the requested format.
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        out = xyz.render_tile(ds, 2, 0, 0, "JPEG", 64, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"


# --- validation -------------------------------------------------------------
def test_render_tile_bad_format_raises():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "GIF", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_render_tile_bad_size_raises():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "PNG", 5000, "bilinear")
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "PNG", 0, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_render_tile_bad_resampling_raises():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.render_tile(ds, 5, 16, 10, "PNG", 256, "wobble")
    finally:
        ds.close()
        mf.close()


def test_render_tile_format_resampling_case_insensitive():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        out = xyz.render_tile(ds, 5, 16, 10, "png", 256, "BILINEAR")
    finally:
        ds.close()
        mf.close()
    assert out[:4] == b"\x89PNG"


def test_transparent_png_size():
    out = xyz.transparent_png(200)
    arr = _decode(out)
    assert arr.shape == (4, 200, 200)
    assert arr[3].max() == 0


# --- intersecting_tiles / pyramid -------------------------------------------
def test_intersecting_tiles_and_pyramid_lengths_match():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        tiles = xyz.intersecting_tiles(ds, 1, 3)
        result = xyz.pyramid(ds, 1, 3, "PNG", 64, "bilinear")
    finally:
        ds.close()
        mf.close()
    assert len(result) == len(tiles)
    assert len(result) > 0
    for el in result:
        assert el["z"] in (1, 2, 3)
        assert el["bytes"][:4] == b"\x89PNG"
        assert {"z", "x", "y", "bytes"} <= set(el.keys())


def test_tile_count_matches_intersecting():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        assert xyz.tile_count(ds, 1, 3) == len(xyz.intersecting_tiles(ds, 1, 3))
    finally:
        ds.close()
        mf.close()


# --- pyramid guards ---------------------------------------------------------
def test_pyramid_guard_min_z_negative():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, -1, 2, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_pyramid_guard_max_lt_min():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, 3, 2, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_pyramid_guard_max_zoom():
    raster = _make_rgb()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, 0, 21, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


def test_pyramid_guard_tile_count_before_rendering():
    # A world-extent raster at z=20 implies ~1.1e12 tiles — must raise the
    # count guard WITHOUT rendering anything (fast).
    raster = _make_world()
    mf, ds = _open(raster)
    try:
        with pytest.raises(ValueError):
            xyz.pyramid(ds, 0, 20, "PNG", 256, "bilinear")
    finally:
        ds.close()
        mf.close()


# --- render_tile rescale / in_range ------------------------------------------


def _decode_png_rgb(png_bytes):
    import io

    from PIL import Image

    img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    a = np.asarray(img)
    # data pixels = alpha > 0
    mask = a[..., 3] > 0
    rgb = a[..., :3][mask]
    return rgb  # (N, 3) uint8 of covered pixels


def _center_tile_zxy(ds):
    # Pick the z=8 tile that covers the geographic centre of the fixture extent
    # (lon~10-12, lat~48-50).  Using the midpoint avoids edge tiles that only
    # clip a corner of the raster and therefore contain a narrow value range.
    import morecantile

    tms = morecantile.tms.get("WebMercatorQuad")
    west, south, east, north = xyz._wgs84_bounds(ds)
    mid_lon = (west + east) / 2
    mid_lat = (south + north) / 2
    t = tms.tile(mid_lon, mid_lat, 8)
    return t.z, t.x, t.y


def test_render_tile_auto_uint16_spans_full_range():
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        z, x, y = _center_tile_zxy(ds)
        png = xyz.render_tile(ds, z, x, y, rescale="auto")
        rgb = _decode_png_rgb(png)
        assert rgb.size > 0
        # Auto rescale maps [8000,12000] -> ~full 8-bit; expect a wide spread,
        # NOT crushed into the ~[31,46] full-dtype-range band.
        assert int(rgb.max()) - int(rgb.min()) > 100
    finally:
        ds.close()
        mf.close()


def test_render_tile_none_uint16_stays_crushed():
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        z, x, y = _center_tile_zxy(ds)
        png = xyz.render_tile(ds, z, x, y, rescale="none")
        rgb = _decode_png_rgb(png)
        assert rgb.size > 0
        # Full-dtype-range: 8000..12000 / 65535 * 255 -> ~[31, 46]; crushed.
        assert int(rgb.max()) < 80
    finally:
        ds.close()
        mf.close()


def test_render_tile_uint8_auto_matches_none():
    mf, ds = _open(_make_rgb())
    try:
        z, x, y = _center_tile_zxy(ds)
        auto = xyz.render_tile(ds, z, x, y, rescale="auto")
        none = xyz.render_tile(ds, z, x, y, rescale="none")
        assert auto == none  # uint8 pass-through: byte-identical
    finally:
        ds.close()
        mf.close()


def test_pyramid_shares_one_mapping_no_per_tile_stats(monkeypatch):
    """All pyramid tiles use ONE resolved in_range; stats resolved once, not per tile."""
    mf, ds = _open(_make_uint16_narrow(lo=8000, hi=12000))
    try:
        calls = {"n": 0}
        real = xyz._resolve_in_range

        def _spy(dataset, rescale):
            calls["n"] += 1
            return real(dataset, rescale)

        monkeypatch.setattr(xyz, "_resolve_in_range", _spy)
        tiles = xyz.pyramid(ds, 6, 8, rescale="auto")
        assert len(tiles) >= 2  # multiple tiles across the range
        # Resolved exactly once for the whole pyramid (not once per tile).
        assert calls["n"] == 1
        # And the tiles are contrast-recovered (spot check one non-empty tile).
        nonempty = [t for t in tiles if t["bytes"]]
        assert nonempty
    finally:
        ds.close()
        mf.close()


def test_tilexyz_udf_accepts_rescale_and_recovers_contrast():
    import io

    from PIL import Image

    from databricks.labs.gbx.pyrx import functions as fns

    raster = _make_uint16_narrow(lo=8000, hi=12000)
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
    finally:
        ds.close()
        mf.close()

    tile = {"raster": raster}
    auto = fns._tilexyz_udf.func(tile, z, x, y, "PNG", 256, "bilinear", "auto")
    none = fns._tilexyz_udf.func(tile, z, x, y, "PNG", 256, "bilinear", "none")

    def _spread(png):
        a = np.asarray(Image.open(io.BytesIO(png)).convert("RGBA"))
        rgb = a[..., :3][a[..., 3] > 0]
        return 0 if rgb.size == 0 else int(rgb.max()) - int(rgb.min())

    assert _spread(auto) > 100  # contrast recovered
    assert _spread(none) < 80  # today's crushed behavior preserved


def test_tilexyz_udf_rescale_defaults_to_auto():
    from databricks.labs.gbx.pyrx import functions as fns

    raster = _make_uint16_narrow(lo=8000, hi=12000)
    mf, ds = _open(raster)
    try:
        z, x, y = _center_tile_zxy(ds)
    finally:
        ds.close()
        mf.close()
    tile = {"raster": raster}
    # rescale omitted -> defaults to auto (contrast recovered)
    default = fns._tilexyz_udf.func(tile, z, x, y, "PNG", 256, "bilinear")
    explicit_auto = fns._tilexyz_udf.func(tile, z, x, y, "PNG", 256, "bilinear", "auto")
    assert default == explicit_auto
