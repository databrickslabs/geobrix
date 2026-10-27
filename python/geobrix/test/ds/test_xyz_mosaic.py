import io
import os
import tempfile

import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

from databricks.labs.gbx.ds._xyz_mosaic import (
    enumerate_tiles,
    render_tile,
    source_bounds_union,
    to_render_rgb,
)


def _cog_bytes(w, s, e, n, px=128, val=200):
    """A uint8 RGB EPSG:4326 raster filling [w,s,e,n] with a constant value."""
    data = np.full((3, px, px), val, dtype="uint8")
    profile = dict(
        driver="GTiff",
        width=px,
        height=px,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(w, s, e, n, px, px),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return mf.read()


def _tmp(b):
    """Write bytes to a temp file on disk and return the path."""
    fd, p = tempfile.mkstemp(suffix=".tif")
    os.write(fd, b)
    os.close(fd)
    return p


def test_enumerate_tiles_covers_bbox():
    tiles = enumerate_tiles((-122.52, 37.70, -122.35, 37.83), 12, 13)
    zs = {z for z, x, y in tiles}
    assert zs == {12, 13}
    assert all(isinstance(v, int) for t in tiles for v in t)
    assert len(tiles) >= 4  # multiple tiles across the AOI


def test_source_bounds_union():
    paths = []
    for bb in (_cog_bytes(10.0, 50.0, 11.0, 51.0), _cog_bytes(11.0, 49.0, 12.0, 51.0)):
        paths.append(_tmp(bb))
    try:
        u = source_bounds_union(paths)
        assert u == pytest.approx((10.0, 49.0, 12.0, 51.0), abs=1e-9)
    finally:
        for p in paths:
            os.unlink(p)


def test_render_tile_composites_all_covering_sources():
    # Two adjacent quads; a tile spanning the seam must composite BOTH (the cluster bug).
    left_p = _tmp(_cog_bytes(-122.50, 37.74, -122.45, 37.79, val=120))
    right_p = _tmp(_cog_bytes(-122.45, 37.74, -122.40, 37.79, val=220))
    try:
        import morecantile

        tms = morecantile.tms.get("WebMercatorQuad")
        # a high zoom tile straddling the seam lon=-122.45, lat~37.766
        # Use a tight bbox centred on the seam so the first returned tile crosses it.
        t = next(iter(tms.tiles(-122.451, 37.765, -122.449, 37.768, [16])))
        png = render_tile(t.z, t.x, t.y, [left_p, right_p])
        assert png is not None
        arr = np.asarray(Image.open(io.BytesIO(png)).convert("RGBA"))
        assert float(np.mean(arr[:, :, 3] == 255)) > 0.99  # fully covered, no seam gap
        # both source values appear (left ~120, right ~220) -> composited from both
        lo = float(np.mean((arr[:, :, 0] > 90) & (arr[:, :, 0] < 150)))
        hi = float(np.mean(arr[:, :, 0] > 190))
        assert lo > 0 and hi > 0
    finally:
        os.unlink(left_p)
        os.unlink(right_p)


def test_render_tile_none_when_no_source_covers():
    only_p = _tmp(_cog_bytes(-122.50, 37.74, -122.45, 37.79))
    try:
        import morecantile

        tms = morecantile.tms.get("WebMercatorQuad")
        far = next(iter(tms.tiles(10.0, 50.0, 10.1, 50.1, [16])))  # far away
        assert render_tile(far.z, far.x, far.y, [only_p]) is None
    finally:
        os.unlink(only_p)


def _rgba_tmp(w, s, e, n, px=512, rgb=200, alpha=0):
    """A 4-band RGBA EPSG:4326 raster (NAIP-like) with a constant RGB and alpha band."""
    from rasterio.enums import ColorInterp

    data = np.full((4, px, px), rgb, dtype="uint8")
    data[3] = alpha
    fd, p = tempfile.mkstemp(suffix=".tif")
    os.close(fd)
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        width=px,
        height=px,
        count=4,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(w, s, e, n, px, px),
    ) as ds:
        ds.write(data)
        ds.colorinterp = [
            ColorInterp.red,
            ColorInterp.green,
            ColorInterp.blue,
            ColorInterp.alpha,
        ]
    return p


def test_to_render_rgb_strips_alpha_so_tile_is_opaque():
    # NAIP-style 4-band RGBA whose alpha reads 0 (the staged-quad pathology): rendering
    # it directly masks the valid RGB to transparent; to_render_rgb -> RGB fixes it.
    rgba = _rgba_tmp(-122.50, 37.74, -122.40, 37.80, rgb=200, alpha=0)
    try:
        import morecantile

        tms = morecantile.tms.get("WebMercatorQuad")
        t = next(iter(tms.tiles(-122.47, 37.76, -122.46, 37.77, [15])))  # interior tile

        direct = render_tile(t.z, t.x, t.y, [rgba])
        assert direct is not None
        a0 = np.asarray(Image.open(io.BytesIO(direct)).convert("RGBA"))
        assert float(np.mean(a0[:, :, 3] == 255)) < 0.01  # alpha=0 masks it (the bug)

        rgb_path = to_render_rgb(rgba)
        assert rgb_path != rgba  # a new RGB sibling was written
        fixed = render_tile(t.z, t.x, t.y, [rgb_path])
        a1 = np.asarray(Image.open(io.BytesIO(fixed)).convert("RGBA"))
        assert float(np.mean(a1[:, :, 3] == 255)) > 0.99  # fully opaque after strip
    finally:
        os.unlink(rgba)


def test_to_render_rgb_passthrough_for_three_band():
    # A 3-band RGB raster has no alpha to strip -> returned unchanged.
    rgb = _tmp(_cog_bytes(-122.50, 37.74, -122.45, 37.79))
    try:
        assert to_render_rgb(rgb) == rgb
    finally:
        os.unlink(rgb)


def test_to_render_rgb_keeps_the_sources_own_mask_when_it_strips_the_alpha_band():
    """The alpha band is dropped on purpose (its zeros mask valid imagery), but a source's own
    (per-dataset) mask band is real: the RGB sibling carries it, so masked rows stay invalid
    while the rows the alpha band called transparent are rendered."""
    from ..pyrx import _alpha_layouts as al

    masked, transparent = (3, 4), (8,)
    raw = al.row_coded(
        "rgba", height=12, masked_rows=masked, transparent_rows=transparent
    )
    src = _tmp(raw)
    out = None
    try:
        out = to_render_rgb(src)
        assert out != src
        with rasterio.open(out) as ds:
            assert ds.count == 3
            valid = ds.read_masks(1)[:, 0] != 0
        assert valid.tolist() == [r not in masked for r in range(12)]
    finally:
        os.unlink(src)
        if out and out != src and os.path.exists(out):
            os.unlink(out)
