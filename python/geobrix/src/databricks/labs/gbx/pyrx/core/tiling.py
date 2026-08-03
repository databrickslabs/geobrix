"""Spark-free tiling ops. Each returns a list of GTiff byte strings (one per
output tile); the Spark layer wraps each into a tile struct."""

import math

import numpy as np
from rasterio.io import MemoryFile
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import compression as _comp


def _write(profile, data) -> bytes:
    dtype = profile.get("dtype", str(np.asarray(data).dtype))
    decoded_bytes = data.nbytes if hasattr(data, "nbytes") else None
    profile.update(
        _comp.creation_opts(dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def iter_separate_bands(ds):
    """Yield one single-band GTiff byte string per band (streaming)."""
    for i in range(1, ds.count + 1):
        profile = ds.profile.copy()
        profile.update(driver="GTiff", count=1)
        yield _write(profile, ds.read(i)[np.newaxis, :, :])


def separate_bands(ds) -> list:
    return list(iter_separate_bands(ds))


def _iter_window_tiles(ds, tile_width, tile_height, step_x, step_y):
    """Yield one GTiff byte string per window tile (streaming, never buffered)."""
    tw, th = int(tile_width), int(tile_height)
    row = 0
    while row < ds.height:
        col = 0
        while col < ds.width:
            w = min(tw, ds.width - col)
            h = min(th, ds.height - row)
            if w > 0 and h > 0:
                win = Window(col, row, w, h)
                data = ds.read(window=win)
                profile = ds.profile.copy()
                profile.update(
                    driver="GTiff",
                    width=w,
                    height=h,
                    transform=ds.window_transform(win),
                )
                yield _write(profile, data)
            col += step_x
        row += step_y


def _window_tiles(ds, tile_width, tile_height, step_x, step_y) -> list:
    return list(_iter_window_tiles(ds, tile_width, tile_height, step_x, step_y))


def iter_retile(ds, tile_width, tile_height):
    tw, th = int(tile_width), int(tile_height)
    return _iter_window_tiles(ds, tw, th, tw, th)


def retile(ds, tile_width, tile_height) -> list:
    return list(iter_retile(ds, tile_width, tile_height))


def _overlap_steps(tile_width, tile_height, overlap):
    # ``overlap`` is a percentage of tile size, matching heavy
    # OverlappingTiles.generateWindows: overlap_px = ceil(tile_dim * overlap / 100),
    # step = tile_dim - overlap_px. (Heavy is the v0.3.0-released contract.)
    tw, th, ov = int(tile_width), int(tile_height), int(overlap)
    overlap_w = math.ceil(tw * ov / 100.0)
    overlap_h = math.ceil(th * ov / 100.0)
    return tw, th, max(1, tw - overlap_w), max(1, th - overlap_h)


def plan_grid_windows(width, height, tile_width, tile_height, overlap=0):
    """Enumerate a regular grid of (col_off, row_off, w, h) windows over
    ``width x height``, stepping by the overlap-adjusted stride and clamping
    each window to the extent. Pure window planning -- no dataset, no bytes.
    Overlap semantics match ``_iter_window_tiles`` / ``rst_tooverlappingtiles``.
    """
    tw, th, step_x, step_y = _overlap_steps(tile_width, tile_height, overlap)
    windows = []
    row = 0
    while row < height:
        col = 0
        while col < width:
            w = min(tw, width - col)
            h = min(th, height - row)
            if w > 0 and h > 0:
                windows.append((col, row, w, h))
            col += step_x
        row += step_y
    return windows


def iter_to_overlapping_tiles(ds, tile_width, tile_height, overlap):
    tw, th, sx, sy = _overlap_steps(tile_width, tile_height, overlap)
    return _iter_window_tiles(ds, tw, th, sx, sy)


def to_overlapping_tiles(ds, tile_width, tile_height, overlap) -> list:
    return list(iter_to_overlapping_tiles(ds, tile_width, tile_height, overlap))


def _get_tile_size(width, height, size_bytes, size_in_mb):
    """Power-of-4 tile dimensions, ported from heavy BalancedSubdivision.getTileSize.

    Finds the smallest number of quad-split rounds ``k`` such that the per-tile
    byte size ``size_bytes >> (2*k)`` no longer exceeds the MB limit, capped so
    the split count ``4**(k+1)`` stays within 512.  The raster is then split
    into a ``2**k x 2**k`` grid via ceil-div tile dimensions.

    ``size_bytes`` is the *encoded* raster byte length (heavy keys on GDAL's
    in-memory file size, i.e. the serialized GTiff buffer length), NOT the raw
    width*height*bands*itemsize pixel-array size.

    ``size_in_mb <= 0`` means *no split*: the whole-image dimensions are
    returned so the caller emits one tile per file (the reader default). Only a
    positive MB value opts into tiling.
    """
    if int(size_in_mb) <= 0:
        return width, height
    limit = int(size_in_mb) * 1024 * 1024
    k = 0
    while k < 9 and (size_bytes >> (2 * k)) > limit and (1 << (2 * (k + 1))) <= 512:
        k += 1
    nx = ny = 1 << k
    tile_x = (width + nx - 1) // nx  # ceil-div
    tile_y = (height + ny - 1) // ny
    return tile_x, tile_y


def _encoded_size_bytes(ds) -> int:
    """Serialized GTiff byte length of ``ds`` -- the analog of heavy's memSize.

    Used when the caller did not supply ``size_bytes``; re-encodes the open
    dataset to an in-memory GTiff and measures the buffer length, matching the
    vsimem buffer size heavy reads via GetMemFileBuffer.

    # EXEMPT: measurement-only encode (size probe for tiling-budget decisions),
    # no compression policy. The result is used only to count bytes, not stored
    # or returned to callers. Compression here would only change the estimate
    # vs the uncompressed-size contract heavy uses.
    """
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(ds.read())
        return len(mf.read())


def iter_make_tiles(ds, size_in_mb, size_bytes=None):
    """Streaming variant of :func:`make_tiles`; yields one GTiff byte string per tile."""
    size_in_mb = int(size_in_mb)  # heavy casts sizeInMB to Int (truncates)
    if size_in_mb <= 0:
        return iter_retile(ds, ds.width, ds.height)
    if size_bytes is None:
        size_bytes = _encoded_size_bytes(ds)
    tile_x, tile_y = _get_tile_size(ds.width, ds.height, size_bytes, size_in_mb)
    return iter_retile(ds, tile_x, tile_y)


def make_tiles(ds, size_in_mb, size_bytes=None) -> list:
    """Split a raster into a power-of-4 grid of tiles, matching heavy rst_maketiles.

    Aligned to heavy BalancedSubdivision: keyed on the encoded raster byte size
    versus the MB limit, the raster is quad-split ``k`` times into a
    ``2**k x 2**k`` grid (so 1, 4, 16, 64, ... tiles).  Returns one tile when the
    full raster already fits the budget.

    ``size_in_mb`` is truncated to an integer to mirror heavy, whose Catalyst
    cast to Int drops the fraction (so e.g. 0.7 -> 0 -> a single tile).

    ``size_bytes`` is the encoded raster byte length; callers that already hold
    the raster bytes (the Spark UDF) should pass it so the split count matches
    heavy exactly.  When omitted it is derived by re-encoding ``ds`` to GTiff.
    """
    size_in_mb = int(size_in_mb)  # heavy casts sizeInMB to Int (truncates)
    if size_in_mb <= 0:
        return retile(ds, ds.width, ds.height)
    if size_bytes is None:
        size_bytes = _encoded_size_bytes(ds)
    tile_x, tile_y = _get_tile_size(ds.width, ds.height, size_bytes, size_in_mb)
    return retile(ds, tile_x, tile_y)
