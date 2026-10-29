"""Spark-free tiling ops. Each returns a list of GTiff byte strings (one per
output tile); the Spark layer wraps each into a tile struct."""

import math

import numpy as np
from rasterio.io import MemoryFile
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import compression as _comp


def tile_range(
    coord_m: float, origin_m: float, radius_m: float, tile_m: float
) -> tuple[int, int]:
    """Return the inclusive ``(lo, hi)`` tile indices whose extents overlap
    ``[coord_m - radius_m, coord_m + radius_m]``.

    Tile ``i`` covers ``[origin_m + i*tile_m, origin_m + (i+1)*tile_m)``.  The
    returned pair is the smallest closed index range such that every point in the
    window is contained in at least one tile.

    Args:
        coord_m:   Centre of the analysis window in projected metres.
        origin_m:  Tile-grid origin in the same coordinate system.
        radius_m:  Half-width of the analysis window (metres).
        tile_m:    Tile width in metres (must be > 0).

    Returns:
        ``(lo, hi)`` — inclusive tile index range (both ends included).
    """
    lo = math.floor((coord_m - radius_m - origin_m) / tile_m)
    hi = math.floor((coord_m + radius_m - origin_m) / tile_m)
    return (lo, hi)


def iter_separate_bands_indexed(ds):
    """Yield ``(band, gtiff_bytes)`` per band (streaming); *band* is 1-based."""
    for i in range(1, ds.count + 1):
        profile = ds.profile.copy()
        profile.update(driver="GTiff", count=1)
        yield i, _comp.write_gtiff(profile, ds.read(i)[np.newaxis, :, :])


def iter_separate_bands(ds):
    """Yield one single-band GTiff byte string per band (streaming)."""
    for _, b in iter_separate_bands_indexed(ds):
        yield b


def separate_bands(ds) -> list:
    return list(iter_separate_bands(ds))


def require_window(tile_width, tile_height, fn="gbx_rst_retile"):
    """Raise unless the window size is 1 pixel or more in each direction (NULL, or NaN, is left to the caller).

    A window of 0 or less never advances through the raster. It is a PARAMETER error, so it raises with a
    Fix and is never an error tile; the JVM tier raises the same words.
    """
    for name, value in (("tile_width", tile_width), ("tile_height", tile_height)):
        # NULL (an Arrow UDTF hands an INT NULL over as NaN) is data, for the caller's error tile
        if value is None or value != value:
            continue
        if int(value) <= 0:
            raise ValueError(
                f"{fn}: {name} must be 1 or more pixels, got {int(value)}. "
                "Fix: pass a window of at least 1 pixel."
            )


def _iter_window_tiles(ds, tile_width, tile_height, step_x, step_y):
    """Yield one GTiff byte string per window tile (streaming, never buffered)."""
    for _, b in _iter_window_tiles_located(ds, tile_width, tile_height, step_x, step_y):
        yield b


def _iter_window_tiles_located(ds, tile_width, tile_height, step_x, step_y):
    """Yield ``((col_off, row_off, width, height), gtiff_bytes)`` per window tile.

    The window is the tile's pixel window inside *ds* (clamped at the raster
    edge), i.e. the provenance a tile struct records for where it was cut from.
    """
    tw, th = int(tile_width), int(tile_height)
    row = 0
    while row < ds.height:
        col = 0
        while col < ds.width:
            w = min(tw, ds.width - col)
            h = min(th, ds.height - row)
            if w > 0 and h > 0:
                yield (col, row, w, h), cut_window(ds, (col, row, w, h))
            col += step_x
        row += step_y


def cut_window(ds, win) -> bytes:
    """GTiff bytes of one pixel window of *ds*; *win* is ``(col_off, row_off, width, height)``.

    The window is the part of the raster that lies inside it (the caller clamps it to the raster
    edge); the output keeps the source's CRS, nodata, mask and band layout, positioned by the
    window's geotransform.
    """
    col, row, w, h = (int(v) for v in win)
    window = Window(col, row, w, h)
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        width=w,
        height=h,
        transform=ds.window_transform(window),
    )
    return _comp.write_gtiff(profile, ds.read(window=window), alpha_like=ds.colorinterp)


def _window_tiles(ds, tile_width, tile_height, step_x, step_y) -> list:
    return list(_iter_window_tiles(ds, tile_width, tile_height, step_x, step_y))


def iter_retile(ds, tile_width, tile_height):
    tw, th = int(tile_width), int(tile_height)
    return _iter_window_tiles(ds, tw, th, tw, th)


def iter_retile_located(ds, tile_width, tile_height):
    """Like :func:`iter_retile` but yields ``(window, gtiff_bytes)``."""
    tw, th = int(tile_width), int(tile_height)
    return _iter_window_tiles_located(ds, tw, th, tw, th)


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


def covers_whole_input(ds, tile_width, tile_height, overlap=0) -> bool:
    """True when a window grid over *ds* yields exactly ONE window and it is the
    whole raster, so the single output tile is the input itself. Pure arithmetic on
    the grid steps (no window list is built)."""
    tw, th, step_x, step_y = _overlap_steps(tile_width, tile_height, overlap)
    return (
        tw >= ds.width
        and th >= ds.height
        and step_x >= ds.width
        and step_y >= ds.height
    )


def iter_to_overlapping_tiles(ds, tile_width, tile_height, overlap):
    tw, th, sx, sy = _overlap_steps(tile_width, tile_height, overlap)
    return _iter_window_tiles(ds, tw, th, sx, sy)


def iter_to_overlapping_tiles_located(ds, tile_width, tile_height, overlap):
    """Like :func:`iter_to_overlapping_tiles` but yields ``(window, gtiff_bytes)``."""
    tw, th, sx, sy = _overlap_steps(tile_width, tile_height, overlap)
    return _iter_window_tiles_located(ds, tw, th, sx, sy)


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
    profile = _comp.source_profile(ds.profile, unless_jpeg=True)
    profile.update(driver="GTiff")
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(ds.read())
        return len(mf.read())


def make_tiles_size(ds, size_in_mb, size_bytes=None):
    """``(tile_width, tile_height)`` that :func:`make_tiles` cuts *ds* into."""
    size_in_mb = int(size_in_mb)  # heavy casts sizeInMB to Int (truncates)
    if size_in_mb <= 0:
        return ds.width, ds.height
    if size_bytes is None:
        size_bytes = _encoded_size_bytes(ds)
    return _get_tile_size(ds.width, ds.height, size_bytes, size_in_mb)


def iter_make_tiles_located(ds, size_in_mb, size_bytes=None):
    """Streaming variant of :func:`make_tiles`; yields ``(window, gtiff_bytes)``."""
    tile_x, tile_y = make_tiles_size(ds, size_in_mb, size_bytes)
    return iter_retile_located(ds, tile_x, tile_y)


def iter_make_tiles(ds, size_in_mb, size_bytes=None):
    """Streaming variant of :func:`make_tiles`; yields one GTiff byte string per tile."""
    for _, b in iter_make_tiles_located(ds, size_in_mb, size_bytes):
        yield b


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
