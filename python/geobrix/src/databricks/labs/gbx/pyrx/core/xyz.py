"""Spark-free web-mercator XYZ slippy-map tiling (rio-tiler + morecantile).

Mirrors the heavyweight rasterx ``RST_TileXYZ`` / ``RST_XYZPyramid`` semantics:

  * ``render_tile`` renders a single (z, x, y) web-mercator tile to PNG / JPEG /
    WEBP bytes. Out-of-extent or empty tiles return a transparent PNG (RGBA,
    alpha=0) of the requested size — NEVER null — because slippy-map servers
    need a 200-status non-empty body outside source coverage. On ANY hard
    failure we likewise return a transparent PNG.
  * ``pyramid`` enumerates every intersecting (z, x, y) tile across a zoom range
    and renders each, returning a list of ``{"z","x","y","bytes"}`` dicts.

Primary backend: rio-tiler (lazy import, checked at call time via
``_riotiler_available``).  Fallback backend: rasterio warp + GDAL CreateCopy
(always available; handles the Databricks Serverless env where rio-tiler 9.3.0
re-resolves and fails to import under py3.12).  Public API is identical for
both backends.
"""

import morecantile
import numpy as np
from rasterio.warp import transform_bounds

# NOTE: rio_tiler is imported LAZILY (inside _transparent_png_riotiler /
# _render_tile_riotiler), not at module top. rio-tiler 9.x defines
# TypedDict(extra_items=...) (PEP 728), which fails to import under Databricks
# Serverless %pip (its immutable constraints hold typing_extensions back).
# Keeping the import lazy means `import pyrx` — and every rst_* function that
# does NOT use XYZ tiling — works on Serverless; only rst_tilexyz /
# rst_xyzpyramid require a working backend at call time.  See pyproject
# [light-base] rio-tiler pin.

# --- constants (mirror heavyweight) -----------------------------------------
MAX_ZOOM = 20
MAX_TILE_COUNT = 1_000_000

ALLOWED_FORMATS = {"PNG", "JPEG", "WEBP"}

# Heavyweight gdalwarp resampling name -> rasterio.enums.Resampling name.
_RESAMPLING_MAP = {
    "near": "nearest",
    "bilinear": "bilinear",
    "cubic": "cubic",
    "cubicspline": "cubic_spline",
    "lanczos": "lanczos",
    "average": "average",
    "mode": "mode",
    "max": "max",
    "min": "min",
    "med": "med",
    "q1": "q1",
    "q3": "q3",
}

_TMS = morecantile.tms.get("WebMercatorQuad")

# Sentinel: iter_pyramid uses this to distinguish "no rescale by design (mode=none)"
# from "in_range not yet computed (None)". render_tile maps it back to None so that
# no in_range parameter is passed to img.post_process / _to_uint8.
_NO_RESCALE = object()

# --- backend availability probe ---------------------------------------------

_RIOTILER_OK = None  # module-level cache; None = not yet checked


def _riotiler_available() -> bool:
    """True if rio-tiler imports cleanly (primary backend). Cached. Any import
    failure — ImportError OR the 9.3.0 py3.12 TypedDict TypeError — selects the
    rasterio fallback."""
    global _RIOTILER_OK
    if _RIOTILER_OK is None:
        try:
            import rio_tiler.io  # noqa: F401
            import rio_tiler.models  # noqa: F401

            _RIOTILER_OK = True
        except Exception:
            _RIOTILER_OK = False
    return _RIOTILER_OK


# --- rasterio fallback helpers ----------------------------------------------


def _to_uint8(dst, in_range):
    """Scale ``dst`` (nb, s, s) to uint8 per the resolved ``in_range``.

    - ``in_range is None`` and dtype uint8 → pass through unchanged.
    - ``in_range is None`` and integer dtype → linear map ``[0, dtype_max] → [0, 255]``
      (matches "rescale=none" full-range-mapped / crushed behavior).
    - ``in_range is None`` and float dtype → per-band data ``[min, max] → [0, 255]``.
    - ``in_range`` list of ``(lo, hi)`` → per-band clip-linear map.
    """
    dtype = dst.dtype
    if in_range is None:
        if dtype == np.uint8:
            return dst
        nb = dst.shape[0]
        out = np.zeros_like(dst, dtype="uint8")
        for i in range(nb):
            band = dst[i].astype("float64")
            if np.issubdtype(dtype, np.integer):
                lo, hi = 0.0, float(np.iinfo(dtype).max)
            else:
                lo = float(band.min())
                hi = float(band.max())
                if lo == hi:
                    hi = lo + 1.0
            out[i] = np.clip((band - lo) / (hi - lo) * 255, 0, 255).astype("uint8")
        return out
    # in_range is a list of (lo, hi) per band
    nb = dst.shape[0]
    out = np.zeros_like(dst, dtype="uint8")
    for i in range(nb):
        band = dst[i].astype("float64")
        lo, hi = float(in_range[i][0]), float(in_range[i][1])
        out[i] = np.clip((band - lo) / (hi - lo) * 255, 0, 255).astype("uint8")
    return out


def _encode(arr_u8, driver):
    """Encode ``arr_u8`` (bands, height, width) uint8 to bytes via GDAL CreateCopy.

    CreateCopy is the version-safe path for drivers that only support that mode
    (e.g. JPEG, WEBP on some GDAL builds).
    """
    import rasterio.shutil as rasterio_shutil
    from rasterio.io import MemoryFile

    n = arr_u8.shape[0]
    with MemoryFile() as src_m:
        with src_m.open(
            driver="GTiff",
            width=arr_u8.shape[2],
            height=arr_u8.shape[1],
            count=n,
            dtype="uint8",
        ) as src:
            src.write(arr_u8)
        with src_m.open() as src_ds, MemoryFile(ext="." + driver.lower()) as dst_m:
            rasterio_shutil.copy(src_ds, dst_m.name, driver=driver)
            return dst_m.read()


# --- transparent PNG (two backends) -----------------------------------------


def _transparent_png_riotiler(size: int) -> bytes:
    """Transparent RGBA PNG via rio-tiler (primary backend)."""
    from rio_tiler.models import ImageData  # lazy: see module-top note

    s = int(size)
    arr = np.zeros((4, s, s), dtype="uint8")
    return ImageData(arr).render(add_mask=False, img_format="PNG")


def _transparent_png_rasterio(size: int) -> bytes:
    """Transparent RGBA PNG via rasterio (fallback backend, no rio-tiler)."""
    s = int(size)
    arr = np.zeros((4, s, s), dtype="uint8")
    return _encode(arr, "PNG")


def transparent_png(size: int) -> bytes:
    """Return a fully transparent RGBA PNG of ``size`` x ``size`` (alpha=0)."""
    if _riotiler_available():
        return _transparent_png_riotiler(size)
    return _transparent_png_rasterio(size)


def _validate(fmt: str, size: int, resampling: str) -> tuple:
    """Validate + normalize (fmt upper, resampling -> rasterio name). Raises ValueError."""
    fmt_u = str(fmt).upper()
    if fmt_u not in ALLOWED_FORMATS:
        raise ValueError(
            f"rst_tilexyz: format must be one of {', '.join(sorted(ALLOWED_FORMATS))}; "
            f"got '{fmt}'"
        )
    resamp_l = str(resampling).lower()
    if resamp_l not in _RESAMPLING_MAP:
        raise ValueError(
            f"rst_tilexyz: unsupported resampling '{resampling}'; allowed: "
            f"{', '.join(sorted(_RESAMPLING_MAP))}"
        )
    s = int(size)
    if not (0 < s <= 4096):
        raise ValueError(f"rst_tilexyz: size must be in (0, 4096]; got {s}")
    return fmt_u, s, _RESAMPLING_MAP[resamp_l]


def _validate_rescale(rescale):
    """Normalize/validate the rescale arg.

    Returns the string ``"auto"`` / ``"none"``, or a normalized ``(min, max)``
    float tuple. ``None`` -> ``"auto"``. Raises ValueError on anything else.
    """
    if rescale is None:
        return "auto"
    if isinstance(rescale, str):
        r = rescale.lower()
        if r in ("auto", "none"):
            return r
        raise ValueError(
            f"rst_tilexyz: rescale must be 'auto', 'none', or a (min, max) pair; "
            f"got string '{rescale}'"
        )
    # Sequence -> (min, max)
    try:
        lo, hi = rescale  # unpacks exactly two; else ValueError
    except (TypeError, ValueError):
        raise ValueError(
            f"rst_tilexyz: rescale tuple must have exactly two numbers (min, max); "
            f"got {rescale!r}"
        )
    lo, hi = float(lo), float(hi)
    if not (lo < hi):
        raise ValueError(
            f"rst_tilexyz: rescale (min, max) must have min < max; got ({lo}, {hi})"
        )
    return (lo, hi)


def _resolve_in_range(ds, rescale):
    """Resolve the per-band ``in_range`` for rio-tiler render, or None for no rescale.

    - ``"none"`` -> None (today's full-dtype-range behavior).
    - explicit ``(min, max)`` -> that pair repeated for every band.
    - ``"auto"``:
        * uint8 source -> None (already display-ready; pass through unchanged).
        * non-uint8 -> per-band whole-dataset (min, max) via rasterio statistics.
          A constant band (min == max) is widened to (min, min + 1).
    """
    mode = _validate_rescale(rescale)
    if mode == "none":
        return None
    nbands = ds.count
    if isinstance(mode, tuple):
        return [mode] * nbands
    # mode == "auto"
    if np.dtype(ds.dtypes[0]) == np.uint8:
        return None
    out = []
    for b in range(1, nbands + 1):
        stats = ds.statistics(b, approx=False)
        lo, hi = float(stats.min), float(stats.max)
        if not (lo < hi):
            hi = lo + 1.0
        out.append((lo, hi))
    return out


# --- render_tile (two backends) ---------------------------------------------


def _render_tile_riotiler(ds, z, x, y, fmt_u, s, resamp_name, in_range) -> bytes:
    """Render a single XYZ tile via rio-tiler (primary backend).

    Receives already-validated args; does NOT re-validate. Returns
    ``transparent_png(s)`` on out-of-extent or any hard failure.
    """
    from rio_tiler.errors import TileOutsideBounds  # lazy: see module-top note
    from rio_tiler.io import Reader

    try:
        with Reader(None, dataset=ds) as cog:
            img = cog.tile(
                int(x), int(y), int(z), tilesize=s, resampling_method=resamp_name
            )
            if in_range is not None:
                img = img.post_process(in_range=in_range)
            out = img.render(img_format=fmt_u)
        if not out:
            return transparent_png(s)
        return out
    except TileOutsideBounds:
        return transparent_png(s)
    except Exception:
        # Slippy-map servers need a non-null 200 body even on failure.
        return transparent_png(s)


def _render_tile_rasterio(ds, z, x, y, fmt_u, s, resamp_name, in_range) -> bytes:
    """Render a single XYZ tile via rasterio warp (fallback backend, no rio-tiler).

    Receives already-validated args; does NOT re-validate. Returns
    ``transparent_png(s)`` on out-of-extent or any hard failure.
    """
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds
    from rasterio.warp import reproject

    try:
        tile = morecantile.Tile(int(x), int(y), int(z))
        west, south, east, north = _TMS.xy_bounds(tile)  # EPSG:3857 metres
        dst_transform = from_bounds(west, south, east, north, s, s)
        nb = ds.count
        # Coverage mask: reproject an all-valid sentinel band; 0 where no source
        # pixel maps into this tile.
        cov = np.zeros((1, s, s), dtype="uint8")
        reproject(
            source=np.full((1, ds.height, ds.width), 255, "uint8"),
            destination=cov,
            src_transform=ds.transform,
            src_crs=ds.crs,
            dst_transform=dst_transform,
            dst_crs="EPSG:3857",
            resampling=Resampling.nearest,
        )
        if int(cov.max()) == 0:
            return transparent_png(s)  # out-of-extent: no source pixels map here
        # Data: reproject in native dtype with the requested resampling.
        dst = np.zeros((nb, s, s), dtype=ds.dtypes[0])
        reproject(
            source=ds.read(),
            destination=dst,
            src_transform=ds.transform,
            src_crs=ds.crs,
            dst_transform=dst_transform,
            dst_crs="EPSG:3857",
            resampling=Resampling[resamp_name],
        )
        u8 = _to_uint8(dst, in_range)
        alpha = cov[0]
        # PNG / WEBP: append coverage mask as alpha channel (add_mask=True parity).
        # JPEG: colour bands only (no alpha in JPEG).
        if fmt_u == "JPEG":
            out_arr = u8[: min(nb, 3)]
        elif nb == 1:
            out_arr = np.stack([u8[0], alpha], axis=0)  # GA
        elif nb == 3:
            out_arr = np.concatenate([u8, alpha[np.newaxis]], axis=0)  # RGBA
        else:
            out_arr = u8  # nb == 4: already carries an alpha channel
        driver = {"JPEG": "JPEG", "WEBP": "WEBP"}.get(fmt_u, "PNG")
        return _encode(out_arr, driver)
    except Exception:
        # Slippy-map servers need a non-null 200 body even on failure.
        return transparent_png(s)


def render_tile(
    ds,
    z,
    x,
    y,
    fmt="PNG",
    size=256,
    resampling="bilinear",
    rescale="auto",
    in_range=None,
) -> bytes:
    """Render a single web-mercator (z, x, y) tile from open dataset ``ds``.

    Validates inputs (raises ValueError on bad format/size/resampling/rescale).
    Out-of-extent / empty tiles, or any hard render failure, return a transparent
    PNG of ``size`` x ``size`` (mirrors heavyweight: PNG regardless of ``fmt``).

    ``rescale`` controls 8-bit encoding contrast (see _resolve_in_range): "auto"
    (default) rescales non-8-bit rasters by whole-dataset min/max and passes uint8
    through unchanged; "none" keeps the raw full-dtype-range mapping; a (min, max)
    pair sets explicit bounds. ``in_range`` (internal) lets the pyramid path pass a
    precomputed per-band range so stats are read once, not per tile; when given it
    overrides ``rescale``.

    Dispatches to ``_render_tile_riotiler`` (primary) or ``_render_tile_rasterio``
    (fallback when rio-tiler is unavailable/broken on this executor).
    """
    fmt_u, s, resamp_name = _validate(fmt, size, resampling)
    if in_range is None:
        in_range = _resolve_in_range(ds, rescale)  # may raise ValueError on bad rescale
    # _NO_RESCALE sentinel: iter_pyramid passes this to signal "rescale=none, no scaling".
    # Map it back to None here so neither post_process nor _to_uint8 sees it.
    if in_range is _NO_RESCALE:
        in_range = None
    if _riotiler_available():
        return _render_tile_riotiler(ds, z, x, y, fmt_u, s, resamp_name, in_range)
    return _render_tile_rasterio(ds, z, x, y, fmt_u, s, resamp_name, in_range)


def _wgs84_bounds(ds) -> tuple:
    """Source extent as (west, south, east, north) in EPSG:4326."""
    return transform_bounds(ds.crs, "EPSG:4326", *ds.bounds)


def _zoom_tile_count(west, south, east, north, z) -> int:
    return sum(1 for _ in _TMS.tiles(west, south, east, north, [int(z)]))


def tile_count(ds, min_z, max_z) -> int:
    """Total intersecting tiles across [min_z, max_z]. Validates the zoom guards."""
    lo, hi = _validate_zoom_range(min_z, max_z)
    west, south, east, north = _wgs84_bounds(ds)
    total = 0
    for z in range(lo, hi + 1):
        total += _zoom_tile_count(west, south, east, north, z)
        if total > MAX_TILE_COUNT:
            _raise_count(lo, hi)
    return total


def intersecting_tiles(ds, min_z, max_z) -> list:
    """List of (z, x, y) tuples intersecting the source extent across the range."""
    lo, hi = _validate_zoom_range(min_z, max_z)
    west, south, east, north = _wgs84_bounds(ds)
    out = []
    for z in range(lo, hi + 1):
        for t in _TMS.tiles(west, south, east, north, [z]):
            out.append((t.z, t.x, t.y))
    return out


def iter_pyramid(
    ds, min_z, max_z, fmt="PNG", size=256, resampling="bilinear", rescale="auto"
):
    """Render every intersecting (z, x, y) tile across [min_z, max_z], streaming.

    Yields ``(z, x, y, bytes)`` tuples one tile at a time — never buffers the full
    pyramid (large-fan-out OOM guard). Validates zoom guards, the render args, the
    rescale arg, and the tile-count guard BEFORE rendering any tile. The rescale
    ``in_range`` is resolved ONCE so every tile shares one 8-bit mapping (no seams)
    and source statistics are read a single time.
    """
    lo, hi = _validate_zoom_range(min_z, max_z)
    # Validate render args up front (so bad format/size fails fast, not per-tile).
    _validate(fmt, size, resampling)
    in_range = _resolve_in_range(ds, rescale)  # once; also validates rescale
    # _resolve_in_range returns None for both "none" (no scaling) and uint8-auto
    # (pass-through). To distinguish "no scaling by design" from "not yet computed",
    # replace None with _NO_RESCALE sentinel so render_tile won't re-resolve rescale.
    if in_range is None:
        in_range = _NO_RESCALE
    west, south, east, north = _wgs84_bounds(ds)

    # Count guard first — never materialize a giant list to count.
    total = 0
    for z in range(lo, hi + 1):
        total += _zoom_tile_count(west, south, east, north, z)
        if total > MAX_TILE_COUNT:
            _raise_count(lo, hi)

    for z in range(lo, hi + 1):
        for t in _TMS.tiles(west, south, east, north, [z]):
            b = render_tile(ds, t.z, t.x, t.y, fmt, size, resampling, in_range=in_range)
            yield (t.z, t.x, t.y, b)


def pyramid(
    ds, min_z, max_z, fmt="PNG", size=256, resampling="bilinear", rescale="auto"
) -> list:
    """Render every intersecting (z, x, y) tile across [min_z, max_z].

    Returns a list of ``{"z","x","y","bytes"}`` dicts. List-materializing wrapper
    around :func:`iter_pyramid`.
    """
    return [
        {"z": z, "x": x, "y": y, "bytes": b}
        for z, x, y, b in iter_pyramid(ds, min_z, max_z, fmt, size, resampling, rescale)
    ]


def _validate_zoom_range(min_z, max_z) -> tuple:
    lo, hi = int(min_z), int(max_z)
    if lo < 0:
        raise ValueError(f"rst_xyzpyramid: min_z must be >= 0; got {lo}")
    if hi < lo:
        raise ValueError(f"rst_xyzpyramid: max_z ({hi}) must be >= min_z ({lo})")
    if hi > MAX_ZOOM:
        raise ValueError(
            f"rst_xyzpyramid: max_z must be <= {MAX_ZOOM} "
            f"(cell-count explosion at higher zooms); got {hi}"
        )
    return lo, hi


def _raise_count(lo, hi):
    raise ValueError(
        f"rst_xyzpyramid: tile-count across zoom range [{lo}, {hi}] exceeds "
        f"{MAX_TILE_COUNT} (raster extent is too large for that pyramid depth). "
        f"Lower max_z, or upstream-resample the raster before pyramidizing."
    )
