"""Cloud-Optimized GeoTIFF viewer for gbx.vizx.

Reads a COG decimated (an overview-equivalent read) and renders it over a
contextily basemap as a static matplotlib figure. Driver-side; requires the
[vizx] extra plus rasterio.
"""

from __future__ import annotations

import warnings


def _strip_scheme(path: str) -> str:
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    if path.startswith("//"):
        path = "/" + path.lstrip("/")
    return path


# ---------------------------------------------------------------------------
# emphasis styling defaults (static raster / COG tier)
# ---------------------------------------------------------------------------
#
# ``emphasis="data"`` renders the raster at full strength with a vivid
# colormap; ``emphasis="blend"`` (default) keeps the prior softer render. The basemap stays
# full-strength in both modes -- we emphasize the DATA, not the basemap.
_COG_EMPHASIS = {
    "data": {"cmap": "viridis", "alpha": 1.0},
    "blend": {"cmap": "viridis", "alpha": 1.0},  # prior: imshow had no alpha (=1.0)
}


def _validate_emphasis(emphasis):
    if emphasis not in _COG_EMPHASIS:
        raise ValueError(f"emphasis must be 'data' or 'blend'; got {emphasis!r}")
    return emphasis


def _render_cog(
    data,
    transform,
    *,
    crs,
    fig_w,
    fig_h,
    title,
    basemap,
    basemap_source,
    ax=None,
    emphasis="blend",
):
    """Render a decimated COG array (bands, h, w) over a contextily basemap.

    When *ax* is provided the caller owns the figure; this function draws onto
    it and returns it without calling ``plt.show``.  When *ax* is ``None`` a
    new figure is created (original behaviour). ``emphasis="data"`` renders the
    raster vivid at full opacity; ``"blend"`` (default) keeps the prior softer
    render. The basemap is full-strength in both modes.
    """
    import matplotlib.pyplot as plt
    from rasterio.plot import plotting_extent, show

    from databricks.labs.gbx.vizx._raster import (
        _needs_percentile_stretch,
        _percentile_stretch,
    )

    em = _COG_EMPHASIS[emphasis]
    if _needs_percentile_stretch(data):
        data = _percentile_stretch(data)
    owns_fig = ax is None
    if owns_fig:
        _, ax = plt.subplots(1, figsize=(fig_w, fig_h))
    # Draw the COG ABOVE the basemap (zorder): a basemap added after imshow renders
    # on top and hides an opaque full-extent raster (a DEM showed only the map).
    # With the raster on top, the basemap shows through nodata / around a partial COG.
    if data.shape[0] == 1:
        band = data[0]
        ax.imshow(
            band,
            extent=plotting_extent(band, transform),
            cmap=em["cmap"],
            alpha=em["alpha"],
            zorder=2,
        )
    else:
        show(data, ax=ax, transform=transform, zorder=2, alpha=em["alpha"])
    if basemap and crs is not None:
        try:
            import contextily as cx

            source = basemap_source or cx.providers.CartoDB.Positron
            cx.add_basemap(ax, source=source, crs=crs, zorder=1)
        except Exception as exc:  # noqa: BLE001 — offline/no-egress -> warn + skip
            warnings.warn(
                f"plot_cog: basemap unavailable ({type(exc).__name__}: {exc}); "
                "rendering without basemap.",
                stacklevel=2,
            )
    if title:
        ax.set_title(title)
    ax.set_axis_off()
    return ax


def plot_tile(
    tile,
    *,
    band=1,
    window_bounds=None,
    mask_below=None,
    mask_below_percentile=None,
    stretch=(2, 98),
    cmap="inferno",
    colorbar_label=None,
    basemap=True,
    basemap_source=None,
    title=None,
    fig_w=10,
    fig_h=9,
    ax=None,
):
    """Drape a single raster-tile band over a contextily basemap (static figure).

    The in-memory companion to :func:`plot_cog`: takes a GeoBrix tile struct (a
    Row/dict with a ``raster`` GeoTIFF ``bytes`` member) or raw ``bytes`` rather
    than a file path. Reads ``band`` (1-based), optionally windowed to
    ``window_bounds`` ``(minx, miny, maxx, maxy)`` in the tile's own CRS for a
    zoomed-in view; and masks the background so the basemap shows through and the
    signal glows in place — pass ``mask_below`` (an absolute value, robust across
    zoom levels) or ``mask_below_percentile`` (a percentile of the window). The
    contrast ``stretch`` percentile pair is computed on the VISIBLE (post-mask)
    values so the signal keeps its contrast at any zoom. A labeled colorbar is
    drawn when ``colorbar_label`` is given. The basemap is rendered in the tile's
    CRS; any fetch failure (no egress) degrades to a warning and a basemap-less
    render. Requires the [vizx] extra plus rasterio. Returns the matplotlib Axes.
    """
    import matplotlib.pyplot as plt
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import array_bounds
    from rasterio.windows import from_bounds

    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    raw = tile if isinstance(tile, (bytes, bytearray)) else tile["raster"]
    with MemoryFile(bytes(raw)) as mf, mf.open() as ds:
        crs = ds.crs
        if window_bounds is not None:
            win = from_bounds(*window_bounds, ds.transform)
            arr = ds.read(band, window=win, masked=True)
            wt = ds.window_transform(win)
        else:
            arr = ds.read(band, masked=True)
            wt = ds.transform
    left, bottom, right, top = array_bounds(arr.shape[0], arr.shape[1], wt)

    valid = arr.compressed() if np.ma.isMaskedArray(arr) else arr[np.isfinite(arr)]
    shown = arr
    alpha = 1.0
    thr = None
    if mask_below is not None:
        thr = float(mask_below)
    elif mask_below_percentile is not None:
        thr = float(np.percentile(valid, mask_below_percentile))
    if thr is not None:
        shown = np.ma.masked_less(arr, thr)
        alpha = 0.85  # let the basemap read through around the masked signal
    # stretch over the VISIBLE values so the signal keeps contrast at any zoom
    basis = shown.compressed() if np.ma.isMaskedArray(shown) else valid
    basis = basis if basis.size else valid
    lo, hi = (float(x) for x in np.percentile(basis, list(stretch)))

    owns_fig = ax is None
    if owns_fig:
        _, ax = plt.subplots(1, figsize=(fig_w, fig_h))
    im = ax.imshow(
        shown,
        cmap=cmap,
        extent=(left, right, bottom, top),
        vmin=lo,
        vmax=hi,
        alpha=alpha,
        zorder=2,
    )
    ax.set_xlim(left, right)
    ax.set_ylim(bottom, top)
    if basemap and crs is not None:
        try:
            import contextily as cx

            source = basemap_source or cx.providers.CartoDB.Positron
            cx.add_basemap(ax, source=source, crs=crs, zorder=1)
        except Exception as exc:  # noqa: BLE001 — offline/no-egress -> warn + skip
            warnings.warn(
                f"plot_tile: basemap unavailable ({type(exc).__name__}: {exc}); "
                "rendering without basemap.",
                stacklevel=2,
            )
    if colorbar_label:
        ax.figure.colorbar(im, ax=ax, shrink=0.82).set_label(colorbar_label)
    if title:
        ax.set_title(title)
    return ax


def plot_cog(
    path,
    *,
    band=None,
    max_pixels=2000,
    fig_w=10,
    fig_h=10,
    basemap=True,
    basemap_source=None,
    title=None,
    ax=None,
    emphasis="blend",
    debug_mode=1,
    **kw,
):
    """Render a Cloud-Optimized GeoTIFF inline over a contextily basemap.

    Reads ``path`` decimated so the longest edge is <= ``max_pixels`` (uses the
    COG's overviews when present). ``band`` (1-based) selects a single band;
    otherwise all bands render (1 -> viridis, 3+ -> RGB). Volume/DBFS scheme
    prefixes are stripped. ``emphasis="data"`` renders the raster vivid
    at full opacity so it pops against the full-strength basemap; ``"blend"``
    keeps the prior softer render. ``debug_mode`` (``0`` silent, ``1`` default,
    ``2`` diagnostics) mirrors the other entrypoints. Requires the [vizx] extra
    plus rasterio.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available
    from databricks.labs.gbx.vizx._maplibre import _emit

    _validate_emphasis(emphasis)
    assert_viz_available()
    import rasterio

    em = _COG_EMPHASIS[emphasis]
    _emit(
        f"[vizx]   emphasis={emphasis}: cmap={em['cmap']}, alpha={em['alpha']}",
        level=2,
        debug_mode=debug_mode,
    )

    from databricks.labs.gbx.vizx._raster import _decimated_read

    p = _strip_scheme(str(path))
    with rasterio.open(p) as src:
        if band is not None:
            scale = max(src.width, src.height) / max_pixels
            out_h = max(1, int(src.height // scale)) if scale > 1 else src.height
            out_w = max(1, int(src.width // scale)) if scale > 1 else src.width
            data = src.read(
                indexes=[band],
                out_shape=(1, out_h, out_w),
                resampling=rasterio.enums.Resampling.bilinear,
                masked=True,
            )
            transform = src.transform * src.transform.scale(
                src.width / out_w, src.height / out_h
            )
        else:
            data, transform, _ = _decimated_read(src, max_pixels)
        crs = src.crs
    return _render_cog(
        data,
        transform,
        crs=crs,
        fig_w=fig_w,
        fig_h=fig_h,
        title=title or "COG",
        basemap=basemap,
        basemap_source=basemap_source,
        ax=ax,
        emphasis=emphasis,
    )
