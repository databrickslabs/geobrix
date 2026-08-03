"""Render raster tiles from a Spark DataFrame; resolve virtual/materialized payloads.

The tile->pixels boundary for VizX. Delegates opening to pyrx.core.open_tile so
virtual (path+window) tiles read their window and apply pending nodata/srid/bands
instructions, and v1/v2/bytes shapes all normalize identically — no reimplementation.
"""

import warnings
from contextlib import contextmanager


def _extract_tile(tile_or_row, tile_col):
    """Pull the tile value out of a Row/dict that has a tile_col, else pass through."""
    # bytes / VirtualTile / a tile-struct dict|Row -> use as-is; a wrapper Row with
    # a tile_col field -> extract that field.
    if isinstance(tile_or_row, (bytes, bytearray)):
        return tile_or_row
    # dict/Row: if it has the tile_col AND that looks like a tile (struct), extract.
    d = None
    if hasattr(tile_or_row, "asDict"):
        d = tile_or_row.asDict()
    elif isinstance(tile_or_row, dict):
        d = tile_or_row
    if d is not None and tile_col in d and not ("raster" in d or "path" in d):
        return d[tile_col]
    return tile_or_row


@contextmanager
def resolve_tile_row(tile_or_row, tile_col="tile"):
    """Yield an open rasterio DatasetReader for a tile from any shape.

    Accepts a Spark Row (with ``tile_col``), a tile struct (dict/Row), a
    ``VirtualTile``, or raw GeoTIFF bytes. Virtual tiles are read via
    ``pyrx.core.open_tile`` (window + pending instructions applied); materialized
    tiles and bytes open directly. v1 (3-field) and v2 (8-field) both supported.
    """
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    tile = _extract_tile(tile_or_row, tile_col)
    with _ot.open_tile(_ot._to_virtual_tile(tile)) as ds:
        yield ds


# ---------------------------------------------------------------------------
# Bounded DataFrame collect
# ---------------------------------------------------------------------------

_MODE_DEFAULT_LIMITS = {"first": 1, "facet": 25, "mosaic": 64}


def _collect_bounded(df, tile_col, limit):
    """Pull at most ``limit`` rows to the driver; warn if the DF has more.

    Never collects the whole DF: uses ``df.limit(limit+1)`` to peek at overflow
    without a full ``count()``.
    """
    peek = df.limit(limit + 1).collect()
    if len(peek) > limit:
        warnings.warn(
            f"plot_tiles: DataFrame has more than limit={limit} tiles; "
            f"rendering the first {limit}. Filter the DataFrame or raise `limit`.",
            UserWarning,
            stacklevel=3,
        )
    return peek[:limit]


# ---------------------------------------------------------------------------
# Mosaic stub (Task 3)
# ---------------------------------------------------------------------------


def _plot_tiles_mosaic(
    df, tile_col, limit, *, fig_w, fig_h, max_pixels, composite, emphasis
):
    """Stitch same-CRS tiles into one georeferenced image via rasterio.merge."""
    from contextlib import ExitStack

    import matplotlib.pyplot as plt
    from rasterio.merge import merge

    from databricks.labs.gbx.vizx._raster import _render

    rows = _collect_bounded(df, tile_col, limit)
    with ExitStack() as stack:
        datasets = [stack.enter_context(resolve_tile_row(r, tile_col)) for r in rows]
        crs_set = {ds.crs.to_string() if ds.crs else None for ds in datasets}
        if len(crs_set) > 1:
            raise ValueError(
                f"plot_tiles(mode='mosaic'): tiles have differing CRS "
                f"{sorted(map(str, crs_set))}; filter the DataFrame to a single "
                f"CRS (cross-CRS mosaic is not supported here)."
            )
        mosaic, transform = merge(datasets)
    # decimate the merged array for display
    scale = max(mosaic.shape[-1], mosaic.shape[-2]) / max_pixels
    nodata = datasets[0].nodata if datasets else None
    _render(
        mosaic,
        transform,
        title="mosaic",
        fig_w=fig_w,
        fig_h=fig_h,
        scale=max(scale, 1.0),
        composite=composite,
        nodata=nodata,
        emphasis=emphasis,
    )
    return plt.gca()


# ---------------------------------------------------------------------------
# plot_tiles entry point
# ---------------------------------------------------------------------------


def plot_tiles(
    df,
    tile_col="tile",
    *,
    mode="facet",
    limit=None,
    fig_w=10,
    fig_h=10,
    max_pixels=2000,
    composite="auto",
    emphasis="blend",
):
    """Render raster tiles from a (filtered) Spark DataFrame.

    Parameters
    ----------
    df:
        A Spark DataFrame with a tile column (virtual or materialized, v1 or v2).
    tile_col:
        Name of the tile column. Default ``"tile"``.
    mode:
        ``"facet"`` (default) — grid of thumbnail panels, up to ``limit`` tiles.
        ``"first"`` — render only the first tile.
        ``"mosaic"`` — stitch same-CRS tiles into one georeferenced image; raises
        ``ValueError`` on mixed CRS (Task 3).
    limit:
        Maximum number of tiles collected from the DataFrame. Defaults to 25 for
        ``facet``, 1 for ``first``, 64 for ``mosaic``. The whole DataFrame is
        never collected; a ``UserWarning`` is emitted when the DF has more rows
        than ``limit``.
    fig_w, fig_h:
        Figure width and height in inches.
    max_pixels:
        Decimate rasters so their longest edge does not exceed this pixel count.
    composite:
        ``"auto"`` — single band → viridis, 3+ bands → RGB. ``"depth"`` —
        per-pixel band-coverage count rendered as viridis.
    emphasis:
        ``"blend"`` (default) or ``"data"``.

    Returns
    -------
    ``mode="first"`` → the Axes that was rendered into.
    ``mode="facet"`` → the Figure containing the panel grid.
    ``mode="mosaic"`` → the Axes that was rendered into.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import matplotlib.pyplot as plt

    from databricks.labs.gbx.vizx._raster import _decimated_read, _render

    if mode not in ("first", "facet", "mosaic"):
        raise ValueError(f"plot_tiles: mode must be first|facet|mosaic; got {mode!r}")
    if limit is None:
        limit = _MODE_DEFAULT_LIMITS[mode]

    if mode == "mosaic":
        return _plot_tiles_mosaic(
            df,
            tile_col,
            limit,
            fig_w=fig_w,
            fig_h=fig_h,
            max_pixels=max_pixels,
            composite=composite,
            emphasis=emphasis,
        )

    rows = _collect_bounded(df, tile_col, limit if mode == "facet" else 1)

    if mode == "first":
        with resolve_tile_row(rows[0], tile_col) as src:
            data, transform, scale = _decimated_read(src, max_pixels)
            _render(
                data,
                transform,
                title="tile",
                fig_w=fig_w,
                fig_h=fig_h,
                scale=scale,
                composite=composite,
                nodata=src.nodata,
                emphasis=emphasis,
            )
        return plt.gca()

    # facet: grid of panels
    n = len(rows)
    ncols = min(n, 4) or 1
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h), squeeze=False)
    flat = [a for r in axes for a in r]
    for ax, row in zip(flat, rows):
        with resolve_tile_row(row, tile_col) as src:
            data, transform, scale = _decimated_read(src, max_pixels)
            _render(
                data,
                transform,
                title=None,
                fig_w=fig_w,
                fig_h=fig_h,
                scale=scale,
                composite=composite,
                nodata=src.nodata,
                emphasis=emphasis,
                ax=ax,
            )
    for ax in flat[n:]:
        ax.axis("off")
    return fig
