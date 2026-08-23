"""Raster rendering pipeline for gbx.vizx (decimation + percentile stretch).

Ported from notebooks/examples/eo-series/library.py. matplotlib/rasterio are
lazy-imported inside the public plotters (Task 3); the numeric helpers here use
only numpy and the rasterio dataset passed in.
"""

import os

import numpy as np

# ---------------------------------------------------------------------------
# emphasis styling defaults (raster tier)
# ---------------------------------------------------------------------------
#
# ``emphasis="data"`` renders the raster vivid at full strength;
# ``emphasis="blend"`` (default) keeps the prior softer render. plot_raster has no basemap
# (bytes are not always georeferenced), so emphasis here tunes the data render
# itself: a vivid colormap at full alpha vs a softer alpha.
_RASTER_EMPHASIS = {
    "data": {"cmap": "viridis", "alpha": 1.0},
    "blend": {"cmap": "viridis", "alpha": 1.0},  # prior: imshow had no alpha (=1.0)
}


def _validate_emphasis(emphasis):
    if emphasis not in _RASTER_EMPHASIS:
        raise ValueError(f"emphasis must be 'data' or 'blend'; got {emphasis!r}")
    return emphasis


def _decimated_read(src, max_pixels):
    """Read `src` (rasterio DatasetReader) decimated so max(width,height)<=max_pixels.

    Returns (data, transform, scale). masked=True so nodata is honored downstream.
    """
    import rasterio

    scale = max(src.width, src.height) / max_pixels
    if scale > 1:
        out_shape = (src.count, int(src.height // scale), int(src.width // scale))
        data = src.read(
            out_shape=out_shape,
            resampling=rasterio.enums.Resampling.bilinear,
            masked=True,
        )
        transform = src.transform * src.transform.scale(
            src.width / data.shape[-1],
            src.height / data.shape[-2],
        )
    else:
        data = src.read(masked=True)
        transform = src.transform
    return data, transform, scale


def _read_windowed(src, max_pixels, window=None, resampling="bilinear"):
    """Decimated, masked read of `src`, optionally restricted to `window`.

    Like ``_decimated_read`` but (a) accepts a rasterio Window (None = whole
    dataset) and (b) a resampling method name. Returns (data, transform, scale).
    Peak RAM is bounded by the output buffer, not the source: GDAL reads from an
    internal overview when present, else block-streams a decimated read of the
    base level, so a single-zoom-level tile still renders memory-safely.
    """
    from rasterio.enums import Resampling

    resamp = getattr(Resampling, resampling)
    if window is None:
        win_w, win_h = src.width, src.height
        base_transform = src.transform
    else:
        win_w, win_h = int(round(window.width)), int(round(window.height))
        base_transform = src.window_transform(window)

    scale = max(win_w, win_h) / max_pixels
    if scale > 1:
        out_h = max(1, int(win_h // scale))
        out_w = max(1, int(win_w // scale))
        data = src.read(
            out_shape=(src.count, out_h, out_w),
            window=window,
            resampling=resamp,
            masked=True,
        )
        transform = base_transform * base_transform.scale(win_w / out_w, win_h / out_h)
    else:
        data = src.read(window=window, masked=True)
        transform = base_transform
    return data, transform, scale


def _needs_percentile_stretch(data):
    """True when data is integer-typed with a max above matplotlib's RGB int 255."""
    if not np.issubdtype(data.dtype, np.integer):
        return False
    mx = np.ma.max(data) if isinstance(data, np.ma.MaskedArray) else data.max()
    if mx is np.ma.masked:
        return False
    return int(mx) > 255


def _needs_display_scaling(data):
    """True when data needs scaling for display: integers >255 or floats outside [0,1].

    Returns True if data should be stretched to [0,1] for display (e.g., uint16 or
    float reflectance). Used in the enhanced path to gate percentile/shared stretching.
    Does NOT gate fixed-range stretching, which is unconditional.

    Args:
        data: (count,H,W) array (MaskedArray or ndarray).

    Returns:
        True if (integer dtype AND max > 255) OR (float dtype AND (max > 1.0 OR min < 0)).
    """
    is_masked = isinstance(data, np.ma.MaskedArray)
    if np.issubdtype(data.dtype, np.integer):
        mx = np.ma.max(data) if is_masked else data.max()
        if mx is np.ma.masked:
            return False
        return int(mx) > 255
    elif np.issubdtype(data.dtype, np.floating):
        mx = np.ma.max(data) if is_masked else data.max()
        mn = np.ma.min(data) if is_masked else data.min()
        if mx is np.ma.masked or mn is np.ma.masked:
            return False
        return float(mx) > 1.0 or float(mn) < 0.0
    return False


def _percentile_stretch(data, lo_pct=2, hi_pct=98):
    """Per-band 2-98th percentile stretch to [0,1] float32; masked pixels excluded."""
    if data.ndim == 2:
        data = data[np.newaxis, ...]
    is_masked = isinstance(data, np.ma.MaskedArray)
    out = np.empty(data.shape, dtype=np.float32)
    for b in range(data.shape[0]):
        band = data[b]
        valid = band.compressed() if is_masked else np.asarray(band).ravel()
        if valid.size == 0:
            out[b] = 0.0
            continue
        lo, hi = np.percentile(valid, (lo_pct, hi_pct))
        rng = max(float(hi - lo), 1e-9)
        out[b] = np.clip((np.asarray(band, dtype=np.float32) - lo) / rng, 0.0, 1.0)
    return np.ma.MaskedArray(out, mask=data.mask) if is_masked else out


def _select_bands(data, bands):
    """Select and reorder bands by 1-based indices. Returns (count,H,W) subset.

    Args:
        data:  (count,H,W) array (MaskedArray or ndarray).
        bands: None (all), int (single band), or sequence of 1-based band indices
               (e.g., (3,1,2)). Scalar int converts to single-band selection.

    Returns:
        The subset array with bands reordered. 1-based indices are converted to
        0-based for array indexing.

    Raises:
        ValueError: band index out of range, length==0/2 (ambiguous), or invalid type.
    """
    if bands is None:
        return data
    # Coerce scalar int to 1-tuple (FIX 6)
    if isinstance(bands, int) and not isinstance(bands, bool):
        bands = (bands,)
    # Validate type (reject non-sequence after scalar coercion)
    try:
        bands_len = len(bands)
    except TypeError:
        raise ValueError(
            f"bands must be None, int, or sequence of band indices; "
            f"got {type(bands).__name__}"
        )
    # Reject empty selection (FIX 3a)
    if bands_len == 0:
        raise ValueError("bands selection cannot be empty (length 0)")
    # Reject length-2 as ambiguous
    if bands_len == 2:
        raise ValueError(
            f"Band selection of length 2 is ambiguous (RGB needs 3, viridis needs 1); "
            f"got {bands_len} bands"
        )
    count = data.shape[0]
    for b in bands:
        if b < 1 or b > count:
            raise ValueError(f"band index {b} out of range [1, {count}]")
    indices = [b - 1 for b in bands]  # convert to 0-based
    return data[indices]


def _apply_fill_mask(data, fill):
    """Mask pixels equal to fill; combine with existing mask if data is MaskedArray.

    Handles NaN fill values via np.isnan (since NaN != NaN).

    Args:
        data: (count,H,W) array.
        fill: Scalar fill value, or None to skip masking. Can be NaN.

    Returns:
        MaskedArray with fill pixels masked, or data unchanged if fill is None.
    """
    if fill is None:
        return data
    is_masked = isinstance(data, np.ma.MaskedArray)
    # Handle NaN fill: use np.isnan instead of data == fill (NaN != NaN)
    if isinstance(fill, float) and np.isnan(fill):
        new_mask = np.isnan(data)
    else:
        new_mask = data == fill
    if is_masked:
        combined_mask = data.mask | new_mask
        return np.ma.MaskedArray(data.data, mask=combined_mask)
    else:
        return np.ma.MaskedArray(data, mask=new_mask)


def _stretch_shared(data, lo_pct=2, hi_pct=98):
    """Pooled-percentile stretch to [0,1] float32; single (lo,hi) across all bands.

    Computes one (lo, hi) from the pooled valid pixels of all bands, then applies
    it identically to each band. This preserves inter-band color offsets (e.g.,
    correlated bands remain visually distinct after stretch).

    Args:
        data: (count,H,W) MaskedArray or ndarray.
        lo_pct, hi_pct: Percentile thresholds (default 2, 98).

    Returns:
        float32 array in [0,1], mask preserved if data is MaskedArray.
    """
    if data.ndim == 2:
        data = data[np.newaxis, ...]
    is_masked = isinstance(data, np.ma.MaskedArray)

    # Pool all valid pixels across all bands
    if is_masked:
        valid = np.asarray(data.compressed())
    else:
        valid = np.asarray(data).ravel()
    if valid.size == 0:
        return np.zeros_like(data, dtype=np.float32)

    lo, hi = np.percentile(valid, (lo_pct, hi_pct))
    rng = max(float(hi - lo), 1e-9)

    # Apply the same (lo, hi) to all bands, ensure float32 output
    out = np.clip(
        (np.asarray(data, dtype=np.float32) - float(lo)) / float(rng), 0.0, 1.0
    ).astype(np.float32)
    return np.ma.MaskedArray(out, mask=data.mask) if is_masked else out


def _stretch_fixed(data, lo, hi):
    """Fixed-range stretch: (x - lo) / (hi - lo) clipped to [0,1] float32.

    Args:
        data: (count,H,W) array.
        lo, hi: Range endpoints (hi > lo, or ValueError).

    Returns:
        float32 array in [0,1], mask preserved if data is MaskedArray.

    Raises:
        ValueError: if hi <= lo.
    """
    if hi <= lo:
        raise ValueError(f"hi ({hi}) must be greater than lo ({lo})")
    is_masked = isinstance(data, np.ma.MaskedArray)
    out = np.clip(
        (np.asarray(data, dtype=np.float32) - float(lo)) / float(hi - lo), 0.0, 1.0
    ).astype(np.float32)
    return np.ma.MaskedArray(out, mask=data.mask) if is_masked else out


def _compose_rgba(rgb, base_alpha):
    """Convert 3-band float [0,1] RGB to (H,W,4) RGBA with alpha channel.

    Masked pixels get alpha=0 (transparent); valid pixels get base_alpha.
    RGB channels are clipped to [0,1].

    Args:
        rgb: (3,H,W) MaskedArray or ndarray in [0,1] (float).
        base_alpha: Alpha value for valid pixels (float in [0,1]).

    Returns:
        (H,W,4) float32 ndarray with RGBA channels. Pixels masked in any of the
        3 RGB channels are set to RGBA=(0,0,0,0) (transparent black).
    """
    if rgb.shape[0] != 3:
        raise ValueError(f"_compose_rgba expects 3 RGB bands; got {rgb.shape[0]}")
    h, w = rgb.shape[1], rgb.shape[2]
    rgba = np.zeros((h, w, 4), dtype=np.float32)

    # Clip RGB to [0,1] and place in output
    rgb_clipped = np.asarray(rgb, dtype=np.float32)
    rgb_clipped = np.clip(rgb_clipped, 0.0, 1.0)
    rgba[..., :3] = np.transpose(rgb_clipped, (1, 2, 0))

    # Set alpha: 0 where any channel is masked, base_alpha otherwise
    if isinstance(rgb, np.ma.MaskedArray):
        # Any pixel masked in ANY band -> alpha=0
        any_masked = rgb.mask.any(axis=0)
        rgba[..., 3] = np.where(any_masked, 0.0, base_alpha)
    else:
        rgba[..., 3] = base_alpha

    return rgba


def _coverage_depth(data, nodata):
    """Per-pixel count of bands that cover the pixel (valid / not-NoData).

    Args:
        data:   3-D array of shape (bands, height, width).  May be a
                ``numpy.ma.MaskedArray`` (masked pixels = not covered) or a
                plain ndarray where ``nodata`` marks missing values.
        nodata: Scalar nodata sentinel used when *data* is not masked.

    Returns:
        2-D ``float32`` array of shape (height, width) with values in
        ``[0, bands]``.  A value of 0 means no band covers that pixel.
    """
    if isinstance(data, np.ma.MaskedArray):
        covered = (~data.mask).astype(np.float32)
    else:
        arr = np.asarray(data, dtype=np.float32)
        if nodata is not None and np.isnan(nodata):
            covered = (~np.isnan(arr)).astype(np.float32)
        elif nodata is not None:
            covered = (arr != float(nodata)).astype(np.float32)
        else:
            covered = np.ones(arr.shape, dtype=np.float32)
    return covered.sum(axis=0)


def _single_band_clim(valid):
    """(vmin, vmax) override for a single band, or None to let matplotlib auto-scale.

    valid: 1-D array of the band's unmasked values. Returns an explicit
    (vmin, vmax) only when the band is constant (vmax <= vmin), which would
    otherwise make matplotlib's normalizer degenerate and render blank — e.g. a
    presence mask of all 1.0. Maps the constant onto a non-degenerate range so it
    gets a distinct color. Returns None for normal (varying) data.
    """
    if valid.size == 0:
        return None
    vmin = float(valid.min())
    vmax = float(valid.max())
    if vmax > vmin:
        return None
    lo = min(0.0, vmin)
    hi = vmin if vmin > lo else lo + 1.0
    return (lo, hi)


def _draw_single_band(ax, band, transform, em):
    """Draw a single-band raster on ax using viridis.

    Used by both back-compat and enhanced paths to avoid duplication.
    Handles constant-valued bands by overriding vmin/vmax via _single_band_clim.

    Args:
        ax: matplotlib Axes to draw into.
        band: (H,W) single-band array (MaskedArray or ndarray).
        transform: rasterio Transform for georeference.
        em: emphasis dict with 'cmap' and 'alpha' keys.
    """
    from rasterio.plot import plotting_extent

    valid = (
        band.compressed()
        if isinstance(band, np.ma.MaskedArray)
        else np.asarray(band).ravel()
    )
    ax.set_facecolor("whitesmoke")
    clim = _single_band_clim(valid)
    kw = {"cmap": em["cmap"], "alpha": em["alpha"]}
    if clim is not None:
        kw["vmin"], kw["vmax"] = clim
    ax.imshow(band, extent=plotting_extent(band, transform), **kw)


def _render(  # noqa: C901
    data,
    transform,
    *,
    title,
    fig_w,
    fig_h,
    scale,
    composite="auto",
    nodata=None,
    emphasis="blend",
    ax=None,
    bands=None,
    stretch="perband",
    fill=None,
):
    """Stretch when needed, then plot via rasterio.plot.show (Agg-safe).

    Args:
        composite: ``"auto"`` — 1 band → viridis; 3+ → RGB (default).
                   ``"depth"`` — render per-pixel band coverage count as viridis;
                   depth==0 (no band covers the pixel) is masked transparent.
        ax:        Optional matplotlib Axes to draw into. When given, the function
                   draws into that Axes instead of creating a new figure, and
                   returns the Axes. When ``None`` (default), a new figure is
                   created and ``pyplot.show()`` is called — existing behavior.
        bands:     None (all), or tuple of 1-based band indices to select/reorder.
                   Single band or 3+ bands; length-2 raises ValueError (ambiguous).
        stretch:   "perband" (default) — independent per-band percentile stretch.
                   "shared" — pooled percentile across all selected bands.
                   (lo, hi) tuple/list — fixed-range stretch [lo, hi].
        fill:      None (default), or scalar fill value to treat as NoData for display.
                   Excluded from stretch percentiles and rendered transparent.
    """
    import sys

    import matplotlib

    # Select Agg before pyplot is imported only when: (a) no explicit backend has
    # been requested via MPLBACKEND or a prior matplotlib.use() call (detected by
    # pyplot not yet imported), and (b) there is no display available (headless
    # cluster/CI).  Databricks notebooks set their own inline/Agg backend before
    # this point, so pyplot will already be in sys.modules and we skip the override.
    if "matplotlib.pyplot" not in sys.modules and "MPLBACKEND" not in os.environ:
        if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
            matplotlib.use("Agg")
    from matplotlib import pyplot
    from rasterio.plot import show

    em = _RASTER_EMPHASIS[emphasis]
    _owns_fig = ax is None  # True when we create our own figure

    # FIX 4: Raise error if composite="depth" + any new kwargs
    if composite == "depth" and (
        bands is not None or fill is not None or stretch != "perband"
    ):
        raise ValueError(
            "composite='depth' does not support bands=, stretch=, or fill=; "
            "it renders per-pixel band coverage only"
        )

    if composite == "depth":
        depth = _coverage_depth(data, nodata)
        # Mask pixels where no band covers — render as transparent.
        depth_masked = np.ma.MaskedArray(
            depth[np.newaxis, ...], mask=(depth == 0)[np.newaxis, ...]
        )
        full_title = (
            f"coverage depth (bands) (scale 1/{round(scale, 1)}x)"
            if scale > 1
            else "coverage depth (bands)"
        )
        if _owns_fig:
            fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))
        show(depth_masked, ax=ax, transform=transform, cmap="viridis")
        ax.set_title(full_title)
        if _owns_fig:
            pyplot.show()
        return ax

    # Back-compat gate: if no new kwargs are set, use the existing code path
    using_new_path = bands is not None or stretch != "perband" or fill is not None

    if not using_new_path:
        # EXISTING CODE PATH (unchanged for back-compat)
        if _needs_percentile_stretch(data):
            data = _percentile_stretch(data)
        if _owns_fig:
            fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))
        if data.shape[0] == 1:
            _draw_single_band(ax, data[0], transform, em)
        else:
            show(data, ax=ax, transform=transform, alpha=em["alpha"])
        full_title = f"{title} (scale 1/{round(scale, 1)}x)" if scale > 1 else title
        if title is not None:
            ax.set_title(full_title)
        if _owns_fig:
            pyplot.show()
        return ax

    # ENHANCED PATH (new kwargs used)
    # 1. Select bands
    if bands is not None:
        data = _select_bands(data, bands)

    # 2. Apply fill mask
    if fill is not None:
        data = _apply_fill_mask(data, fill)

    # 3. Apply stretch
    if isinstance(stretch, (tuple, list)) and len(stretch) == 2:
        # Fixed range stretch: unconditional (works for uint8, uint16, float)
        lo, hi = stretch
        data = _stretch_fixed(data, lo, hi)
    elif stretch == "shared":
        # Shared percentile stretch (FIX 1: use _needs_display_scaling for float support)
        if _needs_display_scaling(data):
            data = _stretch_shared(data)
    elif stretch == "perband":
        # Per-band percentile stretch (FIX 1: use _needs_display_scaling for float support)
        if _needs_display_scaling(data):
            data = _percentile_stretch(data)
    else:
        raise ValueError(
            f"stretch must be 'perband', 'shared', or (lo, hi) tuple; got {stretch!r}"
        )

    # FIX 3b: Validate band count before rendering RGB
    if data.shape[0] != 1 and data.shape[0] < 3:
        raise ValueError(
            f"RGB render needs >=3 bands (or exactly 1 for single-band); "
            f"got {data.shape[0]}. Use bands= to select 1 or >=3 bands."
        )

    # 4. Render
    if _owns_fig:
        fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))

    if data.shape[0] == 1:
        # Single-band viridis path
        _draw_single_band(ax, data[0], transform, em)
    else:
        # Multi-band RGB path: use _compose_rgba + imshow with alpha
        from rasterio.plot import plotting_extent

        # Use first 3 bands for RGB
        rgb_data = data[:3]
        # Normalize uint8 and other integer data <= 255 to float [0,1]
        # _compose_rgba expects float in [0,1]; uint16 is stretched earlier,
        # but uint8 / small integers are not, so normalize by 255.0.
        if np.issubdtype(rgb_data.dtype, np.integer) and np.ma.max(rgb_data) <= 255:
            is_masked = isinstance(rgb_data, np.ma.MaskedArray)
            mask_data = rgb_data.mask if is_masked else None
            rgb_data = rgb_data.astype("float32") / 255.0
            if is_masked:
                rgb_data = np.ma.MaskedArray(rgb_data, mask=mask_data)
        rgba_image = _compose_rgba(rgb_data, em["alpha"])
        ax.set_facecolor("whitesmoke")
        ax.imshow(rgba_image, extent=plotting_extent(data[0], transform))

    full_title = f"{title} (scale 1/{round(scale, 1)}x)" if scale > 1 else title
    if title is not None:
        ax.set_title(full_title)
    if _owns_fig:
        pyplot.show()
    return ax


def plot_raster(
    raster_bytes,
    *,
    fig_w=10,
    fig_h=10,
    max_pixels=2000,
    composite="auto",
    emphasis="blend",
    debug_mode=1,
    bands=None,
    stretch="perband",
    fill=None,
):
    """Render a raster from in-memory bytes (e.g. a tile's `raster` field).

    Auto-decimates above max_pixels; integer rasters whose values exceed 255
    (typical EO UInt16) get a per-band 2-98% percentile stretch. Single-band ->
    viridis; multi-band -> RGB. ``emphasis="data"`` renders the raster
    vivid at full opacity; ``"blend"`` (default) keeps the prior softer render.
    ``debug_mode`` (``0`` silent, ``1`` default, ``2`` diagnostics) mirrors the
    other entrypoints. Requires the [vizx] extra.

    Args:
        composite: ``"auto"`` (default) — 1 band → viridis; 3+ → RGB.
                   ``"depth"`` — render per-pixel coverage depth (count of bands
                   covering each pixel) as a viridis gradient; uncovered pixels
                   are masked transparent.  Useful for multi-band presence masks
                   where an RGB composite would appear mostly black.
        bands:     None (default, all bands), or tuple of 1-based band indices
                   to select/reorder (e.g., ``(3, 1, 2)``). Single band renders
                   as viridis; 3+ as RGB. Length-2 raises ValueError (ambiguous).
        stretch:   "perband" (default) — independent per-band percentile stretch.
                   "shared" — pooled percentile across all selected bands.
                   (lo, hi) tuple/list — fixed-range stretch [lo, hi].
        fill:      None (default), or scalar fill value to treat as NoData for
                   display. Excluded from stretch percentiles and rendered
                   transparent over the background.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available
    from databricks.labs.gbx.vizx._maplibre import _emit

    _validate_emphasis(emphasis)
    assert_viz_available()
    from rasterio.io import MemoryFile

    em = _RASTER_EMPHASIS[emphasis]
    _emit(
        f"[vizx]   emphasis={emphasis}: cmap={em['cmap']}, alpha={em['alpha']}",
        level=2,
        debug_mode=debug_mode,
    )

    with MemoryFile(bytes(raster_bytes)) as mf:
        with mf.open() as src:
            data, transform, scale = _decimated_read(src, max_pixels)
            _render(
                data,
                transform,
                title="tile.raster",
                fig_w=fig_w,
                fig_h=fig_h,
                scale=scale,
                composite=composite,
                nodata=src.nodata,
                emphasis=emphasis,
                bands=bands,
                stretch=stretch,
                fill=fill,
            )


def plot_mask_layers(
    layers, *, fig_w=10, fig_h=8, max_pixels=2000, colors=None, title="coverage layers"
):
    """Overlay several single-band presence-mask tiles on one axes, with a legend.

    Each layer is drawn as a single solid colour where it is covered (any
    non-NoData pixel); NoData is transparent, so layers stack and a legend maps
    colour → label. Tiles must share the same grid/extent (e.g. produced on a shared
    canvas via ``rst_h3_gridspec``). Layers are drawn in order, so pass the largest
    footprint first and the smallest last to keep nested coverage visible. Requires
    the [vizx] extra.

    Args:
        layers:     list of ``(label, raster_bytes)`` — each a single-band mask.
        fig_w/fig_h: figure size in inches.
        max_pixels: decimate above this longest-edge pixel count.
        colors:     optional list of matplotlib colours, one per layer (defaults to
                    the ``tab10`` qualitative cycle).
        title:      axes title.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import sys

    import matplotlib

    if "matplotlib.pyplot" not in sys.modules and "MPLBACKEND" not in os.environ:
        if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
            matplotlib.use("Agg")
    from matplotlib import pyplot
    from matplotlib.colors import ListedColormap
    from matplotlib.patches import Patch
    from rasterio.io import MemoryFile
    from rasterio.plot import plotting_extent

    if colors is None:
        cmap = pyplot.get_cmap("tab10")
        colors = [cmap(i % 10) for i in range(len(layers))]

    fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))
    ax.set_facecolor("whitesmoke")
    handles = []
    for (label, raster_bytes), color in zip(layers, colors):
        with MemoryFile(bytes(raster_bytes)) as mf:
            with mf.open() as src:
                data, transform, _ = _decimated_read(src, max_pixels)
        band = data[0]
        if isinstance(band, np.ma.MaskedArray):
            covered = ~np.ma.getmaskarray(band)
        else:
            covered = np.ones(band.shape, dtype=bool)
        overlay = np.ma.MaskedArray(np.ones(band.shape, dtype="float32"), mask=~covered)
        ax.imshow(
            overlay,
            cmap=ListedColormap([color]),
            extent=plotting_extent(band, transform),
            vmin=0,
            vmax=1,
        )
        handles.append(Patch(facecolor=color, label=str(label)))
    ax.legend(handles=handles, loc="upper right", framealpha=0.9)
    ax.set_title(title)
    pyplot.show()


def plot_file(
    path,
    *,
    fig_w=10,
    fig_h=10,
    max_pixels=2000,
    composite="auto",
    bands=None,
    stretch="perband",
    fill=None,
):
    """Render a raster from disk (TIF, VRT, ...) with the plot_raster pipeline.

    Args:
        composite: ``"auto"`` (default) — 1 band → viridis; 3+ → RGB.
                   ``"depth"`` — per-pixel coverage depth rendered as viridis.
        bands:     None (default, all bands), or tuple of 1-based band indices
                   to select/reorder. Single band → viridis; 3+ → RGB.
        stretch:   "perband" (default), "shared", or (lo, hi) tuple.
        fill:      None (default), or scalar fill value to treat as NoData.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import rasterio

    # On Databricks, Volume/DBFS paths are often scheme-qualified (e.g.
    # "dbfs:/Volumes/.../x.tif" or "file:///Volumes/.../x.tif"), but the FUSE
    # mount rasterio reads is at the bare path ("/Volumes/.../x.tif"). Strip a
    # leading dbfs:/file: scheme so any of those forms works.
    path = str(path)
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    # file:// forms leave extra leading slashes (file:///p -> ///p); collapse.
    if path.startswith("//"):
        path = "/" + path.lstrip("/")

    with rasterio.open(path) as src:
        data, transform, scale = _decimated_read(src, max_pixels)
        _render(
            data,
            transform,
            title=f"File: {str(path).split('/')[-1]}",
            fig_w=fig_w,
            fig_h=fig_h,
            scale=scale,
            composite=composite,
            nodata=src.nodata,
            bands=bands,
            stretch=stretch,
            fill=fill,
        )


def _mosaic_grid_system(vrt_path):
    """Read GBX_GRIDSYSTEM from the first VRT member ('h3'/'quadbin'/'bng'), or None."""
    from databricks.labs.gbx.ds.raster import _parse_vrt_members, _read_gbx_member_tags

    try:
        members = _parse_vrt_members(vrt_path)
    except Exception:
        return None
    if not members:
        return None
    return _read_gbx_member_tags(members[0]).get("gridSystem")


def _overlay_h3_cells(ax, vrt_path, crs):
    """Draw each member's h3 cell boundary (from GBX_CELLID) as an outline."""
    import h3
    from matplotlib.patches import Polygon as MplPolygon

    from databricks.labs.gbx.ds.raster import _parse_vrt_members, _read_gbx_member_tags

    reproj = None
    if crs is not None and crs.to_epsg() != 4326:
        from databricks.labs.gbx.core.crs import get_transformer

        reproj = get_transformer("EPSG:4326", crs)

    for member in _parse_vrt_members(vrt_path):
        cellid = _read_gbx_member_tags(member).get("cellid")
        if not cellid:
            continue
        boundary = h3.cell_to_boundary(cellid)  # [(lat, lon), ...]
        lons = [pt[1] for pt in boundary]
        lats = [pt[0] for pt in boundary]
        # skip antimeridian-wrapping cells rather than draw a spurious polygon
        if max(lons) - min(lons) > 180.0:
            continue
        if reproj is not None:
            lons, lats = reproj.transform(lons, lats)
        ax.add_patch(
            MplPolygon(
                list(zip(lons, lats)),
                closed=True,
                fill=False,
                edgecolor="white",
                linewidth=0.6,
            )
        )


def _resolve_vrt_path(vrt):
    """Return a `.vrt` path from a direct path or a directory containing one."""
    import glob

    p = str(vrt)
    if os.path.isdir(p):
        hits = sorted(glob.glob(os.path.join(p, "*.vrt")))
        if len(hits) == 0:
            raise ValueError(f"plot_mosaic: no .vrt found in directory {p}")
        if len(hits) > 1:
            raise ValueError(
                f"plot_mosaic: {len(hits)} .vrt files in {p}; pass one explicitly"
            )
        return hits[0]
    return p


def plot_mosaic(
    vrt,
    *,
    bbox=None,
    bbox_crs=None,
    max_pixels=2000,
    resampling="bilinear",
    show_cells=False,
    fig_w=10,
    fig_h=10,
    composite="auto",
    bands=None,
    stretch="perband",
    fill=None,
    emphasis="blend",
    debug_mode=1,
):
    """Render a VRT mini-COG mosaic memory-safely (light tier).

    Opens the ``.vrt`` once and does a single decimated, windowed, masked read;
    GDAL selects each member's internal overview (or block-streams a decimated
    read of a single base level), so peak RAM is bounded by ``max_pixels`` — NOT
    the mosaic size. ``max_pixels`` is a READ CEILING, not a floor: it never
    upsamples beyond a tile's stored resolution.

    Args:
        vrt: path to a ``.vrt``, or a directory containing exactly one ``.vrt``.
        bbox: optional (minx, miny, maxx, maxy) viewport; in ``bbox_crs`` if
            given, else the mosaic's own CRS.
        bbox_crs: CRS of ``bbox`` — int SRID / "EPSG:x" / "ESRI:x" / WKT / PROJ4;
            None means the mosaic CRS.
        max_pixels: target render size (memory-safe read ceiling).
        resampling: on-the-fly downsample method ("bilinear" default; "nearest"
            for categorical rasters).
        show_cells: h3 only — overlay hex-cell outlines from member cellids.
        (remaining args mirror ``plot_raster``.)

    Returns None (matches ``plot_raster``/``plot_file``).
    """
    _validate_emphasis(emphasis)
    import matplotlib.pyplot as plt
    import rasterio

    vrt_path = _resolve_vrt_path(vrt)
    with rasterio.open(vrt_path) as src:
        window = None
        if bbox is not None:
            from rasterio.windows import Window, from_bounds

            minx, miny, maxx, maxy = bbox
            if bbox_crs is not None:
                from databricks.labs.gbx.core.crs import get_transformer, resolve_crs

                dst = src.crs
                src_crs = resolve_crs(bbox_crs)
                if src_crs != dst:
                    tr = get_transformer(src_crs, dst)
                    left, bottom, right, top = tr.transform_bounds(
                        minx, miny, maxx, maxy
                    )
                    if all(np.isfinite(v) for v in (left, bottom, right, top)):
                        minx, miny, maxx, maxy = left, bottom, right, top
                    else:
                        # Pathological global box: fall back to 2-corner transform
                        (minx, maxx), (miny, maxy) = tr.transform(
                            [minx, maxx], [miny, maxy]
                        )
            win = from_bounds(minx, miny, maxx, maxy, transform=src.transform)
            full = Window(col_off=0, row_off=0, width=src.width, height=src.height)
            try:
                window = win.intersection(full)
            except rasterio.errors.WindowError:
                window = None
            if window is None or window.width < 1 or window.height < 1:
                raise ValueError(
                    f"plot_mosaic: bbox {bbox} does not intersect the mosaic"
                )
        data, transform, scale = _read_windowed(
            src, max_pixels, window=window, resampling=resampling
        )
        nodata = src.nodata
        _, ax = plt.subplots(figsize=(fig_w, fig_h))
        _render(
            data,
            transform,
            title=f"Mosaic: {os.path.basename(vrt_path)}",
            fig_w=fig_w,
            fig_h=fig_h,
            scale=scale,
            composite=composite,
            nodata=nodata,
            emphasis=emphasis,
            ax=ax,
            bands=bands,
            stretch=stretch,
            fill=fill,
        )
        if show_cells or debug_mode >= 2:
            grid_system = _mosaic_grid_system(vrt_path)
        else:
            grid_system = None
        if show_cells:
            if grid_system != "h3":
                raise ValueError(
                    "show_cells is only supported for h3 mosaics; this mosaic "
                    f"is gridSystem={grid_system}"
                )
            _overlay_h3_cells(ax, vrt_path, src.crs)
        if debug_mode >= 2:
            from databricks.labs.gbx.core.crs import crs_to_canonical

            print(
                f"[plot_mosaic] gridSystem={grid_system} "
                f"crs={crs_to_canonical(src.crs)} out_shape={data.shape} "
                f"scale={scale:.3f}"
            )
    return None
