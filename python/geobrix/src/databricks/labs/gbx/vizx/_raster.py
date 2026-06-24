"""Raster rendering pipeline for gbx.vizx (decimation + percentile stretch).

Ported from notebooks/examples/eo-series/library.py. matplotlib/rasterio are
lazy-imported inside the public plotters (Task 3); the numeric helpers here use
only numpy and the rasterio dataset passed in.
"""

import os

import numpy as np


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


def _needs_percentile_stretch(data):
    """True when data is integer-typed with a max above matplotlib's RGB int 255."""
    if not np.issubdtype(data.dtype, np.integer):
        return False
    mx = np.ma.max(data) if isinstance(data, np.ma.MaskedArray) else data.max()
    if mx is np.ma.masked:
        return False
    return int(mx) > 255


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


def _render(
    data, transform, *, title, fig_w, fig_h, scale, composite="auto", nodata=None
):
    """Stretch when needed, then plot via rasterio.plot.show (Agg-safe).

    Args:
        composite: ``"auto"`` — 1 band → viridis; 3+ → RGB (default).
                   ``"depth"`` — render per-pixel band coverage count as viridis;
                   depth==0 (no band covers the pixel) is masked transparent.
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
        fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))
        show(depth_masked, ax=ax, transform=transform, cmap="viridis")
        ax.set_title(full_title)
        pyplot.show()
        return

    if _needs_percentile_stretch(data):
        data = _percentile_stretch(data)
    fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))
    if data.shape[0] == 1:
        # Render the single band with ax.imshow rather than rasterio.plot.show:
        # show() renders a constant-valued band (e.g. an H3 presence mask, all 1.0)
        # as blank and ignores an explicit vmin/vmax, whereas imshow honors both the
        # clim and the masked array (NoData -> transparent over the facecolor).
        from rasterio.plot import plotting_extent

        band = data[0]
        valid = (
            band.compressed()
            if isinstance(band, np.ma.MaskedArray)
            else np.asarray(band).ravel()
        )
        ax.set_facecolor("whitesmoke")
        clim = _single_band_clim(valid)
        kw = {"cmap": "viridis"}
        if clim is not None:
            kw["vmin"], kw["vmax"] = clim
        ax.imshow(band, extent=plotting_extent(band, transform), **kw)
    else:
        show(data, ax=ax, transform=transform)
    full_title = f"{title} (scale 1/{round(scale, 1)}x)" if scale > 1 else title
    ax.set_title(full_title)
    pyplot.show()


def plot_raster(raster_bytes, *, fig_w=10, fig_h=10, max_pixels=2000, composite="auto"):
    """Render a raster from in-memory bytes (e.g. a tile's `raster` field).

    Auto-decimates above max_pixels; integer rasters whose values exceed 255
    (typical EO UInt16) get a per-band 2-98% percentile stretch. Single-band ->
    viridis; multi-band -> RGB. Requires the [vizx] extra.

    Args:
        composite: ``"auto"`` (default) — 1 band → viridis; 3+ → RGB.
                   ``"depth"`` — render per-pixel coverage depth (count of bands
                   covering each pixel) as a viridis gradient; uncovered pixels
                   are masked transparent.  Useful for multi-band presence masks
                   where an RGB composite would appear mostly black.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    from rasterio.io import MemoryFile

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


def plot_file(path, *, fig_w=10, fig_h=10, max_pixels=2000, composite="auto"):
    """Render a raster from disk (TIF, VRT, ...) with the plot_raster pipeline.

    Args:
        composite: ``"auto"`` (default) — 1 band → viridis; 3+ → RGB.
                   ``"depth"`` — per-pixel coverage depth rendered as viridis.
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
        )
