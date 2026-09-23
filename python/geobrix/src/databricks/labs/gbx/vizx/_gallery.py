"""Contact-sheet / small-multiples viewer for an image collection (gbx.vizx).

``plot_gallery`` is the "small-multiples of plot_file": preview a COLLECTION
of independent images/rasters as a thumbnail grid (reading-order, NOT
spatially placed — that is ``plot_mosaic``'s job). It graduates a drone-image
contact-sheet from a notebook into a reusable viewer. Requires the ``[vizx]``
extra.
"""

import os
from math import ceil
from os.path import basename
from pathlib import Path

# ---------------------------------------------------------------------------
# Pure helpers (tested independently)
# ---------------------------------------------------------------------------


def _resolve_sources(source, extensions):
    """Return a sorted list of image paths from *source*.

    Parameters
    ----------
    source : str | Path | sequence
        - A directory path → recursive glob for files whose lower-cased
          suffix is in *extensions*.
        - A sequence of paths → list them as-is.
        - A single file path → ``[that]``.
    extensions : tuple/sequence of str
        Lower-cased suffixes to include (e.g. ``(".jpg", ".tif")``).

    Strips a leading ``dbfs:``/``file:`` scheme and collapses ``//``
    (mirrors ``plot_file``).  Returns ``[]`` when nothing matches.
    """

    def _clean(p):
        p = str(p)
        for scheme in ("dbfs:", "file:"):
            if p.startswith(scheme):
                p = p[len(scheme) :]
                break
        if p.startswith("//"):
            p = "/" + p.lstrip("/")
        return p

    # Sequence of paths (not a bare str/Path)
    if not isinstance(source, (str, Path)):
        return [_clean(p) for p in source]

    source = _clean(source)
    exts = {e.lower() for e in extensions}

    if os.path.isdir(source):
        hits = []
        for root, _, files in os.walk(source):
            for f in files:
                if os.path.splitext(f)[1].lower() in exts:
                    hits.append(os.path.join(root, f))
        return sorted(hits)

    # Single file
    return [source]


def _select_images(paths, mode, limit):
    """Select a subset of *paths* based on *mode*.

    Parameters
    ----------
    paths : list[str]
    mode  : "all" | "limit" | "sample"
        ``"all"``    — return all paths unchanged.
        ``"limit"``  — return ``paths[:limit]``.
        ``"sample"`` — evenly-spaced: ``step = max(1, len(paths)//limit)``
                       then ``paths[::step][:limit]``.
    limit : int
        Maximum count for ``"limit"`` and ``"sample"`` modes.

    Raises
    ------
    ValueError
        If *mode* is not one of the three recognised values.
    """
    if mode == "all":
        return list(paths)
    if mode == "limit":
        return paths[:limit]
    if mode == "sample":
        step = max(1, len(paths) // limit)
        return paths[::step][:limit]
    raise ValueError(f"mode must be 'all', 'limit', or 'sample'; got {mode!r}")


def _pick_renderer(path, renderer):
    """Return ``'photo'`` or ``'raster'`` for *path* and *renderer* hint.

    When ``renderer != "auto"`` the value is returned as-is.  Otherwise
    ``'raster'`` for suffixes in ``{.tif, .tiff, .vrt, .cog}`` and
    ``'photo'`` for everything else.
    """
    if renderer != "auto":
        return renderer
    suffix = os.path.splitext(str(path))[1].lower()
    if suffix in {".tif", ".tiff", ".vrt", ".cog"}:
        return "raster"
    return "photo"


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def plot_gallery(  # noqa: C901 — intentional single-function layout
    source,
    *,
    mode="sample",
    limit=24,
    cols=6,
    rows=None,
    renderer="auto",
    thumb_px=256,
    labels="name",
    title=None,
    extensions=(".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp", ".vrt"),
    bands=None,
    stretch="perband",
    fill=None,
    cmap=None,
    cell_w=2.2,
    cell_h=1.7,
):
    """Preview a collection of images/rasters as a thumbnail grid.

    This is the *small-multiples of plot_file*: it shows a reading-order
    thumbnail grid of independent images. It is NOT a georeferenced mosaic;
    for spatially-placed rendering use ``plot_mosaic``.

    Three selection modes control which thumbnails are shown:

    - ``"all"``    — every image found in the collection.
    - ``"limit"``  — the first *limit* images (sorted / reading order).
    - ``"sample"`` (default) — *limit* images evenly spaced across the full
      set, giving a representative survey without head-bias.

    ``renderer="auto"`` routes JPEG/PNG/BMP/WebP images through Pillow
    (``"photo"``) and GeoTIFF/VRT files through ``plot_file`` with its
    percentile-stretch pipeline (``"raster"``).

    Requires the ``[vizx]`` extra.

    Parameters
    ----------
    source : str | Path | sequence[str | Path]
        A directory, a single file path, or an explicit list of paths.
    mode : "sample" | "all" | "limit"
        Sub-selection strategy (default ``"sample"``).
    limit : int
        Cap for ``"limit"`` and ``"sample"`` modes (default 24).
    cols : int
        Thumbnail columns (default 6); auto-reduced when fewer images found.
    rows : int | None
        Rows; ``None`` → ``ceil(n / cols)``.
    renderer : "auto" | "photo" | "raster"
        Force a render path, or let suffix decide (``"auto"``).
    thumb_px : int
        Per-cell maximum long-edge in pixels (default 256).
    labels : "name" | None | sequence[str]
        Cell caption: ``"name"`` → basename; ``None`` → no caption;
        a sequence → one label per selected image.
    title : str | None
        Figure suptitle. ``None`` → auto ``"N of M images"``.
    extensions : tuple[str, ...]
        File-suffix filter for directory sources.
    bands, stretch, fill, cmap
        Passed through to ``plot_file`` for raster cells.
    cell_w, cell_h : float
        Per-cell figure size in inches (default 2.2 × 1.7).

    Returns
    -------
    matplotlib.figure.Figure or None
        The completed figure, or ``None`` when no images were found.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    total = _resolve_sources(source, extensions)
    sel = _select_images(total, mode, limit)

    if not sel:
        print(f"[plot_gallery] [skip] no images found in {source!r}")
        return None

    n = len(sel)
    effective_cols = max(1, min(cols, n))
    effective_rows = rows if rows is not None else ceil(n / effective_cols)

    import sys

    import matplotlib

    # Agg guard — same pattern as _raster.py
    if "matplotlib.pyplot" not in sys.modules and "MPLBACKEND" not in os.environ:
        if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
            matplotlib.use("Agg")
    import numpy as _np
    from matplotlib import pyplot

    from databricks.labs.gbx.vizx._raster import plot_file

    fig, axes_raw = pyplot.subplots(
        effective_rows,
        effective_cols,
        figsize=(effective_cols * cell_w, effective_rows * cell_h),
    )

    # Normalise to a flat list regardless of shape (scalar, 1-D, or 2-D)
    axes_flat = _np.asarray(axes_raw).ravel().tolist()

    for i, (ax, path) in enumerate(zip(axes_flat, sel)):
        name = basename(str(path))
        _rend = _pick_renderer(path, renderer)
        try:
            if _rend == "photo":
                from PIL import Image

                with Image.open(path) as im:
                    im.draft("RGB", (thumb_px, thumb_px))
                    im.thumbnail((thumb_px, thumb_px))
                    ax.imshow(im)
            else:
                plot_file(
                    path,
                    ax=ax,
                    max_pixels=thumb_px,
                    bands=bands,
                    stretch=stretch,
                    fill=fill,
                    cmap=cmap,
                    title=None,
                )
        except Exception as exc:  # noqa: BLE001
            ax.text(
                0.5,
                0.5,
                f"err: {name}\n{type(exc).__name__}",
                ha="center",
                va="center",
                fontsize=6,
                transform=ax.transAxes,
            )
        ax.axis("off")
        # Caption
        if labels == "name":
            ax.set_title(name, fontsize=6)
        elif labels is not None:
            ax.set_title(str(labels[i]), fontsize=6)
        # else: labels is None → no title

    # Blank unused trailing axes
    for ax in axes_flat[n:]:
        ax.axis("off")

    sup = title if title is not None else f"{n} of {len(total)} images"
    fig.suptitle(sup, fontsize=8)

    pyplot.tight_layout()
    pyplot.show()
    return fig
