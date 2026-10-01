"""Inline PMTiles viewer for gbx.vizx.

Interactive path: a self-contained MapLibre GL JS + pmtiles.js HTML page
(CDN-loaded at pinned versions, SRI-hashed) with the archive base64-embedded
as an in-browser FileSource — no tile server, no remote range requests.
Interactive by default; when the embedded archive would exceed ``max_embed_mb``
and ``fallback`` is set (the default), decode tiles on the driver and reuse
plot_raster (raster) / plot_static (vector) over a contextily basemap
(``max_embed_mb=0`` forces this static path). Requires the [vizx] extra for the
static fallback. Driver-side only.

CDN pins and SRI hashes live in ``_maplibre.py`` (the single source of truth).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

from pmtiles.reader import MemorySource, all_tiles  # noqa: F401

if TYPE_CHECKING:
    import matplotlib.figure

_RASTER_TYPES = frozenset({"png", "jpeg", "webp", "avif"})


_SUPPORTED_TILE_TYPES = frozenset({"png", "jpeg", "webp", "avif", "mvt"})


def _is_raster_type(tile_type: str) -> bool:
    """True for image tile types (raster layer); False for mvt (vector)."""
    return tile_type in _RASTER_TYPES


def _strip_scheme(path: str) -> str:
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    if path.startswith("//"):
        path = "/" + path.lstrip("/")
    return path


def _archive_bytes(path_or_bytes: Union[str, bytes, bytearray]) -> bytes:
    """Read a .pmtiles path (Volume/DBFS scheme stripped) or pass bytes through."""
    if isinstance(path_or_bytes, (bytes, bytearray)):
        return bytes(path_or_bytes)
    with open(_strip_scheme(str(path_or_bytes)), "rb") as f:
        return f.read()


def _maybe_gunzip(payload: bytes) -> bytes:
    """Inflate a gzip-compressed tile payload (PMTiles tile_compression=gzip).

    PMTiles stores tiles per the archive's tile_compression and the reader yields
    the raw stored bytes, so gzipped tiles (gzip magic 0x1f 0x8b) must be inflated
    before decoding — rasterio's MemoryFile and mapbox_vector_tile.decode both
    reject gzip-wrapped bytes. Idempotent no-op on already-raw payloads.
    """
    import gzip

    if payload and payload[:2] == b"\x1f\x8b":
        return gzip.decompress(payload)
    return payload


def _decode_mvt_to_geoms(payload: bytes, z: int, x: int, y: int):
    """Decode one MVT tile to (shapely_geom, props) pairs in WGS-84 (EPSG:4326).

    MVT features are tile-local pixel coords [0, extent] with the NW origin
    (y down), matching what pyvx writes; invert that transform back to lon/lat
    using the same tile-bounds math.
    """
    import mapbox_vector_tile as mvt
    from shapely.geometry import shape
    from shapely.ops import transform

    from databricks.labs.gbx.pyvx._mvt import _tile_bounds

    decoded = mvt.decode(_maybe_gunzip(payload))
    out = []
    for layer in decoded.values():
        extent = layer.get("extent", 4096)
        minx, miny, maxx, maxy = _tile_bounds(z, x, y)
        sx = (maxx - minx) / extent
        sy = (maxy - miny) / extent

        def _to_lonlat(px, py, zc=None, _minx=minx, _maxy=maxy, _sx=sx, _sy=sy):
            return (_minx + px * _sx, _maxy - py * _sy)

        for feat in layer.get("features", []):
            geom = shape(feat["geometry"])
            if geom.is_empty:
                continue
            out.append((transform(_to_lonlat, geom), feat.get("properties", {})))
    return out


def plot_pmtiles(
    path_or_bytes,
    *,
    max_embed_mb=None,
    set_cell_max_output=True,
    fallback=True,
    interactive_fit="downzoom",
    style=None,
    debug_mode=1,
    emphasis="blend",
    **kw,
):
    """Render a .pmtiles archive inline in a Databricks notebook.

    Thin delegator to :func:`~databricks.labs.gbx.vizx._interactive.plot_interactive`
    with a single :func:`~databricks.labs.gbx.vizx._layers.pmtiles_layer` wrapping
    the archive. The ``style`` kwarg is forwarded to ``pmtiles_layer``; remaining
    ``**kw`` (``basemap``, ``center``, ``zoom``, etc.) are forwarded to
    ``plot_interactive``.

    Interactive path (default, when the archive fits within ``max_embed_mb``):
    a MapLibre GL JS page with the archive base64-embedded — no tile server, no
    remote range requests. Static fallback when oversized (``fallback=True``,
    the default) or ``max_embed_mb=0`` to force it.

    On Databricks Serverless a notebook cell caps output at 10 MB (20 MB max via
    ``%set_cell_max_output_size_in_mb``), and the cap counts the base64-rendered
    HTML (~4/3x the archive). An archive whose rendered size exceeds that ceiling
    cannot embed inline. ``interactive_fit`` controls how to still get an interactive
    experience — an "investment dial":

    - ``"downzoom"`` (default): fit the single archive to the budget, dropping the
      highest (densest) zoom levels only if it doesn't already fit (see
      :func:`~databricks.labs.gbx.vizx._pmtiles_autofit.autofit_archive`), then
      embed. A small archive is embedded as-is (no-op, no message); a large one is
      auto-fit and a one-line note reports the zoom levels dropped. One interactive
      map of the whole extent, at reduced detail only when necessary. Fast (no
      re-tiling). If even the coarsest zoom level exceeds the budget, falls back to
      static.
    - ``None``: no reduction. Embed if it fits; otherwise fall back to a static
      render (or raise if ``fallback=False``).
    - ``"all"``: invest more for full detail — spatially shard into per-region
      sub-archives, each under budget, rendered as a multi-shard interactive
      experience. **Not yet implemented** (planned); raises
      :exc:`NotImplementedError`.

    For an archive of any size with zero embed cost, stage it at an ``https://``
    URL and pass that URL to ``pmtiles_layer`` / ``plot_pmtiles`` — it streams
    remotely and is always interactive regardless of the cell cap.

    ``emphasis="data"`` styles the archive to pop against the
    full-strength basemap (firmer fill, contrasting dark outline, full raster
    opacity); ``"blend"`` (default) reproduces the prior soft composite. Forwarded to
    ``plot_interactive``.
    """
    if interactive_fit not in (None, "downzoom", "all"):
        raise ValueError(
            f"plot_pmtiles: interactive_fit must be None, 'downzoom', or 'all'; "
            f"got {interactive_fit!r}"
        )
    if interactive_fit == "all":
        raise NotImplementedError(
            "plot_pmtiles: interactive_fit='all' (multi-shard full-detail interactive "
            "rendering) is not yet implemented. Use interactive_fit='downzoom' for a "
            "reduced-detail interactive map now, stage the archive at an https:// "
            "URL for full detail at zero embed cost, or pre-shard your data."
        )

    from databricks.labs.gbx.vizx._interactive import plot_interactive
    from databricks.labs.gbx.vizx._layers import pmtiles_layer
    from databricks.labs.gbx.vizx._maplibre import _emit, _resolve_embed_budget

    # Resolve the embed budget up front so the downzoom autofit + the audit all use
    # the same value (6 MB when set_cell_max_output raises the cap, else 3 MB).
    max_embed_mb = _resolve_embed_budget(max_embed_mb, set_cell_max_output)

    archive = path_or_bytes
    if interactive_fit == "downzoom" and max_embed_mb and max_embed_mb > 0:
        # Invest-little path: auto-fit by down-zooming until the archive's
        # rendered size is within budget, then embed the reduced archive.
        from databricks.labs.gbx.vizx._pmtiles_autofit import autofit_archive

        raw = _archive_bytes(path_or_bytes)
        reduced, report = autofit_archive(raw, max_embed_mb=max_embed_mb)
        if report["dropped_zooms"]:
            # Concise one-line emit: we down-zoomed to make it fit. A small archive
            # that already fits drops nothing and stays silent.
            _emit(
                f"[vizx] downzoomed to fit {max_embed_mb:.0f} MB: dropped zoom level(s) "
                f"{report['dropped_zooms']} (kept z<={report['kept_max_zoom']}); "
                "stage at an https:// URL for full-detail interactivity.",
                level=1,
                debug_mode=debug_mode,
            )
        if debug_mode >= 2:
            _emit(f"[vizx]   autofit report: {report}", level=2, debug_mode=debug_mode)
        archive = reduced

    return plot_interactive(
        [pmtiles_layer(archive, style=style)],
        max_embed_mb=max_embed_mb,
        set_cell_max_output=set_cell_max_output,
        fallback=fallback,
        debug_mode=debug_mode,
        emphasis=emphasis,
        **kw,
    )


def plot_point_cloud(
    data,
    *,
    color="rgb",
    cmap="viridis",
    point_size=2.0,
    max_points=300_000,
    max_embed_mb=None,
    elev=30.0,
    azim=-60.0,
    background="#111111",
    title=None,
    crs=None,
    seed=0,
) -> "str | None | matplotlib.figure.Figure":
    """Render a point cloud as an interactive deck.gl orbit view or a static 3D scatter.

    Thin composer over :func:`~databricks.labs.gbx.vizx._pointcloud.load_point_cloud`
    (normalizes ``data`` -- a LAS/LAZ path, GeoDataFrame, or x/y/z DataFrame -- to
    decimated ``x, y, z, values, rgb, src_crs`` arrays), then routes to one of two
    render backends via the same embed-budget machinery as
    :func:`~databricks.labs.gbx.vizx._interactive.plot_interactive`:
    :func:`~databricks.labs.gbx.vizx._pointcloud_html.build_pointcloud_html`
    (interactive deck.gl ``PointCloudLayer``) or
    :func:`~databricks.labs.gbx.vizx._pointcloud_static.render_point_cloud_3d`
    (static matplotlib ``Axes3D`` scatter).

    ``color`` selects the per-point color source: ``"rgb"`` (default) uses true
    color from the source when present -- falling back to ``cmap`` on elevation
    (with a warning) when it isn't; ``"z"`` colors by elevation via ``cmap``; any
    other value names a column in ``data`` to color by via ``cmap``.

    ``max_embed_mb`` controls the interactive/static split exactly as in
    ``plot_interactive``: ``None`` (default) resolves to the standard embed
    budget, ``0`` forces the static render, and a built HTML page over budget
    falls back to static automatically.

    Returns:
        In a Databricks/IPython notebook, on the interactive path: calls
        ``displayHTML`` and returns ``None``. Outside a notebook: returns the
        HTML string. On the static path (``max_embed_mb=0`` or over budget):
        returns the :class:`matplotlib.figure.Figure` from
        ``render_point_cloud_3d``.
    """
    import warnings

    from databricks.labs.gbx.vizx._interactive import (
        _format_audit_line,
        _notebook_display_html,
    )
    from databricks.labs.gbx.vizx._maplibre import _emit, _resolve_embed_budget
    from databricks.labs.gbx.vizx._pointcloud import load_point_cloud
    from databricks.labs.gbx.vizx._pointcloud_html import build_pointcloud_html
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    column = None if color in ("rgb", "z") else color
    x, y, z, values, rgb, _src_crs = load_point_cloud(
        data, column=column, max_points=max_points, crs=crs, seed=seed
    )

    if color == "rgb" and rgb is None:
        warnings.warn(
            "plot_point_cloud: no RGB in source; falling back to elevation cmap",
            stacklevel=2,
        )

    def _static():
        return render_point_cloud_3d(
            x,
            y,
            z,
            rgb=rgb,
            values=values,
            cmap=cmap,
            point_size=point_size,
            elev=elev,
            azim=azim,
            background=background,
            title=title,
        )

    max_embed_mb = _resolve_embed_budget(max_embed_mb, True)
    if max_embed_mb == 0:
        return _static()

    html = build_pointcloud_html(
        x,
        y,
        z,
        rgb=rgb,
        values=values,
        cmap=cmap,
        point_size=point_size,
        background=background,
        title=title,
    )
    embed_bytes = len(html.encode("utf-8"))
    fits = embed_bytes <= max_embed_mb * 1_048_576
    audit = {
        "layers": [{"label": "point_cloud", "embed_bytes": embed_bytes}],
        "total_embed_bytes": embed_bytes,
        "fits": fits,
        "verdict": "embed" if fits else "static",
    }
    _emit(_format_audit_line(audit, max_embed_mb), level=1)

    if not fits:
        return _static()

    dh = _notebook_display_html()
    if dh is not None:
        dh(html)
        return None
    return html
