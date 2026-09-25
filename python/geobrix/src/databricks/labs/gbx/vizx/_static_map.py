"""Static (non-interactive) map rendering for gbx.vizx.

plot_static renders Spark- or GeoPandas-derived geometries / DGGS cells over a
contextily basemap as a static matplotlib figure -- the GitHub-renderable
counterpart to GeoDataFrame.explore(). Requires the [vizx] extra.
"""

import warnings

_GEOM_COL_CANDIDATES = ("wkt", "geometry", "geom", "ewkt", "wkb", "ewkb")
_CELL_COL_CANDIDATES = ("cellid", "cell", "cell_id", "h3", "quadbin", "bng", "index")

# Sentinel marking a styling kwarg the caller did NOT pass, so emphasis controls
# its default. An explicit value (including None) replaces the sentinel and wins.
_UNSET = object()

# ---------------------------------------------------------------------------
# emphasis styling defaults (static / matplotlib tier)
# ---------------------------------------------------------------------------
#
# ``emphasis="data"`` makes a newly-added data layer pop against the
# full-strength basemap; ``emphasis="blend"`` (default) reproduces the prior soft render.
# These set DEFAULTS only -- an explicit user kwarg (alpha/edgecolor/markersize/
# linewidth) always wins.
_STATIC_EMPHASIS = {
    "data": {
        "alpha": 0.85,
        "edgecolor": "#222222",
        "linewidth": 0.6,
        # point/line emphasis: bump marker size and line width, dark point edge.
        "markersize": 30,
        "point_edgecolor": "#222222",
        "line_linewidth": 2.0,
        # raster (COG / ndarray): vivid colormap at full strength.
        "cmap": "viridis",
        "raster_alpha": 1.0,
    },
    "blend": {
        # EXACTLY prior behavior: alpha 0.8, edgecolor "face" (no bold outline), no
        # forced linewidth/markersize, raster imshow at alpha 1.0 (no prior alpha).
        "alpha": 0.8,
        "edgecolor": "face",
        "linewidth": None,
        "markersize": None,
        "point_edgecolor": None,
        "line_linewidth": None,
        "cmap": "viridis",
        "raster_alpha": 1.0,
    },
}


def _validate_emphasis(emphasis):
    if emphasis not in _STATIC_EMPHASIS:
        raise ValueError(f"emphasis must be 'data' or 'blend'; got {emphasis!r}")
    return emphasis


def _geom_strategy(dtype):
    """Decode strategy for a Spark geometry column's dataType.

    Returns 'native' (Databricks GEOMETRY/GEOGRAPHY -> st_asbinary in Spark),
    'binary' (WKB/EWKB), or 'string' (WKT/EWKT). Raises ValueError otherwise.
    """
    name = dtype.typeName().lower()
    simple = dtype.simpleString().lower()
    if (
        "geometry" in name
        or "geography" in name
        or "geometry" in simple
        or "geography" in simple
    ):
        return "native"
    if name == "binary":
        return "binary"
    if name == "string":
        return "string"
    raise ValueError(
        f"plot_static: geometry column has unsupported type {dtype.simpleString()!r}; "
        "coerce it to WKB/WKT first (e.g. st_asbinary(col) / st_astext(col)), or "
        "pass grid_system= for DGGS cell ids."
    )


def _detect_geom_col(df, grid_system):
    """Auto-detect the geometry/cell column name. Raise ValueError if ambiguous."""
    cols = df.columns
    lower = {c.lower(): c for c in cols}
    if grid_system is not None:
        for cand in _CELL_COL_CANDIDATES:
            if cand in lower:
                return lower[cand]
        if len(cols) == 1:
            return cols[0]
        raise ValueError(
            "plot_static: could not auto-detect the cell-id column; pass "
            f"geom_col= explicitly (columns: {cols})."
        )
    for f in df.schema.fields:
        s = f.dataType.simpleString().lower()
        if "geometry" in s or "geography" in s:
            return f.name
    for cand in _GEOM_COL_CANDIDATES:
        if cand in lower:
            return lower[cand]
    raise ValueError(
        "plot_static: could not auto-detect the geometry column; pass geom_col= "
        f"explicitly (columns: {cols})."
    )


def _collect_limited(df, max_rows, sample_seed=None):
    """Collect a Spark DataFrame to pandas with a truncate-and-warn row guard.

    ``sample_seed=None`` keeps the first ``max_rows`` rows (``.limit``); an int
    draws a reproducible seeded sample. Delegates to the shared adapter helper so
    plotters and adapters share one capping implementation.
    """
    from databricks.labs.gbx.vizx._vector import _collect_capped

    return _collect_capped(df, max_rows, sample_seed, "plot_static")


def _h3_boundary(cell):
    import h3
    from shapely.geometry import Polygon

    idx = cell if isinstance(cell, str) else h3.int_to_str(int(cell))
    ring = h3.cell_to_boundary(idx)  # (lat, lng) pairs in h3 v4
    return Polygon([(lng, lat) for lat, lng in ring])


def _h3_boundaries(values):
    return [_h3_boundary(c) for c in values]


def _quadbin_boundaries(values):
    # Drive the lightweight (pygx) scalar cell->WKB on the driver, per cell,
    # exactly as the gbx_quadbin_aswkb pandas_udf does. Quadbin cells are bigint
    # (lon/lat tile bounds, EPSG:4326).
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pygx import _quadbin

    return [parse_geom(_quadbin.as_wkb(int(c))) for c in values]


def _bng_boundaries(values):
    # Mirror gbx_bng_aswkb: _bng.parse() decodes the STRING cellid to its int
    # form, then cell_aswkb builds the cell polygon in EPSG:27700.
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pygx import _bng

    return [parse_geom(_bng.cell_aswkb(_bng.parse(c))) for c in values]


def _custom_boundaries(values, grid_conf):
    # Custom grids are not global: a cell id only resolves with the grid's
    # config (origin/extents/cell sizes/srid). Reuse the lightweight
    # _custom.conf_from_row + cell_aswkb (the same path as gbx_custom_cellaswkb).
    # Returns (geometries, srid); srid<=0 means the grid declares no CRS.
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pygx import _custom

    if grid_conf is None:
        raise ValueError(
            "plot_static: grid_system='custom' requires grid_conf= -- the grid "
            "spec (Row/dict with bound_x_min/max, bound_y_min/max, cell_splits, "
            "root_cell_size_x/y, srid) that defines the custom grid."
        )
    conf = _custom.conf_from_row(grid_conf)
    srid = conf.srid if conf.srid and conf.srid > 0 else None
    geoms = [parse_geom(_custom.cell_aswkb(conf, int(c))) for c in values]
    return geoms, srid


# grid_system -> (cell-ids -> boundary geometries, source EPSG). h3/quadbin are
# lon/lat (4326); BNG is British National Grid eastings/northings (27700).
# 'custom' is handled separately in _resolve_cells: it needs grid_conf and its
# CRS comes from the grid spec, not a fixed EPSG.
_GRID_DISPATCH = {
    "h3": (_h3_boundaries, 4326),
    "quadbin": (_quadbin_boundaries, 4326),
    "bng": (_bng_boundaries, 27700),
}

_GRID_SYSTEMS = (*_GRID_DISPATCH, "custom")


def _resolve_cells(data, col, grid_system, max_rows, grid_conf, sample_seed=None):
    """DGGS cell-id column -> boundary-polygon GeoDataFrame in the grid's CRS."""
    import geopandas as gpd

    if grid_system not in _GRID_SYSTEMS:
        raise ValueError(
            f"plot_static: grid_system={grid_system!r} is not one of "
            f"{sorted(_GRID_SYSTEMS)} or None."
        )
    pdf = _collect_limited(data, max_rows, sample_seed)
    cells = pdf[col].tolist()
    if grid_system == "custom":
        geometry, srid = _custom_boundaries(cells, grid_conf)
    else:
        boundaries, srid = _GRID_DISPATCH[grid_system]
        geometry = boundaries(cells)
    return gpd.GeoDataFrame(pdf.drop(columns=[col]), geometry=geometry, crs=srid)


def _resolve_gdf(
    data, geom_col, grid_system, max_rows, srid, grid_conf=None, sample_seed=None
):
    """Spark DataFrame or GeoDataFrame -> geopandas.GeoDataFrame (EPSG:4326 or srid)."""
    import geopandas as gpd

    if isinstance(data, gpd.GeoDataFrame):
        return data

    col = geom_col or _detect_geom_col(data, grid_system)

    if grid_system is not None:
        return _resolve_cells(data, col, grid_system, max_rows, grid_conf, sample_seed)

    from databricks.labs.gbx._geom import parse_geom

    field = data.schema[col]
    strategy = _geom_strategy(field.dataType)
    work = data
    if strategy == "native":
        from pyspark.sql.functions import expr

        work = data.withColumn(col, expr(f"st_asbinary(`{col}`)"))
        if srid is None and "geography" in field.dataType.simpleString().lower():
            srid = 4326

    pdf = _collect_limited(work, max_rows, sample_seed)
    geoms = [parse_geom(v) for v in pdf[col]]
    pdf = pdf.drop(columns=[col])
    return gpd.GeoDataFrame(pdf, geometry=geoms, crs=(srid or 4326))


def _draw_one_layer(
    lyr,
    ax,
    *,
    max_rows=10_000,
    sample_seed=None,
    srid=None,
    legend=True,
    emphasis="blend",
):
    """Draw a single Layer onto an existing matplotlib Axes (already in EPSG:3857).

    Dispatches by lyr.kind:
    - 'vector' / 'grid': resolve via _resolve_gdf, reproject to 3857, plot.
    - 'raster': delegate to plot_cog (Task 2) with basemap=False.

    ``emphasis`` sets the per-layer styling defaults: ``"data"`` pops the layer
    (dark outline, firmer alpha, full raster strength); ``"blend"`` (default) keeps the
    prior soft render. Explicit per-layer style attributes (opacity/color/width)
    always win.
    """
    em = _STATIC_EMPHASIS[emphasis]
    if lyr.kind in ("vector", "grid"):
        gdf = _resolve_gdf(
            lyr.data,
            lyr.geom_col if lyr.kind == "vector" else lyr.cellid_col,
            lyr.grid_system if lyr.kind == "grid" else None,
            max_rows,
            srid,
            lyr.grid_conf,
            sample_seed,
        )
        plot_gdf = gdf.to_crs(3857) if gdf.crs is not None else gdf
        geom_kinds = set(plot_gdf.geom_type.dropna().unique())
        is_pointish = bool(geom_kinds & {"Point", "MultiPoint"})
        is_lineish = bool(geom_kinds & {"LineString", "MultiLineString"})
        # User opacity wins over the emphasis alpha default.
        alpha = lyr.opacity if lyr.opacity is not None else em["alpha"]
        edgecolor = (
            (em["point_edgecolor"] or "face") if is_pointish else em["edgecolor"]
        )
        kwargs = {"ax": ax, "alpha": alpha, "edgecolor": edgecolor}
        if lyr.color is not None:
            # Scalar color: geopandas disallows 'cmap' + 'color' together.
            kwargs["color"] = lyr.color
        else:
            kwargs["cmap"] = lyr.cmap
        if lyr.column is not None:
            kwargs["column"] = lyr.column
            kwargs["legend"] = legend
        # Line width: explicit lyr.width wins; else the emphasis bump (bolder for
        # lines, a thin outline for polygons in data mode).
        if lyr.width is not None:
            kwargs["linewidth"] = lyr.width
        else:
            em_lw = em["line_linewidth"] if is_lineish else em["linewidth"]
            if em_lw is not None:
                kwargs["linewidth"] = em_lw
        if is_pointish and em["markersize"] is not None:
            kwargs["markersize"] = em["markersize"]
        if not lyr.fill:
            kwargs["facecolor"] = "none"
        plot_gdf.plot(**kwargs)
    elif lyr.kind == "raster":
        import numpy as np

        if isinstance(lyr.data, np.ndarray):
            # A decoded image array (e.g. a PMTiles raster overview tile, or any
            # ndarray raster_layer) — show it directly. Raw web-map tiles are not
            # georeferenced, so they cannot go through plot_cog/rasterio. User
            # opacity wins; else emphasis sets the raster strength.
            ras_alpha = lyr.opacity if lyr.opacity is not None else em["raster_alpha"]
            ax.imshow(lyr.data, alpha=ras_alpha)
            ax.set_axis_off()
        else:
            from databricks.labs.gbx.vizx._cog import plot_cog

            plot_cog(lyr.data, band=lyr.band, basemap=False, ax=ax, emphasis=emphasis)
    elif lyr.kind == "pmtiles":
        warnings.warn(
            "plot_static: 'pmtiles' layers are not rendered by the static compositor "
            "and produce no output here. Use plot_interactive for pmtiles, or let the "
            ">64MB static fallback decode them to a raster first.",
            stacklevel=2,
        )


def _resolve_static_style(plot_gdf, em, *, cmap, alpha, edgecolor, markersize):
    """Resolve effective matplotlib draw style: an explicit (non-_UNSET) kwarg wins,
    else the emphasis profile ``em`` keyed by geometry kind. Returns
    ``(cmap, alpha, edgecolor, markersize, linewidth)``; ``linewidth`` is ``None``
    unless the emphasis profile declares one (polygon/point outline vs line bump)."""
    geom_kinds = set(plot_gdf.geom_type.dropna().unique())
    is_pointish = bool(geom_kinds & {"Point", "MultiPoint"})
    is_lineish = bool(geom_kinds & {"LineString", "MultiLineString"})

    eff_cmap = "viridis" if cmap is _UNSET else cmap
    eff_alpha = em["alpha"] if alpha is _UNSET else alpha
    if edgecolor is not _UNSET:
        eff_edgecolor = edgecolor
    elif is_pointish:
        eff_edgecolor = em["point_edgecolor"] or "face"
    else:
        eff_edgecolor = em["edgecolor"]
    if markersize is not _UNSET:
        eff_markersize = markersize
    else:
        eff_markersize = em["markersize"] if is_pointish else None
    em_lw = em["line_linewidth"] if is_lineish else em["linewidth"]
    return eff_cmap, eff_alpha, eff_edgecolor, eff_markersize, em_lw


def plot_static(
    data,
    *,
    column=None,
    geom_col=None,
    grid_system=None,
    grid_conf=None,
    max_rows=10_000,
    sample_seed=None,
    srid=None,
    cmap=_UNSET,
    legend=True,
    basemap=True,
    basemap_source=None,
    alpha=_UNSET,
    edgecolor=_UNSET,
    fill=True,
    markersize=_UNSET,
    title=None,
    fig_w=10,
    fig_h=10,
    ax=None,
    emphasis="blend",
    debug_mode=1,
):
    """Render geometries / DGGS cells over a basemap as a static figure.

    ``data`` accepts a Spark DataFrame, a geopandas.GeoDataFrame, a
    :class:`~databricks.labs.gbx.vizx.Layer`, or a ``list[Layer]`` for
    multi-layer compositing. When a list of Layers is given every layer is drawn
    in order on a single :class:`matplotlib.axes.Axes` and the function returns
    that axes.

    Geometry columns accept WKT/EWKT/WKB/EWKB and native GEOMETRY/GEOGRAPHY
    (decoded via the shared parse_geom); set ``grid_system`` to treat the column
    as DGGS cell ids instead -- ``'h3'`` / ``'quadbin'`` (lon/lat) or ``'bng'``
    (EPSG:27700), string or long. ``grid_system='custom'`` additionally requires
    ``grid_conf=`` (the grid-spec Row/dict that defines the custom grid); its CRS
    comes from the grid's ``srid`` (a custom grid with ``srid<=0`` has no CRS, so
    the basemap is skipped). The contextily basemap is rendered when
    ``basemap=True``; any failure (no egress / missing dep) degrades to a
    warning and a basemap-less render. Returns the matplotlib Axes; pass it back
    via ``ax=`` to overlay layers -- every layer is reprojected to Web Mercator
    (EPSG:3857), so a ``basemap=False`` overlay aligns with a basemap layer on
    the same axes.

    ``plot_static`` does not call ``pyplot.show()``; in a notebook the figure
    auto-displays at cell end with all overlaid layers present, and a script can
    call ``plt.show()`` itself. ``sample_seed`` (Spark-only) selects how the
    ``max_rows`` cap is filled: ``None`` (default) takes the first ``max_rows``
    rows; an int draws a reproducible seeded sample. Pass ``fill=False`` to draw
    geometries as
    outlines only (no face) -- e.g. a canvas/footprint boundary over a filled
    choropleth; combine with ``edgecolor`` to colour the outline.

    ``emphasis`` controls the default styling: ``"data"`` makes the
    layer pop against the full-strength basemap (a dark outline ~#222222,
    ``linewidth`` ~0.6, ``alpha`` ~0.85, bumped point markers); ``"blend"``
    reproduces the prior soft render (``edgecolor="face"``, ``alpha`` 0.8, no bold
    outline). Explicit ``alpha`` / ``edgecolor`` / ``markersize`` / ``cmap`` /
    ``column`` / ``color`` kwargs always override the emphasis defaults.
    ``debug_mode`` mirrors the interactive entrypoints: ``0`` silent, ``1``
    (default) concise notes, ``2`` prints the chosen emphasis style values.
    Requires the [vizx] extra.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available
    from databricks.labs.gbx.vizx._layers import Layer, as_layers
    from databricks.labs.gbx.vizx._maplibre import _emit

    _validate_emphasis(emphasis)
    assert_viz_available()

    import matplotlib.pyplot as plt

    em = _STATIC_EMPHASIS[emphasis]

    # --- Layer-list path ---
    # Detect a Layer / list[Layer] input and route through the multi-layer loop.
    # A bare GeoDataFrame / Spark DataFrame / other coercible passes through
    # as_layers too (wraps to a single vector_layer), so the legacy keyword
    # arguments (column, geom_col, etc.) are applied onto that single layer.
    if isinstance(data, (list, tuple)) and len(data) == 0:
        raise ValueError("plot_static: no layers provided")

    is_layer_input = isinstance(data, Layer) or (
        isinstance(data, (list, tuple)) and data and isinstance(data[0], Layer)
    )

    if is_layer_input:
        lyrs = as_layers(data)
        created = ax is None
        if created:
            _, ax = plt.subplots(1, figsize=(fig_w, fig_h))
        _emit(
            f"[vizx]   emphasis={emphasis}: edgecolor={em['edgecolor']}, "
            f"linewidth={em['linewidth']}, alpha={em['alpha']}, "
            f"raster_alpha={em['raster_alpha']}",
            level=2,
            debug_mode=debug_mode,
        )
        for lyr in lyrs:
            _draw_one_layer(
                lyr,
                ax,
                max_rows=max_rows,
                sample_seed=sample_seed,
                srid=srid,
                legend=legend,
                emphasis=emphasis,
            )
        if basemap:
            try:
                import contextily as cx

                # OpenStreetMap.Mapnik needs no API key (CartoDB now requires one);
                # pass basemap_source=... to use CartoDB/another provider.
                source = basemap_source or cx.providers.OpenStreetMap.Mapnik
                cx.add_basemap(ax, source=source, crs="EPSG:3857")
            except Exception as exc:  # noqa: BLE001
                warnings.warn(
                    f"plot_static: basemap unavailable ({type(exc).__name__}: {exc}); "
                    "rendering without basemap.",
                    stacklevel=2,
                )
        # Compositor owns the title — set unconditionally so a per-layer plot_cog
        # default ("COG") never leaks onto the composite.
        ax.set_title(title or "")
        ax.set_axis_off()
        return ax

    # --- Legacy single-data path (Spark DataFrame / GeoDataFrame / bare) ---
    gdf = _resolve_gdf(
        data, geom_col, grid_system, max_rows, srid, grid_conf, sample_seed
    )

    created = ax is None
    if created:
        _, ax = plt.subplots(1, figsize=(fig_w, fig_h))

    # Reproject to Web Mercator (3857) regardless of `basemap` so that (a) a
    # contextily basemap lines up and (b) layers drawn on the same `ax` via
    # `ax=` share one coordinate system and overlay correctly -- `basemap` only
    # toggles whether tiles are fetched, not the projection. A CRS-less
    # GeoDataFrame cannot be reprojected, so it is plotted as-is.
    plot_gdf = gdf.to_crs(3857) if gdf.crs is not None else gdf

    # Resolve styling: an explicit user kwarg (not _UNSET) always wins; otherwise
    # the emphasis profile supplies the default (keyed by geometry kind).
    eff_cmap, eff_alpha, eff_edgecolor, eff_markersize, em_lw = _resolve_static_style(
        plot_gdf,
        em,
        cmap=cmap,
        alpha=alpha,
        edgecolor=edgecolor,
        markersize=markersize,
    )

    _emit(
        f"[vizx]   emphasis={emphasis}: edgecolor={eff_edgecolor}, "
        f"alpha={eff_alpha}, markersize={eff_markersize}",
        level=2,
        debug_mode=debug_mode,
    )
    kwargs = {
        "ax": ax,
        "alpha": eff_alpha,
        "edgecolor": eff_edgecolor,
        "cmap": eff_cmap,
    }
    if column is not None:
        kwargs["column"] = column
        kwargs["legend"] = legend
    if eff_markersize is not None:
        kwargs["markersize"] = eff_markersize
    # Emphasis line width (resolved above; None unless emphasis declares one).
    if em_lw is not None:
        kwargs["linewidth"] = em_lw
    if not fill:
        # Outline-only: no face, so polygons don't cover layers beneath them.
        # The caller supplies a visible `edgecolor` (default "face" would be
        # invisible against a "none" face).
        kwargs["facecolor"] = "none"
    plot_gdf.plot(**kwargs)

    if basemap and plot_gdf.crs is None:
        # No CRS to align tiles against (e.g. a custom grid with srid<=0);
        # a basemap would be placed against arbitrary coordinates, so skip it.
        warnings.warn(
            "plot_static: data has no CRS (e.g. a custom grid with srid<=0); "
            "skipping the basemap. Pass basemap=False to silence this, or use "
            "geometries / a grid that declares a real CRS.",
            stacklevel=2,
        )
    elif basemap:
        try:
            import contextily as cx

            # OpenStreetMap.Mapnik needs no API key (CartoDB now requires one);
            # pass basemap_source=... to use CartoDB/another provider.
            source = basemap_source or cx.providers.OpenStreetMap.Mapnik
            cx.add_basemap(ax, source=source, crs=plot_gdf.crs)
        except Exception as exc:  # noqa: BLE001 — offline/no-egress/missing -> fallback
            warnings.warn(
                f"plot_static: basemap unavailable ({type(exc).__name__}: {exc}); "
                "rendering without basemap. Ensure network egress to the tile "
                "server at execution time for the basemap to bake into the output.",
                stacklevel=2,
            )

    if title:
        ax.set_title(title)
    ax.set_axis_off()

    # No pyplot.show(): the inline/Databricks backend auto-displays the figure at
    # cell end with all overlaid layers; calling show() on the creating call
    # would flush the base layer before an `ax=` overlay is added. (`created` is
    # retained only to gate figure creation above.)
    return ax
