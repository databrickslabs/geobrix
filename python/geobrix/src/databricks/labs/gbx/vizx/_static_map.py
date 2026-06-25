"""Static (non-interactive) map rendering for gbx.vizx.

plot_static renders Spark- or GeoPandas-derived geometries / DGGS cells over a
contextily basemap as a static matplotlib figure -- the GitHub-renderable
counterpart to GeoDataFrame.explore(). Requires the [vizx] extra.
"""

import warnings

_GEOM_COL_CANDIDATES = ("wkt", "geometry", "geom", "ewkt", "wkb", "ewkb")
_CELL_COL_CANDIDATES = ("cellid", "cell", "cell_id", "h3", "quadbin", "bng", "index")


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


def _collect_limited(df, max_rows):
    """Collect a Spark DataFrame to pandas with a truncate-and-warn row guard."""
    if max_rows is None:
        return df.toPandas()
    pdf = df.limit(max_rows + 1).toPandas()
    if len(pdf) > max_rows:
        pdf = pdf.iloc[:max_rows]
        warnings.warn(
            f"plot_static: output truncated to max_rows={max_rows} for driver-side "
            "viz; pass max_rows=None to collect all rows.",
            stacklevel=2,
        )
    return pdf


def _h3_boundary(cell):
    import h3
    from shapely.geometry import Polygon

    idx = cell if isinstance(cell, str) else h3.int_to_str(int(cell))
    ring = h3.cell_to_boundary(idx)  # (lat, lng) pairs in h3 v4
    return Polygon([(lng, lat) for lat, lng in ring])


def _h3_boundaries(values):
    return [_h3_boundary(c) for c in values]


def _nyi(name):
    def _raise(_values):
        raise NotImplementedError(
            f"plot_static: grid_system={name!r} is a planned fast-follow; "
            "not supported yet."
        )

    return _raise


_GRID_DISPATCH = {
    "h3": _h3_boundaries,
    "quadbin": _nyi("quadbin"),
    "bng": _nyi("bng"),
    "custom": _nyi("custom"),
}


def _resolve_cells(data, col, grid_system, max_rows):
    """DGGS cell-id column -> boundary-polygon GeoDataFrame (EPSG:4326)."""
    import geopandas as gpd

    if grid_system not in _GRID_DISPATCH:
        raise ValueError(
            f"plot_static: grid_system={grid_system!r} is not one of "
            f"{sorted(_GRID_DISPATCH)} or None."
        )
    pdf = _collect_limited(data, max_rows)
    geometry = _GRID_DISPATCH[grid_system](pdf[col].tolist())
    return gpd.GeoDataFrame(pdf.drop(columns=[col]), geometry=geometry, crs=4326)


def _resolve_gdf(data, geom_col, grid_system, max_rows, srid):
    """Spark DataFrame or GeoDataFrame -> geopandas.GeoDataFrame (EPSG:4326 or srid)."""
    import geopandas as gpd

    if isinstance(data, gpd.GeoDataFrame):
        return data

    col = geom_col or _detect_geom_col(data, grid_system)

    if grid_system is not None:
        return _resolve_cells(data, col, grid_system, max_rows)

    from databricks.labs.gbx._geom import parse_geom

    field = data.schema[col]
    strategy = _geom_strategy(field.dataType)
    work = data
    if strategy == "native":
        from pyspark.sql.functions import expr

        work = data.withColumn(col, expr(f"st_asbinary(`{col}`)"))
        if srid is None and "geography" in field.dataType.simpleString().lower():
            srid = 4326

    pdf = _collect_limited(work, max_rows)
    geoms = [parse_geom(v) for v in pdf[col]]
    pdf = pdf.drop(columns=[col])
    return gpd.GeoDataFrame(pdf, geometry=geoms, crs=(srid or 4326))


def plot_static(
    data,
    *,
    geom_col=None,
    grid_system=None,
    column=None,
    cmap="viridis",
    legend=True,
    basemap=True,
    basemap_source=None,
    alpha=0.8,
    edgecolor="face",
    fill=True,
    markersize=None,
    title=None,
    fig_w=10,
    fig_h=10,
    max_rows=10_000,
    srid=None,
    ax=None,
):
    """Render geometries / DGGS cells over a basemap as a static figure.

    ``data`` is a Spark DataFrame or a geopandas.GeoDataFrame. Geometry columns
    accept WKT/EWKT/WKB/EWKB and native GEOMETRY/GEOGRAPHY (decoded via the
    shared parse_geom); set ``grid_system`` ('h3' in v1) to treat the column as
    DGGS cell ids (string or long). The contextily basemap is rendered when
    ``basemap=True``; any failure (no egress / missing dep) degrades to a
    warning and a basemap-less render. Returns the matplotlib Axes; pass it back
    via ``ax=`` to overlay layers -- every layer is reprojected to Web Mercator
    (EPSG:3857), so a ``basemap=False`` overlay aligns with a basemap layer on
    the same axes.

    ``plot_static`` does not call ``pyplot.show()``; in a notebook the figure
    auto-displays at cell end with all overlaid layers present, and a script can
    call ``plt.show()`` itself. Pass ``fill=False`` to draw geometries as
    outlines only (no face) -- e.g. a canvas/footprint boundary over a filled
    choropleth; combine with ``edgecolor`` to colour the outline. Requires the
    [vizx] extra.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    import matplotlib.pyplot as plt

    gdf = _resolve_gdf(data, geom_col, grid_system, max_rows, srid)

    created = ax is None
    if created:
        _, ax = plt.subplots(1, figsize=(fig_w, fig_h))

    # Reproject to Web Mercator (3857) regardless of `basemap` so that (a) a
    # contextily basemap lines up and (b) layers drawn on the same `ax` via
    # `ax=` share one coordinate system and overlay correctly -- `basemap` only
    # toggles whether tiles are fetched, not the projection. A CRS-less
    # GeoDataFrame cannot be reprojected, so it is plotted as-is.
    plot_gdf = gdf.to_crs(3857) if gdf.crs is not None else gdf

    kwargs = {"ax": ax, "alpha": alpha, "edgecolor": edgecolor, "cmap": cmap}
    if column is not None:
        kwargs["column"] = column
        kwargs["legend"] = legend
    if markersize is not None:
        kwargs["markersize"] = markersize
    if not fill:
        # Outline-only: no face, so polygons don't cover layers beneath them.
        # The caller supplies a visible `edgecolor` (default "face" would be
        # invisible against a "none" face).
        kwargs["facecolor"] = "none"
    plot_gdf.plot(**kwargs)

    if basemap:
        try:
            import contextily as cx

            source = basemap_source or cx.providers.CartoDB.Positron
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
