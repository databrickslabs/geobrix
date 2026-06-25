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


def _resolve_gdf(data, geom_col, grid_system, max_rows, srid):
    """Spark DataFrame or GeoDataFrame -> geopandas.GeoDataFrame (EPSG:4326 or srid)."""
    import geopandas as gpd

    if isinstance(data, gpd.GeoDataFrame):
        return data

    col = geom_col or _detect_geom_col(data, grid_system)

    if grid_system is not None:
        return _resolve_cells(data, col, grid_system, max_rows)  # noqa: F821

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
