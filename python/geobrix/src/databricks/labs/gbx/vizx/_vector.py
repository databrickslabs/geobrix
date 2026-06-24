"""Spark DataFrame -> GeoDataFrame adapters for gbx.vizx interactive maps.

Collect to the driver (single-node viz); guarded by max_rows so a large frame
does not OOM the driver. Boundaries for H3 cells use the h3 lib (portable), not
the Databricks-native h3_boundaryaswkt.
"""

import warnings


def as_gdf(df, wkt_col="wkt", *, max_rows=10_000):
    """Spark DataFrame with a WKT column -> geopandas.GeoDataFrame (EPSG:4326).

    Collects to the driver. With max_rows set (default 10_000) the frame is
    truncated to max_rows and a warning is emitted; pass max_rows=None to opt out.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import geopandas as gpd

    if wkt_col not in df.columns:
        raise ValueError(
            f"as_gdf: column {wkt_col!r} not in DataFrame columns {df.columns}"
        )
    if max_rows is None:
        pdf = df.toPandas()
    else:
        pdf = df.limit(max_rows + 1).toPandas()
        if len(pdf) > max_rows:
            pdf = pdf.iloc[:max_rows]
            warnings.warn(
                f"as_gdf: output truncated to max_rows={max_rows} for driver-side "
                "viz; pass max_rows=None to collect all rows.",
                stacklevel=2,
            )
    geometry = gpd.GeoSeries.from_wkt(pdf[wkt_col], crs=4326)
    pdf = pdf.drop(columns=[wkt_col])
    pdf["geometry"] = geometry.values
    return gpd.GeoDataFrame(pdf, geometry="geometry", crs=4326)


def grid_as_gdf(grid, srid=None):
    """Grid spec (from rst_h3_gridspec) -> 1-row GeoDataFrame (EPSG:4326).

    ``grid`` is a Spark Row or dict with fields ``xmin, ymin, xmax, ymax`` and
    optionally ``srid``, ``pixel_size``, ``width``, ``height`` (the struct that
    ``rst_h3_gridspec`` returns in its ``grid`` field).

    ``srid`` overrides the grid's own ``srid`` field; if both are absent, 4326
    is assumed. When the source CRS is not 4326 the bounding box is reprojected
    via ``pyproj`` before building the GeoDataFrame.

    Optional metadata columns ``pixel_size``, ``width``, and ``height`` are
    carried through if present on the input.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    import geopandas as gpd
    from shapely.geometry import box

    # Resolve SRID: explicit arg > grid field > default 4326
    if srid is None:
        try:
            srid = grid["srid"]
        except (KeyError, TypeError):
            srid = 4326
    if srid is None:
        srid = 4326

    xmin = grid["xmin"]
    ymin = grid["ymin"]
    xmax = grid["xmax"]
    ymax = grid["ymax"]

    geom = box(xmin, ymin, xmax, ymax)

    if int(srid) != 4326:
        from shapely.ops import transform

        try:
            import pyproj
        except ImportError as exc:
            raise ImportError(
                "grid_as_gdf: pyproj is required for CRS reprojection. "
                "Install with: pip install pyproj"
            ) from exc
        transformer = pyproj.Transformer.from_crs(int(srid), 4326, always_xy=True)
        geom = transform(transformer.transform, geom)

    row = {"geometry": geom}
    for key in ("pixel_size", "width", "height"):
        try:
            val = grid[key]
            row[key] = val
        except Exception:  # noqa: BLE001 — KeyError/PySparkValueError/TypeError
            pass

    return gpd.GeoDataFrame([row], geometry="geometry", crs=4326)


def cells_as_gdf(
    df, cell_col="cellid", extra_cols=(), *, max_rows=10_000, dissolve_by=None
):
    """H3 cell ids (bigint) -> boundary polygons as a GeoDataFrame (EPSG:4326).

    Boundaries come from the h3 lib (h3 v4 takes a string index, so each bigint
    cellid is converted via h3.int_to_str). extra_cols are carried through.

    ``dissolve_by`` must be one of ``extra_cols`` when set. When provided the
    returned GeoDataFrame contains one dissolved polygon per distinct value of
    that column (the union footprint) rather than one row per cell. Raises
    ``ValueError`` if ``dissolve_by`` is set but not in ``extra_cols``.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    if dissolve_by is not None and dissolve_by not in extra_cols:
        raise ValueError(
            f"cells_as_gdf: dissolve_by={dissolve_by!r} is not in "
            f"extra_cols={list(extra_cols)!r}; add it to extra_cols first."
        )

    import h3
    from shapely.geometry import Polygon

    cols = [cell_col, *extra_cols]
    if max_rows is None:
        pdf = df.select(*cols).toPandas()
    else:
        pdf = df.select(*cols).limit(max_rows + 1).toPandas()
        if len(pdf) > max_rows:
            pdf = pdf.iloc[:max_rows]
            warnings.warn(
                f"cells_as_gdf: output truncated to max_rows={max_rows} for "
                "driver-side viz; pass max_rows=None to collect all rows.",
                stacklevel=2,
            )

    def _boundary(cell_int):
        ring = h3.cell_to_boundary(h3.int_to_str(int(cell_int)))
        # h3 v4 returns (lat, lng) pairs; shapely wants (lng, lat).
        return Polygon([(lng, lat) for lat, lng in ring])

    import geopandas as gpd

    geometry = [_boundary(c) for c in pdf[cell_col]]
    gdf = gpd.GeoDataFrame(pdf, geometry=geometry, crs=4326)

    if dissolve_by is not None:
        gdf = gdf.dissolve(by=dissolve_by).reset_index()

    return gdf
