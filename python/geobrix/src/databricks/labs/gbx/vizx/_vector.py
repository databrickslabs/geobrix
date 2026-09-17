"""Spark DataFrame -> GeoDataFrame adapters for gbx.vizx interactive maps.

Collect to the driver (single-node viz); guarded by max_rows so a large frame
does not OOM the driver. Boundaries for H3 cells use the h3 lib (portable), not
the Databricks-native h3_boundaryaswkt.
"""

import warnings

# 1.3x headroom on the sample fraction: pyspark .sample is Bernoulli
# (approximate-N), so over-sampling then .limit(max_rows) reliably fills up to
# max_rows rows while staying reproducible by seed.
_SAMPLE_HEADROOM = 1.3

# Default cap for driver-side cell collects (shared across callers).
_DEFAULT_MAX_ROWS = 10_000

# Safety ceiling: raise when the caller did not set max_rows explicitly and the
# frame would exceed this count.  Forces an intentional choice rather than a
# silent slow/OOM collect.
_DRIVER_SAFE_CELLS = 500_000


class _Unset:
    """Sentinel so cells_as_gdf can detect whether max_rows was supplied."""

    __slots__ = ()


_UNSET = _Unset()


def _collect_capped(df, max_rows, sample_seed, label):
    """Collect a Spark DataFrame to pandas with a max_rows cap.

    ``sample_seed=None`` -> first ``max_rows`` rows via ``.limit`` (current
    behaviour; deterministic, cheapest). ``sample_seed=<int>`` -> a reproducible
    Bernoulli sample via ``pyspark.sql.DataFrame.sample`` (seeded; one extra
    ``count()`` job) capped at ``max_rows``. Emits a truncate warning labelled
    ``label`` when the cap fires. ``max_rows=None`` collects everything.
    """
    if max_rows is None:
        return df.toPandas()

    if sample_seed is not None:
        total = df.count()
        frac = min(1.0, (max_rows * _SAMPLE_HEADROOM) / total) if total else 1.0
        sampled = df.sample(
            withReplacement=False, fraction=frac, seed=sample_seed
        ).limit(max_rows)
        pdf = sampled.toPandas()
        if total > len(pdf):
            warnings.warn(
                f"{label}: output sampled to max_rows={max_rows} (seed="
                f"{sample_seed}) for driver-side viz; pass max_rows=None to "
                "collect all rows.",
                stacklevel=3,
            )
        return pdf

    pdf = df.limit(max_rows + 1).toPandas()
    if len(pdf) > max_rows:
        pdf = pdf.iloc[:max_rows]
        warnings.warn(
            f"{label}: output truncated to the first {max_rows} contiguous rows "
            "(NOT a representative sample). Pass sample_seed=<int> for a "
            "reproducible random sample, or max_rows=None to collect all rows.",
            stacklevel=3,
        )
    return pdf


def as_gdf(df, wkt_col="wkt", *, max_rows=10_000, sample_seed=None):
    """Spark DataFrame with a WKT column -> geopandas.GeoDataFrame (EPSG:4326).

    Collects to the driver. With max_rows set (default 10_000) the frame is
    truncated to max_rows and a warning is emitted; pass max_rows=None to opt out.

    ``sample_seed`` (Spark-only; ignored for an in-memory input): ``None``
    (default) takes the first ``max_rows`` rows via ``.limit`` (deterministic,
    partition-order arbitrary); an int draws a reproducible seeded sample via
    ``pyspark.sql.DataFrame.sample`` (same seed -> same rows) at the cost of one
    extra ``count()`` job.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()
    import geopandas as gpd

    if wkt_col not in df.columns:
        raise ValueError(
            f"as_gdf: column {wkt_col!r} not in DataFrame columns {df.columns}"
        )
    pdf = _collect_capped(df, max_rows, sample_seed, "as_gdf")
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


# ---------------------------------------------------------------------------
# Internal dissolve helpers (module-level so tests can patch them)
# ---------------------------------------------------------------------------


def _dissolve_product(df, cell_col, dissolve_by):
    """Spark-side dissolve via Databricks product ST functions.

    Uses ``h3_boundaryaswkb``, ``st_geomfromwkb``, ``st_union_agg``, and
    ``st_asbinary`` — built-in on DBR 11.3+ / Serverless.  The SQL expression
    is built as a string so it resolves against the DBR runtime engine without
    requiring local Python imports of those functions.

    Returns a ``geopandas.GeoDataFrame`` with one row per distinct
    ``dissolve_by`` value and a geometry column in EPSG:4326.

    Raises on any failure (AnalysisException, Py4JJavaError, etc.); the caller
    decides whether to fall back.
    """
    from pyspark.sql import functions as F

    import geopandas as gpd
    import shapely.wkb

    agg_expr = F.expr(
        f"st_asbinary(st_union_agg(st_geomfromwkb("
        f"h3_boundaryaswkb({cell_col})))) AS _geom_wkb"
    )
    dissolved = df.groupBy(dissolve_by).agg(agg_expr)
    pdf = dissolved.toPandas()

    geoms = [shapely.wkb.loads(bytes(b)) for b in pdf["_geom_wkb"]]
    return gpd.GeoDataFrame(
        pdf.drop(columns=["_geom_wkb"]), geometry=geoms, crs=4326
    )


def _dissolve_geopandas(df, cell_col, extra_cols, dissolve_by, max_rows, sample_seed):
    """Collect cells and dissolve per group using the h3 lib + geopandas.

    This is the portable fallback path: it collects up to ``max_rows`` cells
    to the driver (capped + warned by ``_collect_capped``), converts each cell
    id to a boundary polygon via the ``h3`` library, then dissolves via
    ``geopandas.GeoDataFrame.dissolve``.
    """
    import geopandas as gpd
    import h3
    from shapely.geometry import Polygon

    cols = [cell_col, *extra_cols]
    pdf = _collect_capped(df.select(*cols), max_rows, sample_seed, "cells_as_gdf")

    def _boundary(cell_int):
        ring = h3.cell_to_boundary(h3.int_to_str(int(cell_int)))
        # h3 v4 returns (lat, lng) pairs; shapely wants (lng, lat).
        return Polygon([(lng, lat) for lat, lng in ring])

    geometry = [_boundary(c) for c in pdf[cell_col]]
    gdf = gpd.GeoDataFrame(pdf, geometry=geometry, crs=4326)
    return gdf.dissolve(by=dissolve_by).reset_index()


def cells_as_gdf(
    df,
    cell_col="cellid",
    extra_cols=(),
    *,
    max_rows=_UNSET,
    dissolve_by=None,
    dissolve_engine="auto",
    sample_seed=None,
):
    """H3 cell ids (bigint) -> boundary polygons as a GeoDataFrame (EPSG:4326).

    Boundaries come from the h3 lib (h3 v4 takes a string index, so each bigint
    cellid is converted via h3.int_to_str). extra_cols are carried through.

    **Dissolve on Spark (``dissolve_by`` + product engine):**
    When ``dissolve_by`` is set and ``dissolve_engine`` is ``"auto"`` or
    ``"product"``, the dissolve runs on the Spark side using the Databricks
    product functions ``h3_boundaryaswkb``, ``st_geomfromwkb``,
    ``st_union_agg``, and ``st_asbinary`` (DBR 11.3+ / Serverless).  No
    ``max_rows`` cap is applied on this path — the aggregation returns only one
    row per group, which is small enough to collect safely.

    **Engine selector (``dissolve_engine``):**

    * ``"auto"`` (default) — attempt the product path; if the product functions
      are unavailable (local Spark, old DBR), fall back to the geopandas path.
    * ``"product"`` — product-only; raises if the product functions are not
      available (no silent fallback).
    * ``"geopandas"`` — force the driver-side geopandas dissolve (collects
      cells with the ``max_rows`` cap, then unions via
      ``GeoDataFrame.dissolve``).

    **Oversize guard (``_DRIVER_SAFE_CELLS = 500_000``):**
    On a driver-collect path (non-dissolve or ``dissolve_engine='geopandas'``)
    where ``max_rows`` was not supplied explicitly, if the frame count exceeds
    ``_DRIVER_SAFE_CELLS`` a ``ValueError`` is raised naming ``max_rows=``,
    ``sample_seed=``, and ``dissolve_by=`` as remedies.

    ``dissolve_by`` must be one of ``extra_cols`` when set. Raises ``ValueError``
    if ``dissolve_by`` is set but not in ``extra_cols``, or if ``dissolve_engine``
    is not one of ``{"auto", "product", "geopandas"}``.

    ``sample_seed`` (Spark-only): ``None`` (default) takes the first ``max_rows``
    cells via ``.limit`` (first-N contiguous — NOT a representative sample; use
    ``sample_seed=<int>`` for a reproducible random sample); an int draws a
    reproducible seeded sample via ``pyspark.sql.DataFrame.sample`` at the cost
    of one extra ``count()`` job.
    """
    from databricks.labs.gbx.vizx._env import assert_viz_available

    assert_viz_available()

    # Validate dissolve_engine
    _valid_engines = {"auto", "product", "geopandas"}
    if dissolve_engine not in _valid_engines:
        raise ValueError(
            f"cells_as_gdf: dissolve_engine={dissolve_engine!r} must be one of "
            f"{sorted(_valid_engines)}; got {dissolve_engine!r}."
        )

    # Validate dissolve_by
    if dissolve_by is not None and dissolve_by not in extra_cols:
        raise ValueError(
            f"cells_as_gdf: dissolve_by={dissolve_by!r} is not in "
            f"extra_cols={list(extra_cols)!r}; add it to extra_cols first."
        )

    # Resolve max_rows sentinel
    _max_rows_is_default = isinstance(max_rows, _Unset)
    if _max_rows_is_default:
        max_rows = _DEFAULT_MAX_ROWS

    # -----------------------------------------------------------------------
    # Spark-side product dissolve path
    # -----------------------------------------------------------------------
    if dissolve_by is not None and dissolve_engine != "geopandas":
        if dissolve_engine == "product":
            try:
                return _dissolve_product(df, cell_col, dissolve_by)
            except Exception as exc:
                raise RuntimeError(
                    "cells_as_gdf(dissolve_engine='product'): the Databricks "
                    "product ST functions (h3_boundaryaswkb, st_geomfromwkb, "
                    "st_union_agg, st_asbinary) are required (DBR 11.3+). "
                    "Use dissolve_engine='geopandas' to force the driver-side "
                    "path, or dissolve_engine='auto' to choose automatically."
                ) from exc
        else:  # auto: try product, fall back to geopandas
            try:
                return _dissolve_product(df, cell_col, dissolve_by)
            except Exception:
                # Check oversize guard before driver-collect fallback
                if _max_rows_is_default:
                    n = df.count()
                    if n > _DRIVER_SAFE_CELLS:
                        raise ValueError(
                            f"cells_as_gdf: {n:,} cells exceed the driver-collect "
                            f"safety limit ({_DRIVER_SAFE_CELLS:,}) and the product "
                            f"dissolve is not available. Pass max_rows=<n> to cap the "
                            f"driver collect, sample_seed=<int> for a reproducible "
                            f"random sample, or dissolve_by=<col> with "
                            f"dissolve_engine='product' to require the Spark-side path."
                        )
                return _dissolve_geopandas(
                    df, cell_col, extra_cols, dissolve_by, max_rows, sample_seed
                )

    # -----------------------------------------------------------------------
    # Driver-collect path (non-dissolve, or dissolve_engine="geopandas")
    # -----------------------------------------------------------------------
    if _max_rows_is_default:
        n = df.count()
        if n > _DRIVER_SAFE_CELLS:
            raise ValueError(
                f"cells_as_gdf: {n:,} cells exceed the driver-collect safety limit "
                f"({_DRIVER_SAFE_CELLS:,}). Pass max_rows=<n> to collect a subset, "
                f"sample_seed=<int> for a reproducible random sample, or "
                f"dissolve_by=<col> to dissolve on Spark via product functions."
            )

    if dissolve_by is not None:
        # dissolve_engine == "geopandas" (only way to reach here with dissolve_by)
        return _dissolve_geopandas(
            df, cell_col, extra_cols, dissolve_by, max_rows, sample_seed
        )

    # Non-dissolve: collect + build per-cell GDF
    import geopandas as gpd
    import h3
    from shapely.geometry import Polygon

    cols = [cell_col, *extra_cols]
    pdf = _collect_capped(df.select(*cols), max_rows, sample_seed, "cells_as_gdf")

    def _boundary(cell_int):
        ring = h3.cell_to_boundary(h3.int_to_str(int(cell_int)))
        # h3 v4 returns (lat, lng) pairs; shapely wants (lng, lat).
        return Polygon([(lng, lat) for lat, lng in ring])

    geometry = [_boundary(c) for c in pdf[cell_col]]
    return gpd.GeoDataFrame(pdf, geometry=geometry, crs=4326)
