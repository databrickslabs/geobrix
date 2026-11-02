"""RasterX Python API.

Thin wrappers around GeoBrix Scala functions (gbx_rst_*). Register with
rx.register(spark) then use the functions on raster tile columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_rst_<name>;

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)`` — so you can write ``rst_clip(tile, geom, True)``
and ``rst_transform(tile, 4326)`` instead of wrapping in ``f.lit``. Strings and
``Column`` values pass through unchanged — pyspark treats a bare string as a
dataframe column reference (``f.col("name")``); wrap in ``f.lit(...)`` to pass
a string literal.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from typing import Optional, Union  # noqa: F401

from pyspark.sql import Column, DataFrame, SparkSession  # noqa: F401
from pyspark.sql import functions as f

from databricks.labs.gbx import _api  # noqa: F401

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via f.lit(); pass strings and Columns through.

    Strings stay as strings so pyspark's call_function treats them as column
    references (matching the library's existing idiom, e.g. rx.rst_width("tile")).
    Use f.lit("...") for string literals.
    """
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


def _crs_col(x: ColLike) -> Column:
    """Coerce a CRS argument to a literal Column.

    A bare ``str`` (e.g. ``"EPSG:4326"``) is a CRS descriptor, not a column
    name, so lift it via ``f.lit``. Columns pass through unchanged.
    """
    if isinstance(x, str):
        return f.lit(x)
    return _col(x)


def register(_spark: SparkSession) -> None:
    """Register RasterX functions with the Spark session.

    Call once (e.g. after creating the session) so that gbx_rst_* SQL
    functions are available. Uses the active Spark session if needed.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "rasterx").load().collect()

    # gbx_rst_fromfile reads a file on the executor. On Databricks the JVM (Scala) cannot read a
    # UC Volume (/Volumes/...) FUSE path in the Spark execution context -- the UC credential is held
    # only by Spark's managed Python worker -- so the canonical implementation is the pyrx Python
    # UDF, which DOES read /Volumes (and /Workspace, DBFS, local). Override the Scala registration
    # with it when pyrx (the lightweight tier) is importable. If it is NOT present, skip gracefully and leave the
    # Scala gbx_rst_fromfile in place (it still reads local / DBFS / Workspace, just not /Volumes).
    try:
        # Register the 2-arg (path, driver) SQL UDF, but the HEAVY variant that
        # MATERIALIZES: heavy/JVM callers need bytes (a virtual path-only tile is
        # useless to them). Same call text as the light registration
        # (gbx_rst_fromfile(path, driver)); the tier decides virtual vs
        # materialized, last register() wins. (_fromfile_udf is the 3-arg
        # Python-binding variant and must NOT be registered here, or 2-arg SQL
        # calls break on arity.)
        from databricks.labs.gbx import _gdal_policy
        from databricks.labs.gbx.pyrx.functions import (
            _fromfile_sql_materialized_udf as _pyrx_fromfile_materialized_udf,
        )

        # the light implementation it registers carries the driver's remote-path switch to the workers
        _gdal_policy.carrying_session(_spark).udf.register(
            "gbx_rst_fromfile", _pyrx_fromfile_materialized_udf
        )
    except (
        Exception
    ):  # noqa: BLE001 - pyrx/light-tier not installed: keep the Scala fallback
        pass


def rst_getsubdataset(tile: ColLike, subset_name: ColLike) -> Column:
    """Return a sub-dataset (e.g. HDF sublayer) by name.

    Args:
        tile: Raster tile column.
        subset_name: Name of the sub-dataset.

    Returns:
        Column of raster tile (sub-dataset).
    """
    return f.call_function("gbx_rst_getsubdataset", _col(tile), _col(subset_name))


# Aggregators


def rst_combineavg_agg(tile: ColLike) -> Column:
    """Aggregate multiple raster tiles by averaging (use with groupBy).

    Args:
        tile: Raster tile column.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg_agg", _col(tile))


def rst_derivedband_agg(
    tile: ColLike, python_func: ColLike, func_name: ColLike
) -> Column:
    """Aggregate tiles and apply a Python UDF per band (use with groupBy).

    Args:
        tile: Raster tile column.
        python_func: Python source code of the UDF (string).
        func_name: Name of the callable in python_func.

    Returns:
        Column of derived raster tile.
    """
    return f.call_function(
        "gbx_rst_derivedband_agg", _col(tile), _col(python_func), _col(func_name)
    )


def rst_rasterize_agg(
    geom: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike,
) -> Column:
    """Rasterize streaming (geom, value) rows into a single raster tile (use with groupBy).

    Streams one geometry/value pair per row; the extent and pixel-size arguments
    are per-group constants.  Overlap is last-wins (nondeterministic across the group).

    Args:
        geom: Geometry (WKB, EWKB, WKT, or EWKT) column (Polygon, MultiPolygon, etc.).
        value: DOUBLE burn value column.
        xmin: Minimum X of the output raster extent.
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        out_srid: EPSG SRID of the output raster.

    Returns:
        Column of raster tile.
    """
    return f.call_function(
        "gbx_rst_rasterize_agg",
        _col(geom),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid),
    )


def rst_frombands_agg(tile: ColLike, band_index: ColLike) -> Column:
    """Stack single-band tiles into a multi-band tile by explicit band index (use with groupBy).

    Streams one (tile, band_index) pair per row.  On evaluation the tiles are sorted
    by ``band_index`` ascending and stacked via ``gbx_rst_frombands``.  Unlike the
    non-aggregator :func:`rst_frombands` (which reads ARRAY position as band order),
    this aggregator accepts an explicit integer ``band_index`` to guarantee ordering
    independent of row arrival order.

    ``band_index`` accepts both ``IntegerType`` and ``LongType`` columns; PySpark
    infers Python ``int`` literals as ``LongType``, which is handled transparently.

    Args:
        tile: Single-band raster tile column.
        band_index: Integer (or long) column (1-based) indicating the output band position.

    Returns:
        Column of multi-band raster tile.
    """
    return f.call_function(
        "gbx_rst_frombands_agg",
        _col(tile),
        _col(band_index),
    )


def rst_h3_rasterize_agg(
    cellid: ColLike,
    value: ColLike = None,
    out_srid: ColLike = None,
    pixel_size: ColLike = None,
    xmin: ColLike = None,
    ymin: ColLike = None,
    xmax: ColLike = None,
    ymax: ColLike = None,
    width: ColLike = None,
    height: ColLike = None,
    mode: ColLike = None,
    kring_pad: ColLike = None,
) -> Column:
    """Rasterize a group's H3 cells into one tile (pixel-centroid burn).

    Use with ``groupBy``; each row contributes one H3 ``cellid`` (and optionally
    a ``value``). When ``value`` is omitted (or ``None``), pixels covered by any
    cell are burned with ``1.0`` (presence mask).  Supply an explicit canvas
    (``xmin`` … ``height``) from ``rst_h3_gridspec`` for aligned multi-band
    stacking; otherwise the grid is auto-derived from the cell set.

    Every argument except ``cellid`` is optional and defaults to the same value
    the lightweight tier uses, so the common call is simply
    ``rx.rst_h3_rasterize_agg("cellid", "value")``. Because SQL binds
    positionally, an omitted keyword is filled here with a correctly-typed SQL
    literal (a typed ``NULL`` for the extent/size fields).

    Args:
        cellid:     LONG column of H3 cell ids.
        value:      DOUBLE burn-value column; omit for a presence mask.
        out_srid:   EPSG SRID of the output raster (default ``4326``).
        pixel_size: Pixel size in CRS units (used when extent is auto-derived).
        xmin:       Minimum X of the output canvas (CRS units).
        ymin:       Minimum Y of the output canvas (CRS units).
        xmax:       Maximum X of the output canvas (CRS units).
        ymax:       Maximum Y of the output canvas (CRS units).
        width:      Canvas width in pixels (INTEGER).
        height:     Canvas height in pixels (INTEGER).
        mode:       Sampling mode string (default ``"centroids"``).
        kring_pad:  K-ring expansion around each cell before rasterizing
                    (default ``1``; ``0`` = no expansion).

    Returns:
        Column of raster tile — the v2 tile struct with fields
        ``cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata``.
    """

    def _c(x, default):
        return _col(x) if x is not None else default

    return f.call_function(
        "gbx_rst_h3_rasterize_agg",
        _col(cellid),
        _c(value, f.lit(None).cast("double")),
        _c(out_srid, f.lit(4326)),
        _c(pixel_size, f.lit(None).cast("double")),
        _c(xmin, f.lit(None).cast("double")),
        _c(ymin, f.lit(None).cast("double")),
        _c(xmax, f.lit(None).cast("double")),
        _c(ymax, f.lit(None).cast("double")),
        _c(width, f.lit(None).cast("int")),
        _c(height, f.lit(None).cast("int")),
        _c(mode, f.lit("centroids")),
        _c(kring_pad, f.lit(1)),
    )


# Constructors


def rst_fromcontent(content: ColLike, driver: ColLike) -> Column:
    """Build a raster tile from binary content and GDAL driver name.

    Args:
        content: Column of binary content (e.g. from binaryFile reader).
        driver: GDAL driver name (e.g. GTiff, COG).

    Returns:
        Column of raster tile.
    """
    return f.call_function("gbx_rst_fromcontent", _col(content), _col(driver))


def rst_fromfile(path: ColLike, driver: ColLike) -> Column:
    """Build a raster tile from a file path and GDAL driver name.

    Args:
        path: Column of SOURCE FILE path (string) — a filesystem or Volume path read to
            produce a materialized bytes-tile.  This is a raster-constructor argument,
            not a "path-tile" field; the returned tile always carries raster bytes.
        driver: GDAL driver name (e.g. GTiff).

    Returns:
        Column of raster tile (materialized — raster bytes present).

    Note:
        ``gbx_rst_fromfile`` is **lightweight-only**: it is implemented by the ``pyrx`` Python loader
        (a ``pandas_udf``) and has no JVM/Scala implementation. On Databricks the executor JVM cannot
        read a UC Volume (``/Volumes/...``) FUSE path — the UC credential is held only by Spark's
        managed Python worker — so this delegates to ``pyrx.rst_fromfile`` and **requires
        the lightweight tier (the env wheel for the runtime, or the plain wheel off Databricks)**. Without
        it, this raises with guidance. The portable alternative (works on every tier) is
        ``spark.read.format("binaryFile").load(path)`` + ``gbx_rst_fromcontent(content, driver)``.
    """
    try:
        from databricks.labs.gbx.pyrx import functions as _pyrx
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "gbx_rst_fromfile is lightweight-only and requires the lightweight tier "
            "(implemented by the pyrx Python reader; the JVM cannot read UC Volumes). "
            "Install the env wheel for the runtime (or pip install geobrix off Databricks), or use "
            "spark.read.format('binaryFile').load(path) + gbx_rst_fromcontent(content, driver)."
        ) from e
    # The heavyweight surface always yields a MATERIALIZED tile (raster bytes
    # present): a virtual tile carries only a path, which JVM expressions cannot
    # read. pyrx.rst_fromfile now defaults to a virtual tile, so force
    # materialize=True here to preserve this shim's bytes-present contract.
    return _pyrx.rst_fromfile(path, driver, materialize=True)


def rst_frombands(bands: ColLike) -> Column:
    """Build a raster tile from a list of band tiles (same dimensions).

    Args:
        bands: Column of array of raster tiles (one per band).

    Returns:
        Column of multi-band raster tile.
    """
    return f.call_function("gbx_rst_frombands", _col(bands))


# Generators


def rst_h3_tessellate(
    tile: ColLike,
    resolution: ColLike,
    assignment: ColLike = "centroid",
    coverage: ColLike = "complete",
) -> Column:
    """Tessellate the raster into H3 cells at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        assignment: ``"centroid"`` (default) single-assigns each valid pixel to
            the one cell whose hexagon contains its centroid (chips partition the
            valid pixels); ``"covering"`` keeps every cell whose hexagon overlaps
            the raster footprint (chips may share pixels). String literals are
            auto-wrapped in ``f.lit``; pass a ``Column`` to defer.
        coverage: ``"complete"`` (default) emits a chip for every candidate cell,
            including synthetic all-NoData chips for covered cells with no
            assigned pixels (centroid) or all-NoData clipped chips (covering);
            ``"sparse"`` omits all-NoData chips. String literals are auto-wrapped
            in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Column of array of (H3 index, tile) or similar.
    """
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    return f.call_function(
        "gbx_rst_h3_tessellate",
        _col(tile),
        _col(resolution),
        assignment_col,
        coverage_col,
    )


def rst_maketiles(tile: ColLike, size_in_mb: ColLike) -> Column:
    """Split the raster into smaller tiles by approximate size in MB.

    Args:
        tile: Raster tile column.
        size_in_mb: Target tile size in megabytes (column).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function("gbx_rst_maketiles", _col(tile), _col(size_in_mb))


def rst_separatebands(tile: ColLike) -> Column:
    """Split the raster into one tile per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of single-band raster tiles.
    """
    return f.call_function("gbx_rst_separatebands", _col(tile))


def rst_tooverlappingtiles(
    tile: ColLike, tile_width: ColLike, tile_height: ColLike, overlap: ColLike
) -> Column:
    """Produce overlapping tiles with the given dimensions and overlap.

    Args:
        tile: Raster tile column.
        tile_width: Width of each tile (pixels).
        tile_height: Height of each tile (pixels).
        overlap: Overlap in pixels (e.g. for stitching).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function(
        "gbx_rst_tooverlappingtiles",
        _col(tile),
        _col(tile_width),
        _col(tile_height),
        _col(overlap),
    )


# Grid


def rst_h3_rastertogridavg(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute average pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"`` — whether to emit
            cells with no valid pixels.
        assignment: ``"centroid"`` (default) single-assigns each valid pixel to
            one cell; ``"covering"`` distributes pixels to all overlapping cells.

    Returns:
        Column of grid values (e.g. struct with H3 index and avg).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridavg",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_h3_rastertogridcount(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute pixel count per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridcount",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_h3_rastertogridmax(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute maximum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column of grid values (e.g. struct with H3 index and max).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridmax",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_h3_rastertogridmin(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute minimum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column of grid values (e.g. struct with H3 index and min).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridmin",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_h3_rastertogridmedian(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute median pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column of grid values (e.g. struct with H3 index and median).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridmedian",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_h3_rastertogridsum(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the sum of pixel values per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column of grid values (e.g. struct with H3 index and sum).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridsum",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_h3_rastertogridvariance(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population variance of pixel values per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column of grid values (e.g. struct with H3 index and variance).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridvariance",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_h3_rastertogridstddev(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population standard deviation of pixel values per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column of grid values (e.g. struct with H3 index and stddev).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_h3_rastertogridstddev",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridavg(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute average pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridavg",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridcount(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute pixel count per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridcount",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridmax(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute maximum pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridmax",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridmin(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute minimum pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridmin",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridmedian(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute median pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridmedian",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridsum(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the sum of pixel values per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridsum",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridvariance(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population variance of pixel values per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridvariance",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_rastertogridstddev(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population standard deviation of pixel values per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_quadbin_rastertogridstddev",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridavg(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute average pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridavg",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridcount(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute pixel count per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridcount",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridmax(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute maximum pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridmax",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridmin(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute minimum pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridmin",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridmedian(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute median pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridmedian",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridsum(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the sum of pixel values per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridsum",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridvariance(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population variance of pixel values per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridvariance",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_bng_rastertogridstddev(
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population standard deviation of pixel values per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_bng_rastertogridstddev",
        _col(tile),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridavg(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute average pixel value per custom-grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridavg",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridcount(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute pixel count per custom-grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridcount",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridmax(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute maximum pixel value per custom-grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridmax",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridmin(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute minimum pixel value per custom-grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridmin",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridmedian(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute median pixel value per custom-grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridmedian",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridsum(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the sum of pixel values per custom-grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridsum",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridvariance(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population variance of pixel values per custom-grid cell.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridvariance",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_custom_rastertogridstddev(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = "complete",
    assignment: ColLike = "centroid",
) -> Column:
    """Compute the population standard deviation of pixel values per custom-grid cell.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        coverage: ``"complete"`` (default) or ``"sparse"``.
        assignment: ``"centroid"`` (default) or ``"covering"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
        Output cell ids are BIGINT encoded by the custom-grid layout.
    """
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    return f.call_function(
        "gbx_rst_custom_rastertogridstddev",
        _col(tile),
        _col(grid),
        _col(resolution),
        coverage_col,
        assignment_col,
    )


def rst_quadbin_tessellate(
    tile: ColLike,
    resolution: ColLike,
    assignment: ColLike = "centroid",
    coverage: ColLike = "complete",
) -> Column:
    """Tessellate the raster into CARTO quadbin v0 cells at the given zoom level.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        assignment: ``"centroid"`` (default) single-assigns each valid pixel to
            the one cell containing its centroid (chips partition the valid
            pixels); ``"covering"`` keeps every cell whose tile overlaps the
            raster footprint. String literals are auto-wrapped in ``f.lit``; pass a
            ``Column`` to defer.
        coverage: ``"complete"`` (default) emits a chip for every candidate cell;
            ``"sparse"`` omits all-NoData chips. String literals are auto-wrapped
            in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Column of array of (quadbin cell id BIGINT, tile) pairs.
    """
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    return f.call_function(
        "gbx_rst_quadbin_tessellate",
        _col(tile),
        _col(resolution),
        assignment_col,
        coverage_col,
    )


def rst_bng_tessellate(
    tile: ColLike,
    resolution: ColLike,
    assignment: ColLike = "centroid",
    coverage: ColLike = "complete",
) -> Column:
    """Tessellate the raster into BNG grid cells at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.
        assignment: ``"centroid"`` (default) single-assigns each valid pixel to
            the one cell containing its centroid; ``"covering"`` keeps every cell
            whose grid square overlaps the raster footprint. String literals are
            auto-wrapped in ``f.lit``; pass a ``Column`` to defer.
        coverage: ``"complete"`` (default) emits a chip for every candidate cell;
            ``"sparse"`` omits all-NoData chips. String literals are auto-wrapped
            in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Column of array of (BNG cell id STRING, tile) pairs.
    """
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    return f.call_function(
        "gbx_rst_bng_tessellate",
        _col(tile),
        _col(resolution),
        assignment_col,
        coverage_col,
    )


def rst_custom_tessellate(
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    assignment: ColLike = "centroid",
    coverage: ColLike = "complete",
) -> Column:
    """Tessellate the raster into user-defined custom-grid cells.

    One chip per overlapping cell, each clipped to that cell's extent. The
    raster must already be in the custom grid's native CRS — no automatic
    reprojection is performed (use ``rst_transform`` upstream if needed). Cell
    IDs are BIGINT encoded by the custom-grid layout.

    Args:
        tile: Raster tile column.
        grid: Custom-grid spec struct (from ``gbx_custom_grid(...)``).
        resolution: Custom-grid resolution int ``[0, conf.max_resolution]``.
        assignment: ``"centroid"`` (default) — each pixel assigned to one cell
            by its centroid; ``"covering"`` — every cell overlapping the raster
            bbox receives a chip. String literals are auto-wrapped in ``f.lit``.
        coverage: ``"complete"`` (default) emits a chip for every candidate cell;
            ``"sparse"`` omits all-NoData chips. String literals are auto-wrapped
            in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Column of (cellid BIGINT, raster TILE_STRUCT) pairs — one row per chip.
    """
    assignment_col = (
        f.lit(assignment) if isinstance(assignment, str) else _col(assignment)
    )
    coverage_col = f.lit(coverage) if isinstance(coverage, str) else _col(coverage)
    return f.call_function(
        "gbx_rst_custom_tessellate",
        _col(tile),
        _col(grid),
        _col(resolution),
        assignment_col,
        coverage_col,
    )


def rst_quadbin_rasterize_agg(
    cellid: ColLike,
    value: ColLike = None,
    out_srid: ColLike = None,
    pixel_size: ColLike = None,
    xmin: ColLike = None,
    ymin: ColLike = None,
    xmax: ColLike = None,
    ymax: ColLike = None,
    width: ColLike = None,
    height: ColLike = None,
    mode: ColLike = None,
    kring_pad: ColLike = None,
) -> Column:
    """Rasterize a group's CARTO quadbin v0 cells into one tile (pixel-centroid burn).

    Use with ``groupBy``; each row contributes one quadbin ``cellid`` (BIGINT) and
    optionally a ``value``. When ``value`` is omitted (or ``None``), pixels covered
    by any cell are burned with ``1.0`` (presence mask). Supply an explicit canvas
    (``xmin`` … ``height``) for aligned multi-band stacking; otherwise the grid is
    auto-derived from the cell set.

    Every argument except ``cellid`` is optional and defaults to the same value
    the lightweight tier uses, so the common call is simply
    ``rx.rst_quadbin_rasterize_agg("cellid", "value")``. Because SQL binds
    positionally, an omitted keyword is filled here with a correctly-typed SQL
    literal (a typed ``NULL`` for the extent/size fields).

    Args:
        cellid:     BIGINT column of quadbin cell ids.
        value:      DOUBLE burn-value column; omit for a presence mask.
        out_srid:   EPSG SRID of the output raster (default ``4326``).
        pixel_size: Pixel size in CRS units (used when extent is auto-derived).
        xmin:       Minimum X of the output canvas (CRS units).
        ymin:       Minimum Y of the output canvas (CRS units).
        xmax:       Maximum X of the output canvas (CRS units).
        ymax:       Maximum Y of the output canvas (CRS units).
        width:      Canvas width in pixels (INTEGER).
        height:     Canvas height in pixels (INTEGER).
        mode:       Sampling mode string (default ``"centroids"``).
        kring_pad:  K-ring expansion around each cell before rasterizing
                    (default ``1``; ``0`` = no expansion).

    Returns:
        Column of raster tile — the v2 tile struct with fields
        ``cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata``.
    """

    def _c(x, default):
        return _col(x) if x is not None else default

    return f.call_function(
        "gbx_rst_quadbin_rasterize_agg",
        _col(cellid),
        _c(value, f.lit(None).cast("double")),
        _c(out_srid, f.lit(4326)),
        _c(pixel_size, f.lit(None).cast("double")),
        _c(xmin, f.lit(None).cast("double")),
        _c(ymin, f.lit(None).cast("double")),
        _c(xmax, f.lit(None).cast("double")),
        _c(ymax, f.lit(None).cast("double")),
        _c(width, f.lit(None).cast("int")),
        _c(height, f.lit(None).cast("int")),
        _c(mode, f.lit("centroids")),
        _c(kring_pad, f.lit(1)),
    )


# Operations


def rst_asformat(tile: ColLike, new_format: ColLike) -> Column:
    """Convert the raster to a different GDAL format (e.g. COG, Zarr).

    Args:
        tile: Raster tile column.
        new_format: Target format/driver name.

    Returns:
        Column of raster tile in the new format.
    """
    return f.call_function("gbx_rst_asformat", _col(tile), _col(new_format))


def rst_clip(
    tile: ColLike, geom: ColLike, cutline_all_touched: ColLike, clip_crs: ColLike = None
) -> Column:
    """Clip the raster to a geometry (or mask).

    Args:
        tile: Raster tile column.
        geom: Clipping geometry column (WKT/WKB/EWKT/EWKB).
        cutline_all_touched: If True, include pixels touched by the boundary.
        clip_crs: Optional source CRS of a plain WKB/WKT cutline (int SRID or
            CRS string). An EWKB/EWKT embedded SRID wins; absent + no SRID ->
            assumed already in the raster CRS.

    Returns:
        Column of clipped raster tile.
    """
    if clip_crs is None:
        return f.call_function(
            "gbx_rst_clip", _col(tile), _col(geom), _col(cutline_all_touched)
        )
    return f.call_function(
        "gbx_rst_clip",
        _col(tile),
        _col(geom),
        _col(cutline_all_touched),
        _crs_col(clip_crs),
    )


def rst_combineavg(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by averaging (same extent/cellsize).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg", _col(tiles))


def rst_combinecount(tiles: ColLike) -> Column:
    """Count valid (non-NoData) inputs per pixel across aligned tiles.

    Args:
        tiles: Column of array of raster tiles (same extent/cellsize/CRS).

    Returns:
        Column of combined raster tile where each pixel is the count of valid inputs.
    """
    return f.call_function("gbx_rst_combinecount", _col(tiles))


def rst_combinemax(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by taking the per-pixel maximum.

    Args:
        tiles: Column of array of raster tiles (same extent/cellsize/CRS).

    Returns:
        Column of combined raster tile where each pixel is the maximum of valid inputs.
    """
    return f.call_function("gbx_rst_combinemax", _col(tiles))


def rst_combinemedian(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by taking the per-pixel median.

    Args:
        tiles: Column of array of raster tiles (same extent/cellsize/CRS).

    Returns:
        Column of combined raster tile where each pixel is the median of valid inputs.
    """
    return f.call_function("gbx_rst_combinemedian", _col(tiles))


def rst_combinemin(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by taking the per-pixel minimum.

    Args:
        tiles: Column of array of raster tiles (same extent/cellsize/CRS).

    Returns:
        Column of combined raster tile where each pixel is the minimum of valid inputs.
    """
    return f.call_function("gbx_rst_combinemin", _col(tiles))


def rst_combinestddev(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by computing per-pixel population std-dev.

    Args:
        tiles: Column of array of raster tiles (same extent/cellsize/CRS).

    Returns:
        Column of combined raster tile where each pixel is the population std-dev of valid inputs.
    """
    return f.call_function("gbx_rst_combinestddev", _col(tiles))


def rst_combinesum(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by summing per pixel.

    Args:
        tiles: Column of array of raster tiles (same extent/cellsize/CRS).

    Returns:
        Column of combined raster tile where each pixel is the sum of valid inputs.
    """
    return f.call_function("gbx_rst_combinesum", _col(tiles))


def rst_align_to(tile: ColLike, reference_tile: ColLike) -> Column:
    """Warp a tile to match the grid of a reference tile.

    Output has exactly the same CRS, extent, width, height, and geotransform as
    the reference tile. Uses nearest-neighbour resampling.

    Args:
        tile: The raster tile to warp.
        reference_tile: Reference tile whose grid defines the output grid.

    Returns:
        Column of warped raster tile aligned to the reference grid.
    """
    return f.call_function("gbx_rst_align_to", _col(tile), _col(reference_tile))


def rst_chm(dsm_tile: ColLike, dem_tile: ColLike) -> Column:
    """Compute Canopy Height Model from DSM and DEM tiles.

    Returns ``clamp(align(DSM→DEM) - DEM, min=0)``. The DSM is warped onto the
    DEM's grid (CRS, extent, width, height) via nearest-neighbour resampling
    before subtraction. Negative differences are clamped to 0. NoData in either
    input propagates to the output.

    Args:
        dsm_tile: Digital Surface Model tile (surface heights).
        dem_tile: Digital Elevation Model tile (bare-earth reference; also defines
            the output grid).

    Returns:
        Column of Float32 CHM tile (NoData -9999).
    """
    return f.call_function("gbx_rst_chm", _col(dsm_tile), _col(dem_tile))


def rst_isoband(tile: ColLike, breaks: ColLike) -> Column:
    """Reclassify a raster band into value bins and return contour polygons.

    Returns ``ARRAY<struct(geom_wkb BINARY, band INT, lower DOUBLE, upper DOUBLE)>`` —
    one entry per contiguous region in the same break interval. NoData pixels are
    excluded. Bins are half-open ``[breaks[i], breaks[i+1])``.

    Args:
        tile: Raster tile column.
        breaks: ``ARRAY<DOUBLE>`` of N+1 strictly-ascending boundary values
            defining N bands.

    Returns:
        Column of ``ARRAY<struct>`` — one polygon per contiguous patch.
    """
    return f.call_function("gbx_rst_isoband", _col(tile), _col(breaks))


def rst_convolve(tile: ColLike, kernel: ColLike) -> Column:
    """Apply a convolution kernel to the raster.

    Args:
        tile: Raster tile column.
        kernel: Kernel matrix (e.g. 3x3) as column.

    Returns:
        Column of convolved raster tile.
    """
    return f.call_function("gbx_rst_convolve", _col(tile), _col(kernel))


def rst_derivedband(tile: ColLike, python_func: ColLike, func_name: ColLike) -> Column:
    """Apply a Python UDF to each pixel (or band) to produce a derived band.

    Args:
        tile: Raster tile column.
        python_func: Python source code of the UDF (string).
        func_name: Name of the callable in python_func.

    Returns:
        Column of raster tile with derived band(s).
    """
    return f.call_function(
        "gbx_rst_derivedband", _col(tile), _col(python_func), _col(func_name)
    )


def rst_filter(tile: ColLike, kernel_size: ColLike, operation: ColLike) -> Column:
    """Apply a filter (e.g. min, max, mean) over a kernel window.

    Args:
        tile: Raster tile column.
        kernel_size: Size of the kernel (e.g. 3 for 3x3).
        operation: Filter operation name (e.g. min, max, mean).

    Returns:
        Column of filtered raster tile.
    """
    return f.call_function(
        "gbx_rst_filter", _col(tile), _col(kernel_size), _col(operation)
    )


def rst_initnodata(tile: ColLike) -> Column:
    """Initialise or fix NoData values in the raster (e.g. from metadata).

    Args:
        tile: Raster tile column.

    Returns:
        Column of raster tile with NoData set.
    """
    return f.call_function("gbx_rst_initnodata", _col(tile))


def rst_merge(tiles: ColLike) -> Column:
    """Merge multiple raster tiles into one (e.g. mosaic).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge", _col(tiles))


def rst_ndvi(tile: ColLike, red_idx: ColLike, nir_idx: ColLike) -> Column:
    """Compute NDVI from red and NIR band indices.

    Args:
        tile: Raster tile column.
        red_idx: 1-based red band index.
        nir_idx: 1-based NIR band index.

    Returns:
        Column of raster tile (single-band NDVI).
    """
    return f.call_function("gbx_rst_ndvi", _col(tile), _col(red_idx), _col(nir_idx))


def rst_transform(tile: ColLike, srid: ColLike) -> Column:
    """Reproject the raster to the target SRID (EPSG code).

    Args:
        tile: Raster tile column.
        srid: Target spatial reference ID (e.g. 4326 for WGS84).

    Returns:
        Column of reprojected raster tile.
    """
    return f.call_function("gbx_rst_transform", _col(tile), _col(srid))


def rst_updatetype(tile: ColLike, new_type: ColLike) -> Column:
    """Update the declared data type of the raster (e.g. after conversion).

    Args:
        tile: Raster tile column.
        new_type: New GDAL data type name (e.g. Byte, Float32).

    Returns:
        Column of raster tile with updated type metadata.
    """
    return f.call_function("gbx_rst_updatetype", _col(tile), _col(new_type))


def rst_to_webmercator(
    tile: ColLike, resampling: Union[ColLike, None] = None
) -> Column:
    """Reproject the tile to EPSG:3857 (web mercator).

    Most slippy-map workflows start here because rasters typically arrive
    in EPSG:4326 or a UTM zone — neither renders directly in tile servers.

    Args:
        tile: Raster tile column.
        resampling: gdalwarp -r algorithm (default ``"bilinear"``). Use
            ``"near"`` for categorical rasters. String literals are auto
            wrapped in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Tile column reprojected to EPSG:3857.
    """
    resampling_col = (
        f.lit("bilinear")
        if resampling is None
        else (f.lit(resampling) if isinstance(resampling, str) else _col(resampling))
    )
    return f.call_function("gbx_rst_to_webmercator", _col(tile), resampling_col)


def rst_tilexyz(
    tile: ColLike,
    z: ColLike,
    x: ColLike,
    y: ColLike,
    format: Union[ColLike, None] = None,
    size: ColLike = 256,
    resampling: Union[ColLike, None] = None,
    rescale: Union[ColLike, None] = None,
) -> Column:
    """Render a single web-mercator XYZ tile to PNG / JPEG / WEBP bytes.

    Returns ``BinaryType`` with the encoded tile bytes for ``(z, x, y)``.
    Out-of-extent tiles return a transparent PNG (alpha=0) of the requested
    size -- NOT null. Slippy-map tile servers expect a 200-status non-zero
    body even outside source coverage.

    Args:
        tile: Raster tile column.
        z: Zoom level (0 <= z <= 20).
        x: Tile X coordinate (0 <= x < 2^z).
        y: Tile Y coordinate (0 <= y < 2^z, Y north-down).
        format: Output image format -- ``"PNG"`` (default), ``"JPEG"``, or ``"WEBP"``.
            String literals are auto-wrapped in ``f.lit``.
        size: Output edge length in pixels (default 256).
        resampling: gdalwarp -r algorithm (default ``"bilinear"``). String literals
            are auto-wrapped in ``f.lit``.
        rescale: 8-bit encoding contrast -- ``"auto"`` (default) rescales non-8-bit
            imagery by whole-dataset per-band min/max for display contrast; ``"none"``
            keeps the raw full-dtype-range mapping; a ``"min,max"`` string (e.g.
            ``"8000,12000"``) sets explicit bounds applied to every band. An explicit
            ``(min, max)`` tuple is NOT supported through the Column API -- pass a
            ``"min,max"`` string literal or use ``f.lit("min,max")``. String literals
            are auto-wrapped in ``f.lit``. Note: rescale currently affects PNG output
            only; JPEG/WEBP rescale support is a future follow-up.

    Returns:
        Binary column with the encoded image bytes.
    """
    format_col = (
        f.lit("PNG")
        if format is None
        else (f.lit(format) if isinstance(format, str) else _col(format))
    )
    resampling_col = (
        f.lit("bilinear")
        if resampling is None
        else (f.lit(resampling) if isinstance(resampling, str) else _col(resampling))
    )
    rescale_col = (
        f.lit("auto")
        if rescale is None
        else (f.lit(rescale) if isinstance(rescale, str) else _col(rescale))
    )
    return f.call_function(
        "gbx_rst_tilexyz",
        _col(tile),
        _col(z),
        _col(x),
        _col(y),
        format_col,
        _col(size),
        resampling_col,
        rescale_col,
    )


def rst_xyzpyramid(
    tile: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    format: Union[ColLike, None] = None,
    size: ColLike = 256,
    resampling: Union[ColLike, None] = None,
    rescale: Union[ColLike, None] = None,
) -> Column:
    """Generator: emit one row per intersecting (z, x, y) tile across [min_z, max_z].

    Per-row output column is a struct ``tile: STRUCT<z INT, x INT, y INT, bytes BINARY>``.
    Invoke directly in ``select(...)`` (top-level generator, do not wrap in ``F.explode``).
    Cell-count is capped at 10^6 candidate tiles across the requested zoom range;
    ``max_z`` is capped at 20.

    The scale mapping is resolved ONCE from the source raster before the tile loop so
    that every tile in the pyramid shares one 8-bit mapping -- no tile-to-tile seams.

    Args:
        tile: Raster tile column.
        min_z: Inclusive minimum zoom level.
        max_z: Inclusive maximum zoom level (<= 20).
        format: Output image format -- ``"PNG"`` (default), ``"JPEG"``, or ``"WEBP"``.
            String literals are auto-wrapped in ``f.lit``.
        size: Output edge length in pixels (default 256).
        resampling: gdalwarp -r algorithm (default ``"bilinear"``). String literals
            are auto-wrapped in ``f.lit``.
        rescale: 8-bit encoding contrast -- ``"auto"`` (default) rescales non-8-bit
            imagery by whole-dataset per-band min/max; ``"none"`` keeps the raw
            full-dtype-range mapping; a ``"min,max"`` string sets explicit bounds.
            String literals are auto-wrapped in ``f.lit``. Note: rescale currently
            affects PNG output only; JPEG/WEBP rescale support is a future follow-up.

    Returns:
        Array column of structs (use ``F.explode`` to get one row per tile).
    """
    format_col = (
        f.lit("PNG")
        if format is None
        else (f.lit(format) if isinstance(format, str) else _col(format))
    )
    resampling_col = (
        f.lit("bilinear")
        if resampling is None
        else (f.lit(resampling) if isinstance(resampling, str) else _col(resampling))
    )
    rescale_col = (
        f.lit("auto")
        if rescale is None
        else (f.lit(rescale) if isinstance(rescale, str) else _col(rescale))
    )
    return f.call_function(
        "gbx_rst_xyzpyramid",
        _col(tile),
        _col(min_z),
        _col(max_z),
        format_col,
        _col(size),
        resampling_col,
        rescale_col,
    )


def rst_rasterize(
    geom: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike,
) -> Column:
    """Burn a vector geometry into a raster tile at the given extent and resolution.

    Returns a GTiff-backed tile of shape ``width_px x height_px`` covering the
    bounding box ``(xmin, ymin) -> (xmax, ymax)`` in the given SRID. Pixels
    inside the geometry receive ``value``; pixels outside receive NoData
    (-9999.0, Float64).

    Args:
        geom: Geometry (WKB, EWKB, WKT, or EWKT) column.
        value: Burn value (``float``).
        xmin: Minimum X of the output raster extent.
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        out_srid: EPSG SRID of the output raster.

    Returns:
        Raster tile column.
    """
    return f.call_function(
        "gbx_rst_rasterize",
        _col(geom),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid),
    )


def rst_polygonize(
    tile: ColLike,
    band: ColLike = None,
    connectedness: ColLike = None,
) -> Column:
    """Extract vector polygons from a raster tile's contiguous value regions.

    Returns ``ARRAY<struct(geom_wkb BINARY, value DOUBLE)>``, one entry per
    connected component of equal pixel values. NoData pixels are excluded.

    Args:
        tile: Raster tile column.
        band: 1-based band index to polygonize (default 1).
        connectedness: 4 or 8; passed as GDAL ``8CONNECTED`` option (default 4).

    Returns:
        Array column of structs (use ``F.explode`` to get one row per polygon).
    """
    band_col = f.lit(1) if band is None else _col(band)
    conn_col = f.lit(4) if connectedness is None else _col(connectedness)
    return f.call_function("gbx_rst_polygonize", _col(tile), band_col, conn_col)


# ---------------------------------------------------------------------------
# Terrain analysis (DEM processing)
#
# Seven thin wrappers around gdal.DEMProcessing. All take a single source tile
# and return a derived tile. Defaults match the GDAL conventions.
# ---------------------------------------------------------------------------


def rst_aspect(
    tile: ColLike,
    trigonometric: ColLike = None,
    zero_for_flat: ColLike = None,
) -> Column:
    """Compute aspect (slope direction) from a DEM tile via ``gdal.DEMProcessing("aspect")``.

    Args:
        tile: Single-band DEM tile column.
        trigonometric: If true, output trigonometric angles measured
            counterclockwise from east; if false (default), output compass
            angles measured clockwise from north.
        zero_for_flat: If true, flat areas get value 0; if false (default),
            flat areas get -9999.

    Returns:
        Single-band Float32 GTiff tile column.
    """
    trig_col = f.lit(False) if trigonometric is None else _col(trigonometric)
    zff_col = f.lit(False) if zero_for_flat is None else _col(zero_for_flat)
    return f.call_function("gbx_rst_aspect", _col(tile), trig_col, zff_col)


def rst_hillshade(
    tile: ColLike,
    azimuth: ColLike = None,
    altitude: ColLike = None,
    z_factor: ColLike = None,
) -> Column:
    """Compute hillshade (shaded relief) from a DEM tile via ``gdal.DEMProcessing("hillshade")``.

    Args:
        tile: Single-band DEM tile column.
        azimuth: Light-source azimuth in degrees (default 315.0;
            0=N, 90=E, 180=S, 270=W).
        altitude: Light-source altitude above horizon in degrees
            (default 45.0).
        z_factor: Vertical exaggeration (default 1.0).

    Returns:
        Single-band Byte GTiff tile column with values 0..255.
    """
    az_col = f.lit(315.0) if azimuth is None else _col(azimuth)
    alt_col = f.lit(45.0) if altitude is None else _col(altitude)
    z_col = f.lit(1.0) if z_factor is None else _col(z_factor)
    return f.call_function("gbx_rst_hillshade", _col(tile), az_col, alt_col, z_col)


def rst_tri(tile: ColLike) -> Column:
    """Compute Terrain Ruggedness Index (TRI) via ``gdal.DEMProcessing("TRI")``.

    TRI is the mean absolute difference between a pixel and its 8 neighbours;
    used in landscape ecology and habitat analysis.

    Args:
        tile: Single-band DEM tile column.

    Returns:
        Single-band Float32 GTiff tile column.
    """
    return f.call_function("gbx_rst_tri", _col(tile))


def rst_tpi(tile: ColLike) -> Column:
    """Compute Topographic Position Index (TPI) via ``gdal.DEMProcessing("TPI")``.

    TPI is the difference between a pixel's elevation and the mean of its 8
    neighbours; positive values indicate ridges/peaks, negative values
    valleys.

    Args:
        tile: Single-band DEM tile column.

    Returns:
        Single-band Float32 GTiff tile column.
    """
    return f.call_function("gbx_rst_tpi", _col(tile))


def rst_roughness(tile: ColLike) -> Column:
    """Compute Roughness via ``gdal.DEMProcessing("Roughness")``.

    Roughness is the largest inter-cell difference of a central pixel and
    its 8 neighbours.

    Args:
        tile: Single-band DEM tile column.

    Returns:
        Single-band Float32 GTiff tile column.
    """
    return f.call_function("gbx_rst_roughness", _col(tile))


def rst_color_relief(
    tile: ColLike,
    color_table_path: ColLike,
) -> Column:
    """Apply a color relief mapping to a DEM tile via ``gdal.DEMProcessing("color-relief")``.

    Args:
        tile: Single-band DEM tile column.
        color_table_path: Path (FUSE-mounted Volume or local) to a gdaldem
            color file. Each line is ``elevation R G B [A]``; special values
            ``nv``, ``default``, ``0%``, ``100%`` are accepted.

    Returns:
        3- or 4-band Byte GTiff tile column (RGB or RGBA).
    """
    ctp_col = (
        f.lit(color_table_path)
        if isinstance(color_table_path, str)
        else _col(color_table_path)
    )
    return f.call_function("gbx_rst_color_relief", _col(tile), ctp_col)


# ---------------------------------------------------------------------------
# Spectral indices
#
# Five thin wrappers that build a per-pixel formula string from user-supplied
# band indices and delegate to ``gbx_rst_mapalgebra`` internally. All return a
# single-band Float32 GTiff tile sized to the input raster's extent.
# ---------------------------------------------------------------------------


def rst_evi(
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    blue_idx: ColLike,
    l: ColLike = None,
    c1: ColLike = None,
    c2: ColLike = None,
    g: ColLike = None,
) -> Column:
    """Enhanced Vegetation Index (EVI).

    Formula: ``G * (NIR - Red) / (NIR + C1*Red - C2*Blue + L)``.

    Args:
        tile: Multi-band raster tile column.
        red_idx: 1-based red band index.
        nir_idx: 1-based NIR band index.
        blue_idx: 1-based blue band index.
        l: Canopy background adjustment (default 1.0).
        c1: Aerosol resistance coefficient for red (default 6.0).
        c2: Aerosol resistance coefficient for blue (default 7.5).
        g: Gain factor (default 2.5).

    Returns:
        Single-band Float32 GTiff tile column.
    """
    l_col = f.lit(1.0) if l is None else _col(l)
    c1_col = f.lit(6.0) if c1 is None else _col(c1)
    c2_col = f.lit(7.5) if c2 is None else _col(c2)
    g_col = f.lit(2.5) if g is None else _col(g)
    return f.call_function(
        "gbx_rst_evi",
        _col(tile),
        _col(red_idx),
        _col(nir_idx),
        _col(blue_idx),
        l_col,
        c1_col,
        c2_col,
        g_col,
    )


def rst_savi(
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    l: ColLike = None,
) -> Column:
    """Soil-Adjusted Vegetation Index (SAVI).

    Formula: ``(NIR - Red) / (NIR + Red + L) * (1 + L)``.

    Args:
        tile: Multi-band raster tile column.
        red_idx: 1-based red band index.
        nir_idx: 1-based NIR band index.
        l: Soil-brightness correction factor (default 0.5; ``L=0`` reduces to
            NDVI; ``L=1`` is appropriate for very low vegetation cover).

    Returns:
        Single-band Float32 GTiff tile column.
    """
    l_col = f.lit(0.5) if l is None else _col(l)
    return f.call_function(
        "gbx_rst_savi",
        _col(tile),
        _col(red_idx),
        _col(nir_idx),
        l_col,
    )


def rst_ndwi(
    tile: ColLike,
    green_idx: ColLike,
    nir_idx: ColLike,
) -> Column:
    """Normalized Difference Water Index (NDWI, McFeeters 1996).

    Formula: ``(Green - NIR) / (Green + NIR)``. Positive values typically
    indicate open water, negative values indicate land/vegetation.

    Args:
        tile: Multi-band raster tile column.
        green_idx: 1-based green band index.
        nir_idx: 1-based NIR band index.

    Returns:
        Single-band Float32 GTiff tile column.
    """
    return f.call_function(
        "gbx_rst_ndwi",
        _col(tile),
        _col(green_idx),
        _col(nir_idx),
    )


def rst_nbr(
    tile: ColLike,
    nir_idx: ColLike,
    swir_idx: ColLike,
) -> Column:
    """Normalized Burn Ratio (NBR).

    Formula: ``(NIR - SWIR) / (NIR + SWIR)``. The difference between pre-fire
    and post-fire NBR (``dNBR``) is the canonical burn-severity index.

    Args:
        tile: Multi-band raster tile column.
        nir_idx: 1-based NIR band index.
        swir_idx: 1-based SWIR band index.

    Returns:
        Single-band Float32 GTiff tile column.
    """
    return f.call_function(
        "gbx_rst_nbr",
        _col(tile),
        _col(nir_idx),
        _col(swir_idx),
    )


# ---------------------------------------------------------------------------
# Resample family and IDW interpolation
#
# Three resample wrappers delegate to gdal.Warp with -tr / -ts; IDW pair
# (`rst_gridfrompoints` non-aggregator + `rst_gridfrompoints_agg` aggregator)
# delegates to gdal.Grid with the invdist algorithm.
# ---------------------------------------------------------------------------


def rst_resample(
    tile: ColLike,
    factor: ColLike,
    algorithm: Union[ColLike, None] = None,
) -> Column:
    """Resample a raster tile by a multiplicative ``factor``.

    ``factor > 1`` upsamples, ``0 < factor < 1`` downsamples. CRS and extent
    are preserved; output dimensions are ``round(srcW * factor) x round(srcH * factor)``.

    Args:
        tile: Raster tile column.
        factor: Multiplicative scale factor (``float``).
        algorithm: gdalwarp ``-r`` algorithm (default ``"bilinear"``). One of
            ``near``, ``bilinear``, ``cubic``, ``cubicspline``, ``lanczos``,
            ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
            String literals are auto-wrapped via ``f.lit``.

    Returns:
        Resampled raster tile column.
    """
    alg_col = (
        f.lit("bilinear")
        if algorithm is None
        else (f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm))
    )
    return f.call_function("gbx_rst_resample", _col(tile), _col(factor), alg_col)


def rst_resample_to_size(
    tile: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    algorithm: Union[ColLike, None] = None,
) -> Column:
    """Resample a raster tile to an explicit output size ``width_px x height_px``.

    Args:
        tile: Raster tile column.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        algorithm: gdalwarp ``-r`` algorithm (default ``"bilinear"``).

    Returns:
        Resampled raster tile column.
    """
    alg_col = (
        f.lit("bilinear")
        if algorithm is None
        else (f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm))
    )
    return f.call_function(
        "gbx_rst_resample_to_size",
        _col(tile),
        _col(width_px),
        _col(height_px),
        alg_col,
    )


def rst_resample_to_res(
    tile: ColLike,
    x_res: ColLike,
    y_res: ColLike,
    algorithm: Union[ColLike, None] = None,
) -> Column:
    """Resample a raster tile to an explicit ground resolution.

    ``x_res`` / ``y_res`` are in source CRS units (metres for UTM, degrees for
    EPSG:4326). Output extent matches the source bounding box adjusted to the
    new pixel size.

    Args:
        tile: Raster tile column.
        x_res: Target X resolution (``float``, CRS units / pixel).
        y_res: Target Y resolution (``float``).
        algorithm: gdalwarp ``-r`` algorithm (default ``"bilinear"``).

    Returns:
        Resampled raster tile column.
    """
    alg_col = (
        f.lit("bilinear")
        if algorithm is None
        else (f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm))
    )
    return f.call_function(
        "gbx_rst_resample_to_res",
        _col(tile),
        _col(x_res),
        _col(y_res),
        alg_col,
    )


def rst_gridfrompoints(
    points_array: ColLike,
    values_array: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike,
    power: ColLike = None,
    max_pts: ColLike = None,
) -> Column:
    """Inverse-Distance-Weighted (IDW) interpolation - non-aggregator form.

    Points (``ARRAY<BINARY>`` WKB or ``ARRAY<STRING>`` WKT) and ``values_array``
    (``ARRAY<DOUBLE>``) are passed in a single row. The output is a Float64
    GTiff tile of shape ``width_px x height_px`` covering
    ``(xmin, ymin) -> (xmax, ymax)`` in the given SRID.

    Args:
        points_array: Column of array of point geometries (WKB or WKT).
        values_array: Column of array of double values (same length as ``points_array``).
        xmin: Minimum X of the output raster extent.
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        out_srid: EPSG SRID of the output raster.
        power: IDW exponent (default 2.0).
        max_pts: Maximum neighbour points per cell (default 12).

    Returns:
        Raster tile column.
    """
    power_col = f.lit(2.0) if power is None else _col(power)
    max_pts_col = f.lit(12) if max_pts is None else _col(max_pts)
    return f.call_function(
        "gbx_rst_gridfrompoints",
        _col(points_array),
        _col(values_array),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid),
        power_col,
        max_pts_col,
    )


def rst_gridfrompoints_agg(
    point: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike,
    power: ColLike = None,
    max_pts: ColLike = None,
) -> Column:
    """IDW interpolation aggregator - one point/value per row.

    Aggregator counterpart of :func:`rst_gridfrompoints`. Group rows by an
    extent key and pass per-row ``point`` / ``value`` columns plus per-group
    literal extent parameters.

    Args:
        point: Point geometry column (WKB binary or WKT string).
        value: Double value column.
        xmin: Minimum X of the output raster extent (per-group literal).
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        out_srid: EPSG SRID of the output raster.
        power: IDW exponent (default 2.0).
        max_pts: Maximum neighbour points per cell (default 12).

    Returns:
        Raster tile column.
    """
    power_col = f.lit(2.0) if power is None else _col(power)
    max_pts_col = f.lit(12) if max_pts is None else _col(max_pts)
    return f.call_function(
        "gbx_rst_gridfrompoints_agg",
        _col(point),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid),
        power_col,
        max_pts_col,
    )


def rst_binpoints(
    x_array: ColLike,
    y_array: ColLike,
    z_array: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    statistic: ColLike = "max",
) -> Column:
    """Bin parallel x/y/z ``ARRAY<DOUBLE>`` columns into a Float32 raster tile.

    Non-aggregator (scalar) form — all three arrays live in a single row.
    For the grouped-agg form use :func:`rst_binpoints_agg`.

    Output is a single-band Float32 GTiff tile of shape
    ``width_px x height_px`` covering ``(xmin, ymin) -> (xmax, ymax)`` in the
    given SRID. Empty cells (no points) carry NoData (``-9999.0``).

    Args:
        x_array: Column of ``ARRAY<DOUBLE>`` x-coordinates.
        y_array: Column of ``ARRAY<DOUBLE>`` y-coordinates.
        z_array: Column of ``ARRAY<DOUBLE>`` z-values to reduce per cell.
        xmin: Minimum X of the output raster extent.
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        srid: EPSG SRID of the output raster.
        statistic: Reduction statistic — one of ``"max"`` (default), ``"min"``,
            ``"mean"``, ``"median"``, ``"count"``, or ``"percentile:<p>"``
            (e.g. ``"percentile:90"``).

    Returns:
        Raster tile column (single-band Float32 GTiff).
    """
    stat_col = f.lit(statistic) if isinstance(statistic, str) else _col(statistic)
    return f.call_function(
        "gbx_rst_binpoints",
        _col(x_array),
        _col(y_array),
        _col(z_array),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
        stat_col,
    )


def rst_binpoints_agg(
    x: ColLike,
    y: ColLike,
    z: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    statistic: ColLike = "max",
) -> Column:
    """Bin one ``(x, y, z)`` point per row into a Float32 raster tile (aggregator).

    Aggregator counterpart of :func:`rst_binpoints`. Group rows by an extent
    key and pass one scalar ``x`` / ``y`` / ``z`` per row together with
    per-group literal extent parameters.

    Args:
        x: Column of X-coordinates (DOUBLE).
        y: Column of Y-coordinates (DOUBLE).
        z: Column of Z-values to bin (DOUBLE).
        xmin: Minimum X of the output raster extent (per-group literal).
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        srid: EPSG SRID of the output raster.
        statistic: Reduction statistic — one of ``"max"`` (default), ``"min"``,
            ``"mean"``, ``"median"``, ``"count"``, or ``"percentile:<p>"``
            (e.g. ``"percentile:90"``).

    Returns:
        Raster tile column (single-band Float32 GTiff).
    """
    stat_col = f.lit(statistic) if isinstance(statistic, str) else _col(statistic)
    return f.call_function(
        "gbx_rst_binpoints_agg",
        _col(x),
        _col(y),
        _col(z),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
        stat_col,
    )


def rst_custom_rasterize_agg(
    cellid: ColLike,
    grid: ColLike,
    value: ColLike = None,
    out_srid: ColLike = None,
    pixel_size: ColLike = None,
    xmin: ColLike = None,
    ymin: ColLike = None,
    xmax: ColLike = None,
    ymax: ColLike = None,
    width: ColLike = None,
    height: ColLike = None,
    mode: ColLike = "centroids",
    kring_pad: ColLike = 1,
) -> Column:
    """Rasterize a group's custom-grid cells into one tile (pixel-centroid burn, heavy tier).

    Aggregator counterpart of :func:`rst_custom_rastertogrid*`: where those functions
    reduce raster pixels to per-cell statistics, this function reconstructs a raster
    from per-cell values.

    ``grid`` is a custom-grid STRUCT column produced by ``gbx_custom_grid(...)``.
    ``value`` omitted or null -> presence mask (1.0). Supply an explicit extent
    (``xmin`` .. ``height``) for aligned band stacking; else the grid is auto-derived
    from the cell set per ``mode``/``kring_pad``.

    Output CRS: ``out_srid`` wins; falls back to the grid's own SRID when not supplied.
    Use inside ``.agg()``::

        df.groupBy(k).agg(rx.rst_custom_rasterize_agg("cellid", "grid").alias("tile"))

    Args:
        cellid: Column of BIGINT custom-grid cell IDs.
        grid: Custom-grid STRUCT column from ``gbx_custom_grid(...)``.
        value: Numeric burn value column (null -> 1.0 presence mask).
        out_srid: EPSG SRID of the output CRS (null -> grid's SRID).
        pixel_size: Output pixel size in target CRS units (null -> auto).
        xmin, ymin, xmax, ymax: Output extent (null -> auto from cell set).
        width, height: Output dimensions in pixels (null -> auto).
        mode: ``"centroids"`` (default) or ``"spatial_envelope"``.
        kring_pad: Neighbourhood expansion for auto-extent (default 1).

    Returns:
        Raster tile column (single-band GTiff).
    """

    return f.call_function(
        "gbx_rst_custom_rasterize_agg",
        _col(cellid),
        f.lit(None).cast("double") if value is None else _col(value),
        _col(grid),
        f.lit(None).cast("int") if out_srid is None else _col(out_srid),
        f.lit(None).cast("double") if pixel_size is None else _col(pixel_size),
        f.lit(None).cast("double") if xmin is None else _col(xmin),
        f.lit(None).cast("double") if ymin is None else _col(ymin),
        f.lit(None).cast("double") if xmax is None else _col(xmax),
        f.lit(None).cast("double") if ymax is None else _col(ymax),
        f.lit(None).cast("int") if width is None else _col(width),
        f.lit(None).cast("int") if height is None else _col(height),
        f.lit(mode) if isinstance(mode, str) else _col(mode),
        f.lit(kring_pad) if isinstance(kring_pad, int) else _col(kring_pad),
    )


# ---------------------------------------------------------------------------
# Delaunay-TIN Digital Terrain Model (DTM) interpolation
#
# Two wrappers: `rst_dtmfromgeoms` (non-aggregator, Z-valued points as an
# array column) + `rst_dtmfromgeoms_agg` (aggregator, one Z-valued point
# per row). Both delegate to gbx_rst_dtmfromgeoms / gbx_rst_dtmfromgeoms_agg.
# ---------------------------------------------------------------------------


def rst_dtmfromgeoms(
    points_array: ColLike,
    breaklines_array: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike,
    no_data: ColLike = None,
) -> Column:
    """DTM from Z-valued points + optional breaklines via Delaunay-TIN interpolation.

    Output is a single-band Float64 GTiff of ``width_px x height_px`` over the bbox.
    For N-unit cells set ``width_px = round((xmax-xmin)/N)``,
    ``height_px = round((ymax-ymin)/N)`` (e.g. a 1000 m extent at 10 m cells -> 100 px).

    Args:
        points_array: Array column of Z-valued point geometries (WKB binary or WKT string).
        breaklines_array: Array column of breakline LineString geometries; pass an empty array for none.
        merge_tolerance: Delaunay segment-merge tolerance.
        snap_tolerance: Vertex-to-breakline snap tolerance.
        xmin, ymin, xmax, ymax: Output raster extent.
        width_px, height_px: Output raster size in pixels.
        out_srid: EPSG SRID of the output raster.
        no_data: No-data sentinel (default -9999.0).

    Returns:
        Raster tile column.
    """
    nd = f.lit(-9999.0) if no_data is None else _col(no_data)
    return f.call_function(
        "gbx_rst_dtmfromgeoms",
        _col(points_array),
        _col(breaklines_array),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid),
        nd,
    )


def rst_dtmfromgeoms_agg(
    point: ColLike,
    breaklines: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike,
    no_data: ColLike = None,
) -> Column:
    """DTM aggregator - one Z-valued ``point`` per row, grouped by extent key.

    Aggregator counterpart of :func:`rst_dtmfromgeoms`. ``point`` is the only
    aggregated (per-row) input; ``breaklines`` and all extent/tolerance args are
    per-group constants. Produces the same DTM as the non-agg form over the same grid.

    Returns:
        Raster tile column.
    """
    nd = f.lit(-9999.0) if no_data is None else _col(no_data)
    return f.call_function(
        "gbx_rst_dtmfromgeoms_agg",
        _col(point),
        _col(breaklines),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid),
        nd,
    )


def rst_index(
    tile: ColLike,
    formula_name: ColLike,
    band_map: ColLike,
) -> Column:
    """Generic dispatcher for named spectral indices.

    Built-in formulae (case-insensitive ``formula_name``):

    * ``ndvi``: ``(NIR-Red)/(NIR+Red)`` - bands ``red``, ``nir``.
    * ``gndvi``: ``(NIR-Green)/(NIR+Green)`` - bands ``green``, ``nir``.
    * ``msavi``: modified SAVI - bands ``red``, ``nir``.
    * ``ndvi_re``: red-edge NDVI - bands ``red_edge``, ``nir``.
    * ``ndmi``: ``(NIR-SWIR)/(NIR+SWIR)`` - bands ``nir``, ``swir``.
    * ``ndsi``: snow-index ``(Green-SWIR)/(Green+SWIR)`` - bands ``green``, ``swir``.

    For arbitrary user-supplied formulae, drop down to ``rst_mapalgebra``.

    Args:
        tile: Multi-band raster tile column.
        formula_name: Built-in formula name (e.g. ``"ndvi"``). Passed as a
            string literal; wrap in ``f.lit(...)`` if you want a column
            reference instead.
        band_map: ``MAP<STRING, INT>`` column wiring the formula's band names
            to 1-based band indices in ``tile`` (e.g.
            ``F.create_map(F.lit("red"), F.lit(1), F.lit("nir"), F.lit(2))``).

    Returns:
        Single-band Float32 GTiff tile column.
    """
    formula_col = (
        f.lit(formula_name) if isinstance(formula_name, str) else _col(formula_name)
    )
    return f.call_function(
        "gbx_rst_index",
        _col(tile),
        formula_col,
        _col(band_map),
    )


# ---------------------------------------------------------------------------
# Pixel ops + extraction
#
# Seven thin wrappers over GDAL per-pixel / per-tile primitives that the
# rest of the RasterX surface assumed were "always available" but weren't
# actually exposed: FillNodata, ReadRaster-at-point sampling, SetProjection,
# GetHistogram, threshold (via MapAlgebra), BuildOverviews, single-band
# extraction.
# ---------------------------------------------------------------------------


def rst_fillnodata(
    tile: ColLike,
    max_search_dist: ColLike = None,
    smoothing_iter: ColLike = None,
) -> Column:
    """Interpolate NoData pixels from valid neighbours via ``gdal.FillNodata``.

    Args:
        tile: Raster tile column.
        max_search_dist: Maximum pixel distance to search for a valid value
            to fill from (default 100.0).
        smoothing_iter: Number of 3x3 smoothing iterations after fill
            (default 0).

    Returns:
        Raster tile column with NoData holes filled.
    """
    msd_col = f.lit(100.0) if max_search_dist is None else _col(max_search_dist)
    si_col = f.lit(0) if smoothing_iter is None else _col(smoothing_iter)
    return f.call_function("gbx_rst_fillnodata", _col(tile), msd_col, si_col)


def rst_setcrs(tile: ColLike, crs: ColLike) -> Column:
    """Stamp the CRS WITHOUT reprojecting, accepting any CRS string.

    Accepts an int EPSG code, an int-castable string (``"4326"``), or any
    string accepted by rasterio's CRS parser such as ``"ESRI:54008"``, WKT, or
    PROJ4 strings. Pixel values and the GeoTransform are unchanged; only the
    CRS metadata is rewritten. Use ``rst_transformcrs`` for a reprojecting warp.

    Args:
        tile: Tile struct column.
        crs:  CRS descriptor — int EPSG code, int-castable string, authority
              string (``"EPSG:4326"``, ``"ESRI:54008"``), WKT, or PROJ4.

    Returns:
        Tile with the same pixels/transform but the new CRS.
    """
    return f.call_function("gbx_rst_setcrs", _col(tile), _crs_col(crs))


def rst_setsrid(tile: ColLike, srid: ColLike) -> Column:
    """Stamp an EPSG code on the raster's spatial-reference header (no warp).

    Use when the source raster lost or has incorrect CRS metadata but the
    actual pixel grid is already aligned with the target CRS. For real
    reprojection (with pixel-grid warp) use :func:`rst_transform`.

    Args:
        tile: Raster tile column.
        srid: EPSG code (positive integer).

    Returns:
        Raster tile column with rewritten SR header.
    """
    return f.call_function("gbx_rst_setsrid", _col(tile), _col(srid))


def rst_threshold(
    tile: ColLike,
    op: Union[ColLike, None] = None,
    value: ColLike = None,
) -> Column:
    """Binarise a raster: ``(pixel <op> value)`` -> 0/1.

    Args:
        tile: Raster tile column.
        op: Comparison operator — one of ``">"``, ``">="``, ``"<"``, ``"<="``,
            ``"=="``, ``"!="``. String literals auto-wrapped via ``f.lit``.
        value: Threshold value (``float``).

    Returns:
        Single-band Float32 GTiff tile column with values 0 or 1.
    """
    op_col = (
        f.lit(op)
        if isinstance(op, str)
        else _col(op) if op is not None else f.lit(None)
    )
    return f.call_function("gbx_rst_threshold", _col(tile), op_col, _col(value))


def rst_percentile_stretch(tile: ColLike, lo_pct: ColLike, hi_pct: ColLike) -> Column:
    """Per-band percentile contrast stretch to uint8 [0,255].

    For each band, computes lo_pct and hi_pct percentiles over valid (non-NoData)
    pixels, clips to that range, and linearly rescales to [0,255]. Output is uint8.
    NoData pixels are preserved as 0.

    Args:
        tile: Raster tile column (any number of bands).
        lo_pct: Lower percentile threshold (0-100), e.g., 2.0 for 2nd percentile.
        hi_pct: Upper percentile threshold (0-100), e.g., 98.0 for 98th percentile.

    Returns:
        Multi-band uint8 GTiff tile column (same number of bands as input).
    """
    return f.call_function(
        "gbx_rst_percentile_stretch", _col(tile), _col(lo_pct), _col(hi_pct)
    )


def rst_buildoverviews(
    tile: ColLike,
    levels: ColLike,
    resampling: Union[ColLike, None] = None,
) -> Column:
    """Build internal overviews on a raster via ``Dataset.BuildOverviews``.

    Args:
        tile: Raster tile column.
        levels: ``ARRAY<INT>`` of downsampling factors (e.g. ``[2, 4, 8, 16]``).
            Each factor produces one overview level at ``1 / factor`` resolution.
        resampling: Overview resampling algorithm — one of ``nearest``,
            ``average``, ``rms``, ``gauss``, ``cubic``, ``cubicspline``,
            ``lanczos``, ``bilinear``, ``mode``, ``none``. Defaults to
            ``"average"``. String literals auto-wrapped via ``f.lit``.

    Returns:
        Raster tile column with embedded overview pyramid.
    """
    res_col = (
        f.lit("average")
        if resampling is None
        else (f.lit(resampling) if isinstance(resampling, str) else _col(resampling))
    )
    return f.call_function("gbx_rst_buildoverviews", _col(tile), _col(levels), res_col)


def rst_band(tile: ColLike, band_index: ColLike) -> Column:
    """Extract a single band as a new single-band tile via ``gdal.Translate -b <i>``.

    Args:
        tile: Multi-band raster tile column.
        band_index: 1-based band index to extract.

    Returns:
        Single-band raster tile column.
    """
    return f.call_function("gbx_rst_band", _col(tile), _col(band_index))


def rst_cog_convert(
    tile: ColLike,
    compression: Union[ColLike, None] = None,
    blocksize: ColLike = None,
    overview_resampling: Union[ColLike, None] = None,
) -> Column:
    """Convert a raster tile to Cloud Optimized GeoTIFF (COG) layout.

    Wraps ``gdal.Translate -of COG`` with the requested compression, internal
    block size, and overview resampling. The result is still a GTiff on disk
    (downstream ``metadata.driver`` reads ``GTiff``) but laid out so HTTP range
    reads can extract regions or overview levels cheaply.

    Args:
        tile: Raster tile column.
        compression: Pixel compression — one of ``NONE``, ``DEFLATE``, ``LZW``,
            ``ZSTD``, ``LERC``, ``JPEG``, ``WEBP``. Default ``"ZSTD"``.
            String literals auto-wrapped via ``f.lit``.
        blocksize: Internal tile size in pixels (square). Default ``512``.
        overview_resampling: Downsampling algorithm for the overview pyramid —
            one of ``NEAREST``, ``AVERAGE``, ``GAUSS``, ``CUBIC``, ``CUBICSPLINE``,
            ``LANCZOS``, ``BILINEAR``, ``MODE``. Default ``"AVERAGE"``.

    Returns:
        COG-laid-out raster tile column.
    """
    comp_col = (
        f.lit("ZSTD")
        if compression is None
        else (f.lit(compression) if isinstance(compression, str) else _col(compression))
    )
    bs_col = f.lit(512) if blocksize is None else _col(blocksize)
    or_col = (
        f.lit("AVERAGE")
        if overview_resampling is None
        else (
            f.lit(overview_resampling)
            if isinstance(overview_resampling, str)
            else _col(overview_resampling)
        )
    )
    return f.call_function("gbx_rst_cog_convert", _col(tile), comp_col, bs_col, or_col)


def rst_proximity(
    tile: ColLike,
    target_values: Union[ColLike, None] = None,
    dist_units: Union[ColLike, None] = None,
    max_distance: ColLike = None,
) -> Column:
    """Compute a proximity raster: each pixel = distance to nearest source pixel.

    Wraps ``gdal.ComputeProximity``. The output preserves the source extent /
    CRS / GeoTransform; pixel dtype is Float32. Pixels beyond ``max_distance``
    or with no source in range get the output's NoData value (``-1.0``).

    Args:
        tile: Raster tile column.
        target_values: Optional comma-separated list of source-pixel values to
            measure distance to (e.g. ``"1,2,3"``). ``None`` = any non-NoData
            pixel is a target.
        dist_units: ``"GEO"`` (CRS ground units, default) or ``"PIXEL"``.
        max_distance: Optional cap on output distance (in the same units as
            ``dist_units``). ``None`` = unlimited.

    Returns:
        Float32 proximity raster tile column.
    """
    tv_col = (
        f.lit(None).cast("string")
        if target_values is None
        else (
            f.lit(target_values)
            if isinstance(target_values, str)
            else _col(target_values)
        )
    )
    du_col = (
        f.lit("GEO")
        if dist_units is None
        else (f.lit(dist_units) if isinstance(dist_units, str) else _col(dist_units))
    )
    md_col = f.lit(None).cast("double") if max_distance is None else _col(max_distance)
    return f.call_function("gbx_rst_proximity", _col(tile), tv_col, du_col, md_col)


def rst_contour(
    tile: ColLike,
    levels: ColLike,
    interval: ColLike = None,
    base: ColLike = None,
    attr_field: Union[ColLike, None] = None,
) -> Column:
    """Generate contour LineStrings from a raster as ``ARRAY<struct(geom_wkb, value)>``.

    Wraps ``gdal.ContourGenerateEx``. Supply EITHER a non-empty ``levels`` array
    (explicit contour values) OR ``interval`` (equal-step contours at
    ``base + n*interval``). Pass ``levels=array()`` to use interval mode.

    Args:
        tile: Raster tile column.
        levels: ``ARRAY<DOUBLE>`` of explicit contour values; empty -> use
            ``interval``.
        interval: Step between contours; ignored if ``levels`` is non-empty.
        base: Contour base value; only meaningful with ``interval``. Default 0.
        attr_field: Internal OGR field name carrying the contour value
            (default ``"elev"``). Read back via the ``value`` member of each
            output struct.

    Returns:
        Column of ``ARRAY<struct(geom_wkb BINARY, value DOUBLE)>``.
    """
    int_col = f.lit(0.0) if interval is None else _col(interval)
    base_col = f.lit(0.0) if base is None else _col(base)
    af_col = (
        f.lit("elev")
        if attr_field is None
        else (f.lit(attr_field) if isinstance(attr_field, str) else _col(attr_field))
    )
    return f.call_function(
        "gbx_rst_contour", _col(tile), _col(levels), int_col, base_col, af_col
    )


def rst_viewshed(
    tile: ColLike,
    observer_geom: ColLike,
    observer_height: ColLike,
    target_height: ColLike = None,
    max_distance: ColLike = None,
    crs: ColLike = None,
) -> Column:
    """Compute a binary viewshed raster from a DEM and an observer POINT.

    Wraps ``gdal.ViewshedGenerate``. Output is a Byte raster in the source CRS,
    pixel-aligned with it: visible pixels = ``255``, invisible / out-of-range
    pixels = ``0``. With a ``max_distance`` it is the window around the observer
    (the observer's pixel plus ``max_distance`` each way, clipped to the DEM);
    without one it is the whole DEM. Non-POINT ``observer_geom`` is rejected at
    runtime.

    Args:
        tile: Single-band DEM raster tile column.
        observer_geom: Observer POINT — WKB ``bytes`` or WKT ``string`` column.
            Coordinates must be in the raster's CRS.
        observer_height: Observer height above DEM at the observer pixel
            (e.g. eye height + tower height).
        target_height: Target height above DEM at each tested pixel.
            Default ``1.6`` (~average eye height).
        max_distance: Optional clipping distance in CRS units; ``None`` =
            unlimited (only bounded by raster extent).
        crs: Optional source CRS of a plain WKB/WKT observer point (int SRID
            or CRS string). An EWKB/EWKT embedded SRID wins; absent + no SRID
            -> assumed already in the raster CRS.

    Returns:
        Byte raster tile column (0 / 255).
    """
    th_col = f.lit(1.6) if target_height is None else _col(target_height)
    md_col = f.lit(None).cast("double") if max_distance is None else _col(max_distance)
    if crs is None:
        return f.call_function(
            "gbx_rst_viewshed",
            _col(tile),
            _col(observer_geom),
            _col(observer_height),
            th_col,
            md_col,
        )
    return f.call_function(
        "gbx_rst_viewshed",
        _col(tile),
        _col(observer_geom),
        _col(observer_height),
        th_col,
        md_col,
        _crs_col(crs),
    )


# BEGIN GENERATED by gbx:manifest:gen: edit manifest/functions.json, then run the generator


def rst_avg(
    tile: ColLike,
) -> Column:
    """Return the mean of the valid pixels of each band.

    Args:
        tile: The raster tile.

    Returns:
        One value per band; a band with no valid pixel gives NULL.

    SQL: ``gbx_rst_avg(tile)``
    """
    return _api.call(
        "gbx_rst_avg",
        tile,
    )


def rst_bandmetadata(
    tile: ColLike,
    band: ColLike,
) -> Column:
    """Return the metadata of one band as a map.

    Args:
        tile: The raster tile.
        band: The 1-based band number.

    Returns:
        The band's tags (empty when it has none), or NULL for a NULL or error tile.

    SQL: ``gbx_rst_bandmetadata(tile, band)``
    """
    return _api.call(
        "gbx_rst_bandmetadata",
        tile,
        band,
    )


def rst_bng_rasterize_agg(
    cellid: ColLike,
    value: ColLike,
    out_srid: ColLike,
    pixel_size: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width: ColLike,
    height: ColLike,
    mode: ColLike,
    kring_pad: ColLike,
    out_crs: ColLike = None,
) -> Column:
    """Rasterize a group's BNG cells into one tile (pixel-centroid burn).

    Args:
        cellid: The BNG cell id, for example `'TQ3080'`.
        value: The value burned for the cell. A repeated cell burns its largest value.
        out_srid: Not read: the output is always EPSG:27700.
        pixel_size: The pixel size in metres.
        xmin: The west edge of the canvas in EPSG:27700 metres.
        ymin: The south edge of the canvas in EPSG:27700 metres.
        xmax: The east edge of the canvas in EPSG:27700 metres.
        ymax: The north edge of the canvas in EPSG:27700 metres.
        width: The canvas width in pixels.
        height: The canvas height in pixels.
        mode: How the canvas is derived when no extent is given, for example
            `'centroids'`.
        kring_pad: Cells of padding around the derived canvas.
        out_crs: Must be NULL. Reproject the tile with `gbx_rst_transformcrs` instead.
            SQL default: ``NULL``.

    Returns:
        The EPSG:27700 raster of the group; NoData is -9999.

    SQL: ``gbx_rst_bng_rasterize_agg(cellid, value, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad, [out_crs])``
    """
    return _api.call(
        "gbx_rst_bng_rasterize_agg",
        cellid,
        value,
        out_srid,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
        out_crs,
    )


def rst_boundingbox(
    tile: ColLike,
) -> Column:
    """Return the raster's bounding box as a polygon.

    Args:
        tile: The raster tile.

    Returns:
        A WKB polygon in the raster CRS, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_boundingbox(tile)``
    """
    return _api.call(
        "gbx_rst_boundingbox",
        tile,
    )


def rst_crs(
    tile: ColLike,
) -> Column:
    """Return the canonical CRS of the raster.

    Args:
        tile: The raster tile.

    Returns:
        `EPSG:<code>` or `ESRI:<code>` when the CRS has an authority code, its WKT when
        it has none, or NULL when the raster has no CRS.

    SQL: ``gbx_rst_crs(tile)``
    """
    return _api.call(
        "gbx_rst_crs",
        tile,
    )


def rst_format(
    tile: ColLike,
) -> Column:
    """Return the GDAL driver short name of the raster.

    Args:
        tile: The raster tile.

    Returns:
        The driver the raster was read with, for example `GTiff` or `HFA`, or NULL for a
        NULL or error tile.

    SQL: ``gbx_rst_format(tile)``
    """
    return _api.call(
        "gbx_rst_format",
        tile,
    )


def rst_georeference(
    tile: ColLike,
) -> Column:
    """Return the geotransform as a map.

    Args:
        tile: The raster tile.

    Returns:
        The keys `upperLeftX`, `upperLeftY`, `scaleX`, `scaleY`, `skewX` and `skewY`, or
        NULL for a NULL or error tile.

    SQL: ``gbx_rst_georeference(tile)``
    """
    return _api.call(
        "gbx_rst_georeference",
        tile,
    )


def rst_getnodata(
    tile: ColLike,
) -> Column:
    """Return the NoData value of each band.

    Args:
        tile: The raster tile.

    Returns:
        One value per band, or NULL when the raster declares no NoData.

    SQL: ``gbx_rst_getnodata(tile)``
    """
    return _api.call(
        "gbx_rst_getnodata",
        tile,
    )


def rst_height(
    tile: ColLike,
) -> Column:
    """Return the height of the raster in pixels.

    Args:
        tile: The raster tile.

    Returns:
        The pixel count along y, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_height(tile)``
    """
    return _api.call(
        "gbx_rst_height",
        tile,
    )


def rst_histogram(
    tile: ColLike,
    n_buckets: ColLike = None,
    min_val: ColLike = None,
    max_val: ColLike = None,
    include_nodata: ColLike = None,
) -> Column:
    """Return the histogram of each band.

    Args:
        tile: The raster tile.
        n_buckets: The number of equal-width buckets across the range. SQL default:
            ``256``.
        min_val: The lower bound of the range; the band's smallest valid value when
            NULL. SQL default: ``NULL``.
        max_val: The upper bound of the range; the band's largest valid value when NULL.
            SQL default: ``NULL``.
        include_nodata: Whether NoData pixels are binned too; the default leaves them
            out. SQL default: ``false``.

    Returns:
        A map from `band_<i>` to the bucket counts; values outside the range are
        dropped.

    SQL: ``gbx_rst_histogram(tile, [n_buckets], [min_val], [max_val], [include_nodata])``
    """
    return _api.call(
        "gbx_rst_histogram",
        tile,
        n_buckets,
        min_val,
        max_val,
        include_nodata,
    )


def rst_isempty(
    tile: ColLike,
) -> Column:
    """Test whether a raster holds no valid pixel.

    Args:
        tile: The raster tile.

    Returns:
        True when the raster has no size, no band or only NoData; an error tile is
        empty.

    SQL: ``gbx_rst_isempty(tile)``
    """
    return _api.call(
        "gbx_rst_isempty",
        tile,
    )


def rst_mapalgebra(
    tiles: ColLike,
    expression: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Apply a map-algebra expression across an array of tiles.

    Args:
        tiles: The input tiles. Band 1 of each tile binds to `A`, `B`, `C` and so on, in
            array order.
        expression: The expression: a JSON envelope with a `calc` key, or a bare
            expression such as `'(A - B) / (A + B)'`.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile on the georeference of the first tile.

    SQL: ``gbx_rst_mapalgebra(tiles, expression)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_mapalgebra",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
            expression,
        )
    return _api.call(
        "gbx_rst_mapalgebra",
        tiles,
        expression,
    )


def rst_max(
    tile: ColLike,
) -> Column:
    """Return the maximum of the valid pixels of each band.

    Args:
        tile: The raster tile.

    Returns:
        One value per band; a band with no valid pixel gives NULL.

    SQL: ``gbx_rst_max(tile)``
    """
    return _api.call(
        "gbx_rst_max",
        tile,
    )


def rst_median(
    tile: ColLike,
) -> Column:
    """Return the median of the valid pixels of each band.

    Args:
        tile: The raster tile.

    Returns:
        One value per band; a band with no valid pixel gives NULL.

    SQL: ``gbx_rst_median(tile)``
    """
    return _api.call(
        "gbx_rst_median",
        tile,
    )


def rst_memsize(
    tile: ColLike,
) -> Column:
    """Return the in-memory size of a tile in bytes.

    Args:
        tile: The raster tile.

    Returns:
        The raster buffer length for a materialized tile, or the decoded footprint of
        the window for a virtual one.

    SQL: ``gbx_rst_memsize(tile)``
    """
    return _api.call(
        "gbx_rst_memsize",
        tile,
    )


def rst_merge_agg(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Merge a group's tile rasters into one mosaic tile.

    Args:
        tile: The tiles of the group.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The merged raster over the union extent, or NULL for an empty group.

    SQL: ``gbx_rst_merge_agg(tile)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_merge_agg",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
        )
    return _api.call(
        "gbx_rst_merge_agg",
        tile,
    )


def rst_metadata(
    tile: ColLike,
) -> Column:
    """Return the raster's metadata as a map.

    Args:
        tile: The raster tile.

    Returns:
        The metadata entries, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_metadata(tile)``
    """
    return _api.call(
        "gbx_rst_metadata",
        tile,
    )


def rst_min(
    tile: ColLike,
) -> Column:
    """Return the minimum of the valid pixels of each band.

    Args:
        tile: The raster tile.

    Returns:
        One value per band; a band with no valid pixel gives NULL.

    SQL: ``gbx_rst_min(tile)``
    """
    return _api.call(
        "gbx_rst_min",
        tile,
    )


def rst_numbands(
    tile: ColLike,
) -> Column:
    """Return the number of bands of the raster.

    Args:
        tile: The raster tile.

    Returns:
        The band count (0 for a container with only subdatasets), or NULL for a NULL or
        error tile.

    SQL: ``gbx_rst_numbands(tile)``
    """
    return _api.call(
        "gbx_rst_numbands",
        tile,
    )


def rst_pixelcount(
    tile: ColLike,
) -> Column:
    """Return the number of valid pixels of each band.

    Args:
        tile: The raster tile.

    Returns:
        One count per band; a band with no valid pixel gives 0.

    SQL: ``gbx_rst_pixelcount(tile)``
    """
    return _api.call(
        "gbx_rst_pixelcount",
        tile,
    )


def rst_pixelheight(
    tile: ColLike,
) -> Column:
    """Return the ground size of a pixel along y.

    Args:
        tile: The raster tile.

    Returns:
        The non-negative pixel height, including any skew, or NULL for a NULL or error
        tile.

    SQL: ``gbx_rst_pixelheight(tile)``
    """
    return _api.call(
        "gbx_rst_pixelheight",
        tile,
    )


def rst_pixelwidth(
    tile: ColLike,
) -> Column:
    """Return the ground size of a pixel along x.

    Args:
        tile: The raster tile.

    Returns:
        The non-negative pixel width, including any skew, or NULL for a NULL or error
        tile.

    SQL: ``gbx_rst_pixelwidth(tile)``
    """
    return _api.call(
        "gbx_rst_pixelwidth",
        tile,
    )


def rst_rastertoworldcoord(
    tile: ColLike,
    x: ColLike,
    y: ColLike,
) -> Column:
    """Convert a pixel to the world coordinate of its centre.

    Args:
        tile: The raster tile.
        x: The pixel column, counted from 0.
        y: The pixel row, counted from 0.

    Returns:
        A struct with the x and y of the pixel centre in the raster CRS.

    SQL: ``gbx_rst_rastertoworldcoord(tile, x, y)``
    """
    return _api.call(
        "gbx_rst_rastertoworldcoord",
        tile,
        x,
        y,
    )


def rst_rastertoworldcoordx(
    tile: ColLike,
    x: ColLike,
    y: ColLike,
) -> Column:
    """Convert a pixel to the x of its centre.

    Args:
        tile: The raster tile.
        x: The pixel column, counted from 0.
        y: The pixel row, counted from 0.

    Returns:
        The x of the pixel centre in the raster CRS.

    SQL: ``gbx_rst_rastertoworldcoordx(tile, x, y)``
    """
    return _api.call(
        "gbx_rst_rastertoworldcoordx",
        tile,
        x,
        y,
    )


def rst_rastertoworldcoordy(
    tile: ColLike,
    x: ColLike,
    y: ColLike,
) -> Column:
    """Convert a pixel to the y of its centre.

    Args:
        tile: The raster tile.
        x: The pixel column, counted from 0.
        y: The pixel row, counted from 0.

    Returns:
        The y of the pixel centre in the raster CRS.

    SQL: ``gbx_rst_rastertoworldcoordy(tile, x, y)``
    """
    return _api.call(
        "gbx_rst_rastertoworldcoordy",
        tile,
        x,
        y,
    )


def rst_retile(
    df: DataFrame,
    tile: ColLike,
    tile_width: ColLike,
    tile_height: ColLike,
) -> DataFrame:
    """Partition a tile into non-overlapping sub-tiles of the given pixel size.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.
        tile_width: The sub-tile width in pixels.
        tile_height: The sub-tile height in pixels.

    Returns:
        One row per sub-tile; the edge tiles are smaller when the size does not divide
        the raster.

    SQL: ``gbx_rst_retile(tile, tile_width, tile_height)``
    """
    return _api.table(
        df,
        "gbx_rst_retile",
        tile,
        tile_width,
        tile_height,
    )


def rst_rotation(
    tile: ColLike,
) -> Column:
    """Return the rotation of the raster grid in radians.

    Args:
        tile: The raster tile.

    Returns:
        The angle atan(skew y / scale x), or NULL for a NULL or error tile.

    SQL: ``gbx_rst_rotation(tile)``
    """
    return _api.call(
        "gbx_rst_rotation",
        tile,
    )


def rst_sample(
    tile: ColLike,
    geom: ColLike,
    crs: ColLike = None,
) -> Column:
    """Return the pixel value of each band at a point.

    Args:
        tile: The raster tile.
        geom: A POINT as WKB, EWKB, WKT or EWKT.
        crs: The CRS of a point with no SRID: `EPSG:<code>`, `ESRI:<code>`, WKT or an
            integer code. The raster CRS when NULL. SQL default: ``NULL``.

    Returns:
        One value per band, or NULL when the point lies outside the raster.

    SQL: ``gbx_rst_sample(tile, geom, [crs])``
    """
    return _api.call(
        "gbx_rst_sample",
        tile,
        geom,
        crs,
    )


def rst_scalex(
    tile: ColLike,
) -> Column:
    """Return the signed x scale of the geotransform.

    Args:
        tile: The raster tile.

    Returns:
        The geotransform's pixel width, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_scalex(tile)``
    """
    return _api.call(
        "gbx_rst_scalex",
        tile,
    )


def rst_scaley(
    tile: ColLike,
) -> Column:
    """Return the signed y scale of the geotransform.

    Args:
        tile: The raster tile.

    Returns:
        The geotransform's pixel height, negative for a north-up raster, or NULL for a
        NULL or error tile.

    SQL: ``gbx_rst_scaley(tile)``
    """
    return _api.call(
        "gbx_rst_scaley",
        tile,
    )


def rst_skewx(
    tile: ColLike,
) -> Column:
    """Return the x skew of the geotransform.

    Args:
        tile: The raster tile.

    Returns:
        The geotransform's third term, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_skewx(tile)``
    """
    return _api.call(
        "gbx_rst_skewx",
        tile,
    )


def rst_skewy(
    tile: ColLike,
) -> Column:
    """Return the y skew of the geotransform.

    Args:
        tile: The raster tile.

    Returns:
        The geotransform's fifth term, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_skewy(tile)``
    """
    return _api.call(
        "gbx_rst_skewy",
        tile,
    )


def rst_slope(
    tile: ColLike,
    unit: ColLike = None,
    xscale: ColLike = None,
    yscale: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute terrain slope from a single-band DEM tile (Horn's 3x3 method).

    Args:
        tile: The single-band DEM tile.
        unit: `'degrees'` or `'percent'`. SQL default: ``'degrees'``.
        xscale: Vertical units per horizontal unit along x; set it with `yscale`. When
            both are NULL, the scale comes from the raster CRS. SQL default: ``NULL``.
        yscale: Vertical units per horizontal unit along y; set it with `xscale`. SQL
            default: ``NULL``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999.

    SQL: ``gbx_rst_slope(tile, [unit], [xscale], [yscale])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_slope",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            unit,
            xscale,
            yscale,
        )
    return _api.call(
        "gbx_rst_slope",
        tile,
        unit,
        xscale,
        yscale,
    )


def rst_srid(
    tile: ColLike,
) -> Column:
    """Return the integer SRID of the raster CRS.

    Args:
        tile: The raster tile.

    Returns:
        The authority code, or NULL when the raster has no CRS or its CRS has no code.

    SQL: ``gbx_rst_srid(tile)``
    """
    return _api.call(
        "gbx_rst_srid",
        tile,
    )


def rst_subdatasets(
    tile: ColLike,
) -> Column:
    """Return the subdatasets of a container raster.

    Args:
        tile: The raster tile.

    Returns:
        The `SUBDATASET_<n>_NAME` and `SUBDATASET_<n>_DESC` entries, empty for a raster
        with no subdatasets, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_subdatasets(tile)``
    """
    return _api.call(
        "gbx_rst_subdatasets",
        tile,
    )


def rst_summary(
    tile: ColLike,
) -> Column:
    """Return a JSON summary of the raster with per-band statistics.

    Args:
        tile: The raster tile.

    Returns:
        A JSON string with the driver, size, CRS, geotransform and the statistics of
        each band.

    SQL: ``gbx_rst_summary(tile)``
    """
    return _api.call(
        "gbx_rst_summary",
        tile,
    )


def rst_transformcrs(
    tile: ColLike,
    target_crs: ColLike,
) -> Column:
    """Reproject the raster to a target CRS given as a string.

    Args:
        tile: The tile to reproject.
        target_crs: The target CRS: an authority code such as `'EPSG:3857'` or
            `'ESRI:54008'`, WKT, PROJJSON or PROJ4. An integer string is an EPSG code.

    Returns:
        The tile reprojected to the target CRS.

    SQL: ``gbx_rst_transformcrs(tile, target_crs)``
    """
    return _api.call(
        "gbx_rst_transformcrs",
        tile,
        target_crs,
    )


def rst_tryopen(
    tile: ColLike,
) -> Column:
    """Test whether tile bytes open as a raster.

    Args:
        tile: The raster tile.

    Returns:
        True when the tile opens; false for corrupt, empty or error tiles.

    SQL: ``gbx_rst_tryopen(tile)``
    """
    return _api.call(
        "gbx_rst_tryopen",
        tile,
    )


def rst_type(
    tile: ColLike,
) -> Column:
    """Return the GDAL data type of each band.

    Args:
        tile: The raster tile.

    Returns:
        One type name per band, for example `Float32`, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_type(tile)``
    """
    return _api.call(
        "gbx_rst_type",
        tile,
    )


def rst_upperleftx(
    tile: ColLike,
) -> Column:
    """Return the x coordinate of the raster's upper-left corner.

    Args:
        tile: The raster tile.

    Returns:
        The corner's x in the raster CRS, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_upperleftx(tile)``
    """
    return _api.call(
        "gbx_rst_upperleftx",
        tile,
    )


def rst_upperlefty(
    tile: ColLike,
) -> Column:
    """Return the y coordinate of the raster's upper-left corner.

    Args:
        tile: The raster tile.

    Returns:
        The corner's y in the raster CRS, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_upperlefty(tile)``
    """
    return _api.call(
        "gbx_rst_upperlefty",
        tile,
    )


def rst_width(
    tile: ColLike,
) -> Column:
    """Return the width of the raster in pixels.

    Args:
        tile: The raster tile.

    Returns:
        The pixel count along x, or NULL for a NULL or error tile.

    SQL: ``gbx_rst_width(tile)``
    """
    return _api.call(
        "gbx_rst_width",
        tile,
    )


def rst_worldtorastercoord(
    tile: ColLike,
    x: ColLike,
    y: ColLike,
) -> Column:
    """Convert a world coordinate to the pixel that contains it.

    Args:
        tile: The raster tile.
        x: The x coordinate in the raster CRS.
        y: The y coordinate in the raster CRS.

    Returns:
        A struct with the pixel column x and row y; a point outside the raster gives a
        pixel outside its size.

    SQL: ``gbx_rst_worldtorastercoord(tile, x, y)``
    """
    return _api.call(
        "gbx_rst_worldtorastercoord",
        tile,
        x,
        y,
    )


def rst_worldtorastercoordx(
    tile: ColLike,
    x: ColLike,
    y: ColLike,
) -> Column:
    """Convert a world coordinate to a pixel column.

    Args:
        tile: The raster tile.
        x: The x coordinate in the raster CRS.
        y: The y coordinate in the raster CRS.

    Returns:
        The column of the pixel that contains the point.

    SQL: ``gbx_rst_worldtorastercoordx(tile, x, y)``
    """
    return _api.call(
        "gbx_rst_worldtorastercoordx",
        tile,
        x,
        y,
    )


def rst_worldtorastercoordy(
    tile: ColLike,
    x: ColLike,
    y: ColLike,
) -> Column:
    """Convert a world coordinate to a pixel row.

    Args:
        tile: The raster tile.
        x: The x coordinate in the raster CRS.
        y: The y coordinate in the raster CRS.

    Returns:
        The row of the pixel that contains the point.

    SQL: ``gbx_rst_worldtorastercoordy(tile, x, y)``
    """
    return _api.call(
        "gbx_rst_worldtorastercoordy",
        tile,
        x,
        y,
    )


# END GENERATED
