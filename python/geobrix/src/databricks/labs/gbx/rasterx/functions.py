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


def rst_merge(tiles: ColLike) -> Column:
    """Merge multiple raster tiles into one (e.g. mosaic).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge", _col(tiles))


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


# ---------------------------------------------------------------------------
# Resample family and IDW interpolation
#
# Three resample wrappers delegate to gdal.Warp with -tr / -ts; IDW pair
# (`rst_gridfrompoints` non-aggregator + `rst_gridfrompoints_agg` aggregator)
# delegates to gdal.Grid with the invdist algorithm.
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Pixel ops + extraction
#
# Seven thin wrappers over GDAL per-pixel / per-tile primitives that the
# rest of the RasterX surface assumed were "always available" but weren't
# actually exposed: FillNodata, ReadRaster-at-point sampling, SetProjection,
# GetHistogram, threshold (via MapAlgebra), BuildOverviews, single-band
# extraction.
# ---------------------------------------------------------------------------


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


def rst_align_to(
    tile: ColLike,
    reference_tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Warp a tile onto the grid of a reference tile, with nearest-neighbour resampling.

    Args:
        tile: The tile to warp.
        reference_tile: The tile whose grid (CRS, extent, width, height) the result
            takes.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile on the reference tile's grid: its CRS, width, height and geotransform,
        nearest-neighbour values; NoData where the tile does not cover the grid. NULL
        when either tile is NULL; an error tile in either comes out as itself.

    SQL: ``gbx_rst_align_to(tile, reference_tile)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_align_to",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            reference_tile,
        )
    return _api.call(
        "gbx_rst_align_to",
        tile,
        reference_tile,
    )


def rst_asformat(
    tile: ColLike,
    new_format: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Re-encode the raster to another GDAL driver (for example PNG, GTiff or COG).

    Args:
        tile: The tile to re-encode.
        new_format: A GDAL driver short name that can write a raster, in any case:
            `'GTiff'`, `'PNG'`, `'COG'`. A name that names none raises.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile with its raster bytes encoded in `new_format` and its `driver` metadata
        set to the driver the bytes open as (`'COG'` is a layout of a GeoTIFF and opens
        as `'GTiff'`).

    SQL: ``gbx_rst_asformat(tile, new_format)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_asformat",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            new_format,
        )
    return _api.call(
        "gbx_rst_asformat",
        tile,
        new_format,
    )


def rst_aspect(
    tile: ColLike,
    trigonometric: ColLike = None,
    zero_for_flat: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute the direction of steepest descent from a single-band DEM tile (Horn's 3x3 method).

    Args:
        tile: The single-band DEM tile.
        trigonometric: `true` gives counter-clockwise degrees from east; `false` gives
            compass degrees clockwise from north. SQL default: ``false``.
        zero_for_flat: `true` gives 0 for flat cells; `false` leaves them NoData. SQL
            default: ``false``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile in degrees from 0 to 360; NoData is -9999.

    SQL: ``gbx_rst_aspect(tile, [trigonometric], [zero_for_flat])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_aspect",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            trigonometric,
            zero_for_flat,
        )
    return _api.call(
        "gbx_rst_aspect",
        tile,
        trigonometric,
        zero_for_flat,
    )


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


def rst_band(
    tile: ColLike,
    band_index: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Extract one band as a new single-band tile.

    Args:
        tile: The tile to take a band from.
        band_index: The 1-based band to extract, in `[1 .. numbands]`.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band tile with the band's pixels, NoData, data type and georeferencing.
        A band outside the tile raises.

    SQL: ``gbx_rst_band(tile, band_index)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_band",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            band_index,
        )
    return _api.call(
        "gbx_rst_band",
        tile,
        band_index,
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


def rst_chm(
    dsm_tile: ColLike,
    dem_tile: ColLike,
) -> Column:
    """Compute the canopy height model: the surface aligned to the terrain, minus the terrain, no lower than 0.

    Args:
        dsm_tile: The Digital Surface Model tile.
        dem_tile: The Digital Elevation Model tile; it defines the output grid.

    Returns:
        A single-band Float32 tile on the grid of `dem_tile`; NoData is -9999.

    SQL: ``gbx_rst_chm(dsm_tile, dem_tile)``
    """
    return _api.call(
        "gbx_rst_chm",
        dsm_tile,
        dem_tile,
    )


def rst_clip(
    tile: ColLike,
    geom: ColLike,
    cutline_all_touched: ColLike,
    clip_crs: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Clip the raster to a polygon.

    Args:
        tile: The tile to clip.
        geom: The cutline: a polygon as WKB, EWKB, WKT or EWKT.
        cutline_all_touched: `true` keeps every pixel the cutline touches; `false` keeps
            the pixels whose centre it holds.
        clip_crs: The CRS of a cutline that carries no SRID: `EPSG:<code>`,
            `ESRI:<code>`, WKT or an integer code. The CRS of the tile when NULL. An EWKB or
            EWKT SRID wins. SQL default: ``NULL``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile cropped to the polygon's envelope on the tile's grid, NoData outside
        the polygon; NULL when the polygon misses the tile.

    SQL: ``gbx_rst_clip(tile, geom, cutline_all_touched, [clip_crs])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_clip",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            geom,
            cutline_all_touched,
            clip_crs,
        )
    return _api.call(
        "gbx_rst_clip",
        tile,
        geom,
        cutline_all_touched,
        clip_crs,
    )


def rst_convolve(
    tile: ColLike,
    kernel: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Convolve each band with a 2-D kernel.

    Args:
        tile: The tile to convolve.
        kernel: A 2-D array of weights with an odd number of rows and columns, applied
            unflipped (correlation).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A Float64 tile: each band is the weighted sum of its neighbourhood, zero-padded
        at the edges. A NoData or masked neighbour adds 0; the result is never NoData.

    SQL: ``gbx_rst_convolve(tile, kernel)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_convolve",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            kernel,
        )
    return _api.call(
        "gbx_rst_convolve",
        tile,
        kernel,
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


def rst_derivedband(
    tile: ColLike,
    python_func: ColLike,
    func_name: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Apply a Python pixel function to the bands of the raster.

    Args:
        tile: The tile whose bands are the pixel function's input.
        python_func: Python source that defines the pixel function, with GDAL's VRT
            pixel-function signature `func(in_ar, out_ar, xoff, yoff, xsize, ysize,
            raster_xsize, raster_ysize, buf_radius, gt, **kwargs)`: `in_ar` is the list of
            band arrays and `out_ar` the output array the function fills in place.
        func_name: The name of the function in `python_func`. A source that does not
            parse, or that defines no such name, raises.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float64 tile of the function's output, with the tile's
        georeferencing; where the tile is transparent or masked the result is masked.

    SQL: ``gbx_rst_derivedband(tile, python_func, func_name)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_derivedband",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            python_func,
            func_name,
        )
    return _api.call(
        "gbx_rst_derivedband",
        tile,
        python_func,
        func_name,
    )


def rst_evi(
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    blue_idx: ColLike,
    l: ColLike = None,
    c1: ColLike = None,
    c2: ColLike = None,
    g: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute EVI = G * (NIR - Red) / (NIR + C1 * Red - C2 * Blue + L).

    Args:
        tile: The multi-band tile.
        red_idx: The 1-based band of red.
        nir_idx: The 1-based band of near infrared.
        blue_idx: The 1-based band of blue.
        l: The canopy background adjustment. SQL default: ``1.0``.
        c1: The aerosol coefficient for red. SQL default: ``6.0``.
        c2: The aerosol coefficient for blue. SQL default: ``7.5``.
        g: The gain factor. SQL default: ``2.5``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999, also where the denominator is 0.

    SQL: ``gbx_rst_evi(tile, red_idx, nir_idx, blue_idx, [l], [c1], [c2], [g])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_evi",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            red_idx,
            nir_idx,
            blue_idx,
            l,
            c1,
            c2,
            g,
        )
    return _api.call(
        "gbx_rst_evi",
        tile,
        red_idx,
        nir_idx,
        blue_idx,
        l,
        c1,
        c2,
        g,
    )


def rst_fillnodata(
    tile: ColLike,
    max_search_dist: ColLike = None,
    smoothing_iter: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Interpolate across NoData gaps in the raster.

    Args:
        tile: The tile to fill.
        max_search_dist: How far, in pixels, to search for valid pixels to interpolate
            from. SQL default: ``100.0``.
        smoothing_iter: The number of 3x3 smoothing passes over the filled pixels. SQL
            default: ``0``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile with the NoData pixels that have a valid pixel within `max_search_dist`
        filled; the rest stay NoData.

    SQL: ``gbx_rst_fillnodata(tile, [max_search_dist], [smoothing_iter])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_fillnodata",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            max_search_dist,
            smoothing_iter,
        )
    return _api.call(
        "gbx_rst_fillnodata",
        tile,
        max_search_dist,
        smoothing_iter,
    )


def rst_filter(
    tile: ColLike,
    kernel_size: ColLike,
    operation: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Apply a focal minimum, maximum, mean or median over a square window.

    Args:
        tile: The tile to filter.
        kernel_size: The side of the square window, a positive odd integer.
        operation: `'min'`, `'max'`, `'mean'` or `'median'`, in any case. Another word
            raises.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile with each band filtered over the valid pixels of the window, which
        shrinks at the edges; a window with no valid pixel is NoData. `'mean'` is
        Float32, the others keep the data type.

    SQL: ``gbx_rst_filter(tile, kernel_size, operation)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_filter",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            kernel_size,
            operation,
        )
    return _api.call(
        "gbx_rst_filter",
        tile,
        kernel_size,
        operation,
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


def rst_getsubdataset(
    tile: ColLike,
    subset_name: ColLike,
) -> Column:
    """Extract the named subdataset of a container raster as a tile.

    Args:
        tile: A tile whose raster is a container with subdatasets, such as a NetCDF
            file.
        subset_name: The name of the subdataset (the last part of its GDAL name, such as
            `'prAdjust'`). A name that matches none raises.

    Returns:
        The subdataset as a GeoTIFF tile.

    SQL: ``gbx_rst_getsubdataset(tile, subset_name)``
    """
    return _api.call(
        "gbx_rst_getsubdataset",
        tile,
        subset_name,
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


def rst_hillshade(
    tile: ColLike,
    azimuth: ColLike = None,
    altitude: ColLike = None,
    z_factor: ColLike = None,
    xscale: ColLike = None,
    yscale: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute shaded relief from a single-band DEM tile (Horn's 3x3 method).

    Args:
        tile: The single-band DEM tile.
        azimuth: Sun azimuth in degrees clockwise from north. SQL default: ``315.0``.
        altitude: Sun elevation above the horizon in degrees. SQL default: ``45.0``.
        z_factor: Vertical exaggeration applied to the gradients. SQL default: ``1.0``.
        xscale: Vertical units per horizontal unit along x; set it with `yscale`. When
            both are NULL, the scale comes from the raster CRS. SQL default: ``NULL``.
        yscale: Vertical units per horizontal unit along y; set it with `xscale`. SQL
            default: ``NULL``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Byte tile with values from 1 to 255; 0 is NoData.

    SQL: ``gbx_rst_hillshade(tile, [azimuth], [altitude], [z_factor], [xscale], [yscale])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_hillshade",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            azimuth,
            altitude,
            z_factor,
            xscale,
            yscale,
        )
    return _api.call(
        "gbx_rst_hillshade",
        tile,
        azimuth,
        altitude,
        z_factor,
        xscale,
        yscale,
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


def rst_index(
    tile: ColLike,
    formula_name: ColLike,
    band_map: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute a named spectral index from a map of band names to band numbers.

    Args:
        tile: The multi-band tile.
        formula_name: The index: `ndvi`, `gndvi`, `msavi`, `ndvi_re`, `ndmi` or `ndsi`
            (any case).
        band_map: The 1-based band of each name the index needs, such as `map('red', 1,
            'nir', 2)`.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999.

    SQL: ``gbx_rst_index(tile, formula_name, band_map)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_index",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            formula_name,
            band_map,
        )
    return _api.call(
        "gbx_rst_index",
        tile,
        formula_name,
        band_map,
    )


def rst_initnodata(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Ensure a NoData value is set on the raster; an existing value is kept.

    Args:
        tile: The tile to give a NoData value.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile with a NoData value: the one it declares, or the data type's fallback
        (for example 65535 for UInt16) when it declares none.

    SQL: ``gbx_rst_initnodata(tile)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_initnodata",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
        )
    return _api.call(
        "gbx_rst_initnodata",
        tile,
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


def rst_land_cover(
    tile: ColLike,
    method: ColLike = None,
    n_clusters: ColLike = None,
    smooth: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Classify an RGB tile into land-cover class ids.

    Args:
        tile: A tile of 3 or more bands: red, green, blue (and optionally
            near-infrared).
        method: `'spectral'` (thresholds on excess green and brightness), `'kmeans'` or
            `'hybrid'` (a k-means partition labelled by the same thresholds), in any case.
            Another word raises. SQL default: ``'hybrid'``.
        n_clusters: The k-means cluster count (`'kmeans'` and `'hybrid'`), 1 or more.
            SQL default: ``6``.
        smooth: The de-speckle window in pixels: isolated specks of a class are dropped;
            0 turns it off. SQL default: ``3``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Int32 tile of class ids: 0 vegetation, 1 bare, 2 impervious, 3
        dark; -1 (the tile's NoData) for near-black pixels and dropped specks. A tile of
        fewer than 3 bands is an error tile.

    SQL: ``gbx_rst_land_cover(tile, [method], [n_clusters], [smooth])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_land_cover",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            method,
            n_clusters,
            smooth,
        )
    return _api.call(
        "gbx_rst_land_cover",
        tile,
        method,
        n_clusters,
        smooth,
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


def rst_nbr(
    tile: ColLike,
    nir_idx: ColLike,
    swir_idx: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute NBR = (NIR - SWIR) / (NIR + SWIR).

    Args:
        tile: The multi-band tile.
        nir_idx: The 1-based band of near infrared.
        swir_idx: The 1-based band of shortwave infrared.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999, also where the denominator is 0.

    SQL: ``gbx_rst_nbr(tile, nir_idx, swir_idx)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_nbr",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            nir_idx,
            swir_idx,
        )
    return _api.call(
        "gbx_rst_nbr",
        tile,
        nir_idx,
        swir_idx,
    )


def rst_ndvi(
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute NDVI = (NIR - Red) / (NIR + Red).

    Args:
        tile: The multi-band tile.
        red_idx: The 1-based band of red.
        nir_idx: The 1-based band of near infrared.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999, also where the denominator is 0.

    SQL: ``gbx_rst_ndvi(tile, red_idx, nir_idx)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_ndvi",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            red_idx,
            nir_idx,
        )
    return _api.call(
        "gbx_rst_ndvi",
        tile,
        red_idx,
        nir_idx,
    )


def rst_ndwi(
    tile: ColLike,
    green_idx: ColLike,
    nir_idx: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute NDWI = (Green - NIR) / (Green + NIR).

    Args:
        tile: The multi-band tile.
        green_idx: The 1-based band of green.
        nir_idx: The 1-based band of near infrared.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999, also where the denominator is 0.

    SQL: ``gbx_rst_ndwi(tile, green_idx, nir_idx)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_ndwi",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            green_idx,
            nir_idx,
        )
    return _api.call(
        "gbx_rst_ndwi",
        tile,
        green_idx,
        nir_idx,
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


def rst_percentile_stretch(
    tile: ColLike,
    lo_pct: ColLike,
    hi_pct: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Stretch the contrast of each band between two percentiles, to 8 bits.

    Args:
        tile: The tile to stretch.
        lo_pct: The lower percentile, in `[0, 100]` and below `hi_pct`: the value at it
            becomes 0.
        hi_pct: The upper percentile, in `[0, 100]`: the value at it becomes 255.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A UInt8 tile with the bands of the input, each stretched on its own valid pixels
        between its two percentiles; a NoData, masked or transparent pixel is 0, which
        is the NoData value.

    SQL: ``gbx_rst_percentile_stretch(tile, lo_pct, hi_pct)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_percentile_stretch",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            lo_pct,
            hi_pct,
        )
    return _api.call(
        "gbx_rst_percentile_stretch",
        tile,
        lo_pct,
        hi_pct,
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


def rst_proximity(
    tile: ColLike,
    target_values: ColLike = None,
    dist_units: ColLike = None,
    max_distance: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute a proximity raster: each pixel's distance to the nearest source pixel.

    Args:
        tile: The tile to measure: its first band.
        target_values: The source pixel values as a comma-separated list, such as
            `'1,2'`. NULL makes every pixel that is not 0 a source. SQL default: ``NULL``.
        dist_units: `'GEO'` measures in the units of the tile's CRS; `'PIXEL'` counts
            pixels. NULL is `'GEO'`. SQL default: ``'GEO'``.
        max_distance: The largest distance to compute, above 0: farther pixels are
            NoData. NULL for no limit. SQL default: ``NULL``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile on the grid of the input: 0 on a source pixel, NoData
        (-1) beyond `max_distance`.

    SQL: ``gbx_rst_proximity(tile, [target_values], [dist_units], [max_distance])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_proximity",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            target_values,
            dist_units,
            max_distance,
        )
    return _api.call(
        "gbx_rst_proximity",
        tile,
        target_values,
        dist_units,
        max_distance,
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


def rst_resample(
    tile: ColLike,
    factor: ColLike,
    algorithm: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Resample a raster tile by a multiplicative factor.

    Args:
        tile: The tile to resample.
        factor: The multiplicative factor, above 0: above 1 upsamples, below 1
            downsamples.
        algorithm: The resampling algorithm: `near`, `bilinear`, `cubic`, `cubicspline`,
            `lanczos`, `average`, `mode`, `max`, `min`, `med`, `q1` or `q3` (any case;
            `nearest` is accepted as `near`). A word that names none raises. SQL default:
            ``'bilinear'``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile on a grid `factor` times as large in each direction; the CRS and the
        extent stay.

    SQL: ``gbx_rst_resample(tile, factor, [algorithm])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_resample",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            factor,
            algorithm,
        )
    return _api.call(
        "gbx_rst_resample",
        tile,
        factor,
        algorithm,
    )


def rst_resample_to_res(
    tile: ColLike,
    x_res: ColLike,
    y_res: ColLike,
    algorithm: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Resample a raster tile to a target ground resolution.

    Args:
        tile: The tile to resample.
        x_res: The size of a pixel along x in the units of the tile's CRS, above 0.
        y_res: The size of a pixel along y in the units of the tile's CRS, above 0.
        algorithm: The resampling algorithm: `near`, `bilinear`, `cubic`, `cubicspline`,
            `lanczos`, `average`, `mode`, `max`, `min`, `med`, `q1` or `q3` (any case;
            `nearest` is accepted as `near`). A word that names none raises. SQL default:
            ``'bilinear'``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile on a grid of `x_res` by `y_res` pixels; the CRS and the extent stay.

    SQL: ``gbx_rst_resample_to_res(tile, x_res, y_res, [algorithm])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_resample_to_res",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            x_res,
            y_res,
            algorithm,
        )
    return _api.call(
        "gbx_rst_resample_to_res",
        tile,
        x_res,
        y_res,
        algorithm,
    )


def rst_resample_to_size(
    tile: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    algorithm: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Resample a raster tile to exact pixel dimensions.

    Args:
        tile: The tile to resample.
        width_px: The width of the result in pixels, above 0.
        height_px: The height of the result in pixels, above 0.
        algorithm: The resampling algorithm: `near`, `bilinear`, `cubic`, `cubicspline`,
            `lanczos`, `average`, `mode`, `max`, `min`, `med`, `q1` or `q3` (any case;
            `nearest` is accepted as `near`). A word that names none raises. SQL default:
            ``'bilinear'``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile on a `width_px` by `height_px` grid; the CRS and the extent stay.

    SQL: ``gbx_rst_resample_to_size(tile, width_px, height_px, [algorithm])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_resample_to_size",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            width_px,
            height_px,
            algorithm,
        )
    return _api.call(
        "gbx_rst_resample_to_size",
        tile,
        width_px,
        height_px,
        algorithm,
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


def rst_roughness(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute terrain roughness from a single-band DEM tile.

    Args:
        tile: The single-band DEM tile.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile holding the largest minus the smallest value of each
        3x3 window; NoData is -9999.

    SQL: ``gbx_rst_roughness(tile)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_roughness",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
        )
    return _api.call(
        "gbx_rst_roughness",
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


def rst_savi(
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    l: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute SAVI = (NIR - Red) / (NIR + Red + L) * (1 + L).

    Args:
        tile: The multi-band tile.
        red_idx: The 1-based band of red.
        nir_idx: The 1-based band of near infrared.
        l: The soil-brightness correction; 0 reduces SAVI to NDVI. SQL default: ``0.5``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999, also where the denominator is 0.

    SQL: ``gbx_rst_savi(tile, red_idx, nir_idx, [l])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_savi",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            red_idx,
            nir_idx,
            l,
        )
    return _api.call(
        "gbx_rst_savi",
        tile,
        red_idx,
        nir_idx,
        l,
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


def rst_setcrs(
    tile: ColLike,
    crs: ColLike,
) -> Column:
    """Stamp a CRS on the raster without reprojecting it, from a CRS string.

    Args:
        tile: The tile to relabel.
        crs: The CRS to stamp: an authority code such as `'EPSG:3857'` or
            `'ESRI:54008'`, WKT, PROJ4, PROJJSON, or an integer string (an EPSG code). A
            value that is no CRS raises.

    Returns:
        The tile with the same pixels and geotransform and the CRS stamped; nothing is
        reprojected.

    SQL: ``gbx_rst_setcrs(tile, crs)``
    """
    return _api.call(
        "gbx_rst_setcrs",
        tile,
        crs,
    )


def rst_setsrid(
    tile: ColLike,
    srid: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Stamp the CRS of a raster from an integer SRID without reprojecting it.

    Args:
        tile: The tile to relabel.
        srid: The code to stamp, an EPSG or ESRI code; `0` clears the CRS. A negative
            number, or a positive code that is neither, raises.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile with the same pixels and geotransform and the CRS `EPSG:<srid>`;
        nothing is reprojected.

    SQL: ``gbx_rst_setsrid(tile, srid)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_setsrid",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            srid,
        )
    return _api.call(
        "gbx_rst_setsrid",
        tile,
        srid,
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


def rst_threshold(
    tile: ColLike,
    op: ColLike = None,
    value: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Keep the pixels that satisfy the comparison and set the others to NoData.

    Args:
        tile: The tile to threshold.
        op: The comparison: `>`, `<`, `>=`, `<=`, `==` or `!=`. SQL default: ``'>'``.
        value: The value to compare each pixel with. SQL default: ``0.0``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A tile with the dtype and band count of the input.

    SQL: ``gbx_rst_threshold(tile, [op], [value])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_threshold",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            op,
            value,
        )
    return _api.call(
        "gbx_rst_threshold",
        tile,
        op,
        value,
    )


def rst_to_webmercator(
    tile: ColLike,
    resampling: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Reproject the tile to EPSG:3857 (web mercator).

    Args:
        tile: The tile to reproject.
        resampling: The resampling algorithm: `near`, `bilinear`, `cubic`,
            `cubicspline`, `lanczos`, `average`, `mode`, `max`, `min`, `med`, `q1` or `q3`
            (any case; `nearest` is accepted as `near`). A word that names none raises. SQL
            default: ``'bilinear'``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile in EPSG:3857; an error tile with the reason when the tile has no CRS.

    SQL: ``gbx_rst_to_webmercator(tile, [resampling])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_to_webmercator",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            resampling,
        )
    return _api.call(
        "gbx_rst_to_webmercator",
        tile,
        resampling,
    )


def rst_tpi(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute the Topographic Position Index from a single-band DEM tile.

    Args:
        tile: The single-band DEM tile.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; positive on ridges, negative in valleys; NoData is
        -9999.

    SQL: ``gbx_rst_tpi(tile)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_tpi",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
        )
    return _api.call(
        "gbx_rst_tpi",
        tile,
    )


def rst_transform(
    tile: ColLike,
    srid: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Reproject the raster to the target SRID (an EPSG code).

    Args:
        tile: The tile to reproject.
        srid: The EPSG (or ESRI) code of the target CRS, a positive integer.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        The tile in the target CRS; the tile as it is when it already has that CRS. An
        error tile with the reason when the tile has no CRS or the SRID names none.

    SQL: ``gbx_rst_transform(tile, srid)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_transform",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            srid,
        )
    return _api.call(
        "gbx_rst_transform",
        tile,
        srid,
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


def rst_tri(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute the Terrain Ruggedness Index (Riley) from a single-band DEM tile.

    Args:
        tile: The single-band DEM tile.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A single-band Float32 tile; NoData is -9999.

    SQL: ``gbx_rst_tri(tile)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_tri",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
        )
    return _api.call(
        "gbx_rst_tri",
        tile,
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


def rst_updatetype(
    tile: ColLike,
    new_type: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Cast all bands of the raster to another data type.

    Args:
        tile: The tile to cast.
        new_type: A GDAL data type name, in any case: `Byte`, `Int8`, `UInt16`, `Int16`,
            `UInt32`, `Int32`, `Float32` or `Float64`. Another word raises.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A GeoTIFF tile with every band cast to the type: a floating-point value cast to
        an integer type is rounded and clamped to the type's range; a NoData value the
        type cannot hold is dropped.

    SQL: ``gbx_rst_updatetype(tile, new_type)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_updatetype",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            new_type,
        )
    return _api.call(
        "gbx_rst_updatetype",
        tile,
        new_type,
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
