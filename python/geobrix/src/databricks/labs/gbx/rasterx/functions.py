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
    # The JAR registers a scalar under every Scala name, including names the manifest serves another way (a light
    # table function or UDF, a BINARY aggregate, a light-preferred row): put the manager's choice back for every
    # name, before the fromfile override below.
    from databricks.labs.gbx import manager

    manager.reapply_manager_choice(_spark, "gbx_rst_")

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
        from databricks.labs.gbx.ds.file_gbx import report_detected_cap
        from databricks.labs.gbx.pyrx.functions import (
            _build_fromfile_sql_materialized_udf,
        )

        # the light implementation it registers carries the driver's remote-path switch to the workers, and the
        # driver's connect-aware materialize cap (a worker cannot measure it)
        _gdal_policy.carrying_session(_spark).udf.register(
            "gbx_rst_fromfile",
            _build_fromfile_sql_materialized_udf(report_detected_cap(_spark)),
        )
    except (
        Exception
    ):  # noqa: BLE001 - pyrx/light-tier not installed: keep the Scala fallback
        pass


# Aggregators


# Constructors


# Generators


# Grid


# Operations


# ---------------------------------------------------------------------------
# Terrain analysis (DEM processing)
#
# Seven thin wrappers around gdal.DEMProcessing. All take a single source tile
# and return a derived tile. Defaults match the GDAL conventions.
# ---------------------------------------------------------------------------


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
# Three resample wrappers delegate to gdal.Warp with -tr / -ts; the IDW aggregator
# (`rst_gridfrompoints_agg`) delegates to gdal.Grid with the invdist algorithm.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Delaunay-TIN Digital Terrain Model (DTM) interpolation
#
# `rst_dtmfromgeoms_agg` (aggregator, one Z-valued point per row) delegates to
# gbx_rst_dtmfromgeoms_agg. The non-aggregator form is generated from the manifest.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Pixel ops + extraction
#
# Seven thin wrappers over GDAL per-pixel / per-tile primitives that the
# rest of the RasterX surface assumed were "always available" but weren't
# actually exposed: FillNodata, ReadRaster-at-point sampling, SetProjection,
# GetHistogram, threshold (via MapAlgebra), BuildOverviews, single-band
# extraction.
# ---------------------------------------------------------------------------


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
    statistic: ColLike = None,
) -> Column:
    """Bin scattered points into a raster by a statistic of their values.

    Args:
        x_array: The x coordinate of each point.
        y_array: The y coordinate of each point, in the same order.
        z_array: The value of each point, in the same order.
        xmin: The west edge of the output in the output CRS.
        ymin: The south edge of the output.
        xmax: The east edge of the output, above `xmin`.
        ymax: The north edge of the output, above `ymin`.
        width_px: The output width in pixels, above 0.
        height_px: The output height in pixels, above 0.
        srid: The EPSG or ESRI code of the output CRS.
        statistic: How the points in a pixel combine: `max`, `min`, `mean`, `median`,
            `count` or `percentile:<p>` (for example `percentile:90`), in any case. Another
            word raises. SQL default: ``'max'``.

    Returns:
        A single-band Float32 tile over the extent; a point on the east or south edge is
        left out, and a pixel with no point is NoData (-9999).

    SQL: ``gbx_rst_binpoints(x_array, y_array, z_array, xmin, ymin, xmax, ymax, width_px, height_px, srid, [statistic])``
    """
    return _api.call(
        "gbx_rst_binpoints",
        x_array,
        y_array,
        z_array,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        srid,
        statistic,
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
    statistic: ColLike = None,
) -> Column:
    """Bin a group's points into one single-band tile.

    Args:
        x: The point's x.
        y: The point's y.
        z: The point's value.
        xmin: The west edge of the output.
        ymin: The south edge.
        xmax: The east edge.
        ymax: The north edge.
        width_px: The output width in pixels.
        height_px: The output height in pixels.
        srid: The EPSG code of the output CRS.
        statistic: `'max'`, `'min'`, `'mean'`, `'median'`, `'count'` or
            `'percentile:<p>'`. SQL default: ``'max'``.

    Returns:
        GeoTIFF bytes, Float32; a pixel with no point is NoData; NULL for a group with
        no point.

    SQL: ``gbx_rst_binpoints_agg(x, y, z, xmin, ymin, xmax, ymax, width_px, height_px, srid, [statistic])``
    """
    return _api.call(
        "gbx_rst_binpoints_agg",
        x,
        y,
        z,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        srid,
        statistic,
    )


def rst_bng_rasterize(
    cells: ColLike,
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
    """Rasterize an array of BNG cells into one tile (pixel-centroid burn).

    Args:
        cells: The group's cells as an array of structs `(cellid, value)`, for example
            `collect_list(struct(cellid, value))`. The rules of the aggregate hold: a
            repeated cell burns its largest value, a NULL cell id is skipped.
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
        The same bytes as the aggregate `gbx_rst_bng_rasterize_agg` over the same cells:
        GeoTIFF, Float64, NoData -9999; NULL for an array with no cell.

    SQL: ``gbx_rst_bng_rasterize(cells, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad, [out_crs])``
    """
    return _api.call(
        "gbx_rst_bng_rasterize",
        cells,
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
        value: The value burned for the cell. A repeated cell burns its largest value; a
            NULL value in a typed column never wins against a number, and a cell of nothing
            but NULL burns NaN. An untyped NULL (the argument left out, or `None` in Python)
            is the presence mask and burns 1.0.
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
        GeoTIFF bytes, Float64 with NoData -9999, in EPSG:27700 over the canvas; NULL
        for a group with no cell. The same on every compute.

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


def rst_bng_rasterize_tvf(
    df: DataFrame,
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    width: int,
    height: int,
) -> DataFrame:
    """Rasterize each partition's BNG cells onto an explicit EPSG:27700 canvas, as a table function over `TABLE(...) PARTITION BY`.

    Args:
        df: The input DataFrame, with the columns ``key``, ``cellid``, ``value`` (others
            are left out); it is partitioned by ``key``.
        xmin: The west edge of the canvas in EPSG:27700 metres.
        ymin: The south edge of the canvas in EPSG:27700 metres.
        xmax: The east edge of the canvas in EPSG:27700 metres.
        ymax: The north edge of the canvas in EPSG:27700 metres.
        width: The canvas width in pixels.
        height: The canvas height in pixels.

    Returns:
        One row per partition: its `key` and the `raster` (GeoTIFF bytes, Float64 with
        NoData -9999, in EPSG:27700 over the canvas). A partition with no cell gives no
        row.

    SQL: ``gbx_rst_bng_rasterize_tvf(rows, xmin, ymin, xmax, ymax, width, height)``
    """
    return _api.table_by(
        df,
        "gbx_rst_bng_rasterize_tvf",
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
    )


def rst_bng_rastertogridavg(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Mean of the pixel values in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the mean of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:27700 (nearest neighbour, so values are
        never interpolated); pixels outside Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridavg(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridavg",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_rastertogridcount(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Count the pixels in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the number
        of the band's valid pixels in the cell (a DOUBLE; with `covering`, the sum of
        their area shares). A pixel is valid when the band's NoData and mask (and the
        alpha band, if the raster has one) say so. A NULL tile, a NULL resolution, an
        error tile and a tile that cannot be read give no rows. A raster in another CRS
        is reprojected to EPSG:27700 (nearest neighbour, so values are never
        interpolated); pixels outside Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridcount(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridcount",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_rastertogridmax(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Maximum of the pixel values in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the largest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution, an error tile and a tile that cannot be read give no rows. A
        raster in another CRS is reprojected to EPSG:27700 (nearest neighbour, so values
        are never interpolated); pixels outside Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridmax(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridmax",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_rastertogridmedian(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Median of the pixel values in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the median
        of the band's valid pixels in the cell. A pixel is valid when the band's NoData
        and mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:27700 (nearest neighbour, so values are
        never interpolated); pixels outside Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridmedian(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridmedian",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_rastertogridmin(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Minimum of the pixel values in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the smallest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution, an error tile and a tile that cannot be read give no rows. A
        raster in another CRS is reprojected to EPSG:27700 (nearest neighbour, so values
        are never interpolated); pixels outside Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridmin(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridmin",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_rastertogridstddev(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population standard deviation of the pixel values in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population standard deviation (divisor n) of the band's valid pixels in the
        cell. A pixel is valid when the band's NoData and mask (and the alpha band, if
        the raster has one) say so. A NULL tile, a NULL resolution, an error tile and a
        tile that cannot be read give no rows. A raster in another CRS is reprojected to
        EPSG:27700 (nearest neighbour, so values are never interpolated); pixels outside
        Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridstddev(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridstddev",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_rastertogridsum(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Sum of the pixel values in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the sum of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:27700 (nearest neighbour, so values are
        never interpolated); pixels outside Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridsum(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridsum",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_rastertogridvariance(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population variance of the pixel values in each BNG cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population variance (divisor n) of the band's valid pixels in the cell. A pixel
        is valid when the band's NoData and mask (and the alpha band, if the raster has
        one) say so. A NULL tile, a NULL resolution, an error tile and a tile that
        cannot be read give no rows. A raster in another CRS is reprojected to
        EPSG:27700 (nearest neighbour, so values are never interpolated); pixels outside
        Great Britain are left out.

    SQL: ``gbx_rst_bng_rastertogridvariance(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_rastertogridvariance",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_bng_tessellate(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    assignment: ColLike = None,
    coverage: ColLike = None,
) -> DataFrame:
    """Cut a tile in EPSG:27700 into BNG cells, one tile per cell.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.
        resolution: BNG resolution: an index (1 to 6, negative for a quadrant) or a size
            such as `'1km'` or `'100m'`.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps). Any other word raises.
            SQL default: ``'centroid'``.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, as all-NoData tiles; `'sparse'` leaves them out. Any other word
            raises. SQL default: ``'complete'``.

    Returns:
        One row per tile, with the columns of a tile. A NULL tile gives no rows. An
        error tile comes out as itself. A tile that cannot be read is one error tile.
        Each tile is clipped to its cell and carries the cell id in `cellid`.

    SQL: ``gbx_rst_bng_tessellate(tile, resolution, [assignment], [coverage])``
    """
    return _api.table(
        df,
        "gbx_rst_bng_tessellate",
        tile,
        resolution,
        assignment,
        coverage,
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


def rst_buildoverviews(
    tile: ColLike,
    levels: ColLike,
    resampling: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Build internal pyramid overviews at the given decimation levels.

    Args:
        tile: The tile to add overviews to.
        levels: The decimation factors, such as `array(2, 4, 8)`: each at least 2.
        resampling: The overview resampling: `nearest`, `average`, `gauss`, `cubic`,
            `cubicspline`, `lanczos`, `bilinear` or `mode`. Any other word raises. SQL
            default: ``'average'``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A GeoTIFF tile with the same pixels and one overview per level. A NULL tile or
        NULL levels gives NULL; an error tile comes out as itself; a tile that cannot be
        read gives an error tile.

    SQL: ``gbx_rst_buildoverviews(tile, levels, [resampling])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_buildoverviews",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            levels,
            resampling,
        )
    return _api.call(
        "gbx_rst_buildoverviews",
        tile,
        levels,
        resampling,
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


def rst_cog_convert(
    tile: ColLike,
    compression: ColLike = None,
    blocksize: ColLike = None,
    overview_resampling: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Convert a tile to a Cloud Optimized GeoTIFF (COG).

    Args:
        tile: The tile to convert.
        compression: `'AUTO'` (a size-adaptive ZSTD with the predictor for the data
            type) or a codec: `ZSTD`, `DEFLATE`, `LZW`, `LERC`, `JPEG`, `WEBP` or `NONE`.
            Any other word raises. SQL default: ``'AUTO'``.
        blocksize: The internal block size in pixels, square: a multiple of 16. Any
            other value raises. SQL default: ``512``.
        overview_resampling: The resampling of the overview pyramid, such as `AVERAGE`.
            SQL default: ``'AVERAGE'``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A GeoTIFF tile in COG layout with the same pixels. A NULL tile gives NULL; an
        error tile comes out as itself; a tile that cannot be read gives an error tile.

    SQL: ``gbx_rst_cog_convert(tile, [compression], [blocksize], [overview_resampling])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_cog_convert",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            compression,
            blocksize,
            overview_resampling,
        )
    return _api.call(
        "gbx_rst_cog_convert",
        tile,
        compression,
        blocksize,
        overview_resampling,
    )


def rst_color_relief(
    tile: ColLike,
    color_table_path: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Colour a DEM through a `gdaldem` colour table.

    Args:
        tile: The single-band DEM tile.
        color_table_path: The path of a `gdaldem` colour file: one `elevation R G B [A]`
            line per stop, with `nv` for NoData and `N%` for a share of the value range. The
            file must exist.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A Byte tile of 3 bands (RGB), or 4 (RGBA) when a stop has an alpha column. A
        NULL tile or path gives NULL; an error tile comes out as itself; a tile that
        cannot be read gives an error tile.

    SQL: ``gbx_rst_color_relief(tile, color_table_path)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_color_relief",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            color_table_path,
        )
    return _api.call(
        "gbx_rst_color_relief",
        tile,
        color_table_path,
    )


def rst_combineavg(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Per-pixel mean across an array of aligned tiles, NoData aware.

    Args:
        tiles: The tiles to combine, in an array: aligned tiles that share size, extent
            and CRS (use `gbx_rst_align_to` first).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        Mean of the valid values at each pixel; a pixel that is NoData in every tile
        stays NoData. The output cellid is the tiles' shared cellid, or -1 when they
        differ. A NULL or empty array gives NULL; an array of only error tiles gives the
        first one; a tile that cannot be read is skipped. Tiles of different shapes
        raise; an array over the memory budget gives an error tile with a `Fix:`.

    SQL: ``gbx_rst_combineavg(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_combineavg",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_combineavg",
        tiles,
    )


def rst_combineavg_agg(
    tile: ColLike,
) -> Column:
    """Per-pixel mean across a group's aligned tiles, ignoring NoData.

    Args:
        tile: The tile of one member. An error tile or a tile that cannot be read is
            skipped.

    Returns:
        GeoTIFF bytes of the mean, on the grid of the first tile; NULL for a group with
        no usable tile. Tiles of different shapes raise.

    SQL: ``gbx_rst_combineavg_agg(tile)``
    """
    return _api.call(
        "gbx_rst_combineavg_agg",
        tile,
    )


def rst_combinecount(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Per-pixel count of valid values across an array of aligned tiles.

    Args:
        tiles: The tiles to combine, in an array: aligned tiles that share size, extent
            and CRS (use `gbx_rst_align_to` first).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        Number of valid values at each pixel; a pixel that is NoData in every tile is
        NoData, never 0. A NULL or empty array gives NULL; an array of only error tiles
        gives the first one; a tile that cannot be read is skipped. Tiles of different
        shapes raise; an array over the memory budget gives an error tile with a `Fix:`.

    SQL: ``gbx_rst_combinecount(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_combinecount",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_combinecount",
        tiles,
    )


def rst_combinemax(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Per-pixel maximum across an array of aligned tiles, NoData aware.

    Args:
        tiles: The tiles to combine, in an array: aligned tiles that share size, extent
            and CRS (use `gbx_rst_align_to` first).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        Largest valid value at each pixel; a pixel that is NoData in every tile stays
        NoData. A NULL or empty array gives NULL; an array of only error tiles gives the
        first one; a tile that cannot be read is skipped. Tiles of different shapes
        raise; an array over the memory budget gives an error tile with a `Fix:`.

    SQL: ``gbx_rst_combinemax(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_combinemax",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_combinemax",
        tiles,
    )


def rst_combinemedian(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Per-pixel median across an array of aligned tiles, NoData aware.

    Args:
        tiles: The tiles to combine, in an array: aligned tiles that share size, extent
            and CRS (use `gbx_rst_align_to` first).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        Median of the valid values at each pixel (the mean of the two middle values for
        an even count); a pixel that is NoData in every tile stays NoData. A NULL or
        empty array gives NULL; an array of only error tiles gives the first one; a tile
        that cannot be read is skipped. Tiles of different shapes raise; an array over
        the memory budget gives an error tile with a `Fix:`.

    SQL: ``gbx_rst_combinemedian(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_combinemedian",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_combinemedian",
        tiles,
    )


def rst_combinemin(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Per-pixel minimum across an array of aligned tiles, NoData aware.

    Args:
        tiles: The tiles to combine, in an array: aligned tiles that share size, extent
            and CRS (use `gbx_rst_align_to` first).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        Smallest valid value at each pixel; a pixel that is NoData in every tile stays
        NoData. A NULL or empty array gives NULL; an array of only error tiles gives the
        first one; a tile that cannot be read is skipped. Tiles of different shapes
        raise; an array over the memory budget gives an error tile with a `Fix:`.

    SQL: ``gbx_rst_combinemin(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_combinemin",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_combinemin",
        tiles,
    )


def rst_combinestddev(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Per-pixel population standard deviation across an array of aligned tiles, NoData aware.

    Args:
        tiles: The tiles to combine, in an array: aligned tiles that share size, extent
            and CRS (use `gbx_rst_align_to` first).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        Population standard deviation (divisor n) of the valid values at each pixel; a
        pixel that is NoData in every tile stays NoData. A NULL or empty array gives
        NULL; an array of only error tiles gives the first one; a tile that cannot be
        read is skipped. Tiles of different shapes raise; an array over the memory
        budget gives an error tile with a `Fix:`.

    SQL: ``gbx_rst_combinestddev(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_combinestddev",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_combinestddev",
        tiles,
    )


def rst_combinesum(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Per-pixel sum across an array of aligned tiles, NoData aware.

    Args:
        tiles: The tiles to combine, in an array: aligned tiles that share size, extent
            and CRS (use `gbx_rst_align_to` first).
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        Sum of the valid values at each pixel; a pixel that is NoData in every tile
        stays NoData. A NULL or empty array gives NULL; an array of only error tiles
        gives the first one; a tile that cannot be read is skipped. Tiles of different
        shapes raise; an array over the memory budget gives an error tile with a `Fix:`.

    SQL: ``gbx_rst_combinesum(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_combinesum",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_combinesum",
        tiles,
    )


def rst_contour(
    tile: ColLike,
    levels: ColLike,
    interval: ColLike = None,
    base: ColLike = None,
    attr_field: ColLike = None,
) -> Column:
    """Trace contour lines of the first band at given values or at a regular interval.

    Args:
        tile: The tile to trace.
        levels: The values to trace; NULL or empty to use `interval` and `base`.
        interval: The spacing of the lines, used when `levels` is empty; above 0. SQL
            default: ``0.0``.
        base: The value one line is drawn at, used with `interval`. SQL default:
            ``0.0``.
        attr_field: Accepted for compatibility; the value is always the struct field
            `value`. SQL default: ``'elev'``.

    Returns:
        An array of structs, one per line: `geom_wkb` (a WKB line in the tile's CRS) and
        `value`. NULL for a NULL tile.

    SQL: ``gbx_rst_contour(tile, levels, [interval], [base], [attr_field])``
    """
    return _api.call(
        "gbx_rst_contour",
        tile,
        levels,
        interval,
        base,
        attr_field,
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


def rst_custom_rasterize(
    cells: ColLike,
    grid: ColLike,
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
) -> Column:
    """Rasterize an array of custom cells into one tile (pixel-centroid burn).

    Args:
        cells: The group's cells as an array of structs `(cellid, value)`, for example
            `collect_list(struct(cellid, value))`. The rules of the aggregate hold: a
            repeated cell burns its largest value, a NULL cell id is skipped.
        grid: The grid, from `gbx_custom_grid`. The same for every row of the group.
        out_srid: The EPSG code of the output CRS.
        pixel_size: The pixel size in CRS units; NULL takes it from the cells'
            resolution.
        xmin: The west edge of the canvas; NULL derives the canvas from the cells.
        ymin: The south edge of the canvas.
        xmax: The east edge of the canvas.
        ymax: The north edge of the canvas.
        width: The canvas width in pixels.
        height: The canvas height in pixels.
        mode: How the canvas is derived when no extent is given: `'centroids'` or
            `'spatial_envelope'`.
        kring_pad: Rings of padding around the derived canvas.

    Returns:
        The same bytes as the aggregate `gbx_rst_custom_rasterize_agg` over the same
        cells: GeoTIFF, Float64, NoData -9999; NULL for an array with no cell.

    SQL: ``gbx_rst_custom_rasterize(cells, grid, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad)``
    """
    return _api.call(
        "gbx_rst_custom_rasterize",
        cells,
        grid,
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
    )


def rst_custom_rasterize_agg(
    cellid: ColLike,
    value: ColLike,
    grid: ColLike,
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
) -> Column:
    """Rasterize a group's custom-grid cells into one tile (pixel-centroid burn).

    Args:
        cellid: The custom-grid cell id.
        value: The value burned for the cell. A repeated cell burns its largest value; a
            NULL value in a typed column never wins against a number, and a cell of nothing
            but NULL burns NaN. An untyped NULL (the argument left out, or `None` in Python)
            is the presence mask and burns 1.0.
        grid: The grid, from `gbx_custom_grid`. The same for every row of the group.
        out_srid: The EPSG code of the output CRS.
        pixel_size: The pixel size in CRS units; NULL takes it from the cells'
            resolution.
        xmin: The west edge of the canvas; NULL derives the canvas from the cells.
        ymin: The south edge of the canvas.
        xmax: The east edge of the canvas.
        ymax: The north edge of the canvas.
        width: The canvas width in pixels.
        height: The canvas height in pixels.
        mode: How the canvas is derived when no extent is given: `'centroids'` or
            `'spatial_envelope'`.
        kring_pad: Rings of padding around the derived canvas.

    Returns:
        GeoTIFF bytes, Float64 with NoData -9999, over the canvas; NULL for a group with
        no cell. The same on every compute.

    SQL: ``gbx_rst_custom_rasterize_agg(cellid, value, grid, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad)``
    """
    return _api.call(
        "gbx_rst_custom_rasterize_agg",
        cellid,
        value,
        grid,
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
    )


def rst_custom_rastertogridavg(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Mean of the pixel values in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the mean of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution or grid, an error tile and a tile that cannot be read give no rows.
        The raster is not reprojected: it must already be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridavg(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridavg",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_rastertogridcount(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Count the pixels in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the number
        of the band's valid pixels in the cell (a DOUBLE; with `covering`, the sum of
        their area shares). A pixel is valid when the band's NoData and mask (and the
        alpha band, if the raster has one) say so. A NULL tile, a NULL resolution or
        grid, an error tile and a tile that cannot be read give no rows. The raster is
        not reprojected: it must already be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridcount(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridcount",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_rastertogridmax(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Maximum of the pixel values in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the largest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution or grid, an error tile and a tile that cannot be read give no
        rows. The raster is not reprojected: it must already be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridmax(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridmax",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_rastertogridmedian(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Median of the pixel values in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the median
        of the band's valid pixels in the cell. A pixel is valid when the band's NoData
        and mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution or grid, an error tile and a tile that cannot be read give no rows.
        The raster is not reprojected: it must already be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridmedian(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridmedian",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_rastertogridmin(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Minimum of the pixel values in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the smallest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution or grid, an error tile and a tile that cannot be read give no
        rows. The raster is not reprojected: it must already be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridmin(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridmin",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_rastertogridstddev(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population standard deviation of the pixel values in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population standard deviation (divisor n) of the band's valid pixels in the
        cell. A pixel is valid when the band's NoData and mask (and the alpha band, if
        the raster has one) say so. A NULL tile, a NULL resolution or grid, an error
        tile and a tile that cannot be read give no rows. The raster is not reprojected:
        it must already be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridstddev(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridstddev",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_rastertogridsum(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Sum of the pixel values in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the sum of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution or grid, an error tile and a tile that cannot be read give no rows.
        The raster is not reprojected: it must already be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridsum(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridsum",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_rastertogridvariance(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population variance of the pixel values in each custom-grid cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population variance (divisor n) of the band's valid pixels in the cell. A pixel
        is valid when the band's NoData and mask (and the alpha band, if the raster has
        one) say so. A NULL tile, a NULL resolution or grid, an error tile and a tile
        that cannot be read give no rows. The raster is not reprojected: it must already
        be in the grid's CRS.

    SQL: ``gbx_rst_custom_rastertogridvariance(tile, grid, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_rastertogridvariance",
        tile,
        grid,
        resolution,
        coverage,
        assignment,
    )


def rst_custom_tessellate(
    df: DataFrame,
    tile: ColLike,
    grid: ColLike,
    resolution: ColLike,
    assignment: ColLike = None,
    coverage: ColLike = None,
) -> DataFrame:
    """Cut a tile into custom-grid cells, one tile per cell.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.
        grid: The grid, as built by `gbx_custom_grid`.
        resolution: The grid resolution: 0 is the root cells, each level splits a cell
            into `cell_splits` by `cell_splits`.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps). Any other word raises.
            SQL default: ``'centroid'``.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, as all-NoData tiles; `'sparse'` leaves them out. Any other word
            raises. SQL default: ``'complete'``.

    Returns:
        One row per tile, with the columns of a tile. A NULL tile gives no rows. An
        error tile comes out as itself. A tile that cannot be read is one error tile.
        Each tile is clipped to its cell and carries the cell id in `cellid`.

    SQL: ``gbx_rst_custom_tessellate(tile, grid, resolution, [assignment], [coverage])``
    """
    return _api.table(
        df,
        "gbx_rst_custom_tessellate",
        tile,
        grid,
        resolution,
        assignment,
        coverage,
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


def rst_derivedband_agg(
    tile: ColLike,
    python_func: ColLike,
    func_name: ColLike,
) -> Column:
    """Apply a GDAL VRT pixel function across a group's tiles.

    Args:
        tile: One input band per tile.
        python_func: The source of a GDAL VRT pixel function. It runs in the process:
            pass only trusted code.
        func_name: The name of the function in `python_func`.

    Returns:
        GeoTIFF bytes of the single-band result; NULL for a group with no usable tile.

    SQL: ``gbx_rst_derivedband_agg(tile, python_func, func_name)``
    """
    return _api.call(
        "gbx_rst_derivedband_agg",
        tile,
        python_func,
        func_name,
    )


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
    out_srid: ColLike = None,
    no_data: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Build a terrain model from Z-valued points by Delaunay triangulation.

    Args:
        points_array: The points, each with a Z value: WKB, EWKB, WKT or EWKT, as an
            array.
        breaklines_array: Break lines, as an array; NULL or empty for none. The light
            level uses their vertices as extra points and does not enforce them as edges.
        merge_tolerance: Accepted for compatibility with the JVM level; not applied.
        snap_tolerance: Accepted for compatibility with the JVM level; not applied.
        xmin: The west edge of the output in the output CRS.
        ymin: The south edge of the output.
        xmax: The east edge of the output, above `xmin`.
        ymax: The north edge of the output, above `ymin`.
        width_px: The output width in pixels, above 0.
        height_px: The output height in pixels, above 0.
        out_srid: The EPSG or ESRI code of the output CRS. NULL leaves the output
            without a CRS, or with the geometry's own when `out_crs` is NULL too. SQL
            default: ``NULL``.
        no_data: The value of a pixel outside the points' hull. SQL default:
            ``-9999.0``.
        out_crs: The output CRS as `EPSG:<n>`, `ESRI:<n>` or WKT. When set it wins over
            `out_srid`; setting both raises. SQL default: ``NULL``.

    Returns:
        A single-band Float64 tile over the extent; each pixel holds the height
        interpolated in its triangle, and `no_data` outside the hull of the points.

    SQL: ``gbx_rst_dtmfromgeoms(points_array, breaklines_array, merge_tolerance, snap_tolerance, xmin, ymin, xmax, ymax, width_px, height_px, [out_srid], [no_data], [out_crs])``
    """
    return _api.call(
        "gbx_rst_dtmfromgeoms",
        points_array,
        breaklines_array,
        merge_tolerance,
        snap_tolerance,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        out_srid,
        no_data,
        out_crs,
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
    out_srid: ColLike = None,
    no_data: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Build a digital terrain model from a group of Z points (Delaunay TIN).

    Args:
        point: A point with a Z coordinate: WKB, EWKB, WKT or EWKT. An empty one is
            skipped.
        breaklines: Breaklines, the same for the whole group. Accepted but not enforced.
        merge_tolerance: Accepted, not applied.
        snap_tolerance: Accepted, not applied.
        xmin: The west edge of the output.
        ymin: The south edge.
        xmax: The east edge.
        ymax: The north edge.
        width_px: The output width in pixels.
        height_px: The output height in pixels.
        out_srid: The EPSG or ESRI code of the output CRS. NULL leaves the output
            without a CRS. SQL default: ``NULL``.
        no_data: The NoData value. SQL default: ``-9999.0``.
        out_crs: The output CRS as `EPSG:<n>`, `ESRI:<n>` or WKT. When set it wins over
            `out_srid`; setting both raises. SQL default: ``NULL``.

    Returns:
        GeoTIFF bytes, Float64: the points' Z interpolated over an unconstrained
        Delaunay triangulation, NoData outside their hull; NULL for a group with no
        point.

    SQL: ``gbx_rst_dtmfromgeoms_agg(point, breaklines, merge_tolerance, snap_tolerance, xmin, ymin, xmax, ymax, width_px, height_px, [out_srid], [no_data], [out_crs])``
    """
    return _api.call(
        "gbx_rst_dtmfromgeoms_agg",
        point,
        breaklines,
        merge_tolerance,
        snap_tolerance,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        out_srid,
        no_data,
        out_crs,
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


def rst_frombands(
    bands: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Stack an array of single-band tiles into one multi-band tile.

    Args:
        bands: The single-band tiles, in an array: element 0 becomes band 1, element 1
            band 2, and so on. The georeference, data type and NoData come from the first.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A tile with one band per readable input tile, in array order. A NULL or empty
        array gives NULL; an array of only error tiles gives the first one; a tile that
        cannot be read is skipped.

    SQL: ``gbx_rst_frombands(bands)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_frombands",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            bands,
        )
    return _api.call(
        "gbx_rst_frombands",
        bands,
    )


def rst_frombands_agg(
    tile: ColLike,
    band_index: ColLike,
) -> Column:
    """Stack a group's single-band tiles into one multi-band tile.

    Args:
        tile: A single-band tile. An error tile or a tile that cannot be read is
            skipped.
        band_index: The band the tile becomes, counted up from 1. A NULL index skips the
            row.

    Returns:
        GeoTIFF bytes with the tiles' first bands in ascending `band_index` order; NULL
        for a group with no usable tile.

    SQL: ``gbx_rst_frombands_agg(tile, band_index)``
    """
    return _api.call(
        "gbx_rst_frombands_agg",
        tile,
        band_index,
    )


def rst_fromcontent(
    content: ColLike,
    driver: ColLike,
) -> Column:
    """Build a tile from the bytes of a raster file.

    Args:
        content: The raster file's bytes, for example the `content` column of the
            `binaryFile` reader.
        driver: The GDAL driver short name the bytes are in, for example `'GTiff'`, in
            any case. It goes into the tile's `driver` metadata; a name that is no raster
            driver (no driver of that name, or a vector-only one such as `'GeoJSON'`)
            raises.

    Returns:
        A tile that holds the bytes, with the driver, width, height and band count in
        its metadata. Bytes GDAL cannot open give an error tile; a NULL content or
        driver gives NULL.

    SQL: ``gbx_rst_fromcontent(content, driver)``
    """
    return _api.call(
        "gbx_rst_fromcontent",
        content,
        driver,
    )


def rst_fromfile(
    path: ColLike,
    driver: ColLike = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Read the raster file at a path into a tile.

    Args:
        path: The path of the raster file: local, `/Volumes/...` or `/Workspace/...`. A
            remote URL is refused unless the driver's remote-read switch is on.
        driver: The GDAL driver short name hint, in any case. It goes into the tile's
            `driver` metadata; a name that is no raster driver (no driver of that name, or a
            vector-only one such as `'GeoJSON'`) raises. SQL default: ``'GTiff'``.
        materialize: Python only: unset or ``False`` returns a virtual tile that points at the file and reads no pixel; ``True`` reads the pixels now, as SQL does.

    Returns:
        A tile that holds the file's bytes, re-encoded as a GeoTIFF. NULL for a NULL
        path or a file that cannot be opened; an error tile with a `Fix:` for a format
        GeoBrix refuses, or a file whose decoded size is over the materialize cap.

    SQL: ``gbx_rst_fromfile(path, [driver])``
    """
    return _api.extra_call(
        "gbx_rst_fromfile",
        (materialize,),
        path,
        driver,
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


def rst_gridfrompoints(
    points_array: ColLike,
    values_array: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike = None,
    power: ColLike = None,
    max_pts: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Interpolate scattered point values to a grid by inverse-distance weighting.

    Args:
        points_array: The points: WKB, EWKB, WKT or EWKT, as an array. An empty or NULL
            element is dropped with its value.
        values_array: The value of each point, in the same order.
        xmin: The west edge of the output in the output CRS.
        ymin: The south edge of the output.
        xmax: The east edge of the output, above `xmin`.
        ymax: The north edge of the output, above `ymin`.
        width_px: The output width in pixels, above 0.
        height_px: The output height in pixels, above 0.
        out_srid: The EPSG or ESRI code of the output CRS. NULL leaves the output
            without a CRS, or with the geometry's own when `out_crs` is NULL too. SQL
            default: ``NULL``.
        power: The inverse-distance exponent, above 0. SQL default: ``2.0``.
        max_pts: The number of nearest points used per pixel, above 0. SQL default:
            ``12``.
        out_crs: The output CRS as `EPSG:<n>`, `ESRI:<n>` or WKT. When set it wins over
            `out_srid`; setting both raises. SQL default: ``NULL``.

    Returns:
        A single-band Float64 tile over the extent; each pixel holds the
        inverse-distance-weighted mean of its nearest points, NoData (-9999) where there
        are none. The points are taken to be in the output CRS.

    SQL: ``gbx_rst_gridfrompoints(points_array, values_array, xmin, ymin, xmax, ymax, width_px, height_px, [out_srid], [power], [max_pts], [out_crs])``
    """
    return _api.call(
        "gbx_rst_gridfrompoints",
        points_array,
        values_array,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        out_srid,
        power,
        max_pts,
        out_crs,
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
    out_srid: ColLike = None,
    power: ColLike = None,
    max_pts: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Interpolate a group's points to a grid by inverse-distance weighting.

    Args:
        point: The point: WKB, EWKB, WKT or EWKT. A row with a NULL value or an empty
            point is dropped.
        value: The point's value.
        xmin: The west edge of the output.
        ymin: The south edge.
        xmax: The east edge.
        ymax: The north edge.
        width_px: The output width in pixels.
        height_px: The output height in pixels.
        out_srid: The EPSG or ESRI code of the output CRS. NULL leaves the output
            without a CRS. SQL default: ``NULL``.
        power: The inverse-distance exponent, above 0. SQL default: ``2.0``.
        max_pts: The number of nearest points used per pixel, above 0. SQL default:
            ``12``.
        out_crs: The output CRS as `EPSG:<n>`, `ESRI:<n>` or WKT. When set it wins over
            `out_srid`; setting both raises. SQL default: ``NULL``.

    Returns:
        GeoTIFF bytes, Float64 with NoData -9999 where a pixel has no point; NULL for a
        group with no point.

    SQL: ``gbx_rst_gridfrompoints_agg(point, value, xmin, ymin, xmax, ymax, width_px, height_px, [out_srid], [power], [max_pts], [out_crs])``
    """
    return _api.call(
        "gbx_rst_gridfrompoints_agg",
        point,
        value,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        out_srid,
        power,
        max_pts,
        out_crs,
    )


def rst_h3_rasterize(
    cells: ColLike,
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
    """Rasterize an array of H3 cells into one tile (pixel-centroid burn).

    Args:
        cells: The group's cells as an array of structs `(cellid, value)`, for example
            `collect_list(struct(cellid, value))`. The rules of the aggregate hold: a
            repeated cell burns its largest value, a NULL cell id is skipped.
        out_srid: The EPSG code of the output CRS.
        pixel_size: The pixel size in CRS units; NULL takes it from the cells'
            resolution.
        xmin: The west edge of the canvas; NULL derives the canvas from the cells.
        ymin: The south edge of the canvas.
        xmax: The east edge of the canvas.
        ymax: The north edge of the canvas.
        width: The canvas width in pixels.
        height: The canvas height in pixels.
        mode: How the canvas is derived when no extent is given: `'centroids'` or
            `'spatial_envelope'`.
        kring_pad: Rings of padding around the derived canvas.
        out_crs: The output CRS as `EPSG:<n>`; when set it wins over `out_srid`. SQL
            default: ``NULL``.

    Returns:
        The same bytes as the aggregate `gbx_rst_h3_rasterize_agg` over the same cells:
        GeoTIFF, Float64, NoData -9999; NULL for an array with no cell.

    SQL: ``gbx_rst_h3_rasterize(cells, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad, [out_crs])``
    """
    return _api.call(
        "gbx_rst_h3_rasterize",
        cells,
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


def rst_h3_rasterize_agg(
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
    """Rasterize a group's H3 cells into one tile (pixel-centroid burn).

    Args:
        cellid: The H3 cell id.
        value: The value burned for the cell. A repeated cell burns its largest value; a
            NULL value in a typed column never wins against a number, and a cell of nothing
            but NULL burns NaN. An untyped NULL (the argument left out, or `None` in Python)
            is the presence mask and burns 1.0.
        out_srid: The EPSG code of the output CRS.
        pixel_size: The pixel size in CRS units; NULL takes it from the cells'
            resolution.
        xmin: The west edge of the canvas; NULL derives the canvas from the cells.
        ymin: The south edge of the canvas.
        xmax: The east edge of the canvas.
        ymax: The north edge of the canvas.
        width: The canvas width in pixels.
        height: The canvas height in pixels.
        mode: How the canvas is derived when no extent is given: `'centroids'` or
            `'spatial_envelope'`.
        kring_pad: Rings of padding around the derived canvas.
        out_crs: The output CRS as `EPSG:<n>`; when set it wins over `out_srid`. SQL
            default: ``NULL``.

    Returns:
        GeoTIFF bytes, Float64 with NoData -9999, over the canvas; NULL for a group with
        no cell. The same on every compute.

    SQL: ``gbx_rst_h3_rasterize_agg(cellid, value, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad, [out_crs])``
    """
    return _api.call(
        "gbx_rst_h3_rasterize_agg",
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


def rst_h3_rastertogridavg(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Mean of the pixel values in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the mean of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values are
        never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridavg(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridavg",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_rastertogridcount(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Count the pixels in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the number
        of the band's valid pixels in the cell (a DOUBLE; with `covering`, the sum of
        their area shares). A pixel is valid when the band's NoData and mask (and the
        alpha band, if the raster has one) say so. A NULL tile, a NULL resolution, an
        error tile and a tile that cannot be read give no rows. A raster in another CRS
        is reprojected to EPSG:4326 (nearest neighbour, so values are never
        interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridcount(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridcount",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_rastertogridmax(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Maximum of the pixel values in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the largest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution, an error tile and a tile that cannot be read give no rows. A
        raster in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values
        are never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridmax(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridmax",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_rastertogridmedian(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Median of the pixel values in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the median
        of the band's valid pixels in the cell. A pixel is valid when the band's NoData
        and mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values are
        never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridmedian(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridmedian",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_rastertogridmin(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Minimum of the pixel values in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the smallest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution, an error tile and a tile that cannot be read give no rows. A
        raster in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values
        are never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridmin(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridmin",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_rastertogridstddev(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population standard deviation of the pixel values in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population standard deviation (divisor n) of the band's valid pixels in the
        cell. A pixel is valid when the band's NoData and mask (and the alpha band, if
        the raster has one) say so. A NULL tile, a NULL resolution, an error tile and a
        tile that cannot be read give no rows. A raster in another CRS is reprojected to
        EPSG:4326 (nearest neighbour, so values are never interpolated); a raster with
        no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridstddev(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridstddev",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_rastertogridsum(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Sum of the pixel values in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the sum of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values are
        never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridsum(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridsum",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_rastertogridvariance(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population variance of the pixel values in each H3 cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The H3 resolution, a whole number from 0 to 15.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population variance (divisor n) of the band's valid pixels in the cell. A pixel
        is valid when the band's NoData and mask (and the alpha band, if the raster has
        one) say so. A NULL tile, a NULL resolution, an error tile and a tile that
        cannot be read give no rows. A raster in another CRS is reprojected to EPSG:4326
        (nearest neighbour, so values are never interpolated); a raster with no CRS is
        taken as already in EPSG:4326.

    SQL: ``gbx_rst_h3_rastertogridvariance(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_rastertogridvariance",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_h3_tessellate(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    assignment: ColLike = None,
    coverage: ColLike = None,
) -> DataFrame:
    """Cut a tile into H3 cells, one tile per cell.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.
        resolution: The H3 resolution, a whole number from 0 to 15.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps). Any other word raises.
            SQL default: ``'centroid'``.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, as all-NoData tiles; `'sparse'` leaves them out. Any other word
            raises. SQL default: ``'complete'``.

    Returns:
        One row per tile, with the columns of a tile. A NULL tile gives no rows. An
        error tile comes out as itself. A tile that cannot be read is one error tile.
        Each tile is clipped to its cell and carries the cell id in `cellid`.

    SQL: ``gbx_rst_h3_tessellate(tile, resolution, [assignment], [coverage])``
    """
    return _api.table(
        df,
        "gbx_rst_h3_tessellate",
        tile,
        resolution,
        assignment,
        coverage,
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


def rst_isoband(
    tile: ColLike,
    breaks: ColLike,
) -> Column:
    """Group the pixels of the first band into value bands and return one polygon per connected patch.

    Args:
        tile: The tile to classify.
        breaks: The N+1 boundaries of N bands, strictly ascending. Band i holds the
            pixels from `breaks[i]` up to, not including, `breaks[i+1]`.

    Returns:
        An array of structs, one per patch: `geom_wkb` (a WKB polygon in the tile's
        CRS), `band` (the zero-based band), `lower` and `upper`. NoData pixels and
        pixels outside the breaks are left out. NULL for a NULL tile or NULL breaks.

    SQL: ``gbx_rst_isoband(tile, breaks)``
    """
    return _api.call(
        "gbx_rst_isoband",
        tile,
        breaks,
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


def rst_maketiles(
    df: DataFrame,
    tile: ColLike,
    size_in_mb: ColLike,
) -> DataFrame:
    """Split a tile into tiles of about the given size in megabytes.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.
        size_in_mb: The most megabytes one tile may hold. The tile is cut into a 2^k by
            2^k grid (1, 4, 16 ... at most 256 tiles) until each part fits; 0 or less does
            not cut. A fraction is cut to a whole number.

    Returns:
        One row per tile, with the columns of a tile. A NULL tile gives no rows. An
        error tile comes out as itself. A tile that cannot be read is one error tile.

    SQL: ``gbx_rst_maketiles(tile, size_in_mb)``
    """
    return _api.table(
        df,
        "gbx_rst_maketiles",
        tile,
        size_in_mb,
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


def rst_merge(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Mosaic an array of tiles into one tile that spans their union extent.

    Args:
        tiles: The tiles to mosaic, in an array. Tiles in another CRS are reprojected
            onto the grid of a tile in the reference CRS.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        One tile over the union extent. A NULL or empty array gives NULL; an array of
        only error tiles gives the first one; a tile that cannot be read is skipped. An
        array over the memory budget gives an error tile with a `Fix:`. A member over
        the read cap names `merge_by_key(..., method='vrt')`; a mosaic or result over
        the cap names `mint_vrt`.

    SQL: ``gbx_rst_merge(tiles)``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_merge",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tiles,
        )
    return _api.call(
        "gbx_rst_merge",
        tiles,
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


def rst_merge_tvf(
    df: DataFrame,
) -> DataFrame:
    """Merge each partition's tiles into one mosaic, as a table function over `TABLE(...) PARTITION BY`.

    Args:
        df: The input DataFrame, with the columns ``key``, ``tile`` (others are left
            out); it is partitioned by ``key``.

    Returns:
        One row per partition: its `key` and the merged `raster` (GeoTIFF bytes over the
        union extent). A partition with no readable tile gives no row; one past what a
        merge can build raises with a `Fix:`. A member over the read cap names
        `merge_by_key(..., method='vrt')`; a mosaic or result over the cap names
        `mint_vrt`.

    SQL: ``gbx_rst_merge_tvf(rows)``
    """
    return _api.table_by(
        df,
        "gbx_rst_merge_tvf",
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


def rst_polygonize(
    df: DataFrame,
    tile: ColLike,
    band: ColLike = None,
    connectedness: ColLike = None,
) -> DataFrame:
    """Extract a polygon for each connected region of equal pixel values.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to polygonize.
        band: The band to read, from 1. SQL default: ``1``.
        connectedness: `4` joins pixels that share an edge; `8` also joins pixels that
            touch at a corner. SQL default: ``4``.

    Returns:
        One row per region: `geom_wkb` (a WKB polygon in the tile's CRS) and `value`.
        NoData pixels give no polygon. A NULL tile gives no rows.

    SQL: ``gbx_rst_polygonize(tile, [band], [connectedness])``
    """
    return _api.table(
        df,
        "gbx_rst_polygonize",
        tile,
        band,
        connectedness,
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


def rst_quadbin_rasterize(
    cells: ColLike,
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
    """Rasterize an array of quadbin cells into one tile (pixel-centroid burn).

    Args:
        cells: The group's cells as an array of structs `(cellid, value)`, for example
            `collect_list(struct(cellid, value))`. The rules of the aggregate hold: a
            repeated cell burns its largest value, a NULL cell id is skipped.
        out_srid: The EPSG code of the output CRS.
        pixel_size: The pixel size in CRS units; NULL takes it from the cells'
            resolution.
        xmin: The west edge of the canvas; NULL derives the canvas from the cells.
        ymin: The south edge of the canvas.
        xmax: The east edge of the canvas.
        ymax: The north edge of the canvas.
        width: The canvas width in pixels.
        height: The canvas height in pixels.
        mode: How the canvas is derived when no extent is given: `'centroids'` or
            `'spatial_envelope'`.
        kring_pad: Rings of padding around the derived canvas.
        out_crs: The output CRS as `EPSG:<n>`; when set it wins over `out_srid`. SQL
            default: ``NULL``.

    Returns:
        The same bytes as the aggregate `gbx_rst_quadbin_rasterize_agg` over the same
        cells: GeoTIFF, Float64, NoData -9999; NULL for an array with no cell.

    SQL: ``gbx_rst_quadbin_rasterize(cells, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad, [out_crs])``
    """
    return _api.call(
        "gbx_rst_quadbin_rasterize",
        cells,
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


def rst_quadbin_rasterize_agg(
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
    """Rasterize a group's quadbin cells into one tile (pixel-centroid burn).

    Args:
        cellid: The quadbin cell id.
        value: The value burned for the cell. A repeated cell burns its largest value; a
            NULL value in a typed column never wins against a number, and a cell of nothing
            but NULL burns NaN. An untyped NULL (the argument left out, or `None` in Python)
            is the presence mask and burns 1.0.
        out_srid: The EPSG code of the output CRS.
        pixel_size: The pixel size in CRS units; NULL takes it from the cells'
            resolution.
        xmin: The west edge of the canvas; NULL derives the canvas from the cells.
        ymin: The south edge of the canvas.
        xmax: The east edge of the canvas.
        ymax: The north edge of the canvas.
        width: The canvas width in pixels.
        height: The canvas height in pixels.
        mode: How the canvas is derived when no extent is given: `'centroids'` or
            `'spatial_envelope'`.
        kring_pad: Rings of padding around the derived canvas.
        out_crs: The output CRS as `EPSG:<n>`; when set it wins over `out_srid`. SQL
            default: ``NULL``.

    Returns:
        GeoTIFF bytes, Float64 with NoData -9999, over the canvas; NULL for a group with
        no cell. The same on every compute.

    SQL: ``gbx_rst_quadbin_rasterize_agg(cellid, value, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad, [out_crs])``
    """
    return _api.call(
        "gbx_rst_quadbin_rasterize_agg",
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


def rst_quadbin_rastertogridavg(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Mean of the pixel values in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the mean of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values are
        never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridavg(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridavg",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_rastertogridcount(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Count the pixels in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the number
        of the band's valid pixels in the cell (a DOUBLE; with `covering`, the sum of
        their area shares). A pixel is valid when the band's NoData and mask (and the
        alpha band, if the raster has one) say so. A NULL tile, a NULL resolution, an
        error tile and a tile that cannot be read give no rows. A raster in another CRS
        is reprojected to EPSG:4326 (nearest neighbour, so values are never
        interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridcount(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridcount",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_rastertogridmax(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Maximum of the pixel values in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the largest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution, an error tile and a tile that cannot be read give no rows. A
        raster in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values
        are never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridmax(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridmax",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_rastertogridmedian(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Median of the pixel values in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the median
        of the band's valid pixels in the cell. A pixel is valid when the band's NoData
        and mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values are
        never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridmedian(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridmedian",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_rastertogridmin(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Minimum of the pixel values in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the smallest
        value of the band's valid pixels in the cell. A pixel is valid when the band's
        NoData and mask (and the alpha band, if the raster has one) say so. A NULL tile,
        a NULL resolution, an error tile and a tile that cannot be read give no rows. A
        raster in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values
        are never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridmin(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridmin",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_rastertogridstddev(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population standard deviation of the pixel values in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population standard deviation (divisor n) of the band's valid pixels in the
        cell. A pixel is valid when the band's NoData and mask (and the alpha band, if
        the raster has one) say so. A NULL tile, a NULL resolution, an error tile and a
        tile that cannot be read give no rows. A raster in another CRS is reprojected to
        EPSG:4326 (nearest neighbour, so values are never interpolated); a raster with
        no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridstddev(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridstddev",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_rastertogridsum(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Sum of the pixel values in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the sum of
        the band's valid pixels in the cell. A pixel is valid when the band's NoData and
        mask (and the alpha band, if the raster has one) say so. A NULL tile, a NULL
        resolution, an error tile and a tile that cannot be read give no rows. A raster
        in another CRS is reprojected to EPSG:4326 (nearest neighbour, so values are
        never interpolated); a raster with no CRS is taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridsum(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridsum",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_rastertogridvariance(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    coverage: ColLike = None,
    assignment: ColLike = None,
) -> DataFrame:
    """Population variance of the pixel values in each quadbin cell, per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to aggregate.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, with a NULL measure (0 for a count); `'sparse'` gives only the
            cells that hold a valid pixel. Any other word raises. SQL default:
            ``'complete'``.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps, in proportion to the
            overlapping area). Any other word raises. SQL default: ``'centroid'``.

    Returns:
        One row per band and cell: `band` (from 1), `cellID` and `measure`, the
        population variance (divisor n) of the band's valid pixels in the cell. A pixel
        is valid when the band's NoData and mask (and the alpha band, if the raster has
        one) say so. A NULL tile, a NULL resolution, an error tile and a tile that
        cannot be read give no rows. A raster in another CRS is reprojected to EPSG:4326
        (nearest neighbour, so values are never interpolated); a raster with no CRS is
        taken as already in EPSG:4326.

    SQL: ``gbx_rst_quadbin_rastertogridvariance(tile, resolution, [coverage], [assignment])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_rastertogridvariance",
        tile,
        resolution,
        coverage,
        assignment,
    )


def rst_quadbin_tessellate(
    df: DataFrame,
    tile: ColLike,
    resolution: ColLike,
    assignment: ColLike = None,
    coverage: ColLike = None,
) -> DataFrame:
    """Cut a tile into quadbin cells, one tile per cell.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.
        resolution: The quadbin zoom level, a whole number from 0 to 20.
        assignment: How a pixel joins a cell: `'centroid'` (the cell holds the pixel's
            centre) or `'covering'` (every cell the pixel overlaps). Any other word raises.
            SQL default: ``'centroid'``.
        coverage: `'complete'` also gives the cells that overlap the raster but hold no
            valid pixel, as all-NoData tiles; `'sparse'` leaves them out. Any other word
            raises. SQL default: ``'complete'``.

    Returns:
        One row per tile, with the columns of a tile. A NULL tile gives no rows. An
        error tile comes out as itself. A tile that cannot be read is one error tile.
        Each tile is clipped to its cell and carries the cell id in `cellid`.

    SQL: ``gbx_rst_quadbin_tessellate(tile, resolution, [assignment], [coverage])``
    """
    return _api.table(
        df,
        "gbx_rst_quadbin_tessellate",
        tile,
        resolution,
        assignment,
        coverage,
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
    out_srid: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Burn a geometry into a new raster tile.

    Args:
        geom: The geometry: WKB, EWKB, WKT or EWKT. A geometry with an SRID is
            reprojected into the output CRS first.
        value: The value burned into the pixels the geometry covers.
        xmin: The west edge of the output in the output CRS.
        ymin: The south edge of the output.
        xmax: The east edge of the output, above `xmin`.
        ymax: The north edge of the output, above `ymin`.
        width_px: The output width in pixels, above 0.
        height_px: The output height in pixels, above 0.
        out_srid: The EPSG or ESRI code of the output CRS. NULL leaves the output
            without a CRS, or with the geometry's own when `out_crs` is NULL too. SQL
            default: ``NULL``.
        out_crs: The output CRS as `EPSG:<n>`, `ESRI:<n>` or WKT. When set it wins over
            `out_srid`; setting both raises. SQL default: ``NULL``.

    Returns:
        A single-band Float64 tile over the extent; the pixels whose centre the geometry
        covers hold `value`, the others NoData (-9999).

    SQL: ``gbx_rst_rasterize(geom, value, xmin, ymin, xmax, ymax, width_px, height_px, [out_srid], [out_crs])``
    """
    return _api.call(
        "gbx_rst_rasterize",
        geom,
        value,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        out_srid,
        out_crs,
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
    """Burn a group's `(geom, value)` features into one tile.

    Args:
        geom: The feature geometry as WKB. A NULL one is skipped.
        value: The value burned where the geometry covers a pixel centre. A NULL value
            skips the row.
        xmin: The west edge of the output.
        ymin: The south edge.
        xmax: The east edge.
        ymax: The north edge.
        width_px: The output width in pixels.
        height_px: The output height in pixels.
        out_srid: The EPSG code of the output CRS.

    Returns:
        GeoTIFF bytes, Float64 with NoData -9999, with each feature burned where it
        covers a pixel centre. Where features overlap, the one whose WKB bytes sort last
        wins, whatever order the rows arrive in. NULL for a group with no feature.

    SQL: ``gbx_rst_rasterize_agg(geom, value, xmin, ymin, xmax, ymax, width_px, height_px, out_srid)``
    """
    return _api.call(
        "gbx_rst_rasterize_agg",
        geom,
        value,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        out_srid,
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


def rst_separatebands(
    df: DataFrame,
    tile: ColLike,
) -> DataFrame:
    """Split a multi-band tile into single-band tiles, one row per band.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.

    Returns:
        One row per tile, with the columns of a tile. A NULL tile gives no rows. An
        error tile comes out as itself. A tile that cannot be read is one error tile.
        Each tile records its band number in its metadata.

    SQL: ``gbx_rst_separatebands(tile)``
    """
    return _api.table(
        df,
        "gbx_rst_separatebands",
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


def rst_tilexyz(
    tile: ColLike,
    z: ColLike,
    x: ColLike,
    y: ColLike,
    format: ColLike = None,
    size: ColLike = None,
    resampling: ColLike = None,
    rescale: ColLike = None,
) -> Column:
    """Render one web-mercator XYZ slippy-map tile of the raster as image bytes.

    Args:
        tile: The tile to render.
        z: The zoom level of the web tile, from 0.
        x: The tile column, from 0 to 2^z - 1.
        y: The tile row (north-down slippy-map order), from 0 to 2^z - 1.
        format: `'PNG'`, `'JPEG'` or `'WEBP'`, in any case. Any other word raises. SQL
            default: ``'PNG'``.
        size: The side of the output tile in pixels, in (0, 4096]. Any other value
            raises. SQL default: ``256``.
        resampling: The GDAL warp resampling: `near`, `bilinear`, `cubic`,
            `cubicspline`, `lanczos`, `average`, `mode`, `max`, `min`, `med`, `q1` or `q3`.
            Any other word raises. SQL default: ``'bilinear'``.
        rescale: The 8-bit contrast: `'auto'` rescales a tile that is not 8-bit by its
            band minimum and maximum and passes an 8-bit one through, `'none'` keeps the
            full range of the data type, and `'min,max'` rescales between two numbers. Any
            other text raises. SQL default: ``'auto'``.

    Returns:
        The image bytes: PNG and WebP are RGBA, JPEG is RGB. A pixel that is NoData, or
        outside the raster, is transparent; a tile outside the raster, a NULL tile and a
        tile that cannot be read give a fully transparent image of the requested size. A
        NULL z, x or y gives NULL.

    SQL: ``gbx_rst_tilexyz(tile, z, x, y, [format], [size], [resampling], [rescale])``
    """
    return _api.call(
        "gbx_rst_tilexyz",
        tile,
        z,
        x,
        y,
        format,
        size,
        resampling,
        rescale,
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


def rst_tooverlappingtiles(
    df: DataFrame,
    tile: ColLike,
    tile_width: ColLike,
    tile_height: ColLike,
    overlap: ColLike,
) -> DataFrame:
    """Partition a tile into overlapping tiles of the given pixel size.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to split.
        tile_width: The width of each tile in pixels, above 0.
        tile_height: The height of each tile in pixels, above 0.
        overlap: The overlap as a percentage of the tile size, from 0 to below 100. Each
            edge overlaps by the tile size times this, rounded up.

    Returns:
        One row per tile, with the columns of a tile. A NULL tile gives no rows. An
        error tile comes out as itself. A tile that cannot be read is one error tile.
        The tiles at the right and bottom edges are smaller. Every window is a row, also
        one that holds only NoData.

    SQL: ``gbx_rst_tooverlappingtiles(tile, tile_width, tile_height, overlap)``
    """
    return _api.table(
        df,
        "gbx_rst_tooverlappingtiles",
        tile,
        tile_width,
        tile_height,
        overlap,
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


def rst_viewshed(
    tile: ColLike,
    observer_geom: ColLike,
    observer_height: ColLike,
    target_height: ColLike = None,
    max_distance: ColLike = None,
    crs: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute a binary viewshed from a DEM tile and an observer point.

    Args:
        tile: The DEM tile.
        observer_geom: The observer: a POINT as WKB, EWKB, WKT or EWKT, in the DEM's CRS
            (an SRID in EWKB or EWKT wins over `crs`). Any other geometry raises.
        observer_height: The observer's height above the DEM, 0 or more.
        target_height: The height of the target above the DEM at each cell, 0 or more.
            SQL default: ``1.6``.
        max_distance: The largest distance to analyse in the DEM's CRS units, above 0.
            NULL for no limit: the whole DEM. A limit gives the window around the observer.
            SQL default: ``NULL``.
        crs: The CRS of the observer when it has no SRID (`EPSG:n`, `ESRI:n` or WKT).
            NULL means the DEM's CRS. SQL default: ``NULL``.
        virtualize_dir: Python only: write the result under this directory and return a virtual tile.
        virtualize_prefix: Python only: the file-name prefix used with ``virtualize_dir``.
        materialize: Python only: ``True`` returns the raster bytes in the tile.

    Returns:
        A Byte tile: 255 where the cell is visible, 0 where it is not. A NULL tile or
        observer gives NULL; an error tile comes out as itself; a tile that cannot be
        read gives an error tile.

    SQL: ``gbx_rst_viewshed(tile, observer_geom, observer_height, [target_height], [max_distance], [crs])``
    """
    if _api.forced(virtualize_dir, virtualize_prefix, materialize):
        return _api.force_output(
            "gbx_rst_viewshed",
            virtualize_dir,
            virtualize_prefix,
            materialize,
            tile,
            observer_geom,
            observer_height,
            target_height,
            max_distance,
            crs,
        )
    return _api.call(
        "gbx_rst_viewshed",
        tile,
        observer_geom,
        observer_height,
        target_height,
        max_distance,
        crs,
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


def rst_xyzpyramid(
    df: DataFrame,
    tile: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    format: ColLike = None,
    size: ColLike = None,
    resampling: ColLike = None,
    rescale: ColLike = None,
) -> DataFrame:
    """Render every web-mercator XYZ tile that the raster touches, across a range of zoom levels.

    Args:
        df: The input DataFrame; each row is joined with the rows generated from it.
        tile: The tile to render.
        min_z: The first zoom level, from 0.
        max_z: The last zoom level, at least `min_z` and at most 20.
        format: `'PNG'`, `'JPEG'` or `'WEBP'`, in any case. Any other word raises. SQL
            default: ``'PNG'``.
        size: The side of each output tile in pixels, in (0, 4096]. Any other value
            raises. SQL default: ``256``.
        resampling: The GDAL warp resampling, as for `gbx_rst_tilexyz`. Any other word
            raises. SQL default: ``'bilinear'``.
        rescale: The 8-bit contrast, as for `gbx_rst_tilexyz`. SQL default: ``'auto'``.

    Returns:
        One row per tile: `z`, `x`, `y` and the image `bytes`. A NULL tile, an error
        tile and a tile that cannot be read give no rows. A pyramid of more than
        1,000,000 candidate tiles raises.

    SQL: ``gbx_rst_xyzpyramid(tile, min_z, max_z, [format], [size], [resampling], [rescale])``
    """
    return _api.table(
        df,
        "gbx_rst_xyzpyramid",
        tile,
        min_z,
        max_z,
        format,
        size,
        resampling,
        rescale,
    )


# END GENERATED
