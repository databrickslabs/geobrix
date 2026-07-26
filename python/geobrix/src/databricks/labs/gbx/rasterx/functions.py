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

from typing import Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f

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
    # with it when pyrx ([light]) is importable. If it is NOT present, skip gracefully and leave the
    # Scala gbx_rst_fromfile in place (it still reads local / DBFS / Workspace, just not /Volumes).
    try:
        from databricks.labs.gbx.pyrx.functions import (
            _fromfile_udf as _pyrx_fromfile_udf,
        )

        _spark.udf.register("gbx_rst_fromfile", _pyrx_fromfile_udf)
    except (
        Exception
    ):  # noqa: BLE001 - pyrx/[light] not installed: keep the Scala fallback
        pass


def rst_avg(tile: ColLike) -> Column:
    """Return the average pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_avg", _col(tile))


def rst_bandmetadata(tile: ColLike, band: ColLike) -> Column:
    """Return metadata for the given band index (e.g. nodata, data type).

    Args:
        tile: Raster tile column.
        band: 1-based band index column.

    Returns:
        Column of map (string -> string).
    """
    return f.call_function("gbx_rst_bandmetadata", _col(tile), _col(band))


def rst_boundingbox(tile: ColLike) -> Column:
    """Return the bounding box of the raster in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of WKB (binary).
    """
    return f.call_function("gbx_rst_boundingbox", _col(tile))


def rst_format(tile: ColLike) -> Column:
    """Return the GDAL format/driver name of the raster (e.g. GTiff).

    Args:
        tile: Raster tile column.

    Returns:
        Column of format string.
    """
    return f.call_function("gbx_rst_format", _col(tile))


def rst_georeference(tile: ColLike) -> Column:
    """Return the georeference (affine transform) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> double).
    """
    return f.call_function("gbx_rst_georeference", _col(tile))


def rst_getnodata(tile: ColLike) -> Column:
    """Return the NoData value for the raster (or null if not set).

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band), or null.
    """
    return f.call_function("gbx_rst_getnodata", _col(tile))


def rst_getsubdataset(tile: ColLike, subset_name: ColLike) -> Column:
    """Return a sub-dataset (e.g. HDF sublayer) by name.

    Args:
        tile: Raster tile column.
        subset_name: Name of the sub-dataset.

    Returns:
        Column of raster tile (sub-dataset).
    """
    return f.call_function("gbx_rst_getsubdataset", _col(tile), _col(subset_name))


def rst_height(tile: ColLike) -> Column:
    """Return the pixel height (number of rows) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer height.
    """
    return f.call_function("gbx_rst_height", _col(tile))


def rst_max(tile: ColLike) -> Column:
    """Return the maximum pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_max", _col(tile))


def rst_median(tile: ColLike) -> Column:
    """Return the median pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_median", _col(tile))


def rst_memsize(tile: ColLike) -> Column:
    """Return the approximate memory size of the tile in bytes.

    Args:
        tile: Raster tile column.

    Returns:
        Column of long (bytes).
    """
    return f.call_function("gbx_rst_memsize", _col(tile))


def rst_metadata(tile: ColLike) -> Column:
    """Return full metadata of the raster (driver, dimensions, CRS, etc.).

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> string).
    """
    return f.call_function("gbx_rst_metadata", _col(tile))


def rst_min(tile: ColLike) -> Column:
    """Return the minimum pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_min", _col(tile))


def rst_numbands(tile: ColLike) -> Column:
    """Return the number of bands in the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer band count.
    """
    return f.call_function("gbx_rst_numbands", _col(tile))


def rst_pixelcount(tile: ColLike) -> Column:
    """Return the valid pixel count per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of long (one per band).
    """
    return f.call_function("gbx_rst_pixelcount", _col(tile))


def rst_pixelheight(tile: ColLike) -> Column:
    """Return the pixel height (ground size in Y) in CRS units.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double (may be negative).
    """
    return f.call_function("gbx_rst_pixelheight", _col(tile))


def rst_pixelwidth(tile: ColLike) -> Column:
    """Return the pixel width (ground size in X) in CRS units.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_pixelwidth", _col(tile))


def rst_rotation(tile: ColLike) -> Column:
    """Return the rotation component of the georeference (if any).

    Args:
        tile: Raster tile column.

    Returns:
        Column of rotation (double).
    """
    return f.call_function("gbx_rst_rotation", _col(tile))


def rst_scalex(tile: ColLike) -> Column:
    """Return the X scale (pixel size in X) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_scalex", _col(tile))


def rst_scaley(tile: ColLike) -> Column:
    """Return the Y scale (pixel size in Y) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double (often negative).
    """
    return f.call_function("gbx_rst_scaley", _col(tile))


def rst_skewx(tile: ColLike) -> Column:
    """Return the X skew component of the georeference.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_skewx", _col(tile))


def rst_skewy(tile: ColLike) -> Column:
    """Return the Y skew component of the georeference.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_skewy", _col(tile))


def rst_srid(tile: ColLike) -> Column:
    """Return the spatial reference ID (EPSG code) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer SRID.
    """
    return f.call_function("gbx_rst_srid", _col(tile))


def rst_subdatasets(tile: ColLike) -> Column:
    """Return the sub-dataset names and descriptions (e.g. for HDF/NetCDF).

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> string, name to description).
    """
    return f.call_function("gbx_rst_subdatasets", _col(tile))


def rst_summary(tile: ColLike) -> Column:
    """Return a short text summary of the raster (dimensions, CRS, bands).

    Args:
        tile: Raster tile column.

    Returns:
        Column of string summary.
    """
    return f.call_function("gbx_rst_summary", _col(tile))


def rst_type(tile: ColLike) -> Column:
    """Return the raster data type (e.g. Byte, Int16, Float32) per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of strings (one per band).
    """
    return f.call_function("gbx_rst_type", _col(tile))


def rst_upperleftx(tile: ColLike) -> Column:
    """Return the X coordinate of the upper-left corner in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_upperleftx", _col(tile))


def rst_upperlefty(tile: ColLike) -> Column:
    """Return the Y coordinate of the upper-left corner in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_upperlefty", _col(tile))


def rst_width(tile: ColLike) -> Column:
    """Return the pixel width (number of columns) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer width.
    """
    return f.call_function("gbx_rst_width", _col(tile))


# Aggregators


def rst_combineavg_agg(tile: ColLike) -> Column:
    """Aggregate multiple raster tiles by averaging (use with groupBy).

    Args:
        tile: Raster tile column.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg_agg", _col(tile))


def rst_derivedband_agg(tile: ColLike, pyfunc: ColLike, func_name: ColLike) -> Column:
    """Aggregate tiles and apply a Python UDF per band (use with groupBy).

    Args:
        tile: Raster tile column.
        pyfunc: Python source code of the UDF (string).
        func_name: Name of the callable in pyfunc.

    Returns:
        Column of derived raster tile.
    """
    return f.call_function(
        "gbx_rst_derivedband_agg", _col(tile), _col(pyfunc), _col(func_name)
    )


def rst_merge_agg(tile: ColLike) -> Column:
    """Aggregate multiple raster tiles by merging (use with groupBy).

    Args:
        tile: Raster tile column.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge_agg", _col(tile))


def rst_rasterize_agg(
    geom_wkb: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
) -> Column:
    """Rasterize streaming (geom_wkb, value) rows into a single raster tile (use with groupBy).

    Streams one geometry/value pair per row; the extent and pixel-size arguments
    are per-group constants.  Overlap is last-wins (nondeterministic across the group).

    Args:
        geom_wkb: BINARY column of geometry WKB (Polygon, MultiPolygon, etc.).
        value: DOUBLE burn value column.
        xmin: Minimum X of the output raster extent.
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        srid: EPSG SRID of the geometry / output raster.

    Returns:
        Column of raster tile.
    """
    return f.call_function(
        "gbx_rst_rasterize_agg",
        _col(geom_wkb),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
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
    value: ColLike,
    srid: ColLike,
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
    """Rasterize a group's H3 cells into one tile (pixel-centroid burn).

    Use with ``groupBy``; each row contributes one H3 ``cellid`` (and optionally
    a ``value``). When ``value`` is ``None`` / ``f.lit(None)``, pixels covered by
    any cell are burned with ``1.0`` (presence mask).  Supply an explicit canvas
    (``xmin`` … ``height``) from ``rst_h3_gridspec`` for aligned multi-band
    stacking; otherwise the grid is auto-derived from the cell set.

    Args:
        cellid:     LONG column of H3 cell ids.
        value:      DOUBLE burn-value column, or ``f.lit(None).cast("double")``
                    for a presence mask.
        srid:       EPSG SRID of the output raster (e.g. ``f.lit(4326)``).
        pixel_size: Pixel size in CRS units (used when extent is auto-derived).
        xmin:       Minimum X of the output canvas (CRS units).
        ymin:       Minimum Y of the output canvas (CRS units).
        xmax:       Maximum X of the output canvas (CRS units).
        ymax:       Maximum Y of the output canvas (CRS units).
        width:      Canvas width in pixels (INTEGER).
        height:     Canvas height in pixels (INTEGER).
        mode:       Sampling mode string (e.g. ``f.lit("centroids")``).
        kring_pad:  K-ring expansion around each cell before rasterizing
                    (``f.lit(0)`` = no expansion).

    Returns:
        Column of raster tile (tile struct with ``source``, ``raster``,
        ``metadata`` fields).
    """
    return f.call_function(
        "gbx_rst_h3_rasterize_agg",
        _col(cellid),
        _col(value),
        _col(srid),
        _col(pixel_size),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width),
        _col(height),
        _col(mode),
        _col(kring_pad),
    )


def gbx_h3_cell_bbox(
    cellid: ColLike,
    srid: ColLike = None,
    mode: ColLike = None,
    kring_pad: ColLike = 0,
) -> Column:
    """Bounding box of one H3 cell in *srid* (centroid point or hexagon envelope).

    Returns a ``STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE>``.

    Args:
        cellid:    Column holding the H3 cell id (integer / long).
        srid:      Output CRS EPSG code.  Defaults to ``4326``.
        mode:      ``"centroids"`` (default) or ``"spatial_envelope"``.
        kring_pad: K-ring expansion before computing the bbox (default ``0``).
                   When > 0 the returned bbox covers the full padded neighbourhood.

    Returns:
        Column of STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE>.
    """
    srid_col = _col(srid) if srid is not None else f.lit(4326)
    mode_col = _col(mode) if mode is not None else f.lit("centroids")
    return f.call_function(
        "gbx_h3_cell_bbox",
        _col(cellid),
        srid_col,
        mode_col,
        _col(kring_pad),
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
        path: Column of file path (string).
        driver: GDAL driver name (e.g. GTiff).

    Returns:
        Column of raster tile.

    Note:
        ``gbx_rst_fromfile`` is **lightweight-only**: it is implemented by the ``pyrx`` Python loader
        (a ``pandas_udf``) and has no JVM/Scala implementation. On Databricks the executor JVM cannot
        read a UC Volume (``/Volumes/...``) FUSE path — the UC credential is held only by Spark's
        managed Python worker — so this delegates to ``pyrx.rst_fromfile`` and **requires
        ``geobrix[light]``**. If ``[light]`` is not installed, this raises with guidance. The
        portable alternative (no ``[light]``, works on every tier) is
        ``spark.read.format("binaryFile").load(path)`` + ``gbx_rst_fromcontent(content, driver)``.
    """
    try:
        from databricks.labs.gbx.pyrx import functions as _pyrx
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "gbx_rst_fromfile is lightweight-only and requires geobrix[light] (it is implemented by "
            "the pyrx Python reader; the JVM cannot read UC Volumes). Install geobrix[light], or use "
            "spark.read.format('binaryFile').load(path) + gbx_rst_fromcontent(content, driver)."
        ) from e
    return _pyrx.rst_fromfile(path, driver)


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
    tile: ColLike, resolution: ColLike, mode: ColLike = "covering"
) -> Column:
    """Tessellate the raster into H3 cells at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).
        mode: ``"covering"`` (default) keeps every cell whose hexagon overlaps
            the raster bbox (chips may share pixels); ``"centroid"`` single-assigns
            each valid pixel to the one cell whose hexagon contains its centroid
            (chips partition the valid pixels). String literals are auto-wrapped
            in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Column of array of (H3 index, tile) or similar.
    """
    mode_col = f.lit(mode) if isinstance(mode, str) else _col(mode)
    return f.call_function(
        "gbx_rst_h3_tessellate", _col(tile), _col(resolution), mode_col
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


def rst_retile(tile: ColLike, tile_width: ColLike, tile_height: ColLike) -> Column:
    """Retile the raster into tiles of the given pixel dimensions.

    Args:
        tile: Raster tile column.
        tile_width: Width of output tiles (pixels).
        tile_height: Height of output tiles (pixels).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function(
        "gbx_rst_retile", _col(tile), _col(tile_width), _col(tile_height)
    )


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


def rst_h3_rastertogridavg(tile: ColLike, resolution: ColLike) -> Column:
    """Compute average pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and avg).
    """
    return f.call_function("gbx_rst_h3_rastertogridavg", _col(tile), _col(resolution))


def rst_h3_rastertogridcount(tile: ColLike, resolution: ColLike) -> Column:
    """Compute pixel count per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and count).
    """
    return f.call_function("gbx_rst_h3_rastertogridcount", _col(tile), _col(resolution))


def rst_h3_rastertogridmax(tile: ColLike, resolution: ColLike) -> Column:
    """Compute maximum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and max).
    """
    return f.call_function("gbx_rst_h3_rastertogridmax", _col(tile), _col(resolution))


def rst_h3_rastertogridmin(tile: ColLike, resolution: ColLike) -> Column:
    """Compute minimum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and min).
    """
    return f.call_function("gbx_rst_h3_rastertogridmin", _col(tile), _col(resolution))


def rst_h3_rastertogridmedian(tile: ColLike, resolution: ColLike) -> Column:
    """Compute median pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and median).
    """
    return f.call_function(
        "gbx_rst_h3_rastertogridmedian", _col(tile), _col(resolution)
    )


def rst_h3_rastertogridsum(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the sum of pixel values per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and sum).
    """
    return f.call_function("gbx_rst_h3_rastertogridsum", _col(tile), _col(resolution))


def rst_h3_rastertogridvariance(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the population variance of pixel values per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and variance).
    """
    return f.call_function(
        "gbx_rst_h3_rastertogridvariance", _col(tile), _col(resolution)
    )


def rst_h3_rastertogridstddev(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the population standard deviation of pixel values per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and stddev).
    """
    return f.call_function(
        "gbx_rst_h3_rastertogridstddev", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridavg(tile: ColLike, resolution: ColLike) -> Column:
    """Compute average pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridavg", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridcount(tile: ColLike, resolution: ColLike) -> Column:
    """Compute pixel count per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure BIGINT)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridcount", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridmax(tile: ColLike, resolution: ColLike) -> Column:
    """Compute maximum pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridmax", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridmin(tile: ColLike, resolution: ColLike) -> Column:
    """Compute minimum pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridmin", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridmedian(tile: ColLike, resolution: ColLike) -> Column:
    """Compute median pixel value per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridmedian", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridsum(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the sum of pixel values per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridsum", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridvariance(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the population variance of pixel values per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridvariance", _col(tile), _col(resolution)
    )


def rst_quadbin_rastertogridstddev(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the population standard deviation of pixel values per CARTO quadbin v0 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).

    Returns:
        Column ARRAY<ARRAY<struct(cellID BIGINT, measure DOUBLE)>>.
    """
    return f.call_function(
        "gbx_rst_quadbin_rastertogridstddev", _col(tile), _col(resolution)
    )


def rst_bng_rastertogridavg(tile: ColLike, resolution: ColLike) -> Column:
    """Compute average pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function("gbx_rst_bng_rastertogridavg", _col(tile), _col(resolution))


def rst_bng_rastertogridcount(tile: ColLike, resolution: ColLike) -> Column:
    """Compute pixel count per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure BIGINT)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function(
        "gbx_rst_bng_rastertogridcount", _col(tile), _col(resolution)
    )


def rst_bng_rastertogridmax(tile: ColLike, resolution: ColLike) -> Column:
    """Compute maximum pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function("gbx_rst_bng_rastertogridmax", _col(tile), _col(resolution))


def rst_bng_rastertogridmin(tile: ColLike, resolution: ColLike) -> Column:
    """Compute minimum pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function("gbx_rst_bng_rastertogridmin", _col(tile), _col(resolution))


def rst_bng_rastertogridmedian(tile: ColLike, resolution: ColLike) -> Column:
    """Compute median pixel value per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function(
        "gbx_rst_bng_rastertogridmedian", _col(tile), _col(resolution)
    )


def rst_bng_rastertogridsum(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the sum of pixel values per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function("gbx_rst_bng_rastertogridsum", _col(tile), _col(resolution))


def rst_bng_rastertogridvariance(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the population variance of pixel values per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function(
        "gbx_rst_bng_rastertogridvariance", _col(tile), _col(resolution)
    )


def rst_bng_rastertogridstddev(tile: ColLike, resolution: ColLike) -> Column:
    """Compute the population standard deviation of pixel values per BNG grid cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.

    Returns:
        Column ARRAY<ARRAY<struct(cellID STRING, measure DOUBLE)>>.
        Output cell ids are BNG grid-square strings (e.g. ``"TQ3080"``).
    """
    return f.call_function(
        "gbx_rst_bng_rastertogridstddev", _col(tile), _col(resolution)
    )


def rst_quadbin_tessellate(
    tile: ColLike, resolution: ColLike, mode: ColLike = "covering"
) -> Column:
    """Tessellate the raster into CARTO quadbin v0 cells at the given zoom level.

    Args:
        tile: Raster tile column.
        resolution: Quadbin resolution / zoom (0–20).
        mode: ``"covering"`` (default) keeps every cell whose tile overlaps
            the raster bbox (chips may share pixels); ``"centroid"`` single-assigns
            each valid pixel to the one cell containing its centroid (chips
            partition the valid pixels). String literals are auto-wrapped
            in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Column of array of (quadbin cell id BIGINT, tile) pairs.
    """
    mode_col = f.lit(mode) if isinstance(mode, str) else _col(mode)
    return f.call_function(
        "gbx_rst_quadbin_tessellate", _col(tile), _col(resolution), mode_col
    )


def rst_bng_tessellate(
    tile: ColLike, resolution: ColLike, mode: ColLike = "covering"
) -> Column:
    """Tessellate the raster into BNG grid cells at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution — integer index ±1..±6 (1=100km … 6=1m;
            negative indices select quadrant subdivisions) or a resolution
            string such as ``"1km"`` or ``"100m"``.
        mode: ``"covering"`` (default) keeps every cell whose grid square overlaps
            the raster bbox; ``"centroid"`` single-assigns each valid pixel to
            the one cell containing its centroid. String literals are auto-wrapped
            in ``f.lit``; pass a ``Column`` to defer.

    Returns:
        Column of array of (BNG cell id STRING, tile) pairs.
    """
    mode_col = f.lit(mode) if isinstance(mode, str) else _col(mode)
    return f.call_function(
        "gbx_rst_bng_tessellate", _col(tile), _col(resolution), mode_col
    )


def rst_quadbin_rasterize_agg(
    cellid: ColLike,
    value: ColLike,
    srid: ColLike,
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
    """Rasterize a group's CARTO quadbin v0 cells into one tile (pixel-centroid burn).

    Use with ``groupBy``; each row contributes one quadbin ``cellid`` (BIGINT) and
    optionally a ``value``. When ``value`` is ``None`` / ``f.lit(None)``, pixels
    covered by any cell are burned with ``1.0`` (presence mask). Supply an explicit
    canvas (``xmin`` … ``height``) for aligned multi-band stacking; otherwise the
    grid is auto-derived from the cell set.

    Args:
        cellid:     BIGINT column of quadbin cell ids.
        value:      DOUBLE burn-value column, or ``f.lit(None).cast("double")``
                    for a presence mask.
        srid:       EPSG SRID of the output raster (e.g. ``f.lit(4326)``).
        pixel_size: Pixel size in CRS units (used when extent is auto-derived).
        xmin:       Minimum X of the output canvas (CRS units).
        ymin:       Minimum Y of the output canvas (CRS units).
        xmax:       Maximum X of the output canvas (CRS units).
        ymax:       Maximum Y of the output canvas (CRS units).
        width:      Canvas width in pixels (INTEGER).
        height:     Canvas height in pixels (INTEGER).
        mode:       Sampling mode string (e.g. ``f.lit("centroids")``).
        kring_pad:  K-ring expansion around each cell before rasterizing
                    (``f.lit(0)`` = no expansion).

    Returns:
        Column of raster tile (tile struct with ``source``, ``raster``,
        ``metadata`` fields).
    """
    return f.call_function(
        "gbx_rst_quadbin_rasterize_agg",
        _col(cellid),
        _col(value),
        _col(srid),
        _col(pixel_size),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width),
        _col(height),
        _col(mode),
        _col(kring_pad),
    )


def rst_bng_rasterize_agg(
    cellid: ColLike,
    value: ColLike,
    srid: ColLike,
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
    """Rasterize a group's BNG grid cells into one tile (pixel-centroid burn).

    Use with ``groupBy``; each row contributes one BNG ``cellid`` (STRING, e.g.
    ``"TQ3080"``) and optionally a ``value``. When ``value`` is ``None`` /
    ``f.lit(None)``, pixels covered by any cell are burned with ``1.0``
    (presence mask). Supply an explicit canvas (``xmin`` … ``height``) for
    aligned multi-band stacking; otherwise the grid is auto-derived from the
    cell set.

    Note on ``srid``: BNG always operates in EPSG:27700 (British National Grid).
    The ``srid`` argument is accepted for API consistency with the H3 and
    quadbin variants but is a no-op — the function forces EPSG:27700 regardless
    of the value passed.

    Args:
        cellid:     STRING column of BNG cell ids (e.g. ``"TQ3080"``).
        value:      DOUBLE burn-value column, or ``f.lit(None).cast("double")``
                    for a presence mask.
        srid:       Ignored — BNG forces EPSG:27700. Pass ``f.lit(27700)`` for
                    clarity or any integer; the value has no effect.
        pixel_size: Pixel size in CRS units (used when extent is auto-derived).
        xmin:       Minimum X of the output canvas (BNG eastings).
        ymin:       Minimum Y of the output canvas (BNG northings).
        xmax:       Maximum X of the output canvas (BNG eastings).
        ymax:       Maximum Y of the output canvas (BNG northings).
        width:      Canvas width in pixels (INTEGER).
        height:     Canvas height in pixels (INTEGER).
        mode:       Sampling mode string (e.g. ``f.lit("centroids")``).
        kring_pad:  K-ring expansion around each cell before rasterizing
                    (``f.lit(0)`` = no expansion).

    Returns:
        Column of raster tile (tile struct with ``source``, ``raster``,
        ``metadata`` fields).
    """
    return f.call_function(
        "gbx_rst_bng_rasterize_agg",
        _col(cellid),
        _col(value),
        _col(srid),
        _col(pixel_size),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width),
        _col(height),
        _col(mode),
        _col(kring_pad),
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


def rst_clip(tile: ColLike, clip: ColLike, cutline_all_touched: ColLike) -> Column:
    """Clip the raster to a geometry (or mask).

    Args:
        tile: Raster tile column.
        clip: Clipping geometry column (WKT/WKB) or raster mask.
        cutline_all_touched: If True, include pixels touched by the boundary.

    Returns:
        Column of clipped raster tile.
    """
    return f.call_function(
        "gbx_rst_clip", _col(tile), _col(clip), _col(cutline_all_touched)
    )


def rst_combineavg(tiles: ColLike) -> Column:
    """Combine multiple raster tiles by averaging (same extent/cellsize).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg", _col(tiles))


def rst_convolve(tile: ColLike, kernel: ColLike) -> Column:
    """Apply a convolution kernel to the raster.

    Args:
        tile: Raster tile column.
        kernel: Kernel matrix (e.g. 3x3) as column.

    Returns:
        Column of convolved raster tile.
    """
    return f.call_function("gbx_rst_convolve", _col(tile), _col(kernel))


def rst_derivedband(tile_expr: ColLike, pyfunc: ColLike, func_name: ColLike) -> Column:
    """Apply a Python UDF to each pixel (or band) to produce a derived band.

    Args:
        tile_expr: Raster tile column (or expression).
        pyfunc: Python source code of the UDF (string).
        func_name: Name of the callable in pyfunc.

    Returns:
        Column of raster tile with derived band(s).
    """
    return f.call_function(
        "gbx_rst_derivedband", _col(tile_expr), _col(pyfunc), _col(func_name)
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


def rst_isempty(tile: ColLike) -> Column:
    """Return true if the raster tile is empty or invalid.

    Args:
        tile: Raster tile column.

    Returns:
        Column of boolean.
    """
    return f.call_function("gbx_rst_isempty", _col(tile))


def rst_mapalgebra(tiles: ColLike, expression: ColLike) -> Column:
    """Apply a map algebra expression to one or more tiles.

    Args:
        tiles: Column of array of raster tiles (or single tile).
        expression: Expression string (e.g. A + B, A * 2).

    Returns:
        Column of result raster tile.
    """
    return f.call_function("gbx_rst_mapalgebra", _col(tiles), _col(expression))


def rst_merge(tiles: ColLike) -> Column:
    """Merge multiple raster tiles into one (e.g. mosaic).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge", _col(tiles))


def rst_ndvi(tile: ColLike, red_band: ColLike, nir_band: ColLike) -> Column:
    """Compute NDVI from red and NIR band indices.

    Args:
        tile: Raster tile column.
        red_band: 1-based red band index.
        nir_band: 1-based NIR band index.

    Returns:
        Column of raster tile (single-band NDVI).
    """
    return f.call_function("gbx_rst_ndvi", _col(tile), _col(red_band), _col(nir_band))


def rst_rastertoworldcoord(tile: ColLike, pixel_x: ColLike, pixel_y: ColLike) -> Column:
    """Convert pixel (x, y) to world (x, y) in the CRS of the raster.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of struct (x, y as double) in world coordinates.
    """
    return f.call_function(
        "gbx_rst_rastertoworldcoord", _col(tile), _col(pixel_x), _col(pixel_y)
    )


def rst_rastertoworldcoordx(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    """Convert pixel (x, y) to world X coordinate.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of double.
    """
    return f.call_function(
        "gbx_rst_rastertoworldcoordx", _col(tile), _col(pixel_x), _col(pixel_y)
    )


def rst_rastertoworldcoordy(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    """Convert pixel (x, y) to world Y coordinate.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of double.
    """
    return f.call_function(
        "gbx_rst_rastertoworldcoordy", _col(tile), _col(pixel_x), _col(pixel_y)
    )


def rst_transform(tile: ColLike, target_srid: ColLike) -> Column:
    """Reproject the raster to the target SRID (EPSG code).

    Args:
        tile: Raster tile column.
        target_srid: Target spatial reference ID (e.g. 4326 for WGS84).

    Returns:
        Column of reprojected raster tile.
    """
    return f.call_function("gbx_rst_transform", _col(tile), _col(target_srid))


def rst_tryopen(tile: ColLike) -> Column:
    """Attempt to open/validate the raster; return true if successful.

    Args:
        tile: Raster tile column.

    Returns:
        Column of boolean.
    """
    return f.call_function("gbx_rst_tryopen", _col(tile))


def rst_updatetype(tile: ColLike, new_type: ColLike) -> Column:
    """Update the declared data type of the raster (e.g. after conversion).

    Args:
        tile: Raster tile column.
        new_type: New GDAL data type name (e.g. Byte, Float32).

    Returns:
        Column of raster tile with updated type metadata.
    """
    return f.call_function("gbx_rst_updatetype", _col(tile), _col(new_type))


def rst_worldtorastercoord(tile: ColLike, world_x: ColLike, world_y: ColLike) -> Column:
    """Convert world (x, y) to pixel (x, y) in the raster.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of struct (x, y as integer) in pixel coordinates.
    """
    return f.call_function(
        "gbx_rst_worldtorastercoord", _col(tile), _col(world_x), _col(world_y)
    )


def rst_worldtorastercoordx(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    """Convert world (x, y) to pixel column index.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of integer (pixel column index).
    """
    return f.call_function(
        "gbx_rst_worldtorastercoordx", _col(tile), _col(world_x), _col(world_y)
    )


def rst_worldtorastercoordy(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    """Convert world (x, y) to pixel row index.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of integer (pixel row index).
    """
    return f.call_function(
        "gbx_rst_worldtorastercoordy", _col(tile), _col(world_x), _col(world_y)
    )


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
    geom_wkb: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
) -> Column:
    """Burn a vector geometry into a raster tile at the given extent and resolution.

    Returns a GTiff-backed tile of shape ``width_px x height_px`` covering the
    bounding box ``(xmin, ymin) -> (xmax, ymax)`` in the given SRID. Pixels
    inside the geometry receive ``value``; pixels outside receive NoData
    (-9999.0, Float64).

    Args:
        geom_wkb: Geometry as WKB ``bytes`` column.
        value: Burn value (``float``).
        xmin: Minimum X of the output raster extent.
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        srid: EPSG SRID of the extent / geometry.

    Returns:
        Raster tile column.
    """
    return f.call_function(
        "gbx_rst_rasterize",
        _col(geom_wkb),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
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
# Terrain analysis (DEM processing) - Wave 8a
#
# Seven thin wrappers around gdal.DEMProcessing. All take a single source tile
# and return a derived tile. Defaults match the GDAL conventions.
# ---------------------------------------------------------------------------


def rst_slope(
    tile: ColLike,
    unit: ColLike = None,
    scale: ColLike = None,
) -> Column:
    """Compute slope from a DEM tile via ``gdal.DEMProcessing("slope")``.

    Args:
        tile: Single-band DEM tile column.
        unit: ``"degrees"`` (default) or ``"percent"``.
        scale: Horizontal scale (ratio of vertical units to horizontal units).
            By default the scale is auto-derived from the raster CRS (GDAL 3.11
            behavior), so projected CRS in metres and geographic CRS in degrees
            both work without an explicit value. Pass an explicit ``scale``
            (e.g. 111120 for degree grids) to override.

    Returns:
        Single-band Float32 GTiff tile column.
    """
    unit_col = (
        f.lit("degrees")
        if unit is None
        else (f.lit(unit) if isinstance(unit, str) else _col(unit))
    )
    scale_col = f.lit(float("nan")) if scale is None else _col(scale)
    return f.call_function("gbx_rst_slope", _col(tile), unit_col, scale_col)


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
# Spectral indices (Wave 8b)
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
    points: ColLike,
    values: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    power: ColLike = None,
    max_pts: ColLike = None,
) -> Column:
    """Inverse-Distance-Weighted (IDW) interpolation - non-aggregator form.

    Points (``ARRAY<BINARY>`` WKB or ``ARRAY<STRING>`` WKT) and ``values``
    (``ARRAY<DOUBLE>``) are passed in a single row. The output is a Float64
    GTiff tile of shape ``width_px x height_px`` covering
    ``(xmin, ymin) -> (xmax, ymax)`` in the given SRID.

    Args:
        points: Column of array of point geometries (WKB or WKT).
        values: Column of array of double values (same length as ``points``).
        xmin: Minimum X of the output raster extent.
        ymin: Minimum Y of the output raster extent.
        xmax: Maximum X of the output raster extent.
        ymax: Maximum Y of the output raster extent.
        width_px: Output raster width in pixels.
        height_px: Output raster height in pixels.
        srid: EPSG SRID of the extent / point geometries.
        power: IDW exponent (default 2.0).
        max_pts: Maximum neighbour points per cell (default 12).

    Returns:
        Raster tile column.
    """
    power_col = f.lit(2.0) if power is None else _col(power)
    max_pts_col = f.lit(12) if max_pts is None else _col(max_pts)
    return f.call_function(
        "gbx_rst_gridfrompoints",
        _col(points),
        _col(values),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
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
    srid: ColLike,
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
        srid: EPSG SRID.
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
        _col(srid),
        power_col,
        max_pts_col,
    )


# ---------------------------------------------------------------------------
# Delaunay-TIN Digital Terrain Model (DTM) interpolation
#
# Two wrappers: `rst_dtmfromgeoms` (non-aggregator, Z-valued points as an
# array column) + `rst_dtmfromgeoms_agg` (aggregator, one Z-valued point
# per row). Both delegate to gbx_rst_dtmfromgeoms / gbx_rst_dtmfromgeoms_agg.
# ---------------------------------------------------------------------------


def rst_dtmfromgeoms(
    points: ColLike,
    breaklines: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    no_data: ColLike = None,
) -> Column:
    """DTM from Z-valued points + optional breaklines via Delaunay-TIN interpolation.

    Output is a single-band Float64 GTiff of ``width_px x height_px`` over the bbox.
    For N-unit cells set ``width_px = round((xmax-xmin)/N)``,
    ``height_px = round((ymax-ymin)/N)`` (e.g. a 1000 m extent at 10 m cells -> 100 px).

    Args:
        points: Array column of Z-valued point geometries (WKB binary or WKT string).
        breaklines: Array column of breakline LineString geometries; pass an empty array for none.
        merge_tolerance: Delaunay segment-merge tolerance.
        snap_tolerance: Vertex-to-breakline snap tolerance.
        xmin, ymin, xmax, ymax: Output raster extent.
        width_px, height_px: Output raster size in pixels.
        srid: EPSG SRID.
        no_data: No-data sentinel (default -9999.0).

    Returns:
        Raster tile column.
    """
    nd = f.lit(-9999.0) if no_data is None else _col(no_data)
    return f.call_function(
        "gbx_rst_dtmfromgeoms",
        _col(points),
        _col(breaklines),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
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
    srid: ColLike,
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
        _col(srid),
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


def rst_sample(tile: ColLike, geom: ColLike) -> Column:
    """Sample raster pixel values at a POINT geometry — returns one Double per band.

    The point coordinates must be in the raster's CRS. Out-of-extent points
    return ``null`` (not a partial array).

    Args:
        tile: Raster tile column.
        geom: POINT geometry — WKB ``bytes`` or WKT ``string`` column.

    Returns:
        Column of ``ARRAY<DOUBLE>`` (one value per band) or ``null`` outside extent.
    """
    return f.call_function("gbx_rst_sample", _col(tile), _col(geom))


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


def rst_histogram(
    tile: ColLike,
    n_buckets: ColLike = None,
    min_val: ColLike = None,
    max_val: ColLike = None,
    include_nodata: ColLike = None,
) -> Column:
    """Per-band pixel histogram via ``band.GetHistogram``.

    Returns ``MAP<STRING, ARRAY<LONG>>`` keyed by ``"band_<i>"`` (1-based) with
    a length-``n_buckets`` array of bucket counts per band. Pixels outside
    ``[min_val, max_val]`` are excluded.

    Args:
        tile: Raster tile column.
        n_buckets: Number of equal-width buckets across ``[min_val, max_val]``
            (default 256).
        min_val: Histogram lower bound (default: derived from band statistics).
        max_val: Histogram upper bound (default: derived from band statistics).
        include_nodata: Reserved — GDAL excludes NoData regardless. Default False.

    Returns:
        Column of ``MAP<STRING, ARRAY<LONG>>``.
    """
    nb_col = f.lit(256) if n_buckets is None else _col(n_buckets)
    min_col = f.lit(None).cast("double") if min_val is None else _col(min_val)
    max_col = f.lit(None).cast("double") if max_val is None else _col(max_val)
    inc_col = f.lit(False) if include_nodata is None else _col(include_nodata)
    return f.call_function(
        "gbx_rst_histogram", _col(tile), nb_col, min_col, max_col, inc_col
    )


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
            ``ZSTD``, ``LERC``, ``JPEG``, ``WEBP``. Default ``"DEFLATE"``.
            String literals auto-wrapped via ``f.lit``.
        blocksize: Internal tile size in pixels (square). Default ``512``.
        overview_resampling: Downsampling algorithm for the overview pyramid —
            one of ``NEAREST``, ``AVERAGE``, ``GAUSS``, ``CUBIC``, ``CUBICSPLINE``,
            ``LANCZOS``, ``BILINEAR``, ``MODE``. Default ``"AVERAGE"``.

    Returns:
        COG-laid-out raster tile column.
    """
    comp_col = (
        f.lit("DEFLATE")
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
    distunits: Union[ColLike, None] = None,
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
        distunits: ``"GEO"`` (CRS ground units, default) or ``"PIXEL"``.
        max_distance: Optional cap on output distance (in the same units as
            ``distunits``). ``None`` = unlimited.

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
        if distunits is None
        else (f.lit(distunits) if isinstance(distunits, str) else _col(distunits))
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
) -> Column:
    """Compute a binary viewshed raster from a DEM and an observer POINT.

    Wraps ``gdal.ViewshedGenerate``. Output is a Byte raster matching the
    source extent / CRS: visible pixels = ``255``, invisible / out-of-range
    pixels = ``0``. Non-POINT ``observer_geom`` is rejected at runtime.

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

    Returns:
        Byte raster tile column (0 / 255).
    """
    th_col = f.lit(1.6) if target_height is None else _col(target_height)
    md_col = f.lit(None).cast("double") if max_distance is None else _col(max_distance)
    return f.call_function(
        "gbx_rst_viewshed",
        _col(tile),
        _col(observer_geom),
        _col(observer_height),
        th_col,
        md_col,
    )
