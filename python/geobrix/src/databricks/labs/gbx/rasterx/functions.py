"""RasterX Python API.

Thin wrappers around GeoBrix Scala functions (gbx_rst_*). Register with
rx.register(spark) then use the functions on raster tile columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_rst_<name>;
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Column


def register(_spark: SparkSession) -> None:
    """Register RasterX functions with the Spark session.

    Call once (e.g. after creating the session) so that gbx_rst_* SQL
    functions are available. Uses the active Spark session if needed.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "rasterx").load().collect()


def rst_avg(tile: Column) -> Column:
    """Return the average pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_avg", tile)


def rst_bandmetadata(tile: Column, band: Column) -> Column:
    """Return metadata for the given band index (e.g. nodata, data type).

    Args:
        tile: Raster tile column.
        band: 1-based band index column.

    Returns:
        Column of map (string -> string).
    """
    return f.call_function("gbx_rst_bandmetadata", tile, band)


def rst_boundingbox(tile: Column) -> Column:
    """Return the bounding box of the raster in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of WKB (binary).
    """
    return f.call_function("gbx_rst_boundingbox", tile)


def rst_format(tile: Column) -> Column:
    """Return the GDAL format/driver name of the raster (e.g. GTiff).

    Args:
        tile: Raster tile column.

    Returns:
        Column of format string.
    """
    return f.call_function("gbx_rst_format", tile)


def rst_georeference(tile: Column) -> Column:
    """Return the georeference (affine transform) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> double).
    """
    return f.call_function("gbx_rst_georeference", tile)


def rst_getnodata(tile: Column) -> Column:
    """Return the NoData value for the raster (or null if not set).

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band), or null.
    """
    return f.call_function("gbx_rst_getnodata", tile)


def rst_getsubdataset(tile: Column, subset_name: Column) -> Column:
    """Return a sub-dataset (e.g. HDF sublayer) by name.

    Args:
        tile: Raster tile column.
        subset_name: Name of the sub-dataset.

    Returns:
        Column of raster tile (sub-dataset).
    """
    return f.call_function("gbx_rst_getsubdataset", tile, subset_name)


def rst_height(tile: Column) -> Column:
    """Return the pixel height (number of rows) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer height.
    """
    return f.call_function("gbx_rst_height", tile)


def rst_max(tile: Column) -> Column:
    """Return the maximum pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_max", tile)


def rst_median(tile: Column) -> Column:
    """Return the median pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_median", tile)


def rst_memsize(tile: Column) -> Column:
    """Return the approximate memory size of the tile in bytes.

    Args:
        tile: Raster tile column.

    Returns:
        Column of long (bytes).
    """
    return f.call_function("gbx_rst_memsize", tile)


def rst_metadata(tile: Column) -> Column:
    """Return full metadata of the raster (driver, dimensions, CRS, etc.).

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> string).
    """
    return f.call_function("gbx_rst_metadata", tile)


def rst_min(tile: Column) -> Column:
    """Return the minimum pixel value per band for the tile.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of double (one per band).
    """
    return f.call_function("gbx_rst_min", tile)


def rst_numbands(tile: Column) -> Column:
    """Return the number of bands in the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer band count.
    """
    return f.call_function("gbx_rst_numbands", tile)


def rst_pixelcount(tile: Column) -> Column:
    """Return the valid pixel count per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of long (one per band).
    """
    return f.call_function("gbx_rst_pixelcount", tile)


def rst_pixelheight(tile: Column) -> Column:
    """Return the pixel height (ground size in Y) in CRS units.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double (may be negative).
    """
    return f.call_function("gbx_rst_pixelheight", tile)


def rst_pixelwidth(tile: Column) -> Column:
    """Return the pixel width (ground size in X) in CRS units.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_pixelwidth", tile)


def rst_rotation(tile: Column) -> Column:
    """Return the rotation component of the georeference (if any).

    Args:
        tile: Raster tile column.

    Returns:
        Column of rotation (double).
    """
    return f.call_function("gbx_rst_rotation", tile)


def rst_scalex(tile: Column) -> Column:
    """Return the X scale (pixel size in X) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_scalex", tile)


def rst_scaley(tile: Column) -> Column:
    """Return the Y scale (pixel size in Y) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double (often negative).
    """
    return f.call_function("gbx_rst_scaley", tile)


def rst_skewx(tile: Column) -> Column:
    """Return the X skew component of the georeference.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_skewx", tile)


def rst_skewy(tile: Column) -> Column:
    """Return the Y skew component of the georeference.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_skewy", tile)


def rst_srid(tile: Column) -> Column:
    """Return the spatial reference ID (EPSG code) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer SRID.
    """
    return f.call_function("gbx_rst_srid", tile)


def rst_subdatasets(tile: Column) -> Column:
    """Return the sub-dataset names and descriptions (e.g. for HDF/NetCDF).

    Args:
        tile: Raster tile column.

    Returns:
        Column of map (string -> string, name to description).
    """
    return f.call_function("gbx_rst_subdatasets", tile)


def rst_summary(tile: Column) -> Column:
    """Return a short text summary of the raster (dimensions, CRS, bands).

    Args:
        tile: Raster tile column.

    Returns:
        Column of string summary.
    """
    return f.call_function("gbx_rst_summary", tile)


def rst_type(tile: Column) -> Column:
    """Return the raster data type (e.g. Byte, Int16, Float32) per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of strings (one per band).
    """
    return f.call_function("gbx_rst_type", tile)


def rst_upperleftx(tile: Column) -> Column:
    """Return the X coordinate of the upper-left corner in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_upperleftx", tile)


def rst_upperlefty(tile: Column) -> Column:
    """Return the Y coordinate of the upper-left corner in world coordinates.

    Args:
        tile: Raster tile column.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_upperlefty", tile)


def rst_width(tile: Column) -> Column:
    """Return the pixel width (number of columns) of the raster.

    Args:
        tile: Raster tile column.

    Returns:
        Column of integer width.
    """
    return f.call_function("gbx_rst_width", tile)


# Aggregators


def rst_combineavg_agg(tile: Column) -> Column:
    """Aggregate multiple raster tiles by averaging (use with groupBy).

    Args:
        tile: Raster tile column.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg_agg", tile)


def rst_derivedband_agg(tile: Column, pyfunc: str, func_name: str) -> Column:
    """Aggregate tiles and apply a Python UDF per band (use with groupBy).

    Args:
        tile: Raster tile column.
        pyfunc: Python source code of the UDF (string).
        func_name: Name of the callable in pyfunc.

    Returns:
        Column of derived raster tile.
    """
    return f.call_function("gbx_rst_derivedband_agg", tile, f.lit(pyfunc), f.lit(func_name))


def rst_merge_agg(tile: Column) -> Column:
    """Aggregate multiple raster tiles by merging (use with groupBy).

    Args:
        tile: Raster tile column.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge_agg", tile)


# Constructors


def rst_fromcontent(content: Column, driver: Column) -> Column:
    """Build a raster tile from binary content and GDAL driver name.

    Args:
        content: Column of binary content (e.g. from binaryFile reader).
        driver: GDAL driver name (e.g. GTiff, COG).

    Returns:
        Column of raster tile.
    """
    return f.call_function("gbx_rst_fromcontent", content, driver)


def rst_fromfile(path: Column, driver: Column) -> Column:
    """Build a raster tile from a file path and GDAL driver name.

    Args:
        path: Column of file path (string).
        driver: GDAL driver name (e.g. GTiff).

    Returns:
        Column of raster tile.
    """
    return f.call_function("gbx_rst_fromfile", path, driver)


def rst_frombands(bands: Column) -> Column:
    """Build a raster tile from a list of band tiles (same dimensions).

    Args:
        bands: Column of array of raster tiles (one per band).

    Returns:
        Column of multi-band raster tile.
    """
    return f.call_function("gbx_rst_frombands", bands)


# Generators


def rst_h3_tessellate(tile: Column, resolution: Column) -> Column:
    """Tessellate the raster into H3 cells at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of array of (H3 index, tile) or similar.
    """
    return f.call_function("gbx_rst_h3_tessellate", tile, resolution)


def rst_maketiles(tile: Column, size_in_mb: Column) -> Column:
    """Split the raster into smaller tiles by approximate size in MB.

    Args:
        tile: Raster tile column.
        size_in_mb: Target tile size in megabytes (column).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function("gbx_rst_maketiles", tile, size_in_mb)


def rst_retile(tile: Column, tile_width: Column, tile_height: Column) -> Column:
    """Retile the raster into tiles of the given pixel dimensions.

    Args:
        tile: Raster tile column.
        tile_width: Width of output tiles (pixels).
        tile_height: Height of output tiles (pixels).

    Returns:
        Column of array of raster tiles.
    """
    return f.call_function("gbx_rst_retile", tile, tile_width, tile_height)


def rst_separatebands(tile: Column) -> Column:
    """Split the raster into one tile per band.

    Args:
        tile: Raster tile column.

    Returns:
        Column of array of single-band raster tiles.
    """
    return f.call_function("gbx_rst_separatebands", tile)


def rst_tooverlappingtiles(
    tile: Column, tile_width: Column, tile_height: Column, overlap: Column
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
        "gbx_rst_tooverlappingtiles", tile, tile_width, tile_height, overlap
    )


# Grid


def rst_h3_rastertogridavg(tile: Column, resolution: Column) -> Column:
    """Compute average pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and avg).
    """
    return f.call_function("gbx_rst_h3_rastertogridavg", tile, resolution)


def rst_h3_rastertogridcount(tile: Column, resolution: Column) -> Column:
    """Compute pixel count per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and count).
    """
    return f.call_function("gbx_rst_h3_rastertogridcount", tile, resolution)


def rst_h3_rastertogridmax(tile: Column, resolution: Column) -> Column:
    """Compute maximum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and max).
    """
    return f.call_function("gbx_rst_h3_rastertogridmax", tile, resolution)


def rst_h3_rastertogridmin(tile: Column, resolution: Column) -> Column:
    """Compute minimum pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and min).
    """
    return f.call_function("gbx_rst_h3_rastertogridmin", tile, resolution)


def rst_h3_rastertogridmedian(tile: Column, resolution: Column) -> Column:
    """Compute median pixel value per H3 cell at the given resolution.

    Args:
        tile: Raster tile column.
        resolution: H3 resolution (0–15).

    Returns:
        Column of grid values (e.g. struct with H3 index and median).
    """
    return f.call_function("gbx_rst_h3_rastertogridmedian", tile, resolution)


# Operations


def rst_asformat(tile: Column, new_format: Column) -> Column:
    """Convert the raster to a different GDAL format (e.g. COG, Zarr).

    Args:
        tile: Raster tile column.
        new_format: Target format/driver name.

    Returns:
        Column of raster tile in the new format.
    """
    return f.call_function("gbx_rst_asformat", tile, new_format)


def rst_clip(tile: Column, clip: Column, cutline_all_touched: Column) -> Column:
    """Clip the raster to a geometry (or mask).

    Args:
        tile: Raster tile column.
        clip: Clipping geometry column (WKT/WKB) or raster mask.
        cutline_all_touched: If True, include pixels touched by the boundary.

    Returns:
        Column of clipped raster tile.
    """
    return f.call_function("gbx_rst_clip", tile, clip, cutline_all_touched)


def rst_combineavg(tiles: Column) -> Column:
    """Combine multiple raster tiles by averaging (same extent/cellsize).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of combined raster tile.
    """
    return f.call_function("gbx_rst_combineavg", tiles)


def rst_convolve(tile: Column, kernel: Column) -> Column:
    """Apply a convolution kernel to the raster.

    Args:
        tile: Raster tile column.
        kernel: Kernel matrix (e.g. 3x3) as column.

    Returns:
        Column of convolved raster tile.
    """
    return f.call_function("gbx_rst_convolve", tile, kernel)


def rst_derivedband(tile_expr: Column, pyfunc: str, func_name: str) -> Column:
    """Apply a Python UDF to each pixel (or band) to produce a derived band.

    Args:
        tile_expr: Raster tile column (or expression).
        pyfunc: Python source code of the UDF (string).
        func_name: Name of the callable in pyfunc.

    Returns:
        Column of raster tile with derived band(s).
    """
    return f.call_function("gbx_rst_derivedband", tile_expr, f.lit(pyfunc), f.lit(func_name))


def rst_filter(tile: Column, kernel_size: Column, operation: Column) -> Column:
    """Apply a filter (e.g. min, max, mean) over a kernel window.

    Args:
        tile: Raster tile column.
        kernel_size: Size of the kernel (e.g. 3 for 3x3).
        operation: Filter operation name (e.g. min, max, mean).

    Returns:
        Column of filtered raster tile.
    """
    return f.call_function("gbx_rst_filter", tile, kernel_size, operation)


def rst_initnodata(tile: Column) -> Column:
    """Initialise or fix NoData values in the raster (e.g. from metadata).

    Args:
        tile: Raster tile column.

    Returns:
        Column of raster tile with NoData set.
    """
    return f.call_function("gbx_rst_initnodata", tile)


def rst_isempty(tile: Column) -> Column:
    """Return true if the raster tile is empty or invalid.

    Args:
        tile: Raster tile column.

    Returns:
        Column of boolean.
    """
    return f.call_function("gbx_rst_isempty", tile)


def rst_mapalgebra(tiles: Column, expression: Column) -> Column:
    """Apply a map algebra expression to one or more tiles.

    Args:
        tiles: Column of array of raster tiles (or single tile).
        expression: Expression string (e.g. A + B, A * 2).

    Returns:
        Column of result raster tile.
    """
    return f.call_function("gbx_rst_mapalgebra", tiles, expression)


def rst_merge(tiles: Column) -> Column:
    """Merge multiple raster tiles into one (e.g. mosaic).

    Args:
        tiles: Column of array of raster tiles.

    Returns:
        Column of merged raster tile.
    """
    return f.call_function("gbx_rst_merge", tiles)


def rst_ndvi(tile: Column, red_band: Column, nir_band: Column) -> Column:
    """Compute NDVI from red and NIR band indices.

    Args:
        tile: Raster tile column.
        red_band: 1-based red band index.
        nir_band: 1-based NIR band index.

    Returns:
        Column of raster tile (single-band NDVI).
    """
    return f.call_function("gbx_rst_ndvi", tile, red_band, nir_band)


def rst_rastertoworldcoord(tile: Column, pixel_x: Column, pixel_y: Column) -> Column:
    """Convert pixel (x, y) to world (x, y) in the CRS of the raster.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of struct (x, y as double) in world coordinates.
    """
    return f.call_function("gbx_rst_rastertoworldcoord", tile, pixel_x, pixel_y)


def rst_rastertoworldcoordx(tile: Column, pixel_x: Column, pixel_y: Column) -> Column:
    """Convert pixel (x, y) to world X coordinate.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_rastertoworldcoordx", tile, pixel_x, pixel_y)


def rst_rastertoworldcoordy(tile: Column, pixel_x: Column, pixel_y: Column) -> Column:
    """Convert pixel (x, y) to world Y coordinate.

    Args:
        tile: Raster tile column.
        pixel_x: Pixel column index.
        pixel_y: Pixel row index.

    Returns:
        Column of double.
    """
    return f.call_function("gbx_rst_rastertoworldcoordy", tile, pixel_x, pixel_y)


def rst_transform(tile: Column, target_srid: Column) -> Column:
    """Reproject the raster to the target SRID (EPSG code).

    Args:
        tile: Raster tile column.
        target_srid: Target spatial reference ID (e.g. 4326 for WGS84).

    Returns:
        Column of reprojected raster tile.
    """
    return f.call_function("gbx_rst_transform", tile, target_srid)


def rst_tryopen(tile: Column) -> Column:
    """Attempt to open/validate the raster; return true if successful.

    Args:
        tile: Raster tile column.

    Returns:
        Column of boolean.
    """
    return f.call_function("gbx_rst_tryopen", tile)


def rst_updatetype(tile: Column, new_type: Column) -> Column:
    """Update the declared data type of the raster (e.g. after conversion).

    Args:
        tile: Raster tile column.
        new_type: New GDAL data type name (e.g. Byte, Float32).

    Returns:
        Column of raster tile with updated type metadata.
    """
    return f.call_function("gbx_rst_updatetype", tile, new_type)


def rst_worldtorastercoord(tile: Column, world_x: Column, world_y: Column) -> Column:
    """Convert world (x, y) to pixel (x, y) in the raster.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of struct (x, y as integer) in pixel coordinates.
    """
    return f.call_function("gbx_rst_worldtorastercoord", tile, world_x, world_y)


def rst_worldtorastercoordx(tile: Column, world_x: Column, world_y: Column) -> Column:
    """Convert world (x, y) to pixel column index.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of integer (pixel column index).
    """
    return f.call_function("gbx_rst_worldtorastercoordx", tile, world_x, world_y)


def rst_worldtorastercoordy(tile: Column, world_x: Column, world_y: Column) -> Column:
    """Convert world (x, y) to pixel row index.

    Args:
        tile: Raster tile column.
        world_x: World X coordinate.
        world_y: World Y coordinate.

    Returns:
        Column of integer (pixel row index).
    """
    return f.call_function("gbx_rst_worldtorastercoordy", tile, world_x, world_y)
