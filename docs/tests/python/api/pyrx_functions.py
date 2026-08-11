"""
Python code examples for pyrx (lightweight raster API) documentation.
Single source of truth for docs/docs/api/pyrx-functions.mdx

JAR-free: the accessor/transform/clip examples build a synthetic in-memory
GeoTIFF (rasterio + numpy); the Setup example loads the committed sample
rasters under src/test/resources/binary/ (shown in the docs as GTIFF_SAMPLE_DIR
etc.). No /Volumes mount or path_config import is needed.
"""

try:
    from databricks.labs.gbx.pyrx import functions as rx
except ImportError:
    rx = None

# Committed sample rasters (resolved from the repo root) that the Setup example
# loads. In the rendered docs these appear as GTIFF_SAMPLE_DIR / GTIFF_MULTI_DIR /
# DTM_DIR / NETCDF_DIR placeholders — a reader points them at their own files.
from pathlib import Path as _Path  # noqa: E402

_REPO_ROOT = _Path(__file__).resolve().parents[4]
_BIN = _REPO_ROOT / "src/test/resources/binary"
_SAMPLE_PATHS = {
    "gtiff": str(_BIN / "geotiff-small/nyc_sentinel2_red_small.tif"),
    "gtiff_multi": str(_BIN / "geotiff-small/rgb_nir_small.tif"),
    "dtm": str(_BIN / "elevation/dem_small.tif"),
    "netcdf": str(
        _BIN
        / "netcdf-CMIP5"
        / "prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc"
        "_rcp45_r1i1p1_20201201-20201231.nc"
    ),
}


# ---------------------------------------------------------------------------
# Shared helper — builds a small in-memory GeoTIFF (used by every example)
# ---------------------------------------------------------------------------


def _make_geotiff_bytes(width=4, height=3, count=2, epsg=4326):
    """Return in-memory 2-band float32 GTiff bytes (4 x 3, EPSG:4326, origin (10, 50), 0.5 px)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    from pyspark.sql import functions as f

    raster = _make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


# ---------------------------------------------------------------------------
# Setup example
# ---------------------------------------------------------------------------


def pyrx_setup_example(spark):
    """Register pyrx and load each sample raster into a `tile` DataFrame + temp view.

    Assumes the geobrix wheel is already installed (see the Installation guide).
    The examples on this page read from four temp views; this builds all of them.
    """
    from pyspark.sql import functions as f
    from databricks.labs.gbx.pyrx import functions as rx

    # Point these at your own rasters. The examples on this page use four samples:
    GTIFF_SAMPLE_DIR = _SAMPLE_PATHS["gtiff"]  # single-band GeoTIFF
    GTIFF_MULTI_DIR = _SAMPLE_PATHS["gtiff_multi"]  # multi-band GeoTIFF (red/NIR/green)
    DTM_DIR = _SAMPLE_PATHS["dtm"]  # digital elevation model
    NETCDF_DIR = _SAMPLE_PATHS["netcdf"]  # NetCDF with subdatasets

    def load_tiles(path, driver, view):
        # binaryFile reads the bytes; rst_fromcontent wraps them into a `tile` column.
        binary_df = spark.read.format("binaryFile").load(path)
        tile_df = binary_df.select(
            rx.rst_fromcontent("content", f.lit(driver)).alias("tile")
        )
        tile_df.createOrReplaceTempView(view)
        return tile_df

    # Load the default single-band raster into the `rasters` view:
    rasters = load_tiles(GTIFF_SAMPLE_DIR, "GTiff", "rasters")

    # The other three views load exactly the same way (driver "GTiff", or "netCDF"):
    load_tiles(GTIFF_MULTI_DIR, "GTiff", "multiband_rasters")
    load_tiles(DTM_DIR, "GTiff", "dem_rasters")
    load_tiles(NETCDF_DIR, "netCDF", "netcdf_rasters")
    return rasters


pyrx_setup_example_output = """
Four temp views created — `rasters` (single-band), `multiband_rasters`,
`dem_rasters`, and `netcdf_rasters` — each a DataFrame with a `tile` column.
Every example on this page reads from one of these views.
"""


# ---------------------------------------------------------------------------
# Accessor example
# ---------------------------------------------------------------------------


def pyrx_accessors_example(spark):
    """Read basic raster properties from the tile struct."""
    from databricks.labs.gbx.pyrx import functions as rx

    tile_df = _tile_df(spark, width=4, height=3, count=2, epsg=4326)
    row = tile_df.select(
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_srid("tile").alias("srid"),
        rx.rst_numbands("tile").alias("bands"),
    ).first()
    return row


pyrx_accessors_example_output = """
Row(width=4, height=3, srid=4326, bands=2)
"""


# ---------------------------------------------------------------------------
# Transform example
# ---------------------------------------------------------------------------


def pyrx_transform_example(spark):
    """Reproject the raster tile to a target CRS (EPSG:3857)."""
    from databricks.labs.gbx.pyrx import functions as rx

    tile_df = _tile_df(spark, epsg=4326)
    out = tile_df.select(rx.rst_transform("tile", 3857).alias("t"))
    srid = out.select(rx.rst_srid("t").alias("s")).first()["s"]
    return srid


pyrx_transform_example_output = """
3857
"""


# ---------------------------------------------------------------------------
# Clip example
# ---------------------------------------------------------------------------


def pyrx_clip_example(spark):
    """Clip the raster to a smaller bounding box geometry (WKB)."""
    import shapely.wkb
    from pyspark.sql import functions as f
    from shapely.geometry import box
    from databricks.labs.gbx.pyrx import functions as rx

    tile_df = _tile_df(spark, width=4, height=3, epsg=4326)
    # Clip to a 1 x 0.5 degree box — smaller than the full 2 x 1.5 degree extent.
    clip_geom = shapely.wkb.dumps(box(10.5, 49.0, 11.5, 49.5))
    df = tile_df.withColumn("clip_geom", f.lit(clip_geom))
    out = df.select(rx.rst_clip("tile", "clip_geom", False).alias("t"))
    row = out.select(
        rx.rst_width("t").alias("w"),
        rx.rst_height("t").alias("h"),
    ).first()
    return row


pyrx_clip_example_output = """
Clipped tile is smaller than the original 4 x 3 (e.g. Row(w=2, h=1)).
"""


# ---------------------------------------------------------------------------
# Polygonize example
# ---------------------------------------------------------------------------


def pyrx_polygonize_example(spark):
    """Extract vector polygons from contiguous equal-value regions in the raster.

    rst_polygonize (pyrx) is a streaming Python UDTF. Register first, then query
    with a SQL LATERAL table function — this avoids buffering all polygons in
    memory (unbounded fan-out OOM guard).
    """
    import numpy as np
    from pyspark.sql import functions as f
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin
    from databricks.labs.gbx.pyrx import functions as rx

    # Build a 4 x 4 raster with a 2 x 2 block of value 5.0 in the centre;
    # all other pixels are NoData so polygonize traces only the filled region.
    data = np.full((4, 4), -9999.0, dtype="float32")
    data[1:3, 1:3] = 5.0
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 4, 1, 1),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        raster_bytes = mf.read()

    df = spark.createDataFrame([(raster_bytes,)], ["raster"])
    tile_df = df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))

    # rst_polygonize is a streaming Python UDTF; invoke via SQL LATERAL.
    rx.register(spark)
    tile_df.createOrReplaceTempView("_pyrx_poly_demo")
    rows = spark.sql(
        "SELECT t.value FROM _pyrx_poly_demo, LATERAL gbx_rst_polygonize(tile, 1, 4) t"
    ).collect()
    return rows


pyrx_polygonize_example_output = """
[Row(value=5.0)]
"""


# ---------------------------------------------------------------------------
# SQL example
# ---------------------------------------------------------------------------


def pyrx_sql_example(spark):
    """Register pyrx SQL functions and query them from Spark SQL."""
    from pyspark.sql import functions as f
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)

    raster_bytes = _make_geotiff_bytes(width=4, height=3, count=2, epsg=4326)
    df = spark.createDataFrame([(raster_bytes,)], ["raster"])
    tile_df = df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    tile_df.createOrReplaceTempView("rasters_sql")

    result = spark.sql(
        "SELECT gbx_rst_width(tile) AS width, gbx_rst_srid(tile) AS srid FROM rasters_sql"
    )
    row = result.first()
    return row


pyrx_sql_example_output = """
Row(width=4, srid=4326)
"""
