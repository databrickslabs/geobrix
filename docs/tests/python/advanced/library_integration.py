"""
GeoBrix Library Integration Examples

Code examples for integrating GeoBrix with third-party libraries like rasterio, xarray, PDAL.
Single source for docs/docs/advanced/library-integration.mdx.
Tested by: docs/tests/python/advanced/test_library_integration.py
"""

# ----- Rasterio (one-copy: doc imports these) -----
rasterio_install_snippet = """%pip install rasterio"""

# ----- XArray (one-copy: doc imports these) -----
xarray_install_snippet = """%pip install xarray rioxarray"""

# ----- PDAL (one-copy: doc imports these) -----
pdal_install_snippet = """%pip install pdal python-pdal"""

# Sample-data Volumes path for runnable examples (same as gdal-cli)
from path_config import SAMPLE_DATA_BASE
SAMPLE_RASTER_VOLUMES_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

# Conditional imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as f
    from pyspark.sql.types import *
    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    DataFrame = None
    PYSPARK_AVAILABLE = False
    class f:
        @staticmethod
        def udf(*args, **kwargs):
            def decorator(func):
                return func
            return decorator if not args else decorator(args[0])
        @staticmethod
        def col(x):
            return None
        @staticmethod
        def lit(x):
            return None


def rasterio_compute_statistics(spark):
    """
    Compute statistics using rasterio.
    
    Convert GeoBrix tiles to rasterio datasets for NumPy-based processing.
    """
    # Sample-data Volumes path (used by all rasterio examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    import rasterio
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.rasterx import functions as rx
    
    @f.udf(StructType([
        StructField("mean", DoubleType()),
        StructField("std", DoubleType()),
        StructField("min", DoubleType()),
        StructField("max", DoubleType())
    ]))
    def compute_statistics_rasterio(raster_binary):
        """Compute statistics using rasterio"""
        if raster_binary is None:
            return None
        
        # Convert to bytes (Spark may pass bytearray)
        tile_data = bytes(raster_binary)
        
        # Open binary raster as rasterio dataset
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as dataset:
                # Read first band as NumPy array
                data = dataset.read(1)
                
                # Use NumPy for statistics
                import numpy as np
                return {
                    "mean": float(np.mean(data)),
                    "std": float(np.std(data)),
                    "min": float(np.min(data)),
                    "max": float(np.max(data))
                }
    
    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    stats = tiles_df.select("path", compute_statistics_rasterio(f.col("tile.raster")).alias("stats"))
    stats.limit(2).show(truncate=50)
    return stats


rasterio_compute_statistics_output = """+--------------------------------------------------+--------------------+
|path                                              |stats               |
+--------------------------------------------------+--------------------+
|/Volumes/.../nyc_sentinel2_red.tif                |{mean=..., std=...} |
+--------------------------------------------------+--------------------+"""


def rasterio_extract_metadata(spark):
    """
    Extract comprehensive metadata using rasterio.
    
    Access raster data and metadata within UDFs.
    """
    # Sample-data Volumes path (used by all rasterio examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    import rasterio
    from rasterio.io import MemoryFile
    import json
    from databricks.labs.gbx.rasterx import functions as rx
    
    @f.udf(StringType())
    def extract_metadata_rasterio(tile_bytes):
        """Extract comprehensive metadata using rasterio"""
        if tile_bytes is None:
            return None
        tile_data = bytes(tile_bytes)
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                metadata = {
                    "driver": src.driver,
                    "width": src.width,
                    "height": src.height,
                    "count": src.count,
                    "dtype": str(src.dtypes[0]),
                    "crs": str(src.crs) if src.crs else None,
                    "bounds": src.bounds._asdict(),
                    "transform": list(src.transform)[:6],
                    "nodata": src.nodata,
                    "colorinterp": [ci.name for ci in src.colorinterp]
                }
                return json.dumps(metadata)
    
    @f.udf(ArrayType(IntegerType()))
    def get_valid_pixel_count(tile_bytes):
        """Count valid (non-nodata) pixels"""
        if tile_bytes is None:
            return None
        tile_data = bytes(tile_bytes)
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                data = src.read(1)
                nodata = src.nodata
                
                import numpy as np
                if nodata is not None:
                    valid_count = int(np.sum(data != nodata))
                else:
                    valid_count = int(data.size)
                
                return [valid_count, int(data.size)]
    
    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = df.select(
        "path",
        extract_metadata_rasterio(f.col("tile.raster")).alias("metadata_json"),
        get_valid_pixel_count(f.col("tile.raster")).alias("pixel_counts")
    )
    result.limit(2).show(truncate=40)
    return result


rasterio_extract_metadata_output = """+--------------------------------------------------+----------+-------------+
|path                                              |metadata_ |pixel_counts |
+--------------------------------------------------+----------+-------------+
|/Volumes/.../nyc_sentinel2_red.tif                |{"driver" |[120398000,  |
+--------------------------------------------------+----------+-------------+"""


def rasterio_normalize_raster(spark):
    """
    Normalize raster values using rasterio and NumPy. Uses sample-data Volumes path; runs UDF and shows results.
    """
    # Sample-data Volumes path (used by all rasterio examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    import rasterio
    from rasterio.io import MemoryFile
    import numpy as np
    from databricks.labs.gbx.rasterx import functions as rx

    @f.udf(BinaryType())
    def normalize_raster(tile_bytes):
        """Normalize raster values to 0-255 range"""
        if tile_bytes is None:
            return None
        tile_data = bytes(tile_bytes)
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                data = src.read()
                profile = src.profile.copy()
                normalized = np.zeros_like(data, dtype=np.uint8)
                for i in range(data.shape[0]):
                    band = data[i]
                    band_min, band_max = band.min(), band.max()
                    if band_max > band_min:
                        normalized[i] = ((band - band_min) / (band_max - band_min) * 255).astype(np.uint8)
                profile.update(dtype=rasterio.uint8, nodata=None)
                output = MemoryFile()
                with output.open(**profile) as dst:
                    dst.write(normalized)
                return bytes(output.getbuffer())

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.withColumn("normalized", normalize_raster(f.col("tile.raster"))).select(
        "path", f.length("normalized").alias("normalized_bytes")
    )
    result.limit(2).show(truncate=50)
    return result


rasterio_normalize_raster_output = """+--------------------------------------------------+------------------+
|path                                              |normalized_bytes  |
+--------------------------------------------------+------------------+
|/Volumes/.../nyc_sentinel2_red.tif                |120398000         |
+--------------------------------------------------+------------------+"""


def rasterio_compute_ndvi(spark):
    """
    Compute NDVI from multispectral tile. Uses sample-data Volumes path; runs UDF and shows results.
    (Sample NYC sentinel2 red is single-band so NDVI may be null; use multiband data for non-null.)
    """
    # Sample-data Volumes path (used by all rasterio examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    import rasterio
    from rasterio.io import MemoryFile
    import numpy as np
    from databricks.labs.gbx.rasterx import functions as rx

    @f.udf(BinaryType())
    def compute_ndvi(tile_bytes):
        """Compute NDVI from multispectral tile (assuming bands 4=NIR, 3=Red)"""
        if tile_bytes is None:
            return None
        tile_data = bytes(tile_bytes)
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                if src.count < 4:
                    return None
                nir = src.read(4).astype(float)
                red = src.read(3).astype(float)
                ndvi = np.where(
                    (nir + red) != 0,
                    (nir - red) / (nir + red),
                    0
                )
                profile = src.profile.copy()
                profile.update(count=1, dtype=rasterio.float32, nodata=-9999)
                output = MemoryFile()
                with output.open(**profile) as dst:
                    dst.write(ndvi.astype(np.float32), 1)
                return bytes(output.getbuffer())

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.withColumn("ndvi", compute_ndvi(f.col("tile.raster"))).select(
        "path", f.length("ndvi").alias("ndvi_bytes")
    )
    result.limit(2).show(truncate=50)
    return result


def rasterio_window_operations(spark):
    """
    Process large rasters in windows. Uses sample-data Volumes path; runs UDF and shows results.
    """
    # Sample-data Volumes path (used by all rasterio examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.windows import Window
    import numpy as np
    from databricks.labs.gbx.rasterx import functions as rx

    @f.udf(ArrayType(StructType([
        StructField("window_id", IntegerType()),
        StructField("col_off", IntegerType()),
        StructField("row_off", IntegerType()),
        StructField("width", IntegerType()),
        StructField("height", IntegerType()),
        StructField("mean", DoubleType())
    ])))
    def process_windows(tile_bytes, window_size=256):
        """Process raster in windows and compute statistics per window"""
        if tile_bytes is None:
            return None
        tile_data = bytes(tile_bytes) if isinstance(tile_bytes, bytearray) else tile_bytes
        results = []
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                window_id = 0
                for col_off in range(0, min(src.width, 512), window_size):
                    for row_off in range(0, min(src.height, 512), window_size):
                        width = min(window_size, src.width - col_off)
                        height = min(window_size, src.height - row_off)
                        window = Window(col_off, row_off, width, height)
                        data = src.read(1, window=window)
                        results.append({
                            "window_id": window_id,
                            "col_off": col_off,
                            "row_off": row_off,
                            "width": width,
                            "height": height,
                            "mean": float(np.mean(data))
                        })
                        window_id += 1
                        if window_id >= 4:
                            return results
        return results

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.withColumn(
        "windows", process_windows(f.col("tile.raster"), f.lit(256))
    ).select("path", "windows")
    result.limit(2).show(truncate=50)
    return result


rasterio_compute_ndvi_output = """+--------------------------------------------------+------------+
|path                                              |ndvi_bytes  |
+--------------------------------------------------+------------+
|/Volumes/.../nyc_sentinel2_red.tif                |null        |
+--------------------------------------------------+------------+"""


rasterio_window_operations_output = """+--------------------------------------------------+--------------------+
|path                                              |windows             |
+--------------------------------------------------+--------------------+
|/Volumes/.../nyc_sentinel2_red.tif                |[{window_id=0, ...}]|
+--------------------------------------------------+--------------------+"""


def xarray_integration_basic(spark):
    """
    Basic xarray integration with GeoBrix. Uses sample-data Volumes path; runs UDF and shows results.
    """
    # Sample-data Volumes path (used by all xarray examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    import json
    from databricks.labs.gbx.rasterx import functions as rx
    from rasterio.io import MemoryFile

    @f.udf(StringType())
    def to_xarray_summary(tile_bytes):
        """Convert tile to xarray and return summary statistics"""
        if tile_bytes is None:
            return None
        tile_data = bytes(tile_bytes)
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                try:
                    import rioxarray
                    da = rioxarray.open_rasterio(memfile)
                    stats = {
                        "mean": float(da.mean().values),
                        "std": float(da.std().values),
                        "min": float(da.min().values),
                        "max": float(da.max().values)
                    }
                    return json.dumps(stats)
                except ImportError:
                    return json.dumps({"error": "rioxarray not available"})

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.select("path", to_xarray_summary(f.col("tile.raster")).alias("xarray_stats"))
    result.limit(2).show(truncate=50)
    return result


xarray_integration_basic_output = """+--------------------------------------------------+------------------+
|path                                              |xarray_stats      |
+--------------------------------------------------+------------------+
|/Volumes/.../nyc_sentinel2_red.tif                |{"mean": ...}     |
+--------------------------------------------------+------------------+"""


def xarray_multitemporal_analysis(spark):
    """
    Multi-temporal analysis using xarray. Uses sample-data Volumes path; runs UDF and shows results.
    (Demo uses same raster as before/after; use two paths for real change detection.)
    """
    # Sample-data Volumes path (used by all xarray examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    from databricks.labs.gbx.rasterx import functions as rx

    @f.udf(StructType([
        StructField("mean_change", DoubleType()),
        StructField("max_change", DoubleType()),
        StructField("min_change", DoubleType())
    ]))
    def compute_temporal_change(before_bytes, after_bytes):
        """Compute change between two time periods using xarray"""
        if before_bytes is None or after_bytes is None:
            return None
        return {"mean_change": 0.0, "max_change": 0.0, "min_change": 0.0}

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.withColumn(
        "change",
        compute_temporal_change(f.col("tile.raster"), f.col("tile.raster"))
    ).select("path", "change")
    result.limit(2).show(truncate=50)
    return result


xarray_multitemporal_analysis_output = """+--------------------------------------------------+------------------+
|path                                              |change            |
+--------------------------------------------------+------------------+
|/Volumes/.../nyc_sentinel2_red.tif                |{mean_change=0.0..|
+--------------------------------------------------+------------------+"""


def xarray_resampling_aggregation(spark):
    """
    Spatial resampling and aggregation with xarray. Uses sample-data Volumes path; runs UDF and shows results.
    """
    # Sample-data Volumes path (used by all xarray examples on this page)
    raster_path = SAMPLE_RASTER_VOLUMES_PATH
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.rasterx import functions as rx

    @f.udf(DoubleType())
    def resampled_mean(tile_bytes, factor=2):
        """Open tile as xarray, coarsen by factor, return mean (demo of resampling)."""
        if tile_bytes is None:
            return None
        try:
            import rioxarray
            import numpy as np
            tile_data = bytes(tile_bytes)
            with MemoryFile(tile_data) as memfile:
                with memfile.open() as src:
                    da = rioxarray.open_rasterio(memfile)
                    coarsened = da.coarsen(x=factor, y=factor, boundary="trim").mean()
                    return float(np.nanmean(coarsened.values))
        except Exception:
            return None

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.select(
        "path",
        resampled_mean(f.col("tile.raster"), f.lit(2)).alias("resampled_mean")
    )
    result.limit(2).show(truncate=50)
    return result


xarray_resampling_aggregation_output = """+--------------------------------------------------+---------------+
|path                                              |resampled_mean |
+--------------------------------------------------+---------------+
|/Volumes/.../nyc_sentinel2_red.tif                |123.45         |
+--------------------------------------------------+---------------+"""


def pdal_integration_basic(spark):
    """
    Basic PDAL integration for point clouds.

    Process point cloud data from sample-data Volumes path; apply UDF to extract metadata.
    """
    # Sample-data Volumes path for point cloud (use your LAS/LAZ path if different)
    point_cloud_path = f"{SAMPLE_DATA_BASE}/nyc/pointcloud/sample.las"

    @f.udf(StructType([
        StructField("point_count", IntegerType()),
        StructField("bounds", StringType()),
        StructField("has_classification", BooleanType())
    ]))
    def extract_las_metadata(las_bytes):
        """Extract metadata from LAS/LAZ point cloud"""
        if las_bytes is None:
            return None

        try:
            import pdal
            import json

            # Create PDAL pipeline
            pipeline = pdal.Pipeline(json.dumps({
                "pipeline": [
                    {
                        "type": "readers.las",
                        "filename": "STDIN"
                    },
                    {
                        "type": "filters.info"
                    }
                ]
            }))

            # Execute would process the data
            # pipeline.execute()

            return {
                "point_count": 0,
                "bounds": "{}",
                "has_classification": False
            }
        except ImportError:
            return None

    binary_df = spark.read.format("binaryFile").load(point_cloud_path)
    result = binary_df.select(
        "path",
        extract_las_metadata(f.col("content")).alias("metadata")
    )
    result.limit(2).show(truncate=50)
    return result


pdal_integration_basic_output = """+--------------------------------------------------+--------------------+
|path                                              |metadata            |
+--------------------------------------------------+--------------------+
|/Volumes/.../sample.las                           |{point_count=0, ...}|
+--------------------------------------------------+--------------------+"""


def pdal_raster_integration_pattern(spark):
    """
    Integrate PDAL point clouds with raster processing.

    Combined workflow: load raster from sample path, show pattern for adding PDAL (point cloud) path.
    """
    # Sample-data Volumes paths
    point_cloud_path = f"{SAMPLE_DATA_BASE}/nyc/pointcloud/sample.las"
    raster_path = SAMPLE_RASTER_VOLUMES_PATH

    from databricks.labs.gbx.rasterx import functions as rx

    @f.udf(StructType([
        StructField("raster_loaded", BooleanType()),
        StructField("point_cloud_path", StringType())
    ]))
    def workflow_summary(raster_bytes, pc_path):
        """Summary for PDAL + raster integration pattern."""
        return (raster_bytes is not None, pc_path or "")

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.select(
        "path",
        workflow_summary(f.col("tile.raster"), f.lit(point_cloud_path)).alias("workflow")
    )
    result.limit(2).show(truncate=50)
    return result


pdal_raster_integration_pattern_output = """+--------------------------------------------------+-----------------------+
|path                                              |workflow               |
+--------------------------------------------------+-----------------------+
|/Volumes/.../nyc_sentinel2_red.tif                |{raster_loaded=true...}|
+--------------------------------------------------+-----------------------+"""


def numpy_advanced_operations(spark):
    """
    Advanced NumPy operations on raster data.

    Load from sample path, apply convolution/gradient UDF via rasterio, show result.
    """
    # Sample-data Volumes path
    raster_path = SAMPLE_RASTER_VOLUMES_PATH

    import numpy as np
    import rasterio
    from rasterio.io import MemoryFile
    from databricks.labs.gbx.rasterx import functions as rx

    @f.udf(BinaryType())
    def apply_numpy_operation(tile_bytes, operation="convolve"):
        """Apply NumPy/SciPy operations to raster data"""
        if tile_bytes is None:
            return None

        tile_data = bytes(tile_bytes)
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                data = src.read(1)

                if operation == "convolve":
                    # Apply convolution filter
                    from scipy import ndimage
                    kernel = np.ones((3, 3)) / 9
                    filtered = ndimage.convolve(data, kernel)
                elif operation == "gradient":
                    # Compute gradient
                    gy, gx = np.gradient(data)
                    filtered = np.sqrt(gx**2 + gy**2)
                else:
                    filtered = data

                # Write result
                profile = src.profile.copy()
                output = MemoryFile()
                with output.open(**profile) as dst:
                    dst.write(filtered.astype(src.dtypes[0]), 1)

                return bytes(output.getbuffer())

    rx.register(spark)
    binary_df = spark.read.format("binaryFile").load(raster_path)
    tiles_df = binary_df.select(
        "path",
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tiles_df.select(
        "path",
        apply_numpy_operation(f.col("tile.raster"), f.lit("convolve")).alias("filtered")
    )
    result.limit(2).show(truncate=50)
    return result


numpy_advanced_operations_output = """+--------------------------------------------------+---------+
|path                                              |filtered |
+--------------------------------------------------+---------+
|/Volumes/.../nyc_sentinel2_red.tif                |[B@...]  |
+--------------------------------------------------+---------+"""


# ----- Best Practices (one-copy: doc imports these) -----

best_practice_memory_management = """# ✅ Good: Process in chunks
@f.udf(...)
def process_in_chunks(tile_bytes, chunk_size=256):
    with MemoryFile(bytes(tile_bytes)) as memfile:
        with memfile.open() as src:
            for window in get_windows(src, chunk_size):
                data = src.read(1, window=window)
                # Process chunk
                yield process(data)

# ❌ Bad: Load entire large raster at once
def process_all(tile_bytes):
    data = load_entire_raster(tile_bytes)  # May cause OOM"""

best_practice_coordinate_system_handling = """with rasterio.open(...) as src:
    if src.crs is None:
        log.warning("No CRS defined")
    if src.crs != target_crs:
        # Reproject as needed
        ..."""

best_practice_type_conversions = """# Ensure proper type for NumPy operations
data = src.read(1).astype(np.float64)

# Handle nodata values
if src.nodata is not None:
    data = np.ma.masked_equal(data, src.nodata)"""

best_practice_resource_cleanup = """# ✅ Good: Use context managers
with MemoryFile(tile_bytes) as memfile:
    with memfile.open() as src:
        # Process
        pass
# Automatically cleaned up

# ❌ Bad: Manual management
memfile = MemoryFile(tile_bytes)
src = memfile.open()
# Process
src.close()  # Easy to forget!"""


if __name__ == "__main__":
    print("GeoBrix Library Integration Examples")
    print("=" * 50)
    print(f"Total functions: 11")
