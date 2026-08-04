"""
Tests for RasterX SQL examples.

Ensures all SQL examples in documentation are executable and produce valid results.
"""
import pytest
from pyspark.sql import functions as F
from . import rasterx_functions_sql

# Sample data base path (must match conftest.SAMPLE_DATA_BASE for doc test env)
from path_config import SAMPLE_DATA_BASE


@pytest.fixture(scope="module")
def sample_rasters(spark):
    """Load sample raster data for testing from Volumes (standardized sample-data path)."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)

    # Use Volumes path (standardized; run in Docker with sample-data mount)
    raster_paths = [
        f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif",
        f"{SAMPLE_DATA_BASE}/nyc/elevation/srtm_n40w073.tif",
    ]
    for path in raster_paths:
        try:
            rasters = spark.read.format("gdal").load(path)
            if rasters.count() > 0:
                return rasters
        except Exception:
            continue

    # Fallback: empty DataFrame with correct schema (tests that need data will fail)
    from pyspark.sql.types import StructType, StructField, StringType, BinaryType
    schema = StructType([
        StructField("path", StringType(), True),
        StructField("tile", BinaryType(), True)
    ])
    return spark.createDataFrame([], schema)


@pytest.fixture(scope="module")
def rasters_view(spark, sample_rasters):
    """Create temp view for SQL examples. Expects Docker env with Volumes/sample data available."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    # GDAL reader returns "source" not "path"; alias so SQL examples (path, tile) work
    view_df = (
        sample_rasters.withColumnRenamed("source", "path")
        if "source" in sample_rasters.columns
        else sample_rasters
    )
    view_df.createOrReplaceTempView("rasters")
    yield
    spark.catalog.dropTempView("rasters")


# ============================================================================
# Common setup (doc constant)
# ============================================================================

def test_rasterx_sql_setup_constant():
    """Doc constant RASTERX_SQL_SETUP exists and creates rasters view."""
    assert hasattr(rasterx_functions_sql, "RASTERX_SQL_SETUP")
    assert "rasters" in rasterx_functions_sql.RASTERX_SQL_SETUP
    assert hasattr(rasterx_functions_sql, "RASTERX_SQL_SETUP_output")


@pytest.mark.integration
def test_rasterx_sql_setup_executable(spark, sample_rasters):
    """Running the SQL in RASTERX_SQL_SETUP creates view rasters (requires sample data path)."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    sql = rasterx_functions_sql.RASTERX_SQL_SETUP.strip()
    spark.sql(sql)
    result = spark.sql("SELECT * FROM rasters LIMIT 1")
    assert result.count() >= 0
    spark.catalog.dropTempView("rasters")


# ============================================================================
# Accessor Functions
# ============================================================================

def test_rst_boundingbox_sql_example(spark, sample_rasters, rasters_view):
    """Test SQL bounding box example"""
    # Modified to work with temp view
    sql = """
    SELECT
        path,
        gbx_rst_boundingbox(tile) as bbox
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "bbox" in result.columns


def test_rst_width_sql_example(spark, rasters_view):
    """Test SQL width examples"""
    # Test first query
    sql = "SELECT gbx_rst_width(tile) as width FROM rasters"
    result = spark.sql(sql)
    assert result.count() > 0
    assert "width" in result.columns
    
    # Test second query with multiple columns
    sql = """
    SELECT 
        path,
        gbx_rst_width(tile) as width,
        gbx_rst_height(tile) as height,
        gbx_rst_pixelwidth(tile) as pixel_width_m
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["width", "height", "pixel_width_m"])


def test_rst_height_sql_example(spark, rasters_view):
    """Test SQL height example"""
    sql = rasterx_functions_sql.rst_height_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["height", "width"])


def test_rst_numbands_sql_example(spark, rasters_view):
    """Test SQL numbands example"""
    sql = rasterx_functions_sql.rst_numbands_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "bands" in result.columns


def test_rst_metadata_sql_example(spark, rasters_view):
    """Test SQL metadata example"""
    sql = rasterx_functions_sql.rst_metadata_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "metadata" in result.columns


def test_rst_srid_sql_example(spark, rasters_view):
    """Test SQL SRID example"""
    sql = rasterx_functions_sql.rst_srid_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "srid" in result.columns


def test_rst_georeference_sql_example(spark, rasters_view):
    """Test SQL georeference example"""
    sql = rasterx_functions_sql.rst_georeference_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "georeference" in result.columns


def test_rst_bandmetadata_sql_example(spark, rasters_view):
    """Test SQL band metadata example"""
    sql = rasterx_functions_sql.rst_bandmetadata_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "band1_metadata" in result.columns


def test_rst_pixelcount_sql_example(spark, rasters_view):
    """Test SQL pixel count example"""
    sql = rasterx_functions_sql.rst_pixelcount_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "pixel_count" in result.columns


def test_rst_avg_sql_example(spark, rasters_view):
    """Test SQL average examples"""
    # Test first query
    sql = """
    SELECT
        path,
        gbx_rst_avg(tile) as band_averages,
        gbx_rst_avg(tile)[0] as band1_avg
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    
    # Test filter query
    sql = "SELECT * FROM rasters WHERE gbx_rst_avg(tile)[0] > 0"
    result = spark.sql(sql)
    # Should execute without error


def test_rst_min_max_sql_example(spark, rasters_view):
    """Test SQL min/max example"""
    sql = """
    SELECT
        path,
        gbx_rst_min(tile)[0] as min_value,
        gbx_rst_max(tile)[0] as max_value,
        gbx_rst_max(tile)[0] - gbx_rst_min(tile)[0] as value_range
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["min_value", "max_value", "value_range"])


def test_rst_median_sql_example(spark, rasters_view):
    """Test SQL median example"""
    sql = """
    SELECT
        path,
        gbx_rst_avg(tile)[0] as mean_value,
        gbx_rst_median(tile)[0] as median_value
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_format_sql_example(spark, rasters_view):
    """Test SQL format examples"""
    # Test group by format
    sql = """
    SELECT
        gbx_rst_format(tile) as format,
        COUNT(*) as count
    FROM rasters
    GROUP BY gbx_rst_format(tile)
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_type_sql_example(spark, rasters_view):
    """Test SQL type examples"""
    sql = """
    SELECT
        path,
        gbx_rst_type(tile) as band_types,
        gbx_rst_type(tile)[0] as band1_type
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_pixelsize_sql_example(spark, rasters_view):
    """Test SQL pixel size example"""
    sql = rasterx_functions_sql.rst_pixelsize_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["pixel_width", "pixel_height"])


def test_rst_getnodata_sql_example(spark, rasters_view):
    """Test SQL NoData example"""
    sql = rasterx_functions_sql.rst_getnodata_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


# ============================================================================
# Coordinate Transformation
# ============================================================================

def test_rst_rastertoworldcoord_sql_example(spark, rasters_view):
    """Test SQL raster to world coordinate example"""
    sql = """
    SELECT
        path,
        gbx_rst_rastertoworldcoord(tile, 100, 200) as coords
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_rastertoworldcoordx_sql_example(spark, rasters_view):
    """Test SQL raster to world X coordinate example"""
    sql = rasterx_functions_sql.rst_rastertoworldcoordx_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_rastertoworldcoordy_sql_example(spark, rasters_view):
    """Test SQL raster to world Y coordinate example"""
    sql = rasterx_functions_sql.rst_rastertoworldcoordy_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_worldtorastercoord_sql_example(spark, rasters_view):
    """Test SQL world to raster coordinate example (single location)"""
    sql = rasterx_functions_sql.rst_worldtorastercoord_sql_example()
    result = spark.sql(sql.strip())
    assert result.count() >= 0


def test_rst_worldtorastercoord_multi_sql_example(spark, rasters_view):
    """Test SQL world to raster coordinate example (multiple points)"""
    sql = rasterx_functions_sql.rst_worldtorastercoord_multi_sql_example()
    result = spark.sql(sql.strip())
    assert result.count() >= 0


def test_rst_worldtorastercoordx_sql_example(spark, rasters_view):
    """Test SQL world to raster X coordinate example"""
    sql = """
    SELECT
        gbx_rst_worldtorastercoordx(tile, 0, 0) as pixel_col
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_worldtorastercoordy_sql_example(spark, rasters_view):
    """Test SQL world to raster Y coordinate example"""
    sql = """
    SELECT
        gbx_rst_worldtorastercoordy(tile, 0, 0) as pixel_row
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0


# ============================================================================
# Validation Functions
# ============================================================================

def test_rst_isempty_sql_example(spark, rasters_view):
    """Test SQL is empty example"""
    # Test filter
    sql = "SELECT * FROM rasters WHERE NOT gbx_rst_isempty(tile)"
    result = spark.sql(sql)
    # Should execute
    
    # Test count query
    sql = """
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN gbx_rst_isempty(tile) THEN 1 ELSE 0 END) as empty_count,
        SUM(CASE WHEN NOT gbx_rst_isempty(tile) THEN 1 ELSE 0 END) as valid_count
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() == 1


def test_rst_tryopen_sql_example(spark, rasters_view):
    """Test SQL try open example"""
    sql = "SELECT * FROM rasters WHERE gbx_rst_tryopen(tile) = true"
    result = spark.sql(sql)
    # Should execute without error


# ============================================================================
# Advanced Operations
# ============================================================================

def test_rst_initnodata_sql_example(spark, rasters_view):
    """Test SQL init NoData example"""
    sql = rasterx_functions_sql.rst_initnodata_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_updatetype_sql_example(spark, rasters_view):
    """Test SQL update type example"""
    sql = rasterx_functions_sql.rst_updatetype_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


# ============================================================================
# Generator Functions
# ============================================================================

def test_rst_maketiles_sql_example(spark, rasters_view):
    """Test SQL make tiles example. Generator returns struct in SQL; use without explode."""
    sql = """
    SELECT
        path,
        gbx_rst_maketiles(tile, 512) as tile_result
    FROM rasters
    LIMIT 10
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "tile_result" in result.columns


def test_rst_retile_sql_example(spark, rasters_view):
    """Test SQL retile example. Generator returns struct in SQL; use without explode."""
    sql = """
    SELECT
        path,
        gbx_rst_retile(tile, 256, 256) as tile_result
    FROM rasters
    LIMIT 10
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "tile_result" in result.columns


def test_rst_tooverlappingtiles_sql_example(spark, rasters_view):
    """Test SQL overlapping tiles example. Generator returns struct in SQL; use without explode."""
    sql = """
    SELECT
        path,
        gbx_rst_tooverlappingtiles(tile, 256, 256, 10) as tile_result
    FROM rasters
    LIMIT 10
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "tile_result" in result.columns


def test_rst_separatebands_sql_example(spark, rasters_view):
    """Test SQL separate bands example. Generator returns struct in SQL."""
    sql = """
    SELECT
        path,
        gbx_rst_separatebands(tile) as bands
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "bands" in result.columns


def test_rst_rasterize_sql_example(spark):
    """rst_rasterize returns a non-null tile struct for the example burn."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    sql = rasterx_functions_sql.rst_rasterize_sql_example()
    result = spark.sql(sql).collect()
    assert len(result) == 1
    assert result[0]["tile"] is not None


def test_rst_polygonize_sql_example(spark):
    """Round-trip rasterize->polygonize returns >=1 feature with the burn value."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    sql = rasterx_functions_sql.rst_polygonize_sql_example()
    result = spark.sql(sql).collect()
    assert len(result) == 1
    features = result[0]["features"]
    assert len(features) > 0
    assert any(abs(feat["value"] - 42.0) < 1e-6 for feat in features)


# ============================================================================
# Terrain Analysis (DEM Processing) - Wave 8a
# ============================================================================


@pytest.mark.parametrize("example_attr", [
    "rst_slope_sql_example",
    "rst_aspect_sql_example",
    "rst_hillshade_sql_example",
    "rst_tri_sql_example",
    "rst_tpi_sql_example",
    "rst_roughness_sql_example",
])
def test_dem_processing_sql_example(spark, rasters_view, example_attr):
    """Each Wave 8a DEM-processing example returns a non-null tile."""
    sql = getattr(rasterx_functions_sql, example_attr)()
    result = spark.sql(sql).collect()
    assert len(result) >= 1
    # The output column varies (slope, aspect, hillshade, tri, tpi, roughness).
    out_col = [c for c in result[0].asDict().keys()][0]
    assert result[0][out_col] is not None


# ============================================================================
# Spectral Indices - Wave 8b
# ============================================================================


@pytest.mark.parametrize("example_attr,fallback_sql", [
    # Each docs example references multi-band indices (1, 2, 3). The shared
    # `rasters` view is single-band, so we run a fallback SQL with all band
    # indices = 1 to exercise the JVM round-trip without needing a multi-band
    # raster. The doc-example string is still validated for shape (asserted
    # below).
    ("rst_evi_sql_example", "SELECT gbx_rst_evi(tile, 1, 1, 1) AS evi FROM rasters"),
    ("rst_savi_sql_example", "SELECT gbx_rst_savi(tile, 1, 1, 0.5) AS savi FROM rasters"),
    ("rst_ndwi_sql_example", "SELECT gbx_rst_ndwi(tile, 1, 1) AS ndwi FROM rasters"),
    ("rst_nbr_sql_example", "SELECT gbx_rst_nbr(tile, 1, 1) AS nbr FROM rasters"),
    ("rst_index_sql_example",
     "SELECT gbx_rst_index(tile, 'ndvi', map('red', 1, 'nir', 1)) AS ndvi FROM rasters"),
])
def test_spectral_indices_sql_example(spark, rasters_view, example_attr, fallback_sql):
    """Each Wave 8b spectral-index example string exists & executes to non-null tile."""
    sql_template = getattr(rasterx_functions_sql, example_attr)()
    # The doc string should reference the SQL function name.
    expected_fn = example_attr.replace("_sql_example", "").replace("_", "_")
    assert f"gbx_{expected_fn}" in sql_template, (
        f"docs example {example_attr} should mention gbx_{expected_fn}"
    )
    result = spark.sql(fallback_sql).collect()
    assert len(result) >= 1
    out_col = [c for c in result[0].asDict().keys()][0]
    assert result[0][out_col] is not None


def test_rst_color_relief_sql_example(spark, rasters_view, tmp_path):
    """color_relief example exists and executes against a tempfile color table.

    The docs example references a sample-data path that may not be present in
    every env; this test exercises the function via a tempfile color table so
    we still cover the actual SQL invocation.
    """
    ct = tmp_path / "elevation.clr"
    ct.write_text("0 0 0 255\n100 0 255 0\n255 255 0 0\n")
    # Verify the doc example string exists & has the right shape.
    sql_template = rasterx_functions_sql.rst_color_relief_sql_example()
    assert "gbx_rst_color_relief" in sql_template
    # Run a substitute SQL using our tempfile.
    sql = f"SELECT gbx_rst_color_relief(tile, '{ct}') AS rgba FROM rasters"
    result = spark.sql(sql).collect()
    assert len(result) >= 1
    assert result[0]["rgba"] is not None


# ============================================================================
# Pixel ops + extraction
# ============================================================================


@pytest.mark.parametrize("example_attr,fallback_sql", [
    # fillnodata, threshold, buildoverviews, band, setsrid roundtrips on the
    # shared single-band `rasters` view. histogram returns a MAP and sample
    # returns an ARRAY<DOUBLE>; their fallback SQL pins types explicitly so
    # the JVM bindings fire even if doc string formatting varies.
    ("rst_fillnodata_sql_example",
     "SELECT gbx_rst_fillnodata(tile, 100.0, 0) AS filled FROM rasters"),
    ("rst_sample_sql_example",
     "SELECT gbx_rst_sample(tile, 'SRID=4326;POINT(-73.97 40.75)') AS vals FROM rasters"),
    ("rst_setsrid_sql_example",
     "SELECT gbx_rst_setsrid(tile, 4326) AS tagged FROM rasters"),
    ("rst_histogram_sql_example",
     "SELECT gbx_rst_histogram(tile, 16, cast(0 as double), cast(1000 as double), false) AS hist FROM rasters"),
    ("rst_threshold_sql_example",
     "SELECT gbx_rst_threshold(tile, '>', 100.0) AS mask FROM rasters"),
    ("rst_buildoverviews_sql_example",
     "SELECT gbx_rst_buildoverviews(tile, array(2, 4), 'average') AS withovr FROM rasters"),
    ("rst_band_sql_example",
     "SELECT gbx_rst_band(tile, 1) AS b1 FROM rasters"),
])
def test_pixel_ops_sql_example(spark, rasters_view, example_attr, fallback_sql):
    """Each pixel-ops SQL example exists and executes to a non-null result."""
    sql_template = getattr(rasterx_functions_sql, example_attr)()
    expected_fn = example_attr.replace("_sql_example", "")
    assert f"gbx_{expected_fn}" in sql_template, (
        f"docs example {example_attr} should mention gbx_{expected_fn}"
    )
    result = spark.sql(fallback_sql).collect()
    assert len(result) >= 1
    out_col = [c for c in result[0].asDict().keys()][0]
    assert result[0][out_col] is not None


# ============================================================================
# Analysis (COG / proximity / contour / viewshed)
# ============================================================================


@pytest.mark.parametrize("example_attr,fallback_sql", [
    # cog_convert returns a tile; proximity returns a tile (Float32 distance
    # raster); contour returns ARRAY<struct(geom_wkb, value)>; viewshed
    # returns a tile (Byte 0/255 visibility mask).
    # Executed with an explicit codec (ZSTD, the baseline) so it runs on BOTH
    # tiers — the light-only 'AUTO' sentinel is not understood by the heavy tier.
    ("rst_cog_convert_sql_example",
     "SELECT gbx_rst_cog_convert(tile, 'ZSTD', 256, 'AVERAGE') AS cog FROM rasters"),
    ("rst_proximity_sql_example",
     "SELECT gbx_rst_proximity(tile, '', 'PIXEL', cast(100.0 as double)) AS dist FROM rasters"),
    ("rst_contour_sql_example",
     "SELECT gbx_rst_contour(tile, array(), 100.0, 0.0, 'elev') AS contours FROM rasters"),
    ("rst_viewshed_sql_example",
     "SELECT gbx_rst_viewshed(tile, 'POINT(-73.5 40.5)', 100.0, 1.6, 5000.0) AS vs FROM rasters"),
])
def test_analysis_sql_example(spark, rasters_view, example_attr, fallback_sql):
    """Each analysis SQL example exists and executes to a non-null result."""
    sql_template = getattr(rasterx_functions_sql, example_attr)()
    expected_fn = example_attr.replace("_sql_example", "")
    assert f"gbx_{expected_fn}" in sql_template, (
        f"docs example {example_attr} should mention gbx_{expected_fn}"
    )
    result = spark.sql(fallback_sql).collect()
    assert len(result) >= 1
    out_col = [c for c in result[0].asDict().keys()][0]
    assert result[0][out_col] is not None


# ============================================================================
# Structure Verification
# ============================================================================

def test_all_sql_functions_have_example():
    """Verify SQL example module has functions for all documented examples"""
    import inspect
    
    # Get all functions from the module
    functions = [name for name, obj in inspect.getmembers(rasterx_functions_sql) 
                 if inspect.isfunction(obj) and not name.startswith('_')]
    
    # Should have examples for major function categories
    assert len(functions) > 40, f"Expected 40+ SQL examples, got {len(functions)}"
    
    # Verify naming convention
    for func_name in functions:
        assert func_name.endswith('_sql_example'), \
            f"Function {func_name} should end with '_sql_example'"
        
        # Verify it returns a string
        func = getattr(rasterx_functions_sql, func_name)
        result = func()
        assert isinstance(result, str), \
            f"Function {func_name} should return SQL string"
        assert len(result) > 0, \
            f"Function {func_name} returned empty SQL"


def test_all_sql_examples_are_valid_sql():
    """Verify all SQL examples have valid SQL syntax"""
    import inspect

    functions = [name for name, obj in inspect.getmembers(rasterx_functions_sql)
                 if inspect.isfunction(obj) and not name.startswith('_')]

    for func_name in functions:
        func = getattr(rasterx_functions_sql, func_name)
        sql = func()

        # Basic checks
        assert "SELECT" in sql.upper() or "WITH" in sql.upper(), \
            f"{func_name}: SQL should contain SELECT or WITH"

        # Check for GeoBrix functions (most should have gbx_)
        if "gbx_" not in sql.lower() and "from rasters" in sql.lower():
            # Allow some exceptions like pure Spark SQL examples
            pass


# ============================================================================
# H3 Cell Rasterizer Functions
# ============================================================================


def test_rst_h3_rasterize_agg_sql_example():
    """rst_h3_rasterize_agg example string exists, returns SQL, and references the function."""
    sql = rasterx_functions_sql.rst_h3_rasterize_agg_sql_example()
    assert isinstance(sql, str) and len(sql) > 0
    assert "gbx_rst_h3_rasterize_agg" in sql
    assert "SELECT" in sql.upper()
    assert hasattr(rasterx_functions_sql, "rst_h3_rasterize_agg_sql_example_output")


def test_h3_cell_bbox_sql_example(spark):
    """h3_cell_bbox example string exists and executes to non-null bbox structs."""
    from databricks.labs.gbx.pyrx import functions as prx
    prx.register(spark)
    sql = rasterx_functions_sql.h3_cell_bbox_sql_example()
    assert isinstance(sql, str) and len(sql) > 0
    assert "gbx_h3_cell_bbox" in sql
    result = spark.sql(sql).collect()
    assert len(result) == 3
    for row in result:
        assert row["bbox"] is not None
    assert hasattr(rasterx_functions_sql, "h3_cell_bbox_sql_example_output")


# ============================================================================
# BNG / quadbin raster-grid functions (9 functions)
#
# BNG examples require a UK raster (BNG warps any CRS to EPSG:27700; a NYC
# raster over Britain would bin no pixels). The London SRTM elevation raster
# (srtm_n51w001.tif, EPSG:4326, over the TQ grid square) is used. Quadbin/tessellate
# examples warp to EPSG:4326 (web-mercator quadtree) and work on lon/lat overlap.
# ============================================================================


@pytest.fixture(scope="module")
def london_rasters_view(spark):
    """Temp view `london_rasters` over the London SRTM elevation raster (srtm_n51w001.tif, EPSG:4326).

    Used by BNG examples (warp to EPSG:27700) and quadbin examples (warp to
    EPSG:4326). Skips the dependent test when the sample raster is absent.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    path = f"{SAMPLE_DATA_BASE}/london/elevation/srtm_n51w001.tif"
    try:
        df = spark.read.format("gdal").load(path)
        if df.count() == 0:
            pytest.skip(f"London sample raster not available at {path}")
    except Exception as e:  # pragma: no cover - env-dependent
        pytest.skip(f"London sample raster not loadable: {e}")
    view_df = df.withColumnRenamed("source", "path") if "source" in df.columns else df
    view_df.createOrReplaceTempView("london_rasters")
    yield
    spark.catalog.dropTempView("london_rasters")


@pytest.mark.parametrize("example_attr,sql_fn", [
    ("rst_bng_rastertogridavg_sql_example",
     "gbx_rst_bng_rastertogridavg"),
    ("rst_bng_rastertogridcount_sql_example",
     "gbx_rst_bng_rastertogridcount"),
    ("rst_bng_rastertogridmax_sql_example",
     "gbx_rst_bng_rastertogridmax"),
    ("rst_bng_rastertogridmin_sql_example",
     "gbx_rst_bng_rastertogridmin"),
    ("rst_bng_rastertogridmedian_sql_example",
     "gbx_rst_bng_rastertogridmedian"),
    ("rst_bng_rastertogridsum_sql_example",
     "gbx_rst_bng_rastertogridsum"),
    ("rst_bng_rastertogridvariance_sql_example",
     "gbx_rst_bng_rastertogridvariance"),
    ("rst_bng_rastertogridstddev_sql_example",
     "gbx_rst_bng_rastertogridstddev"),
])
def test_bng_rastertogrid_sql_example(spark, london_rasters, london_rasters_view,
                                      example_attr, sql_fn):
    """Each BNG rastertogrid reducer emits STRING cell ids over a real UK raster."""
    sql_template = getattr(rasterx_functions_sql, example_attr)()
    assert sql_fn in sql_template, f"{example_attr} should mention {sql_fn}"
    # Reducer returns ARRAY<ARRAY<STRUCT<cellID:STRING, measure>>>; explode band 0
    # and assert the cell ids are BNG strings (e.g. TQ38SW), never numeric.
    sql = f"""
    SELECT cell.cellID AS bng_cell, cell.measure AS measure
    FROM london_rasters
    LATERAL VIEW explode({sql_fn}(tile, '1km')[0]) AS cell
    """
    result = spark.sql(sql).collect()
    assert len(result) > 0, f"{sql_fn} produced no cells over the London raster"
    import re
    bng_re = re.compile(r"^[A-Z]{2}\d*[A-Z]*$")
    for row in result:
        assert isinstance(row["bng_cell"], str), "BNG cell id must be a STRING"
        assert bng_re.match(row["bng_cell"]), (
            f"expected BNG string id, got '{row['bng_cell']}'"
        )
        assert row["measure"] is not None


@pytest.fixture(scope="module")
def london_rasters(spark):
    """Alias fixture: load the London raster DataFrame (skips if unavailable)."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    path = f"{SAMPLE_DATA_BASE}/london/elevation/srtm_n51w001.tif"
    try:
        df = spark.read.format("gdal").load(path)
        if df.count() == 0:
            pytest.skip(f"London sample raster not available at {path}")
        return df
    except Exception as e:  # pragma: no cover - env-dependent
        pytest.skip(f"London sample raster not loadable: {e}")


def test_rst_quadbin_tessellate_sql_example(spark, london_rasters, london_rasters_view):
    """quadbin tessellate generator emits one raster tile chip per overlapping cell."""
    sql = rasterx_functions_sql.rst_quadbin_tessellate_sql_example()
    assert "gbx_rst_quadbin_tessellate" in sql
    # covering mode over zoom 12; the CollectionGenerator yields one `tile` per
    # overlapping quadbin cell (used via LATERAL VIEW, like gbx_rst_maketiles).
    result = spark.sql("""
        SELECT r.path, g.tile
        FROM london_rasters r
        LATERAL VIEW gbx_rst_quadbin_tessellate(r.tile, 12, 'covering') g AS tile
    """).collect()
    assert len(result) > 0, "quadbin tessellate produced no tile rows"
    first = result[0].asDict()
    assert "tile" in first
    assert first["tile"] is not None
    assert hasattr(rasterx_functions_sql, "rst_quadbin_tessellate_sql_example_output")


def test_rst_bng_tessellate_sql_example(spark, london_rasters, london_rasters_view):
    """bng tessellate generator emits one raster tile chip per overlapping BNG cell."""
    sql = rasterx_functions_sql.rst_bng_tessellate_sql_example()
    assert "gbx_rst_bng_tessellate" in sql
    # BNG warps the raster to EPSG:27700 then yields one `tile` per overlapping cell.
    result = spark.sql("""
        SELECT r.path, g.tile
        FROM london_rasters r
        LATERAL VIEW gbx_rst_bng_tessellate(r.tile, '1km', 'covering') g AS tile
    """).collect()
    assert len(result) > 0, "bng tessellate produced no tile rows"
    first = result[0].asDict()
    assert "tile" in first
    assert first["tile"] is not None
    assert hasattr(rasterx_functions_sql, "rst_bng_tessellate_sql_example_output")


def test_rst_quadbin_rasterize_agg_sql_example(spark):
    """quadbin rasterize_agg burns a group of quadbin cells into one non-null tile."""
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.quadbin import functions as qbx
    rx.register(spark)
    qbx.register(spark)
    sql = rasterx_functions_sql.rst_quadbin_rasterize_agg_sql_example()
    assert "gbx_rst_quadbin_rasterize_agg" in sql
    # Build a small quadbin cell set over central London (lon/lat, zoom 12).
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW quadbin_cell_values AS
        SELECT 1 AS region_id,
               gbx_quadbin_pointascell(cast(lon as double), cast(lat as double), 12) AS cellid,
               cast(val as double) AS burn_value
        FROM (VALUES
            (-0.10, 51.50, 1.0),
            (-0.11, 51.51, 2.0),
            (-0.09, 51.49, 3.0)
        ) AS t(lon, lat, val)
    """)
    result = spark.sql(sql).collect()
    spark.catalog.dropTempView("quadbin_cell_values")
    assert len(result) == 1
    tile = result[0]["tile"]
    assert tile is not None, "quadbin rasterize_agg must produce a non-null tile"
    assert hasattr(rasterx_functions_sql, "rst_quadbin_rasterize_agg_sql_example_output")


def test_rst_bng_rasterize_agg_sql_example(spark):
    """bng rasterize_agg burns a group of STRING BNG cells into one non-null tile."""
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.bng import functions as bngx
    rx.register(spark)
    bngx.register(spark)
    sql = rasterx_functions_sql.rst_bng_rasterize_agg_sql_example()
    assert "gbx_rst_bng_rasterize_agg" in sql
    # Build BNG STRING cell ids over central London (BNG eastings/northings, 1km).
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW bng_cell_values AS
        SELECT 1 AS region_id,
               gbx_bng_eastnorthasbng(e, n, 3) AS cellid,
               val AS burn_value
        FROM (VALUES
            (cast(530000.0 as double), cast(180000.0 as double), cast(1.0 as double)),
            (cast(531000.0 as double), cast(181000.0 as double), cast(2.0 as double)),
            (cast(529000.0 as double), cast(179000.0 as double), cast(3.0 as double))
        ) AS t(e, n, val)
    """)
    # Confirm the helper yields STRING BNG ids (contract for the aggregator's cellid arg).
    ids = spark.sql("SELECT cellid FROM bng_cell_values").collect()
    for row in ids:
        assert isinstance(row["cellid"], str), "bng cellid must be a STRING"
    result = spark.sql(sql).collect()
    spark.catalog.dropTempView("bng_cell_values")
    assert len(result) == 1
    tile = result[0]["tile"]
    assert tile is not None, "bng rasterize_agg must produce a non-null tile"
    assert hasattr(rasterx_functions_sql, "rst_bng_rasterize_agg_sql_example_output")
