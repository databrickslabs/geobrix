"""netcdf_gbx (lightweight) Reader Examples — single source of truth.

Code shown in docs/docs/readers/netcdf.mdx is imported from here. Pure-Python
DataSource V2 reader; no JAR (registered via gbx.ds.register). Two modes:
raster (CF regular grid -> the shared (source, tile) schema) and vector
(DSG points, or any 2-D field incl. swath -> one point per cell).
"""

REGISTER = """# Register the lightweight DataSources (once per session)
from databricks.labs.gbx.ds.register import register
register(spark)"""

READ_RASTER = """# Raster mode (default): a CF regular lat/lon grid -> (source, tile).
# e.g. ERA5 2m-temperature on a regular grid.
df = (spark.read.format("netcdf_gbx")
      .option("variable", "t2m")
      .load("/Volumes/main/geobrix_samples/netcdf/era5_sample.nc"))
df.show()"""

READ_VECTOR = """# Vector mode: swath / point NetCDF -> one point per cell (lossless,
# no regridding). e.g. Sentinel-5P TROPOMI CH4 (netCDF-4 swath); the quality
# flag rides along as its own column so you filter downstream.
df = (spark.read.format("netcdf_gbx")
      .option("mode", "vector")
      .option("group", "/PRODUCT")
      .option("variables", "methane_mixing_ratio_bias_corrected,qa_value")
      .load("/Volumes/main/geobrix_samples/netcdf/s5p_ch4_sample.nc"))
# columns: <vars...>, geom_0 (WKB), geom_0_srid, geom_0_srid_proj"""


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def read_raster(spark, path):
    """Verify READ_RASTER: raster mode yields the (source, tile) schema."""
    _register(spark)
    df = spark.read.format("netcdf_gbx").option("variable", "t2m").load(path)
    assert [f.name for f in df.schema.fields] == ["source", "tile"]
    assert df.collect()[0]["tile"]["cellid"] == -1
    return df


def read_vector(spark, path, variables, group=None):
    """Verify READ_VECTOR: vector mode yields attrs + geom_0 (WKB) + srid cols."""
    _register(spark)
    reader = (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", variables)
    )
    if group:
        reader = reader.option("group", group)
    df = reader.load(path)
    assert "geom_0" in df.columns and "geom_0_srid" in df.columns
    assert df.count() >= 1
    return df
