"""netcdf_gbx (lightweight) Writer Examples — single source of truth.

Code shown in docs/docs/writers/netcdf.mdx is imported from here. Pure-Python
DataSource V2 writer; no JAR (registered via gbx.ds.register). It is the inverse
of the netcdf_gbx reader:

- raster mode (default): each (source, tile) grid row -> one CF grid .nc.
- vector mode: point rows -> one CF Discrete Sampling Geometry .nc per partition.
- singleFile="true": one .nc (vector = points concatenated; raster = distinct
  variables sharing one grid merged).
- merge="true": post-hoc fold of .nc files already in a directory (no re-run).

There is NO heavyweight NetCDF writer — this lightweight writer runs on any
compute (Serverless, standard, ARM, classic).

The verifier functions synthesize their own tiny inputs via the netcdf_gbx
reader (the test writes small .nc grids/points to a tmp dir first), then
round-trip read -> write -> re-read to assert real values.
"""

REGISTER = """# Register the lightweight DataSources (once per session)
from databricks.labs.gbx.ds.register import register
register(spark)"""

WRITE_RASTER = """# Raster mode (default): each (source, tile) grid row -> one CF grid .nc.
# lat/lon coordinate variables, a _FillValue for NoData, and — when the tile CRS
# resolves to an EPSG code other than 4326 — a `crs` grid-mapping variable.
df = (spark.read.format("netcdf_gbx")
      .option("variable", "t2m")
      .load("/Volumes/main/geobrix_samples/netcdf/era5_sample.nc"))
(df.write
   .format("netcdf_gbx")
   .save("/Volumes/main/geobrix_samples/netcdf/output"))

# nameCol supplies the output filename stem; varNameCol overrides the variable name.
(df.write
   .format("netcdf_gbx")
   .option("nameCol", "file_stem")
   .option("varNameCol", "var_name")
   .save("/Volumes/main/geobrix_samples/netcdf/output"))"""

WRITE_VECTOR = """# Vector mode: point rows -> one CF DSG .nc per Spark partition
# (featureType="point"). Attribute columns plus lon/lat are written; null cells
# become CF fill (integer _FillValue for integer columns, NaN for float columns).
df = (spark.read.format("netcdf_gbx")
      .option("mode", "vector")
      .option("variables", "methane_mixing_ratio_bias_corrected,qa_value")
      .load("/Volumes/main/geobrix_samples/netcdf/s5p_ch4_sample.nc"))
(df.write
   .format("netcdf_gbx")
   .option("mode", "vector")
   .save("/Volumes/main/geobrix_samples/netcdf/output-points"))"""

WRITE_SINGLEFILE = """# Consolidate into ONE .nc with singleFile="true".
#   vector -> all points concatenated into one CF-DSG file.
#   raster -> distinct variables sharing one grid merged into one CF grid file.
# The merge is funneled through the driver, so reach for it when the combined
# output fits in driver memory; otherwise keep the default sharded parts.
(df.write
   .format("netcdf_gbx")
   .option("mode", "vector")
   .option("singleFile", "true")
   .option("fileName", "combined")     # optional output stem
   .save("/Volumes/main/geobrix_samples/netcdf/output-single"))"""

WRITE_MERGE = """# Post-hoc: merge the .nc files ALREADY in a directory into one, without
# re-running the source DataFrame (the DataFrame rows are ignored). Use keepParts
# to retain the source parts; partPrefix/fileName control shard/merged names.
(spark.range(1).write               # any DataFrame; the source rows are ignored
   .format("netcdf_gbx")
   .mode("append")
   .option("mode", "vector")
   .option("merge", "true")
   .option("keepParts", "true")      # keep the source parts alongside the merged file
   .save("/Volumes/main/geobrix_samples/netcdf/output-points"))"""


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def _read_raster(spark, in_nc, variable="t2m"):
    return (
        spark.read.format("netcdf_gbx")
        .option("variable", variable)
        .load(in_nc)
    )


def _read_vector(spark, in_nc, variables):
    return (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("variables", variables)
        .load(in_nc)
    )


def write_raster(spark, in_nc, out_dir, variable="t2m"):
    """Verify WRITE_RASTER: raster mode writes one CF grid .nc per row; re-read
    the written variable and confirm the physical values round-trip."""
    import os

    import netCDF4
    import numpy as np

    _register(spark)
    df = _read_raster(spark, in_nc, variable)
    df.write.format("netcdf_gbx").mode("overwrite").save(out_dir)

    files = [f for f in os.listdir(out_dir) if f.endswith(".nc")]
    assert files, "no .nc written"
    # variable name derived from the source subdataset selector NETCDF:"...":<var>
    with netCDF4.Dataset(os.path.join(out_dir, files[0]), "r") as nc:
        assert variable in nc.variables, f"{variable} not in {list(nc.variables)}"
        written = np.asarray(nc.variables[variable][:])
    with netCDF4.Dataset(in_nc, "r") as src:
        truth = np.asarray(src.variables[variable][:])
    assert written.shape == truth.shape
    np.testing.assert_allclose(written, truth, rtol=1e-4, atol=1e-4)


def write_vector(spark, in_nc, out_dir, variables):
    """Verify WRITE_VECTOR: vector mode writes CF-DSG point .nc; re-read obs
    count + lon/lat and confirm they round-trip."""
    import glob
    import os

    import netCDF4
    import numpy as np

    _register(spark)
    df = _read_vector(spark, in_nc, variables)
    expected = df.count()
    df.write.format("netcdf_gbx").mode("overwrite").option("mode", "vector").save(
        out_dir
    )

    parts = glob.glob(os.path.join(out_dir, "*.nc"))
    assert parts, "no .nc written"
    total = 0
    for p in parts:
        with netCDF4.Dataset(p, "r") as nc:
            assert nc.featureType == "point"
            n = int(nc.dimensions["obs"].size)
            total += n
            # lon/lat present as CF DSG coordinate variables
            assert "latitude" in nc.variables and "longitude" in nc.variables
            assert np.isfinite(np.asarray(nc.variables["latitude"][:])).all()
    assert total == expected, f"obs {total} != rows {expected}"


def write_singlefile(spark, in_nc, out_dir, variables):
    """Verify WRITE_SINGLEFILE: singleFile="true" collapses all partitions into
    exactly ONE .nc with every observation concatenated."""
    import glob
    import os

    import netCDF4

    _register(spark)
    df = _read_vector(spark, in_nc, variables).repartition(3)
    expected = df.count()
    (
        df.write.format("netcdf_gbx")
        .mode("overwrite")
        .option("mode", "vector")
        .option("singleFile", "true")
        .option("fileName", "combined")
        .save(out_dir)
    )

    ncs = glob.glob(os.path.join(out_dir, "*.nc"))
    assert len(ncs) == 1, f"expected exactly one .nc, got {ncs}"
    assert os.path.basename(ncs[0]) == "combined.nc"
    with netCDF4.Dataset(ncs[0], "r") as nc:
        assert int(nc.dimensions["obs"].size) == expected


def write_merge(spark, in_nc, out_dir, variables):
    """Verify WRITE_MERGE: write sharded parts, then post-hoc merge="true" folds
    them into one file WITHOUT re-running the DataFrame; keepParts="true" retains
    the source parts alongside the merged output."""
    import glob
    import os

    import netCDF4

    _register(spark)
    df = _read_vector(spark, in_nc, variables).repartition(3)
    expected = df.count()
    # 1) write sharded parts (default parts mode)
    df.write.format("netcdf_gbx").mode("overwrite").option("mode", "vector").save(
        out_dir
    )
    parts_before = sorted(glob.glob(os.path.join(out_dir, "*.nc")))
    assert parts_before, "no parts written"

    # 2) post-hoc merge; the DataFrame rows are ignored, keepParts retains parts
    (
        spark.range(1)
        .write.format("netcdf_gbx")
        .mode("append")
        .option("mode", "vector")
        .option("merge", "true")
        .option("keepParts", "true")
        .option("fileName", "merged")
        .save(out_dir)
    )

    merged = os.path.join(out_dir, "merged.nc")
    assert os.path.exists(merged), "merged output missing"
    # keepParts=true -> every source part still on disk
    for p in parts_before:
        assert os.path.exists(p), f"part {p} was deleted despite keepParts"
    with netCDF4.Dataset(merged, "r") as nc:
        assert int(nc.dimensions["obs"].size) == expected
