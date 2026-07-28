"""Unit test: writer bench times a light write and emits a ResultRow."""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.bench import readers


def _write_sample(path):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_run_format_write_light(spark, tmp_path):
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    for i in range(2):
        _write_sample(str(src_dir / f"s{i}.tif"))
    out = str(tmp_path / "out")
    r = readers.run_format_write(
        spark,
        str(src_dir),
        out,
        "t",
        warmup=1,
        measured=2,
        write_api="lightweight",
        read_fmt="raster_gbx",
        write_fmt="gtiff_gbx",
        options={"filterRegex": r".*\.tif$"},
        where="venv",
    )
    assert r.api == "lightweight"
    assert r.fn == "raster_write"
    assert r.category == "writer"
    assert r.mode == "spark-path"
    assert r.rows == 2
    assert r.status == "ok"
    assert r.iter_median_s >= 0.0
    # Default (empty) label leaves the note as the canonical "<fmt> write of N tiles".
    assert r.note == "gtiff_gbx write of 2 tiles"


def test_run_format_write_label_appends_to_note(spark, tmp_path):
    """A non-empty label is appended to the note so parts vs single rows are distinct."""
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    for i in range(2):
        _write_sample(str(src_dir / f"s{i}.tif"))
    out = str(tmp_path / "out")
    r = readers.run_format_write(
        spark,
        str(src_dir),
        out,
        "t",
        warmup=1,
        measured=2,
        write_api="lightweight",
        read_fmt="raster_gbx",
        write_fmt="gtiff_gbx",
        options={"filterRegex": r".*\.tif$"},
        label="singleFile",
        where="venv",
    )
    assert r.status == "ok"
    assert r.note == "gtiff_gbx write of 2 tiles [singleFile]"


def _write_grid_nc(path, var):
    """Stage a minimal CF regular-grid .nc with a single data var on a shared grid."""
    from netCDF4 import Dataset

    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lat.standard_name = "latitude"
        lon = ds.createVariable("lon", "f8", ("lon",))
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        v = ds.createVariable(var, "f4", ("lat", "lon"), fill_value=-9999.0)
        v[:] = np.arange(12, dtype="float32").reshape(3, 4)


def test_run_format_write_merge_netcdf(spark, tmp_path):
    """merge mode pre-populates parts (untimed) then times a post-hoc dir merge.

    Two distinct data vars on the SAME grid -> the reader emits two (source, tile)
    rows -> the untimed setup write drops two .nc parts into out_path -> the timed
    ``merge=true keepParts=true`` job folds them into ONE merged .nc while retaining
    the parts (so warmup+measured iterations stay idempotent). Real netcdf_gbx merge.
    """
    import os

    src_dir = tmp_path / "in"
    src_dir.mkdir()
    # Distinct variable names sharing one grid -> mergeable (not duplicate vars).
    _write_grid_nc(str(src_dir / "tas.nc"), "tas")
    _write_grid_nc(str(src_dir / "pr.nc"), "pr")
    out = str(tmp_path / "out")
    r = readers.run_format_write(
        spark,
        str(src_dir),
        out,
        "t",
        warmup=1,
        measured=2,
        write_api="lightweight",
        read_fmt="netcdf_gbx",
        write_fmt="netcdf_gbx",
        options={
            "filterRegex": r".*\.nc$",
            "merge": "true",
            "keepParts": "true",
        },
        label="merge",
        where="venv",
    )
    assert r.status == "ok", r.note
    assert r.rows == 2  # two distinct-var grid rows read from the corpus
    assert r.note == "netcdf_gbx write of 2 tiles [merge]"
    # keepParts=true -> the two source parts survive AND a single merged .nc exists.
    ncs = sorted(f for f in os.listdir(out) if f.endswith(".nc"))
    assert "tas.nc" in ncs and "pr.nc" in ncs  # parts retained across iterations
    merged = os.path.basename(out) + ".nc"
    assert merged in ncs, f"expected merged {merged!r} in {ncs}"


def _stage_vector_nc_corpus(spark, src_dir, n=6, parts=3):
    """Stage a CF-DSG point .nc corpus (>=1 .nc part) via the vector writer, so the
    bench merge leg has real swath-shaped parts to read back with mode=vector."""
    import shapely
    from pyspark.sql.types import (
        BinaryType,
        FloatType,
        StringType,
        StructField,
        StructType,
    )

    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource

    spark.dataSource.register(NetcdfGbxDataSource)
    schema = StructType(
        [
            StructField("methane_mixing_ratio_bias_corrected", FloatType(), True),
            StructField("qa_value", FloatType(), True),
            StructField("geom_0", BinaryType(), True),
            StructField("geom_0_srid", StringType(), True),
            StructField("geom_0_srid_proj", StringType(), True),
        ]
    )
    pts = [
        (
            float(i),
            float(i % 2),
            bytes(shapely.to_wkb(shapely.Point(10.0 + i * 0.1, 50.0 + i * 0.1))),
            "4326",
            "EPSG:4326",
        )
        for i in range(n)
    ]
    df = spark.createDataFrame(pts, schema).repartition(parts)
    df.write.format("netcdf_gbx").option("mode", "vector").mode("overwrite").save(
        str(src_dir)
    )


def test_run_format_write_merge_netcdf_vector(spark, tmp_path):
    """VECTOR merge leg (mirrors the cluster swath leg that errored).

    The single options dict flows mode=vector to BOTH the read and the untimed
    setup-parts write; the seed must land CF-DSG .nc parts so the timed merge has
    files to fold (regression for 'no .nc files to merge under .../netcdf-swath-out-merge').
    """
    import os

    src_dir = tmp_path / "vin"
    _stage_vector_nc_corpus(spark, src_dir, n=6, parts=3)
    out = str(tmp_path / "vout")
    r = readers.run_format_write(
        spark,
        str(src_dir),
        out,
        "t",
        warmup=1,
        measured=2,
        write_api="lightweight",
        read_fmt="netcdf_gbx",
        write_fmt="netcdf_gbx",
        options={
            "mode": "vector",
            "variables": "methane_mixing_ratio_bias_corrected,qa_value",
            "filterRegex": r".*\.nc$",
            "merge": "true",
            "keepParts": "true",
        },
        label="merge",
        where="venv",
    )
    assert r.status == "ok", r.note
    assert r.rows == 6  # six point rows read from the CF-DSG corpus
    assert r.note == "netcdf_gbx write of 6 tiles [merge]"
    # keepParts=true -> a single merged .nc exists alongside the retained parts.
    merged = os.path.basename(out) + ".nc"
    ncs = sorted(f for f in os.listdir(out) if f.endswith(".nc"))
    assert merged in ncs, f"expected merged {merged!r} in {ncs}"
    assert len(ncs) >= 2, f"parts must survive keepParts=true, got {ncs}"
