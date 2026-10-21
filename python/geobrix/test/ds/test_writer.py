"""Round-trip: raster_gbx read -> gtiff_gbx write -> re-read; + strict schema."""

import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource


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


def test_round_trip(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)

    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))

    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_strict_schema_rejects_extra_columns(spark, tmp_path):
    import pytest
    from pyspark.sql import functions as F

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src)).withColumn("extra", F.lit(1))
    with pytest.raises(Exception):
        df.write.format("gtiff_gbx").mode("overwrite").save(str(tmp_path / "o2"))


def test_overwrite_replaces_not_accumulates(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))
    first = {f for f in os.listdir(out_dir) if f.endswith(".tif")}
    assert len(first) == 1
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))
    second = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(second) == 1, f"overwrite accumulated stale tiles: {second}"


def test_namecol_controls_filenames(spark, tmp_path):
    from pyspark.sql import functions as F

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_named"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .load(str(src))
        .withColumn("source", F.lit("mytile"))
    )
    df.write.format("gtiff_gbx").mode("overwrite").option("nameCol", "source").save(
        str(out_dir)
    )
    files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert files == ["mytile.tif"]


def test_default_names_do_not_collide_when_tiles_share_a_cellid(spark, tmp_path):
    """Retile windows share their parent's cellid. With no nameCol the writer's
    opaque name must still differ per tile: both tiles land, each with its own
    pixels."""
    from databricks.labs.gbx.ds.raster import reader_schema

    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    _write_sample(str(first))
    with rasterio.open(str(first)) as ds:
        profile = ds.profile
        data = ds.read(1)
    with rasterio.open(str(second), "w", **profile) as ds:
        ds.write(data + 100, 1)

    out_dir = tmp_path / "out_shared_cellid"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    rows = [
        ("", (7, first.read_bytes(), {"driver": "GTiff"})),
        ("", (7, second.read_bytes(), {"driver": "GTiff"})),
    ]
    df = spark.createDataFrame(rows, schema=reader_schema()).repartition(1)
    df.write.format("gtiff_gbx").mode("overwrite").save(str(out_dir))

    files = sorted(f for f in os.listdir(out_dir) if f.endswith(".tif"))
    assert len(files) == 2, f"tiles collided on one name: {files}"
    mins = []
    for f in files:
        with rasterio.open(os.path.join(out_dir, f)) as ds:
            mins.append(float(ds.read(1).min()))
    assert sorted(mins) == [0.0, 100.0]


def test_ext_option_controls_suffix(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_ext"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("overwrite").option("ext", "tiff").save(
        str(out_dir)
    )
    assert all(f.endswith(".tiff") for f in os.listdir(out_dir))


def test_raster_gbx_catch_all_writer_round_trips(spark, tmp_path):
    import numpy as np
    import rasterio

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_catchall"
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("raster_gbx").mode("overwrite").save(str(out_dir))
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_append_mode_adds_files(spark, tmp_path):
    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_append"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    df.write.format("gtiff_gbx").mode("append").save(str(out_dir))
    df.write.format("gtiff_gbx").mode("append").save(str(out_dir))
    n = len([f for f in os.listdir(out_dir) if f.endswith(".tif")])
    assert n == 2, f"append should accumulate, got {n}"


def test_missing_column_rejected(spark, tmp_path):
    import pytest

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src)).select("tile")  # drop 'source'
    with pytest.raises(Exception):
        df.write.format("gtiff_gbx").mode("overwrite").save(str(tmp_path / "o_missing"))


def test_metadata_driver_cog_triggers_reencode(spark, tmp_path):
    # Catch-all raster_gbx writer honors tile.metadata["driver"]: overriding it to
    # COG forces a re-encode (vs verbatim GTiff). Output decodes to the same pixels
    # and carries the RASTERX_CELL tag stamped on the re-encode path.
    from pyspark.sql import functions as F

    src = tmp_path / "in.tif"
    _write_sample(str(src))
    out_dir = tmp_path / "out_cog"
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(src))
    # Replace metadata with a clean map whose driver=COG (map_concat with the
    # existing metadata would duplicate the 'driver' key -> Spark rejects it).
    df2 = df.withColumn(
        "tile",
        F.col("tile").withField(
            "metadata",
            F.create_map(
                F.lit("driver"),
                F.lit("COG"),
                F.lit("format"),
                F.lit("COG"),
                F.lit("compression"),
                F.lit("DEFLATE"),
            ),
        ),
    )
    try:
        df2.write.format("raster_gbx").mode("overwrite").save(str(out_dir))
    except Exception as exc:  # noqa: BLE001 - COG driver may be absent locally
        import pytest

        pytest.skip(f"COG driver unavailable in this env: {str(exc)[:80]}")
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        arr = ds.read(1)
        tags = ds.tags()
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )
    assert tags.get("RASTERX_CELL") == "-1"
