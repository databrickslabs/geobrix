import numpy as np
import rasterio
from pyspark.sql import functions as F

from databricks.labs.gbx.pyrx import functions as rx


def _pixels(row):
    # rst_binpoints_agg / bin_points_tiled emit the v2 tile struct; the GTiff bytes
    # live in its "raster" field (see pyrx/_serde.py TILE schema).
    raw = row["tile"]["raster"]
    with rasterio.MemoryFile(bytes(raw)) as mf, mf.open() as ds:
        return ds.read(1)


def _mk_df(spark):
    rows = [
        (0.5, 0.5, 10.0),
        (0.6, 0.6, 20.0),
        (1.5, 0.5, 5.0),
        (0.5, 1.5, 7.0),
        (1.5, 1.5, 9.0),
        (1.6, 1.6, 3.0),
    ]
    df = spark.createDataFrame(rows, "x double, y double, z double")
    return (
        df.withColumn("tk", F.lit(1))
        .withColumn("xmin", F.lit(0.0))
        .withColumn("ymin", F.lit(0.0))
        .withColumn("xmax", F.lit(2.0))
        .withColumn("ymax", F.lit(2.0))
    )


def test_bin_points_tiled_parity(spark):
    rx.register(spark)
    df = _mk_df(spark)
    for s in ("max", "min", "mean", "count"):
        got = _pixels(
            rx.bin_points_tiled(
                df,
                x="x",
                y="y",
                z="z",
                by=["tk"],
                xmin="xmin",
                ymin="ymin",
                xmax="xmax",
                ymax="ymax",
                width=2,
                height=2,
                srid=2227,
                stat=s,
            ).first()
        )
        ref = _pixels(
            df.groupBy("tk")
            .agg(
                rx.rst_binpoints_agg(
                    "x",
                    "y",
                    "z",
                    "xmin",
                    "ymin",
                    "xmax",
                    "ymax",
                    F.lit(2),
                    F.lit(2),
                    F.lit(2227),
                    s,
                ).alias("tile")
            )
            .first()
        )
        np.testing.assert_allclose(
            np.nan_to_num(got, nan=-9999.0),
            np.nan_to_num(ref, nan=-9999.0),
            rtol=1e-6,
            err_msg=f"stat={s}",
        )


def test_bin_points_tiled_multi_tile(spark):
    rx.register(spark)
    rows = [
        (0.5, 0.5, 10.0, 1, 0.0, 0.0, 2.0, 2.0),
        (1.5, 1.5, 4.0, 1, 0.0, 0.0, 2.0, 2.0),
        (5.5, 5.5, 8.0, 2, 4.0, 4.0, 6.0, 6.0),
    ]
    df = spark.createDataFrame(
        rows,
        "x double,y double,z double,tk int,xmin double,ymin double,xmax double,ymax double",
    )
    out = rx.bin_points_tiled(
        df,
        x="x",
        y="y",
        z="z",
        by=["tk"],
        xmin="xmin",
        ymin="ymin",
        xmax="xmax",
        ymax="ymax",
        width=2,
        height=2,
        srid=2227,
        stat="max",
    ).collect()
    assert {r["tk"] for r in out} == {1, 2}


def test_bin_points_tiled_empty(spark):
    rx.register(spark)
    df = _mk_df(spark).where(F.col("x") > 1000.0)  # no rows
    assert (
        rx.bin_points_tiled(
            df,
            x="x",
            y="y",
            z="z",
            by=["tk"],
            xmin="xmin",
            ymin="ymin",
            xmax="xmax",
            ymax="ymax",
            width=2,
            height=2,
            srid=2227,
            stat="max",
        ).count()
        == 0
    )


def test_bin_points_tiled_median_delegates(spark):
    rx.register(spark)
    df = _mk_df(spark)
    got = _pixels(
        rx.bin_points_tiled(
            df,
            x="x",
            y="y",
            z="z",
            by=["tk"],
            xmin="xmin",
            ymin="ymin",
            xmax="xmax",
            ymax="ymax",
            width=2,
            height=2,
            srid=2227,
            stat="median",
        ).first()
    )
    ref = _pixels(
        df.groupBy("tk")
        .agg(
            rx.rst_binpoints_agg(
                "x",
                "y",
                "z",
                "xmin",
                "ymin",
                "xmax",
                "ymax",
                F.lit(2),
                F.lit(2),
                F.lit(2227),
                "median",
            ).alias("tile")
        )
        .first()
    )
    np.testing.assert_allclose(
        np.nan_to_num(got, nan=-9999.0), np.nan_to_num(ref, nan=-9999.0), rtol=1e-6
    )
