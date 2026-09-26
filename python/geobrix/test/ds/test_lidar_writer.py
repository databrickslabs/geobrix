"""Tests for lidar_gbx DataSource writer (sharded parts mode).

TDD: new tests written BEFORE implementation (RED -> GREEN).
Task 1 test (write_xyz_laz) is retained here; Task 2 adds the DataSource writer tests.
"""

import numpy as np
import pytest


def _decode_laz(path):
    """Read a written .las/.laz back with laspy; return (n, xs, ys, zs, fmt_id)."""
    import laspy

    las = laspy.read(path)
    return (
        len(las.points),
        np.asarray(las.x),
        np.asarray(las.y),
        np.asarray(las.z),
        int(las.header.point_format.id),
    )


def test_write_xyz_laz_roundtrips_xyz_format0(tmp_path):
    from databricks.labs.gbx.pyrx.imagery import write_xyz_laz

    x = np.array([0.0, 1.0, 2.5], dtype=np.float64)
    y = np.array([10.0, 11.0, 12.0], dtype=np.float64)
    z = np.array([100.0, 101.0, 102.0], dtype=np.float64)
    out = write_xyz_laz(str(tmp_path / "xyz.laz"), x, y, z, crs=32611)

    n, xs, ys, zs, fmt = _decode_laz(out)
    assert n == 3
    assert fmt == 0  # XYZ-only point format
    np.testing.assert_allclose(sorted(xs), sorted(x), atol=1e-4)
    np.testing.assert_allclose(sorted(zs), sorted(z), atol=1e-4)


def _points_df(spark, n=30, with_rgb=True, groups=("A", "B")):
    """A points DataFrame with x,y,z[,r,g,b] + group,cluster columns."""
    from pyspark.sql import Row

    rng = np.random.default_rng(3)
    rows = []
    for i in range(n):
        grp = groups[i % len(groups)]
        clu = i % 2
        base = dict(
            x=float(rng.uniform(0, 100)),
            y=float(rng.uniform(0, 100)),
            z=float(rng.uniform(0, 50)),
            group=grp,
            cluster=int(clu),
        )
        if with_rgb:
            base.update(
                r=int(rng.integers(0, 256)),
                g=int(rng.integers(0, 256)),
                b=int(rng.integers(0, 256)),
            )
        rows.append(Row(**base))
    return spark.createDataFrame(rows)


def test_writer_sharded_roundtrips_via_reader(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    out = str(tmp_path / "cloud")
    df = _points_df(spark, n=40, with_rgb=True)
    (
        df.repartition(4, "group", "cluster")
        .write.format("lidar_gbx")
        .option("groupCol", "group")
        .option("clusterCol", "cluster")
        .mode("overwrite")
        .save(out)
    )
    # Deterministic names: *_<group>_<cluster>.laz, no uuid parts.
    import glob
    import os

    parts = sorted(os.path.basename(p) for p in glob.glob(os.path.join(out, "*.la*")))
    assert parts, "no part files written"
    assert all("_" in p and "part-" not in p for p in parts)
    # Round-trip point count via the reader (RGB not in reader schema; xyz+count is).
    back = spark.read.format("lidar_gbx").option("mode", "points").load(out)
    assert back.count() == 40


def test_writer_rejects_v1_unsupported_columns(spark, tmp_path):
    from pyspark.sql import Row

    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    df = spark.createDataFrame([Row(x=1.0, y=2.0, z=3.0, classification=2)])
    with pytest.raises(Exception) as ei:
        df.write.format("lidar_gbx").mode("overwrite").save(str(tmp_path / "c"))
    assert "classification" in str(ei.value) and "v1" in str(ei.value).lower()
