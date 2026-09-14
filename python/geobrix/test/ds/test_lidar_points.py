"""Tests for lidar_gbx DataSource — points mode (chunk-iterated, filters).

Task 3 of the v0.5.2 LiDAR feature: one row per point with optional
class/return/decimate filters and .las/.laz parity.
"""
import numpy as np
import pytest

from .test_lidar_metadata import _write_tiny_las


def _write_classified_las(tmp_path, classes=(2, 6, 9), n_each=20):
    """Write a LAS file with *n_each* points per class; return path string."""
    import laspy

    n = len(classes) * n_each
    hdr = laspy.LasHeader(point_format=3, version="1.4")
    hdr.offsets = [0.0, 0.0, 0.0]
    hdr.scales = [0.01, 0.01, 0.01]
    las = laspy.LasData(hdr)
    rng = np.random.default_rng(7)
    las.x = rng.uniform(0, 100, n)
    las.y = rng.uniform(0, 100, n)
    las.z = rng.uniform(0, 50, n)
    las.classification = np.array(
        [c for c in classes for _ in range(n_each)], dtype=np.uint8
    )
    # half of each class = first return (1), other half = second return (2)
    half = n_each // 2
    las.return_number = np.array(
        [r for _ in classes for r in ([1] * half + [2] * half)], dtype=np.uint8
    )
    las.number_of_returns = np.full(n, 2, dtype=np.uint8)
    las.gps_time = rng.uniform(0, 1e9, n)
    p = tmp_path / "classified.las"
    las.write(str(p))
    return str(p)


def test_points_count_matches_header(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    path = _write_tiny_las(tmp_path, n=250)
    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass  # already registered in this session
    df = spark.read.format("lidar_gbx").option("mode", "points").load(path)
    assert df.count() == 250
    cols = set(df.columns)
    assert {"x", "y", "z", "classification", "return_number"} <= cols


def test_points_decimate(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    path = _write_tiny_las(tmp_path, n=100)
    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    df = (
        spark.read.format("lidar_gbx")
        .option("mode", "points")
        .option("decimate", "10")
        .load(path)
    )
    assert df.count() == 10  # every 10th of 100


def test_points_class_filter(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    # 3 classes × 20 pts = 60 total; filter to class 2 (20 pts) and 6 (20 pts) = 40
    path = _write_classified_las(tmp_path, classes=(2, 6, 9), n_each=20)
    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    df = (
        spark.read.format("lidar_gbx")
        .option("mode", "points")
        .option("classFilter", "2,6")
        .load(path)
    )
    assert df.count() == 40
    cls_vals = {r["classification"] for r in df.select("classification").collect()}
    assert cls_vals == {2, 6}


def test_points_return_filter(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    # 3 classes × 20 pts, half first-return = 30 first-return total
    path = _write_classified_las(tmp_path, classes=(2, 6, 9), n_each=20)
    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    df = (
        spark.read.format("lidar_gbx")
        .option("mode", "points")
        .option("returnFilter", "1")
        .load(path)
    )
    assert df.count() == 30
    rn_vals = {r["return_number"] for r in df.select("return_number").collect()}
    assert rn_vals == {1}


def test_points_laz_parity(spark, tmp_path):
    """Reading .las and .laz of the same points must yield identical counts and values."""
    import laspy

    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass

    n = 80
    hdr = laspy.LasHeader(point_format=3, version="1.4")
    hdr.offsets = [0.0, 0.0, 0.0]
    hdr.scales = [0.001, 0.001, 0.001]
    las = laspy.LasData(hdr)
    rng = np.random.default_rng(99)
    las.x = rng.uniform(10, 90, n)
    las.y = rng.uniform(10, 90, n)
    las.z = rng.uniform(0, 30, n)
    las.classification = np.array([2] * 40 + [6] * 40, dtype=np.uint8)
    las.return_number = np.ones(n, dtype=np.uint8)
    las.number_of_returns = np.ones(n, dtype=np.uint8)
    las.gps_time = rng.uniform(0, 1e9, n)

    las_path = str(tmp_path / "parity.las")
    laz_path = str(tmp_path / "parity.laz")
    las.write(las_path)
    las.write(laz_path)  # laspy writes .laz via lazrs backend

    df_las = spark.read.format("lidar_gbx").option("mode", "points").load(las_path)
    df_laz = spark.read.format("lidar_gbx").option("mode", "points").load(laz_path)

    assert df_las.count() == n
    assert df_laz.count() == n

    # Collect and compare x values (rounded to mm precision)
    xs_las = sorted(r["x"] for r in df_las.select("x").collect())
    xs_laz = sorted(r["x"] for r in df_laz.select("x").collect())
    assert xs_las == xs_laz, "x values differ between .las and .laz reads"

    cls_las = sorted(r["classification"] for r in df_las.select("classification").collect())
    cls_laz = sorted(r["classification"] for r in df_laz.select("classification").collect())
    assert cls_las == cls_laz, "classification values differ between .las and .laz reads"
