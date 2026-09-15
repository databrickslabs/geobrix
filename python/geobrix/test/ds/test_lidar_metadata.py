"""Tests for lidar_gbx DataSource — metadata mode (header-only, one row per file).

Task 2 of the v0.5.2 LiDAR feature: LAS/LAZ header scanning without reading points.
"""

import numpy as np


def _write_tiny_las(tmp_path, n=100, crs_epsg=None):
    """Write a minimal LAS 1.4 file with *n* random points; return its path string."""
    import laspy

    hdr = laspy.LasHeader(point_format=3, version="1.4")
    hdr.offsets = [0.0, 0.0, 0.0]
    hdr.scales = [0.01, 0.01, 0.01]
    las = laspy.LasData(hdr)
    rng = np.random.default_rng(0)
    las.x = rng.uniform(0, 100, n)
    las.y = rng.uniform(0, 100, n)
    las.z = rng.uniform(0, 50, n)
    p = tmp_path / "tiny.las"
    las.write(str(p))
    return str(p)


def test_metadata_header_fields(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    path = _write_tiny_las(tmp_path, n=100)
    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass  # already registered in this session

    df = spark.read.format("lidar_gbx").option("mode", "metadata").load(path)
    row = df.collect()[0]

    assert row["point_count"] == 100
    assert row["name"] == "tiny.las"
    assert 0.0 <= row["x_min"] <= row["x_max"] <= 100.0
    assert row["z_max"] <= 50.0
    assert row["point_format"] == 3
    assert "X" in row["dimensions"] or "x" in row["dimensions"]
    assert row["size"] > 0
    # Schema completeness: all LIDAR_META_SCHEMA fields are present
    for field in (
        "path",
        "name",
        "point_count",
        "x_min",
        "x_max",
        "y_min",
        "y_max",
        "z_min",
        "z_max",
        "crs",
        "point_format",
        "dimensions",
        "scale",
        "offset",
        "return_histogram",
        "density",
        "version",
        "size",
        "modificationTime",
    ):
        assert field in row.asDict(), f"Missing field: {field}"


def test_metadata_fuse_path_resolution(spark, tmp_path):
    """Loading via a file:-scheme URI must strip to bare path and still succeed."""
    from databricks.labs.gbx.ds._listing import to_spark_uri
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    path = _write_tiny_las(tmp_path, n=5)
    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass

    # Pass file:-prefixed URI — to_local_path should strip it before open().
    file_uri = "file:" + path
    df = spark.read.format("lidar_gbx").option("mode", "metadata").load(file_uri)
    row = df.collect()[0]

    assert row["point_count"] == 5
    assert row["name"] == "tiny.las"
    # The path column is to_spark_uri of the bare resolved path.
    # For a plain /tmp/... path, to_spark_uri is a no-op → same as the bare path.
    assert row["path"] == to_spark_uri(path)


def test_reader_source_is_serverless_safe():
    """No forbidden Spark APIs (sparkContext, _jvm, .rdd, _jsc) in lidar.py."""
    import inspect

    from databricks.labs.gbx.ds import lidar

    src = inspect.getsource(lidar)
    for forbidden in ("sparkContext", "_jvm", ".rdd", "_jsc"):
        assert forbidden not in src, f"forbidden API {forbidden!r} found in lidar.py"
