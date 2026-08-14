"""Tests for plan/read split instrumentation in run_spark_path_reader."""

import numpy as np
import rasterio
from rasterio.transform import from_origin


def _write_sample(path, width=64, height=64, epsg=4326):
    data = np.zeros((height, width), dtype="float32")
    profile = dict(
        driver="GTiff", width=width, height=height, count=1, dtype="float32",
        crs=f"EPSG:{epsg}", transform=from_origin(10.0, 50.0, 0.01, 0.01), nodata=-9999.0,
    )
    with rasterio.open(str(path), "w", **profile) as ds:
        ds.write(data, 1)


def test_run_spark_path_reader_plan_s_populated(tmp_path, spark):
    """split_plan_read=True → emitted ResultRow has plan_s > 0."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource
    from databricks.labs.gbx.bench.readers import run_spark_path_reader

    for i in range(3):
        _write_sample(tmp_path / f"r{i}.tif")

    spark.dataSource.register(RasterGbxDataSource)

    rows = run_spark_path_reader(
        spark,
        path=str(tmp_path),
        run_id="test",
        warmup=0,
        measured=1,
        size_mib=-1,
        where="venv",
        split_plan_read=True,
    )

    assert len(rows) >= 1
    ok_rows = [r for r in rows if r.status == "ok"]
    assert ok_rows, f"Expected at least one ok row; got: {rows}"
    assert ok_rows[0].plan_s >= 0.0, "plan_s must be non-negative"
    # plan_s should be populated when split_plan_read=True
    assert ok_rows[0].plan_s > 0.0, (
        "plan_s should be > 0 when split_plan_read=True (planning is not instant)"
    )


def test_run_spark_path_reader_plan_s_default_zero(tmp_path, spark):
    """split_plan_read=False (default) → plan_s is 0.0."""
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource
    from databricks.labs.gbx.bench.readers import run_spark_path_reader

    _write_sample(tmp_path / "r.tif")
    spark.dataSource.register(RasterGbxDataSource)

    rows = run_spark_path_reader(
        spark,
        path=str(tmp_path),
        run_id="test",
        warmup=0,
        measured=1,
        size_mib=-1,
        where="venv",
    )

    ok_rows = [r for r in rows if r.status == "ok"]
    assert ok_rows
    assert ok_rows[0].plan_s == 0.0


def test_result_row_plan_s_default():
    """ResultRow can be constructed without plan_s (backward compat)."""
    from databricks.labs.gbx.bench.results import ResultRow

    r = ResultRow(
        run_id="x", api="lightweight", fn="f", category="c", mode="spark-path",
        tile_px=0, bands=0, dtype="", srid=0, rows=1, nodata_frac=0.0,
        warmup_iters=0, measured_iters=1, iter_median_s=1.0, iter_min_s=1.0,
        iter_p90_s=1.0, throughput_mpix_s=0.0, throughput_rows_s=1.0,
        peak_rss_mb=0.0, status="ok", note="",
        env_arch="x86_64", env_cpu_model="test", env_cpu_count=1,
        env_os="linux", env_gbx_version="0.5.0", env_gdal_version="3.9",
        env_runtime_version="3.12", env_where="venv",
    )
    assert r.plan_s == 0.0  # default
