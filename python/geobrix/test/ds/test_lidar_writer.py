"""Tests for lidar_gbx DataSource writer (sharded parts mode).

TDD: new tests written BEFORE implementation (RED -> GREEN).
Task 1 test (write_xyz_laz) is retained here; Task 2 adds the DataSource writer tests.
Task 3 adds singleFile + merge mode tests (M1/M2/M3).
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
    """A points DataFrame with x,y,z[,r,g,b] + group,cluster columns.

    Uses clu = (i // 2) % 2 so that both cluster values (0 and 1) appear with
    each group, producing all 4 (group, cluster) combinations when len(groups)==2.
    This ensures repartitionByRange(4, 'group', 'cluster') places each combination
    in its own partition (M3 guard: catches stem-collision naming bugs).
    """
    from pyspark.sql import Row

    rng = np.random.default_rng(3)
    rows = []
    for i in range(n):
        grp = groups[i % len(groups)]
        clu = (i // 2) % 2  # 0,0,1,1,0,0,... — all 4 (grp,clu) combos when 2 groups
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
    # repartitionByRange ensures each (group, cluster) lands in its own partition,
    # giving exactly 4 output files — required for the M3 naming-collision guard.
    (
        df.repartitionByRange(4, "group", "cluster")
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
    # M3: assert all 4 (group × cluster) combinations produce a distinct file.
    assert (
        len(parts) == 4
    ), f"expected 4 part files (A_0, A_1, B_0, B_1) but got {len(parts)}: {parts}"
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


# ---------------------------------------------------------------------------
# M2: partial-RGB rejection
# ---------------------------------------------------------------------------


def test_writer_rejects_partial_rgb(spark, tmp_path):
    """A DataFrame with x,y,z,r (no g,b) must raise ValueError about RGB."""
    from pyspark.sql import Row

    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    df = spark.createDataFrame([Row(x=1.0, y=2.0, z=3.0, r=128)])
    with pytest.raises(Exception) as ei:
        df.write.format("lidar_gbx").mode("overwrite").save(
            str(tmp_path / "rgb_partial")
        )
    msg = str(ei.value).lower()
    assert "rgb" in msg or "r,g,b" in msg or ("r" in msg and "g" in msg and "b" in msg)


# ---------------------------------------------------------------------------
# Task 3: singleFile mode
# ---------------------------------------------------------------------------


def test_writer_single_file_merges_to_one(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    out = str(tmp_path / "single")
    df = _points_df(spark, n=50, with_rgb=True)
    (
        df.repartition(4, "group", "cluster")
        .write.format("lidar_gbx")
        .option("singleFile", "true")
        .option("fileName", "merged")
        .option("groupCol", "group")
        .option("clusterCol", "cluster")
        .mode("overwrite")
        .save(out)
    )
    import glob
    import os

    files = glob.glob(os.path.join(out, "merged.la*"))
    assert len(files) == 1
    back = spark.read.format("lidar_gbx").option("mode", "points").load(files[0])
    assert back.count() == 50


# ---------------------------------------------------------------------------
# Task 3: post-hoc merge mode
# ---------------------------------------------------------------------------


def test_writer_merge_folds_parts_and_deletes(spark, tmp_path):
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    out = str(tmp_path / "m")
    df = _points_df(spark, n=30, with_rgb=True)
    df.repartition(3, "group", "cluster").write.format("lidar_gbx").option(
        "groupCol", "group"
    ).option("clusterCol", "cluster").mode("overwrite").save(out)
    import glob
    import os

    n_parts = len(glob.glob(os.path.join(out, "*.la[sz]")))
    assert n_parts >= 2
    # Post-hoc merge (no recompute); default deletes parts.
    (
        spark.createDataFrame([(1,)], ["dummy"])
        .write.format("lidar_gbx")
        .option("merge", "true")
        .option("fileName", "all")
        .mode("append")
        .save(out)
    )
    merged = glob.glob(os.path.join(out, "all.la*"))
    assert len(merged) == 1
    back = spark.read.format("lidar_gbx").option("mode", "points").load(merged[0])
    assert back.count() == 30


# ---------------------------------------------------------------------------
# Task 4: _merge_budget_bytes unit tests (monkeypatching gpu_infra)
# ---------------------------------------------------------------------------

_MID_CLOUD_BYTES = 1 * 1024**3  # 1 GiB — representative "mid" cloud for gate assertions


def test_merge_budget_bytes_override_wins(monkeypatch):
    """mergeMaxMB (or GBX_LIDAR_MERGE_MAX_MB) takes priority over any RAM probe."""
    from databricks.labs.gbx.ds._write_lidar import _merge_budget_bytes

    # Patch gpu_infra to ensure the probe is never reached when override is set.
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.mvs.gpu_infra",
        lambda: (_ for _ in ()).throw(RuntimeError("should not be called")),
    )
    result = _merge_budget_bytes("100")
    assert result == int(100 * 1024**2)


def test_merge_budget_bytes_high_ram_gpu_large_budget(monkeypatch):
    """High-RAM AIR/GPU node → budget >> 1 GiB mid cloud (merge passes)."""
    from databricks.labs.gbx.ds._write_lidar import (
        _MERGE_FALLBACK_BYTES,
        _merge_budget_bytes,
    )

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.mvs.gpu_infra",
        lambda: {
            "host_ram_available_mb": 64 * 1024,
            "host_ram_mb": 64 * 1024,
            "gpu_count": 2,
            "per_gpu_vram_mb": 24576,
            "cores": 32,
        },
    )
    budget = _merge_budget_bytes(None)
    assert budget > _MERGE_FALLBACK_BYTES
    # A mid cloud (1 GiB) should be well within the budget on a 64 GiB GPU node.
    assert budget > _MID_CLOUD_BYTES


def test_merge_budget_bytes_tiny_ram_fallback_floor(monkeypatch):
    """RAM below reserve → budget collapses to fallback floor (< 1 GiB mid cloud → raises)."""
    from databricks.labs.gbx.ds._write_lidar import (
        _MERGE_FALLBACK_BYTES,
        _merge_budget_bytes,
    )

    # 1 GiB available RAM is below the 2 GiB non-GPU reserve → usable = 0 → floor.
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.mvs.gpu_infra",
        lambda: {
            "host_ram_available_mb": 1024,
            "host_ram_mb": 1024,
            "gpu_count": 0,
            "per_gpu_vram_mb": 0,
            "cores": 4,
        },
    )
    budget = _merge_budget_bytes(None)
    assert budget == _MERGE_FALLBACK_BYTES
    # The fallback floor is < 1 GiB, so a 1 GiB mid cloud would raise the gate.
    assert budget < _MID_CLOUD_BYTES


def test_merge_budget_bytes_probe_failure_returns_fallback(monkeypatch):
    """Any exception from gpu_infra → conservative fallback floor returned."""
    from databricks.labs.gbx.ds._write_lidar import (
        _MERGE_FALLBACK_BYTES,
        _merge_budget_bytes,
    )

    def _raise():
        raise RuntimeError("nvidia-smi: command not found")

    monkeypatch.setattr("databricks.labs.gbx.pyrx.mvs.gpu_infra", _raise)
    budget = _merge_budget_bytes(None)
    assert budget == _MERGE_FALLBACK_BYTES


# ---------------------------------------------------------------------------
# Task 4: writer-level gate tests (mergeMaxMB option)
# ---------------------------------------------------------------------------


def test_writer_merge_over_budget_raises(spark, tmp_path):
    """mergeMaxMB=0 forces budget to 0; any real cloud raises with a compute-aware message."""
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    out = str(tmp_path / "over")
    # Write a few-point xyz-only LAZ file first (no naming columns needed).
    rng = np.random.default_rng(7)
    rows = [
        {
            "x": float(rng.uniform(0, 100)),
            "y": float(rng.uniform(0, 100)),
            "z": float(rng.uniform(0, 50)),
        }
        for _ in range(30)
    ]
    df = spark.createDataFrame(rows)
    df.repartition(1).write.format("lidar_gbx").mode("overwrite").save(out)
    # Now try to merge with a zero-byte budget — must raise with the right message.
    with pytest.raises(Exception) as ei:
        (
            spark.createDataFrame([(1,)], ["_"])
            .write.format("lidar_gbx")
            .option("merge", "true")
            .option("mergeMaxMB", "0")
            .option("fileName", "merged")
            .mode("append")
            .save(out)
        )
    msg = str(ei.value).lower()
    assert "budget" in msg
    assert "compute-aware" in msg or "host-ram" in msg or "mergemaxmb" in msg.lower()


def test_writer_merge_under_budget_succeeds(spark, tmp_path):
    """mergeMaxMB=10000 (10 GB) is plenty; a small cloud merges successfully."""
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    out = str(tmp_path / "under")
    rng = np.random.default_rng(8)
    rows = [
        {
            "x": float(rng.uniform(0, 100)),
            "y": float(rng.uniform(0, 100)),
            "z": float(rng.uniform(0, 50)),
            "r": int(rng.integers(0, 256)),
            "g": int(rng.integers(0, 256)),
            "b": int(rng.integers(0, 256)),
        }
        for _ in range(30)
    ]
    df = spark.createDataFrame(rows)
    df.repartition(2).write.format("lidar_gbx").mode("overwrite").save(out)
    (
        spark.createDataFrame([(1,)], ["_"])
        .write.format("lidar_gbx")
        .option("merge", "true")
        .option("mergeMaxMB", "10000")
        .option("fileName", "merged_ok")
        .mode("append")
        .save(out)
    )
    import glob as _glob
    import os as _os

    merged = _glob.glob(_os.path.join(out, "merged_ok.la*"))
    assert len(merged) == 1
    back = spark.read.format("lidar_gbx").option("mode", "points").load(merged[0])
    assert back.count() == 30


def test_writer_singlefile_over_budget_raises(spark, tmp_path):
    """singleFile + mergeMaxMB=0 → _gate_merge raises with a compute-aware message."""
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    out = str(tmp_path / "sf_over")
    rng = np.random.default_rng(9)
    rows = [
        {
            "x": float(rng.uniform(0, 100)),
            "y": float(rng.uniform(0, 100)),
            "z": float(rng.uniform(0, 50)),
        }
        for _ in range(30)
    ]
    df = spark.createDataFrame(rows)
    with pytest.raises(Exception) as ei:
        (
            df.repartition(2)
            .write.format("lidar_gbx")
            .option("singleFile", "true")
            .option("mergeMaxMB", "0")
            .option("fileName", "sf_merged")
            .mode("overwrite")
            .save(out)
        )
    msg = str(ei.value).lower()
    assert "budget" in msg
    assert "compute-aware" in msg or "host-ram" in msg or "mergemaxmb" in msg.lower()
