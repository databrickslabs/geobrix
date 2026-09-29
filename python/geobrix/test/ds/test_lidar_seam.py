import pytest
from pyspark.sql.types import DoubleType, StructField, StructType

from databricks.labs.gbx.ds._write_lidar import LidarGbxWriter, _reject_v2_options

_XYZ = StructType([StructField(c, DoubleType()) for c in ("x", "y", "z")])


def test_seam_options_graduated():
    # overlap=drop / dedupeExact / voxelSize no longer rejected outright.
    _reject_v2_options({"overlap": "drop"})
    _reject_v2_options({"dedupeExact": "true"})
    _reject_v2_options({"voxelSize": "0.5"})


def test_thinoverlap_and_bad_overlap_still_rejected():
    with pytest.raises(ValueError, match="thinOverlap"):
        _reject_v2_options({"thinOverlap": "0.5"})
    with pytest.raises(ValueError, match="overlap"):
        _reject_v2_options({"overlap": "bogus"})


def test_seam_option_requires_merge_mode():
    # A seam option in parts mode (merge not set) is a clear error.
    with pytest.raises(ValueError, match="requires .*merge"):
        LidarGbxWriter({"path": "/tmp/x", "overlap": "drop"}, _XYZ, overwrite=True)


def test_merge_mode_parses_seam_options():
    w = LidarGbxWriter(
        {
            "path": "/tmp/x",
            "merge": "true",
            "overlap": "drop",
            "dedupeExact": "true",
            "voxelSize": "0.5",
        },
        _XYZ,
        overwrite=True,
    )
    assert w.overlap == "drop" and w.dedupe_exact is True and w.voxel_size == 0.5


def _write_laz(path, xs, ys, zs):
    import laspy
    import numpy as np

    h = laspy.LasHeader(point_format=0)
    h.offsets = [min(xs), min(ys), min(zs)]
    h.scales = [0.001, 0.001, 0.001]
    d = laspy.LasData(h)
    d.x, d.y, d.z = np.array(xs), np.array(ys), np.array(zs)
    d.write(str(path))


def test_part_cluster_meta_from_names_and_headers(tmp_path):
    from databricks.labs.gbx.ds._write_lidar import _part_cluster_meta

    p0 = tmp_path / "dense_all_0.las"
    _write_laz(p0, [0, 1], [0, 1], [0, 0])
    p1 = tmp_path / "dense_all_1.las"
    _write_laz(p1, [10, 11], [0, 1], [0, 0])
    meta = _part_cluster_meta([str(p0), str(p1)])
    assert meta["per_part"][str(p0)] == "0"
    assert meta["center"]["1"][0] == pytest.approx(10.5, abs=0.01)
    assert meta["bbox"]["0"][1] == pytest.approx(1.0, abs=0.01)  # x_max


def test_part_cluster_meta_rejects_uuid_parts(tmp_path):
    from databricks.labs.gbx.ds._write_lidar import _part_cluster_meta

    p = tmp_path / "part-a1b2c3d4.las"
    _write_laz(p, [0], [0], [0])
    with pytest.raises(ValueError, match="cluster-identifiable"):
        _part_cluster_meta([str(p)])
