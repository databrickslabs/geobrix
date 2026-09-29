import pytest
from pyspark.sql.types import DoubleType, StructField, StructType

from databricks.labs.gbx.ds._write_lidar import (
    LidarGbxWriter,
    _part_cluster_meta,
    _reject_v2_options,
)

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
    p0 = tmp_path / "dense_all_0.las"
    _write_laz(p0, [0, 1], [0, 1], [0, 0])
    p1 = tmp_path / "dense_all_1.las"
    _write_laz(p1, [10, 11], [0, 1], [0, 0])
    meta = _part_cluster_meta([str(p0), str(p1)])
    assert meta["per_part"][str(p0)] == "0"
    assert meta["center"]["1"][0] == pytest.approx(10.5, abs=0.01)
    assert meta["bbox"]["0"][1] == pytest.approx(1.0, abs=0.01)  # x_max


def test_part_cluster_meta_unions_bbox_across_shared_cluster_id(tmp_path):
    # Two parts resolve to the SAME cluster id ("0") but cover disjoint x
    # ranges — the bbox/center for that cluster must union across both parts
    # rather than reflecting only the last-seen part.
    pa = tmp_path / "a_all_0.las"
    _write_laz(pa, [0, 1], [0, 1], [0, 0])
    pb = tmp_path / "b_all_0.las"
    _write_laz(pb, [10, 11], [0, 1], [0, 0])
    meta = _part_cluster_meta([str(pa), str(pb)])
    assert meta["per_part"][str(pa)] == "0"
    assert meta["per_part"][str(pb)] == "0"
    xmin, xmax, _, _ = meta["bbox"]["0"]
    assert xmin == pytest.approx(0.0, abs=0.01)
    assert xmax == pytest.approx(11.0, abs=0.01)
    assert meta["center"]["0"][0] == pytest.approx(5.5, abs=0.01)


def test_part_cluster_meta_rejects_uuid_parts(tmp_path):
    p = tmp_path / "part-a1b2c3d4.las"
    _write_laz(p, [0], [0], [0])
    with pytest.raises(ValueError, match="cluster-identifiable"):
        _part_cluster_meta([str(p)])


def test_part_cluster_meta_rejects_no_underscore_token(tmp_path):
    p = tmp_path / "nogroup.las"
    _write_laz(p, [0], [0], [0])
    with pytest.raises(ValueError, match="cluster-identifiable"):
        _part_cluster_meta([str(p)])
