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


def _write_laz_rgb(path, xs, ys, zs, rgb=None):
    import laspy
    import numpy as np

    fmt = 2 if rgb is not None else 0
    h = laspy.LasHeader(point_format=fmt)
    h.offsets = [min(xs), min(ys), min(zs)]
    h.scales = [0.001, 0.001, 0.001]
    d = laspy.LasData(h)
    d.x, d.y, d.z = np.array(xs, float), np.array(ys, float), np.array(zs, float)
    if rgb is not None:
        r, g, b = rgb
        d.red = np.array(r, np.uint16) << 8
        d.green = np.array(g, np.uint16) << 8
        d.blue = np.array(b, np.uint16) << 8
    d.write(str(path))


def test_overlap_drop_owns_seam_once_and_keeps_interior(tmp_path):
    import laspy

    from databricks.labs.gbx.ds._write_lidar import _merge_laz_parts_seam

    # cluster 0: x in [0,10]; cluster 1: x in [8,18]; seam strip x in [8,10].
    # A seam point at x=9 is in both parts (both bboxes) -> kept once (nearest center).
    # An interior point x=1 (only cluster 0) survives. A point x=9.9 only in cluster 0
    # but nearer cluster 1's center (13) yet OUTSIDE cluster 1's data — still in c1 bbox
    # [8,18], so c1 owns it and it's dropped from c0's copy; provide it in c1 too.
    c0 = tmp_path / "d_all_0.las"
    _write_laz_rgb(c0, [1, 9], [0, 0], [0, 0])
    c1 = tmp_path / "d_all_1.las"
    _write_laz_rgb(c1, [9, 17], [0, 0], [0, 0])
    out = tmp_path / "merged.laz"
    kept = _merge_laz_parts_seam(
        [str(c0), str(c1)],
        str(out),
        has_rgb=False,
        crs=None,
        overlap_drop=True,
        dedupe_exact=False,
        voxel_size=None,
    )
    # 4 input points, the x=9 seam point double-covered -> owned by exactly one -> 3 kept.
    assert kept == 3
    xs = sorted(round(v, 1) for v in laspy.read(str(out)).x)
    assert xs == [1.0, 9.0, 17.0]


def test_overlap_drop_no_hole_for_own_only_point(tmp_path):
    from databricks.labs.gbx.ds._write_lidar import _merge_laz_parts_seam

    # cluster 0 bbox x[0,2]; cluster 1 bbox x[10,12]; a point x=1 only in c0 and
    # outside c1's bbox -> c0 is its sole candidate -> kept (no drop-hole).
    c0 = tmp_path / "d_all_0.las"
    _write_laz_rgb(c0, [1], [0], [0])
    c1 = tmp_path / "d_all_1.las"
    _write_laz_rgb(c1, [11], [0], [0])
    out = tmp_path / "m.laz"
    kept = _merge_laz_parts_seam(
        [str(c0), str(c1)],
        str(out),
        False,
        None,
        overlap_drop=True,
        dedupe_exact=False,
        voxel_size=None,
    )
    assert kept == 2


def test_overlap_drop_preserves_rgb(tmp_path):
    import laspy

    from databricks.labs.gbx.ds._write_lidar import _merge_laz_parts_seam

    c0 = tmp_path / "d_all_0.las"
    _write_laz_rgb(c0, [1, 9], [0, 0], [0, 0], rgb=([10, 20], [30, 40], [50, 60]))
    c1 = tmp_path / "d_all_1.las"
    _write_laz_rgb(c1, [9, 17], [0, 0], [0, 0], rgb=([21, 70], [41, 80], [61, 90]))
    out = tmp_path / "m.laz"
    kept = _merge_laz_parts_seam(
        [str(c0), str(c1)],
        str(out),
        has_rgb=True,
        crs=None,
        overlap_drop=True,
        dedupe_exact=False,
        voxel_size=None,
    )
    assert kept == 3
    las = laspy.read(str(out))
    reds = {round(float(x), 1): int(r) >> 8 for x, r in zip(las.x, las.red)}
    assert reds[1.0] == 10  # interior point keeps its real color through the merge


def test_dedupe_exact_drops_identical(tmp_path):
    from databricks.labs.gbx.ds._write_lidar import _merge_laz_parts_seam

    a = tmp_path / "a_all_0.las"
    _write_laz_rgb(a, [1, 1, 2], [0, 0, 0], [5, 5, 6])
    out = tmp_path / "m.laz"
    kept = _merge_laz_parts_seam(
        [str(a)],
        str(out),
        False,
        None,
        overlap_drop=False,
        dedupe_exact=True,
        voxel_size=None,
    )
    assert kept == 2  # (1,0,5) duplicated -> once; (2,0,6) once


def test_dedupe_exact_rgb_key_distinguishes_color(tmp_path):
    import laspy

    from databricks.labs.gbx.ds._write_lidar import _merge_laz_parts_seam

    # Same xyz (1,0,0) but different colors -> both kept (color is part of the key).
    # Identical xyz+color at (2,0,0) -> collapses to one.
    a = tmp_path / "a_all_0.las"
    _write_laz_rgb(
        a,
        [1, 1, 2, 2],
        [0, 0, 0, 0],
        [0, 0, 0, 0],
        rgb=([255, 0, 0, 0], [0, 255, 0, 0], [0, 0, 255, 255]),
    )
    out = tmp_path / "m.laz"
    kept = _merge_laz_parts_seam(
        [str(a)],
        str(out),
        has_rgb=True,
        crs=None,
        overlap_drop=False,
        dedupe_exact=True,
        voxel_size=None,
    )
    assert kept == 3
    las = laspy.read(str(out))
    colors_at_1_0_0 = {
        (int(r) >> 8, int(g) >> 8, int(b) >> 8)
        for x, r, g, b in zip(las.x, las.red, las.green, las.blue)
        if round(float(x), 1) == 1.0
    }
    assert colors_at_1_0_0 == {(255, 0, 0), (0, 255, 0)}


def test_voxel_keeps_one_real_point_per_cell(tmp_path):
    import laspy

    from databricks.labs.gbx.ds._write_lidar import _merge_laz_parts_seam

    # 3 points inside one 10m cell + 1 in another cell -> 2 kept, both REAL inputs.
    a = tmp_path / "a_all_0.las"
    _write_laz_rgb(a, [1.0, 2.0, 3.0, 25.0], [1.0, 1.0, 1.0, 1.0], [0, 0, 0, 0])
    out = tmp_path / "m.laz"
    kept = _merge_laz_parts_seam(
        [str(a)],
        str(out),
        False,
        None,
        overlap_drop=False,
        dedupe_exact=False,
        voxel_size=10.0,
    )
    assert kept == 2
    xs = set(round(v, 3) for v in laspy.read(str(out)).x)
    assert xs <= {1.0, 2.0, 3.0, 25.0}  # every kept point is a REAL input point
    # cell [0,10) center x=5 -> nearest of {1,2,3} is 3.0
    assert 3.0 in xs and 25.0 in xs
