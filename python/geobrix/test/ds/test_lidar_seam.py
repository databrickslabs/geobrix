import itertools

import numpy as np
import pytest
from pyspark.sql.types import DoubleType, StructField, StructType

from databricks.labs.gbx.ds._write_lidar import (
    LidarGbxWriter,
    _part_cluster_meta,
    _reject_v2_options,
    _seam_select,
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


def test_commit_merge_overlap_drop_end_to_end(tmp_path):
    from databricks.labs.gbx.ds._write_lidar import LidarGbxWriter

    c0 = tmp_path / "d_all_0.las"
    _write_laz_rgb(c0, [1, 9], [0, 0], [0, 0])
    c1 = tmp_path / "d_all_1.las"
    _write_laz_rgb(c1, [9, 17], [0, 0], [0, 0])
    w = LidarGbxWriter(
        {
            "path": str(tmp_path),
            "merge": "true",
            "overlap": "drop",
            "fileName": "merged",
            "keepParts": "true",
        },
        StructType([StructField(c, DoubleType()) for c in ("x", "y", "z")]),
        overwrite=False,
    )
    w.commit([])
    import laspy

    merged = tmp_path / "merged.laz"
    real = merged if merged.exists() else tmp_path / "merged.las"
    assert real.exists()
    assert int(laspy.open(str(real)).header.point_count) == 3  # seam point once
    assert (c0).exists() and (c1).exists()  # keepParts retained


def test_commit_merge_empty_unify_does_not_publish(tmp_path, monkeypatch):
    # If a unify drops everything, publish must not emit a 0-point file; parts kept.
    from databricks.labs.gbx.ds import _write_lidar as W

    c0 = tmp_path / "d_all_0.las"
    _write_laz_rgb(c0, [1], [0], [0])
    w = W.LidarGbxWriter(
        {"path": str(tmp_path), "merge": "true", "voxelSize": "1.0", "fileName": "m"},
        StructType([StructField(c, DoubleType()) for c in ("x", "y", "z")]),
        overwrite=False,
    )
    monkeypatch.setattr(W, "_merge_laz_parts_seam", lambda *a, **k: 0)
    with pytest.raises(ValueError, match="0 points|empty"):
        w.commit([])
    assert c0.exists()


def test_commit_merge_overlap_and_voxel_roundtrips(spark, tmp_path):
    from databricks.labs.gbx.ds._write_lidar import LidarGbxWriter
    from databricks.labs.gbx.ds.lidar import LidarGbxDataSource

    try:
        spark.dataSource.register(LidarGbxDataSource)
    except Exception:
        pass
    c0 = tmp_path / "d_all_0.las"
    _write_laz_rgb(c0, [1.0, 1.2, 9.0], [0, 0, 0], [0, 0, 0])
    c1 = tmp_path / "d_all_1.las"
    _write_laz_rgb(c1, [9.0, 17.0], [0, 0], [0, 0])
    w = LidarGbxWriter(
        {
            "path": str(tmp_path),
            "merge": "true",
            "overlap": "drop",
            "voxelSize": "0.5",
            "fileName": "u",
        },
        StructType([StructField(c, DoubleType()) for c in ("x", "y", "z")]),
        overwrite=False,
    )
    w.commit([])
    real = tmp_path / "u.laz"
    real = real if real.exists() else tmp_path / "u.las"
    df = spark.read.format("lidar_gbx").option("mode", "metadata").load(str(real))
    assert df.collect()[0]["point_count"] >= 1


# ---------------------------------------------------------------------------
# _seam_select vectorization: oracle-equivalence + constructed-tie tests
# ---------------------------------------------------------------------------


def _ref_nearest_owner(x: float, y: float, centers: dict, bboxes: dict) -> str:
    """Faithful copy of the (removed) scalar `_nearest_owner` reference."""
    cand = [
        cid
        for cid, (xmin, xmax, ymin, ymax) in bboxes.items()
        if xmin <= x <= xmax and ymin <= y <= ymax
    ]
    best, best_d = None, None
    for cid in sorted(cand):
        cx, cy = centers[cid]
        d = (x - cx) ** 2 + (y - cy) ** 2
        if best_d is None or d < best_d:
            best, best_d = cid, d
    return best


def _ref_seam_select(
    x, y, z, r, g, b, part_ids, meta, *, overlap_drop, dedupe_exact, voxel_size, has_rgb
):
    """Faithful scalar reimplementation of the ORIGINAL per-point loop in
    `_merge_laz_parts_seam` (before vectorization) — the oracle for equivalence
    testing. Operates on in-memory arrays instead of re-reading LAZ parts."""
    n = len(x)
    kx, ky, kz, kr, kg, kb = [], [], [], [], [], []
    seen = set() if dedupe_exact else None
    v = voxel_size
    cells = {} if v else None  # cell -> (dist2_to_center, (x,y,z,r,g,b))

    def _emit(xx, yy, zz, rr, gg, bb):
        if seen is not None:
            key = (
                (round(xx, 3), round(yy, 3), round(zz, 3), rr, gg, bb)
                if has_rgb
                else (round(xx, 3), round(yy, 3), round(zz, 3))
            )
            if key in seen:
                return
            seen.add(key)
        if cells is not None:
            cx = (xx // v + 0.5) * v
            cy = (yy // v + 0.5) * v
            cz = (zz // v + 0.5) * v
            d2 = (xx - cx) ** 2 + (yy - cy) ** 2 + (zz - cz) ** 2
            cell = (int(xx // v), int(yy // v), int(zz // v))
            cur = cells.get(cell)
            if cur is None or d2 < cur[0]:
                cells[cell] = (d2, (xx, yy, zz, rr, gg, bb))
            return
        kx.append(xx)
        ky.append(yy)
        kz.append(zz)
        if has_rgb:
            kr.append(rr)
            kg.append(gg)
            kb.append(bb)

    for i in range(n):
        xx, yy, zz = float(x[i]), float(y[i]), float(z[i])
        if overlap_drop:
            owner = _ref_nearest_owner(xx, yy, meta["center"], meta["bbox"])
            if owner != part_ids[i]:
                continue  # another cluster owns this seam point
        rr = int(r[i]) if has_rgb else 0
        gg = int(g[i]) if has_rgb else 0
        bb = int(b[i]) if has_rgb else 0
        _emit(xx, yy, zz, rr, gg, bb)

    if cells is not None:
        for _d2, (xx, yy, zz, rr, gg, bb) in cells.values():
            kx.append(xx)
            ky.append(yy)
            kz.append(zz)
            if has_rgb:
                kr.append(rr)
                kg.append(gg)
                kb.append(bb)

    return (
        np.asarray(kx, dtype=np.float64),
        np.asarray(ky, dtype=np.float64),
        np.asarray(kz, dtype=np.float64),
        np.asarray(kr, dtype=np.uint8),
        np.asarray(kg, dtype=np.uint8),
        np.asarray(kb, dtype=np.uint8),
    )


def _canon_rows(x, y, z, r, g, b, has_rgb):
    """Canonical row ordering (lexsort) for set-equality comparison of kept points."""
    x = np.asarray(x, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    z = np.asarray(z, dtype=np.float64)
    if has_rgb:
        r = np.asarray(r, dtype=np.int64)
        g = np.asarray(g, dtype=np.int64)
        b = np.asarray(b, dtype=np.int64)
        order = np.lexsort((b, g, r, z, y, x))
        return np.stack(
            [x[order], y[order], z[order], r[order], g[order], b[order]], axis=1
        )
    order = np.lexsort((z, y, x))
    return np.stack([x[order], y[order], z[order]], axis=1)


def _make_random_case(rng, overlap_drop, has_rgb):
    """Random points + (when overlap_drop) a consistent meta/part_ids: bboxes that
    cover each part's own points, with deliberate seam overlap between neighbors."""
    n = int(rng.integers(150, 300))
    x = rng.uniform(0.0, 6.0, n)
    y = rng.uniform(0.0, 3.0, n)
    z = rng.uniform(0.0, 2.0, n)
    if has_rgb:
        r = rng.integers(0, 256, n).astype(np.uint8)
        g = rng.integers(0, 256, n).astype(np.uint8)
        b = rng.integers(0, 256, n).astype(np.uint8)
    else:
        r = np.zeros(n, dtype=np.uint8)
        g = np.zeros(n, dtype=np.uint8)
        b = np.zeros(n, dtype=np.uint8)

    # Force some exact mm-rounded duplicate keys so dedupe has real work to do.
    dup_n = min(20, n // 4)
    if dup_n:
        src = rng.integers(0, n, dup_n)
        dst = rng.integers(0, n, dup_n)
        x[dst] = np.round(x[src], 3)
        y[dst] = np.round(y[src], 3)
        z[dst] = np.round(z[src], 3)
        if has_rgb:
            r[dst] = r[src]
            g[dst] = g[src]
            b[dst] = b[src]

    meta = None
    part_ids = None
    if overlap_drop:
        ncluster = int(rng.integers(2, 6))
        width = 6.0 / ncluster
        margin = width * 0.3  # deliberate seam overlap between neighbor bboxes
        cids = [str(i) for i in range(ncluster)]
        bbox = {}
        center = {}
        for i, cid in enumerate(cids):
            xmin, xmax = i * width - margin, (i + 1) * width + margin
            bbox[cid] = (xmin, xmax, -1.0, 4.0)
            center[cid] = ((xmin + xmax) / 2.0, 1.5)
        meta = {"center": center, "bbox": bbox}
        home = np.clip((x // width).astype(int), 0, ncluster - 1)
        part_ids = np.array([cids[h] for h in home], dtype=object)

    return x, y, z, r, g, b, part_ids, meta


def test_seam_select_oracle_equivalence():
    combos = list(
        itertools.product((False, True), (False, True), (None, 0.5), (False, True))
    )
    for overlap_drop, dedupe_exact, voxel_size, has_rgb in combos:
        for seed in range(25):
            rng = np.random.default_rng(
                seed * 1000 + hash((overlap_drop, dedupe_exact, has_rgb)) % 997
            )
            x, y, z, r, g, b, part_ids, meta = _make_random_case(
                rng, overlap_drop, has_rgb
            )
            prod = _seam_select(
                x,
                y,
                z,
                r,
                g,
                b,
                part_ids,
                meta,
                overlap_drop=overlap_drop,
                dedupe_exact=dedupe_exact,
                voxel_size=voxel_size,
                has_rgb=has_rgb,
            )
            ref = _ref_seam_select(
                x,
                y,
                z,
                r,
                g,
                b,
                part_ids,
                meta,
                overlap_drop=overlap_drop,
                dedupe_exact=dedupe_exact,
                voxel_size=voxel_size,
                has_rgb=has_rgb,
            )
            ctx = (
                f"overlap_drop={overlap_drop} dedupe_exact={dedupe_exact} "
                f"voxel_size={voxel_size} has_rgb={has_rgb} seed={seed}"
            )
            assert len(prod[0]) == len(ref[0]), f"kept-count mismatch: {ctx}"
            prod_rows = _canon_rows(*prod, has_rgb=has_rgb)
            ref_rows = _canon_rows(*ref, has_rgb=has_rgb)
            assert np.array_equal(prod_rows, ref_rows), f"kept-set mismatch: {ctx}"


def test_seam_select_dedupe_keeps_first_of_near_duplicate():
    # Both round(_, 3) to 1.000 but have different unrounded coords -> kept point
    # must be the FIRST occurrence's real (unrounded) coords.
    x = np.array([1.0001, 1.0004])
    y = np.array([2.0, 2.0])
    z = np.array([3.0, 3.0])
    zeros = np.zeros(2, dtype=np.uint8)
    kx, ky, kz, kr, kg, kb = _seam_select(
        x,
        y,
        z,
        zeros,
        zeros,
        zeros,
        None,
        None,
        overlap_drop=False,
        dedupe_exact=True,
        voxel_size=None,
        has_rgb=False,
    )
    assert len(kx) == 1
    assert kx[0] == pytest.approx(1.0001, abs=1e-12)


def test_seam_select_voxel_tie_keeps_earliest():
    # Two points in the same 1.0 voxel cell [0,1)^3, exactly equidistant from the
    # cell center (0.5,0.5,0.5) -> strict "<" in the original loop means the
    # EARLIEST stream-index point wins the tie.
    x = np.array([0.4, 0.6])
    y = np.array([0.5, 0.5])
    z = np.array([0.5, 0.5])
    zeros = np.zeros(2, dtype=np.uint8)
    kx, ky, kz, kr, kg, kb = _seam_select(
        x,
        y,
        z,
        zeros,
        zeros,
        zeros,
        None,
        None,
        overlap_drop=False,
        dedupe_exact=False,
        voxel_size=1.0,
        has_rgb=False,
    )
    assert len(kx) == 1
    assert kx[0] == pytest.approx(0.4)
