"""Tests for cluster_by_gps — GPS-proximity partitioning for memory-bounded SfM.

The orthomosaic example reconstructs images in spatially-contiguous, RAM-bounded
clusters (with an overlap ring) instead of one monolithic COLMAP reconstruction,
so peak driver RAM stays bounded on Serverless env5. This function decides the
partition; it must be a pure, session-free helper.
"""

import math

import pandas as pd

from databricks.labs.gbx.pyrx.imagery import cluster_by_gps


def _grid_df(n_side: int, spacing_deg: float = 1e-4, lat0: float = 37.75, lon0: float = -122.45):
    """A regular n_side x n_side grid of GPS points (n_side**2 images)."""
    rows = []
    for i in range(n_side):
        for j in range(n_side):
            rows.append(
                {
                    "source": f"IMG_{i:02d}_{j:02d}.JPG",
                    "latitude": lat0 + i * spacing_deg,
                    "longitude": lon0 + j * spacing_deg,
                }
            )
    return pd.DataFrame(rows)


def test_ram_bound_no_cluster_too_large():
    """No cluster exceeds the target size (plus overlap ring + a small merge slack)."""
    df = _grid_df(14)  # 196 images
    target, overlap, min_c = 40, 0.30, 8
    out = cluster_by_gps(
        df, target_cluster_images=target, overlap_frac=overlap, min_cluster_images=min_c
    )
    sizes = out.groupby("_cluster").size()
    cap = math.ceil(target * (1 + 2 * overlap)) + min_c
    assert sizes.max() <= cap, f"largest cluster {sizes.max()} exceeds cap {cap}"
    assert sizes.min() >= min_c or len(sizes) == 1


def test_coverage_every_image_in_at_least_one_cluster():
    df = _grid_df(12)  # 144
    out = cluster_by_gps(df, target_cluster_images=40, overlap_frac=0.3)
    assert set(out["source"]) == set(df["source"])


def test_overlap_duplicates_boundary_images():
    df = _grid_df(12)
    out = cluster_by_gps(df, target_cluster_images=30, overlap_frac=0.3)
    per_image = out.groupby("source")["_cluster"].nunique()
    assert (per_image >= 2).any(), "overlap_frac>0 should share some images across clusters"
    # more clusters than one, and total rows exceed input (duplication happened)
    assert out["_cluster"].nunique() >= 2
    assert len(out) > len(df)


def test_zero_overlap_is_a_partition():
    df = _grid_df(12)
    out = cluster_by_gps(df, target_cluster_images=30, overlap_frac=0.0, min_cluster_images=1)
    per_image = out.groupby("source")["_cluster"].nunique()
    assert (per_image == 1).all(), "overlap_frac=0 must give a strict partition"
    assert len(out) == len(df)
    assert set(out["source"]) == set(df["source"])


def test_min_merge_no_tiny_clusters():
    df = _grid_df(13)  # 169
    out = cluster_by_gps(
        df, target_cluster_images=40, overlap_frac=0.2, min_cluster_images=10
    )
    sizes = out.groupby("_cluster").size()
    if len(sizes) > 1:
        assert sizes.min() >= 10, f"tiny cluster survived: {sizes.to_dict()}"


def test_small_n_single_cluster():
    df = _grid_df(5)  # 25 <= target
    out = cluster_by_gps(df, target_cluster_images=40, overlap_frac=0.3)
    assert out["_cluster"].nunique() == 1
    assert set(out["source"]) == set(df["source"])


def test_degenerate_identical_points_single_cluster():
    df = pd.DataFrame(
        {"source": [f"i{k}.JPG" for k in range(50)],
         "latitude": [37.75] * 50, "longitude": [-122.45] * 50}
    )
    out = cluster_by_gps(df, target_cluster_images=20, overlap_frac=0.3)
    assert out["_cluster"].nunique() == 1
    assert len(out) == 50


def test_cluster_ids_are_contiguous_from_zero():
    df = _grid_df(12)
    out = cluster_by_gps(df, target_cluster_images=30, overlap_frac=0.3)
    ids = sorted(out["_cluster"].unique())
    assert ids == list(range(len(ids)))
