"""Driver-side planning for clipPolygons / windows / tileSize selection."""

import numpy as np
import pytest
import rasterio
import shapely.wkb
from rasterio.transform import from_origin
from shapely import set_srid
from shapely.geometry import box

from databricks.labs.gbx.ds.raster import _plan_partitions_for_file


def _write(tmp_path, width=1000, height=1000):
    p = str(tmp_path / "r.tif")
    prof = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.001, 0.001),
        nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.zeros((height, width), "float32"), 1)
    return p


def _write_header(tmp_path, width, height, name="h.tif"):
    """Create a GTiff with given dimensions but minimal data (no large pixel array).

    The guard reads only ds.width/height/count/dtypes so we write a tiled GTiff
    with 1x1 block to avoid allocating a width*height pixel array at test time.
    """
    p = str(tmp_path / name)
    # blocksize must be a multiple of 16 per TIFF spec; 16 is the minimum.
    blocksize = 16
    prof = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.001, 0.001),
        tiled=True,
        blockxsize=blocksize,
        blockysize=blocksize,
    )
    with rasterio.open(p, "w", **prof) as ds:
        # Write only the first block (16x16) — guard reads only metadata.
        ds.write(
            np.zeros((16, 16), "float32"),
            1,
            window=rasterio.windows.Window(0, 0, 16, 16),
        )
    return p


def _boxwkb(c0, r0, c1, r1):
    return shapely.wkb.dumps(
        box(10.0 + c0 * 0.001, 50.0 - r1 * 0.001, 10.0 + c1 * 0.001, 50.0 - r0 * 0.001)
    )


def test_clip_polygons_one_partition_each(tmp_path):
    p = _write(tmp_path)
    parts = _plan_partitions_for_file(
        p,
        0,
        clip_polygons=[_boxwkb(100, 50, 300, 250), _boxwkb(400, 400, 500, 500)],
        clip_crs="EPSG:4326",
        windows=[],
    )
    assert len(parts) == 2
    assert parts[0].window == (100, 50, 200, 200)
    assert parts[0].clip_polygon is not None and parts[0].clip_crs == "EPSG:4326"


def test_clip_polygon_disjoint_skipped(tmp_path):
    p = _write(tmp_path)
    parts = _plan_partitions_for_file(
        p,
        0,
        clip_polygons=[shapely.wkb.dumps(box(100.0, 10.0, 101.0, 11.0))],
        clip_crs="EPSG:4326",
        windows=[],
    )
    assert parts == []


def test_windows_one_partition_each_partial_clipped(tmp_path):
    p = _write(tmp_path, width=200, height=200)
    parts = _plan_partitions_for_file(
        p,
        0,
        clip_polygons=[],
        clip_crs=None,
        windows=[(0, 0, 256, 256), (50, 50, 100, 100)],
    )
    # first window overhangs 200x200 -> clipped to (0,0,200,200); second fits
    assert len(parts) == 2
    assert parts[0].window == (0, 0, 200, 200)
    assert parts[1].window == (50, 50, 100, 100)
    assert all(p.clip_polygon is None for p in parts)


def test_window_fully_outside_skipped(tmp_path):
    p = _write(tmp_path, width=200, height=200)
    parts = _plan_partitions_for_file(
        p, 0, clip_polygons=[], clip_crs=None, windows=[(500, 500, 10, 10)]
    )
    assert parts == []


def test_embedded_srid_overrides_clip_crs(tmp_path):
    p = _write(tmp_path)
    g = set_srid(box(10.1, 49.8, 10.2, 49.9), 4326)
    parts = _plan_partitions_for_file(
        p,
        0,
        clip_polygons=[shapely.wkb.dumps(g, include_srid=True)],
        clip_crs="EPSG:3857",
        windows=[],
    )  # clipCrs is 3857 but embedded is 4326
    assert len(parts) == 1
    assert parts[0].clip_crs == "EPSG:4326"  # embedded SRID wins


# ---------------------------------------------------------------------------
# tileSize planning branch
# ---------------------------------------------------------------------------


def test_tile_size_plans_grid(tmp_path):
    p = _write(tmp_path, width=512, height=512)
    parts = _plan_partitions_for_file(p, 0, tile_size=(256, 256), overlap_percent=0)
    assert len(parts) == 4
    assert (0, 0, 256, 256) in [pt.window for pt in parts]
    assert all(pt.clip_polygon is None for pt in parts)


def test_tile_size_overlap_changes_count(tmp_path):
    p = _write(tmp_path, width=512, height=512)
    parts = _plan_partitions_for_file(p, 0, tile_size=(256, 256), overlap_percent=25)
    assert len(parts) == 9  # 3x3 with step 192


def test_tile_size_larger_than_raster_single(tmp_path):
    p = _write(tmp_path, width=300, height=200)
    parts = _plan_partitions_for_file(p, 0, tile_size=(512, 512), overlap_percent=0)
    assert len(parts) == 1
    assert parts[0].window == (0, 0, 300, 200)


def test_tile_size_materialized_guard_raises(tmp_path):
    # a 40000x40000 float32 cell would be ~6.4 GB > ~1.8 GiB guard
    p = _write_header(
        tmp_path, width=50000, height=50000
    )  # header only; not materialized
    with pytest.raises(ValueError, match="tileSize"):
        _plan_partitions_for_file(
            p, 0, tile_size=(40000, 40000), overlap_percent=0, emit_virtual=False
        )


def test_tile_size_virtual_no_guard(tmp_path):
    # same oversized tile is fine in virtual mode (bytes-free)
    p = _write_header(tmp_path, width=50000, height=50000, name="h2.tif")
    parts = _plan_partitions_for_file(
        p, 0, tile_size=(40000, 40000), overlap_percent=0, emit_virtual=True
    )
    assert len(parts) >= 1  # no raise
