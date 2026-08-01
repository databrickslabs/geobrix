"""Driver-side planning for clipPolygons / windows selection."""

import numpy as np
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
