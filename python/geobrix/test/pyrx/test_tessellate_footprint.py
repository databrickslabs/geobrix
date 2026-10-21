import numpy as np
from affine import Affine
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds, from_origin
from rasterio.warp import transform_bounds
from shapely.geometry import LinearRing, box

from databricks.labs.gbx.pyrx.core import tessellate as T


def _utm_scene():
    # ~110 km UTM 18N square at 100 m, like the scene where the tiers differed by 35 cells
    prof = dict(
        driver="GTiff",
        width=1098,
        height=1098,
        count=1,
        dtype="uint16",
        nodata=0,
        crs="EPSG:32618",
        transform=from_bounds(499980, 4390200, 609780, 4500000, 1098, 1098),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.ones((1, 1098, 1098), "uint16"))
        return mf.read()


def _tiny(crs, transform, width=4, height=3):
    prof = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="uint8",
        crs=crs,
        transform=transform,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.ones((1, height, width), "uint8"))
        return mf.read()


def _utm_cell_sets(ds):
    """Candidate h3 cells of the lon/lat box, split by who overlaps what (all as encoded ids)."""
    quad = T._footprint_polygon(ds, ds, "h3")
    old_box = box(*transform_bounds(ds.crs, "EPSG:4326", *ds.bounds))
    cands = set(T._polyfill_cells(old_box, 7, "h3"))

    def overlapping(region):
        return {
            T._encode_cellid(c, "h3")
            for c in cands
            if T._has_positive_area_overlap(T._cell_geom(c, "h3"), region)
        }

    return overlapping(quad), overlapping(old_box)


def test_complete_synthetic_cells_use_the_reprojected_corner_footprint():
    with MemoryFile(_utm_scene()) as mf, mf.open() as ds:
        in_quad, in_box = _utm_cell_sets(ds)
        sparse = {
            cid
            for cid, _ in T.iter_tessellate(
                ds, 7, "h3", mode="centroid", coverage="sparse"
            )
        }
        # A cell holding pixels is always emitted (as heavy does); only the synthetic
        # (pixel-less) covered cells are kept against the footprint.
        box_only = (in_box - in_quad) - sparse
        assert box_only, "fixture must expose the axis-aligned-box-only cells"
        emitted = {
            cid
            for cid, _ in T.iter_tessellate(
                ds, 7, "h3", mode="centroid", coverage="complete"
            )
        }
    assert emitted == sparse | in_quad
    assert emitted.isdisjoint(box_only)


def test_covering_uses_the_reprojected_corner_footprint():
    with MemoryFile(_utm_scene()) as mf, mf.open() as ds:
        in_quad, in_box = _utm_cell_sets(ds)
        assert in_box - in_quad, "fixture must expose the axis-aligned-box-only cells"
        emitted = {cid for cid, _ in T.iter_tessellate(ds, 7, "h3", mode="covering")}
    assert emitted <= in_quad


def test_footprint_polygon_is_the_corner_quad_in_the_work_crs():
    # h3/quadbin on an EPSG:4326 raster and on a CRS-less one: corners as-is, CCW
    for crs in ("EPSG:4326", None):
        with MemoryFile(_tiny(crs, from_origin(10, 50, 1, 1))) as mf, mf.open() as ds:
            for grid in ("h3", "quadbin", "custom"):
                quad = T._footprint_polygon(ds, ds, grid)
                assert quad.bounds == (10.0, 47.0, 14.0, 50.0)
                assert LinearRing(quad.exterior.coords).is_ccw
    # the rotation terms are ignored, exactly as heavy's windowBBox ignores gt2/gt4
    with (
        MemoryFile(_tiny("EPSG:4326", Affine(1, 0.5, 10, 0.5, -1, 50))) as mf,
        mf.open() as ds,
    ):
        assert T._footprint_polygon(ds, ds, "h3").bounds == (10.0, 47.0, 14.0, 50.0)
    # bng takes its corners from the EPSG:27700 work dataset, never from the source raster
    with (
        MemoryFile(_tiny("EPSG:4326", from_origin(10, 50, 1, 1))) as src_mf,
        src_mf.open() as src,
        MemoryFile(
            _tiny("EPSG:27700", from_origin(400000, 300000, 100, 100))
        ) as work_mf,
        work_mf.open() as work,
    ):
        quad = T._footprint_polygon(src, work, "bng")
        assert quad.bounds == (400000.0, 299700.0, 400400.0, 300000.0)
    # an h3 raster in a projected CRS lands in lon/lat, slanted rather than the axis-aligned box
    with MemoryFile(_utm_scene()) as mf, mf.open() as ds:
        quad = T._footprint_polygon(ds, ds, "h3")
        assert -75.1 < quad.bounds[0] < quad.bounds[2] < -73.6
        assert len(quad.exterior.coords) == 5
        assert not quad.equals(box(*quad.bounds))
