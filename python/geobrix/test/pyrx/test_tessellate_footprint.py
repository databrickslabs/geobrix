import numpy as np
import shapely
from affine import Affine
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds, from_origin
from rasterio.warp import transform_bounds
from shapely.geometry import LinearRing, Polygon, box

from databricks.labs.gbx.core.crs import get_transformer
from databricks.labs.gbx.pyrx.core import tessellate as T

# H3 res-7 cells on the UTM scene below that hold the pixels of its slanted east/west
# fringe (17 valid pixels between them) but lie outside the chord between the corners.
_FRINGE_CELLS = {
    608725936707207167,
    608725953383759871,
    608725953467645951,
    608726101828567039,
}


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
    footprint = T._footprint_polygon(ds, ds, "h3")
    old_box = box(*transform_bounds(ds.crs, "EPSG:4326", *ds.bounds))
    cands = set(T._polyfill_cells(old_box, 7, "h3"))

    def overlapping(region):
        return {
            T._encode_cellid(c, "h3")
            for c in cands
            if T._has_positive_area_overlap(T._cell_geom(c, "h3"), region)
        }

    return overlapping(footprint), overlapping(old_box)


def _centroid_cells(ds, coverage):
    return {
        cid
        for cid, _ in T.iter_tessellate(ds, 7, "h3", mode="centroid", coverage=coverage)
    }


def test_complete_synthetic_cells_use_the_reprojected_footprint():
    with MemoryFile(_utm_scene()) as mf, mf.open() as ds:
        in_footprint, in_box = _utm_cell_sets(ds)
        sparse = _centroid_cells(ds, "sparse")
        # A cell holding pixels is always emitted (as heavy does); only the synthetic
        # (pixel-less) covered cells are kept against the footprint.
        box_only = (in_box - in_footprint) - sparse
        assert box_only, "fixture must expose the axis-aligned-box-only cells"
        emitted = _centroid_cells(ds, "complete")
    assert emitted == sparse | in_footprint
    assert emitted.isdisjoint(box_only)


def test_covering_keeps_every_pixel_bearing_cell():
    with MemoryFile(_utm_scene()) as mf, mf.open() as ds:
        sparse = _centroid_cells(ds, "sparse")
        emitted = {cid for cid, _ in T.iter_tessellate(ds, 7, "h3", mode="covering")}
    assert sparse <= emitted
    assert _FRINGE_CELLS <= emitted


def test_footprint_follows_the_curved_edge_of_a_reprojected_raster():
    with MemoryFile(_utm_scene()) as mf, mf.open() as ds:
        footprint = T._footprint_polygon(ds, ds, "h3")
        t = ds.transform
        # centres of the outermost pixel ring, in lon/lat
        cols = np.concatenate([np.arange(ds.width), np.arange(ds.width)])
        rows = np.concatenate([np.zeros(ds.width), np.full(ds.width, ds.height - 1)])
        rows2 = np.concatenate([np.arange(ds.height), np.arange(ds.height)])
        cols2 = np.concatenate([np.zeros(ds.height), np.full(ds.height, ds.width - 1)])
        cols = np.concatenate([cols, cols2]) + 0.5
        rows = np.concatenate([rows, rows2]) + 0.5
        tr = get_transformer(ds.crs, "EPSG:4326")
        lon, lat = tr.transform(t.c + cols * t.a, t.f + rows * t.e)
        # the chord between the four reprojected corners cuts inside the curved edge ...
        corners = tr.transform(
            [t.c, t.c + ds.width * t.a, t.c + ds.width * t.a, t.c],
            [t.f, t.f, t.f + ds.height * t.e, t.f + ds.height * t.e],
        )
        chord = Polygon(zip(*corners))
        assert not shapely.contains_xy(chord, lon, lat).all()
        # ... the footprint does not
        assert shapely.contains_xy(footprint, lon, lat).all()


def test_footprint_polygon_is_the_pixel_grid_edge_in_the_work_crs():
    # h3/quadbin on an EPSG:4326 raster and on a CRS-less one: corners as-is, CCW
    for crs in ("EPSG:4326", None):
        with MemoryFile(_tiny(crs, from_origin(10, 50, 1, 1))) as mf, mf.open() as ds:
            for grid in ("h3", "quadbin", "custom"):
                footprint = T._footprint_polygon(ds, ds, grid)
                assert footprint.bounds == (10.0, 47.0, 14.0, 50.0)
                assert len(footprint.exterior.coords) == 5
                assert LinearRing(footprint.exterior.coords).is_ccw
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
        footprint = T._footprint_polygon(src, work, "bng")
        assert footprint.bounds == (400000.0, 299700.0, 400400.0, 300000.0)
        assert len(footprint.exterior.coords) == 5
    # a raster in a projected CRS: every edge walked with the densify count, closed ring, CCW
    with MemoryFile(_utm_scene()) as mf, mf.open() as ds:
        footprint = T._footprint_polygon(ds, ds, "h3")
        assert len(footprint.exterior.coords) == 4 * (T._FOOTPRINT_EDGE_POINTS - 1) + 1
        assert LinearRing(footprint.exterior.coords).is_ccw
        assert -75.1 < footprint.bounds[0] < footprint.bounds[2] < -73.6
        assert not footprint.equals(box(*footprint.bounds))
