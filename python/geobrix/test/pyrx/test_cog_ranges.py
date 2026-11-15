from test._cog_fixture import cog_bytes as _cog_bytes

import numpy as np
import pytest
import shapely.geometry as sg
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import cog_ranges as cr


def _ranged_read(blob, geom=None, geom_crs=None, resolution=None):
    head = blob[: cr.HEAD_BYTES]
    plan = cr.plan_ranges(head, len(blob), geom, geom_crs, resolution)
    pieces = [(0, head)] + [(o, blob[o : o + n]) for o, n in plan.ranges]
    with cr.open_ranges(len(blob), pieces, plan.overview_level) as ds:
        c, r, w, h = plan.window
        return plan, ds.read(window=Window(c, r, w, h))


def _full_read(blob, plan):
    kw = {} if plan.overview_level is None else {"overview_level": plan.overview_level}
    with MemoryFile(blob) as mf, mf.open(**kw) as ds:
        c, r, w, h = plan.window
        return ds.read(window=Window(c, r, w, h))


@pytest.mark.parametrize("interleave", ["pixel", "band"])
def test_ranged_equals_full(interleave):
    blob = _cog_bytes(interleave=interleave)
    poly = sg.box(405000, 3990000, 412000, 3996000)  # inside the raster, in its CRS
    plan, got = _ranged_read(blob, poly)
    assert plan.overview_level is None
    assert np.array_equal(got, _full_read(blob, plan))
    assert sum(n for _, n in plan.ranges) < len(blob) / 2  # a window fetches a fraction


def test_ranged_equals_full_separate_planar():
    blob = _cog_bytes(interleave="band", count=4)
    plan, got = _ranged_read(blob, sg.box(400000, 3985000, 420000, 4000000))
    assert np.array_equal(got, _full_read(blob, plan))


def test_sparse_block_reads_as_nodata():
    blob = _cog_bytes(sparse=True)
    plan, got = _ranged_read(
        blob, sg.box(400000, 3994000, 406000, 4000000)
    )  # covers the sparse block
    assert np.array_equal(got, _full_read(blob, plan))


def test_edge_blocks():
    blob = _cog_bytes(width=1100, height=1030)  # partial right/bottom blocks
    plan, got = _ranged_read(blob, None)  # whole raster
    assert plan.window == (0, 0, 1100, 1030)
    assert np.array_equal(got, _full_read(blob, plan))


def test_overview_by_resolution():
    blob = _cog_bytes()
    plan, got = _ranged_read(
        blob, None, None, resolution=40.0
    )  # native 10 m -> factor 4 -> level 1
    assert plan.overview_level == 1
    assert np.array_equal(got, _full_read(blob, plan))


def test_pick_overview_bounds():
    blob = _cog_bytes()
    with MemoryFile(blob) as mf, mf.open() as ds:
        assert cr.pick_overview(ds, None) is None
        assert cr.pick_overview(ds, 5.0) is None  # finer than native
        assert cr.pick_overview(ds, 10.0) is None  # native
        assert cr.pick_overview(ds, 25.0) == 0  # factor 2
        assert (
            cr.pick_overview(ds, 1000.0) == 2
        )  # coarser than coarsest -> coarsest (factor 8)


def test_other_crs_window():
    blob = _cog_bytes()
    from pyproj import Transformer

    t = Transformer.from_crs("EPSG:32614", "EPSG:4326", always_xy=True)
    x0, y0 = t.transform(405000, 3990000)
    x1, y1 = t.transform(412000, 3996000)
    plan, got = _ranged_read(blob, sg.box(x0, y0, x1, y1), "EPSG:4326")
    assert plan.window is not None and np.array_equal(got, _full_read(blob, plan))


def test_outside_window_is_none():
    blob = _cog_bytes()
    plan = cr.plan_ranges(
        blob[: cr.HEAD_BYTES], len(blob), sg.box(0, 0, 10, 10), None, None
    )
    assert plan.window is None and plan.ranges == ()


def test_missing_range_raises():
    """A read past the fetched bytes fails; it never returns zeros. GDAL may wrap the Python error."""
    blob = _cog_bytes()
    opener = cr._Opener(len(blob), [(0, blob[: cr.HEAD_BYTES])])
    import rasterio

    with pytest.raises(Exception) as exc:
        with rasterio.open(cr._NAME, driver="GTiff", opener=opener) as ds:
            ds.read(1)  # pixel blocks were never fetched
    assert isinstance(exc.value, cr.MissingRangeError) or opener.misses
    assert opener.misses or isinstance(exc.value, cr.MissingRangeError)


def test_header_too_large():
    blob = _cog_bytes()
    with pytest.raises(cr.HeaderTooLargeError, match="Fix:"):
        cr.plan_ranges(
            blob[:200], len(blob), None, None, None
        )  # head far too short for the IFDs


def test_striped_refused():
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=600,
            height=600,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(0, 10, 0.01, 0.01),
        ) as ds:  # not tiled: strips
            ds.write(np.ones((1, 600, 600), "uint8"))
        blob = mf.read()
    with pytest.raises(cr.NotTiledError, match="Fix:"):
        cr.plan_ranges(blob[: cr.HEAD_BYTES], len(blob), None, None, None)
