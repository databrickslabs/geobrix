from test._cog_fixture import cog_bytes as _cog_bytes

import numpy as np
import pytest
import shapely.geometry as sg
from rasterio.errors import RasterioIOError
from rasterio.io import MemoryFile
from rasterio.shutil import copy
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


def test_sparse_block_is_not_planned():
    blob = _cog_bytes(sparse=True)
    # cols 0-300, rows 0-400: inside block (0, 0), the unwritten one
    plan, got = _ranged_read(blob, sg.box(400000, 3996000, 403000, 4000000))
    assert plan.window == (0, 0, 300, 400)
    assert plan.ranges == ()  # nothing to fetch for a sparse block
    assert not got.any()  # nodata (0), from no fetched bytes


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
    """A read past the fetched bytes fails: GDAL gets a short read, never zeros."""
    blob = _cog_bytes()
    misses = []
    with pytest.raises(RasterioIOError):
        with cr.open_ranges(
            len(blob), [(0, blob[: cr.HEAD_BYTES])], misses=misses
        ) as ds:
            ds.read(1)  # pixel blocks were never fetched
    assert misses
    assert all(off >= 0 and n > 0 for off, n in misses)


def _many_tile_cog(side=12000, block=256):
    """A COG with thousands of tiles: its offset and size arrays are larger than a small head."""
    prof = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=1,
        dtype="uint8",
        crs="EPSG:32614",
        transform=from_origin(400000, 4000000, 10, 10),
        tiled=True,
        blockxsize=block,
        blockysize=block,
        compress="deflate",
    )
    with MemoryFile() as tmp:
        with tmp.open(**prof) as ds:
            for row in range(0, side, 1000):
                h = min(1000, side - row)
                ds.write(
                    np.full((1, h, side), 7, "uint8"), window=Window(0, row, side, h)
                )
        with tmp.open() as src, MemoryFile() as out:
            copy(src, out.name, driver="COG", BLOCKSIZE=block, COMPRESS="DEFLATE")
            return out.read()


@pytest.mark.parametrize("head_bytes", [200, 2000, 8000, 16000])
def test_header_too_large(head_bytes):
    blob = _many_tile_cog()
    # a truncated head is never read as a sparse file: the window of tiles is far past the fetched block table
    with pytest.raises(cr.HeaderTooLargeError, match="Fix:"):
        cr.plan_ranges(
            blob[:head_bytes],
            len(blob),
            sg.box(400000, 3880000, 420000, 3900000),
            None,
            None,
        )


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
