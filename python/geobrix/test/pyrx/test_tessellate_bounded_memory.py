"""Light centroid tessellate assigns pixels to cells in bounded memory.

Assignment runs per row strip (capped by pixel count, whole source blocks when they
fit) with array coordinate transforms and an int32 label raster; each chip is read by
its cell's window. The partition, the chip pixels and
the emission order (first-seen pixel, row-major) are those of the per-pixel
definition, across strip boundaries, on a projected raster, for bng and custom
grids, and for a path-backed (virtual) tile. BNG warps the raster once per call.
"""

import tracemalloc

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pygx._custom import CustomGridConf
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import tessellate as T
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from .test_tessellate_centroid_window import _oracle_and_check, _raster


@pytest.mark.perf
def test_centroid_assignment_memory_is_bounded():
    b = _raster(2048, 2048)  # 4.2 M px
    with MemoryFile(b) as mf, mf.open() as ds:
        tracemalloc.start()
        for _ in T.iter_tessellate(ds, 7, "h3", mode="centroid", coverage="sparse"):
            pass
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    # Per-pixel loop: ~90 B/px here (~170 B/px with a reprojection). Bounded design:
    # int32 labels (4 B/px) + one strip of at most _STRIP_PIXELS (1 M) px of coords,
    # encodes and transient h3 ints (~80 B per strip px): ~23 B/px here.
    assert peak / (2048 * 2048) < 64, f"{peak / (2048 * 2048):.1f} B/px"


@pytest.mark.perf
def test_single_strip_source_memory_is_bounded():
    # One GTiff strip holds the whole image: the block height must not set the strip.
    b = _raster(2048, 2048, blockysize=2048)
    with MemoryFile(b) as mf, mf.open() as ds:
        assert ds.block_shapes[0] == (2048, 2048)
        tracemalloc.start()
        for _ in T.iter_tessellate(ds, 7, "h3", mode="centroid", coverage="sparse"):
            pass
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    assert peak / (2048 * 2048) < 64, f"{peak / (2048 * 2048):.1f} B/px"


@pytest.mark.parametrize(
    "blockysize,strip_pixels,rows",
    [
        (300, 300 * 100, 100),  # single-strip source: 3 strips inside its one block
        (16, 300 * 40, 32),  # 16-row blocks: 40 rows round down to 2 whole blocks
        (64, 300 * 40, 40),  # block taller than the cap: the pixel cap wins
    ],
)
def test_strip_rows_follow_the_pixel_cap_and_block_height(
    monkeypatch, blockysize, strip_pixels, rows
):
    monkeypatch.setattr(T, "_STRIP_PIXELS", strip_pixels)
    b = _raster(300, 300, blockysize=blockysize)
    with MemoryFile(b) as mf, mf.open() as ds:
        assert ds.block_shapes[0][0] == blockysize
        assert T._strip_rows(ds) == rows
    _oracle_and_check(b, 7, "h3")


def test_oracle_on_utm_raster():  # the array transform path
    b = _raster(256, 256, crs="EPSG:32618", bounds=(500000, 4490000, 525600, 4515600))
    _oracle_and_check(b, 7, "h3")


def test_strip_boundaries_are_seamless(monkeypatch):
    monkeypatch.setattr(T, "_STRIP_PIXELS", 1300 * 512)  # ~512-row strips
    b = _raster(1300, 700)
    with MemoryFile(b) as mf, mf.open() as ds:
        assert T._strip_rows(ds) < 700  # two strips, uneven tail
    _oracle_and_check(b, 7, "h3")


def test_window_table_grows_across_strips(monkeypatch):
    # ~1 quadbin res-18 cell per pixel: ~4k cells over four 16-row strips, so the
    # 1024-label table grows after strips that already filled windows.
    monkeypatch.setattr(T, "_STRIP_PIXELS", 64 * 16)
    b = _raster(64, 64, bounds=(-122.6, 37.6, -122.5, 37.7))
    with MemoryFile(b) as mf, mf.open() as ds:
        assert T._strip_rows(ds) == 16
    chips = _oracle_and_check(b, 18, "quadbin")
    assert len(chips) > 2 * T._LABEL_CAPACITY


def test_bng_oracle_and_single_warp(monkeypatch):
    calls = []
    real = T._as_bng_dataset
    monkeypatch.setattr(T, "_as_bng_dataset", lambda ds: (calls.append(1), real(ds))[1])
    b = _raster(256, 256, crs="EPSG:27700", bounds=(530000, 180000, 535120, 185120))
    with MemoryFile(b) as mf, mf.open() as ds:
        list(T.iter_tessellate(ds, 3, "bng", mode="centroid", coverage="complete"))
    assert len(calls) == 1
    _oracle_and_check(b, 3, "bng")  # 3 = 1 km cells


def test_bng_from_another_crs_reprojects_once(monkeypatch):
    warps = []
    real = T.warp.reproject_to_srid
    monkeypatch.setattr(
        T.warp, "reproject_to_srid", lambda *a, **k: (warps.append(1), real(*a, **k))[1]
    )
    b = _raster(128, 128, bounds=(-0.2, 51.4, -0.1, 51.5))  # EPSG:4326, London
    with MemoryFile(b) as mf, mf.open() as ds:
        cells = [
            c
            for c, _ in T.iter_tessellate(
                ds, 3, "bng", mode="centroid", coverage="complete"
            )
        ]
    assert cells
    assert len(warps) == 1


def test_custom_grid_oracle():
    conf = CustomGridConf(
        bound_x_min=0,
        bound_x_max=200,
        bound_y_min=0,
        bound_y_max=200,
        cell_splits=2,
        root_cell_size_x=200,
        root_cell_size_y=200,
        srid=-1,
    )
    # Grid-native, CRS-less; the west and north 50 m fall outside the grid (dropped).
    b = _raster(
        96,
        96,
        crs=None,
        transform=from_origin(-50.0, 250.0, 3.125, 3.125),
        hole=(60, 70, 0, 96),
    )
    _oracle_and_check(b, 2, "custom", conf=conf)


def _virtual_tile_for(path):
    """A path-backed (virtual) tile over the whole file, as the open_tile tests build one."""
    with rasterio.open(path) as src:
        width, height = src.width, src.height
    return VirtualTile(cellid=0, path=path, window=(0, 0, width, height))


def test_virtual_tile_gives_the_same_chips(tmp_path):  # review focus 3
    b = _raster(600, 600)
    p = tmp_path / "v.tif"
    p.write_bytes(b)
    with MemoryFile(b) as mf, mf.open() as ds:
        mat = [
            (c, x)
            for c, x in T.iter_tessellate(
                ds, 7, "h3", mode="centroid", coverage="sparse"
            )
        ]
    vtile = _virtual_tile_for(str(p))
    with ot._open(vtile) as ds:  # open_tile._open(tile, file_ref=None)
        virt = [
            (c, x)
            for c, x in T.iter_tessellate(
                ds, 7, "h3", mode="centroid", coverage="sparse"
            )
        ]
    assert [c for c, _ in mat] == [c for c, _ in virt]
    for (_, a), (_, b2) in zip(mat, virt):
        with (
            MemoryFile(a) as m1,
            m1.open() as c1,
            MemoryFile(b2) as m2,
            m2.open() as c2,
        ):
            assert np.array_equal(c1.read(), c2.read()) and c1.transform == c2.transform
