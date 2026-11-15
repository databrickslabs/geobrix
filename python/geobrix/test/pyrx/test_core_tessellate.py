"""Light raster tessellation (`iter_tessellate`): one table of (grid x assignment x coverage x data) rows.

Each row tessellates a small raster on one grid and judges the chips against definitions the function does not use:

* a valid pixel belongs to the cell its centre is in (the grid library's own point lookup);
* the cells a raster overlaps are the cells whose square or hexagon shares POSITIVE AREA with its extent, found by
  walking the cells near the extent and taking each polygon from the grid library (an edge-only touch is not an overlap);
* `centroid` + `sparse` emits the cells holding a valid pixel centre, each with exactly the pixels there;
  `centroid` + `complete` and `covering` + `complete` emit every overlapping cell, empty ones included;
  `covering` + `sparse` drops the cells whose chip holds no valid pixel.

Per grid the table has the raster and resolution it runs on: H3 and quadbin on a 4326 raster over London, BNG on a
1 km-misaligned EPSG:27700 window (the buffer-before-polyfill trap: boundary cells whose centre is outside the window),
and the custom grid on a CRS-less raster with bounds 0..200. The raster is run valid and as all NoData.
The footprint helpers, chip windows, NoData and alpha handling are in the other tessellate test files.
"""

import h3
import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile
from shapely.geometry import box

from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tessellate as T

from . import _grid_defs as gd
from ._grid_defs import CUSTOM_CONF, GRIDS

# grid -> (crs, bounds of the raster, pixels a side, resolution, step of the walk over the extent)
ROWS = {
    "h3": ("EPSG:4326", (-0.12, 51.20, 0.20, 51.52), 64, 7, 0.004),
    "quadbin": ("EPSG:4326", (-0.12, 51.20, 0.20, 51.52), 64, 12, 0.02),
    "bng": ("EPSG:27700", (529500, 179500, 531500, 181500), 32, 3, 400),
    "custom": (None, (0, 0, 200, 200), 32, 1, 40),
}


def _gtiff(crs, bounds, size, empty):
    holes = [(1, r, c) for r in range(size) for c in range(size)] if empty else []
    return gd.gtiff(crs, bounds, size, holes=holes)


def _overlapping_cells(grid):
    _, bounds, _, res, step = ROWS[grid]
    return gd.overlapping_cells(grid, res, bounds, step)


def _pixel_cells(grid, data, bounds):
    return {
        c: sum(v)
        for c, v in gd.pixel_values_by_cell(grid, ROWS[grid][3], data, bounds).items()
    }


def _run(grid, mode, coverage, empty):
    crs, bounds, size, res, _ = ROWS[grid]
    raw = _gtiff(crs, bounds, size, empty)
    kwargs = {"conf": CUSTOM_CONF} if grid == "custom" else {}
    with MemoryFile(raw) as mf, mf.open() as ds:
        data = ds.read()
        chips = list(
            T.iter_tessellate(ds, res, grid, mode=mode, coverage=coverage, **kwargs)
        )
    return data, chips


def _chip_sum_and_count(raster):
    with _serde.open_tile(raster) as chip:
        band = chip.read(1, masked=True)
        return (float(band.sum()) if band.count() else 0.0), int(band.count()), chip


@pytest.mark.parametrize("empty", [False, True], ids=["valid", "all_nodata"])
@pytest.mark.parametrize("coverage", ["sparse", "complete"])
@pytest.mark.parametrize("grid", GRIDS)
def test_centroid_assigns_each_valid_pixel_to_the_cell_its_centre_is_in(
    grid, coverage, empty
):
    data, chips = _run(grid, "centroid", coverage, empty)
    ids = [c for c, _ in chips]
    assert len(ids) == len(set(ids))
    want = _pixel_cells(grid, data, ROWS[grid][1])
    got = {c: _chip_sum_and_count(r)[0] for c, r in chips}
    if coverage == "sparse":
        assert set(got) == set(want)
    else:
        assert set(got) == set(want) | _overlapping_cells(grid)
    for cell, total in got.items():
        assert total == pytest.approx(want.get(cell, 0.0)), cell


@pytest.mark.parametrize("empty", [False, True], ids=["valid", "all_nodata"])
@pytest.mark.parametrize("coverage", [None, "complete", "sparse"])
@pytest.mark.parametrize("grid", GRIDS)
def test_covering_emits_every_cell_with_positive_overlap(grid, coverage, empty):
    data, chips = _run(grid, "covering", coverage, empty)
    ids = [c for c, _ in chips]
    assert len(ids) == len(set(ids))
    overlapping = _overlapping_cells(grid)
    if coverage == "sparse":
        # a cell is dropped when its chip has no valid pixel; with data every overlapping cell holds one
        assert set(ids) == (set() if empty else overlapping)
    else:
        assert (
            set(ids) == overlapping
        ), f"missing {sorted(overlapping - set(ids))[:3]}, extra {sorted(set(ids) - overlapping)[:3]}"
    extent = box(*ROWS[grid][1])
    for cell, raster in chips:
        total, valid, chip = _chip_sum_and_count(raster)
        assert chip.width > 0 and chip.height > 0
        assert valid == 0 if empty else (valid > 0 or coverage != "sparse")
        # a chip lies within the raster extent, and within its cell's square or hexagon
        assert (
            box(*chip.bounds)
            .difference(extent.buffer(1e-6 * max(extent.bounds[2:])))
            .area
            == 0
        )
        if grid == "bng":
            assert _bng.format(_bng.parse(cell)) == cell
        else:
            assert isinstance(cell, int)
    if grid == "h3":
        assert all(h3.is_valid_cell(h3.int_to_str(c)) for c in ids)


def test_a_cell_that_touches_the_grid_aligned_extent_only_on_its_edge_is_not_emitted():
    # 2 km x 2 km window on the 1 km grid: four whole cells; the cell west of it shares only the line x = 529000
    res = _bng.get_resolution("1km")
    raw = _gtiff("EPSG:27700", (529000, 179000, 531000, 181000), 32, False)
    with MemoryFile(raw) as mf, mf.open() as ds:
        ids = {c for c, _ in T.iter_tessellate(ds, "1km", "bng", mode="covering")}
    west = _bng.format(_bng.point_to_cell_id(528500, 179500, res))
    assert len(ids) == 4 and west not in ids


def test_a_4326_raster_over_great_britain_is_warped_to_27700_for_the_bng_grid():
    raw = _gtiff("EPSG:4326", (-0.12, 51.36, 0.20, 51.52), 64, False)
    with MemoryFile(raw) as mf, mf.open() as ds:
        chips = list(T.iter_tessellate(ds, "1km", "bng", mode="centroid"))
    assert chips
    for cell, raster in chips:
        with _serde.open_tile(raster) as chip:
            assert chip.crs.to_epsg() == 27700
            assert chip.count == 1 and _bng.format(_bng.parse(cell)) == cell


@pytest.mark.parametrize("grid", GRIDS)
def test_a_mode_that_is_not_covering_or_centroid_raises(grid):
    crs, bounds, size, res, _ = ROWS[grid]
    kwargs = {"conf": CUSTOM_CONF} if grid == "custom" else {}
    with MemoryFile(_gtiff(crs, bounds, size, False)) as mf, mf.open() as ds:
        with pytest.raises(
            ValueError, match="mode must be one of covering, centroid; got 'bad'"
        ):
            list(T.iter_tessellate(ds, res, grid, mode="bad", **kwargs))
        with pytest.raises(
            ValueError, match="coverage must be one of sparse, complete; got 'bad'"
        ):
            list(
                T.iter_tessellate(
                    ds, res, grid, mode="centroid", coverage="bad", **kwargs
                )
            )


def test_a_grid_that_is_not_h3_quadbin_bng_or_custom_raises():
    with MemoryFile(_gtiff(*ROWS["h3"][:3], False)) as mf, mf.open() as ds:
        with pytest.raises(ValueError, match="grid must be one of"):
            list(T.iter_tessellate(ds, 9, "xyz"))


@pytest.mark.parametrize("res", [-1, 16])
def test_an_h3_resolution_outside_0_to_15_raises(res):
    with MemoryFile(_gtiff(*ROWS["h3"][:3], False)) as mf, mf.open() as ds:
        with pytest.raises(ValueError):
            T.tessellate_h3(ds, res)


def test_covering_chips_of_a_source_without_nodata_mark_the_fill_as_nodata():
    """Covering chips of a source without NoData carry the dtype fallback (65535).

    The clip fill outside each hexagon is then NoData, not valid zero pixels: every
    valid chip pixel is a source pixel with the source's value, and the source's
    real zero rows stay valid.
    """
    size = 64
    data = (np.arange(size * size, dtype="uint32") % 4000 + 1).astype("uint16")
    data = data.reshape(size, size)
    data[:4, :] = 0
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(-0.1, 51.5, 0.002, 0.002),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        with mf.open() as ds:
            inv = ~ds.transform
            chips = list(
                T.iter_tessellate(ds, 7, "h3", mode="covering", coverage="complete")
            )
    assert len(chips) > 1
    filled = zeros = 0
    for _, chip in chips:
        with _serde.open_tile(chip) as c:
            assert c.nodata == 65535.0
            valid = c.read_masks(1) != 0
            filled += int((~valid).sum())
            rows, cols = np.nonzero(valid)
            xs, ys = c.transform * (cols + 0.5, rows + 0.5)
            sc, sr = inv * (xs, ys)
            sr, sc = np.floor(sr).astype(int), np.floor(sc).astype(int)
            assert ((sr >= 0) & (sr < size) & (sc >= 0) & (sc < size)).all()
            values = c.read(1)[rows, cols]
            assert (values == data[sr, sc]).all(), "a fill pixel reads as valid data"
            zeros += int((values == 0).sum())
    assert filled > 0, "fixture must clip some pixels outside a hexagon"
    assert zeros >= 4 * size, "the real zero rows must stay valid"


@pytest.mark.parametrize("mode", [None, "covering", "centroid"])
@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
def test_the_per_grid_iterator_is_the_generic_one_with_its_grid_and_covering_by_default(
    grid, mode
):
    crs, bounds, size, res, _ = ROWS[grid]
    wrapper = getattr(T, f"iter_tessellate_{grid}")
    kwargs = {} if mode is None else {"mode": mode}
    with MemoryFile(_gtiff(crs, bounds, size, False)) as mf, mf.open() as ds:
        got = list(wrapper(ds, res, **kwargs))
        want = list(T.iter_tessellate(ds, res, grid, mode=mode or "covering"))
    assert got == want and got


def test_tessellate_h3_lists_the_covering_chips():
    crs, bounds, size, res, _ = ROWS["h3"]
    with MemoryFile(_gtiff(crs, bounds, size, False)) as mf, mf.open() as ds:
        got = T.tessellate_h3(ds, res)
        want = list(T.iter_tessellate(ds, res, "h3", mode="covering"))
    assert isinstance(got, list) and got == want and got


@pytest.mark.parametrize("res", [3, "1km"])
def test_the_bng_iterator_takes_an_index_or_a_name_for_the_resolution(res):
    crs, bounds, size, _, _ = ROWS["bng"]
    with MemoryFile(_gtiff(crs, bounds, size, False)) as mf, mf.open() as ds:
        ids = {c for c, _ in T.iter_tessellate_bng(ds, res)}
    assert ids == _overlapping_cells("bng")
