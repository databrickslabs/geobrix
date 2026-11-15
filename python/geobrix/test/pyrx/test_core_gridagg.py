"""Raster -> grid aggregation (`gridagg.raster_to_grid`): one table of (grid x coverage x assignment x statistic) rows.

Each row aggregates a small two-band raster on one grid and judges every (band, cell) against definitions the function
does not use:

* centroid: a valid pixel belongs to the cell its centre is in, and numpy reduces the values there (population variance
  and standard deviation, the median of the values); `complete` coverage adds the cells that share positive area with
  the raster and hold no valid pixel, with a NULL measure (0.0 for a count); `sparse` adds none;
* covering: every valid pixel is shared out over the cells it overlaps by area (the share of the pixel in the cell,
  from shapely), a count adds up to the valid pixels, a sum, mean, variance and standard deviation are weighted by the
  shares, and a minimum and maximum take every value that touches the cell.

Per grid the raster straddles cell boundaries both ways, and has NoData holes that differ by band. The BNG ids are text,
the others integers; a count is a float. The BNG warp, the GB edge and the alias of a resolution are separate rows.
"""

from dataclasses import asdict

import numpy as np
import pytest
from rasterio.io import MemoryFile
from shapely.geometry import box

from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.pyrx.core import gridagg

from . import _grid_defs as gd
from ._grid_defs import CUSTOM_CONF, GRIDS, NODATA

STATS = ["avg", "count", "max", "median", "min", "stddev", "sum", "variance"]
HOLES = [(1, 0, 1), (1, 5, 5), (2, 2, 2), (2, 2, 3), (2, 9, 9)]

# grid -> (crs, bounds, pixels a side, resolution, step of the walk over the extent)
ROWS = {
    "h3": ("EPSG:4326", (-0.100, 51.500, -0.052, 51.548), 12, 7, 0.004),
    "quadbin": ("EPSG:4326", (-0.100, 51.500, -0.052, 51.548), 12, 12, 0.01),
    "bng": ("EPSG:27700", (529600, 179600, 530560, 180560), 12, 3, 200),
    "custom": (None, (20, 20, 164, 164), 12, 1, 20),
}


def _raster(grid):
    crs, bounds, size, _, _ = ROWS[grid]
    return gd.gtiff(crs, bounds, size, bands=2, holes=HOLES)


def _aggregate(grid, stat, coverage, assignment, res=None):
    crs, bounds, size, default_res, _ = ROWS[grid]
    res = res or default_res
    conf = asdict(CUSTOM_CONF) if grid == "custom" else None
    with MemoryFile(_raster(grid)) as mf, mf.open() as ds:
        data = ds.read()
        out = gridagg.raster_to_grid(
            ds,
            res,
            grid,
            stat,
            coverage=coverage,
            assignment=assignment,
            grid_conf=conf,
        )
    return data, out


def _reduce(stat, v):
    v = np.asarray(v, dtype="float64")
    return {
        "avg": v.mean(),
        "count": float(v.size),
        "max": v.max(),
        "median": float(np.median(v)),
        "min": v.min(),
        "stddev": v.std(),
        "sum": v.sum(),
        "variance": v.var(),
    }[stat]


def _weighted(stat, pairs):
    """The covering statistic of [(value, share of the pixel in the cell)]."""
    v = np.array([p[0] for p in pairs])
    w = np.array([p[1] for p in pairs])
    mean = (v * w).sum() / w.sum()
    if stat == "median":
        order = np.argsort(v, kind="stable")
        v, w = v[order], w[order]
        cum = np.cumsum(w)
        i = int(np.searchsorted(cum, w.sum() / 2 - 1e-12))
        if i < len(v) - 1 and abs(cum[i] - w.sum() / 2) < 1e-9:
            return float((v[i] + v[i + 1]) / 2)
        return float(v[i])
    return {
        "avg": mean,
        "count": w.sum(),
        "max": v.max(),
        "min": v.min(),
        "stddev": np.sqrt((w * (v - mean) ** 2).sum() / w.sum()),
        "sum": (v * w).sum(),
        "variance": (w * (v - mean) ** 2).sum() / w.sum(),
    }[stat]


def _covering_shares(grid, data, band):
    """{cell: [(value, area share of the pixel in the cell)]} for the valid pixels of a band."""
    crs, (minx, miny, maxx, maxy), size, res, _ = ROWS[grid]
    out = {}
    for r in range(size):
        for c in range(size):
            v = float(data[band, r, c])
            if v == NODATA:
                continue
            px = box(
                minx + c * (maxx - minx) / size,
                maxy - (r + 1) * (maxy - miny) / size,
                minx + (c + 1) * (maxx - minx) / size,
                maxy - r * (maxy - miny) / size,
            )
            x, y = px.centroid.coords[0]
            corners = {
                gd.cell_of(grid, res, *xy)
                for xy in list(px.exterior.coords)[:4] + [(x, y)]
            }
            for cell in {n for k in corners for n in gd._near(grid, res, k)} | corners:
                share = px.intersection(gd.polygon_of(grid, res, cell)).area / px.area
                if share > 1e-12:
                    out.setdefault(cell, []).append((v, share))
    return out


def _got(band):
    keys = [c["cellID"] for c in band]
    assert len(keys) == len(set(keys))
    return {c["cellID"]: c["measure"] for c in band}


@pytest.mark.parametrize("stat", STATS)
@pytest.mark.parametrize("coverage", ["sparse", "complete"])
@pytest.mark.parametrize("grid", GRIDS)
def test_centroid_reduces_the_valid_pixels_of_each_cell(grid, coverage, stat):
    _, bounds, _, res, step = ROWS[grid]
    data, out = _aggregate(grid, stat, coverage, "centroid")
    assert len(out) == 2
    for b, band in enumerate(out):
        want = {
            cell: _reduce(stat, v)
            for cell, v in gd.pixel_values_by_cell(grid, res, data, bounds, b).items()
        }
        got = _got(band)
        empty = 0.0 if stat == "count" else None
        if coverage == "sparse":
            assert set(got) == set(want), f"band {b + 1}"
        else:
            extra = gd.overlapping_cells(grid, res, bounds, step) - set(want)
            assert set(got) == set(want) | extra
            assert all(got[c] == empty for c in set(got) - set(want))
        for cell, value in want.items():
            assert got[cell] == pytest.approx(value, rel=1e-6), (b + 1, cell)
        for c in band:
            assert isinstance(c["cellID"], str if grid == "bng" else int)
            assert c["measure"] is None or isinstance(c["measure"], float)


@pytest.mark.parametrize("stat", STATS)
@pytest.mark.parametrize("grid", GRIDS)
def test_covering_shares_each_pixel_out_over_the_cells_it_overlaps(grid, stat):
    data, out = _aggregate(grid, stat, "sparse", "covering")
    for b, band in enumerate(out):
        want = {
            cell: _weighted(stat, pairs)
            for cell, pairs in _covering_shares(grid, data, b).items()
        }
        got = _got(band)
        assert set(got) == set(want), f"band {b + 1}"
        for cell, value in want.items():
            assert got[cell] == pytest.approx(value, rel=1e-6, abs=1e-9), (b + 1, cell)
        if stat == "count":
            assert sum(got.values()) == pytest.approx(
                float((data[b] != NODATA).sum()), rel=1e-9
            )


@pytest.mark.parametrize("grid", GRIDS)
def test_covering_with_complete_coverage_adds_the_empty_cells(grid):
    data, sparse = _aggregate(grid, "count", "sparse", "covering")
    _, complete = _aggregate(grid, "count", "complete", "covering")
    _, bounds, _, res, step = ROWS[grid]
    for s, c in zip(sparse, complete):
        have = _got(s)
        got = _got(c)
        assert set(got) == set(have) | gd.overlapping_cells(grid, res, bounds, step)
        assert all(got[k] == 0.0 for k in set(got) - set(have))


def test_a_band_with_no_valid_pixel_has_no_cell_when_sparse_and_only_empty_cells_when_complete():
    holes = [(1, r, c) for r in range(12) for c in range(12)]
    raw = gd.gtiff("EPSG:27700", ROWS["bng"][1], 12, bands=2, holes=holes)
    with MemoryFile(raw) as mf, mf.open() as ds:
        sparse = gridagg.raster_to_grid(ds, 3, "bng", "avg", coverage="sparse")
        complete = gridagg.raster_to_grid(ds, 3, "bng", "avg", coverage="complete")
    assert sparse[0] == [] and sparse[1]
    assert complete[0] and all(c["measure"] is None for c in complete[0])


@pytest.mark.parametrize(
    "grid, fine", [("h3", 10), ("quadbin", 18), ("bng", 5), ("custom", 5)]
)
def test_a_cell_with_one_valid_pixel_has_no_spread(grid, fine):
    # cells smaller than a pixel: each holds at most one pixel centre, so every measure is its own value, spread 0
    data, var = _aggregate(grid, "variance", "sparse", "centroid", res=fine)
    _, total = _aggregate(grid, "count", "sparse", "centroid", res=fine)
    for b in range(2):
        valid = int((data[b] != NODATA).sum())
        assert {c["measure"] for c in var[b]} == {0.0}
        assert len(var[b]) == valid and sum(c["measure"] for c in total[b]) == valid


def test_a_resolution_the_grid_does_not_have_raises():
    raw = _raster("h3")
    with MemoryFile(raw) as mf, mf.open() as ds:
        for grid, res in (("h3", 16), ("h3", -1), ("quadbin", 21), ("quadbin", -1)):
            with pytest.raises(ValueError):
                gridagg.raster_to_grid(ds, res, grid, "count")
        with pytest.raises(ValueError, match="between 0 and 15"):
            gridagg.raster_to_grid(ds, 16, "h3", "count")
    with MemoryFile(raw) as mf, mf.open() as ds:
        with pytest.raises(ValueError):
            gridagg.raster_to_grid(ds, 7, "h3", "mode")
        with pytest.raises(ValueError):
            gridagg.raster_to_grid(ds, 7, "h3", "avg", coverage="bad")
        with pytest.raises(ValueError):
            gridagg.raster_to_grid(ds, 7, "h3", "avg", assignment="bad")


def test_the_bng_resolution_alias_is_the_index():
    with MemoryFile(_raster("bng")) as mf, mf.open() as ds:
        by_index = gridagg.raster_to_grid(ds, 3, "bng", "avg", coverage="sparse")
        by_name = gridagg.raster_to_grid(ds, "1km", "bng", "avg", coverage="sparse")
    assert by_index == by_name


def test_a_4326_raster_over_great_britain_is_warped_to_27700_for_the_bng_grid():
    raw = gd.gtiff("EPSG:4326", (-0.13, 51.49, -0.10, 51.52), 3, holes=[])
    with MemoryFile(raw) as mf, mf.open() as ds:
        band = gridagg.raster_to_grid(ds, 2, "bng", "count", coverage="sparse")[0]
    assert band and all(
        _bng.is_valid(_bng.parse(c["cellID"])) and isinstance(c["cellID"], str)
        for c in band
    )


def test_a_pixel_east_of_the_gb_grid_is_dropped():
    raw = gd.gtiff("EPSG:27700", (690000, 200000, 720000, 205000), 6, height=1)
    with MemoryFile(raw) as mf, mf.open() as ds:
        band = gridagg.raster_to_grid(ds, 2, "bng", "count", coverage="sparse")[0]
    want = set()
    for col in range(6):
        cid = _bng.point_to_cell_id(690000 + (col + 0.5) * 5000, 202500, 2)
        if _bng.is_valid(cid):
            want.add(_bng.format(cid))
    assert len(want) < 6 and {c["cellID"] for c in band} == want
