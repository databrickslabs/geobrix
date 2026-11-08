import h3
import pytest

from databricks.labs.gbx.pygx import _bng, _quadbin
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import cellraster as cr
from databricks.labs.gbx.pyrx.core import gridagg

from . import _duplicate_cells as dc


def _cell(lat, lon, res=9):
    return h3.str_to_int(h3.latlng_to_cell(lat, lon, res))


def test_h3_str_normalizes_signed_long():
    cid_unsigned = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, 9))
    signed = cid_unsigned - (1 << 64) if cid_unsigned >= (1 << 63) else cid_unsigned
    assert cr._h3_str(signed) == cr._h3_str(cid_unsigned) == h3.int_to_str(cid_unsigned)


def test_compute_gridspec_single_cell_centroids_uses_kring1():
    cid = _cell(0.0, 0.0, 9)
    # kring_pad=1 (default): a single cell is non-degenerate (neighbor centroids)
    xmin, ymin, xmax, ymax, px, w, h, srid = cr.compute_gridspec([cid])
    assert w >= 3 and h >= 3 and srid == 4326
    assert xmax > xmin and ymax > ymin
    # kring_pad=0: degenerate -> 1x1 (centroid only)
    g0 = cr.compute_gridspec([cid], kring_pad=0)
    assert g0[5] == 1 and g0[6] == 1


def test_compute_gridspec_origin_snapped_to_lattice():
    cid = _cell(10.0, 20.0, 9)
    xmin, ymin, xmax, ymax, px, w, h, srid = cr.compute_gridspec([cid], pixel_size=0.01)
    # origin is an integer multiple of pixel_size -> independently-built grids align
    assert abs((xmin / 0.01) - round(xmin / 0.01)) < 1e-9
    assert abs((ymax / 0.01) - round(ymax / 0.01)) < 1e-9


def test_compute_gridspec_rejects_mixed_resolution():
    import pytest

    a = _cell(0.0, 0.0, 9)
    b = _cell(0.0, 0.0, 8)
    with pytest.raises(ValueError, match="resolution"):
        cr.compute_gridspec([a, b])


def test_cells_to_raster_partition_property():
    # polyfill a small area -> cells; rasterize as presence mask; every burned
    # pixel centroid must re-index to a cell IN the set, every NoData pixel must not.
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = {h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)}
    cell_values = {c: 1.0 for c in cells}
    g = cr.compute_gridspec(list(cells), kring_pad=1)
    raster = cr.cells_to_raster(cell_values, *g, resolution=res)
    with _serde.open_tile(raster) as ds:
        arr = ds.read(1)
        t = ds.transform
        nod = ds.nodata
        cellset = {cr._h3_str(c) for c in cells}
        for row in range(ds.height):
            for col in range(ds.width):
                lon, lat = t * (col + 0.5, row + 0.5)
                idx = h3.latlng_to_cell(lat, lon, res)
                burned = arr[row, col] != nod
                assert burned == (idx in cellset)


def _bng_reference(cell_values, xmin, ymax, px, width, height, res):
    """The scalar loop `cells_to_raster` used to run, kept here as the reference: one `point_to_cell_id` a pixel."""
    import numpy as np

    from databricks.labs.gbx.pygx import _bng

    lut = {_bng.parse(c): float(v) for c, v in cell_values.items()}
    out = np.full((height, width), -9999.0)
    for r in range(height):
        for c in range(width):
            v = lut.get(
                _bng.point_to_cell_id(xmin + (c + 0.5) * px, ymax - (r + 0.5) * px, res)
            )
            if v is not None:
                out[r, c] = v
    return out


@pytest.mark.parametrize(
    "xmin,ymax,px,width,height,nan_every",
    [
        (530000.0, 182000.0, 100.0, 40, 30, 0),  # cells and canvas on the same grid
        (
            -3000.0,
            1500.0,
            100.0,
            100,
            60,
            7,
        ),  # west and south of the grid, with NaN values
        (
            531500.0,
            181500.0,
            250.0,
            30,
            30,
            0,
        ),  # a canvas that overlaps the cells in part
    ],
    ids=["dense", "negative-with-nan", "partial-overlap"],
)
def test_the_bng_burn_is_one_index_per_band_not_one_per_pixel(
    monkeypatch, xmin, ymax, px, width, height, nan_every
):
    """The burn gives what a point-to-cell lookup per pixel gave, and never makes that lookup per pixel."""
    import numpy as np

    rng = np.random.default_rng(3)
    cells = {}
    for i in range(300):
        e, n = rng.integers(0, 10), rng.integers(0, 10)
        cell = f"TQ{30 + e:02d}{80 + n:02d}" if i % 3 else f"SV{e % 6:02d}{n % 6:02d}"
        cells[cell] = float("nan") if nan_every and i % nan_every == 0 else float(i)
    ad = cr._adapter("bng")
    want = _bng_reference(cells, xmin, ymax, px, width, height, 3)

    def per_pixel(*_a):
        raise AssertionError("one lookup per pixel")

    monkeypatch.setattr(type(ad), "pixel_key", per_pixel)
    raster = cr.cells_to_raster(
        cells,
        xmin,
        ymax - height * px,
        xmin + width * px,
        ymax,
        px,
        width,
        height,
        27700,
        3,
        grid="bng",
    )
    with _serde.open_tile(raster) as ds:
        got = ds.read(1)
        assert (
            ds.nodata == -9999.0 and ds.transform.c == xmin and ds.transform.f == ymax
        )
    assert np.array_equal(got, want, equal_nan=True)


def test_the_bng_burn_refuses_a_coordinate_that_is_not_finite():
    with pytest.raises(ValueError, match="Non-finite"):
        cr.cells_to_raster(
            {"TQ3080": 1.0},
            float("nan"),
            0.0,
            1000.0,
            1000.0,
            100.0,
            10,
            10,
            27700,
            3,
            grid="bng",
        )


def test_the_bng_burn_of_no_cells_is_all_nodata_and_still_refuses_a_canvas_that_is_not_finite():
    """No cells is no pixel burned, and a canvas with a NaN corner is refused all the same, as it was when every
    pixel was indexed one by one."""
    raster = cr.cells_to_raster(
        {}, 0.0, 0.0, 1000.0, 1000.0, 100.0, 10, 10, 27700, 3, grid="bng"
    )
    with _serde.open_tile(raster) as ds:
        assert (ds.read(1) == -9999.0).all()
    with pytest.raises(ValueError, match="Non-finite"):
        cr.cells_to_raster(
            {}, float("nan"), 0.0, 1000.0, 1000.0, 100.0, 10, 10, 27700, 3, grid="bng"
        )


def _one_cell():
    cell = h3.str_to_int(h3.latlng_to_cell(37.77, -122.42, 8))
    return cell, cr._adapter("h3")


def test_a_canvas_given_in_part_is_refused_naming_the_null_arguments():
    cell, ad = _one_cell()
    with pytest.raises(ValueError, match=r"given in part \(xmin, width\).*ymin.*Fix:"):
        cr.rasterize_cells(
            {cell: 1.0},
            [cell],
            ad,
            "h3",
            4326,
            None,
            -123.0,
            None,
            None,
            None,
            10,
            None,
            "centroids",
            0,
        )


def test_the_resolved_canvas_is_handed_to_the_caller_before_the_burn(monkeypatch):
    cell, ad = _one_cell()
    seen = []

    def refuse(w, h):
        seen.append((w, h))
        raise RuntimeError("stop")

    monkeypatch.setattr(
        cr, "cells_to_raster", lambda *a, **k: pytest.fail("the burn ran")
    )
    with pytest.raises(RuntimeError, match="stop"):
        cr.rasterize_cells(
            {cell: 1.0},
            [cell],
            ad,
            "h3",
            4326,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "centroids",
            0,
            check_canvas=refuse,
        )
    assert seen and seen[0][0] > 0 and seen[0][1] > 0


def test_a_null_is_none_nan_or_pandas_na_and_nothing_else():
    import pandas as pd

    assert all(cr.is_missing(v) for v in (None, float("nan"), pd.NA))
    assert not any(cr.is_missing(v) for v in (0, 0.0, "centroids", -9999.0))
    # the NA-safe check keeps cell_out_srid and the canvas rule working on pd.NA, which has no truth value
    assert cr.cell_out_srid("fn", pd.NA, pd.NA) == 4326


_RES = {"h3": 9, "quadbin": 12, "bng": 3}


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
def test_cells_burn_to_a_raster_that_reduces_back_to_the_same_values(grid):
    """A centroid burn is the inverse of the pixel-centre reducer: avg over a cell's pixels is its burned value."""
    cells = dc.GRIDS[grid]()
    res = _RES[grid]
    values = {c: float(100 + i) for i, c in enumerate(cells)}
    srid = 27700 if grid == "bng" else 4326
    spec = cr.compute_gridspec(cells, srid=srid, kring_pad=0, grid=grid)
    assert spec[7] == srid
    raster = cr.cells_to_raster(values, *spec, resolution=res, grid=grid)
    with _serde.open_tile(raster) as ds:
        assert ds.nodata == -9999.0 and ds.crs.to_epsg() == srid
        back = gridagg.raster_to_grid(
            ds, res, grid, "avg", coverage="sparse", assignment="centroid"
        )[0]
    got = {r["cellID"]: r["measure"] for r in back}
    for c, v in values.items():
        key = c if grid == "bng" else int(c) & cr._U64 if grid == "quadbin" else c
        assert got[key] == pytest.approx(v, abs=1e-9), c


@pytest.mark.parametrize(
    "grid, other",
    [
        ("h3", lambda: _cell(0.0, 0.0, 8)),
        ("quadbin", lambda: _quadbin.point_as_cell(-0.1, 51.5, 11)),
        ("bng", lambda: _bng.point_to_cell_id(530000, 180000, 4)),
    ],
)
def test_a_gridspec_of_cells_at_two_resolutions_is_refused(grid, other):
    cell = dc.GRIDS[grid]()[0]
    if grid == "bng":
        cell = _bng.parse(cell)
    with pytest.raises(ValueError, match="resolution"):
        cr.compute_gridspec([cell, other()], grid=grid)
