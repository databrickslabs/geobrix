"""Identity tests for the vectorized pyrx raster->grid aggregation.

These pin the vectorized ``gridagg`` against (a) the heavy point-to-cell rule
(``pygx._quadbin.point_as_cell``: floor and clamp), including a pixel centre on
the antimeridian and on tile edges, and (b) a faithful reference copy of the
original per-pixel double-loop implementation (identical output for every
grid x agg).

The downstream bench gates these functions on a SORTED cell-id hash plus
order-independent measure stats (``fingerprint_dggs_grid``), so cell ORDER does
not matter -- only the cell-id SET and the per-cell measures.
"""

import h3
import numpy as np
import pytest
import quadbin
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pygx import _quadbin
from databricks.labs.gbx.pyrx.core import gridagg


def _open(raster_bytes):
    return MemoryFile(raster_bytes).open()


def _custom_raster(data, nodata=-9999.0, epsg=4326, origin=(10.0, 50.0), px=0.5):
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


# --- reference copy of the ORIGINAL per-pixel loop (the thing we replaced) ---
def _reduce_ref(values, agg):
    arr = np.asarray(values, dtype="float64")
    if agg == "avg":
        return float(arr.mean())
    if agg == "count":
        return float(arr.size)  # float for heavy-tier parity (count is now Double)
    if agg == "min":
        return float(arr.min())
    if agg == "max":
        return float(arr.max())
    if agg == "median":
        return float(np.median(arr))
    raise ValueError(agg)


def _raster_to_grid_ref(ds, resolution, grid, agg):
    resolution = int(resolution)
    if grid == "h3":

        def cell_fn(lon, lat, res):
            return h3.str_to_int(h3.latlng_to_cell(lat, lon, res))

    else:

        def cell_fn(lon, lat, res):
            return _quadbin.point_as_cell(lon, lat, res)

    gt = ds.transform.to_gdal()
    out = []
    for bi in range(1, ds.count + 1):
        band = ds.read(bi).astype("float64")
        mask = ds.read_masks(bi)
        height, width = band.shape
        acc = {}
        for y in range(height):
            for x in range(width):
                if mask[y, x] == 0:
                    continue
                x_off = x + 0.5
                y_off = y + 0.5
                lon = gt[0] + x_off * gt[1] + y_off * gt[2]
                lat = gt[3] + x_off * gt[4] + y_off * gt[5]
                cid = cell_fn(lon, lat, resolution)
                acc.setdefault(cid, []).append(band[y, x])
        out.append(
            [
                {"cellID": cid, "measure": _reduce_ref(vals, agg)}
                for cid, vals in acc.items()
            ]
        )
    return out


def _as_map(band):
    """{cellID: measure} -- order-independent comparison (matches the bench)."""
    return {c["cellID"]: c["measure"] for c in band}


def _assert_band_equal(new_band, old_band, agg):
    new_map, old_map = _as_map(new_band), _as_map(old_band)
    # No duplicate cell ids (the group-reduce must collapse them).
    assert len(new_band) == len(new_map)
    assert set(new_map) == set(old_map)
    for cid, old_v in old_map.items():
        new_v = new_map[cid]
        assert type(new_v) is type(old_v), (agg, cid, type(new_v), type(old_v))
        assert np.isclose(new_v, old_v, rtol=0, atol=0) or np.isclose(new_v, old_v)


# --- 1. quadbin raster->grid uses heavy's point->cell rule -------------------
# Heavy ``Quadbin.pointToCellID`` and light tessellate floor the web-mercator tile
# and clamp it into [0, 2^z - 1] (``_quadbin.point_as_cell``). The CARTO library
# (``quadbin.point_to_cell``) instead wraps the tile x, so lon = 180 lands on tile
# 0, and rounds the tile y differently on an edge.
_EDGE_Z = 10
_EDGE_TILE_DEG = 360.0 / (1 << _EDGE_Z)  # 0.3515625: exact in binary


def _centres(ds):
    """Pixel-centre (lon, lat) of every pixel, as the aggregation computes them."""
    gt = ds.transform.to_gdal()
    return [
        (
            gt[0] + (c + 0.5) * gt[1] + (r + 0.5) * gt[2],
            gt[3] + (c + 0.5) * gt[4] + (r + 0.5) * gt[5],
        )
        for r in range(ds.height)
        for c in range(ds.width)
    ]


def _expected_counts(ds, z):
    exp = {}
    for lon, lat in _centres(ds):
        cid = _quadbin.point_as_cell(lon, lat, z)
        exp[cid] = exp.get(cid, 0.0) + 1.0
    return exp


def test_quadbin_raster_to_grid_tile_x_edges_and_antimeridian():
    # A point-registered 1025-column row: the centres sit exactly on every zoom-10
    # tile x edge, from lon = -180 to lon = +180 (the last column is the antimeridian).
    w = (1 << _EDGE_Z) + 1
    data = np.arange(1, w + 1, dtype="float32").reshape(1, w)
    raster = _custom_raster(
        data, origin=(-180.0 - _EDGE_TILE_DEG / 2, 10.0), px=_EDGE_TILE_DEG
    )
    with _open(raster) as ds:
        centres = _centres(ds)
        assert centres[0][0] == -180.0 and centres[-1][0] == 180.0
        exp = _expected_counts(ds, _EDGE_Z)
        got = gridagg.raster_to_grid(
            ds, _EDGE_Z, "quadbin", "count", coverage="sparse", assignment="centroid"
        )
    assert {c["cellID"]: c["measure"] for c in got[0]} == exp
    # lon = 180 is the easternmost tile (n - 1), never the wrapped tile 0.
    east_cell = _quadbin.point_as_cell(180.0, 10.0, _EDGE_Z)
    assert quadbin.cell_to_tile(east_cell)[0] == (1 << _EDGE_Z) - 1
    assert east_cell in {c["cellID"] for c in got[0]}


def test_quadbin_raster_to_grid_tile_y_edges():
    # One pixel per zoom-10 tile y edge (the latitude where web-mercator row k
    # starts), centre exactly on it. Compare to heavy's rule, and make sure the
    # fixture reaches edges where the CARTO library disagrees (else the test is vacuous).
    n = 1 << _EDGE_Z
    px = 2.0**-20
    differing = 0
    for k in range(1, n, 17):
        lat = np.degrees(np.arctan(np.sinh(np.pi * (1.0 - 2.0 * k / n))))
        raster = _custom_raster(
            np.ones((1, 1), dtype="float32"),
            origin=(10.0 - px / 2, float(lat) + px / 2),
            px=px,
        )
        with _open(raster) as ds:
            lon_c, lat_c = _centres(ds)[0]
            exp = _expected_counts(ds, _EDGE_Z)
            got = gridagg.raster_to_grid(
                ds,
                _EDGE_Z,
                "quadbin",
                "count",
                coverage="sparse",
                assignment="centroid",
            )
        assert {c["cellID"]: c["measure"] for c in got[0]} == exp, (k, lat_c)
        differing += quadbin.point_to_cell(lon_c, lat_c, _EDGE_Z) != next(iter(exp))
    assert differing > 0, "no tile-y edge distinguishes the CARTO rule from heavy's"


# --- 2. raster_to_grid identity: new == old, every grid x agg ----------------
@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["avg", "count", "min", "max", "median"])
def test_raster_to_grid_identity_synthetic(grid, agg):
    rng = np.random.default_rng(7)
    data = rng.normal(100.0, 20.0, size=(13, 17)).astype("float32")
    data[2, 3] = -9999.0  # nodata pixels exercising the mask==0 skip
    data[7, 11] = -9999.0
    data[0, 0] = -9999.0
    raster = _custom_raster(data, px=0.25)
    for res in (3, 6, 9):
        with _open(raster) as ds:
            new = gridagg.raster_to_grid(
                ds, res, grid, agg, coverage="sparse", assignment="centroid"
            )
        with _open(raster) as ds:
            old = _raster_to_grid_ref(ds, res, grid, agg)
        assert len(new) == len(old) == 1
        _assert_band_equal(new[0], old[0], agg)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["avg", "count", "min", "max", "median"])
def test_raster_to_grid_identity_multiband(grid, agg):
    rng = np.random.default_rng(11)
    b1 = rng.uniform(0, 50, size=(9, 9)).astype("float32")
    b2 = rng.uniform(50, 99, size=(9, 9)).astype("float32")
    b1[1, 1] = -9999.0
    profile = dict(
        driver="GTiff",
        width=9,
        height=9,
        count=2,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.3, 0.3),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(b1, 1)
            dst.write(b2, 2)
        raster = mf.read()
    with _open(raster) as ds:
        new = gridagg.raster_to_grid(
            ds, 7, grid, agg, coverage="sparse", assignment="centroid"
        )
    with _open(raster) as ds:
        old = _raster_to_grid_ref(ds, 7, grid, agg)
    assert len(new) == len(old) == 2
    for nb, ob in zip(new, old):
        _assert_band_equal(nb, ob, agg)
