"""rst_{h3,quadbin}_rastertogrid* must
auto-reproject a differently-CRS'd raster to grid-native EPSG:4326 (instead of
silently treating easting/northing as lon/lat), with a `crs` override for a
CRS-less-but-known raster, and never error on a CRS-less raster.
"""

import h3
import numpy as np
import pytest
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import gridagg


def _utm33n_raster_bytes(crs=CRS.from_epsg(32633)):
    """A small raster in UTM 33N (or CRS-less if crs=None) over central Europe.

    Origin easting 500000, northing 5000000 is on the zone-33 central meridian
    (~15E, ~45N). If treated as lon/lat it would land at lon=500000 (nonsense).
    """
    transform = from_origin(500000.0, 5000000.0, 30.0, 30.0)
    profile = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.ones((1, 8, 8), dtype="float32"))
        return mf.read()


def _cells_of(out):
    return [c["cellID"] for band in out for c in band]


def test_h3_rastertogrid_utm_reprojects_not_silently_wrong():
    b = _utm33n_raster_bytes()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(
            ds,
            resolution=6,
            grid="h3",
            agg="count",
            coverage="sparse",
            assignment="centroid",
        )
    cells = _cells_of(out)
    assert cells, "expected non-empty grid"
    # Decoded cell centroids must land in zone-33 lon range (~9-21E), NOT at a
    # nonsense lon derived from easting-as-lon.
    lons = [h3.cell_to_latlng(h3.int_to_str(c))[1] for c in cells]
    assert all(6.0 < lo < 24.0 for lo in lons), f"UTM-33N lon out of range: {lons[:3]}"


def test_quadbin_rastertogrid_utm_reprojects():
    b = _utm33n_raster_bytes()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(
            ds,
            resolution=10,
            grid="quadbin",
            agg="count",
            coverage="sparse",
            assignment="centroid",
        )
    assert _cells_of(out), "expected non-empty quadbin grid after reprojection"


def test_h3_rastertogrid_crsless_assumes_4326_no_error():
    # A raster in geographic degrees but with NO crs set -> assume 4326, no raise.
    transform = from_origin(9.0, 50.0, 0.01, 0.01)
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs=None,
        transform=transform,
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.ones((1, 4, 4), dtype="float32"))
        b = mf.read()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(
            ds,
            resolution=6,
            grid="h3",
            agg="count",
            coverage="sparse",
            assignment="centroid",
        )
    assert out is not None  # no exception; assumed grid-native


def test_h3_rastertogrid_crs_override_for_crsless():
    # A CRS-less raster whose true CRS is UTM-33N, supplied via the crs override.
    b = _utm33n_raster_bytes(crs=None)
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(
            ds,
            resolution=6,
            grid="h3",
            agg="count",
            crs="EPSG:32633",
            coverage="sparse",
            assignment="centroid",
        )
    cells = _cells_of(out)
    assert cells, "expected non-empty grid with crs override"
    lons = [h3.cell_to_latlng(h3.int_to_str(c))[1] for c in cells]
    assert all(6.0 < lo < 24.0 for lo in lons), f"override lon out of range: {lons[:3]}"


def test_h3_rastertogrid_already_4326_unchanged():
    # A 4326 raster must NOT be warped (identity) and must still produce cells.
    transform = from_origin(9.0, 50.0, 0.01, 0.01)
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=transform,
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.ones((1, 4, 4), dtype="float32"))
        b = mf.read()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(
            ds,
            resolution=6,
            grid="h3",
            agg="count",
            coverage="sparse",
            assignment="centroid",
        )
    assert _cells_of(out)


# ---------------------------------------------------------------------------
# The warp to the grid CRS aggregates only the source's valid pixels
# ---------------------------------------------------------------------------

_N = 40
_FALLBACK_ROW = 3
_MASKED_ROWS = (10, 11)
# Norfolk, far from the EPSG:27700 central meridian: either warp rotates the grid by
# about 2.4 degrees and fills the corners.
_SRC_27700 = dict(crs="EPSG:27700", origin=(600000.0, 320000.0), px=100.0)
_SRC_4326 = dict(crs="EPSG:4326", origin=(1.00, 52.60), px=0.005)
# grid -> (source, grid CRS, resolution)
_WARP_GRIDS = {
    "h3": (_SRC_27700, "EPSG:4326", 9),
    "quadbin": (_SRC_27700, "EPSG:4326", 15),
    "bng": (_SRC_4326, "EPSG:27700", 3),
}
# label -> (masked, declared NoData)
_WARP_SOURCES = {
    "no-nodata": (False, None),
    "masked": (True, None),
    "masked-nodata-7": (True, 7),
}


def _warp_source_values():
    """Pixel p holds p + 1 (no 0, so 7 is pixel 6); row 3 holds 65535 (the fallback)."""
    vals = (np.arange(_N * _N) + 1).reshape(_N, _N).astype("uint16")
    vals[_FALLBACK_ROW, :] = 65535
    return vals


def _warp_source(*, crs, origin, px, masked, nodata):
    """uint16 GTiff of ``_warp_source_values`` with an internal per-dataset mask over
    ``_MASKED_ROWS`` when ``masked``; ``crs=None`` leaves it CRS-less."""
    import rasterio

    prof = dict(
        driver="GTiff",
        width=_N,
        height=_N,
        count=1,
        dtype="uint16",
        crs=crs,
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(**prof) as dst:
                dst.write(_warp_source_values(), 1)
                if masked:
                    mask = np.full((_N, _N), 255, dtype="uint8")
                    mask[list(_MASKED_ROWS), :] = 0
                    dst.write_mask(mask)
            return mf.read()


def _expected_warp_totals(src_crs, src, dst_crs, masked, nodata):
    """(count, sum) of the valid source pixels the nearest-neighbour warp maps: an
    index raster of the source (pixel p holds p + 1) warped to the same grid."""
    from rasterio.warp import Resampling, calculate_default_transform, reproject

    tf = from_origin(src["origin"][0], src["origin"][1], src["px"], src["px"])
    left, top = src["origin"]
    right, bottom = left + _N * src["px"], top - _N * src["px"]
    dst_tf, w, h = calculate_default_transform(
        src_crs, dst_crs, _N, _N, left, bottom, right, top
    )
    index = (np.arange(_N * _N) + 1).reshape(_N, _N).astype("uint32")
    out = np.zeros((h, w), dtype="uint32")
    reproject(
        index,
        out,
        src_transform=tf,
        src_crs=src_crs,
        dst_transform=dst_tf,
        dst_crs=dst_crs,
        resampling=Resampling.nearest,
        dst_nodata=0,
    )
    p = out[out > 0].astype("int64") - 1
    vals = _warp_source_values().ravel().astype("int64")[p]
    keep = vals != (65535 if nodata is None else nodata)
    if masked:
        keep &= ~np.isin(p // _N, _MASKED_ROWS)
    return int(keep.sum()), int(vals[keep].sum())


def _totals(b, grid, resolution, crs=None):
    """(count, sum, smallest value, largest value) the reducers aggregate, over all cells."""
    out = {}
    with MemoryFile(b) as mf, mf.open() as ds:
        for agg in ("count", "sum", "min", "max"):
            band = gridagg.raster_to_grid(
                ds, resolution, grid, agg, coverage="sparse", crs=crs
            )[0]
            out[agg] = [c["measure"] for c in band]
    return sum(out["count"]), sum(out["sum"]), min(out["min"]), max(out["max"])


@pytest.mark.parametrize("label", list(_WARP_SOURCES))
@pytest.mark.parametrize("grid", list(_WARP_GRIDS))
def test_rastertogrid_warp_aggregates_only_valid_source_pixels(grid, label):
    """A source in another CRS: the warp fill is not aggregated as zeros, nor are the
    masked pixels or the pixels equal to the NoData (the fallback 65535, or the
    source's own 7, which keeps 65535 a real value)."""
    src, dst_crs, resolution = _WARP_GRIDS[grid]
    masked, nodata = _WARP_SOURCES[label]
    b = _warp_source(**src, masked=masked, nodata=nodata)
    count, total, smallest, largest = _totals(b, grid, resolution)
    exp_count, exp_total = _expected_warp_totals(
        src["crs"], src, dst_crs, masked, nodata
    )
    assert smallest >= 1, f"{grid} {label}: a warp-fill 0 was aggregated"
    if nodata is None:
        assert largest <= _N * _N, f"{grid} {label}: a 65535 (or 65534) was aggregated"
    assert (
        count == exp_count
    ), f"{grid} {label}: {count} pixels aggregated, expected {exp_count}"
    assert total == exp_total, f"{grid} {label}: sum {total}, expected {exp_total}"


@pytest.mark.parametrize("grid", ["h3", "bng"])
def test_rastertogrid_crs_override_keeps_the_mask(grid):
    """A CRS-less masked source given its CRS by the ``crs`` override keeps its mask."""
    src, dst_crs, resolution = _WARP_GRIDS[grid]
    b = _warp_source(**{**src, "crs": None}, masked=True, nodata=None)
    count, total, smallest, _ = _totals(b, grid, resolution, crs=src["crs"])
    exp_count, exp_total = _expected_warp_totals(src["crs"], src, dst_crs, True, None)
    assert smallest >= 1, f"{grid}: a warp-fill 0 was aggregated"
    assert (count, total) == (exp_count, exp_total), f"{grid}: masked pixels aggregated"
