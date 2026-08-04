"""Task 3 (RasterX CRS-100, Group C): rst_{h3,quadbin}_rastertogrid* must
auto-reproject a differently-CRS'd raster to grid-native EPSG:4326 (instead of
silently treating easting/northing as lon/lat), with a `crs` override for a
CRS-less-but-known raster, and never error on a CRS-less raster.
"""

import h3
import numpy as np
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
        out = gridagg.raster_to_grid(ds, resolution=6, grid="h3", agg="count")
    cells = _cells_of(out)
    assert cells, "expected non-empty grid"
    # Decoded cell centroids must land in zone-33 lon range (~9-21E), NOT at a
    # nonsense lon derived from easting-as-lon.
    lons = [h3.cell_to_latlng(h3.int_to_str(c))[1] for c in cells]
    assert all(6.0 < lo < 24.0 for lo in lons), f"UTM-33N lon out of range: {lons[:3]}"


def test_quadbin_rastertogrid_utm_reprojects():
    b = _utm33n_raster_bytes()
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(ds, resolution=10, grid="quadbin", agg="count")
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
        out = gridagg.raster_to_grid(ds, resolution=6, grid="h3", agg="count")
    assert out is not None  # no exception; assumed grid-native


def test_h3_rastertogrid_crs_override_for_crsless():
    # A CRS-less raster whose true CRS is UTM-33N, supplied via the crs override.
    b = _utm33n_raster_bytes(crs=None)
    with MemoryFile(b) as mf, mf.open() as ds:
        out = gridagg.raster_to_grid(
            ds, resolution=6, grid="h3", agg="count", crs="EPSG:32633"
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
        out = gridagg.raster_to_grid(ds, resolution=6, grid="h3", agg="count")
    assert _cells_of(out)
