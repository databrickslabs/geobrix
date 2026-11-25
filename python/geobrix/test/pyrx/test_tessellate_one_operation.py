"""Tessellate moves pixels to lon/lat and cells back by one operation: in a few CRSs PROJ moves points to EPSG:4326
and back by two operations metres apart, and a covering chip then missed pixels of its own cell.
"""

from test.pyrx.test_rastertogrid_native_crs import _open, _tif

import numpy as np
import pytest
from rasterio.transform import from_origin

from databricks.labs.gbx.core.crs import get_transformer

# --- tessellate: one operation for both ways ------------------------------------------------------------------------------


@pytest.mark.parametrize("lon,lat", [(4.0, 59.0), (3.0, 44.0)])
def test_tessellate_covering_chips_hold_the_pixels_of_their_cells_centroid_chips(
    lon, lat
):
    # EPSG:23031 (ED50 / UTM 31N): PROJ moves points to lon/lat and back by different operations there, 2-3 m apart.
    # The covering chips were cut with cells moved back by GDAL's own transform while the footprint and the centroid
    # chips moved pixels forward, so 12-19% of the pixels were missing from their own cell's covering chip at 0.5 m
    from databricks.labs.gbx.pyrx.core import tessellate as T

    crs, res, n = "EPSG:23031", 12, 60
    x0, y0 = get_transformer(4326, crs).transform(lon, lat)
    data = (np.arange(n * n).reshape(n, n) + 1).astype("float64")
    b = _tif(data, crs, from_origin(round(x0), round(y0), 0.5, 0.5))

    def values(mode):
        out = {}
        with _open(b) as ds:
            for cid, raster in T.iter_tessellate(
                ds, res, "h3", mode=mode, coverage="sparse"
            ):
                with _open(raster) as chip:
                    out[cid] = set(
                        chip.read(1)[chip.read_masks(1) != 0].astype(int).tolist()
                    )
        return out

    centroid, covering = values("centroid"), values("covering")
    assert sum(len(v) for v in centroid.values()) == n * n
    missing = sum(len(v - covering.get(cid, set())) for cid, v in centroid.items())
    assert missing == 0


def _chip_pixels(b, grid, res, mode):
    from databricks.labs.gbx.pyrx.core import tessellate as T

    n = 0
    with _open(b) as ds:
        for _, raster in T.iter_tessellate(ds, res, grid, mode=mode, coverage="sparse"):
            with _open(raster) as chip:
                n += int((chip.read_masks(1) != 0).sum())
    return n


def test_tessellate_centroid_drops_pixels_outside_the_grid_as_rastertogrid_does():
    # a centre south of BNG's origin (northing < 0) or beyond quadbin's 85.05 degrees is in no cell: before, the
    # encoders put it in the first row (BNG 64 of 64 pixels, quadbin 100 of 100), while rastertogrid dropped it
    from databricks.labs.gbx.pyrx.core import gridagg

    bng = _tif(
        np.ones((8, 8)), "EPSG:27700", from_origin(400000.0, 6000.0, 1000.0, 1000.0)
    )
    qb = _tif(np.ones((10, 10)), "EPSG:4326", from_origin(0.0, 90.0, 1.0, 1.0))
    for b, grid, res, want in ((bng, "bng", 3, 48), (qb, "quadbin", 4, 50)):
        assert _chip_pixels(b, grid, res, "centroid") == want, grid
        with _open(b) as ds:
            (band,) = gridagg.raster_to_grid_partial(
                ds, res, grid, coverage="sparse", assignment="centroid"
            )
        assert sum(c["count"] for c in band) == want


def test_tessellate_centroid_takes_a_longitude_past_180_a_turn_back():
    # a 0..360 raster: a centre at 250E is in the cell at 110W (quadbin clamped it into the east-most column)
    east = _tif(np.ones((4, 4)), "EPSG:4326", from_origin(250.0, 45.0, 1.0, 1.0))
    west = _tif(np.ones((4, 4)), "EPSG:4326", from_origin(-110.0, 45.0, 1.0, 1.0))
    from databricks.labs.gbx.pyrx.core import tessellate as T

    def cells(b):
        with _open(b) as ds:
            return sorted(
                c
                for c, _ in T.iter_tessellate(
                    ds, 6, "quadbin", mode="centroid", coverage="sparse"
                )
            )

    assert cells(east) == cells(west)
