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
