"""Raster drivers through ``gbx_rst_asformat``: a GeoTIFF read by ``raster_gbx`` is re-encoded to the driver, and the
tile that comes back is held to what GDAL's own writer makes of the same pixels (the file copied alone, as the tile's
bytes are): band count, size, type, pixels, NoData, geotransform and CRS. A lossy encoder is held to a small mean
error from the source as well.
"""

import dataclasses
import os
import shutil
from test.drivers_extended._asserts import assert_tile_is, tile_dict
from test.drivers_extended._raster import (
    BY_DRIVER,
    param,
    reference,
    source_arrays,
    write_sample,
)

import numpy as np
import pytest

pytestmark = pytest.mark.filterwarnings("ignore")

# the drivers `gbx_rst_asformat` encodes that the suite covers, with the mean pixel error a lossy one may add
# (None: exact). A driver is listed here because the function accepts it; the test shows it round-trips.
WRITE = {
    "netCDF": None,
    "GTiff": None,
    "COG": None,
    "PNG": None,
    "JPEG": 6.0,
    "JP2OpenJPEG": 2.0,
    "AAIGrid": None,
    "XYZ": None,
    "GSAG": None,
    "GRIB": None,
    "NITF": None,
    "SRTMHGT": None,
    "RMF": None,  # the 8-bit .rsw form; the 16-bit .mtw form needs a creation option
    "BAG": None,
    "GTX": None,
    "WEBP": 6.0,
    "JPEGXL": 1.0,
    "MBTiles": None,
    "GPKG": None,
}


def write_case(driver: str):
    """The case a driver is written from: its first, except RMF, whose first (16-bit) form needs the MTW option."""
    return BY_DRIVER[driver][-1 if driver == "RMF" else 0]


_CASES = [write_case(d) for d in WRITE]


@pytest.mark.parametrize("case", [param(c, "asformat") for c in _CASES])
def test_rst_asformat_encodes_the_driver_as_gdal_does(gb, spark, tmp_path, case):
    # the pixels, CRS and NoData of the case, as a GeoTIFF read by raster_gbx
    src_case = dataclasses.replace(
        case, driver="GTiff", ext=".tif", co=(), name="src.tif", path_only=False
    )
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src_path = write_sample(gb, src_case, str(src_dir))
    # GDAL's own encoding of the same pixels, the one file alone
    direct = tmp_path / "direct"
    direct.mkdir()
    gdal_path = write_sample(
        gb,
        dataclasses.replace(case, co=tuple(o for o in case.co if o.startswith("MTW"))),
        str(direct),
    )
    alone = tmp_path / "alone"
    alone.mkdir()
    shutil.copy(gdal_path, str(alone / os.path.basename(gdal_path)))
    ref = reference(gb, str(alone / os.path.basename(gdal_path)))

    row = (
        spark.read.format("raster_gbx")
        .load(src_path)
        .selectExpr(f"gbx_rst_asformat(tile, '{case.driver}') AS tile")
        .collect()[0]
    )
    assert row["tile"] is not None
    tol = WRITE[case.driver]
    assert_tile_is(tile_dict(row["tile"]), ref, f"rst_asformat {case.driver}", tol=tol)
    if tol is not None:  # a lossy encoder stays near the source pixels
        src = np.stack(source_arrays(case)).astype("f8")
        from databricks.labs.gbx.pyrx.core import open_tile as ot

        with ot._open(tile_dict(row["tile"])) as ds:
            got = ds.read().astype("f8")
        valid = np.ones_like(src[0], dtype=bool)
        if case.nodata is not None:
            valid = src[0] != case.nodata
        assert float(np.abs(got[:, valid] - src[:, valid]).mean()) <= max(tol, 8.0)
