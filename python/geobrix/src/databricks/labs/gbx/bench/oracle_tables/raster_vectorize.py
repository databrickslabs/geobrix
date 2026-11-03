"""Oracle table for the raster functions that turn pixels into vector features, here the iso-bands (the contour
lines are in SPEC_DEM and the polygonize rows in SPEC_ANALYSIS, which hold the GDAL spec engine's functions):
inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixture's definition, never from the function under test. `vz_columns` is a 6 x 6
raster of 10 m pixels in EPSG:3857 whose value is its column number, 0 to 5, so a band of breaks is a block of whole
columns and its area is the number of pixels times 100 square metres.
"""

import os

from databricks.labs.gbx.bench.oracles import Build, Case, _gtiff, _tile, builder

ISO = "gbx_rst_isoband"
COLUMNS, ERR, CORRUPT = Build("vz_columns"), Build("tile_error"), Build("tile_corrupt")
MISSING = Build("vz_missing")
ONLY = ("session",)


@builder("vz_columns")
def _vz_columns(tmpdir):
    import numpy as np

    z = np.tile(np.arange(6, dtype="float64"), (6, 1))
    return _tile(_gtiff(z[np.newaxis], "EPSG:3857", nodata=None))


@builder("vz_missing")
def _vz_missing(tmpdir):
    """A virtual tile whose file is not there (a UC Volume path that has not caught up): not corrupt bytes."""
    return _tile(None, os.path.join(tmpdir, "gbx_missing_tile.tif"))


@builder("vz_columns_virtual")
def _vz_columns_virtual(tmpdir):
    """`vz_columns` as a virtual tile: the same pixels in a file, the tile a path and no bytes."""
    import numpy as np

    z = np.tile(np.arange(6, dtype="float64"), (6, 1))
    path = os.path.join(tmpdir, "gbx_vz_columns.tif")
    with open(path, "wb") as fh:
        fh.write(_gtiff(z[np.newaxis], "EPSG:3857", nodata=None))
    return _tile(None, path)


RASTER_VECTORIZE = (
    # [0, 2) holds columns 0 and 1, [2, 4) columns 2 and 3, [4, 6) columns 4 and 5: 12 pixels each, 1200 m2.
    Case(
        "isoband_three_bands",
        ISO,
        (COLUMNS, [0.0, 2.0, 4.0, 6.0]),
        "isobands",
        [(0, 0.0, 2.0, 1200.0), (1, 2.0, 4.0, 1200.0), (2, 4.0, 6.0, 1200.0)],
        ONLY,
    ),
    # Pixels below the first break and at or above the last are in no band.
    Case(
        "isoband_outside_dropped",
        ISO,
        (COLUMNS, [1.0, 3.0]),
        "isobands",
        [(0, 1.0, 3.0, 1200.0)],
        ONLY,
    ),
    Case(
        "isoband_virtual",
        ISO,
        (Build("vz_columns_virtual"), [1.0, 3.0]),
        "isobands",
        [(0, 1.0, 3.0, 1200.0)],
        ONLY,
    ),
    Case("isoband_null_tile", ISO, (None, [0.0, 1.0]), "null", None, ONLY),
    Case("isoband_null_breaks", ISO, (COLUMNS, None), "null", None, ONLY),
    Case("isoband_empty_breaks", ISO, (COLUMNS, []), "null", None, ONLY),
    Case(
        "isoband_not_ascending",
        ISO,
        (COLUMNS, [0.0, 4.0, 2.0]),
        "raises",
        "ascending",
        ONLY,
    ),
    Case(
        "isoband_null_element_break",
        ISO,
        (COLUMNS, [0.0, None, 2.0]),
        "raises",
        "NULL",
        ONLY,
    ),
    Case("isoband_error_tile", ISO, (ERR, [0.0, 1.0]), "null", None, ONLY),
    Case("isoband_corrupt", ISO, (CORRUPT, [0.0, 1.0]), "null", None, ONLY),
    # A file that is not reachable fails the task so that Spark retries; only bytes GDAL cannot read give NULL.
    Case(
        "isoband_missing_path",
        ISO,
        (MISSING, [0.0, 1.0]),
        "raises",
        "gbx_missing_tile",
        ONLY,
    ),
)
