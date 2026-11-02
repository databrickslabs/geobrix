"""Oracle table for the analysis functions of the GDAL spec engine family that read a tile (today: the summary): inputs ->
expected, run at every level by test_family_cross_tier.py.

The expected values come from the fixtures' definitions (see raster_accessor.py): the 4 x 3 grid of 1..12 with its
first pixel NoData has the valid values 2..12, so its minimum is 2, its maximum 12 and its mean 7.
"""

from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _tile,
    builder,
)

S, PRX, FND = "gbx_rst_summary", "gbx_rst_proximity", "gbx_rst_fillnodata"
STATS, VIRT, ERR = Build("acc_stats"), Build("tile_virtual"), Build("tile_error")
CLASSES, DEM, CORRUPT = (
    Build("analysis_classes"),
    Build("dem_3857"),
    Build("tile_corrupt"),
)
ONLY = ("session",)


@builder("analysis_classes")
def _classes(tmpdir):
    """A 64 x 48 Byte raster of three classes in blocks (1, 2, 3; 0 elsewhere), NoData 0, EPSG:3857 on a 10 m grid."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    z = np.zeros((1, 48, 64), dtype="uint8")
    z[0, 5:16, 5:16] = 1
    z[0, 30:41, 40:56] = 2
    z[0, 20:26, 25:31] = 3
    profile = dict(
        driver="GTiff",
        width=64,
        height=48,
        count=1,
        dtype="uint8",
        nodata=0,
        crs="EPSG:3857",
        transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(z)
        return _tile(mf.read())


SPEC_ANALYSIS = (
    Case(
        "summary",
        S,
        (STATS,),
        "json_subset",
        {
            "driverShortName": "GTiff",
            "size": [4, 3],
            "bands": [
                {
                    "band": 1,
                    "type": "Float32",
                    "noDataValue": -9999.0,
                    "min": 2.0,
                    "max": 12.0,
                    "mean": 7.0,
                }
            ],
        },
        ("session",),
    ),
    Case(
        "summary_virtual", S, (VIRT,), "json_subset", {"size": [64, 48]}, ("session",)
    ),
    # the mean of single-precision pixels is accumulated in double (see avg_float32_precision in raster_accessor.py)
    Case(
        "summary_float32_precision",
        S,
        (Build("tile_4326"),),
        "json_subset",
        {"bands": [{"mean": 47.71793193717277}]},
        ("session",),
    ),
    Case("summary_null_tile", S, (None,), "null", None, ("session",)),
    Case("summary_error_tile", S, (ERR,), "null", None, ("session",)),
    # proximity: the distance to the nearest source pixel, judged against scipy's exact Euclidean distance
    Case("proximity_targets", PRX, (CLASSES, "1,2"), "ref", "proximity_geo", ONLY),
    # no target values: every pixel that is not 0 is a source
    Case("proximity_default_sources", PRX, (CLASSES,), "ref", "proximity_geo", ONLY),
    Case(
        "proximity_pixel_capped",
        PRX,
        (CLASSES, "1", "PIXEL", 5.0),
        "ref",
        "proximity_pixel",
        ONLY,
    ),
    Case("proximity_bad_units", PRX, (CLASSES, "1", "feet"), "raises", "feet", ONLY),
    Case("proximity_bad_cap", PRX, (CLASSES, "1", "GEO", -1.0), "raises", "-1.0", ONLY),
    Case("proximity_null_tile", PRX, (None, "1"), "null", None, ONLY),
    # a NULL unit is the default, GEO, as a NULL list of target values and a NULL cap mean none
    Case(
        "proximity_null_units",
        PRX,
        (CLASSES, "1", None),
        "ref",
        "proximity_geo",
        ONLY,
    ),
    # an explicit NULL cap is no cap: the SQL call sends all four arguments to every level
    Case(
        "proximity_null_cap",
        PRX,
        (CLASSES, "1", "GEO", None),
        "ref",
        "proximity_geo",
        ONLY,
    ),
    # NaN is the heavy tier's absent sentinel: no cap, at every level
    Case(
        "proximity_nan_cap",
        PRX,
        (CLASSES, "1", "GEO", float("nan")),
        "ref",
        "proximity_geo",
        ONLY,
    ),
    Case("proximity_error_in", PRX, (ERR, "1"), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("proximity_corrupt", PRX, (CORRUPT, "1"), "error_tile", None, ONLY),
    # fillnodata: rasterio's own GDAL FillNodata call is the reference (the 3 x 3 hole of dem_3857)
    Case("fillnodata_default", FND, (DEM,), "ref", "fillnodata", ONLY),
    # a search distance of one pixel cannot reach the centre of the hole: it stays NoData
    Case("fillnodata_virtual", FND, (Build("dem_file"),), "ref", "fillnodata", ONLY),
    Case("fillnodata_short_search", FND, (DEM, 1.0, 0), "ref", "fillnodata", ONLY),
    Case("fillnodata_smoothed", FND, (DEM, 100.0, 2), "ref", "fillnodata", ONLY),
    Case("fillnodata_null_tile", FND, (None,), "null", None, ONLY),
    Case("fillnodata_null_distance", FND, (DEM, None, 0), "null", None, ONLY),
    Case("fillnodata_error_in", FND, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("fillnodata_corrupt", FND, (CORRUPT,), "error_tile", None, ONLY),
)
