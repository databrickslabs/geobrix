"""Oracle table for the analysis functions of the GDAL spec engine family that read a tile (today: the summary): inputs ->
expected, run at every level by test_family_cross_tier.py.

The expected values come from the fixtures' definitions (see raster_accessor.py): the 4 x 3 grid of 1..12 with its
first pixel NoData has the valid values 2..12, so its minimum is 2, its maximum 12 and its mean 7.
"""

from databricks.labs.gbx.bench.oracles import Build, Case

S = "gbx_rst_summary"
STATS, VIRT, ERR = Build("acc_stats"), Build("tile_virtual"), Build("tile_error")


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
    Case("summary_null_tile", S, (None,), "null", None, ("session",)),
    Case("summary_error_tile", S, (ERR,), "null", None, ("session",)),
)
