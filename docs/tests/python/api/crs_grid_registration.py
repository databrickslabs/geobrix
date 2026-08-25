"""
Custom PROJ grid-shift registration.

Demonstrates how to register a directory of PROJ grid-shift files so that
CRS transforms requiring datum grids produce accurate results on every worker.

The session-start call::

    gbx.register_proj_grids(spark, "/Volumes/<catalog>/<schema>/proj-grids")

is all that is needed — after that, every CRS-handling function on both the
lightweight (pyrx / pyvx) and heavyweight (rasterx / vectorx) tiers finds the
grid files automatically.

## How this works offline

Production datasets use real datum grids (OSTN15, NADCON, PROJ CDN) that require
network access. This example uses a synthetic NTv2 fixture committed to the repo
(``sample-data/…/proj-grids/synthetic.gsb``) that applies a known constant
+30 arc-second latitude shift everywhere inside its coverage box. That lets the
test run fully offline and the assertion be exact — the synthetic shift is the
same mechanism as a real OSTN15 shift, just with a simpler known value.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Fixture path
# ---------------------------------------------------------------------------

# Synthetic NTv2 fixture committed under sample-data/ in the repo.
# In production you would pass your own Volume path instead, e.g.:
#   "/Volumes/<catalog>/<schema>/proj-grids"
_REPO_ROOT = Path(__file__).resolve().parents[4]  # docs/tests/python/api/ -> 4 levels up
GRID_DIR = str(
    _REPO_ROOT
    / "sample-data"
    / "Volumes"
    / "main"
    / "geobrix_samples"
    / "geobrix-examples"
    / "proj-grids"
)

# The synthetic grid applies a constant +30 arc-second latitude offset.
_GRID_CRS = "+proj=longlat +ellps=GRS80 +nadgrids=synthetic.gsb +no_defs"
_TARGET_CRS = "EPSG:4326"
_INPUT_POINT = "POINT (0 51.5)"
# Expected latitude after the +30 arc-second shift (hand-computable).
EXPECTED_LAT = 51.5 + 30.0 / 3600.0  # 51.50833...


# ---------------------------------------------------------------------------
# Doc example
# ---------------------------------------------------------------------------


def register_custom_grids_example(spark):
    """Register a Volume directory of PROJ grid files at session start, then run a datum-aware transform.

    After calling ``register_proj_grids`` once, every CRS-handling function on
    both the lightweight and heavyweight tiers finds the grid files automatically.
    """
    import databricks.labs.gbx as gbx  # noqa: PLC0415
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    # Step 1 — Session start: register the Volume directory that holds your grids.
    # On a real cluster: gbx.register_proj_grids(spark, "/Volumes/catalog/schema/proj-grids")
    gbx.register_proj_grids(spark, GRID_DIR)

    # Step 2 — Register pyvx functions (idempotent; picks up the grid dirs above).
    vx.register(spark)

    # Step 3 — Apply a CRS transform that requires the grid.
    # st_transformcrs(geom, target_crs [, source_crs]) reprojects to target_crs.
    geom_df = spark.createDataFrame([(_INPUT_POINT,)], ["geom"])
    result = geom_df.select(
        vx.st_transformcrs(f.col("geom"), _TARGET_CRS, _GRID_CRS).alias("shifted")
    ).first()

    return result["shifted"]
