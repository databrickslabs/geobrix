"""
GeoBrix sample-data module: download Essential and Complete bundles to Unity Catalog Volumes.

Packaged with the GeoBrix WHL so end users can run the setup notebook or call
these functions from their own code without the full repo.

Requires Python 3.11+. For downloads, install: requests, pystac-client, planetary-computer, geopandas
(optional; only needed for the bundles that use them).
"""

from databricks.labs.gbx.sample._bundle import (
    get_temp_dir,
    get_volumes_path,
    run_complete_bundle,
    run_essential_bundle,
)

__all__ = [
    "get_temp_dir",
    "get_volumes_path",
    "run_complete_bundle",
    "run_essential_bundle",
]
