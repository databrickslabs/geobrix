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
from databricks.labs.gbx.sample.dem import DemDownloader, download_dem_aoi
from databricks.labs.gbx.sample.naip import NaipDownloader, download_naip_aoi
from databricks.labs.gbx.sample.overture import OvertureClient, download_overture_aoi
from databricks.labs.gbx.sample.tropomi import TropomiDownloader, download_tropomi_aoi

__all__ = [
    "DemDownloader",
    "NaipDownloader",
    "OvertureClient",
    "TropomiDownloader",
    "download_dem_aoi",
    "download_naip_aoi",
    "download_overture_aoi",
    "download_tropomi_aoi",
    "get_temp_dir",
    "get_volumes_path",
    "run_complete_bundle",
    "run_essential_bundle",
]
