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
from databricks.labs.gbx.sample.csb import CsbDownloader, download_csb_state
from databricks.labs.gbx.sample.dem import DemDownloader, download_dem_aoi
from databricks.labs.gbx.sample.emit import EmitDownloader, download_emit_aoi
from databricks.labs.gbx.sample.fire import (
    FirePerimeterDownloader,
    download_fire_perimeter_aoi,
)
from databricks.labs.gbx.sample.grace import GraceDaDownloader, download_grace_aoi
from databricks.labs.gbx.sample.gridmet import GridmetDownloader, download_gridmet
from databricks.labs.gbx.sample.hls import HlsDownloader, download_hls_aoi
from databricks.labs.gbx.sample.naip import NaipDownloader, download_naip_aoi
from databricks.labs.gbx.sample.nasanex import NasaNexDownloader, download_nasanex_aoi
from databricks.labs.gbx.sample.nclimgrid import (
    NclimgridDownloader,
    download_precip_normals,
)
from databricks.labs.gbx.sample.overture import OvertureClient, download_overture_aoi
from databricks.labs.gbx.sample.stage4 import StageIvDownloader, download_stage4
from databricks.labs.gbx.sample.tropomi import TropomiDownloader, download_tropomi_aoi
from databricks.labs.gbx.sample.wells import WellsDownloader, download_wells_aoi
from databricks.labs.gbx.sample.worldpop import (
    WorldPopDownloader,
    download_worldpop_aoi,
)

__all__ = [
    "CsbDownloader",
    "DemDownloader",
    "EmitDownloader",
    "FirePerimeterDownloader",
    "GraceDaDownloader",
    "GridmetDownloader",
    "HlsDownloader",
    "NasaNexDownloader",
    "NaipDownloader",
    "NclimgridDownloader",
    "OvertureClient",
    "StageIvDownloader",
    "TropomiDownloader",
    "WellsDownloader",
    "WorldPopDownloader",
    "download_csb_state",
    "download_dem_aoi",
    "download_emit_aoi",
    "download_fire_perimeter_aoi",
    "download_grace_aoi",
    "download_gridmet",
    "download_hls_aoi",
    "download_naip_aoi",
    "download_nasanex_aoi",
    "download_overture_aoi",
    "download_precip_normals",
    "download_stage4",
    "download_tropomi_aoi",
    "download_wells_aoi",
    "download_worldpop_aoi",
    "get_temp_dir",
    "get_volumes_path",
    "run_complete_bundle",
    "run_essential_bundle",
]
