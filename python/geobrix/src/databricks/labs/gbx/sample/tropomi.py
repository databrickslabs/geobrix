"""TropomiDownloader — AOI-driven Sentinel-5P L2 CH4 staging via Planetary Computer.

Mirrors DemDownloader: driver-side discovery (metadata-only), then DISTRIBUTED
asset download via StacClient.download(). S5P L2 CH4 is netCDF-4 swath — read()
loads it through the netcdf_gbx reader in VECTOR mode (per-pixel points).

ONLINE-ONLY (pystac-client + planetary-computer). Injection seam: _stac_client.
Serverless-safe: no spark.conf.set, _jvm, .rdd, cache, or persist.
"""

from __future__ import annotations

from typing import Optional, Sequence

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"
# Verified against the PC collection (2026-07-10): collection id + CH4 asset key.
S5P_COLLECTION = "sentinel-5p-l2-netcdf"
_CH4_ASSET = "ch4"
# S5P L2 CH4 stores its fields under the /PRODUCT group (Sentinel-5P L2 PUM).
_S5P_GROUP = "/PRODUCT"
_S5P_VARIABLES = "methane_mixing_ratio_bias_corrected,qa_value"
_S5P_DATETIME = "2018-01-01/2030-01-01"


def _bbox_to_geojson_polygon(bbox: Sequence[float]) -> str:
    import json

    minx, miny, maxx, maxy = bbox
    coords = [
        [minx, miny],
        [maxx, miny],
        [maxx, maxy],
        [minx, maxy],
        [minx, miny],
    ]
    return json.dumps({"type": "Polygon", "coordinates": [coords]})


class TropomiDownloader:
    """Distributed, AOI-driven Sentinel-5P L2 CH4 downloader via Planetary Computer.

    Discovery (``discover``) is driver-side, metadata-only. Download (``download``)
    fans out via StacClient.download() — Serverless-safe. ``read`` loads the staged
    netCDF-4 swath granules via the netcdf_gbx reader in vector mode (per-pixel
    points), passing the CH4 variable(s) through as attribute columns.

    Parameters
    ----------
    catalog:      STAC API root URL (default: Planetary Computer).
    sign:         Signing modifier for StacClient (``"planetary_computer"``).
    collection:   STAC collection ID (default ``"sentinel-5p-l2-netcdf"``).
    asset:        Asset name to download (default ``"ch4"``).
    _stac_client: Injectable StacClient (or mock) for offline unit tests.
    """

    def __init__(
        self,
        catalog: str = PLANETARY_COMPUTER,
        sign: str = "planetary_computer",
        collection: str = S5P_COLLECTION,
        asset: str = _CH4_ASSET,
        _stac_client=None,
    ):
        self.catalog = catalog
        self.sign = sign
        self.collection = collection
        self.asset = asset
        self._stac_client = _stac_client

    def _get_stac_client(self):
        if self._stac_client is not None:
            return self._stac_client
        from databricks.labs.gbx.stac import StacClient

        return StacClient(catalog=self.catalog, sign=self.sign)

    def _aoi_dataframe(self, bbox: Sequence[float], spark=None):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return spark.createDataFrame([(_bbox_to_geojson_polygon(bbox),)], ["geojson"])

    def discover(self, bbox: Sequence[float], spark=None):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        raw = client.search(
            self._aoi_dataframe(bbox, spark),
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_S5P_DATETIME,
        )
        return (
            raw.filter(F.col("asset_name") == self.asset)
            .select("item_id", "asset_name", "item_bbox", "href")
            .distinct()
        )

    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        bbox_crs: str = "EPSG:4326",
        partitions: Optional[int] = None,
        spark=None,
    ):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        raw = client.search(
            self._aoi_dataframe(bbox, spark),
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_S5P_DATETIME,
        )
        granules = raw.filter(F.col("asset_name") == self.asset).select(
            "item_id", "asset_name", "href"
        )
        return client.download(
            granules,
            out_dir,
            bbox=list(bbox),
            bbox_crs=bbox_crs,
            partitions=partitions,
        )

    def read(
        self,
        out_dir: str,
        variables: str = _S5P_VARIABLES,
        group: str = _S5P_GROUP,
        spark=None,
    ):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("netcdf_gbx")
            .option("mode", "vector")
            .option("group", group)
            .option("variables", variables)
            .option("filterRegex", r".*\.nc$")
            .load(out_dir)
            .repartition(64, F.col("geom_0_srid"))
        )


def download_tropomi_aoi(spark, bbox: Sequence[float], out_dir: str, **kw):
    """One-shot: default TropomiDownloader + download S5P CH4 for an AOI."""
    return TropomiDownloader().download(bbox, out_dir, spark=spark, **kw)
