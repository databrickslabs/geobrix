"""NasaNexDownloader — AOI-driven NASA-NEX GDDP-CMIP6 climate raster staging via Planetary Computer.

Mirrors TropomiDownloader: driver-side discovery (metadata-only), then DISTRIBUTED
asset download via StacClient.download(). NASA-NEX GDDP-CMIP6 granules are regular
0.25-degree lat/lon NetCDF-4 global grids — read() loads them through the netcdf_gbx
reader in RASTER mode.

ONLINE-ONLY (pystac-client + planetary-computer). Injection seam: _stac_client.
Serverless-safe: no Spark-config mutation, JVM access, low-level partition
APIs, or caching.
"""

from __future__ import annotations

from typing import Optional, Sequence, Tuple

from databricks.labs.gbx.stac import PLANETARY_COMPUTER  # canonical STAC catalog root

# Verified against the PC collection: regular 0.25-degree global grids, anonymous access.
_NEX_COLLECTION = "nasa-nex-gddp-cmip6"
_NEX_DATETIME = "1950-01-01/2100-12-31"
# Default climate variable; asset names in this collection ARE the variable ids.
_NEX_DEFAULT_VARIABLES: Tuple[str, ...] = ("tas",)


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


class NasaNexDownloader:
    """Distributed, AOI-driven NASA-NEX GDDP-CMIP6 climate raster downloader via Planetary Computer.

    Discovery (``discover``) is driver-side, metadata-only. Download (``download``)
    fans out via StacClient.download() — Serverless-safe. ``read`` loads the staged
    NetCDF-4 regular-grid granules via the netcdf_gbx reader in raster mode.

    Parameters
    ----------
    catalog:      STAC API root URL (default: Planetary Computer).
    sign:         Signing modifier for StacClient (``"planetary_computer"``).
    collection:   STAC collection ID (default ``"nasa-nex-gddp-cmip6"``).
    _stac_client: Injectable StacClient (or mock) for offline unit tests.
    """

    def __init__(
        self,
        catalog: str = PLANETARY_COMPUTER,
        sign: str = "planetary_computer",
        collection: str = _NEX_COLLECTION,
        _stac_client=None,
    ):
        self.catalog = catalog
        self.sign = sign
        self.collection = collection
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

    def discover(
        self,
        bbox: Sequence[float],
        temporal: Optional[str] = None,
        variables: Tuple[str, ...] = _NEX_DEFAULT_VARIABLES,
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
            datetime=temporal or _NEX_DATETIME,
        )
        return (
            raw.filter(F.col("asset_name").isin(list(variables)))
            .select("item_id", "asset_name", "item_bbox", "href")
            .distinct()
        )

    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        temporal: Optional[str] = None,
        variables: Tuple[str, ...] = _NEX_DEFAULT_VARIABLES,
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
            datetime=temporal or _NEX_DATETIME,
        )
        granules = raw.filter(F.col("asset_name").isin(list(variables))).select(
            "item_id", "asset_name", "href"
        )
        # NASA-NEX GDDP-CMIP6 granules are regular-grid NetCDF (not swath); they are
        # consumed whole as raster tiles. Skip rasterio read-validation (validate=False)
        # and use a size-floor existence check instead — same rationale as TropomiDownloader.
        # name="{item_id}_{asset_name}.nc": one file per climate variable per item.
        return client.download(
            granules,
            out_dir,
            name="{item_id}_{asset_name}.nc",
            validate=False,
            partitions=partitions,
        )

    def read(
        self,
        out_dir: str,
        spark=None,
    ):
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("netcdf_gbx")
            .option("mode", "raster")
            .option("filterRegex", r".*\.nc$")
            .load(out_dir)
        )


def download_nasanex_aoi(spark, bbox: Sequence[float], out_dir: str, **kw):
    """One-shot: default NasaNexDownloader + download NASA-NEX GDDP-CMIP6 for an AOI."""
    return NasaNexDownloader().download(bbox, out_dir, spark=spark, **kw)
