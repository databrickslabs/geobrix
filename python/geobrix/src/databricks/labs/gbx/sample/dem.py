"""DemDownloader — AOI-driven USGS 3DEP elevation staging via Planetary Computer STAC.

Mirrors NaipDownloader's shape: a driver-side discovery step (metadata-only), then
DISTRIBUTED asset I/O via StacClient.download(). The selection axis is resolution (gsd):
``download(resolution="finest")`` picks the minimum gsd (10 m over 30 m); an int picks
that exact gsd. Signing is handled by StacClient (``planetary_computer`` modifier).

ONLINE-ONLY — no offline fallback. Requires pystac-client and planetary-computer.

Injection seam (offline tests): pass ``_stac_client`` (a pre-built or mock StacClient)
to bypass catalog network access.

Serverless-safe: no spark.conf.set, _jvm, .rdd, cache, or persist. Parallelism via
StacClient.download()'s spark.range fan-out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Union

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from databricks.labs.gbx.stac import PLANETARY_COMPUTER  # canonical STAC catalog root

DEM_COLLECTION = "3dep-seamless"
# 3DEP-seamless exposes its DEM raster under the "data" asset (not "image").
_DEM_ASSET = "data"
# 3dep-seamless is a mosaic; a wide datetime bracket avoids guessing vintages.
_DEM_DATETIME = "2000-01-01/2030-01-01"


def _bbox_to_geojson_polygon(bbox: Sequence[float]) -> str:
    """Convert (minx, miny, maxx, maxy) to a GeoJSON Polygon string."""
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


class DemDownloader:
    """Distributed, AOI-driven 3DEP DEM downloader via Planetary Computer STAC.

    Discovery (``discover``) is driver-side, metadata-only. Download (``download``)
    fans out via StacClient.download() — Serverless-safe. Selection is by resolution
    (gsd): ``"finest"`` picks the minimum gsd; an int picks that exact gsd.

    Parameters
    ----------
    catalog:      STAC API root URL (default: Planetary Computer).
    sign:         Signing modifier for StacClient (``"planetary_computer"``).
    collection:   STAC collection ID (default ``"3dep-seamless"``).
    asset:        Asset name to download (default ``"data"``).
    _stac_client: Injectable StacClient (or mock) for offline unit tests.
    """

    def __init__(
        self,
        catalog: str = PLANETARY_COMPUTER,
        sign: str = "planetary_computer",
        collection: str = DEM_COLLECTION,
        asset: str = _DEM_ASSET,
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

    def _aoi_dataframe(self, bbox: Sequence[float], spark=None) -> "DataFrame":
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return spark.createDataFrame([(_bbox_to_geojson_polygon(bbox),)], ["geojson"])

    def _gsd_col(self):
        """Column expr: item_properties['gsd'] as an int (nullable)."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType

        return F.col("item_properties")["gsd"].cast(IntegerType())

    def discover(
        self, bbox: Sequence[float], resolution: Optional[int] = None, spark=None
    ) -> "DataFrame":
        """Search Planetary Computer for 3DEP items intersecting bbox.

        Returns one row per distinct DEM ``data`` asset: item_id (str), gsd (int),
        item_bbox (array<double>), href (str). ``resolution=None`` returns all gsd
        tiers; an int keeps only items whose gsd equals it.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        aoi_df = self._aoi_dataframe(bbox, spark)

        raw = client.search(
            aoi_df,
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_DEM_DATETIME,
        )
        img = raw.filter(F.col("asset_name") == self.asset)
        out = (
            img.withColumn("gsd", self._gsd_col())
            .select("item_id", "gsd", "item_bbox", "href")
            .distinct()
        )
        if resolution is not None:
            out = out.filter(F.col("gsd") == int(resolution))
        return out

    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        resolution: Union[int, str] = "finest",
        bbox_crs: str = "EPSG:4326",
        max_mpp: Optional[float] = None,
        partitions: Optional[int] = None,
        spark=None,
    ) -> "DataFrame":
        """Search, select a gsd tier, and download 3DEP tiles to out_dir.

        resolution="finest" (default) picks the minimum gsd (e.g. 10 m over 30 m);
        an int picks that exact gsd. When a source has no gsd property, "finest"
        keeps all matching items (graceful no-op). Returns StacClient.download's
        result: item_id, asset_name, out_file_path, out_file_sz, is_out_file_valid,
        last_update.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        aoi_df = self._aoi_dataframe(bbox, spark)

        raw = client.search(
            aoi_df,
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_DEM_DATETIME,
        )
        img = raw.filter(F.col("asset_name") == self.asset).withColumn(
            "_gsd", self._gsd_col()
        )

        if resolution == "finest":
            min_row = img.agg(F.min("_gsd").alias("m")).first()
            selected = min_row["m"] if min_row is not None else None
            # min over the gsd column: a real gsd tier -> keep the finest (min). None
            # covers two cases that both correctly fall through to `img`: NO gsd property
            # on the source (keep the whole matching set), or an EMPTY search (img is
            # already empty, so client.download returns the canonical empty schema).
            vintage = (
                img.filter(F.col("_gsd") == selected) if selected is not None else img
            )
        else:
            vintage = img.filter(F.col("_gsd") == int(resolution))

        vintage = vintage.select("item_id", "asset_name", "href")
        return client.download(
            vintage,
            out_dir,
            bbox=list(bbox),
            bbox_crs=bbox_crs,
            max_mpp=max_mpp,
            partitions=partitions,
        )

    def read(self, out_dir: str, spark=None) -> "DataFrame":
        """Load downloaded DEM GeoTIFFs from out_dir into a raster tile DataFrame.

        Mirrors NaipDownloader.read(): the ``raster_gbx`` reader, filtered to ``*.tif``,
        repartitioned by source path (Serverless-safe, column-hash repartition).
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        return (
            spark.read.format("raster_gbx")
            .option("filterRegex", r".*\.tif$")
            .load(out_dir)
            .repartition(64, F.col("source"))
            .select("tile")
        )


def download_dem_aoi(
    spark,
    bbox: Sequence[float],
    out_dir: str,
    resolution: Union[int, str] = "finest",
    max_mpp: Optional[float] = None,
    **kw,
) -> "DataFrame":
    """One-shot: construct a default DemDownloader and download a DEM for an AOI.

    Convenience wrapper — Planetary Computer catalog, planetary_computer signing,
    3dep-seamless collection, "data" asset. Forwards **kw (e.g. partitions, bbox_crs).
    """
    downloader = DemDownloader()
    return downloader.download(
        bbox, out_dir, resolution=resolution, max_mpp=max_mpp, spark=spark, **kw
    )
