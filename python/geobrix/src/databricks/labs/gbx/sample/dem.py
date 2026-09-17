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
# 3DEP lidar-derived bare-earth DTM: finer than seamless (2 m today across the US on
# Planetary Computer; 1 m where a survey provides it). Unlike seamless it exposes NO
# ``gsd`` property — grid resolution is encoded in the item id ("-dtm-<N>m-"), which
# ``_resolution_col`` parses. True 1 m for an arbitrary AOI otherwise comes from binning
# the raw lidar point cloud (LidarDownloader + rst_binpoints), not this raster catalog.
DEM_LIDAR_DTM_COLLECTION = "3dep-lidar-dtm"
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

    @classmethod
    def lidar_dtm(cls, **kw) -> "DemDownloader":
        """DemDownloader configured for the 3DEP lidar-derived bare-earth DTM
        collection (``3dep-lidar-dtm``) — finer than seamless (2 m across the US
        today; 1 m where a survey provides it). Resolution selection is identical
        (``resolution="finest"`` or an int in metres), driven off the id-parsed
        grid resolution. Extra kwargs (catalog, sign, _stac_client) pass through.
        """
        kw.setdefault("collection", DEM_LIDAR_DTM_COLLECTION)
        kw.setdefault("asset", _DEM_ASSET)
        return cls(**kw)

    def _get_stac_client(self):
        if self._stac_client is not None:
            return self._stac_client
        from databricks.labs.gbx.stac import StacClient

        return StacClient(catalog=self.catalog, sign=self.sign)

    def _aoi_dataframe(self, bbox: Sequence[float], spark=None) -> "DataFrame":
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        return spark.createDataFrame([(_bbox_to_geojson_polygon(bbox),)], ["geojson"])

    def _resolution_col(self):
        """Column expr: resolution in metres, as an int (nullable).

        Uses ``item_properties['gsd']`` when the collection exposes it (3dep-seamless:
        10/30). Falls back to parsing the item id's ``-dtm-<N>m-`` token, which is how
        3dep-lidar-dtm carries its grid resolution (that collection has no gsd
        property). A non-matching id yields NULL, so ``"finest"`` still degrades
        gracefully to "keep all" when neither source provides a resolution.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType

        gsd = F.col("item_properties")["gsd"].cast(IntegerType())
        # regexp_extract yields "" on no match; under ANSI mode casting "" -> int
        # throws, so map the no-match case to NULL before the cast.
        parsed = F.regexp_extract(F.col("item_id"), r"-dtm-(\d+)m", 1)
        from_id = F.when(parsed == "", F.lit(None).cast(IntegerType())).otherwise(
            parsed.cast(IntegerType())
        )
        return F.coalesce(gsd, from_id)

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
            img.withColumn("gsd", self._resolution_col())
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
            "_gsd", self._resolution_col()
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
