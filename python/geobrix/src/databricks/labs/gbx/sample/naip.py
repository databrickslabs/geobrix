"""NaipDownloader — AOI-driven NAIP imagery staging via Planetary Computer STAC.

Mirrors OvertureClient's shape: a driver-side discovery step (metadata-only),
then DISTRIBUTED asset I/O via StacClient.download(). Signing is handled by
StacClient using the ``planetary_computer`` modifier.

ONLINE-ONLY — no offline fallback. Requires pystac-client and planetary-computer.

Injection seams (offline tests): pass ``_stac_client`` (a pre-built or mock
StacClient) to bypass catalog network access in unit tests.

Serverless-safe: no spark.conf.set, _jvm, .rdd, cache, or persist calls.
Parallelism via StacClient.download()'s spark.range fan-out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Union

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from databricks.labs.gbx.stac import PLANETARY_COMPUTER  # canonical STAC catalog root

NAIP_COLLECTION = "naip"

# NAIP is always the "image" asset (the only downloaded asset).
_NAIP_ASSET = "image"

# NAIP covers the continental US from ~2010 onward; a wide datetime bracket
# avoids having to know the exact vintages for a given AOI.
_NAIP_DATETIME = "2010-01-01/2030-01-01"


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


class NaipDownloader:
    """Distributed, AOI-driven NAIP imagery downloader via Planetary Computer STAC.

    Discovery (``discover``) is driver-side, metadata-only, and returns a
    DataFrame of NAIP items intersecting the given bounding box. Download
    (``download``) fans out via StacClient.download() — a ``spark.range``
    range-scan so each tile downloads in its own Spark task, Serverless-safe.

    Parameters
    ----------
    catalog:
        STAC API root URL (default: Planetary Computer).
    sign:
        Signing modifier name passed to StacClient (``"planetary_computer"``
        for Planetary Computer signed asset URLs).
    collection:
        STAC collection ID (default: ``"naip"``).
    _stac_client:
        Injectable StacClient (or compatible mock) for offline unit tests.
        When provided, the ``catalog``/``sign``/``_catalog_opener`` args are
        ignored and this client is used directly.
    """

    def __init__(
        self,
        catalog: str = PLANETARY_COMPUTER,
        sign: str = "planetary_computer",
        collection: str = NAIP_COLLECTION,
        _stac_client=None,
    ):
        self.catalog = catalog
        self.sign = sign
        self.collection = collection
        self._stac_client = _stac_client

    def _get_stac_client(self):
        """Return the StacClient to use (injected or live)."""
        if self._stac_client is not None:
            return self._stac_client
        from databricks.labs.gbx.stac import StacClient

        return StacClient(catalog=self.catalog, sign=self.sign)

    def _aoi_dataframe(self, bbox: Sequence[float], spark=None) -> "DataFrame":
        """Build a 1-row DataFrame with the AOI as a GeoJSON polygon string."""
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        geojson = _bbox_to_geojson_polygon(bbox)
        return spark.createDataFrame([(geojson,)], ["geojson"])

    def discover(
        self,
        bbox: Sequence[float],
        year: Optional[int] = None,
        spark=None,
    ) -> "DataFrame":
        """Search Planetary Computer for NAIP items intersecting bbox.

        Returns a DataFrame with one row per distinct NAIP image asset:
        - item_id (str): STAC item ID
        - year (int): from item_properties["naip:year"] (fallback: date[:4])
        - item_bbox (array<double>): item bounding box
        - href (str): signed asset URL

        Parameters
        ----------
        bbox:
            (minx, miny, maxx, maxy) in EPSG:4326.
        year:
            When given, keep only items from that year. None returns all years.
        spark:
            SparkSession. Defaults to the active session.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()
        aoi_df = self._aoi_dataframe(bbox, spark)

        raw = client.search(
            aoi_df,
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_NAIP_DATETIME,
        )

        # Keep only the image asset.
        img = raw.filter(F.col("asset_name") == _NAIP_ASSET)

        # Extract naip:year from item_properties; fall back to date[:4].
        year_col = F.when(
            F.col("item_properties")["naip:year"].isNotNull(),
            F.col("item_properties")["naip:year"].cast(IntegerType()),
        ).otherwise(F.col("date").substr(1, 4).cast(IntegerType()))

        out = (
            img.withColumn("year", year_col)
            .select("item_id", "year", "item_bbox", "href")
            .distinct()
        )

        if year is not None:
            out = out.filter(F.col("year") == year)

        return out

    def download(
        self,
        bbox: Sequence[float],
        out_dir: str,
        year: Union[int, str] = "latest",
        bbox_crs: str = "EPSG:4326",
        max_mpp: float = 2.4,
        partitions: Optional[int] = None,
        spark=None,
    ) -> "DataFrame":
        """Search, select a vintage, and download NAIP tiles to out_dir.

        Parameters
        ----------
        bbox:
            (minx, miny, maxx, maxy) AOI in EPSG:4326.
        out_dir:
            Output directory (UC Volume path or local path).
        year:
            ``"latest"`` (default) → pick the maximum naip:year in the search
            results (falling back to maximum date when the property is absent).
            An integer → keep only items from that exact year.
        bbox_crs:
            CRS of the bbox (default ``"EPSG:4326"``).
        max_mpp:
            Maximum pixel size in source-CRS units passed to StacClient.download.
            For NAIP (UTM, metres) the default 2.4 m keeps full 1-m native
            resolution (with a factor-of-2 safety margin for decimated reads).
        partitions:
            Target partition count for the spark.range fan-out. None → one task
            per asset (StacClient default).
        spark:
            SparkSession. Defaults to the active session.

        Returns
        -------
        DataFrame
            StacClient.download result: item_id, asset_name, out_file_path,
            out_file_sz, is_out_file_valid, last_update.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = spark or SparkSession.getActiveSession()
        client = self._get_stac_client()

        # -- search -------------------------------------------------------
        from pyspark.sql.types import IntegerType

        aoi_df = self._aoi_dataframe(bbox, spark)

        raw = client.search(
            aoi_df,
            geojson_col="geojson",
            collections=[self.collection],
            datetime=_NAIP_DATETIME,
        )

        img = raw.filter(F.col("asset_name") == _NAIP_ASSET)

        year_col = F.when(
            F.col("item_properties")["naip:year"].isNotNull(),
            F.col("item_properties")["naip:year"].cast(IntegerType()),
        ).otherwise(F.col("date").substr(1, 4).cast(IntegerType()))

        img = img.withColumn("_year", year_col)

        # -- vintage selection -------------------------------------------
        if year == "latest":
            # Collect the max year from the driver (modest list of items).
            max_year_row = img.agg(F.max("_year").alias("max_year")).first()
            if max_year_row is None or max_year_row["max_year"] is None:
                selected_year = None
            else:
                selected_year = max_year_row["max_year"]
        else:
            selected_year = int(year)

        if selected_year is not None:
            vintage = img.filter(F.col("_year") == selected_year).select(
                "item_id", "asset_name", "href"
            )
        else:
            # No items found — pass the empty selection to client.download so it
            # returns the correct schema (same field names + types as the non-empty
            # path).  StacClient.download handles empty input by returning an
            # empty DataFrame with the canonical schema (no href, BooleanType
            # is_out_file_valid).
            vintage = img.select("item_id", "asset_name", "href").limit(0)

        # -- download via StacClient -------------------------------------
        # partitions controls the spark.range fan-out inside StacClient.download.
        return client.download(
            vintage,
            out_dir,
            bbox=list(bbox),
            bbox_crs=bbox_crs,
            max_mpp=max_mpp,
            partitions=partitions,
        )

    def read(self, out_dir: str, spark=None) -> "DataFrame":
        """Load downloaded NAIP GeoTIFFs from out_dir into a raster tile DataFrame.

        Mirrors OvertureClient.read(). Uses the ``raster_gbx`` Spark data source
        (the light-tier pyrx reader), filters to ``*.tif`` files, and repartitions
        by source path — Serverless-safe (column-hash repartition, not number-only).

        Parameters
        ----------
        out_dir:
            Root directory written by download().
        spark:
            SparkSession. Defaults to the active session.

        Returns
        -------
        DataFrame
            Columns: tile (the tile struct from the raster reader).
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


def download_naip_aoi(
    spark: "SparkSession",
    bbox: Sequence[float],
    out_dir: str,
    year: Union[int, str] = "latest",
    max_mpp: float = 2.4,
    **kw,
) -> "DataFrame":
    """One-shot: discover and download NAIP imagery for an AOI to out_dir.

    Convenience wrapper around NaipDownloader — constructs a default downloader
    (Planetary Computer catalog, planetary_computer signing, naip collection)
    and calls download() in one step.

    Parameters
    ----------
    spark:
        Active SparkSession.
    bbox:
        (minx, miny, maxx, maxy) in EPSG:4326.
    out_dir:
        Output directory (UC Volume path or local path).
    year:
        ``"latest"`` (default) or an integer year.
    max_mpp:
        Maximum pixel size in source-CRS units (default 2.4 m for NAIP).
    **kw:
        Forwarded to NaipDownloader.download() (e.g. ``partitions``,
        ``bbox_crs``).

    Returns
    -------
    DataFrame
        StacClient.download result: item_id, asset_name, out_file_path,
        out_file_sz, is_out_file_valid, last_update.
    """
    downloader = NaipDownloader()
    return downloader.download(
        bbox, out_dir, year=year, max_mpp=max_mpp, spark=spark, **kw
    )
