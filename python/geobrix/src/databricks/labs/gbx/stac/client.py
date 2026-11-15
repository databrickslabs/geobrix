"""StacClient — catalog-agnostic, Serverless-safe STAC search/download/repair.

Download parallelism is via ``spark.range(N, numPartitions=N)`` — a Range SCAN,
which is NOT subject to AQE coalescePartitions (unlike a shuffle Exchange produced
by repartition(N, col)).  The target list is driver-collected (fine for dozens of
assets) and captured in the UDF closure — the Serverless-safe alternative to
broadcast (which requires sparkContext, forbidden on Serverless).

Search parallelism is via ``df.repartition(partitions, col)`` — hash by the AOI
geometry column so ~one search task per AOI.

No caching, persistence, sparkContext, _jvm, or .rdd calls — Serverless-safe.
The catalog opener is injectable (_catalog_opener) for unit tests.

Note on item_properties: values are stringified into a MapType(String, String).
Downstream numeric filters must cast (e.g. ``CAST(item_properties['eo:cloud_cover']
AS DOUBLE) < 20``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Sequence

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"


def _spark_types():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    asset_schema = ArrayType(
        StructType(
            [
                StructField("asset_name", StringType()),
                StructField("href", StringType()),
            ]
        )
    )
    item_schema = StructType(
        [
            StructField("item_id", StringType()),
            StructField("date", StringType()),
            StructField("item_bbox", ArrayType(DoubleType())),
            StructField("item_properties", MapType(StringType(), StringType())),
        ]
    )
    return asset_schema, item_schema


def _search_driver(
    catalog, df: "DataFrame", geojson_col: str, collections: List[str], datetime: str
) -> "DataFrame":
    """Run search on the driver (for test injection; no UDF pickling required)."""
    from databricks.labs.gbx.stac._search import extract_assets, parse_item, search_one

    rows = []
    carried = [c for c in df.columns if c != geojson_col]
    for r in df.collect():
        geojson = r[geojson_col]
        items = search_one(catalog, list(collections), datetime, geojson)
        for item_json in items:
            p = parse_item(item_json)
            props = {k: str(v) for k, v in (p["item_properties"] or {}).items()}
            for asset in extract_assets(item_json):
                row_dict = {c: r[c] for c in carried}
                row_dict.update(
                    {
                        "item_id": p["item_id"],
                        "date": p["date"],
                        "item_bbox": p["item_bbox"],
                        "item_properties": props,
                        "asset_name": asset["asset_name"],
                        "href": asset["href"],
                    }
                )
                rows.append(row_dict)

    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    spark = SparkSession.getActiveSession()
    if not rows:
        schema = StructType(
            [StructField(c, df.schema[c].dataType) for c in carried]
            + [
                StructField("item_id", StringType()),
                StructField("date", StringType()),
                StructField("item_bbox", ArrayType(DoubleType())),
                StructField("item_properties", MapType(StringType(), StringType())),
                StructField("asset_name", StringType()),
                StructField("href", StringType()),
            ]
        )
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows)


class StacClient:
    # The hosts an asset href may name (None: any public host). See ``__init__``.
    allowed_hosts = None

    def __init__(
        self,
        catalog=PLANETARY_COMPUTER,
        sign="planetary_computer",
        _catalog_opener=None,
        allowed_hosts=None,
    ):
        """``allowed_hosts``: when set, ``download`` and ``repair`` fetch an asset href only from these hosts
        (``"example.com"`` is that host, ``".example.com"`` it and every host under it). Whatever it is, an href
        whose host resolves to a loopback, private, link-local or unspecified address is refused, at every redirect
        hop, and no ``~/.netrc`` is read: the hrefs come from the catalog or from the table you pass.
        """
        self.catalog = catalog
        self.sign = sign
        self._catalog_opener = _catalog_opener
        self.allowed_hosts = allowed_hosts

    def _open_catalog(self):
        if self._catalog_opener is not None:
            return self._catalog_opener()
        import pystac_client

        from databricks.labs.gbx.stac._sign import resolve_modifier

        return pystac_client.Client.open(
            self.catalog, modifier=resolve_modifier(self.sign)
        )

    def search(
        self,
        df: "DataFrame",
        geojson_col: str,
        collections: List[str],
        datetime: str,
        partitions: int = 512,
    ) -> "DataFrame":
        """Search a STAC catalog for all AOIs in df[geojson_col].

        Returns one row per (AOI, item, asset). Carried columns (all columns except
        geojson_col) are preserved. Duplicate (item_id, asset_name) rows that arise
        when multiple AOIs overlap the same item are collapsed by .distinct().

        item_properties values are stringified to MapType(String, String); downstream
        numeric filters must cast (e.g. CAST(item_properties['eo:cloud_cover'] AS DOUBLE)).
        """
        # When a catalog opener is injected (test mode) run on the driver to avoid
        # pickling test-local callables into the Spark UDF worker.
        if self._catalog_opener is not None:
            return _search_driver(
                self._catalog_opener(), df, geojson_col, collections, datetime
            )

        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, StringType

        _ASSET_SCHEMA, _ITEM_SCHEMA = _spark_types()
        catalog_url = (
            self.catalog
        )  # search stores raw hrefs (no modifier); signing is at download

        @F.udf(ArrayType(StringType()))
        def _items(geojson):
            import pystac_client

            from databricks.labs.gbx.stac._search import search_one

            # Open without modifier so asset hrefs are stored raw (unsigned).
            # Signing happens once in _fetch at download time via resolve_signer.
            cat = pystac_client.Client.open(catalog_url)
            return search_one(cat, list(collections), datetime, geojson)

        @F.udf(_ASSET_SCHEMA)
        def _assets(item_json):
            from databricks.labs.gbx.stac._search import extract_assets

            return [(a["asset_name"], a["href"]) for a in extract_assets(item_json)]

        @F.udf(_ITEM_SCHEMA)
        def _item_fields(item_json):
            from databricks.labs.gbx.stac._search import parse_item

            p = parse_item(item_json)
            props = {k: str(v) for k, v in (p["item_properties"] or {}).items()}
            return (p["item_id"], p["date"], p["item_bbox"], props)

        carried = [c for c in df.columns if c != geojson_col]
        return (
            # Hash-by-column repartition (NOT number-only): on Serverless a round-robin
            # repartition(N) is AQE-coalesced back toward 1 partition (serial); repartition by
            # the AOI geometry column is respected and spreads ~one search task per AOI.
            df.repartition(partitions, F.col(geojson_col))
            .withColumn("_item", F.explode(_items(F.col(geojson_col))))
            .withColumn("_f", _item_fields("_item"))
            .withColumn("_a", F.explode(_assets("_item")))
            .select(
                *carried,
                F.col("_f.item_id").alias("item_id"),
                F.col("_f.date").alias("date"),
                F.col("_f.item_bbox").alias("item_bbox"),
                F.col("_f.item_properties").alias("item_properties"),
                F.col("_a.asset_name").alias("asset_name"),
                F.col("_a.href").alias("href"),
            )
            .dropDuplicates(["item_id", "asset_name"])
        )

    def download(
        self,
        df: "DataFrame",
        out_dir: str,
        asset_names: Optional[List[str]] = None,
        name: str = "{asset_name}_{item_id}.tif",
        validate: bool = True,
        max_tries: int = 5,
        partitions: Optional[int] = None,
        bbox: Optional[Sequence[float]] = None,
        bbox_crs: Optional[str] = None,
        max_mpp: Optional[float] = None,
        _get_fn=None,
    ) -> "DataFrame":
        """Download STAC assets to out_dir.

        validate=True (default): publish only if rasterio can decode a window of the
            file; is_out_file_valid reflects decode success.
        validate=False: publish without rasterio decode; is_out_file_valid reflects
            download+publish success only.

        Already-valid files (idempotency): if the target path exists and passes the
        validity check, the asset is skipped (no re-download).

        bbox=(minx, miny, maxx, maxy) windows each asset to the AOI on read (source
            CRS by default; bbox_crs declares the bbox CRS). Uses rasterio range reads
            (/vsicurl for https) so only the required pixel window is transferred.

        max_mpp: maximum pixel size in source-CRS units (metres for UTM sources such as
            NAIP; degrees for EPSG:4326). When set and coarser than the native pixel
            size, each windowed read is DECIMATED so the output pixel size is
            approximately max_mpp. Bounds UDF memory on Serverless (1 GB/UDF cap)
            for high-resolution sources. Requires bbox. Ignored when bbox is None.

        Returns a DataFrame with columns: item_id, asset_name, out_file_path,
        out_file_sz, is_out_file_valid, last_update.
        """
        from pyspark.sql import functions as F

        if asset_names:
            df = df.filter(F.col("asset_name").isin(list(asset_names)))
        # Carry raw href so _fetch can sign it once (avoids get_item which requires
        # a collection parameter on the PC STAC API). Fail LOUDLY if it's missing —
        # silently null-filling it makes every download invalid with no explanation
        # (the exact symptom that hid this in eo-series nb02). The href comes from
        # StacClient.search output; band tables drop it, so download/repair off a band
        # table won't work — feed a cell_assets-derived df instead.
        missing = [c for c in ("item_id", "asset_name", "href") if c not in df.columns]
        if missing:
            raise ValueError(
                f"download() input is missing required column(s) {missing}; it needs "
                f"item_id, asset_name, href (the StacClient.search output). Got {df.columns}. "
                "Note: band tables carry band_name and no href — pass a cell_assets-derived "
                "DataFrame (or use build_band_table(force_rebuild=True))."
            )
        targets = df.select("item_id", "asset_name", "href").distinct()

        # Driver-collect the (modest) target list.  This is the Serverless-safe
        # alternative to sparkContext.broadcast — which is forbidden on Serverless.
        # For dozens-of-assets scale the collect is negligible.
        rows = [(r["item_id"], r["asset_name"], r["href"]) for r in targets.collect()]
        if not rows:
            from pyspark.sql.types import (
                BooleanType,
                LongType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            empty_schema = StructType(
                [
                    StructField("item_id", StringType()),
                    StructField("asset_name", StringType()),
                    StructField("out_file_path", StringType()),
                    StructField("out_file_sz", LongType()),
                    StructField("is_out_file_valid", BooleanType()),
                    StructField("last_update", TimestampType()),
                ]
            )
            return df.sparkSession.createDataFrame([], empty_schema)

        n = partitions if partitions is not None else len(rows)
        sign = self.sign
        _validate = validate
        _injected_get = _get_fn  # None in production; injectable for tests
        _bbox = tuple(bbox) if bbox is not None else None
        _bbox_crs = bbox_crs
        _max_mpp = max_mpp  # picklable scalar; captured per closure for the UDF
        _allowed_hosts = (
            None if self.allowed_hosts is None else list(self.allowed_hosts)
        )

        from pyspark.sql.types import StringType, StructField, StructType

        _result_schema = StructType(
            [
                StructField("item_id", StringType()),
                StructField("asset_name", StringType()),
                StructField("out_file_path", StringType()),
            ]
        )

        @F.udf(_result_schema)
        def _fetch_by_index(idx):
            """Look up target by range index and download.  Closure captures `rows` +
            all download params — the Serverless-safe substitute for broadcast."""
            from databricks.labs.gbx.stac._download import fetch_validate_publish
            from databricks.labs.gbx.stac._sign import resolve_signer

            item_id, asset_name, href = rows[idx]
            signer = resolve_signer(sign)

            def href_fn():
                if not href:
                    raise ValueError(
                        f"no href available for item_id={item_id!r} asset={asset_name!r}"
                    )
                return signer(href)

            filename = name.format(asset_name=asset_name, item_id=item_id)
            kwargs = {} if _injected_get is None else {"get": _injected_get}
            out_path = fetch_validate_publish(
                href_fn,
                out_dir,
                filename,
                max_tries=max_tries,
                validate=_validate,
                bbox=_bbox,
                bbox_crs=_bbox_crs,
                max_mpp=_max_mpp,
                allowed_hosts=_allowed_hosts,
                **kwargs,
            )
            return (item_id, asset_name, out_path)

        @F.udf("long")
        def _size(path):
            import os

            return os.path.getsize(path) if path and os.path.exists(path) else None

        spark = df.sparkSession
        # spark.range produces a Range SCAN — NOT a shuffle Exchange — so AQE
        # coalescePartitions cannot collapse it back to 1 partition.  This guarantees
        # N concurrent download tasks on Serverless regardless of AQE settings.
        return (
            spark.range(0, len(rows), 1, numPartitions=n)
            .withColumn("_r", _fetch_by_index(F.col("id")))
            .select(
                F.col("_r.item_id").alias("item_id"),
                F.col("_r.asset_name").alias("asset_name"),
                F.col("_r.out_file_path").alias("out_file_path"),
            )
            .withColumn("out_file_sz", _size("out_file_path"))
            .withColumn("is_out_file_valid", F.col("out_file_path").isNotNull())
            .withColumn("last_update", F.current_timestamp())
        )

    def repair(
        self,
        target,
        where: str = "is_out_file_valid = false",
        spark=None,
        out_dir: Optional[str] = None,
    ) -> "DataFrame":
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        is_table = isinstance(target, str)
        df = spark.table(target) if is_table else target
        invalid = df.filter(where)
        # Fail loudly with an actionable message: repair re-downloads, which needs the raw
        # href to re-sign. A band table (band_name, no asset_name/href) can't be repaired —
        # rebuild it from cell_assets instead. (Previously this raised a bare UNRESOLVED_COLUMN.)
        missing = [
            c for c in ("item_id", "asset_name", "href") if c not in invalid.columns
        ]
        if missing:
            raise ValueError(
                f"repair() target is missing required column(s) {missing}; it needs "
                f"item_id, asset_name, href to re-download. Got {invalid.columns}. A band table "
                "carries band_name and no href and cannot be repaired directly — re-run "
                "build_band_table(force_rebuild=True) (which reads cell_assets), or call repair() "
                "on a cell_assets-derived table/DataFrame."
            )
        repaired = self.download(
            invalid.select("item_id", "asset_name", "href"),
            out_dir or _common_dir(invalid),
        )
        if is_table:
            from delta.tables import DeltaTable

            dt = DeltaTable.forName(spark, target)
            (
                dt.alias("t")
                .merge(
                    repaired.alias("u"),
                    "t.item_id = u.item_id AND t.asset_name = u.asset_name",
                )
                .whenMatchedUpdate(
                    set={
                        "out_file_path": "u.out_file_path",
                        "out_file_sz": "u.out_file_sz",
                        "is_out_file_valid": "u.is_out_file_valid",
                        "last_update": "u.last_update",
                    }
                )
                .execute()
            )
        return repaired


def _common_dir(df: "DataFrame") -> str:
    """Infer the output dir from existing out_file_path values (repair convenience)."""
    import os

    from pyspark.sql import functions as F

    row = df.filter(F.col("out_file_path").isNotNull()).select("out_file_path").first()
    if row is None:
        raise ValueError("repair: cannot infer out_dir; pass out_dir=...")
    return os.path.dirname(row["out_file_path"])
