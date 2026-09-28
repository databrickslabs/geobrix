"""OvertureClient — distributed, AOI-driven Overture Maps GeoParquet data source.

Mirrors gbx.stac.StacClient: a static-catalog discovery step (driver-side,
metadata-only), then DISTRIBUTED asset I/O. Default I/O path is a distributed
Spark read of Overture GeoParquet over the cloud path with bbox-struct predicate
pushdown (AOI rows only), written to a UC Volume + an optional metadata Delta
table; an HTTP-href whole-file download is the fallback. Serverless-safe:
parallelism only via repartition(N, col); no spark.conf/cache/persist/.rdd.

Injection seams (offline tests): _catalog_opener (returns a pystac.Catalog-shaped
object) and _get_fn (an HTTP fetcher passed to the fallback downloader).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

OVERTURE_CATALOG = "https://stac.overturemaps.org/catalog.json"


def _overturemaps_cli_path():
    """Return the Path to the overturemaps CLI if runnable, else None."""
    bin_dir = Path(sys.executable).parent
    exe = bin_dir / "overturemaps"
    if exe.exists() and os.access(exe, os.X_OK):
        return exe
    found = shutil.which("overturemaps")
    return Path(found) if found else None


def _overturemaps_cli_available():
    """Return True if the official overturemaps CLI is runnable."""
    return _overturemaps_cli_path() is not None


def _run_overture_download(cli, *, bbox_str, type_, local_out, retries=2):
    """Run `overturemaps download` resiliently, returning on first success.

    The ``--stac`` mode (the CLI default) makes an outbound HTTP call to the
    Overture STAC catalog to limit which Parquet files are fetched. That hop can
    fail transiently (catalog unreachable / timeout / TLS) and the CLI exits
    non-zero. ``--no-stac`` skips the catalog and reads the S3 dataset directly —
    it returns the SAME data (slightly faster, no catalog dependency).

    Strategy: try ``--stac`` up to ``retries`` times, then fall back to
    ``--no-stac`` once. Only if every attempt fails do we raise — with the last
    command's STDERR included in the message (subprocess.run(check=True) otherwise
    hides it inside CalledProcessError, leaving the failure undiagnosable).
    """
    base = [
        str(cli),
        "download",
        f"--bbox={bbox_str}",
        "-f",
        "geoparquet",
        "--type",
        type_,
        "-o",
        local_out,
    ]
    # (flag, attempts): the STAC catalog path first (the optimization), then a
    # single direct-S3 fallback that does not depend on the catalog being up.
    attempts = [("--stac", max(1, retries)), ("--no-stac", 1)]
    last = None
    for flag, n in attempts:
        cmd = [base[0], base[1], flag, *base[2:]]
        for _ in range(n):
            last = subprocess.run(cmd, capture_output=True, text=True)
            if last.returncode == 0:
                return
    # All attempts (stac retries + no-stac fallback) failed — surface the cause.
    stderr = (last.stderr or "").strip() if last is not None else ""
    raise RuntimeError(
        f"overturemaps download failed for type '{type_}' (bbox {bbox_str}) "
        f"after --stac retries and a --no-stac fallback. "
        f"Last exit code: {last.returncode if last else 'n/a'}. "
        f"CLI stderr:\n{stderr or '(empty)'}"
    )


# Spark schema for discover() output — pinned; SP2/SP3 depend on it.
_DISCOVER_COLS = ["theme", "type", "href", "asset_bbox", "release"]

# Columns for the download() metadata output — pinned; Tasks 6-10 + SP3 depend on it.
_META_COLS = [
    "theme",
    "type",
    "source",
    "path",
    "out_file_sz",
    "is_out_file_valid",
    "last_update",
    "asset_bbox",
    "release",
    "href",
]


def _meta_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("source", StringType()),
            StructField("out_file_sz", LongType()),
            StructField("is_out_file_valid", BooleanType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
            StructField("href", StringType()),
        ]
    )


def _meta_dataframe(spark, meta_rows, partitions):
    """Build the download metadata DataFrame.

    Repartitions by (theme, type, source) using column-hash (NOT number-only) so
    the result stays distributed on Serverless (AQE cannot coalesce a column-hash
    repartition to 1). Aliases `source` as `path` and adds `last_update`.
    """
    from pyspark.sql import functions as F

    cols = [
        "theme",
        "type",
        "source",
        "out_file_sz",
        "is_out_file_valid",
        "asset_bbox",
        "release",
        "href",
    ]
    if not meta_rows:
        df = spark.createDataFrame([], _meta_schema())
    else:
        df = spark.createDataFrame(
            [tuple(r[c] for c in cols) for r in meta_rows], _meta_schema()
        )
    n = max(1, partitions or 1)
    # Column-hash repartition: on Serverless, number-only repartition(N) is
    # AQE-coalesced to 1 (serial); hashing by real columns keeps it distributed.
    return (
        df.repartition(n, F.col("theme"), F.col("type"), F.col("source"))
        .withColumn("path", F.col("source"))
        .withColumn("last_update", F.current_timestamp())
        .select(
            "theme",
            "type",
            "source",
            "path",
            "out_file_sz",
            "is_out_file_valid",
            "last_update",
            "asset_bbox",
            "release",
            "href",
        )
    )


def _is_valid_parquet(path: str) -> bool:
    """True iff the parquet opens (pyarrow). Validity = opens, not raster-decodable."""
    try:
        import pyarrow.parquet as pq

        pq.ParquetFile(path).metadata  # touch metadata to force a read
        return True
    except Exception:  # noqa: BLE001
        return False


def _discover_schema():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("theme", StringType()),
            StructField("type", StringType()),
            StructField("href", StringType()),
            StructField("asset_bbox", ArrayType(DoubleType())),
            StructField("release", StringType()),
        ]
    )


class OvertureClient:
    def __init__(
        self,
        catalog: str = OVERTURE_CATALOG,
        release: Optional[str] = None,
        _catalog_opener=None,
        _get_fn=None,
        _item_loader=None,
    ):
        self.catalog = catalog
        self.release = release
        self._catalog_opener = _catalog_opener
        self._get_fn = _get_fn
        self._item_loader = _item_loader

    def _open_catalog(self):
        import pystac

        return pystac.Catalog.from_file(self.catalog)

    def _opener(self):
        return (
            self._catalog_opener
            if self._catalog_opener is not None
            else self._open_catalog
        )

    def _download_distributed(
        self, assets_df, out_dir, *, bbox, validate, partitions
    ) -> "DataFrame":
        """Performant default: distributed read of each asset's GeoParquet with a
        bbox-struct predicate pushdown, AOI subset written to the Volume per
        (theme, type). Returns the metadata rows. Serverless-safe: repartition by
        column only; no spark.conf/cache/persist/.rdd.

        Parameters
        ----------
        assets_df:
            DataFrame from discover() — columns: theme, type, href, asset_bbox, release.
        out_dir:
            Root output directory (UC Volume path or local path).
        bbox:
            AOI as (minx, miny, maxx, maxy).
        validate:
            If True, read-back one row from the written parquet to confirm it is valid.
        partitions:
            Target partition count for repartition(N, col). Must be >= 1.
        """
        import hashlib
        import os

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        from databricks.labs.gbx.sample._overture_discover import normalize_bbox

        spark = SparkSession.getActiveSession()
        minx, miny, maxx, maxy = normalize_bbox(bbox)
        assets = assets_df.select(*_DISCOVER_COLS).collect()

        meta_rows = []
        for a in assets:
            df = spark.read.parquet(a["href"])
            # bbox-struct predicate pushdown when the Overture `bbox` struct is present.
            if "bbox" in df.columns:
                df = df.filter(
                    (F.col("bbox.xmin") <= F.lit(maxx))
                    & (F.col("bbox.xmax") >= F.lit(minx))
                    & (F.col("bbox.ymin") <= F.lit(maxy))
                    & (F.col("bbox.ymax") >= F.lit(miny))
                )
            # Per-asset unique subdirectory: strip query params then hash the
            # canonical href so each asset gets a STABLE, DETERMINISTIC token.
            # Multiple assets sharing the same (theme, type) — e.g. Overture
            # buildings shards — each write to their own subdir; mode("overwrite")
            # is now safe and idempotent (re-running the same asset hits the same
            # subdir). Without this, the 2nd asset's overwrite deletes the 1st
            # asset's data → silent data loss.
            canonical = a["href"].split("?")[0]
            token = hashlib.sha1(canonical.encode()).hexdigest()[:12]
            target = os.path.join(out_dir, a["theme"], a["type"], token)
            # Hash-by-column repartition (NOT number-only): on Serverless a
            # round-robin repartition(N) is AQE-coalesced to 1 (serial). Hash by a
            # real source column so the per-asset row groups spread across cores.
            # Prefer the Overture `id` column; else hash by the first column.
            key = "id" if "id" in df.columns else df.columns[0]
            (
                df.repartition(partitions, F.col(key))
                .write.mode("overwrite")
                .parquet(target)
            )
            valid = True
            if validate:
                try:
                    spark.read.parquet(target).limit(1).count()
                except Exception:
                    valid = False
            try:
                sz = sum(
                    os.path.getsize(os.path.join(target, f))
                    for f in os.listdir(target)
                    if f.endswith(".parquet")
                )
            except OSError:
                sz = None
            meta_rows.append(
                {
                    "theme": a["theme"],
                    "type": a["type"],
                    "source": target,
                    "out_file_sz": sz,
                    "is_out_file_valid": valid,
                    "asset_bbox": a["asset_bbox"],
                    "release": a["release"],
                    "href": a["href"],
                }
            )

        return _meta_dataframe(spark, meta_rows, partitions)

    def _download_via_cli(
        self, assets, out_dir, *, bbox, validate, force=False
    ) -> "DataFrame":
        """Download via the official overturemaps CLI (--stac bbox pushdown).

        Runs `overturemaps download --stac --bbox=W,S,E,N -f geoparquet --type <type>`
        for each distinct (theme, type) pair. Sequential, driver-side. Requires bbox.
        Volume-safe: assembles locally then shutil.copyfile to target.
        Idempotent: skips the CLI when the target already exists and is a valid
        parquet, unless force=True.
        Returns a metadata DataFrame matching _meta_dataframe schema.
        """
        from pyspark.sql import SparkSession

        from databricks.labs.gbx.sample._overture_discover import (
            normalize_bbox,
            resolve_release,
        )

        spark = SparkSession.getActiveSession()
        minx, miny, maxx, maxy = normalize_bbox(bbox)
        bbox_str = f"{minx},{miny},{maxx},{maxy}"
        cli = _overturemaps_cli_path()

        pairs = assets.select("theme", "type").distinct().collect()
        rel = resolve_release(self._opener(), self.release)

        meta_rows = []
        for row in pairs:
            theme = row["theme"]
            type_ = row["type"]
            target_dir = os.path.join(out_dir, theme, type_)
            os.makedirs(target_dir, exist_ok=True)
            target = os.path.join(target_dir, f"{type_}.parquet")

            # Idempotent skip: valid existing target is reused unless force=True.
            if not force and os.path.exists(target) and _is_valid_parquet(target):
                sz = os.path.getsize(target)
                valid = True
                meta_rows.append(
                    {
                        "theme": theme,
                        "type": type_,
                        "source": target,
                        "out_file_sz": sz,
                        "is_out_file_valid": valid,
                        "asset_bbox": list(normalize_bbox(bbox)),
                        "release": rel,
                        "href": "",
                    }
                )
                continue

            tmpdir = new_local_temp_dir("gbx_overture_cli_")
            try:
                local_out = os.path.join(tmpdir, f"{type_}.parquet")
                _run_overture_download(
                    cli, bbox_str=bbox_str, type_=type_, local_out=local_out
                )
                shutil.copyfile(local_out, target)
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

            sz = os.path.getsize(target) if os.path.exists(target) else None
            valid = True
            if validate:
                try:
                    spark.read.parquet(target).limit(1).count()
                except Exception:  # noqa: BLE001
                    valid = False

            meta_rows.append(
                {
                    "theme": theme,
                    "type": type_,
                    "source": target,
                    "out_file_sz": sz,
                    "is_out_file_valid": valid,
                    "asset_bbox": list(normalize_bbox(bbox)),
                    "release": rel,
                    "href": "",
                }
            )

        n = max(1, len(meta_rows))
        return _meta_dataframe(spark, meta_rows, partitions=n)

    def _download_fallback(
        self, assets_df, out_dir, *, validate, max_tries, partitions
    ) -> "DataFrame":
        """Fallback: whole-file HTTP download fanned out by href (column-hash
        repartition, Serverless-safe). Temp-file then sequential copy (Volume-safe).
        Returns the same metadata schema as the distributed path."""
        import os
        import shutil

        from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        get_fn = self._get_fn  # None in production; injectable for tests

        row_schema = StructType(
            [
                StructField("theme", StringType()),
                StructField("type", StringType()),
                StructField("source", StringType()),
                StructField("out_file_sz", LongType()),
                StructField("is_out_file_valid", BooleanType()),
                StructField("asset_bbox", ArrayType(DoubleType())),
                StructField("release", StringType()),
                StructField("href", StringType()),
            ]
        )

        @F.udf(row_schema)
        def _fetch(theme, type_, href, asset_bbox, release):
            getter = get_fn
            if getter is None:
                import requests

                getter = requests.get
            target_dir = os.path.join(out_dir, theme, type_)
            os.makedirs(target_dir, exist_ok=True)
            basename = os.path.basename(href.split("?")[0]) or "asset.parquet"
            outpath = os.path.join(target_dir, basename)
            # idempotent skip: a present, openable file is left as-is
            if os.path.exists(outpath) and _is_valid_parquet(outpath):
                sz = os.path.getsize(outpath)
                return (theme, type_, outpath, sz, True, asset_bbox, release, href)
            for _ in range(max(1, max_tries)):
                tmpd = new_local_temp_dir("gbx_overture_")
                try:
                    local = os.path.join(tmpd, basename)
                    resp = getter(href, timeout=100, stream=True)
                    resp.raise_for_status()
                    with open(local, "wb") as fh:
                        for chunk in resp.iter_content(1024 * 1024):
                            if chunk:
                                fh.write(chunk)
                    ok = (not validate) or _is_valid_parquet(local)
                    if ok:
                        shutil.copyfile(local, outpath)
                        sz = os.path.getsize(outpath)
                        return (
                            theme,
                            type_,
                            outpath,
                            sz,
                            True,
                            asset_bbox,
                            release,
                            href,
                        )
                except Exception:  # noqa: BLE001
                    pass
                finally:
                    shutil.rmtree(tmpd, ignore_errors=True)
            return (theme, type_, None, None, False, asset_bbox, release, href)

        n = max(1, partitions or 1)
        fetched = (
            assets_df.select(*_DISCOVER_COLS)
            # column-hash repartition by href (NOT number-only) for Serverless.
            .repartition(n, F.col("href"))
            .withColumn(
                "_m",
                _fetch("theme", "type", "href", "asset_bbox", "release"),
            )
            .select("_m.*")
            .withColumn("path", F.col("source"))
            .withColumn("last_update", F.current_timestamp())
            .select(
                "theme",
                "type",
                "source",
                "path",
                "out_file_sz",
                "is_out_file_valid",
                "last_update",
                "asset_bbox",
                "release",
                "href",
            )
        )
        return fetched

    def _merge_metadata(self, meta_df, table):
        """Create or UPSERT the metadata Delta table keyed by (theme, type, source)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if not spark.catalog.tableExists(table):
            meta_df.write.format("delta").mode("overwrite").saveAsTable(table)
            return meta_df

        from delta.tables import DeltaTable

        dt = DeltaTable.forName(spark, table)
        (
            dt.alias("t")
            .merge(
                meta_df.alias("u"),
                "t.theme = u.theme AND t.type = u.type AND t.source = u.source",
            )
            .whenMatchedUpdate(
                set={
                    "path": "u.path",
                    "out_file_sz": "u.out_file_sz",
                    "is_out_file_valid": "u.is_out_file_valid",
                    "last_update": "u.last_update",
                    "asset_bbox": "u.asset_bbox",
                    "release": "u.release",
                    "href": "u.href",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        return meta_df

    _CLOUD_SCHEMES = ("s3://", "s3a://", "abfs://", "abfss://", "gs://", "wasbs://")

    def download(
        self,
        assets_df,
        out_dir,
        *,
        bbox=None,
        table=None,
        validate=True,
        max_tries=5,
        partitions=None,
        force: bool = False,
    ) -> "DataFrame":
        """Distributed download of discovered assets to out_dir (a Volume).

        Cloud predicate — routes to the distributed read path when ALL hrefs:
        - start with a cloud object-store scheme (``s3://``, ``s3a://``, ``abfs://``,
          ``abfss://``, ``gs://``, ``wasbs://``), OR
        - start with ``/`` (FUSE-mounted UC Volumes such as ``/Volumes/...``, which
          Spark can read directly without HTTP).

        Any other scheme (``http://``, ``https://``, etc.) falls through to the
        whole-file HTTP download fallback (``_download_fallback``).

        When ``table=<name>`` is set, the per-asset metadata DataFrame is
        persisted to a Delta table via idempotent MERGE keyed by
        ``(theme, type, source)``: the first call creates the table; subsequent
        calls update the volatile columns (``path``, ``out_file_sz``,
        ``is_out_file_valid``, ``last_update``, ``asset_bbox``, ``release``,
        ``href``) so the catalog stays queryable and re-runnable without
        accumulating duplicate rows. The returned DataFrame is unchanged.

        force:
            When True, re-download even if the target already exists and is valid.
            Only applies to the CLI path; the distributed and fallback paths manage
            their own idempotency independently.

        Serverless-safe; idempotent skip on valid existing targets.
        """
        assets = assets_df.select(*_DISCOVER_COLS)
        n = partitions if partitions is not None else max(1, assets.count())

        # CLI fast-path: bbox present + overturemaps CLI available → server-side pushdown.
        if bbox is not None and _overturemaps_cli_available():
            meta = self._download_via_cli(
                assets, out_dir, bbox=bbox, validate=validate, force=force
            )
            if table is not None:
                meta = self._merge_metadata(meta, table)
            return meta

        hrefs = [r["href"] for r in assets.select("href").distinct().collect()]
        is_cloud = bool(hrefs) and all(
            any(h.startswith(s) for s in self._CLOUD_SCHEMES)
            or h.startswith("/")  # local Volume / FUSE path is Spark-readable
            for h in hrefs
        )

        if is_cloud:
            meta = self._download_distributed(
                assets, out_dir, bbox=bbox, validate=validate, partitions=n
            )
        else:
            meta = self._download_fallback(
                assets, out_dir, validate=validate, max_tries=max_tries, partitions=n
            )

        if table is not None:
            meta = self._merge_metadata(meta, table)
        return meta

    def read(self, source, theme=None, type=None, bbox=None) -> "DataFrame":
        """Load downloaded GeoParquet back into Spark with an optional bbox AOI filter.

        source may be a Volume directory, a metadata Delta table NAME, or a
        metadata DataFrame carrying a source/path column pointing at per-asset paths.
        """
        from pyspark.sql import DataFrame as _DF
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.getActiveSession()

        def _read_paths(paths):
            # union-read each per-asset path (recursive parquet under each)
            if not paths:
                raise ValueError(
                    f"read: no asset paths found in {source!r}; nothing to read"
                )
            dfs = [spark.read.parquet(p) for p in paths]
            out = dfs[0]
            for d in dfs[1:]:
                out = out.unionByName(d, allowMissingColumns=True)
            return out

        def _looks_like_table_name(s):
            """True iff the string could be a catalog/schema.table identifier.

            A table name never starts with "/" or a cloud scheme prefix, and
            never contains path separators — those are filesystem paths, not
            SQL identifiers. Without this guard, spark.catalog.tableExists()
            raises ParseException on a bare path string.
            """
            import os

            _CLOUD_PREFIXES = (
                "s3://",
                "s3a://",
                "abfs://",
                "abfss://",
                "gs://",
                "wasbs://",
            )
            if s.startswith("/") or any(s.startswith(p) for p in _CLOUD_PREFIXES):
                return False
            if os.sep in s or "/" in s:
                return False
            return True

        if isinstance(source, _DF):
            col = "source" if "source" in source.columns else "path"
            paths = [r[col] for r in source.select(col).distinct().collect()]
            df = _read_paths(paths)
        elif (
            isinstance(source, str)
            and _looks_like_table_name(source)
            and spark.catalog.tableExists(source)
        ):
            meta = spark.table(source)
            col = "source" if "source" in meta.columns else "path"
            paths = [r[col] for r in meta.select(col).distinct().collect()]
            df = _read_paths(paths)
        else:
            # a Volume directory: read parquet recursively (per theme/type subdirs)
            import os

            base = source
            if theme is not None and type is not None:
                base = os.path.join(source, theme, type)
            df = spark.read.option("recursiveFileLookup", "true").parquet(base)

        if bbox is not None and "bbox" in df.columns:
            from databricks.labs.gbx.sample._overture_discover import normalize_bbox

            minx, miny, maxx, maxy = normalize_bbox(bbox)
            df = df.filter(
                (F.col("bbox.xmin") <= F.lit(maxx))
                & (F.col("bbox.xmax") >= F.lit(minx))
                & (F.col("bbox.ymin") <= F.lit(maxy))
                & (F.col("bbox.ymax") >= F.lit(miny))
            )
        return df

    def discover(self, bbox, themes=None, release=None) -> "DataFrame":
        """One row per intersecting GeoParquet asset for the AOI.

        Columns: theme, type, href, asset_bbox, release. themes=None => ALL.
        Driver-side + metadata-only (lightweight); asset I/O happens in download().
        """
        from pyspark.sql import SparkSession

        from databricks.labs.gbx.sample._overture_discover import (
            expand_themes,
            resolve_release,
            traverse_catalog,
        )

        opener = self._opener()
        rel = resolve_release(opener, release or self.release)
        pairs = expand_themes(themes)

        rows = traverse_catalog(opener, bbox, pairs, self._item_loader)

        for r in rows:
            r["release"] = rel

        spark = SparkSession.getActiveSession()
        schema = _discover_schema()
        if not rows:
            return spark.createDataFrame([], schema)
        ordered = [tuple(r[c] for c in _DISCOVER_COLS) for r in rows]
        return spark.createDataFrame(ordered, schema)


def download_overture_aoi(
    bbox, out_dir, themes=None, release=None, table=None
) -> "DataFrame":
    """One-shot: discover the AOI's Overture assets and download them to out_dir.

    Constructs a default OvertureClient, discovers (themes=None => all), then
    downloads with the AOI bbox pushdown and an optional metadata Delta table.

    Parameters
    ----------
    bbox
        Area of interest as (minx, miny, maxx, maxy).
    out_dir
        Root output directory (UC Volume path or local path).
    themes
        List of theme names (e.g. ["buildings"]). None => all themes.
    release
        Release date (e.g. "2024-07-01"). None => latest available.
    table
        Optional metadata Delta table name. If provided, metadata is persisted
        via idempotent MERGE keyed by (theme, type, source).

    Returns
    -------
    DataFrame
        Metadata DataFrame with columns matching _META_COLS, one row per
        discovered and downloaded asset.
    """
    client = OvertureClient(release=release)
    assets = client.discover(bbox, themes=themes, release=release)
    return client.download(assets, out_dir, bbox=bbox, table=table)
