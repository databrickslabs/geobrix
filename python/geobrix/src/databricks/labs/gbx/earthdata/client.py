"""EarthdataClient — generic NASA Earthdata (LP DAAC / CMR) data client via earthaccess.

The Earthdata analog of ``StacClient``: owns Earthdata-Login auth (the
``EARTHDATA_TOKEN`` env var, set from a Databricks secret), CMR search
(``earthaccess.search_data``), and a download loop that validates each asset and
returns the shared result contract — ``item_id, asset_name, out_file_path,
out_file_sz, is_out_file_valid, last_update`` — with a ``repair()`` Delta MERGE.

Per-dataset downloaders supply the CMR short-names, an ``asset_fn(url)`` classifier
(return an asset label or ``None`` to skip a link), and a ``validate_fn(path,
asset)`` predicate. Download is DRIVER-SIDE (EMIT/LP DAAC auth mints short-lived
per-request credentials that don't fan out like anonymous hrefs) — fine at demo
scale. Serverless-safe: no runtime Spark-config mutation or JVM-bridge access.

Injection seam: pass ``_earthaccess`` (a module-like object with
login/search_data/download) for offline unit tests.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Callable, List, Optional, Sequence, Tuple

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RESULT_SCHEMA = StructType(
    [
        StructField("item_id", StringType(), False),
        StructField("asset_name", StringType(), False),
        StructField("out_file_path", StringType(), True),
        StructField("out_file_sz", LongType(), True),
        StructField("is_out_file_valid", BooleanType(), True),
        StructField("last_update", TimestampType(), True),
    ]
)

_DISCOVER_SCHEMA = StructType(
    [
        StructField("item_id", StringType(), False),
        StructField("asset_name", StringType(), False),
        StructField("href", StringType(), False),
    ]
)


def _norm_temporal(temporal):
    """Normalize a temporal window to the (start, end) tuple earthaccess/CMR wants.

    Callers may pass the STAC-style slash string ``"start/end"`` (as the vapor-eyes
    notebooks do, sharing one ``DATE_WINDOW`` across STAC and Earthdata searches);
    earthaccess expects a 2-tuple and mis-parses a single slash string into an
    out-of-range tz offset. A tuple/list passes through; ``None`` stays ``None``.
    """
    if temporal is None:
        return None
    if isinstance(temporal, str):
        parts = [p.strip() for p in temporal.split("/") if p.strip()]
        return tuple(parts) if len(parts) == 2 else (temporal,)
    return tuple(temporal)


class EarthdataClient:
    def __init__(self, _earthaccess=None):
        self._earthaccess = _earthaccess

    def _ea(self):
        if self._earthaccess is not None:
            return self._earthaccess
        try:
            import earthaccess
        except ImportError as exc:
            raise ImportError(
                "earthaccess is required for the NASA Earthdata / EMIT client. "
                "Install the optional extra: geobrix[earthdata] "
                "(e.g. geobrix[light_env6,earthdata])."
            ) from exc
        return earthaccess

    def login(self) -> None:
        # strategy="environment" reads EARTHDATA_TOKEN (Databricks secret -> env).
        self._ea().login(strategy="environment")

    def _search_rows(
        self,
        short_names: Sequence[str],
        version: str,
        bbox: Sequence[float],
        temporal,
        asset_fn: Callable[[str], Optional[str]],
    ) -> List[Tuple[str, str, str]]:
        ea = self._ea()
        w, s, e, n = bbox
        temporal = _norm_temporal(temporal)
        out: List[Tuple[str, str, str]] = []
        for short in short_names:
            _kwargs = dict(
                short_name=short,
                version=version,
                bounding_box=(w, s, e, n),
            )
            if temporal is not None:
                _kwargs["temporal"] = temporal
            for g in ea.search_data(**_kwargs):
                for url in g.data_links(access="external"):
                    asset = asset_fn(url)
                    if asset is None:
                        continue
                    item_id = url.split("/")[-1].rsplit(".", 1)[0]
                    out.append((item_id, asset, url))
        return out

    def search(
        self,
        short_names: Sequence[str],
        version: str,
        bbox: Sequence[float],
        temporal,
        asset_fn: Callable[[str], Optional[str]],
        spark=None,
    ):
        """Metadata-only CMR search → DataFrame[item_id, asset_name, href]."""
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        rows = self._search_rows(short_names, version, bbox, temporal, asset_fn)
        return spark.createDataFrame(rows, _DISCOVER_SCHEMA)

    def download(
        self,
        rows,
        out_dir: str,
        validate_fn: Callable[[str, str], bool],
        force: bool = False,
        spark=None,
    ):
        """Fetch each (item_id, asset_name, href) row to out_dir, validate, and
        return the 6-column result frame. ``rows`` is a DataFrame (with those
        columns) or a list of tuples."""
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        ea = self._ea()
        # earthaccess renders a tqdm/ipywidgets progress bar per file; in a
        # Databricks notebook that surfaces as a stack of "Loading the widget is
        # taking longer than expected" placeholders. Force tqdm to no-op.
        os.environ["TQDM_DISABLE"] = "1"
        if hasattr(rows, "collect"):
            rows = [
                (r["item_id"], r["asset_name"], r["href"])
                for r in rows.select("item_id", "asset_name", "href").collect()
            ]
        os.makedirs(out_dir, exist_ok=True)
        results = []
        for item_id, asset, href in rows:
            dest = os.path.join(out_dir, os.path.basename(href))
            if not force and os.path.exists(dest) and validate_fn(dest, asset):
                results.append(
                    (item_id, asset, dest, os.path.getsize(dest), True, datetime.now())
                )
                continue
            # Pass the href STRING (not a granule wrapper): earthaccess.download
            # dispatches via multimethod and only overloads str/list[str] and real
            # DataGranule objects — a custom adapter raises DispatchError. Honor the
            # returned path; fall back to the basename in out_dir.
            got = None
            try:
                ret = ea.download([href], local_path=out_dir)
                if ret:
                    got = str(ret[0])
            except Exception:
                got = None
            cand = got if (got and os.path.exists(got)) else dest
            if os.path.exists(cand) and validate_fn(cand, asset):
                results.append(
                    (item_id, asset, cand, os.path.getsize(cand), True, datetime.now())
                )
            else:
                results.append((item_id, asset, None, 0, False, datetime.now()))
        return spark.createDataFrame(results, RESULT_SCHEMA)

    def repair(
        self,
        target,
        validate_fn: Callable[[str, str], bool],
        where: str = "is_out_file_valid = false",
        spark=None,
        out_dir: Optional[str] = None,
    ):
        """Re-fetch invalid/missing assets; MERGE into a Delta table by
        (item_id, asset_name). ``target`` is a table name (str → MERGE in place) or
        a DataFrame (returns the repaired frame). Requires item_id/asset_name/href.
        """
        from pyspark.sql import SparkSession

        spark = spark or SparkSession.getActiveSession()
        is_table = isinstance(target, str)
        df = spark.table(target) if is_table else target
        invalid = df.filter(where)
        missing = [
            c for c in ("item_id", "asset_name", "href") if c not in invalid.columns
        ]
        if missing:
            raise ValueError(
                f"repair() needs columns {missing} on the target (persist href to "
                f"enable repair)."
            )
        rows = [
            (r["item_id"], r["asset_name"], r["href"])
            for r in invalid.select("item_id", "asset_name", "href").collect()
        ]
        if not rows:
            return invalid
        if out_dir is None:
            paths = [
                r["out_file_path"]
                for r in df.select("out_file_path").collect()
                if r["out_file_path"]
            ]
            out_dir = os.path.dirname(paths[0]) if paths else "."
        self.login()
        repaired = self.download(rows, out_dir, validate_fn, force=True, spark=spark)
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
