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

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import inspect
import os
import re
import time
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


EULA_FIX = (
    "Fix: the Earthdata user must approve the data provider's application at "
    "https://urs.earthdata.nasa.gov/profile (Applications > Authorized Apps), "
    "e.g. 'NASA GESDISC DATA ARCHIVE' for GES DISC; then re-run (valid files "
    "are kept)."
)


class EarthdataAuthorizationError(RuntimeError):
    """The Earthdata user has not authorised the data provider (EULA not accepted).

    Not a per-file failure: every download from that provider will fail the same
    way, so the client raises instead of recording invalid rows.
    """


_EULA_WORD = re.compile(r"\beula\b", re.IGNORECASE)
_EULA_CLASS = re.compile(r"\bEulaNotAccepted\b")
_URL = re.compile(r"\S+://\S+")
# An explicit status phrase ("status code: 403", "HTTP 403", "403 Forbidden"),
# never a bare number.
_AUTH_STATUS = re.compile(
    r"\b(?:status(?:\s+code)?|http|error|code)\b[:=\s]*40[13]\b"
    r"|\b40[13]\s+(?:forbidden|unauthori[sz]ed|client\s+error)",
    re.IGNORECASE,
)


def _http_status(exc: BaseException) -> Optional[int]:
    """Status code from the exception's ``response`` (requests-style), else None."""
    code = getattr(getattr(exc, "response", None), "status_code", None)
    return code if isinstance(code, int) else None


_SERVER_ERROR_TEXT = re.compile(r"\b5\d\d\s+server\s+error", re.IGNORECASE)
_CMR_ATTEMPTS = 4
_CMR_BACKOFF_S = 2.0
_sleep = time.sleep  # tests replace this to skip the waits


def _chain(exc: BaseException):
    """``exc`` and its ``__cause__`` / ``__context__`` chain, each error once."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = exc.__cause__ or exc.__context__


def _connection_types() -> tuple:
    """The errors of a dropped or timed-out connection: the builtins, and requests' own (which are not subclasses)."""
    try:
        import requests
    except ImportError:
        return (ConnectionError, TimeoutError)
    ex = requests.exceptions
    return (
        ConnectionError,
        TimeoutError,
        ex.ConnectionError,
        ex.Timeout,
        ex.ChunkedEncodingError,
    )


def _is_transient(exc: BaseException) -> bool:
    """A CMR failure worth another try: a 5xx, or a dropped or timed-out connection.

    earthaccess raises ``RuntimeError(response.text) from <requests.HTTPError>``, so the status is on the cause, not
    on the error raised: the whole cause/context chain is read. A status anywhere in it decides (5xx retries, 4xx never
    does); without one a connection-level error retries, and so does a message that names a 5xx server error.
    Anything else, a bad argument for instance, is not retried.
    """
    chain = list(_chain(exc))
    for e in chain:
        status = _http_status(e)
        if status is not None:
            return status >= 500
    if any(isinstance(e, _connection_types()) for e in chain):
        return True
    return any(_SERVER_ERROR_TEXT.search(_URL.sub(" ", str(e))) for e in chain)


def _is_eula_failure(exc: BaseException) -> bool:
    """True when ``exc`` or anything in its cause/context chain is a EULA refusal.

    Matches (a) earthaccess's ``EulaNotAccepted`` by type name anywhere in the MRO,
    or by name inside a wrapping error's message; or (b) for earthaccess releases
    without that class, an HTTP 401/403 whose text mentions the word "EULA". The
    status comes from the exception's ``response`` when present, else from the
    message with URLs stripped (so a URL containing "eula" or "403" never counts);
    a 500 is not a match unless the message names ``EulaNotAccepted``.

    A message that names ``EulaNotAccepted`` matches without any status: earthaccess
    raises that class only on a 401/403 refusal, so the name alone is conclusive.
    """
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if any(c.__name__ == "EulaNotAccepted" for c in type(exc).__mro__):
            return True
        text = _URL.sub(" ", str(exc))
        if _EULA_CLASS.search(text):
            return True
        if _EULA_WORD.search(text):
            status = _http_status(exc)
            if status in (401, 403) or (
                status is None and _AUTH_STATUS.search(text) is not None
            ):
                return True
        exc = exc.__cause__ or exc.__context__
    return False


def _accepts_kwarg(fn, name: str) -> bool:
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return name in params or any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
    )


class EarthdataClient:
    def __init__(self, _earthaccess=None):
        self._earthaccess = _earthaccess

    def _ea(self):
        if self._earthaccess is not None:
            return self._earthaccess
        # tqdm reads TQDM_DISABLE once at import: set it BEFORE earthaccess pulls
        # tqdm in (the only switch on releases without show_progress). A
        # user-set value wins.
        os.environ.setdefault("TQDM_DISABLE", "1")
        try:
            import earthaccess
        except ImportError as exc:
            raise ImportError(
                "earthaccess is required for the NASA Earthdata / EMIT client. "
                "Install it as a separate step after the env wheel: "
                "pip install 'earthaccess>=0.11,<1' 'botocore<1.44.0'."
            ) from exc
        return earthaccess

    def login(self) -> None:
        # strategy="environment" reads EARTHDATA_TOKEN (Databricks secret -> env).
        self._ea().login(strategy="environment")

    @staticmethod
    def _cmr_search(ea, kwargs) -> list:
        """``ea.search_data(**kwargs)``, retried on a 5xx or a dropped connection.

        Waits 2 s, 4 s, 8 s between four tries; a 4xx or any other error is raised at once.
        """
        for attempt in range(1, _CMR_ATTEMPTS + 1):
            try:
                return list(ea.search_data(**kwargs))
            except Exception as exc:
                if attempt == _CMR_ATTEMPTS or not _is_transient(exc):
                    raise
                _sleep(_CMR_BACKOFF_S * 2 ** (attempt - 1))
        raise AssertionError("unreachable")  # pragma: no cover

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
            for g in self._cmr_search(ea, _kwargs):
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
        # notebook that floods the output. Newer earthaccess takes show_progress;
        # older ones rely on TQDM_DISABLE, which _ea() sets before the import.
        dl_kwargs = {"local_path": out_dir}
        if _accepts_kwarg(ea.download, "show_progress"):
            dl_kwargs["show_progress"] = False
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
                ret = ea.download([href], **dl_kwargs)
                if ret:
                    got = str(ret[0])
            except Exception as exc:
                if _is_eula_failure(exc):
                    raise EarthdataAuthorizationError(
                        f"Earthdata download refused for {href}: the user has not "
                        f"accepted the data provider's EULA ({exc}). {EULA_FIX}"
                    ) from exc
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
