"""Does a GeoBrix function registered in one Lakeflow flow resolve in another?

- fr_a_tiles      registers GeoBrix (gbx.register) and builds a tiny MVT pyramid (gbx_st_asmvt_pyramid).
- fr_b_api_noreg  reads fr_a_tiles and calls the Python API pmtiles_agg(...) with no register call.
- fr_c_sql_noreg  reads fr_a_tiles and calls gbx_pmtiles_agg through F.expr with no register call.
- fr_d_api_reg    registers GeoBrix in its own body, then calls pmtiles_agg(...).
- fr_e_diag       reads fr_a_tiles and only records what this flow's requests can see.
- fr_f_api_one    registers only gbx_pmtiles_agg in its body (manager.register(only=...)), then pmtiles_agg(...).

Each body writes one JSON line of diagnostics to <probe.diag_dir>/<probe.label>/ when it runs (and puts the same
line in its output): the thread, the Spark Connect user-context extensions the thread's requests carry, and
whether the functions exist as this flow sees them. A body that fails still leaves its line.

With ``probe.patch_flow_key=true`` each body first swaps ``_api._flow`` for ``_proposed_flow`` (a fix under test):
on a classic session it keys the Python API's registration cache by the JVM session the pipeline gives each run of
a dataset function.
"""

import json
import os
import re
import threading
import time
import uuid

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

META = json.dumps({"vector_layers": [{"id": "pts", "fields": {"name": "String"}}]})
NAMES = ("gbx_pmtiles_agg", "gbx_st_asmvt_pyramid")


def _spark():
    spark = SparkSession.getActiveSession()
    if spark.conf.get("probe.patch_flow_key", "false") == "true":
        _patch_flow_key()
    return spark


def _proposed_flow(spark) -> tuple:
    """A proposed ``_api._flow``: on a classic session, the JVM session it talks to now (a Lakeflow pipeline gives each
    dataset function's run its own); on Connect, the shipped pipeline-analysis-context key."""
    from databricks.labs.gbx import _api

    if _api._client(spark) is not None:
        return _SHIPPED_FLOW[0](spark)
    try:
        js = spark._jsparkSession
    except Exception:  # noqa: BLE001
        return ()
    return (("jvm-session", getattr(js, "_target_id", None) or id(js)),)


_SHIPPED_FLOW = []


def _patch_flow_key():
    from databricks.labs.gbx import _api

    if not _SHIPPED_FLOW:
        _SHIPPED_FLOW.append(_api._flow)
        _api._flow = _proposed_flow


def _readable(value: bytes) -> list:
    return re.findall(rb"[\x20-\x7e]{3,}", bytes(value or b""))


def _exts(pairs) -> list:
    return [
        {"type": str(getattr(e, "type_url", "")).rsplit(".", 1)[-1],
         "text": [t.decode() for t in _readable(getattr(e, "value", b""))]}
        for _id, e in (pairs or ())
    ]


def _diag(flow: str, step: str, spark, **extra) -> dict:
    """What this thread's requests carry and see, now. Never raises."""
    d = {"flow": flow, "step": step, "t": round(time.time(), 3), "thread": threading.current_thread().name, **extra}
    d["patched"] = bool(_SHIPPED_FLOW)
    try:
        import pyspark

        d["pyspark"] = pyspark.__version__
        from databricks.labs.gbx import _build_info

        d["gbx_sha"] = _build_info.GIT_SHA[:9]
    except Exception as exc:  # noqa: BLE001
        d["info_error"] = repr(exc)[:200]
    d["session"] = type(spark).__module__ + "." + type(spark).__name__
    d["session_id"] = id(spark)
    d["runtime"] = os.environ.get("DATABRICKS_RUNTIME_VERSION")
    d["active_is_spark"] = SparkSession.getActiveSession() is spark
    for key, get in (
        ("jsession_id", lambda: id(spark._jsparkSession)),
        ("jsession_uuid", lambda: spark._jsparkSession.sessionUUID()),
        ("jsession_hash", lambda: spark._jsparkSession.hashCode()),
        ("jvm_active_uuid", lambda: spark._jvm.org.apache.spark.sql.SparkSession.getActiveSession().get().sessionUUID()),
        ("jvm_active_is_jsession", lambda: spark._jvm.org.apache.spark.sql.SparkSession.getActiveSession().get()
         .equals(spark._jsparkSession)),
    ):
        try:
            d[key] = get()
        except Exception as exc:  # noqa: BLE001
            d[key] = f"error {type(exc).__name__}: {str(exc)[:160]}"
    try:
        from databricks.labs.gbx.manager import capabilities

        d["gbx_is_connect"] = bool(capabilities.is_connect(spark))
    except Exception as exc:  # noqa: BLE001
        d["gbx_is_connect"] = f"error {exc!r}"[:160]
    try:
        client = getattr(spark, "client", None)
    except Exception as exc:  # noqa: BLE001 - a classic session raises
        client = None
        d["client_error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    d["client"] = type(client).__module__ + "." + type(client).__name__
    local = getattr(client, "thread_local", None)
    d["has_thread_local"] = local is not None
    d["has_tl_exts_attr"] = hasattr(local, "user_context_extensions") if local is not None else False
    d["has_add_tl_ext"] = hasattr(client, "add_threadlocal_user_context_extension")
    d["has_remove_ext"] = hasattr(client, "remove_user_context_extension")
    d["has_global_exts_attr"] = hasattr(client, "global_user_context_extensions")
    d["thread_exts"] = _exts(getattr(local, "user_context_extensions", None))
    d["global_exts"] = _exts(getattr(client, "global_user_context_extensions", None))
    for n in NAMES:  # the catalog API is py4j-blocked in a pipeline; DESCRIBE FUNCTION is SQL
        try:
            spark.sql(f"DESCRIBE FUNCTION {n}").collect()
            d[f"visible:{n}"] = True
        except Exception as exc:  # noqa: BLE001
            d[f"visible:{n}"] = f"no: {type(exc).__name__}: {str(exc)[:100]}"
    try:
        from databricks.labs.gbx import _api

        done = getattr(_api, "_DONE", {})
        mine = done.get(spark) if hasattr(done, "get") else None
        if isinstance(mine, dict):  # keyed by flow
            d["api_cache"] = {repr(k)[:120]: len(v) for k, v in mine.items()}
        else:  # one set per session
            d["api_cache"] = None if mine is None else len(mine)
    except Exception as exc:  # noqa: BLE001
        d["api_cache"] = f"error {exc!r}"[:200]
    _write(spark, d)
    return d


def _write(spark, d: dict) -> None:
    try:
        root = spark.conf.get("probe.diag_dir", "")
        if not root:
            return
        out = os.path.join(root, spark.conf.get("probe.label", "run"))
        os.makedirs(out, exist_ok=True)
        with open(os.path.join(out, f"{d['flow']}-{d['step']}-{int(d['t'] * 1000)}-{uuid.uuid4().hex[:6]}.json"),
                  "w") as f:
            f.write(json.dumps(d, default=str))
    except Exception as exc:  # noqa: BLE001 - diagnostics never break the flow
        d["write_error"] = repr(exc)[:200]


def _internals(spark) -> dict:
    """The Lakeflow session's own attributes and its module's names: is there a per-flow identity to key on?"""
    import contextvars
    import sys

    out = {"type_mro": [f"{c.__module__}.{c.__name__}" for c in type(spark).__mro__]}
    try:
        out["instance_attrs"] = {k: type(v).__name__ for k, v in vars(spark).items()}
    except Exception as exc:  # noqa: BLE001
        out["instance_attrs"] = repr(exc)[:200]
    out["class_attrs"] = sorted(k for k in vars(type(spark)) if not k.startswith("__"))
    mods = {}
    for name, mod in list(sys.modules.items()):
        if not (name == "dlt" or name.startswith("dlt.") or name.startswith("pyspark.pipelines")):
            continue
        names = {}
        for k, v in list(vars(mod).items()):
            if k.startswith("__"):
                continue
            if isinstance(v, (threading.local, contextvars.ContextVar)) or "flow" in k.lower() or "context" in k.lower():
                try:
                    names[k] = f"{type(v).__name__}: {repr(v)[:160]}"
                except Exception:  # noqa: BLE001
                    names[k] = type(v).__name__
        mods[name] = names
    out["modules"] = mods
    try:
        out["contextvars"] = {k.name: repr(v)[:200] for k, v in contextvars.copy_context().items()}
    except Exception as exc:  # noqa: BLE001
        out["contextvars"] = repr(exc)[:200]
    for k in ("spark.databricks.pipelines.flowName", "pipelines.flowName", "spark.databricks.sql.pipelines.flowName"):
        try:
            out[f"conf:{k}"] = spark.conf.get(k, None)
        except Exception as exc:  # noqa: BLE001
            out[f"conf:{k}"] = repr(exc)[:120]
    return out


def _register(spark) -> str:
    from databricks.labs import gbx

    return repr(gbx.register(spark))[:200]


def _with_diag(df, diags):
    return df.withColumn("diag", F.lit(json.dumps(diags, default=str)))


@dp.materialized_view(name="fr_a_tiles", comment="A: registers, then a tiny MVT pyramid")
def fr_a_tiles():
    spark = _spark()
    diags = [_diag("a", "enter", spark)]
    summary = _register(spark)
    diags.append(_diag("a", "after_register", spark) | {"register": summary})
    pts = spark.sql(
        """
        SELECT h3_boundaryaswkb(h3_longlatash3(lon, lat, 6)) AS geom_wkb, named_struct('name', name) AS attrs
        FROM VALUES (-0.1276, 51.5072, 'london'), (-0.10, 51.52, 'islington'), (-0.15, 51.49, 'chelsea')
            AS t(lon, lat, name)
        """
    )
    pts.repartition(2, "geom_wkb").createOrReplaceTempView("_fr_pts")
    tiles = spark.sql(
        "SELECT 'pts' AS layer, t.z, t.x, t.y, t.mvt_bytes FROM _fr_pts, "
        "LATERAL gbx_st_asmvt_pyramid(geom_wkb, attrs, 6, 8, 'pts') AS t"
    )
    return _with_diag(tiles, diags)


@dp.materialized_view(name="fr_b_api_noreg", comment="B: the Python API pmtiles_agg, no register call")
def fr_b_api_noreg():
    spark = _spark()
    diags = [_diag("b", "enter", spark)]
    from databricks.labs.gbx.pmtiles.functions import pmtiles_agg

    tiles = spark.read.table("fr_a_tiles")
    t0 = time.time()
    col = pmtiles_agg("mvt_bytes", "z", "x", "y", META)
    t1 = time.time()
    pmtiles_agg("mvt_bytes", "z", "x", "y", META)  # a second call in the same run: a cache hit when the key is right
    diags.append(_diag("b", "after_api_call", spark, first_call_s=round(t1 - t0, 3),
                       second_call_s=round(time.time() - t1, 3)))
    out = tiles.groupBy(F.lit(1).alias("g")).agg(col.alias("archive"), F.count("*").alias("tiles"))
    return _with_diag(out.select("tiles", F.length("archive").alias("archive_bytes")), diags)


@dp.materialized_view(name="fr_c_sql_noreg", comment="C: gbx_pmtiles_agg by SQL name (F.expr), no register call")
def fr_c_sql_noreg():
    spark = _spark()
    diags = [_diag("c", "enter", spark)]
    tiles = spark.read.table("fr_a_tiles")
    out = tiles.groupBy(F.lit(1).alias("g")).agg(
        F.expr(f"gbx_pmtiles_agg(mvt_bytes, z, x, y, '{META}')").alias("archive"), F.count("*").alias("tiles")
    )
    return _with_diag(out.select("tiles", F.length("archive").alias("archive_bytes")), diags)


@dp.materialized_view(name="fr_d_api_reg", comment="D: register in the body, then the Python API pmtiles_agg")
def fr_d_api_reg():
    spark = _spark()
    diags = [_diag("d", "enter", spark)]
    summary = _register(spark)
    diags.append(_diag("d", "after_register", spark) | {"register": summary})
    from databricks.labs.gbx.pmtiles.functions import pmtiles_agg

    tiles = spark.read.table("fr_a_tiles")
    out = tiles.groupBy(F.lit(1).alias("g")).agg(
        pmtiles_agg("mvt_bytes", "z", "x", "y", META).alias("archive"), F.count("*").alias("tiles")
    )
    return _with_diag(out.select("tiles", F.length("archive").alias("archive_bytes")), diags)


@dp.materialized_view(name="fr_e_diag", comment="E: no register, no GeoBrix call; what this flow sees")
def fr_e_diag():
    spark = _spark()
    diags = [_diag("e", "enter", spark)]
    _write(spark, {"flow": "e", "step": "internals", "t": time.time(), **_internals(spark)})
    tiles = spark.read.table("fr_a_tiles")
    return _with_diag(tiles.groupBy(F.lit(1).alias("g")).agg(F.count("*").alias("tiles")), diags)


@dp.materialized_view(name="fr_f_api_one", comment="F: register the one name in the body, then pmtiles_agg")
def fr_f_api_one():
    spark = _spark()
    diags = [_diag("f", "enter", spark)]
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.pmtiles.functions import pmtiles_agg

    t0 = time.time()
    manager.register(spark, only=["gbx_pmtiles_agg"])
    diags.append(_diag("f", "after_register_one", spark, register_one_s=round(time.time() - t0, 3)))
    tiles = spark.read.table("fr_a_tiles")
    out = tiles.groupBy(F.lit(1).alias("g")).agg(
        pmtiles_agg("mvt_bytes", "z", "x", "y", META).alias("archive"), F.count("*").alias("tiles")
    )
    return _with_diag(out.select("tiles", F.length("archive").alias("archive_bytes")), diags)
