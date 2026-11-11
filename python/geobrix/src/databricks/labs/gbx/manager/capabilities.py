"""Capability detection. The Spark Connect check runs BEFORE any JVM access."""

from __future__ import annotations

import importlib
import logging
import os
import re
import threading
import warnings
import weakref
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from databricks.labs.gbx import __version__

BUILDINFO_UDF = "com.databricks.labs.gbx.kernels.udf.BuildInfoUdf"
GDALPROBE_UDF = "com.databricks.labs.gbx.kernels.udf.GdalProbeUdf"
# Standard access mode runs a Java UDF in a sandbox that loads only Java classes of a JAR added to the session
# (see `attach_jar`), so the two probes have Java twins there. Registration does not call them today (`detect` returns
# before any JAR probe on a Connect session); they stay for a possible standard-mode JVM level.
BUILDINFO_CONNECT_UDF = "com.databricks.labs.gbx.kernels.udf.BuildInfoConnectUdf"
GDALPROBE_CONNECT_UDF = "com.databricks.labs.gbx.kernels.udf.GdalProbeConnectUdf"
JAR_PATH_ENV = "GBX_JAR_PATH"


@dataclass(frozen=True)
class JarIdentity:
    version: str
    sha256: str


@dataclass(frozen=True)
class Capabilities:
    connect: bool
    serverless: bool
    jar: Optional[JarIdentity]
    jar_reason: str  # "" when the JAR is usable
    gdal_reason: str  # "" when GDAL natives load in the JVM
    sql_functions: bool  # CREATE TEMPORARY FUNCTION ... RETURN works here
    jvm_gdal: str = (
        ""  # the GDAL release the JVM loaded ("" when not probed or not loadable)
    )


def is_connect(spark) -> bool:
    from databricks.labs.gbx import _gdal_policy

    return "connect" in type(_gdal_policy.real_session(spark)).__module__


def is_serverless() -> bool:
    return os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client.")


_JAR_ATTACHED: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


def attach_jar(spark) -> str:
    """Add the GeoBrix JAR to a Spark Connect session so its Java UDFs can be registered; "" when it is there,
    else one line saying why not. Once per session.

    A JAR installed as a cluster library is not seen by the standard-access Java UDF sandbox, whatever the class:
    only a JAR added to the session is. The path is `GBX_JAR_PATH`, else the JAR the env wheel carries.
    """
    done = _JAR_ATTACHED.get(spark)
    if done is not None:
        return done
    path = os.environ.get(JAR_PATH_ENV) or ""
    if not path:
        try:
            path = (
                importlib.import_module("databricks.labs.gbx._native").jar_path() or ""
            )
        except ModuleNotFoundError:
            path = ""
    if not path:
        why = (
            "on standard access mode the Java UDFs need the GeoBrix JAR added to the session: set "
            f"{JAR_PATH_ENV} to its path (a Volume path works) or install the env wheel that carries it"
        )
    else:
        try:
            spark.addArtifact(path)
            why = ""
        except (
            Exception
        ) as exc:  # noqa: BLE001 - a refused artifact is a fallback reason
            why = f"adding the JAR {os.path.basename(path)} to the session failed ({type(exc).__name__}: {str(exc)[:120]})"
    try:
        _JAR_ATTACHED[spark] = why
    except TypeError:  # a session object that cannot be weakly referenced
        pass
    return why


def register_java(spark, name: str, cls: str, returns: str) -> None:
    """`spark.udf.registerJavaFunction` on a Connect session, with the JAR added to the session first.
    Raises when the JAR cannot be added."""
    why = attach_jar(spark)
    if why:
        raise RuntimeError(why)
    spark.udf.registerJavaFunction(name, cls, returns)


def _runtime_guard() -> None:
    """The env-wheel guard, when this wheel ships one. It already runs at import; re-checking is cheap."""
    try:
        guard = importlib.import_module("databricks.labs.gbx._env_guard")
    except ModuleNotFoundError:
        return
    guard.check()


def _public(v: str) -> str:
    return v.split("+", 1)[0]


def _expected_sha() -> Optional[str]:
    """sha256 of the JAR bundled in this wheel, when the wheel bundles one."""
    import hashlib

    try:
        path = importlib.import_module("databricks.labs.gbx._native").jar_path()
    except ModuleNotFoundError:
        return None
    if not path:
        return None
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _identity(spark, connect: bool) -> Tuple[Optional[str], str]:
    """(raw 'version|sha', reason). Classic uses py4j; Connect uses the Java UDF probe."""
    try:
        if not connect:
            return (
                str(spark._jvm.com.databricks.labs.gbx.kernels.BuildInfo.identity()),
                "",
            )
        register_java(spark, "gbx__buildinfo", BUILDINFO_CONNECT_UDF, "string")
        return str(spark.sql("SELECT gbx__buildinfo() AS i").first()["i"]), ""
    except (
        Exception
    ):  # noqa: BLE001 - absent class, refused registration, py4j JavaPackage
        if not connect:
            try:
                spark._jvm.java.lang.Class.forName(
                    "com.databricks.labs.gbx.rasterx.gdal.GDALManager"
                )
                return (
                    None,
                    "the geobrix JAR on the classpath predates the 0.6.0 kernels API",
                )
            except Exception:  # noqa: BLE001
                pass
        return None, "no geobrix JAR on classpath"


def _gdal(spark, connect: bool) -> Tuple[str, str]:
    """(reason, release): "" and the JVM's GDAL release when the natives load, else why not and ""."""
    try:
        if not connect:
            r = str(spark._jvm.com.databricks.labs.gbx.kernels.KernelGdal.probe())
        else:
            register_java(spark, "gbx__gdalprobe", GDALPROBE_CONNECT_UDF, "string")
            r = str(spark.sql("SELECT gbx__gdalprobe() AS g").first()["g"])
    except Exception:  # noqa: BLE001
        return "GDAL natives not loadable", ""
    if r.startswith("ok:"):
        return "", r[len("ok:") :]
    return f"GDAL natives not loadable ({r[:120]})", ""


def _probe_sql_functions(spark) -> bool:
    """Two statements, each a round trip on Spark Connect: create a temporary SQL function and drop it again."""
    try:
        spark.sql(
            "CREATE OR REPLACE TEMPORARY FUNCTION gbx__probe_sqlfn() RETURNS INT RETURN 1"
        )
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS gbx__probe_sqlfn")
        return True
    except Exception:  # noqa: BLE001 - OSS Spark builds without SQL UDFs
        return False


# session -> bool: whether the session creates SQL functions. The answer is a property of the session, and the
# installers ask it once per function (a full registration asked it ~80 times, 2 round trips each on Spark Connect).
_SQLFN: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
_SQLFN_LOCK = (
    threading.Lock()
)  # installs that overlap (see `register`) must not probe at the same moment


def _sql_functions(spark) -> bool:
    """Whether ``spark`` creates temporary SQL functions, probed once per session (`forget` drops the answer). A
    session that cannot be weakly referenced is probed every time."""
    try:
        known = _SQLFN.get(spark)
        if known is None:
            with _SQLFN_LOCK:
                known = _SQLFN.get(spark)
                if known is None:
                    known = _SQLFN[spark] = _probe_sql_functions(spark)
        return known
    except TypeError:
        return _probe_sql_functions(spark)


# session -> {need_jvm: Capabilities}: the first-use registration of each name (`_api.ensure`) calls `detect`, and
# its probes (the JAR, GDAL in the JVM, SQL functions) answer the same for the whole session.
_CACHE: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


def detect(spark, need_jvm: bool) -> Capabilities:
    """What this session offers, probed once per session: a ``need_jvm=True`` result also answers
    ``need_jvm=False``. A session that cannot be weakly referenced is probed every time.
    """
    try:
        seen = _CACHE.setdefault(spark, {})
    except TypeError:
        return _detect_uncached(spark, need_jvm)
    for key in (True,) if need_jvm else (True, False):
        if key in seen:
            return seen[key]
    seen[need_jvm] = _detect_uncached(spark, need_jvm)
    return seen[need_jvm]


def forget(spark) -> None:
    """Drop the probes of ``spark``, so the next ``detect`` runs them again (a forced level after a JAR is staged)."""
    try:
        _CACHE.pop(spark, None)
        _SQLFN.pop(spark, None)
        real = _real(spark)
        _RESOLVED.pop(real, None)
        _SERVER.pop(real, None)
        _NAME_LOCKS.pop(real, None)
    except TypeError:
        pass


def _detect_uncached(spark, need_jvm: bool) -> Capabilities:
    _runtime_guard()
    connect = is_connect(spark)  # no JVM access before this line
    serverless = connect and is_serverless()
    if serverless:
        reason = "Spark Connect session (Serverless)"
        return Capabilities(True, True, None, reason, reason, False)
    if not need_jvm:
        return Capabilities(connect, False, None, "not probed", "not probed", False)
    if connect:
        # Standard access mode. Every JVM level is refused on a Connect session (`levels._jvm_unavailable`) before
        # the JAR's identity is read, so probing it would upload the whole JAR to the session for an answer nothing
        # uses. Only the SQL-function probe is kept: the SQL kernels (L0) are what a standard session serves.
        reason = "Spark Connect session (standard access mode)"
        return Capabilities(True, False, None, reason, reason, _sql_functions(spark))
    raw, reason = _identity(spark, connect)
    jar = None
    if raw is not None:
        version, _, sha = raw.partition("|")
        expected = _expected_sha()
        if _public(version) != _public(__version__):
            reason = f"JAR {version} on classpath != wheel {__version__}"
        elif expected is not None and sha != expected:
            reason = f"JAR sha256 {sha[:12]} != bundled {expected[:12]}"
        else:
            jar = JarIdentity(version, sha)
    if jar is not None and not connect:
        # Point the JVM at the wheel's natives BEFORE probing them: the probe is GDAL's first load in this JVM, and
        # without the key it looks only for GBX_NATIVE_ROOT and the system JNI (none on a stock runtime).
        from databricks.labs.gbx.manager import jvm

        jvm._point_jvm_at_payload(spark)
    gdal_reason, jvm_gdal = _gdal(spark, connect) if jar is not None else (reason, "")
    sqlf = _sql_functions(spark)
    return Capabilities(connect, False, jar, reason, gdal_reason, sqlf, jvm_gdal)


# ---------------------------------------------------------------------------
# Spark-feature capabilities: use a newer Spark feature where the SESSION proves it has it, else the older path.
#
# The server decides, not the client: on Spark Connect the client library (databricks-connect, pyspark) can be a
# Spark minor AHEAD of the server, and a client that can name an eval type the server lacks must not register it.
# Order of evidence for one capability, cheapest first:
#   1. an override: the env var ``GBX_CAP_<NAME>`` (or a legacy alias), else the Spark conf ``spark.geobrix.cap.<name>``
#      (read, never set): 1/true/yes/on forces the capability on WHERE IT IS AVAILABLE, 0/false/no/off forces it off;
#   2. the client API is present (the pyspark that built the call);
#   3. the SERVER's ``spark.version`` is at least the capability's minimum (one round trip per session on Connect);
#   4. only where a version cannot decide (a bug fix that is not versioned, a backport): a tiny functional probe,
#      run once per session under a time limit. Any error counts as "absent".
# Nothing is decided at import. A decision is made by the first use that has a session, kept for that session and
# logged once. An ON that is not available is refused (with a warning), never forced: it would fail in the worker.
# ---------------------------------------------------------------------------

_log = logging.getLogger(__name__)

CAP_ENV_PREFIX = "GBX_CAP_"
CAP_CONF_PREFIX = "spark.geobrix.cap."
PROBE_TIMEOUT_S = 60.0

Version = Tuple[int, int]


@dataclass(frozen=True)
class Verified:
    """One run that confirmed a capability on a runtime: the evidence behind trusting it there."""

    runtime: str  # the runtime key, e.g. "19.x-snapshot-scala2.13" or "serverless-env6"
    spark_version: str  # the server's ``spark.version`` in that run
    arch: str
    date: str
    result: str = "pass"


@dataclass(frozen=True)
class Capability:
    """A named Spark feature with its detectors and its default.

    ``client``: True when this pyspark offers the API (None: no client requirement). ``min_server``: the first Spark
    (major, minor) of the server that has it (None: no server requirement). ``conf_key``: a Spark conf the feature
    owns, read (never set) for the report, and the probe of a server older than ``min_server`` (a backport shows its
    conf). ``probe``: ``f(spark) -> (ok, detail)``, the functional test for what a version cannot say.
    ``default_when_present``: whether the feature is used when available and not overridden. ``aliases``: legacy env
    var names that mean the same as ``GBX_CAP_<NAME>``. ``verified_on``: the runs that confirmed it (empty until probed).
    """

    name: str
    why: str
    client: Optional[Callable[[], bool]] = None
    min_server: Optional[Version] = None
    conf_key: Optional[str] = None
    probe: Optional[Callable[[Any], Tuple[bool, str]]] = None
    default_when_present: bool = False
    aliases: Tuple[str, ...] = ()
    verified_on: Tuple[Verified, ...] = ()


@dataclass(frozen=True)
class Resolved:
    """What one session decided for one capability.

    ``value``: use it (available AND enabled). ``available``: the session has it (None: not known, a probe has not run).
    ``source``: who decided: ``override``, ``client`` / ``server`` / ``probe`` (the layer that said it is absent, or
    for a conf feature that found it), ``default`` (available; the capability's own default decided), ``unprobed``.
    ``verified``: the server's Spark minor is one a ``verified_on`` run covers.
    """

    name: str
    value: bool
    available: Optional[bool]
    source: str
    detail: str = ""
    verified: bool = False


def parse_version(text: Any) -> Optional[Version]:
    """(major, minor) of a Spark version string (``4.3.0``, ``4.3.0.dev0+databricks.connect.19.1``), None if unreadable."""
    m = re.match(r"\s*(\d+)\.(\d+)", str(text or ""))
    return (int(m.group(1)), int(m.group(2))) if m else None


# session -> (major, minor) | None: the SERVER's version, asked once (a round trip on Connect)
_SERVER: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
# session -> {name: Resolved}
_RESOLVED: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
_CAP_LOCK = threading.RLock()
_UNKNOWN = object()


def _real(spark):
    from databricks.labs.gbx import _gdal_policy

    return _gdal_policy.real_session(spark)


def server_version(spark) -> Optional[Version]:
    """The (major, minor) of the Spark SERVER of ``spark`` (on Connect ``spark.version`` is the server's, not the
    client's), None when it cannot be read. Cached per session."""
    real = _real(spark)
    try:
        known = _SERVER.get(real, _UNKNOWN)
    except TypeError:
        return _read_server_version(real)
    if known is _UNKNOWN:
        known = _SERVER[real] = _read_server_version(real)
    return known


def _read_server_version(real) -> Optional[Version]:
    try:
        return parse_version(real.version)
    except (
        Exception
    ):  # noqa: BLE001 - a session that cannot say is a session without the feature
        return None


def client_version() -> str:
    try:
        import pyspark

        return str(pyspark.__version__)
    except Exception:  # noqa: BLE001
        return ""


def _has_arrow_eval(name: str) -> bool:
    """This pyspark has ``arrow_udf`` and ``ArrowUDFType.<name>`` (Spark 4.1+ for GROUPED_AGG, 4.2+ for GROUPED_AGG_ITER)."""
    try:
        from pyspark.sql.functions import arrow_udf  # noqa: F401
        from pyspark.sql.pandas.functions import ArrowUDFType

        return hasattr(ArrowUDFType, name)
    except ImportError:
        return False


def _has_function(name: str) -> bool:
    try:
        from pyspark.sql import functions

        return hasattr(functions, name)
    except ImportError:
        return False


def _conf_value(real, key: str) -> Optional[str]:
    """The value of a Spark conf as the session reports it, None when unset or unreadable. Never sets anything, and
    never asks on Serverless (it allows no conf of ours: the environment variable is the only override there).
    """
    if is_serverless():
        return None
    try:
        v = real.conf.get(
            key
        )  # no default: a conf the session does not know raises, and reads as unset
    except Exception:  # noqa: BLE001 - an unknown or forbidden key
        return None
    return v if isinstance(v, str) else None


def _udtf_partition_probe(
    use_arrow: Optional[bool],
) -> Callable[[Any], Tuple[bool, str]]:
    """A probe of ``TABLE(...) PARTITION BY`` into a Python UDTF with two rows in one partition (the second row of a
    task is where the Serverless failure shows): passes when both rows come back. *use_arrow*: the UDTF's ``useArrow``
    (None: the session default)."""

    def probe(spark) -> Tuple[bool, str]:
        import uuid

        from pyspark.sql.functions import udtf

        tag = uuid.uuid4().hex[:8]
        name, view = f"gbx__probe_udtf_{tag}", f"gbx__probe_view_{tag}"

        class _Handler:
            def eval(self, row):
                yield (row["v"],)

        kw = {} if use_arrow is None else {"useArrow": use_arrow}
        fn = udtf(_Handler, returnType="v INT", **kw)
        try:
            spark.udtf.register(name, fn)
            spark.createDataFrame(
                [(0, 1), (0, 2)], "k INT, v INT"
            ).createOrReplaceTempView(view)
            n = spark.sql(
                f"SELECT count(*) AS n FROM {name}(TABLE(SELECT * FROM {view}) PARTITION BY k)"
            ).first()["n"]
            return n == 2, f"{n} of 2 rows"
        finally:
            for statement in (
                f"DROP VIEW IF EXISTS {view}",
                f"DROP TEMPORARY FUNCTION IF EXISTS {name}",  # the registered probe UDTF
            ):
                try:
                    spark.sql(statement)
                except Exception:  # noqa: BLE001
                    pass

    return probe


def _sql_file_type_probe(spark) -> Tuple[bool, str]:
    """Whether a SQL function whose body mints a FILE value (`try_to_file`) can be created AND listed. A runtime that
    parses the FILE type in an expression can still refuse it when it lists the session's functions (DBR 17.3:
    `SHOW USER FUNCTIONS` fails with UNSUPPORTED_DATATYPE "FILE" once a function with such a body exists), and one
    such function breaks the listing of all of them. The probe function is dropped again.

    A runtime whose listing fails for another reason (DBR 19 dedicated, Spark 4.2.0: every `SHOW FUNCTIONS` form fails
    with PARSE_SYNTAX_ERROR, FILE or not) says nothing about FILE: the check there is that `DESCRIBE FUNCTION` reads it.
    """
    import uuid

    name = f"gbx__probe_filetype_{uuid.uuid4().hex[:8]}"
    try:
        spark.sql(
            f"CREATE OR REPLACE TEMPORARY FUNCTION {name}(p STRING) RETURNS BOOLEAN "
            "RETURN try_to_file(p) IS NULL"
        )
        try:
            spark.sql(f"SHOW USER FUNCTIONS LIKE '{name}'").collect()
            return True, "created and listed"
        except (
            Exception
        ) as exc:  # noqa: BLE001 - only a FILE refusal means no FILE support
            if "UNSUPPORTED_DATATYPE" in str(exc):
                raise
        spark.sql(f"DESCRIBE FUNCTION {name}").collect()
        return True, "created and described (the runtime cannot list functions)"
    finally:
        try:
            spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")
        except Exception:  # noqa: BLE001
            pass


def _caps(*items: Capability) -> Dict[str, Capability]:
    return {c.name: c for c in items}


REGISTRY: Dict[str, Capability] = _caps(
    Capability(
        "arrow_scalar_udf",
        "scalar UDFs as Arrow arrays in and out (no pandas hop); the pandas UDF where absent",
        client=lambda: _has_arrow_eval("SCALAR"),
        min_server=(4, 1),
        default_when_present=True,  # what the scalar seams already do where the client has arrow_udf
    ),
    Capability(
        "arrow_grouped_agg",
        "grouped aggregates as Arrow arrays (ArrowUDFType.GROUPED_AGG); the pandas UDF where absent",
        client=lambda: _has_arrow_eval("GROUPED_AGG"),
        min_server=(4, 1),
        aliases=("GBX_ARROW_AGG",),
        default_when_present=False,  # measured 0-6% on 4.1.1: off until an env6 run shows a win
    ),
    Capability(
        "arrow_grouped_agg_iter",
        "grouped aggregates over an iterator of a group's Arrow batches (GROUPED_AGG_ITER, SPARK-53615): "
        "the aggregate holds one batch at a time, not the whole group",
        client=lambda: _has_arrow_eval("GROUPED_AGG_ITER"),
        min_server=(4, 2),
        default_when_present=False,  # decided by the peak-RSS probe (P2)
    ),
    Capability(
        "arrow_udtf_native",
        "UDTFs as Arrow batches (arrow_udtf); the row UDTF where absent",
        client=lambda: _has_function("arrow_udtf"),
        min_server=(4, 1),
        default_when_present=False,
    ),
    Capability(
        "udtf_table_partition_ok",
        "a Python UDTF behind TABLE(...) PARTITION BY returns every row of a partition on the session's UDTF default",
        client=lambda: _has_function("udtf"),
        probe=_udtf_partition_probe(None),
        default_when_present=False,  # nothing consumes it yet: the Serverless refusal still stands
    ),
    Capability(
        "udtf_table_partition_noarrow_ok",
        "the same with useArrow=False (the row UDTF): the way out when only the non-Arrow form passes",
        client=lambda: _has_function("udtf"),
        probe=_udtf_partition_probe(False),
        default_when_present=False,
    ),
    Capability(
        "sql_file_type",
        "a SQL function body can carry the FILE type and the session still lists its functions; the SQL tile "
        "functions pass NULL for the FILE argument (a path read) where it cannot",
        probe=_sql_file_type_probe,
        default_when_present=True,  # as before: FILE wrappers wherever FILE works
    ),
    Capability(
        "arrow_max_bytes_per_batch",
        "the Arrow batch byte cap conf exists (report only: pyrx never sets conf)",
        min_server=(4, 0),
        conf_key="spark.sql.execution.arrow.maxBytesPerBatch",
        default_when_present=True,  # report only: "on" reads as "the session has it"
    ),
    Capability(
        "python_udf_max_bytes_per_batch",
        "pickled Python UDF input batches bounded by bytes (SPARK-57593, Spark 4.3; report only)",
        min_server=(4, 3),
        conf_key="spark.sql.execution.python.udf.maxBytesPerBatch",
        default_when_present=True,  # report only: "on" reads as "the session has it"
    ),
)


def _truthy(raw: str) -> Optional[bool]:
    v = raw.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return None


def _override(real, cap: Capability) -> Tuple[Optional[bool], str]:
    """(True / False, where it was set) when the capability is overridden, else (None, "")."""
    for var in (CAP_ENV_PREFIX + cap.name.upper(),) + cap.aliases:
        raw = os.environ.get(var)
        want = _truthy(raw) if raw is not None else None
        if want is not None:
            return want, var
    key = CAP_CONF_PREFIX + cap.name
    raw = _conf_value(real, key)
    want = _truthy(raw) if raw is not None else None
    return (want, key) if want is not None else (None, "")


def _run_probe(real, cap: Capability) -> Tuple[bool, str]:
    """The capability's probe under a time limit; any error or a timeout is "absent".

    A Python thread cannot be killed. The probe runs on a daemon thread, and on a Spark Connect session its work is
    tagged, so a timeout interrupts it (``interruptTag``); a probe cleans up what it registered in its own ``finally``,
    which runs when its Spark call returns or is interrupted."""
    import uuid

    out: Dict[str, Tuple[bool, str]] = {}
    tag = f"gbx-cap-probe-{cap.name}-{uuid.uuid4().hex[:8]}"
    tagged = hasattr(real, "addTag") and hasattr(real, "interruptTag")

    def work() -> None:
        try:
            if tagged:
                real.addTag(
                    tag
                )  # a thread-local tag: only this probe's operations carry it
            out["r"] = cap.probe(real)
        except Exception as exc:  # noqa: BLE001
            out["r"] = (False, f"{type(exc).__name__}: {str(exc)[:160]}")
        finally:
            if tagged:
                try:
                    real.removeTag(tag)
                except Exception:  # noqa: BLE001
                    pass

    t = threading.Thread(target=work, name=f"gbx-cap-probe-{cap.name}", daemon=True)
    t.start()
    t.join(PROBE_TIMEOUT_S)
    if t.is_alive() and tagged:
        try:
            real.interruptTag(tag)
        except Exception:  # noqa: BLE001
            pass
    return out.get("r", (False, f"probe did not finish in {PROBE_TIMEOUT_S:.0f}s"))


def _availability(
    real, cap: Capability, allow_probe: bool
) -> Tuple[Optional[bool], str, str]:
    """(available, the layer that decided, detail). available None: a probe is needed and not allowed here."""
    if cap.client is not None:
        try:
            ok = bool(cap.client())
        except Exception:  # noqa: BLE001
            ok = False
        if not ok:
            return False, "client", f"this pyspark ({client_version()}) lacks it"
    layer = "client"
    if cap.min_server is not None:
        layer = "server"
        sv = server_version(real)
        if sv is None:
            return False, "server", "the server's Spark version is unknown"
        if sv < cap.min_server:
            if cap.conf_key and _conf_value(real, cap.conf_key) is not None:
                layer = "probe"  # a backport: the feature's own conf is there
            else:
                return (
                    False,
                    "server",
                    f"server Spark {sv[0]}.{sv[1]} < {cap.min_server[0]}.{cap.min_server[1]}",
                )
    if cap.probe is not None:
        if not allow_probe:
            return None, "probe", "not probed"
        ok, detail = _run_probe(real, cap)
        return ok, "probe", detail
    detail = ""
    if cap.conf_key:
        detail = f"{cap.conf_key}={_conf_value(real, cap.conf_key)}"
    return True, layer, detail


def _verified(real, cap: Capability) -> bool:
    sv = server_version(real)
    return sv is not None and any(
        parse_version(v.spark_version) == sv for v in cap.verified_on
    )


def _decide(real, cap: Capability, probe: bool) -> Resolved:
    want, via = _override(real, cap)
    verified = _verified(real, cap)
    if want is False:
        avail, _, detail = _availability(real, cap, allow_probe=False)
        return Resolved(
            cap.name,
            False,
            avail,
            "override",
            f"{via} turns it off; {detail}".rstrip("; "),
            verified,
        )
    avail, layer, detail = _availability(real, cap, allow_probe=probe)
    if avail is None:
        return Resolved(cap.name, False, None, "unprobed", detail, verified)
    if not avail:
        if want is True:
            warnings.warn(
                f"{via} turns on '{cap.name}', but this session does not have it ({detail}): the older path is "
                f"used. Fix: unset {via} on this runtime.",
                RuntimeWarning,
                stacklevel=4,
            )
        return Resolved(cap.name, False, False, layer, detail, verified)
    if want is True:
        return Resolved(
            cap.name,
            True,
            True,
            "override",
            f"{via} turns it on; {detail}".rstrip("; "),
            verified,
        )
    return Resolved(
        cap.name,
        cap.default_when_present,
        True,
        "probe" if layer == "probe" else "default",
        detail,
        verified,
    )


# session -> {name: Lock}: the single-flight guard of one decision
_NAME_LOCKS: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


def _name_lock(real, name: str) -> "threading.Lock":
    with _CAP_LOCK:  # held only to find or make the lock, never while a decision runs
        return _NAME_LOCKS.setdefault(real, {}).setdefault(name, threading.Lock())


def resolve(spark, name: str, *, probe: bool = True) -> Resolved:
    """The decision of ``spark`` for capability *name*, made once per session and kept (`forget` drops it).
    *probe*: run the capability's functional probe if it has one and a version cannot decide (False: report it as
    ``unprobed`` and keep nothing)."""
    try:
        cap = REGISTRY[name]
    except KeyError:
        raise ValueError(
            f"unknown capability {name!r}; known: {', '.join(sorted(REGISTRY))}"
        ) from None
    real = _real(spark)
    try:
        seen = _RESOLVED.setdefault(real, {})
    except TypeError:  # a session that cannot be weakly referenced is judged every time
        return _decide(real, cap, probe)
    got = seen.get(name)
    if got is not None:
        return got
    # One decision per (session, name) at a time: a second caller of the same name waits for the first's answer (a
    # probe runs once); a lookup of another name, or of one already decided, is never held up by a slow probe.
    with _name_lock(real, name):
        got = seen.get(name)
        if got is None:
            got = _decide(real, cap, probe)
            if got.source != "unprobed":
                seen[name] = got
                _log.info(
                    "capability %s=%s (%s%s)",
                    name,
                    got.value,
                    got.source,
                    f": {got.detail}" if got.detail else "",
                )
    return got


def has(spark, name: str) -> bool:
    """Whether to use the newer path of capability *name* on ``spark``: available there AND enabled."""
    return resolve(spark, name).value


def available(spark, name: str) -> bool:
    """Whether ``spark`` has capability *name* at all, enabled or not."""
    return resolve(spark, name).available is True


def report(spark, *, probe: bool = False) -> Dict[str, Dict[str, Any]]:
    """``{name: {value, available, source, detail, verified}}`` for every capability, for logs and bench rows. A probe
    runs only with ``probe=True`` (a probe starts Spark work); otherwise a capability that needs one is ``unprobed``
    unless a use already decided it."""
    out = {}
    for name in REGISTRY:
        r = resolve(spark, name, probe=probe)
        out[name] = {
            "value": r.value,
            "available": r.available,
            "source": r.source,
            "detail": r.detail,
            "verified": r.verified,
        }
    return out


def _merge_agg_path(spark) -> str:
    if has(spark, "arrow_grouped_agg_iter"):
        return "arrow_iter"
    return "arrow" if has(spark, "arrow_grouped_agg") else "pandas"


# op -> which path a session takes for it (the label a log line or a bench row carries)
_PATHS: Dict[str, Callable[[Any], str]] = {"merge_agg": _merge_agg_path}


def path(spark, op: str) -> str:
    """The code path *op* takes on ``spark``, as ``op:label`` (``merge_agg:arrow_iter``): what a capability chose."""
    try:
        chooser = _PATHS[op]
    except KeyError:
        raise ValueError(
            f"no capability-chosen path for {op!r}; known: {', '.join(sorted(_PATHS))}"
        ) from None
    return f"{op}:{chooser(spark)}"


def runtime_info(spark=None) -> Dict[str, str]:
    """Where this run is: the server's Spark version (on Connect, the server's), the client pyspark, the Databricks
    runtime and the Serverless environment version. "" for what cannot be read. Read-only, Serverless-safe.
    """
    info = {
        "spark_version": "",
        "pyspark_version": client_version(),
        "dbr_version": os.environ.get("DATABRICKS_RUNTIME_VERSION", "").strip(),
        "env_version": os.environ.get("DATABRICKS_ENV_VERSION", "").strip(),
    }
    if spark is None:
        return info
    real = _real(spark)
    try:
        info["spark_version"] = str(real.version)
    except Exception:  # noqa: BLE001
        pass
    if not is_serverless():
        # a classic cluster names its runtime ("19.x-snapshot-scala2.13"); the environment variable only its number
        tag = _conf_value(real, "spark.databricks.clusterUsageTags.sparkVersion")
        if tag:
            info["dbr_version"] = tag
    return info
