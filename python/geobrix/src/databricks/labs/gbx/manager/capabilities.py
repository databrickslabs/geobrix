"""Capability detection. The Spark Connect check runs BEFORE any JVM access."""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Optional, Tuple

from databricks.labs.gbx import __version__

BUILDINFO_UDF = "com.databricks.labs.gbx.kernels.udf.BuildInfoUdf"
GDALPROBE_UDF = "com.databricks.labs.gbx.kernels.udf.GdalProbeUdf"


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


def is_connect(spark) -> bool:
    return "connect" in type(spark).__module__


def is_serverless() -> bool:
    return os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client.")


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
        spark.udf.registerJavaFunction("gbx__buildinfo", BUILDINFO_UDF, "string")
        return str(spark.sql("SELECT gbx__buildinfo() AS i").first()["i"]), ""
    except (
        Exception
    ) as exc:  # noqa: BLE001 - absent class, refused registration, py4j JavaPackage
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
        return None, f"no geobrix JAR on classpath ({type(exc).__name__})"


def _gdal(spark, connect: bool) -> str:
    try:
        if not connect:
            r = str(spark._jvm.com.databricks.labs.gbx.kernels.KernelGdal.probe())
        else:
            spark.udf.registerJavaFunction("gbx__gdalprobe", GDALPROBE_UDF, "string")
            r = str(spark.sql("SELECT gbx__gdalprobe() AS g").first()["g"])
    except Exception as exc:  # noqa: BLE001
        return f"GDAL natives not loadable ({type(exc).__name__})"
    return "" if r.startswith("ok:") else f"GDAL natives not loadable ({r[:120]})"


def _sql_functions(spark) -> bool:
    try:
        spark.sql(
            "CREATE OR REPLACE TEMPORARY FUNCTION gbx__probe_sqlfn() RETURNS INT RETURN 1"
        )
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS gbx__probe_sqlfn")
        return True
    except Exception:  # noqa: BLE001 - OSS Spark builds without SQL UDFs
        return False


def detect(spark, need_jvm: bool) -> Capabilities:
    _runtime_guard()
    connect = is_connect(spark)  # no JVM access before this line
    serverless = connect and is_serverless()
    if serverless:
        reason = "Spark Connect session (Serverless)"
        return Capabilities(True, True, None, reason, reason, False)
    if not need_jvm:
        return Capabilities(connect, False, None, "not probed", "not probed", False)
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
    gdal_reason = _gdal(spark, connect) if jar is not None else reason
    sqlf = _sql_functions(spark)
    return Capabilities(connect, False, jar, reason, gdal_reason, sqlf)
