"""L1 (Catalyst companions over py4j; classic only) and L2 (registerJavaFunction) installers.

Each returns None on success, else a one-line reason, and never raises, so the registrar can
fall back. Called only after `capabilities.detect` found a matching JAR.
"""

from __future__ import annotations

import importlib
from typing import Optional

from databricks.labs.gbx.manager import l3, sqlwrap


def _priv(fn, level: str) -> str:
    return f"gbx__{level.lower()}_{fn.sql_name[len('gbx_'):]}"


NATIVE_DIR_KEY = (
    "spark.databricks.labs.gbx.native.dir"  # the JVM's GDALManager reads it
)


def _point_jvm_at_payload(spark) -> None:
    """On dedicated (classic only) tell the JVM to load the wheel's natives, when the wheel ships them."""
    try:
        native = importlib.import_module("databricks.labs.gbx._native")
    except ModuleNotFoundError:
        return  # this wheel bundles no natives
    d = native.native_dir()
    if d and spark.conf.get(NATIVE_DIR_KEY, None) != d:
        spark.conf.set(
            NATIVE_DIR_KEY, d
        )  # classic dedicated only: never reached on Connect


def _install_light_side(spark, fn) -> str:
    """Register the light kernel under its private name for the dispatch wrapper; return that name."""
    name = _priv(fn, "L3")
    l3.register_l3(spark, fn, l3.l3_variant(fn)[0], name=name)
    return name


def _create_wrapper(spark, fn, ddl: str) -> None:
    """Install the wrapper under the SQL name. CREATE OR REPLACE cannot replace a function that
    is not a SQL function (the light UDF an earlier registration left under the same name).
    """
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {fn.sql_name}")
    spark.sql(ddl)


def install_l1(spark, fn, caps) -> Optional[str]:
    try:
        _point_jvm_at_payload(spark)
        fqn = fn.levels["L1"]["companion"]
        inst = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
        want = ",".join(str(n) for n in range(fn.min_args, fn.max_args + 1))
        got = str(inst.acceptedArities(fqn, fn.min_args, fn.max_args))
        if got != want:
            return f"{fn.sql_name}: heavy accepts {got or 'no'} argument counts, the signature needs {want}"
        if fn.kind == "generator":
            # A table function (FROM t, LATERAL f(...)), the shape the light UDTFs have. There is no
            # dispatch wrapper: a generator cannot be wrapped in a scalar SQL function.
            r = str(inst.installGenerator(spark._jsparkSession, fn.sql_name, fqn))
            return None if r == "ok" else r
        if not fn.accepts_virtual_tile:
            r = str(inst.install(spark._jsparkSession, fn.sql_name, fqn))
            return None if r == "ok" else r
        r = str(inst.install(spark._jsparkSession, _priv(fn, "L1"), fqn))
        if r != "ok":
            return r
        light = _install_light_side(spark, fn)
        _create_wrapper(
            spark,
            fn,
            sqlwrap.tile_dispatch(fn, _priv(fn, "L1") + "({tile}, {rest})", light),
        )
        return None
    except Exception as exc:  # noqa: BLE001 - any failure is a fallback reason
        return f"{fn.sql_name}: heavy install failed ({type(exc).__name__}: {str(exc)[:160]})"


def install_l2(spark, fn, caps) -> Optional[str]:
    try:
        spec = fn.levels["L2"]
        if "bytes_param" in spec:
            k = _priv(fn, "L2")
            spark.udf.registerJavaFunction(k, spec["class"], spec["returns"])
            light = _install_light_side(spark, fn)
            tile = fn.params[0].name
            jvm = sqlwrap.tile_from_raster(k + "({tile}.raster, {rest})", tile)
            _create_wrapper(spark, fn, sqlwrap.tile_dispatch(fn, jvm, light))
            return None
        if fn.min_args != fn.max_args:
            return f"{fn.sql_name}: optional arguments at L2 need a SQL wrapper this row does not declare"
        spark.udf.registerJavaFunction(fn.sql_name, spec["class"], spec["returns"])
        return None
    except Exception as exc:  # noqa: BLE001
        return f"{fn.sql_name}: Java UDF registration failed ({type(exc).__name__}: {str(exc)[:160]})"
