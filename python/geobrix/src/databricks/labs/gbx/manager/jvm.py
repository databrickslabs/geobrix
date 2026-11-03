"""L1 (Catalyst companions over py4j; classic only) and L2 (registerJavaFunction) installers.

Each returns None on success, else a one-line reason, and never raises, so the registrar can
fall back. Called only after `capabilities.detect` found a matching JAR.
"""

from __future__ import annotations

import importlib
from typing import Optional

from databricks.labs.gbx.manager import capabilities, l3, levels, sqlwrap


def _priv(fn, level: str) -> str:
    return f"gbx__{level.lower()}_{fn.sql_name[len('gbx_'):]}"


NATIVE_DIR_KEY = (
    "spark.databricks.labs.gbx.native.dir"  # the JVM's GDALManager reads it
)
# The decoded size, in bytes, a materialized tile may have where the JVM reads its bytes (RasterDriver's one
# tile-from-bytes point, through ExpressionConfig): over it the tile is an error tile with a Fix. Imported from the
# rasterio-free budget module: the heavy tier registers through here and must not load the light readers (ds).
from databricks.labs.gbx.pyrx.core.budget import READ_CAP_KEY  # noqa: E402


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


def _pass_read_cap(spark) -> None:
    """On dedicated (classic only) give the JVM the light tier's decoded budget as its read cap, unless the session
    already names one: one number decides when a tile is too large to read, whichever level reads it.
    """
    if spark.conf.get(READ_CAP_KEY, None) is None:
        from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes

        spark.conf.set(READ_CAP_KEY, str(decoded_budget_bytes("classic")))


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
        _pass_read_cap(spark)
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
        if _is_tiles(fn):
            ddl = sqlwrap.tiles_dispatch(fn, _priv(fn, "L1") + "({tiles}{rest})", light)
        else:
            ddl = sqlwrap.tile_dispatch(fn, _priv(fn, "L1") + "({tile}{rest})", light)
        _create_wrapper(spark, fn, ddl)
        return None
    except Exception as exc:  # noqa: BLE001 - any failure is a fallback reason
        return f"{fn.sql_name}: heavy install failed ({type(exc).__name__}: {str(exc)[:160]})"


def _is_tiles(fn) -> bool:
    return fn.params[0].session[0] == "ARRAY<TILE>"


def install_l1_engine(spark, fn, caps) -> Optional[str]:
    """Install `fn`'s GDAL spec engine expression (``GdalSpecExpr``, read from the JAR's copy of the manifest).
    A row that takes virtual tiles gets it under its private name, the engine's row-wise light UDF beside it, and
    the dispatch wrapper under the SQL name."""
    try:
        _point_jvm_at_payload(spark)
        _pass_read_cap(spark)
        inst = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
        target = _priv(fn, "L1") if fn.accepts_virtual_tile else fn.sql_name
        r = str(inst.installSpec(spark._jsparkSession, target, fn.sql_name))
        if r != "ok" or not fn.accepts_virtual_tile:
            return None if r == "ok" else r
        light = _priv(fn, "L3")
        l3.register_l3_engine(spark, fn, "engine-c0", name=light)
        if _is_tiles(fn):
            ddl = sqlwrap.tiles_dispatch(fn, target + "({tiles}{rest})", light)
        else:
            ddl = sqlwrap.tile_dispatch(fn, target + "({tile}{rest})", light)
        _create_wrapper(spark, fn, ddl)
        return None
    except Exception as exc:  # noqa: BLE001 - any failure is a fallback reason
        return f"{fn.sql_name}: heavy engine install failed ({type(exc).__name__}: {str(exc)[:160]})"


def _register_java(spark, name: str, spec) -> None:
    """Register `spec`'s Java UDF as `name`: its Scala class on a classic session, its Java `connect_class` (with
    the JAR added to the session first) on Spark Connect."""
    if capabilities.is_connect(spark):
        capabilities.register_java(spark, name, spec["connect_class"], spec["returns"])
    else:
        spark.udf.registerJavaFunction(name, spec["class"], spec["returns"])


def install_l2(spark, fn, caps) -> Optional[str]:
    try:
        spec = fn.levels["L2"]
        if (
            caps.connect and "connect_class" not in spec
        ):  # a forced L2 skips `levels.available`
            return levels.no_connect_handler(fn)
        if ("bytes_param" in spec or "arg_types" in spec) and not caps.sql_functions:
            # A forced L2 skips `levels.available`. Refuse before anything is registered: the wrapper step
            # drops whatever holds the SQL name (an L3 registration) and would then fail its CREATE.
            return levels.no_sql_functions(fn)
        if "bytes_param" in spec:
            k = _priv(fn, "L2")
            _register_java(spark, k, spec)
            light = _install_light_side(spark, fn)
            tile = fn.params[0].name
            jvm = sqlwrap.tile_from_raster(k + "({tile}.raster{rest})", tile)
            _create_wrapper(spark, fn, sqlwrap.tile_dispatch(fn, jvm, light))
            return None
        if fn.min_args != fn.max_args:
            return f"{fn.sql_name}: optional arguments at L2 need a SQL wrapper this row does not declare"
        if "arg_types" in spec:
            # The Java UDF under its private name, and a SQL wrapper under the SQL name that casts each
            # argument to the type the Java class declares (an INT resolution to STRING).
            k = _priv(fn, "L2")
            _register_java(spark, k, spec)
            _create_wrapper(spark, fn, sqlwrap.cast_call(fn, k, spec["arg_types"]))
            return None
        _register_java(spark, fn.sql_name, spec)
        return None
    except Exception as exc:  # noqa: BLE001
        return f"{fn.sql_name}: Java UDF registration failed ({type(exc).__name__}: {str(exc)[:160]})"


def install_companion(spark, fqn: str, name: str) -> Optional[str]:
    """Install the Catalyst companion `fqn` under the session name `name` (its own SQL name, or the private
    `gbx__l1_<x>`), with no dispatch wrapper in front of it. None when it is installed, else one line saying why
    not. Called only after `capabilities.detect` found a matching JAR."""
    try:
        _point_jvm_at_payload(spark)
        _pass_read_cap(spark)
        r = str(
            spark._jvm.com.databricks.labs.gbx.kernels.L1Installer.install(
                spark._jsparkSession, name, fqn
            )
        )
        return None if r == "ok" else f"{name}: {r}"
    except Exception as exc:  # noqa: BLE001 - any failure is a fallback reason
        return f"{name}: heavy install failed ({type(exc).__name__}: {str(exc)[:160]})"


def install_private_l1(spark, fqn: str, name: str) -> Optional[str]:
    """`install_companion` under a private session name (`gbx__l1_<x>`): a helper that calls the heavy step itself
    uses it as it is, beside whatever the registrar put under the SQL name."""
    return install_companion(spark, fqn, name)
