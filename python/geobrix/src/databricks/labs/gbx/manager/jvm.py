"""The L1 installers (Catalyst companions over py4j; classic only).

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
    if levels.engine_only(fn, "L3"):
        # a row whose only light form is the GDAL spec engine (no numpy kernel): its row-wise UDF reads the virtual tile
        l3.register_engine_c0(spark, fn, name)
    else:
        l3.register_l3(spark, fn, l3.l3_variant(fn)[0], name=name)
    return name


def _create_wrapper(spark, fn, ddl: str) -> None:
    """Install the wrapper under the SQL name. CREATE OR REPLACE cannot replace a function that
    is not a SQL function (the light UDF an earlier registration left under the same name): the engine refuses it,
    and only then is the name dropped and created. The common case, a name nothing holds or a SQL function, costs one
    statement, not two (each is a round trip on Spark Connect).
    """
    try:
        spark.sql(ddl)
    except (
        Exception
    ):  # noqa: BLE001 - a non-SQL function holds the name (or the DDL is bad, and fails again below)
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
        _create_wrapper(spark, fn, dispatch_ddl(fn, _priv(fn, "L1"), light))
        return None
    except Exception as exc:  # noqa: BLE001 - any failure is a fallback reason
        return f"{fn.sql_name}: heavy install failed ({type(exc).__name__}: {str(exc)[:160]})"


def _is_tiles(fn) -> bool:
    return fn.params[0].session[0] == "ARRAY<TILE>"


def dispatch_ddl(fn, l1_name: str, l3_name: str) -> str:
    """The wrapper that goes under the SQL name of a tile function served by the JVM: a virtual tile (a path, no raster)
    goes to the light function ``l3_name``, which reads it, and a materialized tile to the JVM function ``l1_name``,
    which cannot (it raises ``VirtualTileException``). The array form (``ARRAY<TILE>``) sends an array with any virtual,
    NULL or error element to light. A row whose L1 ``dispatch`` is ``"bytes_to_jvm"`` sends every tile without bytes to
    light, NULL and error tiles included (``sqlwrap.bytes_dispatch``).
    """
    dispatch = fn.levels["L1"].get("dispatch")
    if dispatch == "bytes_to_jvm":
        return sqlwrap.bytes_dispatch(fn, l1_name + "({tile}{rest})", l3_name)
    if dispatch is not None:
        raise ValueError(f"{fn.sql_name}: unknown L1 dispatch {dispatch!r}")
    if _is_tiles(fn):
        return sqlwrap.tiles_dispatch(fn, l1_name + "({tiles}{rest})", l3_name)
    return sqlwrap.tile_dispatch(fn, l1_name + "({tile}{rest})", l3_name)


def install_companion(spark, fqn: str, name: str) -> Optional[str]:
    """Install the Catalyst companion `fqn` under the session name `name` (its own SQL name, or the private
    `gbx__l1_<x>`), with no dispatch wrapper in front of it. None when it is installed, else one line saying why
    not. Called only after `capabilities.detect` found a matching JAR. A Spark Connect session (Serverless, standard
    access mode) has no JVM bridge, so it is refused here, before any JVM access."""
    if capabilities.is_connect(spark):
        return (
            f"{name}: the heavy engine cannot be installed from a Spark Connect session (Serverless or standard "
            "access mode). Fix: run on dedicated compute, or use the Python level here."
        )
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
