"""L3: register the light (Python or Rust) kernel for a manifest row."""

from __future__ import annotations

from typing import Optional, Tuple

from databricks.labs.gbx.manifest import resolve_ref

_TYPES = {
    "binary": "BinaryType",
    "string": "StringType",
    "double": "DoubleType",
    "boolean": "BooleanType",
}


def _return_type(name: str):
    """A manifest `returns` word -> the pyspark type (imported here, so the manifest stays pyspark-free)."""
    from pyspark.sql import types as T

    if (
        name == "bng_chips"
    ):  # array<struct<cellid, core, chip>>, the chips of gbx_bng_tessellate
        from databricks.labs.gbx.pygx._serde import BNG_CHIP_SCHEMA

        return T.ArrayType(BNG_CHIP_SCHEMA)
    return getattr(T, _TYPES[name])()


def l3_variant(fn) -> Tuple[str, Optional[str]]:
    """(variant, skip-reason) for the first usable variant in L3_ORDER."""
    from databricks.labs.gbx.manager.levels import L3_ORDER

    variants = fn.levels["L3"].get("variants")
    if not variants:
        return "-", None
    reason = None
    for v in L3_ORDER:
        if v not in variants:
            continue
        if v == "d":
            from databricks.labs.gbx import _rs

            why = _rs.unavailable_reason()
            if why:
                reason = f"geobrix: Rust kernels unavailable: {why}; using Python"
                continue
        return v, reason
    raise RuntimeError(f"{fn.sql_name}: no L3 variant")


def _factory_args(kind: str, spark=None):
    if kind == "proj_grid_dirs":
        from databricks.labs.gbx.core import proj_grids

        return (tuple(proj_grids.get_registered_dirs()),)
    if (
        kind == "read_cap"
    ):  # the driver's connect-aware read cap, which a worker cannot measure itself
        from databricks.labs.gbx.ds.file_gbx import report_detected_cap

        return (report_detected_cap(spark),)
    raise ValueError(kind)


def register_l3(spark, fn, variant: str, name: Optional[str] = None) -> None:
    from databricks.labs.gbx import _gdal_policy

    spec, name = fn.levels["L3"], name or fn.sql_name
    reg = _gdal_policy.carrying_session(
        spark
    )  # the driver's remote-path switch goes to the workers
    if "variants" in spec:
        reg.udf.register(name, resolve_ref(spec["variants"][variant]))
    elif "factory" in spec:
        reg.udf.register(
            name,
            resolve_ref(spec["factory"])(*_factory_args(spec["factory_args"], spark)),
        )
    elif "callable" in spec:
        reg.udf.register(
            name, resolve_ref(spec["callable"]), _return_type(spec["returns"])
        )
    elif "udtf" in spec:
        reg.udtf.register(name, resolve_ref(spec["udtf"]))
    else:
        raise ValueError(f"{fn.sql_name}: unknown L3 registration")


def register_l3_engine(
    spark, fn, form: str = "engine", name: Optional[str] = None
) -> str:
    """Register `fn`'s GDAL spec engine UDFs under `name` (default its SQL name), each carrying the driver's
    remote-path switch to the workers; returns the form used.

    ``engine-c1`` (and ``engine``, the default, where SQL functions work on a classic session): the batch UDF
    behind a wrapper that sends virtual, NULL and error tiles to the row-wise UDF. ``engine-c0`` (and the
    default anywhere else, Spark Connect included): the row-wise UDF alone.
    """
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.ds.file_gbx import report_detected_cap
    from databricks.labs.gbx.engine import l3 as engine_l3
    from databricks.labs.gbx.manager import capabilities, jvm, sqlwrap

    reg = _gdal_policy.carrying_session(spark)  # its UDFs carry the driver's switch
    grid = _factory_args("proj_grid_dirs")[0]
    cap = report_detected_cap(
        spark
    )  # the connect-aware read cap, which a worker cannot measure itself
    name = name or fn.sql_name
    wrapped = form == "engine-c1" or (
        form == "engine"
        and not capabilities.is_connect(spark)
        and capabilities._sql_functions(spark)
    )
    if not wrapped:
        reg.udf.register(name, engine_l3.c0(fn.sql_name, grid, read_cap=cap))
        return "engine-c0"
    c0_name, c1_name = jvm._priv(fn, "L3"), jvm._priv(fn, "L3c1")
    reg.udf.register(c0_name, engine_l3.c0(fn.sql_name, grid, read_cap=cap))
    reg.udf.register(c1_name, engine_l3.c1(fn.sql_name, grid))
    first = fn.params[0].name
    if fn.gdal.inputs[0][1] == "rasters":
        ddl = sqlwrap.tiles_dispatch(
            fn,
            sqlwrap.tile_from_result(
                c1_name + "(transform({tiles}, gbx_t -> gbx_t.raster), {rest})",
                "{tiles}[0]",
            ),
            c0_name,
        )
    else:
        ddl = sqlwrap.tile_dispatch(
            fn,
            sqlwrap.tile_from_result(c1_name + "({tile}.raster, {rest})", first),
            c0_name,
        )
    jvm._create_wrapper(spark, fn, ddl)
    return "engine-c1"
