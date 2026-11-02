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
    """A manifest `returns` value -> what `spark.udf.register` takes: the pyspark type of a word in `_TYPES` or
    `bng_chips` (imported here, so the manifest stays pyspark-free), else the value as it is, a DDL type string
    such as ``array<bigint>``."""
    from pyspark.sql import types as T

    if (
        name == "bng_chips"
    ):  # array<struct<cellid, core, chip>>, the chips of gbx_bng_tessellate
        from databricks.labs.gbx.pygx._serde import BNG_CHIP_SCHEMA

        return T.ArrayType(BNG_CHIP_SCHEMA)
    if name in _TYPES:
        return getattr(T, _TYPES[name])()
    return name


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
    if spec.get("file_ref") and name == fn.sql_name:
        from databricks.labs.gbx.manager import capabilities

        if capabilities._sql_functions(spark):
            # the light UDF that also takes a FILE reference (second), behind a wrapper that mints it from the
            # tile's path with the read rule of `file_ref_arg`
            from databricks.labs.gbx.ds.file_gbx import file_ref_sql
            from databricks.labs.gbx.manager import jvm, sqlwrap

            private = jvm._priv(fn, "L3f")
            reg.udf.register(private, resolve_ref(spec["file_ref"]))
            jvm._create_wrapper(
                spark,
                fn,
                sqlwrap.file_ref_wrapper(
                    fn, private, file_ref_sql(fn.params[0].name, spark)
                ),
            )
            return
    target = name
    if _needs_default_wrapper(spark, fn, spec, name):
        # a scalar UDF takes every argument it was written with, and a SQL call that leaves an optional one out
        # binds fewer: it registers under a private name, behind a wrapper that passes each default
        from databricks.labs.gbx.manager import jvm

        target = jvm._priv(fn, "L3d")
    if "variants" in spec:
        reg.udf.register(target, resolve_ref(spec["variants"][variant]))
    elif "factory" in spec:
        reg.udf.register(
            target,
            resolve_ref(spec["factory"])(*_factory_args(spec["factory_args"], spark)),
        )
    elif "callable" in spec:
        reg.udf.register(
            target, resolve_ref(spec["callable"]), _return_type(spec["returns"])
        )
    elif "udtf" in spec:
        reg.udtf.register(name, resolve_ref(spec["udtf"]))
    else:
        raise ValueError(f"{fn.sql_name}: unknown L3 registration")
    if target != name:
        from databricks.labs.gbx.manager import jvm, sqlwrap

        jvm._create_wrapper(spark, fn, sqlwrap.default_wrapper(fn, target))


def _needs_default_wrapper(spark, fn, spec, name: str) -> bool:
    """True for a scalar row with optional parameters, registered under its own SQL name where SQL functions
    exist. (A generator or an aggregate cannot sit behind a scalar SQL function; the light kernels of those rows
    take their own defaults.)"""
    if name != fn.sql_name or fn.kind != "scalar" or fn.max_args == fn.min_args:
        return False
    if not any(k in spec for k in ("variants", "factory", "callable")):
        return False
    from databricks.labs.gbx.manager import capabilities

    if not capabilities._sql_functions(spark):
        return False
    # The wrapper declares each parameter with its first session type, so a parameter that accepts several (a geometry
    # as BINARY or STRING, a resolution as INT or STRING) would cast the other forms: the kernel keeps its own defaults.
    if any(len(p.session) > 1 for p in fn.params):
        return False
    # A kernel that already defaults its own optional arguments needs no wrapper, and must not sit behind one:
    # Spark rejects a SQL function inside a generator, and `explode(gbx_bng_tessellate(g, 3))` is the way
    # to read the chips of an array-returning scalar.
    return not _takes_its_defaults(fn, spec)


def _takes_its_defaults(fn, spec) -> bool:
    """True when every registered form of the row gives each optional parameter a default in its own signature
    (a UDF object is read through the function it wraps). A form that cannot be read counts as one that does not.
    """
    import inspect

    from databricks.labs.gbx.manifest import resolve_ref

    refs = (
        list(spec["variants"].values())
        if "variants" in spec
        else [spec["callable"]] if "callable" in spec else []
    )
    if not refs:
        return (
            False  # a factory builds its function from the session; it is not read here
        )
    for ref in refs:
        obj = resolve_ref(ref)
        try:
            params = list(
                inspect.signature(getattr(obj, "func", obj)).parameters.values()
            )
        except (TypeError, ValueError):
            return False
        if any(p.kind is p.VAR_POSITIONAL for p in params):
            continue
        optional = params[fn.min_args : fn.max_args]
        if len(optional) < fn.max_args - fn.min_args or any(
            p.default is p.empty for p in optional
        ):
            return False
    return True


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
