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
    if spec.get("file_ref") not in (None, "engine") and name == fn.sql_name:
        from databricks.labs.gbx.manager import capabilities

        if capabilities._sql_functions(spark):
            # the light UDF that also takes a FILE reference (second), behind a wrapper that mints it from the
            # tile's path with the read rule of `file_ref_arg`
            from databricks.labs.gbx.ds.file_gbx import file_ref_sql
            from databricks.labs.gbx.manager import jvm, sqlwrap

            private = jvm._priv(fn, "L3f")
            file_udf = resolve_ref(spec["file_ref"])
            if (
                "factory" in spec
            ):  # a row built by a factory builds its FILE form the same way
                file_udf = file_udf(*_factory_args(spec["factory_args"], spark))
            reg.udf.register(private, file_udf)
            jvm._create_wrapper(
                spark,
                fn,
                sqlwrap.file_ref_wrapper(
                    fn, private, file_ref_sql(fn.params[0].name, spark)
                ),
            )
            return
    target = name
    # a factory builds its function from the session, once: the default check reads it, the registration reuses it
    built = (
        resolve_ref(spec["factory"])(*_factory_args(spec["factory_args"], spark))
        if "factory" in spec
        else None
    )
    if _needs_default_wrapper(spark, fn, spec, name, built):
        # a scalar UDF takes every argument it was written with, and a SQL call that leaves an optional one out
        # binds fewer: it registers under a private name, behind a wrapper that passes each default
        from databricks.labs.gbx.manager import jvm

        target = jvm._priv(fn, "L3d")
    if "variants" in spec:
        udf = resolve_ref(spec["variants"][variant])
        for_session = getattr(udf, "for_session", None)
        if (
            for_session is not None
        ):  # a UDF whose form the session chooses (a capability): the form it runs
            udf = for_session(spark)
        reg.udf.register(target, udf)
    elif "factory" in spec:
        reg.udf.register(target, built)
    elif "callable" in spec:
        reg.udf.register(
            target, resolve_ref(spec["callable"]), _return_type(spec["returns"])
        )
    elif "udtf" in spec:
        table_fn = resolve_ref(spec["udtf"])
        if spec.get("arrow"):
            # Arrow batches between the JVM and Python: the same class and schema, registered as an Arrow UDTF
            from pyspark.sql.functions import udtf

            table_fn = udtf(
                table_fn.func, returnType=table_fn.returnType, useArrow=True
            )
        reg.udtf.register(name, table_fn)
    else:
        raise ValueError(
            f"{fn.sql_name}: its light level is the GDAL spec engine alone, which this process cannot run "
            "(register it at L3 through the engine, or install the geobrix env wheel)"
        )
    if target != name:
        from databricks.labs.gbx.manager import jvm, sqlwrap

        jvm._create_wrapper(spark, fn, sqlwrap.default_wrapper(fn, target))


def _needs_default_wrapper(spark, fn, spec, name: str, built=None) -> bool:
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
    return not _takes_its_defaults(fn, spec, built)


def _takes_its_defaults(fn, spec, built=None) -> bool:
    """True when every registered form of the row gives each optional parameter a default in its own signature
    (a UDF object is read through the function it wraps). A form that cannot be read counts as one that does not.
    A factory row is read through `built`, the function its factory made.
    """
    import inspect

    from databricks.labs.gbx.manifest import resolve_ref

    if "variants" in spec:
        objs = [resolve_ref(r) for r in spec["variants"].values()]
    elif "callable" in spec:
        objs = [resolve_ref(spec["callable"])]
    else:
        objs = [built] if built is not None else []
    if not objs:
        return False
    for obj in objs:
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


def register_engine_c0(spark, fn, name: str, file_ref: bool = False) -> None:
    """Register `fn`'s row-wise GDAL spec engine UDF under `name`, carrying the driver's remote-path switch and the
    connect-aware read cap. It reads virtual tiles and bytes alike. With `file_ref` it takes the tile's FILE
    reference second. Registering it never imports ``osgeo``: where the engine is missing a call refuses, naming the
    env wheel."""
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.ds.file_gbx import report_detected_cap
    from databricks.labs.gbx.engine import l3 as engine_l3

    reg = _gdal_policy.carrying_session(spark)
    grid = _factory_args("proj_grid_dirs")[0]
    reg.udf.register(
        name,
        engine_l3.c0(
            fn.sql_name, grid, read_cap=report_detected_cap(spark), file_ref=file_ref
        ),
    )


def _file_sql(spark, tile: str) -> str:
    from databricks.labs.gbx.ds.file_gbx import file_ref_sql

    return file_ref_sql(tile, spark)


def _engine_file_ref(spark, fn) -> bool:
    """True when `fn`'s light level reads a virtual tile through its FILE reference (`file_ref: "engine"`) and this
    session has SQL functions to carry the wrapper."""
    from databricks.labs.gbx.manager import capabilities, levels

    return fn.levels["L3"].get("file_ref") == levels.ENGINE_FILE_REF and bool(
        capabilities._sql_functions(spark)
    )


def register_l3_engine(spark, fn, form: str = "engine") -> str:
    """Register `fn`'s GDAL spec engine UDFs under its SQL name, each carrying the driver's
    remote-path switch to the workers; returns the form used.

    ``engine-c1`` (and ``engine``, the default, where SQL functions work on a classic session): the batch UDF
    behind a wrapper that sends virtual, NULL and error tiles to the row-wise UDF. ``engine-c0`` (and the
    default anywhere else, Spark Connect included): the row-wise UDF alone. A row whose output is features has
    no batch form: every form registers its row-wise UDF, and the returned form says ``engine-c0``. The default is the form the row
    measured fastest (``levels.engine_form``).
    """
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.engine import l3 as engine_l3
    from databricks.labs.gbx.manager import capabilities, jvm, levels, sqlwrap

    reg = _gdal_policy.carrying_session(spark)  # its UDFs carry the driver's switch
    grid = _factory_args("proj_grid_dirs")[0]
    name = fn.sql_name
    if form == levels.ENGINE:
        form = levels.engine_form(fn)
    wrapped = fn.gdal.out_kind == "raster" and (
        form == "engine-c1"
        or (
            form == "engine"
            and not capabilities.is_connect(spark)
            and capabilities._sql_functions(spark)
        )
    )  # a features output has no batch form: its row-wise UDF serves every form
    file_aware = _engine_file_ref(spark, fn)
    if not wrapped and file_aware:
        # the row-wise UDF that also takes a FILE reference, behind a wrapper that mints it from the tile's path
        from databricks.labs.gbx.ds.file_gbx import file_ref_sql

        private = jvm._priv(fn, "L3f")
        register_engine_c0(spark, fn, private, file_ref=True)
        jvm._create_wrapper(
            spark,
            fn,
            sqlwrap.file_ref_wrapper(
                fn, private, file_ref_sql(fn.params[0].name, spark)
            ),
        )
        return "engine-c0"
    if not wrapped:
        register_engine_c0(spark, fn, name)
        return "engine-c0"
    c0_name, c1_name = jvm._priv(fn, "L3"), jvm._priv(fn, "L3c1")
    register_engine_c0(spark, fn, c0_name, file_ref=file_aware)
    reg.udf.register(c1_name, engine_l3.c1(fn.sql_name, grid))
    first = fn.params[0].name
    if fn.gdal.inputs[0][1] == "rasters":
        ddl = sqlwrap.tiles_dispatch(
            fn,
            sqlwrap.tile_from_result(
                c1_name + "(transform({tiles}, gbx_t -> gbx_t.raster){rest})",
                "{tiles}[0]",
            ),
            c0_name,
        )
    else:
        ddl = sqlwrap.tile_dispatch(
            fn,
            sqlwrap.tile_from_result(c1_name + "({tile}.raster{rest})", first),
            c0_name,
            (_file_sql(spark, first) if file_aware else ""),
        )
    jvm._create_wrapper(spark, fn, ddl)
    return "engine-c1"
