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


def _factory_args(kind: str):
    if kind == "proj_grid_dirs":
        from databricks.labs.gbx.core import proj_grids

        return (tuple(proj_grids.get_registered_dirs()),)
    raise ValueError(kind)


def register_l3(spark, fn, variant: str, name: Optional[str] = None) -> None:
    spec, name = fn.levels["L3"], name or fn.sql_name
    if "variants" in spec:
        spark.udf.register(name, resolve_ref(spec["variants"][variant]))
    elif "factory" in spec:
        spark.udf.register(
            name, resolve_ref(spec["factory"])(*_factory_args(spec["factory_args"]))
        )
    elif "callable" in spec:
        spark.udf.register(
            name, resolve_ref(spec["callable"]), _return_type(spec["returns"])
        )
    elif "udtf" in spec:
        spark.udtf.register(name, resolve_ref(spec["udtf"]))
    else:
        raise ValueError(f"{fn.sql_name}: unknown L3 registration")
