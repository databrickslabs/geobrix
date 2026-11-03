"""The runtime of the generated Python API (the rasterx, gridx, vectorx and pmtiles ``functions`` modules).

Every generated function calls the SQL function GeoBrix registers under the same manifest name, so the
Python API and SQL run one implementation at one level. The first call of a name in a session registers
it through the manager; later calls are a set lookup. Nothing here touches the JVM.
"""

from __future__ import annotations

import math
import weakref
from typing import Any, Dict, Iterable, Set

_DONE: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
_DONE_BY_ID: Dict[int, Set[str]] = {}


def _active():
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _done(spark) -> Set[str]:
    try:
        return _DONE.setdefault(spark, set())
    except TypeError:  # a session object that cannot be weakly referenced
        return _DONE_BY_ID.setdefault(id(spark), set())


def mark_registered(spark, names: Iterable[str]) -> None:
    """Called by the manager after it registers names, so a later Python call costs nothing."""
    _done(spark).update(names)


def ensure(spark, sql_name: str) -> None:
    """Register ``sql_name`` in ``spark`` once, at the level the manager picks."""
    if sql_name in _done(spark):
        return
    from databricks.labs.gbx import manager

    choices = manager.register(spark, only=[sql_name])
    if sql_name not in choices:
        raise RuntimeError(
            f"{sql_name} cannot run on this compute (the line above says why). "
            "Fix: install its light dependencies, or see the availability page."
        )
    _done(spark).add(sql_name)


def _fn(sql_name: str):
    from databricks.labs.gbx import manifest

    return manifest.load().functions[sql_name]


def _arg(value: Any, param):
    from pyspark.sql import Column
    from pyspark.sql import functions as F

    from databricks.labs.gbx.manifest import sql_type

    if isinstance(value, Column):
        return value
    if value is None:
        return F.lit(None).cast(sql_type(param.session[0]))
    if isinstance(value, str):
        return F.lit(value) if param.py == "lit" else F.col(value)
    return F.lit(value)


def _columns(fn, values, fill_all: bool):
    """Columns for ``values``. A trailing optional left as None is not sent (the SQL default applies); an
    optional left as None before one that is set gets the manifest's SQL default, never a shifted value.
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx.manifest import sql_type

    if len(values) > fn.max_args:
        raise TypeError(
            f"{fn.sql_name} takes at most {fn.max_args} arguments ({len(values)} given)"
        )
    vals = list(values) + [None] * (fn.max_args - len(values))
    set_at = [i for i, v in enumerate(vals) if v is not None]
    last = fn.max_args - 1 if fill_all else max(set_at + [fn.min_args - 1])
    cols = []
    for i in range(last + 1):
        p, v = fn.params[i], vals[i]
        if v is None and i >= fn.min_args:
            cols.append(F.expr(f"CAST({p.default} AS {sql_type(p.session[0])})"))
        else:
            cols.append(_arg(v, p))
    return cols


def call(sql_name: str, *values):
    """The Column for ``sql_name(values...)``, the name registered in the active session first."""
    from pyspark.sql import functions as F

    cols = _columns(_fn(sql_name), values, fill_all=False)
    ensure(_active(), sql_name)
    return F.call_function(sql_name, *cols)


def forced(virtualize_dir, virtualize_prefix, materialize) -> bool:
    """A force-output keyword is set: ``materialize=False`` is the natural (unforced) form, not a force."""
    return (
        virtualize_dir is not None or virtualize_prefix is not None or bool(materialize)
    )


def force_output(
    sql_name: str, virtualize_dir, virtualize_prefix, materialize, *values
):
    """The Python-only force-output path: the row's ``extra_impl`` (a light UDF, run directly, as the light
    Column API does today) with every SQL argument (defaults filled) and the three force-output literals.
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.pyrx.functions import _validate_force_output

    _validate_force_output(virtualize_dir, materialize)
    fn = _fn(sql_name)
    impl = manifest.resolve_ref(fn.python.extra_impl)
    if fn.python.extra_factory_args:
        from databricks.labs.gbx.manager.l3 import _factory_args

        impl = impl(*_factory_args(fn.python.extra_factory_args))
    cols = _columns(fn, values, fill_all=True)
    return impl(
        *cols, F.lit(virtualize_dir), F.lit(virtualize_prefix), F.lit(materialize)
    )


def given(*extras) -> bool:
    """A Python-only keyword is set (``None`` is unset; ``False`` is a value)."""
    return any(e is not None for e in extras)


def extra_call(sql_name: str, extras, *values):
    """The Python-only keyword path of a row whose keywords are not the force-output trio: the row's ``extra_impl``
    (a function of the SQL arguments, defaults filled, then one literal Column per keyword).
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx import manifest

    fn = _fn(sql_name)
    impl = manifest.resolve_ref(fn.python.extra_impl)
    cols = _columns(fn, values, fill_all=True)
    return impl(*cols, *[F.lit(e) for e in extras])


def _sql_literal(value: Any, param) -> str:
    from databricks.labs.gbx.manifest import sql_type

    if value is None:
        return f"CAST(NULL AS {sql_type(param.session[0])})"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value!r}D" if math.isfinite(value) else f"CAST('{value}' AS DOUBLE)"
    if isinstance(value, (bytes, bytearray)):
        return "X'" + bytes(value).hex() + "'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"{param.name}: a {type(value).__name__} is not a SQL literal")


def table(df, sql_name: str, *values):
    """``df`` LATERAL-joined with the table function ``sql_name(values...)``: one output row per generated
    row, beside the input row's columns. A str is a column of ``df`` where the parameter reads columns.
    """
    from pyspark.sql import Column

    from databricks.labs.gbx.manifest import sql_type

    fn = _fn(sql_name)
    if len(values) > fn.max_args:
        raise TypeError(
            f"{sql_name} takes at most {fn.max_args} arguments ({len(values)} given)"
        )
    taken = {c.lower() for c in df.columns}
    clash = [n for n, _ in fn.columns if n.lower() in taken]
    if clash:
        raise ValueError(
            f"{sql_name} returns the column(s) {clash}, which the input DataFrame already has. "
            f"Fix: rename them first, e.g. df = df.withColumnRenamed('{clash[0]}', '{clash[0]}_in')."
        )
    vals = list(values) + [None] * (fn.max_args - len(values))
    set_at = [i for i, v in enumerate(vals) if v is not None]
    last = max(set_at + [fn.min_args - 1])
    args = []
    for i in range(last + 1):
        p, v = fn.params[i], vals[i]
        if isinstance(v, Column):
            raise TypeError(
                f"{sql_name}: pass the column name of {p.name!r} as a str, not a Column. "
                f"Fix: df = df.withColumn('{p.name}', <expression>) first, then pass '{p.name}'."
            )
        if v is None and i >= fn.min_args:
            # an optional left out before one that is set takes its SQL default, never a shifted value
            args.append(f"CAST({p.default} AS {sql_type(p.session[0])})")
        elif isinstance(v, str) and p.py == "col":
            args.append("gbx_in.`" + v.replace("`", "``") + "`")
        else:
            args.append(_sql_literal(v, p))
    spark = df.sparkSession
    ensure(spark, sql_name)
    # spark.sql formats the text with str.format, so braces in an argument are escaped
    call_args = ", ".join(args).replace("{", "{{").replace("}", "}}")
    return spark.sql(
        f"SELECT gbx_in.*, gbx_out.* FROM {{gbx_in}} AS gbx_in, LATERAL {sql_name}({call_args}) AS gbx_out",
        gbx_in=df,
    )
