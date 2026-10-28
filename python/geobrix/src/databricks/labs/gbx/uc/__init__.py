"""gbx.uc.install / verify: deploy the UC surface from an env 6 notebook.

Importing this package pulls in no pyspark: the handlers module runs inside a UC Python sandbox.
"""

from __future__ import annotations

import os
import re
from typing import Iterable, List, Optional

from databricks.labs.gbx import __version__, manifest
from databricks.labs.gbx.uc import ddl

ENV = "6"  # the UC functions run on Serverless env 6 (their wheel is the env 6 wheel)


def _public_version() -> str:
    return __version__.split("+", 1)[0]


def _require_env6() -> None:
    env = os.environ.get("DATABRICKS_ENV_VERSION", "")
    rt = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
    major = rt.split(".")[0]
    if env == ENV or (major.isdigit() and int(major) >= 19):
        return
    raise RuntimeError(
        f"gbx.uc.install runs from an env {ENV} notebook or job (Serverless env {ENV} or DBR 19); "
        f"this runtime is env={env or '-'} runtime={rt or '-'}"
    )


def _env_wheel_name() -> str:
    return f"geobrix-{_public_version()}+env{ENV}-py3-none-any.whl"


def _single_wheel(volume_dir: str) -> str:
    """The one wheel the UC functions install from: the env 6 wheel of this version.

    A plain ``geobrix-<version>-py3-none-any.whl`` is accepted until the per-environment wheels
    (``+env5`` / ``+env6``) are published. A wheel of another environment is refused by name.
    """
    from databricks.labs.gbx.ds._listing import list_files

    pub = _public_version()
    found = list_files(
        volume_dir,
        filter_regex=rf".*/geobrix-{re.escape(pub)}[+-][^/]*\.whl$",
        recursive=False,
        raise_on_empty=False,
    )

    def env_of(path: str) -> Optional[str]:
        m = re.search(r"\+env([0-9]+)-", os.path.basename(path))
        return m.group(1) if m else None

    ok = [p for p in found if env_of(p) in (None, ENV)]
    other = [os.path.basename(p) for p in found if env_of(p) not in (None, ENV)]
    if ok and other:
        raise RuntimeError(
            f"{volume_dir} holds {os.path.basename(ok[0])} and {other[0]}, wheels for different environments. "
            f"Use one folder per environment and never put both environment wheels in one folder; "
            f"keep only {_env_wheel_name()} here"
        )
    if len(ok) == 1:
        return ok[0]
    if other:
        raise RuntimeError(
            f"{volume_dir} holds {other[0]}, a wheel for another environment; the UC functions run on "
            f"env {ENV}: stage {_env_wheel_name()} there (one wheel per environment folder)"
        )
    raise RuntimeError(
        f"{volume_dir} must hold exactly one geobrix-{pub}*.whl for env {ENV} (found {len(ok)}); "
        f"stage {_env_wheel_name()} there (one wheel per environment folder)"
    )


def _l4_names(only: Optional[Iterable[str]]) -> List[str]:
    man = manifest.load()
    uc_names = [n for n, f in man.functions.items() if "L4" in f.levels]
    if only is None:
        return uc_names
    want = [
        n.lower() if n.lower().startswith("gbx_") else f"gbx_{n.lower()}" for n in only
    ]
    unknown = [n for n in want if n not in uc_names]
    if unknown:
        raise ValueError(
            f"install(only=...) got name(s) with no UC function: {unknown}"
        )
    # A table function is a SQL body over another UC function, which has to exist when it is created.
    chosen = set(want) | {
        man.functions[n].levels["L4"]["tvf_over"]
        for n in want
        if "tvf_over" in man.functions[n].levels["L4"]
    }
    return [n for n in uc_names if n in chosen]


def install(
    catalog: str,
    schema: str,
    volume_dir: str,
    *,
    spark=None,
    only: Optional[Iterable[str]] = None,
    dry_run: bool = False,
) -> List[str]:
    """Create the UC functions in ``catalog.schema`` from the wheel in ``volume_dir``; return the DDL.

    ``dry_run`` only returns the DDL (the wheel name is the env 6 wheel's, not looked up).
    """
    # Everything that reaches a statement is checked before any statement is built or any folder read.
    ddl.quote_ident("catalog", catalog)
    ddl.quote_ident("schema", schema)
    ddl.check_volume_dir(volume_dir)
    man = manifest.load()
    names = _l4_names(only)
    if not dry_run:
        _require_env6()
    wheel = (
        f"{volume_dir.rstrip('/')}/{_env_wheel_name()}"
        if dry_run
        else _single_wheel(volume_dir)
    )
    stmts: List[str] = []
    for n in names:
        stmts += ddl.function_ddl(
            man.functions[n],
            catalog,
            schema,
            "file://" + wheel,
            man.uc_dependencies,
            _public_version(),
        )
    if not dry_run:
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        for s in stmts:
            spark.sql(s)
        verify(spark, catalog, schema, names)
    return stmts


def verify(
    spark, catalog: Optional[str], schema: Optional[str], names: Iterable[str]
) -> None:
    """register(target='uc'): every name exists in the schema, created by this wheel's version; else fail."""
    if not catalog or not schema:
        raise ValueError(
            "register(target='uc') needs catalog= and schema=. "
            "Fix: pass the catalog and schema the UC functions were installed in."
        )
    qc, qs = ddl.quote_ident("catalog", catalog), ddl.quote_ident("schema", schema)
    names = list(names)
    # Unity Catalog stores names lower-case whatever case they were created with: compare lower-case.
    rows = spark.sql(
        f"SELECT routine_name, comment FROM {qc}.information_schema.routines "
        f"WHERE lower(routine_schema) = {ddl.quote_str(schema.lower())}"
    ).collect()
    have = {r["routine_name"].lower(): r["comment"] for r in rows}
    want = f"geobrix {_public_version()}"
    bad = [n for n in names if have.get(n.lower()) != want]
    if bad:
        raise RuntimeError(
            f"UC schema {catalog}.{schema} does not match wheel {__version__} for {bad} "
            f"(found {[have.get(n.lower()) for n in bad]}); run gbx.uc.install from an env {ENV} notebook"
        )
    print(
        f"geobrix: UC functions verified in {catalog}.{schema} ({__version__}); for unqualified names run: "
        f"SET PATH = {qc}.{qs}, system.session, system.builtin, system.ai, CURRENT_SCHEMA"
    )
