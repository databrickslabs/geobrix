"""Checks a bench launch can make with no compute: the probe SQL and the client-side input sizes.

* ``probe_failures`` runs the forms' SQL on a LOCAL Spark session with a handful of rows, so a statement
  that cannot run is found before anything is submitted. Two real failures it catches: a non-deterministic
  function (a Java UDF) inside ``sum()`` (AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION) and a sum of raw
  64-bit hashes under ANSI (ARITHMETIC_OVERFLOW). The JVM levels (L1, L2) cannot run without the JAR, so their SQL
  runs against the light function of the same name registered as non-deterministic, the way a Java UDF is.
* ``client_cap_problems`` refuses a ladder whose inputs are built on the client above the cap.

Local, so it needs ``pyspark`` and the light-tier packages (the bench launcher's venv has both).
"""

from __future__ import annotations

import contextlib
import dataclasses
from typing import Dict, List, Optional, Tuple
from unittest import mock

from databricks.labs.gbx.bench import clientcap
from databricks.labs.gbx.bench import levels_forms as lf

_JVM_SQL_LEVELS = ("L1", "L2")


@contextlib.contextmanager
def nondeterministic_udfs():
    """Within the block every UDF registered with ``spark.udf.register`` is non-deterministic, as a Java UDF is."""
    from pyspark.sql.functions import udf
    from pyspark.sql.udf import UDFRegistration

    real = UDFRegistration.register

    def register(self, name, f, returnType=None):
        if hasattr(f, "asNondeterministic"):
            f = f.asNondeterministic()
        elif returnType is not None:
            f, returnType = udf(f, returnType).asNondeterministic(), None
        return real(self, name, f, returnType)

    UDFRegistration.register = register
    try:
        yield
    finally:
        UDFRegistration.register = real


def _jvm_sql_standins(forms) -> list:
    """One L3 stand-in per distinct (function, SQL) of the SQL forms at a JVM level."""
    seen, out = set(), []
    for f in forms:
        if (
            f.level in _JVM_SQL_LEVELS
            and f.sql
            and "{uc}" not in f.sql
            and (f.fn, f.sql) not in seen
        ):
            seen.add((f.fn, f.sql))
            out.append(
                dataclasses.replace(
                    f,
                    level="L3",
                    variant="c0",
                    computes=("dedicated", "standard", "serverless"),
                )
            )
    return out


def probe_failures(
    spark, mvp: str, corpus: str, *, rows: int = 16
) -> Tuple[List[str], List[str]]:
    """Run the forms of `mvp` on `rows` rows locally. Returns ``(failures, skipped)``: one line per form that
    failed, and one per form that could not run here (L4, the JVM levels without a stand-in, no local corpus).
    """
    from databricks.labs.gbx.bench import levels

    failures: List[str] = []
    skipped: List[str] = []

    def run(forms, only, label):
        with mock.patch.dict(lf.FORMS, {mvp: forms}):
            got = levels.run_mvp(
                spark, mvp, "preflight", 0, 1, corpus=corpus, schema=None, scales=(1,),
                where="preflight", base_rows=rows, only_levels=only,
            )  # fmt: skip
        for r in got:
            tag = f"{r.fn} {r.level}/{r.form} ({label})"
            if r.status == "error":
                failures.append(f"{tag}: {r.failure_mode}")
            elif r.status == "na_by_design":
                skipped.append(f"{tag}: {r.note}")

    forms = list(lf.FORMS[mvp])
    run(forms, ("L3", "DF"), "light")
    standins = _jvm_sql_standins(forms)
    if standins:
        with nondeterministic_udfs():
            run(standins, ("L3",), "as a Java UDF")
    skipped += [
        f"{f.fn} {f.level}/{f.variant}: not runnable without the JAR or Unity Catalog"
        for f in forms
        if f.level in ("L4", "L1", "L2", "L1-steps", "JVM-DF")
        and not (f.level in _JVM_SQL_LEVELS and f.sql)
    ]
    return failures, skipped


def client_cap_problems(
    mvp: str, scales, *, base_rows: Optional[int] = None, cap: Optional[int] = None
) -> List[str]:
    """The inputs of `mvp` whose client-built bytes at the largest scale pass the cap, one line each."""
    cap = clientcap.MAX_CLIENT_BYTES if cap is None else cap
    out: Dict[str, str] = {}
    for f in lf.FORMS[mvp]:
        rows = (base_rows or lf.BASE_ROWS[f.input]) * max(scales)
        try:
            clientcap.check_estimate(
                f"{f.input} x {rows}", 1, lf.client_bytes(f.input, rows), cap
            )
        except clientcap.ClientInputTooLarge as exc:
            out[f.input] = str(exc)
    return list(out.values())
