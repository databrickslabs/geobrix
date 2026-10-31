"""Checks a bench launch can make with no compute: the probe SQL and the client-side input sizes.

* ``probe_failures`` runs the forms' SQL on a LOCAL Spark session with a handful of rows, so a statement
  that cannot run is found before anything is submitted. Two real failures it catches: a non-deterministic
  function (a Java UDF) inside ``sum()`` (AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION) and a sum of raw
  64-bit hashes under ANSI (ARITHMETIC_OVERFLOW). The JVM levels (L1, L2) cannot run without the JAR, so their SQL
  runs against the light function of the same name registered as non-deterministic, the way a Java UDF is.
  Its job is to find statements that cannot run, not to measure: a form that is a memory or scale probe by
  design (``Form.memory_probe``) is skipped, and a JVM form that needs the JAR for its own setup is not
  checkable on a JAR-less local session. Both are reported as skipped, with the reason, never as failures.
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
ENGINE_L3 = ("engine", "engine-c0", "engine-c1")


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


def _same_shape(fn, level: str) -> bool:
    """True when the JVM level returns what the light function returns (else a stand-in would not resolve)."""
    return (
        fn.levels.get(level, {}).get("session_returns", fn.returns_session)
        == fn.returns_session
    )


def _jvm_sql_standins(forms) -> list:
    """One L3 stand-in per distinct (function, SQL) of the SQL forms at a JVM level.

    No stand-in for a form with its own `setup` (it registers JVM names, the JAR), nor for a function whose JVM level
    returns another shape than the light one (`session_returns`: a `tile` column, not the tile fields). A function
    on the GDAL spec engine gets the engine stand-in: its JVM form takes the spec the engine takes.
    """
    from databricks.labs.gbx import manifest

    fns = manifest.load().functions
    seen, out = set(), []
    for f in forms:
        fn = fns.get(f.fn)
        if (
            f.level in _JVM_SQL_LEVELS
            and f.sql
            and not f.setup
            and not f.memory_probe
            and (fn is None or _same_shape(fn, f.level))
            and "{uc}" not in f.sql
            and (f.fn, f.sql) not in seen
        ):
            seen.add((f.fn, f.sql))
            engine = fn is not None and fn.levels.get("L3", {}).get("engine")
            out.append(
                dataclasses.replace(
                    f,
                    level="L3",
                    variant="engine-c0" if engine else "c0",
                    computes=("dedicated", "standard", "serverless"),
                )
            )
    return out


def _on_engine(form) -> bool:
    """True when the form runs on the GDAL spec engine (an engine variant, or the bench's engine floor)."""
    base = form.variant.split("@")[0]
    return form.level == "L3" and (base in ENGINE_L3 or base.startswith("floor"))


def _has_standin(form, standins) -> bool:
    return any(s.fn == form.fn and s.sql == form.sql for s in standins)


def probe_failures(
    spark, mvp: str, corpus: str, *, rows: int = 16
) -> Tuple[List[str], List[str]]:
    """Run the forms of `mvp` on `rows` rows locally. Returns ``(failures, skipped)``: one line per form that
    failed, and one per form that was not run here: ``skipped by design`` (a memory or scale probe), or not
    checkable here (L4, a JVM form that needs the JAR, no local corpus). Every form runs on the `rows` it is given.
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

    every = list(lf.FORMS[mvp])
    skipped += [
        f"{f.fn} {f.level}/{f.variant}: skipped by design (a memory or scale probe, not a statement check)"
        for f in every
        if f.memory_probe
    ]
    forms = [f for f in every if not f.memory_probe]
    from databricks.labs.gbx.engine import run as engine_run

    no_engine = (
        engine_run.unavailable_reason()
    )  # the GDAL bindings of this interpreter (a host venv has none)
    if no_engine:
        why = f"not checkable here (the spec engine is unavailable in this interpreter: {no_engine})"
        skipped += [
            f"{f.fn} {f.level}/{f.variant}: {why}" for f in forms if _on_engine(f)
        ]
        forms = [f for f in forms if not _on_engine(f)]
    standins = _jvm_sql_standins(forms)
    standin_src = [f for f in forms if _has_standin(f, standins)]
    run(forms, ("L3", "DF"), "light")
    if no_engine:
        skipped += [
            f"{f.fn} {f.level}/{f.variant}: {why}" for f in standins if _on_engine(f)
        ]
        standins = [f for f in standins if not _on_engine(f)]
    if standins:
        with nondeterministic_udfs():
            run(standins, ("L3",), "as a Java UDF")
    skipped += [
        f"{f.fn} {f.level}/{f.variant}: not checkable here (needs the JAR or Unity Catalog)"
        for f in forms
        if f.level in ("L4", "L1", "L2", "L1-steps", "JVM-DF") and f not in standin_src
    ]
    return failures, skipped


def client_cap_problems(
    mvp: str,
    scales,
    *,
    base_rows: Optional[int] = None,
    cap: Optional[int] = None,
    forms=(),
) -> List[str]:
    """The inputs of `mvp` whose client-built bytes at the largest scale pass the cap, one line each.

    `forms` (the `--levels-forms` names, matched on the form's variant) limits the judgement to those forms; empty
    judges them all. A set with no forms table (IO) has no client-built inputs: no problem.
    """
    cap = clientcap.MAX_CLIENT_BYTES if cap is None else cap
    out: Dict[str, str] = {}
    for f in lf.FORMS.get(mvp, ()):
        if forms and f.variant not in forms:
            continue
        rows = (base_rows or lf.BASE_ROWS[f.input]) * max(scales)
        try:
            clientcap.check_estimate(
                f"{f.input} x {rows}", 1, lf.client_bytes(f.input, rows), cap
            )
        except clientcap.ClientInputTooLarge as exc:
            out[f.input] = str(exc)
    return list(out.values())
