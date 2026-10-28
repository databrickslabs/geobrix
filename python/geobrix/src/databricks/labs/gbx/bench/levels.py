"""Streamline perf matrix: one Form = (function, level, shape) timed on one input at 1x/4x/16x.

Every MVP appends its forms to ``levels_forms.FORMS``; ``run_mvp`` times each one at the
levels the compute offers and records the attribution split (``attribution.py``) in the same
``ResultRow`` the other bench legs write.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from databricks.labs.gbx.bench import attribution
from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.runner import capture_env, time_iters

_L3_VARIANTS = (
    "c0",
    "c1",
    "d",
)  # any other variant is a label: register picks the L3 default
_PYTHON_LEVELS = ("L3", "DF")  # levels whose body runs in Spark's Python workers
_NO_PLAN = {**{k: None for k in attribution.METRICS}, "source": "dataframe-form"}


@dataclass(frozen=True)
class Form:
    mvp: str
    fn: str
    level: str  # L1 | L2 | L3 | L4 | DF | JVM-DF
    variant: str  # c0|c1|d, or a shape name (udtf, array_inline, count_part, udaf, ...)
    input: str  # key in levels_forms.INPUTS
    sql: str = ""  # one-row SELECT over {src} (and {uc} for L4) returning (n, fp)
    # "module:function" -> f(spark, src) -> (n, fp) or (n, fp, df) for DataFrame forms; the
    # optional df is the one that ran, and its plan feeds the attribution split.
    run: str = ""
    computes: Tuple[str, ...] = ("dedicated", "standard", "serverless")
    register: bool = True


def detect_compute(spark) -> str:
    from databricks.labs.gbx.manager.capabilities import is_connect, is_serverless

    if is_serverless():
        return "serverless"
    return "standard" if is_connect(spark) else "dedicated"


def run_mvp(
    spark,
    mvp: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    corpus: str,
    schema: Optional[str],
    uc: Optional[str] = None,
    scales=(1, 4, 16),
    where: str = "cluster",
    only_levels: Optional[Tuple[str, ...]] = None,
    base_rows: Optional[int] = None,
) -> List[ResultRow]:
    """Time every form of `mvp` that this compute offers, at each scale; one row per form and scale.

    `schema` (``catalog.schema``) caches the inputs as Delta tables; None keeps them as temp
    views. `uc` is the ``catalog.schema`` holding the L4 functions (L4 forms are skipped, with a
    row saying so, when it is not given). A form that fails is an ``error`` row, not an exception.
    """
    from databricks.labs.gbx import register
    from databricks.labs.gbx.bench import levels_forms as lf
    from databricks.labs.gbx.manifest import resolve_ref

    if mvp not in lf.FORMS:
        raise ValueError(
            f"no forms for MVP {mvp!r} yet (have: {', '.join(sorted(lf.FORMS))})"
        )
    compute = detect_compute(spark)
    env = capture_env(where)
    out: List[ResultRow] = []
    registered: Dict[str, Tuple[str, str]] = {}
    for form in lf.FORMS[mvp]:
        if compute not in form.computes or (
            only_levels and form.level not in only_levels
        ):
            continue
        for scale in scales:
            requested = (base_rows or lf.BASE_ROWS[form.input]) * scale
            base = dict(
                run_id=run_id,
                api=form.level,
                fn=form.fn,
                category=f"mvp-{mvp}",
                mode="spark-path",
                rows=requested,
                level=form.level,
                form=form.variant,
                compute=compute,
                **env,
            )
            try:
                if form.level == "L4" and not uc:
                    out.append(
                        _row(
                            base,
                            note="no UC schema given (--levels-uc)",
                            status="na_by_design",
                        )
                    )
                    continue
                src = lf.INPUTS[form.input](spark, requested, corpus, schema)
                if src is None:
                    out.append(
                        _row(
                            base,
                            note="input unavailable (no tile corpus)",
                            status="na_by_design",
                        )
                    )
                    continue
                if form.register:
                    want = (
                        form.level,
                        form.variant if form.variant in _L3_VARIANTS else "-",
                    )
                    if registered.get(form.fn) != want:
                        forced = (
                            {form.fn: want}
                            if form.level in ("L1", "L2", "L3")
                            else None
                        )
                        register(spark, only=[form.fn], _levels=forced)
                        registered[form.fn] = want
                holder: Dict = {}

                def _go():
                    holder["t0"] = time.time()
                    if form.run:
                        res = resolve_ref(form.run)(spark, src)
                        holder["n_fp"] = tuple(res[:2])
                        if len(res) > 2:
                            holder["df"] = res[2]
                    else:
                        df = spark.sql(form.sql.format(src=src, uc=uc or ""))
                        r = df.collect()[0]
                        holder["n_fp"], holder["df"] = (r[0], r[1]), df

                timing = time_iters(_go, warmup, measured)
                n, fp = holder["n_fp"]
                if not n or fp is None:
                    raise ValueError(
                        f"the form produced no output (n={n}, fingerprint={fp})"
                    )
                plan = (
                    attribution.from_plan(holder["df"], holder["t0"])
                    if "df" in holder
                    else dict(_NO_PLAN)
                )
                m = {k: plan[k] for k in attribution.METRICS}
                note = [f"attr={plan['source']}"]
                note += _python_side(spark, form.level, _go, m)
                if n < requested:
                    note.append(f"short input: {n} of {requested} rows")
                gone = attribution.unavailable(m)
                if gone:
                    note.append("unavailable: " + ",".join(gone))
                out.append(
                    _row(
                        {**base, "rows": min(int(n), requested)},
                        note="; ".join(note),
                        status="ok",
                        timing=timing,
                        fp=str(fp),
                        m=m,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - a failure is a measured outcome
                out.append(
                    _row(
                        base,
                        status="error",
                        failure=f"{type(exc).__name__}: {str(exc)[:300]}",
                    )
                )
    return out


def _python_side(spark, level: str, go, m: Dict) -> List[str]:
    """Fill the Python-worker measures of `m` for `level`; returns the note tokens."""
    if level in _PYTHON_LEVELS:
        prof = attribution.python_eval_s(spark, go)
        if prof is not None:
            m["py_eval_s"] = prof
        m["py_worker_rss_mb"] = attribution.worker_rss_mb(spark, 8)
        return ["py=" + ("profiler" if prof is not None else "plan")]
    if level == "L4":  # runs in the engine's sandbox, outside Spark's workers
        m["py_eval_s"] = m["py_worker_rss_mb"] = None
    else:  # the JVM: no Python worker is involved
        m["py_eval_s"] = m["py_worker_rss_mb"] = 0.0
    return []


def _row(
    base: Dict,
    status: str,
    note: str = "",
    timing: Optional[Dict] = None,
    fp: str = "",
    m: Optional[Dict] = None,
    failure: str = "",
) -> ResultRow:
    t, m = timing or {}, m or {}
    med = t.get("iter_median_ms", 0.0) / 1000.0
    rows = base["rows"]
    label = base["form"] if base["form"] != "-" else ""
    return ResultRow(
        **base,
        tile_px=0,
        bands=0,
        dtype="",
        srid=0,
        nodata_frac=0.0,
        warmup_iters=t.get("warmup_iters", 0),
        measured_iters=t.get("measured_iters", 0),
        iter_median_s=med,
        iter_min_s=t.get("iter_min_ms", 0.0) / 1000.0,
        iter_p90_s=t.get("iter_p90_ms", 0.0) / 1000.0,
        iter_total_wall_clock_s=t.get("iter_total_wall_clock_ms", 0.0) / 1000.0,
        avg_wall_clock_s=t.get("avg_wall_clock_ms", 0.0) / 1000.0,
        throughput_mpix_s=0.0,
        throughput_rows_s=(rows / med) if med else 0.0,
        peak_rss_mb=0.0,
        status=status,
        note="; ".join(x for x in (label, note) if x),
        output_fingerprint=fp,
        per_tile_avg_s=(med / rows) if rows and med else 0.0,
        per_tile_avg_ms=(med * 1000.0 / rows) if rows and med else 0.0,
        ser_sent_bytes=int(m.get("ser_sent_bytes") or 0),
        ser_recv_bytes=int(m.get("ser_recv_bytes") or 0),
        py_eval_s=float(m.get("py_eval_s") or 0.0),
        shuffle_bytes=int(m.get("shuffle_bytes") or 0),
        spill_bytes=int(m.get("spill_bytes") or 0),
        peak_exec_mem_bytes=int(m.get("peak_exec_mem_bytes") or 0),
        gc_s=float(m.get("gc_s") or 0.0),
        py_worker_rss_mb=float(m.get("py_worker_rss_mb") or 0.0),
        failure_mode=failure,
    )


def summarize(rows: List[ResultRow]) -> str:
    """A markdown table of a levels run: one line per form and scale, the attribution split beside the timing."""

    def mib(b: int) -> str:
        return f"{b / 2**20:.1f}"

    head = [
        "fn",
        "level",
        "form",
        "compute",
        "rows",
        "median_s",
        "rows/s",
        "sent_MiB",
        "recv_MiB",
        "py_eval_s",
        "shuffle_MiB",
        "spill_MiB",
        "peak_MiB",
        "gc_s",
        "rss_MB",
        "status",
        "note",
    ]
    lines = [
        f"# Levels leg: {len(rows)} rows",
        "",
        "| " + " | ".join(head) + " |",
        "|" + "---|" * len(head),
    ]
    for r in sorted(rows, key=lambda r: (r.fn, r.level, r.form, r.rows)):
        cells = [
            r.fn,
            r.level,
            r.form,
            r.compute,
            str(r.rows),
            f"{r.iter_median_s:.3f}",
            f"{r.throughput_rows_s:.0f}",
            mib(r.ser_sent_bytes),
            mib(r.ser_recv_bytes),
            f"{r.py_eval_s:.3f}",
            mib(r.shuffle_bytes),
            mib(r.spill_bytes),
            mib(r.peak_exec_mem_bytes),
            f"{r.gc_s:.3f}",
            f"{r.py_worker_rss_mb:.0f}",
            r.status,
            (r.failure_mode or r.note).replace("|", "/"),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"
