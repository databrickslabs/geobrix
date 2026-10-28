"""Streamline perf matrix: one Form = (function, level, shape) timed on one input at 1x/4x/16x.

Every MVP appends its forms to ``levels_forms.FORMS``; ``run_mvp`` times each one at the
levels the compute offers and records the attribution split (``attribution.py``) in the same
``ResultRow`` the other bench legs write.
"""

from __future__ import annotations

import dataclasses
import time
from dataclasses import dataclass, replace
from typing import Dict, List, Optional, Set, Tuple

from databricks.labs.gbx.bench import attribution
from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.runner import capture_env, time_iters

_L3_VARIANTS = (
    "c0",
    "c1",
    "d",
)  # any other variant is a label: register picks the L3 default
_PYTHON_LEVELS = ("L3", "DF")  # levels whose body runs in Spark's Python workers
# What only a Spark Python worker produces: not applicable to a JVM level or a UC function.
_PYTHON_FIELDS = (
    "ser_sent_bytes",
    "ser_recv_bytes",
    "py_eval_s",
    "py_eval_frac",
    "py_worker_rss_mb",
)
_NO_PLAN = {**{k: None for k in attribution.METRICS}, "source": "dataframe-form"}


@dataclass(frozen=True)
class Form:
    mvp: str
    fn: str
    level: str  # L1 | L2 | L3 | L4 | DF | JVM-DF
    variant: str  # c0|c1|d, or a shape name (udtf, array_inline, count_part, udaf, ...)
    input: str  # key in levels_forms.INPUTS
    # A one-row SELECT over {src} (and {uc} for L4) returning (n, fp, n_out): the output
    # count, an order-independent fingerprint, and count(<the output>), which skips NULLs.
    sql: str = ""
    # "module:function" -> f(spark, src) -> (n, fp, n_out) or (n, fp, n_out, df) for
    # DataFrame forms; the optional df is the one that ran, and its plan feeds the split.
    run: str = ""
    # "module:function" -> f(spark), called once before the form is first timed: registers what
    # `run` calls, so the timed region holds the work and not the registration.
    setup: str = ""
    computes: Tuple[str, ...] = ("dedicated", "standard", "serverless")
    register: bool = True
    # A form whose function may return NULL for some rows. Otherwise n_out != n is an error:
    # an error tile has a NULL raster and costs less than real work, so a form that fails on a
    # subset would come out faster and still read ok.
    allows_null: bool = False


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

    The timing is the median over the measured iterations; the attribution split is the plan of
    the LAST measured iteration (one sample), and the worker memory peak covers warmup and all
    measured iterations. A metric with no value is NULL, with the reason in the row's `note`.
    """
    from databricks.labs.gbx.bench import levels_forms as lf

    if mvp not in lf.FORMS:
        raise ValueError(
            f"no forms for MVP {mvp!r} yet (have: {', '.join(sorted(lf.FORMS))})"
        )
    ctx = _Ctx(spark, warmup, measured, corpus, schema, uc, {})
    compute = detect_compute(spark)
    env = capture_env(where)
    out: List[ResultRow] = []
    for form in lf.FORMS[mvp]:
        if compute not in form.computes or (
            only_levels and form.level not in only_levels
        ):
            continue
        rows: List[ResultRow] = []
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
                rows.append(_run_one(ctx, form, base, requested))
            except Exception as exc:  # noqa: BLE001 - a failure is a measured outcome
                rows.append(
                    _row(
                        base,
                        status="error",
                        failure=f"{type(exc).__name__}: {str(exc)[:300]}",
                    )
                )
        out += _guard_sent(rows)
    return out


@dataclass
class _Ctx:
    spark: object
    warmup: int
    measured: int
    corpus: str
    schema: Optional[str]
    uc: Optional[str]
    registered: Dict[str, Tuple[str, str]]
    setups: Set[str] = dataclasses.field(default_factory=set)


def _run_one(ctx: _Ctx, form: Form, base: Dict, requested: int) -> ResultRow:
    from databricks.labs.gbx import register
    from databricks.labs.gbx.bench import levels_forms as lf
    from databricks.labs.gbx.manifest import resolve_ref

    spark, uc = ctx.spark, ctx.uc
    if form.level == "L4" and not uc:
        return _row(
            base, note="no UC schema given (--levels-uc)", status="na_by_design"
        )
    src = lf.INPUTS[form.input](spark, requested, ctx.corpus, ctx.schema)
    if src is None:
        return _row(
            base, note="input unavailable (no tile corpus)", status="na_by_design"
        )
    if form.register:
        want = (form.level, form.variant if form.variant in _L3_VARIANTS else "-")
        if ctx.registered.get(form.fn) != want:
            forced = {form.fn: want} if form.level in ("L1", "L2", "L3") else None
            register(spark, only=[form.fn], _levels=forced)
            ctx.registered[form.fn] = want
    if form.setup and form.setup not in ctx.setups:
        resolve_ref(form.setup)(spark)
        ctx.setups.add(form.setup)
    holder: Dict = {}

    def _go():
        holder["t0"] = time.time()
        if form.run:
            res = resolve_ref(form.run)(spark, src)
            if len(res) < 3:
                raise ValueError("a run form returns (n, fp, n_out[, df])")
            holder["n_fp"] = tuple(res[:3])
            if len(res) > 3:
                holder["df"] = res[3]
        else:
            df = spark.sql(form.sql.format(src=src, uc=uc or ""))
            r = df.collect()[0]
            holder["n_fp"], holder["df"] = (r[0], r[1], r[2]), df

    peak = attribution.WorkerPeak(spark) if form.level in _PYTHON_LEVELS else None
    if peak:
        peak.start()  # the memory peak is the shape's, not a worker's lifetime
    timing = time_iters(_go, ctx.warmup, ctx.measured)
    n, fp, n_out = holder["n_fp"]
    if n_out != n and not form.allows_null:
        raise ValueError(f"{n - n_out} of {n} outputs NULL")
    if not n or fp is None:
        raise ValueError(f"the form produced no output (n={n}, fingerprint={fp})")
    # The plan first: the worker read below is a query of its own, and the plan lookup must
    # not mistake it for the form's.
    plan = (
        attribution.from_plan(holder["df"], holder["t0"])
        if "df" in holder
        else dict(_NO_PLAN)
    )
    rss = peak.read() if peak else None
    m = {k: plan[k] for k in attribution.METRICS}
    m["py_worker_rss_mb"] = rss
    note = [f"attr={plan['source']}"]
    na: Tuple[str, ...] = ()
    if form.level in _PYTHON_LEVELS:
        for k in ("ser_sent_bytes", "ser_recv_bytes"):
            if m[k] == 0:  # a Python plan that moved no bytes did not report them
                m[k] = None
        if rss is None and peak.reason:
            note.append(f"rss: {peak.reason}")
    else:  # a JVM level or a UC function: no Spark Python worker is involved
        na = _PYTHON_FIELDS
        m.update({k: None for k in na})
    if n < requested:
        note.append(f"short input: {n} of {requested} rows")
    if n_out != n:
        note.append(f"{n - n_out} of {n} outputs NULL")
    gone = attribution.unavailable(m, skip=na)
    if gone:
        note.append("unavailable: " + ",".join(gone))
    return _row(
        {**base, "rows": min(int(n), requested)},
        note="; ".join(note),
        status="ok",
        timing=timing,
        fp=str(fp),
        m=m,
    )


def _guard_sent(rows: List[ResultRow]) -> List[ResultRow]:
    """Keep ser_sent_bytes only if it grows with the row count across the scales.

    Spark's "data sent to Python workers" does not count numeric Arrow input (it stays flat from
    100k to 1.6M rows of DOUBLEs while the data returned grows 16x), so a figure that does not
    scale is not the serialization cost. One scale cannot show it either.
    """
    ok = [r for r in rows if r.status == "ok" and r.ser_sent_bytes is not None]
    if not ok:
        return rows
    points = sorted((r.rows, r.ser_sent_bytes) for r in ok)
    why = "one scale: linearity not checked" if len(points) < 2 else ""
    for (r0, s0), (r1, s1) in zip(points, points[1:]):
        if not 0.5 * (r1 / r0) <= s1 / s0 <= 2 * (r1 / r0):
            why = "Spark under-counts numeric pandas input"
    if not why:
        return rows
    drop = {id(r) for r in ok}
    return [
        (
            replace(
                r, ser_sent_bytes=None, note=f"{r.note}; ser_sent_bytes NULL: {why}"
            )
            if id(r) in drop
            else r
        )
        for r in rows
    ]


def _opt(v, cast):
    return None if v is None else cast(v)


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
        ser_sent_bytes=_opt(m.get("ser_sent_bytes"), int),
        ser_recv_bytes=_opt(m.get("ser_recv_bytes"), int),
        py_eval_s=_opt(m.get("py_eval_s"), float),
        py_eval_frac=_opt(m.get("py_eval_frac"), float),
        shuffle_bytes=_opt(m.get("shuffle_bytes"), int),
        spill_bytes=_opt(m.get("spill_bytes"), int),
        peak_exec_mem_bytes=_opt(m.get("peak_exec_mem_bytes"), int),
        gc_s=_opt(m.get("gc_s"), float),
        py_worker_rss_mb=_opt(m.get("py_worker_rss_mb"), float),
        failure_mode=failure,
    )


def summarize(rows: List[ResultRow]) -> str:
    """A markdown table of a levels run: one line per form and scale, the attribution split beside the timing.

    A NULL metric prints as "-". ``floor_median_s`` is the median of the same function's ``floor``
    form (the Arrow UDF shell with a constant body) at the same row count, beside each other
    variant: the share of a variant's time that a faster kernel cannot win back.
    """
    floor = {
        (r.fn, r.level, r.rows): r.iter_median_s
        for r in rows
        if r.form == "floor" and r.status == "ok"
    }

    def f(v, fmt):
        return "-" if v is None else fmt(v)

    def mib(b):
        return f(b, lambda x: f"{x / 2**20:.1f}")

    head = [
        "fn",
        "level",
        "form",
        "compute",
        "rows",
        "median_s",
        "floor_median_s",
        "rows/s",
        "sent_MiB",
        "recv_MiB",
        "py_eval_s",
        "py_frac",
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
            (
                f(floor.get((r.fn, r.level, r.rows)), lambda x: f"{x:.3f}")
                if r.form != "floor"
                else "-"
            ),
            f"{r.throughput_rows_s:.0f}",
            mib(r.ser_sent_bytes),
            mib(r.ser_recv_bytes),
            f(r.py_eval_s, lambda x: f"{x:.3f}"),
            f(r.py_eval_frac, lambda x: f"{x:.2f}"),
            mib(r.shuffle_bytes),
            mib(r.spill_bytes),
            mib(r.peak_exec_mem_bytes),
            f(r.gc_s, lambda x: f"{x:.3f}"),
            f(r.py_worker_rss_mb, lambda x: f"{x:.0f}"),
            r.status,
            (r.failure_mode or r.note).replace("|", "/"),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"
