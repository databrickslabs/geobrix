"""Perf matrix: one Form = (function, level, shape) timed on one input at 1x/4x/16x.

Every set of functions (S, G, A, H) appends its forms to ``levels_forms.FORMS``; ``run_mvp`` times each one at the
levels the compute offers and records the attribution split (``attribution.py``) in the same
``ResultRow`` the other bench legs write.
"""

from __future__ import annotations

import dataclasses
import hashlib
import re
import statistics
import threading
import time
from dataclasses import dataclass, replace
from typing import Callable, Dict, List, Optional, Set, Tuple

from databricks.labs.gbx.bench import attribution
from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.runner import capture_env, p90_index, time_iters

_L3_VARIANTS = (
    "c0",
    "c1",
    "d",
    "engine",
    "engine-c0",
    "engine-c1",
)  # any other variant is a label: register picks the level's default
_PYTHON_LEVELS = (
    "L3",
    "DF",
)  # levels whose body runs in Spark's Python workers
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
    # A form that is a memory or scale probe BY DESIGN (it collects a whole partition, or runs on the big tiles):
    # the local preflight checks statements, it does not run these.
    memory_probe: bool = False
    # A form that runs only when `--levels-forms` names it (the one-worker readdir probes): the default run of a set
    # skips it, the local preflight judges it only when it is selected.
    opt_in: bool = False
    # A one-row SELECT over {src} answering (n, fp, n_out) like `sql`, but with `fp` a hash of the decoded VALUES of the
    # output (`value_fp`), not of its encoding. It runs once, after the timed iterations and outside them, and its
    # fingerprint replaces the length one in the row (`v1:<n>`; the length one moves to the note as `len_fp`).
    vsql: str = ""


def detect_compute(spark) -> str:
    from databricks.labs.gbx.manager.capabilities import is_connect, is_serverless

    if is_serverless():
        return "serverless"
    return "standard" if is_connect(spark) else "dedicated"


def ladder(mvp: str, scales=None) -> tuple:
    """The multiples to time: the ones given, else the set's own ladder (IO has one), else 1 / 4 / 16. "Not given" is
    None, so an explicit 1 / 4 / 16 is kept."""
    from databricks.labs.gbx.bench import levels_forms as lf

    return lf.SCALES.get(mvp, (1, 4, 16)) if scales is None else tuple(scales)


# Fail fast (run_mvp's default): a rung that cannot finish inside its bar is not waited for.
BAR_FACTOR = 1.5  # the ladder's linearity gate: a rung may cost this much more than linear in the scale
RUNG_CAP_S = (
    1200.0  # the most one rung (setup + warmup + measured iterations) may take: 20 min
)
SETUP_CAP_S = 600.0  # the most a rung's setup may take at 1x (times the scale for later rungs, held to the rung cap)
DEADLINE_FLOOR_S = 120.0  # a rung after the first is never judged by less than this
INTERRUPT_REPEAT_S = 5.0  # an interrupt is sent again this often while the rung is still past its deadline
ESCALATE_AFTER_S = 60.0  # past the first interrupt and still running this long: cancel every job (the next step)
WARMUP_SKIP_S = 120.0  # a rung whose projected run is longer than this skips its warmup (0: always warm)
WATCH_POLL_S = 1.0
WATCH_JOIN_S = 5.0  # how long a rung waits for its watchdog thread to end

OVER_BAR = "over-bar"  # a rung the deadline stopped (its row carries what was captured)
SKIPPED = "skipped"  # a rung not run: projected past the cap, or an earlier rung of the form was stopped


class _RungCancelled(Exception):
    """Raised on the work thread when the rung is past its deadline, at the next point the harness can stop."""


@dataclass
class _Hist:
    """A form's last finished rung: what its next one is projected from."""

    scale: int
    setup_s: float
    exec_s: float  # warmup + measured iterations

    @property
    def wall_s(self) -> float:
        return self.setup_s + self.exec_s


@dataclass
class _Rung:
    """One (form, scale) run under a deadline: the limits, and what the work thread has done so far (the watchdog
    thread reads it; the over-bar row is built from it).

    The deadline is the rung's whole wall (`rung_cap_s` for a form's first rung, else its projection held to a floor).
    Setup is judged on its own, by `setup_cap_s` (a rung whose input build is legitimately long is not an over-bar rung);
    once the setup is done the iterations may run to the deadline or `exec_budget_s` after the setup, whichever is later.
    """

    scale: int
    clock: Callable[[], float]
    deadline_s: float
    setup_cap_s: float
    exec_budget_s: Optional[float] = None
    projected_s: Optional[float] = None
    projected_from: int = 0
    started: float = 0.0
    setup_end: Optional[float] = None
    ended: Optional[float] = None
    phase: str = "setup"
    warm: List[float] = dataclasses.field(default_factory=list)
    meas: List[float] = dataclasses.field(default_factory=list)
    cancelled: str = ""  # "" | "over-bar" | "setup-bound"
    cancelled_at: float = 0.0
    interrupt_note: str = ""
    rearm: Optional[Callable[[], None]] = (
        None  # puts the rung's job group / tag back on the work thread
    )
    abandoned: bool = (
        False  # the work did not return after the escalation: the harness moved on without it
    )
    warmup: Optional[int] = (
        None  # the warmup iterations this rung runs (None: the run's); 0 when it runs cold
    )
    cold_note: str = ""
    early_why: str = ""
    holder: Dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        self.started = self.clock()

    def begin_exec(self) -> None:
        if self.setup_end is None:
            self.setup_end = self.clock()

    def expired(self, now: float) -> str:
        """ "" while the rung is inside its limits (or its iterations are over), else which one it passed."""
        if self.ended is not None:
            return ""  # the measurement is done: what runs now is the post-processing of a finished rung
        if self.setup_end is None:
            return "setup-bound" if now - self.started > self.setup_cap_s else ""
        limit = self.started + self.deadline_s
        if self.exec_budget_s is not None:
            limit = max(limit, self.setup_end + self.exec_budget_s)
        return OVER_BAR if now > limit else ""

    def wall(self) -> Tuple[float, float]:
        """(setup, run) seconds on the run's clock: the run is warmup + measured iterations and what follows them."""
        end = self.ended if self.ended is not None else self.clock()
        if self.setup_end is None:
            return end - self.started, 0.0
        return self.setup_end - self.started, end - self.setup_end

    def full_wall(self, measured: int) -> Tuple[float, float]:
        """`wall`, with the measured iterations an early exit skipped added at the pace of the ones that ran, so the
        next rung is projected from a whole rung and not from a truncated one."""
        setup, run = self.wall()
        if self.meas and len(self.meas) < measured:
            run += (measured - len(self.meas)) * (sum(self.meas) / len(self.meas))
        return setup, run

    def limit_text(self) -> str:
        if self.setup_end is None:
            return f"setup cap {self.setup_cap_s:.0f}s"
        return f"deadline {self.deadline_s:.0f}s"


class _Watchdog:
    """A driver-side thread that, once a rung is past its limit, flags it and interrupts the rung's Spark work (again
    every `repeat_s` while it still runs: an interrupt reaches only what is running at that moment). The work thread
    sees the flag at its next iteration boundary even where no interrupt is available.

    A rung that has not returned `escalate_s` after the first interrupt gets the wider one (`escalate`: every job of the
    session), and one that has not returned `escalate_s` after that is abandoned (`rung.abandoned`): the run moves on
    and leaves its work behind. Each step is written on the rung for the row.
    """

    def __init__(
        self,
        rung: _Rung,
        interrupt,
        *,
        poll=WATCH_POLL_S,
        repeat=INTERRUPT_REPEAT_S,
        escalate_s=ESCALATE_AFTER_S,
        escalate=None,
        say=None,
    ):
        self.rung, self.interrupt, self.poll, self.repeat = (
            rung,
            interrupt,
            poll,
            repeat,
        )
        self.escalate_s, self.escalate = escalate_s, escalate
        self.say = say or (lambda what, tail: None)
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name="gbx-rung-watchdog", daemon=True
        )

    def start(self) -> "_Watchdog":
        self._thread.start()
        return self

    def stop(self) -> bool:
        """End the thread; False (and a note on the rung) when it is still inside an interrupt call after the join."""
        self._stop.set()
        self._thread.join(timeout=WATCH_JOIN_S)
        if self._thread.is_alive():
            self.rung.interrupt_note += "; the watchdog thread was still inside an interrupt call when the rung ended (the call blocks)"
            return False
        return True

    def _loop(self) -> None:
        r = self.rung
        last_fire, escalated = None, False
        while not self._stop.wait(self.poll):
            now = r.clock()
            if r.ended is not None:
                continue  # the iterations are over: there is nothing left to stop
            if not r.cancelled:
                why = r.expired(now)
                if not why:
                    continue
                if self.interrupt is None:
                    # said before the flag: the work thread builds its row the moment it sees the flag
                    r.interrupt_note = "no interrupt API on this session: stopped at the next iteration boundary"
                r.cancelled, r.cancelled_at = why, now
                self.say(
                    "INTERRUPT",
                    f"{why}: elapsed {now - r.started:.0f}s past {r.limit_text()} during {r.phase}",
                )
            if self.interrupt is not None and (
                last_fire is None or now - last_fire >= self.repeat
            ):
                last_fire = now
                self._fire()
            since = now - r.cancelled_at
            if not escalated and since >= self.escalate_s:
                escalated = True
                self._escalate(since)
            elif escalated and since >= 2 * self.escalate_s and not r.abandoned:
                self._abandon(since)

    def _note(self, text: str) -> None:
        if (
            not self._stop.is_set()
        ):  # a step that lands after the rung ended writes nothing on it
            self.rung.interrupt_note += "; " + text

    def _escalate(self, since: float) -> None:
        """Past the window: the wider interrupt, once."""
        if self.escalate is None:
            self._note(
                f"still running {since:.0f}s after the interrupt and no wider interrupt exists on this session"
            )
            return
        try:
            what = self.escalate()
            self._note(
                f"escalated at +{since:.0f}s: cancelled {what or 'the wider set'}"
            )
        except (
            Exception
        ) as exc:  # noqa: BLE001 - recorded; the next window abandons the rung
            self._note(
                f"escalation failed at +{since:.0f}s ({type(exc).__name__}: {str(exc)[:120]})"
            )
        self.say(
            "INTERRUPT",
            f"still running {since:.0f}s after the interrupt: escalated to the wider cancel",
        )

    def _abandon(self, since: float) -> None:
        self._note(
            f"unresponsive after escalation: still running {since:.0f}s after the first interrupt; the rung is "
            "abandoned and its thread left to the cluster (later forms may share its executors)"
        )
        self.rung.abandoned = True
        self.say(
            "INTERRUPT",
            "unresponsive after escalation: the rung is abandoned and the run moves on",
        )

    def _fire(self) -> None:
        if self._stop.is_set():
            return  # the rung has ended: its tag is spent
        try:
            self.interrupt()
        except (
            Exception
        ) as exc:  # noqa: BLE001 - recorded; the launcher's watchdog stays the last resort
            if (
                not self._stop.is_set()
            ):  # a call that returns after the rung ended writes nothing on it
                self.rung.interrupt_note = (
                    f"interrupt failed ({type(exc).__name__}: {str(exc)[:160]}): "
                    "the launcher's watchdog is the fallback"
                )


_JOB_GROUP_PROPS = (
    "spark.jobGroup.id",
    "spark.job.description",
    "spark.job.interruptOnCancel",
)


def _classic_context(spark):
    """The SparkContext of a classic session (None on Spark Connect and Serverless, which forbid it, and for a session
    that has none)."""
    from databricks.labs.gbx.manager.capabilities import is_connect, is_serverless

    if is_serverless() or is_connect(spark):
        return None
    return getattr(spark, "sparkContext", None)


def _interrupt_api(spark, tag: str):
    """`(arm, interrupt, disarm)` for one rung's Spark work, or None where nothing can be interrupted.

    A classic session (a dedicated cluster, a local one) groups the rung's jobs (`setJobGroup`, interrupt on cancel)
    and cancels the group: Spark 4's classic `interruptTag` is accepted but cancels nothing (measured with a local
    Spark 4.0.0: the tagged job ran to its end, the grouped one stopped within the second). Spark Connect (standard
    access, Serverless) tags the queries a thread starts (`addTag`) and cancels them by tag (`interruptTag`), and
    Serverless forbids `sparkContext`, so it is never touched there. `arm` and `disarm` run on the work thread (the
    group and the tags are per thread); `interrupt` on the watchdog's.
    """
    sc = _classic_context(spark)
    if sc is not None:
        saved: Dict[str, Optional[str]] = {}

        def arm():
            # the notebook cell has a job group of its own (the UI's Cancel uses it): keep it for the disarm. Armed
            # again before every iteration (the group is per thread and anything that runs between can clear it), the
            # first call's is the one to put back.
            if not saved:
                for key in _JOB_GROUP_PROPS:
                    saved[key] = sc.getLocalProperty(key)
            sc.setJobGroup(tag, tag, True)

        def disarm():
            for key, val in saved.items():
                sc.setLocalProperty(key, val)

        return arm, lambda: sc.cancelJobGroup(tag), disarm
    if hasattr(spark, "addTag") and hasattr(spark, "interruptTag"):
        remove = getattr(spark, "removeTag", None)
        return (
            lambda: spark.addTag(tag),
            lambda: spark.interruptTag(tag),
            (lambda: remove(tag)) if remove else (lambda: None),
        )
    return None


_RUN_GROUPS: Set[str] = (
    set()
)  # the job groups this process's bench rungs have used (their tags)


def run_groups() -> Set[str]:
    """Every job group the bench has used in this process: the rungs' own, and the worker peak's."""
    return set(_RUN_GROUPS) | set(attribution.RUN_GROUPS)


def _escalate_api(spark, exclusive: bool = False):
    """The wider interrupt, or None: a callable that returns what it cancelled (for the row).

    Classic: the cancel of every job group this run has used; `cancelAllJobs` (every job of the SparkContext, other
    parts' and notebooks' included) only when the cluster is this run's alone (`exclusive`: a pool lease or a job
    cluster). Spark Connect: `interruptAll`, which is this session's operations only.
    """
    sc = _classic_context(spark)
    if sc is not None:
        if exclusive:

            def everything():
                sc.cancelAllJobs()
                return "every job of the cluster (it is this run's alone)"

            return everything

        def ours():
            groups = sorted(run_groups())
            for g in groups:
                sc.cancelJobGroup(g)
            return f"every job group of this run ({len(groups)}); other jobs on the cluster were left alone"

        return ours
    interrupt_all = getattr(spark, "interruptAll", None)
    if interrupt_all is None:
        return None

    def session():
        interrupt_all()
        return "every operation of this session"

    return session


def _tag(run_id: str, label: str, scale: int) -> str:
    """The rung's tag (job group): the label is cut when it is long, the scale suffix never, so two rungs of a form
    keep two tags; a cut label keeps a hash of the whole so two long labels with one start keep two as well.
    """
    head = re.sub(r"[^\w.-]", "_", f"gbx-bench-{run_id}-{label}")
    suffix = f"-{scale}x"
    if len(head) + len(suffix) <= 120:
        return head + suffix
    digest = hashlib.sha1(head.encode()).hexdigest()[:8]
    return head[: 120 - len(suffix) - len(digest) - 1] + "-" + digest + suffix


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
    scales=None,
    where: str = "cluster",
    only_levels: Optional[Tuple[str, ...]] = None,
    base_rows: Optional[int] = None,
    on_form: Optional[Callable[[List[ResultRow]], None]] = None,
    heartbeat=None,
    report_tasks: bool = False,
    inject_stall: float = 0.0,
    form_cap_s: Optional[float] = None,
    fail_fast: bool = True,
    bar_factor: float = BAR_FACTOR,
    rung_cap_s: float = RUNG_CAP_S,
    setup_cap_s: float = SETUP_CAP_S,
    deadline_floor_s: float = DEADLINE_FLOOR_S,
    clock: Callable[[], float] = time.monotonic,
    watch_poll_s: float = WATCH_POLL_S,
    warmup_skip_s: float = WARMUP_SKIP_S,
    escalate_s: float = ESCALATE_AFTER_S,
    run_mode: str = "isolate",
    concurrent: int = 1,
    cluster_id: str = "",
    part: str = "",
    exclusive: bool = False,
) -> List[ResultRow]:
    """Time every form of `mvp` that this compute offers, at each scale; one row per form and scale.

    `schema` (``catalog.schema``) caches the inputs as Delta tables; None keeps them as temp
    views. `uc` is the ``catalog.schema`` holding the L4 functions (L4 forms are skipped, with a
    row saying so, when it is not given). A form that fails is an ``error`` row, not an exception.

    The timing is the median over the measured iterations; the attribution split is the plan of
    the LAST measured iteration (one sample), and the worker memory peak covers warmup and all
    measured iterations. A metric with no value is NULL, with the reason in the row's `note`.

    Liveness: `on_form` is called with one row as soon as that form's scale has run (the caller sinks it there, so a
    hung later scale or form loses nothing; a row's `ser_sent_bytes` is guarded by the scales run so far, the returned
    rows by all of them), and `heartbeat` (``liveness.Heartbeat``)
    is told which form, at which scale, is in progress (one begin/end per scale) together with the cap that
    applies to it: `form_cap_s` (the cap of a 1x form) times the scale, so a long ladder is not judged by
    the 1x cap. `report_tasks` records, per row, the number of Spark tasks the
    form's input scan runs (``input_partitions``): a tile form over a one-file table runs on one task.
    `inject_stall` (seconds, test only) adds a form after the first that sleeps on the driver, to prove the
    launcher's watchdog.

    Fail fast (`fail_fast`, on by default; off runs each form through all its scales, as before, with nothing stopped
    early): the rungs run breadth first (every form at a scale before any at the next), so a slow form no longer holds
    back the others' signal, and a rung is not waited for past its bar.
      * Before a rung, its wall is projected from the form's previous rung (setup + warmup + iterations, times the scale
        ratio, times `bar_factor`). Past `rung_cap_s` it does not run: a ``skipped`` row says so, as do the form's later rungs.
      * A rung has a deadline (`rung_cap_s` for a form's first, else its projection, never under `deadline_floor_s` or
        over the cap). Past it, a watchdog interrupts the rung's Spark queries (tagged, `interruptTag`; a job group on old
        classic) and the rung's row is ``over-bar``: elapsed, the phase it was in, the iterations that finished, and the
        plan metrics of the last finished one. The form's later rungs are skipped; the next form runs.
      * If the first measured iteration alone takes more than its share of the deadline (deadline / measured iterations),
        the rung stops after it: an ``ok`` row of one iteration, with a note.
      * Setup has its own cap (`setup_cap_s` at 1x, times the scale, held to the rung cap); a setup past it is
        interrupted the same way, and the row says ``setup-bound``.
      * A rung that does not return `escalate_s` after the first interrupt gets the wider one (every job of the session),
        and `escalate_s` after that is abandoned: its row says ``unresponsive after escalation``, its form is dead, the
        next form runs (its stuck worker is left to the cluster). The rung runs on a thread of its own for this.
      * A rung whose projected run (the previous rung's warmup + iterations x the scale ratio) is over `warmup_skip_s`
        runs its measured iterations cold (``cold: warmup skipped``); a form's first rung keeps its warmup. 0 keeps them all.
      * Within a scale the forms are ordered by input (stable otherwise) and a rung's cached input is kept for the next
        form of the same input and released when a different one is next, so one input is held at a time; the later
        forms say ``input reused`` and their setup is the lookup.
    The launcher's whole-run watchdog stays the last resort where an interrupt cannot reach the work.

    Every row records how the run was set up: `run_mode` ("isolate": timing-grade, one run on its compute; "explore":
    parts may share a cluster, so the numbers find what is slow and are not compared with isolated ones), `concurrent`
    (the most parts on the cluster at once), `cluster_id` and `part` (which part of a split run). `exclusive` says the
    cluster is this run's alone (a pool lease, a job cluster): only then may an unresponsive rung's escalation cancel every
    job on it; otherwise it cancels the job groups this run used.
    """
    from databricks.labs.gbx.bench import levels_forms as lf

    if mvp not in lf.FORMS:
        raise ValueError(
            f"no forms for MVP {mvp!r} yet (have: {', '.join(sorted(lf.FORMS))})"
        )
    scales = ladder(mvp, scales)
    ctx = _Ctx(
        spark,
        warmup,
        measured,
        corpus,
        schema,
        uc,
        {},
        report_tasks=report_tasks,
        heartbeat=heartbeat,
    )
    compute = detect_compute(spark)
    env = capture_env(where)
    forms = list(lf.FORMS[mvp])
    if inject_stall:
        lf.STALL_SECONDS = float(inject_stall)
        forms.insert(min(1, len(forms)), lf.STALL_FORM)
    todo = [
        f
        for f in forms
        if compute in f.computes and not (only_levels and f.level not in only_levels)
    ]
    labels = [f"{f.fn}:{f.level}:{f.variant}" for f in todo]
    bar = _Bar(
        fail_fast,
        bar_factor,
        float(rung_cap_s),
        float(setup_cap_s),
        deadline_floor_s,
        clock,
        watch_poll_s,
        warmup_skip_s,
        escalate_s,
        exclusive,
    )
    by_form: Dict[int, List[ResultRow]] = {i: [] for i in range(len(todo))}
    hist: Dict[int, _Hist] = {}
    dead: Dict[int, str] = {}  # form -> why its later rungs are not run
    if (
        fail_fast
    ):  # breadth first: every form at a scale, then the next scale for the forms that went on
        first_seen: Dict[str, int] = {}
        for f in todo:
            first_seen.setdefault(f.input, len(first_seen))
        # the forms of one input are neighbours (stable otherwise), so the input is built once and used by them all
        by_input = sorted(range(len(todo)), key=lambda k: first_seen[todo[k].input])
        order = [(i, scale) for scale in scales for i in by_input]
    else:
        order = [(i, scale) for i in range(len(todo)) for scale in scales]
    # A cached input lives from the rung that built it to the next rung of a different input (a replicated input is
    # keyed by its row count, so the forms of one input at one scale share it), and a rung that did not finish ok
    # releases it at once: one input is held at a time, as the cache's memory contract needs.
    for i, scale in order:
        form, label = todo[i], labels[i]
        requested = (base_rows or lf.BASE_ROWS[form.input]) * scale
        input_key = (form.input, requested)
        if ctx.held is not None and ctx.held != input_key:
            _release(lf, spark)
            ctx.held = None
        ctx.input_reused = ctx.last_input == input_key
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
            run_mode=run_mode,
            concurrent=int(concurrent),
            cluster_id=cluster_id,
            part=part,
            **env,
        )
        rows, prev = by_form[i], hist.get(i)
        was_dead = i in dead
        skip_note = _skip_note(bar, prev, dead, i, scale)
        if skip_note:
            if heartbeat and not was_dead and hasattr(heartbeat, "form_skipped"):
                heartbeat.form_skipped()  # the form is over as of this skip (a stopped rung counted it at its end)
            rows.append(_row(base, status=SKIPPED, note=skip_note))
            _say_skip(mvp, i + 1, len(todo), label, scale, requested, skip_note)
            if on_form:
                on_form([_guard_sent(rows)[-1]])
            continue
        rung = _new_rung(bar, prev, scale, warmup)
        ctx.rung = rung
        if heartbeat:
            _begin(
                heartbeat,
                mvp,
                i + 1,
                len(todo),
                label,
                scale,
                requested,
                form_cap_s,
                rung,
            )
        alive_before = [lbl for lbl, th in ctx.stale if th.is_alive()]
        row = _execute(
            ctx,
            form,
            base,
            requested,
            rung,
            _tag(run_id, label, scale),
            bar,
            keep_input=fail_fast,
        )
        if alive_before and row.status != SKIPPED:
            # an abandoned rung's work is still on the cluster: this timing was taken beside it
            row = replace(
                row,
                note=f"{row.note}; ran while an abandoned rung was still running ({', '.join(alive_before)})",
            )
        ctx.held = input_key if fail_fast and row.status == "ok" else None
        ctx.last_input = input_key if row.status == "ok" else None
        rows.append(row)
        if rung is not None:
            if row.status == "ok":
                hist[i] = _Hist(
                    scale, *rung.full_wall(measured)
                )  # what a whole rung took on the run's clock, not what its row sums
            elif row.status == OVER_BAR:
                dead[i] = f"{rung.cancelled} at {scale}x"
        if heartbeat:
            measured_ok = (
                row.status == "ok"
            )  # an error row's 0.0 is a default, not a measurement
            heartbeat.end(
                form_done=scale == scales[-1] or i in dead,
                median_s=row.iter_median_s if measured_ok else None,
                status=row.status,
                setup_s=row.setup_s if measured_ok else None,
                warmup_s=row.warmup_s if measured_ok else None,
            )
        if on_form:
            # sunk now, not after the last scale: a later scale that hangs or is cancelled loses nothing. The
            # linearity guard needs two scales, so this row is guarded by the scales run so far.
            on_form([_guard_sent(rows)[-1]])
    if ctx.held is not None or ctx.stale:
        _release(lf, spark)  # what an abandoned rung held goes with the end of the run
    out: List[ResultRow] = []
    for i in range(len(todo)):
        out += _guard_sent(by_form[i])
    return out


@dataclass(frozen=True)
class _Bar:
    """run_mvp's fail-fast knobs, as one value."""

    fail_fast: bool
    factor: float
    rung_cap_s: float
    setup_cap_s: float
    floor_s: float
    clock: Callable[[], float]
    poll_s: float
    warmup_skip_s: float = WARMUP_SKIP_S
    escalate_s: float = ESCALATE_AFTER_S
    exclusive: bool = False


def _skip_note(
    bar: _Bar, prev: Optional[_Hist], dead: Dict[int, str], i: int, scale: int
) -> str:
    """Why the rung is not run ("" when it is): the form's earlier rung was stopped, or this one is projected past the
    cap. Records the form as dead for the rungs after it."""
    if not bar.fail_fast:
        return ""
    if i in dead:
        return f"skipped: {dead[i]}"
    if prev is None:
        return ""
    ratio = scale / prev.scale
    projected = prev.wall_s * ratio * bar.factor
    if projected <= bar.rung_cap_s:
        return ""
    dead[i] = f"projected past the cap at {scale}x"
    return (
        f"projected {projected:.0f}s > cap {bar.rung_cap_s:.0f}s from {prev.scale}x "
        f"(setup {prev.setup_s:.0f}s + run {prev.exec_s:.0f}s, x{ratio:g}, bar {bar.factor:g})"
    )


def _new_rung(
    bar: _Bar, prev: Optional[_Hist], scale: int, warmup: int = 0
) -> Optional[_Rung]:
    """The limits of one rung (None without fail-fast): a form's first gets the rung cap and its warmup; a later one its
    projection from the rung before it, held between the floor and the cap, and no warmup when its projected run is
    longer than `warmup_skip_s` (a warmup that long is a second measured iteration)."""
    if not bar.fail_fast:
        return None
    setup_cap = min(bar.setup_cap_s * scale, bar.rung_cap_s)
    if prev is None:
        return _Rung(
            scale=scale,
            clock=bar.clock,
            deadline_s=bar.rung_cap_s,
            setup_cap_s=setup_cap,
        )
    ratio = scale / prev.scale
    projected = prev.wall_s * ratio * bar.factor
    rung = _Rung(
        scale=scale,
        clock=bar.clock,
        deadline_s=min(bar.rung_cap_s, max(projected, bar.floor_s)),
        setup_cap_s=setup_cap,
        exec_budget_s=min(
            bar.rung_cap_s, max(prev.exec_s * ratio * bar.factor, bar.floor_s)
        ),
        projected_s=projected,
        projected_from=prev.scale,
    )
    run = prev.exec_s * ratio
    if warmup > 0 and bar.warmup_skip_s > 0 and run > bar.warmup_skip_s:
        rung.warmup = 0
        rung.cold_note = f"cold: warmup skipped (projected {run:.0f}s)"
    return rung


def _begin(
    heartbeat, mvp, index, total, label, scale, requested, form_cap_s, rung
) -> None:
    kw = {}
    if rung is not None:  # a rung without a bar (fail_fast off) begins as it always did
        kw = dict(deadline_s=round(rung.deadline_s, 1))
        if rung.projected_s is not None:
            kw["projected_s"] = round(rung.projected_s, 1)
    heartbeat.begin(
        f"{label} @{scale}x",
        index,
        total,
        scale=scale,
        cap_s=form_cap_s * scale if form_cap_s else None,
        rows=requested,
        name=label,
        set_name=mvp,
        **kw,
    )


def _attempt(
    ctx: "_Ctx", form: Form, base: Dict, requested: int, rung: Optional[_Rung]
) -> ResultRow:
    """`_run_one`, with its failure as an ``error`` row and a stopped rung as an ``over-bar`` one."""
    try:
        row = _run_one(ctx, form, base, requested)
        if rung is not None:
            if rung.ended is None:
                rung.ended = rung.clock()
            if rung.cancelled:
                # flagged during an iteration that then finished (the interrupt found nothing running): the rung went
                # past its bar, so it is over-bar and its form is dead, whatever the iteration returned
                row = _over_bar_row(base, rung, form)
        return row
    except Exception as exc:  # noqa: BLE001 - a failure is a measured outcome
        if rung is not None and rung.cancelled:
            return _over_bar_row(base, rung, form)
        return _row(base, status="error", failure=failure_text(exc))


def _execute(
    ctx: "_Ctx",
    form: Form,
    base: Dict,
    requested: int,
    rung: Optional[_Rung],
    tag: str,
    bar: _Bar,
    keep_input: bool = False,
) -> ResultRow:
    """Run one rung and return its row: a failure is an ``error`` row, a stopped rung an ``over-bar`` one. Under
    fail-fast the rung runs on a thread of its own with its Spark work tagged (grouped), and a watchdog thread watches
    its limits until it ends: a rung the watchdog abandons (no return after the interrupt and the wider one) is written
    as ``over-bar`` and left running, so one stuck worker does not end the run. `keep_input` leaves a successful rung's
    cached inputs for the next form of the same input.
    """
    from databricks.labs.gbx.bench import levels_forms as lf

    spark = ctx.spark
    stuck, row, stale = False, None, None
    # A supervised rung works on a copy of the run's state: a rung the run gives up on keeps running on its copy, and what
    # it does late (a registration, a setup) cannot reach the forms that follow; a rung that ends merges its state back.
    rctx = ctx
    if rung is not None:
        rctx = dataclasses.replace(
            ctx, registered=dict(ctx.registered), setups=set(ctx.setups), rung=rung
        )
    try:
        if rung is None:
            row = _attempt(ctx, form, base, requested, None)
        else:
            row, stuck, stale = _supervised(rctx, form, base, requested, rung, tag, bar)
            if stale is None:
                ctx.registered.update(rctx.registered)
                ctx.setups |= rctx.setups
                ctx.jvm_why = rctx.jvm_why if rctx.jvm_why is not None else ctx.jvm_why
            else:
                ctx.stale.append(
                    (f"{form.fn}:{form.level}:{form.variant}@{rung.scale}x", stale)
                )
    finally:
        ctx.rung = None
        # an abandoned rung may still be using its cached inputs: they are released at the end of the run, not under it
        if stale is None and (row is None or row.status != "ok" or not keep_input):
            _release(lf, spark)  # the rung is over: its cached inputs go
    if stuck and rung is not None:
        row = replace(row, note=f"{row.note}; {rung.interrupt_note.lstrip('; ')}")
    return row


def _release(lf, spark) -> None:
    try:
        lf.release_cached_inputs(spark)
    except (
        Exception
    ) as exc:  # noqa: BLE001 - an interrupted rung may leave a cache that will not unpersist
        print(
            f"[LEVELS input] releasing the cached inputs failed ({type(exc).__name__}: {str(exc)[:160]})",
            flush=True,
        )


def _wait_for_rung(worker, done, rung, api, poll_s, set_thread) -> None:
    """Start the rung's thread and wait for it, until it ends or the watchdog abandons it. A notebook cancel
    (KeyboardInterrupt) reaches this thread and not the rung's: it is passed on to the rung's work.
    """
    try:
        worker.start()
        if set_thread is not None:
            set_thread(
                worker.ident
            )  # the stack in the beat is the rung's, not the waiting thread's
        while not done.wait(poll_s):
            if rung.abandoned:
                break
    except BaseException:
        rung.cancelled = rung.cancelled or "cancelled"
        if api is not None:
            try:
                api[1]()
            except Exception:  # noqa: BLE001 - the run ends either way
                pass
        raise
    finally:
        if set_thread is not None:
            set_thread(None)


def _supervised(
    ctx: "_Ctx",
    form: Form,
    base: Dict,
    requested: int,
    rung: _Rung,
    tag: str,
    bar: _Bar,
) -> Tuple[ResultRow, bool, Optional[threading.Thread]]:
    """The rung on a thread of its own under a watchdog; `(row, watchdog_stuck, abandoned_thread)`. The thread arms the interrupt (the
    job group and the tags are per thread) and disarms it; the run waits for it, and moves on without it when the
    watchdog abandons the rung."""
    spark = ctx.spark
    api = None
    try:
        api = _interrupt_api(spark, tag)
    except Exception as exc:  # noqa: BLE001 - no interrupt: the rung still has its flag
        rung.interrupt_note = (
            f"interrupt unavailable ({type(exc).__name__}: {str(exc)[:120]})"
        )
    _RUN_GROUPS.add(tag)
    try:
        wider = _escalate_api(spark, bar.exclusive)
    except (
        Exception
    ):  # noqa: BLE001 - no wider interrupt: the rung is abandoned after the window
        wider = None
    say = getattr(ctx.heartbeat, "event", None) or (lambda what, tail: None)
    watch = _Watchdog(
        rung,
        api[1] if api else None,
        poll=bar.poll_s,
        escalate_s=bar.escalate_s,
        escalate=wider,
        say=say,
    ).start()
    box: Dict = {}
    done = threading.Event()

    def work() -> None:
        disarm = None
        try:
            if api is not None:
                try:
                    api[0]()
                    disarm = api[2]
                    rung.rearm = api[0]
                except Exception as exc:  # noqa: BLE001 - no tag, no interrupt
                    rung.interrupt_note = f"interrupt unavailable ({type(exc).__name__}: {str(exc)[:120]})"
            box["row"] = _attempt(ctx, form, base, requested, rung)
        except BaseException as exc:  # noqa: BLE001 - handed to the waiting thread
            box["exc"] = exc
        finally:
            if disarm is not None:
                try:
                    disarm()
                except Exception:  # noqa: BLE001 - the tag is spent either way
                    pass
            done.set()

    worker = threading.Thread(target=work, name="gbx-rung-worker", daemon=True)
    set_thread = getattr(ctx.heartbeat, "set_work_thread", None)
    _wait_for_rung(worker, done, rung, api, bar.poll_s, set_thread)
    stuck = not watch.stop()
    if not done.is_set():  # abandoned: the thread is left running
        rung.cancelled = rung.cancelled or OVER_BAR
        return _over_bar_row(base, rung, form), stuck, worker
    if "exc" in box:
        raise box["exc"]
    return box["row"], stuck, None


def _say_skip(mvp, index, total, label, scale, rows, note) -> None:
    try:
        print(
            f"[LEVELS {mvp}] form {index}/{total} {label} scale {scale}x rows {rows} SKIP {note}",
            flush=True,
        )
    except Exception:  # noqa: BLE001 - a closed stdout must not end the run
        pass


def _fmt_secs(xs: List[float]) -> str:
    return "[" + ", ".join(f"{x:.1f}" for x in xs) + "]"


def _over_bar_row(base: Dict, rung: _Rung, form: Form) -> ResultRow:
    """The row of a rung the watchdog stopped: where it was, what finished, and the plan metrics of the last finished
    query (a rung cut in setup has none)."""
    end = rung.clock()
    elapsed = end - rung.started
    setup_s = (rung.setup_end - rung.started) if rung.setup_end is not None else elapsed
    parts = [
        f"{rung.cancelled}: stopped at {elapsed:.0f}s past {rung.limit_text()}"
        + (
            f" (projected {rung.projected_s:.0f}s from {rung.projected_from}x)"
            if rung.projected_s
            else ""
        )
        + f" during {rung.phase}",
        f"setup {setup_s:.0f}s"
        + ("" if rung.setup_end is not None else " (not finished)"),
        f"warmup done {_fmt_secs(rung.warm)}",
        f"measured done {_fmt_secs(rung.meas)}",
    ]
    if rung.interrupt_note:
        parts.append(rung.interrupt_note.lstrip("; "))
    timing = {
        "warmup_iters": len(rung.warm),
        "measured_iters": len(rung.meas),
        "warmup_s": sum(rung.warm),
        "iter_total_wall_clock_ms": sum(rung.meas) * 1000.0,
    }
    if rung.meas:
        ms = sorted(x * 1000.0 for x in rung.meas)
        timing.update(
            iter_median_ms=statistics.median(ms), iter_min_ms=ms[0], iter_p90_ms=ms[p90_index(len(ms))],
            avg_wall_clock_ms=sum(ms) / len(ms),
        )  # fmt: skip
    m = _partial_metrics(rung, form)
    if m is None:
        parts.append("no plan metrics (no query finished)")
    return _row(
        base,
        status=OVER_BAR,
        note="; ".join(parts),
        timing=timing,
        m=m,
        setup_s=setup_s,
    )


def _partial_metrics(rung: _Rung, form: Form) -> Optional[Dict]:
    """The attribution split of the last query that finished before the stop, or None."""
    holder = rung.holder
    if "df" not in holder or "done_t0" not in holder:
        return None
    try:
        plan = attribution.from_plan(holder["df"], holder["done_t0"])
        m = {k: plan[k] for k in attribution.METRICS}
    except (
        Exception
    ):  # noqa: BLE001 - metrics are what is left of the rung, never the reason it has no row
        return None
    m["py_worker_rss_mb"] = None
    if form.level not in _PYTHON_LEVELS:
        m.update({k: None for k in _PYTHON_FIELDS})
    return m


_EXC_LINE = re.compile(r"^\s*[A-Za-z_][\w.]*(Error|Exception|Exit|Interrupt)\b")
# Spark's "Job aborted ... Lost task 3.3 in stage 7.0 (TID 9) (10.0.0.1 executor 4): java.lang.X: reason" is one line
# far longer than the head the row keeps; the executor's exception class and reason come after its last "): "
_EXECUTOR_CAUSE = re.compile(
    r"\)\s*:\s+((?:[\w$]+\.)+[\w$]*(?:Error|Exception)\b[^\n]*)"
)


_TRACEBACK = "Traceback (most recent call last):"
_FRAME_LINES = 12  # the last six frames (a "File" line and its code line each)
_FRAMES_BUDGET = 1100


def _worker_frames(lines: List[str]) -> str:
    """The last frames of a Python worker's traceback (` || tb: ...`), where the failing file:line is; "" when the
    error carries no traceback. The cause line alone ("IndexError: list index out of range") names no code.
    """
    start = max((i for i, ln in enumerate(lines) if _TRACEBACK in ln), default=None)
    if start is None:
        return ""
    frames = []
    for ln in lines[start + 1 :]:
        if _EXC_LINE.match(ln):
            break
        frames.append(ln[:200])
    if not frames:
        return ""
    return " || tb: " + " | ".join(frames[-_FRAME_LINES:])[-_FRAMES_BUDGET:]


def failure_text(exc: BaseException) -> str:
    """A failed form's `failure_mode`: the exception class, its first line, and the cause. A Python worker's error
    names the cause in its LAST exception line (after the worker traceback), which a plain prefix cuts off.
    """
    lines = [ln.strip() for ln in str(exc).splitlines() if ln.strip()]
    head = lines[0][:200] if lines else ""
    cause = next((ln for ln in reversed(lines) if _EXC_LINE.match(ln)), "")
    if not cause:
        on_executor = _EXECUTOR_CAUSE.search(str(exc))
        cause = on_executor.group(1).strip() if on_executor else ""
        # a wrapper exception ("Failed to materialize ... after 3 retries") hides the failure under "Caused by:"
        root = next((ln for ln in reversed(lines) if ln.startswith("Caused by:")), "")
        if cause and root:
            cause = f"{cause[:250]} {root[:250]}"
    out = f"{type(exc).__name__}: {head}"
    if cause and cause != head:
        return f"{out} ... {cause[:500]}{_worker_frames(lines)}"
    if len(lines) > 1:
        # no exception-class line to point at (a pip failure in a UC function environment is plain output): keep
        # the first lines, where the actionable one is, within the same budget
        return f"{out} | {' | '.join(ln[:200] for ln in lines[1:10])[:500]}"
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
    jvm_why: Optional[str] = (
        None  # why this session has no geobrix JAR ("" = it has one), once probed
    )
    report_tasks: bool = False
    heartbeat: object = None
    rung: Optional["_Rung"] = (
        None  # the running rung under fail-fast (None: nothing is stopped early)
    )
    held: Optional[Tuple[str, int]] = (
        None  # the (input, rows) whose cached frames are kept for the next form
    )
    last_input: Optional[Tuple[str, int]] = (
        None  # the (input, rows) of the last rung that finished ok
    )
    input_reused: bool = False  # the rung about to run has the input the last one used
    stale: list = dataclasses.field(
        default_factory=list
    )  # (label, thread) of rungs given up on that may still be running


_JVM_LEVELS = ("L1", "L2", "JVM-DF")  # forms whose body is the geobrix JAR


def _no_jar(ctx: "_Ctx") -> str:
    """Why this session has no geobrix JAR ("" when it has one); probed once per run."""
    if ctx.jvm_why is None:
        from databricks.labs.gbx.manager import capabilities

        caps = capabilities.detect(ctx.spark, need_jvm=True)
        ctx.jvm_why = "" if caps.jar is not None else caps.jar_reason
    return ctx.jvm_why


def _cannot_run_here(
    form: Form, uc: Optional[str], ctx: Optional["_Ctx"] = None
) -> Optional[str]:
    """Why this run cannot time the form (None when it can): an L4 form needs a UC schema, a JVM form
    the JAR, `d` the Rust build."""
    if ctx is not None and form.level in _JVM_LEVELS:
        why = _no_jar(ctx)
        if why:
            return f"no geobrix JAR on this session ({why})"
    if form.level == "L4" and not uc:
        return "no UC schema given (--levels-uc)"
    if form.level == "L3" and form.variant == "d":
        from databricks.labs.gbx import _rs

        why = _rs.unavailable_reason()
        return f"Rust kernels unavailable: {why}" if why else None
    return None


def _ensure_setup(ctx: _Ctx, form: Form) -> None:
    """Run the form's `setup` unless what it installs is already installed. A setup that installs a function says so
    (`setup.installs = {fn: (level, variant)}`) and is judged by what is installed now, so a later form that put another
    level there makes the next one install its own again; any other setup runs once per name.
    """
    if not form.setup:
        return
    from databricks.labs.gbx.manifest import resolve_ref

    setup = resolve_ref(form.setup)
    installs = getattr(setup, "installs", None)
    if installs is None:
        if form.setup not in ctx.setups:
            setup(ctx.spark)
            ctx.setups.add(form.setup)
    elif any(ctx.registered.get(fn) != want for fn, want in installs.items()):
        setup(ctx.spark)
        ctx.registered.update(installs)


def _register_form(ctx: _Ctx, form: Form) -> None:
    """Install the function the form times at the level it times it, unless it is installed so already."""
    from databricks.labs.gbx import register

    if not form.register:
        return
    variant = form.variant.split("@")[
        0
    ]  # "<variant>@<label>": the label only names the input
    want = (form.level, variant if variant in _L3_VARIANTS else "-")
    if ctx.registered.get(form.fn) != want:
        forced = {form.fn: want} if form.level in ("L1", "L2", "L3") else None
        register(ctx.spark, only=[form.fn], _levels=forced)
        ctx.registered[form.fn] = want


def _iteration(
    ctx: _Ctx, form: Form, src, rung: Optional["_Rung"], holder: Dict, warmup: int
):
    """The function `time_iters` calls: one warmup or measured iteration of the form over `src`. Under fail-fast it
    stops a rung the watchdog flagged, names the phase it is in, and tells the rung what finished and how long it took.
    """
    from databricks.labs.gbx.manifest import resolve_ref

    spark, uc = ctx.spark, ctx.uc
    calls = [0]

    def phase(text: str) -> None:
        if rung is not None:
            rung.phase = text
        if ctx.heartbeat is not None and hasattr(ctx.heartbeat, "set_phase"):
            ctx.heartbeat.set_phase(text)

    def body():
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

    def go():
        if rung is not None and rung.cancelled:
            raise _RungCancelled(rung.cancelled)
        if rung is not None and rung.rearm is not None:
            try:
                rung.rearm()
            except (
                Exception
            ):  # noqa: BLE001 - the group is then what the first arm left
                pass
        calls[0] += 1
        warming = calls[0] <= warmup
        phase(
            f"warmup {calls[0]}/{warmup}"
            if warming
            else f"measured {calls[0] - warmup}/{ctx.measured}"
        )
        t_iter = rung.clock() if rung is not None else 0.0
        if "first" not in holder and rung is not None:
            rung.begin_exec()  # the setup is over: from here the rung is judged by its deadline
        holder.setdefault(
            "first", time.perf_counter()
        )  # the first iteration, warmup or measured
        holder["t0"] = time.time()
        body()
        holder["done_t0"] = holder[
            "t0"
        ]  # the query that last finished (the plan lookup is by its start)
        if rung is not None:
            (rung.warm if warming else rung.meas).append(rung.clock() - t_iter)

    return go


def _run_one(ctx: _Ctx, form: Form, base: Dict, requested: int) -> ResultRow:
    from databricks.labs.gbx.bench import levels_forms as lf

    t_start = time.perf_counter()
    spark = ctx.spark
    rung = (
        ctx.rung
    )  # under fail-fast: the limits, and where the work thread reports what it has done. Read first: the run may
    # move on (and set the next rung) while this thread is still in its input build
    reused = ctx.input_reused
    gone = _cannot_run_here(form, ctx.uc, ctx)
    if gone:
        return _row(base, note=gone, status="na_by_design")
    lf._STAMP_CORPUS[0] = (
        ctx.corpus
    )  # an input's stamp holds the tiles of the corpus it reads
    src = lf.INPUTS[form.input](spark, requested, ctx.corpus, ctx.schema)
    if src is None:
        return _row(
            base, note="input unavailable (no tile corpus)", status="na_by_design"
        )
    scan_tasks = _scan_tasks(ctx, src)
    _register_form(ctx, form)
    _ensure_setup(ctx, form)
    holder: Dict = rung.holder if rung is not None else {}
    warmup = rung.warmup if rung is not None and rung.warmup is not None else ctx.warmup
    _go = _iteration(ctx, form, src, rung, holder, warmup)
    peak = attribution.WorkerPeak(spark) if form.level in _PYTHON_LEVELS else None
    if peak:
        peak.start()  # the memory peak is the shape's, not a worker's lifetime
    stop = _first_iteration_stop(rung, ctx.measured) if rung is not None else None
    timing = time_iters(_go, warmup, ctx.measured, stop=stop)
    if rung is not None:
        rung.ended = (
            rung.clock()
        )  # the iterations are over: the watchdog has nothing left to stop
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
    # the reading takes the DataFrame, whose plan decides an early stop
    rss = peak.read(holder.get("df")) if peak else None
    m = {k: plan[k] for k in attribution.METRICS}
    m["py_worker_rss_mb"] = rss
    note = [f"attr={plan['source']}"]
    if plan.get("stage_tasks"):
        note.append(f"stage_tasks={plan['stage_tasks']}")
    parts = attribution.shuffle_partitions(holder.get("df"))
    if parts:
        note.append(f"shuffle_parts={parts}")
    if reused:
        note.append("input reused")
    if rung is not None and rung.cold_note:
        note.append(rung.cold_note)
    if timing["measured_iters"] < ctx.measured:
        note.append(
            f"early exit: {timing['measured_iters']} of {ctx.measured} measured iterations ({rung.early_why})"
        )
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
    note += _tasks_note(scan_tasks)
    return _row(
        {**base, "rows": min(int(n), requested)},
        note="; ".join(note),
        status="ok",
        timing=timing,
        fp=fp,
        m=m,
        input_partitions=scan_tasks or 0,
        setup_s=holder["first"] - t_start,
    )


def _first_iteration_stop(rung: "_Rung", measured: int) -> Callable[[float], bool]:
    """The measured loop's early exit: after the first measured iteration if it alone took more than its share of the
    rung's budget (budget / measured iterations), and after any iteration if the rung has gone past its deadline.
    """
    budget = rung.exec_budget_s if rung.exec_budget_s is not None else rung.deadline_s
    share = budget / max(measured, 1)
    seen = [0]
    fp = str(fp)
    if form.vsql:
        fp, vnote = _value_fingerprint(ctx, form, src, n, n_out, fp)
        note += vnote

    def stop(secs: float) -> bool:
        secs = rung.meas[-1] if rung.meas else secs  # on the run's clock
        seen[0] += 1
        if rung.cancelled:
            rung.early_why = (
                f"the rung went past its {rung.limit_text()} during an iteration"
            )
            return True
        if seen[0] == 1 and measured > 1 and secs > share:
            rung.early_why = f"the first took {secs:.1f}s > its share {share:.1f}s of the {budget:.0f}s budget"
            return True
def _value_fingerprint(
    ctx: "_Ctx", form: Form, src: str, n: int, n_out: int, length_fp: str
) -> Tuple[str, List[str]]:
    """The form's value-level fingerprint (``v1:<n>``), from its ``vsql`` run once outside the timed iterations, and the
    notes for the row. A failure of this pass is a note, not a failure of the form: the length fingerprint stays.
    """
    from databricks.labs.gbx.bench import value_fp

    try:
        if "value_fp" not in ctx.setups:
            value_fp.register(ctx.spark)
            ctx.setups.add("value_fp")
        r = ctx.spark.sql(form.vsql.format(src=src, uc=ctx.uc or "")).collect()[0]
        if r[2] != r[0] and not form.allows_null:
            return length_fp, [f"value_fp: {r[0] - r[2]} of {r[0]} outputs NULL"]
        if r[1] is None:
            return length_fp, ["value_fp: no value"]
        return f"{value_fp.VERSION}:{r[1]}", [f"len_fp={length_fp}"]
    except Exception as exc:  # noqa: BLE001 - the timed row stands
        return length_fp, [f"value_fp failed: {type(exc).__name__}: {str(exc)[:120]}"]


        return False

    return stop


def _scan_tasks(ctx: "_Ctx", src: str) -> Optional[int]:
    """The number of Spark tasks that scan `src` (its non-empty input partitions); None when the run does not
    report it (`report_tasks`) or `src` is not a table.

    A tile input is a few files in one Delta table, so a tile form may run on one or two tasks whatever the
    cluster's size: the smoke stage reports this so that is seen before the real scales are submitted.
    """
    if not ctx.report_tasks:
        return None
    try:
        r = ctx.spark.sql(
            f"SELECT count(DISTINCT p) FROM (SELECT spark_partition_id() AS p FROM {src})"
        ).collect()
        return int(r[0][0])
    except Exception:  # noqa: BLE001 - a source that is not a table
        return None


def _tasks_note(tasks: Optional[int]) -> List[str]:
    return [] if tasks is None else [f"tasks={tasks}"]


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
    input_partitions: int = 0,
    setup_s: float = 0.0,
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
        input_partitions=input_partitions,
        setup_s=max(setup_s, 0.0),
        warmup_s=t.get("warmup_s", 0.0),
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
