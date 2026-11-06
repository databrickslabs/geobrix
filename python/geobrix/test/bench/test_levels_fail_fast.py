"""Fail fast in the levels leg: breadth-first rungs, a projected bar, a deadline that interrupts a rung, the first-iteration
early exit, and a setup cap. The forms are fakes over a fake Spark and a fake clock: nothing here runs a query.
"""

import threading
import time
import types

import pytest

from databricks.labs.gbx.bench import levels
from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import liveness, results, runner
from databricks.labs.gbx.bench.levels import Form

SET = "FF"
WAIT = 10  # seconds a fake query waits for its interrupt before the test gives up


class Clock:
    """The run's clock, moved by the fake work: a watchdog poll sees the jump."""

    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class FakeSpark:
    """Tags and interrupts like a Spark Connect session; `released` fires on the interrupt of the tag in flight."""

    def __init__(self):
        self.calls = []
        self.released = threading.Event()
        self.fail_interrupt = False

    def addTag(self, tag):
        self.calls.append(("add", tag))

    def removeTag(self, tag):
        self.calls.append(("remove", tag))

    def interruptTag(self, tag):
        self.calls.append(("interrupt", tag))
        if self.fail_interrupt:
            raise RuntimeError("no route to the server")
        self.released.set()


def form(name, **kw):
    kw.setdefault("level", "L4")  # a UC function level: no Python worker, no JAR probe
    return Form(
        mvp=SET, fn=name, variant="v", input="ff_in",
        run=f"{__name__}:ff_run", register=False, **kw,
    )  # fmt: skip


# What each fake form does, by fn name; set per test.
PLAN = {}
CALLS = []  # (fn, rows) of every query the fake forms ran


def ff_run(spark, src):
    fn, rows = src
    CALLS.append((fn, rows))
    PLAN[fn](spark, src)
    return rows, rows, rows


CURRENT = [""]  # the form whose input is being built (the builder is not told)


def ff_in(spark, n, corpus, schema):
    fn = CURRENT[0]
    PLAN.get(fn + ":build", lambda *a: None)(spark, n)
    return (fn, n)


@pytest.fixture
def env(monkeypatch):
    spark, clock = FakeSpark(), Clock()
    PLAN.clear()
    CALLS.clear()
    monkeypatch.setitem(lf.INPUTS, "ff_in", ff_in)
    monkeypatch.setitem(lf.BASE_ROWS, "ff_in", 10)
    monkeypatch.setattr(levels, "detect_compute", lambda s: "dedicated")
    real_run_one = levels._run_one

    def run_one(ctx, f, base, requested):
        CURRENT[0] = f.fn
        return real_run_one(ctx, f, base, requested)

    monkeypatch.setattr(levels, "_run_one", run_one)

    def run(forms, scales=(1, 4), warmup=0, measured=1, **kw):
        monkeypatch.setitem(lf.FORMS, SET, forms)
        sunk = []
        kw.setdefault("watch_poll_s", 0.01)
        rows = levels.run_mvp(
            spark, SET, "run1", warmup, measured, corpus="/none", schema=None, uc="c.s", scales=scales,
            where="venv", clock=clock, on_form=lambda rs: sunk.extend(rs), **kw,
        )  # fmt: skip
        return rows, sunk

    return types.SimpleNamespace(spark=spark, clock=clock, run=run)


def advance(env, secs):
    """Work that takes `secs` on the run's clock."""
    env.clock.t += secs


def stall_until_interrupted(env):
    """A query that never ends by itself: it is released by the interrupt, as a cancelled Spark query raises."""
    assert env.spark.released.wait(WAIT), "the watchdog never interrupted the query"
    env.spark.released.clear()
    raise RuntimeError("OPERATION_CANCELED: the operation was cancelled")


def by(rows):
    return {(r.fn, r.rows // 10): r for r in rows}  # (form, scale)


# ---- breadth first ----------------------------------------------------------------------------------------------


def test_rungs_run_breadth_first_and_the_old_order_is_one_flag_away(env):
    for fn in ("a", "b"):
        PLAN[fn] = lambda sp, src: advance(env, 1)
    forms = [form("a"), form("b")]
    rows, sunk = env.run(forms)
    assert [c[0] + str(c[1] // 10) for c in CALLS] == ["a1", "b1", "a4", "b4"]
    assert [(r.fn, r.rows // 10) for r in sunk] == [
        ("a", 1),
        ("b", 1),
        ("a", 4),
        ("b", 4),
    ]
    # the returned rows keep the form-grouped order callers have always had
    assert [(r.fn, r.rows // 10) for r in rows] == [
        ("a", 1),
        ("a", 4),
        ("b", 1),
        ("b", 4),
    ]

    CALLS.clear()
    rows, sunk = env.run(forms, fail_fast=False)
    assert [c[0] + str(c[1] // 10) for c in CALLS] == ["a1", "a4", "b1", "b4"]
    assert [(r.fn, r.rows // 10) for r in sunk] == [
        ("a", 1),
        ("a", 4),
        ("b", 1),
        ("b", 4),
    ]


def test_without_fail_fast_nothing_is_projected_skipped_or_interrupted(env):
    PLAN["slow"] = lambda sp, src: advance(env, 5000)  # 5000 s on the run's clock
    rows, _ = env.run([form("slow")], scales=(1, 4, 16), fail_fast=False)
    assert [r.status for r in rows] == ["ok", "ok", "ok"]
    assert env.spark.calls == []  # no tag was set, so none was interrupted


# ---- the projected bar ------------------------------------------------------------------------------------------


def test_a_rung_projected_past_the_cap_is_skipped_with_the_numbers_and_the_other_forms_go_on(
    env,
):
    PLAN["slow"] = lambda sp, src: advance(
        env, 100
    )  # 1x: 100 s of iteration, 0 of setup
    PLAN["fast"] = lambda sp, src: advance(env, 1)
    rows, sunk = env.run(
        [form("slow"), form("fast")], scales=(1, 4, 16), rung_cap_s=500, bar_factor=1.5
    )
    got = by(rows)
    assert got[("slow", 1)].status == "ok"
    # 100 s x 4 x 1.5 = 600 s > 500 s: not run
    assert got[("slow", 4)].status == "skipped"
    assert "projected 600s > cap 500s from 1x" in got[("slow", 4)].note
    # and the form's later rung is accounted for, not run
    assert (
        got[("slow", 16)].status == "skipped"
        and "projected past the cap at 4x" in got[("slow", 16)].note
    )
    assert [c for c in CALLS if c[0] == "slow"] == [("slow", 10)]  # only 1x ever ran
    assert [got[("fast", s)].status for s in (1, 4, 16)] == ["ok", "ok", "ok"]
    assert [r.status for r in sunk if r.fn == "slow"] == [
        "ok",
        "skipped",
        "skipped",
    ]  # each is sunk


def test_the_projection_counts_the_setup_not_only_the_iterations(env):
    PLAN["f:build"] = lambda sp, n: (
        advance(env, 90) if n == 10 else None
    )  # 90 s to build the 1x input
    PLAN["f"] = lambda sp, src: advance(env, 10)
    rows, _ = env.run([form("f")], rung_cap_s=500, setup_cap_s=1000)
    r1, r4 = rows
    # (90 s of setup + 10 s of run) x 4 x 1.5 = 600 s; the iterations alone would project 60 s
    assert r1.status == "ok"
    assert (
        r4.status == "skipped"
        and "projected 600s > cap 500s from 1x (setup 90s + run 10s" in r4.note
    )


# ---- the deadline -----------------------------------------------------------------------------------------------


def test_a_rung_past_its_deadline_is_interrupted_and_its_row_says_where_and_what_finished(
    env,
):
    calls = []

    def hangs(sp, src):
        calls.append(1)
        if len(calls) == 1:  # the warmup finishes in 3 s
            return advance(env, 3)
        advance(env, 5000)  # the first measured iteration never ends
        stall_until_interrupted(env)

    PLAN["hang"] = hangs
    PLAN["ok"] = lambda sp, src: advance(env, 1)
    rows, sunk = env.run(
        [form("hang"), form("ok")], warmup=1, measured=2, rung_cap_s=1200
    )
    got = by(rows)
    bar = got[("hang", 1)]
    assert bar.status == "over-bar"
    assert "stopped at 5003s past deadline 1200s during measured 1/2" in bar.note
    assert "warmup done [3.0]" in bar.note and "measured done []" in bar.note
    assert bar.warmup_iters == 1 and bar.measured_iters == 0
    # the interrupt named this rung's tag, and the tag is cleared afterwards
    tag = next(t for kind, t in env.spark.calls if kind == "add")
    assert "hang" in tag and "," not in tag
    assert ("interrupt", tag) in env.spark.calls and env.spark.calls.index(
        ("remove", tag)
    ) > env.spark.calls.index(("interrupt", tag))
    # the form's later rung is not run, the next form is
    assert (
        got[("hang", 4)].status == "skipped"
        and "over-bar at 1x" in got[("hang", 4)].note
    )
    assert [got[("ok", s)].status for s in (1, 4)] == ["ok", "ok"]
    assert [r.status for r in sunk][:2] == [
        "over-bar",
        "ok",
    ]  # the over-bar row was sunk the moment it happened


def test_a_later_rung_gets_its_projection_as_the_deadline_never_under_the_floor(env):
    def work(sp, src):
        advance(env, 10)
        if src[1] == 40:
            advance(env, 100_000)
            stall_until_interrupted(env)

    PLAN["f"] = work
    hb = []

    class HB:
        def begin(self, label, index, total, **kw):
            hb.append((label, kw))

        def end(self, **kw):
            pass

    rows, _ = env.run(
        [form("f")], heartbeat=HB(), deadline_floor_s=120, rung_cap_s=1200
    )
    r1, r4 = rows
    assert r1.status == "ok" and r4.status == "over-bar"
    # 10 s at 1x: projected 10 x 4 x 1.5 = 60 s, held to the floor of 120 s; the heartbeat shows both
    assert hb[0][1]["deadline_s"] == 1200.0 and "projected_s" not in hb[0][1]
    assert hb[1][1]["deadline_s"] == 120.0 and hb[1][1]["projected_s"] == pytest.approx(
        60.0, abs=1
    )
    assert "past deadline 120s" in r4.note and "projected 60s from 1x" in r4.note


def test_an_interrupt_that_fails_is_recorded_and_the_rung_still_stops_at_the_next_boundary(
    env,
):
    env.spark.fail_interrupt = True
    state = {"n": 0}

    def work(sp, src):
        state["n"] += 1
        if state["n"] == 1:
            advance(
                env, 5000
            )  # past the deadline: the watchdog flags the rung and its interrupt fails
            assert wait_for(lambda: any(k == "interrupt" for k, _ in env.spark.calls))
            time.sleep(0.2)  # the failure is noted just after the call raises

    PLAN["f"] = work
    rows, _ = env.run([form("f")], warmup=1, measured=1, scales=(1,), rung_cap_s=1200)
    (row,) = rows
    # the warmup ended by itself; the next iteration never starts, and the failure is on the row
    assert (
        row.status == "over-bar"
        and "during warmup 1/1" in row.note
        and "warmup done [5000.0]" in row.note
    )
    assert "interrupt failed (RuntimeError: no route to the server)" in row.note
    assert state["n"] == 1


def wait_for(cond, secs=WAIT):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.005)
    return False


# ---- the first iteration ----------------------------------------------------------------------------------------


def test_a_first_iteration_past_its_share_of_the_deadline_ends_the_rung_there(env):
    PLAN["f"] = lambda sp, src: advance(
        env, 300
    )  # one iteration is 300 s; the share is 1200 / 5 = 240 s
    rows, _ = env.run([form("f")], scales=(1,), measured=5, rung_cap_s=1200)
    (row,) = rows
    assert row.status == "ok" and row.measured_iters == 1
    assert "early exit: 1 of 5 measured iterations (the first took" in row.note
    assert len([c for c in CALLS if c[0] == "f"]) == 1

    CALLS.clear()
    rows, _ = env.run([form("f")], scales=(1,), measured=5, fail_fast=False)
    assert rows[0].measured_iters == 5 and "early exit" not in rows[0].note


def test_a_first_iteration_inside_its_share_runs_them_all(env):
    PLAN["f"] = lambda sp, src: advance(env, 100)
    rows, _ = env.run([form("f")], scales=(1,), measured=5, rung_cap_s=1200)
    assert rows[0].measured_iters == 5 and "early exit" not in rows[0].note


# ---- setup ------------------------------------------------------------------------------------------------------


def test_a_setup_past_its_own_cap_is_interrupted_and_noted_and_the_higher_rungs_are_skipped(
    env,
):
    PLAN["big:build"] = lambda sp, n: (
        advance(env, 700),
        stall_until_interrupted(env),
    )  # builds for ever
    PLAN["big"] = lambda sp, src: advance(env, 1)
    PLAN["small"] = lambda sp, src: advance(env, 1)
    rows, _ = env.run(
        [form("big"), form("small")],
        scales=(1, 4, 16),
        setup_cap_s=600,
        rung_cap_s=1200,
    )
    got = by(rows)
    assert got[("big", 1)].status == "over-bar"
    assert (
        "setup-bound: stopped at 700s past setup cap 600s during setup; setup 700s (not finished)"
        in got[("big", 1)].note
    )
    assert got[("big", 1)].setup_s == pytest.approx(700, abs=1)
    assert all(
        got[("big", s)].status == "skipped"
        and "setup-bound at 1x" in got[("big", s)].note
        for s in (4, 16)
    )
    assert [got[("small", s)].status for s in (1, 4, 16)] == ["ok"] * 3


def test_the_setup_cap_grows_with_the_scale_and_is_held_to_the_rung_cap(env):
    # 1x setup is 10 s; the 4x setup is 1500 s: under 600 x 4 = 2400 s but the cap is held to the 1200 s rung cap
    PLAN["f"] = lambda sp, src: advance(env, 1)
    PLAN["f:build"] = lambda sp, n: (
        advance(env, 10)
        if n == 10
        else (advance(env, 1500), stall_until_interrupted(env))
    )
    rows, _ = env.run([form("f")], setup_cap_s=600, rung_cap_s=1200, bar_factor=0.01)
    r1, r4 = rows
    assert r1.status == "ok"
    assert (
        r4.status == "over-bar"
        and "setup-bound: stopped at 1500s past setup cap 1200s" in r4.note
    )


def test_a_legitimately_long_setup_is_not_an_over_bar_rung(env):
    # 1x ran with a cached input (setup 0); the 4x input takes 500 s to build, which is inside its setup cap
    # (600 x 4) though far past the rung's projected 120 s deadline: the iterations are what the deadline judges
    PLAN["f:build"] = lambda sp, n: advance(env, 500) if n == 40 else None
    PLAN["f"] = lambda sp, src: advance(env, 1)
    rows, _ = env.run([form("f")], setup_cap_s=600, rung_cap_s=1200)
    assert [r.status for r in rows] == ["ok", "ok"]


# ---- the pieces -------------------------------------------------------------------------------------------------


def test_time_iters_stops_when_asked_and_counts_the_iterations_that_ran():
    ran = []
    out = runner.time_iters(
        lambda: ran.append(1), 0, 5, stop=lambda secs: len(ran) == 2
    )
    assert len(ran) == 2 and out["measured_iters"] == 2
    ran.clear()
    assert (
        runner.time_iters(lambda: ran.append(1), 0, 3)["measured_iters"] == 3
    )  # no stop: as before


def test_the_heartbeat_carries_the_deadline_the_projection_and_the_phase_only_when_given(
    tmp_path, capsys
):
    import json

    path = tmp_path / "hb.json"
    hb = liveness.Heartbeat(str(path), interval=3600, print_every=3600).start()
    hb.begin(
        "f @4x",
        1,
        1,
        scale=4,
        cap_s=240,
        rows=40,
        name="f",
        set_name="FF",
        deadline_s=120.0,
        projected_s=60.0,
    )
    hb.set_phase("warmup 1/2")
    hb.beat()
    beat = json.loads(path.read_text())
    hb._say_running()
    hb.end()
    hb.stop()
    out = capsys.readouterr().out
    assert (beat["deadline_s"], beat["projected_s"], beat["rung_phase"]) == (
        120.0,
        60.0,
        "warmup 1/2",
    )
    assert "START (cap 240s, deadline 120s, projected 60s)" in out
    assert "RUNNING elapsed" in out and "deadline 120s phase warmup 1/2" in out


def test_the_interrupt_api_by_session_kind(monkeypatch):
    from databricks.labs.gbx.manager import capabilities

    sc = types.SimpleNamespace(
        calls=[], setJobGroup=lambda *a: sc.calls.append(("group", a)),
        cancelJobGroup=lambda t: sc.calls.append(("cancel", t)),
        setLocalProperty=lambda k, v: sc.calls.append(("prop", k, v)),
        getLocalProperty=lambda k: None,
    )  # fmt: skip
    classic = types.SimpleNamespace(sparkContext=sc)
    monkeypatch.setattr(capabilities, "is_connect", lambda s: False)
    monkeypatch.setattr(capabilities, "is_serverless", lambda: False)
    arm, interrupt, disarm = levels._interrupt_api(classic, "t1")
    arm(), interrupt(), disarm()
    assert sc.calls[:2] == [("group", ("t1", "t1", True)), ("cancel", "t1")]
    assert ("prop", "spark.jobGroup.id", None) in sc.calls[2:]
    # Spark 4 classic has addTag too, but its interruptTag cancels nothing (measured): a classic session groups
    classic4 = types.SimpleNamespace(
        sparkContext=sc, addTag=lambda t: 1 / 0, interruptTag=lambda t: 1 / 0
    )
    sc.calls.clear()
    arm, interrupt, disarm = levels._interrupt_api(classic4, "t2")
    arm(), interrupt(), disarm()
    assert [c[0] for c in sc.calls][:2] == ["group", "cancel"] and {
        c[0] for c in sc.calls[2:]
    } == {"prop"}
    # Spark Connect has no SparkContext: it tags
    monkeypatch.setattr(capabilities, "is_connect", lambda s: True)
    tags = []
    connect = types.SimpleNamespace(
        addTag=lambda t: tags.append(("add", t)), interruptTag=lambda t: tags.append(("int", t)),
        removeTag=lambda t: tags.append(("rm", t)),
    )  # fmt: skip
    arm, interrupt, disarm = levels._interrupt_api(connect, "t3")
    arm(), interrupt(), disarm()
    assert tags == [("add", "t3"), ("int", "t3"), ("rm", "t3")]
    monkeypatch.setattr(capabilities, "is_connect", lambda s: False)
    # Serverless forbids sparkContext: no tag API, no interrupt (and sparkContext is never touched)
    monkeypatch.setattr(capabilities, "is_serverless", lambda: True)

    class Connect:
        @property
        def sparkContext(self):
            raise AssertionError("Serverless forbids sparkContext")

    assert levels._interrupt_api(Connect(), "t1") is None


def test_over_bar_and_skipped_rows_are_counted_in_the_summary_status_line():
    base = dict(
        run_id="r", api="L4", fn="f", category="c", mode="spark-path", rows=10, level="L4", form="v",
        compute="dedicated", **levels.capture_env("venv"),
    )  # fmt: skip
    rows = [
        levels._row(base, status=s) for s in ("ok", "over-bar", "skipped", "skipped")
    ]
    assert (
        "ok 1 · na_by_design 0 · error 0 · skipped 2 · over-bar 1"
        in results.summarize(rows)
    )


def test_the_notebook_hands_the_fail_fast_knobs_to_the_levels_leg_and_its_smoke():
    from databricks.labs.gbx.bench import cluster as cl

    cfg = dict(
        wheel="/Volumes/c/s/v/w.whl", corpus="/Volumes/c/s/v/corpus", out_dir="/Volumes/c/s/v/out",
        table="c.s.bench_results", run_id="r1", functions="", modes="spark-path", row_counts="10",
        warmup=0, measured=1, lightweight=True, levels_mvp="S", levels_uc="c.s",
    )  # fmt: skip

    def source(**kw):
        nb = cl.build_bench_notebook({**cfg, **kw})
        return "\n".join("".join(c["source"]) for c in nb["cells"])

    on = source()
    assert (
        "FAIL_FAST = True" in on
        and "BAR_FACTOR = 1.5" in on
        and "RUNG_CAP_S = 1200.0" in on
        and "SETUP_CAP_S = 600.0" in on
    )
    assert (
        on.count("fail_fast=FAIL_FAST, bar_factor=BAR_FACTOR") == 1
    )  # the levels cell
    assert "FAIL_FAST = False" in source(fail_fast=False)
    assert "RUNG_CAP_S = 300.0" in source(
        rung_cap_s=300
    ) and "BAR_FACTOR = 2.0" in source(bar_factor=2)


def ff_sleepy(spark, src):
    """A real Spark job whose Python UDF sleeps far longer than the test lets it."""
    from pyspark.sql import functions as F

    nap = F.udf(lambda i: (time.sleep(30), i)[1], "long")
    r = (
        spark.range(4, numPartitions=4)
        .select(nap("id").alias("x"))
        .agg(F.count("x"), F.sum("x"), F.count("x"))
        .collect()[0]
    )
    return r[0], r[1], r[2]


def test_a_real_spark_job_is_cancelled_at_the_deadline_not_waited_for(
    spark_session_local, monkeypatch
):
    """The decisive check of the interrupt on a classic session: a tagged job is not cancelled by Spark 4's
    interruptTag, a grouped one is."""
    monkeypatch.setitem(lf.INPUTS, "ff_in", lambda spark, n, corpus, schema: (n, n))
    monkeypatch.setitem(lf.BASE_ROWS, "ff_in", 4)
    monkeypatch.setitem(
        lf.FORMS,
        SET,
        [
            Form(
                mvp=SET,
                fn="nap",
                level="L4",
                variant="v",
                input="ff_in",
                run=f"{__name__}:ff_sleepy",
                register=False,
            )
        ],
    )
    t0 = time.monotonic()
    rows = levels.run_mvp(
        spark_session_local, SET, "run1", 0, 1, corpus="/none", schema=None, uc="c.s", scales=(1,),
        where="venv", rung_cap_s=3, watch_poll_s=0.1,
    )  # fmt: skip
    assert time.monotonic() - t0 < 20  # the sleep is 30 s
    (row,) = rows
    assert (
        row.status == "over-bar"
        and "stopped at" in row.note
        and "during measured 1/1" in row.note
    )


# ---- review fixes -----------------------------------------------------------------------------------------------


def ff_run_df(spark, src):
    """A run form that also returns the DataFrame whose plan feeds the split."""
    fn, rows = src
    CALLS.append((fn, rows))
    PLAN[fn](spark, src)
    return rows, rows, rows, object()


def test_without_an_interrupt_api_the_rung_stops_at_the_next_iteration_boundary(
    env, monkeypatch
):
    monkeypatch.setattr(levels, "_interrupt_api", lambda spark, tag: None)

    def work(sp, src):
        advance(env, 5000)  # the warmup runs past the rung cap
        time.sleep(0.2)  # the watchdog's poll sees it

    PLAN["f"] = work
    rows, _ = env.run([form("f")], warmup=1, measured=1, scales=(1,), rung_cap_s=1200)
    (row,) = rows
    assert row.status == "over-bar" and "no interrupt API on this session" in row.note
    assert [c for c in CALLS if c[0] == "f"] == [
        ("f", 10)
    ]  # the warmup only: the measured iteration never ran
    assert env.spark.calls == []


def test_a_watchdog_that_outlives_its_rung_writes_nothing_on_it_and_says_it_was_stuck(
    monkeypatch,
):
    monkeypatch.setattr(levels, "WATCH_JOIN_S", 0.2)
    clock = Clock()
    rung = levels._Rung(scale=1, clock=clock, deadline_s=10, setup_cap_s=5)
    rung.begin_exec()
    entered, release = threading.Event(), threading.Event()

    def interrupt():
        entered.set()
        release.wait(WAIT)
        raise RuntimeError("late")

    watch = levels._Watchdog(rung, interrupt, poll=0.01).start()
    clock.t += 100
    assert entered.wait(WAIT)
    assert (
        watch.stop() is False
    )  # the join timed out: the interrupt call is still blocking
    assert "still inside an interrupt call" in rung.interrupt_note
    note = rung.interrupt_note
    release.set()
    time.sleep(0.2)
    assert (
        rung.interrupt_note == note
    )  # the late failure is not written on a rung that has ended


def test_the_watchdog_leaves_a_rung_alone_once_its_iterations_are_done(
    env, monkeypatch
):
    """The plan-metrics lookup, the worker read and the output checks run after the last iteration: a rung past its
    deadline there is a finished measurement, not an over-bar one."""
    monkeypatch.setitem(lf.INPUTS, "ff_in", ff_in)

    def from_plan(df, t0):
        advance(env, 5000)
        time.sleep(0.2)  # the watchdog polls while the lookup runs
        raise RuntimeError("the lookup failed")

    monkeypatch.setattr(levels.attribution, "from_plan", from_plan)
    PLAN["f"] = lambda sp, src: advance(env, 1)
    f = Form(
        mvp=SET,
        fn="f",
        variant="v",
        level="L4",
        input="ff_in",
        run=f"{__name__}:ff_run_df",
        register=False,
    )
    rows, _ = env.run([f], scales=(1,), rung_cap_s=1200)
    assert rows[0].status == "error" and "the lookup failed" in rows[0].failure_mode
    assert not any(k == "interrupt" for k, _ in env.spark.calls)


def test_a_classic_rung_puts_the_cells_job_group_back(monkeypatch):
    from databricks.labs.gbx.manager import capabilities

    monkeypatch.setattr(capabilities, "is_connect", lambda s: False)
    monkeypatch.setattr(capabilities, "is_serverless", lambda: False)
    keys = ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")

    def classic(props):
        def setj(group, desc, interrupt):
            props.update(
                {keys[0]: group, keys[1]: desc, keys[2]: str(interrupt).lower()}
            )

        def setp(k, v):
            props.pop(k, None) if v is None else props.__setitem__(k, v)

        sc = types.SimpleNamespace(
            getLocalProperty=props.get,
            setLocalProperty=setp,
            setJobGroup=setj,
            cancelJobGroup=lambda t: None,
        )
        return types.SimpleNamespace(sparkContext=sc)

    cell = {keys[0]: "cell-9", keys[1]: "the cell", keys[2]: "false"}
    props = dict(cell)
    arm, _, disarm = levels._interrupt_api(classic(props), "t1")
    arm()
    assert props[keys[0]] == "t1"
    disarm()
    assert props == cell  # the notebook cell's group, description and flag are back
    props.clear()
    arm, _, disarm = levels._interrupt_api(classic(props), "t1")
    arm(), disarm()
    assert props == {}  # no group before, none after


def test_a_tag_keeps_its_scale_and_stays_apart_from_another_rungs_when_the_label_is_long():
    label = "gbx_rst_a_very_long_function_name_" * 4
    tags = {
        levels._tag("run1", label + str(k), s) for k in range(2) for s in (1, 4, 16)
    }
    assert len(tags) == 6 and all(len(t) <= 120 and "," not in t for t in tags)
    assert levels._tag("run1", label, 16).endswith("-16x") and levels._tag(
        "run1", label, 4
    ).endswith("-4x")
    assert (
        levels._tag("r", "f:L3:c0", 4) == "gbx-bench-r-f_L3_c0-4x"
    )  # a short one is as it was


def test_a_form_skipped_by_projection_counts_as_done_in_the_heartbeat(env, tmp_path):
    import json

    PLAN["slow"] = lambda sp, src: advance(env, 100)
    PLAN["fast"] = lambda sp, src: advance(env, 1)
    hb = liveness.Heartbeat(str(tmp_path / "hb.json"), interval=3600).start()
    env.run(
        [form("slow"), form("fast")], scales=(1, 4, 16), rung_cap_s=500, heartbeat=hb
    )
    hb.stop()
    assert json.loads((tmp_path / "hb.json").read_text())["forms_done"] == 2


def test_the_projection_from_a_rung_that_exited_early_is_that_of_a_full_rung(env):
    PLAN["f"] = lambda sp, src: advance(
        env, 300
    )  # 5 iterations of 300 s would be 1500 s; one ran
    rows, _ = env.run(
        [form("f")], scales=(1, 4), measured=5, rung_cap_s=1200, bar_factor=0.5
    )
    r1, r4 = rows
    assert r1.status == "ok" and r1.measured_iters == 1
    # a full rung is 5 x 300 s: x4 x 0.5 = 3000 s > 1200 s (the early-exit rung's own 300 s would project 600 s)
    assert r4.status == "skipped" and "projected 3000s > cap 1200s from 1x" in r4.note


def test_a_partial_input_table_does_not_stay_to_be_read_as_complete():
    calls = []

    class Writer:
        def format(self, f):
            return self

        def saveAsTable(self, name):
            calls.append(("write", name))
            raise RuntimeError("OPERATION_CANCELED")

    class Spark:
        catalog = types.SimpleNamespace(tableExists=lambda n: False)

        def sql(self, q):
            calls.append(("sql", q))

    build = lambda: types.SimpleNamespace(write=Writer())  # noqa: E731
    with pytest.raises(RuntimeError, match="OPERATION_CANCELED"):
        lf._table(Spark(), "c.s", "mvp_in_x", build)
    assert ("sql", "DROP TABLE IF EXISTS c.s.mvp_in_x") in calls

    calls.clear()

    class Raced(Spark):
        catalog = types.SimpleNamespace(tableExists=lambda n: len(calls) > 0)

    Writer.saveAsTable = lambda self, name: (
        calls.append(("write", name)),
        (_ for _ in ()).throw(RuntimeError("TABLE_OR_VIEW_ALREADY_EXISTS")),
    )[1]
    assert lf._table(Raced(), "c.s", "mvp_in_x", build) == "c.s.mvp_in_x"
    assert not any(
        k == "sql" for k, _ in calls
    )  # another run's table is its own: not dropped


def test_an_over_bar_row_has_the_p90_the_ok_rows_have(env):
    samples = [float(k) for k in range(1, 11)]  # 1 .. 10 s
    state = {"n": 0}

    def work(sp, src):
        state["n"] += 1
        if state["n"] <= 10:
            return advance(env, samples[state["n"] - 1])
        advance(env, 5000)
        stall_until_interrupted(env)

    PLAN["f"] = work
    rows, _ = env.run([form("f")], scales=(1,), measured=11, rung_cap_s=1200)
    (row,) = rows
    assert row.status == "over-bar" and row.measured_iters == 10
    assert row.iter_p90_s == 9.0 and row.iter_min_s == 1.0 and row.iter_median_s == 5.5


def test_a_rung_whose_iteration_finishes_after_the_flag_is_over_bar_and_its_form_is_dead(
    env,
):
    def work(sp, src):
        advance(env, 5000)
        time.sleep(
            0.2
        )  # the watchdog flags it; the interrupt misses (nothing was running) and the iteration ends

    PLAN["f"] = work
    PLAN["g"] = lambda sp, src: advance(env, 1)
    rows, _ = env.run(
        [form("f"), form("g")], scales=(1, 4), measured=1, rung_cap_s=1200
    )
    got = by(rows)
    assert got[("f", 1)].status == "over-bar" and got[("f", 1)].measured_iters == 1
    assert got[("f", 4)].status == "skipped" and "over-bar at 1x" in got[("f", 4)].note
    assert [got[("g", s)].status for s in (1, 4)] == ["ok", "ok"]


def test_the_heartbeat_says_an_interrupt_on_its_own_tagged_line(tmp_path, capsys):
    hb = liveness.Heartbeat(
        str(tmp_path / "hb.json"), interval=3600, print_every=3600
    ).start()
    hb.begin("f @1x", 1, 1, scale=1, cap_s=60, rows=10, name="f", set_name="FF")
    hb.event(
        "INTERRUPT", "over-bar: elapsed 1300s past deadline 1200s during warmup 1/1"
    )
    hb.end()
    hb.stop()
    assert (
        "[LEVELS FF] form 1/1 f scale 1x rows 10 INTERRUPT over-bar: elapsed 1300s past deadline 1200s"
        in capsys.readouterr().out
    )
