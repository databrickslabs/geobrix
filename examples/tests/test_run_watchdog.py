"""The run watchdog: a stalled bench run is cancelled by the launcher and named, never retried.

Pure (no workspace, no databricks-sdk, no Spark): the clock, the heartbeat reader and the run state are fakes.
"""

import _run_watchdog as wd
import pytest


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def beat(seq, form="bng:L3:c0", elapsed=0.0, state="running", stack=("File x.py, line 1, in run_mvp", "  time.sleep(9)")):
    return {"seq": seq, "state": state, "form": form, "form_elapsed_s": elapsed, "forms_done": 3, "stack": list(stack)}


def test_a_form_past_its_cap_is_a_stall_even_while_the_heartbeat_keeps_beating():
    """The driver is alive (beats arrive) but one form has run too long: name the form."""
    c = Clock()
    w = wd.Watchdog(form_cap_s=600, stale_s=300, clock=c)
    assert w.observe(beat(1, elapsed=0)) is None
    for i in range(2, 30):
        c.t += 30
        stall = w.observe(beat(i, elapsed=(i - 1) * 30))
        if stall:
            break
    assert stall is not None and stall.form == "bng:L3:c0"
    assert "cap" in stall.reason and 10 <= stall.minutes <= 11
    assert stall.line == f"STALLED: bng:L3:c0 after {stall.minutes:.1f} min"


def test_a_form_at_a_larger_scale_is_judged_by_its_own_scaled_cap_and_named_with_its_scale():
    """The ladder 1x/4x/16x: a 16x form that runs past the 1x cap is healthy; one past its scaled cap is stalled."""

    def run(elapsed_end):
        c = Clock()
        w = wd.Watchdog(form_cap_s=600, stale_s=300, clock=c)  # the 1x cap: 10 min
        stall = None
        for i, el in enumerate(range(0, int(elapsed_end) + 1, 30), 1):
            c.t += 30
            b = {**beat(i, "bng:L3:c0 @16x", el), "scale": 16, "cap_s": 9600}  # 160 min at 16x
            stall = w.observe(b) or stall
            if stall:
                break
        return stall

    assert run(3600) is None  # 60 min at 16x: past the 1x cap six times over, inside the 16x cap
    stall = run(9600 + 120)  # past 160 min
    assert stall is not None and stall.form == "bng:L3:c0 @16x" and "160 min cap" in stall.reason
    assert stall.line.startswith("STALLED: bng:L3:c0 @16x after 160.")


def test_a_beat_with_no_cap_falls_back_to_the_watchdogs_own_cap():
    c = Clock()
    w = wd.Watchdog(form_cap_s=60, stale_s=300, clock=c)
    assert w.observe(beat(1, elapsed=0)) is None
    c.t += 30
    assert w.observe(beat(2, elapsed=30)) is None
    c.t += 61
    assert w.observe(beat(3, elapsed=91)) is not None


def test_a_heartbeat_that_stops_changing_is_a_stall_and_the_clock_of_the_notebook_does_not_matter():
    c = Clock()
    w = wd.Watchdog(form_cap_s=3600, stale_s=300, clock=c)
    assert w.observe(beat(7, elapsed=5)) is None
    c.t += 200
    assert w.observe(beat(7, elapsed=5)) is None  # the same beat again: quiet, not yet stale
    c.t += 150
    stall = w.observe(beat(7, elapsed=5))
    assert stall is not None and "heartbeat stale" in stall.reason and stall.form == "bng:L3:c0"


def test_nothing_is_judged_before_the_first_beat_or_after_a_finished_run():
    c = Clock()
    w = wd.Watchdog(form_cap_s=60, stale_s=60, clock=c)
    c.t = 10_000
    assert w.observe(None) is None  # the cluster is still starting
    assert w.observe(beat(1, state="done")) is None
    c.t += 10_000
    assert w.observe(beat(1, state="done")) is None


def test_a_heartbeat_file_another_launch_left_at_the_same_path_is_not_this_runs():
    """A cancelled run's file (old token, a form already 8 min in) once got the next run cancelled seven seconds in."""
    c = Clock()
    w = wd.Watchdog(form_cap_s=300, stale_s=300, clock=c, token="new-launch")
    left_over = {**beat(32, elapsed=501), "token": "old-launch"}
    assert w.observe(left_over) is None and w.last_beat == {}  # not seen at all
    assert w.observe({**beat(1, elapsed=3), "token": "new-launch"}) is None
    assert w.last_beat["seq"] == 1


def run_watch(beats, states, **kw):
    """watch_run over scripted beats and states; returns (outcome, cancels, printed lines)."""
    c = Clock()
    beats, states, cancels, out = iter(beats), iter(states), [], []
    last = {"state": ("RUNNING", "")}

    def get_state():
        try:
            last["state"] = next(states)
        except StopIteration:
            pass
        return last["state"]

    outcome = wd.watch_run(
        get_state=get_state,
        read_beat_fn=lambda: next(beats, None),
        cancel=lambda: (cancels.append(c.t), last.update(state=("TERMINATED", "CANCELED"))),
        watchdog=wd.Watchdog(form_cap_s=120, stale_s=300, clock=c),
        poll_secs=30,
        sleep=c.sleep,
        clock=c,
        print_fn=out.append,
        **kw,
    )
    return outcome, cancels, out


def test_a_stalled_run_is_cancelled_once_named_with_its_stack_and_not_retried():
    stack = ["File levels.py, line 401, in _run_one", "File levels_forms.py, line 470, in uc_scalar", "  rows = df.collect()"]
    beats = [beat(1, "gbx_bng:L4:uc-scalar", 0, stack=stack)] + [
        beat(2, "gbx_bng:L4:uc-scalar", 30, stack=stack)
    ] * 40
    (kind, stall), cancels, out = run_watch(beats, [("RUNNING", "")] * 100)
    assert kind == "stalled" and stall.form == "gbx_bng:L4:uc-scalar"
    assert len(cancels) == 1
    text = "\n".join(out)
    assert "STALLED: gbx_bng:L4:uc-scalar after" in text
    assert "rows = df.collect()" in text  # the stack tail names where


def test_a_run_that_finishes_on_its_own_is_not_cancelled():
    outcome, cancels, out = run_watch([beat(1)], [("RUNNING", ""), ("TERMINATED", "SUCCESS")])
    assert outcome == ("finished", "TERMINATED") and cancels == []


def test_the_heartbeat_reader_returns_none_for_a_missing_or_half_written_file():
    class W:
        class files:
            @staticmethod
            def download(path):
                raise FileNotFoundError(path)

    assert wd.read_beat(W, "/Volumes/x/heartbeat.json") is None


def test_a_cancel_that_fails_still_reports_the_stall_and_says_to_cancel_by_hand():
    c = Clock()
    out = []
    stuck = [beat(1, "f", 0)] + [beat(2, "f", 500)] * 40

    def cancel():
        raise RuntimeError("PermissionDenied: cannot cancel")

    beats = iter(stuck)
    kind, stall = wd.watch_run(
        get_state=lambda: ("RUNNING", ""),
        read_beat_fn=lambda: next(beats, None),
        cancel=cancel,
        watchdog=wd.Watchdog(form_cap_s=120, stale_s=300, clock=c),
        poll_secs=30,
        sleep=c.sleep,
        clock=c,
        cancel_wait_s=60,
        print_fn=out.append,
        run_id=4242,
    )
    text = "\n".join(out)
    assert kind == "stalled" and stall.cancel_error and "cannot cancel" in stall.cancel_error
    assert "STALLED: f after" in text
    assert "cancel FAILED: " in text and "cancel run 4242 by hand" in text
