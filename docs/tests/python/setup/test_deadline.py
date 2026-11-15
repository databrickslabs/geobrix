"""The per-test deadline (conftest.py ``_deadline`` fixture, deadline_guard.py).

None of these tests uses Spark, so none touches the JVM.
"""

import faulthandler
import os
import signal
import sys
import time
import warnings
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deadline_guard  # noqa: E402


@pytest.mark.deadline(2)
def test_a_test_past_its_deadline_fails_instead_of_blocking():
    with pytest.raises(TimeoutError, match="still running after 2 s"):
        time.sleep(120)


def test_a_test_that_finishes_during_the_dump_is_not_signalled(monkeypatch):
    """The watchdog's dump takes a while. If the test ends in that window, its teardown
    has restored the previous SIGALRM handler, and a signal sent now would be handled
    by it (the default one ends pytest with "Alarm clock")."""
    stray = []
    outer = signal.signal(signal.SIGALRM, lambda signum, frame: stray.append(signum))
    try:
        guard = deadline_guard.DeadlineGuard("t", 60, None, uses_jvm=False)
        guard.start()
        monkeypatch.setattr(faulthandler, "dump_traceback", lambda **kw: guard.stop())
        guard._expire()  # the deadline passes; the test finishes while the stacks dump
        time.sleep(0.2)  # a signal sent to the main thread would be handled by now
    finally:
        signal.signal(signal.SIGALRM, outer)
    assert stray == []
    assert guard.finished.is_set()


def test_a_late_signal_after_the_test_finished_is_ignored():
    guard = deadline_guard.DeadlineGuard("t", 60, None, uses_jvm=False)
    guard.finished.set()
    guard._raise(signal.SIGALRM, None)  # no TimeoutError


def test_stop_restores_the_previous_handler():
    outer = signal.getsignal(signal.SIGALRM)
    guard = deadline_guard.DeadlineGuard("t", 60, None, uses_jvm=False)
    guard.start()
    assert signal.getsignal(signal.SIGALRM) == guard._raise
    guard.stop()
    assert signal.getsignal(signal.SIGALRM) == outer


class _AlarmLandsWhileWaiting:
    """Stands in for the guard's send lock: the timer thread holds the real one while it
    signals, so SIGALRM lands in the main thread while ``stop`` is still waiting for the
    lock, before ``finished`` is set."""

    def __enter__(self):
        signal.raise_signal(signal.SIGALRM)  # the Python handler runs right here

    def __exit__(self, *exc):
        return False


def test_stop_cleans_up_when_the_alarm_lands_while_it_waits():
    outer = signal.getsignal(signal.SIGALRM)
    guard = deadline_guard.DeadlineGuard("t", 60, None, uses_jvm=False)
    guard.start()
    guard._send = _AlarmLandsWhileWaiting()
    try:
        with pytest.raises(TimeoutError, match="still running after 60 s"):
            guard.stop()
        # The error still reports the deadline, but the guard is fully torn down.
        assert not guard._timer.is_alive()
        assert guard.finished.is_set()
        assert signal.getsignal(signal.SIGALRM) == outer
    finally:
        signal.signal(signal.SIGALRM, outer)
        guard._timer.cancel()


@pytest.mark.parametrize(
    "raw, expected",
    [(None, 600), ("", 600), ("  ", 600), ("900", 900), (" 45 ", 45), ("2.5", 2.5)],
)
def test_deadline_env_value_is_parsed(raw, expected):
    assert deadline_guard.parse_deadline_s(raw) == expected


@pytest.mark.parametrize("raw", ["ten minutes", "10m", "0", "-5", "nan", "inf"])
def test_a_bad_deadline_env_value_warns_and_uses_the_default(raw):
    with pytest.warns(RuntimeWarning, match="GBX_DOC_TEST_TIMEOUT_S") as caught:
        assert deadline_guard.parse_deadline_s(raw) == 600
    assert repr(raw) in str(caught[0].message)


def test_without_a_local_jvm_the_deadline_fails_the_test_instead_of_ending_the_run(
    monkeypatch,
):
    """Under Spark Connect there is no py4j gateway to probe. Reporting "the JVM does
    not respond" and ending the run would be wrong: raise in the test like any other."""
    from pyspark import SparkContext

    class _Capman:
        def suspend_global_capture(self, in_=False):
            pass

    def _ended_the_run(code):
        raise AssertionError(f"os._exit({code}) called")

    monkeypatch.setattr(SparkContext, "_gateway", None)
    monkeypatch.setattr(SparkContext, "_jvm", None)
    monkeypatch.setattr(os, "_exit", _ended_the_run)
    monkeypatch.setattr(faulthandler, "dump_traceback", lambda **kw: None)
    guard = deadline_guard.DeadlineGuard("t", 60, _Capman(), uses_jvm=True)
    guard.start()
    try:
        with pytest.raises(TimeoutError, match="still running after 60 s"):
            guard._expire()  # signals this (the main) thread
            time.sleep(5)  # the handler raises here
    finally:
        guard.stop()


class _Query:
    def __init__(self, name):
        self.name, self.stopped = name, False

    def stop(self):
        self.stopped = True


class _Spark:
    def __init__(self, *queries):
        self.streams = type("Streams", (), {"active": list(queries)})()


class _Guard:
    def __init__(self, error=None):
        self.error, self.stopped = error, False

    def stop(self):
        self.stopped = True
        if self.error:
            raise self.error


def test_a_test_ends_by_stopping_the_guard_and_the_queries_it_left_running():
    queries = [_Query("a"), _Query("b")]
    guard = _Guard()
    left = deadline_guard.end_test(guard, _Spark(*queries))
    assert guard.stopped and all(q.stopped for q in queries)
    assert [q.name for q in left] == ["a", "b"]  # reported, so the test fails


def test_queries_are_stopped_even_when_the_guard_raises_on_stop():
    """The guard's stop raises the deadline's TimeoutError when the alarm lands while
    it waits; a streaming query must not outlive the test because of it."""
    queries = [_Query("a")]
    with pytest.raises(TimeoutError, match="still running"):
        deadline_guard.end_test(
            _Guard(TimeoutError("t still running after 5 s")), _Spark(*queries)
        )
    assert queries[0].stopped


def test_a_test_without_spark_ends_cleanly():
    guard = _Guard()
    assert deadline_guard.end_test(guard, None) == []
    assert guard.stopped
