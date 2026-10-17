"""The per-test deadline (conftest.py ``_deadline`` fixture, deadline_guard.py).

None of these tests uses Spark, so none touches the JVM.
"""

import faulthandler
import signal
import sys
import time
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
