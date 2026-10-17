"""Per-test deadline for the doc tests (used by the autouse ``_deadline`` fixture in
conftest.py; ``@pytest.mark.deadline(seconds)`` sets one test's limit).

The amd64 dev image runs under Rosetta on Apple Silicon, where the JVM has been seen
to wedge with every thread blocked: Spark is idle, nothing errors, and the run never
ends. pytest-timeout is not installed in the image, so every doc test runs under this
guard. The default limit is the GBX_DOC_TEST_TIMEOUT_S environment variable (600 when
unset; ``gbx:test:python-docs --timeout`` sets it).
"""

import faulthandler
import math
import os
import signal
import sys
import threading
import warnings

FALLBACK_DEADLINE_S = 600
_JVM_PROBE_S = 15


def end_test(guard, spark):
    """Stop ``guard``, then every streaming query the test left running -- also when
    ``guard.stop()`` raises (it reports the deadline when the alarm lands while it
    waits). Returns the queries that were left, for the fixture to fail the test on."""
    leaked = []
    try:
        guard.stop()
    finally:
        leaked = list(spark.streams.active) if spark is not None else []
        for query in leaked:
            query.stop()
    return leaked


def parse_deadline_s(raw, fallback=FALLBACK_DEADLINE_S):
    """Seconds from the GBX_DOC_TEST_TIMEOUT_S value ``raw``: ``fallback`` when it is
    unset or blank. A value that is not a positive number is reported in a warning and
    ``fallback`` is used, so a typo does not stop the whole suite at conftest import."""
    if raw is None or not raw.strip():
        return fallback
    try:
        seconds = float(raw)
    except ValueError:
        seconds = math.nan
    if not (math.isfinite(seconds) and seconds > 0):
        warnings.warn(
            f"GBX_DOC_TEST_TIMEOUT_S={raw!r} is not a positive number of seconds; "
            f"using {fallback} s.",
            RuntimeWarning,
            stacklevel=2,
        )
        return fallback
    return int(seconds) if seconds == int(seconds) else seconds


DEFAULT_DEADLINE_S = parse_deadline_s(os.environ.get("GBX_DOC_TEST_TIMEOUT_S"))


def _local_jvm_gateway():
    """The py4j gateway to the local JVM, or None (no classic Spark context started)."""
    try:
        from pyspark import SparkContext
    except ImportError:
        return None
    return SparkContext._gateway


class DeadlineGuard:
    """Watchdog for one test. When the test is still running after ``seconds``:

    * the JVM answers a probe (or the test never used Spark, or there is no local JVM
      to probe, as under Spark Connect): every Python stack goes to the captured
      output, so does the JVM thread dump (SIGQUIT) when there is a local JVM, and the
      main thread gets SIGALRM. Python code then raises ``TimeoutError``; if the main
      thread is blocked in a py4j call, the test fails with the py4j-wrapped error
      instead.
    * the local JVM does not answer: every later Spark call would block too, so the
      run ends here with the stacks on the terminal.

    ``stop`` ends the guard before the signal handler is restored: once it returns, no
    signal is sent, and one already on its way is ignored. It always finishes tearing
    down, even if the alarm lands while it waits for the lock.
    """

    def __init__(self, name, seconds, capman, uses_jvm):
        self.name = name
        self.seconds = seconds
        self.capman = capman
        self.uses_jvm = uses_jvm
        self.main_thread = threading.get_ident()
        self.finished = threading.Event()
        self._send = threading.Lock()  # makes "not finished, so signal" atomic
        self._timer = threading.Timer(seconds, self._expire)
        self._timer.daemon = True
        self._previous = None

    def start(self):
        self._previous = signal.signal(signal.SIGALRM, self._raise)
        self._timer.start()

    def stop(self):
        try:
            with self._send:
                self.finished.set()
        finally:
            # If the timer's SIGALRM lands while this waits for the lock, `_raise` runs
            # before `finished` is set and its TimeoutError leaves the block above: the
            # teardown below must still run.
            self.finished.set()
            self._timer.cancel()
            self._timer.join()
            signal.signal(signal.SIGALRM, self._previous)

    def _raise(self, signum, frame):
        if not self.finished.is_set():
            raise TimeoutError(f"{self.name} still running after {self.seconds} s")

    def _expire(self):
        proc = None
        # Under Spark Connect there is no py4j gateway: no local JVM to probe or dump.
        if self.uses_jvm and _local_jvm_gateway() is not None:
            from pyspark import SparkContext

            proc = getattr(SparkContext._gateway, "proc", None)
            answered = []
            probe = threading.Thread(
                target=lambda: answered.append(
                    SparkContext._jvm.java.lang.System.nanoTime()
                ),
                daemon=True,
            )
            probe.start()
            probe.join(_JVM_PROBE_S)
            if self.finished.is_set():
                return
            if not answered:
                self.capman.suspend_global_capture(in_=False)
                sys.stderr.write(
                    f"\n{self.name} still running after {self.seconds} s and the "
                    f"JVM (pid {getattr(proc, 'pid', '?')}) does not respond; "
                    "ending the run.\n"
                )
                faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                sys.stderr.flush()
                if proc is not None:
                    proc.kill()
                os._exit(1)
        if self.finished.is_set():
            return
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        if proc is not None:
            os.kill(proc.pid, signal.SIGQUIT)  # thread dump to the JVM's stdout
            self.finished.wait(2)  # let the dump be written
        with self._send:
            # The test may have finished during the dump: its teardown has then
            # restored the previous SIGALRM handler, and a signal would kill pytest.
            if not self.finished.is_set():
                signal.pthread_kill(self.main_thread, signal.SIGALRM)
