"""Per-test deadline for the doc tests (used by the autouse ``_deadline`` fixture in
conftest.py; ``@pytest.mark.deadline(seconds)`` sets one test's limit).

The amd64 dev image runs under Rosetta on Apple Silicon, where the JVM has been seen
to wedge with every thread blocked: Spark is idle, nothing errors, and the run never
ends. pytest-timeout is not installed in the image, so every doc test runs under this
guard. The default limit is the GBX_DOC_TEST_TIMEOUT_S environment variable (600 when
unset).
"""

import faulthandler
import os
import signal
import sys
import threading

DEFAULT_DEADLINE_S = int(os.environ.get("GBX_DOC_TEST_TIMEOUT_S", "600"))
_JVM_PROBE_S = 15


class DeadlineGuard:
    """Watchdog for one test. When the test is still running after ``seconds``:

    * the JVM answers a probe (or the test never used Spark): every Python stack goes
      to the captured output, so does the JVM thread dump (SIGQUIT), and the main
      thread gets SIGALRM. Python code then raises ``TimeoutError``; if the main
      thread is blocked in a py4j call, the test fails with the py4j-wrapped error
      instead.
    * the JVM does not answer: every later Spark call would block too, so the run ends
      here with the stacks on the terminal.

    ``stop`` ends the guard before the signal handler is restored: once it returns, no
    signal is sent, and one already on its way is ignored.
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
        with self._send:
            self.finished.set()
        self._timer.cancel()
        self._timer.join()
        signal.signal(signal.SIGALRM, self._previous)

    def _raise(self, signum, frame):
        if not self.finished.is_set():
            raise TimeoutError(f"{self.name} still running after {self.seconds} s")

    def _expire(self):
        proc = None
        if self.uses_jvm:
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
