"""Liveness of a bench run: a heartbeat file the launcher can read while the notebook works.

A leg of the bench used to print nothing and sink its rows at the end, so "still no rows" could not
tell a slow form from a dead driver. ``Heartbeat`` writes a small JSON file every few seconds from a
daemon thread: the form being timed, how long it has run, how many are done, and the stack of the
thread that is running it. The launcher's watchdog reads the file (``examples/tests/_run_watchdog.py``):
a file that stops changing means the driver is stuck (a thread that cannot run, a call that holds the
GIL); a form past its cap means the form is. Either way the stack in the last beat names where.

Plain Python and file writes only: no Spark conf, no ``_jvm``, no ``sparkContext``, so it is safe on
Serverless and on a Spark Connect session.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
from typing import Optional

STACK_LINES = 60  # the stack lines kept in a beat (the innermost)


class Heartbeat:
    """Write ``path`` (JSON) every ``interval`` seconds and on every form change, until ``stop()``.

    ``start()`` must be called from the thread that runs the work: that thread's stack is the one
    recorded. ``begin(label, ...)`` / ``end()`` mark the form in progress.

    ``begin`` names the form and, for a ladder, the scale it is at, and may carry the cap that applies to
    that scale (``cap_s``): the watchdog judges a form against the cap in its own beat.

    Beats are serialised by a lock (the work thread and the daemon thread both write) and land through a
    temp file in the same directory and ``os.replace``, so a reader sees the previous beat or the new one,
    never half of one. Where the replace is refused the beat is written in place and a reader retries.

    The notebook sees the same progress: a form begun with a ``set_name`` prints a tagged line (``[LEVELS <set>] ...``)
    at its start, its end, and every ``print_every`` seconds while it runs (its own cadence, apart from the JSON
    ``interval``; a print writes no beat). A form begun without a ``set_name`` prints nothing.
    """

    def __init__(
        self,
        path: str,
        interval: float = 30.0,
        *,
        phase: str = "",
        token: str = "",
        print_every: float = 60.0,
    ):
        self.path = path
        self.interval = float(interval)
        self.print_every = float(print_every)
        self.phase = phase
        self.token = token  # the launcher's id of this launch: a reader ignores a file another launch left
        self._seq = 0
        self._form: Optional[str] = None
        self._form_started = 0.0
        self._index = 0
        self._total = 0
        self._done = 0
        self._scale = 1
        self._cap_s: Optional[float] = None
        self._set_name: Optional[str] = None
        self._rows: Optional[int] = None
        self._name = ""
        self._work_thread: Optional[int] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()  # one beat file write at a time
        self._state = "idle"

    # -- the work thread's side -------------------------------------------------------------
    def start(self) -> "Heartbeat":
        self._work_thread = threading.get_ident()
        self._state = "running"
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        except OSError:
            pass  # the first beat reports it
        self.beat()
        self._thread = threading.Thread(
            target=self._loop, name="gbx-heartbeat", daemon=True
        )
        self._thread.start()
        return self

    def begin(
        self,
        label: str,
        index: int = 0,
        total: int = 0,
        *,
        scale: int = 1,
        cap_s: Optional[float] = None,
        rows: Optional[int] = None,
        name: Optional[str] = None,
        set_name: Optional[str] = None,
    ) -> None:
        """``rows`` and ``name`` (the form without its scale) are for the printed lines; ``set_name`` turns them on."""
        with self._lock:
            self._form, self._form_started = label, time.monotonic()
            self._index, self._total = index, total
            self._scale, self._cap_s = scale, cap_s
            self._set_name, self._rows, self._name = set_name, rows, name or label
        self.beat()
        self._say("START", f"(cap {self._cap_text(cap_s)})")

    def end(
        self,
        *,
        form_done: bool = True,
        median_s: Optional[float] = None,
        status: Optional[str] = None,
    ) -> None:
        """Mark the form (or one scale of it) finished; ``form_done=False`` for a scale that is not the form's last.

        ``median_s`` (the form's median, when it has one) and ``status`` (when it did not end ``ok``) are printed.
        """
        elapsed = time.monotonic() - self._form_started if self._form else 0.0
        tail = f"elapsed {elapsed:.1f}s"
        if median_s is not None:
            tail += f" median {median_s:.2f}s"
        if status and status != "ok":
            tail += f" status {status}"
        self._say("DONE", tail)
        with self._lock:
            self._form = None
            self._cap_s = None
            self._set_name = None
            if form_done:
                self._done += 1
        self.beat()

    def stop(self, state: str = "done") -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._state = state
        self.beat()

    # -- the beat ---------------------------------------------------------------------------
    def _loop(self) -> None:
        """One wake-up per ``min(interval, print_every)``: the JSON beat and the printed line each keep their own cadence."""
        tick = max(min(self.interval, self.print_every), 0.01)
        last_beat = last_print = time.monotonic()
        while not self._stop.wait(tick):
            now = time.monotonic()
            if now - last_beat >= self.interval - 1e-3:
                last_beat = now
                self.beat()
            if now - last_print >= self.print_every - 1e-3:
                last_print = now
                self._say_running()

    @staticmethod
    def _cap_text(cap_s: Optional[float]) -> str:
        return "none" if cap_s is None else f"{cap_s:g}s"

    def _say(self, what: str, tail: str) -> None:
        """Print one tagged line for the form in progress (nothing when it was begun without a ``set_name``).

        A print that fails is reported on stderr and never raised, as ``beat`` does for its write.
        """
        with self._lock:
            if not self._set_name or not self._form:
                return
            rows = "?" if self._rows is None else self._rows
            line = (
                f"[LEVELS {self._set_name}] form {self._index}/{self._total} {self._name} "
                f"scale {self._scale}x rows {rows} {what} {tail}"
            )
        try:
            print(line, flush=True)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - a closed stdout must not end the heartbeat thread (and its JSON beat)
            try:
                print(
                    f"heartbeat print failed ({type(exc).__name__}: {exc})",
                    file=sys.stderr,
                    flush=True,
                )
            except Exception:  # noqa: BLE001 - nowhere left to say it
                pass

    def _say_running(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._form_started
            cap = self._cap_s
        self._say("RUNNING", f"elapsed {elapsed:.0f}s of cap {self._cap_text(cap)}")

    def snapshot(self) -> dict:
        with self._lock:
            form, started = self._form, self._form_started
            index, total, done = self._index, self._total, self._done
            scale, cap_s = self._scale, self._cap_s
            self._seq += 1
            seq = self._seq
        return {
            "seq": seq,
            "state": self._state,
            "phase": self.phase,
            "token": self.token,
            "form": form,
            "form_elapsed_s": round(time.monotonic() - started, 1) if form else 0.0,
            "scale": scale,
            "cap_s": cap_s,
            "form_index": index,
            "form_total": total,
            "forms_done": done,
            "beat_at": time.time(),
            "interval_s": self.interval,
            "stack": self._work_stack(),
        }

    def _work_stack(self) -> list:
        frame = (
            sys._current_frames().get(self._work_thread) if self._work_thread else None
        )
        if frame is None:
            return []
        lines = [
            ln
            for chunk in traceback.format_stack(frame)
            for ln in chunk.rstrip().splitlines()
        ]
        return lines[-STACK_LINES:]

    def beat(self) -> None:
        """Write one beat. A failed write is printed and never raised: the heartbeat must not kill the run."""
        try:
            with self._write_lock:
                self._write(json.dumps(self.snapshot()))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the work matters more than its heartbeat
            print(
                f"heartbeat write failed ({type(exc).__name__}: {exc})",
                file=sys.stderr,
                flush=True,
            )

    def _write(self, text: str) -> None:
        tmp = f"{self.path}.{os.getpid()}.tmp"
        try:
            with open(tmp, "w") as fh:
                fh.write(text)
            os.replace(tmp, self.path)
        except OSError:
            # a filesystem that refuses the replace: write in place (a reader retries on a half-written file)
            with open(self.path, "w") as fh:
                fh.write(text)
            try:
                os.remove(tmp)
            except OSError:
                pass
