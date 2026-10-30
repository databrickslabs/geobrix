"""A watchdog for a submitted bench run: end a stalled run instead of waiting hours for it.

A leg of the bench once sat for hours with no Spark task and nothing to say why. The notebook now
writes a heartbeat (``databricks.labs.gbx.bench.liveness``): the form in progress, how long it has run,
and the stack of the thread running it. ``Watchdog`` judges the beats; ``watch_run`` polls the run
state and the heartbeat, and cancels the run itself when either:

* the heartbeat stopped changing for ``stale_s`` (the driver cannot even write a file), or
* one form has run past ``form_cap_s``.

It then says which form, prints the tail of the stack the last beat held, and returns ``EXIT_STALLED``.
It never retries: the caller decides what to do about a stalled run.

The judging is pure (injectable clock and readers), so it is tested without a workspace; the
``read_beat`` / ``cancel`` / ``get_state`` callables are the only parts that touch the SDK.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Callable, Optional

EXIT_STALLED = 3
DEFAULT_STALE_S = 300.0  # ten missed beats
DEFAULT_FORM_CAP_S = 15 * 60.0
TERMINAL_STATES = ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
STACK_TAIL_LINES = 25


@dataclass
class Stall:
    form: str
    minutes: float
    reason: str
    stack: list

    @property
    def line(self) -> str:
        """The line the launcher prints and a caller greps for: ``STALLED: <form> after <m> min``."""
        return f"STALLED: {self.form} after {self.minutes:.1f} min"


def read_beat(w, path: str) -> Optional[dict]:
    """The heartbeat file as a dict, or None when it is absent, half-written or unreadable (the caller retries)."""
    try:
        return json.loads(w.files.download(path).contents.read().decode("utf-8"))
    except Exception:  # noqa: BLE001 - absent before the notebook starts; half-written now and then
        return None


class Watchdog:
    """Judge the beats of one run. ``observe(beat)`` returns a ``Stall`` or None; call it on every poll.

    Time comes from this process's own clock, never from the notebook's: a beat counts as new when its
    ``seq`` changes, so a skewed cluster clock cannot fake a stall or hide one. Before the first beat is
    seen nothing is judged (cluster start and the wheel install take minutes; the run's own
    ``timeout_seconds`` bounds that).
    """

    def __init__(
        self,
        *,
        form_cap_s: float = DEFAULT_FORM_CAP_S,
        stale_s: float = DEFAULT_STALE_S,
        clock: Callable[[], float] = time.monotonic,
        token: Optional[str] = None,
    ):
        self.form_cap_s = form_cap_s
        self.stale_s = stale_s
        self._clock = clock
        self.token = token  # this launch's id: a beat another launch wrote (same path) is not this run's
        self._seq = None
        self._seen_at = 0.0
        self._last: dict = {}

    @property
    def last_beat(self) -> dict:
        return self._last

    def observe(self, beat: Optional[dict]) -> Optional[Stall]:
        now = self._clock()
        if beat is not None and self.token is not None and beat.get("token") != self.token:
            beat = None  # a file an earlier launch left at the same path
        if beat is not None and beat.get("seq") != self._seq:
            self._seq, self._seen_at, self._last = beat.get("seq"), now, beat
        if not self._last or self._last.get("state") in ("done", "failed"):
            return None
        quiet = now - self._seen_at
        form = self._last.get("form")
        if form:
            # elapsed at the last beat, plus the time since it (the form kept running while no beat came)
            ran = float(self._last.get("form_elapsed_s") or 0.0) + quiet
            if ran > self.form_cap_s:
                return Stall(
                    form, ran / 60, f"form past its {self.form_cap_s / 60:.0f} min cap", self._last.get("stack", [])
                )
        if quiet > self.stale_s:
            what = form or f"after {self._last.get('forms_done', 0)} forms (between forms)"
            return Stall(
                what, quiet / 60, f"heartbeat stale for {quiet / 60:.1f} min (the driver is not responding)",
                self._last.get("stack", []),
            )
        return None


_FAULT_DUMP = re.compile(r"(Thread 0x[0-9a-f]+ \(most recent call first\):.*?)(?=\n\s*\n|\Z)", re.S)


def faulthandler_tail(w, run_id: int, lines: int = STACK_TAIL_LINES) -> str:
    """The tail of a faulthandler dump in the run's exported cell output, or "" (best effort).

    A dump goes to the driver's stderr, which is not always in the cell output; the heartbeat's own stack
    is the reliable one, this is a second witness when the output carries one.
    """
    try:
        from databricks.sdk.service.jobs import ViewsToExport

        views = w.jobs.export_run(run_id=run_id, views_to_export=ViewsToExport.ALL).views or []
        text = "\n".join(v.content or "" for v in views)
    except Exception:  # noqa: BLE001
        return ""
    dumps = _FAULT_DUMP.findall(text)
    return "\n".join(dumps[-1].splitlines()[-lines:]) if dumps else ""


def report_stall(stall: Stall, *, extra_dump: str = "", print_fn=print) -> None:
    print_fn(stall.line)
    print_fn(f"  reason : {stall.reason}")
    if stall.stack:
        print_fn(f"  driver stack at the last heartbeat (innermost {min(len(stall.stack), STACK_TAIL_LINES)} lines):")
        for ln in stall.stack[-STACK_TAIL_LINES:]:
            print_fn("    " + ln)
    else:
        print_fn("  (no stack in the last heartbeat)")
    if extra_dump:
        print_fn("  faulthandler dump from the run's cell output:")
        for ln in extra_dump.splitlines():
            print_fn("    " + ln)


def watch_run(
    *,
    get_state: Callable[[], tuple],
    read_beat_fn: Callable[[], Optional[dict]],
    cancel: Callable[[], None],
    on_stall_dump: Callable[[], str] = lambda: "",
    on_poll: Callable[[str], None] = lambda lc: None,
    watchdog: Optional[Watchdog] = None,
    poll_secs: float = 5.0,
    cancel_wait_s: float = 180.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    print_fn=print,
) -> tuple:
    """Poll until the run ends. Returns ``("finished", life_cycle_state)`` or ``("stalled", Stall)``.

    ``get_state()`` returns ``(life_cycle_state, result_state)`` as strings. On a stall the run is cancelled
    (and waited for, up to ``cancel_wait_s``), the stall is reported, and no retry follows.
    """
    wd = watchdog or Watchdog()
    while True:
        lc, _ = get_state()
        if lc in TERMINAL_STATES:
            return "finished", lc
        on_poll(lc)
        stall = wd.observe(read_beat_fn())
        if stall:
            print_fn(f"{stall.reason}: cancelling the run")
            cancel()
            deadline = clock() + cancel_wait_s
            while clock() < deadline and get_state()[0] not in TERMINAL_STATES:
                sleep(poll_secs)
            report_stall(stall, extra_dump=on_stall_dump(), print_fn=print_fn)
            return "stalled", stall
        sleep(poll_secs)
