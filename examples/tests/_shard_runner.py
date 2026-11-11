"""The claim-and-run loop of gbx:test:databricks' runner tasks (stdlib only: the runner notebook carries this file's source).

A run submits one runner task per slot (clusters x --max-parallel). Each runner claims the next shard from a queue shared by
every runner of the pool (longest expected first), runs it, and claims again until the queue is empty. No slot waits behind
a lane of its own while another slot's shards are still queued.

The queue on a cluster is a Delta table (DeltaQueue). A claim is ``UPDATE ... SET owner = <runner> WHERE slug = <s> AND owner
IS NULL``; Delta commits are atomic and two concurrent writes that change the same row conflict, so one commit wins and the
other fails with a Concurrent*Exception (docs: lakehouse/acid, optimizations/isolation/row-level-concurrency). A UC Volume's
FUSE mount documents no exclusive create and no atomic rename, so the claim does not rely on files there. DirQueue (one file
made with O_CREAT|O_EXCL per claim) is the same claim on a POSIX filesystem: the tests use it, and so could runners that
share one node's local disk.

A heavy chunk (the GDAL engine's oracle tables, or a file or table the timing history records as long) slows its neighbours on the
node's one driver by several times, so at most one heavy chunk runs on a cluster at a time: HeavyGate, a lock on the node's local
disk that the runners of one cluster share. A runner that cannot take the gate claims only the light chunks, and waits (WAIT)
when only heavy ones are left.

Each shard runs in its own notebook run (``dbutils.notebook.run``): its own execution context, so its own Python process
and its own Spark session (confs and temp views), on the cluster's driver JVM. That is what a job task gave each shard
before. The shard notebook keeps its watchdog (--shard-cap-min, --stall-min); the runner adds the outer timeout, records a
shard that did not finish, and goes on with the next one.

Fail fast: each chunk that ends leaves a one-line result (``results/<slug>.json``: the counts and its first failure) in the run's
Volume folder at once. ``stop_rule`` reads those, in the order the chunks ended, against the run's policy (the first K chunks all
failed, N failed chunks, M failed tests). The launcher applies it and cancels the run; a runner applies it too before each claim, and
claims nothing more once it fires (or once the launcher's ``stop.json`` is there), so a run stops even when its launcher is gone.
"""

import json
import os
import random
import re
import time
from typing import Callable, Dict, Iterable, List, Optional

# The exceptions of a lost Delta commit (docs: "Conflict exceptions"), by class name or error class.
CONFLICT = re.compile(
    r"Concurrent\w*Exception|DELTA_CONCURRENT|MetadataChangedException|DELTA_METADATA_CHANGED|ProtocolChangedException|DELTA_PROTOCOL_CHANGED"
)


class _Wait:
    """What ``claim`` returns when shards are queued but this runner may take none of them yet (only heavy ones are left, and a heavy
    chunk runs on its cluster)."""

    def __repr__(self):
        return "WAIT"


WAIT = _Wait()


class HeavyGate:
    """One heavy chunk at a time on a cluster: an exclusive ``flock`` on a file of the node's local disk, which every runner of the
    cluster opens. Released with the process, so a crashed runner never holds it."""

    def __init__(self, path: str):
        self.path, self.fd = path, None
        self.error = ""  # why the lock file cannot be made: the runner then goes on ungated (a slower node, never a stuck one)

    def try_acquire(self) -> bool:
        import fcntl

        if self.fd is not None or self.error:
            return True
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        except OSError as exc:
            self.error = f"{type(exc).__name__}: {exc}"[:300]
            return True
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        self.fd = fd
        return True

    def release(self) -> None:
        if self.fd is not None:
            os.close(self.fd)  # closing the descriptor drops the lock
            self.fd = None


def queue_order(expected: Dict[str, float]) -> List[str]:
    """The shards in the order the runners take them: longest expected first, then by name (a stable order)."""
    return [s for s, _ in sorted(expected.items(), key=lambda kv: (-float(kv[1]), kv[0]))]


def _lit(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


class DirQueue:
    """The queue as claim files in ``root``: a claim is ``os.open(<slug>.claim, O_CREAT | O_EXCL)``, which exactly one creator
    wins on a POSIX filesystem. For the tests and for one node's local disk; not for a UC Volume (see the module docstring)."""

    def __init__(self, root: str, order: List[str], heavy: Iterable[str] = ()):
        self.root, self.order, self.heavy = root, list(order), set(heavy)
        os.makedirs(root, exist_ok=True)

    def claim(self, who: str, offset: int = 0, heavy_ok: bool = True):
        free = [s for s in self.order if not os.path.exists(os.path.join(self.root, s + ".claim"))]
        if not heavy_ok:
            light = [s for s in free if s not in self.heavy]
            if free and not light:
                return WAIT
            free = light
        if offset and free:  # a first claim starts at the runner's own index, so N runners that start together do not collide
            free = free[offset % len(free):] + free[: offset % len(free)]
        for slug in free:
            try:
                fd = os.open(os.path.join(self.root, slug + ".claim"), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                continue
            with os.fdopen(fd, "w") as f:
                f.write(who)
            return slug
        if not heavy_ok and any(not os.path.exists(os.path.join(self.root, s + ".claim")) for s in self.order if s in self.heavy):
            return WAIT  # other runners took the light ones first; a heavy chunk is still queued
        return None


class DeltaQueue:
    """The queue as a Delta table (slug, rank, expected_min, owner, claimed_at, and the chunk's own notebook run: child_run_id,
    child_job_id, which the chunk writes when it starts). ``sql`` runs one statement and returns its rows
    as tuples (``lambda q: [tuple(r) for r in spark.sql(q).collect()]`` on a cluster)."""

    def __init__(self, sql: Callable[[str], list], table: str, *, attempts: int = 60, sleep: Callable[[float], None] = time.sleep,
                 rng: Callable[[], float] = random.random, heavy: Iterable[str] = (), clock: Callable[[], float] = time.time,
                 budget_s: float = 600.0):
        self.sql, self.table, self.attempts, self.sleep, self.rng = sql, table, attempts, sleep, rng
        self.heavy = set(heavy)  # the plan says which chunks are heavy; the table holds the claims only
        self.conflicts = 0
        # a lost commit (another runner's claim, or a chunk noting its run id, won the table) is retried until this much time has
        # gone, not counted against ``attempts``: with many runners the losses are many, and each is progress for someone else
        self.clock, self.budget_s = clock, budget_s

    @staticmethod
    def create_sql(table: str, order: List[str], expected: Dict[str, float]) -> str:
        rows = ", ".join(f"({_lit(s)}, {i}, {float(expected.get(s, 0.0))!r})" for i, s in enumerate(order))
        return (f"CREATE OR REPLACE TABLE {table} AS SELECT slug, CAST(rank AS INT) AS rank, CAST(expected_min AS DOUBLE) AS expected_min, "
                f"CAST(NULL AS STRING) AS owner, CAST(NULL AS TIMESTAMP) AS claimed_at, CAST(NULL AS STRING) AS child_run_id, "
                f"CAST(NULL AS STRING) AS child_job_id FROM VALUES {rows} AS v(slug, rank, expected_min)")

    def claim(self, who: str, offset: int = 0, heavy_ok: bool = True):
        errors, attempt, lost = 0, 0, 0
        t0 = self.clock()
        while attempt < self.attempts:
            free = [r[0] for r in self.sql(f"SELECT slug FROM {self.table} WHERE owner IS NULL ORDER BY rank")]
            if not free:
                return None
            if not heavy_ok:
                light = [s for s in free if s not in self.heavy]
                if not light:
                    return WAIT
                free = light
            if attempt == 0 and not lost:
                slug = free[offset % len(free)] if offset else free[0]
            else:  # a retry takes a random free row: the runners that lost the same commit do not all fight for free[0] again
                slug = free[min(len(free) - 1, int(self.rng() * len(free)))]
            try:
                rows = self.sql(f"UPDATE {self.table} SET owner = {_lit(who)}, claimed_at = current_timestamp() "
                                f"WHERE slug = {_lit(slug)} AND owner IS NULL")
            except Exception as exc:  # noqa: BLE001 - a lost commit is retried; anything else a few times, then raised
                if CONFLICT.search(f"{type(exc).__name__}: {exc}"):
                    self.conflicts += 1
                    lost += 1
                    if self.clock() - t0 > self.budget_s:
                        break
                    self.sleep(min(5.0, 0.3 * (1 + lost // 10)) + self.rng() * 1.2)
                    continue
                errors += 1
                if errors > 3:
                    raise
                attempt += 1
                self.sleep(0.3 + self.rng() * 1.2)
                continue
            attempt += 1
            if rows and int(rows[0][0]) == 1:
                owner = self.sql(f"SELECT owner FROM {self.table} WHERE slug = {_lit(slug)}")
                if owner and owner[0][0] == who:  # read back: the row is ours
                    return slug
        raise RuntimeError(f"no claim after {attempt} attempts and {lost} lost commits in {self.clock() - t0:.0f} s on {self.table}")


WATCHDOG_LABEL = "killed by watchdog: "


def watchdog_reason(why) -> str:
    """The reason a shard the watchdog stopped carries, in the shard's result, the runner's record and the summary alike."""
    why = str(why or "").strip()
    return why if why.startswith(WATCHDOG_LABEL) else WATCHDOG_LABEL + why


def heartbeat_watchdog(path: str) -> str:
    """What the shard's last heartbeat (``hb/<slug>.json``) says its watchdog fired for: "" when it did not, or the file is missing
    or unreadable. The watchdog writes the heartbeat before it interrupts the test process, and before it kills it."""
    try:
        with open(path) as f:
            return str(json.load(f).get("watchdog") or "")
    except (OSError, ValueError, AttributeError):
        return ""


def read_json_file(path: str):
    """The JSON object in ``path``; None when it is absent or not (yet) whole."""
    try:
        with open(path) as f:
            got = json.load(f)
        return got if isinstance(got, dict) else None
    except (OSError, ValueError):
        return None


def run_shard(run_notebook: Callable[[], str], timeout_s: int, watchdog: Optional[Callable[[], str]] = None,
              result: Optional[Callable[[], Optional[dict]]] = None) -> dict:
    """Run one shard's notebook and return its result (the JSON the shard notebook exits with). A run that fails, is killed by
    its watchdog or outlasts ``timeout_s`` comes back as a result with a reason, never as an exception.

    A shard whose tests failed writes its result file and then fails its own notebook run, so that run's status tells the truth:
    ``result()`` reads that file, and it is the result (``child_failed`` keeps the platform's message).

    ``watchdog()`` returns what the shard's own watchdog fired for ("" when it did not). The watchdog kills a process that does not stop (SIGKILL,
    exit 137), and Databricks reports any such kill as "Execution ran out of memory" or "kernel unresponsive": a run the watchdog
    ended is reported as that (``killed by watchdog: <why>``), never as the platform's guess."""
    try:
        out = run_notebook()
    except Exception as exc:  # noqa: BLE001 - the shard is over either way; the runner goes on
        msg = " | ".join(x.strip() for x in str(exc).strip().splitlines()[:3])[:400] or type(exc).__name__
        got = _result(result)
        if got is not None:
            return dict(got, child_failed=msg)
        fired = _fired(watchdog)
        if fired:
            return {"rc": None, "reason": watchdog_reason(fired), "platform_message": msg}
        if re.search(r"time[ds]? ?out|TimeoutException", msg, re.I):
            return {"rc": None, "reason": f"no result within {timeout_s} s (outer timeout): {msg}"}
        return {"rc": None, "reason": f"the shard's notebook run ended without a result: {type(exc).__name__}: {msg}"}
    try:
        res = json.loads(out) if out else None
    except ValueError:
        res = None
    if isinstance(res, dict):
        return res
    fired = _fired(watchdog)
    if fired:
        return {"rc": None, "reason": watchdog_reason(fired)}
    return {"rc": None, "reason": f"the shard's notebook returned no result ({str(out)[:120]!r})"}


def _result(result: Optional[Callable[[], Optional[dict]]]) -> Optional[dict]:
    try:
        return result() if result else None
    except Exception:  # noqa: BLE001 - no file: the platform's message stands
        return None


def _fired(watchdog: Optional[Callable[[], str]]) -> str:
    try:
        return (watchdog() if watchdog else "") or ""
    except Exception:  # noqa: BLE001 - the heartbeat is a hint; the platform's message stands without it
        return ""


def run_loop(queue, who: str, execute: Callable[[str], dict], *, clock: Callable[[], float] = time.time, offset: int = 0,
             on_change: Optional[Callable[[dict], None]] = None, heavy: Iterable[str] = (), gate: Optional[HeavyGate] = None,
             wait_s: float = 10.0, sleep: Callable[[float], None] = time.sleep, stop: Optional[Callable[[], str]] = None,
             on_result: Optional[Callable[[str, dict, float, float], None]] = None, limit: int = 0,
             hold: Optional[Callable[[], bool]] = None, hold_s: float = 15.0) -> dict:
    """Claim and run shards until the queue is empty. ``execute(slug)`` runs one shard and returns its result; an exception
    from it is recorded as the shard's reason and the loop goes on. Returns the runner's record: when it started and ended,
    and per shard the claim time, the start, the end, rc and reason.

    ``heavy`` names the chunks that run one at a time per cluster, and ``gate`` is the cluster's lock for them: a runner takes the
    gate before it claims, and keeps it only for a heavy chunk. Without the gate it claims light chunks, and waits (``wait_s``)
    when only heavy ones are left.

    ``stop()`` is asked before each claim: a reason ("" to go on) ends the loop and is kept as the record's ``stopped``.
    ``on_result(slug, result, start, end)`` is called the moment a shard ends (the one-line result the launcher polls). ``limit``
    caps the shards this runner takes (the smoke task takes one). ``hold()``, asked before the first claim every ``hold_s``,
    keeps the runner waiting while it says True (the ramp: the runners past the first few wait for clean results)."""
    heavy = set(heavy)
    gated = bool(heavy and gate)
    rec = {"runner": who, "start": clock(), "end": None, "current": None, "done": False, "shards": [], "error": "", "stopped": ""}

    def note(r):  # the record is a heartbeat and a report: one that cannot be saved (a Volume write) stops nothing
        try:
            (on_change or (lambda _r: None))(r)
        except Exception as exc:  # noqa: BLE001
            rec["note_errors"] = rec.get("note_errors", 0) + 1
            rec["note_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"

    try:
        return _loop(queue, who, execute, rec, note, clock=clock, offset=offset, heavy=heavy, gate=gate if gated else None, wait_s=wait_s,
                     sleep=sleep, stop=stop, on_result=on_result, limit=limit, hold=hold, hold_s=hold_s)
    finally:  # whatever ends the loop (an error, an interrupt), the cluster's heavy gate is not left held
        if gated:
            gate.release()


def _loop(queue, who, execute, rec, note, *, clock, offset, heavy, gate, wait_s, sleep, stop, on_result, limit, hold, hold_s) -> dict:
    gated = gate is not None
    note(rec)
    first = True
    while hold is not None and not rec["stopped"]:
        why = _ask(stop)
        if why:
            rec["stopped"] = why
        elif _held(hold):
            sleep(hold_s)
            continue
        break
    rec["held_s"] = round(clock() - rec["start"], 1)
    while not rec["stopped"]:
        if limit and len(rec["shards"]) >= limit:
            break
        why = _ask(stop)
        if why:
            rec["stopped"] = why
            break
        t_claim = clock()
        held = gate.try_acquire() if gated else True
        if gated and gate.error and not rec.get("gate_error"):
            rec["gate_error"] = f"no heavy gate ({gate.error}): heavy chunks run ungated on this runner"
        try:
            slug = queue.claim(who, offset=offset if first else 0, **({"heavy_ok": held} if gated else {}))
        except Exception as exc:  # noqa: BLE001 - a runner that cannot claim stops; the others go on
            if gated:
                gate.release()
            rec["error"] = f"claim failed: {type(exc).__name__}: {str(exc)[:300]}"
            break
        if slug is WAIT:
            sleep(wait_s)  # only heavy chunks are left and one runs on this cluster
            continue
        first = False
        if gated and (slug is None or slug not in heavy):
            gate.release()  # the gate is for a heavy chunk only
        if slug is None:
            break
        t0 = clock()
        rec["current"] = slug
        note(rec)
        try:
            out = execute(slug) or {}
        except Exception as exc:  # noqa: BLE001 - a failed shard never stops its runner
            out = {"rc": None, "reason": f"{type(exc).__name__}: {str(exc)[:300]}"}
        finally:
            if gated and slug in heavy:
                gate.release()
        t1 = clock()
        rec["shards"].append({"slug": slug, "claim_s": round(t0 - t_claim, 3), "start": t0, "end": t1, "rc": out.get("rc"),
                              "reason": out.get("reason") or "", "pid": out.get("pid"), "isolation": out.get("isolation"),
                              "heavy": slug in heavy, "child": out.get("child") or {},
                              **{k: out.get(k) for k in ("passed", "failed", "errors", "skipped")}})
        rec["current"] = None
        if on_result is not None:
            try:
                on_result(slug, out, t0, t1)
            except Exception as exc:  # noqa: BLE001 - the live result is a report; the run goes on without it (never a runner error)
                rec.setdefault("result_errors", []).append(f"result not written for {slug}: {type(exc).__name__}: {str(exc)[:200]}")
        note(rec)
    rec["end"], rec["done"] = clock(), True
    note(rec)
    return rec


def _held(hold: Callable[[], bool]) -> bool:
    try:
        return bool(hold())
    except Exception:  # noqa: BLE001 - a ramp check that cannot read its inputs lets the runner go
        return False


def ramp_open(results: List[dict], need: int, queued: int) -> bool:
    """Whether a runner held back by the ramp may start: ``need`` chunks have passed, or no chunk is left to wait for."""
    passed = sum(1 for r in results if r and not r.get("failed_chunk"))
    return passed >= need or queued <= 0


def _ask(stop: Optional[Callable[[], str]]) -> str:
    try:
        return str((stop() if stop else "") or "")
    except Exception:  # noqa: BLE001 - a stop check that cannot read its inputs never stops a run
        return ""


# ---------------------------------------------------------------------------------------------- fail fast

# The default policy (``--first-failed``, ``--max-failed-chunks``, ``--max-failures``, ``--maxfail``, ``--start-chunks``): stop the run
# when the first 2 chunks to end both failed, or 3 chunks failed, or 20 tests failed; stop each chunk's pytest at its 3rd failure; and
# start 2 runners only, the others once 2 chunks have passed.
POLICY = {"first_k": 2, "max_failed_chunks": 3, "max_failures": 20, "maxfail": 3, "off": False, "start_chunks": 2}


def chunk_failed(res: dict) -> bool:
    """Whether a chunk's result fails the run: a test failed or errored (collection included), pytest ended with an error code, or
    the chunk ended without finishing (a watchdog stop, no result). A chunk that collected nothing (rc 5) is not a failure here."""
    if not res:
        return True
    if res.get("rc") == 5 and not (res.get("failed") or res.get("errors") or res.get("reason")):
        return False
    if res.get("overrun") and not (res.get("failed") or res.get("errors")):
        return False  # stopped at 2x its estimate: a timing miss (a lower bound for the next plan), not a test failure
    return bool(res.get("reason")) or res.get("rc") not in (0,) or bool(res.get("failed")) or bool(res.get("errors"))


def result_line(slug: str, res: dict, start: float, end: float, runner: str = "") -> dict:
    """The one-line result a chunk leaves the moment it ends (``results/<slug>.json``): counts, the verdict for fail fast, and its
    first failure (test id and a short message), or why it ended without a result."""
    first = res.get("first_failure") or {}
    if not first and res.get("reason"):
        first = {"id": "", "msg": str(res.get("reason"))[:300]}
    return {"slug": slug, "runner": runner, "start": start, "end": end, "rc": res.get("rc"), "reason": res.get("reason") or "",
            "failed_chunk": chunk_failed(res), "maxfail_hit": bool(res.get("maxfail_hit")), "smoke": bool(res.get("smoke")),
            "overrun": bool(res.get("overrun")), "child": res.get("child") or {}, "first_failure": first, **{k: int(res.get(k) or 0) for k in ("passed", "failed", "errors", "skipped", "total")}}


def stop_rule(results: List[dict], policy: Optional[dict] = None) -> str:
    """Why the run should stop now ("" to go on), from the one-line results of the chunks that ended (any order: they are taken in
    the order they ended). Rules, in this order: a smoke chunk failed; the first ``first_k`` chunks to end all failed; ``max_failed_chunks``
    chunks failed; ``max_failures`` tests failed or errored over the chunks. A rule set to 0 is off; ``off`` turns all but the smoke off."""
    p = dict(POLICY, **(policy or {}))
    done = sorted((r for r in results if r), key=lambda r: (float(r.get("end") or 0), str(r.get("slug"))))
    bad = [r for r in done if r.get("failed_chunk")]
    smoke = [r for r in bad if r.get("smoke")]
    if smoke:
        return f"the smoke chunk {smoke[0].get('slug')} failed"
    if p.get("off"):
        return ""
    k = int(p.get("first_k") or 0)
    if k and len(done) >= k and all(r.get("failed_chunk") for r in done[:k]):
        return f"the first {k} chunks to finish all failed"
    n = int(p.get("max_failed_chunks") or 0)
    if n and len(bad) >= n:
        return f"{len(bad)} chunks failed (--max-failed-chunks {n})"
    m = int(p.get("max_failures") or 0)
    tests = sum(int(r.get("failed") or 0) + int(r.get("errors") or 0) for r in done)
    if m and tests >= m:
        return f"{tests} tests failed or errored (--max-failures {m})"
    return ""


def read_results(folder: str) -> List[dict]:
    """The one-line results under ``folder`` (the node reads the Volume's FUSE mount); [] while there are none. A file being
    written (not yet JSON) is skipped until the next read."""
    out = []
    try:
        names = sorted(os.listdir(folder))
    except OSError:
        return out
    for n in names:
        if not n.endswith(".json"):
            continue
        try:
            with open(os.path.join(folder, n)) as f:
                out.append(json.load(f))
        except (OSError, ValueError):
            continue
    return out


def slot_metrics(records: List[dict]) -> dict:
    """What the slots did, from the runners' records: per slot the busy time, the time it waited for a shard while the queue
    still held one (claims and hand-offs), and the idle time at the end (after its last shard, while other slots still ran);
    and for the pool the chunks, the longest and mean shard, the span, the ideal wall (sum over the slots) and the tail."""
    recs = [r for r in records if r and r.get("start") is not None]
    shards = [s for r in recs for s in r.get("shards", [])]
    if not recs or not shards:
        return {}
    run_start = min(r["start"] for r in recs)
    run_end = max((r.get("end") or max([s["end"] for s in r["shards"]] or [r["start"]])) for r in recs)
    last_claim = max(s["start"] for s in shards)  # after this the queue was empty
    per = []
    for r in sorted(recs, key=lambda r: (len(str(r["runner"])), str(r["runner"]))):  # r2 before r10
        end = r.get("end") or max([s["end"] for s in r["shards"]] or [r["start"]])
        busy = sum(s["end"] - s["start"] for s in r["shards"])
        upto = max(r["start"], last_claim)  # the window in which the queue still held a shard
        busy_q = sum(max(0.0, min(s["end"], upto) - max(s["start"], r["start"])) for s in r["shards"])
        per.append({"runner": r["runner"], "shards": len(r["shards"]), "busy_s": int(round(busy)),
                    "waited_s": int(round(max(0.0, (upto - r["start"]) - busy_q))), "idle_end_s": int(round(max(0.0, run_end - end))),
                    "start_offset_s": int(round(r["start"] - run_start)), "end": end, "error": r.get("error", "")})
    dur = [s["end"] - s["start"] for s in shards]
    span = run_end - run_start
    ends = [p.pop("end") for p in per]
    return {
        "chunks": len(shards), "slots": len(recs), "task_longest_s": int(max(dur)), "task_mean_s": int(sum(dur) / len(dur)),
        "task_sum_s": int(sum(dur)), "span_s": int(span), "ideal_s": int(sum(dur) / len(recs)), "tail_s": int(max(ends) - min(ends)),
        "idle_slot_s": int(max(0.0, len(recs) * span - sum(dur))), "waited_s": sum(p["waited_s"] for p in per),
        "per_slot": per,
    }
