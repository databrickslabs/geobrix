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

Each shard runs in its own notebook run (``dbutils.notebook.run``): its own execution context, so its own Python process
and its own Spark session (confs and temp views), on the cluster's driver JVM. That is what a job task gave each shard
before. The shard notebook keeps its watchdog (--shard-cap-min, --stall-min); the runner adds the outer timeout, records a
shard that did not finish, and goes on with the next one.
"""

import json
import os
import random
import re
import time
from typing import Callable, Dict, List, Optional

# The exceptions of a lost Delta commit (docs: "Conflict exceptions"), by class name or error class.
CONFLICT = re.compile(
    r"Concurrent\w*Exception|DELTA_CONCURRENT|MetadataChangedException|DELTA_METADATA_CHANGED|ProtocolChangedException|DELTA_PROTOCOL_CHANGED"
)


def queue_order(expected: Dict[str, float]) -> List[str]:
    """The shards in the order the runners take them: longest expected first, then by name (a stable order)."""
    return [s for s, _ in sorted(expected.items(), key=lambda kv: (-float(kv[1]), kv[0]))]


def _lit(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


class DirQueue:
    """The queue as claim files in ``root``: a claim is ``os.open(<slug>.claim, O_CREAT | O_EXCL)``, which exactly one creator
    wins on a POSIX filesystem. For the tests and for one node's local disk; not for a UC Volume (see the module docstring)."""

    def __init__(self, root: str, order: List[str]):
        self.root, self.order = root, list(order)
        os.makedirs(root, exist_ok=True)

    def claim(self, who: str, offset: int = 0) -> Optional[str]:
        free = [s for s in self.order if not os.path.exists(os.path.join(self.root, s + ".claim"))]
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
        return None


class DeltaQueue:
    """The queue as a Delta table (slug, rank, expected_min, owner, claimed_at). ``sql`` runs one statement and returns its rows
    as tuples (``lambda q: [tuple(r) for r in spark.sql(q).collect()]`` on a cluster)."""

    def __init__(self, sql: Callable[[str], list], table: str, *, attempts: int = 60, sleep: Callable[[float], None] = time.sleep,
                 rng: Callable[[], float] = random.random):
        self.sql, self.table, self.attempts, self.sleep, self.rng = sql, table, attempts, sleep, rng
        self.conflicts = 0

    @staticmethod
    def create_sql(table: str, order: List[str], expected: Dict[str, float]) -> str:
        rows = ", ".join(f"({_lit(s)}, {i}, {float(expected.get(s, 0.0))!r})" for i, s in enumerate(order))
        return (f"CREATE OR REPLACE TABLE {table} AS SELECT slug, CAST(rank AS INT) AS rank, CAST(expected_min AS DOUBLE) AS expected_min, "
                f"CAST(NULL AS STRING) AS owner, CAST(NULL AS TIMESTAMP) AS claimed_at FROM VALUES {rows} AS v(slug, rank, expected_min)")

    def claim(self, who: str, offset: int = 0) -> Optional[str]:
        errors = 0
        for attempt in range(self.attempts):
            free = [r[0] for r in self.sql(f"SELECT slug FROM {self.table} WHERE owner IS NULL ORDER BY rank")]
            if not free:
                return None
            slug = free[offset % len(free)] if attempt == 0 and offset else free[0]
            try:
                rows = self.sql(f"UPDATE {self.table} SET owner = {_lit(who)}, claimed_at = current_timestamp() "
                                f"WHERE slug = {_lit(slug)} AND owner IS NULL")
            except Exception as exc:  # noqa: BLE001 - a lost commit is retried; anything else a few times, then raised
                if CONFLICT.search(f"{type(exc).__name__}: {exc}"):
                    self.conflicts += 1
                else:
                    errors += 1
                    if errors > 3:
                        raise
                self.sleep(0.3 + self.rng() * 1.2)
                continue
            if rows and int(rows[0][0]) == 1:
                owner = self.sql(f"SELECT owner FROM {self.table} WHERE slug = {_lit(slug)}")
                if owner and owner[0][0] == who:  # read back: the row is ours
                    return slug
        raise RuntimeError(f"no claim after {self.attempts} attempts on {self.table} ({self.conflicts} lost commits)")


def run_shard(run_notebook: Callable[[], str], timeout_s: int) -> dict:
    """Run one shard's notebook and return its result (the JSON the shard notebook exits with). A run that fails, is killed by
    its watchdog or outlasts ``timeout_s`` comes back as a result with a reason, never as an exception."""
    try:
        out = run_notebook()
    except Exception as exc:  # noqa: BLE001 - the shard is over either way; the runner goes on
        msg = " | ".join(x.strip() for x in str(exc).strip().splitlines()[:3])[:400] or type(exc).__name__
        if re.search(r"time[ds]? ?out|TimeoutException", msg, re.I):
            return {"rc": None, "reason": f"no result within {timeout_s} s (outer timeout): {msg}"}
        return {"rc": None, "reason": f"the shard's notebook run ended without a result: {type(exc).__name__}: {msg}"}
    try:
        res = json.loads(out) if out else None
    except ValueError:
        res = None
    return res if isinstance(res, dict) else {"rc": None, "reason": f"the shard's notebook returned no result ({str(out)[:120]!r})"}


def run_loop(queue, who: str, execute: Callable[[str], dict], *, clock: Callable[[], float] = time.time, offset: int = 0,
             on_change: Optional[Callable[[dict], None]] = None) -> dict:
    """Claim and run shards until the queue is empty. ``execute(slug)`` runs one shard and returns its result; an exception
    from it is recorded as the shard's reason and the loop goes on. Returns the runner's record: when it started and ended,
    and per shard the claim time, the start, the end, rc and reason."""
    note = on_change or (lambda rec: None)
    rec = {"runner": who, "start": clock(), "end": None, "current": None, "done": False, "shards": [], "error": ""}
    note(rec)
    first = True
    while True:
        t_claim = clock()
        try:
            slug = queue.claim(who, offset=offset if first else 0)
        except Exception as exc:  # noqa: BLE001 - a runner that cannot claim stops; the others go on
            rec["error"] = f"claim failed: {type(exc).__name__}: {str(exc)[:300]}"
            break
        first = False
        if slug is None:
            break
        t0 = clock()
        rec["current"] = slug
        note(rec)
        try:
            out = execute(slug) or {}
        except Exception as exc:  # noqa: BLE001 - a failed shard never stops its runner
            out = {"rc": None, "reason": f"{type(exc).__name__}: {str(exc)[:300]}"}
        t1 = clock()
        rec["shards"].append({"slug": slug, "claim_s": round(t0 - t_claim, 3), "start": t0, "end": t1, "rc": out.get("rc"),
                              "reason": out.get("reason") or "", "pid": out.get("pid"), "isolation": out.get("isolation"),
                              **{k: out.get(k) for k in ("passed", "failed", "errors", "skipped")}})
        rec["current"] = None
        note(rec)
    rec["end"], rec["done"] = clock(), True
    note(rec)
    return rec


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
