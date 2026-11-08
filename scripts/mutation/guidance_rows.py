"""Rows and the summary of a full-mutant guidance run (gbx:test:mutation-databricks).

Pure functions over what mutmut leaves behind: the exit code of every mutant, the first failing test of each killed
one and the time each took. Standard library only, so the cluster, the launcher and the tests all load it.

Status of a mutant, from mutmut's exit code:

* killed     a test failed (1, or 3 for an internal pytest error);
* survived   every selected test passed (0);
* timeout    a test run hit the time limit (-24, 24, 36, 152, 255);
* segfault   the test process crashed (-11, -9);
* absent     no selected test reaches the mutant (33, 5) or it was never run (None, 34): nothing can kill it;
* unchecked  the run was stopped (a cap) before the mutant was tried.

The kill rate leaves `absent` and `unchecked` out: a mutant no test reaches says the selection does not cover the
line, which the survivor list does not.
"""

import collections
import json
import re
from typing import Dict, Iterable, List, Optional

STATUSES = ("killed", "survived", "timeout", "segfault", "absent", "unchecked")
DETECTED = ("killed", "timeout", "segfault")

# the Delta table's columns, in order: (name, Spark SQL type)
COLUMNS = (
    ("run_id", "STRING"),
    ("run_ts", "TIMESTAMP"),
    ("git_sha", "STRING"),
    ("family", "STRING"),
    ("module", "STRING"),
    ("function", "STRING"),
    ("mutant", "STRING"),
    ("status", "STRING"),
    ("exit_code", "INT"),
    ("killing_test", "STRING"),
    ("wall_s", "DOUBLE"),
    ("diff", "STRING"),
    ("tests", "STRING"),
    ("node_type", "STRING"),
    ("purpose", "STRING"),
)


def status_of(code: Optional[int]) -> str:
    if code in (1, 3):
        return "killed"
    if code == 0:
        return "survived"
    if code in (-24, 24, 36, 152, 255):
        return "timeout"
    if code in (-11, -9):
        return "segfault"
    if code is None:
        return "unchecked"
    if code in (33, 5, 34):
        return "absent"
    return "killed" if code else "survived"  # an unlisted non-zero exit is a failed run: a kill


def function_of(key: str) -> str:
    """The function a mutant sits in: 'x_foo__mutmut_3' -> 'foo'; a method 'xǁCǁbar__mutmut_1' -> 'C.bar'."""
    last = re.sub(r"__mutmut_\d+$", "", key.rsplit(".", 1)[-1])
    m = re.match(r"^xǁ([^ǁ]*)ǁ_?(.*)$", last)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return re.sub(r"^x_?", "", last)


def module_of(key: str, modules: Iterable[str]) -> str:
    """The module of a mutant key among the run's modules ('pyvx/_validity'): the key holds its dotted path."""
    dotted = key.rsplit(".", 1)[0]
    best = ""
    for m in modules:
        tail = m.replace("/", ".")
        if (dotted == tail or dotted.endswith("." + tail)) and len(m) > len(best):
            best = m
    return best or dotted.split(".")[-1]


def rows_from_run(
    codes: Dict[str, Optional[int]],
    *,
    modules: List[str],
    run: dict,
    killers: Optional[Dict[str, str]] = None,
    durations: Optional[Dict[str, float]] = None,
    diffs: Optional[Dict[str, str]] = None,
) -> List[dict]:
    """One dict per mutant (the columns of COLUMNS). `run` holds run_id, run_ts, git_sha, family, tests, node_type, purpose."""
    killers = killers or {}
    durations = durations or {}
    diffs = diffs or {}
    rows = []
    for key in sorted(codes):
        code = codes[key]
        status = status_of(code)
        rows.append(
            {
                "run_id": run["run_id"],
                "run_ts": run["run_ts"],
                "git_sha": run["git_sha"],
                "family": run["family"],
                "module": module_of(key, modules),
                "function": function_of(key),
                "mutant": key,
                "status": status,
                "exit_code": code,
                "killing_test": killers.get(key) if status in DETECTED else None,
                "wall_s": durations.get(key),
                "diff": diffs.get(key),
                "tests": run.get("tests", ""),
                "node_type": run.get("node_type", ""),
                "purpose": run.get("purpose", ""),
            }
        )
    return rows


def kill_rate(counts: Dict[str, int]) -> Optional[float]:
    """Percent of the mutants some test could reach that a test killed; None when none was reachable."""
    detected = sum(counts.get(s, 0) for s in DETECTED)
    tried = detected + counts.get("survived", 0)
    return 100.0 * detected / tried if tried else None


def per_module(rows: List[dict]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = collections.defaultdict(lambda: {s: 0 for s in STATUSES})
    for r in rows:
        out[r["module"]][r["status"]] += 1
    return dict(sorted(out.items()))


def summary_md(rows: List[dict], run: dict, trend: Optional[Dict[str, float]] = None) -> str:
    """The run's summary.md: a line per module, the survivors grouped by function, the tests that killed the most.

    `trend` is {module: kill rate of the run before} (the table's previous run), shown as a delta when given.
    """
    mods = per_module(rows)
    lines = [
        f"# Mutation guidance run {run['run_id']}",
        "",
        f"- purpose: {run.get('purpose', '')}",
        f"- git sha: {run['git_sha']}  family: {run['family']}  node: {run.get('node_type', '')} ({run.get('cores', '?')} vCPU)",
        f"- tests: `{run.get('tests', '')}`",
        f"- wall: {run.get('wall_s', '?')} s  mutants: {len(rows)}  stopped early: {run.get('stopped') or 'no'}",
        "",
        "## Kill rate per module",
        "",
        "Kill rate = (killed + timeout + segfault) / (those + survived). Absent: no selected test reaches the mutant.",
        "",
        "| module | mutants | killed | survived | timeout | segfault | absent | unchecked | kill rate |"
        + (" vs before |" if trend else ""),
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|" + ("---:|" if trend else ""),
    ]
    for m, c in mods.items():
        kr = kill_rate(c)
        cell = "n/a" if kr is None else f"{kr:.1f}%"
        extra = ""
        if trend:
            prev = trend.get(m)
            extra = " " + ("new |" if prev is None or kr is None else f"{kr - prev:+.1f} |")
        lines.append(
            f"| {m} | {sum(c.values())} | {c['killed']} | {c['survived']} | {c['timeout']} | {c['segfault']} | {c['absent']} | {c['unchecked']} | {cell} |"
            + extra
        )
    tot = {s: sum(c[s] for c in mods.values()) for s in STATUSES}
    kr = kill_rate(tot)
    lines += ["", f"All modules: {len(rows)} mutants, kill rate {'n/a' if kr is None else f'{kr:.1f}%'}.", ""]

    lines += ["## Survivors by function", ""]
    surv = collections.defaultdict(list)
    for r in rows:
        if r["status"] == "survived":
            surv[(r["module"], r["function"])].append(r)
    if not surv:
        lines.append("None.")
    for (m, fn), rs in sorted(surv.items(), key=lambda kv: (kv[0][0], -len(kv[1]), kv[0][1])):
        lines.append(f"- `{m}` `{fn}`: {len(rs)} survivor(s)")
        for r in rs:
            idx = re.search(r"__mutmut_(\d+)$", r["mutant"])
            lines.append(f"  - mutant {idx.group(1) if idx else r['mutant']}")
            if r.get("diff"):
                for ln in r["diff"].strip().splitlines():
                    if ln.startswith(("+", "-")) and not ln.startswith(("+++", "---")):
                        lines.append(f"    {ln}")
    absent = collections.Counter((r["module"], r["function"]) for r in rows if r["status"] == "absent")
    lines += ["", "## Functions no selected test reaches", ""]
    if not absent:
        lines.append("None.")
    for (m, fn), n in sorted(absent.items()):
        lines.append(f"- `{m}` `{fn}`: {n} mutant(s)")

    kills = collections.Counter(r["killing_test"] for r in rows if r["killing_test"])
    lines += ["", "## Tests that killed the most mutants (first failing test of each)", ""]
    if not kills:
        lines.append("No killing test was recorded.")
    for t, n in kills.most_common(15):
        lines.append(f"- {n}  {t}")
    lines.append("")
    return "\n".join(lines)


def read_jsonl(path) -> list:
    with open(path) as fh:
        return [json.loads(ln) for ln in fh if ln.strip()]


def killers_from_jsonl(path) -> Dict[str, str]:
    """{mutant key: the first failing test}. The first line for a mutant wins: pytest runs with -x."""
    out: Dict[str, str] = {}
    try:
        for rec in read_jsonl(path):
            out.setdefault(rec["mutant"], rec["test"])
    except FileNotFoundError:
        pass
    return out


# ---------------------------------------------------------------------------------------------- the Delta table


def table_ddl(table: str) -> str:
    cols = ",\n  ".join(f"{n} {t}" for n, t in COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING DELTA "
        "COMMENT 'One row per full-mutant guidance run x mutant (gbx:test:mutation-databricks). "
        "Guidance for the test-gap backlog, never a gate.'"
    )


def write_delta(spark, rows: List[dict], table: str) -> int:
    """Append the run's rows to `table` (made when absent). The run's earlier rows are removed first, so a rerun of
    the same run id does not double them; mergeSchema lets a column added to COLUMNS later append to an older table.
    Spark Connect safe: SQL and DataFrame calls only."""
    import datetime as _dt

    if not rows:
        return 0
    spark.sql(table_ddl(table))
    run_ids = sorted({r["run_id"] for r in rows})
    for rid in run_ids:
        spark.sql(f"DELETE FROM {table} WHERE run_id = '{rid.replace(chr(39), '')}'")

    def cell(name, typ, v):
        if v is None:
            return None
        if typ == "TIMESTAMP" and isinstance(v, str):
            return _dt.datetime.strptime(v[:19], "%Y-%m-%dT%H:%M:%S")
        return {"INT": int, "DOUBLE": float}.get(typ, lambda x: x)(v)

    data = [tuple(cell(n, t, r.get(n)) for n, t in COLUMNS) for r in rows]
    ddl = ", ".join(f"{n} {t}" for n, t in COLUMNS)
    df = spark.createDataFrame(data, schema=ddl)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
    return len(data)


def previous_rates(spark, table: str, family: str, run_id: str) -> Dict[str, float]:
    """{module: kill rate of the latest earlier run of the family that has the module}; {} when there is none."""
    try:
        got = spark.sql(
            f"SELECT module, run_id, MAX(run_ts) AS ts, "
            f"SUM(CASE WHEN status IN ('killed','timeout','segfault') THEN 1 ELSE 0 END) AS d, "
            f"SUM(CASE WHEN status = 'survived' THEN 1 ELSE 0 END) AS s "
            f"FROM {table} WHERE family = '{family.replace(chr(39), '')}' AND run_id <> '{run_id.replace(chr(39), '')}' "
            f"GROUP BY module, run_id"
        ).collect()
    except Exception:  # noqa: BLE001 - no table yet, or no access: no trend
        return {}
    best: Dict[str, tuple] = {}
    for r in got:
        if (r["d"] + r["s"]) and (r["module"] not in best or r["ts"] > best[r["module"]][0]):
            best[r["module"]] = (r["ts"], 100.0 * r["d"] / (r["d"] + r["s"]))
    return {m: v[1] for m, v in best.items()}
