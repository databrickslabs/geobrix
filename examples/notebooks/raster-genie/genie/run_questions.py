"""Ask the slice Genie Agents their headline questions in Agent mode and write the evidence.

    python run_questions.py --profile <name> --space live|precomputed|both
                            [--live-id <id>] [--precomputed-id <id>] [--runs 3]
                            [--out test-logs/raster-genie/evidence.json]

For each space and question it runs `--runs` times (one fresh conversation per run) after a pre-warm
of one call per GeoBrix function family, and writes one row per run: space, question, run, status,
seconds, correct, sql. `correct` is decided by `check_q3` and `check_q1`: the numbers in the agent's
answer are compared with the direct-SQL ground truth of notebook 01.

- The live space must also show the GeoBrix chain: its Q3 SQL contains `gbx_rst_fromfile` and
  `gbx_rst_nbr` and never `burn_severity_summary`; its Q1 SQL makes a live `gbx_rst_fromfile` read.
- The precomputed space is judged on the numbers only (it may read the saved table).
- Per-class acres are compared within 1% of the perimeter (1,054,160 acres), not of the class. The
  High class is 48 acres, about 0.005% of the perimeter and 7 pixels of 6,400 m2 at 80 m: a miss of
  a few pixels there is below the precision of an 80 m read, so the per-class check needs 4 of the 5
  classes within 1% of the perimeter. Low or worse acres, and people, are compared within 1% of their
  own value; a correct Q3 needs (low or worse, or the per-class check) and people.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from databricks.labs.gbx.genie.agent import ask  # noqa: E402

Q3 = "How much land burned in the Smokehouse Creek fire, how severe, and how many people live in the burned area?"
Q1 = "What imagery and elevation do we have for Hutchinson County, at what quality, and how recent?"
QUESTIONS = {"Q3": Q3, "Q1": Q1}

#: Notebook 01 (run 156927278790426), acres per severity class; people inside the perimeter.
GROUND_TRUTH = {
    "acres": {
        "Unburned": 421906,
        "Low": 582790,
        "Moderate-low": 46779,
        "Moderate-high": 2377,
        "High": 48,
    },
    "low_or_worse": 631994,
    "perimeter": 1054160,
    "people": 3849,
}
#: `coverage` rows of Hutchinson County: product -> (tier, best_resolution_m, first_date, last_date, assets).
COVERAGE_TRUTH = {
    "s2_l2a": ("enhanced", 10.0, "2024-01-27", "2024-03-12", 6),
    "population": ("enhanced", 92.77, "2020-01-01", "2020-01-01", 1),
}

#: One cheap call per function family, so the first timed run is not a cold start.
PREWARM = [
    "How many Sentinel-2 scenes are in the rasters table?",
    "What is the pixel width and height of the newest Sentinel-2 raster, read for Carson County at 160 m?",
    "What is the mean NDVI of the newest Sentinel-2 scene over Carson County, read at 160 m?",
    "How many people live in Carson County?",
]


def _within(a: float, b: float, tol: float = 0.01) -> bool:
    return b != 0 and abs(a - b) / abs(b) <= tol


def _numbers(text: str) -> list[float]:
    out = []
    for m in re.finditer(
        r"(?<![\w.])(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)(?![\w])", text
    ):
        try:
            out.append(float(m.group(1).replace(",", "")))
        except ValueError:
            pass
    return out


def check_q3(
    answer_text: str, rows: list[dict], sql: list[str], chain_required: bool = True
) -> tuple[bool, str]:
    """Q3 is correct when the burned acres (low or worse, or the per class acres) and the people count
    each match the ground truth within 1%. With `chain_required` (the live space) the agent must also
    have computed them with the GeoBrix chain: its SQL reads the COGs (`gbx_rst_fromfile`), takes
    `gbx_rst_nbr`, and never touches `burn_severity_summary`. `answer_text` is what the agent said;
    `rows` are the rows of its SQL re-run (may be empty).
    """
    nums = _numbers(answer_text)
    for r in rows:
        for v in r.values():
            try:
                nums.append(float(v))
            except (TypeError, ValueError):
                pass
    gt = GROUND_TRUTH
    burned = any(_within(n, gt["low_or_worse"]) for n in nums)
    # per class acres within 1% of the perimeter, not of the class (see the module docstring)
    per_class = (
        sum(
            any(abs(n - a) <= 0.01 * gt["perimeter"] for n in nums)
            for a in gt["acres"].values()
        )
        >= 4
    )
    people = any(_within(n, gt["people"]) for n in nums)
    joined = "\n".join(sql).lower()
    chain = (
        "gbx_rst_fromfile" in joined
        and "gbx_rst_nbr" in joined
        and "burn_severity_summary" not in joined
    )
    ok = (burned or per_class) and people and (chain or not chain_required)
    return (
        ok,
        f"burned={'ok' if burned or per_class else 'MISS'} people={'ok' if people else 'MISS'} "
        f"chain={'ok' if chain else 'MISS'}",
    )


def check_q1(
    answer_text: str, rows: list[dict], sql: list[str], chain_required: bool = True
) -> tuple[bool, str]:
    """Q1 is correct when the answer reports Sentinel-2 imagery at 10 m, enhanced tier, 2024-01-27 to
    2024-03-12, and says that no elevation is held. With `chain_required` (the live space) its SQL
    must also read `coverage` and make a live `gbx_rst_fromfile` read of an asset."""
    low = answer_text.lower()
    nums = _numbers(answer_text)
    s2 = "sentinel" in low or "s2_l2a" in low
    res = any(_within(n, 10.0, 0.0) for n in nums)
    dates = (
        "2024-01-27" in answer_text
        and "2024-03-12" in answer_text
        or ("jan" in low and "mar" in low and "2024" in low)
    )
    tier = "enhanced" in low
    no_elev = bool(
        re.search(
            r"no (?:elevation|dem)|not (?:hold|have|include)|elevation[^.]{0,40}(?:not|none|no )",
            low,
        )
    )
    joined = "\n".join(sql).lower()
    live = "gbx_rst_fromfile" in joined and "coverage" in joined
    ok = s2 and res and dates and tier and no_elev and (live or not chain_required)
    return (
        ok,
        f"s2={s2} res10={res} dates={dates} tier={tier} no_elevation={no_elev} live_read={live}",
    )


CHECKS = {"Q3": check_q3, "Q1": check_q1}


def rerun_sql(sql: str, warehouse_id: str, client) -> list[dict]:
    """Run the agent's SQL on the warehouse and return its rows as dicts (empty on any failure)."""
    from databricks.sdk.service.sql import StatementState

    try:
        r = client.statement_execution.execute_statement(
            statement=sql, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        t0 = time.time()
        while (
            r.status.state in (StatementState.PENDING, StatementState.RUNNING)
            and time.time() - t0 < 600
        ):
            time.sleep(3)
            r = client.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            return []
        cols = [c.name for c in r.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in (r.result.data_array or [])]
    except Exception:
        return []


#: The live space (computes everything from the COGs); the precomputed id is given with --precomputed-id.
LIVE_ID = "01f1e19c817b18e4a36c61acc8ebc60d"


def summarize(rows: list[dict]) -> list[dict]:
    """One row per (space, question): runs, correct, median and max seconds."""
    out = []
    keys = dict.fromkeys((r["space"], r["question"]) for r in rows)
    for space, question in keys:
        rs = [r for r in rows if (r["space"], r["question"]) == (space, question)]
        secs = sorted(r["seconds"] for r in rs)
        n = len(secs)
        median = (
            secs[n // 2] if n % 2 else round((secs[n // 2 - 1] + secs[n // 2]) / 2, 2)
        )
        out.append(
            {
                "space": space,
                "question": question,
                "runs": n,
                "correct": sum(bool(r["correct"]) for r in rs),
                "median_s": median,
                "max_s": secs[-1],
            }
        )
    return out


def main(argv=None, client=None) -> list[dict]:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile")
    ap.add_argument("--space", choices=["live", "precomputed", "both"], default="live")
    ap.add_argument("--live-id", default=LIVE_ID)
    ap.add_argument("--precomputed-id")
    ap.add_argument(
        "--warehouse-id",
        default="de2336249255be4e",
        help="warehouse used to re-run the agent's SQL",
    )
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--questions", default="Q3,Q1", help="comma list of Q3, Q1")
    ap.add_argument("--no-prewarm", action="store_true")
    ap.add_argument(
        "--rerun",
        action="store_true",
        help="also re-run the agent's SQL on the warehouse and judge its rows (adds the SQL time)",
    )
    ap.add_argument("--timeout", type=int, default=420)
    ap.add_argument("--out", default="test-logs/raster-genie/evidence.json")
    args = ap.parse_args(argv)

    spaces = {"live": args.live_id, "precomputed": args.precomputed_id}
    chosen = ["live", "precomputed"] if args.space == "both" else [args.space]
    for name in chosen:
        if not spaces[name]:
            ap.error(f"--{name}-id is required for --space {args.space}")

    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient(profile=args.profile)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    rows_out: list[dict] = []
    for name in chosen:
        space_id = spaces[name]
        strict = name == "live"  # the live space must show the GeoBrix chain
        if not args.no_prewarm:
            for q in PREWARM:
                a = ask(space_id, q, client=client, timeout_s=args.timeout)
                print(
                    f"... [{name}] pre-warm {a['status']} in {a['seconds']} s: {q[:70]}"
                )
        for key in [k.strip() for k in args.questions.split(",")]:
            q = QUESTIONS[key]
            for run in range(1, args.runs + 1):
                a = ask(space_id, q, client=client, timeout_s=args.timeout)
                text = "\n".join(a["text"])
                rows: list[dict] = []
                if args.rerun:
                    for statement in a["sql"]:
                        rows += rerun_sql(statement, args.warehouse_id, client)
                ok, why = (
                    (False, a["error"] or "no answer")
                    if a["status"] != "COMPLETED"
                    else CHECKS[key](text, rows, a["sql"], strict)
                )
                print(
                    f"... [{name}] {key} run {run}: {a['status']} in {a['seconds']} s, correct={ok} ({why})"
                )
                rows_out.append(
                    {
                        "space": name,
                        "space_id": space_id,
                        "question": key,
                        "run": run,
                        "status": a["status"],
                        "seconds": a["seconds"],
                        "correct": ok,
                        "why": why,
                        "sql": a["sql"],
                        "text": a["text"],
                        "rerun_rows": rows[:20],
                        "error": a["error"],
                        "conversation_id": a["conversation_id"],
                    }
                )
                out.write_text(
                    json.dumps(rows_out, indent=2, default=str)
                )  # keep partial runs
    print(f"... wrote {out}")
    for r in summarize(rows_out):
        print(
            f"... [{r['space']}] {r['question']}: {r['correct']}/{r['runs']} correct, "
            f"median {r['median_s']} s, max {r['max_s']} s"
        )
    return rows_out


if __name__ == "__main__":
    main()
