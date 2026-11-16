"""Ask the raster-genie Genie Agents their headline questions in Agent mode and write the evidence.

    python run_questions.py --profile <name> --space live|precomputed|both
                            --live-id <id> [--precomputed-id <id>] --warehouse-id <id> [--runs 3]
                            [--questions Q3,Q1,Q5n] [--out test-logs/raster-genie/evidence.json]

The ids and the warehouse come from the arguments or from the environment (`GENIE_SPACE_ID`,
`GENIE_SPACE_ID_PRECOMPUTED`, `GENIE_WAREHOUSE_ID`); nothing is built in.

For each space and question it runs `--runs` times (one fresh conversation per run) after a pre-warm
of one call per GeoBrix function family, and writes one row per run: space, question, run, status,
seconds, correct, sql. Every question is plain language: no function name, no SQL term, no method.

`correct` is decided from the values the agent's SQL returned (its statements are re-executed on the
warehouse; `--no-rerun` judges the answer text alone) and from the answer text, never from a number
found anywhere in the text:

- Q3 (the fire): the low-or-worse acres (631,994), EVERY one of the five class acres read from its own
  label or row, and the people inside the perimeter (3,849). A class is right within 1% of the
  perimeter (1,054,160 acres), not of the class: High is 48 acres, about 0.005% of the perimeter and 7
  pixels at 80 m, so a few pixels there are below the precision of an 80 m read. Burned total and
  people are right within 1% of their own value.
- Q1 (Hutchinson): the product, tier, resolution and dates of the `coverage` rows (`COVERAGE_TRUTH`).
- Q5n (a question no example answers: the average burn severity and the acres at moderate-low or worse
  in Hutchinson County): the mean dNBR within 0.01 and the acres within 1% of the county area, against
  a direct GeoBrix SQL run with the same method (per-tile ring offset, county clip, 80 m). It also
  records whether the SQL differs from every example (`novel`) and how close it is (`similarity`).

The live space must also show the GeoBrix chain: a statement that calls `gbx_rst_fromfile` and
`gbx_rst_nbr` (comments and string literals do not count) and reads no table or view outside the live
space's `TABLES`. The precomputed space is judged on the numbers only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import agent_spec as spec  # noqa: E402

from databricks.labs.gbx.genie.agent import ask  # noqa: E402

Q3 = "How much land burned in the Smokehouse Creek fire, how severe, and how many people live inside the fire perimeter?"
Q1 = "What imagery and elevation do we have for Hutchinson County, at what quality, and how recent?"
Q5N = (
    "How badly did the Smokehouse Creek fire burn Hutchinson County? What's the average burn severity there, "
    "and how many acres burned at moderate-low or worse?"
)
QUESTIONS = {"Q3": Q3, "Q1": Q1, "Q5n": Q5N}

#: Notebook 01 (run of the current sources), acres per severity class; people inside the perimeter.
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
#: Direct GeoBrix SQL on the warehouse, 80 m: the Smokehouse Creek severity chain (per-tile ring offset from
#: the fire's ring) read through the Hutchinson County geometry and merged. 361,602 valid pixels cover
#: 572,399 of the county's 572,926 acres.
Q5N_TRUTH = {
    "mean_dnbr": 0.0287,
    "acres_moderate_low_or_worse": 3854,
    "acres_low_or_worse": 94923,
    "county_acres": 572926,
}
MEAN_TOLERANCE = 0.01

CLASS_NAMES = ["Unburned", "Low", "Moderate-low", "Moderate-high", "High"]

#: One cheap call per function family, so the first timed run is not a cold start.
PREWARM = [
    "How many Sentinel-2 scenes do we hold?",
    "What is the pixel size of the newest Sentinel-2 image of Carson County?",
    "How green was Carson County in the newest Sentinel-2 image (the average vegetation index)?",
    "How many people live in Carson County?",
]

_CITATION = re.compile(r"\\?\[\[(\d+)\]\(https?://[^)]*\)\\?\]")
_URL = re.compile(r"https?://[^\s)>\]]+")


def clean_text(text: str) -> str:
    """Drop Genie room links and any other URL (they carry the workspace host) from an answer text."""
    return _URL.sub("<url>", _CITATION.sub(r"[\1]", text))


# -- SQL reading: comments and literals do not count --------------------------------------------------------


def strip_sql(sql: str) -> str:
    """The SQL without comments and with string literals emptied, so a name inside either never counts."""
    out = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    out = re.sub(r"--[^\n]*", " ", out)
    return re.sub(r"'(?:[^']|'')*'", "''", out)


def _calls(sql_clean: str, name: str) -> bool:
    return bool(re.search(rf"(?i)\b(?:\w+\.)*{name}\s*\(", sql_clean))


def tables_read(sql: str) -> set[str]:
    """Tables and views a statement reads (its FROM and JOIN names), CTE names excluded, lower-cased."""
    s = re.sub(r"(?i)\b(?:extract|substring|trim)\s*\([^()]*\)", " ", strip_sql(sql))
    ctes = {
        m.group(1).lower()
        for m in re.finditer(r"(?i)(?:\bwith|,)\s*(\w+)\s+as\s*\(", s)
    }
    found = set()
    keyword = r"(?:where|join|left|right|inner|cross|full|on|group|order|lateral|union|limit|having|using|natural|outer)"
    comma = re.compile(rf"(?i)\s*(?:(?:as\s+)?(?!{keyword}\b)\w+)?\s*,\s*([\w.`]+)")
    for m in re.finditer(r"(?i)\b(?:from|join)\s+([\w.`]+)", s):
        names, end = [m.group(1)], m.end()
        while True:  # `FROM a x, b y`: the comma-joined tables
            c = comma.match(s, end)
            if not c or s[: m.start()].count("(") - s[: m.start()].count(")") != s[
                :end
            ].count("(") - s[:end].count(")"):
                break
            names.append(c.group(1))
            end = c.end()
        for raw in names:
            name = raw.replace("`", "").lower()
            if (
                name in ctes
                or name == "lateral"
                or s[m.end() :].lstrip().startswith("(")
                and raw == m.group(1)
            ):
                continue  # a CTE, or a table function such as range(...)
            found.add(name)
    return found


def _allowed_names(tables: list[str]) -> set[str]:
    return {t.lower() for t in tables} | {t.split(".")[-1].lower() for t in tables}


def chain_ok(sql: list[str], tables: list[str] | None = None) -> tuple[bool, str]:
    """The live chain: ONE statement calls `gbx_rst_fromfile` and `gbx_rst_nbr` (as calls, not as text in a
    comment or a literal), and no statement reads a table or view outside `tables` (default: the live space).
    """
    allowed = _allowed_names(tables if tables is not None else spec.TABLES)
    chain = any(
        _calls(strip_sql(s), "gbx_rst_fromfile") and _calls(strip_sql(s), "gbx_rst_nbr")
        for s in sql
    )
    foreign = sorted({t for s in sql for t in tables_read(s)} - allowed)
    return (
        chain and not foreign,
        f"chain={'ok' if chain else 'MISS'} foreign_tables={foreign or 'none'}",
    )


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", strip_sql(sql).lower()).strip()


def novelty(
    sql: list[str], examples: list[tuple[str, str]] | None = None
) -> tuple[bool, float]:
    """(differs from every example, token similarity to the closest example in 0..1)."""
    examples = (
        examples if examples is not None else spec.EXAMPLES_PRECOMPUTED + spec.EXAMPLES
    )
    ex = [_norm(e) for _, e in examples]
    best = 0.0
    same = False
    for s in sql:
        n = _norm(s)
        same = same or n in ex
        toks = set(n.split())
        for e in ex:
            et = set(e.split())
            if toks and et:
                best = max(best, len(toks & et) / len(toks | et))
    return not same, round(best, 3)


# -- number reading ------------------------------------------------------------------------------------------

_NUM = r"(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"


def _num(x) -> float | None:
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _within(a: float, b: float, tol: float = 0.01) -> bool:
    return b != 0 and abs(a - b) / abs(b) <= tol


def class_acres_from_rows(rows: list[dict]) -> dict[str, float]:
    """Acres per class from result rows that carry a severity label and an acres column."""
    out: dict[str, float] = {}
    for r in rows:
        label = next(
            (str(v) for k, v in r.items() if str(k).lower() == "severity"), None
        )
        acres = next((r[k] for k in r if str(k).lower() == "acres"), None)
        if label in CLASS_NAMES and _num(acres) is not None:
            out[label] = _num(acres)
    return out


def class_acres_from_text(text: str) -> dict[str, float]:
    """Acres per class from the answer text, each number taken from its OWN label: a markdown table row
    (the cell under the `acres` column) or a phrase `<label> ... <number> acres`."""
    out: dict[str, float] = {}
    col = None
    for line in text.splitlines():
        if not line.strip().startswith("|"):
            col = None
            continue
        cells = [
            c.strip().strip("*").strip() for c in line.strip().strip("|").split("|")
        ]
        if any(c.lower() == "acres" for c in cells):
            col = next(i for i, c in enumerate(cells) if c.lower() == "acres")
        elif col is not None and col < len(cells):
            label = next((c for c in cells if c in CLASS_NAMES), None)
            if label and _num(cells[col]) is not None:
                out[label] = _num(cells[col])
    for name in CLASS_NAMES:
        if name in out:
            continue
        pat = rf"(?<![-\w]){re.escape(name)}(?![-\w])[^\n|]{{0,60}}?{_NUM}\s*acres"
        m = re.search(pat, text)
        if m:
            out[name] = _num(m.group(1))
    return out


def people_values(text: str, rows: list[dict]) -> list[float]:
    """People counts: result columns named for people, and numbers next to the word people or population."""
    vals = []
    for r in rows:
        for k, v in r.items():
            if (
                re.search(r"(?i)people|population|persons", str(k))
                and _num(v) is not None
            ):
                vals.append(_num(v))
    for m in re.finditer(
        rf"(?i){_NUM}\s*(?:\w+\s+){{0,2}}(?:people|residents|persons)", text
    ):
        vals.append(_num(m.group(1)))
    for m in re.finditer(
        rf"(?i)(?:people|population|residents)[^\n.]{{0,40}}?{_NUM}", text
    ):
        vals.append(_num(m.group(1)))
    return [v for v in vals if v is not None]


def burned_total_values(
    text: str, rows: list[dict], classes: dict[str, float]
) -> list[float]:
    """Low-or-worse acres: a result column `low_or_worse_acres`/`burned`, the sum of the four class rows, or
    a number next to `acres` in a sentence about the burned area."""
    vals = []
    for r in rows:
        for k, v in r.items():
            if re.search(r"(?i)low_or_worse|burned", str(k)) and _num(v) is not None:
                vals.append(_num(v))
    if all(n in classes for n in CLASS_NAMES[1:]):
        vals.append(sum(classes[n] for n in CLASS_NAMES[1:]))
    for m in re.finditer(
        rf"(?i)(?:burn\w*|low severity or worse|low or worse)[^\n|]{{0,80}}?{_NUM}\s*(?:acres|ac\b)",
        text,
    ):
        vals.append(_num(m.group(1)))
    for m in re.finditer(
        rf"(?i){_NUM}\s*(?:acres|ac\b)[^\n|]{{0,60}}?(?:burn\w*|low severity or worse|low or worse)",
        text,
    ):
        vals.append(_num(m.group(1)))
    return [v for v in vals if v is not None]


def check_q3(
    answer_text: str, rows: list[dict], sql: list[str], chain_required: bool = True
) -> tuple[bool, str]:
    """Q3 is correct when the burned total (low or worse), EVERY class acres, each read from its own label or
    row, and the people count are right; with `chain_required` (the live space) the SQL must also be the
    live chain and read nothing outside the live tables."""
    gt = GROUND_TRUTH
    classes = {**class_acres_from_text(answer_text), **class_acres_from_rows(rows)}
    tol = 0.01 * gt["perimeter"]
    wrong = [
        n
        for n in CLASS_NAMES
        if n not in classes or abs(classes[n] - gt["acres"][n]) > tol
    ]
    burned = any(
        _within(v, gt["low_or_worse"])
        for v in burned_total_values(answer_text, rows, classes)
    )
    people = any(_within(v, gt["people"]) for v in people_values(answer_text, rows))
    chain, chain_why = chain_ok(sql)
    ok = burned and not wrong and people and (chain or not chain_required)
    return (
        ok,
        f"burned={'ok' if burned else 'MISS'} classes={'ok' if not wrong else 'MISS ' + ','.join(wrong)} "
        f"people={'ok' if people else 'MISS'} {chain_why}",
    )


def _dates_in(text: str, iso: str) -> bool:
    """The date iso (YYYY-MM-DD) written as ISO, `27 Jan 2024` or `Jan 27, 2024`."""
    y, m, d = iso.split("-")
    mon = [
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
    ][int(m) - 1]
    day = str(int(d))
    low = text.lower()
    return (
        iso in text
        or bool(
            re.search(rf"\b{day}(?:st|nd|rd|th)?\s+{mon}\w*\.?(?:,?\s+{y})?\b", low)
        )
        or bool(re.search(rf"\b{mon}\w*\.?\s+{day}(?:st|nd|rd|th)?\b", low))
    )


def check_q1(
    answer_text: str, rows: list[dict], sql: list[str], chain_required: bool = True
) -> tuple[bool, str]:
    """Q1 is correct when the `coverage` rows the agent read match `COVERAGE_TRUTH` (product, tier, resolution,
    dates), the answer states the imagery's resolution (10 m), tier and both dates, and says that no elevation
    is held; with `chain_required` it also made a live `gbx_rst_fromfile` read and read only live tables.
    """
    problems = []
    cov = {}
    for r in rows:
        if "product" in r and "tier" in r and "best_resolution_m" in r:
            cov[str(r["product"])] = r
    if rows:
        for prod, (tier, res, first, last, assets) in COVERAGE_TRUTH.items():
            r = cov.get(prod)
            if r is None:
                problems.append(f"{prod} row missing")
                continue
            if str(r["tier"]) != tier:
                problems.append(f"{prod} tier {r['tier']}")
            if (
                _num(r["best_resolution_m"]) is None
                or abs(_num(r["best_resolution_m"]) - res) > 0.01 * res
            ):
                problems.append(f"{prod} resolution {r['best_resolution_m']}")
            if str(r.get("first_date")) != first or str(r.get("last_date")) != last:
                problems.append(
                    f"{prod} dates {r.get('first_date')}..{r.get('last_date')}"
                )
    _, res, first, last, _ = COVERAGE_TRUTH["s2_l2a"]
    low = answer_text.lower()
    says_s2 = "sentinel" in low or "s2_l2a" in low
    says_res = bool(re.search(r"(?<![\d.])10(?:\.0+)?\s*(?:m\b|metre|meter)", low))
    says_tier = "enhanced" in low
    says_dates = _dates_in(answer_text, first) and _dates_in(answer_text, last)
    no_elev = bool(
        re.search(
            r"\b(?:no|not|none|without|zero)\b[^.\n]{0,50}\b(?:elevation|dem)\b"
            r"|\b(?:elevation|dem)\b[^.\n]{0,50}\b(?:not held|not available|none|no data|absent|missing)\b",
            low,
        )
    )
    joined = "\n".join(strip_sql(s) for s in sql)
    live = _calls(joined, "gbx_rst_fromfile") and "coverage" in joined.lower()
    foreign = sorted(
        {t for s in sql for t in tables_read(s)} - _allowed_names(spec.TABLES)
    )
    ok = (
        not problems
        and says_s2
        and says_res
        and says_tier
        and says_dates
        and no_elev
        and ((live and not foreign) or not chain_required)
    )
    return (
        ok,
        f"rows={'ok' if rows and not problems else ('none' if not rows else problems)} s2={says_s2} res10m={says_res} "
        f"tier={says_tier} dates={says_dates} no_elevation={no_elev} live_read={live} foreign_tables={foreign or 'none'}",
    )


def _near(text: str, pattern: str, value_re: str, span: int = 80) -> list[float]:
    out = []
    for m in re.finditer(rf"(?i)(?:{pattern})[^\n|]{{0,{span}}}?{value_re}", text):
        v = _num(m.group(m.lastindex))
        if v is not None:
            out.append(v)
    return out


def check_q5n(
    answer_text: str, rows: list[dict], sql: list[str], chain_required: bool = True
) -> tuple[bool, str]:
    """Q5n is correct when the mean dNBR is within 0.01 and the acres at moderate-low or worse within 1% of the
    county area of the direct-SQL truth (from result columns, or from the numbers next to their labels in the
    answer), and, for the live space, the SQL is the live chain."""
    t = Q5N_TRUTH
    means, acres = [], []
    for r in rows:
        for k, v in r.items():
            if re.search(r"(?i)mean|avg|average|dnbr", str(k)) and not re.search(
                r"(?i)acre|px|pixel|count", str(k)
            ):
                means.append(_num(v))
            if re.search(r"(?i)acre", str(k)):
                acres.append(_num(v))
    means += _near(
        answer_text,
        r"mean|average|dnbr|severity index|burn severity",
        r"(-?\d+\.\d{2,4})(?!\d)",
    )
    acres += _near(answer_text, r"moderate.low", rf"{_NUM}\s*acres", span=120)
    acres += [
        _num(m.group(1))
        for m in re.finditer(
            rf"(?i){_NUM}\s*acres[^\n|]{{0,60}}?moderate.low", answer_text
        )
    ]
    means = [v for v in means if v is not None]
    acres = [v for v in acres if v is not None]
    mean_ok = any(abs(v - t["mean_dnbr"]) <= MEAN_TOLERANCE for v in means)
    acres_ok = any(
        abs(v - t["acres_moderate_low_or_worse"]) <= 0.01 * t["county_acres"]
        for v in acres
    )
    chain, chain_why = chain_ok(sql)
    novel, sim = novelty(sql)
    clip = any("hutchinson" in s.lower() for s in sql)
    ok = mean_ok and acres_ok and (chain or not chain_required)
    return (
        ok,
        f"mean={'ok' if mean_ok else 'MISS'} acres={'ok' if acres_ok else 'MISS'} {chain_why} "
        f"novel={novel} similarity={sim} hutchinson_clip={clip}",
    )


CHECKS = {"Q3": check_q3, "Q1": check_q1, "Q5n": check_q5n}


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
            and time.time() - t0 < 900
        ):
            time.sleep(3)
            r = client.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            return []
        cols = [c.name for c in r.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in (r.result.data_array or [])]
    except Exception:
        return []


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
    ap.add_argument("--live-id", default=os.environ.get("GENIE_SPACE_ID"))
    ap.add_argument(
        "--precomputed-id", default=os.environ.get("GENIE_SPACE_ID_PRECOMPUTED")
    )
    ap.add_argument(
        "--warehouse-id",
        default=os.environ.get("GENIE_WAREHOUSE_ID"),
        help="SQL warehouse that re-runs the agent's SQL (not needed with --no-rerun)",
    )
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument(
        "--questions",
        default=None,
        help="comma list of Q3, Q1, Q5n (default: Q3,Q1 on precomputed; Q3,Q1,Q5n on live)",
    )
    ap.add_argument("--no-prewarm", action="store_true")
    ap.add_argument(
        "--no-rerun",
        action="store_true",
        help="judge the answer text only; by default the agent's SQL is re-run and its rows are judged",
    )
    ap.add_argument("--timeout", type=int, default=420)
    ap.add_argument("--out", default="test-logs/raster-genie/evidence.json")
    args = ap.parse_args(argv)

    spaces = {"live": args.live_id, "precomputed": args.precomputed_id}
    chosen = ["live", "precomputed"] if args.space == "both" else [args.space]
    for name in chosen:
        if not spaces[name]:
            ap.error(
                f"the {name} space id is required (--{name}-id or its environment variable)"
            )
    if not args.no_rerun and not args.warehouse_id:
        ap.error("--warehouse-id (or GENIE_WAREHOUSE_ID) is required unless --no-rerun")

    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient(profile=args.profile)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    rows_out: list[dict] = []
    for name in chosen:
        space_id = spaces[name]
        strict = name == "live"  # the live space must show the GeoBrix chain
        keys = (
            [k.strip() for k in args.questions.split(",")]
            if args.questions
            else (["Q3", "Q1", "Q5n"] if strict else ["Q3", "Q1"])
        )
        if not args.no_prewarm:
            for q in PREWARM:
                a = ask(space_id, q, client=client, timeout_s=args.timeout)
                print(
                    f"... [{name}] pre-warm {a['status']} in {a['seconds']} s: {q[:70]}"
                )
        for key in keys:
            q = QUESTIONS[key]
            for run in range(1, args.runs + 1):
                a = ask(space_id, q, client=client, timeout_s=args.timeout)
                text = clean_text("\n".join(a["text"]))
                rows: list[dict] = []
                if not args.no_rerun:
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
                novel, sim = novelty(a["sql"]) if a["sql"] else (None, None)
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
                        "novel": novel,
                        "similarity": sim,
                        "sql": a["sql"],
                        "text": [clean_text(t) for t in a["text"]],
                        "rerun_rows": rows[:30],
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
