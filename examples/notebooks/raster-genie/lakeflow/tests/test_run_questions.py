"""run_questions: the gates judge each number from its own label or row, and the live chain as real calls."""

import re
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve()
sys.path.insert(0, str(_HERE.parents[2] / "genie"))
import agent_spec as spec  # noqa: E402
import run_questions as rq  # noqa: E402

G = "geospatial_catalog.geobrix_uc."
T = "geospatial_catalog.raster_genie."
LIVE_SQL = [
    f"SELECT {G}gbx_rst_nbr({G}gbx_rst_fromfile(r.file, 'GTiff', a.geom, 'EPSG:4326', 80.0), 4, 6) "
    f"FROM {T}rasters r JOIN {T}aois a ON st_intersects(r.footprint, a.geom)"
]

TABLE = """| class_id | severity | pixels | km2 | acres | pct |
| --- | --- | --- | --- | --- | --- |
| 1 | Unburned | 265407 | 1707.4 | 421906.0 | 40.0 |
| 2 | Low | 366614 | 2358.5 | 582790.0 | 55.3 |
| 3 | Moderate-low | 29427 | 189.3 | 46779.0 | 4.4 |
| 4 | Moderate-high | 1495 | 9.6 | 2377.0 | 0.2 |
| 5 | High | 30 | 0.2 | 48.0 | 0.0 |
"""
HEAD = "About 632,000 acres burned at low severity or worse. About 3,850 people live inside the fire perimeter.\n"


def test_the_correct_answer_passes_from_text_and_from_rows():
    ok, why = rq.check_q3(HEAD + TABLE, [], LIVE_SQL)
    assert ok, why
    rows = [
        {"severity": n, "acres": a, "low_or_worse_acres": 631994}
        for n, a in rq.GROUND_TRUTH["acres"].items()
    ] + [{"people": 3849}]
    assert rq.check_q3("done", rows, LIVE_SQL)[0]


def test_swapped_labels_fail():
    swapped = TABLE.replace(
        "| Unburned | 265407 | 1707.4 | 421906.0",
        "| Unburned | 265407 | 1707.4 | 582790.0",
    ).replace(
        "| Low | 366614 | 2358.5 | 582790.0", "| Low | 366614 | 2358.5 | 421906.0"
    )
    ok, why = rq.check_q3(HEAD + swapped, [], LIVE_SQL)
    assert not ok and "classes=MISS" in why


def test_a_small_class_six_times_too_big_with_no_burned_total_fails():
    bad = TABLE.replace("46779.0", "280674.0")
    answer = "Roughly 3,850 people live inside the perimeter.\n" + bad
    ok, why = rq.check_q3(answer, [], LIVE_SQL)
    assert not ok and "Moderate-low" in why and "burned=MISS" in why


def test_a_year_or_a_people_count_cannot_fill_a_class():
    answer = (
        "About 632,000 acres burned; 3,849 people live inside. Sentinel-2 scenes of 2024, "
        "2,377 people were counted elsewhere, 48 scenes."
    )
    ok, why = rq.check_q3(answer, [], LIVE_SQL)
    assert not ok and "classes=MISS" in why  # no class has its own label and number


def test_every_class_is_needed():
    no_high = "\n".join(line for line in TABLE.splitlines() if "| High |" not in line)
    assert not rq.check_q3(HEAD + no_high, [], LIVE_SQL)[0]


def test_people_must_be_near_the_word_people():
    answer = TABLE + "About 632,000 acres burned. The scene id 3849 was used."
    assert not rq.check_q3(answer, [], LIVE_SQL)[0]


def test_a_miss_on_high_alone_is_accepted_within_one_percent_of_the_perimeter():
    assert rq.check_q3(HEAD + TABLE.replace("48.0", "90.0"), [], LIVE_SQL)[0]
    assert not rq.check_q3(HEAD + TABLE.replace("48.0", "20000.0"), [], LIVE_SQL)[0]


def test_a_table_lookup_fails_the_live_gate_and_passes_the_numbers_only_gate():
    lookup = [
        f"SELECT * FROM {T}burn_severity_summary",
        f"SELECT {G}gbx_rst_fromfile(f) FROM {T}rasters",
    ]
    assert not rq.check_q3(HEAD + TABLE, [], lookup)[0]
    assert not rq.check_q3(HEAD + TABLE, [], [])[0]
    assert rq.check_q3(HEAD + TABLE, [], lookup, chain_required=False)[0]


def test_the_chain_gate_ignores_comments_and_string_literals():
    loophole = [
        f"-- gbx_rst_nbr gbx_rst_fromfile\nSELECT 'gbx_rst_nbr(' AS x, {G}gbx_rst_fromfile(f) FROM {T}rasters",
        f"/* {G}gbx_rst_nbr(x) */ SELECT {G}gbx_rst_fromfile(f) FROM {T}rasters",
    ]
    assert not rq.chain_ok(loophole)[0]
    assert not rq.check_q3(HEAD + TABLE, [], loophole)[0]
    assert rq.chain_ok(LIVE_SQL)[0]


def test_the_two_calls_must_be_in_one_statement():
    split = [
        f"SELECT {G}gbx_rst_fromfile(f) FROM {T}rasters",
        f"SELECT {G}gbx_rst_nbr(t, 4, 6) FROM {T}rasters",
    ]
    assert not rq.chain_ok(split)[0]


def test_a_table_outside_the_live_tables_fails_the_chain():
    foreign = [LIVE_SQL[0] + f" JOIN {T}burn_severity_summary b ON 1 = 1"]
    ok, why = rq.chain_ok(foreign)
    assert not ok and "burn_severity_summary" in why
    other = [LIVE_SQL[0].replace(f"{T}aois", "system.information_schema.tables")]
    assert not rq.chain_ok(other)[0]


def test_ctes_functions_and_extract_are_not_foreign_tables():
    sql = (
        f"WITH a AS (SELECT geom FROM {T}aois), b AS (SELECT * FROM a) "
        f"SELECT extract(YEAR FROM acquired_date), {G}gbx_rst_nbr({G}gbx_rst_fromfile(r.file), 4, 6) "
        f"FROM b, {T}rasters r LATERAL VIEW posexplode(x) h AS p, n"
    )
    assert rq.tables_read(sql) == {f"{T}aois".lower(), f"{T}rasters".lower(), "a"} - {
        "a"
    }
    assert "acquired_date" not in rq.tables_read(sql)
    assert rq.chain_ok([sql])[0]


COV_ROWS = [
    {
        "product": "population",
        "tier": "enhanced",
        "best_resolution_m": "92.7666662956",
        "first_date": "2020-01-01",
        "last_date": "2020-01-01",
    },
    {
        "product": "s2_l2a",
        "tier": "enhanced",
        "best_resolution_m": "10.0",
        "first_date": "2024-01-27",
        "last_date": "2024-03-12",
    },
]
Q1_SQL = [
    f"SELECT * FROM {T}coverage",
    f"SELECT {G}gbx_rst_fromfile(f, 'GTiff') FROM {T}rasters",
]
Q1_TEXT = "Sentinel-2 L2A imagery, enhanced tier, 10 m, from 27 Jan to 12 Mar 2024. We hold no elevation data."


def test_q1_matches_the_coverage_rows():
    assert rq.check_q1(Q1_TEXT, COV_ROWS, Q1_SQL)[0]
    wrong = [dict(COV_ROWS[0]), {**COV_ROWS[1], "best_resolution_m": "20.0"}]
    assert not rq.check_q1(Q1_TEXT, wrong, Q1_SQL)[0]
    late = [dict(COV_ROWS[0]), {**COV_ROWS[1], "last_date": "2024-03-30"}]
    assert not rq.check_q1(Q1_TEXT, late, Q1_SQL)[0]


def test_q1_text_must_state_resolution_dates_tier_and_no_elevation():
    assert not rq.check_q1(Q1_TEXT.replace("10 m", "100 m"), COV_ROWS, Q1_SQL)[0]
    assert not rq.check_q1(Q1_TEXT.replace("enhanced", "standard"), COV_ROWS, Q1_SQL)[0]
    assert not rq.check_q1(
        Q1_TEXT.replace("from 27 Jan to 12 Mar 2024", "from January"), COV_ROWS, Q1_SQL
    )[0]
    assert not rq.check_q1(
        Q1_TEXT.replace(
            "We hold no elevation data.", "We do not have the right thing."
        ),
        COV_ROWS,
        Q1_SQL,
    )[0]
    assert not rq.check_q1(
        "Sentinel-2 at 1010 m, enhanced, 2024-01-27 to 2024-03-12, no elevation.",
        COV_ROWS,
        Q1_SQL,
    )[0]


def test_q1_accepts_a_plain_denial_of_elevation():
    base = "Sentinel-2 L2A, enhanced tier, 10 m, from 27 Jan to 12 Mar 2024. "
    for denial in (
        "Elevation: We don't hold any elevation data yet.",
        "Imagery is available, elevation is not.",
        "We do not hold elevation.",
    ):
        assert rq.check_q1(base + denial, COV_ROWS, Q1_SQL)[0], denial
    assert not rq.check_q1(
        base + "Elevation is 600 m above sea level.", COV_ROWS, Q1_SQL
    )[0]


def test_q1_needs_the_live_read_in_the_live_space_only():
    assert not rq.check_q1(Q1_TEXT, COV_ROWS, [f"SELECT * FROM {T}coverage"])[0]
    assert rq.check_q1(
        Q1_TEXT, COV_ROWS, [f"SELECT * FROM {T}coverage"], chain_required=False
    )[0]


Q5_SQL = [
    f"SELECT {G}gbx_rst_avg(t)[0] AS mean_dnbr FROM (SELECT {G}gbx_rst_nbr({G}gbx_rst_fromfile(f, 'GTiff', g), 4, 6) AS t "
    f"FROM {T}rasters r JOIN {T}aois a ON st_intersects(r.footprint, a.geom) WHERE a.name = 'Hutchinson')"
]


Q5_OK = "The average burn severity (dNBR) in Hutchinson County is 0.029. {n} acres burned at moderate-low or worse."


def _q5(n, mean="0.029", rows=(), sql=Q5_SQL, **kw):
    return rq.check_q5n(
        Q5_OK.format(n=n).replace("0.029", mean), list(rows), sql, **kw
    )[0]


def test_q5n_mean_and_acres_are_judged_against_the_direct_sql():
    assert _q5("3,854")
    assert _q5("3,900") and _q5("3,810")  # within 2%
    assert not _q5("3,854", mean="0.080")
    assert not _q5("3,854", mean="0.031")  # 0.002 is the limit: 0.031 is 0.0023 off
    assert _q5("3,854", mean="0.030")
    assert not _q5("12,000")
    assert not _q5("3,854", sql=[f"SELECT 1 FROM {T}burn_severity_summary"])
    assert _q5("3,854", sql=[f"SELECT 1 FROM {T}burn_severity_summary"], chain_required=False)


def test_q5n_rejects_the_reviewers_wrong_acres():
    for wrong in ("0", "200", "381", "9,500", "3,950", "3,700", "92"):
        assert not _q5(wrong), wrong
    # a High-only row of 92 acres is a class, not the total
    assert not rq.check_q5n(
        "done",
        [{"mean_dnbr": "0.029", "severity": "High", "acres": "92"}],
        Q5_SQL,
    )[0]


def test_q5n_takes_acres_only_from_a_value_labelled_moderate_low_or_worse():
    mean = {"mean_dnbr": "0.0291"}
    # a bare `acres` column is not labelled, even when it equals the truth
    assert not rq.check_q5n("done", [{**mean, "acres": "3854"}], Q5_SQL)[0]
    assert rq.check_q5n("done", [{**mean, "mod_low_or_worse_acres": "3841"}], Q5_SQL)[0]
    assert rq.check_q5n(
        "done", [{**mean, "moderate_low_or_worse_acres": "3854"}], Q5_SQL
    )[0]
    assert rq.check_q5n(
        "done",
        [{**mean, "severity": "Moderate-low or worse", "acres": "3854"}],
        Q5_SQL,
    )[0]
    # one class alone is not the total
    assert not rq.check_q5n(
        "average 0.029. Moderate-low severity covers 3,854 acres.", [], Q5_SQL
    )[0]
    # a right number beside a wrong one, both labelled, fails
    assert not rq.check_q5n(
        "average 0.029. 3,854 acres at moderate-low or worse. Moderate-low or worse: 9,500 acres.",
        [],
        Q5_SQL,
    )[0]
    # a class breakdown after the label is not read as the total
    assert rq.check_q5n(
        "average 0.029. **3,854 acres** burned at moderate-low severity or worse. "
        "Moderate-low or worse: 3,532 acres moderate-low, 231 acres moderate-high and 92 acres high.",
        [],
        Q5_SQL,
    )[0]


def test_q5n_reads_result_columns_too():
    rows = [{"mean_dnbr": "0.0291", "mod_low_or_worse_acres": "3841"}]
    assert rq.check_q5n("done", rows, Q5_SQL)[0]
    assert not rq.check_q5n(
        "done", [{"mean_dnbr": "0.2", "mod_low_or_worse_acres": "3841"}], Q5_SQL
    )[0]


def test_the_independent_truth_agrees_with_the_gate_truth():
    t, i = rq.Q5N_TRUTH, rq.Q5N_INDEPENDENT
    assert abs(t["mean_dnbr"] - i["mean_dnbr"]) <= rq.MEAN_TOLERANCE
    acres = abs(t["acres_moderate_low_or_worse"] - i["acres_moderate_low_or_worse"])
    assert acres <= 0.02 * t["acres_moderate_low_or_worse"]


def test_novelty_flags_a_copied_example_and_scores_a_new_chain():
    copied = [spec.EXAMPLES[0][1]]
    novel, sim = rq.novelty(copied)
    assert not novel and sim == 1.0
    novel, sim = rq.novelty(Q5_SQL)
    assert novel and sim < 0.5


def test_the_questions_are_plain_language():
    banned = re.compile(
        r"(?i)gbx_|rst_|select |dnbr|fromfile|mapalgebra|histogram|\bnbr\b|st_|geojson|cast\("
    )
    for q in (*rq.QUESTIONS.values(), *rq.PREWARM):
        assert not banned.search(q), q
    assert "how many people live inside the fire perimeter" in rq.Q3
    assert "moderate-low or worse" in rq.Q5N and "average burn severity" in rq.Q5N


def test_the_runner_has_no_workspace_specific_literals():
    src = Path(rq.__file__).read_text()
    for literal in (
        "de2336249255be4e",
        "01f1e19c",
        "01f1e1df",
        "mjohns",
        "fevm-geospatial",
        "cloud.databricks.com",
    ):
        assert literal not in src, literal
    seed = (Path(rq.__file__).parent / "seed_agent.py").read_text()
    for literal in ("mjohns", "databricks.com/geobrix", "de2336249255be4e"):
        assert literal not in seed, literal


def test_answers_are_cleaned_of_workspace_links():
    text = "Low severity.\\[[1](https://x.cloud.databricks.com/genie/rooms/abc?o=1)\\] See https://x.cloud.databricks.com/y."
    got = rq.clean_text(text)
    assert "databricks.com" not in got and "[1]" in got


def test_summarize_gives_median_and_max_per_space_and_question():
    rows = [
        {"space": s, "question": "Q3", "seconds": x, "correct": ok}
        for s, x, ok in [
            ("live", 60.0, True),
            ("live", 70.0, True),
            ("live", 90.0, False),
            ("precomputed", 20.0, True),
            ("precomputed", 30.0, True),
        ]
    ]
    got = {r["space"]: r for r in rq.summarize(rows)}
    assert (got["live"]["median_s"], got["live"]["max_s"], got["live"]["correct"]) == (
        70.0,
        90.0,
        2,
    )
    assert got["precomputed"]["median_s"] == 25.0


def test_the_runner_requires_its_ids(monkeypatch, capsys):
    for var in ("GENIE_SPACE_ID", "GENIE_SPACE_ID_PRECOMPUTED", "GENIE_WAREHOUSE_ID"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit):
        rq.main(["--space", "live"], client=object())
    with pytest.raises(SystemExit):
        rq.main(
            ["--space", "both", "--live-id", "a", "--warehouse-id", "w"],
            client=object(),
        )


def test_q1_without_rerun_rows_is_unverified_not_correct():
    ok, why = rq.check_q1(Q1_TEXT, [], Q1_SQL)
    assert not ok and "unverified" in why


def test_q1_accepts_curly_quote_and_lacks_denials():
    base = "Sentinel-2 L2A, enhanced tier, 10 m, from 27 Jan to 12 Mar 2024. "
    for denial in (
        "We don\u2019t hold any elevation data.",
        "The collection lacks elevation.",
        "Elevation: none held.",
        "Elevation data is not available.",
    ):
        assert rq.check_q1(base + denial, COV_ROWS, Q1_SQL)[0], denial


def test_q1_rejects_an_invented_elevation_product():
    base = "Sentinel-2 L2A, enhanced tier, 10 m, from 27 Jan to 12 Mar 2024. "
    for invented in (
        "Elevation: a 30 m DEM covers the county.",
        "Elevation is available from a DEM at 30 m. We hold no soil data.",
        "We hold no wells data, and elevation is 600 m.",
    ):
        assert not rq.check_q1(base + invented, COV_ROWS, Q1_SQL)[0], invented
    dem = COV_ROWS + [
        {**COV_ROWS[1], "product": "elevation", "best_resolution_m": "30.0"}
    ]
    assert not rq.check_q1(Q1_TEXT, dem, Q1_SQL)[0]


def test_q1_compares_the_asset_count():
    wrong = [dict(COV_ROWS[0]), {**COV_ROWS[1], "assets": "3"}]
    assert not rq.check_q1(Q1_TEXT, wrong, Q1_SQL)[0]
    right = [dict(COV_ROWS[0], assets="1"), {**COV_ROWS[1], "assets": "6"}]
    assert rq.check_q1(Q1_TEXT, right, Q1_SQL)[0]


def test_strip_sql_handles_double_quotes_escapes_and_identifier():
    q = (
        "SELECT 'a''b' AS x, \"gbx_rst_nbr(\" AS y, 'it\\'s gbx_rst_nbr(' AS z "
        "-- gbx_rst_fromfile(\n FROM t"
    )
    clean = rq.strip_sql(q)
    assert "gbx_rst_nbr" not in clean and "gbx_rst_fromfile" not in clean
    assert not rq.chain_ok([q + " /* gbx_rst_nbr( */"], ["t"])[0]
    ident = "SELECT * FROM IDENTIFIER('other.schema.tbl') x"
    assert rq.tables_read(ident) == {"other.schema.tbl"}
    assert not rq.chain_ok([ident + f" JOIN {T}rasters r"])[0]
    dq = 'SELECT * FROM IDENTIFIER("geospatial_catalog.raster_genie.rasters")'
    assert rq.tables_read(dq) == {"geospatial_catalog.raster_genie.rasters"}
    call = f"SELECT IDENTIFIER('{G}gbx_rst_nbr')({G}gbx_rst_fromfile(f)) FROM {T}rasters"
    assert rq.chain_ok([call])[0]


def test_q3_burned_total_must_be_stated_not_summed():
    rows = [
        {"severity": n, "acres": str(a)}
        for n, a in rq.GROUND_TRUTH["acres"].items()
    ]
    text = "\n".join(
        f"{n} {a} acres" for n, a in rq.GROUND_TRUTH["acres"].items()
    ) + "\nAbout 3,849 people live inside."
    ok, why = rq.check_q3(text, rows, [], chain_required=False)
    assert not ok and "burned=MISS" in why
    stated = [{**r, "low_or_worse_acres": "631994"} for r in rows]
    assert rq.check_q3(text, stated, [], chain_required=False)[0]


def test_rejudge_recomputes_the_verdict_and_marks_the_rows(tmp_path):
    import json

    good = {
        "space": "live",
        "space_id": "x",
        "question": "Q5n",
        "run": 1,
        "status": "COMPLETED",
        "seconds": 5.0,
        "correct": False,
        "why": "old gate",
        "sql": Q5_SQL,
        "text": [Q5_OK.format(n="3,854")],
        "rerun_rows": [],
    }
    bad = {**good, "run": 2, "correct": True, "text": [Q5_OK.format(n="381")]}
    src, dst = tmp_path / "in.json", tmp_path / "out.json"
    src.write_text(json.dumps([good, bad]))
    out = rq.rejudge(src, dst)
    assert [r["correct"] for r in out] == [True, False]
    assert all(r["rejudged_offline"] and r["gate_version"] == rq.GATE_VERSION for r in out)
    assert json.loads(dst.read_text()) == out
