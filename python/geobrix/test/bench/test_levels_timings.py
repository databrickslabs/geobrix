"""The part builder sizes run parts from the times of earlier runs."""

from databricks.labs.gbx.bench import levels_timings as lt


def _timings():
    return {
        "fixed_min": 5.0,
        "functions": {
            "a": {"4": {"min": 30.0}},
            "b": {"4": {"min": 8.0}},
            "c": {"4": {"min": 3.0}},
            "d": {"1": {"min": 2.0}},
            "e": {"4": {"min": 9.0}},
        },
    }


def test_a_long_function_runs_alone_and_the_longest_part_comes_first():
    inp = {"a": "x", "b": "x", "c": "x", "d": "x", "e": "y"}.get
    parts = lt.plan_parts(["b", "c", "a", "d", "e"], _timings(), 4, 25.0, input_of=inp)
    assert parts[0]["fns"] == ["a"]
    assert [p["predicted_min"] for p in parts] == sorted(
        (p["predicted_min"] for p in parts), reverse=True
    )
    # b, c and d share input x and fit in 20 min of room; e is on another input
    assert sorted(parts[1]["fns"]) == ["b", "c", "d"]
    assert parts[-1]["fns"] == ["e"]


def test_a_scale_with_no_record_is_projected_from_the_nearest_one():
    assert lt.fn_minutes(_timings(), "d", 4) == 8.0
    assert lt.fn_minutes(_timings(), "a", 16) == 120.0
    assert lt.fn_minutes(_timings(), "zz", 4, default_fn_min=3.0) == 3.0


def test_collect_adds_the_minutes_of_the_rows_of_a_run():
    rows = lt.parse_rows(
        [
            "r1|f|L1|l1@n|250|ok|2.0|2.0|0|6.0|1|v1:1|nulls=0",
            "r1|f|L3|c0@n|250|ok|4.0|4.0|4.0|0|1|v1:1|nulls=0",
            "r1|g|L3|c0@n|250|error|4.0|4.0|4.0|0|1||",
        ]
    )
    t = lt.collect({}, rows, {"r1": "1"}, measured=3)
    # (6 + 2*5)/60 + (4 + 4*5)/60 = 40 s
    assert t["functions"]["f"]["1"]["min"] == round(40 / 60, 2)
    assert "g" not in t["functions"]
