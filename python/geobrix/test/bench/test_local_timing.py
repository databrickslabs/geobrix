"""``bench.local_timing``: the flags of the local timing pass (gbx:bench:local-timing) and its corpus copy."""

import dataclasses
import json
import math
import os
import time
import types

import pytest

from databricks.labs.gbx.bench import local_timing as lt

NS = (6, 24, 96)
GOOD = {  # seconds at n = 6 / 24 / 96: the validated run after the topology fix, every form scaling with n
    "b": (1.0, 1.9, 5.5), "c": (1.6, 2.6, 7.4), "udf": (1.5, 2.5, 7.0),
    "d-py": (2.4, 5.5, 17.0), "d-rs": (2.0, 4.2, 12.4), "floor": (1.5, 1.5, 1.6),
}  # fmt: skip


def _res(**changes):
    table = {**GOOD, **changes}
    return {(f, n): t for f, ts in table.items() for n, t in zip(NS, ts)}


def _flags(res, k=3.0):
    return lt.flags(res, forms=tuple(GOOD), ns=NS, k=k)


def test_a_pass_where_every_form_scales_with_n_has_no_flags():
    assert _flags(_res()) == []


def test_a_form_that_does_not_scale_with_n_is_flagged_as_decoding_more_than_n():
    (flag,) = _flags(_res(udf=(7.1, 7.1, 7.4)))
    assert flag.startswith("udf: n-independent") and "decodes more than n tiles" in flag


def test_a_form_that_is_non_linear_is_flagged():
    (flag,) = _flags(_res(**{"d-py": (2.0, 2.5, 18.0)}))  # t96/t24 = 7.2 (4 expected)
    assert flag.startswith("d-py: non-linear") and "7.20" in flag


def test_growth_below_linear_is_normal_with_a_fixed_cost_but_a_flat_time_is_not():
    assert (
        _flags(_res(**{"d-py": (2.4, 3.7, 5.2)})) == []
    )  # t96/t24 = 1.4: a fixed cost and 8 cores
    (flag,) = _flags(_res(**{"d-py": (2.0, 5.0, 5.2)}))  # t96/t24 = 1.04
    assert flag.startswith("d-py: non-linear") and "1.04" in flag


def test_a_form_more_than_k_times_the_row_udf_is_flagged_and_k_is_overridable():
    res = _res(**{"d-rs": (3.0, 9.0, 36.0)})
    assert any(f.startswith("d-rs: 5.1x the row UDF at n=96") for f in _flags(res))
    assert not any("the row UDF" in f for f in _flags(res, k=6.0))


def test_a_form_whose_cost_per_tile_is_more_than_k_times_its_peers_is_flagged():
    flags = _flags(
        _res(b=(3.0, 11.0, 44.0))
    )  # per tile ~440 ms against its peers' median ~108 ms
    assert [f for f in flags if f.startswith("b: per-tile")]


def test_a_form_that_failed_is_a_flag_not_a_crash():
    flags = _flags(_res(**{"d-rs": (2.0, float("nan"), 12.4)}))
    assert flags == [
        "d-rs: no time at n=24 (the form failed, was skipped or timed 0 s)"
    ]


def test_the_table_has_seconds_per_n_the_ratios_to_floor_and_udf_and_t96_over_t24():
    head, *rows = lt.table(_res(), forms=tuple(GOOD), ns=NS).splitlines()
    assert head.split() == [
        "form",
        "6",
        "24",
        "96",
        "x",
        "floor",
        "x",
        "udf",
        "t96/t24",
    ]
    udf = next(r.split() for r in rows if r.startswith("udf"))
    assert udf == ["udf", "1.5", "2.5", "7.0", "4.4", "1.0", "2.80"]


def test_only_the_io_set_is_implemented(capsys):
    assert set(lt.SETS) == {"IO"}
    with pytest.raises(
        SystemExit
    ) as e:  # a usage error: 2, so 1 stays "the pass flagged a form"
        lt.main(["--set", "G"])
    assert e.value.code == 2 and "set 'G' is not implemented" in capsys.readouterr().err


def test_the_corpus_is_made_once_from_real_copies(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    for i in range(10):
        (src / f"r{i}.tif").write_bytes(b"x" * (i + 1))
    assert lt.ensure_corpus(str(src), str(dst), 4) == 4
    names = sorted(os.listdir(dst))
    assert len(names) == 4 and not any(os.path.islink(dst / n) for n in names)
    (dst / names[0]).write_bytes(b"kept")
    assert (
        lt.ensure_corpus(str(src), str(dst), 4) == 4
    )  # already there: nothing is copied again
    assert (dst / names[0]).read_bytes() == b"kept"
    with pytest.raises(lt.PreconditionMissing, match="holds only 10"):
        lt.ensure_corpus(str(src), str(tmp_path / "other"), 20)


def test_the_corpus_count_is_tiles_not_every_file_in_the_folder(tmp_path):
    """A sidecar (`.aux.xml`) in the copy must not satisfy the count."""
    src, dst = tmp_path / "src", tmp_path / "dst"
    src.mkdir()
    dst.mkdir()
    for i in range(6):
        (src / f"r{i}.tif").write_bytes(b"x")
    for i in range(6):
        (dst / f"r{i}.tif.aux.xml").write_bytes(b"<x/>")
    assert lt.ensure_corpus(str(src), str(dst), 4) == 4
    assert len([f for f in os.listdir(dst) if f.endswith(".tif")]) == 4


def test_a_missing_precondition_exits_2_and_a_flag_exits_1(
    tmp_path, capsys, monkeypatch
):
    src = tmp_path / "src"
    src.mkdir()
    (src / "r0.tif").write_bytes(b"x")
    argv = ["--corpus-src", str(src), "--corpus-dir", str(tmp_path / "dst")]
    assert lt.main(argv) == 2
    assert "PRECONDITION MISSING" in capsys.readouterr().err
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)
    monkeypatch.setattr(
        lt, "measure_runs", lambda spec, corpus, repeats: [_res(udf=(7.1, 7.1, 7.4))]
    )
    assert lt.main(argv) == 1  # flagged
    monkeypatch.setattr(lt, "measure_runs", lambda spec, corpus, repeats: [_res()])
    assert lt.main(argv) == 0


def test_a_pass_that_crashes_is_not_a_flag(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)

    def boom(spec, corpus, repeats):
        raise RuntimeError("spark died")

    monkeypatch.setattr(lt, "measure_runs", boom)
    assert lt.main(["--corpus-dir", str(tmp_path)]) == 3
    assert "spark died" in capsys.readouterr().err


def test_a_missing_floor_or_reference_row_is_a_flag_not_a_key_error():
    res = {k: v for k, v in _res().items() if k[0] not in ("floor", "udf")}
    flags = _flags(res)
    assert "floor: no time at n=6 (the form failed, was skipped or timed 0 s)" in flags
    assert any(f.startswith("udf: no time") for f in flags)
    assert lt.table(res, forms=tuple(GOOD), ns=NS)  # renders, with nan cells


def test_a_zero_time_is_a_flag_not_a_division_by_zero():
    for res in (
        _res(udf=(0.0, 0.0, 0.0)),
        _res(**{"d-py": (2.0, 0.0, 5.0)}),
        _res(floor=(0.0, 0.0, 0.0)),
    ):
        flags = _flags(res)
        assert flags and any("no time" in f for f in flags), flags
        assert lt.table(res, forms=tuple(GOOD), ns=NS)


def test_the_jar_is_found_in_the_order_env_then_lib_then_target(tmp_path, monkeypatch):
    lib = tmp_path / "python" / "geobrix" / "lib"
    tgt = tmp_path / "target"
    lib.mkdir(parents=True)
    tgt.mkdir()
    monkeypatch.delenv("GBX_JAR", raising=False)
    with pytest.raises(lt.PreconditionMissing, match="no geobrix JAR"):
        lt._jar(str(tmp_path))
    (tgt / "geobrix-0.2-jar-with-dependencies.jar").write_text("t")
    assert lt._jar(str(tmp_path)).endswith(
        "target/geobrix-0.2-jar-with-dependencies.jar"
    )
    (lib / "geobrix-0.1-jar-with-dependencies.jar").write_text("l")
    (lib / "geobrix-0.3-jar-with-dependencies.jar").write_text("l")
    assert lt._jar(str(tmp_path)).endswith(
        "lib/geobrix-0.3-jar-with-dependencies.jar"
    )  # lib first, the last sorted
    mine = tmp_path / "mine.jar"
    mine.write_text("m")
    monkeypatch.setenv("GBX_JAR", str(mine))
    assert lt._jar(str(tmp_path)) == str(mine)  # the environment first


def test_measure_warms_up_at_the_smallest_n_keeps_the_measured_run_and_turns_a_failure_into_nan(
    monkeypatch, capsys
):
    from types import SimpleNamespace as R

    from databricks.labs.gbx.bench import levels
    from databricks.labs.gbx.bench import levels_forms as lf

    spec = lt.SETS["IO"]
    calls, seen_forms = [], []

    def fake_run_mvp(spark, mvp, *a, base_rows, **kw):
        calls.append(base_rows)
        seen_forms.append([f.variant for f in lf.FORMS[mvp]])
        warm = len(calls) == 1
        return [
            (
                R(
                    form=f.variant,
                    iter_median_s=100.0 if warm else base_rows / 10,
                    status="ok",
                    failure_mode="",
                    note="",
                )
                if f.variant != "d-rs"
                else R(
                    form="d-rs",
                    iter_median_s=0.0,
                    status="error",
                    failure_mode="boom",
                    note="",
                )
            )
            for f in lf.FORMS[mvp]
        ]

    monkeypatch.setattr(levels, "run_mvp", fake_run_mvp)
    res = lt.measure(spec, "/corpus", spark=object())
    assert calls == [6, 6, 24, 96]  # a first pass at the smallest n only warms up
    assert all(
        v == list(spec.forms) for v in seen_forms
    )  # the set's forms, in its order (floor last)
    assert res[("udf", 6)] == 0.6  # the second pass at 6 overwrote the warmup's 100 s
    assert res[("udf", 96)] == 9.6
    assert all(math.isnan(res[("d-rs", n)]) for n in spec.ns)
    assert "d-rs n=6: error boom" in capsys.readouterr().out


# ---- the stored per-form baseline: a per-tile regression under K x is caught


def _doubled_per_tile(form, base=GOOD):
    """The timings of `form` with its per-tile cost doubled and its fixed cost unchanged: t(n) = F + 2 p n."""
    t6, t24, t96 = base[form]
    p = (t96 - t24) / 72
    fixed = t24 - 24 * p
    return tuple(fixed + 2 * p * n for n in NS)


def test_the_per_tile_cost_is_the_slope_between_the_two_larger_ns_minus_the_floors():
    per_tile = lt.per_tile_ms(_res(), forms=tuple(GOOD), ns=NS)
    assert set(per_tile) == set(GOOD) - {"floor"}
    assert per_tile["udf"] == pytest.approx(
        ((7.0 - 2.5) / 72 - (1.6 - 1.5) / 72) * 1000
    )
    assert (
        lt.per_tile_ms(_res(udf=(1, float("nan"), 7)), forms=tuple(GOOD), ns=NS).get(
            "udf"
        )
        is None
    )


def test_a_2x_per_tile_regression_with_the_same_fixed_cost_is_missed_by_the_ratio_rules_and_caught_by_the_baseline():
    base = lt.per_tile_ms(_res(), forms=tuple(GOOD), ns=NS)
    for form in ("udf", "b"):  # the reference, and a form compared with it
        slow = _res(**{form: _doubled_per_tile(form)})
        assert _flags(slow) == [], "the review's blind spot: the K x rules need 3x"
        got = lt.baseline_flags(
            lt.per_tile_ms(slow, forms=tuple(GOOD), ns=NS), base, k=1.5
        )
        assert (
            len(got) == 1
            and got[0].startswith(f"{form}: per-tile ")
            and "x its baseline" in got[0]
        ), got


def test_the_baseline_is_quiet_for_the_same_timings_noise_inside_k_and_a_faster_form():
    base = lt.per_tile_ms(_res(), forms=tuple(GOOD), ns=NS)
    assert lt.baseline_flags(base, base, k=1.5) == []
    noisy = {f: v * 1.4 for f, v in base.items()}
    assert lt.baseline_flags(noisy, base, k=1.5) == []
    assert lt.baseline_flags({f: v * 0.2 for f, v in base.items()}, base, k=1.5) == []
    assert lt.baseline_flags(
        {f: v * 1.4 for f, v in base.items()}, base, k=1.2
    )  # --baseline-k lowers the bar


def test_a_form_missing_from_the_baseline_or_from_the_run_is_a_flag():
    base = lt.per_tile_ms(_res(), forms=tuple(GOOD), ns=NS)
    now = dict(base)
    now["new-form"] = 5.0
    assert lt.baseline_flags(now, base, k=1.5) == [
        "new-form: not in the baseline (write it again)"
    ]
    gone = {f: v for f, v in base.items() if f != "b"}
    assert lt.baseline_flags(gone, base, k=1.5) == [
        "b: in the baseline but not timed in this pass"
    ]


def test_a_baseline_of_zero_or_negative_noise_does_not_divide_by_zero_or_flag_everything():
    base = {"b": 0.0, "c": -2.0}
    assert lt.baseline_flags({"b": 0.5, "c": 0.5}, base, k=1.5) == []
    assert lt.baseline_flags(
        {"b": 40.0, "c": 40.0}, base, k=1.5
    )  # a real jump from nothing is still caught


def test_the_baseline_file_round_trips_with_its_environment(tmp_path):
    path = tmp_path / "sub" / "base.json"
    per_tile = {"b": 18.0, "udf": 62.5}
    lt.write_baseline(
        str(path),
        "IO",
        NS,
        per_tile,
        {"cores": 8, "arch": "aarch64", "git_sha": "abc1234"},
    )
    got = lt.read_baseline(str(path), "IO")
    assert got["per_tile_ms"] == per_tile and got["ns"] == list(NS)
    assert got["env"] == {"cores": 8, "arch": "aarch64", "git_sha": "abc1234"}
    with pytest.raises(lt.PreconditionMissing, match="--baseline-write"):
        lt.read_baseline(str(tmp_path / "none.json"), "IO")
    with pytest.raises(lt.PreconditionMissing, match="set"):
        lt.read_baseline(str(path), "G")


def test_main_writes_a_baseline_then_compares_against_it(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)
    base = str(tmp_path / "base.json")
    argv = ["--corpus-dir", str(tmp_path)]
    monkeypatch.setattr(lt, "measure_runs", lambda spec, corpus, repeats: [_res()])
    assert lt.main(argv + ["--baseline-write", "--baseline", base]) == 0
    assert "baseline written" in capsys.readouterr().out and os.path.exists(base)
    assert lt.main(argv + ["--baseline", base]) == 0  # the same timings
    monkeypatch.setattr(
        lt,
        "measure_runs",
        lambda spec, corpus, repeats: [_res(udf=_doubled_per_tile("udf"))],
    )
    assert lt.main(argv + ["--baseline", base]) == 1
    assert "udf: per-tile" in capsys.readouterr().out
    assert lt.main(argv + ["--baseline", base, "--baseline-k", "2.5"]) == 0
    assert lt.main(argv + ["--baseline", str(tmp_path / "nope.json")]) == 2


def test_a_flagged_pass_does_not_write_a_baseline(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)
    monkeypatch.setattr(
        lt, "measure_runs", lambda spec, corpus, repeats: [_res(udf=(7.1, 7.1, 7.4))]
    )
    base = tmp_path / "base.json"
    assert (
        lt.main(
            ["--corpus-dir", str(tmp_path), "--baseline-write", "--baseline", str(base)]
        )
        == 1
    )
    assert not base.exists() and "baseline not written" in capsys.readouterr().out


# ---- repeated measured runs: the median, not one noisy run


def _scaled(f):
    """GOOD with every form's time above the floor scaled by f: a slower (f > 1) or faster run of the same machine."""
    floor = GOOD["floor"]
    return _res(
        **{
            form: tuple(fl + (t - fl) * f for t, fl in zip(ts, floor))
            for form, ts in GOOD.items()
            if form != "floor"
        }
    )


def test_the_median_time_of_three_runs_ignores_the_one_odd_run():
    runs = [_scaled(1.0), _scaled(1.6), _scaled(1.0)]
    med = lt.median_res(runs)
    assert med == _scaled(1.0)  # the 1.6x run does not move the median
    assert lt.median_res([_scaled(1.0)]) == _scaled(1.0)


def test_a_cell_that_failed_in_any_run_stays_a_failure_in_the_median():
    bad = {**_scaled(1.0), ("d-rs", 24): float("nan")}
    med = lt.median_res([_scaled(1.0), bad, _scaled(1.0)])
    assert (
        math.isnan(med[("d-rs", 24)]) and med[("udf", 24)] == _scaled(1.0)[("udf", 24)]
    )


def test_the_median_slope_of_three_runs_is_the_middle_cost_per_tile_not_the_mean():
    kw = dict(forms=tuple(GOOD), ns=NS)
    one = lt.per_tile_ms(_scaled(1.0), **kw)
    odd = lt.per_tile_ms(_scaled(1.6), **kw)  # the 1.6x spread seen on d-py
    med = lt.median_per_tile([_scaled(1.0), _scaled(1.6), _scaled(1.0)], **kw)
    assert med == pytest.approx(one) and odd["d-py"] > 1.5 * one["d-py"]
    mid = lt.median_per_tile([_scaled(1.0), _scaled(1.3), _scaled(1.6)], **kw)
    assert mid["d-py"] == pytest.approx(lt.per_tile_ms(_scaled(1.3), **kw)["d-py"])


def test_a_form_missing_a_slope_in_one_run_takes_the_median_of_the_others():
    kw = dict(forms=tuple(GOOD), ns=NS)
    gone = {k: v for k, v in _scaled(1.0).items() if k != ("b", 96)}
    med = lt.median_per_tile([_scaled(1.0), gone, _scaled(1.6)], **kw)
    assert med["b"] == pytest.approx(
        (
            lt.per_tile_ms(_scaled(1.0), **kw)["b"]
            + lt.per_tile_ms(_scaled(1.6), **kw)["b"]
        )
        / 2
    )


def test_measure_runs_warms_up_once_and_times_every_n_per_repeat(monkeypatch):
    from types import SimpleNamespace as R

    from databricks.labs.gbx.bench import levels
    from databricks.labs.gbx.bench import levels_forms as lf

    spec = lt.SETS["IO"]
    calls = []

    def fake_run_mvp(spark, mvp, *a, base_rows, **kw):
        calls.append(base_rows)
        return [
            R(
                form=f.variant,
                iter_median_s=base_rows / 10,
                status="ok",
                failure_mode="",
                note="",
            )
            for f in lf.FORMS[mvp]
        ]

    monkeypatch.setattr(levels, "run_mvp", fake_run_mvp)
    runs = lt.measure_runs(spec, "/corpus", 3, spark=object())
    assert calls == [6] + [6, 24, 96] * 3  # one warmup, then a full ladder per repeat
    assert len(runs) == 3 and all(r[("udf", 96)] == 9.6 for r in runs)


def test_the_repeats_default_is_one_for_the_plain_pass_and_three_for_the_baseline_modes(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)
    seen = []

    def fake(spec, corpus, repeats):
        seen.append(repeats)
        return [_res()] * repeats

    monkeypatch.setattr(lt, "measure_runs", fake)
    base = str(tmp_path / "b.json")
    argv = ["--corpus-dir", str(tmp_path)]
    assert lt.main(argv) == 0
    assert lt.main(argv + ["--baseline-write", "--baseline", base]) == 0
    assert lt.main(argv + ["--baseline", base]) == 0
    assert lt.main(argv + ["--baseline", base, "--repeats", "5"]) == 0
    assert lt.main(argv + ["--repeats", "2"]) == 0
    assert seen == [1, 3, 3, 5, 2]


def test_a_baseline_write_of_fewer_than_three_runs_is_a_usage_error(
    tmp_path, capsys, monkeypatch
):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)
    monkeypatch.setattr(
        lt, "measure_runs", lambda spec, corpus, repeats: pytest.fail("must not time")
    )
    with pytest.raises(SystemExit) as e:
        lt.main(["--corpus-dir", str(tmp_path), "--baseline-write", "--repeats", "2"])
    assert e.value.code == 2 and "at least 3" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        lt.main(["--corpus-dir", str(tmp_path), "--repeats", "0"])


def test_one_odd_run_in_three_neither_flags_nor_moves_the_stored_baseline(
    tmp_path, capsys, monkeypatch
):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)
    base = str(tmp_path / "b.json")
    argv = ["--corpus-dir", str(tmp_path)]
    # d-py came out 1.6x slower in one run of three (the spread seen in a single-run pass): the median holds
    monkeypatch.setattr(
        lt,
        "measure_runs",
        lambda spec, corpus, repeats: [_scaled(1.0), _scaled(1.0), _scaled(1.0)],
    )
    assert lt.main(argv + ["--baseline-write", "--baseline", base]) == 0
    monkeypatch.setattr(
        lt,
        "measure_runs",
        lambda spec, corpus, repeats: [_scaled(1.6), _scaled(1.0), _scaled(1.0)],
    )
    assert lt.main(argv + ["--baseline", base]) == 0
    out = capsys.readouterr().out
    assert "per run" in out  # the spread is shown, so a noisy machine is visible


# --- a corpus folder read in place (the big-folder shape) ----------------------------------------


def _tiles_folder(path, n):
    path.mkdir(parents=True)
    for i in range(n):
        (path / f"r{i}.tif").write_bytes(b"x")
    (path / "notes.json").write_text("{}")  # not a tile
    return path


def test_a_folder_named_rows_is_read_in_place_and_nothing_is_copied(
    tmp_path, capsys, monkeypatch
):
    rows = _tiles_folder(tmp_path / "bench" / "rows", 120)
    seen = {}

    def no_copy(*a):
        raise AssertionError("a corpus read in place is never copied")

    monkeypatch.setattr(lt, "ensure_corpus", no_copy)
    monkeypatch.setattr(
        lt,
        "measure_runs",
        lambda spec, corpus, repeats: seen.update(corpus=corpus) or [_res()],
    )
    monkeypatch.setattr(lt, "probe_readdir", lambda r: "readdir_mode probe line")
    assert lt.main(["--corpus-dir", str(rows)]) == 0
    assert seen["corpus"] == str(
        tmp_path / "bench"
    )  # the parent: `<corpus>/rows` is the folder
    out = capsys.readouterr().out
    assert f"corpus {rows}: 120 tiles" in out and "in place" in out
    assert "readdir_mode probe line" in out
    assert sorted(os.listdir(tmp_path / "bench")) == ["rows"]


def test_a_folder_read_in_place_with_too_few_tiles_is_a_precondition_not_a_copy(
    tmp_path, capsys, monkeypatch
):
    rows = _tiles_folder(tmp_path / "rows", 10)
    monkeypatch.setattr(
        lt, "measure_runs", lambda *a: pytest.fail("the pass must not run")
    )
    assert lt.main(["--corpus-dir", str(rows)]) == 2
    err = capsys.readouterr().err
    assert "PRECONDITION MISSING" in err and "holds only 10 tiles" in err
    assert len(os.listdir(rows)) == 11  # nothing written


def test_count_tiles_counts_tifs_only(tmp_path):
    rows = _tiles_folder(tmp_path / "rows", 5)
    assert lt.count_tiles(str(rows)) == 5
    assert lt.count_tiles(str(tmp_path / "absent")) == 0


def test_the_probe_reports_the_mode_a_file_of_a_big_folder_takes(tmp_path):
    rows = _tiles_folder(tmp_path / "rows", 1200)  # past GDAL's listing limit of 1000
    line = lt.probe_readdir(str(rows))
    assert "1201 files" in line or "1200 tiles" in line
    assert "readdir_mode = EMPTY_DIR" in line, line


def test_the_probe_reports_by_name_checks_for_a_file_with_a_sidecar(tmp_path):
    rows = _tiles_folder(tmp_path / "rows", 3)
    (rows / "r1.tif.ovr").write_bytes(
        b"x"
    )  # a sidecar: r1 (the file the probe judges) takes TRUE
    line = lt.probe_readdir(str(rows))
    assert "readdir_mode = TRUE" in line, line


def test_the_spark_workers_do_not_inherit_the_value_pyrx_set_in_the_driver(monkeypatch):
    """Importing pyrx sets GDAL_DISABLE_READDIR_ON_OPEN=TRUE in the driver; a JVM started after that hands it to the
    Python workers, where it reads as the user's own value and turns the per-file sidecar mode off. The session is
    launched without it (a value the user exported is kept), and the driver gets it back.
    """
    from databricks.labs.gbx.pyrx import _env

    key = "GDAL_DISABLE_READDIR_ON_OPEN"
    monkeypatch.setenv(key, "TRUE")
    monkeypatch.setattr(_env, "_READDIR_SET_HERE", True)  # pyrx set it
    with lt._launch_env_without_pyrx_readdir():
        assert key not in os.environ
    assert os.environ[key] == "TRUE"
    monkeypatch.setattr(_env, "_READDIR_SET_HERE", False)  # the user's
    with lt._launch_env_without_pyrx_readdir():
        assert os.environ[key] == "TRUE"


# ---- the readdir probe


class _Done:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


def _probe_json(**changes):
    got = {
        "files": 1200,
        "cap": 1000,
        "bound": 100000,
        "tiles": ["r0.tif", "r1.tif"],
        "modes": [None, "EMPTY_DIR"],
        "listings": 1,
        "listing_seconds": 0.25,
        "per_open": True,
    }
    return json.dumps({**got, **changes})


def _fake_child(monkeypatch, **kw):
    monkeypatch.setattr(lt.subprocess, "run", lambda *a, **k: _Done(**kw))


def test_the_probe_times_the_one_cold_listing_and_nothing_else(tmp_path, monkeypatch):
    """The listing cache is cleared before the timed opens, and the time reported is the scan of the listing itself:
    no second `os.listdir` of the folder inside it (that made the figure a warm listing plus another).
    """
    from databricks.labs.gbx.pyrx import _env

    rows = _tiles_folder(tmp_path / "rows", 5)
    inside = {"on": False, "listdirs": 0, "cleared_before": None}
    real_listdir, real_clear = os.listdir, _env._LISTINGS.clear

    def listdir(p="."):
        inside["listdirs"] += inside["on"]
        return real_listdir(p)

    def clear():
        inside["cleared_before"] = not inside["on"]
        return real_clear()

    def scan(folder):
        time.sleep(0.05)
        return _env._IndexedNames(real_listdir(folder))

    def fake_mode(path):
        inside["on"] = True
        _env._LISTINGS._list(os.path.dirname(path))
        inside["on"] = False
        return "EMPTY_DIR"

    monkeypatch.setattr(os, "listdir", listdir)
    monkeypatch.setattr(_env, "configure_gdal_env", lambda: None)
    monkeypatch.setattr(_env._LISTINGS, "clear", clear)
    monkeypatch.setattr(_env._LISTINGS, "_list", scan)
    monkeypatch.setattr(_env, "readdir_mode", fake_mode)
    got = lt.probe_folder(str(rows))
    assert inside["cleared_before"] and inside["listdirs"] == 0
    assert 0.05 <= got["listing_seconds"] < 0.5
    assert got["files"] == 6 and got["tiles"] == ["r0.tif", "r1.tif"]


def test_the_probe_says_which_of_two_reasons_left_the_mode_none(monkeypatch):
    _fake_child(monkeypatch, stdout=_probe_json(modes=[None, None], per_open=True))
    kept = lt.probe_readdir("/x/rows")
    assert "readdir_mode = None" in kept and "keeps the process value TRUE" in kept
    assert "cannot be set" not in kept
    _fake_child(monkeypatch, stdout=_probe_json(modes=[None, None], per_open=False))
    unreachable = lt.probe_readdir("/x/rows")
    assert "readdir_mode = None" in unreachable
    assert (
        "per-open value cannot be set" in unreachable
        and "GBX_NATIVE_PTH" in unreachable
    )


def test_the_probe_names_the_listing_limit_it_is_past_and_its_cold_listing(monkeypatch):
    _fake_child(monkeypatch, stdout=_probe_json(files=999, cap=1000))
    line = lt.probe_readdir("/x/rows")
    assert (
        "past GDAL's listing limit of 1000" in line
    )  # "." and ".." count: 999 names are past it
    assert "cold listing 0.25 s" in line
    _fake_child(monkeypatch, stdout=_probe_json(files=998, cap=1000))
    assert "past GDAL's" not in lt.probe_readdir("/x/rows")
    _fake_child(
        monkeypatch, stdout=_probe_json(files=50000, cap=None)
    )  # GDAL_READDIR_LIMIT_ON_OPEN=0
    assert "past GDAL's" not in lt.probe_readdir("/x/rows")


def test_a_probe_child_that_crashes_is_a_failure_with_its_stderr(monkeypatch):
    _fake_child(monkeypatch, returncode=-11, stdout="", stderr="Segmentation fault\n")
    with pytest.raises(RuntimeError) as e:
        lt.probe_readdir("/x/rows")
    assert "-11" in str(e.value) and "Segmentation fault" in str(e.value)
    _fake_child(monkeypatch, returncode=1, stdout="not json", stderr="ImportError: x")
    with pytest.raises(RuntimeError, match="ImportError: x"):
        lt.probe_readdir("/x/rows")
    _fake_child(
        monkeypatch, returncode=0, stdout="not json"
    )  # exits 0 but prints no result
    with pytest.raises(RuntimeError, match="no result"):
        lt.probe_readdir("/x/rows")


def test_a_probe_that_crashes_ends_the_pass_with_exit_3(tmp_path, capsys, monkeypatch):
    rows = _tiles_folder(tmp_path / "rows", 120)
    _fake_child(monkeypatch, returncode=1, stderr="boom in the child")
    monkeypatch.setattr(
        lt, "measure_runs", lambda *a: pytest.fail("the pass must not run")
    )
    assert lt.main(["--corpus-dir", str(rows)]) == 3
    assert "boom in the child" in capsys.readouterr().err


def test_the_spark_session_is_started_through_the_env_wrapper(monkeypatch):
    """`getOrCreate` is the call that launches the JVM: it runs inside `_launch_env_without_pyrx_readdir`."""
    import contextlib

    import pyspark.sql

    state = {"in": False, "created_in": None, "entered": 0}

    @contextlib.contextmanager
    def wrapper():
        state["in"] = True
        state["entered"] += 1
        try:
            yield
        finally:
            state["in"] = False

    class _Builder:
        def __getattr__(self, name):
            return lambda *a, **k: self

        def getOrCreate(self):
            state["created_in"] = state["in"]
            return types.SimpleNamespace(
                sparkContext=types.SimpleNamespace(setLogLevel=lambda lvl: None)
            )

    monkeypatch.setattr(lt, "_launch_env_without_pyrx_readdir", wrapper)
    monkeypatch.setattr(lt, "_jar", lambda: "x.jar")
    monkeypatch.setattr(
        pyspark.sql, "SparkSession", types.SimpleNamespace(builder=_Builder())
    )
    lt._spark()
    assert state["entered"] == 1 and state["created_in"] is True


# ---- an in-place corpus is never written to


def test_the_io_set_writes_nothing_under_the_corpus_it_reads_in_place():
    from databricks.labs.gbx.bench import levels_forms as lf

    forms = [f for f in lf.FORMS["IO"] if f.variant in lt.SETS["IO"].forms]
    assert forms and lf.forms_writing_under_corpus(forms) == []


def test_a_set_whose_forms_write_under_the_corpus_is_refused_for_an_in_place_one(
    tmp_path, capsys, monkeypatch
):
    rows = _tiles_folder(tmp_path / "bench" / "rows", 120)
    writing = dataclasses.replace(
        lt.SETS["IO"],
        mvp="H",
        forms=("h_merge_fold", "h_merge_vrt"),
        reference="h_merge_fold",
    )
    monkeypatch.setitem(lt.SETS, "H", writing)
    monkeypatch.setattr(
        lt, "measure_runs", lambda *a: pytest.fail("the pass must not run")
    )
    monkeypatch.setattr(lt, "probe_readdir", lambda r: "probe")
    assert lt.main(["--set", "H", "--corpus-dir", str(rows)]) == 2
    err = capsys.readouterr().err
    assert "PRECONDITION MISSING" in err and "_synth" in err and "in place" in err
    assert sorted(os.listdir(tmp_path / "bench")) == ["rows"]  # nothing written
