"""``bench.local_timing``: the flags of the local timing pass (gbx:bench:local-timing) and its corpus copy."""

import math
import os

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
    monkeypatch.setattr(lt, "measure", lambda spec, corpus: _res(udf=(7.1, 7.1, 7.4)))
    assert lt.main(argv) == 1  # flagged
    monkeypatch.setattr(lt, "measure", lambda spec, corpus: _res())
    assert lt.main(argv) == 0


def test_a_pass_that_crashes_is_not_a_flag(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)

    def boom(spec, corpus):
        raise RuntimeError("spark died")

    monkeypatch.setattr(lt, "measure", boom)
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
    monkeypatch.setattr(lt, "measure", lambda spec, corpus: _res())
    assert lt.main(argv + ["--baseline-write", "--baseline", base]) == 0
    assert "baseline written" in capsys.readouterr().out and os.path.exists(base)
    assert lt.main(argv + ["--baseline", base]) == 0  # the same timings
    monkeypatch.setattr(
        lt, "measure", lambda spec, corpus: _res(udf=_doubled_per_tile("udf"))
    )
    assert lt.main(argv + ["--baseline", base]) == 1
    assert "udf: per-tile" in capsys.readouterr().out
    assert lt.main(argv + ["--baseline", base, "--baseline-k", "2.5"]) == 0
    assert lt.main(argv + ["--baseline", str(tmp_path / "nope.json")]) == 2


def test_a_flagged_pass_does_not_write_a_baseline(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(lt, "ensure_corpus", lambda *a: 96)
    monkeypatch.setattr(lt, "measure", lambda spec, corpus: _res(udf=(7.1, 7.1, 7.4)))
    base = tmp_path / "base.json"
    assert (
        lt.main(
            ["--corpus-dir", str(tmp_path), "--baseline-write", "--baseline", str(base)]
        )
        == 1
    )
    assert not base.exists() and "baseline not written" in capsys.readouterr().out
