"""``bench.local_timing``: the flags of the local timing pass (gbx:bench:local-timing) and its corpus copy."""

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
    assert flags == ["d-rs: no time at n=24 (the form failed or was skipped)"]


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


def test_only_the_io_set_is_implemented():
    assert set(lt.SETS) == {"IO"}
    with pytest.raises(SystemExit, match="G"):
        lt.main(["--set", "G"])


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
    with pytest.raises(SystemExit, match="holds only 10"):
        lt.ensure_corpus(str(src), str(tmp_path / "other"), 20)
