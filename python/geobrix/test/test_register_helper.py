"""Unit tests for the shared selective-registration helper."""

import threading

import pytest

from databricks.labs.gbx import _register


def test_normalize_name_short_and_full_and_case():
    assert _register.normalize_name("rst_slope") == "gbx_rst_slope"
    assert _register.normalize_name("gbx_rst_slope") == "gbx_rst_slope"
    assert _register.normalize_name("RST_Slope") == "gbx_rst_slope"
    assert _register.normalize_name("GBX_RST_Slope") == "gbx_rst_slope"
    assert _register.normalize_name("  BNG_Polyfill  ") == "gbx_bng_polyfill"


def test_normalize_datasource_name_suffix_and_case():
    assert _register.normalize_datasource_name("raster") == "raster_gbx"
    assert _register.normalize_datasource_name("raster_gbx") == "raster_gbx"
    assert _register.normalize_datasource_name("RASTER_GBX") == "raster_gbx"
    assert _register.normalize_datasource_name("  Shapefile  ") == "shapefile_gbx"


def test_resolve_only_with_datasource_normalizer():
    valid = {"raster_gbx", "gtiff_gbx", "shapefile_gbx"}
    assert _register.resolve_only(
        ["raster", "GTIFF_GBX"], valid, normalizer=_register.normalize_datasource_name
    ) == {"raster_gbx", "gtiff_gbx"}


def test_resolve_only_returns_canonical_subset():
    valid = {"gbx_rst_slope", "gbx_rst_clip", "gbx_rst_width"}
    assert _register.resolve_only(["rst_slope", "GBX_RST_Clip"], valid) == {
        "gbx_rst_slope",
        "gbx_rst_clip",
    }


def test_resolve_only_empty_returns_empty_set():
    assert _register.resolve_only([], {"gbx_rst_slope"}) == set()


def test_resolve_only_unknown_raises_with_name_and_suggestion():
    with pytest.raises(ValueError) as ei:
        _register.resolve_only(["rst_slpe"], {"gbx_rst_slope", "gbx_rst_clip"})
    msg = str(ei.value)
    assert "rst_slpe" in msg
    assert "gbx_rst_slope" in msg  # close-match suggestion


def test_run_groups_only_registers_selected_and_runs_only_their_guards():
    calls = {"guardA": 0, "guardB": 0}
    registered = []

    def guardA():
        calls["guardA"] += 1

    def guardB():
        calls["guardB"] += 1

    groups = [
        (
            guardA,
            {
                "gbx_a_one": lambda s: registered.append("a_one"),
                "gbx_a_two": lambda s: registered.append("a_two"),
            },
        ),
        (guardB, {"gbx_b_one": lambda s: registered.append("b_one")}),
    ]
    _register.run_groups(groups, spark=None, only=["a_one"])
    assert registered == ["a_one"]
    assert calls == {"guardA": 1, "guardB": 0}  # guardB not run — no b fn selected


def test_run_groups_none_registers_all_and_runs_all_guards():
    calls = []
    registered = []
    groups = [
        (
            lambda: calls.append("gA"),
            {"gbx_a_one": lambda s: registered.append("a_one")},
        ),
        (
            lambda: calls.append("gB"),
            {"gbx_b_one": lambda s: registered.append("b_one")},
        ),
    ]
    _register.run_groups(groups, spark=None, only=None)
    assert registered == ["a_one", "b_one"]
    assert calls == ["gA", "gB"]


def test_run_groups_validates_against_union_of_groups():
    groups = [
        (lambda: None, {"gbx_a_one": lambda s: None}),
        (lambda: None, {"gbx_b_one": lambda s: None}),
    ]
    # b_one is valid (other group); typo is not
    with pytest.raises(ValueError):
        _register.run_groups(groups, spark=None, only=["a_one", "nope_x"])


def test_run_groups_empty_only_registers_nothing_and_no_guards():
    calls = []
    registered = []
    groups = [
        (
            lambda: calls.append("gA"),
            {"gbx_a_one": lambda s: registered.append("a_one")},
        ),
    ]
    _register.run_groups(groups, spark=None, only=[])
    assert registered == []
    assert calls == []  # no function selected => guard not run


class _Registry:
    """A session's ``udf`` / ``udtf``: what each registration was handed."""

    def __init__(self):
        self.got = {}

    def register(self, name, f, *args, **kwargs):
        self.got[name] = f
        return f


def test_overlapping_registrations_each_carry_their_own_switch_and_wrap_nothing_else(
    monkeypatch,
):
    """Two register() calls overlapping on threads (the first ends first) keep each one's remote-path switch; a
    UDF the user registers on the session meanwhile is registered as given; Spark's registration class is left as
    it was."""
    from databricks.labs.gbx import _gdal_policy

    class Session:
        def __init__(self):
            self.udf, self.udtf = _Registry(), _Registry()

    spark, original = Session(), _Registry.register

    def probe():
        return _gdal_policy._CARRIED.get()

    def user_fn():
        return "mine"

    def registrar(tag, entered, release):
        def reg(s):
            s.udf.register(f"{tag}1", probe)
            entered.set()
            assert release.wait(10)
            s.udf.register(f"{tag}2", probe)

        return [(lambda: None, {f"gbx_{tag}": reg})]

    events = {t: (threading.Event(), threading.Event()) for t in ("a", "b")}
    threads = {}
    for tag, switch in (("a", "true"), ("b", "false")):
        monkeypatch.setenv(
            _gdal_policy.REMOTE_ENV, switch
        )  # read as each register() starts
        threads[tag] = threading.Thread(
            target=_register.run_groups,
            args=(registrar(tag, *events[tag]), spark, None),
        )
        threads[tag].start()
        assert events[tag][0].wait(10)
    spark.udf.register(
        "user_fn", user_fn
    )  # both register() calls are inside their blocks
    for tag in ("a", "b"):  # the first one ends first
        events[tag][1].set()
        threads[tag].join(10)
    got = spark.udf.got
    assert {n: got[n]() for n in ("a1", "a2", "b1", "b2")} == {
        "a1": True,
        "a2": True,
        "b1": False,
        "b2": False,
    }
    assert got["user_fn"] is user_fn
    assert _Registry.register is original and "register" not in vars(spark.udf)
