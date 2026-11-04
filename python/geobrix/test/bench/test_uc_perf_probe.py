"""The Unity Catalog performance probes (bench.uc_perf): the statement text, the gating on functions that do not exist yet,
the runners on a session that records its SQL, and the notebook writer. Nothing here touches Databricks.
"""

import copy
import json
import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles, probes, uc_perf

WHEEL = "/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl"
JAR = "/Volumes/c/s/v/geobrix-0.6.0-jar-with-dependencies.jar"
RAW = json.loads((Path(manifest.__file__).with_name("functions.json")).read_text())


def _sql(**kw):
    return probes.uc_perf_sql("cat", "sch", kw.pop("jar", JAR), WHEEL, **kw)


def test_the_warehouse_file_is_one_statement_per_cid_probes_only_and_round_trips():
    pairs = _sql()
    cids = [c for c, _ in pairs]
    assert len(cids) == len(set(cids))
    for cid, s in pairs:
        assert ";" not in s and "\n\n" not in s, cid
        assert not any(ln.lstrip().startswith("--") for ln in s.splitlines()), cid
    # every statement is qualified with the 3-level `cat`.`sch`., except the floor and the inline-expression controls
    unqualified = {cid for cid, s in pairs if "`cat`.`sch`." not in s}
    assert unqualified == {
        "m0:select1",
        "m1:explain_inline",
        "m1:t_inline_1m",
        "m1:t_inline_10m",
    }
    # the file the warehouse runner reads gives the same pairs back
    assert oracles.parse_uc_sql(oracles.uc_sql_text(pairs)) == pairs
    # only probe routines and tables are created or dropped: the product functions are never replaced
    for cid, s in pairs:
        m = re.match(r"CREATE OR REPLACE (?:FUNCTION|TABLE) (\S+?)[( ]", s)
        if m:
            assert m.group(1).split(".")[-1].startswith("gbx_mvp_"), cid
        d = re.match(r"(?:DROP FUNCTION IF EXISTS|DROP TABLE IF EXISTS) (\S+)", s)
        if d:
            assert d.group(1).split(".")[-1].startswith("gbx_mvp_perf_"), cid


def test_every_measurement_is_built_or_says_what_it_needs():
    plan = uc_perf.perf_plan("cat", "sch", jar_path=JAR, wheel_path=WHEEL)
    assert [u.id for u in plan] == [
        f"m{i}" for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13)
    ]
    for u in plan:
        assert u.legs or u.skipped, u.id  # a unit is never silently empty
    missing = uc_perf.skipped(plan)
    # the functions that other work adds are named, and the units that do not need them are whole
    assert "m5" not in missing  # the partial and its seven table functions exist
    assert (
        "m6" not in missing
    )  # the SQL fill, the quadbin union and the BNG chip union all exist
    assert "m7" not in missing  # the partial and the finalize exist, so the recipe legs are built
    assert "m8" not in missing  # both tree procedures exist, so the tree leg is built
    assert "gbx_rst_zonalstats_partial" in missing["m11"]["partial"]
    assert not {"m0", "m1", "m12", "m13"} & set(missing)
    # the legs that can run today do: the native recipe C1 and the Python twin of the fill, the array twin of binpoints
    labels = {u.id: [leg.label for leg in u.legs] for u in plan}
    assert {"c1_100k", "py_twin_100k", "sql_array_100k"} <= set(labels["m6"])
    # the chip union is timed three ways: the native recipe, the SQL fold and the Python twin (a probe-only routine)
    assert {"c_c3", "c_fold", "c_python_twin"} <= set(labels["m6"])
    m6 = {u.id: u for u in plan}["m6"]
    assert any("gbx_mvp_perf_bng_chips_union" in sql for _, sql in m6.setup)
    assert all(
        "struct(cellid, core, chip)" in leg.sql
        for leg in m6.legs
        if leg.label in ("c_fold", "c_python_twin")
    )
    assert {"twin_1m", "d1_1m"} <= set(labels["m7"])


def test_a_unit_whose_function_arrives_builds_with_no_change_to_the_probe(monkeypatch):
    monkeypatch.setattr(uc_perf, "_has_uc", lambda c, *names: [])
    monkeypatch.setattr(
        uc_perf, "_procedure_names", lambda: ("gbx_mosaic_by_key", "gbx_combine_by_key")
    )
    plan = {
        u.id: u
        for u in uc_perf.perf_plan(
            "cat", "sch", jar_path=JAR, wheel_path=WHEEL, surface="s6"
        )
    }
    m5 = " ".join(leg.sql for leg in plan["m5"].legs)
    for fn in ("rastertogrid_partial", *(f"rastertogrid{s}" for s in uc_perf.H3_STATS)):
        assert fn in m5
    labels = [leg.label for leg in plan["m5"].legs]
    assert {"fused_10t", "single_avg_10t", "f1_100t", "tvf_groupby_1000t"} <= set(
        labels
    )
    assert any(
        leg.label == "d1_1m" and "binpoints_finalize" in leg.sql
        for leg in plan["m7"].legs
    )
    assert any(
        leg.label == "combine_by_key_10t" and "op => 'avg'" in leg.sql
        for leg in plan["m8"].legs
    )
    assert any(
        leg.label == "sql_array_1k" and "gbx_h3_cellfill_array" in leg.sql
        for leg in plan["m6"].legs
    )
    assert not uc_perf.skipped(plan.values()).get("m5")


def test_a_sql_kernel_adds_a_sql_copy_and_an_equality_leg_against_python():
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if r["sql_name"] == "gbx_bng_cellarea")
    row["levels"]["L4"]["sql"] = "0.0 * length(cellid)"
    man = manifest.parse(raw)
    c = uc_perf.Ctx(
        "cat",
        "sch",
        "`cat`.`sch`",
        "file://" + WHEEL,
        JAR,
        "/t",
        dict(uc_perf.DEFAULT_SIZES),
        "w",
        man,
        "0.6.0",
    )
    u = uc_perf.Unit("m2", "t")
    forms = uc_perf._forms(c, u, "bng_cellarea", "gbx_bng_cellarea")
    assert set(forms) == {"py", "sql", "scala"}
    created = dict(u.setup)
    assert "LANGUAGE SQL\nDETERMINISTIC" in created["create_bng_cellarea_sql"]
    assert "LANGUAGE SCALA" in created["create_bng_cellarea_scala"]
    assert "LANGUAGE PYTHON" in created["create_bng_cellarea_py"]
    # no JAR: no Scala copy, and the reason is recorded
    c2 = uc_perf.Ctx(**{**c.__dict__, "jar_path": None})
    u2 = uc_perf.Unit("m2", "t")
    assert "scala" not in uc_perf._forms(c2, u2, "bng_cellarea", "gbx_bng_cellarea")
    assert u2.skipped["bng_cellarea_scala"] == "no JAR given"


def test_m3_runs_one_scale_on_a_warehouse_and_two_on_serverless():
    w = {
        u.id: u
        for u in uc_perf.perf_plan(
            "cat", "sch", wheel_path=WHEEL, surface="w", only=["m3"]
        )
    }
    s = {
        u.id: u
        for u in uc_perf.perf_plan(
            "cat", "sch", wheel_path=WHEEL, surface="s6", only=["m3"]
        )
    }
    assert not any("100m" in leg.label for leg in w["m3"].legs)
    assert any("100m" in leg.label for leg in s["m3"].legs)
    # the session-only legs stay off the warehouse file
    w8 = uc_perf.flatten(
        uc_perf.perf_plan("cat", "sch", wheel_path=WHEEL, surface="w", only=["m8"])
    )
    s8 = uc_perf.flatten(
        uc_perf.perf_plan("cat", "sch", wheel_path=WHEEL, surface="s6", only=["m8"])
    )
    assert not any("gbx_rst_merge_agg(" in sql for _, sql in w8)
    assert any("gbx_rst_merge_agg(" in sql for _, sql in s8)


def test_m4_compares_per_function_and_shared_dependency_lists_on_copies():
    plan = {
        u.id: u for u in uc_perf.perf_plan("cat", "sch", wheel_path=WHEEL, only=["m4"])
    }
    u = plan["m4"]
    created = " ".join(s for _, s in u.setup)
    # six probe copies, each with the whole dependency list
    deps = manifest.load().uc_dependencies
    for name in ("sh_kring", "sh_width", "sh_ndvi"):
        assert f"gbx_mvp_perf_{name}" in created
    env = (
        re.search(r"dependencies = '(\[.*?\])'", u.setup[0][1])
        .group(1)
        .replace("\\'", "'")
    )
    assert sorted(json.loads(env)[1:]) == sorted(f"{n}=={v}" for n, v in deps.items())
    labels = {leg.label for leg in u.legs}
    assert {
        "a_1fn_1r",
        "b_1fn_1r",
        "a_3fn_100kr",
        "b_3fn_100kr",
        "a_6fn_1r",
        "b_6fn_1r",
        "c_once_100kr",
        "c_twice_100kr",
    } <= labels
    by = {leg.label: leg.sql for leg in u.legs}
    assert (
        "gbx_mvp_perf_sh_" in by["b_3fn_100kr"]
        and "gbx_mvp_perf_sh_" not in by["a_3fn_100kr"]
    )
    assert (
        "perf_64MiB.tif" in by["b_width_64mib"]
        and "perf_1MiB.tif" in by["b_width_1mib"]
    )


def _kernel_manifest():
    """The packaged manifest with a SQL kernel and a helper on the two grid scalars the probes copy."""
    raw = copy.deepcopy(RAW)
    for name, arg in (("gbx_bng_cellarea", "cellid"), ("gbx_bng_eastnorthasbng", "e")):
        row = next(r for r in raw["functions"] if r["sql_name"] == name)
        row["levels"]["L4"]["sql"] = f"{{q}}gbx__shared_helper({arg})"
        row["levels"]["L4"]["sql_helpers"] = [
            {
                "name": "gbx__shared_helper",
                "params": [[arg, "STRING" if arg == "cellid" else "DOUBLE"]],
                "returns": "DOUBLE",
                "sql": "1.0",
            }
        ]
    return manifest.parse(raw)


def test_a_sql_probe_creates_its_own_helpers_and_drops_them_never_the_products():
    man = _kernel_manifest()
    c = uc_perf.Ctx(
        "cat",
        "sch",
        "`cat`.`sch`",
        "file://" + WHEEL,
        None,
        "/t",
        dict(uc_perf.DEFAULT_SIZES),
        "w",
        man,
        "0.6.0",
    )
    u = uc_perf.Unit("m2", "t")
    uc_perf._copy(c, u, "gbx_bng_cellarea", "bng_cellarea_sql", form="sql")
    created = "\n".join(s for _, s in u.setup)
    assert "gbx__shared_helper" not in created  # the product's helper is not touched
    assert "FUNCTION `cat`.`sch`.gbx_mvp_perf_shared_helper(" in created
    assert "RETURN `cat`.`sch`.gbx_mvp_perf_shared_helper(cellid)" in created
    assert sorted(s for _, s in u.drops) == [
        "DROP FUNCTION IF EXISTS `cat`.`sch`.gbx_mvp_perf_bng_cellarea_sql",
        "DROP FUNCTION IF EXISTS `cat`.`sch`.gbx_mvp_perf_shared_helper",
    ]


def test_every_routine_a_probe_creates_or_drops_is_a_gbx_mvp_routine(monkeypatch):
    monkeypatch.setattr(manifest, "load", _kernel_manifest)
    monkeypatch.setattr(uc_perf, "_has_uc", lambda c, *names: [])
    monkeypatch.setattr(
        uc_perf,
        "_procedure_names",
        lambda: ("gbx_mosaic_by_key", "gbx_combine_by_key", "gbx_features_to_mvt"),
    )
    plan = uc_perf.perf_plan("cat", "sch", jar_path=JAR, wheel_path=WHEEL, surface="s6")
    stmts = [sql for _, sql in uc_perf.flatten(plan)]
    made = [
        m.group(1)
        for sql in stmts
        for m in [re.match(r"CREATE OR REPLACE (?:TEMPORARY )?FUNCTION (\S+?)\(", sql)]
        if m
    ]
    dropped = [
        m.group(1)
        for sql in stmts
        for m in [re.match(r"DROP FUNCTION IF EXISTS (\S+)", sql)]
        if m
    ]
    assert any("shared_helper" in n for n in made)  # the kernels were exercised
    for name in made + dropped:
        assert name.split(".")[-1].startswith("gbx_mvp_"), name


# ---- the runners on a session that records its SQL -----------------------------------------------------------


class _Rows(list):
    pass


class _Spark:
    def __init__(self, fail_on=()):
        self.stmts, self.fail_on = [], tuple(fail_on)

    def sql(self, text):
        self.stmts.append(text)
        if any(f in text for f in self.fail_on):
            raise RuntimeError("[UNSUPPORTED_SQL_UDF_USAGE] boom\nsecond line")
        return type("R", (), {"collect": lambda self_: _Rows([(1,)])})()


@pytest.fixture
def no_volume(monkeypatch):
    from databricks.labs.gbx import uc

    monkeypatch.setattr(uc, "_single_wheel", lambda d: WHEEL)
    monkeypatch.setattr(probes, "_sha256", lambda p: "sha")
    staged = []
    monkeypatch.setattr(
        uc_perf, "perf_stage_tiles", lambda d, mibs=(): staged.append(d) or {"ok": 1}
    )
    return staged


def test_run_uc_perf_prints_start_and_done_times_each_leg_and_drops_after_an_error(
    no_volume, capsys
):
    spark = _Spark(fail_on=("gbx_mvp_perf_py_env(1)",))
    out = probes.run_uc_perf(
        spark, "cat", "sch", "/Volumes/c/s/v", only=["m0"], repeats=3, cap_s=60
    )
    text = capsys.readouterr().out
    assert "[T28 uc-perf] m0:select1#1 START (cap 60s)" in text
    assert re.search(r"\[T28 uc-perf\] m0:select1#3 DONE [\d.]+s ok", text)
    assert re.search(
        r"\[T28 uc-perf\] m0:py_env#1 DONE [\d.]+s ERROR RuntimeError", text
    )
    legs = out["units"]["m0"]["legs"]
    assert len(legs["select1"]["wall_s"]) == 3 and legs["select1"]["value"] == "1"
    assert (
        legs["py_env"]["error"].startswith("RuntimeError")
        and "\n" not in legs["py_env"]["error"]
    )
    assert len(legs["py_env"]["wall_s"]) == 1  # an error ends that statement's repeats
    assert out["wheel"] == WHEEL and out["wheel_sha256"] == "sha"
    # every probe function is dropped, after the error too, and the tiles were staged first
    assert [s for s in spark.stmts if s.startswith("DROP FUNCTION IF EXISTS")]
    assert len(out["dropped"]) == 3 and no_volume == ["/Volumes/c/s/v/perf_tiles"]


def test_run_uc_perf_registers_the_session_functions_a_leg_calls(
    no_volume, monkeypatch
):
    registered = []
    from databricks.labs import gbx

    monkeypatch.setattr(
        gbx, "register", lambda spark, only=None, **kw: registered.append(list(only))
    )
    probes.run_uc_perf(
        _Spark(),
        "cat",
        "sch",
        "/Volumes/c/s/v",
        only=["m8"],
        sizes={"m8": (10,)},
        repeats=1,
    )
    assert registered == [["gbx_rst_merge_agg"]]


def test_run_proc_warm_times_cold_warm_and_idle_calls_and_both_target_forms(capsys):
    spark = _Spark()
    out = probes.run_proc_warm(
        spark, "cat", "sch", idle_s=0, calls=3, polygons=10, cap_s=60
    )
    text = capsys.readouterr().out
    calls = [
        i
        for i, s in enumerate(spark.stmts)
        if s.startswith("CALL `cat`.`sch`.gbx_polygons_to_bng(")
    ]
    assert len(calls) == 4  # three back to back, one after the idle gap
    assert all(
        spark.stmts[i - 1].startswith("DROP TABLE IF EXISTS") for i in calls
    )  # every CALL creates its target
    assert (
        len(out["call_s"]) == 3
        and out["after_idle_s"] is not None
        and out["idle_s"] == 0
    )
    old = next(s for s in spark.stmts if "warm_old" in s and s.startswith("CREATE"))
    new = next(s for s in spark.stmts if "warm_new" in s and s.startswith("CREATE"))
    assert "WHERE false" in old and "gbx_bng_kring" in old
    assert "(id INT, ring ARRAY<STRING>)" in new and "gbx_bng_kring" not in new
    assert out["create_old_s"] is not None and out["create_new_s"] is not None
    assert "[T28 uc-perf] m9:call_1 START (cap 60s)" in text and "m9:idle" in text
    assert (
        spark.stmts[-1].startswith("DROP TABLE IF EXISTS") and len(out["dropped"]) == 4
    )


def test_run_proc_warm_records_a_failed_call_and_still_drops(capsys):
    spark = _Spark(fail_on=("CALL ",))
    out = probes.run_proc_warm(spark, "cat", "sch", idle_s=0, calls=1, polygons=2)
    assert set(out["errors"]) >= {"call_1", "call_after_idle"}
    assert len(out["dropped"]) == 4


def test_the_notebook_writer_makes_the_two_serverless_notebooks(tmp_path):
    paths = uc_perf.write_uc_perf_notebooks(
        str(tmp_path),
        WHEEL,
        "cat",
        "sch",
        "/Volumes/c/s/v",
        jar_path=JAR,
        only=["m0", "m4"],
        idle_s=30,
    )
    perf = json.loads(Path(paths["uc_perf"]).read_text())
    warm = json.loads(Path(paths["proc_warm"]).read_text())
    run_cell = perf["cells"][2]["source"]
    assert (
        "probes.run_uc_perf(spark, **" in run_cell
        and "'only': ['m0', 'm4']" in run_cell
        and JAR in run_cell
    )
    assert f"geobrix @ file://{WHEEL}" in perf["cells"][0]["source"]
    assert (
        "probes.run_proc_warm(spark, **" in warm["cells"][2]["source"]
        and "'idle_s': 30" in warm["cells"][2]["source"]
    )


def test_the_staged_tiles_are_valid_georeferenced_geotiffs_of_the_decoded_size(
    tmp_path,
):
    import rasterio

    out = uc_perf.perf_stage_tiles(str(tmp_path), mibs=(1,))
    assert set(out) == {"perf_1MiB.tif", "perf_4band.tif"}
    with rasterio.open(out["perf_1MiB.tif"]["path"]) as ds:
        assert (ds.width, ds.height, ds.count, str(ds.dtypes[0])) == (
            512,
            512,
            1,
            "float32",
        )
        assert ds.crs.to_epsg() == 32633 and ds.width * ds.height * 4 == 1 << 20
    with rasterio.open(out["perf_4band.tif"]["path"]) as ds:
        assert (ds.width, ds.height, ds.count) == (512, 512, 4)
    again = uc_perf.perf_stage_tiles(
        str(tmp_path), mibs=(1,)
    )  # a file that is there is kept
    assert again["perf_1MiB.tif"]["bytes"] == out["perf_1MiB.tif"]["bytes"]
    assert uc_perf._side(64) == 4096 and uc_perf._side(256) == 8192


# ---- a probe degrades to `skipped` and never crashes ---------------------------------------------------------


def _manifest_without(*names, private=(), edit=None):
    raw = copy.deepcopy(RAW)
    raw["functions"] = [r for r in raw["functions"] if r["sql_name"] not in names]
    raw["uc_private"] = [
        r for r in raw.get("uc_private", []) if r["name"] not in private
    ]
    if edit:
        edit(raw)
    return manifest.parse(raw)


def test_a_row_with_no_handler_has_no_batch_twin_and_a_missing_row_is_skipped(
    monkeypatch,
):
    man = manifest.load()
    bare = type(man.functions["gbx_bng_cellarea"])(
        **{**man.functions["gbx_bng_cellarea"].__dict__, "levels": {"L3": {}}}
    )
    assert uc_perf._has_batch(bare) is False  # no L4 at all: no KeyError
    handlerless = type(bare)(**{**bare.__dict__, "levels": {"L4": {"sql": "1.0"}}})
    assert uc_perf._has_batch(handlerless) is False  # no handler: no ValueError

    gone = _manifest_without(
        "gbx_bng_cellarea",
        "gbx_bng_eastnorthasbng",
        "gbx_bng_polyfill",
        "gbx_rst_clip",
        "gbx_st_asmvt_pyramid",
        private=("gbx__h3_cellfill_cells", "gbx__st_asmvt_features"),
    )
    monkeypatch.setattr(manifest, "load", lambda: gone)
    plan = {
        u.id: u for u in uc_perf.perf_plan("cat", "sch", jar_path=JAR, wheel_path=WHEEL)
    }
    missing = uc_perf.skipped(plan.values())
    assert {"bng_cellarea", "bng_eastnorthasbng"} <= set(missing["m2"])
    assert {"bng_eastnorthasbng", "polyfill"} <= set(missing["m3"])
    assert "py_twin" in missing["m6"]
    assert "gbx_rst_clip" in missing["m12"]["m12"] and not plan["m12"].legs
    assert "gbx_st_asmvt_pyramid" in missing["m13"]["m13"] and not plan["m13"].legs


def test_m13_needs_the_procedures_for_its_call_and_its_finalize_leg(monkeypatch):
    monkeypatch.setattr(uc_perf, "_procedure_names", lambda: ())
    (u,) = uc_perf.perf_plan("cat", "sch", wheel_path=WHEEL, only=["m13"])
    assert [leg.label for leg in u.legs] == ["pyramid_only"]
    assert {"call", "finalize_only"} <= set(u.skipped)
    assert "procedures=True" in u.skipped["finalize_only"]
    monkeypatch.undo()
    (whole,) = uc_perf.perf_plan("cat", "sch", wheel_path=WHEEL, only=["m13"])
    assert {"call", "pyramid_only", "finalize_only"} <= {
        leg.label for leg in whole.legs
    }


def test_m13_gates_on_the_procedures_the_run_installed_not_the_ones_the_wheel_defines():
    assert "gbx_features_to_mvt" in uc_perf._procedure_names()  # the wheel defines it
    (none,) = uc_perf.perf_plan(
        "cat", "sch", wheel_path=WHEEL, only=["m13"], procedures=()
    )
    assert {"call", "finalize_only"} <= set(none.skipped)
    assert "call" not in {leg.label for leg in none.legs}
    (some,) = uc_perf.perf_plan(
        "cat",
        "sch",
        wheel_path=WHEEL,
        only=["m13"],
        procedures=("gbx_features_to_mvt",),
    )
    assert "call" in {leg.label for leg in some.legs}


def test_the_installed_procedures_are_read_from_the_schema_and_an_unreadable_schema_is_unknown(
    capsys,
):
    from types import SimpleNamespace as NS

    rows = [{"routine_name": n} for n in ("GBX_FEATURES_TO_MVT", "gbx_bng_kring")]
    spark = NS(sql=lambda q: NS(collect=lambda: rows))
    assert uc_perf._installed_procedures(spark, "cat", "sch") == (
        "gbx_features_to_mvt",
    )

    def boom(q):
        raise RuntimeError("no information_schema")

    assert uc_perf._installed_procedures(NS(sql=boom), "cat", "sch") is None
    assert "schema not read" in capsys.readouterr().out


def test_the_diff_legs_are_skipped_when_the_python_copy_is_not_there():
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if r["sql_name"] == "gbx_bng_cellarea")
    row["levels"]["L4"]["sql"] = "0.0 * length(cellid)"
    del row["levels"]["L4"]["handler"]
    man = manifest.parse(raw)
    c = uc_perf.Ctx(
        "cat",
        "sch",
        "`cat`.`sch`",
        "file://" + WHEEL,
        None,
        "/t",
        dict(uc_perf.DEFAULT_SIZES),
        "w",
        man,
        "0.6.0",
    )
    u = uc_perf._m2(
        c
    )  # a row with no Python handler: the sql copy times, no diff leg reads forms["py"]
    assert "diff_bng_cellarea" in u.skipped
    assert not any(leg.label.startswith("diff_bng_cellarea") for leg in u.legs)
    assert any(leg.label.startswith("t_bng_cellarea_sql") for leg in u.legs)


def test_leg_labels_are_unique_per_unit_with_several_sizes_and_m4_has_no_repeated_rung():
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if r["sql_name"] == "gbx_bng_cellarea")
    row["levels"]["L4"]["sql"] = "0.0 * length(cellid)"
    man = manifest.parse(raw)
    import unittest.mock as mock

    with mock.patch.object(manifest, "load", lambda: man):
        plan = uc_perf.perf_plan(
            "cat",
            "sch",
            jar_path=JAR,
            wheel_path=WHEEL,
            sizes={"m2_diff": (100, 200), "m4_rows": (1, 100)},
        )
    for u in plan:
        labels = [leg.label for leg in u.legs]
        assert len(labels) == len(set(labels)), u.id
    m2 = [leg.label for leg in next(u for u in plan if u.id == "m2").legs]
    assert (
        "diff_bng_cellarea_sql_vs_py_100" in m2
        and "diff_bng_cellarea_sql_vs_py_200" in m2
    )


def test_the_step_total_counts_the_register_and_cleanup_steps(no_volume, monkeypatch):
    seen = []
    real = uc_perf._Run

    class Run(real):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            seen.append(self)

    monkeypatch.setattr(uc_perf, "_Run", Run)
    from databricks.labs import gbx

    monkeypatch.setattr(gbx, "register", lambda spark, only=None, **kw: None)
    probes.run_uc_perf(
        _Spark(),
        "cat",
        "sch",
        "/Volumes/c/s/v",
        only=["m8"],
        sizes={"m8": (10,)},
        repeats=1,
    )
    (run,) = seen
    assert (
        run.n == run.total
    )  # stage + register + setup + legs + one cleanup, no more and no fewer


def test_run_proc_warm_drops_the_rest_when_one_drop_fails(capsys):
    spark = _Spark(fail_on=("DROP TABLE IF EXISTS `cat`.`sch`.gbx_mvp_perf_warm_tgt",))
    # the target drops before each CALL fail too; the final cleanup must still try every table
    out = probes.run_proc_warm(spark, "cat", "sch", idle_s=0, calls=1, polygons=2)
    cleanup = [e for e in out["errors"] if e.startswith("drop:")]
    assert cleanup == ["drop:drop_gbx_mvp_perf_warm_tgt"]
    assert len(out["dropped"]) == 4 and any("ERROR" in d for d in out["dropped"])
    assert spark.stmts[-1].endswith(
        "gbx_mvp_perf_warm_new"
    )  # the drops after the failed one still ran


def test_proc_warm_plan_quotes_both_names_and_refuses_a_backtick():
    plan = uc_perf.proc_warm_plan("cat", "sch")
    assert "`cat`.`sch`.gbx_mvp_perf_warm_src" in plan["setup"][0][1]
    with pytest.raises(ValueError, match="schema"):
        uc_perf.proc_warm_plan("cat", "sch`; DROP SCHEMA x")
    with pytest.raises(ValueError, match="catalog"):
        uc_perf.proc_warm_plan("ca`t", "sch")


def _w_ctx(jar=JAR):
    return uc_perf.Ctx(
        "cat",
        "sch",
        "`cat`.`sch`",
        "file://" + WHEEL,
        jar,
        "/t",
        dict(uc_perf.DEFAULT_SIZES),
        "w",
        manifest.load(),
        "0.6.0",
    )


def test_the_candidate_bodies_give_m2_its_sql_leg_with_no_manifest_sql_key():
    c = _w_ctx()
    u = uc_perf.Unit("m2", "t")
    forms = uc_perf._forms(c, u, "bng_eastnorthasbng", "gbx_bng_eastnorthasbng")
    assert set(forms) == {"py", "sql", "scala", "batch"}
    created = dict(u.setup)
    names = [k for k in created if k.startswith("create_bng_eastnorthasbng_sql")]
    sql = created[names[-1]]  # the helpers are created first, the function last
    assert (
        "gbx_mvp_perf_bng_eastnorthasbng_sql(" in sql
        and "LANGUAGE SQL\nDETERMINISTIC" in sql
    )
    # the candidate's helpers are probe routines of their own, created before the function and dropped with it
    assert any("gbx_mvp_perf_bng_cell(" in created[k] for k in names[:-1])
    assert all("gbx__bng_" not in created[k] for k in names)
    dropped = " ".join(s for _, s in u.drops)
    assert (
        "gbx_mvp_perf_bng_resindex" in dropped
        and "`gbx_bng_eastnorthasbng`" not in dropped
    )
    assert "sql" not in manifest.load().functions["gbx_bng_eastnorthasbng"].levels["L4"]
    # no candidate and no kernel: no SQL leg, with the reason
    u2 = uc_perf.Unit("m2", "t")
    forms2 = uc_perf._forms(c, u2, "bng_polyfill", "gbx_bng_polyfill")
    assert (
        "sql" not in forms2
        and u2.skipped["bng_polyfill_sql"] == "no SQL kernel for this row yet"
    )


def test_m2_times_and_diffs_the_sql_candidate_of_both_grid_scalars():
    u = uc_perf._m2(_w_ctx())
    labels = {leg.label for leg in u.legs}
    for short in ("bng_cellarea", "bng_eastnorthasbng"):
        assert f"t_{short}_sql_1m" in labels and f"t_{short}_sql_10m" in labels
        assert f"diff_{short}_sql_vs_py_100k" in labels
        assert f"t_{short}_py_1m" in labels and f"t_{short}_scala_1m" in labels
    assert not [k for k in u.skipped if k.endswith("_sql")]


def test_m1_compares_the_products_own_quadbin_kernel_with_the_same_expression_inline():
    u = uc_perf._m1(_w_ctx())
    created = dict(u.setup)["create_qres"]
    assert "gbx_mvp_perf_qres(" in created and "(cell >> 52) & 31" in created
    inline = next(leg.sql for leg in u.legs if leg.label == "explain_inline")
    assert "`cat`" not in inline and "(shiftleft(id % 27, 52)) >> 52" in inline
    assert [leg.label for leg in u.legs][-2:] == ["t_sql_10m", "t_inline_10m"]
    # a wheel without the function says what it needs and builds nothing
    gone = uc_perf._m1(
        uc_perf.Ctx(
            **{**_w_ctx().__dict__, "man": _manifest_without("gbx_quadbin_resolution")}
        )
    )
    assert not gone.legs and "gbx_quadbin_resolution" in gone.skipped["qres"]
