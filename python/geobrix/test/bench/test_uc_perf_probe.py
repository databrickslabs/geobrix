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
        assert "`cat`.`sch`." in s or s.startswith(
            ("SELECT 1", "EXPLAIN", "WITH t AS", "SELECT")
        ), cid
    # the file the warehouse runner reads gives the same pairs back
    assert oracles.parse_uc_sql(oracles.uc_sql_text(pairs)) == pairs
    # only probe routines and tables are created or dropped: the product functions are never replaced
    for cid, s in pairs:
        m = re.match(r"CREATE OR REPLACE (?:FUNCTION|TABLE) (\S+?)[( ]", s)
        if m:
            assert m.group(1).split(".")[-1].startswith(("gbx_mvp_", "gbx__")), cid
        d = re.match(r"(?:DROP FUNCTION IF EXISTS|DROP TABLE IF EXISTS) (\S+)", s)
        if d:
            assert (
                d.group(1).split(".")[-1].startswith(("gbx_mvp_perf_", "gbx__mvp_"))
            ), cid


def test_every_measurement_is_built_or_says_what_it_needs():
    plan = uc_perf.perf_plan("cat", "sch", jar_path=JAR, wheel_path=WHEEL)
    assert [u.id for u in plan] == [
        f"m{i}" for i in (0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13)
    ]
    for u in plan:
        assert u.legs or u.skipped, u.id  # a unit is never silently empty
    missing = uc_perf.skipped(plan)
    # the functions that other work adds are named, and the units that do not need them are whole
    assert "gbx_rst_h3_rastertogrid_partial" in missing["m5"]["m5"]
    assert "gbx_h3_cellfill_array" in missing["m6"]["sql_array"]
    assert "gbx_rst_binpoints_finalize" in missing["m7"]["d1"]
    assert "gbx_combine_by_key" in missing["m8"]["tree"]
    assert "gbx_rst_zonalstats_partial" in missing["m11"]["partial"]
    assert not {"m0", "m1", "m12", "m13"} & set(missing)
    # the legs that can run today do: the native recipe C1 and the Python twin of the fill, the array twin of binpoints
    labels = {u.id: [leg.label for leg in u.legs] for u in plan}
    assert "c1_100k" in labels["m6"] and "py_twin_100k" in labels["m6"]
    assert "twin_1m" in labels["m7"] and "d1_1m" not in labels["m7"]


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


def test_the_product_helpers_of_a_sql_probe_are_never_dropped():
    raw = copy.deepcopy(RAW)
    row = next(r for r in raw["functions"] if r["sql_name"] == "gbx_bng_cellarea")
    row["levels"]["L4"]["sql"] = "{q}gbx__shared_helper(cellid)"
    row["levels"]["L4"]["sql_helpers"] = [
        {
            "name": "gbx__shared_helper",
            "params": [["cellid", "STRING"]],
            "returns": "DOUBLE",
            "sql": "1.0",
        }
    ]
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
    u = uc_perf.Unit("m2", "t")
    uc_perf._copy(c, u, "gbx_bng_cellarea", "bng_cellarea_sql", form="sql")
    assert any(
        "gbx__shared_helper(" in s for _, s in u.setup
    )  # created beside the probe
    assert [s for _, s in u.drops] == [
        "DROP FUNCTION IF EXISTS `cat`.`sch`.gbx_mvp_perf_bng_cellarea_sql"
    ]


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
