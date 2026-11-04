"""bench.probes.run_procedures: the cases, the CALL text and the progress lines, on a session that records its SQL."""

import re

import pytest

from databricks.labs.gbx.bench import probes
from databricks.labs.gbx.uc import procedures


class _Result:
    def collect(self):
        return []

    def first(self):
        return None


class _Spark:
    def __init__(self):
        self.stmts = []

    def sql(self, text):
        self.stmts.append(text)
        return _Result()

    def createDataFrame(self, *_a, **_k):  # noqa: N802 - the Spark API name
        raise RuntimeError("no Spark in this test")


def _params(ddl_text):
    """(name, has_default) of each IN parameter of a procedure's DDL."""
    head = ddl_text.split("\n)\nLANGUAGE", 1)[0]
    return [
        (m.group(1), " DEFAULT " in m.group(0))
        for m in re.finditer(r"^  IN (\w+) [^\n]*$", head, re.M)
    ]


def test_each_case_calls_its_procedure_with_the_names_it_declares():
    cases = probes.proc_cases("c", "s")
    assert cases
    for case in cases:
        declared = _params(procedures.RECIPES[case["name"]]("c", "s"))
        assert set(case["args"]) <= {n for n, _ in declared}, case["name"]
        assert {n for n, d in declared if not d} <= set(case["args"]), case["name"]
        assert case["compare"] in ("rows", "pixels", "mvt")


def test_the_probe_covers_every_recipe_that_takes_a_table(monkeypatch):
    import inspect

    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    covered = {c["name"] for c in probes.proc_cases("c", "s")}
    assert covered == {
        "gbx_polygons_to_bng",
        "gbx_clean_geometries",
        "gbx_migrate_mosaic_geometry",
        "gbx_points_to_h3_surface",
        "gbx_features_to_mvt",
    }
    # building the cases takes no session, so it can send nothing
    assert list(inspect.signature(probes.proc_cases).parameters) == [
        "catalog",
        "schema",
    ]


def test_run_procedures_calls_each_procedure_and_prints_its_progress(
    monkeypatch, capsys, tmp_path
):
    import databricks.labs.gbx as gbx

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    spark = _Spark()
    out = probes.run_procedures(spark, "c", "s", str(tmp_path))
    text = capsys.readouterr().out
    calls = [s for s in spark.stmts if s.startswith("CALL ")]
    assert any(
        s.startswith("CALL `c`.`s`.gbx_polygons_to_bng(source_table => '")
        for s in calls
    )
    assert set(out["recipes"]) >= {
        "gbx_polygons_to_bng",
        "gbx_clean_geometries",
        "zarr",
    }
    for name in ("gbx_polygons_to_bng", "gbx_features_to_mvt"):
        assert f"[PROC] {name} call START" in text
        assert re.search(rf"\[PROC\] {name} call DONE [0-9.]+s rows=0", text)
        assert re.search(rf"\[PROC\] {name} session DONE [0-9.]+s", text)
    # both sides are empty here: that proves nothing, so it is not a match
    rec = out["recipes"]["gbx_polygons_to_bng"]
    assert rec["status"] == "empty" and rec["match"] is False
    assert out["dropped"]  # the tables are dropped at the end, whatever happened
    assert any(
        s.startswith("DROP TABLE IF EXISTS `c`.`s`.gbxproc_") for s in spark.stmts
    )


def test_a_failed_call_is_recorded_and_the_run_goes_on(monkeypatch, capsys, tmp_path):
    import databricks.labs.gbx as gbx

    class Failing(_Spark):
        def sql(self, text):
            if text.startswith("CALL ") and "gbx_clean_geometries" in text:
                raise RuntimeError("boom")
            return super().sql(text)

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    out = probes.run_procedures(Failing(), "c", "s", str(tmp_path))
    assert "boom" in out["recipes"]["gbx_clean_geometries"]["error"]
    assert out["recipes"]["gbx_migrate_mosaic_geometry"]["status"] == "empty"
    assert "[PROC] gbx_clean_geometries call DONE" in capsys.readouterr().out


@pytest.mark.parametrize(
    "value,want",
    [
        (b"ab", "sha:fb8e20fc2e4c3f24"),
        (1.23456789012, 1.23456789),
        ([1.0, b""], [1.0, "sha:e3b0c44298fc1c14"]),
    ],
)
def test_cells_are_compared_by_digest_and_rounded_value(value, want):
    assert probes._proc_norm(value) == want


def test_warehouse_statements_call_each_procedure_then_diff_it_against_plain_sql():
    stmts = dict(probes.proc_warehouse_statements("c", "s", "/Volumes/c/s/v/scratch"))
    ids = list(stmts)
    for name in procedures.RECIPES:
        if name.startswith("gbx_zarr_"):
            continue
        assert stmts[f"{name}:call"].startswith(f"CALL `c`.`s`.{name}("), name
        assert ids.index(f"{name}:call") < ids.index(f"{name}:diff"), name
    # every table a case makes is dropped at the end, and the zarr manifest is one of them
    created = {
        m.group(1)
        for sql in stmts.values()
        for m in [
            re.match(r"CREATE (?:OR REPLACE )?TABLE `c`\.`s`\.(gbxproc_\w+)", sql)
        ]
        if m
    }
    dropped = {c.split(":", 1)[1] for c in ids if c.startswith("drop:")}
    assert created <= dropped and "gbxproc_zm_wh" in dropped
    # a statement is one line of SQL per cid: no semicolon, no blank line (gbx-uc-sql splits on the cid marks)
    assert all(";" not in s and "\n\n" not in s for s in stmts.values())
    assert (
        "FILE" not in stmts["zarr:create"]
    )  # the fuse manifest: a plain STRING file column


def test_install_procedures_replaces_only_the_procedures_before_the_calls(
    monkeypatch, capsys, tmp_path
):
    import databricks.labs.gbx as gbx

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    spark = _Spark()
    out = probes.run_procedures(spark, "c", "s", str(tmp_path), install_procedures=True)
    creates = [
        s
        for s in spark.stmts
        if s.startswith(("CREATE OR REPLACE PROCEDURE", "CREATE OR REPLACE FUNCTION"))
    ]
    assert creates and all(
        s.startswith("CREATE OR REPLACE PROCEDURE `c`.`s`.gbx_") for s in creates
    )  # no function is touched
    assert out["installed"] == len(creates)
    first_call = next(i for i, s in enumerate(spark.stmts) if s.startswith("CALL "))
    assert spark.stmts.index(creates[-1]) < first_call
    assert "[PROC] install procedures DONE" in capsys.readouterr().out


class _Row:
    def __init__(self, **kw):
        self._d = kw

    def asDict(self, recursive=False):  # noqa: N802 - the Spark Row API name
        return dict(self._d)


def test_the_compare_reads_ewkb_and_wkb_as_one_geometry_and_merges_mvt_per_tile():
    import mapbox_vector_tile as mvt
    import shapely

    g = shapely.from_wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")
    plain = shapely.to_wkb(g)
    ewkb = shapely.to_wkb(shapely.set_srid(g, 27700), include_srid=True)
    assert plain != ewkb
    case = {"compare": "rows", "geometries": ["chip"]}
    assert probes._proc_rows([_Row(chip=plain)], case) == probes._proc_rows(
        [_Row(chip=ewkb)], case
    )

    def tile(*points):
        return mvt.encode(
            [
                {
                    "name": "layer",
                    "features": [
                        {"geometry": f"POINT ({x} {y})", "properties": {"n": n}}
                        for n, (x, y) in enumerate(points)
                    ],
                }
            ]
        )

    case = {"compare": "mvt"}
    merged = [_Row(z=0, x=0, y=0, mvt=tile((1, 1), (2, 2)))]
    split = [
        _Row(z=0, x=0, y=0, mvt=tile((1, 1))),
        _Row(z=0, x=0, y=0, mvt=tile((2, 2))),
    ]
    got = probes._proc_rows(merged, case)
    assert len(got) == 1 and len(got[0]["mvt"]) == 2
    # one tile with both features equals two one-feature tiles of the same tile id (the property n restarts at 0)
    assert [len(r["mvt"]) for r in probes._proc_rows(split, case)] == [2]


class _Rows(_Spark):
    """A session whose output and reference queries of the polygons-to-BNG case return the rows it is given."""

    def __init__(self, out_rows, ref_rows):
        super().__init__()
        self._out, self._ref = out_rows, ref_rows

    def sql(self, text):
        self.stmts.append(text)
        rows = []
        if "FROM `c`.`s`.gbxproc_bng_tgt" in text:
            rows = self._out
        elif "LATERAL gbx_bng_tessellateexplode" in text:
            rows = self._ref
        return type(
            "R", (), {"collect": lambda self_: rows, "first": lambda self_: None}
        )()


def _bng_status(monkeypatch, tmp_path, out_rows, ref_rows):
    import databricks.labs.gbx as gbx

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    out = probes.run_procedures(_Rows(out_rows, ref_rows), "c", "s", str(tmp_path))
    return out["recipes"]["gbx_polygons_to_bng"]


def test_a_result_that_differs_from_the_session_flow_is_a_mismatch(
    monkeypatch, tmp_path
):
    got = [_Row(id=1, cellid=5, core=True, chip=None)]
    want = [_Row(id=1, cellid=6, core=True, chip=None)]
    rec = _bng_status(monkeypatch, tmp_path, got, want)
    assert rec["status"] == "mismatch" and rec["match"] is False
    assert (
        rec["diff"]["procedure"][0]["cellid"] == 5
        and rec["diff"]["session"][0]["cellid"] == 6
    )


def test_equal_rows_match_and_two_empty_results_do_not(monkeypatch, tmp_path):
    rows = [_Row(id=1, cellid=5, core=True, chip=None)]
    rec = _bng_status(monkeypatch, tmp_path, rows, list(rows))
    assert rec["status"] == "match" and rec["match"] is True and "diff" not in rec
    rec = _bng_status(monkeypatch, tmp_path, [], [])
    assert rec["status"] == "empty" and rec["match"] is False
    # the procedure wrote nothing where the session flow has rows: a mismatch, not an empty
    assert _bng_status(monkeypatch, tmp_path, [], rows)["status"] == "mismatch"


def _zarr_tag(monkeypatch, tmp_path, tagged, twin):
    from databricks.labs.gbx.zarr import _maint
    from databricks.labs.gbx.zarr import _manifest as M

    class Spark(_Spark):
        def sql(self, text):
            self.stmts.append(text)
            rows = []
            if text.startswith("SELECT kind, array, key, write_id FROM"):
                rows = tagged if "zm_proc" in text else twin
            return type(
                "R", (), {"collect": lambda self_: rows, "first": lambda self_: None}
            )()

    monkeypatch.setattr(M, "create_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(_maint, "gbx_zarr_tag", lambda *_a, **_k: None)

    def step(_label, _surface, fn):
        return fn(), None, 0.0

    return probes._proc_zarr(Spark(), "c", "s", str(tmp_path), step, [])


def test_the_zarr_tag_compare_needs_tagged_rows_on_both_sides(monkeypatch, tmp_path):
    row = ("group", None, ".", "w2")
    assert _zarr_tag(monkeypatch, tmp_path, [row], [row])["tag_status"] == "match"
    assert (
        _zarr_tag(monkeypatch, tmp_path, [row], [("array", "v", "v/.zarray", "w2")])[
            "tag_status"
        ]
        == "mismatch"
    )
    rec = _zarr_tag(monkeypatch, tmp_path, [], [])
    assert rec["tag_status"] == "empty" and rec["tag_match"] is False


def test_the_tile_cases_register_each_table_before_creating_it():
    # createDataFrame raises in this session: the first create fails, and the tables are already listed
    tables = []
    with pytest.raises(RuntimeError, match="no Spark"):
        probes._proc_tile_cases(_Spark(), "c", "s", tables)
    assert "`c`.`s`.gbxproc_mos_src" in tables and "`c`.`s`.gbxproc_zs_tgt" in tables
