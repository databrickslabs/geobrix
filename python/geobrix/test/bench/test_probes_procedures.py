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
    spark = _Spark()
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    covered = {c["name"] for c in probes.proc_cases("c", "s")}
    assert covered == {
        "gbx_polygons_to_bng",
        "gbx_clean_geometries",
        "gbx_migrate_mosaic_geometry",
        "gbx_points_to_h3_surface",
        "gbx_features_to_mvt",
    }
    assert spark.stmts == []  # building the cases sends nothing


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
    assert out["recipes"]["gbx_polygons_to_bng"]["match"] is True
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
    assert out["recipes"]["gbx_migrate_mosaic_geometry"].get("match") is True
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
