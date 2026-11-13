"""Tests for examples/tests/uc_examples_sql.py (the gbx:examples:uc-sql launcher): no workspace, no SDK call."""

import importlib
import re
import subprocess
from pathlib import Path

import pytest

from conftest import REPO_ROOT

EXAMPLES = REPO_ROOT / "examples" / "uc-functions"


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    return importlib.import_module("uc_examples_sql")


def test_help_exits_zero_and_names_the_options():
    res = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "commands" / "gbx-examples-uc-sql.sh"), "--help"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, res.stderr
    for option in ("--profile", "--files", "--upload", "--upload-only", "--dry-run"):
        assert option in res.stdout


def test_an_unknown_option_exits_2():
    res = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "commands" / "gbx-examples-uc-sql.sh"), "--nope"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 2


def test_dry_run_parses_every_example_file_with_no_auth(runner, capsys):
    assert runner.main(["--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "dry run:" in out and "upload no" in out
    for f in EXAMPLES.glob("*.sql"):
        assert f.name in out


def test_files_selects_by_prefix_in_name_order(runner):
    names = [f.name[:2] for f in runner.example_files(EXAMPLES, "05,00")]
    assert names == ["00", "05"]
    with pytest.raises(SystemExit):
        runner.example_files(EXAMPLES, "99")


def test_only_selects_and_withs_must_return_rows(runner):
    assert runner.is_query("  SELECT 1") and runner.is_query("with t as (select 1) select * from t")
    assert not runner.is_query("USE SCHEMA s") and not runner.is_query("CREATE TABLE t AS SELECT 1")


def test_judge_flags_an_error_an_empty_or_all_null_query_and_a_statement_that_did_not_run(runner):
    pairs = [("a", "SELECT 1"), ("b", "SELECT 2"), ("c", "USE SCHEMA s"), ("d", "SELECT 3"), ("e", "SELECT 4"), ("f", "SELECT 5")]
    results = {
        "a": {"value": 1, "wall_s": [1.0], "rows": 1, "first_row": ["1", None]},
        "f": {"value": None, "wall_s": [1.0], "rows": 1, "first_row": [None, None]},
        "b": {"value": None, "wall_s": [1.0], "rows": 0},
        "c": {"value": None, "wall_s": [0.1], "rows": 0},
        "d": {"error": "boom"},
    }
    problems = runner.judge(pairs, results)
    assert [p.split(":")[0] for p in problems] == ["b", "d", "e", "f"]


def test_every_example_file_is_well_formed():
    files = sorted(EXAMPLES.glob("*.sql"))
    assert 8 <= len(files) <= 12
    seen = set()
    for f in files:
        text = f.read_text()
        cids = re.findall(r"^-- cid: (\S+)$", text, flags=re.M)
        assert cids[:2] == ["use_catalog", "use_schema"], f.name
        assert len(cids) == len(set(cids)), f"{f.name}: a cid repeats"
        assert not re.search(r"/Users/|@databricks", text), f.name
        seen.update(cids)
    assert "check_functions_installed" in seen


def test_retarget_rewrites_every_name_of_the_files_for_another_install(runner):
    text = (EXAMPLES / "00_setup.sql").read_text() + (EXAMPLES / "11_procedures.sql").read_text()
    out = runner.retarget(text, "cat_x", "sch_x")
    assert "USE CATALOG cat_x" in out and "USE SCHEMA sch_x" in out
    assert "table_schema = 'sch_x'" in out and "cat_x.sch_x.ex_parcels" in out
    assert "geobrix_uc" not in out and "geospatial_docs" not in out
    assert runner.retarget(text, "geospatial_docs", "geobrix_uc") == text  # the defaults change nothing


def test_dry_run_with_another_schema_passes(runner, capsys):
    assert runner.main(["--dry-run", "--schema", "geobrix_uc_it_abc", "--catalog", "geospatial_docs"]) == 0
    assert "geospatial_docs.geobrix_uc_it_abc" in capsys.readouterr().out


def test_upload_imports_the_files_retargeted_to_the_install(runner, tmp_path, monkeypatch):
    """A presenter opens the uploaded files and runs them: they must name the install they were run against."""
    import base64
    import sys
    from types import SimpleNamespace

    # No SDK needed: upload only reads ImportFormat.AUTO from it.
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.workspace", SimpleNamespace(ImportFormat=SimpleNamespace(AUTO="AUTO")))

    src = tmp_path / "00_setup.sql"
    src.write_text((EXAMPLES / "00_setup.sql").read_text())
    sent = {}

    class Ws:
        def mkdirs(self, path):
            pass

        def import_(self, path, content, format, overwrite):
            sent[path] = base64.b64decode(content).decode()

        def list(self, path):
            return [SimpleNamespace(path=p, object_type="FILE") for p in sent]

    assert runner.upload(SimpleNamespace(workspace=Ws()), [src], "/Workspace/Users/u/x", "cat_x", "sch_x") == 0
    text = sent["/Users/u/x/00_setup.sql"]
    assert "USE CATALOG cat_x" in text and "USE SCHEMA sch_x" in text
    assert "geospatial_docs" not in text and "geobrix_uc" not in text
