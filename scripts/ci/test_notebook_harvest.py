"""The output harvest (examples/tests/harvest_notebook_outputs.py) and the runner's --set-var placement.

The harvest writes ONE run's outputs into the local .ipynb: every code cell it covers ends with exactly that run's
outputs, or none. Pure: no workspace, no Spark. Run: python -m pytest scripts/ci/test_notebook_harvest.py
"""

import base64
import importlib
import json
import subprocess
import sys
import types
import urllib.parse

import pytest
from conftest import REPO_ROOT

sys.path.insert(0, str(REPO_ROOT / "examples" / "tests"))
hno = importlib.import_module("harvest_notebook_outputs")
rns = importlib.import_module("run_notebooks_serverless")
_wheel_guard = importlib.import_module("_wheel_guard")


def _code(src, outputs=None, execution_count=None):
    return {"cell_type": "code", "execution_count": execution_count, "metadata": {}, "outputs": outputs or [],
            "source": src.splitlines(keepends=True)}


def _cmd(pos, src, *items):
    return {"position": pos, "command": src, "results": {"type": "listResults", "data": list(items)}}


ANSI = {"type": "ansi", "name": "stdout", "data": "a\nb\n"}
PNG = {"type": "mimeBundle", "executionCount": None,
       "data": {"image/png": "iVBORw0KGgo=", "text/plain": "<Figure size 10x10 with 1 Axes>"}}
TABLE = {"type": "table", "data": [[1, "x<y"], [2, None]],
         "schema": [{"name": "id", "type": '"long"', "metadata": "{}"}, {"name": "s", "type": '"string"', "metadata": "{}"}],
         "truncated": False, "isJsonSchema": True}
H3_HINT = {"type": "mimeBundle", "data": {"application/vnd.databricks.v1+h3_hint": "", "text/plain": ""}}
STALE = [{"output_type": "stream", "name": "stdout", "text": ["old run\n"]}]
WHEEL = "/Volumes/c/s/v/geobrix/0.6.0/env6/geobrix-0.6.0+env6-py3-none-any.whl"
PROV = {"version": "0.6.0+env6", "git_sha": "1234567890abcdef1234567890abcdef12345678", "dirty": False,
        "wheel": WHEEL, "wheel_sha256": "f" * 64, "runtime": "client.6.1"}


def _guard_cmd(pos=1.5, prov=PROV):
    src = "# Wheel guard: the run tests the staged wheel (see _wheel_guard.py); it fails here ...\nimport os\n"
    out = "WHEEL GUARD expected: ...\nWHEEL GUARD provenance: %s\nWHEEL GUARD ok: driver and 1 worker identity\n" % json.dumps(prov)
    return _cmd(pos, src, {"type": "ansi", "name": "stdout", "data": out})


# ---------------------------------------------------------------------------------------------- conversion


def test_decode_model_base64_then_url_quoted():
    model = {"commands": [{"command": "print('é')"}]}
    blob = base64.b64encode(urllib.parse.quote(json.dumps(model)).encode()).decode()
    assert hno.decode_model(f"<script>__DATABRICKS_NOTEBOOK_MODEL = '{blob}';</script>") == model


def test_to_outputs_stream_display_and_execute_result():
    exe = {"type": "mimeBundle", "executionCount": 7, "data": {"text/plain": "DataFrame[]"}}
    stream, disp, res = hno.to_outputs({"type": "listResults", "data": [ANSI, PNG, exe]})
    assert stream == {"name": "stdout", "output_type": "stream", "text": ["a\n", "b\n"]}
    assert disp["output_type"] == "display_data" and disp["data"]["image/png"] == "iVBORw0KGgo="
    assert disp["data"]["text/plain"] == ["<Figure size 10x10 with 1 Axes>"]
    assert res["output_type"] == "execute_result" and res["execution_count"] == 0


def test_a_display_table_becomes_the_html_table_databricks_exports():
    (out,) = hno.to_outputs({"type": "listResults", "data": [TABLE]})
    html = "".join(out["data"]["text/html"])
    assert out["output_type"] == "display_data" and "<table class='table-result'>" in html
    assert "<th>id</th><th>s</th>" in html and "<td>x&lt;y</td>" in html and "<td>null</td>" in html
    assert out["data"]["text/plain"]  # a plain-text form too, for viewers without HTML


def test_a_bare_table_result_and_a_display_html_result_are_converted():
    (t,) = hno.to_outputs(dict(TABLE))  # display(df) alone in a cell: the results ARE the table
    assert "<table class='table-result'>" in "".join(t["data"]["text/html"])
    (h,) = hno.to_outputs({"type": "htmlSandbox", "data": "<b>hi</b>"})
    assert h["data"]["text/html"] == ["<b>hi</b>"]


def test_an_h3_hint_keeps_its_databricks_mime_type():
    (out,) = hno.to_outputs({"type": "listResults", "data": [H3_HINT]})
    assert out["data"]["application/vnd.databricks.v1+h3_hint"] == ""


def test_an_unknown_result_type_is_refused_not_dropped():
    with pytest.raises(hno.UnconvertibleResult, match="mystery"):
        hno.to_outputs({"type": "listResults", "data": [ANSI, {"type": "mystery", "data": 1}]})


def test_a_notebook_exit_cell_has_no_output():
    assert hno.to_outputs({"type": "raw", "data": "done"}) == []
    assert hno.to_outputs(None) == []


def test_an_error_result_becomes_an_error_output():
    cmd = {"command": "1/0", "results": None, "errorSummary": "ZeroDivisionError: division by zero",
           "error": "Traceback...\nZeroDivisionError: division by zero"}
    (out,) = hno.command_outputs(cmd)
    assert out["output_type"] == "error" and out["ename"] == "ZeroDivisionError"
    assert out["evalue"] == "division by zero" and out["traceback"]


# ---------------------------------------------------------------------------------------------- applying one run


def test_every_covered_cell_carries_this_runs_outputs_or_none():
    nb = {"cells": [
        _code("%pip install x", STALE),
        _code("%run ./config_nb", STALE),
        {"cell_type": "markdown", "metadata": {}, "source": ["# t"]},
        _code("FORCE = False\nprint(FORCE)", STALE),
        _code("plot()  ", STALE),
        _code("display(df)", STALE),
        _code("quiet = 1", STALE),       # ran, printed nothing this time
        _code("never_ran()", STALE),     # not in the run (source edited, or after an exit)
    ]}
    model = {"commands": [
        _cmd(1, "%run ./config_nb"),
        _cmd(2, "%md\n# t"),                               # markdown: no code cell takes it
        _cmd(3, "FORCE = False\nprint(FORCE)", ANSI),
        _cmd(3.5, "# --- validation override (injected by runner; NOT in the committed notebook) ---\nFORCE = True"),
        _cmd(4, "plot()", PNG),
        _cmd(5, "display(df)", TABLE),
        _cmd(6, "quiet = 1"),
    ]}
    rep = hno.apply_outputs(nb, model)
    cells = nb["cells"]
    assert cells[0]["outputs"] == [] and cells[1]["outputs"] == []        # nothing stale left anywhere
    assert cells[0]["execution_count"] is None and cells[1]["execution_count"] == 0
    assert cells[3]["outputs"][0]["text"] == ["a\n", "b\n"]
    assert cells[4]["outputs"][0]["output_type"] == "display_data"
    assert "<table" in "".join(cells[5]["outputs"][0]["data"]["text/html"])
    assert cells[6]["outputs"] == [] and cells[6]["execution_count"] == 0
    assert cells[7]["outputs"] == [] and cells[7]["execution_count"] is None
    assert rep.not_run == [7] and rep.updated == 5
    assert [c["command"] for c in rep.leftover] == ["%md\n# t"]


def test_identical_cells_each_get_their_own_command():
    nb = {"cells": [_code("show()"), _code("x = 2"), _code("show()")]}
    model = {"commands": [_cmd(1, "show()", {"type": "ansi", "data": "one\n"}), _cmd(2, "x = 2"),
                          _cmd(3, "show()", {"type": "ansi", "data": "two\n"})]}
    hno.apply_outputs(nb, model)
    assert nb["cells"][0]["outputs"][0]["text"] == ["one\n"] and nb["cells"][2]["outputs"][0]["text"] == ["two\n"]


def test_an_unconvertible_cell_fails_the_harvest_listing_the_cells():
    nb = {"cells": [_code("print(1)", STALE), _code("odd()", STALE)]}
    model = {"commands": [_cmd(1, "print(1)", ANSI), _cmd(2, "odd()", {"type": "mystery"})]}
    with pytest.raises(hno.HarvestError, match=r"cell 2 .*mystery"):
        hno.apply_outputs(nb, model)


def test_a_classic_runs_forced_install_and_repointed_wheel_still_match_the_install_cell():
    local = '%pip install "geobrix[vizx] @ file:///Volumes/old/geobrix-0.6.0+env6-py3-none-any.whl"\n'
    ran = _wheel_guard.force_line(WHEEL) + "\n" + local.replace("/Volumes/old/geobrix-0.6.0+env6-py3-none-any.whl", WHEEL)
    nb = {"cells": [_code(local, STALE)]}
    model = {"commands": [_cmd(1, ran, {"type": "ansi", "data": "Successfully installed geobrix\n"})]}
    hno.apply_outputs(nb, model, provenance=PROV, run_context={"compute": "cluster", "label": "Databricks Runtime 19.0"})
    texts = ["".join(o["text"]) for o in nb["cells"][0]["outputs"]]
    assert texts[0] == "Successfully installed geobrix\n" and texts[-1].startswith("GeoBrix 0.6.0+env6 installed")


def test_a_serverless_install_cell_gets_the_provenance_line_and_other_pip_cells_are_cleared():
    nb = {"cells": [
        _code('%pip install "geobrix[stac] @ file:///Volumes/x/geobrix-0.6.0+env6-py3-none-any.whl"', STALE),
        _code('%pip install "overturemaps>=1.0.2,<2"', STALE),
        _code("dbutils.library.restartPython()", STALE),
        _code("print(1)", STALE),
    ]}
    model = {"commands": [_guard_cmd(0.5), _cmd(1, "print(1)", ANSI)]}
    rep = hno.apply_outputs(nb, model, provenance=hno.provenance_from_model(model),
                            run_context={"compute": "serverless", "label": "Serverless environment 6"})
    (line,) = nb["cells"][0]["outputs"]
    assert "".join(line["text"]) == (
        "GeoBrix 0.6.0+env6 installed (build 1234567890ab, wheel sha256 ffffffffffff) on Serverless environment 6\n")
    assert nb["cells"][0]["execution_count"] == 0
    assert nb["cells"][1]["outputs"] == [] and nb["cells"][2]["outputs"] == []
    assert rep.install_cell == 0 and rep.leftover == []  # the guard cell is the runner's, never a leftover


def test_only_the_cells_of_a_piece_are_touched():
    nb = {"cells": [_code("a()", STALE), _code("b()", STALE), _code("c()", STALE)]}
    model = {"commands": [_cmd(1, "a()", ANSI), _cmd(2, "b()", ANSI)]}  # a() ran as the prelude
    hno.apply_outputs(nb, model, cells=range(1, 2))
    assert nb["cells"][0]["outputs"] == STALE and nb["cells"][2]["outputs"] == STALE
    assert nb["cells"][1]["outputs"][0]["text"] == ["a\n", "b\n"]


def test_a_run_cell_gets_what_its_notebook_printed():
    """The export holds a %run notebook's run as sub-commands (empty source, parentHierarchy = the %run's guid)."""
    nb = {"cells": [_code("%run ./config_nb", STALE), _code("main()", STALE)]}
    model = {"commands": [
        dict(_cmd(1, "%run ./config_nb"), guid="g1", results=None),
        dict(_cmd(1.5, "", {"type": "ansi", "data": "config loaded 42\n"}), parentHierarchy=["g1"]),
        _cmd(2, "main()", ANSI),
    ]}
    rep = hno.apply_outputs(nb, model)
    assert nb["cells"][0]["outputs"][0]["text"] == ["config loaded 42\n"] and rep.leftover == []


def test_a_real_serverless_export_leaves_no_stale_output_in_the_fixture_notebook():
    """examples/tests/fixtures/harvest_smoke_export.json is the model of a real env6 run of harvest_smoke.ipynb."""
    fixtures = REPO_ROOT / "examples" / "tests" / "fixtures"
    nb = json.loads((fixtures / "harvest_smoke.ipynb").read_text())
    model = json.loads((fixtures / "harvest_smoke_export.json").read_text())
    prov = hno.provenance_from_model(model)
    rep = hno.apply_outputs(nb, model, provenance=prov, run_context={"label": "Serverless environment 6"})
    code = {i + 1: c for i, c in enumerate(nb["cells"]) if c["cell_type"] == "code"}
    assert not any("STALE" in json.dumps(c["outputs"]) for c in code.values())
    kinds = {n: [o["output_type"] for o in c["outputs"]] for n, c in code.items()}
    assert kinds == {2: ["stream"], 3: ["stream"], 4: ["stream"], 5: ["display_data"], 6: ["display_data"],
                     7: ["display_data"], 8: ["display_data"], 9: [], 10: ["execute_result"], 11: ["stream"]}
    assert "".join(code[2]["outputs"][0]["text"]).startswith("GeoBrix 0.6.0+env6 installed (build e523f16e6dfb")
    assert code[3]["outputs"][0]["text"] == ["config loaded 42\n"]          # the %run notebook's print
    assert "<td>608692970249519103</td>" in "".join(code[7]["outputs"][0]["data"]["text/html"])
    assert prov["wheel_sha256"].startswith("70fac9419b5c") and prov["runtime"] == "client.6.5"
    assert rep.not_run == [] and rep.install_cell == 1 and all(c["execution_count"] == 0 for c in code.values())


# ---------------------------------------------------------------------------------------------- provenance


def test_the_guard_line_is_the_provenance_and_the_runner_cells_never_count():
    assert hno.provenance_from_model({"commands": [_guard_cmd()]}) == PROV
    assert hno.provenance_from_model({"commands": [_cmd(1, "print(1)", ANSI)]}) is None


def test_the_notebook_metadata_records_the_wheel_the_runtime_and_each_piece():
    nb = {"cells": [_code("a()"), _code("b()")], "metadata": {"kernelspec": {}}}
    ctx = {"compute": "serverless", "environment_version": "6", "label": "Serverless environment 6"}
    hno.stamp_provenance(nb, PROV, run_id=11, cells=None, run_context=ctx, n_cells=2)
    meta = nb["metadata"]["geobrix_run"]
    assert meta["version"] == "0.6.0+env6" and meta["git_sha"] == PROV["git_sha"]
    assert meta["wheel_sha256"] == "f" * 64 and meta["wheel"] == WHEEL
    assert meta["runtime"] == "Serverless environment 6" and meta["runs"] == [{"run_id": 11, "cells": "1:2"}]
    assert nb["metadata"]["kernelspec"] == {}
    hno.stamp_provenance(nb, PROV, run_id=12, cells=range(1, 2), run_context=ctx, n_cells=2)
    assert nb["metadata"]["geobrix_run"]["runs"] == [{"run_id": 11, "cells": "1:2"}, {"run_id": 12, "cells": "2:2"}]


def test_a_piece_from_another_wheel_drops_the_older_runs_and_says_so(capsys):
    nb = {"cells": [_code("a()"), _code("b()")], "metadata": {}}
    hno.stamp_provenance(nb, PROV, run_id=11, cells=range(0, 1), run_context={"label": "x"}, n_cells=2)
    other = dict(PROV, wheel_sha256="0" * 64, git_sha="9" * 40)
    hno.stamp_provenance(nb, other, run_id=12, cells=range(1, 2), run_context={"label": "x"}, n_cells=2)
    assert nb["metadata"]["geobrix_run"]["runs"] == [{"run_id": 12, "cells": "2:2"}]
    assert "another wheel" in capsys.readouterr().err


def test_the_guard_prints_a_provenance_line_with_the_wheel_sha256(tmp_path, monkeypatch):
    import hashlib
    import zipfile

    pkg = tmp_path / "site" / "databricks" / "labs" / "gbx"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text('__version__ = "0.6.0"\n')
    (pkg / "_build_info.py").write_text('GIT_SHA = "%s"\nGIT_DIRTY = False\nVERSION = "0.6.0+env6"\n' % ("a" * 40))
    whl = tmp_path / "geobrix-0.6.0+env6-py3-none-any.whl"
    with zipfile.ZipFile(whl, "w") as z:
        for f in pkg.iterdir():
            z.write(f, "databricks/labs/gbx/" + f.name)
    for mod in [m for m in sys.modules if m == "databricks" or m.startswith("databricks.")]:
        monkeypatch.delitem(sys.modules, mod)
    monkeypatch.syspath_prepend(str(tmp_path / "site"))
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.6.1")
    lines = []
    _wheel_guard.run_guard({"wheel": str(whl)}, say=lines.append)
    prov = hno.provenance_from_model({"commands": [_cmd(1, "# Wheel guard: x", {"type": "ansi", "data": "\n".join(lines)})]})
    assert prov["wheel_sha256"] == hashlib.sha256(whl.read_bytes()).hexdigest()
    assert prov["git_sha"] == "a" * 40 and prov["runtime"] == "client.6.1" and prov["wheel"] == str(whl)


# ---------------------------------------------------------------------------------------------- the file


def test_dump_like_preserves_serialization():
    nb = {"cells": [_code("x = '—'")], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    for ensure_ascii in (False, True):
        for tail in ("", "\n"):
            original = json.dumps(nb, indent=1, ensure_ascii=ensure_ascii) + tail
            assert hno._dump_like(json.loads(original), original) == original


def _export_stdout(model, view_type="NOTEBOOK"):
    blob = base64.b64encode(urllib.parse.quote(json.dumps(model)).encode()).decode()
    html = f"<script>__DATABRICKS_NOTEBOOK_MODEL = '{blob}';</script>"
    return json.dumps({"views": [{"type": view_type, "name": "x", "content": html}]})


def _fake_cli(monkeypatch, *stdouts):
    calls = []

    def run(cmd, **kw):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdouts[min(len(calls), len(stdouts)) - 1], "")

    monkeypatch.setattr(hno.subprocess, "run", run)
    monkeypatch.setattr(hno.time, "sleep", lambda s: None)
    return calls


def test_export_model_reads_the_notebook_view(monkeypatch):
    model = {"commands": [_cmd(1, "x = 1", ANSI)]}
    calls = _fake_cli(monkeypatch, _export_stdout(model))
    assert hno._export_model(123, "p") == model
    assert calls[0][:4] == ["databricks", "jobs", "export-run", "123"] and "p" in calls[0]


def test_export_model_raises_without_a_notebook_view(monkeypatch):
    _fake_cli(monkeypatch, _export_stdout({"commands": []}, view_type="DASHBOARD"))
    with pytest.raises(RuntimeError, match="no NOTEBOOK view"):
        hno._export_model(123, "p")


def test_harvest_re_exports_until_results_stop_growing_and_writes_provenance(monkeypatch, tmp_path):
    early = {"commands": [_guard_cmd(), _cmd(2, "plot()")]}            # export taken too early: no results yet
    late = {"commands": [_guard_cmd(), _cmd(2, "plot()", PNG)]}
    calls = _fake_cli(monkeypatch, _export_stdout(early), _export_stdout(late), _export_stdout(late))
    nb_path = tmp_path / "nb.ipynb"
    nb_path.write_text(json.dumps({"cells": [_code("plot()", STALE)], "metadata": {}}, indent=1))
    rep = hno.harvest_run_outputs(7, "p", nb_path, run_context={"label": "Serverless environment 6"})
    written = json.loads(nb_path.read_text())
    assert rep.updated == 1 and written["cells"][0]["outputs"][0]["output_type"] == "display_data"
    assert written["metadata"]["geobrix_run"]["git_sha"] == PROV["git_sha"]
    assert len(calls) == 3


def test_a_failed_harvest_leaves_the_notebook_as_it_was(monkeypatch, tmp_path):
    model = {"commands": [_cmd(1, "odd()", {"type": "mystery"})]}
    _fake_cli(monkeypatch, _export_stdout(model))
    nb_path = tmp_path / "nb.ipynb"
    before = json.dumps({"cells": [_code("odd()", STALE)], "metadata": {}}, indent=1)
    nb_path.write_text(before)
    with pytest.raises(hno.HarvestError):
        hno.harvest_run_outputs(7, "p", nb_path, settle_s=0)
    assert nb_path.read_text() == before


# ---------------------------------------------------------------------------------------------- the runner


def test_set_var_override_lands_after_the_notebooks_own_assignment():
    cells = [_code("%run ./config_nb"), _code("FORCE_RELOAD    = False\nX = 1"), _code("use(FORCE_RELOAD)")]
    out = json.loads(rns._inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    srcs = ["".join(c["source"]) for c in out["cells"]]
    assert srcs[2].endswith("FORCE_RELOAD=True\n") and srcs[3] == "use(FORCE_RELOAD)"


def test_set_var_override_lands_after_a_later_run_include():
    cells = [_code("FORCE_RELOAD = False"), _code("%run ./config_nb"), _code("use(FORCE_RELOAD)")]
    out = json.loads(rns._inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    assert "FORCE_RELOAD=True" in "".join(out["cells"][2]["source"])


def test_set_var_override_falls_back_to_after_run_cell():
    cells = [_code("a = 1"), _code("%run ./config_nb"), _code("b = FORCE_RELOAD == 1")]
    out = json.loads(rns._inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    assert "FORCE_RELOAD=True" in "".join(out["cells"][2]["source"])


def test_the_runner_submits_the_notebooks_as_they_were_at_launch_and_harvests_into_the_original(tmp_path, monkeypatch):
    """A notebook edited while the launch is live must not change what is submitted; outputs go back to the original."""
    nb = tmp_path / "main.ipynb"
    dep = tmp_path / "dep.ipynb"
    nb.write_text("main v1")
    dep.write_text("dep v1")
    seen = {"runs": [], "deps": []}

    def client(*a, **k):  # built after the launch-time snapshot: the edit lands between snapshot and submit
        nb.write_text("main v2 (edited during the launch)")
        dep.write_text("dep v2 (edited during the launch)")
        return types.SimpleNamespace()

    monkeypatch.setitem(sys.modules, "databricks.sdk", types.SimpleNamespace(WorkspaceClient=client))
    monkeypatch.setattr(rns, "_import_notebook", lambda w, path, ws_dir, strip_pip, **kw: seen["deps"].append(path.read_text()))
    monkeypatch.setattr(
        rns, "run_one",
        lambda **kw: seen["runs"].append((kw["local_path"].read_text(), kw["harvest_path"])) or True,
    )  # fmt: skip
    monkeypatch.setattr(
        sys, "argv",
        ["run_notebooks_serverless.py", "--notebook", str(nb), "--dep-notebook", str(dep), "--ws-dir", "/Users/x/y",
         "--harvest-outputs", "--profile", "p"],
    )  # fmt: skip

    assert rns.main() == 0
    assert seen["runs"][0][0] == "main v1" and seen["deps"] == ["dep v1"]  # what was there at launch
    assert seen["runs"][0][1] == nb.resolve()  # the harvest targets the original, not the snapshot copy


def test_each_piece_is_its_own_run_harvested_into_the_same_notebook(tmp_path, monkeypatch):
    nb = tmp_path / "main.ipynb"
    nb.write_text(json.dumps({"cells": [_code("a = 1"), _code("print(a)"), _code("print(a + 1)")]}))
    pieces = []
    monkeypatch.setitem(sys.modules, "databricks.sdk", types.SimpleNamespace(WorkspaceClient=lambda **k: types.SimpleNamespace()))
    monkeypatch.setattr(rns, "run_one", lambda **kw: pieces.append((kw["cells"], kw["harvest_path"])) or True)
    monkeypatch.setattr(
        sys, "argv",
        ["run_notebooks_serverless.py", "--notebook", str(nb), "--ws-dir", "/Users/x/y", "--harvest-outputs",
         "--cells", "1:2", "--cells", "3:3", "--profile", "p"],
    )  # fmt: skip
    assert rns.main() == 0
    assert pieces == [("1:2", nb.resolve()), ("3:3", nb.resolve())]


def test_the_profile_is_required(monkeypatch, tmp_path):
    nb = tmp_path / "main.ipynb"
    nb.write_text("{}")
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr(sys, "argv", ["run_notebooks_serverless.py", "--notebook", str(nb)])
    with pytest.raises(SystemExit) as exc:
        rns.main()
    assert exc.value.code == 2


# ---------------------------------------------------------------------------------------------- the default wheel


@pytest.mark.parametrize("env,expected", [("6", "env6"), ("5", "env5")])
def test_the_default_wheel_is_the_versioned_staged_env_wheel(env, expected):
    version = rns.package_version()
    assert version and version[0].isdigit()
    assert rns.default_wheel(int(env)) == (
        f"/Volumes/geospatial_docs/geobrix/sample-data/geobrix/{version}/{expected}/"
        f"geobrix-{version}+{expected}-py3-none-any.whl")


@pytest.mark.parametrize("env_version,new_cluster,expected", [
    ("6", None, 6), ("5", None, 5), ("6", "17.3.x-scala2.13", 5), ("5", "18.0.x-scala2.13", 5),
    ("6", "19.x-scala2.13", 6),
])
def test_the_default_wheel_follows_the_runtime(env_version, new_cluster, expected):
    assert rns.default_wheel_env(env_version, new_cluster) == expected


def test_the_package_version_is_the_checkouts():
    text = (REPO_ROOT / "python/geobrix/src/databricks/labs/gbx/__init__.py").read_text()
    assert f'__version__ = "{rns.package_version()}"' in text
