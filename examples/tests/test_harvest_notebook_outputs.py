"""Unit tests for harvest_notebook_outputs + the runner's --set-var placement.

Pure (no workspace, no Spark). Run: gbx:test:notebooks --path test_harvest_notebook_outputs.py
"""

import base64
import json
import subprocess
import urllib.parse

import harvest_notebook_outputs as hno
import pytest
from harvest_notebook_outputs import _dump_like, apply_outputs, decode_model, to_outputs
from run_notebooks_serverless import _inject_override_cell


def _code(src, outputs=None):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": outputs or [],
            "source": src.splitlines(keepends=True)}


def _cmd(pos, src, *items):
    return {"position": pos, "command": src, "results": {"type": "listResults", "data": list(items)}}


ANSI = {"type": "ansi", "name": "stdout", "data": "a\nb\n"}
PNG = {"type": "mimeBundle", "executionCount": None,
       "data": {"image/png": "iVBORw0KGgo=", "text/plain": "<Figure size 10x10 with 1 Axes>"}}


def test_decode_model_base64_then_url_quoted():
    model = {"commands": [{"command": "print('é')"}]}
    blob = base64.b64encode(urllib.parse.quote(json.dumps(model)).encode()).decode()
    assert decode_model(f"<script>__DATABRICKS_NOTEBOOK_MODEL = '{blob}';</script>") == model


def test_to_outputs_stream_display_and_execute_result():
    exe = {"type": "mimeBundle", "executionCount": 7, "data": {"text/plain": "DataFrame[]"}}
    stream, disp, res = to_outputs({"data": [ANSI, PNG, exe]})
    assert stream == {"name": "stdout", "output_type": "stream", "text": ["a\n", "b\n"]}
    assert disp["output_type"] == "display_data" and disp["data"]["image/png"] == "iVBORw0KGgo="
    assert disp["data"]["text/plain"] == ["<Figure size 10x10 with 1 Axes>"]
    assert res["output_type"] == "execute_result" and res["execution_count"] == 0


def test_apply_outputs_aligns_by_source_skipping_stripped_injected_and_inlined_cells():
    nb = {"cells": [
        _code("%pip install x"),
        _code("%run ./config_nb"),
        {"cell_type": "markdown", "metadata": {}, "source": ["# t"]},
        _code("FORCE = False\nprint(FORCE)"),
        _code("plot()  "),
        _code("never_ran()"),
    ]}
    model = {"commands": [
        _cmd(1, "%run ./config_nb"),
        _cmd(2.5, "CONFIG = 1", ANSI),                     # inlined from %run — no local cell
        _cmd(3, "FORCE = False\nprint(FORCE)", ANSI),
        _cmd(3.5, "# --- validation override\nFORCE = True"),  # runner-injected
        _cmd(4, "plot()", PNG),
    ]}
    updated, unmatched = apply_outputs(nb, model)
    assert (updated, unmatched) == (2, [5])
    assert nb["cells"][3]["outputs"][0]["text"] == ["a\n", "b\n"]
    assert nb["cells"][4]["outputs"][0]["output_type"] == "display_data"
    assert nb["cells"][4]["execution_count"] == 0 and nb["cells"][5]["execution_count"] is None


def test_apply_outputs_keeps_cells_with_unsupported_result_types():
    kept = [{"output_type": "stream", "name": "stdout", "text": ["old\n"]}]
    nb = {"cells": [_code("display(df)", kept)]}
    model = {"commands": [_cmd(1, "display(df)", {"type": "table", "data": [[1]]})]}
    assert apply_outputs(nb, model) == (0, [0])
    assert nb["cells"][0]["outputs"] == kept


def test_apply_outputs_keeps_the_outputs_of_a_cell_whose_result_is_not_a_list_of_items():
    kept = [{"output_type": "stream", "name": "stdout", "text": ["old\n"]}]
    nb = {"cells": [_code("fails()", kept)]}
    model = {"commands": [{"position": 1, "command": "fails()",
                           "results": {"type": "error", "summary": "boom"}}]}
    assert apply_outputs(nb, model) == (0, [0])
    assert nb["cells"][0]["outputs"] == kept


def test_apply_outputs_tolerates_the_result_of_a_notebook_exit_cell():
    """A cell that ends in dbutils.notebook.exit(value) comes back with ``results`` of type
    "raw" whose ``data`` is the exit value (a string), not a list of items: it has no
    output of its own, and its neighbours still get theirs."""
    nb = {"cells": [_code("print('a')"), _code("dbutils.notebook.exit('done')")]}
    model = {"commands": [
        _cmd(1, "print('a')", ANSI),
        {"position": 2, "command": "dbutils.notebook.exit('done')",
         "results": {"type": "raw", "data": "done"}},
    ]}
    assert apply_outputs(nb, model) == (2, [])
    assert nb["cells"][0]["outputs"][0]["text"] == ["a\n", "b\n"]
    assert nb["cells"][1]["outputs"] == []
    assert hno._n_items(model) == 1


def test_dump_like_preserves_serialization():
    nb = {"cells": [_code("x = '—'")], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    for ensure_ascii in (False, True):
        for tail in ("", "\n"):
            original = json.dumps(nb, indent=1, ensure_ascii=ensure_ascii) + tail
            assert _dump_like(json.loads(original), original) == original


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


def test_harvest_re_exports_until_results_stop_growing(monkeypatch, tmp_path):
    early = {"commands": [_cmd(1, "plot()")]}            # export taken too early: no results yet
    late = {"commands": [_cmd(1, "plot()", PNG)]}
    calls = _fake_cli(monkeypatch, _export_stdout(early), _export_stdout(late), _export_stdout(late))
    monkeypatch.setattr(hno.time, "sleep", lambda s: None)
    nb_path = tmp_path / "nb.ipynb"
    nb_path.write_text(json.dumps({"cells": [_code("plot()")], "metadata": {}}, indent=1))
    assert hno.harvest_run_outputs(7, "p", nb_path) == (1, [])
    assert json.loads(nb_path.read_text())["cells"][0]["outputs"][0]["output_type"] == "display_data"
    assert len(calls) == 3


def test_set_var_override_lands_after_the_notebooks_own_assignment():
    cells = [_code("%run ./config_nb"), _code("FORCE_RELOAD    = False\nX = 1"), _code("use(FORCE_RELOAD)")]
    out = json.loads(_inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    srcs = ["".join(c["source"]) for c in out["cells"]]
    assert srcs[2].endswith("FORCE_RELOAD=True\n") and srcs[3] == "use(FORCE_RELOAD)"


def test_set_var_override_lands_after_a_later_run_include():
    cells = [_code("FORCE_RELOAD = False"), _code("%run ./config_nb"), _code("use(FORCE_RELOAD)")]
    out = json.loads(_inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    assert "FORCE_RELOAD=True" in "".join(out["cells"][2]["source"])


def test_set_var_override_falls_back_to_after_run_cell():
    cells = [_code("a = 1"), _code("%run ./config_nb"), _code("b = FORCE_RELOAD == 1")]
    out = json.loads(_inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    assert "FORCE_RELOAD=True" in "".join(out["cells"][2]["source"])


def test_the_runner_submits_the_notebooks_as_they_were_at_launch_and_harvests_into_the_original(tmp_path, monkeypatch):
    """A notebook edited while the launch is live must not change what is submitted; outputs go back to the original."""
    import sys
    import types

    import run_notebooks_serverless as rns

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
         "--harvest-outputs"],
    )  # fmt: skip

    assert rns.main() == 0
    assert seen["runs"][0][0] == "main v1" and seen["deps"] == ["dep v1"]  # what was there at launch
    assert seen["runs"][0][1] == nb.resolve()  # the harvest targets the original, not the snapshot copy
