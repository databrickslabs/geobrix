"""Unit tests for harvest_notebook_outputs + the runner's --set-var placement.

Pure (no workspace, no Spark). Run: gbx:test:notebooks --path test_harvest_notebook_outputs.py
"""

import base64
import json
import urllib.parse

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


def test_dump_like_preserves_serialization():
    nb = {"cells": [_code("x = '—'")], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    for ensure_ascii in (False, True):
        for tail in ("", "\n"):
            original = json.dumps(nb, indent=1, ensure_ascii=ensure_ascii) + tail
            assert _dump_like(json.loads(original), original) == original


def test_set_var_override_lands_after_the_notebooks_own_assignment():
    cells = [_code("%run ./config_nb"), _code("FORCE_RELOAD    = False\nX = 1"), _code("use(FORCE_RELOAD)")]
    out = json.loads(_inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    srcs = ["".join(c["source"]) for c in out["cells"]]
    assert srcs[2].endswith("FORCE_RELOAD=True\n") and srcs[3] == "use(FORCE_RELOAD)"


def test_set_var_override_falls_back_to_after_run_cell():
    cells = [_code("a = 1"), _code("%run ./config_nb"), _code("b = FORCE_RELOAD == 1")]
    out = json.loads(_inject_override_cell(json.dumps({"cells": cells}).encode(), ["FORCE_RELOAD=True"]))
    assert "FORCE_RELOAD=True" in "".join(out["cells"][2]["source"])
