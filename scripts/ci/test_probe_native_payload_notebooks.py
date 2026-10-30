"""The payload probe notebooks (examples/tests/probe_native_payload*.ipynb).

They run on Databricks, so these rows read them as text: they parse, they call only names the package has,
nothing outside the dedicated branch touches the JVM or a Spark configuration (the Serverless path forbids
both), and the runner's --set-var puts MODE where the notebook will see it.
"""

import ast
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS = REPO_ROOT / "examples" / "tests"
SELFCHECK = REPO_ROOT / "python/geobrix/src/databricks/labs/gbx/_native/_selfcheck.py"
NAMES = ("probe_native_payload", "probe_native_payload_pip")
FORBIDDEN_ATTRS = {"_jvm", "rdd", "sparkContext", "_jsc"}


def _cells(name):
    return json.loads((TESTS / f"{name}.ipynb").read_text())["cells"]


def _source(cell):
    return "".join(cell["source"])


def _code(name):
    """(cell index, parsed module) of each code cell with no %-magic line."""
    out = []
    for i, c in enumerate(_cells(name)):
        src = _source(c)
        if c["cell_type"] == "code" and not any(ln.startswith(("%", "dbutils.library")) for ln in src.splitlines()):
            out.append((i, ast.parse(src)))
    return out


@pytest.mark.parametrize("name", NAMES)
def test_the_notebook_is_valid_and_written_with_indent_one(name):
    nbformat = pytest.importorskip("nbformat")
    path = TESTS / f"{name}.ipynb"
    nb = nbformat.read(str(path), as_version=4)
    nbformat.validate(nb)
    assert path.read_text() == json.dumps(json.loads(path.read_text()), indent=1) + "\n"


def test_the_probe_has_the_cells_the_runs_rely_on():
    cells = _cells("probe_native_payload")
    assert [c["cell_type"] for c in cells] == ["markdown"] + ["code"] * 5
    config = _source(cells[1])
    for line in ('SET_NATIVE_DIR = True', 'SCRATCH = "/Volumes/geospatial_docs/geobrix/sample-data/_probe/natives"',
                 'UC_FUNCTION = "geospatial_docs.gbx_zarr_scratch.gbx_probe_native_payload"'):  # fmt: skip
        assert line in config
    assert 'MODE = globals().get("MODE", "serverless")' in config  # a preset MODE (the pip notebook's) survives
    assert _source(cells[5]).count("dbutils.notebook.exit(") == 1
    pip = [_source(c) for c in _cells("probe_native_payload_pip") if c["cell_type"] == "code"]
    assert pip[0].startswith('%pip install "geobrix @ file:///Volumes/') and "+env6-py3-none-any.whl" in pip[0]
    assert pip[1:] == ["dbutils.library.restartPython()\n", 'MODE = "dedicated"\n', "%run ./probe_native_payload\n"]


def test_every_name_the_probe_calls_in_the_package_is_there():
    defined = {n.name for n in ast.parse(SELFCHECK.read_text()).body if isinstance(n, ast.FunctionDef)}
    native_init = (SELFCHECK.parent / "__init__.py").read_text()
    jvm = (SELFCHECK.parents[1] / "manager" / "jvm.py").read_text()
    called = set()
    for _, tree in _code("probe_native_payload"):
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                if node.value.id == "_selfcheck":
                    called.add(node.attr)
                if node.value.id == "_native":
                    assert f"def {node.attr}(" in native_init, node.attr
    assert called == {"stack", "light", "light_sql", "payload_size_mb", "installed_closure_mb", "heavy_sql",
                      "install_paths"}  # fmt: skip
    assert called <= defined, called - defined
    assert "NATIVE_DIR_KEY = (" in jvm  # the Python name of the Scala GDALManager.NativeDirKey


def test_only_the_dedicated_branch_sets_a_spark_conf_or_touches_the_jvm():
    for i, tree in _code("probe_native_payload"):
        parents = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node

        def dedicated(node):
            while node in parents:
                node = parents[node]
                if isinstance(node, ast.If) and ast.unparse(node.test) == "MODE == 'dedicated'":
                    return True
            return False

        for node in ast.walk(tree):
            conf_set = isinstance(node, ast.Call) and ast.unparse(node.func).endswith("conf.set")
            jvm = isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_ATTRS
            if conf_set or jvm:
                assert dedicated(node), f"cell {i}: {ast.unparse(node)} outside the dedicated branch"
    sets = [n for _, t in _code("probe_native_payload") for n in ast.walk(t)
            if isinstance(n, ast.Call) and ast.unparse(n.func).endswith("conf.set")]  # fmt: skip
    assert len(sets) == 1  # the one in the dedicated branch is there


def test_the_runners_set_var_lands_after_the_cell_that_assigns_mode(monkeypatch):
    monkeypatch.syspath_prepend(str(TESTS))
    runner = pytest.importorskip("run_notebooks_serverless")
    raw = (TESTS / "probe_native_payload.ipynb").read_bytes()
    cells = json.loads(runner._inject_override_cell(raw, ['MODE="dedicated"']))["cells"]
    assert 'MODE="dedicated"' in _source(cells[2]) and "MODE = globals()" in _source(cells[1])
