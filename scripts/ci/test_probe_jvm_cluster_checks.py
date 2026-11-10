"""The cluster probe notebooks of the measurement pass (examples/tests/probe_dcs_image.ipynb, probe_jvm_cluster_checks.ipynb).

They run on Databricks, so these rows read them as text: the code cells parse, the DCS probe installs nothing (the image
carries the wheel and the JAR), the standard-access branch touches no JVM handle (Spark Connect has none), and the Java
hypot vectors the notebook carries are the test fixture's.
"""

import ast
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS = REPO_ROOT / "examples" / "tests"
FIXTURE = REPO_ROOT / "python/geobrix/test/pyvx/data/java_hypot.txt"
JVM_HANDLES = ("_jvm", "sparkContext", "_jsc", ".rdd")


def _cells(name):
    return json.loads((TESTS / f"{name}.ipynb").read_text())["cells"]


def _src(cell):
    return "".join(cell["source"])


@pytest.mark.parametrize("name", ["probe_dcs_image", "probe_jvm_cluster_checks"])
def test_the_code_cells_parse_and_the_first_cell_says_what_the_run_is(name):
    cells = _cells(name)
    head = _src(cells[0])
    assert cells[0]["cell_type"] == "markdown"
    for part in ("**What and why:**", "**Expected output:**", "**Expected time:**", "**Monitoring:**"):
        assert part in head, part
    for c in cells:
        if c["cell_type"] == "code" and not _src(c).lstrip().startswith("%"):
            ast.parse(_src(c))


def test_the_dcs_probe_installs_nothing():
    for c in _cells("probe_dcs_image"):
        assert not _src(c).lstrip().startswith(("%pip", "%restart_python"))


def test_the_standard_access_branch_touches_no_jvm_handle():
    (branch,) = [_src(c) for c in _cells("probe_jvm_cluster_checks") if 'if MODE == "standard_budget"' in _src(c)]
    for h in JVM_HANDLES:
        assert h not in branch, h


def test_the_notebook_carries_the_fixture_vectors():
    want = [ln for ln in FIXTURE.read_text().splitlines() if ln and not ln.startswith("#")]
    (cell,) = [_src(c) for c in _cells("probe_jvm_cluster_checks") if _src(c).startswith("HYPOT_VECTORS = ")]
    got = ast.literal_eval(cell[len("HYPOT_VECTORS = ") :])
    assert got == want and len(got) > 1000
