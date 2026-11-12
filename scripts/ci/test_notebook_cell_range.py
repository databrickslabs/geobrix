"""Running a notebook in pieces (examples/tests/_cell_range.py) and the runner's --cells / --prelude wiring.

Pure: no workspace, no Spark.
"""

import importlib
import json

import pytest
from conftest import REPO_ROOT


@pytest.fixture
def cr(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    return importlib.import_module("_cell_range")


def _code(src):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
            "source": src.splitlines(keepends=True)}


def _md(src):
    return {"cell_type": "markdown", "metadata": {}, "source": [src]}


NB = {"cells": [
    _md("# Title"),                                                   # 1
    _code('%pip install "geobrix[vizx] @ file:///Volumes/a/b.whl"'),  # 2
    _code("%run ./config_nb"),                                        # 3
    _code("import os\nFORCE_RELOAD = False\nTABLE = f'{CATALOG}.t'"),  # 4  definitions only
    _code("df = spark.table(TABLE)\nheavy = df.count()"),             # 5  compute
    _code("def helper(x):\n    return x + 1"),                        # 6  definitions only
    _md("## Part 2"),                                                 # 7
    _code("print(helper(1))"),                                        # 8
    _code("display(spark.table(TABLE))"),                             # 9
]}


def test_a_range_is_1_based_inclusive_over_all_cells_with_open_ends(cr):
    assert cr.parse_range("2:4", 9) == range(1, 4)
    assert cr.parse_range("7:", 9) == range(6, 9)
    assert cr.parse_range(":3", 9) == range(0, 3)
    assert cr.range_label(cr.parse_range("7:", 9)) == "7:9"
    for bad in ("", ":", "0:3", "5:4", "3:10", "a:b"):
        with pytest.raises(ValueError, match="--cells"):
            cr.parse_range(bad, 9)


def test_the_prelude_list_takes_numbers_and_ranges(cr):
    assert cr.parse_cell_list("5, 2:3", 9) == [1, 2, 4]


def test_the_automatic_prelude_is_the_setup_cells_before_the_piece(cr):
    assert cr.auto_prelude(NB["cells"], cr.parse_range("7:9", 9)) == [1, 2, 3, 5]  # cells 2, 3, 4, 6; not 5 (compute)


def test_a_piece_keeps_its_cells_and_the_prelude_only(cr):
    out, rng, pre = cr.select_cells(NB, "7:9")
    assert rng == range(6, 9) and pre == [1, 2, 3, 5]
    assert [cr.cell_source(c).splitlines()[0] for c in out["cells"]] == [
        '%pip install "geobrix[vizx] @ file:///Volumes/a/b.whl"', "%run ./config_nb", "import os",
        "def helper(x):", "## Part 2", "print(helper(1))", "display(spark.table(TABLE))"]
    assert NB["cells"][4] not in out["cells"]


def test_a_piece_that_needs_a_skipped_compute_cell_is_refused_with_the_prelude_to_add(cr):
    nb = {"cells": NB["cells"] + [_code("print(heavy)")]}
    with pytest.raises(ValueError, match=r"heavy \(cell 5\).*--prelude 5"):
        cr.select_cells(nb, "10:10")
    out, _, pre = cr.select_cells(nb, "10:10", prelude="5")
    assert 4 in pre and NB["cells"][4] in out["cells"]


def test_the_imports_and_flags_of_a_skipped_compute_cell_are_carried_not_its_compute(cr):
    nb = {"cells": [_code("import json, os\nFLAG = True\nrows = spark.range(9).collect()"),
                    _code("print(json.dumps(FLAG), os.sep)")]}
    out, _, pre = cr.select_cells(nb, "2:2")
    carried = cr.cell_source(out["cells"][0])
    assert pre == [] and carried.startswith(cr.CARRIED_PREFIX + " 1 ")
    assert "import json, os" in carried and "FLAG = True" in carried and "collect" not in carried
    hno = importlib.import_module("harvest_notebook_outputs")
    assert hno.CARRIED_PREFIX == cr.CARRIED_PREFIX


def test_function_locals_of_a_skipped_cell_are_not_session_names(cr):
    nb = {"cells": [_code("def f(df):\n    n = df.count()\n    return n\nf(spark.range(1))"), _code("x = 1"), _code("n = 3\nprint(n, x)")]}
    out, _, _ = cr.select_cells(nb, "3:3")  # n is bound in the piece; f's locals are not the session's
    assert len(out["cells"]) == 2


def test_magic_cells_other_than_install_run_restart_are_not_setup(cr):
    assert not cr.is_setup_cell(_code("%sql\nSELECT 1"))
    assert cr.is_setup_cell(_code("dbutils.library.restartPython()"))
    assert not cr.is_setup_cell(_code("x = spark.range(3)"))
    assert cr.is_setup_cell(_code('"""doc"""\nA = [1, 2]\nB: int = 3'))


def test_the_runner_uploads_the_piece_and_harvests_only_its_cells(cr, monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    rns = importlib.import_module("run_notebooks_serverless")
    raw = json.dumps(NB).encode()
    sliced = json.loads(rns._select_piece(raw, "8:9", None))
    srcs = [cr.cell_source(c) for c in sliced["cells"]]
    assert "print(helper(1))" in srcs and "df = spark.table(TABLE)\nheavy = df.count()" not in srcs
