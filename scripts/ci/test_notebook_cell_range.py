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


def test_a_carried_statement_whose_inputs_only_the_skipped_compute_binds_is_refused(cr):
    # `land_wkb = land_geom.wkb` has no call, but land_geom comes from the cell's compute: carrying the line alone
    # would raise NameError on the compute, so the piece is refused with the prelude to add.
    nb = {"cells": [_code("land_geom = make_geom()\nland_wkb = land_geom.wkb\nN = 3"), _code("print(land_wkb, N)")]}
    with pytest.raises(ValueError, match=r"land_wkb \(cell 1\).*--prelude 1"):
        cr.select_cells(nb, "2:2")
    nb = {"cells": [_code("import os\nSEP = os.sep\nrows = spark.range(9).collect()\nF = lambda x: x"),
                    _code("print(SEP, F(1))")]}
    carried = cr.cell_source(cr.select_cells(nb, "2:2")[0]["cells"][0])  # inputs bound by carried lines: carried
    assert "SEP = os.sep" in carried and "F = lambda x: x" in carried


def test_a_prelude_cell_that_needs_a_skipped_compute_cell_is_refused_too(cr):
    # The --prelude cell re-runs in the piece, so the names it reads count like the piece's own.
    nb = {"cells": [_code("import os\nF = make_functions()"), _code("land = F.col('x')"), _code("print(land)")]}
    with pytest.raises(ValueError, match=r"F \(cell 1\).*--prelude 1"):
        cr.select_cells(nb, "3:3", prelude="2")
    assert len(cr.select_cells(nb, "3:3", prelude="1,2")[0]["cells"]) == 3


def test_carried_lines_run_where_their_cell_was_before_a_later_prelude_cell(cr):
    # Cell 1's import is carried; prelude cell 2 reads it, so the carried cell must come before cell 2.
    nb = {"cells": [_code("from pyspark.sql import functions as F\nrows = spark.range(3).collect()"),
                    _code("land = F.col('x')\nn = land.count()"), _code("print(land)")]}
    out, _, pre = cr.select_cells(nb, "3:3", prelude="2")
    srcs = [cr.cell_source(c) for c in out["cells"]]
    assert pre == [1] and srcs[0].startswith(cr.CARRIED_PREFIX + " 1 ") and "import functions as F" in srcs[0]
    assert srcs[1].startswith("land = F.col") and srcs[2] == "print(land)"


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


def test_a_cell_after_the_piece_is_never_a_source(cr):
    # rx comes from %run ./config_nb; a later cell that re-imports it (or computes it) must not refuse or feed the piece.
    nb = {"cells": [_code("%run ./config_nb"), _code("print(rx, df)"), _code("df = spark.range(1).toDF('x')"),
                    _code("from x import functions as rx")]}
    out, _, _ = cr.select_cells(nb, "1:2")
    assert [cr.cell_source(c) for c in out["cells"]] == ["%run ./config_nb", "print(rx, df)"]


# --- params cells with a print; names read through globals(); names a %run notebook defines or reads -------------

FIXTURES = REPO_ROOT / "examples" / "tests" / "fixtures"
PIECES = json.loads((FIXTURES / "cell_range_pieces.ipynb").read_text())
# cells: 1 md, 2 params + print, 3 %run ./cell_range_pieces_config, 4 compute (rows), 5 reads via globals(), 6 reads rows


def test_a_params_cell_with_a_print_is_setup_but_a_cell_that_works_is_not(cr):
    assert cr.is_setup_cell(_code('DEMO = True\nTABLE_PREFIX = "x_"\nprint(f"DEMO={DEMO}")'))
    assert cr.is_setup_cell(_code('import logging\nlogger = logging.getLogger("x")\nN = int(os.environ.get("N", 3))\n'
                                  'if N > 2:\n    MODE = "big"\nelse:\n    MODE = "small"\nlogger.info("mode %s", MODE)'))
    assert cr.is_setup_cell(_code('print("{} rows".format(len(NAMES)), sep="")\nT = globals().get("T", "t")'))
    for src in ("df = spark.table('x')\nprint(df.count())",       # a Spark action
                "print(spark.range(3).count())",                   # a print that works
                "DEMO = True\nspark.range(3).write.saveAsTable('t')",  # a write
                "DEMO = True\nprint(do_work())",                   # an unknown call
                "DEMO = True\nT = make_table()",                   # an assignment of a call
                "if DEMO:\n    spark.sql('DROP TABLE t')"):         # an if over work
        assert not cr.is_setup_cell(_code(src)), src


def test_the_params_cell_with_a_print_reaches_the_piece(cr):
    out, _, pre = cr.select_cells(PIECES, "5:5", base_dir=FIXTURES)
    assert pre == [1, 2]  # the params cell (2) and the %run (3); not the compute (4)
    srcs = [cr.cell_source(c) for c in out["cells"]]
    assert srcs[0].startswith("DEMO = True") and "print(f\"DEMO={DEMO}\")" in srcs[0]
    assert srcs[1] == "%run ./cell_range_pieces_config" and "rows = " not in "".join(srcs)


def test_a_setup_cell_reading_a_name_only_a_skipped_cell_defines_is_not_kept(cr):
    # cell 2 would raise NameError on `rows` in the piece: it is skipped, and only its FLAG line is carried
    nb = {"cells": [_code("rows = spark.range(3).count()"), _code('print(f"n={rows}")\nFLAG = True'), _code("print(FLAG)")]}
    assert cr.auto_prelude(nb["cells"], cr.parse_range("3:3", 3)) == []
    out, _, pre = cr.select_cells(nb, "3:3")
    carried = cr.cell_source(out["cells"][0])
    assert pre == [] and "FLAG = True" in carried and "print(f" not in carried


def test_a_globals_read_of_a_name_only_a_skipped_cell_defines_is_refused_naming_the_cell(cr):
    with pytest.raises(ValueError, match=r"rows \(cell 4\).*--prelude 4"):
        cr.select_cells(PIECES, "6:6", base_dir=FIXTURES)
    nb = {"cells": [_code("X = make()"), _code('print(globals()["X"])')]}
    with pytest.raises(ValueError, match=r"X \(cell 1\)"):
        cr.select_cells(nb, "2:2")
    nb = {"cells": [_code("X = make()"), _code('print("X" in globals())')]}
    with pytest.raises(ValueError, match=r"X \(cell 1\)"):
        cr.select_cells(nb, "2:2")
    nb = {"cells": [_code("X = make()"), _code('print(globals().get("Y", 1))')]}  # not defined by a skipped cell: fine
    assert len(cr.select_cells(nb, "2:2")[0]["cells"]) == 1


def test_a_skipped_cells_setup_statements_are_carried_when_the_piece_reads_them_through_globals(cr):
    nb = {"cells": [_code('TABLE_PREFIX = "demo_"\nrows = spark.range(3).count()'),
                    _code('print(globals().get("TABLE_PREFIX", ""))')]}
    out, _, _ = cr.select_cells(nb, "2:2")
    carried = cr.cell_source(out["cells"][0])
    assert carried.startswith(cr.CARRIED_PREFIX) and 'TABLE_PREFIX = "demo_"' in carried and "rows" not in carried


def test_a_name_a_run_notebook_reads_is_used_by_the_piece(cr):
    # cell_range_pieces_config reads TABLE_PREFIX through globals().get: a skipped cell before the %run that computes it
    # is refused; one that only assigns it is carried; one after the %run never ran before it.
    cfg = {"cells": [_code("TABLE_PREFIX = make_prefix()"), _code("%run ./cell_range_pieces_config"), _code("print(1)")]}
    with pytest.raises(ValueError, match=r"TABLE_PREFIX \(cell 1\).*--prelude 1"):
        cr.select_cells(cfg, "3:3", base_dir=FIXTURES)
    assert len(cr.select_cells(cfg, "3:3")[0]["cells"]) == 2  # no base_dir: the notebook cannot be read
    carry = {"cells": [_code('TABLE_PREFIX = "p_"\nrows = spark.range(3).count()'), _code("%run ./cell_range_pieces_config"),
                       _code("print(1)")]}
    out, _, pre = cr.select_cells(carry, "3:3", base_dir=FIXTURES)
    assert 'TABLE_PREFIX = "p_"' in cr.cell_source(out["cells"][0]) and pre == [1]
    after = {"cells": [_code("%run ./cell_range_pieces_config"), _code("TABLE_PREFIX = make_prefix()"), _code("print(1)")]}
    assert len(cr.select_cells(after, "3:3", base_dir=FIXTURES)[0]["cells"]) == 2


def test_a_name_a_run_notebook_defines_is_defined_for_the_cells_before_it_only(cr):
    # the config defines CONFIG_VALUE and TABLE_NAME; a skipped cell before the %run is overwritten by it, one after overrides it
    before = {"cells": [_code("TABLE_NAME = make()"), _code("%run ./cell_range_pieces_config"), _code("print(TABLE_NAME)")]}
    out, _, _ = cr.select_cells(before, "3:3", base_dir=FIXTURES)
    assert [cr.cell_source(c) for c in out["cells"]] == ["%run ./cell_range_pieces_config", "print(TABLE_NAME)"]
    with pytest.raises(ValueError, match=r"TABLE_NAME \(cell 1\)"):  # without the notebook the name is the skipped cell's
        cr.select_cells(before, "3:3")
    after = {"cells": [_code("%run ./cell_range_pieces_config"), _code("TABLE_NAME = make()"), _code("print(TABLE_NAME)")]}
    with pytest.raises(ValueError, match=r"TABLE_NAME \(cell 2\)"):
        cr.select_cells(after, "3:3", base_dir=FIXTURES)


def test_prelude_stays_the_manual_override(cr):
    out, _, pre = cr.select_cells(PIECES, "6:6", prelude="4", base_dir=FIXTURES)
    assert 3 in pre and "rows = spark.range(3).count()" in [cr.cell_source(c) for c in out["cells"]]


def test_the_runner_passes_the_notebooks_folder_so_run_notebooks_resolve(cr, monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    rns = importlib.import_module("run_notebooks_serverless")
    raw = (FIXTURES / "cell_range_pieces.ipynb").read_bytes()
    with pytest.raises(ValueError, match=r"rows \(cell 4\)"):
        rns._select_piece(raw, "6:6", None, FIXTURES)
    assert "DEMO = True" in rns._select_piece(raw, "5:5", None, FIXTURES).decode()


# ---------------------------------------------- the cell-by-cell runner skips Databricks-only fixtures


@pytest.fixture
def cbc(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    return importlib.import_module("run_notebooks_cell_by_cell")


def _write_nb(path, *, cell_by_cell=None):
    meta = {} if cell_by_cell is None else {"gbx": {"cell_by_cell": cell_by_cell}}
    path.write_text(json.dumps({"cells": [], "metadata": meta, "nbformat": 4, "nbformat_minor": 5}))


def test_a_directory_run_skips_notebooks_marked_not_for_cell_by_cell(cbc, monkeypatch, tmp_path):
    # harvest_smoke / cell_range_pieces need Spark, display and a Volumes wheel: a kernel-less exec() run on the
    # dev container fails on them by design. They carry metadata.gbx.cell_by_cell = false.
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True)
    _write_nb(fixtures / "plain.ipynb")
    _write_nb(fixtures / "databricks_only.ipynb", cell_by_cell=False)
    _write_nb(fixtures / "marked_true.ipynb", cell_by_cell=True)
    monkeypatch.setattr(cbc, "EXAMPLES_ROOT", tmp_path)
    names = lambda found: [p.name for p in found]  # noqa: E731
    assert names(cbc.discover_notebooks(None)) == ["marked_true.ipynb", "plain.ipynb"]
    assert names(cbc.discover_notebooks("fixtures")) == ["marked_true.ipynb", "plain.ipynb"]
    # Named on its own, the notebook runs: the caller asked for it.
    assert names(cbc.discover_notebooks("tests/fixtures/databricks_only.ipynb")) == ["databricks_only.ipynb"]


def test_the_databricks_only_fixtures_are_marked():
    for name in ("cell_range_pieces", "harvest_smoke"):
        nb = json.loads((FIXTURES / f"{name}.ipynb").read_text())
        assert nb["metadata"]["gbx"]["cell_by_cell"] is False, name
