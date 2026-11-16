"""Notebook 01 and config_nb: the comment helper quotes, a rebuilt master cascades, nothing personal ships."""

import json
import re
from pathlib import Path

NB_DIR = Path(__file__).resolve().parents[2]


def _cells(name):
    nb = json.loads((NB_DIR / name).read_text())
    return ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]


def _nb01(containing):
    return next(c for c in _cells("01_smokehouse_creek.ipynb") if containing in c)


class _Spark:
    def __init__(self):
        self.sql_log = []

    def sql(self, text):
        self.sql_log.append(text)


def test_comment_table_escapes_single_quotes_and_backslashes():
    ns = {"spark": _Spark()}
    exec(
        _nb01("def comment_table"), {"step": None, **ns}, ns
    )  # defines comment_table in ns
    ns["comment_table"]("t", "Aoi's \\ shape", {"c": "it's the area's name"})
    comment, column = ns["spark"].sql_log
    assert comment == "COMMENT ON TABLE t IS 'Aoi\\'s \\\\ shape'"
    assert column == "ALTER TABLE t ALTER COLUMN c COMMENT 'it\\'s the area\\'s name'"


def test_a_rebuilt_master_rebuilds_what_is_derived_from_it():
    stack = _nb01('step("stack the master COGs")')
    assert "rebuilt.append(asset_id)" in stack
    assert "rasters_stale = bool(rebuilt)" in stack
    assert "DROP TABLE IF EXISTS burn_severity_summary" in stack
    assert (
        "are rebuilt from the new files" in stack
    )  # the cascade is printed, with its reason
    land = _nb01('step("land the COGs as a FILE table")')
    assert re.search(
        r"if FORCE_REBUILD or rasters_stale or not spark.catalog.tableExists\(RASTERS\):",
        land,
    )
    assert (
        "gbx.stack_version" in land
    )  # the table records the version it was written under


def test_config_ships_no_space_ids_and_no_host():
    cfg = "\n".join(_cells("config_nb.ipynb"))
    assert re.search(r'^GENIE_SPACE_ID = ""$', cfg, re.M)
    assert re.search(r'^GENIE_SPACE_ID_PRECOMPUTED = ""$', cfg, re.M)
    raw = (NB_DIR / "config_nb.ipynb").read_text() + (
        NB_DIR / "01_smokehouse_creek.ipynb"
    ).read_text()
    assert "cloud.databricks.com" not in raw
    assert "mjohns" not in raw


def test_the_agent_cell_prints_titles_and_strips_links():
    cell = _nb01('step("Genie Agent")')
    assert "space_title" in cell and "plain(" in cell
    assert "how many people live inside the fire perimeter" in cell
    assert "answers in under half a minute" in "".join(
        json.loads((NB_DIR / "01_smokehouse_creek.ipynb").read_text())["cells"][31][
            "source"
        ]
    )
