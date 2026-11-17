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
    cells = json.loads((NB_DIR / "01_smokehouse_creek.ipynb").read_text())["cells"]
    md = next(
        "".join(c["source"])
        for c in cells
        if c["cell_type"] == "markdown"
        and "".join(c["source"]).startswith("## 9. Ask the Genie Agents")
    )
    assert md.startswith("## 9. Ask the Genie Agents")  # two agents, plural
    assert "answers well under a minute" in md
    assert "under half a minute" not in md and "as long as the query above" not in md
    assert "space_id})" not in cell  # the title is printed, never the id


def test_the_stack_version_check_is_the_tested_helper():
    stack = _nb01('step("stack the master COGs")')
    assert "table_stack_version(spark, RASTERS)" in stack
    assert "def table_stack_version" not in stack  # one copy, in lakeflow/land/scenes.py
    assert "table_stack_version" in _nb01("from land.scenes import")


def test_no_genie_space_id_in_any_output():
    nb = json.loads((NB_DIR / "01_smokehouse_creek.ipynb").read_text())
    # a Genie space id is 32 hex digits starting 01f (file checksums are 32 hex digits too, and are not ids)
    ids = re.compile(r"\b01f[0-9a-f]{29}\b")
    for i, cell in enumerate(nb["cells"], 1):
        for out in cell.get("outputs", []):
            assert not ids.search(json.dumps(out)), f"cell {i} output holds a Genie space id"
