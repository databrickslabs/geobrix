"""The raster-genie example diagram: its pills and chips come from the example, and the committed HTML is fresh.

Reads three sources, all in the repo: the agent spec's FUNCTIONS, the question constants of run_questions.py and
the generator resources/images/generators/raster-genie-architecture.py. Needs no Spark and no browser.
"""

import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
GENERATOR = ROOT / "resources/images/generators/raster-genie-architecture.py"
DIAGRAM = ROOT / "resources/images/diagrams/raster-genie/raster-genie-architecture.html"
AGENT_SPEC = ROOT / "examples/notebooks/raster-genie/genie/agent_spec.py"
RUN_QUESTIONS = ROOT / "examples/notebooks/raster-genie/genie/run_questions.py"

pytestmark = pytest.mark.skipif(
    not all(p.exists() for p in (GENERATOR, DIAGRAM, AGENT_SPEC, RUN_QUESTIONS)),
    reason="repo resources/ or examples/raster-genie not present",
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def html() -> str:
    return DIAGRAM.read_text()


@pytest.fixture(scope="module")
def text(html) -> str:
    body = re.sub(r"<style>.*?</style>|<!--.*?-->", " ", html, flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", body))


def test_every_attached_function_is_a_pill(html):
    fns = _load(AGENT_SPEC, "agent_spec_t").FUNCTIONS
    pills = re.findall(r'data-fn="(gbx_rst_[a-z0-9_]+)"', html)
    assert sorted(pills) == sorted(f.rsplit(".", 1)[-1] for f in fns)


def test_the_four_headline_questions_are_chips(html, text):
    assert re.findall(r'data-q="(Q\d)"', html) == ["Q1", "Q2", "Q3", "Q4"]
    src = RUN_QUESTIONS.read_text()
    for qid in ("Q1", "Q3"):  # the questions the example asks, read from the runner
        q = re.search(rf'^{qid} = "(.+)"$', src, re.M).group(1)
        assert q in text
    for audience in (
        "data product owner",
        "ag lender / insurer",
        "insurer / emergency mgmt",
        "operator / regulator",
    ):
        assert audience in text


def test_headline_and_blocks(text):
    assert "Raster is ready for Genie and SQL warehouses" in text
    for word in (
        "Sentinel-2 L2A",
        "WorldPop",
        "NIFC",
        "TIGER",
        "3DEP",
        "WorldCover",
        "TX RRC",
    ):
        assert word in text
    for word in (
        "512 px tiles",
        "overviews",
        "compression",
        "Standard",
        "Enhanced",
        "FILE column",
    ):
        assert word in text
    for word in (
        "aois",
        "s2_scene_assets",
        "coverage",
        "Live: GeoBrix composes",
        "Precomputed summary",
        "Genie Map",
    ):
        assert word in text


def test_no_h3_anywhere(text):
    assert not re.search(r"\bh3\b", text, re.I)


def test_committed_html_is_fresh(html):
    assert (
        _load(GENERATOR, "raster_genie_architecture_t").render() == html
    ), "stale: run bash scripts/commands/gbx-docs-diagrams.sh raster-genie-architecture and commit"


def test_chain_and_table_come_from_the_agent_spec(html):
    spec = _load(AGENT_SPEC, "agent_spec_chain")
    gen = _load(GENERATOR, "raster_genie_architecture_chain")
    steps = re.findall(r'data-step="([a-z0-9_]+)"', html)
    (extra,) = [
        t.rsplit(".", 1)[-1] for t in spec.TABLES_PRECOMPUTED if t not in spec.TABLES
    ]
    assert steps == [
        "fromfile",
        "nbr",
        "mapalgebra",
        "merge",
        "histogram",
        extra,
    ]  # live chain, then the table
    attached = {f.rsplit(".", 1)[-1] for f in spec.FUNCTIONS}
    assert all(f"gbx_rst_{n}" in attached for n in steps[:-1])
    assert gen.table_precomputed() == extra


def test_a_changed_spec_changes_the_diagram(tmp_path):
    """The diagram follows the spec: a renamed precomputed table and a dropped function show up on render."""
    gen = _load(GENERATOR, "raster_genie_architecture_mut")
    src = AGENT_SPEC.read_text()
    changed = tmp_path / "agent_spec.py"
    changed.write_text(
        src.replace("burn_severity_summary", "other_summary").replace(
            '        "median",\n', ""
        )
    )
    gen.AGENT_SPEC = changed
    fresh = gen.render()
    assert "other_summary" in fresh and "burn_severity_summary" not in fresh
    assert 'data-fn="gbx_rst_median"' not in fresh
    assert fresh != DIAGRAM.read_text()
