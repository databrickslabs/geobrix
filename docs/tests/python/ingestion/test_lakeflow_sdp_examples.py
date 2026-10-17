"""Checks the Lakeflow SDP doc examples (Docker).

Validates docs/docs/ingestion/lakeflow-sdp.mdx: the dependency lines and the
channel table name extras the wheel ships, the bundle matches the shape of the
production example bundles, and the sink helper behaves like the one
declare_ingest uses.
"""

import re
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import lakeflow_sdp_examples as ex  # noqa: E402
import mdx_tables  # noqa: E402

REPO = Path(__file__).resolve().parents[4]
PAGE = "lakeflow-sdp"
EXAMPLE_BUNDLES = {
    "wireless-coverage": REPO / "examples/notebooks/wireless-coverage/lakeflow/databricks.yml",
    "vapor-eyes": REPO / "examples/notebooks/vapor-eyes/lakeflow/databricks.yml",
}


def _extras():
    pyproject = tomllib.loads((REPO / "python/geobrix/pyproject.toml").read_text())
    return set(pyproject["project"]["optional-dependencies"])


def _requested_extras(requirement):
    return re.search(r"\[([\w,]+)\]", requirement).group(1).split(",")


@pytest.mark.parametrize("line", [ex.DEPENDENCY_VOLUME, ex.DEPENDENCY_GITHUB])
def test_dependency_lines_install_unquoted(line):
    """One requirement per line, with no whitespace anywhere (a pipeline runs an
    unquoted %pip install per line), naming extras the wheel ships."""
    assert "\n" not in line and not re.search(r"\s", line)
    match = re.fullmatch(
        r"geobrix\[([\w,]+)\]@(file:///Volumes/|https://github\.com/databrickslabs/"
        r"geobrix/releases/download/v<version>/)\S*geobrix-<version>-py3-none-any\.whl",
        line,
    )
    assert match, line
    assert set(match.group(1).split(",")) <= _extras()


def test_channel_table_matches_the_code_and_the_wheel():
    rows = mdx_tables.section_rows(PAGE, "Match the GeoBrix extra to the channel")
    shown = {
        re.search(r"`(\w+)`", channel).group(1): (
            re.search(r"DBR (\d+)", runtime).group(1),
            extra.strip("`"),
        )
        for channel, runtime, extra in rows
    }
    assert shown == ex.CHANNEL_EXTRAS
    for runtime, extra in shown.values():
        assert extra == f"light_dbr{runtime}" and extra in _extras()
    assert _requested_extras(ex.DEPENDENCY_VOLUME) == [ex.CHANNEL_EXTRAS["CURRENT"][1]]


def _pipeline(bundle_text):
    (pipeline,) = yaml.safe_load(bundle_text)["resources"]["pipelines"].values()
    return pipeline


def test_bundle_has_the_shape_of_the_example_bundles():
    """The displayed pipeline resource uses only settings the production example
    bundles use (plus ``channel``), and lists GeoBrix the way they do: the Volume
    wheel path followed by its extras, one entry."""
    shown = _pipeline(ex.BUNDLE_PIPELINE)
    assert shown["serverless"] is True
    assert shown["channel"] in ex.CHANNEL_EXTRAS
    used = set()
    for path in EXAMPLE_BUNDLES.values():
        example = _pipeline(path.read_text())
        used |= set(example)
        assert example["libraries"] == shown["libraries"]
        (dep,) = example["environment"]["dependencies"]
        assert re.fullmatch(r"\$\{var\.gbx_wheel\}\[[\w,]+\]", dep), dep
    assert set(shown) <= used | {"channel"}

    (dep,) = shown["environment"]["dependencies"]
    assert re.fullmatch(r"/Volumes/\S+/geobrix-<version>-py3-none-any\.whl\[[\w,]+\]", dep)
    assert _requested_extras(dep) == [ex.CHANNEL_EXTRAS[shown["channel"]][1]]


@pytest.mark.parametrize("name", EXAMPLE_BUNDLES)
def test_example_bundles_use_the_current_channel_extra(name):
    """The production examples the page points to install the CURRENT channel's
    extra (they run on the default channel)."""
    example = _pipeline(EXAMPLE_BUNDLES[name].read_text())
    assert "channel" not in example  # the default, CURRENT
    (dep,) = example["environment"]["dependencies"]
    assert ex.CHANNEL_EXTRAS["CURRENT"][1] in _requested_extras(dep)


@pytest.mark.parametrize("name", EXAMPLE_BUNDLES)
def test_example_job_environments_pair_the_env_version_with_its_extra(name):
    """A Serverless job environment installs the extra of its own environment
    version (version 5 -> light_env5), not another regime's."""
    jobs = yaml.safe_load(EXAMPLE_BUNDLES[name].read_text())["resources"]["jobs"]
    environments = [e["spec"] for job in jobs.values() for e in job["environments"]]
    assert environments
    for spec in environments:
        (dep,) = spec["dependencies"]
        assert f"light_env{spec['environment_version']}" in _requested_extras(dep)


def test_table_exists_in_sink_snippet(spark):
    """The displayed helper answers like the one declare_ingest's sink uses, and
    still raises errors other than "not found"."""
    from databricks.labs.gbx.ds import changes

    # The docs component shows only the body of a snippet that starts with `def`.
    assert not ex.TABLE_EXISTS_IN_SINK.lstrip().startswith("def ")
    scope = {}
    exec(ex.TABLE_EXISTS_IN_SINK, scope)  # noqa: S102 - the displayed snippet
    table_exists = scope["table_exists"]

    spark.range(1).createOrReplaceTempView("sdp_doc_exists")
    try:
        for name, want in (("sdp_doc_exists", True), ("sdp_doc_missing", False)):
            assert table_exists(spark, name) is want
            assert changes._table_exists(spark, name) is want
        with pytest.raises(Exception, match="PARSE_SYNTAX_ERROR|ParseException"):
            table_exists(spark, "not a ( table name")
    finally:
        spark.catalog.dropTempView("sdp_doc_exists")
