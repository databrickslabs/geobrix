"""Checks the Lakeflow SDP doc examples (Docker).

Validates docs/docs/ingestion/lakeflow-sdp.mdx: the dependency lines and the
channel table name the env wheel for the channel's runtime and extras that wheel
ships, the bundle matches the shape of the production example bundles, and the
sink helper behaves like the one declare_ingest uses.
"""

import re
import sys
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


# A runtime's major version -> the env wheel for it (DBR 17.3 / 18: env5; DBR 19: env6).
ENV_OF_RUNTIME = {"17": 5, "18": 5, "19": 6}


def _extras(env):
    """The feature extras the env<N> wheel ships: python/geobrix/envs/env<N>/extra-<name>.txt."""
    return {p.stem.removeprefix("extra-") for p in (REPO / f"python/geobrix/envs/env{env}").glob("extra-*.txt")}


def _requested_extras(requirement):
    return re.search(r"\[([\w,]+)\]", requirement).group(1).split(",")


def _wheel_env(requirement):
    return int(re.search(r"geobrix-[^/\s\[]*\+env([56])-py3-none-any\.whl", requirement).group(1))


@pytest.mark.parametrize("line", [ex.DEPENDENCY_VOLUME, ex.DEPENDENCY_GITHUB])
def test_dependency_lines_install_unquoted(line):
    """One requirement per line, with no whitespace anywhere (a pipeline runs an
    unquoted %pip install per line), naming the CURRENT channel's env wheel and extras
    that wheel ships (no light extra)."""
    assert "\n" not in line and not re.search(r"\s", line)
    match = re.fullmatch(
        r"geobrix\[([\w,]+)\]@(file:///Volumes/|https://github\.com/databrickslabs/"
        r"geobrix/releases/download/v<version>/)\S*geobrix-<version>\+env([56])-py3-none-any\.whl",
        line,
    )
    assert match, line
    env = int(match.group(3))
    assert env == ENV_OF_RUNTIME[ex.CHANNEL_WHEELS["CURRENT"][0]]
    assert set(match.group(1).split(",")) <= _extras(env)


def test_channel_table_matches_the_code_and_the_wheel():
    rows = mdx_tables.section_rows(PAGE, "Match the GeoBrix wheel to the channel")
    shown = {
        re.search(r"`(\w+)`", channel).group(1): (
            re.search(r"DBR (\d+)", runtime).group(1),
            re.search(r"\+env[56]", wheel).group(0),
        )
        for channel, runtime, wheel in rows
    }
    assert shown == ex.CHANNEL_WHEELS
    for runtime, wheel in shown.values():
        assert wheel == f"+env{ENV_OF_RUNTIME[runtime]}"
    assert f"+env{_wheel_env(ex.DEPENDENCY_VOLUME)}" == ex.CHANNEL_WHEELS["CURRENT"][1]


def _pipeline(bundle_text):
    (pipeline,) = yaml.safe_load(bundle_text)["resources"]["pipelines"].values()
    return pipeline


def test_bundle_has_the_shape_of_the_example_bundles():
    """The displayed pipeline resource uses only settings the production example
    bundles use (plus ``channel``), and lists GeoBrix the way they do: the Volume
    wheel path followed by its extras, one entry."""
    shown = _pipeline(ex.BUNDLE_PIPELINE)
    assert shown["serverless"] is True
    assert shown["channel"] in ex.CHANNEL_WHEELS
    used = set()
    for path in EXAMPLE_BUNDLES.values():
        example = _pipeline(path.read_text())
        used |= set(example)
        assert example["libraries"] == shown["libraries"]
        geobrix = [d for d in example["environment"]["dependencies"] if "gbx_wheel" in d]
        assert len(geobrix) == 1 and re.fullmatch(r"\$\{var\.gbx_wheel\}\[[\w,]+\]", geobrix[0]), geobrix
    assert set(shown) <= used | {"channel"}

    (dep,) = shown["environment"]["dependencies"]
    assert re.fullmatch(r"/Volumes/\S+/geobrix-<version>\+env[56]-py3-none-any\.whl\[[\w,]+\]", dep)
    env = _wheel_env(dep)
    assert f"+env{env}" == ex.CHANNEL_WHEELS[shown["channel"]][1]
    assert set(_requested_extras(dep)) <= _extras(env)


@pytest.mark.parametrize("name", EXAMPLE_BUNDLES)
def test_example_bundles_default_to_the_current_channels_wheel(name):
    """The production examples the page points to run on the default channel, CURRENT,
    so a bundle's default `gbx_wheel` is this release's env wheel for that channel's
    runtime (the artifact Volume keeps only the current version), and every extra a
    dependency asks for is one that wheel ships."""
    version = re.search(
        r'__version__ = "([^"]+)"',
        (REPO / "python/geobrix/src/databricks/labs/gbx/__init__.py").read_text(),
    ).group(1)
    bundle = yaml.safe_load(EXAMPLE_BUNDLES[name].read_text())
    assert "channel" not in _pipeline(EXAMPLE_BUNDLES[name].read_text())  # the default, CURRENT
    default = bundle["variables"]["gbx_wheel"]["default"]
    suffix = ex.CHANNEL_WHEELS["CURRENT"][1]
    assert default.rsplit("/", 1)[1] == f"geobrix-{version}{suffix}-py3-none-any.whl"
    env = int(suffix.removeprefix("+env"))
    deps = list(_pipeline(EXAMPLE_BUNDLES[name].read_text())["environment"]["dependencies"])
    deps += [d for job in bundle["resources"]["jobs"].values() for e in job["environments"] for d in e["spec"]["dependencies"]]
    for dep in deps:
        if "gbx_wheel" in dep:
            assert set(_requested_extras(dep)) <= _extras(env), dep


@pytest.mark.parametrize("name", EXAMPLE_BUNDLES)
def test_example_job_environments_pair_the_env_version_with_its_wheel(name):
    """A Serverless job environment installs the env wheel of its own environment
    version (version 5 -> the +env5 wheel), not the other family's, which refuses to import."""
    bundle = yaml.safe_load(EXAMPLE_BUNDLES[name].read_text())
    env = _wheel_env(bundle["variables"]["gbx_wheel"]["default"])
    environments = [e["spec"] for job in bundle["resources"]["jobs"].values() for e in job["environments"]]
    assert environments
    for spec in environments:
        assert int(spec["environment_version"]) == env
        assert [d for d in spec["dependencies"] if "gbx_wheel" in d], spec


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
