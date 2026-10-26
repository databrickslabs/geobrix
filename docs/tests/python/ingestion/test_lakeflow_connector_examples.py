"""Checks the Lakeflow connector doc page against the connector (Docker).

Validates docs/docs/ingestion/lakeflow-connector.mdx. The connector module imports
the community-connectors framework, which the doc-test environment does not have,
so its tables (extensions, allowlist) are read from the connector's source and
spec rather than imported.
"""

import ast
import re
import sys
import tomllib
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import lakeflow_connector_examples as ex  # noqa: E402
import mdx_tables  # noqa: E402

REPO = Path(__file__).resolve().parents[4]
CONNECTOR = REPO / "connectors/lakeflow/sources/geobrix"
PAGE = "lakeflow-connector"


def _allowlist():
    spec = yaml.safe_load((CONNECTOR / "connector_spec.yaml").read_text())
    return spec["external_options_allowlist"]


def _module_constant(path, name):
    tree = ast.parse(Path(path).read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


def test_framework_commit_is_the_one_the_connector_is_tested_against():
    script = (REPO / "scripts/commands/gbx-test-connectors.sh").read_text()
    tested = re.search(r'^LFCC_COMMIT="([0-9a-f]{40})"', script, re.M).group(1)
    assert ex.FRAMEWORK_COMMIT == tested
    assert f"lakeflow-community-connectors@{tested}" in ex.BUILD_WHEELS


def test_build_commands_build_the_wheels_the_pipeline_installs():
    """The two `pip wheel` targets are the packages whose wheels the dependency
    lines name; the GeoBrix line names a shipped extra."""
    pyproject = tomllib.loads((REPO / "connectors/lakeflow/pyproject.toml").read_text())
    connector = pyproject["project"]["name"].replace("-", "_")
    assert "./geobrix/connectors/lakeflow" in ex.BUILD_WHEELS
    assert "lakeflow-community-connectors @ git+" in ex.BUILD_WHEELS
    lines = ex.PIPELINE_DEPENDENCIES.splitlines()
    assert len(lines) == 3 and not any(re.search(r"\s", line) for line in lines)
    assert re.fullmatch(r"geobrix\[light_dbr\d+\]@file:///Volumes/\S+\.whl", lines[0])
    assert lines[1].endswith("/lakeflow_community_connectors-<version>-py3-none-any.whl")
    assert lines[2].endswith(f"/{connector}-<version>-py3-none-any.whl")
    geobrix = tomllib.loads((REPO / "python/geobrix/pyproject.toml").read_text())
    extra = re.search(r"\[(\w+)\]", lines[0]).group(1)
    assert extra in geobrix["project"]["optional-dependencies"]
    assert "geobrix>=0.6.0" in pyproject["project"]["dependencies"]  # "0.6.0 or later"


def test_connection_options_match_the_spec():
    rows = {r[0].strip("`"): r for r in mdx_tables.section_rows(PAGE, "2. Create the connection")}
    assert set(rows) == {"sourceName", "externalOptionsAllowList", "root_path"}
    assert rows["sourceName"][2] == f"`{CONNECTOR.name}`"
    assert rows["externalOptionsAllowList"][2] == f"`{_allowlist()}`"
    assert [r["name"] for r in yaml.safe_load(
        (CONNECTOR / "connector_spec.yaml").read_text()
    )["connection"]["parameters"]] == ["root_path"]


def test_table_options_table_lists_the_allowlist():
    shown = [r[0].strip("`") for r in mdx_tables.section_rows(PAGE, "Table options", level="##")]
    assert sorted(shown) == sorted(_allowlist().split(","))


def test_formats_table_matches_the_readers_and_extensions():
    """One row per read_paths reader (short name), and each extension the
    connector infers is listed on its reader's row."""
    from databricks.labs.gbx.ds.paths import SUPPORTED_FORMATS

    table = {
        r[0].strip("`"): r for r in mdx_tables.section_rows(PAGE, "Formats", level="##")
    }
    assert sorted(f"{name}_gbx" for name in table) == sorted(SUPPORTED_FORMATS)
    # Each row links its reader's page, as the formats table of read-paths does.
    for name, (_, reader, _) in table.items():
        assert re.search(r"\]\(\.\./(readers|writers)/[\w-]+\)", reader), name
    ext_to_format = _module_constant(CONNECTOR / "lakeflow_connect.py", "EXT_TO_FORMAT")
    for ext, fmt in ext_to_format.items():
        assert f"`{ext}`" in table[fmt[: -len("_gbx")]][2], (ext, fmt)


def test_pipeline_template_uses_only_allowed_table_options():
    """The displayed template's table_configuration keys are allowlisted table
    options (plus the framework's scd_type), and its formats are reader names."""
    from databricks.labs.gbx.ds.paths import resolve_format

    spec = _module_constant(CONNECTOR / "pipeline/ingest.py", "pipeline_spec")
    allowed = set(_allowlist().split(","))
    assert spec["objects"]
    for entry in spec["objects"]:
        config = dict(entry["table"]["table_configuration"])
        assert config.pop("scd_type") == "APPEND_ONLY"
        assert set(config) <= allowed, set(config) - allowed
        resolve_format(config["format"])
