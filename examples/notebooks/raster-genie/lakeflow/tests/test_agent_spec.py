"""agent_spec: the slice Genie Agent's tables, functions, text and examples hold together."""

import json
import re
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve()
_REPO = _HERE.parents[5]
sys.path.insert(0, str(_HERE.parents[2] / "genie"))
import agent_spec as spec  # noqa: E402

MANIFEST = _REPO / "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
FUNCTION_NAME = re.compile(r"[\w.]*gbx_\w+")


@pytest.fixture(scope="module")
def l4_names():
    rows = json.loads(MANIFEST.read_text())["functions"]
    return {r["sql_name"] for r in rows if "L4" in r.get("levels", {})}


def test_every_function_is_qualified_and_an_l4_function(l4_names):
    assert spec.FUNCTIONS and len(set(spec.FUNCTIONS)) == len(spec.FUNCTIONS)
    for fq in spec.FUNCTIONS:
        catalog, schema, name = fq.split(".")
        assert (catalog, schema) == ("geospatial_catalog", "geobrix_uc"), fq
        assert (
            name in l4_names
        ), f"{name} has no L4 (Unity Catalog) level in the manifest"


def test_instruction_count_is_within_the_genie_limit():
    # functions + examples + 1 text block; tables do not count
    assert spec.INSTRUCTION_COUNT == len(spec.FUNCTIONS) + len(spec.EXAMPLES) + 1
    assert spec.INSTRUCTION_COUNT <= 100
    assert (
        spec.INSTRUCTION_COUNT_PRECOMPUTED
        == len(spec.FUNCTIONS_PRECOMPUTED) + len(spec.EXAMPLES_PRECOMPUTED) + 1
    )
    assert spec.INSTRUCTION_COUNT_PRECOMPUTED <= 100


def test_tables_are_qualified():
    assert spec.TABLES and spec.TABLES_PRECOMPUTED
    for t in (*spec.TABLES, *spec.TABLES_PRECOMPUTED):
        assert t.startswith("geospatial_catalog.raster_genie."), t


def _balanced(sql: str) -> bool:
    depth = 0
    for ch in re.sub(r"'[^']*'", "", sql):  # ignore parens inside string literals
        depth += ch == "("
        depth -= ch == ")"
        if depth < 0:
            return False
    return depth == 0


ALL_EXAMPLES = list(dict.fromkeys([*spec.EXAMPLES, *spec.EXAMPLES_PRECOMPUTED]))


@pytest.mark.parametrize("question,sql", ALL_EXAMPLES)
def test_example_sql_is_well_formed(question, sql):
    assert question.strip()
    body = sql.strip()
    assert re.match(r"(?is)(with|select)\b", body), question
    assert re.search(r"(?i)\bselect\b", body)
    assert _balanced(body), question
    assert not body.endswith(";")


@pytest.mark.parametrize("question,sql", ALL_EXAMPLES)
def test_example_functions_and_tables_are_qualified_and_attached(question, sql):
    for m in FUNCTION_NAME.finditer(sql):
        assert m.group(0) in {
            f for f in spec.FUNCTIONS
        }, f"{m.group(0)} is not an attached, qualified function"
    # every table is referenced with its full name; the CTE names of an example are not tables
    for t in (
        "rasters",
        "aois",
        "s2_scene_assets",
        "coverage",
        "burn_severity_summary",
    ):
        for m in re.finditer(rf"(?i)\b(from|join)\s+((?:[\w]+\.)*{t})\b", sql):
            assert m.group(2).startswith("geospatial_catalog.raster_genie."), (
                question,
                m.group(2),
            )


@pytest.mark.parametrize("question,sql", ALL_EXAMPLES)
def test_examples_pass_file_and_geometry_directly(question, sql):
    assert not re.search(r"(?i)\bas\s+file\b", sql), question
    assert "'NULL'" not in sql, question


@pytest.mark.parametrize("t", [spec.TEXT, spec.TEXT_PRECOMPUTED])
def test_text_says_to_pass_file_and_geom_directly_and_never_cast_to_file(t):
    assert "rasters.file" in t and "aois.geom" in t
    assert re.search(r"NEVER wrap them in CAST\(\.\.\. AS FILE\)", t)
    assert "Never pass the string 'NULL'" in t
    assert "geospatial_catalog.geobrix_uc.gbx_rst_" in t  # tells the agent to qualify
    assert "ST_ASGEOJSON" in t


@pytest.mark.parametrize("t", [spec.TEXT, spec.TEXT_PRECOMPUTED])
def test_text_functions_are_attached(t):
    for m in re.finditer(r"geospatial_catalog\.geobrix_uc\.gbx_rst_\w+", t):
        assert m.group(0) in spec.FUNCTIONS or m.group(0).endswith("gbx_rst_<name>")


@pytest.mark.parametrize("variant", ["live", "precomputed"])
def test_the_space_builds_and_counts_match(variant):
    from databricks.labs.gbx.genie.space import build_serialized_space

    tables, functions, text, examples = spec.VARIANTS[variant]
    s = build_serialized_space(tables, functions, text, examples)
    ins = s["instructions"]
    assert len(ins["sql_functions"]) == len(functions)
    assert len(ins["example_question_sqls"]) == len(examples)
    assert len(ins["text_instructions"]) == 1
    assert len(s["data_sources"]["tables"]) == len(tables)


def test_no_precomputed_result_is_in_the_live_space():
    assert not any("burn_severity_summary" in t for t in spec.TABLES)
    assert "burn_severity_summary" not in spec.TEXT
    for _, sql in spec.EXAMPLES:
        assert "burn_severity_summary" not in sql


def test_the_precomputed_space_is_the_live_one_plus_the_saved_table():
    assert spec.TABLES_PRECOMPUTED[: len(spec.TABLES)] == spec.TABLES
    extra = spec.TABLES_PRECOMPUTED[len(spec.TABLES) :]
    assert extra == ["geospatial_catalog.raster_genie.burn_severity_summary"]
    assert spec.FUNCTIONS_PRECOMPUTED == spec.FUNCTIONS
    assert "burn_severity_summary" in spec.TEXT_PRECOMPUTED
    assert re.search(r"(?i)read burn_severity_summary", spec.TEXT_PRECOMPUTED)
    assert any("burn_severity_summary" in sql for _, sql in spec.EXAMPLES_PRECOMPUTED)
    # the live chain stays available in the precomputed space, for a recompute
    assert any(
        "gbx_rst_nbr" in sql and "burn_severity_summary" not in sql
        for _, sql in spec.EXAMPLES_PRECOMPUTED
    )


def test_the_variants_map_to_the_constants():
    assert spec.VARIANTS["live"] == (
        spec.TABLES,
        spec.FUNCTIONS,
        spec.TEXT,
        spec.EXAMPLES,
    )
    assert set(spec.VARIANTS) == {"live", "precomputed"}


def _example(prefix: str) -> str:
    return next(sql for q, sql in spec.EXAMPLES if q.startswith(prefix))


def test_the_severity_example_is_the_live_geobrix_chain():
    sql = _example("How much land burned")
    for fn in ("fromfile", "nbr", "mapalgebra", "merge", "histogram", "pixelwidth"):
        assert f"gbx_rst_{fn}" in sql, fn
    assert "geospatial_catalog.raster_genie.rasters" in sql


def test_the_coverage_example_makes_a_live_read():
    sql = _example("What imagery and elevation")
    assert "raster_genie.coverage" in sql
    assert (
        "gbx_rst_fromfile" in sql
        and "gbx_rst_pixelwidth" in sql
        and "gbx_rst_pixelcount" in sql
    )


def test_the_text_is_consistent_and_teaches_the_method():
    for t in (spec.TEXT, spec.TEXT_PRECOMPUTED):
        assert "one self-contained SQL statement for each part of a question" in t
        assert "Compute every number in SQL" in t
        assert "never convert it to WKB or WKT" in t
        assert "1.3 to 2.5 minutes" in t
        assert "about a minute" not in t
        assert "as it is" not in t  # the example is adapted, not copied
        assert "adapt" in t and "aois" in t
        assert "kind = 'county'" in t
    # the method behind a question about any area: dNBR, the ring offset, the classes
    for word in (
        "dNBR",
        "ring",
        "offset",
        "Key and Benson",
        "gbx_rst_mapalgebra",
        "gbx_rst_histogram",
    ):
        assert word in spec.TEXT, word


def test_the_headline_numbers_are_sql_output_not_arithmetic():
    sev = _example("How much land burned")
    assert re.search(
        r"(?s)sum\(CASE WHEN pos \+ 1 >= 2 THEN n END\) OVER \(\).*AS low_or_worse_acres",
        sev,
    )
    summary = next(
        sql for q, sql in spec.EXAMPLES_PRECOMPUTED if "burn_severity_summary" in sql
    )
    assert "AS low_or_worse_acres" in summary
    people = _example("How many people")
    assert (
        re.search(r"gbx_rst_avg\(t\) AS avg_values", people)
        and "gbx_rst_pixelcount" in people
    )
    assert not re.search(r"\d\.\d{4,}\s*\*", people)  # no typed-in constant


def test_no_example_answers_the_composition_question():
    # the novel question (average burn severity of a county and its acres) is left to the agent
    for q, sql in (*spec.EXAMPLES, *spec.EXAMPLES_PRECOMPUTED):
        assert "Hutchinson" not in q and "Hutchinson" not in sql
        assert "county" not in q.lower() or "Carson" in q or "Gray" in q


class _FakeApi:
    """GET list + GET one over a dict of {space_id: (title, stored parent_path)}; records writes."""

    def __init__(self, spaces):
        self.spaces = spaces
        self.writes = []

    def do(self, method, path, query=None, body=None):
        if method != "GET":
            self.writes.append((method, path))
            return {}
        if path.endswith("/spaces"):
            return {
                "spaces": [{"space_id": i, "title": t} for i, (t, _) in self.spaces.items()]
            }
        sid = path.rsplit("/", 1)[1]
        return {"space_id": sid, "parent_path": self.spaces[sid][1]}


class _FakeClient:
    def __init__(self, spaces):
        self.api_client = _FakeApi(spaces)


@pytest.mark.parametrize(
    "stored",
    [
        "/Users/me@x.com/geobrix/raster-genie",
        "/Workspace/Users/me@x.com/geobrix/raster-genie",
        "/Users/me@x.com/geobrix/raster-genie/",
    ],
)
@pytest.mark.parametrize(
    "asked",
    [
        "/Workspace/Users/me@x.com/geobrix/raster-genie",
        "/Users/me@x.com/geobrix/raster-genie",
    ],
)
def test_find_space_matches_the_folder_with_or_without_the_workspace_prefix(stored, asked):
    import seed_agent

    client = _FakeClient({"s1": ("T", stored), "s2": ("Other", stored), "s3": ("T", "/Users/x/else")})
    assert seed_agent.find_space(client, "T", asked) == "s1"
    assert seed_agent.find_space(client, "Missing", asked) is None
    assert client.api_client.writes == []


# --- another catalog and schema: every qualified name follows ----------------------------------------------------------

_OTHER = ("my_cat", "my_data", "my_fns")


def _qualified_names(built):
    """Every catalog-qualified name in a built spec: tables, functions and each name in the text and the SQL."""
    texts = [built.TEXT, built.TEXT_PRECOMPUTED]
    texts += [sql for _, sql in built.EXAMPLES + built.EXAMPLES_PRECOMPUTED]
    found = set(built.TABLES + built.TABLES_PRECOMPUTED + built.FUNCTIONS)
    for t in texts:
        found |= set(re.findall(r"\b\w+\.\w+\.\w+", t))
    return found


def test_the_defaults_are_the_module_constants():
    built = spec.spec()
    assert built.TABLES == spec.TABLES and built.FUNCTIONS == spec.FUNCTIONS
    assert built.TEXT == spec.TEXT and built.EXAMPLES == spec.EXAMPLES
    assert built.VARIANTS == spec.VARIANTS


def test_an_override_reaches_every_qualified_name():
    catalog, schema, uc_schema = _OTHER
    built = spec.spec(catalog, schema, uc_schema)
    names = _qualified_names(built)
    assert names, "no qualified name found"
    for name in names:
        c, s, _ = name.split(".")
        assert c == catalog and s in (schema, uc_schema), name
    assert all(t.startswith(f"{catalog}.{schema}.") for t in built.TABLES_PRECOMPUTED)
    assert all(f.startswith(f"{catalog}.{uc_schema}.gbx_rst_") for f in built.FUNCTIONS)
    for variant in ("live", "precomputed"):
        tables, functions, text, examples = built.VARIANTS[variant]
        assert tables[0].startswith(f"{catalog}.{schema}.")
        assert functions == built.FUNCTIONS
        assert f"{catalog}.{uc_schema}.gbx_rst_fromfile" in text
        assert all(f"{catalog}." in sql for _, sql in examples)
    assert built.INSTRUCTION_COUNT == spec.INSTRUCTION_COUNT


def test_no_default_name_leaks_when_overridden():
    built = spec.spec(*_OTHER)
    blob = "\n".join(
        [built.TEXT, built.TEXT_PRECOMPUTED, *built.TABLES_PRECOMPUTED, *built.FUNCTIONS]
        + [sql for _, sql in built.EXAMPLES + built.EXAMPLES_PRECOMPUTED]
    )
    for default in (spec.CATALOG, spec.DATA_SCHEMA, spec.UC_SCHEMA):
        assert default not in blob, default
    assert "{T}" not in blob and "{G}" not in blob


@pytest.mark.parametrize("bad", ["", "a.b", "a b", "x`y", "c;DROP"])
def test_a_name_that_is_not_plain_is_refused(bad):
    with pytest.raises(ValueError, match="Fix:"):
        spec.spec(catalog=bad)
