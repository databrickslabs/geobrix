"""Checks the declare_ingest doc examples (Docker; real sample data).

Validates docs/docs/ingestion/auto-loader.mdx. ``declare_ingest`` only runs inside
a Lakeflow pipeline, so each displayed call is bound to the real signature and its
inputs go through the validation ``declare_ingest`` runs first, with the sample
data standing in for the landing folder.
"""

import ast
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import auto_loader_examples as ex  # noqa: E402
import mdx_tables  # noqa: E402

pytestmark = pytest.mark.deadline(360)

PAGE = "auto-loader"
PIPELINES = (ex.PIPELINE_SCD2, ex.PIPELINE_RASTER)


def test_displayed_snippets_name_real_api():
    from databricks.labs.gbx.ds.changes import ON_CHANGE_MODES, declare_ingest  # noqa: F401

    for snippet in PIPELINES:
        assert "from databricks.labs.gbx.ds.changes import declare_ingest" in snippet
        ast.parse(snippet)
    for mode in ON_CHANGE_MODES:  # the quick start's comment names every mode
        assert mode in ex.PIPELINE_SCD2
    from databricks.labs.gbx.ds.changes import ON_ERROR_MODES

    for mode in ON_ERROR_MODES:  # ... and every on_error value
        assert mode in ex.PIPELINE_SCD2


def test_snippet_calls_bind_to_real_signatures():
    """Every displayed declare_ingest call binds to the real signature, names a
    reader declare_ingest supports, and has a fully qualified target."""
    from databricks.labs.gbx.ds.changes import declare_ingest
    from databricks.labs.gbx.ds.paths import resolve_format

    for snippet in PIPELINES:
        (call,) = mdx_tables.calls(snippet, "declare_ingest")
        fmt = mdx_tables.bind(declare_ingest, call).arguments["format"]
        assert isinstance(fmt, str), "the displayed format is a literal"
        resolve_format(fmt)  # raises for a name that is not a reader
        target = ast.literal_eval(call.args[0])
        assert len(target.split(".")) == 3  # declare_ingest needs catalog.schema.table


@pytest.mark.parametrize("snippet", PIPELINES, ids=["scd2", "raster"])
def test_snippet_inputs_pass_declare_ingests_own_checks(snippet):
    """The steps declare_ingest runs before it declares anything: names and file
    pattern (_ingest_setup), then the table schema from the folder, which is the
    sample data in place of the landing folder."""
    from databricks.labs.gbx.ds import changes
    from databricks.labs.gbx.ds.paths import default_glob, path_schema

    (call,) = mdx_tables.calls(snippet, "declare_ingest")
    bound = mdx_tables.bind(changes.declare_ingest, call)
    bound.apply_defaults()
    a = bound.arguments
    setup = changes._ingest_setup(
        a["target"], a["source_dir"], a["format"], a["on_change"], a["glob"],
        a["schema_path"], a["options"], a["on_error"],
    )
    short = a["target"].split(".")[-1]
    assert setup["events"].endswith(f"{short}_file_events")
    assert setup["versions"].endswith(f"{short}_file_versions")
    assert setup["glob"] == default_glob(a["format"], a["options"])

    sample = ex.SAMPLE_FOR_LANDING[a["source_dir"]]
    schema = path_schema(a["format"], a["options"], sample, glob=setup["glob"])
    assert {"_source_path", "_source_mtime"} <= set(schema.fieldNames())


def _table_header(heading):
    text = mdx_tables.page(PAGE)
    section = text.split(f"\n## {heading}\n", 1)[1].split("\n#", 1)[0]
    header = next(line for line in section.splitlines() if line.startswith("|"))
    return [c.strip() for c in header.strip().strip("|").split("|")]


def test_on_change_tables_name_every_mode():
    """Both on_change tables have one column per mode, in the code's order."""
    from databricks.labs.gbx.ds.changes import ON_CHANGE_MODES

    header = _table_header("Choosing `on_change`")
    assert [re.search(r"`(\w+)`", c).group(1) for c in header[1:]] == list(ON_CHANGE_MODES)
    objects = mdx_tables.section_rows(PAGE, "Choosing `on_change`", level="##")
    names = [re.match(r"`([^`]+)`", row[0]).group(1) for row in objects if row[0].startswith("`")]
    assert {
        "<t>_file_events",
        "<t>_file_versions",
        "<target>_current",
        "<t>_bad_records",
    } <= set(names)


def test_query_current_and_as_of(spark, tmp_path):
    """QUERY_CURRENT against tables shaped like the pipeline's output: a file that
    changed twice, and whose newest content (h3) could not be read, so it has no
    rows: current is the latest readable version (h2)."""
    from databricks.labs.gbx.ds import changes

    rows, versions = "al_doc_boroughs", "al_doc_boroughs_file_versions"
    spark.sql(f"DROP VIEW IF EXISTS {rows}_current")
    spark.sql(f"DROP TABLE IF EXISTS {rows}")
    spark.sql(f"DROP TABLE IF EXISTS {versions}")
    try:
        # Built in SQL: no Python workers are needed for these few rows.
        spark.sql(
            f"""CREATE TABLE {rows} USING parquet LOCATION '{tmp_path / "rows"}' AS
            SELECT * FROM VALUES
                ('/a.geojson', 'h1', 1), ('/a.geojson', 'h1', 2),
                ('/a.geojson', 'h2', 1), ('/a.geojson', 'h2', 2), ('/a.geojson', 'h2', 3)
            AS t(_source_path, _source_hash, id)"""
        )
        spark.sql(
            f"""CREATE TABLE {versions} USING parquet LOCATION '{tmp_path / "versions"}' AS
            SELECT '/a.geojson' _source_path, 'h1' _source_hash,
                   TIMESTAMP'2026-10-17 10:00:00' __START_AT,
                   TIMESTAMP'2026-10-17 11:00:00' __END_AT
            UNION ALL
            SELECT '/a.geojson', 'h2', TIMESTAMP'2026-10-17 11:00:00',
                   TIMESTAMP'2026-10-17 13:00:00'
            UNION ALL
            SELECT '/a.geojson', 'h3', TIMESTAMP'2026-10-17 13:00:00',
                   CAST(NULL AS TIMESTAMP)"""
        )
        changes._ensure_current_view(spark, rows, versions)

        current, as_of = [
            s.strip().replace("main.geo.boroughs", rows)
            for s in ex.QUERY_CURRENT.split(";")
            if "SELECT" in s
        ]
        assert spark.sql(current).count() == 3  # the h2 rows
        assert spark.sql(as_of).count() == 3  # 12:00 falls in h2's interval
        earlier = as_of.replace("12:00:00", "10:30:00")  # inside h1's interval
        assert spark.sql(earlier).count() == 2
        assert spark.sql(as_of.replace("12:00:00", "09:00:00")).count() == 0
        # When the content could not be read, the as-of query has no rows.
        assert spark.sql(as_of.replace("12:00:00", "14:00:00")).count() == 0
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {rows}_current")
        spark.sql(f"DROP TABLE IF EXISTS {rows}")
        spark.sql(f"DROP TABLE IF EXISTS {versions}")


def _tables_under(heading):
    """Each table under ``### <heading>`` as a list of rows (cells), headers
    included."""
    text = mdx_tables.page(PAGE)
    section = text.split(f"\n### {heading}\n", 1)[1].split("\n#", 1)[0]
    tables, cur = [], []
    for line in section.splitlines():
        if line.startswith("|"):
            cur.append([c.strip() for c in line.strip().strip("|").split("|")])
        elif cur:
            tables.append(cur)
            cur = []
    if cur:
        tables.append(cur)
    return tables


def test_bad_records_tables_match_the_code():
    """The column table lists the bad-records table's columns in order, and the
    error_class table every check the code can record."""
    from databricks.labs.gbx.ds import _quarantine

    columns, classes = _tables_under("Query the bad records")
    assert [re.match(r"`(\w+)`", r[0]).group(1) for r in columns[2:]] == list(
        _quarantine.BAD_RECORDS_COLUMNS
    )
    assert sorted(re.match(r"`(\w+)`", r[0]).group(1) for r in classes[2:]) == sorted(
        _quarantine.CHECK_CLASSES
    )


def test_query_bad_records(spark, tmp_path):
    """QUERY_BAD_RECORDS against a table shaped like <t>_bad_records: two files,
    one with a feature set aside and one set aside whole."""
    from databricks.labs.gbx.ds import _quarantine

    table = "al_doc_boroughs_bad_records"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    path = "/Volumes/main/geo/landing/boroughs/boroughs.geojson"
    try:
        spark.sql(
            f"""CREATE TABLE {table} USING parquet LOCATION '{tmp_path / "bad"}' AS
            SELECT * FROM VALUES
              ('{path}', TIMESTAMP'2026-10-17 10:00:00', 'h1', 10L, 'feature=3',
               'UnreadableGeometry', 'geom_0 is not readable WKB', '{{"boroname": "Bronx"}}',
               TIMESTAMP'2026-10-17 10:05:00'),
              ('/Volumes/main/geo/landing/boroughs/bad.geojson',
               TIMESTAMP'2026-10-17 11:00:00', 'h2', 20L, CAST(NULL AS STRING),
               'pyogrio.errors.DataSourceError', 'not recognized', CAST(NULL AS STRING),
               TIMESTAMP'2026-10-17 11:05:00')
            AS t({", ".join(_quarantine.BAD_RECORDS_COLUMNS)})"""
        )
        per_file, features = [
            s.strip().replace("main.geo.boroughs_bad_records", table)
            for s in ex.QUERY_BAD_RECORDS.split(";")
            if "SELECT" in s
        ]
        rows = spark.sql(per_file).collect()
        assert [r["_source_path"] for r in rows] == [
            "/Volumes/main/geo/landing/boroughs/bad.geojson",
            path,
        ]
        (row,) = spark.sql(features).collect()
        assert row["record_index"] == "feature=3"
        assert row["error_class"] == "UnreadableGeometry"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
