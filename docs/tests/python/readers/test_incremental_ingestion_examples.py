"""Executes the incremental-ingestion doc examples (Docker; real sample data).

Validates docs/docs/readers/incremental-ingestion.mdx.
"""

import ast
import inspect
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import incremental_ingestion_examples as ex  # noqa: E402


def _expected_borough_rows(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    return spark.read.format("geojson_gbx").load(ex.SAMPLE_BOROUGHS_DIR).count()


def test_read_paths_batch(spark):
    df = ex.read_paths_batch(spark)
    rows = df.select("_source_path", "_source_mtime").collect()
    assert len(rows) == _expected_borough_rows(spark) > 0
    assert all(r["_source_path"].endswith("nyc_boroughs.geojson") for r in rows)
    assert all(r["_source_path"].startswith("/Volumes/") for r in rows)
    assert all(r["_source_mtime"] is not None for r in rows)
    assert {"geom_0", "geom_0_srid", "geom_0_srid_proj"} <= set(df.columns)
    assert "boroname" in df.columns  # the column READ_PATHS_BATCH selects


def test_read_paths_stream(spark, tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copy(f"{ex.SAMPLE_BOROUGHS_DIR}/nyc_boroughs.geojson", landing)
    out = ex.read_paths_stream(spark, str(landing), str(tmp_path / "checkpoint"))
    assert out.count() == _expected_borough_rows(spark)
    assert out.select("_source_path").distinct().count() == 1


def test_read_paths_stream_needs_schema_path(spark, tmp_path):
    from databricks.labs.gbx.ds.paths import read_paths

    stream = (
        spark.readStream.format("binaryFile")
        .schema("path string, modificationTime timestamp, length long, content binary")
        .load(str(tmp_path))
        .select("path", "modificationTime")
    )
    with pytest.raises(ValueError, match="schema_path"):
        read_paths(stream, "geojson_gbx")


def test_read_paths_raster_windows(spark):
    tiles = ex.read_paths_raster(spark)
    assert tiles.rdd.getNumPartitions() == 8  # test-only RDD use
    rows = tiles.select("tile.window", "_source_path").collect()
    sources = {r["_source_path"] for r in rows}
    assert sources and all(p.endswith(".tif") for p in sources)
    from databricks.labs.gbx.ds.register import register

    register(spark)
    whole = spark.read.format("gtiff_gbx").load(ex.SAMPLE_SENTINEL_DIR).count()
    assert len(rows) >= whole  # tileSize cuts each file into one or more windows


def test_displayed_snippets_name_real_api():
    from databricks.labs.gbx.ds.changes import ON_CHANGE_MODES, declare_ingest  # noqa: F401
    from databricks.labs.gbx.ds.paths import read_paths  # noqa: F401

    for snippet in (ex.READ_PATHS_BATCH, ex.READ_PATHS_STREAM, ex.READ_PATHS_RASTER):
        assert "from databricks.labs.gbx.ds.paths import read_paths" in snippet
        ast.parse(snippet)
    assert "read_paths(files" in ex.READ_PATHS_BATCH
    assert "declare_ingest" in ex.PIPELINE_SCD2
    for mode in ON_CHANGE_MODES:
        assert mode in ex.PIPELINE_SCD2


def _calls(snippet, name):
    return [
        n
        for n in ast.walk(ast.parse(snippet))
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == name
    ]


def test_snippet_calls_bind_to_real_signatures():
    """Every displayed read_paths / declare_ingest call binds to the real signature."""
    from databricks.labs.gbx.ds.changes import declare_ingest
    from databricks.labs.gbx.ds.paths import read_paths

    def bind(fn, call):
        args = [
            ast.literal_eval(a) if isinstance(a, ast.Constant) else object()
            for a in call.args
        ]
        kwargs = {
            k.arg: ast.literal_eval(k.value)
            if isinstance(k.value, (ast.Constant, ast.Dict))
            else object()
            for k in call.keywords
        }
        inspect.signature(fn).bind(*args, **kwargs)

    for snippet in (ex.READ_PATHS_BATCH, ex.READ_PATHS_STREAM, ex.READ_PATHS_RASTER):
        calls = _calls(snippet, "read_paths")
        assert calls
        for call in calls:
            bind(read_paths, call)
    (call,) = _calls(ex.PIPELINE_SCD2, "declare_ingest")
    bind(declare_ingest, call)
    target = ast.literal_eval(call.args[0])
    assert len(target.split(".")) == 3  # declare_ingest needs catalog.schema.table


def test_query_current_and_as_of(spark, tmp_path):
    """QUERY_CURRENT against tables shaped like the pipeline's output: a file that
    changed once, so two content versions with one closed interval."""
    from databricks.labs.gbx.ds import changes

    rows, versions = "ii_doc_boroughs", "ii_doc_boroughs_file_versions"
    spark.sql(f"DROP VIEW IF EXISTS {rows}_current")
    spark.sql(f"DROP TABLE IF EXISTS {rows}")
    spark.sql(f"DROP TABLE IF EXISTS {versions}")
    try:
        spark.createDataFrame(
            [("/a.geojson", "h1", 1), ("/a.geojson", "h1", 2)]
            + [("/a.geojson", "h2", i) for i in (1, 2, 3)],
            "_source_path string, _source_hash string, id int",
        ).write.option("path", str(tmp_path / "rows")).saveAsTable(rows)
        spark.sql(
            f"""CREATE TABLE {versions} USING parquet LOCATION '{tmp_path / "versions"}' AS
            SELECT '/a.geojson' _source_path, 'h1' _source_hash,
                   TIMESTAMP'2026-10-17 10:00:00' __START_AT,
                   TIMESTAMP'2026-10-17 11:00:00' __END_AT
            UNION ALL
            SELECT '/a.geojson', 'h2', TIMESTAMP'2026-10-17 11:00:00',
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
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {rows}_current")
        spark.sql(f"DROP TABLE IF EXISTS {rows}")
        spark.sql(f"DROP TABLE IF EXISTS {versions}")
