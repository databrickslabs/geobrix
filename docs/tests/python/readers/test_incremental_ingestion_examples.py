"""Executes the incremental-ingestion doc examples (Docker; real sample data).

Validates docs/docs/readers/incremental-ingestion.mdx.
"""

import ast
import faulthandler
import inspect
import math
import os
import shutil
import signal
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import incremental_ingestion_examples as ex  # noqa: E402

# Longest any test here may run (override per test with @_deadline).
_TEST_TIMEOUT_S = 300


def _deadline(seconds):
    def mark(fn):
        fn._deadline_s = seconds
        return fn

    return mark


def _on_deadline(name, seconds, capman, main_thread, finished):
    """Watchdog for a test still running after ``seconds``. If the JVM still answers,
    the test fails with every Python stack and the JVM thread dump in its captured
    output. If the JVM does not answer (it has been seen to wedge with every thread
    blocked under x86-64 emulation on Apple Silicon), every later Spark call would
    block too, so the run ends here with the stacks on the terminal."""
    from pyspark import SparkContext

    proc = getattr(SparkContext._gateway, "proc", None)
    answered = []
    probe = threading.Thread(
        target=lambda: answered.append(SparkContext._jvm.java.lang.System.nanoTime()),
        daemon=True,
    )
    probe.start()
    probe.join(15)
    if finished.is_set():
        return
    if not answered:
        capman.suspend_global_capture(in_=False)
        sys.stderr.write(
            f"\n{name} still running after {seconds} s and the JVM (pid "
            f"{getattr(proc, 'pid', '?')}) does not respond; ending the run.\n"
        )
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        sys.stderr.flush()
        if proc is not None:
            proc.kill()
        os._exit(1)
    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
    if proc is not None:
        os.kill(proc.pid, signal.SIGQUIT)  # thread dump to the JVM's stdout
        time.sleep(2)
    signal.pthread_kill(main_thread, signal.SIGALRM)


@pytest.fixture(autouse=True)
def _bounded(request, spark):
    """Per-test deadline, and no streaming query may outlive its test."""
    name = request.node.name
    seconds = getattr(request.function, "_deadline_s", _TEST_TIMEOUT_S)

    def _expired(signum, frame):
        raise TimeoutError(f"{name} still running after {seconds} s")

    previous = signal.signal(signal.SIGALRM, _expired)
    capman = request.config.pluginmanager.getplugin("capturemanager")
    finished = threading.Event()
    timer = threading.Timer(
        seconds,
        _on_deadline,
        (name, seconds, capman, threading.get_ident(), finished),
    )
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        finished.set()
        timer.cancel()
        signal.signal(signal.SIGALRM, previous)
        leaked = spark.streams.active
        for q in leaked:
            q.stop()
    assert not leaked, f"streaming queries left running: {[q.name for q in leaked]}"


@_deadline(3)
def test_a_test_past_its_deadline_fails_instead_of_blocking():
    with pytest.raises(TimeoutError, match="still running after 3 s"):
        time.sleep(120)


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
    # Sorts first but does not match the format's file pattern: the folder
    # schema_path skips it and takes the schema from the GeoJSON file.
    (landing / "0_readme.txt").write_text("not a GeoJSON file")
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


def _dims(path):
    import rasterio

    with rasterio.open(path) as src:
        return src.width, src.height


def _grid(path, tile):
    w, h = _dims(path)
    return math.ceil(w / tile) * math.ceil(h / tile)


def _windows_per_file(tiles):
    rows = tiles.select("tile.window", "_source_path").collect()
    return rows, Counter(r["_source_path"] for r in rows)


def test_read_paths_raster_windows(spark):
    """``tileSize`` cuts each raster into a grid of ceil(w/ts) * ceil(h/ts) windows,
    spread over ``num_partitions`` tasks. Every count comes from the files'
    dimensions, so this holds on the minimal and the complete sample bundle."""
    folder = ex.SAMPLE_SENTINEL_DIR
    files = sorted(os.path.join(folder, n) for n in os.listdir(folder) if n.endswith(".tif"))
    assert files

    # The displayed size (512) over the whole folder.
    tiles = ex.read_paths_raster(spark)
    assert tiles.rdd.getNumPartitions() == 8  # test-only RDD use
    rows, per_file = _windows_per_file(tiles)
    assert per_file == {f: _grid(f, 512) for f in files}
    assert all(0 < r.window.width <= 512 and 0 < r.window.height <= 512 for r in rows)

    # A smaller grid on the smallest raster: several windows (64 px on the sample
    # rasters), at most 8 x 8 however large the bundle's rasters are.
    smallest = min(files, key=lambda f: (math.prod(_dims(f)), f))
    tile = max(64, math.ceil(max(_dims(smallest)) / 8))
    rows, per_file = _windows_per_file(
        ex.read_paths_raster(spark, smallest, tile_size=str(tile))
    )
    assert per_file == {smallest: _grid(smallest, tile)}
    assert len(rows) > 1
    assert all(0 < r.window.width <= tile and 0 < r.window.height <= tile for r in rows)


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


def test_object_store_uri_is_rejected(spark):
    """Paths section: an object-store URI or GDAL virtual path fails with an error
    naming it."""
    from databricks.labs.gbx.ds.paths import read_paths

    files = ex._files(spark, ex.SAMPLE_BOROUGHS_DIR, "*.geojson")
    for uri in ("s3://bucket/landing", "/vsis3/bucket/landing", "/vsimem/landing"):
        with pytest.raises(ValueError, match=f"Unity Catalog Volume.*{uri}"):
            read_paths(files, "geojson_gbx", schema_path=uri)


def _mdx_section_rows(heading):
    mdx = (
        Path(__file__).resolve().parents[3]
        / "docs"
        / "readers"
        / "incremental-ingestion.mdx"
    ).read_text()
    section = mdx.split(f"### {heading}\n", 1)[1].split("\n#", 1)[0]
    rows = [
        [c.strip() for c in line.strip().strip("|").split("|")]
        for line in section.splitlines()
        if line.startswith("|")
    ]
    return rows[2:]  # past the header and separator rows


def test_default_file_patterns_table_matches_the_code():
    """The page's default-pattern table lists exactly DEFAULT_GLOBS."""
    from databricks.labs.gbx.ds.paths import DEFAULT_GLOBS

    shown = {}
    for formats, glob in _mdx_section_rows("Default file patterns"):
        for fmt in formats.split(","):
            shown[fmt.strip().strip("`")] = glob.strip("`")
    assert shown == DEFAULT_GLOBS


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
