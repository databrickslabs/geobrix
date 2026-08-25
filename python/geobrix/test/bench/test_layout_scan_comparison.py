"""TDD tests for the writer-layout effects leg (scan/pruning/shuffle-input).

Task 9 of the GeoBrix Phase-2 bench plan.

Verifies ``run_layout_scan_comparison``:
- emits exactly one ``"layout-scan"`` ResultRow per layout
- records the ``layout`` field from the ``tables_by_layout`` mapping
- does NOT invoke ``grouped_tile_map`` (measures scan cost, not grouped-read wall-clock)
- optional ``"layout-shuffle-input"`` rows are emitted when ``include_shuffle_input=True``
"""

import shutil
from pathlib import Path


def _make_tile_table(spark, name):
    """Create a minimal tile Delta table (path + crs) for layout-scan testing.

    Follows the ``_make_plain_table`` pattern from test_file_table_reader.py:
    drops any stale catalog entry and physical directory, writes a two-row
    Delta table via ``saveAsTable``, and returns the table name.
    The table has plain columns (no FILE column), so ``read_file_table``
    falls through to the external path-mode branch on any tier.
    """
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    df = spark.createDataFrame(
        [
            (1, "/Volumes/main/s/v/a.tif", "EPSG:4326"),
            (2, "/Volumes/main/s/v/b.tif", "EPSG:4326"),
        ],
        "cellid bigint, path string, crs string",
    )
    df.write.saveAsTable(name)
    return name


def test_layout_scan_comparison_row_per_layout(spark, tmp_path):
    """One 'layout-scan' row per layout, layout field set correctly."""
    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    # two small delta tables standing in for the two layouts
    t_ord = _make_tile_table(spark, "scan_order")
    t_pln = _make_tile_table(spark, "scan_plain")
    rows = run_layout_scan_comparison(
        spark,
        tables_by_layout={"order": t_ord, "plain": t_pln},
        run_id="t",
        warmup=0,
        measured=1,
        file_mode="fuse",
        where="venv",
    )
    assert {r.layout for r in rows} == {"order", "plain"}
    assert all(r.category == "layout-scan" for r in rows)


def test_layout_scan_comparison_status_ok(spark, tmp_path):
    """Rows are status='ok' when the table is non-empty."""
    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    t = _make_tile_table(spark, "scan_ok_chk")
    rows = run_layout_scan_comparison(
        spark,
        tables_by_layout={"plain": t},
        run_id="t2",
        warmup=0,
        measured=1,
        file_mode="fuse",
        where="venv",
    )
    assert len(rows) == 1
    assert rows[0].status == "ok"
    assert rows[0].rows == 2


def test_layout_scan_comparison_file_mode_recorded(spark, tmp_path):
    """file_mode is forwarded onto every emitted row."""
    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    t = _make_tile_table(spark, "scan_fmode")
    rows = run_layout_scan_comparison(
        spark,
        tables_by_layout={"order": t},
        run_id="t3",
        warmup=0,
        measured=1,
        file_mode="fuse",
        where="venv",
    )
    assert all(r.file_mode == "fuse" for r in rows)


def test_layout_scan_comparison_shuffle_input_rows(spark, tmp_path):
    """include_shuffle_input=True emits additional 'layout-shuffle-input' rows."""
    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    t = _make_tile_table(spark, "scan_shuf")
    rows = run_layout_scan_comparison(
        spark,
        tables_by_layout={"order": t},
        run_id="t4",
        warmup=0,
        measured=1,
        file_mode="fuse",
        where="venv",
        include_shuffle_input=True,
    )
    cats = {r.category for r in rows}
    assert "layout-scan" in cats
    assert "layout-shuffle-input" in cats


def test_layout_scan_comparison_no_grouped_tile_map(spark, tmp_path):
    """run_layout_scan_comparison must not import or call grouped_tile_map.

    Structural guard: inspect the source of run_layout_scan_comparison to confirm
    'grouped_tile_map' does not appear anywhere in its body — the leg measures
    scan/shuffle cost, not grouped-read wall-clock (which self-amortizes regardless
    of layout).
    """
    import inspect

    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    src = inspect.getsource(run_layout_scan_comparison)
    assert "grouped_tile_map" not in src, (
        "run_layout_scan_comparison must NOT call grouped_tile_map "
        "(scan leg measures scan/shuffle cost, not grouped-read wall-clock)"
    )


def test_layout_scan_comparison_shuffle_repartitions_by_tile_path(spark, tmp_path):
    """Shuffle-input leg repartitions by tile.path (struct subfield), not bare 'path'.

    Structural guard: the _shuffle closure inside run_layout_scan_comparison must use
    col("tile.path") for the repartition key.  A bare "path" string would silently
    random-repartition because read_file_table returns a DataFrame with a 'tile' struct
    column (whose .path sub-field carries the file path), not a top-level 'path' column.
    """
    import inspect

    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    src = inspect.getsource(run_layout_scan_comparison)
    # Must reference the struct sub-field, not a bare top-level column name.
    assert "tile.path" in src, (
        "_shuffle must repartition by tile.path (struct subfield); "
        "bare 'path' would silently random-repartition on a FILE table"
    )
    # Regression guard: the old bare-string form must not remain.
    assert 'repartition(n_parts, "path")' not in src, (
        '_shuffle must NOT use repartition(n_parts, "path") — '
        'that silently random-repartitions; use col("tile.path") instead'
    )


def test_layout_scan_comparison_shuffle_input_no_analysis_exception(spark, tmp_path):
    """include_shuffle_input=True completes without AnalysisException on a tile-struct table.

    Builds a table whose schema matches what read_file_table returns for a plain-path
    table (cellid + path + crs), exercises the _shuffle closure, and asserts it returns
    a positive count.  The test fails if the wrong repartition key raises AnalysisException.
    """
    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    t = _make_tile_table(spark, "scan_shuf_ae")
    rows = run_layout_scan_comparison(
        spark,
        tables_by_layout={"order": t},
        run_id="t_ae",
        warmup=0,
        measured=1,
        file_mode="fuse",
        where="venv",
        include_shuffle_input=True,
    )
    shuf_rows = [r for r in rows if r.category == "layout-shuffle-input"]
    assert len(shuf_rows) == 1, "expected exactly one layout-shuffle-input row"
    assert (
        shuf_rows[0].rows > 0
    ), "_shuffle returned 0 rows; repartition key may be wrong"
