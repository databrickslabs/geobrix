"""The writer-layout effects leg (scan / shuffle-input): one ``layout-scan`` row per layout, tagged with its layout
and file mode, and a ``layout-shuffle-input`` row per layout when asked."""

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


def test_layout_scan_comparison_rows_per_layout_and_shuffle_input(spark):
    import inspect

    from databricks.labs.gbx.bench import readers
    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    rows = run_layout_scan_comparison(
        spark,
        tables_by_layout={
            "order": _make_tile_table(spark, "scan_order"),
            "plain": _make_tile_table(spark, "scan_plain"),
        },
        run_id="t",
        warmup=0,
        measured=1,
        file_mode="fuse",
        where="venv",
        include_shuffle_input=True,
    )
    scan = [r for r in rows if r.category == "layout-scan"]
    shuffle = [r for r in rows if r.category == "layout-shuffle-input"]
    assert {r.layout for r in scan} == {r.layout for r in shuffle} == {"order", "plain"}
    assert len(rows) == 4 and all(r.file_mode == "fuse" for r in rows)
    assert all(r.status == "ok" and r.rows == 2 for r in scan)
    # a wrong repartition key raises AnalysisException or returns nothing
    assert all(r.rows > 0 for r in shuffle)
    # the shuffle repartitions by the struct sub-field: a bare "path" would silently random-repartition
    assert "tile.path" in inspect.getsource(readers.run_layout_scan_comparison)
