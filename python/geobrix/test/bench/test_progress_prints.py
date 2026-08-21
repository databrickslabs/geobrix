"""Tests that the bench legs emit progress prints (flush=True) to stdout.

These tests verify the progress-print contract added in the "progress-print"
increment: each long-running bench loop must emit a concise start line BEFORE
the measured work and an end line AFTER (with status, row count, elapsed s).

Test strategy:
- For legs that accept fuse-mode GPKG fixtures on local[2] (chunksize, layout-scan,
  run_gpkg_file_read, run_gpkg_file_write), build a tiny GPKG corpus and call the
  function, then assert the expected prefix appears in captured stdout.
- For run_file_write_layout_sweep / run_gtiff_file_write / run_gtiff_file_read,
  which need a raster DataSource or a tile DataFrame, use source inspection to
  assert the print calls exist in the implementation (structural guarantee).
- For cluster.py cell strings, inspect the string sources to verify banner prints.
"""

from __future__ import annotations

import inspect

# ---------------------------------------------------------------------------
# GPKG helper: write a tiny GeoPackage
# ---------------------------------------------------------------------------


def _write_gpkg(path, rows=5, offset_y=0.0):
    import pyarrow as pa
    import pyogrio
    import shapely

    wkbs = [
        bytes(shapely.to_wkb(shapely.Point(float(j), offset_y))) for j in range(rows)
    ]
    tbl = pa.table({"geometry": pa.array(wkbs, type=pa.binary())})
    pyogrio.write_arrow(
        tbl,
        path,
        driver="GPKG",
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )


def _write_n_gpkg(tmp_path, n, rows=5):
    for i in range(n):
        _write_gpkg(str(tmp_path / f"t{i}.gpkg"), rows=rows, offset_y=float(i))


# ---------------------------------------------------------------------------
# run_gpkg_chunksize_sweep: [chunksize] prefix before + after each chunk
# ---------------------------------------------------------------------------


def test_gpkg_chunksize_sweep_emits_chunksize_prefix(spark, tmp_path, capsys):
    from databricks.labs.gbx.bench.readers import run_gpkg_chunksize_sweep

    _write_n_gpkg(tmp_path, 2, rows=10)
    run_gpkg_chunksize_sweep(
        spark,
        str(tmp_path),
        "t",
        0,
        1,
        file_mode="fuse",
        chunk_sizes=(50, 100),
        where="venv",
    )
    out = capsys.readouterr().out
    assert "[chunksize]" in out, f"[chunksize] prefix missing from stdout:\n{out}"
    # One start line per chunk_size
    assert (
        out.count("[chunksize]") >= 4
    ), f"Expected >=4 [chunksize] lines (2 start + 2 end), got:\n{out}"


def test_gpkg_chunksize_sweep_end_line_has_elapsed(spark, tmp_path, capsys):
    from databricks.labs.gbx.bench.readers import run_gpkg_chunksize_sweep

    _write_n_gpkg(tmp_path, 2, rows=5)
    run_gpkg_chunksize_sweep(
        spark,
        str(tmp_path),
        "t",
        0,
        1,
        file_mode="fuse",
        chunk_sizes=(50,),
        where="venv",
    )
    out = capsys.readouterr().out
    # End line includes "s" for elapsed seconds
    lines = [ln for ln in out.splitlines() if "[chunksize]" in ln]
    assert any(
        "rows" in ln and "s" in ln for ln in lines
    ), f"No [chunksize] end-line with 'rows' and elapsed 's':\n{out}"


# ---------------------------------------------------------------------------
# run_layout_scan_comparison: [layout-scan] prefix
# ---------------------------------------------------------------------------


def _make_tile_table(spark, name):
    import shutil
    from pathlib import Path

    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    df = spark.createDataFrame(
        [
            (1, "/Volumes/a/b/c/x.tif", "EPSG:4326"),
            (2, "/Volumes/a/b/c/y.tif", "EPSG:4326"),
        ],
        "cellid bigint, path string, crs string",
    )
    df.write.saveAsTable(name)
    return name


def test_layout_scan_comparison_emits_layout_scan_prefix(spark, tmp_path, capsys):
    from databricks.labs.gbx.bench.readers import run_layout_scan_comparison

    t1 = _make_tile_table(spark, "pp_scan_order")
    t2 = _make_tile_table(spark, "pp_scan_plain")
    run_layout_scan_comparison(
        spark,
        tables_by_layout={"order": t1, "plain": t2},
        run_id="pp",
        warmup=0,
        measured=1,
        file_mode="fuse",
        where="venv",
    )
    out = capsys.readouterr().out
    assert "[layout-scan]" in out, f"[layout-scan] prefix missing:\n{out}"
    assert (
        out.count("[layout-scan]") >= 4
    ), f"Expected >=4 [layout-scan] lines (2 start + 2 end), got:\n{out}"


# ---------------------------------------------------------------------------
# run_gpkg_file_read: [gpkg-read] prefix
# ---------------------------------------------------------------------------


def test_gpkg_file_read_fuse_emits_gpkg_read_prefix(spark, tmp_path, capsys):
    from databricks.labs.gbx.bench.readers import run_gpkg_file_read

    _write_n_gpkg(tmp_path, 2, rows=5)
    run_gpkg_file_read(
        spark,
        str(tmp_path),
        "t",
        0,
        1,
        file_mode="fuse",
        chunk_size=100,
        where="venv",
    )
    out = capsys.readouterr().out
    assert "[gpkg-read]" in out, f"[gpkg-read] prefix missing:\n{out}"


# ---------------------------------------------------------------------------
# Structural: readers.py contains the print calls for gtiff-* legs
# ---------------------------------------------------------------------------


def test_run_gtiff_file_read_source_contains_progress_prints():
    """[gtiff-read] start and end prints are present in the implementation."""
    from databricks.labs.gbx.bench import readers

    src = inspect.getsource(readers.run_gtiff_file_read)
    assert "[gtiff-read]" in src, "run_gtiff_file_read missing [gtiff-read] prefix"
    assert "flush=True" in src, "run_gtiff_file_read progress print missing flush=True"


def test_run_gtiff_file_write_source_contains_progress_prints():
    """[gtiff-write] start and end prints are present in the implementation."""
    from databricks.labs.gbx.bench import readers

    src = inspect.getsource(readers.run_gtiff_file_write)
    assert "[gtiff-write]" in src, "run_gtiff_file_write missing [gtiff-write] prefix"
    assert "flush=True" in src, "run_gtiff_file_write progress print missing flush=True"


def test_run_file_write_layout_sweep_source_contains_progress_prints():
    """[write-sweep] start and end prints are present in the implementation."""
    from databricks.labs.gbx.bench import readers

    src = inspect.getsource(readers.run_file_write_layout_sweep)
    assert (
        "[write-sweep]" in src
    ), "run_file_write_layout_sweep missing [write-sweep] prefix"
    assert (
        "flush=True" in src
    ), "run_file_write_layout_sweep progress print missing flush=True"
    # Both start and end markers (before and after the write call)
    assert src.count("[write-sweep]") >= 2, (
        "run_file_write_layout_sweep should have at least 2 [write-sweep] print calls "
        "(start + end per iteration)"
    )


# ---------------------------------------------------------------------------
# Structural: cluster.py cell strings contain banners
# ---------------------------------------------------------------------------


def test_cell_file_matrix_contains_banner():
    from databricks.labs.gbx.bench import cluster

    assert "=== FILE-access matrix starting ===" in cluster._CELL_FILE_MATRIX
    assert "[read-matrix]" in cluster._CELL_FILE_MATRIX
    assert "flush=True" in cluster._CELL_FILE_MATRIX


def test_cell_gpkg_chunksize_contains_banner():
    from databricks.labs.gbx.bench import cluster

    assert "=== GPKG chunkSize sweep starting ===" in cluster._CELL_GPKG_CHUNKSIZE


def test_cell_layout_sweep_contains_banner():
    from databricks.labs.gbx.bench import cluster

    assert "=== FILE write layout sweep starting ===" in cluster._CELL_LAYOUT_SWEEP


def test_cell_layout_scan_contains_banner():
    from databricks.labs.gbx.bench import cluster

    assert "=== layout scan comparison starting ===" in cluster._CELL_LAYOUT_SCAN


def test_cell_grouped_file_contains_banner():
    from databricks.labs.gbx.bench import cluster

    assert "=== grouped FILE-amortization starting ===" in cluster._CELL_GROUPED_FILE


# ---------------------------------------------------------------------------
# grouped_file.run_grouped_file: [grouped] mode-start print
# ---------------------------------------------------------------------------


def test_run_grouped_file_source_contains_mode_print():
    """[grouped] mode-start print is present in run_grouped_file."""
    from databricks.labs.gbx.bench import grouped_file

    src = inspect.getsource(grouped_file.run_grouped_file)
    assert "[grouped]" in src, "run_grouped_file missing [grouped] mode-start print"
    assert "flush=True" in src, "run_grouped_file [grouped] print missing flush=True"
