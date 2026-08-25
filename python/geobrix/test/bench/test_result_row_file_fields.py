"""TDD tests for FILE-access matrix fields on ResultRow (Phase 2 bench, Task 1)."""

import dataclasses


def test_resultrow_has_file_matrix_fields():
    from databricks.labs.gbx.bench.results import ResultRow

    names = {f.name for f in dataclasses.fields(ResultRow)}
    assert {
        "file_mode",
        "layout",
        "input_partitions",
        "launched_tasks",
        "slots_available",
        "chunk_size",
    } <= names


def test_resultrow_file_fields_default_to_na_and_zero():
    from databricks.labs.gbx.bench.results import ResultRow

    r = ResultRow(
        run_id="t",
        api="lightweight",
        fn="f",
        category="reader",
        mode="spark-path",
        tile_px=0,
        bands=0,
        dtype="",
        srid=0,
        rows=0,
        nodata_frac=0.0,
        warmup_iters=0,
        measured_iters=1,
        iter_median_s=0.0,
        iter_min_s=0.0,
        iter_p90_s=0.0,
        throughput_mpix_s=0.0,
        throughput_rows_s=0.0,
        peak_rss_mb=0.0,
        status="ok",
        note="",
        env_arch="",
        env_cpu_model="",
        env_cpu_count=0,
        env_os="",
        env_gbx_version="",
        env_gdal_version="",
        env_runtime_version="",
        env_where="cluster",
    )
    assert (r.file_mode, r.layout) == ("na", "na")
    assert (r.input_partitions, r.launched_tasks, r.slots_available, r.chunk_size) == (
        0,
        0,
        0,
        0,
    )


def test_cluster_order_matches_resultrow_fields():
    # Importing cluster runs the drift guard at module load; a mismatch raises.
    import importlib

    from databricks.labs.gbx.bench import cluster, results

    importlib.reload(cluster)
    assert set(cluster.ORDER) == {f.name for f in dataclasses.fields(results.ResultRow)}
