"""Unit tests for the two-leg vector pipeline additions to readers.py:

- run_format_read(..., ingest_table=...) -- reader leg writes to Delta/Parquet table
- run_vector_write(..., src_is_table=True) -- writer leg reads from a table
"""

import json
import sys

from databricks.labs.gbx.bench import readers


def _tiny_geojson(path: str, n: int = 5) -> None:
    """Write a minimal GeoJSON FeatureCollection with ``n`` point features."""
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
            "properties": {"id": i, "name": f"feat_{i}"},
        }
        for i in range(n)
    ]
    fc = {"type": "FeatureCollection", "features": features}
    with open(path, "w") as fh:
        json.dump(fc, fh)


# ---------------------------------------------------------------------------
# Test A: run_format_read with ingest_table
# ---------------------------------------------------------------------------


def test_run_format_read_ingests_to_table(spark, tmp_path):
    from databricks.labs.gbx.ds.register import register

    register(spark)

    n_features = 5
    gj_path = str(tmp_path / "tiny.geojson")
    _tiny_geojson(gj_path, n=n_features)

    tbl = "t_vec_ingest"
    # Drop in case a prior failed run left it.
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")

    try:
        r = readers.run_format_read(
            spark,
            gj_path,
            run_id="test",
            warmup=1,
            measured=1,
            api="lightweight",
            fmt="geojson_gbx",
            ingest_table=tbl,
            where="venv",
        )
        assert r.status == "ok", f"expected ok, got error: {r.note}"
        assert r.rows == n_features, f"rows mismatch: {r.rows} != {n_features}"
        assert r.note == f"geojson_gbx -> {tbl}"
        # The Delta/Parquet-backed table must exist and have the right count.
        assert spark.table(tbl).count() == n_features
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")


# ---------------------------------------------------------------------------
# Test B: run_vector_write with src_is_table=True
# ---------------------------------------------------------------------------


def test_run_vector_write_from_table(spark, tmp_path):
    from databricks.labs.gbx.bench.corpus_vector import generate_polygon_seed
    from databricks.labs.gbx.ds.register import register

    register(spark)

    n_rows = 8
    src_tbl = "t_vec_wsrc"
    spark.sql(f"DROP TABLE IF EXISTS {src_tbl}")

    try:
        # Build the writer-schema source table (same schema as the cluster pipeline).
        generate_polygon_seed(spark, n_rows).write.mode("overwrite").saveAsTable(
            src_tbl
        )

        out_dir = str(tmp_path / "vec_out")
        r = readers.run_vector_write(
            spark,
            src_tbl,
            out_dir,
            run_id="test",
            warmup=1,
            measured=1,
            fmt="geojson_gbx",
            src_is_table=True,
            where="venv",
        )
        assert r.status == "ok", f"expected ok, got error: {r.note}"
        assert r.rows > 0
        # rows must equal the source table size.
        assert r.rows == n_rows, f"rows mismatch: {r.rows} != {n_rows}"
        # The driver-side Python workers run the commit: their peak RSS is reported. The
        # sampler reads /proc, so the note carries it on Linux only (macOS: "nothing sampled").
        if sys.platform.startswith("linux"):
            assert "driver python-worker peak RSS" in r.note, r.note
            assert "MB anonymous" in r.note, r.note
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {src_tbl}")


def test_run_vector_write_omits_the_worker_rss_when_nothing_was_sampled(
    spark, tmp_path, monkeypatch
):
    """With no /proc to sample (peak 0) the note says nothing about worker memory, rather
    than 'peak RSS 0 MB', and peak_rss_mb falls back to this process's peak."""
    from databricks.labs.gbx.bench.corpus_vector import generate_polygon_seed
    from databricks.labs.gbx.ds.register import register

    class _Nothing:
        peak_mb = 0.0
        peak_anon_mb = 0.0

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

    monkeypatch.setattr(readers, "WorkerRssSampler", _Nothing)
    register(spark)
    src_tbl = "t_vec_wsrc_nosample"
    spark.sql(f"DROP TABLE IF EXISTS {src_tbl}")
    try:
        generate_polygon_seed(spark, 4).write.mode("overwrite").saveAsTable(src_tbl)
        r = readers.run_vector_write(
            spark,
            src_tbl,
            str(tmp_path / "vec_out"),
            run_id="test",
            warmup=1,
            measured=1,
            fmt="geojson_gbx",
            src_is_table=True,
            where="venv",
        )
        assert r.status == "ok", r.note
        assert "peak RSS" not in r.note and "anonymous" not in r.note, r.note
        assert r.note.endswith("write+readback of 4 features"), r.note
        assert r.peak_rss_mb > 0
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {src_tbl}")
