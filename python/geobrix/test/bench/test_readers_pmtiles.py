"""Smoke test: the writer bench can run a pmtiles_gbx write and return a row."""

import os

from databricks.labs.gbx.bench.readers import run_pmtiles_write


def test_run_pmtiles_write_returns_row(spark, tmp_path):
    # shard_zoom=0 -> single-archive mode. The pmtiles_gbx writer completes a file-like
    # target's extension to `.pmtiles` (same naming contract the on-cluster caller relies
    # on, which passes a `.pmtiles` path), so the produced archive is `<out>.pmtiles`.
    out = str(tmp_path / "bench_tiles.pmtiles")
    row = run_pmtiles_write(
        spark,
        out_path=out,
        run_id="t",
        warmup=0,
        measured=1,
        n_tiles=8,
        shard_zoom=0,
        write_fmt="pmtiles_gbx",
    )
    assert row.category == "writer"
    assert row.fn == "pmtiles_gbx"
    assert row.status == "ok", row.note
    assert os.path.exists(out)
    assert os.path.getsize(out) > 0  # a real archive, not an empty placeholder
