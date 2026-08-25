"""TDD tests for Task 8: file_mode labeling on run_grouped_file / _ok_row / _err_row.

Tests that ``_ok_row`` / ``_err_row`` accept a ``file_mode`` param and store it
on the returned ``ResultRow``, and that ``run_grouped_file`` accepts and threads
the trailing ``file_mode`` kwarg through to every emitted row.

Uses direct ``_ok_row``/``_err_row`` assertions (no Spark needed) plus a
corpus-backed end-to-end run that mirrors the fixture pattern in
``test_grouped_file_bench.py``.
"""

import pytest

from databricks.labs.gbx.bench import grouped_file as gf

# ---------------------------------------------------------------------------
# Unit tests: _ok_row / _err_row accept and propagate file_mode
# ---------------------------------------------------------------------------

_STATS = {
    "warmup_iters": 0,
    "measured_iters": 1,
    "iter_median_ms": 50.0,
    "iter_min_ms": 50.0,
    "iter_p90_ms": 50.0,
    "iter_total_wall_clock_ms": 50.0,
    "avg_wall_clock_ms": 50.0,
}

_ENV = {
    "env_arch": "x86_64",
    "env_cpu_model": "test",
    "env_cpu_count": 2,
    "env_os": "Linux",
    "env_gbx_version": "0.5.0",
    "env_gdal_version": "3.8.0",
    "env_runtime_version": "py3.12",
    "env_where": "docker",
}


def test_ok_row_carries_file_mode():
    """_ok_row with file_mode='external' sets ResultRow.file_mode='external'."""
    row = gf._ok_row(
        "rst_clip_grouped",
        "grouped-file",
        "run-1",
        "virtual-file-on",
        "virtual",
        16,
        _STATS,
        _ENV,
        file_mode="external",
    )
    assert row.file_mode == "external"


def test_ok_row_default_file_mode_is_na():
    """_ok_row default file_mode='na' preserves existing callers."""
    row = gf._ok_row(
        "rst_clip_grouped",
        "grouped-file",
        "run-1",
        "materialized",
        "materialized",
        16,
        _STATS,
        _ENV,
        file_mode="na",
    )
    assert row.file_mode == "na"


def test_err_row_carries_file_mode():
    """_err_row with file_mode='managed' sets ResultRow.file_mode='managed'."""
    row = gf._err_row(
        "rst_clip_grouped",
        "grouped-file",
        "run-1",
        "virtual-file-on",
        "virtual",
        16,
        _ENV,
        RuntimeError("boom"),
        file_mode="managed",
    )
    assert row.file_mode == "managed"


def test_err_row_default_file_mode_is_na():
    """_err_row default file_mode='na' preserves existing callers."""
    row = gf._err_row(
        "rst_clip_grouped",
        "grouped-file",
        "run-1",
        "materialized",
        "materialized",
        16,
        _ENV,
        RuntimeError("oops"),
        file_mode="na",
    )
    assert row.file_mode == "na"


# ---------------------------------------------------------------------------
# Integration test: run_grouped_file accepts and threads file_mode
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def corpus(tmp_path_factory):
    """A tiny 2-COG x 8-window multiwindow corpus; returns the manifest path."""
    out = tmp_path_factory.mktemp("gf_label_corpus")
    return gf.synthesize_multiwindow_corpus(
        out,
        cog_count=1,
        windows_per_cog=2,
        cog_px=256,
        window_px=64,
        srid=4326,
        bands=1,
        dtype="float32",
        seed=42,
    )


def test_run_grouped_file_sets_file_mode(spark, corpus):
    """run_grouped_file with file_mode='external' -> all rows carry file_mode='external'."""
    rows = gf.run_grouped_file(
        spark,
        manifest_path=corpus,
        fns=["rst_clip_grouped"],
        modes=("materialized",),
        warmup=0,
        measured=1,
        progress=False,
        file_mode="external",
    )
    assert rows, "expected at least one row"
    assert all(
        r.file_mode == "external" for r in rows
    ), f"expected file_mode='external' on all rows; got: {[r.file_mode for r in rows]}"


def test_run_grouped_file_default_file_mode_is_na(spark, corpus):
    """run_grouped_file without file_mode= -> rows carry default 'na'."""
    rows = gf.run_grouped_file(
        spark,
        manifest_path=corpus,
        fns=["rst_clip_grouped"],
        modes=("materialized",),
        warmup=0,
        measured=1,
        progress=False,
    )
    assert rows, "expected at least one row"
    assert all(
        r.file_mode == "na" for r in rows
    ), f"expected file_mode='na' on all rows; got: {[r.file_mode for r in rows]}"
