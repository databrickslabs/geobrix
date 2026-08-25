"""Unit tests: large-raster bench profile (no cluster, no sample-data dependency).

All fixtures are tiny synthetic GeoTIFFs created in tmp_path so the tests run
locally in seconds.  The Spark session comes from the module-scoped ``spark``
fixture in conftest.py.
"""

from __future__ import annotations

import os

import numpy as np
import rasterio

from databricks.labs.gbx.bench.readers import (
    _write_striped_gtiff,
    _write_tiled_cog,
    run_large_raster_profile,
)

# ---------------------------------------------------------------------------
# Fixture dimensions.
# 256x256 is large enough that cog_convert generates >=1 overview level with
# the default cog_blocksize=64 (rule: max(w,h) > blocksize -> at least 1 level).
# ---------------------------------------------------------------------------

_W, _H, _B, _DT = 256, 256, 1, "float32"


def _decoded_mib(w, h, b, dt):
    return (w * h * b * np.dtype(dt).itemsize) / (1024 * 1024)


# ---------------------------------------------------------------------------
# Fixture-writer unit tests
# ---------------------------------------------------------------------------


def test_write_striped_gtiff_creates_valid_file(tmp_path):
    """_write_striped_gtiff produces a readable single-strip GeoTIFF."""
    p = str(tmp_path / "strip.tif")
    _write_striped_gtiff(p, _W, _H, _B, _DT)
    assert os.path.exists(p)
    with rasterio.open(p) as ds:
        assert ds.width == _W
        assert ds.height == _H
        assert ds.count == _B
        # Striped layout: no tile flag.
        assert not ds.profile.get("tiled", False)


def test_write_tiled_cog_creates_valid_file(tmp_path):
    """_write_tiled_cog produces a genuine COG: tiled AND has overview levels."""
    from databricks.labs.gbx.pyrx.core.cog import sniff_header

    p = str(tmp_path / "cog.tif")
    _write_tiled_cog(p, _W, _H, _B, _DT)
    assert os.path.exists(p)

    # Structural check via rasterio: tiled and has at least one overview.
    with rasterio.open(p) as ds:
        assert ds.width == _W
        assert ds.height == _H
        assert ds.profile.get("tiled", False), "COG must be internally tiled"
        ovr_count = len(ds.overviews(1))
        assert ovr_count >= 1, (
            f"COG must have >=1 overview level for band 1; got {ovr_count}. "
            "Fixture may be too small relative to cog_blocksize."
        )

    # Header-sniff check via the shared COG detector: is_cog must be True.
    raw = open(p, "rb").read()
    info = sniff_header(raw)
    assert info.is_cog, (
        f"sniff_header reports is_cog=False (tiled={info.tiled}, "
        f"overview_levels={info.overview_levels}). "
        "The fixture is not a genuine COG."
    )
    assert (
        info.overview_levels >= 1
    ), f"sniff_header found {info.overview_levels} overview IFD(s); need >=1."


def test_write_striped_is_idempotent(tmp_path):
    """Calling _write_striped_gtiff twice on the same path does not raise."""
    p = str(tmp_path / "strip.tif")
    _write_striped_gtiff(p, _W, _H, _B, _DT)
    mtime1 = os.path.getmtime(p)
    # Second call overwrites cleanly.
    _write_striped_gtiff(p, _W, _H, _B, _DT)
    mtime2 = os.path.getmtime(p)
    assert mtime2 >= mtime1  # file was rewritten (mtime advanced or equal)


# ---------------------------------------------------------------------------
# Profile construction: verify the right number of legs and metadata
# ---------------------------------------------------------------------------


def test_run_large_raster_profile_legs(spark, tmp_path):
    """Profile constructs the expected number of result rows for two strategies."""
    strategies = ("serverless", "classic")
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=strategies,
        where="venv",
    )
    # 2 strategies × 2 layouts (striped + cog) + 2 delta rows.
    assert len(rows) == 2 * 2 + 2, f"unexpected row count: {len(rows)}"


def test_run_large_raster_profile_categories(spark, tmp_path):
    """All result rows carry category='large_raster'."""
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("serverless",),
        where="venv",
    )
    for r in rows:
        assert r.category == "large_raster", f"unexpected category: {r.category}"


def test_run_large_raster_profile_api_label(spark, tmp_path):
    """All result rows carry api='lightweight'."""
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("serverless",),
        where="venv",
    )
    for r in rows:
        assert r.api == "lightweight"


# ---------------------------------------------------------------------------
# Rows > 0 assertion: no silent zero-row successes
# ---------------------------------------------------------------------------


def test_run_large_raster_profile_striped_rows_gt_zero(spark, tmp_path):
    """Striped leg records rows > 0 (bench-verify-nonzero rule)."""
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("serverless",),
        where="venv",
    )
    striped = [r for r in rows if "striped" in r.fn and "delta" not in r.fn]
    assert len(striped) == 1
    r = striped[0]
    assert r.status == "ok", f"striped leg failed: {r.note}"
    assert r.rows > 0, "striped leg recorded 0 rows — bench-verify-nonzero violation"


def test_run_large_raster_profile_cog_rows_gt_zero(spark, tmp_path):
    """Tiled-COG leg records rows > 0 (bench-verify-nonzero rule)."""
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("classic",),
        where="venv",
    )
    cog = [r for r in rows if "tiled-cog" in r.fn and "delta" not in r.fn]
    assert len(cog) == 1
    r = cog[0]
    assert r.status == "ok", f"tiled-cog leg failed: {r.note}"
    assert r.rows > 0, "tiled-cog leg recorded 0 rows — bench-verify-nonzero violation"


# ---------------------------------------------------------------------------
# Delta row: striped-vs-COG ratio is recorded
# ---------------------------------------------------------------------------


def test_run_large_raster_profile_delta_row_present(spark, tmp_path):
    """A delta row is emitted for each strategy with split_strategy set."""
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("serverless",),
        where="venv",
    )
    delta = [r for r in rows if "delta" in r.fn]
    assert len(delta) == 1
    r = delta[0]
    # split_strategy carries the strategy label; dtype holds the raster dtype.
    assert (
        r.split_strategy == "serverless"
    ), f"split_strategy should be 'serverless', got {r.split_strategy!r}"
    assert r.dtype == _DT, f"dtype should be the raster dtype '{_DT}', got {r.dtype!r}"
    # iter_median_s holds the ratio; for a well-formed run it is positive.
    assert r.iter_median_s >= 0.0


# ---------------------------------------------------------------------------
# Corpus is idempotent: second call reuses fixtures
# ---------------------------------------------------------------------------


def test_run_large_raster_profile_idempotent(spark, tmp_path):
    """Second call to run_large_raster_profile reuses existing fixture files."""
    corpus = str(tmp_path / "corpus")
    rows1 = run_large_raster_profile(
        spark,
        corpus,
        run_id="unit1",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("serverless",),
        where="venv",
    )
    # Record fixture mtimes after first run.
    strip_path = os.path.join(corpus, "striped", "large_striped.tif")
    cog_path = os.path.join(corpus, "tiled_cog", "large_cog.tif")
    mt_strip = os.path.getmtime(strip_path)
    mt_cog = os.path.getmtime(cog_path)

    # Second run — fixtures must NOT be regenerated.
    rows2 = run_large_raster_profile(
        spark,
        corpus,
        run_id="unit2",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("serverless",),
        where="venv",
    )
    assert os.path.getmtime(strip_path) == mt_strip, "striped fixture was regenerated"
    assert os.path.getmtime(cog_path) == mt_cog, "cog fixture was regenerated"
    assert len(rows2) == len(rows1)


# ---------------------------------------------------------------------------
# Corpus size is stamped in the note
# ---------------------------------------------------------------------------


def test_run_large_raster_profile_note_contains_corpus_size(spark, tmp_path):
    """The result-row note includes the decoded corpus size."""
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=("serverless",),
        where="venv",
    )
    data_rows = [r for r in rows if "delta" not in r.fn]
    for r in data_rows:
        assert "MiB" in r.note, f"corpus size not stamped in note: {r.note!r}"


# ---------------------------------------------------------------------------
# split_strategy field carries the strategy; dtype carries the raster dtype
# ---------------------------------------------------------------------------


def test_run_large_raster_profile_strategy_in_split_strategy_field(spark, tmp_path):
    """Each data row carries its splitStrategy in split_strategy, not in dtype."""
    strategies = ("none", "serverless")
    rows = run_large_raster_profile(
        spark,
        str(tmp_path / "corpus"),
        run_id="unit",
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=strategies,
        where="venv",
    )
    # Every row (data + delta) must carry the strategy in split_strategy.
    strategies_seen = {r.split_strategy for r in rows}
    for s in strategies:
        assert (
            s in strategies_seen
        ), f"strategy {s!r} not found in split_strategy field of results"
    # dtype must hold the ACTUAL raster dtype, not the strategy string.
    for r in rows:
        assert r.dtype == _DT, (
            f"dtype field must be the raster dtype '{_DT}', "
            f"not a strategy label; got {r.dtype!r} (fn={r.fn})"
        )
