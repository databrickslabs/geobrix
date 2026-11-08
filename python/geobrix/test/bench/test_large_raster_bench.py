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


def test_write_striped_gtiff_writes_locally_then_copies(tmp_path, monkeypatch):
    """The fixture can land on a Volume: GDAL must write a local file, never the destination."""
    from databricks.labs.gbx.bench import readers

    dest = tmp_path / "vol" / "strip.tif"
    dest.parent.mkdir()
    written = []
    real = readers.open_data

    def spy(path, mode="r", **kw):
        if mode == "w":
            written.append(os.fspath(path))
        return real(path, mode, **kw)

    monkeypatch.setattr(readers, "open_data", spy)
    _write_striped_gtiff(str(dest), _W, _H, _B, _DT)
    assert written and all(w != str(dest) for w in written), written
    assert [p.name for p in dest.parent.iterdir()] == ["strip.tif"]
    with rasterio.open(dest) as ds:
        assert (ds.width, ds.height) == (_W, _H)


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


def _profile(spark, corpus, strategies, run_id="unit"):
    return run_large_raster_profile(
        spark,
        str(corpus),
        run_id=run_id,
        warmup=0,
        measured=1,
        width=_W,
        height=_H,
        bands=_B,
        dtype=_DT,
        split_strategies=strategies,
        where="venv",
    )


def test_run_large_raster_profile_rows(spark, tmp_path):
    """Two strategies give a striped, a tiled-COG and a delta row each. Every row is a lightweight large_raster row
    carrying its strategy in split_strategy and the raster dtype in dtype; data rows read rows > 0 (no silent zero-row
    success), stamp the corpus size in the note, and the delta row holds the ratio."""
    strategies = ("none", "serverless")
    rows = _profile(spark, tmp_path / "corpus", strategies)
    assert len(rows) == 2 * 2 + 2
    assert {(r.category, r.api, r.dtype) for r in rows} == {
        ("large_raster", "lightweight", _DT)
    }
    assert {r.split_strategy for r in rows} == set(strategies)
    data = [r for r in rows if "delta" not in r.fn]
    assert {("striped" in r.fn, "tiled-cog" in r.fn) for r in data} == {
        (True, False),
        (False, True),
    }
    for r in data:
        assert r.status == "ok", (r.fn, r.note)
        assert r.rows > 0, f"{r.fn} recorded 0 rows"
        assert "MiB" in r.note, r.note
    deltas = [r for r in rows if "delta" in r.fn]
    assert sorted(r.split_strategy for r in deltas) == sorted(strategies)
    assert all(r.iter_median_s >= 0.0 for r in deltas)


def test_run_large_raster_profile_idempotent(spark, tmp_path):
    """A second call reuses the existing fixture files."""
    corpus = tmp_path / "corpus"
    rows1 = _profile(spark, corpus, ("serverless",), "unit1")
    paths = [
        corpus / "striped" / "large_striped.tif",
        corpus / "tiled_cog" / "large_cog.tif",
    ]
    before = [p.stat().st_mtime for p in paths]
    rows2 = _profile(spark, corpus, ("serverless",), "unit2")
    assert [p.stat().st_mtime for p in paths] == before, "a fixture was regenerated"
    assert len(rows2) == len(rows1)
