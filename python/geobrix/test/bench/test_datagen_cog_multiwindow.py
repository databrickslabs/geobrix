"""Tests for generate_cog_multiwindow_corpus in bench/datagen.py."""

import json
import shutil as _shutil
from collections import defaultdict

import rasterio


def test_cog_multiwindow_manifest_shape(tmp_path):
    """Two-COG, three-windows corpus: manifest row count and path grouping are correct."""
    from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=42,
        cog_count=2,
        windows_per_cog=3,
        cog_px=256,
        bands=1,
        dtype="float32",
        srid=4326,
    )

    rows = json.loads(manifest_path.read_text())
    assert len(rows) == 6  # 2 COGs x 3 windows

    by_path = defaultdict(list)
    for r in rows:
        by_path[r["path"]].append(tuple(r["window"]))
    assert len(by_path) == 2  # two distinct COG files

    # Windows within each COG are distinct
    for path, windows in by_path.items():
        assert len(windows) == len(set(windows)), f"{path}: duplicate windows"


def test_cog_multiwindow_file_is_valid_cog(tmp_path):
    """Generated COG is internally tiled (blockxsize in profile)."""
    from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=7,
        cog_count=1,
        windows_per_cog=2,
        cog_px=256,
        bands=1,
        dtype="float32",
        srid=32618,
    )

    rows = json.loads(manifest_path.read_text())
    cog_path = tmp_path / rows[0]["path"]
    with rasterio.open(cog_path) as ds:
        assert ds.width == 256
        assert ds.count == 1
        # driver="COG" writes internally tiled blocks; check either blockxsize or is_tiled
        assert (
            ds.profile.get("blockxsize") is not None or ds.is_tiled
        ), "expected tiled COG (blockxsize missing and is_tiled=False)"


def test_cog_multiwindow_windows_are_within_bounds(tmp_path):
    """Every manifest window fits within [0, 0, cog_px, cog_px]."""
    from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

    cog_px = 256
    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=99,
        cog_count=1,
        windows_per_cog=5,
        cog_px=cog_px,
        bands=1,
        dtype="uint8",
        srid=4326,
    )

    rows = json.loads(manifest_path.read_text())
    for r in rows:
        off_x, off_y, win_w, win_h = r["window"]
        assert off_x >= 0 and off_y >= 0
        assert off_x + win_w <= cog_px
        assert off_y + win_h <= cog_px
        assert win_w > 0 and win_h > 0


def test_cog_write_goes_through_temp_copy(tmp_path, monkeypatch):
    """COG is written via a local temp file then shutil.copy to dest (FUSE-safe).

    The GDAL COG driver does backward seeks during finalization; FUSE-mounted
    Volumes (/Volumes/…) reject backward seeks with "Input/output error".  This
    test confirms that shutil.copy is invoked (write-local-then-copy pattern) and
    that the resulting file at dest is a valid, readable COG.
    """
    import databricks.labs.gbx.bench.datagen as _datagen

    copy_calls = []
    real_copy = _shutil.copy

    def spy_copy(src, dst):
        copy_calls.append((src, dst))
        return real_copy(src, dst)

    monkeypatch.setattr(_datagen.shutil, "copy", spy_copy)

    manifest_path = _datagen.generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=10,
        cog_count=1,
        windows_per_cog=2,
        cog_px=256,
        bands=1,
        dtype="uint8",
        srid=4326,
    )

    # shutil.copy must have been called exactly once (one COG written)
    assert (
        len(copy_calls) == 1
    ), f"expected shutil.copy called 1 time, got {len(copy_calls)}"
    tmp_src, final_dst = copy_calls[0]
    # The source must be a local temp file, not a /Volumes path
    assert "/Volumes" not in tmp_src, f"COG src should be local temp, got {tmp_src}"
    # The final COG at dest is readable and has the expected dimensions
    rows = json.loads(manifest_path.read_text())
    cog_path = tmp_path / rows[0]["path"]
    with rasterio.open(cog_path) as ds:
        assert ds.driver == "GTiff"
        assert ds.width == 256
        assert ds.count == 1


def test_cog_multiwindow_manifest_paths_are_absolute(tmp_path):
    """generate_cog_multiwindow_corpus writes ABSOLUTE paths in the manifest.

    Executors call rasterio.open(path) with the manifest path field directly;
    a relative path like 'cogs/cog_0.tif' fails on any executor whose CWD is
    not the corpus dir (which is always the case on Spark workers).  Paths must
    be absolute so the executor can open the file regardless of its CWD.
    """
    import json
    import os

    import rasterio

    from databricks.labs.gbx.bench.datagen import generate_cog_multiwindow_corpus

    manifest_path = generate_cog_multiwindow_corpus(
        out_dir=tmp_path,
        seed=42,
        cog_count=1,
        windows_per_cog=2,
        cog_px=256,
        bands=1,
        dtype="float32",
        srid=4326,
    )

    rows = json.loads(manifest_path.read_text())
    assert rows, "manifest must have rows"

    for row in rows:
        p = row["path"]
        assert os.path.isabs(
            p
        ), f"manifest path must be absolute so executors can open it; got: {p!r}"
        # Confirm the absolute path is actually openable (not just syntactically correct).
        with rasterio.open(p) as ds:
            assert ds.width == 256
