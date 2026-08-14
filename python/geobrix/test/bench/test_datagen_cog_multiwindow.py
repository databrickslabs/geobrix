"""Tests for generate_cog_multiwindow_corpus in bench/datagen.py."""

import json
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
        assert ds.profile.get("blockxsize") is not None or ds.is_tiled, (
            "expected tiled COG (blockxsize missing and is_tiled=False)"
        )


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
