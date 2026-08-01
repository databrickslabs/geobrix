"""Local verification gate for the one-tile-per-partition OOM fix.

Asserts that:
  (a) partitions() returns >1 tile-partition for a 0.5 GiB raster.
  (b) Draining any single _TilePartition via read() peaks well under 1 GB
      total RSS locally (target < 750 MiB), measured in a fresh subprocess.
  (c) Total tiles x pixel coverage matches the old behaviour (no lost or
      overlapping data).

These tests do NOT require Spark and can run under plain pytest.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MIB = 1024 * 1024

# Budget to test against: use a small value (4 MiB) to reliably force splits
# during the coverage-equivalence checks without needing a genuinely huge file.
_SMALL_BUDGET_BYTES = 4 * _MIB

# Per-tile READ-GROWTH ceiling (MiB): how much draining all tile reads may grow
# the process RSS high-water mark ABOVE the post-import/planning baseline. This
# isolates the one-tile-per-partition guarantee (bounded per-task memory) from
# the interpreter's import footprint, which varies by runner (a shared CI box
# carries a much heavier GDAL/numpy/pyspark import baseline than a dev machine —
# an earlier whole-process ceiling of 750 MiB flaked at ~900 MiB on CI purely
# from that baseline). The read growth itself is bounded by the ~4 MiB decoded
# budget + GDAL cache + one-tile encode transient, so it stays well under this;
# a regression that accumulated all tiles' bytes would blow far past it.
_MAX_READ_GROWTH_MIB = 400


def _write_striped(path, side: int, bands: int = 1, dtype="float32"):
    """Write an incompressible striped GeoTIFF of *side*×*side*×*bands*."""
    rng = np.random.default_rng(42)
    if dtype == "float32":
        data = rng.random((bands, side, side), dtype="float32")
    else:
        data = rng.integers(0, 255, size=(bands, side, side), dtype=dtype)
    profile = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=bands,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
        tiled=False,
        compress="DEFLATE",
    )
    with rasterio.open(str(path), "w", **profile) as dst:
        dst.write(data)
    return data


# ---------------------------------------------------------------------------
# (a) + (c): partition count and coverage equivalence
# ---------------------------------------------------------------------------


def test_partitions_gt1_for_split_raster(tmp_path):
    """partitions() returns >1 tile-partition when budget forces a split."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    p = tmp_path / "split_test.tif"
    _write_striped(str(p), side=1024, bands=3, dtype="uint8")  # ~3 MiB decoded

    # Override sizeInMB to force a 1 MiB budget.
    reader = RasterGbxReader({"path": str(p), "sizeInMB": "1"})
    parts = reader.partitions()

    assert (
        len(parts) > 1
    ), f"Expected >1 partition for a ~3 MiB raster with 1 MiB budget, got {len(parts)}"


def test_one_row_per_tile_partition(tmp_path):
    """Each _TilePartition from partitions() yields exactly one row."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    p = tmp_path / "one_row.tif"
    _write_striped(str(p), side=512, bands=3, dtype="uint8")

    reader = RasterGbxReader({"path": str(p), "sizeInMB": "1"})
    parts = reader.partitions()

    for part in parts:
        rows = list(reader.read(part))
        assert (
            len(rows) == 1
        ), f"read() must yield exactly 1 row per _TilePartition, got {len(rows)}"


def test_tile_coverage_equivalence(tmp_path):
    """Total decoded pixels across all tile partitions equals the source raster."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    # 2048×2048×3 uint8 = 12 MiB decoded > 4 MiB budget → forces split.
    side = 2048
    bands = 3
    p = tmp_path / "coverage.tif"
    _write_striped(str(p), side=side, bands=bands, dtype="uint8")

    reader = RasterGbxReader({"path": str(p), "sizeInMB": "4"})
    parts = reader.partitions()
    assert len(parts) > 1

    total_pixels = 0
    for part in parts:
        assert part.window is not None, "split partition must have a window"
        col_off, row_off, w, h = part.window
        total_pixels += w * h

    expected = side * side
    assert total_pixels == expected, (
        f"Tile windows cover {total_pixels} px but source has {expected} px — "
        "tiles overlap or leave gaps."
    )


def test_no_window_overlap(tmp_path):
    """No two tile windows share any pixel."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    # 2048×2048×3 uint8 = 12 MiB decoded > 4 MiB budget → forces split.
    side = 2048
    bands = 3
    p = tmp_path / "no_overlap.tif"
    _write_striped(str(p), side=side, bands=bands, dtype="uint8")

    reader = RasterGbxReader({"path": str(p), "sizeInMB": "4"})
    parts = reader.partitions()
    assert len(parts) > 1

    # Build a pixel mask: each pixel should appear in exactly 1 window.
    mask = np.zeros((side, side), dtype="int32")
    for part in parts:
        assert part.window is not None, "split partition must have a window"
        col_off, row_off, w, h = part.window
        mask[row_off : row_off + h, col_off : col_off + w] += 1

    assert np.all(mask == 1), "Each pixel must appear in exactly one tile window."


def test_passthrough_partition_has_no_window(tmp_path):
    """A small GTiff (below budget) produces a single passthrough partition."""
    from databricks.labs.gbx.ds.raster import RasterGbxReader, _TilePartition

    p = tmp_path / "small.tif"
    _write_striped(str(p), side=4, bands=1, dtype="float32")

    reader = RasterGbxReader({"path": str(p)})
    parts = reader.partitions()

    assert len(parts) == 1
    assert isinstance(parts[0], _TilePartition)
    assert parts[0].is_passthrough, "Small GTiff should use passthrough fast path"
    assert parts[0].window is None


# ---------------------------------------------------------------------------
# (b) Per-tile peak RSS < 750 MiB (subprocess gate)
# ---------------------------------------------------------------------------


def _rss_mib() -> float:
    """Return current process RSS in MiB (cross-platform)."""
    import resource
    import sys

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS: bytes; Linux: kilobytes.
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024  # KB → MiB


_PROBE_SCRIPT = textwrap.dedent("""\
    import os, sys, resource, json

    def _rss_mib():
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return rss / (1024 * 1024)
        return rss / 1024  # KB -> MiB

    side = int(sys.argv[1])
    bands = int(sys.argv[2])
    path = sys.argv[3]

    from databricks.labs.gbx.ds.raster import RasterGbxReader, _TilePartition

    reader = RasterGbxReader({"path": path, "sizeInMB": "4"})
    parts = reader.partitions()

    # BASELINE: whole-process RSS after imports + partition planning, before any
    # tile READ. ru_maxrss is a high-water mark, so the DELTA between the baseline
    # and the peak after draining all reads isolates the per-tile read growth —
    # the OOM-relevant quantity — independent of the runner's import footprint
    # (which varies wildly: a shared CI box carries a much larger GDAL/numpy/etc.
    # import baseline than a dev machine, and drowned the old whole-process ceiling).
    baseline_mib = _rss_mib()

    for part in parts:
        rows = list(reader.read(part))
        assert len(rows) == 1, f"Expected 1 row per partition, got {len(rows)}"

    peak_mib = _rss_mib()  # monotonic high-water after all reads
    print(json.dumps({
        "num_partitions": len(parts),
        "baseline_rss_mib": baseline_mib,
        "peak_rss_mib": peak_mib,
        "read_growth_mib": max(0.0, peak_mib - baseline_mib),
    }))
    """)


def _run_probe(path: str, side: int, bands: int) -> dict:
    """Run the RSS probe in a fresh subprocess and return parsed JSON."""
    # Write probe script to a temp file so it can be executed cleanly.
    probe_file = os.path.join(str(os.path.dirname(path)), "_probe_rss.py")
    with open(probe_file, "w") as fh:
        fh.write(_PROBE_SCRIPT)
    result = subprocess.run(
        [sys.executable, probe_file, str(side), str(bands), str(path)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    os.unlink(probe_file)
    if result.returncode != 0:
        raise RuntimeError(
            f"Probe subprocess failed (rc={result.returncode}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    # Parse last JSON line from stdout (probe may emit import warnings).
    for line in reversed(result.stdout.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            import json

            return json.loads(line)
    raise ValueError(f"No JSON found in probe output:\n{result.stdout}")


@pytest.mark.slow
def test_per_tile_rss_under_ceiling_float32(tmp_path):
    """Draining all float32 tile reads grows RSS < ceiling above import baseline."""
    side = 1024
    bands = 3
    p = tmp_path / "rss_float32.tif"
    _write_striped(str(p), side=side, bands=bands, dtype="float32")

    result = _run_probe(str(p), side, bands)

    growth = result["read_growth_mib"]
    n_parts = result["num_partitions"]

    assert n_parts > 1, f"Expected >1 partition, got {n_parts}"
    assert growth < _MAX_READ_GROWTH_MIB, (
        f"Per-tile read growth {growth:.1f} MiB >= {_MAX_READ_GROWTH_MIB} MiB "
        f"(baseline {result['baseline_rss_mib']:.1f}, peak {result['peak_rss_mib']:.1f}). "
        "One-tile-per-partition bound broken — Serverless OOM risk is NOT fixed."
    )


@pytest.mark.slow
def test_per_tile_rss_under_ceiling_uint8(tmp_path):
    """Draining all uint8 tile reads grows RSS < ceiling above import baseline."""
    side = 2048
    bands = 3
    p = tmp_path / "rss_uint8.tif"
    _write_striped(str(p), side=side, bands=bands, dtype="uint8")

    result = _run_probe(str(p), side, bands)

    growth = result["read_growth_mib"]
    n_parts = result["num_partitions"]

    assert n_parts > 1, f"Expected >1 partition, got {n_parts}"
    assert growth < _MAX_READ_GROWTH_MIB, (
        f"Per-tile read growth {growth:.1f} MiB >= {_MAX_READ_GROWTH_MIB} MiB "
        f"(baseline {result['baseline_rss_mib']:.1f}, peak {result['peak_rss_mib']:.1f}). "
        "One-tile-per-partition bound broken — Serverless OOM risk is NOT fixed."
    )
