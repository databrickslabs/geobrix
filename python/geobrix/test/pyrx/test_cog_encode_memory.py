"""Peak-RAM regression tests for the COG encode path (serverless OOM fix).

These tests assert that encode_tile(tile_format='cog') and
analysis.cog_convert() stay within a memory-frugal envelope after the
driver='COG' fix (target ratio < 4.0x decoded size vs the old ~10x).

Each test runs the hot code in a FRESH SUBPROCESS so ru_maxrss is clean
(process-level peak, not cumulative session peak).  The subprocess script
reports the ratio on stdout; the parent asserts the bound.

COG validity (cog_validate) is asserted inline (no subprocess needed).
"""

import subprocess
import sys
import textwrap

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds._encode import encode_tile

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MiB = 1024 * 1024

# 4096x4096 float32 = 64 MiB decoded — big enough to measure, small enough for CI.
_W, _H = 4096, 4096
_DTYPE = "float32"
_ITEMSIZE = 4
_DECODED_BYTES = _W * _H * 1 * _ITEMSIZE  # 1 band


def _make_gtiff_bytes(w=_W, h=_H, dtype=_DTYPE):
    """Build a plain striped GTiff source (float32, EPSG:4326, 1 band)."""
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(0, h, 1.0, 1.0),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.random.rand(1, h, w).astype(dtype))
        return mf.read()


def _run_subprocess_encode(tile_format: str) -> float:
    """Run encode_tile in a fresh subprocess and return the ru_maxrss ratio.

    The subprocess prints ``ratio=<float>`` to stdout.  The parent parses
    that line and returns the float.  Any crash / no output raises.
    """
    script = textwrap.dedent(
        f"""
        import resource
        import sys
        import numpy as np
        import rasterio
        from rasterio.io import MemoryFile

        # Build source GTiff
        W, H, DTYPE = {_W}, {_H}, "{_DTYPE}"
        profile = dict(
            driver="GTiff", width=W, height=H, count=1, dtype=DTYPE,
            crs="EPSG:4326",
            transform=rasterio.transform.from_origin(0, H, 1.0, 1.0),
        )
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(np.random.rand(1, H, W).astype(DTYPE))
            src_bytes = mf.read()

        import gc; gc.collect()

        def rss_bytes():
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Linux: kilobytes; macOS: bytes
            return rss * 1024 if sys.platform.startswith("linux") else rss

        from databricks.labs.gbx.ds._encode import encode_tile

        baseline = rss_bytes()
        with MemoryFile(src_bytes) as cmf, cmf.open() as ds:
            _, out_bytes, _ = encode_tile(
                ds, (0, 0, W, H), "/x.tif", "",
                tile_format="{tile_format}",
                cog_blocksize=256,
            )
        peak = rss_bytes()

        DECODED = W * H * 1 * np.dtype(DTYPE).itemsize
        delta = max(0, peak - baseline)
        ratio = delta / DECODED
        print(f"ratio={{ratio:.4f}} delta={{delta/1024/1024:.1f}}MB decoded={{DECODED/1024/1024:.1f}}MB")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        pytest.fail(
            f"Subprocess failed (rc={result.returncode}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr[-1000:]}"
        )
    for line in result.stdout.splitlines():
        if line.startswith("ratio="):
            return float(line.split()[0].split("=")[1])
    pytest.fail(f"No 'ratio=...' line in subprocess output: {result.stdout!r}")


# ---------------------------------------------------------------------------
# Peak-RAM regression: COG encode must stay frugal (< 4× decoded)
# ---------------------------------------------------------------------------


def test_cog_encode_tile_peak_ram_frugal():
    """COG encode_tile RSS delta must be < 4.0× decoded size (was ~10×).

    A ratio >= 4.0 indicates regression to the old rio-cogeo in_memory path.
    256 MiB serverless budget × 2.8× target = 717 MiB, well under 1 GB cap.
    """
    ratio = _run_subprocess_encode("cog")
    assert ratio < 4.0, (
        f"COG encode_tile peak-RAM ratio is {ratio:.2f}× (target < 4.0×). "
        "This suggests regression to the old rio-cogeo in_memory path (~10×)."
    )


def test_gtiff_encode_tile_peak_ram_baseline():
    """GTiff encode_tile RSS delta should stay under 6× decoded size (sanity).

    The GTiff path has never been the OOM issue (~3.8× historically); this
    guard ensures the COG fix does not inadvertently inflate the GTiff path.
    """
    ratio = _run_subprocess_encode("gtiff")
    assert (
        ratio < 6.0
    ), f"GTiff encode_tile peak-RAM ratio is {ratio:.2f}× (expected < 6.0×)."


# ---------------------------------------------------------------------------
# COG validity: driver='COG' output must pass cog_validate (spec-compliant)
# ---------------------------------------------------------------------------


def test_cog_convert_output_is_spec_valid():
    """cog_convert() output must pass rio_cogeo.cog_validate (IFD order correct).

    The old build_overviews (v4) approach passed sniff_header but FAILED
    cog_validate because overviews were written after the main image IFD.
    driver='COG' (v5) orders IFDs correctly by construction.
    """
    try:
        from rio_cogeo.cogeo import cog_validate
    except ImportError:
        pytest.skip("rio-cogeo not installed")

    from databricks.labs.gbx.pyrx.core.analysis import cog_convert

    src = _make_gtiff_bytes(w=512, h=512)
    with MemoryFile(src) as mf, mf.open() as ds:
        cog_bytes = cog_convert(ds, "DEFLATE", 256, "AVERAGE")

    with MemoryFile(cog_bytes) as mf2:
        is_valid, errors, warnings = cog_validate(mf2.name, strict=True)

    assert is_valid, (
        f"cog_convert output is NOT spec-valid. errors={errors}. "
        "This means the IFD ordering is wrong (likely build_overviews regression, "
        "not driver='COG' path)."
    )


def test_encode_tile_cog_output_is_spec_valid():
    """encode_tile(tile_format='cog') bytes must also pass cog_validate."""
    try:
        from rio_cogeo.cogeo import cog_validate
    except ImportError:
        pytest.skip("rio-cogeo not installed")

    src = _make_gtiff_bytes(w=512, h=512)
    with MemoryFile(src) as cmf, cmf.open() as ds:
        _, cog_bytes, md = encode_tile(
            ds,
            (0, 0, 512, 512),
            "/x.tif",
            "",
            tile_format="cog",
            cog_blocksize=256,
        )

    with MemoryFile(cog_bytes) as mf2:
        is_valid, errors, warnings = cog_validate(mf2.name, strict=True)

    assert is_valid, f"encode_tile(cog) output is NOT spec-valid. errors={errors}"
    # Metadata stamp also agrees
    from databricks.labs.gbx.pyrx.core.cog import GBX_FORMAT

    assert md[GBX_FORMAT] == "cog"
