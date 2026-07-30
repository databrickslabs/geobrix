import glob
import json
import os
import subprocess
import sys
import textwrap

import numpy as np
import rasterio
from pyspark.sql.types import StringType, StructField, StructType
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, assert_path_schema
from databricks.labs.gbx.pyrx.core import cog as gbxcog


def _write_src(path, w=512, h=512):
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.arange(w * h, dtype="uint8").reshape(1, h, w))


# ---------------------------------------------------------------------------
# Memory regression guard — subprocess probe (catches local-vs-serverless gap)
# ---------------------------------------------------------------------------

# Probe script: writes a 8192×8192 float32 GTiff (~0.25 GiB decoded), runs
# cog_convert_file on it, and reports peak RSS via ru_maxrss.
_MEMORY_PROBE = textwrap.dedent(
    """\
    import os, sys, resource, json, tempfile
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    def _rss_mib():
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return rss / (1024 * 1024)
        return rss / 1024  # KB -> MiB

    side = int(sys.argv[1])
    work_dir = sys.argv[2]

    # Write a striped GTiff (NOT internally tiled) — worst case for array decode.
    src = os.path.join(work_dir, "large.tif")
    profile = dict(driver="GTiff", width=side, height=side, count=1,
                   dtype="float32", crs="EPSG:4326",
                   transform=from_origin(0, 60, 0.001, 0.001))
    with rasterio.open(src, "w", **profile) as ds:
        ds.write(np.zeros((1, side, side), dtype="float32"))

    before = _rss_mib()
    dst = os.path.join(work_dir, "out.tif")
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file
    cog_convert_file(src, dst, compression="DEFLATE", blocksize=512,
                     overview_resampling="AVERAGE")
    after = _rss_mib()
    delta = after - before

    print(json.dumps({"delta_mib": delta, "peak_rss_mib": after}))
    """
)

_RSS_LIMIT_MIB = 250


def _run_memory_probe(work_dir: str, side: int) -> dict:
    probe_path = os.path.join(work_dir, "_cog_memory_probe.py")
    with open(probe_path, "w") as fh:
        fh.write(_MEMORY_PROBE)
    result = subprocess.run(
        [sys.executable, probe_path, str(side), work_dir],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Memory probe failed (rc={result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    for line in reversed(result.stdout.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise ValueError(f"No JSON in probe output:\n{result.stdout}")


def test_cog_convert_file_peak_rss_bounded(tmp_path):
    """Streaming cog_convert_file must not decode the whole raster into RAM.

    A 8192×8192 float32 raster = ~0.25 GiB decoded.  The array-based path
    (cog_convert) would consume ≥256 MiB just for ds.read().  The streaming
    path (rasterio.shutil.copy + GDAL_CACHEMAX=200) should stay well under
    that ceiling.  Threshold: {rss_limit} MiB RSS delta in a fresh subprocess.
    """.format(rss_limit=_RSS_LIMIT_MIB)
    result = _run_memory_probe(str(tmp_path), side=8192)
    delta = result["delta_mib"]
    assert delta < _RSS_LIMIT_MIB, (
        f"cog_convert_file RSS delta {delta:.1f} MiB exceeds {_RSS_LIMIT_MIB} MiB "
        f"— whole-raster decode path may have been used (peak_rss={result['peak_rss_mib']:.1f} MiB)"
    )


def test_writer_retries_transient_stage_failure(tmp_path, monkeypatch):
    """write() must succeed when the source open raises FileNotFoundError transiently.
    Simulates UC Volume FUSE eventual-consistency via _get_or_stage_file retry."""
    import databricks.labs.gbx.ds._listing as _listing_mod
    import builtins

    src = tmp_path / "in" / "transient.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])

    real_open = builtins.open
    open_calls = [0]

    def _flaky_open(path, mode="r", *args, **kwargs):
        # Fail the first open of the source file in binary read mode only.
        if str(path) == str(src) and "b" in str(mode) and open_calls[0] == 0:
            open_calls[0] += 1
            raise FileNotFoundError("transient FUSE miss")
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(_listing_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(builtins, "open", _flaky_open)

    # Clear any cached staging state so this test gets a fresh stage attempt.
    import databricks.labs.gbx.ds.raster as _raster_mod
    _raster_mod._STAGED_FILES.clear()

    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    row = {"path": str(src)}
    msg = w.write(iter([row]))
    assert len(msg.paths) == 1 and os.path.exists(msg.paths[0])


def test_writer_stages_source_before_converting(tmp_path, monkeypatch):
    """Regression guard: write() must stage the source to local disk before
    calling cog_convert_file. GDAL's rasterio.shutil.copy needs random seeks
    on the source; a /Volumes FUSE path can't serve seeks efficiently (buffers
    the whole file → OOM on large sources). Verify cog_convert_file receives a
    path that differs from the original source path — i.e. a staged local copy."""
    import databricks.labs.gbx.ds.cog_writer as _cw_mod

    # Capture the real function BEFORE patching to avoid recursion in the spy.
    import databricks.labs.gbx.pyrx.core.analysis as _analysis_mod
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file as _real_convert

    captured_src = []

    def _fake_convert(src_path, dst_path, **kwargs):
        captured_src.append(src_path)
        # Call the real conversion so the output COG is valid.
        _real_convert(src_path, dst_path, **kwargs)

    # Patch inside write()'s import scope by monkeypatching the analysis module.
    monkeypatch.setattr(_analysis_mod, "cog_convert_file", _fake_convert)

    src = tmp_path / "in" / "staged.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    row = {"path": str(src)}
    w.write(iter([row]))

    assert len(captured_src) == 1, "cog_convert_file should have been called once"
    # The path passed to cog_convert_file must NOT be the original source path —
    # it must be the staged (worker-local) copy.
    assert captured_src[0] != str(src), (
        f"cog_convert_file was called with the original source path {src!r}; "
        "expected a worker-local staged copy"
    )
    assert os.path.exists(captured_src[0]), "staged path must exist on disk"


def test_writer_uses_copyfile_not_copy(tmp_path, monkeypatch):
    """Regression guard: write() must not call shutil.copy (which invokes chmod).
    UC Volume FUSE rejects chmod with PermissionError. Simulate this by patching
    os.chmod to raise — copyfile never calls chmod, so the write succeeds."""
    import shutil as _shutil

    def _no_chmod(path, mode, **kwargs):
        raise PermissionError(f"chmod not permitted on FUSE: {path}")

    monkeypatch.setattr(os, "chmod", _no_chmod)

    src = tmp_path / "in" / "small.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    row = {"path": str(src)}
    msg = w.write(iter([row]))
    assert len(msg.paths) == 1 and os.path.exists(msg.paths[0])


def test_assert_path_schema_requires_path():
    ok = StructType([StructField("path", StringType(), False)])
    assert_path_schema(ok)  # no raise
    bad = StructType([StructField("name", StringType(), False)])
    try:
        assert_path_schema(bad)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_writer_prepares_valid_cog(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType(
        [
            StructField("path", StringType(), False),
            StructField("name", StringType(), False),
        ]
    )
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    # rows are plain dicts (works like a Spark Row via subscript)
    row = {"path": str(src), "name": "scene.tif"}
    w.write(iter([row]))
    produced = glob.glob(os.path.join(str(out), "*.tif"))
    assert len(produced) == 1
    with open(produced[0], "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1
