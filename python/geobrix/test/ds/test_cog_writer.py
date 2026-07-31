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
_MEMORY_PROBE = textwrap.dedent("""\
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
    """)

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


def test_writer_reads_source_directly_no_heap_copy(tmp_path, monkeypatch):
    """write() must pass the source path DIRECTLY to cog_convert_file — GDAL
    reads it natively block-by-block, so the whole source never passes through
    the Python heap (the only in-worker path that avoids OOM on large sources).

    Regression guard for the reverted dbutils/copyfileobj staging: assert
    cog_convert_file receives the exact source path, not a staged copy."""
    import databricks.labs.gbx.ds.cog_writer as _cw_mod

    seen_src = []
    from databricks.labs.gbx.pyrx.core import analysis as _analysis

    real_convert = _analysis.cog_convert_file

    def _spy_convert(src, dst, **kwargs):
        seen_src.append(src)
        return real_convert(src, dst, **kwargs)

    monkeypatch.setattr(_cw_mod, "cog_convert_file", _spy_convert, raising=False)
    monkeypatch.setattr(_analysis, "cog_convert_file", _spy_convert)

    src = tmp_path / "in" / "direct.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    msg = w.write(iter([{"path": str(src)}]))

    assert len(seen_src) == 1, "cog_convert_file should be called once"
    # The exact source path is passed through — no staged/temp copy in between.
    assert seen_src[0] == str(src), (
        f"cog_convert_file got {seen_src[0]!r}, expected the source path {str(src)!r} "
        "(a staged copy would indicate a Python-heap read of the whole source)"
    )
    assert len(msg.paths) == 1 and os.path.exists(msg.paths[0])
    with open(msg.paths[0], "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1


def test_writer_uses_copyfile_not_copy(tmp_path, monkeypatch):
    """Regression guard: write() must not call shutil.copy (which invokes chmod).
    UC Volume FUSE rejects chmod with PermissionError. Simulate this by patching
    os.chmod to raise — copyfile never calls chmod, so the write succeeds."""

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


def test_writer_skip_if_exists_default_skips(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    (out / "scene.tif").write_bytes(b"sentinel")  # ext default "tif" → <stem>.tif
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(
        str(out), schema, overwrite=False, cog_blocksize=256, cog_skip_if_exists=True
    )
    w.write(iter([{"path": str(src)}]))
    # untouched sentinel — skipped, not reconverted
    assert (out / "scene.tif").read_bytes() == b"sentinel"


def test_writer_skip_if_exists_false_reconverts(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    (out / "scene.tif").write_bytes(b"sentinel")
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(
        str(out), schema, overwrite=False, cog_blocksize=256, cog_skip_if_exists=False
    )
    w.write(iter([{"path": str(src)}]))
    with open(out / "scene.tif", "rb") as fh:
        assert gbxcog.sniff_header(fh.read()).is_cog is True  # real COG now


def test_driver_mode_write_gathers_paths_no_conversion(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(
        str(out), schema, overwrite=True, cog_blocksize=256, driver_mode=True
    )
    msg = w.write(iter([{"path": str(src)}]))
    # write() gathered the source path, no conversion on executor
    assert list(msg.paths) == [str(src)]
    assert not glob.glob(os.path.join(str(out), "*"))


def test_driver_mode_commit_prepares_cogs(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(
        str(out),
        schema,
        overwrite=True,
        cog_blocksize=256,
        driver_mode=True,
        driver_mode_verbose=False,
    )
    msg = w.write(iter([{"path": str(src)}]))
    w.commit([msg])
    produced = glob.glob(os.path.join(str(out), "*.cog"))
    assert len(produced) == 1
    with open(produced[0], "rb") as fh:
        assert gbxcog.sniff_header(fh.read()).is_cog is True
    assert os.path.basename(produced[0]) == "scene.tif.cog"  # source naming


def test_default_mode_unchanged(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(
        str(out), schema, overwrite=True, cog_blocksize=256
    )  # driver_mode default False
    w.write(iter([{"path": str(src)}]))
    # default path converts in write() → <stem>.tif exists
    assert glob.glob(os.path.join(str(out), "*.tif"))


# ---------------------------------------------------------------------------
# abort() regression — driverMode must NOT delete source files
# ---------------------------------------------------------------------------


def test_driver_mode_abort_does_not_delete_sources(tmp_path):
    """Regression guard: abort() in driverMode must leave source files untouched.

    In driverMode, write() stores SOURCE paths in CogCommitMessage.paths (no
    conversion on executor).  If the unconditional os.remove() path runs on
    abort, it deletes the user's original input rasters.  The guard returns
    early on abort when driver_mode=True so sources survive.
    """
    # Create two source files that must survive abort().
    src_a = tmp_path / "in" / "a.tif"
    src_b = tmp_path / "in" / "b.tif"
    src_a.parent.mkdir(parents=True, exist_ok=True)
    _write_src(str(src_a))
    _write_src(str(src_b))

    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(
        str(out), schema, overwrite=True, cog_blocksize=256, driver_mode=True
    )

    # Simulate what Spark does: write() gathers source paths, then abort() fires.
    msg_a = w.write(iter([{"path": str(src_a)}]))
    msg_b = w.write(iter([{"path": str(src_b)}]))

    # Sanity: paths in the messages are the SOURCE files, not outputs.
    assert msg_a.paths == [str(src_a)]
    assert msg_b.paths == [str(src_b)]

    # abort() must NOT remove them.
    w.abort([msg_a, msg_b])

    assert src_a.exists(), "abort() deleted source file a.tif — data-loss bug"
    assert src_b.exists(), "abort() deleted source file b.tif — data-loss bug"


def test_default_mode_abort_removes_outputs(tmp_path):
    """Confirm existing default-mode abort behavior is unchanged.

    In default (non-driverMode) mode, CogCommitMessage.paths holds OUTPUT .tif
    files produced by write().  abort() should remove them (partial outputs).
    """
    from databricks.labs.gbx.ds.cog_writer import CogCommitMessage

    out = tmp_path / "out"
    out.mkdir(parents=True, exist_ok=True)
    # Simulate two output files that a failed write() would have produced.
    out_a = out / "a.tif"
    out_b = out / "b.tif"
    out_a.write_bytes(b"partial-a")
    out_b.write_bytes(b"partial-b")

    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=False, cog_blocksize=256)
    # driver_mode is False (default) — abort() should delete the listed outputs.
    msg = CogCommitMessage(paths=[str(out_a), str(out_b)])
    w.abort([msg])

    assert not out_a.exists(), "default-mode abort() should remove partial output a.tif"
    assert not out_b.exists(), "default-mode abort() should remove partial output b.tif"
