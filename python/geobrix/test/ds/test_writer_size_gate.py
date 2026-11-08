"""Writer size gates: a source over the executor cap is not read whole.

RasterGbxWriter: small tiles materialise fully; large tiles stream block-wise.
CogGbxWriter: a source over the cap is converted driver-side, one over the driver bound is refused.
The decision itself (``materialize_decision``) is tested once in test/pyrx/test_materialize_decision.py;
here are the writer-side routes it drives and, for the COG kind, its driver bound.


Unit tests (no Spark, no JAR, no /Volumes data required):

1. ``test_windowed_pixels_profile_equiv`` — _windowed_materialize_bytes is pixel-array-equal
   AND profile-equal to materialize_to_bytes on the same no-warp/no-clip virtual tile.
2. ``test_small_tile_uses_materialize_path`` — with GBX_STREAM_MAX_BYTES unset (default 256 MiB
   cap), a tile whose path_file_size is well below the cap routes to materialize_to_bytes.
3. ``test_large_tile_uses_windowed_path`` — with GBX_STREAM_MAX_BYTES=1 (forces "fuse" for any
   file), the writer routes to _windowed_materialize_bytes; materialize_to_bytes is NOT called.
4. ``test_large_tile_round_trip`` — write via the large branch then read back; pixels match.
5. ``test_large_tile_with_pending_bands`` — windowed path correctly applies band-select.
6. ``test_large_tile_subwindow`` — windowed path reads the correct sub-window of the source.
"""

from __future__ import annotations

import logging
import os
from typing import Iterator
from unittest.mock import patch

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

import databricks.labs.gbx.ds.cog_writer as _cw_mod
import databricks.labs.gbx.ds.file_gbx as _fgbx
from databricks.labs.gbx.ds.cog_writer import CogCommitMessage, CogGbxWriter
from databricks.labs.gbx.ds.file_gbx import StageTooLargeError, materialize_decision
from databricks.labs.gbx.ds.raster import reader_schema_v2
from databricks.labs.gbx.ds.writer import RasterGbxWriter
from databricks.labs.gbx.pyrx.core.open_tile import (
    _windowed_materialize_bytes,
    materialize_to_bytes,
)
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_geotiff(path: str, width: int = 8, height: int = 6, count: int = 1) -> None:
    """Write a small Float32 GTiff to *path* with known pixels."""
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(path, "w", **profile) as ds:
        for b in range(1, count + 1):
            ds.write(data + (b - 1) * 100.0, b)


def _write_compressible_geotiff(
    path: str, width: int, height: int, count: int = 1, dtype: str = "uint16"
) -> "tuple[int, int]":
    """Write a large all-zeros (highly-compressible) raster.

    Returns ``(compressed_on_disk_bytes, decoded_bytes)`` — the compressed file is
    tiny while the decoded array (count*width*height*itemsize) is large, so a gate
    that keys on compressed size would wrongly under-estimate RAM.
    """
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        compress="deflate",
    )
    data = np.zeros((count, height, width), dtype=dtype)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)
    decoded = count * width * height * int(np.dtype(dtype).itemsize)
    return os.path.getsize(path), decoded


def _virtual_tile(path: str, window=None, metadata: dict | None = None) -> VirtualTile:
    """Build a whole-file (or windowed) virtual tile pointing at *path*."""
    file_size = os.path.getsize(path)
    meta = {"path_file_size": str(file_size)}
    if metadata:
        meta.update(metadata)
    return VirtualTile(cellid=1, path=path, window=window, metadata=meta)


def _row(vt: VirtualTile) -> dict:
    """Pack *vt* into a row dict the writer iterator expects."""
    return {"tile": vt.to_row(), "source": vt.path or ""}


def _write_one(out_dir: str, vt: VirtualTile) -> str:
    """Write a single virtual tile via RasterGbxWriter; return the output path."""
    schema = reader_schema_v2()
    writer = RasterGbxWriter(out_dir, schema, overwrite=True)

    def _iter() -> Iterator[dict]:
        yield _row(vt)

    writer.write(_iter())

    written = sorted(f for f in os.listdir(out_dir) if f.endswith(".tif"))
    assert len(written) == 1, f"expected one .tif output, got {written}"
    return os.path.join(out_dir, written[0])


def _profile_key(ds) -> dict:
    """Extract the profile keys relevant to equivalence checks.

    Includes the STRUCTURAL layout fields (tiled / blockxsize / blockysize /
    interleave) in addition to georeferencing/pixel-shape fields.  Empirically
    (see the Fix-4 determination in the serverless-safe-materialize-policy fix
    round) ``_windowed_materialize_bytes`` and ``materialize_to_bytes`` produce
    IDENTICAL values for these four fields across striped/tiled sources, single-
    and multi-band, small and large — because both build the profile from the
    same source ``.profile.copy()`` and neither's compression authority
    (``creation_opts``) touches tiling/interleave.  Asserting them here therefore
    strengthens the equivalence guarantee rather than being an over-claim.
    """
    p = ds.profile
    return {
        "width": ds.width,
        "height": ds.height,
        "count": ds.count,
        "dtype": ds.dtypes[0],
        "crs": str(ds.crs) if ds.crs else None,
        "nodata": ds.nodata,
        "transform": ds.transform,
        "tiled": p.get("tiled"),
        "blockxsize": p.get("blockxsize"),
        "blockysize": p.get("blockysize"),
        "interleave": p.get("interleave"),
    }


# ---------------------------------------------------------------------------
# 1. Pixel + profile equivalence between windowed and full-materialize paths
# ---------------------------------------------------------------------------


def test_windowed_pixels_profile_equiv(tmp_path):
    """_windowed_materialize_bytes output is pixel-array-equal and profile-equal
    to materialize_to_bytes for a no-warp / no-clip virtual tile."""
    src = str(tmp_path / "in.tif")
    _write_geotiff(src)

    vt = _virtual_tile(src)

    # Reference: full-materialize path.
    ref_bytes = materialize_to_bytes(vt).raster
    with MemoryFile(ref_bytes) as mf:
        with mf.open() as ref_ds:
            ref_arr = ref_ds.read()
            ref_prof = _profile_key(ref_ds)

    # Test: windowed-streaming path.
    win_bytes = _windowed_materialize_bytes(vt)
    with MemoryFile(win_bytes) as mf:
        with mf.open() as win_ds:
            win_arr = win_ds.read()
            win_prof = _profile_key(win_ds)

    np.testing.assert_array_equal(win_arr, ref_arr, err_msg="pixel arrays differ")
    assert (
        win_prof == ref_prof
    ), f"profile mismatch:\n  ref={ref_prof}\n  win={win_prof}"


# ---------------------------------------------------------------------------
# 2. Small tile → materialize_to_bytes path taken
# ---------------------------------------------------------------------------


def test_small_tile_uses_materialize_path(tmp_path):
    """With default cap (256 MiB), a small tile routes to materialize_to_bytes."""
    src = str(tmp_path / "in.tif")
    _write_geotiff(src)
    out_dir = str(tmp_path / "out_small")
    os.makedirs(out_dir)

    vt = _virtual_tile(src)  # path_file_size = real file size (a few hundred bytes)

    schema = reader_schema_v2()
    writer = RasterGbxWriter(out_dir, schema, overwrite=True)

    from databricks.labs.gbx.pyrx.core.open_tile import materialize_to_bytes as _mtb

    with patch(
        "databricks.labs.gbx.pyrx.core.open_tile.materialize_to_bytes",
        wraps=_mtb,
    ) as spy:

        def _iter() -> Iterator[dict]:
            yield _row(vt)

        writer.write(_iter())
        assert spy.call_count == 1, (
            f"expected materialize_to_bytes called once for small tile, "
            f"got call_count={spy.call_count}"
        )


# ---------------------------------------------------------------------------
# 3. Large tile → windowed path taken; materialize_to_bytes NOT called
# ---------------------------------------------------------------------------


def test_large_tile_uses_windowed_path(tmp_path, monkeypatch):
    """With GBX_STREAM_MAX_BYTES=1, any file triggers the windowed path;
    materialize_to_bytes must NOT be called."""
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

    src = str(tmp_path / "in.tif")
    _write_geotiff(src)
    out_dir = str(tmp_path / "out_large")
    os.makedirs(out_dir)

    vt = _virtual_tile(src)  # path_file_size > 1 byte → "fuse" decision

    schema = reader_schema_v2()
    writer = RasterGbxWriter(out_dir, schema, overwrite=True)

    with patch(
        "databricks.labs.gbx.pyrx.core.open_tile.materialize_to_bytes"
    ) as mock_mat:
        # _windowed_materialize_bytes must not call materialize_to_bytes in the
        # no-warp / no-clip branch.
        mock_mat.side_effect = AssertionError(
            "materialize_to_bytes must NOT be called on the large (windowed) path"
        )

        def _iter() -> Iterator[dict]:
            yield _row(vt)

        writer.write(_iter())  # must succeed without triggering mock_mat

    # Verify mock was never called.
    assert mock_mat.call_count == 0, (
        "windowed path must not delegate to materialize_to_bytes for a "
        "no-warp / no-clip tile"
    )

    # Verify the output file exists and has the right pixels.
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        actual = ds.read()
    with rasterio.open(src) as orig:
        expected = orig.read()
    np.testing.assert_array_equal(actual, expected)


# ---------------------------------------------------------------------------
# 5. Windowed path correctly applies pending band-select
# ---------------------------------------------------------------------------


def test_large_tile_with_pending_bands(tmp_path, monkeypatch):
    """Windowed path correctly applies pending band-select (PENDING_BANDS)."""
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

    src = str(tmp_path / "in3.tif")
    _write_geotiff(src, count=3)  # 3-band raster

    # Virtual tile with pending band-select: only band 2
    vt = VirtualTile(
        cellid=1,
        path=src,
        metadata={
            "path_file_size": str(os.path.getsize(src)),
            "pending_bands": "2",
        },
    )

    win_bytes = _windowed_materialize_bytes(vt)
    with MemoryFile(win_bytes) as mf:
        with mf.open() as ds:
            assert ds.count == 1, f"expected 1 band after band-select, got {ds.count}"
            actual = ds.read(1)

    # Expected: band 2 of the original raster
    with rasterio.open(src) as orig:
        expected = orig.read(2)

    np.testing.assert_array_equal(
        actual, expected, err_msg="band-select pixel mismatch"
    )


# ---------------------------------------------------------------------------
# 6. Windowed path reads the correct sub-window of the source
# ---------------------------------------------------------------------------


def test_large_tile_subwindow(tmp_path, monkeypatch):
    """Windowed path reads only the specified sub-window."""
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")

    src = str(tmp_path / "in.tif")
    _write_geotiff(src, width=8, height=6)

    # Sub-window: top-left 4x3 block
    window = (0, 0, 4, 3)  # (col_off, row_off, width, height)
    vt = VirtualTile(
        cellid=1,
        path=src,
        window=window,
        metadata={"path_file_size": str(os.path.getsize(src))},
    )

    win_bytes = _windowed_materialize_bytes(vt)
    with MemoryFile(win_bytes) as mf:
        with mf.open() as ds:
            assert ds.width == 4
            assert ds.height == 3
            actual = ds.read(1)

    # Expected: same sub-window from materialize_to_bytes (reference)
    ref_bytes = materialize_to_bytes(vt).raster
    with MemoryFile(ref_bytes) as mf:
        with mf.open() as ds:
            expected = ds.read(1)

    np.testing.assert_array_equal(actual, expected, err_msg="subwindow pixel mismatch")


# ---------------------------------------------------------------------------
# 8. Fix 2 — gate on DECODED size, not compressed on-disk size
# ---------------------------------------------------------------------------


def test_write_gate_uses_decoded_size_not_compressed(tmp_path, monkeypatch):
    """A highly-compressible tile (small compressed, large decoded) routes to the
    windowed 'fuse' path because the DECODED size exceeds the cap — even though the
    compressed on-disk size (advertised in path_file_size) does NOT. Proves the
    write gate keys on decoded size, not compressed size."""
    src = str(tmp_path / "compressible.tif")
    compressed, decoded = _write_compressible_geotiff(src, 2048, 2048, dtype="uint16")

    cap = 1 * 1024**2  # 1 MiB
    assert compressed < cap < decoded, (
        f"invalid test setup: need compressed({compressed}) < cap({cap}) "
        f"< decoded({decoded})"
    )
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(cap))

    out_dir = str(tmp_path / "out")
    os.makedirs(out_dir)

    # Metadata advertises the SMALL compressed size; the OLD gate (path_file_size
    # primary) would read this and wrongly choose "stream" (full materialize).
    vt = VirtualTile(
        cellid=1,
        path=src,
        window=None,
        metadata={"path_file_size": str(compressed)},
    )

    schema = reader_schema_v2()
    writer = RasterGbxWriter(out_dir, schema, overwrite=True)
    assert writer._cap == cap

    with patch(
        "databricks.labs.gbx.pyrx.core.open_tile.materialize_to_bytes"
    ) as mock_mat:
        mock_mat.side_effect = AssertionError(
            "materialize_to_bytes must NOT be called: decoded > cap → windowed path"
        )

        def _iter() -> Iterator[dict]:
            yield _row(vt)

        writer.write(_iter())

    assert mock_mat.call_count == 0, (
        "decoded size exceeds cap → the write gate must take the windowed 'fuse' "
        "path, not full materialize"
    )

    # Output round-trips.
    written = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert len(written) == 1
    with rasterio.open(os.path.join(out_dir, written[0])) as ds:
        actual = ds.read()
    with rasterio.open(src) as orig:
        expected = orig.read()
    np.testing.assert_array_equal(actual, expected)


# ---------------------------------------------------------------------------
# 9. Fix 3 — documented limitation: warp/clip tile falls back to full materialize
# ---------------------------------------------------------------------------


def test_windowed_materialize_falls_back_for_warp(tmp_path):
    """ACCEPTED LIMITATION (documented in open_tile docstring + serverless-and-memory.mdx):
    a virtual tile with a pending warp cannot be block-streamed, so
    _windowed_materialize_bytes delegates to materialize_to_bytes — a FULL
    materialize that holds the whole decoded array in RAM. This test documents that
    the fall-back path IS taken (the OOM risk for very large warp/clip tiles remains
    an accepted limitation; WarpedVRT block-streaming is backlog, not implemented)."""
    src = str(tmp_path / "in.tif")
    _write_geotiff(src)

    # Pending reprojection (source is EPSG:4326; request EPSG:3857) → warp required.
    vt = VirtualTile(
        cellid=1,
        path=src,
        window=(0, 0, 8, 6),
        crs="EPSG:3857",
        metadata={"path_file_size": str(os.path.getsize(src))},
    )

    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    real = _ot.materialize_to_bytes
    called = {"n": 0}

    def _spy(tile):
        called["n"] += 1
        return real(tile)

    with patch.object(_ot, "materialize_to_bytes", side_effect=_spy):
        out = _windowed_materialize_bytes(vt)

    assert called["n"] == 1, "warp tile must fall back to materialize_to_bytes"
    with MemoryFile(out) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() == 3857, "fall-back output must be the warped result"


# ---------------------------------------------------------------------------
# 7. Both writers carry the cap captured on the driver; a worker never asks a session
# ---------------------------------------------------------------------------


def _cog_writer(tmp_path, **kw):
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField("path", StringType(), False)])
    return CogGbxWriter(
        str(tmp_path / "cog_out"), schema, overwrite=True, cog_blocksize=256, **kw
    )


def _write_cog_src(path: str) -> None:
    profile = dict(
        driver="GTiff",
        width=256,
        height=256,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.zeros((1, 256, 256), dtype="uint8"))


@pytest.mark.parametrize("writer,kind", [("raster", "write"), ("cog", "cog_write")])
def test_the_writer_captures_the_cap_on_the_driver_and_passes_it_to_the_decision(
    tmp_path, monkeypatch, writer, kind
):
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "31337")
    src = str(tmp_path / "in.tif")
    _write_geotiff(src) if writer == "raster" else _write_cog_src(src)
    if writer == "raster":
        os.makedirs(tmp_path / "out")
        w = RasterGbxWriter(str(tmp_path / "out"), reader_schema_v2(), overwrite=True)
        rows, module = iter([_row(_virtual_tile(src))]), _fgbx
    else:
        w, rows, module = _cog_writer(tmp_path), iter([{"path": src}]), _cw_mod
    assert (
        w._cap == 31337
    ), "the cap is read when the writer is built, not on the worker"

    real, seen = module.materialize_decision, {}

    def _spy(size, kind, spark=None, cap_bytes=None):
        seen.update(kind=kind, cap_bytes=cap_bytes)
        return real(size, kind, spark=spark, cap_bytes=cap_bytes)

    monkeypatch.setattr(module, "materialize_decision", _spy)
    w.write(rows)
    assert seen == {"kind": kind, "cap_bytes": 31337}


# ---------------------------------------------------------------------------
# 8. CogGbxWriter: the COG kind adds a driver bound between "stream" and "error"
# ---------------------------------------------------------------------------

# (size, executor cap, driver bound or None for the default, the route)
COG_DECISION = [
    (0, 1000, None, "stream"),
    (1000, 1000, None, "stream"),
    (1001, 1000, None, "driver"),
    (1024**3, 1000, None, "driver"),
    (5000, 100, 5000, "driver"),
    (5001, 100, 5000, "error"),
    (100 * 1024**3, 100, 5000, "error"),
]


@pytest.mark.parametrize("size,cap,bound,route", COG_DECISION)
def test_the_cog_write_decision_has_a_driver_bound(
    monkeypatch, size, cap, bound, route
):
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(cap))
    if bound is not None:
        monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", bound)
    assert materialize_decision(size, kind="cog_write") == route


def test_a_source_of_unknown_size_streams():
    assert materialize_decision(None, kind="cog_write") == "stream"


def test_a_small_source_converts_on_the_executor(tmp_path):
    src = tmp_path / "in" / "small.tif"
    src.parent.mkdir()
    _write_cog_src(str(src))
    msg = _cog_writer(tmp_path).write(iter([{"path": str(src)}]))
    assert isinstance(msg, CogCommitMessage)
    assert (
        msg.paths == [str(tmp_path / "cog_out" / "small.tif")]
        and msg.pending_paths == []
    ), "a small source is not deferred"


def test_a_source_over_the_cap_is_deferred_to_the_driver_and_logged(
    tmp_path, monkeypatch, caplog
):
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")
    src = tmp_path / "in" / "large.tif"
    src.parent.mkdir()
    _write_cog_src(str(src))
    with patch.object(_cw_mod, "cog_convert_file", create=True) as convert:
        with caplog.at_level(logging.INFO, logger="databricks.labs.gbx.ds.cog_writer"):
            msg = _cog_writer(tmp_path).write(iter([{"path": str(src)}]))
    convert.assert_not_called()
    assert msg.paths == [] and msg.pending_paths == [str(src)]
    assert any("auto-routing" in r.message for r in caplog.records), [
        r.message for r in caplog.records
    ]


def test_a_source_beyond_the_driver_bound_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", 1)
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "0")
    src = tmp_path / "in" / "giant.tif"
    src.parent.mkdir()
    _write_cog_src(str(src))
    with pytest.raises(StageTooLargeError):
        _cog_writer(tmp_path).write(iter([{"path": str(src)}]))


def test_driver_mode_gathers_every_path_whatever_its_size(tmp_path, monkeypatch):
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "1")
    monkeypatch.setattr(_fgbx, "_COG_DRIVER_MAX_BYTES", 1)
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_cog_src(str(src))
    msg = _cog_writer(tmp_path, driver_mode=True).write(iter([{"path": str(src)}]))
    assert msg.paths == [str(src)] and msg.pending_paths == []


@pytest.mark.parametrize("pending", [True, False])
def test_commit_converts_the_deferred_sources_and_only_those(tmp_path, pending):
    import databricks.labs.gbx.pyrx.core.preparer as _prep

    src, calls = tmp_path / "scene.tif", []

    def fake_prepare(sources, out_dir, **kwargs):
        calls.append((list(sources), out_dir, kwargs))

    msg = CogCommitMessage(
        paths=["/fake/out.tif"], pending_paths=[str(src)] if pending else []
    )
    with patch.object(_prep, "prepare_cogs", side_effect=fake_prepare):
        _cog_writer(tmp_path, driver_mode=False).commit([msg])
    if not pending:
        assert calls == []
        return
    ((sources, out_dir, kwargs),) = calls
    assert (sources, out_dir) == ([str(src)], str(tmp_path / "cog_out"))
    assert (kwargs["blocksize"], kwargs["resampling"], kwargs["compression"]) == (
        256,
        "AVERAGE",
        "ZSTD",
    )
