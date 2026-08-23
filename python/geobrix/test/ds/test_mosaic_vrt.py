"""Task 3 (SDD — Phase A native mini-COG mosaic): VRT index write via commit().

Tests the mosaic-mode VRT write path in CogGbxWriter.commit() added in Task 3.
Pure Python (no Spark, no JAR).

Coverage:
  1. After write+commit, <out_dir>/mosaic.vrt exists.
  2. VRT references exactly the written (pruned) members; default paths are relative
     (relativeToVRT="1", bare filename like "tile_0_0.tif").
  3. rasterio.open(mosaic.vrt) opens correctly: width/height/CRS/transform match source.
  4. Windowed read returns correct pixels (compare to same window from source raster).
  5. write_vrt=False → no .vrt produced.
  6. vrt_paths="absolute" → absolute SourceFilename paths (full path, relativeToVRT="0").
  7. VRT references only the pruned (written) members, not pruned empty tiles.
  8. Single-COG commit() still works (regression, no VRT written, no crash).

Run (in Docker):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/ds/test_mosaic_vrt.py \\
        --log mosaic-vrt.log
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from typing import List, Tuple

import numpy as np
import rasterio
from pyspark.sql.types import StringType, StructField, StructType
from rasterio.transform import from_origin
from rasterio.windows import Window

from databricks.labs.gbx.ds import _listing
from databricks.labs.gbx.ds.cog_writer import (
    CogCommitMessage,
    CogGbxWriter,
    _source_discriminator,
    parse_mosaic_options,
)

# ---------------------------------------------------------------------------
# Helpers shared with test_mosaic_write (duplicated for test isolation)
# ---------------------------------------------------------------------------


def _path_schema() -> StructType:
    return StructType([StructField("path", StringType(), False)])


def _write_src(
    path: str,
    w: int = 200,
    h: int = 120,
    count: int = 1,
    dtype: str = "uint16",
    nodata=None,
) -> None:
    """Write a small striped GTiff with deterministic pixel values."""
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=count,
        dtype=dtype,
        crs="EPSG:32632",
        transform=from_origin(400000.0, 5000000.0, 10.0, 10.0),
    )
    if nodata is not None:
        profile["nodata"] = nodata
    data = np.arange(w * h, dtype=dtype).reshape(1, h, w) % np.iinfo(dtype).max
    if count > 1:
        data = np.stack(
            [data[0] + np.iinfo(dtype).max // count * b for b in range(count)]
        )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def _write_src_with_nodata_patch(
    path: str, w: int = 200, h: int = 120, nodata: float = 0.0
) -> None:
    """Write a raster where the bottom-right 100x20 block is all nodata."""
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32632",
        transform=from_origin(400000.0, 5000000.0, 10.0, 10.0),
        nodata=nodata,
    )
    data = np.ones((1, h, w), dtype="float32") * 99.0
    data[0, 100:, 100:] = nodata
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def _run_mosaic(src_path: str, out_dir: str, **mosaic_kwargs) -> CogCommitMessage:
    """Run write() + commit() on the driver.  Returns the CogCommitMessage."""
    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "none", **mosaic_kwargs}
    )
    writer = CogGbxWriter(
        str(out_dir),
        _path_schema(),
        overwrite=True,
        cog_blocksize=256,
        mosaic_opts=opts,
    )
    msg = writer.write(iter([{"path": src_path}]))
    writer.commit([msg])
    return msg


def _source_filenames(vrt_path: str) -> List[str]:
    """Return SourceFilename text values from a VRT XML."""
    tree = ET.parse(vrt_path)
    return [sf.text for sf in tree.getroot().iter("SourceFilename")]


def _source_filenames_with_rel(vrt_path: str) -> List[Tuple[str, str]]:
    """Return [(text, relativeToVRT)] tuples from a VRT XML."""
    tree = ET.parse(vrt_path)
    return [
        (sf.text or "", sf.get("relativeToVRT", "0"))
        for sf in tree.getroot().iter("SourceFilename")
    ]


# ---------------------------------------------------------------------------
# 1. VRT file is created after write+commit
# ---------------------------------------------------------------------------


def test_vrt_file_created(tmp_path):
    """After write+commit in mosaic mode, mosaic.vrt exists in out_dir."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src)
    _run_mosaic(src, str(tmp_path / "out"))
    assert os.path.exists(str(tmp_path / "out" / "mosaic.vrt"))


# ---------------------------------------------------------------------------
# 2. VRT references exactly the written members with relative paths
# ---------------------------------------------------------------------------


def test_vrt_relative_paths_default(tmp_path):
    """Default vrtPaths='relative': SourceFilename uses bare filename + relativeToVRT='1'."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src, w=200, h=120)
    msg = _run_mosaic(src, str(tmp_path / "out"), tileSize=100)
    vrt_path = str(tmp_path / "out" / "mosaic.vrt")

    fns_rel = _source_filenames_with_rel(vrt_path)

    # All SourceFilename must have relativeToVRT="1"
    bad_abs = [(fn, r) for fn, r in fns_rel if r != "1"]
    assert not bad_abs, f"some SourceFilename not relative: {bad_abs}"

    # Paths must be bare filenames (no directory separators)
    names = [fn for fn, _ in fns_rel]
    not_bare = [fn for fn in names if os.sep in fn or "/" in fn]
    assert not not_bare, f"SourceFilename contains path separator: {not_bare}"

    # Must reference exactly the written tiles and nothing else
    assert set(names) == {
        os.path.basename(p) for p in msg.paths
    }, f"VRT member set mismatch: VRT={set(names)}, msg={set(os.path.basename(p) for p in msg.paths)}"


# ---------------------------------------------------------------------------
# 3. VRT opens with correct mosaic metadata
# ---------------------------------------------------------------------------


def test_vrt_opens_with_correct_metadata(tmp_path):
    """rasterio.open(mosaic.vrt) reports the same width/height/CRS/transform as source."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src, w=200, h=120)
    _run_mosaic(src, str(tmp_path / "out"), tileSize=100)

    with rasterio.open(src) as ref:
        ref_w, ref_h = ref.width, ref.height
        ref_crs = ref.crs
        ref_tr = ref.transform

    vrt_path = str(tmp_path / "out" / "mosaic.vrt")
    with rasterio.open(vrt_path) as vrt:
        assert vrt.width == ref_w, f"VRT width {vrt.width} != source {ref_w}"
        assert vrt.height == ref_h, f"VRT height {vrt.height} != source {ref_h}"
        assert vrt.crs == ref_crs, f"VRT CRS {vrt.crs!r} != source {ref_crs!r}"
        assert abs(vrt.transform.c - ref_tr.c) < 1e-3, "VRT x-origin mismatch"
        assert abs(vrt.transform.f - ref_tr.f) < 1e-3, "VRT y-origin mismatch"
        assert abs(vrt.transform.a - ref_tr.a) < 1e-6, "VRT x-pixel-size mismatch"
        assert abs(vrt.transform.e - ref_tr.e) < 1e-6, "VRT y-pixel-size mismatch"


# ---------------------------------------------------------------------------
# 4. Windowed read from VRT matches source pixels
# ---------------------------------------------------------------------------


def test_vrt_windowed_read_matches_source(tmp_path):
    """A windowed read from mosaic.vrt returns pixels equal to the source raster."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src, w=200, h=120, dtype="uint16")
    _run_mosaic(src, str(tmp_path / "out"), tileSize=100)

    # Read a window that spans two tile columns (cols 50–150, rows 30–90).
    win = Window(50, 30, 100, 60)

    with rasterio.open(src) as ref_ds:
        ref_data = ref_ds.read(window=win)

    vrt_path = str(tmp_path / "out" / "mosaic.vrt")
    with rasterio.open(vrt_path) as vrt_ds:
        vrt_data = vrt_ds.read(window=win)

    np.testing.assert_array_equal(
        vrt_data,
        ref_data,
        err_msg="VRT windowed read differs from source raster",
    )


def test_vrt_full_read_matches_source(tmp_path):
    """Reading the full VRT extent returns all source pixels correctly."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src, w=200, h=120, dtype="uint16")
    _run_mosaic(src, str(tmp_path / "out"), tileSize=100)

    with rasterio.open(src) as ref_ds:
        ref_data = ref_ds.read()

    vrt_path = str(tmp_path / "out" / "mosaic.vrt")
    with rasterio.open(vrt_path) as vrt_ds:
        vrt_data = vrt_ds.read()

    np.testing.assert_array_equal(
        vrt_data,
        ref_data,
        err_msg="Full VRT read differs from source raster",
    )


# ---------------------------------------------------------------------------
# 5. write_vrt=False → no .vrt produced
# ---------------------------------------------------------------------------


def test_write_vrt_false_no_vrt_file(tmp_path):
    """write_vrt=False: commit() must not create mosaic.vrt."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src)
    _run_mosaic(src, str(tmp_path / "out"), writeVrt="false")
    vrt_path = str(tmp_path / "out" / "mosaic.vrt")
    assert not os.path.exists(vrt_path), f"mosaic.vrt unexpectedly created: {vrt_path}"


def test_write_vrt_false_tiles_still_written(tmp_path):
    """write_vrt=False: mini-COG tiles are still written (VRT skip is silent)."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src, w=200, h=120)
    msg = _run_mosaic(src, str(tmp_path / "out"), tileSize=100, writeVrt="false")
    assert len(msg.paths) == 4, f"expected 4 tiles, got {len(msg.paths)}"
    assert all(os.path.exists(p) for p in msg.paths)


# ---------------------------------------------------------------------------
# 6. vrt_paths="absolute" → absolute SourceFilename
# ---------------------------------------------------------------------------


def test_vrt_absolute_paths_option(tmp_path):
    """vrtPaths='absolute': SourceFilename uses full absolute path + relativeToVRT='0'."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src, w=200, h=120)
    _run_mosaic(src, str(tmp_path / "out"), tileSize=100, vrtPaths="absolute")
    vrt_path = str(tmp_path / "out" / "mosaic.vrt")

    fns_rel = _source_filenames_with_rel(vrt_path)
    for fn, rel in fns_rel:
        assert os.path.isabs(fn), f"expected absolute path, got: {fn!r}"
        assert rel == "0", f"expected relativeToVRT='0', got: {rel!r}"


# ---------------------------------------------------------------------------
# 7. VRT references only written (non-pruned) tiles
# ---------------------------------------------------------------------------


def test_vrt_references_only_written_tiles(tmp_path):
    """VRT does not reference pruned (all-nodata) tiles."""
    src = str(tmp_path / "src" / "nodata.tif")
    _write_src_with_nodata_patch(src, w=200, h=120, nodata=0.0)
    msg = _run_mosaic(src, str(tmp_path / "out"), tileSize=100, pruneEmpty="true")
    vrt_path = str(tmp_path / "out" / "mosaic.vrt")

    fns = _source_filenames(vrt_path)
    # Normalise: strip to bare filename
    bare_fns = {os.path.basename(fn) if os.path.isabs(fn) else fn for fn in fns}

    # tile_1_1 is all-nodata and should NOT appear (name carries source disc).
    disc = _source_discriminator(_listing.to_local_path(src))
    assert (
        f"tile_{disc}_1_1.tif" not in bare_fns
    ), "VRT must not reference pruned tile_1_1"

    # VRT member count == written tile count
    assert len(fns) == len(
        msg.paths
    ), f"VRT has {len(fns)} members but {len(msg.paths)} tiles were written"

    # VRT names == written tile names
    assert bare_fns == {os.path.basename(p) for p in msg.paths}


# ---------------------------------------------------------------------------
# 8. Single-COG commit() regression: no VRT, no crash
# ---------------------------------------------------------------------------


def test_single_cog_commit_no_vrt_no_crash(tmp_path):
    """mosaic_opts=None: commit() runs without error and creates no mosaic.vrt."""
    src = str(tmp_path / "src" / "input.tif")
    _write_src(src, w=100, h=80)

    writer = CogGbxWriter(
        str(tmp_path / "out"),
        _path_schema(),
        overwrite=True,
        cog_blocksize=256,
        mosaic_opts=None,
    )
    msg = writer.write(iter([{"path": src}]))
    writer.commit([msg])  # must not raise

    assert not os.path.exists(str(tmp_path / "out" / "mosaic.vrt"))
    assert len(msg.paths) == 1 and os.path.exists(msg.paths[0])


# ---------------------------------------------------------------------------
# 9. Multi-band source — VRT has one band element per source band
# ---------------------------------------------------------------------------


def test_vrt_multiband_source(tmp_path):
    """A 3-band source produces a VRT with 3 VRTRasterBand elements."""
    src = str(tmp_path / "src" / "mb.tif")
    _write_src(src, w=200, h=120, count=3)
    _run_mosaic(src, str(tmp_path / "out"), tileSize=100)
    vrt_path = str(tmp_path / "out" / "mosaic.vrt")

    tree = ET.parse(vrt_path)
    bands = [el for el in tree.getroot() if el.tag == "VRTRasterBand"]
    assert len(bands) == 3, f"expected 3 VRTRasterBand elements, got {len(bands)}"
    # Each band should have ≥ 1 SimpleSource
    for band in bands:
        sources = list(band.iter("SimpleSource"))
        assert sources, f"VRTRasterBand band={band.get('band')} has no SimpleSource"


def test_vrt_multiband_nodata_on_every_band(tmp_path):
    """A multi-band source carrying nodata writes NoDataValue on EVERY VRT band.

    Guards the per-band nodatavals path in _build_mosaic_vrt. (rasterio's
    high-level API can only set a uniform dataset nodata, so differing per-band
    nodata is honored in code but cannot be constructed here without osgeo.)"""
    src = str(tmp_path / "src" / "mb_nd.tif")
    _write_src(src, w=200, h=120, count=3, nodata=0.0)
    _run_mosaic(src, str(tmp_path / "out"), tileSize=100)
    vrt_path = str(tmp_path / "out" / "mosaic.vrt")

    tree = ET.parse(vrt_path)
    bands = [el for el in tree.getroot() if el.tag == "VRTRasterBand"]
    assert len(bands) == 3
    for band in bands:
        nd = band.find("NoDataValue")
        assert nd is not None, f"band {band.get('band')} missing NoDataValue"
        assert float(nd.text) == 0.0, f"band {band.get('band')} nodata != 0.0"
