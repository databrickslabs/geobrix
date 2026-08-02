"""cog_gbx writer — v2 tile-struct input (path-direct + windowed materialize).

cog_gbx has always consumed a TOP-LEVEL ``path`` column (file_gbx output) and
path-direct converts each source to a master COG with no bytes round-trip. This
suite covers the added v2 ``(source, tile)`` envelope acceptance:

  (a) WHOLE-FILE virtual tile (raster None, window None-or-full, no clip) →
      PATH-DIRECT: cog_convert_file(local(tile.path), out); materialize_to_bytes
      is NOT called.
  (b) WINDOWED virtual tile (sub-window) → materialize the window to bytes, then
      convert those bytes to COG; output dims == the window; materialize called.
  (c) existing top-level-``path`` (file_gbx-style) input still writes (regression).
"""

import glob
import os

import numpy as np
import rasterio
from pyspark.sql.types import StringType, StructField, StructType
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, assert_path_schema
from databricks.labs.gbx.ds.raster import reader_schema, reader_schema_v2
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
        ds.write(np.arange(w * h, dtype="uint8").reshape(1, h, w) % 251)


def _v2_row(cellid, *, path=None, raster=None, window=None):
    """A minimal v2 (source, tile) envelope row (dicts, subscriptable like a Row).

    ``window`` is a 4-tuple ``(col_off, row_off, width, height)`` or None; it is
    emitted as the nested struct dict the v2 schema uses.
    """
    win = None
    if window is not None:
        c, r, w, h = window
        win = {"col_off": c, "row_off": r, "width": w, "height": h}
    tile = {
        "cellid": int(cellid),
        "raster": raster,
        "path": path,
        "window": win,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    return {"source": path or "", "tile": tile}


def _v2_schema():
    return reader_schema_v2()


# ---------------------------------------------------------------------------
# schema acceptance
# ---------------------------------------------------------------------------


def test_assert_path_schema_accepts_top_level_path():
    ok = StructType([StructField("path", StringType(), False)])
    assert_path_schema(ok)  # no raise


def test_assert_path_schema_accepts_v2_envelope():
    assert_path_schema(reader_schema_v2())  # (source, tile) v2 → no raise
    assert_path_schema(reader_schema())  # (source, tile) v1 → no raise


def test_assert_path_schema_rejects_unknown():
    bad = StructType([StructField("name", StringType(), False)])
    try:
        assert_path_schema(bad)
        assert False, "expected ValueError"
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# (a) whole-file virtual tile → PATH-DIRECT (no materialize)
# ---------------------------------------------------------------------------


def test_whole_file_virtual_is_path_direct(tmp_path, monkeypatch):
    """A whole-file virtual tile (raster None, full window, no clip) must be
    path-direct converted: cog_convert_file gets the SOURCE path and
    materialize_to_bytes is NOT called (no pixels through the Python heap)."""
    import databricks.labs.gbx.ds.cog_writer as _cw

    materialize_calls = []
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    real_mat = _ot.materialize_to_bytes

    def _spy_mat(tile):
        materialize_calls.append(tile)
        return real_mat(tile)

    monkeypatch.setattr(_cw, "materialize_to_bytes", _spy_mat, raising=False)
    monkeypatch.setattr(_ot, "materialize_to_bytes", _spy_mat)

    seen_src = []
    from databricks.labs.gbx.pyrx.core import analysis as _analysis

    real_convert = _analysis.cog_convert_file

    def _spy_convert(src, dst, **kwargs):
        seen_src.append(src)
        return real_convert(src, dst, **kwargs)

    monkeypatch.setattr(_cw, "cog_convert_file", _spy_convert, raising=False)
    monkeypatch.setattr(_analysis, "cog_convert_file", _spy_convert)

    src = tmp_path / "in" / "whole.tif"
    src.parent.mkdir()
    _write_src(str(src), w=512, h=512)
    out = tmp_path / "out"

    w = CogGbxWriter(str(out), _v2_schema(), overwrite=True, cog_blocksize=256)
    # whole-file virtual: full-extent window (a virtual tile always carries a
    # window; full extent == whole-file → path-direct).
    msg = w.write(iter([_v2_row(0, path=str(src), window=(0, 0, 512, 512))]))

    assert materialize_calls == [], "whole-file virtual must NOT materialize bytes"
    assert seen_src == [
        str(src)
    ], f"path-direct convert expected src path; got {seen_src}"
    assert len(msg.paths) == 1 and os.path.exists(msg.paths[0])
    with open(msg.paths[0], "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1


def test_whole_file_virtual_full_window_is_path_direct(tmp_path, monkeypatch):
    """A window that equals the source's full extent is also whole-file →
    path-direct (window == (0, 0, W, H))."""
    import databricks.labs.gbx.ds.cog_writer as _cw
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    materialize_calls = []
    real_mat = _ot.materialize_to_bytes
    monkeypatch.setattr(
        _ot,
        "materialize_to_bytes",
        lambda t: (materialize_calls.append(t) or real_mat(t)),
    )
    monkeypatch.setattr(
        _cw,
        "materialize_to_bytes",
        _ot.materialize_to_bytes,
        raising=False,
    )

    src = tmp_path / "in" / "full.tif"
    src.parent.mkdir()
    _write_src(str(src), w=400, h=300)
    out = tmp_path / "out"

    w = CogGbxWriter(str(out), _v2_schema(), overwrite=True, cog_blocksize=256)
    msg = w.write(iter([_v2_row(0, path=str(src), window=(0, 0, 400, 300))]))

    assert materialize_calls == [], "full-extent window must NOT materialize"
    assert len(msg.paths) == 1 and os.path.exists(msg.paths[0])
    with open(msg.paths[0], "rb") as fh:
        assert gbxcog.sniff_header(fh.read()).is_cog is True


# ---------------------------------------------------------------------------
# (b) windowed virtual tile → materialize path
# ---------------------------------------------------------------------------


def test_windowed_virtual_materializes(tmp_path, monkeypatch):
    """A sub-window virtual tile must be materialized (window read to bytes) then
    converted; the output holds ONLY the window's extent and materialize is
    called."""
    import databricks.labs.gbx.ds.cog_writer as _cw
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    materialize_calls = []
    real_mat = _ot.materialize_to_bytes

    def _spy_mat(tile):
        materialize_calls.append(tile)
        return real_mat(tile)

    monkeypatch.setattr(_ot, "materialize_to_bytes", _spy_mat)
    monkeypatch.setattr(_cw, "materialize_to_bytes", _spy_mat, raising=False)

    src = tmp_path / "in" / "big.tif"
    src.parent.mkdir()
    _write_src(str(src), w=1024, h=1024)
    out = tmp_path / "out"

    w = CogGbxWriter(str(out), _v2_schema(), overwrite=True, cog_blocksize=128)
    # sub-window: 640x480 at offset (32, 16) — big enough to build overviews so
    # the output is a valid COG, while proving only the window (not the whole
    # 1024x1024 source) lands in the output.
    msg = w.write(iter([_v2_row(7, path=str(src), window=(32, 16, 640, 480))]))

    assert len(materialize_calls) == 1, "windowed virtual MUST materialize the window"
    assert len(msg.paths) == 1 and os.path.exists(msg.paths[0])
    with rasterio.open(msg.paths[0]) as ds:
        assert (ds.width, ds.height) == (
            640,
            480,
        ), f"output must hold only the window extent; got {ds.width}x{ds.height}"
    info = gbxcog.sniff_header(open(msg.paths[0], "rb").read())
    assert info.is_cog is True


# ---------------------------------------------------------------------------
# (c) regression: existing top-level-path input still writes
# ---------------------------------------------------------------------------


def test_top_level_path_input_still_writes(tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    schema = StructType([StructField("path", StringType(), False)])
    w = CogGbxWriter(str(out), schema, overwrite=True, cog_blocksize=256)
    w.write(iter([{"path": str(src)}]))
    produced = glob.glob(os.path.join(str(out), "*.tif"))
    assert len(produced) == 1
    with open(produced[0], "rb") as fh:
        assert gbxcog.sniff_header(fh.read()).is_cog is True
