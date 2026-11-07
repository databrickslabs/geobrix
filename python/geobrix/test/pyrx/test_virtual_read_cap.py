"""A virtual tile's window is gated by the read cap where it is decoded into memory.

A materialized tile is judged when its bytes are opened (``_serde.open_tile``). A virtual tile carries no bytes, so
the window is judged where it is read out of its file: width x height x the selected bands x the type size, against
the cap the task carries. Paths that decode nothing (the plain-window warp, the block-streaming materialize, the
retile family cutting windows from the open source) are not gated.
"""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx import _gdal_policy
from databricks.labs.gbx.pyrx import functions as fx
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

SIDE = 64  # 64 x 64 float32 = 16 KiB a band


def _write(path, side=SIDE, count=1):
    prof = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=count,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0.0, float(side), 1.0, 1.0),
        nodata=-9999.0,
        tiled=True,
        blockxsize=16,
        blockysize=16,
    )
    with rasterio.open(path, "w", **prof) as ds:
        for b in range(count):
            ds.write(
                (np.arange(side * side, dtype="float32") + 10000 * b).reshape(
                    side, side
                ),
                b + 1,
            )
    return str(path)


def _tile(path, window=(0, 0, SIDE, SIDE), **kw):
    return VirtualTile(cellid=7, path=path, window=window, **kw)


def _open_refused(tile, cap, **kw):
    with _gdal_policy.carried(None, cap), pytest.raises(ValueError) as err:
        with ot._open(tile, **kw):
            pass
    return str(err.value)


def test_a_window_over_the_cap_is_refused_with_the_retile_fix(tmp_path):
    reason = _open_refused(_tile(_write(tmp_path / "a.tif")), 1000)
    assert "over this compute's read cap" in reason
    assert "gbx_rst_retile" in reason


def test_the_window_is_judged_not_the_file(tmp_path):
    """A small window of a file over the cap is read; the whole of it is refused."""
    path = _write(tmp_path / "a.tif")
    with _gdal_policy.carried(
        None, 2000
    ):  # 16 x 16 x 4 = 1 KiB fits, the file does not
        with ot._open(_tile(path, window=(8, 8, 16, 16))) as ds:
            assert (ds.width, ds.height) == (16, 16)
    _open_refused(_tile(path), 2000)


def test_only_the_selected_bands_count(tmp_path):
    path = _write(tmp_path / "three.tif", count=3)  # 16 KiB a band
    one = _tile(path, metadata={"pending_bands": "2"})
    with _gdal_policy.carried(None, 20000):
        with ot._open(one) as ds:
            assert ds.count == 1
    _open_refused(_tile(path), 20000)  # all three: 48 KiB


def test_a_window_under_the_cap_is_read_whole(tmp_path):
    path = _write(tmp_path / "a.tif")
    with _gdal_policy.carried(None, 1 << 20):
        with ot._open(_tile(path)) as ds:
            assert ds.read().shape == (1, SIDE, SIDE)


def test_a_windowless_tile_is_judged_as_the_whole_file(tmp_path):
    """The whole-file materialize reads the source in one go: the same gate, before a pixel is read."""
    tile = VirtualTile(cellid=7, path=_write(tmp_path / "a.tif"))
    with _gdal_policy.carried(None, 1000), pytest.raises(ValueError, match="read cap"):
        ot.materialize_to_bytes(tile)
    with _gdal_policy.carried(None, 1 << 20):
        assert ot.materialize_to_bytes(tile).raster


def test_a_windowed_materialize_is_gated_too(tmp_path):
    tile = _tile(_write(tmp_path / "a.tif"), window=(0, 0, 32, 32))
    with _gdal_policy.carried(None, 1000), pytest.raises(ValueError, match="read cap"):
        ot.materialize_to_bytes(tile)


class _Ref:
    """A FileRef over a local file; counts how often it is asked for a local copy."""

    def __init__(self, path):
        self.path = path
        self.local_asked = 0

    def open(self):
        return open(self.path, "rb")

    def as_local_file(self):
        self.local_asked += 1
        return self.path


def test_the_file_branch_refuses_without_staging_the_file(tmp_path):
    """The FILE branch reads the window from a stream; over the cap it refuses there, instead of falling through to a
    local copy of the whole file."""
    path = _write(tmp_path / "a.tif")
    ref = _Ref(path)
    reason = _open_refused(_tile(path), 1000, file_ref=ref)
    assert "gbx_rst_retile" in reason
    assert ref.local_asked == 0


def test_the_file_branch_reads_a_window_under_the_cap(tmp_path):
    path = _write(tmp_path / "a.tif")
    with _gdal_policy.carried(None, 2000):
        with ot._open(_tile(path, window=(0, 0, 16, 16)), file_ref=_Ref(path)) as ds:
            assert (ds.width, ds.height) == (16, 16)


def test_paths_that_decode_nothing_stay_ungated(tmp_path):
    """The plain-window warp hands GDAL the file and the window, and the block-streaming materialize writes block
    by block: neither holds the window, so the cap does not judge it."""
    tile = _tile(_write(tmp_path / "a.tif"))
    seen = []
    with _gdal_policy.carried(None, 1000):
        out = ot.with_plain_window(
            tile, None, lambda src, window: seen.append((src.width, window.width)) or 1
        )
        streamed = ot._windowed_materialize_bytes(tile)
    assert out == 1 and seen == [(SIDE, SIDE)]
    assert streamed


# --- the retile family cuts windows from the open source -----------------------------------------------------------


def _retile(tile, tw, th, **kw):
    udtf = fx._RstRetileUDTF.func()
    return list(udtf.eval(tile, tw, th, **kw))


def _pixels(row):
    from rasterio.io import MemoryFile

    with MemoryFile(row["raster"]) as mf, mf.open() as ds:
        return ds.read(), ds.transform, ds.crs


def test_retile_of_a_virtual_tile_over_the_cap_cuts_windows_from_the_source(
    tmp_path, monkeypatch
):
    """The very function the refusal names must work on the tile it refuses: windows are cut from the open file, the
    tile's window is never decoded whole."""
    path = _write(tmp_path / "a.tif")
    whole = []
    real = ot._window_dataset_bytes
    monkeypatch.setattr(
        ot,
        "_window_dataset_bytes",
        lambda *a, **k: whole.append(1) or real(*a, **k),
    )
    with _gdal_policy.carried(
        None, 3000
    ):  # one 16 x 16 piece is 1 KiB, the tile is 16 KiB
        rows = _retile(_tile(path), 16, 16)
    assert len(rows) == 16 and not whole
    assert all(
        r["raster"] is not None and not r["metadata"].get("last_error") for r in rows
    )


def test_retile_of_a_virtual_tile_equals_retile_of_its_bytes(tmp_path):
    path = _write(tmp_path / "a.tif", count=2)
    virtual = _tile(path, window=(4, 4, 40, 40), metadata={"pending_bands": "2"})
    with _gdal_policy.carried(None, 1 << 20):
        with ot._open(virtual) as ds:
            from databricks.labs.gbx.pyrx import _serde

            materialized = _serde.build_tile(ot._dataset_gtiff_bytes(ds), "GTiff", 7)
        want = _retile(materialized, 16, 16)
    with _gdal_policy.carried(None, 3000):
        got = _retile(virtual, 16, 16)
    assert len(got) == len(want) == 9
    for g, w in zip(got, want):
        gp, gt, gc = _pixels(g)
        wp, wt, wc = _pixels(w)
        assert np.array_equal(gp, wp) and gt == wt and gc == wc
        assert g["window"] == w["window"]


def test_retile_of_a_virtual_tile_with_a_file_ref_cuts_windows_too(tmp_path):
    path = _write(tmp_path / "a.tif")
    with _gdal_policy.carried(None, 3000):
        rows = _retile(_tile(path), 32, 32, file_ref=_Ref(path))
    assert len(rows) == 4 and not any(r["metadata"].get("last_error") for r in rows)


def test_retile_of_a_clipped_virtual_tile_cuts_windows_too(tmp_path):
    """A clipped tile is no longer decoded whole to be clipped: each piece is clipped as it is cut (the cases that
    differ are in test_virtual_windowed_cuts)."""
    path = _write(tmp_path / "a.tif")
    polygon = b"\x01\x03\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00" + b"".join(
        np.array(xy, dtype="<f8").tobytes()
        for xy in [(0, 0), (60, 0), (60, 60), (0, 60), (0, 0)]
    )
    tile = _tile(path, clip_polygon=polygon, clip_crs="EPSG:32633")
    with _gdal_policy.carried(None, 3000):
        rows = _retile(tile, 16, 16)
    assert len(rows) == 16
    assert not any(r["metadata"].get("last_error") for r in rows)


def test_tooverlappingtiles_of_a_virtual_tile_over_the_cap_cuts_windows_too(tmp_path):
    path = _write(tmp_path / "a.tif")
    udtf = fx._RstToOverlappingTilesUDTF.func()
    with _gdal_policy.carried(None, 3000):
        rows = list(udtf.eval(_tile(path), 16, 16, 25))
    assert rows and not any(r["metadata"].get("last_error") for r in rows)
