"""A huge virtual tile is cut into windows from its open file, whatever is pending on it.

``rst_retile``, ``rst_tooverlappingtiles`` and ``rst_maketiles`` cut a virtual tile window by window, so none holds the
tile's window whole. This holds for a tile that is clipped, reprojected, or read from a file with its own mask band, as
for a plain window. Each case runs under a read cap smaller than the tile (a whole decode would be refused), asserts
that no read passes the size of one piece, and compares the pieces with the legacy route (the window decoded whole,
under a cap that allows it, then cut): same pixels, transform, CRS, NoData, mask, grid, order.
"""

import numpy as np
import pytest
import rasterio
import rasterio.windows
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import _gdal_policy
from databricks.labs.gbx.pyrx import functions as fx
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

SIDE = 64  # 64 x 64 float32 = 16 KiB a band
PIECE = 16  # a 16 x 16 float32 piece = 1 KiB
SMALL_CAP = 8000  # under the 16 KiB tile, over a piece (and its margin when warped)
BIG_CAP = 1 << 30


def _write(path, side=SIDE, count=1, own_mask=False, nodata=-9999.0):
    prof = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=count,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(500000.0, 5000000.0, 1000.0, 1000.0),
        nodata=nodata,
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
        if own_mask:
            m = np.full((side, side), 255, dtype="uint8")
            m[:10, :] = 0  # the first ten rows are invalid
            m[:, 40:44] = 0
            ds.write_mask(m)
    return str(path)


def _tile(path, window=(0, 0, SIDE, SIDE), **kw):
    return VirtualTile(cellid=7, path=path, window=window, **kw)


def _ewkb_box(x0, y0, x1, y1):
    xy = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    return b"\x01\x03\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00" + b"".join(
        np.array(p, dtype="<f8").tobytes() for p in xy
    )


# the area the 64 x 64 fixture covers: x 500000..564000, y 4936000..5000000
CLIP_INSIDE = _ewkb_box(510000.5, 4950000.5, 548000.5, 4990000.5)
CLIP_DISJOINT = _ewkb_box(0.0, 0.0, 10.0, 10.0)


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


@pytest.fixture
def decoded(monkeypatch):
    """The pixel counts of every window ``_window_dataset_bytes`` is asked to decode."""
    seen = []
    real = ot._window_dataset_bytes

    def spy(src, window, *a, **k):
        seen.append(int(window.width) * int(window.height))
        return real(src, window, *a, **k)

    monkeypatch.setattr(ot, "_window_dataset_bytes", spy)
    real_read = ot._WindowPointView.read

    def spy_read(self, indexes=None, window=None, masked=False):
        win = window or rasterio.windows.Window(0, 0, self.width, self.height)
        seen.append(int(win.width) * int(win.height))
        return real_read(self, indexes, window, masked)

    monkeypatch.setattr(ot._WindowPointView, "read", spy_read)
    return seen


def _describe(row):
    with MemoryFile(row["raster"]) as mf, mf.open() as ds:
        return dict(
            pixels=ds.read(),
            masks=ds.read_masks(),
            transform=ds.transform,
            crs=ds.crs,
            nodata=ds.nodata,
            dtypes=ds.dtypes,
            colorinterp=ds.colorinterp,
            window=row["window"],
            cellid=row["cellid"],
        )


# GDAL places a warped pixel with an approximate transformer (0.125 px): the pieces of a warp and its whole differ
# in which source pixel a few borderline pixels take, never by more than one source step.
BORDERLINE = 0.1


def _assert_same(got, want, approx=False):
    assert len(got) == len(want) > 0
    differing = total = 0
    for g, w in zip(got, want):
        assert not g["metadata"].get("last_error"), g["metadata"].get("last_error")
        gd, wd = _describe(g), _describe(w)
        for key in ("transform", "crs", "nodata", "dtypes", "colorinterp", "window"):
            assert gd[key] == wd[key], key
        assert gd["pixels"].shape == wd["pixels"].shape
        if approx:
            differ = gd["pixels"] != wd["pixels"]
            differing += int(differ.sum())
            total += differ.size
            assert (
                np.abs(gd["pixels"] - wd["pixels"])[differ].max(initial=0) <= SIDE + 1
            )
            differing += int((gd["masks"] != wd["masks"]).sum())
            total += differ.size
        else:
            assert np.array_equal(gd["pixels"], wd["pixels"], equal_nan=True)
            assert np.array_equal(gd["masks"], wd["masks"])
    assert differing <= BORDERLINE * total


def _rows(tile, tw, th, **kw):
    return list(fx._RstRetileUDTF.func().eval(tile, tw, th, **kw))


def _legacy(tile, tw, th, overlap=None):
    """Today's route: the window decoded whole (a cap that allows it), then cut, and wrapped as the UDTF wraps it."""
    out = []
    with _gdal_policy.carried(None, BIG_CAP), ot._open(tile) as ds:
        it = (
            tiling.iter_retile_located(ds, tw, th)
            if overlap is None
            else tiling.iter_to_overlapping_tiles_located(ds, tw, th, overlap)
        )
        whole = tiling.covers_whole_input(ds, tw, th, overlap or 0)
        for win, b in it:
            out.append(fx._cut_tile(b, 7, win, tile if whole else None, None))
    return out


def _check_cut(
    tile, tw=PIECE, th=PIECE, decoded=None, ref=None, overlap=None, approx=False
):
    kw = {} if ref is None else {"file_ref": ref}
    want = _legacy(tile, tw, th, overlap)
    if decoded is not None:
        decoded.clear()  # the legacy route decoded the whole window; only the cut under the cap is judged
    with _gdal_policy.carried(None, SMALL_CAP):
        if overlap is None:
            got = _rows(tile, tw, th, **kw)
        else:
            udtf = fx._RstToOverlappingTilesUDTF.func()
            got = list(udtf.eval(tile, tw, th, overlap, **kw))
    _assert_same(got, want, approx)
    if decoded is not None:
        assert decoded, "something was decoded for the pieces"
        assert max(decoded) <= 40 * 40, f"a read of {max(decoded)} pixels: not a piece"
    return got


# --- a source with its own mask band -------------------------------------------------------------------------------


def test_retile_of_a_masked_file_cuts_the_mask_with_the_pieces(tmp_path, decoded):
    path = _write(tmp_path / "m.tif", own_mask=True)
    got = _check_cut(_tile(path), decoded=decoded)
    first = _describe(got[0])
    assert (first["masks"][0, :10, :] == 0).all() and (
        first["masks"][0, 10:, :] == 255
    ).all()


def test_retile_of_a_masked_file_window_with_bands_and_nodata_pending(
    tmp_path, decoded
):
    path = _write(tmp_path / "m.tif", count=2, own_mask=True, nodata=None)
    tile = _tile(
        path,
        window=(4, 4, 40, 40),
        metadata={"pending_bands": "2", "pending_nodata": "auto"},
    )
    _check_cut(tile, decoded=decoded)


def test_retile_of_a_masked_file_through_a_file_ref(tmp_path, decoded):
    path = _write(tmp_path / "m.tif", own_mask=True)
    ref = _Ref(path)
    _check_cut(_tile(path), decoded=decoded, ref=ref)
    assert ref.local_asked == 0


# --- a clipped tile ------------------------------------------------------------------------------------------------


def test_retile_of_a_clipped_tile_clips_each_piece(tmp_path, decoded):
    path = _write(tmp_path / "c.tif")
    tile = _tile(path, clip_polygon=CLIP_INSIDE, clip_crs="EPSG:32633")
    got = _check_cut(tile, decoded=decoded)
    assert len(got) > 1


def test_retile_of_a_clipped_window_of_a_masked_file(tmp_path, decoded):
    path = _write(tmp_path / "c.tif", own_mask=True)
    tile = _tile(
        path,
        window=(2, 2, 60, 60),
        clip_polygon=CLIP_INSIDE,
        clip_crs="EPSG:32633",
    )
    _check_cut(tile, decoded=decoded)


def test_retile_of_a_disjoint_clip_is_the_one_empty_tile(tmp_path, decoded):
    path = _write(tmp_path / "c.tif")
    tile = _tile(path, clip_polygon=CLIP_DISJOINT, clip_crs="EPSG:32633")
    got = _check_cut(tile, decoded=decoded)
    assert len(got) == 1


def test_tooverlappingtiles_of_a_clipped_tile(tmp_path, decoded):
    path = _write(tmp_path / "c.tif")
    tile = _tile(path, clip_polygon=CLIP_INSIDE, clip_crs="EPSG:32633")
    _check_cut(tile, decoded=decoded, overlap=25)


# --- a reprojected tile --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("crs", ["EPSG:4326", "EPSG:3857", "ESRI:54009"])
def test_retile_of_a_reprojected_tile_warps_each_piece(tmp_path, decoded, crs):
    path = _write(tmp_path / "w.tif")
    got = _check_cut(_tile(path, crs=crs), tw=24, th=24, decoded=decoded, approx=True)
    assert len(got) > 1


def test_retile_of_a_reprojected_masked_tile(tmp_path, decoded):
    path = _write(tmp_path / "w.tif", own_mask=True, nodata=None)
    _check_cut(_tile(path, crs="EPSG:4326"), tw=24, th=24, decoded=decoded, approx=True)


def test_retile_of_a_reprojected_window_with_pending_band_and_srid(tmp_path, decoded):
    path = _write(tmp_path / "w.tif", count=2)
    tile = _tile(
        path,
        window=(8, 8, 48, 48),
        crs="EPSG:3857",
        metadata={"pending_bands": "2", "pending_nodata": "auto"},
    )
    _check_cut(tile, tw=24, th=24, decoded=decoded, approx=True)


def test_retile_of_a_reprojected_and_clipped_tile(tmp_path, decoded):
    path = _write(tmp_path / "w.tif")
    # the clip is in the tile's own (target) CRS: a box inside the reprojected tile
    tile = _tile(path, crs="EPSG:4326")
    with _gdal_policy.carried(None, BIG_CAP), ot._open(tile) as ds:
        left, bottom, right, top = ds.bounds
    w, h = right - left, top - bottom
    poly = _ewkb_box(left + 0.2 * w, bottom + 0.2 * h, right - 0.2 * w, top - 0.3 * h)
    tile = _tile(path, crs="EPSG:4326", clip_polygon=poly, clip_crs="EPSG:4326")
    _check_cut(tile, tw=24, th=24, decoded=decoded, approx=True)


@pytest.mark.parametrize(
    "extra",
    [
        dict(crs="EPSG:4326"),
        dict(clip_polygon=CLIP_INSIDE, clip_crs="EPSG:32633"),
    ],
)
def test_a_tile_that_fits_the_cap_is_decoded_whole_as_ever(tmp_path, decoded, extra):
    """What the cap lets be decoded takes the route it always took: the output is bit for bit the legacy one."""
    path = _write(tmp_path / "w.tif", own_mask=True)
    tile = _tile(path, **extra)
    want = _legacy(tile, 24, 24)
    decoded.clear()
    with _gdal_policy.carried(None, BIG_CAP):
        got = _rows(tile, 24, 24)
    _assert_same(got, want)
    assert SIDE * SIDE in decoded  # the window was read whole


# --- rst_maketiles -------------------------------------------------------------------------------------------------

MK_SIDE = (
    768  # 768 x 768 float32 = 2.25 MiB: over one MiB, so sizeInMB=1 splits it 2 x 2
)


def _maketiles(tile, mb, **kw):
    return list(fx._RstMakeTilesUDTF.func().eval(tile, mb, **kw))


def _mk_tile(tmp_path, **kw):
    path = _write(tmp_path / "big.tif", side=MK_SIDE)
    return _tile(path, window=(0, 0, MK_SIDE, MK_SIDE), **kw)


def test_maketiles_of_a_huge_virtual_tile_estimates_from_the_header(
    tmp_path, decoded, monkeypatch
):
    """Over the cap the tile count comes from the header (the decoded size bounds the encoded one): the window is
    never decoded, nor encoded to be measured; the tiles are windows of the open file.
    """
    tile = _mk_tile(tmp_path)

    def no_encode(ds):
        raise AssertionError("the window was encoded to be measured")

    monkeypatch.setattr(tiling, "_encoded_size_bytes", no_encode)
    with _gdal_policy.carried(None, 1 << 20):  # 2.25 MiB window: over the cap
        got = _maketiles(tile, 1)
    assert len(got) == 4
    assert decoded and max(decoded) <= (MK_SIDE // 2) ** 2  # a quarter at most
    want = _legacy(tile, MK_SIDE // 2, MK_SIDE // 2)
    _assert_same(got, want)


def test_maketiles_of_a_huge_virtual_tile_with_a_file_ref(tmp_path, decoded):
    tile = _mk_tile(tmp_path)
    ref = _Ref(tile.path)
    with _gdal_policy.carried(None, 1 << 20):
        got = _maketiles(tile, 1, file_ref=ref)
    assert len(got) == 4 and ref.local_asked == 0


def test_maketiles_of_a_huge_clipped_virtual_tile(tmp_path, decoded):
    tile = _mk_tile(
        tmp_path,
        clip_polygon=_ewkb_box(504000.5, 4940000.5, 560000.5, 4996000.5),
        clip_crs="EPSG:32633",
    )
    # every piece stays a piece: nothing larger than a quarter of the clip is read
    with _gdal_policy.carried(None, 1 << 20):
        got = _maketiles(tile, 1)
    assert got and not any(r["metadata"].get("last_error") for r in got)
    assert max(decoded) <= (MK_SIDE // 2) ** 2


def test_maketiles_under_the_cap_keeps_the_exact_encoded_size(tmp_path):
    """Under the cap the estimate is today's (the encoded window), so a compressible raster is split as it was."""
    path = _write(tmp_path / "flat.tif", side=MK_SIDE)
    with rasterio.open(
        path, "r+"
    ) as ds:  # a constant raster compresses to almost nothing
        ds.write(np.zeros((1, MK_SIDE, MK_SIDE), dtype="float32"))
    tile = _tile(path, window=(0, 0, MK_SIDE, MK_SIDE))
    with _gdal_policy.carried(None, BIG_CAP):
        got = _maketiles(tile, 1)
    assert len(got) == 1  # encoded: far under one MiB
