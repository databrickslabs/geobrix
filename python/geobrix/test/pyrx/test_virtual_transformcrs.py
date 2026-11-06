"""rst_transformcrs on a virtual tile warps from the file, not from a copy of the window.

The ordinary open of a virtual tile reads its window whole, encodes it as GeoTIFF bytes and opens
those again, holding the window three times over before the warp starts (and spending the encode).
A tile that is only a window of a file goes to ``open_tile.with_plain_window`` instead. These
tests pin that the result is the same pixels, that the memory the warp holds does not grow with the
source, and the two set-up costs it cuts (one warp call for the bands, one grid per footprint).
"""

import tracemalloc

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import functions as pf
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import warp
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

_CRSS = ["EPSG:3857", "ESRI:54008"]


def _write(
    path, *, n=96, count=3, dtype="float32", nodata=None, alpha=False, mask=False
):
    rng = np.random.default_rng(7)
    if np.dtype(dtype).kind == "f":
        data = rng.random((count, n, n)).astype(dtype)
    else:
        data = rng.integers(1, 200, (count, n, n)).astype(dtype)
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=count,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(-73.99, 40.80, 1e-3, 1e-3),
    )
    if nodata is not None:
        prof["nodata"] = nodata
        data[:, 5:9, 5:9] = nodata
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with rasterio.open(path, "w", **prof) as d:
            d.write(data)
            if alpha:
                from rasterio.enums import ColorInterp

                ci = list(d.colorinterp)
                ci[-1] = ColorInterp.alpha
                d.colorinterp = ci
            if mask:
                m = np.full((n, n), 255, dtype="uint8")
                m[20:30, :] = 0
                d.write_mask(m)
    return str(path)


def _decode(b):
    with MemoryFile(b) as mf, mf.open() as d:
        return (
            d.read(),
            d.read_masks(1),
            tuple(d.transform),
            d.crs.to_wkt(),
            d.nodatavals,
        )


def _reference(tile, crs):
    """The result of the ordinary open: window bytes, then the warp."""
    with ot._open(tile) as ds:
        return warp.reproject_to_crs(ds, crs)


def _same(a, b):
    ra, rb = _decode(a), _decode(b)
    return all(
        np.array_equal(x, y) if isinstance(x, np.ndarray) else x == y
        for x, y in zip(ra, rb)
    )


_SOURCES = {
    "plain": dict(),
    "nodata": dict(nodata=-9999.0),
    "int16": dict(dtype="int16", nodata=-1),
    "own-mask": dict(nodata=-9999.0, mask=True),
    "own-mask-no-nodata": dict(mask=True),
    "alpha": dict(count=4, dtype="uint8", alpha=True),
    "one-band": dict(count=1),
}
_WINDOWS = [None, (0, 0, 96, 96), (10, 20, 50, 40)]


@pytest.mark.parametrize("crs", _CRSS)
@pytest.mark.parametrize("window", _WINDOWS, ids=str)
@pytest.mark.parametrize("kind", sorted(_SOURCES))
def test_virtual_warp_equals_the_ordinary_open(tmp_path, kind, window, crs):
    path = _write(tmp_path / "a.tif", **_SOURCES[kind])
    tile = VirtualTile(cellid=0, path=path, window=window)
    assert _same(pf._transformcrs_bytes(tile, crs), _reference(tile, crs))


def test_virtual_warp_equals_the_materialized_tile(tmp_path):
    path = _write(tmp_path / "a.tif", nodata=-9999.0)
    virtual = VirtualTile(cellid=0, path=path, window=(10, 20, 50, 40))
    with ot._open(virtual) as ds:
        materialized = VirtualTile(cellid=0, raster=warp._identity(ds))
    assert _same(
        pf._transformcrs_bytes(virtual, "EPSG:3857"),
        pf._transformcrs_bytes(materialized, "EPSG:3857"),
    )


def test_identity_target_of_a_window_is_the_window(tmp_path):
    path = _write(tmp_path / "a.tif", nodata=-9999.0)
    tile = VirtualTile(cellid=0, path=path, window=(10, 20, 50, 40))
    out = pf._transformcrs_bytes(tile, "EPSG:4326")
    assert _same(out, _reference(tile, "EPSG:4326"))
    with MemoryFile(out) as mf, mf.open() as d:
        assert (d.width, d.height) == (50, 40)


@pytest.mark.parametrize(
    "change",
    [
        dict(crs="EPSG:3857"),
        dict(metadata={"pending_srid": "3857"}),
        dict(clip_polygon=b"x"),
    ],
    ids=["target-crs", "pending-instruction", "clip"],
)
def test_a_tile_with_an_instruction_is_not_a_plain_window(tmp_path, change):
    path = _write(tmp_path / "a.tif")
    tile = VirtualTile(cellid=0, path=path, **change)
    assert ot.with_plain_window(tile, None, lambda s, w: b"hit") is ot.NOT_PLAIN


def test_materialized_and_file_ref_tiles_are_not_plain(tmp_path):
    path = _write(tmp_path / "a.tif")
    assert (
        ot.with_plain_window(
            VirtualTile(cellid=0, raster=b"bytes"), None, lambda s, w: b"hit"
        )
        is ot.NOT_PLAIN
    )
    assert (
        ot.with_plain_window(
            VirtualTile(cellid=0, path=path), object(), lambda s, w: b"hit"
        )
        is ot.NOT_PLAIN
    )


def test_a_window_outside_the_file_is_not_plain(tmp_path):
    path = _write(tmp_path / "a.tif")
    tile = VirtualTile(cellid=0, path=path, window=(80, 80, 50, 50))
    assert ot.with_plain_window(tile, None, lambda s, w: b"hit") is ot.NOT_PLAIN


def test_a_plain_tile_is_handed_the_file_and_its_window(tmp_path):
    path = _write(tmp_path / "a.tif")
    tile = VirtualTile(cellid=0, path=path, window=(10, 20, 50, 40))
    seen = ot.with_plain_window(
        tile,
        None,
        lambda src, w: (
            src.width,
            src.height,
            tuple(map(int, (w.col_off, w.row_off, w.width, w.height))),
        ),
    )
    assert seen == (96, 96, (10, 20, 50, 40))


# --- memory ---------------------------------------------------------------------------------------------------


def _noise_tile(tmp_path, n):
    """A float32 noise file of n x n x 4 (does not compress) and its virtual tile."""
    rng = np.random.default_rng(1)
    path = str(tmp_path / f"n{n}.tif")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=n,
        height=n,
        count=4,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(-73.9, 40.8, 1e-4, 1e-4),
    ) as d:
        d.write(rng.random((4, n, n), dtype="float32"))
    return VirtualTile(cellid=0, path=path)


@pytest.mark.parametrize("n", [300, 700])
def test_the_warp_holds_only_its_output_not_copies_of_the_source(tmp_path, n):
    """Python-side peak (tracemalloc sees numpy arrays and bytes; GDAL's native buffers are not in it)
    is the output bytes and nothing the size of the source: the ordinary open held 2.2x the output
    (the window as an array, the window as GeoTIFF bytes, then the output), and its multiple of the
    source grows with the source."""
    tile = _noise_tile(tmp_path, n)
    pf._transformcrs_bytes(tile, "EPSG:3857")  # warm: imports, PROJ
    tracemalloc.start()
    out = pf._transformcrs_bytes(tile, "EPSG:3857")
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak < 1.25 * len(out), (peak, len(out))


# --- set-up costs ---------------------------------------------------------------------------------------------


def test_the_bands_go_through_one_warp_call(tmp_path, monkeypatch):
    path = _write(tmp_path / "a.tif", count=4)
    calls = []
    real = warp.reproject
    monkeypatch.setattr(
        warp, "reproject", lambda *a, **k: calls.append(1) or real(*a, **k)
    )
    pf._transformcrs_bytes(VirtualTile(cellid=0, path=path), "EPSG:3857")
    assert len(calls) == 1


def test_masked_bands_keep_a_warp_call_each(tmp_path, monkeypatch):
    """A source with its own mask and a NoData reads each band masked, so each band is its own call."""
    masked = _write(tmp_path / "m.tif", count=2, nodata=-9999.0, mask=True)
    calls = []
    real = warp.reproject
    monkeypatch.setattr(
        warp, "reproject", lambda *a, **k: calls.append(1) or real(*a, **k)
    )
    pf._transformcrs_bytes(VirtualTile(cellid=0, path=masked), "EPSG:3857")
    assert len(calls) == 2


def test_a_footprint_warped_again_builds_its_grid_once(tmp_path, monkeypatch):
    warp._GRID_CACHE.clear()
    path = _write(tmp_path / "a.tif")
    built = []
    real = warp.calculate_default_transform
    monkeypatch.setattr(
        warp,
        "calculate_default_transform",
        lambda *a, **k: built.append(1) or real(*a, **k),
    )
    tile = VirtualTile(cellid=0, path=path)
    first = pf._transformcrs_bytes(tile, "EPSG:3857")
    second = pf._transformcrs_bytes(tile, "EPSG:3857")
    assert len(built) == 1
    assert _same(first, second)
    pf._transformcrs_bytes(tile, "ESRI:54008")  # another target: its own grid
    pf._transformcrs_bytes(
        VirtualTile(cellid=0, path=path, window=(10, 20, 50, 40)), "EPSG:3857"
    )  # another footprint: its own grid
    assert len(built) == 3


def test_the_grid_cache_is_bounded(monkeypatch):
    warp._GRID_CACHE.clear()
    monkeypatch.setattr(warp, "_GRID_CACHE_SIZE", 3)
    for i in range(10):
        warp._cached_default_transform(
            "EPSG:4326", "EPSG:3857", 10, 10, (i, 0.0, i + 1.0, 1.0)
        )
    assert len(warp._GRID_CACHE) == 3


def test_a_window_of_a_source_with_its_own_mask_is_not_plain(tmp_path):
    path = _write(tmp_path / "a.tif", mask=True)
    window = VirtualTile(cellid=0, path=path, window=(10, 20, 50, 40))
    whole = VirtualTile(cellid=0, path=path)
    assert ot.with_plain_window(window, None, lambda s, w: b"hit") is ot.NOT_PLAIN
    assert ot.with_plain_window(whole, None, lambda s, w: b"hit") == b"hit"
