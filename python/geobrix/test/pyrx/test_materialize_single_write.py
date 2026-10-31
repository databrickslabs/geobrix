"""``materialize_to_bytes`` of a whole-file tile with nothing to apply writes its GeoTIFF once.

It used to write the window to GeoTIFF bytes (``open_tile`` -> ``_window_dataset_bytes``), reopen them, and write
them again: the second write was 35 % of the pyrx decode's time. A tile with no window, clip, pending instruction or
warp now goes from the source to the output in one write. The old path is still what a windowed tile takes, so the same
file as a full-extent window is the reference: the two must agree on pixels, mask, CRS, dtype, nodata and the creation
options (and, GDAL being deterministic, on the bytes).
"""

import numpy as np
import pytest
import rasterio
from rasterio.enums import ColorInterp
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import compression as comp
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

SIZE = 96


def _source(tmp_path, kind):
    """A small GeoTIFF of the given kind: a plain float32 with NoData, a tiled uint16 with an own mask band, an RGBA."""
    path = str(tmp_path / f"{kind}.tif")
    rng = np.random.default_rng(7)
    base = dict(
        driver="GTiff",
        width=SIZE,
        height=SIZE,
        crs="EPSG:32633",
        transform=from_origin(5e5, 5e6, 10, 10),
    )
    if kind == "plain":
        data = rng.random((1, SIZE, SIZE)).astype("float32")
        with rasterio.open(
            path, "w", count=1, dtype="float32", nodata=-9999.0, **base
        ) as dst:
            data[0, :4, :4] = -9999.0
            dst.write(data)
    elif kind == "tiled-own-mask":
        data = (rng.random((3, SIZE, SIZE)) * 60000).astype("uint16")
        with rasterio.open(
            path,
            "w",
            count=3,
            dtype="uint16",
            tiled=True,
            blockxsize=32,
            blockysize=32,
            **base,
        ) as dst:
            dst.write(data)
            mask = np.full((SIZE, SIZE), 255, dtype="uint8")
            mask[:8] = 0
            dst.write_mask(mask)
    else:
        data = (rng.random((4, SIZE, SIZE)) * 255).astype("uint8")
        with rasterio.open(
            path, "w", count=4, dtype="uint8", photometric="RGB", **base
        ) as dst:
            dst.write(data)
            dst.colorinterp = [
                ColorInterp.red,
                ColorInterp.green,
                ColorInterp.blue,
                ColorInterp.alpha,
            ]
    return path


def _facts(raster):
    with MemoryFile(raster) as mf, mf.open() as ds:
        return dict(
            pixels=ds.read().tobytes(),
            mask=ds.read_masks().tobytes(),
            crs=ds.crs.to_wkt(),
            transform=tuple(ds.transform),
            dtype=ds.dtypes,
            nodata=ds.nodatavals,
            colorinterp=ds.colorinterp,
            options=dict(
                ds.profile
            ),  # compress, predictor, tiled, block sizes, interleave, ...
            structure=ds.tags(ns="IMAGE_STRUCTURE"),
        )


@pytest.fixture
def writes(monkeypatch):
    """The GeoTIFF writes made by a call: `write_gtiff` is the one writer of the light tier."""
    calls = []
    real = comp.write_gtiff

    def counting(*a, **k):
        calls.append(1)
        return real(*a, **k)

    monkeypatch.setattr(comp, "write_gtiff", counting)
    return calls


@pytest.mark.parametrize("kind", ["plain", "tiled-own-mask", "rgba"])
def test_a_whole_file_tile_is_written_once_and_matches_the_two_write_path(
    tmp_path, writes, kind
):
    path = _source(tmp_path, kind)
    whole = ot.materialize_to_bytes(VirtualTile(cellid=1, path=path))
    assert len(writes) == 1, "a tile with nothing to apply is written once"
    del writes[:]
    windowed = ot.materialize_to_bytes(
        VirtualTile(cellid=1, path=path, window=(0, 0, SIZE, SIZE))
    )
    assert (
        len(writes) == 2
    ), "a windowed tile keeps the window write, then the materialize write"
    assert _facts(whole.raster) == _facts(windowed.raster)
    assert (
        whole.raster == windowed.raster
    )  # GDAL writes deterministically: the same bytes
    assert whole.metadata["tile_byte_size"] == str(len(whole.raster))


@pytest.mark.parametrize(
    "tile",
    [
        dict(window=(8, 8, 40, 32)),
        dict(metadata={"pending_nodata": "auto"}),
        dict(metadata={"pending_bands": "1"}),
        dict(crs="EPSG:4326"),  # a warp
    ],
    ids=["window", "pending-nodata", "pending-bands", "warp"],
)
def test_a_tile_with_something_to_apply_still_takes_the_window_write(
    tmp_path, writes, tile
):
    path = _source(tmp_path, "plain")
    out = ot.materialize_to_bytes(VirtualTile(cellid=1, path=path, **tile))
    assert len(writes) >= 2
    with MemoryFile(out.raster) as mf, mf.open() as ds:
        if "window" in tile:
            assert (ds.width, ds.height) == (40, 32)


def test_a_tile_that_needs_a_warp_opens_its_source_once_and_gives_the_same_bytes(
    tmp_path, monkeypatch
):
    """`_whole_file_bytes` used to open the source to see that a warp was pending, return None, and let `open_tile`
    open it again. The dataset it holds goes on to the warp; the output is what `open_tile` + one write gave."""
    path = _source(tmp_path, "plain")
    tile = VirtualTile(cellid=1, path=path, crs="EPSG:4326")
    with ot.open_tile(tile) as ds:
        reference = ot._dataset_gtiff_bytes(ds)
    opens = []
    real = ot.open_raster

    def counting(p, *a, **k):
        opens.append(p)
        return real(p, *a, **k)

    monkeypatch.setattr(ot, "open_raster", counting)
    out = ot.materialize_to_bytes(tile)
    assert opens == [path], "the source is opened once"
    assert out.raster == reference
