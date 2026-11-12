"""The viewshed row on the engine: the window a distance limit gives, and the edge of the DEM.

The rows of SPEC_ANALYSIS judge the masks; these pin what a mask cannot say: where the window lies on the DEM's pixel grid,
which pixel an observer near the edge stands in, and that an observer outside the DEM gets GDAL's own result. They run where
the engine does (the dev container).
"""

import math

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import run

pytestmark = pytest.mark.skipif(
    bool(run.unavailable_reason()), reason=str(run.unavailable_reason())
)
FN = "gbx_rst_viewshed"


def _flat_dem(n: int) -> bytes:
    """An n x n flat DEM, 1 m pixels, origin (0, n)."""
    profile = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=from_origin(0.0, float(n), 1.0, 1.0),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, n, n), dtype="float32"))
        return mf.read()


def _viewshed(n: int, x: float, y: float, height: float, limit):
    values = {
        "observer_geom": f"POINT({x} {y})",
        "observer_height": height,
        "target_height": 0.0,
        "max_distance": limit,
        "crs": None,
    }
    res = run.run(manifest.load().functions[FN], [_flat_dem(n)], values)
    assert res.error is None, res.error
    return res.raster


@pytest.mark.parametrize("n, r", [(200, 80.0), (1000, 200.0)])
def test_a_distance_limit_gives_the_window_around_the_observer(n, r):
    """The output is the window around the observer (side 2r + 1, its origin r + half a pixel off the observer), on the
    DEM's own pixel grid, holding a disc of pi r^2 visible pixels: not the DEM with the disc stretched across it.
    """
    ox, oy = n // 2 + 0.5, n // 2 - 0.5  # a pixel centre
    with MemoryFile(_viewshed(n, ox, oy, 10.0, r)) as mf, mf.open() as o:
        a = o.read(1)
        side = int(2 * r) + 1
        assert a.shape == (side, side)
        assert (o.transform.c, o.transform.f) == (ox - r - 0.5, oy + r + 0.5)
        assert (o.transform.a, o.transform.e) == (1.0, -1.0)
    disc = math.pi * r * r
    assert abs(int((a == 255).sum()) - disc) <= 0.01 * disc


def test_an_observer_between_the_edge_and_the_first_pixel_centre_stands_in_that_pixel():
    """GDAL takes the pixel the observer stands in, so an observer within half a pixel of the edge gets a real viewshed."""
    with MemoryFile(_viewshed(40, 0.2, 20.0, 5.0, None)) as mf, mf.open() as o:
        assert int((o.read(1) == 255).sum()) > 0


@pytest.mark.parametrize("x, limit", [(-3.0, None), (-200.0, 300.0)])
def test_an_observer_outside_the_dem_gets_the_mask_gdal_gives_and_its_error(x, limit):
    """GDAL takes an observer outside the DEM as it is (nothing between it and the DEM blocks the view) and writes the
    DEM's cells; the row passes that through."""
    n = 40
    dem = np.zeros((1, n, n), dtype="float32")
    dem[0, :, 25] = 30.0  # a wall
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=n,
            height=n,
            count=1,
            dtype="float32",
            crs="EPSG:27700",
            transform=from_origin(0.0, float(n), 1.0, 1.0),
        ) as dst:
            dst.write(dem)
        data = mf.read()
    gdal = run._gdal()
    gdal.FileFromMemBuffer("/vsimem/outside_dem.tif", data)
    try:
        src = gdal.Open("/vsimem/outside_dem.tif")
        want = gdal.ViewshedGenerate(
            src.GetRasterBand(1),
            "GTiff",
            "/vsimem/outside_mask.tif",
            [],
            x,
            20.0,
            2.0,
            1.6,
            255.0,
            0.0,
            0.0,
            0.0,
            0.85714,
            gdal.GVM_Edge,
            limit or 0.0,
        ).ReadAsArray()
    finally:
        src = None
        gdal.Unlink("/vsimem/outside_dem.tif")
        gdal.Unlink("/vsimem/outside_mask.tif")
    values = {
        "observer_geom": f"POINT({x} 20.0)",
        "observer_height": 2.0,
        "target_height": 1.6,
        "max_distance": limit,
        "crs": None,
    }
    res = run.run(manifest.load().functions[FN], [data], values)
    assert res.error is None, res.error
    with MemoryFile(res.raster) as mf, mf.open() as o:
        assert o.nodata is None and (o.read(1) == want).all()
    assert 0 < int((want == 255).sum()) < want.size


def test_a_distance_limit_that_leaves_no_part_of_the_dem_is_gdals_error():
    """An observer 200 m from the DEM with a 5 m limit: GDAL cannot size the window, and says so in the tile's error."""
    values = {**_VALUES, "observer_geom": "POINT(-200.0 20.0)", "max_distance": 5.0}
    res = run.run(manifest.load().functions[FN], [_flat_dem(40)], values)
    assert res.raster is None and "Invalid target raster size" in res.error, res


def _file_tile(path) -> dict:
    """A virtual tile that is nothing but the whole file at `path`."""
    return {
        "cellid": 0,
        "raster": None,
        "path": str(path),
        "path_mode": None,
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }


def test_a_dem_over_the_read_cap_is_read_by_gdal_from_its_file_not_materialized(
    tmp_path,
):
    """A virtual tile that is a whole file goes to GDAL by its path, so the read cap (which bounds what the engine
    holds in memory) does not apply: the 4 MiB DEM under a 1 MiB cap gives the mask the bytes of the same DEM give under
    a large cap. A tile that is only part of its file is still read under the cap, so it is refused.
    """
    from databricks.labs.gbx.engine import l3

    fn = manifest.load().functions[FN]
    dem = tmp_path / "dem.tif"
    dem.write_bytes(_flat_dem(1024))
    values = {
        "observer_geom": "POINT(512.5 511.5)",
        "observer_height": 10.0,
        "target_height": 0.0,
        "max_distance": None,
        "crs": None,
    }
    cap = 1 << 20

    import contextlib

    with contextlib.ExitStack() as stack:
        rasters, _, _ = l3._inputs(fn, _file_tile(dem), cap, None, None, values, stack)
        assert isinstance(rasters[0], run.PathSource)
        res = run.run(fn, rasters, values)
    assert res.error is None, res.error
    assert "<input>" in res.command and str(dem) not in res.command
    with pytest.raises(run.DataError, match="read cap"):
        l3._inputs(
            fn, _file_tile(dem), cap, None, None, values, None
        )  # no stack: bytes
    windowed = {
        **_file_tile(dem),
        "window": {"col_off": 0, "row_off": 0, "width": 1024, "height": 600},
    }
    with pytest.raises(run.DataError, match="read cap"):
        with contextlib.ExitStack() as stack:
            l3._inputs(fn, windowed, cap, None, None, values, stack)
    want = run.run(
        fn, [dem.read_bytes()], values
    )  # the same DEM as bytes, nothing capped
    with (
        MemoryFile(res.raster) as a,
        a.open() as x,
        MemoryFile(want.raster) as b,
        b.open() as y,
    ):
        assert x.read(1).shape == (1024, 1024) and (x.read(1) == y.read(1)).all()
        assert int((x.read(1) == 255).sum()) > 0


def test_a_file_read_by_path_that_cannot_be_read_is_an_error_tile_not_zeros(tmp_path):
    """GDAL's viewshed carries on past a block it cannot read and returns success. The engine collects the failures GDAL
    reports and gives the tile's error, naming the file, never a plausible mask."""
    import contextlib

    from databricks.labs.gbx.engine import l3

    fn = manifest.load().functions[FN]
    dem = tmp_path / "cut_short.tif"
    data = _flat_dem(256)
    profile = dict(driver="GTiff", width=256, height=256, count=1, dtype="float32")
    with MemoryFile() as mf:
        with mf.open(
            **profile,
            crs="EPSG:27700",
            transform=from_origin(0.0, 256.0, 1.0, 1.0),
            compress="zstd",
            blockysize=16,
            tiled=False,
        ) as dst:
            dst.write(np.random.default_rng(1).random((1, 256, 256)).astype("float32"))
        data = mf.read()
    dem.write_bytes(data[: int(len(data) * 0.6)])
    values = {
        "observer_geom": "POINT(128.5 127.5)",
        "observer_height": 10.0,
        "target_height": 0.0,
        "max_distance": None,
        "crs": None,
    }
    with contextlib.ExitStack() as stack:
        rasters, _, _ = l3._inputs(
            fn, _file_tile(dem), 1 << 30, None, None, values, stack
        )
        res = run.run(fn, rasters, values)
    assert res.raster is None and "cut_short.tif" in res.error


def test_the_read_cap_judges_bytes_and_the_decoded_budget_judges_a_file_read_by_path(
    tmp_path, monkeypatch
):
    """The engine holds a bytes tile, so the read cap refuses one over it (ccaaad072 dropped that check for every row).
    A file GDAL reads by its path holds nothing, so the cap skips it, but the decoded budget still judges its output.
    """
    import contextlib

    from databricks.labs.gbx.engine import l3
    from databricks.labs.gbx.pyrx.core import budget

    fn = manifest.load().functions[FN]
    dem = tmp_path / "dem.tif"
    dem.write_bytes(_flat_dem(256))  # 256 KiB decoded
    values = {
        "observer_geom": "POINT(128.5 127.5)",
        "observer_height": 10.0,
        "target_height": 0.0,
        "max_distance": None,
        "crs": None,
    }
    monkeypatch.setattr(budget, "tile_read_cap", lambda: 4096)
    res = run.run(fn, [dem.read_bytes()], values)
    assert res.raster is None and "read cap (4 KiB)" in res.error, res
    with contextlib.ExitStack() as stack:
        rasters, _, _ = l3._inputs(fn, _file_tile(dem), 4096, None, None, values, stack)
        assert isinstance(rasters[0], run.PathSource)
        assert run.run(fn, rasters, values).error is None  # under the cap: not read
        monkeypatch.setattr(run, "decoded_budget", lambda: 1000)
        res = run.run(fn, rasters, values)
    assert res.raster is None and "decoded" in res.error, res


_VALUES = {
    "observer_geom": "POINT(64.5 63.5)",
    "observer_height": 10.0,
    "target_height": 0.0,
    "max_distance": None,
    "crs": None,
}


def test_an_observer_that_cannot_be_moved_into_the_rasters_crs_is_an_error_tile():
    """A latitude past the pole has no place in British National Grid: the tile's error, not a failed task."""
    values = {**_VALUES, "observer_geom": "SRID=4326;POINT(0 95)"}
    res = run.run(manifest.load().functions[FN], [_flat_dem(128)], values)
    assert res.raster is None and "cannot be moved into the raster" in res.error


def test_a_vrt_read_by_its_path_is_named_by_its_own_driver(tmp_path):
    """A virtual tile whose file is a VRT goes to GDAL by its path as a VRT (the argv names GeoTIFF, the format of the
    bytes the engine writes), and gives the mask the GeoTIFF gives; a file no driver reads is refused by name.
    """
    import contextlib

    from databricks.labs.gbx.engine import l3

    fn = manifest.load().functions[FN]
    gdal = run._gdal()
    dem, vrt = tmp_path / "dem.tif", tmp_path / "dem.vrt"
    dem.write_bytes(_flat_dem(128))
    assert gdal.Translate(str(vrt), str(dem), format="VRT") is not None
    out = {}
    for name, path in (("vrt", vrt), ("tif", dem)):
        with contextlib.ExitStack() as stack:
            rasters, _, _ = l3._inputs(
                fn, _file_tile(path), 1 << 30, None, None, _VALUES, stack
            )
            assert isinstance(rasters[0], run.PathSource)
            out[name] = run.run(fn, rasters, _VALUES)
            assert out[name].error is None, out[name].error
    assert "--input-format=VRT" in out["vrt"].command
    with (
        MemoryFile(out["vrt"].raster) as a,
        a.open() as x,
        MemoryFile(out["tif"].raster) as b,
        b.open() as y,
    ):
        assert (x.read(1) == y.read(1)).all()
    junk = tmp_path / "notes.txt"
    junk.write_text("not a raster")
    with pytest.raises(run.DataError, match="is not a raster GeoBrix reads"):
        run._path_input_format(gdal, ["--input-format=GTiff"], str(junk), True)


def test_the_block_cache_is_capped_while_gdal_reads_a_file_by_its_path(
    tmp_path, monkeypatch
):
    """GDAL reads a file by its path block by block, so its block cache is what it holds: capped to
    ``streaming_cache_bytes`` during the run and put back after. A bytes tile in /vsimem runs with the cache as it is.
    """
    import contextlib

    from databricks.labs.gbx.engine import l3
    from databricks.labs.gbx.pyrx.core import budget

    fn = manifest.load().functions[FN]
    gdal = run._gdal()
    dem = tmp_path / "dem.tif"
    dem.write_bytes(_flat_dem(128))
    seen, execute = [], run._execute

    def recorded(*a, **k):
        seen.append(gdal.GetCacheMax())
        return execute(*a, **k)

    monkeypatch.setattr(run, "_execute", recorded)
    before = gdal.GetCacheMax()
    monkeypatch.setattr(budget, "streaming_cache_bytes", lambda *a: before // 2)
    with contextlib.ExitStack() as stack:
        rasters, _, _ = l3._inputs(
            fn, _file_tile(dem), 1 << 30, None, None, _VALUES, stack
        )
        assert run.run(fn, rasters, _VALUES).error is None
    assert run.run(fn, [dem.read_bytes()], _VALUES).error is None
    assert seen == [before // 2, before]
    assert gdal.GetCacheMax() == before


def test_a_file_read_by_its_path_gets_the_cache_its_budget_leaves(tmp_path, monkeypatch):
    """GDAL's viewshed reads its DEM a line at a time, across the whole window. Over tiled sources a line touches a row
    of blocks, and a cache smaller than that row decodes each block again for each of its lines: a 25-tile window of
    512-pixel float32 blocks took minutes under the 4 MiB share. The cache takes what the call's budget leaves after its
    output, so a row of blocks stays cached and the output is the same."""
    import contextlib

    import rasterio

    from databricks.labs.gbx.ds._mosaic import mint_vrt
    from databricks.labs.gbx.engine import l3
    from databricks.labs.gbx.pyrx.core import budget

    fn = manifest.load().functions[FN]
    gdal = run._gdal()
    rng = np.random.default_rng(0)
    paths = []
    for i in range(4):  # 4 tiles side by side: a line of the window crosses 8 blocks of 128 x 128 float32 (512 KiB)
        p = str(tmp_path / f"t{i}.tif")
        with rasterio.open(
            p,
            "w",
            driver="GTiff",
            width=256,
            height=256,
            count=1,
            dtype="float32",
            crs="EPSG:27700",
            transform=from_origin(256.0 * i, 256.0, 1.0, 1.0),
            tiled=True,
            blockxsize=128,
            blockysize=128,
            compress="deflate",
        ) as dst:
            dst.write((rng.random((1, 256, 256)) * 5).astype("float32"))
        paths.append(p)
    vrt = mint_vrt(paths, out=str(tmp_path / "w.vrt"))
    values = {**_VALUES, "observer_geom": "POINT(512.5 127.5)"}
    seen, execute = [], run._execute

    def recorded(*a, **k):
        seen.append(gdal.GetCacheMax())
        return execute(*a, **k)

    def viewshed():
        with contextlib.ExitStack() as stack:
            rasters, _, _ = l3._inputs(fn, _file_tile(vrt), 1 << 30, None, None, values, stack)
            res = run.run(fn, rasters, values)
        assert res.error is None, res.error
        return res.raster

    reference = viewshed()
    monkeypatch.setattr(run, "_execute", recorded)
    monkeypatch.setattr(run, "decoded_budget", lambda: 8 << 20)
    monkeypatch.setattr(budget, "block_cache_bytes", lambda *a, **k: 64 << 10)
    before = gdal.GetCacheMax()
    got = viewshed()
    row_of_blocks = 8 * 128 * 128 * 4
    assert row_of_blocks < seen[0] <= 8 << 20  # one 1024 x 256 Byte output (256 KiB) held beside it
    assert gdal.GetCacheMax() == before
    with MemoryFile(reference) as a, a.open() as x, MemoryFile(got) as b, b.open() as y:
        assert (x.read(1) == y.read(1)).all()
