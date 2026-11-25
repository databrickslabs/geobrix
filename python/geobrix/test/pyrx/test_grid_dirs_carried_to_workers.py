"""The PROJ grid directories a user registers reach the raster-to-grid and fit-resolution UDFs on a worker.

A worker has no driver registry: a UDF carries the directories in its closure (as ``_header_accessor_udf`` does) and
hands them to ``configure_gdal_env``. Before this, ``gbx_rst_h3_fitresolution`` and the ``gbx_rst_*_rastertogrid*``
functions called ``configure_gdal_env()`` with none, so a CRS that needs a grid file gave NULL on a worker.
"""

import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest
from databricks.labs.gbx.core import proj_grids
from databricks.labs.gbx.pyrx import _env, _serde
from databricks.labs.gbx.pyrx import functions as prx

DIRS = ("/Volumes/t/grids", "/Volumes/t/more")
CALLS = []


@pytest.fixture(autouse=True)
def _record(monkeypatch):
    CALLS.clear()
    real = _env.configure_gdal_env

    def spy(extra_proj_dirs=None):
        CALLS.append(None if extra_proj_dirs is None else tuple(extra_proj_dirs))
        return real([])  # the dirs are fake: do not let PROJ list them

    monkeypatch.setattr(_env, "configure_gdal_env", spy)
    yield


def _tile():
    import numpy as np

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=4,
            height=4,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(10.0, 50.0, 0.01, 0.01),
        ) as dst:
            dst.write(np.ones((4, 4), "float32"), 1)
        raw = mf.read()
    return _serde.build_tile(raw, "GTiff", 0)


def _func(udf):
    return getattr(udf, "_func", None) or udf.func


def _unwrap(obj):
    return getattr(obj, "_f", obj)


def test_fitresolution_udf_hands_its_dirs_to_the_env():
    udf = prx._build_h3_fitresolution_udf(DIRS)
    assert _func(udf)(_tile()) is not None
    assert CALLS == [DIRS]


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
def test_partial_udf_hands_its_dirs_to_the_env(grid):
    udf = prx._build_rastertogrid_partial_udf(grid, DIRS)
    res = {"bng": "1km"}.get(grid, 5)
    try:
        _func(udf)(_tile(), res)
    except (
        Exception
    ):  # noqa: BLE001 - a tile outside the grid may raise; the env call comes first
        pass
    assert CALLS and CALLS[0] == DIRS


def test_custom_partial_udf_carries_its_dirs():
    udf = prx._build_rastertogrid_partial_udf("custom", DIRS)
    cells = [c.cell_contents for c in _func(udf).__closure__]
    assert DIRS in cells


@pytest.mark.parametrize("grid,res", [("h3", 5), ("quadbin", 5), ("bng", "1km")])
def test_udtf_hands_its_dirs_to_the_env(grid, res):
    base = _unwrap(getattr(prx, f"_Rst{grid.capitalize()}RasterToGridAvgUDTF"))
    udtf = base.with_grid_dirs(DIRS)
    list(udtf._cls().eval(_tile(), res))
    assert CALLS and CALLS[0] == DIRS


def test_udtf_without_dirs_keeps_the_driver_default():
    base = _unwrap(prx._RstH3RasterToGridAvgUDTF)
    list(base._cls().eval(_tile(), 5))
    assert CALLS and CALLS[0] is None


def _rows_to_carry():
    fns = manifest.load().functions
    return sorted(
        n
        for n in fns
        if n == "gbx_rst_h3_fitresolution"
        or (
            "_rastertogrid" in n
            and n.startswith("gbx_rst_")
            and "L3" in fns[n].levels
            and fns[n].levels["L3"].get("register") in ("udf", "udtf")
        )
    )


def test_every_row_that_reads_the_raster_declares_the_dirs():
    rows = _rows_to_carry()
    assert len(rows) == 1 + 4 + 32
    fns = manifest.load().functions
    missing = [
        n for n in rows if fns[n].levels["L3"].get("factory_args") != "proj_grid_dirs"
    ]
    assert not missing


def test_registration_builds_the_udf_with_the_registered_dirs():
    from databricks.labs.gbx.manager import l3

    proj_grids.set_registered_dirs(["/Volumes/reg/grids"], replace=True)
    try:
        fn = manifest.load().functions["gbx_rst_h3_fitresolution"]
        built = manifest.resolve_ref(fn.levels["L3"]["factory"])(
            *l3._factory_args(fn.levels["L3"]["factory_args"])
        )
        assert _func(built)(_tile()) is not None
        assert CALLS == [("/Volumes/reg/grids",)]
    finally:
        proj_grids.set_registered_dirs([], replace=True)


def test_registering_a_table_function_builds_it_with_the_registered_dirs(
    spark, monkeypatch
):
    """The registrar hands the driver's dirs to a raster-to-grid table function: a live SQL binding carries them."""
    from databricks.labs.gbx import _transport, manager

    seen = []
    real = _transport.SessionTileUdtf.with_grid_dirs

    def spy(self, dirs):
        seen.append(tuple(dirs))
        return real(self, dirs)

    monkeypatch.setattr(_transport.SessionTileUdtf, "with_grid_dirs", spy)
    proj_grids.set_registered_dirs(["/Volumes/reg/grids"], replace=True)
    try:
        manager.register_family(
            spark, ("gbx_rst_",), only=["gbx_rst_h3_rastertogridavg"], level="L3"
        )
    finally:
        proj_grids.set_registered_dirs([], replace=True)
    assert seen == [("/Volumes/reg/grids",)]
