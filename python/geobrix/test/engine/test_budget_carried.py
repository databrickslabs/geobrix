"""The GDAL spec engine on a worker of standard access mode (Spark Connect over classic compute): it sees no memory
signal of its own, so its own budget is Serverless's 64 MiB, and the read cap the registration carried (the classic
1536 MiB) is the one its read cap and its decoded budget judge by. Skips where ``osgeo`` is not rasterio's GDAL.
"""

import json

import numpy as np
import pytest
from rasterio.io import MemoryFile

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import l3, run

SUMMARY = manifest.load().functions["gbx_rst_summary"]


@pytest.fixture(autouse=True)
def _engine():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)


def _standard_access_worker(monkeypatch):
    """A worker that sees no memory signal of its own (standard access mode: Connect over classic compute)."""
    from databricks.labs.gbx.pyrx.core import budget

    for k in (*budget.CLASSIC_MEMORY_ENV, "IS_SERVERLESS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(budget, "_cgroup_task_limit_bytes", lambda: None)
    monkeypatch.setattr(
        budget, "process_read_cap", lambda: budget.decoded_budget_bytes("auto")
    )
    assert budget.decoded_budget_bytes("auto") == 64 << 20


def _big_tile(path=None):
    """A Float32 tile of 100 MiB decoded (5120 x 5120), a few KiB of DEFLATE bytes."""
    from affine import Affine

    kw = dict(
        driver="GTiff",
        width=5120,
        height=5120,
        count=1,
        dtype="float32",
        crs="EPSG:3857",
        transform=Affine(1, 0, 0, 0, -1, 5120),
        compress="deflate",
        tiled=True,
        nodata=-1.0,
    )
    z = np.zeros((1, 5120, 5120), "float32")
    if path is not None:
        import rasterio

        with rasterio.open(path, "w", **kw) as ds:
            ds.write(z)
        return path
    with MemoryFile() as mf:
        with mf.open(**kw) as ds:
            ds.write(z)
        return mf.read()


def test_a_standard_access_worker_judges_the_call_by_the_carried_cap(monkeypatch):
    from databricks.labs.gbx import _gdal_policy

    _standard_access_worker(monkeypatch)
    raw = _big_tile()
    alone = run.run(SUMMARY, [raw], {})
    assert (
        alone.error and "(64 MiB)" in alone.error
    )  # nothing carried: the worker's own read cap, 64 MiB
    with _gdal_policy.carried(None, 1536 << 20):
        res = run.run(SUMMARY, [raw], {})
    assert res.error is None and json.loads(res.text)["size"] == [5120, 5120]


def test_a_standard_access_worker_reads_a_virtual_tile_under_the_carried_cap(
    monkeypatch, tmp_path
):
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    _standard_access_worker(monkeypatch)
    tile = VirtualTile(cellid=0, path=_big_tile(str(tmp_path / "big.tif"))).to_row()
    with pytest.raises(run.DataError, match="read cap"):
        l3._inputs(
            SUMMARY, tile, 64 << 20
        )  # the driver's connect-aware cap, nothing carried
    with _gdal_policy.carried(None, 1536 << 20):
        rasters, _p, _c = l3._inputs(SUMMARY, tile, 64 << 20)
    assert rasters and bytes(rasters[0][:4]) in run._TIFF_MAGIC


def test_a_worker_that_knows_its_memory_keeps_its_own_budget(monkeypatch):
    """A read cap raised on the driver does not raise what one call may hold where the worker can tell its budget."""
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.pyrx.core import budget

    _standard_access_worker(monkeypatch)
    monkeypatch.setenv(budget.CLASSIC_MEMORY_ENV[0], "16g")
    with _gdal_policy.carried(None, 4096 << 20):
        assert budget.call_decoded_budget() == budget.decoded_budget_bytes("classic")
    monkeypatch.delenv(budget.CLASSIC_MEMORY_ENV[0])
    with _gdal_policy.carried(None, 4096 << 20):
        assert (
            budget.call_decoded_budget() == 4096 << 20
        )  # standard access: the carried cap
