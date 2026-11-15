"""The ladder's blocks recipe (ZL-W6): read blocks, compute wind speed per block, write a plain store.

The compute is a column UDF, so the writer's first pass over the block positions does not run it.
"""

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from databricks.labs.gbx.bench import zarr_ladder as zl  # noqa: E402
from databricks.labs.gbx.bench import zarr_ladder_write as zw  # noqa: E402
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    return zl.generate(
        str(tmp_path_factory.mktemp("ladder")), "county_wind", "10MB", workers=2
    )["path"]


def test_the_recipe_writes_the_wind_speed_of_each_cell(spark_delta, store, tmp_path):
    out = str(tmp_path / "w6.zarr")
    gbx_zarr_write(
        zw.blocks_source(spark_delta, store),
        out,
        transactional=False,
        spark=spark_delta,
    )
    src = xr.open_zarr(store, chunks=None)
    got = xr.open_zarr(out, chunks=None)
    want = np.sqrt(
        src["u10"].values.astype("float64") ** 2
        + src["v10"].values.astype("float64") ** 2
    )
    assert got["wind_speed"].dims == src["u10"].dims
    np.testing.assert_allclose(got["wind_speed"].values, want, rtol=1e-5)
    for v in ("t2m", "u10", "v10"):
        assert np.array_equal(got[v].values, src[v].values)


def test_the_position_pass_does_not_run_the_compute(spark_delta, store):
    """The first pass selects positions only; a Python stage over the blocks (mapInArrow) would run for every block."""
    from databricks.labs.gbx.zarr._write import _blocks_frame

    frame = _blocks_frame(zw.blocks_source(spark_delta, store), None)
    plan = (
        frame.select("variable", "start", "stop")
        ._jdf.queryExecution()
        .executedPlan()
        .toString()
    )
    assert "ArrowEvalPython" not in plan and "PythonMapInArrow" not in plan
    # the write pass, which needs the bytes, does run it
    full = frame._jdf.queryExecution().executedPlan().toString()
    assert "ArrowEvalPython" in full
