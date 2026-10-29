"""Writing GeoBrix tiles into Zarr: ``gbx_zarr_write`` with a tile column.

The grid rules run without Spark (``place`` and the planner are pure functions of a tile's
facts); the Spark tests keep one run per behaviour: tiles read from a store, changed by an
``rst_*`` function and written back, a stack of GeoTIFFs made into a cube, the refusals,
and the plan on a Spark Connect client.
"""

import dataclasses
import uuid

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from test.zarr.test_write import _create, _dataset, _read  # noqa: E402

import pandas as pd  # noqa: E402

from databricks.labs.gbx.zarr import _write  # noqa: E402
from databricks.labs.gbx.zarr import _write_tiles as WT  # noqa: E402
from databricks.labs.gbx.zarr._read import gbx_zarr_read  # noqa: E402
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import ZarrError, ZarrGridMismatch  # noqa: E402


@pytest.fixture
def drop(spark_delta):
    made = []
    yield made.append
    for t in made:
        spark_delta.sql(f"DROP TABLE IF EXISTS {t}")


@pytest.fixture
def no_hooks(monkeypatch):
    monkeypatch.setattr(_write, "_FAULT", None)
    monkeypatch.setattr(_write, "_BEFORE_COMMIT", None)


# --- a stack of small GeoTIFFs (no provenance) ---------------------------------------------


def gtiff_tile(
    values, *, origin=(10.0, 50.0), cell=0.5, crs="EPSG:4326", nodata=None, k=0
):
    """A materialized tile struct (a dict) holding a GeoTIFF of ``values`` (rows x cols)."""
    from rasterio.transform import Affine

    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx.core.compression import write_gtiff

    data = np.asarray(values)[None]
    profile = {
        "driver": "GTiff",
        "width": data.shape[2],
        "height": data.shape[1],
        "count": 1,
        "dtype": data.dtype.name,
        "crs": crs,
        "transform": Affine(cell, 0, origin[0], 0, -cell, origin[1]),
    }
    if nodata is not None:
        profile["nodata"] = nodata
    return _serde.build_tile(write_gtiff(profile, data), "GTiff", k)


def hourly(n=3, start="2024-03-01", **kw):
    """``(tile, acq_time)`` rows: ``n`` hourly tiles of the same 6 x 8 grid, step ``k`` holding
    ``100 * k + cell index``."""
    base = np.arange(48, dtype="float32").reshape(6, 8)
    times = pd.date_range(start, periods=n, freq="h")
    return [
        (gtiff_tile(base + 100 * k, k=k, **kw), times[k].to_pydatetime())
        for k in range(n)
    ]


def tiles_df(spark, rows):
    from pyspark.sql import types as T

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    schema = T.StructType(
        [
            T.StructField("tile", V2_TILE_SCHEMA),
            T.StructField("acq_time", T.TimestampType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


# --- the grid rules, without Spark -----------------------------------------------------------


def _fact(**kw):
    base = dict(
        variable=None,
        gt=[10.0, 0.5, 0.0, 50.0, 0.0, -0.5],
        crs="EPSG:4326",
        height=6,
        width=8,
        count=1,
        dtype="float32",
        nodata=None,
        slices={},
        band_dim=None,
        bands=[],
        plane_gt=None,
        plane_crs=None,
        dims={"time": ["M", 0]},
    )
    base.update(kw)
    return WT.Fact(**base)


def _target(**kw):
    base = dict(
        name="v",
        dims=("time", "latitude", "longitude"),
        y=1,
        x=2,
        gt=(10.0, 0.5, 0.0, 50.0, 0.0, -0.5),
        crs="EPSG:4326",
        height=6,
        width=8,
        flip_y=False,
        flip_x=False,
        slack=0.0,
        maps={"time": {("M", 0): 0}},
        existing=True,
    )
    base.update(kw)
    return WT.Target(**base)


# one row per way a tile can fail to fit its grid: the change, and what the error says
GRID_MISMATCHES = {
    "other crs": (dict(crs="EPSG:3857"), {}, "3857"),
    "other cell size": (dict(gt=[10.0, 0.25, 0.0, 50.0, 0.0, -0.25]), {}, "cells of"),
    "origin off the cell edges": (
        dict(gt=[10.25, 0.5, 0.0, 50.0, 0.0, -0.5]),
        {},
        "not aligned",
    ),
    "beyond the grid": (
        dict(gt=[12.0, 0.5, 0.0, 50.0, 0.0, -0.5]),
        {},
        "outside the array",
    ),
    "rotated": (dict(gt=[10.0, 0.5, 0.1, 50.0, 0.0, -0.5]), {}, "rotated"),
    "from another plane": (
        dict(
            plane_gt=[9.0, 0.5, 0.0, 50.0, 0.0, -0.5],
            plane_crs="EPSG:4326",
            variable="v",
        ),
        {},
        "come from a grid",
    ),
}


@pytest.mark.parametrize("name", GRID_MISMATCHES)
def test_grid_mismatch_raises(name):
    change, target_change, says = GRID_MISMATCHES[name]
    t = _target(**target_change)
    assert [s[0] for s in WT.place(_fact(), t)] == [0]  # the unchanged tile fits
    with pytest.raises(ZarrGridMismatch, match=says) as e:
        WT.place(_fact(**change), t)
    assert "Fix:" in str(e.value)


def test_a_new_store_takes_its_grid_from_the_tiles_and_rejects_the_odd_one():
    a = _fact(dims={"time": ["M", 0]})
    b = _fact(dims={"time": ["M", 1]}, gt=[11.0, 0.5, 0.0, 49.0, 0.0, -0.5])
    plan = WT.plan_write(
        [a, b],
        mode="create",
        dim_cols={"time": "t"},
        names=None,
        append_dim=None,
        store=None,
    )
    t = plan.targets["data"]
    # the extent of both tiles (b lies 2 rows down and 2 columns right of a), from the
    # north-west corner
    assert (t.gt, t.height, t.width, t.dims) == (
        (10.0, 0.5, 0.0, 50.0, 0.0, -0.5),
        8,
        10,
        ("time", "latitude", "longitude"),
    )
    odd = dataclasses.replace(b, gt=[11.3, 0.5, 0.0, 49.0, 0.0, -0.5])
    with pytest.raises(ZarrGridMismatch, match="not aligned"):
        WT.plan_write(
            [a, odd],
            mode="create",
            dim_cols={"time": "t"},
            names=None,
            append_dim=None,
            store=None,
        )


# --- Spark: tiles read from a store, changed, written back -----------------------------------


@pytest.mark.parametrize(
    "carry, latitude",
    [("provenance", "descending"), ("dim_cols", "ascending")],  # the second flips rows
)
def test_round_trip_tiles_after_rst_op(
    spark_delta, drop, tmp_path, no_hooks, carry, latitude
):
    """Four hourly steps are read as tiles, doubled by ``rst_mapalgebra`` (which returns a
    fresh tile: the provenance has to come back, or ``dim_cols`` say where the tile goes),
    and written with ``update``: the cube holds twice the values there, the rest as before.
    Tiles are north-up whichever way the store's latitude runs.
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as prx

    ds = _dataset()
    if latitude == "ascending":
        ds = ds.isel(latitude=slice(None, None, -1))
    table, _, v1 = _create(spark_delta, drop, tmp_path, ds)
    tiles = gbx_zarr_read(
        spark_delta,
        table,
        variables=["t2m"],
        isel={"time": slice(0, 4)},
        output="tiles",
    )
    df = tiles.select(
        "tile",
        F.col("tile.metadata").alias("provenance"),
        F.to_timestamp(F.col("tile.metadata")["sliceCoord_time"]).alias("t"),
    )
    doubled = df.withColumn("tile", prx.rst_mapalgebra(F.array("tile"), F.lit("A*2")))
    if carry == "provenance":  # the tile is a GeoTIFF with the provenance put back
        out = doubled.withColumn(
            "tile", F.col("tile").withField("metadata", F.col("provenance"))
        ).select("tile")
        kw = {}
    else:
        out = doubled.select("tile", "t")
        kw = dict(dim_cols={"time": "t"}, variables="t2m")
    v2 = gbx_zarr_write(out, table, mode="update", **kw)
    assert v2 > v1
    want = ds.t2m.values.copy()
    want[:4] *= 2
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), want)
    np.testing.assert_array_equal(_read(spark_delta, table, "u10"), ds.u10.values)


def test_band_stack_tiles_land_on_their_dimension(
    spark_delta, drop, tmp_path, no_hooks
):
    """Tiles read with ``band_dim`` write back one band per step, into another store on
    the same grid (its grid is compared, not its name)."""
    ds = _dataset()
    src, _, _ = _create(spark_delta, drop, tmp_path, ds, label="src")
    zeros = ds.assign(t2m=ds.t2m * 0, u10=ds.u10 * 0)
    dst, _, _ = _create(spark_delta, drop, tmp_path, zeros, label="dst")
    tiles = gbx_zarr_read(
        spark_delta,
        src,
        variables=["t2m"],
        isel={"time": slice(0, 3)},
        band_dim="time",
        output="tiles",
    )
    gbx_zarr_write(tiles, dst, mode="update")
    want = np.zeros_like(ds.t2m.values)
    want[:3] = ds.t2m.values[:3]
    np.testing.assert_array_equal(_read(spark_delta, dst, "t2m"), want)


def test_values_are_cast_to_the_arrays_type_and_nodata_becomes_its_fill(
    spark_delta, drop, tmp_path, no_hooks
):
    time = pd.date_range("2024-03-01", periods=2, freq="h")
    ds = xr.Dataset(
        {"n": (("time", "latitude", "longitude"), np.zeros((2, 6, 8), "float32"))},
        coords={
            "time": time,
            "latitude": 50.0 - 0.25 - 0.5 * np.arange(6),
            "longitude": 10.25 + 0.5 * np.arange(8),
        },
    )
    ds["n"].encoding = {"dtype": "int16", "_FillValue": -1}
    chunks = {"time": 1, "latitude": 6, "longitude": 8}
    table, _, _ = _create(spark_delta, drop, tmp_path, ds, chunks=chunks)
    values = np.full((6, 8), 0.4, dtype="float32")
    values[0] = [2.5, 3.5, 4.5, -9999.0, 0.4, 1.6, -2.5, 7.0]
    rows = [(gtiff_tile(values, nodata=-9999.0), time[0].to_pydatetime())]
    gbx_zarr_write(
        tiles_df(spark_delta, rows),
        table,
        mode="update",
        dim_cols={"time": "acq_time"},
        variables="n",
    )
    got = _read(spark_delta, table, "n")  # decoded: the fill reads as NaN
    nan = np.nan
    np.testing.assert_array_equal(
        got[0, 0], [2, 4, 4, nan, 0, 2, -2, 7]
    )  # half to even
    assert (got[0, 1:] == 0).all() and (got[1] == 0).all()


# --- Spark: a stack of GeoTIFFs becomes a cube ----------------------------------------------


GRIDS = {
    "geographic": dict(crs="EPSG:4326", origin=(10.0, 50.0), cell=0.5, y="latitude"),
    "projected": dict(crs="EPSG:32633", origin=(500000.0, 5000000.0), cell=30.0, y="y"),
}


@pytest.mark.parametrize("grid", GRIDS)
def test_cog_stack_to_cube_with_dim_cols(spark_delta, drop, tmp_path, no_hooks, grid):
    g = GRIDS[grid]
    kw = dict(crs=g["crs"], origin=g["origin"], cell=g["cell"])
    table, root = f"gbx_zarr_tiles_{uuid.uuid4().hex[:8]}", tmp_path / "cube"
    drop(table)
    rows = hourly(3, **kw)
    gbx_zarr_write(
        tiles_df(spark_delta, rows),
        table,
        root=str(root),
        dim_cols={"time": "acq_time"},
    )
    base = np.arange(48, dtype="float32").reshape(6, 8)
    cube = _read(spark_delta, table, "data")
    assert cube.shape == (3, 6, 8) and cube.dtype == np.float32
    for k in range(3):
        np.testing.assert_array_equal(cube[k], base + 100 * k)
    np.testing.assert_array_equal(
        _read(spark_delta, table, "time"),
        np.array([r[1] for r in rows], dtype="datetime64[ns]"),
    )
    cell, (ox, oy) = g["cell"], g["origin"]
    np.testing.assert_allclose(
        _read(spark_delta, table, g["y"]), oy - cell / 2 - cell * np.arange(6)
    )
    # the new store reads back as tiles on the grid and in the CRS the GeoTIFFs had
    back = gbx_zarr_read(
        spark_delta, table, variables=["data"], isel={"time": 0}, output="tiles"
    ).first()
    assert back.tile.metadata["zarr_gt"] == f"{ox!r},{cell!r},0.0,{oy!r},0.0,{-cell!r}"
    assert back.tile.metadata["zarr_crs"] == g["crs"]
    # an append extends the time dimension with the next hour's tile
    gbx_zarr_write(
        tiles_df(spark_delta, hourly(4, **kw)[3:]),
        table,
        mode="append",
        dim_cols={"time": "acq_time"},
        variables="data",
    )
    grown = _read(spark_delta, table, "data")
    assert grown.shape == (4, 6, 8)
    np.testing.assert_array_equal(grown[3], base + 300)
    assert str(_read(spark_delta, table, "time")[3]) == "2024-03-01T03:00:00.000000000"


def test_dim_cols_required_without_provenance(spark_delta, drop, tmp_path, no_hooks):
    table = f"gbx_zarr_tiles_{uuid.uuid4().hex[:8]}"
    drop(table)
    stack = tiles_df(spark_delta, hourly(2))
    with pytest.raises(ZarrError, match="dim_cols") as e:  # two tiles on the same cells
        gbx_zarr_write(stack, table, root=str(tmp_path / "a"))
    assert "Fix:" in str(e.value)
    # into a cube that has a time dimension, a tile with no provenance has no time position
    cube, _, _ = _create(spark_delta, drop, tmp_path, _dataset(), label="c")
    one = tiles_df(
        spark_delta,
        [
            (
                gtiff_tile(
                    np.zeros((10, 12), "float32"), origin=(-0.5, 45.5), cell=1.0
                ),
                hourly(1)[0][1],
            )
        ],
    )
    with pytest.raises(ZarrError, match="no position along 'time'"):
        gbx_zarr_write(one.select("tile"), cube, mode="update", variables="t2m")
    with pytest.raises(ValueError, match="variables"):  # and no variable to name it
        gbx_zarr_write(one.select("tile"), cube, mode="update")
    with pytest.raises(ValueError, match="holds none"):
        gbx_zarr_write(_dataset(), cube, mode="update", dim_cols={"time": "t"})


def test_a_tile_off_the_grid_is_refused_on_the_driver(
    spark_delta, drop, tmp_path, no_hooks
):
    """A mismatch surfaces as ``ZarrGridMismatch`` from the call itself, before any file."""
    table = f"gbx_zarr_tiles_{uuid.uuid4().hex[:8]}"
    drop(table)
    rows = hourly(2)
    rows[1] = (
        hourly(2, cell=0.25)[1][0],
        rows[1][1],
    )  # the second tile has finer cells
    root = tmp_path / "never"
    with pytest.raises(ZarrGridMismatch, match="cell sizes") as e:
        gbx_zarr_write(
            tiles_df(spark_delta, rows),
            table,
            root=str(root),
            dim_cols={"time": "acq_time"},
        )
    assert "Fix:" in str(e.value) and not root.exists()
    # tiles read from one store are not tiles of another store's grid
    coarse = _dataset(ny=5, nx=6)
    cube, _, _ = _create(spark_delta, drop, tmp_path, coarse, label="coarse")
    ds = _dataset()
    other, _, _ = _create(spark_delta, drop, tmp_path, ds, label="fine")
    tiles = gbx_zarr_read(
        spark_delta, other, variables=["t2m"], isel={"time": 0}, output="tiles"
    )
    with pytest.raises(ZarrGridMismatch, match="come from a grid"):
        gbx_zarr_write(tiles, cube, mode="update")


# --- Spark Connect ----------------------------------------------------------------------------


def test_the_tile_plan_on_a_spark_connect_client(spark_connect_local):
    """The plan shapes the write uses (``mapInArrow`` over a tile struct with a map, a small
    Arrow frame unioned to it, ``toArrow``) on a Connect client, the session type of
    Serverless. Run alone: ``--path .../test_write_tiles.py -k connect``."""
    from databricks.labs.gbx.zarr._write import _blocks_struct

    df = tiles_df(spark_connect_local, hourly(3))
    prep = WT.prepare(
        df,
        "tile",
        mode="create",
        dim_cols={"time": "acq_time"},
        names=None,
        append_dim=None,
        store=None,
        schema=_blocks_struct(),
    )
    rows = prep.frame.collect()
    assert len(rows) == 3 and {r.variable for r in rows} == {"data"}
    assert sorted(r.start[0] for r in rows) == [0, 1, 2]
    assert [c["variable"] for c in prep.coords] == ["time", "latitude", "longitude"]
    assert prep.seen.num_rows == 3 + 3
