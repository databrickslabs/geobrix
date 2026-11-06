"""A table of cells into Zarr: ``gbx_zarr_write`` with ``dim_cols`` over a table with one row
per cell (a column per dimension, a column per variable).

The contract is that the store equals the one written from the same data as an
``xarray.Dataset``: create and append, manifest and plain. The rest is what a table can do
that a cube cannot (miss cells, repeat them, carry nulls) and the shape of the work: one
shuffle, no staging table, a partition per unit touched, a unit that fits in a task.
"""

import numpy as np
import pandas as pd
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from test.zarr.test_write import CHUNKS, _create, _dataset, _read  # noqa: E402

from databricks.labs.gbx.zarr import _write, _write_cells  # noqa: E402
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import ZarrError  # noqa: E402

DIM_COLS = {"time": "time", "latitude": "latitude", "longitude": "longitude"}
SHARDS = {"time": 24, "latitude": 10, "longitude": 12}


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


def _pdf(ds, names=("t2m", "u10")):
    """One row per cell, in no particular order."""
    pdf = ds[list(names)].to_dataframe().reset_index()
    return pdf.sample(frac=1.0, random_state=1).reset_index(drop=True)


def _cells(spark, ds, names=("t2m", "u10"), keep=None, parts=3):
    pdf = _pdf(ds, names)
    if keep is not None:
        pdf = pdf[keep(pdf)]
    return spark.createDataFrame(pdf).repartition(parts)


def _coords(ds):
    return {
        "time": ds["time"].values,
        "latitude": ds["latitude"].values,
        "longitude": ds["longitude"].values,
    }


def _plain_read(path):
    return xr.open_zarr(path).load()


# --- the store equals the cube's -------------------------------------------------------


def test_create_from_cells_equals_the_cube(spark_delta, drop, tmp_path, no_hooks):
    """Sharded, with the cube's own labels (latitude descends): every cell and label."""
    ds = _dataset()
    table, root, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(spark_delta, ds),
        label="cells",
        dim_cols=DIM_COLS,
        coords=_coords(ds),
        shards=SHARDS,
    )
    for n in ("t2m", "u10"):
        np.testing.assert_array_equal(_read(spark_delta, table, n), ds[n].values)
    np.testing.assert_array_equal(
        _read(spark_delta, table, "latitude"), ds["latitude"].values
    )
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds["time"].values)


def test_create_derives_ascending_labels_when_none_are_given(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset(hours=24)
    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(spark_delta, ds),
        label="derived",
        dim_cols=DIM_COLS,
    )
    want = ds.sortby("latitude")
    np.testing.assert_array_equal(
        _read(spark_delta, table, "latitude"), want["latitude"].values
    )
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), want["t2m"].values)


def test_plain_create_from_cells_equals_the_cube(spark_delta, tmp_path, no_hooks):
    ds = _dataset()
    out = tmp_path / "cells.zarr"
    result = gbx_zarr_write(
        _cells(spark_delta, ds),
        str(out),
        transactional=False,
        dim_cols=DIM_COLS,
        coords=_coords(ds),
        chunks=CHUNKS,
        shards=SHARDS,
        spark=spark_delta,
    )
    assert result["chunks"] > 0
    got = _plain_read(out)
    for n in ("t2m", "u10"):
        np.testing.assert_array_equal(got[n].values, ds[n].values)
    np.testing.assert_array_equal(got["latitude"].values, ds["latitude"].values)


def test_unsharded_create_cuts_by_chunk(spark_delta, drop, tmp_path, no_hooks):
    ds = _dataset(hours=24)
    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(spark_delta, ds),
        label="unsharded",
        dim_cols=DIM_COLS,
        coords=_coords(ds),
        shards=False,
    )
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds["t2m"].values)


@pytest.mark.parametrize("plain", [False, True], ids=["manifest", "plain"])
def test_append_a_day_from_cells_lands_off_the_shard_grid(
    spark_delta, drop, tmp_path, no_hooks, plain
):
    """36 hours in shards of 24, then hours 36..60: the append starts inside a shard."""
    full = _dataset(hours=60)
    first, day = full.isel(time=slice(0, 36)), full.isel(time=slice(36, 60))
    if plain:
        out = str(tmp_path / "app.zarr")
        gbx_zarr_write(
            _cells(spark_delta, first),
            out,
            transactional=False,
            dim_cols=DIM_COLS,
            coords=_coords(first),
            chunks=CHUNKS,
            shards=SHARDS,
            spark=spark_delta,
        )
        gbx_zarr_write(
            _cells(spark_delta, day),
            out,
            mode="append",
            transactional=False,
            dim_cols=DIM_COLS,
            append_dim="time",
            spark=spark_delta,
        )
        got = _plain_read(out)
        for n in ("t2m", "u10"):
            np.testing.assert_array_equal(got[n].values, full[n].values)
        np.testing.assert_array_equal(got["time"].values, full["time"].values)
    else:
        table, _, _ = _create(
            spark_delta,
            drop,
            tmp_path,
            _cells(spark_delta, first),
            label="app",
            dim_cols=DIM_COLS,
            coords=_coords(first),
            shards=SHARDS,
        )
        gbx_zarr_write(
            _cells(spark_delta, day),
            table,
            mode="append",
            dim_cols=DIM_COLS,
            append_dim="time",
            spark=spark_delta,
        )
        for n in ("t2m", "u10"):
            np.testing.assert_array_equal(_read(spark_delta, table, n), full[n].values)
        np.testing.assert_array_equal(
            _read(spark_delta, table, "time"), full["time"].values
        )


# --- cells a table can lack, repeat or null ----------------------------------------------


def test_missing_cells_read_as_the_fill_value(spark_delta, drop, tmp_path, no_hooks):
    """A shard with no cells at all, and scattered holes elsewhere."""
    ds = _dataset()
    rng = np.random.default_rng(3)
    hole = rng.random(len(_pdf(ds))) < 0.2

    def keep(pdf):
        first_shard = pdf["time"] < pdf["time"].min() + pd.Timedelta(hours=24)
        return ~(first_shard & (pdf["latitude"] >= 41.0)) & ~pd.Series(hole)

    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(spark_delta, ds, keep=keep),
        label="holes",
        dim_cols=DIM_COLS,
        coords=_coords(ds),
        shards=SHARDS,
    )
    pdf = _pdf(ds)
    gone = ~keep(pdf).to_numpy()
    want = np.full(ds["t2m"].shape, np.nan, np.float32)
    kept = pdf[~gone]
    ti = ((kept["time"] - ds["time"].values[0]) / np.timedelta64(1, "h")).astype(int)
    yi = np.abs(
        ds["latitude"].values[None, :] - kept["latitude"].values[:, None]
    ).argmin(1)
    xi = kept["longitude"].astype(int).to_numpy()
    want[ti, yi, xi] = kept["t2m"].to_numpy()
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), want)


def test_integer_columns_keep_their_type_and_missing_cells_are_zero(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset(hours=24)
    ds["count"] = (
        ds["t2m"] * 0 + np.arange(ds["t2m"].size).reshape(ds["t2m"].shape)
    ).astype("int32")
    ds["wind"] = ds["u10"].astype("float64")
    table, root, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(
            spark_delta,
            ds,
            names=("count", "wind"),
            keep=lambda p: p["longitude"] != 3.0,
        ),
        label="types",
        dim_cols=DIM_COLS,
        coords=_coords(ds),
    )
    count, wind = _read(spark_delta, table, "count"), _read(spark_delta, table, "wind")
    assert count.dtype == np.int32 and wind.dtype == np.float64
    want = ds["count"].values.copy()
    want[:, :, 3] = 0
    np.testing.assert_array_equal(count, want)
    assert np.isnan(wind[:, :, 3]).all()
    np.testing.assert_array_equal(
        np.delete(wind, 3, axis=2), np.delete(ds["wind"].values, 3, axis=2)
    )


def test_a_null_value_is_a_missing_cell_and_a_null_label_is_refused(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset(hours=12)
    pdf = _pdf(ds).copy()
    pdf.loc[pdf.index[:5], "t2m"] = np.nan
    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        spark_delta.createDataFrame(pdf),
        label="nulls",
        dim_cols=DIM_COLS,
        coords=_coords(ds),
    )
    assert np.isnan(_read(spark_delta, table, "t2m")).sum() == 5
    bad = spark_delta.createDataFrame(_pdf(ds)).selectExpr(
        "CASE WHEN longitude = 2 THEN NULL ELSE longitude END AS longitude",
        "time",
        "latitude",
        "t2m",
    )
    with pytest.raises(Exception, match="null value"):
        gbx_zarr_write(
            bad,
            str(tmp_path / "bad.zarr"),
            transactional=False,
            dim_cols=DIM_COLS,
            spark=spark_delta,
        )


@pytest.mark.parametrize("plain", [False, True], ids=["manifest", "plain"])
def test_duplicate_cells_are_refused_before_anything_is_committed(
    spark_delta, drop, tmp_path, no_hooks, plain
):
    ds = _dataset(hours=24)
    pdf = _pdf(ds)
    twice = spark_delta.createDataFrame(pd.concat([pdf, pdf.iloc[:3]]))
    with pytest.raises(
        Exception, match=r"share a position.*Fix: keep one row per cell"
    ):
        if plain:
            gbx_zarr_write(
                twice,
                str(tmp_path / "dup.zarr"),
                transactional=False,
                dim_cols=DIM_COLS,
                spark=spark_delta,
            )
        else:
            table = "gbx_zarr_write_dup_cells"
            drop(table)
            gbx_zarr_write(
                twice,
                table,
                root=str(tmp_path / "dup"),
                dim_cols=DIM_COLS,
                spark=spark_delta,
            )
    if plain:
        assert not (tmp_path / "dup.zarr" / "zarr.json").exists()
    else:
        assert not spark_delta.catalog.tableExists("gbx_zarr_write_dup_cells") or (
            spark_delta.sql("SELECT count(*) FROM gbx_zarr_write_dup_cells").first()[0]
            == 0
        )


def test_append_to_a_label_the_store_lacks_is_refused(
    spark_delta, drop, tmp_path, no_hooks
):
    full = _dataset(hours=36)
    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(spark_delta, full.isel(time=slice(0, 12))),
        label="unk",
        dim_cols=DIM_COLS,
        coords=_coords(full.isel(time=slice(0, 12))),
    )
    pdf = _pdf(full.isel(time=slice(12, 24)))
    pdf["latitude"] = pdf["latitude"] + 0.3
    with pytest.raises(Exception, match="no 'latitude' label"):
        gbx_zarr_write(
            spark_delta.createDataFrame(pdf),
            table,
            mode="append",
            dim_cols=DIM_COLS,
            append_dim="time",
            spark=spark_delta,
        )


def test_update_and_overwrite_take_blocks_not_cells(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset(hours=12)
    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(spark_delta, ds),
        label="upd",
        dim_cols=DIM_COLS,
        coords=_coords(ds),
    )
    with pytest.raises(ValueError, match=r"'create' or 'append'.*Fix"):
        gbx_zarr_write(
            _cells(spark_delta, ds),
            table,
            mode="update",
            dim_cols=DIM_COLS,
            spark=spark_delta,
        )


def test_coords_belong_to_a_create_of_cells(spark_delta, tmp_path, no_hooks):
    ds = _dataset(hours=12)
    with pytest.raises(ValueError, match="coords applies to mode='create' only"):
        gbx_zarr_write(
            _cells(spark_delta, ds),
            str(tmp_path / "x.zarr"),
            mode="append",
            transactional=False,
            dim_cols=DIM_COLS,
            coords=_coords(ds),
            spark=spark_delta,
        )
    with pytest.raises(ValueError, match="coords gives the labels of a table of cells"):
        gbx_zarr_write(
            ds,
            str(tmp_path / "y.zarr"),
            transactional=False,
            coords=_coords(ds),
            spark=spark_delta,
        )


# --- the shape of the work ---------------------------------------------------------------


def _tables(spark):
    return {r["tableName"] for r in spark.sql("SHOW TABLES").collect()}


def _prepare(spark, df, **kw):
    args = _write_cells.CellsArgs(
        coords=kw.pop("coords", None), chunks=CHUNKS, shards=SHARDS
    )
    return _write._prepare_tiles(
        spark, df, (None, DIM_COLS), "create", None, None, None, args
    )


def test_one_partition_per_unit_touched_capped_by_the_parallelism(
    spark_delta, no_hooks, monkeypatch
):
    """48 h x 10 x 12 in shards of 24 x 10 x 12: two units, so two partitions; with the
    parallelism at 1 the shuffle has one."""
    ds = _dataset()
    prep = _prepare(spark_delta, _cells(spark_delta, ds), coords=_coords(ds))
    assert prep.frame.rdd.getNumPartitions() == 2
    monkeypatch.setattr(_write, "_shuffle_partitions", lambda spark: 1)
    prep = _prepare(spark_delta, _cells(spark_delta, ds), coords=_coords(ds))
    assert prep.frame.rdd.getNumPartitions() == 1


def test_complete_coords_skip_the_survey_of_the_table(
    spark_delta, tmp_path, no_hooks, monkeypatch
):
    """Fail-on-revert: with every label fixed and ``complete=True`` the table is read once
    (the data pass), not once more to find its labels or its units."""

    def scanned(*a, **k):
        raise AssertionError("the table was scanned to plan the write")

    monkeypatch.setattr(_write_cells, "_survey_distinct", scanned)
    monkeypatch.setattr(_write_cells, "_survey_units", scanned)
    ds = _dataset()
    out = tmp_path / "complete.zarr"
    gbx_zarr_write(
        _cells(spark_delta, ds),
        str(out),
        transactional=False,
        dim_cols=DIM_COLS,
        coords=_coords(ds),
        complete=True,
        chunks=CHUNKS,
        shards=SHARDS,
        spark=spark_delta,
    )
    got = _plain_read(out)
    for n in ("t2m", "u10"):
        np.testing.assert_array_equal(got[n].values, ds[n].values)


def test_complete_with_a_unit_that_has_no_cells_is_refused(
    spark_delta, tmp_path, no_hooks
):
    ds = _dataset()
    out = tmp_path / "gap.zarr"
    with pytest.raises(Exception, match="the write pass saw .*complete=True"):
        gbx_zarr_write(
            _cells(
                spark_delta,
                ds,
                keep=lambda p: p["time"] < p["time"].min() + pd.Timedelta(hours=24),
            ),
            str(out),
            transactional=False,
            dim_cols=DIM_COLS,
            coords=_coords(ds),
            complete=True,
            chunks=CHUNKS,
            shards=SHARDS,
            spark=spark_delta,
        )


def test_a_sparse_table_plans_only_the_units_it_touches(spark_delta, no_hooks):
    ds = _dataset()
    prep = _prepare(
        spark_delta,
        _cells(
            spark_delta,
            ds,
            keep=lambda p: p["time"] < p["time"].min() + pd.Timedelta(hours=24),
        ),
        coords=_coords(ds),
    )
    seen = prep.seen.to_pandas()
    assert set(seen[seen.variable == "t2m"]["start"].map(tuple)) == {(0, 0, 0)}
    assert prep.frame.rdd.getNumPartitions() == 1


def test_a_create_is_one_shuffle_and_stages_nothing(
    spark_delta, drop, tmp_path, no_hooks
):
    """Fail-on-revert for the staged path: the data pass has exactly one repartition by the
    unit key, and the write makes no table besides its target."""
    ds = _dataset()
    prep = _prepare(spark_delta, _cells(spark_delta, ds), coords=_coords(ds))
    plan = prep.frame._jdf.queryExecution().optimizedPlan().toString()
    assert plan.count("RepartitionByExpression") == 1
    before = _tables(spark_delta)
    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        _cells(spark_delta, ds),
        label="shuffle",
        dim_cols=DIM_COLS,
        coords=_coords(ds),
        shards=SHARDS,
    )
    made = _tables(spark_delta) - before
    assert made == {table}


def test_a_unit_over_the_task_budget_is_refused_naming_the_shards(
    spark_delta, no_hooks, monkeypatch
):
    ds = _dataset(hours=24)
    monkeypatch.setattr(_write, "_budget", lambda: 1 << 20)
    args = _write_cells.CellsArgs(
        coords=_coords(ds),
        chunks=CHUNKS,
        shards={"time": 24, "latitude": 10, "longitude": 12},
    )
    monkeypatch.setattr(_write_cells, "_unit_bytes", lambda g: 2 << 20)
    with pytest.raises(ZarrError, match=r"one storage unit.*Fix: use smaller shards"):
        _write._prepare_tiles(
            spark_delta,
            _cells(spark_delta, ds),
            (None, DIM_COLS),
            "create",
            None,
            None,
            None,
            args,
        )


# --- an off-grid append is spread over its shards -------------------------------------------


def test_an_offgrid_append_from_few_input_partitions_runs_in_a_task_per_block(
    spark_delta, drop, tmp_path, no_hooks, monkeypatch
):
    """The first stage of an append (cut the blocks into chunks, encode what they cover whole)
    ran in as many tasks as the input had partitions, whatever the shards it touched: a
    staged day of 63 shards ran in the 7 tasks of its 7 files. It is now spread by block.
    """
    from test.zarr import _helpers as H

    full = _dataset(hours=60)
    first, day = full.isel(time=slice(0, 36)), full.isel(time=slice(36, 60))
    table, _, _ = _create(
        spark_delta, drop, tmp_path, first, label="spread", shards=SHARDS
    )
    blocks = []
    for n in ("t2m", "u10"):
        for t in (0, 12):
            for y in (0, 5):
                blocks.append((n, (t, y, 0), day[n].values[t : t + 12, y : y + 5]))
    blocks.append(("time", (0,), day["time"].values.astype("datetime64[ns]")))
    frame = H.blocks_frame(spark_delta, *blocks).coalesce(1)
    seen = []
    real = _write._spread_out

    def spy(spark, plan, f):
        out = real(spark, plan, f)
        seen.append((plan.mode, out.rdd.getNumPartitions()))
        return out

    monkeypatch.setattr(_write, "_spread_out", spy)
    monkeypatch.setattr(_write, "_shuffle_partitions", lambda spark: 64)
    gbx_zarr_write(frame, table, mode="append", append_dim="time", spark=spark_delta)
    assert seen and seen[0][0] == "append"
    assert seen[0][1] == 8  # 8 data blocks: one task each, not one task for the input
    for n in ("t2m", "u10"):
        np.testing.assert_array_equal(
            _read(spark_delta, table, n)[36:, :10], full[n].values[36:, :10]
        )
