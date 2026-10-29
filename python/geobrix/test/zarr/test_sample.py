"""``gbx_zarr_sample``: point samples of a Zarr store.

The matrix runs without Spark (``_helpers.sample_table`` is the driver's plan and a task's
read, in process) against numpy's own reading of the same stores; three tests run the
function on a local Spark session, one per route (driver, tasks, tasks with the points
mapped on executors), and one on a Spark Connect client.
"""

from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pytest

zarr = pytest.importorskip("zarr")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.zarr import _sample  # noqa: E402
from databricks.labs.gbx.zarr._sample import gbx_zarr_sample  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrAccessUnavailable,
    ZarrUnsupported,
)

N_POINTS = 50
T0 = np.datetime64("2023-01-01T00:00", "us")
HOURS = T0 + np.arange(48) * np.timedelta64(1, "h")


def _wkb(x, y, srid=0):
    import shapely

    point = shapely.Point(x, y)
    return shapely.to_wkb(
        shapely.set_srid(point, srid) if srid else point, include_srid=bool(srid)
    )


def _era5_points(seed=11):
    rng = np.random.default_rng(seed)
    xs = rng.uniform(-180, 180, N_POINTS)  # negative longitudes wrap onto 0..360
    ys = rng.uniform(-90, 90, N_POINTS)
    return xs, ys, [_wkb(x, y) for x, y in zip(xs, ys)]


def _era5_oracle(store, name, xs, ys):
    """``name`` at the nearest cell of each point: (points, 48), by brute-force argmin."""
    lat, lon = H.direct(store, "latitude"), H.direct(store, "longitude")
    iy = np.abs(lat[None, :] - ys[:, None]).argmin(axis=1)
    ring = np.abs((lon[None, :] - (xs[:, None] % 360) + 180) % 360 - 180)
    return H.direct(store, name)[:, iy, ring.argmin(axis=1)].T


def _by_point(table, name, steps=48):
    return table.column(name).to_numpy(zero_copy_only=False).reshape(-1, steps)


# --- the matrix, without Spark ---------------------------------------------------------


@pytest.mark.parametrize(
    "fixture, route",
    [
        ("era5_v3_sharded", "tensorstore"),  # a plain store: a tensorstore vindex
        ("era5_v3_sharded", "zarr"),  # ... and the zarr vindex a task uses
        ("era5_v2", "tensorstore"),
        ("era5_v3_sharded", "registered"),  # a manifest store: native chunk gather
        ("era5_v2", "registered"),
    ],
)
def test_sample_matches_numpy_nearest(fixture, route, request, monkeypatch, tmp_path):
    store = request.getfixturevalue(fixture)
    xs, ys, points = _era5_points()
    if route == "registered":
        source = H.registered_source(monkeypatch, store, overlay=tmp_path)
        engine = "auto"
    else:
        source, engine = store, {"tensorstore": "auto", "zarr": "zarr"}[route]
    cfg, table = H.sample_table(source, points, engine=engine)

    assert table.column_names == ["point", "time", "t2m", "u10"]
    assert table.num_rows == N_POINTS * 48
    assert (
        table.column("point").to_pylist() == np.repeat(np.arange(N_POINTS), 48).tolist()
    )
    for name in ("t2m", "u10"):
        np.testing.assert_array_equal(
            _by_point(table, name), _era5_oracle(store, name, xs, ys)
        )
    times = table.column("time").cast(pa.int64()).to_numpy().reshape(-1, 48)
    assert (times == HOURS.astype("int64")).all()  # every point has the same 48 hours

    # sel narrows the other dimensions (by label), variables the columns
    _, part = H.sample_table(
        source,
        points[:5],
        engine=engine,
        variables=["u10"],
        sel={"time": slice("2023-01-01T05", "2023-01-01T07")},
    )
    assert part.column_names == ["point", "time", "u10"] and part.num_rows == 5 * 3
    np.testing.assert_array_equal(
        _by_point(part, "u10", 3), _era5_oracle(store, "u10", xs, ys)[:5, 5:8]
    )
    assert part.column("time").cast(pa.int64()).to_pylist()[:3] == (
        HOURS[5:8].astype("int64").tolist()
    )


@pytest.mark.parametrize(
    "route, step, dense",
    [
        ("tensorstore", 1, True),
        ("zarr", 1, True),
        ("zarr", 6, True),  # a sixth of the cells: above the tenth
        ("zarr", 50, False),  # a fiftieth: below it
        ("registered", 1, True),
    ],
)
def test_a_dense_sample_reads_its_box_once(
    era5_v3_sharded, route, step, dense, monkeypatch, tmp_path
):
    """Points on a tenth or more of a box's cells read the box and gather from it (MEASURED
    on the ERA5 cube on Serverless: the box was faster from 12% of the cells up, 2x there and
    4x at 25%; point reads were faster at 1%); sparser points keep the vectorized point read.
    Same values either way. ``step``: every step-th cell of chunk (0, 0) gets a point.
    """
    from databricks.labs.gbx.zarr import _engine

    store = era5_v3_sharded
    lat, lon = H.direct(store, "latitude"), H.direct(store, "longitude")
    rows, cols = np.meshgrid(
        np.arange(37), np.arange(72), indexing="ij"
    )  # chunk (0, 0)
    take = slice(None, None, step)
    ys, xs = lat[rows.ravel()[take]], lon[cols.ravel()[take]]
    used = {"box": 0, "points": 0}
    real_box, real_points = _engine.read_box, _engine.read_points

    def box(arr, *a, **k):
        used["box"] += len(arr.shape) == 3  # a read of t2m, not of a coordinate
        return real_box(arr, *a, **k)

    def points(arr, *a, **k):
        used["points"] += len(arr.shape) == 3
        return real_points(arr, *a, **k)

    monkeypatch.setattr(_engine, "read_box", box)
    monkeypatch.setattr(_engine, "read_points", points)
    if route == "registered":
        source = H.registered_source(monkeypatch, store, overlay=tmp_path)
        engine = "auto"
    else:
        source, engine = store, {"tensorstore": "auto", "zarr": "zarr"}[route]
    _, table = H.sample_table(
        source, [_wkb(x, y) for x, y in zip(xs, ys)], engine=engine, variables=["t2m"]
    )
    np.testing.assert_array_equal(
        _by_point(table, "t2m"), _era5_oracle(store, "t2m", xs, ys)
    )
    assert (used["box"] > 0, used["points"] > 0) == (dense, not dense)


def test_a_dense_unit_is_read_once_across_its_batches(tmp_path, monkeypatch):
    """The dense decision is made for the whole unit: with batches of two points, a unit of
    every cell of a chunk (plus two points given twice, and the fill cell) still reads its
    box once, and each batch gathers from it; the fill cell is NULL, the repeats repeat.
    """
    from databricks.labs.gbx.zarr import _engine

    data = _regional(tmp_path / "r.zarr")
    cells = [(j, i) for j in range(5) for i in range(5)] + [(0, 0), (4, 4)]
    points = [
        _wkb(10.0 + i, 50.0 - j) for j, i in cells
    ]  # chunk (0, 0), fill at (2, 3)
    used = {"box": 0, "points": 0}
    real_box, real_points = _engine.read_box, _engine.read_points

    def box(arr, *a, **k):
        used["box"] += len(arr.shape) == 3
        return real_box(arr, *a, **k)

    def point(arr, *a, **k):
        used["points"] += len(arr.shape) == 3
        return real_points(arr, *a, **k)

    monkeypatch.setattr(_engine, "read_box", box)
    monkeypatch.setattr(_engine, "read_points", point)
    _, table = H.sample_table(tmp_path / "r.zarr", points, batch_cells=6)

    assert used == {"box": 1, "points": 0}  # 14 batches, one read
    got = _by_point(table, "v", 3)
    null = table.column("v").is_null().to_numpy(zero_copy_only=False).reshape(-1, 3)
    for n, (j, i) in enumerate(cells):
        if (j, i) == (2, 3):
            assert null[n].all()
        else:
            assert not null[n].any()
            np.testing.assert_array_equal(got[n], data[:, j, i])


def _regional(path):
    """A v3 store of 3 steps over latitude 50..41 (descending) and longitude 10..19, whose
    cell ``(j, i)`` at step ``t`` holds ``1000 t + 10 j + i`` except one fill cell."""
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, values, units in (
        ("latitude", np.arange(50.0, 40.0, -1.0), "degrees_north"),
        ("longitude", np.arange(10.0, 20.0), "degrees_east"),
    ):
        a = g.create_array(
            name, shape=(10,), chunks=(5,), dtype="f8", dimension_names=(name,)
        )
        a[:] = values
        a.attrs["units"] = units
    step = g.create_array(
        "step", shape=(3,), chunks=(3,), dtype="i8", dimension_names=("step",)
    )
    step[:] = [0, 1, 2]
    v = g.create_array(
        "v",
        shape=(3, 10, 10),
        chunks=(3, 5, 5),
        dtype="f4",
        dimension_names=("step", "latitude", "longitude"),
        attributes={"_FillValue": -9999.0},
    )
    data = (
        1000.0 * np.arange(3)[:, None, None]
        + 10.0 * np.arange(10)[None, :, None]
        + np.arange(10)[None, None, :]
    ).astype("f4")
    data[:, 2, 3] = -9999.0
    v[:] = data
    return data


def test_out_of_extent_is_null(tmp_path):
    data = _regional(tmp_path / "r.zarr")
    probes = [  # (x, y, row, col): the cell a point reads, None for no cell
        (12.4, 45.2, 5, 2),  # inside: latitude 45 is row 5, longitude 12 is column 2
        (10.0, 50.5, 0, 0),  # half a cell beyond the first latitude is still its cell
        (9.5, 45.0, 5, 0),  # ... and beyond the first longitude
        (10.0, 50.6, None, None),  # past half a cell: out
        (9.4, 45.0, None, None),
        (25.0, 45.0, None, None),
        (13.0, 48.0, 2, 3),  # inside, but the cell is the _FillValue: masked
    ]
    points = [_wkb(x, y) for x, y, *_ in probes] + [None, "POINT EMPTY"]
    _, table = H.sample_table(tmp_path / "r.zarr", points)

    assert table.column_names == ["point", "step", "v"]
    assert table.num_rows == len(points) * 3  # a point with no cell keeps its rows
    got = _by_point(table, "v", 3)
    null = table.column("v").is_null().to_numpy(zero_copy_only=False).reshape(-1, 3)
    for i, (_, _, row, col) in enumerate(probes):
        if row is None or (row, col) == (2, 3):
            assert null[i].all(), probes[i]
        else:
            assert not null[i].any(), probes[i]
            np.testing.assert_array_equal(got[i], data[:, row, col])
    assert null[len(probes) :].all()  # a missing geometry and an empty point
    assert table.column("step").to_pylist()[:3] == [0, 1, 2]


def _projected(path):
    """A v3 store on an EPSG:32633 grid (1 km cells), whose cell ``(j, i)`` holds ``100 j + i``."""
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, values in (
        ("y", 5_000_000.0 - 1000.0 * np.arange(20)),
        ("x", 500_000.0 + 1000.0 * np.arange(20)),
    ):
        a = g.create_array(
            name, shape=(20,), chunks=(10,), dtype="f8", dimension_names=(name,)
        )
        a[:] = values
    crs = g.create_array(
        "crs", shape=(), dtype="i1", attributes={"epsg_code": "EPSG:32633"}
    )
    crs[...] = 0
    v = g.create_array(
        "v",
        shape=(20, 20),
        chunks=(10, 10),
        dtype="f8",
        dimension_names=("y", "x"),
        attributes={"grid_mapping": "crs"},
    )
    v[:] = 100.0 * np.arange(20)[:, None] + np.arange(20)[None, :]
    return path


def test_ewkb_srid_wins(tmp_path):
    from pyproj import Transformer

    store = _projected(tmp_path / "p.zarr")
    lon, lat = Transformer.from_crs(32633, 4326, always_xy=True).transform(
        507_000.0, 4_995_000.0
    )  # the centre of cell (5, 7)
    want = 507.0
    cases = {
        "ewkb 4326 beats crs=3857": (_wkb(lon, lat, 4326), 3857),
        "ewkt 4326 beats crs=3857": (f"SRID=4326;POINT ({lon} {lat})", 3857),
        "crs 4326 for a plain point": (_wkb(lon, lat), 4326),
        "no crs: already in the store's": (_wkb(507_000.0, 4_995_000.0), None),
    }
    for label, (geom, crs) in cases.items():
        _, table = H.sample_table(store, [geom], crs=crs)
        assert table.column("v").to_pylist() == [want], label
    _, table = H.sample_table(
        store, [_wkb(lon, lat)]
    )  # degrees read as metres: nowhere
    assert table.column("v").to_pylist() == [None]


def test_bilinear_unsupported():
    with pytest.raises(ZarrUnsupported) as e:
        gbx_zarr_sample(None, None, "/nowhere", method="bilinear")
    assert e.value.fix == 'use method="nearest"; bilinear is planned'
    with pytest.raises(
        ZarrAccessUnavailable
    ) as e:  # a plain store has no direct access
        gbx_zarr_sample(None, None, "/nowhere", access="direct")
    assert 'access="auto"' in e.value.fix


# --- the function, on Spark -------------------------------------------------------------


def test_sample_checks_its_arguments(spark_delta, era5_v3_sharded):
    path = str(era5_v3_sharded)
    pts = spark_delta.createDataFrame(
        [(1, _wkb(10, 10)), (2, _wkb(20, 20))], "id BIGINT, geom BINARY"
    )

    def sample(df=pts, **kw):
        return gbx_zarr_sample(spark_delta, df, path, **kw)

    with pytest.raises(ValueError, match="no column 'geometry'.*Fix: "):
        sample(geom_col="geometry")
    with pytest.raises(ValueError, match="not a geometry.*Fix: "):
        sample(geom_col="id")
    with pytest.raises(ValueError, match="already has column.*time.*Fix: "):
        sample(pts.withColumn("time", pts.id))
    with pytest.raises(ValueError, match="latitude.*points choose.*Fix: "):
        sample(sel={"latitude": slice(0, 10)})
    with pytest.raises(ValueError, match="both.*Fix: "):
        sample(version_as_of=1, timestamp_as_of="2024-01-01")
    with pytest.raises(ValueError, match="cannot read the CRS.*Fix: "):
        sample(crs="EPSG:not-a-code")
    line = spark_delta.createDataFrame([("LINESTRING (0 0, 1 1)",)], "geom STRING")
    with pytest.raises(ValueError, match="POINT geometries.*Fix: "):
        sample(line).collect()
    assert sample(sel={"time": slice("2030-01-01", "2030-01-02")}).count() == 0


def _points_df(spark, xs, ys):
    rows = [
        (i, f"p{i}", datetime(2024, 1, 1, i % 24, tzinfo=timezone.utc), _wkb(x, y))
        for i, (x, y) in enumerate(zip(xs, ys))
    ]
    return spark.createDataFrame(
        rows, "id BIGINT, tag STRING, seen TIMESTAMP, geom BINARY"
    )


def _check_against_numpy(df, store, xs, ys):
    assert df.columns == ["id", "tag", "seen", "geom", "time", "t2m", "u10"]
    rows = sorted(df.collect(), key=lambda r: (r.id, r.time))
    assert len(rows) == N_POINTS * 48
    for name in ("t2m", "u10"):
        got = np.array([getattr(r, name) for r in rows]).reshape(N_POINTS, 48)
        np.testing.assert_array_equal(got, _era5_oracle(store, name, xs, ys))
    assert {(r.id, r.tag) for r in rows} == {(i, f"p{i}") for i in range(N_POINTS)}
    assert all(r.seen.hour == r.id % 24 for r in rows)  # the point's columns ride along
    first = int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp())
    assert (
        int(rows[0].time.timestamp()) == first
    )  # TIMESTAMP, the instant, not a string


def test_small_job_runs_on_driver(spark_delta, era5_v3_sharded, monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a small job must not start a Spark stage")

    xs, ys, _ = _era5_points()
    points = _points_df(spark_delta, xs, ys)
    monkeypatch.setattr(type(points), "mapInArrow", refuse)  # the session's own class
    df = gbx_zarr_sample(spark_delta, points, str(era5_v3_sharded))
    _check_against_numpy(
        df, era5_v3_sharded, xs, ys
    )  # 50 points in 4 chunks, 2400 rows


@pytest.mark.parametrize(
    "limits, stages",
    [
        ({"_SMALL_UNITS": 0}, 1),  # tasks, the driver mapped the points
        (
            {"_POINT_LIMIT": 10},
            3,
        ),  # tasks, executors mapped them (a pass to plan, a pass to read)
    ],
)
def test_tasks_agree_with_the_driver(
    spark_delta, era5_v3_sharded, monkeypatch, limits, stages
):
    for name, value in limits.items():
        monkeypatch.setattr(_sample, name, value)
    xs, ys, _ = _era5_points()
    points = _points_df(spark_delta, xs, ys)
    calls, real = [], type(points).mapInArrow
    monkeypatch.setattr(
        type(points),
        "mapInArrow",
        lambda self, *a, **k: calls.append(1) or real(self, *a, **k),
    )
    df = gbx_zarr_sample(spark_delta, points, str(era5_v3_sharded))
    _check_against_numpy(df, era5_v3_sharded, xs, ys)
    assert len(calls) == stages


def test_a_managed_table_is_sampled_with_its_rows(
    spark_delta, era5_v3_sharded, monkeypatch
):
    """One end-to-end table source: every unit carries the manifest rows it reads."""
    import uuid
    from test.zarr.test_read import _manage

    table = f"gbx_zarr_sample_{uuid.uuid4().hex[:8]}"
    _manage(spark_delta, era5_v3_sharded, table)
    monkeypatch.setattr(_sample, "_SMALL_UNITS", 0)
    xs, ys, _ = _era5_points()
    try:
        df = gbx_zarr_sample(spark_delta, _points_df(spark_delta, xs, ys), table)
        _check_against_numpy(df, era5_v3_sharded, xs, ys)
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


def test_sample_on_a_spark_connect_client(
    spark_connect_local, era5_v3_sharded, monkeypatch
):
    """The plan shape on a Connect client, the session type of Serverless: collect and
    ``createDataFrame`` of the points, ``repartition`` and ``sortWithinPartitions`` by chunk,
    and ``mapInArrow`` with the points' own columns carried through. Run alone:
    ``-k connect`` (a classic session in the same process makes the fixture skip)."""
    monkeypatch.setattr(_sample, "_SMALL_UNITS", 0)
    xs, ys, _ = _era5_points()
    df = gbx_zarr_sample(
        spark_connect_local,
        _points_df(spark_connect_local, xs, ys),
        str(era5_v3_sharded),
    )
    _check_against_numpy(df, era5_v3_sharded, xs, ys)


def test_a_unit_with_no_selected_step_is_not_read_as_a_box():
    """No step selected: the box would be empty (and its bounds undefined), so the unit is
    not dense, whatever else it holds."""
    import types

    import numpy as np

    from databricks.labs.gbx.zarr import _sample

    cfg = types.SimpleNamespace(n_steps=0, box_bytes=1 << 20)
    rows = np.array([3, 4])
    assert _sample._unit_boxes(cfg, None, None, rows, rows) is None
