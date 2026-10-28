"""``zarr_gbx``: the session-free, read-only DataSource for plain Zarr stores."""

import json

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

from databricks.labs.gbx.ds import register as ds_register  # noqa: E402
from databricks.labs.gbx.zarr import datasource  # noqa: E402
from databricks.labs.gbx.zarr.datasource import (  # noqa: E402
    ZarrGbxDataSource,
    ZarrGbxReader,
)
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrAccessUnavailable,
    ZarrError,
)
from databricks.labs.gbx.zarr.functions import gbx_zarr_read  # noqa: E402


@pytest.fixture(scope="module")
def spark(spark_delta):
    ds_register.register(spark_delta, only=["zarr"])
    return spark_delta


def _load(spark, path, **options):
    """``spark.read.format("zarr_gbx")`` with ``options``; a non-string is sent as JSON."""
    reader = spark.read.format("zarr_gbx")
    for key, value in options.items():
        reader = reader.option(
            key, value if isinstance(value, str) else json.dumps(value)
        )
    return reader.load(str(path))


def _without_cap(row):
    """A tile row with the tile-size cap taken out of its ``zarr_meta``. The function takes
    the cap from its session (classic: 256 MiB) and the reader from the runtime it plans
    on (Serverless 64 MiB, classic 256 MiB; test_the_tile_cap_follows_the_runtime)."""
    row = row.asDict(recursive=True)
    meta = row["tile"]["metadata"]
    meta["zarr_meta"] = json.dumps({**json.loads(meta["zarr_meta"]), "cap": None})
    return row


# --- the DataSource reads what the function reads ---------------------------------------


@pytest.mark.parametrize("store", ["era5_v3_sharded", "era5_v2"])
def test_blocks_via_datasource_equal_function_read(spark, request, store):
    """The reader is pickled to a Python worker and each unit is read there: whole blocks
    (data bytes included) must equal what ``gbx_zarr_read`` returns for the same cut."""
    path = request.getfixturevalue(store)
    df = _load(
        spark,
        path,
        sel={"latitude": [60, 50], "longitude": [-10, 10]},
        isel={"time": [0, 30]},
    )
    want = gbx_zarr_read(
        spark,
        str(path),
        sel={"latitude": slice(60, 50), "longitude": slice(-10, 10)},
        isel={"time": slice(0, 30)},
    )
    assert df.schema == want.schema
    key = lambda r: (r["variable"], r["start"])  # noqa: E731
    got, expected = sorted(df.collect(), key=key), sorted(want.collect(), key=key)
    assert got and got == expected
    assert {r["variable"] for r in got} == {"t2m", "u10"}
    # nothing selected plans no partition, and Spark then hands read() a partition of None
    assert _load(spark, path, sel={"latitude": [200, 300]}).count() == 0


@pytest.mark.parametrize(
    "output, options, call",
    [
        (
            "tiles",
            {"variables": "t2m", "isel": {"time": [0, 2]}},
            {"variables": ["t2m"], "isel": {"time": slice(0, 2)}},
        ),
        (
            "tiles",  # materialized: the tile holds its GeoTIFF bytes
            {"variables": "t2m", "isel": {"time": [0, 1]}, "virtualTiles": "false"},
            {
                "variables": ["t2m"],
                "isel": {"time": slice(0, 1)},
                "virtual_tiles": False,
            },
        ),
        (
            "table",
            {
                "variables": "t2m,u10",
                "sel": {"latitude": [60, 50], "longitude": [0, 10]},
                "isel": {"time": [0, 2]},
            },
            {
                "variables": ["t2m", "u10"],
                "sel": {"latitude": slice(60, 50), "longitude": slice(0, 10)},
                "isel": {"time": slice(0, 2)},
            },
        ),
    ],
    ids=["tiles", "tiles-materialized", "table"],
)
def test_tiles_and_table_outputs(spark, era5_v3_sharded, output, options, call):
    df = _load(spark, era5_v3_sharded, output=output, **options)
    want = gbx_zarr_read(spark, str(era5_v3_sharded), output=output, **call)
    assert df.schema == want.schema
    if output == "tiles":
        key = lambda r: (  # noqa: E731
            r["source"],
            r["tile"]["window"]["row_off"],
            r["tile"]["window"]["col_off"],
        )
        got, expected = (
            sorted(map(_without_cap, df.collect()), key=key),
            sorted(map(_without_cap, want.collect()), key=key),
        )
        virtual = "virtualTiles" not in options
        assert all((r["tile"]["raster"] is None) == virtual for r in got)
    else:
        order = [c for c in want.columns if c in ("time", "latitude", "longitude")]
        got, expected = df.orderBy(*order).collect(), want.orderBy(*order).collect()
    assert expected and got == expected


# --- options ----------------------------------------------------------------------------


def _cube(path):
    """A (time, level, latitude, longitude) store: a variable with two non-spatial dims."""
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, values in (
        ("time", np.arange(3.0)),
        ("level", np.array([1000.0, 850.0])),
        ("latitude", np.array([1.5, 0.5, -0.5, -1.5])),
        ("longitude", np.arange(6.0) * 0.5),
    ):
        coord = g.create_array(
            name, shape=values.shape, dtype="float64", dimension_names=(name,)
        )
        coord[:] = values
    dims = ("time", "level", "latitude", "longitude")
    g.create_array(
        "t",
        shape=(3, 2, 4, 6),
        chunks=(1, 1, 4, 6),
        dtype="float32",
        dimension_names=dims,
    )
    return path


def test_options_parse_dimindex_fanout_banddim(era5_v3_sharded, tmp_path):
    """dimIndex / fanout / bandDim / tileSize go through the NetCDF and raster readers' own
    parsers; the rest is JSON and comma lists, and a direct access that a session-free
    reader cannot vend is refused. Then what they mean for the planner: dimIndex pins a
    dimension, fanout names the ones to expand and, as in the NetCDF reader, the other
    non-spatial dimensions are read at index 0."""
    base = {"path": str(era5_v3_sharded)}
    opts = datasource.parse_options(
        {
            **base,
            "variables": " t2m , u10 ",
            "sel": '{"latitude": [60, 50], "time": "2023-01-01T05:00", "x": [1, 2, 3]}',
            "isel": '{"longitude": [0, 5], "depth": 2}',
            "dimIndex": "member=2,step=1",
            "output": "TILES",
            "virtualTiles": "False",
            "fanout": "time, level",
            "bandDim": " band ",
            "tileSize": "64,32",
            "access": "FUSE",
        }
    )
    assert opts.variables == ("t2m", "u10")
    assert opts.sel == {
        "latitude": slice(60, 50),  # a 2-element list is an inclusive slice
        "time": "2023-01-01T05:00",
        "x": [1, 2, 3],  # any other length selects those labels
    }
    assert opts.dim_index == {"member": 2, "step": 1}
    assert opts.isel == {"longitude": slice(0, 5), "depth": 2}
    assert (opts.output, opts.virtual_tiles) == ("tiles", False)
    assert (opts.fanout, opts.band_dim, opts.tile_size) == (
        ("time", "level"),
        "band",
        (64, 32),
    )

    with pytest.warns(UserWarning, match="duplicate dim"):  # the NetCDF parser's own
        again = datasource.parse_options({**base, "dimIndex": "time=2,time=3"})
    assert again.dim_index == {"time": 3}

    plain = datasource.parse_options(base)
    assert (plain.output, plain.virtual_tiles, plain.engine) == ("blocks", True, "auto")
    assert plain.variables is None and plain.sel is None and plain.fanout is None

    with pytest.raises(
        ZarrAccessUnavailable, match=r"Fix: .*access=\"auto\" or \"fuse\""
    ):
        datasource.parse_options({**base, "access": "direct"})
    for bad in (
        {"path": ""},  # no store
        {"output": "cube"},
        {"access": "cloud"},
        {"engine": "gdal"},
        {"virtualTiles": "maybe"},
        {"sel": "[1, 2]"},  # JSON, but not a dict of dimension to selection
        {"dimIndex": "time"},  # not dim=index
        {
            "output": "tiles",
            "dimIndex": "time=1",
            "fanout": "time",
        },  # pinned and expanded
        {"output": "tiles", "bandDim": "time", "fanout": "time"},
        {"output": "tiles", "bandDim": "time", "dimIndex": "time=1"},
        {"dimIndex": "time=1", "isel": '{"time": 2}'},  # one dimension, two selections
        {"output": "table", "bandDim": "time"},  # tiles-only options
        {"output": "blocks", "tileSize": "64"},
        {"output": "tiles", "engine": "zarr"},  # tiles are read by the tile engine
    ):
        with pytest.raises(ValueError, match="Fix:"):
            datasource.parse_options({**base, **bad})

    cube = _cube(tmp_path / "cube.zarr")

    def isel_of(**options):
        opts = datasource.parse_options(
            {"path": str(cube), "output": "tiles", **options}
        )
        return datasource._open(opts)[3]

    assert isel_of() is None  # every slice of every dimension
    assert isel_of(fanout="time") == {"level": 0}
    assert isel_of(fanout="level", dimIndex="time=2") == {"time": 2}
    assert isel_of(dimIndex="time=1,level=1") == {"time": 1, "level": 1}
    assert isel_of(fanout="time", isel='{"level": [0, 2]}') == {"level": slice(0, 2)}
    assert isel_of(fanout="time", bandDim="level") is None  # stacked, so not pinned
    # any one of the three switches on the NetCDF rules: what nothing names is read at 0
    assert isel_of(dimIndex="time=1") == {"time": 1, "level": 0}
    assert isel_of(bandDim="level") == {"time": 0}
    for given in ({"fanout": "depth"}, {"dimIndex": "depth=1"}, {"bandDim": "depth"}):
        with pytest.raises(ValueError, match="Fix:"):
            isel_of(**given)


# --- a read plans for the store it reads ------------------------------------------------


def _growing(path):
    """A (time=4, 4, 6) store with a coordinate per dimension; returns its group."""
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, values in (
        ("time", np.arange(4.0)),
        ("latitude", np.array([1.5, 0.5, -0.5, -1.5])),
        ("longitude", np.arange(6.0) * 0.5),
    ):
        coord = g.create_array(
            name, shape=values.shape, dtype="float64", dimension_names=(name,)
        )
        coord[:] = values
    v = g.create_array(
        "v",
        shape=(4, 4, 6),
        chunks=(2, 4, 6),
        dtype="float32",
        dimension_names=("time", "latitude", "longitude"),
    )
    v[:] = 1.0
    return g


def _grow(path, steps):
    """Append ``steps`` time steps to the store made by ``_growing``."""
    g = zarr.open_group(str(path), mode="r+")
    n = g["time"].shape[0]
    g["time"].resize((n + steps,))
    g["time"][n:] = np.arange(n, n + steps, dtype="float64")
    g["v"].resize((n + steps, 4, 6))
    g["v"][n:] = 1.0


def _cells(reader, partitions):
    return sum(
        len(x) // 4
        for p in partitions
        for b in reader.read(p)
        for x in b.column("data").to_pylist()
    )


def test_a_reread_of_a_changed_store_plans_for_the_store_it_reads(tmp_path):
    """Spark reuses Python workers, so what one read derived must not serve the next read of
    the same path and options: after the time axis grows, a new read sees all of it. A store
    that changes between planning a read and reading it is an error, not other cells."""
    path = tmp_path / "s.zarr"
    _growing(path)
    options = {"path": str(path), "variables": "v"}

    def read_all():
        reader = ZarrGbxReader(options)  # a new read: Spark makes a new reader
        return _cells(reader, reader.partitions())

    assert read_all() == 4 * 4 * 6
    _grow(path, 2)
    assert read_all() == 6 * 4 * 6  # the first read's plan must not be reused

    reader = ZarrGbxReader(options)
    partitions = reader.partitions()
    _grow(path, 2)  # after planning, before a task reads
    datasource._task_state.cache_clear()  # a worker that has not planned this read yet
    with pytest.raises(ZarrError, match="Fix:"):
        _cells(reader, partitions)


def test_the_tile_cap_follows_the_runtime(tmp_path, monkeypatch):
    """A session-free reader takes the largest tile a task may hold from the runtime probe
    (as raster_gbx sizes its tiles): 64 MiB on Serverless, 256 MiB on classic, and
    GBX_STREAM_MAX_BYTES wins. It is planned once, on the planner, and travels in the tiles.
    """
    from databricks.labs.gbx.pyrx.core import budget

    path = tmp_path / "s.zarr"
    _growing(path)

    def cap():
        reader = ZarrGbxReader({"path": str(path), "output": "tiles"})
        first = next(iter(reader.read(reader.partitions()[0])))
        meta = dict(first.to_pylist()[0]["tile"]["metadata"])  # an Arrow map
        return json.loads(meta["zarr_meta"])["cap"]

    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.setattr(budget, "runtime_kind", lambda: "serverless")
    assert cap() == 64 * 1024**2
    monkeypatch.setattr(budget, "runtime_kind", lambda: "classic")
    assert cap() == 256 * 1024**2
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", "12345")
    assert cap() == 12345


# --- the shape of the source ------------------------------------------------------------


def test_no_pushfilters_method():
    """On Serverless env 6 a reader that merely defines pushFilters fails with
    DATA_SOURCE_PUSHDOWN_DISABLED (measured); pruning is option-based."""
    assert not hasattr(ZarrGbxReader, "pushFilters")
    assert not hasattr(ZarrGbxDataSource, "pushFilters")


def test_register_lists_zarr_gbx(spark, era5_v3_sharded):
    assert ZarrGbxDataSource in ds_register._SOURCES
    assert ZarrGbxDataSource.name() == "zarr_gbx"
    ds_register.register(spark, only=["zarr_gbx"])  # the bare name works too: "zarr"
    assert _load(spark, era5_v3_sharded, variables="t2m", isel={"time": [0, 1]}).count()
