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
from databricks.labs.gbx.zarr.errors import ZarrAccessUnavailable  # noqa: E402
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
        got, expected = sorted(df.collect(), key=key), sorted(want.collect(), key=key)
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
    with pytest.raises(ValueError, match="Fix:"):
        isel_of(fanout="depth")


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
