"""Executes the Zarr reader doc examples on the sample cube (Docker only).

Every code block on ``docs/docs/readers/zarr.mdx`` is a function of ``zarr_read_examples.py``;
these tests run each one against the real sample cube (or a small store written for the test)
on a local Delta session and assert what the page says it does. The manifest of a managed or
registered store is a Delta table, so the session is the Delta one from ``zarr_delta.py``.
"""

import os
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import zarr_read_examples as ex  # noqa: E402
from zarr_delta import copy_of_cube, cube, new_table, spark  # noqa: E402,F401

xr = pytest.importorskip("xarray")
zarr = pytest.importorskip("zarr")

pytestmark = pytest.mark.delta_session  # runs after the tests that use the shared session

from databricks.labs.gbx.zarr import (  # noqa: E402
    gbx_zarr_read,
    gbx_zarr_register,
    gbx_zarr_tag,
    gbx_zarr_write,
)

DOCS = Path(__file__).resolve().parents[3] / "docs"
PAGE = DOCS / "readers" / "zarr.mdx"


# --- helpers ---------------------------------------------------------------------------


def shown(capsys):
    """What an example printed, without the progress lines register prints."""
    lines = capsys.readouterr().out.splitlines()
    return "\n".join(ln for ln in lines if not ln.startswith("[register] ")).strip()


@pytest.fixture(scope="module")
def truth(cube):
    """The sample cube, loaded: what every read must give."""
    return xr.open_zarr(cube, consolidated=False)[["t2m", "msl"]].load()


@pytest.fixture
def gauges(tmp_path):
    """Daily rainfall at three stations in whole millimetres: int16 with a ``_FillValue``."""
    rain = np.array(
        [[0.0, 12.0, np.nan], [3.0, np.nan, 8.0], [1.0, 2.0, 0.0]], dtype="float32"
    )
    ds = xr.Dataset(
        {"rain": (("day", "station"), rain)},
        coords={
            "day": pd.date_range("2023-06-01", periods=3, freq="D"),
            "station": np.array([101, 102, 103]),
        },
    )
    ds["rain"].encoding = {"dtype": "int16", "_FillValue": -9999}
    path = str(tmp_path / "gauges.zarr")
    ds.to_zarr(path, mode="w", consolidated=False)
    return path


@pytest.fixture
def counts(tmp_path):
    """A Zarr v2 array of counts whose ``fill_value`` is 0, with real zeros among the values."""
    path = str(tmp_path / "counts.zarr")
    group = zarr.open_group(path, mode="w", zarr_format=2)
    array = group.create_array(
        "counts", shape=(2, 3), chunks=(2, 3), dtype="int32", fill_value=0
    )
    array[:] = np.array([[0, 4, 7], [2, 0, 5]], dtype="int32")
    array.attrs["_ARRAY_DIMENSIONS"] = ["row", "col"]
    return path


@pytest.fixture(scope="module")
def two_versions(spark, cube, tmp_path_factory):
    """A managed store with two versions and a tag on the first: 12 hours, then 24."""
    table = f"rdr_weather_{uuid.uuid4().hex[:8]}"
    root = str(tmp_path_factory.mktemp("rdr") / "era5")
    names = ["t2m", "time", "latitude", "longitude"]

    def hours(start, stop):
        return gbx_zarr_read(
            spark, cube, variables=names, sel={"time": slice(start, stop)}
        )

    gbx_zarr_write(
        hours("2023-01-01T00:00", "2023-01-01T11:00"), table, mode="create", root=root
    )
    gbx_zarr_tag(spark, table, "first-day")
    gbx_zarr_write(hours("2023-01-01T12:00", "2023-01-01T23:00"), table, mode="append")
    yield table
    spark.sql(f"DROP TABLE IF EXISTS {table}")


# --- sample data -----------------------------------------------------------------------


def test_setup_paths_match_the_sample_data_layout():
    assert ex.CUBE_SUBPATH in ex.SETUP
    assert ex.SETUP.count("cube = ") == 1


# --- blocks and selections ------------------------------------------------------------


def test_blocks_are_one_row_per_chunk_with_their_cells(spark, cube, truth, capsys):
    blocks = ex.read_blocks(spark, cube)
    assert shown(capsys) == ex.READ_BLOCKS_output.strip()
    rows = blocks.collect()
    assert len(rows) == 4  # one time chunk x two latitude chunks x two longitude chunks
    total = 0
    for row in rows:
        assert row["variable"] == "t2m" and np.dtype(row["dtype"]) == np.float32
        assert row["shape"][0] == 3  # the three selected hours of the 12-hour chunk
        total += row["shape"][0] * row["shape"][1] * row["shape"][2]
    assert total == 3 * 181 * 360


def test_labels_and_positions_select_the_same_cells(spark, cube, capsys):
    by_label, by_position = ex.select_by_label_or_position(spark, cube)
    assert shown(capsys) == ex.SELECT_output.strip()
    key = ["time", "latitude", "longitude"]
    a = by_label.toPandas().sort_values(key).reset_index(drop=True)
    b = by_position.toPandas().sort_values(key).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b)
    assert len(a) == 3 * 11 * 11


def test_a_table_has_a_column_per_dimension_and_variable(
    spark, cube, truth, capsys
):
    df = ex.read_a_table(spark, cube)
    assert shown(capsys) == ex.READ_A_TABLE_output.strip()
    got = df.where("time = timestamp'2023-01-01 00:00:00'").toPandas()
    want = truth["t2m"].sel(
        time="2023-01-01T00:00", latitude=slice(60, 40), longitude=slice(0, 10)
    )
    assert len(got) == want.size
    assert np.isclose(got["t2m"].sum(), float(want.sum()), rtol=1e-5)


# --- missing data ----------------------------------------------------------------------


def test_masked_integers_stay_integers_and_become_null(spark, gauges, capsys):
    from pyspark.sql import types as T

    df = ex.read_missing_data(spark, gauges)
    assert shown(capsys) == ex.READ_MISSING_DATA_output.strip()
    assert df.schema["rain"].dataType == T.ShortType()
    assert df.schema["day"].dataType == T.TimestampType()
    assert df.where("rain IS NULL").count() == 2
    raw = gbx_zarr_read(spark, gauges, variables=["rain"], output="table", decode=False)
    assert raw.where("rain = -9999").count() == 2 and raw.where("rain IS NULL").count() == 0


def test_a_nan_fill_value_gives_null_cells_and_data_nan_stays_nan(spark, tmp_path):
    # Zarr v2 masks on the array's fill_value; written with zarr directly because xarray
    # would turn a data NaN into the numeric fill
    path = str(tmp_path / "fills.zarr")
    group = zarr.open_group(path, mode="w", zarr_format=2)
    for name, fill in (("nan_fill", float("nan")), ("numeric_fill", -9999.0)):
        a = group.create_array(
            name, shape=(3,), chunks=(3,), dtype="float32", fill_value=fill
        )
        a[:] = np.array([1.0, float("nan"), fill], dtype="float32")
        a.attrs["_ARRAY_DIMENSIONS"] = ["i"]
    nan_fill = gbx_zarr_read(spark, path, variables=["nan_fill"], output="table")
    assert nan_fill.where("nan_fill IS NULL").count() == 2  # every NaN is the fill
    numeric = gbx_zarr_read(spark, path, variables=["numeric_fill"], output="table")
    assert numeric.where("numeric_fill IS NULL").count() == 1  # the -9999
    assert numeric.where("isnan(numeric_fill)").count() == 1  # the NaN that is data


def test_the_type_of_each_column_follows_the_stored_type(spark, tmp_path):
    from pyspark.sql import types as T

    cases = {
        "int8": T.ByteType(),
        "int16": T.ShortType(),
        "int32": T.IntegerType(),
        "int64": T.LongType(),
        "uint8": T.ShortType(),
        "uint16": T.IntegerType(),
        "uint32": T.LongType(),
        "uint64": T.DecimalType(20, 0),
        "bool": T.BooleanType(),
        "float32": T.FloatType(),
        "float64": T.DoubleType(),
    }
    ds = xr.Dataset(
        {f"v_{n}": (("row",), np.arange(3).astype(n)) for n in cases},
    )
    path = str(tmp_path / "types.zarr")
    ds.to_zarr(path, mode="w", consolidated=False)
    df = gbx_zarr_read(spark, path, output="table")
    for name, spark_type in cases.items():
        assert df.schema[f"v_{name}"].dataType == spark_type, name


def test_an_integer_array_read_as_booleans_masks_on_an_explicit_fill_only(
    spark, tmp_path
):
    """Tables give NULL for a cell equal to an explicit fill, where blocks (like xarray)
    cast the masked cell to True; zarr's default fill of 0 is a stored False, never a mask."""
    from databricks.labs.gbx.zarr import block_to_numpy

    path = str(tmp_path / "flags.zarr")
    group = zarr.open_group(path, mode="w", zarr_format=2)
    for name, fill in (("masked", None), ("plain", 0)):
        a = group.create_array(
            name, shape=(4,), chunks=(4,), dtype="int8", fill_value=fill if fill is not None else -1
        )
        a[:] = np.array([1, 0, -1, 1], dtype="int8") if name == "masked" else np.array([1, 0, 0, 1], dtype="int8")
        a.attrs["_ARRAY_DIMENSIONS"] = ["i"]
        a.attrs["dtype"] = "bool"
        if name == "masked":
            a.attrs["_FillValue"] = -1
    table = gbx_zarr_read(spark, path, variables=["masked"], output="table")
    assert table.where("masked IS NULL").count() == 1
    plain = gbx_zarr_read(spark, path, variables=["plain"], output="table")
    assert plain.where("plain IS NULL").count() == 0 and plain.where("plain = false").count() == 2
    blocks = gbx_zarr_read(spark, path, variables=["masked"]).collect()
    assert block_to_numpy(blocks[0]).tolist() == [True, False, True, True]


def test_zeros_are_data_when_the_flag_is_false(spark, counts, capsys):
    ex.keep_zero_counts(spark, counts)
    assert shown(capsys) == ex.KEEP_ZEROS_output.strip()


def test_the_time_column_is_utc_to_the_microsecond(spark, cube):
    df = gbx_zarr_read(
        spark,
        cube,
        variables=["t2m"],
        sel={"time": "2023-01-01T05:00", "latitude": 0.0, "longitude": 0.0},
        output="table",
    )
    stamp = df.select("time").first()["time"]
    spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
    try:
        # the instant does not move with the session zone: it is 05:00 UTC
        epoch = df.selectExpr("unix_timestamp(time)").first()[0]
    finally:
        spark.conf.unset("spark.sql.session.timeZone")
    assert epoch == int(pd.Timestamp("2023-01-01T05:00", tz="UTC").timestamp())
    assert stamp.microsecond == 0


# --- versions ---------------------------------------------------------------------------


def test_a_table_read_pins_a_version(spark, two_versions, capsys):
    ex.read_a_version(spark, two_versions)
    assert shown(capsys) == ex.READ_A_VERSION_output.strip()


# --- tiles ------------------------------------------------------------------------------


def test_tiles_are_virtual_and_name_their_window(spark, cube, capsys):
    tiles = ex.read_tiles(spark, cube)
    assert shown(capsys) == ex.READ_TILES_output.strip()
    row = tiles.first()["tile"]
    assert row["raster"] is None  # no pixels until a function opens the tile


def test_every_rst_function_works_on_the_tiles(spark, cube, truth, capsys):
    out = ex.measure_tiles(spark, cube)
    assert shown(capsys) == ex.MEASURE_TILES_output.strip()
    want = truth["t2m"].sel(time="2023-01-01T00:00")
    rows = out.collect()
    cells = sum(r["width"] * r["height"] for r in rows)
    assert cells == want.size  # the four windows cover the plane
    mean = sum(r["width"] * r["height"] * r["mean"] for r in rows) / cells
    assert mean == pytest.approx(float(want.mean()), rel=1e-3)


def test_materialized_tiles_hold_their_bytes(spark, cube, capsys):
    tiles = ex.materialize_tiles(spark, cube)
    assert shown(capsys) == ex.MATERIALIZE_TILES_output.strip()
    row = tiles.first()["tile"]
    assert row["raster"] is not None and len(row["raster"]) > 0


def test_a_dimension_stacks_as_the_bands_of_a_tile(spark, cube, capsys):
    ex.stack_time_as_bands(spark, cube)
    assert shown(capsys) == ex.STACK_BANDS_output.strip()


def test_a_0_to_360_longitude_axis_gives_tiles_east_of_180(spark, tmp_path):
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx import functions as rx

    ds = xr.Dataset(
        {"t": (("latitude", "longitude"), np.zeros((4, 360), "float32"))},
        coords={
            "latitude": np.array([1.5, 0.5, -0.5, -1.5]),
            "longitude": np.arange(360, dtype="float64"),
        },
    )
    ds["longitude"].attrs["units"] = "degrees_east"
    ds["latitude"].attrs["units"] = "degrees_north"
    path = str(tmp_path / "wide.zarr")
    ds.to_zarr(path, mode="w", consolidated=False)
    tiles = gbx_zarr_read(
        spark,
        path,
        output="tiles",
        tile_size=(360, 4),
    )
    row = tiles.select(
        rx.rst_srid("tile").alias("srid"),
        rx.rst_upperleftx("tile").alias("west"),
        rx.rst_width("tile").alias("width"),
        rx.rst_pixelwidth("tile").alias("step"),
    ).first()
    assert row["srid"] == 4326
    assert row["west"] + row["width"] * row["step"] > 180  # x reaches 359.5, not 180


# --- the DataSource ----------------------------------------------------------------------


def test_the_datasource_reads_a_plain_store(spark, cube, capsys):
    df = ex.read_with_the_datasource(spark, cube)
    assert shown(capsys) == ex.DATASOURCE_output.strip()
    assert df.count() == 2 * 21 * 31


def test_the_datasource_options_the_page_lists_are_the_options_it_has():
    import re

    from databricks.labs.gbx.zarr import datasource

    doc = datasource.__doc__
    doc = doc[doc.index("Options (camelCase") :]
    options = set(re.findall(r"^``(\w+)``", doc, re.M))
    text = PAGE.read_text()
    table = text[text.index("## The `zarr_gbx` DataSource") :]
    table = table[: table.index("\n## ", 5)]
    listed = set(re.findall(r"^\| `(\w+)` \|", table, re.M))
    assert options == listed, options ^ listed


# --- sampling ---------------------------------------------------------------------------


def test_sample_a_time_series_at_points(spark, copy_of_cube, truth, new_table, capsys):
    table = new_table("sample")
    gbx_zarr_register(spark, copy_of_cube, table)
    capsys.readouterr()
    rows = ex.sample_at_points(spark, table).collect()
    assert shown(capsys) == ex.SAMPLE_output.strip()
    assert len(rows) == 2
    for r in rows:
        lon, lat = (2.0, 48.0) if r["name"] == "paris" else (0.0, 51.0)
        for var in ("t2m", "msl"):
            want = truth[var].sel(time="2023-01-01T06:00", latitude=lat, longitude=lon)
            assert r[var] == pytest.approx(float(want))


def test_points_in_another_crs_are_moved_to_the_stores(spark, cube, capsys):
    same, moved = ex.sample_projected_points(spark, cube)
    assert shown(capsys) == ex.SAMPLE_PROJECTED_output.strip()
    assert same.first()["t2m"] == pytest.approx(moved.first()["t2m"])


# --- errors -----------------------------------------------------------------------------


def test_an_unknown_variable_is_refused_on_the_driver_with_a_fix(spark, cube, capsys):
    ex.ask_for_an_unknown_variable(spark, cube)
    assert shown(capsys) == ex.UNKNOWN_VARIABLE_output.strip()


def test_an_error_in_a_task_arrives_as_a_python_exception_with_the_fix(
    spark, copy_of_cube, new_table, capsys
):
    table = new_table("drift")
    gbx_zarr_register(spark, copy_of_cube, table)
    shard = Path(copy_of_cube) / "t2m" / "c" / "0" / "0" / "0"
    stat = shard.stat()
    os.utime(shard, (stat.st_atime, stat.st_mtime + 30))  # another tool rewrote it
    capsys.readouterr()
    ex.read_a_changed_store(spark, table)
    assert shown(capsys) == ex.EXECUTOR_ERROR_output.strip()


# --- the page (no Spark) ------------------------------------------------------------------


@pytest.mark.structure
def test_every_snippet_the_page_shows_exists():
    import re

    text = PAGE.read_text()
    names = set(re.findall(r'functionName="([A-Za-z_0-9]+)"', text))
    names |= set(re.findall(r'outputConstant="([A-Za-z_0-9]+)"', text))
    assert names, "the page shows no code from the examples"
    missing = sorted(n for n in names if not hasattr(ex, n))
    assert not missing, f"not in zarr_read_examples.py: {missing}"


@pytest.mark.structure
def test_the_page_is_wired_in_and_speaks_to_users():
    import re

    sidebar = (DOCS.parent / "sidebars.js").read_text()
    assert "'readers/zarr'" in sidebar
    pattern = re.compile(r"\bwave[ -]?\d+|\btask \d+|gate g\d|ruling|\bZT\d+", re.I)
    text = PAGE.read_text()
    assert not pattern.findall(text), pattern.findall(text)
    formats = (DOCS / "formats" / "zarr.mdx").read_text()
    overview = (DOCS / "formats" / "overview.mdx").read_text()
    for page in (formats, overview):
        assert "../readers/zarr" in page
