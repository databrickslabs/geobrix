"""What the four cell rasterizers (``rst_{h3,quadbin,bng,custom}_rasterize_agg``) burn for one group of rows.

One table of (grid x group) rows. The UDF bodies are called directly (``.func``), so no Spark is needed, the row order
is the order given, and a nullable id column keeps its 64 bits (``Int64`` with ``pd.NA``, where Spark would hand the
body a rounded float).

An omitted value is the presence mask (1.0 on every covered pixel); a value burns on its own cell; a NULL cell id is
skipped together with ITS value (the values of the rows after it keep their own ids); a group with no usable id has no
raster. The largest-value-per-cell rule and the null rules for values are in ``test_rasterize_agg_duplicate_cells``.
"""

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("rasterio")

from test.pyrx import _duplicate_cells as dc  # noqa: E402

from databricks.labs.gbx.pyrx import _serde  # noqa: E402
from databricks.labs.gbx.pyrx import functions as rx  # noqa: E402

GRIDS = sorted(dc.GRIDS)


def _udf_args(grid, cellid, value):
    """The arguments of the body for a group: id, value, (the grid), then the constants left to their defaults."""
    if grid == "custom":
        from pyspark.sql import Row

        args = [cellid, value, pd.Series([Row(**dc.CUSTOM_GRID)])] + [None] * 10
        args[4], args[-1] = pd.Series([dc.CUSTOM_PIXEL_SIZE]), pd.Series([0])
        return args
    return [cellid, value] + [None] * 10


def _id_series(grid, ids):
    """The id column as Spark delivers it without a rounding step: strings for BNG, ``Int64`` (NULL = ``pd.NA``)."""
    if grid == "bng":
        return pd.Series(ids, dtype="object")
    return pd.Series(pd.array(ids, dtype="Int64"))


def _burn(grid, ids, values=None):
    """(first band, nodata, dataset CRS EPSG) of the tile one group produces, or None when the body gives no raster."""
    value = pd.Series(
        [None] * len(ids) if values is None else values,
        dtype=object if values is None else "float64",
    )
    udf = getattr(rx, f"_rst_{grid}_rasterize_agg_udf")
    raster = udf.func(*_udf_args(grid, _id_series(grid, ids), value))
    if raster is None:
        return None
    with _serde.open_tile(bytes(raster)) as ds:
        return ds.read(1), ds.nodata, ds.crs.to_epsg() if ds.crs else None


@pytest.mark.parametrize("grid", GRIDS)
def test_an_omitted_value_is_the_presence_mask(grid):
    cells = dc.GRIDS[grid]()
    pixels, nodata, epsg = _burn(grid, cells)
    assert nodata == -9999.0
    assert set(pixels[pixels != nodata].tolist()) == {1.0}
    assert (pixels == 1.0).sum() >= len(cells)
    if grid == "bng":
        assert epsg == 27700


@pytest.mark.parametrize("grid", GRIDS)
def test_a_value_burns_on_its_cell_and_nowhere_else(grid):
    cell = dc.GRIDS[grid]()[0]
    pixels, nodata, _ = _burn(grid, [cell], [42.0])
    assert set(pixels[pixels != nodata].tolist()) == {42.0}


@pytest.mark.parametrize("grid", GRIDS)
def test_a_null_cell_id_is_skipped_with_its_own_value(grid):
    c0, c1 = dc.GRIDS[grid]()[:2]
    pixels, nodata, _ = _burn(grid, [c0, None, c1], [10.0, 99.0, 20.0])
    assert set(pixels[pixels != nodata].tolist()) == {10.0, 20.0}


@pytest.mark.parametrize("grid", GRIDS)
def test_a_group_with_no_usable_cell_id_has_no_raster(grid):
    assert _burn(grid, [None], [42.0]) is None


def _cell_of(grid, lon, lat):
    """The cell of a point, from the grid library (or the grid's own point lookup for BNG, whose pixels are in metres)."""
    if grid == "h3":
        import h3

        return h3.str_to_int(h3.latlng_to_cell(lat, lon, 9))
    if grid == "quadbin":
        import quadbin

        return int(quadbin.point_to_cell(lon, lat, 12))
    from databricks.labs.gbx.pygx import _bng

    return _bng.format(_bng.point_to_cell_id(lon, lat, 3))


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
def test_a_pixel_is_burned_exactly_where_its_centre_is_in_a_cell_of_the_group(grid):
    cells = dc.GRIDS[grid]()
    pixels, nodata, _ = _burn(grid, cells)
    burned = pixels != nodata
    assert 0 < burned.sum() < burned.size  # some cell pixels, and some outside them
    # the transform of the tile gives each pixel centre; the cell is read from the grid library, not the body
    udf = getattr(rx, f"_rst_{grid}_rasterize_agg_udf")
    raster = udf.func(
        *_udf_args(
            grid, _id_series(grid, cells), pd.Series([None] * len(cells), dtype=object)
        )
    )
    with _serde.open_tile(bytes(raster)) as ds:
        t = ds.transform
        want = {str(c) for c in cells}
        for row in range(ds.height):
            for col in range(ds.width):
                x, y = t * (col + 0.5, row + 0.5)
                assert (str(_cell_of(grid, x, y)) in want) == bool(burned[row, col]), (
                    row,
                    col,
                )


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_a_null_id_in_a_bigint_column_does_not_round_the_other_ids(spark, grid):
    """Through the Python wrapper, an id past 2**53 beside a NULL id burns the same pixels as the id alone (Spark
    would hand a body that BIGINT column as float64 and round the id to another cell).
    """
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    cell = {"h3": dc.h3_cells, "quadbin": dc.quadbin_cells}[grid]()[0]
    if grid == "quadbin":
        import quadbin

        cell = int(
            quadbin.point_to_cell(-0.1, 51.5, 15)
        )  # a fine zoom, so the id is well past 2**53
    assert cell > 2**53
    schema = StructType(
        [
            StructField("cellid", LongType(), True),
            StructField("val", DoubleType(), True),
            StructField("group", StringType(), False),
        ]
    )
    df = spark.createDataFrame(
        [(cell, 7.0, "alone"), (cell, 7.0, "beside"), (None, 99.0, "beside")], schema
    )
    agg = getattr(rx, f"_light_rst_{grid}_rasterize_agg")
    tiles = {
        r["group"]: r["tile"]["raster"]
        for r in df.groupBy("group").agg(agg("cellid", "val").alias("tile")).collect()
    }
    got = {}
    for name, raster in tiles.items():
        with _serde.open_tile(bytes(raster)) as ds:
            band = ds.read(1)
            got[name] = {
                (int(r), int(c)): float(band[r, c])
                for r, c in zip(*np.where(band != ds.nodata))
            }
    assert got["alone"] and got["beside"] == got["alone"]
    assert set(got["beside"].values()) == {7.0}


def test_a_null_canvas_arriving_as_nan_derives_the_canvas_and_a_partial_one_raises():
    """A NULL INT or DOUBLE constant reaches a pandas UDF as NaN, not None. A NULL `xmin`..`height` derives the
    canvas from the cells (the documented contract); a canvas given in part names the NULL constants.
    """
    from databricks.labs.gbx.pyrx.core import cellraster

    ad = cellraster._adapter("bng")
    cells = ["TQ3080", "TQ3180"]
    values = rx._max_value_per_cell(cells, [1.0, 2.0], ad.to_key)

    def body(canvas, px=1000.0):
        s = lambda v: pd.Series([v])  # noqa: E731
        return rx._rasterize_agg_body(
            values,
            cells,
            ad,
            "bng",
            27700,
            s(px),
            *map(s, canvas),
            s("centroids"),
            s(0),
        )

    derived = body([None] * 6)
    assert derived is not None
    assert body([np.nan] * 6) == derived
    with pytest.raises(
        ValueError, match=r"given in part \(xmin\).*ymin.*are NULL.*Fix:"
    ):
        body([530000.0, np.nan, np.nan, np.nan, np.nan, np.nan])
