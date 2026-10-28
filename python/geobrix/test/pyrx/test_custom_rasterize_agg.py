"""Custom rasterize_agg + consolidation-invariance checks.

Failing tests before implementation (Step 2 run-to-fail):
  - test_rst_custom_rasterize_agg_presence_mask
  - test_rst_custom_rasterize_agg_burns_value
  - test_rst_custom_rasterize_agg_null_cellid_returns_none

Passing consolidation-invariance tests (must remain passing before AND after
consolidation — they verify h3/quadbin/bng pixel values are unchanged):
  - test_h3_rasterize_agg_consolidation_invariance
  - test_quadbin_rasterize_agg_consolidation_invariance
  - test_bng_rasterize_agg_consolidation_invariance
"""

import h3
import numpy as np
import pandas as pd

from databricks.labs.gbx.pygx._custom import CustomGridConf
from databricks.labs.gbx.pygx._custom import polyfill as _custom_polyfill
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as rx

# ---------------------------------------------------------------------------
# Consolidation-invariance: UDF `.func` called directly with fixed inputs.
# These call the *named* UDF function objects, so they exercise whatever code
# is currently registered under that name — pre- or post-consolidation.  The
# expected pixel values are stable properties of the math, not byte hashes, so
# they are refactoring-independent as long as the computation is unchanged.
# ---------------------------------------------------------------------------


def test_h3_rasterize_agg_consolidation_invariance():
    """H3: one cell, value 42.0 -> all covered pixels equal 42.0 (no NaN, no 1.0)."""
    from databricks.labs.gbx.pyrx.functions import _rst_h3_rasterize_agg_udf

    res = 9
    c0 = int(h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, res)))

    cellid_s = pd.Series([c0], dtype="int64")
    value_s = pd.Series([42.0], dtype="float64")
    none_s = None  # triggers default paths in the UDF

    result = _rst_h3_rasterize_agg_udf.func(
        cellid_s,
        value_s,
        none_s,
        none_s,  # srid, pixel_size
        none_s,
        none_s,
        none_s,
        none_s,  # xmin, ymin, xmax, ymax
        none_s,
        none_s,  # width, height
        none_s,
        none_s,  # mode, kring_pad
    )
    assert result is not None, "H3 rasterize_agg returned None for valid input"
    with _serde.open_tile(bytes(result)) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size >= 1, "no covered pixels in H3 invariance output"
        assert np.all(covered == 42.0), (
            f"H3 consolidation changed pixel values: expected 42.0, "
            f"got {np.unique(covered)}"
        )
        assert ds.nodata == -9999.0, "nodata changed"


def test_quadbin_rasterize_agg_consolidation_invariance():
    """Quadbin: small set of cells (res=12), uniform value 42.0 -> all covered = 42.0."""
    from shapely import set_srid, to_wkb
    from shapely.geometry import box

    from databricks.labs.gbx.pygx import _quadbin as qb
    from databricks.labs.gbx.pyrx.functions import _rst_quadbin_rasterize_agg_udf

    # res=12 cells are ~0.088° wide; a 0.2°×0.2° box reliably yields >=1 cell
    # whose center lies inside, ensuring the pixel-burn loop hits at least one.
    res = 12
    ewkb = to_wkb(set_srid(box(-0.10, 51.50, 0.10, 51.70), 4326), include_srid=True)
    cells = qb.polyfill(ewkb, res)
    assert len(cells) >= 1, "polyfill returned no cells for test bbox"

    cellid_s = pd.Series([int(c) for c in cells], dtype="int64")
    value_s = pd.Series([42.0] * len(cells), dtype="float64")
    none_s = None

    result = _rst_quadbin_rasterize_agg_udf.func(
        cellid_s,
        value_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
    )
    assert result is not None, "quadbin rasterize_agg returned None for valid input"
    with _serde.open_tile(bytes(result)) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size >= 1, "no covered pixels in quadbin invariance output"
        assert np.all(covered == 42.0), (
            f"quadbin consolidation changed pixel values: expected 42.0, "
            f"got {np.unique(covered)}"
        )


def test_bng_rasterize_agg_consolidation_invariance():
    """BNG: one cell, value 42.0 -> all covered pixels equal 42.0, CRS=EPSG:27700."""
    from databricks.labs.gbx.pygx import _bng as bng
    from databricks.labs.gbx.pyrx.functions import _rst_bng_rasterize_agg_udf

    c0 = bng.point_as_cell(530000, 180000, 3)  # STRING cell id, e.g. "TQ3080"

    cellid_s = pd.Series([c0], dtype="object")
    value_s = pd.Series([42.0], dtype="float64")
    none_s = None

    result = _rst_bng_rasterize_agg_udf.func(
        cellid_s,
        value_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
        none_s,
    )
    assert result is not None, "BNG rasterize_agg returned None for valid input"
    with _serde.open_tile(bytes(result)) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size >= 1, "no covered pixels in BNG invariance output"
        assert np.all(covered == 42.0), (
            f"BNG consolidation changed pixel values: expected 42.0, "
            f"got {np.unique(covered)}"
        )
        assert ds.crs.to_epsg() == 27700, "BNG CRS changed after consolidation"


# ---------------------------------------------------------------------------
# Custom rasterize_agg tests (FAIL before implementation, PASS after)
# ---------------------------------------------------------------------------

# Custom grid: London EPSG:27700 coords, 4x4 km extent, cell_splits=2,
# root 4000x4000 -> at resolution 1: 4 cells of 2000x2000 each.
_CONF = CustomGridConf(
    bound_x_min=530000,
    bound_x_max=534000,
    bound_y_min=180000,
    bound_y_max=184000,
    cell_splits=2,
    root_cell_size_x=4000,
    root_cell_size_y=4000,
    srid=27700,
)


def _cells_res1():
    """4 custom cells at resolution 1 from polyfilling the full extent."""
    from shapely.geometry import box

    poly = box(530000, 180000, 534000, 184000)
    return _custom_polyfill(_CONF, poly, 1)


def _add_grid_col(df, conf):
    """Add a CUSTOM_GRID_SCHEMA struct literal column named 'grid' to df."""
    from pyspark.sql import functions as F

    return df.withColumn(
        "grid",
        F.struct(
            F.lit(conf.bound_x_min).cast("long").alias("bound_x_min"),
            F.lit(conf.bound_x_max).cast("long").alias("bound_x_max"),
            F.lit(conf.bound_y_min).cast("long").alias("bound_y_min"),
            F.lit(conf.bound_y_max).cast("long").alias("bound_y_max"),
            F.lit(conf.cell_splits).alias("cell_splits"),
            F.lit(conf.root_cell_size_x).alias("root_cell_size_x"),
            F.lit(conf.root_cell_size_y).alias("root_cell_size_y"),
            F.lit(conf.srid).alias("srid"),
        ),
    )


def test_rst_custom_rasterize_agg_presence_mask(spark):
    """Custom rasterize_agg with no value -> presence mask (1.0 on covered pixels)."""
    cells = _cells_res1()
    assert len(cells) >= 2, "need >=2 custom cells for presence-mask test"

    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    df = _add_grid_col(df, _CONF)

    out = (
        df.groupBy("tx")
        .agg(rx.rst_custom_rasterize_agg("cellid", "grid").alias("tile"))
        .collect()
    )
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        # At least one covered pixel per cell (centroid mode, res=1 cells are 2000m wide)
        assert (arr == 1.0).sum() >= len(cells), (
            f"expected >={len(cells)} presence pixels, "
            f"got {(arr == 1.0).sum()} covered"
        )
        assert ds.nodata == -9999.0


def test_rst_custom_rasterize_agg_burns_value(spark):
    """Custom rasterize_agg: a cell with value 42.0 must appear in the output raster."""
    cells = _cells_res1()
    c0 = int(cells[0])

    df = spark.createDataFrame([(c0, 42.0, "TX1")], ["cellid", "val", "tx"])
    df = _add_grid_col(df, _CONF)

    out = (
        df.groupBy("tx")
        .agg(rx.rst_custom_rasterize_agg("cellid", "grid", "val").alias("tile"))
        .collect()
    )
    with _serde.open_tile(bytes(out[0]["tile"]["raster"])) as ds:
        arr = ds.read(1)
        assert (
            arr == 42.0
        ).sum() >= 1, f"expected value 42.0 in raster, covered values: {np.unique(arr[arr != ds.nodata])}"


def test_rst_custom_rasterize_agg_null_cellid_returns_none():
    """Custom rasterize_agg: all-null cellid group -> None (no error raised)."""
    from pyspark.sql import Row

    from databricks.labs.gbx.pyrx.functions import _rst_custom_rasterize_agg_udf

    grid_row = Row(
        bound_x_min=530000,
        bound_x_max=534000,
        bound_y_min=180000,
        bound_y_max=184000,
        cell_splits=2,
        root_cell_size_x=4000,
        root_cell_size_y=4000,
        srid=27700,
    )

    cellid_s = pd.Series(pd.array([pd.NA], dtype="Int64"))
    value_s = pd.Series([42.0], dtype="float64")
    grid_s = pd.Series([grid_row])
    none_s = None

    result = _rst_custom_rasterize_agg_udf.func(
        cellid_s,
        value_s,
        grid_s,
        none_s,  # out_srid
        none_s,  # pixel_size
        none_s,
        none_s,
        none_s,
        none_s,  # xmin, ymin, xmax, ymax
        none_s,
        none_s,  # width, height
        none_s,
        none_s,  # mode, kring_pad
    )
    assert result is None, f"expected None for all-null cellid group, got {result!r}"
