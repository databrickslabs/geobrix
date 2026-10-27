"""Cross-tier (light pyrx vs heavy Scala) parity for repeated cell ids in the cell rasterizers.

A cell id that repeats in a group with different values burns the LARGEST value in both
tiers, whatever order the rows arrive in. For each of ``rst_{h3,quadbin,bng,custom}_rasterize_agg``
the same repeated rows are sent through both tiers in two row orders (as given, and
reversed), onto one explicit canvas, and the four rasters must be identical, pixel for
pixel, with only the largest value of each cell burned. A null value is the presence mask
(1.0) and a negative value is compared as a number, so the rows include both.

HARNESS REUSE: the shared canvas comes from the light ``cellraster.compute_gridspec`` as in
``test_parity_bng_quadbin_raster_grid.py``; the custom grid derives its canvas in both
tiers from the cell set, as in ``test_parity_custom_raster.py``. Light is collected FIRST,
because ``hx.register`` overwrites the shared ``gbx_rst_*`` SQL names.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/pyrx/test_parity_rasterize_duplicate_cells.py \\
        --skip-build --log rasterize-duplicates-parity.log
"""

import logging
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("rasterio")

from test.pyrx import _duplicate_cells as dc  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_NODATA = -9999.0
_SRID = {"h3": 4326, "quadbin": 4326, "bng": 27700, "custom": dc.CUSTOM_GRID["srid"]}


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live; run this test in isolation: "
                "gbx:test:parity --path "
                "python/geobrix/test/pyrx/test_parity_rasterize_duplicate_cells.py"
            )

    yield (
        SparkSession.builder.master("local[2]")
        .appName("gbx-rasterize-duplicates-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )


def _frame(spark, grid, rows):
    """One group (``tx``) of ``(cellid, val)`` rows, in the order given, in ONE partition."""
    from pyspark.sql import functions as f

    cell_type = "string" if grid == "bng" else "long"
    values = [(c, None if np.isnan(v) else float(v), "TX1") for c, v in rows]
    df = spark.createDataFrame(values, f"cellid {cell_type}, val double, tx string")
    df = df.coalesce(1)
    if grid == "custom":
        conf = dc.CUSTOM_GRID
        df = df.withColumn(
            "grid",
            f.struct(
                f.lit(conf["bound_x_min"]).cast("long").alias("bound_x_min"),
                f.lit(conf["bound_x_max"]).cast("long").alias("bound_x_max"),
                f.lit(conf["bound_y_min"]).cast("long").alias("bound_y_min"),
                f.lit(conf["bound_y_max"]).cast("long").alias("bound_y_max"),
                f.lit(conf["cell_splits"]).alias("cell_splits"),
                f.lit(conf["root_cell_size_x"]).alias("root_cell_size_x"),
                f.lit(conf["root_cell_size_y"]).alias("root_cell_size_y"),
                f.lit(conf["srid"]).alias("srid"),
            ),
        )
    return df


def _canvas(grid, cells):
    """Explicit canvas kwargs shared by both tiers (custom derives its own, equally)."""
    from pyspark.sql import functions as f

    if grid == "custom":
        return dict(pixel_size=f.lit(dc.CUSTOM_PIXEL_SIZE), kring_pad=f.lit(0))
    from databricks.labs.gbx.pyrx.core import cellraster

    xmin, ymin, xmax, ymax, pixel_size, width, height, srid = (
        cellraster.compute_gridspec(cells, srid=_SRID[grid], kring_pad=1, grid=grid)
    )
    return dict(
        out_srid=f.lit(srid),
        pixel_size=f.lit(pixel_size),
        xmin=f.lit(xmin),
        ymin=f.lit(ymin),
        xmax=f.lit(xmax),
        ymax=f.lit(ymax),
        width=f.lit(width),
        height=f.lit(height),
        mode=f.lit("centroids"),
        kring_pad=f.lit(1),
    )


def _light(df, grid, canvas):
    from databricks.labs.gbx.pyrx import functions as prx

    fn = getattr(prx, f"rst_{grid}_rasterize_agg")
    args = ("cellid", "grid") if grid == "custom" else ("cellid",)
    return (
        df.groupBy("tx").agg(fn(*args, value="val", **canvas).alias("tile")).collect()
    )


def _heavy(df, grid, canvas):
    from pyspark.sql import functions as f

    if grid == "custom":
        # 13 positional arguments: cellid, value, grid, out_srid, pixel_size, xmin .. height,
        # mode, kring_pad. The canvas is derived from the cells in both tiers.
        none_d, none_i = f.lit(None).cast("double"), f.lit(None).cast("int")
        agg = f.call_function(
            "gbx_rst_custom_rasterize_agg",
            f.col("cellid"),
            f.col("val"),
            f.col("grid"),
            f.lit(dc.CUSTOM_GRID["srid"]),
            canvas["pixel_size"],
            none_d,
            none_d,
            none_d,
            none_d,
            none_i,
            none_i,
            f.lit("centroids"),
            canvas["kring_pad"],
        )
    else:
        from databricks.labs.gbx.rasterx import functions as hx

        agg = getattr(hx, f"rst_{grid}_rasterize_agg")(
            f.col("cellid"),
            f.col("val"),
            *[
                canvas[k]
                for k in (
                    "out_srid",
                    "pixel_size",
                    "xmin",
                    "ymin",
                    "xmax",
                    "ymax",
                    "width",
                    "height",
                    "mode",
                    "kring_pad",
                )
            ],
        )
    return df.groupBy("tx").agg(agg.alias("tile")).collect()


def _read(out, label):
    from databricks.labs.gbx.pyrx import _serde

    assert len(out) == 1, f"{label}: expected one tile"
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None, f"{label}: no raster"
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.nodata == _NODATA, f"{label}: band NoData must be {_NODATA}"
        return ds.read(1)


@pytest.mark.parametrize("grid", sorted(dc.GRIDS))
def test_repeated_cells_burn_the_largest_value_in_both_tiers(spark_with_jar, grid):
    from databricks.labs.gbx.rasterx import functions as hx

    spark = spark_with_jar
    cells = dc.GRIDS[grid]()
    assert len(cells) >= 4, f"{grid}: need four cells, got {len(cells)}"
    rows, expected = dc.duplicate_rows(cells)
    orders = {"as given": rows, "reversed": rows[::-1]}
    canvas = _canvas(grid, cells)

    # LIGHT first: hx.register below overwrites the shared gbx_rst_* SQL names.
    light = {
        name: _read(_light(_frame(spark, grid, order), grid, canvas), f"light ({name})")
        for name, order in orders.items()
    }
    hx.register(spark)
    heavy = {
        name: _read(_heavy(_frame(spark, grid, order), grid, canvas), f"heavy ({name})")
        for name, order in orders.items()
    }

    burned = set(expected.values())
    rasters = {
        f"{tier} ({name})": a
        for tier, d in (("light", light), ("heavy", heavy))
        for name, a in d.items()
    }
    for label, arr in rasters.items():
        covered = set(arr[arr != _NODATA].tolist())
        assert covered, f"{grid} {label}: nothing burned"
        assert (
            covered <= burned
        ), f"{grid} {label}: a smaller duplicate was burned: {sorted(covered - burned)}"

    reference_label, reference = "heavy (as given)", rasters["heavy (as given)"]
    for label, arr in rasters.items():
        assert (
            arr.shape == reference.shape
        ), f"{grid}: {label} canvas differs from {reference_label}"
        diverging = np.argwhere(arr != reference)
        assert not len(diverging), (
            f"{grid}: {label} differs from {reference_label} at {len(diverging)} pixel(s), "
            f"first (row, col): {diverging[:5].tolist()}"
        )
