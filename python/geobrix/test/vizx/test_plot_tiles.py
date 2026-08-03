import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pytest  # noqa: E402
import rasterio  # noqa: E402
from rasterio.transform import from_bounds  # noqa: E402


def _write_tif(path, size=32, crs="EPSG:4326"):
    data = (np.random.rand(size, size) * 100).astype("uint16")
    transform = from_bounds(-104.0, 31.0, -103.9, 31.1, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="uint16",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(data, 1)
    return size


def _virtual_df(spark, paths):
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile

    rows = []
    for p in paths:
        with rasterio.open(p) as ds:
            w, h = ds.width, ds.height
        rows.append(
            (VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, w, h)).to_row(),)
        )
    from pyspark.sql.types import StructField, StructType

    schema = StructType([StructField("tile", V2_TILE_SCHEMA, False)])
    return spark.createDataFrame([(row[0],) for row in rows], schema)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_plot_tiles_first(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles

    paths = [str(tmp_path / f"t{i}.tif") for i in range(3)]
    for p in paths:
        _write_tif(p)
    df = _virtual_df(spark, paths)
    plt.close("all")
    ax = plot_tiles(df, mode="first")
    assert ax is not None and len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_tiles_facet_bounded(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles

    paths = [str(tmp_path / f"f{i}.tif") for i in range(5)]
    for p in paths:
        _write_tif(p)
    df = _virtual_df(spark, paths)
    plt.close("all")
    fig = plot_tiles(df, mode="facet", limit=4)
    # 5 rows, limit 4 -> at most 4 panels rendered
    axes = fig.get_axes() if hasattr(fig, "get_axes") else []
    assert 1 <= len(axes) <= 4
    plt.close("all")


def test_plot_tiles_facet_warns_on_overflow(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles

    paths = [str(tmp_path / f"w{i}.tif") for i in range(6)]
    for p in paths:
        _write_tif(p)
    df = _virtual_df(spark, paths)
    plt.close("all")
    with pytest.warns(UserWarning, match="limit"):
        plot_tiles(df, mode="facet", limit=2)
    plt.close("all")
