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


def test_plot_tiles_mosaic_same_crs(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles

    # two adjacent tiles, same CRS -> one stitched image
    paths = [str(tmp_path / f"m{i}.tif") for i in range(2)]
    for p in paths:
        _write_tif(p, crs="EPSG:4326")
    df = _virtual_df(spark, paths)
    plt.close("all")
    ax = plot_tiles(df, mode="mosaic")
    assert ax is not None and len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_tiles_mosaic_mixed_crs_raises(spark, tmp_path):
    from databricks.labs.gbx.vizx import plot_tiles

    p1 = str(tmp_path / "a.tif")
    _write_tif(p1, crs="EPSG:4326")
    p2 = str(tmp_path / "b.tif")
    _write_tif(p2, crs="EPSG:3857")
    df = _virtual_df(spark, [p1, p2])
    with pytest.raises(ValueError, match="CRS"):
        plot_tiles(df, mode="mosaic")


def test_plot_tiles_facet_mixed_crs_ok(spark, tmp_path):
    # facet is CRS-agnostic -> no raise on mixed CRS
    from databricks.labs.gbx.vizx import plot_tiles

    p1 = str(tmp_path / "c.tif")
    _write_tif(p1, crs="EPSG:4326")
    p2 = str(tmp_path / "d.tif")
    _write_tif(p2, crs="EPSG:3857")
    df = _virtual_df(spark, [p1, p2])
    plt.close("all")
    fig = plot_tiles(df, mode="facet")
    assert fig is not None
    plt.close("all")


# ---------------------------------------------------------------------------
# Finding 3: empty DataFrame raises a clear ValueError in all modes
# ---------------------------------------------------------------------------


def test_plot_tiles_empty_df_first_raises(spark, tmp_path):
    """Empty DF -> ValueError with a helpful message (mode=first)."""
    from databricks.labs.gbx.vizx import plot_tiles

    p = str(tmp_path / "any.tif")
    _write_tif(p)
    df = _virtual_df(spark, [p]).filter("1 = 0")  # empty
    with pytest.raises(ValueError, match="empty"):
        plot_tiles(df, mode="first")


def test_plot_tiles_empty_df_facet_raises(spark, tmp_path):
    """Empty DF -> ValueError with a helpful message (mode=facet)."""
    from databricks.labs.gbx.vizx import plot_tiles

    p = str(tmp_path / "any.tif")
    _write_tif(p)
    df = _virtual_df(spark, [p]).filter("1 = 0")
    with pytest.raises(ValueError, match="empty"):
        plot_tiles(df, mode="facet")


def test_plot_tiles_empty_df_mosaic_raises(spark, tmp_path):
    """Empty DF -> ValueError with a helpful message (mode=mosaic)."""
    from databricks.labs.gbx.vizx import plot_tiles

    p = str(tmp_path / "any.tif")
    _write_tif(p)
    df = _virtual_df(spark, [p]).filter("1 = 0")
    with pytest.raises(ValueError, match="empty"):
        plot_tiles(df, mode="mosaic")


# ---------------------------------------------------------------------------
# Finding 4: first mode emits NO overflow warning on a multi-row DataFrame
# ---------------------------------------------------------------------------


def test_plot_tiles_first_no_overflow_warn(spark, tmp_path):
    """first-mode must never emit an overflow UserWarning for a multi-row DF."""
    import warnings

    from databricks.labs.gbx.vizx import plot_tiles

    paths = [str(tmp_path / f"fw{i}.tif") for i in range(5)]
    for p in paths:
        _write_tif(p)
    df = _virtual_df(spark, paths)
    plt.close("all")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        plot_tiles(df, mode="first")
    overflow_warns = [
        w
        for w in caught
        if issubclass(w.category, UserWarning) and "limit" in str(w.message).lower()
    ]
    assert (
        overflow_warns == []
    ), f"first mode should not warn about limit; got: {[str(w.message) for w in overflow_warns]}"
    plt.close("all")


# ---------------------------------------------------------------------------
# Finding 5: _extract_tile handles wrapper Row that has sibling path/raster col
# ---------------------------------------------------------------------------


def test_extract_tile_wrapper_with_sibling_path(tmp_path):
    """Wrapper dict with both 'tile' struct and a sibling 'path' key resolves correctly."""
    import rasterio
    from rasterio.transform import from_bounds

    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
    from databricks.labs.gbx.vizx._tiles import resolve_tile_row

    p = str(tmp_path / "sib.tif")
    data = (np.random.rand(32, 32) * 100).astype("uint16")
    transform = from_bounds(-104.0, 31.0, -103.9, 31.1, 32, 32)
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        height=32,
        width=32,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data, 1)

    vt_row = {
        "cellid": -1,
        "raster": None,
        "path": p,
        "window": {"col_off": 0, "row_off": 0, "width": 32, "height": 32},
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    # Wrapper has BOTH 'tile' struct AND a sibling 'path' at the top level.
    wrapper = {"tile": vt_row, "path": p}
    with resolve_tile_row(wrapper, tile_col="tile") as ds:
        assert ds.width == 32


# ---------------------------------------------------------------------------
# Finding 2: mosaic decimates — output dataset fits within max_pixels
# ---------------------------------------------------------------------------


def test_plot_tiles_mosaic_decimates(spark, tmp_path):
    """mosaic with a large tile (size > max_pixels) must decimate before rendering."""
    from databricks.labs.gbx.vizx import plot_tiles

    # Create a tile larger than max_pixels=100 so decimation is needed.
    paths = [str(tmp_path / f"big{i}.tif") for i in range(2)]
    for p in paths:
        _write_tif(p, size=256, crs="EPSG:4326")
    df = _virtual_df(spark, paths)
    plt.close("all")
    # max_pixels=100; merged width will be ~256 → must decimate
    ax = plot_tiles(df, mode="mosaic", max_pixels=100)
    assert ax is not None
    # The rendered raster array must be bounded by max_pixels on its largest axis
    # (proves decimation actually happened, not just a title relabel).
    assert ax.images, "expected a rendered raster image"
    arr = ax.images[0].get_array()
    assert (
        max(arr.shape[-2], arr.shape[-1]) <= 100
    ), f"mosaic not decimated: rendered dims {arr.shape} exceed max_pixels=100"
    plt.close("all")
