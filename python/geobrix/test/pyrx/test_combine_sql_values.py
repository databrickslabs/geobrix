"""The gbx_rst_combine* SQL functions on one canonical stack, with the pixel values stated.

The six per-pixel statistics are light-only (lightweight level, registered through the manager), so
this test reads the result of the SQL call itself against the known value of each pixel. The Column
API and the numpy reducers are covered in test_combine_family.py; this is the SQL path over the
tile structs that ``gbx_rst_fromcontent`` builds.

Stack layout (2x2 FLOAT64 tiles, row-major, NoData=-9999):
  tile 0: [1, 10, ND, 4]
  tile 1: [2, ND, ND, 4]
  tile 2: [3, 20, ND, 4]

Pixel positions in flattened order:
  p0=(row0,col0): values=[1,2,3]    -> all valid
  p1=(row0,col1): values=[10,ND,20] -> valid=[10,20]   (even-count median / stddev exercised)
  p2=(row1,col0): values=[ND,ND,ND] -> all-NoData      -> NoData in output
  p3=(row1,col1): values=[4,4,4]    -> all valid

Per-pixel expected values:
         p0         p1       p2   p3
  min:   1.0        10.0     ND   4.0
  max:   3.0        20.0     ND   4.0
  sum:   6.0        30.0     ND  12.0
  count: 3.0         2.0     ND   3.0
  median:2.0        15.0     ND   4.0     (even-count: (10+20)/2=15)
  stddev:sqrt(2/3)   5.0     ND   0.0     (population ddof=0; {10,20}: std=5)
"""

import numpy as np
import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (the lightweight tier required)",
)

_ND = -9999.0

_EXPECTED = {
    "min": [1.0, 10.0, _ND, 4.0],
    "max": [3.0, 20.0, _ND, 4.0],
    "sum": [6.0, 30.0, _ND, 12.0],
    "count": [3.0, 2.0, _ND, 3.0],
    "median": [2.0, 15.0, _ND, 4.0],
    "stddev": [(2.0 / 3.0) ** 0.5, 5.0, _ND, 0.0],
}


def _make_tile(values, *, crs, ulx, uly, px, nodata=_ND, dtype="float64"):
    """Single-band GTiff bytes from a flat list of pixel values."""
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    n = len(values)
    side = int(n**0.5)
    assert side * side == n, f"_make_tile: {n} values must form a square grid"
    arr = np.array(values, dtype=dtype).reshape(1, side, side)
    profile = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=1,
        dtype=dtype,
        crs=crs,
        transform=from_origin(ulx, uly, px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr)
        return mf.read()


def _canonical_tiles():
    """Three aligned 2x2 FLOAT64 EPSG:4326 tiles with the standard NoData hole."""
    return [
        _make_tile([1.0, 10.0, _ND, 4.0], crs="EPSG:4326", ulx=0.0, uly=2.0, px=1.0),
        _make_tile([2.0, _ND, _ND, 4.0], crs="EPSG:4326", ulx=0.0, uly=2.0, px=1.0),
        _make_tile([3.0, 20.0, _ND, 4.0], crs="EPSG:4326", ulx=0.0, uly=2.0, px=1.0),
    ]


@pytest.fixture(scope="module")
def combine_view(spark):
    """A one-row view whose ``tiles`` column is the canonical stack as an array of tile structs."""
    from pyspark.sql.types import ArrayType, StructField, StructType

    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    raw = spark.createDataFrame([(t,) for t in _canonical_tiles()], "content binary")
    built = raw.selectExpr("gbx_rst_fromcontent(content, 'GTiff') AS t")
    structs = [r["t"] for r in built.collect()]
    df = spark.createDataFrame(
        [{"tiles": structs}],
        schema=StructType(
            [StructField("tiles", ArrayType(built.schema["t"].dataType), True)]
        ),
    )
    df.createOrReplaceTempView("_combine_sql_values_tbl")
    return spark


def _decode(tile_bytes: bytes):
    """(flat pixels, nodata) of single-band GeoTIFF bytes."""
    from databricks.labs.gbx.pyrx import _serde

    with _serde.open_tile(tile_bytes) as ds:
        return ds.read(1).astype("float64").flatten(), ds.nodata


@pytest.mark.parametrize("stat", sorted(_EXPECTED))
def test_combine_stat_sql_values(combine_view, stat):
    """gbx_rst_combine{stat} over the canonical stack gives each pixel its known value.

    Covers the valid-pixel values within 1e-9, the all-NoData pixel p2 left at the NoData
    sentinel the output declares, the even-count median (p1 = 15.0) and the population stddev.
    """
    rows = combine_view.sql(
        f"SELECT gbx_rst_combine{stat}(tiles) AS result FROM _combine_sql_values_tbl"
    ).collect()
    assert rows and rows[0]["result"] is not None, f"{stat} returned no row"
    arr, nodata = _decode(bytes(rows[0]["result"]["raster"]))

    assert arr.shape == (4,)
    assert nodata == _ND
    for idx, exp in enumerate(_EXPECTED[stat]):
        assert abs(float(arr[idx]) - exp) <= 1e-9 * max(
            1.0, abs(exp)
        ), f"combine_{stat} pixel[{idx}] expected={exp} got={arr[idx]:.15g}"
