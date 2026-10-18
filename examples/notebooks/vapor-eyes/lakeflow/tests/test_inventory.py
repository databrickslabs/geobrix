"""transformations/_inventory.py: read_paths over a bronze inventory, joined back
to the inventory rows (local Spark)."""
import datetime
import os

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("databricks.labs.gbx.ds.paths")

from transformations._inventory import local_path, read_inventory, staged  # noqa: E402

S5P_OPTIONS = {"mode": "vector", "group": "/PRODUCT", "variables": "ch4,qa_value"}


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session


def _granule(path, n):
    """An S5P-like granule: n scanlines x 2 ground pixels in the PRODUCT group."""
    np = pytest.importorskip("numpy")
    Dataset = pytest.importorskip("netCDF4").Dataset

    with Dataset(path, "w") as ds:
        g = ds.createGroup("PRODUCT")
        g.createDimension("scanline", n)
        g.createDimension("ground_pixel", 2)
        for name, base in (("latitude", 31.0), ("longitude", -103.0)):
            v = g.createVariable(name, "f4", ("scanline", "ground_pixel"))
            v.standard_name = name
            v[:] = base + np.arange(n * 2, dtype="float32").reshape(n, 2) / 100
        g.createVariable("ch4", "f4", ("scanline", "ground_pixel"))[:] = 1800.0
        g.createVariable("qa_value", "f4", ("scanline", "ground_pixel"))[:] = 1.0


def _band(path, value):
    np = pytest.importorskip("numpy")
    rasterio = pytest.importorskip("rasterio")

    with rasterio.open(
        path, "w", driver="GTiff", width=8, height=8, count=1, dtype="uint16",
        crs="EPSG:32613", transform=rasterio.Affine(20, 0, 500000, 0, -20, 3500000),
    ) as dst:
        dst.write(np.full((1, 8, 8), value, dtype="uint16"))


def test_local_path_matches_to_local_path(spark):
    from databricks.labs.gbx.ds._listing import to_local_path

    paths = [
        "dbfs:/Volumes/c/s/v/s5p/a.nc",
        "file:/tmp/a.nc",
        "/Volumes/c/s/v/s5p/a.nc",
        "s3://bucket/a.nc",
        "dbfs:relative.nc",
        "/vsimem/a.nc",
    ]
    df = spark.createDataFrame([(p,) for p in paths], "path string")
    got = [r[0] for r in df.select(local_path("path")).collect()]
    assert got == [to_local_path(p) for p in paths]


def test_read_inventory_joins_each_row_to_its_inventory_row(spark, tmp_path):
    folder = tmp_path / "s5p"
    folder.mkdir()
    files = {"S5P_a_20230601T182251.nc": 3, "S5P_b_20230602T194520.nc": 5}
    for name, n in files.items():
        _granule(str(folder / name), n)
    days = {
        "S5P_a_20230601T182251.nc": datetime.date(2023, 6, 1),
        "S5P_b_20230602T194520.nc": datetime.date(2023, 6, 2),
    }
    inventory = spark.createDataFrame(
        [("dbfs:" + str(folder / name), name, day) for name, day in days.items()],
        "path string, source_file string, observation_date date",
    )

    pts = read_inventory(
        inventory, "netcdf_gbx", str(folder), S5P_OPTIONS,
        keep=["source_file", "observation_date"], num_partitions=2,
    )

    assert "_source_mtime" not in pts.columns
    counts = {
        (r["_source_path"], r["source_file"], r["observation_date"]): r["count"]
        for r in pts.groupBy("_source_path", "source_file", "observation_date")
        .count()
        .collect()
    }
    assert counts == {
        (str(folder / name), name, days[name]): n * 2 for name, n in files.items()
    }


def test_read_inventory_carries_the_band_of_each_tile(spark, tmp_path):
    folder = tmp_path / "sentinel2"
    folder.mkdir()
    rows = []
    for band, value in (("B11", 100), ("B12", 90)):
        path = str(folder / f"{band}_S2B_MSIL2A_20230805T171859.tif")
        _band(path, value)
        rows.append(("dbfs:" + path, band, "S2B_MSIL2A_20230805T171859"))
    inventory = spark.createDataFrame(rows, "path string, band string, item_id string")

    tiles = read_inventory(inventory, "gtiff_gbx", str(folder), keep=["band", "item_id"])

    got = sorted(
        (os.path.basename(r["_source_path"]), r["band"], r["item_id"])
        for r in tiles.collect()
    )
    assert got == [
        ("B11_S2B_MSIL2A_20230805T171859.tif", "B11", "S2B_MSIL2A_20230805T171859"),
        ("B12_S2B_MSIL2A_20230805T171859.tif", "B12", "S2B_MSIL2A_20230805T171859"),
    ]


def test_read_inventory_is_none_until_a_file_is_staged(spark, tmp_path):
    inventory = spark.createDataFrame([], "path string, observation_date date")
    missing = str(tmp_path / "missing")
    empty = tmp_path / "empty"
    empty.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    (other / "notes.txt").write_text("not a granule")

    for folder in (missing, str(empty), str(other)):
        assert not staged(folder, "netcdf_gbx")
        assert read_inventory(inventory, "netcdf_gbx", folder, S5P_OPTIONS) is None

    _granule(str(other / "S5P_c.nc"), 1)
    assert staged(str(other), "netcdf_gbx")
