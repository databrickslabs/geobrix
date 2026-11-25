"""Offline tests for the shared HTTP asset helper (no network: an injected fetch copies local bytes)."""

import os
import re

import numpy as np
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.core import _http  # noqa: E402
from databricks.labs.gbx.sample import _http_assets as H  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("http-assets-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def write_nc(path, var="wind_speed", days=3):
    import netCDF4

    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("day", days)
        ds.createDimension("lat", 4)
        ds.createDimension("lon", 5)
        ds.createVariable("lat", "f8", ("lat",))[:] = [42.2, 42.1, 42.0, 41.9]
        ds.createVariable("lon", "f8", ("lon",))[:] = [
            -94.0,
            -93.9,
            -93.8,
            -93.7,
            -93.6,
        ]
        v = ds.createVariable(var, "f4", ("day", "lat", "lon"))
        v[:] = np.arange(days * 20, dtype="f4").reshape(days, 4, 5)
    return path


class Fetch:
    """Stands in for core._http.download: copies `src`, optionally truncated, and records each call."""

    def __init__(self, src, truncate=False, fail=False):
        self.src, self.truncate, self.fail, self.calls = src, truncate, fail, []

    def __call__(self, url, outpath, allowed_hosts=None, **kw):
        self.calls.append((url, allowed_hosts))
        if self.fail:
            raise _http.RefusedHref(f"{url}: refused")
        data = open(self.src, "rb").read()
        with open(outpath, "wb") as fh:
            fh.write(data[: len(data) // 2] if self.truncate else data)
        return outpath


def test_fetch_publishes_validates_and_reuses(tmp_path):
    src = write_nc(str(tmp_path / "src.nc"))
    dest = str(tmp_path / "out" / "vs_2023.nc")
    f = Fetch(src)
    ok = H.fetch_asset(
        "https://www.northwestknowledge.net/x.nc",
        dest,
        H.netcdf_valid("wind_speed"),
        ("x",),
        fetch=f,
    )
    assert ok and os.path.exists(dest) and len(f.calls) == 1
    assert H.fetch_asset(
        "https://www.northwestknowledge.net/x.nc",
        dest,
        H.netcdf_valid("wind_speed"),
        ("x",),
        fetch=f,
    )
    assert len(f.calls) == 1  # a valid file is kept
    assert not os.path.exists(str(tmp_path / "out" / ".gbx_scratch"))


def test_truncated_netcdf_is_invalid_and_refetched(tmp_path, spark):
    # a half-written file is invalid, publishes nothing, and a re-run fetches only it
    src = write_nc(str(tmp_path / "src.nc"))
    out = str(tmp_path / "out")
    good = Fetch(src)
    assets = [
        ("a", "vs", "https://h/a.nc", "a.nc"),
        ("b", "vs", "https://h/b.nc", "b.nc"),
    ]
    H.fetch_all(
        assets[:1], out, H.netcdf_valid("wind_speed"), ("h",), fetch=good, spark=spark
    )
    bad = Fetch(src, truncate=True)
    res = {
        r["item_id"]: r
        for r in H.fetch_all(
            assets, out, H.netcdf_valid("wind_speed"), ("h",), fetch=bad, spark=spark
        ).collect()
    }
    assert res["a"]["is_out_file_valid"] and not res["b"]["is_out_file_valid"]
    assert [c[0] for c in bad.calls] == [
        "https://h/b.nc"
    ]  # only the missing one was fetched
    assert not os.path.exists(
        os.path.join(out, "b.nc")
    )  # nothing partial under the final name
    fixed = H.fetch_all(
        assets, out, H.netcdf_valid("wind_speed"), ("h",), fetch=good, spark=spark
    ).collect()
    assert all(r["is_out_file_valid"] for r in fixed)
    assert [c[0] for c in good.calls] == ["https://h/a.nc", "https://h/b.nc"]


def test_refused_host_gives_an_invalid_row(tmp_path, spark):
    src = write_nc(str(tmp_path / "src.nc"))
    rows = H.fetch_all(
        [("a", "vs", "https://evil/a.nc", "a.nc")],
        str(tmp_path / "o"),
        H.netcdf_valid(),
        ("h",),
        fetch=Fetch(src, fail=True),
        spark=spark,
    ).collect()
    assert (
        rows[0]["is_out_file_valid"] is False
        and rows[0]["out_file_path"] is None
        and rows[0]["out_file_sz"] == 0
    )


def test_raster_validation(tmp_path):
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=64,
            height=64,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(-94, 42, 0.01, 0.01),
        ) as ds:
            ds.write(np.ones((64, 64), "float32"), 1)
        raw = mf.read()
    src = str(tmp_path / "src.tif")
    with open(src, "wb") as fh:
        fh.write(raw)
    dest = str(tmp_path / "o" / "x.tif")
    assert H.fetch_asset(
        "https://h/x.tif", dest, H.raster_valid, ("h",), fetch=Fetch(src)
    )
    assert H.raster_valid(dest)
    assert not H.fetch_asset(
        "https://h/y.tif",
        str(tmp_path / "o" / "y.tif"),
        H.raster_valid,
        ("h",),
        fetch=Fetch(src, truncate=True),
    )  # a cut-short GeoTIFF is not published


def test_result_schema(tmp_path, spark):
    src = write_nc(str(tmp_path / "src.nc"))
    df = H.fetch_all(
        [("a", "vs", "https://h/a.nc", "a.nc")],
        str(tmp_path / "o"),
        H.netcdf_valid(),
        ("h",),
        fetch=Fetch(src),
        spark=spark,
    )
    assert [f.name for f in df.schema.fields] == [
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    ]


class RecordingSpark:
    """Delegates to a real session but records spark.sql text instead of running it (local Spark has no Delta MERGE)."""

    def __init__(self, session):
        self._session, self.statements = session, []

    def sql(self, text):
        self.statements.append(text)

    def __getattr__(self, name):
        return getattr(self._session, name)


def _one_valid_one_truncated(tmp_path, spark):
    src = write_nc(str(tmp_path / "src.nc"))
    out = str(tmp_path / "out")
    assets = [
        ("a", "vs", "https://h/a.nc", "a.nc"),
        ("b", "vs", "https://h/b.nc", "b.nc"),
    ]
    H.fetch_all(
        assets[:1],
        out,
        H.netcdf_valid("wind_speed"),
        ("h",),
        fetch=Fetch(src),
        spark=spark,
    )
    first = H.fetch_all(
        assets,
        out,
        H.netcdf_valid("wind_speed"),
        ("h",),
        fetch=Fetch(src, truncate=True),
        spark=spark,
    )
    return src, out, assets, first


def test_repair_rows_refetches_only_matching_rows_and_merges_a_table(tmp_path, spark):
    src, out, assets, first = _one_valid_one_truncated(tmp_path, spark)
    first.createOrReplaceTempView("h3px_assets")
    rec, good = RecordingSpark(spark), Fetch(src)
    fixed = H.repair_rows(
        "h3px_assets",
        assets,
        out,
        H.netcdf_valid("wind_speed"),
        ("h",),
        fetch=good,
        spark=rec,
    ).collect()
    assert [c[0] for c in good.calls] == [
        "https://h/b.nc"
    ]  # only the invalid row is re-fetched
    assert [r["item_id"] for r in fixed] == ["b"] and fixed[0]["is_out_file_valid"]
    assert len(rec.statements) == 1
    merge = " ".join(rec.statements[0].split())
    view = re.match(r"MERGE INTO `h3px_assets` t USING (_gbx_repaired_\w+) u", merge)
    assert view and view.group(1) != "_gbx_repaired_assets"  # a per-call view name
    assert not spark.catalog.tableExists(view.group(1))  # dropped after the MERGE
    assert (
        "ON t.item_id = u.item_id AND t.asset_name = u.asset_name WHEN MATCHED THEN UPDATE SET"
        in merge
    )
    for col in ("out_file_path", "out_file_sz", "is_out_file_valid", "last_update"):
        assert f"{col} = u.{col}" in merge


def test_two_repairs_use_their_own_temp_view(tmp_path, spark):
    src, out, assets, first = _one_valid_one_truncated(tmp_path, spark)
    first.createOrReplaceTempView("h3px_assets")
    rec = RecordingSpark(spark)
    for _ in range(2):
        H.repair_rows(
            "h3px_assets",
            assets,
            out,
            H.netcdf_valid("wind_speed"),
            ("h",),
            fetch=Fetch(src),
            spark=rec,
        )
    views = [re.search(r"USING (\S+) u", st).group(1) for st in rec.statements]
    assert len(set(views)) == 2


@pytest.mark.parametrize(
    "target, quoted",
    [
        ("t", "`t`"),
        ("main.gx.assets", "`main`.`gx`.`assets`"),
        ("`main`.`gx`.`assets`", "`main`.`gx`.`assets`"),
        ("a`b.c", "`a``b`.`c`"),
    ],
)
def test_the_merge_target_is_quoted_per_part(target, quoted):
    assert H._quote_table(target) == quoted


def test_repair_rows_on_a_dataframe_returns_rows_and_runs_no_merge(tmp_path, spark):
    src, out, assets, first = _one_valid_one_truncated(tmp_path, spark)
    rec, good = RecordingSpark(spark), Fetch(src)
    fixed = H.repair_rows(
        first, assets, out, H.netcdf_valid("wind_speed"), ("h",), fetch=good, spark=rec
    ).collect()
    assert (
        [r["item_id"] for r in fixed] == ["b"]
        and fixed[0]["is_out_file_valid"]
        and rec.statements == []
    )
    again = H.repair_rows(
        first.where("item_id = 'a'"),
        assets,
        out,
        H.netcdf_valid("wind_speed"),
        ("h",),
        fetch=good,
        spark=rec,
    ).collect()
    assert again == [] and len(good.calls) == 1  # nothing matches: nothing fetched
