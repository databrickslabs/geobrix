"""Offline tests for StageIvDownloader: file names, the 12Z day rule, validation, repair, a missing day, the read."""

import os
from datetime import date

import numpy as np
import pytest
import requests

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.core.crs import crs_equal  # noqa: E402
from databricks.labs.gbx.sample import StageIvDownloader, download_stage4  # noqa: E402
from databricks.labs.gbx.sample import stage4 as S  # noqa: E402

from .test_http_assets import Fetch  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("stage4-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(
        s,
        ("gbx_rst_", "gbx_pmtiles_"),
        only=["gbx_rst_fromcontent", "gbx_rst_getsubdataset"],
        level="L3",
    )
    yield s
    s.stop()


def write_hrap_nc(path, value=1.0):
    """A 5 x 4 CF grid on the HRAP polar stereographic projection with two variables, like one daily NWS file."""
    import netCDF4

    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("y", 4)
        ds.createDimension("x", 5)
        x = ds.createVariable("x", "f8", ("x",))
        x.standard_name = "projection_x_coordinate"
        x.units = "m"
        x[:] = -400_000.0 + 4762.5 * np.arange(5)
        y = ds.createVariable("y", "f8", ("y",))
        y.standard_name = "projection_y_coordinate"
        y.units = "m"
        y[:] = -5_500_000.0 - 4762.5 * np.arange(4)
        crs = ds.createVariable("crs", "i4")
        crs.grid_mapping_name = "polar_stereographic"
        crs.straight_vertical_longitude_from_pole = -105.0
        crs.latitude_of_projection_origin = 90.0
        crs.standard_parallel = 60.0
        crs.false_easting = 0.0
        crs.false_northing = 0.0
        crs.earth_radius = 6371200.0
        for name, fill in ((S.NWS_VARIABLE, value), ("normal", 0.0)):
            v = ds.createVariable(name, "f4", ("y", "x"), fill_value=-10000.0)
            v.grid_mapping = "crs"
            v.units = S.NWS_UNITS
            v[:] = np.full((4, 5), fill, "f4")
    return path


def test_day_rule_and_month_window():
    assert S.collect_date_of(date(2023, 6, 1)) == date(2023, 5, 31)
    assert S.collect_date_of(date(2023, 5, 2)) == date(2023, 5, 1)
    a = StageIvDownloader().assets([2023], [5])
    ends = [S.file_end_date(x[3]) for x in a]
    assert ends[0] == date(2023, 5, 2) and ends[-1] == date(2023, 6, 1) and len(a) == 31
    assert {S.collect_date_of(e).month for e in ends} == {5}


def test_one_netcdf_per_day():
    assert StageIvDownloader().assets([2023], [5])[0] == (
        "ST4.20230502",
        "apcp_24h",
        "https://water.noaa.gov/resources/downloads/precip/stageIV/2023/05/02/nws_precip_1day_20230502_conus.nc",
        "nws_precip_1day_20230502_conus.nc",
    )
    assert S.TO_MM == S.UNITS_TO_MM[S.NWS_UNITS]
    assert callable(download_stage4)  # exported with the class


def test_download_validates_and_is_idempotent(spark, tmp_path):
    f = Fetch(write_hrap_nc(str(tmp_path / "src.nc")))
    rows = (
        StageIvDownloader(_fetch=f)
        .download([2023], [5], str(tmp_path / "st4"), spark=spark)
        .collect()
    )
    assert (
        len(rows) == 31
        and all(r["is_out_file_valid"] for r in rows)
        and len(f.calls) == 31
    )
    StageIvDownloader(_fetch=f).download(
        [2023], [5], str(tmp_path / "st4"), spark=spark
    )
    assert len(f.calls) == 31  # every valid file is kept


def test_repair_refetches_only_the_invalid_day(spark, tmp_path):
    src = write_hrap_nc(str(tmp_path / "src.nc"))
    out = str(tmp_path / "st4")
    StageIvDownloader(_fetch=Fetch(src)).download([2023], [5], out, spark=spark)
    os.remove(os.path.join(out, "nws_precip_1day_20230505_conus.nc"))
    bad = Fetch(
        src, truncate=True
    )  # a cut-short NetCDF: the re-fetched day is invalid, nothing is published
    target = StageIvDownloader(_fetch=bad).download([2023], [5], out, spark=spark)
    assert len(bad.calls) == 1 and target.where("NOT is_out_file_valid").count() == 1
    good = Fetch(src)
    fixed = (
        StageIvDownloader(_fetch=good)
        .repair(target, [2023], [5], out, spark=spark)
        .collect()
    )
    assert [c[0] for c in good.calls] == [S.NWS_TEMPLATE.format(y=2023, m=5, d=5)]
    assert [r["item_id"] for r in fixed] == ["ST4.20230505"] and fixed[0][
        "is_out_file_valid"
    ]


def _http_error(status):
    resp = requests.Response()
    resp.status_code = status

    def fetch(url, outpath, allowed_hosts=None, **kw):
        raise requests.HTTPError(f"{status} for {url}", response=resp)

    return fetch


def test_a_day_missing_from_the_archive_raises_with_the_gdex_fix(spark, tmp_path):
    with pytest.raises(S.StageIvDayMissing, match="d507005") as e:
        StageIvDownloader(_fetch=_http_error(404)).download(
            [2023], [5], str(tmp_path / "st4"), spark=spark
        )
    assert "2023-05-02" in str(e.value) and "Fix:" in str(e.value)


def test_another_http_error_is_an_invalid_row(spark, tmp_path):
    rows = (
        StageIvDownloader(_fetch=_http_error(503))
        .download([2023], [5], str(tmp_path / "st4"), spark=spark)
        .collect()
    )
    assert len(rows) == 31 and not any(r["is_out_file_valid"] for r in rows)


def test_read_gives_one_hrap_tile_per_day(spark, tmp_path):
    from rasterio.io import MemoryFile

    out = str(tmp_path / "st4")
    StageIvDownloader(
        _fetch=Fetch(write_hrap_nc(str(tmp_path / "src.nc"), value=0.5))
    ).download([2023], [5], out, spark=spark)
    rows = StageIvDownloader().read(spark, out).collect()
    assert len(rows) == 31 and all("nws_precip_1day_" in r["source"] for r in rows)
    with MemoryFile(bytes(rows[0]["tile"]["raster"])) as mf, mf.open() as rds:
        assert (rds.width, rds.height, rds.count) == (5, 4, 1)
        assert crs_equal(rds.crs, S.HRAP_CRS) and abs(rds.transform.a) == pytest.approx(
            4762.5
        )
        assert float(rds.read(1).mean()) == pytest.approx(
            0.5
        )  # observation, not the second variable


@pytest.mark.integration
def test_real_nws_day(spark, tmp_path):
    """One real file (2023-05-02): the variable, its units and the HRAP grid the module pins, read the GeoBrix way."""
    import netCDF4
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.sample import _http_assets as H

    out = str(tmp_path / "st4")
    one_day = StageIvDownloader().assets([2023], [5])[:1]  # ST4.20230502
    # validated as any NetCDF (no variable named), so a different variable name is reported below, not hidden
    rows = H.fetch_all(
        one_day, out, H.netcdf_valid(), S._ALLOWED_HOSTS, spark=spark
    ).collect()
    assert (
        rows[0]["is_out_file_valid"] and rows[0]["out_file_sz"] > 1_000_000
    )  # 2023-05-01 was 3,681,112 bytes
    with netCDF4.Dataset(rows[0]["out_file_path"]) as ds:
        print(
            "variables:",
            {
                k: (v.dimensions, getattr(v, "units", None))
                for k, v in ds.variables.items()
            },
        )
        v = ds.variables[S.NWS_VARIABLE]
        assert v.ndim == 2 and v.units == S.NWS_UNITS
        gm = ds.variables[v.grid_mapping]
        print("grid mapping:", {a: gm.getncattr(a) for a in gm.ncattrs()})
        assert gm.grid_mapping_name == "polar_stereographic"
    tile = (
        StageIvDownloader()
        .read(spark, out)
        .where("source LIKE '%20230502%'")
        .first()["tile"]
    )
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as rds:
        print("GDAL:", rds.crs.to_wkt(), rds.transform, rds.width, rds.height)
        assert crs_equal(rds.crs, S.HRAP_CRS)
        assert (rds.width, rds.height) == (1121, 881) and abs(
            rds.transform.a
        ) == pytest.approx(4762.5, abs=0.5)
