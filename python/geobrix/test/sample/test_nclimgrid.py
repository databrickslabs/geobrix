"""Offline tests for NclimgridDownloader (normals and monthly)."""

import contextlib
import os
import shutil
import sys

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.core import _http  # noqa: E402
from databricks.labs.gbx.sample import NclimgridDownloader  # noqa: E402
from databricks.labs.gbx.sample import nclimgrid as N  # noqa: E402

from .test_http_assets import Fetch, write_nc  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("nclimgrid-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_normals_and_monthly_names_and_hosts(spark, tmp_path):
    src = write_nc(str(tmp_path / "n.nc"), var=N.NORMALS_VARIABLE, days=12)
    normals_fetch = Fetch(src)
    n = (
        NclimgridDownloader(_fetch=normals_fetch)
        .normals(str(tmp_path / "normals"), spark=spark)
        .collect()
    )
    assert n[0]["item_id"] == "prcp_normals_2006_2020" and n[0]["is_out_file_valid"]
    monthly_fetch = Fetch(
        write_nc(str(tmp_path / "m.nc"), var=N.MONTHLY_VARIABLE, days=24)
    )
    NclimgridDownloader(_fetch=monthly_fetch).monthly(
        str(tmp_path / "monthly"), spark=spark
    )
    calls = normals_fetch.calls + monthly_fetch.calls
    assert [url for url, _ in calls] == [N.NORMALS_URL, N.MONTHLY_URL]
    for url, allowed_hosts in calls:
        # the allow-list that reaches the fetch must admit the host the URL names, by the rule core._http applies
        assert tuple(allowed_hosts) == N._ALLOWED_HOSTS
        assert _http._allowed(url.split("/")[2].lower(), allowed_hosts)


def test_wrong_variable_is_invalid(spark, tmp_path):
    src = write_nc(str(tmp_path / "n.nc"), var="other", days=12)
    n = (
        NclimgridDownloader(_fetch=Fetch(src))
        .normals(str(tmp_path / "normals"), spark=spark)
        .collect()
    )
    assert n[0]["is_out_file_valid"] is False


def test_repair_refetches_only_the_invalid_normals(spark, tmp_path):
    out = str(tmp_path / "normals")
    bad = write_nc(str(tmp_path / "bad.nc"), var="other", days=12)
    target = NclimgridDownloader(_fetch=Fetch(bad)).normals(out, spark=spark)
    good = Fetch(write_nc(str(tmp_path / "n.nc"), var=N.NORMALS_VARIABLE, days=12))
    fixed = NclimgridDownloader(_fetch=good).repair(target, out, spark=spark).collect()
    assert [c[0] for c in good.calls] == [
        N.NORMALS_URL
    ]  # the monthly file is not in the target: not fetched
    assert [r["item_id"] for r in fixed] == ["prcp_normals_2006_2020"] and fixed[0][
        "is_out_file_valid"
    ]


# Iowa cells for the period check: Story and Boone (Census internal points), Des Moines, Iowa City, Spencer.
IOWA_POINTS = [
    (-93.466093, 42.037538),
    (-93.938605, 42.038623),
    (-93.61, 41.59),
    (-91.53, 41.66),
    (-95.14, 43.14),
]


@pytest.fixture(scope="module")
def real_normals(spark, tmp_path_factory):
    folder = tmp_path_factory.mktemp("normals")
    rows = NclimgridDownloader().normals(str(folder), spark=spark).collect()
    assert (
        rows[0]["is_out_file_valid"] and rows[0]["out_file_sz"] > 100_000_000
    )  # 280,673,972 bytes at the time of writing
    yield rows[0]["out_file_path"]
    shutil.rmtree(folder, ignore_errors=True)  # not left in pytest's basetemp


def _nearest(values, target):
    import numpy as np

    return int(np.abs(np.asarray(values, dtype="f8") - target).argmin())


@contextlib.contextmanager
def _open_monthly(tmp_path_factory, spark):
    """nClimGrid monthly, opened by HTTP byte ranges (netCDF-C ``#mode=bytes``: only the header, the coordinates and
    the chunks read travel; no OPeNDAP server). A netCDF-C without byte-range support raises OSError; the whole
    1.8 GB file is then downloaded once (several minutes), which is why this check is integration-marked. That
    download is removed when the block ends, so it does not stay in pytest's basetemp.
    """
    import netCDF4

    try:
        ds = netCDF4.Dataset(N.MONTHLY_URL + "#mode=bytes")
        folder = None
    except OSError as exc:
        print(
            f"... byte-range open refused ({exc}); downloading the whole nClimGrid monthly file (1.8 GB)"
        )
        folder = tmp_path_factory.mktemp("monthly")
        rows = NclimgridDownloader().monthly(str(folder), spark=spark).collect()
        assert rows[0]["is_out_file_valid"]
        ds = netCDF4.Dataset(rows[0]["out_file_path"])
    try:
        yield ds
    finally:
        ds.close()
        if folder is not None:
            shutil.rmtree(folder, ignore_errors=True)


def test_the_whole_file_fallback_leaves_nothing_in_the_temp_folder(
    spark, tmp_path, tmp_path_factory, monkeypatch
):
    """Without byte-range support the monthly file is downloaded; it is removed when the check is done."""
    import netCDF4

    real = netCDF4.Dataset

    def no_ranges(path, *a, **kw):
        if str(path).endswith("#mode=bytes"):
            raise OSError("byte ranges refused")
        return real(path, *a, **kw)

    downloader = NclimgridDownloader  # the real class, before the name is replaced
    monthly = write_nc(str(tmp_path / "m.nc"), var=N.MONTHLY_VARIABLE, days=12)
    monkeypatch.setattr(netCDF4, "Dataset", no_ranges)
    monkeypatch.setattr(
        sys.modules[__name__],
        "NclimgridDownloader",
        lambda: downloader(_fetch=Fetch(monthly)),
    )
    with _open_monthly(tmp_path_factory, spark) as ds:
        folder = os.path.dirname(ds.filepath())
        assert os.path.exists(ds.filepath())
    assert not os.path.exists(folder)


@pytest.mark.integration
def test_real_normals(real_normals):
    import netCDF4

    with netCDF4.Dataset(real_normals) as ds:
        v = ds.variables[N.NORMALS_VARIABLE]
        assert v.dimensions == ("time", "lat", "lon") and v.shape[0] == 12
        assert v.units == "millimeter"  # so the series' NORMALS_TO_MM = 1.0
        assert [int(t) for t in ds.variables["time"][:]] == list(
            range(1, 13)
        )  # time = calendar month


@pytest.mark.integration
def test_real_normals_may_matches_the_2006_2020_nclimgrid_may_mean(
    real_normals, spark, tmp_path_factory
):
    """The file's name says 2006-2020 and its title says 1991-2020: the May normal must equal the 2006-2020 mean of
    the nClimGrid May totals (within 1%) at every Iowa cell checked."""
    import netCDF4
    import numpy as np

    with netCDF4.Dataset(real_normals) as ds:
        v, lat, lon = (
            ds.variables[N.NORMALS_VARIABLE],
            ds.variables["lat"][:],
            ds.variables["lon"][:],
        )
        normal_may = [
            float(v[4, _nearest(lat, y), _nearest(lon, x)]) for x, y in IOWA_POINTS
        ]
    with _open_monthly(tmp_path_factory, spark) as ds:
        v = ds.variables[N.MONTHLY_VARIABLE]
        assert v.dimensions[0] == "time" and v.units in (
            "mm",
            "millimeter",
            "millimeters",
        )
        t = ds.variables["time"]
        dates = netCDF4.num2date(t[:], t.units, getattr(t, "calendar", "standard"))
        mays = [
            i for i, d in enumerate(dates) if d.month == 5 and 2006 <= d.year <= 2020
        ]
        assert len(mays) == 15
        lat, lon = ds.variables["lat"][:], ds.variables["lon"][:]
        means = [
            float(
                np.mean([float(v[i, _nearest(lat, y), _nearest(lon, x)]) for i in mays])
            )
            for x, y in IOWA_POINTS
        ]
    gaps = {pt: abs(n - m) / m for pt, n, m in zip(IOWA_POINTS, normal_may, means)}
    print(
        "May normal vs 2006-2020 nClimGrid May mean (relative gap):",
        {p: f"{g:.3%}" for p, g in gaps.items()},
    )
    assert max(gaps.values()) <= 0.01, (
        f"the normals' May value differs from the 2006-2020 nClimGrid May mean by up to {max(gaps.values()):.2%}: "
        "the file may hold the 1991-2020 normals its title names, not the 2006-2020 normals."
    )
