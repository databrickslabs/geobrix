"""Offline tests for CsbDownloader: one zip copy, one extract, a single-pass state clip, idempotent re-runs."""

import os
import zipfile

import numpy as np
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from databricks.labs.gbx.core import _http  # noqa: E402
from databricks.labs.gbx.sample import CsbDownloader  # noqa: E402
from databricks.labs.gbx.sample import csb as C  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("csb-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(s, only=["gpkg_gbx"])
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def _no_local_disk_floor(monkeypatch):
    """The tests use tiny zips: the library's free-disk floor (the real national zip's size) must not read the
    runner's disk. ``test_download_refuses_without_local_disk`` sets it huge to prove the raise.
    """
    monkeypatch.setattr(C, "MIN_LOCAL_FREE_BYTES", 0)


def _tiny_national(tmp_path):
    """A GeoPackage with the CSB layer name and fields: 3 Iowa polys, 2 Illinois, zipped like the national file."""
    import pyogrio
    import shapely

    polys = [shapely.box(x, 0, x + 100, 100) for x in range(0, 500, 100)]
    geom = np.array(
        [shapely.to_wkb(shapely.MultiPolygon([p])) for p in polys], dtype=object
    )
    state = np.array(["19", "19", "19", "17", "17"], dtype=object)
    fields = [
        np.array([f"id{i}" for i in range(5)], dtype=object),
        state,
        np.array(["169", "169", "015", "001", "001"], dtype=object),
    ]
    fields += [np.array([1, 5, 1, 1, 1], dtype=np.int32) for _ in range(8)]
    names = ["CSBID", "STATEFIPS", "CNTYFIPS"] + [f"CDL{y}" for y in range(2018, 2026)]
    gpkg = str(tmp_path / "national.gpkg")
    pyogrio.raw.write(
        gpkg,
        geom,
        fields,
        names,
        layer=C.LAYER,
        driver="GPKG",
        crs="EPSG:5070",
        geometry_type="MultiPolygon",
        layer_options={"GEOMETRY_NAME": "shape"},
    )  # as the FileGDB's SHAPE
    z = str(tmp_path / "NationalCSB_tiny.zip")
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(gpkg, "CSB_tiny/national.gpkg")
    return z


class _Fetch:
    def __init__(self, src):
        self.src, self.calls = src, 0

    def __call__(self, url, outpath, allowed_hosts=None, **kw):
        self.calls += 1
        with open(self.src, "rb") as a, open(outpath, "wb") as b:
            b.write(a.read())
        return outpath


LAST_MODIFIED = "Mon, 05 Oct 2026 16:31:44 GMT"


def _probe(src, last_modified=LAST_MODIFIED):
    """The national zip's server: no ETag, a size and a Last-Modified."""
    return lambda url, allowed_hosts=None, timeout=100: _http.RangeProbe(
        False, os.path.getsize(src), None, last_modified
    )


def test_download_clips_one_state_and_reuses_everything(spark, tmp_path):
    z = _tiny_national(tmp_path)
    f = _Fetch(z)
    out = str(tmp_path / "csb")
    dl = CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=f,
        _probe=_probe(z),
    )
    rows = dl.download(out, state_fips="19", spark=spark).collect()
    assert (
        len(rows) == 1
        and rows[0]["is_out_file_valid"]
        and rows[0]["out_file_path"].endswith("/csb_19.gpkg")
    )
    df = dl.read(spark, out, "19")
    assert df.count() == 3 and {
        r["STATEFIPS"] for r in df.select("STATEFIPS").collect()
    } == {"19"}
    assert (
        "geom" in df.columns and "shape" not in df.columns
    )  # the source says shape; the output says geom
    assert pyogrio_geometry_name(rows[0]["out_file_path"]) == C.GEOM_COLUMN == "geom"
    dl.download(out, state_fips="19", spark=spark)
    assert f.calls == 1  # the zip copy, the extract and the clip are all reused
    assert not os.path.exists(os.path.join(out, ".gbx_scratch"))


def pyogrio_geometry_name(path):
    import pyogrio

    return pyogrio.read_info(path, layer="csb")["geometry_name"]


def test_repair_reclips_an_invalid_state_and_skips_a_valid_one(spark, tmp_path):
    z = _tiny_national(tmp_path)
    f = _Fetch(z)
    out = str(tmp_path / "csb")
    dl = CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=f,
        _probe=_probe(z),
    )
    rows = dl.download(out, state_fips="19", spark=spark)
    assert (
        dl.repair(rows, out, state_fips="19", spark=spark).count() == 0
    )  # valid: nothing to repair
    with open(
        os.path.join(out, "csb_19.gpkg"), "wb"
    ) as fh:  # a broken GeoPackage under the final name
        fh.write(b"\0" * 4096)
    broken = rows.withColumn("is_out_file_valid", F.lit(False))
    fixed = dl.repair(broken, out, state_fips="19", spark=spark).collect()
    assert (
        len(fixed) == 1 and fixed[0]["is_out_file_valid"] and f.calls == 1
    )  # re-clipped from the cached zip
    assert dl.read(spark, out, "19").count() == 3


def test_clip_counts(tmp_path):
    import pyogrio

    z = _tiny_national(tmp_path)
    with zipfile.ZipFile(z) as zf:
        zf.extractall(tmp_path / "x")
    n = C.clip_gdb_to_state(
        str(tmp_path / "x" / "CSB_tiny" / "national.gpkg"),
        C.LAYER,
        "17",
        str(tmp_path / "il.gpkg"),
    )
    assert n == 2 and pyogrio.read_info(str(tmp_path / "il.gpkg"))["features"] == 2


def test_a_new_last_modified_fetches_the_zip_again(spark, tmp_path):
    z = _tiny_national(tmp_path)
    f = _Fetch(z)
    out = str(tmp_path / "csb")
    CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=f,
        _probe=_probe(z),
    ).download(out, spark=spark)
    same = CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=f,
        _probe=_probe(z),
    )
    same.download(out, force=True, spark=spark)
    assert f.calls == 1  # same size and Last-Modified: the cached zip is current
    newer = CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=f,
        _probe=_probe(z, "Tue, 06 Oct 2026 09:00:00 GMT"),
    )
    newer.download(out, force=True, spark=spark)
    assert f.calls == 2  # the server's file changed: fetched again
    assert not any(
        n.endswith(".d") for n in os.listdir(os.path.join(out, "_source"))
    )  # no extract on the Volume


def test_download_refuses_inside_a_spark_task(tmp_path, monkeypatch):
    import pyspark

    monkeypatch.setattr(pyspark.TaskContext, "get", staticmethod(lambda: object()))
    with pytest.raises(RuntimeError, match="driver"):
        CsbDownloader().download(str(tmp_path))


def test_download_refuses_without_local_disk(spark, tmp_path, monkeypatch):
    z = _tiny_national(tmp_path)
    monkeypatch.setattr(C, "MIN_LOCAL_FREE_BYTES", 1 << 62)  # more than any disk
    f = _Fetch(z)
    dl = CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=f,
        _probe=_probe(z),
    )
    with pytest.raises(OSError, match="local disk"):
        dl.download(str(tmp_path / "csb"), spark=spark)
    assert f.calls == 0  # refused before the 3.76 GB fetch, not after it


def test_the_last_modified_stamp_is_published_atomically(spark, tmp_path, monkeypatch):
    z = _tiny_national(tmp_path)
    out = str(tmp_path / "csb")
    replaced = []
    real_replace = os.replace
    monkeypatch.setattr(
        os, "replace", lambda a, b: (replaced.append(b), real_replace(a, b))[1]
    )
    CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=_Fetch(z),
        _probe=_probe(z),
    ).download(out, spark=spark)
    stamp = os.path.join(out, "_source", "NationalCSB_tiny.zip.last_modified")
    assert stamp in replaced  # temp file, then os.replace: never a half-written stamp
    with open(stamp) as fh:
        assert fh.read() == LAST_MODIFIED
    source = os.listdir(os.path.join(out, "_source"))
    assert ".gbx_scratch" not in source  # the stamp's scratch folder is removed
    assert sorted(source) == [
        "NationalCSB_tiny.zip",
        "NationalCSB_tiny.zip.last_modified",
    ]


def test_a_state_with_no_polys_raises_and_leaves_nothing(spark, tmp_path):
    z = _tiny_national(tmp_path)
    dl = CsbDownloader(
        href="https://www.nass.usda.gov/x/NationalCSB_tiny.zip",
        _fetch=_Fetch(z),
        _probe=_probe(z),
    )
    out = str(tmp_path / "csb")
    with pytest.raises(ValueError, match="no field polys"):
        dl.download(out, state_fips="02", spark=spark)
    assert not os.path.exists(os.path.join(out, "csb_02.gpkg"))
    assert not os.path.exists(os.path.join(out, ".gbx_scratch"))


def test_bad_state_raises(tmp_path):
    with pytest.raises(ValueError, match="two-digit"):
        CsbDownloader().download(str(tmp_path), state_fips="IA")


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("GBX_CSB_INTEGRATION") != "1",
    reason="3.76 GB download; set GBX_CSB_INTEGRATION=1",
)
def test_real_iowa(spark, tmp_path):
    info = _http.probe_range(C.CSB_ZIP_URL, allowed_hosts=C._ALLOWED_HOSTS)
    assert (
        info.size == C.CSB_ZIP_BYTES and info.last_modified
    )  # no ETag on this server: size + Last-Modified
    rows = CsbDownloader().download(str(tmp_path / "csb"), spark=spark).collect()
    assert rows[0]["is_out_file_valid"]
    assert (
        CsbDownloader().read(spark, str(tmp_path / "csb")).count() == 628_672
    )  # every Iowa poly, 2018-2025


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("GBX_CSB_LOCAL_ZIP"),
    reason="set GBX_CSB_LOCAL_ZIP to a copy of the national zip on local disk",
)
def test_real_iowa_from_a_local_copy_of_the_zip(spark, tmp_path):
    """The real FileGDB and layer, without the 3.76 GB download: the zip comes from a local path."""
    z = os.environ["GBX_CSB_LOCAL_ZIP"]
    dl = CsbDownloader(href=C.CSB_ZIP_URL, _fetch=_Fetch(z), _probe=_probe(z))
    rows = dl.download(str(tmp_path / "csb"), spark=spark).collect()
    assert rows[0]["is_out_file_valid"]
    df = dl.read(spark, str(tmp_path / "csb"))
    assert df.count() == 628_672
    assert set(df.columns) >= set(C.FIELDS) | {"geom"}
