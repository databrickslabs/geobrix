"""Offline unit tests for WorldPopDownloader (injected _download; no network)."""

from __future__ import annotations

import os

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.core import _http  # noqa: E402
from databricks.labs.gbx.sample.worldpop import (  # noqa: E402
    USA_2020_CONSTRAINED,
    WorldPopDownloader,
)

BBOX = [-101.5, 35.0, -100.0, 36.0]
BBOX2 = [-100.0, 35.0, -98.5, 36.0]


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """The default range probe never reaches the network: it says ranges are served."""

    def probe(url, allowed_hosts=None, timeout=100):
        return _http.RangeProbe(True, 1000, None)

    monkeypatch.setattr(_http, "probe_range", probe)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("worldpop-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


class _FakeDownload:
    """Stands in for ``stac._download.windowed_download``; writes a 2x2 GeoTIFF."""

    def __init__(self, raises=None, fail_after=None):
        self.calls = []
        self.raises = raises
        self.fail_after = fail_after  # calls made before every later one fails

    def __call__(self, href, outpath, bbox, **kw):
        self.calls.append((href, outpath, tuple(bbox), kw))
        if self.raises:
            raise self.raises
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            with open(outpath, "wb") as fh:  # a partial file, then the failure
                fh.write(b"partial")
            raise OSError("network dropped")
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin

        with rasterio.open(
            outpath,
            "w",
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(bbox[0], bbox[3], 0.5, 0.5),
        ) as dst:
            dst.write(np.ones((2, 2), dtype="float32"), 1)
        return outpath


def test_href_is_the_verified_worldpop_constrained_2020_usa_file():
    assert USA_2020_CONSTRAINED == (
        "https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/"
        "2020/BSGM/USA/usa_ppp_2020_constrained.tif"
    )


def test_download_passes_bbox_and_allowed_host(spark, tmp_path):
    fake = _FakeDownload()
    res = (
        WorldPopDownloader(_download=fake)
        .download(BBOX, str(tmp_path / "wp"), spark=spark)
        .collect()[0]
    )
    href, outpath, bbox, kw = fake.calls[0]
    assert href == USA_2020_CONSTRAINED
    assert bbox == tuple(BBOX)
    assert kw["bbox_crs"] == "EPSG:4326"
    assert kw["allowed_hosts"] == ("data.worldpop.org",)
    # the window is written on local disk (never under the Volume out_dir), then published under the bbox-tagged name
    assert os.path.basename(res["out_file_path"]) == os.path.basename(outpath)
    assert (
        not outpath.startswith(str(tmp_path))
        and ".gbx_scratch" not in res["out_file_path"]
    )
    assert res["is_out_file_valid"] is True
    assert res["out_file_sz"] == os.path.getsize(res["out_file_path"]) > 0
    assert outpath.endswith(".tif")


def test_columns_match_stac_download(spark, tmp_path):
    df = WorldPopDownloader(_download=_FakeDownload()).download(
        BBOX, str(tmp_path / "c"), spark=spark
    )
    assert df.columns == [
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    ]


def test_bbox_outside_the_asset_is_invalid_not_a_crash(spark, tmp_path):
    fake = _FakeDownload(raises=ValueError("bbox does not overlap"))
    with pytest.warns(UserWarning, match="does not overlap"):
        res = (
            WorldPopDownloader(_download=fake)
            .download(BBOX, str(tmp_path / "o"), spark=spark)
            .collect()[0]
        )
    assert res["is_out_file_valid"] is False
    assert not os.path.exists(res["out_file_path"])


def test_other_errors_propagate_and_leave_no_partial_file(spark, tmp_path):
    fake = _FakeDownload(raises=OSError("network down"))
    with pytest.raises(OSError):
        WorldPopDownloader(_download=fake).download(
            BBOX, str(tmp_path / "p"), spark=spark
        )
    assert not list((tmp_path / "p").glob("*.tif"))


def test_repair_skips_a_valid_file_and_redownloads_a_missing_one(spark, tmp_path):
    d = str(tmp_path / "r")
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    row = dl.download(BBOX, d, spark=spark).collect()[0]
    row = dl.repair(BBOX, d, spark=spark).collect()[0]
    assert len(fake.calls) == 1 and row["is_out_file_valid"] is True
    os.remove(row["out_file_path"])
    dl.repair(BBOX, d, spark=spark)
    assert len(fake.calls) == 2


def test_read_uses_raster_gbx(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    d = str(tmp_path / "rd")
    dl = WorldPopDownloader(_download=_FakeDownload())
    dl.download(BBOX, d, spark=spark)
    assert dl.read(d, spark=spark).count() == 1


def test_exports_from_sample():
    from databricks.labs.gbx.sample import WorldPopDownloader as W
    from databricks.labs.gbx.sample import download_worldpop_aoi

    assert W is WorldPopDownloader
    assert callable(download_worldpop_aoi)


def test_two_bboxes_in_one_dir_get_different_files(spark, tmp_path):
    d = str(tmp_path / "two")
    dl = WorldPopDownloader(_download=_FakeDownload())
    a = dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"]
    b = dl.download(BBOX2, d, spark=spark).collect()[0]["out_file_path"]
    assert a != b and os.path.exists(a) and os.path.exists(b)
    # deterministic: the same bbox names the same file
    assert dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"] == a


def test_repair_checks_the_file_of_this_bbox(spark, tmp_path):
    d = str(tmp_path / "rp")
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    dl.download(BBOX, d, spark=spark)  # only bbox 1 exists
    dl.repair(BBOX, d, spark=spark)
    assert len(fake.calls) == 1  # bbox 1 is on disk: no download
    row = dl.repair(BBOX2, d, spark=spark).collect()[0]
    assert len(fake.calls) == 2  # bbox 2 was NOT covered by bbox 1's file
    assert row["is_out_file_valid"] is True and os.path.exists(row["out_file_path"])


def test_failed_second_call_leaves_the_first_file_intact(spark, tmp_path):
    d = str(tmp_path / "keep")
    fake = _FakeDownload(fail_after=1)
    dl = WorldPopDownloader(_download=fake)
    path = dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"]
    before = open(path, "rb").read()
    with pytest.raises(OSError):
        dl.download(BBOX, d, spark=spark)
    assert open(path, "rb").read() == before
    assert os.listdir(d) == [os.path.basename(path)]  # no scratch or partial left


@pytest.mark.parametrize(
    "bad",
    [
        [1, 2, 3],
        [-101, 35, float("nan"), 36],
        [-101, 35, float("inf"), 36],
        [-100, 35, -101, 36],
        [-101, 36, -100, 35],
        "bbox",
        None,
    ],
)
def test_invalid_bbox_raises_with_fix(spark, tmp_path, bad):
    fake = _FakeDownload()
    with pytest.raises(ValueError, match="Fix:"):
        WorldPopDownloader(_download=fake).download(
            bad, str(tmp_path / "b"), spark=spark
        )
    assert fake.calls == []


def test_max_mpp_is_passed_through_and_changes_the_name(spark, tmp_path):
    d = str(tmp_path / "m")
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    a = dl.download(BBOX, d, spark=spark, max_mpp=0.01).collect()[0]["out_file_path"]
    assert fake.calls[0][3]["max_mpp"] == 0.01
    b = dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"]
    assert "max_mpp" not in fake.calls[1][3] and a != b


def test_window_over_the_decoded_budget_is_refused(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.core.budget.decoded_budget_bytes",
        lambda strategy: 1 << 20,  # 1 MiB; BBOX is about 8 MiB decoded
    )
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    with pytest.raises(ValueError, match="Fix:.*max_mpp"):
        dl.download(BBOX, str(tmp_path / "big"), spark=spark)
    assert fake.calls == []
    # decimating to ~0.01 degree brings the same bbox under the cap
    dl.download(BBOX, str(tmp_path / "big"), spark=spark, max_mpp=0.01)
    assert len(fake.calls) == 1


def test_read_with_bbox_reads_only_that_window(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    d = str(tmp_path / "rb")
    dl = WorldPopDownloader(_download=_FakeDownload())
    dl.download(BBOX, d, spark=spark)
    dl.download(BBOX2, d, spark=spark)
    assert dl.read(d, spark=spark).count() == 2
    assert dl.read(d, spark=spark, bbox=BBOX2).count() == 1


# ---------------------------------------------------------------------------
# A server with no range support (data.worldpop.org): the national file is downloaded once into
# cache_dir and every window is cut from that local copy, by the REAL windowed_download.
# ---------------------------------------------------------------------------

SRC_BBOX = [1.0, 1.0, 4.0, 4.0]  # 3 x 3 pixels of the 1-degree source grid below
SRC_BBOX2 = [4.0, 4.0, 7.0, 7.0]


def _write_source(path):
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
    return os.path.getsize(path)


class _Fetch:
    """Stands in for ``_http.download``: copies a local file, honoring expected_size like it."""

    def __init__(self, source, truncate=False):
        self.source = source
        self.truncate = truncate
        self.calls = []

    def __call__(self, url, outpath, allowed_hosts=None, **kw):
        self.calls.append((url, allowed_hosts, kw))
        data = open(self.source, "rb").read()
        with open(outpath, "wb") as fh:
            fh.write(data[: len(data) // 2] if self.truncate else data)


def _no_range(tmp_path, truncate=False, **kw):
    src = str(tmp_path / "national.tif")
    size = _write_source(src)
    fetch = _Fetch(src, truncate=truncate)
    probes = []

    def probe(url, allowed_hosts=None, timeout=100):
        probes.append((url, allowed_hosts))
        return _http.RangeProbe(False, size, '"abc"')

    dl = WorldPopDownloader(
        _probe=probe, _fetch=fetch, native_deg=1.0, **kw
    )  # default _download: the real windowed_download
    return dl, fetch, probes, size


def test_no_range_downloads_once_then_windows_the_local_copy(spark, tmp_path):
    import numpy as np
    import rasterio

    dl, fetch, probes, size = _no_range(tmp_path)
    out = str(tmp_path / "out")
    row = dl.download(SRC_BBOX, out, spark=spark).collect()[0]
    assert row["is_out_file_valid"] is True
    # the probe and the one download went to the allowed host, and the size and ETag were passed
    assert probes == [(USA_2020_CONSTRAINED, ("data.worldpop.org",))]
    url, hosts, kw = fetch.calls[0]
    assert url == USA_2020_CONSTRAINED and hosts == ("data.worldpop.org",)
    assert kw["expected_size"] == size and kw["expected_etag"] == '"abc"'
    # the source copy is published under <out_dir>/_source; no scratch is left behind
    cached = os.path.join(out, "_source", "usa_ppp_2020_constrained.tif")
    assert os.path.getsize(cached) == size
    assert not os.path.exists(os.path.join(out, "_source", ".gbx_scratch"))
    # the window is a real read of the local copy: rows 4..6, cols 1..3 of the 8x8 ramp
    with rasterio.open(row["out_file_path"]) as win:
        assert (win.width, win.height) == (3, 3)
        assert win.bounds.left == 1.0 and win.bounds.top == 4.0
        expect = np.arange(64, dtype="float32").reshape(8, 8)[4:7, 1:4]
        assert (win.read(1) == expect).all()


def test_second_bbox_reuses_the_cached_copy_without_a_second_download(spark, tmp_path):
    dl, fetch, probes, _ = _no_range(tmp_path)
    out = str(tmp_path / "out")
    a = dl.download(SRC_BBOX, out, spark=spark).collect()[0]["out_file_path"]
    b = dl.download(SRC_BBOX2, out, spark=spark).collect()[0]["out_file_path"]
    assert a != b and os.path.exists(a) and os.path.exists(b)
    assert len(fetch.calls) == 1  # the national file came down once
    assert len(probes) == 1  # and the href was probed once


def test_a_new_downloader_reuses_the_copy_by_its_size(spark, tmp_path):
    dl, fetch, _, size = _no_range(tmp_path)
    out = str(tmp_path / "out")
    dl.download(SRC_BBOX, out, spark=spark)
    dl2, fetch2, _, _ = _no_range(tmp_path)  # the same file, a fresh downloader
    dl2.download(SRC_BBOX2, out, spark=spark)
    assert fetch2.calls == []


def test_truncated_download_is_refused_and_publishes_nothing(spark, tmp_path):
    dl, fetch, _, _ = _no_range(tmp_path, truncate=True)
    out = str(tmp_path / "out")
    with pytest.raises(OSError, match="Fix:"):
        dl.download(SRC_BBOX, out, spark=spark)
    assert not os.path.exists(
        os.path.join(out, "_source", "usa_ppp_2020_constrained.tif")
    )
    assert not list((tmp_path / "out").rglob("*.tif"))
    assert not os.path.exists(os.path.join(out, "_source", ".gbx_scratch"))


def test_fetch_that_raises_leaves_no_copy(spark, tmp_path):
    dl, fetch, _, _ = _no_range(tmp_path)

    def boom(url, outpath, **kw):
        open(outpath, "wb").write(b"part")
        raise OSError("downloaded 4 bytes, expected 999. Fix: run it again.")

    dl._fetch = boom
    with pytest.raises(OSError):
        dl.download(SRC_BBOX, str(tmp_path / "o"), spark=spark)
    assert not list((tmp_path / "o").rglob("*.tif"))


def test_cache_dir_is_used_and_a_file_scheme_is_stripped(spark, tmp_path):
    dl, fetch, _, size = _no_range(tmp_path)
    cache = tmp_path / "vol" / "src"
    dl.download(SRC_BBOX, str(tmp_path / "o"), spark=spark, cache_dir=f"file:{cache}")
    assert (cache / "usa_ppp_2020_constrained.tif").stat().st_size == size
    assert not (tmp_path / "o" / "_source").exists()
    assert not os.path.exists("file:")  # a bare path, never a "file:" folder


def test_source_over_the_limit_is_refused_before_downloading(spark, tmp_path):
    dl, fetch, _, _ = _no_range(tmp_path, max_source_bytes=10)
    with pytest.raises(ValueError, match="Fix:"):
        dl.download(SRC_BBOX, str(tmp_path / "o"), spark=spark)
    assert fetch.calls == []


def test_range_support_keeps_the_direct_windowed_read(spark, tmp_path):
    fake = _FakeDownload()
    fetch = _Fetch("/nonexistent")
    dl = WorldPopDownloader(
        _download=fake,
        _fetch=fetch,
        _probe=lambda url, allowed_hosts=None: _http.RangeProbe(True, 5, None),
    )
    dl.download(BBOX, str(tmp_path / "r"), spark=spark)
    assert fake.calls[0][0] == USA_2020_CONSTRAINED  # windowed from the href itself
    assert fetch.calls == [] and not (tmp_path / "r" / "_source").exists()


def test_read_skips_the_source_copy(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    dl, _, _, _ = _no_range(tmp_path)
    out = str(tmp_path / "out")
    dl.download(SRC_BBOX, out, spark=spark)
    assert dl.read(out, spark=spark).count() == 1  # the window, not the source copy


def test_window_is_written_where_gdal_can_seek_not_under_the_volume(
    spark, tmp_path, volume_like_tmp
):
    """A Volume refuses GDAL's header write-back: the window must be written on local disk and copied over."""
    out_dir = str(tmp_path / "vol")
    fake = (
        _FakeDownload()
    )  # writes with rasterio: refused under out_dir, as a Volume refuses it
    res = (
        WorldPopDownloader(_download=fake)
        .download(BBOX, out_dir, spark=spark)
        .collect()[0]
    )
    assert res["is_out_file_valid"] is True
    import rasterio

    with rasterio.open(
        res["out_file_path"]
    ) as src:  # published bytes are a readable GeoTIFF
        assert src.shape == (2, 2)
    assert os.listdir(out_dir) == [
        os.path.basename(res["out_file_path"])
    ]  # no scratch left


def test_a_failing_local_temp_leaves_no_scratch_on_the_volume(
    spark, tmp_path, monkeypatch
):
    """The local temp dir is made first, inside the try: if it cannot be made, no .gbx_scratch is left under out_dir."""
    from databricks.labs.gbx.sample import worldpop

    def boom(prefix):
        raise OSError("no local disk")

    monkeypatch.setattr(worldpop, "new_local_temp_dir", boom)
    out_dir = tmp_path / "vol"
    with pytest.raises(OSError, match="no local disk"):
        WorldPopDownloader(_download=_FakeDownload()).download(
            BBOX, str(out_dir), spark=spark
        )
    assert os.listdir(out_dir) == []
