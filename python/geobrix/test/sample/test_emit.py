"""Offline unit tests for EmitDownloader (injected earthaccess; no network)."""

from __future__ import annotations

import json
import os

import numpy as np
import pytest

pyspark = pytest.importorskip("pyspark")

import rasterio  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from databricks.labs.gbx.sample.emit import EmitDownloader  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("emit-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


class _FakeGranule:
    def __init__(self, links):
        self._links = links

    def data_links(self, access=None, in_region=False):
        return self._links


class _FakeEarthaccess:
    """Records login/search/download; returns controlled granules."""

    def __init__(self, granules_by_short, writer=None):
        self._by_short = granules_by_short
        self._writer = writer  # optional fn(granules, local_path) that writes files
        self.login_calls = 0
        self.download_calls = []

    def login(self, strategy="all", persist=False):
        self.login_calls += 1
        return object()

    def search_data(
        self,
        short_name=None,
        version=None,
        bounding_box=None,
        temporal=None,
        count=-1,
        **kw,
    ):
        return list(self._by_short.get(short_name, []))

    def download(self, granules, local_path=None, threads=8, **kw):
        self.download_calls.append({"n": len(granules), "local_path": local_path})
        if self._writer is not None:
            self._writer(granules, local_path)
        return []


def _fake_ea(writer=None):
    enh = _FakeGranule(
        [
            "https://x/EMIT_L2B_CH4ENH_002_20240823T1_o_s.tif",
            "https://x/EMIT_L2B_CH4UNCERT_002_20240823T1_o_s.tif",
        ]
    )
    plm = _FakeGranule(
        [
            "https://x/EMIT_L2B_CH4PLM_002_20240823T1_p.tif",
            "https://x/EMIT_L2B_CH4PLM_002_20240823T1_p.json",
        ]
    )
    return _FakeEarthaccess(
        {"EMITL2BCH4ENH": [enh], "EMITL2BCH4PLM": [plm]}, writer=writer
    )


BBOX = [-103.9, 31.65, -103.4, 32.15]


def _write_cog(path):
    # 64x64 so the encoded file clears the 1 KB COG validity floor.
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=64,
        height=64,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(-103.9, 32.15, 0.001, 0.001),
    ) as ds:
        ds.write(np.arange(64 * 64, dtype="float32").reshape(64, 64), 1)


def _write_geojson(path):
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"rate": 1200.0},
                "geometry": {"type": "Point", "coordinates": [-103.65, 31.9]},
            }
        ],
    }
    with open(path, "w") as fh:
        json.dump(fc, fh)


def _write_plm_meta(path):
    # EMIT CH4PLMMETA GeoJSON: the exact JPL property names read_plumes normalizes.
    props = {
        "Plume ID": "CH4_PlumeComplex-1",
        "UTC Time Observed": "2024-08-23T17:34:10Z",
        "Orbit": "2423611",
        "DCID": "1408315670",
        "Max Plume Concentration (ppm m)": 2715.0,
        "Latitude of max concentration": 31.90,
        "Longitude of max concentration": -103.65,
        "Wind Speed (m/s)": 4.4,
        "Wind Speed Std (m/s)": 0.2,
        "Wind Speed Source": "HRRR",
        "Emissions Rate Estimate (kg/hr)": 1622.3,
        "Emissions Rate Estimate Uncertainty (kg/hr)": 115.4,
        "Fetch Length (m)": 906.1,
    }
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": props,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-103.66, 31.89], [-103.64, 31.89], [-103.64, 31.91],
                            [-103.66, 31.91], [-103.66, 31.89],
                        ]
                    ],
                },
            }
        ],
    }
    with open(path, "w") as fh:
        json.dump(fc, fh)


def test_discover_extracts_enh_and_plm_assets(spark):
    dl = EmitDownloader(_earthaccess=_fake_ea())
    df = dl.discover(BBOX, spark=spark)
    rows = {(r["asset_name"], r["href"].split("/")[-1]) for r in df.collect()}
    assert ("ch4enh", "EMIT_L2B_CH4ENH_002_20240823T1_o_s.tif") in rows
    assert ("plm_cog", "EMIT_L2B_CH4PLM_002_20240823T1_p.tif") in rows
    assert ("plm_geojson", "EMIT_L2B_CH4PLM_002_20240823T1_p.json") in rows
    # the CH4UNCERT tif is not a selected asset
    assert all("CH4UNCERT" not in r["href"] for r in df.collect())
    assert [f.name for f in df.schema.fields] == ["item_id", "asset_name", "href"]


def test_download_validates_and_builds_result_frame(spark, tmp_path):
    def _writer(granules, local_path):
        # client.download passes href strings (earthaccess.download's str form)
        os.makedirs(local_path, exist_ok=True)
        for url in granules:
            dest = os.path.join(local_path, os.path.basename(url))
            if dest.endswith(".json"):
                _write_geojson(dest)
            else:
                _write_cog(dest)

    dl = EmitDownloader(_earthaccess=_fake_ea(writer=_writer))
    res = dl.download(BBOX, str(tmp_path / "emit"), spark=spark)
    assert [f.name for f in res.schema.fields] == [
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    ]
    rows = res.collect()
    assert len(rows) == 3  # ch4enh + plm_cog + plm_geojson
    assert all(r["is_out_file_valid"] for r in rows)
    by_asset = {r["asset_name"]: r for r in rows}
    assert by_asset["ch4enh"]["out_file_sz"] > 1024  # COG clears the size floor
    assert by_asset["plm_cog"]["out_file_sz"] > 1024
    assert by_asset["plm_geojson"]["out_file_sz"] > 0  # small valid GeoJSON is fine


def test_download_marks_missing_invalid(spark, tmp_path):
    # writer that writes nothing -> every asset invalid, null path
    dl = EmitDownloader(_earthaccess=_fake_ea(writer=lambda g, p: None))
    rows = dl.download(BBOX, str(tmp_path / "emit2"), spark=spark).collect()
    assert len(rows) == 3
    assert all(not r["is_out_file_valid"] for r in rows)
    assert all(r["out_file_path"] is None for r in rows)


def test_read_enh_uses_raster_gbx(spark, tmp_path):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    d = tmp_path / "emit3"
    d.mkdir()
    _write_cog(str(d / "EMIT_L2B_CH4ENH_002_a.tif"))
    _write_cog(str(d / "EMIT_L2B_CH4ENH_002_b.tif"))
    _write_cog(str(d / "EMIT_L2B_CH4UNCERT_002_a.tif"))  # excluded by filter
    df = EmitDownloader(_earthaccess=_fake_ea()).read_enh(str(d), spark=spark)
    assert df.count() == 2
    assert "tile" in df.columns


def test_read_plumes_uses_geojson_gbx(spark, tmp_path):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    d = tmp_path / "emit4"
    d.mkdir()
    _write_plm_meta(str(d / "EMIT_L2B_CH4PLMMETA_002_p.json"))
    df = EmitDownloader(_earthaccess=_fake_ea()).read_plumes(str(d), spark=spark)
    assert df.count() == 1
    cols = [f.name for f in df.schema.fields]
    assert "plume_geom" in cols and "emission_rate_kg_hr" in cols
    row = df.first()
    assert row["plume_id"] == "CH4_PlumeComplex-1"
    assert abs(row["emission_rate_kg_hr"] - 1622.3) < 1e-6
    assert abs(row["max_conc_ppmm"] - 2715.0) < 1e-6


def test_exports_from_sample():
    from databricks.labs.gbx.sample import EmitDownloader, download_emit_aoi

    assert EmitDownloader is not None
    assert callable(download_emit_aoi)
