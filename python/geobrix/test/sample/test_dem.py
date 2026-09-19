"""Tests for DemDownloader — no network; StacClient is fully stubbed.

Mirrors test_naip.py's driver-path stubbing. Selection axis is gsd (resolution),
not year: 3dep-seamless offers the same area at 10 m and 30 m.

Coverage:
  D1  — discover() returns [item_id, gsd, item_bbox, href], only the "data" asset
  D2  — discover(resolution=10) filters to that gsd
  D3  — discover() deduplicates overlapping items
  DL1 — download(resolution="finest") picks the minimum gsd (10 m)
  DL2 — download(resolution=30) selects that exact gsd
  DL3 — download() passes bbox / bbox_crs / max_mpp / partitions to StacClient.download
  DL4 — download() returned DataFrame carries StacClient.download schema
  DL5 — download() empty result carries the canonical schema (no href, Boolean valid)
  DL6 — download(resolution="finest") with no gsd property keeps all items (graceful)
  E1  — export: DemDownloader + download_dem_aoi importable from sample
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.gbx.sample.dem import (  # noqa: E402
    DemDownloader,
    _bbox_to_geojson_polygon,
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("dem-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


# Two gsd tiers (10 m + 30 m), two AOI items each; asset "data" + a non-data asset.
_FAKE_ITEMS = [
    {
        "id": "dep_10_a",
        "bbox": [-122.45, 37.74, -122.40, 37.78],
        "properties": {"datetime": "2021-01-01T00:00:00Z", "gsd": "10"},
        "assets": {
            "data": {"href": "file:///fake/dep_10_a.tif"},
            "rendered_preview": {"href": "file:///fake/prev_a.png"},
        },
    },
    {
        "id": "dep_10_b",
        "bbox": [-122.50, 37.70, -122.42, 37.76],
        "properties": {"datetime": "2021-01-01T00:00:00Z", "gsd": "10"},
        "assets": {"data": {"href": "file:///fake/dep_10_b.tif"}},
    },
    {
        "id": "dep_30_a",
        "bbox": [-122.45, 37.74, -122.40, 37.78],
        "properties": {"datetime": "2019-01-01T00:00:00Z", "gsd": "30"},
        "assets": {"data": {"href": "file:///fake/dep_30_a.tif"}},
    },
    {
        "id": "dep_30_b",
        "bbox": [-122.50, 37.70, -122.42, 37.76],
        "properties": {"datetime": "2019-01-01T00:00:00Z", "gsd": "30"},
        "assets": {"data": {"href": "file:///fake/dep_30_b.tif"}},
    },
]

_FAKE_ITEMS_NO_GSD = [
    {
        "id": "dep_nogsd",
        "bbox": [-122.45, 37.74, -122.40, 37.78],
        "properties": {"datetime": "2020-01-01T00:00:00Z"},
        "assets": {"data": {"href": "file:///fake/dep_nogsd.tif"}},
    },
]


class _MockStacClient:
    """Captures search + download calls; returns controlled DataFrames."""

    def __init__(self, search_df, download_df=None):
        self._search_df = search_df
        self._download_df = download_df
        self.download_calls = []

    def search(self, df, geojson_col, collections, datetime, partitions=512):
        return self._search_df

    def download(self, df, out_dir, **kwargs):
        rows = df.select("item_id", "asset_name", "href").collect()
        self.download_calls.append(
            {
                "item_ids": sorted(r["item_id"] for r in rows),
                "out_dir": out_dir,
                **kwargs,
            }
        )
        spark = SparkSession.getActiveSession()
        schema = StructType(
            [
                StructField("item_id", StringType()),
                StructField("asset_name", StringType()),
                StructField("out_file_path", StringType()),
                StructField("out_file_sz", LongType()),
                StructField("is_out_file_valid", BooleanType()),
            ]
        )
        if self._download_df is not None:
            return self._download_df
        _rows = [
            (iid, "data", f"/fake/out/{iid}.tif", 1000, True)
            for iid in self.download_calls[-1]["item_ids"]
        ]
        return spark.createDataFrame(_rows, schema).withColumn(
            "last_update", F.current_timestamp()
        )


def _make_search_df(spark, items):
    rows = []
    for d in items:
        props = {k: str(v) for k, v in d["properties"].items()}
        bbox = list(d["bbox"])
        date = d["properties"].get("datetime", "")[:10]
        for asset_name, asset in d["assets"].items():
            rows.append((d["id"], date, bbox, props, asset_name, asset["href"]))
    schema = StructType(
        [
            StructField("item_id", StringType()),
            StructField("date", StringType()),
            StructField("item_bbox", ArrayType(DoubleType())),
            StructField("item_properties", MapType(StringType(), StringType())),
            StructField("asset_name", StringType()),
            StructField("href", StringType()),
        ]
    )
    return spark.createDataFrame(rows if rows else [], schema)


def _make_download_df(spark, item_ids):
    rows = [(iid, "data", f"/fake/out/{iid}.tif", 12345, True) for iid in item_ids]
    schema = StructType(
        [
            StructField("item_id", StringType()),
            StructField("asset_name", StringType()),
            StructField("out_file_path", StringType()),
            StructField("out_file_sz", LongType()),
            StructField("is_out_file_valid", BooleanType()),
        ]
    )
    return spark.createDataFrame(rows, schema).withColumn(
        "last_update", F.current_timestamp()
    )


# --- D1 ---
def test_discover_returns_expected_columns_and_rows(spark):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS))
    dd = DemDownloader(_stac_client=mock)
    df = dd.discover((-122.52, 37.70, -122.36, 37.83), spark=spark)
    assert set(df.columns) == {"item_id", "gsd", "item_bbox", "href"}, df.columns
    rows = df.collect()
    assert {r["item_id"] for r in rows} == {
        "dep_10_a",
        "dep_10_b",
        "dep_30_a",
        "dep_30_b",
    }
    # only "data" asset hrefs (rendered_preview filtered out)
    assert all("prev" not in r["href"] for r in rows)


# --- D2 ---
def test_discover_resolution_filter(spark):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS))
    dd = DemDownloader(_stac_client=mock)
    rows = dd.discover(
        (-122.52, 37.70, -122.36, 37.83), resolution=10, spark=spark
    ).collect()
    assert len(rows) == 2 and all(r["gsd"] == 10 for r in rows)
    assert {r["item_id"] for r in rows} == {"dep_10_a", "dep_10_b"}


# --- D3 ---
def test_discover_deduplicates_items(spark):
    mock = _MockStacClient(_make_search_df(spark, _FAKE_ITEMS + _FAKE_ITEMS))
    dd = DemDownloader(_stac_client=mock)
    ids = [
        r["item_id"]
        for r in dd.discover((-122.52, 37.70, -122.36, 37.83), spark=spark).collect()
    ]
    assert len(ids) == len(set(ids)), ids


# --- DL1: finest picks the minimum gsd (10 m) ---
def test_download_picks_finest_gsd(spark, tmp_path):
    mock = _MockStacClient(
        _make_search_df(spark, _FAKE_ITEMS),
        _make_download_df(spark, ["dep_10_a", "dep_10_b"]),
    )
    dd = DemDownloader(_stac_client=mock)
    result = dd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "o"),
        resolution="finest",
        spark=spark,
    )
    assert len(mock.download_calls) == 1
    assert set(mock.download_calls[0]["item_ids"]) == {"dep_10_a", "dep_10_b"}
    assert {"item_id", "asset_name", "out_file_path", "last_update"} <= {
        f.name for f in result.schema
    }


# --- DL2: resolution=int selects that gsd ---
def test_download_resolution_int_selects_tier(spark, tmp_path):
    mock = _MockStacClient(
        _make_search_df(spark, _FAKE_ITEMS),
        _make_download_df(spark, ["dep_30_a", "dep_30_b"]),
    )
    dd = DemDownloader(_stac_client=mock)
    dd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "o"),
        resolution=30,
        spark=spark,
    )
    assert set(mock.download_calls[0]["item_ids"]) == {"dep_30_a", "dep_30_b"}


# --- DL3: kwargs forwarded ---
def test_download_passes_kwargs(spark, tmp_path):
    mock = _MockStacClient(
        _make_search_df(spark, _FAKE_ITEMS), _make_download_df(spark, ["dep_10_a"])
    )
    bbox = (-122.52, 37.70, -122.36, 37.83)
    dd = DemDownloader(_stac_client=mock)
    dd.download(
        bbox,
        str(tmp_path / "o"),
        resolution="finest",
        bbox_crs="EPSG:4326",
        max_mpp=5.0,
        partitions=8,
        spark=spark,
    )
    call = mock.download_calls[0]
    assert call["bbox"] == list(bbox)
    assert call["bbox_crs"] == "EPSG:4326"
    assert call["max_mpp"] == 5.0
    assert call["partitions"] == 8


# --- DL4: result carries StacClient.download schema ---
def test_download_returns_stac_schema(spark, tmp_path):
    mock = _MockStacClient(
        _make_search_df(spark, _FAKE_ITEMS), _make_download_df(spark, ["dep_10_a"])
    )
    dd = DemDownloader(_stac_client=mock)
    result = dd.download(
        (-122.52, 37.70, -122.36, 37.83), str(tmp_path / "o"), spark=spark
    )
    required = {
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    }
    assert required <= {f.name for f in result.schema}


# --- DL5: empty result carries the canonical schema ---
def test_download_empty_returns_canonical_schema(spark, tmp_path):
    mock = _MockStacClient(_make_search_df(spark, []))
    dd = DemDownloader(_stac_client=mock)
    result = dd.download(
        (-122.52, 37.70, -122.36, 37.83), str(tmp_path / "o"), spark=spark
    )
    assert result.count() == 0
    schema_map = {f.name: f.dataType for f in result.schema}
    assert {
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    } <= set(schema_map)
    assert "href" not in schema_map
    assert isinstance(schema_map["is_out_file_valid"], BooleanType)
    assert isinstance(schema_map["out_file_sz"], LongType)


# --- DL6: finest with no gsd property keeps all items (graceful) ---
def test_download_finest_no_gsd_keeps_all(spark, tmp_path):
    mock = _MockStacClient(
        _make_search_df(spark, _FAKE_ITEMS_NO_GSD),
        _make_download_df(spark, ["dep_nogsd"]),
    )
    dd = DemDownloader(_stac_client=mock)
    dd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "o"),
        resolution="finest",
        spark=spark,
    )
    assert mock.download_calls[0]["item_ids"] == ["dep_nogsd"]


# --- 3dep-lidar-dtm (2 m today; 1 m where available): resolution lives in the item
#     id ("-dtm-<N>m-"), not a gsd property (gsd is null for this collection). ---
_FAKE_LIDAR_DTM = [
    # 1 m item (QL1) — proves "finest" can reach 1 m where a survey provides it
    {
        "id": "USGS_LPC_AreaX_QL1_2020-dtm-1m-1-2",
        "bbox": [-100.05, 40.00, -100.00, 40.05],
        "properties": {"start_datetime": "2020-01-01T00:00:00Z"},  # NO gsd, NO datetime
        "assets": {
            "data": {"href": "file:///fake/x_1m.tif"},
            "rendered_preview": {"href": "file:///fake/x_prev.png"},
        },
    },
    # 2 m item (what SF actually offers today)
    {
        "id": "USGS_LPC_CA_Wildfires_QL1_2018-dtm-2m-6-0",
        "bbox": [-100.05, 40.00, -100.00, 40.05],
        "properties": {"start_datetime": "2018-01-01T00:00:00Z"},  # NO gsd
        "assets": {"data": {"href": "file:///fake/x_2m.tif"}},
    },
]

_LIDAR_BBOX = (-100.05, 40.00, -100.00, 40.05)


# --- LD1: discover() derives gsd (metres) from the item id when no gsd property ---
def test_lidar_dtm_discover_parses_resolution_from_id(spark):
    from databricks.labs.gbx.sample.dem import DEM_LIDAR_DTM_COLLECTION

    mock = _MockStacClient(_make_search_df(spark, _FAKE_LIDAR_DTM))
    dd = DemDownloader(collection=DEM_LIDAR_DTM_COLLECTION, _stac_client=mock)
    rows = dd.discover(_LIDAR_BBOX, spark=spark).collect()
    gsd_by_id = {r["item_id"]: r["gsd"] for r in rows}
    assert gsd_by_id["USGS_LPC_AreaX_QL1_2020-dtm-1m-1-2"] == 1
    assert gsd_by_id["USGS_LPC_CA_Wildfires_QL1_2018-dtm-2m-6-0"] == 2


# --- LD2: resolution filter works off the id-parsed metres ---
def test_lidar_dtm_resolution_filter(spark):
    from databricks.labs.gbx.sample.dem import DEM_LIDAR_DTM_COLLECTION

    mock = _MockStacClient(_make_search_df(spark, _FAKE_LIDAR_DTM))
    dd = DemDownloader(collection=DEM_LIDAR_DTM_COLLECTION, _stac_client=mock)
    r1 = dd.discover(_LIDAR_BBOX, resolution=1, spark=spark).collect()
    assert {r["item_id"] for r in r1} == {"USGS_LPC_AreaX_QL1_2020-dtm-1m-1-2"}
    r2 = dd.discover(_LIDAR_BBOX, resolution=2, spark=spark).collect()
    assert {r["item_id"] for r in r2} == {"USGS_LPC_CA_Wildfires_QL1_2018-dtm-2m-6-0"}


# --- LD3: download(finest) picks the minimum id-parsed resolution (1 m) ---
def test_lidar_dtm_download_finest_picks_1m(spark, tmp_path):
    from databricks.labs.gbx.sample.dem import DEM_LIDAR_DTM_COLLECTION

    mock = _MockStacClient(
        _make_search_df(spark, _FAKE_LIDAR_DTM),
        _make_download_df(spark, ["USGS_LPC_AreaX_QL1_2020-dtm-1m-1-2"]),
    )
    dd = DemDownloader(collection=DEM_LIDAR_DTM_COLLECTION, _stac_client=mock)
    dd.download(_LIDAR_BBOX, str(tmp_path / "o"), resolution="finest", spark=spark)
    assert mock.download_calls[0]["item_ids"] == ["USGS_LPC_AreaX_QL1_2020-dtm-1m-1-2"]


# --- LD4: lidar_dtm() convenience + collection constant ---
def test_lidar_dtm_convenience_and_constant():
    from databricks.labs.gbx.sample.dem import DEM_LIDAR_DTM_COLLECTION, DemDownloader

    assert DEM_LIDAR_DTM_COLLECTION == "3dep-lidar-dtm"
    dd = DemDownloader.lidar_dtm()
    assert dd.collection == DEM_LIDAR_DTM_COLLECTION
    assert dd.asset == "data"


# --- E1 ---
def test_export_dem_downloader_from_sample_init():
    from databricks.labs.gbx.sample import DemDownloader as DD
    from databricks.labs.gbx.sample import download_dem_aoi as dda

    assert DD is DemDownloader
    assert callable(dda)


def test_bbox_to_geojson_polygon_shape():
    import json

    d = json.loads(_bbox_to_geojson_polygon((-1.0, 2.0, 3.0, 4.0)))
    assert d["type"] == "Polygon"
    ring = d["coordinates"][0]
    assert len(ring) == 5 and ring[0] == ring[-1]
