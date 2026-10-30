"""Tests for NaipDownloader — no network; StacClient is fully stubbed.

Stubbing strategy mirrors test_client.py's driver-path approach:
- Pass ``_catalog_opener`` to StacClient (which forces the _search_driver
  shim — all search runs on the driver, no UDF pickling, no addPyFile needed).
- Pass ``_get_fn`` to StacClient.download() for asset bytes (via the
  NaipDownloader._stac_client injection seam).
- For download tests: build a MockStacClient that records call arguments and
  returns a controlled DataFrame, keeping tests fast and deterministic.

Test coverage:
  - discover() returns expected columns + item rows
  - discover(year=X) filters to that year
  - discover() deduplicates overlapping items
  - download() picks the latest vintage (max naip:year)
  - download(year=int) selects that specific vintage
  - download() passes bbox / bbox_crs / max_mpp to StacClient.download
  - download() returned DataFrame carries StacClient.download schema
  - download() falls back to max(date) when naip:year is absent
  - export check: NaipDownloader + download_naip_aoi importable from sample
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

from databricks.labs.gbx.sample.naip import (  # noqa: E402
    NaipDownloader,
    _bbox_to_geojson_polygon,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("naip-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# Fake STAC catalog: returns NAIP-shaped items for two years (2020 + 2022)
# ---------------------------------------------------------------------------

# Two AOI items per year, each with an "image" asset and a "thumbnail" asset.
_FAKE_ITEMS = [
    {
        "id": "naip_2020_a",
        "collection": "naip",
        "bbox": [-122.45, 37.74, -122.40, 37.78],
        "properties": {
            "datetime": "2020-07-15T00:00:00Z",
            "naip:year": "2020",
        },
        "assets": {
            "image": {"href": "file:///fake/naip_2020_a.tif"},
            "thumbnail": {"href": "file:///fake/naip_2020_a_thumb.jpg"},
        },
    },
    {
        "id": "naip_2020_b",
        "collection": "naip",
        "bbox": [-122.50, 37.70, -122.42, 37.76],
        "properties": {
            "datetime": "2020-08-01T00:00:00Z",
            "naip:year": "2020",
        },
        "assets": {
            "image": {"href": "file:///fake/naip_2020_b.tif"},
            "thumbnail": {"href": "file:///fake/naip_2020_b_thumb.jpg"},
        },
    },
    {
        "id": "naip_2022_a",
        "collection": "naip",
        "bbox": [-122.45, 37.74, -122.40, 37.78],
        "properties": {
            "datetime": "2022-06-10T00:00:00Z",
            "naip:year": "2022",
        },
        "assets": {
            "image": {"href": "file:///fake/naip_2022_a.tif"},
            "thumbnail": {"href": "file:///fake/naip_2022_a_thumb.jpg"},
        },
    },
    {
        "id": "naip_2022_b",
        "collection": "naip",
        "bbox": [-122.50, 37.70, -122.42, 37.76],
        "properties": {
            "datetime": "2022-09-05T00:00:00Z",
            "naip:year": "2022",
        },
        "assets": {
            "image": {"href": "file:///fake/naip_2022_b.tif"},
            "thumbnail": {"href": "file:///fake/naip_2022_b_thumb.jpg"},
        },
    },
]

# Variant: items whose properties omit naip:year (for the fallback-year test).
_FAKE_ITEMS_NO_YEAR = [
    {
        "id": "naip_noyear_2019",
        "collection": "naip",
        "bbox": [-122.45, 37.74, -122.40, 37.78],
        "properties": {
            "datetime": "2019-07-15T00:00:00Z",
        },
        "assets": {
            "image": {"href": "file:///fake/naip_2019.tif"},
        },
    },
    {
        "id": "naip_noyear_2021",
        "collection": "naip",
        "bbox": [-122.45, 37.74, -122.40, 37.78],
        "properties": {
            "datetime": "2021-08-01T00:00:00Z",
        },
        "assets": {
            "image": {"href": "file:///fake/naip_2021.tif"},
        },
    },
]


class _FakeItem:
    def __init__(self, d):
        self._d = d

    def to_dict(self):
        return self._d


class _FakeSearch:
    def __init__(self, items):
        self._items = items

    def item_collection(self):
        return [_FakeItem(d) for d in self._items]


class _FakeCatalog:
    def __init__(self, items):
        self._items = items

    def search(self, collections, intersects, datetime):
        return _FakeSearch(self._items)


def _make_stac_client_with_items(items):
    """Build a StacClient with a fake catalog returning the given item dicts."""
    from databricks.labs.gbx.stac import StacClient

    return StacClient(
        catalog="http://fake-naip-catalog",
        sign=None,
        _catalog_opener=lambda: _FakeCatalog(items),
    )


# ---------------------------------------------------------------------------
# MockStacClient — records download() call args, returns a controlled DF
# ---------------------------------------------------------------------------


class _MockStacClient:
    """Minimal StacClient mock that captures search and download calls.

    search() returns a controlled DataFrame; download() records its arguments
    and returns a DataFrame matching the real StacClient.download() schema.
    """

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
        if self._download_df is not None:
            return self._download_df
        # Return a minimal valid DataFrame when no download_df was given.
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            LongType,
            StringType,
            StructField,
            StructType,
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
        _rows = [
            (iid, "image", f"/fake/out/{iid}.tif", 1000, True)
            for iid in self.download_calls[-1]["item_ids"]
        ]
        return spark.createDataFrame(_rows, schema).withColumn(
            "last_update", F.current_timestamp()
        )


def _make_search_df(spark, items):
    """Build a search-result DataFrame matching StacClient.search() output."""
    rows = []
    for d in items:
        props = {k: str(v) for k, v in d["properties"].items()}
        bbox = list(d["bbox"])
        # Keep the date as the first 10 chars of datetime.
        date = d["properties"].get("datetime", "")[:10]
        for asset_name, asset in d["assets"].items():
            rows.append(
                (
                    d["id"],
                    date,
                    bbox,
                    props,
                    asset_name,
                    asset["href"],
                )
            )
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
    if not rows:
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows, schema)


def _make_download_df(spark, item_ids):
    """Return a fake StacClient.download() result DataFrame."""
    rows = [(iid, "image", f"/fake/out/{iid}.tif", 12345, True) for iid in item_ids]
    schema = StructType(
        [
            StructField("item_id", StringType()),
            StructField("asset_name", StringType()),
            StructField("out_file_path", StringType()),
            StructField("out_file_sz", LongType()),
            StructField("is_out_file_valid", BooleanType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    return df.withColumn("last_update", F.current_timestamp())


# ---------------------------------------------------------------------------
# discover() returns expected columns + item rows
# ---------------------------------------------------------------------------


def test_discover_returns_expected_columns_and_rows(spark):
    """discover() columns = [item_id, year, item_bbox, href], all 4 items.

    Uses _MockStacClient to avoid tenacity dependency (search_one imports it
    when a real StacClient with _catalog_opener calls the driver-shim path).
    """
    search_df = _make_search_df(spark, _FAKE_ITEMS)
    mock = _MockStacClient(search_df)
    nd = NaipDownloader(_stac_client=mock)
    df = nd.discover((-122.52, 37.70, -122.36, 37.83), spark=spark)

    assert set(df.columns) == {
        "item_id",
        "year",
        "item_bbox",
        "href",
    }, f"Unexpected columns: {df.columns}"
    rows = df.collect()
    ids = {r["item_id"] for r in rows}
    assert ids == {"naip_2020_a", "naip_2020_b", "naip_2022_a", "naip_2022_b"}
    # Only image asset hrefs should appear (thumbnail filtered out).
    hrefs = {r["href"] for r in rows}
    assert all(
        "thumb" not in h for h in hrefs
    ), f"Thumbnail leaked into discover: {hrefs}"


# ---------------------------------------------------------------------------
# discover(year=X) filters to that year
# ---------------------------------------------------------------------------


def test_discover_year_filter(spark):
    """discover(year=2020) keeps only 2020 items."""
    search_df = _make_search_df(spark, _FAKE_ITEMS)
    mock = _MockStacClient(search_df)
    nd = NaipDownloader(_stac_client=mock)
    df = nd.discover((-122.52, 37.70, -122.36, 37.83), year=2020, spark=spark)

    rows = df.collect()
    assert len(rows) == 2
    assert all(r["year"] == 2020 for r in rows)
    ids = {r["item_id"] for r in rows}
    assert ids == {"naip_2020_a", "naip_2020_b"}


def test_discover_year_filter_2022(spark):
    """discover(year=2022) keeps only 2022 items."""
    search_df = _make_search_df(spark, _FAKE_ITEMS)
    mock = _MockStacClient(search_df)
    nd = NaipDownloader(_stac_client=mock)
    df = nd.discover((-122.52, 37.70, -122.36, 37.83), year=2022, spark=spark)

    rows = df.collect()
    assert len(rows) == 2
    assert all(r["year"] == 2022 for r in rows)


# ---------------------------------------------------------------------------
# discover() deduplicates (each item appears exactly once)
# ---------------------------------------------------------------------------


def test_discover_deduplicates_items(spark):
    """distinct() means the same item_id appears at most once."""
    # Duplicate the fake items to simulate overlapping AOI searches.
    search_df = _make_search_df(spark, _FAKE_ITEMS + _FAKE_ITEMS)
    mock = _MockStacClient(search_df)
    nd = NaipDownloader(_stac_client=mock)
    df = nd.discover((-122.52, 37.70, -122.36, 37.83), spark=spark)

    rows = df.collect()
    ids = [r["item_id"] for r in rows]
    assert len(ids) == len(set(ids)), f"Duplicate item_ids in discover output: {ids}"


# ---------------------------------------------------------------------------
# download() picks the latest vintage
# ---------------------------------------------------------------------------


def test_download_picks_latest_vintage(spark, tmp_path):
    """year='latest' selects the max naip:year (2022 here)."""
    search_df = _make_search_df(spark, _FAKE_ITEMS)
    dl_df = _make_download_df(spark, ["naip_2022_a", "naip_2022_b"])
    mock = _MockStacClient(search_df, dl_df)

    nd = NaipDownloader(_stac_client=mock)
    result = nd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "out"),
        year="latest",
        spark=spark,
    )

    assert len(mock.download_calls) == 1, "Expected exactly one download() call"
    call = mock.download_calls[0]
    # Only the 2022 items should have been passed to download.
    assert set(call["item_ids"]) == {
        "naip_2022_a",
        "naip_2022_b",
    }, f"Latest-vintage selection passed wrong items: {call['item_ids']}"
    # Result schema contains expected columns.
    col_names = {f.name for f in result.schema}
    assert {"item_id", "asset_name", "out_file_path", "last_update"} <= col_names


# ---------------------------------------------------------------------------
# download(year=int) selects that specific vintage
# ---------------------------------------------------------------------------


def test_download_year_int_selects_vintage(spark, tmp_path):
    """year=2020 downloads only 2020 tiles."""
    search_df = _make_search_df(spark, _FAKE_ITEMS)
    dl_df = _make_download_df(spark, ["naip_2020_a", "naip_2020_b"])
    mock = _MockStacClient(search_df, dl_df)

    nd = NaipDownloader(_stac_client=mock)
    nd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "out"),
        year=2020,
        spark=spark,
    )

    call = mock.download_calls[0]
    assert set(call["item_ids"]) == {
        "naip_2020_a",
        "naip_2020_b",
    }, f"year=2020 selection wrong: {call['item_ids']}"


# ---------------------------------------------------------------------------
# download() passes bbox / bbox_crs / max_mpp to StacClient.download
# ---------------------------------------------------------------------------


def test_download_passes_kwargs_to_stac_client(spark, tmp_path):
    """bbox, bbox_crs, and max_mpp are forwarded to StacClient.download."""
    search_df = _make_search_df(spark, _FAKE_ITEMS)
    dl_df = _make_download_df(spark, ["naip_2022_a", "naip_2022_b"])
    mock = _MockStacClient(search_df, dl_df)

    bbox = (-122.52, 37.70, -122.36, 37.83)
    nd = NaipDownloader(_stac_client=mock)
    nd.download(
        bbox,
        str(tmp_path / "out"),
        year="latest",
        bbox_crs="EPSG:4326",
        max_mpp=1.0,
        partitions=8,
        spark=spark,
    )

    call = mock.download_calls[0]
    assert call["bbox"] == list(bbox), f"bbox mismatch: {call['bbox']}"
    assert call["bbox_crs"] == "EPSG:4326", f"bbox_crs mismatch: {call['bbox_crs']}"
    assert call["max_mpp"] == 1.0, f"max_mpp mismatch: {call['max_mpp']}"
    assert call["partitions"] == 8, f"partitions mismatch: {call['partitions']}"


# ---------------------------------------------------------------------------
# download() returned DataFrame has StacClient.download schema
# ---------------------------------------------------------------------------


def test_download_returns_stac_download_schema(spark, tmp_path):
    """download() output columns match StacClient.download() output."""
    search_df = _make_search_df(spark, _FAKE_ITEMS)
    dl_df = _make_download_df(spark, ["naip_2022_a"])
    mock = _MockStacClient(search_df, dl_df)

    nd = NaipDownloader(_stac_client=mock)
    result = nd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "out"),
        spark=spark,
    )

    col_names = {f.name for f in result.schema}
    required = {
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    }
    missing = required - col_names
    assert not missing, f"download() result missing columns: {missing}"


# ---------------------------------------------------------------------------
# download() falls back to max(date) when naip:year is absent
# ---------------------------------------------------------------------------


def test_download_falls_back_to_date_when_no_naip_year(spark, tmp_path):
    """When naip:year is absent, year is derived from date field."""
    search_df = _make_search_df(spark, _FAKE_ITEMS_NO_YEAR)
    dl_df = _make_download_df(spark, ["naip_noyear_2021"])
    mock = _MockStacClient(search_df, dl_df)

    nd = NaipDownloader(_stac_client=mock)
    nd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "out"),
        year="latest",
        spark=spark,
    )

    call = mock.download_calls[0]
    # Latest year from dates is 2021 (dates: "2019-07-15", "2021-08-01").
    assert call["item_ids"] == [
        "naip_noyear_2021"
    ], f"date-fallback latest selection wrong: {call['item_ids']}"


# ---------------------------------------------------------------------------
# empty vintage returns correct schema (no href, BooleanType is_out_file_valid)
# ---------------------------------------------------------------------------


def test_download_empty_vintage_returns_correct_schema(spark, tmp_path):
    """When no items are found the result schema matches a non-empty download.

    Verifies the I1 fix: the empty-path no longer hand-builds a frame with
    ``href`` and wrong types — it flows through ``StacClient.download`` which
    returns the canonical schema.
    """
    # Empty search result — no items match.
    empty_search_df = _make_search_df(spark, [])
    mock = _MockStacClient(empty_search_df)

    nd = NaipDownloader(_stac_client=mock)
    result = nd.download(
        (-122.52, 37.70, -122.36, 37.83),
        str(tmp_path / "out"),
        year="latest",
        spark=spark,
    )

    # Must have 0 rows.
    assert result.count() == 0, "Expected empty DataFrame for empty vintage"

    # Schema must match the non-empty download schema: canonical field names + types.
    schema_map = {f.name: f.dataType for f in result.schema}
    required = {
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    }
    missing = required - set(schema_map)
    assert not missing, f"Empty-vintage result missing columns: {missing}"

    # href must NOT appear in the output (it is an input-only column).
    assert (
        "href" not in schema_map
    ), "Empty-vintage result incorrectly includes 'href' column"

    # is_out_file_valid must be BooleanType (not StringType or NullType).
    assert isinstance(
        schema_map["is_out_file_valid"], BooleanType
    ), f"is_out_file_valid should be BooleanType, got {schema_map['is_out_file_valid']}"

    # out_file_sz must be LongType.
    assert isinstance(
        schema_map["out_file_sz"], LongType
    ), f"out_file_sz should be LongType, got {schema_map['out_file_sz']}"


# ---------------------------------------------------------------------------
# export check
# ---------------------------------------------------------------------------


def test_export_naip_downloader_from_sample_init():
    """NaipDownloader and download_naip_aoi are importable from sample."""
    from databricks.labs.gbx.sample import NaipDownloader as ND
    from databricks.labs.gbx.sample import download_naip_aoi as dna

    assert ND is NaipDownloader
    assert callable(dna)


# ---------------------------------------------------------------------------
# Helper unit test — _bbox_to_geojson_polygon
# ---------------------------------------------------------------------------


def test_bbox_to_geojson_polygon_shape():
    """_bbox_to_geojson_polygon produces a valid closed GeoJSON Polygon."""
    import json

    geojson = _bbox_to_geojson_polygon((-1.0, 2.0, 3.0, 4.0))
    d = json.loads(geojson)
    assert d["type"] == "Polygon"
    ring = d["coordinates"][0]
    assert len(ring) == 5  # closed ring
    assert ring[0] == ring[-1]  # first == last


# ---------------------------------------------------------------------------
# Integration (real Planetary Computer — skipped in unit CI)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_naip_discover_real_pc(spark, tmp_path):
    """Integration: real PC catalog, small AOI in the Bay Area."""
    nd = NaipDownloader()
    # Small AOI: downtown San Francisco
    bbox = (-122.42, 37.77, -122.39, 37.80)
    df = nd.discover(bbox, year=2020, spark=spark)
    assert df.count() > 0, "Expected at least one NAIP item for SF 2020"
    row = df.first()
    assert row["year"] == 2020
    assert row["href"].startswith("https://")
